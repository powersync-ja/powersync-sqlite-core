use core::cell::RefCell;
use core::ffi::{c_int, c_void};

use super::streaming_sync::SyncClient;
use super::sync_status::DownloadSyncStatus;
use crate::constants::SUBTYPE_JSON;
use crate::create_sqlite_text_fn;
use crate::error::PowerSyncError;
use crate::schema::Schema;
use crate::state::DatabaseState;
use crate::sync::storage_adapter::StorageAdapter;
use crate::sync::subscriptions::{StreamKey, apply_subscriptions};
use alloc::borrow::Cow;
use alloc::boxed::Box;
use alloc::rc::Rc;
use alloc::{string::String, vec::Vec};
use powersync_sqlite_nostd::bindings::SQLITE_RESULT_SUBTYPE;
use powersync_sqlite_nostd::{self as sqlite, ColumnType};
use powersync_sqlite_nostd::{Connection, Context};
use serde::{Deserialize, Serialize};
use sqlite::{ResultCode, Value};

use crate::sync::BucketPriority;
use crate::util::JsonString;

/// Payload provided by SDKs when requesting a sync iteration.
#[derive(Deserialize)]
pub struct StartSyncStream {
    /// Bucket parameters to include in the request when opening a sync stream.
    #[serde(default)]
    pub parameters: Option<serde_json::Map<String, serde_json::Value>>,
    #[serde(default)]
    pub schema: Schema,

    /// Whether to request default streams in the generated sync request.
    #[serde(default = "StartSyncStream::include_defaults_by_default")]
    pub include_defaults: bool,
    /// Streams that are currently active in the app.
    ///
    /// We will increase the expiry date for those streams at the time we connect and disconnect.
    #[serde(default)]
    pub active_streams: Rc<Vec<StreamKey>>,
}

impl StartSyncStream {
    pub const fn include_defaults_by_default() -> bool {
        true
    }
}

impl Default for StartSyncStream {
    fn default() -> Self {
        Self {
            parameters: Default::default(),
            schema: Default::default(),
            include_defaults: Self::include_defaults_by_default(),
            active_streams: Default::default(),
        }
    }
}

/// A request sent from a client SDK to the [SyncClient] with a `powersync_control` invocation.
pub enum SyncControlRequest<'a> {
    /// The client requests to start a sync iteration.
    ///
    /// Earlier iterations are implicitly dropped when receiving this request.
    StartSyncStream(StartSyncStream),
    /// The client requests to stop the current sync iteration.
    StopSyncStream,
    /// The client is forwading a sync event to the core extension.
    SyncEvent(SyncEvent<'a>),
}

pub enum SyncEvent<'a> {
    /// A synthetic event forwarded to the [SyncClient] after being started.
    Initialize,
    /// An event requesting the sync client to shut down.
    TearDown,
    /// Notifies the sync client that a token has been refreshed.
    ///
    /// In response, we'll stop the current iteration to begin another one with the new token.
    DidRefreshToken,
    /// Notifies the sync client that the current CRUD upload (for which the client SDK is
    /// responsible) has finished.
    ///
    /// If pending CRUD entries have previously prevented a sync from completing, this even can be
    /// used to try again.
    UploadFinished,
    ConnectionEstablished,
    StreamEnded,
    /// Forward a text line (JSON) received from the sync service.
    TextLine {
        data: &'a str,
    },
    /// Forward a binary line (BSON) received from the sync service.
    BinaryLine {
        data: &'a [u8],
    },
    /// The active stream subscriptions (as in, `SyncStreamSubscription` instances active right now)
    /// have changed.
    ///
    /// The client will compare the new active subscriptions with the current one and will issue a
    /// request to restart the sync iteration if necessary.
    DidUpdateSubscriptions {
        active_streams: Rc<Vec<StreamKey>>,
    },
}

/// An instruction sent by the core extension to the SDK.
#[derive(Serialize)]
pub enum Instruction {
    LogLine {
        severity: LogSeverity,
        line: Cow<'static, str>,
    },
    /// Update the download status for the ongoing sync iteration.
    UpdateSyncStatus {
        status: Rc<RefCell<DownloadSyncStatus>>,
    },
    /// Connect to the sync service using the [StreamingSyncRequest] created by the core extension,
    /// and then forward received lines via [SyncEvent::TextLine] and [SyncEvent::BinaryLine].
    EstablishSyncStream { request: StreamingSyncRequest },
    FetchCredentials {
        /// Whether the credentials currently used have expired.
        ///
        /// If false, this is a pre-fetch.
        did_expire: bool,
    },
    // These are defined like this because deserializers in Kotlin can't support either an
    // object or a literal value
    /// Close the websocket / HTTP stream to the sync service.
    CloseSyncStream(CloseSyncStream),
    /// Flush the file-system if it's non-durable (only applicable to the Dart SDK).
    FlushFileSystem {},
    /// Notify that a sync has been completed, prompting client SDKs to clear earlier errors.
    DidCompleteSync {},
}

#[derive(Serialize, Default)]
pub struct CloseSyncStream {
    /// Whether clients should hide the brief disconnected status from the public sync status and
    /// reconnect immediately.
    pub hide_disconnect: bool,
}

#[derive(Serialize)]
pub enum LogSeverity {
    DEBUG,
    INFO,
    WARNING,
}

#[derive(Serialize)]
pub struct StreamingSyncRequest {
    pub buckets: Vec<BucketRequest>,
    pub include_checksum: bool,
    pub raw_data: bool,
    pub binary_data: bool,
    pub client_id: String,
    pub parameters: Option<serde_json::Map<String, serde_json::Value>>,
    pub streams: Rc<StreamSubscriptionRequest>,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct StreamSubscriptionRequest {
    pub include_defaults: bool,
    pub subscriptions: Vec<RequestedStreamSubscription>,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct RequestedStreamSubscription {
    /// The name of the sync stream to subscribe to.
    pub stream: String,
    /// Parameters to make available in the stream's definition.
    pub parameters: Option<Box<JsonString>>,
    pub override_priority: Option<BucketPriority>,
}

#[derive(Serialize)]
pub struct BucketRequest {
    pub name: String,
    pub after: String,
}

/// Wrapper around a [SyncClient].
///
/// We allocate one instance of this per database (in [register]) - the [SyncClient] has an initial
/// empty state that doesn't consume any resources.
struct SqlController {
    client: SyncClient,
}

pub fn register(db: *mut sqlite::sqlite3, state: Rc<DatabaseState>) -> Result<(), ResultCode> {
    extern "C" fn control(
        ctx: *mut sqlite::context,
        argc: c_int,
        argv: *mut *mut sqlite::value,
    ) -> () {
        let result = (|| -> Result<(), PowerSyncError> {
            debug_assert!(!ctx.db_handle().get_autocommit());

            let controller = unsafe { ctx.user_data().cast::<SqlController>().as_mut() }
                .ok_or_else(|| PowerSyncError::unknown_internal())?;

            let args = sqlite::args!(argc, argv);
            let [op, payload] = args else {
                // This should be unreachable, we register the function with two arguments.
                return Err(PowerSyncError::unknown_internal());
            };

            if op.value_type() != ColumnType::Text {
                return Err(PowerSyncError::argument_error(
                    "First argument must be a string",
                ));
            }

            let op = op.text();
            let event = match op {
                "start" => SyncControlRequest::StartSyncStream({
                    if payload.value_type() == ColumnType::Text {
                        serde_json::from_str(payload.text())
                            .map_err(PowerSyncError::as_argument_error)?
                    } else {
                        StartSyncStream::default()
                    }
                }),
                "stop" => SyncControlRequest::StopSyncStream,
                "line_text" => SyncControlRequest::SyncEvent(SyncEvent::TextLine {
                    data: if payload.value_type() == ColumnType::Text {
                        payload.text()
                    } else {
                        return Err(PowerSyncError::argument_error(
                            "Second argument must be a string",
                        ));
                    },
                }),
                "line_binary" => SyncControlRequest::SyncEvent(SyncEvent::BinaryLine {
                    data: if payload.value_type() == ColumnType::Blob {
                        payload.blob()
                    } else {
                        return Err(PowerSyncError::argument_error(
                            "Second argument must be a byte array",
                        ));
                    },
                }),
                "refreshed_token" => SyncControlRequest::SyncEvent(SyncEvent::DidRefreshToken),
                "completed_upload" => SyncControlRequest::SyncEvent(SyncEvent::UploadFinished),
                "update_subscriptions" => {
                    SyncControlRequest::SyncEvent(SyncEvent::DidUpdateSubscriptions {
                        active_streams: serde_json::from_str(payload.text())
                            .map_err(PowerSyncError::as_argument_error)?,
                    })
                }
                "connection" => SyncControlRequest::SyncEvent(match payload.text() {
                    "established" => SyncEvent::ConnectionEstablished,
                    "end" => SyncEvent::StreamEnded,
                    _ => {
                        return Err(PowerSyncError::argument_error("unknown connection event"));
                    }
                }),
                "subscriptions" => {
                    let request = serde_json::from_str(payload.text())
                        .map_err(PowerSyncError::as_argument_error)?;
                    return apply_subscriptions(ctx.db_handle(), request);
                }
                _ => {
                    return Err(PowerSyncError::argument_error("Unknown operation"));
                }
            };

            let instructions = controller.client.push_event(event)?;
            let formatted =
                serde_json::to_string(&instructions).map_err(PowerSyncError::internal)?;
            ctx.result_text_transient(&formatted);
            ctx.result_subtype(SUBTYPE_JSON);

            Ok(())
        })();

        if let Err(e) = result {
            e.apply_to_ctx("powersync_control", ctx);
        }
    }

    unsafe extern "C" fn destroy(ptr: *mut c_void) {
        drop(unsafe { Box::from_raw(ptr.cast::<SqlController>()) });
    }

    let controller = Box::new(SqlController {
        client: SyncClient::new(db, state),
    });

    db.create_function_v2(
        "powersync_control",
        2,
        sqlite::UTF8 | sqlite::DIRECTONLY | SQLITE_RESULT_SUBTYPE,
        Some(Box::into_raw(controller).cast()),
        Some(control),
        None,
        None,
        Some(destroy),
    )?;

    db.create_function_v2(
        "powersync_offline_sync_status",
        0,
        sqlite::UTF8 | sqlite::DIRECTONLY | SQLITE_RESULT_SUBTYPE,
        None,
        Some(powersync_offline_sync_status),
        None,
        None,
        None,
    )?;

    Ok(())
}

fn powersync_offline_sync_status_impl(
    ctx: *mut sqlite::context,
    _args: &[*mut sqlite::value],
) -> Result<String, PowerSyncError> {
    let adapter = StorageAdapter::new(ctx.db_handle())?;
    let state = adapter.offline_sync_state()?;
    let serialized = serde_json::to_string(&state).map_err(PowerSyncError::internal)?;

    Ok(serialized)
}

create_sqlite_text_fn!(
    powersync_offline_sync_status,
    powersync_offline_sync_status_impl,
    "powersync_offline_sync_status"
);
