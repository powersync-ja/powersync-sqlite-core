use core::cell::RefCell;
use core::ffi::{c_int, c_void};

use alloc::borrow::Cow;
use alloc::boxed::Box;
use alloc::rc::Rc;
use alloc::sync::Arc;
use alloc::{string::String, vec::Vec};
use serde::{Deserialize, Serialize};
use sqlite::{ResultCode, Value};
use sqlite_nostd::{self as sqlite, ColumnType};
use sqlite_nostd::{Connection, Context};

use crate::error::PowerSyncError;
use crate::schema::Schema;
use crate::state::DatabaseState;

use super::streaming_sync::SyncClient;
use super::sync_status::DownloadSyncStatus;

/// Payload provided by SDKs when requesting a sync iteration.
#[derive(Default, Deserialize)]
pub struct StartSyncStream {
    /// Bucket parameters to include in the request when opening a sync stream.
    #[serde(default)]
    pub parameters: Option<serde_json::Map<String, serde_json::Value>>,
    #[serde(default)]
    pub schema: Schema,
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
    /// Forward a text line (JSON) received from the sync service.
    TextLine { data: &'a str },
    /// Forward a binary line (BSON) received from the sync service.
    BinaryLine { data: &'a [u8] },
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
    CloseSyncStream {},
    /// Flush the file-system if it's non-durable (only applicable to the Dart SDK).
    FlushFileSystem {},
    /// Notify that a sync has been completed, prompting client SDKs to clear earlier errors.
    DidCompleteSync {},
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

pub fn register(db: *mut sqlite::sqlite3, state: Arc<DatabaseState>) -> Result<(), ResultCode> {
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
                _ => {
                    return Err(PowerSyncError::argument_error("Unknown operation"));
                }
            };

            let instructions = controller.client.push_event(event)?;
            let formatted =
                serde_json::to_string(&instructions).map_err(PowerSyncError::internal)?;
            ctx.result_text_transient(&formatted);

            Ok(())
        })();

        if let Err(e) = result {
            e.apply_to_ctx("powersync_control", ctx);
        }
    }

    unsafe extern "C" fn destroy(ptr: *mut c_void) {
        drop(Box::from_raw(ptr.cast::<SqlController>()));
    }

    let controller = Box::new(SqlController {
        client: SyncClient::new(db, state),
    });

    db.create_function_v2(
        "powersync_control",
        2,
        sqlite::UTF8 | sqlite::DIRECTONLY,
        Some(Box::into_raw(controller).cast()),
        Some(control),
        None,
        None,
        Some(destroy),
    )?;

    Ok(())
}
