use core::cell::RefCell;
use core::ffi::{c_int, c_void};

use alloc::borrow::Cow;
use alloc::boxed::Box;
use alloc::rc::Rc;
use alloc::string::ToString;
use alloc::{string::String, vec::Vec};
use serde::Serialize;
use sqlite::{ResultCode, Value};
use sqlite_nostd::{self as sqlite, ColumnType};
use sqlite_nostd::{Connection, Context};

use crate::error::SQLiteError;
use crate::util::context_set_error;

use super::streaming_sync::SyncClient;
use super::sync_status::DownloadSyncStatus;

pub enum SyncControlRequest<'a> {
    StartSyncStream {
        parameters: Option<serde_json::Map<String, serde_json::Value>>,
    },
    StopSyncStream,
    SyncEvent(SyncEvent<'a>),
}

pub enum SyncEvent<'a> {
    Initialize,
    TearDown,
    DidRefreshToken,
    UploadFinished,
    TextLine { data: &'a str },
    BinaryLine { data: &'a [u8] },
}

/// An instruction sent by the core extension to the SDK.
#[derive(Serialize)]
pub enum Instruction {
    LogLine {
        severity: LogSeverity,
        line: Cow<'static, str>,
    },
    UpdateSyncStatus {
        status: Rc<RefCell<DownloadSyncStatus>>,
    },
    EstablishSyncStream {
        request: StreamingSyncRequest,
    },
    FetchCredentials {
        did_expire: bool,
    },
    // These are defined like this because deserializers in Kotlin can't support either an
    // object or a literal value
    CloseSyncStream {},
    FlushFileSystem {},
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

struct SqlController {
    client: SyncClient,
}

pub fn register(db: *mut sqlite::sqlite3) -> Result<(), ResultCode> {
    extern "C" fn control(
        ctx: *mut sqlite::context,
        argc: c_int,
        argv: *mut *mut sqlite::value,
    ) -> () {
        let result = (|| -> Result<(), SQLiteError> {
            let controller = unsafe { ctx.user_data().cast::<SqlController>().as_ref() }
                .ok_or_else(|| SQLiteError::from(ResultCode::INTERNAL))?;

            let args = sqlite::args!(argc, argv);
            let [op, payload] = args else {
                return Err(ResultCode::MISUSE.into());
            };

            if op.value_type() != ColumnType::Text {
                return Err(SQLiteError(
                    ResultCode::MISUSE,
                    Some("First argument must be a string".to_string()),
                ));
            }

            let op = op.text();
            let event = match op {
                "start" => SyncControlRequest::StartSyncStream {
                    parameters: if payload.value_type() == ColumnType::Text {
                        Some(serde_json::from_str(payload.text())?)
                    } else {
                        None
                    },
                },
                "stop" => SyncControlRequest::StopSyncStream,
                "line_text" => SyncControlRequest::SyncEvent(SyncEvent::TextLine {
                    data: if payload.value_type() == ColumnType::Text {
                        payload.text()
                    } else {
                        return Err(SQLiteError(
                            ResultCode::MISUSE,
                            Some("Second argument must be a string".to_string()),
                        ));
                    },
                }),
                "line_binary" => SyncControlRequest::SyncEvent(SyncEvent::BinaryLine {
                    data: if payload.value_type() == ColumnType::Blob {
                        payload.blob()
                    } else {
                        return Err(SQLiteError(
                            ResultCode::MISUSE,
                            Some("Second argument must be a byte array".to_string()),
                        ));
                    },
                }),
                "refreshed_token" => SyncControlRequest::SyncEvent(SyncEvent::DidRefreshToken),
                "completed_upload" => SyncControlRequest::SyncEvent(SyncEvent::UploadFinished),
                _ => {
                    return Err(SQLiteError(
                        ResultCode::MISUSE,
                        Some("Unknown operation".to_string()),
                    ))
                }
            };

            let instructions = controller.client.push_event(event)?;
            let formatted = serde_json::to_string(&instructions)?;
            ctx.result_text_transient(&formatted);

            Ok(())
        })();

        if let Err(e) = result {
            context_set_error(ctx, e, "powersync_control");
        }
    }

    unsafe extern "C" fn destroy(ptr: *mut c_void) {
        drop(Box::from_raw(ptr.cast::<SqlController>()));
    }

    let controller = Box::new(SqlController {
        client: SyncClient::new(db),
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
