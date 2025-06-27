extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;
use core::ffi::c_int;

use serde::Serialize;
use serde_json as json;
use sqlite::ResultCode;
use sqlite_nostd as sqlite;
use sqlite_nostd::{Connection, Context, Value};

use crate::create_sqlite_text_fn;
use crate::error::PowerSyncError;
use crate::sync::checkpoint::{validate_checkpoint, OwnedBucketChecksum};
use crate::sync::line::Checkpoint;

#[derive(Serialize)]
struct CheckpointResult {
    valid: bool,
    failed_buckets: Vec<String>,
}

fn powersync_validate_checkpoint_impl(
    ctx: *mut sqlite::context,
    args: &[*mut sqlite::value],
) -> Result<String, PowerSyncError> {
    let data = args[0].text();
    let checkpoint: Checkpoint = serde_json::from_str(data)?;
    let db = ctx.db_handle();
    let buckets: Vec<OwnedBucketChecksum> = checkpoint
        .buckets
        .iter()
        .map(OwnedBucketChecksum::from)
        .collect();

    let failures = validate_checkpoint(buckets.iter(), None, db)?;
    let mut failed_buckets = Vec::<String>::with_capacity(failures.len());
    for failure in failures {
        failed_buckets.push(failure.bucket_name);
    }

    let result = CheckpointResult {
        valid: failed_buckets.is_empty(),
        failed_buckets: failed_buckets,
    };

    Ok(json::to_string(&result)?)
}

create_sqlite_text_fn!(
    powersync_validate_checkpoint,
    powersync_validate_checkpoint_impl,
    "powersync_validate_checkpoint"
);

pub fn register(db: *mut sqlite::sqlite3) -> Result<(), ResultCode> {
    db.create_function_v2(
        "powersync_validate_checkpoint",
        1,
        sqlite::UTF8 | sqlite::DETERMINISTIC,
        None,
        Some(powersync_validate_checkpoint),
        None,
        None,
        None,
    )?;

    Ok(())
}
