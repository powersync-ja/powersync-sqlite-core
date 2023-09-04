extern crate alloc;


use alloc::format;
use alloc::string::{String};
use alloc::vec::Vec;
use core::ffi::c_int;
use core::slice;

use serde::{Deserialize, Serialize};
use serde_json as json;
use sqlite::ResultCode;
use sqlite_nostd as sqlite;
use sqlite_nostd::{Connection, Context, Value};

use crate::create_sqlite_text_fn;
use crate::error::SQLiteError;
use crate::sync_types::Checkpoint;

#[derive(Serialize, Deserialize)]
struct CheckpointResult {
    valid: bool,
    failed_buckets: Vec<String>,
}

fn powersync_validate_checkpoint_impl(
    ctx: *mut sqlite::context,
    args: &[*mut sqlite::value],
) -> Result<String, SQLiteError> {
    let data = args[0].text();

    let _checkpoint: Checkpoint = serde_json::from_str(data)?;

    let db = ctx.db_handle();

    // language=SQLite
    let statement = db.prepare_v2("WITH
bucket_list(bucket, lower_op_id, checksum) AS (
  SELECT
        json_extract(json_each.value, '$.bucket') as bucket,
        0 as lower_op_id,
        json_extract(json_each.value, '$.checksum') as checksum
  FROM json_each(json_extract(?1, '$.buckets'))
)
SELECT
  bucket_list.bucket as bucket,
  IFNULL(buckets.add_checksum, 0) as add_checksum,
  IFNULL(SUM(oplog.hash), 0) as oplog_checksum,
  COUNT(oplog.op_id) as count,
  IFNULL(MAX(oplog.op_id), 0) as last_op_id,
  IFNULL(buckets.last_applied_op, 0) as last_applied_op,
  bucket_list.checksum as expected_checksum
FROM bucket_list
  LEFT OUTER JOIN ps_buckets AS buckets ON
     buckets.name = bucket_list.bucket
  LEFT OUTER JOIN ps_oplog AS oplog ON
     bucket_list.bucket = oplog.bucket AND
     oplog.op_id <= CAST(json_extract(?1, '$.last_op_id') as INTEGER) AND
     oplog.op_id > bucket_list.lower_op_id
GROUP BY bucket_list.bucket")?;

    statement.bind_text(1, data, sqlite::Destructor::STATIC)?;

    let mut failures: Vec<String> = alloc::vec![];

    while statement.step()? == ResultCode::ROW {
        let name = statement.column_text(0)?;
        let add_checksum = statement.column_int(1)?;
        let oplog_checksum = statement.column_int(2)?;
        let _count = statement.column_int(3)?;
        let _last_op_id = statement.column_int64(4)?;
        let _last_applied_op = statement.column_int64(5)?;
        let expected_checksum = statement.column_int(6)?;

        let checksum = oplog_checksum + add_checksum;

        if checksum != expected_checksum {
            failures.push(String::from(name));
        }
    }

    let result = CheckpointResult {
        valid: failures.is_empty(),
        failed_buckets: failures,
    };

    Ok(json::to_string(&result)?)
}

create_sqlite_text_fn!(powersync_validate_checkpoint, powersync_validate_checkpoint_impl, "powersync_validate_checkpoint");

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


