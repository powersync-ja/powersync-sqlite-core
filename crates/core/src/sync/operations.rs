use alloc::format;
use alloc::string::String;
use num_traits::Zero;
use sqlite_nostd::Connection;
use sqlite_nostd::{self as sqlite, ResultCode};

use crate::{
    error::{PSResult, SQLiteError},
    ext::SafeManagedStmt,
};

use super::line::OplogData;
use super::Checksum;
use super::{
    line::{DataLine, OpType},
    storage_adapter::{BucketInfo, StorageAdapter},
};

pub fn insert_bucket_operations(
    adapter: &StorageAdapter,
    data: &DataLine,
) -> Result<(), SQLiteError> {
    let db = adapter.db;
    let BucketInfo {
        id: bucket_id,
        last_applied_op,
    } = adapter.lookup_bucket(&*data.bucket)?;

    // This is an optimization for initial sync - we can avoid persisting individual REMOVE
    // operations when last_applied_op = 0.
    // We do still need to do the "supersede_statement" step for this case, since a REMOVE
    // operation can supersede another PUT operation we're syncing at the same time.
    let mut is_empty = last_applied_op == 0;

    // Statement to supersede (replace) operations with the same key.
    // language=SQLite
    let supersede_statement = db.prepare_v2(
        "\
DELETE FROM ps_oplog
    WHERE unlikely(ps_oplog.bucket = ?1)
    AND ps_oplog.row_type = ?2
    AND ps_oplog.row_id = ?3
    AND ps_oplog.subkey = ?4
RETURNING op_id, hash",
    )?;
    supersede_statement.bind_int64(1, bucket_id)?;

    // language=SQLite
    let insert_statement = db.prepare_v2("\
INSERT INTO ps_oplog(bucket, op_id, subkey, row_type, row_id, data, hash) VALUES (?, ?, ?, ?, ?, ?, ?)")?;
    insert_statement.bind_int64(1, bucket_id)?;

    let updated_row_statement = db.prepare_v2(
        "\
INSERT OR IGNORE INTO ps_updated_rows(row_type, row_id) VALUES(?1, ?2)",
    )?;

    let mut last_op: Option<i64> = None;
    let mut add_checksum = Checksum::zero();
    let mut op_checksum = Checksum::zero();
    let mut added_ops: i32 = 0;

    for line in &data.data {
        let op_id = line.op_id;
        let op = line.op;
        let object_type = line.object_type.as_ref();
        let object_id = line.object_id.as_ref();
        let checksum = line.checksum;
        let op_data = line.data.as_ref();

        last_op = Some(op_id);
        added_ops += 1;

        if op == OpType::PUT || op == OpType::REMOVE {
            let subkey = line.subkey.as_ref().map(|i| &**i);

            if let Some(subkey) = subkey {
                supersede_statement.bind_text(4, &subkey, sqlite::Destructor::STATIC)?;
            } else {
                supersede_statement.bind_text(4, "", sqlite::Destructor::STATIC)?;
            }

            if let Some(object_type) = object_type {
                supersede_statement.bind_text(2, &object_type, sqlite::Destructor::STATIC)?;
            } else {
                supersede_statement.bind_text(2, "", sqlite::Destructor::STATIC)?;
            }

            if let Some(object_id) = object_id {
                supersede_statement.bind_text(3, &object_id, sqlite::Destructor::STATIC)?;
            } else {
                supersede_statement.bind_text(3, "", sqlite::Destructor::STATIC)?;
            }

            let mut superseded = false;

            while supersede_statement.step()? == ResultCode::ROW {
                // Superseded (deleted) a previous operation, add the checksum
                let supersede_checksum = Checksum::from_i32(supersede_statement.column_int(1));
                add_checksum += supersede_checksum;
                op_checksum -= supersede_checksum;

                // Superseded an operation, only skip if the bucket was empty
                // Previously this checked "superseded_op <= last_applied_op".
                // However, that would not account for a case where a previous
                // PUT operation superseded the original PUT operation in this
                // same batch, in which case superseded_op is not accurate for this.
                if !is_empty {
                    superseded = true;
                }
            }
            supersede_statement.reset()?;

            if op == OpType::REMOVE {
                let should_skip_remove = !superseded;

                add_checksum += checksum;

                if !should_skip_remove {
                    if let (Some(object_type), Some(object_id)) = (object_type, object_id) {
                        updated_row_statement.bind_text(
                            1,
                            object_type,
                            sqlite::Destructor::STATIC,
                        )?;
                        updated_row_statement.bind_text(
                            2,
                            object_id,
                            sqlite::Destructor::STATIC,
                        )?;
                        updated_row_statement.exec()?;
                    }
                }

                continue;
            }

            insert_statement.bind_int64(2, op_id)?;
            if let Some(subkey) = subkey {
                insert_statement.bind_text(3, &subkey, sqlite::Destructor::STATIC)?;
            } else {
                insert_statement.bind_text(3, "", sqlite::Destructor::STATIC)?;
            }

            if let (Some(object_type), Some(object_id)) = (object_type, object_id) {
                insert_statement.bind_text(4, object_type, sqlite::Destructor::STATIC)?;
                insert_statement.bind_text(5, object_id, sqlite::Destructor::STATIC)?;
            } else {
                insert_statement.bind_null(4)?;
                insert_statement.bind_null(5)?;
            }
            if let Some(data) = op_data {
                let OplogData::Json { data } = data;

                insert_statement.bind_text(6, data, sqlite::Destructor::STATIC)?;
            } else {
                insert_statement.bind_null(6)?;
            }

            insert_statement.bind_int(7, checksum.bitcast_i32())?;
            insert_statement.exec()?;

            op_checksum += checksum;
        } else if op == OpType::MOVE {
            add_checksum += checksum;
        } else if op == OpType::CLEAR {
            // Any remaining PUT operations should get an implicit REMOVE
            // language=SQLite
            let clear_statement1 = db
                .prepare_v2(
                    "INSERT OR IGNORE INTO ps_updated_rows(row_type, row_id)
SELECT row_type, row_id
FROM ps_oplog
WHERE bucket = ?1",
                )
                .into_db_result(db)?;
            clear_statement1.bind_int64(1, bucket_id)?;
            clear_statement1.exec()?;

            let clear_statement2 = db
                .prepare_v2("DELETE FROM ps_oplog WHERE bucket = ?1")
                .into_db_result(db)?;
            clear_statement2.bind_int64(1, bucket_id)?;
            clear_statement2.exec()?;

            // And we need to re-apply all of those.
            // We also replace the checksum with the checksum of the CLEAR op.
            // language=SQLite
            let clear_statement2 = db.prepare_v2(
                "UPDATE ps_buckets SET last_applied_op = 0, add_checksum = ?1, op_checksum = 0 WHERE id = ?2",
            )?;
            clear_statement2.bind_int64(2, bucket_id)?;
            clear_statement2.bind_int(1, checksum.bitcast_i32())?;
            clear_statement2.exec()?;

            add_checksum = Checksum::zero();
            is_empty = true;
            op_checksum = Checksum::zero();
        }
    }

    if let Some(last_op) = &last_op {
        // language=SQLite
        let statement = db.prepare_v2(
            "UPDATE ps_buckets
                SET last_op = ?2,
                    add_checksum = (add_checksum + ?3) & 0xffffffff,
                    op_checksum = (op_checksum + ?4) & 0xffffffff,
                    count_since_last = count_since_last + ?5
            WHERE id = ?1",
        )?;
        statement.bind_int64(1, bucket_id)?;
        statement.bind_int64(2, *last_op)?;
        statement.bind_int(3, add_checksum.bitcast_i32())?;
        statement.bind_int(4, op_checksum.bitcast_i32())?;
        statement.bind_int(5, added_ops)?;

        statement.exec()?;
    }

    Ok(())
}
