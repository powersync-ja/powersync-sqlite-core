use alloc::{borrow::Cow, format};
use sqlite_nostd::Connection;
use sqlite_nostd::{self as sqlite, ResultCode};

use crate::{
    error::{PSResult, SQLiteError},
    ext::SafeManagedStmt,
};

use super::line::OplogData;
use super::{
    line::{DataLine, OpType},
    storage_adapter::{BucketInfo, StorageAdapter},
};

pub fn insert_bucket_operations(
    adapter: &StorageAdapter,
    line: &DataLine,
) -> Result<(), SQLiteError> {
    let db = adapter.db;
    let BucketInfo {
        id: bucket_id,
        last_applied_op,
    } = adapter.lookup_bucket(line.bucket)?;

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
    AND ps_oplog.key = ?2
RETURNING op_id, hash",
    )?;
    supersede_statement.bind_int64(1, bucket_id)?;

    // language=SQLite
    let insert_statement = db.prepare_v2("\
INSERT INTO ps_oplog(bucket, op_id, key, row_type, row_id, data, hash) VALUES (?, ?, ?, ?, ?, ?, ?)")?;
    insert_statement.bind_int64(1, bucket_id)?;

    let updated_row_statement = db.prepare_v2(
        "\
INSERT OR IGNORE INTO ps_updated_rows(row_type, row_id) VALUES(?1, ?2)",
    )?;

    let mut last_op: Option<i64> = None;
    let mut add_checksum: i32 = 0;
    let mut op_checksum: i32 = 0;
    let mut added_ops: i32 = 0;

    for data in &line.data {
        last_op = Some(data.op_id);
        added_ops += 1;
        let checksum = data.checksum;

        match data.op {
            OpType::PUT | OpType::REMOVE => {
                let key: Cow<'static, str> = if let (Some(object_type), Some(object_id)) =
                    (data.object_type, data.object_id)
                {
                    let subkey = data.subkey.unwrap_or("null");
                    Cow::Owned(format!("{}/{}/{}", &object_type, &object_id, subkey))
                } else {
                    Cow::Borrowed("")
                };

                supersede_statement.bind_text(2, key.as_ref(), sqlite::Destructor::STATIC)?;

                let mut superseded = false;

                while supersede_statement.step()? == ResultCode::ROW {
                    // Superseded (deleted) a previous operation, add the checksum
                    let supersede_checksum = supersede_statement.column_int(1);
                    add_checksum = add_checksum.wrapping_add(supersede_checksum);
                    op_checksum = op_checksum.wrapping_sub(supersede_checksum);

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

                if data.op == OpType::REMOVE {
                    let should_skip_remove = !superseded;

                    add_checksum = add_checksum.wrapping_add(checksum);

                    if !should_skip_remove {
                        if let (Some(object_type), Some(object_id)) =
                            (data.object_type, data.object_id)
                        {
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

                insert_statement.bind_int64(2, data.op_id)?;
                if key != "" {
                    insert_statement.bind_text(3, &key, sqlite::Destructor::STATIC)?;
                } else {
                    insert_statement.bind_null(3)?;
                }

                if let (Some(object_type), Some(object_id)) = (data.object_type, data.object_id) {
                    insert_statement.bind_text(4, object_type, sqlite::Destructor::STATIC)?;
                    insert_statement.bind_text(5, object_id, sqlite::Destructor::STATIC)?;
                } else {
                    insert_statement.bind_null(4)?;
                    insert_statement.bind_null(5)?;
                }

                match data.data {
                    Some(OplogData::JsonString { ref data }) => {
                        insert_statement.bind_text(6, data.as_ref(), sqlite::Destructor::STATIC)?
                    }
                    Some(OplogData::BsonDocument { ref data }) => {
                        let data = data.as_ref();
                        insert_statement.bind_blob(6, data, sqlite::Destructor::STATIC)?
                    }
                    None => insert_statement.bind_null(6)?,
                };

                insert_statement.bind_int(7, checksum)?;
                insert_statement.exec()?;

                op_checksum = op_checksum.wrapping_add(checksum);
            }
            OpType::MOVE => {
                add_checksum = add_checksum.wrapping_add(checksum);
            }
            OpType::CLEAR => {
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
                clear_statement2.bind_int(1, checksum)?;
                clear_statement2.exec()?;

                add_checksum = 0;
                is_empty = true;
                op_checksum = 0;
            }
        }
    }

    if let Some(last_op) = last_op {
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
        statement.bind_int64(2, last_op)?;
        statement.bind_int(3, add_checksum)?;
        statement.bind_int(4, op_checksum)?;
        statement.bind_int(5, added_ops)?;

        statement.exec()?;
    }

    Ok(())
}
