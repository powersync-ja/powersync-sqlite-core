use alloc::format;
use alloc::string::String;

use crate::bucket_priority::BucketPriority;
use crate::error::{PSResult, SQLiteError};
use sqlite_nostd as sqlite;
use sqlite_nostd::{Connection, ResultCode};

use crate::ext::SafeManagedStmt;

// Run inside a transaction
pub fn insert_operation(db: *mut sqlite::sqlite3, data: &str) -> Result<(), SQLiteError> {
    // language=SQLite
    let statement = db.prepare_v2(
        "\
SELECT
    json_extract(e.value, '$.bucket') as bucket,
    json_extract(e.value, '$.data') as data,
    json_extract(e.value, '$.has_more') as has_more,
    json_extract(e.value, '$.after') as after,
    json_extract(e.value, '$.next_after') as next_after,
    json_extract(d.value, '$.priority') as priority
FROM json_each(json_extract(?1, '$.buckets')) e
    LEFT OUTER JOIN json_each(json_extract(?1, '$.descriptions')) d
        ON json_extract(e.value, '$.bucket') == d.key",
    )?;
    statement.bind_text(1, data, sqlite::Destructor::STATIC)?;

    while statement.step()? == ResultCode::ROW {
        let bucket = statement.column_text(0)?;
        let data = statement.column_text(1)?;
        // let _has_more = statement.column_int(2)? != 0;
        // let _after = statement.column_text(3)?;
        // let _next_after = statement.column_text(4)?;
        let priority = match statement.column_type(5)? {
            sqlite_nostd::ColumnType::Integer => {
                BucketPriority::try_from(statement.column_int(5)?).ok()
            }
            _ => None,
        }
        .unwrap_or_default();

        insert_bucket_operations(db, bucket, data, priority)?;
    }

    Ok(())
}

pub fn insert_bucket_operations(
    db: *mut sqlite::sqlite3,
    bucket: &str,
    data: &str,
    priority: BucketPriority,
) -> Result<(), SQLiteError> {
    // Statement to insert new operations (only for PUT and REMOVE).
    // language=SQLite
    let iterate_statement = db.prepare_v2(
        "\
SELECT
    json_extract(e.value, '$.op_id') as op_id,
    json_extract(e.value, '$.op') as op,
    json_extract(e.value, '$.object_type') as object_type,
    json_extract(e.value, '$.object_id') as object_id,
    json_extract(e.value, '$.checksum') as checksum,
    json_extract(e.value, '$.data') as data,
    json_extract(e.value, '$.subkey') as subkey
FROM json_each(?) e",
    )?;
    iterate_statement.bind_text(1, data, sqlite::Destructor::STATIC)?;

    // We do an ON CONFLICT UPDATE simply so that the RETURNING bit works for existing rows.
    // We can consider splitting this into separate SELECT and INSERT statements.
    // language=SQLite
    let bucket_statement = db.prepare_v2(
        "INSERT INTO ps_buckets(name, priority)
                            VALUES(?, ?)
                        ON CONFLICT DO UPDATE
                            SET last_applied_op = last_applied_op
                        RETURNING id, last_applied_op",
    )?;
    bucket_statement.bind_text(1, bucket, sqlite::Destructor::STATIC)?;
    bucket_statement.bind_int(2, priority.into())?;
    bucket_statement.step()?;

    let bucket_id = bucket_statement.column_int64(0)?;

    // This is an optimization for initial sync - we can avoid persisting individual REMOVE
    // operations when last_applied_op = 0.
    // We do still need to do the "supersede_statement" step for this case, since a REMOVE
    // operation can supersede another PUT operation we're syncing at the same time.
    let mut is_empty = bucket_statement.column_int64(1)? == 0;

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

    bucket_statement.reset()?;

    let mut last_op: Option<i64> = None;
    let mut add_checksum: i32 = 0;
    let mut op_checksum: i32 = 0;

    while iterate_statement.step()? == ResultCode::ROW {
        let op_id = iterate_statement.column_int64(0)?;
        let op = iterate_statement.column_text(1)?;
        let object_type = iterate_statement.column_text(2);
        let object_id = iterate_statement.column_text(3);
        let checksum = iterate_statement.column_int(4)?;
        let op_data = iterate_statement.column_text(5);

        last_op = Some(op_id);

        if op == "PUT" || op == "REMOVE" {
            let key: String;
            if let (Ok(object_type), Ok(object_id)) = (object_type.as_ref(), object_id.as_ref()) {
                let subkey = iterate_statement.column_text(6).unwrap_or("null");
                key = format!("{}/{}/{}", &object_type, &object_id, subkey);
            } else {
                key = String::from("");
            }

            supersede_statement.bind_text(2, &key, sqlite::Destructor::STATIC)?;

            let mut superseded = false;

            while supersede_statement.step()? == ResultCode::ROW {
                // Superseded (deleted) a previous operation, add the checksum
                let supersede_checksum = supersede_statement.column_int(1)?;
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

            if op == "REMOVE" {
                let should_skip_remove = !superseded;

                add_checksum = add_checksum.wrapping_add(checksum);

                if !should_skip_remove {
                    if let (Ok(object_type), Ok(object_id)) = (object_type, object_id) {
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
            if key != "" {
                insert_statement.bind_text(3, &key, sqlite::Destructor::STATIC)?;
            } else {
                insert_statement.bind_null(3)?;
            }

            if let (Ok(object_type), Ok(object_id)) = (object_type, object_id) {
                insert_statement.bind_text(4, object_type, sqlite::Destructor::STATIC)?;
                insert_statement.bind_text(5, object_id, sqlite::Destructor::STATIC)?;
            } else {
                insert_statement.bind_null(4)?;
                insert_statement.bind_null(5)?;
            }
            if let Ok(data) = op_data {
                insert_statement.bind_text(6, data, sqlite::Destructor::STATIC)?;
            } else {
                insert_statement.bind_null(6)?;
            }

            insert_statement.bind_int(7, checksum)?;
            insert_statement.exec()?;

            op_checksum = op_checksum.wrapping_add(checksum);
        } else if op == "MOVE" {
            add_checksum = add_checksum.wrapping_add(checksum);
        } else if op == "CLEAR" {
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

    if let Some(last_op) = &last_op {
        // language=SQLite
        let statement = db.prepare_v2(
            "UPDATE ps_buckets
                SET last_op = ?2,
                    add_checksum = (add_checksum + ?3) & 0xffffffff,
                    op_checksum = (op_checksum + ?4) & 0xffffffff
            WHERE id = ?1",
        )?;
        statement.bind_int64(1, bucket_id)?;
        statement.bind_int64(2, *last_op)?;
        statement.bind_int(3, add_checksum)?;
        statement.bind_int(4, op_checksum)?;

        statement.exec()?;
    }

    Ok(())
}

pub fn clear_remove_ops(_db: *mut sqlite::sqlite3, _data: &str) -> Result<(), SQLiteError> {
    // No-op

    Ok(())
}

pub fn delete_pending_buckets(_db: *mut sqlite::sqlite3, _data: &str) -> Result<(), SQLiteError> {
    // No-op

    Ok(())
}

pub fn delete_bucket(db: *mut sqlite::sqlite3, name: &str) -> Result<(), SQLiteError> {
    // language=SQLite
    let statement = db.prepare_v2("DELETE FROM ps_buckets WHERE name = ?1 RETURNING id")?;
    statement.bind_text(1, name, sqlite::Destructor::STATIC)?;

    if statement.step()? == ResultCode::ROW {
        let bucket_id = statement.column_int64(0)?;

        // language=SQLite
        let updated_statement = db.prepare_v2(
            "\
INSERT OR IGNORE INTO ps_updated_rows(row_type, row_id)
SELECT row_type, row_id
FROM ps_oplog
WHERE bucket = ?1",
        )?;
        updated_statement.bind_int64(1, bucket_id)?;
        updated_statement.exec()?;

        // language=SQLite
        let delete_statement = db.prepare_v2("DELETE FROM ps_oplog WHERE bucket=?1")?;
        delete_statement.bind_int64(1, bucket_id)?;
        delete_statement.exec()?;
    }

    Ok(())
}
