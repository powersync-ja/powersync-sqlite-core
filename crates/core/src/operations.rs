use alloc::format;
use alloc::string::{String, ToString};

use crate::error::{PSResult, SQLiteError};
use sqlite_nostd as sqlite;
use sqlite_nostd::{Connection, ResultCode};

use crate::ext::SafeManagedStmt;
use crate::util::*;

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
    json_extract(e.value, '$.next_after') as next_after
FROM json_each(json_extract(?, '$.buckets')) e",
    )?;
    statement.bind_text(1, data, sqlite::Destructor::STATIC)?;

    while statement.step()? == ResultCode::ROW {
        let bucket = statement.column_text(0)?;
        let data = statement.column_text(1)?;
        // let _has_more = statement.column_int(2)? != 0;
        // let _after = statement.column_text(3)?;
        // let _next_after = statement.column_text(4)?;

        insert_bucket_operations(db, bucket, data)?;
    }

    Ok(())
}

pub fn insert_bucket_operations(
    db: *mut sqlite::sqlite3,
    bucket: &str,
    data: &str,
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

    // Statement to supersede (replace) operations with the same key.
    // language=SQLite
    let supersede_statement = db.prepare_v2(
        "\
DELETE FROM ps_oplog
    WHERE ps_oplog.superseded = 0
    AND unlikely(ps_oplog.bucket = ?1)
    AND ps_oplog.key = ?2
RETURNING op_id, hash",
    )?;
    supersede_statement.bind_text(1, bucket, sqlite::Destructor::STATIC)?;

    // language=SQLite
    let insert_statement = db.prepare_v2("\
INSERT INTO ps_oplog(bucket, op_id, op, key, row_type, row_id, data, hash, superseded) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)")?;
    insert_statement.bind_text(1, bucket, sqlite::Destructor::STATIC)?;

    // We do an ON CONFLICT UPDATE simply so that the RETURNING bit works for existing rows.
    // We can consider splitting this into separate SELECT and INSERT statements.
    // language=SQLite
    let bucket_statement = db.prepare_v2(
        "INSERT INTO ps_buckets(name)
                            VALUES(?)
                        ON CONFLICT DO UPDATE
                            SET last_applied_op = last_applied_op
                        RETURNING last_applied_op",
    )?;
    bucket_statement.bind_text(1, bucket, sqlite::Destructor::STATIC)?;
    bucket_statement.step()?;

    // This is an optimization for initial sync - we can avoid persisting individual REMOVE
    // operations when last_applied_op = 0.
    // We do still need to do the "supersede_statement" step for this case, since a REMOVE
    // operation can supersede another PUT operation we're syncing at the same time.
    let mut last_applied_op = bucket_statement.column_int64(0)?;

    bucket_statement.reset()?;

    let mut last_op: Option<i64> = None;
    let mut add_checksum: i32 = 0;
    let mut op_checksum: i32 = 0;
    let mut pending_compact: i32 = 0;

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
                let superseded_op = supersede_statement.column_int64(0)?;
                let supersede_checksum = supersede_statement.column_int(1)?;
                add_checksum = add_checksum.wrapping_add(supersede_checksum);
                op_checksum = op_checksum.wrapping_sub(supersede_checksum);

                if superseded_op <= last_applied_op {
                    // Superseded an operation previously applied - we cannot skip removes
                    // For initial sync, last_applied_op = 0, so this is always false.
                    // For subsequent sync, this is only true if the row was previously
                    // synced, not when it was first synced in the current batch.
                    superseded = true;
                }
            }
            supersede_statement.reset()?;

            let should_skip_remove = !superseded && op == "REMOVE";
            if should_skip_remove {
                // If a REMOVE statement did not replace (supersede) any previous
                // operations, we do not need to persist it.
                // The same applies if the bucket was not synced to the local db yet,
                // even if it did supersede another operation.
                // Handle the same as MOVE.
                add_checksum = add_checksum.wrapping_add(checksum);
                continue;
            }

            let opi = if op == "PUT" { 3 } else { 4 };
            insert_statement.bind_int64(2, op_id)?;
            insert_statement.bind_int(3, opi)?;
            if key != "" {
                insert_statement.bind_text(4, &key, sqlite::Destructor::STATIC)?;
            } else {
                insert_statement.bind_null(4)?;
            }

            if let (Ok(object_type), Ok(object_id)) = (object_type, object_id) {
                insert_statement.bind_text(5, object_type, sqlite::Destructor::STATIC)?;
                insert_statement.bind_text(6, object_id, sqlite::Destructor::STATIC)?;
            } else {
                insert_statement.bind_null(5)?;
                insert_statement.bind_null(6)?;
            }
            if let Ok(data) = op_data {
                insert_statement.bind_text(7, data, sqlite::Destructor::STATIC)?;
            } else {
                insert_statement.bind_null(7)?;
            }

            insert_statement.bind_int(8, checksum)?;
            insert_statement.exec()?;

            op_checksum = op_checksum.wrapping_add(checksum);

            if opi == 4 {
                // We persisted a REMOVE statement, so the bucket needs
                // to be compacted at some point.
                pending_compact = 1;
            }
        } else if op == "MOVE" {
            add_checksum = add_checksum.wrapping_add(checksum);
        } else if op == "CLEAR" {
            // Any remaining PUT operations should get an implicit REMOVE
            // language=SQLite
            let clear_statement = db.prepare_v2("UPDATE ps_oplog SET op=4, data=NULL, hash=0 WHERE (op=3 OR op=4) AND bucket=?1").into_db_result(db)?;
            clear_statement.bind_text(1, bucket, sqlite::Destructor::STATIC)?;
            clear_statement.exec()?;

            // And we need to re-apply all of those.
            // We also replace the checksum with the checksum of the CLEAR op.
            // language=SQLite
            let clear_statement2 = db.prepare_v2(
                "UPDATE ps_buckets SET last_applied_op = 0, add_checksum = ?1, op_checksum = 0 WHERE name = ?2",
            )?;
            clear_statement2.bind_text(2, bucket, sqlite::Destructor::STATIC)?;
            clear_statement2.bind_int(1, checksum)?;
            clear_statement2.exec()?;

            add_checksum = 0;
            last_applied_op = 0;
            op_checksum = 0;
        }
    }

    if let Some(last_op) = &last_op {
        // language=SQLite
        let statement = db.prepare_v2(
            "UPDATE ps_buckets
                SET last_op = ?2,
                    add_checksum = (add_checksum + ?3) & 0xffffffff,
                    op_checksum = (op_checksum + ?4) & 0xffffffff,
                    pending_compact = (pending_compact OR ?5)
            WHERE name = ?1",
        )?;
        statement.bind_text(1, bucket, sqlite::Destructor::STATIC)?;
        statement.bind_int64(2, *last_op)?;
        statement.bind_int(3, add_checksum)?;
        statement.bind_int(4, op_checksum)?;
        statement.bind_int(5, pending_compact)?;

        statement.exec()?;
    }

    Ok(())
}

pub fn clear_remove_ops(db: *mut sqlite::sqlite3, _data: &str) -> Result<(), SQLiteError> {
    // language=SQLite
    let statement = db.prepare_v2(
        "
SELECT
    name,
    last_applied_op,
    (SELECT IFNULL(SUM(oplog.hash), 0)
    FROM ps_oplog oplog
    WHERE oplog.bucket = ps_buckets.name
      AND oplog.op_id <= ps_buckets.last_applied_op
      AND (oplog.superseded = 1 OR oplog.op != 3)
    ) as checksum
FROM ps_buckets
WHERE ps_buckets.pending_delete = 0 AND ps_buckets.pending_compact = 1",
    )?;

    // language=SQLite
    let update_statement = db.prepare_v2(
        "
        UPDATE ps_buckets
           SET add_checksum = (add_checksum + ?2) & 0xffffffff,
               op_checksum = (op_checksum - ?2) & 0xffffffff
           WHERE ps_buckets.name = ?1",
    )?;

    // language=SQLite
    let delete_statement = db.prepare_v2(
        "DELETE
           FROM ps_oplog
           WHERE (superseded = 1 OR op != 3)
             AND bucket = ?1
             AND op_id <= ?2",
    )?;

    while statement.step()? == ResultCode::ROW {
        // Note: Each iteration here may be run in a separate transaction.
        let name = statement.column_text(0)?;
        let last_applied_op = statement.column_int64(1)?;
        let checksum = statement.column_int(2)?;

        update_statement.bind_text(1, name, sqlite::Destructor::STATIC)?;
        update_statement.bind_int(2, checksum)?;
        update_statement.exec()?;

        // Must use the same values as above
        delete_statement.bind_text(1, name, sqlite::Destructor::STATIC)?;
        delete_statement.bind_int64(2, last_applied_op)?;
        delete_statement.exec()?;
    }

    Ok(())
}

pub fn delete_pending_buckets(db: *mut sqlite::sqlite3, _data: &str) -> Result<(), SQLiteError> {
    // language=SQLite
    let statement = db.prepare_v2(
        "DELETE FROM ps_oplog WHERE bucket IN (SELECT name FROM ps_buckets WHERE pending_delete = 1 AND last_applied_op = last_op AND last_op >= target_op)")?;
    statement.exec()?;

    // language=SQLite
    let statement = db.prepare_v2("DELETE FROM ps_buckets WHERE pending_delete = 1 AND last_applied_op = last_op AND last_op >= target_op")?;
    statement.exec()?;

    Ok(())
}

pub fn delete_bucket(db: *mut sqlite::sqlite3, name: &str) -> Result<(), SQLiteError> {
    let id = gen_uuid();
    let new_name = format!("$delete_{}_{}", name, id.hyphenated().to_string());

    // language=SQLite
    let statement = db.prepare_v2(
        "UPDATE ps_oplog SET op=4, data=NULL, bucket=?1 WHERE op=3 AND superseded=0 AND bucket=?2",
    )?;
    statement.bind_text(1, &new_name, sqlite::Destructor::STATIC)?;
    statement.bind_text(2, &name, sqlite::Destructor::STATIC)?;
    statement.exec()?;

    // Rename bucket
    // language=SQLite
    let statement = db.prepare_v2("UPDATE ps_oplog SET bucket=?1 WHERE bucket=?2")?;
    statement.bind_text(1, &new_name, sqlite::Destructor::STATIC)?;
    statement.bind_text(2, name, sqlite::Destructor::STATIC)?;
    statement.exec()?;

    // language=SQLite
    let statement = db.prepare_v2("DELETE FROM ps_buckets WHERE name = ?1")?;
    statement.bind_text(1, name, sqlite::Destructor::STATIC)?;
    statement.exec()?;

    // language=SQLite
    let statement = db.prepare_v2(
        "INSERT INTO ps_buckets(name, pending_delete, last_op) SELECT ?1, 1, IFNULL(MAX(op_id), 0) FROM ps_oplog WHERE bucket = ?1")?;
    statement.bind_text(1, &new_name, sqlite::Destructor::STATIC)?;
    statement.exec()?;

    Ok(())
}
