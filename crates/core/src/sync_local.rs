use alloc::collections::BTreeSet;
use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use serde::Deserialize;

use crate::bucket_priority::BucketPriority;
use crate::error::{PSResult, SQLiteError};
use sqlite_nostd::{self as sqlite, Value};
use sqlite_nostd::{ColumnType, Connection, ResultCode};

use crate::ext::SafeManagedStmt;
use crate::util::{internal_table_name, quote_internal_name};

fn can_apply_sync_changes(
    db: *mut sqlite::sqlite3,
    priority: BucketPriority,
) -> Result<bool, SQLiteError> {
    // Don't publish downloaded data until the upload queue is empty (except for downloaded data in
    // priority 0, which is published earlier).
    if !priority.may_publish_with_outstanding_uploads() {
        // language=SQLite
        let statement = db.prepare_v2(
            "\
SELECT group_concat(name)
FROM ps_buckets
WHERE target_op > last_op AND name = '$local'",
        )?;

        if statement.step()? != ResultCode::ROW {
            return Err(SQLiteError::from(ResultCode::ABORT));
        }

        if statement.column_type(0)? == ColumnType::Text {
            return Ok(false);
        }

        let statement = db.prepare_v2("SELECT 1 FROM ps_crud LIMIT 1")?;
        if statement.step()? != ResultCode::DONE {
            return Ok(false);
        }
    }

    Ok(true)
}

pub fn sync_local(db: *mut sqlite::sqlite3, data: *mut sqlite::value) -> Result<i64, SQLiteError> {
    #[derive(Deserialize)]
    struct SyncLocalArguments {
        #[serde(rename = "buckets")]
        _buckets: Vec<String>,
        priority: Option<BucketPriority>,
    }

    const FALLBACK_PRIORITY: BucketPriority = BucketPriority::LOWEST;
    let (has_args, priority) = match data.value_type() {
        ColumnType::Text => {
            let text = data.text();
            if text.len() > 0 {
                let args: SyncLocalArguments = serde_json::from_str(text)?;
                (true, args.priority.unwrap_or(FALLBACK_PRIORITY))
            } else {
                (false, FALLBACK_PRIORITY)
            }
        }
        _ => (false, FALLBACK_PRIORITY),
    };

    if !can_apply_sync_changes(db, priority)? {
        return Ok(0);
    }

    // language=SQLite
    let statement = db
        .prepare_v2("SELECT name FROM sqlite_master WHERE type='table' AND name GLOB 'ps_data_*'")
        .into_db_result(db)?;
    let mut tables: BTreeSet<String> = BTreeSet::new();

    while statement.step()? == ResultCode::ROW {
        let name = statement.column_text(0)?;
        tables.insert(String::from(name));
    }

    // Query for updated objects

    // language=SQLite
    let statement = db
        .prepare_v2(
            "\
-- 1. Filter oplog by the ops added but not applied yet (oplog b).
--    SELECT DISTINCT / UNION is important for cases with many duplicate ids.
WITH 
  involved_buckets (id) AS (
    SELECT id FROM ps_buckets WHERE ?1 IS NULL
      OR name IN (SELECT value FROM json_each(json_extract(?1, '$.buckets')))
  ),
  updated_rows AS (
    SELECT DISTINCT FALSE as local, b.row_type, b.row_id FROM ps_buckets AS buckets
      CROSS JOIN ps_oplog AS b ON b.bucket = buckets.id AND (b.op_id > buckets.last_applied_op)
      WHERE buckets.id IN (SELECT id FROM involved_buckets)
    UNION SELECT TRUE, row_type, row_id FROM ps_updated_rows
  )

-- 3. Group the objects from different buckets together into a single one (ops).
SELECT b.row_type as type,
    b.row_id as id,
    b.local as local,
    r.data as data,
    count(r.bucket) as buckets,
    /* max() affects which row is used for 'data' */
    max(r.op_id) as op_id
-- 2. Find *all* current ops over different buckets for those objects (oplog r).
FROM updated_rows b
    LEFT OUTER JOIN ps_oplog AS r
               ON r.row_type = b.row_type
                 AND r.row_id = b.row_id
                 AND r.bucket IN (SELECT id FROM involved_buckets)
-- Group for (3)
GROUP BY b.row_type, b.row_id",
        )
        .into_db_result(db)?;

    if has_args {
        statement.bind_value(1, data)?;
    } else {
        statement.bind_null(1)?;
    }

    // TODO: cache statements
    while statement.step().into_db_result(db)? == ResultCode::ROW {
        let type_name = statement.column_text(0)?;
        let id = statement.column_text(1)?;
        let local = statement.column_int(2)? == 1;
        let buckets = statement.column_int(4)?;
        let data = statement.column_text(3);

        let table_name = internal_table_name(type_name);

        if local && buckets == 0 && priority == BucketPriority::HIGHEST {
            // These rows are still local and they haven't been uploaded yet (which we allow for
            // buckets with priority=0 completing). We should just keep them around.
            continue;
        }

        if tables.contains(&table_name) {
            let quoted = quote_internal_name(type_name, false);

            if buckets == 0 {
                // DELETE
                let delete_statement = db
                    .prepare_v2(&format!("DELETE FROM {} WHERE id = ?", quoted))
                    .into_db_result(db)?;
                delete_statement.bind_text(1, id, sqlite::Destructor::STATIC)?;
                delete_statement.exec()?;
            } else {
                // INSERT/UPDATE
                let insert_statement = db
                    .prepare_v2(&format!("REPLACE INTO {}(id, data) VALUES(?, ?)", quoted))
                    .into_db_result(db)?;
                insert_statement.bind_text(1, id, sqlite::Destructor::STATIC)?;
                insert_statement.bind_text(2, data?, sqlite::Destructor::STATIC)?;
                insert_statement.exec()?;
            }
        } else {
            if buckets == 0 {
                // DELETE
                // language=SQLite
                let delete_statement = db
                    .prepare_v2("DELETE FROM ps_untyped WHERE type = ? AND id = ?")
                    .into_db_result(db)?;
                delete_statement.bind_text(1, type_name, sqlite::Destructor::STATIC)?;
                delete_statement.bind_text(2, id, sqlite::Destructor::STATIC)?;
                delete_statement.exec()?;
            } else {
                // INSERT/UPDATE
                // language=SQLite
                let insert_statement = db
                    .prepare_v2("REPLACE INTO ps_untyped(type, id, data) VALUES(?, ?, ?)")
                    .into_db_result(db)?;
                insert_statement.bind_text(1, type_name, sqlite::Destructor::STATIC)?;
                insert_statement.bind_text(2, id, sqlite::Destructor::STATIC)?;
                insert_statement.bind_text(3, data?, sqlite::Destructor::STATIC)?;
                insert_statement.exec()?;
            }
        }
    }

    // language=SQLite
    let updated = db
        .prepare_v2(
            "UPDATE ps_buckets
                 SET last_applied_op = last_op
                 WHERE last_applied_op != last_op AND
                    (?1 IS NULL OR name IN (SELECT value FROM json_each(json_extract(?1, '$.buckets'))))",
        )
        .into_db_result(db)?;
    if has_args {
        updated.bind_value(1, data)?;
    } else {
        updated.bind_null(1)?;
    }
    updated.exec()?;

    if priority == BucketPriority::LOWEST {
        // language=SQLite
        db.exec_safe("DELETE FROM ps_updated_rows")
            .into_db_result(db)?;

        // language=SQLite
        db.exec_safe(
            "insert or replace into ps_kv(key, value) values('last_synced_at', datetime())",
        )
        .into_db_result(db)?;
    }

    Ok(1)
}
