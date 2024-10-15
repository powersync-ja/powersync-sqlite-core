use alloc::collections::BTreeSet;
use alloc::format;
use alloc::string::String;

use crate::error::{PSResult, SQLiteError};
use sqlite_nostd as sqlite;
use sqlite_nostd::{ColumnType, Connection, ResultCode};

use crate::ext::SafeManagedStmt;
use crate::util::{internal_table_name, quote_internal_name};

pub fn can_update_local(db: *mut sqlite::sqlite3) -> Result<bool, SQLiteError> {
    // language=SQLite
    let statement = db.prepare_v2(
        "\
SELECT group_concat(name)
FROM ps_buckets
WHERE target_op > last_op",
    )?;

    if statement.step()? != ResultCode::ROW {
        return Err(SQLiteError::from(ResultCode::ABORT));
    }

    if statement.column_type(0)? == ColumnType::Text {
        return Ok(false);
    }

    // This is specifically relevant for when data is added to crud before another batch is completed.

    // language=SQLite
    let statement = db.prepare_v2("SELECT 1 FROM ps_crud LIMIT 1")?;
    if statement.step()? != ResultCode::DONE {
        return Ok(false);
    }

    Ok(true)
}

pub fn sync_local(db: *mut sqlite::sqlite3, _data: &str) -> Result<i64, SQLiteError> {
    if !can_update_local(db)? {
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
WITH updated_rows AS (
  SELECT DISTINCT b.row_type, b.row_id FROM ps_buckets AS buckets
    CROSS JOIN ps_oplog AS b ON b.bucket = buckets.id
  AND (b.op_id > buckets.last_applied_op)
  UNION SELECT row_type, row_id FROM ps_updated_rows
)

-- 3. Group the objects from different buckets together into a single one (ops).
SELECT b.row_type as type,
    b.row_id as id,
    r.data as data,
    count(r.bucket) as buckets,
    /* max() affects which row is used for 'data' */
    max(r.op_id) as op_id
-- 2. Find *all* current ops over different buckets for those objects (oplog r).
FROM updated_rows b
    LEFT OUTER JOIN ps_oplog AS r
               ON r.row_type = b.row_type
                 AND r.row_id = b.row_id
-- Group for (3)
GROUP BY b.row_type, b.row_id",
        )
        .into_db_result(db)?;

    // TODO: cache statements

    while statement.step().into_db_result(db)? == ResultCode::ROW {
        let type_name = statement.column_text(0)?;
        let id = statement.column_text(1)?;
        let buckets = statement.column_int(3)?;
        let data = statement.column_text(2);

        let table_name = internal_table_name(type_name);

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
    db.exec_safe(
        "UPDATE ps_buckets
                 SET last_applied_op = last_op
                 WHERE last_applied_op != last_op",
    )
    .into_db_result(db)?;

    // language=SQLite
    db.exec_safe("DELETE FROM ps_updated_rows")
        .into_db_result(db)?;

    // language=SQLite
    db.exec_safe("insert or replace into ps_kv(key, value) values('last_synced_at', datetime())")
        .into_db_result(db)?;

    Ok(1)
}
