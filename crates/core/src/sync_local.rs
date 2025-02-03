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

    // Be careful with modifying this query - it is critical for performance. When modifying, make sure to check
    // performance of the query with a large number of rows, and also with a large number of duplicate rows (same row_id).
    //
    // This form uses a subquery with max(r.op_id) instead of a JOIN to get the latest oplog entry for each updated row.
    // The subquery is because:
    // 1. We need the GROUP BY to execute _before_ looking up the latest op_id for each row, otherwise
    //    we get terrible performance if there are lots of duplicate ids (O(N^2) performance).
    // 2. We want to avoid using a second GROUP BY, which would use a secondary TEMP B-TREE.
    //
    // It does not appear to be feasible to avoid the single TEMP B-TREE here.
    //
    // The query roughly does the following:
    // 1. Filter oplog by the ops added but not applied yet (oplog b). These are not unique.
    // 2. Use GROUP BY to get unique rows. This adds some overhead because of the TEMP B-TREE, but is necessary
    //    to cover cases of duplicate rows. DISTINCT would do the same in theory, but is slower than GROUP BY in practice.
    // 3. For each op, find the latest version of the data. This is done using a subquery, with `max(r.op_id)`` to
    //    select the latest version.
    //
    // The subquery instead of a JOIN is because:
    // 1. We need the GROUP BY to execute _before_ looking up the latest op_id for each row, otherwise
    //    we get terrible performance if there are lots of duplicate ids (O(N^2) performance).
    // 2. We want to avoid using a second GROUP BY, which would use a second TEMP B-TREE.
    //
    // The `ifnull(data, max(op_id))` clause is a hack to pick the row with the largest op_id, but only select the data.
    //
    // QUERY PLAN
    // |--CO-ROUTINE updated_rows
    // |  `--COMPOUND QUERY
    // |     |--LEFT-MOST SUBQUERY
    // |     |  |--SCAN buckets USING COVERING INDEX ps_buckets_name
    // |     |  `--SEARCH b USING INDEX ps_oplog_opid (bucket=? AND op_id>?)
    // |     `--UNION ALL
    // |        `--SCAN ps_updated_rows
    // |--SCAN b
    // |--USE TEMP B-TREE FOR GROUP BY
    // `--CORRELATED SCALAR SUBQUERY 3
    //    `--SEARCH r USING INDEX ps_oplog_row (row_type=? AND row_id=?)

    // language=SQLite
    let statement = db
        .prepare_v2(
            "\
WITH updated_rows AS (
    SELECT b.row_type, b.row_id FROM ps_buckets AS buckets
        CROSS JOIN ps_oplog AS b ON b.bucket = buckets.id
        AND (b.op_id > buckets.last_applied_op)
    UNION ALL SELECT row_type, row_id FROM ps_updated_rows
)

SELECT
    b.row_type,
    b.row_id,
    (
        SELECT ifnull(r.data, max(r.op_id))
                 FROM ps_oplog r
                WHERE r.row_type = b.row_type
                  AND r.row_id = b.row_id
    ) as data
    FROM updated_rows b;
    GROUP BY b.row_type, b.row_id;
    ",
        )
        .into_db_result(db)?;

    // An alternative form of the query is this:
    //
    // SELECT r.row_type as type,
    //     r.row_id as id,
    //     r.data as data,
    //     max(r.op_id) as op_id
    // FROM ps_oplog r
    // GROUP BY r.row_type, r.row_id;
    //
    // This form is simple and fast, but does not filter only on updated rows. It also ignores ps_updated_rows.
    // We could later add heuristics to use this form on initial sync, or when a large number of rows have been re-synced.
    //
    // QUERY PLAN
    // `--SCAN r USING INDEX ps_oplog_row

    // TODO: cache individual statements

    while statement.step().into_db_result(db)? == ResultCode::ROW {
        let type_name = statement.column_text(0)?;
        let id = statement.column_text(1)?;
        let data = statement.column_text(2);

        let table_name = internal_table_name(type_name);

        if tables.contains(&table_name) {
            let quoted = quote_internal_name(type_name, false);

            // is_err() is essentially a NULL check here
            if data.is_err() {
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
            // is_err() is essentially a NULL check here
            if data.is_err() {
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
