use alloc::collections::BTreeSet;
use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use serde::Deserialize;

use crate::bucket_priority::BucketPriority;
use crate::error::{PSResult, SQLiteError};
use sqlite_nostd::{self as sqlite, Destructor, ManagedStmt, Value};
use sqlite_nostd::{ColumnType, Connection, ResultCode};

use crate::ext::SafeManagedStmt;
use crate::util::{internal_table_name, quote_internal_name};

pub fn sync_local<V: Value>(db: *mut sqlite::sqlite3, data: &V) -> Result<i64, SQLiteError> {
    let mut operation = SyncOperation::new(db, data)?;
    operation.apply()
}

struct PartialSyncOperation<'a> {
    /// The lowest priority part of the partial sync operation.
    priority: BucketPriority,
    /// The JSON-encoded arguments passed by the client SDK. This includes the priority and a list
    /// of bucket names in that (and higher) priorities.
    args: &'a str,
}

struct SyncOperation<'a> {
    db: *mut sqlite::sqlite3,
    data_tables: BTreeSet<String>,
    partial: Option<PartialSyncOperation<'a>>,
}

impl<'a> SyncOperation<'a> {
    fn new<V: Value>(db: *mut sqlite::sqlite3, data: &'a V) -> Result<Self, SQLiteError> {
        return Ok(Self {
            db: db,
            data_tables: BTreeSet::new(),
            partial: match data.value_type() {
                ColumnType::Text => {
                    let text = data.text();
                    if text.len() > 0 {
                        #[derive(Deserialize)]
                        struct PartialSyncLocalArguments {
                            #[serde(rename = "buckets")]
                            _buckets: Vec<String>,
                            priority: BucketPriority,
                        }

                        let args: PartialSyncLocalArguments = serde_json::from_str(text)?;
                        Some(PartialSyncOperation {
                            priority: args.priority,
                            args: text,
                        })
                    } else {
                        None
                    }
                }
                _ => None,
            },
        });
    }

    fn can_apply_sync_changes(&self) -> Result<bool, SQLiteError> {
        // Don't publish downloaded data until the upload queue is empty (except for downloaded data
        // in priority 0, which is published earlier).

        let needs_check = match &self.partial {
            Some(p) => !p.priority.may_publish_with_outstanding_uploads(),
            None => true,
        };

        if needs_check {
            // language=SQLite
            let statement = self.db.prepare_v2(
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

            let statement = self.db.prepare_v2("SELECT 1 FROM ps_crud LIMIT 1")?;
            if statement.step()? != ResultCode::DONE {
                return Ok(false);
            }
        }

        Ok(true)
    }

    fn apply(&mut self) -> Result<i64, SQLiteError> {
        if !self.can_apply_sync_changes()? {
            return Ok(0);
        }

        self.collect_tables()?;
        let statement = self.collect_full_operations()?;

        // We cache the last insert and delete statements for each row
        let mut last_insert_table: Option<String> = None;
        let mut last_insert_statement: Option<ManagedStmt> = None;

        let mut last_delete_table: Option<String> = None;
        let mut last_delete_statement: Option<ManagedStmt> = None;

        let mut untyped_delete_statement: Option<ManagedStmt> = None;
        let mut untyped_insert_statement: Option<ManagedStmt> = None;

        while statement.step().into_db_result(self.db)? == ResultCode::ROW {
            let type_name = statement.column_text(0)?;
            let id = statement.column_text(1)?;
            let data = statement.column_text(2);

            let table_name = internal_table_name(type_name);

            if self.data_tables.contains(&table_name) {
                let quoted = quote_internal_name(type_name, false);

                // is_err() is essentially a NULL check here.
                // NULL data means no PUT operations found, so we delete the row.
                if data.is_err() {
                    // DELETE
                    if last_delete_table.as_deref() != Some(&quoted) {
                        // Prepare statement when the table changed
                        last_delete_statement = Some(
                            self.db
                                .prepare_v2(&format!("DELETE FROM {} WHERE id = ?", quoted))
                                .into_db_result(self.db)?,
                        );
                        last_delete_table = Some(quoted.clone());
                    }
                    let delete_statement = last_delete_statement.as_mut().unwrap();

                    delete_statement.reset()?;
                    delete_statement.bind_text(1, id, sqlite::Destructor::STATIC)?;
                    delete_statement.exec()?;
                } else {
                    // INSERT/UPDATE
                    if last_insert_table.as_deref() != Some(&quoted) {
                        // Prepare statement when the table changed
                        last_insert_statement = Some(
                            self.db
                                .prepare_v2(&format!(
                                    "REPLACE INTO {}(id, data) VALUES(?, ?)",
                                    quoted
                                ))
                                .into_db_result(self.db)?,
                        );
                        last_insert_table = Some(quoted.clone());
                    }
                    let insert_statement = last_insert_statement.as_mut().unwrap();
                    insert_statement.reset()?;
                    insert_statement.bind_text(1, id, sqlite::Destructor::STATIC)?;
                    insert_statement.bind_text(2, data?, sqlite::Destructor::STATIC)?;
                    insert_statement.exec()?;
                }
            } else {
                if data.is_err() {
                    // DELETE
                    if untyped_delete_statement.is_none() {
                        // Prepare statement on first use
                        untyped_delete_statement = Some(
                            self.db
                                .prepare_v2("DELETE FROM ps_untyped WHERE type = ? AND id = ?")
                                .into_db_result(self.db)?,
                        );
                    }
                    let delete_statement = untyped_delete_statement.as_mut().unwrap();
                    delete_statement.reset()?;
                    delete_statement.bind_text(1, type_name, sqlite::Destructor::STATIC)?;
                    delete_statement.bind_text(2, id, sqlite::Destructor::STATIC)?;
                    delete_statement.exec()?;
                } else {
                    // INSERT/UPDATE
                    if untyped_insert_statement.is_none() {
                        // Prepare statement on first use
                        untyped_insert_statement = Some(
                            self.db
                                .prepare_v2(
                                    "REPLACE INTO ps_untyped(type, id, data) VALUES(?, ?, ?)",
                                )
                                .into_db_result(self.db)?,
                        );
                    }
                    let insert_statement = untyped_insert_statement.as_mut().unwrap();
                    insert_statement.reset()?;
                    insert_statement.bind_text(1, type_name, sqlite::Destructor::STATIC)?;
                    insert_statement.bind_text(2, id, sqlite::Destructor::STATIC)?;
                    insert_statement.bind_text(3, data?, sqlite::Destructor::STATIC)?;
                    insert_statement.exec()?;
                }
            }
        }

        self.set_last_applied_op()?;
        self.mark_completed()?;

        Ok(1)
    }

    fn collect_tables(&mut self) -> Result<(), SQLiteError> {
        // language=SQLite
        let statement = self
            .db
            .prepare_v2(
                "SELECT name FROM sqlite_master WHERE type='table' AND name GLOB 'ps_data_*'",
            )
            .into_db_result(self.db)?;

        while statement.step()? == ResultCode::ROW {
            let name = statement.column_text(0)?;
            self.data_tables.insert(String::from(name));
        }
        Ok(())
    }

    fn collect_full_operations(&self) -> Result<ManagedStmt, SQLiteError> {
        Ok(match &self.partial {
            None => {
                // Complete sync
                // See dart/test/sync_local_performance_test.dart for an annotated version of this query.
                self.db
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
        SELECT iif(max(r.op_id), r.data, null)
                 FROM ps_oplog r
                WHERE r.row_type = b.row_type
                  AND r.row_id = b.row_id

    ) as data
    FROM updated_rows b
    GROUP BY b.row_type, b.row_id;",
                    )
                    .into_db_result(self.db)?
            }
            Some(partial) => {
                let stmt = self
                    .db
                    .prepare_v2(
                        "\
-- 1. Filter oplog by the ops added but not applied yet (oplog b).
--    We do not do any DISTINCT operation here, since that introduces a temp b-tree.
--    We filter out duplicates using the GROUP BY below.
WITH 
  involved_buckets (id) AS MATERIALIZED (
    SELECT id FROM ps_buckets WHERE ?1 IS NULL
      OR name IN (SELECT value FROM json_each(json_extract(?1, '$.buckets')))
  ),
  updated_rows AS (
    SELECT b.row_type, b.row_id FROM ps_buckets AS buckets
        CROSS JOIN ps_oplog AS b ON b.bucket = buckets.id
        AND (b.op_id > buckets.last_applied_op)
        WHERE buckets.id IN (SELECT id FROM involved_buckets)
  )

-- 2. Find *all* current ops over different buckets for those objects (oplog r).
SELECT
    b.row_type,
    b.row_id,
    (
        -- 3. For each unique row, select the data from the latest oplog entry.
        -- The max(r.op_id) clause is used to select the latest oplog entry.
        -- The iif is to avoid the max(r.op_id) column ending up in the results.
        SELECT iif(max(r.op_id), r.data, null)
                 FROM ps_oplog r
                WHERE r.row_type = b.row_type
                  AND r.row_id = b.row_id
                  AND r.bucket IN (SELECT id FROM involved_buckets)

    ) as data
    FROM updated_rows b
    -- Group for (2)
    GROUP BY b.row_type, b.row_id;",
                    )
                    .into_db_result(self.db)?;
                stmt.bind_text(1, partial.args, Destructor::STATIC)?;

                stmt
            }
        })
    }

    fn set_last_applied_op(&self) -> Result<(), SQLiteError> {
        match &self.partial {
            Some(partial) => {
                // language=SQLite
                let updated = self
                    .db
                    .prepare_v2(   "\
                        UPDATE ps_buckets
                            SET last_applied_op = last_op
                            WHERE last_applied_op != last_op AND
                                name IN (SELECT value FROM json_each(json_extract(?1, '$.buckets')))",
                    )
                    .into_db_result(self.db)?;
                updated.bind_text(1, partial.args, Destructor::STATIC)?;
                updated.exec()?;
            }
            None => {
                // language=SQLite
                self.db
                    .exec_safe(
                        "UPDATE ps_buckets
                                SET last_applied_op = last_op
                                WHERE last_applied_op != last_op",
                    )
                    .into_db_result(self.db)?;
            }
        }

        Ok(())
    }

    fn mark_completed(&self) -> Result<(), SQLiteError> {
        let priority_code: i32 = match &self.partial {
            None => {
                // language=SQLite
                self.db
                    .exec_safe("DELETE FROM ps_updated_rows")
                    .into_db_result(self.db)?;
                BucketPriority::SENTINEL
            }
            Some(partial) => partial.priority,
        }
        .into();

        // Higher-priority buckets are always part of lower-priority sync operations too, so we can
        // delete information about higher-priority syncs (represented as lower priority numbers).
        // A complete sync is represented by a number higher than the lowest priority we allow.
        // language=SQLite
        let stmt = self
            .db
            .prepare_v2("DELETE FROM ps_sync_state WHERE priority < ?1;")
            .into_db_result(self.db)?;
        stmt.bind_int(1, priority_code)?;
        stmt.exec()?;

        // language=SQLite
        let stmt = self
            .db
            .prepare_v2("INSERT OR REPLACE INTO ps_sync_state (priority, last_synced_at) VALUES (?, datetime());")
            .into_db_result(self.db)?;
        stmt.bind_int(1, priority_code)?;
        stmt.exec()?;

        Ok(())
    }
}
