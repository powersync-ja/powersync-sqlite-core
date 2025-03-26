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
        // TODO: cache statements
        while statement.step().into_db_result(self.db)? == ResultCode::ROW {
            let type_name = statement.column_text(0)?;
            let id = statement.column_text(1)?;
            let buckets = statement.column_int(3)?;
            let data = statement.column_text(2);

            let table_name = internal_table_name(type_name);

            if self.data_tables.contains(&table_name) {
                let quoted = quote_internal_name(type_name, false);

                if buckets == 0 {
                    // DELETE
                    let delete_statement = self
                        .db
                        .prepare_v2(&format!("DELETE FROM {} WHERE id = ?", quoted))
                        .into_db_result(self.db)?;
                    delete_statement.bind_text(1, id, sqlite::Destructor::STATIC)?;
                    delete_statement.exec()?;
                } else {
                    // INSERT/UPDATE
                    let insert_statement = self
                        .db
                        .prepare_v2(&format!("REPLACE INTO {}(id, data) VALUES(?, ?)", quoted))
                        .into_db_result(self.db)?;
                    insert_statement.bind_text(1, id, sqlite::Destructor::STATIC)?;
                    insert_statement.bind_text(2, data?, sqlite::Destructor::STATIC)?;
                    insert_statement.exec()?;
                }
            } else {
                if buckets == 0 {
                    // DELETE
                    // language=SQLite
                    let delete_statement = self
                        .db
                        .prepare_v2("DELETE FROM ps_untyped WHERE type = ? AND id = ?")
                        .into_db_result(self.db)?;
                    delete_statement.bind_text(1, type_name, sqlite::Destructor::STATIC)?;
                    delete_statement.bind_text(2, id, sqlite::Destructor::STATIC)?;
                    delete_statement.exec()?;
                } else {
                    // INSERT/UPDATE
                    // language=SQLite
                    let insert_statement = self
                        .db
                        .prepare_v2("REPLACE INTO ps_untyped(type, id, data) VALUES(?, ?, ?)")
                        .into_db_result(self.db)?;
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
                self.db
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
                    .into_db_result(self.db)?
            }
            Some(partial) => {
                let stmt = self
                    .db
                    .prepare_v2(
                        "\
-- 1. Filter oplog by the ops added but not applied yet (oplog b).
--    SELECT DISTINCT / UNION is important for cases with many duplicate ids.
WITH 
  involved_buckets (id) AS MATERIALIZED (
    SELECT id FROM ps_buckets WHERE ?1 IS NULL
      OR name IN (SELECT value FROM json_each(json_extract(?1, '$.buckets')))
  ),
  updated_rows AS (
    SELECT DISTINCT FALSE as local, b.row_type, b.row_id FROM ps_buckets AS buckets
      CROSS JOIN ps_oplog AS b ON b.bucket = buckets.id AND (b.op_id > buckets.last_applied_op)
      WHERE buckets.id IN (SELECT id FROM involved_buckets)
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
                 AND r.bucket IN (SELECT id FROM involved_buckets)
-- Group for (3)
GROUP BY b.row_type, b.row_id",
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
                // Note: This one deliberately doesn't reset count_since_last or updates
                // count_at_last! We want a download progress to always cover a complete sync
                // checkpoint instead of resetting for partial completions.
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
                                SET last_applied_op = last_op,
                                    count_since_last = 0,
                                    count_at_last = count_at_last + count_since_last
                                WHERE (last_applied_op != last_op) OR count_since_last",
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
