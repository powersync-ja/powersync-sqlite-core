use alloc::collections::btree_map::BTreeMap;
use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use serde::Deserialize;

use crate::error::{PowerSyncError, RawPowerSyncError};
use crate::schema::{PendingStatement, PendingStatementValue, RawTable, Schema};
use crate::state::DatabaseState;
use crate::sync::BucketPriority;
use sqlite_nostd::{self as sqlite, Destructor, ManagedStmt, Value};
use sqlite_nostd::{ColumnType, Connection, ResultCode};

use crate::ext::SafeManagedStmt;
use crate::util::quote_internal_name;

pub fn sync_local<V: Value>(
    state: &DatabaseState,
    db: *mut sqlite::sqlite3,
    data: &V,
) -> Result<i64, PowerSyncError> {
    let mut operation: SyncOperation<'_> = SyncOperation::from_args(state, db, data)?;
    operation.apply()
}

pub struct PartialSyncOperation<'a> {
    /// The lowest priority part of the partial sync operation.
    pub priority: BucketPriority,
    /// The JSON-encoded arguments passed by the client SDK. This includes the priority and a list
    /// of bucket names in that (and higher) priorities.
    pub args: &'a str,
}

pub struct SyncOperation<'a> {
    state: &'a DatabaseState,
    db: *mut sqlite::sqlite3,
    schema: ParsedDatabaseSchema<'a>,
    partial: Option<PartialSyncOperation<'a>>,
}

impl<'a> SyncOperation<'a> {
    fn from_args<V: Value>(
        state: &'a DatabaseState,
        db: *mut sqlite::sqlite3,
        data: &'a V,
    ) -> Result<Self, serde_json::Error> {
        Ok(Self::new(
            state,
            db,
            match data.value_type() {
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
        ))
    }

    pub fn new(
        state: &'a DatabaseState,
        db: *mut sqlite::sqlite3,
        partial: Option<PartialSyncOperation<'a>>,
    ) -> Self {
        Self {
            state,
            db,
            schema: ParsedDatabaseSchema::new(),
            partial,
        }
    }

    pub fn use_schema(&mut self, schema: &'a Schema) {
        self.schema.add_from_schema(schema);
    }

    fn can_apply_sync_changes(&self) -> Result<bool, PowerSyncError> {
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
                return Err(RawPowerSyncError::Internal.into());
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

    pub fn apply(&mut self) -> Result<i64, PowerSyncError> {
        let guard = self.state.sync_local_guard();

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

        while statement.step()? == ResultCode::ROW {
            let type_name = statement.column_text(0)?;
            let id = statement.column_text(1)?;
            let data = statement.column_text(2);

            if let Some(known) = self.schema.tables.get_mut(type_name) {
                if let Some(raw) = &mut known.raw {
                    match data {
                        Ok(data) => {
                            let stmt = raw.put_statement(self.db)?;
                            let parsed: serde_json::Value = serde_json::from_str(data)?;
                            stmt.bind_for_put(id, &parsed)?;
                            stmt.stmt.exec()?;
                        }
                        Err(_) => {
                            let stmt = raw.delete_statement(self.db)?;
                            stmt.bind_for_delete(id)?;
                            stmt.stmt.exec()?;
                        }
                    }
                } else {
                    let quoted = quote_internal_name(type_name, false);

                    // is_err() is essentially a NULL check here.
                    // NULL data means no PUT operations found, so we delete the row.
                    if data.is_err() {
                        // DELETE
                        if last_delete_table.as_deref() != Some(&quoted) {
                            // Prepare statement when the table changed
                            last_delete_statement = Some(
                                self.db
                                    .prepare_v2(&format!("DELETE FROM {} WHERE id = ?", quoted))?,
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
                            last_insert_statement = Some(self.db.prepare_v2(&format!(
                                "REPLACE INTO {}(id, data) VALUES(?, ?)",
                                quoted
                            ))?);
                            last_insert_table = Some(quoted.clone());
                        }
                        let insert_statement = last_insert_statement.as_mut().unwrap();
                        insert_statement.reset()?;
                        insert_statement.bind_text(1, id, sqlite::Destructor::STATIC)?;
                        insert_statement.bind_text(2, data?, sqlite::Destructor::STATIC)?;
                        insert_statement.exec()?;
                    }
                }
            } else {
                if data.is_err() {
                    // DELETE
                    if untyped_delete_statement.is_none() {
                        // Prepare statement on first use
                        untyped_delete_statement = Some(
                            self.db
                                .prepare_v2("DELETE FROM ps_untyped WHERE type = ? AND id = ?")?,
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
                        untyped_insert_statement = Some(self.db.prepare_v2(
                            "REPLACE INTO ps_untyped(type, id, data) VALUES(?, ?, ?)",
                        )?);
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

        drop(guard);
        Ok(1)
    }

    fn collect_tables(&mut self) -> Result<(), ResultCode> {
        self.schema.add_from_db(self.db)
    }

    fn collect_full_operations(&self) -> Result<ManagedStmt, ResultCode> {
        Ok(match &self.partial {
            None => {
                // Complete sync
                // See dart/test/sync_local_performance_test.dart for an annotated version of this query.
                self.db.prepare_v2(
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
                )?
            }
            Some(partial) => {
                let stmt = self.db.prepare_v2(
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
                )?;
                stmt.bind_text(1, partial.args, Destructor::STATIC)?;

                stmt
            }
        })
    }

    fn set_last_applied_op(&self) -> Result<(), ResultCode> {
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
                    )?;
                updated.bind_text(1, partial.args, Destructor::STATIC)?;
                updated.exec()?;
            }
            None => {
                // language=SQLite
                self.db.exec_safe(
                    "UPDATE ps_buckets
                                SET last_applied_op = last_op
                                WHERE last_applied_op != last_op",
                )?;
            }
        }

        Ok(())
    }

    fn mark_completed(&self) -> Result<(), ResultCode> {
        let priority_code: i32 = match &self.partial {
            None => {
                // language=SQLite
                self.db.exec_safe("DELETE FROM ps_updated_rows")?;
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
            .prepare_v2("DELETE FROM ps_sync_state WHERE priority < ?1;")?;
        stmt.bind_int(1, priority_code)?;
        stmt.exec()?;

        // language=SQLite
        let stmt = self
            .db
            .prepare_v2("INSERT OR REPLACE INTO ps_sync_state (priority, last_synced_at) VALUES (?, datetime());")?;
        stmt.bind_int(1, priority_code)?;
        stmt.exec()?;

        Ok(())
    }
}

struct ParsedDatabaseSchema<'a> {
    tables: BTreeMap<String, ParsedSchemaTable<'a>>,
}

impl<'a> ParsedDatabaseSchema<'a> {
    fn new() -> Self {
        Self {
            tables: BTreeMap::new(),
        }
    }

    fn add_from_schema(&mut self, schema: &'a Schema) {
        for raw in &schema.raw_tables {
            self.tables
                .insert(raw.name.clone(), ParsedSchemaTable::raw(raw));
        }
    }

    fn add_from_db(&mut self, db: *mut sqlite::sqlite3) -> Result<(), ResultCode> {
        // language=SQLite
        let statement = db.prepare_v2(
            "SELECT name FROM sqlite_master WHERE type='table' AND name GLOB 'ps_data_*'",
        )?;

        while statement.step()? == ResultCode::ROW {
            let name = statement.column_text(0)?;
            // Strip the ps_data__ prefix so that we can lookup tables by their sync protocol name.
            let visible_name = name.get(9..).unwrap_or(name);

            // Tables which haven't been passed explicitly are assumed to not be raw tables.
            self.tables
                .insert(String::from(visible_name), ParsedSchemaTable::json_table());
        }
        Ok(())
    }
}

struct ParsedSchemaTable<'a> {
    raw: Option<RawTableWithCachedStatements<'a>>,
}

struct RawTableWithCachedStatements<'a> {
    definition: &'a RawTable,
    cached_put: Option<PreparedPendingStatement<'a>>,
    cached_delete: Option<PreparedPendingStatement<'a>>,
}

impl<'a> RawTableWithCachedStatements<'a> {
    fn put_statement(
        &mut self,
        db: *mut sqlite::sqlite3,
    ) -> Result<&PreparedPendingStatement, PowerSyncError> {
        let cache_slot = &mut self.cached_put;
        if let None = cache_slot {
            let stmt = PreparedPendingStatement::prepare(db, &self.definition.put)?;
            *cache_slot = Some(stmt);
        }

        return Ok(cache_slot.as_ref().unwrap());
    }

    fn delete_statement(
        &mut self,
        db: *mut sqlite::sqlite3,
    ) -> Result<&PreparedPendingStatement, PowerSyncError> {
        let cache_slot = &mut self.cached_delete;
        if let None = cache_slot {
            let stmt = PreparedPendingStatement::prepare(db, &self.definition.delete)?;
            *cache_slot = Some(stmt);
        }

        return Ok(cache_slot.as_ref().unwrap());
    }
}

impl<'a> ParsedSchemaTable<'a> {
    pub const fn json_table() -> Self {
        Self { raw: None }
    }

    pub fn raw(definition: &'a RawTable) -> Self {
        Self {
            raw: Some(RawTableWithCachedStatements {
                definition,
                cached_put: None,
                cached_delete: None,
            }),
        }
    }
}

struct PreparedPendingStatement<'a> {
    stmt: ManagedStmt,
    params: &'a [PendingStatementValue],
}

impl<'a> PreparedPendingStatement<'a> {
    pub fn prepare(
        db: *mut sqlite::sqlite3,
        pending: &'a PendingStatement,
    ) -> Result<Self, PowerSyncError> {
        let stmt = db.prepare_v2(&pending.sql)?;
        if stmt.bind_parameter_count() as usize != pending.params.len() {
            return Err(RawPowerSyncError::InvalidPendingStatement {
                description: format!(
                    "Statement {} has {} parameters, but {} values were provided as sources.",
                    &pending.sql,
                    stmt.bind_parameter_count(),
                    pending.params.len(),
                )
                .into(),
            }
            .into());
        }

        // TODO: other validity checks?

        Ok(Self {
            stmt,
            params: &pending.params,
        })
    }

    pub fn bind_for_put(
        &self,
        id: &str,
        json_data: &serde_json::Value,
    ) -> Result<(), PowerSyncError> {
        use serde_json::Value;
        for (i, source) in self.params.iter().enumerate() {
            let i = (i + 1) as i32;

            match source {
                PendingStatementValue::Id => {
                    self.stmt.bind_text(i, id, Destructor::STATIC)?;
                }
                PendingStatementValue::Column(column) => {
                    let parsed = json_data.as_object().ok_or_else(|| {
                        PowerSyncError::argument_error("expected oplog data to be an object")
                    })?;

                    match parsed.get(column) {
                        Some(Value::Bool(value)) => {
                            self.stmt.bind_int(i, if *value { 1 } else { 0 })
                        }
                        Some(Value::Number(value)) => {
                            if let Some(value) = value.as_f64() {
                                self.stmt.bind_double(i, value)
                            } else if let Some(value) = value.as_u64() {
                                self.stmt.bind_int64(i, value as i64)
                            } else {
                                self.stmt.bind_int64(i, value.as_i64().unwrap())
                            }
                        }
                        Some(Value::String(source)) => {
                            self.stmt.bind_text(i, &source, Destructor::STATIC)
                        }
                        _ => self.stmt.bind_null(i),
                    }?;
                }
            }
        }

        Ok(())
    }

    pub fn bind_for_delete(&self, id: &str) -> Result<(), PowerSyncError> {
        for (i, source) in self.params.iter().enumerate() {
            if let PendingStatementValue::Id = source {
                self.stmt
                    .bind_text((i + 1) as i32, id, Destructor::STATIC)?;
            } else {
                return Err(RawPowerSyncError::InvalidPendingStatement {
                    description: "Raw delete statement parameters must only reference id".into(),
                }
                .into());
            }
        }

        Ok(())
    }
}
