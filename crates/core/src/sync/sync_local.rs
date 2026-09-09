use core::fmt::Write;

use alloc::collections::btree_map::BTreeMap;
use alloc::rc::Rc;
use alloc::string::String;
use alloc::vec::Vec;
use alloc::{format, vec};
use serde::Serialize;
use serde::ser::SerializeMap;

use crate::error::{PowerSyncError, Result};
use crate::schema::inspection::ExistingTable;
use crate::schema::{
    InferredSchemaCache, PendingStatement, PendingStatementValue, RawTable, Schema, SchemaTable,
    Table,
};
use crate::state::DatabaseState;
use crate::sync::BucketPriority;
use crate::sync::storage_adapter::{
    LAST_SEEN_CHECKPOINT_REQUEST_ID_KEY, TARGET_CHECKPOINT_REQUEST_ID_KEY,
};
use crate::sync::sync_status::TimestampMicros;
use crate::utils::SqlBuffer;
use crate::utils::database::{Database, Statement};
use const_format::formatcp;
use powersync_sqlite_nostd::{self as sqlite, Destructor};

pub struct PartialSyncOperation<'a> {
    /// The lowest priority part of the partial sync operation.
    pub priority: BucketPriority,
    pub involved_buckets: Vec<&'a str>,
}

pub struct SyncOperation<'a> {
    state: &'a DatabaseState,
    db: Database,
    schema: ParsedDatabaseSchema<'a>,
    partial: Option<PartialSyncOperation<'a>>,
    time: TimestampMicros,
}

impl<'a> SyncOperation<'a> {
    pub fn new(
        state: &'a DatabaseState,
        db: Database,
        partial: Option<PartialSyncOperation<'a>>,
        time: TimestampMicros,
    ) -> Self {
        Self {
            state,
            db,
            schema: ParsedDatabaseSchema::new(),
            partial,
            time,
        }
    }

    pub fn use_schema(&mut self, schema: &'a Schema) {
        self.schema.add_from_schema(schema);
    }

    fn can_apply_sync_changes(&self) -> Result<bool> {
        // Don't publish downloaded data until the upload queue is empty (except for downloaded data
        // in priority 0, which is published earlier).

        let needs_check = match &self.partial {
            Some(p) => !p.priority.may_publish_with_outstanding_uploads(),
            None => true,
        };

        if needs_check {
            // language=SQLite
            let statement = self.db.prepare_v2(formatcp!(
                "SELECT 1
FROM ps_kv AS target
LEFT JOIN ps_kv AS seen ON seen.key = '{LAST_SEEN_CHECKPOINT_REQUEST_ID_KEY}'
WHERE target.key = '{TARGET_CHECKPOINT_REQUEST_ID_KEY}'
  AND CAST(target.value AS INTEGER) > COALESCE(CAST(seen.value AS INTEGER), 0)"
            ))?;

            if statement.step()? {
                return Ok(false);
            }

            let statement = self.db.prepare_v2("SELECT 1 FROM ps_crud LIMIT 1")?;
            if statement.step()? {
                return Ok(false);
            }
        }

        Ok(true)
    }

    pub fn apply(&mut self) -> Result<i64> {
        let guard = self.state.sync_local_guard();

        if !self.can_apply_sync_changes()? {
            return Ok(0);
        }

        self.collect_tables()?;
        let statement = self.collect_full_operations()?;
        // We're in a transaction, so the schem can't change while we're applying changes.
        let schema_version = InferredSchemaCache::current_schema_version(self.db)?;
        let schema_cache = &self.state.inferred_schema_cache;

        let mut untyped_delete_statement: Option<Statement> = None;
        let mut untyped_insert_statement: Option<Statement> = None;

        while statement.step()? {
            let type_name = statement.column_text(0)?;
            let id = statement.column_text(1)?;
            let data = statement.column_text(2);

            if let Some(known) = self.schema.tables.get_mut(type_name) {
                match data {
                    Ok(data) => {
                        let stmt = known.put_statement(self.db, schema_version, schema_cache)?;

                        if stmt.needs_parsed_json {
                            let parsed: serde_json::Value = serde_json::from_str(data)
                                .map_err(PowerSyncError::json_local_error)?;
                            let json_object = parsed.as_object().ok_or_else(|| {
                                PowerSyncError::argument_error(
                                    "expected oplog data to be an object",
                                )
                            })?;
                            let rest = stmt.render_rest_object(json_object)?;
                            stmt.bind_for_put(id, data, Some(json_object), rest.as_ref())?;
                            stmt.exec(type_name, id, Some(&data))?;
                        } else {
                            stmt.bind_for_put(id, data, None, None)?;
                            stmt.exec(type_name, id, Some(&data))?;
                        }
                    }
                    Err(_) => {
                        // is_err() is essentially a NULL check here.
                        // NULL data means no PUT operations found, so we delete the row.
                        let stmt = known.delete_statement(self.db, schema_version, schema_cache)?;
                        stmt.bind_for_delete(id)?;
                        stmt.exec(type_name, id, None)?;
                    }
                }
            } else {
                if data.is_err() {
                    // DELETE
                    let delete_statement =
                        match &untyped_delete_statement {
                            Some(stmt) => stmt,
                            None => {
                                // Prepare statement on first use
                                untyped_delete_statement.insert(self.db.prepare_v2(
                                    "DELETE FROM ps_untyped WHERE type = ? AND id = ?",
                                )?)
                            }
                        };

                    delete_statement.reset()?;
                    delete_statement.bind_text(1, type_name, sqlite::Destructor::STATIC)?;
                    delete_statement.bind_text(2, id, sqlite::Destructor::STATIC)?;
                    delete_statement.exec()?;
                } else {
                    // INSERT/UPDATE
                    let insert_statement = match &untyped_insert_statement {
                        Some(stmt) => stmt,
                        None => {
                            // Prepare statement on first use
                            untyped_insert_statement.insert(self.db.prepare_v2(
                                "REPLACE INTO ps_untyped(type, id, data) VALUES(?, ?, ?)",
                            )?)
                        }
                    };

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

    fn collect_tables(&mut self) -> Result<()> {
        self.schema.add_from_db(self.db)
    }

    fn collect_full_operations(&self) -> Result<Statement> {
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
    SELECT id FROM ps_buckets
      WHERE name IN (SELECT value FROM json_each(?1))
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

                let bucket_ids = serde_json::to_string(&partial.involved_buckets)
                    .map_err(PowerSyncError::internal)?;
                stmt.bind_text(1, &bucket_ids, Destructor::TRANSIENT)?;

                stmt
            }
        })
    }

    fn set_last_applied_op(&self) -> Result<()> {
        match &self.partial {
            Some(partial) => {
                // language=SQLite
                let updated = self.db.prepare_v2(
                    "\
                        UPDATE ps_buckets
                            SET last_applied_op = last_op
                            WHERE last_applied_op != last_op AND name = ?",
                )?;

                for bucket in &partial.involved_buckets {
                    updated.bind_text(1, bucket, Destructor::STATIC)?;
                    updated.exec()?;
                }
            }
            None => {
                // language=SQLite
                self.db.exec_safe(
                    c"UPDATE ps_buckets
                                SET last_applied_op = last_op
                                WHERE last_applied_op != last_op",
                )?;
            }
        }

        Ok(())
    }

    fn mark_completed(&self) -> Result<()> {
        let priority_code: i32 = match &self.partial {
            None => {
                // language=SQLite
                self.db.exec_safe(c"DELETE FROM ps_updated_rows")?;
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
        let stmt = self.db.prepare_v2(
            "INSERT OR REPLACE INTO ps_sync_state (priority, last_synced_at) VALUES (?, ?);",
        )?;
        stmt.bind_int(1, priority_code)?;
        stmt.bind_int64(2, self.time.0)?;
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
        for regular in &schema.tables {
            if regular.direct && !regular.local_only() {
                self.tables.insert(
                    regular.name.clone(),
                    ParsedSchemaTable::new(TableDefinition::Direct(regular)),
                );
            }
        }

        for raw in &schema.raw_tables {
            self.tables.insert(
                raw.name.clone(),
                ParsedSchemaTable::new(TableDefinition::Raw(raw)),
            );
        }
    }

    fn add_from_db(&mut self, db: Database) -> Result<()> {
        // Ignore direct tables here, we can rely on them being added via add_from_schema.
        // TODO: Remove this function, SDKs should always pass the used schema when they connect.
        let tables = ExistingTable::list_filtered(db, true)?;
        for table in tables {
            if !table.local_only && !self.tables.contains_key(&table.name) {
                let visible_name = table.name;

                self.tables.insert(
                    visible_name,
                    ParsedSchemaTable::new(TableDefinition::JsonView {
                        local_table: table.internal_name,
                    }),
                );
            }
        }

        Ok(())
    }
}

struct ParsedSchemaTable<'a> {
    definition: TableDefinition<'a>,
    cached_put: Option<PreparedPendingStatement>,
    cached_delete: Option<PreparedPendingStatement>,
}

impl<'a> ParsedSchemaTable<'a> {
    const fn new(definition: TableDefinition<'a>) -> Self {
        Self {
            definition,
            cached_put: None,
            cached_delete: None,
        }
    }

    fn prepare_lazily(
        db: Database,
        slot: &mut Option<PreparedPendingStatement>,
        create_stmt: impl FnOnce() -> Result<Rc<PendingStatement>>,
    ) -> Result<&PreparedPendingStatement> {
        Ok(match slot {
            Some(stmt) => stmt,
            None => {
                let stmt = PreparedPendingStatement::prepare(db, create_stmt()?)?;
                slot.insert(stmt)
            }
        })
    }

    fn put_statement(
        &'_ mut self,
        db: Database,
        schema_version: usize,
        cache: &InferredSchemaCache,
    ) -> Result<&'_ PreparedPendingStatement> {
        Self::prepare_lazily(db, &mut self.cached_put, || {
            Ok(match self.definition {
                TableDefinition::Raw(raw_table) => match raw_table.put {
                    Some(ref stmt) => stmt.clone(),
                    None => cache.infer_put_statement(db, schema_version, raw_table)?,
                },
                TableDefinition::JsonView { ref local_table } => {
                    let mut statement = SqlBuffer::new();
                    statement.push_str("REPLACE INTO ");
                    let _ = statement.identifier().write_str(local_table);
                    statement.push_str("(id, data) VALUES (?, ?)");

                    Rc::new(PendingStatement {
                        sql: statement.sql,
                        params: vec![PendingStatementValue::Id, PendingStatementValue::Row],
                        named_parameters_index: None,
                    })
                }
                TableDefinition::Direct(table) => {
                    Rc::new(SchemaTable::Json(table).infer_put_stmt(&table.name))
                }
            })
        })
    }

    fn delete_statement(
        &'_ mut self,
        db: Database,
        schema_version: usize,
        cache: &InferredSchemaCache,
    ) -> Result<&'_ PreparedPendingStatement> {
        Self::prepare_lazily(db, &mut self.cached_delete, || {
            Ok(match self.definition {
                TableDefinition::Raw(raw_table) => match raw_table.delete {
                    Some(ref stmt) => stmt.clone(),
                    None => cache.infer_delete_statement(db, schema_version, raw_table)?,
                },
                TableDefinition::JsonView { ref local_table } => {
                    let mut statement = SqlBuffer::new();
                    statement.push_str("DELETE FROM ");
                    let _ = statement.identifier().write_str(&local_table);
                    statement.push_str(" WHERE id = ?");

                    Rc::new(PendingStatement {
                        sql: statement.sql,
                        params: vec![PendingStatementValue::Id],
                        named_parameters_index: None,
                    })
                }
                TableDefinition::Direct(table) => {
                    Rc::new(SchemaTable::Json(table).infer_delete_stmt(&table.name))
                }
            })
        })
    }
}

enum TableDefinition<'a> {
    Raw(&'a RawTable),
    JsonView { local_table: String },
    Direct(&'a Table),
}

struct PreparedPendingStatement {
    stmt: Statement,
    needs_parsed_json: bool,
    definition: Rc<PendingStatement>,
}

impl PreparedPendingStatement {
    pub fn prepare(db: Database, pending: Rc<PendingStatement>) -> Result<Self> {
        let stmt = db.prepare_v2(&pending.sql)?;
        if stmt.bind_parameter_count() != pending.params.len() {
            return Err(PowerSyncError::argument_error(format!(
                "Statement {} has {} parameters, but {} values were provided as sources.",
                &pending.sql,
                stmt.bind_parameter_count(),
                pending.params.len(),
            )));
        }

        // TODO: other validity checks?

        Ok(Self {
            stmt,
            needs_parsed_json: pending.params.iter().any(|p| match p {
                PendingStatementValue::Id | PendingStatementValue::Row => false,
                PendingStatementValue::Column(_) | PendingStatementValue::Rest => true,
            }),
            definition: pending,
        })
    }

    pub fn render_rest_object(
        &self,
        json_data: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<Option<String>> {
        use serde_json::Value;

        let Some(ref index) = self.definition.named_parameters_index else {
            return Ok(None);
        };

        struct UnmatchedValues<'a>(BTreeMap<&'a String, &'a Value>);

        impl<'a> Serialize for UnmatchedValues<'a> {
            fn serialize<S>(&self, serializer: S) -> core::result::Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                let mut map = serializer.serialize_map(Some(self.0.len()))?;

                for (k, v) in &self.0 {
                    map.serialize_entry(k, v)?;
                }

                map.end()
            }
        }

        let mut unmatched_values: Option<UnmatchedValues> = None;
        for (key, value) in json_data {
            if !index.named_parameters.contains(key) {
                unmatched_values
                    .get_or_insert_with(|| UnmatchedValues(BTreeMap::new()))
                    .0
                    .insert(key, value);
            }
        }

        Ok(match unmatched_values {
            None => None,
            Some(unmatched) => {
                Some(serde_json::to_string(&unmatched).map_err(|e| PowerSyncError::internal(e))?)
            }
        })
    }

    pub fn bind_for_put(
        &self,
        id: &str,
        row: &str,
        json_data: Option<&serde_json::Map<String, serde_json::Value>>,
        rest: Option<&String>,
    ) -> Result<()> {
        use serde_json::Value;

        for (i, source) in self.definition.params.iter().enumerate() {
            let i = (i + 1) as i32;

            match source {
                PendingStatementValue::Id => {
                    self.stmt.bind_text(i, id, Destructor::STATIC)?;
                }
                PendingStatementValue::Row => {
                    self.stmt.bind_text(i, row, Destructor::STATIC)?;
                }
                PendingStatementValue::Column(column) => {
                    match json_data.and_then(|m| m.get(column)) {
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
                PendingStatementValue::Rest => {
                    // These are bound later.
                    debug_assert!(self.definition.named_parameters_index.is_some());
                }
            }
        }

        if let Some(index) = &self.definition.named_parameters_index {
            for target in &index.rest_parameter_positions {
                let index = (*target + 1) as i32;
                match rest {
                    None => self.stmt.bind_null(index),
                    Some(value) => self.stmt.bind_text(index, &*value, Destructor::STATIC),
                }?;
            }
        }

        Ok(())
    }

    pub fn bind_for_delete(&self, id: &str) -> Result<()> {
        for (i, source) in self.definition.params.iter().enumerate() {
            if let PendingStatementValue::Id = source {
                self.stmt
                    .bind_text((i + 1) as i32, id, Destructor::STATIC)?;
            } else {
                return Err(PowerSyncError::argument_error(
                    "Raw delete statement parameters must only reference id",
                ));
            }
        }

        Ok(())
    }

    /// Executes the prepared statement, contextualizing errors with the id / data that we've tried
    /// to insert.
    pub fn exec(&self, table: &str, id: &str, data: Option<&str>) -> Result<()> {
        self.stmt.exec().map_err(|e| {
            let context = match data {
                None => format!("deleting from {table}, id = {id}"),
                Some(data) => format!("replacing into {table}, id = {id}, data = {data}"),
            };

            e.context(context)
        })
    }
}
