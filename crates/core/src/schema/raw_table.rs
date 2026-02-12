use core::{
    cell::RefCell,
    fmt::{self, Formatter, Write, from_fn},
};

use alloc::{
    collections::btree_map::BTreeMap,
    format,
    rc::Rc,
    string::{String, ToString},
    vec,
    vec::Vec,
};
use powersync_sqlite_nostd::{self as sqlite, Connection, Destructor, ResultCode};

use crate::{
    error::PowerSyncError,
    schema::{ColumnFilter, PendingStatement, PendingStatementValue, RawTable, SchemaTable},
    utils::{InsertIntoCrud, SqlBuffer, WriteType},
    views::table_columns_to_json_object,
};

pub struct InferredTableStructure {
    pub name: String,
    pub columns: Vec<String>,
}

impl InferredTableStructure {
    pub fn read_from_database(
        table_name: &str,
        db: impl Connection,
        synced_columns: &Option<ColumnFilter>,
    ) -> Result<Self, PowerSyncError> {
        let stmt = db.prepare_v2("select name from pragma_table_info(?)")?;
        stmt.bind_text(1, table_name, Destructor::STATIC)?;

        let mut has_id_column = false;
        let mut columns = vec![];

        while let ResultCode::ROW = stmt.step()? {
            let name = stmt.column_text(0)?;
            if name == "id" {
                has_id_column = true;
            } else if let Some(filter) = synced_columns
                && !filter.matches(name)
            {
                // This column isn't part of the synced columns, skip.
            } else {
                columns.push(name.to_string());
            }
        }

        if !has_id_column && columns.is_empty() {
            Err(PowerSyncError::argument_error(format!(
                "Could not find {table_name} in local schema."
            )))
        } else if !has_id_column {
            Err(PowerSyncError::argument_error(format!(
                "Table {table_name} has no id column."
            )))
        } else {
            Ok(Self {
                name: table_name.to_string(),
                columns,
            })
        }
    }

    /// Generates a statement of the form `INSERT OR REPLACE INTO $tbl ($cols) VALUES (?, ...)` for
    /// the sync client.
    pub fn infer_put_stmt(&self) -> PendingStatement {
        let mut buffer = SqlBuffer::new();
        let mut params = vec![];

        buffer.push_str("INSERT OR REPLACE INTO ");
        let _ = buffer.identifier().write_str(&self.name);
        buffer.push_str(" (id");
        for column in &self.columns {
            buffer.comma();
            let _ = buffer.identifier().write_str(column);
        }
        buffer.push_str(") VALUES (?");
        params.push(PendingStatementValue::Id);
        for column in &self.columns {
            buffer.comma();
            buffer.push_str("?");
            params.push(PendingStatementValue::Column(column.clone()));
        }
        buffer.push_str(")");

        PendingStatement {
            sql: buffer.sql,
            params,
            named_parameters_index: None,
        }
    }

    /// Generates a statement of the form `DELETE FROM $tbl WHERE id = ?` for the sync client.
    pub fn infer_delete_stmt(&self) -> PendingStatement {
        let mut buffer = SqlBuffer::new();
        buffer.push_str("DELETE FROM ");
        let _ = buffer.identifier().write_str(&self.name);
        buffer.push_str(" WHERE id = ?");

        PendingStatement {
            sql: buffer.sql,
            params: vec![PendingStatementValue::Id],
            named_parameters_index: None,
        }
    }
}

/// A cache of inferred raw table schema and associated put and delete statements for `sync_local`.
///
/// This cache avoids having to re-generate statements on every (partial) checkpoint in the sync
/// client.
#[derive(Default)]
pub struct InferredSchemaCache {
    entries: RefCell<BTreeMap<String, SchemaCacheEntry>>,
}

impl InferredSchemaCache {
    pub fn current_schema_version(db: *mut sqlite::sqlite3) -> Result<usize, PowerSyncError> {
        let version = db.prepare_v2("PRAGMA schema_version")?;
        version.step()?;
        let version = version.column_int64(0) as usize;
        Ok(version)
    }

    pub fn infer_put_statement(
        &self,
        db: *mut sqlite::sqlite3,
        schema_version: usize,
        tbl: &RawTable,
    ) -> Result<Rc<PendingStatement>, PowerSyncError> {
        self.with_entry(db, schema_version, tbl, SchemaCacheEntry::put)
    }

    pub fn infer_delete_statement(
        &self,
        db: *mut sqlite::sqlite3,
        schema_version: usize,
        tbl: &RawTable,
    ) -> Result<Rc<PendingStatement>, PowerSyncError> {
        self.with_entry(db, schema_version, tbl, SchemaCacheEntry::delete)
    }

    fn with_entry(
        &self,
        db: *mut sqlite::sqlite3,
        schema_version: usize,
        tbl: &RawTable,
        f: impl FnOnce(&mut SchemaCacheEntry) -> Rc<PendingStatement>,
    ) -> Result<Rc<PendingStatement>, PowerSyncError> {
        let mut entries = self.entries.borrow_mut();
        if let Some(value) = entries.get_mut(&tbl.name) {
            if value.schema_version != schema_version {
                // Values are outdated, refresh.
                *value = SchemaCacheEntry::infer(db, schema_version, tbl)?;
            }

            Ok(f(value))
        } else {
            let mut entry = SchemaCacheEntry::infer(db, schema_version, tbl)?;
            let stmt = f(&mut entry);
            entries.insert(tbl.name.clone(), entry);
            Ok(stmt)
        }
    }
}

pub struct SchemaCacheEntry {
    schema_version: usize,
    structure: InferredTableStructure,
    put_stmt: Option<Rc<PendingStatement>>,
    delete_stmt: Option<Rc<PendingStatement>>,
}

impl SchemaCacheEntry {
    fn infer(
        db: *mut sqlite::sqlite3,
        schema_version: usize,
        table: &RawTable,
    ) -> Result<Self, PowerSyncError> {
        let local_table_name = table.require_table_name()?;
        let structure = InferredTableStructure::read_from_database(
            local_table_name,
            db,
            &table.schema.synced_columns,
        )?;

        Ok(Self {
            schema_version,
            structure,
            put_stmt: None,
            delete_stmt: None,
        })
    }

    fn put(&mut self) -> Rc<PendingStatement> {
        self.put_stmt
            .get_or_insert_with(|| Rc::new(self.structure.infer_put_stmt()))
            .clone()
    }

    fn delete(&mut self) -> Rc<PendingStatement> {
        self.delete_stmt
            .get_or_insert_with(|| Rc::new(self.structure.infer_delete_stmt()))
            .clone()
    }
}

/// Generates a `CREATE TRIGGER` statement to capture writes on raw tables and to forward them to
/// ps-crud.
pub fn generate_raw_table_trigger(
    db: impl Connection,
    table: &RawTable,
    trigger_name: &str,
    write: WriteType,
) -> Result<String, PowerSyncError> {
    let local_table_name = table.require_table_name()?;
    let synced_columns = &table.schema.synced_columns;
    let resolved_table =
        InferredTableStructure::read_from_database(local_table_name, db, synced_columns)?;

    let as_schema_table = SchemaTable::Raw {
        definition: table,
        schema: &resolved_table,
    };

    let mut buffer = SqlBuffer::new();
    buffer.create_trigger("", trigger_name);
    buffer.trigger_after(write, local_table_name);
    // Skip the trigger for writes during sync_local, these aren't crud writes.
    buffer.push_str("WHEN NOT powersync_in_sync_operation()");

    if write == WriteType::Update && synced_columns.is_some() {
        buffer.push_str(" AND\n(");
        // If we have a filter for synced columns (instead of syncing all of them), we want to add
        // additional WHEN clauses to enesure the trigger runs for updates on those columns only.
        for (i, name) in as_schema_table.column_names().enumerate() {
            if i != 0 {
                buffer.push_str(" OR ");
            }

            // Generate OLD."column" IS NOT NEW."column"
            buffer.push_str("OLD.");
            let _ = buffer.identifier().write_str(name);
            buffer.push_str(" IS NOT NEW.");
            let _ = buffer.identifier().write_str(name);
        }
        buffer.push_str(")");
    }

    buffer.push_str(" BEGIN\n");

    if table.schema.options.flags.insert_only() {
        if write != WriteType::Insert {
            // Prevent illegal writes to a table marked as insert-only by raising errors here.
            buffer.push_str("SELECT RAISE(FAIL, 'Unexpected update on insert-only table');\n");
        } else {
            // Write directly to powersync_crud_ to skip writing the $local bucket for insert-only
            // tables.
            let fragment = table_columns_to_json_object("NEW", &as_schema_table)?;
            buffer.powersync_crud_manual_put(&table.name, &fragment);
        }
    } else {
        if write == WriteType::Update {
            // Updates must not change the id.
            buffer.check_id_not_changed();
        }

        let json_fragment_new = table_columns_to_json_object("NEW", &as_schema_table)?;
        let json_fragment_old = if write == WriteType::Update {
            Some(table_columns_to_json_object("OLD", &as_schema_table)?)
        } else {
            None
        };

        let write_data = from_fn(|f: &mut Formatter| -> fmt::Result {
            write!(f, "json(powersync_diff(")?;

            if let Some(ref old) = json_fragment_old {
                f.write_str(old)?;
            } else {
                // We don't have OLD values for inserts, we diff from an empty JSON object
                // instead.
                f.write_str("'{}'")?;
            };

            write!(f, ", {json_fragment_new}))")
        });

        buffer.insert_into_powersync_crud(InsertIntoCrud {
            op: write,
            table: &as_schema_table,
            id_expr: if write == WriteType::Delete {
                "OLD.id"
            } else {
                "NEW.id"
            },
            type_name: &table.name,
            data: match write {
                // There is no data for deleted rows.
                WriteType::Delete => None,
                _ => Some(&write_data),
            },
            metadata: None::<&'static str>,
        })?;
    }

    buffer.trigger_end();
    Ok(buffer.sql)
}
