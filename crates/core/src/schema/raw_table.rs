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
use powersync_sqlite_nostd::Destructor;

use crate::{
    error::{PowerSyncError, Result},
    schema::{ColumnFilter, PendingStatement, RawTable, SchemaTable},
    utils::{InsertIntoCrud, SqlBuffer, WriteType, database::Database},
    views::table_columns_to_json_object,
};

pub struct InferredTableStructure {
    pub columns: Vec<String>,
}

impl InferredTableStructure {
    pub fn read_from_database(
        table_name: &str,
        db: Database,
        synced_columns: &Option<ColumnFilter>,
    ) -> Result<Self> {
        let stmt = db.prepare_v2("select name from pragma_table_info(?)")?;
        stmt.bind_text(1, table_name, Destructor::STATIC)?;

        let mut has_id_column = false;
        let mut columns = vec![];

        while stmt.step()? {
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
            Ok(Self { columns })
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
    pub fn current_schema_version(db: Database) -> Result<usize> {
        let version = db.prepare_v2("PRAGMA schema_version")?;
        version.step()?;
        let version = version.column_int64(0) as usize;
        Ok(version)
    }

    pub fn infer_put_statement(
        &self,
        db: Database,
        schema_version: usize,
        tbl: &RawTable,
    ) -> Result<Rc<PendingStatement>> {
        self.with_entry(db, schema_version, tbl, |entry| entry.put_stmt.clone())
    }

    pub fn infer_delete_statement(
        &self,
        db: Database,
        schema_version: usize,
        tbl: &RawTable,
    ) -> Result<Rc<PendingStatement>> {
        self.with_entry(db, schema_version, tbl, |entry| entry.delete_stmt.clone())
    }

    fn with_entry(
        &self,
        db: Database,
        schema_version: usize,
        tbl: &RawTable,
        f: impl FnOnce(&mut SchemaCacheEntry) -> Rc<PendingStatement>,
    ) -> Result<Rc<PendingStatement>> {
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
    pub put_stmt: Rc<PendingStatement>,
    pub delete_stmt: Rc<PendingStatement>,
}

impl SchemaCacheEntry {
    fn infer(db: Database, schema_version: usize, table: &RawTable) -> Result<Self> {
        let local_table_name = table.require_table_name()?;
        let structure = InferredTableStructure::read_from_database(
            local_table_name,
            db,
            &table.schema.synced_columns,
        )?;
        let schema_table = SchemaTable::Raw {
            definition: table,
            schema: &structure,
        };

        Ok(Self {
            schema_version,
            put_stmt: Rc::new(schema_table.infer_put_stmt(local_table_name)),
            delete_stmt: Rc::new(schema_table.infer_delete_stmt(local_table_name)),
        })
    }
}

/// Generates a `CREATE TRIGGER` statement to capture writes on raw tables and to forward them to
/// ps-crud.
pub fn generate_raw_table_trigger(
    db: Database,
    table: &RawTable,
    trigger_name: &str,
    write: WriteType,
) -> Result<String> {
    let local_table_name = table.require_table_name()?;
    let synced_columns = &table.schema.synced_columns;
    let resolved_table =
        InferredTableStructure::read_from_database(local_table_name, db, synced_columns)?;

    let as_schema_table = SchemaTable::Raw {
        definition: table,
        schema: &resolved_table,
    };

    generate_schema_table_trigger(
        local_table_name,
        as_schema_table,
        synced_columns.as_ref(),
        trigger_name,
        write,
    )
}

pub fn generate_schema_table_trigger(
    local_table_name: &str,
    table: SchemaTable,
    synced_columns: Option<&ColumnFilter>,
    trigger_name: &str,
    write: WriteType,
) -> Result<String> {
    let mut buffer = SqlBuffer::new();
    buffer.create_trigger("", trigger_name);
    buffer.trigger_after(write, local_table_name);
    // Skip the trigger for writes during sync_local, these aren't crud writes.
    buffer.push_str("WHEN NOT powersync_in_sync_operation()");

    if write == WriteType::Update && synced_columns.is_some() {
        buffer.push_str(" AND\n(");
        // If we have a filter for synced columns (instead of syncing all of them), we want to add
        // additional WHEN clauses to enesure the trigger runs for updates on those columns only.
        for (i, name) in table.column_names().enumerate() {
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

    if table.common_options().flags.insert_only() {
        if write != WriteType::Insert {
            // Prevent illegal writes to a table marked as insert-only by raising errors here.
            buffer.push_str("SELECT RAISE(FAIL, 'Unexpected update on insert-only table');\n");
        } else {
            // Insert-only tables use manual CRUD writes so they don't block incoming data.
            let fragment = table_columns_to_json_object("NEW", &table)?;
            buffer.powersync_crud_manual_put(table.name(), &fragment);
        }
    } else {
        if write == WriteType::Update {
            // Updates must not change the id.
            buffer.check_id_not_changed();
        }

        let json_fragment_new = table_columns_to_json_object("NEW", &table)?;
        let json_fragment_old = if write == WriteType::Update {
            Some(table_columns_to_json_object("OLD", &table)?)
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

        if write == WriteType::Update
            && let Some(data_column) = table.data_column()
        {
            // If the table has a __data column storing the full JSON row, we also need to update
            // that.
            let _ = write!(
                &mut buffer,
                "UPDATE {local_table_name} SET {data_column} = {json_fragment_new} WHERE id = NEW.id;\n"
            );
        }

        buffer.insert_into_powersync_crud(InsertIntoCrud {
            op: write,
            table: &table,
            id_expr: if write == WriteType::Delete {
                "OLD.id"
            } else {
                "NEW.id"
            },
            type_name: table.name(),
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
