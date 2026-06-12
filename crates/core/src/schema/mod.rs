mod common;
pub mod inspection;
mod management;
mod raw_table;
mod table_info;

use core::{ffi::c_void, fmt::Write};

use alloc::{format, rc::Rc, vec::Vec};
pub use common::{ColumnFilter, SchemaTable};
use powersync_sqlite_nostd::{self as sqlite, Connection, Context, Value, args};
pub use raw_table::InferredSchemaCache;
use serde::Deserialize;
use sqlite::ResultCode;
pub use table_info::{
    Column, CommonTableOptions, PendingStatement, PendingStatementValue, RawTable, Table,
    TableInfoFlags,
};

use crate::{
    error::{PSResult, PowerSyncError},
    ext::SafeManagedStmt,
    schema::raw_table::{InferredTableStructure, generate_raw_table_trigger},
    state::DatabaseState,
    sync::RawTableWithCachedStatements,
    utils::{SqlBuffer, WriteType},
    views::table_columns_to_json_object,
};

#[derive(Deserialize, Default)]
pub struct Schema {
    pub tables: Vec<table_info::Table>,
    #[serde(default)]
    pub raw_tables: Vec<table_info::RawTable>,
}

impl Schema {
    pub fn find_raw_table(&self, name: &str) -> Option<&table_info::RawTable> {
        self.raw_tables.iter().find(|tbl| tbl.name == name)
    }
}

fn create_trigger(
    context: *mut sqlite::context,
    args: &[*mut sqlite::value],
) -> Result<(), PowerSyncError> {
    // Args: Table (JSON), trigger_name, write_type
    let table: RawTable =
        serde_json::from_str(args[0].text()).map_err(PowerSyncError::as_argument_error)?;
    let trigger_name = args[1].text();
    let write_type: WriteType = args[2].text().parse()?;

    let db = context.db_handle();
    let create_trigger_stmt = generate_raw_table_trigger(db, &table, trigger_name, write_type)?;
    db.exec_safe(&create_trigger_stmt).into_db_result(db)?;
    Ok(())
}

enum RawTableMigration<'a> {
    CreateFromUntyped { table_name: &'a str },
    DropIntoUntyped { table: RawTable },
}

impl<'a> RawTableMigration<'a> {
    fn from_args(args: &'a [*mut sqlite::value]) -> Result<Self, PowerSyncError> {
        match args[0].text() {
            "create" => Ok(RawTableMigration::CreateFromUntyped {
                table_name: args[1].text(),
            }),
            "drop" => {
                let table: RawTable = serde_json::from_str(args[1].text())
                    .map_err(PowerSyncError::as_argument_error)?;
                Ok(RawTableMigration::DropIntoUntyped { table })
            }
            _ => Err(PowerSyncError::argument_error(
                "Unknown action for powersync_raw_table_migrate",
            )),
        }
    }
}

/// Utility to help with migrations involving raw tables.
///
/// `powersync_raw_table_migrate('create', $name)` moves existing rows matching the table from
/// `ps_untyped` into the raw table (to be called after adding the raw table to the schema).
///
/// `powersync_raw_table_migrate('drop', $json)` moves rows from the raw table back into
/// `ps_untyped` (call before removing the raw table from the schema), where `$json` is a JSON
/// representation of the [`RawTable`] being dropped.
fn raw_table_migration(
    context: *mut sqlite::context,
    args: &[*mut sqlite::value],
) -> Result<(), PowerSyncError> {
    let db = context.db_handle();
    let context = unsafe { DatabaseState::from_context(&context) };
    let action = RawTableMigration::from_args(args)?;

    // Disable triggers forwarding writes to ps_crud.
    let _guard = context.sync_local_guard();

    let schema_version = InferredSchemaCache::current_schema_version(db)?;

    match action {
        RawTableMigration::CreateFromUntyped { table_name } => {
            // Move data from ps_untyped into this raw table.
            let Some(schema) = context.view_schema() else {
                return Err(PowerSyncError::state_error("Schema not initialized"));
            };

            let Some(table) = schema.find_raw_table(table_name) else {
                return Err(PowerSyncError::argument_error(format!(
                    "Raw table {table_name} not found"
                )));
            };
            let delete_untyped =
                db.prepare_v2("DELETE FROM ps_untyped WHERE type = ? RETURNING id, data")?;
            delete_untyped.bind_text(1, table_name, powersync_sqlite_nostd::Destructor::STATIC)?;
            let mut table_statements = RawTableWithCachedStatements::new(table);

            while let ResultCode::ROW = delete_untyped.step()? {
                let put = table_statements.put_statement(
                    db,
                    schema_version,
                    &context.inferred_schema_cache,
                )?;

                let id = delete_untyped.column_text(0)?;
                let data_text = delete_untyped.column_text(1)?;
                let parsed: serde_json::Value =
                    serde_json::from_str(data_text).map_err(PowerSyncError::json_local_error)?;
                let json_object = parsed.as_object().ok_or_else(|| {
                    PowerSyncError::argument_error("expected oplog data to be an object")
                })?;

                let rest = put.render_rest_object(&json_object)?;
                put.bind_for_put(id, &json_object, &rest)?;
                put.exec(db, table_name, id, Some(&parsed))?;
            }
        }
        RawTableMigration::DropIntoUntyped { table } => {
            // Copy data from the raw table into ps_untyped, ignoring local-only columns.
            let local_table_name = table.require_table_name()?;
            let resolved_table = InferredTableStructure::read_from_database(
                local_table_name,
                db,
                &table.schema.synced_columns,
            )?;
            let as_schema_table = SchemaTable::Raw {
                definition: &table,
                schema: &resolved_table,
            };

            let sql = {
                // Generate an INSERT INTO ps_untyped with a SELECT source transforming existing
                // rows into JSON objects.
                let mut buffer = SqlBuffer::new();
                let fragment = table_columns_to_json_object(&local_table_name, &as_schema_table)?;
                buffer.push_str("INSERT INTO ps_untyped (type, id, data) SELECT ?, id, ");
                buffer.push_str(&fragment);
                buffer.push_str(" FROM ");
                let _ = buffer.identifier().write_str(local_table_name);
                buffer.push_char(';');
                buffer.sql
            };
            {
                let stmt = db.prepare_v2(&sql).into_db_result(db)?;
                stmt.bind_text(1, &table.name, powersync_sqlite_nostd::Destructor::STATIC)?;
                stmt.exec().into_db_result(db)?;
            }

            // Then, delete raw table contents.
            {
                let mut truncate_buf = SqlBuffer::new();
                truncate_buf.push_str("DELETE FROM ");
                let _ = truncate_buf.identifier().write_str(local_table_name);
                db.exec_safe(&truncate_buf.sql).into_db_result(db)?;
            }
        }
    }

    Ok(())
}

pub fn register(db: *mut sqlite::sqlite3, state: Rc<DatabaseState>) -> Result<(), ResultCode> {
    management::register(db, state.clone())?;

    {
        extern "C" fn create_raw_trigger_sqlite(
            context: *mut sqlite::context,
            argc: i32,
            args: *mut *mut sqlite::value,
        ) {
            let args = args!(argc, args);
            if let Err(e) = create_trigger(context, args) {
                e.apply_to_ctx("powersync_create_raw_table_crud_trigger", context);
            }
        }

        extern "C" fn raw_table_migrate_sqlite(
            context: *mut sqlite::context,
            argc: i32,
            args: *mut *mut sqlite::value,
        ) {
            let args = args!(argc, args);
            if let Err(e) = raw_table_migration(context, args) {
                e.apply_to_ctx("powersync_raw_table_migrate", context);
            }
        }

        db.create_function_v2(
            "powersync_create_raw_table_crud_trigger",
            3,
            sqlite::UTF8,
            None,
            Some(create_raw_trigger_sqlite),
            None,
            None,
            None,
        )?;

        db.create_function_v2(
            "powersync_raw_table_migrate",
            2,
            sqlite::UTF8,
            Some(Rc::into_raw(state) as *mut c_void),
            Some(raw_table_migrate_sqlite),
            None,
            None,
            Some(DatabaseState::destroy_rc),
        )?;
    }

    Ok(())
}
