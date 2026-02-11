use core::fmt::{self, Formatter, Write, from_fn};

use alloc::{
    format,
    string::{String, ToString},
    vec,
    vec::Vec,
};
use powersync_sqlite_nostd::{Connection, Destructor, ResultCode};

use crate::{
    error::PowerSyncError,
    schema::{ColumnFilter, RawTable, SchemaTable},
    utils::{InsertIntoCrud, SqlBuffer, WriteType},
    views::table_columns_to_json_object,
};

pub struct InferredTableStructure {
    pub columns: Vec<String>,
}

impl InferredTableStructure {
    pub fn read_from_database(
        table_name: &str,
        db: impl Connection,
        ignored_local_columns: &ColumnFilter,
    ) -> Result<Option<Self>, PowerSyncError> {
        let stmt = db.prepare_v2("select name from pragma_table_info(?)")?;
        stmt.bind_text(1, table_name, Destructor::STATIC)?;

        let mut has_id_column = false;
        let mut columns = vec![];

        while let ResultCode::ROW = stmt.step()? {
            let name = stmt.column_text(0)?;
            if name == "id" {
                has_id_column = true;
            } else if !ignored_local_columns.matches(name) {
                columns.push(name.to_string());
            }
        }

        if !has_id_column && columns.is_empty() {
            Ok(None)
        } else if !has_id_column {
            Err(PowerSyncError::argument_error(format!(
                "Table {table_name} has no id column."
            )))
        } else {
            Ok(Some(Self { columns }))
        }
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
    let Some(local_table_name) = table.schema.table_name.as_ref() else {
        return Err(PowerSyncError::argument_error("Table has no local name"));
    };

    let local_only_columns = &table.schema.local_only_columns;
    let Some(resolved_table) =
        InferredTableStructure::read_from_database(local_table_name, db, local_only_columns)?
    else {
        return Err(PowerSyncError::argument_error(format!(
            "Could not find {} in local schema",
            local_table_name
        )));
    };

    let as_schema_table = SchemaTable::Raw {
        definition: table,
        schema: &resolved_table,
    };

    let mut buffer = SqlBuffer::new();
    buffer.create_trigger("", trigger_name);
    buffer.trigger_after(write, local_table_name);
    // Skip the trigger for writes during sync_local, these aren't crud writes.
    buffer.push_str("WHEN NOT powersync_in_sync_operation()");

    if write == WriteType::Update && !local_only_columns.as_ref().is_empty() {
        buffer.push_str(" AND\n(");
        // If we have local-only columns, we want to add additional WHEN clauses to ensure the
        // trigger runs for updates on synced columns.
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
