extern crate alloc;

use alloc::borrow::ToOwned;
use alloc::collections::btree_map::BTreeMap;
use alloc::rc::Rc;
use alloc::string::String;
use alloc::vec::Vec;
use alloc::{format, vec};
use core::ffi::c_int;

use powersync_sqlite_nostd as sqlite;
use powersync_sqlite_nostd::Context;
use sqlite::{Connection, ResultCode, Value};

use crate::error::{PSResult, PowerSyncError};
use crate::ext::ExtendedDatabase;
use crate::schema::inspection::{ExistingTable, ExistingView};
use crate::state::DatabaseState;
use crate::util::{quote_identifier, quote_json_path};
use crate::views::{
    powersync_trigger_delete_sql, powersync_trigger_insert_sql, powersync_trigger_update_sql,
    powersync_view_sql,
};
use crate::{create_auto_tx_function, create_sqlite_text_fn};

use super::Schema;

fn update_tables(db: *mut sqlite::sqlite3, schema: &Schema) -> Result<(), PowerSyncError> {
    let existing_tables = ExistingTable::list(db)?;
    let mut existing_tables = {
        let mut map = BTreeMap::new();
        for table in &existing_tables {
            map.insert(&*table.name, table);
        }
        map
    };

    {
        // In a block so that all statements are finalized before dropping tables.
        for table in &schema.tables {
            if let Some(_) = existing_tables.remove(&*table.name) {
                // This table exists already, nothing to do.
                // TODO: Handle switch between local only <-> regular tables?
            } else {
                // New table.
                let quoted_internal_name = quote_identifier(&table.internal_name());

                db.exec_safe(&format!(
                    "CREATE TABLE {:}(id TEXT PRIMARY KEY NOT NULL, data TEXT)",
                    quoted_internal_name
                ))
                .into_db_result(db)?;

                if !table.local_only() {
                    // MOVE data if any
                    db.exec_text(
                        &format!(
                            "INSERT INTO {:}(id, data)
    SELECT id, data
    FROM ps_untyped
    WHERE type = ?",
                            quoted_internal_name
                        ),
                        &table.name,
                    )
                    .into_db_result(db)?;

                    // language=SQLite
                    db.exec_text("DELETE FROM ps_untyped WHERE type = ?", &table.name)?;
                }
            }
        }

        // Remaining tables need to be dropped. But first, we want to move their contents to
        // ps_untyped.
        for remaining in existing_tables.values() {
            if !remaining.local_only {
                db.exec_text(
                    &format!(
                        "INSERT INTO ps_untyped(type, id, data) SELECT ?, id, data FROM {:}",
                        quote_identifier(&remaining.internal_name)
                    ),
                    &remaining.name,
                )
                .into_db_result(db)?;
            }
        }
    }

    // We cannot have any open queries on sqlite_master at the point that we drop tables, otherwise
    // we get "table is locked" errors.
    for remaining in existing_tables.values() {
        let q = format!("DROP TABLE {:}", quote_identifier(&remaining.internal_name));
        db.exec_safe(&q).into_db_result(db)?;
    }

    Ok(())
}

fn update_indexes(db: *mut sqlite::sqlite3, schema: &Schema) -> Result<(), PowerSyncError> {
    let mut statements: Vec<String> = alloc::vec![];
    let mut expected_index_names: Vec<String> = vec![];

    {
        // In a block so that the statement is finalized before dropping indexes
        // language=SQLite
        let find_index =
            db.prepare_v2("SELECT sql FROM sqlite_master WHERE name = ? AND type = 'index'")?;

        for table in &schema.tables {
            let table_name = table.internal_name();

            for index in &table.indexes {
                let index_name = format!("{}__{}", table_name, &index.name);

                let existing_sql = {
                    find_index.reset()?;
                    find_index.bind_text(1, &index_name, sqlite::Destructor::STATIC)?;

                    let result = if let ResultCode::ROW = find_index.step()? {
                        Some(find_index.column_text(0)?)
                    } else {
                        None
                    };

                    result
                };

                let mut column_values: Vec<String> = alloc::vec![];
                for indexed_column in &index.columns {
                    let mut value = format!(
                        "CAST(json_extract(data, {:}) as {:})",
                        quote_json_path(&indexed_column.name),
                        &indexed_column.type_name
                    );

                    if !indexed_column.ascending {
                        value += " DESC";
                    }

                    column_values.push(value);
                }

                let sql = format!(
                    "CREATE INDEX {} ON {}({})",
                    quote_identifier(&index_name),
                    quote_identifier(&table_name),
                    column_values.join(", ")
                );

                if existing_sql.is_none() {
                    statements.push(sql);
                } else if existing_sql != Some(&sql) {
                    statements.push(format!("DROP INDEX {}", quote_identifier(&index_name)));
                    statements.push(sql);
                }

                expected_index_names.push(index_name);
            }
        }

        // In a block so that the statement is finalized before dropping indexes
        // language=SQLite
        let statement = db
            .prepare_v2(
                "\
SELECT
    sqlite_master.name as index_name
      FROM sqlite_master
          WHERE sqlite_master.type = 'index'
            AND sqlite_master.name GLOB 'ps_data_*'
            AND sqlite_master.name NOT IN (SELECT value FROM json_each(?))
",
            )
            .into_db_result(db)?;
        let json_names = serde_json::to_string(&expected_index_names)
            .map_err(PowerSyncError::as_argument_error)?;
        statement.bind_text(1, &json_names, sqlite::Destructor::STATIC)?;

        while statement.step()? == ResultCode::ROW {
            let name = statement.column_text(0)?;

            statements.push(format!("DROP INDEX {}", quote_identifier(name)));
        }
    }

    // We cannot have any open queries on sqlite_master at the point that we drop indexes, otherwise
    // we get "database table is locked (code 6)" errors.
    for statement in statements {
        db.exec_safe(&statement).into_db_result(db)?;
    }

    Ok(())
}

fn update_views(db: *mut sqlite::sqlite3, schema: &Schema) -> Result<(), PowerSyncError> {
    // First, find all existing views and index them by name.
    let existing = ExistingView::list(db)?;
    let mut existing = {
        let mut map = BTreeMap::new();
        for entry in &existing {
            map.insert(&*entry.name, entry);
        }
        map
    };

    for table in &schema.tables {
        let view_sql = powersync_view_sql(table);
        let delete_trigger_sql = powersync_trigger_delete_sql(table)?;
        let insert_trigger_sql = powersync_trigger_insert_sql(table)?;
        let update_trigger_sql = powersync_trigger_update_sql(table)?;

        let wanted_view = ExistingView {
            name: table.view_name().to_owned(),
            sql: view_sql,
            delete_trigger_sql,
            insert_trigger_sql,
            update_trigger_sql,
        };

        if let Some(actual_view) = existing.remove(table.view_name()) {
            if *actual_view == wanted_view {
                // View exists with identical definition, don't re-create.
                continue;
            }
        }

        // View does not exist or has been defined differently, re-create.
        wanted_view.create(db)?;
    }

    // Delete old views.
    for remaining in existing.values() {
        ExistingView::drop_by_name(db, &remaining.name)?;
    }

    Ok(())
}

// SELECT powersync_replace_schema('{"tables": [{"name": "test", "columns": [{"name": "name", "type": "text"}]}]}');
// This cannot be a TRIGGER or a virtual table insert. There are locking issues due to both
// querying sqlite_master and dropping tables in those cases, which are not present when this is
// a plain function.
fn powersync_replace_schema_impl(
    ctx: *mut sqlite::context,
    args: &[*mut sqlite::value],
) -> Result<String, PowerSyncError> {
    let schema = args[0].text();
    let state = unsafe { DatabaseState::from_context(&ctx) };
    let parsed_schema =
        serde_json::from_str::<Schema>(schema).map_err(PowerSyncError::as_argument_error)?;

    let db = ctx.db_handle();

    // language=SQLite
    db.exec_safe("SELECT powersync_init()").into_db_result(db)?;

    update_tables(db, &parsed_schema)?;
    update_indexes(db, &parsed_schema)?;
    update_views(db, &parsed_schema)?;

    state.set_schema(parsed_schema);
    Ok(String::from(""))
}

create_auto_tx_function!(powersync_replace_schema_tx, powersync_replace_schema_impl);
create_sqlite_text_fn!(
    powersync_replace_schema,
    powersync_replace_schema_tx,
    "powersync_replace_schema"
);

pub fn register(db: *mut sqlite::sqlite3, state: Rc<DatabaseState>) -> Result<(), ResultCode> {
    db.create_function_v2(
        "powersync_replace_schema",
        1,
        sqlite::UTF8,
        Some(Rc::into_raw(state) as *mut _),
        Some(powersync_replace_schema),
        None,
        None,
        Some(DatabaseState::destroy_rc),
    )?;

    Ok(())
}
