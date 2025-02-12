extern crate alloc;

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::ffi::c_int;
use core::slice;

use sqlite::{ResultCode, Value};
use sqlite_nostd as sqlite;
use sqlite_nostd::{Connection, Context};

use crate::error::SQLiteError;
use crate::migrations::powersync_migrate;
use crate::util::quote_identifier;
use crate::{create_auto_tx_function, create_sqlite_text_fn};

fn powersync_drop_view_impl(
    ctx: *mut sqlite::context,
    args: &[*mut sqlite::value],
) -> Result<String, ResultCode> {
    let name = args[0].text();

    let local_db = ctx.db_handle();
    let q = format!("DROP VIEW IF EXISTS {:}", quote_identifier(name));
    let stmt2 = local_db.prepare_v2(&q)?;

    if stmt2.step()? == ResultCode::ROW {
        Ok(String::from(name))
    } else {
        Ok(String::from(""))
    }
}

create_sqlite_text_fn!(
    powersync_drop_view,
    powersync_drop_view_impl,
    "powersync_drop_view"
);

fn powersync_exec_impl(
    ctx: *mut sqlite::context,
    args: &[*mut sqlite::value],
) -> Result<String, ResultCode> {
    let q = args[0].text();

    if q != "" {
        let local_db = ctx.db_handle();
        local_db.exec_safe(q)?;
    }

    Ok(String::from(""))
}

create_sqlite_text_fn!(powersync_exec, powersync_exec_impl, "powersync_exec");

fn powersync_internal_table_name_impl(
    ctx: *mut sqlite::context,
    args: &[*mut sqlite::value],
) -> Result<String, ResultCode> {
    // schema: JSON
    let schema = args[0].text();

    let local_db = ctx.db_handle();

    // language=SQLite
    let stmt1 = local_db.prepare_v2(
        "SELECT json_extract(?1, '$.name') as name, ifnull(json_extract(?1, '$.local_only'), 0)",
    )?;
    stmt1.bind_text(1, schema, sqlite::Destructor::STATIC)?;

    let step_result = stmt1.step()?;
    if step_result != ResultCode::ROW {
        return Err(ResultCode::SCHEMA);
    }

    let name = stmt1.column_text(0)?;
    let local_only = stmt1.column_int(1)? != 0;

    if local_only {
        Ok(format!("ps_data_local__{:}", name))
    } else {
        Ok(format!("ps_data__{:}", name))
    }
}

create_sqlite_text_fn!(
    powersync_internal_table_name,
    powersync_internal_table_name_impl,
    "powersync_internal_table_name"
);

fn powersync_external_table_name_impl(
    _ctx: *mut sqlite::context,
    args: &[*mut sqlite::value],
) -> Result<String, SQLiteError> {
    // name: full table name
    let name = args[0].text();

    if name.starts_with("ps_data_local__") {
        Ok(String::from(&name[15..]))
    } else if name.starts_with("ps_data__") {
        Ok(String::from(&name[9..]))
    } else {
        Err(SQLiteError::from(ResultCode::CONSTRAINT_DATATYPE))
    }
}

create_sqlite_text_fn!(
    powersync_external_table_name,
    powersync_external_table_name_impl,
    "powersync_external_table_name"
);

fn powersync_init_impl(
    ctx: *mut sqlite::context,
    _args: &[*mut sqlite::value],
) -> Result<String, SQLiteError> {
    let local_db = ctx.db_handle();

    setup_internal_views(local_db)?;

    powersync_migrate(ctx, 6)?;

    Ok(String::from(""))
}

create_auto_tx_function!(powersync_init_tx, powersync_init_impl);
create_sqlite_text_fn!(powersync_init, powersync_init_tx, "powersync_init");

fn powersync_test_migration_impl(
    ctx: *mut sqlite::context,
    args: &[*mut sqlite::value],
) -> Result<String, SQLiteError> {
    let target_version = args[0].int();
    powersync_migrate(ctx, target_version)?;

    Ok(String::from(""))
}

create_auto_tx_function!(powersync_test_migration_tx, powersync_test_migration_impl);
create_sqlite_text_fn!(
    powersync_test_migration,
    powersync_test_migration_tx,
    "powersync_test_migration"
);

fn powersync_clear_impl(
    ctx: *mut sqlite::context,
    args: &[*mut sqlite::value],
) -> Result<String, SQLiteError> {
    let local_db = ctx.db_handle();

    let clear_local = args[0].int();

    // language=SQLite
    local_db.exec_safe(
        "\
DELETE FROM ps_oplog;
DELETE FROM ps_crud;
DELETE FROM ps_buckets;
DELETE FROM ps_untyped;
DELETE FROM ps_updated_rows;
DELETE FROM ps_kv WHERE key != 'client_id';
",
    )?;

    let table_glob = if clear_local != 0 {
        "ps_data_*"
    } else {
        "ps_data__*"
    };

    let tables_stmt = local_db
        .prepare_v2("SELECT name FROM sqlite_master WHERE type='table' AND name GLOB ?1")?;
    tables_stmt.bind_text(1, table_glob, sqlite::Destructor::STATIC)?;

    let mut tables: Vec<String> = alloc::vec![];

    while tables_stmt.step()? == ResultCode::ROW {
        let name = tables_stmt.column_text(0)?;
        tables.push(name.to_string());
    }

    for name in tables {
        let quoted = quote_identifier(&name);
        // The first delete statement deletes a single row, to trigger an update notification for the table.
        // The second delete statement uses the truncate optimization to delete the remainder of the data.
        let delete_sql = format!(
            "\
DELETE FROM {table} WHERE rowid IN (SELECT rowid FROM {table} LIMIT 1);
DELETE FROM {table};",
            table = quoted
        );
        local_db.exec_safe(&delete_sql)?;
    }

    Ok(String::from(""))
}

create_auto_tx_function!(powersync_clear_tx, powersync_clear_impl);
create_sqlite_text_fn!(powersync_clear, powersync_clear_tx, "powersync_clear");

fn setup_internal_views(db: *mut sqlite::sqlite3) -> Result<(), ResultCode> {
    // powersync_views - just filters sqlite_master, and combines the view and related triggers
    // into one row.

    // These views are only usable while the extension is loaded, so use TEMP views.
    // TODO: This should not be a public view - implement internally instead
    // language=SQLite
    db.exec_safe("\
    CREATE TEMP VIEW IF NOT EXISTS powersync_views(name, sql, delete_trigger_sql, insert_trigger_sql, update_trigger_sql)
    AS SELECT
        view.name name,
        view.sql sql,
        ifnull(trigger1.sql, '') delete_trigger_sql,
        ifnull(trigger2.sql, '') insert_trigger_sql,
        ifnull(trigger3.sql, '') update_trigger_sql
        FROM sqlite_master view
        LEFT JOIN sqlite_master trigger1
            ON trigger1.tbl_name = view.name AND trigger1.type = 'trigger' AND trigger1.name GLOB 'ps_view_delete*'
        LEFT JOIN sqlite_master trigger2
            ON trigger2.tbl_name = view.name AND trigger2.type = 'trigger' AND trigger2.name GLOB 'ps_view_insert*'
        LEFT JOIN sqlite_master trigger3
            ON trigger3.tbl_name = view.name AND trigger3.type = 'trigger' AND trigger3.name GLOB 'ps_view_update*'
        WHERE view.type = 'view' AND view.sql GLOB  '*-- powersync-auto-generated';

    CREATE TRIGGER IF NOT EXISTS powersync_views_insert
    INSTEAD OF INSERT ON powersync_views
    FOR EACH ROW
    BEGIN
        SELECT powersync_drop_view(NEW.name);
        SELECT powersync_exec(NEW.sql);
        SELECT powersync_exec(NEW.delete_trigger_sql);
        SELECT powersync_exec(NEW.insert_trigger_sql);
        SELECT powersync_exec(NEW.update_trigger_sql);
    END;

    CREATE TRIGGER IF NOT EXISTS powersync_views_update
    INSTEAD OF UPDATE ON powersync_views
    FOR EACH ROW
    BEGIN
        SELECT powersync_drop_view(OLD.name);
        SELECT powersync_exec(NEW.sql);
        SELECT powersync_exec(NEW.delete_trigger_sql);
        SELECT powersync_exec(NEW.insert_trigger_sql);
        SELECT powersync_exec(NEW.update_trigger_sql);
    END;

    CREATE TRIGGER IF NOT EXISTS powersync_views_delete
    INSTEAD OF DELETE ON powersync_views
    FOR EACH ROW
    BEGIN
        SELECT powersync_drop_view(OLD.name);
    END;")?;

    // language=SQLite
    db.exec_safe(
        "\
    CREATE TEMP VIEW IF NOT EXISTS powersync_tables(name, internal_name, local_only)
    AS SELECT
        powersync_external_table_name(name) as name,
        name as internal_name,
        name GLOB 'ps_data_local__*' as local_only
        FROM sqlite_master
        WHERE type = 'table' AND name GLOB 'ps_data_*';",
    )?;

    Ok(())
}

pub fn register(db: *mut sqlite::sqlite3) -> Result<(), ResultCode> {
    // This entire module is just making it easier to edit sqlite_master using queries.
    // The primary interfaces exposed are:
    // 1. Individual views:
    //
    //    CREATE VIEW powersync_views(name TEXT, sql TEXT, delete_trigger_sql TEXT, insert_trigger_sql TEXT, update_trigger_sql TEXT)
    //
    // The views can be queried and updated using powersync_views.
    // UPSERT is not supported on powersync_views (or any view or virtual table for that matter),
    // but "INSERT OR REPLACE" is supported. However, it's a potentially expensive operation
    // (drops and re-creates the view and trigger), so avoid where possible.
    //
    // 2. All-in-one schema updates:
    //
    //    INSERT INTO powersync_replace_schema(schema) VALUES('{"tables": [...]}');
    //
    // This takes care of updating, inserting and deleting powersync_views to get it in sync
    // with the schema.
    //
    // The same results could be achieved using virtual tables, but the interface would remain the same.
    // A potential disadvantage of using views is that the JSON may be re-parsed multiple times.

    // Internal function, used in triggers for powersync_views.
    db.create_function_v2(
        "powersync_drop_view",
        1,
        sqlite::UTF8,
        None,
        Some(powersync_drop_view),
        None,
        None,
        None,
    )?;

    // Internal function, used in triggers for powersync_views.
    db.create_function_v2(
        "powersync_exec",
        1,
        sqlite::UTF8,
        None,
        Some(powersync_exec),
        None,
        None,
        None,
    )?;

    // Initialize the extension internal tables.
    db.create_function_v2(
        "powersync_init",
        0,
        sqlite::UTF8,
        None,
        Some(powersync_init),
        None,
        None,
        None,
    )?;

    db.create_function_v2(
        "powersync_test_migration",
        1,
        sqlite::UTF8,
        None,
        Some(powersync_test_migration),
        None,
        None,
        None,
    )?;

    // Initialize the extension internal tables.
    db.create_function_v2(
        "powersync_clear",
        1,
        sqlite::UTF8,
        None,
        Some(powersync_clear),
        None,
        None,
        None,
    )?;

    db.create_function_v2(
        "powersync_external_table_name",
        1,
        sqlite::UTF8 | sqlite::DETERMINISTIC,
        None,
        Some(powersync_external_table_name),
        None,
        None,
        None,
    )?;

    db.create_function_v2(
        "powersync_internal_table_name",
        1,
        sqlite::UTF8 | sqlite::DETERMINISTIC,
        None,
        Some(powersync_internal_table_name),
        None,
        None,
        None,
    )?;

    Ok(())
}
