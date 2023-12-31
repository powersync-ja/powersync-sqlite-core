extern crate alloc;

use alloc::format;
use alloc::string::String;
use core::ffi::c_int;
use core::slice;

use sqlite::{ResultCode, Value};
use sqlite_nostd as sqlite;
use sqlite_nostd::{Connection, Context};

use crate::{create_auto_tx_function, create_sqlite_text_fn};
use crate::error::{PSResult, SQLiteError};
use crate::util::quote_identifier;

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
    let stmt1 = local_db.prepare_v2("SELECT json_extract(?1, '$.name') as name, ifnull(json_extract(?1, '$.local_only'), 0)")?;
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

    // language=SQLite
    local_db.exec_safe("\
CREATE TABLE IF NOT EXISTS ps_migration(id INTEGER PRIMARY KEY, down_migrations TEXT)")?;

    // language=SQLite
    let stmt = local_db.prepare_v2("SELECT ifnull(max(id), 0) as version FROM ps_migration")?;
    let rc = stmt.step()?;
    if rc != ResultCode::ROW {
        return Err(SQLiteError::from(ResultCode::ABORT));
    }

    let version = stmt.column_int(0)?;

    if version > 2 {
        // We persist down migrations, but don't support running them yet
        return Err(SQLiteError(ResultCode::MISUSE, Some(String::from("Downgrade not supported"))));
    }

    if version < 1 {
        // language=SQLite
        local_db.exec_safe("
CREATE TABLE ps_oplog(
  bucket TEXT NOT NULL,
  op_id INTEGER NOT NULL,
  op INTEGER NOT NULL,
  row_type TEXT,
  row_id TEXT,
  key TEXT,
  data TEXT,
  hash INTEGER NOT NULL,
  superseded INTEGER NOT NULL);

CREATE INDEX ps_oplog_by_row ON ps_oplog (row_type, row_id) WHERE superseded = 0;
CREATE INDEX ps_oplog_by_opid ON ps_oplog (bucket, op_id);
CREATE INDEX ps_oplog_by_key ON ps_oplog (bucket, key) WHERE superseded = 0;

CREATE TABLE ps_buckets(
  name TEXT PRIMARY KEY,
  last_applied_op INTEGER NOT NULL DEFAULT 0,
  last_op INTEGER NOT NULL DEFAULT 0,
  target_op INTEGER NOT NULL DEFAULT 0,
  add_checksum INTEGER NOT NULL DEFAULT 0,
  pending_delete INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE ps_untyped(type TEXT NOT NULL, id TEXT NOT NULL, data TEXT, PRIMARY KEY (type, id));

CREATE TABLE ps_crud (id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT);

INSERT INTO ps_migration(id, down_migrations) VALUES(1, NULL);
").into_db_result(local_db)?;
    }

    if version < 2 {
        // language=SQLite
        local_db.exec_safe("\
CREATE TABLE ps_tx(id INTEGER PRIMARY KEY NOT NULL, current_tx INTEGER, next_tx INTEGER);
INSERT INTO ps_tx(id, current_tx, next_tx) VALUES(1, NULL, 1);

ALTER TABLE ps_crud ADD COLUMN tx_id INTEGER;

INSERT INTO ps_migration(id, down_migrations) VALUES(2, json_array(json_object('sql', 'DELETE FROM ps_migrations WHERE id >= 2', 'params', json_array()), json_object('sql', 'DROP TABLE ps_tx', 'params', json_array()), json_object('sql', 'ALTER TABLE ps_crud DROP COLUMN tx_id', 'params', json_array())));
").into_db_result(local_db)?;
    }

    Ok(String::from(""))
}


create_auto_tx_function!(powersync_init_tx, powersync_init_impl);
create_sqlite_text_fn!(
    powersync_init,
    powersync_init_tx,
    "powersync_init"
);

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

    // powersync_views - just filters sqlite_master, and combines the view and related triggers
    // into one row.

    // These views are only usable while the extension is loaded, so use TEMP views.
    // TODO: This should not be a public view - implement internally instead
    // language=SQLite
    db.exec_safe("\
CREATE TEMP VIEW powersync_views(name, sql, delete_trigger_sql, insert_trigger_sql, update_trigger_sql)
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

CREATE TRIGGER powersync_views_insert
INSTEAD OF INSERT ON powersync_views
FOR EACH ROW
BEGIN
    SELECT powersync_drop_view(NEW.name);
    SELECT powersync_exec(NEW.sql);
    SELECT powersync_exec(NEW.delete_trigger_sql);
    SELECT powersync_exec(NEW.insert_trigger_sql);
    SELECT powersync_exec(NEW.update_trigger_sql);
END;

CREATE TRIGGER powersync_views_update
INSTEAD OF UPDATE ON powersync_views
FOR EACH ROW
BEGIN
    SELECT powersync_drop_view(OLD.name);
    SELECT powersync_exec(NEW.sql);
    SELECT powersync_exec(NEW.delete_trigger_sql);
    SELECT powersync_exec(NEW.insert_trigger_sql);
    SELECT powersync_exec(NEW.update_trigger_sql);
END;

CREATE TRIGGER powersync_views_delete
INSTEAD OF DELETE ON powersync_views
FOR EACH ROW
BEGIN
    SELECT powersync_drop_view(OLD.name);
END;")?;

    // language=SQLite
    db.exec_safe("\
CREATE TEMP VIEW powersync_tables(name, internal_name, local_only)
AS SELECT
    powersync_external_table_name(name) as name,
    name as internal_name,
    name GLOB 'ps_data_local__*' as local_only
    FROM sqlite_master
    WHERE type = 'table' AND name GLOB 'ps_data_*';")?;

    Ok(())
}
