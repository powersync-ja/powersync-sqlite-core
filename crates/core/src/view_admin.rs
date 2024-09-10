extern crate alloc;

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::ffi::c_int;
use core::slice;

use sqlite::{ResultCode, Value};
use sqlite_nostd as sqlite;
use sqlite_nostd::{Connection, Context};

use crate::error::{PSResult, SQLiteError};
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

pub fn powersync_init_impl(
    ctx: *mut sqlite::context,
    _args: &[*mut sqlite::value],
) -> Result<String, SQLiteError> {
    let local_db = ctx.db_handle();

    // language=SQLite
    local_db.exec_safe(
        "\
CREATE TABLE IF NOT EXISTS ps_migration(id INTEGER PRIMARY KEY, down_migrations TEXT)",
    )?;

    // language=SQLite
    let current_version_stmt =
        local_db.prepare_v2("SELECT ifnull(max(id), 0) as version FROM ps_migration")?;
    let rc = current_version_stmt.step()?;
    if rc != ResultCode::ROW {
        return Err(SQLiteError::from(ResultCode::ABORT));
    }

    const CODE_VERSION: i32 = 4;

    let mut current_version = current_version_stmt.column_int(0)?;

    while current_version > CODE_VERSION {
        // Run down migrations.
        // This is rare, we don't worry about optimizing this.

        current_version_stmt.reset()?;

        let down_migrations_stmt = local_db.prepare_v2("select e.value ->> 'sql' as sql from (select id, down_migrations from ps_migration where id > ?1 order by id desc limit 1) m, json_each(m.down_migrations) e")?;
        down_migrations_stmt.bind_int(1, CODE_VERSION)?;

        let mut down_sql: Vec<String> = alloc::vec![];

        while down_migrations_stmt.step()? == ResultCode::ROW {
            let sql = down_migrations_stmt.column_text(0)?;
            down_sql.push(sql.to_string());
        }

        for sql in down_sql {
            let rs = local_db.exec_safe(&sql);
            if let Err(code) = rs {
                return Err(SQLiteError(
                    code,
                    Some(format!(
                        "Down migration failed for {:} {:}",
                        current_version, sql
                    )),
                ));
            }
        }

        // Refresh the version
        current_version_stmt.reset()?;
        let rc = current_version_stmt.step()?;
        if rc != ResultCode::ROW {
            return Err(SQLiteError(
                rc,
                Some("Down migration failed - could not get version".to_string()),
            ));
        }
        let new_version = current_version_stmt.column_int(0)?;
        if new_version >= current_version {
            // Database down from version $currentVersion to $version failed - version not updated after dow migration
            return Err(SQLiteError(
                ResultCode::ABORT,
                Some(format!(
                    "Down migration failed - version not updated from {:}",
                    current_version
                )),
            ));
        }
        current_version = new_version;
    }
    current_version_stmt.reset()?;

    if current_version < 1 {
        // language=SQLite
        local_db
            .exec_safe(
                "
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
",
            )
            .into_db_result(local_db)?;
    }

    if current_version < 2 {
        // language=SQLite
        local_db.exec_safe("\
CREATE TABLE ps_tx(id INTEGER PRIMARY KEY NOT NULL, current_tx INTEGER, next_tx INTEGER);
INSERT INTO ps_tx(id, current_tx, next_tx) VALUES(1, NULL, 1);

ALTER TABLE ps_crud ADD COLUMN tx_id INTEGER;

INSERT INTO ps_migration(id, down_migrations) VALUES(2, json_array(json_object('sql', 'DELETE FROM ps_migration WHERE id >= 2', 'params', json_array()), json_object('sql', 'DROP TABLE ps_tx', 'params', json_array()), json_object('sql', 'ALTER TABLE ps_crud DROP COLUMN tx_id', 'params', json_array())));
").into_db_result(local_db)?;
    }

    if current_version < 3 {
        // language=SQLite
        local_db.exec_safe("\
CREATE TABLE ps_kv(key TEXT PRIMARY KEY NOT NULL, value BLOB);
INSERT INTO ps_kv(key, value) values('client_id', uuid());

INSERT INTO ps_migration(id, down_migrations) VALUES(3, json_array(json_object('sql', 'DELETE FROM ps_migration WHERE id >= 3'), json_object('sql', 'DROP TABLE ps_kv')));
    ").into_db_result(local_db)?;
    }

    if current_version < 4 {
        // language=SQLite
        local_db.exec_safe("\
ALTER TABLE ps_buckets ADD COLUMN op_checksum INTEGER NOT NULL DEFAULT 0;
ALTER TABLE ps_buckets ADD COLUMN remove_operations INTEGER NOT NULL DEFAULT 0;

UPDATE ps_buckets SET op_checksum = (
  SELECT IFNULL(SUM(ps_oplog.hash), 0) & 0xffffffff FROM ps_oplog WHERE ps_oplog.bucket = ps_buckets.name
);

INSERT INTO ps_migration(id, down_migrations)
  VALUES(4,
    json_array(
      json_object('sql', 'DELETE FROM ps_migration WHERE id >= 4'),
      json_object('sql', 'ALTER TABLE ps_buckets DROP COLUMN op_checksum'),
      json_object('sql', 'ALTER TABLE ps_buckets DROP COLUMN remove_operations')
    ));
    ").into_db_result(local_db)?;
    }

    if current_version < 4 {
        // language=SQLite
        local_db
            .exec_safe(
                "\
DROP TABLE ps_buckets;
DROP TABLE ps_oplog;

CREATE TABLE ps_buckets(
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    last_applied_op INTEGER NOT NULL DEFAULT 0,
    last_op INTEGER NOT NULL DEFAULT 0,
    target_op INTEGER NOT NULL DEFAULT 0,
    add_checksum INTEGER NOT NULL DEFAULT 0,
    op_checksum INTEGER NOT NULL DEFAULT 0,
    pending_delete INTEGER NOT NULL DEFAULT 0
  );

CREATE UNIQUE INDEX ps_buckets_name ON ps_buckets (name);

CREATE TABLE ps_oplog(
  bucket INTEGER NOT NULL,
  op_id INTEGER NOT NULL,
  row_type TEXT,
  row_id TEXT,
  key TEXT,
  data TEXT,
  hash INTEGER NOT NULL);

CREATE INDEX ps_oplog_by_row ON ps_oplog (row_type, row_id);
CREATE INDEX ps_oplog_by_opid ON ps_oplog (bucket, op_id);
CREATE INDEX ps_oplog_by_key ON ps_oplog (bucket, key);

CREATE TABLE ps_updated_rows(
  row_type TEXT,
  row_id TEXT);

CREATE UNIQUE INDEX ps_updated_rows_row ON ps_updated_rows (row_type, row_id);

INSERT INTO ps_migration(id, down_migrations)
  VALUES(5,
    json_array(
      json_object('sql', 'DELETE FROM ps_migration WHERE id >= 5')
    ));
    ",
            )
            .into_db_result(local_db)?;
    }

    Ok(String::from(""))
}

create_auto_tx_function!(powersync_init_tx, powersync_init_impl);
create_sqlite_text_fn!(powersync_init, powersync_init_tx, "powersync_init");

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
    db.exec_safe(
        "\
CREATE TEMP VIEW powersync_tables(name, internal_name, local_only)
AS SELECT
    powersync_external_table_name(name) as name,
    name as internal_name,
    name GLOB 'ps_data_local__*' as local_only
    FROM sqlite_master
    WHERE type = 'table' AND name GLOB 'ps_data_*';",
    )?;

    Ok(())
}
