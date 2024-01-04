extern crate alloc;

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::ffi::c_int;
use core::slice;

use sqlite::{Connection, Context, ResultCode, Value};
use sqlite_nostd as sqlite;

use crate::create_sqlite_text_fn;
use crate::error::{SQLiteError, PSResult};
use crate::util::*;

fn powersync_view_sql_impl(
    ctx: *mut sqlite::context,
    args: &[*mut sqlite::value],
) -> Result<String, SQLiteError> {
    let db = ctx.db_handle();
    let table = args[0].text();
    let statement = extract_table_info(db, table)?;

    let name = statement.column_text(0)?;
    let view_name = statement.column_text(1)?;
    let local_only = statement.column_int(2)? != 0;

    let quoted_name = quote_identifier(view_name);
    let internal_name = quote_internal_name(name, local_only);

    let stmt2 = db.prepare_v2("select json_extract(e.value, '$.name') as name, json_extract(e.value, '$.type') as type from json_each(json_extract(?, '$.columns')) e")?;
    stmt2.bind_text(1, table, sqlite::Destructor::STATIC)?;

    let mut column_names_quoted: Vec<String> = alloc::vec![];
    let mut column_values: Vec<String> = alloc::vec![];
    column_names_quoted.push(quote_identifier("id"));
    column_values.push(String::from("id"));
    while stmt2.step()? == ResultCode::ROW {
        let name = stmt2.column_text(0)?;
        let type_name = stmt2.column_text(1)?;
        column_names_quoted.push(quote_identifier(name));

        let foo = format!(
            "CAST(json_extract(data, {:}) as {:})",
            quote_json_path(name),
            type_name
        );
        column_values.push(foo);
    }

    let view_statement = format!(
        "CREATE VIEW {:}({:}) AS SELECT {:} FROM {:} -- powersync-auto-generated",
        quoted_name,
        column_names_quoted.join(", "),
        column_values.join(", "),
        internal_name
    );

    return Ok(view_statement);
}

create_sqlite_text_fn!(powersync_view_sql, powersync_view_sql_impl, "powersync_view_sql");

fn powersync_trigger_delete_sql_impl(
    ctx: *mut sqlite::context,
    args: &[*mut sqlite::value],
) -> Result<String, SQLiteError> {
    let table = args[0].text();
    let statement = extract_table_info(ctx.db_handle(), table)?;

    let name = statement.column_text(0)?;
    let view_name = statement.column_text(1)?;
    let local_only = statement.column_int(2)? != 0;
    let insert_only = statement.column_int(3)? != 0;

    let quoted_name = quote_identifier(view_name);
    let internal_name = quote_internal_name(name, local_only);
    let trigger_name = quote_identifier_prefixed("ps_view_delete_", view_name);
    let type_string = quote_string(name);

    return if !local_only && !insert_only {
        let trigger = format!("\
CREATE TRIGGER {:}
INSTEAD OF DELETE ON {:}
FOR EACH ROW
BEGIN
DELETE FROM {:} WHERE id = OLD.id;
INSERT INTO powersync_crud_(data) VALUES(json_object('op', 'DELETE', 'type', {:}, 'id', OLD.id));
INSERT INTO ps_oplog(bucket, op_id, op, row_type, row_id, hash, superseded)
      SELECT '$local',
              1,
              'REMOVE',
              {:},
              OLD.id,
              0,
              0;
INSERT OR REPLACE INTO ps_buckets(name, pending_delete, last_op, target_op) VALUES('$local', 1, 0, {:});
END", trigger_name, quoted_name, internal_name, type_string, type_string, MAX_OP_ID);
        Ok(trigger)
    } else if local_only {
        let trigger = format!("\
CREATE TRIGGER {:}
INSTEAD OF DELETE ON {:}
FOR EACH ROW
BEGIN
DELETE FROM {:} WHERE id = OLD.id;
END", trigger_name, quoted_name, internal_name);
        Ok(trigger)
    } else if insert_only {
        Ok(String::from(""))
    } else {
        Err(SQLiteError::from(ResultCode::MISUSE))
    };
}

create_sqlite_text_fn!(
    powersync_trigger_delete_sql,
    powersync_trigger_delete_sql_impl, "powersync_trigger_delete_sql"
);

fn powersync_trigger_insert_sql_impl(
    ctx: *mut sqlite::context,
    args: &[*mut sqlite::value],
) -> Result<String, SQLiteError> {
    let table = args[0].text();

    let statement = extract_table_info(ctx.db_handle(), table)?;

    let name = statement.column_text(0)?;
    let view_name = statement.column_text(1)?;
    let local_only = statement.column_int(2)? != 0;
    let insert_only = statement.column_int(3)? != 0;

    let quoted_name = quote_identifier(view_name);
    let internal_name = quote_internal_name(name, local_only);
    let trigger_name = quote_identifier_prefixed("ps_view_insert_", view_name);
    let type_string = quote_string(name);

    let local_db = ctx.db_handle();
    let stmt2 = local_db.prepare_v2("select json_extract(e.value, '$.name') as name from json_each(json_extract(?, '$.columns')) e")?;
    stmt2.bind_text(1, table, sqlite::Destructor::STATIC)?;

    let mut column_names_quoted: Vec<String> = alloc::vec![];
    while stmt2.step()? == ResultCode::ROW {
        let name = stmt2.column_text(0)?;

        let foo: String = format!("{:}, NEW.{:}", quote_string(name), quote_identifier(name));
        column_names_quoted.push(foo);
    }

    let json_fragment = column_names_quoted.join(", ");

    return if !local_only && !insert_only {
        let trigger = format!("\
    CREATE TRIGGER {:}
    INSTEAD OF INSERT ON {:}
    FOR EACH ROW
    BEGIN
      SELECT CASE
      WHEN (NEW.id IS NULL)
      THEN RAISE (FAIL, 'id is required')
      END;
      INSERT INTO {:}
      SELECT NEW.id, json_object({:});
      INSERT INTO powersync_crud_(data) VALUES(json_object('op', 'PUT', 'type', {:}, 'id', NEW.id, 'data', json(powersync_diff('{{}}', json_object({:})))));
      INSERT INTO ps_oplog(bucket, op_id, op, row_type, row_id, hash, superseded)
      SELECT '$local',
              1,
              'REMOVE',
              {:},
              NEW.id,
              0,
              0;
      INSERT OR REPLACE INTO ps_buckets(name, pending_delete, last_op, target_op) VALUES('$local', 1, 0, {:});
    END", trigger_name, quoted_name, internal_name, json_fragment, type_string, json_fragment, type_string, MAX_OP_ID);
        Ok(trigger)
    } else if local_only {
        let trigger = format!("\
    CREATE TRIGGER {:}
    INSTEAD OF INSERT ON {:}
    FOR EACH ROW
    BEGIN
      INSERT INTO {:} SELECT NEW.id, json_object({:});
    END", trigger_name, quoted_name, internal_name, json_fragment);
        Ok(trigger)
    } else if insert_only {
        let trigger = format!("\
    CREATE TRIGGER {:}
    INSTEAD OF INSERT ON {:}
    FOR EACH ROW
    BEGIN
      INSERT INTO powersync_crud_(data) VALUES(json_object('op', 'PUT', 'type', {}, 'id', NEW.id, 'data', json(powersync_diff('{{}}', json_object({:})))));
    END", trigger_name, quoted_name, type_string, json_fragment);
        Ok(trigger)
    } else {
        Err(SQLiteError::from(ResultCode::MISUSE))
    };
}

create_sqlite_text_fn!(
    powersync_trigger_insert_sql,
    powersync_trigger_insert_sql_impl, "powersync_trigger_insert_sql"
);

fn powersync_trigger_update_sql_impl(
    ctx: *mut sqlite::context,
    args: &[*mut sqlite::value],
) -> Result<String, SQLiteError> {
    let table = args[0].text();

    let statement = extract_table_info(ctx.db_handle(), table)?;

    let name = statement.column_text(0)?;
    let view_name = statement.column_text(1)?;
    let local_only = statement.column_int(2)? != 0;
    let insert_only = statement.column_int(3)? != 0;

    let quoted_name = quote_identifier(view_name);
    let internal_name = quote_internal_name(name, local_only);
    let trigger_name = quote_identifier_prefixed("ps_view_update_", view_name);
    let type_string = quote_string(name);

    let db = ctx.db_handle();
    let stmt2 = db.prepare_v2("select json_extract(e.value, '$.name') as name from json_each(json_extract(?, '$.columns')) e").into_db_result(db)?;
    stmt2.bind_text(1, table, sqlite::Destructor::STATIC)?;

    let mut column_names_quoted_new: Vec<String> = alloc::vec![];
    let mut column_names_quoted_old: Vec<String> = alloc::vec![];
    while stmt2.step()? == ResultCode::ROW {
        let name = stmt2.column_text(0)?;

        let foo_new: String = format!("{:}, NEW.{:}", quote_string(name), quote_identifier(name));
        column_names_quoted_new.push(foo_new);
        let foo_old: String = format!("{:}, OLD.{:}", quote_string(name), quote_identifier(name));
        column_names_quoted_old.push(foo_old);
    }

    let json_fragment_new = column_names_quoted_new.join(", ");
    let json_fragment_old = column_names_quoted_old.join(", ");

    return if !local_only && !insert_only {
        let trigger = format!("\
CREATE TRIGGER {:}
INSTEAD OF UPDATE ON {:}
FOR EACH ROW
BEGIN
  SELECT CASE
  WHEN (OLD.id != NEW.id)
  THEN RAISE (FAIL, 'Cannot update id')
  END;
  UPDATE {:}
      SET data = json_object({:})
      WHERE id = NEW.id;
  INSERT INTO powersync_crud_(data) VALUES(json_object('op', 'PATCH', 'type', {:}, 'id', NEW.id, 'data', json(powersync_diff(json_object({:}), json_object({:})))));
  INSERT INTO ps_oplog(bucket, op_id, op, row_type, row_id, hash, superseded)
  SELECT '$local',
          1,
          'REMOVE',
          {:},
          NEW.id,
          0,
          0;
  INSERT OR REPLACE INTO ps_buckets(name, pending_delete, last_op, target_op) VALUES('$local', 1, 0, {:});
END", trigger_name, quoted_name, internal_name, json_fragment_new, type_string, json_fragment_old, json_fragment_new, type_string, MAX_OP_ID);
        Ok(trigger)
    } else if local_only {
        let trigger = format!("\
CREATE TRIGGER {:}
INSTEAD OF UPDATE ON {:}
FOR EACH ROW
BEGIN
  SELECT CASE
  WHEN (OLD.id != NEW.id)
  THEN RAISE (FAIL, 'Cannot update id')
  END;
  UPDATE {:}
      SET data = json_object({:})
      WHERE id = NEW.id;
END", trigger_name, quoted_name, internal_name, json_fragment_new);
        Ok(trigger)
    } else if insert_only {
        Ok(String::from(""))
    } else {
        Err(SQLiteError::from(ResultCode::MISUSE))
    };
}

create_sqlite_text_fn!(
    powersync_trigger_update_sql,
    powersync_trigger_update_sql_impl, "powersync_trigger_update_sql"
);

pub fn register(db: *mut sqlite::sqlite3) -> Result<(), ResultCode> {
    db.create_function_v2(
        "powersync_view_sql",
        1,
        sqlite::UTF8 | sqlite::DETERMINISTIC | sqlite::DIRECTONLY,
        None,
        Some(powersync_view_sql),
        None,
        None,
        None,
    )?;

    db.create_function_v2(
        "powersync_trigger_delete_sql",
        1,
        sqlite::UTF8 | sqlite::DETERMINISTIC | sqlite::DIRECTONLY,
        None,
        Some(powersync_trigger_delete_sql),
        None,
        None,
        None,
    )?;

    db.create_function_v2(
        "powersync_trigger_insert_sql",
        1,
        sqlite::UTF8 | sqlite::DETERMINISTIC | sqlite::DIRECTONLY,
        None,
        Some(powersync_trigger_insert_sql),
        None,
        None,
        None,
    )?;

    db.create_function_v2(
        "powersync_trigger_update_sql",
        1,
        sqlite::UTF8 | sqlite::DETERMINISTIC | sqlite::DIRECTONLY,
        None,
        Some(powersync_trigger_update_sql),
        None,
        None,
        None,
    )?;

    Ok(())
}
