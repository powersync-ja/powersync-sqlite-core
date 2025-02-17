extern crate alloc;

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::ffi::c_int;
use core::slice;

use sqlite::{Connection, Context, ResultCode, Value};
use sqlite_nostd::{self as sqlite, ManagedStmt};

use crate::create_sqlite_text_fn;
use crate::error::{PSResult, SQLiteError};
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
    let include_metadata = statement.column_int(5)? != 0;

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

    if include_metadata {
        column_names_quoted.push(quote_identifier("_metadata"));
        column_values.push(String::from("NULL"));
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

create_sqlite_text_fn!(
    powersync_view_sql,
    powersync_view_sql_impl,
    "powersync_view_sql"
);

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
        let trigger = format!(
            "\
CREATE TRIGGER {:}
INSTEAD OF DELETE ON {:}
FOR EACH ROW
BEGIN
DELETE FROM {:} WHERE id = OLD.id;
INSERT INTO powersync_crud_(data) VALUES(json_object('op', 'DELETE', 'type', {:}, 'id', OLD.id));
INSERT OR IGNORE INTO ps_updated_rows(row_type, row_id) VALUES({:}, OLD.id);
INSERT OR REPLACE INTO ps_buckets(name, last_op, target_op) VALUES('$local', 0, {:});
END",
            trigger_name, quoted_name, internal_name, type_string, type_string, MAX_OP_ID
        );
        Ok(trigger)
    } else if local_only {
        let trigger = format!(
            "\
CREATE TRIGGER {:}
INSTEAD OF DELETE ON {:}
FOR EACH ROW
BEGIN
DELETE FROM {:} WHERE id = OLD.id;
END",
            trigger_name, quoted_name, internal_name
        );
        Ok(trigger)
    } else if insert_only {
        Ok(String::from(""))
    } else {
        Err(SQLiteError::from(ResultCode::MISUSE))
    };
}

create_sqlite_text_fn!(
    powersync_trigger_delete_sql,
    powersync_trigger_delete_sql_impl,
    "powersync_trigger_delete_sql"
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
    let json_fragment = json_object_fragment("NEW", &stmt2)?;

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
      SELECT NEW.id, {:};
      INSERT INTO powersync_crud_(data) VALUES(json_object('op', 'PUT', 'type', {:}, 'id', NEW.id, 'data', json(powersync_diff('{{}}', {:}))));
      INSERT OR IGNORE INTO ps_updated_rows(row_type, row_id) VALUES({:}, NEW.id);
      INSERT OR REPLACE INTO ps_buckets(name, last_op, target_op) VALUES('$local', 0, {:});
    END", trigger_name, quoted_name, internal_name, json_fragment, type_string, json_fragment, type_string, MAX_OP_ID);
        Ok(trigger)
    } else if local_only {
        let trigger = format!(
            "\
    CREATE TRIGGER {:}
    INSTEAD OF INSERT ON {:}
    FOR EACH ROW
    BEGIN
      INSERT INTO {:} SELECT NEW.id, {:};
    END",
            trigger_name, quoted_name, internal_name, json_fragment
        );
        Ok(trigger)
    } else if insert_only {
        let trigger = format!("\
    CREATE TRIGGER {:}
    INSTEAD OF INSERT ON {:}
    FOR EACH ROW
    BEGIN
      INSERT INTO powersync_crud_(data) VALUES(json_object('op', 'PUT', 'type', {}, 'id', NEW.id, 'data', json(powersync_diff('{{}}', {:}))));
    END", trigger_name, quoted_name, type_string, json_fragment);
        Ok(trigger)
    } else {
        Err(SQLiteError::from(ResultCode::MISUSE))
    };
}

create_sqlite_text_fn!(
    powersync_trigger_insert_sql,
    powersync_trigger_insert_sql_impl,
    "powersync_trigger_insert_sql"
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
    // TODO: allow accepting a column list
    let include_old = statement.column_type(4)? == sqlite::ColumnType::Text;
    let include_metadata = statement.column_int(5)? != 0;

    let quoted_name = quote_identifier(view_name);
    let internal_name = quote_internal_name(name, local_only);
    let trigger_name = quote_identifier_prefixed("ps_view_update_", view_name);
    let type_string = quote_string(name);

    let db = ctx.db_handle();
    let stmt2 = db.prepare_v2("select json_extract(e.value, '$.name') as name from json_each(json_extract(?, '$.columns')) e").into_db_result(db)?;
    stmt2.bind_text(1, table, sqlite::Destructor::STATIC)?;
    let json_fragment_new = json_object_fragment("NEW", &stmt2)?;
    stmt2.reset()?;
    let json_fragment_old = json_object_fragment("OLD", &stmt2)?;
    let old_fragment: String;
    let metadata_fragment: &str;

    if include_old {
        old_fragment = format!(", 'old', {:}", json_fragment_old);
    } else {
        old_fragment = String::from("");
    }

    if include_metadata {
        metadata_fragment = ", 'metadata', NEW._metadata";
    } else {
        metadata_fragment = "";
    }

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
      SET data = {:}
      WHERE id = NEW.id;
  INSERT INTO powersync_crud_(data) VALUES(json_object('op', 'PATCH', 'type', {:}, 'id', NEW.id, 'data', json(powersync_diff({:}, {:})){:}{:}));
  INSERT OR IGNORE INTO ps_updated_rows(row_type, row_id) VALUES({:}, NEW.id);
  INSERT OR REPLACE INTO ps_buckets(name, last_op, target_op) VALUES('$local', 0, {:});
END", trigger_name, quoted_name, internal_name, json_fragment_new, type_string, json_fragment_old, json_fragment_new, old_fragment, metadata_fragment, type_string, MAX_OP_ID);
        Ok(trigger)
    } else if local_only {
        let trigger = format!(
            "\
CREATE TRIGGER {:}
INSTEAD OF UPDATE ON {:}
FOR EACH ROW
BEGIN
  SELECT CASE
  WHEN (OLD.id != NEW.id)
  THEN RAISE (FAIL, 'Cannot update id')
  END;
  UPDATE {:}
      SET data = {:}
      WHERE id = NEW.id;
END",
            trigger_name, quoted_name, internal_name, json_fragment_new
        );
        Ok(trigger)
    } else if insert_only {
        Ok(String::from(""))
    } else {
        Err(SQLiteError::from(ResultCode::MISUSE))
    };
}

create_sqlite_text_fn!(
    powersync_trigger_update_sql,
    powersync_trigger_update_sql_impl,
    "powersync_trigger_update_sql"
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

/// Given a query returning column names, return a JSON object fragment for a trigger.
///
/// Example output with prefix "NEW": "json_object('id', NEW.id, 'name', NEW.name, 'age', NEW.age)".
fn json_object_fragment(prefix: &str, name_results: &ManagedStmt) -> Result<String, SQLiteError> {
    // floor(SQLITE_MAX_FUNCTION_ARG / 2).
    // To keep databases portable, we use the default limit of 100 args for this,
    // and don't try to query the limit dynamically.
    const MAX_ARG_COUNT: usize = 50;

    let mut column_names_quoted: Vec<String> = alloc::vec![];
    while name_results.step()? == ResultCode::ROW {
        let name = name_results.column_text(0)?;

        let quoted: String = format!(
            "{:}, {:}.{:}",
            quote_string(name),
            prefix,
            quote_identifier(name)
        );
        column_names_quoted.push(quoted);
    }

    // SQLITE_MAX_COLUMN - 1 (because of the id column)
    if column_names_quoted.len() > 1999 {
        return Err(SQLiteError::from(ResultCode::TOOBIG));
    } else if column_names_quoted.len() <= MAX_ARG_COUNT {
        // Small number of columns - use json_object() directly.
        let json_fragment = column_names_quoted.join(", ");
        return Ok(format!("json_object({:})", json_fragment));
    } else {
        // Too many columns to use json_object directly.
        // Instead, we build up the JSON object in chunks,
        // and merge using powersync_json_merge().
        let mut fragments: Vec<String> = alloc::vec![];
        for chunk in column_names_quoted.chunks(MAX_ARG_COUNT) {
            let sub_fragment = chunk.join(", ");
            fragments.push(format!("json_object({:})", sub_fragment));
        }
        return Ok(format!("powersync_json_merge({:})", fragments.join(", ")));
    }
}
