extern crate alloc;

use alloc::borrow::Cow;
use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::ffi::c_int;
use core::fmt::Write;

use powersync_sqlite_nostd::{self as sqlite};
use sqlite::{Connection, Context, ResultCode, Value};

use crate::create_sqlite_text_fn;
use crate::error::PowerSyncError;
use crate::schema::{Column, DiffIncludeOld, Table};
use crate::util::*;

fn powersync_view_sql_impl(
    _ctx: *mut sqlite::context,
    args: &[*mut sqlite::value],
) -> Result<String, PowerSyncError> {
    let table_info = Table::from_json(args[0].text()).map_err(PowerSyncError::as_argument_error)?;

    let name = &table_info.name;
    let view_name = &table_info.view_name();
    let local_only = table_info.flags.local_only();
    let include_metadata = table_info.flags.include_metadata();

    let quoted_name = quote_identifier(view_name);
    let internal_name = quote_internal_name(name, local_only);

    let mut column_names_quoted: Vec<String> = alloc::vec![];
    let mut column_values: Vec<String> = alloc::vec![];
    column_names_quoted.push(quote_identifier("id"));
    column_values.push(String::from("id"));
    for column in &table_info.columns {
        column_names_quoted.push(quote_identifier(&column.name));

        column_values.push(format!(
            "CAST(json_extract(data, {:}) as {:})",
            quote_json_path(&column.name),
            &column.type_name
        ));
    }

    if include_metadata {
        column_names_quoted.push(quote_identifier("_metadata"));
        column_values.push(String::from("NULL"));

        column_names_quoted.push(quote_identifier("_deleted"));
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
    _ctx: *mut sqlite::context,
    args: &[*mut sqlite::value],
) -> Result<String, PowerSyncError> {
    let table_info = Table::from_json(args[0].text()).map_err(PowerSyncError::as_argument_error)?;

    let name = &table_info.name;
    let view_name = &table_info.view_name();
    let local_only = table_info.flags.local_only();
    let insert_only = table_info.flags.insert_only();

    let quoted_name = quote_identifier(view_name);
    let internal_name = quote_internal_name(name, local_only);
    let trigger_name = quote_identifier_prefixed("ps_view_delete_", view_name);
    let type_string = quote_string(name);

    let (old_data_name, old_data_value): (&'static str, Cow<'static, str>) =
        match &table_info.diff_include_old {
            Some(include_old) => {
                let mut json = match include_old {
                    DiffIncludeOld::OnlyForColumns { columns } => json_object_fragment(
                        "OLD",
                        &mut table_info.filtered_columns(columns.iter().map(|c| c.as_str())),
                    ),
                    DiffIncludeOld::ForAllColumns => {
                        json_object_fragment("OLD", &mut table_info.columns.iter())
                    }
                }?;

                json.insert(0, ',');
                (",old_values", json.into())
            }
            None => ("", "".into()),
        };

    return if !local_only && !insert_only {
        let mut trigger = format!(
            "\
CREATE TRIGGER {trigger_name}
INSTEAD OF DELETE ON {quoted_name}
FOR EACH ROW
BEGIN
DELETE FROM {internal_name} WHERE id = OLD.id;
INSERT INTO powersync_crud(op,id,type{old_data_name}) VALUES ('DELETE',OLD.id,{type_string}{old_data_value});
END"
        );

        // The DELETE statement can't include metadata for the delete operation, so we create
        // another trigger to delete with a fake UPDATE syntax.
        if table_info.flags.include_metadata() {
            let trigger_name = quote_identifier_prefixed("ps_view_delete2_", view_name);
            write!(&mut trigger,  "\
;
CREATE TRIGGER {trigger_name}
INSTEAD OF UPDATE ON {quoted_name}
FOR EACH ROW
WHEN NEW._deleted IS TRUE
BEGIN
DELETE FROM {internal_name} WHERE id = NEW.id;
INSERT INTO powersync_crud(op,id,type,metadata{old_data_name}) VALUES ('DELETE',OLD.id,{type_string},NEW._metadata{old_data_value});
END"
                    ).expect("writing to string should be infallible");
        }

        Ok(trigger)
    } else if local_only {
        debug_assert!(!table_info.flags.include_metadata());

        let trigger = format!(
            "\
CREATE TRIGGER {trigger_name}
INSTEAD OF DELETE ON {quoted_name}
FOR EACH ROW
BEGIN
DELETE FROM {internal_name} WHERE id = OLD.id;
END",
        );
        Ok(trigger)
    } else if insert_only {
        Ok(String::from(""))
    } else {
        Err(PowerSyncError::argument_error("invalid flags for table"))
    };
}

create_sqlite_text_fn!(
    powersync_trigger_delete_sql,
    powersync_trigger_delete_sql_impl,
    "powersync_trigger_delete_sql"
);

fn powersync_trigger_insert_sql_impl(
    _ctx: *mut sqlite::context,
    args: &[*mut sqlite::value],
) -> Result<String, PowerSyncError> {
    let table_info = Table::from_json(args[0].text()).map_err(PowerSyncError::as_argument_error)?;

    let name = &table_info.name;
    let view_name = &table_info.view_name();
    let local_only = table_info.flags.local_only();
    let insert_only = table_info.flags.insert_only();

    let quoted_name = quote_identifier(view_name);
    let internal_name = quote_internal_name(name, local_only);
    let trigger_name = quote_identifier_prefixed("ps_view_insert_", view_name);
    let type_string = quote_string(name);

    let json_fragment = json_object_fragment("NEW", &mut table_info.columns.iter())?;

    let (metadata_key, metadata_value) = if table_info.flags.include_metadata() {
        (",metadata", ",NEW._metadata")
    } else {
        ("", "")
    };

    return if !local_only && !insert_only {
        let trigger = format!("\
    CREATE TRIGGER {trigger_name}
    INSTEAD OF INSERT ON {quoted_name}
    FOR EACH ROW
    BEGIN
      SELECT CASE
      WHEN (NEW.id IS NULL)
      THEN RAISE (FAIL, 'id is required')
      WHEN (typeof(NEW.id) != 'text')
      THEN RAISE (FAIL, 'id should be text')
      END;
      INSERT INTO {internal_name} SELECT NEW.id, {json_fragment};
      INSERT INTO powersync_crud(op,id,type,data{metadata_key}) VALUES ('PUT',NEW.id,{type_string},json(powersync_diff('{{}}', {:})){metadata_value});
    END",  json_fragment);
        Ok(trigger)
    } else if local_only {
        let trigger = format!(
            "\
    CREATE TRIGGER {trigger_name}
    INSTEAD OF INSERT ON {quoted_name}
    FOR EACH ROW
    BEGIN
      INSERT INTO {internal_name} SELECT NEW.id, {json_fragment};
    END",
        );
        Ok(trigger)
    } else if insert_only {
        // This is using the manual powersync_crud_ instead of powersync_crud because insert-only
        // writes shouldn't prevent us from receiving new data.
        let trigger = format!("\
    CREATE TRIGGER {trigger_name}
    INSTEAD OF INSERT ON {quoted_name}
    FOR EACH ROW
    BEGIN
      INSERT INTO powersync_crud_(data) VALUES(json_object('op', 'PUT', 'type', {}, 'id', NEW.id, 'data', json(powersync_diff('{{}}', {:}))));
    END", type_string, json_fragment);
        Ok(trigger)
    } else {
        Err(PowerSyncError::argument_error("invalid flags for table"))
    };
}

create_sqlite_text_fn!(
    powersync_trigger_insert_sql,
    powersync_trigger_insert_sql_impl,
    "powersync_trigger_insert_sql"
);

fn powersync_trigger_update_sql_impl(
    _ctx: *mut sqlite::context,
    args: &[*mut sqlite::value],
) -> Result<String, PowerSyncError> {
    let table_info = Table::from_json(args[0].text()).map_err(PowerSyncError::as_argument_error)?;

    let name = &table_info.name;
    let view_name = &table_info.view_name();
    let insert_only = table_info.flags.insert_only();
    let local_only = table_info.flags.local_only();

    let quoted_name = quote_identifier(view_name);
    let internal_name = quote_internal_name(name, local_only);
    let trigger_name = quote_identifier_prefixed("ps_view_update_", view_name);
    let type_string = quote_string(name);

    let json_fragment_new = json_object_fragment("NEW", &mut table_info.columns.iter())?;
    let json_fragment_old = json_object_fragment("OLD", &mut table_info.columns.iter())?;

    let mut old_values_fragment = match &table_info.diff_include_old {
        None => None,
        Some(DiffIncludeOld::ForAllColumns) => Some(json_fragment_old.clone()),
        Some(DiffIncludeOld::OnlyForColumns { columns }) => Some(json_object_fragment(
            "OLD",
            &mut table_info.filtered_columns(columns.iter().map(|c| c.as_str())),
        )?),
    };

    if table_info.flags.include_old_only_when_changed() {
        old_values_fragment = match old_values_fragment {
            None => None,
            Some(f) => {
                let filtered_new_fragment = match &table_info.diff_include_old {
                    // When include_old_only_when_changed is combined with a column filter, make sure we
                    // only include the powersync_diff of columns matched by the filter.
                    Some(DiffIncludeOld::OnlyForColumns { columns }) => {
                        Cow::Owned(json_object_fragment(
                            "NEW",
                            &mut table_info.filtered_columns(columns.iter().map(|c| c.as_str())),
                        )?)
                    }
                    _ => Cow::Borrowed(json_fragment_new.as_str()),
                };

                Some(format!(
                    "json(powersync_diff({filtered_new_fragment}, {f}))"
                ))
            }
        }
    }

    let (old_key, old_value): (&'static str, Cow<'static, str>) = match old_values_fragment {
        Some(f) => (",old_values", format!(",{f}").into()),
        None => ("", "".into()),
    };

    let (metadata_key, metadata_value) = if table_info.flags.include_metadata() {
        (",metadata", ",NEW._metadata")
    } else {
        ("", "")
    };

    return if !local_only && !insert_only {
        // If we're supposed to include metadata, we support UPDATE ... SET _deleted = TRUE with
        // another trigger (because there's no way to attach data to DELETE statements otherwise).
        let when = if table_info.flags.include_metadata() {
            " WHEN NEW._deleted IS NOT TRUE"
        } else {
            ""
        };

        let flags = table_info.flags.0;

        let trigger = format!("\
CREATE TRIGGER {trigger_name}
INSTEAD OF UPDATE ON {quoted_name}
FOR EACH ROW{when}
BEGIN
  SELECT CASE
  WHEN (OLD.id != NEW.id)
  THEN RAISE (FAIL, 'Cannot update id')
  END;
  UPDATE {internal_name}
      SET data = {json_fragment_new}
      WHERE id = NEW.id;
  INSERT INTO powersync_crud(op,type,id,data,options{old_key}{metadata_key}) VALUES ('PATCH',{type_string},NEW.id,json(powersync_diff({:}, {:})),{flags}{old_value}{metadata_value});
END", json_fragment_old, json_fragment_new);
        Ok(trigger)
    } else if local_only {
        debug_assert!(!table_info.flags.include_metadata());

        let trigger = format!(
            "\
CREATE TRIGGER {trigger_name}
INSTEAD OF UPDATE ON {quoted_name}
FOR EACH ROW
BEGIN
  SELECT CASE
  WHEN (OLD.id != NEW.id)
  THEN RAISE (FAIL, 'Cannot update id')
  END;
  UPDATE {internal_name}
      SET data = {json_fragment_new}
      WHERE id = NEW.id;
END"
        );
        Ok(trigger)
    } else if insert_only {
        Ok(String::from(""))
    } else {
        Err(PowerSyncError::argument_error("invalid flags for table"))
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
fn json_object_fragment<'a>(
    prefix: &str,
    columns: &mut dyn Iterator<Item = &'a Column>,
) -> Result<String, PowerSyncError> {
    // floor(SQLITE_MAX_FUNCTION_ARG / 2).
    // To keep databases portable, we use the default limit of 100 args for this,
    // and don't try to query the limit dynamically.
    const MAX_ARG_COUNT: usize = 50;

    let mut column_names_quoted: Vec<String> = alloc::vec![];
    while let Some(column) = columns.next() {
        let name = &*column.name;

        // We really want the individual columns here to appear as they show up in the database.
        // For text columns however, it's possible that e.g. NEW.column was created by a JSON
        // function, meaning that it has a JSON subtype active - causing the json_object() call
        // we're about to emit to include it as a subobject instead of a string.
        column_names_quoted.push(format!(
            "{:}, powersync_strip_subtype({:}.{:})",
            QuotedString(name),
            prefix,
            quote_identifier(name)
        ));
    }

    // SQLITE_MAX_COLUMN - 1 (because of the id column)
    if column_names_quoted.len() > 1999 {
        return Err(PowerSyncError::argument_error(
            "too many parameters to json_object_fragment",
        ));
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
