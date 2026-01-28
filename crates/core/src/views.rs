extern crate alloc;

use alloc::borrow::Cow;
use alloc::string::String;
use alloc::{format, vec};
use core::fmt::{Write, from_fn};
use core::mem;

use crate::error::PowerSyncError;
use crate::schema::{Column, DiffIncludeOld, Table};
use crate::utils::{InsertIntoCrud, SqlBuffer};

pub fn powersync_view_sql(table_info: &Table) -> String {
    let name = &table_info.name;
    let view_name = &table_info.view_name();
    let local_only = table_info.flags.local_only();
    let include_metadata = table_info.flags.include_metadata();

    let mut sql = SqlBuffer::new();
    sql.push_str("CREATE VIEW ");
    let _ = sql.identifier().write_str(view_name);
    sql.push_char('(');
    {
        let mut sql = sql.comma_separated();
        let _ = sql.element().identifier().write_str("id");

        for column in &table_info.columns {
            let _ = sql.element().identifier().write_str(&column.name);
        }

        if include_metadata {
            let _ = sql.element().identifier().write_str("_metadata");
            let _ = sql.element().identifier().write_str("_deleted");
        }
    }

    sql.push_str(") AS SELECT ");
    {
        let mut sql = sql.comma_separated();
        sql.element().push_str("id");

        for column in &table_info.columns {
            let sql = sql.element();

            sql.json_extract_and_cast("data", &column.name, &column.type_name);
        }

        if include_metadata {
            // For _metadata and _deleted columns
            sql.element().push_str("NULL");
            sql.element().push_str("NULL");
        }
    }

    sql.push_str(" FROM ");
    sql.quote_internal_name(name, local_only);
    sql.push_str(" -- powersync-auto-generated");

    return sql.sql;
}

pub fn powersync_trigger_delete_sql(table_info: &Table) -> Result<String, PowerSyncError> {
    if table_info.flags.insert_only() {
        // Insert-only tables have no DELETE triggers
        return Ok(String::new());
    }

    let name = &table_info.name;
    let view_name = table_info.view_name();
    let local_only = table_info.flags.local_only();

    let mut sql = SqlBuffer::new();
    sql.create_trigger("ps_view_delete_", view_name);
    sql.trigger_instead_of("DELETE", view_name);
    sql.push_str("BEGIN\n");
    // First, forward to internal data table.
    sql.push_str("DELETE FROM ");
    sql.quote_internal_name(name, local_only);
    sql.push_str(" WHERE id = OLD.id;\n");

    let old_data_value = match &table_info.diff_include_old {
        Some(include_old) => {
            let json = match include_old {
                DiffIncludeOld::OnlyForColumns { columns } => json_object_fragment(
                    "OLD",
                    &mut table_info.filtered_columns(columns.iter().map(|c| c.as_str())),
                ),
                DiffIncludeOld::ForAllColumns => {
                    json_object_fragment("OLD", &mut table_info.columns.iter())
                }
            }?;

            Some(json)
        }
        None => None,
    };

    if !local_only {
        // We also need to record the write in powersync_crud.
        sql.insert_into_powersync_crud(InsertIntoCrud {
            op: "DELETE",
            id_expr: "OLD.id",
            type_name: name,
            data: None::<&'static str>,
            old_values: old_data_value.as_ref(),
            metadata: None::<&'static str>,
            options: None,
        });

        if table_info.flags.include_metadata() {
            // The DELETE statement can't include metadata for the delete operation, so we create
            // another trigger to delete with a fake UPDATE syntax.
            sql.trigger_end();
            sql.push_str(";\n");

            sql.create_trigger("ps_view_delete2_", view_name);
            sql.trigger_instead_of("UPDATE", view_name);
            sql.push_str("WHEN NEW._deleted IS TRUE BEGIN DELETE FROM ");
            sql.quote_internal_name(name, local_only);
            sql.push_str(" WHERE id = OLD.id; ");

            sql.insert_into_powersync_crud(InsertIntoCrud {
                op: "DELETE",
                id_expr: "OLD.id",
                type_name: name,
                data: None::<&'static str>,
                old_values: old_data_value.as_ref(),
                metadata: Some("NEW._metadata"),
                options: None,
            });
        }
    }

    sql.trigger_end();
    return Ok(sql.sql);
}

pub fn powersync_trigger_insert_sql(table_info: &Table) -> Result<String, PowerSyncError> {
    let name = &table_info.name;
    let view_name = table_info.view_name();
    let local_only = table_info.flags.local_only();
    let insert_only = table_info.flags.insert_only();

    let mut sql = SqlBuffer::new();
    sql.create_trigger("ps_view_insert_", view_name);
    sql.trigger_instead_of("INSERT", view_name);
    sql.push_str("BEGIN\n");

    if !local_only {
        sql.check_id_valid();
    }

    let json_fragment = json_object_fragment("NEW", &mut table_info.columns.iter())?;

    if insert_only {
        // This is using the manual powersync_crud_ instead of powersync_crud because insert-only
        // writes shouldn't prevent us from receiving new data.
        sql.push_str("INSERT INTO powersync_crud_(data) VALUES(json_object('op', 'PUT', 'type', ");
        let _ = sql.string_literal().write_str(name);

        let _ = write!(
            &mut sql,
            ", 'id', NEW.id, 'data', json(powersync_diff('{{}}', {:}))));",
            json_fragment,
        );
    } else {
        // Insert into the underlying data table.
        sql.push_str("INSERT INTO ");
        sql.quote_internal_name(name, local_only);
        let _ = write!(&mut sql, " SELECT NEW.id, {json_fragment};\n");

        if !local_only {
            // Record write into powersync_crud
            sql.insert_into_powersync_crud(InsertIntoCrud {
                op: "PUT",
                id_expr: "NEW.id",
                type_name: name,
                data: Some(from_fn(|f| {
                    write!(f, "json(powersync_diff('{{}}', {:}))", json_fragment)
                })),
                old_values: None::<&'static str>,
                metadata: if table_info.flags.include_metadata() {
                    Some("NEW._metadata")
                } else {
                    None
                },
                options: None,
            });
        }
    }

    sql.trigger_end();
    Ok(sql.sql)
}

pub fn powersync_trigger_update_sql(table_info: &Table) -> Result<String, PowerSyncError> {
    if table_info.flags.insert_only() {
        // Insert-only tables have no UPDATE triggers
        return Ok(String::new());
    }

    let name = &table_info.name;
    let view_name = table_info.view_name();
    let local_only = table_info.flags.local_only();

    let mut sql = SqlBuffer::new();
    sql.create_trigger("ps_view_update_", view_name);
    sql.trigger_instead_of("UPDATE", view_name);

    // If we're supposed to include metadata, we support UPDATE ... SET _deleted = TRUE with
    // another trigger (because there's no way to attach data to DELETE statements otherwise).
    if table_info.flags.include_metadata() {
        sql.push_str(" WHEN NEW._deleted IS NOT TRUE ");
    }
    sql.push_str("BEGIN\n");
    sql.check_id_not_changed();

    let json_fragment_new = json_object_fragment("NEW", &mut table_info.columns.iter())?;
    let json_fragment_old = json_object_fragment("OLD", &mut table_info.columns.iter())?;

    // UPDATE {internal_name} SET data = {json_fragment_new} WHERE id = NEW.id;
    sql.push_str("UPDATE ");
    sql.quote_internal_name(name, local_only);
    let _ = write!(
        &mut sql,
        " SET data = {json_fragment_new} WHERE id = NEW.id;\n"
    );

    if !local_only {
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
                                &mut table_info
                                    .filtered_columns(columns.iter().map(|c| c.as_str())),
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

        // Also forward write to powersync_crud vtab.
        sql.insert_into_powersync_crud(InsertIntoCrud {
            op: "PATCH",
            id_expr: "NEW.id",
            type_name: name,
            data: Some(from_fn(|f| {
                write!(
                    f,
                    "json(powersync_diff({json_fragment_old}, {json_fragment_new}))"
                )
            })),
            old_values: old_values_fragment.as_ref(),
            metadata: if table_info.flags.include_metadata() {
                Some("NEW._metadata")
            } else {
                None
            },
            options: Some(table_info.flags.0),
        });
    }

    sql.trigger_end();
    Ok(sql.sql)
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

    let mut pending_json_object_invocations = vec![];
    let mut pending_json_object = None::<(usize, SqlBuffer)>;
    let mut total_columns = 0usize;

    fn new_pending_object() -> (usize, SqlBuffer) {
        let mut buffer = SqlBuffer::new();
        buffer.push_str("json_object(");
        (0, buffer)
    }

    fn build_pending_object(obj: &mut (usize, SqlBuffer)) -> String {
        obj.1.push_char(')'); // close json_object( invocation
        let (_, buffer) = mem::replace(obj, new_pending_object());
        buffer.sql
    }

    while let Some(column) = columns.next() {
        total_columns += 1;
        // SQLITE_MAX_COLUMN - 1 (because of the id column)
        if total_columns > 1999 {
            return Err(PowerSyncError::argument_error(
                "too many parameters to json_object_fragment",
            ));
        }

        let name = &*column.name;

        let pending_object = pending_json_object.get_or_insert_with(new_pending_object);
        if pending_object.0 == MAX_ARG_COUNT {
            // We already have 50 key-value pairs in this call, finish.
            pending_json_object_invocations.push(build_pending_object(pending_object));
        }

        let existing_elements = pending_object.0;
        let sql = &mut pending_object.1;

        if pending_object.0 != 0 {
            sql.comma();
        }

        // Append a "key", powersync_strip_subtype(prefix."KEY") pair to the json_each invocation.
        let _ = sql.string_literal().write_str(name); // JSON object key
        sql.comma();

        // We really want the individual columns here to appear as they show up in the database.
        // For text columns however, it's possible that e.g. NEW.column was created by a JSON
        // function, meaning that it has a JSON subtype active - causing the json_object() call
        // we're about to emit to include it as a subobject instead of a string.
        sql.push_str("powersync_strip_subtype(");
        sql.push_str(prefix);
        sql.push_char('.');
        let _ = sql.identifier().write_str(name);
        sql.push_char(')');

        pending_object.0 = existing_elements + 1;
    }

    if pending_json_object_invocations.is_empty() {
        // Not exceeding 50 elements, return single json_object invocation.
        Ok(build_pending_object(
            pending_json_object.get_or_insert_with(new_pending_object),
        ))
    } else {
        // Too many columns to use json_object directly. Instead, we build up the JSON object in
        // chunks, and merge using powersync_json_merge().
        if let Some(pending) = &mut pending_json_object {
            pending_json_object_invocations.push(build_pending_object(pending));
        }

        let mut sql = SqlBuffer::new();
        sql.push_str("powersync_json_merge(");
        let mut comma_separated = sql.comma_separated();
        for fragment in pending_json_object_invocations {
            let _ = comma_separated.element().push_str(&fragment);
        }
        sql.push_char(')');
        Ok(sql.sql)
    }
}

#[cfg(test)]
mod test {
    use alloc::{string::ToString, vec};

    use crate::{
        schema::{Column, Table, TableInfoFlags},
        views::{
            json_object_fragment, powersync_trigger_delete_sql, powersync_trigger_insert_sql,
            powersync_trigger_update_sql, powersync_view_sql,
        },
    };

    fn test_table() -> Table {
        return Table {
            name: "table".to_string(),
            view_name_override: None,
            columns: vec![
                Column {
                    name: "a".to_string(),
                    type_name: "text".to_string(),
                },
                Column {
                    name: "b".to_string(),
                    type_name: "integer".to_string(),
                },
            ],
            indexes: vec![],
            diff_include_old: None,
            flags: TableInfoFlags::default(),
        };
    }

    #[test]
    fn test_json_object_fragment() {
        let fragment =
            json_object_fragment("NEW", &mut test_table().columns.iter()).expect("should generate");

        assert_eq!(
            fragment,
            r#"json_object('a', powersync_strip_subtype(NEW."a"), 'b', powersync_strip_subtype(NEW."b"))"#
        );
    }

    #[test]
    fn test_view() {
        let stmt = powersync_view_sql(&test_table());

        assert_eq!(
            stmt,
            r#"CREATE VIEW "table"("id", "a", "b") AS SELECT "id", CAST(json_extract(data, '$.a') as text), CAST(json_extract(data, '$.b') as integer) FROM "ps_data__table" -- powersync-auto-generated"#
        );
    }

    #[test]
    fn test_delete_trigger() {
        let stmt = powersync_trigger_delete_sql(&test_table()).expect("should generate");

        assert_eq!(
            stmt,
            r#"CREATE TRIGGER "ps_view_delete_table" INSTEAD OF DELETE ON "table" FOR EACH ROW BEGIN DELETE FROM "ps_data__table" WHERE id = OLD.id; INSERT INTO powersync_crud(op,id,type) VALUES ('DELETE', OLD.id, 'table');END"#
        );
    }

    #[test]
    fn local_only_does_not_write_into_ps_crud() {
        let mut table = test_table();
        table.flags.0 = 1; // local-only bit

        assert!(
            !powersync_trigger_insert_sql(&table)
                .unwrap()
                .contains("powersync_crud")
        );
        assert!(
            !powersync_trigger_update_sql(&table)
                .unwrap()
                .contains("powersync_crud")
        );
        assert!(
            !powersync_trigger_delete_sql(&table)
                .unwrap()
                .contains("powersync_crud")
        );
    }
}
