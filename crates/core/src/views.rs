extern crate alloc;

use alloc::string::String;
use alloc::vec;
use core::fmt::{Write, from_fn};
use core::mem;

use crate::error::PowerSyncError;
use crate::schema::{ColumnFilter, SchemaTable, Table};
use crate::utils::{InsertIntoCrud, SqlBuffer, WriteType};

pub fn powersync_view_sql(table_info: &Table) -> String {
    let name = &table_info.name;
    let view_name = &table_info.view_name();
    let local_only = table_info.options.flags.local_only();
    let include_metadata = table_info.options.flags.include_metadata();

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
    if table_info.options.flags.insert_only() {
        // Insert-only tables have no DELETE triggers
        return Ok(String::new());
    }

    let name = &table_info.name;
    let view_name = table_info.view_name();
    let local_only = table_info.options.flags.local_only();
    let as_schema_table = SchemaTable::from(table_info);

    let mut sql = SqlBuffer::new();
    sql.create_trigger("ps_view_delete_", view_name);
    sql.trigger_instead_of(WriteType::Delete, view_name);
    sql.push_str("BEGIN\n");
    // First, forward to internal data table.
    sql.push_str("DELETE FROM ");
    sql.quote_internal_name(name, local_only);
    sql.push_str(" WHERE id = OLD.id;\n");

    if !local_only {
        // We also need to record the write in powersync_crud.
        sql.insert_into_powersync_crud(InsertIntoCrud {
            op: WriteType::Delete,
            table: &as_schema_table,
            id_expr: "OLD.id",
            type_name: name,
            data: None::<&'static str>,
            metadata: None::<&'static str>,
        })?;

        if table_info.options.flags.include_metadata() {
            // The DELETE statement can't include metadata for the delete operation, so we create
            // another trigger to delete with a fake UPDATE syntax.
            sql.trigger_end();
            sql.push_str(";\n");

            sql.create_trigger("ps_view_delete2_", view_name);
            sql.trigger_instead_of(WriteType::Update, view_name);
            sql.push_str("WHEN NEW._deleted IS TRUE BEGIN DELETE FROM ");
            sql.quote_internal_name(name, local_only);
            sql.push_str(" WHERE id = OLD.id; ");

            sql.insert_into_powersync_crud(InsertIntoCrud {
                op: WriteType::Delete,
                table: &as_schema_table,
                id_expr: "OLD.id",
                type_name: name,
                data: None::<&'static str>,
                metadata: Some("NEW._metadata"),
            })?;
        }
    }

    sql.trigger_end();
    return Ok(sql.sql);
}

pub fn powersync_trigger_insert_sql(table_info: &Table) -> Result<String, PowerSyncError> {
    let name = &table_info.name;
    let view_name = table_info.view_name();
    let local_only = table_info.options.flags.local_only();
    let insert_only = table_info.options.flags.insert_only();
    let as_schema_table = SchemaTable::from(table_info);

    let mut sql = SqlBuffer::new();
    sql.create_trigger("ps_view_insert_", view_name);
    sql.trigger_instead_of(WriteType::Insert, view_name);
    sql.push_str("BEGIN\n");

    if !local_only {
        sql.check_id_valid();
    }

    let json_fragment = table_columns_to_json_object("NEW", &as_schema_table)?;

    if insert_only {
        // This is using the manual powersync_crud_ instead of powersync_crud because insert-only
        // writes shouldn't prevent us from receiving new data.
        sql.powersync_crud_manual_put(name, &json_fragment);
    } else {
        // Insert into the underlying data table.
        sql.push_str("INSERT INTO ");
        sql.quote_internal_name(name, local_only);
        let _ = write!(&mut sql, " SELECT NEW.id, {json_fragment};\n");

        if !local_only {
            // Record write into powersync_crud
            sql.insert_into_powersync_crud(InsertIntoCrud {
                op: WriteType::Insert,
                id_expr: "NEW.id",
                table: &as_schema_table,
                type_name: name,
                data: Some(from_fn(|f| {
                    write!(f, "json(powersync_diff('{{}}', {:}))", json_fragment)
                })),
                metadata: if table_info.options.flags.include_metadata() {
                    Some("NEW._metadata")
                } else {
                    None
                },
            })?;
        }
    }

    sql.trigger_end();
    Ok(sql.sql)
}

pub fn powersync_trigger_update_sql(table_info: &Table) -> Result<String, PowerSyncError> {
    if table_info.options.flags.insert_only() {
        // Insert-only tables have no UPDATE triggers
        return Ok(String::new());
    }

    let name = &table_info.name;
    let view_name = table_info.view_name();
    let local_only = table_info.options.flags.local_only();
    let as_schema_table = SchemaTable::from(table_info);

    let mut sql = SqlBuffer::new();
    sql.create_trigger("ps_view_update_", view_name);
    sql.trigger_instead_of(WriteType::Update, view_name);

    // If we're supposed to include metadata, we support UPDATE ... SET _deleted = TRUE with
    // another trigger (because there's no way to attach data to DELETE statements otherwise).
    if table_info.options.flags.include_metadata() {
        sql.push_str(" WHEN NEW._deleted IS NOT TRUE ");
    }
    sql.push_str("BEGIN\n");
    sql.check_id_not_changed();

    let json_fragment_new = table_columns_to_json_object("NEW", &as_schema_table)?;
    let json_fragment_old = table_columns_to_json_object("OLD", &as_schema_table)?;

    // UPDATE {internal_name} SET data = {json_fragment_new} WHERE id = NEW.id;
    sql.push_str("UPDATE ");
    sql.quote_internal_name(name, local_only);
    let _ = write!(
        &mut sql,
        " SET data = {json_fragment_new} WHERE id = NEW.id;\n"
    );

    if !local_only {
        // Also forward write to powersync_crud vtab.
        sql.insert_into_powersync_crud(InsertIntoCrud {
            op: WriteType::Update,
            id_expr: "NEW.id",
            table: &as_schema_table,
            type_name: name,
            data: Some(from_fn(|f| {
                write!(
                    f,
                    "json(powersync_diff({json_fragment_old}, {json_fragment_new}))"
                )
            })),
            metadata: if table_info.options.flags.include_metadata() {
                Some("NEW._metadata")
            } else {
                None
            },
        })?;
    }

    sql.trigger_end();
    Ok(sql.sql)
}

/// Given a query returning column names, return a JSON object fragment for a trigger.
///
/// Example output with prefix "NEW": "json_object('id', NEW.id, 'name', NEW.name, 'age', NEW.age)".
pub fn table_columns_to_json_object<'a>(
    prefix: &str,
    table: &'a SchemaTable<'a>,
) -> Result<String, PowerSyncError> {
    table_columns_to_json_object_with_filter(prefix, table, None)
}

pub fn table_columns_to_json_object_with_filter<'a>(
    prefix: &str,
    table: &'a SchemaTable<'a>,
    filter: Option<&'a ColumnFilter>,
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

    let mut columns = table.column_names();
    while let Some(name) = columns.next() {
        if let Some(filter) = filter
            && !filter.matches(name)
        {
            continue;
        }

        total_columns += 1;
        // SQLITE_MAX_COLUMN - 1 (because of the id column)
        if total_columns > 1999 {
            return Err(PowerSyncError::argument_error(
                "too many parameters to json_object_fragment",
            ));
        }

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
        schema::{Column, Table},
        views::{
            powersync_trigger_delete_sql, powersync_trigger_insert_sql,
            powersync_trigger_update_sql, powersync_view_sql, table_columns_to_json_object,
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
            options: Default::default(),
        };
    }

    #[test]
    fn test_json_object_fragment() {
        let fragment =
            table_columns_to_json_object("NEW", &(&test_table()).into()).expect("should generate");

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
            r#"CREATE VIEW "table"("id", "a", "b") AS SELECT id, CAST(json_extract(data, '$.a') as text), CAST(json_extract(data, '$.b') as integer) FROM "ps_data__table" -- powersync-auto-generated"#
        );
    }

    #[test]
    fn test_delete_trigger() {
        let stmt = powersync_trigger_delete_sql(&test_table()).expect("should generate");

        assert_eq!(
            stmt,
            r#"CREATE TRIGGER "ps_view_delete_table" INSTEAD OF DELETE ON "table" FOR EACH ROW BEGIN
DELETE FROM "ps_data__table" WHERE id = OLD.id;
INSERT INTO powersync_crud(op,id,type) VALUES ('DELETE', OLD.id, 'table');
END"#
        );
    }

    #[test]
    fn local_only_does_not_write_into_ps_crud() {
        let mut table = test_table();
        table.options.flags.0 = 1; // local-only bit

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
