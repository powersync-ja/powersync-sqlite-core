extern crate alloc;

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::ffi::c_int;

use sqlite::{Connection, ResultCode, Value};
use sqlite_nostd as sqlite;
use sqlite_nostd::Context;

use crate::error::{PSResult, SQLiteError};
use crate::ext::ExtendedDatabase;
use crate::util::{quote_identifier, quote_json_path};
use crate::{create_auto_tx_function, create_sqlite_text_fn};

fn update_tables(db: *mut sqlite::sqlite3, schema: &str) -> Result<(), SQLiteError> {
    {
        // In a block so that the statement is finalized before dropping tables
        // language=SQLite
        let statement = db
            .prepare_v2(
                "\
SELECT
        json_extract(json_each.value, '$.name') as name,
        powersync_internal_table_name(json_each.value) as internal_name,
        ifnull(json_extract(json_each.value, '$.local_only'), 0) as local_only
      FROM json_each(json_extract(?, '$.tables'))
        WHERE name NOT IN (SELECT name FROM powersync_tables)",
            )
            .into_db_result(db)?;
        statement.bind_text(1, schema, sqlite::Destructor::STATIC)?;

        while statement.step().into_db_result(db)? == ResultCode::ROW {
            let name = statement.column_text(0)?;
            let internal_name = statement.column_text(1)?;
            let local_only = statement.column_int(2) != 0;

            db.exec_safe(&format!(
                "CREATE TABLE {:}(id TEXT PRIMARY KEY NOT NULL, data TEXT)",
                quote_identifier(internal_name)
            ))
            .into_db_result(db)?;

            if !local_only {
                // MOVE data if any
                db.exec_text(
                    &format!(
                        "INSERT INTO {:}(id, data)
    SELECT id, data
    FROM ps_untyped
    WHERE type = ?",
                        quote_identifier(internal_name)
                    ),
                    name,
                )
                .into_db_result(db)?;

                // language=SQLite
                db.exec_text(
                    "DELETE
    FROM ps_untyped
    WHERE type = ?",
                    name,
                )?;
            }

            if !local_only {
                // MOVE data if any
                db.exec_text(
                    &format!(
                        "INSERT INTO {:}(id, data)
    SELECT id, data
    FROM ps_untyped
    WHERE type = ?",
                        quote_identifier(internal_name)
                    ),
                    name,
                )
                .into_db_result(db)?;

                // language=SQLite
                db.exec_text(
                    "DELETE
    FROM ps_untyped
    WHERE type = ?",
                    name,
                )?;
            }
        }
    }

    let mut tables_to_drop: Vec<String> = alloc::vec![];

    {
        // In a block so that the statement is finalized before dropping tables
        // language=SQLite
        let statement = db
            .prepare_v2(
                "\
SELECT name, internal_name, local_only FROM powersync_tables WHERE name NOT IN (
    SELECT json_extract(json_each.value, '$.name')
    FROM json_each(json_extract(?, '$.tables'))
  )",
            )
            .into_db_result(db)?;
        statement.bind_text(1, schema, sqlite::Destructor::STATIC)?;

        while statement.step()? == ResultCode::ROW {
            let name = statement.column_text(0)?;
            let internal_name = statement.column_text(1)?;
            let local_only = statement.column_int(2) != 0;

            tables_to_drop.push(String::from(internal_name));

            if !local_only {
                db.exec_text(
                    &format!(
                        "INSERT INTO ps_untyped(type, id, data) SELECT ?, id, data FROM {:}",
                        quote_identifier(internal_name)
                    ),
                    name,
                )
                .into_db_result(db)?;
            }
        }
    }

    // We cannot have any open queries on sqlite_master at the point that we drop tables, otherwise
    // we get "table is locked" errors.
    for internal_name in tables_to_drop {
        let q = format!("DROP TABLE {:}", quote_identifier(&internal_name));
        db.exec_safe(&q).into_db_result(db)?;
    }

    Ok(())
}

fn update_indexes(db: *mut sqlite::sqlite3, schema: &str) -> Result<(), SQLiteError> {
    let mut statements: Vec<String> = alloc::vec![];

    {
        // In a block so that the statement is finalized before dropping indexes
        // language=SQLite
        let statement = db.prepare_v2("\
SELECT
        powersync_internal_table_name(tables.value) as table_name,
        (powersync_internal_table_name(tables.value) || '__' || json_extract(indexes.value, '$.name')) as index_name,
        json_extract(indexes.value, '$.columns') as index_columns,
        ifnull(sqlite_master.sql, '') as sql
      FROM json_each(json_extract(?, '$.tables')) tables
      CROSS JOIN json_each(json_extract(tables.value, '$.indexes')) indexes
      LEFT JOIN sqlite_master ON sqlite_master.name = index_name AND sqlite_master.type = 'index'
      ").into_db_result(db)?;
        statement.bind_text(1, schema, sqlite::Destructor::STATIC)?;

        while statement.step().into_db_result(db)? == ResultCode::ROW {
            let table_name = statement.column_text(0)?;
            let index_name = statement.column_text(1)?;
            let columns = statement.column_text(2)?;
            let existing_sql = statement.column_text(3)?;

            // language=SQLite
            let stmt2 = db.prepare_v2("select json_extract(e.value, '$.name') as name, json_extract(e.value, '$.type') as type, json_extract(e.value, '$.ascending') as ascending from json_each(?) e")?;
            stmt2.bind_text(1, columns, sqlite::Destructor::STATIC)?;

            let mut column_values: Vec<String> = alloc::vec![];
            while stmt2.step()? == ResultCode::ROW {
                let name = stmt2.column_text(0)?;
                let type_name = stmt2.column_text(1)?;
                let ascending = stmt2.column_int(2) != 0;

                if ascending {
                    let value = format!(
                        "CAST(json_extract(data, {:}) as {:})",
                        quote_json_path(name),
                        type_name
                    );
                    column_values.push(value);
                } else {
                    let value = format!(
                        "CAST(json_extract(data, {:}) as {:}) DESC",
                        quote_json_path(name),
                        type_name
                    );
                    column_values.push(value);
                }
            }

            let sql = format!(
                "CREATE INDEX {} ON {}({})",
                quote_identifier(index_name),
                quote_identifier(table_name),
                column_values.join(", ")
            );
            if existing_sql == "" {
                statements.push(sql);
            } else if existing_sql != sql {
                statements.push(format!("DROP INDEX {}", quote_identifier(index_name)));
                statements.push(sql);
            }
        }

        // In a block so that the statement is finalized before dropping indexes
        // language=SQLite
        let statement = db.prepare_v2("\
WITH schema_indexes AS (
SELECT
        powersync_internal_table_name(tables.value) as table_name,
        (powersync_internal_table_name(tables.value) || '__' || json_extract(indexes.value, '$.name')) as index_name
      FROM json_each(json_extract(?1, '$.tables')) tables
      CROSS JOIN json_each(json_extract(tables.value, '$.indexes')) indexes
)
SELECT
    sqlite_master.name as index_name
      FROM sqlite_master
          WHERE sqlite_master.type = 'index'
            AND sqlite_master.name GLOB 'ps_data_*'
            AND sqlite_master.name NOT IN (SELECT index_name FROM schema_indexes)
").into_db_result(db)?;
        statement.bind_text(1, schema, sqlite::Destructor::STATIC)?;

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

fn update_views(db: *mut sqlite::sqlite3, schema: &str) -> Result<(), SQLiteError> {
    // Update existing views if modified
    // language=SQLite
    db.exec_text("\
UPDATE powersync_views SET
sql = gen.sql,
delete_trigger_sql = gen.delete_trigger_sql,
insert_trigger_sql = gen.insert_trigger_sql,
update_trigger_sql = gen.update_trigger_sql
FROM (SELECT
      ifnull(json_extract(json_each.value, '$.view_name'), json_extract(json_each.value, '$.name')) as name,
                   powersync_view_sql(json_each.value) as sql,
                   powersync_trigger_delete_sql(json_each.value) as delete_trigger_sql,
                   powersync_trigger_insert_sql(json_each.value) as insert_trigger_sql,
                   powersync_trigger_update_sql(json_each.value) as update_trigger_sql
                   FROM json_each(json_extract(?, '$.tables'))) as gen
                   WHERE powersync_views.name = gen.name AND
                       (powersync_views.sql IS NOT gen.sql OR
                        powersync_views.delete_trigger_sql IS NOT gen.delete_trigger_sql OR
                        powersync_views.insert_trigger_sql IS NOT gen.insert_trigger_sql OR
                        powersync_views.update_trigger_sql IS NOT gen.update_trigger_sql)
    ", schema).into_db_result(db)?;

    // Create new views
    // language=SQLite
    db.exec_text("\
INSERT INTO powersync_views(
    name,
    sql,
    delete_trigger_sql,
    insert_trigger_sql,
    update_trigger_sql
)
SELECT
ifnull(json_extract(json_each.value, '$.view_name'), json_extract(json_each.value, '$.name')) as name,
             powersync_view_sql(json_each.value) as sql,
             powersync_trigger_delete_sql(json_each.value) as delete_trigger_sql,
             powersync_trigger_insert_sql(json_each.value) as insert_trigger_sql,
             powersync_trigger_update_sql(json_each.value) as update_trigger_sql
             FROM json_each(json_extract(?, '$.tables'))
                            WHERE name NOT IN (SELECT name FROM powersync_views)", schema).into_db_result(db)?;

    // Delete old views
    // language=SQLite
    db.exec_text("\
DELETE FROM powersync_views WHERE name NOT IN (
    SELECT ifnull(json_extract(json_each.value, '$.view_name'), json_extract(json_each.value, '$.name'))
                        FROM json_each(json_extract(?, '$.tables'))
            )", schema).into_db_result(db)?;

    Ok(())
}

// SELECT powersync_replace_schema('{"tables": [{"name": "test", "columns": [{"name": "name", "type": "text"}]}]}');
// This cannot be a TRIGGER or a virtual table insert. There are locking issues due to both
// querying sqlite_master and dropping tables in those cases, which are not present when this is
// a plain function.
fn powersync_replace_schema_impl(
    ctx: *mut sqlite::context,
    args: &[*mut sqlite::value],
) -> Result<String, SQLiteError> {
    let schema = args[0].text();
    let db = ctx.db_handle();

    // language=SQLite
    db.exec_safe("SELECT powersync_init()").into_db_result(db)?;

    update_tables(db, schema)?;
    update_indexes(db, schema)?;
    update_views(db, schema)?;

    Ok(String::from(""))
}

create_auto_tx_function!(powersync_replace_schema_tx, powersync_replace_schema_impl);
create_sqlite_text_fn!(
    powersync_replace_schema,
    powersync_replace_schema_tx,
    "powersync_replace_schema"
);

pub fn register(db: *mut sqlite::sqlite3) -> Result<(), ResultCode> {
    db.create_function_v2(
        "powersync_replace_schema",
        1,
        sqlite::UTF8,
        None,
        Some(powersync_replace_schema),
        None,
        None,
        None,
    )?;

    Ok(())
}
