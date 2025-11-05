extern crate alloc;

use alloc::format;
use alloc::rc::Rc;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::ffi::{c_int, c_void};

use powersync_sqlite_nostd as sqlite;
use powersync_sqlite_nostd::{Connection, Context};
use sqlite::{ResultCode, Value};

use crate::error::PowerSyncError;
use crate::migrations::{LATEST_VERSION, powersync_migrate};
use crate::schema::inspection::ExistingView;
use crate::state::DatabaseState;
use crate::util::quote_identifier;
use crate::{create_auto_tx_function, create_sqlite_text_fn};

// Used in old down migrations, do not remove.
extern "C" fn powersync_drop_view(
    ctx: *mut sqlite::context,
    argc: c_int,
    argv: *mut *mut sqlite::value,
) {
    let args = sqlite::args!(argc, argv);
    let name = args[0].text();

    if let Err(e) = ExistingView::drop_by_name(ctx.db_handle(), name) {
        e.apply_to_ctx("powersync_drop_view", ctx);
    }
}

fn powersync_init_impl(
    ctx: *mut sqlite::context,
    _args: &[*mut sqlite::value],
) -> Result<String, PowerSyncError> {
    powersync_migrate(ctx, LATEST_VERSION)?;

    Ok(String::from(""))
}

create_auto_tx_function!(powersync_init_tx, powersync_init_impl);
create_sqlite_text_fn!(powersync_init, powersync_init_tx, "powersync_init");

fn powersync_test_migration_impl(
    ctx: *mut sqlite::context,
    args: &[*mut sqlite::value],
) -> Result<String, PowerSyncError> {
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
) -> Result<String, PowerSyncError> {
    let local_db = ctx.db_handle();
    let state = unsafe { DatabaseState::from_context(&ctx) };

    let flags = PowerSyncClearFlags(args[0].int());

    if !flags.soft_clear() {
        // With a soft clear, we want to delete public data while keeping internal data around. When
        // connect() is called with compatible JWTs yielding a large overlap of buckets, this can
        // speed up the next sync.
        local_db.exec_safe("DELETE FROM ps_oplog; DELETE FROM ps_buckets")?;
    } else {
        local_db.exec_safe("UPDATE ps_buckets SET last_applied_op = 0")?;
        local_db.exec_safe("DELETE FROM ps_buckets WHERE name = '$local'")?;
    }

    // language=SQLite
    local_db.exec_safe(
        "\
DELETE FROM ps_crud;
DELETE FROM ps_untyped;
DELETE FROM ps_updated_rows;
DELETE FROM ps_kv WHERE key != 'client_id';
DELETE FROM ps_sync_state;
DELETE FROM ps_stream_subscriptions;
",
    )?;

    let table_glob = if flags.clear_local() {
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

    if let Some(schema) = state.view_schema() {
        for raw_table in &schema.raw_tables {
            if let Some(stmt) = &raw_table.clear {
                local_db.exec_safe(&stmt).map_err(|e| {
                    PowerSyncError::from_sqlite(
                        local_db,
                        e,
                        format!("Clearing raw table {}", raw_table.name),
                    )
                })?;
            }
        }
    }

    Ok(String::from(""))
}

#[derive(Clone, Copy)]
struct PowerSyncClearFlags(i32);

impl PowerSyncClearFlags {
    const MASK_CLEAR_LOCAL: i32 = 0x01;
    const MASK_SOFT_CLEAR: i32 = 0x02;

    fn clear_local(self) -> bool {
        self.0 & Self::MASK_CLEAR_LOCAL != 0
    }

    fn soft_clear(self) -> bool {
        self.0 & Self::MASK_SOFT_CLEAR != 0
    }
}

create_auto_tx_function!(powersync_clear_tx, powersync_clear_impl);
create_sqlite_text_fn!(powersync_clear, powersync_clear_tx, "powersync_clear");

pub fn register(db: *mut sqlite::sqlite3, state: Rc<DatabaseState>) -> Result<(), ResultCode> {
    // This entire module is just making it easier to edit sqlite_master using queries.

    // Internal function, used exclusively in existing migrations.
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

    // Initialize the extension internal tables, and start a migration.
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

    db.create_function_v2(
        "powersync_clear",
        1,
        sqlite::UTF8,
        Some(Rc::into_raw(state) as *mut c_void),
        Some(powersync_clear),
        None,
        None,
        Some(DatabaseState::destroy_rc),
    )?;

    Ok(())
}
