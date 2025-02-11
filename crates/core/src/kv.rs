extern crate alloc;

use alloc::format;
use alloc::string::{String, ToString};
use core::ffi::c_int;
use core::slice;

use sqlite::ResultCode;
use sqlite_nostd as sqlite;
use sqlite_nostd::{Connection, Context};

use crate::bucket_priority::BucketPriority;
use crate::create_sqlite_optional_text_fn;
use crate::create_sqlite_text_fn;
use crate::error::SQLiteError;

fn powersync_client_id_impl(
    ctx: *mut sqlite::context,
    _args: &[*mut sqlite::value],
) -> Result<String, SQLiteError> {
    let db = ctx.db_handle();

    // language=SQLite
    let statement = db.prepare_v2("select value from ps_kv where key = 'client_id'")?;

    if statement.step()? == ResultCode::ROW {
        let client_id = statement.column_text(0)?;
        return Ok(client_id.to_string());
    } else {
        return Err(SQLiteError(
            ResultCode::ABORT,
            Some(format!("No client_id found in ps_kv")),
        ));
    }
}

create_sqlite_text_fn!(
    powersync_client_id,
    powersync_client_id_impl,
    "powersync_client_id"
);

fn powersync_last_synced_at_impl(
    ctx: *mut sqlite::context,
    _args: &[*mut sqlite::value],
) -> Result<Option<String>, SQLiteError> {
    let db = ctx.db_handle();

    // language=SQLite
    let statement = db.prepare_v2("select last_synced_at from ps_sync_state where priority = ?")?;
    statement.bind_int(1, BucketPriority::SENTINEL.into())?;

    if statement.step()? == ResultCode::ROW {
        let client_id = statement.column_text(0)?;
        Ok(Some(client_id.to_string()))
    } else {
        Ok(None)
    }
}

create_sqlite_optional_text_fn!(
    powersync_last_synced_at,
    powersync_last_synced_at_impl,
    "powersync_last_synced_at"
);

pub fn register(db: *mut sqlite::sqlite3) -> Result<(), ResultCode> {
    db.create_function_v2(
        "powersync_client_id",
        0,
        sqlite::UTF8 | sqlite::DETERMINISTIC,
        None,
        Some(powersync_client_id),
        None,
        None,
        None,
    )?;
    db.create_function_v2(
        "powersync_last_synced_at",
        0,
        sqlite::UTF8 | sqlite::DETERMINISTIC,
        None,
        Some(powersync_last_synced_at),
        None,
        None,
        None,
    )?;

    Ok(())
}
