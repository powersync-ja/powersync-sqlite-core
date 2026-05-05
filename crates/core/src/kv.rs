extern crate alloc;

use alloc::string::{String, ToString};
use core::ffi::c_int;

use powersync_sqlite_nostd as sqlite;
use powersync_sqlite_nostd::{Connection, Context};
use sqlite::ResultCode;

use crate::create_sqlite_text_fn;
use crate::error::PowerSyncError;

fn powersync_client_id_impl(
    ctx: *mut sqlite::context,
    _args: &[*mut sqlite::value],
) -> Result<String, PowerSyncError> {
    let db = ctx.db_handle();

    client_id(db)
}

pub fn client_id(db: *mut sqlite::sqlite3) -> Result<String, PowerSyncError> {
    // language=SQLite
    let statement = db.prepare_v2("select value from ps_kv where key = 'client_id'")?;

    if statement.step()? == ResultCode::ROW {
        let client_id = statement.column_text(0)?;
        Ok(client_id.to_string())
    } else {
        Err(PowerSyncError::missing_client_id())
    }
}

create_sqlite_text_fn!(
    powersync_client_id,
    powersync_client_id_impl,
    "powersync_client_id"
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

    Ok(())
}
