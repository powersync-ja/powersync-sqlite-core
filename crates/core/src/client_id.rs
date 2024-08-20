extern crate alloc;

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::ffi::c_int;
use core::slice;

use serde::{Deserialize, Serialize};
use serde_json as json;
use sqlite::ResultCode;
use sqlite_nostd as sqlite;
use sqlite_nostd::{Connection, Context, Value};

use crate::create_sqlite_text_fn;
use crate::error::SQLiteError;
use crate::sync_types::Checkpoint;

fn powersync_client_id_impl(
    ctx: *mut sqlite::context,
    args: &[*mut sqlite::value],
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
