extern crate alloc;

use alloc::format;
use alloc::string::String;
use core::ffi::c_int;

use sqlite::ResultCode;
use sqlite_nostd as sqlite;
use sqlite_nostd::{Connection, Context};

use crate::create_sqlite_text_fn;
use crate::error::SQLiteError;

fn powersync_rs_version_impl(
    _ctx: *mut sqlite::context,
    _args: &[*mut sqlite::value],
) -> Result<String, ResultCode> {
    let cargo_version = env!("CARGO_PKG_VERSION");
    let full_hash = String::from(env!("GIT_HASH"));
    let version = format!("{}/{}", cargo_version, &full_hash[0..8]);
    Ok(version)
}

create_sqlite_text_fn!(
    powersync_rs_version,
    powersync_rs_version_impl,
    "powersync_rs_version"
);

pub fn register(db: *mut sqlite::sqlite3) -> Result<(), ResultCode> {
    db.create_function_v2(
        "powersync_rs_version",
        0,
        sqlite::UTF8,
        None,
        Some(powersync_rs_version),
        None,
        None,
        None,
    )?;

    Ok(())
}
