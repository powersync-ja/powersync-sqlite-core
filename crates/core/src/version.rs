extern crate alloc;

use alloc::format;
use alloc::string::String;
use core::ffi::c_int;

use sqlite::ResultCode;
use sqlite_nostd as sqlite;
use sqlite_nostd::{Connection, Context};

use crate::constants::{short_git_hash, CORE_PKG_VERSION};
use crate::create_sqlite_text_fn;
use crate::error::PowerSyncError;

fn powersync_rs_version_impl(
    _ctx: *mut sqlite::context,
    _args: &[*mut sqlite::value],
) -> Result<String, ResultCode> {
    let version = format!("{}/{}", CORE_PKG_VERSION, short_git_hash());
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
