extern crate alloc;

use alloc::format;
use alloc::string::String;
use alloc::string::ToString;
use core::ffi::c_int;
use core::slice;

use sqlite::ResultCode;
use sqlite_nostd as sqlite;
use sqlite_nostd::{Connection, Context};
use uuid::Uuid;

use crate::create_sqlite_text_fn;
use crate::error::SQLiteError;

fn uuid_v4_impl(
    _ctx: *mut sqlite::context,
    _args: &[*mut sqlite::value],
) -> Result<String, ResultCode> {
    let id = Uuid::new_v4();
    Ok(id.hyphenated().to_string())
}

create_sqlite_text_fn!(uuid_v4, uuid_v4_impl, "gen_random_uuid");

pub fn register(db: *mut sqlite::sqlite3) -> Result<(), ResultCode> {
    db.create_function_v2(
        "gen_random_uuid",
        0,
        sqlite::UTF8,
        None,
        Some(uuid_v4),
        None,
        None,
        None,
    )?;

    db.create_function_v2(
        "uuid",
        0,
        sqlite::UTF8,
        None,
        Some(uuid_v4),
        None,
        None,
        None,
    )?;

    Ok(())
}
