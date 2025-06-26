extern crate alloc;

use alloc::string::String;
use core::ffi::c_int;

use sqlite::ResultCode;
use sqlite_nostd::{self as sqlite, Value};
use sqlite_nostd::{Connection, Context};

use crate::create_sqlite_text_fn;
use crate::error::SQLiteError;
use zstd_safe::DCtx;

fn powersync_zstd_impl(
    _ctx: *mut sqlite::context,
    args: &[*mut sqlite::value],
) -> Result<String, ResultCode> {
    let arg = args.get(0).ok_or(ResultCode::MISMATCH)?.blob();
    let dict = args.get(1).ok_or(ResultCode::MISMATCH)?.blob();
    // TODO: Use a form of streaming decompression to avoid pre-allocating a large buffer.
    let mut dest = alloc::vec![0u8; 1024 * 20];
    let mut ctx = DCtx::create();
    let size = ctx
        .decompress_using_dict(&mut dest[..], arg, dict)
        .map_err(|_| ResultCode::CORRUPT)?;
    dest.truncate(size);
    let text = String::from_utf8(dest).map_err(|_| ResultCode::MISUSE)?;
    Ok(text)
}

create_sqlite_text_fn!(powersync_zstd, powersync_zstd_impl, "zstd_decompress_text");

pub fn register(db: *mut sqlite::sqlite3) -> Result<(), ResultCode> {
    db.create_function_v2(
        "zstd_decompress_text",
        2,
        sqlite::UTF8,
        None,
        Some(powersync_zstd),
        None,
        None,
        None,
    )?;

    Ok(())
}
