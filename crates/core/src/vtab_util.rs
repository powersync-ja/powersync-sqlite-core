extern crate alloc;

use alloc::string::ToString;
use core::ffi::{c_char, c_int};

use powersync_sqlite_nostd as sqlite;
use powersync_sqlite_nostd::VTab;
use sqlite::ResultCode;

use crate::error::PowerSyncError;

// For insert-only virtual tables, there are many functions that have to be defined, even if they're
// not intended to be used. We return MISUSE for each.

pub extern "C" fn vtab_no_filter(
    _cursor: *mut sqlite::vtab_cursor,
    _idx_num: c_int,
    _idx_str: *const c_char,
    _argc: c_int,
    _argv: *mut *mut sqlite::value,
) -> c_int {
    ResultCode::MISUSE as c_int
}

pub extern "C" fn vtab_no_next(_cursor: *mut sqlite::vtab_cursor) -> c_int {
    ResultCode::MISUSE as c_int
}

pub extern "C" fn vtab_no_eof(_cursor: *mut sqlite::vtab_cursor) -> c_int {
    ResultCode::MISUSE as c_int
}

pub extern "C" fn vtab_no_column(
    _cursor: *mut sqlite::vtab_cursor,
    _ctx: *mut sqlite::context,
    _col_num: c_int,
) -> c_int {
    ResultCode::MISUSE as c_int
}

pub extern "C" fn vtab_no_rowid(
    _cursor: *mut sqlite::vtab_cursor,
    _row_id: *mut sqlite::int64,
) -> c_int {
    ResultCode::MISUSE as c_int
}

pub extern "C" fn vtab_no_best_index(
    _vtab: *mut sqlite::vtab,
    _index_info: *mut sqlite::index_info,
) -> c_int {
    return ResultCode::MISUSE as c_int;
}

pub extern "C" fn vtab_no_open(
    _vtab: *mut sqlite::vtab,
    _cursor: *mut *mut sqlite::vtab_cursor,
) -> c_int {
    ResultCode::MISUSE as c_int
}

pub extern "C" fn vtab_no_close(_cursor: *mut sqlite::vtab_cursor) -> c_int {
    // If open never allocates a cursor, this should never be called
    ResultCode::MISUSE as c_int
}

pub fn vtab_result<T, E: Into<PowerSyncError>>(
    vtab: *mut sqlite::vtab,
    result: Result<T, E>,
) -> c_int {
    if let Err(error) = result {
        let error = error.into();

        vtab.set_err(&error.to_string());
        error.sqlite_error_code() as c_int
    } else {
        ResultCode::OK as c_int
    }
}
