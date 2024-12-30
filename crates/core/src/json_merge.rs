extern crate alloc;

use alloc::format;
use alloc::string::{String, ToString};
use core::ffi::c_int;
use core::slice;

use sqlite::ResultCode;
use sqlite_nostd as sqlite;
use sqlite_nostd::{Connection, Context, Value};

use serde_json as json;

use crate::create_sqlite_text_fn;
use crate::error::SQLiteError;

/// Given any number of JSON TEXT arguments, merge them into a single JSON object.
///
/// TODO: If we know these are all valid JSON objects, we could perhaps do string concatenation instead.
fn powersync_json_merge_impl(
    _ctx: *mut sqlite::context,
    args: &[*mut sqlite::value],
) -> Result<String, SQLiteError> {
    let mut v_result = json::Value::Object(json::Map::new());
    for arg in args {
        let v: json::Value = json::from_str(arg.text())?;
        if let json::Value::Object(map) = v {
            for (key, value) in map {
                v_result[key] = value;
            }
        } else {
            return Err(SQLiteError::from(ResultCode::MISMATCH));
        }
    }
    return Ok(v_result.to_string());
}

create_sqlite_text_fn!(
    powersync_json_merge,
    powersync_json_merge_impl,
    "powersync_json_merge"
);

pub fn register(db: *mut sqlite::sqlite3) -> Result<(), ResultCode> {
    db.create_function_v2(
        "powersync_json_merge",
        -1,
        sqlite::UTF8 | sqlite::DETERMINISTIC,
        None,
        Some(powersync_json_merge),
        None,
        None,
        None,
    )?;

    Ok(())
}
