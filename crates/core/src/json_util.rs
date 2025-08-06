extern crate alloc;

use alloc::string::{String, ToString};
use core::ffi::c_int;

use crate::constants::SUBTYPE_JSON;
use crate::create_sqlite_text_fn;
use crate::error::PowerSyncError;
use sqlite::ResultCode;
use sqlite_nostd as sqlite;
use sqlite_nostd::bindings::{SQLITE_RESULT_SUBTYPE, SQLITE_SUBTYPE};
use sqlite_nostd::{Connection, Context, Value};

extern "C" fn powersync_strip_subtype(
    ctx: *mut sqlite::context,
    argc: c_int,
    argv: *mut *mut sqlite::value,
) {
    if argc != 1 {
        return;
    }

    let arg = unsafe { *argv };
    ctx.result_value(arg);
    ctx.result_subtype(0);
}

/// Given any number of JSON TEXT arguments, merge them into a single JSON object.
///
/// This assumes each argument is a valid JSON object, with no duplicate keys.
/// No JSON parsing or validation is performed - this performs simple string concatenation.
fn powersync_json_merge_impl(
    ctx: *mut sqlite::context,
    args: &[*mut sqlite::value],
) -> Result<String, PowerSyncError> {
    if args.is_empty() {
        return Ok("{}".to_string());
    }
    let mut result = String::from("{");
    for arg in args {
        let chunk = arg.text();
        if chunk.is_empty() || !chunk.starts_with('{') || !chunk.ends_with('}') {
            return Err(PowerSyncError::argument_error("Expected json object"));
        }

        // Strip outer braces
        let inner = &chunk[1..(chunk.len() - 1)];

        // If this is not the first chunk, insert a comma
        if result.len() > 1 {
            result.push(',');
        }

        // Append the inner content
        result.push_str(inner);
    }

    // Close the outer brace
    result.push('}');
    ctx.result_subtype(SUBTYPE_JSON);
    Ok(result)
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
        sqlite::UTF8 | sqlite::DETERMINISTIC | SQLITE_RESULT_SUBTYPE,
        None,
        Some(powersync_json_merge),
        None,
        None,
        None,
    )?;

    db.create_function_v2(
        "powersync_strip_subtype",
        -1,
        sqlite::UTF8 | sqlite::DETERMINISTIC | SQLITE_SUBTYPE | SQLITE_RESULT_SUBTYPE,
        None,
        Some(powersync_strip_subtype),
        None,
        None,
        None,
    )?;

    Ok(())
}
