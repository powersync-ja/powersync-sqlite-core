extern crate alloc;

use alloc::format;
use alloc::string::{String, ToString};
use core::ffi::c_int;

use sqlite::ResultCode;
use sqlite_nostd as sqlite;
use sqlite_nostd::{Connection, Context, Value};

use serde_json as json;

use crate::create_sqlite_text_fn;
use crate::error::SQLiteError;

fn powersync_diff_impl(
    _ctx: *mut sqlite::context,
    args: &[*mut sqlite::value],
) -> Result<String, SQLiteError> {
    let data_old = args[0].text();
    let data_new = args[1].text();
    let ignore_removed = args.get(2).map_or(false, |v| v.int() != 0);

    diff_objects_with_options(data_old, data_new, ignore_removed)
}

/// Returns a JSON object containing entries from [data_new] that are not present in [data_old].
///
/// When [ignore_removed_columns] is set, columns that are present in [data_old] but not in
/// [data_new] will not be present in the returned object. Otherwise, they will be set to `null`.
fn diff_objects_with_options(
    data_old: &str,
    data_new: &str,
    ignore_removed_columns: bool,
) -> Result<String, SQLiteError> {
    let v_new: json::Value = json::from_str(data_new)?;
    let v_old: json::Value = json::from_str(data_old)?;

    if let (json::Value::Object(mut left), json::Value::Object(mut right)) = (v_new, v_old) {
        // Remove all null values
        right.retain(|_, v| !v.is_null());
        left.retain(|_, v| !v.is_null());

        if right.len() == 0 {
            // Simple case
            return Ok(json::Value::Object(left).to_string());
        }

        // Add missing nulls to left
        if !ignore_removed_columns {
            for key in right.keys() {
                if !left.contains_key(key) {
                    left.insert(key.clone(), json::Value::Null);
                }
            }
        }

        left.retain(|key, value| {
            let r = right.get(key);
            if let Some(r) = r {
                // Check if value is different
                value != r
            } else {
                // Value not present in right
                true
            }
        });

        Ok(json::Value::Object(left).to_string())
    } else {
        Err(SQLiteError::from(ResultCode::MISMATCH))
    }
}

create_sqlite_text_fn!(powersync_diff, powersync_diff_impl, "powersync_diff");

pub fn register(db: *mut sqlite::sqlite3) -> Result<(), ResultCode> {
    db.create_function_v2(
        "powersync_diff",
        2,
        sqlite::UTF8 | sqlite::DETERMINISTIC,
        None,
        Some(powersync_diff),
        None,
        None,
        None,
    )?;

    db.create_function_v2(
        "powersync_diff",
        3,
        sqlite::UTF8 | sqlite::DETERMINISTIC,
        None,
        Some(powersync_diff),
        None,
        None,
        None,
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn diff_objects(data_old: &str, data_new: &str) -> Result<String, SQLiteError> {
        diff_objects_with_options(data_old, data_new, false)
    }

    #[test]
    fn basic_diff_test() {
        assert_eq!(diff_objects("{}", "{}").unwrap(), "{}");
        assert_eq!(diff_objects(r#"{"a": null}"#, "{}").unwrap(), "{}");
        assert_eq!(diff_objects(r#"{}"#, r#"{"a": null}"#).unwrap(), "{}");
        assert_eq!(
            diff_objects(r#"{"b": 1}"#, r#"{"a": null, "b": 1}"#).unwrap(),
            "{}"
        );
        assert_eq!(
            diff_objects(r#"{"b": 1}"#, r#"{"a": null, "b": 2}"#).unwrap(),
            r#"{"b":2}"#
        );
        assert_eq!(
            diff_objects(r#"{"a": 0, "b": 1}"#, r#"{"a": null, "b": 2}"#).unwrap(),
            r#"{"a":null,"b":2}"#
        );
        assert_eq!(
            diff_objects(r#"{"a": 1}"#, r#"{"a": null}"#).unwrap(),
            r#"{"a":null}"#
        );
        assert_eq!(
            diff_objects(r#"{"a": 1}"#, r#"{}"#).unwrap(),
            r#"{"a":null}"#
        );
        assert_eq!(
            diff_objects(r#"{"a": 1}"#, r#"{"a": 2}"#).unwrap(),
            r#"{"a":2}"#
        );
        assert_eq!(
            diff_objects(r#"{"a": 1}"#, r#"{"a": "1"}"#).unwrap(),
            r#"{"a":"1"}"#
        );
        assert_eq!(
            diff_objects(r#"{"a": 1}"#, r#"{"a": 1.0}"#).unwrap(),
            r#"{"a":1.0}"#
        );
        assert_eq!(
            diff_objects(r#"{"a": 1.00}"#, r#"{"a": 1.0}"#).unwrap(),
            r#"{}"#
        );
        assert_eq!(
            diff_objects(r#"{}"#, r#"{"a": 1.0}"#).unwrap(),
            r#"{"a":1.0}"#
        );
        assert_eq!(
            diff_objects(r#"{}"#, r#"{"a": [1,2,3]}"#).unwrap(),
            r#"{"a":[1,2,3]}"#
        );
        assert_eq!(
            diff_objects(r#"{"a": 1}"#, r#"{"a": [1,2,3]}"#).unwrap(),
            r#"{"a":[1,2,3]}"#
        );
    }
}
