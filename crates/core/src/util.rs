extern crate alloc;

use alloc::format;
use alloc::string::String;

use serde::{Deserialize};
use serde_json as json;

use sqlite::{Connection, ResultCode};
use sqlite_nostd as sqlite;
use sqlite_nostd::ManagedStmt;

use crate::error::SQLiteError;

pub fn quote_string(s: &str) -> String {
    format!("'{:}'", s.replace("'", "''"))
}

pub fn quote_json_path(s: &str) -> String {
    quote_string(&format!("$.{:}", s))
}

pub fn quote_identifier(name: &str) -> String {
    format!("\"{:}\"", name.replace("\"", "\"\""))
}

pub fn quote_internal_name(name: &str, local_only: bool) -> String {
    if local_only {
        quote_identifier_prefixed("ps_data_local__", name)
    } else {
        quote_identifier_prefixed("ps_data__", name)
    }
}

pub fn internal_table_name(name: &str) -> String {
    return format!("ps_data__{}", name);
}

pub fn quote_identifier_prefixed(prefix: &str, name: &str) -> String {
    return format!("\"{:}{:}\"", prefix, name.replace("\"", "\"\""));
}

pub fn extract_table_info(db: *mut sqlite::sqlite3, data: &str) -> Result<ManagedStmt, SQLiteError> {
    // language=SQLite
    let statement = db.prepare_v2("SELECT
        json_extract(?1, '$.name') as name,
        ifnull(json_extract(?1, '$.view_name'), json_extract(?1, '$.name')) as view_name,
        json_extract(?1, '$.local_only') as local_only,
        json_extract(?1, '$.insert_only') as insert_only")?;
    statement.bind_text(1, data, sqlite::Destructor::STATIC)?;

    let step_result = statement.step()?;
    if step_result != ResultCode::ROW {
        return Err(SQLiteError::from(ResultCode::SCHEMA));
    }
    Ok(statement)
}


pub fn deserialize_string_to_i64<'de, D>(deserializer: D) -> Result<i64, D::Error>
    where
        D: serde::Deserializer<'de>,
{
    let value = json::Value::deserialize(deserializer)?;

    match value {
        json::Value::String(s) => s.parse::<i64>().map_err(serde::de::Error::custom),
        _ => Err(serde::de::Error::custom("Expected a string.")),
    }
}

pub fn deserialize_optional_string_to_i64<'de, D>(deserializer: D) -> Result<Option<i64>, D::Error>
    where
        D: serde::Deserializer<'de>,
{
    let value = json::Value::deserialize(deserializer)?;

    match value {
        json::Value::Null => Ok(None),
        json::Value::String(s) => s.parse::<i64>()
            .map(Some)
            .map_err(serde::de::Error::custom),
        _ => Err(serde::de::Error::custom("Expected a string or null.")),
    }
}


pub const MAX_OP_ID: &str = "9223372036854775807";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_identifier_test() {
        assert_eq!(quote_identifier("test"), "\"test\"");
        assert_eq!(quote_identifier("\"quote\""), "\"\"\"quote\"\"\"");
        assert_eq!(quote_identifier("other characters."), "\"other characters.\"");
    }

    #[test]
    fn quote_string_test() {
        assert_eq!(quote_string("test"), "'test'");
        assert_eq!(quote_string("\"quote\""), "'\"quote\"'");
        assert_eq!(quote_string("'quote'"), "'''quote'''");
    }
}
