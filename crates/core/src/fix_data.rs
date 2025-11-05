use core::ffi::c_int;

use alloc::format;
use alloc::string::String;

use crate::create_sqlite_optional_text_fn;
use crate::error::{PSResult, PowerSyncError};
use crate::schema::inspection::ExistingTable;
use powersync_sqlite_nostd::{self as sqlite, ColumnType, Value};
use powersync_sqlite_nostd::{Connection, Context, ResultCode};

use crate::ext::SafeManagedStmt;
use crate::util::quote_identifier;

// Apply a data migration to fix any existing data affected by the issue
// fixed in v0.3.5.
//
// The issue was that the `ps_updated_rows` table was not being populated
// with remove operations in some cases. This causes the rows to be removed
// from ps_oplog, but not from the ps_data__tables, resulting in dangling rows.
//
// The fix here is to find these dangling rows, and add them to ps_updated_rows.
// The next time the sync_local operation is run, these rows will be removed.
pub fn apply_v035_fix(db: *mut sqlite::sqlite3) -> Result<i64, PowerSyncError> {
    // language=SQLite
    let statement = db
        .prepare_v2("SELECT name FROM sqlite_master WHERE type='table' AND name GLOB 'ps_data__*'")
        .into_db_result(db)?;

    while statement.step()? == ResultCode::ROW {
        let full_name = statement.column_text(0)?;
        let Some((short_name, _)) = ExistingTable::external_name(full_name) else {
            continue;
        };

        let quoted = quote_identifier(full_name);

        // language=SQLite
        let statement = db.prepare_v2(&format!(
            "
INSERT OR IGNORE INTO ps_updated_rows(row_type, row_id)
SELECT ?1, id FROM {}
  WHERE NOT EXISTS (
      SELECT 1 FROM ps_oplog
      WHERE row_type = ?1 AND row_id = {}.id
  );",
            quoted, quoted
        ))?;
        statement.bind_text(1, short_name, sqlite::Destructor::STATIC)?;

        statement.exec()?;
    }

    Ok(1)
}

/// Older versions of the JavaScript SDK for PowerSync used to encode the subkey in oplog data
/// entries as JSON.
///
/// It wasn't supposed to do that, since the keys are regular strings already. To make databases
/// created with those SDKs compatible with other SDKs or the sync client implemented in the core
/// extensions, a migration is necessary. Since this migration is only relevant for the JS SDK, it
/// is mostly implemented there. However, the helper function to remove the key encoding is
/// implemented here because user-defined functions are expensive on JavaScript.
fn remove_duplicate_key_encoding(key: &str) -> Option<String> {
    // Acceptable format: <type>/<id>/<subkey>
    // Inacceptable format: <type>/<id>/"<subkey>"
    // This is a bit of a tricky conversion because both type and id can contain slashes and quotes.
    // However, the subkey is either a UUID value or a `<table>/UUID` value - so we know it can't
    // end in a quote unless the improper encoding was used.
    if !key.ends_with('"') {
        return None;
    }

    // Since the subkey is JSON-encoded, find the start quote by going backwards.
    let mut chars = key.char_indices();
    chars.next_back()?; // Skip the quote ending the string

    enum FoundStartingQuote {
        HasQuote { index: usize },
        HasBackslachThenQuote { quote_index: usize },
    }
    let mut state: Option<FoundStartingQuote> = None;
    let found_starting_quote = loop {
        if let Some((i, char)) = chars.next_back() {
            state = match state {
                Some(FoundStartingQuote::HasQuote { index }) => {
                    if char == '\\' {
                        // We've seen a \" pattern, not the start of the string
                        Some(FoundStartingQuote::HasBackslachThenQuote { quote_index: index })
                    } else {
                        break Some(index);
                    }
                }
                Some(FoundStartingQuote::HasBackslachThenQuote { quote_index }) => {
                    if char == '\\' {
                        // \\" pattern, the quote is unescaped
                        break Some(quote_index);
                    } else {
                        None
                    }
                }
                None => {
                    if char == '"' {
                        Some(FoundStartingQuote::HasQuote { index: i })
                    } else {
                        None
                    }
                }
            }
        } else {
            break None;
        }
    }?;

    let before_json = &key[..found_starting_quote];
    let mut result: String = serde_json::from_str(&key[found_starting_quote..]).ok()?;

    result.insert_str(0, before_json);
    Some(result)
}

fn powersync_remove_duplicate_key_encoding_impl(
    _ctx: *mut sqlite::context,
    args: &[*mut sqlite::value],
) -> Result<Option<String>, PowerSyncError> {
    let arg = args.get(0).ok_or(ResultCode::MISUSE)?;

    if arg.value_type() != ColumnType::Text {
        return Err(ResultCode::MISMATCH.into());
    }

    return Ok(remove_duplicate_key_encoding(arg.text()));
}

create_sqlite_optional_text_fn!(
    powersync_remove_duplicate_key_encoding,
    powersync_remove_duplicate_key_encoding_impl,
    "powersync_remove_duplicate_key_encoding"
);

pub fn register(db: *mut sqlite::sqlite3) -> Result<(), ResultCode> {
    db.create_function_v2(
        "powersync_remove_duplicate_key_encoding",
        1,
        sqlite::UTF8 | sqlite::DETERMINISTIC,
        None,
        Some(powersync_remove_duplicate_key_encoding),
        None,
        None,
        None,
    )?;
    Ok(())
}

#[cfg(test)]
mod test {

    use super::remove_duplicate_key_encoding;

    fn assert_unaffected(source: &str) {
        assert!(matches!(remove_duplicate_key_encoding(source), None));
    }

    #[test]
    fn does_not_change_unaffected_keys() {
        assert_unaffected("object_type/object_id/subkey");
        assert_unaffected("object_type/object_id/null");

        // Object type and ID could technically contain quotes and forward slashes
        assert_unaffected(r#""object"/"type"/subkey"#);
        assert_unaffected("object\"/type/object\"/id/subkey");

        // Invalid key, but we shouldn't crash
        assert_unaffected("\"key\"");
    }

    #[test]
    fn removes_quotes() {
        assert_eq!(
            remove_duplicate_key_encoding("foo/bar/\"baz\"").unwrap(),
            "foo/bar/baz",
        );

        assert_eq!(
            remove_duplicate_key_encoding(r#"foo/bar/"nested/subkey""#).unwrap(),
            "foo/bar/nested/subkey"
        );

        assert_eq!(
            remove_duplicate_key_encoding(r#"foo/bar/"escaped\"key""#).unwrap(),
            "foo/bar/escaped\"key"
        );
        assert_eq!(
            remove_duplicate_key_encoding(r#"foo/bar/"escaped\\key""#).unwrap(),
            "foo/bar/escaped\\key"
        );
        assert_eq!(
            remove_duplicate_key_encoding(r#"foo/bar/"/\\"subkey""#).unwrap(),
            "foo/bar/\"/\\\\subkey"
        );
    }
}
