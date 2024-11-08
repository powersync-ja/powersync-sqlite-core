use alloc::format;

use crate::error::{PSResult, SQLiteError};
use sqlite_nostd as sqlite;
use sqlite_nostd::{Connection, ResultCode};

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
pub fn apply_v035_fix(db: *mut sqlite::sqlite3) -> Result<i64, SQLiteError> {
    // language=SQLite
    let statement = db
      .prepare_v2("SELECT name, powersync_external_table_name(name) FROM sqlite_master WHERE type='table' AND name GLOB 'ps_data__*'")
      .into_db_result(db)?;

    while statement.step()? == ResultCode::ROW {
        let full_name = statement.column_text(0)?;
        let short_name = statement.column_text(1)?;
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
