use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use num_traits::Zero;
use serde::Deserialize;

use crate::error::{PSResult, SQLiteError};
use crate::sync::line::DataLine;
use crate::sync::operations::insert_bucket_operations;
use crate::sync::Checksum;
use sqlite_nostd as sqlite;
use sqlite_nostd::{Connection, ResultCode};

use crate::ext::SafeManagedStmt;

// Run inside a transaction
pub fn insert_operation(db: *mut sqlite::sqlite3, data: &str) -> Result<(), SQLiteError> {
    #[derive(Deserialize)]
    struct BucketBatch<'a> {
        #[serde(borrow)]
        buckets: Vec<DataLine<'a>>,
    }

    let batch: BucketBatch = serde_json::from_str(data)?;
    for line in &batch.buckets {
        insert_bucket_operations(db, line)?;
    }

    Ok(())
}

pub fn clear_remove_ops(_db: *mut sqlite::sqlite3, _data: &str) -> Result<(), SQLiteError> {
    // No-op

    Ok(())
}

pub fn delete_pending_buckets(_db: *mut sqlite::sqlite3, _data: &str) -> Result<(), SQLiteError> {
    // No-op

    Ok(())
}

pub fn delete_bucket(db: *mut sqlite::sqlite3, name: &str) -> Result<(), SQLiteError> {
    // language=SQLite
    let statement = db.prepare_v2("DELETE FROM ps_buckets WHERE name = ?1 RETURNING id")?;
    statement.bind_text(1, name, sqlite::Destructor::STATIC)?;

    if statement.step()? == ResultCode::ROW {
        let bucket_id = statement.column_int64(0);

        // language=SQLite
        let updated_statement = db.prepare_v2(
            "\
INSERT OR IGNORE INTO ps_updated_rows(row_type, row_id)
SELECT row_type, row_id
FROM ps_oplog
WHERE bucket = ?1",
        )?;
        updated_statement.bind_int64(1, bucket_id)?;
        updated_statement.exec()?;

        // language=SQLite
        let delete_statement = db.prepare_v2("DELETE FROM ps_oplog WHERE bucket=?1")?;
        delete_statement.bind_int64(1, bucket_id)?;
        delete_statement.exec()?;
    }

    Ok(())
}
