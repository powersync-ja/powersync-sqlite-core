use alloc::{
    collections::btree_map::BTreeMap,
    string::{String, ToString},
    vec::Vec,
};
use sqlite_nostd::{self as sqlite, Connection, ManagedStmt, ResultCode};
use streaming_iterator::StreamingIterator;

use crate::{error::SQLiteError, operations::delete_bucket};

use super::{bucket_priority::BucketPriority, interface::BucketRequest};

/// An adapter for storing sync state.
///
/// This is used to encapsulate some SQL queries used for the sync implementation, making the code
/// in `streaming_sync.rs` easier to read.
pub struct StorageAdapter {
    db: *mut sqlite::sqlite3,
    progress_stmt: ManagedStmt,
}

impl StorageAdapter {
    pub fn new(db: *mut sqlite::sqlite3) -> Result<Self, ResultCode> {
        let progress =
            db.prepare_v2("SELECT name, count_at_last, count_since_last FROM ps_buckets")?;

        Ok(Self {
            db,
            progress_stmt: progress,
        })
    }

    pub fn collect_local_bucket_state(
        &self,
    ) -> Result<
        (
            Vec<BucketRequest>,
            BTreeMap<String, Option<BucketDescription>>,
        ),
        SQLiteError,
    > {
        // language=SQLite
        let statement = self.db.prepare_v2(
            "SELECT name, last_op FROM ps_buckets WHERE pending_delete = 0 AND name != '$local'",
        )?;

        let mut requests = Vec::<BucketRequest>::new();
        let mut local_state = BTreeMap::<String, Option<BucketDescription>>::new();

        while statement.step()? == ResultCode::ROW {
            let bucket_name = statement.column_text(0)?.to_string();
            let last_op = statement.column_int64(1);

            requests.push(BucketRequest {
                name: bucket_name.clone(),
                after: last_op.to_string(),
            });
            local_state.insert(bucket_name, None);
        }

        Ok((requests, local_state))
    }

    pub fn delete_buckets<'a>(
        &self,
        buckets: impl IntoIterator<Item = &'a str>,
    ) -> Result<(), SQLiteError> {
        for bucket in buckets {
            // TODO: This is a neat opportunity to create the statements here and cache them
            delete_bucket(self.db, bucket)?;
        }

        Ok(())
    }

    pub fn local_progress(
        &self,
    ) -> Result<
        impl StreamingIterator<Item = Result<PersistedBucketProgress, ResultCode>>,
        ResultCode,
    > {
        self.progress_stmt.reset()?;

        fn step(stmt: &ManagedStmt) -> Result<Option<PersistedBucketProgress>, ResultCode> {
            if stmt.step()? == ResultCode::ROW {
                let bucket = stmt.column_text(0)?;
                let count_at_last = stmt.column_int64(1);
                let count_since_last = stmt.column_int64(1);

                return Ok(Some(PersistedBucketProgress {
                    bucket,
                    count_at_last,
                    count_since_last,
                }));
            }

            Ok(None)
        }

        Ok(streaming_iterator::from_fn(|| {
            match step(&self.progress_stmt) {
                Err(e) => Some(Err(e)),
                Ok(Some(other)) => Some(Ok(other)),
                Ok(None) => None,
            }
        }))
    }
}

/// Information about the amount of operations a bucket had at the last checkpoint and how many
/// operations have been inserted in the meantime.
pub struct PersistedBucketProgress<'a> {
    pub bucket: &'a str,
    pub count_at_last: i64,
    pub count_since_last: i64,
}

pub struct BucketDescription {
    pub priority: BucketPriority,
    pub name: String,
}
