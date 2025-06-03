use core::{assert_matches::debug_assert_matches, fmt::Display};

use alloc::{string::ToString, vec::Vec};
use serde::Serialize;
use sqlite_nostd::{self as sqlite, Connection, ManagedStmt, ResultCode};
use streaming_iterator::StreamingIterator;

use crate::{
    error::SQLiteError,
    ext::SafeManagedStmt,
    operations::delete_bucket,
    sync::checkpoint::{validate_checkpoint, ChecksumMismatch},
    sync_local::{PartialSyncOperation, SyncOperation},
};

use super::{
    bucket_priority::BucketPriority, interface::BucketRequest, streaming_sync::OwnedCheckpoint,
    sync_status::Timestamp,
};

/// An adapter for storing sync state.
///
/// This is used to encapsulate some SQL queries used for the sync implementation, making the code
/// in `streaming_sync.rs` easier to read. It also allows caching some prepared statements that are
/// used frequently as an optimization, but we're not taking advantage of that yet.
pub struct StorageAdapter {
    pub db: *mut sqlite::sqlite3,
    progress_stmt: ManagedStmt,
    time_stmt: ManagedStmt,
}

impl StorageAdapter {
    pub fn new(db: *mut sqlite::sqlite3) -> Result<Self, ResultCode> {
        // language=SQLite
        let progress =
            db.prepare_v2("SELECT name, count_at_last, count_since_last FROM ps_buckets")?;

        // language=SQLite
        let time = db.prepare_v2("SELECT unixepoch()")?;

        Ok(Self {
            db,
            progress_stmt: progress,
            time_stmt: time,
        })
    }

    pub fn collect_bucket_requests(&self) -> Result<Vec<BucketRequest>, SQLiteError> {
        // language=SQLite
        let statement = self.db.prepare_v2(
            "SELECT name, last_op FROM ps_buckets WHERE pending_delete = 0 AND name != '$local'",
        )?;

        let mut requests = Vec::<BucketRequest>::new();

        while statement.step()? == ResultCode::ROW {
            let bucket_name = statement.column_text(0)?.to_string();
            let last_op = statement.column_int64(1);

            requests.push(BucketRequest {
                name: bucket_name.clone(),
                after: last_op.to_string(),
            });
        }

        Ok(requests)
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
                let count_since_last = stmt.column_int64(2);

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

    pub fn reset_progress(&self) -> Result<(), ResultCode> {
        self.db
            .exec_safe("UPDATE ps_buckets SET count_since_last = 0, count_at_last = 0;")?;
        Ok(())
    }

    pub fn lookup_bucket(&self, bucket: &str) -> Result<BucketInfo, ResultCode> {
        // We do an ON CONFLICT UPDATE simply so that the RETURNING bit works for existing rows.
        // We can consider splitting this into separate SELECT and INSERT statements.
        // language=SQLite
        let bucket_statement = self.db.prepare_v2(
            "INSERT INTO ps_buckets(name)
                            VALUES(?)
                        ON CONFLICT DO UPDATE
                            SET last_applied_op = last_applied_op
                        RETURNING id, last_applied_op",
        )?;
        bucket_statement.bind_text(1, bucket, sqlite::Destructor::STATIC)?;
        let res = bucket_statement.step()?;
        debug_assert_matches!(res, ResultCode::ROW);

        let bucket_id = bucket_statement.column_int64(0);
        let last_applied_op = bucket_statement.column_int64(1);

        return Ok(BucketInfo {
            id: bucket_id,
            last_applied_op,
        });
    }

    pub fn sync_local(
        &self,
        checkpoint: &OwnedCheckpoint,
        priority: Option<BucketPriority>,
    ) -> Result<SyncLocalResult, SQLiteError> {
        let mismatched_checksums =
            validate_checkpoint(checkpoint.buckets.values(), priority, self.db)?;

        if !mismatched_checksums.is_empty() {
            self.delete_buckets(mismatched_checksums.iter().map(|i| i.bucket_name.as_str()))?;

            return Ok(SyncLocalResult::ChecksumFailure(CheckpointResult {
                failed_buckets: mismatched_checksums,
            }));
        }

        let update_bucket = self
            .db
            .prepare_v2("UPDATE ps_buckets SET last_op = ? WHERE name = ?")?;

        for bucket in checkpoint.buckets.values() {
            if bucket.is_in_priority(priority) {
                update_bucket.bind_int64(1, checkpoint.last_op_id)?;
                update_bucket.bind_text(2, &bucket.bucket, sqlite::Destructor::STATIC)?;
                update_bucket.exec()?;
            }
        }

        if let (None, Some(write_checkpoint)) = (&priority, &checkpoint.write_checkpoint) {
            update_bucket.bind_int64(1, *write_checkpoint)?;
            update_bucket.bind_text(2, "$local", sqlite::Destructor::STATIC)?;
            update_bucket.exec()?;
        }

        #[derive(Serialize)]
        struct PartialArgs<'a> {
            priority: BucketPriority,
            buckets: Vec<&'a str>,
        }

        let sync_result = match priority {
            None => SyncOperation::new(self.db, None).apply(),
            Some(priority) => {
                let args = PartialArgs {
                    priority,
                    buckets: checkpoint
                        .buckets
                        .values()
                        .filter_map(|item| {
                            if item.is_in_priority(Some(priority)) {
                                Some(item.bucket.as_str())
                            } else {
                                None
                            }
                        })
                        .collect(),
                };

                // TODO: Avoid this serialization, it's currently used to bind JSON SQL parameters.
                let serialized_args = serde_json::to_string(&args)?;
                SyncOperation::new(
                    self.db,
                    Some(PartialSyncOperation {
                        priority,
                        args: &serialized_args,
                    }),
                )
                .apply()
            }
        }?;

        if sync_result == 1 {
            if priority.is_none() {
                // Reset progress counters. We only do this for a complete sync, as we want a
                // download progress to always cover a complete checkpoint instead of resetting for
                // partial completions.
                let update = self.db.prepare_v2(
                    "UPDATE ps_buckets SET count_since_last = 0, count_at_last = ? WHERE name = ?",
                )?;

                for bucket in checkpoint.buckets.values() {
                    if let Some(count) = bucket.count {
                        update.bind_int64(1, count)?;
                        update.bind_text(2, bucket.bucket.as_str(), sqlite::Destructor::STATIC)?;

                        update.exec()?;
                        update.reset()?;
                    }
                }
            }

            Ok(SyncLocalResult::ChangesApplied)
        } else {
            Ok(SyncLocalResult::PendingLocalChanges)
        }
    }

    pub fn now(&self) -> Result<Timestamp, ResultCode> {
        self.time_stmt.reset()?;
        self.time_stmt.step()?;

        Ok(Timestamp(self.time_stmt.column_int64(0)))
    }
}

pub struct BucketInfo {
    pub id: i64,
    pub last_applied_op: i64,
}

pub struct CheckpointResult {
    failed_buckets: Vec<ChecksumMismatch>,
}

impl CheckpointResult {
    pub fn is_valid(&self) -> bool {
        self.failed_buckets.is_empty()
    }
}

impl Display for CheckpointResult {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        if self.is_valid() {
            write!(f, "Valid checkpoint result")
        } else {
            write!(f, "Checksums didn't match, failed for: ")?;
            for (i, item) in self.failed_buckets.iter().enumerate() {
                if i != 0 {
                    write!(f, ", ")?;
                }

                item.fmt(f)?;
            }

            Ok(())
        }
    }
}

impl Display for ChecksumMismatch {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let actual = self.actual_add_checksum + self.actual_op_checksum;
        write!(
            f,
            "{} (expected {}, got {} = {} (op) + {} (add))",
            self.bucket_name,
            self.expected_checksum,
            actual,
            self.actual_op_checksum,
            self.actual_add_checksum
        )
    }
}

pub enum SyncLocalResult {
    /// Changes could not be applied due to a checksum mismatch.
    ChecksumFailure(CheckpointResult),
    /// Changes could not be applied because they would break consistency - we need to wait for
    /// pending local CRUD data to be uploaded and acknowledged in a write checkpoint.
    PendingLocalChanges,
    /// The checkpoint has been applied and changes have been published.
    ChangesApplied,
}

/// Information about the amount of operations a bucket had at the last checkpoint and how many
/// operations have been inserted in the meantime.
pub struct PersistedBucketProgress<'a> {
    pub bucket: &'a str,
    pub count_at_last: i64,
    pub count_since_last: i64,
}
