use core::{assert_matches::debug_assert_matches, fmt::Display};

use alloc::{string::ToString, vec::Vec};
use serde::Serialize;
use serde_json::value::RawValue;
use sqlite_nostd::{self as sqlite, Connection, ManagedStmt, ResultCode};

use crate::{
    error::{PSResult, PowerSyncError},
    ext::SafeManagedStmt,
    kv::client_id,
    operations::delete_bucket,
    schema::Schema,
    state::DatabaseState,
    sync::{
        checkpoint::{ChecksumMismatch, validate_checkpoint},
        interface::{RequestedStreamSubscription, StreamSubscriptionRequest},
        streaming_sync::OwnedStreamDefinition,
        subscriptions::LocallyTrackedSubscription,
        sync_status::SyncPriorityStatus,
    },
    sync_local::{PartialSyncOperation, SyncOperation},
    util::{JsonString, column_nullable},
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
    pub progress_stmt: ManagedStmt,
    time_stmt: ManagedStmt,
    delete_subscription: ManagedStmt,
    update_subscription: ManagedStmt,
}

impl StorageAdapter {
    pub fn new(db: *mut sqlite::sqlite3) -> Result<Self, PowerSyncError> {
        // language=SQLite
        let progress = db
            .prepare_v2("SELECT name, count_at_last, count_since_last FROM ps_buckets")
            .into_db_result(db)?;

        // language=SQLite
        let time = db.prepare_v2("SELECT unixepoch()")?;

        // language=SQLite
        let delete_subscription =
            db.prepare_v2("DELETE FROM ps_stream_subscriptions WHERE id = ?")?;

        // language=SQLite
        let update_subscription =
            db.prepare_v2("UPDATE ps_stream_subscriptions SET active = ?2, is_default = ?3, ttl = ?, expires_at = ?, last_synced_at = ? WHERE id = ?1")?;

        Ok(Self {
            db,
            progress_stmt: progress,
            time_stmt: time,
            delete_subscription,
            update_subscription,
        })
    }

    pub fn collect_bucket_requests(&self) -> Result<Vec<BucketRequest>, PowerSyncError> {
        // language=SQLite
        let statement = self.db.prepare_v2(
            "SELECT name, last_op FROM ps_buckets WHERE pending_delete = 0 AND name != '$local'",
        ).into_db_result(self.db)?;

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

    pub fn collect_sync_state(&self) -> Result<Vec<SyncPriorityStatus>, PowerSyncError> {
        // language=SQLite
        let statement = self
            .db
            .prepare_v2(
                "SELECT priority, unixepoch(last_synced_at) FROM ps_sync_state ORDER BY priority",
            )
            .into_db_result(self.db)?;

        let mut items = Vec::<SyncPriorityStatus>::new();
        while statement.step()? == ResultCode::ROW {
            let priority = BucketPriority {
                number: statement.column_int(0),
            };
            let timestamp = statement.column_int64(1);

            items.push(SyncPriorityStatus {
                priority,
                last_synced_at: Some(Timestamp(timestamp)),
                has_synced: Some(true),
            });
        }

        return Ok(items);
    }

    pub fn delete_buckets<'a>(
        &self,
        buckets: impl IntoIterator<Item = &'a str>,
    ) -> Result<(), ResultCode> {
        for bucket in buckets {
            // TODO: This is a neat opportunity to create the statements here and cache them
            delete_bucket(self.db, bucket)?;
        }

        Ok(())
    }

    pub fn step_progress(&self) -> Result<Option<PersistedBucketProgress>, ResultCode> {
        if self.progress_stmt.step()? == ResultCode::ROW {
            let bucket = self.progress_stmt.column_text(0)?;
            let count_at_last = self.progress_stmt.column_int64(1);
            let count_since_last = self.progress_stmt.column_int64(2);

            Ok(Some(PersistedBucketProgress {
                bucket,
                count_at_last,
                count_since_last,
            }))
        } else {
            // Done
            self.progress_stmt.reset()?;
            Ok(None)
        }
    }

    pub fn reset_progress(&self) -> Result<(), PowerSyncError> {
        self.db
            .exec_safe("UPDATE ps_buckets SET count_since_last = 0, count_at_last = 0;")
            .into_db_result(self.db)?;
        Ok(())
    }

    pub fn lookup_bucket(&self, bucket: &str) -> Result<BucketInfo, PowerSyncError> {
        // We do an ON CONFLICT UPDATE simply so that the RETURNING bit works for existing rows.
        // We can consider splitting this into separate SELECT and INSERT statements.
        // language=SQLite
        let bucket_statement = self
            .db
            .prepare_v2(
                "INSERT INTO ps_buckets(name)
                            VALUES(?)
                        ON CONFLICT DO UPDATE
                            SET last_applied_op = last_applied_op
                        RETURNING id, last_applied_op",
            )
            .into_db_result(self.db)?;
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
        state: &DatabaseState,
        checkpoint: &OwnedCheckpoint,
        priority: Option<BucketPriority>,
        schema: &Schema,
    ) -> Result<SyncLocalResult, PowerSyncError> {
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
            .prepare_v2("UPDATE ps_buckets SET last_op = ? WHERE name = ?")
            .into_db_result(self.db)?;

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
            None => {
                let mut sync = SyncOperation::new(state, self.db, None);
                sync.use_schema(schema);
                sync.apply()
            }
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
                let serialized_args =
                    serde_json::to_string(&args).map_err(PowerSyncError::internal)?;
                let mut sync = SyncOperation::new(
                    state,
                    self.db,
                    Some(PartialSyncOperation {
                        priority,
                        args: &serialized_args,
                    }),
                );
                sync.use_schema(schema);
                sync.apply()
            }
        }?;

        if sync_result == 1 {
            if priority.is_none() {
                // Reset progress counters. We only do this for a complete sync, as we want a
                // download progress to always cover a complete checkpoint instead of resetting for
                // partial completions.
                let update = self.db.prepare_v2(
                    "UPDATE ps_buckets SET count_since_last = 0, count_at_last = ? WHERE name = ?",
                ).into_db_result(self.db)?;

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

    pub fn collect_subscription_requests(
        &self,
        include_defaults: bool,
    ) -> Result<StreamSubscriptionRequest, PowerSyncError> {
        self.delete_outdated_subscriptions()?;

        let mut subscriptions: Vec<RequestedStreamSubscription> = Vec::new();
        let stmt = self
            .db
            .prepare_v2("SELECT * FROM ps_stream_subscriptions WHERE NOT is_default;")?;

        while let ResultCode::ROW = stmt.step()? {
            let subscription = Self::read_stream_subscription(&stmt)?;

            subscriptions.push(RequestedStreamSubscription {
                stream: subscription.stream_name,
                parameters: subscription.local_params,
                override_priority: subscription.local_priority,
                client_id: subscription.id,
            });
        }

        Ok(StreamSubscriptionRequest {
            include_defaults,
            subscriptions,
        })
    }

    pub fn now(&self) -> Result<Timestamp, ResultCode> {
        self.time_stmt.step()?;
        let res = Timestamp(self.time_stmt.column_int64(0));
        self.time_stmt.reset()?;

        Ok(res)
    }

    fn read_stream_subscription(
        stmt: &ManagedStmt,
    ) -> Result<LocallyTrackedSubscription, PowerSyncError> {
        let raw_params = stmt.column_text(5)?;

        Ok(LocallyTrackedSubscription {
            id: stmt.column_int64(0),
            stream_name: stmt.column_text(1)?.to_string(),
            active: stmt.column_int(2) != 0,
            is_default: stmt.column_int(3) != 0,
            local_priority: column_nullable(&stmt, 4, || {
                BucketPriority::try_from(stmt.column_int(4))
            })?,
            local_params: if raw_params == "null" {
                None
            } else {
                Some(JsonString::from_string(stmt.column_text(5)?.to_string())?)
            },
            ttl: column_nullable(&stmt, 6, || Ok(stmt.column_int64(6)))?,
            expires_at: column_nullable(&stmt, 7, || Ok(stmt.column_int64(7)))?,
            last_synced_at: column_nullable(&stmt, 8, || Ok(stmt.column_int64(8)))?,
        })
    }

    fn delete_outdated_subscriptions(&self) -> Result<(), PowerSyncError> {
        self.db
            .exec_safe("DELETE FROM ps_stream_subscriptions WHERE expires_at < unixepoch()")?;
        Ok(())
    }

    pub fn iterate_local_subscriptions<F: FnMut(LocallyTrackedSubscription) -> ()>(
        &self,
        mut action: F,
    ) -> Result<(), PowerSyncError> {
        let stmt = self
            .db
            .prepare_v2("SELECT * FROM ps_stream_subscriptions ORDER BY id ASC")?;

        while stmt.step()? == ResultCode::ROW {
            action(Self::read_stream_subscription(&stmt)?);
        }
        Ok(())
    }

    pub fn create_default_subscription(
        &self,
        stream: &OwnedStreamDefinition,
    ) -> Result<LocallyTrackedSubscription, PowerSyncError> {
        let stmt = self.db.prepare_v2("INSERT INTO ps_stream_subscriptions (stream_name, active, is_default) VALUES (?, TRUE, TRUE) RETURNING *;")?;
        stmt.bind_text(1, &stream.name, sqlite_nostd::Destructor::STATIC)?;

        if stmt.step()? == ResultCode::ROW {
            Self::read_stream_subscription(&stmt)
        } else {
            Err(PowerSyncError::unknown_internal())
        }
    }

    pub fn update_subscription(
        &self,
        subscription: &LocallyTrackedSubscription,
    ) -> Result<(), PowerSyncError> {
        let _ = self.update_subscription.reset();

        self.update_subscription.bind_int64(1, subscription.id)?;
        self.update_subscription
            .bind_int(2, if subscription.active { 1 } else { 0 })?;
        self.update_subscription
            .bind_int(3, if subscription.is_default { 1 } else { 0 })?;
        if let Some(ttl) = subscription.ttl {
            self.update_subscription.bind_int64(4, ttl)?;
        } else {
            self.update_subscription.bind_null(4)?;
        }

        if let Some(expires_at) = subscription.expires_at {
            self.update_subscription.bind_int64(5, expires_at)?;
        } else {
            self.update_subscription.bind_null(5)?;
        }

        if let Some(last_synced_at) = subscription.last_synced_at {
            self.update_subscription.bind_int64(6, last_synced_at)?;
        } else {
            self.update_subscription.bind_null(6)?;
        }

        self.update_subscription.exec()?;
        Ok(())
    }

    pub fn delete_subscription(&self, id: i64) -> Result<(), PowerSyncError> {
        let _ = self.delete_subscription.reset();
        self.delete_subscription.bind_int64(1, id)?;
        self.delete_subscription.exec()?;
        Ok(())
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
