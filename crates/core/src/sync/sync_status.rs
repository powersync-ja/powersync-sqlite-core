use alloc::{boxed::Box, collections::btree_map::BTreeMap, rc::Rc, string::String, vec::Vec};
use core::{
    cell::RefCell,
    cmp::min,
    hash::{BuildHasher, Hash},
};
use rustc_hash::FxBuildHasher;
use serde::Serialize;
use sqlite_nostd::ResultCode;

use crate::{
    sync::{
        checkpoint::OwnedBucketChecksum, storage_adapter::StorageAdapter,
        subscriptions::LocallyTrackedSubscription,
    },
    util::JsonString,
};

use super::{
    bucket_priority::BucketPriority, interface::Instruction, line::DataLine,
    streaming_sync::OwnedCheckpoint,
};

/// Information about a progressing download.
#[derive(Serialize, Hash)]
pub struct DownloadSyncStatus {
    /// Whether the socket to the sync service is currently open and connected.
    ///
    /// This starts being true once we receive the first line, and is set to false as the iteration
    /// ends.
    pub connected: bool,
    /// Whether we've requested the client SDK to connect to the socket while not receiving sync
    /// lines yet.
    pub connecting: bool,
    /// Provides stats over which bucket priorities have already been synced (or when they've last
    /// been changed).
    ///
    /// Always sorted by descending [BucketPriority] in [SyncPriorityStatus] (or, in other words,
    /// increasing priority numbers).
    pub priority_status: Vec<SyncPriorityStatus>,
    /// When a download is active (that is, a `checkpoint` or `checkpoint_diff` line has been
    /// received), information about how far the download has progressed.
    pub downloading: Option<SyncDownloadProgress>,
    pub streams: Vec<ActiveStreamSubscription>,
}

impl DownloadSyncStatus {
    fn debug_assert_priority_status_is_sorted(&self) {
        debug_assert!(
            self.priority_status
                .is_sorted_by(|a, b| a.priority >= b.priority)
        )
    }

    pub fn disconnect(&mut self) {
        self.connected = false;
        self.connecting = false;
        self.downloading = None;
    }

    pub fn start_connecting(&mut self) {
        self.connected = false;
        self.downloading = None;
        self.connecting = true;
        self.debug_assert_priority_status_is_sorted();
    }

    pub fn mark_connected(&mut self) {
        self.connecting = false;
        self.connected = true;
    }

    /// Transitions state after receiving a checkpoint line.
    ///
    /// This sets the [downloading] state to include [progress].
    pub fn start_tracking_checkpoint<'a>(
        &mut self,
        progress: SyncDownloadProgress,
        subscriptions: Vec<ActiveStreamSubscription>,
    ) {
        self.mark_connected();

        self.downloading = Some(progress);
        self.streams = subscriptions;
    }

    /// Increments [SyncDownloadProgress] progress for the given [DataLine].
    pub fn track_line(&mut self, line: &DataLine) {
        if let Some(ref mut downloading) = self.downloading {
            downloading.increment_download_count(line);
        }
    }

    pub fn partial_checkpoint_complete(&mut self, priority: BucketPriority, now: Timestamp) {
        self.debug_assert_priority_status_is_sorted();
        // We can delete entries with a higher priority because this partial sync includes them.
        self.priority_status.retain(|i| i.priority < priority);
        self.priority_status.insert(
            0,
            SyncPriorityStatus {
                priority: priority,
                last_synced_at: Some(now),
                has_synced: Some(true),
            },
        );
        self.debug_assert_priority_status_is_sorted();
    }

    pub fn applied_checkpoint(&mut self, now: Timestamp) {
        self.downloading = None;
        self.priority_status.clear();

        self.priority_status.push(SyncPriorityStatus {
            priority: BucketPriority::SENTINEL,
            last_synced_at: Some(now),
            has_synced: Some(true),
        });
    }
}

impl Default for DownloadSyncStatus {
    fn default() -> Self {
        Self {
            connected: false,
            connecting: false,
            downloading: None,
            priority_status: Vec::new(),
            streams: Vec::new(),
        }
    }
}

pub struct SyncStatusContainer {
    status: Rc<RefCell<DownloadSyncStatus>>,
    last_published_hash: u64,
}

impl SyncStatusContainer {
    pub fn new() -> Self {
        Self {
            status: Rc::new(RefCell::new(Default::default())),
            last_published_hash: 0,
        }
    }

    pub fn inner(&self) -> &Rc<RefCell<DownloadSyncStatus>> {
        &self.status
    }

    /// Invokes a function to update the sync status, then emits an [Instruction::UpdateSyncStatus]
    /// if the function did indeed change the status.
    pub fn update<F: FnOnce(&mut DownloadSyncStatus) -> ()>(
        &mut self,
        apply: F,
        instructions: &mut Vec<Instruction>,
    ) {
        self.update_only(apply);
        self.emit_changes(instructions);
    }

    /// Invokes a function to update the sync status without emitting a status event.
    pub fn update_only<F: FnOnce(&mut DownloadSyncStatus) -> ()>(&self, apply: F) {
        let mut status = self.status.borrow_mut();
        apply(&mut *status);
    }

    /// If the status has been changed since the last time an [Instruction::UpdateSyncStatus] event
    /// was emitted, emit such an event now.
    pub fn emit_changes(&mut self, instructions: &mut Vec<Instruction>) {
        let status = self.status.borrow();
        let hash = FxBuildHasher.hash_one(&*status);
        if hash != self.last_published_hash {
            self.last_published_hash = hash;
            instructions.push(Instruction::UpdateSyncStatus {
                status: self.status.clone(),
            });
        }
    }
}

#[repr(transparent)]
#[derive(Serialize, Hash, Clone, Copy)]
pub struct Timestamp(pub i64);

#[derive(Serialize, Hash)]
pub struct SyncPriorityStatus {
    pub priority: BucketPriority,
    pub last_synced_at: Option<Timestamp>,
    pub has_synced: Option<bool>,
}

/// Per-bucket download progress information.
#[derive(Serialize, Hash)]
pub struct BucketProgress {
    pub priority: BucketPriority,
    pub at_last: i64,
    pub since_last: i64,
    pub target_count: i64,
}

#[derive(Serialize, Hash)]
pub struct SyncDownloadProgress {
    buckets: BTreeMap<String, BucketProgress>,
}

pub struct SyncProgressFromCheckpoint {
    pub progress: SyncDownloadProgress,
    pub needs_counter_reset: bool,
}

impl SyncDownloadProgress {
    pub fn for_checkpoint<'a>(
        checkpoint: &OwnedCheckpoint,
        adapter: &StorageAdapter,
    ) -> Result<SyncProgressFromCheckpoint, ResultCode> {
        let mut buckets = BTreeMap::<String, BucketProgress>::new();
        let mut needs_reset = false;
        for bucket in checkpoint.buckets.values() {
            buckets.insert(
                bucket.bucket.clone(),
                BucketProgress {
                    priority: bucket.priority,
                    target_count: bucket.count.unwrap_or(0),
                    // Will be filled out later by iterating local_progress
                    at_last: 0,
                    since_last: 0,
                },
            );
        }

        // Ignore errors here - SQLite seems to report errors from an earlier statement iteration
        // sometimes.
        let _ = adapter.progress_stmt.reset();

        // Go through local bucket states to detect pending progress from previous sync iterations
        // that may have been interrupted.
        while let Some(row) = adapter.step_progress()? {
            let Some(progress) = buckets.get_mut(row.bucket) else {
                continue;
            };

            progress.at_last = row.count_at_last;
            progress.since_last = row.count_since_last;

            if progress.target_count < row.count_at_last + row.count_since_last {
                needs_reset = true;
                // Either due to a defrag / sync rule deploy or a compactioon operation, the size
                // of the bucket shrank so much that the local ops exceed the ops in the updated
                // bucket. We can't possibly report progress in this case (it would overshoot 100%).
                for (_, progress) in &mut buckets {
                    progress.at_last = 0;
                    progress.since_last = 0;
                }
                break;
            }
        }

        adapter.progress_stmt.reset()?;

        Ok(SyncProgressFromCheckpoint {
            progress: Self { buckets },
            needs_counter_reset: needs_reset,
        })
    }

    pub fn increment_download_count(&mut self, line: &DataLine) {
        if let Some(info) = self.buckets.get_mut(&*line.bucket) {
            info.since_last += line.data.len() as i64
        }
    }
}

#[derive(Serialize, Hash)]
pub struct ActiveStreamSubscription {
    #[serde(skip)]
    pub id: i64,
    pub name: String,
    pub parameters: Option<Box<JsonString>>,
    pub associated_buckets: Vec<String>,
    pub priority: Option<BucketPriority>,
    pub active: bool,
    pub is_default: bool,
    pub has_explicit_subscription: bool,
    pub expires_at: Option<Timestamp>,
    pub last_synced_at: Option<Timestamp>,
}

impl ActiveStreamSubscription {
    pub fn from_local(local: &LocallyTrackedSubscription) -> Self {
        Self {
            id: local.id,
            name: local.stream_name.clone(),
            parameters: local.local_params.clone(),
            is_default: local.is_default,
            priority: None,
            associated_buckets: Vec::new(),
            active: local.active,

            has_explicit_subscription: local.has_subscribed_manually(),
            expires_at: local.expires_at.clone().map(|e| Timestamp(e)),
            last_synced_at: local.last_synced_at.map(|e| Timestamp(e)),
        }
    }

    pub fn mark_associated_with_bucket(&mut self, bucket: &OwnedBucketChecksum) {
        match self.associated_buckets.binary_search(&bucket.bucket) {
            Ok(_) => {
                // The bucket is already part of the list
                return;
            }
            Err(position) => {
                // Insert here to keep vec sorted
                self.associated_buckets
                    .insert(position, bucket.bucket.clone());
            }
        };

        self.priority = Some(match self.priority {
            None => bucket.priority,
            Some(prio) => min(prio, bucket.priority),
        });
    }

    pub fn is_in_priority(&self, prio: Option<BucketPriority>) -> bool {
        match prio {
            None => true,
            Some(prio) => self.priority >= Some(prio),
        }
    }
}
