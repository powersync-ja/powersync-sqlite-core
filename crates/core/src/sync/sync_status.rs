use alloc::{collections::btree_map::BTreeMap, rc::Rc, string::String, vec::Vec};
use core::{cell::RefCell, hash::BuildHasher};
use rustc_hash::FxBuildHasher;
use serde::Serialize;
use sqlite_nostd::ResultCode;
use streaming_iterator::StreamingIterator;

use super::{
    bucket_priority::BucketPriority, interface::Instruction, line::DataLine,
    storage_adapter::PersistedBucketProgress, streaming_sync::OwnedCheckpoint,
};

#[derive(Serialize, Hash)]
pub struct DownloadSyncStatus {
    pub connected: bool,
    pub connecting: bool,
    pub priority_status: Vec<SyncPriorityStatus>,
    pub downloading: Option<SyncDownloadProgress>,
}

impl DownloadSyncStatus {
    pub fn start_connecting(&mut self) {
        self.connected = false;
        self.downloading = None;
        self.connecting = true;
    }

    pub fn mark_connected(&mut self) {
        self.connecting = false;
        self.connected = true;
    }

    /// Transitions state after receiving a checkpoint line.
    ///
    /// This sets the [downloading] state to include [progress].
    pub fn start_tracking_checkpoint<'a>(&mut self, progress: SyncDownloadProgress) {
        self.mark_connected();

        self.downloading = Some(progress);
    }

    pub fn track_line(&mut self, line: &DataLine) {
        if let Some(ref mut downloading) = self.downloading {
            downloading.increment_download_count(line);
        }
    }
}

impl Default for DownloadSyncStatus {
    fn default() -> Self {
        Self {
            connected: false,
            connecting: false,
            downloading: None,
            priority_status: Vec::new(),
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

    /// Invokes a function to update the sync status, then emits an [Instruction::UpdateSyncStatus]
    /// if the function did indeed change the status.
    pub fn update<F: FnOnce(&mut DownloadSyncStatus) -> ()>(
        &mut self,
        apply: F,
        instructions: &mut Vec<Instruction>,
    ) {
        let mut status = self.status.borrow_mut();
        apply(&mut *status);

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
#[derive(Serialize, Hash)]
pub struct Timestamp(pub i64);

#[derive(Serialize, Hash)]
pub struct SyncPriorityStatus {
    priority: BucketPriority,
    last_synced_at: Option<Timestamp>,
    has_synced: Option<bool>,
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

impl SyncDownloadProgress {
    pub fn for_checkpoint<'a>(
        checkpoint: &OwnedCheckpoint,
        mut local_progress: impl StreamingIterator<
            Item = Result<PersistedBucketProgress<'a>, ResultCode>,
        >,
    ) -> Result<Self, ResultCode> {
        let mut buckets = BTreeMap::<String, BucketProgress>::new();
        for bucket in &checkpoint.buckets {
            buckets.insert(
                bucket.bucket.clone(),
                BucketProgress {
                    priority: bucket.priority.unwrap_or(BucketPriority::FALLBACK),
                    target_count: bucket.count.unwrap_or(0),
                    // Will be filled out later by iterating local_progress
                    at_last: 0,
                    since_last: 0,
                },
            );
        }

        while let Some(row) = local_progress.next() {
            let row = match row {
                Ok(row) => row,
                Err(e) => return Err(*e),
            };

            let Some(progress) = buckets.get_mut(row.bucket) else {
                continue;
            };

            progress.at_last = row.count_at_last;
            progress.since_last = row.count_since_last;
        }

        Ok(Self { buckets })
    }

    pub fn increment_download_count(&mut self, line: &DataLine) {
        if let Some(info) = self.buckets.get_mut(line.bucket) {
            info.since_last += line.data.len() as i64
        }
    }
}
