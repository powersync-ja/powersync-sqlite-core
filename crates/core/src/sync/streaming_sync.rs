use core::{
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use alloc::{
    boxed::Box,
    collections::{btree_map::BTreeMap, btree_set::BTreeSet},
    format,
    string::{String, ToString},
    vec::Vec,
};
use futures_lite::FutureExt;

use crate::{
    bson,
    error::SQLiteError,
    kv::client_id,
    util::{sqlite3_mutex, Mutex},
};
use sqlite_nostd::{self as sqlite, ResultCode};

use super::{
    bucket_priority::BucketPriority,
    interface::{Instruction, LogSeverity, StreamingSyncRequest, SyncControlRequest, SyncEvent},
    line::{BucketChecksum, Checkpoint, CheckpointDiff, SyncLine},
    operations::insert_bucket_operations,
    storage_adapter::{BucketDescription, StorageAdapter, SyncLocalResult},
    sync_status::{SyncDownloadProgress, SyncStatusContainer},
};

pub struct SyncClient {
    db: *mut sqlite::sqlite3,
    state: Mutex<ClientState>,
}

impl SyncClient {
    pub fn new(db: *mut sqlite::sqlite3) -> Self {
        Self {
            db,
            state: sqlite3_mutex(ClientState::Idle),
        }
    }

    pub fn push_event<'a>(
        &self,
        event: SyncControlRequest<'a>,
    ) -> Result<Vec<Instruction>, SQLiteError> {
        let mut state = self.state.lock();

        match event {
            SyncControlRequest::StartSyncStream { parameters } => {
                state.tear_down()?;

                let mut handle = SyncIterationHandle::new(self.db, parameters)?;
                let instructions = handle.initialize()?;
                *state = ClientState::IterationActive(handle);

                Ok(instructions)
            }
            SyncControlRequest::SyncEvent(sync_event) => {
                let mut active = ActiveEvent::new(sync_event);

                let ClientState::IterationActive(handle) = &mut *state else {
                    return Err(SQLiteError(
                        ResultCode::MISUSE,
                        Some("No iteration is active".to_string()),
                    ));
                };

                match handle.run(&mut active) {
                    Err(e) => {
                        *state = ClientState::Idle;
                        return Err(e);
                    }
                    Ok(done) => {
                        if done {
                            *state = ClientState::Idle;
                        }
                    }
                };

                Ok(active.instructions)
            }
            SyncControlRequest::StopSyncStream => state.tear_down(),
        }
    }
}

enum ClientState {
    Idle,
    IterationActive(SyncIterationHandle),
}

impl ClientState {
    fn tear_down(&mut self) -> Result<Vec<Instruction>, SQLiteError> {
        let mut event = ActiveEvent::new(SyncEvent::TearDown);

        if let ClientState::IterationActive(old) = self {
            old.run(&mut event)?;
        };

        *self = ClientState::Idle;
        Ok(event.instructions)
    }
}

struct SyncIterationHandle {
    future: Pin<Box<dyn Future<Output = Result<(), SQLiteError>>>>,
}

impl SyncIterationHandle {
    fn new(
        db: *mut sqlite::sqlite3,
        parameters: Option<serde_json::Map<String, serde_json::Value>>,
    ) -> Result<Self, ResultCode> {
        let runner = StreamingSyncIteration {
            db,
            parameters,
            adapter: StorageAdapter::new(db)?,
            status: SyncStatusContainer::new(),
        };
        let future = runner.run().boxed_local();

        Ok(Self { future })
    }

    fn initialize(&mut self) -> Result<Vec<Instruction>, SQLiteError> {
        let mut event = ActiveEvent::new(SyncEvent::Initialize);
        let result = self.run(&mut event)?;
        assert!(!result, "Stream client aborted initialization");

        Ok(event.instructions)
    }

    fn run(&mut self, active: &mut ActiveEvent) -> Result<bool, SQLiteError> {
        // Using a noop waker because the only event thing StreamingSyncIteration::run polls on is
        // the next incoming sync event.
        let waker = unsafe {
            Waker::new(
                active as *const ActiveEvent as *const (),
                Waker::noop().vtable(),
            )
        };
        let mut context = Context::from_waker(&waker);

        Ok(
            if let Poll::Ready(result) = self.future.poll(&mut context) {
                result?;

                active.instructions.push(Instruction::CloseSyncStream {});
                true
            } else {
                false
            },
        )
    }
}

struct ActiveEvent<'a> {
    handled: bool,
    event: SyncEvent<'a>,
    instructions: Vec<Instruction>,
}

impl<'a> ActiveEvent<'a> {
    pub fn new(event: SyncEvent<'a>) -> Self {
        Self {
            handled: false,
            event,
            instructions: Vec::new(),
        }
    }
}

struct StreamingSyncIteration {
    db: *mut sqlite::sqlite3,
    adapter: StorageAdapter,
    parameters: Option<serde_json::Map<String, serde_json::Value>>,
    status: SyncStatusContainer,
}

impl StreamingSyncIteration {
    fn receive_event<'a>() -> impl Future<Output = &'a mut ActiveEvent<'a>> {
        struct Wait<'a> {
            a: PhantomData<&'a StreamingSyncIteration>,
        }

        impl<'a> Future for Wait<'a> {
            type Output = &'a mut ActiveEvent<'a>;

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                let context = cx.waker().data().cast_mut() as *mut ActiveEvent;
                let context = unsafe { &mut *context };

                if context.handled {
                    Poll::Pending
                } else {
                    context.handled = true;
                    Poll::Ready(context)
                }
            }
        }

        Wait { a: PhantomData }
    }

    async fn run(mut self) -> Result<(), SQLiteError> {
        let mut validated = None::<OwnedCheckpoint>;
        let mut applied = None::<OwnedCheckpoint>;

        let mut target = SyncTarget::BeforeCheckpoint(self.prepare_request().await?);

        loop {
            let event = Self::receive_event().await;

            let line: SyncLine = match event.event {
                SyncEvent::Initialize { .. } => {
                    panic!("Initialize should only be emited once")
                }
                SyncEvent::TearDown => break,
                SyncEvent::TextLine { data } => serde_json::from_str(data)?,
                SyncEvent::BinaryLine { data } => bson::from_bytes(data)?,
                SyncEvent::DidRefreshToken => {
                    // Break so that the client SDK starts another iteration.
                    break;
                }
            };

            match line {
                SyncLine::Checkpoint(checkpoint) => {
                    let to_delete = target.track_checkpoint(&checkpoint);

                    self.adapter
                        .delete_buckets(to_delete.iter().map(|b| b.as_str()))?;
                    let progress = self.load_progress(target.target_checkpoint().unwrap())?;
                    self.status.update(
                        |s| s.start_tracking_checkpoint(progress),
                        &mut event.instructions,
                    );
                }
                SyncLine::CheckpointDiff(diff) => {
                    let Some(target) = target.target_checkpoint_mut() else {
                        return Err(SQLiteError(
                            ResultCode::ABORT,
                            Some(
                                "Received checkpoint_diff without previous checkpoint".to_string(),
                            ),
                        ));
                    };

                    target.apply_diff(&diff);
                    self.adapter
                        .delete_buckets(diff.removed_buckets.iter().copied())?;

                    let progress = self.load_progress(target)?;
                    self.status.update(
                        |s| s.start_tracking_checkpoint(progress),
                        &mut event.instructions,
                    );
                }
                SyncLine::CheckpointComplete(checkpoint_complete) => {
                    let Some(target) = target.target_checkpoint_mut() else {
                        return Err(SQLiteError(
                            ResultCode::ABORT,
                            Some(
                                "Received checkpoint complete without previous checkpoint"
                                    .to_string(),
                            ),
                        ));
                    };
                    let result = self.adapter.sync_local(target, None)?;

                    match result {
                        SyncLocalResult::ChecksumFailure(checkpoint_result) => {
                            // This means checksums failed. Start again with a new checkpoint.
                            // TODO: better back-off
                            // await new Promise((resolve) => setTimeout(resolve, 50));
                            event.instructions.push(Instruction::LogLine {
                                severity: LogSeverity::WARNING,
                                line: format!("Could not apply checkpoint, {checkpoint_result}"),
                            });
                            break;
                        }
                        SyncLocalResult::PendingLocalChanges => {
                            event.instructions.push(Instruction::LogLine {
                                severity: LogSeverity::WARNING,
                                line: format!("TODO: Await pending uploads and try again"),
                            });
                            break;
                        }
                        SyncLocalResult::ChangesApplied => {
                            event.instructions.push(Instruction::LogLine {
                                severity: LogSeverity::DEBUG,
                                line: format!("Validated and applied checkpoint"),
                            });
                            event.instructions.push(Instruction::DidCompleteSync {});

                            let now = self.adapter.now()?;
                            self.status.update(
                                |status| status.applied_checkpoint(target, now),
                                &mut event.instructions,
                            );
                        }
                    }
                }
                SyncLine::CheckpointPartiallyComplete(complete) => {
                    let priority = complete.priority;
                    let Some(target) = target.target_checkpoint_mut() else {
                        return Err(SQLiteError(
                            ResultCode::ABORT,
                            Some(
                                "Received checkpoint complete without previous checkpoint"
                                    .to_string(),
                            ),
                        ));
                    };
                    let result = self.adapter.sync_local(target, Some(priority))?;

                    match result {
                        SyncLocalResult::ChecksumFailure(checkpoint_result) => {
                            // This means checksums failed. Start again with a new checkpoint.
                            // TODO: better back-off
                            // await new Promise((resolve) => setTimeout(resolve, 50));
                            event.instructions.push(Instruction::LogLine {
                                severity: LogSeverity::WARNING,
                                line: format!(
                                    "Could not apply partial checkpoint, {checkpoint_result}"
                                ),
                            });
                            break;
                        }
                        SyncLocalResult::PendingLocalChanges => {
                            // If we have pending uploads, we can't complete new checkpoints outside
                            // of priority 0. We'll resolve this for a complete checkpoint later.
                        }
                        SyncLocalResult::ChangesApplied => {
                            let now = self.adapter.now()?;
                            self.status.update(
                                |status| {
                                    status.partial_checkpoint_complete(priority, now);
                                },
                                &mut event.instructions,
                            );
                        }
                    }
                }
                SyncLine::Data(data_line) => {
                    self.status
                        .update(|s| s.track_line(&data_line), &mut event.instructions);
                    insert_bucket_operations(&self.adapter, &data_line)?;
                }
                SyncLine::KeepAlive(token) => {
                    if token.is_expired() {
                        // Token expired already - stop the connection immediately.
                        event
                            .instructions
                            .push(Instruction::FetchCredentials { did_expire: true });
                        break;
                    } else if token.should_prefetch() {
                        event
                            .instructions
                            .push(Instruction::FetchCredentials { did_expire: false });
                    }
                }
            }
        }

        Ok(())
    }

    fn load_progress(
        &self,
        checkpoint: &OwnedCheckpoint,
    ) -> Result<SyncDownloadProgress, SQLiteError> {
        let local_progress = self.adapter.local_progress()?;
        Ok(SyncDownloadProgress::for_checkpoint(
            checkpoint,
            local_progress,
        )?)
    }

    async fn prepare_request(&mut self) -> Result<Vec<String>, SQLiteError> {
        let event = Self::receive_event().await;
        let SyncEvent::Initialize = event.event else {
            return Err(SQLiteError::from(ResultCode::MISUSE));
        };

        self.status
            .update(|s| s.start_connecting(), &mut event.instructions);

        let requests = self.adapter.collect_bucket_requests()?;
        let local_bucket_names: Vec<String> = requests.iter().map(|s| s.name.clone()).collect();
        let request = StreamingSyncRequest {
            buckets: requests,
            include_checksum: true,
            raw_data: true,
            binary_data: true,
            client_id: client_id(self.db)?,
            parameters: self.parameters.take(),
        };

        event
            .instructions
            .push(Instruction::EstablishSyncStream { request });
        Ok(local_bucket_names)
    }
}

#[derive(Debug)]
enum SyncTarget {
    /// We've received a checkpoint line towards the given checkpoint. The tracked checkpoint is
    /// updated for subsequent checkpoint or checkpoint_diff lines.
    Tracking(OwnedCheckpoint),
    /// We have not received a checkpoint message yet. We still keep a list of local buckets around
    /// so that we know which ones to delete depending on the first checkpoint message.
    BeforeCheckpoint(Vec<String>),
}

impl SyncTarget {
    fn target_checkpoint(&self) -> Option<&OwnedCheckpoint> {
        match self {
            Self::Tracking(cp) => Some(cp),
            _ => None,
        }
    }

    fn target_checkpoint_mut(&mut self) -> Option<&mut OwnedCheckpoint> {
        match self {
            Self::Tracking(cp) => Some(cp),
            _ => None,
        }
    }

    fn track_checkpoint<'a>(&mut self, checkpoint: &Checkpoint<'a>) -> BTreeSet<String> {
        let mut to_delete: BTreeSet<String> = match &self {
            SyncTarget::Tracking(checkpoint) => checkpoint.buckets.keys().cloned().collect(),
            SyncTarget::BeforeCheckpoint(buckets) => buckets.iter().cloned().collect(),
        };

        let mut buckets = BTreeMap::<String, OwnedBucketChecksum>::new();
        for bucket in &checkpoint.buckets {
            buckets.insert(bucket.bucket.to_string(), OwnedBucketChecksum::from(bucket));
            to_delete.remove(bucket.bucket);
        }

        *self = SyncTarget::Tracking(OwnedCheckpoint::from_checkpoint(checkpoint, buckets));
        to_delete
    }
}

#[derive(Debug)]
pub struct OwnedCheckpoint {
    pub last_op_id: i64,
    pub write_checkpoint: Option<i64>,
    pub buckets: BTreeMap<String, OwnedBucketChecksum>,
}

impl OwnedCheckpoint {
    fn from_checkpoint<'a>(
        checkpoint: &Checkpoint<'a>,
        buckets: BTreeMap<String, OwnedBucketChecksum>,
    ) -> Self {
        Self {
            last_op_id: checkpoint.last_op_id,
            write_checkpoint: checkpoint.write_checkpoint,
            buckets: buckets,
        }
    }

    fn apply_diff<'a>(&mut self, diff: &CheckpointDiff<'a>) {
        for removed in &diff.removed_buckets {
            self.buckets.remove(*removed);
        }

        for updated in &diff.updated_buckets {
            let owned = OwnedBucketChecksum::from(updated);
            self.buckets.insert(owned.bucket.clone(), owned);
        }

        self.last_op_id = diff.last_op_id;
        self.write_checkpoint = diff.write_checkpoint;
    }
}

#[derive(Debug)]
pub struct OwnedBucketChecksum {
    pub bucket: String,
    pub checksum: i32,
    pub priority: BucketPriority,
    pub count: Option<i64>,
    pub last_op_id: Option<i64>,
}

impl OwnedBucketChecksum {
    pub fn is_in_priority(&self, prio: Option<BucketPriority>) -> bool {
        match prio {
            None => true,
            Some(prio) => self.priority >= prio,
        }
    }
}

impl From<&'_ BucketChecksum<'_>> for OwnedBucketChecksum {
    fn from(value: &'_ BucketChecksum<'_>) -> Self {
        Self {
            bucket: value.bucket.to_string(),
            checksum: value.checksum,
            priority: value.priority.unwrap_or(BucketPriority::FALLBACK),
            count: value.count,
            last_op_id: value.last_op_id,
        }
    }
}
