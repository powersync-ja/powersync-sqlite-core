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
    sync::Arc,
    vec::Vec,
};
use futures_lite::FutureExt;

use crate::{
    bson,
    error::SQLiteError,
    kv::client_id,
    state::DatabaseState,
    sync::{checkpoint::OwnedBucketChecksum, interface::StartSyncStream},
};
use sqlite_nostd::{self as sqlite, ResultCode};

use super::{
    interface::{Instruction, LogSeverity, StreamingSyncRequest, SyncControlRequest, SyncEvent},
    line::{Checkpoint, CheckpointDiff, SyncLine},
    operations::insert_bucket_operations,
    storage_adapter::{StorageAdapter, SyncLocalResult},
    sync_status::{SyncDownloadProgress, SyncProgressFromCheckpoint, SyncStatusContainer},
};

/// The sync client implementation, responsible for parsing lines received by the sync service and
/// persisting them to the database.
///
/// The client consumes no resources and prepares no statements until a sync iteration is
/// initialized.
pub struct SyncClient {
    db: *mut sqlite::sqlite3,
    db_state: Arc<DatabaseState>,
    /// The current [ClientState] (essentially an optional [StreamingSyncIteration]).
    state: ClientState,
}

impl SyncClient {
    pub fn new(db: *mut sqlite::sqlite3, state: Arc<DatabaseState>) -> Self {
        Self {
            db,
            db_state: state,
            state: ClientState::Idle,
        }
    }

    pub fn push_event<'a>(
        &mut self,
        event: SyncControlRequest<'a>,
    ) -> Result<Vec<Instruction>, SQLiteError> {
        match event {
            SyncControlRequest::StartSyncStream(options) => {
                self.state.tear_down()?;

                let mut handle = SyncIterationHandle::new(self.db, options, self.db_state.clone())?;
                let instructions = handle.initialize()?;
                self.state = ClientState::IterationActive(handle);

                Ok(instructions)
            }
            SyncControlRequest::SyncEvent(sync_event) => {
                let mut active = ActiveEvent::new(sync_event);

                let ClientState::IterationActive(handle) = &mut self.state else {
                    return Err(SQLiteError(
                        ResultCode::MISUSE,
                        Some("No iteration is active".to_string()),
                    ));
                };

                match handle.run(&mut active) {
                    Err(e) => {
                        self.state = ClientState::Idle;
                        return Err(e);
                    }
                    Ok(done) => {
                        if done {
                            self.state = ClientState::Idle;
                        }
                    }
                };

                Ok(active.instructions)
            }
            SyncControlRequest::StopSyncStream => self.state.tear_down(),
        }
    }
}

enum ClientState {
    /// No sync iteration is currently active.
    Idle,
    /// A sync iteration has begun on the database.
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

/// A handle that allows progressing a [StreamingSyncIteration].
///
/// The sync itertion itself is implemented as an `async` function, as this allows us to treat it
/// as a coroutine that preserves internal state between multiple `powersync_control` invocations.
/// At each invocation, the future is polled once (and gets access to context that allows it to
/// render [Instruction]s to return from the function).
struct SyncIterationHandle {
    future: Pin<Box<dyn Future<Output = Result<(), SQLiteError>>>>,
}

impl SyncIterationHandle {
    /// Creates a new sync iteration in a pending state by preparing statements for
    /// [StorageAdapter] and setting up the initial downloading state for [StorageAdapter] .
    fn new(
        db: *mut sqlite::sqlite3,
        options: StartSyncStream,
        state: Arc<DatabaseState>,
    ) -> Result<Self, ResultCode> {
        let runner = StreamingSyncIteration {
            db,
            options,
            state,
            adapter: StorageAdapter::new(db)?,
            status: SyncStatusContainer::new(),
        };
        let future = runner.run().boxed_local();

        Ok(Self { future })
    }

    /// Forwards a [SyncEvent::Initialize] to the current sync iteration, returning the initial
    /// instructions generated.
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

/// A [SyncEvent] currently being handled by a [StreamingSyncIteration].
struct ActiveEvent<'a> {
    handled: bool,
    /// The event to handle
    event: SyncEvent<'a>,
    /// Instructions to forward to the client when the `powersync_control` invocation completes.
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
    state: Arc<DatabaseState>,
    adapter: StorageAdapter,
    options: StartSyncStream,
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
        let mut target = SyncTarget::BeforeCheckpoint(self.prepare_request().await?);

        // A checkpoint that has been fully received and validated, but couldn't be applied due to
        // pending local data. We will retry applying this checkpoint when the client SDK informs us
        // that it has finished uploading changes.
        let mut validated_but_not_applied = None::<OwnedCheckpoint>;

        loop {
            let event = Self::receive_event().await;

            let line: SyncLine = match event.event {
                SyncEvent::Initialize { .. } => {
                    panic!("Initialize should only be emited once")
                }
                SyncEvent::TearDown => {
                    self.status
                        .update(|s| s.disconnect(), &mut event.instructions);
                    break;
                }
                SyncEvent::TextLine { data } => serde_json::from_str(data)?,
                SyncEvent::BinaryLine { data } => bson::from_bytes(data)?,
                SyncEvent::UploadFinished => {
                    if let Some(checkpoint) = validated_but_not_applied.take() {
                        let result = self.adapter.sync_local(
                            &self.state,
                            &checkpoint,
                            None,
                            &self.options.schema,
                        )?;

                        match result {
                            SyncLocalResult::ChangesApplied => {
                                event.instructions.push(Instruction::LogLine {
                                    severity: LogSeverity::DEBUG,
                                    line: "Applied pending checkpoint after completed upload"
                                        .into(),
                                });

                                self.handle_checkpoint_applied(event)?;
                            }
                            _ => {
                                event.instructions.push(Instruction::LogLine {
                                    severity: LogSeverity::WARNING,
                                    line: "Could not apply pending checkpoint even after completed upload"
                                        .into(),
                                });
                            }
                        }
                    }

                    continue;
                }
                SyncEvent::DidRefreshToken => {
                    // Break so that the client SDK starts another iteration.
                    break;
                }
            };

            self.status.update_only(|s| s.mark_connected());

            match line {
                SyncLine::Checkpoint(checkpoint) => {
                    validated_but_not_applied = None;
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
                    validated_but_not_applied = None;
                    self.adapter
                        .delete_buckets(diff.removed_buckets.iter().map(|i| &**i))?;

                    let progress = self.load_progress(target)?;
                    self.status.update(
                        |s| s.start_tracking_checkpoint(progress),
                        &mut event.instructions,
                    );
                }
                SyncLine::CheckpointComplete(_) => {
                    let Some(target) = target.target_checkpoint_mut() else {
                        return Err(SQLiteError(
                            ResultCode::ABORT,
                            Some(
                                "Received checkpoint complete without previous checkpoint"
                                    .to_string(),
                            ),
                        ));
                    };
                    let result =
                        self.adapter
                            .sync_local(&self.state, target, None, &self.options.schema)?;

                    match result {
                        SyncLocalResult::ChecksumFailure(checkpoint_result) => {
                            // This means checksums failed. Start again with a new checkpoint.
                            // TODO: better back-off
                            // await new Promise((resolve) => setTimeout(resolve, 50));
                            event.instructions.push(Instruction::LogLine {
                                severity: LogSeverity::WARNING,
                                line: format!("Could not apply checkpoint, {checkpoint_result}")
                                    .into(),
                            });
                            break;
                        }
                        SyncLocalResult::PendingLocalChanges => {
                            event.instructions.push(Instruction::LogLine {
                                    severity: LogSeverity::INFO,
                                    line: "Could not apply checkpoint due to local data. Will retry at completed upload or next checkpoint.".into(),
                                });

                            validated_but_not_applied = Some(target.clone());
                        }
                        SyncLocalResult::ChangesApplied => {
                            event.instructions.push(Instruction::LogLine {
                                severity: LogSeverity::DEBUG,
                                line: "Validated and applied checkpoint".into(),
                            });
                            event.instructions.push(Instruction::FlushFileSystem {});
                            self.handle_checkpoint_applied(event)?;
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
                    let result = self.adapter.sync_local(
                        &self.state,
                        target,
                        Some(priority),
                        &self.options.schema,
                    )?;

                    match result {
                        SyncLocalResult::ChecksumFailure(checkpoint_result) => {
                            // This means checksums failed. Start again with a new checkpoint.
                            // TODO: better back-off
                            // await new Promise((resolve) => setTimeout(resolve, 50));
                            event.instructions.push(Instruction::LogLine {
                                severity: LogSeverity::WARNING,
                                line: format!(
                                    "Could not apply partial checkpoint, {checkpoint_result}"
                                )
                                .into(),
                            });
                            break;
                        }
                        SyncLocalResult::PendingLocalChanges => {
                            // If we have pending uploads, we can't complete new checkpoints outside
                            // of priority 0. We'll resolve this for a complete checkpoint later.
                        }
                        SyncLocalResult::ChangesApplied => {
                            let now = self.adapter.now()?;
                            event.instructions.push(Instruction::FlushFileSystem {});
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

            self.status.emit_changes(&mut event.instructions);
        }

        Ok(())
    }

    fn load_progress(
        &self,
        checkpoint: &OwnedCheckpoint,
    ) -> Result<SyncDownloadProgress, SQLiteError> {
        let local_progress = self.adapter.local_progress()?;
        let SyncProgressFromCheckpoint {
            progress,
            needs_counter_reset,
        } = SyncDownloadProgress::for_checkpoint(checkpoint, local_progress)?;

        if needs_counter_reset {
            self.adapter.reset_progress()?;
        }

        Ok(progress)
    }

    /// Prepares a sync iteration by handling the initial [SyncEvent::Initialize].
    ///
    /// This prepares a [StreamingSyncRequest] by fetching local sync state and the requested bucket
    /// parameters.
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
            parameters: self.options.parameters.take(),
        };

        event
            .instructions
            .push(Instruction::EstablishSyncStream { request });
        Ok(local_bucket_names)
    }

    fn handle_checkpoint_applied(&mut self, event: &mut ActiveEvent) -> Result<(), ResultCode> {
        event.instructions.push(Instruction::DidCompleteSync {});

        let now = self.adapter.now()?;
        self.status.update(
            |status| status.applied_checkpoint(now),
            &mut event.instructions,
        );

        Ok(())
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

    /// Starts tracking the received `Checkpoint`.
    ///
    /// This updates the internal state and returns a set of buckets to delete because they've been
    /// tracked locally but not in the new checkpoint.
    fn track_checkpoint<'a>(&mut self, checkpoint: &Checkpoint<'a>) -> BTreeSet<String> {
        let mut to_delete: BTreeSet<String> = match &self {
            SyncTarget::Tracking(checkpoint) => checkpoint.buckets.keys().cloned().collect(),
            SyncTarget::BeforeCheckpoint(buckets) => buckets.iter().cloned().collect(),
        };

        let mut buckets = BTreeMap::<String, OwnedBucketChecksum>::new();
        for bucket in &checkpoint.buckets {
            buckets.insert(bucket.bucket.to_string(), OwnedBucketChecksum::from(bucket));
            to_delete.remove(&*bucket.bucket);
        }

        *self = SyncTarget::Tracking(OwnedCheckpoint::from_checkpoint(checkpoint, buckets));
        to_delete
    }
}

#[derive(Debug, Clone)]
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
            self.buckets.remove(&**removed);
        }

        for updated in &diff.updated_buckets {
            let owned = OwnedBucketChecksum::from(updated);
            self.buckets.insert(owned.bucket.clone(), owned);
        }

        self.last_op_id = diff.last_op_id;
        self.write_checkpoint = diff.write_checkpoint;
    }
}
