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
    error::{PowerSyncError, PowerSyncErrorCause},
    kv::client_id,
    state::DatabaseState,
    sync::{
        BucketPriority, checkpoint::OwnedBucketChecksum, interface::StartSyncStream,
        line::DataLine, sync_status::Timestamp,
    },
};
use sqlite_nostd::{self as sqlite};

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
    ) -> Result<Vec<Instruction>, PowerSyncError> {
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
                    return Err(PowerSyncError::state_error("No iteration is active"));
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

                if let Some(recoverable) = active.recoverable_error.take() {
                    Err(recoverable)
                } else {
                    Ok(active.instructions)
                }
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
    fn tear_down(&mut self) -> Result<Vec<Instruction>, PowerSyncError> {
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
    future: Pin<Box<dyn Future<Output = Result<(), PowerSyncError>>>>,
}

impl SyncIterationHandle {
    /// Creates a new sync iteration in a pending state by preparing statements for
    /// [StorageAdapter] and setting up the initial downloading state for [StorageAdapter] .
    fn new(
        db: *mut sqlite::sqlite3,
        options: StartSyncStream,
        state: Arc<DatabaseState>,
    ) -> Result<Self, PowerSyncError> {
        let runner = StreamingSyncIteration {
            db,
            options,
            state,
            adapter: StorageAdapter::new(db)?,
            status: SyncStatusContainer::new(),
            validated_but_not_applied: None,
        };
        let future = runner.run().boxed_local();

        Ok(Self { future })
    }

    /// Forwards a [SyncEvent::Initialize] to the current sync iteration, returning the initial
    /// instructions generated.
    fn initialize(&mut self) -> Result<Vec<Instruction>, PowerSyncError> {
        let mut event = ActiveEvent::new(SyncEvent::Initialize);
        let result = self.run(&mut event)?;
        assert!(!result, "Stream client aborted initialization");

        Ok(event.instructions)
    }

    fn run(&mut self, active: &mut ActiveEvent) -> Result<bool, PowerSyncError> {
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
    /// An error to return to the client for a `powersync_control` invocation when that error
    /// shouldn't interrupt the sync iteration.
    ///
    /// For errors that do close the iteration, we report a result by having [SyncIterationHandle::run]
    /// returning the error.
    recoverable_error: Option<PowerSyncError>,
    /// Instructions to forward to the client when the `powersync_control` invocation completes.
    instructions: Vec<Instruction>,
}

impl<'a> ActiveEvent<'a> {
    pub fn new(event: SyncEvent<'a>) -> Self {
        Self {
            handled: false,
            event,
            recoverable_error: None,
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
    // A checkpoint that has been fully received and validated, but couldn't be applied due to
    // pending local data. We will retry applying this checkpoint when the client SDK informs us
    // that it has finished uploading changes.
    validated_but_not_applied: Option<OwnedCheckpoint>,
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

    /// Starts handling a single sync line without altering any in-memory state of the state
    /// machine.
    ///
    /// After this call succeeds, the returned value can be used to update the state. For a
    /// discussion on why this split is necessary, see [SyncStateMachineTransition].
    fn prepare_handling_sync_line<'a>(
        &self,
        target: &SyncTarget,
        event: &mut ActiveEvent,
        line: &'a SyncLine<'a>,
    ) -> Result<SyncStateMachineTransition<'a>, PowerSyncError> {
        Ok(match line {
            SyncLine::Checkpoint(checkpoint) => {
                let (to_delete, updated_target) = target.track_checkpoint(&checkpoint);

                self.adapter
                    .delete_buckets(to_delete.iter().map(|b| b.as_str()))?;
                let progress = self.load_progress(updated_target.target_checkpoint().unwrap())?;
                SyncStateMachineTransition::StartTrackingCheckpoint {
                    progress,
                    updated_target,
                }
            }
            SyncLine::CheckpointDiff(diff) => {
                let Some(target) = target.target_checkpoint() else {
                    return Err(PowerSyncError::sync_protocol_error(
                        "Received checkpoint_diff without previous checkpoint",
                        PowerSyncErrorCause::Unknown,
                    ));
                };

                let mut target = target.clone();
                target.apply_diff(&diff);
                self.adapter
                    .delete_buckets(diff.removed_buckets.iter().map(|i| &**i))?;

                let progress = self.load_progress(&target)?;
                SyncStateMachineTransition::StartTrackingCheckpoint {
                    progress,
                    updated_target: SyncTarget::Tracking(target),
                }
            }
            SyncLine::CheckpointComplete(_) => {
                let Some(target) = target.target_checkpoint() else {
                    return Err(PowerSyncError::sync_protocol_error(
                        "Received checkpoint complete without previous checkpoint",
                        PowerSyncErrorCause::Unknown,
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
                            line: format!("Could not apply checkpoint, {checkpoint_result}").into(),
                        });
                        SyncStateMachineTransition::CloseIteration
                    }
                    SyncLocalResult::PendingLocalChanges => {
                        event.instructions.push(Instruction::LogLine {
                                    severity: LogSeverity::INFO,
                                    line: "Could not apply checkpoint due to local data. Will retry at completed upload or next checkpoint.".into(),
                                });

                        SyncStateMachineTransition::SyncLocalFailedDueToPendingCrud {
                            validated_but_not_applied: target.clone(),
                        }
                    }
                    SyncLocalResult::ChangesApplied => {
                        event.instructions.push(Instruction::LogLine {
                            severity: LogSeverity::DEBUG,
                            line: "Validated and applied checkpoint".into(),
                        });
                        event.instructions.push(Instruction::FlushFileSystem {});
                        SyncStateMachineTransition::SyncLocalChangesApplied {
                            partial: None,
                            timestamp: self.adapter.now()?,
                        }
                    }
                }
            }
            SyncLine::CheckpointPartiallyComplete(complete) => {
                let priority = complete.priority;
                let Some(target) = target.target_checkpoint() else {
                    return Err(PowerSyncError::state_error(
                        "Received checkpoint complete without previous checkpoint",
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
                        SyncStateMachineTransition::CloseIteration
                    }
                    SyncLocalResult::PendingLocalChanges => {
                        // If we have pending uploads, we can't complete new checkpoints outside
                        // of priority 0. We'll resolve this for a complete checkpoint later.
                        SyncStateMachineTransition::Empty
                    }
                    SyncLocalResult::ChangesApplied => {
                        let now = self.adapter.now()?;
                        SyncStateMachineTransition::SyncLocalChangesApplied {
                            partial: Some(priority),
                            timestamp: now,
                        }
                    }
                }
            }
            SyncLine::Data(data_line) => {
                insert_bucket_operations(&self.adapter, &data_line)?;
                SyncStateMachineTransition::DataLineSaved { line: data_line }
            }
            SyncLine::KeepAlive(token) => {
                if token.is_expired() {
                    // Token expired already - stop the connection immediately.
                    event
                        .instructions
                        .push(Instruction::FetchCredentials { did_expire: true });

                    SyncStateMachineTransition::CloseIteration
                } else if token.should_prefetch() {
                    event
                        .instructions
                        .push(Instruction::FetchCredentials { did_expire: false });
                    SyncStateMachineTransition::Empty
                } else {
                    SyncStateMachineTransition::Empty
                }
            }
            SyncLine::UnknownSyncLine => {
                event.instructions.push(Instruction::LogLine {
                    severity: LogSeverity::DEBUG,
                    line: "Unknown sync line".into(),
                });
                SyncStateMachineTransition::Empty
            }
        })
    }

    /// Applies a sync state transition, returning whether the iteration should be stopped.
    fn apply_transition(
        &mut self,
        target: &mut SyncTarget,
        event: &mut ActiveEvent,
        transition: SyncStateMachineTransition,
    ) -> bool {
        match transition {
            SyncStateMachineTransition::StartTrackingCheckpoint {
                progress,
                updated_target,
            } => {
                self.status.update(
                    |s| s.start_tracking_checkpoint(progress),
                    &mut event.instructions,
                );
                self.validated_but_not_applied = None;
                *target = updated_target;
            }
            SyncStateMachineTransition::DataLineSaved { line } => {
                self.status
                    .update(|s| s.track_line(&line), &mut event.instructions);
            }
            SyncStateMachineTransition::CloseIteration => return true,
            SyncStateMachineTransition::SyncLocalFailedDueToPendingCrud {
                validated_but_not_applied,
            } => {
                self.validated_but_not_applied = Some(validated_but_not_applied);
            }
            SyncStateMachineTransition::SyncLocalChangesApplied { partial, timestamp } => {
                if let Some(priority) = partial {
                    self.status.update(
                        |status| {
                            status.partial_checkpoint_complete(priority, timestamp);
                        },
                        &mut event.instructions,
                    );
                } else {
                    self.handle_checkpoint_applied(event, timestamp);
                }
            }
            SyncStateMachineTransition::Empty => {}
        };

        false
    }

    /// Handles a single sync line.
    ///
    /// When it returns `Ok(true)`, the sync iteration should be stopped. For errors, the type of
    /// error determines whether the iteration can continue.
    fn handle_line(
        &mut self,
        target: &mut SyncTarget,
        event: &mut ActiveEvent,
        line: &SyncLine,
    ) -> Result<bool, PowerSyncError> {
        let transition = self.prepare_handling_sync_line(target, event, line)?;
        Ok(self.apply_transition(target, event, transition))
    }

    /// Runs a full sync iteration, returning nothing when it completes regularly or an error when
    /// the sync iteration should be interrupted.
    async fn run(mut self) -> Result<(), PowerSyncError> {
        let mut target = SyncTarget::BeforeCheckpoint(self.prepare_request().await?);

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
                SyncEvent::TextLine { data } => serde_json::from_str(data)
                    .map_err(|e| PowerSyncError::sync_protocol_error("invalid text line", e))?,
                SyncEvent::BinaryLine { data } => bson::from_bytes(data)
                    .map_err(|e| PowerSyncError::sync_protocol_error("invalid binary line", e))?,
                SyncEvent::UploadFinished => {
                    if let Some(checkpoint) = self.validated_but_not_applied.take() {
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

                                self.handle_checkpoint_applied(event, self.adapter.now()?);
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

            match self.handle_line(&mut target, event, &line) {
                Ok(end_iteration) => {
                    if end_iteration {
                        break;
                    } else {
                        ()
                    }
                }
                Err(e) if e.can_retry() => {
                    event.recoverable_error = Some(e);
                }
                Err(e) => return Err(e),
            };

            self.status.emit_changes(&mut event.instructions);
        }

        Ok(())
    }

    fn load_progress(
        &self,
        checkpoint: &OwnedCheckpoint,
    ) -> Result<SyncDownloadProgress, PowerSyncError> {
        let SyncProgressFromCheckpoint {
            progress,
            needs_counter_reset,
        } = SyncDownloadProgress::for_checkpoint(checkpoint, &self.adapter)?;

        if needs_counter_reset {
            self.adapter.reset_progress()?;
        }

        Ok(progress)
    }

    /// Prepares a sync iteration by handling the initial [SyncEvent::Initialize].
    ///
    /// This prepares a [StreamingSyncRequest] by fetching local sync state and the requested bucket
    /// parameters.
    async fn prepare_request(&mut self) -> Result<Vec<String>, PowerSyncError> {
        let event = Self::receive_event().await;
        let SyncEvent::Initialize = event.event else {
            return Err(PowerSyncError::argument_error(
                "first event must initialize",
            ));
        };

        let sync_state = self.adapter.collect_sync_state()?;
        self.status.update(
            move |s| s.start_connecting(sync_state),
            &mut event.instructions,
        );

        let requests = self.adapter.collect_bucket_requests()?;
        let local_bucket_names: Vec<String> = requests.iter().map(|s| s.name.clone()).collect();
        let request = StreamingSyncRequest {
            buckets: requests,
            include_checksum: true,
            raw_data: true,
            binary_data: true,
            client_id: client_id(self.db)?,
            parameters: self.options.parameters.take(),
            streams: self
                .adapter
                .collect_subscription_requests(self.options.include_defaults)?,
        };

        event
            .instructions
            .push(Instruction::EstablishSyncStream { request });
        Ok(local_bucket_names)
    }

    fn handle_checkpoint_applied(&mut self, event: &mut ActiveEvent, timestamp: Timestamp) {
        event.instructions.push(Instruction::DidCompleteSync {});

        self.status.update(
            |status| status.applied_checkpoint(timestamp),
            &mut event.instructions,
        );
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

    /// Starts tracking the received `Checkpoint`.
    ///
    /// This returns a set of buckets to delete because they've been tracked locally but not in the
    /// checkpoint, as well as the updated state of the [SyncTarget] to apply after deleting those
    /// buckets.
    ///
    /// The new state is not applied automatically - the old state should be kept in-memory until
    /// the buckets have actually been deleted so that the operation can be retried if deleting
    /// buckets fails.
    fn track_checkpoint<'a>(&self, checkpoint: &Checkpoint<'a>) -> (BTreeSet<String>, Self) {
        let mut to_delete: BTreeSet<String> = match &self {
            SyncTarget::Tracking(checkpoint) => checkpoint.buckets.keys().cloned().collect(),
            SyncTarget::BeforeCheckpoint(buckets) => buckets.iter().cloned().collect(),
        };

        let mut buckets = BTreeMap::<String, OwnedBucketChecksum>::new();
        for bucket in &checkpoint.buckets {
            buckets.insert(bucket.bucket.to_string(), OwnedBucketChecksum::from(bucket));
            to_delete.remove(&*bucket.bucket);
        }

        (
            to_delete,
            SyncTarget::Tracking(OwnedCheckpoint::from_checkpoint(checkpoint, buckets)),
        )
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

/// A transition representing pending changes between [StreamingSyncIteration::prepare_handling_sync_line]
/// and [StreamingSyncIteration::apply_transition].
///
/// This split allows the main logic handling sync lines to take a non-mutable reference to internal
/// client state, guaranteeing that it does not mutate state until changes have been written to the
/// database. Only after those writes have succeeded are the internal state changes applied.
///
/// This split ensures that `powersync_control` calls are idempotent when running into temporary
/// SQLite errors, a property we need for compatibility with e.g. WA-sqlite, where the VFS can
/// return `BUSY` errors and the SQLite library automatically retries running statements.
enum SyncStateMachineTransition<'a> {
    StartTrackingCheckpoint {
        progress: SyncDownloadProgress,
        updated_target: SyncTarget,
    },
    DataLineSaved {
        line: &'a DataLine<'a>,
    },
    SyncLocalFailedDueToPendingCrud {
        validated_but_not_applied: OwnedCheckpoint,
    },
    SyncLocalChangesApplied {
        partial: Option<BucketPriority>,
        timestamp: Timestamp,
    },
    CloseIteration,
    Empty,
}
