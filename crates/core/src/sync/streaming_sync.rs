use core::{
    fmt::Write,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use alloc::{
    borrow::Cow,
    boxed::Box,
    collections::{btree_map::BTreeMap, btree_set::BTreeSet},
    format,
    rc::{Rc, Weak},
    string::{String, ToString},
    vec::Vec,
};
use futures_lite::FutureExt;

use crate::{
    error::{PowerSyncError, PowerSyncErrorCause},
    kv::client_id,
    state::DatabaseState,
    sync::{
        BucketPriority,
        checkpoint::OwnedBucketChecksum,
        diagnostics::DiagnosticsCollector,
        interface::{CloseSyncStream, StartSyncStream, StreamSubscriptionRequest},
        line::{
            BucketSubscriptionReason, DataLine, StreamDescription, StreamSubscriptionError,
            StreamSubscriptionErrorCause, SyncLineWithSource,
        },
        subscriptions::LocallyTrackedSubscription,
        sync_status::{ActiveStreamSubscription, Timestamp},
    },
};
use powersync_sqlite_nostd::{self as sqlite, Connection, ResultCode};

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
    db_state: Weak<DatabaseState>,
    /// The current [ClientState] (essentially an optional [StreamingSyncIteration]).
    state: ClientState,
}

impl SyncClient {
    pub fn new(db: *mut sqlite::sqlite3, state: &Rc<DatabaseState>) -> Self {
        Self {
            db,
            db_state: Rc::downgrade(state),
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

    /// Whether a sync iteration is currently active on the connection.
    pub fn has_sync_iteration(&self) -> bool {
        matches!(self.state, ClientState::IterationActive(_))
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
    future: Pin<Box<dyn Future<Output = Result<CloseSyncStream, PowerSyncError>>>>,
}

impl SyncIterationHandle {
    /// Creates a new sync iteration in a pending state by preparing statements for
    /// [StorageAdapter] and setting up the initial downloading state for [StorageAdapter] .
    fn new(
        db: *mut sqlite::sqlite3,
        options: StartSyncStream,
        state: Weak<DatabaseState>,
    ) -> Result<Self, PowerSyncError> {
        let runner = StreamingSyncIteration {
            db,
            validated_but_not_applied: None,
            diagnostics: DiagnosticsCollector::for_options(&options),
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
                let close = result?;

                active
                    .instructions
                    .push(Instruction::CloseSyncStream(close));
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
    state: Weak<DatabaseState>,
    adapter: StorageAdapter,
    options: StartSyncStream,
    status: SyncStatusContainer,
    // A checkpoint that has been fully received and validated, but couldn't be applied due to
    // pending local data. We will retry applying this checkpoint when the client SDK informs us
    // that it has finished uploading changes.
    validated_but_not_applied: Option<OwnedCheckpoint>,
    diagnostics: Option<DiagnosticsCollector>,
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
        line: &'a SyncLineWithSource<'a>,
    ) -> Result<SyncStateMachineTransition<'a>, PowerSyncError> {
        let SyncLineWithSource { source, line } = line;

        Ok(match line {
            SyncLine::Checkpoint(checkpoint) => {
                let (to_delete, updated_target) = target.track_checkpoint(&checkpoint);

                self.adapter
                    .delete_buckets(to_delete.iter().map(|b| b.as_str()))?;
                let target = updated_target.target_checkpoint().unwrap();
                let progress = self.load_progress(&target.checkpoint)?;
                SyncStateMachineTransition::StartTrackingCheckpoint {
                    progress,
                    subscription_state: self.resolve_subscription_state(&target, event)?,
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

                let mut target = (*target).clone();
                target.apply_diff(&diff);
                self.adapter
                    .delete_buckets(diff.removed_buckets.iter().map(|i| &**i))?;

                let progress = self.load_progress(&target.checkpoint)?;
                SyncStateMachineTransition::StartTrackingCheckpoint {
                    progress,
                    subscription_state: self.resolve_subscription_state(&target, event)?,
                    updated_target: SyncTarget::Tracking(target),
                }
            }
            SyncLine::CheckpointComplete(_) => {
                let Some(checkpoint) = target.target_checkpoint() else {
                    return Err(PowerSyncError::sync_protocol_error(
                        "Received checkpoint complete without previous checkpoint",
                        PowerSyncErrorCause::Unknown,
                    ));
                };
                let target = &checkpoint.checkpoint;
                let result = self.sync_local(target, None)?;

                match result {
                    SyncLocalResult::ChecksumFailure(checkpoint_result) => {
                        // This means checksums failed. Start again with a new checkpoint.
                        // TODO: better back-off
                        // await new Promise((resolve) => setTimeout(resolve, 50));
                        event.instructions.push(Instruction::LogLine {
                            severity: LogSeverity::WARNING,
                            line: format!("Could not apply checkpoint, {checkpoint_result}").into(),
                        });
                        SyncStateMachineTransition::CloseIteration(Default::default())
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
                let result = self.sync_local(&target.checkpoint, Some(priority))?;

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
                        SyncStateMachineTransition::CloseIteration(Default::default())
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
                insert_bucket_operations(&self.adapter, &data_line, source.len())?;
                SyncStateMachineTransition::DataLineSaved { line: data_line }
            }
            SyncLine::KeepAlive(token) => {
                self.adapter.increase_ttl(&self.options.active_streams)?;

                if token.is_expired() {
                    // Token expired already - stop the connection immediately.
                    event
                        .instructions
                        .push(Instruction::FetchCredentials { did_expire: true });

                    SyncStateMachineTransition::CloseIteration(Default::default())
                } else if token.should_prefetch() {
                    event
                        .instructions
                        .push(Instruction::FetchCredentials { did_expire: false });
                    SyncStateMachineTransition::Empty
                } else {
                    // Periodically check whether any subscriptions that are part of this stream
                    // are expired. We currently do this by re-creating the request and aborting the
                    // iteration if it has changed.
                    let updated_request = self
                        .adapter
                        .collect_subscription_requests(self.options.include_defaults)?;
                    if updated_request.request != target.explicit_stream_subscriptions().request {
                        SyncStateMachineTransition::CloseIteration(CloseSyncStream {
                            hide_disconnect: true,
                        })
                    } else {
                        SyncStateMachineTransition::Empty
                    }
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
    ) -> Option<CloseSyncStream> {
        match transition {
            SyncStateMachineTransition::StartTrackingCheckpoint {
                progress,
                updated_target,
                subscription_state,
            } => {
                self.status.update(
                    |s| s.start_tracking_checkpoint(progress, subscription_state),
                    &mut event.instructions,
                );

                // Technically, we could still try to apply a pending checkpoint after receiving a
                // new one. However, sync_local assumes it's only called in a state where there's no
                // pending checkpoint, so we'd have to take the oplog state at the time we've
                // originally received the validated-but-not-applied checkpoint. This is likely not
                // something worth doing.
                self.validated_but_not_applied = None;
                *target = updated_target;

                if let Some(diagnostics) = &self.diagnostics {
                    let status = self.status.inner().borrow();
                    diagnostics.handle_tracking_checkpoint(&*status, &mut event.instructions);
                }
            }
            SyncStateMachineTransition::DataLineSaved { line } => {
                self.status
                    .update(|s| s.track_line(&line), &mut event.instructions);

                if let Some(diagnostics) = &mut self.diagnostics {
                    let status = self.status.inner().borrow();
                    diagnostics.handle_data_line(line, &*status, &mut event.instructions);
                }
            }
            SyncStateMachineTransition::CloseIteration(close) => return Some(close),
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

        None
    }

    /// Handles a single sync line.
    ///
    /// When it returns `Ok(true)`, the sync iteration should be stopped. For errors, the type of
    /// error determines whether the iteration can continue.
    fn handle_line(
        &mut self,
        target: &mut SyncTarget,
        event: &mut ActiveEvent,
        line: &SyncLineWithSource,
    ) -> Result<Option<CloseSyncStream>, PowerSyncError> {
        let transition = self.prepare_handling_sync_line(target, event, line)?;
        Ok(self.apply_transition(target, event, transition))
    }

    /// Runs a full sync iteration, returning nothing when it completes regularly or an error when
    /// the sync iteration should be interrupted.
    async fn run(mut self) -> Result<CloseSyncStream, PowerSyncError> {
        let mut target = SyncTarget::BeforeCheckpoint(self.prepare_request().await?);

        let hide_disconnect = loop {
            let event = Self::receive_event().await;

            let line: SyncLineWithSource = match event.event {
                SyncEvent::Initialize { .. } => {
                    panic!("Initialize should only be emited once")
                }
                SyncEvent::TearDown => {
                    self.status
                        .update(|s| s.disconnect(), &mut event.instructions);
                    break false;
                }
                SyncEvent::TextLine { data } => SyncLineWithSource::from_text(data)?,
                SyncEvent::BinaryLine { data } => SyncLineWithSource::from_binary(data)?,
                SyncEvent::UploadFinished => {
                    self.try_applying_write_after_completed_upload(event)?;

                    continue;
                }
                SyncEvent::DidUpdateSubscriptions { ref active_streams } => {
                    self.adapter.increase_ttl(&active_streams)?;
                    let new_request = self
                        .adapter
                        .collect_subscription_requests(self.options.include_defaults)?;

                    if new_request.request != target.explicit_stream_subscriptions().request {
                        // This changes stream requests, start another iteration.
                        break true;
                    } else {
                        // Stream request unchanged, but update our references so that we don't
                        // extend the expiry date of previous subscriptions.
                        self.options.active_streams = Rc::clone(active_streams);
                        continue;
                    }
                }
                SyncEvent::ConnectionEstablished => {
                    self.status
                        .update(|s| s.mark_connected(), &mut event.instructions);
                    continue;
                }
                SyncEvent::StreamEnded => {
                    break false;
                }
                SyncEvent::DidRefreshToken => {
                    // Break so that the client SDK starts another iteration.
                    break true;
                }
            };

            self.status.update_only(|s| s.mark_connected());

            match self.handle_line(&mut target, event, &line) {
                Ok(end_iteration) => {
                    if let Some(options) = end_iteration {
                        break options.hide_disconnect;
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
        };

        Ok(CloseSyncStream { hide_disconnect })
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

    fn try_applying_write_after_completed_upload(
        &mut self,
        event: &mut ActiveEvent,
    ) -> Result<(), PowerSyncError> {
        let Some(checkpoint) = self.validated_but_not_applied.take() else {
            return Ok(());
        };

        let target_write = self.adapter.local_state()?.map(|e| e.target_op);
        if checkpoint.write_checkpoint < target_write {
            // Note: None < Some(x). The pending checkpoint does not contain the write
            // checkpoint created during the upload, so we don't have to try applying it, it's
            // guaranteed to be outdated.
            return Ok(());
        }

        let result = self.sync_local(&checkpoint, None)?;
        match result {
            SyncLocalResult::ChangesApplied => {
                event.instructions.push(Instruction::LogLine {
                    severity: LogSeverity::DEBUG,
                    line: "Applied pending checkpoint after completed upload".into(),
                });

                self.handle_checkpoint_applied(event, self.adapter.now()?);
            }
            _ => {
                event.instructions.push(Instruction::LogLine {
                    severity: LogSeverity::WARNING,
                    line: "Could not apply pending checkpoint even after completed upload".into(),
                });
            }
        }

        Ok(())
    }

    /// Reconciles local stream subscriptions with service-side state received in a checkpoint.
    ///
    /// This involves:
    ///
    ///  1. Marking local streams that don't exist in the checkpoint as inactive or deleting them.
    ///  2. Creating new subscriptions for auto-subscribed streams we weren't tracking before.
    ///  3. Associating buckets in the checkpoint with the stream subscriptions that created them.
    ///  4. Reporting errors for stream subscriptions that are marked as errorenous in the
    ///     checkpoint.
    fn resolve_subscription_state(
        &self,
        tracked: &TrackedCheckpoint,
        event: &mut ActiveEvent,
    ) -> Result<Vec<ActiveStreamSubscription>, PowerSyncError> {
        struct LocalAndServerSubscription<'a, T> {
            local: T,
            /// If this subscription has an acknowledged stream included in the checkpoint, the
            /// index of that stream in [Checkpoint::streams] and the corresponding description.
            server: Option<(usize, &'a OwnedStreamDescription)>,
        }

        let mut tracked_subscriptions: Vec<LocalAndServerSubscription<LocallyTrackedSubscription>> =
            Vec::new();

        // Load known subscriptions from database
        self.adapter.iterate_local_subscriptions(|mut sub| {
            // We will mark it as active again if it's part of the streams included in the
            // checkpoint.
            sub.active = false;
            sub.is_default = false;

            tracked_subscriptions.push(LocalAndServerSubscription {
                local: sub,
                server: None,
            });
        })?;

        for (server_index, subscription) in tracked.streams.iter().enumerate() {
            let matching_local_subscriptions = tracked_subscriptions
                .iter_mut()
                .filter(|s| s.local.stream_name == subscription.name);

            let mut has_local = false;
            for local in matching_local_subscriptions {
                local.server = Some((server_index, subscription));
                local.local.active = true;
                local.local.is_default = subscription.is_default;
                has_local = true;
            }

            for error in &*subscription.errors {
                match error.subscription {
                    StreamSubscriptionErrorCause::Default => {
                        event.instructions.push(Instruction::LogLine {
                            severity: LogSeverity::WARNING,
                            line: Cow::Owned(format!(
                                "Default subscription {} has errors: {}",
                                subscription.name, error.message
                            )),
                        });
                    }
                    StreamSubscriptionErrorCause::ExplicitSubscription(index) => {
                        let Some(local_id_for_error) =
                            tracked.requested_subscriptions.subscription_ids.get(index)
                        else {
                            continue;
                        };

                        // Find the matching explicit subscription to contextualize this error
                        // message with the name of the stream and parameters used for the
                        // subscription.
                        for local in &tracked_subscriptions {
                            if *local_id_for_error == local.local.id {
                                let mut desc = String::new();
                                let _ = write!(
                                    &mut desc,
                                    "Subscription to stream {} ",
                                    local.local.stream_name
                                );
                                if let Some(params) = &local.local.local_params {
                                    let _ = write!(&mut desc, "(with parameters {params})");
                                } else {
                                    desc.push_str("(without parameters)");
                                }

                                let _ =
                                    write!(&mut desc, " could not be resolved: {}", error.message);
                                event.instructions.push(Instruction::LogLine {
                                    severity: LogSeverity::WARNING,
                                    line: Cow::Owned(desc),
                                });
                            }
                        }
                    }
                };
            }

            // If they don't exist already, create default subscriptions included in checkpoint
            if !has_local && subscription.is_default {
                let local = self.adapter.create_default_subscription(subscription)?;
                tracked_subscriptions.push(LocalAndServerSubscription {
                    local,
                    server: Some((server_index, subscription)),
                });
            }
        }

        // Clean up subscriptions that are no longer active and haven't been requested explicitly.
        for subscription in &tracked_subscriptions {
            if !subscription.local.has_subscribed_manually() && subscription.server.is_none() {
                self.adapter.delete_subscription(subscription.local.id)?;
            } else {
                self.adapter.update_subscription(&subscription.local)?;
            }
        }
        tracked_subscriptions.retain(|subscription| {
            subscription.local.has_subscribed_manually() || subscription.server.is_some()
        });

        let mut resolved: Vec<ActiveStreamSubscription> =
            Vec::with_capacity(tracked_subscriptions.len());
        // Contains (index in Checkpoint::streams, index in resolved) pairs for default streams.
        let mut default_stream_index: Vec<(usize, usize)> = Vec::new();

        for (i, subscription) in tracked_subscriptions.iter().enumerate() {
            resolved.push(ActiveStreamSubscription::from_local(&subscription.local));

            if let Some((server_index, server)) = subscription.server {
                if server.is_default && !subscription.local.has_subscribed_manually() {
                    let pair = (server_index, i);
                    match default_stream_index.binary_search_by_key(&server_index, |p| p.0) {
                        Ok(_) => {
                            debug_assert!(
                                false,
                                "Looks like we have more than one local subscription for one default server-side subscription."
                            )
                        }
                        Err(index) => default_stream_index.insert(index, pair),
                    }
                }
            }
        }

        debug_assert!(tracked_subscriptions.is_sorted_by_key(|s| s.local.id));

        // Iterate over buckets to associate them with subscriptions
        for bucket in tracked.checkpoint.buckets.values() {
            for reason in &*bucket.subscriptions {
                let subscription_index = match reason {
                    BucketSubscriptionReason::DerivedFromDefaultStream(stream_index) => {
                        default_stream_index
                            .binary_search_by_key(stream_index, |s| s.0)
                            .ok()
                            .map(|idx| default_stream_index[idx].1)
                    }
                    BucketSubscriptionReason::DerivedFromExplicitSubscription(index) => {
                        let subscription_id =
                            tracked.requested_subscriptions.subscription_ids.get(*index);

                        if let Some(subscription_id) = subscription_id {
                            tracked_subscriptions
                                .binary_search_by_key(subscription_id, |s| s.local.id)
                                .ok()
                        } else {
                            None
                        }
                    }
                };

                if let Some(index) = subscription_index {
                    resolved[index].mark_associated_with_bucket(&bucket);
                }
            }
        }

        Ok(resolved)
    }

    /// Performs a partial or a complete local sync.
    fn sync_local(
        &self,
        target: &OwnedCheckpoint,
        priority: Option<BucketPriority>,
    ) -> Result<SyncLocalResult, PowerSyncError> {
        let state = match self.state.upgrade() {
            Some(state) => state,
            None => return Err(PowerSyncError::unknown_internal()),
        };

        let result = self
            .adapter
            .sync_local(&*state, target, priority, &self.options.schema)?;

        if matches!(&result, SyncLocalResult::ChangesApplied) {
            // Update affected stream subscriptions to mark them as synced.
            let mut status = self.status.inner().borrow_mut();

            if !status.streams.is_empty() {
                let stmt = self.adapter.db.prepare_v2(
                    "UPDATE ps_stream_subscriptions SET last_synced_at = unixepoch() WHERE id = ? RETURNING last_synced_at",
                )?;

                for stream in &mut status.streams {
                    if stream.is_in_priority(priority) {
                        stmt.bind_int64(1, stream.id)?;
                        if stmt.step()? == ResultCode::ROW {
                            let timestamp = Timestamp(stmt.column_int64(0));
                            stream.last_synced_at = Some(timestamp);
                        }

                        stmt.reset()?;
                    }
                }
            }
        }

        Ok(result)
    }

    /// Prepares a sync iteration by handling the initial [SyncEvent::Initialize].
    ///
    /// This prepares a [StreamingSyncRequest] by fetching local sync state and the requested bucket
    /// parameters.
    ///
    /// This returns local bucket names (used to delete buckets that don't appear in checkpoints
    /// anymore) and the [LocallyTrackedSubscription::id] of explicitly-requested stream
    /// subscriptions, used to associate [BucketSubscriptionReason::DerivedFromExplicitSubscription].
    async fn prepare_request(&mut self) -> Result<BeforeCheckpoint, PowerSyncError> {
        let event = Self::receive_event().await;
        let SyncEvent::Initialize = event.event else {
            return Err(PowerSyncError::argument_error(
                "first event must initialize",
            ));
        };

        let offline_state = self.adapter.offline_sync_state()?;
        self.status.update(
            move |s| {
                *s = offline_state;
                s.start_connecting();
            },
            &mut event.instructions,
        );

        let requests = self.adapter.collect_bucket_requests()?;
        let local_bucket_names: Vec<String> = requests.iter().map(|s| s.name.clone()).collect();
        self.adapter.increase_ttl(&self.options.active_streams)?;
        let stream_subscriptions = self
            .adapter
            .collect_subscription_requests(self.options.include_defaults)?;

        let request = StreamingSyncRequest {
            buckets: requests,
            include_checksum: true,
            raw_data: true,
            // Clients are not supposed to set this field, but old versions of the PowerSync service
            // will break if it's not set and the SDK requests sync data as BSON.
            // For details, see https://github.com/powersync-ja/powersync-service/pull/332
            binary_data: true,
            client_id: client_id(self.db)?,
            parameters: self.options.parameters.take(),
            streams: stream_subscriptions.request.clone(),
            app_metadata: self.options.app_metadata.take(),
        };

        event
            .instructions
            .push(Instruction::EstablishSyncStream { request });
        Ok(BeforeCheckpoint {
            local_buckets: local_bucket_names,
            stream_subscriptions: stream_subscriptions,
        })
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
    Tracking(TrackedCheckpoint),
    /// We have not received a checkpoint message yet. We still keep a list of local buckets around
    /// so that we know which ones to delete depending on the first checkpoint message.
    BeforeCheckpoint(BeforeCheckpoint),
}

#[derive(Debug)]
struct BeforeCheckpoint {
    /// Local bucket names, kept so that we can delete outdated ones when we receive the first
    /// checkpoint.
    local_buckets: Vec<String>,
    stream_subscriptions: RequestedStreamSubscriptions,
}

impl SyncTarget {
    fn target_checkpoint(&self) -> Option<&TrackedCheckpoint> {
        match self {
            Self::Tracking(tracked) => Some(tracked),
            _ => None,
        }
    }

    fn explicit_stream_subscriptions(&self) -> &RequestedStreamSubscriptions {
        match self {
            SyncTarget::Tracking(tracking) => &tracking.requested_subscriptions,
            SyncTarget::BeforeCheckpoint(before) => &before.stream_subscriptions,
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
            SyncTarget::Tracking(tracked) => tracked.checkpoint.buckets.keys().cloned().collect(),
            SyncTarget::BeforeCheckpoint(before) => before.local_buckets.iter().cloned().collect(),
        };

        let mut buckets = BTreeMap::<String, OwnedBucketChecksum>::new();
        for bucket in &checkpoint.buckets {
            buckets.insert(bucket.bucket.to_string(), OwnedBucketChecksum::from(bucket));
            to_delete.remove(&*bucket.bucket);
        }

        (
            to_delete,
            SyncTarget::Tracking(TrackedCheckpoint {
                checkpoint: OwnedCheckpoint::from_checkpoint(checkpoint, buckets),
                streams: checkpoint
                    .streams
                    .iter()
                    .map(OwnedStreamDescription::from_definition)
                    .collect(),
                requested_subscriptions: self.explicit_stream_subscriptions().clone(),
            }),
        )
    }
}

#[derive(Clone, Debug)]
pub struct RequestedStreamSubscriptions {
    pub request: Rc<StreamSubscriptionRequest>,
    /// Local stream subscription ids ([LocallyTrackedSubscription::id]), in order in which they
    /// appear in the [StreamSubscriptionRequest]. This is used to associate buckets, which
    /// reference an index into this vector ([BucketSubscriptionReason::DerivedFromExplicitSubscription]),
    /// with the local subscription.
    pub subscription_ids: Rc<Vec<i64>>,
}

/// Information about the currently-tracked checkpoint of the sync client.
///
/// This struct is initially created from the first [Checkpoint] line and then patched as we receive
/// [CheckpointDiff] lines afterwards.
#[derive(Debug, Clone)]
pub struct TrackedCheckpoint {
    pub checkpoint: OwnedCheckpoint,
    /// Streams included in the checkpoint
    pub streams: Vec<OwnedStreamDescription>,
    pub requested_subscriptions: RequestedStreamSubscriptions,
}

impl TrackedCheckpoint {
    fn apply_diff<'a>(&mut self, diff: &CheckpointDiff<'a>) {
        self.checkpoint.apply_diff(diff);
        // stream definitions are never changed by a checkpoint_diff line
    }
}

#[derive(Debug, Clone)]
pub struct OwnedStreamDescription {
    pub name: String,
    pub is_default: bool,
    pub errors: Rc<Vec<StreamSubscriptionError>>,
}

impl OwnedStreamDescription {
    pub fn from_definition<'a>(definition: &StreamDescription<'a>) -> Self {
        Self {
            name: definition.name.clone().into_owned(),
            is_default: definition.is_default,
            errors: Rc::clone(&definition.errors),
        }
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
        subscription_state: Vec<ActiveStreamSubscription>,
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
    CloseIteration(CloseSyncStream),
    Empty,
}
