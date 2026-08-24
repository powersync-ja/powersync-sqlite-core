use core::fmt::Write;

use alloc::{
    borrow::Cow,
    collections::{btree_map::BTreeMap, btree_set::BTreeSet},
    format,
    rc::{Rc, Weak},
    string::{String, ToString},
    vec::Vec,
};

use crate::{
    error::{PowerSyncError, PowerSyncErrorCause, Result},
    kv::client_id,
    state::DatabaseState,
    sync::{
        BucketPriority,
        checkpoint::OwnedBucketChecksum,
        diagnostics::DiagnosticsCollector,
        interface::{
            CheckpointMode, CheckpointRequestPayload, CloseSyncStream, StartSyncStream,
            StreamSubscriptionRequest,
        },
        line::{
            BucketSubscriptionReason, DataLine, StreamDescription, StreamSubscriptionError,
            StreamSubscriptionErrorCause, SyncLineWithSource,
        },
        subscriptions::{LocallyTrackedSubscription, StreamKey},
        sync_status::{ActiveStreamSubscription, TimestampMicros},
    },
    utils::database::Database,
};

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
    db: Database,
    adapter: Rc<StorageAdapter>,
    db_state: Weak<DatabaseState>,
    current_iteration: Option<StreamingSyncIteration>,
}

impl SyncClient {
    pub fn new(db: Database, state: &Rc<DatabaseState>) -> Result<Self> {
        let adapter = state.storage_adapter(db)?;

        Ok(Self {
            db,
            adapter,
            db_state: Rc::downgrade(state),
            current_iteration: None,
        })
    }

    pub fn push_event<'a>(&mut self, event: SyncControlRequest<'a>) -> Result<Vec<Instruction>> {
        match event {
            SyncControlRequest::StartSyncStream(options) => {
                let mut event = ActiveEvent::new(SyncEvent::Initialize);
                let handle = StreamingSyncIteration::create(
                    self.db,
                    options,
                    self.adapter.clone(),
                    self.db_state.clone(),
                    &mut event,
                )?;
                self.current_iteration = Some(handle);

                Ok(event.instructions)
            }
            SyncControlRequest::SyncEvent(sync_event) => {
                let mut active = ActiveEvent::new(sync_event);

                let Some(iteration) = &mut self.current_iteration else {
                    return Err(PowerSyncError::state_error("No iteration is active"));
                };

                let done = iteration.handle_event(&mut active)?;
                if done {
                    self.current_iteration = None;
                }

                Ok(active.instructions)
            }
            SyncControlRequest::StopSyncStream => {
                let mut active = ActiveEvent::new(SyncEvent::TearDown);

                if let Some(mut iteration) = self.current_iteration.take() {
                    iteration.handle_event(&mut active)?;
                }

                Ok(active.instructions)
            }
        }
    }

    /// Whether a sync iteration is currently active on the connection.
    pub fn has_sync_iteration(&self) -> bool {
        self.current_iteration.is_some()
    }
}

/// A [SyncEvent] currently being handled by a [StreamingSyncIteration].
struct ActiveEvent<'a> {
    /// The event to handle
    event: SyncEvent<'a>,
    /// Instructions to forward to the client when the `powersync_control` invocation completes.
    instructions: Vec<Instruction>,
}

impl<'a> ActiveEvent<'a> {
    pub fn new(event: SyncEvent<'a>) -> Self {
        Self {
            event,
            instructions: Vec::new(),
        }
    }
}

struct StreamingSyncIteration {
    state: Weak<DatabaseState>,
    adapter: Rc<StorageAdapter>,
    status: SyncStatusContainer,
    options: StartSyncStream,
    target: SyncTarget,
    // A checkpoint that has been fully received and validated, but couldn't be applied due to
    // pending local data. We will retry applying this checkpoint when the client SDK informs us
    // that it has finished uploading changes.
    validated_but_not_applied: Option<OwnedCheckpoint>,
    diagnostics: Option<DiagnosticsCollector>,
}

impl StreamingSyncIteration {
    fn create(
        db: Database,
        mut options: StartSyncStream,
        adapter: Rc<StorageAdapter>,
        state: Weak<DatabaseState>,
        event: &mut ActiveEvent,
    ) -> Result<Self> {
        let mut status = SyncStatusContainer::new();
        let prepared_request =
            Self::prepare_request(db, &adapter, &mut status, &mut options, event)?;
        let diagnostics = DiagnosticsCollector::for_options(&options);

        Ok(Self {
            state,
            adapter,
            status,
            options,
            target: SyncTarget::BeforeCheckpoint(prepared_request),
            validated_but_not_applied: Default::default(),
            diagnostics: diagnostics,
        })
    }

    /// Starts handling a single sync line without altering any in-memory state of the state
    /// machine.
    ///
    /// After this call succeeds, the returned value can be used to update the state. For a
    /// discussion on why this split is necessary, see [SyncStateMachineTransition].
    fn prepare_handling_sync_line<'a>(
        &self,
        // Note: Only mutable so that resolve_subscription_state can push log events.
        event: &mut ActiveEvent,
        line: SyncLineWithSource<'a>,
    ) -> Result<SyncStateMachineTransition<'a>> {
        let SyncLineWithSource { source, line } = line;

        Ok(match line {
            SyncLine::Checkpoint(checkpoint) => {
                let (to_delete, updated_target) = self.target.track_checkpoint(&checkpoint);

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
                let Some(target) = self.target.target_checkpoint() else {
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
                let Some(checkpoint) = self.target.target_checkpoint() else {
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
                    SyncLocalResult::ChangesApplied { timestamp } => {
                        event.instructions.push(Instruction::LogLine {
                            severity: LogSeverity::DEBUG,
                            line: "Validated and applied checkpoint".into(),
                        });

                        // Persist here so that all database writes happen while preparing the
                        // transition, keeping apply_transition infallible.
                        if let Some(request_id) = target.write_checkpoint {
                            self.adapter
                                .persist_last_applied_checkpoint_request_id(request_id)?;
                        }

                        SyncStateMachineTransition::SyncLocalChangesApplied {
                            applied_checkpoint_request_id: target.write_checkpoint,
                            partial: None,
                            timestamp,
                        }
                    }
                }
            }
            SyncLine::CheckpointPartiallyComplete(complete) => {
                let priority = complete.priority;
                let Some(target) = self.target.target_checkpoint() else {
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
                        SyncStateMachineTransition::EmptyAndConnected
                    }
                    SyncLocalResult::ChangesApplied { timestamp } => {
                        SyncStateMachineTransition::SyncLocalChangesApplied {
                            // A checkpoint request is only considered applied once the full
                            // checkpoint has been applied, not for partial completions.
                            applied_checkpoint_request_id: None,
                            partial: Some(priority),
                            timestamp,
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
                    SyncStateMachineTransition::EmptyAndConnected
                } else {
                    // Periodically check whether any subscriptions that are part of this stream
                    // are expired. We currently do this by re-creating the request and aborting the
                    // iteration if it has changed.
                    let updated_request = self
                        .adapter
                        .collect_subscription_requests(self.options.include_defaults)?;
                    if updated_request.request
                        != self.target.explicit_stream_subscriptions().request
                    {
                        SyncStateMachineTransition::CloseIteration(CloseSyncStream {
                            hide_disconnect: true,
                        })
                    } else {
                        SyncStateMachineTransition::EmptyAndConnected
                    }
                }
            }
            SyncLine::UnknownSyncLine => {
                event.instructions.push(Instruction::LogLine {
                    severity: LogSeverity::DEBUG,
                    line: "Unknown sync line".into(),
                });
                SyncStateMachineTransition::EmptyAndConnected
            }
        })
    }

    /// Applies a sync state transition, returning whether the iteration should be stopped.
    fn apply_transition(
        &mut self,
        event: &mut ActiveEvent,
        transition: SyncStateMachineTransition,
    ) -> bool {
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
                self.target = updated_target;

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
                    diagnostics.handle_data_line(&line, &*status, &mut event.instructions);
                }
            }
            SyncStateMachineTransition::CloseIteration(close) => {
                self.status
                    .update(|s| s.disconnect(), &mut event.instructions);

                event.instructions.push(Instruction::CloseSyncStream(close));
                return true;
            }
            SyncStateMachineTransition::SyncLocalFailedDueToPendingCrud {
                validated_but_not_applied,
            } => {
                self.validated_but_not_applied = Some(validated_but_not_applied);
            }
            SyncStateMachineTransition::SyncLocalChangesApplied {
                applied_checkpoint_request_id,
                partial,
                timestamp,
            } => {
                self.validated_but_not_applied = None;

                if let Some(priority) = partial {
                    self.status.update(
                        |status| {
                            status.partial_checkpoint_complete(priority, timestamp);
                        },
                        &mut event.instructions,
                    );
                } else {
                    self.handle_checkpoint_applied(event, timestamp, applied_checkpoint_request_id);
                }
            }
            SyncStateMachineTransition::ChangeActiveStreams(streams) => {
                self.options.active_streams = streams;
            }
            SyncStateMachineTransition::EmptyAndConnected => {
                self.status
                    .update(|s| s.mark_connected(), &mut event.instructions);
            }
            SyncStateMachineTransition::Empty => {}
        };

        false
    }

    fn prepare_handling_event<'a>(
        &self,
        event: &mut ActiveEvent<'a>,
    ) -> Result<SyncStateMachineTransition<'a>> {
        Ok(match event.event {
            SyncEvent::Initialize { .. } => {
                panic!("Initialize should only be emited once")
            }
            SyncEvent::TearDown | SyncEvent::StreamEnded => {
                SyncStateMachineTransition::CloseIteration(CloseSyncStream {
                    hide_disconnect: false,
                })
            }
            SyncEvent::TextLine { data } => {
                self.prepare_handling_sync_line(event, SyncLineWithSource::from_text(data)?)?
            }
            SyncEvent::BinaryLine { data } => {
                self.prepare_handling_sync_line(event, SyncLineWithSource::from_binary(data)?)?
            }
            SyncEvent::UploadFinished => self.try_applying_write_after_completed_upload(event)?,
            SyncEvent::DidUpdateSubscriptions { ref active_streams } => {
                self.adapter.increase_ttl(&active_streams)?;
                let new_request = self
                    .adapter
                    .collect_subscription_requests(self.options.include_defaults)?;

                if new_request.request != self.target.explicit_stream_subscriptions().request {
                    // This changes stream requests, start another iteration.
                    SyncStateMachineTransition::CloseIteration(CloseSyncStream {
                        hide_disconnect: true,
                    })
                } else {
                    // Stream request unchanged, but update our references so that we don't
                    // extend the expiry date of previous subscriptions.
                    SyncStateMachineTransition::ChangeActiveStreams(Rc::clone(active_streams))
                }
            }
            SyncEvent::ConnectionEstablished => SyncStateMachineTransition::EmptyAndConnected,
            SyncEvent::DidRefreshToken => {
                // Break so that the client SDK starts another iteration.
                SyncStateMachineTransition::CloseIteration(CloseSyncStream {
                    hide_disconnect: true,
                })
            }
        })
    }

    /// Runs a full sync iteration, returning nothing when it completes regularly or an error when
    /// the sync iteration should be interrupted.
    fn handle_event(&mut self, event: &mut ActiveEvent) -> Result<bool> {
        let transition = self.prepare_handling_event(event)?;

        let maybe_close = self.apply_transition(event, transition);
        self.status.emit_changes(&mut event.instructions);

        Ok(maybe_close)
    }

    fn load_progress(&self, checkpoint: &OwnedCheckpoint) -> Result<SyncDownloadProgress> {
        let SyncProgressFromCheckpoint {
            progress,
            needs_counter_reset,
        } = SyncDownloadProgress::for_checkpoint(checkpoint, &self.adapter)?;

        if needs_counter_reset {
            self.adapter.reset_progress()?;
        }

        Ok(progress)
    }

    fn try_applying_write_after_completed_upload<'a>(
        &'_ self,
        event: &mut ActiveEvent<'a>,
    ) -> Result<SyncStateMachineTransition<'a>> {
        let Some(checkpoint) = &self.validated_but_not_applied else {
            return Ok(SyncStateMachineTransition::Empty);
        };

        let target_write = self.adapter.target_checkpoint_request_id()?;
        if checkpoint.write_checkpoint < target_write {
            // Note: None < Some(x). The pending checkpoint does not contain the write
            // checkpoint created during the upload, so we don't have to try applying it, it's
            // guaranteed to be outdated.
            return Ok(SyncStateMachineTransition::Empty);
        }

        let result = self.sync_local(&checkpoint, None)?;
        Ok(match result {
            SyncLocalResult::ChangesApplied { timestamp } => {
                event.instructions.push(Instruction::LogLine {
                    severity: LogSeverity::DEBUG,
                    line: "Applied pending checkpoint after completed upload".into(),
                });

                if let Some(request_id) = checkpoint.write_checkpoint {
                    self.adapter
                        .persist_last_applied_checkpoint_request_id(request_id)?;
                }
                SyncStateMachineTransition::SyncLocalChangesApplied {
                    applied_checkpoint_request_id: checkpoint.write_checkpoint,
                    partial: None,
                    timestamp,
                }
            }
            _ => {
                event.instructions.push(Instruction::LogLine {
                    severity: LogSeverity::WARNING,
                    line: "Could not apply pending checkpoint even after completed upload".into(),
                });

                SyncStateMachineTransition::Empty
            }
        })
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
    ) -> Result<Vec<ActiveStreamSubscription>> {
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
    ) -> Result<SyncLocalResult> {
        let state = match self.state.upgrade() {
            Some(state) => state,
            None => return Err(PowerSyncError::unknown_internal()),
        };

        let result = self
            .adapter
            .sync_local(&*state, target, priority, &self.options.schema)?;

        if let SyncLocalResult::ChangesApplied { timestamp } = result {
            // Update affected stream subscriptions to mark them as synced.
            let mut status = self.status.inner().borrow_mut();

            if !status.streams.is_empty() {
                let stmt = self.adapter.db.prepare_v2(
                    "UPDATE ps_stream_subscriptions SET last_synced_at = ?2 WHERE id = ?1",
                )?;

                for stream in &mut status.streams {
                    if stream.is_in_priority(priority) {
                        stmt.bind_int64(1, stream.id)?;
                        stmt.bind_int64(2, timestamp.0)?;
                        stream.last_synced_at = Some(timestamp);
                        stmt.exec()?;
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
    fn prepare_request(
        db: Database,
        adapter: &StorageAdapter,
        status: &mut SyncStatusContainer,
        options: &mut StartSyncStream,
        event: &mut ActiveEvent,
    ) -> Result<BeforeCheckpoint> {
        let SyncEvent::Initialize = event.event else {
            return Err(PowerSyncError::argument_error(
                "first event must initialize",
            ));
        };

        let offline_state = adapter.offline_sync_state()?;
        status.update(
            move |s| {
                *s = offline_state;
                s.start_connecting();
            },
            &mut event.instructions,
        );

        let requests = adapter.collect_bucket_requests()?;
        let local_bucket_names: Vec<String> = requests.iter().map(|s| s.name.clone()).collect();
        adapter.increase_ttl(&options.active_streams)?;
        let stream_subscriptions =
            adapter.collect_subscription_requests(options.include_defaults)?;

        let client_id = client_id(db)?;
        let checkpoint_request = if options.checkpoint_mode == CheckpointMode::Requests {
            Some(CheckpointRequestPayload {
                client_id: client_id.clone(),
                checkpoint_request_id: adapter.initial_checkpoint_request_id()?.to_string(),
            })
        } else {
            None
        };

        let request = StreamingSyncRequest {
            buckets: requests,
            include_checksum: true,
            raw_data: true,
            // Clients are not supposed to set this field, but old versions of the PowerSync service
            // will break if it's not set and the SDK requests sync data as BSON.
            // For details, see https://github.com/powersync-ja/powersync-service/pull/332
            binary_data: true,
            client_id,
            parameters: options.parameters.take(),
            streams: stream_subscriptions.request.clone(),
            app_metadata: options.app_metadata.take(),
        };

        event.instructions.push(Instruction::EstablishSyncStream {
            request,
            checkpoint_request,
        });
        Ok(BeforeCheckpoint {
            local_buckets: local_bucket_names,
            stream_subscriptions: stream_subscriptions,
        })
    }

    /// Emits the instructions and status update for a fully applied checkpoint.
    ///
    /// The applied checkpoint request id must already have been persisted by the caller: this
    /// runs while applying a state transition, which must stay infallible (see
    /// [SyncStateMachineTransition]).
    fn handle_checkpoint_applied(
        &mut self,
        event: &mut ActiveEvent,
        timestamp: TimestampMicros,
        applied_checkpoint_request_id: Option<i64>,
    ) {
        if let Some(request_id) = applied_checkpoint_request_id {
            event.instructions.push(Instruction::LogLine {
                severity: LogSeverity::DEBUG,
                line: format!("Applied checkpoint request id {request_id}").into(),
            });
        }

        event.instructions.push(Instruction::DidCompleteSync {
            applied_checkpoint_request_id,
        });

        self.status.update(
            |status| status.applied_checkpoint(timestamp, applied_checkpoint_request_id),
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
        line: DataLine<'a>,
    },
    SyncLocalFailedDueToPendingCrud {
        validated_but_not_applied: OwnedCheckpoint,
    },
    SyncLocalChangesApplied {
        applied_checkpoint_request_id: Option<i64>,
        partial: Option<BucketPriority>,
        timestamp: TimestampMicros,
    },
    CloseIteration(CloseSyncStream),
    ChangeActiveStreams(Rc<Vec<StreamKey>>),
    EmptyAndConnected,
    Empty,
}
