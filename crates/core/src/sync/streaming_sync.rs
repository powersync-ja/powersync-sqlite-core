use core::{
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use alloc::{
    boxed::Box,
    collections::{btree_map::BTreeMap, btree_set::BTreeSet},
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
use sqlite_nostd::{self as sqlite, Connection, ResultCode};

use super::{
    bucket_priority::BucketPriority,
    interface::{Instruction, StreamingSyncRequest, SyncControlRequest, SyncEvent},
    line::{BucketChecksum, Checkpoint, SyncLine},
    operations::insert_bucket_operations,
    storage_adapter::{BucketDescription, StorageAdapter},
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

                let done = handle.run(&mut active)?;
                if done {
                    *state = ClientState::Idle;
                }

                Ok(active.instructions)
            }
            SyncControlRequest::StopSyncStream => {
                state.tear_down()?;
                Ok(Vec::new())
            }
        }
    }
}

enum ClientState {
    Idle,
    IterationActive(SyncIterationHandle),
}

impl ClientState {
    fn tear_down(&mut self) -> Result<(), SQLiteError> {
        if let ClientState::IterationActive(old) = self {
            old.tear_down()?;
        };

        *self = ClientState::Idle;
        Ok(())
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

    fn tear_down(&mut self) -> Result<(), SQLiteError> {
        self.run(&mut ActiveEvent::new(SyncEvent::TearDown))?;
        Ok(())
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
        let mut target = None::<OwnedCheckpoint>;
        let mut validated = None::<OwnedCheckpoint>;
        let mut applied = None::<OwnedCheckpoint>;

        let mut bucket_map = self.prepare_request().await?;

        loop {
            let event = Self::receive_event().await;

            let line: SyncLine = match event.event {
                SyncEvent::Initialize { .. } => {
                    panic!("Initialize should only be emited once")
                }
                SyncEvent::TearDown => break,
                SyncEvent::TextLine { data } => serde_json::from_str(data)?,
                SyncEvent::BinaryLine { data } => bson::from_bytes(data)?,
            };

            match line {
                SyncLine::Checkpoint(checkpoint) => {
                    let new_target = OwnedCheckpoint::from(&checkpoint);

                    let mut to_delete: BTreeSet<&str> =
                        bucket_map.keys().map(|s| s.as_str()).collect();
                    let mut new_buckets = BTreeMap::<String, Option<BucketDescription>>::new();
                    for bucket in &new_target.buckets {
                        new_buckets.insert(
                            bucket.bucket.clone(),
                            Some(BucketDescription {
                                priority: bucket.priority.unwrap_or(BucketPriority::FALLBACK),
                                name: bucket.bucket.clone(),
                            }),
                        );
                        to_delete.remove(bucket.bucket.as_str());
                    }

                    self.adapter.delete_buckets(to_delete)?;
                    let progress = self.load_progress(&new_target)?;
                    self.status.update(
                        |s| s.start_tracking_checkpoint(progress),
                        &mut event.instructions,
                    );

                    bucket_map = new_buckets;
                    target = Some(new_target);
                }
                SyncLine::CheckpointDiff(checkpoint_diff) => todo!(),
                SyncLine::CheckpointComplete(checkpoint_complete) => todo!(),
                SyncLine::CheckpointPartiallyComplete(checkpoint_partially_complete) => todo!(),
                SyncLine::Data(data_line) => {
                    self.status
                        .update(|s| s.track_line(&data_line), &mut event.instructions);
                    insert_bucket_operations(&self.adapter, &data_line)?;
                }
                SyncLine::KeepAlive(token_expires_in) => todo!(),
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

    async fn prepare_request(
        &mut self,
    ) -> Result<BTreeMap<String, Option<BucketDescription>>, SQLiteError> {
        let event = Self::receive_event().await;
        let SyncEvent::Initialize = event.event else {
            return Err(SQLiteError::from(ResultCode::MISUSE));
        };

        self.status
            .update(|s| s.start_connecting(), &mut event.instructions);

        let (requests, bucket_map) = self.adapter.collect_local_bucket_state()?;
        let request = StreamingSyncRequest {
            buckets: requests,
            include_checksum: true,
            raw_data: true,
            client_id: client_id(self.db)?,
            parameters: self.parameters.take(),
        };

        event
            .instructions
            .push(Instruction::EstablishSyncStream { request });
        Ok(bucket_map)
    }
}

pub struct OwnedCheckpoint {
    pub last_op_id: i64,
    pub write_checkpoint: Option<i64>,
    pub buckets: Vec<OwnedBucketChecksum>,
}

impl From<&'_ Checkpoint<'_>> for OwnedCheckpoint {
    fn from(value: &'_ Checkpoint<'_>) -> Self {
        Self {
            last_op_id: value.last_op_id,
            write_checkpoint: value.write_checkpoint,
            buckets: value
                .buckets
                .iter()
                .map(OwnedBucketChecksum::from)
                .collect(),
        }
    }
}

pub struct OwnedBucketChecksum {
    pub bucket: String,
    pub checksum: i32,
    pub priority: Option<BucketPriority>,
    pub count: Option<i64>,
    pub last_op_id: Option<i64>,
}

impl From<&'_ BucketChecksum<'_>> for OwnedBucketChecksum {
    fn from(value: &'_ BucketChecksum<'_>) -> Self {
        Self {
            bucket: value.bucket.to_string(),
            checksum: value.checksum,
            priority: value.priority,
            count: value.count,
            last_op_id: value.last_op_id,
        }
    }
}
