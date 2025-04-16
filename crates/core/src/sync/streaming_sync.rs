use core::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, RawWakerVTable, Waker},
};

use alloc::{
    boxed::Box,
    collections::btree_map::BTreeMap,
    string::{String, ToString},
    vec::Vec,
};
use futures_lite::FutureExt;
use serde_json::Map;

use crate::{
    bson,
    bucket_priority::BucketPriority,
    error::SQLiteError,
    kv::client_id,
    util::{sqlite3_mutex, Mutex},
};
use sqlite_nostd::{self as sqlite, Connection, ResultCode};

use super::{
    interface::{BucketRequest, Instruction, StreamingSyncRequest, SyncEvent},
    line::SyncLine,
};

pub struct SyncClient {
    state: Mutex<ClientState>,
}

impl SyncClient {
    pub fn new() -> Self {
        Self {
            state: sqlite3_mutex(ClientState::Idle),
        }
    }

    pub fn push_event<'a>(&self, event: SyncEvent<'a>) -> Vec<Instruction> {
        let mut active = ActiveEvent {
            handled: false,
            event,
            instructions: Vec::new(),
        };

        let mut state = self.state.lock();
        match &mut *state {
            ClientState::Idle => todo!(),
            ClientState::IterationActive(handle) => {
                let done = handle.run(&mut active);
                if done {
                    *state = ClientState::Idle;
                }
            }
        }

        debug_assert!(active.handled);
        active.instructions
    }
}

enum ClientState {
    Idle,
    IterationActive(SyncIterationHandle),
}

struct SyncIterationHandle {
    future: Pin<Box<dyn Future<Output = ()>>>,
}

impl SyncIterationHandle {
    fn run(&mut self, active: &mut ActiveEvent) -> bool {
        // Using a noop waker because the only event thing StreamingSyncIteration::run polls on is
        // the next incoming sync event.
        let waker = unsafe {
            Waker::new(
                active as *const ActiveEvent as *const (),
                Waker::noop().vtable(),
            )
        };
        let mut context = Context::from_waker(Waker::noop());

        let result = self.future.poll(&mut context);
        result.is_ready()
    }
}

struct ActiveEvent<'a> {
    handled: bool,
    event: SyncEvent<'a>,
    instructions: Vec<Instruction>,
}

struct StreamingSyncIteration {
    db: *mut sqlite::sqlite3,
    parameters: Option<serde_json::Map<String, serde_json::Value>>,
}

impl StreamingSyncIteration {
    fn receive_event<'a>(&'a self) -> impl Future<Output = &'a mut ActiveEvent<'a>> {
        struct Wait<'a> {
            a: &'a StreamingSyncIteration,
        }

        impl<'a> Future for Wait<'a> {
            type Output = &'a mut ActiveEvent<'a>;

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                let context = cx.waker().data().cast_mut() as *mut ActiveEvent;
                let mut context = unsafe { &mut *context };

                if context.handled {
                    Poll::Pending
                } else {
                    context.handled = true;
                    Poll::Ready(context)
                }
            }
        }

        Wait { a: self }
    }

    async fn run(mut self) -> Result<(), SQLiteError> {
        let mut target = None::<OwnedCheckpoint>;
        let mut validated = None::<OwnedCheckpoint>;
        let mut applied = None::<OwnedCheckpoint>;

        let mut bucket_map = self.prepare_request().await?;

        loop {
            let event = self.receive_event().await;

            let line: SyncLine = match event.event {
                SyncEvent::StartSyncStream => {
                    panic!("Starting sync stream should have reset iteration")
                }
                SyncEvent::SyncStreamClosed { error: _ } => break,
                SyncEvent::TextLine { data } => serde_json::from_str(data)?,
                SyncEvent::BinaryLine { data } => bson::from_bytes(data)?,
            };

            match line {
                SyncLine::Checkpoint(checkpoint) => todo!(),
                SyncLine::CheckpointDiff(checkpoint_diff) => todo!(),
                SyncLine::CheckpointComplete(checkpoint_complete) => todo!(),
                SyncLine::CheckpointPartiallyComplete(checkpoint_partially_complete) => todo!(),
                SyncLine::Data(data_line) => todo!(),
                SyncLine::KeepAlive(token_expires_in) => todo!(),
            }
        }

        Ok(())
    }

    async fn prepare_request(
        &mut self,
    ) -> Result<BTreeMap<String, Option<BucketDescription>>, SQLiteError> {
        let event = self.receive_event().await;
        assert!(matches!(event.event, SyncEvent::StartSyncStream));

        let (request, bucket_map) = self.collect_local_bucket_state()?;
        event
            .instructions
            .push(Instruction::EstablishSyncStream { request });
        Ok(bucket_map)
    }

    fn collect_local_bucket_state(
        &self,
    ) -> Result<
        (
            StreamingSyncRequest,
            BTreeMap<String, Option<BucketDescription>>,
        ),
        SQLiteError,
    > {
        // language=SQLite
        let statement = self.db.prepare_v2(
            "SELECT name, last_op FROM ps_buckets WHERE pending_delete = 0 AND name != '$local'",
        )?;

        let mut requests = Vec::<BucketRequest>::new();
        let mut local_state = BTreeMap::<String, Option<BucketDescription>>::new();

        while statement.step()? == ResultCode::ROW {
            let bucket_name = statement.column_text(0)?.to_string();
            let last_op = statement.column_int64(1);

            requests.push(BucketRequest {
                name: bucket_name.clone(),
                after: last_op.to_string(),
            });
            local_state.insert(bucket_name, None);
        }

        let request = StreamingSyncRequest {
            buckets: requests,
            include_checksum: true,
            raw_data: true,
            client_id: client_id(self.db)?,
            parameters: self.parameters.clone(),
        };

        Ok((request, local_state))
    }
}

struct OwnedCheckpoint {
    last_op_id: i64,
    write_checkpoint: Option<i64>,
    buckets: Vec<OwnedBucketChecksum>,
}

struct OwnedBucketChecksum {
    pub bucket: String,
    pub checksum: i32,
    pub priority: Option<BucketPriority>,
    pub count: Option<i64>,
    pub last_op_id: Option<i64>,
}

struct BucketDescription {
    priority: BucketPriority,
    name: String,
}
