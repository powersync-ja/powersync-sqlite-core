use alloc::{string::String, vec::Vec};
use serde::Serialize;

pub enum SyncEvent<'a> {
    StartSyncStream,
    SyncStreamClosed { error: bool },
    TextLine { data: &'a str },
    BinaryLine { data: &'a [u8] },
}

/// An instruction sent by the core extension to the SDK.
#[derive(Serialize)]
pub enum Instruction {
    LogLine { severity: LogSeverity, line: String },
    EstablishSyncStream { request: StreamingSyncRequest },
    CloseSyncStream,
}

#[derive(Serialize)]
pub enum LogSeverity {
    DEBUG,
    INFO,
    WARNING,
}

#[derive(Serialize)]
pub struct StreamingSyncRequest {
    pub buckets: Vec<BucketRequest>,
    pub include_checksum: bool,
    pub raw_data: bool,
    pub client_id: String,
    pub parameters: Option<serde_json::Map<String, serde_json::Value>>,
}

#[derive(Serialize)]
pub struct BucketRequest {
    pub name: String,
    pub after: String,
}
