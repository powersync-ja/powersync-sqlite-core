use alloc::string::String;
use alloc::vec::Vec;
use serde::{de, Deserialize, Deserializer, Serialize};
use serde_json as json;

use crate::util::{deserialize_string_to_i64, deserialize_optional_string_to_i64};
use alloc::format;
use alloc::string::{ToString};
use core::fmt;
use serde::de::{MapAccess, Visitor};

use sqlite_nostd as sqlite;
use sqlite_nostd::{Connection, ResultCode};
use uuid::Uuid;
use crate::error::{SQLiteError, PSResult};

use crate::ext::SafeManagedStmt;

#[derive(Serialize, Deserialize, Debug)]
pub struct Checkpoint {
    #[serde(deserialize_with = "deserialize_string_to_i64")]
    pub last_op_id: i64,
    #[serde(default)]
    #[serde(deserialize_with = "deserialize_optional_string_to_i64")]
    pub write_checkpoint: Option<i64>,
    pub buckets: Vec<BucketChecksum>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BucketChecksum {
    pub bucket: String,
    pub checksum: i32,
}


#[derive(Serialize, Deserialize, Debug)]
pub struct CheckpointComplete {
    #[serde(deserialize_with = "deserialize_string_to_i64")]
    last_op_id: i64
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SyncBucketData {
    // TODO: complete this
    bucket: String
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Keepalive {
    token_expires_in: i32
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CheckpointDiff {
    #[serde(deserialize_with = "deserialize_string_to_i64")]
    last_op_id: i64,
    updated_buckets: Vec<BucketChecksum>,
    removed_buckets: Vec<String>,
    #[serde(default)]
    #[serde(deserialize_with = "deserialize_optional_string_to_i64")]
    write_checkpoint: Option<i64>
}



#[derive(Debug)]
pub enum StreamingSyncLine {
    CheckpointLine(Checkpoint),
    CheckpointDiffLine(CheckpointDiff),
    CheckpointCompleteLine(CheckpointComplete),
    SyncBucketDataLine(SyncBucketData),
    KeepaliveLine(i32),
    Unknown
}

// Serde does not supporting ignoring unknown fields in externally-tagged enums, so we use our own
// serializer.

struct StreamingSyncLineVisitor;

impl<'de> Visitor<'de> for StreamingSyncLineVisitor {
    type Value = StreamingSyncLine;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("sync data")
    }

    fn visit_map<A>(self, mut access: A) -> Result<Self::Value, A::Error>
        where
            A: MapAccess<'de>,
    {
        let mut r = StreamingSyncLine::Unknown;
        while let Some((key, value)) = access.next_entry::<String, json::Value>()? {
            if !matches!(r, StreamingSyncLine::Unknown) {
                // Generally, we don't expect to receive multiple in one line.
                // But if it does happen, we keep the first one.
                continue;
            }
            match key.as_str() {
                "checkpoint" => {
                    r = StreamingSyncLine::CheckpointLine(
                        serde_json::from_value(value).map_err(de::Error::custom)?,
                    );
                }
                "checkpoint_diff" => {
                    r = StreamingSyncLine::CheckpointDiffLine(
                        serde_json::from_value(value).map_err(de::Error::custom)?,
                    );
                }
                "checkpoint_complete" => {
                    r = StreamingSyncLine::CheckpointCompleteLine(
                        serde_json::from_value(value).map_err(de::Error::custom)?,
                    );
                }
                "data" => {
                    r = StreamingSyncLine::SyncBucketDataLine(
                        serde_json::from_value(value).map_err(de::Error::custom)?,
                    );
                }
                "token_expires_in" => {
                    r = StreamingSyncLine::KeepaliveLine(
                        serde_json::from_value(value).map_err(de::Error::custom)?,
                    );
                }
                _ => {}
            }
        }

        Ok(r)
    }
}

impl<'de> Deserialize<'de> for StreamingSyncLine {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
    {
        deserializer.deserialize_map(StreamingSyncLineVisitor)
    }
}


#[cfg(test)]
mod tests {
    use core::assert_matches::assert_matches;
    use super::*;

    #[test]
    fn json_parsing_test() {
        let line: StreamingSyncLine = serde_json::from_str(r#"{"token_expires_in": 42}"#).unwrap();
        assert_matches!(line, StreamingSyncLine::KeepaliveLine(42));

        let line: StreamingSyncLine = serde_json::from_str(r#"{"checkpoint_complete": {"last_op_id": "123"}}"#).unwrap();
        assert_matches!(line, StreamingSyncLine::CheckpointCompleteLine(CheckpointComplete { last_op_id: 123 }));

        let line: StreamingSyncLine = serde_json::from_str(r#"{"checkpoint_complete": {"last_op_id": "123", "other": "foo"}}"#).unwrap();
        assert_matches!(line, StreamingSyncLine::CheckpointCompleteLine(CheckpointComplete { last_op_id: 123 }));

        let line: StreamingSyncLine = serde_json::from_str(r#"{"checkpoint": {"last_op_id": "123", "buckets": []}}"#).unwrap();
        assert_matches!(line, StreamingSyncLine::CheckpointLine(Checkpoint { last_op_id: 123, .. }));

        let line: StreamingSyncLine = serde_json::from_str(r#"{"checkpoint": {"last_op_id": "123", "write_checkpoint": "42", "buckets": []}}"#).unwrap();
        assert_matches!(line, StreamingSyncLine::CheckpointLine(Checkpoint { last_op_id: 123, write_checkpoint: Some(42), .. }));

        let line: StreamingSyncLine = serde_json::from_str(r#"{"checkpoint_diff": {"last_op_id": "123", "updated_buckets": [], "removed_buckets": []}}"#).unwrap();
        assert_matches!(line, StreamingSyncLine::CheckpointDiffLine(CheckpointDiff { last_op_id: 123, .. }));

        // Additional/unknown fields
        let line: StreamingSyncLine = serde_json::from_str(r#"{"token_expires_in": 42, "foo": 1}"#).unwrap();
        assert_matches!(line, StreamingSyncLine::KeepaliveLine(42));
        let line: StreamingSyncLine = serde_json::from_str(r#"{}"#).unwrap();
        assert_matches!(line, StreamingSyncLine::Unknown);
        let line: StreamingSyncLine = serde_json::from_str(r#"{"other":"test"}"#).unwrap();
        assert_matches!(line, StreamingSyncLine::Unknown);

        // Multiple - keep the first one
        let line: StreamingSyncLine = serde_json::from_str(r#"{"token_expires_in": 42, "checkpoint_complete": {"last_op_id": "123"}}"#).unwrap();
        assert_matches!(line, StreamingSyncLine::KeepaliveLine(42));

        // Test error handling
        let line: Result<StreamingSyncLine, _> = serde_json::from_str(r#"{"token_expires_in": "42"}"#);
        assert!(line.is_err());
    }
}
