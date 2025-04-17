use alloc::string::String;
use alloc::vec::Vec;
use serde::Deserialize;

use crate::util::{deserialize_optional_string_to_i64, deserialize_string_to_i64};

use super::bucket_priority::BucketPriority;

#[derive(Deserialize, Debug)]

pub enum SyncLine<'a> {
    #[serde(rename = "checkpoint", borrow)]
    Checkpoint(Checkpoint<'a>),
    #[serde(rename = "checkpoint_diff", borrow)]
    CheckpointDiff(CheckpointDiff<'a>),

    #[serde(rename = "checkpoint_complete")]
    CheckpointComplete(CheckpointComplete),
    #[serde(rename = "partial_checkpoint_complete")]
    CheckpointPartiallyComplete(CheckpointPartiallyComplete),

    #[serde(rename = "data", borrow)]
    Data(DataLine<'a>),

    #[serde(rename = "token_expires_in")]
    KeepAlive(TokenExpiresIn),
}

#[derive(Deserialize, Debug)]
pub struct Checkpoint<'a> {
    #[serde(deserialize_with = "deserialize_string_to_i64")]
    pub last_op_id: i64,
    #[serde(default)]
    #[serde(deserialize_with = "deserialize_optional_string_to_i64")]
    pub write_checkpoint: Option<i64>,
    #[serde(borrow)]
    pub buckets: Vec<BucketChecksum<'a>>,
}

#[derive(Deserialize, Debug)]
pub struct CheckpointDiff<'a> {
    #[serde(deserialize_with = "deserialize_string_to_i64")]
    pub last_op_id: i64,
    #[serde(borrow)]
    pub updated_buckets: Vec<BucketChecksum<'a>>,
    #[serde(borrow)]
    pub removed_buckets: Vec<&'a str>,
    #[serde(borrow)]
    pub write_checkpoint: Option<&'a str>,
}

#[derive(Deserialize, Debug)]
pub struct CheckpointComplete {
    #[serde(deserialize_with = "deserialize_string_to_i64")]
    pub last_op_id: i64,
}

#[derive(Deserialize, Debug)]
pub struct CheckpointPartiallyComplete {
    #[serde(deserialize_with = "deserialize_string_to_i64")]
    pub last_op_id: i64,
    pub priority: BucketPriority,
}

#[derive(Deserialize, Debug)]
pub struct BucketChecksum<'a> {
    pub bucket: &'a str,
    pub checksum: i32,
    pub priority: Option<BucketPriority>,
    pub count: Option<i64>,
    #[serde(default)]
    #[serde(deserialize_with = "deserialize_optional_string_to_i64")]
    pub last_op_id: Option<i64>,
}

#[derive(Deserialize, Debug)]
pub struct DataLine<'a> {
    pub bucket: &'a str,
    pub data: Vec<OplogEntry>,
    #[serde(default)]
    pub has_more: bool,
    #[serde(default, borrow)]
    pub after: Option<&'a str>,
    #[serde(default, borrow)]
    pub next_after: Option<&'a str>,
}

#[derive(Deserialize, Debug)]
pub struct OplogEntry {
    pub checksum: i32,
    #[serde(deserialize_with = "deserialize_string_to_i64")]
    pub op_id: i64,
    pub op: OpType,
    #[serde(default)]
    pub object_id: Option<String>,
    #[serde(default)]
    pub object_type: Option<String>,
    #[serde(default)]
    pub subkey: Option<String>,
    // TODO: BSON?
    #[serde(default)]
    pub data: Option<String>,
}

#[derive(Deserialize, Debug, Clone, Copy)]
pub enum OpType {
    CLEAR,
    MOVE,
    PUT,
    REMOVE,
}

#[derive(Deserialize, Debug)]
pub struct TokenExpiresIn(pub i32);

#[cfg(test)]
mod tests {
    use core::assert_matches::assert_matches;

    use super::*;

    fn deserialize(source: &str) -> SyncLine {
        serde_json::from_str(source).expect("Should have deserialized")
    }

    #[test]
    fn parse_token_expires_in() {
        assert_matches!(
            deserialize(r#"{"token_expires_in": 123}"#),
            SyncLine::KeepAlive(TokenExpiresIn(123))
        );
    }

    #[test]
    fn parse_checkpoint() {
        assert_matches!(
            deserialize(r#"{"checkpoint": {"last_op_id": "10", "buckets": []}}"#),
            SyncLine::Checkpoint(Checkpoint {
                last_op_id: 10,
                write_checkpoint: None,
                buckets: _,
            })
        );

        let SyncLine::Checkpoint(checkpoint) = deserialize(
            r#"{"checkpoint": {"last_op_id": "10", "buckets": [{"bucket": "a", "checksum": 10}]}}"#,
        ) else {
            panic!("Expected checkpoint");
        };

        assert_eq!(checkpoint.buckets.len(), 1);
        let bucket = &checkpoint.buckets[0];
        assert_eq!(bucket.bucket, "a");
        assert_eq!(bucket.checksum, 10);
        assert_eq!(bucket.priority, None);

        let SyncLine::Checkpoint(checkpoint) = deserialize(
            r#"{"checkpoint": {"last_op_id": "10", "buckets": [{"bucket": "a", "priority": 1, "checksum": 10}]}}"#,
        ) else {
            panic!("Expected checkpoint");
        };

        assert_eq!(checkpoint.buckets.len(), 1);
        let bucket = &checkpoint.buckets[0];
        assert_eq!(bucket.bucket, "a");
        assert_eq!(bucket.checksum, 10);
        assert_eq!(bucket.priority, Some(BucketPriority { number: 1 }));
    }

    #[test]
    fn parse_checkpoint_diff() {
        let SyncLine::CheckpointDiff(diff) = deserialize(
            r#"{"checkpoint_diff": {"last_op_id": "10", "buckets": [], "updated_buckets": [], "removed_buckets": []}}"#,
        ) else {
            panic!("Expected checkpoint diff")
        };

        assert_eq!(diff.updated_buckets.len(), 0);
        assert_eq!(diff.removed_buckets.len(), 0);
    }

    #[test]
    fn parse_checkpoint_complete() {
        assert_matches!(
            deserialize(r#"{"checkpoint_complete": {"last_op_id": "10"}}"#),
            SyncLine::CheckpointComplete(CheckpointComplete { last_op_id: 10 })
        );
    }

    #[test]
    fn parse_checkpoint_partially_complete() {
        assert_matches!(
            deserialize(r#"{"partial_checkpoint_complete": {"last_op_id": "10", "priority": 1}}"#),
            SyncLine::CheckpointPartiallyComplete(CheckpointPartiallyComplete {
                last_op_id: 10,
                priority: BucketPriority { number: 1 }
            })
        );
    }

    #[test]
    fn parse_data() {
        let SyncLine::Data(data) = deserialize(
            r#"{"data": {
                "bucket": "bkt",
                "data": [{"checksum":10,"op_id":"1","object_id":"test","object_type":"users","op":"PUT","subkey":null,"data":"{\"name\":\"user 0\",\"email\":\"0@example.org\"}"}],
                "after": null,
                "next_after": null}
                }"#,
        ) else {
            panic!("Expected data line")
        };

        assert_eq!(data.bucket, "bkt");
        assert_eq!(data.after, None);
        assert_eq!(data.next_after, None);

        assert_eq!(data.data.len(), 1);
        assert_matches!(
            &data.data[0],
            OplogEntry {
                checksum: 10,
                op_id: 1,
                object_id: Some(_),
                object_type: Some(_),
                op: OpType::PUT,
                subkey: None,
                data: _,
            }
        );
    }
}
