use alloc::borrow::Cow;
use alloc::vec::Vec;
use serde::Deserialize;
use serde::de::{IgnoredAny, VariantAccess, Visitor};

use crate::util::{deserialize_optional_string_to_i64, deserialize_string_to_i64};

use super::Checksum;
use super::bucket_priority::BucketPriority;

/// While we would like to always borrow strings for efficiency, that's not consistently possible.
/// With the JSON decoder, borrowing from input data is only possible when the string contains no
/// escape sequences (otherwise, the string is not a direct view of input data and we need an
/// internal copy).
type SyncLineStr<'a> = Cow<'a, str>;

#[derive(Debug)]

pub enum SyncLine<'a> {
    Checkpoint(Checkpoint<'a>),
    CheckpointDiff(CheckpointDiff<'a>),
    CheckpointComplete(CheckpointComplete),
    CheckpointPartiallyComplete(CheckpointPartiallyComplete),
    Data(DataLine<'a>),
    KeepAlive(TokenExpiresIn),
    UnknownSyncLine,
}

impl<'de> Deserialize<'de> for SyncLine<'de> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct SyncLineVisitor;

        impl<'de> Visitor<'de> for SyncLineVisitor {
            type Value = SyncLine<'de>;

            fn expecting(&self, formatter: &mut core::fmt::Formatter) -> core::fmt::Result {
                write!(formatter, "a sync line")
            }

            fn visit_enum<A>(self, data: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::EnumAccess<'de>,
            {
                let (name, payload) = data.variant::<&'de str>()?;
                Ok(match name {
                    "checkpoint" => SyncLine::Checkpoint(payload.newtype_variant::<Checkpoint>()?),
                    "checkpoint_diff" => {
                        SyncLine::CheckpointDiff(payload.newtype_variant::<CheckpointDiff>()?)
                    }
                    "checkpoint_complete" => SyncLine::CheckpointComplete(
                        payload.newtype_variant::<CheckpointComplete>()?,
                    ),
                    "partial_checkpoint_complete" => SyncLine::CheckpointPartiallyComplete(
                        payload.newtype_variant::<CheckpointPartiallyComplete>()?,
                    ),
                    "data" => SyncLine::Data(payload.newtype_variant::<DataLine>()?),
                    "token_expires_in" => {
                        SyncLine::KeepAlive(payload.newtype_variant::<TokenExpiresIn>()?)
                    }
                    _ => {
                        payload.newtype_variant::<IgnoredAny>()?;

                        SyncLine::UnknownSyncLine
                    }
                })
            }
        }

        deserializer.deserialize_enum("SyncLine", &[], SyncLineVisitor)
    }
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
    pub removed_buckets: Vec<SyncLineStr<'a>>,
    #[serde(default)]
    #[serde(deserialize_with = "deserialize_optional_string_to_i64")]
    pub write_checkpoint: Option<i64>,
}

#[derive(Deserialize, Debug)]
pub struct CheckpointComplete {
    //    #[serde(deserialize_with = "deserialize_string_to_i64")]
    //    pub last_op_id: i64,
}

#[derive(Deserialize, Debug)]
pub struct CheckpointPartiallyComplete {
    //    #[serde(deserialize_with = "deserialize_string_to_i64")]
    //    pub last_op_id: i64,
    pub priority: BucketPriority,
}

#[derive(Deserialize, Debug)]
pub struct BucketChecksum<'a> {
    #[serde(borrow)]
    pub bucket: SyncLineStr<'a>,
    pub checksum: Checksum,
    #[serde(default)]
    pub priority: Option<BucketPriority>,
    #[serde(default)]
    pub count: Option<i64>,
    #[serde(
        default,
        deserialize_with = "BucketChecksum::deserialize_subscriptions"
    )]
    pub subscriptions: Option<Vec<i64>>,
    //    #[serde(default)]
    //    #[serde(deserialize_with = "deserialize_optional_string_to_i64")]
    //    pub last_op_id: Option<i64>,
}

impl BucketChecksum<'_> {
    fn deserialize_subscriptions<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Vec<i64>>, D::Error> {
        struct MyVisitor;

        impl<'de> Visitor<'de> for MyVisitor {
            type Value = Option<Vec<i64>>;

            fn expecting(&self, formatter: &mut core::fmt::Formatter) -> core::fmt::Result {
                write!(formatter, "optional list of subscriptions")
            }

            fn visit_none<E>(self) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(None)
            }

            fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                deserializer.deserialize_seq(self)
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut result: Vec<i64> = Vec::new();
                while let Some(element) = seq.next_element::<&'de str>()? {
                    result.push(element.parse::<i64>().map_err(serde::de::Error::custom)?);
                }

                Ok(Some(result))
            }
        }

        deserializer.deserialize_option(MyVisitor)
    }
}

#[derive(Deserialize, Debug)]
pub struct DataLine<'a> {
    #[serde(borrow)]
    pub bucket: SyncLineStr<'a>,
    pub data: Vec<OplogEntry<'a>>,
    //    #[serde(default)]
    //    pub has_more: bool,
    //    #[serde(default, borrow)]
    //    pub after: Option<SyncLineStr<'a>>,
    //    #[serde(default, borrow)]
    //    pub next_after: Option<SyncLineStr<'a>>,
}

#[derive(Deserialize, Debug)]
pub struct OplogEntry<'a> {
    pub checksum: Checksum,
    #[serde(deserialize_with = "deserialize_string_to_i64")]
    pub op_id: i64,
    pub op: OpType,
    #[serde(default, borrow)]
    pub object_id: Option<SyncLineStr<'a>>,
    #[serde(default, borrow)]
    pub object_type: Option<SyncLineStr<'a>>,
    #[serde(default, borrow)]
    pub subkey: Option<SyncLineStr<'a>>,
    #[serde(default, borrow)]
    pub data: Option<OplogData<'a>>,
}

#[derive(Debug)]
pub enum OplogData<'a> {
    /// A string encoding a well-formed JSON object representing values of the row.
    Json { data: Cow<'a, str> },
    //    BsonDocument { data: Cow<'a, [u8]> },
}

#[derive(Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpType {
    CLEAR,
    MOVE,
    PUT,
    REMOVE,
}

#[repr(transparent)]
#[derive(Deserialize, Debug, Clone, Copy)]
pub struct TokenExpiresIn(pub i32);

impl TokenExpiresIn {
    pub fn is_expired(self) -> bool {
        self.0 <= 0
    }

    pub fn should_prefetch(self) -> bool {
        !self.is_expired() && self.0 <= 30
    }
}

impl<'a, 'de: 'a> Deserialize<'de> for OplogData<'a> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // For now, we will always get oplog data as a string. In the future, there may be the
        // option of the sync service sending BSON-encoded data lines too, but that's not relevant
        // for now.
        return Ok(OplogData::Json {
            data: Deserialize::deserialize(deserializer)?,
        });
    }
}

#[cfg(test)]
mod tests {
    use core::assert_matches::assert_matches;

    use alloc::string::ToString;

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
        assert_eq!(bucket.checksum, 10u32.into());
        assert_eq!(bucket.priority, None);

        let SyncLine::Checkpoint(checkpoint) = deserialize(
            r#"{"checkpoint": {"last_op_id": "10", "buckets": [{"bucket": "a", "priority": 1, "checksum": 10}]}}"#,
        ) else {
            panic!("Expected checkpoint");
        };

        assert_eq!(checkpoint.buckets.len(), 1);
        let bucket = &checkpoint.buckets[0];
        assert_eq!(bucket.bucket, "a");
        assert_eq!(bucket.checksum, 10u32.into());
        assert_eq!(bucket.priority, Some(BucketPriority { number: 1 }));

        assert_matches!(
            deserialize(
                r#"{"checkpoint":{"write_checkpoint":null,"last_op_id":"1","buckets":[{"bucket":"a","checksum":0,"priority":3,"count":1}]}}"#
            ),
            SyncLine::Checkpoint(Checkpoint {
                last_op_id: 1,
                write_checkpoint: None,
                buckets: _,
            })
        );
    }

    #[test]
    fn parse_checkpoint_diff() {
        let SyncLine::CheckpointDiff(diff) = deserialize(
            r#"{"checkpoint_diff": {"last_op_id": "10", "buckets": [], "updated_buckets": [], "removed_buckets": [], "write_checkpoint": null}}"#,
        ) else {
            panic!("Expected checkpoint diff")
        };

        assert_eq!(diff.updated_buckets.len(), 0);
        assert_eq!(diff.removed_buckets.len(), 0);
    }

    #[test]
    fn parse_checkpoint_diff_escape() {
        let SyncLine::CheckpointDiff(diff) = deserialize(
            r#"{"checkpoint_diff": {"last_op_id": "10", "buckets": [], "updated_buckets": [], "removed_buckets": ["foo\""], "write_checkpoint": null}}"#,
        ) else {
            panic!("Expected checkpoint diff")
        };

        assert_eq!(diff.removed_buckets[0], "foo\"");
    }

    #[test]
    fn parse_checkpoint_diff_no_write_checkpoint() {
        let SyncLine::CheckpointDiff(_diff) = deserialize(
            r#"{"checkpoint_diff":{"last_op_id":"12","updated_buckets":[{"bucket":"a","count":12,"checksum":0,"priority":3}],"removed_buckets":[]}}"#,
        ) else {
            panic!("Expected checkpoint diff")
        };
    }

    #[test]
    fn parse_checkpoint_complete() {
        assert_matches!(
            deserialize(r#"{"checkpoint_complete": {"last_op_id": "10"}}"#),
            SyncLine::CheckpointComplete(CheckpointComplete {
                // last_op_id: 10
            })
        );
    }

    #[test]
    fn parse_checkpoint_partially_complete() {
        assert_matches!(
            deserialize(r#"{"partial_checkpoint_complete": {"last_op_id": "10", "priority": 1}}"#),
            SyncLine::CheckpointPartiallyComplete(CheckpointPartiallyComplete {
                //last_op_id: 10,
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

        assert_eq!(data.data.len(), 1);
        let entry = &data.data[0];
        assert_eq!(entry.checksum, 10u32.into());
        assert_matches!(
            &data.data[0],
            OplogEntry {
                checksum: _,
                op_id: 1,
                object_id: Some(_),
                object_type: Some(_),
                op: OpType::PUT,
                subkey: None,
                data: _,
            }
        );
    }

    #[test]
    fn parse_unknown() {
        assert_matches!(deserialize("{\"foo\": {}}"), SyncLine::UnknownSyncLine);
        assert_matches!(deserialize("{\"foo\": 123}"), SyncLine::UnknownSyncLine);
    }

    #[test]
    fn parse_invalid_duplicate_key() {
        let e = serde_json::from_str::<SyncLine>(r#"{"foo": {}, "bar": {}}"#).unwrap_err();
        assert_eq!(e.to_string(), "expected value at line 1 column 10");
    }
}
