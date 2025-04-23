use alloc::borrow::Cow;
use alloc::string::ToString;
use alloc::vec::Vec;
use serde::de::DeserializeSeed;
use serde::{de::Visitor, Deserialize};

use crate::bson::{self, BsonWriter};
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
    #[serde(deserialize_with = "deserialize_optional_string_to_i64")]
    pub write_checkpoint: Option<i64>,
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
    pub data: Vec<OplogEntry<'a>>,
    #[serde(default)]
    pub has_more: bool,
    #[serde(default, borrow)]
    pub after: Option<&'a str>,
    #[serde(default, borrow)]
    pub next_after: Option<&'a str>,
}

#[derive(Deserialize, Debug)]
pub struct OplogEntry<'a> {
    pub checksum: i32,
    #[serde(deserialize_with = "deserialize_string_to_i64")]
    pub op_id: i64,
    pub op: OpType,
    #[serde(default, borrow)]
    pub object_id: Option<&'a str>,
    #[serde(default, borrow)]
    pub object_type: Option<&'a str>,
    #[serde(default, borrow)]
    pub subkey: Option<&'a str>,
    // TODO: BSON?
    #[serde(default, borrow)]
    pub data: Option<OplogData<'a>>,
}

#[derive(Debug)]
pub enum OplogData<'a> {
    JsonString { data: Cow<'a, str> },
    BsonDocument { data: Cow<'a, [u8]> },
}

#[derive(Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpType {
    CLEAR,
    MOVE,
    PUT,
    REMOVE,
}

#[derive(Deserialize, Debug)]
pub struct TokenExpiresIn(pub i32);

impl<'a, 'de: 'a> Deserialize<'de> for OplogData<'a> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Data can come in the following formats:
        //
        //  1. A `Map<String, PrimitiveValue>`
        //  2. A string encoding a `Map<String, PrimitiveValue>` as JSON.
        //  2. A `Map<String, PrimitiveValue>`, encoded as JSON.
        struct ReadDataVisitor;

        impl<'de> Visitor<'de> for ReadDataVisitor {
            type Value = OplogData<'de>;

            fn expecting(&self, formatter: &mut core::fmt::Formatter) -> core::fmt::Result {
                formatter.write_str("a string or an object")
            }

            fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                // Sync service sent data as JSON string. We will save that same string into
                // ps_oplog without any transformations.
                Ok(OplogData::JsonString {
                    data: Cow::Borrowed(v),
                })
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                // Same case, but if the deserializer doesn't let us borrow the JSON string.
                Ok(OplogData::JsonString {
                    data: Cow::Owned(v.to_string()),
                })
            }

            fn visit_borrowed_bytes<E>(self, v: &'de [u8]) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(OplogData::BsonDocument {
                    data: Cow::Borrowed(v),
                })
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                // Ok, we have a sub-document / JSON object. We can't save that as-is, we need to
                // serialize it. serde_json's Serializer is std-only, so we use a small serializer
                // to BSON that supports only the values we care about.
                let mut writer = BsonWriter::new();

                struct PendingKey<'a, 'de> {
                    key: &'de str,
                    writer: &'a mut BsonWriter,
                }

                impl<'a, 'de> Visitor<'de> for PendingKey<'a, 'de> {
                    type Value = ();

                    fn expecting(&self, formatter: &mut core::fmt::Formatter) -> core::fmt::Result {
                        formatter.write_str("SQLite-compatible value")
                    }

                    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
                    where
                        E: serde::de::Error,
                    {
                        self.writer.put_str(self.key, v);
                        Ok(())
                    }

                    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
                    where
                        E: serde::de::Error,
                    {
                        self.writer.put_float(self.key, v);
                        Ok(())
                    }

                    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
                    where
                        E: serde::de::Error,
                    {
                        self.writer.put_int(self.key, v as i64);
                        Ok(())
                    }

                    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
                    where
                        E: serde::de::Error,
                    {
                        self.writer.put_int(self.key, v);
                        Ok(())
                    }
                }

                impl<'a, 'de> DeserializeSeed<'de> for PendingKey<'a, 'de> {
                    type Value = ();

                    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
                    where
                        D: serde::Deserializer<'de>,
                    {
                        deserializer.deserialize_any(self)
                    }
                }

                while let Some(key) = map.next_key::<&'de str>()? {
                    let pending = PendingKey {
                        key,
                        writer: &mut writer,
                    };
                    map.next_value_seed(pending)?;
                }

                Ok(OplogData::BsonDocument {
                    data: Cow::Owned(writer.finish()),
                })
            }
        }

        let is_from_bson = !deserializer.is_human_readable();
        if is_from_bson {
            // If possible, try to read the data document as a BSON blob instead parsing individual
            // fields from it. We don't actually care about contents here (we'll just store them in
            // the database), so we just need the unparsed document.
            deserializer.deserialize_enum(
                bson::Deserializer::SPECIAL_CASE_EMBEDDED_DOCUMENT,
                &[],
                ReadDataVisitor,
            )
        } else {
            deserializer.deserialize_any(ReadDataVisitor)
        }
    }
}

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
