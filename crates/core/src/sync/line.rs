use alloc::borrow::Cow;
use alloc::string::String;
use alloc::string::ToString;
use alloc::vec::Vec;
use serde::Deserialize;

use super::BucketPriority;
use super::Checksum;

use crate::util::{deserialize_optional_string_to_i64, deserialize_string_to_i64};

/// While we would like to always borrow strings for efficiency, that's not consistently possible.
/// With the JSON decoder, borrowing from input data is only possible when the string contains no
/// escape sequences (otherwise, the string is not a direct view of input data and we need an
/// internal copy).
type SyncLineStr<'a> = Cow<'a, str>;

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
pub struct BucketChecksum<'a> {
    #[serde(borrow)]
    pub bucket: SyncLineStr<'a>,
    pub checksum: Checksum,
    #[serde(default)]
    pub priority: Option<BucketPriority>,
    #[serde(default)]
    pub count: Option<i64>,
    //    #[serde(default)]
    //    #[serde(deserialize_with = "deserialize_optional_string_to_i64")]
    //    pub last_op_id: Option<i64>,
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
