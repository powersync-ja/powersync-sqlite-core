use alloc::string::String;
use alloc::vec::Vec;
use serde::{Deserialize, Serialize};

use crate::util::{deserialize_optional_string_to_i64, deserialize_string_to_i64};

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
    pub priority: Option<i32>,
}
