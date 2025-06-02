use alloc::{string::String, vec::Vec};
use num_traits::Zero;
use serde::Deserialize;

use crate::{
    error::SQLiteError,
    sync::{
        line::{BucketChecksum, Checkpoint},
        BucketPriority, Checksum,
    },
};
use sqlite_nostd::{self as sqlite, Connection, ResultCode};

#[derive(Debug, Clone)]
pub struct OwnedBucketChecksum {
    pub bucket: String,
    pub checksum: Checksum,
    pub priority: BucketPriority,
    pub count: Option<i64>,
}

impl OwnedBucketChecksum {
    pub fn is_in_priority(&self, prio: Option<BucketPriority>) -> bool {
        match prio {
            None => true,
            Some(prio) => self.priority >= prio,
        }
    }
}

impl From<&'_ BucketChecksum<'_>> for OwnedBucketChecksum {
    fn from(value: &'_ BucketChecksum<'_>) -> Self {
        Self {
            bucket: value.bucket.clone().into_owned(),
            checksum: value.checksum,
            priority: value.priority.unwrap_or(BucketPriority::FALLBACK),
            count: value.count,
        }
    }
}

pub struct ChecksumMismatch {
    pub bucket_name: String,
    pub expected_checksum: Checksum,
    pub actual_op_checksum: Checksum,
    pub actual_add_checksum: Checksum,
}

pub fn validate_checkpoint<'a>(
    buckets: impl Iterator<Item = &'a OwnedBucketChecksum>,
    priority: Option<BucketPriority>,
    db: *mut sqlite::sqlite3,
) -> Result<Vec<ChecksumMismatch>, SQLiteError> {
    // language=SQLite
    let statement = db.prepare_v2(
        "
SELECT
    ps_buckets.add_checksum as add_checksum,
    ps_buckets.op_checksum as oplog_checksum
FROM ps_buckets WHERE name = ?;",
    )?;

    let mut failures: Vec<ChecksumMismatch> = Vec::new();
    for bucket in buckets {
        if bucket.is_in_priority(priority) {
            statement.bind_text(1, &bucket.bucket, sqlite_nostd::Destructor::STATIC)?;

            let (add_checksum, oplog_checksum) = match statement.step()? {
                ResultCode::ROW => {
                    let add_checksum = Checksum::from_i32(statement.column_int(0));
                    let oplog_checksum = Checksum::from_i32(statement.column_int(1));
                    (add_checksum, oplog_checksum)
                }
                _ => (Checksum::zero(), Checksum::zero()),
            };

            let actual = add_checksum + oplog_checksum;

            if actual != bucket.checksum {
                failures.push(ChecksumMismatch {
                    bucket_name: bucket.bucket.clone(),
                    expected_checksum: bucket.checksum,
                    actual_add_checksum: add_checksum,
                    actual_op_checksum: oplog_checksum,
                });
            }

            statement.reset()?;
        }
    }

    Ok(failures)
}
