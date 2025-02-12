use core::ops::RangeInclusive;

use sqlite_nostd::ResultCode;

use crate::error::SQLiteError;

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct BucketPriority(i32);

impl BucketPriority {
    pub fn may_publish_with_outstanding_uploads(self) -> bool {
        self == BucketPriority::HIGHEST
    }

    pub const HIGHEST: BucketPriority = BucketPriority(0);
    pub const LOWEST: BucketPriority = BucketPriority(3);
}

impl TryFrom<i32> for BucketPriority {
    type Error = SQLiteError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        const VALID: RangeInclusive<i32> = (BucketPriority::HIGHEST.0)..=(BucketPriority::LOWEST.0);

        if !VALID.contains(&value) {
            return Err(SQLiteError(
                ResultCode::MISUSE,
                Some("Invalid bucket priority".into()),
            ));
        }

        return Ok(BucketPriority(value));
    }
}

impl Default for BucketPriority {
    fn default() -> Self {
        Self(1)
    }
}

impl Into<i32> for BucketPriority {
    fn into(self) -> i32 {
        self.0
    }
}

impl PartialOrd<BucketPriority> for BucketPriority {
    fn partial_cmp(&self, other: &BucketPriority) -> Option<core::cmp::Ordering> {
        Some(self.0.partial_cmp(&other.0)?.reverse())
    }
}
