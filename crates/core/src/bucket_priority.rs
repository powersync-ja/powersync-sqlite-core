use sqlite_nostd::ResultCode;

use crate::error::SQLiteError;

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct BucketPriority(i32);

impl BucketPriority {
    pub fn may_publish_with_outstanding_uploads(self) -> bool {
        self.0 == 0
    }

    pub const HIGHEST: BucketPriority = BucketPriority(0);
    pub const LOWEST: BucketPriority = BucketPriority(3);
}

impl TryFrom<i32> for BucketPriority {
    type Error = SQLiteError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        if value < BucketPriority::LOWEST.0 || value > BucketPriority::HIGHEST.0 {
            return Err(SQLiteError::from(ResultCode::MISUSE));
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
