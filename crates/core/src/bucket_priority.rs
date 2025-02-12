use core::ops::RangeInclusive;

use serde::{de::Visitor, Deserialize};
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

impl<'de> Deserialize<'de> for BucketPriority {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct PriorityVisitor;
        impl<'de> Visitor<'de> for PriorityVisitor {
            type Value = BucketPriority;

            fn expecting(&self, formatter: &mut core::fmt::Formatter) -> core::fmt::Result {
                formatter.write_str("a priority as an integer between 0 and 3 (inclusive)")
            }

            fn visit_i32<E>(self, v: i32) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                BucketPriority::try_from(v).map_err(|e| E::custom(e.1.unwrap_or_default()))
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let i: i32 = v.try_into().map_err(|_| E::custom("int too large"))?;
                Self::visit_i32(self, i)
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let i: i32 = v.try_into().map_err(|_| E::custom("int too large"))?;
                Self::visit_i32(self, i)
            }
        }

        deserializer.deserialize_i32(PriorityVisitor)
    }
}
