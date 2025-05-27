use core::{
    fmt::Display,
    num::Wrapping,
    ops::{Add, AddAssign, Sub, SubAssign},
};

use num_traits::float::FloatCore;
use num_traits::Zero;
use serde::{de::Visitor, Deserialize, Serialize};

/// A checksum as received from the sync service.
///
/// Conceptually, we use unsigned 32 bit integers to represent checksums, and adding checksums
/// should be a wrapping add.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct Checksum(Wrapping<u32>);

impl Checksum {
    pub const fn value(self) -> u32 {
        self.0 .0
    }

    pub const fn from_value(value: u32) -> Self {
        Self(Wrapping(value))
    }

    pub const fn from_i32(value: i32) -> Self {
        Self::from_value(value as u32)
    }

    pub const fn bitcast_i32(self) -> i32 {
        self.value() as i32
    }
}

impl Zero for Checksum {
    fn zero() -> Self {
        const { Self::from_value(0) }
    }

    fn is_zero(&self) -> bool {
        self.value() == 0
    }
}

impl Add for Checksum {
    type Output = Self;

    #[inline]
    fn add(self, rhs: Self) -> Self::Output {
        Self(self.0 + rhs.0)
    }
}

impl AddAssign for Checksum {
    #[inline]
    fn add_assign(&mut self, rhs: Self) {
        self.0 += rhs.0
    }
}

impl Sub for Checksum {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Self(self.0 - rhs.0)
    }
}

impl SubAssign for Checksum {
    fn sub_assign(&mut self, rhs: Self) {
        self.0 -= rhs.0;
    }
}

impl From<u32> for Checksum {
    fn from(value: u32) -> Self {
        Self::from_value(value)
    }
}

impl Display for Checksum {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{:#010x}", self.value())
    }
}

impl<'de> Deserialize<'de> for Checksum {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct MyVisitor;

        impl<'de> Visitor<'de> for MyVisitor {
            type Value = Checksum;

            fn expecting(&self, formatter: &mut core::fmt::Formatter) -> core::fmt::Result {
                write!(formatter, "a number to interpret as a checksum")
            }

            fn visit_u32<E>(self, v: u32) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(v.into())
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let as_u32: u32 = v.try_into().map_err(|_| {
                    E::invalid_value(serde::de::Unexpected::Unsigned(v), &"a 32-bit int")
                })?;
                Ok(as_u32.into())
            }

            fn visit_i32<E>(self, v: i32) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Checksum::from_i32(v))
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                // This is supposed to be an u32, but it could also be a i32 that we need to
                // normalize.
                let min: i64 = u32::MIN.into();
                let max: i64 = u32::MAX.into();

                if v >= min && v <= max {
                    return Ok(Checksum::from(v as u32));
                }

                let as_i32: i32 = v.try_into().map_err(|_| {
                    E::invalid_value(serde::de::Unexpected::Signed(v), &"a 32-bit int")
                })?;
                Ok(Checksum::from_i32(as_i32))
            }

            fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                if !v.is_finite() || f64::trunc(v) != v {
                    return Err(E::invalid_value(
                        serde::de::Unexpected::Float(v),
                        &"a whole number",
                    ));
                }

                self.visit_i64(v as i64)
            }
        }

        deserializer.deserialize_u32(MyVisitor)
    }
}

#[cfg(test)]
mod test {
    use super::Checksum;

    #[test]
    pub fn test_binary_representation() {
        assert_eq!(Checksum::from_i32(-1).value(), u32::MAX);
        assert_eq!(Checksum::from(u32::MAX).value(), u32::MAX);
        assert_eq!(Checksum::from(u32::MAX).bitcast_i32(), -1);
    }

    fn deserialize(from: &str) -> Checksum {
        serde_json::from_str(from).expect("should deserialize")
    }

    #[test]
    pub fn test_deserialize() {
        assert_eq!(deserialize("0").value(), 0);
        assert_eq!(deserialize("-1").value(), u32::MAX);
        assert_eq!(deserialize("-1.0").value(), u32::MAX);

        assert_eq!(deserialize("3573495687").value(), 3573495687);
        assert_eq!(deserialize("3573495687.0").value(), 3573495687);
        assert_eq!(deserialize("-721471609.0").value(), 3573495687);
    }
}
