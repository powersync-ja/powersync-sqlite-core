extern crate alloc;

use core::ptr::{self, null_mut};

use alloc::format;
use alloc::string::String;

#[cfg(not(feature = "getrandom"))]
use crate::sqlite;
use serde::de::Visitor;

use uuid::Uuid;

#[cfg(not(feature = "getrandom"))]
use uuid::Builder;

pub fn quote_string(s: &str) -> String {
    format!("'{:}'", s.replace("'", "''"))
}

pub fn quote_json_path(s: &str) -> String {
    quote_string(&format!("$.{:}", s))
}

pub fn quote_identifier(name: &str) -> String {
    format!("\"{:}\"", name.replace("\"", "\"\""))
}

pub fn quote_internal_name(name: &str, local_only: bool) -> String {
    if local_only {
        quote_identifier_prefixed("ps_data_local__", name)
    } else {
        quote_identifier_prefixed("ps_data__", name)
    }
}

pub fn internal_table_name(name: &str) -> String {
    return format!("ps_data__{}", name);
}

pub fn quote_identifier_prefixed(prefix: &str, name: &str) -> String {
    return format!("\"{:}{:}\"", prefix, name.replace("\"", "\"\""));
}

pub fn deserialize_string_to_i64<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct ValueVisitor;

    impl<'de> Visitor<'de> for ValueVisitor {
        type Value = i64;

        fn expecting(&self, formatter: &mut core::fmt::Formatter) -> core::fmt::Result {
            formatter.write_str("a string representation of a number")
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            v.parse::<i64>().map_err(serde::de::Error::custom)
        }
    }

    // Using a custom visitor here to avoid an intermediate string allocation
    deserializer.deserialize_str(ValueVisitor)
}

pub fn deserialize_optional_string_to_i64<'de, D>(deserializer: D) -> Result<Option<i64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct ValueVisitor;

    impl<'de> Visitor<'de> for ValueVisitor {
        type Value = Option<i64>;

        fn expecting(&self, formatter: &mut core::fmt::Formatter) -> core::fmt::Result {
            formatter.write_str("a string or null")
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
            Ok(Some(deserialize_string_to_i64(deserializer)?))
        }
    }

    deserializer.deserialize_option(ValueVisitor)
}

// Use getrandom crate to generate UUID.
// This is not available in all WASM builds - use the default in those cases.
#[cfg(feature = "getrandom")]
pub fn gen_uuid() -> Uuid {
    let id = Uuid::new_v4();
    id
}

// Default - use sqlite3_randomness to generate UUID
// This uses ChaCha20 PRNG, with /dev/urandom as a seed on unix.
// On Windows, it uses custom logic for the seed, which may not be secure.
// Rather avoid this version for most builds.
#[cfg(not(feature = "getrandom"))]
pub fn gen_uuid() -> Uuid {
    let mut random_bytes: [u8; 16] = [0; 16];
    sqlite::randomness(&mut random_bytes);
    let id = Builder::from_random_bytes(random_bytes).into_uuid();
    id
}

pub const MAX_OP_ID: &str = "9223372036854775807";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_identifier_test() {
        assert_eq!(quote_identifier("test"), "\"test\"");
        assert_eq!(quote_identifier("\"quote\""), "\"\"\"quote\"\"\"");
        assert_eq!(
            quote_identifier("other characters."),
            "\"other characters.\""
        );
    }

    #[test]
    fn quote_string_test() {
        assert_eq!(quote_string("test"), "'test'");
        assert_eq!(quote_string("\"quote\""), "'\"quote\"'");
        assert_eq!(quote_string("'quote'"), "'''quote'''");
    }
}
