extern crate alloc;

use core::ptr::null_mut;

use alloc::format;
use alloc::string::String;

use lock_api::{GuardSend, Mutex as MutexApi, RawMutex};
use serde::de::Visitor;
use serde::Deserialize;
use serde_json as json;
use sqlite_nostd::bindings::SQLITE_MUTEX_FAST;
use sqlite_nostd::{
    sqlite3_mutex_alloc, sqlite3_mutex_enter, sqlite3_mutex_free, sqlite3_mutex_leave,
    sqlite3_mutex_try,
};

#[cfg(feature = "getrandom")]
use crate::sqlite;
use crate::sqlite::bindings::sqlite3_mutex;

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
    deserialize_optional_string_to_i64(deserializer)?
        .ok_or_else(|| serde::de::Error::custom("Expected a string."))
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

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            v.parse::<i64>().map(Some).map_err(serde::de::Error::custom)
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(None)
        }
    }

    // Using a custom visitor here to avoid an intermediate string allocation
    deserializer.deserialize_any(ValueVisitor)
}

pub struct SqliteMutex {
    ptr: *mut sqlite3_mutex,
}

impl SqliteMutex {
    pub fn new() -> Self {
        Self {
            ptr: sqlite3_mutex_alloc(SQLITE_MUTEX_FAST as i32),
        }
    }
}

unsafe impl RawMutex for SqliteMutex {
    const INIT: Self = SqliteMutex { ptr: null_mut() };

    type GuardMarker = GuardSend;

    fn lock(&self) {
        sqlite3_mutex_enter(self.ptr);
    }

    fn try_lock(&self) -> bool {
        sqlite3_mutex_try(self.ptr) == 0
    }

    unsafe fn unlock(&self) {
        sqlite3_mutex_free(self.ptr);
    }
}

impl Drop for SqliteMutex {
    fn drop(&mut self) {
        sqlite3_mutex_free(self.ptr);
    }
}

pub type Mutex<T> = MutexApi<SqliteMutex, T>;

pub fn sqlite3_mutex<T>(value: T) -> Mutex<T> {
    let raw = SqliteMutex::new();
    MutexApi::from_raw(raw, value)
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
