extern crate alloc;

use core::ptr::{self, null_mut};

use alloc::format;
use alloc::string::String;

use lock_api::{GuardSend, Mutex as MutexApi, RawMutex};
use serde::de::Visitor;
use sqlite_nostd::bindings::SQLITE_MUTEX_FAST;
use sqlite_nostd::{api_routines, Connection, Context};

use crate::error::SQLiteError;
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

        fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            deserializer.deserialize_str(self)
        }
    }

    // Using a custom visitor here to avoid an intermediate string allocation
    deserializer.deserialize_option(ValueVisitor)
}

pub struct SqliteMutex {
    ptr: *mut sqlite3_mutex,
}

// We always invoke mutex APIs through the api routines, even when we link the rest of SQLite
// statically.
// The reason is that it's possible to omit the mutex code (in which case we don't want to link
// undefined symbols).
pub(crate) static mut SQLITE3_API: *mut api_routines = ptr::null_mut();

impl SqliteMutex {
    pub fn new() -> Self {
        let native_alloc = unsafe { (*SQLITE3_API).mutex_alloc };

        Self {
            ptr: match native_alloc {
                None => null_mut(),
                Some(mutex_alloc) => unsafe { mutex_alloc(SQLITE_MUTEX_FAST as i32) },
            },
        }
    }
}

unsafe impl RawMutex for SqliteMutex {
    const INIT: Self = SqliteMutex { ptr: null_mut() };

    type GuardMarker = GuardSend;

    fn lock(&self) {
        if self.ptr.is_null() {
            // Disable mutex code
        } else {
            unsafe { (*SQLITE3_API).mutex_enter.unwrap_unchecked()(self.ptr) }
        }
    }

    fn try_lock(&self) -> bool {
        if self.ptr.is_null() {
            // Disable mutex code
            true
        } else {
            let res = unsafe { (*SQLITE3_API).mutex_try.unwrap_unchecked()(self.ptr) };
            res == 0
        }
    }

    unsafe fn unlock(&self) {
        if self.ptr.is_null() {
            // Disable mutex code
        } else {
            unsafe { (*SQLITE3_API).mutex_leave.unwrap_unchecked()(self.ptr) }
        }
    }
}

impl Drop for SqliteMutex {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe { (*SQLITE3_API).mutex_free.unwrap_unchecked()(self.ptr) };
        }
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
