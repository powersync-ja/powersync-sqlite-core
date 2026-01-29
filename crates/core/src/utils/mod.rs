mod sql_buffer;

use core::{cmp::Ordering, fmt::Display, hash::Hash};

use alloc::{boxed::Box, string::String};
use powersync_sqlite_nostd::{ColumnType, ManagedStmt};
use serde::Serialize;
use serde_json::value::RawValue;
pub use sql_buffer::{InsertIntoCrud, SqlBuffer};

use crate::error::PowerSyncError;
use uuid::Uuid;

/// Calls [read] to read a column if it's not null, otherwise returns [None].
#[inline]
pub fn column_nullable<T, R: FnOnce() -> Result<T, PowerSyncError>>(
    stmt: &ManagedStmt,
    index: i32,
    read: R,
) -> Result<Option<T>, PowerSyncError> {
    if stmt.column_type(index)? == ColumnType::Null {
        Ok(None)
    } else {
        Ok(Some(read()?))
    }
}

/// An opaque wrapper around a JSON-serialized value.
///
/// This wraps [RawValue] from `serde_json`, adding implementations for comparisons and hashes.
#[derive(Debug)]
#[repr(transparent)]
pub struct JsonString(pub RawValue);

impl JsonString {
    pub fn from_string(string: String) -> Result<Box<Self>, PowerSyncError> {
        let underlying =
            RawValue::from_string(string).map_err(PowerSyncError::as_argument_error)?;
        unsafe {
            // Safety: repr(transparent)
            core::mem::transmute(underlying)
        }
    }
}

impl Hash for JsonString {
    fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
        self.0.get().hash(state);
    }
}

impl PartialEq for JsonString {
    fn eq(&self, other: &Self) -> bool {
        self.0.get() == other.0.get()
    }
}

impl Eq for JsonString {}

impl PartialOrd for JsonString {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for JsonString {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.get().cmp(other.0.get())
    }
}

impl Serialize for JsonString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl Clone for Box<JsonString> {
    fn clone(&self) -> Self {
        let raw_value_box: &Box<RawValue> = unsafe {
            // SAFETY: repr(transparent)
            core::mem::transmute(self)
        };

        unsafe { core::mem::transmute(raw_value_box.clone()) }
    }
}

impl Display for JsonString {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        self.0.fmt(f)
    }
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
    use crate::sqlite;
    use uuid::Builder;

    let mut random_bytes: [u8; 16] = [0; 16];
    sqlite::randomness(&mut random_bytes);
    let id = Builder::from_random_bytes(random_bytes).into_uuid();
    id
}

pub const MAX_OP_ID: &str = "9223372036854775807";
