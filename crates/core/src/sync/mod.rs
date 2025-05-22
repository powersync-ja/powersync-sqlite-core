use sqlite_nostd::{self as sqlite, ResultCode};

pub mod bucket_priority;
mod checksum;
mod interface;
pub mod line;
pub mod operations;
pub mod storage_adapter;
mod streaming_sync;
mod sync_status;

pub use checksum::Checksum;

pub fn register(db: *mut sqlite::sqlite3) -> Result<(), ResultCode> {
    interface::register(db)
}
