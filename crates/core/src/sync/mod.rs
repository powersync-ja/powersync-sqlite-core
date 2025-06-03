use sqlite_nostd::{self as sqlite, ResultCode};

mod bucket_priority;
pub mod checkpoint;
mod checksum;
mod interface;
pub mod line;
pub mod operations;
pub mod storage_adapter;
mod streaming_sync;
mod sync_status;

pub use bucket_priority::BucketPriority;
pub use checksum::Checksum;

pub fn register(db: *mut sqlite::sqlite3) -> Result<(), ResultCode> {
    interface::register(db)
}
