mod management;
mod table_info;

use alloc::vec::Vec;
use serde::Deserialize;
use sqlite::ResultCode;
use sqlite_nostd as sqlite;
pub use table_info::{
    DiffIncludeOld, PendingStatement, PendingStatementValue, RawTableDefinition, Table,
    TableInfoFlags,
};

#[derive(Deserialize, Default)]
pub struct Schema {
    pub tables: Vec<table_info::Table>,
}

pub fn register(db: *mut sqlite::sqlite3) -> Result<(), ResultCode> {
    management::register(db)
}
