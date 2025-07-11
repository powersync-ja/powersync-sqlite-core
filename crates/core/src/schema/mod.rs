mod management;
mod table_info;

use alloc::vec::Vec;
use serde::Deserialize;
use sqlite::ResultCode;
use sqlite_nostd as sqlite;
pub use table_info::{
    Column, DiffIncludeOld, PendingStatement, PendingStatementValue, RawTable, Table,
    TableInfoFlags,
};

#[derive(Deserialize, Default)]
pub struct Schema {
    pub tables: Vec<table_info::Table>,
    #[serde(default)]
    pub raw_tables: Vec<table_info::RawTable>,
}

pub fn register(db: *mut sqlite::sqlite3) -> Result<(), ResultCode> {
    management::register(db)
}
