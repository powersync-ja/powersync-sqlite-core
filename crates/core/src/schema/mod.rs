mod management;
mod table_info;

use alloc::vec::Vec;
use serde::Deserialize;
use sqlite::ResultCode;
use sqlite_nostd as sqlite;
pub use table_info::{Column, DiffIncludeOld, Table, TableInfoFlags};

#[derive(Deserialize)]
pub struct Schema {
    tables: Vec<table_info::Table>,
}

pub fn register(db: *mut sqlite::sqlite3) -> Result<(), ResultCode> {
    management::register(db)
}
