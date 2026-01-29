pub mod inspection;
mod management;
mod raw_table;
mod table_info;

use alloc::{rc::Rc, vec::Vec};
use powersync_sqlite_nostd as sqlite;
use serde::Deserialize;
use sqlite::ResultCode;
pub use table_info::{
    Column, DiffIncludeOld, PendingStatement, PendingStatementValue, RawTable, Table,
    TableInfoFlags,
};

use crate::state::DatabaseState;

#[derive(Deserialize, Default)]
pub struct Schema {
    pub tables: Vec<table_info::Table>,
    #[serde(default)]
    pub raw_tables: Vec<table_info::RawTable>,
}

pub fn register(db: *mut sqlite::sqlite3, state: Rc<DatabaseState>) -> Result<(), ResultCode> {
    management::register(db, state)
}
