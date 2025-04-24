mod management;
mod table_info;

use sqlite::ResultCode;
use sqlite_nostd as sqlite;
pub use table_info::{
    ColumnInfo, ColumnNameAndTypeStatement, DiffIncludeOld, TableInfo, TableInfoFlags,
};

pub fn register(db: *mut sqlite::sqlite3) -> Result<(), ResultCode> {
    management::register(db)
}
