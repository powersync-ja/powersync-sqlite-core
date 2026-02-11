mod common;
pub mod inspection;
mod management;
mod raw_table;
mod table_info;

use alloc::{rc::Rc, vec::Vec};
pub use common::{ColumnFilter, SchemaTable};
use powersync_sqlite_nostd::{self as sqlite, Connection, Context, Value, args};
use serde::Deserialize;
use sqlite::ResultCode;
pub use table_info::{
    Column, CommonTableOptions, PendingStatement, PendingStatementValue, RawTable, Table,
    TableInfoFlags,
};

use crate::{
    error::{PSResult, PowerSyncError},
    schema::raw_table::generate_raw_table_trigger,
    state::DatabaseState,
    utils::WriteType,
};

#[derive(Deserialize, Default)]
pub struct Schema {
    pub tables: Vec<table_info::Table>,
    #[serde(default)]
    pub raw_tables: Vec<table_info::RawTable>,
}

pub fn register(db: *mut sqlite::sqlite3, state: Rc<DatabaseState>) -> Result<(), ResultCode> {
    management::register(db, state)?;

    {
        fn create_trigger(
            context: *mut sqlite::context,
            args: &[*mut sqlite::value],
        ) -> Result<(), PowerSyncError> {
            // Args: Table (JSON), trigger_name, write_type
            let table: RawTable =
                serde_json::from_str(args[0].text()).map_err(PowerSyncError::as_argument_error)?;
            let trigger_name = args[1].text();
            let write_type: WriteType = args[2].text().parse()?;

            let db = context.db_handle();
            let create_trigger_stmt =
                generate_raw_table_trigger(db, &table, trigger_name, write_type)?;
            db.exec_safe(&create_trigger_stmt).into_db_result(db)?;
            Ok(())
        }

        extern "C" fn create_raw_trigger_sqlite(
            context: *mut sqlite::context,
            argc: i32,
            args: *mut *mut sqlite::value,
        ) {
            let args = args!(argc, args);
            if let Err(e) = create_trigger(context, args) {
                e.apply_to_ctx("powersync_create_raw_table_crud_trigger", context);
            }
        }

        db.create_function_v2(
            "powersync_create_raw_table_crud_trigger",
            3,
            sqlite::UTF8,
            None,
            Some(create_raw_trigger_sqlite),
            None,
            None,
            None,
        )?;
    }
    Ok(())
}
