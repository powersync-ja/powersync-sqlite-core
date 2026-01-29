use alloc::{
    string::{String, ToString},
    vec,
    vec::Vec,
};
use powersync_sqlite_nostd::{Connection, Destructor, ResultCode};

use crate::error::PowerSyncError;

pub struct InferredTableStructure {
    pub table_name: String,
    pub has_id_column: bool,
    pub columns: Vec<String>,
}

impl InferredTableStructure {
    pub fn read_from_database(
        table_name: &str,
        db: impl Connection,
    ) -> Result<Option<Self>, PowerSyncError> {
        let stmt = db.prepare_v2("select name from pragma_table_info(?)")?;
        stmt.bind_text(1, table_name, Destructor::STATIC)?;

        let mut has_id_column = false;
        let mut columns = vec![];

        while let ResultCode::ROW = stmt.step()? {
            let name = stmt.column_text(0)?;
            if name == "id" {
                has_id_column = true;
            } else {
                columns.push(name.to_string());
            }
        }

        if !has_id_column && columns.is_empty() {
            Ok(None)
        } else {
            Ok(Some(Self {
                table_name: table_name.to_string(),
                has_id_column,
                columns,
            }))
        }
    }
}
