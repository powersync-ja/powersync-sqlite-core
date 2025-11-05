use alloc::borrow::ToOwned;
use alloc::{format, vec};
use alloc::{string::String, vec::Vec};
use powersync_sqlite_nostd::Connection;
use powersync_sqlite_nostd::{self as sqlite, ResultCode};

use crate::error::{PSResult, PowerSyncError};
use crate::util::quote_identifier;

/// An existing PowerSync-managed view that was found in the schema.
#[derive(PartialEq)]
pub struct ExistingView {
    /// The name of the view itself.
    pub name: String,
    /// SQL contents of the `CREATE VIEW` statement.
    pub sql: String,
    /// SQL contents of all triggers implementing deletes by forwarding to
    /// `ps_data` and `ps_crud`.
    pub delete_trigger_sql: String,
    /// SQL contents of the trigger implementing inserts on this view.
    pub insert_trigger_sql: String,
    /// SQL contents of the trigger implementing updates on this view.
    pub update_trigger_sql: String,
}

impl ExistingView {
    pub fn list(db: *mut sqlite::sqlite3) -> Result<Vec<Self>, PowerSyncError> {
        let mut results = vec![];
        let stmt = db.prepare_v2("
SELECT
    view.name,
    view.sql,
    ifnull(group_concat(trigger1.sql, ';\n' ORDER BY trigger1.name DESC), ''),
    ifnull(trigger2.sql, ''),
    ifnull(trigger3.sql, '')
    FROM sqlite_master view
    LEFT JOIN sqlite_master trigger1
        ON trigger1.tbl_name = view.name AND trigger1.type = 'trigger' AND trigger1.name GLOB 'ps_view_delete*'
    LEFT JOIN sqlite_master trigger2
        ON trigger2.tbl_name = view.name AND trigger2.type = 'trigger' AND trigger2.name GLOB 'ps_view_insert*'
    LEFT JOIN sqlite_master trigger3
        ON trigger3.tbl_name = view.name AND trigger3.type = 'trigger' AND trigger3.name GLOB 'ps_view_update*'
    WHERE view.type = 'view' AND view.sql GLOB  '*-- powersync-auto-generated'
    GROUP BY view.name;
        ").into_db_result(db)?;

        while stmt.step()? == ResultCode::ROW {
            let name = stmt.column_text(0)?.to_owned();
            let sql = stmt.column_text(1)?.to_owned();
            let delete = stmt.column_text(2)?.to_owned();
            let insert = stmt.column_text(3)?.to_owned();
            let update = stmt.column_text(4)?.to_owned();

            results.push(ExistingView {
                name,
                sql,
                delete_trigger_sql: delete,
                insert_trigger_sql: insert,
                update_trigger_sql: update,
            });
        }

        Ok(results)
    }

    pub fn drop_by_name(db: *mut sqlite::sqlite3, name: &str) -> Result<(), PowerSyncError> {
        let q = format!("DROP VIEW IF EXISTS {:}", quote_identifier(name));
        db.exec_safe(&q)?;
        Ok(())
    }

    pub fn create(&self, db: *mut sqlite::sqlite3) -> Result<(), PowerSyncError> {
        Self::drop_by_name(db, &self.name)?;
        db.exec_safe(&self.sql).into_db_result(db)?;
        db.exec_safe(&self.delete_trigger_sql).into_db_result(db)?;
        db.exec_safe(&self.insert_trigger_sql).into_db_result(db)?;
        db.exec_safe(&self.update_trigger_sql).into_db_result(db)?;

        Ok(())
    }
}

pub struct ExistingTable {
    pub name: String,
    pub internal_name: String,
    pub local_only: bool,
}

impl ExistingTable {
    pub fn list(db: *mut sqlite::sqlite3) -> Result<Vec<Self>, PowerSyncError> {
        let mut results = vec![];
        let stmt = db
            .prepare_v2(
                "
SELECT name FROM sqlite_master WHERE type = 'table' AND name GLOB 'ps_data_*';
        ",
            )
            .into_db_result(db)?;

        while stmt.step()? == ResultCode::ROW {
            let internal_name = stmt.column_text(0)?;
            let Some((name, local_only)) = Self::external_name(internal_name) else {
                continue;
            };

            results.push(ExistingTable {
                internal_name: internal_name.to_owned(),
                name: name.to_owned(),
                local_only: local_only,
            });
        }

        Ok(results)
    }

    /// Extracts the public name from a `ps_data__` or a `ps_data_local__` table.
    ///
    /// Also returns whether the name is from a local table.
    pub fn external_name(name: &str) -> Option<(&str, bool)> {
        const LOCAL_PREFIX: &str = "ps_data_local__";
        const NORMAL_PREFIX: &str = "ps_data__";

        if name.starts_with(LOCAL_PREFIX) {
            Some((&name[LOCAL_PREFIX.len()..], true))
        } else if name.starts_with(NORMAL_PREFIX) {
            Some((&name[NORMAL_PREFIX.len()..], false))
        } else {
            None
        }
    }
}
