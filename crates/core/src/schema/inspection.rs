use core::fmt::Write;

use alloc::borrow::ToOwned;
use alloc::vec;
use alloc::{string::String, vec::Vec};
use powersync_sqlite_nostd::Destructor;

use crate::error::Result;
use crate::schema::Table;
use crate::schema::raw_table::InferredTableStructure;
use crate::utils::database::Database;
use crate::utils::{SqlBuffer, WriteType};
use crate::views::table_columns_to_json_object;

/// An existing PowerSync-managed view that was found in the schema.
#[derive(PartialEq)]
pub struct ExistingView {
    /// The name of the view itself.
    pub key: ViewKey,
    /// SQL contents of all triggers implementing deletes by forwarding to
    /// `ps_data` and `ps_crud`.
    pub delete_trigger_sql: String,
    /// SQL contents of the trigger implementing inserts on this view.
    pub insert_trigger_sql: String,
    /// SQL contents of the trigger implementing updates on this view.
    pub update_trigger_sql: String,
}

#[derive(PartialEq)]
pub enum ViewKey {
    JsonTable {
        /// The name of the view itself.
        name: String,
        /// SQL contents of the `CREATE VIEW` statement.
        sql: String,
    },
    DirectTable {
        /// The name of the direct table for which this view has been created.
        table_name: String,
    },
}

impl ExistingView {
    pub fn list(db: Database, existing_tables: &[ExistingTable]) -> Result<Vec<Self>> {
        let mut results = vec![];

        let find_triggers = db.prepare_v2(
            "SELECT name, sql FROM sqlite_schema WHERE type = 'trigger' AND tbl_name = ? ORDER BY name DESC",
        )?;
        let find_view = db.prepare_v2(
            "SELECT sql FROM sqlite_schema WHERE type = 'view' AND name = ? AND sql GLOB '*-- powersync-auto-generated'",
        )?;

        for table in existing_tables {
            find_triggers.bind_text(1, &table.name, Destructor::STATIC)?;

            let mut insert_trigger_sql = String::new();
            let mut update_trigger_sql = String::new();
            let mut delete_trigger_sql = String::new();

            while find_triggers.step()? {
                let trigger_name = find_triggers.column_text(0)?;
                let trigger_sql = find_triggers.column_text(1)?;

                let stmt = if trigger_name.starts_with("ps_view_delete") {
                    &mut delete_trigger_sql
                } else if trigger_name.starts_with("ps_view_insert") {
                    &mut insert_trigger_sql
                } else if trigger_name.starts_with("ps_view_update") {
                    &mut update_trigger_sql
                } else {
                    continue;
                };

                if !stmt.is_empty() {
                    stmt.push_str(";\n");
                }

                stmt.push_str(trigger_sql);
            }

            find_triggers.reset()?;

            let key = if table.direct.is_some() {
                ViewKey::DirectTable {
                    table_name: table.name.clone(),
                }
            } else {
                find_view.bind_text(1, &table.name, Destructor::STATIC)?;
                let sql = if find_view.step()? {
                    find_view.column_text(0)?.to_owned()
                } else {
                    String::new()
                };
                find_view.reset()?;

                ViewKey::JsonTable {
                    name: table.name.clone(),
                    sql,
                }
            };

            results.push(ExistingView {
                key,
                delete_trigger_sql,
                insert_trigger_sql,
                update_trigger_sql,
            });
        }

        Ok(results)
    }

    pub fn name(&self) -> &str {
        match &self.key {
            ViewKey::JsonTable { name, .. } => &*name,
            ViewKey::DirectTable { table_name } => &*table_name,
        }
    }

    pub fn drop_by_name(db: Database, name: &str) -> Result<()> {
        let mut buffer = SqlBuffer::new();
        buffer.drop("VIEW", true, name);

        db.exec_safe_str(&buffer.sql)?;
        Ok(())
    }

    pub fn delete_from_db(&self, db: Database) -> Result<()> {
        match &self.key {
            ViewKey::JsonTable { name, .. } => {
                Self::drop_by_name(db, &name)?;
            }
            ViewKey::DirectTable { table_name } => {
                for write in WriteType::VALUES {
                    let mut buffer = SqlBuffer::new();
                    buffer.drop(
                        "TRIGGER",
                        true,
                        &Table::direct_trigger_name(table_name, *write),
                    );

                    db.exec_safe_str(&buffer.sql)?;
                }
            }
        }

        Ok(())
    }

    pub fn create(&self, db: Database) -> Result<()> {
        self.delete_from_db(db)?;

        if let ViewKey::JsonTable { sql, .. } = &self.key {
            db.exec_safe_str(sql)?;
        }
        db.exec_safe_str(&self.delete_trigger_sql)?;
        db.exec_safe_str(&self.insert_trigger_sql)?;
        db.exec_safe_str(&self.update_trigger_sql)?;

        Ok(())
    }
}

pub struct ExistingTable {
    pub name: String,
    pub internal_name: String,
    pub local_only: bool,
    pub direct: Option<InferredTableStructure>,
}

impl ExistingTable {
    pub fn list(db: Database) -> Result<Vec<Self>> {
        Self::list_filtered(db, false)
    }

    pub fn list_filtered(db: Database, ignore_direct: bool) -> Result<Vec<Self>> {
        let mut results = vec![];
        let stmt = db.prepare_v2("SELECT name, sql FROM sqlite_master WHERE type = 'table';")?;

        while stmt.step()? {
            let internal_name = stmt.column_text(0)?;
            let Ok(sql) = stmt.column_text(1) else {
                continue;
            };

            if let Some((name, local_only)) = Self::external_name(internal_name) {
                results.push(ExistingTable {
                    internal_name: internal_name.to_owned(),
                    name: name.to_owned(),
                    local_only: local_only,
                    direct: None,
                });
            } else if sql.contains("/* ps-managed") && !ignore_direct {
                results.push(ExistingTable {
                    internal_name: internal_name.to_owned(),
                    name: internal_name.to_owned(),
                    local_only: sql.contains("local-only"),
                    direct: Some(InferredTableStructure::read_from_database(
                        internal_name,
                        db,
                        &None,
                    )?),
                });
            }
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

    pub fn move_into_ps_untyped(&self, db: Database) -> Result<()> {
        if self.local_only {
            return Ok(());
        }

        let mut buffer = SqlBuffer::new();
        buffer.push_str("INSERT INTO ps_untyped(type, id, data) SELECT ?, id, ");

        if let Some(ref schema) = self.direct {
            buffer.push_str(&table_columns_to_json_object(
                &self.internal_name,
                &schema.columns,
            )?);
        } else {
            buffer.push_str("data");
        }

        buffer.push_str(" FROM ");
        let _ = buffer.identifier().write_str(&self.internal_name);

        db.exec_text(&buffer.sql, &self.name)
    }
}
