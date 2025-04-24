use core::marker::PhantomData;

use alloc::{
    string::{String, ToString},
    vec::Vec,
};
use streaming_iterator::StreamingIterator;

use crate::error::SQLiteError;
use sqlite::{Connection, ResultCode};
use sqlite_nostd::{self as sqlite, ManagedStmt};

pub struct TableInfo {
    pub name: String,
    pub view_name: String,
    pub diff_include_old: Option<DiffIncludeOld>,
    pub flags: TableInfoFlags,
}

impl TableInfo {
    pub fn parse_from(db: *mut sqlite::sqlite3, data: &str) -> Result<TableInfo, SQLiteError> {
        // language=SQLite
        let statement = db.prepare_v2(
            "SELECT
        json_extract(?1, '$.name'),
        ifnull(json_extract(?1, '$.view_name'), json_extract(?1, '$.name')),
        json_extract(?1, '$.local_only'),
        json_extract(?1, '$.insert_only'),
        json_extract(?1, '$.include_old'),
        json_extract(?1, '$.include_metadata'),
        json_extract(?1, '$.include_old_only_when_changed')",
        )?;
        statement.bind_text(1, data, sqlite::Destructor::STATIC)?;

        let step_result = statement.step()?;
        if step_result != ResultCode::ROW {
            return Err(SQLiteError::from(ResultCode::SCHEMA));
        }

        let name = statement.column_text(0)?.to_string();
        let view_name = statement.column_text(1)?.to_string();
        let flags = {
            let local_only = statement.column_int(2) != 0;
            let insert_only = statement.column_int(3) != 0;
            let include_metadata = statement.column_int(5) != 0;
            let include_old_only_when_changed = statement.column_int(6) != 0;

            let mut flags = TableInfoFlags::default();
            flags = flags.set_flag(TableInfoFlags::LOCAL_ONLY, local_only);
            flags = flags.set_flag(TableInfoFlags::INSERT_ONLY, insert_only);
            flags = flags.set_flag(TableInfoFlags::INCLUDE_METADATA, include_metadata);
            flags = flags.set_flag(
                TableInfoFlags::INCLUDE_OLD_ONLY_WHEN_CHANGED,
                include_old_only_when_changed,
            );

            flags
        };

        let include_old = match statement.column_type(4)? {
            sqlite_nostd::ColumnType::Text => {
                let columns: Vec<String> = serde_json::from_str(statement.column_text(4)?)?;
                Some(DiffIncludeOld::OnlyForColumns { columns })
            }

            sqlite_nostd::ColumnType::Integer => {
                if statement.column_int(4) != 0 {
                    Some(DiffIncludeOld::ForAllColumns)
                } else {
                    None
                }
            }
            _ => None,
        };

        // Don't allow include_metadata for local_only tables, it breaks our trigger setup and makes
        // no sense because these changes are never inserted into ps_crud.
        if flags.include_metadata() && flags.local_only() {
            return Err(SQLiteError(
                ResultCode::ERROR,
                Some("include_metadata and local_only are incompatible".to_string()),
            ));
        }

        return Ok(TableInfo {
            name,
            view_name,
            diff_include_old: include_old,
            flags,
        });
    }
}

pub enum DiffIncludeOld {
    OnlyForColumns { columns: Vec<String> },
    ForAllColumns,
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct TableInfoFlags(u32);

impl TableInfoFlags {
    pub const LOCAL_ONLY: u32 = 1;
    pub const INSERT_ONLY: u32 = 2;
    pub const INCLUDE_METADATA: u32 = 4;
    pub const INCLUDE_OLD_ONLY_WHEN_CHANGED: u32 = 8;

    pub const fn local_only(self) -> bool {
        self.0 & Self::LOCAL_ONLY != 0
    }

    pub const fn insert_only(self) -> bool {
        self.0 & Self::INSERT_ONLY != 0
    }

    pub const fn include_metadata(self) -> bool {
        self.0 & Self::INCLUDE_METADATA != 0
    }

    pub const fn include_old_only_when_changed(self) -> bool {
        self.0 & Self::INCLUDE_OLD_ONLY_WHEN_CHANGED != 0
    }

    const fn with_flag(self, flag: u32) -> Self {
        Self(self.0 | flag)
    }

    const fn without_flag(self, flag: u32) -> Self {
        Self(self.0 & !flag)
    }

    const fn set_flag(self, flag: u32, enable: bool) -> Self {
        if enable {
            self.with_flag(flag)
        } else {
            self.without_flag(flag)
        }
    }
}

impl Default for TableInfoFlags {
    fn default() -> Self {
        Self(0)
    }
}

pub struct ColumnNameAndTypeStatement<'a> {
    pub stmt: ManagedStmt,
    table: PhantomData<&'a str>,
}

impl ColumnNameAndTypeStatement<'_> {
    pub fn new(db: *mut sqlite::sqlite3, table: &str) -> Result<Self, ResultCode> {
        let stmt = db.prepare_v2("select json_extract(e.value, '$.name'), json_extract(e.value, '$.type') from json_each(json_extract(?, '$.columns')) e")?;
        stmt.bind_text(1, table, sqlite::Destructor::STATIC)?;

        Ok(Self {
            stmt,
            table: PhantomData,
        })
    }

    fn step(stmt: &ManagedStmt) -> Result<Option<ColumnInfo>, ResultCode> {
        if stmt.step()? == ResultCode::ROW {
            let name = stmt.column_text(0)?;
            let type_name = stmt.column_text(1)?;

            return Ok(Some(ColumnInfo { name, type_name }));
        }

        Ok(None)
    }

    pub fn streaming_iter(
        &mut self,
    ) -> impl StreamingIterator<Item = Result<ColumnInfo, ResultCode>> {
        streaming_iterator::from_fn(|| match Self::step(&self.stmt) {
            Err(e) => Some(Err(e)),
            Ok(Some(other)) => Some(Ok(other)),
            Ok(None) => None,
        })
    }

    pub fn names_iter(&mut self) -> impl StreamingIterator<Item = Result<&str, ResultCode>> {
        self.streaming_iter().map(|item| match item {
            Ok(row) => Ok(row.name),
            Err(e) => Err(*e),
        })
    }
}

#[derive(Clone)]
pub struct ColumnInfo<'a> {
    pub name: &'a str,
    pub type_name: &'a str,
}
