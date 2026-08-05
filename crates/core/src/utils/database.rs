use core::ffi::{CStr, c_char};

use alloc::ffi::CString;
use num_traits::FromPrimitive;
use powersync_sqlite_nostd::{
    self as sqlite, ColumnType, Destructor, ManagedStmt, ResultCode, convert_rc,
};

use crate::error::{PowerSyncError, Result};

/// A safe-ish wrapper around SQLite statements, providing better errors including the causing
/// statement.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Database {
    pub sqlite: *mut sqlite::sqlite3,
}

impl From<*mut sqlite::sqlite3> for Database {
    fn from(sqlite: *mut sqlite::sqlite3) -> Self {
        Self { sqlite }
    }
}

impl Database {
    fn map_error(self, code: ResultCode, sql: Option<&str>) -> PowerSyncError {
        PowerSyncError::from_sqlite(self.sqlite, code, sql)
    }

    fn map_error_cstr(self, code: ResultCode, sql: Option<&CStr>) -> PowerSyncError {
        PowerSyncError::from_sqlite(self.sqlite, code, sql)
    }

    pub fn use_inner<T>(
        self,
        inner: impl FnOnce(*mut sqlite::sqlite3) -> core::result::Result<T, ResultCode>,
    ) -> Result<T> {
        inner(self.sqlite).map_err(|e| self.map_error(e, None))
    }

    pub fn get_autocommit(self) -> bool {
        sqlite::get_autocommit(self.sqlite) != 0
    }

    pub fn prepare_v2(self, sql: &str) -> Result<Statement> {
        self.prepare_v3(sql, 0)
    }

    pub fn prepare_v3(self, sql: &str, flags: u32) -> Result<Statement> {
        let mut stmt = core::ptr::null_mut();
        let mut tail = core::ptr::null();
        let rc = ResultCode::from_i32(sqlite::prepare_v3(
            self.sqlite,
            sql.as_ptr() as *const c_char,
            sql.len() as i32,
            flags,
            &mut stmt as *mut *mut sqlite::stmt,
            &mut tail as *mut *const c_char,
        ))
        .unwrap();
        if rc == ResultCode::OK {
            Ok(Statement {
                db: self,
                stmt: ManagedStmt { stmt },
            })
        } else {
            Err(self.map_error(rc, Some(sql)))
        }
    }

    pub fn exec_safe(self, sql: &CStr) -> Result<()> {
        convert_rc(sqlite::exec(self.sqlite, sql.as_ptr()))
            .map_err(|e| self.map_error_cstr(e, Some(sql)))?;
        Ok(())
    }

    pub fn exec_safe_str(self, sql: &str) -> Result<()> {
        self.exec_safe(&CString::new(sql)?)
    }

    pub fn exec_text(self, sql: &str, param: &str) -> Result<()> {
        let statement = self.prepare_v2(sql)?;
        statement.bind_text(1, param, Destructor::STATIC)?;
        statement.exec()
    }
}

pub struct Statement {
    db: Database,
    stmt: ManagedStmt,
}

impl Statement {
    pub fn map_error(&self, code: ResultCode) -> PowerSyncError {
        let sql_ptr = sqlite::sql(self.stmt.stmt);
        let str = if sql_ptr.is_null() {
            None
        } else {
            Some(unsafe { CStr::from_ptr(sql_ptr) })
        };

        self.db.map_error_cstr(code, str)
    }

    pub fn step(&self) -> Result<bool> {
        let rc = ResultCode::from_i32(sqlite::step(self.stmt.stmt)).unwrap();

        match rc {
            ResultCode::ROW => Ok(true),
            ResultCode::DONE => Ok(false),
            _ => Err(self.map_error(rc)),
        }
    }

    pub fn bind_parameter_count(&self) -> usize {
        self.stmt.bind_parameter_count() as usize
    }

    pub fn bind_text(&self, i: i32, text: &str, d: Destructor) -> Result<()> {
        self.stmt
            .bind_text(i, text, d)
            .map_err(|e| self.map_error(e))?;
        Ok(())
    }

    pub fn bind_int(&self, i: i32, val: i32) -> Result<()> {
        self.stmt.bind_int(i, val).map_err(|e| self.map_error(e))?;
        Ok(())
    }

    pub fn bind_int64(&self, i: i32, val: i64) -> Result<()> {
        self.stmt
            .bind_int64(i, val)
            .map_err(|e| self.map_error(e))?;
        Ok(())
    }

    pub fn bind_double(&self, i: i32, val: f64) -> Result<()> {
        self.stmt
            .bind_double(i, val)
            .map_err(|e| self.map_error(e))?;
        Ok(())
    }

    pub fn bind_null(&self, i: i32) -> Result<()> {
        self.stmt.bind_null(i).map_err(|e| self.map_error(e))?;
        Ok(())
    }

    /// Calls [read] to read a column if it's not null, otherwise returns [None].
    #[inline]
    pub fn column_nullable<T, R: FnOnce() -> Result<T>>(
        &self,
        index: i32,
        read: R,
    ) -> Result<Option<T>> {
        if self.stmt.column_type(index) == ColumnType::Null {
            Ok(None)
        } else {
            Ok(Some(read()?))
        }
    }

    pub fn column_text(&self, i: i32) -> Result<&str> {
        self.stmt.column_text(i).map_err(|e| self.map_error(e))
    }

    pub fn column_int(&self, i: i32) -> i32 {
        self.stmt.column_int(i)
    }

    pub fn column_int64(&self, i: i32) -> i64 {
        self.stmt.column_int64(i)
    }

    pub fn reset(&self) -> Result<()> {
        self.stmt.reset().map_err(|e| self.map_error(e))?;
        Ok(())
    }

    pub fn exec(&self) -> Result<()> {
        let result = loop {
            break match self.step() {
                Ok(row) => {
                    if row {
                        continue;
                    };
                    Ok(())
                }
                Err(e) => Err(e),
            };
        };

        self.reset()?;
        result
    }
}
