use alloc::{
    borrow::Cow,
    format,
    string::{String, ToString},
};
use core::error::Error;
use sqlite_nostd::{context, sqlite3, Connection, Context, ResultCode};

use crate::bson::BsonError;

#[derive(Debug)]
pub struct SQLiteError(pub ResultCode, pub Option<Cow<'static, str>>);

impl SQLiteError {
    pub fn with_description(code: ResultCode, message: impl Into<Cow<'static, str>>) -> Self {
        Self(code, Some(message.into()))
    }

    pub fn misuse(message: impl Into<Cow<'static, str>>) -> Self {
        Self::with_description(ResultCode::MISUSE, message)
    }
}

impl core::fmt::Display for SQLiteError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "SQLiteError: {:?}", self.0)?;
        if let Some(desc) = &self.1 {
            write!(f, ", desc: {}", desc)?;
        }
        Ok(())
    }
}

impl SQLiteError {
    pub fn apply_to_ctx(self, description: &str, ctx: *mut context) {
        let SQLiteError(code, message) = self;

        if let Some(msg) = message {
            ctx.result_error(&format!("{:} {:}", description, msg));
        } else {
            let error = ctx.db_handle().errmsg().unwrap();
            if error == "not an error" {
                ctx.result_error(&format!("{:}", description));
            } else {
                ctx.result_error(&format!("{:} {:}", description, error));
            }
        }
        ctx.result_error_code(code);
    }
}

impl Error for SQLiteError {}

pub trait PSResult<T> {
    fn into_db_result(self, db: *mut sqlite3) -> Result<T, SQLiteError>;
}

impl<T> PSResult<T> for Result<T, ResultCode> {
    fn into_db_result(self, db: *mut sqlite3) -> Result<T, SQLiteError> {
        if let Err(code) = self {
            let message = db.errmsg().unwrap_or(String::from("Conversion error"));
            if message == "not an error" {
                Err(SQLiteError(code, None))
            } else {
                Err(SQLiteError(code, Some(message.into())))
            }
        } else if let Ok(r) = self {
            Ok(r)
        } else {
            Err(SQLiteError(ResultCode::ABORT, None))
        }
    }
}

impl From<ResultCode> for SQLiteError {
    fn from(value: ResultCode) -> Self {
        SQLiteError(value, None)
    }
}

impl From<serde_json::Error> for SQLiteError {
    fn from(value: serde_json::Error) -> Self {
        SQLiteError::with_description(ResultCode::ABORT, value.to_string())
    }
}

impl From<core::fmt::Error> for SQLiteError {
    fn from(value: core::fmt::Error) -> Self {
        SQLiteError::with_description(ResultCode::INTERNAL, format!("{}", value))
    }
}

impl From<BsonError> for SQLiteError {
    fn from(value: BsonError) -> Self {
        SQLiteError::with_description(ResultCode::ERROR, value.to_string())
    }
}
