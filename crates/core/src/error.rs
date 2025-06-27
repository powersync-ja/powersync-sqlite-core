use core::fmt::Display;

use alloc::{
    borrow::Cow,
    boxed::Box,
    format,
    string::{String, ToString},
};
use sqlite_nostd::{context, sqlite3, Connection, Context, ResultCode};
use thiserror::Error;

use crate::bson::BsonError;

/// A [RawPowerSyncError], but boxed.
///
/// We allocate errors in boxes to avoid large [Result] types returning these.
pub struct PowerSyncError {
    inner: Box<RawPowerSyncError>,
}

impl PowerSyncError {
    pub fn from_sqlite(code: ResultCode, context: impl Into<Cow<'static, str>>) -> Self {
        RawPowerSyncError::Sqlite {
            code,
            context: Some(context.into()),
        }
        .into()
    }

    pub fn argument_error(desc: &'static str) -> Self {
        RawPowerSyncError::ArgumentError { desc }.into()
    }

    pub fn state_error(desc: &'static str) -> Self {
        RawPowerSyncError::StateError { desc }.into()
    }

    pub fn apply_to_ctx(self, description: &str, ctx: *mut context) {
        let mut desc = self.description(ctx.db_handle());
        desc.insert_str(0, description);
        desc.insert_str(description.len(), ": ");

        ctx.result_error(&desc);
        ctx.result_error_code(self.sqlite_error_code());
    }

    /// Obtains a description of this error, fetching it from SQLite if necessary.
    pub fn description(&self, db: *mut sqlite3) -> String {
        if let RawPowerSyncError::Sqlite { .. } = &*self.inner {
            let message = db.errmsg().unwrap_or(String::from("Conversion error"));
            if message != "not an error" {
                return format!("{}, caused by: {message}", self.inner);
            }
        }

        self.inner.to_string()
    }

    pub fn sqlite_error_code(&self) -> ResultCode {
        use RawPowerSyncError::*;

        match self.inner.as_ref() {
            Sqlite { code, .. } => *code,
            InvalidPendingStatement { .. }
            | InvalidBucketPriority
            | ExpectedJsonObject
            | ArgumentError { .. }
            | StateError { .. }
            | JsonObjectTooBig
            | CrudVtabOutsideOfTransaction => ResultCode::MISUSE,
            MissingClientId | Internal => ResultCode::ABORT,
            JsonError { .. } | BsonError { .. } => ResultCode::CONSTRAINT_DATATYPE,
        }
    }
}

impl Display for PowerSyncError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        self.inner.fmt(f)
    }
}

impl From<RawPowerSyncError> for PowerSyncError {
    fn from(value: RawPowerSyncError) -> Self {
        return PowerSyncError {
            inner: Box::new(value),
        };
    }
}

impl From<ResultCode> for PowerSyncError {
    fn from(value: ResultCode) -> Self {
        return RawPowerSyncError::Sqlite {
            code: value,
            context: None,
        }
        .into();
    }
}

impl From<serde_json::Error> for PowerSyncError {
    fn from(value: serde_json::Error) -> Self {
        RawPowerSyncError::JsonError(value).into()
    }
}

impl From<BsonError> for PowerSyncError {
    fn from(value: BsonError) -> Self {
        RawPowerSyncError::from(value).into()
    }
}

#[derive(Error, Debug)]
pub enum RawPowerSyncError {
    #[error("internal SQLite call returned {code}")]
    Sqlite {
        code: ResultCode,
        context: Option<Cow<'static, str>>,
    },
    #[error("invalid argument: {desc}")]
    ArgumentError { desc: &'static str },
    #[error("invalid state: {desc}")]
    StateError { desc: &'static str },
    #[error("Function required a JSON object, but got another type of JSON value")]
    ExpectedJsonObject,
    #[error("No client_id found in ps_kv")]
    MissingClientId,
    #[error("Invalid pending statement for raw table: {description}")]
    InvalidPendingStatement { description: Cow<'static, str> },
    #[error("Invalid bucket priority value")]
    InvalidBucketPriority,
    #[error("Internal PowerSync error")]
    Internal,
    #[error("Error decoding JSON: {0}")]
    JsonError(serde_json::Error),
    #[error("Error decoding BSON")]
    BsonError {
        #[from]
        source: BsonError,
    },
    #[error("Too many arguments passed to json_object_fragment")]
    JsonObjectTooBig,
    #[error("No tx_id")]
    CrudVtabOutsideOfTransaction,
}
