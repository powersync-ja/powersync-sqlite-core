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

    pub fn argument_error(desc: impl Into<Cow<'static, str>>) -> Self {
        RawPowerSyncError::ArgumentError {
            desc: desc.into(),
            cause: PowerSyncErrorCause::Unknown,
        }
        .into()
    }

    pub fn json_argument_error(cause: serde_json::Error) -> Self {
        RawPowerSyncError::ArgumentError {
            desc: "".into(),
            cause: PowerSyncErrorCause::Json(cause),
        }
        .into()
    }

    pub fn json_local_error(cause: serde_json::Error) -> Self {
        RawPowerSyncError::LocalDataError {
            cause: PowerSyncErrorCause::Json(cause),
        }
        .into()
    }

    pub fn state_error(desc: &'static str) -> Self {
        RawPowerSyncError::StateError { desc }.into()
    }

    pub fn unknown_internal() -> Self {
        Self::internal(PowerSyncErrorCause::Unknown)
    }

    pub fn internal(cause: impl Into<PowerSyncErrorCause>) -> Self {
        RawPowerSyncError::Internal {
            cause: cause.into(),
        }
        .into()
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
            InvalidBucketPriority | ArgumentError { .. } | StateError { .. } => ResultCode::MISUSE,
            MissingClientId | SyncProtocolError { .. } => ResultCode::ABORT,
            LocalDataError { .. } => ResultCode::CORRUPT,
            Internal { .. } => ResultCode::INTERNAL,
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

/// A structured enumeration of possible errors that can occur in the core extension.
#[derive(Error, Debug)]
pub enum RawPowerSyncError {
    /// An internal call to SQLite made by the core extension has failed. We store the original
    /// result code and an optional context describing what the core extension was trying to do when
    /// the error occurred.
    ///
    /// We don't call `sqlite3_errstr` at the time the error is created. Instead, we stop using the
    /// database, bubble the error up to the outermost function/vtab definition and then use
    /// [PowerSyncError::description] to create a detailed error message.
    ///
    /// This error should _never_ be created for anything but rethrowing underlying SQLite errors.
    #[error("internal SQLite call returned {code}")]
    Sqlite {
        code: ResultCode,
        context: Option<Cow<'static, str>>,
    },
    /// A user (e.g. the one calling a PowerSync function, likely an SDK) has provided invalid
    /// arguments.
    ///
    /// This always indicates an error in how the core extension is used.
    #[error("invalid argument: {desc}. {cause}")]
    ArgumentError {
        desc: Cow<'static, str>,
        cause: PowerSyncErrorCause,
    },
    /// A PowerSync function or vtab was used in a state where it's unavailable.
    ///
    /// This always indicates an error in how the core extension is used.
    #[error("invalid state: {desc}")]
    StateError { desc: &'static str },
    /// We've received a sync line we couldn't parse, or in a state where it doesn't make sense
    /// (e.g. a checkpoint diff before we've ever received a checkpoint).
    ///
    /// This interrupts a sync iteration as we cannot reasonably continue afterwards (the client and
    /// server are necessarily in different states).
    #[error("Sync protocol error: {desc}. {cause}")]
    SyncProtocolError {
        desc: &'static str,
        cause: PowerSyncErrorCause,
    },
    /// There's invalid local data in the database (like malformed JSON in the oplog table).
    #[error("invalid local data")]
    LocalDataError { cause: PowerSyncErrorCause },
    #[error("No client_id found in ps_kv")]
    MissingClientId,
    #[error("Invalid bucket priority value")]
    InvalidBucketPriority,
    #[error("Internal PowerSync error. {cause}")]
    Internal { cause: PowerSyncErrorCause },
}

#[derive(Debug)]
pub enum PowerSyncErrorCause {
    Json(serde_json::Error),
    Bson(BsonError),
    Unknown,
}

impl From<serde_json::Error> for PowerSyncErrorCause {
    fn from(value: serde_json::Error) -> Self {
        return PowerSyncErrorCause::Json(value);
    }
}

impl From<BsonError> for PowerSyncErrorCause {
    fn from(value: BsonError) -> Self {
        return PowerSyncErrorCause::Bson(value);
    }
}

impl Display for PowerSyncErrorCause {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "cause: ")?;

        match self {
            PowerSyncErrorCause::Json(error) => error.fmt(f),
            PowerSyncErrorCause::Bson(error) => error.fmt(f),
            PowerSyncErrorCause::Unknown => write!(f, "unknown"),
        }
    }
}
