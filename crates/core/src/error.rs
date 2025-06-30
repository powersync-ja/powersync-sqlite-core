use core::{error::Error, fmt::Display};

use alloc::{
    borrow::Cow,
    boxed::Box,
    string::{String, ToString},
};
use sqlite_nostd::{context, sqlite3, Connection, Context, ResultCode};
use thiserror::Error;

use crate::bson::BsonError;

/// A [RawPowerSyncError], but boxed.
///
/// We allocate errors in boxes to avoid large [Result] types (given the large size of the
/// [RawPowerSyncError] enum type).
#[derive(Debug)]
pub struct PowerSyncError {
    inner: Box<RawPowerSyncError>,
}

impl PowerSyncError {
    fn errstr(db: *mut sqlite3) -> Option<String> {
        let message = db.errmsg().unwrap_or(String::from("Conversion error"));
        if message != "not an error" {
            Some(message)
        } else {
            None
        }
    }

    pub fn from_sqlite(
        db: *mut sqlite3,
        code: ResultCode,
        context: impl Into<Cow<'static, str>>,
    ) -> Self {
        RawPowerSyncError::Sqlite(SqliteError {
            code,
            errstr: Self::errstr(db),
            context: Some(context.into()),
        })
        .into()
    }

    pub fn argument_error(desc: impl Into<Cow<'static, str>>) -> Self {
        RawPowerSyncError::ArgumentError {
            desc: desc.into(),
            cause: PowerSyncErrorCause::Unknown,
        }
        .into()
    }

    /// Converts something that can be a [PowerSyncErrorCause] into an argument error.
    ///
    /// This can be used to represent e.g. JSON parsing errors as argument errors, e.g. with
    /// ` serde_json::from_str(payload.text()).map_err(PowerSyncError::as_argument_error)`.
    pub fn as_argument_error(cause: impl Into<PowerSyncErrorCause>) -> Self {
        RawPowerSyncError::ArgumentError {
            desc: "".into(),
            cause: cause.into(),
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

    pub fn sync_protocol_error(desc: &'static str, cause: impl Into<PowerSyncErrorCause>) -> Self {
        RawPowerSyncError::SyncProtocolError {
            desc,
            cause: cause.into(),
        }
        .into()
    }

    /// A generic internal error.
    ///
    /// This should only be used rarely since this error provides no further details.
    pub fn unknown_internal() -> Self {
        Self::internal(PowerSyncErrorCause::Unknown)
    }

    /// A generic internal error with an associated cause.
    pub fn internal(cause: impl Into<PowerSyncErrorCause>) -> Self {
        RawPowerSyncError::Internal {
            cause: cause.into(),
        }
        .into()
    }

    pub fn missing_client_id() -> Self {
        RawPowerSyncError::MissingClientId.into()
    }

    pub fn down_migration_did_not_update_version(current_version: i32) -> Self {
        return RawPowerSyncError::DownMigrationDidNotUpdateVersion { current_version }.into();
    }

    /// Applies this error to a function result context, setting the error code and a descriptive
    /// text.
    pub fn apply_to_ctx(self, description: &str, ctx: *mut context) {
        let mut desc = self.to_string();
        desc.insert_str(0, description);
        desc.insert_str(description.len(), ": ");

        ctx.result_error(&desc);
        ctx.result_error_code(self.sqlite_error_code());
    }

    pub fn sqlite_error_code(&self) -> ResultCode {
        use RawPowerSyncError::*;

        match self.inner.as_ref() {
            Sqlite(desc) => desc.code,
            ArgumentError { .. } => ResultCode::CONSTRAINT_DATATYPE,
            StateError { .. } => ResultCode::MISUSE,
            MissingClientId
            | SyncProtocolError { .. }
            | DownMigrationDidNotUpdateVersion { .. } => ResultCode::ABORT,
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

impl Error for PowerSyncError {}

impl From<RawPowerSyncError> for PowerSyncError {
    fn from(value: RawPowerSyncError) -> Self {
        return PowerSyncError {
            inner: Box::new(value),
        };
    }
}

impl From<ResultCode> for PowerSyncError {
    fn from(value: ResultCode) -> Self {
        return RawPowerSyncError::Sqlite(SqliteError {
            code: value,
            errstr: None,
            context: None,
        })
        .into();
    }
}

/// A structured enumeration of possible errors that can occur in the core extension.
#[derive(Error, Debug)]
enum RawPowerSyncError {
    /// An internal call to SQLite made by the core extension has failed. We store the original
    /// result code and an optional context describing what the core extension was trying to do when
    /// the error occurred.
    ///
    /// We don't call `sqlite3_errstr` at the time the error is created. Instead, we stop using the
    /// database, bubble the error up to the outermost function/vtab definition and then use
    /// [PowerSyncError::description] to create a detailed error message.
    ///
    /// This error should _never_ be created for anything but rethrowing underlying SQLite errors.
    #[error("{0}")]
    Sqlite(SqliteError),
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
    /// server are necessarily in diverged states).
    #[error("Sync protocol error: {desc}. {cause}")]
    SyncProtocolError {
        desc: &'static str,
        cause: PowerSyncErrorCause,
    },
    /// There's invalid local data in the database (like malformed JSON in the oplog table).
    #[error("invalid local data: {cause}")]
    LocalDataError { cause: PowerSyncErrorCause },
    #[error("No client_id found in ps_kv")]
    MissingClientId,
    #[error("Down migration failed - version not updated from {current_version}")]
    DownMigrationDidNotUpdateVersion { current_version: i32 },
    /// A catch-all for remaining internal errors that are very unlikely to happen.
    #[error("Internal PowerSync error. {cause}")]
    Internal { cause: PowerSyncErrorCause },
}

#[derive(Debug)]
struct SqliteError {
    code: ResultCode,
    errstr: Option<String>,
    context: Option<Cow<'static, str>>,
}

impl Display for SqliteError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        if let Some(context) = &self.context {
            write!(f, "{}: ", context)?;
        }

        write!(f, "internal SQLite call returned {}", self.code)?;
        if let Some(desc) = &self.errstr {
            write!(f, ": {}", desc)?
        }

        Ok(())
    }
}

pub trait PSResult<T> {
    fn into_db_result(self, db: *mut sqlite3) -> Result<T, PowerSyncError>;
}

impl<T> PSResult<T> for Result<T, ResultCode> {
    fn into_db_result(self, db: *mut sqlite3) -> Result<T, PowerSyncError> {
        self.map_err(|code| {
            RawPowerSyncError::Sqlite(SqliteError {
                code,
                errstr: PowerSyncError::errstr(db),
                context: None,
            })
            .into()
        })
    }
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
