use core::{fmt::Display, str::Utf8Error};

use alloc::{
    boxed::Box,
    string::{String, ToString},
};
use serde::de::{self, StdError};

use super::parser::ElementType;

#[derive(Debug)]
pub struct BsonError {
    /// Using a [Box] here keeps the size of this type as small, which makes results of this error
    /// type smaller (at the cost of making errors more expensive to report, but that's fine because
    /// we expect them to be rare).
    err: Box<BsonErrorImpl>,
}

#[derive(Debug)]
struct BsonErrorImpl {
    offset: Option<usize>,
    kind: ErrorKind,
}

#[derive(Debug)]
pub enum ErrorKind {
    Custom(String),
    UnknownElementType(i8),
    UnterminatedCString,
    InvalidCString(Utf8Error),
    UnexpectedEoF,
    InvalidEndOfDocument,
    InvalidSize,
    InvalidStateExpectedType,
    InvalidStateExpectedName,
    InvalidStateExpectedValue,
    ExpectedEnum { actual: ElementType },
    ExpectedString,
    UnexpectedEndOfDocumentForEnumVariant,
}

impl BsonError {
    pub fn new(offset: Option<usize>, kind: ErrorKind) -> Self {
        Self {
            err: Box::new(BsonErrorImpl { offset, kind }),
        }
    }
}

impl core::error::Error for BsonError {}

impl Display for BsonError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        self.err.fmt(f)
    }
}

impl Display for BsonErrorImpl {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        if let Some(offset) = self.offset {
            write!(f, "bson error, at {offset}: {}", self.kind)
        } else {
            write!(f, "bson error at unknown offset: {}", self.kind)
        }
    }
}

impl Display for ErrorKind {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ErrorKind::Custom(msg) => write!(f, "custom {msg}"),
            ErrorKind::UnknownElementType(code) => write!(f, "unknown element code: {code}"),
            ErrorKind::UnterminatedCString => write!(f, "unterminated cstring"),
            ErrorKind::InvalidCString(e) => write!(f, "cstring with non-utf8 content: {e}"),
            ErrorKind::UnexpectedEoF => write!(f, "unexpected end of file"),
            ErrorKind::InvalidEndOfDocument => write!(f, "unexpected end of document"),
            ErrorKind::InvalidSize => write!(f, "invalid document size"),
            ErrorKind::InvalidStateExpectedType => write!(f, "internal state error, expected type"),
            ErrorKind::InvalidStateExpectedName => write!(f, "internal state error, expected name"),
            ErrorKind::InvalidStateExpectedValue => {
                write!(f, "internal state error, expected value")
            }
            ErrorKind::ExpectedEnum { actual } => write!(f, "expected enum, got {}", *actual as u8),
            ErrorKind::ExpectedString => write!(f, "expected a string value"),
            ErrorKind::UnexpectedEndOfDocumentForEnumVariant => {
                write!(f, "unexpected end of document for enum variant")
            }
        }
    }
}

impl de::Error for BsonError {
    fn custom<T>(msg: T) -> Self
    where
        T: Display,
    {
        BsonError::new(None, ErrorKind::Custom(msg.to_string()))
    }
}
impl StdError for BsonError {}
