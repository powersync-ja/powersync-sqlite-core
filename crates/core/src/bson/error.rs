use core::fmt::Display;

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
    InvalidCString,
    UnexpectedEoF,
    InvalidEndOfDocument,
    InvalidSize,
    InvalidStateExpectedType,
    InvalidStateExpectedName,
    InvalidStateExpectedValue,
    ExpectedEnum { actual: ElementType },
    ExpectedString,
    IllegalFloatToIntConversion(f64),
    UnexpectedEndOfDocumentForEnumVariant,
}

impl BsonError {
    pub fn new(offset: Option<usize>, kind: ErrorKind) -> Self {
        Self {
            err: Box::new(BsonErrorImpl { offset, kind }),
        }
    }
}

impl Display for BsonError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "bson error: {:?}", &self.err)
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
