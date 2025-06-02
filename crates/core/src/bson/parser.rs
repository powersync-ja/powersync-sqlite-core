use core::ffi::CStr;

use super::{error::ErrorKind, BsonError};
use num_traits::{FromBytes, Num};

pub struct Parser<'de> {
    offset: usize,
    remaining_input: &'de [u8],
}

impl<'de> Parser<'de> {
    pub fn new(source: &'de [u8]) -> Self {
        Self {
            offset: 0,
            remaining_input: source,
        }
    }

    #[cold]
    pub fn error(&self, kind: ErrorKind) -> BsonError {
        BsonError::new(Some(self.offset), kind)
    }

    /// Advances the position of the parser, panicking on bound errors.
    fn advance(&mut self, by: usize) {
        self.offset = self.offset.strict_add(by);
        self.remaining_input = &self.remaining_input[by..];
    }

    /// Reads a sized buffer from the parser and advances the input accordingly.
    ///
    /// This returns an error if not enough bytes are left in the input.
    fn advance_checked(&mut self, size: usize) -> Result<&'de [u8], BsonError> {
        let (taken, rest) = self
            .remaining_input
            .split_at_checked(size)
            .ok_or_else(|| self.error(ErrorKind::UnexpectedEoF))?;

        self.offset += size;
        self.remaining_input = rest;
        Ok(taken)
    }

    fn advance_byte(&mut self) -> Result<u8, BsonError> {
        let value = *self
            .remaining_input
            .split_off_first()
            .ok_or_else(|| self.error(ErrorKind::UnexpectedEoF))?;

        Ok(value)
    }

    fn advance_bytes<const N: usize>(&mut self) -> Result<&'de [u8; N], BsonError> {
        let bytes = self.advance_checked(N)?;
        Ok(bytes.try_into().expect("should have correct length"))
    }

    pub fn read_cstr(&mut self) -> Result<&'de str, BsonError> {
        let raw = CStr::from_bytes_until_nul(self.remaining_input)
            .map_err(|_| self.error(ErrorKind::UnterminatedCString))?;
        let str = raw
            .to_str()
            .map_err(|e| self.error(ErrorKind::InvalidCString(e)))?;

        self.advance(str.len() + 1);
        Ok(str)
    }

    fn read_number<const N: usize, T: Num + FromBytes<Bytes = [u8; N]>>(
        &mut self,
    ) -> Result<T, BsonError> {
        let bytes = self.advance_bytes::<N>()?;
        Ok(T::from_le_bytes(&bytes))
    }

    pub fn read_int32(&mut self) -> Result<i32, BsonError> {
        self.read_number()
    }

    fn read_length(&mut self) -> Result<usize, BsonError> {
        let raw = self.read_int32()?;
        u32::try_from(raw)
            .and_then(usize::try_from)
            .map_err(|_| self.error(ErrorKind::InvalidSize))
    }

    pub fn read_int64(&mut self) -> Result<i64, BsonError> {
        self.read_number()
    }

    pub fn read_uint64(&mut self) -> Result<u64, BsonError> {
        self.read_number()
    }

    pub fn read_double(&mut self) -> Result<f64, BsonError> {
        self.read_number()
    }

    pub fn read_bool(&mut self) -> Result<bool, BsonError> {
        let byte = self.advance_byte()?;
        Ok(byte != 0)
    }

    pub fn read_object_id(&mut self) -> Result<&'de [u8], BsonError> {
        self.advance_checked(12)
    }

    /// Reads a BSON string, `string ::= int32 (byte*) unsigned_byte(0)`
    pub fn read_string(&mut self) -> Result<&'de str, BsonError> {
        let length_including_null = self.read_length()?;
        let bytes = self.advance_checked(length_including_null)?;

        str::from_utf8(&bytes[..length_including_null - 1])
            .map_err(|e| self.error(ErrorKind::InvalidCString(e)))
    }

    pub fn read_binary(&mut self) -> Result<(BinarySubtype, &'de [u8]), BsonError> {
        let length = self.read_length()?;
        let subtype = self.advance_byte()?;
        let binary = self.advance_checked(length)?;

        Ok((BinarySubtype(subtype), binary))
    }

    pub fn read_element_type(&mut self) -> Result<ElementType, BsonError> {
        let raw_type = self.advance_byte()? as i8;
        Ok(match raw_type {
            1 => ElementType::Double,
            2 => ElementType::String,
            3 => ElementType::Document,
            4 => ElementType::Array,
            5 => ElementType::Binary,
            6 => ElementType::Undefined,
            7 => ElementType::ObjectId,
            8 => ElementType::Boolean,
            9 => ElementType::DatetimeUtc,
            10 => ElementType::Null,
            16 => ElementType::Int32,
            17 => ElementType::Timestamp,
            18 => ElementType::Int64,
            _ => return Err(self.error(ErrorKind::UnknownElementType(raw_type))),
        })
    }

    fn subreader(&mut self, len: usize) -> Result<Parser<'de>, BsonError> {
        let current_offset = self.offset;
        let for_sub_reader = self.advance_checked(len)?;
        Ok(Parser {
            offset: current_offset,
            remaining_input: for_sub_reader,
        })
    }

    /// Reads a document header and skips over the contents of the document.
    ///
    /// Returns a new [Parser] that can only read contents of the document.
    pub fn document_scope(&mut self) -> Result<Parser<'de>, BsonError> {
        let total_size = self.read_length()?;
        if total_size < 5 {
            return Err(self.error(ErrorKind::InvalidSize))?;
        }

        self.subreader(total_size - 4)
    }

    /// Skips over a document at the current offset, returning the bytes making up the document.
    pub fn skip_document(&mut self) -> Result<&'de [u8], BsonError> {
        let Some(peek_size) = self.remaining_input.get(0..4) else {
            return Err(self.error(ErrorKind::UnexpectedEoF));
        };

        let parsed_size = u32::try_from(i32::from_le_bytes(
            peek_size.try_into().expect("should have correct length"),
        ))
        .and_then(usize::try_from)
        .map_err(|_| self.error(ErrorKind::InvalidSize))?;

        if parsed_size < 5 || parsed_size >= self.remaining_input.len() {
            return Err(self.error(ErrorKind::InvalidSize))?;
        }

        Ok(self.subreader(parsed_size)?.remaining())
    }

    /// If only a single byte is left in the current scope, validate that it is a zero byte.
    ///
    /// Otherwise returns false as we haven't reached the end of a document.
    pub fn end_document(&mut self) -> Result<bool, BsonError> {
        Ok(if self.remaining_input.len() == 1 {
            let trailing_zero = self.advance_byte()?;
            if trailing_zero != 0 {
                return Err(self.error(ErrorKind::InvalidEndOfDocument));
            }

            true
        } else {
            false
        })
    }

    pub fn remaining(&self) -> &'de [u8] {
        self.remaining_input
    }
}

#[repr(transparent)]
pub struct BinarySubtype(pub u8);

#[derive(Clone, Copy, Debug)]
pub enum ElementType {
    Double = 1,
    String = 2,
    Document = 3,
    Array = 4,
    Binary = 5,
    Undefined = 6,
    ObjectId = 7,
    Boolean = 8,
    DatetimeUtc = 9,
    Null = 10,
    Int32 = 16,
    Timestamp = 17,
    Int64 = 18,
}
