use core::ffi::CStr;

use super::{BsonError, error::ErrorKind};
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_read_int64_negative_values() {
        let neg_one_bytes = (-1i64).to_le_bytes();
        let mut parser = Parser::new(&neg_one_bytes);
        assert_eq!(parser.read_int64().unwrap(), -1);

        let min_bytes = (i64::MIN).to_le_bytes();
        let mut parser = Parser::new(&min_bytes);
        assert_eq!(parser.read_int64().unwrap(), i64::MIN);

        let neg_42_bytes = (-42i64).to_le_bytes();
        let mut parser = Parser::new(&neg_42_bytes);
        assert_eq!(parser.read_int64().unwrap(), -42);
    }

    #[test]
    fn test_read_int32_negative_values() {
        let neg_one_bytes = (-1i32).to_le_bytes();
        let mut parser = Parser::new(&neg_one_bytes);
        assert_eq!(parser.read_int32().unwrap(), -1);

        let min_bytes = (i32::MIN).to_le_bytes();
        let mut parser = Parser::new(&min_bytes);
        assert_eq!(parser.read_int32().unwrap(), i32::MIN);

        let neg_42_bytes = (-42i32).to_le_bytes();
        let mut parser = Parser::new(&neg_42_bytes);
        assert_eq!(parser.read_int32().unwrap(), -42);
    }

    #[test]
    fn test_read_double_negative_and_special() {
        let neg_pi_bytes = (-3.14159f64).to_le_bytes();
        let mut parser = Parser::new(&neg_pi_bytes);
        let val = parser.read_double().unwrap();
        assert!((val - (-3.14159)).abs() < 0.00001);

        let neg_inf_bytes = f64::NEG_INFINITY.to_le_bytes();
        let mut parser = Parser::new(&neg_inf_bytes);
        assert_eq!(parser.read_double().unwrap(), f64::NEG_INFINITY);

        let nan_bytes = f64::NAN.to_le_bytes();
        let mut parser = Parser::new(&nan_bytes);
        assert!(parser.read_double().unwrap().is_nan());
    }

    #[test]
    fn test_read_bool_edge_cases() {
        let mut parser = Parser::new(&[0x00]);
        assert_eq!(parser.read_bool().unwrap(), false);

        let mut parser = Parser::new(&[0x01]);
        assert_eq!(parser.read_bool().unwrap(), true);

        let mut parser = Parser::new(&[0xFF]);
        assert_eq!(parser.read_bool().unwrap(), true);

        let mut parser = Parser::new(&[0x7F]);
        assert_eq!(parser.read_bool().unwrap(), true);
    }

    #[test]
    fn test_read_string_empty() {
        // Empty string: length=1, content=null terminator
        let data = &[0x01, 0x00, 0x00, 0x00, 0x00];
        let mut parser = Parser::new(data);
        assert_eq!(parser.read_string().unwrap(), "");
    }

    #[test]
    fn test_read_string_unicode() {
        // String "🦀" (4 UTF-8 bytes + null terminator)
        let data = &[0x05, 0x00, 0x00, 0x00, 0xf0, 0x9f, 0xa6, 0x80, 0x00];
        let mut parser = Parser::new(data);
        assert_eq!(parser.read_string().unwrap(), "🦀");
    }

    #[test]
    fn test_read_cstr_empty() {
        let data = &[0x00];
        let mut parser = Parser::new(data);
        assert_eq!(parser.read_cstr().unwrap(), "");
    }

    #[test]
    fn test_read_cstr_unicode() {
        let data = &[0xf0, 0x9f, 0xa6, 0x80, 0x00]; // "🦀\0"
        let mut parser = Parser::new(data);
        assert_eq!(parser.read_cstr().unwrap(), "🦀");
    }

    #[test]
    fn test_element_type_all_valid() {
        let valid_types = [
            (1, ElementType::Double),
            (2, ElementType::String),
            (3, ElementType::Document),
            (4, ElementType::Array),
            (5, ElementType::Binary),
            (6, ElementType::Undefined),
            (7, ElementType::ObjectId),
            (8, ElementType::Boolean),
            (9, ElementType::DatetimeUtc),
            (10, ElementType::Null),
            (16, ElementType::Int32),
            (17, ElementType::Timestamp),
            (18, ElementType::Int64),
        ];

        for (byte, expected) in valid_types {
            let data = [byte];
            let mut parser = Parser::new(&data);
            let result = parser.read_element_type().unwrap();
            assert_eq!(result as u8, expected as u8);
        }
    }

    #[test]
    fn test_element_type_invalid() {
        let invalid_types = [0, 11, 12, 13, 14, 15, 19, 20, 99, 255];

        for invalid_type in invalid_types {
            let data = [invalid_type];
            let mut parser = Parser::new(&data);
            let result = parser.read_element_type();
            assert!(result.is_err());
        }
    }

    #[test]
    fn test_document_scope_minimum_size() {
        // Minimum valid document: 5 bytes total
        let data = &[0x05, 0x00, 0x00, 0x00, 0x00];
        let mut parser = Parser::new(data);
        let sub_parser = parser.document_scope().unwrap();
        assert_eq!(sub_parser.remaining().len(), 1); // Just the terminator
    }

    #[test]
    fn test_document_scope_invalid_size() {
        // Document claiming size < 5
        let data = &[0x04, 0x00, 0x00, 0x00];
        let mut parser = Parser::new(data);
        assert!(parser.document_scope().is_err());
    }

    #[test]
    fn test_binary_data_empty() {
        // Binary with length 0, subtype 0
        let data = &[0x00, 0x00, 0x00, 0x00, 0x00];
        let mut parser = Parser::new(data);
        let (subtype, binary) = parser.read_binary().unwrap();
        assert_eq!(subtype.0, 0);
        assert_eq!(binary.len(), 0);
    }

    #[test]
    fn test_binary_data_with_content() {
        // Binary with length 3, subtype 5, content [1,2,3]
        let data = &[0x03, 0x00, 0x00, 0x00, 0x05, 0x01, 0x02, 0x03];
        let mut parser = Parser::new(data);
        let (subtype, binary) = parser.read_binary().unwrap();
        assert_eq!(subtype.0, 5);
        assert_eq!(binary, &[1, 2, 3]);
    }

    #[test]
    fn test_object_id_exact_size() {
        let data = &[
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c,
        ];
        let mut parser = Parser::new(data);
        let oid = parser.read_object_id().unwrap();
        assert_eq!(oid, data);
    }

    #[test]
    fn test_advance_checked_boundary() {
        let data = &[0x01, 0x02, 0x03];
        let mut parser = Parser::new(data);

        // Should succeed
        assert!(parser.advance_checked(3).is_ok());
        assert_eq!(parser.remaining().len(), 0);

        // Should fail - no more data
        assert!(parser.advance_checked(1).is_err());
    }

    #[test]
    fn test_end_document_valid() {
        let data = &[0x00];
        let mut parser = Parser::new(data);
        assert_eq!(parser.end_document().unwrap(), true);
        assert_eq!(parser.remaining().len(), 0);
    }

    #[test]
    fn test_end_document_invalid_terminator() {
        let data = &[0x01];
        let mut parser = Parser::new(data);
        assert!(parser.end_document().is_err());
    }

    #[test]
    fn test_end_document_not_at_end() {
        let data = &[0x01, 0x02, 0x03];
        let mut parser = Parser::new(data);
        assert_eq!(parser.end_document().unwrap(), false);
    }

    // Error boundary tests

    #[test]
    fn test_unexpected_eof_int32() {
        let data = &[0x01, 0x02]; // Only 2 bytes, need 4
        let mut parser = Parser::new(data);
        assert!(parser.read_int32().is_err());
    }

    #[test]
    fn test_unexpected_eof_int64() {
        let data = &[0x01, 0x02, 0x03, 0x04]; // Only 4 bytes, need 8
        let mut parser = Parser::new(data);
        assert!(parser.read_int64().is_err());
    }

    #[test]
    fn test_unexpected_eof_double() {
        let data = &[0x01, 0x02, 0x03, 0x04]; // Only 4 bytes, need 8
        let mut parser = Parser::new(data);
        assert!(parser.read_double().is_err());
    }

    #[test]
    fn test_unexpected_eof_object_id() {
        let data = &[0x01, 0x02, 0x03, 0x04]; // Only 4 bytes, need 12
        let mut parser = Parser::new(data);
        assert!(parser.read_object_id().is_err());
    }

    #[test]
    fn test_string_length_overflow() {
        // Invalid negative length
        let data = &[0xff, 0xff, 0xff, 0xff, 0x00];
        let mut parser = Parser::new(data);
        assert!(parser.read_string().is_err());
    }

    #[test]
    fn test_string_insufficient_data() {
        // Claims length 10 but only has 5 bytes total
        let data = &[0x0a, 0x00, 0x00, 0x00, 0x00];
        let mut parser = Parser::new(data);
        assert!(parser.read_string().is_err());
    }

    #[test]
    fn test_binary_length_overflow() {
        // Invalid negative length
        let data = &[0xff, 0xff, 0xff, 0xff, 0x00];
        let mut parser = Parser::new(data);
        assert!(parser.read_binary().is_err());
    }

    #[test]
    fn test_binary_insufficient_data() {
        // Claims length 10 but only has 2 bytes after subtype
        let data = &[0x0a, 0x00, 0x00, 0x00, 0x05, 0x01, 0x02];
        let mut parser = Parser::new(data);
        assert!(parser.read_binary().is_err());
    }

    #[test]
    fn test_cstr_unterminated() {
        let data = &[0x48, 0x65, 0x6c, 0x6c, 0x6f]; // "Hello" without null terminator
        let mut parser = Parser::new(data);
        assert!(parser.read_cstr().is_err());
    }

    #[test]
    fn test_invalid_utf8_string() {
        // Invalid UTF-8 sequence in string
        let data = &[0x05, 0x00, 0x00, 0x00, 0xff, 0xfe, 0xfd, 0xfc, 0x00];
        let mut parser = Parser::new(data);
        assert!(parser.read_string().is_err());
    }

    #[test]
    fn test_invalid_utf8_cstr() {
        // Invalid UTF-8 sequence in cstring
        let data = &[0xff, 0xfe, 0xfd, 0xfc, 0x00];
        let mut parser = Parser::new(data);
        assert!(parser.read_cstr().is_err());
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
