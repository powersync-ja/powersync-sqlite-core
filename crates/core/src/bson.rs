use alloc::string::{String, ToString};
use core::error::Error as CoreError;
use core::ffi::CStr;
use core::fmt::Display;

use serde::de::{
    self, DeserializeSeed, EnumAccess, IntoDeserializer, MapAccess, SeqAccess, StdError,
    VariantAccess, Visitor,
};
use serde::{forward_to_deserialize_any, Deserialize};

pub struct Parser<'de> {
    offset: usize,
    remaining_input: &'de [u8],
}

impl<'de> Parser<'de> {
    fn error(&self, kind: ErrorKind) -> BsonError {
        BsonError {
            offset: Some(self.offset),
            kind: kind,
        }
    }

    fn advance(&mut self, by: usize) {
        self.offset = self.offset.strict_add(by);
        self.remaining_input = &self.remaining_input[by..];
    }

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
        let slice = self.advance_checked(1)?;
        Ok(slice[0])
    }

    fn read_cstr(&mut self) -> Result<&'de str, BsonError> {
        let raw = CStr::from_bytes_until_nul(self.remaining_input)
            .map_err(|_| self.error(ErrorKind::UnterminatedCString))?;
        let str = raw
            .to_str()
            .map_err(|_| self.error(ErrorKind::InvalidCString))?;

        self.advance(str.len() + 1);
        Ok(str)
    }

    fn read_int32(&mut self) -> Result<i32, BsonError> {
        let slice = self.advance_checked(4)?;
        Ok(i32::from_le_bytes(
            slice.try_into().expect("should have correct length"),
        ))
    }

    fn read_length(&mut self) -> Result<usize, BsonError> {
        let raw = self.read_int32()?;
        u32::try_from(raw)
            .and_then(usize::try_from)
            .map_err(|_| self.error(ErrorKind::InvalidSize))
    }

    fn read_int64(&mut self) -> Result<i64, BsonError> {
        let slice = self.advance_checked(8)?;
        Ok(i64::from_le_bytes(
            slice.try_into().expect("should have correct length"),
        ))
    }

    fn read_double(&mut self) -> Result<f64, BsonError> {
        let slice = self.advance_checked(8)?;
        Ok(f64::from_le_bytes(
            slice.try_into().expect("should have correct length"),
        ))
    }

    /// Reads a BSON string, `string ::= int32 (byte*) unsigned_byte(0)`
    fn read_string(&mut self) -> Result<&'de str, BsonError> {
        let length_including_null = self.read_length()?;
        let bytes = self.advance_checked(length_including_null)?;

        str::from_utf8(&bytes[..length_including_null - 1])
            .map_err(|_| self.error(ErrorKind::InvalidCString))
    }

    fn read_binary(&mut self) -> Result<(BinarySubtype, &'de [u8]), BsonError> {
        let length = self.read_length()?;
        let subtype = self.advance_byte()?;
        let binary = self.advance_checked(length)?;

        Ok((BinarySubtype(subtype), binary))
    }

    fn read_element_type(&mut self) -> Result<ElementType, BsonError> {
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

    fn document_scope(&mut self) -> Result<Parser<'de>, BsonError> {
        let total_size = self.read_length()?;
        if total_size < 5 {
            return Err(self.error(ErrorKind::InvalidSize))?;
        }

        self.subreader(total_size - 4)
    }
}

#[repr(transparent)]
struct BinarySubtype(pub u8);

enum ElementType {
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

struct Deserializer<'de> {
    parser: Parser<'de>,
    is_outside_of_document: bool,
    pending_value_type: Option<ElementType>,
    consumed_name: bool,
}

impl<'de, 'a> de::Deserializer<'de> for &'a mut Deserializer<'de> {
    type Error = BsonError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        // BSON always start with a document, so we need this for the outermost visit_map.
        if self.is_outside_of_document {
            self.parser = self.parser.document_scope()?;
            self.is_outside_of_document = false;

            let object = BsonObject { de: self };
            return visitor.visit_map(object);
        }

        if !self.consumed_name {
            self.consumed_name = true;
            // We've read an element type, but not the associated name. Do that now.
            return visitor.visit_borrowed_str(self.parser.read_cstr()?);
        }

        if let Some(element_type) = self.pending_value_type.take() {
            return match element_type {
                ElementType::Double => visitor.visit_f64(self.parser.read_double()?),
                ElementType::String => visitor.visit_borrowed_str(self.parser.read_string()?),
                ElementType::Document => {
                    let parser = self.parser.document_scope()?;
                    let mut deserializer = Deserializer {
                        parser,
                        is_outside_of_document: false,
                        pending_value_type: None,
                        consumed_name: false,
                    };
                    let object = BsonObject {
                        de: &mut deserializer,
                    };

                    visitor.visit_map(object)
                }
                ElementType::Array => todo!(),
                ElementType::Binary => {
                    let (_, bytes) = self.parser.read_binary()?;
                    visitor.visit_borrowed_bytes(bytes)
                }
                ElementType::ObjectId => todo!(),
                ElementType::Boolean => {
                    let value = self.parser.advance_byte()?;
                    visitor.visit_bool(value != 0)
                }
                ElementType::DatetimeUtc => todo!(),
                ElementType::Null | ElementType::Undefined => visitor.visit_none(),
                ElementType::Int32 => visitor.visit_i32(self.parser.read_int32()?),
                ElementType::Int64 => visitor.visit_i64(self.parser.read_int64()?),
                ElementType::Timestamp => todo!(),
            };
        }

        todo!()
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct map struct enum identifier ignored_any
    }
}
struct BsonObject<'a, 'de: 'a> {
    de: &'a mut Deserializer<'de>,
}

impl<'de, 'a> MapAccess<'de> for BsonObject<'a, 'de> {
    type Error = BsonError;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        if self.de.parser.remaining_input.len() == 1 {
            // Expect trailing 0 for document
            let trailing_zero = self.de.parser.advance_byte()?;
            if trailing_zero != 0 {
                return Err(self.de.parser.error(ErrorKind::InvalidEndOfDocument));
            }

            return Ok(None);
        }

        self.de.pending_value_type = Some(self.de.parser.read_element_type()?);
        self.de.consumed_name = false;

        Ok(Some(seed.deserialize(&mut *self.de)?))
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        debug_assert!(self.de.consumed_name);
        debug_assert!(self.de.pending_value_type.is_some());

        seed.deserialize(&mut *self.de)
    }
}

#[derive(Debug)]
pub struct BsonError {
    offset: Option<usize>,
    kind: ErrorKind,
}

#[derive(Debug)]
enum ErrorKind {
    Custom(String),
    UnknownElementType(i8),
    UnterminatedCString,
    InvalidCString,
    UnexpectedEoF,
    InvalidEndOfDocument,
    InvalidSize,
}

impl Display for BsonError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "bson error")
    }
}

impl de::Error for BsonError {
    fn custom<T>(msg: T) -> Self
    where
        T: Display,
    {
        BsonError {
            offset: None,
            kind: ErrorKind::Custom(msg.to_string()),
        }
    }
}

impl StdError for BsonError {}

pub fn from_bytes<'de, T: Deserialize<'de>>(bytes: &'de [u8]) -> Result<T, BsonError> {
    let parser = Parser {
        offset: 0,
        remaining_input: bytes,
    };
    let mut deserializer = Deserializer {
        parser,
        is_outside_of_document: true,
        pending_value_type: None,
        consumed_name: false,
    };

    T::deserialize(&mut deserializer)
}

#[cfg(feature = "std")]
#[cfg(test)]
mod test {
    extern crate std;
    use super::*;
    use bson::{Bson, Document};
    use serde::de::DeserializeOwned;

    use std::vec::Vec;
    use std::*;

    #[test]
    fn test_hello_world() {
        let mut bytes: Vec<u8> = std::vec![];
        let mut doc = Document::new();
        doc.insert("hello", "world");
        doc.to_writer(&mut bytes).expect("should serialize");

        #[derive(Deserialize)]
        struct Expected<'a> {
            hello: &'a str,
        }

        let expected: Expected = from_bytes(&bytes).expect("should deserialize");
        assert_eq!(expected.hello, "world");
    }
}
