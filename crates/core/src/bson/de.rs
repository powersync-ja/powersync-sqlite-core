use serde::{
    de::{self, DeserializeSeed, MapAccess, Visitor},
    forward_to_deserialize_any,
};

use super::{
    parser::{ElementType, Parser},
    BsonError,
};

pub struct Deserializer<'de> {
    parser: Parser<'de>,
    is_outside_of_document: bool,
    pending_value_type: Option<ElementType>,
    consumed_name: bool,
}

impl<'de> Deserializer<'de> {
    pub fn outside_of_document(parser: Parser<'de>) -> Self {
        Self {
            parser,
            is_outside_of_document: true,
            pending_value_type: None,
            consumed_name: false,
        }
    }
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
                ElementType::Boolean => visitor.visit_bool(self.parser.read_bool()?),
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
        if self.de.parser.end_document()? {
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
