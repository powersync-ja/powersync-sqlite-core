use serde::{
    de::{
        self, DeserializeSeed, EnumAccess, IntoDeserializer, MapAccess, SeqAccess, VariantAccess,
        Visitor,
    },
    forward_to_deserialize_any,
};

use super::{
    BsonError,
    error::ErrorKind,
    parser::{ElementType, Parser},
};

pub struct Deserializer<'de> {
    parser: Parser<'de>,
    position: DeserializerPosition,
}

#[derive(Clone, Debug)]
enum DeserializerPosition {
    /// The deserializer is outside of the initial document header.
    OutsideOfDocument,
    /// The deserializer expects the beginning of a key-value pair, or the end of the current
    /// document.
    BeforeTypeOrAtEndOfDocument,
    /// The deserializer has read past the type of a key-value pair, but did not scan the name yet.
    BeforeName { pending_type: ElementType },
    /// Read type and name of a key-value pair, position is before the value now.
    BeforeValue { pending_type: ElementType },
}

impl<'de> Deserializer<'de> {
    /// When used as a name hint to [de::Deserialize.deserialize_enum], the BSON deserializer will
    /// report documents a byte array view instead of parsing them.
    ///
    /// This is used as an internal optimization when we want to keep a reference to a BSON sub-
    /// document without actually inspecting the structure of that document.
    pub const SPECIAL_CASE_EMBEDDED_DOCUMENT: &'static str = "\0SpecialCaseEmbedDoc";

    fn outside_of_document(parser: Parser<'de>) -> Self {
        Self {
            parser,
            position: DeserializerPosition::OutsideOfDocument,
        }
    }

    pub fn from_bytes(bytes: &'de [u8]) -> Self {
        let parser = Parser::new(bytes);
        Self::outside_of_document(parser)
    }

    fn prepare_to_read(&mut self, allow_key: bool) -> Result<KeyOrValue<'de>, BsonError> {
        match self.position.clone() {
            DeserializerPosition::OutsideOfDocument => {
                // The next value we're reading is a document
                self.position = DeserializerPosition::BeforeValue {
                    pending_type: ElementType::Document,
                };
                Ok(KeyOrValue::PendingValue(ElementType::Document))
            }
            DeserializerPosition::BeforeValue { pending_type } => {
                Ok(KeyOrValue::PendingValue(pending_type))
            }
            DeserializerPosition::BeforeTypeOrAtEndOfDocument { .. } => {
                Err(self.parser.error(ErrorKind::InvalidStateExpectedType))
            }
            DeserializerPosition::BeforeName { pending_type } => {
                if !allow_key {
                    return Err(self.parser.error(ErrorKind::InvalidStateExpectedName));
                }

                self.position = DeserializerPosition::BeforeValue {
                    pending_type: pending_type,
                };
                Ok(KeyOrValue::Key(self.parser.read_cstr()?))
            }
        }
    }

    fn prepare_to_read_value(&mut self) -> Result<ElementType, BsonError> {
        let result = self.prepare_to_read(false)?;
        match result {
            KeyOrValue::Key(_) => unreachable!(),
            KeyOrValue::PendingValue(element_type) => Ok(element_type),
        }
    }

    fn object_reader(&mut self) -> Result<Deserializer<'de>, BsonError> {
        let parser = self.parser.document_scope()?;
        let deserializer = Deserializer {
            parser,
            position: DeserializerPosition::BeforeTypeOrAtEndOfDocument,
        };
        Ok(deserializer)
    }

    fn advance_to_next_name(&mut self) -> Result<Option<()>, BsonError> {
        if self.parser.end_document()? {
            return Ok(None);
        }

        self.position = DeserializerPosition::BeforeName {
            pending_type: self.parser.read_element_type()?,
        };
        Ok(Some(()))
    }
}

impl<'de, 'a> de::Deserializer<'de> for &'a mut Deserializer<'de> {
    type Error = BsonError;

    fn is_human_readable(&self) -> bool {
        false
    }

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let element_type = match self.prepare_to_read(true)? {
            KeyOrValue::Key(name) => return visitor.visit_borrowed_str(name),
            KeyOrValue::PendingValue(element_type) => element_type,
        };

        match element_type {
            ElementType::Double => visitor.visit_f64(self.parser.read_double()?),
            ElementType::String => visitor.visit_borrowed_str(self.parser.read_string()?),
            ElementType::Document => {
                let mut object = self.object_reader()?;
                visitor.visit_map(&mut object)
            }
            ElementType::Array => {
                let mut object = self.object_reader()?;
                visitor.visit_seq(&mut object)
            }
            ElementType::Binary => {
                let (_, bytes) = self.parser.read_binary()?;
                visitor.visit_borrowed_bytes(bytes)
            }
            ElementType::ObjectId => visitor.visit_borrowed_bytes(self.parser.read_object_id()?),
            ElementType::Boolean => visitor.visit_bool(self.parser.read_bool()?),
            ElementType::DatetimeUtc | ElementType::Timestamp => {
                visitor.visit_u64(self.parser.read_uint64()?)
            }
            ElementType::Null | ElementType::Undefined => visitor.visit_unit(),
            ElementType::Int32 => visitor.visit_i32(self.parser.read_int32()?),
            ElementType::Int64 => visitor.visit_i64(self.parser.read_int64()?),
        }
    }

    fn deserialize_enum<V>(
        self,
        name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let kind = self.prepare_to_read_value()?;

        // With this special name, the visitor indicates that it doesn't actually want to read an
        // enum, it wants to read values regularly. Except that a document appearing at this
        // position should not be parsed, it should be forwarded as an embedded byte array.
        if name == Deserializer::SPECIAL_CASE_EMBEDDED_DOCUMENT {
            return if matches!(kind, ElementType::Document) {
                let object = self.parser.skip_document()?;
                visitor.visit_borrowed_bytes(object)
            } else {
                self.deserialize_any(visitor)
            };
        }

        match kind {
            ElementType::String => {
                visitor.visit_enum(self.parser.read_string()?.into_deserializer())
            }
            ElementType::Document => {
                let mut object = self.object_reader()?;
                visitor.visit_enum(&mut object)
            }
            _ => Err(self.parser.error(ErrorKind::ExpectedEnum { actual: kind })),
        }
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let kind = self.prepare_to_read_value()?;
        match kind {
            ElementType::Null => visitor.visit_none(),
            _ => visitor.visit_some(self),
        }
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.prepare_to_read_value()?;
        visitor.visit_newtype_struct(self)
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf unit unit_struct  seq tuple
        tuple_struct map struct ignored_any identifier
    }
}

impl<'de> MapAccess<'de> for Deserializer<'de> {
    type Error = BsonError;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        if let None = self.advance_to_next_name()? {
            return Ok(None);
        }
        Ok(Some(seed.deserialize(self)?))
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        seed.deserialize(self)
    }
}

impl<'de> SeqAccess<'de> for Deserializer<'de> {
    type Error = BsonError;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        // Array elements are encoded as an object like `{"0": value, "1": another}`
        if let None = self.advance_to_next_name()? {
            return Ok(None);
        }

        // Skip name
        assert!(matches!(
            self.position,
            DeserializerPosition::BeforeName { .. }
        ));
        self.prepare_to_read(true)?;

        // And deserialize value!
        Ok(Some(seed.deserialize(self)?))
    }
}

impl<'a, 'de> EnumAccess<'de> for &'a mut Deserializer<'de> {
    type Error = BsonError;
    type Variant = Self;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        if let None = self.advance_to_next_name()? {
            return Err(self
                .parser
                .error(ErrorKind::UnexpectedEndOfDocumentForEnumVariant));
        }

        let value = seed.deserialize(&mut *self)?;
        Ok((value, self))
    }
}

impl<'a, 'de> VariantAccess<'de> for &'a mut Deserializer<'de> {
    type Error = BsonError;

    fn unit_variant(self) -> Result<(), Self::Error> {
        // Unit variants are encoded as simple string values, which are handled directly in
        // Deserializer::deserialize_enum.
        Err(self.parser.error(ErrorKind::ExpectedString))
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        // Newtype variants are represented as `{ NAME: VALUE }`, so we just have to deserialize the
        // value here.
        seed.deserialize(self)
    }

    fn tuple_variant<V>(self, _len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        // Tuple variants are represented as `{ NAME: VALUES[] }`, so we deserialize the array here.
        de::Deserializer::deserialize_seq(self, visitor)
    }

    fn struct_variant<V>(
        self,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        // Struct variants are represented as `{ NAME: { ... } }`, so we deserialize the struct.
        de::Deserializer::deserialize_map(self, visitor)
    }
}

enum KeyOrValue<'de> {
    Key(&'de str),
    PendingValue(ElementType),
}
