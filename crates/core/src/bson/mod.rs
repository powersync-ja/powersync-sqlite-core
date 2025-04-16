use de::Deserializer;
pub use error::BsonError;
use parser::Parser;
use serde::Deserialize;

mod de;
mod error;
mod parser;

/// Deserializes BSON [bytes] into a structure [T].
pub fn from_bytes<'de, T: Deserialize<'de>>(bytes: &'de [u8]) -> Result<T, BsonError> {
    let parser = Parser::new(bytes);
    let mut deserializer = Deserializer::outside_of_document(parser);

    T::deserialize(&mut deserializer)
}

#[cfg(test)]
mod test {
    use super::*;
    use serde::de::DeserializeOwned;

    #[test]
    fn test_hello_world() {
        // {"hello": "world"}
        let bson = b"\x16\x00\x00\x00\x02hello\x00\x06\x00\x00\x00world\x00\x00";

        #[derive(Deserialize)]
        struct Expected<'a> {
            hello: &'a str,
        }

        let expected: Expected = from_bytes(bson.as_slice()).expect("should deserialize");
        assert_eq!(expected.hello, "world");
    }
}
