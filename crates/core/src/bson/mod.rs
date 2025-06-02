pub use de::Deserializer;
pub use error::BsonError;
use serde::Deserialize;

mod de;
mod error;
mod parser;

/// Deserializes BSON [bytes] into a structure [T].
pub fn from_bytes<'de, T: Deserialize<'de>>(bytes: &'de [u8]) -> Result<T, BsonError> {
    let mut deserializer = Deserializer::from_bytes(bytes);

    T::deserialize(&mut deserializer)
}

#[cfg(test)]
mod test {
    use core::assert_matches::assert_matches;

    use crate::sync::line::{SyncLine, TokenExpiresIn};

    use super::*;

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

    #[test]
    fn test_checkpoint_line() {
        let bson = b"\x85\x00\x00\x00\x03checkpoint\x00t\x00\x00\x00\x02last_op_id\x00\x02\x00\x00\x001\x00\x0awrite_checkpoint\x00\x04buckets\x00B\x00\x00\x00\x030\x00:\x00\x00\x00\x02bucket\x00\x02\x00\x00\x00a\x00\x10checksum\x00\x00\x00\x00\x00\x10priority\x00\x03\x00\x00\x00\x10count\x00\x01\x00\x00\x00\x00\x00\x00\x00";

        let expected: SyncLine = from_bytes(bson.as_slice()).expect("should deserialize");
        let SyncLine::Checkpoint(checkpoint) = expected else {
            panic!("Expected to deserialize as checkpoint line")
        };

        assert_eq!(checkpoint.buckets.len(), 1);
    }

    #[test]
    fn test_newtype_tuple() {
        let bson = b"\x1b\x00\x00\x00\x10token_expires_in\x00<\x00\x00\x00\x00";

        let expected: SyncLine = from_bytes(bson.as_slice()).expect("should deserialize");
        assert_matches!(expected, SyncLine::KeepAlive(TokenExpiresIn(60)));
    }
}
