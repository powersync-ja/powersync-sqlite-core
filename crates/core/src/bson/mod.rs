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
    use alloc::{vec, vec::Vec};
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

    #[test]
    fn test_int64_positive_max() {
        // {"value": 9223372036854775807} (i64::MAX)
        let bson = b"\x14\x00\x00\x00\x12value\x00\xff\xff\xff\xff\xff\xff\xff\x7f\x00";

        #[derive(Deserialize)]
        struct TestDoc {
            value: i64,
        }

        let doc: TestDoc = from_bytes(bson).expect("should deserialize");
        assert_eq!(doc.value, i64::MAX);
    }

    #[test]
    fn test_int64_negative_max() {
        // {"value": -9223372036854775808} (i64::MIN)
        let bson = b"\x14\x00\x00\x00\x12value\x00\x00\x00\x00\x00\x00\x00\x00\x80\x00";

        #[derive(Deserialize)]
        struct TestDoc {
            value: i64,
        }

        let doc: TestDoc = from_bytes(bson).expect("should deserialize");
        assert_eq!(doc.value, i64::MIN);
    }

    #[test]
    fn test_int64_negative_one() {
        // {"value": -1}
        let bson = b"\x14\x00\x00\x00\x12value\x00\xff\xff\xff\xff\xff\xff\xff\xff\x00";

        #[derive(Deserialize)]
        struct TestDoc {
            value: i64,
        }

        let doc: TestDoc = from_bytes(bson).expect("should deserialize");
        assert_eq!(doc.value, -1);
    }

    #[test]
    fn test_int64_negative_small() {
        // {"value": -42}
        let bson = b"\x14\x00\x00\x00\x12value\x00\xd6\xff\xff\xff\xff\xff\xff\xff\x00";

        #[derive(Deserialize)]
        struct TestDoc {
            value: i64,
        }

        let doc: TestDoc = from_bytes(bson).expect("should deserialize");
        assert_eq!(doc.value, -42);
    }

    #[test]
    fn test_int32_negative_values() {
        // {"small": -1, "large": -2147483648} (i32::MIN)
        let bson =
            b"\x1b\x00\x00\x00\x10small\x00\xff\xff\xff\xff\x10large\x00\x00\x00\x00\x80\x00";

        #[derive(Deserialize)]
        struct TestDoc {
            small: i32,
            large: i32,
        }

        let doc: TestDoc = from_bytes(bson).expect("should deserialize");
        assert_eq!(doc.small, -1);
        assert_eq!(doc.large, i32::MIN);
    }

    #[test]
    fn test_double_negative_values() {
        // {"neg": -3.14159}
        let bson = b"\x12\x00\x00\x00\x01neg\x00\x6e\x86\x1b\xf0\xf9\x21\x09\xc0\x00";

        #[derive(Deserialize)]
        struct TestDoc {
            neg: f64,
        }

        let doc: TestDoc = from_bytes(bson).expect("should deserialize");
        assert!((doc.neg - (-3.14159)).abs() < 0.00001);
    }

    #[test]
    fn test_double_special_values() {
        // Test infinity, negative infinity, and NaN representations
        // {"inf": Infinity, "ninf": -Infinity, "nan": NaN}
        let bson = b"\x2d\x00\x00\x00\x01\x69\x6e\x66\x00\x00\x00\x00\x00\x00\x00\xf0\x7f\x01\x6e\x69\x6e\x66\x00\x00\x00\x00\x00\x00\x00\xf0\xff\x01\x6e\x61\x6e\x00\x00\x00\x00\x00\x00\x00\xf8\x7f\x00";

        #[derive(Deserialize)]
        struct TestDoc {
            inf: f64,
            ninf: f64,
            nan: f64,
        }

        let doc: TestDoc = from_bytes(bson).expect("should deserialize");
        assert_eq!(doc.inf, f64::INFINITY);
        assert_eq!(doc.ninf, f64::NEG_INFINITY);
        assert!(doc.nan.is_nan());
    }

    #[test]
    fn test_empty_string() {
        // {"empty": ""}
        let bson = b"\x11\x00\x00\x00\x02empty\x00\x01\x00\x00\x00\x00\x00";

        #[derive(Deserialize)]
        struct TestDoc<'a> {
            empty: &'a str,
        }

        let doc: TestDoc = from_bytes(bson).expect("should deserialize");
        assert_eq!(doc.empty, "");
    }

    #[test]
    fn test_unicode_string() {
        // {"unicode": "🦀💖"}
        let bson = b"\x1b\x00\x00\x00\x02unicode\x00\x09\x00\x00\x00\xf0\x9f\xa6\x80\xf0\x9f\x92\x96\x00\x00";

        #[derive(Deserialize)]
        struct TestDoc<'a> {
            unicode: &'a str,
        }

        let doc: TestDoc = from_bytes(bson).expect("should deserialize");
        assert_eq!(doc.unicode, "🦀💖");
    }

    #[test]
    fn test_boolean_values() {
        // {"true_val": true, "false_val": false}
        let bson = b"\x1c\x00\x00\x00\x08true_val\x00\x01\x08false_val\x00\x00\x00";

        #[derive(Deserialize)]
        struct TestDoc {
            true_val: bool,
            false_val: bool,
        }

        let doc: TestDoc = from_bytes(bson).expect("should deserialize");
        assert_eq!(doc.true_val, true);
        assert_eq!(doc.false_val, false);
    }

    #[test]
    fn test_null_value() {
        // {"null_val": null}
        let bson = b"\x0f\x00\x00\x00\x0anull_val\x00\x00";

        #[derive(Deserialize)]
        struct TestDoc {
            null_val: Option<i32>,
        }

        let doc: TestDoc = from_bytes(bson).expect("should deserialize");
        assert_eq!(doc.null_val, None);
    }

    #[test]
    fn test_empty_document() {
        // {}
        let bson = b"\x05\x00\x00\x00\x00";

        #[derive(Deserialize)]
        struct TestDoc {}

        let _doc: TestDoc = from_bytes(bson).expect("should deserialize");
    }

    #[test]
    fn test_nested_document() {
        // {"nested": {"inner": 42}}
        let bson =
            b"\x1d\x00\x00\x00\x03nested\x00\x10\x00\x00\x00\x10inner\x00*\x00\x00\x00\x00\x00";

        #[derive(Deserialize)]
        struct Inner {
            inner: i32,
        }

        #[derive(Deserialize)]
        struct TestDoc {
            nested: Inner,
        }

        let doc: TestDoc = from_bytes(bson).expect("should deserialize");
        assert_eq!(doc.nested.inner, 42);
    }

    #[test]
    fn test_array_with_integers() {
        // {"array": [1, 2]} - simplified array test
        // Array format: {"0": 1, "1": 2}
        let bson = b"\x1f\x00\x00\x00\x04array\x00\x13\x00\x00\x00\x100\x00\x01\x00\x00\x00\x101\x00\x02\x00\x00\x00\x00\x00";

        #[derive(Deserialize)]
        struct TestDoc {
            array: Vec<i32>,
        }

        let doc: TestDoc = from_bytes(bson).expect("should deserialize");
        assert_eq!(doc.array, vec![1, 2]);
    }

    #[test]
    fn test_binary_data() {
        // {"binary": <binary data>}
        let bson = b"\x16\x00\x00\x00\x05binary\x00\x04\x00\x00\x00\x00\x01\x02\x03\x04\x00";

        #[derive(Deserialize)]
        struct TestDoc<'a> {
            binary: &'a [u8],
        }

        let doc: TestDoc = from_bytes(bson).expect("should deserialize");
        assert_eq!(doc.binary, &[1, 2, 3, 4]);
    }

    // Error case tests

    #[test]
    fn test_invalid_element_type() {
        // Document with invalid element type (99)
        let bson = b"\x10\x00\x00\x00\x63test\x00\x01\x00\x00\x00\x00";

        #[derive(Deserialize)]
        #[allow(dead_code)]
        struct TestDoc {
            test: i32,
        }

        let result: Result<TestDoc, _> = from_bytes(bson);
        assert!(result.is_err());
    }

    #[test]
    fn test_truncated_document() {
        // Document claims to be longer than actual data
        let bson = b"\xff\x00\x00\x00\x10test\x00";

        #[derive(Deserialize)]
        #[allow(dead_code)]
        struct TestDoc {
            test: i32,
        }

        let result: Result<TestDoc, _> = from_bytes(bson);
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_string_length() {
        // String with invalid length
        let bson = b"\x15\x00\x00\x00\x02test\x00\xff\xff\xff\xff\x00";

        #[derive(Deserialize)]
        #[allow(dead_code)]
        struct TestDoc<'a> {
            test: &'a str,
        }

        let result: Result<TestDoc, _> = from_bytes(bson);
        assert!(result.is_err());
    }

    #[test]
    fn test_unterminated_cstring() {
        // Document with field name that doesn't have null terminator
        let bson = b"\x10\x00\x00\x00\x10test\x01\x00\x00\x00\x00\x00";

        #[derive(Deserialize)]
        #[allow(dead_code)]
        struct TestDoc {
            test: i32,
        }

        let result: Result<TestDoc, _> = from_bytes(bson);
        assert!(result.is_err());
    }

    #[test]
    fn test_document_without_terminator() {
        // Document missing the final null byte
        let bson = b"\x0d\x00\x00\x00\x10test\x00*\x00\x00\x00";

        #[derive(Deserialize)]
        #[allow(dead_code)]
        struct TestDoc {
            test: i32,
        }

        let result: Result<TestDoc, _> = from_bytes(bson);
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_document_size() {
        // Document with size less than minimum (5 bytes)
        let bson = b"\x04\x00\x00\x00\x00";

        #[derive(Deserialize)]
        struct TestDoc {}

        let result: Result<TestDoc, _> = from_bytes(bson);
        assert!(result.is_err());
    }
}
