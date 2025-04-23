extern crate alloc;

use alloc::string::{String, ToString};
use core::ffi::c_int;
use serde::de::{DeserializeSeed, Deserializer, IgnoredAny, Visitor};

use sqlite::ResultCode;
use sqlite_nostd::{self as sqlite, ColumnType};
use sqlite_nostd::{Connection, Context, Value};

use crate::error::SQLiteError;
use crate::util::context_set_error;
use crate::{bson, create_sqlite_text_fn};

/// Given any number of JSON TEXT arguments, merge them into a single JSON object.
///
/// This assumes each argument is a valid JSON object, with no duplicate keys.
/// No JSON parsing or validation is performed - this performs simple string concatenation.
fn powersync_json_merge_impl(
    _ctx: *mut sqlite::context,
    args: &[*mut sqlite::value],
) -> Result<String, SQLiteError> {
    if args.is_empty() {
        return Ok("{}".to_string());
    }
    let mut result = String::from("{");
    for arg in args {
        let chunk = arg.text();
        if chunk.is_empty() || !chunk.starts_with('{') || !chunk.ends_with('}') {
            return Err(SQLiteError::from(ResultCode::MISMATCH));
        }

        // Strip outer braces
        let inner = &chunk[1..(chunk.len() - 1)];

        // If this is not the first chunk, insert a comma
        if result.len() > 1 {
            result.push(',');
        }

        // Append the inner content
        result.push_str(inner);
    }

    // Close the outer brace
    result.push('}');
    Ok(result)
}

create_sqlite_text_fn!(
    powersync_json_merge,
    powersync_json_merge_impl,
    "powersync_json_merge"
);

/// A variant of `json_extract` that works both with JSON and BSON.
///
/// We only support extracting top-level keys here, so an invocation would look like
/// `powersync_extract('{"foo": "bar"}', 'foo')`.
fn powersync_extract_impl(
    ctx: *mut sqlite::context,
    argc: c_int,
    argv: *mut *mut sqlite::value,
) -> Result<(), SQLiteError> {
    let args = sqlite::args!(argc, argv);

    if args.len() != 2 {
        return Err(SQLiteError(
            ResultCode::MISUSE,
            Some("Expected two arguments".to_string()),
        ));
    }

    let document = args[0];
    let key = args[1];

    if key.value_type() != ColumnType::Text {
        return Err(SQLiteError(
            ResultCode::MISMATCH,
            Some("Second argument to powersync_extract must be text".to_string()),
        ));
    }

    struct FindKeyVisitor<'a> {
        key: &'a str,
        ctx: *mut sqlite::context,
    }

    struct ApplyAsResultVisitor {
        ctx: *mut sqlite::context,
    }

    impl<'a, 'de> Visitor<'de> for FindKeyVisitor<'a> {
        type Value = (); // We don't actually return the value, we set the result on the context.

        fn expecting(&self, formatter: &mut core::fmt::Formatter) -> core::fmt::Result {
            write!(formatter, "a JSON or BSON document")
        }

        fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::MapAccess<'de>,
        {
            loop {
                let key = map.next_key::<&'de str>()?;
                match key {
                    None => break,
                    Some(key) => {
                        if key == self.key {
                            map.next_value_seed(ApplyAsResultVisitor { ctx: self.ctx })?;
                        } else {
                            map.next_value::<IgnoredAny>()?;
                            continue;
                        }
                    }
                }
            }

            Ok(())
        }
    }

    impl<'de> DeserializeSeed<'de> for ApplyAsResultVisitor {
        type Value = ();

        fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserializer.deserialize_any(self)
        }
    }

    impl<'de> Visitor<'de> for ApplyAsResultVisitor {
        type Value = ();

        fn expecting(&self, formatter: &mut core::fmt::Formatter) -> core::fmt::Result {
            write!(formatter, "a SQLite value (int, blob, string, real)")
        }

        fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            self.ctx.result_int(if v { 1 } else { 0 });
            Ok(())
        }

        fn visit_unit<E>(self) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            self.ctx.result_null();
            Ok(())
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            self.ctx.result_text_transient(v);
            Ok(())
        }

        fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            self.ctx.result_blob_transient(v);
            Ok(())
        }

        fn visit_f32<E>(self, v: f32) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            self.ctx.result_double(v.into());
            Ok(())
        }

        fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            self.ctx.result_double(v);
            Ok(())
        }

        fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            self.ctx.result_int64(v);
            Ok(())
        }

        fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            self.ctx.result_int64(v as i64);
            Ok(())
        }

        fn visit_i32<E>(self, v: i32) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            self.ctx.result_int(v);
            Ok(())
        }

        fn visit_u32<E>(self, v: u32) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            self.ctx.result_int(v as i32);
            Ok(())
        }
    }

    let visitor = FindKeyVisitor {
        key: key.text(),
        ctx,
    };

    match document.value_type() {
        ColumnType::Text => {
            let mut deserializer = serde_json::Deserializer::from_str(document.text());
            deserializer.deserialize_map(visitor)?;
        }
        ColumnType::Blob => {
            let mut deserializer = bson::Deserializer::from_bytes(document.blob());
            deserializer.deserialize_map(visitor)?;
        }
        _ => {
            return Err(SQLiteError(
                ResultCode::MISMATCH,
                Some("First argument to powersync_extract must be text or a blob".to_string()),
            ));
        }
    }

    Ok(())
}

extern "C" fn powersync_extract(
    ctx: *mut sqlite::context,
    argc: c_int,
    argv: *mut *mut sqlite::value,
) {
    if let Err(e) = powersync_extract_impl(ctx, argc, argv) {
        context_set_error(ctx, e, "powersync_extract");
    }
}

pub fn register(db: *mut sqlite::sqlite3) -> Result<(), ResultCode> {
    db.create_function_v2(
        "powersync_json_merge",
        -1,
        sqlite::UTF8 | sqlite::DETERMINISTIC,
        None,
        Some(powersync_json_merge),
        None,
        None,
        None,
    )?;

    db.create_function_v2(
        "powersync_extract",
        2,
        sqlite::UTF8 | sqlite::DETERMINISTIC,
        None,
        Some(powersync_extract),
        None,
        None,
        None,
    )?;

    Ok(())
}
