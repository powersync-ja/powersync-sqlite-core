extern crate alloc;

use core::fmt::{Display, Write};

use alloc::format;
use alloc::string::{String, ToString};

#[cfg(not(feature = "getrandom"))]
use crate::sqlite;

use uuid::Uuid;

#[cfg(not(feature = "getrandom"))]
use uuid::Builder;

pub fn quote_string(s: &str) -> String {
    return QuotedString(s).to_string();
}

pub fn quote_json_path(s: &str) -> String {
    quote_string(&format!("$.{:}", s))
}

pub fn quote_identifier(name: &str) -> String {
    format!("\"{:}\"", name.replace("\"", "\"\""))
}

pub fn quote_internal_name(name: &str, local_only: bool) -> String {
    if local_only {
        quote_identifier_prefixed("ps_data_local__", name)
    } else {
        quote_identifier_prefixed("ps_data__", name)
    }
}

/// A string that [Display]s as a SQLite string literal.
pub struct QuotedString<'a>(pub &'a str);

impl<'a> Display for QuotedString<'a> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        const SINGLE_QUOTE: char = '\'';
        const ESCAPE_SEQUENCE: &'static str = "''";

        f.write_char(SINGLE_QUOTE)?;

        for (i, group) in self.0.split(SINGLE_QUOTE).enumerate() {
            if i != 0 {
                f.write_str(ESCAPE_SEQUENCE)?;
            }

            f.write_str(group)?;
        }

        f.write_char(SINGLE_QUOTE)
    }
}

pub fn quote_identifier_prefixed(prefix: &str, name: &str) -> String {
    return format!("\"{:}{:}\"", prefix, name.replace("\"", "\"\""));
}

// Use getrandom crate to generate UUID.
// This is not available in all WASM builds - use the default in those cases.
#[cfg(feature = "getrandom")]
pub fn gen_uuid() -> Uuid {
    let id = Uuid::new_v4();
    id
}

// Default - use sqlite3_randomness to generate UUID
// This uses ChaCha20 PRNG, with /dev/urandom as a seed on unix.
// On Windows, it uses custom logic for the seed, which may not be secure.
// Rather avoid this version for most builds.
#[cfg(not(feature = "getrandom"))]
pub fn gen_uuid() -> Uuid {
    let mut random_bytes: [u8; 16] = [0; 16];
    sqlite::randomness(&mut random_bytes);
    let id = Builder::from_random_bytes(random_bytes).into_uuid();
    id
}

pub const MAX_OP_ID: &str = "9223372036854775807";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_identifier_test() {
        assert_eq!(quote_identifier("test"), "\"test\"");
        assert_eq!(quote_identifier("\"quote\""), "\"\"\"quote\"\"\"");
        assert_eq!(
            quote_identifier("other characters."),
            "\"other characters.\""
        );
    }

    #[test]
    fn quote_string_test() {
        assert_eq!(quote_string("test"), "'test'");
        assert_eq!(quote_string("\"quote\""), "'\"quote\"'");
        assert_eq!(quote_string("'quote'"), "'''quote'''");
    }
}
