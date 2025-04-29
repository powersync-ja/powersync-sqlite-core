use core::fmt::Write;

use alloc::string::String;

/// A very simple JSON writer for primitive values.
pub struct JsonWriter {
    buffer: String,
    has_entry: bool,
}

impl JsonWriter {
    pub fn new() -> Self {
        Self {
            buffer: String::from("{"),
            has_entry: false,
        }
    }

    fn write_key(&mut self, key: &str) {
        if self.has_entry {
            self.buffer.push(',');
        }
        self.buffer.push('"');
        format_escaped_str_contents(&mut self.buffer, key);
        self.buffer.push('"');
    }

    pub fn write_str(&mut self, key: &str, value: &str) {
        self.write_key(key);
        self.buffer.push('"');
        format_escaped_str_contents(&mut self.buffer, value);
        self.buffer.push('"');
    }

    pub fn write_i64(&mut self, key: &str, value: i64) {
        self.write_key(key);
        // Unwrap is safe, we're writing to a string
        write!(self.buffer, "{}", value).unwrap();
    }

    pub fn write_f64(&mut self, key: &str, value: f64) {
        self.write_key(key);
        // Unwrap is safe, we're writing to a string
        write!(self.buffer, "{}", value).unwrap();
    }

    pub fn finish(mut self) -> String {
        self.buffer.push('}');
        self.buffer
    }
}

// Adopted from https://github.com/serde-rs/json/blob/8a56cfa6d0a93c39ee4ef07d431de0748eed9028/src/ser.rs#L2081
fn format_escaped_str_contents(writer: &mut String, value: &str) {
    let bytes = value.as_bytes();

    let mut start = 0;

    for (i, &byte) in bytes.iter().enumerate() {
        let is_quote = (byte as char) == '"';
        if !is_quote {
            continue;
        }

        if start < i {
            writer.push_str(&value[start..i]);
        }

        writer.push_str("\\\"");
        start = i + 1;
    }

    if start == bytes.len() {
        return;
    }
    writer.push_str(&value[start..]);
}
