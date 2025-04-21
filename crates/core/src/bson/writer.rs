use alloc::vec::Vec;
use bytes::BufMut;

use super::parser::ElementType;

pub struct BsonWriter {
    output: Vec<u8>,
}

impl BsonWriter {
    pub fn new() -> Self {
        let mut data = Vec::<u8>::new();
        data.put_i32_le(0); // Total document size, filled out later.

        Self { output: data }
    }

    fn put_entry(&mut self, kind: ElementType, name: &str) {
        self.output.push(kind as i8 as u8);
        let bytes = name.as_bytes();
        self.output.put_slice(bytes);
        self.output.push(0);
    }

    pub fn put_str(&mut self, name: &str, value: &str) {
        self.put_entry(ElementType::String, name);

        let bytes = name.as_bytes();
        self.output.put_i32_le(bytes.len() as i32);
        self.output.put_slice(bytes);
        self.output.push(0);
    }

    pub fn put_float(&mut self, name: &str, value: f64) {
        self.put_entry(ElementType::Double, name);
        self.output.put_f64_le(value);
    }

    pub fn put_int(&mut self, name: &str, value: i64) {
        self.put_entry(ElementType::Int64, name);
        self.output.put_i64_le(value);
    }

    pub fn finish(mut self) -> Vec<u8> {
        self.output.push(0);
        let length = self.output.len() as i32;

        let length_field = &mut self.output[0..4];
        length_field.copy_from_slice(&length.to_le_bytes());
        self.output
    }
}
