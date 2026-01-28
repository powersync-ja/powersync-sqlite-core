use core::fmt::{Display, Write};

use alloc::string::String;

const DOUBLE_QUOTE: char = '"';
const SINGLE_QUOTE: char = '\'';

#[derive(Default)]
pub struct SqlBuffer {
    pub sql: String,
}

impl SqlBuffer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_str(&mut self, str: &str) {
        self.sql.push_str(str);
    }

    pub fn push_char(&mut self, char: char) {
        self.sql.push(char);
    }

    pub fn comma(&mut self) {
        self.push_str(", ");
    }

    pub fn comma_separated<'a>(&'a mut self) -> CommaSeparated<'a> {
        CommaSeparated::new(self)
    }

    /// Creates a writer wrapped in double quotes for SQL identifiers.
    pub fn identifier<'a>(&'a mut self) -> impl Write + 'a {
        EscapingWriter::<'a, DOUBLE_QUOTE>::new(self)
    }

    /// Creates a writer wrapped in single quotes for SQL strings.
    pub fn string_literal<'a>(&'a mut self) -> impl Write + 'a {
        EscapingWriter::<'a, SINGLE_QUOTE>::new(self)
    }

    pub fn quote_internal_name(&mut self, name: &str, local_only: bool) {
        self.quote_identifier_prefixed(
            if local_only {
                "ps_data_local__"
            } else {
                "ps_data__"
            },
            name,
        );
    }

    pub fn quote_identifier_prefixed(&mut self, prefix: &str, name: &str) {
        let mut id = self.identifier();
        let _ = write!(id, "{prefix}{name}");
    }

    pub fn quote_json_path(&mut self, s: &str) {
        let mut str = self.string_literal();
        let _ = write!(str, "$.{s}");
    }

    pub fn create_trigger(&mut self, prefix: &str, view_name: &str) {
        self.push_str("CREATE TRIGGER ");
        self.quote_identifier_prefixed(prefix, view_name);
        self.push_char(' ');
    }

    /// Writes an `INSTEAD OF $write_type ON $on FOR EACH ROW` segment.
    pub fn trigger_instead_of(&mut self, write_type: &str, on: &str) {
        self.push_str("INSTEAD OF ");
        self.push_str(write_type);
        self.push_str(" ON ");
        let _ = self.identifier().write_str(on);
        self.push_str(" FOR EACH ROW ");
    }

    pub fn trigger_end(&mut self) {
        self.push_str("END");
    }

    /// Writes a select statement throwing in triggers if `OLD.id != NEW.id`.
    pub fn check_id_not_changed(&mut self) {
        self.push_str(
            "SELECT CASE WHEN (OLD.id != NEW.id) THEN RAISE (FAIL, 'Cannot update id') END;\n",
        );
    }

    /// Writes a select statement throwing in triggers if `NEW.id` is null or not a string.
    pub fn check_id_valid(&mut self) {
        self.push_str(
            "SELECT CASE WHEN (NEW.id IS NULL) THEN RAISE (FAIL, 'id is required') WHEN (typeof(NEW.id) != 'text') THEN RAISE (FAIL, 'id should be text') END;\n",
        );
    }

    /// Writes an `INSERT INTO powersync_crud` statement.
    pub fn insert_into_powersync_crud<Id, Data, Old, Metadata>(
        &mut self,
        insert: InsertIntoCrud<Id, Data, Old, Metadata>,
    ) where
        Id: Display,
        Data: Display,
        Old: Display,
        Metadata: Display,
    {
        self.push_str("INSERT INTO powersync_crud(op,id,type");
        if insert.data.is_some() {
            self.push_str(",data");
        }
        if insert.old_values.is_some() {
            self.push_str(",old_values");
        }
        if insert.metadata.is_some() {
            self.push_str(",metadata");
        }
        if insert.options.is_some() {
            self.push_str(",options");
        }
        self.push_str(") VALUES (");

        let _ = self.string_literal().write_str(insert.op);
        self.comma();

        let _ = write!(self, "{}", insert.id_expr);
        self.comma();

        let _ = self.string_literal().write_str(insert.type_name);

        if let Some(data) = insert.data {
            self.comma();
            let _ = write!(self, "{}", data);
        }

        if let Some(old) = insert.old_values {
            self.comma();
            let _ = write!(self, "{}", old);
        }

        if let Some(meta) = insert.metadata {
            self.comma();
            let _ = write!(self, "{}", meta);
        }

        if let Some(options) = insert.options {
            self.comma();
            let _ = write!(self, "{}", options);
        }

        self.push_str(");\n");
    }

    /// Generates a `CAST(json_extract(<source>, "$.<name>") as <cast_to>)`
    pub fn json_extract_and_cast(&mut self, source: &str, name: &str, cast_to: &str) {
        let _ = write!(self, "CAST(json_extract({source}, ");
        self.quote_json_path(name);
        self.push_str(") as ");
        self.push_str(cast_to);
        self.push_char(')');
    }

    /// Utility to write `inner` as an SQL identifier.
    pub fn quote_identifier(inner: impl Display) -> String {
        let mut buffer = SqlBuffer::new();
        let _ = write!(buffer.identifier(), "{}", inner);
        buffer.sql
    }
}

impl Write for SqlBuffer {
    fn write_str(&mut self, s: &str) -> core::fmt::Result {
        self.sql.write_str(s)
    }

    fn write_char(&mut self, c: char) -> core::fmt::Result {
        self.sql.write_char(c)
    }
}

/// A [Write] wrapper escaping identifiers or strings.
struct EscapingWriter<'a, const DELIMITER: char> {
    buffer: &'a mut SqlBuffer,
}

impl<'a, const DELIMITER: char> EscapingWriter<'a, DELIMITER> {
    pub fn new(buffer: &'a mut SqlBuffer) -> Self {
        let mut escaped = Self { buffer };
        escaped.write_delimiter();
        escaped
    }

    fn write_delimiter(&mut self) {
        self.buffer.sql.push(DELIMITER);
    }

    fn write_escape_sequence(&mut self) {
        self.write_delimiter();
        self.write_delimiter();
    }
}

impl<'a, const DELIMITER: char> Write for EscapingWriter<'a, DELIMITER> {
    fn write_str(&mut self, s: &str) -> core::fmt::Result {
        for (i, component) in s.split(DELIMITER).enumerate() {
            if i != 0 {
                self.write_escape_sequence();
            }
            self.buffer.sql.push_str(component);
        }

        Ok(())
    }
}

impl<const DELIMITER: char> Drop for EscapingWriter<'_, DELIMITER> {
    fn drop(&mut self) {
        self.write_delimiter();
    }
}

pub struct CommaSeparated<'a> {
    buffer: &'a mut SqlBuffer,
    is_first: bool,
}

impl<'a> CommaSeparated<'a> {
    fn new(buffer: &'a mut SqlBuffer) -> Self {
        Self {
            buffer,
            is_first: true,
        }
    }

    pub fn element(&mut self) -> &mut SqlBuffer {
        if !self.is_first {
            self.buffer.comma();
        }

        self.is_first = false;
        self.buffer
    }
}

pub struct InsertIntoCrud<'a, Id, Data, Old, Metadata>
where
    Id: Display,
    Data: Display,
    Old: Display,
    Metadata: Display,
{
    pub op: &'a str,
    pub id_expr: Id,
    pub type_name: &'a str,
    pub data: Option<Data>,
    pub old_values: Option<Old>,
    pub metadata: Option<Metadata>,
    pub options: Option<u32>,
}

#[cfg(test)]
mod test {
    use super::SqlBuffer;
    use core::fmt::{Display, Write};

    #[test]
    fn identifier() {
        fn check_identifier<T: Display>(element: T, expected: &str) {
            let mut buffer = SqlBuffer::default();
            let mut id = buffer.identifier();
            write!(&mut id, "{}", element).unwrap();
            drop(id);

            assert_eq!(buffer.sql, expected)
        }

        check_identifier("foo", "\"foo\"");
        check_identifier("foo\"bar", "\"foo\"\"bar\"");
    }

    #[test]
    fn string() {
        fn check_string<T: Display>(element: T, expected: &str) {
            let mut buffer = SqlBuffer::default();
            let mut id = buffer.string_literal();
            write!(&mut id, "{}", element).unwrap();
            drop(id);

            assert_eq!(buffer.sql, expected)
        }

        check_string("foo", "'foo'");
        check_string("foo'bar", "'foo''bar'");
        check_string("foo'", "'foo'''");
    }
}
