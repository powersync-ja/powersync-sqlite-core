use core::{
    fmt::{Display, Write},
    str::FromStr,
};

use alloc::{format, string::String};

use crate::{
    error::PowerSyncError, schema::SchemaTable, views::table_columns_to_json_object_with_filter,
};

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
    pub fn trigger_instead_of(&mut self, write_type: WriteType, on: &str) {
        let _ = write!(self, "INSTEAD OF {write_type} ON ");
        let _ = self.identifier().write_str(on);
        self.push_str(" FOR EACH ROW ");
    }

    /// Writes an `INSTEAD OF $write_type ON $on FOR EACH ROW` segment.
    pub fn trigger_after(&mut self, write_type: WriteType, on: &str) {
        let _ = write!(self, "AFTER {write_type} ON ");
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
    pub fn insert_into_powersync_crud<Id, Data, Metadata>(
        &mut self,
        insert: InsertIntoCrud<Id, Data, Metadata>,
    ) -> Result<(), PowerSyncError>
    where
        Id: Display,
        Data: Display,
        Metadata: Display,
    {
        let old_values = if insert.op == WriteType::Insert {
            // Inserts don't have previous values we'd have to track.
            None
        } else {
            let options = insert.table.common_options();

            match &options.diff_include_old {
                None => None,
                Some(include_old) => {
                    let old_values = table_columns_to_json_object_with_filter(
                        "OLD",
                        insert.table,
                        include_old.column_filter(),
                    )?;

                    if options.flags.include_old_only_when_changed() {
                        // When include_old_only_when_changed is combined with a column filter, make sure we
                        // only include the powersync_diff of columns matched by the filter.
                        let filtered_new_fragment = table_columns_to_json_object_with_filter(
                            "NEW",
                            insert.table,
                            include_old.column_filter(),
                        )?;

                        Some(format!(
                            "json(powersync_diff({filtered_new_fragment}, {old_values}))"
                        ))
                    } else {
                        Some(old_values)
                    }
                }
            }
        };

        // Options to ps_crud are only used to conditionally skip empty updates if IGNORE_EMPTY_UPDATE is set.
        let options = match insert.op {
            WriteType::Update => Some(insert.table.common_options().flags.0),
            _ => None,
        };

        self.push_str("INSERT INTO powersync_crud(op,id,type");
        if insert.data.is_some() {
            self.push_str(",data");
        }
        if old_values.is_some() {
            self.push_str(",old_values");
        }
        if insert.metadata.is_some() {
            self.push_str(",metadata");
        }
        if options.is_some() {
            self.push_str(",options");
        }
        self.push_str(") VALUES (");

        let _ = self.string_literal().write_str(insert.op.ps_crud_op_type());
        self.comma();

        let _ = write!(self, "{}", insert.id_expr);
        self.comma();

        let _ = self.string_literal().write_str(insert.type_name);

        if let Some(data) = insert.data {
            self.comma();
            let _ = write!(self, "{}", data);
        }

        if let Some(old) = old_values {
            self.comma();
            let _ = write!(self, "{}", old);
        }

        if let Some(meta) = insert.metadata {
            self.comma();
            let _ = write!(self, "{}", meta);
        }

        if let Some(options) = options {
            self.comma();
            let _ = write!(self, "{}", options);
        }

        self.push_str(");\n");
        Ok(())
    }

    pub fn powersync_crud_manual_put(&mut self, name: &str, json_fragment: &str) {
        self.push_str("INSERT INTO powersync_crud_(data) VALUES(json_object('op', 'PUT', 'type', ");
        let _ = self.string_literal().write_str(name);

        let _ = write!(
            self,
            ", 'id', NEW.id, 'data', json(powersync_diff('{{}}', {:}))));",
            json_fragment,
        );
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

pub struct InsertIntoCrud<'a, Id, Data, Metadata>
where
    Id: Display,
    Data: Display,
    Metadata: Display,
{
    pub op: WriteType,
    pub id_expr: Id,
    pub type_name: &'a str,
    pub data: Option<Data>,
    pub table: &'a SchemaTable<'a>,
    pub metadata: Option<Metadata>,
}

#[derive(Clone, Copy, PartialEq)]
pub enum WriteType {
    Insert,
    Update,
    Delete,
}

impl WriteType {
    pub fn ps_crud_op_type(&self) -> &'static str {
        match self {
            WriteType::Insert => "PUT",
            WriteType::Update => "PATCH",
            WriteType::Delete => "DELETE",
        }
    }
}

impl Display for WriteType {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(match self {
            WriteType::Insert => "INSERT",
            WriteType::Update => "UPDATE",
            WriteType::Delete => "DELETE",
        })
    }
}

impl FromStr for WriteType {
    type Err = PowerSyncError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "INSERT" => Self::Insert,
            "UPDATE" => Self::Update,
            "DELETE" => Self::Delete,
            _ => {
                return Err(PowerSyncError::argument_error(format!(
                    "unexpected write type {}",
                    s
                )));
            }
        })
    }
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
