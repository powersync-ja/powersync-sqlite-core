use core::fmt::Write;

use alloc::{
    string::{String, ToString},
    vec,
    vec::Vec,
};
use serde::Deserialize;

use crate::{
    schema::{
        Column, CommonTableOptions, PendingStatement, PendingStatementValue, RawTable, Table,
        raw_table::InferredTableStructure, table_info::RestColumnIndex,
    },
    utils::SqlBuffer,
};

/// Utility to wrap both PowerSync-managed JSON tables and raw tables (with their schema snapshot
/// inferred from reading `pragma_table_info`) into a common implementation.
pub enum SchemaTable<'a> {
    Json(&'a Table),
    Raw {
        definition: &'a RawTable,
        schema: &'a InferredTableStructure,
    },
}

impl<'a> SchemaTable<'a> {
    /// The type name used for the table when referenced in `ps_crud`, `ps_oplog` and other tables.
    pub fn name(&self) -> &str {
        match self {
            SchemaTable::Json(table) => &table.name,
            SchemaTable::Raw {
                definition,
                schema: _,
            } => &definition.name,
        }
    }

    pub fn common_options(&self) -> &CommonTableOptions {
        match self {
            Self::Json(table) => &table.options,
            Self::Raw {
                definition,
                schema: _,
            } => &definition.schema.options,
        }
    }

    pub fn columns(&self) -> &'a [Column] {
        match self {
            Self::Json(table) => &table.columns,
            Self::Raw {
                definition: _,
                schema,
            } => &schema.columns,
        }
    }

    /// Iterates over defined column names in this table (not including the `id` column).
    pub fn column_names(&self) -> impl Iterator<Item = &'a str> {
        self.columns().iter().map(|c| &*c.name)
    }

    /// Generates a statement of the form `INSERT INTO $tbl ($cols) VALUES (?, ...) ON CONFLICT (id)
    /// DO UPDATE SET ...` for the sync client.
    pub fn infer_put_stmt(&self, table_name: &str) -> PendingStatement {
        let mut buffer = SqlBuffer::new();
        let mut params = vec![];
        let mut rest = match self {
            SchemaTable::Json(_) => Some(("_rest", RestColumnIndex::default())),
            SchemaTable::Raw { .. } => None,
        };

        buffer.push_str("INSERT INTO ");
        let _ = buffer.identifier().write_str(table_name);
        buffer.push_str(" (id");
        if let Some((column, _)) = rest {
            let _ = write!(&mut buffer, ", {column}");
        }

        for column in self.column_names() {
            buffer.comma();
            let _ = buffer.identifier().write_str(column);
        }
        buffer.push_str(") VALUES (?1");
        params.push(PendingStatementValue::Id);
        if let Some((_, ref mut rest_index)) = rest {
            params.push(PendingStatementValue::Rest);
            buffer.push_str(", ?2");
            rest_index.rest_parameter_positions.push(1); // this is zero-indexed
        }

        let data_start_index = if rest.is_some() { 3 } else { 2 };
        for (i, column) in self.column_names().enumerate() {
            buffer.comma();
            let _ = write!(&mut buffer, "?{}", i + data_start_index);
            params.push(PendingStatementValue::Column(column.to_string()));

            if let Some((_, ref mut index)) = rest {
                index.named_parameters.insert(column.to_string());
            }
        }
        buffer.push_str(") ON CONFLICT (id) DO UPDATE SET ");
        let mut do_update = buffer.comma_separated();

        if let Some((column, _)) = rest {
            let entry = do_update.element();
            let _ = write!(entry, "{column} = ?2");
        }

        // Generate an "x" = ? for all synced columns to update them without affecting local-only
        // columns.
        for (i, column) in self.column_names().enumerate() {
            let entry = do_update.element();
            let _ = entry.identifier().write_str(column);
            let _ = write!(entry, " = ?{}", i + data_start_index);
        }

        PendingStatement {
            sql: buffer.sql,
            params,
            named_parameters_index: rest.map(|e| e.1),
        }
    }

    /// Generates a statement of the form `DELETE FROM $tbl WHERE id = ?` for the sync client.
    pub fn infer_delete_stmt(&self, table_name: &str) -> PendingStatement {
        let mut buffer = SqlBuffer::new();
        buffer.push_str("DELETE FROM ");
        let _ = buffer.identifier().write_str(table_name);
        buffer.push_str(" WHERE id = ?");

        PendingStatement {
            sql: buffer.sql,
            params: vec![PendingStatementValue::Id],
            named_parameters_index: None,
        }
    }
}

impl<'a> From<&'a Table> for SchemaTable<'a> {
    fn from(value: &'a Table) -> Self {
        Self::Json(value)
    }
}

#[derive(Default)]
pub struct ColumnFilter {
    sorted_names: Vec<String>,
}

impl From<Vec<String>> for ColumnFilter {
    fn from(mut value: Vec<String>) -> Self {
        value.sort();
        Self {
            sorted_names: value,
        }
    }
}

impl ColumnFilter {
    /// Whether this filter matches the given column name.
    pub fn matches(&self, column: &str) -> bool {
        self.sorted_names
            .binary_search_by(|item| item.as_str().cmp(column))
            .is_ok()
    }
}

impl AsRef<Vec<String>> for ColumnFilter {
    fn as_ref(&self) -> &Vec<String> {
        &self.sorted_names
    }
}

impl<'de> Deserialize<'de> for ColumnFilter {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(Self::from(Vec::<String>::deserialize(deserializer)?))
    }
}
#[cfg(test)]
mod test {
    use alloc::{string::ToString, vec};
    use core::assert_matches;

    use crate::schema::{
        Column, PendingStatementValue, RawTable, SchemaTable, raw_table::InferredTableStructure,
        table_info::RawTableSchema,
    };

    #[test]
    fn infer_sync_statements() {
        let raw_table = RawTable {
            name: "users".to_string(),
            schema: RawTableSchema::default(),
            put: None,
            delete: None,
            clear: None,
        };
        let structure = InferredTableStructure {
            columns: vec![
                Column {
                    name: "foo".to_string(),
                    type_name: "TEXT".to_string(),
                },
                Column {
                    name: "bar".to_string(),
                    type_name: "TEXT".to_string(),
                },
            ],
        };
        let schema_table = SchemaTable::Raw {
            definition: &raw_table,
            schema: &structure,
        };

        let put = schema_table.infer_put_stmt("tbl");
        assert_eq!(
            put.sql,
            r#"INSERT INTO "tbl" (id, "foo", "bar") VALUES (?1, ?2, ?3) ON CONFLICT (id) DO UPDATE SET "foo" = ?2, "bar" = ?3"#
        );
        assert_eq!(put.params.len(), 3);
        assert_matches!(put.params[0], PendingStatementValue::Id);
        assert_matches!(
            put.params[1],
            PendingStatementValue::Column(ref name) if name == "foo"
        );
        assert_matches!(
            put.params[2],
            PendingStatementValue::Column(ref name) if name == "bar"
        );

        let delete = schema_table.infer_delete_stmt("tbl");
        assert_eq!(delete.sql, r#"DELETE FROM "tbl" WHERE id = ?"#);
        assert_eq!(delete.params.len(), 1);
        assert_matches!(delete.params[0], PendingStatementValue::Id);
    }
}
