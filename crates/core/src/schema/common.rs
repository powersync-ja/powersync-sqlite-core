use core::slice;

use alloc::{string::String, vec::Vec};

use crate::schema::{
    Column, CommonTableOptions, RawTable, Table, raw_table::InferredTableStructure,
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
    pub fn common_options(&self) -> &CommonTableOptions {
        match self {
            Self::Json(table) => &table.options,
            Self::Raw {
                definition,
                schema: _,
            } => &definition.schema.options,
        }
    }

    /// Iterates over defined column names in this table (not including the `id` column).
    pub fn column_names(&self) -> impl Iterator<Item = &'a str> {
        match self {
            Self::Json(table) => SchemaTableColumnIterator::Json(table.columns.iter()),
            Self::Raw {
                definition: _,
                schema,
            } => SchemaTableColumnIterator::Raw(schema.columns.iter()),
        }
    }
}

impl<'a> From<&'a Table> for SchemaTable<'a> {
    fn from(value: &'a Table) -> Self {
        Self::Json(value)
    }
}

enum SchemaTableColumnIterator<'a> {
    Json(slice::Iter<'a, Column>),
    Raw(slice::Iter<'a, String>),
}

impl<'a> Iterator for SchemaTableColumnIterator<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        Some(match self {
            Self::Json(iter) => &iter.next()?.name,
            Self::Raw(iter) => iter.next()?.as_ref(),
        })
    }
}

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
