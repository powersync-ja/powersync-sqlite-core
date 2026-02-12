use alloc::rc::Rc;
use alloc::string::ToString;
use alloc::vec;
use alloc::{collections::btree_set::BTreeSet, format, string::String, vec::Vec};
use serde::{Deserialize, de::Visitor};

use crate::error::PowerSyncError;
use crate::schema::ColumnFilter;

#[derive(Deserialize)]
pub struct Table {
    pub name: String,
    #[serde(rename = "view_name")]
    pub view_name_override: Option<String>,
    pub columns: Vec<Column>,
    #[serde(default)]
    pub indexes: Vec<Index>,
    #[serde(flatten)]
    pub options: CommonTableOptions,
}

/// Options shared between regular and raw tables.
#[derive(Deserialize, Default)]
pub struct CommonTableOptions {
    #[serde(
        default,
        rename = "include_old",
        deserialize_with = "deserialize_include_old"
    )]
    pub diff_include_old: Option<DiffIncludeOld>,
    #[serde(flatten)]
    pub flags: TableInfoFlags,
}

#[derive(Deserialize, Default)]
pub struct RawTableSchema {
    /// The actual name of the raw table in the local schema.
    ///
    /// Currently, this is only used to generate `CREATE TRIGGER` statements for the raw table.
    #[serde(default)]
    pub table_name: Option<String>,
    #[serde(default)]
    pub synced_columns: Option<ColumnFilter>,
    #[serde(flatten)]
    pub options: CommonTableOptions,
}

#[derive(Deserialize)]
pub struct RawTable {
    /// The [crate::sync::line::OplogEntry::object_type] for which rows should be forwarded to this
    /// raw table.
    ///
    /// This is not necessarily the same as the local name of the raw table.
    pub name: String,
    #[serde(flatten, default)]
    pub schema: RawTableSchema,
    pub put: Option<Rc<PendingStatement>>,
    pub delete: Option<Rc<PendingStatement>>,
    #[serde(default)]
    pub clear: Option<String>,
}

impl Table {
    pub fn view_name(&self) -> &str {
        self.view_name_override
            .as_deref()
            .unwrap_or(self.name.as_str())
    }

    pub fn local_only(&self) -> bool {
        self.options.flags.local_only()
    }

    pub fn internal_name(&self) -> String {
        if self.local_only() {
            format!("ps_data_local__{:}", self.name)
        } else {
            format!("ps_data__{:}", self.name)
        }
    }
}

impl RawTable {
    pub fn require_table_name(&self) -> Result<&str, PowerSyncError> {
        let Some(local_table_name) = self.schema.table_name.as_ref() else {
            return Err(PowerSyncError::argument_error(format!(
                "Raw table {} has no local name",
                self.name,
            )));
        };
        Ok(local_table_name)
    }
}

#[derive(Deserialize)]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
    pub type_name: String,
}

#[derive(Deserialize)]
pub struct Index {
    pub name: String,
    pub columns: Vec<IndexedColumn>,
}

#[derive(Deserialize)]
pub struct IndexedColumn {
    pub name: String,
    pub ascending: bool,
    #[serde(rename = "type")]
    pub type_name: String,
}

pub enum DiffIncludeOld {
    OnlyForColumns(ColumnFilter),
    ForAllColumns,
}

impl DiffIncludeOld {
    pub fn column_filter(&self) -> Option<&ColumnFilter> {
        match self {
            Self::ForAllColumns => None,
            Self::OnlyForColumns(filter) => Some(filter),
        }
    }
}

fn deserialize_include_old<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<DiffIncludeOld>, D::Error> {
    struct IncludeOldVisitor;

    impl<'de> Visitor<'de> for IncludeOldVisitor {
        type Value = Option<DiffIncludeOld>;

        fn expecting(&self, formatter: &mut core::fmt::Formatter) -> core::fmt::Result {
            write!(formatter, "an array of columns, or true")
        }

        fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            deserializer.deserialize_any(self)
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            return Ok(None);
        }

        fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(if v {
                Some(DiffIncludeOld::ForAllColumns)
            } else {
                None
            })
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            let mut elements: Vec<String> = vec![];
            while let Some(next) = seq.next_element::<String>()? {
                elements.push(next);
            }

            Ok(Some(DiffIncludeOld::OnlyForColumns(elements.into())))
        }
    }

    deserializer.deserialize_option(IncludeOldVisitor)
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct TableInfoFlags(pub u32);

impl TableInfoFlags {
    pub const LOCAL_ONLY: u32 = 1;
    pub const INSERT_ONLY: u32 = 2;
    pub const INCLUDE_METADATA: u32 = 4;
    pub const INCLUDE_OLD_ONLY_WHEN_CHANGED: u32 = 8;
    pub const IGNORE_EMPTY_UPDATE: u32 = 16;

    pub const fn local_only(self) -> bool {
        self.0 & Self::LOCAL_ONLY != 0
    }

    pub const fn insert_only(self) -> bool {
        // Note: insert_only is incompatible with local_only. For backwards compatibility, we want
        // to silently ignore insert_only if local_only is set.
        if self.local_only() {
            return false;
        }

        self.0 & Self::INSERT_ONLY != 0
    }

    pub const fn include_metadata(self) -> bool {
        self.0 & Self::INCLUDE_METADATA != 0
    }

    pub const fn include_old_only_when_changed(self) -> bool {
        self.0 & Self::INCLUDE_OLD_ONLY_WHEN_CHANGED != 0
    }

    pub const fn ignore_empty_update(self) -> bool {
        self.0 & Self::IGNORE_EMPTY_UPDATE != 0
    }

    const fn with_flag(self, flag: u32) -> Self {
        Self(self.0 | flag)
    }

    const fn without_flag(self, flag: u32) -> Self {
        Self(self.0 & !flag)
    }

    const fn set_flag(self, flag: u32, enable: bool) -> Self {
        if enable {
            self.with_flag(flag)
        } else {
            self.without_flag(flag)
        }
    }
}

impl Default for TableInfoFlags {
    fn default() -> Self {
        Self(0)
    }
}

impl<'de> Deserialize<'de> for TableInfoFlags {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct FlagsVisitor;

        impl<'de> Visitor<'de> for FlagsVisitor {
            type Value = TableInfoFlags;

            fn expecting(&self, formatter: &mut core::fmt::Formatter) -> core::fmt::Result {
                write!(formatter, "an object with table flags")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut flags = TableInfoFlags::default();

                while let Some((key, value)) = map.next_entry::<&'de str, bool>()? {
                    flags = flags.set_flag(
                        match key {
                            "local_only" => TableInfoFlags::LOCAL_ONLY,
                            "insert_only" => TableInfoFlags::INSERT_ONLY,
                            "include_metadata" => TableInfoFlags::INCLUDE_METADATA,
                            "include_old_only_when_changed" => {
                                TableInfoFlags::INCLUDE_OLD_ONLY_WHEN_CHANGED
                            }
                            "ignore_empty_update" => TableInfoFlags::IGNORE_EMPTY_UPDATE,
                            _ => continue,
                        },
                        value,
                    );
                }

                Ok(flags)
            }
        }

        deserializer.deserialize_struct(
            "TableInfoFlags",
            &[
                "local_only",
                "insert_only",
                "include_metadata",
                "include_old_only_when_changed",
                "ignore_empty_update",
            ],
            FlagsVisitor,
        )
    }
}

pub struct PendingStatement {
    pub sql: String,
    /// This vec should contain an entry for each parameter in [sql].
    pub params: Vec<PendingStatementValue>,

    /// Present if this statement has a [PendingStatementValue::Rest] parameter.
    pub named_parameters_index: Option<RestColumnIndex>,
}

pub struct RestColumnIndex {
    /// All column names referenced by this statement.
    pub named_parameters: BTreeSet<String>,
    /// Parameter indices that should be bound to a JSON object containing those values from the
    /// source row that haven't been referenced by [PendingStatementValue::Column].
    pub rest_parameter_positions: Vec<usize>,
}

impl<'de> Deserialize<'de> for PendingStatement {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct PendingStatementSource {
            pub sql: String,
            /// This vec should contain an entry for each parameter in [sql].
            pub params: Vec<PendingStatementValue>,
        }

        let source = PendingStatementSource::deserialize(deserializer)?;
        let mut named_parameters_index = None;
        if source
            .params
            .iter()
            .any(|s| matches!(s, PendingStatementValue::Rest))
        {
            let mut set = BTreeSet::new();
            let mut rest_parameter_positions = vec![];
            for (i, column) in source.params.iter().enumerate() {
                set.insert(match column {
                    PendingStatementValue::Id => "id".to_string(),
                    PendingStatementValue::Column(name) => name.clone(),
                    PendingStatementValue::Rest => {
                        rest_parameter_positions.push(i);
                        continue;
                    }
                });
            }

            named_parameters_index = Some(RestColumnIndex {
                named_parameters: set,
                rest_parameter_positions,
            });
        }

        return Ok(Self {
            sql: source.sql,
            params: source.params,
            named_parameters_index,
        });
    }
}

#[derive(Deserialize)]
pub enum PendingStatementValue {
    /// Bind to the PowerSync row id of the affected row.
    Id,
    /// Bind to the value of column in the synced row.
    Column(String),
    /// Bind to a JSON object containing all columns from the synced row that haven't been matched
    /// by other statement values.
    Rest,
    // TODO: Stuff like a raw object of put data?
}
