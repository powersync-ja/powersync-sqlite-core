use alloc::{format, string::String, vec, vec::Vec};
use serde::{de::Visitor, Deserialize};

#[derive(Deserialize)]
pub struct Table {
    pub name: String,
    #[serde(rename = "view_name")]
    pub view_name_override: Option<String>,
    pub columns: Vec<Column>,
    #[serde(default)]
    pub indexes: Vec<Index>,
    #[serde(
        default,
        rename = "include_old",
        deserialize_with = "deserialize_include_old"
    )]
    pub diff_include_old: Option<DiffIncludeOld>,
    #[serde(flatten)]
    pub flags: TableInfoFlags,
}

#[derive(Deserialize)]
pub struct RawTable {
    pub name: String,
    pub put: PendingStatement,
    pub delete: PendingStatement,
}

impl Table {
    pub fn from_json(text: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(text)
    }

    pub fn view_name(&self) -> &str {
        self.view_name_override
            .as_deref()
            .unwrap_or(self.name.as_str())
    }

    pub fn internal_name(&self) -> String {
        if self.flags.local_only() {
            format!("ps_data_local__{:}", self.name)
        } else {
            format!("ps_data__{:}", self.name)
        }
    }

    pub fn filtered_columns<'a>(
        &'a self,
        names: impl Iterator<Item = &'a str>,
    ) -> impl Iterator<Item = &'a Column> {
        // First, sort all columns by name for faster lookups by name.
        let mut sorted_by_name: Vec<&Column> = self.columns.iter().collect();
        sorted_by_name.sort_by_key(|c| &*c.name);

        names.filter_map(move |name| {
            let index = sorted_by_name
                .binary_search_by_key(&name, |c| c.name.as_str())
                .ok()?;

            Some(sorted_by_name[index])
        })
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
    OnlyForColumns { columns: Vec<String> },
    ForAllColumns,
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

            Ok(Some(DiffIncludeOld::OnlyForColumns { columns: elements }))
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

#[derive(Deserialize)]
pub struct PendingStatement {
    pub sql: String,
    /// This vec should contain an entry for each parameter in [sql].
    pub params: Vec<PendingStatementValue>,
}

#[derive(Deserialize)]
pub enum PendingStatementValue {
    Id,
    Column(String),
    // TODO: Stuff like a raw object of put data?
}
