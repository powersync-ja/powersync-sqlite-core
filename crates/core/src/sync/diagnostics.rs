use alloc::vec;
use alloc::{
    borrow::Cow,
    collections::btree_map::BTreeMap,
    string::{String, ToString},
    vec::Vec,
};
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{IgnoredAny, Visitor},
};

use crate::sync::{
    interface::{Instruction, StartSyncStream},
    line::{DataLine, OplogData, SyncLineStr},
    sync_status::{BucketProgress, DownloadSyncStatus},
};

#[derive(Deserialize)]
pub struct DiagnosticOptions {
    // currently empty, we enable diagnostics if an Option<Self> is Some()
}

#[derive(Serialize)]
pub enum DiagnosticsEvent {
    BucketStateChange {
        changes: Vec<BucketDownloadState>,
        incremental: bool,
    },
    SchemaChange(ObservedSchemaType),
}

#[derive(Serialize)]
pub struct BucketDownloadState {
    pub name: String,
    pub progress: BucketProgress,
}

#[derive(Serialize)]
pub struct ObservedSchemaType {
    pub table: String,
    pub column: String,
    pub value_type: ValueType,
}

#[derive(Serialize, Clone, Copy, PartialEq)]
pub enum ValueType {
    Null,
    String,
    Integer,
    Real,
}

#[derive(Default)]
pub struct DiagnosticsCollector {
    inferred_schema: BTreeMap<String, BTreeMap<String, ValueType>>,
}

impl DiagnosticsCollector {
    pub fn for_options(options: &StartSyncStream) -> Option<Self> {
        options.diagnostics.as_ref().map(|_| Self::default())
    }

    pub fn handle_tracking_checkpoint(
        &self,
        status: &DownloadSyncStatus,
        instructions: &mut Vec<Instruction>,
    ) {
        let mut buckets = vec![];
        if let Some(downloading) = &status.downloading {
            for (name, progress) in &downloading.buckets {
                buckets.push(BucketDownloadState {
                    name: name.clone(),
                    progress: progress.clone(),
                });
            }
        }

        instructions.push(Instruction::HandleDiagnostics(
            DiagnosticsEvent::BucketStateChange {
                changes: buckets,
                incremental: false,
            },
        ));
    }

    /// Updates the internal inferred schema with types from the handled data line.
    ///
    /// Emits a diagnostic line for each changed column.
    pub fn handle_data_line<'a>(
        &mut self,
        line: &'a DataLine<'a>,
        status: &DownloadSyncStatus,
        instructions: &mut Vec<Instruction>,
    ) {
        if let Some(download_status) = &status.downloading {
            if let Some(progress) = download_status.buckets.get(line.bucket.as_ref()) {
                let mut changes = vec![];
                changes.push(BucketDownloadState {
                    name: line.bucket.to_string(),
                    progress: progress.clone(),
                });

                instructions.push(Instruction::HandleDiagnostics(
                    DiagnosticsEvent::BucketStateChange {
                        changes,
                        incremental: true,
                    },
                ));
            }
        }

        for op in &line.data {
            if let (Some(data), Some(object_type)) = (&op.data, &op.object_type) {
                let OplogData::Json { data } = data;
                let table = self
                    .inferred_schema
                    .entry(object_type.to_string())
                    .or_default();

                let mut de = serde_json::Deserializer::from_str(data);

                struct TypeInferringVisitor<'a> {
                    table_name: &'a str,
                    table: &'a mut BTreeMap<String, ValueType>,
                    instructions: &'a mut Vec<Instruction>,
                }

                impl TypeInferringVisitor<'_> {
                    fn observe_type<'a>(&mut self, name: Cow<'a, str>, column_type: ValueType) {
                        if column_type == ValueType::Null {
                            // We don't track nullability in the inferred schema.
                            return;
                        }

                        if let Some(existing) = self.table.get_mut(name.as_ref()) {
                            if *existing != column_type && *existing != ValueType::String {
                                *existing = column_type;

                                self.instructions.push(Instruction::HandleDiagnostics(
                                    DiagnosticsEvent::SchemaChange(ObservedSchemaType {
                                        table: self.table_name.to_string(),
                                        column: name.into_owned(),
                                        value_type: column_type,
                                    }),
                                ));
                            }
                        } else {
                            let name = name.into_owned();
                            self.table.insert(name.clone(), column_type);

                            self.instructions.push(Instruction::HandleDiagnostics(
                                DiagnosticsEvent::SchemaChange(ObservedSchemaType {
                                    table: self.table_name.to_string(),
                                    column: name,
                                    value_type: column_type,
                                }),
                            ));
                        }
                    }
                }

                impl<'de> Visitor<'de> for TypeInferringVisitor<'de> {
                    type Value = ();

                    fn expecting(&self, formatter: &mut core::fmt::Formatter) -> core::fmt::Result {
                        write!(formatter, "a map")
                    }

                    fn visit_map<A>(mut self, mut map: A) -> Result<Self::Value, A::Error>
                    where
                        A: serde::de::MapAccess<'de>,
                    {
                        while let Some(k) = map.next_key::<SyncLineStr<'de>>()? {
                            if k == "id" {
                                map.next_value::<IgnoredAny>()?;
                            } else {
                                let value_type = map.next_value::<ValueToValueType>()?.0;
                                self.observe_type(k, value_type);
                            }
                        }

                        Ok(())
                    }
                }

                let _ = de.deserialize_map(TypeInferringVisitor {
                    table_name: object_type,
                    table,
                    instructions,
                });
            }
        }
    }
}

/// Utility to deserialize the [ValueType] from a [serde_json::Value] without reading it into a
/// structure that requires allocation.
struct ValueToValueType(ValueType);

impl<'de> Deserialize<'de> for ValueToValueType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct ValueTypeVisitor;

        impl<'de> Visitor<'de> for ValueTypeVisitor {
            type Value = ValueType;

            fn expecting(&self, formatter: &mut core::fmt::Formatter) -> core::fmt::Result {
                write!(formatter, "a sync value")
            }

            fn visit_f64<E>(self, _v: f64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(ValueType::Real)
            }

            fn visit_u64<E>(self, _v: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(ValueType::Integer)
            }

            fn visit_i64<E>(self, _v: i64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(ValueType::Integer)
            }

            fn visit_str<E>(self, _v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(ValueType::String)
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                // Unit is used to represent nulls, see https://github.com/serde-rs/json/blob/4f6dbfac79647d032b0997b5ab73022340c6dab7/src/de.rs#L1404-L1409
                Ok(ValueType::Null)
            }
        }

        Ok(ValueToValueType(
            deserializer.deserialize_any(ValueTypeVisitor)?,
        ))
    }
}
