// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;

use serde::{Deserialize, Serialize, Serializer};
use typed_builder::TypedBuilder;

use crate::error::invalid_data;
use crate::expr::BoundPredicate;
use crate::spec::{
    DataContentType, DataFile, DataFileFormat, ManifestEntryRef, NameMapping,
    PartitionSpec, Schema, SchemaRef, SortOrderRef, Struct, StructType,
};
use crate::{Error, ErrorKind, Result};

/// One maintenance planning result that retains the source manifest data file.
/// Ordinary query planning does not construct this wrapper.
#[derive(Debug)]
pub struct PlannedFileScanTask {
    pub task: FileScanTask,
    pub data_file: DataFile,
}

/// Current-snapshot scan inventory produced by one manifest traversal.
#[derive(Debug)]
pub struct FileScanPlan {
    pub files: Vec<PlannedFileScanTask>,
    pub manifest_count: usize,
}

/// Serialization helper that always returns NotImplementedError.
/// Used for fields that should not be serialized but we want to be explicit about it.
fn serialize_not_implemented<S, T>(
    _: &T,
    _: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    Err(serde::ser::Error::custom(
        "Serialization not implemented for this field",
    ))
}

/// Deserialization helper that always returns NotImplementedError.
/// Used for fields that should not be deserialized but we want to be explicit about it.
fn deserialize_not_implemented<'de, D, T>(_: D) -> std::result::Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Err(serde::de::Error::custom(
        "Deserialization not implemented for this field",
    ))
}

/// A task to scan part of file.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, TypedBuilder)]
#[builder(field_defaults(setter(prefix = "with_")))]
pub struct FileScanTask {
    /// The total size of the data file in bytes, from the manifest entry.
    /// Used to avoid an additional metadata lookup when reading Parquet footers.
    pub file_size_in_bytes: u64,
    /// The start offset of the file to scan.
    pub start: u64,
    /// The length of the file to scan.
    pub length: u64,
    /// The number of records in the file to scan.
    ///
    /// This is an optional field, and only available if we are
    /// reading the entire data file.
    #[builder(default)]
    pub record_count: Option<u64>,

    /// Effective first row id for this data file in format v3 tables.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub first_row_id: Option<u64>,

    /// Data sequence number inherited from the manifest entry.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub data_sequence_number: Option<i64>,

    /// The data file path corresponding to the task.
    pub data_file_path: String,

    /// The format of the file to scan.
    pub data_file_format: DataFileFormat,

    /// ID of the partition spec used to encode this data file's partition.
    #[serde(default)]
    #[builder(default)]
    pub partition_spec_id: i32,

    /// The schema of the file to scan.
    pub schema: SchemaRef,
    /// The field ids to project.
    pub project_field_ids: Vec<i32>,
    /// The predicate to filter.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub predicate: Option<BoundPredicate>,

    /// The list of delete files that may need to be applied to this data file
    #[builder(default)]
    pub deletes: Vec<FileScanTaskDeleteFile>,

    /// Partition data from the manifest entry, used to identify which columns can use
    /// constant values from partition metadata vs. reading from the data file.
    /// Per the Iceberg spec, only identity-transformed partition fields should use constants.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "serialize_not_implemented")]
    #[serde(deserialize_with = "deserialize_not_implemented")]
    #[builder(default)]
    pub partition: Option<Struct>,

    /// The partition spec for this file, used to distinguish identity transforms
    /// (which use partition metadata constants) from non-identity transforms like
    /// bucket/truncate (which must read source columns from the data file).
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "serialize_not_implemented")]
    #[serde(deserialize_with = "deserialize_not_implemented")]
    #[builder(default)]
    pub partition_spec: Option<Arc<PartitionSpec>>,

    /// Union of partition fields across all table partition specs.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "serialize_not_implemented")]
    #[serde(deserialize_with = "deserialize_not_implemented")]
    #[builder(default)]
    pub unified_partition_type: Option<Arc<StructType>>,

    /// Raw sort order id recorded on the data file.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub sort_order_id: Option<i32>,

    /// Resolved non-empty sort order for this data file, when available.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub sort_order: Option<SortOrderRef>,

    /// Name mapping from table metadata (property: schema.name-mapping.default),
    /// used to resolve field IDs from column names when Parquet files lack field IDs
    /// or have field ID conflicts.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "serialize_not_implemented")]
    #[serde(deserialize_with = "deserialize_not_implemented")]
    #[builder(default)]
    pub name_mapping: Option<Arc<NameMapping>>,

    /// Whether this scan task should treat column names as case-sensitive when binding predicates.
    pub case_sensitive: bool,

    /// Key metadata for encrypted data files (Parquet Modular Encryption).
    /// When present, the reader uses this to build `FileDecryptionProperties`.
    ///
    /// Note on the trust boundary: for the standard encryption scheme this
    /// carries `StandardKeyMetadata`, whose payload is the *plaintext* DEK.
    /// Because `FileScanTask` derives `Serialize`, that plaintext DEK is part
    /// of the serialized scan plan should these tasks ever be serialized and sent
    /// over the network.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub key_metadata: Option<Box<[u8]>>,
}

impl FileScanTask {
    /// Validate that partition values, partition spec, and scan schema form a
    /// self-consistent task.
    ///
    /// This is deliberately separate from the builder so existing callers
    /// that construct tasks directly remain source-compatible. Arrow readers
    /// treat scan tasks as trusted execution plans and do not call this method
    /// on the read hot path. Direct constructors must uphold the task
    /// invariants and should call this method when their input is not already
    /// trusted.
    pub fn validate(&self) -> Result<()> {
        match (self.partition.as_ref(), self.partition_spec.as_deref()) {
            (None, None) => Ok(()),
            (None, Some(partition_spec)) if partition_spec.is_unpartitioned() => {
                Ok(())
            }
            (None, Some(_)) => Err(Error::new(
                ErrorKind::DataInvalid,
                "FileScanTask with a partitioned spec requires partition values",
            )),
            (Some(partition), None) if partition.fields().is_empty() => Ok(()),
            (Some(_), None) => Err(Error::new(
                ErrorKind::DataInvalid,
                "Non-empty FileScanTask partition requires a partition spec",
            )),
            (Some(partition), Some(partition_spec))
                if partition.fields().len() != partition_spec.fields().len() =>
            {
                Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "FileScanTask partition has {} fields but partition spec has {} fields",
                        partition.fields().len(),
                        partition_spec.fields().len()
                    ),
                ))
            }
            (Some(_), Some(partition_spec)) => {
                partition_spec.partition_type(&self.schema)?;
                Ok(())
            }
        }
    }

    /// Returns whether this task reads the entire data file.
    ///
    /// Manifest-planned tasks use the canonical range
    /// `start = 0, length = file_size_in_bytes`. The zero range remains a
    /// whole-file representation because the reader accepts it for backwards
    /// compatibility.
    pub fn is_whole_file(&self) -> bool {
        self.start == 0
            && (self.length == 0 || self.length == self.file_size_in_bytes)
    }

    /// Returns the data file path of this file scan task.
    pub fn data_file_path(&self) -> &str {
        &self.data_file_path
    }

    /// Returns the project field id of this file scan task.
    pub fn project_field_ids(&self) -> &[i32] {
        &self.project_field_ids
    }

    /// Returns the predicate of this file scan task.
    pub fn predicate(&self) -> Option<&BoundPredicate> {
        self.predicate.as_ref()
    }

    /// Replace the predicate used by the reader for this task.
    ///
    /// File planning and row filtering can intentionally use different
    /// predicates: planning needs a stable pruning predicate, while repeated
    /// reads of the same planned task list may need the current runtime row
    /// predicate.
    pub fn set_predicate(&mut self, predicate: Option<BoundPredicate>) {
        self.predicate = predicate;
    }

    /// Returns the schema of this file scan task as a reference
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Returns the schema of this file scan task as a SchemaRef
    pub fn schema_ref(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Returns the partition spec id for this file scan task.
    pub fn partition_spec_id(&self) -> i32 {
        self.partition_spec_id
    }

    /// Returns the raw sort order id recorded on the data file.
    pub fn sort_order_id(&self) -> Option<i32> {
        self.sort_order_id
    }

    /// Returns the resolved non-empty sort order, when available.
    pub fn sort_order(&self) -> Option<&SortOrderRef> {
        self.sort_order.as_ref()
    }
}

#[derive(Debug)]
pub(crate) struct DeleteFileContext {
    pub(crate) manifest_entry: ManifestEntryRef,
    pub(crate) partition_spec_id: i32,
}

impl TryFrom<&DeleteFileContext> for FileScanTaskDeleteFile {
    type Error = Error;

    fn try_from(ctx: &DeleteFileContext) -> Result<Self> {
        let data_file = &ctx.manifest_entry.data_file;
        let referenced_data_file = if data_file.is_deletion_vector() {
            data_file.referenced_data_file_path()
        } else {
            data_file.position_delete_target_data_file_path()
        };
        let task = FileScanTaskDeleteFile {
            file_path: ctx.manifest_entry.file_path().to_string(),
            file_size_in_bytes: ctx.manifest_entry.file_size_in_bytes(),
            file_type: ctx.manifest_entry.content_type(),
            file_format: ctx.manifest_entry.file_format(),
            partition_spec_id: ctx.partition_spec_id,
            equality_ids: ctx.manifest_entry.data_file.equality_ids.clone(),
            key_metadata: data_file.key_metadata().map(Box::from),
            referenced_data_file: referenced_data_file.map(str::to_owned),
            content_offset: data_file.content_offset(),
            content_size_in_bytes: data_file.content_size_in_bytes(),
            record_count: data_file.record_count(),
        };
        task.validate()?;
        Ok(task)
    }
}

fn default_delete_file_format() -> DataFileFormat {
    DataFileFormat::Parquet
}

/// A task to scan part of file.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, TypedBuilder)]
#[serde(try_from = "DeleteFileTaskSerde")]
#[builder(field_defaults(setter(prefix = "with_")), build_method(into = Result<FileScanTaskDeleteFile>))]
pub struct FileScanTaskDeleteFile {
    /// The delete file path
    pub(crate) file_path: String,

    /// The total size of the delete file in bytes, from the manifest entry.
    pub(crate) file_size_in_bytes: u64,

    /// delete file type
    pub(crate) file_type: DataContentType,

    /// delete file format
    #[serde(default = "default_delete_file_format")]
    #[builder(default = DataFileFormat::Parquet)]
    pub(crate) file_format: DataFileFormat,

    /// partition id
    pub(crate) partition_spec_id: i32,

    /// equality ids for equality deletes (null for anything other than equality-deletes)
    #[serde(default)]
    #[builder(default)]
    pub(crate) equality_ids: Option<Vec<i32>>,

    /// Key metadata for encrypted delete files (Parquet Modular Encryption).
    /// When present, the reader uses this to build `FileDecryptionProperties`.
    ///
    /// Same plaintext-DEK trust boundary as [`FileScanTask::key_metadata`](super::FileScanTask::key_metadata):
    /// this is part of the serialized scan plan should these tasks be serialized.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub(crate) key_metadata: Option<Box<[u8]>>,

    /// Effective data file path referenced by file-scoped position deletes or
    /// deletion vectors.
    ///
    /// For legacy Parquet position deletes this may be inferred from
    /// `file_path` lower/upper bounds in the delete manifest entry, not only
    /// from the manifest's explicit `referenced_data_file` field.
    #[serde(default)]
    #[builder(default)]
    pub(crate) referenced_data_file: Option<String>,

    /// Offset of a referenced Puffin blob for deletion vectors.
    #[serde(default)]
    #[builder(default)]
    pub(crate) content_offset: Option<i64>,

    /// Size of a referenced Puffin blob for deletion vectors.
    #[serde(default)]
    #[builder(default)]
    pub(crate) content_size_in_bytes: Option<i64>,

    /// Delete record count, or deletion-vector cardinality for Puffin DVs.
    pub(crate) record_count: u64,
}

impl FileScanTaskDeleteFile {
    /// Returns true when this scan task delete file is a position delete file.
    pub fn is_position_delete(&self) -> bool {
        self.file_type == DataContentType::PositionDeletes
    }

    /// Returns true when this scan task delete file is an Iceberg deletion
    /// vector.
    pub fn is_deletion_vector(&self) -> bool {
        self.is_position_delete() && self.file_format == DataFileFormat::Puffin
    }

    /// Returns the effective target data file path for a file-scoped position
    /// delete or deletion vector.
    pub fn referenced_data_file_path(&self) -> Option<&str> {
        self.referenced_data_file.as_deref()
    }

    /// Returns true when this is a legacy position delete that may contain
    /// deletes for more than one data file.
    pub fn is_broad_scoped_position_delete(&self) -> bool {
        self.is_position_delete()
            && !self.is_deletion_vector()
            && self.referenced_data_file.is_none()
    }

    /// Returns true when a DV rewrite that has merged this effective
    /// file-scoped delete can remove the old delete file from table metadata.
    pub fn can_remove_after_dv_rewrite(&self) -> bool {
        self.is_deletion_vector()
            || (self.is_position_delete() && self.referenced_data_file.is_some())
    }
}

#[derive(Deserialize)]
struct DeleteFileTaskSerde {
    file_path: String,
    file_size_in_bytes: u64,
    file_type: DataContentType,
    #[serde(default = "default_delete_file_format")]
    file_format: DataFileFormat,
    partition_spec_id: i32,
    equality_ids: Option<Vec<i32>>,
    key_metadata: Option<Box<[u8]>>,
    referenced_data_file: Option<String>,
    content_offset: Option<i64>,
    content_size_in_bytes: Option<i64>,
    record_count: u64,
}

impl TryFrom<DeleteFileTaskSerde> for FileScanTaskDeleteFile {
    type Error = Error;

    fn try_from(value: DeleteFileTaskSerde) -> Result<Self> {
        let task = Self {
            file_path: value.file_path,
            file_size_in_bytes: value.file_size_in_bytes,
            file_type: value.file_type,
            file_format: value.file_format,
            partition_spec_id: value.partition_spec_id,
            equality_ids: value.equality_ids,
            key_metadata: value.key_metadata,
            referenced_data_file: value.referenced_data_file,
            content_offset: value.content_offset,
            content_size_in_bytes: value.content_size_in_bytes,
            record_count: value.record_count,
        };
        task.validate()?;
        Ok(task)
    }
}

impl From<FileScanTaskDeleteFile> for Result<FileScanTaskDeleteFile> {
    fn from(task: FileScanTaskDeleteFile) -> Self {
        task.validate()?;
        Ok(task)
    }
}

impl FileScanTaskDeleteFile {
    /// Returns the delete file's file path.
    pub fn file_path(&self) -> &str {
        &self.file_path
    }

    /// Returns the delete file's file size in bytes.
    pub fn file_size_in_bytes(&self) -> u64 {
        self.file_size_in_bytes
    }

    /// Returns the delete file's file type.
    pub fn file_type(&self) -> DataContentType {
        self.file_type
    }

    /// Returns the delete file's file format.
    pub fn file_format(&self) -> DataFileFormat {
        self.file_format
    }

    /// Returns the delete file's partition spec id.
    pub fn partition_spec_id(&self) -> i32 {
        self.partition_spec_id
    }

    /// Returns the delete file's equality ids.
    pub fn equality_ids(&self) -> Option<&[i32]> {
        self.equality_ids.as_deref()
    }

    /// Returns the delete file's key metadata.
    pub fn key_metadata(&self) -> Option<&[u8]> {
        self.key_metadata.as_deref()
    }

    /// Returns the delete file's content offset.
    pub fn content_offset(&self) -> Option<i64> {
        self.content_offset
    }

    /// Returns the delete file's content size in bytes.
    pub fn content_size_in_bytes(&self) -> Option<i64> {
        self.content_size_in_bytes
    }

    /// Returns the delete file's record count.
    pub fn record_count(&self) -> u64 {
        self.record_count
    }

    pub(super) fn validate(&self) -> Result<()> {
        for (name, value) in [
            ("content_offset", self.content_offset),
            ("content_size_in_bytes", self.content_size_in_bytes),
        ] {
            if value.is_some_and(|value| value < 0) {
                return Err(invalid_data!(
                    "delete file {} has negative {name}",
                    self.file_path
                ));
            }
        }
        if self.content_offset.is_some() && self.content_size_in_bytes.is_none() {
            return Err(invalid_data!(
                "delete file {} is missing content_size_in_bytes while content_offset is present",
                self.file_path
            ));
        }
        if self.is_deletion_vector() {
            let missing = if self.referenced_data_file.is_none() {
                Some("referenced_data_file")
            } else if self.content_offset.is_none() {
                Some("content_offset")
            } else if self.content_size_in_bytes.is_none() {
                Some("content_size_in_bytes")
            } else {
                None
            };
            if let Some(field) = missing {
                return Err(invalid_data!(
                    "deletion vector {} is missing {field}",
                    self.file_path
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::spec::{Literal, NestedField, PrimitiveType, Transform, Type};

    fn task(
        schema: SchemaRef,
        partition: Option<Struct>,
        partition_spec: Option<Arc<PartitionSpec>>,
    ) -> FileScanTask {
        FileScanTask::builder()
            .with_file_size_in_bytes(100)
            .with_start(0)
            .with_length(100)
            .with_data_file_path("data.parquet".to_owned())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(schema)
            .with_project_field_ids(Vec::new())
            .with_partition(partition)
            .with_partition_spec(partition_spec)
            .with_case_sensitive(true)
            .build()
    }

    #[test]
    fn validates_partition_shape_without_breaking_direct_construction() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![Arc::new(NestedField::required(
                    1,
                    "x",
                    Type::Primitive(PrimitiveType::Long),
                ))])
                .build()
                .unwrap(),
        );
        let spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .add_partition_field("x", "x", Transform::Identity)
                .unwrap()
                .build()
                .unwrap(),
        );

        task(
            schema.clone(),
            Some(Struct::from_iter([Some(Literal::long(7))])),
            Some(spec.clone()),
        )
        .validate()
        .unwrap();

        let error = task(schema, Some(Struct::empty()), Some(spec))
            .validate()
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert!(error.message().contains("partition has 0 fields"));
    }

    #[test]
    fn rejects_non_empty_partition_without_spec() {
        let schema = Arc::new(Schema::builder().build().unwrap());
        let error = task(
            schema,
            Some(Struct::from_iter([Some(Literal::long(7))])),
            None,
        )
        .validate()
        .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
    }

    #[test]
    fn serde_rejects_incomplete_or_negative_dv_coordinates() {
        // Use the type's serialized representation, including enum spelling.
        let task = FileScanTaskDeleteFile::builder()
            .with_file_path("dv.puffin".into())
            .with_file_size_in_bytes(100)
            .with_file_type(DataContentType::PositionDeletes)
            .with_file_format(DataFileFormat::Puffin)
            .with_partition_spec_id(0)
            .with_referenced_data_file(Some("data.parquet".into()))
            .with_content_offset(Some(4))
            .with_content_size_in_bytes(Some(34))
            .with_record_count(3)
            .build()
            .unwrap();
        let valid = serde_json::to_value(&task).unwrap();
        assert_eq!(
            serde_json::from_value::<FileScanTaskDeleteFile>(valid.clone()).unwrap(),
            task
        );
        for field in [
            "referenced_data_file",
            "content_offset",
            "content_size_in_bytes",
            "record_count",
        ] {
            let mut incomplete = valid.clone();
            incomplete.as_object_mut().unwrap().remove(field);
            assert!(
                serde_json::from_value::<FileScanTaskDeleteFile>(incomplete).is_err(),
                "{field}"
            );
        }
        for field in ["content_offset", "content_size_in_bytes"] {
            let mut negative = valid.clone();
            negative[field] = json!(-1);
            assert!(
                serde_json::from_value::<FileScanTaskDeleteFile>(negative).is_err(),
                "{field}"
            );
        }
    }

    #[test]
    fn builder_rejects_offset_without_size() {
        let result = FileScanTaskDeleteFile::builder()
            .with_file_path("delete.parquet".into())
            .with_file_size_in_bytes(100)
            .with_file_type(DataContentType::PositionDeletes)
            .with_partition_spec_id(0)
            .with_content_offset(Some(4))
            .with_record_count(3)
            .build();
        assert!(result.is_err());
    }
}
