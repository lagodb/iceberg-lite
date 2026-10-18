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

use std::str::FromStr;

use uuid::Uuid;

#[cfg(test)]
use crate::spec::DataFile;
use crate::spec::{DataFileFormat, PartitionKey, SchemaRef};
use crate::table::Table;
use crate::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use crate::writer::file_writer::ParquetWriterBuilder;
use crate::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator, LocationGenerator,
    ObjectStorageLocationGenerator,
};
use crate::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use crate::writer::{IcebergWriter, IcebergWriterBuilder};
use crate::{Error, ErrorKind, Result};

/// Builds a boxed replacement-data-file writer.
///
/// `write_schema` is the schema the input batches are encoded in. It must match
/// scan schema: the current schema by default, or the snapshot's schema when
/// a snapshot ID is explicitly selected.
///
/// Building the writer is cheap: no physical file is opened until the first
/// batch is written, so it is safe to construct one optimistically and only
/// write to it once a source file is known to have changed.
///
/// The location layout follows `write.object-storage.enabled`: tables that opt
/// into the object-storage layout get hash-entropy paths from
/// [`ObjectStorageLocationGenerator`], everything else gets the default layout.
pub(crate) fn build_replacement_writer(
    table: &Table,
    write_schema: SchemaRef,
    partition_key: Option<PartitionKey>,
) -> Result<Box<dyn IcebergWriter>> {
    let table_props = table.metadata().table_properties();
    // The replacement writer only produces parquet today; refuse to run on a
    // table configured for another format instead of silently writing parquet
    // against `write.format.default`.
    let write_format_default = table_props.write_format_default()?;
    if DataFileFormat::from_str(&write_format_default)? != DataFileFormat::Parquet {
        return Err(Error::new(
            ErrorKind::FeatureUnsupported,
            format!(
                "File format {write_format_default} is not supported for COW rewrite yet!"
            ),
        ));
    }
    let mut parquet_builder =
        ParquetWriterBuilder::from_table_properties(&table_props, write_schema)?;
    // Replacement files must not silently downgrade an encrypted table to
    // plaintext: source rows were read through decryption, so re-encrypt the
    // rewritten rows when the table carries an encryption manager.
    if let Some(encryption_manager) = table.encryption_manager_ref() {
        parquet_builder =
            parquet_builder.with_encryption_manager(encryption_manager.clone());
    }

    if table_props.write_object_storage_enabled()? {
        let location_generator =
            ObjectStorageLocationGenerator::new(table.metadata())?;
        build_data_file_writer(
            table,
            parquet_builder,
            location_generator,
            partition_key,
        )
    } else {
        let location_generator = DefaultLocationGenerator::new(table.metadata())?;
        build_data_file_writer(
            table,
            parquet_builder,
            location_generator,
            partition_key,
        )
    }
}

fn build_data_file_writer<L: LocationGenerator>(
    table: &Table,
    parquet_builder: ParquetWriterBuilder,
    location_generator: L,
    partition_key: Option<PartitionKey>,
) -> Result<Box<dyn IcebergWriter>> {
    let file_name_generator = DefaultFileNameGenerator::new(
        format!("cow-rewrite-{}", Uuid::now_v7()),
        None,
        DataFileFormat::Parquet,
    );
    let table_props = table.metadata().table_properties();
    let rolling_builder = RollingFileWriterBuilder::new(
        parquet_builder,
        table_props.write_target_file_size_bytes()?,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );
    let data_writer_builder = DataFileWriterBuilder::new(rolling_builder);
    Ok(Box::new(data_writer_builder.build(partition_key)?))
}

/// Writes replacement record batches as Iceberg data files.
///
/// This is a convenience wrapper around [`build_replacement_writer`] that
/// consumes an iterator end-to-end. Empty input or all-zero-row batches produce no
/// data files.
#[cfg(test)]
pub(crate) fn write_replacement_batches<S>(
    table: &Table,
    write_schema: SchemaRef,
    partition_key: Option<PartitionKey>,
    batches: S,
) -> Result<Vec<DataFile>>
where
    S: IntoIterator<Item = Result<arrow_array::RecordBatch>>,
{
    let mut writer = build_replacement_writer(table, write_schema, partition_key)?;

    let mut wrote_rows = false;
    for batch in batches {
        let batch = batch?;
        if batch.num_rows() == 0 {
            continue;
        }
        wrote_rows = true;
        writer.write(batch)?;
    }

    if !wrote_rows {
        return Ok(vec![]);
    }

    writer.close()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use tempfile::TempDir;

    use super::write_replacement_batches;
    use crate::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use crate::spec::{
        DataContentType, NestedField, PartitionKey, PrimitiveType, Schema, Struct,
        Transform, Type,
    };
    use crate::{Catalog, CatalogBuilder, NamespaceIdent, Result, TableCreation};

    #[test]
    fn cow_replacement_writer_outputs_data_files() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let warehouse =
            format!("file://{}", temp_dir.path().join("warehouse").display());
        let catalog = MemoryCatalogBuilder::default().load(
            "memory",
            HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse)]),
        )?;
        let namespace = NamespaceIdent::new("ns".to_string());
        catalog.create_namespace(&namespace, HashMap::new())?;

        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int))
                    .into(),
            ])
            .build()?;
        let table = catalog.create_table(
            &namespace,
            TableCreation::builder()
                .name("cow_writer".to_string())
                .schema(schema)
                .build(),
        )?;

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([
                (PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string()),
            ])),
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef],
        )?;

        let data_files = write_replacement_batches(
            &table,
            table.metadata().current_schema().clone(),
            None,
            vec![Ok(batch)],
        )?;

        assert_eq!(data_files.len(), 1);
        assert_eq!(data_files[0].record_count(), 3);
        assert_eq!(data_files[0].content_type(), DataContentType::Data);

        Ok(())
    }

    #[test]
    fn cow_replacement_writer_skips_empty_input() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let warehouse =
            format!("file://{}", temp_dir.path().join("warehouse").display());
        let catalog = MemoryCatalogBuilder::default().load(
            "memory",
            HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse)]),
        )?;
        let namespace = NamespaceIdent::new("ns".to_string());
        catalog.create_namespace(&namespace, HashMap::new())?;

        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int))
                    .into(),
            ])
            .build()?;
        let table = catalog.create_table(
            &namespace,
            TableCreation::builder()
                .name("cow_empty_writer".to_string())
                .schema(schema)
                .build(),
        )?;

        let data_files = write_replacement_batches(
            &table,
            table.metadata().current_schema().clone(),
            None,
            vec![],
        )?;

        assert!(data_files.is_empty());

        Ok(())
    }

    #[test]
    fn cow_replacement_writer_skips_zero_row_batches() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let warehouse =
            format!("file://{}", temp_dir.path().join("warehouse").display());
        let catalog = MemoryCatalogBuilder::default().load(
            "memory",
            HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse)]),
        )?;
        let namespace = NamespaceIdent::new("ns".to_string());
        catalog.create_namespace(&namespace, HashMap::new())?;

        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int))
                    .into(),
            ])
            .build()?;
        let table = catalog.create_table(
            &namespace,
            TableCreation::builder()
                .name("cow_zero_row_writer".to_string())
                .schema(schema)
                .build(),
        )?;

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([
                (PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string()),
            ])),
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![Arc::new(Int32Array::from(Vec::<i32>::new())) as ArrayRef],
        )?;

        let data_files = write_replacement_batches(
            &table,
            table.metadata().current_schema().clone(),
            None,
            vec![Ok(batch)],
        )?;

        assert!(data_files.is_empty());

        Ok(())
    }

    /// With `write.object-storage.enabled` the replacement file must land in
    /// the hash-entropy object storage layout instead of the flat default one.
    #[test]
    fn cow_replacement_writer_honors_object_storage_layout() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let warehouse =
            format!("file://{}", temp_dir.path().join("warehouse").display());
        let catalog = MemoryCatalogBuilder::default().load(
            "memory",
            HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse)]),
        )?;
        let namespace = NamespaceIdent::new("ns".to_string());
        catalog.create_namespace(&namespace, HashMap::new())?;

        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int))
                    .into(),
            ])
            .build()?;
        let table = catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("cow_object_storage_writer".to_string())
                    .schema(schema)
                    .properties(HashMap::from([(
                        crate::spec::TableProperties::PROPERTY_WRITE_OBJECT_STORAGE_ENABLED
                            .to_string(),
                        "true".to_string(),
                    )]))
                    .build(),
            )?;

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([
                (PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string()),
            ])),
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef],
        )?;

        let data_files = write_replacement_batches(
            &table,
            table.metadata().current_schema().clone(),
            None,
            vec![Ok(batch)],
        )?;

        assert_eq!(data_files.len(), 1);
        // Object storage layout: `{data_location}/{entropy dirs}/{file name}`,
        // where the entropy dirs are binary strings of 4, 4, 4, and 8 chars.
        let path = data_files[0].file_path();
        let after_data = path
            .split("/data/")
            .nth(1)
            .unwrap_or_else(|| panic!("expected /data/ in {path}"));
        let segments = after_data.split('/').collect::<Vec<_>>();
        assert_eq!(segments.len(), 5, "{path}");
        for (segment, len) in segments[..4].iter().zip([4, 4, 4, 8]) {
            assert_eq!(segment.len(), len, "{path}");
            assert!(segment.chars().all(|c| c == '0' || c == '1'), "{path}");
        }
        assert!(segments[4].starts_with("cow-rewrite-"), "{path}");

        Ok(())
    }

    #[test]
    fn cow_replacement_writer_preserves_partition_key() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let warehouse =
            format!("file://{}", temp_dir.path().join("warehouse").display());
        let catalog = MemoryCatalogBuilder::default().load(
            "memory",
            HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse)]),
        )?;
        let namespace = NamespaceIdent::new("ns".to_string());
        catalog.create_namespace(&namespace, HashMap::new())?;

        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int))
                    .into(),
            ])
            .build()?;
        let partition_spec =
            crate::spec::PartitionSpec::builder(Arc::new(schema.clone()))
                .add_partition_field("id", "id", Transform::Identity)?
                .build()?;
        let table = catalog.create_table(
            &namespace,
            TableCreation::builder()
                .name("cow_partitioned_writer".to_string())
                .schema(schema)
                .partition_spec(partition_spec.clone().into_unbound())
                .build(),
        )?;

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([
                (PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string()),
            ])),
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![Arc::new(Int32Array::from(vec![1, 1, 1])) as ArrayRef],
        )?;
        let partition_key = PartitionKey::new(
            table.metadata().default_partition_spec().as_ref().clone(),
            table.metadata().current_schema().clone(),
            Struct::from_iter([Some(crate::spec::Literal::int(1))]),
        );

        let data_files = write_replacement_batches(
            &table,
            table.metadata().current_schema().clone(),
            Some(partition_key),
            vec![Ok(batch)],
        )?;

        assert_eq!(data_files.len(), 1);
        assert_eq!(data_files[0].partition_spec_id, 0);
        assert_eq!(
            data_files[0].partition(),
            &Struct::from_iter([Some(crate::spec::Literal::int(1))])
        );

        Ok(())
    }
}
