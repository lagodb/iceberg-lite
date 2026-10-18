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

use super::*;
use crate::transaction::manifest_rewrite::ManifestRewritePlan;

struct ActiveManifestWriter {
    schema: SchemaRef,
    writer: ManifestWriter,
}

impl<'a> DeltaSnapshotProducer<'a> {
    pub(in crate::transaction) fn commit_manifest_rewrite(
        &mut self,
        min_count_to_merge: usize,
        target_size_bytes: u64,
    ) -> Result<ActionCommit> {
        let Some(current_snapshot) = self.table.metadata().current_snapshot() else {
            return Ok(ActionCommit::new(Vec::new(), Vec::new()));
        };
        let manifest_list = current_snapshot
            .load_manifest_list(self.table.file_io(), &self.table.metadata_ref())?;
        let rewrite_plan = ManifestRewritePlan::build(
            manifest_list.entries(),
            min_count_to_merge,
            target_size_bytes,
        )?;
        if rewrite_plan.is_empty() {
            return Ok(ActionCommit::new(Vec::new(), Vec::new()));
        }
        let (by_group, selected) = rewrite_plan.into_parts();
        let mut by_group: Vec<_> = by_group.into_iter().collect();
        by_group.sort_unstable_by(
            |((left_spec, left_content), _), ((right_spec, right_content), _)| {
                left_spec.cmp(right_spec).then_with(|| {
                    (*left_content as i32).cmp(&(*right_content as i32))
                })
            },
        );

        let mut output = Vec::new();
        for ((spec_id, content), manifests) in by_group {
            let selected_group: Vec<&ManifestFile> = manifests
                .iter()
                .filter(|manifest| selected.contains(manifest.manifest_path.as_str()))
                .collect();
            if selected_group.is_empty() {
                output.extend(manifests);
                continue;
            }

            let partition_spec = self.partition_spec(spec_id)?;
            let total_bytes =
                selected_group.iter().try_fold(0_u64, |total, manifest| {
                    total
                        .checked_add(
                            u64::try_from(manifest.manifest_length).map_err(
                                |_| {
                                    Error::new(
                                        ErrorKind::DataInvalid,
                                        "negative manifest length",
                                    )
                                },
                            )?,
                        )
                        .ok_or_else(|| {
                            Error::new(
                                ErrorKind::DataInvalid,
                                "manifest byte count overflow",
                            )
                        })
                })?;
            let output_count =
                usize::try_from(total_bytes.div_ceil(target_size_bytes).max(1))
                    .map_err(|_| {
                        Error::new(ErrorKind::DataInvalid, "manifest count overflow")
                    })?;
            let total_live_entries = selected_group.iter().try_fold(
                Some(0_usize),
                |total, manifest| {
                    let Some(total) = total else {
                        return Ok::<Option<usize>, Error>(None);
                    };
                    let (Some(added), Some(existing)) =
                        (manifest.added_files_count, manifest.existing_files_count)
                    else {
                        return Ok(None);
                    };
                    let live = usize::try_from(added)
                        .ok()
                        .and_then(|added| {
                            usize::try_from(existing)
                                .ok()
                                .and_then(|existing| added.checked_add(existing))
                        })
                        .ok_or_else(|| {
                            Error::new(
                                ErrorKind::DataInvalid,
                                "manifest live-entry count overflow",
                            )
                        })?;
                    total.checked_add(live).map(Some).ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            "manifest group live-entry count overflow",
                        )
                    })
                },
            )?;
            let entries_per_output =
                total_live_entries.map(|count| count.div_ceil(output_count).max(1));
            let source_manifests_per_output =
                selected_group.len().div_ceil(output_count).max(1);
            let mut writer: Option<ActiveManifestWriter> = None;
            let mut writer_entries = 0_usize;
            let mut writer_source_manifests = 0_usize;
            let mut written_outputs = 0_usize;
            for manifest_file in selected_group {
                let manifest = manifest_file.load_manifest(self.table.file_io())?;
                let schema = manifest.metadata().schema();
                // V1 can omit schema-id; compare the schemas that encode the entries.
                if writer
                    .as_ref()
                    .is_some_and(|active| active.schema != *schema)
                {
                    let previous =
                        writer.take().expect("writer schema was inspected");
                    output.push(previous.writer.write_manifest_file()?);
                    written_outputs += 1;
                    writer_entries = 0;
                    writer_source_manifests = 0;
                }
                let mut row_ids =
                    FirstRowIdInheritance::new(manifest_file.first_row_id);
                for entry in manifest.entries() {
                    let effective_first_row_id = row_ids.resolve(entry)?;
                    if !entry.is_alive() {
                        continue;
                    }
                    let mut file = entry.data_file().clone();
                    if self.table.metadata().format_version() == FormatVersion::V3
                        && file.content_type() == DataContentType::Data
                    {
                        file.first_row_id = effective_first_row_id
                            .map(|value| {
                                i64::try_from(value).map_err(|_| {
                                    Error::new(
                                        ErrorKind::DataInvalid,
                                        "first row id does not fit Iceberg long",
                                    )
                                })
                            })
                            .transpose()?;
                    }
                    let snapshot_id = entry.snapshot_id().ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            "live manifest entry has no snapshot id",
                        )
                    })?;
                    let sequence_number =
                        entry.sequence_number().ok_or_else(|| {
                            Error::new(
                                ErrorKind::DataInvalid,
                                "live manifest entry has no sequence number",
                            )
                        })?;
                    let file_sequence_number =
                        entry.file_sequence_number.ok_or_else(|| {
                            Error::new(
                                ErrorKind::DataInvalid,
                                "live manifest entry has no file sequence number",
                            )
                        })?;
                    if writer.is_none() {
                        writer = Some(ActiveManifestWriter {
                            schema: Arc::clone(schema),
                            writer: self.new_manifest_writer(
                                content,
                                Arc::clone(schema),
                                Arc::clone(&partition_spec),
                            )?,
                        });
                    }
                    writer
                        .as_mut()
                        .expect("manifest writer was initialized")
                        .writer
                        .add_existing_file(
                            file,
                            snapshot_id,
                            sequence_number,
                            Some(file_sequence_number),
                        )?;
                    writer_entries =
                        writer_entries.checked_add(1).ok_or_else(|| {
                            Error::new(
                                ErrorKind::DataInvalid,
                                "manifest output entry count overflow",
                            )
                        })?;
                    if entries_per_output.is_some_and(|target| {
                        writer_entries >= target
                            && written_outputs
                                .checked_add(1)
                                .is_some_and(|written| written < output_count)
                    }) {
                        output.push(
                            writer
                                .take()
                                .expect("manifest writer was initialized")
                                .writer
                                .write_manifest_file()?,
                        );
                        written_outputs = written_outputs.checked_add(1).expect(
                            "written output count is bounded by output_count",
                        );
                        writer_entries = 0;
                        writer_source_manifests = 0;
                    }
                }
                writer_source_manifests =
                    writer_source_manifests.checked_add(1).ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            "source manifest count overflow",
                        )
                    })?;
                if entries_per_output.is_none()
                    && writer.is_some()
                    && writer_source_manifests >= source_manifests_per_output
                    && written_outputs
                        .checked_add(1)
                        .is_some_and(|written| written < output_count)
                {
                    output.push(
                        writer
                            .take()
                            .expect("manifest writer was initialized")
                            .writer
                            .write_manifest_file()?,
                    );
                    written_outputs = written_outputs
                        .checked_add(1)
                        .expect("written output count is bounded by output_count");
                    writer_entries = 0;
                    writer_source_manifests = 0;
                }
            }
            if let Some(active) = writer {
                output.push(active.writer.write_manifest_file()?);
            }
        }

        let summary = update_snapshot_summaries(
            Summary {
                operation: Operation::Replace,
                additional_properties: HashMap::new(),
            },
            Some(current_snapshot.summary()),
            false,
        )?;
        let manifest_list = self.write_manifest_list(output)?;
        let snapshot = self.new_snapshot(&manifest_list, summary)?;
        Ok(ActionCommit::new(
            vec![
                TableUpdate::AddSnapshot { snapshot },
                TableUpdate::SetSnapshotRef {
                    ref_name: MAIN_BRANCH.to_owned(),
                    reference: SnapshotReference::new(
                        self.snapshot_id,
                        SnapshotRetention::branch(None, None, None),
                    ),
                },
            ],
            vec![
                TableRequirement::UuidMatch {
                    uuid: self.table.metadata().uuid(),
                },
                TableRequirement::RefSnapshotIdMatch {
                    r#ref: MAIN_BRANCH.to_owned(),
                    snapshot_id: self.table.metadata().current_snapshot_id(),
                },
            ],
        ))
    }
}

#[cfg(test)]
mod tests {
    use apache_avro::{Reader as AvroReader, Writer as AvroWriter};

    use super::*;
    use crate::TableIdent;
    use crate::io::FileIO;
    use crate::overlay::SnapshotDelta;
    use crate::spec::{
        DataFileBuilder, Datum, ManifestStatus, NestedField, PartitionSpec,
        PrimitiveType, Schema, SortOrder, TableMetadataBuilder, Transform, Type,
    };

    struct HistoricalManifestFixture {
        table: Table,
        int_schema: SchemaRef,
        long_schema: SchemaRef,
    }

    impl HistoricalManifestFixture {
        fn new() -> Result<Self> {
            let int_schema = Arc::new(
                Schema::builder()
                    .with_fields([NestedField::required(
                        1,
                        "value",
                        Type::Primitive(PrimitiveType::Int),
                    )
                    .into()])
                    .build()?,
            );
            let long_schema = Arc::new(
                Schema::builder()
                    .with_schema_id(1)
                    .with_fields([NestedField::required(
                        1,
                        "value",
                        Type::Primitive(PrimitiveType::Long),
                    )
                    .into()])
                    .build()?,
            );
            let initial_spec = PartitionSpec::builder(Arc::clone(&int_schema))
                .add_partition_field("value", "value", Transform::Identity)?
                .build()?;
            let unpartitioned =
                PartitionSpec::builder(Arc::clone(&int_schema)).build()?;
            let metadata = TableMetadataBuilder::new(
                int_schema.as_ref().clone(),
                initial_spec,
                SortOrder::unsorted_order(),
                "memory://historical-manifests".to_owned(),
                FormatVersion::V2,
                HashMap::new(),
            )?
            .add_default_partition_spec(unpartitioned.into_unbound())?
            .add_current_schema(long_schema.as_ref().clone())?
            .set_default_partition_spec(0)?
            .build()?
            .metadata;
            assert_eq!(metadata.default_partition_spec_id(), 0);
            assert!(metadata.partition_spec_by_id(1).is_some());
            let table = Table::builder()
                .metadata(metadata)
                .identifier(TableIdent::from_strs(["ns", "historical"])?)
                .file_io(FileIO::memory())
                .build()
                .unwrap();
            Ok(Self {
                table,
                int_schema,
                long_schema,
            })
        }

        fn write_v1_manifest(
            &self,
            name: &str,
            schema: SchemaRef,
            bound: Datum,
        ) -> Result<ManifestFile> {
            let output = self
                .table
                .file_io()
                .new_output(format!("memory://historical-manifests/{name}.avro"))?;
            let mut writer = ManifestWriterBuilder::new(
                output,
                Some(7),
                schema,
                self.table
                    .metadata()
                    .partition_spec_by_id(1)
                    .unwrap()
                    .as_ref()
                    .clone(),
            )
            .build_v1();
            let file = DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path(format!("memory://historical-manifests/{name}.parquet"))
                .file_format(DataFileFormat::Parquet)
                .file_size_in_bytes(100)
                .record_count(1)
                .partition_spec_id(1)
                .partition(Struct::empty())
                .lower_bounds(HashMap::from([(1, bound.clone())]))
                .upper_bounds(HashMap::from([(1, bound)]))
                .build()
                .unwrap();
            writer.add_existing_file(file, 7, 0, Some(0))?;
            let mut manifest = writer.write_manifest_file()?;

            // V1 permits both IDs to be absent from the Avro header.
            let bytes = self
                .table
                .file_io()
                .new_input(&manifest.manifest_path)?
                .read()?;
            let reader = AvroReader::new(bytes.as_ref())?;
            let avro_schema = reader.writer_schema().clone();
            let mut writer = AvroWriter::new(&avro_schema, Vec::new())?;
            for (key, value) in reader.user_metadata() {
                if key != "schema-id" && key != "partition-spec-id" {
                    writer.add_user_metadata(key.clone(), value.clone())?;
                }
            }
            for entry in reader {
                writer.append_value(entry?)?;
            }
            let bytes = writer.into_inner()?;
            self.table
                .file_io()
                .new_output(&manifest.manifest_path)?
                .write(&bytes)?;
            manifest.manifest_length = bytes.len().try_into().unwrap();
            // Published v1 manifests have sequence number zero in v2 manifest lists.
            manifest.sequence_number = 0;
            Ok(manifest)
        }

        fn install(&mut self, manifests: Vec<ManifestFile>) -> Result<()> {
            let producer = DeltaSnapshotProducer::new(
                &self.table,
                Uuid::now_v7(),
                HashMap::new(),
            );
            let list = producer.write_manifest_list(manifests)?;
            let snapshot = producer.new_snapshot(
                &list,
                Summary {
                    operation: Operation::Append,
                    additional_properties: HashMap::new(),
                },
            )?;
            let metadata = self
                .table
                .metadata()
                .clone()
                .into_builder(None)
                .set_branch_snapshot(snapshot, MAIN_BRANCH)?
                .build()?
                .metadata;
            self.table = Table::builder()
                .metadata(metadata)
                .identifier(self.table.identifier().clone())
                .file_io(self.table.file_io().clone())
                .build()
                .unwrap();
            Ok(())
        }
    }

    #[test]
    fn rewrite_v1_manifests_groups_actual_schemas_and_preserves_spec() -> Result<()> {
        let mut fixture = HistoricalManifestFixture::new()?;
        let inputs = vec![
            fixture.write_v1_manifest(
                "int-a",
                Arc::clone(&fixture.int_schema),
                Datum::int(10),
            )?,
            fixture.write_v1_manifest(
                "int-b",
                Arc::clone(&fixture.int_schema),
                Datum::int(20),
            )?,
            fixture.write_v1_manifest(
                "long",
                Arc::clone(&fixture.long_schema),
                Datum::long(3_000_000_000_i64),
            )?,
        ];
        for input in &inputs {
            let manifest = input.load_manifest(fixture.table.file_io())?;
            assert_eq!(manifest.metadata().schema_id(), 0);
            assert_eq!(manifest.metadata().partition_spec().spec_id(), 0);
        }
        fixture.install(inputs)?;
        let mut producer = DeltaSnapshotProducer::new(
            &fixture.table,
            Uuid::now_v7(),
            HashMap::new(),
        );
        let mut commit = producer.commit_manifest_rewrite(2, u64::MAX)?;
        let snapshot = commit
            .take_updates()
            .into_iter()
            .find_map(|update| match update {
                TableUpdate::AddSnapshot { snapshot } => Some(snapshot),
                _ => None,
            })
            .unwrap();
        let list = snapshot
            .load_manifest_list(fixture.table.file_io(), fixture.table.metadata())?;
        // Equal schemas still consolidate, even though the third manifest needs its own writer.
        assert_eq!(list.entries().len(), 2);
        for (output, (schema, bounds)) in list.entries().iter().zip([
            (&fixture.int_schema, vec![Datum::int(10), Datum::int(20)]),
            (&fixture.long_schema, vec![Datum::long(3_000_000_000_i64)]),
        ]) {
            assert_eq!(output.partition_spec_id, 1);
            let manifest = output.load_manifest(fixture.table.file_io())?;
            assert_eq!(manifest.metadata().schema(), schema);
            assert_eq!(
                manifest.metadata().partition_spec(),
                fixture
                    .table
                    .metadata()
                    .partition_spec_by_id(1)
                    .unwrap()
                    .as_ref()
            );
            assert_eq!(manifest.entries().len(), bounds.len());
            for (entry, bound) in manifest.entries().iter().zip(bounds) {
                assert_eq!(entry.data_file().partition_spec_id, 1);
                assert_eq!(
                    entry.data_file().lower_bounds(),
                    &HashMap::from([(1, bound.clone())])
                );
                assert_eq!(
                    entry.data_file().upper_bounds(),
                    &HashMap::from([(1, bound)])
                );
                assert_eq!(entry.status(), ManifestStatus::Existing);
                assert_eq!(entry.snapshot_id(), Some(7));
                assert_eq!(entry.sequence_number(), Some(0));
                assert_eq!(entry.file_sequence_number, Some(0));
            }
        }
        Ok(())
    }

    #[test]
    fn rewrite_single_v1_manifest_preserves_historical_schema_and_spec() -> Result<()>
    {
        let fixture = HistoricalManifestFixture::new()?;
        let input = fixture.write_v1_manifest(
            "removed",
            Arc::clone(&fixture.int_schema),
            Datum::int(10),
        )?;
        let manifest = input.load_manifest(fixture.table.file_io())?;
        let path = manifest.entries()[0].file_path();
        let mut delta = SnapshotDelta::new();
        delta.remove_data_file(path)?;
        let plan = DeltaPlan::from_delta_with_truncate(&delta, false);
        let mut removed = HashSet::new();
        let mut producer = DeltaSnapshotProducer::new(
            &fixture.table,
            Uuid::now_v7(),
            HashMap::new(),
        );
        let output = producer.rewrite_manifest(
            &input,
            &manifest,
            &plan.removals,
            &mut removed,
            &mut HashSet::new(),
            &mut SnapshotSummaryCollector::default(),
        )?;
        assert_eq!(removed, HashSet::from([path.to_owned()]));
        assert_eq!(output.partition_spec_id, 1);
        let rewritten = output.load_manifest(fixture.table.file_io())?;
        assert_eq!(rewritten.metadata().schema(), &fixture.int_schema);
        assert_eq!(
            rewritten.metadata().partition_spec(),
            fixture
                .table
                .metadata()
                .partition_spec_by_id(1)
                .unwrap()
                .as_ref()
        );
        assert_eq!(rewritten.entries().len(), 1);
        let entry = &rewritten.entries()[0];
        assert_eq!(entry.status(), ManifestStatus::Deleted);
        assert_eq!(entry.file_path(), path);
        assert_eq!(entry.data_file().partition_spec_id, 1);
        assert_eq!(
            entry.data_file().lower_bounds(),
            &HashMap::from([(1, Datum::int(10))])
        );
        assert_eq!(
            entry.data_file().upper_bounds(),
            &HashMap::from([(1, Datum::int(10))])
        );
        Ok(())
    }
}
