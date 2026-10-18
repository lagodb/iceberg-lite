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

use crate::Result;
use crate::expr::Predicate;
use crate::scan::FileScanTask;
use crate::spec::DataFile;
use crate::table::Table;

/// A data file selected for COW rewrite.
///
/// This is a read-only output type: the planning entry points that produce
/// candidates are crate-internal and reached through
/// [`crate::cow_rewrite::CowRewriteBuilder`], and the fields are exposed
/// through accessors only. Follow-up commit-adapter work (overwrite and
/// row-delta actions) consumes planned candidates through those accessors;
/// construction from outside the crate is intentionally not exposed yet and
/// can be added when an adapter actually needs it.
#[derive(Debug, Clone)]
pub struct CowRewriteFile {
    /// Original data file from the manifest entry.
    pub(crate) old_data_file: DataFile,
    /// Full-read task for visible rows in the original file.
    pub(crate) scan_task: FileScanTask,
}

impl CowRewriteFile {
    /// Original data file from the manifest entry.
    pub fn old_data_file(&self) -> &DataFile {
        &self.old_data_file
    }

    /// Full-read task for visible rows in the original file.
    pub fn scan_task(&self) -> &FileScanTask {
        &self.scan_task
    }
}

/// Plan data files that may need copy-on-write rewrite.
pub(crate) fn plan_cow_rewrite_files(
    table: &Table,
    predicate: Option<Predicate>,
    snapshot_id: Option<i64>,
    case_sensitive: bool,
) -> Result<Vec<CowRewriteFile>> {
    let mut scan_builder = table
        .scan()
        .select_all()
        .with_case_sensitive(case_sensitive);

    if let Some(predicate) = predicate {
        scan_builder = scan_builder.with_filter(predicate);
    }

    if let Some(snapshot_id) = snapshot_id {
        scan_builder = scan_builder.snapshot_id(snapshot_id);
    }

    scan_builder.build()?.plan_cow_rewrite_files()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::Result;
    use crate::expr::{Predicate, Reference};
    use crate::spec::{NestedField, PrimitiveType, Schema, SortOrder, Type};
    use crate::table::Table;
    use crate::test_utils::scan::TableTestFixture;

    #[test]
    fn cow_planner_uses_current_schema_unless_snapshot_is_explicit() -> Result<()> {
        for replacement in [
            NestedField::required(
                2,
                "renamed_y",
                Type::Primitive(PrimitiveType::Long),
            ),
            NestedField::optional(9, "y", Type::Primitive(PrimitiveType::Long)),
        ] {
            let mut fixture = TableTestFixture::new();
            fixture.setup_manifest_files();
            let snapshot =
                fixture.table.metadata().current_snapshot().unwrap().clone();
            let historical_schema = snapshot.schema(fixture.table.metadata())?;
            let field_id = replacement.id;
            let field_name = replacement.name.clone();
            let predicate = if field_id == 9 {
                Reference::new(&field_name).is_null()
            } else {
                Reference::new(&field_name).is_not_null()
            };
            let fields = historical_schema
                .as_struct()
                .fields()
                .iter()
                .filter(|field| field.id != 2)
                .cloned()
                .chain([Arc::new(replacement)]);
            let schema = Schema::builder()
                .with_schema_id(2)
                .with_fields(fields)
                .build()?;
            let metadata = fixture
                .table
                .metadata()
                .clone()
                .into_builder(None)
                .add_current_schema(schema)?
                .add_sort_order(SortOrder::unsorted_order())?
                .set_default_sort_order(0)?
                .build()?
                .metadata;
            fixture.table = Table::builder()
                .metadata(metadata)
                .identifier(fixture.table.identifier().clone())
                .file_io(fixture.table.file_io().clone())
                .build()
                .unwrap();

            let files = super::plan_cow_rewrite_files(
                &fixture.table,
                Some(predicate),
                None,
                true,
            )?;
            assert_eq!(files.len(), 2);
            for file in files {
                assert_eq!(
                    file.scan_task.schema(),
                    fixture.table.metadata().current_schema().as_ref()
                );
                assert_eq!(
                    file.scan_task
                        .schema()
                        .field_by_name(&field_name)
                        .unwrap()
                        .id,
                    field_id
                );
                assert!(file.scan_task.predicate().is_none());
            }

            let historical = super::plan_cow_rewrite_files(
                &fixture.table,
                Some(Reference::new("y").is_not_null()),
                Some(snapshot.snapshot_id()),
                true,
            )?;
            assert_eq!(historical.len(), 2);
            for file in historical {
                assert_eq!(file.scan_task.schema(), historical_schema.as_ref());
                assert_eq!(file.scan_task.schema().field_by_name("y").unwrap().id, 2);
            }
        }
        Ok(())
    }

    #[test]
    fn cow_planner_returns_old_file_and_full_read_task() -> Result<()> {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files();

        let mut files = super::plan_cow_rewrite_files(
            &fixture.table,
            Some(Predicate::AlwaysTrue),
            None,
            true,
        )?;

        assert_eq!(files.len(), 2);

        files.sort_by_key(|file| file.old_data_file.file_path().to_string());
        assert_eq!(
            files[0].old_data_file.file_path(),
            format!("{}/1.parquet", fixture.table_location)
        );
        assert_eq!(
            files[1].old_data_file.file_path(),
            format!("{}/3.parquet", fixture.table_location)
        );

        for file in files {
            assert_eq!(
                file.old_data_file.file_path(),
                file.scan_task.data_file_path()
            );
            assert!(file.scan_task.predicate().is_none());
            assert_eq!(
                Some(file.old_data_file.record_count()),
                file.scan_task.record_count
            );
            assert_eq!(0, file.scan_task.start);
            assert_eq!(
                file.old_data_file.file_size_in_bytes(),
                file.scan_task.length
            );
        }

        Ok(())
    }

    #[test]
    fn cow_planner_preserves_delete_files() -> Result<()> {
        let mut fixture = TableTestFixture::new();
        fixture.setup_deadlock_manifests();

        let scan = fixture
            .table
            .scan()
            .select_all()
            .with_concurrency_limit(1)
            .build()?;

        let files = scan.plan_cow_rewrite_files()?;

        assert_eq!(files.len(), 10);
        for file in files {
            assert_eq!(file.scan_task.deletes.len(), 1);
            assert_eq!(
                file.scan_task.deletes[0].file_path(),
                format!("{}/del.parquet", fixture.table_location)
            );
        }

        Ok(())
    }
}
