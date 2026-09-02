// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use arrow_array::RecordBatch;

use crate::Result;
use crate::arrow::SelectedRowsReadRequest;
use crate::expr::Reference;
use crate::scan::tests::TableTestFixture;
use crate::spec::Datum;

#[test]
fn empty_projection_preserves_rows_selected_by_filter() {
    let mut fixture = TableTestFixture::new_unpartitioned();
    fixture.setup_unpartitioned_manifest_files();

    let scan = fixture
        .table
        .scan()
        .select_empty()
        .with_filter(Reference::new("y").less_than(Datum::long(3)))
        .with_row_selection_enabled(true)
        .build()
        .unwrap();
    let batches = scan
        .to_arrow()
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert!(batches.iter().all(|batch| batch.num_columns() == 0));
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        1_024
    );
}

#[test]
fn empty_projection_preserves_explicit_row_selection() {
    let mut fixture = TableTestFixture::new();
    fixture.setup_manifest_files();
    let data_file_path = format!("{}/1.parquet", fixture.table_location);
    let scan = fixture.table.scan().select_empty().build().unwrap();
    let mut task = scan
        .plan_files()
        .unwrap()
        .into_iter()
        .find(|task| task.data_file_path == data_file_path)
        .unwrap();

    task.record_count = Some(1_024);
    let request =
        SelectedRowsReadRequest::try_new(task, vec![0, 511, 1_023]).unwrap();
    let batches = fixture
        .table
        .reader_builder()
        .with_row_selection_enabled(true)
        .build()
        .read_selected_rows(vec![request])
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert!(batches.iter().all(|batch| batch.num_columns() == 0));
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
}
