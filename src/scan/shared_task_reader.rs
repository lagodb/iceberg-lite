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

//! Construction of stateful readers for shared planned task inventories.

use std::sync::Arc;

use crate::Result;
use crate::arrow::ArrowReaderBuilder;
pub use crate::arrow::SharedTaskArrowReader;
use crate::expr::Predicate;

use super::{FileScanTask, TableScan};

impl TableScan {
    /// Creates a per-scan reader for a shared immutable task inventory whose
    /// supplemental predicate may change between unopened tasks.
    pub fn shared_task_arrow_reader(
        &self,
        tasks: Arc<[FileScanTask]>,
        batch_size: usize,
        base_filter: Option<Predicate>,
    ) -> Result<SharedTaskArrowReader> {
        let (snapshot_schema, case_sensitive) = self
            .plan_context
            .as_ref()
            .map(|context| {
                (
                    Some(Arc::clone(&context.snapshot_schema)),
                    context.case_sensitive,
                )
            })
            .unwrap_or((None, true));
        let reader = ArrowReaderBuilder::new(self.file_io.clone())
            .with_data_file_concurrency_limit(self.concurrency_limit_data_files)
            .with_row_group_filtering_enabled(self.row_group_filtering_enabled)
            .with_row_selection_enabled(self.row_selection_enabled)
            .with_bloom_filter_enabled(self.bloom_filter_enabled)
            .with_batch_size(batch_size)
            .build();
        SharedTaskArrowReader::new(
            reader,
            tasks,
            snapshot_schema,
            case_sensitive,
            base_filter,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::Reference;
    use crate::spec::Datum;
    use crate::test_utils::scan::TableTestFixture;

    #[test]
    fn shared_task_paths_honor_case_sensitivity_and_bloom_filter_setting() {
        let fixture = TableTestFixture::new();
        let scan = fixture
            .table
            .scan()
            .with_case_sensitive(false)
            .with_bloom_filter_enabled(true)
            .build()
            .unwrap();

        let filter = || Reference::new("X").equal_to(Datum::long(1));
        let _task_reader = scan
            .to_arrow_with_tasks_and_filter(Vec::new(), Some(filter()))
            .unwrap();
        let _shared_task_reader = scan
            .to_arrow_with_shared_tasks_and_filter(
                Arc::<[FileScanTask]>::from([]),
                Some(filter()),
            )
            .unwrap();

        let mut reader = scan
            .shared_task_arrow_reader(
                Arc::<[FileScanTask]>::from([]),
                1024,
                Some(filter()),
            )
            .unwrap();
        assert!(reader.bloom_filter_enabled());
        reader.replace_supplemental_filter(Some(filter())).unwrap();
    }
}
