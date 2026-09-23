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

//! Stateful reader for a shared inventory of planned file tasks.

use std::sync::Arc;

use super::pipeline::FileReadRequest;
use super::{ArrowReader, ParquetReadOptions};
use crate::arrow::caching_delete_file_loader::CachingDeleteFileLoader;
use crate::arrow::scan_metrics::ScanMetrics;
use crate::expr::{Bind, BoundPredicate, Predicate};
use crate::io::FileIO;
use crate::scan::{ArrowRecordBatchIterator, FileScanTask};
use crate::spec::SchemaRef;
use crate::{Error, ErrorKind, Result};

/// Per-scan execution state created once for a synchronous scan and reused for
/// every file task opened by that scan.
pub(super) struct FileScanTaskReader {
    batch_size: Option<usize>,
    file_io: FileIO,
    delete_file_loader: CachingDeleteFileLoader,
    row_group_filtering_enabled: bool,
    row_selection_enabled: bool,
    bloom_filter_enabled: bool,
    parquet_read_options: ParquetReadOptions,
    scan_metrics: ScanMetrics,
}

impl FileScanTaskReader {
    pub(super) fn new(reader: ArrowReader) -> Self {
        let scan_metrics = ScanMetrics::new();
        let delete_file_loader = reader
            .delete_file_loader
            .with_scan_metrics(scan_metrics.clone());
        Self {
            batch_size: reader.batch_size,
            file_io: reader.file_io,
            delete_file_loader,
            row_group_filtering_enabled: reader.row_group_filtering_enabled,
            row_selection_enabled: reader.row_selection_enabled,
            bloom_filter_enabled: reader.bloom_filter_enabled,
            parquet_read_options: reader.parquet_read_options,
            scan_metrics,
        }
    }

    pub(super) fn scan_metrics(&self) -> &ScanMetrics {
        &self.scan_metrics
    }

    pub(super) fn read(
        &self,
        request: FileReadRequest,
    ) -> Result<ArrowRecordBatchIterator> {
        ArrowReader::process_file_read_request(
            request,
            self.batch_size,
            self.file_io.clone(),
            self.delete_file_loader.clone(),
            self.row_group_filtering_enabled,
            self.row_selection_enabled,
            self.bloom_filter_enabled,
            self.parquet_read_options,
            self.scan_metrics.clone(),
        )
    }

    fn read_shared_task(
        &self,
        tasks: Arc<[FileScanTask]>,
        task_index: usize,
        base_predicate: Option<Arc<BoundPredicate>>,
        supplemental_predicate: Option<Arc<BoundPredicate>>,
    ) -> Result<ArrowRecordBatchIterator> {
        self.read(FileReadRequest::SharedScan {
            tasks,
            task_index,
            base_predicate,
            supplemental_predicate,
        })
        .map_err(|error| {
            Error::new(ErrorKind::Unexpected, "file read request failed")
                .with_source(error)
        })
    }
}

/// Per-scan reader that preserves immutable task planning while allowing a
/// supplemental predicate to be replaced between unopened file tasks.
pub struct SharedTaskArrowReader {
    task_reader: FileScanTaskReader,
    tasks: Arc<[FileScanTask]>,
    next_task: usize,
    snapshot_schema: Option<SchemaRef>,
    case_sensitive: bool,
    base_predicate: Option<Arc<BoundPredicate>>,
    supplemental_predicate: Option<Arc<BoundPredicate>>,
}

impl SharedTaskArrowReader {
    pub(crate) fn new(
        reader: ArrowReader,
        tasks: Arc<[FileScanTask]>,
        snapshot_schema: Option<SchemaRef>,
        case_sensitive: bool,
        base_filter: Option<Predicate>,
    ) -> Result<Self> {
        let base_filter = base_filter.map(Predicate::rewrite_not);
        let base_predicate = Self::bind_filter(
            snapshot_schema.as_ref(),
            base_filter.as_ref(),
            case_sensitive,
        )?;
        Ok(Self {
            task_reader: FileScanTaskReader::new(reader),
            tasks,
            next_task: 0,
            snapshot_schema,
            case_sensitive,
            base_predicate,
            supplemental_predicate: None,
        })
    }

    /// Replaces the supplemental predicate applied to subsequently opened
    /// tasks. Passing `None` restores the immutable base predicate.
    pub fn replace_supplemental_filter(
        &mut self,
        filter: Option<Predicate>,
    ) -> Result<()> {
        let supplemental_predicate = match filter {
            Some(supplemental) => {
                let supplemental = supplemental.rewrite_not();
                if matches!(
                    self.base_predicate.as_deref(),
                    Some(BoundPredicate::AlwaysFalse)
                ) {
                    None
                } else {
                    Self::bind_filter(
                        self.snapshot_schema.as_ref(),
                        Some(&supplemental),
                        self.case_sensitive,
                    )?
                }
            }
            None => None,
        };
        self.supplemental_predicate = supplemental_predicate;
        Ok(())
    }

    /// Opens the next planned task with the current effective predicate.
    pub fn read_next_task(&mut self) -> Result<Option<ArrowRecordBatchIterator>> {
        if self.next_task == self.tasks.len() {
            return Ok(None);
        }
        let task_index = self.next_task;
        let cursor = self.task_reader.read_shared_task(
            Arc::clone(&self.tasks),
            task_index,
            self.base_predicate.clone(),
            self.supplemental_predicate.clone(),
        )?;
        self.next_task += 1;
        Ok(Some(cursor))
    }

    fn bind_filter(
        snapshot_schema: Option<&SchemaRef>,
        filter: Option<&Predicate>,
        case_sensitive: bool,
    ) -> Result<Option<Arc<BoundPredicate>>> {
        match (filter, snapshot_schema) {
            (Some(filter), Some(schema)) => Ok(Some(Arc::new(
                filter.bind(Arc::clone(schema), case_sensitive)?,
            ))),
            (Some(_), None) | (None, _) => Ok(None),
        }
    }

    #[cfg(test)]
    pub(crate) fn bloom_filter_enabled(&self) -> bool {
        self.task_reader.bloom_filter_enabled
    }
}
