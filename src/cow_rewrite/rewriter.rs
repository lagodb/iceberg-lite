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

use arrow_array::RecordBatch;

use crate::Result;

/// Result of rewriting a single record batch.
#[derive(Debug)]
pub struct CowBatchRewrite {
    /// Rewritten output batch, or `None` when the input batch is fully removed.
    ///
    /// Output batches must use the same schema as their input batch: the current
    /// schema by default, or the snapshot's schema when explicitly selected.
    /// Rewriters must also preserve each source file's partition
    /// values: this primitive writes replacements into the source file's
    /// partition and does not repartition rows.
    pub output: Option<RecordBatch>,
    /// Whether the rewrite changed the input batch contents.
    ///
    /// Set this to `true` whenever `output` differs from the input batch,
    /// including filtered rows, updated values, reordered rows, or `None`.
    pub changed: bool,
}

/// Rewrites record batches for copy-on-write operations.
///
/// `rewrite_batch` runs synchronously on the caller's thread as part of the
/// read/write pipeline. The trait is object safe for `Arc<dyn CowBatchRewriter>`.
pub trait CowBatchRewriter: Send + Sync {
    /// Rewrites a record batch and reports whether it changed.
    ///
    /// `output: None` means the batch is fully removed, which is itself a
    /// change: the orchestrator treats it as changed regardless of the
    /// `changed` flag, so a rewriter cannot accidentally keep dropped rows
    /// alive by reporting `changed: false` alongside a `None` output.
    fn rewrite_batch(&self, batch: RecordBatch) -> Result<CowBatchRewrite>;
}
