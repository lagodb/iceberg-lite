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

//! Projection planning for one Parquet data-file read.
//!
//! The requested projection is the schema visible to the scan caller. The
//! reader projection additionally contains fields needed by post-transform
//! predicates. The physical data projection excludes Iceberg metadata fields,
//! whose values are supplied separately by the reader pipeline.

use std::collections::HashSet;

use crate::metadata_columns::is_metadata_field;

/// Separates caller-visible output from fields required internally while
/// reading and transforming a data file.
pub(super) struct ReadProjectionPlan {
    requested_field_ids: Vec<i32>,
    reader_field_ids: Vec<i32>,
    physical_data_field_ids: Vec<i32>,
}

impl ReadProjectionPlan {
    pub(super) fn new(
        requested_field_ids: &[i32],
        post_transform_field_ids: &HashSet<i32>,
    ) -> Self {
        let requested_field_ids = requested_field_ids.to_vec();
        let mut reader_field_ids = requested_field_ids.clone();
        let mut ordered_post_transform_field_ids =
            post_transform_field_ids.iter().copied().collect::<Vec<_>>();
        ordered_post_transform_field_ids.sort_unstable();

        for field_id in ordered_post_transform_field_ids {
            if !reader_field_ids.contains(&field_id) {
                reader_field_ids.push(field_id);
            }
        }

        let physical_data_field_ids = reader_field_ids
            .iter()
            .copied()
            .filter(|field_id| !is_metadata_field(*field_id))
            .collect();

        Self {
            requested_field_ids,
            reader_field_ids,
            physical_data_field_ids,
        }
    }

    /// Adds a field needed only by the reader pipeline.
    pub(super) fn with_internal_field(mut self, field_id: i32) -> Self {
        if !self.reader_field_ids.contains(&field_id) {
            self.reader_field_ids.push(field_id);
            if !is_metadata_field(field_id) {
                self.physical_data_field_ids.push(field_id);
            }
        }
        self
    }

    #[cfg(test)]
    pub(super) fn requested_field_ids(&self) -> &[i32] {
        &self.requested_field_ids
    }

    pub(super) fn reader_field_ids(&self) -> &[i32] {
        &self.reader_field_ids
    }

    pub(super) fn physical_data_field_ids(&self) -> &[i32] {
        &self.physical_data_field_ids
    }

    pub(super) fn requires_output_pruning(&self) -> bool {
        self.reader_field_ids != self.requested_field_ids
    }

    pub(super) fn into_requested_field_ids(self) -> Vec<i32> {
        self.requested_field_ids
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_columns::RESERVED_FIELD_ID_POS;

    #[test]
    fn separates_requested_internal_and_physical_fields() {
        let plan =
            ReadProjectionPlan::new(&[3], &HashSet::from([RESERVED_FIELD_ID_POS, 2]));

        assert_eq!(plan.requested_field_ids(), &[3]);
        assert_eq!(plan.reader_field_ids(), &[3, 2, RESERVED_FIELD_ID_POS]);
        assert_eq!(plan.physical_data_field_ids(), &[3, 2]);
        assert!(plan.requires_output_pruning());
    }

    #[test]
    fn preserves_explicit_empty_projection() {
        let plan = ReadProjectionPlan::new(&[], &HashSet::new());

        assert!(plan.requested_field_ids().is_empty());
        assert!(plan.reader_field_ids().is_empty());
        assert!(plan.physical_data_field_ids().is_empty());
        assert!(!plan.requires_output_pruning());
    }
}
