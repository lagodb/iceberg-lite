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

use crate::TableUpdate;
use crate::table::Table;

use super::{Transaction, TransactionOrigin};

/// A catalog-invisible table initialized for a create transaction.
#[derive(Debug, Clone)]
pub struct StagedTableCreate {
    table: Table,
    initial_updates: Vec<TableUpdate>,
}

impl StagedTableCreate {
    pub(crate) fn new(table: Table) -> Self {
        // The catalog response is authoritative for UUID, location and IDs.
        // REST create transactions must replay that initialization explicitly.
        let metadata = table.metadata();
        let mut initial_updates = Vec::with_capacity(10);
        initial_updates.push(TableUpdate::AssignUuid {
            uuid: metadata.uuid(),
        });
        initial_updates.push(TableUpdate::UpgradeFormatVersion {
            format_version: metadata.format_version(),
        });
        initial_updates.push(TableUpdate::AddSchema {
            schema: metadata.current_schema().as_ref().clone(),
        });
        initial_updates.push(TableUpdate::SetCurrentSchema { schema_id: -1 });
        initial_updates.push(TableUpdate::AddSpec {
            spec: metadata
                .default_partition_spec()
                .as_ref()
                .clone()
                .into_unbound(),
        });
        initial_updates.push(TableUpdate::SetDefaultSpec { spec_id: -1 });
        initial_updates.push(TableUpdate::AddSortOrder {
            sort_order: metadata.default_sort_order().as_ref().clone(),
        });
        initial_updates.push(TableUpdate::SetDefaultSortOrder { sort_order_id: -1 });
        initial_updates.push(TableUpdate::SetLocation {
            location: metadata.location().to_owned(),
        });
        if !metadata.properties().is_empty() {
            initial_updates.push(TableUpdate::SetProperties {
                updates: metadata.properties().clone(),
            });
        }
        Self {
            table,
            initial_updates,
        }
    }

    /// Returns the transaction-local table initialized by the catalog.
    pub fn table(&self) -> &Table {
        &self.table
    }

    /// Starts a transaction that must be prepared and published by the
    /// originating catalog's transaction API.
    pub fn transaction(&self) -> Transaction {
        Transaction::for_staged_create(
            self.table.clone(),
            self.initial_updates.clone(),
        )
    }
}

impl Transaction {
    pub(super) fn for_staged_create(
        table: Table,
        initial_updates: Vec<TableUpdate>,
    ) -> Self {
        Self {
            table,
            actions: vec![],
            origin: TransactionOrigin::StagedCreate { initial_updates },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::StagedTableCreate;
    use crate::TableUpdate;
    use crate::transaction::tests::make_v2_table;

    #[test]
    fn initialization_updates_describe_the_authoritative_staged_metadata() {
        let table = make_v2_table();
        let metadata = table.metadata();
        let mut expected = vec![
            TableUpdate::AssignUuid {
                uuid: metadata.uuid(),
            },
            TableUpdate::UpgradeFormatVersion {
                format_version: metadata.format_version(),
            },
            TableUpdate::AddSchema {
                schema: metadata.current_schema().as_ref().clone(),
            },
            TableUpdate::SetCurrentSchema { schema_id: -1 },
            TableUpdate::AddSpec {
                spec: metadata
                    .default_partition_spec()
                    .as_ref()
                    .clone()
                    .into_unbound(),
            },
            TableUpdate::SetDefaultSpec { spec_id: -1 },
            TableUpdate::AddSortOrder {
                sort_order: metadata.default_sort_order().as_ref().clone(),
            },
            TableUpdate::SetDefaultSortOrder { sort_order_id: -1 },
            TableUpdate::SetLocation {
                location: metadata.location().to_owned(),
            },
        ];
        if !metadata.properties().is_empty() {
            expected.push(TableUpdate::SetProperties {
                updates: metadata.properties().clone(),
            });
        }

        let staged = StagedTableCreate::new(table);

        assert_eq!(staged.initial_updates, expected);
    }
}
