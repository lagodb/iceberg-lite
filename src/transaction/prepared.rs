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

//! A materialized Iceberg transaction whose catalog publication is deferred.

use crate::table::Table;
use crate::{TableCommit, TableIdent};

/// Result of preparing a [`super::Transaction`] without publishing it.
#[derive(Debug)]
pub enum PreparedTransaction {
    /// Every action was empty, so no catalog update is required.
    Noop(Table),
    /// A catalog-visible table update is ready to publish.
    Commit(PreparedTableCommit),
}

impl PreparedTransaction {
    /// Returns the table view produced while preparing the transaction.
    pub fn table(&self) -> &Table {
        match self {
            Self::Noop(table) => table,
            Self::Commit(commit) => commit.table(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TableCommitKind {
    Create,
    Update,
}

/// A table commit prepared for creation or against a refreshed catalog table.
///
/// Construction remains owned by [`super::Transaction`], so callers cannot
/// bypass action validation or manufacture requirements and updates directly.
#[derive(Debug)]
pub struct PreparedTableCommit {
    table: Table,
    commit: TableCommit,
    kind: TableCommitKind,
}

impl PreparedTableCommit {
    pub(crate) fn new(table: Table, commit: TableCommit) -> Self {
        Self {
            table,
            commit,
            kind: TableCommitKind::Update,
        }
    }

    pub(crate) fn new_create(table: Table, commit: TableCommit) -> Self {
        Self {
            table,
            commit,
            kind: TableCommitKind::Create,
        }
    }

    /// Returns the table identifier targeted by this commit.
    pub fn identifier(&self) -> &TableIdent {
        self.commit.identifier()
    }

    /// Returns the transaction-local table view after applying all actions.
    pub fn table(&self) -> &Table {
        &self.table
    }

    pub(crate) fn into_parts(self) -> (Table, TableCommit) {
        (self.table, self.commit)
    }

    pub(crate) fn is_create(&self) -> bool {
        self.kind == TableCommitKind::Create
    }
}
