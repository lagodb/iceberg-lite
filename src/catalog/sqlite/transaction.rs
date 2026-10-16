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

use rusqlite::params;

use crate::table::Table;
use crate::transaction::PreparedTableCommit;
use crate::{Catalog, Error, ErrorKind, Result, TableCommit, TableIdent};

use super::SqliteCatalog;

/// File materialization is complete; only an atomic SQLite pointer update remains.
///
/// Owns a shared handle to the originating catalog until publication or drop.
/// This is not a prepared database transaction and holds no SQLite lock.
#[derive(Debug)]
#[must_use = "a prepared SQLite commit must be published to update catalog pointers"]
pub struct PreparedSqliteCommit {
    catalog: SqliteCatalog,
    updates: Vec<SqliteTableUpdate>,
}

#[derive(Debug)]
pub(super) struct SqliteTableUpdate {
    identifier: TableIdent,
    namespace: String,
    previous: String,
    next: String,
}

impl SqliteCatalog {
    /// Materializes metadata before entering the catalog publication phase.
    /// No SQLite transaction or connection lock survives this method.
    pub fn prepare_transaction_commit(
        &self,
        commits: Vec<PreparedTableCommit>,
    ) -> Result<PreparedSqliteCommit> {
        if commits.is_empty() {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "SQLite transaction requires a table change",
            ));
        }
        let mut updates = Vec::with_capacity(commits.len());
        for prepared in commits {
            let (_, commit) = prepared.into_parts();
            let (_, update) = self.prepare_update(commit)?;
            updates.push(update);
        }
        Ok(PreparedSqliteCommit::new(self.clone(), updates))
    }

    pub(super) fn prepare_update(
        &self,
        commit: TableCommit,
    ) -> Result<(Table, SqliteTableUpdate)> {
        // Preserve upstream update_table's load/apply/write/CAS sequence.
        let identifier = commit.identifier().clone();
        let current = self.load_table(&identifier)?;
        let previous = current.metadata_location_result()?.to_owned();
        let staged = commit.apply(current)?;
        let next = staged.metadata_location_result()?.to_owned();
        staged.metadata().write_to(staged.file_io(), &next)?;
        let namespace = Self::namespace_key(identifier.namespace())?;
        Ok((
            staged,
            SqliteTableUpdate {
                identifier,
                namespace,
                previous,
                next,
            },
        ))
    }
}

impl PreparedSqliteCommit {
    pub(super) fn new(
        catalog: SqliteCatalog,
        updates: Vec<SqliteTableUpdate>,
    ) -> Self {
        Self { catalog, updates }
    }

    /// Publishes all table changes atomically. A conflict rolls back the batch.
    /// Does not rebase, retry, or perform table-file I/O after preparation.
    pub fn publish(self) -> Result<()> {
        let mut connection = self.catalog.connection()?;
        let tx = connection.transaction()?;
        for update in self.updates {
            let changed = tx.execute(
                "UPDATE iceberg_tables SET metadata_location = ?1, previous_metadata_location = ?2
                 WHERE catalog_name = ?3 AND table_namespace = ?4 AND table_name = ?5
                 AND (iceberg_type = 'TABLE' OR iceberg_type IS NULL) AND metadata_location = ?2",
                params![update.next, update.previous, self.catalog.name(), update.namespace, update.identifier.name()],
            )?;
            if changed == 0 {
                return Err(Error::new(
                    ErrorKind::CatalogCommitConflicts,
                    format!("Commit conflicted for table: {}", update.identifier),
                )
                .with_retryable(true));
            }
        }
        tx.commit()?;
        Ok(())
    }
}
