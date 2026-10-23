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

//! Shared action replay for published updates and staged table creation.
//!
//! Fork synchronization note: this module owns the action replay, requirement
//! accumulation, and [`crate::TableCommit`] construction performed by
//! iceberg-rust's `Transaction::do_commit`. Reconcile upstream changes to
//! `Transaction::do_commit` and `Transaction::apply` here.

use crate::table::Table;
use crate::{Result, TableCommit, TableRequirement};

use super::action::TransactionActionEntry;
use super::prepared::TableCommitKind;
use super::{
    PreparedTableCommit, PreparedTransaction, Transaction, TransactionOrigin,
};

impl Transaction {
    pub(super) fn replay_actions(
        base: &Table,
        actions: &mut [TransactionActionEntry],
        origin: TransactionOrigin,
    ) -> Result<PreparedTransaction> {
        let (mut updates, mut requirements, kind) = match origin {
            TransactionOrigin::Published => {
                (Vec::new(), Vec::new(), TableCommitKind::Update)
            }
            TransactionOrigin::StagedCreate { initial_updates } => (
                initial_updates,
                vec![TableRequirement::NotExist],
                TableCommitKind::Create,
            ),
        };
        // The staged base already contains initialization updates. Include
        // them in the catalog request without applying them a second time.
        let mut current_table = base.clone();
        // Reuse each action entry so its retry-persistent state survives replay.
        for entry in actions {
            let action_commit = entry.commit(&current_table)?;
            if action_commit.is_empty() {
                continue;
            }
            current_table = Self::apply(
                current_table,
                action_commit,
                base.metadata(),
                kind,
                &mut updates,
                &mut requirements,
            )?;
        }

        // The create requirement also keeps action-free staged creates from
        // becoming no-ops: their initialized table still needs publication.
        if updates.is_empty() && requirements.is_empty() {
            return Ok(PreparedTransaction::Noop(current_table));
        }

        if kind == TableCommitKind::Update {
            let identity = TableRequirement::UuidMatch {
                uuid: base.metadata().uuid(),
            };
            if !requirements.contains(&identity) {
                requirements.push(identity);
            }
        }

        let commit = TableCommit::builder()
            .ident(base.identifier().to_owned())
            .file_io(base.file_io().clone())
            .updates(updates)
            .requirements(requirements)
            .build();
        let prepared = match kind {
            TableCommitKind::Update => {
                PreparedTableCommit::new(current_table, commit)
            }
            TableCommitKind::Create => {
                PreparedTableCommit::new_create(current_table, commit)
            }
        };
        Ok(PreparedTransaction::Commit(prepared))
    }
}
