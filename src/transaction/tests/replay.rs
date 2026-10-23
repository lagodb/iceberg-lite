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

use super::{make_v2_minimal_table, make_v2_table};
use crate::TableRequirement;
use crate::catalog::MockCatalog;
use crate::transaction::{
    ApplyTransactionAction, PreparedTransaction, StagedTableCreate, Transaction,
};

#[test]
fn empty_snapshot_expiration_remains_a_noop() {
    let table = make_v2_minimal_table();
    let transaction = Transaction::new(&table);
    let transaction = transaction.expire_snapshots().apply(transaction).unwrap();
    let mut catalog = MockCatalog::new();
    catalog
        .expect_load_table()
        .times(1)
        .returning_st(move |_| Ok(table.clone()));
    catalog.expect_update_table().times(0);

    assert!(matches!(
        transaction.prepare(&catalog).unwrap(),
        PreparedTransaction::Noop(_)
    ));
}

#[test]
fn snapshot_expiration_retains_one_uuid_requirement() {
    let table = make_v2_table();
    let snapshot_id = table
        .metadata()
        .snapshots()
        .find(|snapshot| {
            Some(snapshot.snapshot_id()) != table.metadata().current_snapshot_id()
        })
        .unwrap()
        .snapshot_id();
    let expected = vec![
        TableRequirement::UuidMatch {
            uuid: table.metadata().uuid(),
        },
        TableRequirement::RefSnapshotIdMatch {
            r#ref: "main".to_owned(),
            snapshot_id: table.metadata().current_snapshot_id(),
        },
    ];
    let transaction = Transaction::new(&table);
    let transaction = transaction
        .expire_snapshots()
        .expire_snapshot_ids([snapshot_id])
        .apply(transaction)
        .unwrap();
    let mut catalog = MockCatalog::new();
    catalog
        .expect_load_table()
        .times(1)
        .returning_st(move |_| Ok(table.clone()));
    let PreparedTransaction::Commit(prepared) =
        transaction.prepare(&catalog).unwrap()
    else {
        panic!("snapshot expiration must produce a commit");
    };
    let (_, mut commit) = prepared.into_parts();

    assert_eq!(commit.take_requirements(), expected);
}

#[test]
fn action_free_staged_create_requires_absence_without_loading_a_table() {
    let staged = StagedTableCreate::new(make_v2_minimal_table());
    let mut catalog = MockCatalog::new();
    catalog.expect_load_table().times(0);
    catalog.expect_update_table().times(0);
    let PreparedTransaction::Commit(prepared) =
        staged.transaction().prepare(&catalog).unwrap()
    else {
        panic!("staged create must produce a commit");
    };
    let (_, mut commit) = prepared.into_parts();

    assert_eq!(commit.take_requirements(), vec![TableRequirement::NotExist]);
}
