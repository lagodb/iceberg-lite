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

use super::Fixture;
use crate::transaction::{ApplyTransactionAction, PreparedTransaction, Transaction};
use crate::{Catalog, ErrorKind};

#[test]
fn catalog_preparation_rejects_replacement_after_transaction_prepare() {
    let fixture = Fixture::new();
    let table = fixture.table("events");
    let transaction = Transaction::new(&table);
    let transaction = transaction
        .update_table_properties()
        .set("owner".to_owned(), "stale".to_owned())
        .apply(transaction)
        .unwrap();
    let PreparedTransaction::Commit(commit) =
        transaction.prepare(&fixture.catalog).unwrap()
    else {
        panic!("property update must produce a commit");
    };

    let other = Fixture::open(&fixture.directory);
    other.drop_table(table.identifier()).unwrap();
    let replacement = fixture.table("events");
    assert_ne!(replacement.metadata().uuid(), table.metadata().uuid());

    let error = fixture
        .catalog
        .prepare_transaction_commit(vec![commit])
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::CatalogCommitConflicts);
    let loaded = other.load_table(table.identifier()).unwrap();
    assert_eq!(loaded.metadata(), replacement.metadata());
    assert_eq!(loaded.metadata_location(), replacement.metadata_location());
}

#[test]
fn publication_rejects_replacement_after_catalog_prepare() {
    let fixture = Fixture::new();
    let table = fixture.table("events");
    let transaction = Transaction::new(&table);
    let transaction = transaction
        .update_table_properties()
        .set("owner".to_owned(), "stale".to_owned())
        .apply(transaction)
        .unwrap();
    let PreparedTransaction::Commit(commit) =
        transaction.prepare(&fixture.catalog).unwrap()
    else {
        panic!("property update must produce a commit");
    };
    let prepared = fixture
        .catalog
        .prepare_transaction_commit(vec![commit])
        .unwrap();

    let other = Fixture::open(&fixture.directory);
    other.drop_table(table.identifier()).unwrap();
    let replacement = fixture.table("events");
    assert_ne!(replacement.metadata().uuid(), table.metadata().uuid());

    let error = prepared.publish().unwrap_err();
    assert_eq!(error.kind(), ErrorKind::CatalogCommitConflicts);
    let loaded = other.load_table(table.identifier()).unwrap();
    assert_eq!(loaded.metadata(), replacement.metadata());
    assert_eq!(loaded.metadata_location(), replacement.metadata_location());
}
