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

use std::collections::HashMap;
use std::sync::Arc;

use uuid::Uuid;

use super::{create_test_transaction, make_v2_table, setup_test_table};
use crate::catalog::MockCatalog;
use crate::{Error, ErrorKind};

#[test]
fn prepare_rejects_recreated_table_without_replacing_base() {
    let table = make_v2_table();
    let mut transaction = create_test_transaction(&table);
    let metadata = table
        .metadata()
        .clone()
        .into_builder(None)
        .assign_uuid(Uuid::new_v4())
        .build()
        .unwrap()
        .metadata;
    let recreated = table.clone().with_metadata(Arc::new(metadata));
    let mut catalog = MockCatalog::new();
    catalog
        .expect_load_table()
        .times(1)
        .returning_st(move |_| Ok(recreated.clone()));
    catalog.expect_update_table().times(0);

    let error = transaction.prepare_once(&catalog).unwrap_err();

    assert_eq!(error.kind(), ErrorKind::CatalogCommitConflicts);
    assert!(!error.retryable());
    assert_eq!(transaction.table.metadata().uuid(), table.metadata().uuid());
    assert!(
        !transaction
            .table
            .metadata()
            .properties()
            .contains_key("test.key")
    );
}

#[test]
fn commit_rejects_recreated_table_before_or_after_retry() {
    // Cover replacement both before the first attempt and after a commit
    // conflict causes the transaction to refresh and retry.
    for successful_refreshes in [0, 1] {
        let table = setup_test_table("3");
        let transaction = create_test_transaction(&table);
        let metadata = table
            .metadata()
            .clone()
            .into_builder(None)
            .assign_uuid(Uuid::new_v4())
            .build()
            .unwrap()
            .metadata;
        let recreated = table.clone().with_metadata(Arc::new(metadata));
        let mut catalog = MockCatalog::new();
        let mut loads = 0;
        catalog
            .expect_load_table()
            .times(successful_refreshes + 1)
            .returning_st(move |_| {
                loads += 1;
                Ok(if loads <= successful_refreshes {
                    table.clone()
                } else {
                    recreated.clone()
                })
            });
        catalog
            .expect_update_table()
            .times(successful_refreshes)
            .returning_st(|_| {
                Err(
                    Error::new(ErrorKind::CatalogCommitConflicts, "commit conflict")
                        .with_retryable(true),
                )
            });

        let error = transaction.commit(&catalog).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::CatalogCommitConflicts);
        assert_eq!(
            error.message(),
            "Requirement failed: Table UUID does not match"
        );
        assert!(!error.retryable());
    }
}

#[test]
fn prepare_preserves_concurrent_metadata_updates_on_the_same_table() {
    let table = make_v2_table();
    let transaction = create_test_transaction(&table);
    let metadata = table
        .metadata()
        .clone()
        .into_builder(None)
        .set_properties(HashMap::from([("concurrent.key".into(), "value".into())]))
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    let refreshed = table.clone().with_metadata(Arc::new(metadata));
    let mut catalog = MockCatalog::new();
    catalog
        .expect_load_table()
        .times(1)
        .returning_st(move |_| Ok(refreshed.clone()));
    catalog.expect_update_table().times(0);

    let prepared = transaction.prepare(&catalog).unwrap();
    let metadata = prepared.table().metadata();

    assert_eq!(metadata.uuid(), table.metadata().uuid());
    assert_eq!(metadata.properties()["concurrent.key"], "value");
    assert_eq!(metadata.properties()["test.key"], "test.value");
}
