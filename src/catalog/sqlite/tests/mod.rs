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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use rusqlite::Connection;
use tempfile::TempDir;

use crate::io::{LocalStorage, Storage, StorageConfig, StorageFactory};
use crate::spec::{NestedField, PrimitiveType, Schema, Type};
use crate::table::Table;
use crate::transaction::PreparedTableCommit;
use crate::{
    Catalog, CatalogBuilder, ErrorKind, NamespaceIdent, Result, TableCommit,
    TableCreation, TableIdent, TableUpdate,
};

use super::{SqliteCatalog, SqliteCatalogBuilder};

#[derive(Debug)]
struct LocalFactory;

impl StorageFactory for LocalFactory {
    fn build(&self, _config: StorageConfig) -> Result<Arc<dyn Storage>> {
        Ok(Arc::new(LocalStorage))
    }
}

struct Fixture {
    directory: TempDir,
    catalog: SqliteCatalog,
    namespace: NamespaceIdent,
}

impl Fixture {
    fn new() -> Self {
        let directory = TempDir::new().unwrap();
        Connection::open(directory.path().join("catalog.db")).unwrap();
        let catalog = Self::open(&directory);
        let namespace = NamespaceIdent::new("test".to_owned());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .unwrap();
        Self {
            directory,
            catalog,
            namespace,
        }
    }

    fn open(directory: &TempDir) -> SqliteCatalog {
        SqliteCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFactory))
            .load(
                "local",
                HashMap::from([
                    (
                        "uri".to_owned(),
                        format!(
                            "sqlite:{}",
                            directory.path().join("catalog.db").display()
                        ),
                    ),
                    (
                        "warehouse".to_owned(),
                        directory
                            .path()
                            .join("warehouse")
                            .to_str()
                            .unwrap()
                            .to_owned(),
                    ),
                ]),
            )
            .unwrap()
    }

    fn table(&self, name: &str) -> Table {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long))
                    .into(),
            ])
            .build()
            .unwrap();
        self.catalog
            .create_table(
                &self.namespace,
                TableCreation::builder()
                    .name(name.to_owned())
                    .schema(schema)
                    .build(),
            )
            .unwrap()
    }

    fn commit(table: &Table, value: &str) -> TableCommit {
        TableCommit::builder()
            .ident(table.identifier().clone())
            .file_io(table.file_io().clone())
            .requirements(vec![])
            .updates(vec![TableUpdate::SetProperties {
                updates: HashMap::from([("owner".to_owned(), value.to_owned())]),
            }])
            .build()
    }

    fn prepared(table: &Table, value: &str) -> PreparedTableCommit {
        PreparedTableCommit::new(table.clone(), Self::commit(table, value))
    }
}

#[test]
fn metadata_pointer_survives_reopen_and_registration() {
    let fixture = Fixture::new();
    let table = fixture.table("events");
    let committed = fixture
        .catalog
        .update_table(Fixture::commit(&table, "writer"))
        .unwrap();
    let reopened = Fixture::open(&fixture.directory);
    let loaded = reopened.load_table(table.identifier()).unwrap();
    assert_eq!(loaded.metadata_location(), committed.metadata_location());
    assert_ne!(loaded.metadata_location(), table.metadata_location());
    let registered =
        TableIdent::new(fixture.namespace.clone(), "registered".to_owned());
    reopened
        .register_table(
            &registered,
            loaded.metadata_location_result().unwrap().to_owned(),
        )
        .unwrap();
    assert_eq!(
        reopened.load_table(&registered).unwrap().metadata().uuid(),
        table.metadata().uuid()
    );
}

#[test]
fn prepared_commit_keeps_its_catalog_alive_and_preserves_other_catalogs() {
    let fixture = Fixture::new();
    let table = fixture.table("events");
    let other = SqliteCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFactory))
        .load(
            "other",
            HashMap::from([
                (
                    "uri".to_owned(),
                    format!(
                        "sqlite:{}",
                        fixture.directory.path().join("catalog.db").display()
                    ),
                ),
                (
                    "warehouse".to_owned(),
                    fixture
                        .directory
                        .path()
                        .join("warehouse")
                        .to_str()
                        .unwrap()
                        .to_owned(),
                ),
            ]),
        )
        .unwrap();
    other
        .register_table(
            table.identifier(),
            table.metadata_location_result().unwrap().to_owned(),
        )
        .unwrap();
    let cloned = fixture.catalog.clone();
    let prepared = cloned
        .prepare_transaction_commit(vec![Fixture::prepared(
            &table,
            "original_catalog",
        )])
        .unwrap();
    let Fixture {
        directory, catalog, ..
    } = fixture;
    drop(catalog);
    drop(cloned);

    prepared.publish().unwrap();

    let reopened = Fixture::open(&directory);
    let updated = reopened.load_table(table.identifier()).unwrap();
    assert_eq!(
        updated
            .metadata()
            .properties()
            .get("owner")
            .map(String::as_str),
        Some("original_catalog")
    );
    assert_ne!(updated.metadata_location(), table.metadata_location());
    assert_eq!(
        other
            .load_table(table.identifier())
            .unwrap()
            .metadata_location(),
        table.metadata_location()
    );
}

#[test]
fn stale_prepared_pointer_does_not_overwrite_another_connection() {
    let fixture = Fixture::new();
    let table = fixture.table("events");
    let prepared = fixture
        .catalog
        .prepare_transaction_commit(vec![Fixture::prepared(&table, "stale")])
        .unwrap();
    let other = Fixture::open(&fixture.directory);
    let winner = other
        .update_table(Fixture::commit(&table, "winner"))
        .unwrap();
    let error = prepared.publish().unwrap_err();
    assert_eq!(error.kind(), ErrorKind::CatalogCommitConflicts);
    assert_eq!(
        fixture
            .catalog
            .load_table(table.identifier())
            .unwrap()
            .metadata_location(),
        winner.metadata_location()
    );
}

#[test]
fn later_table_conflict_rolls_back_all_pointer_updates() {
    let fixture = Fixture::new();
    let first = fixture.table("first");
    let second = fixture.table("second");
    let prepared = fixture
        .catalog
        .prepare_transaction_commit(vec![
            Fixture::prepared(&first, "batch"),
            Fixture::prepared(&second, "batch"),
        ])
        .unwrap();
    let other = Fixture::open(&fixture.directory);
    let winner = other
        .update_table(Fixture::commit(&second, "winner"))
        .unwrap();
    assert_eq!(
        prepared.publish().unwrap_err().kind(),
        ErrorKind::CatalogCommitConflicts
    );
    assert_eq!(
        other
            .load_table(first.identifier())
            .unwrap()
            .metadata_location(),
        first.metadata_location()
    );
    assert_eq!(
        other
            .load_table(second.identifier())
            .unwrap()
            .metadata_location(),
        winner.metadata_location()
    );
}

#[test]
fn namespace_properties_replace_and_listing_respects_components() {
    let fixture = Fixture::new();
    let a = NamespaceIdent::new("a".to_owned());
    let ab = NamespaceIdent::new("ab".to_owned());
    let child = NamespaceIdent::from_strs(["a", "child"]).unwrap();
    for namespace in [
        &a,
        &ab,
        &child,
        &NamespaceIdent::from_strs(["a", "child", "grandchild"]).unwrap(),
    ] {
        fixture
            .catalog
            .create_namespace(namespace, HashMap::new())
            .unwrap();
    }
    assert_eq!(
        fixture
            .catalog
            .list_namespaces(Some(&a))
            .unwrap()
            .into_iter()
            .collect::<HashSet<_>>(),
        HashSet::from([
            child,
            NamespaceIdent::from_strs(["a", "child", "grandchild"]).unwrap()
        ])
    );
    fixture
        .catalog
        .update_namespace(&a, HashMap::from([("old".to_owned(), "value".to_owned())]))
        .unwrap();
    fixture
        .catalog
        .update_namespace(&a, HashMap::new())
        .unwrap();
    assert!(
        !fixture
            .catalog
            .get_namespace(&a)
            .unwrap()
            .properties()
            .contains_key("old")
    );
    assert!(fixture.catalog.namespace_exists(&a).unwrap());
    assert_eq!(
        fixture
            .catalog
            .create_namespace(&NamespaceIdent::new("a.b".to_owned()), HashMap::new())
            .unwrap_err()
            .kind(),
        ErrorKind::DataInvalid
    );
}

#[test]
fn locked_database_returns_an_error_without_publishing() {
    let fixture = Fixture::new();
    let table = fixture.table("events");
    let prepared = fixture
        .catalog
        .prepare_transaction_commit(vec![Fixture::prepared(&table, "blocked")])
        .unwrap();
    let blocker =
        Connection::open(fixture.directory.path().join("catalog.db")).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    assert!(prepared.publish().is_err());
    blocker.execute_batch("ROLLBACK").unwrap();
    assert_eq!(
        fixture
            .catalog
            .load_table(table.identifier())
            .unwrap()
            .metadata_location(),
        table.metadata_location()
    );
}

mod contract;
