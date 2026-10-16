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
use std::path::Path;

use rusqlite::{Connection, params};
use uuid::Uuid;

use crate::{
    Catalog, ErrorKind, NamespaceIdent, TableCommit, TableCreation, TableIdent,
    TableRequirement,
};

use super::Fixture;

#[test]
fn descendant_listing_does_not_invent_missing_intermediate_namespaces() {
    let fixture = Fixture::new();
    let descendant = NamespaceIdent::from_strs(["test", "missing", "leaf"]).unwrap();
    fixture
        .catalog
        .create_namespace(&descendant, HashMap::new())
        .unwrap();
    let listed = fixture
        .catalog
        .list_namespaces(Some(&fixture.namespace))
        .unwrap();
    assert_eq!(listed, vec![descendant.clone()]);
    assert!(fixture.catalog.namespace_exists(&listed[0]).unwrap());
    assert_eq!(
        fixture.catalog.get_namespace(&listed[0]).unwrap().name(),
        &descendant
    );
}

#[test]
fn rename_preserves_metadata_and_upstream_error_precedence() {
    let fixture = Fixture::new();
    let source = fixture.table("source");
    let occupied = fixture.table("occupied");
    let missing = TableIdent::new(fixture.namespace.clone(), "missing".to_owned());
    assert_eq!(
        fixture
            .catalog
            .rename_table(&missing, occupied.identifier())
            .unwrap_err()
            .kind(),
        ErrorKind::TableNotFound
    );
    assert_eq!(
        fixture
            .catalog
            .rename_table(source.identifier(), occupied.identifier())
            .unwrap_err()
            .kind(),
        ErrorKind::TableAlreadyExists
    );
    let destination_namespace =
        NamespaceIdent::from_strs(["destination", "nested"]).unwrap();
    let destination =
        TableIdent::new(destination_namespace.clone(), "renamed".to_owned());
    assert_eq!(
        fixture
            .catalog
            .rename_table(source.identifier(), &destination)
            .unwrap_err()
            .kind(),
        ErrorKind::NamespaceNotFound
    );
    fixture
        .catalog
        .create_namespace(&destination_namespace, HashMap::new())
        .unwrap();
    fixture
        .catalog
        .rename_table(source.identifier(), &destination)
        .unwrap();
    assert!(!fixture.catalog.table_exists(source.identifier()).unwrap());
    let loaded = fixture.catalog.load_table(&destination).unwrap();
    assert_eq!(loaded.metadata_location(), source.metadata_location());
    assert_eq!(loaded.metadata().uuid(), source.metadata().uuid());
    assert_eq!(
        fixture.catalog.list_tables(&destination_namespace).unwrap(),
        vec![destination]
    );
    // Upstream treats identical identifiers as a no-op, including absent ones.
    fixture.catalog.rename_table(&missing, &missing).unwrap();
}

#[test]
fn drop_keeps_files_purge_removes_metadata_and_nonempty_namespace_is_rejected() {
    let fixture = Fixture::new();
    let table = fixture.table("events");
    let path = table.metadata_location_result().unwrap().to_owned();
    assert_eq!(
        fixture
            .catalog
            .drop_namespace(&fixture.namespace)
            .unwrap_err()
            .kind(),
        ErrorKind::PreconditionFailed
    );
    fixture.catalog.drop_table(table.identifier()).unwrap();
    assert!(Path::new(&path).exists());
    assert_eq!(
        fixture
            .catalog
            .load_table(table.identifier())
            .unwrap_err()
            .kind(),
        ErrorKind::TableNotFound
    );
    assert_eq!(
        fixture
            .catalog
            .drop_table(table.identifier())
            .unwrap_err()
            .kind(),
        ErrorKind::TableNotFound
    );
    fixture
        .catalog
        .register_table(table.identifier(), path.clone())
        .unwrap();
    assert_eq!(
        fixture
            .catalog
            .register_table(table.identifier(), path.clone())
            .unwrap_err()
            .kind(),
        ErrorKind::TableAlreadyExists
    );
    fixture.catalog.purge_table(table.identifier()).unwrap();
    assert!(!Path::new(&path).exists());
    assert!(!fixture.catalog.table_exists(table.identifier()).unwrap());
    fixture.catalog.drop_namespace(&fixture.namespace).unwrap();
    assert!(
        !fixture
            .catalog
            .namespace_exists(&fixture.namespace)
            .unwrap()
    );
    assert_eq!(
        fixture
            .catalog
            .get_namespace(&fixture.namespace)
            .unwrap_err()
            .kind(),
        ErrorKind::NamespaceNotFound
    );
    assert_eq!(
        fixture
            .catalog
            .list_tables(&fixture.namespace)
            .unwrap_err()
            .kind(),
        ErrorKind::NamespaceNotFound
    );
}

#[test]
fn registration_creates_an_implicit_namespace_without_rewriting_metadata() {
    let fixture = Fixture::new();
    let table = fixture.table("events");
    let namespace = NamespaceIdent::new("registered".to_owned());
    let identifier = TableIdent::new(namespace.clone(), "alias".to_owned());
    let path = table.metadata_location_result().unwrap();
    let before = std::fs::read(path).unwrap();
    fixture
        .catalog
        .register_table(&identifier, path.to_owned())
        .unwrap();
    assert!(fixture.catalog.namespace_exists(&namespace).unwrap());
    assert!(
        fixture
            .catalog
            .get_namespace(&namespace)
            .unwrap()
            .properties()
            .is_empty()
    );
    assert_eq!(std::fs::read(path).unwrap(), before);
}

#[test]
fn explicit_and_namespace_locations_override_warehouse() {
    let fixture = Fixture::new();
    let base = fixture.table("warehouse_default");
    assert_eq!(
        base.metadata().location(),
        fixture
            .directory
            .path()
            .join("warehouse/test/warehouse_default")
            .to_str()
            .unwrap()
    );
    let namespace_location = fixture.directory.path().join("custom_namespace");
    fixture
        .catalog
        .update_namespace(
            &fixture.namespace,
            HashMap::from([(
                "location".to_owned(),
                namespace_location.to_str().unwrap().to_owned(),
            )]),
        )
        .unwrap();
    let namespaced = fixture.table("namespace_default");
    assert_eq!(
        namespaced.metadata().location(),
        namespace_location
            .join("namespace_default")
            .to_str()
            .unwrap()
    );
    let explicit_location = fixture.directory.path().join("explicit_table");
    let table = fixture
        .catalog
        .create_table(
            &fixture.namespace,
            TableCreation::builder()
                .name("explicit".to_owned())
                .location(explicit_location.to_str().unwrap().to_owned())
                .schema(base.metadata().current_schema().as_ref().clone())
                .build(),
        )
        .unwrap();
    assert_eq!(
        table.metadata().location(),
        explicit_location.to_str().unwrap()
    );
    assert_eq!(
        fixture
            .catalog
            .create_table(
                &fixture.namespace,
                TableCreation::builder()
                    .name("explicit".to_owned())
                    .schema(base.metadata().current_schema().as_ref().clone())
                    .build()
            )
            .unwrap_err()
            .kind(),
        ErrorKind::TableAlreadyExists
    );
}

#[test]
fn v1_filters_views_and_other_catalogs_but_accepts_null_record_type() {
    let fixture = Fixture::new();
    let table = fixture.table("legacy");
    let connection =
        Connection::open(fixture.directory.path().join("catalog.db")).unwrap();
    connection.execute("UPDATE iceberg_tables SET iceberg_type = NULL WHERE table_name = 'legacy'", []).unwrap();
    connection.execute(
        "INSERT INTO iceberg_tables VALUES ('local', 'test', 'view', ?1, NULL, 'VIEW'),
         ('another_catalog', 'test', 'other', ?1, NULL, 'TABLE')",
        params![table.metadata_location_result().unwrap()],
    ).unwrap();
    assert_eq!(
        fixture.catalog.list_tables(&fixture.namespace).unwrap(),
        vec![table.identifier().clone()]
    );
    let view = TableIdent::new(fixture.namespace.clone(), "view".to_owned());
    assert!(!fixture.catalog.table_exists(&view).unwrap());
    assert_eq!(
        fixture.catalog.drop_table(&view).unwrap_err().kind(),
        ErrorKind::TableNotFound
    );
    let updated = fixture
        .catalog
        .update_table(Fixture::commit(&table, "updated"))
        .unwrap();
    assert_ne!(updated.metadata_location(), table.metadata_location());
    fixture.catalog.drop_table(table.identifier()).unwrap();
    // A V1 view still occupies the namespace even though the table API excludes it.
    assert_eq!(
        fixture
            .catalog
            .drop_namespace(&fixture.namespace)
            .unwrap_err()
            .kind(),
        ErrorKind::PreconditionFailed
    );
}

#[test]
fn failed_requirements_leave_catalog_pointer_and_metadata_directory_unchanged() {
    let fixture = Fixture::new();
    let table = fixture.table("events");
    let directory = Path::new(table.metadata_location_result().unwrap())
        .parent()
        .unwrap();
    let count = std::fs::read_dir(directory).unwrap().count();
    let commit = TableCommit::builder()
        .ident(table.identifier().clone())
        .file_io(table.file_io().clone())
        .requirements(vec![TableRequirement::UuidMatch {
            uuid: Uuid::new_v4(),
        }])
        .updates(vec![])
        .build();
    assert!(fixture.catalog.update_table(commit).is_err());
    assert_eq!(std::fs::read_dir(directory).unwrap().count(), count);
    assert_eq!(
        fixture
            .catalog
            .load_table(table.identifier())
            .unwrap()
            .metadata_location(),
        table.metadata_location()
    );
}
