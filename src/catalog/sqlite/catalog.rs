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
use std::sync::{Arc, Mutex, MutexGuard};

use rusqlite::{Connection, OptionalExtension, Result as SqliteResult, ffi, params};

use crate::catalog::MetadataLocation;
use crate::catalog::utils::drop_table_data;
use crate::encryption::kms::KeyManagementClient;
use crate::io::FileIO;
use crate::spec::{TableMetadata, TableMetadataBuilder};
use crate::table::Table;
use crate::transaction::StagedTableCreate;
use crate::{
    Catalog, Error, ErrorKind, Namespace, NamespaceIdent, Result, TableCommit,
    TableCreation, TableIdent,
};

use super::PreparedSqliteCommit;

/// SQLite-backed Iceberg SQL catalog. SQL schema and pointer CAS follow
/// iceberg-rust's SQL catalog; file I/O is supplied by the embedding application.
/// Clones share the same connection and configuration without reopening the database.
#[derive(Clone, Debug)]
pub struct SqliteCatalog {
    inner: Arc<SqliteCatalogInner>,
}

#[derive(Debug)]
struct SqliteCatalogInner {
    name: String,
    warehouse: String,
    connection: Mutex<Connection>,
    file_io: FileIO,
    kms_client: Option<Arc<dyn KeyManagementClient>>,
}

impl SqliteCatalog {
    pub(super) fn new(
        name: String,
        warehouse: String,
        connection: Connection,
        file_io: FileIO,
        kms_client: Option<Arc<dyn KeyManagementClient>>,
    ) -> Result<Self> {
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS iceberg_tables (
                catalog_name VARCHAR(255) NOT NULL,
                table_namespace VARCHAR(255) NOT NULL,
                table_name VARCHAR(255) NOT NULL,
                metadata_location VARCHAR(1000),
                previous_metadata_location VARCHAR(1000),
                iceberg_type VARCHAR(5),
                PRIMARY KEY (catalog_name, table_namespace, table_name));
             CREATE TABLE IF NOT EXISTS iceberg_namespace_properties (
                catalog_name VARCHAR(255) NOT NULL,
                namespace VARCHAR(255) NOT NULL,
                property_key VARCHAR(255),
                property_value VARCHAR(1000),
                PRIMARY KEY (catalog_name, namespace, property_key));",
        )?;
        // V1 only: prepare detects an older schema without silently migrating it.
        connection.prepare("SELECT iceberg_type FROM iceberg_tables LIMIT 0")?;
        Ok(Self {
            inner: Arc::new(SqliteCatalogInner {
                name,
                warehouse,
                connection: Mutex::new(connection),
                file_io,
                kms_client,
            }),
        })
    }

    pub(super) fn name(&self) -> &str {
        &self.inner.name
    }

    pub(super) fn connection(&self) -> Result<MutexGuard<'_, Connection>> {
        self.inner.connection.lock().map_err(|_| {
            Error::new(
                ErrorKind::Unexpected,
                "SQLite catalog connection lock is poisoned",
            )
        })
    }

    pub(super) fn namespace_key(namespace: &NamespaceIdent) -> Result<String> {
        // SQL catalog's dot-separated representation cannot distinguish an
        // embedded dot from a namespace level. Reject it at this boundary.
        if namespace
            .iter()
            .any(|part| part.is_empty() || part.contains('.'))
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "SQLite namespace components must be nonempty and must not contain dots",
            ));
        }
        Ok(namespace.join("."))
    }

    fn namespace_exists_on(
        &self,
        connection: &Connection,
        namespace: &str,
    ) -> Result<bool> {
        Ok(connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM iceberg_tables WHERE catalog_name = ?1 AND table_namespace = ?2)
                 OR EXISTS(SELECT 1 FROM iceberg_namespace_properties WHERE catalog_name = ?1 AND namespace = ?2)",
            params![self.inner.name, namespace], |row| row.get(0),
        )?)
    }

    pub(super) fn require_namespace(
        &self,
        connection: &Connection,
        namespace: &str,
    ) -> Result<()> {
        if !self.namespace_exists_on(connection, namespace)? {
            return Err(Error::new(
                ErrorKind::NamespaceNotFound,
                format!("No such namespace: {namespace}"),
            ));
        }
        Ok(())
    }

    fn build_table(
        &self,
        identifier: TableIdent,
        location: String,
        metadata: TableMetadata,
    ) -> Result<Table> {
        let mut builder = Table::builder()
            .identifier(identifier)
            .metadata_location(location)
            .metadata(metadata)
            .file_io(self.inner.file_io.clone());
        if let Some(client) = &self.inner.kms_client {
            builder = builder.kms_client(Arc::clone(client));
        }
        builder.build()
    }

    fn build_staged_table(
        &self,
        identifier: TableIdent,
        metadata: TableMetadata,
    ) -> Result<Table> {
        let mut builder = Table::builder()
            .identifier(identifier)
            .metadata(metadata)
            .file_io(self.inner.file_io.clone());
        if let Some(client) = &self.inner.kms_client {
            builder = builder.kms_client(Arc::clone(client));
        }
        builder.build()
    }

    fn prepare_table_creation(
        &self,
        namespace: &NamespaceIdent,
        mut creation: TableCreation,
    ) -> Result<(TableIdent, TableMetadata)> {
        let namespace_info = self.get_namespace(namespace)?;
        let identifier = TableIdent::new(namespace.clone(), creation.name.clone());
        self.ensure_table_absent(&identifier)?;
        if creation.location.is_none() {
            let base = match namespace_info.properties().get("location") {
                Some(location) => location.clone(),
                None => {
                    if self.inner.warehouse.is_empty() {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            "Table creation requires a location or warehouse",
                        ));
                    }
                    format!("{}/{}", self.inner.warehouse, namespace.join("/"))
                }
            };
            creation.location = Some(format!("{base}/{}", identifier.name()));
        }
        let metadata = TableMetadataBuilder::from_table_creation(creation)?
            .build()?
            .metadata;
        Ok((identifier, metadata))
    }

    /// Initializes a table without inserting its catalog pointer.
    ///
    /// # Errors
    ///
    /// Returns namespace, duplicate-table, location, or metadata errors.
    pub fn stage_create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<StagedTableCreate> {
        let (identifier, metadata) =
            self.prepare_table_creation(namespace, creation)?;
        let table = self.build_staged_table(identifier, metadata)?;
        Ok(StagedTableCreate::new(table))
    }

    pub(super) fn insert_table(
        &self,
        connection: &Connection,
        namespace: &str,
        identifier: &TableIdent,
        location: &str,
    ) -> Result<()> {
        connection
            .execute(
                "INSERT INTO iceberg_tables (catalog_name, table_namespace, table_name, metadata_location, iceberg_type)
                 VALUES (?1, ?2, ?3, ?4, 'TABLE')",
                params![self.inner.name, namespace, identifier.name(), location],
            )
            .map_err(|error| {
                if error.sqlite_extended_error_code()
                    == Some(ffi::SQLITE_CONSTRAINT_PRIMARYKEY)
                {
                    Error::new(
                        ErrorKind::TableAlreadyExists,
                        format!("Table {identifier} already exists"),
                    )
                    .with_source(error)
                } else {
                    error.into()
                }
            })?;
        Ok(())
    }

    fn table_exists_on(
        &self,
        connection: &Connection,
        namespace: &str,
        name: &str,
    ) -> Result<bool> {
        Ok(connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM iceberg_tables WHERE catalog_name = ?1
             AND table_namespace = ?2 AND table_name = ?3 AND (iceberg_type = 'TABLE' OR iceberg_type IS NULL))",
            params![self.inner.name, namespace, name], |row| row.get(0),
        )?)
    }

    fn ensure_table_absent(&self, identifier: &TableIdent) -> Result<()> {
        if self.table_exists(identifier)? {
            return Err(Error::new(
                ErrorKind::TableAlreadyExists,
                format!("Table {identifier} already exists"),
            ));
        }
        Ok(())
    }
}

impl Catalog for SqliteCatalog {
    fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        // Known limitation inherited from iceberg-rust's SQL catalog: with
        // only a.b stored, top-level listing returns a, but namespace_exists(a)
        // is false and listing children of a returns NamespaceNotFound. Dropping
        // an explicit a while retaining a.b produces the same state. Direct
        // list_tables(a.b) still works without traversing parent namespaces.
        // TODO: retain this upstream behavior pending an upstream fix, then
        // align namespace discovery, existence and deletion together rather
        // than changing the listing semantics in isolation. Reference:
        // https://github.com/apache/iceberg-rust/blob/main/crates/catalog/sql/src/catalog.rs
        let parent_key = parent.map(Self::namespace_key).transpose()?;
        let connection = self.connection()?;
        if let Some(key) = &parent_key {
            self.require_namespace(&connection, key)?;
        }
        let mut statement = connection.prepare(
            "SELECT table_namespace FROM iceberg_tables WHERE catalog_name = ?1
             UNION SELECT namespace FROM iceberg_namespace_properties WHERE catalog_name = ?1",
        )?;
        let rows =
            statement.query_map([&self.inner.name], |row| row.get::<_, String>(0))?;
        let mut namespaces = HashSet::new();
        for row in rows {
            let namespace = row?;
            if let Some(parent) = &parent_key {
                // Preserve upstream SQL catalog's stored descendant names.
                // Requiring the separator excludes unrelated prefixes (a/ab).
                if namespace
                    .strip_prefix(parent.as_str())
                    .is_some_and(|suffix| suffix.starts_with('.'))
                {
                    namespaces
                        .insert(NamespaceIdent::from_strs(namespace.split('.'))?);
                }
            } else {
                let first = namespace
                    .split_once('.')
                    .map_or(namespace.as_str(), |(first, _)| first);
                namespaces.insert(NamespaceIdent::new(first.to_owned()));
            }
        }
        Ok(namespaces.into_iter().collect())
    }

    fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        mut properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        let key = Self::namespace_key(namespace)?;
        let mut connection = self.connection()?;
        let tx = connection.transaction()?;
        if self.namespace_exists_on(&tx, &key)? {
            return Err(Error::new(
                ErrorKind::NamespaceAlreadyExists,
                format!("Namespace {namespace:?} already exists"),
            ));
        }
        let had_properties = !properties.is_empty();
        properties.insert("exists".to_owned(), "true".to_owned());
        {
            let mut insert = tx.prepare(
                "INSERT INTO iceberg_namespace_properties (catalog_name, namespace, property_key, property_value)
                 VALUES (?1, ?2, ?3, ?4)",
            )?;
            for (name, value) in &properties {
                insert.execute(params![self.inner.name, key, name, value])?;
            }
        }
        tx.commit()?;
        if !had_properties {
            properties.clear();
        }
        Ok(Namespace::with_properties(namespace.clone(), properties))
    }

    fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
        let key = Self::namespace_key(namespace)?;
        let connection = self.connection()?;
        self.require_namespace(&connection, &key)?;
        let mut statement = connection.prepare(
            "SELECT property_key, property_value FROM iceberg_namespace_properties
             WHERE catalog_name = ?1 AND namespace = ?2",
        )?;
        let properties = statement
            .query_map(params![self.inner.name, key], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<SqliteResult<HashMap<_, _>>>()?;
        Ok(Namespace::with_properties(namespace.clone(), properties))
    }

    fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool> {
        let key = Self::namespace_key(namespace)?;
        let connection = self.connection()?;
        self.namespace_exists_on(&connection, &key)
    }

    fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        mut properties: HashMap<String, String>,
    ) -> Result<()> {
        let key = Self::namespace_key(namespace)?;
        let mut connection = self.connection()?;
        let tx = connection.transaction()?;
        self.require_namespace(&tx, &key)?;
        tx.execute("DELETE FROM iceberg_namespace_properties WHERE catalog_name = ?1 AND namespace = ?2",
            params![self.inner.name, key])?;
        // Catalog::update_namespace accepts the complete property set. Keep
        // upstream's existence marker when the user removes all properties.
        properties.insert("exists".to_owned(), "true".to_owned());
        {
            let mut insert = tx.prepare(
                "INSERT INTO iceberg_namespace_properties (catalog_name, namespace, property_key, property_value)
                 VALUES (?1, ?2, ?3, ?4)",
            )?;
            for (name, value) in &properties {
                insert.execute(params![self.inner.name, key, name, value])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    fn drop_namespace(&self, namespace: &NamespaceIdent) -> Result<()> {
        let key = Self::namespace_key(namespace)?;
        let mut connection = self.connection()?;
        let tx = connection.transaction()?;
        self.require_namespace(&tx, &key)?;
        let has_tables: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM iceberg_tables WHERE catalog_name = ?1 AND table_namespace = ?2)",
            params![self.inner.name, key], |row| row.get(0),
        )?;
        if has_tables {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                format!("Namespace {namespace:?} is not empty"),
            ));
        }
        tx.execute("DELETE FROM iceberg_namespace_properties WHERE catalog_name = ?1 AND namespace = ?2",
            params![self.inner.name, key])?;
        tx.commit()?;
        Ok(())
    }

    fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        let key = Self::namespace_key(namespace)?;
        let connection = self.connection()?;
        self.require_namespace(&connection, &key)?;
        let mut statement = connection.prepare(
            "SELECT table_name FROM iceberg_tables WHERE catalog_name = ?1 AND table_namespace = ?2
             AND (iceberg_type = 'TABLE' OR iceberg_type IS NULL)",
        )?;
        let names = statement.query_map(params![self.inner.name, key], |row| {
            row.get::<_, String>(0)
        })?;
        names
            .map(|name| Ok(TableIdent::new(namespace.clone(), name?)))
            .collect()
    }

    fn table_exists(&self, identifier: &TableIdent) -> Result<bool> {
        let namespace = Self::namespace_key(identifier.namespace())?;
        let connection = self.connection()?;
        self.table_exists_on(&connection, &namespace, identifier.name())
    }

    fn load_table(&self, identifier: &TableIdent) -> Result<Table> {
        let namespace = Self::namespace_key(identifier.namespace())?;
        let location: Option<String> = self.connection()?.query_row(
            "SELECT metadata_location FROM iceberg_tables WHERE catalog_name = ?1 AND table_namespace = ?2
             AND table_name = ?3 AND (iceberg_type = 'TABLE' OR iceberg_type IS NULL)",
            params![self.inner.name, namespace, identifier.name()], |row| row.get(0),
        ).optional()?;
        let location = location.ok_or_else(|| {
            Error::new(
                ErrorKind::TableNotFound,
                format!("No such table: {identifier}"),
            )
        })?;
        // The connection guard is gone before any embedding application's I/O.
        let metadata = TableMetadata::read_from(&self.inner.file_io, &location)?;
        self.build_table(identifier.clone(), location, metadata)
    }

    fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        let (identifier, metadata) =
            self.prepare_table_creation(namespace, creation)?;
        let location =
            MetadataLocation::try_new_with_metadata(&metadata)?.to_string();
        metadata.write_to(&self.inner.file_io, &location)?;
        // TODO(upstream): Retain the SQL catalog's insert-before-build ordering
        // pending an upstream fix. For a V3 table with encryption.key-id but no
        // KMS client, build_table returns PreconditionFailed after the INSERT
        // commits. The catalog entry remains despite the API returning an error;
        // loading with the same configuration fails, and retrying creation or
        // registration returns TableAlreadyExists. This also affects
        // register_table below. Upstream main still has this ordering as checked
        // on 2026-10-14; no matching fix PR was found in the upstream search.
        // Reference implementation:
        // https://github.com/apache/iceberg-rust/blob/bb1e4a4861f02377489eff818b75138f414c4cb0/crates/catalog/sql/src/catalog.rs#L1069-L1168
        let namespace = Self::namespace_key(identifier.namespace())?;
        {
            let connection = self.connection()?;
            self.insert_table(&connection, &namespace, &identifier, &location)?;
        }
        self.build_table(identifier, location, metadata)
    }

    fn register_table(
        &self,
        identifier: &TableIdent,
        metadata_location: String,
    ) -> Result<Table> {
        self.ensure_table_absent(identifier)?;
        let metadata =
            TableMetadata::read_from(&self.inner.file_io, &metadata_location)?;
        // Same known insert-before-build limitation as create_table above;
        // retain this ordering pending the upstream fix described there.
        let namespace = Self::namespace_key(identifier.namespace())?;
        {
            let connection = self.connection()?;
            self.insert_table(
                &connection,
                &namespace,
                identifier,
                &metadata_location,
            )?;
        }
        self.build_table(identifier.clone(), metadata_location, metadata)
    }

    fn drop_table(&self, identifier: &TableIdent) -> Result<()> {
        let namespace = Self::namespace_key(identifier.namespace())?;
        let changed = self.connection()?.execute(
            "DELETE FROM iceberg_tables WHERE catalog_name = ?1 AND table_namespace = ?2
             AND table_name = ?3 AND (iceberg_type = 'TABLE' OR iceberg_type IS NULL)",
            params![self.inner.name, namespace, identifier.name()],
        )?;
        if changed == 0 {
            return Err(Error::new(
                ErrorKind::TableNotFound,
                format!("No such table: {identifier}"),
            ));
        }
        Ok(())
    }

    fn purge_table(&self, identifier: &TableIdent) -> Result<()> {
        let table = self.load_table(identifier)?;
        self.drop_table(identifier)?;
        drop_table_data(&table)
    }

    fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> Result<()> {
        if src == dest {
            return Ok(());
        }
        let source_namespace = Self::namespace_key(src.namespace())?;
        let dest_namespace = Self::namespace_key(dest.namespace())?;
        let mut connection = self.connection()?;
        let tx = connection.transaction()?;
        // Keep upstream's source/namespace/destination error precedence, and
        // keep the checks and rename in one SQLite transaction.
        if !self.table_exists_on(&tx, &source_namespace, src.name())? {
            return Err(Error::new(
                ErrorKind::TableNotFound,
                format!("No such table: {src}"),
            ));
        }
        self.require_namespace(&tx, &dest_namespace)?;
        if self.table_exists_on(&tx, &dest_namespace, dest.name())? {
            return Err(Error::new(
                ErrorKind::TableAlreadyExists,
                format!("Table {dest} already exists"),
            ));
        }
        tx.execute(
            "UPDATE iceberg_tables SET table_namespace = ?1, table_name = ?2
             WHERE catalog_name = ?3 AND table_namespace = ?4 AND table_name = ?5
             AND (iceberg_type = 'TABLE' OR iceberg_type IS NULL)",
            params![
                dest_namespace,
                dest.name(),
                self.inner.name,
                source_namespace,
                src.name()
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    fn update_table(&self, commit: TableCommit) -> Result<Table> {
        let (table, update) = self.prepare_update(commit)?;
        PreparedSqliteCommit::new(self.clone(), vec![update]).publish()?;
        Ok(table)
    }
}
