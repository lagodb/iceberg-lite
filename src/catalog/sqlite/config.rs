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
use std::sync::Arc;
use std::time::Duration;

use rusqlite::{Connection, OpenFlags};

use crate::encryption::kms::KmsClientFactory;
use crate::io::{FileIO, StorageConfig, StorageFactory};
use crate::{CatalogBuilder, Error, ErrorKind, Result};

use super::SqliteCatalog;

/// Builds a synchronous SQLite catalog with application-owned file storage.
#[derive(Debug, Default)]
pub struct SqliteCatalogBuilder {
    storage_factory: Option<Arc<dyn StorageFactory>>,
    kms_client_factory: Option<Arc<dyn KmsClientFactory>>,
}

impl SqliteCatalogBuilder {
    /// Supplies the storage used for metadata, manifests and data files.
    pub fn with_storage_factory(mut self, factory: Arc<dyn StorageFactory>) -> Self {
        self.storage_factory = Some(factory);
        self
    }

    /// Resolves the supported SQLite URI to an absolute database filename.
    ///
    /// This catalog accepts `sqlite:/absolute/path` and
    /// `sqlite:///absolute/path`. In-memory databases and URI query options
    /// are deliberately excluded: foreign bindings must reopen the same database.
    pub fn database_path(uri: &str) -> Result<&Path> {
        let path = uri.strip_prefix("sqlite:").ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "SQLite catalog URI must start with sqlite:",
            )
        })?;
        let path = path.strip_prefix("//").unwrap_or(path);
        if !Path::new(path).is_absolute() || path.contains(['?', '#', '\0']) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "SQLite catalog URI must name an absolute file without query or fragment",
            ));
        }
        Ok(Path::new(path))
    }
}

impl CatalogBuilder for SqliteCatalogBuilder {
    type C = SqliteCatalog;

    fn with_kms_client_factory(mut self, factory: Arc<dyn KmsClientFactory>) -> Self {
        self.kms_client_factory = Some(factory);
        self
    }

    fn load(
        self,
        name: impl Into<String>,
        mut props: HashMap<String, String>,
    ) -> Result<Self::C> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Catalog name cannot be empty",
            ));
        }
        let uri = props.remove("uri").ok_or_else(|| {
            Error::new(ErrorKind::DataInvalid, "SQLite catalog uri is required")
        })?;
        let path = Self::database_path(&uri)?;
        let warehouse = props.get("warehouse").cloned().unwrap_or_default();
        let factory = self.storage_factory.ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "SQLite catalog requires a StorageFactory",
            )
        })?;
        let kms_client = self
            .kms_client_factory
            .map(|factory| factory.create_kms_client(&props))
            .transpose()?;
        let file_io = FileIO::new(factory.build(StorageConfig::new(
            warehouse.clone(),
            props,
            Vec::new(),
        ))?);
        // Like upstream SQLx's connect path, opening a catalog does not create
        // a missing database. Bootstrap the SQLite file explicitly first.
        let connection =
            Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
        // Never sleep inside SQLite's C busy callback on a PG backend thread.
        connection.busy_timeout(Duration::ZERO)?;
        connection.pragma_update(None, "synchronous", "EXTRA")?;
        let journal: String =
            connection.pragma_query_value(None, "journal_mode", |row| row.get(0))?;
        if journal.eq_ignore_ascii_case("off")
            || journal.eq_ignore_ascii_case("memory")
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "SQLite catalog requires a durable journal",
            ));
        }
        SqliteCatalog::new(name, warehouse, connection, file_io, kms_client)
    }
}
