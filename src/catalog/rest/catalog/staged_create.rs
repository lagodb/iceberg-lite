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

//! REST staged-table creation.
//!
//! Fork synchronization note: this path parallels
//! `RestSessionCatalog::create_table`. Reconcile upstream changes to create
//! response handling, authentication, table configuration, credentials,
//! `FileIO`, KMS, and metadata locations here.

use http::{Method, StatusCode};

use crate::table::Table;
use crate::transaction::StagedTableCreate;
use crate::{
    Error, ErrorKind, NamespaceIdent, Result, SessionContext, TableCreation,
    TableIdent,
};

use super::super::client::{
    deserialize_catalog_response, deserialize_unexpected_catalog_error,
};
use super::super::endpoint::{V1_CREATE_TABLE, V1_UPDATE_TABLE};
use super::super::request::HttpRequest;
use super::super::types::{CreateTableRequest, LoadTableResult};
use super::RestSessionCatalog;

impl RestSessionCatalog {
    fn request_staged_table_create(
        &self,
        context: &SessionContext,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<(TableIdent, LoadTableResult)> {
        let client = self.client()?;
        client.require_endpoint(&V1_CREATE_TABLE)?;
        client.require_endpoint(&V1_UPDATE_TABLE)?;
        let table_ident = TableIdent::new(namespace.clone(), creation.name.clone());
        let request = HttpRequest::build(
            client
                .http_client
                .request(Method::POST, client.config.tables_endpoint(namespace))
                .json(&CreateTableRequest::from_table_creation(creation, true)),
        )?;
        let response = client.query_catalog(context, request)?;
        match response.status() {
            StatusCode::OK => {
                Ok((table_ident, deserialize_catalog_response(response)?))
            }
            StatusCode::NOT_FOUND => Err(Error::new(
                ErrorKind::NamespaceNotFound,
                "Tried to create a table under a namespace that does not exist",
            )),
            StatusCode::CONFLICT => Err(Error::new(
                ErrorKind::TableAlreadyExists,
                "The table already exists",
            )),
            _ => Err(deserialize_unexpected_catalog_error(
                response,
                client.http_client.disable_header_redaction(),
            )),
        }
    }

    pub(super) fn stage_create_table(
        &self,
        context: &SessionContext,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<StagedTableCreate> {
        let (identifier, response) =
            self.request_staged_table_create(context, namespace, creation)?;
        let LoadTableResult {
            metadata_location,
            metadata,
            config,
            storage_credentials,
        } = response;
        let storage_location = metadata_location
            .as_deref()
            .unwrap_or_else(|| metadata.location());
        let file_io = self.load_file_io(
            Some(storage_location),
            Some(config),
            storage_credentials.unwrap_or_default(),
        )?;
        let mut builder = Table::builder()
            .identifier(identifier)
            .file_io(file_io)
            .metadata(metadata);
        if let Some(location) = metadata_location {
            builder = builder.metadata_location(location);
        }
        if let Some(kms_client) = self.kms_client.clone() {
            builder = builder.kms_client(kms_client);
        }
        Ok(StagedTableCreate::new(builder.build()?))
    }
}
