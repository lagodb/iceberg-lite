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

use crate::Result;
use crate::io::FileIO;
use crate::spec::{Manifest, ManifestFile};

/// Reads manifest files through iceberg-lite's synchronous I/O layer.
///
/// Encryption, inherited manifest-entry fields, and V3 row-id inheritance are
/// handled transparently by the underlying manifest loader.
#[derive(Clone)]
pub struct ManifestReader {
    file_io: FileIO,
}

impl ManifestReader {
    pub(crate) fn new(file_io: FileIO) -> Self {
        Self { file_io }
    }

    /// Read, decrypt, and parse the referenced manifest.
    pub fn read(&self, manifest_file: &ManifestFile) -> Result<Manifest> {
        manifest_file.load_manifest(&self.file_io)
    }
}
