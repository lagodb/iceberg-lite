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
use std::fmt::Display;
use std::str::FromStr;

use crate::compression::CompressionCodec;
use crate::spec::NameMapping;
use crate::util::location::strip_trailing_slash;
use crate::{Error, ErrorKind, Result};

/// Isolation level governing Iceberg row-delta conflict validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    /// Validate concurrent appended data files that may match the row-delta conflict filter.
    Serializable,
    /// Allow concurrent appends, while still validating target-file and delete-file conflicts.
    Snapshot,
}

impl IsolationLevel {
    /// Canonical Iceberg table-property value for this isolation level.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Serializable => "serializable",
            Self::Snapshot => "snapshot",
        }
    }
}

impl FromStr for IsolationLevel {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        if value.eq_ignore_ascii_case(Self::Serializable.as_str()) {
            Ok(Self::Serializable)
        } else if value.eq_ignore_ascii_case(Self::Snapshot.as_str()) {
            Ok(Self::Snapshot)
        } else {
            Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Invalid isolation level: {value}. Valid values are 'serializable' and 'snapshot'",
                ),
            ))
        }
    }
}

impl Display for IsolationLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// Helper function to parse a property from a HashMap
// If the property is not found, use the default value
fn parse_property<T: FromStr>(
    properties: &HashMap<String, String>,
    key: &str,
    default: T,
) -> Result<T>
where
    <T as FromStr>::Err: Display,
{
    properties.get(key).map_or(Ok(default), |value| {
        value.parse::<T>().map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Invalid value for {key}: {e}"),
            )
        })
    })
}

/// Parse an optional property, returning `None` when the key is absent and an
/// error when the value is present but fails to parse.
fn parse_optional_property<T: FromStr>(
    properties: &HashMap<String, String>,
    key: &str,
) -> Result<Option<T>>
where
    <T as FromStr>::Err: Display,
{
    properties
        .get(key)
        .map(|value| {
            value.parse::<T>().map_err(|e| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Invalid value for {key}: {e}"),
                )
            })
        })
        .transpose()
}

/// Parse compression codec for metadata files from table properties.
fn parse_metadata_file_compression(
    properties: &HashMap<String, String>,
) -> Result<CompressionCodec> {
    let value = properties
        .get(TableProperties::PROPERTY_METADATA_COMPRESSION_CODEC)
        .map(String::as_str)
        .unwrap_or(TableProperties::PROPERTY_METADATA_COMPRESSION_CODEC_DEFAULT);

    if value.is_empty() {
        return Ok(CompressionCodec::None);
    }

    let codec: CompressionCodec = serde_json::from_value(
        serde_json::Value::String(value.to_lowercase()),
    )
    .map_err(|_| {
        Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Invalid metadata compression codec: {value}. Only '{}' and '{}' are supported.",
                CompressionCodec::None.name(),
                CompressionCodec::gzip_default().name()
            ),
        )
    })?;

    match codec {
        CompressionCodec::None | CompressionCodec::Gzip(_) => Ok(codec),
        _ => Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Invalid metadata compression codec: {value}. Only '{}' and '{}' are supported for metadata files.",
                CompressionCodec::None.name(),
                CompressionCodec::gzip_default().name()
            ),
        )),
    }
}

/// Parse the Parquet data-file compression codec (`write.parquet.compression-codec`)
/// and fold in the compression level (`write.parquet.compression-level`) for the
/// codecs that accept one (`zstd`, `gzip`, `brotli`).
fn parse_parquet_compression(
    properties: &HashMap<String, String>,
) -> Result<CompressionCodec> {
    let value = properties
        .get(TableProperties::PROPERTY_PARQUET_COMPRESSION_CODEC)
        .map(|s| s.as_str())
        .unwrap_or(TableProperties::PROPERTY_PARQUET_COMPRESSION_CODEC_DEFAULT);

    let codec: CompressionCodec = serde_json::from_value(serde_json::Value::String(
        value.to_lowercase(),
    ))
    .map_err(|_| {
        Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Invalid Parquet compression codec: {value}. Supported codecs: \
                     uncompressed, snappy, gzip, lzo, brotli, lz4, lz4_raw, zstd"
            ),
        )
    })?;

    let level: Option<u8> = parse_optional_property(
        properties,
        TableProperties::PROPERTY_PARQUET_COMPRESSION_LEVEL,
    )?;

    Ok(match (codec, level) {
        (CompressionCodec::Zstd(_), Some(level)) => CompressionCodec::Zstd(level),
        (CompressionCodec::Gzip(_), Some(level)) => CompressionCodec::Gzip(level),
        (CompressionCodec::Brotli(_), Some(level)) => CompressionCodec::Brotli(level),
        (codec, _) => codec,
    })
}

/// Parse boolean property case insensitively
/// Rust standard library only accepts "true" and "false", see https://doc.rust-lang.org/std/primitive.bool.html#method.from_str
/// Users might accidentally trigger fallback with valid configuration values such as "False" or "True"
fn parse_property_bool(
    properties: &HashMap<String, String>,
    key: &str,
    default: bool,
) -> Result<bool> {
    properties.get(key).map_or(Ok(default), |value| {
        value.to_lowercase().parse::<bool>().map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Invalid value for {key}: {e}"),
            )
        })
    })
}

fn parse_location_property(
    properties: &HashMap<String, String>,
    key: &str,
) -> Result<Option<String>> {
    properties
        .get(key)
        .map(|path| {
            if path.is_empty() {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!("Invalid value for {key}: path must not be empty"),
                ));
            }
            Ok(strip_trailing_slash(path).to_string())
        })
        .transpose()
}

/// A borrowed, lazily parsed view of a table's raw properties.
///
/// Each getter parses only the property it consumes. An invalid unrelated
/// property therefore does not prevent other table operations from proceeding.
#[derive(Debug, Clone, Copy)]
pub struct TableProperties<'a> {
    properties: &'a HashMap<String, String>,
}

impl<'a> TableProperties<'a> {
    /// Creates a lazy view over raw table properties.
    pub const fn new(properties: &'a HashMap<String, String>) -> Self {
        Self { properties }
    }

    /// Reserved table property for table format version.
    ///
    /// Iceberg will default a new table's format version to the latest stable and recommended
    /// version. This reserved property keyword allows users to override the Iceberg format version of
    /// the table metadata.
    ///
    /// If this table property exists when creating a table, the table will use the specified format
    /// version. If a table updates this property, it will try to upgrade to the specified format
    /// version.
    pub const PROPERTY_FORMAT_VERSION: &'static str = "format-version";
    /// Reserved table property for table UUID.
    pub const PROPERTY_UUID: &'static str = "uuid";
    /// Reserved table property for the total number of snapshots.
    pub const PROPERTY_SNAPSHOT_COUNT: &'static str = "snapshot-count";
    /// Reserved table property for current snapshot summary.
    pub const PROPERTY_CURRENT_SNAPSHOT_SUMMARY: &'static str =
        "current-snapshot-summary";
    /// Reserved table property for current snapshot id.
    pub const PROPERTY_CURRENT_SNAPSHOT_ID: &'static str = "current-snapshot-id";
    /// Reserved table property for current snapshot timestamp.
    pub const PROPERTY_CURRENT_SNAPSHOT_TIMESTAMP: &'static str =
        "current-snapshot-timestamp-ms";
    /// Reserved table property for the JSON representation of current schema.
    pub const PROPERTY_CURRENT_SCHEMA: &'static str = "current-schema";
    /// Reserved table property for the JSON representation of current(default) partition spec.
    pub const PROPERTY_DEFAULT_PARTITION_SPEC: &'static str =
        "default-partition-spec";
    /// Reserved table property for the JSON representation of current(default) sort order.
    pub const PROPERTY_DEFAULT_SORT_ORDER: &'static str = "default-sort-order";

    /// Property key for max number of previous versions to keep.
    pub const PROPERTY_METADATA_PREVIOUS_VERSIONS_MAX: &'static str =
        "write.metadata.previous-versions-max";
    /// Default value for max number of previous versions to keep.
    pub const PROPERTY_METADATA_PREVIOUS_VERSIONS_MAX_DEFAULT: usize = 100;
    pub const PROPERTY_METADATA_DELETE_AFTER_COMMIT_ENABLED: &'static str =
        "write.metadata.delete-after-commit.enabled";
    pub const PROPERTY_METADATA_DELETE_AFTER_COMMIT_ENABLED_DEFAULT: bool = false;

    /// Property key for max number of partitions to keep summary stats for.
    pub const PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT: &'static str =
        "write.summary.partition-limit";
    /// Default value for the max number of partitions to keep summary stats for.
    pub const PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT_DEFAULT: u64 = 0;

    /// Reserved Iceberg table properties list.
    ///
    /// Reserved table properties are only used to control behaviors when creating or updating a
    /// table. The value of these properties are not persisted as a part of the table metadata.
    pub const RESERVED_PROPERTIES: [&'static str; 9] = [
        Self::PROPERTY_FORMAT_VERSION,
        Self::PROPERTY_UUID,
        Self::PROPERTY_SNAPSHOT_COUNT,
        Self::PROPERTY_CURRENT_SNAPSHOT_ID,
        Self::PROPERTY_CURRENT_SNAPSHOT_SUMMARY,
        Self::PROPERTY_CURRENT_SNAPSHOT_TIMESTAMP,
        Self::PROPERTY_CURRENT_SCHEMA,
        Self::PROPERTY_DEFAULT_PARTITION_SPEC,
        Self::PROPERTY_DEFAULT_SORT_ORDER,
    ];

    /// Property key for number of commit retries.
    pub const PROPERTY_COMMIT_NUM_RETRIES: &'static str = "commit.retry.num-retries";
    /// Default value for number of commit retries.
    pub const PROPERTY_COMMIT_NUM_RETRIES_DEFAULT: usize = 4;

    /// Property key for minimum wait time (ms) between retries.
    pub const PROPERTY_COMMIT_MIN_RETRY_WAIT_MS: &'static str =
        "commit.retry.min-wait-ms";
    /// Default value for minimum wait time (ms) between retries.
    pub const PROPERTY_COMMIT_MIN_RETRY_WAIT_MS_DEFAULT: u64 = 100;

    /// Property key for maximum wait time (ms) between retries.
    pub const PROPERTY_COMMIT_MAX_RETRY_WAIT_MS: &'static str =
        "commit.retry.max-wait-ms";
    /// Default value for maximum wait time (ms) between retries.
    pub const PROPERTY_COMMIT_MAX_RETRY_WAIT_MS_DEFAULT: u64 = 60 * 1000; // 1 minute

    /// Property key for total maximum retry time (ms).
    pub const PROPERTY_COMMIT_TOTAL_RETRY_TIME_MS: &'static str =
        "commit.retry.total-timeout-ms";
    /// Default value for total maximum retry time (ms).
    pub const PROPERTY_COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT: u64 = 30 * 60 * 1000; // 30 minutes

    /// Default file format for data files
    pub const PROPERTY_DEFAULT_FILE_FORMAT: &'static str = "write.format.default";
    /// Default file format for delete files
    pub const PROPERTY_DELETE_DEFAULT_FILE_FORMAT: &'static str =
        "write.delete.format.default";
    /// Default value for data file format
    pub const PROPERTY_DEFAULT_FILE_FORMAT_DEFAULT: &'static str = "parquet";

    /// Property key for delete command isolation level.
    pub const PROPERTY_WRITE_DELETE_ISOLATION_LEVEL: &'static str =
        "write.delete.isolation-level";
    /// Property key for update command isolation level.
    pub const PROPERTY_WRITE_UPDATE_ISOLATION_LEVEL: &'static str =
        "write.update.isolation-level";
    /// Property key for merge command isolation level.
    pub const PROPERTY_WRITE_MERGE_ISOLATION_LEVEL: &'static str =
        "write.merge.isolation-level";
    /// Default write isolation level.
    pub const PROPERTY_WRITE_ISOLATION_LEVEL_DEFAULT: IsolationLevel =
        IsolationLevel::Serializable;

    /// Target file size for newly written files.
    pub const PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES: &'static str =
        "write.target-file-size-bytes";
    /// Default target file size
    pub const PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT: usize =
        512 * 1024 * 1024; // 512 MB
    pub const PROPERTY_WRITE_METADATA_PATH: &'static str = "write.metadata.path";
    /// Minimum live manifest count before automatic merging is considered.
    pub const PROPERTY_MANIFEST_MIN_COUNT_TO_MERGE: &'static str =
        "commit.manifest.min-count-to-merge";
    pub const PROPERTY_MANIFEST_MIN_COUNT_TO_MERGE_DEFAULT: usize = 100;
    /// Target size for rewritten manifest files.
    pub const PROPERTY_MANIFEST_TARGET_SIZE_BYTES: &'static str =
        "commit.manifest.target-size-bytes";
    pub const PROPERTY_MANIFEST_TARGET_SIZE_BYTES_DEFAULT: u64 = 8 * 1024 * 1024;
    pub const PROPERTY_MANIFEST_MERGE_ENABLED: &'static str =
        "commit.manifest-merge.enabled";
    pub const PROPERTY_MANIFEST_MERGE_ENABLED_DEFAULT: bool = true;
    /// Compression codec for metadata files.
    pub const PROPERTY_METADATA_COMPRESSION_CODEC: &'static str =
        "write.metadata.compression-codec";
    /// Default metadata compression codec.
    pub const PROPERTY_METADATA_COMPRESSION_CODEC_DEFAULT: &'static str = "none";
    /// Whether to use `FanoutWriter` for partitioned tables.
    pub const PROPERTY_DATAFUSION_WRITE_FANOUT_ENABLED: &'static str =
        "write.datafusion.fanout.enabled";
    /// Default value for fanout writer enabled.
    pub const PROPERTY_DATAFUSION_WRITE_FANOUT_ENABLED_DEFAULT: bool = true;

    /// Enable content-defined chunking with parquet defaults or per-property overrides.
    pub const PROPERTY_PARQUET_CDC_ENABLED: &'static str =
        "write.parquet.content-defined-chunking.enabled";
    /// Default value for content-defined chunking enabled.
    pub const PROPERTY_PARQUET_CDC_ENABLED_DEFAULT: bool = false;
    /// Minimum chunk size in bytes for content-defined chunking.
    pub const PROPERTY_PARQUET_CDC_MIN_CHUNK_SIZE: &'static str =
        "write.parquet.content-defined-chunking.min-chunk-size";
    /// Default matches `parquet::file::properties::DEFAULT_CDC_MIN_CHUNK_SIZE`.
    pub const PROPERTY_PARQUET_CDC_MIN_CHUNK_SIZE_DEFAULT: usize = 256 * 1024;
    /// Maximum chunk size in bytes for content-defined chunking.
    pub const PROPERTY_PARQUET_CDC_MAX_CHUNK_SIZE: &'static str =
        "write.parquet.content-defined-chunking.max-chunk-size";
    /// Default matches `parquet::file::properties::DEFAULT_CDC_MAX_CHUNK_SIZE`.
    pub const PROPERTY_PARQUET_CDC_MAX_CHUNK_SIZE_DEFAULT: usize = 1024 * 1024;
    /// Normalization level for content-defined chunking.
    pub const PROPERTY_PARQUET_CDC_NORM_LEVEL: &'static str =
        "write.parquet.content-defined-chunking.norm-level";
    /// Default matches `parquet::file::properties::DEFAULT_CDC_NORM_LEVEL`.
    pub const PROPERTY_PARQUET_CDC_NORM_LEVEL_DEFAULT: i32 = 0;

    pub const PROPERTY_PARQUET_COMPRESSION_CODEC: &'static str =
        "write.parquet.compression-codec";
    pub const PROPERTY_PARQUET_COMPRESSION_CODEC_DEFAULT: &'static str = "zstd";
    pub const PROPERTY_PARQUET_COMPRESSION_LEVEL: &'static str =
        "write.parquet.compression-level";
    pub const PROPERTY_PARQUET_ROW_GROUP_SIZE_BYTES: &'static str =
        "write.parquet.row-group-size-bytes";
    pub const PROPERTY_PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT: usize =
        128 * 1024 * 1024;
    pub const PROPERTY_PARQUET_PAGE_SIZE_BYTES: &'static str =
        "write.parquet.page-size-bytes";
    pub const PROPERTY_PARQUET_PAGE_SIZE_BYTES_DEFAULT: usize = 1024 * 1024;
    pub const PROPERTY_PARQUET_PAGE_ROW_LIMIT: &'static str =
        "write.parquet.page-row-limit";
    pub const PROPERTY_PARQUET_PAGE_ROW_LIMIT_DEFAULT: usize = 20_000;
    pub const PROPERTY_PARQUET_DICT_SIZE_BYTES: &'static str =
        "write.parquet.dict-size-bytes";
    pub const PROPERTY_PARQUET_DICT_SIZE_BYTES_DEFAULT: usize = 2 * 1024 * 1024;

    pub const PROPERTY_WRITE_DATA_LOCATION: &'static str = "write.data.path";
    pub const PROPERTY_WRITE_FOLDER_STORAGE_LOCATION: &'static str =
        "write.folder-storage.path";
    pub const PROPERTY_WRITE_OBJECT_STORAGE_LOCATION: &'static str =
        "write.object-storage.path";
    /// Property key for enabling the object storage location layout for new data files.
    pub const PROPERTY_WRITE_OBJECT_STORAGE_ENABLED: &'static str =
        "write.object-storage.enabled";
    /// Default value for [`TableProperties::PROPERTY_WRITE_OBJECT_STORAGE_ENABLED`].
    pub const PROPERTY_WRITE_OBJECT_STORAGE_ENABLED_DEFAULT: bool = false;
    pub const PROPERTY_WRITE_OBJECT_STORAGE_PARTITIONED_PATHS: &'static str =
        "write.object-storage.partitioned-paths";
    pub const PROPERTY_WRITE_OBJECT_STORAGE_PARTITIONED_PATHS_DEFAULT: bool = true;

    /// Property key for enabling garbage collection.
    /// Defaults to `true`.
    pub const PROPERTY_GC_ENABLED: &'static str = "gc.enabled";
    /// Default value for gc.enabled.
    pub const PROPERTY_GC_ENABLED_DEFAULT: bool = true;

    /// Property key for the default maximum age of a snapshot to keep when expiring snapshots.
    pub const PROPERTY_MAX_SNAPSHOT_AGE_MS: &'static str =
        "history.expire.max-snapshot-age-ms";
    /// Default value for history.expire.max-snapshot-age-ms (5 days).
    pub const PROPERTY_MAX_SNAPSHOT_AGE_MS_DEFAULT: i64 = 5 * 24 * 60 * 60 * 1000;
    /// Property key for the default minimum number of snapshots to keep when expiring snapshots.
    pub const PROPERTY_MIN_SNAPSHOTS_TO_KEEP: &'static str =
        "history.expire.min-snapshots-to-keep";
    /// Default value for history.expire.min-snapshots-to-keep.
    pub const PROPERTY_MIN_SNAPSHOTS_TO_KEEP_DEFAULT: usize = 1;
    /// Property key for the default maximum age of a snapshot reference to keep when expiring.
    pub const PROPERTY_MAX_REF_AGE_MS: &'static str = "history.expire.max-ref-age-ms";
    /// Default value for history.expire.max-ref-age-ms (effectively never expire refs).
    pub const PROPERTY_MAX_REF_AGE_MS_DEFAULT: i64 = i64::MAX;

    /// Property key for the default schema name mapping.
    pub const PROPERTY_DEFAULT_NAME_MAPPING: &'static str =
        "schema.name-mapping.default";

    /// Property key for the master key id used to encrypt the table's manifest list and data files.
    pub const PROPERTY_ENCRYPTION_KEY_ID: &'static str = "encryption.key-id";
    /// Property key for the encryption data encryption key (DEK) length in bytes.
    pub const PROPERTY_ENCRYPTION_DATA_KEY_LENGTH: &'static str =
        "encryption.data-key-length";
    /// Default value for the encryption DEK length (16 bytes = AES-128).
    pub const PROPERTY_ENCRYPTION_DATA_KEY_LENGTH_DEFAULT: usize = 16;

    pub fn commit_num_retries(&self) -> Result<usize> {
        parse_property(
            self.properties,
            Self::PROPERTY_COMMIT_NUM_RETRIES,
            Self::PROPERTY_COMMIT_NUM_RETRIES_DEFAULT,
        )
    }

    pub fn commit_min_retry_wait_ms(&self) -> Result<u64> {
        parse_property(
            self.properties,
            Self::PROPERTY_COMMIT_MIN_RETRY_WAIT_MS,
            Self::PROPERTY_COMMIT_MIN_RETRY_WAIT_MS_DEFAULT,
        )
    }

    pub fn commit_max_retry_wait_ms(&self) -> Result<u64> {
        parse_property(
            self.properties,
            Self::PROPERTY_COMMIT_MAX_RETRY_WAIT_MS,
            Self::PROPERTY_COMMIT_MAX_RETRY_WAIT_MS_DEFAULT,
        )
    }

    pub fn commit_total_retry_timeout_ms(&self) -> Result<u64> {
        parse_property(
            self.properties,
            Self::PROPERTY_COMMIT_TOTAL_RETRY_TIME_MS,
            Self::PROPERTY_COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT,
        )
    }

    pub fn write_format_default(&self) -> Result<String> {
        parse_property(
            self.properties,
            Self::PROPERTY_DEFAULT_FILE_FORMAT,
            Self::PROPERTY_DEFAULT_FILE_FORMAT_DEFAULT.to_string(),
        )
    }

    pub fn write_target_file_size_bytes(&self) -> Result<usize> {
        parse_property(
            self.properties,
            Self::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES,
            Self::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT,
        )
    }

    pub fn write_metadata_path(&self) -> Result<Option<String>> {
        parse_location_property(self.properties, Self::PROPERTY_WRITE_METADATA_PATH)
    }

    pub fn metadata_previous_versions_max(&self) -> Result<usize> {
        parse_property(
            self.properties,
            Self::PROPERTY_METADATA_PREVIOUS_VERSIONS_MAX,
            Self::PROPERTY_METADATA_PREVIOUS_VERSIONS_MAX_DEFAULT,
        )
    }

    pub fn metadata_delete_after_commit_enabled(&self) -> Result<bool> {
        parse_property_bool(
            self.properties,
            Self::PROPERTY_METADATA_DELETE_AFTER_COMMIT_ENABLED,
            Self::PROPERTY_METADATA_DELETE_AFTER_COMMIT_ENABLED_DEFAULT,
        )
    }

    pub fn write_partition_summary_limit(&self) -> Result<u64> {
        parse_property(
            self.properties,
            Self::PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT,
            Self::PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT_DEFAULT,
        )
    }

    pub fn manifest_min_count_to_merge(&self) -> Result<usize> {
        parse_property(
            self.properties,
            Self::PROPERTY_MANIFEST_MIN_COUNT_TO_MERGE,
            Self::PROPERTY_MANIFEST_MIN_COUNT_TO_MERGE_DEFAULT,
        )
    }

    pub fn manifest_target_size_bytes(&self) -> Result<u64> {
        parse_property(
            self.properties,
            Self::PROPERTY_MANIFEST_TARGET_SIZE_BYTES,
            Self::PROPERTY_MANIFEST_TARGET_SIZE_BYTES_DEFAULT,
        )
    }

    pub fn manifest_merge_enabled(&self) -> Result<bool> {
        parse_property_bool(
            self.properties,
            Self::PROPERTY_MANIFEST_MERGE_ENABLED,
            Self::PROPERTY_MANIFEST_MERGE_ENABLED_DEFAULT,
        )
    }

    pub fn metadata_compression_codec(&self) -> Result<CompressionCodec> {
        parse_metadata_file_compression(self.properties)
    }

    pub fn write_datafusion_fanout_enabled(&self) -> Result<bool> {
        parse_property_bool(
            self.properties,
            Self::PROPERTY_DATAFUSION_WRITE_FANOUT_ENABLED,
            Self::PROPERTY_DATAFUSION_WRITE_FANOUT_ENABLED_DEFAULT,
        )
    }

    pub fn cdc_enabled(&self) -> Result<bool> {
        parse_property_bool(
            self.properties,
            Self::PROPERTY_PARQUET_CDC_ENABLED,
            Self::PROPERTY_PARQUET_CDC_ENABLED_DEFAULT,
        )
    }

    pub fn cdc_min_chunk_size(&self) -> Result<usize> {
        parse_property(
            self.properties,
            Self::PROPERTY_PARQUET_CDC_MIN_CHUNK_SIZE,
            Self::PROPERTY_PARQUET_CDC_MIN_CHUNK_SIZE_DEFAULT,
        )
    }

    pub fn cdc_max_chunk_size(&self) -> Result<usize> {
        parse_property(
            self.properties,
            Self::PROPERTY_PARQUET_CDC_MAX_CHUNK_SIZE,
            Self::PROPERTY_PARQUET_CDC_MAX_CHUNK_SIZE_DEFAULT,
        )
    }

    pub fn cdc_norm_level(&self) -> Result<i32> {
        parse_property(
            self.properties,
            Self::PROPERTY_PARQUET_CDC_NORM_LEVEL,
            Self::PROPERTY_PARQUET_CDC_NORM_LEVEL_DEFAULT,
        )
    }

    pub fn parquet_compression_codec(&self) -> Result<CompressionCodec> {
        parse_parquet_compression(self.properties)
    }

    pub fn parquet_row_group_size_bytes(&self) -> Result<usize> {
        parse_property(
            self.properties,
            Self::PROPERTY_PARQUET_ROW_GROUP_SIZE_BYTES,
            Self::PROPERTY_PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT,
        )
    }

    pub fn parquet_page_size_bytes(&self) -> Result<usize> {
        parse_property(
            self.properties,
            Self::PROPERTY_PARQUET_PAGE_SIZE_BYTES,
            Self::PROPERTY_PARQUET_PAGE_SIZE_BYTES_DEFAULT,
        )
    }

    pub fn parquet_page_row_limit(&self) -> Result<usize> {
        parse_property(
            self.properties,
            Self::PROPERTY_PARQUET_PAGE_ROW_LIMIT,
            Self::PROPERTY_PARQUET_PAGE_ROW_LIMIT_DEFAULT,
        )
    }

    pub fn parquet_dict_size_bytes(&self) -> Result<usize> {
        parse_property(
            self.properties,
            Self::PROPERTY_PARQUET_DICT_SIZE_BYTES,
            Self::PROPERTY_PARQUET_DICT_SIZE_BYTES_DEFAULT,
        )
    }

    pub fn write_data_location(&self) -> Result<Option<String>> {
        parse_location_property(self.properties, Self::PROPERTY_WRITE_DATA_LOCATION)
    }

    pub fn write_folder_storage_location(&self) -> Result<Option<String>> {
        parse_location_property(
            self.properties,
            Self::PROPERTY_WRITE_FOLDER_STORAGE_LOCATION,
        )
    }

    pub fn write_object_storage_location(&self) -> Result<Option<String>> {
        parse_location_property(
            self.properties,
            Self::PROPERTY_WRITE_OBJECT_STORAGE_LOCATION,
        )
    }

    /// Whether new data files use the object storage layout with hash entropy.
    pub fn write_object_storage_enabled(&self) -> Result<bool> {
        parse_property_bool(
            self.properties,
            Self::PROPERTY_WRITE_OBJECT_STORAGE_ENABLED,
            Self::PROPERTY_WRITE_OBJECT_STORAGE_ENABLED_DEFAULT,
        )
    }

    pub fn write_object_storage_partitioned_paths(&self) -> Result<bool> {
        parse_property_bool(
            self.properties,
            Self::PROPERTY_WRITE_OBJECT_STORAGE_PARTITIONED_PATHS,
            Self::PROPERTY_WRITE_OBJECT_STORAGE_PARTITIONED_PATHS_DEFAULT,
        )
    }

    pub fn gc_enabled(&self) -> Result<bool> {
        parse_property_bool(
            self.properties,
            Self::PROPERTY_GC_ENABLED,
            Self::PROPERTY_GC_ENABLED_DEFAULT,
        )
    }

    pub fn max_snapshot_age_ms(&self) -> Result<i64> {
        parse_property(
            self.properties,
            Self::PROPERTY_MAX_SNAPSHOT_AGE_MS,
            Self::PROPERTY_MAX_SNAPSHOT_AGE_MS_DEFAULT,
        )
    }

    pub fn min_snapshots_to_keep(&self) -> Result<usize> {
        parse_property(
            self.properties,
            Self::PROPERTY_MIN_SNAPSHOTS_TO_KEEP,
            Self::PROPERTY_MIN_SNAPSHOTS_TO_KEEP_DEFAULT,
        )
    }

    pub fn max_ref_age_ms(&self) -> Result<i64> {
        parse_property(
            self.properties,
            Self::PROPERTY_MAX_REF_AGE_MS,
            Self::PROPERTY_MAX_REF_AGE_MS_DEFAULT,
        )
    }

    pub fn default_name_mapping(&self) -> Result<Option<NameMapping>> {
        parse_optional_property(self.properties, Self::PROPERTY_DEFAULT_NAME_MAPPING)
    }

    pub fn encryption_key_id(&self) -> Result<Option<String>> {
        parse_optional_property(self.properties, Self::PROPERTY_ENCRYPTION_KEY_ID)
    }

    pub fn encryption_data_key_length(&self) -> Result<usize> {
        parse_property(
            self.properties,
            Self::PROPERTY_ENCRYPTION_DATA_KEY_LENGTH,
            Self::PROPERTY_ENCRYPTION_DATA_KEY_LENGTH_DEFAULT,
        )
    }

    pub fn write_delete_isolation_level(&self) -> Result<IsolationLevel> {
        parse_property(
            self.properties,
            Self::PROPERTY_WRITE_DELETE_ISOLATION_LEVEL,
            Self::PROPERTY_WRITE_ISOLATION_LEVEL_DEFAULT,
        )
    }

    pub fn write_update_isolation_level(&self) -> Result<IsolationLevel> {
        parse_property(
            self.properties,
            Self::PROPERTY_WRITE_UPDATE_ISOLATION_LEVEL,
            Self::PROPERTY_WRITE_ISOLATION_LEVEL_DEFAULT,
        )
    }

    pub fn write_merge_isolation_level(&self) -> Result<IsolationLevel> {
        parse_property(
            self.properties,
            Self::PROPERTY_WRITE_MERGE_ISOLATION_LEVEL,
            Self::PROPERTY_WRITE_ISOLATION_LEVEL_DEFAULT,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_properties_default() {
        let props = HashMap::new();
        let table_properties = TableProperties::new(&props);
        assert_eq!(
            table_properties.commit_num_retries().unwrap(),
            TableProperties::PROPERTY_COMMIT_NUM_RETRIES_DEFAULT
        );
        assert_eq!(
            table_properties.commit_min_retry_wait_ms().unwrap(),
            TableProperties::PROPERTY_COMMIT_MIN_RETRY_WAIT_MS_DEFAULT
        );
        assert_eq!(
            table_properties.commit_max_retry_wait_ms().unwrap(),
            TableProperties::PROPERTY_COMMIT_MAX_RETRY_WAIT_MS_DEFAULT
        );
        assert_eq!(
            table_properties.write_format_default().unwrap(),
            TableProperties::PROPERTY_DEFAULT_FILE_FORMAT_DEFAULT.to_string()
        );
        assert_eq!(
            table_properties.write_target_file_size_bytes().unwrap(),
            TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT
        );
        assert_eq!(
            table_properties.metadata_compression_codec().unwrap(),
            CompressionCodec::None
        );
        assert_eq!(
            table_properties.write_delete_isolation_level().unwrap(),
            TableProperties::PROPERTY_WRITE_ISOLATION_LEVEL_DEFAULT
        );
        assert_eq!(
            table_properties.write_update_isolation_level().unwrap(),
            TableProperties::PROPERTY_WRITE_ISOLATION_LEVEL_DEFAULT
        );
        assert_eq!(
            table_properties.write_merge_isolation_level().unwrap(),
            TableProperties::PROPERTY_WRITE_ISOLATION_LEVEL_DEFAULT
        );
    }

    #[test]
    fn test_table_properties_valid() {
        let props = HashMap::from([
            (
                TableProperties::PROPERTY_COMMIT_NUM_RETRIES.to_string(),
                "10".to_string(),
            ),
            (
                TableProperties::PROPERTY_COMMIT_MAX_RETRY_WAIT_MS.to_string(),
                "20".to_string(),
            ),
            (
                TableProperties::PROPERTY_DEFAULT_FILE_FORMAT.to_string(),
                "avro".to_string(),
            ),
            (
                TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES.to_string(),
                "512".to_string(),
            ),
            (
                TableProperties::PROPERTY_WRITE_DELETE_ISOLATION_LEVEL.to_string(),
                "snapshot".to_string(),
            ),
            (
                TableProperties::PROPERTY_WRITE_UPDATE_ISOLATION_LEVEL.to_string(),
                "serializable".to_string(),
            ),
            (
                TableProperties::PROPERTY_WRITE_MERGE_ISOLATION_LEVEL.to_string(),
                "SNAPSHOT".to_string(),
            ),
        ]);
        let table_properties = TableProperties::new(&props);
        assert_eq!(table_properties.commit_num_retries().unwrap(), 10);
        assert_eq!(table_properties.commit_max_retry_wait_ms().unwrap(), 20);
        assert_eq!(table_properties.write_format_default().unwrap(), "avro");
        assert_eq!(
            table_properties.write_target_file_size_bytes().unwrap(),
            512
        );
        assert_eq!(
            table_properties.write_delete_isolation_level().unwrap(),
            IsolationLevel::Snapshot
        );
        assert_eq!(
            table_properties.write_update_isolation_level().unwrap(),
            IsolationLevel::Serializable
        );
        assert_eq!(
            table_properties.write_merge_isolation_level().unwrap(),
            IsolationLevel::Snapshot
        );
    }

    #[test]
    fn test_table_properties_compression() {
        let props = HashMap::from([(
            TableProperties::PROPERTY_METADATA_COMPRESSION_CODEC.to_string(),
            "gzip".to_string(),
        )]);
        let table_properties = TableProperties::new(&props);
        assert_eq!(
            table_properties.metadata_compression_codec().unwrap(),
            CompressionCodec::gzip_default()
        );
    }

    #[test]
    fn test_parse_metadata_file_compression_valid() {
        for (value, expected) in [
            ("none", CompressionCodec::None),
            ("", CompressionCodec::None),
            ("gzip", CompressionCodec::gzip_default()),
            ("NONE", CompressionCodec::None),
            ("GZIP", CompressionCodec::gzip_default()),
            ("GzIp", CompressionCodec::gzip_default()),
        ] {
            let props = HashMap::from([(
                TableProperties::PROPERTY_METADATA_COMPRESSION_CODEC.to_string(),
                value.to_string(),
            )]);
            assert_eq!(parse_metadata_file_compression(&props).unwrap(), expected);
        }

        assert_eq!(
            parse_metadata_file_compression(&HashMap::new()).unwrap(),
            CompressionCodec::None
        );
    }

    #[test]
    fn test_parse_metadata_file_compression_invalid() {
        for codec in ["lz4", "zstd", "snappy"] {
            let props = HashMap::from([(
                TableProperties::PROPERTY_METADATA_COMPRESSION_CODEC.to_string(),
                codec.to_string(),
            )]);
            let err = parse_metadata_file_compression(&props).unwrap_err();
            let err_msg = err.to_string();
            assert!(
                err_msg.contains("Invalid metadata compression codec"),
                "expected invalid codec error, got: {err_msg}"
            );
            assert!(
                err_msg.contains("Only 'none' and 'gzip' are supported"),
                "expected supported codec list, got: {err_msg}"
            );
        }
    }

    #[test]
    fn test_table_properties_invalid() {
        let invalid_retries = HashMap::from([(
            TableProperties::PROPERTY_COMMIT_NUM_RETRIES.to_string(),
            "abc".to_string(),
        )]);

        let err = TableProperties::new(&invalid_retries)
            .commit_num_retries()
            .unwrap_err();
        assert!(
            err.to_string().contains(
                "Invalid value for commit.retry.num-retries: invalid digit found in string"
            )
        );

        let invalid_min_wait = HashMap::from([(
            TableProperties::PROPERTY_COMMIT_MIN_RETRY_WAIT_MS.to_string(),
            "abc".to_string(),
        )]);
        let err = TableProperties::new(&invalid_min_wait)
            .commit_min_retry_wait_ms()
            .unwrap_err();
        assert!(
            err.to_string().contains(
                "Invalid value for commit.retry.min-wait-ms: invalid digit found in string"
            )
        );

        let invalid_max_wait = HashMap::from([(
            TableProperties::PROPERTY_COMMIT_MAX_RETRY_WAIT_MS.to_string(),
            "abc".to_string(),
        )]);
        let err = TableProperties::new(&invalid_max_wait)
            .commit_max_retry_wait_ms()
            .unwrap_err();
        assert!(
            err.to_string().contains(
                "Invalid value for commit.retry.max-wait-ms: invalid digit found in string"
            )
        );

        let invalid_target_size = HashMap::from([(
            TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES.to_string(),
            "abc".to_string(),
        )]);
        let err = TableProperties::new(&invalid_target_size)
            .write_target_file_size_bytes()
            .unwrap_err();
        assert!(err.to_string().contains(
            "Invalid value for write.target-file-size-bytes: invalid digit found in string"
        ));
    }

    #[test]
    fn test_cdc_disabled_by_default() {
        let props = HashMap::new();
        let tp = TableProperties::new(&props);
        assert!(!tp.cdc_enabled().unwrap());
    }

    #[test]
    fn test_cdc_enabled_via_flag() {
        let props = HashMap::from([(
            TableProperties::PROPERTY_PARQUET_CDC_ENABLED.to_string(),
            "true".to_string(),
        )]);
        let tp = TableProperties::new(&props);
        assert!(tp.cdc_enabled().unwrap());
        assert_eq!(tp.cdc_min_chunk_size().unwrap(), 256 * 1024);
        assert_eq!(tp.cdc_max_chunk_size().unwrap(), 1024 * 1024);
        assert_eq!(tp.cdc_norm_level().unwrap(), 0);
    }

    #[test]
    fn test_cdc_size_props_alone_do_not_enable() {
        let props = HashMap::from([(
            TableProperties::PROPERTY_PARQUET_CDC_MIN_CHUNK_SIZE.to_string(),
            "262144".to_string(),
        )]);
        let tp = TableProperties::new(&props);
        assert!(!tp.cdc_enabled().unwrap());
    }

    #[test]
    fn test_cdc_custom_values() {
        let props = HashMap::from([
            (
                TableProperties::PROPERTY_PARQUET_CDC_ENABLED.to_string(),
                "true".to_string(),
            ),
            (
                TableProperties::PROPERTY_PARQUET_CDC_MIN_CHUNK_SIZE.to_string(),
                "200000".to_string(),
            ),
            (
                TableProperties::PROPERTY_PARQUET_CDC_MAX_CHUNK_SIZE.to_string(),
                "900000".to_string(),
            ),
            (
                TableProperties::PROPERTY_PARQUET_CDC_NORM_LEVEL.to_string(),
                "1".to_string(),
            ),
        ]);
        let tp = TableProperties::new(&props);
        assert!(tp.cdc_enabled().unwrap());
        assert_eq!(tp.cdc_min_chunk_size().unwrap(), 200000);
        assert_eq!(tp.cdc_max_chunk_size().unwrap(), 900000);
        assert_eq!(tp.cdc_norm_level().unwrap(), 1);
    }

    #[test]
    fn test_cdc_invalid_min_chunk_size() {
        let props = HashMap::from([
            (
                TableProperties::PROPERTY_PARQUET_CDC_ENABLED.to_string(),
                "true".to_string(),
            ),
            (
                TableProperties::PROPERTY_PARQUET_CDC_MIN_CHUNK_SIZE.to_string(),
                "not_a_number".to_string(),
            ),
        ]);
        let err = TableProperties::new(&props)
            .cdc_min_chunk_size()
            .unwrap_err();
        assert!(err.to_string().contains(
            "Invalid value for write.parquet.content-defined-chunking.min-chunk-size"
        ));
    }

    #[test]
    fn test_properties_are_parsed_lazily() {
        let props = HashMap::from([
            (
                TableProperties::PROPERTY_COMMIT_NUM_RETRIES.to_string(),
                "not-a-number".to_string(),
            ),
            (
                TableProperties::PROPERTY_GC_ENABLED.to_string(),
                "FALSE".to_string(),
            ),
        ]);
        let table_properties = TableProperties::new(&props);

        assert!(!table_properties.gc_enabled().unwrap());
        assert!(table_properties.commit_num_retries().is_err());
    }

    #[test]
    fn test_boolean_properties_are_case_insensitive() {
        let props = HashMap::from([
            (
                TableProperties::PROPERTY_GC_ENABLED.to_string(),
                "FaLsE".to_string(),
            ),
            (
                TableProperties::PROPERTY_DATAFUSION_WRITE_FANOUT_ENABLED.to_string(),
                "TrUe".to_string(),
            ),
            (
                TableProperties::PROPERTY_PARQUET_CDC_ENABLED.to_string(),
                "TrUe".to_string(),
            ),
            (
                TableProperties::PROPERTY_MANIFEST_MERGE_ENABLED.to_string(),
                "FaLsE".to_string(),
            ),
            (
                TableProperties::PROPERTY_METADATA_DELETE_AFTER_COMMIT_ENABLED
                    .to_string(),
                "TrUe".to_string(),
            ),
            (
                TableProperties::PROPERTY_WRITE_OBJECT_STORAGE_PARTITIONED_PATHS
                    .to_string(),
                "FaLsE".to_string(),
            ),
        ]);
        let table_properties = TableProperties::new(&props);

        assert!(!table_properties.gc_enabled().unwrap());
        assert!(table_properties.write_datafusion_fanout_enabled().unwrap());
        assert!(table_properties.cdc_enabled().unwrap());
        assert!(!table_properties.manifest_merge_enabled().unwrap());
        assert!(
            table_properties
                .metadata_delete_after_commit_enabled()
                .unwrap()
        );
        assert!(
            !table_properties
                .write_object_storage_partitioned_paths()
                .unwrap()
        );
    }

    #[test]
    fn test_default_name_mapping() {
        let props = HashMap::from([(
            TableProperties::PROPERTY_DEFAULT_NAME_MAPPING.to_string(),
            r#"[{"field-id":1,"names":["id"]}]"#.to_string(),
        )]);

        assert!(
            TableProperties::new(&props)
                .default_name_mapping()
                .unwrap()
                .is_some()
        );
        assert!(
            TableProperties::new(&HashMap::new())
                .default_name_mapping()
                .unwrap()
                .is_none()
        );
    }
}
