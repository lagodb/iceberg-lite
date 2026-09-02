# iceberg-lite

[![CI](https://github.com/lagodb/iceberg-lite/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/lagodb/iceberg-lite/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

`iceberg-lite` is a lightweight, synchronous Rust implementation of the [Apache Iceberg](https://iceberg.apache.org/) table format. It is derived from [Apache Iceberg Rust (iceberg-rust)](https://github.com/apache/iceberg-rust) and purpose-built for relational database engines, embedded analytical runtimes, and storage layers that require a deterministic, synchronous execution model.

Originally engineered as the core table format engine for [LagoDB](https://github.com/lagodb/lagodb), `iceberg-lite` is maintained as an independent, standalone library.

---

## Architectural Principles & Design Rationale

While upstream `iceberg-rust` is optimized for asynchronous, distributed query engines (such as Apache DataFusion with Tokio and OpenDAL), relational databases like PostgreSQL operate on a synchronous, process-per-connection execution model with complex transactional semantics. `iceberg-lite` redesigns the core Iceberg engine to align with these database-native requirements:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                       Host Engine / SQL Query Planner                       │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │
                    ┌──────────────────┴──────────────────┐
                    ▼                                     ▼
        ┌───────────────────────┐             ┌───────────────────────┐
        │       TableScan       │             │      Transaction      │
        │ (Scan Planning, Expr) │             │ (Schema, DDL, Vacuum) │
        └───────────┬───────────┘             └───────────┬───────────┘
                    │                                     │
                    │      ┌───────────────────────┐      │
                    ├─────►│     SnapshotDelta     │◄─────┤
                    │      │ (Transaction Overlay) │      │
                    │      └───────────────────────┘      │
                    ▼                                     ▼
        ┌───────────────────────┐             ┌───────────────────────┐
        │   Arrow/Parquet IO    │             │   Manifest / Puffin   │
        │ (Row Lineage, Filter) │             │  (Deletion Vectors)   │
        └───────────┬───────────┘             └───────────┬───────────┘
                    │                                     │
                    └──────────────────┬──────────────────┘
                                       ▼
        ┌─────────────────────────────────────────────────────────────┐
        │                   Synchronous FileIO Layer                  │
        │      (Storage Trait: Local, Memory, lagodb-storage, etc.)   │
        └─────────────────────────────────────────────────────────────┘
```

### 1. Pure Synchronous Execution Model
- **PostgreSQL-Native Concurrency**: Traditional database backends manage concurrency at the process/worker boundary without an async runtime. Asynchronous Rust runtimes (e.g. Tokio) introduce thread-pool scheduling overhead, executor context-switching penalties, and impedance mismatches with database memory contexts and signal handlers.
- **Synchronous Call Graphs**: All table operations—from catalog lookups, manifest parsing, and partition pruning to data reading and commit validation—execute synchronously and deterministically on the calling thread.

### 2. Redesigned, Decoupled I/O and Storage Subsystem
- **Zero OpenDAL Runtime Overhead**: The asynchronous OpenDAL abstraction layer is replaced with a lean, synchronous I/O model centered around the `Storage`, `FileIO`, `FileRead`, and `FileWrite` traits.
- **Pluggable Storage Backends**: Ships with built-in `LocalStorage` and `MemoryStorage`, and defines a clean `StorageFactory` boundary. This allows external storage engines—such as `lagodb-storage` (supporting POSIX, S3, and memory tiers)—to be plugged in seamlessly.
- **Multi-Phase Write Lifecycle**: Incorporates a two-phase write protocol (`write` → `finish` → `finalize_write`) to allow object-storage implementations to stage data files locally and reliably commit uploads at the SQL statement boundary.
- **Synchronous Object Caching**: Integrated `ObjectCache` provides bounded, concurrent in-memory caching for manifests, manifest lists, and metadata files without background async workers.

### 3. Transaction-Local Snapshot Overlay (`SnapshotDelta`)
- **Multi-Statement Transaction Support**: Relational database transactions often execute multiple sequential statements (`INSERT`, `UPDATE`, `DELETE`, `TRUNCATE`, `SAVEPOINT`) before committing. Writing immutable Iceberg metadata files to storage on every SQL statement is prohibitively expensive.
- **In-Memory Action Log**: `SnapshotDelta` records pending file additions, position deletes, and file removals within the transaction in memory.
- **Read-Your-Own-Writes**: Query planning dynamically overlays `SnapshotDelta` on top of the committed base snapshot manifests, allowing subsequent statements in the same transaction to read uncommitted changes with zero storage I/O.
- **Net-Effect Resolution & Cancellation**: Transient files added and later deleted/truncated within the same transaction are canceled in memory without ever touching persistent manifests or object storage.
- **Savepoint Rollback & Atomic Commit**: `SnapshotDeltaMarker` supports transaction savepoints and subtransaction rollbacks. At transaction commit, `SnapshotDeltaAction` batches all net modifications into a single atomic Iceberg commit.

### 4. Database-Grade DDL, Maintenance & Metadata Tracking
- **Schema Evolution (ALTER TABLE)**: Full support for schema updates via `UpdateSchemaAction` (adding, dropping, renaming, reordering columns, type promotion, nullability changes, and schema ID assignment).
- **Table Maintenance & Vacuum**:
  - `ExpireSnapshotsAction`: Snapshot expiration and metadata retention management.
  - `RewriteFilesAction`: Compaction and bin-packing of data files.
  - `RewriteManifestsAction`: Manifest merging and index optimization.
  - `TRUNCATE`: Fast table truncation supported at both transaction-delta and commit levels.
- **System Inspection Tables**: Built-in metadata inspection tables (`history`, `manifests`, `snapshots`, `metadata_table`) enable database engines to expose Iceberg internal metadata directly through SQL system catalogs.
- **Two-Phase Commit Preparation**: `PreparedTransaction` and `PreparedTableCommit` decouple metadata generation from catalog pointer updates, facilitating synchronization with external database transaction managers.

### 5. Iceberg Spec v3 & Row Lineage Support
- **Format Version 3**: First-class support for Iceberg Spec v3 tables and metadata serialization.
- **Row ID & Physical Lineage Resolution**: Implements `_row_id` and `_last_updated_sequence_number` generation (`first_row_id`, `next_row_id`), complete with physical column lineage mapping in the Arrow and Parquet reader pipelines.
- **RoaringTreemap Deletion Vectors**: Low-overhead deletion vector structures supporting position-based row deletion and real-time filtering in Arrow record batch readers.

### 6. Enhanced Puffin Format
- **Synchronous Puffin Engine**: Dedicated `PuffinReader` and `PuffinWriter` supporting standard blob types, including Deletion Vectors (`DELETION_VECTOR_V1`) and Theta sketches (`APACHE_DATASKETCHES_THETA_V1`).
- **Sub-File Blob Addressing**: `DeleteFileIdentity` tracks `content_offset` and `content_size_in_bytes`, enabling multiple deletion vector blobs to reside in a single shared physical Puffin file with byte-range CRC validation.
- **Compression Flexibility**: Built-in support for LZ4, Zstandard, and uncompressed Puffin blobs.

### 7. Streamlined Single-Crate Architecture
- **Focused Scope**: External async catalog crates (AWS Glue, Hive Metastore, DynamoDB, SQLx catalogs) have been removed to eliminate heavy transitive dependencies and async network stacks.
- **Clean Catalog Boundary**: Exposes a synchronous `Catalog` trait with reference implementations (`MemoryCatalog`, session-based REST client), allowing host databases to implement catalog metadata tracking directly on top of native database catalogs.
- **Zero Unsafe & Clean Dependencies**: Standalone library with no dependencies on PostgreSQL, `pgrx`, or external workspace crates.

---

## Development

The repository pins its Rust toolchain in `rust-toolchain.toml`. Standard development commands include:

```sh
# Format checking
cargo fmt --check

# Compile checks & unit tests
cargo test

# Lints
cargo clippy --all-targets
```

---

## Upstream Relationship

`iceberg-lite` is derived from [Apache Iceberg Rust (iceberg-rust)](https://github.com/apache/iceberg-rust). It maintains structural alignment with upstream where practical, while intentionally diverging to provide synchronous I/O, transactional overlays, and database-native capabilities.

---

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
