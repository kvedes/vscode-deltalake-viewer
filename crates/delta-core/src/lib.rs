//! Core library for reading Delta Lake tables and Parquet files.
//!
//! Provides async APIs for querying table data, schema, history, and metadata,
//! as well as synchronous Parquet file reading. Results are returned as
//! JSON-compatible structures suitable for serialization over a wire protocol.

pub mod convert;
pub mod delta;
pub mod error;
pub mod parquet;
pub mod schema;

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::schema::ColumnDef;

/// Breakdown of CDF (Change Data Feed) change types.
///
/// Tracks the number of inserts, updates, and deletes observed in a CDF query
/// between two table versions.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CdfCounts {
    /// Number of inserted rows.
    pub inserts: usize,
    /// Number of updated rows (post-image count).
    pub updates: usize,
    /// Number of deleted rows.
    pub deletes: usize,
}

/// Common result type for read operations on both Delta tables and Parquet files.
///
/// Contains a page of rows along with schema information and pagination metadata.
#[derive(Debug, Serialize, Deserialize)]
pub struct ReadResult {
    /// Column definitions for the returned data.
    pub schema: Vec<ColumnDef>,
    /// Row data as JSON key-value maps keyed by column name.
    pub rows: Vec<Map<String, Value>>,
    /// Total number of rows in the source table or file.
    pub total_rows: usize,
    /// Zero-based offset of the first returned row.
    pub offset: usize,
    /// Maximum number of rows requested.
    pub limit: usize,
    /// CDF change-type counts, present only for CDF queries.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cdf_counts: Option<CdfCounts>,
}

/// A single entry in the Delta table commit history.
#[derive(Debug, Serialize, Deserialize)]
pub struct HistoryEntry {
    /// Table version number for this commit.
    pub version: i64,
    /// Unix timestamp in milliseconds when this commit occurred.
    pub timestamp: Option<i64>,
    /// Operation name (e.g., "WRITE", "MERGE", "DELETE").
    pub operation: Option<String>,
    /// Parameters associated with the operation.
    pub operation_params: Option<HashMap<String, String>>,
    /// Name of the user who performed the operation.
    pub user_name: Option<String>,
}

/// Result of a history query, containing all commit entries.
#[derive(Debug, Serialize, Deserialize)]
pub struct HistoryResult {
    /// Commit entries ordered from newest to oldest.
    pub entries: Vec<HistoryEntry>,
}

/// Metadata and properties of a Delta table.
///
/// Aggregates protocol info, storage stats, and table configuration into a
/// single response for the "get table info" command.
#[derive(Debug, Serialize, Deserialize)]
pub struct TableInfoResult {
    /// User-defined table name from Delta metadata.
    pub name: Option<String>,
    /// User-defined table description from Delta metadata.
    pub description: Option<String>,
    /// Filesystem path or URI of the table.
    pub location: String,
    /// Latest committed version number.
    pub current_version: i64,
    /// Unix timestamp in milliseconds when the table was created.
    pub created_time: Option<i64>,
    /// Minimum reader protocol version required.
    pub min_reader_version: i32,
    /// Minimum writer protocol version required.
    pub min_writer_version: i32,
    /// Whether Change Data Feed is enabled on this table.
    pub cdf_enabled: bool,
    /// Column names used for partitioning.
    pub partition_columns: Vec<String>,
    /// Number of data files in the current snapshot.
    pub num_files: usize,
    /// Total size of all data files in bytes.
    pub total_size_bytes: i64,
    /// Delta table configuration properties (e.g., `delta.enableChangeDataFeed`).
    pub configuration: HashMap<String, String>,
    /// Unique table identifier.
    pub id: String,
    /// Storage format provider (typically "parquet").
    pub format_provider: String,
    /// Additional format options.
    pub format_options: HashMap<String, String>,
    /// Reader features required by the table protocol, if any.
    pub reader_features: Option<Vec<String>>,
    /// Writer features required by the table protocol, if any.
    pub writer_features: Option<Vec<String>>,
}
