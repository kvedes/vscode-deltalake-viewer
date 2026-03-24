pub mod convert;
pub mod delta;
pub mod error;
pub mod parquet;
pub mod schema;

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::schema::ColumnDef;

/// Breakdown of CDF change types.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CdfCounts {
    pub inserts: usize,
    pub updates: usize,
    pub deletes: usize,
}

/// Common result type for read operations.
#[derive(Debug, Serialize, Deserialize)]
pub struct ReadResult {
    pub schema: Vec<ColumnDef>,
    pub rows: Vec<Map<String, Value>>,
    pub total_rows: usize,
    pub offset: usize,
    pub limit: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cdf_counts: Option<CdfCounts>,
}

/// A single entry in the Delta table history.
#[derive(Debug, Serialize, Deserialize)]
pub struct HistoryEntry {
    pub version: i64,
    pub timestamp: Option<i64>,
    pub operation: Option<String>,
    pub operation_params: Option<HashMap<String, String>>,
    pub user_name: Option<String>,
}

/// Result of a history query.
#[derive(Debug, Serialize, Deserialize)]
pub struct HistoryResult {
    pub entries: Vec<HistoryEntry>,
}

/// Result of a table info query.
#[derive(Debug, Serialize, Deserialize)]
pub struct TableInfoResult {
    pub name: Option<String>,
    pub description: Option<String>,
    pub location: String,
    pub current_version: i64,
    pub created_time: Option<i64>,
    pub min_reader_version: i32,
    pub min_writer_version: i32,
    pub cdf_enabled: bool,
    pub partition_columns: Vec<String>,
    pub num_files: usize,
    pub total_size_bytes: i64,
    pub configuration: HashMap<String, String>,
    pub id: String,
    pub format_provider: String,
    pub format_options: HashMap<String, String>,
    pub reader_features: Option<Vec<String>>,
    pub writer_features: Option<Vec<String>>,
}
