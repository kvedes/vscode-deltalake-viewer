//! JSON wire protocol types for communication between the VS Code extension and
//! the Rust sidecar process.
//!
//! Messages are newline-delimited JSON objects. Each [`Request`] carries a unique
//! `id` that is echoed back on every [`Response`], allowing the extension to
//! correlate responses (including streamed chunks) with their originating request.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use delta_core::error::ErrorCode;
use delta_core::schema::ColumnDef;

/// An incoming request from the VS Code extension.
#[derive(Debug, Deserialize)]
pub struct Request {
    /// Caller-assigned identifier echoed on all responses for this request.
    pub id: String,
    /// The command and its parameters.
    #[serde(flatten)]
    pub command: Command,
}

/// Supported commands that the sidecar can handle.
#[derive(Debug, Deserialize)]
#[serde(tag = "command", content = "params")]
#[serde(rename_all = "snake_case")]
pub enum Command {
    /// Read paginated rows from a standalone Parquet file.
    ReadParquet {
        path: String,
        #[serde(default = "default_offset")]
        offset: usize,
        #[serde(default = "default_limit")]
        limit: usize,
    },
    /// Read paginated rows from a Delta table, optionally at a specific version.
    ReadDelta {
        path: String,
        #[serde(default = "default_offset")]
        offset: usize,
        #[serde(default = "default_limit")]
        limit: usize,
        /// Pin the read to a specific table version. `None` reads the latest.
        #[serde(default)]
        version: Option<i64>,
        /// If the caller already knows the total row count, pass it here to
        /// skip the count query.
        #[serde(default)]
        known_total: Option<usize>,
    },
    /// Read Change Data Feed rows between two versions.
    ReadCdf {
        path: String,
        start_version: i64,
        end_version: i64,
        #[serde(default = "default_offset")]
        offset: usize,
        #[serde(default = "default_limit")]
        limit: usize,
    },
    /// Return the schema (column definitions) for a Delta table or Parquet file.
    GetSchema { path: String },
    /// Return the full commit history of a Delta table.
    GetHistory { path: String },
    /// Return metadata and properties of a Delta table.
    GetTableInfo { path: String },
    /// Evict all cached data for a table so the next read sees fresh state.
    RefreshTable { path: String },
    /// Health-check command; returns an empty success response.
    Ping {},
    /// Gracefully shut down the sidecar process.
    Shutdown {},
}

fn default_offset() -> usize {
    0
}

fn default_limit() -> usize {
    1000
}

/// An outgoing response to the VS Code extension.
#[derive(Debug, Serialize)]
pub struct Response {
    /// Echoed request identifier.
    pub id: String,
    /// Success payload or structured error.
    #[serde(flatten)]
    pub body: ResponseBody,
}

/// Top-level response body — either a result or a structured error.
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum ResponseBody {
    /// Successful result.
    Result { result: ResultPayload },
    /// Structured error with a machine-readable code and retry hint.
    Error {
        error: String,
        code: ErrorCode,
        retryable: bool,
    },
}

/// Payload variants for successful responses.
///
/// Non-streaming commands return `Data`, `History`, or `TableInfo`.
/// Streaming read commands emit `DataHeader` followed by one or more
/// `DataChunk`s and a final `DataDone`.
#[derive(Debug, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum ResultPayload {
    /// Complete (non-streamed) data response.
    Data {
        schema: Vec<ColumnDef>,
        rows: Vec<Map<String, Value>>,
        total_rows: usize,
        offset: usize,
        limit: usize,
    },
    /// Table commit history.
    History {
        entries: Vec<delta_core::HistoryEntry>,
    },
    /// Table metadata and properties.
    TableInfo(delta_core::TableInfoResult),

    /// First message of a streaming read — carries schema and total row count.
    DataHeader {
        schema: Vec<ColumnDef>,
        total_rows: usize,
        offset: usize,
        #[serde(skip_serializing_if = "Option::is_none")]
        cdf_counts: Option<delta_core::CdfCounts>,
    },
    /// A batch of rows within a streaming read.
    DataChunk {
        rows: Vec<Map<String, Value>>,
        /// Zero-based index of this chunk within the stream.
        chunk_index: usize,
    },
    /// Final sentinel of a streaming read indicating all chunks have been sent.
    DataDone {
        /// Total number of rows sent across all chunks.
        total_sent: usize,
    },
}

impl From<delta_core::ReadResult> for ResultPayload {
    fn from(r: delta_core::ReadResult) -> Self {
        ResultPayload::Data {
            schema: r.schema,
            rows: r.rows,
            total_rows: r.total_rows,
            offset: r.offset,
            limit: r.limit,
        }
    }
}

impl From<delta_core::HistoryResult> for ResultPayload {
    fn from(r: delta_core::HistoryResult) -> Self {
        ResultPayload::History { entries: r.entries }
    }
}

impl From<delta_core::TableInfoResult> for ResultPayload {
    fn from(r: delta_core::TableInfoResult) -> Self {
        ResultPayload::TableInfo(r)
    }
}
