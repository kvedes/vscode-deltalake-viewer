use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use delta_core::error::ErrorCode;
use delta_core::schema::ColumnDef;

#[derive(Debug, Deserialize)]
pub struct Request {
    pub id: String,
    #[serde(flatten)]
    pub command: Command,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "command", content = "params")]
#[serde(rename_all = "snake_case")]
pub enum Command {
    ReadParquet {
        path: String,
        #[serde(default = "default_offset")]
        offset: usize,
        #[serde(default = "default_limit")]
        limit: usize,
    },
    ReadDelta {
        path: String,
        #[serde(default = "default_offset")]
        offset: usize,
        #[serde(default = "default_limit")]
        limit: usize,
        #[serde(default)]
        version: Option<i64>,
        #[serde(default)]
        known_total: Option<usize>,
    },
    ReadCdf {
        path: String,
        start_version: i64,
        end_version: i64,
        #[serde(default = "default_offset")]
        offset: usize,
        #[serde(default = "default_limit")]
        limit: usize,
    },
    GetSchema {
        path: String,
    },
    GetHistory {
        path: String,
    },
    GetTableInfo {
        path: String,
    },
    RefreshTable {
        path: String,
    },
    Ping {},
    Shutdown {},
}

fn default_offset() -> usize {
    0
}

fn default_limit() -> usize {
    1000
}

#[derive(Debug, Serialize)]
pub struct Response {
    pub id: String,
    #[serde(flatten)]
    pub body: ResponseBody,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum ResponseBody {
    Result {
        result: ResultPayload,
    },
    Error {
        error: String,
        code: ErrorCode,
        retryable: bool,
    },
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum ResultPayload {
    Data {
        schema: Vec<ColumnDef>,
        rows: Vec<Map<String, Value>>,
        total_rows: usize,
        offset: usize,
        limit: usize,
    },
    History {
        entries: Vec<delta_core::HistoryEntry>,
    },
    TableInfo(delta_core::TableInfoResult),

    // Streaming variants
    DataHeader {
        schema: Vec<ColumnDef>,
        total_rows: usize,
        offset: usize,
        #[serde(skip_serializing_if = "Option::is_none")]
        cdf_counts: Option<delta_core::CdfCounts>,
    },
    DataChunk {
        rows: Vec<Map<String, Value>>,
        chunk_index: usize,
    },
    DataDone {
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
        ResultPayload::History {
            entries: r.entries,
        }
    }
}

impl From<delta_core::TableInfoResult> for ResultPayload {
    fn from(r: delta_core::TableInfoResult) -> Self {
        ResultPayload::TableInfo(r)
    }
}
