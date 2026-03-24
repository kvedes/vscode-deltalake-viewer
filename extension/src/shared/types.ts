// Types shared between extension host and webview.
// This file is imported by both the Node extension bundle and the browser webview bundle.

export type ErrorCode =
  | "table_not_found"
  | "version_not_found"
  | "permission_denied"
  | "corrupt_log"
  | "io_error"
  | "query_error"
  | "internal";

export interface ColumnDef {
  name: string;
  data_type: string;
  nullable: boolean;
  metadata: Record<string, string>;
}

export interface HistoryEntry {
  version: number;
  timestamp: number | null;
  operation: string | null;
  operation_params: Record<string, string> | null;
  user_name: string | null;
}

// Host-to-webview messages
export interface DataMessage {
  type: "data";
  schema: ColumnDef[];
  rows: Record<string, unknown>[];
  total_rows: number;
  offset: number;
  limit: number;
  version?: number;
}

export interface ErrorMessage {
  type: "error";
  message: string;
  code?: ErrorCode;
  retryable?: boolean;
}

export interface InitMessage {
  type: "init";
  fileType: "parquet" | "delta";
}

export interface HistoryMessage {
  type: "history";
  entries: HistoryEntry[];
}

export interface TableInfoMessage {
  type: "table_info";
  name: string | null;
  description: string | null;
  location: string;
  current_version: number;
  created_time: number | null;
  min_reader_version: number;
  min_writer_version: number;
  cdf_enabled: boolean;
  partition_columns: string[];
  num_files: number;
  total_size_bytes: number;
  configuration: Record<string, string>;
  id: string;
  format_provider: string;
  format_options: Record<string, string>;
  reader_features: string[] | null;
  writer_features: string[] | null;
}

export interface CdfCounts {
  inserts: number;
  updates: number;
  deletes: number;
}

export interface DataHeaderMessage {
  type: "data_header";
  schema: ColumnDef[];
  total_rows: number;
  offset: number;
  version?: number;
  cdf_mode?: boolean;
  cdf_counts?: CdfCounts;
}

export interface DataChunkMessage {
  type: "data_chunk";
  rows: Record<string, unknown>[];
  chunk_index: number;
}

export interface DataDoneMessage {
  type: "data_done";
}

export type HostToWebviewMessage =
  | DataMessage
  | ErrorMessage
  | InitMessage
  | HistoryMessage
  | TableInfoMessage
  | DataHeaderMessage
  | DataChunkMessage
  | DataDoneMessage;

// Webview-to-host messages
export interface ReadyMessage {
  type: "ready";
}

export interface PageMessage {
  type: "page";
  offset: number;
}

export interface LoadVersionMessage {
  type: "load_version";
  version: number;
}

export interface RequestHistoryMessage {
  type: "request_history";
}

export interface RequestTableInfoMessage {
  type: "request_table_info";
}

export interface LoadCdfMessage {
  type: "load_cdf";
  version: number;
  offset?: number;
}

export type WebviewToHostMessage =
  | ReadyMessage
  | PageMessage
  | LoadVersionMessage
  | RequestHistoryMessage
  | RequestTableInfoMessage
  | LoadCdfMessage;
