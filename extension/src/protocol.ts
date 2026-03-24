export type {
  ColumnDef,
  HistoryEntry,
  ErrorCode,
  HostToWebviewMessage,
  WebviewToHostMessage,
  DataMessage,
  ErrorMessage,
  InitMessage,
  HistoryMessage,
  TableInfoMessage,
  DataHeaderMessage,
  DataChunkMessage,
  DataDoneMessage,
} from "./shared/types.js";

export type { ErrorCode as ErrorCodeType } from "./shared/types.js";

import type { ErrorCode } from "./shared/types.js";

export interface DataResult {
  type: "data";
  schema: import("./shared/types.js").ColumnDef[];
  rows: Record<string, unknown>[];
  total_rows: number;
  offset: number;
  limit: number;
}

export interface HistoryResult {
  type: "history";
  entries: import("./shared/types.js").HistoryEntry[];
}

export interface TableInfoResult {
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

export interface DataHeaderResult {
  type: "data_header";
  schema: import("./shared/types.js").ColumnDef[];
  total_rows: number;
  offset: number;
  cdf_counts?: CdfCounts;
}

export interface DataChunkResult {
  type: "data_chunk";
  rows: Record<string, unknown>[];
  chunk_index: number;
}

export interface DataDoneResult {
  type: "data_done";
  total_sent: number;
}

export type ResultPayload =
  | DataResult
  | HistoryResult
  | TableInfoResult
  | DataHeaderResult
  | DataChunkResult
  | DataDoneResult;

export interface SidecarRequest {
  id: string;
  command: string;
  params: Record<string, unknown>;
}

export interface SidecarResponse {
  id: string;
  result?: ResultPayload;
  error?: string;
  code?: ErrorCode;
  retryable?: boolean;
}

export class SidecarError extends Error {
  public readonly code: ErrorCode | undefined;
  public readonly retryable: boolean;

  constructor(message: string, code?: ErrorCode, retryable?: boolean) {
    super(message);
    this.name = "SidecarError";
    this.code = code;
    this.retryable = retryable ?? false;
  }
}
