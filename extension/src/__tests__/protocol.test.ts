import { describe, it, expect } from "vitest";
import { SidecarError } from "../protocol";
import type { ErrorCode, HostToWebviewMessage } from "../shared/types";

describe("SidecarError", () => {
  it("carries code and retryable fields", () => {
    const err = new SidecarError("not found", "table_not_found", false);
    expect(err.message).toBe("not found");
    expect(err.code).toBe("table_not_found");
    expect(err.retryable).toBe(false);
    expect(err.name).toBe("SidecarError");
    expect(err).toBeInstanceOf(Error);
  });

  it("defaults retryable to false", () => {
    const err = new SidecarError("oops", "internal");
    expect(err.retryable).toBe(false);
  });

  it("handles undefined code", () => {
    const err = new SidecarError("unknown");
    expect(err.code).toBeUndefined();
    expect(err.retryable).toBe(false);
  });
});

describe("ResultPayload type narrowing", () => {
  it("narrows DataResult by type field", () => {
    const payload = {
      type: "data" as const,
      schema: [{ name: "id", data_type: "Int64", nullable: false }],
      rows: [{ id: 1 }],
      total_rows: 1,
      offset: 0,
      limit: 1000,
    };

    // Type narrowing via discriminated union
    if (payload.type === "data") {
      expect(payload.schema).toHaveLength(1);
      expect(payload.rows).toHaveLength(1);
    }
  });

  it("narrows HostToWebviewMessage types", () => {
    const messages: HostToWebviewMessage[] = [
      { type: "init", fileType: "delta" },
      { type: "error", message: "fail", code: "internal", retryable: true },
      {
        type: "data",
        schema: [],
        rows: [],
        total_rows: 0,
        offset: 0,
        limit: 1000,
      },
      { type: "history", entries: [] },
      {
        type: "table_info",
        name: null,
        description: null,
        location: "/tmp",
        current_version: 0,
        created_time: null,
        min_reader_version: 1,
        min_writer_version: 2,
        cdf_enabled: false,
        partition_columns: [],
        num_files: 0,
        total_size_bytes: 0,
        configuration: {},
        id: "test-id",
        format_provider: "parquet",
        format_options: {},
        reader_features: null,
        writer_features: null,
      },
    ];

    for (const msg of messages) {
      switch (msg.type) {
        case "init":
          expect(msg.fileType).toBe("delta");
          break;
        case "error":
          expect(msg.message).toBe("fail");
          expect(msg.code).toBe("internal");
          expect(msg.retryable).toBe(true);
          break;
        case "data":
          expect(msg.total_rows).toBe(0);
          break;
        case "history":
          expect(msg.entries).toHaveLength(0);
          break;
        case "table_info":
          expect(msg.location).toBe("/tmp");
          break;
      }
    }
  });
});
