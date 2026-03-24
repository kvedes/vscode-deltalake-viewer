import { describe, it, expect, vi, beforeEach } from "vitest";

// Mock vscode module
vi.mock("vscode", () => ({
  window: {
    showErrorMessage: vi.fn(),
  },
  workspace: {
    getConfiguration: vi.fn(() => ({
      get: vi.fn(),
    })),
  },
}));

// Mock child_process
const mockStdin = {
  write: vi.fn((_data: string, cb?: (err?: Error) => void) => {
    if (cb) cb();
    return true;
  }),
};

const mockStdout = {
  on: vi.fn(),
};

const mockStderr = {
  on: vi.fn(),
};

const mockProcess = {
  stdin: mockStdin,
  stdout: mockStdout,
  stderr: mockStderr,
  exitCode: null as number | null,
  on: vi.fn(),
  kill: vi.fn(),
};

vi.mock("child_process", () => ({
  spawn: vi.fn(() => mockProcess),
}));

// Mock readline
let lineCallback: ((line: string) => void) | null = null;
vi.mock("readline", () => ({
  createInterface: vi.fn(() => ({
    on: vi.fn((event: string, cb: (line: string) => void) => {
      if (event === "line") {
        lineCallback = cb;
      }
    }),
    close: vi.fn(),
  })),
}));

import { Sidecar } from "../sidecar";

describe("Sidecar", () => {
  let sidecar: Sidecar;

  beforeEach(() => {
    vi.clearAllMocks();
    lineCallback = null;
    mockProcess.exitCode = null;
    sidecar = new Sidecar("/path/to/binary");
  });

  it("send() resolves with result on success", async () => {
    const promise = sidecar.send("read_parquet", { path: "/test.parquet" });

    // Simulate response
    expect(lineCallback).not.toBeNull();
    lineCallback!(
      JSON.stringify({
        id: "1",
        result: {
          type: "data",
          schema: [],
          rows: [],
          total_rows: 0,
          offset: 0,
          limit: 1000,
        },
      }),
    );

    const result = await promise;
    expect(result.type).toBe("data");
  });

  it("send() rejects with SidecarError on error response", async () => {
    const promise = sidecar.send("read_delta", { path: "/bad" });

    lineCallback!(
      JSON.stringify({
        id: "1",
        error: "table not found",
        code: "table_not_found",
        retryable: false,
      }),
    );

    await expect(promise).rejects.toThrow("table not found");
    await expect(promise).rejects.toMatchObject({
      name: "SidecarError",
      code: "table_not_found",
      retryable: false,
    });
  });

  it("readDelta passes version parameter", async () => {
    const promise = sidecar.readDelta("/delta", 0, 100, 5);

    // Check that the written request includes version
    const writeCall = mockStdin.write.mock.calls[0][0] as string;
    const request = JSON.parse(writeCall.trim());
    expect(request.command).toBe("read_delta");
    expect(request.params.version).toBe(5);

    // Resolve the promise
    lineCallback!(
      JSON.stringify({
        id: "1",
        result: {
          type: "data",
          schema: [],
          rows: [],
          total_rows: 0,
          offset: 0,
          limit: 100,
        },
      }),
    );

    await promise;
  });

  it("getHistory calls correct command", async () => {
    const promise = sidecar.getHistory("/delta");

    const writeCall = mockStdin.write.mock.calls[0][0] as string;
    const request = JSON.parse(writeCall.trim());
    expect(request.command).toBe("get_history");
    expect(request.params.path).toBe("/delta");

    lineCallback!(
      JSON.stringify({
        id: "1",
        result: { type: "history", entries: [] },
      }),
    );

    const result = await promise;
    expect(result.type).toBe("history");
  });

  it("getTableInfo calls correct command", async () => {
    const promise = sidecar.getTableInfo("/delta");

    const writeCall = mockStdin.write.mock.calls[0][0] as string;
    const request = JSON.parse(writeCall.trim());
    expect(request.command).toBe("get_table_info");

    lineCallback!(
      JSON.stringify({
        id: "1",
        result: {
          type: "table_info",
          name: null,
          description: null,
          location: "/delta",
          current_version: 0,
          created_time: null,
          min_reader_version: 1,
          min_writer_version: 2,
          cdf_enabled: false,
          partition_columns: [],
          num_files: 1,
          total_size_bytes: 1024,
          configuration: {},
        },
      }),
    );

    const result = await promise;
    expect(result.type).toBe("table_info");
  });
});
