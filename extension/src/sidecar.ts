import { ChildProcess, spawn } from "child_process";
import { createInterface, Interface } from "readline";
import * as path from "path";
import * as vscode from "vscode";
import {
  SidecarRequest,
  SidecarResponse,
  SidecarError,
  ResultPayload,
  DataResult,
  DataHeaderResult,
  DataChunkResult,
  DataDoneResult,
  HistoryResult,
  TableInfoResult,
} from "./protocol";
import { existsSync } from "fs";

interface StreamingEntry {
  onHeader: (result: DataHeaderResult) => void;
  onChunk: (result: DataChunkResult) => void;
  onDone: (result: DataDoneResult) => void;
  onError: (error: SidecarError) => void;
  timer?: NodeJS.Timeout;
}

export class Sidecar implements vscode.Disposable {
  private process: ChildProcess | null = null;
  private readline: Interface | null = null;
  private pending = new Map<
    string,
    {
      resolve: (value: ResultPayload) => void;
      reject: (reason: Error) => void;
    }
  >();
  private streaming = new Map<string, StreamingEntry>();
  private counter = 0;
  private readonly requestTimeout: number;
  private healthCheckInterval: NodeJS.Timeout | null = null;
  private restartCount = 0;
  private readonly MAX_RESTARTS = 3;

  constructor(private binaryPath: string) {
    const config = vscode.workspace.getConfiguration("deltaViewer");
    this.requestTimeout = config.get<number>("requestTimeout", 30_000);
  }

  private ensureRunning(): void {
    if (this.process && this.process.exitCode === null) {
      return;
    }

    this.process = spawn(this.binaryPath, [], {
      stdio: ["pipe", "pipe", "pipe"],
    });

    this.process.on("error", (err) => {
      vscode.window.showErrorMessage(
        `Delta Viewer sidecar error: ${err.message}`,
      );
      this.rejectAll(err);
    });

    this.process.on("exit", (code) => {
      if (code !== 0 && code !== null) {
        this.rejectAll(new Error(`Sidecar exited with code ${code}`));
      }
      this.process = null;
      this.readline = null;
    });

    this.readline = createInterface({ input: this.process.stdout! });
    this.readline.on("line", (line: string) => {
      try {
        const response: SidecarResponse = JSON.parse(line);
        this.restartCount = 0; // Healthy response, reset counter

        // Check streaming first
        const stream = this.streaming.get(response.id);
        if (stream) {
          if (response.error) {
            console.debug(`[delta-viewer] Streaming error id=${response.id}: ${response.error}`);
            this.streaming.delete(response.id);
            if (stream.timer) clearTimeout(stream.timer);
            stream.onError(new SidecarError(response.error, response.code, response.retryable));
            return;
          }
          const result = response.result!;
          if (result.type === "data_header") {
            console.debug(`[delta-viewer] Received data_header id=${response.id} rows=${(result as DataHeaderResult).total_rows}`);
            stream.onHeader(result as DataHeaderResult);
          } else if (result.type === "data_chunk") {
            console.debug(`[delta-viewer] Received data_chunk id=${response.id} chunk_index=${(result as DataChunkResult).chunk_index}`);
            stream.onChunk(result as DataChunkResult);
          } else if (result.type === "data_done") {
            console.debug(`[delta-viewer] Received data_done id=${response.id}`);
            this.streaming.delete(response.id);
            if (stream.timer) clearTimeout(stream.timer);
            stream.onDone(result as DataDoneResult);
          }
          return;
        }

        // Regular (non-streaming) response
        const entry = this.pending.get(response.id);
        if (entry) {
          this.pending.delete(response.id);
          if (response.error) {
            entry.reject(
              new SidecarError(response.error, response.code, response.retryable),
            );
          } else if (response.result) {
            entry.resolve(response.result);
          }
        } else {
          console.warn(`[delta-viewer] Received response for unknown request id=${response.id}. Active requests: ${Array.from(this.pending.keys()).join(", ") || "none"}`);
        }
      } catch (err) {
        // Log malformed lines to help debug issues
        const errorMsg = err instanceof Error ? err.message : String(err);
        console.error(`[delta-viewer] Failed to parse sidecar response: ${errorMsg}`, { line });
      }
    });

    this.process.stderr?.on("data", (data: Buffer) => {
      console.log(`[delta-viewer sidecar] ${data.toString().trim()}`);
    });

    if (!this.healthCheckInterval) {
      this.startHealthCheck();
    }
  }

  private startHealthCheck(): void {
    this.healthCheckInterval = setInterval(async () => {
      if (!this.process || this.process.exitCode !== null) {
        this.attemptRestart("process exited");
        return;
      }

      try {
        await this.send("ping", {});
      } catch {
        this.attemptRestart("ping failed");
      }
    }, 10_000);
  }

  private attemptRestart(reason: string): void {
    if (this.restartCount >= this.MAX_RESTARTS) {
      vscode.window.showErrorMessage(
        `Delta Viewer sidecar failed to restart after ${this.MAX_RESTARTS} attempts. Please reload the window.`,
      );
      return;
    }

    console.log(`[delta-viewer] Restarting sidecar: ${reason}`);
    this.restartCount++;

    // Kill existing process
    this.process?.kill();
    this.process = null;
    this.readline = null;

    // Reject all pending requests as retryable
    for (const [, entry] of this.pending) {
      entry.reject(new SidecarError(
        `Sidecar restarted: ${reason}`,
        "internal",
        true,
      ));
    }
    this.pending.clear();

    // Also reject streaming requests
    for (const [, entry] of this.streaming) {
      if (entry.timer) clearTimeout(entry.timer);
      entry.onError(new SidecarError(
        `Sidecar restarted: ${reason}`,
        "internal",
        true,
      ));
    }
    this.streaming.clear();

    // Restart
    try {
      this.ensureRunning();
    } catch (e) {
      console.error("[delta-viewer] Failed to restart sidecar:", e);
    }
  }

  async send(
    command: string,
    params: Record<string, unknown>,
  ): Promise<ResultPayload & { requestId: string }> {
    this.ensureRunning();

    const id = String(++this.counter);
    const request: SidecarRequest = { id, command, params };

    const result = await new Promise<ResultPayload>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new SidecarError(
          `Request ${command} timed out after ${this.requestTimeout}ms`,
          "internal",
          true,
        ));
      }, this.requestTimeout);

      this.pending.set(id, {
        resolve: (value) => { clearTimeout(timer); resolve(value); },
        reject: (reason) => { clearTimeout(timer); reject(reason); },
      });

      const line = JSON.stringify(request) + "\n";
      this.process!.stdin!.write(line, (err) => {
        if (err) {
          clearTimeout(timer);
          this.pending.delete(id);
          reject(err);
        }
      });
    });

    return Object.assign(result, { requestId: id });
  }

  cancel(id: string): void {
    const entry = this.pending.get(id);
    if (entry) {
      this.pending.delete(id);
      entry.reject(new SidecarError("Request cancelled", "internal", false));
    }
    const stream = this.streaming.get(id);
    if (stream) {
      this.streaming.delete(id);
      if (stream.timer) clearTimeout(stream.timer);
      stream.onError(new SidecarError("Request cancelled", "internal", false));
    }
  }

  cancelMany(ids: Set<string>): void {
    for (const id of ids) {
      this.cancel(id);
    }
  }

  cancelAll(): void {
    for (const [, entry] of this.pending) {
      entry.reject(new SidecarError("Request cancelled", "internal", false));
    }
    this.pending.clear();
    for (const [, entry] of this.streaming) {
      if (entry.timer) clearTimeout(entry.timer);
      entry.onError(new SidecarError("Request cancelled", "internal", false));
    }
    this.streaming.clear();
  }

  async readParquet(
    filePath: string,
    offset = 0,
    limit = 1000,
  ): Promise<DataResult> {
    return this.send("read_parquet", { path: filePath, offset, limit }) as Promise<DataResult>;
  }

  async readDelta(
    dirPath: string,
    offset = 0,
    limit = 1000,
    version?: number,
    knownTotal?: number,
  ): Promise<DataResult> {
    const params: Record<string, unknown> = { path: dirPath, offset, limit };
    if (version !== undefined) {
      params.version = version;
    }
    if (knownTotal !== undefined) {
      params.known_total = knownTotal;
    }
    return this.send("read_delta", params) as Promise<DataResult>;
  }

  readDeltaStreaming(
    dirPath: string,
    offset: number,
    limit: number,
    version: number | undefined,
    knownTotal: number | undefined,
    callbacks: {
      onHeader: (result: DataHeaderResult) => void;
      onChunk: (result: DataChunkResult) => void;
      onDone: (result: DataDoneResult) => void;
    },
  ): { requestId: string; done: Promise<void> } {
    this.ensureRunning();

    const id = String(++this.counter);
    const params: Record<string, unknown> = { path: dirPath, offset, limit };
    if (version !== undefined) params.version = version;
    if (knownTotal !== undefined) params.known_total = knownTotal;

    const streamingTimeout = this.requestTimeout * 5;

    const done = new Promise<void>((resolve, reject) => {
      const resetTimer = (): NodeJS.Timeout =>
        setTimeout(() => {
          this.streaming.delete(id);
          reject(new SidecarError("Streaming request timed out", "internal", true));
        }, streamingTimeout);

      let timer = resetTimer();

      this.streaming.set(id, {
        onHeader: (result) => { clearTimeout(timer); timer = resetTimer(); callbacks.onHeader(result); },
        onChunk: (result) => { clearTimeout(timer); timer = resetTimer(); callbacks.onChunk(result); },
        onDone: (result) => { clearTimeout(timer); callbacks.onDone(result); resolve(); },
        onError: (err) => { clearTimeout(timer); reject(err); },
        timer,
      });

      const request: SidecarRequest = { id, command: "read_delta", params };
      const line = JSON.stringify(request) + "\n";
      console.debug(`[delta-viewer] Sending request id=${id} read_delta offset=${offset} limit=${limit}`);
      this.process!.stdin!.write(line, (err) => {
        if (err) {
          clearTimeout(timer);
          this.streaming.delete(id);
          reject(err);
        }
      });
    });

    return { requestId: id, done };
  }

  readCdfStreaming(
    dirPath: string,
    startVersion: number,
    endVersion: number,
    offset: number,
    limit: number,
    callbacks: {
      onHeader: (result: DataHeaderResult) => void;
      onChunk: (result: DataChunkResult) => void;
      onDone: (result: DataDoneResult) => void;
    },
  ): { requestId: string; done: Promise<void> } {
    this.ensureRunning();

    const id = String(++this.counter);
    const params: Record<string, unknown> = {
      path: dirPath,
      start_version: startVersion,
      end_version: endVersion,
      offset,
      limit,
    };

    const streamingTimeout = this.requestTimeout * 5;

    const done = new Promise<void>((resolve, reject) => {
      const resetTimer = (): NodeJS.Timeout =>
        setTimeout(() => {
          this.streaming.delete(id);
          reject(new SidecarError("Streaming request timed out", "internal", true));
        }, streamingTimeout);

      let timer = resetTimer();

      this.streaming.set(id, {
        onHeader: (result) => { clearTimeout(timer); timer = resetTimer(); callbacks.onHeader(result); },
        onChunk: (result) => { clearTimeout(timer); timer = resetTimer(); callbacks.onChunk(result); },
        onDone: (result) => { clearTimeout(timer); callbacks.onDone(result); resolve(); },
        onError: (err) => { clearTimeout(timer); reject(err); },
        timer,
      });

      const request: SidecarRequest = { id, command: "read_cdf", params };
      const line = JSON.stringify(request) + "\n";
      this.process!.stdin!.write(line, (err) => {
        if (err) {
          clearTimeout(timer);
          this.streaming.delete(id);
          reject(err);
        }
      });
    });

    return { requestId: id, done };
  }

  readParquetStreaming(
    filePath: string,
    offset: number,
    limit: number,
    callbacks: {
      onHeader: (result: DataHeaderResult) => void;
      onChunk: (result: DataChunkResult) => void;
      onDone: (result: DataDoneResult) => void;
    },
  ): { requestId: string; done: Promise<void> } {
    this.ensureRunning();

    const id = String(++this.counter);
    const params: Record<string, unknown> = { path: filePath, offset, limit };

    const streamingTimeout = this.requestTimeout * 5;

    const done = new Promise<void>((resolve, reject) => {
      const resetTimer = (): NodeJS.Timeout =>
        setTimeout(() => {
          this.streaming.delete(id);
          reject(new SidecarError("Streaming request timed out", "internal", true));
        }, streamingTimeout);

      let timer = resetTimer();

      this.streaming.set(id, {
        onHeader: (result) => { clearTimeout(timer); timer = resetTimer(); callbacks.onHeader(result); },
        onChunk: (result) => { clearTimeout(timer); timer = resetTimer(); callbacks.onChunk(result); },
        onDone: (result) => { clearTimeout(timer); callbacks.onDone(result); resolve(); },
        onError: (err) => { clearTimeout(timer); reject(err); },
        timer,
      });

      const request: SidecarRequest = { id, command: "read_parquet", params };
      const line = JSON.stringify(request) + "\n";
      this.process!.stdin!.write(line, (err) => {
        if (err) {
          clearTimeout(timer);
          this.streaming.delete(id);
          reject(err);
        }
      });
    });

    return { requestId: id, done };
  }

  async refreshTable(dirPath: string): Promise<void> {
    await this.send("refresh_table", { path: dirPath });
  }

  async getSchema(filePath: string): Promise<DataResult> {
    return this.send("get_schema", { path: filePath }) as Promise<DataResult>;
  }

  async getHistory(dirPath: string): Promise<HistoryResult> {
    return this.send("get_history", { path: dirPath }) as Promise<HistoryResult>;
  }

  async getTableInfo(dirPath: string): Promise<TableInfoResult> {
    return this.send("get_table_info", { path: dirPath }) as Promise<TableInfoResult>;
  }

  private rejectAll(err: Error): void {
    for (const [, entry] of this.pending) {
      entry.reject(err);
    }
    this.pending.clear();
    for (const [, entry] of this.streaming) {
      if (entry.timer) clearTimeout(entry.timer);
      entry.onError(err instanceof SidecarError ? err : new SidecarError(err.message, "internal", true));
    }
    this.streaming.clear();
  }

  dispose(): void {
    if (this.healthCheckInterval) {
      clearInterval(this.healthCheckInterval);
      this.healthCheckInterval = null;
    }

    this.cancelAll();

    if (this.process && this.process.exitCode === null) {
      const line = JSON.stringify({
        id: "shutdown",
        command: "shutdown",
        params: {},
      });
      this.process.stdin?.write(line + "\n");
      setTimeout(() => {
        this.process?.kill();
      }, 1000);
    }
    this.readline?.close();
  }
}

export function findBinary(context: vscode.ExtensionContext): string {
  // 1. Check user configuration
  const config = vscode.workspace.getConfiguration("deltaViewer");
  const configured = config.get<string>("sidecarPath");
  if (configured) {
    return configured;
  }

  // 2. Check for bundled binary (from release build)
  const platform = process.platform; // linux, darwin, win32
  const arch = process.arch; // x64, arm64
  const ext = platform === "win32" ? ".exe" : "";
  const bundledPath = path.join(
    context.extensionPath, "bin", `${platform}-${arch}`, `delta-vscode${ext}`,
  );

  if (existsSync(bundledPath)) {
    return bundledPath;
  }

  // 3. Fallback: development build
  return path.join(
    context.extensionPath,
    "..",
    "target",
    "release",
    `delta-vscode${ext}`,
  );
}
