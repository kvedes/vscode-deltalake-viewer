import * as vscode from "vscode";
import { Sidecar } from "../sidecar";
import { getWebviewHtml } from "./getHtml";
import { SidecarError } from "../protocol";
import type { WebviewToHostMessage } from "../shared/types";
import { findDeltaTableRoot } from "../deltaDetector";

export class DeltaViewerPanel implements vscode.Disposable {
  private static panels = new Map<string, DeltaViewerPanel>();

  private panel: vscode.WebviewPanel;
  private disposables: vscode.Disposable[] = [];
  private readonly pageSize = 1000;
  private currentVersion: number | undefined;
  private knownTotal: number | undefined;
  private cdfMode = false;
  private activeRequestIds = new Set<string>();

  private constructor(
    private context: vscode.ExtensionContext,
    private sidecar: Sidecar,
    private filePath: string,
    private fileType: "parquet" | "delta",
  ) {
    const title = filePath.split("/").pop() || "Delta Viewer";

    this.panel = vscode.window.createWebviewPanel(
      "deltaViewer",
      title,
      vscode.ViewColumn.One,
      {
        enableScripts: true,
        retainContextWhenHidden: true,
        localResourceRoots: [
          vscode.Uri.joinPath(context.extensionUri, "out"),
          vscode.Uri.joinPath(context.extensionUri, "webview-ui"),
        ],
      },
    );

    this.panel.webview.html = getWebviewHtml(
      this.panel.webview,
      context.extensionUri,
    );

    this.panel.webview.onDidReceiveMessage(
      (msg) => this.onMessage(msg),
      null,
      this.disposables,
    );

    this.panel.onDidDispose(() => this.dispose(), null, this.disposables);

    // Send init message with file type before loading data
    this.panel.webview.postMessage({
      type: "init",
      fileType: this.fileType,
    });

    this.loadData();

    // For delta tables, fetch history and table info in parallel
    if (this.fileType === "delta") {
      this.fetchHistoryAndInfo();
    }
  }

  static createOrShow(
    context: vscode.ExtensionContext,
    sidecar: Sidecar,
    filePath: string,
    fileType: "parquet" | "delta",
  ): DeltaViewerPanel {
    const cacheKey = `${filePath}:${fileType}`;
    const existing = DeltaViewerPanel.panels.get(cacheKey);
    if (existing) {
      existing.panel.reveal();
      return existing;
    }

    const instance = new DeltaViewerPanel(
      context,
      sidecar,
      filePath,
      fileType,
    );
    DeltaViewerPanel.panels.set(cacheKey, instance);
    return instance;
  }

  private async loadData(offset = 0, version?: number): Promise<void> {
    // Cancel previous in-flight requests for this panel only
    this.sidecar.cancelMany(this.activeRequestIds);
    this.activeRequestIds.clear();

    try {
      // On version change, reset cached total
      if (version !== this.currentVersion) {
        this.knownTotal = undefined;
      }

      const streamCallbacks = {
        onHeader: (header: import("../protocol").DataHeaderResult) => {
          this.knownTotal = header.total_rows;
          this.panel.webview.postMessage({
            type: "data_header",
            schema: header.schema,
            total_rows: header.total_rows,
            offset: header.offset,
            version: this.currentVersion,
          });
        },
        onChunk: (chunk: import("../protocol").DataChunkResult) => {
          this.panel.webview.postMessage({
            type: "data_chunk",
            rows: chunk.rows,
            chunk_index: chunk.chunk_index,
          });
        },
        onDone: () => {
          this.panel.webview.postMessage({ type: "data_done" });
        },
      };

      let stream: { requestId: string; done: Promise<void> };
      if (this.fileType === "parquet") {
        stream = this.sidecar.readParquetStreaming(
          this.filePath, offset, this.pageSize, streamCallbacks,
        );
      } else {
        stream = this.sidecar.readDeltaStreaming(
          this.filePath, offset, this.pageSize, version,
          this.knownTotal, streamCallbacks,
        );
      }
      this.activeRequestIds.add(stream.requestId);
      await stream.done;
      this.activeRequestIds.delete(stream.requestId);
    } catch (err: unknown) {
      if (err instanceof SidecarError && err.message === "Request cancelled") {
        return; // Silently ignore cancelled requests
      }
      const message = err instanceof Error ? err.message : String(err);
      const code = err instanceof SidecarError ? err.code : undefined;
      const retryable = err instanceof SidecarError ? err.retryable : true; // default retryable for unknown errors
      this.panel.webview.postMessage({
        type: "error",
        message,
        code,
        retryable,
      });
    }
  }

  private async fetchHistoryAndInfo(): Promise<void> {
    try {
      const [history, tableInfo] = await Promise.all([
        this.sidecar.getHistory(this.filePath),
        this.sidecar.getTableInfo(this.filePath),
      ]);
      this.panel.webview.postMessage({
        type: "history",
        entries: history.entries,
      });
      if (this.currentVersion === undefined) {
        this.currentVersion = tableInfo.current_version;
      }
      const { type: _t, ...tableInfoData } = tableInfo;
      this.panel.webview.postMessage({
        type: "table_info",
        ...tableInfoData,
      });
    } catch (err: unknown) {
      console.error("Failed to fetch history/info:", err);
    }
  }

  private onMessage(msg: WebviewToHostMessage): void {
    switch (msg.type) {
      case "ready":
        this.cdfMode = false;
        this.loadData();
        break;
      case "page":
        if (this.cdfMode && this.currentVersion !== undefined) {
          this.loadCdf(this.currentVersion, msg.offset);
        } else {
          this.loadData(msg.offset, this.currentVersion);
        }
        break;
      case "load_version":
        this.cdfMode = false;
        this.currentVersion = msg.version;
        this.loadData(0, this.currentVersion);
        break;
      case "request_history":
        this.sidecar.getHistory(this.filePath).then((result) => {
          this.panel.webview.postMessage({
            type: "history",
            entries: result.entries,
          });
        });
        break;
      case "request_table_info":
        this.sidecar.getTableInfo(this.filePath).then((result) => {
          const { type: _t, ...data } = result;
          this.panel.webview.postMessage({
            type: "table_info",
            ...data,
          });
        });
        break;
      case "load_cdf":
        this.cdfMode = true;
        this.loadCdf(msg.version, msg.offset);
        break;
    }
  }

  private async loadCdf(version: number, offset = 0): Promise<void> {
    this.sidecar.cancelMany(this.activeRequestIds);
    this.activeRequestIds.clear();

    const startVersion = Math.max(0, version - 1);
    const endVersion = version;
    this.currentVersion = version;

    try {
      const stream = this.sidecar.readCdfStreaming(
        this.filePath, startVersion, endVersion, offset, this.pageSize,
        {
          onHeader: (header: import("../protocol").DataHeaderResult) => {
            this.panel.webview.postMessage({
              type: "data_header",
              schema: header.schema,
              total_rows: header.total_rows,
              offset: header.offset,
              version: this.currentVersion,
              cdf_mode: true,
              cdf_counts: header.cdf_counts,
            });
          },
          onChunk: (chunk: import("../protocol").DataChunkResult) => {
            this.panel.webview.postMessage({
              type: "data_chunk",
              rows: chunk.rows,
              chunk_index: chunk.chunk_index,
            });
          },
          onDone: () => {
            this.panel.webview.postMessage({ type: "data_done" });
          },
        },
      );
      this.activeRequestIds.add(stream.requestId);
      await stream.done;
      this.activeRequestIds.delete(stream.requestId);
    } catch (err: unknown) {
      if (err instanceof SidecarError && err.message === "Request cancelled") {
        return;
      }
      const message = err instanceof Error ? err.message : String(err);
      const code = err instanceof SidecarError ? err.code : undefined;
      const retryable = err instanceof SidecarError ? err.retryable : true;
      this.panel.webview.postMessage({
        type: "error",
        message,
        code,
        retryable,
      });
    }
  }

  dispose(): void {
    this.sidecar.cancelMany(this.activeRequestIds);
    this.activeRequestIds.clear();
    DeltaViewerPanel.panels.delete(`${this.filePath}:${this.fileType}`);
    this.panel.dispose();
    for (const d of this.disposables) {
      d.dispose();
    }
  }
}

export class ParquetEditorProvider
  implements vscode.CustomReadonlyEditorProvider
{
  constructor(
    private context: vscode.ExtensionContext,
    private sidecar: Sidecar,
  ) {}

  openCustomDocument(uri: vscode.Uri): vscode.CustomDocument {
    return { uri, dispose: () => {} };
  }

  async resolveCustomEditor(
    document: vscode.CustomDocument,
    webviewPanel: vscode.WebviewPanel,
  ): Promise<void> {
    const fsPath = document.uri.fsPath;
    const deltaRoot = await findDeltaTableRoot(fsPath);

    if (deltaRoot) {
      DeltaViewerPanel.createOrShow(
        this.context,
        this.sidecar,
        deltaRoot,
        "delta",
      );
    } else {
      DeltaViewerPanel.createOrShow(
        this.context,
        this.sidecar,
        fsPath,
        "parquet",
      );
    }
    // Close the auto-opened panel since createOrShow creates its own
    webviewPanel.dispose();
  }
}
