import * as vscode from "vscode";
import { Sidecar, findBinary } from "./sidecar";
import { registerCommands } from "./commands";
import { DeltaDetector } from "./deltaDetector";
import { ParquetEditorProvider } from "./webview/webviewPanel";

let sidecar: Sidecar | undefined;

export function activate(context: vscode.ExtensionContext): void {
  const binaryPath = findBinary(context);
  sidecar = new Sidecar(binaryPath);
  context.subscriptions.push(sidecar);

  // Register commands
  registerCommands(context, sidecar);

  // Register custom editor for .parquet files
  context.subscriptions.push(
    vscode.window.registerCustomEditorProvider(
      "deltaViewer.parquet",
      new ParquetEditorProvider(context, sidecar),
    ),
  );

  // Start delta table detector and register as file decoration provider
  const detector = new DeltaDetector();
  context.subscriptions.push(detector);
  context.subscriptions.push(
    vscode.window.registerFileDecorationProvider(detector),
  );
}

export function deactivate(): void {
  sidecar?.dispose();
}
