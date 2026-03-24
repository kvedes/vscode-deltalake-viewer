import * as vscode from "vscode";
import { Sidecar } from "./sidecar";
import { DeltaViewerPanel } from "./webview/webviewPanel";

export function registerCommands(
  context: vscode.ExtensionContext,
  sidecar: Sidecar,
): void {
  context.subscriptions.push(
    vscode.commands.registerCommand(
      "deltaViewer.openParquet",
      async (uri?: vscode.Uri) => {
        if (!uri) {
          const uris = await vscode.window.showOpenDialog({
            filters: { "Parquet files": ["parquet"] },
            canSelectMany: false,
          });
          if (!uris || uris.length === 0) return;
          uri = uris[0];
        }
        DeltaViewerPanel.createOrShow(
          context,
          sidecar,
          uri.fsPath,
          "parquet",
        );
      },
    ),
  );

  context.subscriptions.push(
    vscode.commands.registerCommand(
      "deltaViewer.openDelta",
      async (uri?: vscode.Uri) => {
        if (!uri) {
          const uris = await vscode.window.showOpenDialog({
            canSelectFolders: true,
            canSelectFiles: false,
            canSelectMany: false,
            openLabel: "Open Delta Table",
          });
          if (!uris || uris.length === 0) return;
          uri = uris[0];
        }
        // Verify this is a Delta table before opening
        const deltaLogUri = vscode.Uri.joinPath(uri, "_delta_log");
        try {
          const stat = await vscode.workspace.fs.stat(deltaLogUri);
          if (!(stat.type & vscode.FileType.Directory)) {
            vscode.window.showWarningMessage("Not a Delta Lake table");
            return;
          }
        } catch {
          vscode.window.showWarningMessage("Not a Delta Lake table");
          return;
        }
        DeltaViewerPanel.createOrShow(context, sidecar, uri.fsPath, "delta");
      },
    ),
  );

  context.subscriptions.push(
    vscode.commands.registerCommand(
      "deltaViewer.openRawParquet",
      async (uri?: vscode.Uri) => {
        if (!uri) {
          const uris = await vscode.window.showOpenDialog({
            filters: { "Parquet files": ["parquet"] },
            canSelectMany: false,
          });
          if (!uris || uris.length === 0) return;
          uri = uris[0];
        }
        DeltaViewerPanel.createOrShow(
          context,
          sidecar,
          uri.fsPath,
          "parquet",
        );
      },
    ),
  );
}
