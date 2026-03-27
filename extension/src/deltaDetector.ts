import * as vscode from "vscode";
import * as path from "path";

export async function findDeltaTableRoot(filePath: string): Promise<string | undefined> {
  const workspaceFolders = vscode.workspace.workspaceFolders;
  const roots = workspaceFolders
    ? workspaceFolders.map((f) => f.uri.fsPath)
    : [];

  let dir = path.dirname(filePath);
  // eslint-disable-next-line no-constant-condition
  while (true) {
    const deltaLogUri = vscode.Uri.file(path.join(dir, "_delta_log"));
    try {
      const stat = await vscode.workspace.fs.stat(deltaLogUri);
      if (stat.type & vscode.FileType.Directory) {
        return dir;
      }
    } catch {
      // _delta_log doesn't exist at this level, keep walking up
    }

    const parent = path.dirname(dir);
    if (parent === dir) {
      break; // filesystem root
    }
    if (roots.length > 0 && roots.some((r) => dir === r)) {
      break; // workspace root
    }
    dir = parent;
  }
  return undefined;
}

export class DeltaDetector implements vscode.Disposable, vscode.FileDecorationProvider {
  private watcher: vscode.FileSystemWatcher;
  private knownTables = new Set<string>();

  private _onDidChangeFileDecorations = new vscode.EventEmitter<vscode.Uri | vscode.Uri[]>();
  readonly onDidChangeFileDecorations = this._onDidChangeFileDecorations.event;

  constructor() {
    this.watcher = vscode.workspace.createFileSystemWatcher(
      "**/_delta_log/*.json",
    );

    this.watcher.onDidCreate((uri) => this.onDeltaLogFile(uri));
    this.watcher.onDidChange((uri) => this.onDeltaLogFile(uri));

    // Scan workspace for existing delta tables
    this.scanWorkspace();
  }

  private async scanWorkspace(): Promise<void> {
    const deltaLogs = await vscode.workspace.findFiles("**/_delta_log/*.json", null, 1000);
    for (const uri of deltaLogs) {
      this.onDeltaLogFile(uri);
    }
  }

  private onDeltaLogFile(uri: vscode.Uri): void {
    // The Delta table root is two levels up from _delta_log/00000.json
    const parts = uri.fsPath.split(path.sep);
    const deltaLogIdx = parts.lastIndexOf("_delta_log");
    if (deltaLogIdx > 0) {
      const tableRoot = parts.slice(0, deltaLogIdx).join(path.sep);
      if (!this.knownTables.has(tableRoot)) {
        this.knownTables.add(tableRoot);
        this._onDidChangeFileDecorations.fire(vscode.Uri.file(tableRoot));
      }
    }
  }

  provideFileDecoration(uri: vscode.Uri): vscode.FileDecoration | undefined {
    if (this.knownTables.has(uri.fsPath)) {
      return {
        badge: "Δ",
        color: new vscode.ThemeColor("charts.green"),
        tooltip: "Delta Lake table",
        propagate: false,
      };
    }
    return undefined;
  }

  getKnownTables(): string[] {
    return [...this.knownTables];
  }

  dispose(): void {
    this.watcher.dispose();
    this._onDidChangeFileDecorations.dispose();
  }
}
