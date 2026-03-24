import * as crypto from "crypto";
import * as vscode from "vscode";

export function getWebviewHtml(
  webview: vscode.Webview,
  extensionUri: vscode.Uri,
): string {
  const scriptUri = webview.asWebviewUri(
    vscode.Uri.joinPath(extensionUri, "out", "webview.js"),
  );
  const styleUri = webview.asWebviewUri(
    vscode.Uri.joinPath(extensionUri, "webview-ui", "style.css"),
  );

  const nonce = getNonce();

  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta http-equiv="Content-Security-Policy"
    content="default-src 'none';
             style-src ${webview.cspSource} 'unsafe-inline';
             script-src 'nonce-${nonce}';">
  <link rel="stylesheet" href="${styleUri}">
  <title>Delta Viewer</title>
</head>
<body>
  <div id="app">
    <div id="toolbar">
      <div style="display:flex;align-items:center;gap:8px">
        <span id="format-pill"></span>
        <div id="toolbar-actions" hidden>
          <button class="toolbar-btn" id="history-btn">History</button>
          <button class="toolbar-btn" id="info-btn">Info</button>
        </div>
      </div>
      <span id="status">Loading...</span>
    </div>
    <div id="main-content">
      <div id="sidebar" hidden>
        <div class="sidebar-header"><span id="sidebar-title"></span><button class="toolbar-btn" id="changes-btn" hidden>Changes</button></div>
        <div id="cdf-legend" hidden>
          <span class="legend-item"><span class="legend-swatch legend-insert"></span>Insert</span>
          <span class="legend-item"><span class="legend-swatch legend-update"></span>Update</span>
          <span class="legend-item"><span class="legend-swatch legend-delete"></span>Delete</span>
        </div>
        <div id="history-list"></div>
        <div id="info-panel"></div>
        <div id="sidebar-resize-handle"></div>
      </div>
      <div id="table-container">
        <div id="table-header">
          <table class="header-table">
            <thead><tr id="header-row"></tr></thead>
          </table>
        </div>
        <div id="scroll-viewport">
          <div id="scroll-spacer"></div>
          <table class="body-table">
            <tbody id="body-rows"></tbody>
          </table>
        </div>
        <div id="loading-indicator" hidden>Loading...</div>
      </div>
    </div>
  </div>
  <script nonce="${nonce}" src="${scriptUri}"></script>
</body>
</html>`;
}

function getNonce(): string {
  return crypto.randomBytes(16).toString("base64url");
}
