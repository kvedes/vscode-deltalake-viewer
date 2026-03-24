import type {
  ColumnDef,
  CdfCounts,
  HistoryEntry,
  HostToWebviewMessage,
  WebviewToHostMessage,
  DataMessage,
  TableInfoMessage,
  DataHeaderMessage,
  DataChunkMessage,
  DataDoneMessage,
} from "../src/shared/types";

declare function acquireVsCodeApi(): {
  postMessage(msg: unknown): void;
  getState(): unknown;
  setState(state: unknown): void;
};

const vscode = acquireVsCodeApi();

const formatPill = document.getElementById("format-pill")!;
const statusEl = document.getElementById("status")!;
let headerRow = document.getElementById("header-row")!;
let bodyRows = document.getElementById("body-rows")!;
let loadingIndicator = document.getElementById("loading-indicator")!;
const toolbarActions = document.getElementById("toolbar-actions")!;
const historyBtn = document.getElementById("history-btn")!;
const infoBtn = document.getElementById("info-btn")!;
const changesBtn = document.getElementById("changes-btn")!;
const sidebar = document.getElementById("sidebar")!;
const sidebarTitle = document.getElementById("sidebar-title")!;
const historyList = document.getElementById("history-list")!;
const infoPanel = document.getElementById("info-panel")!;
let scrollViewport = document.getElementById("scroll-viewport")!;
let scrollSpacer = document.getElementById("scroll-spacer")!;
let bodyTable = document.querySelector(".body-table") as HTMLTableElement;
let tableHeader = document.getElementById("table-header")!;
const mainContent = document.getElementById("main-content")!;
const resizeHandle = document.getElementById("sidebar-resize-handle")!;
const cdfLegend = document.getElementById("cdf-legend")!;

let activeTab: "history" | "info" | null = null;
let cachedColWidths: number[] = [];

function openSidebarTab(tab: "history" | "info"): void {
  if (activeTab === tab) {
    // Toggle off
    sidebar.hidden = true;
    activeTab = null;
    historyBtn.classList.remove("active");
    infoBtn.classList.remove("active");
    changesBtn.hidden = true;
    changesBtn.classList.remove("active");
    changesMode = false;
    cdfLegend.hidden = true;
    return;
  }

  activeTab = tab;
  sidebar.hidden = false;

  historyList.hidden = tab !== "history";
  infoPanel.hidden = tab !== "info";

  historyBtn.classList.toggle("active", tab === "history");
  infoBtn.classList.toggle("active", tab === "info");

  sidebarTitle.textContent = tab === "history" ? "Version History" : "Table Info";
  changesBtn.hidden = tab !== "history" || !changesBtn.dataset.enabled;
  cdfLegend.hidden = tab !== "history" || !changesMode;

  if (tab === "history" && !historyLoaded) {
    vscode.postMessage({ type: "request_history" } satisfies WebviewToHostMessage);
  }
  if (tab === "info" && !tableInfoLoaded) {
    vscode.postMessage({ type: "request_table_info" } satisfies WebviewToHostMessage);
  }
}

let allRows: Record<string, unknown>[] = [];
let totalRows = 0;
let currentLimit = 1000;
let isLoading = false;
let currentSchema: ColumnDef[] | null = null;
let currentVersion: number | undefined;
let historyLoaded = false;
let tableInfoLoaded = false;
let isDelta = false;
let changesMode = false;
let isCdfData = false;
let serverCdfCounts: CdfCounts | null = null;

function cdfBreakdown(): string {
  if (!serverCdfCounts) return "";
  const parts: string[] = [];
  if (serverCdfCounts.inserts > 0) parts.push(`${serverCdfCounts.inserts.toLocaleString()} inserts`);
  if (serverCdfCounts.updates > 0) parts.push(`${serverCdfCounts.updates.toLocaleString()} updates`);
  if (serverCdfCounts.deletes > 0) parts.push(`${serverCdfCounts.deletes.toLocaleString()} deletes`);
  return parts.length > 0 ? ` (${parts.join(", ")})` : "";
}

function versionStatusLabel(): string {
  if (currentVersion === undefined) return "";
  if (isCdfData) {
    const fromV = Math.max(0, currentVersion - 1);
    return `Changes v${fromV} → v${currentVersion} · `;
  }
  return `Version ${currentVersion} · `;
}

const ROW_HEIGHT = 28;
const BUFFER_ROWS = 20;

function fetchNextPage(): void {
  isLoading = true;
  loadingIndicator.hidden = false;
  if (isCdfData && currentVersion !== undefined) {
    vscode.postMessage({ type: "load_cdf", version: currentVersion, offset: allRows.length } satisfies WebviewToHostMessage);
  } else {
    vscode.postMessage({ type: "page", offset: allRows.length } satisfies WebviewToHostMessage);
  }
}

// Virtual scroll
function updateVirtualScroll(): void {
  if (!currentSchema) return;

  const scrollTop = scrollViewport.scrollTop;
  const viewportHeight = scrollViewport.clientHeight;

  const firstVisible = Math.floor(scrollTop / ROW_HEIGHT);
  const visibleCount = Math.ceil(viewportHeight / ROW_HEIGHT);

  const renderStart = Math.max(0, firstVisible - BUFFER_ROWS);
  const renderEnd = Math.min(allRows.length, firstVisible + visibleCount + BUFFER_ROWS);

  bodyTable.style.transform = `translateY(${renderStart * ROW_HEIGHT}px)`;

  bodyRows.innerHTML = "";

  for (let i = renderStart; i < renderEnd; i++) {
    const row = allRows[i];
    const tr = document.createElement("tr");
    tr.style.height = `${ROW_HEIGHT}px`;
    if (isCdfData) {
      const changeType = row["_change_type"];
      if (changeType === "insert") tr.classList.add("cdf-insert");
      else if (changeType === "update_preimage" || changeType === "update_postimage")
        tr.classList.add("cdf-update");
      else if (changeType === "delete") tr.classList.add("cdf-delete");
    }
    for (let colIdx = 0; colIdx < currentSchema!.length; colIdx++) {
      const col = currentSchema![colIdx];
      const td = document.createElement("td");
      const value = row[col.name];
      if (value === null || value === undefined) {
        td.textContent = "NULL";
        td.classList.add("null-value");
      } else if (typeof value === "object") {
        td.textContent = JSON.stringify(value);
      } else {
        td.textContent = String(value);
      }
      if (cachedColWidths[colIdx]) {
        td.style.width = `${cachedColWidths[colIdx]}px`;
        td.style.minWidth = `${cachedColWidths[colIdx]}px`;
      }
      tr.appendChild(td);
    }
    bodyRows.appendChild(tr);
  }

  if (renderEnd >= allRows.length - BUFFER_ROWS && allRows.length < totalRows && !isLoading) {
    fetchNextPage();
  }
}

scrollViewport.addEventListener("scroll", () => {
  requestAnimationFrame(updateVirtualScroll);
  // Sync horizontal scroll to header
  tableHeader.scrollLeft = scrollViewport.scrollLeft;
});

// Header width sync
function syncHeaderWidths(): void {
  const headerCells = headerRow.children;
  const firstRow = bodyRows.querySelector("tr");
  if (!firstRow) return;
  const bodyCells = firstRow.children;

  // Clear forced widths so we measure natural content widths
  for (let i = 0; i < headerCells.length && i < bodyCells.length; i++) {
    (headerCells[i] as HTMLElement).style.width = "";
    (headerCells[i] as HTMLElement).style.minWidth = "";
    (bodyCells[i] as HTMLElement).style.width = "";
    (bodyCells[i] as HTMLElement).style.minWidth = "";
  }

  cachedColWidths = [];
  for (let i = 0; i < headerCells.length && i < bodyCells.length; i++) {
    const headerWidth = (headerCells[i] as HTMLElement).offsetWidth;
    const bodyWidth = (bodyCells[i] as HTMLElement).offsetWidth;
    const width = Math.max(headerWidth, bodyWidth);
    cachedColWidths.push(width);
    (headerCells[i] as HTMLElement).style.width = `${width}px`;
    (headerCells[i] as HTMLElement).style.minWidth = `${width}px`;
    (bodyCells[i] as HTMLElement).style.width = `${width}px`;
    (bodyCells[i] as HTMLElement).style.minWidth = `${width}px`;
  }
}

// Type helpers
function typeCategory(dataType: string): string {
  const lower = dataType.toLowerCase();
  if (lower.includes("int") || lower.includes("float") || lower.includes("double") || lower.includes("decimal")) return "numeric";
  if (lower.includes("utf8") || lower.includes("string") || lower.includes("varchar")) return "string";
  if (lower.includes("bool")) return "boolean";
  if (lower.includes("date") || lower.includes("time") || lower.includes("timestamp")) return "temporal";
  if (lower.includes("binary") || lower.includes("bytes")) return "binary";
  if (lower.includes("list") || lower.includes("struct") || lower.includes("map")) return "complex";
  return "other";
}

function abbreviateType(dataType: string): string {
  const lower = dataType.toLowerCase();
  if (lower === "utf8" || lower === "largeutf8") return "str";
  if (lower === "boolean") return "bool";
  if (lower === "int8") return "i8";
  if (lower === "int16") return "i16";
  if (lower === "int32") return "i32";
  if (lower === "int64") return "i64";
  if (lower === "uint8") return "u8";
  if (lower === "uint16") return "u16";
  if (lower === "uint32") return "u32";
  if (lower === "uint64") return "u64";
  if (lower === "float16") return "f16";
  if (lower === "float32") return "f32";
  if (lower === "float64") return "f64";
  if (lower === "date32" || lower === "date64") return "date";
  if (lower.startsWith("timestamp")) return "ts";
  if (lower === "binary") return "bin";
  if (lower.startsWith("list")) return "list";
  if (lower.startsWith("struct")) return "struct";
  if (lower.startsWith("map")) return "map";
  if (dataType.length > 8) return dataType.substring(0, 6) + "..";
  return dataType;
}

function renderHeaders(): void {
  headerRow.innerHTML = "";
  for (const col of currentSchema!) {
    const th = document.createElement("th");

    const nameSpan = document.createElement("span");
    nameSpan.className = "col-name";
    nameSpan.textContent = col.name;

    const typeBadge = document.createElement("span");
    typeBadge.className = `type-badge type-${typeCategory(col.data_type)}`;
    typeBadge.textContent = abbreviateType(col.data_type);
    // Build tooltip with type info and column metadata
    const lines = [`${col.data_type}${col.nullable ? " (nullable)" : ""}`];
    const comment = col.metadata?.["comment"];
    if (comment) lines.push(comment);
    const metaEntries = Object.entries(col.metadata || {}).filter(
      ([k]) => k !== "comment",
    );
    for (const [k, v] of metaEntries) {
      lines.push(`${k}: ${v}`);
    }
    typeBadge.title = lines.join("\n");

    th.appendChild(nameSpan);
    th.appendChild(typeBadge);
    headerRow.appendChild(th);
  }
}

// Toolbar button handlers
historyBtn.addEventListener("click", () => openSidebarTab("history"));
infoBtn.addEventListener("click", () => openSidebarTab("info"));
changesBtn.addEventListener("click", () => {
  changesMode = !changesMode;
  changesBtn.classList.toggle("active", changesMode);
  cdfLegend.hidden = !changesMode;
  isCdfData = changesMode;
  if (currentVersion !== undefined) {
    if (changesMode) {
      vscode.postMessage({ type: "load_cdf", version: currentVersion } satisfies WebviewToHostMessage);
    } else {
      vscode.postMessage({ type: "load_version", version: currentVersion } satisfies WebviewToHostMessage);
    }
  }
});

window.addEventListener("message", (event: MessageEvent<HostToWebviewMessage>) => {
  const msg = event.data;

  if (msg.type === "init") {
    formatPill.textContent = msg.fileType === "delta" ? "Delta Lake" : "Parquet";
    formatPill.className = `format-pill-${msg.fileType}`;
    isDelta = msg.fileType === "delta";
    if (isDelta) {
      toolbarActions.hidden = false;
    }
    return;
  }

  if (msg.type === "error") {
    statusEl.textContent = "Error";
    bodyRows.innerHTML = "";
    headerRow.innerHTML = "";
    loadingIndicator.hidden = true;
    const container = document.getElementById("table-container")!;

    const errorDiv = document.createElement("div");
    errorDiv.className = "error-message";
    errorDiv.textContent = msg.message;

    if (msg.retryable) {
      const retryBtn = document.createElement("button");
      retryBtn.className = "retry-btn";
      retryBtn.textContent = "Retry";
      retryBtn.addEventListener("click", () => {
        container.innerHTML = "";
        // Recreate virtual scroll structure
        const newTableHeader = document.createElement("div");
        newTableHeader.id = "table-header";
        const headerTable = document.createElement("table");
        headerTable.className = "header-table";
        headerTable.innerHTML = "<thead><tr id='header-row'></tr></thead>";
        newTableHeader.appendChild(headerTable);
        container.appendChild(newTableHeader);

        const viewport = document.createElement("div");
        viewport.id = "scroll-viewport";
        const spacer = document.createElement("div");
        spacer.id = "scroll-spacer";
        viewport.appendChild(spacer);
        const bTable = document.createElement("table");
        bTable.className = "body-table";
        bTable.innerHTML = "<tbody id='body-rows'></tbody>";
        viewport.appendChild(bTable);
        container.appendChild(viewport);

        const newLoading = document.createElement("div");
        newLoading.id = "loading-indicator";
        newLoading.hidden = true;
        newLoading.textContent = "Loading...";
        container.appendChild(newLoading);

        // Rebind module-level DOM references to the new elements
        headerRow = document.getElementById("header-row")!;
        bodyRows = document.getElementById("body-rows")!;
        scrollViewport = document.getElementById("scroll-viewport")!;
        scrollSpacer = document.getElementById("scroll-spacer")!;
        bodyTable = document.querySelector(".body-table") as HTMLTableElement;
        tableHeader = document.getElementById("table-header")!;
        loadingIndicator = document.getElementById("loading-indicator")!;

        // Re-attach scroll listener on new viewport
        scrollViewport.addEventListener("scroll", () => {
          requestAnimationFrame(updateVirtualScroll);
          tableHeader.scrollLeft = scrollViewport.scrollLeft;
        });

        // Reset state
        allRows = [];
        totalRows = 0;
        currentSchema = null;

        if (isCdfData && currentVersion !== undefined) {
          vscode.postMessage({ type: "load_cdf", version: currentVersion } satisfies WebviewToHostMessage);
        } else {
          vscode.postMessage({ type: "ready" });
        }
      });
      errorDiv.appendChild(retryBtn);
    }

    container.innerHTML = "";
    container.appendChild(errorDiv);
    return;
  }

  if (msg.type === "data_header") {
    totalRows = msg.total_rows;
    currentVersion = msg.version;
    isCdfData = !!msg.cdf_mode;
    cdfLegend.hidden = !changesMode || activeTab !== "history";
    if (msg.offset === 0) {
      currentSchema = msg.schema;
      allRows = [];
      serverCdfCounts = msg.cdf_counts ?? null;
      renderHeaders();
      scrollSpacer.style.height = `${totalRows * ROW_HEIGHT}px`;
    }
    statusEl.textContent = `Loading rows...`;
    return;
  }

  if (msg.type === "data_chunk") {
    allRows.push(...msg.rows);
    updateVirtualScroll();
    statusEl.textContent = `${versionStatusLabel()}Loading... ${allRows.length.toLocaleString()} rows received`;
    return;
  }

  if (msg.type === "data_done") {
    isLoading = false;
    loadingIndicator.hidden = true;
    const loadedLabel = allRows.length < totalRows ? ` (${allRows.length.toLocaleString()} loaded)` : "";
    const breakdown = isCdfData ? cdfBreakdown() : "";
    statusEl.textContent = `${versionStatusLabel()}${totalRows.toLocaleString()} rows${breakdown}${loadedLabel}`;
    updateHistoryHighlight();
    syncHeaderWidths();
    return;
  }

  if (msg.type === "data") {
    renderData(msg);
    return;
  }

  if (msg.type === "history") {
    historyLoaded = true;
    renderHistory(msg.entries);
    return;
  }

  if (msg.type === "table_info") {
    tableInfoLoaded = true;
    if (currentVersion === undefined) {
      currentVersion = msg.current_version;
    }
    if (msg.cdf_enabled) {
      changesBtn.dataset.enabled = "1";
      if (activeTab === "history") {
        changesBtn.hidden = false;
      }
    }
    renderTableInfo(msg);
    return;
  }
});

function renderData(data: DataMessage): void {
  totalRows = data.total_rows;
  currentLimit = data.limit;
  isLoading = false;
  loadingIndicator.hidden = true;
  currentVersion = data.version;

  if (data.offset === 0) {
    currentSchema = data.schema;
    allRows = data.rows;
    serverCdfCounts = null;
    renderHeaders();
    scrollViewport.scrollTop = 0;
  } else {
    allRows.push(...data.rows);
  }

  // Set spacer to full virtual height
  scrollSpacer.style.height = `${totalRows * ROW_HEIGHT}px`;

  // Update status
  const loadedLabel = allRows.length < totalRows ? ` (${allRows.length.toLocaleString()} loaded)` : "";
  const breakdown = isCdfData ? cdfBreakdown() : "";
  statusEl.textContent = `${versionStatusLabel()}${totalRows.toLocaleString()} rows${breakdown}${loadedLabel}`;

  updateHistoryHighlight();
  updateVirtualScroll();
  syncHeaderWidths();
}

function renderHistory(entries: HistoryEntry[]): void {
  historyList.innerHTML = "";
  for (const entry of entries) {
    const div = document.createElement("div");
    div.className = "history-entry";
    if (currentVersion !== undefined && entry.version === currentVersion) {
      div.classList.add("active");
    }

    const versionSpan = document.createElement("span");
    versionSpan.className = "history-version";
    versionSpan.textContent = `v${entry.version}`;

    const metaDiv = document.createElement("div");
    metaDiv.className = "history-meta";

    if (entry.timestamp) {
      const timeSpan = document.createElement("span");
      timeSpan.className = "history-time";
      timeSpan.textContent = new Date(entry.timestamp).toLocaleString();
      metaDiv.appendChild(timeSpan);
    }

    if (entry.operation) {
      const opSpan = document.createElement("span");
      opSpan.className = "history-op";
      opSpan.textContent = entry.operation;
      metaDiv.appendChild(opSpan);
    }

    div.appendChild(versionSpan);
    div.appendChild(metaDiv);

    div.addEventListener("click", () => {
      if (changesMode) {
        vscode.postMessage({ type: "load_cdf", version: entry.version } satisfies WebviewToHostMessage);
      } else {
        vscode.postMessage({ type: "load_version", version: entry.version } satisfies WebviewToHostMessage);
      }
    });

    historyList.appendChild(div);
  }
}

function updateHistoryHighlight(): void {
  const entries = historyList.querySelectorAll(".history-entry");
  entries.forEach((el) => {
    const versionText = el.querySelector(".history-version")?.textContent;
    if (versionText && currentVersion !== undefined) {
      el.classList.toggle("active", versionText === `v${currentVersion}`);
    } else {
      el.classList.remove("active");
    }
  });
}

function renderTableInfo(info: TableInfoMessage): void {
  infoPanel.replaceChildren();

  function addSection(title: string, items: [string, string][]): void {
    if (items.length === 0) return;
    const section = document.createElement("div");
    section.className = "info-section";

    const heading = document.createElement("div");
    heading.className = "info-section-title";
    heading.textContent = title;
    section.appendChild(heading);

    const grid = document.createElement("div");
    grid.className = "info-grid";
    for (const [label, value] of items) {
      const labelEl = document.createElement("div");
      labelEl.className = "info-label";
      labelEl.textContent = label;
      const valueEl = document.createElement("div");
      valueEl.className = "info-value";
      valueEl.textContent = value;
      grid.appendChild(labelEl);
      grid.appendChild(valueEl);
    }
    section.appendChild(grid);
    infoPanel.appendChild(section);
  }

  // Overview
  const overview: [string, string][] = [];
  if (info.name) overview.push(["Name", info.name]);
  if (info.description) overview.push(["Description", info.description]);
  overview.push(["Table ID", info.id]);
  overview.push(["Location", info.location]);
  overview.push(["Current Version", String(info.current_version)]);
  if (info.created_time) {
    overview.push(["Created", new Date(info.created_time).toLocaleString()]);
  }
  addSection("Overview", overview);

  // Protocol
  const protocol: [string, string][] = [];
  protocol.push(["Min Reader Version", String(info.min_reader_version)]);
  protocol.push(["Min Writer Version", String(info.min_writer_version)]);
  if (info.reader_features) {
    protocol.push(["Reader Features", info.reader_features.join(", ")]);
  }
  if (info.writer_features) {
    protocol.push(["Writer Features", info.writer_features.join(", ")]);
  }
  protocol.push(["Change Data Feed", info.cdf_enabled ? "Enabled" : "Disabled"]);
  addSection("Protocol", protocol);

  // Format (omit if default parquet with no options)
  const formatOptions = Object.entries(info.format_options);
  if (info.format_provider !== "parquet" || formatOptions.length > 0) {
    const format: [string, string][] = [["Provider", info.format_provider]];
    for (const [k, v] of formatOptions) format.push([k, v]);
    addSection("Format", format);
  }

  // Storage
  const storage: [string, string][] = [];
  if (info.partition_columns.length > 0) {
    storage.push(["Partition Columns", info.partition_columns.join(", ")]);
  }
  storage.push(["Files", info.num_files.toLocaleString()]);
  storage.push(["Total Size", formatBytes(info.total_size_bytes)]);
  addSection("Storage", storage);

  // Configuration — grouped by category
  const config = Object.entries(info.configuration);
  const constraints = config.filter(([k]) => k.startsWith("delta.constraints."));
  const deltaProps = config.filter(([k]) =>
    k.startsWith("delta.") && !k.startsWith("delta.constraints.")
  );
  const customProps = config.filter(([k]) => !k.startsWith("delta."));

  if (constraints.length > 0) {
    addSection(
      "Constraints",
      constraints.map(([k, v]) => [k.replace("delta.constraints.", ""), v]),
    );
  }
  if (deltaProps.length > 0) {
    addSection("Delta Properties", deltaProps.map(([k, v]) => [k, v]));
  }
  if (customProps.length > 0) {
    addSection("Custom Properties", customProps.map(([k, v]) => [k, v]));
  }
}

function formatBytes(bytes: number): string {
  if (bytes === 0) {
    return "0 B";
  }
  const units = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  const val = bytes / Math.pow(1024, i);
  return `${val.toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}

// Responsive sidebar
function updateSidebarMode(): void {
  const narrow = mainContent.clientWidth < 600;
  sidebar.classList.toggle("sidebar-overlay", narrow);
  sidebar.classList.toggle("sidebar-docked", !narrow);
}

const sidebarResizeObserver = new ResizeObserver(() => {
  updateSidebarMode();
  syncHeaderWidths();
});
sidebarResizeObserver.observe(mainContent);

// Sidebar resize handle (drag)
let isResizing = false;

resizeHandle.addEventListener("mousedown", (e) => {
  if (sidebar.classList.contains("sidebar-overlay")) return;
  isResizing = true;
  e.preventDefault();
});

document.addEventListener("mousemove", (e) => {
  if (!isResizing) return;
  const newWidth = Math.max(160, Math.min(400, e.clientX));
  sidebar.style.width = `${newWidth}px`;
  syncHeaderWidths();
});

document.addEventListener("mouseup", () => {
  isResizing = false;
});

// Overlay close on outside click
mainContent.addEventListener("click", (e) => {
  if (
    sidebar.classList.contains("sidebar-overlay") &&
    !sidebar.hidden &&
    !sidebar.contains(e.target as Node)
  ) {
    sidebar.hidden = true;
    activeTab = null;
    historyBtn.classList.remove("active");
    infoBtn.classList.remove("active");
    changesBtn.hidden = true;
  }
});

// Signal ready
vscode.postMessage({ type: "ready" } satisfies WebviewToHostMessage);
