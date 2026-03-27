# Delta Viewer

View Delta Lake tables and Parquet files as interactive, scrollable tables directly in VS Code.

## Features

- **Parquet file viewing** — open any `.parquet` file directly in VS Code with a tabular viewer
- **Delta Lake table viewing** — automatically detects `_delta_log` and reads the table
- **Infinite scroll pagination** — streams rows on demand for fast browsing of large datasets
- **Version history** — browse and time-travel through Delta table versions
- **Table info** — inspect metadata, protocol versions, partitioning, file stats, and configuration
- **Theme-aware UI** — adapts to your VS Code color theme

## Usage

1. **Open a Parquet file** — double-click any `.parquet` file in the explorer
2. **Open a Delta table** — run the `Delta Viewer: Open Delta Table` command and select a directory containing `_delta_log/`
3. **Open a Parquet file in a Delta folder** — double-click any `.parquet` file within a Delta Lake folder to view the full Delta Lake table
4. **History** — click the History button in the toolbar to browse versions; click a version to time-travel
5. **Table Info** — click the Info button to view table metadata, protocol versions, and file statistics

## Commands

| Command | Description |
|---------|-------------|
| `Delta Viewer: Open Parquet File` | Open a Parquet file in the viewer |
| `Delta Viewer: Open Delta Table` | Open a Delta Lake table directory |
| `Delta Viewer: Open as Raw Parquet` | Open a file as raw Parquet (bypass Delta detection) |

## Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `deltaViewer.requestTimeout` | `30000` | Timeout in milliseconds for sidecar requests |
| `deltaViewer.sidecarPath` | — | Override the path to the sidecar binary |

## Requirements

This extension bundles a native sidecar binary for reading Delta and Parquet files. Platform-specific builds are available for:

- Linux x64
- macOS x64 / ARM64 (Apple Silicon)
- Windows x64
