# Delta Viewer

A VS Code extension for viewing Delta Lake tables and Parquet files as interactive, scrollable tables.

## Features

- **Parquet file viewing** — open any `.parquet` file directly in VS Code
- **Delta Lake table viewing** — automatically detects `_delta_log` and reads the table
- **Infinite scroll pagination** — streams rows on demand (1000 at a time)
- **Version history** — browse and time-travel through Delta table versions
- **Table info** — inspect metadata, protocol versions, partitioning, file stats, and configuration
- **Theme-aware UI** — adapts to your VS Code color theme

## Architecture

```
VS Code Extension (TypeScript)
  └─ spawns ──▸ Sidecar binary (Rust, stdin/stdout JSON protocol)
                  └─ uses ──▸ delta-core library (deltalake + datafusion + arrow)
```

The project is a Cargo workspace with three crates and a VS Code extension:

| Path | Description |
|---|---|
| `crates/delta-core` | Core library — Delta/Parquet reading, pagination, schema conversion |
| `crates/delta-vscode` | Sidecar binary that communicates with the extension over stdio |
| `extension/` | VS Code extension — webview UI, sidecar lifecycle, commands |

## Prerequisites

- [Rust](https://rustup.rs/) (stable toolchain)
- [Node.js](https://nodejs.org/) >= 20
- VS Code >= 1.85

## Building

### Rust sidecar

```sh
cargo build --release
```

This produces the `delta-vscode` binary in `target/release/`.

### VS Code extension

```sh
cd extension
npm install
npm run compile
```

This bundles `extension.js` and `webview.js` into `extension/out/`.

### Packaging the extension

```sh
cd extension
npx vsce package
```

Produces a `.vsix` file you can install in VS Code.

## Development

### Watch mode

Run the extension build in watch mode for live recompilation:

```sh
cd extension
npm run watch
```

Then press `F5` in VS Code to launch the Extension Development Host.

### Sidecar path

By default the extension looks for the sidecar binary at `target/release/delta-vscode` relative to the extension. You can override this in settings:

```json
{
  "deltaViewer.sidecarPath": "/path/to/delta-vscode"
}
```

## Usage

1. **Open a Parquet file** — double-click any `.parquet` file in the explorer
2. **Open a Delta table** — run the `Delta Viewer: Open Delta Table` command and select a directory containing `_delta_log/`
3. **History** — click the History button in the toolbar to browse versions; click a version to time-travel
4. **Table Info** — click the Info button to view table metadata, protocol versions, and file statistics

## Testing

### Manual testing

1. Build the sidecar: `cargo build`
2. Compile the extension: `cd extension && npm run compile`
3. Press `F5` to launch the Extension Development Host
4. Open a Delta table or Parquet file and verify:
   - Data renders with infinite scroll
   - History sidebar shows version list (Delta tables only)
   - Clicking a version reloads data at that version
   - Info panel shows table metadata
   - Plain Parquet files do not show History/Info buttons

### Sidecar protocol testing

Pipe JSON commands directly to the sidecar binary:

```sh
echo '{"id":"1","command":"read_delta","params":{"path":"/path/to/table","offset":0,"limit":10}}' | cargo run --bin delta-vscode
echo '{"id":"2","command":"get_history","params":{"path":"/path/to/table"}}' | cargo run --bin delta-vscode
echo '{"id":"3","command":"get_table_info","params":{"path":"/path/to/table"}}' | cargo run --bin delta-vscode
```

## License

See [LICENSE](LICENSE) for details.
