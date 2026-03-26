#!/usr/bin/env bash
#
# Package platform-specific .vsix files for all supported targets.
#
# Usage:
#   ./scripts/package-extension.sh              # package all targets
#   ./scripts/package-extension.sh linux-x64    # package one target
#
# Prerequisites:
#   - Rust toolchain with cross-compilation targets installed
#   - Node.js >= 20
#   - npm dependencies installed in extension/
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
EXT_DIR="$ROOT/extension"

# Map vsce platform targets to Rust targets
declare -A RUST_TARGETS=(
  [linux-x64]=x86_64-unknown-linux-gnu
  [linux-arm64]=aarch64-unknown-linux-gnu
  [darwin-x64]=x86_64-apple-darwin
  [darwin-arm64]=aarch64-apple-darwin
  [win32-x64]=x86_64-pc-windows-msvc
  [win32-arm64]=aarch64-pc-windows-msvc
)

# Determine which targets to build
if [[ $# -gt 0 ]]; then
  TARGETS=("$@")
else
  TARGETS=(linux-x64 linux-arm64 darwin-x64 darwin-arm64 win32-x64 win32-arm64)
fi

# Compile TypeScript
echo "==> Compiling TypeScript..."
(cd "$EXT_DIR" && npm run compile)

for PLATFORM in "${TARGETS[@]}"; do
  RUST_TARGET="${RUST_TARGETS[$PLATFORM]}"
  if [[ -z "$RUST_TARGET" ]]; then
    echo "ERROR: Unknown platform '$PLATFORM'"
    echo "Valid platforms: ${!RUST_TARGETS[*]}"
    exit 1
  fi

  # Determine binary name
  BINARY="delta-vscode"
  if [[ "$PLATFORM" == win32-* ]]; then
    BINARY="delta-vscode.exe"
  fi

  echo "==> Building sidecar for $PLATFORM ($RUST_TARGET)..."
  cargo build --release --target "$RUST_TARGET" -p delta-vscode

  # Copy binary into extension/bin/{platform}/
  BIN_DIR="$EXT_DIR/bin/$PLATFORM"
  mkdir -p "$BIN_DIR"
  cp "$ROOT/target/$RUST_TARGET/release/$BINARY" "$BIN_DIR/$BINARY"
  chmod +x "$BIN_DIR/$BINARY" 2>/dev/null || true

  echo "==> Packaging VSIX for $PLATFORM..."
  (cd "$EXT_DIR" && npx vsce package --target "$PLATFORM" -o "$ROOT/dist/delta-viewer-$PLATFORM.vsix")

  # Clean up binary to avoid bloating the next package
  rm -rf "$BIN_DIR"

  echo "    => dist/delta-viewer-$PLATFORM.vsix"
done

echo ""
echo "Done! VSIX files are in $ROOT/dist/"
