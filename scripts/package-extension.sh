#!/usr/bin/env bash
#
# Package a universal .vsix file containing all supported platform binaries.
# The extension detects the correct binary at runtime based on platform/arch.
#
# Usage:
#   ./scripts/package-extension.sh              # package all platforms
#   ./scripts/package-extension.sh linux-x64    # build specific platform(s)
#
# Prerequisites:
#   - Rust toolchain with cross-compilation targets installed
#   - Node.js >= 20
#   - npm dependencies installed in extension/
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
EXT_DIR="$ROOT/extension"

# Map platform names to Rust targets
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

# Validate targets
for PLATFORM in "${TARGETS[@]}"; do
  if [[ -z "${RUST_TARGETS[$PLATFORM]:-}" ]]; then
    echo "ERROR: Unknown platform '$PLATFORM'"
    echo "Valid platforms: ${!RUST_TARGETS[*]}"
    exit 1
  fi
done

# Compile TypeScript once
echo "==> Compiling TypeScript..."
(cd "$EXT_DIR" && npm run compile)

# Build and collect binaries
for PLATFORM in "${TARGETS[@]}"; do
  RUST_TARGET="${RUST_TARGETS[$PLATFORM]}"
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

  echo "    => extension/bin/$PLATFORM/$BINARY"
done

# Create dist directory
DIST_DIR="$ROOT/dist"
mkdir -p "$DIST_DIR"

# Package single universal VSIX containing all binaries
echo "==> Packaging universal VSIX with all platform binaries..."
(cd "$EXT_DIR" && npx vsce package -o "$DIST_DIR/delta-viewer.vsix")

# Clean up binaries
echo "==> Cleaning up binaries..."
rm -rf "$EXT_DIR/bin"

echo ""
echo "Done! Universal VSIX is in $DIST_DIR/delta-viewer.vsix"
echo "This VSIX contains binaries for: ${TARGETS[*]}"
