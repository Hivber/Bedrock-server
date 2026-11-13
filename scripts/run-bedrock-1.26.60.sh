#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUST_BIN="$ROOT/target/release/mc-rs-server"
GO_BIN="$ROOT/target/dragonfly-transport"

export MCRS_TRANSPORT_BIND="${MCRS_TRANSPORT_BIND:-127.0.0.1:19133}"
export MCRS_BEDROCK_BIND="${MCRS_BEDROCK_BIND:-0.0.0.0:19132}"

command -v cargo >/dev/null || { echo "cargo is required" >&2; exit 1; }
command -v go >/dev/null || { echo "Go is required" >&2; exit 1; }

echo "[build] MC-RS gameplay runtime"
cargo build --release -p mc-rs-server

echo "[build] Dragonfly/Gophertunnel Bedrock transport"
(
  cd "$ROOT/transport/dragonfly"
  go build -trimpath -o "$GO_BIN" .
)

"$RUST_BIN" &
RUST_PID=$!

cleanup() {
  kill "$RUST_PID" 2>/dev/null || true
  wait "$RUST_PID" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

echo "[run] Bedrock 1.26.60 (protocol 2225) on $MCRS_BEDROCK_BIND"
echo "[run] Rust↔Go IPC on $MCRS_TRANSPORT_BIND"
"$GO_BIN" -bind "$MCRS_BEDROCK_BIND" -ipc "$MCRS_TRANSPORT_BIND"
