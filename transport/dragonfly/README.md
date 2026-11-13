# Dragonfly transport for MC-RS

This directory contains the Bedrock network edge only. Gophertunnel handles
RakNet, Bedrock login/authentication, encryption, network compression and
packet framing. The Rust MC-RS process remains the gameplay server.

## Bedrock 1.26.60

Protocol ID: `2225` (Gophertunnel `v1.63.0`).

From the repository root, start both processes with:

```sh
./scripts/run-bedrock-1.26.60.sh
```

Defaults:
- Public Bedrock/RakNet listener: `0.0.0.0:19132`
- Private Rust↔Go IPC: `127.0.0.1:19133`

Override them with `MCRS_BEDROCK_BIND` and `MCRS_TRANSPORT_BIND`.
The IPC endpoint is required to remain loopback-only; do not expose it to
untrusted networks.

The script builds both executables, starts the Rust gameplay runtime, then
starts the Gophertunnel transport. Stop the foreground process with Ctrl+C.

## Important

Compilation and transport CI do not replace an actual Bedrock 1.26.60 client
test. Verify login, world spawn, movement, chunk streaming, block breaking and
placement, inventory changes, disconnect persistence and reconnect before
using this in a live world.
