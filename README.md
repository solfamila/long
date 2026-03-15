# long

`long` is a macOS-only TWS trading GUI.

Current stack:
- Native `AppKit` window and controls
- Apple `GameController.framework` for controller input on macOS
- Interactive Brokers TWS / Gateway connectivity
- `ixwebsocket` localhost control server
- bridge batch sender for `tape-engine` over local Unix domain sockets

Important architecture note:
- The app now uses native AppKit views instead of `GLFW + Dear ImGui`.
- The trading logic, TWS integration, WebSocket handling, and controller handling remain in C++ / ObjC++ code.

Build requirements:
- macOS
- Xcode Command Line Tools
- Protocol Buffers (`protoc` and headers/libs) when building against the newer IB API

Build:

```bash
cmake -S . -B build
cmake --build build -j4
```

Run:

```bash
open build/tws_gui.app
```

Direct executable inside the bundle:

```bash
./build/tws_gui.app/Contents/MacOS/tws_gui
```

Run the Phase-1 engine daemon:

```bash
./build/tape_engine
```

Query the engine daemon:

```bash
./build/tape_engine_ctl status
./build/tape_engine_ctl read-live-tail 20
./build/tape_engine_ctl read-range 1 50 --include-live-tail
./build/tape_engine_ctl read-range 1 50 --revision 1
./build/tape_engine_ctl replay-snapshot 50 --depth 5
./build/tape_engine_ctl replay-snapshot 50 --depth 5 --revision 1 --include-live-tail
./build/tape_engine_ctl find-order --order-id 701 --revision 1
./build/tape_engine_ctl seek-order --order-id 701 --revision 1
./build/tape_engine_ctl read-order-case --order-id 701 --revision 1
./build/tape_engine_ctl list-order-anchors --limit 20
./build/tape_engine_ctl list-protected-windows --limit 20
./build/tape_engine_ctl read-protected-window 1 --revision 3
./build/tape_engine_ctl list-findings --limit 20
./build/tape_engine_ctl list-incidents --limit 20
./build/tape_engine_ctl read-incident 1 --revision 4
```

Package a distributable zip:

```bash
cmake --build build --target tws_gui_zip
```

Dependency paths:
- ixWebSocket is expected at `./ixwebsocket`
- nlohmann/json is expected at `./nlohmann_json`
- The build auto-detects a downloaded IB API under `~/Downloads/twsapi_macunix/...` when present

External dependency paths also work. If you keep `ixwebsocket` and `nlohmann_json`
outside this workspace, pass them explicitly:

```bash
cmake -S . -B build \
  -DIXWEBSOCKET_DIR=/absolute/path/to/ixwebsocket \
  -DNLOHMANN_JSON_DIR=/absolute/path/to/nlohmann_json
cmake --build build -j4
```

Phase-0 mock bridge validation:

- If no vendored IB API is available, the workspace falls back to the local mock IB API.
- The bridge and recovery semantics are covered by `tws_gui_tests`.

```bash
cmake -S . -B build-phase0 \
  -DCMAKE_BUILD_TYPE=Debug \
  -DIXWEBSOCKET_DIR=/absolute/path/to/ixwebsocket \
  -DNLOHMANN_JSON_DIR=/absolute/path/to/nlohmann_json
cmake --build build-phase0 --target tws_gui_tests -j4
ctest --test-dir build-phase0 --output-on-failure
```

Phase-1 bridge sender notes:

- `long` now batches queued bridge records into framed MessagePack batches for `tape-engine`.
- The sender uses the Unix domain socket path from `LONG_TAPE_ENGINE_SOCKET` when set.
- If that env var is unset, the sender defaults to `/tmp/tape-engine.sock`.
- `tape_engine` now assigns ordered `session_seq` values on accept and freezes accepted batches into revisioned segments on a dedicated writer thread.
- `tape_engine` now routes ingest and query/replay work through separate worker queues so investigative reads do not share the ingest sequencer path.
- `tape_engine` writes per-batch binary MessagePack segment payloads plus JSON metadata and a hash-linked `manifest.jsonl` under `LONG_TAPE_ENGINE_DATA_DIR`.
- If `LONG_TAPE_ENGINE_DATA_DIR` is unset, the daemon defaults to `/tmp/tape-engine`.
- The daemon now answers `status`, `read_live_tail`, `read_range`, `replay_snapshot`, and `find_order_anchor` queries over the same framed MessagePack UDS transport.
- `seek_order_anchor` now returns replay targets and protected-window context for order/fill investigations, so a client can jump straight to the right `session_seq` and replay window around a fill or order-state transition.
- `read_order_case` now returns a report-style order/fill investigation summary with replay targets, protected-window context, related findings, and ranked incidents for the selected anchor.
- `read_order_case` and `read_incident` now include merged investigation timelines and timeline highlights, so clients can render case/incident narratives instead of stitching raw events together themselves.
- Frozen range/replay reads now snapshot engine state up front and use segment `session_seq` bounds to avoid holding the main engine lock across disk I/O and broad rescans.
- Query responses now expose frozen-revision state such as `latest_frozen_revision_id`, `served_revision_id`, and optional mutable-tail overlay via `--include-live-tail`.
- `long` now emits normalized public market records (`market_tick`, `market_depth`) alongside widened private lifecycle records including `open_order`, `order_status`, `commission_report`, `cancel_request`, `broker_error`, and `order_reject`.
- Bridge records now carry canonical `instrument_id`, receive/exchange timestamps, and vendor sequence placeholders so the engine can preserve stronger forensic provenance.
- `replay_snapshot` rebuilds a deterministic bid/ask/last and depth snapshot from frozen `session_seq` history, with optional live-tail overlay.
- Phase 3 now includes hot-path spread-widening, source-quality, trade-pressure, display-instability, post-fill invalidation, and inside-liquidity findings. Repeated findings collapse into ranked logical incidents, so `list-incidents` surfaces the latest scored incident snapshot instead of one row per finding, and `read-incident` returns a report-style drilldown with score breakdown, related findings, and protected-window context.
- Frozen revisions now persist Phase 3 artifact sidecars (`.artifacts.msgpack`) next to segment payloads, so anchors, protected windows, findings, and incidents survive daemon restart and remain revision-pinned evidence.
- Analyzer execution is now split into hot-path analyzers and an explicit deferred analyzer lane. The deferred lane emits order-flow timeline findings from protected order/fill windows on its own background queue instead of keeping all analysis inline on the sequencer path.

Runtime registry and QoS:

- Queue labels, categories, and QoS names are generated from `config/runtime/queues.yaml` into `runtime_registry.generated.h` at build time.
- The generated registry now covers `long`, `tape_engine`, `TapeScope`, and `tape-mcp` queue identities from one checked-in source of truth.
- `runtime_qos` applies the generated queue spec to bridge and engine threads on macOS, including pthread QoS classes and thread names.
- `tape_engine` now waits on POSIX signals for shutdown instead of polling in a timed sleep loop.

Packaging notes:
- The app is built as a real macOS `.app` bundle.
- Non-system protobuf/abseil dylibs are embedded into `Contents/Frameworks`.
- The bundle is ad-hoc signed locally after dependency fixup.
- Notarization is not automated yet.
