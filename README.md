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

Package a distributable zip:

```bash
cmake --build build --target tws_gui_zip
```

Dependency source of truth:
- `ixwebsocket` is tracked as a git submodule at `./ixwebsocket`
  - upstream: `https://github.com/machinezone/IXWebSocket.git`
  - pinned: `v11.4.6` (`2efe037c9cc96fd536774f17bdb5215161ee5087`)
- `nlohmann_json` is tracked as a git submodule at `./nlohmann_json`
  - upstream: `https://github.com/nlohmann/json.git`
  - pinned: `v3.12.0` (`55f93686c01528224f448c19128836e7df245f72`)
- The build auto-detects a downloaded IB API under `~/Downloads/twsapi_macunix/...` when present

Bootstrap dependencies after clone (or in a dependency-empty workspace):

```bash
./scripts/bootstrap_deps.sh
```

CI can run the same command during checkout setup. If you only want submodule
restore without README pin checks:

```bash
./scripts/bootstrap_deps.sh --skip-readme-check
```

Equivalent raw git command (used by the script):

```bash
git submodule update --init --recursive
```

External dependency paths still work. If you keep `ixwebsocket` and
`nlohmann_json` outside this workspace, pass them explicitly:

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
- `tape_engine` writes per-batch segments and a hash-linked `manifest.jsonl` under `LONG_TAPE_ENGINE_DATA_DIR`.
- If `LONG_TAPE_ENGINE_DATA_DIR` is unset, the daemon defaults to `/tmp/tape-engine`.

Packaging notes:
- The app is built as a real macOS `.app` bundle.
- Non-system protobuf/abseil dylibs are embedded into `Contents/Frameworks`.
- The bundle is ad-hoc signed locally after dependency fixup.
- Notarization is not automated yet.
