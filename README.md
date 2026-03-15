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

Packaging notes:
- The app is built as a real macOS `.app` bundle.
- Non-system protobuf/abseil dylibs are embedded into `Contents/Frameworks`.
- The bundle is ad-hoc signed locally after dependency fixup.
- Notarization is not automated yet.
