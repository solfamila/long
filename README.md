# long

`long` is a macOS-only TWS trading GUI.

Current stack:
- Native `AppKit` window and controls
- Apple `GameController.framework` for controller input on macOS
- Interactive Brokers TWS / Gateway connectivity
- `ixwebsocket` localhost control server

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

Packaging notes:
- The app is built as a real macOS `.app` bundle.
- Non-system protobuf/abseil dylibs are embedded into `Contents/Frameworks`.
- The bundle is ad-hoc signed locally after dependency fixup.
- Notarization is not automated yet.
