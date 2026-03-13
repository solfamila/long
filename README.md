# long

A new project created with Intent by Augment.

## Build Instructions

### macOS Build (GLFW + Metal)

**Required Dependencies:**

```bash
# Install system dependencies via Homebrew
brew install cmake glfw protobuf nlohmann-json
```

**External Dependencies:**

This project requires ImGui and ixwebsocket sources. Clone or place them in the project root:
- `imgui/` - Dear ImGui (https://github.com/ocornut/imgui)
- `ixwebsocket/` - ixWebSocket (https://github.com/machinezone/ixwebsocket)

**Optional: Interactive Brokers API**

Place the IB API at `/opt/IBAPI` or specify a custom path via `-DIB_API_ROOT=/path/to/ibapi` when building.

**Build Commands:**

```bash
# Create build directory
mkdir -p build && cd build

# Configure (options: -DCMAKE_BUILD_TYPE=Debug for debug build)
cmake ..

# Build
make -j$(nproc)
```

**Run:**
```bash
./tws_gui
```

**Known macOS Limitations (Wave 1):**
- Controller/gamepad support is disabled on macOS. The app will build and run with keyboard/mouse trading only.
- Window resize handling may need manual adjustment.
- Requires macOS with Metal support.

---

### Windows Build (Direct3D + Win32)

**Required Dependencies:**
- CMake 3.20+
- Visual Studio 2022 or MinGW-w64
- Direct3D 11 SDK (included with Windows SDK)
- GLFW3, Protobuf
- ImGui and ixwebsocket sources (see macOS section)

**Optional:** Interactive Brokers API at `C:\IBAPI` or specify `-DIB_API_ROOT`.

**Build:**
```bash
mkdir build && cd build
cmake .. -G "Visual Studio 17 2022"
cmake --build . --config Release
```

**Run:**
```bash
Release\tws_gui.exe
```
