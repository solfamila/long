#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <string>

#if defined(_WIN32)
#include "dualsense_controller.h"
#endif

inline constexpr int CONTROLLER_BUTTON_SQUARE   = 0;
inline constexpr int CONTROLLER_BUTTON_CROSS    = 1;
inline constexpr int CONTROLLER_BUTTON_CIRCLE   = 2;
inline constexpr int CONTROLLER_BUTTON_TRIANGLE = 3;

#if defined(_WIN32)

using ControllerState = DualSenseState;

inline bool controllerInitialize(ControllerState& cs, void* hInstance, void* hwnd) {
    return dsInitialize(cs, static_cast<HINSTANCE>(hInstance), static_cast<HWND>(hwnd));
}

inline void controllerPoll(ControllerState& cs) {
    dsPoll(cs);
}

inline void controllerSetVibration(ControllerState& cs, bool enable) {
    dsSetVibration(cs, enable);
}

inline void controllerCleanup(ControllerState& cs) {
    dsCleanup(cs);
}

inline bool controllerIsConnected(const ControllerState& cs) {
    return cs.connected;
}

inline std::string controllerGetDeviceName(const ControllerState& cs) {
    return cs.deviceName;
}

#elif defined(__APPLE__)

struct ControllerState {
    bool connected = false;
    std::string deviceName;
    bool vibrating = false;

    struct ButtonState {
        bool current = false;
        bool previous = false;
    };
    ButtonState buttons[4];

    std::chrono::steady_clock::time_point lastButtonPress[4];
    static constexpr std::chrono::milliseconds kDebounceInterval{300};
};

inline bool controllerInitialize(ControllerState& cs, void* hInstance, void* hwnd) {
    cs.connected = false;
    cs.deviceName = "N/A (macOS controller not supported in Wave 1)";
    return true;
}

inline void controllerPoll(ControllerState& cs) {
    // No-op: controller support not implemented for macOS Wave 1
}

inline void controllerSetVibration(ControllerState& cs, bool enable) {
    // No-op: controller support not implemented for macOS Wave 1
    cs.vibrating = enable;
}

inline void controllerCleanup(ControllerState& cs) {
    cs.connected = false;
    cs.vibrating = false;
}

inline bool controllerIsConnected(const ControllerState& cs) {
    return cs.connected;
}

inline std::string controllerGetDeviceName(const ControllerState& cs) {
    return cs.deviceName;
}

#else

struct ControllerState {
    bool connected = false;
    std::string deviceName;
    bool vibrating = false;
};

inline bool controllerInitialize(ControllerState& cs, void* hInstance, void* hwnd) {
    cs.connected = false;
    return true;
}

inline void controllerPoll(ControllerState& cs) {
}

inline void controllerSetVibration(ControllerState& cs, bool enable) {
    cs.vibrating = enable;
}

inline void controllerCleanup(ControllerState& cs) {
    cs.connected = false;
}

inline bool controllerIsConnected(const ControllerState& cs) {
    return cs.connected;
}

inline std::string controllerGetDeviceName(const ControllerState& cs) {
    return cs.deviceName;
}

#endif
