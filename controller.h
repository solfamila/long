#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <string>

#include "app_shared.h"

#if defined(_WIN32)
#include "dualsense_controller.h"
#elif defined(__APPLE__)
#include "controller_mac.h"
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
    cs.prevState = cs.currState;
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
    bool supportsInput = false;
    std::string deviceName;
    bool vibrating = false;
    void* nativeHandle = nullptr;

    struct ButtonState {
        bool current = false;
        bool previous = false;
    };
    ButtonState buttons[4];

    std::chrono::steady_clock::time_point lastButtonPress[4];
    static constexpr std::chrono::milliseconds kDebounceInterval{300};
};

inline void controllerSetSharedState(bool connected, const std::string& deviceName) {
    g_data.controllerConnected.store(connected);
    std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
    g_data.controllerDeviceName = deviceName;
}

inline void controllerClearButtons(ControllerState& cs) {
    for (auto& button : cs.buttons) {
        button.previous = button.current;
        button.current = false;
    }
}

inline bool controllerInitialize(ControllerState& cs, void* hInstance, void* hwnd) {
    (void)hInstance;
    (void)hwnd;
    cs.connected = false;
    cs.supportsInput = false;
    cs.deviceName.clear();
    cs.vibrating = false;
    controllerClearButtons(cs);
    controllerSetSharedState(false, "");

    std::string error;
    if (!macControllerInitialize(&cs.nativeHandle, &error)) {
        if (!error.empty()) {
            const std::string message = "Controller initialization failed: " + error;
            g_data.addMessage(message);
            std::cout << "[" << message << "]" << std::endl;
        }
        return false;
    }

    return true;
}

inline void controllerPoll(ControllerState& cs) {
    controllerClearButtons(cs);
    MacControllerSnapshot snapshot;
    macControllerPoll(cs.nativeHandle, snapshot);
    cs.connected = snapshot.connected;
    cs.supportsInput = snapshot.supportsInput;
    cs.deviceName = snapshot.deviceName;
    cs.buttons[CONTROLLER_BUTTON_SQUARE].current = snapshot.buttons[CONTROLLER_BUTTON_SQUARE];
    cs.buttons[CONTROLLER_BUTTON_CROSS].current = snapshot.buttons[CONTROLLER_BUTTON_CROSS];
    cs.buttons[CONTROLLER_BUTTON_CIRCLE].current = snapshot.buttons[CONTROLLER_BUTTON_CIRCLE];
    cs.buttons[CONTROLLER_BUTTON_TRIANGLE].current = snapshot.buttons[CONTROLLER_BUTTON_TRIANGLE];
    controllerSetSharedState(cs.connected, cs.deviceName);
}

inline void controllerSetVibration(ControllerState& cs, bool enable) {
    cs.vibrating = enable;
    macControllerSetVibration(cs.nativeHandle, enable);
}

inline void controllerCleanup(ControllerState& cs) {
    if (cs.connected) {
        g_data.addMessage("Controller: Disconnected");
        std::cout << "[Controller: Disconnected]" << std::endl;
    }
    cs.connected = false;
    cs.supportsInput = false;
    cs.deviceName.clear();
    cs.vibrating = false;
    controllerClearButtons(cs);
    controllerSetSharedState(false, "");
    macControllerCleanup(&cs.nativeHandle);
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

#if defined(_WIN32)
inline bool controllerButtonJustPressed(const ControllerState& cs, int button) {
    return (cs.currState.rgbButtons[button] & 0x80) != 0 &&
           (cs.prevState.rgbButtons[button] & 0x80) == 0;
}

inline std::chrono::steady_clock::time_point& controllerLastButtonPress(ControllerState& cs, int button) {
    switch (button) {
        case CONTROLLER_BUTTON_SQUARE: return cs.lastSquarePress;
        case CONTROLLER_BUTTON_CROSS: return cs.lastCrossPress;
        case CONTROLLER_BUTTON_CIRCLE: return cs.lastCirclePress;
        default: return cs.lastTrianglePress;
    }
}

inline std::chrono::milliseconds controllerDebounceInterval() {
    return DualSenseState::kDebounceInterval;
}
#elif defined(__APPLE__)
inline bool controllerButtonJustPressed(const ControllerState& cs, int button) {
    return button >= 0 && button < 4 &&
           cs.buttons[button].current &&
           !cs.buttons[button].previous;
}

inline std::chrono::steady_clock::time_point& controllerLastButtonPress(ControllerState& cs, int button) {
    return cs.lastButtonPress[button];
}

inline std::chrono::milliseconds controllerDebounceInterval() {
    return ControllerState::kDebounceInterval;
}
#else
inline bool controllerButtonJustPressed(const ControllerState& cs, int button) {
    (void)cs;
    (void)button;
    return false;
}

inline std::chrono::steady_clock::time_point& controllerLastButtonPress(ControllerState& cs, int button) {
    static std::chrono::steady_clock::time_point dummy;
    (void)cs;
    (void)button;
    return dummy;
}

inline std::chrono::milliseconds controllerDebounceInterval() {
    return std::chrono::milliseconds(300);
}
#endif

inline bool controllerConsumeDebouncedPress(ControllerState& cs, int button,
                                            std::chrono::steady_clock::time_point now) {
    if (!controllerButtonJustPressed(cs, button)) {
        return false;
    }

    auto& lastPress = controllerLastButtonPress(cs, button);
    if ((now - lastPress) <= controllerDebounceInterval()) {
        return false;
    }

    lastPress = now;
    return true;
}
