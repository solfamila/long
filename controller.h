#pragma once

#include <chrono>
#include <string>

#if !defined(__APPLE__)
#error "controller.h is only supported on macOS in this project"
#endif

#include "app_shared.h"
#include "controller_mac.h"

inline constexpr int CONTROLLER_BUTTON_SQUARE   = 0;
inline constexpr int CONTROLLER_BUTTON_CROSS    = 1;
inline constexpr int CONTROLLER_BUTTON_CIRCLE   = 2;
inline constexpr int CONTROLLER_BUTTON_TRIANGLE = 3;

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
    updateControllerConnectionState(connected, deviceName);
}

inline void controllerClearButtons(ControllerState& cs) {
    for (auto& button : cs.buttons) {
        button.previous = button.current;
        button.current = false;
    }
}

inline bool controllerInitialize(ControllerState& cs) {
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
    const bool previousConnected = cs.connected;
    const std::string previousDeviceName = cs.deviceName;
    MacControllerSnapshot snapshot;
    macControllerPoll(cs.nativeHandle, snapshot);
    cs.connected = snapshot.connected;
    cs.supportsInput = snapshot.supportsInput;
    cs.deviceName = snapshot.deviceName;
    cs.buttons[CONTROLLER_BUTTON_SQUARE].current = snapshot.buttons[CONTROLLER_BUTTON_SQUARE];
    cs.buttons[CONTROLLER_BUTTON_CROSS].current = snapshot.buttons[CONTROLLER_BUTTON_CROSS];
    cs.buttons[CONTROLLER_BUTTON_CIRCLE].current = snapshot.buttons[CONTROLLER_BUTTON_CIRCLE];
    cs.buttons[CONTROLLER_BUTTON_TRIANGLE].current = snapshot.buttons[CONTROLLER_BUTTON_TRIANGLE];
    if (cs.connected != previousConnected || cs.deviceName != previousDeviceName) {
        controllerSetSharedState(cs.connected, cs.deviceName);
    }
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
