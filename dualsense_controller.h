#pragma once

#include "app_shared.h"

inline constexpr int DS_BUTTON_SQUARE   = 0; // Buy
inline constexpr int DS_BUTTON_CROSS    = 1; // Toggle quantity
inline constexpr int DS_BUTTON_CIRCLE   = 2; // Close
inline constexpr int DS_BUTTON_TRIANGLE = 3; // Cancel all

struct DualSenseState {
    LPDIRECTINPUT8 di = nullptr;
    LPDIRECTINPUTDEVICE8 device = nullptr;
    DIJOYSTATE2 currState = {};
    DIJOYSTATE2 prevState = {};
    bool connected = false;
    std::string deviceName;
    bool vibrating = false;

    HANDLE hidFile = INVALID_HANDLE_VALUE;
    bool isUSB = true;
    bool hidOutputReady = false;

    std::chrono::steady_clock::time_point lastSquarePress{};
    std::chrono::steady_clock::time_point lastCirclePress{};
    std::chrono::steady_clock::time_point lastTrianglePress{};
    std::chrono::steady_clock::time_point lastCrossPress{};
    static constexpr std::chrono::milliseconds kDebounceInterval{300};
};

bool dsInitialize(DualSenseState& ds, HINSTANCE hInstance, HWND hwnd);
void dsPoll(DualSenseState& ds);
void dsSetVibration(DualSenseState& ds, bool enable);
void dsCleanup(DualSenseState& ds);
