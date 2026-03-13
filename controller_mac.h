#pragma once

#if defined(__APPLE__)

#include <string>

struct MacControllerSnapshot {
    bool connected = false;
    bool supportsInput = false;
    std::string deviceName;
    bool buttons[4] = {false, false, false, false};
};

bool macControllerInitialize(void** handle, std::string* error = nullptr);
void macControllerPoll(void* handle, MacControllerSnapshot& snapshot);
void macControllerSetVibration(void* handle, bool enable);
void macControllerCleanup(void** handle);

#endif
