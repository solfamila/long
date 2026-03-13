#import <Foundation/Foundation.h>
#import <GameController/GameController.h>

#include "controller_mac.h"

#include "app_shared.h"

#include <algorithm>
#include <cctype>
#include <iostream>
#include <mutex>
#include <string>

namespace {

std::string nsStringToStd(NSString* value) {
    if (value == nil) {
        return {};
    }
    return std::string([value UTF8String]);
}

std::string toUpperAscii(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
        return static_cast<char>(std::toupper(ch));
    });
    return value;
}

bool controllerMatchesPreferredName(const std::string& name) {
    const std::string upper = toUpperAscii(name);
    return upper.find("DUALSENSE") != std::string::npos ||
           upper.find("WIRELESS CONTROLLER") != std::string::npos ||
           upper.find("PLAYSTATION") != std::string::npos;
}

int controllerScore(GCController* controller) {
    if (controller == nil) {
        return -1;
    }

    int score = 0;
    if (controller.extendedGamepad != nil) {
        score += 2;
    }

    const std::string name = nsStringToStd(controller.vendorName);
    if (controllerMatchesPreferredName(name)) {
        score += 2;
    }

    return score;
}

void updateControllerSharedState(bool connected, const std::string& deviceName) {
    updateControllerConnectionState(connected, deviceName);
}

void updateControllerLockedState(const std::string& deviceName) {
    updateControllerLockedDeviceName(deviceName);
}

bool setControllerLightColor(GCController* controller, float red, float green, float blue) {
    if (controller == nil || controller.light == nil) {
        return false;
    }

    controller.light.color = [[GCColor alloc] initWithRed:red green:green blue:blue];
    return true;
}

void postControllerMessage(const std::string& message) {
    if (message.empty()) {
        return;
    }

    g_data.addMessage(message);
    std::cout << "[" << message << "]" << std::endl;
}

} // namespace

@interface LongMacControllerManager : NSObject {
@private
    std::mutex _mutex;
    GCController* _activeController;
    GCController* _lockedController;
    id _connectObserver;
    id _disconnectObserver;
    bool _supportsInput;
    bool _buttonStates[4];
    bool _vibrationEnabled;
    std::string _deviceName;
}

- (BOOL)startWithError:(std::string*)error;
- (void)copySnapshot:(MacControllerSnapshot&)snapshot;
- (void)setShouldVibrate:(BOOL)enable;

@end

@interface LongMacControllerManager ()

- (void)setPressed:(BOOL)pressed forButtonIndex:(int)buttonIndex;
- (void)attachController:(GCController*)controller announce:(BOOL)announce;
- (void)detachActiveControllerAnnounce:(BOOL)announce;
- (void)handleControllerConnected:(GCController*)controller;
- (void)handleControllerDisconnected:(GCController*)controller;
- (BOOL)shouldAcceptController:(GCController*)controller ignoredMessage:(std::string*)ignoredMessage;
- (GCController*)bestAvailableController;

@end

@implementation LongMacControllerManager

- (instancetype)init {
    self = [super init];
    if (self != nil) {
        _activeController = nil;
        _lockedController = nil;
        _connectObserver = nil;
        _disconnectObserver = nil;
        _supportsInput = false;
        _vibrationEnabled = false;
        std::fill(std::begin(_buttonStates), std::end(_buttonStates), false);
    }
    return self;
}

- (void)dealloc {
    if (_connectObserver != nil) {
        [[NSNotificationCenter defaultCenter] removeObserver:_connectObserver];
        _connectObserver = nil;
    }

    if (_disconnectObserver != nil) {
        [[NSNotificationCenter defaultCenter] removeObserver:_disconnectObserver];
        _disconnectObserver = nil;
    }

    [self detachActiveControllerAnnounce:NO];
}

- (BOOL)startWithError:(std::string*)error {
    __weak LongMacControllerManager* weakSelf = self;
    NSNotificationCenter* center = [NSNotificationCenter defaultCenter];

    _connectObserver = [center addObserverForName:GCControllerDidConnectNotification
                                           object:nil
                                            queue:nil
                                       usingBlock:^(NSNotification* note) {
        LongMacControllerManager* strongSelf = weakSelf;
        if (strongSelf == nil) {
            return;
        }

        GCController* controller = (GCController*)note.object;
        if (controller != nil) {
            [strongSelf handleControllerConnected:controller];
        }
    }];

    _disconnectObserver = [center addObserverForName:GCControllerDidDisconnectNotification
                                              object:nil
                                               queue:nil
                                          usingBlock:^(NSNotification* note) {
        LongMacControllerManager* strongSelf = weakSelf;
        if (strongSelf == nil) {
            return;
        }

        GCController* controller = (GCController*)note.object;
        if (controller != nil) {
            [strongSelf handleControllerDisconnected:controller];
        }
    }];

    GCController* controller = [self bestAvailableController];
    if (controller != nil) {
        [self attachController:controller announce:YES];
    } else {
        updateControllerSharedState(false, "");
    }

    if (error != nullptr) {
        error->clear();
    }
    return YES;
}

- (BOOL)shouldAcceptController:(GCController*)controller ignoredMessage:(std::string*)ignoredMessage {
    std::lock_guard<std::mutex> lock(_mutex);

    if (_lockedController == nil) {
        return YES;
    }

    if (_lockedController == controller) {
        return YES;
    }

    if (ignoredMessage != nullptr) {
        const std::string ignoredName = nsStringToStd(controller.vendorName ?: @"Game Controller");
        const std::string lockedName = !_deviceName.empty()
            ? _deviceName
            : nsStringToStd(_lockedController.vendorName ?: @"Game Controller");
        *ignoredMessage = "Controller: Ignoring " + ignoredName + " (locked to " + lockedName + " for this app session)";
    }

    return NO;
}

- (void)copySnapshot:(MacControllerSnapshot&)snapshot {
    std::lock_guard<std::mutex> lock(_mutex);
    snapshot.connected = (_activeController != nil);
    snapshot.supportsInput = _supportsInput;
    snapshot.deviceName = _deviceName;
    std::copy(std::begin(_buttonStates), std::end(_buttonStates), std::begin(snapshot.buttons));
}

- (void)setShouldVibrate:(BOOL)enable {
    std::lock_guard<std::mutex> lock(_mutex);
    _vibrationEnabled = enable;
}

- (void)setPressed:(BOOL)pressed forButtonIndex:(int)buttonIndex {
    if (buttonIndex < 0 || buttonIndex >= 4) {
        return;
    }

    std::lock_guard<std::mutex> lock(_mutex);
    _buttonStates[buttonIndex] = pressed;
}

- (GCController*)bestAvailableController {
    NSArray<GCController*>* controllers = [GCController controllers];
    GCController* bestController = nil;
    int bestScore = -1;

    for (GCController* controller in controllers) {
        const int score = controllerScore(controller);
        if (score > bestScore) {
            bestScore = score;
            bestController = controller;
        }
    }

    return bestController;
}

- (void)attachController:(GCController*)controller announce:(BOOL)announce {
    if (controller == nil) {
        return;
    }

    bool lockedControllerChanged = false;
    std::string lockedDeviceName;
    {
        std::lock_guard<std::mutex> lock(_mutex);
        if (_lockedController == nil) {
            _lockedController = controller;
            lockedControllerChanged = true;
            lockedDeviceName = nsStringToStd(controller.vendorName ?: @"Game Controller");
        }
    }
    if (lockedControllerChanged) {
        updateControllerLockedState(lockedDeviceName);
    }

    [self detachActiveControllerAnnounce:NO];

    const std::string deviceName = nsStringToStd(controller.vendorName ?: @"Game Controller");
    const bool supportsInput = (controller.extendedGamepad != nil);

    {
        std::lock_guard<std::mutex> lock(_mutex);
        _activeController = controller;
        _supportsInput = supportsInput;
        _deviceName = deviceName;
        std::fill(std::begin(_buttonStates), std::end(_buttonStates), false);
    }

    if (controller.extendedGamepad != nil) {
        __weak LongMacControllerManager* weakSelf = self;

        controller.extendedGamepad.buttonX.pressedChangedHandler = ^(GCControllerButtonInput*, float, BOOL pressed) {
            LongMacControllerManager* strongSelf = weakSelf;
            if (strongSelf != nil) {
                [strongSelf setPressed:pressed forButtonIndex:0];
            }
        };

        controller.extendedGamepad.buttonA.pressedChangedHandler = ^(GCControllerButtonInput*, float, BOOL pressed) {
            LongMacControllerManager* strongSelf = weakSelf;
            if (strongSelf != nil) {
                [strongSelf setPressed:pressed forButtonIndex:1];
            }
        };

        controller.extendedGamepad.buttonB.pressedChangedHandler = ^(GCControllerButtonInput*, float, BOOL pressed) {
            LongMacControllerManager* strongSelf = weakSelf;
            if (strongSelf != nil) {
                [strongSelf setPressed:pressed forButtonIndex:2];
            }
        };

        controller.extendedGamepad.buttonY.pressedChangedHandler = ^(GCControllerButtonInput*, float, BOOL pressed) {
            LongMacControllerManager* strongSelf = weakSelf;
            if (strongSelf != nil) {
                [strongSelf setPressed:pressed forButtonIndex:3];
            }
        };
    }

    const bool lightUpdated = setControllerLightColor(controller, 0.0f, 1.0f, 0.0f);
    updateControllerSharedState(true, deviceName);

    if (announce) {
        std::string message = "Controller: " + deviceName + " connected";
        if (!supportsInput) {
            message += " (limited mapping)";
        }
        postControllerMessage(message);
        if (lightUpdated) {
            postControllerMessage("Controller: " + deviceName + " lock light set to green");
        }
    }
}

- (void)detachActiveControllerAnnounce:(BOOL)announce {
    GCController* previousController = nil;
    bool hadController = false;

    {
        std::lock_guard<std::mutex> lock(_mutex);
        previousController = _activeController;
        hadController = (_activeController != nil);
        _activeController = nil;
        _supportsInput = false;
        _deviceName.clear();
        std::fill(std::begin(_buttonStates), std::end(_buttonStates), false);
    }

    if (previousController != nil && previousController.extendedGamepad != nil) {
        previousController.extendedGamepad.buttonX.pressedChangedHandler = nil;
        previousController.extendedGamepad.buttonA.pressedChangedHandler = nil;
        previousController.extendedGamepad.buttonB.pressedChangedHandler = nil;
        previousController.extendedGamepad.buttonY.pressedChangedHandler = nil;
    }
    setControllerLightColor(previousController, 0.0f, 0.0f, 0.0f);

    updateControllerSharedState(false, "");

    if (announce && hadController) {
        postControllerMessage("Controller: Disconnected");
    }
}

- (void)handleControllerConnected:(GCController*)controller {
    if (controller == nil) {
        return;
    }

    std::string ignoredMessage;
    if (![self shouldAcceptController:controller ignoredMessage:&ignoredMessage]) {
        postControllerMessage(ignoredMessage);
        return;
    }

    bool needsAttach = false;
    {
        std::lock_guard<std::mutex> lock(_mutex);
        needsAttach = (_activeController != controller);
    }

    if (needsAttach) {
        [self attachController:controller announce:YES];
    }
}

- (void)handleControllerDisconnected:(GCController*)controller {
    if (controller == nil) {
        return;
    }

    bool wasActive = false;
    {
        std::lock_guard<std::mutex> lock(_mutex);
        wasActive = (_activeController == controller);
    }

    if (!wasActive) {
        return;
    }

    [self detachActiveControllerAnnounce:YES];
}

@end

bool macControllerInitialize(void** handle, std::string* error) {
    if (handle == nullptr) {
        if (error != nullptr) {
            *error = "Missing macOS controller handle storage";
        }
        return false;
    }

    @autoreleasepool {
        LongMacControllerManager* manager = [[LongMacControllerManager alloc] init];
        if (![manager startWithError:error]) {
            return false;
        }

        *handle = (__bridge_retained void*)manager;
        return true;
    }
}

void macControllerPoll(void* handle, MacControllerSnapshot& snapshot) {
    snapshot = MacControllerSnapshot{};
    if (handle == nullptr) {
        return;
    }

    @autoreleasepool {
        LongMacControllerManager* manager = (__bridge LongMacControllerManager*)handle;
        [manager copySnapshot:snapshot];
    }
}

void macControllerSetVibration(void* handle, bool enable) {
    if (handle == nullptr) {
        return;
    }

    @autoreleasepool {
        LongMacControllerManager* manager = (__bridge LongMacControllerManager*)handle;
        [manager setShouldVibrate:enable ? YES : NO];
    }
}

void macControllerCleanup(void** handle) {
    if (handle == nullptr || *handle == nullptr) {
        return;
    }

    @autoreleasepool {
        CFBridgingRelease(*handle);
        *handle = nullptr;
    }
}
