#import <Foundation/Foundation.h>
#import <GameController/GameController.h>

#include "controller_mac.h"
#include "controller_claim.h"

#include "app_shared.h"

#include <IOKit/hid/IOHIDDeviceKeys.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <chrono>
#include <iostream>
#include <mutex>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <objc/message.h>

namespace {

constexpr float kLockedControllerRed = 1.0f;
constexpr float kLockedControllerGreen = 0.0f;
// DualSense hardware rendered pure red as orange during manual verification.
// A small blue bias keeps the short app's lock light reading as red on-device.
constexpr float kLockedControllerBlue = 0.12f;
constexpr const char* kControllerIdentityMappingClaimKey = "controller_player_index_mapping";

struct ResolvedControllerIdentity {
    std::string claimKey;
    std::string physicalIdentity;
    std::string source;
    std::string diagnostics;
};

std::string nsStringToStd(NSString* value);
std::string toUpperAscii(std::string value);
bool controllerHasStablePlayerIndex(GCController* controller);

id safeValueForKey(id object, NSString* key) {
    if (object == nil || key == nil) {
        return nil;
    }
    @try {
        return [object valueForKey:key];
    } @catch (NSException*) {
        return nil;
    }
}

id callValueForHIDDeviceKey(id hidDevice, NSString* key) {
    if (hidDevice == nil || key == nil) {
        return nil;
    }
    const SEL selector = NSSelectorFromString(@"valueForHIDDeviceKey:");
    if (![hidDevice respondsToSelector:selector]) {
        return nil;
    }
    using MessageSendFn = id (*)(id, SEL, id);
    return ((MessageSendFn)objc_msgSend)(hidDevice, selector, key);
}

std::string objectToString(id value) {
    if (value == nil) {
        return {};
    }
    if ([value isKindOfClass:[NSString class]]) {
        return nsStringToStd((NSString*)value);
    }
    if ([value isKindOfClass:[NSNumber class]]) {
        return std::to_string([(NSNumber*)value longLongValue]);
    }
    return nsStringToStd([[value description] copy]);
}

std::optional<uint64_t> objectToUInt64(id value) {
    if (value == nil) {
        return std::nullopt;
    }
    if ([value isKindOfClass:[NSNumber class]]) {
        return static_cast<uint64_t>([(NSNumber*)value unsignedLongLongValue]);
    }
    if ([value isKindOfClass:[NSString class]]) {
        const std::string text = nsStringToStd((NSString*)value);
        if (text.empty()) {
            return std::nullopt;
        }
        try {
            std::size_t offset = 0;
            const unsigned long long parsed = std::stoull(text, &offset, 0);
            if (offset == text.size()) {
                return static_cast<uint64_t>(parsed);
            }
        } catch (const std::exception&) {
        }
    }
    return std::nullopt;
}

std::string formatHex(uint64_t value) {
    std::ostringstream stream;
    stream << "0x" << std::hex << std::nouppercase << value;
    return stream.str();
}

std::string joinSet(const std::set<std::string>& values) {
    if (values.empty()) {
        return "none";
    }
    std::string joined;
    bool first = true;
    for (const std::string& value : values) {
        if (!first) {
            joined += ",";
        }
        joined += value;
        first = false;
    }
    return joined;
}

ResolvedControllerIdentity resolveControllerIdentity(GCController* controller) {
    ResolvedControllerIdentity resolved;
    if (controller == nil) {
        return resolved;
    }

    std::set<std::string> usbLocations;
    std::set<std::string> hidPhysicalUniqueIds;
    std::set<std::string> hidServiceIdentifiers;
    std::set<std::string> hidRegistryIds;
    std::set<std::string> hidSerials;
    std::set<std::string> hidTransports;

    id hidServices = safeValueForKey(controller, @"hidServices");
    NSMutableArray* serviceArray = [NSMutableArray array];
    if ([hidServices isKindOfClass:[NSSet class]]) {
        [serviceArray addObjectsFromArray:[(NSSet*)hidServices allObjects]];
    } else if ([hidServices isKindOfClass:[NSArray class]]) {
        [serviceArray addObjectsFromArray:(NSArray*)hidServices];
    }

    for (id serviceInfo in serviceArray) {
        const std::string serviceIdentifier = objectToString(safeValueForKey(serviceInfo, @"identifier"));
        if (!serviceIdentifier.empty()) {
            hidServiceIdentifiers.insert(serviceIdentifier);
        }

        const auto registryId = objectToUInt64(safeValueForKey(serviceInfo, @"registryID"));
        if (registryId.has_value()) {
            hidRegistryIds.insert(formatHex(*registryId));
        }

        id hidDevice = safeValueForKey(serviceInfo, @"service");
        const std::string transport = objectToString(
            callValueForHIDDeviceKey(hidDevice, [NSString stringWithUTF8String:kIOHIDTransportKey]));
        if (!transport.empty()) {
            hidTransports.insert(transport);
        }

        const auto locationId = objectToUInt64(
            callValueForHIDDeviceKey(hidDevice, [NSString stringWithUTF8String:kIOHIDLocationIDKey]));
        if (locationId.has_value() && toUpperAscii(transport) == "USB") {
            usbLocations.insert(formatHex(*locationId));
        }

        const std::string physicalUniqueId = objectToString(
            callValueForHIDDeviceKey(hidDevice, [NSString stringWithUTF8String:kIOHIDPhysicalDeviceUniqueIDKey]));
        if (!physicalUniqueId.empty()) {
            hidPhysicalUniqueIds.insert(physicalUniqueId);
        }

        const std::string serial = objectToString(
            callValueForHIDDeviceKey(hidDevice, [NSString stringWithUTF8String:kIOHIDSerialNumberKey]));
        if (!serial.empty()) {
            hidSerials.insert(serial);
        }
    }

    const std::string controllerPhysicalUniqueId = objectToString(safeValueForKey(controller, @"physicalDeviceUniqueID"));
    const std::string controllerPersistentId = objectToString(safeValueForKey(controller, @"persistentIdentifier"));
    const std::string controllerIdentifier = objectToString(safeValueForKey(controller, @"identifier"));
    const std::string controllerUniqueIdentifier = objectToString(safeValueForKey(controller, @"uniqueIdentifier"));

    if (!usbLocations.empty()) {
        resolved.source = "hid.usb.location";
        resolved.physicalIdentity = "usb_location_" + joinSet(usbLocations);
    } else if (!hidPhysicalUniqueIds.empty()) {
        resolved.source = "hid.physical_device_unique_id";
        resolved.physicalIdentity = "hid_physical_unique_" + joinSet(hidPhysicalUniqueIds);
    } else if (!controllerPhysicalUniqueId.empty()) {
        resolved.source = "controller.physicalDeviceUniqueID";
        resolved.physicalIdentity = "controller_physical_unique_" + controllerPhysicalUniqueId;
    } else if (!controllerPersistentId.empty()) {
        resolved.source = "controller.persistentIdentifier";
        resolved.physicalIdentity = "controller_persistent_" + controllerPersistentId;
    } else if (!hidServiceIdentifiers.empty()) {
        resolved.source = "hid.service.identifier";
        resolved.physicalIdentity = "hid_service_identifier_" + joinSet(hidServiceIdentifiers);
    } else if (controllerHasStablePlayerIndex(controller)) {
        const int playerIndex = static_cast<int>(controller.playerIndex);
        resolved.source = "legacy.player_index";
        resolved.physicalIdentity = "player_index_" + std::to_string(playerIndex);
    }

    if (!resolved.physicalIdentity.empty()) {
        if (resolved.source == "legacy.player_index") {
            resolved.claimKey = controllerClaimKeyForPlayerIndex(static_cast<int>(controller.playerIndex));
        } else {
            resolved.claimKey = controllerClaimKeyForPhysicalIdentity(resolved.physicalIdentity);
        }
    }

    resolved.diagnostics =
        "source=" + (resolved.source.empty() ? "none" : resolved.source) +
        ", usbLocation=" + joinSet(usbLocations) +
        ", hidPhysicalUnique=" + joinSet(hidPhysicalUniqueIds) +
        ", hidServiceIdentifier=" + joinSet(hidServiceIdentifiers) +
        ", hidRegistryId=" + joinSet(hidRegistryIds) +
        ", hidSerial=" + joinSet(hidSerials) +
        ", hidTransport=" + joinSet(hidTransports) +
        ", controllerPhysicalDeviceUniqueID=" + (controllerPhysicalUniqueId.empty() ? "none" : controllerPhysicalUniqueId) +
        ", controllerPersistentIdentifier=" + (controllerPersistentId.empty() ? "none" : controllerPersistentId) +
        ", controllerIdentifier=" + (controllerIdentifier.empty() ? "none" : controllerIdentifier) +
        ", controllerUniqueIdentifier=" + (controllerUniqueIdentifier.empty() ? "none" : controllerUniqueIdentifier);
    return resolved;
}

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

bool controllerHasStablePlayerIndex(GCController* controller) {
    if (controller == nil) {
        return false;
    }
    return isStableControllerPlayerIndex(static_cast<int>(controller.playerIndex));
}

bool controllersNeedStablePlayerIndices(NSArray<GCController*>* controllers) {
    std::array<bool, 4> usedIndices = {false, false, false, false};
    for (GCController* controller in controllers) {
        if (!controllerHasStablePlayerIndex(controller)) {
            return true;
        }

        const std::size_t offset = static_cast<std::size_t>(static_cast<int>(controller.playerIndex));
        if (usedIndices[offset]) {
            return true;
        }
        usedIndices[offset] = true;
    }
    return false;
}

void normalizeControllerPlayerIndicesLocked(NSArray<GCController*>* controllers) {
    std::array<bool, 4> usedIndices = {false, false, false, false};
    std::vector<GCController*> needsAssignment;

    for (GCController* candidate in controllers) {
        if (!controllerHasStablePlayerIndex(candidate)) {
            needsAssignment.push_back(candidate);
            continue;
        }

        const std::size_t offset = static_cast<std::size_t>(static_cast<int>(candidate.playerIndex));
        if (usedIndices[offset]) {
            candidate.playerIndex = GCControllerPlayerIndexUnset;
            needsAssignment.push_back(candidate);
            continue;
        }

        usedIndices[offset] = true;
    }

    std::size_t nextAvailable = 0;
    for (GCController* candidate : needsAssignment) {
        while (nextAvailable < usedIndices.size() && usedIndices[nextAvailable]) {
            ++nextAvailable;
        }
        if (nextAvailable >= usedIndices.size()) {
            candidate.playerIndex = GCControllerPlayerIndexUnset;
            continue;
        }

        candidate.playerIndex = static_cast<GCControllerPlayerIndex>(static_cast<int>(nextAvailable));
        usedIndices[nextAvailable] = true;
    }
}

bool ensureStableControllerPlayerIndices() {
    NSArray<GCController*>* controllers = [GCController controllers];
    if (controllers.count == 0 || !controllersNeedStablePlayerIndices(controllers)) {
        return true;
    }

    ControllerClaimLease mappingLease;
    std::string error;
    for (int attempt = 0; attempt < 25; ++attempt) {
        if (tryAcquireControllerClaim(kControllerIdentityMappingClaimKey, mappingLease, &error)) {
            break;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        controllers = [GCController controllers];
        if (controllers.count == 0 || !controllersNeedStablePlayerIndices(controllers)) {
            return true;
        }
    }

    if (!hasControllerClaim(mappingLease)) {
        controllers = [GCController controllers];
        return controllers.count == 0 || !controllersNeedStablePlayerIndices(controllers);
    }

    controllers = [GCController controllers];
    if (controllersNeedStablePlayerIndices(controllers)) {
        normalizeControllerPlayerIndicesLocked(controllers);
    }
    releaseControllerClaim(mappingLease);

    controllers = [GCController controllers];
    return controllers.count == 0 || !controllersNeedStablePlayerIndices(controllers);
}

bool controllerHasAnyLightColor(GCController* controller) {
    if (controller == nil || controller.light == nil || controller.light.color == nil) {
        return false;
    }
    const GCColor* color = controller.light.color;
    constexpr float kLightEpsilon = 0.01f;
    return color.red > kLightEpsilon || color.green > kLightEpsilon || color.blue > kLightEpsilon;
}

std::string describeControllerIdentity(GCController* controller) {
    if (controller == nil) {
        return "unknown controller";
    }

    const ResolvedControllerIdentity resolvedIdentity = resolveControllerIdentity(controller);
    const std::string vendorName = nsStringToStd(controller.vendorName ?: @"Game Controller");
    const std::string productCategory = nsStringToStd(controller.productCategory);
    const int playerIndex = static_cast<int>(controller.playerIndex);
    const bool lightOn = controllerHasAnyLightColor(controller);
    std::string description = vendorName +
                              " [productCategory=" + (productCategory.empty() ? "unknown" : productCategory) +
                              ", playerIndex=" + std::to_string(playerIndex) +
                              ", physicalIdentity=" + (resolvedIdentity.physicalIdentity.empty() ? "none" : resolvedIdentity.physicalIdentity) +
                              ", identitySource=" + (resolvedIdentity.source.empty() ? "none" : resolvedIdentity.source) +
                              ", claimKey=" + (resolvedIdentity.claimKey.empty() ? "none" : resolvedIdentity.claimKey) +
                              ", light=" + (lightOn ? "on" : "off") +
                              ", attached=" + (controller.isAttachedToDevice ? "yes" : "no") +
                              ", identityDiagnostics=" + resolvedIdentity.diagnostics + "]";
    return description;
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

    appendSharedMessage(message);
    std::cout << "[" << message << "]" << std::endl;
}

} // namespace

@interface LongMacControllerManager : NSObject {
@private
    std::mutex _mutex;
    GCController* _activeController;
    GCController* _lockedController;
    ControllerClaimLease _lockedControllerClaim;
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
- (BOOL)attachController:(GCController*)controller announce:(BOOL)announce;
- (BOOL)attachBestAvailableControllerAnnounce:(BOOL)announce;
- (void)detachActiveControllerAnnounce:(BOOL)announce;
- (void)handleControllerConnected:(GCController*)controller;
- (void)handleControllerDisconnected:(GCController*)controller;
- (BOOL)shouldAcceptController:(GCController*)controller ignoredMessage:(std::string*)ignoredMessage;
- (BOOL)isControllerClaimedByOtherAppViaLight:(GCController*)controller;

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
    releaseControllerClaim(_lockedControllerClaim);
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

    (void)ensureStableControllerPlayerIndices();

    if (![self attachBestAvailableControllerAnnounce:YES]) {
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

- (BOOL)isControllerClaimedByOtherAppViaLight:(GCController*)controller {
    bool lockedToController = false;
    {
        std::lock_guard<std::mutex> lock(_mutex);
        lockedToController = (_lockedController == controller);
    }
    if (lockedToController) {
        return NO;
    }
    return controllerHasAnyLightColor(controller);
}

- (BOOL)attachBestAvailableControllerAnnounce:(BOOL)announce {
    (void)ensureStableControllerPlayerIndices();

    NSArray<GCController*>* controllers = [GCController controllers];
    if (controllers.count == 0) {
        return NO;
    }

    std::vector<GCController*> candidates;
    candidates.reserve(static_cast<std::size_t>(controllers.count));
    for (GCController* controller in controllers) {
        candidates.push_back(controller);
    }
    const bool hasUnlitCandidate = std::any_of(candidates.begin(), candidates.end(), [](GCController* controller) {
        return !controllerHasAnyLightColor(controller);
    });
    std::stable_sort(candidates.begin(), candidates.end(), [](GCController* lhs, GCController* rhs) {
        return controllerScore(lhs) > controllerScore(rhs);
    });

    auto tryAttachPass = [&](bool unlitOnly, const char* passName) {
        for (GCController* controller : candidates) {
            if (unlitOnly && controllerHasAnyLightColor(controller)) {
                continue;
            }
            std::string ignoredMessage;
            if (![self shouldAcceptController:controller ignoredMessage:&ignoredMessage]) {
                if (announce && !ignoredMessage.empty()) {
                    postControllerMessage(ignoredMessage);
                }
                continue;
            }
            if (announce) {
                postControllerMessage(std::string("Controller: Evaluating ") +
                                      describeControllerIdentity(controller) +
                                      " (" + passName + ")");
            }
            if ([self attachController:controller announce:announce]) {
                return YES;
            }
        }
        return NO;
    };

    if (hasUnlitCandidate) {
        if (tryAttachPass(true, "prefer-unlit")) {
            return YES;
        }
        if (announce) {
            postControllerMessage("Controller: No unlit candidate could be claimed; skipping lit fallback to avoid cross-app reuse");
        }
        return NO;
    }

    return tryAttachPass(false, "fallback-all");
}

- (BOOL)attachController:(GCController*)controller announce:(BOOL)announce {
    if (controller == nil) {
        return NO;
    }

    const std::string deviceName = nsStringToStd(controller.vendorName ?: @"Game Controller");
    const bool controllerLightOn = controllerHasAnyLightColor(controller);
    const ResolvedControllerIdentity resolvedIdentity = resolveControllerIdentity(controller);
    const std::string preferredClaimKey = resolvedIdentity.claimKey;
    std::string claimKeyInUse = preferredClaimKey;
    bool alreadyLockedToController = false;
    {
        std::lock_guard<std::mutex> lock(_mutex);
        alreadyLockedToController = (_lockedController == controller);
    }

    ControllerClaimLease newClaim;
    if (!alreadyLockedToController) {
        if (!preferredClaimKey.empty()) {
            std::string claimError;
            if (!tryAcquireControllerClaim(preferredClaimKey, newClaim, &claimError)) {
                if (announce) {
                    postControllerMessage("Controller: Skipping " + deviceName +
                                          " (" + claimError +
                                          "; physicalIdentity=" +
                                          (resolvedIdentity.physicalIdentity.empty() ? "none" : resolvedIdentity.physicalIdentity) +
                                          "; identitySource=" +
                                          (resolvedIdentity.source.empty() ? "none" : resolvedIdentity.source) +
                                          "; " + resolvedIdentity.diagnostics + ")");
                }
                return NO;
            }
            claimKeyInUse = preferredClaimKey;
        } else {
            if (announce) {
                postControllerMessage("Controller: Skipping " + deviceName +
                                      " (missing authoritative physical claim key; " +
                                      resolvedIdentity.diagnostics + ")");
            }
            return NO;
        }

        if (shouldUseControllerLightOwnershipFallback(claimKeyInUse, newClaim) &&
            [self isControllerClaimedByOtherAppViaLight:controller]) {
            releaseControllerClaim(newClaim);
            if (announce) {
                postControllerMessage("Controller: Skipping " + deviceName + " (lock light already owned by another app)");
            }
            return NO;
        }
    }

    bool lockedControllerChanged = false;
    std::string lockedDeviceName;
    {
        std::lock_guard<std::mutex> lock(_mutex);
        if (_lockedController == nil) {
            _lockedController = controller;
            lockedControllerChanged = true;
            lockedDeviceName = deviceName;
        }

        if (_lockedController == controller && hasControllerClaim(newClaim)) {
            releaseControllerClaim(_lockedControllerClaim);
            _lockedControllerClaim = std::move(newClaim);
        }
    }
    if (hasControllerClaim(newClaim)) {
        releaseControllerClaim(newClaim);
    }
    if (lockedControllerChanged) {
        updateControllerLockedState(lockedDeviceName);
    }

    [self detachActiveControllerAnnounce:NO];

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

    const bool lightUpdated = setControllerLightColor(controller,
                                                      kLockedControllerRed,
                                                      kLockedControllerGreen,
                                                      kLockedControllerBlue);
    updateControllerSharedState(true, deviceName);

    if (announce) {
        std::string message = "Controller: " + deviceName + " connected";
        if (!supportsInput) {
            message += " (limited mapping)";
        }
        postControllerMessage(message);
        postControllerMessage("Controller: Claimed physical identity " +
                              (resolvedIdentity.physicalIdentity.empty() ? "none" : resolvedIdentity.physicalIdentity) +
                              " (source=" +
                              (resolvedIdentity.source.empty() ? "none" : resolvedIdentity.source) +
                              ", claimKey=" +
                              (claimKeyInUse.empty() ? "none" : claimKeyInUse) +
                              ", " + resolvedIdentity.diagnostics + ")");
        if (lightUpdated) {
            postControllerMessage("Controller: " + deviceName + " lock light set to red");
        }
    }
    return YES;
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

    (void)ensureStableControllerPlayerIndices();

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
        (void)[self attachBestAvailableControllerAnnounce:YES];
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
