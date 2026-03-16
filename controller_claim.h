#pragma once

#include <string>

struct ControllerClaimLease {
    int fd = -1;
    std::string key;
    std::string path;
};

bool tryAcquireControllerClaim(const std::string& claimKey,
                               ControllerClaimLease& lease,
                               std::string* error = nullptr);
void releaseControllerClaim(ControllerClaimLease& lease);
bool hasControllerClaim(const ControllerClaimLease& lease);
bool shouldUseControllerLightOwnershipFallback(const std::string& claimKey,
                                               const ControllerClaimLease& lease);
bool isStableControllerPlayerIndex(int playerIndex);
std::string controllerClaimKeyForPlayerIndex(int playerIndex);
