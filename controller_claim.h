#pragma once

#include <string>
#include <vector>

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
std::string controllerClaimKeyForPhysicalIdentity(const std::string& physicalIdentity);
std::vector<std::string> controllerClaimKeyFallbackOrderForPlayerIndex(int preferredPlayerIndex);
bool findFirstAvailableAlternateControllerClaimKey(int preferredPlayerIndex,
                                                   std::string* alternateKey,
                                                   std::string* error = nullptr);
