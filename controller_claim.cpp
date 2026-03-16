#include "controller_claim.h"

#include <cerrno>
#include <cctype>
#include <cstring>
#include <cstdlib>
#include <filesystem>
#include <fcntl.h>
#include <sstream>
#include <sys/file.h>
#include <unistd.h>
#include <vector>

namespace {

constexpr int kMinStableControllerPlayerIndex = 0;
constexpr int kMaxStableControllerPlayerIndex = 3;

std::string sanitizeClaimKey(std::string key) {
    for (char& ch : key) {
        const unsigned char value = static_cast<unsigned char>(ch);
        if (std::isalnum(value) != 0 || ch == '-' || ch == '_') {
            continue;
        }
        ch = '_';
    }
    if (key.empty()) {
        return "controller";
    }
    return key;
}

const std::string& controllerClaimDirectory() {
    static const std::string directory = [] {
        namespace fs = std::filesystem;

        fs::path path;
        if (const char* overridePath = std::getenv("TWS_CONTROLLER_CLAIM_DIR");
            overridePath != nullptr && *overridePath != '\0') {
            path = fs::path(overridePath);
        } else {
            path = fs::temp_directory_path() / "tws_controller_claims";
        }

        std::error_code ec;
        fs::create_directories(path, ec);
        return path.string();
    }();
    return directory;
}

std::string claimFilePath(const std::string& claimKey) {
    namespace fs = std::filesystem;
    const std::string key = sanitizeClaimKey(claimKey);
    return (fs::path(controllerClaimDirectory()) / (key + ".lock")).string();
}

std::string claimError(const std::string& key, int errorCode) {
    std::ostringstream oss;
    oss << "Controller claim failed for " << key << ": ";
    if (errorCode == EWOULDBLOCK || errorCode == EAGAIN) {
        oss << "already claimed by another app process";
    } else {
        oss << std::strerror(errorCode);
    }
    return oss.str();
}

} // namespace

bool tryAcquireControllerClaim(const std::string& claimKey,
                               ControllerClaimLease& lease,
                               std::string* error) {
    if (error != nullptr) {
        error->clear();
    }
    if (claimKey.empty()) {
        if (error != nullptr) {
            *error = "Controller claim failed: missing claim key";
        }
        return false;
    }

    releaseControllerClaim(lease);

    const std::string path = claimFilePath(claimKey);
    const int fd = ::open(path.c_str(), O_CREAT | O_RDWR, 0600);
    if (fd < 0) {
        if (error != nullptr) {
            *error = claimError(claimKey, errno);
        }
        return false;
    }

    if (::flock(fd, LOCK_EX | LOCK_NB) != 0) {
        const int lockErrno = errno;
        ::close(fd);
        if (error != nullptr) {
            *error = claimError(claimKey, lockErrno);
        }
        return false;
    }

    lease.fd = fd;
    lease.key = claimKey;
    lease.path = path;
    return true;
}

void releaseControllerClaim(ControllerClaimLease& lease) {
    if (lease.fd >= 0) {
        ::flock(lease.fd, LOCK_UN);
        ::close(lease.fd);
    }
    lease = ControllerClaimLease{};
}

bool hasControllerClaim(const ControllerClaimLease& lease) {
    return lease.fd >= 0;
}

bool shouldUseControllerLightOwnershipFallback(const std::string& claimKey,
                                               const ControllerClaimLease& lease) {
    if (claimKey.empty()) {
        return true;
    }
    return !hasControllerClaim(lease);
}

bool isStableControllerPlayerIndex(int playerIndex) {
    return playerIndex >= kMinStableControllerPlayerIndex &&
           playerIndex <= kMaxStableControllerPlayerIndex;
}

std::string controllerClaimKeyForPlayerIndex(int playerIndex) {
    if (!isStableControllerPlayerIndex(playerIndex)) {
        return {};
    }
    return "controller_player_index_" + std::to_string(playerIndex);
}

std::vector<std::string> controllerClaimKeyFallbackOrderForPlayerIndex(int preferredPlayerIndex) {
    if (!isStableControllerPlayerIndex(preferredPlayerIndex)) {
        return {};
    }

    std::vector<std::string> keys;
    keys.reserve(static_cast<std::size_t>(kMaxStableControllerPlayerIndex -
                                          kMinStableControllerPlayerIndex + 1));
    keys.push_back(controllerClaimKeyForPlayerIndex(preferredPlayerIndex));
    for (int index = kMinStableControllerPlayerIndex; index <= kMaxStableControllerPlayerIndex; ++index) {
        if (index == preferredPlayerIndex) {
            continue;
        }
        keys.push_back(controllerClaimKeyForPlayerIndex(index));
    }
    return keys;
}

bool tryAcquireControllerClaimWithPlayerIndexFallback(int preferredPlayerIndex,
                                                      ControllerClaimLease& lease,
                                                      std::string* claimedKey,
                                                      std::string* error) {
    if (claimedKey != nullptr) {
        claimedKey->clear();
    }

    const std::vector<std::string> keys = controllerClaimKeyFallbackOrderForPlayerIndex(preferredPlayerIndex);
    if (keys.empty()) {
        if (error != nullptr) {
            *error = "Controller claim failed: unstable player index";
        }
        return false;
    }

    std::string firstError;
    std::string attemptError;
    for (const std::string& key : keys) {
        if (tryAcquireControllerClaim(key, lease, &attemptError)) {
            if (claimedKey != nullptr) {
                *claimedKey = key;
            }
            if (error != nullptr) {
                error->clear();
            }
            return true;
        }
        if (firstError.empty()) {
            firstError = attemptError;
        }
    }

    if (error != nullptr) {
        *error = firstError.empty() ? "Controller claim failed: all fallback keys unavailable" : firstError;
    }
    return false;
}
