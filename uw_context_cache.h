#pragma once

#include "uw_context_types.h"

#include <filesystem>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

namespace uw_context_service {

struct CacheEntry {
    json externalContext = json::array();
    json interpretation = json::object();
    json packetArtifact = json::object();
    std::string fetchedAtUtc;
    std::uint64_t fetchedAtUnixSeconds = 0;
    std::string providerPathUsed;
};

class ContextCache {
public:
    explicit ContextCache(std::filesystem::path rootDir = {});

    void setRootDir(std::filesystem::path rootDir);
    [[nodiscard]] std::optional<CacheEntry> lookup(const std::string& key,
                                                   std::uint64_t maxAgeSeconds) const;
    void store(std::string key, CacheEntry entry);

private:
    [[nodiscard]] std::filesystem::path entryPathForKey(const std::string& key) const;

    mutable std::mutex mutex_;
    mutable std::unordered_map<std::string, CacheEntry> entries_;
    std::filesystem::path rootDir_;
};

} // namespace uw_context_service