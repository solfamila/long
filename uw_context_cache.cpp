#include "uw_context_cache.h"

#include "uw_runtime.h"

#include <fstream>
#include <mutex>
#include <unordered_map>

namespace uw_context_service {

ContextCache::ContextCache(std::filesystem::path rootDir)
    : rootDir_(std::move(rootDir)) {}

void ContextCache::setRootDir(std::filesystem::path rootDir) {
    std::scoped_lock lock(mutex_);
    rootDir_ = std::move(rootDir);
}

std::filesystem::path ContextCache::entryPathForKey(const std::string& key) const {
    if (rootDir_.empty()) {
        return {};
    }
    return rootDir_ / (stableHashHex(key) + ".json");
}

std::optional<CacheEntry> ContextCache::lookup(const std::string& key,
                                               std::uint64_t maxAgeSeconds) const {
    std::scoped_lock lock(mutex_);
    const auto it = entries_.find(key);
    if (it != entries_.end()) {
        const std::uint64_t age = nowUnixSeconds() > it->second.fetchedAtUnixSeconds
            ? nowUnixSeconds() - it->second.fetchedAtUnixSeconds
            : 0;
        if (it->second.fetchedAtUnixSeconds == 0 || age <= maxAgeSeconds) {
            return it->second;
        }
    }

    const std::filesystem::path entryPath = entryPathForKey(key);
    if (entryPath.empty()) {
        return std::nullopt;
    }
    std::ifstream input(entryPath);
    if (!input.is_open()) {
        return std::nullopt;
    }
    const json payload = json::parse(input, nullptr, false);
    if (payload.is_discarded() || !payload.is_object()) {
        return std::nullopt;
    }
    CacheEntry entry;
    entry.externalContext = payload.value("external_context", json::object());
    entry.interpretation = payload.value("interpretation", json::object());
    entry.packetArtifact = payload.value("packet_artifact", json::object());
    entry.fetchedAtUtc = payload.value("fetched_at_utc", std::string());
    entry.fetchedAtUnixSeconds = payload.value("fetched_at_unix_seconds", 0ULL);
    entry.providerPathUsed = payload.value("provider_path_used", std::string());
    const std::uint64_t age = nowUnixSeconds() > entry.fetchedAtUnixSeconds
        ? nowUnixSeconds() - entry.fetchedAtUnixSeconds
        : 0;
    if (entry.fetchedAtUnixSeconds > 0 && age > maxAgeSeconds) {
        return std::nullopt;
    }
    entries_[key] = entry;
    return entry;
}

void ContextCache::store(std::string key, CacheEntry entry) {
    std::scoped_lock lock(mutex_);
    const std::string stableKey = key;
    entries_[stableKey] = std::move(entry);
    const auto stored = entries_.find(stableKey);
    if (stored == entries_.end()) {
        return;
    }
    const std::filesystem::path entryPath = entryPathForKey(stored->first);
    if (entryPath.empty()) {
        return;
    }
    std::error_code ec;
    std::filesystem::create_directories(entryPath.parent_path(), ec);
    std::ofstream output(entryPath);
    if (!output.is_open()) {
        return;
    }
    output << json{
        {"external_context", stored->second.externalContext},
        {"interpretation", stored->second.interpretation},
        {"packet_artifact", stored->second.packetArtifact},
        {"fetched_at_utc", stored->second.fetchedAtUtc},
        {"fetched_at_unix_seconds", stored->second.fetchedAtUnixSeconds},
        {"provider_path_used", stored->second.providerPathUsed}
    }.dump(2);
}

} // namespace uw_context_service