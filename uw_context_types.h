#pragma once

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

namespace uw_context_service {

using json = nlohmann::json;

enum class Lane {
    Fast,
    Deep
};

struct BuildRequest {
    std::string requestKind;
    Lane lane = Lane::Fast;
    bool forceRefresh = false;
    std::filesystem::path dataDir;
    std::uint64_t revisionId = 0;
    std::uint64_t logicalIncidentId = 0;
    std::uint64_t traceId = 0;
    long long orderId = 0;
    long long permId = 0;
    std::string execId;
    std::size_t limit = 0;
    bool includeLiveTail = false;
    std::vector<std::string> requestedFacets;
};

struct ProviderStep {
    std::string provider;
    std::string status;
    std::string reason;
    std::uint64_t latencyMs = 0;
    json requestPayload = json::object();
    json rawRecords = json::array();
    json metadata = json::object();
};

struct FetchPlan {
    std::string artifactId;
    std::string requestKind;
    std::string symbol;
    std::uint64_t revisionId = 0;
    std::uint64_t logicalIncidentId = 0;
    std::uint64_t traceId = 0;
    long long orderId = 0;
    long long permId = 0;
    std::string execId;
    std::uint64_t windowStartNs = 0;
    std::uint64_t windowEndNs = 0;
    bool forceRefresh = false;
    std::vector<std::string> evidenceKinds;
    std::vector<std::string> facets;
};

struct NormalizedContext {
    std::string provider = "unusual_whales";
    std::string fetchedAtUtc;
    std::string cacheStatus = "miss";
    json items = json::array();
    json summaries = json::array();
    json warnings = json::array();
    json sourceAttribution = json::array();
    std::vector<std::string> schemaSources;
};

struct GeminiExecutionResult {
    std::string status = "unavailable";
    std::string model;
    std::string finishReason;
    std::uint64_t latencyMs = 0;
    bool jsonValid = false;
    bool schemaValid = false;
    json content = json(nullptr);
    json warnings = json::array();
    std::string rawText;
    std::string error;
};

} // namespace uw_context_service