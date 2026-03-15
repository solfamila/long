#pragma once

#include "tape_engine_client.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

namespace tapescope {

using json = nlohmann::json;

inline constexpr const char* kDefaultSocketPath = "/tmp/tape-engine.sock";

enum class QueryErrorKind {
    None,
    Transport,
    Remote,
    MalformedResponse
};

struct QueryError {
    QueryErrorKind kind = QueryErrorKind::None;
    std::string message;
};

template <typename T>
struct QueryResult {
    T value{};
    QueryError error{};

    [[nodiscard]] bool ok() const {
        return error.kind == QueryErrorKind::None;
    }
};

struct ClientConfig {
    std::string socketPath;
};

struct StatusSnapshot {
    std::string socketPath;
    std::string dataDir;
    std::string instrumentId;
    std::uint64_t latestSessionSeq = 0;
    std::uint64_t liveEventCount = 0;
    std::uint64_t segmentCount = 0;
    std::string manifestHash;
};

struct RangeQuery {
    std::uint64_t firstSessionSeq = 1;
    std::uint64_t lastSessionSeq = 128;
};

struct OrderAnchorQuery {
    std::optional<std::uint64_t> traceId;
    std::optional<long long> orderId;
    std::optional<long long> permId;
    std::optional<std::string> execId;
};

std::string defaultSocketPath();

class QueryClient {
public:
    explicit QueryClient(ClientConfig config = {});

    [[nodiscard]] const ClientConfig& config() const;

    [[nodiscard]] QueryResult<StatusSnapshot> status() const;
    [[nodiscard]] QueryResult<std::vector<json>> readLiveTail(std::size_t limit = 64) const;
    [[nodiscard]] QueryResult<std::vector<json>> readRange(const RangeQuery& query) const;
    [[nodiscard]] QueryResult<json> readSessionOverview(const RangeQuery& query) const;
    [[nodiscard]] QueryResult<json> scanSessionReport(const RangeQuery& query) const;
    [[nodiscard]] QueryResult<json> listSessionReports(std::size_t limit = 20) const;
    [[nodiscard]] QueryResult<json> findOrderAnchor(const OrderAnchorQuery& query) const;
    [[nodiscard]] QueryResult<json> seekOrderAnchor(const OrderAnchorQuery& query) const;
    [[nodiscard]] QueryResult<json> readOrderCase(const OrderAnchorQuery& query) const;
    [[nodiscard]] QueryResult<json> scanOrderCaseReport(const OrderAnchorQuery& query) const;
    [[nodiscard]] QueryResult<json> listCaseReports(std::size_t limit = 20) const;
    [[nodiscard]] QueryResult<json> listIncidents(std::size_t limit = 20) const;
    [[nodiscard]] QueryResult<json> readIncident(std::uint64_t logicalIncidentId) const;
    [[nodiscard]] QueryResult<json> readArtifact(const std::string& artifactId) const;
    [[nodiscard]] QueryResult<json> exportArtifact(const std::string& artifactId, const std::string& exportFormat) const;

    [[nodiscard]] static std::string describeError(const QueryError& error);

private:
    [[nodiscard]] QueryResult<tape_engine::QueryResponse> performQuery(const tape_engine::QueryRequest& request) const;
    [[nodiscard]] static QueryError classifyFailure(const std::string& errorMessage);
    [[nodiscard]] static QueryResult<json> packSummaryAndEvents(const QueryResult<tape_engine::QueryResponse>& response);

    ClientConfig config_;
    tape_engine::Client client_;
};

} // namespace tapescope
