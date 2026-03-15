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

struct EvidenceCitation {
    std::string kind;
    std::string artifactId;
    std::string label;
    json raw;
};

struct EventRow {
    std::uint64_t sessionSeq = 0;
    std::uint64_t sourceSeq = 0;
    std::string eventKind;
    std::string instrumentId;
    std::string side;
    std::optional<double> price;
    std::string summary;
    json raw;
};

struct EventListPayload {
    json raw;
    json summary;
    std::vector<EventRow> events;
};

struct IncidentListRow {
    std::uint64_t logicalIncidentId = 0;
    std::string kind;
    double score = 0.0;
    std::string title;
    json raw;
};

struct InvestigationPayload {
    json raw;
    json summary;
    std::vector<json> events;
    std::vector<IncidentListRow> incidents;
    std::vector<EvidenceCitation> evidence;
    std::optional<RangeQuery> replayRange;
    std::string artifactId;
    std::string artifactKind;
    std::string headline;
    std::string detail;
};

struct SessionQualityPayload {
    json raw;
    json summary;
    json dataQuality;
};

struct SeekOrderPayload {
    json raw;
    json summary;
    std::optional<RangeQuery> replayRange;
    std::uint64_t replayTargetSessionSeq = 0;
    std::uint64_t firstSessionSeq = 0;
    std::uint64_t lastSessionSeq = 0;
    std::uint64_t lastFillSessionSeq = 0;
};

struct IncidentListPayload {
    json raw;
    std::vector<IncidentListRow> incidents;
};

struct ReportInventoryRow {
    std::uint64_t reportId = 0;
    std::uint64_t revisionId = 0;
    std::string artifactId;
    std::string reportType;
    std::string headline;
    json raw;
};

struct ReportInventoryPayload {
    json raw;
    std::vector<ReportInventoryRow> sessionReports;
    std::vector<ReportInventoryRow> caseReports;
};

struct ArtifactExportPayload {
    json raw;
    json summary;
    json artifactExport;
    std::string artifactId;
    std::string format;
    std::uint64_t servedRevisionId = 0;
    std::string markdown;
    json bundle = json::object();
};

std::string defaultSocketPath();

class QueryClient {
public:
    explicit QueryClient(ClientConfig config = {});

    [[nodiscard]] const ClientConfig& config() const;

    [[nodiscard]] QueryResult<StatusSnapshot> status() const;
    [[nodiscard]] QueryResult<std::vector<json>> readLiveTail(std::size_t limit = 64) const;
    [[nodiscard]] QueryResult<std::vector<EventRow>> readLiveTailRows(std::size_t limit = 64) const;
    [[nodiscard]] QueryResult<std::vector<json>> readRange(const RangeQuery& query) const;
    [[nodiscard]] QueryResult<std::vector<EventRow>> readRangeRows(const RangeQuery& query) const;
    // Legacy JSON envelopes are retained for compatibility, but the UI and tests
    // should prefer the typed payload variants below.
    [[nodiscard]] [[deprecated("Use readSessionQualityPayload() for typed TapeScope quality reads")]]
    QueryResult<json> readSessionQuality(const RangeQuery& query, bool includeLiveTail = false) const;
    [[nodiscard]] QueryResult<SessionQualityPayload> readSessionQualityPayload(const RangeQuery& query,
                                                                               bool includeLiveTail = false) const;
    [[nodiscard]] [[deprecated("Use readSessionOverviewPayload() for typed TapeScope investigation reads")]]
    QueryResult<json> readSessionOverview(const RangeQuery& query) const;
    [[nodiscard]] QueryResult<InvestigationPayload> readSessionOverviewPayload(const RangeQuery& query) const;
    [[nodiscard]] [[deprecated("Use scanSessionReportPayload() for typed TapeScope report reads")]]
    QueryResult<json> scanSessionReport(const RangeQuery& query) const;
    [[nodiscard]] QueryResult<InvestigationPayload> scanSessionReportPayload(const RangeQuery& query) const;
    [[nodiscard]] [[deprecated("Use listSessionReportsPayload() for typed TapeScope report inventory reads")]]
    QueryResult<json> listSessionReports(std::size_t limit = 20) const;
    [[nodiscard]] QueryResult<ReportInventoryPayload> listSessionReportsPayload(std::size_t limit = 20) const;
    [[nodiscard]] [[deprecated("Use findOrderAnchorPayload() for typed TapeScope order-anchor reads")]]
    QueryResult<json> findOrderAnchor(const OrderAnchorQuery& query) const;
    [[nodiscard]] QueryResult<EventListPayload> findOrderAnchorPayload(const OrderAnchorQuery& query) const;
    [[nodiscard]] [[deprecated("Use seekOrderAnchorPayload() for typed TapeScope replay-target reads")]]
    QueryResult<json> seekOrderAnchor(const OrderAnchorQuery& query) const;
    [[nodiscard]] QueryResult<SeekOrderPayload> seekOrderAnchorPayload(const OrderAnchorQuery& query) const;
    [[nodiscard]] [[deprecated("Use readFindingPayload() for typed TapeScope investigation reads")]]
    QueryResult<json> readFinding(std::uint64_t findingId) const;
    [[nodiscard]] QueryResult<InvestigationPayload> readFindingPayload(std::uint64_t findingId) const;
    [[nodiscard]] [[deprecated("Use readOrderCasePayload() for typed TapeScope investigation reads")]]
    QueryResult<json> readOrderCase(const OrderAnchorQuery& query) const;
    [[nodiscard]] QueryResult<InvestigationPayload> readOrderCasePayload(const OrderAnchorQuery& query) const;
    [[nodiscard]] [[deprecated("Use scanOrderCaseReportPayload() for typed TapeScope report reads")]]
    QueryResult<json> scanOrderCaseReport(const OrderAnchorQuery& query) const;
    [[nodiscard]] QueryResult<InvestigationPayload> scanOrderCaseReportPayload(const OrderAnchorQuery& query) const;
    [[nodiscard]] [[deprecated("Use listCaseReportsPayload() for typed TapeScope report inventory reads")]]
    QueryResult<json> listCaseReports(std::size_t limit = 20) const;
    [[nodiscard]] QueryResult<ReportInventoryPayload> listCaseReportsPayload(std::size_t limit = 20) const;
    [[nodiscard]] [[deprecated("Use listIncidentsPayload() for typed TapeScope incident reads")]]
    QueryResult<json> listIncidents(std::size_t limit = 20) const;
    [[nodiscard]] QueryResult<IncidentListPayload> listIncidentsPayload(std::size_t limit = 20) const;
    [[nodiscard]] [[deprecated("Use readIncidentPayload() for typed TapeScope investigation reads")]]
    QueryResult<json> readIncident(std::uint64_t logicalIncidentId) const;
    [[nodiscard]] QueryResult<InvestigationPayload> readIncidentPayload(std::uint64_t logicalIncidentId) const;
    [[nodiscard]] [[deprecated("Use readOrderAnchorPayload() for typed TapeScope investigation reads")]]
    QueryResult<json> readOrderAnchor(std::uint64_t anchorId) const;
    [[nodiscard]] QueryResult<InvestigationPayload> readOrderAnchorPayload(std::uint64_t anchorId) const;
    [[nodiscard]] [[deprecated("Use readArtifactPayload() for typed TapeScope investigation reads")]]
    QueryResult<json> readArtifact(const std::string& artifactId) const;
    [[nodiscard]] QueryResult<InvestigationPayload> readArtifactPayload(const std::string& artifactId) const;
    [[nodiscard]] [[deprecated("Use exportArtifactPayload() for typed TapeScope artifact exports")]]
    QueryResult<json> exportArtifact(const std::string& artifactId, const std::string& exportFormat) const;
    [[nodiscard]] QueryResult<ArtifactExportPayload> exportArtifactPayload(const std::string& artifactId,
                                                                           const std::string& exportFormat) const;

    [[nodiscard]] static std::string describeError(const QueryError& error);

private:
    [[nodiscard]] QueryResult<tape_engine::QueryResponse> performQuery(const tape_engine::QueryRequest& request) const;
    [[nodiscard]] static QueryError classifyFailure(const std::string& errorMessage);
    [[nodiscard]] static QueryResult<json> packSummaryAndEvents(const QueryResult<tape_engine::QueryResponse>& response);

    ClientConfig config_;
    tape_engine::Client client_;
};

} // namespace tapescope
