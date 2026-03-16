#pragma once

#include "tape_engine_client.h"

#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

namespace tape_payloads {

using json = nlohmann::json;

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

struct CollectionRowsPayload {
    json raw;
    json summary;
    std::string collectionKind;
    std::uint64_t servedRevisionId = 0;
    bool includesMutableTail = false;
    std::size_t totalCount = 0;
    std::vector<json> rows;
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

struct BundleExportPayload {
    json raw;
    json summary;
    json artifact;
    json bundle;
    json sourceArtifact;
    json sourceReport;
    std::string artifactId;
    std::string bundleId;
    std::string bundleType;
    std::string bundlePath;
    std::uint64_t servedRevisionId = 0;
};

struct ImportedCaseRow {
    std::uint64_t importedCaseId = 0;
    std::string artifactId;
    std::string bundleId;
    std::string bundleType;
    std::string sourceArtifactId;
    std::uint64_t sourceReportId = 0;
    std::uint64_t sourceRevisionId = 0;
    std::uint64_t firstSessionSeq = 0;
    std::uint64_t lastSessionSeq = 0;
    std::string instrumentId;
    std::string headline;
    std::string fileName;
    std::string sourceBundlePath;
    std::string payloadSha256;
    json raw;
};

struct ImportedCaseListPayload {
    json raw;
    std::vector<ImportedCaseRow> importedCases;
};

struct BundleVerifyPayload {
    json raw;
    json summary;
    json artifact;
    json bundle;
    json sourceArtifact;
    json sourceReport;
    json reportSummary;
    ImportedCaseRow importedCase;
    bool hasImportedCase = false;
    bool importSupported = false;
    bool alreadyImported = false;
    bool canImport = false;
    std::string verifyStatus;
    std::string importReason;
    std::string artifactId;
    std::string bundleId;
    std::string bundleType;
    std::string bundlePath;
    std::string payloadSha256;
    std::uint64_t servedRevisionId = 0;
    std::string reportMarkdown;
};

struct CaseBundleImportPayload {
    json raw;
    json summary;
    json artifact;
    ImportedCaseRow importedCase;
    bool duplicateImport = false;
    std::string importStatus;
    std::string artifactId;
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

struct ReplaySnapshotPayload {
    json raw;
    json summary;
    std::uint64_t servedRevisionId = 0;
    bool includesMutableTail = false;
    std::uint64_t targetSessionSeq = 0;
    std::uint64_t replayedThroughSessionSeq = 0;
    std::uint64_t appliedEventCount = 0;
    std::uint64_t gapMarkersEncountered = 0;
    bool checkpointUsed = false;
    std::uint64_t checkpointRevisionId = 0;
    std::uint64_t checkpointSessionSeq = 0;
    json bidPrice = nullptr;
    json askPrice = nullptr;
    json lastPrice = nullptr;
    json bidBook = json::array();
    json askBook = json::array();
    json dataQuality = json::object();
};

template <typename T>
QueryResult<T> makeError(QueryErrorKind kind, std::string message) {
    QueryResult<T> result;
    result.error.kind = kind;
    result.error.message = std::move(message);
    return result;
}

template <typename T>
QueryResult<T> makeSuccess(T value) {
    QueryResult<T> result;
    result.value = std::move(value);
    return result;
}

template <typename T>
QueryResult<T> propagateError(const QueryError& error) {
    QueryResult<T> result;
    result.error = error;
    return result;
}

std::string firstPresentString(const json& payload,
                               std::initializer_list<const char*> keys);
bool parseReplayRange(const json& summary, RangeQuery* range);
bool parseSeekReplayRange(const json& summary, RangeQuery* range);
std::vector<EvidenceCitation> parseEvidenceCitations(const json& summary);
EventRow parseEventRow(const json& event);
std::vector<EventRow> parseEventRows(const json& events);
std::vector<IncidentListRow> parseIncidentRows(const json& events);
std::vector<ReportInventoryRow> parseReportRows(const json& events);

QueryResult<StatusSnapshot> packStatusPayload(const QueryResult<tape_engine::QueryResponse>& response);
QueryResult<InvestigationPayload> packInvestigationPayload(const QueryResult<tape_engine::QueryResponse>& response);
QueryResult<EventListPayload> packEventListPayload(const QueryResult<tape_engine::QueryResponse>& response);
QueryResult<SessionQualityPayload> packSessionQualityPayload(const QueryResult<tape_engine::QueryResponse>& response);
QueryResult<SeekOrderPayload> packSeekOrderPayload(const QueryResult<tape_engine::QueryResponse>& response);
QueryResult<IncidentListPayload> packIncidentListPayload(const QueryResult<tape_engine::QueryResponse>& response);
QueryResult<CollectionRowsPayload> packCollectionRowsPayload(const QueryResult<tape_engine::QueryResponse>& response,
                                                             const char* expectedKind);
QueryResult<ReportInventoryPayload> packReportInventoryPayload(const QueryResult<tape_engine::QueryResponse>& response,
                                                               bool sessionReports);
QueryResult<BundleExportPayload> packBundleExportPayload(const QueryResult<tape_engine::QueryResponse>& response);
QueryResult<BundleVerifyPayload> packBundleVerifyPayload(const QueryResult<tape_engine::QueryResponse>& response);
QueryResult<ImportedCaseListPayload> packImportedCaseListPayload(const QueryResult<tape_engine::QueryResponse>& response);
QueryResult<CaseBundleImportPayload> packCaseBundleImportPayload(const QueryResult<tape_engine::QueryResponse>& response);
QueryResult<ArtifactExportPayload> packArtifactExportPayload(const QueryResult<tape_engine::QueryResponse>& response);
QueryResult<ReplaySnapshotPayload> packReplaySnapshotPayload(const QueryResult<tape_engine::QueryResponse>& response);

} // namespace tape_payloads
