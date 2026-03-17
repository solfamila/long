#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace tape_engine {

inline constexpr std::uint32_t kAckWireVersion = 1;
inline constexpr const char* kAckSchema = "com.foxy.tape-engine.ingest-ack";
inline constexpr const char* kQueryRequestSchema = "com.foxy.tape-engine.query";
inline constexpr const char* kQueryResponseSchema = "com.foxy.tape-engine.query-response";
inline constexpr std::uint32_t kInvestigationEnvelopeVersion = 1;
inline constexpr const char* kInvestigationEnvelopeSchema = "com.foxy.tape-engine.investigation-envelope";
inline constexpr std::uint32_t kArtifactExportVersion = 1;
inline constexpr const char* kArtifactExportSchema = "com.foxy.tape-engine.artifact-export";
inline constexpr std::uint32_t kReportBundleVersion = 1;
inline constexpr const char* kReportBundleSchema = "com.foxy.tape-engine.report-bundle";
inline constexpr std::uint32_t kImportedCaseListVersion = 1;
inline constexpr const char* kImportedCaseListSchema = "com.foxy.tape-engine.imported-case-list";
inline constexpr std::uint32_t kStatusResultVersion = 1;
inline constexpr const char* kStatusResultSchema = "com.foxy.tape-engine.status-result";
inline constexpr std::uint32_t kEventListResultVersion = 1;
inline constexpr const char* kEventListResultSchema = "com.foxy.tape-engine.event-list-result";
inline constexpr std::uint32_t kSessionQualityResultVersion = 1;
inline constexpr const char* kSessionQualityResultSchema = "com.foxy.tape-engine.session-quality-result";
inline constexpr std::uint32_t kInvestigationResultVersion = 1;
inline constexpr const char* kInvestigationResultSchema = "com.foxy.tape-engine.investigation-result";
inline constexpr std::uint32_t kCollectionResultVersion = 1;
inline constexpr const char* kCollectionResultSchema = "com.foxy.tape-engine.collection-result";
inline constexpr std::uint32_t kSeekOrderResultVersion = 1;
inline constexpr const char* kSeekOrderResultSchema = "com.foxy.tape-engine.seek-order-result";
inline constexpr std::uint32_t kArtifactExportResultVersion = 1;
inline constexpr const char* kArtifactExportResultSchema = "com.foxy.tape-engine.artifact-export-result";
inline constexpr std::uint32_t kReplaySnapshotResultVersion = 1;
inline constexpr const char* kReplaySnapshotResultSchema = "com.foxy.tape-engine.replay-snapshot-result";
inline constexpr std::uint32_t kEnrichmentResultVersion = 1;
inline constexpr const char* kEnrichmentResultSchema = "com.foxy.tape-engine.enrichment-result";
inline constexpr std::uint32_t kBundleExportResultVersion = 1;
inline constexpr const char* kBundleExportResultSchema = "com.foxy.tape-engine.bundle-export-result";
inline constexpr std::uint32_t kBundleVerifyResultVersion = 1;
inline constexpr const char* kBundleVerifyResultSchema = "com.foxy.tape-engine.bundle-verify-result";
inline constexpr std::uint32_t kCaseBundleImportResultVersion = 1;
inline constexpr const char* kCaseBundleImportResultSchema = "com.foxy.tape-engine.case-bundle-import-result";
inline constexpr std::uint32_t kImportedCaseInventoryResultVersion = 1;
inline constexpr const char* kImportedCaseInventoryResultSchema = "com.foxy.tape-engine.imported-case-inventory-result";

enum class QueryOperation {
    Unknown = 0,
    Status,
    ReadLiveTail,
    ReadRange,
    ReplaySnapshot,
    ReadSessionQuality,
    ReadSessionOverview,
    ScanSessionReport,
    ReadSessionReport,
    ListSessionReports,
    ScanIncidentReport,
    ScanOrderCaseReport,
    ReadCaseReport,
    ListCaseReports,
    FindOrderAnchor,
    SeekOrderAnchor,
    ReadOrderCase,
    ReadOrderAnchor,
    ListOrderAnchors,
    ListProtectedWindows,
    ReadProtectedWindow,
    ListFindings,
    ReadFinding,
    ReadIncident,
    ListIncidents,
    EnrichIncident,
    ExplainIncident,
    EnrichOrderCase,
    RefreshExternalContext,
    ReadArtifact,
    ExportArtifact,
    ExportSessionBundle,
    ExportCaseBundle,
    VerifyBundle,
    ImportCaseBundle,
    ListImportedCases,
};

const char* queryOperationName(QueryOperation operation);
std::string canonicalizeQueryOperationName(std::string_view operationName);
QueryOperation queryOperationFromString(std::string_view operationName);

struct IngestAck {
    std::uint32_t version = kAckWireVersion;
    std::string schema = kAckSchema;
    std::string status = "accepted";
    std::uint64_t batchSeq = 0;
    std::uint64_t assignedRevisionId = 0;
    std::string adapterId;
    std::string connectionId;
    std::uint64_t acceptedRecords = 0;
    std::uint64_t duplicateRecords = 0;
    std::uint64_t gapMarkers = 0;
    std::uint64_t firstSessionSeq = 0;
    std::uint64_t lastSessionSeq = 0;
    std::uint64_t firstSourceSeq = 0;
    std::uint64_t lastSourceSeq = 0;
    std::string error;
};

json ackToJson(const IngestAck& ack);
IngestAck ackFromJson(const json& payload);

std::vector<std::uint8_t> encodeAckPayload(const IngestAck& ack);
IngestAck decodeAckPayload(const std::vector<std::uint8_t>& payload);

std::vector<std::uint8_t> encodeAckFrame(const IngestAck& ack);
IngestAck decodeAckFrame(const std::vector<std::uint8_t>& frame);

struct QueryRequest {
    std::uint32_t version = kAckWireVersion;
    std::string schema = kQueryRequestSchema;
    std::string requestId;
    std::string operation;
    QueryOperation operationKind = QueryOperation::Unknown;
    std::uint64_t revisionId = 0;
    std::uint64_t fromSessionSeq = 0;
    std::uint64_t toSessionSeq = 0;
    std::uint64_t targetSessionSeq = 0;
    std::uint64_t windowId = 0;
    std::uint64_t logicalIncidentId = 0;
    std::uint64_t findingId = 0;
    std::uint64_t anchorId = 0;
    std::uint64_t reportId = 0;
    std::size_t limit = 0;
    bool includeLiveTail = false;
    std::uint64_t traceId = 0;
    long long orderId = 0;
    long long permId = 0;
    std::string execId;
    std::string artifactId;
    std::string exportFormat;
    std::string bundlePath;
};

QueryRequest makeQueryRequest(QueryOperation operation, std::string requestId = {});

struct QueryResponse {
    std::uint32_t version = kAckWireVersion;
    std::string schema = kQueryResponseSchema;
    std::string requestId;
    std::string operation;
    std::string status = "ok";
    std::string error;
    json result = json::object();
    json summary = json::object();
    json events = json::array();
};

json queryRequestToJson(const QueryRequest& request);
QueryRequest queryRequestFromJson(const json& payload);

json queryResponseToJson(const QueryResponse& response);
QueryResponse queryResponseFromJson(const json& payload);

std::vector<std::uint8_t> encodeQueryRequestPayload(const QueryRequest& request);
QueryRequest decodeQueryRequestPayload(const std::vector<std::uint8_t>& payload);

std::vector<std::uint8_t> encodeQueryRequestFrame(const QueryRequest& request);
QueryRequest decodeQueryRequestFrame(const std::vector<std::uint8_t>& frame);

std::vector<std::uint8_t> encodeQueryResponsePayload(const QueryResponse& response);
QueryResponse decodeQueryResponsePayload(const std::vector<std::uint8_t>& payload);

std::vector<std::uint8_t> encodeQueryResponseFrame(const QueryResponse& response);
QueryResponse decodeQueryResponseFrame(const std::vector<std::uint8_t>& frame);

json decodeFramedJson(const std::vector<std::uint8_t>& frame);
std::vector<std::uint8_t> encodeFramedJson(const json& payload);

} // namespace tape_engine
