#pragma once

#include "tape_engine_client.h"
#include "tape_query_payloads.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>

namespace tape_mcp {

enum class EngineRpcErrorKind {
    None = 0,
    Configuration,
    Transport,
    Remote,
    MalformedResponse
};

struct EngineRpcError {
    EngineRpcErrorKind kind = EngineRpcErrorKind::None;
    std::string code = "ok";
    std::string message;
    bool retryable = false;
};

template <typename T>
struct EngineRpcResult {
    T value{};
    EngineRpcError error{};

    [[nodiscard]] bool ok() const {
        return error.kind == EngineRpcErrorKind::None;
    }
};

struct EngineRpcClientConfig {
    std::string socketPath;
};

struct SessionWindowQuery {
    std::uint64_t firstSessionSeq = 0;
    std::uint64_t lastSessionSeq = 0;
    std::uint64_t revisionId = 0;
    std::size_t limit = 0;
    bool includeLiveTail = false;
};

struct ListQuery {
    std::uint64_t revisionId = 0;
    std::size_t limit = 0;
};

struct OrderCaseQuery {
    std::optional<std::uint64_t> traceId;
    std::optional<long long> orderId;
    std::optional<long long> permId;
    std::optional<std::string> execId;
    std::uint64_t revisionId = 0;
    std::size_t limit = 0;
    bool includeLiveTail = false;
};

struct NumericIdQuery {
    std::uint64_t id = 0;
    std::uint64_t revisionId = 0;
    std::size_t limit = 0;
    bool includeLiveTail = false;
};

struct ReplaySnapshotQuery {
    std::uint64_t targetSessionSeq = 0;
    std::uint64_t revisionId = 0;
    std::size_t depthLimit = 0;
    bool includeLiveTail = false;
};

struct IncidentQuery {
    std::uint64_t logicalIncidentId = 0;
    std::uint64_t revisionId = 0;
    std::size_t limit = 0;
    bool includeLiveTail = false;
};

struct ArtifactQuery {
    std::string artifactId;
    std::uint64_t revisionId = 0;
    std::size_t limit = 0;
    bool includeLiveTail = false;
};

struct ArtifactExportQuery {
    std::string artifactId;
    std::string exportFormat;
    std::uint64_t revisionId = 0;
    std::size_t limit = 0;
    bool includeLiveTail = false;
};

struct BundleExportQuery {
    std::uint64_t reportId = 0;
};

struct BundleImportQuery {
    std::string bundlePath;
};

class EngineRpcClient {
public:
    explicit EngineRpcClient(EngineRpcClientConfig config = {});

    [[nodiscard]] const EngineRpcClientConfig& config() const;

    [[nodiscard]] EngineRpcResult<tape_payloads::StatusSnapshot> status() const;
    [[nodiscard]] EngineRpcResult<tape_payloads::EventListPayload> readLiveTail(std::size_t limit) const;
    [[nodiscard]] EngineRpcResult<tape_payloads::EventListPayload> readRange(
        const SessionWindowQuery& query) const;
    [[nodiscard]] EngineRpcResult<tape_payloads::EventListPayload> findOrderAnchor(
        const OrderCaseQuery& query) const;
    [[nodiscard]] EngineRpcResult<tape_payloads::IncidentListPayload> listIncidents(
        const ListQuery& query) const;
    [[nodiscard]] EngineRpcResult<tape_engine::QueryResponse> listOrderAnchors(
        const ListQuery& query) const;
    [[nodiscard]] EngineRpcResult<tape_engine::QueryResponse> listProtectedWindows(
        const ListQuery& query) const;
    [[nodiscard]] EngineRpcResult<tape_engine::QueryResponse> listFindings(
        const ListQuery& query) const;
    [[nodiscard]] EngineRpcResult<tape_payloads::InvestigationPayload> readSessionOverview(
        const SessionWindowQuery& query) const;
    [[nodiscard]] EngineRpcResult<tape_payloads::InvestigationPayload> scanSessionReport(
        const SessionWindowQuery& query) const;
    [[nodiscard]] EngineRpcResult<tape_payloads::InvestigationPayload> readSessionReport(
        const NumericIdQuery& query) const;
    [[nodiscard]] EngineRpcResult<tape_payloads::ReportInventoryPayload> listSessionReports(
        const ListQuery& query) const;
    [[nodiscard]] EngineRpcResult<tape_payloads::InvestigationPayload> scanIncidentReport(
        const IncidentQuery& query) const;
    [[nodiscard]] EngineRpcResult<tape_payloads::InvestigationPayload> scanOrderCaseReport(
        const OrderCaseQuery& query) const;
    [[nodiscard]] EngineRpcResult<tape_payloads::InvestigationPayload> readCaseReport(
        const NumericIdQuery& query) const;
    [[nodiscard]] EngineRpcResult<tape_payloads::ReportInventoryPayload> listCaseReports(
        const ListQuery& query) const;
    [[nodiscard]] EngineRpcResult<tape_payloads::SeekOrderPayload> seekOrderAnchor(
        const OrderCaseQuery& query) const;
    [[nodiscard]] EngineRpcResult<tape_payloads::InvestigationPayload> readFinding(
        const NumericIdQuery& query) const;
    [[nodiscard]] EngineRpcResult<tape_payloads::InvestigationPayload> readOrderCase(
        const OrderCaseQuery& query) const;
    [[nodiscard]] EngineRpcResult<tape_payloads::InvestigationPayload> readIncident(
        const IncidentQuery& query) const;
    [[nodiscard]] EngineRpcResult<tape_payloads::InvestigationPayload> readOrderAnchor(
        const NumericIdQuery& query) const;
    [[nodiscard]] EngineRpcResult<tape_engine::QueryResponse> readProtectedWindow(
        const NumericIdQuery& query) const;
    [[nodiscard]] EngineRpcResult<tape_engine::QueryResponse> replaySnapshot(
        const ReplaySnapshotQuery& query) const;
    [[nodiscard]] EngineRpcResult<tape_payloads::InvestigationPayload> readArtifact(
        const ArtifactQuery& query) const;
    [[nodiscard]] EngineRpcResult<tape_payloads::ArtifactExportPayload> exportArtifact(
        const ArtifactExportQuery& query) const;
    [[nodiscard]] EngineRpcResult<tape_engine::QueryResponse> exportSessionBundle(
        const BundleExportQuery& query) const;
    [[nodiscard]] EngineRpcResult<tape_engine::QueryResponse> exportCaseBundle(
        const BundleExportQuery& query) const;
    [[nodiscard]] EngineRpcResult<tape_engine::QueryResponse> importCaseBundle(
        const BundleImportQuery& query) const;
    [[nodiscard]] EngineRpcResult<tape_engine::QueryResponse> listImportedCases(
        const ListQuery& query) const;
    [[nodiscard]] EngineRpcResult<tape_payloads::SessionQualityPayload> readSessionQuality(
        const SessionWindowQuery& query) const;

private:
    [[nodiscard]] tape_payloads::QueryResult<tape_engine::QueryResponse> performRawQuery(
        const tape_engine::QueryRequest& request) const;
    [[nodiscard]] static EngineRpcError classifyFailure(const std::string& errorMessage);

    EngineRpcClientConfig config_;
    tape_engine::Client client_;
};

std::string errorCodeForKind(EngineRpcErrorKind kind);

} // namespace tape_mcp
