#pragma once

#include "tape_engine_client.h"
#include "tapescope_client.h"

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

    [[nodiscard]] EngineRpcResult<tapescope::StatusSnapshot> status() const;
    [[nodiscard]] EngineRpcResult<tapescope::EventListPayload> readLiveTail(std::size_t limit) const;
    [[nodiscard]] EngineRpcResult<tapescope::EventListPayload> readRange(
        const SessionWindowQuery& query) const;
    [[nodiscard]] EngineRpcResult<tapescope::EventListPayload> findOrderAnchor(
        const OrderCaseQuery& query) const;
    [[nodiscard]] EngineRpcResult<tapescope::IncidentListPayload> listIncidents(
        const ListQuery& query) const;
    [[nodiscard]] EngineRpcResult<tape_engine::QueryResponse> listOrderAnchors(
        const ListQuery& query) const;
    [[nodiscard]] EngineRpcResult<tape_engine::QueryResponse> listProtectedWindows(
        const ListQuery& query) const;
    [[nodiscard]] EngineRpcResult<tape_engine::QueryResponse> listFindings(
        const ListQuery& query) const;
    [[nodiscard]] EngineRpcResult<tapescope::InvestigationPayload> readSessionOverview(
        const SessionWindowQuery& query) const;
    [[nodiscard]] EngineRpcResult<tapescope::InvestigationPayload> scanSessionReport(
        const SessionWindowQuery& query) const;
    [[nodiscard]] EngineRpcResult<tapescope::InvestigationPayload> readSessionReport(
        const NumericIdQuery& query) const;
    [[nodiscard]] EngineRpcResult<tapescope::ReportInventoryPayload> listSessionReports(
        const ListQuery& query) const;
    [[nodiscard]] EngineRpcResult<tapescope::InvestigationPayload> scanIncidentReport(
        const IncidentQuery& query) const;
    [[nodiscard]] EngineRpcResult<tapescope::InvestigationPayload> scanOrderCaseReport(
        const OrderCaseQuery& query) const;
    [[nodiscard]] EngineRpcResult<tapescope::InvestigationPayload> readCaseReport(
        const NumericIdQuery& query) const;
    [[nodiscard]] EngineRpcResult<tapescope::ReportInventoryPayload> listCaseReports(
        const ListQuery& query) const;
    [[nodiscard]] EngineRpcResult<tapescope::SeekOrderPayload> seekOrderAnchor(
        const OrderCaseQuery& query) const;
    [[nodiscard]] EngineRpcResult<tapescope::InvestigationPayload> readFinding(
        const NumericIdQuery& query) const;
    [[nodiscard]] EngineRpcResult<tapescope::InvestigationPayload> readOrderCase(
        const OrderCaseQuery& query) const;
    [[nodiscard]] EngineRpcResult<tapescope::InvestigationPayload> readIncident(
        const IncidentQuery& query) const;
    [[nodiscard]] EngineRpcResult<tapescope::InvestigationPayload> readOrderAnchor(
        const NumericIdQuery& query) const;
    [[nodiscard]] EngineRpcResult<tape_engine::QueryResponse> readProtectedWindow(
        const NumericIdQuery& query) const;
    [[nodiscard]] EngineRpcResult<tape_engine::QueryResponse> replaySnapshot(
        const ReplaySnapshotQuery& query) const;
    [[nodiscard]] EngineRpcResult<tapescope::InvestigationPayload> readArtifact(
        const ArtifactQuery& query) const;
    [[nodiscard]] EngineRpcResult<tapescope::ArtifactExportPayload> exportArtifact(
        const ArtifactExportQuery& query) const;
    [[nodiscard]] EngineRpcResult<tape_engine::QueryResponse> exportSessionBundle(
        const BundleExportQuery& query) const;
    [[nodiscard]] EngineRpcResult<tape_engine::QueryResponse> exportCaseBundle(
        const BundleExportQuery& query) const;
    [[nodiscard]] EngineRpcResult<tape_engine::QueryResponse> importCaseBundle(
        const BundleImportQuery& query) const;
    [[nodiscard]] EngineRpcResult<tape_engine::QueryResponse> listImportedCases(
        const ListQuery& query) const;
    [[nodiscard]] EngineRpcResult<tapescope::SessionQualityPayload> readSessionQuality(
        const SessionWindowQuery& query) const;

private:
    [[nodiscard]] tapescope::QueryResult<tape_engine::QueryResponse> performRawQuery(
        const tape_engine::QueryRequest& request) const;
    [[nodiscard]] static EngineRpcError classifyFailure(const std::string& errorMessage);

    EngineRpcClientConfig config_;
    tape_engine::Client client_;
};

std::string errorCodeForKind(EngineRpcErrorKind kind);

} // namespace tape_mcp
