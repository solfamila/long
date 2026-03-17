#include "tape_mcp_engine_rpc.h"

#include <cstdlib>
#include <utility>

namespace tape_mcp {

namespace {

using tape_payloads::makeError;
using tape_payloads::makeSuccess;

tape_engine::QueryRequest makeRequest(tape_engine::QueryOperation operation, const char* requestId) {
    return tape_engine::makeQueryRequest(operation, requestId == nullptr ? std::string() : std::string(requestId));
}

void applySessionWindow(tape_engine::QueryRequest* request, const SessionWindowQuery& query) {
    if (request == nullptr) {
        return;
    }
    request->fromSessionSeq = query.firstSessionSeq;
    request->toSessionSeq = query.lastSessionSeq;
    request->revisionId = query.revisionId;
    request->limit = query.limit;
    request->includeLiveTail = query.includeLiveTail;
}

void applyListQuery(tape_engine::QueryRequest* request, const ListQuery& query) {
    if (request == nullptr) {
        return;
    }
    request->revisionId = query.revisionId;
    request->limit = query.limit;
}

void applyReplaySnapshotQuery(tape_engine::QueryRequest* request, const ReplaySnapshotQuery& query) {
    if (request == nullptr) {
        return;
    }
    request->targetSessionSeq = query.targetSessionSeq;
    request->revisionId = query.revisionId;
    request->limit = query.depthLimit;
    request->includeLiveTail = query.includeLiveTail;
}

void applyOrderAnchor(tape_engine::QueryRequest* request, const OrderCaseQuery& query) {
    if (request == nullptr) {
        return;
    }
    request->revisionId = query.revisionId;
    request->limit = query.limit;
    request->includeLiveTail = query.includeLiveTail;
    if (query.traceId.has_value()) {
        request->traceId = *query.traceId;
    }
    if (query.orderId.has_value()) {
        request->orderId = *query.orderId;
    }
    if (query.permId.has_value()) {
        request->permId = *query.permId;
    }
    if (query.execId.has_value()) {
        request->execId = *query.execId;
    }
}

void applyNumericIdQuery(tape_engine::QueryRequest* request, const NumericIdQuery& query) {
    if (request == nullptr) {
        return;
    }
    request->revisionId = query.revisionId;
    request->limit = query.limit;
    request->includeLiveTail = query.includeLiveTail;
}

void applyArtifactQuery(tape_engine::QueryRequest* request, const ArtifactQuery& query) {
    if (request == nullptr) {
        return;
    }
    request->artifactId = query.artifactId;
    request->revisionId = query.revisionId;
    request->limit = query.limit;
    request->includeLiveTail = query.includeLiveTail;
}

void applyExportQuery(tape_engine::QueryRequest* request, const ArtifactExportQuery& query) {
    if (request == nullptr) {
        return;
    }
    request->artifactId = query.artifactId;
    request->exportFormat = query.exportFormat;
    request->revisionId = query.revisionId;
    request->limit = query.limit;
    request->includeLiveTail = query.includeLiveTail;
}

EngineRpcError makeRpcError(const EngineRpcErrorKind kind, std::string message) {
    EngineRpcError error;
    error.kind = kind;
    error.code = errorCodeForKind(kind);
    error.message = std::move(message);
    error.retryable = (kind == EngineRpcErrorKind::Transport);
    return error;
}

template <typename T>
EngineRpcResult<T> makeRpcSuccess(T value) {
    EngineRpcResult<T> result;
    result.value = std::move(value);
    return result;
}

template <typename T>
EngineRpcResult<T> makeRpcFailure(const EngineRpcError& error) {
    EngineRpcResult<T> result;
    result.error = error;
    return result;
}

template <typename T>
EngineRpcResult<T> convertPackedResult(const tape_payloads::QueryResult<T>& result) {
    if (result.ok()) {
        return makeRpcSuccess(result.value);
    }
    return makeRpcFailure<T>(makeRpcError(
        result.error.kind == tape_payloads::QueryErrorKind::Transport
            ? EngineRpcErrorKind::Transport
            : result.error.kind == tape_payloads::QueryErrorKind::Remote
                ? EngineRpcErrorKind::Remote
                : EngineRpcErrorKind::MalformedResponse,
        result.error.message));
}

} // namespace

EngineRpcClient::EngineRpcClient(EngineRpcClientConfig config)
    : config_(std::move(config)),
      client_([&]() {
          if (config_.socketPath.empty()) {
              const char* value = std::getenv("LONG_TAPE_ENGINE_SOCKET");
              config_.socketPath = (value != nullptr && value[0] != '\0')
                  ? std::string(value)
                  : std::string("/tmp/tape-engine.sock");
          }
          return config_.socketPath;
      }()) {}

const EngineRpcClientConfig& EngineRpcClient::config() const {
    return config_;
}

tape_payloads::QueryResult<tape_engine::QueryResponse> EngineRpcClient::performRawQuery(
    const tape_engine::QueryRequest& request) const {
    if (config_.socketPath.empty()) {
        return makeError<tape_engine::QueryResponse>(tape_payloads::QueryErrorKind::Transport,
                                                     "engine socket path is empty");
    }

    tape_engine::QueryResponse response;
    std::string errorMessage;
    if (!client_.query(request, &response, &errorMessage)) {
        const EngineRpcError error = classifyFailure(errorMessage);
        const auto kind = error.kind == EngineRpcErrorKind::Transport
            ? tape_payloads::QueryErrorKind::Transport
            : error.kind == EngineRpcErrorKind::MalformedResponse
                ? tape_payloads::QueryErrorKind::MalformedResponse
                : tape_payloads::QueryErrorKind::Remote;
        return makeError<tape_engine::QueryResponse>(kind, error.message);
    }
    return makeSuccess(std::move(response));
}

EngineRpcError EngineRpcClient::classifyFailure(const std::string& errorMessage) {
    if (errorMessage.rfind("connect:", 0) == 0 ||
        errorMessage.rfind("socket:", 0) == 0 ||
        errorMessage.rfind("write:", 0) == 0 ||
        errorMessage.rfind("read:", 0) == 0 ||
        errorMessage.find("socket path") != std::string::npos) {
        return makeRpcError(EngineRpcErrorKind::Transport, errorMessage);
    }
    return makeRpcError(EngineRpcErrorKind::Remote, errorMessage);
}

EngineRpcResult<tape_payloads::StatusSnapshot> EngineRpcClient::status() const {
    const auto response = performRawQuery(makeRequest(tape_engine::QueryOperation::Status, "tape-mcp-status"));
    auto result = convertPackedResult(tape_payloads::packStatusPayload(response));
    if (result.ok() && result.value.socketPath.empty()) {
        result.value.socketPath = config_.socketPath;
    }
    return result;
}

EngineRpcResult<tape_payloads::EventListPayload> EngineRpcClient::readLiveTail(std::size_t limit) const {
    auto request = makeRequest(tape_engine::QueryOperation::ReadLiveTail, "tape-mcp-read-live-tail");
    request.limit = limit;
    return convertPackedResult(tape_payloads::packEventListPayload(performRawQuery(request)));
}

EngineRpcResult<tape_payloads::EventListPayload> EngineRpcClient::readRange(
    const SessionWindowQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ReadRange, "tape-mcp-read-range");
    applySessionWindow(&request, query);
    return convertPackedResult(tape_payloads::packEventListPayload(performRawQuery(request)));
}

EngineRpcResult<tape_payloads::EventListPayload> EngineRpcClient::findOrderAnchor(
    const OrderCaseQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::FindOrderAnchor, "tape-mcp-find-order-anchor");
    applyOrderAnchor(&request, query);
    return convertPackedResult(tape_payloads::packEventListPayload(performRawQuery(request)));
}

EngineRpcResult<tape_payloads::IncidentListPayload> EngineRpcClient::listIncidents(
    const ListQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ListIncidents, "tape-mcp-list-incidents");
    applyListQuery(&request, query);
    return convertPackedResult(tape_payloads::packIncidentListPayload(performRawQuery(request)));
}

EngineRpcResult<tape_payloads::CollectionRowsPayload> EngineRpcClient::listOrderAnchors(
    const ListQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ListOrderAnchors, "tape-mcp-list-order-anchors");
    applyListQuery(&request, query);
    return convertPackedResult(tape_payloads::packCollectionRowsPayload(performRawQuery(request), "order_anchors"));
}

EngineRpcResult<tape_payloads::CollectionRowsPayload> EngineRpcClient::listProtectedWindows(
    const ListQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ListProtectedWindows, "tape-mcp-list-protected-windows");
    applyListQuery(&request, query);
    return convertPackedResult(tape_payloads::packCollectionRowsPayload(performRawQuery(request), "protected_windows"));
}

EngineRpcResult<tape_payloads::CollectionRowsPayload> EngineRpcClient::listFindings(
    const ListQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ListFindings, "tape-mcp-list-findings");
    applyListQuery(&request, query);
    return convertPackedResult(tape_payloads::packCollectionRowsPayload(performRawQuery(request), "findings"));
}

EngineRpcResult<tape_payloads::InvestigationPayload> EngineRpcClient::readSessionOverview(
    const SessionWindowQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ReadSessionOverview, "tape-mcp-read-session-overview");
    applySessionWindow(&request, query);
    return convertPackedResult(tape_payloads::packInvestigationPayload(performRawQuery(request)));
}

EngineRpcResult<tape_payloads::InvestigationPayload> EngineRpcClient::scanSessionReport(
    const SessionWindowQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ScanSessionReport, "tape-mcp-scan-session-report");
    applySessionWindow(&request, query);
    request.includeLiveTail = false;
    return convertPackedResult(tape_payloads::packInvestigationPayload(performRawQuery(request)));
}

EngineRpcResult<tape_payloads::InvestigationPayload> EngineRpcClient::readSessionReport(
    const NumericIdQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ReadSessionReport, "tape-mcp-read-session-report");
    applyNumericIdQuery(&request, query);
    request.reportId = query.id;
    request.includeLiveTail = false;
    return convertPackedResult(tape_payloads::packInvestigationPayload(performRawQuery(request)));
}

EngineRpcResult<tape_payloads::ReportInventoryPayload> EngineRpcClient::listSessionReports(
    const ListQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ListSessionReports, "tape-mcp-list-session-reports");
    applyListQuery(&request, query);
    return convertPackedResult(tape_payloads::packReportInventoryPayload(performRawQuery(request), true));
}

EngineRpcResult<tape_payloads::InvestigationPayload> EngineRpcClient::scanIncidentReport(
    const IncidentQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ScanIncidentReport, "tape-mcp-scan-incident-report");
    request.logicalIncidentId = query.logicalIncidentId;
    request.revisionId = query.revisionId;
    request.limit = query.limit;
    request.includeLiveTail = false;
    return convertPackedResult(tape_payloads::packInvestigationPayload(performRawQuery(request)));
}

EngineRpcResult<tape_payloads::InvestigationPayload> EngineRpcClient::scanOrderCaseReport(
    const OrderCaseQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ScanOrderCaseReport, "tape-mcp-scan-order-case-report");
    applyOrderAnchor(&request, query);
    request.includeLiveTail = false;
    return convertPackedResult(tape_payloads::packInvestigationPayload(performRawQuery(request)));
}

EngineRpcResult<tape_payloads::InvestigationPayload> EngineRpcClient::readCaseReport(
    const NumericIdQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ReadCaseReport, "tape-mcp-read-case-report");
    applyNumericIdQuery(&request, query);
    request.reportId = query.id;
    request.includeLiveTail = false;
    return convertPackedResult(tape_payloads::packInvestigationPayload(performRawQuery(request)));
}

EngineRpcResult<tape_payloads::ReportInventoryPayload> EngineRpcClient::listCaseReports(
    const ListQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ListCaseReports, "tape-mcp-list-case-reports");
    applyListQuery(&request, query);
    return convertPackedResult(tape_payloads::packReportInventoryPayload(performRawQuery(request), false));
}

EngineRpcResult<tape_payloads::SeekOrderPayload> EngineRpcClient::seekOrderAnchor(
    const OrderCaseQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::SeekOrderAnchor, "tape-mcp-seek-order-anchor");
    applyOrderAnchor(&request, query);
    return convertPackedResult(tape_payloads::packSeekOrderPayload(performRawQuery(request)));
}

EngineRpcResult<tape_payloads::InvestigationPayload> EngineRpcClient::readFinding(
    const NumericIdQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ReadFinding, "tape-mcp-read-finding");
    applyNumericIdQuery(&request, query);
    request.findingId = query.id;
    return convertPackedResult(tape_payloads::packInvestigationPayload(performRawQuery(request)));
}

EngineRpcResult<tape_payloads::InvestigationPayload> EngineRpcClient::readOrderCase(
    const OrderCaseQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ReadOrderCase, "tape-mcp-read-order-case");
    applyOrderAnchor(&request, query);
    return convertPackedResult(tape_payloads::packInvestigationPayload(performRawQuery(request)));
}

EngineRpcResult<tape_payloads::InvestigationPayload> EngineRpcClient::readIncident(
    const IncidentQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ReadIncident, "tape-mcp-read-incident");
    request.logicalIncidentId = query.logicalIncidentId;
    request.revisionId = query.revisionId;
    request.limit = query.limit;
    request.includeLiveTail = query.includeLiveTail;
    return convertPackedResult(tape_payloads::packInvestigationPayload(performRawQuery(request)));
}

EngineRpcResult<tape_payloads::EnrichmentPayload> EngineRpcClient::enrichIncident(
    const IncidentQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::EnrichIncident, "tape-mcp-enrich-incident");
    request.logicalIncidentId = query.logicalIncidentId;
    request.revisionId = query.revisionId;
    request.limit = query.limit;
    request.includeLiveTail = query.includeLiveTail;
    return convertPackedResult(tape_payloads::packEnrichmentPayload(performRawQuery(request)));
}

EngineRpcResult<tape_payloads::EnrichmentPayload> EngineRpcClient::explainIncident(
    const IncidentQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ExplainIncident, "tape-mcp-explain-incident");
    request.logicalIncidentId = query.logicalIncidentId;
    request.revisionId = query.revisionId;
    request.limit = query.limit;
    request.includeLiveTail = query.includeLiveTail;
    return convertPackedResult(tape_payloads::packEnrichmentPayload(performRawQuery(request)));
}

EngineRpcResult<tape_payloads::EnrichmentPayload> EngineRpcClient::enrichOrderCase(
    const OrderCaseQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::EnrichOrderCase, "tape-mcp-enrich-order-case");
    applyOrderAnchor(&request, query);
    return convertPackedResult(tape_payloads::packEnrichmentPayload(performRawQuery(request)));
}

EngineRpcResult<tape_payloads::EnrichmentPayload> EngineRpcClient::refreshExternalContextIncident(
    const IncidentQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::RefreshExternalContext,
                               "tape-mcp-refresh-external-context");
    request.logicalIncidentId = query.logicalIncidentId;
    request.revisionId = query.revisionId;
    request.limit = query.limit;
    request.includeLiveTail = query.includeLiveTail;
    return convertPackedResult(tape_payloads::packEnrichmentPayload(performRawQuery(request)));
}

EngineRpcResult<tape_payloads::EnrichmentPayload> EngineRpcClient::refreshExternalContextOrderCase(
    const OrderCaseQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::RefreshExternalContext,
                               "tape-mcp-refresh-external-context");
    applyOrderAnchor(&request, query);
    return convertPackedResult(tape_payloads::packEnrichmentPayload(performRawQuery(request)));
}

EngineRpcResult<tape_payloads::InvestigationPayload> EngineRpcClient::readOrderAnchor(
    const NumericIdQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ReadOrderAnchor, "tape-mcp-read-order-anchor");
    applyNumericIdQuery(&request, query);
    request.anchorId = query.id;
    return convertPackedResult(tape_payloads::packInvestigationPayload(performRawQuery(request)));
}

EngineRpcResult<tape_payloads::InvestigationPayload> EngineRpcClient::readProtectedWindow(
    const NumericIdQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ReadProtectedWindow, "tape-mcp-read-protected-window");
    applyNumericIdQuery(&request, query);
    request.windowId = query.id;
    return convertPackedResult(tape_payloads::packInvestigationPayload(performRawQuery(request)));
}

EngineRpcResult<tape_payloads::ReplaySnapshotPayload> EngineRpcClient::replaySnapshot(
    const ReplaySnapshotQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ReplaySnapshot, "tape-mcp-replay-snapshot");
    applyReplaySnapshotQuery(&request, query);
    return convertPackedResult(tape_payloads::packReplaySnapshotPayload(performRawQuery(request)));
}

EngineRpcResult<tape_payloads::InvestigationPayload> EngineRpcClient::readArtifact(
    const ArtifactQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ReadArtifact, "tape-mcp-read-artifact");
    applyArtifactQuery(&request, query);
    return convertPackedResult(tape_payloads::packInvestigationPayload(performRawQuery(request)));
}

EngineRpcResult<tape_payloads::ArtifactExportPayload> EngineRpcClient::exportArtifact(
    const ArtifactExportQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ExportArtifact, "tape-mcp-export-artifact");
    applyExportQuery(&request, query);
    return convertPackedResult(tape_payloads::packArtifactExportPayload(performRawQuery(request)));
}

EngineRpcResult<tape_payloads::BundleExportPayload> EngineRpcClient::exportSessionBundle(
    const BundleExportQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ExportSessionBundle, "tape-mcp-export-session-bundle");
    request.reportId = query.reportId;
    return convertPackedResult(tape_payloads::packBundleExportPayload(performRawQuery(request)));
}

EngineRpcResult<tape_payloads::BundleExportPayload> EngineRpcClient::exportCaseBundle(
    const BundleExportQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ExportCaseBundle, "tape-mcp-export-case-bundle");
    request.reportId = query.reportId;
    return convertPackedResult(tape_payloads::packBundleExportPayload(performRawQuery(request)));
}

EngineRpcResult<tape_payloads::BundleVerifyPayload> EngineRpcClient::verifyBundle(
    const BundleImportQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::VerifyBundle, "tape-mcp-verify-bundle");
    request.bundlePath = query.bundlePath;
    return convertPackedResult(tape_payloads::packBundleVerifyPayload(performRawQuery(request)));
}

EngineRpcResult<tape_payloads::CaseBundleImportPayload> EngineRpcClient::importCaseBundle(
    const BundleImportQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ImportCaseBundle, "tape-mcp-import-case-bundle");
    request.bundlePath = query.bundlePath;
    return convertPackedResult(tape_payloads::packCaseBundleImportPayload(performRawQuery(request)));
}

EngineRpcResult<tape_payloads::ImportedCaseListPayload> EngineRpcClient::listImportedCases(
    const ListQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ListImportedCases, "tape-mcp-list-imported-cases");
    applyListQuery(&request, query);
    return convertPackedResult(tape_payloads::packImportedCaseListPayload(performRawQuery(request)));
}

EngineRpcResult<tape_payloads::SessionQualityPayload> EngineRpcClient::readSessionQuality(
    const SessionWindowQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ReadSessionQuality, "tape-mcp-read-session-quality");
    applySessionWindow(&request, query);
    return convertPackedResult(tape_payloads::packSessionQualityPayload(performRawQuery(request)));
}

std::string errorCodeForKind(const EngineRpcErrorKind kind) {
    switch (kind) {
        case EngineRpcErrorKind::None:
            return "ok";
        case EngineRpcErrorKind::Configuration:
            return "adapter_config_error";
        case EngineRpcErrorKind::Transport:
            return "engine_unavailable";
        case EngineRpcErrorKind::Remote:
            return "engine_query_failed";
        case EngineRpcErrorKind::MalformedResponse:
            return "malformed_response";
    }
    return "engine_query_failed";
}

} // namespace tape_mcp
