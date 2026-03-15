#include "tape_mcp_engine_rpc.h"

#include "tapescope_client_internal.h"

#include <utility>

namespace tape_mcp {

namespace {

using tapescope::client_internal::makeError;
using tapescope::client_internal::makeSuccess;

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
EngineRpcResult<T> convertPackedResult(const tapescope::QueryResult<T>& result) {
    if (result.ok()) {
        return makeRpcSuccess(result.value);
    }
    return makeRpcFailure<T>(makeRpcError(
        result.error.kind == tapescope::QueryErrorKind::Transport
            ? EngineRpcErrorKind::Transport
            : result.error.kind == tapescope::QueryErrorKind::Remote
                ? EngineRpcErrorKind::Remote
                : EngineRpcErrorKind::MalformedResponse,
        result.error.message));
}

EngineRpcResult<tape_engine::QueryResponse> convertRawResult(
    const tapescope::QueryResult<tape_engine::QueryResponse>& result) {
    if (result.ok()) {
        return makeRpcSuccess(result.value);
    }
    return makeRpcFailure<tape_engine::QueryResponse>(makeRpcError(
        result.error.kind == tapescope::QueryErrorKind::Transport
            ? EngineRpcErrorKind::Transport
            : result.error.kind == tapescope::QueryErrorKind::Remote
                ? EngineRpcErrorKind::Remote
                : EngineRpcErrorKind::MalformedResponse,
        result.error.message));
}

} // namespace

EngineRpcClient::EngineRpcClient(EngineRpcClientConfig config)
    : config_(std::move(config)),
      client_([&]() {
          if (config_.socketPath.empty()) {
              config_.socketPath = tapescope::defaultSocketPath();
          }
          return config_.socketPath;
      }()) {}

const EngineRpcClientConfig& EngineRpcClient::config() const {
    return config_;
}

tapescope::QueryResult<tape_engine::QueryResponse> EngineRpcClient::performRawQuery(
    const tape_engine::QueryRequest& request) const {
    if (config_.socketPath.empty()) {
        return makeError<tape_engine::QueryResponse>(tapescope::QueryErrorKind::Transport,
                                                     "engine socket path is empty");
    }

    tape_engine::QueryResponse response;
    std::string errorMessage;
    if (!client_.query(request, &response, &errorMessage)) {
        const EngineRpcError error = classifyFailure(errorMessage);
        const auto kind = error.kind == EngineRpcErrorKind::Transport
            ? tapescope::QueryErrorKind::Transport
            : error.kind == EngineRpcErrorKind::MalformedResponse
                ? tapescope::QueryErrorKind::MalformedResponse
                : tapescope::QueryErrorKind::Remote;
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

EngineRpcResult<tapescope::StatusSnapshot> EngineRpcClient::status() const {
    const auto response = performRawQuery(makeRequest(tape_engine::QueryOperation::Status, "tape-mcp-status"));
    if (!response.ok()) {
        return convertPackedResult<tapescope::StatusSnapshot>(
            makeError<tapescope::StatusSnapshot>(response.error.kind, response.error.message));
    }

    const auto& summary = response.value.summary;
    if (!summary.is_object()) {
        return makeRpcFailure<tapescope::StatusSnapshot>(
            makeRpcError(EngineRpcErrorKind::MalformedResponse, "status summary must be an object"));
    }

    tapescope::StatusSnapshot snapshot;
    snapshot.socketPath = summary.value("socket_path", config_.socketPath);
    snapshot.dataDir = summary.value("data_dir", std::string());
    snapshot.instrumentId = summary.value("instrument_id", std::string());
    snapshot.latestSessionSeq = summary.value("latest_session_seq", 0ULL);
    snapshot.liveEventCount = summary.value("live_event_count", 0ULL);
    snapshot.segmentCount = summary.value("segment_count", 0ULL);
    snapshot.manifestHash = summary.value("last_manifest_hash", std::string());
    return makeRpcSuccess(std::move(snapshot));
}

EngineRpcResult<tapescope::EventListPayload> EngineRpcClient::readLiveTail(std::size_t limit) const {
    auto request = makeRequest(tape_engine::QueryOperation::ReadLiveTail, "tape-mcp-read-live-tail");
    request.limit = limit;
    return convertPackedResult(tapescope::client_internal::packEventListPayload(performRawQuery(request)));
}

EngineRpcResult<tapescope::EventListPayload> EngineRpcClient::readRange(
    const SessionWindowQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ReadRange, "tape-mcp-read-range");
    applySessionWindow(&request, query);
    return convertPackedResult(tapescope::client_internal::packEventListPayload(performRawQuery(request)));
}

EngineRpcResult<tapescope::EventListPayload> EngineRpcClient::findOrderAnchor(
    const OrderCaseQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::FindOrderAnchor, "tape-mcp-find-order-anchor");
    applyOrderAnchor(&request, query);
    return convertPackedResult(tapescope::client_internal::packEventListPayload(performRawQuery(request)));
}

EngineRpcResult<tapescope::IncidentListPayload> EngineRpcClient::listIncidents(
    const ListQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ListIncidents, "tape-mcp-list-incidents");
    applyListQuery(&request, query);
    return convertPackedResult(tapescope::client_internal::packIncidentListPayload(performRawQuery(request)));
}

EngineRpcResult<tape_engine::QueryResponse> EngineRpcClient::listOrderAnchors(
    const ListQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ListOrderAnchors, "tape-mcp-list-order-anchors");
    applyListQuery(&request, query);
    const auto response = performRawQuery(request);
    if (!response.ok()) {
        return makeRpcFailure<tape_engine::QueryResponse>(makeRpcError(
            response.error.kind == tapescope::QueryErrorKind::Transport
                ? EngineRpcErrorKind::Transport
                : response.error.kind == tapescope::QueryErrorKind::Remote
                    ? EngineRpcErrorKind::Remote
                    : EngineRpcErrorKind::MalformedResponse,
            response.error.message));
    }
    return makeRpcSuccess(response.value);
}

EngineRpcResult<tape_engine::QueryResponse> EngineRpcClient::listProtectedWindows(
    const ListQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ListProtectedWindows, "tape-mcp-list-protected-windows");
    applyListQuery(&request, query);
    const auto response = performRawQuery(request);
    if (!response.ok()) {
        return makeRpcFailure<tape_engine::QueryResponse>(makeRpcError(
            response.error.kind == tapescope::QueryErrorKind::Transport
                ? EngineRpcErrorKind::Transport
                : response.error.kind == tapescope::QueryErrorKind::Remote
                    ? EngineRpcErrorKind::Remote
                    : EngineRpcErrorKind::MalformedResponse,
            response.error.message));
    }
    return makeRpcSuccess(response.value);
}

EngineRpcResult<tape_engine::QueryResponse> EngineRpcClient::listFindings(
    const ListQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ListFindings, "tape-mcp-list-findings");
    applyListQuery(&request, query);
    const auto response = performRawQuery(request);
    if (!response.ok()) {
        return makeRpcFailure<tape_engine::QueryResponse>(makeRpcError(
            response.error.kind == tapescope::QueryErrorKind::Transport
                ? EngineRpcErrorKind::Transport
                : response.error.kind == tapescope::QueryErrorKind::Remote
                    ? EngineRpcErrorKind::Remote
                    : EngineRpcErrorKind::MalformedResponse,
            response.error.message));
    }
    return makeRpcSuccess(response.value);
}

EngineRpcResult<tapescope::InvestigationPayload> EngineRpcClient::readSessionOverview(
    const SessionWindowQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ReadSessionOverview, "tape-mcp-read-session-overview");
    applySessionWindow(&request, query);
    return convertPackedResult(tapescope::client_internal::packInvestigationPayload(performRawQuery(request)));
}

EngineRpcResult<tapescope::InvestigationPayload> EngineRpcClient::scanSessionReport(
    const SessionWindowQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ScanSessionReport, "tape-mcp-scan-session-report");
    applySessionWindow(&request, query);
    request.includeLiveTail = false;
    return convertPackedResult(tapescope::client_internal::packInvestigationPayload(performRawQuery(request)));
}

EngineRpcResult<tapescope::InvestigationPayload> EngineRpcClient::readSessionReport(
    const NumericIdQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ReadSessionReport, "tape-mcp-read-session-report");
    applyNumericIdQuery(&request, query);
    request.reportId = query.id;
    request.includeLiveTail = false;
    return convertPackedResult(tapescope::client_internal::packInvestigationPayload(performRawQuery(request)));
}

EngineRpcResult<tapescope::ReportInventoryPayload> EngineRpcClient::listSessionReports(
    const ListQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ListSessionReports, "tape-mcp-list-session-reports");
    applyListQuery(&request, query);
    return convertPackedResult(tapescope::client_internal::packReportInventoryPayload(performRawQuery(request), true));
}

EngineRpcResult<tapescope::InvestigationPayload> EngineRpcClient::scanIncidentReport(
    const IncidentQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ScanIncidentReport, "tape-mcp-scan-incident-report");
    request.logicalIncidentId = query.logicalIncidentId;
    request.revisionId = query.revisionId;
    request.limit = query.limit;
    request.includeLiveTail = false;
    return convertPackedResult(tapescope::client_internal::packInvestigationPayload(performRawQuery(request)));
}

EngineRpcResult<tapescope::InvestigationPayload> EngineRpcClient::scanOrderCaseReport(
    const OrderCaseQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ScanOrderCaseReport, "tape-mcp-scan-order-case-report");
    applyOrderAnchor(&request, query);
    request.includeLiveTail = false;
    return convertPackedResult(tapescope::client_internal::packInvestigationPayload(performRawQuery(request)));
}

EngineRpcResult<tapescope::InvestigationPayload> EngineRpcClient::readCaseReport(
    const NumericIdQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ReadCaseReport, "tape-mcp-read-case-report");
    applyNumericIdQuery(&request, query);
    request.reportId = query.id;
    request.includeLiveTail = false;
    return convertPackedResult(tapescope::client_internal::packInvestigationPayload(performRawQuery(request)));
}

EngineRpcResult<tapescope::ReportInventoryPayload> EngineRpcClient::listCaseReports(
    const ListQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ListCaseReports, "tape-mcp-list-case-reports");
    applyListQuery(&request, query);
    return convertPackedResult(tapescope::client_internal::packReportInventoryPayload(performRawQuery(request), false));
}

EngineRpcResult<tapescope::SeekOrderPayload> EngineRpcClient::seekOrderAnchor(
    const OrderCaseQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::SeekOrderAnchor, "tape-mcp-seek-order-anchor");
    applyOrderAnchor(&request, query);
    return convertPackedResult(tapescope::client_internal::packSeekOrderPayload(performRawQuery(request)));
}

EngineRpcResult<tapescope::InvestigationPayload> EngineRpcClient::readFinding(
    const NumericIdQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ReadFinding, "tape-mcp-read-finding");
    applyNumericIdQuery(&request, query);
    request.findingId = query.id;
    return convertPackedResult(tapescope::client_internal::packInvestigationPayload(performRawQuery(request)));
}

EngineRpcResult<tapescope::InvestigationPayload> EngineRpcClient::readOrderCase(
    const OrderCaseQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ReadOrderCase, "tape-mcp-read-order-case");
    applyOrderAnchor(&request, query);
    return convertPackedResult(tapescope::client_internal::packInvestigationPayload(performRawQuery(request)));
}

EngineRpcResult<tapescope::InvestigationPayload> EngineRpcClient::readIncident(
    const IncidentQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ReadIncident, "tape-mcp-read-incident");
    request.logicalIncidentId = query.logicalIncidentId;
    request.revisionId = query.revisionId;
    request.limit = query.limit;
    request.includeLiveTail = query.includeLiveTail;
    return convertPackedResult(tapescope::client_internal::packInvestigationPayload(performRawQuery(request)));
}

EngineRpcResult<tapescope::InvestigationPayload> EngineRpcClient::readOrderAnchor(
    const NumericIdQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ReadOrderAnchor, "tape-mcp-read-order-anchor");
    applyNumericIdQuery(&request, query);
    request.anchorId = query.id;
    return convertPackedResult(tapescope::client_internal::packInvestigationPayload(performRawQuery(request)));
}

EngineRpcResult<tape_engine::QueryResponse> EngineRpcClient::readProtectedWindow(
    const NumericIdQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ReadProtectedWindow, "tape-mcp-read-protected-window");
    applyNumericIdQuery(&request, query);
    request.windowId = query.id;
    return convertRawResult(performRawQuery(request));
}

EngineRpcResult<tape_engine::QueryResponse> EngineRpcClient::replaySnapshot(
    const ReplaySnapshotQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ReplaySnapshot, "tape-mcp-replay-snapshot");
    applyReplaySnapshotQuery(&request, query);
    return convertRawResult(performRawQuery(request));
}

EngineRpcResult<tapescope::InvestigationPayload> EngineRpcClient::readArtifact(
    const ArtifactQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ReadArtifact, "tape-mcp-read-artifact");
    applyArtifactQuery(&request, query);
    return convertPackedResult(tapescope::client_internal::packInvestigationPayload(performRawQuery(request)));
}

EngineRpcResult<tapescope::ArtifactExportPayload> EngineRpcClient::exportArtifact(
    const ArtifactExportQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ExportArtifact, "tape-mcp-export-artifact");
    applyExportQuery(&request, query);
    return convertPackedResult(tapescope::client_internal::packArtifactExportPayload(performRawQuery(request)));
}

EngineRpcResult<tape_engine::QueryResponse> EngineRpcClient::exportSessionBundle(
    const BundleExportQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ExportSessionBundle, "tape-mcp-export-session-bundle");
    request.reportId = query.reportId;
    return convertRawResult(performRawQuery(request));
}

EngineRpcResult<tape_engine::QueryResponse> EngineRpcClient::exportCaseBundle(
    const BundleExportQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ExportCaseBundle, "tape-mcp-export-case-bundle");
    request.reportId = query.reportId;
    return convertRawResult(performRawQuery(request));
}

EngineRpcResult<tape_engine::QueryResponse> EngineRpcClient::importCaseBundle(
    const BundleImportQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ImportCaseBundle, "tape-mcp-import-case-bundle");
    request.bundlePath = query.bundlePath;
    return convertRawResult(performRawQuery(request));
}

EngineRpcResult<tape_engine::QueryResponse> EngineRpcClient::listImportedCases(
    const ListQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ListImportedCases, "tape-mcp-list-imported-cases");
    applyListQuery(&request, query);
    return convertRawResult(performRawQuery(request));
}

EngineRpcResult<tapescope::SessionQualityPayload> EngineRpcClient::readSessionQuality(
    const SessionWindowQuery& query) const {
    auto request = makeRequest(tape_engine::QueryOperation::ReadSessionQuality, "tape-mcp-read-session-quality");
    applySessionWindow(&request, query);
    return convertPackedResult(tapescope::client_internal::packSessionQualityPayload(performRawQuery(request)));
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
