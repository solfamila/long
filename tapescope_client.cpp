#include "tapescope_client.h"

#include <cstdlib>
#include <utility>

namespace tapescope {

namespace {

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

} // namespace

std::string defaultSocketPath() {
    const char* value = std::getenv("LONG_TAPE_ENGINE_SOCKET");
    if (value != nullptr && value[0] != '\0') {
        return std::string(value);
    }
    return std::string(kDefaultSocketPath);
}

QueryClient::QueryClient(ClientConfig config)
    : config_(std::move(config)),
      client_([&]() {
          if (config_.socketPath.empty()) {
              config_.socketPath = defaultSocketPath();
          }
          return config_.socketPath;
      }()) {}

const ClientConfig& QueryClient::config() const {
    return config_;
}

QueryError QueryClient::classifyFailure(const std::string& errorMessage) {
    QueryError error;
    error.message = errorMessage;
    if (errorMessage.rfind("connect:", 0) == 0 ||
        errorMessage.rfind("socket:", 0) == 0 ||
        errorMessage.rfind("write:", 0) == 0 ||
        errorMessage.rfind("read:", 0) == 0 ||
        errorMessage.find("socket path") != std::string::npos) {
        error.kind = QueryErrorKind::Transport;
        return error;
    }
    error.kind = QueryErrorKind::Remote;
    return error;
}

QueryResult<tape_engine::QueryResponse> QueryClient::performQuery(const tape_engine::QueryRequest& request) const {
    tape_engine::QueryResponse response;
    std::string errorMessage;
    if (!client_.query(request, &response, &errorMessage)) {
        return makeError<tape_engine::QueryResponse>(classifyFailure(errorMessage).kind, errorMessage);
    }
    return makeSuccess(std::move(response));
}

QueryResult<json> QueryClient::packSummaryAndEvents(const QueryResult<tape_engine::QueryResponse>& response) {
    if (!response.ok()) {
        return propagateError<json>(response.error);
    }

    json payload = json::object();
    payload["summary"] = response.value.summary;
    payload["events"] = response.value.events;
    return makeSuccess(std::move(payload));
}

QueryResult<StatusSnapshot> QueryClient::status() const {
    tape_engine::QueryRequest request;
    request.requestId = "tapescope-status";
    request.operation = "status";

    const QueryResult<tape_engine::QueryResponse> response = performQuery(request);
    if (!response.ok()) {
        return propagateError<StatusSnapshot>(response.error);
    }

    const json& summary = response.value.summary;
    if (!summary.is_object()) {
        return makeError<StatusSnapshot>(QueryErrorKind::MalformedResponse,
                                         "status summary must be an object");
    }

    StatusSnapshot snapshot;
    snapshot.socketPath = summary.value("socket_path", config_.socketPath);
    snapshot.dataDir = summary.value("data_dir", std::string());
    snapshot.instrumentId = summary.value("instrument_id", std::string());
    snapshot.latestSessionSeq = summary.value("latest_session_seq", 0ULL);
    snapshot.liveEventCount = summary.value("live_event_count", 0ULL);
    snapshot.segmentCount = summary.value("segment_count", 0ULL);
    snapshot.manifestHash = summary.value("last_manifest_hash", std::string());
    return makeSuccess(std::move(snapshot));
}

QueryResult<std::vector<json>> QueryClient::readLiveTail(std::size_t limit) const {
    tape_engine::QueryRequest request;
    request.requestId = "tapescope-live-tail";
    request.operation = "read_live_tail";
    request.limit = limit;

    const QueryResult<tape_engine::QueryResponse> response = performQuery(request);
    if (!response.ok()) {
        return propagateError<std::vector<json>>(response.error);
    }
    if (!response.value.events.is_array()) {
        return makeError<std::vector<json>>(QueryErrorKind::MalformedResponse,
                                            "read_live_tail events must be an array");
    }

    std::vector<json> events;
    events.reserve(response.value.events.size());
    for (const auto& event : response.value.events) {
        events.push_back(event);
    }
    return makeSuccess(std::move(events));
}

QueryResult<std::vector<json>> QueryClient::readRange(const RangeQuery& query) const {
    tape_engine::QueryRequest request;
    request.requestId = "tapescope-read-range";
    request.operation = "read_range";
    request.fromSessionSeq = query.firstSessionSeq;
    request.toSessionSeq = query.lastSessionSeq;

    const QueryResult<tape_engine::QueryResponse> response = performQuery(request);
    if (!response.ok()) {
        return propagateError<std::vector<json>>(response.error);
    }
    if (!response.value.events.is_array()) {
        return makeError<std::vector<json>>(QueryErrorKind::MalformedResponse,
                                            "read_range events must be an array");
    }

    std::vector<json> events;
    events.reserve(response.value.events.size());
    for (const auto& event : response.value.events) {
        events.push_back(event);
    }
    return makeSuccess(std::move(events));
}

QueryResult<json> QueryClient::readSessionQuality(const RangeQuery& query, bool includeLiveTail) const {
    tape_engine::QueryRequest request;
    request.requestId = "tapescope-read-session-quality";
    request.operation = "read_session_quality";
    request.fromSessionSeq = query.firstSessionSeq;
    request.toSessionSeq = query.lastSessionSeq;
    request.includeLiveTail = includeLiveTail;

    return packSummaryAndEvents(performQuery(request));
}

QueryResult<json> QueryClient::readSessionOverview(const RangeQuery& query) const {
    tape_engine::QueryRequest request;
    request.requestId = "tapescope-read-session-overview";
    request.operation = "read_session_overview";
    request.fromSessionSeq = query.firstSessionSeq;
    request.toSessionSeq = query.lastSessionSeq;

    return packSummaryAndEvents(performQuery(request));
}

QueryResult<json> QueryClient::scanSessionReport(const RangeQuery& query) const {
    tape_engine::QueryRequest request;
    request.requestId = "tapescope-scan-session-report";
    request.operation = "scan_session_report";
    request.fromSessionSeq = query.firstSessionSeq;
    request.toSessionSeq = query.lastSessionSeq;

    return packSummaryAndEvents(performQuery(request));
}

QueryResult<json> QueryClient::listSessionReports(std::size_t limit) const {
    tape_engine::QueryRequest request;
    request.requestId = "tapescope-list-session-reports";
    request.operation = "list_session_reports";
    request.limit = limit;

    return packSummaryAndEvents(performQuery(request));
}

QueryResult<json> QueryClient::findOrderAnchor(const OrderAnchorQuery& anchorQuery) const {
    tape_engine::QueryRequest request;
    request.requestId = "tapescope-find-order-anchor";
    request.operation = "find_order_anchor";
    if (anchorQuery.traceId.has_value()) {
        request.traceId = *anchorQuery.traceId;
    }
    if (anchorQuery.orderId.has_value()) {
        request.orderId = *anchorQuery.orderId;
    }
    if (anchorQuery.permId.has_value()) {
        request.permId = *anchorQuery.permId;
    }
    if (anchorQuery.execId.has_value()) {
        request.execId = *anchorQuery.execId;
    }

    return packSummaryAndEvents(performQuery(request));
}

QueryResult<json> QueryClient::seekOrderAnchor(const OrderAnchorQuery& anchorQuery) const {
    tape_engine::QueryRequest request;
    request.requestId = "tapescope-seek-order-anchor";
    request.operation = "seek_order_anchor";
    if (anchorQuery.traceId.has_value()) {
        request.traceId = *anchorQuery.traceId;
    }
    if (anchorQuery.orderId.has_value()) {
        request.orderId = *anchorQuery.orderId;
    }
    if (anchorQuery.permId.has_value()) {
        request.permId = *anchorQuery.permId;
    }
    if (anchorQuery.execId.has_value()) {
        request.execId = *anchorQuery.execId;
    }

    return packSummaryAndEvents(performQuery(request));
}

QueryResult<json> QueryClient::readFinding(std::uint64_t findingId) const {
    tape_engine::QueryRequest request;
    request.requestId = "tapescope-read-finding";
    request.operation = "read_finding";
    request.findingId = findingId;

    return packSummaryAndEvents(performQuery(request));
}

QueryResult<json> QueryClient::readOrderCase(const OrderAnchorQuery& anchorQuery) const {
    tape_engine::QueryRequest request;
    request.requestId = "tapescope-read-order-case";
    request.operation = "read_order_case";
    if (anchorQuery.traceId.has_value()) {
        request.traceId = *anchorQuery.traceId;
    }
    if (anchorQuery.orderId.has_value()) {
        request.orderId = *anchorQuery.orderId;
    }
    if (anchorQuery.permId.has_value()) {
        request.permId = *anchorQuery.permId;
    }
    if (anchorQuery.execId.has_value()) {
        request.execId = *anchorQuery.execId;
    }

    return packSummaryAndEvents(performQuery(request));
}

QueryResult<json> QueryClient::scanOrderCaseReport(const OrderAnchorQuery& anchorQuery) const {
    tape_engine::QueryRequest request;
    request.requestId = "tapescope-scan-order-case-report";
    request.operation = "scan_order_case_report";
    if (anchorQuery.traceId.has_value()) {
        request.traceId = *anchorQuery.traceId;
    }
    if (anchorQuery.orderId.has_value()) {
        request.orderId = *anchorQuery.orderId;
    }
    if (anchorQuery.permId.has_value()) {
        request.permId = *anchorQuery.permId;
    }
    if (anchorQuery.execId.has_value()) {
        request.execId = *anchorQuery.execId;
    }

    return packSummaryAndEvents(performQuery(request));
}

QueryResult<json> QueryClient::listCaseReports(std::size_t limit) const {
    tape_engine::QueryRequest request;
    request.requestId = "tapescope-list-case-reports";
    request.operation = "list_case_reports";
    request.limit = limit;

    return packSummaryAndEvents(performQuery(request));
}

QueryResult<json> QueryClient::listIncidents(std::size_t limit) const {
    tape_engine::QueryRequest request;
    request.requestId = "tapescope-list-incidents";
    request.operation = "list_incidents";
    request.limit = limit;

    return packSummaryAndEvents(performQuery(request));
}

QueryResult<json> QueryClient::readIncident(std::uint64_t logicalIncidentId) const {
    tape_engine::QueryRequest request;
    request.requestId = "tapescope-read-incident";
    request.operation = "read_incident";
    request.logicalIncidentId = logicalIncidentId;

    return packSummaryAndEvents(performQuery(request));
}

QueryResult<json> QueryClient::readOrderAnchor(std::uint64_t anchorId) const {
    tape_engine::QueryRequest request;
    request.requestId = "tapescope-read-order-anchor";
    request.operation = "read_order_anchor";
    request.anchorId = anchorId;

    return packSummaryAndEvents(performQuery(request));
}

QueryResult<json> QueryClient::readArtifact(const std::string& artifactId) const {
    tape_engine::QueryRequest request;
    request.requestId = "tapescope-read-artifact";
    request.operation = "read_artifact";
    request.artifactId = artifactId;

    return packSummaryAndEvents(performQuery(request));
}

QueryResult<json> QueryClient::exportArtifact(const std::string& artifactId, const std::string& exportFormat) const {
    tape_engine::QueryRequest request;
    request.requestId = "tapescope-export-artifact";
    request.operation = "export_artifact";
    request.artifactId = artifactId;
    request.exportFormat = exportFormat;

    return packSummaryAndEvents(performQuery(request));
}

std::string QueryClient::describeError(const QueryError& error) {
    switch (error.kind) {
        case QueryErrorKind::None:
            return "No error";
        case QueryErrorKind::Transport:
            return "Engine unavailable: " + error.message;
        case QueryErrorKind::Remote:
            return "Engine query failed: " + error.message;
        case QueryErrorKind::MalformedResponse:
            return "Engine returned malformed data: " + error.message;
    }
    return error.message;
}

} // namespace tapescope
