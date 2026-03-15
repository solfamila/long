#include "tape_engine_protocol.h"

#include <limits>
#include <string_view>
#include <stdexcept>

namespace tape_engine {

namespace {

std::uint32_t decodeFrameSizePrefix(const std::vector<std::uint8_t>& frame) {
    if (frame.size() < 4) {
        throw std::runtime_error("ingest ack frame too short");
    }
    return (static_cast<std::uint32_t>(frame[0]) << 24) |
           (static_cast<std::uint32_t>(frame[1]) << 16) |
           (static_cast<std::uint32_t>(frame[2]) << 8) |
           static_cast<std::uint32_t>(frame[3]);
}

std::string normalizeOperationName(std::string_view operationName) {
    std::string normalized;
    normalized.reserve(operationName.size());
    for (const char ch : operationName) {
        normalized.push_back(ch == '-' ? '_' : ch);
    }
    return normalized;
}

} // namespace

const char* queryOperationName(QueryOperation operation) {
    switch (operation) {
        case QueryOperation::Status:
            return "status";
        case QueryOperation::ReadLiveTail:
            return "read_live_tail";
        case QueryOperation::ReadRange:
            return "read_range";
        case QueryOperation::ReplaySnapshot:
            return "replay_snapshot";
        case QueryOperation::ReadSessionQuality:
            return "read_session_quality";
        case QueryOperation::ReadSessionOverview:
            return "read_session_overview";
        case QueryOperation::ScanSessionReport:
            return "scan_session_report";
        case QueryOperation::ReadSessionReport:
            return "read_session_report";
        case QueryOperation::ListSessionReports:
            return "list_session_reports";
        case QueryOperation::ScanIncidentReport:
            return "scan_incident_report";
        case QueryOperation::ScanOrderCaseReport:
            return "scan_order_case_report";
        case QueryOperation::ReadCaseReport:
            return "read_case_report";
        case QueryOperation::ListCaseReports:
            return "list_case_reports";
        case QueryOperation::FindOrderAnchor:
            return "find_order_anchor";
        case QueryOperation::SeekOrderAnchor:
            return "seek_order_anchor";
        case QueryOperation::ReadOrderCase:
            return "read_order_case";
        case QueryOperation::ReadOrderAnchor:
            return "read_order_anchor";
        case QueryOperation::ListOrderAnchors:
            return "list_order_anchors";
        case QueryOperation::ListProtectedWindows:
            return "list_protected_windows";
        case QueryOperation::ReadProtectedWindow:
            return "read_protected_window";
        case QueryOperation::ListFindings:
            return "list_findings";
        case QueryOperation::ReadFinding:
            return "read_finding";
        case QueryOperation::ReadIncident:
            return "read_incident";
        case QueryOperation::ListIncidents:
            return "list_incidents";
        case QueryOperation::ReadArtifact:
            return "read_artifact";
        case QueryOperation::ExportArtifact:
            return "export_artifact";
        case QueryOperation::Unknown:
            return "unknown";
    }
    return "unknown";
}

std::string canonicalizeQueryOperationName(std::string_view operationName) {
    return normalizeOperationName(operationName);
}

QueryOperation queryOperationFromString(std::string_view operationName) {
    const std::string normalized = normalizeOperationName(operationName);
    if (normalized == "status") {
        return QueryOperation::Status;
    }
    if (normalized == "read_live_tail") {
        return QueryOperation::ReadLiveTail;
    }
    if (normalized == "read_range") {
        return QueryOperation::ReadRange;
    }
    if (normalized == "replay_snapshot") {
        return QueryOperation::ReplaySnapshot;
    }
    if (normalized == "read_session_quality") {
        return QueryOperation::ReadSessionQuality;
    }
    if (normalized == "read_session_overview") {
        return QueryOperation::ReadSessionOverview;
    }
    if (normalized == "scan_session_report") {
        return QueryOperation::ScanSessionReport;
    }
    if (normalized == "read_session_report") {
        return QueryOperation::ReadSessionReport;
    }
    if (normalized == "list_session_reports") {
        return QueryOperation::ListSessionReports;
    }
    if (normalized == "scan_incident_report") {
        return QueryOperation::ScanIncidentReport;
    }
    if (normalized == "scan_order_case_report") {
        return QueryOperation::ScanOrderCaseReport;
    }
    if (normalized == "read_case_report") {
        return QueryOperation::ReadCaseReport;
    }
    if (normalized == "list_case_reports") {
        return QueryOperation::ListCaseReports;
    }
    if (normalized == "find_order_anchor") {
        return QueryOperation::FindOrderAnchor;
    }
    if (normalized == "seek_order_anchor") {
        return QueryOperation::SeekOrderAnchor;
    }
    if (normalized == "read_order_case") {
        return QueryOperation::ReadOrderCase;
    }
    if (normalized == "read_order_anchor") {
        return QueryOperation::ReadOrderAnchor;
    }
    if (normalized == "list_order_anchors") {
        return QueryOperation::ListOrderAnchors;
    }
    if (normalized == "list_protected_windows") {
        return QueryOperation::ListProtectedWindows;
    }
    if (normalized == "read_protected_window") {
        return QueryOperation::ReadProtectedWindow;
    }
    if (normalized == "list_findings") {
        return QueryOperation::ListFindings;
    }
    if (normalized == "read_finding") {
        return QueryOperation::ReadFinding;
    }
    if (normalized == "read_incident") {
        return QueryOperation::ReadIncident;
    }
    if (normalized == "list_incidents") {
        return QueryOperation::ListIncidents;
    }
    if (normalized == "read_artifact") {
        return QueryOperation::ReadArtifact;
    }
    if (normalized == "export_artifact") {
        return QueryOperation::ExportArtifact;
    }
    if (normalized == "unknown") {
        return QueryOperation::Unknown;
    }
    return QueryOperation::Unknown;
}

QueryRequest makeQueryRequest(QueryOperation operation, std::string requestId) {
    QueryRequest request;
    request.requestId = std::move(requestId);
    request.operationKind = operation;
    request.operation = queryOperationName(operation);
    return request;
}

json decodeFramedJson(const std::vector<std::uint8_t>& frame) {
    const std::uint32_t payloadSize = decodeFrameSizePrefix(frame);
    if (frame.size() != 4 + static_cast<std::size_t>(payloadSize)) {
        throw std::runtime_error("framed payload size prefix does not match payload length");
    }
    return json::from_msgpack(std::vector<std::uint8_t>(frame.begin() + 4, frame.end()));
}

std::vector<std::uint8_t> encodeFramedJson(const json& payload) {
    const std::vector<std::uint8_t> bytes = json::to_msgpack(payload);
    if (bytes.size() > static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
        throw std::runtime_error("framed payload too large");
    }

    const auto size = static_cast<std::uint32_t>(bytes.size());
    std::vector<std::uint8_t> frame;
    frame.reserve(4 + bytes.size());
    frame.push_back(static_cast<std::uint8_t>((size >> 24) & 0xffU));
    frame.push_back(static_cast<std::uint8_t>((size >> 16) & 0xffU));
    frame.push_back(static_cast<std::uint8_t>((size >> 8) & 0xffU));
    frame.push_back(static_cast<std::uint8_t>(size & 0xffU));
    frame.insert(frame.end(), bytes.begin(), bytes.end());
    return frame;
}

json ackToJson(const IngestAck& ack) {
    json payload{
        {"accepted_records", ack.acceptedRecords},
        {"adapter_id", ack.adapterId},
        {"assigned_revision_id", ack.assignedRevisionId},
        {"batch_seq", ack.batchSeq},
        {"connection_id", ack.connectionId},
        {"duplicate_records", ack.duplicateRecords},
        {"first_session_seq", ack.firstSessionSeq},
        {"first_source_seq", ack.firstSourceSeq},
        {"gap_markers", ack.gapMarkers},
        {"last_session_seq", ack.lastSessionSeq},
        {"last_source_seq", ack.lastSourceSeq},
        {"schema", ack.schema},
        {"status", ack.status},
        {"version", ack.version}
    };
    if (!ack.error.empty()) {
        payload["error"] = ack.error;
    }
    return payload;
}

IngestAck ackFromJson(const json& payload) {
    IngestAck ack;
    ack.version = payload.value("version", kAckWireVersion);
    ack.schema = payload.value("schema", std::string(kAckSchema));
    ack.status = payload.value("status", std::string("accepted"));
    ack.batchSeq = payload.value("batch_seq", 0ULL);
    ack.assignedRevisionId = payload.value("assigned_revision_id", 0ULL);
    ack.adapterId = payload.value("adapter_id", std::string());
    ack.connectionId = payload.value("connection_id", std::string());
    ack.acceptedRecords = payload.value("accepted_records", 0ULL);
    ack.duplicateRecords = payload.value("duplicate_records", 0ULL);
    ack.gapMarkers = payload.value("gap_markers", 0ULL);
    ack.firstSessionSeq = payload.value("first_session_seq", 0ULL);
    ack.lastSessionSeq = payload.value("last_session_seq", 0ULL);
    ack.firstSourceSeq = payload.value("first_source_seq", 0ULL);
    ack.lastSourceSeq = payload.value("last_source_seq", 0ULL);
    ack.error = payload.value("error", std::string());
    return ack;
}

std::vector<std::uint8_t> encodeAckPayload(const IngestAck& ack) {
    return json::to_msgpack(ackToJson(ack));
}

IngestAck decodeAckPayload(const std::vector<std::uint8_t>& payload) {
    if (payload.empty()) {
        throw std::runtime_error("ingest ack payload is empty");
    }
    return ackFromJson(json::from_msgpack(payload));
}

std::vector<std::uint8_t> encodeAckFrame(const IngestAck& ack) {
    return encodeFramedJson(ackToJson(ack));
}

IngestAck decodeAckFrame(const std::vector<std::uint8_t>& frame) {
    return ackFromJson(decodeFramedJson(frame));
}

json queryRequestToJson(const QueryRequest& request) {
    const std::string operationName = request.operation.empty()
                                          ? std::string(queryOperationName(request.operationKind))
                                          : canonicalizeQueryOperationName(request.operation);
    json payload{
        {"anchor_id", request.anchorId},
        {"finding_id", request.findingId},
        {"from_session_seq", request.fromSessionSeq},
        {"include_live_tail", request.includeLiveTail},
        {"limit", request.limit},
        {"operation", operationName},
        {"logical_incident_id", request.logicalIncidentId},
        {"order_id", request.orderId},
        {"perm_id", request.permId},
        {"report_id", request.reportId},
        {"revision_id", request.revisionId},
        {"request_id", request.requestId},
        {"schema", request.schema},
        {"target_session_seq", request.targetSessionSeq},
        {"to_session_seq", request.toSessionSeq},
        {"trace_id", request.traceId},
        {"window_id", request.windowId},
        {"version", request.version}
    };
    if (!request.execId.empty()) {
        payload["exec_id"] = request.execId;
    }
    if (!request.artifactId.empty()) {
        payload["artifact_id"] = request.artifactId;
    }
    if (!request.exportFormat.empty()) {
        payload["export_format"] = request.exportFormat;
    }
    return payload;
}

QueryRequest queryRequestFromJson(const json& payload) {
    QueryRequest request;
    request.version = payload.value("version", kAckWireVersion);
    request.schema = payload.value("schema", std::string(kQueryRequestSchema));
    request.requestId = payload.value("request_id", std::string());
    request.operation = canonicalizeQueryOperationName(payload.value("operation", std::string()));
    request.operationKind = queryOperationFromString(request.operation);
    request.revisionId = payload.value("revision_id", 0ULL);
    request.fromSessionSeq = payload.value("from_session_seq", 0ULL);
    request.toSessionSeq = payload.value("to_session_seq", 0ULL);
    request.targetSessionSeq = payload.value("target_session_seq", 0ULL);
    request.windowId = payload.value("window_id", 0ULL);
    request.logicalIncidentId = payload.value("logical_incident_id", 0ULL);
    request.findingId = payload.value("finding_id", 0ULL);
    request.anchorId = payload.value("anchor_id", 0ULL);
    request.reportId = payload.value("report_id", 0ULL);
    request.limit = payload.value("limit", static_cast<std::size_t>(0));
    request.includeLiveTail = payload.value("include_live_tail", false);
    request.traceId = payload.value("trace_id", 0ULL);
    request.orderId = payload.value("order_id", 0LL);
    request.permId = payload.value("perm_id", 0LL);
    request.execId = payload.value("exec_id", std::string());
    request.artifactId = payload.value("artifact_id", std::string());
    request.exportFormat = payload.value("export_format", std::string());
    return request;
}

json queryResponseToJson(const QueryResponse& response) {
    json payload{
        {"events", response.events},
        {"operation", response.operation},
        {"request_id", response.requestId},
        {"schema", response.schema},
        {"status", response.status},
        {"summary", response.summary},
        {"version", response.version}
    };
    if (!response.error.empty()) {
        payload["error"] = response.error;
    }
    return payload;
}

QueryResponse queryResponseFromJson(const json& payload) {
    QueryResponse response;
    response.version = payload.value("version", kAckWireVersion);
    response.schema = payload.value("schema", std::string(kQueryResponseSchema));
    response.requestId = payload.value("request_id", std::string());
    response.operation = payload.value("operation", std::string());
    response.status = payload.value("status", std::string("ok"));
    response.error = payload.value("error", std::string());
    response.summary = payload.value("summary", json::object());
    response.events = payload.value("events", json::array());
    return response;
}

std::vector<std::uint8_t> encodeQueryRequestPayload(const QueryRequest& request) {
    return json::to_msgpack(queryRequestToJson(request));
}

QueryRequest decodeQueryRequestPayload(const std::vector<std::uint8_t>& payload) {
    if (payload.empty()) {
        throw std::runtime_error("query request payload is empty");
    }
    return queryRequestFromJson(json::from_msgpack(payload));
}

std::vector<std::uint8_t> encodeQueryRequestFrame(const QueryRequest& request) {
    return encodeFramedJson(queryRequestToJson(request));
}

QueryRequest decodeQueryRequestFrame(const std::vector<std::uint8_t>& frame) {
    return queryRequestFromJson(decodeFramedJson(frame));
}

std::vector<std::uint8_t> encodeQueryResponsePayload(const QueryResponse& response) {
    return json::to_msgpack(queryResponseToJson(response));
}

QueryResponse decodeQueryResponsePayload(const std::vector<std::uint8_t>& payload) {
    if (payload.empty()) {
        throw std::runtime_error("query response payload is empty");
    }
    return queryResponseFromJson(json::from_msgpack(payload));
}

std::vector<std::uint8_t> encodeQueryResponseFrame(const QueryResponse& response) {
    return encodeFramedJson(queryResponseToJson(response));
}

QueryResponse decodeQueryResponseFrame(const std::vector<std::uint8_t>& frame) {
    return queryResponseFromJson(decodeFramedJson(frame));
}

} // namespace tape_engine
