#include "tape_engine_protocol.h"

#include <limits>
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

} // namespace

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
    json payload{
        {"from_session_seq", request.fromSessionSeq},
        {"include_live_tail", request.includeLiveTail},
        {"limit", request.limit},
        {"operation", request.operation},
        {"order_id", request.orderId},
        {"perm_id", request.permId},
        {"revision_id", request.revisionId},
        {"request_id", request.requestId},
        {"schema", request.schema},
        {"target_session_seq", request.targetSessionSeq},
        {"to_session_seq", request.toSessionSeq},
        {"trace_id", request.traceId},
        {"version", request.version}
    };
    if (!request.execId.empty()) {
        payload["exec_id"] = request.execId;
    }
    return payload;
}

QueryRequest queryRequestFromJson(const json& payload) {
    QueryRequest request;
    request.version = payload.value("version", kAckWireVersion);
    request.schema = payload.value("schema", std::string(kQueryRequestSchema));
    request.requestId = payload.value("request_id", std::string());
    request.operation = payload.value("operation", std::string());
    request.revisionId = payload.value("revision_id", 0ULL);
    request.fromSessionSeq = payload.value("from_session_seq", 0ULL);
    request.toSessionSeq = payload.value("to_session_seq", 0ULL);
    request.targetSessionSeq = payload.value("target_session_seq", 0ULL);
    request.limit = payload.value("limit", static_cast<std::size_t>(0));
    request.includeLiveTail = payload.value("include_live_tail", false);
    request.traceId = payload.value("trace_id", 0ULL);
    request.orderId = payload.value("order_id", 0LL);
    request.permId = payload.value("perm_id", 0LL);
    request.execId = payload.value("exec_id", std::string());
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
