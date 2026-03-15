#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace tape_engine {

inline constexpr std::uint32_t kAckWireVersion = 1;
inline constexpr const char* kAckSchema = "com.foxy.tape-engine.ingest-ack";
inline constexpr const char* kQueryRequestSchema = "com.foxy.tape-engine.query";
inline constexpr const char* kQueryResponseSchema = "com.foxy.tape-engine.query-response";

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
    std::uint64_t revisionId = 0;
    std::uint64_t fromSessionSeq = 0;
    std::uint64_t toSessionSeq = 0;
    std::uint64_t targetSessionSeq = 0;
    std::uint64_t windowId = 0;
    std::size_t limit = 0;
    bool includeLiveTail = false;
    std::uint64_t traceId = 0;
    long long orderId = 0;
    long long permId = 0;
    std::string execId;
};

struct QueryResponse {
    std::uint32_t version = kAckWireVersion;
    std::string schema = kQueryResponseSchema;
    std::string requestId;
    std::string operation;
    std::string status = "ok";
    std::string error;
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
