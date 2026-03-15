#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace tape_engine {

inline constexpr std::uint32_t kAckWireVersion = 1;
inline constexpr const char* kAckSchema = "com.foxy.tape-engine.ingest-ack";

struct IngestAck {
    std::uint32_t version = kAckWireVersion;
    std::string schema = kAckSchema;
    std::string status = "accepted";
    std::uint64_t batchSeq = 0;
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

} // namespace tape_engine
