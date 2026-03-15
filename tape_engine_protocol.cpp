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

json ackToJson(const IngestAck& ack) {
    json payload{
        {"accepted_records", ack.acceptedRecords},
        {"adapter_id", ack.adapterId},
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
    const std::vector<std::uint8_t> payload = encodeAckPayload(ack);
    if (payload.size() > static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
        throw std::runtime_error("ingest ack payload too large to frame");
    }

    const auto size = static_cast<std::uint32_t>(payload.size());
    std::vector<std::uint8_t> frame;
    frame.reserve(4 + payload.size());
    frame.push_back(static_cast<std::uint8_t>((size >> 24) & 0xffU));
    frame.push_back(static_cast<std::uint8_t>((size >> 16) & 0xffU));
    frame.push_back(static_cast<std::uint8_t>((size >> 8) & 0xffU));
    frame.push_back(static_cast<std::uint8_t>(size & 0xffU));
    frame.insert(frame.end(), payload.begin(), payload.end());
    return frame;
}

IngestAck decodeAckFrame(const std::vector<std::uint8_t>& frame) {
    const std::uint32_t payloadSize = decodeFrameSizePrefix(frame);
    if (frame.size() != 4 + static_cast<std::size_t>(payloadSize)) {
        throw std::runtime_error("ingest ack frame size prefix does not match payload length");
    }
    return decodeAckPayload(std::vector<std::uint8_t>(frame.begin() + 4, frame.end()));
}

} // namespace tape_engine
