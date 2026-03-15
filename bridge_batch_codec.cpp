#include "bridge_batch_codec.h"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <limits>
#include <stdexcept>

namespace bridge_batch {

namespace {

json anchorToJson(const BridgeAnchorIdentity& anchor) {
    return json{
        {"trace_id", anchor.traceId},
        {"order_id", static_cast<long long>(anchor.orderId)},
        {"perm_id", anchor.permId},
        {"exec_id", anchor.execId}
    };
}

BridgeAnchorIdentity anchorFromJson(const json& payload) {
    BridgeAnchorIdentity anchor;
    anchor.traceId = payload.value("trace_id", 0ULL);
    anchor.orderId = static_cast<OrderId>(payload.value("order_id", 0LL));
    anchor.permId = payload.value("perm_id", 0LL);
    anchor.execId = payload.value("exec_id", std::string());
    return anchor;
}

std::uint32_t decodeFrameSizePrefix(const std::vector<std::uint8_t>& frame) {
    if (frame.size() < 4) {
        throw std::runtime_error("bridge batch frame too short");
    }
    return (static_cast<std::uint32_t>(frame[0]) << 24) |
           (static_cast<std::uint32_t>(frame[1]) << 16) |
           (static_cast<std::uint32_t>(frame[2]) << 8) |
           static_cast<std::uint32_t>(frame[3]);
}

std::uint8_t parseNibble(char ch) {
    if (ch >= '0' && ch <= '9') {
        return static_cast<std::uint8_t>(ch - '0');
    }
    if (ch >= 'a' && ch <= 'f') {
        return static_cast<std::uint8_t>(10 + (ch - 'a'));
    }
    if (ch >= 'A' && ch <= 'F') {
        return static_cast<std::uint8_t>(10 + (ch - 'A'));
    }
    throw std::runtime_error("invalid hex digit in bridge batch fixture");
}

} // namespace

std::string_view flushReasonName(FlushReason reason) {
    switch (reason) {
        case FlushReason::ThresholdRecords:
            return "threshold_records";
        case FlushReason::ThresholdBytes:
            return "threshold_bytes";
        case FlushReason::TimerElapsed:
            return "timer_elapsed";
        case FlushReason::ImmediateLifecycle:
            return "immediate_lifecycle";
        case FlushReason::RecoveryDrain:
            return "recovery_drain";
        case FlushReason::Manual:
        default:
            return "manual";
    }
}

FlushReason parseFlushReason(std::string_view reason) {
    if (reason == "threshold_records") {
        return FlushReason::ThresholdRecords;
    }
    if (reason == "threshold_bytes") {
        return FlushReason::ThresholdBytes;
    }
    if (reason == "timer_elapsed") {
        return FlushReason::TimerElapsed;
    }
    if (reason == "immediate_lifecycle") {
        return FlushReason::ImmediateLifecycle;
    }
    if (reason == "recovery_drain") {
        return FlushReason::RecoveryDrain;
    }
    if (reason == "manual") {
        return FlushReason::Manual;
    }
    throw std::runtime_error("unknown bridge batch flush reason");
}

json recordToJson(const BridgeOutboxRecord& record) {
    json payload{
        {"anchor", anchorToJson(record.anchor)},
        {"fallback_reason", record.fallbackReason},
        {"fallback_state", record.fallbackState},
        {"note", record.note},
        {"record_type", record.recordType},
        {"side", record.side},
        {"source", record.source},
        {"source_seq", record.sourceSeq},
        {"symbol", record.symbol},
        {"wall_time", record.wallTime}
    };
    if (record.marketField >= 0) {
        payload["market_field"] = record.marketField;
    }
    if (record.bookPosition >= 0) {
        payload["book_position"] = record.bookPosition;
    }
    if (record.bookOperation >= 0) {
        payload["book_operation"] = record.bookOperation;
    }
    if (record.bookSide >= 0) {
        payload["book_side"] = record.bookSide;
    }
    if (std::isfinite(record.price)) {
        payload["price"] = record.price;
    }
    if (std::isfinite(record.size)) {
        payload["size"] = record.size;
    }
    return payload;
}

BridgeOutboxRecord recordFromJson(const json& payload) {
    BridgeOutboxRecord record;
    record.sourceSeq = payload.value("source_seq", 0ULL);
    record.recordType = payload.value("record_type", std::string());
    record.source = payload.value("source", std::string());
    record.symbol = payload.value("symbol", std::string());
    record.side = payload.value("side", std::string());
    record.marketField = payload.value("market_field", -1);
    record.bookPosition = payload.value("book_position", -1);
    record.bookOperation = payload.value("book_operation", -1);
    record.bookSide = payload.value("book_side", -1);
    record.price = payload.contains("price") && payload["price"].is_number()
        ? payload["price"].get<double>()
        : std::numeric_limits<double>::quiet_NaN();
    record.size = payload.contains("size") && payload["size"].is_number()
        ? payload["size"].get<double>()
        : std::numeric_limits<double>::quiet_NaN();
    record.anchor = anchorFromJson(payload.value("anchor", json::object()));
    record.fallbackState = payload.value("fallback_state", std::string());
    record.fallbackReason = payload.value("fallback_reason", std::string());
    record.note = payload.value("note", std::string());
    record.wallTime = payload.value("wall_time", std::string());
    return record;
}

json batchToJson(const Batch& batch) {
    json records = json::array();
    for (const auto& record : batch.records) {
        records.push_back(recordToJson(record));
    }

    return json{
        {"app_session_id", batch.header.appSessionId},
        {"batch_seq", batch.header.batchSeq},
        {"category", batch.header.category},
        {"first_source_seq", batch.header.firstSourceSeq},
        {"flush_reason", batch.header.flushReason},
        {"last_source_seq", batch.header.lastSourceSeq},
        {"producer", batch.header.producer},
        {"queue_label", batch.header.senderLabel},
        {"queue_qos", batch.header.senderQos},
        {"record_count", batch.header.recordCount},
        {"records", std::move(records)},
        {"runtime_session_id", batch.header.runtimeSessionId},
        {"schema", batch.header.schema},
        {"subsystem", batch.header.subsystem},
        {"transport", batch.header.transport},
        {"version", batch.header.version}
    };
}

Batch batchFromJson(const json& payload) {
    Batch batch;
    batch.header.version = payload.value("version", kWireVersion);
    batch.header.schema = payload.value("schema", std::string(kSchemaName));
    batch.header.producer = payload.value("producer", std::string(kProducerName));
    batch.header.transport = payload.value("transport", std::string(kTransportName));
    batch.header.subsystem = payload.value("subsystem", std::string(runtime_registry::kObservabilitySubsystem));
    batch.header.category = payload.value("category", std::string(runtime_registry::logCategoryName(runtime_registry::LogCategory::Bridge)));
    batch.header.senderLabel = payload.value("queue_label", std::string(runtime_registry::queueSpec(runtime_registry::QueueId::BridgeSender).label));
    batch.header.senderQos = payload.value("queue_qos", std::string(runtime_registry::queueSpec(runtime_registry::QueueId::BridgeSender).qosName));
    batch.header.appSessionId = payload.value("app_session_id", std::string());
    batch.header.runtimeSessionId = payload.value("runtime_session_id", std::string());
    batch.header.flushReason = payload.value("flush_reason", std::string(flushReasonName(FlushReason::Manual)));
    batch.header.batchSeq = payload.value("batch_seq", 0ULL);
    batch.header.firstSourceSeq = payload.value("first_source_seq", 0ULL);
    batch.header.lastSourceSeq = payload.value("last_source_seq", 0ULL);
    batch.header.recordCount = payload.value("record_count", 0ULL);

    const json records = payload.value("records", json::array());
    if (!records.is_array()) {
        throw std::runtime_error("bridge batch records payload must be an array");
    }
    batch.records.reserve(records.size());
    for (const auto& item : records) {
        batch.records.push_back(recordFromJson(item));
    }

    if (batch.header.recordCount == 0) {
        batch.header.recordCount = static_cast<std::uint64_t>(batch.records.size());
    }
    if (!batch.records.empty()) {
        if (batch.header.firstSourceSeq == 0) {
            batch.header.firstSourceSeq = batch.records.front().sourceSeq;
        }
        if (batch.header.lastSourceSeq == 0) {
            batch.header.lastSourceSeq = batch.records.back().sourceSeq;
        }
    }
    return batch;
}

Batch buildBatch(const std::vector<BridgeOutboxRecord>& records, const BuildOptions& options) {
    Batch batch;
    batch.header.appSessionId = options.appSessionId;
    batch.header.runtimeSessionId = options.runtimeSessionId;
    batch.header.senderLabel = options.senderLabel;
    batch.header.senderQos = options.senderQos;
    batch.header.flushReason = std::string(flushReasonName(options.flushReason));
    batch.header.batchSeq = options.batchSeq;
    batch.header.recordCount = static_cast<std::uint64_t>(records.size());
    batch.records = records;
    if (!records.empty()) {
        batch.header.firstSourceSeq = records.front().sourceSeq;
        batch.header.lastSourceSeq = records.back().sourceSeq;
    }
    return batch;
}

std::vector<std::uint8_t> encodePayload(const Batch& batch) {
    return json::to_msgpack(batchToJson(batch));
}

Batch decodePayload(const std::vector<std::uint8_t>& payload) {
    if (payload.empty()) {
        throw std::runtime_error("bridge batch payload is empty");
    }
    return batchFromJson(json::from_msgpack(payload));
}

std::vector<std::uint8_t> encodeFrame(const Batch& batch) {
    const std::vector<std::uint8_t> payload = encodePayload(batch);
    if (payload.size() > static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
        throw std::runtime_error("bridge batch payload too large to frame");
    }

    std::vector<std::uint8_t> frame;
    frame.reserve(4 + payload.size());
    const auto size = static_cast<std::uint32_t>(payload.size());
    frame.push_back(static_cast<std::uint8_t>((size >> 24) & 0xffU));
    frame.push_back(static_cast<std::uint8_t>((size >> 16) & 0xffU));
    frame.push_back(static_cast<std::uint8_t>((size >> 8) & 0xffU));
    frame.push_back(static_cast<std::uint8_t>(size & 0xffU));
    frame.insert(frame.end(), payload.begin(), payload.end());
    return frame;
}

Batch decodeFrame(const std::vector<std::uint8_t>& frame) {
    const std::uint32_t payloadSize = decodeFrameSizePrefix(frame);
    if (frame.size() != 4 + static_cast<std::size_t>(payloadSize)) {
        throw std::runtime_error("bridge batch frame size prefix does not match payload length");
    }
    return decodePayload(std::vector<std::uint8_t>(frame.begin() + 4, frame.end()));
}

std::string encodeFrameHex(const Batch& batch) {
    static constexpr char kHexDigits[] = "0123456789abcdef";
    const std::vector<std::uint8_t> frame = encodeFrame(batch);
    std::string encoded;
    encoded.reserve(frame.size() * 2);
    for (std::uint8_t byte : frame) {
        encoded.push_back(kHexDigits[(byte >> 4) & 0x0fU]);
        encoded.push_back(kHexDigits[byte & 0x0fU]);
    }
    return encoded;
}

std::vector<std::uint8_t> decodeHex(std::string_view hex) {
    std::vector<std::uint8_t> bytes;
    bytes.reserve(hex.size() / 2);

    bool highNibble = true;
    std::uint8_t current = 0;
    for (char ch : hex) {
        if (std::isspace(static_cast<unsigned char>(ch)) != 0) {
            continue;
        }
        const std::uint8_t nibble = parseNibble(ch);
        if (highNibble) {
            current = static_cast<std::uint8_t>(nibble << 4);
            highNibble = false;
        } else {
            current = static_cast<std::uint8_t>(current | nibble);
            bytes.push_back(current);
            highNibble = true;
        }
    }
    if (!highNibble) {
        throw std::runtime_error("bridge batch hex payload has odd length");
    }
    return bytes;
}

} // namespace bridge_batch
