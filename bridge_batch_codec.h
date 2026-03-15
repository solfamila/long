#pragma once

#include "app_shared.h"
#include "runtime_registry.h"

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace bridge_batch {

inline constexpr std::uint32_t kWireVersion = 1;
inline constexpr std::string_view kSchemaName = "com.foxy.long.bridge.batch";
inline constexpr std::string_view kProducerName = "long";
inline constexpr std::string_view kTransportName = "framed_msgpack_v1";

enum class FlushReason {
    ThresholdRecords,
    ThresholdBytes,
    TimerElapsed,
    ImmediateLifecycle,
    RecoveryDrain,
    Manual
};

std::string_view flushReasonName(FlushReason reason);
FlushReason parseFlushReason(std::string_view reason);

struct BuildOptions {
    std::string appSessionId;
    std::string runtimeSessionId;
    std::string senderLabel = std::string(runtime_registry::queueSpec(runtime_registry::QueueId::BridgeSender).label);
    std::string senderQos = std::string(runtime_registry::queueSpec(runtime_registry::QueueId::BridgeSender).qosName);
    FlushReason flushReason = FlushReason::Manual;
    std::uint64_t batchSeq = 0;
};

struct BatchHeader {
    std::uint32_t version = kWireVersion;
    std::string schema = std::string(kSchemaName);
    std::string producer = std::string(kProducerName);
    std::string transport = std::string(kTransportName);
    std::string subsystem = std::string(runtime_registry::kObservabilitySubsystem);
    std::string category = std::string(runtime_registry::logCategoryName(runtime_registry::LogCategory::Bridge));
    std::string senderLabel = std::string(runtime_registry::queueSpec(runtime_registry::QueueId::BridgeSender).label);
    std::string senderQos = std::string(runtime_registry::queueSpec(runtime_registry::QueueId::BridgeSender).qosName);
    std::string appSessionId;
    std::string runtimeSessionId;
    std::string flushReason = std::string(flushReasonName(FlushReason::Manual));
    std::uint64_t batchSeq = 0;
    std::uint64_t firstSourceSeq = 0;
    std::uint64_t lastSourceSeq = 0;
    std::uint64_t recordCount = 0;
};

struct Batch {
    BatchHeader header;
    std::vector<BridgeOutboxRecord> records;
};

json recordToJson(const BridgeOutboxRecord& record);
BridgeOutboxRecord recordFromJson(const json& payload);

json batchToJson(const Batch& batch);
Batch batchFromJson(const json& payload);

Batch buildBatch(const std::vector<BridgeOutboxRecord>& records, const BuildOptions& options);

std::vector<std::uint8_t> encodePayload(const Batch& batch);
Batch decodePayload(const std::vector<std::uint8_t>& payload);

std::vector<std::uint8_t> encodeFrame(const Batch& batch);
Batch decodeFrame(const std::vector<std::uint8_t>& frame);

std::string encodeFrameHex(const Batch& batch);
std::vector<std::uint8_t> decodeHex(std::string_view hex);

} // namespace bridge_batch
