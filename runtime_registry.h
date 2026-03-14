#pragma once

#include <array>
#include <string_view>

namespace runtime_registry {

// Keep these constants aligned with queues.yaml (phase-0 subset).
inline constexpr std::string_view kObservabilitySubsystem = "com.foxy.long.bridge";

#define RUNTIME_REGISTRY_SIGNPOST_BRIDGE_STAGE "bridge_stage"
inline constexpr std::string_view kBridgeStageSignpostName = RUNTIME_REGISTRY_SIGNPOST_BRIDGE_STAGE;

enum class LogCategory {
    Bridge
};

constexpr std::string_view logCategoryName(LogCategory category) {
    switch (category) {
        case LogCategory::Bridge:
        default:
            return "bridge";
    }
}

inline LogCategory parseLogCategory(std::string_view category) {
    if (category == "runtime" || category == "orders" || category == "ipc") {
        return LogCategory::Bridge;
    }
    return LogCategory::Bridge;
}

enum class QueueId {
    BridgeCapture,
    BridgeSender,
    OutboxJournal,
    OutboxDrain
};

struct QueueSpec {
    QueueId id;
    std::string_view label;
    std::string_view qosName;
};

inline constexpr std::array<QueueSpec, 4> kQueueSpecs = {{
    {QueueId::BridgeCapture, "com.foxy.long.bridge.capture", "inherited"},
    {QueueId::BridgeSender, "com.foxy.long.bridge.sender", "user_initiated"},
    {QueueId::OutboxJournal, "com.foxy.long.bridge.outbox-journal", "utility"},
    {QueueId::OutboxDrain, "com.foxy.long.bridge.outbox-drain", "utility"},
}};

constexpr QueueSpec queueSpec(QueueId id) {
    switch (id) {
        case QueueId::BridgeCapture:
            return kQueueSpecs[0];
        case QueueId::BridgeSender:
            return kQueueSpecs[1];
        case QueueId::OutboxJournal:
            return kQueueSpecs[2];
        case QueueId::OutboxDrain:
        default:
            return kQueueSpecs[3];
    }
}

} // namespace runtime_registry
