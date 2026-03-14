#pragma once

#include <array>
#include <string_view>

namespace runtime_registry {

// Keep these constants aligned with queues.yaml (phase-0 subset).
inline constexpr std::string_view kObservabilitySubsystem = "com.foxy.twstradinggui";

#define RUNTIME_REGISTRY_SIGNPOST_TRADE_STAGE "trade_stage"
inline constexpr std::string_view kTradeStageSignpostName = RUNTIME_REGISTRY_SIGNPOST_TRADE_STAGE;

enum class LogCategory {
    Runtime,
    Orders,
    Ipc
};

constexpr std::string_view logCategoryName(LogCategory category) {
    switch (category) {
        case LogCategory::Orders:
            return "orders";
        case LogCategory::Ipc:
            return "ipc";
        case LogCategory::Runtime:
        default:
            return "runtime";
    }
}

inline LogCategory parseLogCategory(std::string_view category) {
    if (category == logCategoryName(LogCategory::Orders)) {
        return LogCategory::Orders;
    }
    if (category == logCategoryName(LogCategory::Ipc)) {
        return LogCategory::Ipc;
    }
    return LogCategory::Runtime;
}

enum class QueueId {
    MainThread,
    RuntimeRefreshScheduler
};

struct QueueSpec {
    QueueId id;
    std::string_view label;
    std::string_view qosName;
};

inline constexpr std::array<QueueSpec, 2> kQueueSpecs = {{
    {QueueId::MainThread, "com.apple.main-thread", "user_interactive"},
    {QueueId::RuntimeRefreshScheduler, "com.foxy.twstradinggui.runtime.refresh_scheduler", "user_interactive"},
}};

constexpr QueueSpec queueSpec(QueueId id) {
    switch (id) {
        case QueueId::RuntimeRefreshScheduler:
            return kQueueSpecs[1];
        case QueueId::MainThread:
        default:
            return kQueueSpecs[0];
    }
}

} // namespace runtime_registry
