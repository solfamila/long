#import <os/log.h>
#import <os/signpost.h>

#include "mac_observability.h"
#include "runtime_registry.h"

#include <mutex>
#include <string>
#include <unordered_map>

namespace {

#define TAPE_BRIDGE_STAGE_SIGNPOST "bridge_stage"

os_log_t bridgeLog() {
    static os_log_t log = os_log_create(runtime_registry::kObservabilitySubsystem.data(),
                                        runtime_registry::logCategoryName(runtime_registry::LogCategory::Bridge).data());
    return log;
}

runtime_registry::LogCategory resolveCategory(const std::string& category) {
    for (const auto spec : runtime_registry::kQueueSpecs) {
        if (runtime_registry::logCategoryName(spec.category) == category) {
            return spec.category;
        }
    }
    for (runtime_registry::LogCategory value : {
             runtime_registry::LogCategory::Bridge,
             runtime_registry::LogCategory::Ingest,
             runtime_registry::LogCategory::Sequencer,
             runtime_registry::LogCategory::Recorder,
             runtime_registry::LogCategory::Revision,
             runtime_registry::LogCategory::Replay,
             runtime_registry::LogCategory::Rpc,
             runtime_registry::LogCategory::Ui,
             runtime_registry::LogCategory::Metal,
             runtime_registry::LogCategory::Export,
             runtime_registry::LogCategory::Housekeeping}) {
        if (runtime_registry::logCategoryName(value) == category) {
            return value;
        }
    }
    return runtime_registry::LogCategory::Bridge;
}

os_log_t logForCategory(const std::string& category) {
    static std::mutex mutex;
    static std::unordered_map<std::string, os_log_t> logs;
    const auto resolved = resolveCategory(category);
    const std::string categoryName(runtime_registry::logCategoryName(resolved));

    std::lock_guard<std::mutex> lock(mutex);
    const auto it = logs.find(categoryName);
    if (it != logs.end()) {
        return it->second;
    }
    const auto subsystem = runtime_registry::subsystemName(runtime_registry::SubsystemId::LongBridge);
    const os_log_t log = os_log_create(subsystem.data(), categoryName.c_str());
    logs.emplace(categoryName, log);
    return log;
}

std::mutex& signpostMutex() {
    static std::mutex mutex;
    return mutex;
}

std::unordered_map<std::string, os_signpost_id_t>& signpostIds() {
    static std::unordered_map<std::string, os_signpost_id_t> ids;
    return ids;
}

std::string makeSignpostKey(std::uint64_t traceId, const std::string& stage) {
    return std::to_string(static_cast<unsigned long long>(traceId)) + "|" + stage;
}

os_signpost_id_t signpostIdFor(os_log_t log, std::uint64_t traceId, const std::string& stage, bool createIfMissing) {
    const std::string key = makeSignpostKey(traceId, stage);
    std::lock_guard<std::mutex> lock(signpostMutex());
    auto& ids = signpostIds();
    const auto it = ids.find(key);
    if (it != ids.end()) {
        return it->second;
    }
    if (!createIfMissing) {
        return OS_SIGNPOST_ID_NULL;
    }
    const os_signpost_id_t id = os_signpost_id_generate(log);
    ids.emplace(key, id);
    return id;
}

void clearSignpostId(std::uint64_t traceId, const std::string& stage) {
    std::lock_guard<std::mutex> lock(signpostMutex());
    signpostIds().erase(makeSignpostKey(traceId, stage));
}

} // namespace

void macLogInfo(const std::string& category, const std::string& message) {
    os_log_with_type(logForCategory(category), OS_LOG_TYPE_INFO, "%{public}s", message.c_str());
}

void macLogError(const std::string& category, const std::string& message) {
    os_log_with_type(logForCategory(category), OS_LOG_TYPE_ERROR, "%{public}s", message.c_str());
}

void macTraceBegin(std::uint64_t traceId, const std::string& stage, const std::string& message) {
    const os_log_t log = bridgeLog();
    const os_signpost_id_t id = signpostIdFor(log, traceId, stage, true);
    os_signpost_interval_begin(log,
                               id,
                               TAPE_BRIDGE_STAGE_SIGNPOST,
                               "trace=%{public}llu stage=%{public}s %{public}s",
                               static_cast<unsigned long long>(traceId),
                               stage.c_str(),
                               message.c_str());
}

void macTraceEnd(std::uint64_t traceId, const std::string& stage, const std::string& message) {
    const os_log_t log = bridgeLog();
    const os_signpost_id_t id = signpostIdFor(log, traceId, stage, false);
    if (id != OS_SIGNPOST_ID_NULL) {
        os_signpost_interval_end(log,
                                 id,
                                 TAPE_BRIDGE_STAGE_SIGNPOST,
                                 "trace=%{public}llu stage=%{public}s %{public}s",
                                 static_cast<unsigned long long>(traceId),
                                 stage.c_str(),
                                 message.c_str());
        clearSignpostId(traceId, stage);
    } else {
        os_signpost_event_emit(log,
                               OS_SIGNPOST_ID_EXCLUSIVE,
                               TAPE_BRIDGE_STAGE_SIGNPOST,
                               "trace=%{public}llu stage=%{public}s %{public}s",
                               static_cast<unsigned long long>(traceId),
                               stage.c_str(),
                               message.c_str());
    }
}

void macTraceEvent(std::uint64_t traceId, const std::string& stage, const std::string& message) {
    const os_log_t log = bridgeLog();
    os_signpost_event_emit(log,
                           OS_SIGNPOST_ID_EXCLUSIVE,
                           TAPE_BRIDGE_STAGE_SIGNPOST,
                           "trace=%{public}llu stage=%{public}s %{public}s",
                           static_cast<unsigned long long>(traceId),
                           stage.c_str(),
                           message.c_str());
}
