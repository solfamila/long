#import <os/log.h>
#import <os/signpost.h>

#include "mac_observability.h"

#include <mutex>
#include <string>
#include <unordered_map>

namespace {

os_log_t runtimeLog() {
    static os_log_t log = os_log_create("com.foxy.twstradinggui.short", "runtime");
    return log;
}

os_log_t orderLog() {
    static os_log_t log = os_log_create("com.foxy.twstradinggui.short", "orders");
    return log;
}

os_log_t ipcLog() {
    static os_log_t log = os_log_create("com.foxy.twstradinggui.short", "ipc");
    return log;
}

os_log_t logForCategory(const std::string& category) {
    if (category == "orders") {
        return orderLog();
    }
    if (category == "ipc") {
        return ipcLog();
    }
    return runtimeLog();
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
    const os_log_t log = orderLog();
    const os_signpost_id_t id = signpostIdFor(log, traceId, stage, true);
    os_signpost_interval_begin(log,
                               id,
                               "TradeStage",
                               "trace=%{public}llu stage=%{public}s %{public}s",
                               static_cast<unsigned long long>(traceId),
                               stage.c_str(),
                               message.c_str());
}

void macTraceEnd(std::uint64_t traceId, const std::string& stage, const std::string& message) {
    const os_log_t log = orderLog();
    const os_signpost_id_t id = signpostIdFor(log, traceId, stage, false);
    if (id != OS_SIGNPOST_ID_NULL) {
        os_signpost_interval_end(log,
                                 id,
                                 "TradeStage",
                                 "trace=%{public}llu stage=%{public}s %{public}s",
                                 static_cast<unsigned long long>(traceId),
                                 stage.c_str(),
                                 message.c_str());
        clearSignpostId(traceId, stage);
    } else {
        os_signpost_event_emit(log,
                               OS_SIGNPOST_ID_EXCLUSIVE,
                               "TradeStage",
                               "trace=%{public}llu stage=%{public}s %{public}s",
                               static_cast<unsigned long long>(traceId),
                               stage.c_str(),
                               message.c_str());
    }
}

void macTraceEvent(std::uint64_t traceId, const std::string& stage, const std::string& message) {
    const os_log_t log = orderLog();
    os_signpost_event_emit(log,
                           OS_SIGNPOST_ID_EXCLUSIVE,
                           "TradeStage",
                           "trace=%{public}llu stage=%{public}s %{public}s",
                           static_cast<unsigned long long>(traceId),
                           stage.c_str(),
                           message.c_str());
}
