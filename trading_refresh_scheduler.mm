#import "trading_refresh_scheduler.h"

#include "runtime_registry.h"

#include <cmath>
#include <mutex>
#include <utility>

namespace {

double refreshDelaySeconds(bool appActive, NSProcessInfoThermalState thermalState) {
    switch (thermalState) {
        case NSProcessInfoThermalStateCritical:
            return 1.0;
        case NSProcessInfoThermalStateSerious:
            return appActive ? 0.35 : 0.6;
        case NSProcessInfoThermalStateFair:
            return appActive ? 0.12 : 0.25;
        case NSProcessInfoThermalStateNominal:
        default:
            return appActive ? 0.0 : 0.12;
    }
}

dispatch_qos_class_t dispatchQosClassForName(std::string_view qosName) {
    if (qosName == "user_interactive") {
        return QOS_CLASS_USER_INTERACTIVE;
    }
    if (qosName == "user_initiated") {
        return QOS_CLASS_USER_INITIATED;
    }
    if (qosName == "utility") {
        return QOS_CLASS_UTILITY;
    }
    if (qosName == "background") {
        return QOS_CLASS_BACKGROUND;
    }
    return QOS_CLASS_DEFAULT;
}

dispatch_queue_t refreshSchedulerQueue() {
    static dispatch_queue_t queue = [] {
        const auto spec = runtime_registry::queueSpec(runtime_registry::QueueId::RuntimeRefreshScheduler);
        dispatch_queue_attr_t attrs = dispatch_queue_attr_make_with_qos_class(DISPATCH_QUEUE_SERIAL,
                                                                              dispatchQosClassForName(spec.qosName),
                                                                              0);
        return dispatch_queue_create(spec.label.data(), attrs);
    }();
    return queue;
}

} // namespace

struct TradingRefreshScheduler::Impl {
    std::mutex mutex;
    Callback callback;
    bool pending = false;
    bool appActive = true;
    NSProcessInfoThermalState thermalState = NSProcessInfoThermalStateNominal;
    std::uint64_t generation = 0;

    void dispatchPending(std::shared_ptr<Impl> self,
                         std::uint64_t scheduledGeneration,
                         double delaySeconds) {
        dispatch_block_t block = ^{
            Callback callbackCopy;
            {
                std::lock_guard<std::mutex> lock(self->mutex);
                if (!self->pending || self->generation != scheduledGeneration) {
                    return;
                }
                self->pending = false;
                callbackCopy = self->callback;
            }
            if (callbackCopy) {
                dispatch_async(dispatch_get_main_queue(), ^{
                    callbackCopy();
                });
            }
        };

        if (delaySeconds <= 0.0) {
            dispatch_async(refreshSchedulerQueue(), block);
            return;
        }

        const auto nanos = static_cast<int64_t>(std::llround(delaySeconds * static_cast<double>(NSEC_PER_SEC)));
        dispatch_after(dispatch_time(DISPATCH_TIME_NOW, nanos), refreshSchedulerQueue(), block);
    }

    void scheduleLocked(std::shared_ptr<Impl> self) {
        pending = true;
        const std::uint64_t scheduledGeneration = ++generation;
        const double delaySeconds = refreshDelaySeconds(appActive, thermalState);
        dispatchPending(self, scheduledGeneration, delaySeconds);
    }

    void rescheduleIfPendingLocked(std::shared_ptr<Impl> self) {
        if (!pending) {
            return;
        }
        const std::uint64_t scheduledGeneration = ++generation;
        const double delaySeconds = refreshDelaySeconds(appActive, thermalState);
        dispatchPending(self, scheduledGeneration, delaySeconds);
    }
};

TradingRefreshScheduler::TradingRefreshScheduler()
    : impl_(std::make_shared<Impl>()) {}

TradingRefreshScheduler::~TradingRefreshScheduler() = default;

void TradingRefreshScheduler::setCallback(Callback callback) {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    impl_->callback = std::move(callback);
}

void TradingRefreshScheduler::setAppActive(bool active) {
    const auto impl = impl_;
    std::lock_guard<std::mutex> lock(impl->mutex);
    if (impl->appActive == active) {
        return;
    }
    impl->appActive = active;
    impl->rescheduleIfPendingLocked(impl);
}

void TradingRefreshScheduler::setThermalState(NSProcessInfoThermalState thermalState) {
    const auto impl = impl_;
    std::lock_guard<std::mutex> lock(impl->mutex);
    if (impl->thermalState == thermalState) {
        return;
    }
    impl->thermalState = thermalState;
    impl->rescheduleIfPendingLocked(impl);
}

void TradingRefreshScheduler::schedule() {
    const auto impl = impl_;
    std::lock_guard<std::mutex> lock(impl->mutex);
    if (impl->pending) {
        return;
    }
    impl->scheduleLocked(impl);
}

void TradingRefreshScheduler::cancel() {
    const auto impl = impl_;
    std::lock_guard<std::mutex> lock(impl->mutex);
    impl->pending = false;
    ++impl->generation;
    impl->callback = nullptr;
}
