#import "trading_refresh_scheduler.h"

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
                callbackCopy();
            }
        };

        if (delaySeconds <= 0.0) {
            dispatch_async(dispatch_get_main_queue(), block);
            return;
        }

        const auto nanos = static_cast<int64_t>(std::llround(delaySeconds * static_cast<double>(NSEC_PER_SEC)));
        dispatch_after(dispatch_time(DISPATCH_TIME_NOW, nanos), dispatch_get_main_queue(), block);
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
