#include "trading_runtime_host.h"

#include <utility>

bool TradingRuntimeHost::start(TradingRuntime::UiInvalidationCallback uiInvalidationCallback,
                               TradingRuntime::ControllerActionCallback controllerActionCallback) {
    if (runtime_) {
        return runtime_->isStarted();
    }

    runtime_ = std::make_unique<TradingRuntime>();
    runtime_->setUiInvalidationCallback(std::move(uiInvalidationCallback));
    runtime_->setControllerActionCallback(std::move(controllerActionCallback));
    return runtime_->start();
}

void TradingRuntimeHost::shutdown() {
    if (!runtime_) {
        return;
    }
    runtime_->shutdown();
    runtime_.reset();
}

bool TradingRuntimeHost::isStarted() const {
    return runtime_ && runtime_->isStarted();
}

TradingRuntime* TradingRuntimeHost::runtime() {
    return runtime_.get();
}

const TradingRuntime* TradingRuntimeHost::runtime() const {
    return runtime_.get();
}

void TradingRuntimeHost::setControllerVibration(bool enable) {
    if (runtime_) {
        runtime_->setControllerVibration(enable);
    }
}

BridgeOutboxSnapshot TradingRuntimeHost::captureBridgeOutboxSnapshot(std::size_t maxItems) const {
    if (!runtime_) {
        return {};
    }
    return runtime_->captureBridgeOutboxSnapshot(maxItems);
}
