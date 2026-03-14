#pragma once

#include "trading_runtime.h"

#include <cstddef>
#include <memory>

class TradingRuntimeHost {
public:
    bool start(TradingRuntime::UiInvalidationCallback uiInvalidationCallback,
               TradingRuntime::ControllerActionCallback controllerActionCallback);
    void shutdown();

    bool isStarted() const;
    TradingRuntime* runtime();
    const TradingRuntime* runtime() const;
    void setControllerVibration(bool enable);
    BridgeOutboxSnapshot captureBridgeOutboxSnapshot(std::size_t maxItems = 100) const;

private:
    std::unique_ptr<TradingRuntime> runtime_;
};
