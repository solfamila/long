#pragma once

#include "app_shared.h"

#include <functional>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

enum class TradingRuntimeControllerAction {
    Buy,
    ToggleQuantity,
    Close,
    CancelAll,
};

class TradingRuntime {
public:
    using UiInvalidationCallback = std::function<void()>;
    using ControllerActionCallback = std::function<void(TradingRuntimeControllerAction)>;

    TradingRuntime();
    ~TradingRuntime();

    TradingRuntime(const TradingRuntime&) = delete;
    TradingRuntime& operator=(const TradingRuntime&) = delete;

    void setUiInvalidationCallback(UiInvalidationCallback callback);
    void setControllerActionCallback(ControllerActionCallback callback);

    bool start();
    void shutdown();

    bool isStarted() const;
    void setControllerVibration(bool enable);
    bool requestSymbolSubscription(const std::string& symbol, bool recalcQtyFromFirstAsk, std::string* error = nullptr);
    bool submitOrderIntent(const SubmitIntent& intent,
                           double quantity,
                           double limitPrice,
                           bool closeOnly,
                           std::string* error = nullptr,
                           std::uint64_t* outTraceId = nullptr);
    std::vector<bool> requestCancelOrders(const std::vector<OrderId>& orderIds);

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};
