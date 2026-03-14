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
    void syncGuiInputs(int quantityInput, double priceBuffer, double maxPositionDollars);
    RuntimePresentationSnapshot capturePresentationSnapshot(const std::string& subscribedSymbol,
                                                            std::size_t maxTraceItems = 150) const;
    PendingUiSyncUpdate consumePendingUiSyncUpdate();
    RuntimeConnectionConfig captureConnectionConfig() const;
    void updateConnectionConfig(const RuntimeConnectionConfig& config);
    RiskControlsSnapshot captureRiskControls() const;
    void updateRiskControls(const RiskControlsSnapshot& risk);
    void setControllerArmed(bool armed);
    void setTradingKillSwitch(bool enabled);
    std::string ensureWebSocketAuthToken();
    TradeTraceSnapshot captureTradeTraceSnapshot(std::uint64_t traceId) const;
    std::uint64_t findTradeTraceIdByOrderId(OrderId orderId) const;
    void appendMessage(const std::string& message);
    std::vector<OrderId> markOrdersPendingCancel(const std::vector<OrderId>& orderIds);
    std::vector<OrderId> markAllPendingOrdersForCancel();
    bool requestWebSocketSubscription(const std::string& symbol,
                                      std::string* error = nullptr,
                                      bool* duplicateIgnored = nullptr,
                                      bool* alreadySubscribed = nullptr);
    bool requestSymbolSubscription(const std::string& symbol, bool recalcQtyFromFirstAsk, std::string* error = nullptr);
    bool submitOrderIntent(const SubmitIntent& intent,
                           double quantity,
                           double limitPrice,
                           bool closeOnly,
                           std::string* error = nullptr,
                           std::uint64_t* outTraceId = nullptr,
                           OrderId* outOrderId = nullptr);
    std::vector<bool> requestCancelOrders(const std::vector<OrderId>& orderIds);

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};
