#pragma once

#include "app_shared.h"
#include "trading_runtime.h"

#include <cstdint>
#include <string>
#include <vector>

struct TradingPanelState {
    UiStatusSnapshot status;
    SymbolUiSnapshot symbol;
    RiskControlsSnapshot risk;
    double buyPrice = 0.0;
    double sellPrice = 0.0;
    double orderNotional = 0.0;
    double projectedOpenNotional = 0.0;
    bool buySweepAvailable = false;
    bool sellSweepAvailable = false;
    bool canTrade = false;
    bool canBuy = false;
    bool canClosePosition = false;
    bool hasCancelableOrders = false;
    int askLevels = 0;
    int bidLevels = 0;
    int maxToggleQuantity = 1;
};

struct TradingSubmitResult {
    bool submitted = false;
    std::string error;
    std::uint64_t traceId = 0;
};

struct TradingCancelResult {
    bool runtimeAvailable = false;
    std::vector<OrderId> orderIds;
    std::vector<bool> sent;
};

struct TradingControllerActionResult {
    bool quantityChanged = false;
    int quantityInput = 1;
    std::uint64_t traceId = 0;
    std::vector<std::string> messages;
};

TradingPanelState buildTradingPanelState(const std::string& subscribedSymbol,
                                         bool subscribed,
                                         int quantityInput,
                                         double priceBuffer,
                                         double maxPositionDollars);

bool requestSubscriptionAction(TradingRuntime* runtime,
                               const std::string& rawSymbol,
                               bool recalcQtyFromFirstAsk,
                               std::string* normalizedSymbol,
                               std::string* error);

TradingSubmitResult submitBuyAction(TradingRuntime* runtime,
                                    const TradingPanelState& state,
                                    const std::string& subscribedSymbol,
                                    int quantityInput,
                                    double priceBuffer,
                                    const std::string& source,
                                    const std::string& note);

TradingSubmitResult submitCloseAction(TradingRuntime* runtime,
                                      const TradingPanelState& state,
                                      const std::string& subscribedSymbol,
                                      double priceBuffer,
                                      const std::string& source,
                                      const std::string& note);

TradingCancelResult cancelSelectedOrdersAction(TradingRuntime* runtime,
                                               const std::vector<OrderId>& selectedOrderIds);

TradingCancelResult cancelAllOrdersAction(TradingRuntime* runtime);

TradingControllerActionResult handleControllerActionIntent(TradingRuntime* runtime,
                                                           TradingRuntimeControllerAction action,
                                                           const TradingPanelState& state,
                                                           const std::string& subscribedSymbol,
                                                           int quantityInput,
                                                           double priceBuffer,
                                                           double maxPositionDollars);
