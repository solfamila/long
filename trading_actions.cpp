#include "trading_actions.h"

#include <algorithm>

namespace {

TradingPanelState buildTradingPanelStateFromSnapshot(const RuntimePresentationSnapshot& snapshot,
                                                     bool subscribed,
                                                     int quantityInput,
                                                     double priceBuffer,
                                                     double maxPositionDollars) {
    TradingPanelState state;
    state.status = snapshot.status;
    state.symbol = snapshot.symbol;
    state.risk = snapshot.risk;
    state.canTrade = state.symbol.canTrade;
    state.askLevels = static_cast<int>(state.symbol.askBook.size());
    state.bidLevels = static_cast<int>(state.symbol.bidBook.size());
    state.maxToggleQuantity = computeMaxQuantityFromAsk(state.symbol.askPrice, maxPositionDollars);

    if (subscribed) {
        if (!state.symbol.askBook.empty()) {
            const double sweep = calculateSweepPrice(state.symbol.askBook, quantityInput, priceBuffer, true);
            if (sweep > 0.0) {
                state.buyPrice = sweep;
                state.buySweepAvailable = true;
            }
        }

        if (!state.symbol.bidBook.empty()) {
            const double sweep = calculateSweepPrice(state.symbol.bidBook, quantityInput, priceBuffer, false);
            if (sweep > 0.0) {
                state.sellPrice = sweep;
                state.sellSweepAvailable = true;
            }
        }

        if (state.buyPrice <= 0.0 && state.symbol.askPrice > 0.0) {
            state.buyPrice = state.symbol.askPrice + priceBuffer;
        }
        if (state.sellPrice <= 0.0 && state.symbol.bidPrice > 0.0) {
            state.sellPrice = std::max(0.01, state.symbol.bidPrice - priceBuffer);
        }

        if (state.buyPrice > 0.0 && state.symbol.askPrice > 0.0) {
            state.buyPrice = std::max(state.buyPrice, state.symbol.askPrice + priceBuffer);
        }
        if (state.sellPrice > 0.0 && state.symbol.bidPrice > 0.0) {
            state.sellPrice = std::min(state.sellPrice, std::max(0.01, state.symbol.bidPrice - priceBuffer));
        }
    }

    state.canBuy = state.canTrade && subscribed && quantityInput > 0 && state.buyPrice > 0.0;
    state.canClosePosition = state.canTrade && subscribed &&
                             state.symbol.availableLongToClose > 0.0 &&
                             state.sellPrice > 0.0;
    state.orderNotional = static_cast<double>(quantityInput) * state.buyPrice;
    const double currentPositionBasis = std::max(state.symbol.lastPrice,
        std::max(state.symbol.askPrice, std::max(state.symbol.bidPrice, state.symbol.currentPositionAvgCost)));
    state.projectedOpenNotional = state.symbol.openBuyExposure +
                                  std::max(0.0, state.symbol.currentPositionQty) * currentPositionBasis +
                                  state.orderNotional;

    if (state.risk.tradingKillSwitch || !state.symbol.hasFreshQuote) {
        state.canBuy = false;
        state.canClosePosition = false;
    }
    if (state.risk.maxOrderNotional > 0.0 && state.orderNotional > state.risk.maxOrderNotional) {
        state.canBuy = false;
    }
    if (state.risk.maxOpenNotional > 0.0 && state.projectedOpenNotional > state.risk.maxOpenNotional) {
        state.canBuy = false;
    }

    for (const auto& [id, order] : snapshot.orders) {
        (void)id;
        if (!order.isTerminal() && !order.cancelPending) {
            state.hasCancelableOrders = true;
            break;
        }
    }

    return state;
}

} // namespace

TradingPanelState buildTradingPanelState(const std::string& subscribedSymbol,
                                         bool subscribed,
                                         int quantityInput,
                                         double priceBuffer,
                                         double maxPositionDollars) {
    const RuntimePresentationSnapshot snapshot =
        captureRuntimePresentationSnapshot(subscribed ? subscribedSymbol : std::string(), 0);
    return buildTradingPanelStateFromSnapshot(snapshot, subscribed, quantityInput, priceBuffer, maxPositionDollars);
}

TradingPanelState buildTradingPanelState(const RuntimePresentationSnapshot& snapshot,
                                         bool subscribed,
                                         int quantityInput,
                                         double priceBuffer,
                                         double maxPositionDollars) {
    return buildTradingPanelStateFromSnapshot(snapshot, subscribed, quantityInput, priceBuffer, maxPositionDollars);
}

bool requestSubscriptionAction(TradingRuntime* runtime,
                               const std::string& rawSymbol,
                               bool recalcQtyFromFirstAsk,
                               std::string* normalizedSymbol,
                               std::string* error) {
    if (normalizedSymbol != nullptr) {
        *normalizedSymbol = toUpperCase(rawSymbol);
    }
    if (runtime == nullptr) {
        if (error != nullptr) {
            *error = "Trading runtime is not started";
        }
        return false;
    }
    const std::string symbol = toUpperCase(rawSymbol);
    if (normalizedSymbol != nullptr) {
        *normalizedSymbol = symbol;
    }
    if (symbol.empty()) {
        if (error != nullptr) {
            *error = "Symbol cannot be empty";
        }
        return false;
    }
    return runtime->requestSymbolSubscription(symbol, recalcQtyFromFirstAsk, error);
}

TradingSubmitResult submitBuyAction(TradingRuntime* runtime,
                                    const TradingPanelState& state,
                                    const std::string& subscribedSymbol,
                                    int quantityInput,
                                    double priceBuffer,
                                    const std::string& source,
                                    const std::string& note) {
    TradingSubmitResult result;
    if (runtime == nullptr) {
        result.error = "Trading runtime is not started";
        return result;
    }
    if (!state.canBuy) {
        result.error = "Buy action is not currently available";
        return result;
    }

    SubmitIntent intent = captureSubmitIntent(source,
                                              subscribedSymbol,
                                              "BUY",
                                              quantityInput,
                                              state.buyPrice,
                                              false,
                                              priceBuffer,
                                              state.buySweepAvailable ? state.buyPrice : 0.0,
                                              note);
    result.submitted = runtime->submitOrderIntent(intent,
                                                  static_cast<double>(quantityInput),
                                                  state.buyPrice,
                                                  false,
                                                  &result.error,
                                                  &result.traceId);
    return result;
}

TradingSubmitResult submitCloseAction(TradingRuntime* runtime,
                                      const TradingPanelState& state,
                                      const std::string& subscribedSymbol,
                                      double priceBuffer,
                                      const std::string& source,
                                      const std::string& note) {
    TradingSubmitResult result;
    if (runtime == nullptr) {
        result.error = "Trading runtime is not started";
        return result;
    }
    if (!state.canClosePosition) {
        result.error = "Close action is not currently available";
        return result;
    }

    SubmitIntent intent = captureSubmitIntent(source,
                                              subscribedSymbol,
                                              "SELL",
                                              toShareCount(state.symbol.availableLongToClose),
                                              state.sellPrice,
                                              true,
                                              priceBuffer,
                                              state.sellSweepAvailable ? state.sellPrice : 0.0,
                                              note);
    result.submitted = runtime->submitOrderIntent(intent,
                                                  state.symbol.availableLongToClose,
                                                  state.sellPrice,
                                                  true,
                                                  &result.error,
                                                  &result.traceId);
    return result;
}

TradingCancelResult cancelSelectedOrdersAction(TradingRuntime* runtime,
                                               const std::vector<OrderId>& selectedOrderIds) {
    TradingCancelResult result;
    result.runtimeAvailable = (runtime != nullptr);
    if (runtime == nullptr) {
        return result;
    }
    result.orderIds = runtime->markOrdersPendingCancel(selectedOrderIds);
    result.sent = runtime->requestCancelOrders(result.orderIds);
    return result;
}

TradingCancelResult cancelAllOrdersAction(TradingRuntime* runtime) {
    TradingCancelResult result;
    result.runtimeAvailable = (runtime != nullptr);
    if (runtime == nullptr) {
        return result;
    }
    result.orderIds = runtime->markAllPendingOrdersForCancel();
    result.sent = runtime->requestCancelOrders(result.orderIds);
    return result;
}

TradingControllerActionResult handleControllerActionIntent(TradingRuntime* runtime,
                                                           TradingRuntimeControllerAction action,
                                                           const TradingPanelState& state,
                                                           const std::string& subscribedSymbol,
                                                           int quantityInput,
                                                           double priceBuffer,
                                                           double maxPositionDollars) {
    TradingControllerActionResult result;
    result.quantityInput = quantityInput;

    if (!state.risk.controllerArmed && action != TradingRuntimeControllerAction::ToggleQuantity) {
        result.messages.push_back("[Controller] Ignored input because controller trading is not armed");
        return result;
    }

    switch (action) {
        case TradingRuntimeControllerAction::Buy: {
            const TradingSubmitResult submit = submitBuyAction(runtime,
                                                               state,
                                                               subscribedSymbol,
                                                               quantityInput,
                                                               priceBuffer,
                                                               "Controller",
                                                               "Controller Square button");
            result.traceId = submit.traceId;
            if (!submit.submitted && !submit.error.empty()) {
                result.messages.push_back("[Controller] Buy failed: " + submit.error);
            }
            break;
        }
        case TradingRuntimeControllerAction::Close: {
            const TradingSubmitResult submit = submitCloseAction(runtime,
                                                                 state,
                                                                 subscribedSymbol,
                                                                 priceBuffer,
                                                                 "Controller",
                                                                 "Controller Circle button");
            result.traceId = submit.traceId;
            if (!submit.submitted && !submit.error.empty()) {
                result.messages.push_back("[Controller] Close failed: " + submit.error);
            }
            break;
        }
        case TradingRuntimeControllerAction::CancelAll: {
            const TradingCancelResult cancel = cancelAllOrdersAction(runtime);
            if (!cancel.orderIds.empty()) {
                const int sentCount = static_cast<int>(std::count(cancel.sent.begin(), cancel.sent.end(), true));
                if (sentCount != static_cast<int>(cancel.orderIds.size())) {
                    result.messages.push_back("[Controller] Some cancel requests could not be sent");
                }
                result.messages.push_back("[Controller] Cancel requested for " + std::to_string(sentCount) + " order(s)");
                if (state.risk.controllerArmMode == ControllerArmMode::OneShot) {
                    if (runtime != nullptr) {
                        runtime->setControllerArmed(false);
                    } else {
                        setControllerArmed(false);
                    }
                }
            } else {
                result.messages.push_back("[Controller] No pending orders to cancel");
            }
            break;
        }
        case TradingRuntimeControllerAction::ToggleQuantity:
            if (state.canTrade && !subscribedSymbol.empty()) {
                result.quantityInput = (quantityInput == 1) ? std::max(1, state.maxToggleQuantity) : 1;
                result.quantityChanged = (result.quantityInput != quantityInput);
                if (result.quantityChanged) {
                    if (runtime != nullptr) {
                        runtime->syncGuiInputs(result.quantityInput, priceBuffer, maxPositionDollars);
                    } else {
                        syncSharedGuiInputs(result.quantityInput, priceBuffer, maxPositionDollars);
                    }
                    result.messages.push_back("[Controller] Quantity toggled to " +
                                              std::to_string(result.quantityInput) + " shares");
                }
            }
            break;
    }

    return result;
}
