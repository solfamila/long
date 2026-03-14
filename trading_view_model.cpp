#include "trading_view_model.h"

#include "trace_exporter.h"
#include "trading_ui_format.h"

#include <algorithm>

TradingViewModel buildTradingViewModel(const TradingViewModelInput& input) {
    TradingViewModel model;
    model.symbolInput = input.symbolInput;
    model.subscribedSymbol = input.subscribedSymbol;
    model.subscribed = input.subscribed;
    model.quantityInput = input.quantityInput;
    model.selectedTraceId = input.selectedTraceId;
    model.messagesVersionSeen = input.presentation.messagesVersion;
    model.messagesText = input.presentation.messagesText;

    if (input.pendingUiSync.hasPendingSubscribe) {
        model.symbolInput = input.pendingUiSync.pendingSubscribeSymbol;
        model.subscribedSymbol = input.pendingUiSync.pendingSubscribeSymbol;
        model.subscribed = true;
        model.quantityInput = input.pendingUiSync.quantityInput;
    }
    if (input.pendingUiSync.quantityUpdated) {
        model.quantityInput = input.pendingUiSync.quantityInput;
    }

    if (model.selectedTraceId == 0) {
        model.selectedTraceId = input.presentation.latestTraceId;
    }

    model.panel = buildTradingPanelState(input.presentation,
                                         model.subscribed,
                                         model.quantityInput,
                                         input.priceBuffer,
                                         input.maxPositionDollars);
    model.orders = input.presentation.orders;
    model.traceItems = input.presentation.traceItems;
    if (model.traceItems.empty()) {
        model.traceItems = buildTradeTraceListItemsFromLog(150);
        model.traceItemsFromReplayLog = !model.traceItems.empty();
    }

    if (!model.traceItems.empty()) {
        const auto selectedIt = std::find_if(model.traceItems.begin(), model.traceItems.end(),
                                             [&](const TradeTraceListItem& item) {
                                                 return item.traceId == model.selectedTraceId;
                                             });
        if (selectedIt == model.traceItems.end()) {
            model.selectedTraceId = model.traceItems.front().traceId;
        }
    } else {
        model.selectedTraceId = 0;
    }

    if (model.selectedTraceId == 0) {
        model.traceDetailsText = "No trade trace yet.";
    } else {
        TradeTraceSnapshot snapshot = captureTradeTraceSnapshot(model.selectedTraceId);
        if (!snapshot.found) {
            std::string replayError;
            replayTradeTraceSnapshotFromLog(model.selectedTraceId, &snapshot, &replayError);
            if (!snapshot.found && model.traceItemsFromReplayLog) {
                model.traceDetailsText = replayError.empty() ? "Replay trace not available." : replayError;
            }
        }
        if (model.traceDetailsText.empty()) {
            model.traceDetailsText = formatTradeTraceDetailsText(snapshot);
        }
    }

    model.canExportSelectedTrace = (model.selectedTraceId != 0 && !model.traceItems.empty());
    model.canExportAllTraces = !model.traceItems.empty();
    model.shouldVibrate = model.panel.symbol.hasPosition && model.panel.symbol.currentPositionQty != 0.0;
    return model;
}
