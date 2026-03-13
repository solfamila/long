#pragma once

#include "trading_actions.h"

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

struct TradingViewModelInput {
    std::string symbolInput;
    std::string subscribedSymbol;
    bool subscribed = false;
    int quantityInput = 1;
    double priceBuffer = 0.01;
    double maxPositionDollars = 40000.0;
    std::uint64_t selectedTraceId = 0;
    std::uint64_t messagesVersionSeen = 0;
    std::string messagesText;
};

struct TradingViewModel {
    TradingPanelState panel;
    std::string symbolInput;
    std::string subscribedSymbol;
    bool subscribed = false;
    int quantityInput = 1;
    std::uint64_t selectedTraceId = 0;
    std::uint64_t messagesVersionSeen = 0;
    std::string messagesText;
    std::vector<std::pair<OrderId, OrderInfo>> orders;
    std::vector<TradeTraceListItem> traceItems;
    bool traceItemsFromReplayLog = false;
    std::string traceDetailsText;
    bool canExportSelectedTrace = false;
    bool canExportAllTraces = false;
    bool shouldVibrate = false;
};

TradingViewModel buildTradingViewModel(const TradingViewModelInput& input);
