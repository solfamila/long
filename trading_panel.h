#pragma once

#include "app_shared.h"
#include "controller.h"

struct TradingPanelUiState {
    std::string symbolInput = DEFAULT_SYMBOL;
    int quantityInput = 1;
    double priceBuffer = 0.01;
    double maxPositionDollars = 40000.0;
    bool subscribed = false;
    std::string subscribedSymbol;
    std::uint64_t selectedTraceId = 0;
};

void RenderTradingPanel(ImGuiIO& io, EClientSocket* client, ControllerState& dsState, TradingPanelUiState& uiState);
