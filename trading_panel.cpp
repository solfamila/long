#include "trading_panel.h"
#include "trading_runtime.h"

namespace {

void renderLatencyValue(const char* label, double valueMs) {
    if (valueMs >= 0.0) {
        if (valueMs >= 1000.0) {
            ImGui::Text("%s: %.3f s", label, valueMs / 1000.0);
        } else {
            ImGui::Text("%s: %.1f ms", label, valueMs);
        }
    } else {
        ImGui::TextDisabled("%s: --", label);
    }
}

void renderTraceViewer(TradingRuntime* runtime, TradingPanelUiState& uiState) {
    auto traceItems = runtime ? runtime->getTradeTraceListItems(150) : captureTradeTraceListItems(150);
    if (uiState.selectedTraceId == 0) {
        uiState.selectedTraceId = runtime ? runtime->getLatestTraceId() : latestTradeTraceId();
    }
    if (uiState.selectedTraceId == 0 && !traceItems.empty()) {
        uiState.selectedTraceId = traceItems.front().traceId;
    }

    ImGui::Text("Trade Trace Viewer:");

    std::string selectedLabel = "No trace selected";
    for (const auto& item : traceItems) {
        if (item.traceId == uiState.selectedTraceId) {
            selectedLabel = item.summary;
            break;
        }
    }

    if (ImGui::BeginCombo("##tracecombo", selectedLabel.c_str())) {
        for (const auto& item : traceItems) {
            const bool isSelected = (item.traceId == uiState.selectedTraceId);
            if (ImGui::Selectable(item.summary.c_str(), isSelected)) {
                uiState.selectedTraceId = item.traceId;
            }
            if (isSelected) {
                ImGui::SetItemDefaultFocus();
            }
        }
        ImGui::EndCombo();
    }

    if (uiState.selectedTraceId == 0) {
        ImGui::TextDisabled("No trade trace yet.");
        return;
    }

    const TradeTraceSnapshot snapshot = runtime ? runtime->getTradeTraceSnapshot(uiState.selectedTraceId)
                                                : captureTradeTraceSnapshot(uiState.selectedTraceId);
    if (!snapshot.found) {
        ImGui::TextDisabled("Selected trace not found.");
        return;
    }

    const TradeTrace& trace = snapshot.trace;

    ImGui::Separator();
    ImGui::Text("Trace %llu", static_cast<unsigned long long>(trace.traceId));
    ImGui::SameLine();
    if (trace.orderId > 0) {
        ImGui::Text(" | Order %lld", static_cast<long long>(trace.orderId));
    } else {
        ImGui::TextDisabled(" | No order ID assigned");
    }
    if (trace.permId > 0) {
        ImGui::SameLine();
        ImGui::Text(" | PermId %lld", trace.permId);
    }

    ImGui::Text("Source: %s", trace.source.empty() ? "<unknown>" : trace.source.c_str());
    ImGui::SameLine();
    ImGui::Text(" | Symbol: %s", trace.symbol.empty() ? "<none>" : trace.symbol.c_str());
    ImGui::SameLine();
    ImGui::Text(" | Side: %s", trace.side.empty() ? "<none>" : trace.side.c_str());

    ImGui::Text("Requested: %d @ %.2f", trace.requestedQty, trace.limitPrice);
    ImGui::SameLine();
    if (trace.closeOnly) {
        ImGui::Text(" | close-only");
    }

    if (!trace.latestError.empty()) {
        ImGui::TextColored(ImVec4(1.0f, 0.45f, 0.45f, 1.0f), "Latest error: %s", trace.latestError.c_str());
    }

    if (!trace.terminalStatus.empty()) {
        const ImVec4 color = (trace.terminalStatus == "Filled")
            ? ImVec4(0.4f, 0.9f, 0.4f, 1.0f)
            : ImVec4(1.0f, 0.7f, 0.3f, 1.0f);
        ImGui::TextColored(color, "Terminal status: %s", trace.terminalStatus.c_str());
    } else if (!trace.latestStatus.empty()) {
        ImGui::Text("Current status: %s", trace.latestStatus.c_str());
    }

    ImGui::Text("Decision snapshot: bid %.2f | ask %.2f | last %.2f | sweep %.2f | buffer %.2f",
                trace.decisionBid, trace.decisionAsk, trace.decisionLast,
                trace.sweepEstimate, trace.priceBuffer);
    if (!trace.bookSummary.empty()) {
        ImGui::TextWrapped("Book: %s", trace.bookSummary.c_str());
    }
    if (!trace.notes.empty()) {
        ImGui::TextWrapped("Notes: %s", trace.notes.c_str());
    }

    const double validationMs = durationMs(trace.validationStartMono, trace.validationEndMono);
    const double localSubmitMs = durationMs(trace.triggerMono, trace.placeCallEndMono);
    const double placeToAckMs = durationMs(trace.placeCallEndMono, trace.firstOpenOrderMono);
    const double placeToFirstStatusMs = durationMs(trace.placeCallEndMono, trace.firstStatusMono);
    const double placeToFirstExecMs = durationMs(trace.placeCallEndMono, trace.firstExecMono);
    const double clickToFirstExecMs = durationMs(trace.triggerMono, trace.firstExecMono);
    const double firstExecToFullFillMs = durationMs(trace.firstExecMono, trace.fullFillMono);
    const double totalToFillMs = durationMs(trace.triggerMono, trace.fullFillMono);

    ImGui::Separator();
    ImGui::Text("Latency breakdown:");
    renderLatencyValue("Validation", validationMs);
    renderLatencyValue("Trigger -> placeOrder return", localSubmitMs);
    renderLatencyValue("placeOrder return -> openOrder", placeToAckMs);
    renderLatencyValue("placeOrder return -> first orderStatus", placeToFirstStatusMs);
    renderLatencyValue("placeOrder return -> first exec", placeToFirstExecMs);
    renderLatencyValue("Trigger -> first exec", clickToFirstExecMs);
    renderLatencyValue("First exec -> full fill", firstExecToFullFillMs);
    renderLatencyValue("Trigger -> full fill", totalToFillMs);

    if (!trace.fills.empty()) {
        ImGui::Separator();
        ImGui::Text("Fill slices:");
        if (ImGui::BeginTable("tracefills", 7, ImGuiTableFlags_Borders | ImGuiTableFlags_RowBg | ImGuiTableFlags_ScrollY,
                              ImVec2(-1, 140))) {
            ImGui::TableSetupColumn("Exec ID", ImGuiTableColumnFlags_WidthStretch);
            ImGui::TableSetupColumn("Shares", ImGuiTableColumnFlags_WidthFixed, 60);
            ImGui::TableSetupColumn("Price", ImGuiTableColumnFlags_WidthFixed, 70);
            ImGui::TableSetupColumn("CumQty", ImGuiTableColumnFlags_WidthFixed, 70);
            ImGui::TableSetupColumn("Exchange", ImGuiTableColumnFlags_WidthFixed, 80);
            ImGui::TableSetupColumn("Liquidity", ImGuiTableColumnFlags_WidthFixed, 70);
            ImGui::TableSetupColumn("Commission", ImGuiTableColumnFlags_WidthFixed, 90);
            ImGui::TableHeadersRow();

            for (const auto& fill : trace.fills) {
                ImGui::TableNextRow();
                ImGui::TableNextColumn();
                ImGui::TextUnformatted(fill.execId.c_str());
                ImGui::TableNextColumn();
                ImGui::Text("%d", fill.shares);
                ImGui::TableNextColumn();
                ImGui::Text("%.2f", fill.price);
                ImGui::TableNextColumn();
                ImGui::Text("%.0f", fill.cumQty);
                ImGui::TableNextColumn();
                ImGui::TextUnformatted(fill.exchange.c_str());
                ImGui::TableNextColumn();
                ImGui::Text("%d", fill.liquidity);
                ImGui::TableNextColumn();
                if (fill.commissionKnown) {
                    ImGui::Text("%.4f %s", fill.commission, fill.commissionCurrency.c_str());
                } else {
                    ImGui::TextDisabled("--");
                }
            }

            ImGui::EndTable();
        }
        if (!trace.commissionCurrency.empty()) {
            ImGui::Text("Total commission: %.4f %s", trace.totalCommission, trace.commissionCurrency.c_str());
        }
    }

    if (!trace.events.empty()) {
        ImGui::Separator();
        ImGui::Text("Timeline:");
        if (ImGui::BeginTable("tracetimeline", 4, ImGuiTableFlags_Borders | ImGuiTableFlags_RowBg | ImGuiTableFlags_ScrollY,
                              ImVec2(-1, 180))) {
            ImGui::TableSetupColumn("Time", ImGuiTableColumnFlags_WidthFixed, 95);
            ImGui::TableSetupColumn("+ms", ImGuiTableColumnFlags_WidthFixed, 75);
            ImGui::TableSetupColumn("Stage", ImGuiTableColumnFlags_WidthFixed, 120);
            ImGui::TableSetupColumn("Details", ImGuiTableColumnFlags_WidthStretch);
            ImGui::TableHeadersRow();

            for (const auto& event : trace.events) {
                ImGui::TableNextRow();
                ImGui::TableNextColumn();
                ImGui::TextUnformatted(formatWallTime(event.wallTs).c_str());
                ImGui::TableNextColumn();
                const double sinceTriggerMs = durationMs(trace.triggerMono, event.monoTs);
                if (sinceTriggerMs >= 0.0) {
                    ImGui::Text("%.1f", sinceTriggerMs);
                } else {
                    ImGui::TextDisabled("--");
                }
                ImGui::TableNextColumn();
                ImGui::TextUnformatted(event.stage.c_str());
                ImGui::TableNextColumn();
                ImGui::TextWrapped("%s", event.details.c_str());
            }

            ImGui::EndTable();
        }
    }
}

} // namespace

void RenderTradingPanel(ImGuiIO& io, TradingRuntime* runtime, ControllerState& dsState, TradingPanelUiState& uiState) {
    auto addPanelMessage = [&](const std::string& msg) {
        if (runtime) {
            runtime->addMessage(msg);
        } else {
            g_data.addMessage(msg);
        }
    };

    auto subscribeFromGui = [&](const std::string& requestedSymbol) {
        const std::string upperSymbol = toUpperCase(requestedSymbol);
        if (upperSymbol.empty()) return;

        if (runtime) {
            runtime->submitSubscribe(upperSymbol, false);
            uiState.symbolInput = upperSymbol;
            uiState.subscribed = true;
            uiState.subscribedSymbol = upperSymbol;
        }
    };

    ImGui::SetNextWindowPos(ImVec2(0, 0));
    ImGui::SetNextWindowSize(io.DisplaySize);

    if (!ImGui::Begin("Trading Panel", nullptr,
                      ImGuiWindowFlags_NoCollapse | ImGuiWindowFlags_NoResize |
                      ImGuiWindowFlags_NoMove | ImGuiWindowFlags_NoTitleBar)) {
        ImGui::End();
        return;
    }

    const UiStatusSnapshot statusSnapshot = runtime ? runtime->getStatusSnapshot() : captureUiStatusSnapshot();

    if (statusSnapshot.connected && statusSnapshot.sessionReady) {
        ImGui::TextColored(ImVec4(0.2f, 0.8f, 0.2f, 1.0f), "TWS: Ready");
    } else if (statusSnapshot.connected) {
        ImGui::TextColored(ImVec4(1.0f, 0.8f, 0.2f, 1.0f), "TWS: Connected (initializing)");
    } else {
        ImGui::TextColored(ImVec4(1.0f, 0.3f, 0.3f, 1.0f), "TWS: Disconnected");
    }
    ImGui::SameLine();
    ImGui::Text("Account: %s", statusSnapshot.accountText.c_str());

    ImGui::SameLine();
    ImGui::Text(" | ");
    ImGui::SameLine();
    if (statusSnapshot.wsServerRunning) {
        ImGui::TextColored(ImVec4(0.2f, 0.8f, 0.2f, 1.0f), "WS: localhost:%d", WEBSOCKET_PORT);
        ImGui::SameLine();
        ImGui::Text("(%d clients)", statusSnapshot.wsConnectedClients);
    } else {
        ImGui::TextColored(ImVec4(1.0f, 0.3f, 0.3f, 1.0f), "WS: Not running");
    }

    ImGui::SameLine();
    ImGui::Text(" | ");
    ImGui::SameLine();
    if (statusSnapshot.controllerConnected) {
        const char* controllerLabel = statusSnapshot.controllerDeviceName.empty()
            ? "Connected"
            : statusSnapshot.controllerDeviceName.c_str();
        ImGui::TextColored(ImVec4(0.2f, 0.8f, 0.2f, 1.0f), "Controller: %s", controllerLabel);
    } else {
        ImGui::TextColored(ImVec4(0.6f, 0.6f, 0.6f, 1.0f), "Controller: Not found");
    }

    ImGui::SameLine();
    ImGui::Text(" | ");
    ImGui::SameLine();
    if (!statusSnapshot.controllerLockedDeviceName.empty()) {
        ImVec4 lockColor = statusSnapshot.controllerConnected
            ? ImVec4(0.5f, 0.8f, 1.0f, 1.0f)
            : ImVec4(1.0f, 0.8f, 0.2f, 1.0f);
        ImGui::TextColored(lockColor, "Controller Lock: %s", statusSnapshot.controllerLockedDeviceName.c_str());
    } else {
        ImGui::TextDisabled("Controller Lock: Waiting to lock");
    }

    ImGui::Separator();

    consumeGuiSyncUpdates(uiState.symbolInput, uiState.subscribedSymbol, uiState.subscribed, uiState.quantityInput);
    const std::uint64_t newestTraceId = runtime ? runtime->getLatestTraceId() : latestTradeTraceId();
    if (uiState.selectedTraceId == 0 && newestTraceId != 0) {
        uiState.selectedTraceId = newestTraceId;
    }

    ImGui::Text("Symbol:");
    ImGui::SameLine();
    ImGui::SetNextItemWidth(100);
    if (ImGui::InputText("##symbol", &uiState.symbolInput, ImGuiInputTextFlags_EnterReturnsTrue)) {
        subscribeFromGui(uiState.symbolInput);
    }
    ImGui::SameLine();
    if (ImGui::Button("Subscribe")) {
        subscribeFromGui(uiState.symbolInput);
    }

    ImGui::Separator();

    const SymbolUiSnapshot symbolSnapshot = runtime ? runtime->getSymbolSnapshot(uiState.subscribed ? uiState.subscribedSymbol : std::string())
                                                    : captureSymbolUiSnapshot(uiState.subscribed ? uiState.subscribedSymbol : std::string());
    const double currentPositionQty = symbolSnapshot.currentPositionQty;
    const double currentPositionAvgCost = symbolSnapshot.currentPositionAvgCost;
    const double availableLongToClose = symbolSnapshot.availableLongToClose;
    const bool hasPosition = symbolSnapshot.hasPosition;
    const double bid = symbolSnapshot.bidPrice;
    const double ask = symbolSnapshot.askPrice;
    const double last = symbolSnapshot.lastPrice;
    const int askLevels = static_cast<int>(symbolSnapshot.askBook.size());
    const int bidLevels = static_cast<int>(symbolSnapshot.bidBook.size());

    if (uiState.subscribed) {
        ImGui::Text("Market Data for: %s", uiState.subscribedSymbol.c_str());
        ImGui::Columns(4, "marketdata", true);
        ImGui::TextColored(ImVec4(0.4f, 0.8f, 0.4f, 1.0f), "Bid: $%.2f", bid);
        ImGui::NextColumn();
        ImGui::TextColored(ImVec4(1.0f, 0.4f, 0.4f, 1.0f), "Ask: $%.2f", ask);
        ImGui::NextColumn();
        ImGui::Text("Last: $%.2f", last);
        ImGui::NextColumn();

        if (hasPosition && currentPositionQty != 0.0) {
            if (currentPositionQty > 0) {
                ImGui::TextColored(ImVec4(0.4f, 0.8f, 0.4f, 1.0f), "Position: %.0f LONG", currentPositionQty);
            } else {
                ImGui::TextColored(ImVec4(1.0f, 0.4f, 0.4f, 1.0f), "Position: %.0f SHORT", -currentPositionQty);
            }

            double currentPrice = last > 0.0 ? last : (currentPositionQty > 0 ? bid : ask);
            if (currentPrice > 0.0 && currentPositionAvgCost > 0.0) {
                double unrealizedPnL = (currentPrice - currentPositionAvgCost) * currentPositionQty;
                ImVec4 pnlColor =
                    unrealizedPnL > 0.0 ? ImVec4(0.4f, 0.8f, 0.4f, 1.0f) :
                    unrealizedPnL < 0.0 ? ImVec4(1.0f, 0.4f, 0.4f, 1.0f) :
                                          ImVec4(0.5f, 0.5f, 0.5f, 1.0f);
                ImGui::TextColored(pnlColor, "P&L: %s$%.2f",
                                   unrealizedPnL >= 0.0 ? "+" : "-", std::abs(unrealizedPnL));
            }
        } else {
            ImGui::TextDisabled("Position: FLAT");
        }
        ImGui::Columns(1);
    } else {
        ImGui::TextDisabled("Enter a symbol and press Subscribe to see market data");
    }

    ImGui::Separator();

    ImGui::Text("Order Entry:");

    ImGui::Text("Quantity:");
    ImGui::SameLine();
    ImGui::SetNextItemWidth(100);
    ImGui::InputInt("##qty", &uiState.quantityInput);
    if (uiState.quantityInput < 1) uiState.quantityInput = 1;

    ImGui::SameLine();
    ImGui::Text("   Buffer:");
    ImGui::SameLine();
    ImGui::SetNextItemWidth(80);
    ImGui::InputDouble("##buffer", &uiState.priceBuffer, 0.01, 0.05, "%.2f");
    if (uiState.priceBuffer < 0.0) uiState.priceBuffer = 0.0;

    ImGui::SameLine();
    ImGui::Text("   Max Position $");
    ImGui::SameLine();
    ImGui::SetNextItemWidth(100);
    ImGui::InputDouble("##maxpos", &uiState.maxPositionDollars, 1000.0, 5000.0, "%.0f");
    if (uiState.maxPositionDollars < 1000.0) uiState.maxPositionDollars = 1000.0;

    syncSharedGuiInputs(uiState.quantityInput, uiState.priceBuffer, uiState.maxPositionDollars);

    double buyPrice = 0.0;
    double sellPrice = 0.0;
    bool buySweepAvailable = false;
    bool sellSweepAvailable = false;

    if (uiState.subscribed) {
        const auto& askBookSnap = symbolSnapshot.askBook;
        const auto& bidBookSnap = symbolSnapshot.bidBook;
        const double askSnap = symbolSnapshot.askPrice;
        const double bidSnap = symbolSnapshot.bidPrice;

        if (!askBookSnap.empty()) {
            double sweep = calculateSweepPrice(askBookSnap, uiState.quantityInput, uiState.priceBuffer, true);
            if (sweep > 0.0) {
                buyPrice = sweep;
                buySweepAvailable = true;
            }
        }
        if (!bidBookSnap.empty()) {
            double sweep = calculateSweepPrice(bidBookSnap, uiState.quantityInput, uiState.priceBuffer, false);
            if (sweep > 0.0) {
                sellPrice = sweep;
                sellSweepAvailable = true;
            }
        }

        if (buyPrice <= 0.0 && askSnap > 0.0) {
            buyPrice = askSnap + uiState.priceBuffer;
        }
        if (sellPrice <= 0.0 && bidSnap > 0.0) {
            sellPrice = std::max(0.01, bidSnap - uiState.priceBuffer);
        }

        if (buyPrice > 0.0 && askSnap > 0.0) {
            buyPrice = std::max(buyPrice, askSnap + uiState.priceBuffer);
        }
        if (sellPrice > 0.0 && bidSnap > 0.0) {
            sellPrice = std::min(sellPrice, std::max(0.01, bidSnap - uiState.priceBuffer));
        }
    }

    ImGui::Text("Prices:");
    ImGui::SameLine();
    if (buySweepAvailable) {
        ImGui::TextColored(ImVec4(0.4f, 1.0f, 0.4f, 1.0f), "Buy @ %.2f (sweep+%.2f)", buyPrice, uiState.priceBuffer);
    } else {
        ImGui::Text("Buy @ %.2f (ask+%.2f)", buyPrice, uiState.priceBuffer);
    }
    ImGui::SameLine();
    ImGui::Text("  |  ");
    ImGui::SameLine();
    if (sellSweepAvailable) {
        ImGui::TextColored(ImVec4(1.0f, 0.6f, 0.4f, 1.0f), "Sell @ %.2f (sweep-%.2f)", sellPrice, uiState.priceBuffer);
    } else {
        ImGui::Text("Sell @ %.2f (bid-%.2f)", sellPrice, uiState.priceBuffer);
    }

    if (askLevels > 0 || bidLevels > 0) {
        ImGui::Text("Book Depth: %d ask levels, %d bid levels", askLevels, bidLevels);
    }

    ImGui::Spacing();

    const bool canTrade = symbolSnapshot.canTrade;
    const bool canBuy = canTrade && uiState.subscribed && uiState.quantityInput > 0 && buyPrice > 0.0;
    const bool canClosePosition = canTrade && uiState.subscribed && availableLongToClose > 0.0 && sellPrice > 0.0;

    if (!canBuy) ImGui::BeginDisabled();
    if (ImGui::Button("Buy Limit", ImVec2(120, 30))) {
        if (runtime) {
            runtime->submitOrder(uiState.subscribedSymbol, "BUY",
                                uiState.quantityInput, buyPrice, false, "GUI Button", std::nullopt);
        }
    }
    if (!canBuy) ImGui::EndDisabled();

    ImGui::SameLine();

    if (!canClosePosition) ImGui::BeginDisabled();
    char closeBtnLabel[64];
    if (availableLongToClose > 0.0) {
        std::snprintf(closeBtnLabel, sizeof(closeBtnLabel), "Close Long (%.0f)", availableLongToClose);
    } else {
        std::snprintf(closeBtnLabel, sizeof(closeBtnLabel), "Close Long (N/A)");
    }

    if (ImGui::Button(closeBtnLabel, ImVec2(140, 30))) {
        if (runtime) {
            runtime->submitOrder(uiState.subscribedSymbol, "SELL",
                                toShareCount(availableLongToClose), sellPrice, true, "GUI Button", std::nullopt);
        }
    }
    if (!canClosePosition) ImGui::EndDisabled();

    controllerPoll(dsState);

    if (controllerIsConnected(dsState)) {
        auto now = std::chrono::steady_clock::now();

        if (controllerConsumeDebouncedPress(dsState, CONTROLLER_BUTTON_SQUARE, now)) {
            if (canBuy) {
                if (runtime) {
                    runtime->submitOrder(uiState.subscribedSymbol, "BUY",
                                        uiState.quantityInput, buyPrice, false, "Controller", std::nullopt);
                }
            }
        }

        if (controllerConsumeDebouncedPress(dsState, CONTROLLER_BUTTON_CIRCLE, now)) {
            if (canClosePosition) {
                if (runtime) {
                    runtime->submitOrder(uiState.subscribedSymbol, "SELL",
                                        toShareCount(availableLongToClose), sellPrice, true, "Controller", std::nullopt);
                }
            }
        }

        if (controllerConsumeDebouncedPress(dsState, CONTROLLER_BUTTON_TRIANGLE, now)) {
            const std::vector<OrderId> pendingOrders = markAllPendingOrdersForCancel();

            if (!pendingOrders.empty()) {
                if (runtime) {
                    for (OrderId orderId : pendingOrders) {
                        runtime->submitCancel(orderId);
                    }
                }
                const int sentCount = static_cast<int>(pendingOrders.size());
                addPanelMessage("[Controller] Cancel requested for " + std::to_string(sentCount) + " order(s)");
            } else {
                addPanelMessage("[Controller] No pending orders to cancel");
            }
        }

        if (controllerConsumeDebouncedPress(dsState, CONTROLLER_BUTTON_CROSS, now)) {
            if (canTrade && uiState.subscribed) {
                const int maxQty = computeMaxQuantityFromAsk(symbolSnapshot.askPrice, uiState.maxPositionDollars);
                uiState.quantityInput = (uiState.quantityInput == 1) ? maxQty : 1;
                syncSharedGuiInputs(uiState.quantityInput, uiState.priceBuffer, uiState.maxPositionDollars);
                addPanelMessage("[Controller] Quantity toggled to " + std::to_string(uiState.quantityInput) + " shares");
            }
        }

        const bool shouldVibrate = hasPosition && currentPositionQty != 0.0;
        controllerSetVibration(dsState, shouldVibrate);
    }

    ImGui::Separator();

    ImGui::Text("Working Orders:");

    const auto ordersSnapshot = runtime ? runtime->getOrdersSnapshot() : captureOrdersSnapshot();

    if (ImGui::BeginTable("orders", 8, ImGuiTableFlags_Borders | ImGuiTableFlags_RowBg)) {
        ImGui::TableSetupColumn("ID", ImGuiTableColumnFlags_WidthFixed, 60);
        ImGui::TableSetupColumn("Symbol", ImGuiTableColumnFlags_WidthFixed, 80);
        ImGui::TableSetupColumn("Side", ImGuiTableColumnFlags_WidthFixed, 50);
        ImGui::TableSetupColumn("Qty", ImGuiTableColumnFlags_WidthFixed, 60);
        ImGui::TableSetupColumn("Price", ImGuiTableColumnFlags_WidthFixed, 80);
        ImGui::TableSetupColumn("Status", ImGuiTableColumnFlags_WidthFixed, 100);
        ImGui::TableSetupColumn("Time", ImGuiTableColumnFlags_WidthFixed, 90);
        ImGui::TableSetupColumn("Action", ImGuiTableColumnFlags_WidthFixed, 120);
        ImGui::TableHeadersRow();

        std::vector<OrderId> ordersToCancel;

        for (const auto& [id, order] : ordersSnapshot) {
            ImGui::TableNextRow();

            ImGui::TableNextColumn();
            ImGui::Text("%lld", static_cast<long long>(id));

            ImGui::TableNextColumn();
            ImGui::Text("%s", order.symbol.c_str());

            ImGui::TableNextColumn();
            if (order.side == "BUY") {
                ImGui::TextColored(ImVec4(0.4f, 0.8f, 0.4f, 1.0f), "%s", order.side.c_str());
            } else {
                ImGui::TextColored(ImVec4(1.0f, 0.4f, 0.4f, 1.0f), "%s", order.side.c_str());
            }

            ImGui::TableNextColumn();
            ImGui::Text("%.0f", order.quantity);

            ImGui::TableNextColumn();
            if (order.avgFillPrice > 0.0) {
                ImGui::TextColored(ImVec4(1.0f, 1.0f, 0.4f, 1.0f), "$%.2f", order.avgFillPrice);
            } else {
                ImGui::Text("$%.2f", order.limitPrice);
            }

            ImGui::TableNextColumn();
            ImGui::Text("%s", order.status.c_str());

            ImGui::TableNextColumn();
            if (order.fillDurationMs >= 0.0) {
                if (order.fillDurationMs >= 1000.0) {
                    ImGui::TextColored(ImVec4(0.4f, 0.8f, 0.4f, 1.0f), "%.2f s", order.fillDurationMs / 1000.0);
                } else {
                    ImGui::TextColored(ImVec4(0.4f, 0.8f, 0.4f, 1.0f), "%.0f ms", order.fillDurationMs);
                }
            } else if (order.submitTime.time_since_epoch().count() > 0 && !order.isTerminal()) {
                auto elapsed = std::chrono::steady_clock::now() - order.submitTime;
                double elapsedMs = std::chrono::duration<double, std::milli>(elapsed).count();
                if (elapsedMs >= 1000.0) {
                    ImGui::TextColored(ImVec4(1.0f, 1.0f, 0.4f, 1.0f), "%.1f s...", elapsedMs / 1000.0);
                } else {
                    ImGui::TextColored(ImVec4(1.0f, 1.0f, 0.4f, 1.0f), "%.0f ms...", elapsedMs);
                }
            } else {
                ImGui::TextDisabled("--");
            }

            ImGui::TableNextColumn();
            const std::uint64_t rowTraceId = findTraceIdByOrderId(id);
            if (rowTraceId != 0) {
                char viewId[32];
                std::snprintf(viewId, sizeof(viewId), "View##%lld", static_cast<long long>(id));
                if (ImGui::SmallButton(viewId)) {
                    uiState.selectedTraceId = rowTraceId;
                }
                if (!order.isTerminal() && !order.cancelPending) {
                    ImGui::SameLine();
                }
            }
            if (!order.isTerminal() && !order.cancelPending) {
                char btnId[32];
                std::snprintf(btnId, sizeof(btnId), "Cancel##%lld", static_cast<long long>(id));
                if (ImGui::SmallButton(btnId)) {
                    ordersToCancel.push_back(id);
                }
            } else if (order.cancelPending) {
                ImGui::TextDisabled("Cancelling...");
            }
        }

        if (!ordersToCancel.empty()) {
            const std::vector<OrderId> markedForCancel = markOrdersPendingCancel(ordersToCancel);

            if (runtime) {
                for (OrderId orderId : markedForCancel) {
                    runtime->submitCancel(orderId);
                    addPanelMessage("Cancel request sent for order " + std::to_string(orderId));
                }
            }
        }

        ImGui::EndTable();
    }

    ImGui::Separator();
    renderTraceViewer(runtime, uiState);

    ImGui::Separator();

    ImGui::Text("Messages (select text to copy):");
    float availableHeight = ImGui::GetContentRegionAvail().y - 10;
    if (availableHeight < 100) availableHeight = 100;
    static std::string messagesText;
    static std::uint64_t messagesVersionSeen = 0;
    if (runtime) {
        runtime->copyMessagesTextIfChanged(messagesText, messagesVersionSeen);
    } else {
        g_data.copyMessagesTextIfChanged(messagesText, messagesVersionSeen);
    }

    ImGui::InputTextMultiline("##messageslog", &messagesText,
                              ImVec2(-1, availableHeight),
                              ImGuiInputTextFlags_ReadOnly);

    ImGui::End();
}

void RenderTradingPanel(ImGuiIO& io, EClientSocket* client, ControllerState& dsState, TradingPanelUiState& uiState) {
    if (!client) return;

    auto subscribeFromGui = [&](const std::string& requestedSymbol) {
        const std::string upperSymbol = toUpperCase(requestedSymbol);
        if (upperSymbol.empty()) return;

        std::string error;
        if (!requestSymbolSubscription(client, upperSymbol, false, &error)) {
            g_data.addMessage("Subscribe failed: " + error);
            return;
        }

        uiState.symbolInput = upperSymbol;
        uiState.subscribed = true;
        uiState.subscribedSymbol = upperSymbol;
    };

    ImGui::SetNextWindowPos(ImVec2(0, 0));
    ImGui::SetNextWindowSize(io.DisplaySize);

    if (!ImGui::Begin("Trading Panel", nullptr,
                      ImGuiWindowFlags_NoCollapse | ImGuiWindowFlags_NoResize |
                      ImGuiWindowFlags_NoMove | ImGuiWindowFlags_NoTitleBar)) {
        ImGui::End();
        return;
    }

    const UiStatusSnapshot statusSnapshot = captureUiStatusSnapshot();

    if (statusSnapshot.connected && statusSnapshot.sessionReady) {
        ImGui::TextColored(ImVec4(0.2f, 0.8f, 0.2f, 1.0f), "TWS: Ready");
    } else if (statusSnapshot.connected) {
        ImGui::TextColored(ImVec4(1.0f, 0.8f, 0.2f, 1.0f), "TWS: Connected (initializing)");
    } else {
        ImGui::TextColored(ImVec4(1.0f, 0.3f, 0.3f, 1.0f), "TWS: Disconnected");
    }
    ImGui::SameLine();
    ImGui::Text("Account: %s", statusSnapshot.accountText.c_str());

    ImGui::SameLine();
    ImGui::Text(" | ");
    ImGui::SameLine();
    if (statusSnapshot.wsServerRunning) {
        ImGui::TextColored(ImVec4(0.2f, 0.8f, 0.2f, 1.0f), "WS: localhost:%d", WEBSOCKET_PORT);
        ImGui::SameLine();
        ImGui::Text("(%d clients)", statusSnapshot.wsConnectedClients);
    } else {
        ImGui::TextColored(ImVec4(1.0f, 0.3f, 0.3f, 1.0f), "WS: Not running");
    }

    ImGui::Separator();

    consumeGuiSyncUpdates(uiState.symbolInput, uiState.subscribedSymbol, uiState.subscribed, uiState.quantityInput);
    const std::uint64_t newestTraceId = latestTradeTraceId();

    bool justSubscribed = false;
    if (ImGui::InputText("Symbol", &uiState.symbolInput, ImGuiInputTextFlags_EnterReturnsTrue)) {
        subscribeFromGui(uiState.symbolInput);
        justSubscribed = true;
    }

    ImGui::SameLine();
    if (ImGui::Button("Subscribe") && !justSubscribed) {
        subscribeFromGui(uiState.symbolInput);
    }

    ImGui::Separator();

    const SymbolUiSnapshot symbolSnapshot = captureSymbolUiSnapshot(uiState.subscribedSymbol);
    bool canTrade = uiState.subscribed && statusSnapshot.connected && statusSnapshot.sessionReady;
    bool canBuy = canTrade && symbolSnapshot.canTrade;
    bool canClosePosition = canTrade && symbolSnapshot.hasPosition && symbolSnapshot.availableLongToClose > 0.0;

    ImGui::Text("Position: %.0f @ $%.2f", symbolSnapshot.currentPositionQty, symbolSnapshot.currentPositionAvgCost);
    if (symbolSnapshot.hasPosition) {
        ImGui::SameLine();
        ImGui::Text(" | ");
        ImGui::SameLine();
        ImGui::Text("Can close: %.0f", symbolSnapshot.availableLongToClose);
    }

    ImGui::Separator();

    if (uiState.subscribed) {
        if (symbolSnapshot.askPrice > 0.0) {
            ImGui::Text("Ask: $%.2f", symbolSnapshot.askPrice);
        }
        if (symbolSnapshot.bidPrice > 0.0) {
            ImGui::SameLine();
            ImGui::Text(" | ");
            ImGui::SameLine();
            ImGui::Text("Bid: $%.2f", symbolSnapshot.bidPrice);
        }
        if (symbolSnapshot.lastPrice > 0.0) {
            ImGui::SameLine();
            ImGui::Text(" | ");
            ImGui::SameLine();
            ImGui::Text("Last: $%.2f", symbolSnapshot.lastPrice);
        }
    }

    ImGui::Separator();

    static char quantityStr[32];
    if (ImGui::InputText("Quantity", quantityStr, sizeof(quantityStr), ImGuiInputTextFlags_EnterReturnsTrue)) {
        int qty = std::atoi(quantityStr);
        if (qty > 0) {
            uiState.quantityInput = qty;
            syncSharedGuiInputs(uiState.quantityInput, uiState.priceBuffer, uiState.maxPositionDollars);
        }
    }

    ImGui::SameLine();
    if (ImGui::Button("Max")) {
        if (symbolSnapshot.askPrice > 0.0) {
            int maxQty = computeMaxQuantityFromAsk(symbolSnapshot.askPrice, uiState.maxPositionDollars);
            uiState.quantityInput = maxQty;
            syncSharedGuiInputs(uiState.quantityInput, uiState.priceBuffer, uiState.maxPositionDollars);
            std::snprintf(quantityStr, sizeof(quantityStr), "%d", maxQty);
        }
    }

    ImGui::Separator();

    const double buyPrice = symbolSnapshot.askPrice > 0.0 ? symbolSnapshot.askPrice + uiState.priceBuffer : 0.0;
    const double sellPrice = symbolSnapshot.bidPrice > 0.0 ? symbolSnapshot.bidPrice - uiState.priceBuffer : 0.0;
    const double availableLongToClose = symbolSnapshot.availableLongToClose;
    const bool buySweepAvailable = !symbolSnapshot.askBook.empty();
    const bool sellSweepAvailable = !symbolSnapshot.bidBook.empty();

    ImGui::Text("Buy @ $%.2f", buyPrice);
    ImGui::SameLine();
    ImGui::Text(" | ");
    ImGui::SameLine();
    ImGui::Text("Close @ $%.2f", sellPrice);

    if (!canBuy) ImGui::BeginDisabled();
    if (ImGui::Button("Buy Limit", ImVec2(120, 30))) {
        std::string error;
        std::uint64_t traceId = 0;
        SubmitIntent intent = captureSubmitIntent("GUI Button", uiState.subscribedSymbol, "BUY",
                                                  uiState.quantityInput, buyPrice, false,
                                                  uiState.priceBuffer,
                                                  buySweepAvailable ? buyPrice : 0.0,
                                                  "Buy Limit button pressed");
        if (!submitLimitOrder(client, uiState.subscribedSymbol, "BUY",
                              static_cast<double>(uiState.quantityInput), buyPrice,
                              false, &intent, &error, nullptr, &traceId)) {
            g_data.addMessage("Buy failed: " + error);
        }
    }
    if (!canBuy) ImGui::EndDisabled();

    ImGui::SameLine();

    char closeBtnLabel[64];
    if (availableLongToClose > 0.0) {
        std::snprintf(closeBtnLabel, sizeof(closeBtnLabel), "Close Long (%.0f)", availableLongToClose);
    } else {
        std::snprintf(closeBtnLabel, sizeof(closeBtnLabel), "Close Long (N/A)");
    }

    if (!canClosePosition) ImGui::BeginDisabled();
    if (ImGui::Button(closeBtnLabel, ImVec2(140, 30))) {
        std::string error;
        std::uint64_t traceId = 0;
        SubmitIntent intent = captureSubmitIntent("GUI Button", uiState.subscribedSymbol, "SELL",
                                                  toShareCount(availableLongToClose), sellPrice, true,
                                                  uiState.priceBuffer,
                                                  sellSweepAvailable ? sellPrice : 0.0,
                                                  "Close Long button pressed");
        if (!submitLimitOrder(client, uiState.subscribedSymbol, "SELL",
                              availableLongToClose, sellPrice,
                              true, &intent, &error, nullptr, &traceId)) {
            g_data.addMessage("Close failed: " + error);
        }
    }
    if (!canClosePosition) ImGui::EndDisabled();

    controllerPoll(dsState);

    if (controllerIsConnected(dsState)) {
        auto now = std::chrono::steady_clock::now();

        if (controllerConsumeDebouncedPress(dsState, CONTROLLER_BUTTON_SQUARE, now)) {
            if (canBuy) {
                std::string error;
                std::uint64_t traceId = 0;
                SubmitIntent intent = captureSubmitIntent("Controller", uiState.subscribedSymbol, "BUY",
                                                          uiState.quantityInput, buyPrice, false,
                                                          uiState.priceBuffer,
                                                          buySweepAvailable ? buyPrice : 0.0,
                                                          "Controller Square button");
                if (!submitLimitOrder(client, uiState.subscribedSymbol, "BUY",
                                      static_cast<double>(uiState.quantityInput), buyPrice,
                                      false, &intent, &error, nullptr, &traceId)) {
                    g_data.addMessage("[Controller] Buy failed: " + error);
                }
            }
        }

        if (controllerConsumeDebouncedPress(dsState, CONTROLLER_BUTTON_CIRCLE, now)) {
            if (canClosePosition) {
                std::string error;
                std::uint64_t traceId = 0;
                SubmitIntent intent = captureSubmitIntent("Controller", uiState.subscribedSymbol, "SELL",
                                                          toShareCount(availableLongToClose), sellPrice, true,
                                                          uiState.priceBuffer,
                                                          sellSweepAvailable ? sellPrice : 0.0,
                                                          "Controller Circle button");
                if (!submitLimitOrder(client, uiState.subscribedSymbol, "SELL",
                                      availableLongToClose, sellPrice,
                                      true, &intent, &error, nullptr, &traceId)) {
                    g_data.addMessage("[Controller] Close failed: " + error);
                }
            }
        }

        if (controllerConsumeDebouncedPress(dsState, CONTROLLER_BUTTON_TRIANGLE, now)) {
            const std::vector<OrderId> pendingOrders = markAllPendingOrdersForCancel();

            if (!pendingOrders.empty()) {
                const std::vector<bool> cancelSent = sendCancelRequests(client, pendingOrders);
                const int sentCount = static_cast<int>(std::count(cancelSent.begin(), cancelSent.end(), true));
                if (sentCount != static_cast<int>(pendingOrders.size())) {
                    g_data.addMessage("[Controller] Some cancel requests could not be sent");
                }
                g_data.addMessage("[Controller] Cancel requested for " + std::to_string(sentCount) + " order(s)");
            } else {
                g_data.addMessage("[Controller] No pending orders to cancel");
            }
        }

        if (controllerConsumeDebouncedPress(dsState, CONTROLLER_BUTTON_CROSS, now)) {
            if (canTrade && uiState.subscribed) {
                const int maxQty = computeMaxQuantityFromAsk(symbolSnapshot.askPrice, uiState.maxPositionDollars);
                uiState.quantityInput = (uiState.quantityInput == 1) ? maxQty : 1;
                syncSharedGuiInputs(uiState.quantityInput, uiState.priceBuffer, uiState.maxPositionDollars);
                g_data.addMessage("[Controller] Quantity toggled to " + std::to_string(uiState.quantityInput) + " shares");
            }
        }
    }

    if (ImGui::CollapsingHeader("Open Orders")) {
        ImGui::BeginTable("orders", 7, ImGuiTableFlags_Borders);
        ImGui::TableSetupColumn("ID");
        ImGui::TableSetupColumn("Symbol");
        ImGui::TableSetupColumn("Side");
        ImGui::TableSetupColumn("Qty");
        ImGui::TableSetupColumn("Filled");
        ImGui::TableSetupColumn("Price");
        ImGui::TableSetupColumn("Status");
        ImGui::TableHeadersRow();

        const std::vector<std::pair<OrderId, OrderInfo>> orders = captureOrdersSnapshot();
        std::vector<OrderId> ordersToCancel;

        for (const auto& [id, info] : orders) {
            ImGui::TableNextRow();
            ImGui::TableSetColumnIndex(0);
            ImGui::Text("%lld", static_cast<long long>(id));
            ImGui::TableSetColumnIndex(1);
            ImGui::Text("%s", info.symbol.c_str());
            ImGui::TableSetColumnIndex(2);
            ImGui::Text("%s", info.side.c_str());
            ImGui::TableSetColumnIndex(3);
            ImGui::Text("%.0f", info.quantity);
            ImGui::TableSetColumnIndex(4);
            ImGui::Text("%.0f", info.filledQty);
            ImGui::TableSetColumnIndex(5);
            ImGui::Text("$%.2f", info.avgFillPrice);
            ImGui::TableSetColumnIndex(6);
            ImGui::Text("%s", info.status.c_str());

            if (!info.isTerminal() && !info.cancelPending) {
                ImGui::SameLine();
                char btnId[64];
                std::snprintf(btnId, sizeof(btnId), "Cancel##%lld", static_cast<long long>(id));
                if (ImGui::SmallButton(btnId)) {
                    ordersToCancel.push_back(id);
                }
            } else if (info.cancelPending) {
                ImGui::TextDisabled("Cancelling...");
            }
        }

        if (!ordersToCancel.empty()) {
            const std::vector<OrderId> markedForCancel = markOrdersPendingCancel(ordersToCancel);
            const std::vector<bool> cancelSent = sendCancelRequests(client, markedForCancel);

            for (size_t i = 0; i < markedForCancel.size(); ++i) {
                const OrderId id = markedForCancel[i];
                if (cancelSent[i]) {
                    g_data.addMessage("Cancel request sent for order " + std::to_string(id));
                } else {
                    g_data.addMessage("Cancel failed (not connected) for order " + std::to_string(id));
                }
            }
        }

        ImGui::EndTable();
    }

    if (ImGui::CollapsingHeader("Trade Traces")) {
        ImGui::BeginTable("traces", 4, ImGuiTableFlags_Borders);
        ImGui::TableSetupColumn("TraceId");
        ImGui::TableSetupColumn("OrderId");
        ImGui::TableSetupColumn("Status");
        ImGui::TableSetupColumn("Summary");
        ImGui::TableHeadersRow();

        const std::vector<TradeTraceListItem> traceItems = captureTradeTraceListItems(50);
        for (const auto& item : traceItems) {
            ImGui::TableNextRow();
            ImGui::TableSetColumnIndex(0);
            if (ImGui::Selectable(std::to_string(item.traceId).c_str(), uiState.selectedTraceId == item.traceId, ImGuiSelectableFlags_SpanAllColumns)) {
                uiState.selectedTraceId = item.traceId;
            }
            ImGui::TableSetColumnIndex(1);
            ImGui::Text("%lld", static_cast<long long>(item.orderId));
            ImGui::TableSetColumnIndex(2);
            if (item.terminal) {
                ImGui::TextColored(item.failed ? ImVec4(1.0f, 0.3f, 0.3f, 1.0f) : ImVec4(0.2f, 0.8f, 0.2f, 1.0f), "%s", item.summary.c_str());
            } else {
                ImGui::Text("%s", item.summary.c_str());
            }
            ImGui::TableSetColumnIndex(3);
            ImGui::Text("%s", item.summary.c_str());
        }
        ImGui::EndTable();
    }

    if (uiState.selectedTraceId != 0) {
        if (ImGui::CollapsingHeader("Selected Trace Details", ImGuiTreeNodeFlags_DefaultOpen)) {
            const TradeTraceSnapshot traceSnap = captureTradeTraceSnapshot(uiState.selectedTraceId);
            if (traceSnap.found) {
                ImGui::Text("Order ID: %lld", static_cast<long long>(traceSnap.trace.orderId));
                ImGui::Text("Symbol: %s %s", traceSnap.trace.side.c_str(), traceSnap.trace.symbol.c_str());
                ImGui::Text("Requested: %d @ $%.2f", traceSnap.trace.requestedQty, traceSnap.trace.limitPrice);
                ImGui::Text("Status: %s", traceSnap.trace.latestStatus.c_str());
                if (!traceSnap.trace.terminalStatus.empty()) {
                    ImGui::Text("Terminal: %s", traceSnap.trace.terminalStatus.c_str());
                }
                if (traceSnap.trace.totalCommission > 0.0) {
                    ImGui::Text("Commission: $%.2f %s", traceSnap.trace.totalCommission, traceSnap.trace.commissionCurrency.c_str());
                }

                if (ImGui::CollapsingHeader("Events")) {
                    ImGui::BeginTable("events", 5, ImGuiTableFlags_Borders);
                    ImGui::TableSetupColumn("Time");
                    ImGui::TableSetupColumn("Type");
                    ImGui::TableSetupColumn("Stage");
                    ImGui::TableSetupColumn("Details");
                    ImGui::TableSetupColumn("Filled");
                    ImGui::TableHeadersRow();

                    for (const auto& evt : traceSnap.trace.events) {
                        ImGui::TableNextRow();
                        ImGui::TableSetColumnIndex(0);
                        ImGui::Text("%s", formatWallTime(evt.wallTs).c_str());
                        ImGui::TableSetColumnIndex(1);
                        ImGui::Text("%s", tradeEventTypeToString(evt.type).c_str());
                        ImGui::TableSetColumnIndex(2);
                        ImGui::Text("%s", evt.stage.c_str());
                        ImGui::TableSetColumnIndex(3);
                        ImGui::Text("%s", evt.details.c_str());
                        ImGui::TableSetColumnIndex(4);
                        if (evt.cumFilled >= 0.0) {
                            ImGui::Text("%.0f", evt.cumFilled);
                        }
                    }
                    ImGui::EndTable();
                }

                if (ImGui::CollapsingHeader("Fills")) {
                    ImGui::BeginTable("fills", 7, ImGuiTableFlags_Borders);
                    ImGui::TableSetupColumn("Time");
                    ImGui::TableSetupColumn("ExecId");
                    ImGui::TableSetupColumn("Shares");
                    ImGui::TableSetupColumn("Price");
                    ImGui::TableSetupColumn("CumQty");
                    ImGui::TableSetupColumn("Liquidity");
                    ImGui::TableSetupColumn("Commission");
                    ImGui::TableHeadersRow();

                    for (const auto& fill : traceSnap.trace.fills) {
                        ImGui::TableNextRow();
                        ImGui::TableSetColumnIndex(0);
                        ImGui::Text("%s", fill.execTimeText.c_str());
                        ImGui::TableSetColumnIndex(1);
                        ImGui::Text("%s", fill.execId.substr(0, 8).c_str());
                        ImGui::TableSetColumnIndex(2);
                        ImGui::Text("%d", fill.shares);
                        ImGui::TableSetColumnIndex(3);
                        ImGui::Text("$%.2f", fill.price);
                        ImGui::TableSetColumnIndex(4);
                        ImGui::Text("%.0f", fill.cumQty);
                        ImGui::TableSetColumnIndex(5);
                        ImGui::Text("%c", fill.liquidity > 0 ? 'A' : (fill.liquidity < 0 ? 'R' : '-'));
                        ImGui::TableSetColumnIndex(6);
                        if (fill.commissionKnown) {
                            ImGui::Text("$%.2f", fill.commission);
                        } else {
                            ImGui::TextDisabled("-");
                        }
                    }
                    ImGui::EndTable();
                }
            }
        }
    }

    if (ImGui::CollapsingHeader("Messages")) {
        std::string messagesText;
        std::uint64_t seenVersion = 0;
        g_data.copyMessagesTextIfChanged(messagesText, seenVersion);
        static ImGuiTextFilter filter;
        filter.Draw("Filter");
        float availableHeight = ImGui::GetContentRegionAvail().y - 24;
        ImGui::InputTextMultiline("##messageslog", &messagesText,
                                  ImVec2(-1, availableHeight),
                                  ImGuiInputTextFlags_ReadOnly);

        ImGui::End();
    }
}
