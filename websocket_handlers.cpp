#include "websocket_handlers.h"

void handleWebSocketOrder(const std::string& jsonMessage, ix::WebSocket& webSocket, EClientSocket* client) {
    try {
        json orderRequest = json::parse(jsonMessage);

        if (!orderRequest.contains("action") || !orderRequest["action"].is_string()) {
            webSocket.send(json({{"success", false}, {"error", "Missing required string field: action"}}).dump());
            return;
        }

        const std::string action = orderRequest["action"].get<std::string>();
        if (action != "BUY" && action != "SELL") {
            webSocket.send(json({{"success", false}, {"error", "Invalid action. Must be BUY or SELL"}}).dump());
            return;
        }

        const bool allowOpenShort = orderRequest.value("allowOpenShort", false);

        std::string symbol;
        if (orderRequest.contains("symbol")) {
            symbol = toUpperCase(orderRequest["symbol"].get<std::string>());
        } else {
            std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
            symbol = g_data.currentSymbol;
        }

        if (symbol.empty()) {
            webSocket.send(json({{"success", false}, {"error", "No symbol specified and no active symbol subscribed"}}).dump());
            return;
        }

        double quantity = 0.0;
        if (orderRequest.contains("quantity")) {
            quantity = static_cast<double>(orderRequest["quantity"].get<int>());
        } else {
            std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
            if (action == "SELL" && !allowOpenShort) {
                quantity = availableLongToCloseUnlocked(g_data.selectedAccount, symbol);
            } else {
                quantity = static_cast<double>(g_data.currentQuantity);
            }
        }

        if (quantity <= 0.0) {
            webSocket.send(json({{"success", false}, {"error", "Quantity must be positive"}}).dump());
            return;
        }

        int qtyShares = toShareCount(quantity);
        if (qtyShares <= 0) {
            webSocket.send(json({{"success", false}, {"error", "Quantity rounded to zero"}}).dump());
            return;
        }

        double limitPrice = 0.0;
        double buffer = 0.0;
        double sweepEstimate = 0.0;
        bool derivedLimitPrice = false;
        bool explicitLimitPrice = false;

        if (orderRequest.contains("limitPrice")) {
            limitPrice = orderRequest["limitPrice"].get<double>();
            explicitLimitPrice = true;
        } else {
            std::string currentSymbolSnap;
            double askPrice = 0.0;
            double bidPrice = 0.0;
            std::vector<BookLevel> askBookSnap;
            std::vector<BookLevel> bidBookSnap;

            {
                std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
                currentSymbolSnap = g_data.currentSymbol;
                buffer = g_data.priceBuffer;
                askPrice = g_data.askPrice;
                bidPrice = g_data.bidPrice;
                if (currentSymbolSnap == symbol) {
                    askBookSnap = g_data.askBook;
                    bidBookSnap = g_data.bidBook;
                }
            }

            if (currentSymbolSnap != symbol) {
                webSocket.send(json({{"success", false}, {"error", "No market data for requested symbol; provide limitPrice explicitly"}}).dump());
                return;
            }

            if (action == "BUY") {
                if (!askBookSnap.empty()) {
                    sweepEstimate = calculateSweepPrice(askBookSnap, qtyShares, buffer, true);
                    limitPrice = sweepEstimate;
                }
                if (limitPrice <= 0.0 && askPrice > 0.0) {
                    limitPrice = askPrice + buffer;
                }
                if (limitPrice > 0.0 && askPrice > 0.0) {
                    limitPrice = std::max(limitPrice, askPrice + buffer);
                }
            } else {
                if (!bidBookSnap.empty()) {
                    sweepEstimate = calculateSweepPrice(bidBookSnap, qtyShares, buffer, false);
                    limitPrice = sweepEstimate;
                }
                if (limitPrice <= 0.0 && bidPrice > 0.0) {
                    limitPrice = std::max(0.01, bidPrice - buffer);
                }
                if (limitPrice > 0.0 && bidPrice > 0.0) {
                    limitPrice = std::min(limitPrice, std::max(0.01, bidPrice - buffer));
                }
            }
            derivedLimitPrice = true;
        }

        if (limitPrice <= 0.0) {
            webSocket.send(json({{"success", false}, {"error", "Invalid or missing limit price"}}).dump());
            return;
        }

        const bool closeOnly = (action == "SELL" && !allowOpenShort);
        const std::string notes = explicitLimitPrice
            ? "WebSocket order with explicit limitPrice"
            : (derivedLimitPrice ? "WebSocket order using derived market-based limitPrice" : "WebSocket order");
        SubmitIntent intent = captureSubmitIntent("WebSocket", symbol, action, qtyShares, limitPrice,
                                                  closeOnly, buffer, sweepEstimate, notes);

        std::string submitError;
        OrderId orderId = 0;
        std::uint64_t traceId = 0;
        if (!submitLimitOrder(client, symbol, action, static_cast<double>(qtyShares), limitPrice,
                              closeOnly, &intent, &submitError, &orderId, &traceId)) {
            webSocket.send(json({{"success", false}, {"error", submitError}, {"traceId", static_cast<unsigned long long>(traceId)}}).dump());
            return;
        }

        webSocket.send(json({
            {"success", true},
            {"orderId", static_cast<long long>(orderId)},
            {"traceId", static_cast<unsigned long long>(traceId)},
            {"symbol", symbol},
            {"action", action},
            {"quantity", qtyShares},
            {"limitPrice", limitPrice},
            {"closeOnly", closeOnly}
        }).dump());

    } catch (const json::exception& e) {
        webSocket.send(json({{"success", false}, {"error", std::string("JSON parse error: ") + e.what()}}).dump());
    } catch (const std::exception& e) {
        webSocket.send(json({{"success", false}, {"error", std::string("Error: ") + e.what()}}).dump());
    }
}

void handleWebSocketSubscribe(const std::string& jsonMessage, ix::WebSocket& webSocket, EClientSocket* client) {
    try {
        json request = json::parse(jsonMessage);

        if (!request.contains("subscribe") || !request["subscribe"].is_string()) {
            webSocket.send(json({{"success", false}, {"error", "Missing required string field: subscribe"}}).dump());
            return;
        }

        const std::string symbol = toUpperCase(request["subscribe"].get<std::string>());
        if (symbol.empty()) {
            webSocket.send(json({{"success", false}, {"error", "Symbol cannot be empty"}}).dump());
            return;
        }

        const auto now = std::chrono::steady_clock::now();
        {
            std::lock_guard<std::recursive_mutex> lock(g_data.mutex);

            if (!(g_data.connected && g_data.sessionReady)) {
                webSocket.send(json({{"success", false}, {"error", "TWS session not ready"}}).dump());
                return;
            }

            if (g_data.lastWsRequestedSymbol == symbol &&
                (now - g_data.lastWsSubscribeRequest) < std::chrono::milliseconds(300)) {
                webSocket.send(json({{"success", true}, {"subscribed", symbol}, {"duplicateIgnored", true}}).dump());
                return;
            }

            g_data.lastWsRequestedSymbol = symbol;
            g_data.lastWsSubscribeRequest = now;

            if (g_data.currentSymbol == symbol &&
                g_data.activeMktDataReqId != 0 &&
                g_data.activeDepthReqId != 0) {
                webSocket.send(json({{"success", true}, {"subscribed", symbol}, {"alreadySubscribed", true}}).dump());
                return;
            }
        }

        std::string error;
        if (!requestSymbolSubscription(client, symbol, true, &error)) {
            webSocket.send(json({{"success", false}, {"error", error}}).dump());
            return;
        }

        webSocket.send(json({{"success", true}, {"subscribed", symbol}, {"requestSent", true}}).dump());

    } catch (const json::exception& e) {
        webSocket.send(json({{"success", false}, {"error", std::string("JSON parse error: ") + e.what()}}).dump());
    } catch (const std::exception& e) {
        webSocket.send(json({{"success", false}, {"error", std::string("Error: ") + e.what()}}).dump());
    }
}

void handleWebSocketMessage(const std::string& jsonMessage, ix::WebSocket& webSocket, EClientSocket* client) {
    if (jsonMessage.size() > 4096) {
        webSocket.send(json({{"success", false}, {"error", "Message too large"}}).dump());
        return;
    }

    try {
        json request = json::parse(jsonMessage);

        if (request.contains("subscribe")) {
            handleWebSocketSubscribe(jsonMessage, webSocket, client);
        } else if (request.contains("action")) {
            handleWebSocketOrder(jsonMessage, webSocket, client);
        } else {
            webSocket.send(json({
                {"success", false},
                {"error", "Unknown message. Use {\"subscribe\":\"AAPL\"} or {\"action\":\"BUY\"}"}
            }).dump());
        }
    } catch (const json::exception& e) {
        webSocket.send(json({{"success", false}, {"error", std::string("JSON parse error: ") + e.what()}}).dump());
    }
}
