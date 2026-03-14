#include "websocket_handlers.h"
#include "mac_observability.h"
#include "trading_runtime.h"

namespace {

bool authorizeWebSocketRequest(const json& request, ix::WebSocket& webSocket, TradingRuntime* runtime) {
    if (!request.contains("token") || !request["token"].is_string()) {
        appendRuntimeJournalEvent("ws_auth_failed", {{"reason", "missing_token"}});
        macLogError("ipc", "WebSocket auth failed: missing token");
        webSocket.send(json({{"success", false}, {"error", "Missing required string field: token"}}).dump());
        return false;
    }

    const std::string providedToken = request["token"].get<std::string>();
    const std::string expectedToken = runtime != nullptr
        ? runtime->ensureWebSocketAuthToken()
        : ensureWebSocketAuthToken();
    if (providedToken != expectedToken) {
        appendRuntimeJournalEvent("ws_auth_failed", {{"reason", "token_mismatch"}});
        macLogError("ipc", "WebSocket auth failed: token mismatch");
        webSocket.send(json({{"success", false}, {"error", "Invalid session token"}}).dump());
        return false;
    }
    return true;
}

} // namespace

void handleWebSocketOrder(const std::string& jsonMessage, ix::WebSocket& webSocket, TradingRuntime* runtime) {
    try {
        json orderRequest = json::parse(jsonMessage);

        if (runtime == nullptr || !runtime->isStarted()) {
            webSocket.send(json({{"success", false}, {"error", "Trading runtime is not started"}}).dump());
            return;
        }

        if (!authorizeWebSocketRequest(orderRequest, webSocket, runtime)) {
            return;
        }

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
        const RuntimePresentationSnapshot runtimeSnapshot = runtime->capturePresentationSnapshot(std::string(), 0);

        std::string symbol;
        if (orderRequest.contains("symbol")) {
            symbol = toUpperCase(orderRequest["symbol"].get<std::string>());
        } else {
            symbol = runtimeSnapshot.activeSymbol;
        }

        if (symbol.empty()) {
            webSocket.send(json({{"success", false}, {"error", "No symbol specified and no active symbol subscribed"}}).dump());
            return;
        }

        const RuntimePresentationSnapshot symbolSnapshot = runtime->capturePresentationSnapshot(symbol, 0);
        double quantity = 0.0;
        if (orderRequest.contains("quantity")) {
            quantity = static_cast<double>(orderRequest["quantity"].get<int>());
        } else {
            if (action == "SELL" && !allowOpenShort) {
                quantity = symbolSnapshot.symbol.availableLongToClose;
            } else {
                quantity = static_cast<double>(symbolSnapshot.currentQuantity);
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

        std::string rateLimitError;
        if (!consumeWebSocketOrderRateLimit(&rateLimitError)) {
            appendRuntimeJournalEvent("ws_order_rejected", {{"reason", rateLimitError}});
            webSocket.send(json({{"success", false}, {"error", rateLimitError}}).dump());
            return;
        }

        std::string idempotencyError;
        const std::string idempotencyKey = orderRequest.value("idempotencyKey", std::string());
        if (!reserveWebSocketIdempotencyKey(idempotencyKey, &idempotencyError)) {
            appendRuntimeJournalEvent("ws_order_rejected", {{"reason", idempotencyError}});
            webSocket.send(json({{"success", false}, {"error", idempotencyError}}).dump());
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
            buffer = symbolSnapshot.priceBuffer;
            if (symbolSnapshot.activeSymbol != symbol || !symbolSnapshot.subscriptionActive) {
                webSocket.send(json({{"success", false}, {"error", "No market data for requested symbol; provide limitPrice explicitly"}}).dump());
                return;
            }

            if (action == "BUY") {
                if (!symbolSnapshot.symbol.askBook.empty()) {
                    sweepEstimate = calculateSweepPrice(symbolSnapshot.symbol.askBook, qtyShares, buffer, true);
                    limitPrice = sweepEstimate;
                }
                if (limitPrice <= 0.0 && symbolSnapshot.symbol.askPrice > 0.0) {
                    limitPrice = symbolSnapshot.symbol.askPrice + buffer;
                }
                if (limitPrice > 0.0 && symbolSnapshot.symbol.askPrice > 0.0) {
                    limitPrice = std::max(limitPrice, symbolSnapshot.symbol.askPrice + buffer);
                }
            } else {
                if (!symbolSnapshot.symbol.bidBook.empty()) {
                    sweepEstimate = calculateSweepPrice(symbolSnapshot.symbol.bidBook, qtyShares, buffer, false);
                    limitPrice = sweepEstimate;
                }
                if (limitPrice <= 0.0 && symbolSnapshot.symbol.bidPrice > 0.0) {
                    limitPrice = std::max(0.01, symbolSnapshot.symbol.bidPrice - buffer);
                }
                if (limitPrice > 0.0 && symbolSnapshot.symbol.bidPrice > 0.0) {
                    limitPrice = std::min(limitPrice, std::max(0.01, symbolSnapshot.symbol.bidPrice - buffer));
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
        BridgeOutboxEnqueueResult bridgeOutbox;
        if (!runtime->submitOrderIntent(intent,
                                        static_cast<double>(qtyShares),
                                        limitPrice,
                                        closeOnly,
                                        &submitError,
                                        &traceId,
                                        &orderId,
                                        &bridgeOutbox)) {
            macLogError("ipc", "WebSocket order rejected: " + submitError);
            appendRuntimeJournalEvent("ws_order_rejected", {
                {"reason", submitError},
                {"symbol", symbol},
                {"action", action},
                {"idempotencyKey", idempotencyKey}
            });
            webSocket.send(json({{"success", false}, {"error", submitError}, {"traceId", static_cast<unsigned long long>(traceId)}}).dump());
            return;
        }

        appendRuntimeJournalEvent("ws_order_accepted", {
            {"orderId", static_cast<long long>(orderId)},
            {"traceId", static_cast<unsigned long long>(traceId)},
            {"sourceSeq", static_cast<unsigned long long>(bridgeOutbox.sourceSeq)},
            {"symbol", symbol},
            {"action", action},
            {"idempotencyKey", idempotencyKey},
            {"bridgeFallbackState", bridgeOutbox.fallbackState},
            {"bridgeFallbackReason", bridgeOutbox.fallbackReason}
        });
        macLogInfo("ipc", "WebSocket order accepted for " + symbol + " trace " + std::to_string(static_cast<unsigned long long>(traceId)));
        const BridgeOutboxSnapshot bridgeSnapshot = runtime->captureBridgeOutboxSnapshot(0);
        webSocket.send(json({
            {"success", true},
            {"orderId", static_cast<long long>(orderId)},
            {"traceId", static_cast<unsigned long long>(traceId)},
            {"sourceSeq", static_cast<unsigned long long>(bridgeOutbox.sourceSeq)},
            {"symbol", symbol},
            {"action", action},
            {"quantity", qtyShares},
            {"limitPrice", limitPrice},
            {"closeOnly", closeOnly},
            {"bridgeFallbackState", bridgeOutbox.fallbackState},
            {"bridgeFallbackReason", bridgeOutbox.fallbackReason},
            {"bridgeRecoveryRequired", bridgeSnapshot.recoveryRequired},
            {"bridgePendingCount", bridgeSnapshot.pendingCount},
            {"bridgeLossCount", bridgeSnapshot.lossCount}
        }).dump());

    } catch (const json::exception& e) {
        webSocket.send(json({{"success", false}, {"error", std::string("JSON parse error: ") + e.what()}}).dump());
    } catch (const std::exception& e) {
        webSocket.send(json({{"success", false}, {"error", std::string("Error: ") + e.what()}}).dump());
    }
}

void handleWebSocketSubscribe(const std::string& jsonMessage, ix::WebSocket& webSocket, TradingRuntime* runtime) {
    try {
        json request = json::parse(jsonMessage);

        if (runtime == nullptr || !runtime->isStarted()) {
            webSocket.send(json({{"success", false}, {"error", "Trading runtime is not started"}}).dump());
            return;
        }

        if (!authorizeWebSocketRequest(request, webSocket, runtime)) {
            return;
        }

        if (!request.contains("subscribe") || !request["subscribe"].is_string()) {
            webSocket.send(json({{"success", false}, {"error", "Missing required string field: subscribe"}}).dump());
            return;
        }

        const std::string symbol = toUpperCase(request["subscribe"].get<std::string>());
        if (symbol.empty()) {
            webSocket.send(json({{"success", false}, {"error", "Symbol cannot be empty"}}).dump());
            return;
        }

        std::string error;
        bool duplicateIgnored = false;
        bool alreadySubscribed = false;
        if (!runtime->requestWebSocketSubscription(symbol, &error, &duplicateIgnored, &alreadySubscribed)) {
            webSocket.send(json({{"success", false}, {"error", error}}).dump());
            return;
        }

        webSocket.send(json({
            {"success", true},
            {"subscribed", symbol},
            {"duplicateIgnored", duplicateIgnored},
            {"alreadySubscribed", alreadySubscribed},
            {"requestSent", !duplicateIgnored && !alreadySubscribed}
        }).dump());
        if (!duplicateIgnored && !alreadySubscribed) {
            appendRuntimeJournalEvent("ws_subscribe_accepted", {{"symbol", symbol}});
        }

    } catch (const json::exception& e) {
        webSocket.send(json({{"success", false}, {"error", std::string("JSON parse error: ") + e.what()}}).dump());
    } catch (const std::exception& e) {
        webSocket.send(json({{"success", false}, {"error", std::string("Error: ") + e.what()}}).dump());
    }
}

void handleWebSocketMessage(const std::string& jsonMessage, ix::WebSocket& webSocket, TradingRuntime* runtime) {
    if (jsonMessage.size() > 4096) {
        webSocket.send(json({{"success", false}, {"error", "Message too large"}}).dump());
        return;
    }

    try {
        json request = json::parse(jsonMessage);

        if (request.contains("subscribe")) {
            handleWebSocketSubscribe(jsonMessage, webSocket, runtime);
        } else if (request.contains("action")) {
            handleWebSocketOrder(jsonMessage, webSocket, runtime);
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
