#pragma once

#include "app_shared.h"

class TradingRuntime;

void handleWebSocketOrder(const std::string& jsonMessage, ix::WebSocket& webSocket, TradingRuntime* runtime);
void handleWebSocketSubscribe(const std::string& jsonMessage, ix::WebSocket& webSocket, TradingRuntime* runtime);
void handleWebSocketMessage(const std::string& jsonMessage, ix::WebSocket& webSocket, TradingRuntime* runtime);
