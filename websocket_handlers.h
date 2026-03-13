#pragma once

#include "app_shared.h"

void handleWebSocketOrder(const std::string& jsonMessage, ix::WebSocket& webSocket, EClientSocket* client);
void handleWebSocketSubscribe(const std::string& jsonMessage, ix::WebSocket& webSocket, EClientSocket* client);
void handleWebSocketMessage(const std::string& jsonMessage, ix::WebSocket& webSocket, EClientSocket* client);
