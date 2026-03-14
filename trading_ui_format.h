#pragma once

#include "app_shared.h"

#include <string>

std::string formatOrderLocalStateText(const OrderInfo& order);
std::string formatOrderWatchdogText(const OrderInfo& order);
std::string formatOrderTimingText(const OrderInfo& order);
std::string formatTradeTraceDetailsText(const TradeTraceSnapshot& snapshot);
