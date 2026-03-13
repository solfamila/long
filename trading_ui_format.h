#pragma once

#include "app_shared.h"

#include <string>

std::string formatOrderTimingText(const OrderInfo& order);
std::string formatTradeTraceDetailsText(const TradeTraceSnapshot& snapshot);
