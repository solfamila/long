#pragma once

#include "app_shared.h"

struct TraceExportBundle {
    std::string baseName;
    std::string reportText;
    std::string summaryCsv;
    std::string fillsCsv;
    std::string timelineCsv;
};

bool replayTradeTraceSnapshotFromLog(std::uint64_t traceId,
                                     TradeTraceSnapshot* outSnapshot,
                                     std::string* error = nullptr,
                                     const std::string& logPath = TRADE_TRACE_LOG_FILENAME);
std::vector<TradeTraceListItem> buildTradeTraceListItemsFromLog(std::size_t maxItems = 100,
                                                                const std::string& logPath = TRADE_TRACE_LOG_FILENAME);
bool buildTraceExportBundle(std::uint64_t traceId, TraceExportBundle* outBundle, std::string* error = nullptr);
std::string buildAllTradesSummaryCsv(std::size_t maxItems = 1000);
