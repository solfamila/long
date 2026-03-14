#include "trace_exporter.h"
#include "trading_ui_format.h"

#include <fstream>
#include <limits>
#include <map>
#include <optional>
#include <set>

namespace {

std::string csvEscape(const std::string& value) {
    bool needsQuotes = value.find_first_of(",\"\n") != std::string::npos;
    if (!needsQuotes) {
        return value;
    }

    std::string escaped = "\"";
    for (char ch : value) {
        if (ch == '"') {
            escaped += "\"\"";
        } else {
            escaped.push_back(ch);
        }
    }
    escaped.push_back('"');
    return escaped;
}

std::string sanitizeBaseName(const TradeTrace& trace) {
    std::ostringstream oss;
    oss << "trace-" << trace.traceId;
    if (!trace.symbol.empty()) {
        oss << "-" << trace.symbol;
    }
    return oss.str();
}

TradeEventType parseTradeEventType(const std::string& value) {
    if (value == "Trigger") return TradeEventType::Trigger;
    if (value == "ValidationStart") return TradeEventType::ValidationStart;
    if (value == "ValidationOk") return TradeEventType::ValidationOk;
    if (value == "ValidationFailed") return TradeEventType::ValidationFailed;
    if (value == "PlaceOrderCallStart") return TradeEventType::PlaceOrderCallStart;
    if (value == "PlaceOrderCallEnd") return TradeEventType::PlaceOrderCallEnd;
    if (value == "OpenOrderSeen") return TradeEventType::OpenOrderSeen;
    if (value == "OrderStatusSeen") return TradeEventType::OrderStatusSeen;
    if (value == "ExecDetailsSeen") return TradeEventType::ExecDetailsSeen;
    if (value == "CommissionSeen") return TradeEventType::CommissionSeen;
    if (value == "ErrorSeen") return TradeEventType::ErrorSeen;
    if (value == "CancelRequestSent") return TradeEventType::CancelRequestSent;
    if (value == "CancelAck") return TradeEventType::CancelAck;
    if (value == "FinalState") return TradeEventType::FinalState;
    return TradeEventType::Note;
}

std::chrono::system_clock::time_point parseWallTimeText(const std::string& value) {
    if (value.size() < 12) {
        return {};
    }

    int hour = 0;
    int minute = 0;
    int second = 0;
    int millis = 0;
    if (std::sscanf(value.c_str(), "%d:%d:%d.%d", &hour, &minute, &second, &millis) != 4) {
        return {};
    }

    const auto now = std::chrono::system_clock::now();
    const std::time_t nowTime = std::chrono::system_clock::to_time_t(now);
    std::tm tmLocal{};
#if defined(_WIN32)
    localtime_s(&tmLocal, &nowTime);
#else
    localtime_r(&nowTime, &tmLocal);
#endif
    tmLocal.tm_hour = hour;
    tmLocal.tm_min = minute;
    tmLocal.tm_sec = second;
    const std::time_t wallTime = std::mktime(&tmLocal);
    if (wallTime == static_cast<std::time_t>(-1)) {
        return {};
    }
    return std::chrono::system_clock::from_time_t(wallTime) + std::chrono::milliseconds(millis);
}

bool hasSteadyTime(std::chrono::steady_clock::time_point tp) {
    return tp.time_since_epoch().count() != 0;
}

std::chrono::steady_clock::time_point replayEventMono(TradeTrace& trace, double sinceTriggerMs) {
    if (trace.triggerMono.time_since_epoch().count() == 0) {
        trace.triggerMono = std::chrono::steady_clock::now();
    }
    if (sinceTriggerMs < 0.0) {
        return trace.triggerMono;
    }
    return trace.triggerMono + std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                                   std::chrono::duration<double, std::milli>(sinceTriggerMs));
}

void updateReplayTiming(TradeTrace& trace, TradeEventType type, const std::chrono::steady_clock::time_point& monoTs) {
    switch (type) {
        case TradeEventType::Trigger:
            trace.triggerMono = monoTs;
            break;
        case TradeEventType::ValidationStart:
            if (trace.validationStartMono.time_since_epoch().count() == 0) trace.validationStartMono = monoTs;
            break;
        case TradeEventType::ValidationOk:
        case TradeEventType::ValidationFailed:
            if (trace.validationEndMono.time_since_epoch().count() == 0) trace.validationEndMono = monoTs;
            break;
        case TradeEventType::PlaceOrderCallStart:
            if (trace.placeCallStartMono.time_since_epoch().count() == 0) trace.placeCallStartMono = monoTs;
            break;
        case TradeEventType::PlaceOrderCallEnd:
            if (trace.placeCallEndMono.time_since_epoch().count() == 0) trace.placeCallEndMono = monoTs;
            break;
        case TradeEventType::OpenOrderSeen:
            if (trace.firstOpenOrderMono.time_since_epoch().count() == 0) trace.firstOpenOrderMono = monoTs;
            break;
        case TradeEventType::OrderStatusSeen:
            if (trace.firstStatusMono.time_since_epoch().count() == 0) trace.firstStatusMono = monoTs;
            break;
        case TradeEventType::ExecDetailsSeen:
            if (trace.firstExecMono.time_since_epoch().count() == 0) trace.firstExecMono = monoTs;
            break;
        case TradeEventType::CancelRequestSent:
            if (trace.cancelReqMono.time_since_epoch().count() == 0) trace.cancelReqMono = monoTs;
            break;
        case TradeEventType::FinalState:
            if (trace.fullFillMono.time_since_epoch().count() == 0 &&
                (trace.terminalStatus == "Filled" || trace.latestStatus == "Filled")) {
                trace.fullFillMono = monoTs;
            }
            break;
        default:
            break;
    }
}

void parseExecDetailsIntoFill(const TraceEvent& event, FillSlice& fill) {
    fill.shares = event.shares;
    fill.price = event.price;
    fill.cumQty = event.cumFilled;

    const std::string& details = event.details;
    const auto exchPos = details.find("exch=");
    if (exchPos != std::string::npos) {
        const auto end = details.find(' ', exchPos);
        fill.exchange = details.substr(exchPos + 5, end == std::string::npos ? std::string::npos : end - (exchPos + 5));
    }
    const auto execPos = details.find("execId=");
    if (execPos != std::string::npos) {
        const auto end = details.find(' ', execPos);
        fill.execId = details.substr(execPos + 7, end == std::string::npos ? std::string::npos : end - (execPos + 7));
    }
    const auto timePos = details.find("time=");
    if (timePos != std::string::npos) {
        fill.execTimeText = details.substr(timePos + 5);
    }
}

void applyCommissionToTrace(TradeTrace& trace, const TraceEvent& event) {
    const std::string& details = event.details;
    const auto idEnd = details.find(' ');
    if (idEnd == std::string::npos) {
        return;
    }
    const std::string execId = details.substr(0, idEnd);
    const auto commissionPos = details.find("commission=");
    if (commissionPos == std::string::npos) {
        return;
    }
    const auto valueStart = commissionPos + 11;
    const auto valueEnd = details.find(' ', valueStart);
    const std::string commissionText = details.substr(valueStart, valueEnd == std::string::npos ? std::string::npos : valueEnd - valueStart);
    const double commission = std::atof(commissionText.c_str());
    std::string currency;
    if (valueEnd != std::string::npos && valueEnd + 1 < details.size()) {
        currency = details.substr(valueEnd + 1);
    }
    trace.totalCommission += commission;
    if (!currency.empty()) {
        trace.commissionCurrency = currency;
    }
    for (auto& fill : trace.fills) {
        if (fill.execId == execId) {
            fill.commission = commission;
            fill.commissionKnown = true;
            fill.commissionCurrency = currency;
            break;
        }
    }
}

std::string terminalStatusFromDetails(const std::string& details) {
    const auto colon = details.find(':');
    if (colon == std::string::npos) {
        return details;
    }
    return details.substr(0, colon);
}

TradeTraceListItem makeTraceListItem(const TradeTrace& trace) {
    TradeTraceListItem item;
    item.traceId = trace.traceId;
    item.orderId = trace.orderId;
    item.terminal = !trace.terminalStatus.empty();
    item.failed = trace.failedBeforeSubmit || !trace.latestError.empty();
    std::ostringstream oss;
    oss << "T" << trace.traceId;
    if (trace.orderId > 0) {
        oss << " / O" << static_cast<long long>(trace.orderId);
    } else {
        oss << " / pending";
    }
    oss << " | " << (trace.source.empty() ? "Replay" : trace.source)
        << " | " << (trace.side.empty() ? "?" : trace.side)
        << ' ' << (trace.symbol.empty() ? "<none>" : trace.symbol)
        << ' ' << trace.requestedQty
        << " @ " << std::fixed << std::setprecision(2) << trace.limitPrice;
    if (!trace.terminalStatus.empty()) {
        oss << " | " << trace.terminalStatus;
    } else if (!trace.latestStatus.empty()) {
        oss << " | " << trace.latestStatus;
    }
    if (!trace.latestError.empty()) {
        oss << " | ERR";
    }
    item.summary = oss.str();
    return item;
}

std::string resolvedTraceLogPath(const std::string& logPath) {
    return logPath.empty() ? tradeTraceLogPath() : logPath;
}

std::optional<OrderInfo> findOrderInfo(OrderId orderId) {
    if (orderId <= 0) {
        return std::nullopt;
    }
    const auto orders = captureOrdersSnapshot();
    for (const auto& entry : orders) {
        if (entry.first == orderId) {
            return entry.second;
        }
    }
    return std::nullopt;
}

bool traceContainsExecId(const TradeTrace& trace, const std::string& execId) {
    if (execId.empty()) {
        return false;
    }
    for (const auto& fill : trace.fills) {
        if (fill.execId == execId) {
            return true;
        }
    }
    return false;
}

int traceRichnessScore(const TradeTrace& trace) {
    int score = 0;
    if (!trace.source.empty() && trace.source != "Broker Reconcile") score += 120;
    if (!trace.terminalStatus.empty()) score += 60;
    if (!trace.latestStatus.empty()) score += 25;
    if (!trace.latestError.empty()) score += 15;
    if (hasSteadyTime(trace.validationStartMono)) score += 10;
    if (hasSteadyTime(trace.validationEndMono)) score += 10;
    if (hasSteadyTime(trace.placeCallStartMono)) score += 10;
    if (hasSteadyTime(trace.placeCallEndMono)) score += 10;
    if (hasSteadyTime(trace.firstOpenOrderMono)) score += 10;
    if (hasSteadyTime(trace.firstStatusMono)) score += 10;
    if (hasSteadyTime(trace.firstExecMono)) score += 15;
    if (hasSteadyTime(trace.fullFillMono)) score += 15;
    score += static_cast<int>(std::min<std::size_t>(trace.events.size(), 32));
    score += static_cast<int>(std::min<std::size_t>(trace.fills.size(), 8) * 4);
    return score;
}

const TradeTrace* findMatchingTraceInLog(const std::map<std::uint64_t, TradeTrace>& traces,
                                         std::uint64_t traceId,
                                         OrderId orderId,
                                         long long permId,
                                         const std::string& execId,
                                         const std::vector<FillSlice>& fills) {
    const TradeTrace* bestMatch = nullptr;
    int bestScore = std::numeric_limits<int>::min();

    const auto considerCandidate = [&](const TradeTrace& candidate, bool directMatch) {
        int score = traceRichnessScore(candidate);
        if (directMatch) {
            score += 1;
        }
        if (bestMatch == nullptr || score > bestScore) {
            bestMatch = &candidate;
            bestScore = score;
        }
    };

    if (traceId != 0) {
        const auto direct = traces.find(traceId);
        if (direct != traces.end()) {
            considerCandidate(direct->second, true);
        }
    }

    for (const auto& [candidateId, candidate] : traces) {
        bool matched = false;
        if (orderId > 0 && candidate.orderId == orderId) {
            matched = true;
        }
        if (permId > 0 && candidate.permId == permId) {
            matched = true;
        }
        if (!execId.empty() && traceContainsExecId(candidate, execId)) {
            matched = true;
        }
        for (const auto& fill : fills) {
            if (!fill.execId.empty() && traceContainsExecId(candidate, fill.execId)) {
                matched = true;
                break;
            }
        }
        if (matched) {
            considerCandidate(candidate, candidateId == traceId);
        }
    }
    return bestMatch;
}

bool loadTradeTracesFromLog(const std::string& logPath,
                            std::map<std::uint64_t, TradeTrace>* outTraces,
                            std::vector<std::uint64_t>* outReplayOrder,
                            std::string* error) {
    if (outTraces == nullptr || outReplayOrder == nullptr) {
        if (error) {
            *error = "Missing replay output container";
        }
        return false;
    }

    const std::string resolvedPath = resolvedTraceLogPath(logPath);
    std::ifstream in(resolvedPath);
    if (!in.is_open()) {
        if (error) {
            *error = "Trace log not found at " + resolvedPath;
        }
        return false;
    }

    outTraces->clear();
    outReplayOrder->clear();

    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) {
            continue;
        }
        json parsed = json::parse(line, nullptr, false);
        if (parsed.is_discarded()) {
            continue;
        }
        const std::uint64_t traceId = parsed.value("traceId", 0ULL);
        if (traceId == 0) {
            continue;
        }

        TradeTrace& trace = (*outTraces)[traceId];
        trace.traceId = traceId;
        trace.orderId = parsed.value("orderId", static_cast<long long>(trace.orderId));
        trace.permId = parsed.value("permId", trace.permId);
        const std::string parsedSource = parsed.value("source", std::string());
        if (!parsedSource.empty() &&
            (trace.source.empty() || (trace.source == "Broker Reconcile" && parsedSource != "Broker Reconcile"))) {
            trace.source = parsedSource;
        }
        trace.symbol = parsed.value("symbol", trace.symbol);
        trace.side = parsed.value("side", trace.side);
        trace.requestedQty = parsed.value("requestedQty", trace.requestedQty);
        trace.limitPrice = parsed.value("limitPrice", trace.limitPrice);
        trace.closeOnly = parsed.value("closeOnly", trace.closeOnly);
        trace.notes = parsed.value("notes", trace.notes);
        trace.bookSummary = parsed.value("bookSummary", trace.bookSummary);

        const TradeEventType type = parseTradeEventType(parsed.value("eventType", std::string("Note")));
        const double sinceTriggerMs = parsed.value("sinceTriggerMs", -1.0);
        TraceEvent event;
        event.type = type;
        event.stage = parsed.value("stage", std::string());
        event.details = parsed.value("details", std::string());
        event.wallTs = parseWallTimeText(parsed.value("wallTime", std::string()));
        event.monoTs = replayEventMono(trace, sinceTriggerMs);
        event.cumFilled = parsed.value("cumFilled", -1.0);
        event.remaining = parsed.value("remaining", -1.0);
        event.price = parsed.value("price", 0.0);
        event.shares = parsed.value("shares", 0);
        event.errorCode = parsed.value("errorCode", 0);
        trace.events.push_back(event);
        updateReplayTiming(trace, type, event.monoTs);

        switch (type) {
            case TradeEventType::Trigger:
                trace.triggerWall = event.wallTs;
                break;
            case TradeEventType::ValidationFailed:
                trace.failedBeforeSubmit = true;
                trace.latestError = event.details;
                trace.terminalStatus = "FailedBeforeSubmit";
                break;
            case TradeEventType::OrderStatusSeen:
                trace.latestStatus = event.stage;
                if (event.stage == "Filled" && trace.fullFillMono.time_since_epoch().count() == 0) {
                    trace.fullFillMono = event.monoTs;
                }
                break;
            case TradeEventType::ExecDetailsSeen: {
                FillSlice fill;
                parseExecDetailsIntoFill(event, fill);
                if (!fill.execId.empty()) {
                    trace.fills.push_back(fill);
                }
                break;
            }
            case TradeEventType::CommissionSeen:
                applyCommissionToTrace(trace, event);
                break;
            case TradeEventType::ErrorSeen:
                trace.latestError = event.details;
                break;
            case TradeEventType::FinalState:
                trace.terminalStatus = terminalStatusFromDetails(event.details);
                if (trace.terminalStatus == "Filled" && trace.fullFillMono.time_since_epoch().count() == 0) {
                    trace.fullFillMono = event.monoTs;
                }
                break;
            default:
                break;
        }

        outReplayOrder->push_back(traceId);
    }

    if (outTraces->empty()) {
        if (error) {
            *error = "Trace log did not contain any replayable traces";
        }
        return false;
    }
    return true;
}

} // namespace

bool replayTradeTraceSnapshotFromLog(std::uint64_t traceId,
                                     TradeTraceSnapshot* outSnapshot,
                                     std::string* error,
                                     const std::string& logPath) {
    if (outSnapshot == nullptr) {
        if (error) {
            *error = "Missing output snapshot";
        }
        return false;
    }

    std::map<std::uint64_t, TradeTrace> traces;
    std::vector<std::uint64_t> replayOrder;
    if (!loadTradeTracesFromLog(logPath, &traces, &replayOrder, error)) {
        return false;
    }

    std::uint64_t resolvedTraceId = traceId;
    if (resolvedTraceId == 0 && !replayOrder.empty()) {
        resolvedTraceId = replayOrder.back();
    }

    const auto it = traces.find(resolvedTraceId);
    if (it == traces.end()) {
        if (error) {
            *error = "Trace " + std::to_string(static_cast<unsigned long long>(resolvedTraceId)) +
                     " was not found in " + logPath;
        }
        return false;
    }

    outSnapshot->found = true;
    outSnapshot->trace = it->second;
    return true;
}

bool replayTradeTraceSnapshotByIdentityFromLog(OrderId orderId,
                                               long long permId,
                                               const std::string& execId,
                                               TradeTraceSnapshot* outSnapshot,
                                               std::string* error,
                                               const std::string& logPath) {
    if (outSnapshot == nullptr) {
        if (error) {
            *error = "Missing output snapshot";
        }
        return false;
    }

    std::map<std::uint64_t, TradeTrace> traces;
    std::vector<std::uint64_t> replayOrder;
    if (!loadTradeTracesFromLog(logPath, &traces, &replayOrder, error)) {
        return false;
    }

    const TradeTrace* match = findMatchingTraceInLog(traces, 0, orderId, permId, execId, {});
    if (match == nullptr) {
        if (error) {
            *error = "No matching trace found in " + resolvedTraceLogPath(logPath);
        }
        return false;
    }

    outSnapshot->found = true;
    outSnapshot->trace = *match;
    return true;
}

bool enrichTradeTraceSnapshotFromLog(TradeTraceSnapshot* snapshot,
                                     std::string* error,
                                     const std::string& logPath) {
    if (snapshot == nullptr || !snapshot->found) {
        if (error) {
            *error = "Missing input snapshot";
        }
        return false;
    }

    std::map<std::uint64_t, TradeTrace> traces;
    std::vector<std::uint64_t> replayOrder;
    if (!loadTradeTracesFromLog(logPath, &traces, &replayOrder, error)) {
        return false;
    }

    const TradeTrace& current = snapshot->trace;
    const TradeTrace* match = findMatchingTraceInLog(traces,
                                                     current.traceId,
                                                     current.orderId,
                                                     current.permId,
                                                     {},
                                                     current.fills);
    if (match == nullptr) {
        if (error) {
            *error = "No matching trace found in " + resolvedTraceLogPath(logPath);
        }
        return false;
    }

    if (traceRichnessScore(*match) >= traceRichnessScore(current)) {
        snapshot->trace = *match;
    }
    return true;
}

std::vector<TradeTraceListItem> buildTradeTraceListItemsFromLog(std::size_t maxItems,
                                                                const std::string& logPath) {
    std::map<std::uint64_t, TradeTrace> traces;
    std::vector<std::uint64_t> replayOrder;
    std::vector<TradeTraceListItem> items;
    std::string error;
    if (!loadTradeTracesFromLog(logPath, &traces, &replayOrder, &error)) {
        return items;
    }

    std::set<std::uint64_t> seen;
    for (auto it = replayOrder.rbegin(); it != replayOrder.rend() && items.size() < maxItems; ++it) {
        if (!seen.insert(*it).second) {
            continue;
        }
        const auto traceIt = traces.find(*it);
        if (traceIt == traces.end()) {
            continue;
        }
        items.push_back(makeTraceListItem(traceIt->second));
    }
    return items;
}

bool buildTraceExportBundle(std::uint64_t traceId, TraceExportBundle* outBundle, std::string* error) {
    if (!outBundle) {
        if (error) {
            *error = "Missing output bundle";
        }
        return false;
    }

    TradeTraceSnapshot snapshot = captureTradeTraceSnapshot(traceId);
    if (!snapshot.found) {
        replayTradeTraceSnapshotFromLog(traceId, &snapshot, error);
    } else {
        enrichTradeTraceSnapshotFromLog(&snapshot, nullptr);
    }
    if (!snapshot.found) {
        if (error) {
            *error = "Trace not found";
        }
        return false;
    }

    const TradeTrace& trace = snapshot.trace;
    const std::optional<OrderInfo> orderInfo = findOrderInfo(trace.orderId);
    outBundle->baseName = sanitizeBaseName(trace);

    std::ostringstream report;
    report << "Trade Trace " << trace.traceId << "\n";
    report << "Order ID," << trace.orderId << "\n";
    report << "Symbol," << trace.symbol << "\n";
    report << "Side," << trace.side << "\n";
    report << "Requested Qty," << trace.requestedQty << "\n";
    report << "Limit Price," << std::fixed << std::setprecision(2) << trace.limitPrice << "\n";
    report << "Source," << trace.source << "\n";
    report << "Latest Status," << trace.latestStatus << "\n";
    report << "Terminal Status," << trace.terminalStatus << "\n";
    report << "Latest Error," << trace.latestError << "\n";
    if (orderInfo.has_value()) {
        report << "Local Order State," << formatOrderLocalStateText(*orderInfo) << "\n";
        report << "Watchdog Status," << formatOrderWatchdogText(*orderInfo) << "\n";
        report << "Reconciliation Reason," << orderInfo->lastReconciliationReason << "\n";
    }
    report << "Total Commission," << std::fixed << std::setprecision(4) << trace.totalCommission << "\n";
    report << "Commission Currency," << trace.commissionCurrency << "\n\n";
    report << "Latency Breakdown\n";
    report << "Validation," << durationMs(trace.validationStartMono, trace.validationEndMono) << "\n";
    report << "TriggerToPlaceReturn," << durationMs(trace.triggerMono, trace.placeCallEndMono) << "\n";
    report << "PlaceReturnToOpenOrder," << durationMs(trace.placeCallEndMono, trace.firstOpenOrderMono) << "\n";
    report << "PlaceReturnToFirstStatus," << durationMs(trace.placeCallEndMono, trace.firstStatusMono) << "\n";
    report << "PlaceReturnToFirstExec," << durationMs(trace.placeCallEndMono, trace.firstExecMono) << "\n";
    report << "TriggerToFullFill," << durationMs(trace.triggerMono, trace.fullFillMono) << "\n";
    outBundle->reportText = report.str();

    std::ostringstream summaryCsv;
    summaryCsv << "trace_id,order_id,perm_id,source,symbol,side,requested_qty,limit_price,close_only,"
                  "latest_status,terminal_status,latest_error,total_commission,commission_currency,"
                  "local_order_state,watchdog_status,reconciliation_attempts,last_reconciliation_reason,"
                  "validation_ms,trigger_to_place_return_ms,first_exec_ms,full_fill_ms\n";
    summaryCsv << trace.traceId << ','
               << static_cast<long long>(trace.orderId) << ','
               << trace.permId << ','
               << csvEscape(trace.source) << ','
               << csvEscape(trace.symbol) << ','
               << csvEscape(trace.side) << ','
               << trace.requestedQty << ','
               << std::fixed << std::setprecision(2) << trace.limitPrice << ','
               << (trace.closeOnly ? "true" : "false") << ','
               << csvEscape(trace.latestStatus) << ','
               << csvEscape(trace.terminalStatus) << ','
               << csvEscape(trace.latestError) << ','
               << std::fixed << std::setprecision(4) << trace.totalCommission << ','
               << csvEscape(trace.commissionCurrency) << ','
               << csvEscape(orderInfo ? formatOrderLocalStateText(*orderInfo) : "") << ','
               << csvEscape(orderInfo ? formatOrderWatchdogText(*orderInfo) : "") << ','
               << (orderInfo ? orderInfo->watchdogs.reconciliationAttempts : 0) << ','
               << csvEscape(orderInfo ? orderInfo->lastReconciliationReason : "") << ','
               << durationMs(trace.validationStartMono, trace.validationEndMono) << ','
               << durationMs(trace.triggerMono, trace.placeCallEndMono) << ','
               << durationMs(trace.triggerMono, trace.firstExecMono) << ','
               << durationMs(trace.triggerMono, trace.fullFillMono) << '\n';
    outBundle->summaryCsv = summaryCsv.str();

    std::ostringstream fillsCsv;
    fillsCsv << "trace_id,exec_id,shares,price,cum_qty,avg_price,exchange,liquidity,exec_time,commission,commission_currency\n";
    for (const auto& fill : trace.fills) {
        fillsCsv << trace.traceId << ','
                 << csvEscape(fill.execId) << ','
                 << fill.shares << ','
                 << std::fixed << std::setprecision(4) << fill.price << ','
                 << fill.cumQty << ','
                 << fill.avgPrice << ','
                 << csvEscape(fill.exchange) << ','
                 << fill.liquidity << ','
                 << csvEscape(fill.execTimeText) << ','
                 << (fill.commissionKnown ? fill.commission : 0.0) << ','
                 << csvEscape(fill.commissionCurrency) << '\n';
    }
    outBundle->fillsCsv = fillsCsv.str();

    std::ostringstream timelineCsv;
    timelineCsv << "trace_id,wall_time,event_type,stage,details,since_trigger_ms,cum_filled,remaining,price,shares,error_code\n";
    for (const auto& event : trace.events) {
        timelineCsv << trace.traceId << ','
                    << csvEscape(formatWallTime(event.wallTs)) << ','
                    << csvEscape(tradeEventTypeToString(event.type)) << ','
                    << csvEscape(event.stage) << ','
                    << csvEscape(event.details) << ','
                    << durationMs(trace.triggerMono, event.monoTs) << ','
                    << event.cumFilled << ','
                    << event.remaining << ','
                    << event.price << ','
                    << event.shares << ','
                    << event.errorCode << '\n';
    }
    outBundle->timelineCsv = timelineCsv.str();
    return true;
}

std::string buildAllTradesSummaryCsv(std::size_t maxItems) {
    std::ostringstream out;
    out << "trace_id,order_id,source,symbol,side,requested_qty,limit_price,latest_status,terminal_status,"
           "latest_error,total_commission,local_order_state,watchdog_status,reconciliation_attempts,"
           "last_reconciliation_reason,trigger_to_fill_ms\n";
    std::vector<TradeTraceListItem> items = captureTradeTraceListItems(maxItems);
    if (items.empty()) {
        items = buildTradeTraceListItemsFromLog(maxItems);
    }
    for (const auto& item : items) {
        TradeTraceSnapshot snapshot = captureTradeTraceSnapshot(item.traceId);
        if (!snapshot.found) {
            replayTradeTraceSnapshotFromLog(item.traceId, &snapshot, nullptr);
        } else {
            enrichTradeTraceSnapshotFromLog(&snapshot, nullptr);
        }
        if (!snapshot.found) {
            continue;
        }
        const TradeTrace& trace = snapshot.trace;
        const std::optional<OrderInfo> orderInfo = findOrderInfo(trace.orderId);
        out << trace.traceId << ','
            << static_cast<long long>(trace.orderId) << ','
            << csvEscape(trace.source) << ','
            << csvEscape(trace.symbol) << ','
            << csvEscape(trace.side) << ','
            << trace.requestedQty << ','
            << std::fixed << std::setprecision(2) << trace.limitPrice << ','
            << csvEscape(trace.latestStatus) << ','
            << csvEscape(trace.terminalStatus) << ','
            << csvEscape(trace.latestError) << ','
            << std::fixed << std::setprecision(4) << trace.totalCommission << ','
            << csvEscape(orderInfo ? formatOrderLocalStateText(*orderInfo) : "") << ','
            << csvEscape(orderInfo ? formatOrderWatchdogText(*orderInfo) : "") << ','
            << (orderInfo ? orderInfo->watchdogs.reconciliationAttempts : 0) << ','
            << csvEscape(orderInfo ? orderInfo->lastReconciliationReason : "") << ','
            << durationMs(trace.triggerMono, trace.fullFillMono) << '\n';
    }
    return out.str();
}
