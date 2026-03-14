#include "trading_ui_format.h"

#include <cmath>
#include <iomanip>
#include <sstream>

namespace {

void appendLatencyLine(std::ostringstream& oss, const char* label, double valueMs) {
    oss << label << ": ";
    if (valueMs >= 0.0) {
        if (valueMs >= 1000.0) {
            oss << std::fixed << std::setprecision(3) << (valueMs / 1000.0) << " s";
        } else {
            oss << std::fixed << std::setprecision(1) << valueMs << " ms";
        }
    } else {
        oss << "--";
    }
    oss << '\n';
}

std::string formatDurationFromNow(std::chrono::steady_clock::time_point start) {
    if (start.time_since_epoch().count() == 0) {
        return "--";
    }
    const double elapsedMs =
        std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start).count();
    std::ostringstream oss;
    if (elapsedMs >= 1000.0) {
        oss << std::fixed << std::setprecision(1) << (elapsedMs / 1000.0) << " s";
    } else {
        oss << std::fixed << std::setprecision(0) << elapsedMs << " ms";
    }
    return oss.str();
}

} // namespace

std::string formatOrderLocalStateText(const OrderInfo& order) {
    std::ostringstream oss;
    oss << localOrderStateToString(order.localState);
    const bool showAttempts =
        order.watchdogs.reconciliationAttempts > 0 &&
        (order.localState == LocalOrderState::NeedsReconciliation ||
         order.localState == LocalOrderState::NeedsManualReview);
    if ((order.localState == LocalOrderState::NeedsManualReview && order.manualReviewAcknowledged) || showAttempts) {
        oss << " (";
        bool wroteDetail = false;
        if (order.localState == LocalOrderState::NeedsManualReview && order.manualReviewAcknowledged) {
            oss << "ack";
            wroteDetail = true;
        }
        if (showAttempts) {
            if (wroteDetail) {
                oss << ", ";
            }
            oss << order.watchdogs.reconciliationAttempts;
        }
        oss << ')';
    }
    return oss.str();
}

std::string formatOrderWatchdogText(const OrderInfo& order) {
    std::ostringstream oss;
    if (order.localState == LocalOrderState::AwaitingBrokerEcho && order.watchdogs.brokerEchoArmed) {
        oss << "echo " << formatDurationFromNow(order.submitTime);
    } else if (order.localState == LocalOrderState::AwaitingCancelAck && order.watchdogs.cancelAckArmed) {
        const auto startedAt = order.watchdogs.lastBrokerCallback.time_since_epoch().count() != 0
            ? order.watchdogs.lastBrokerCallback
            : order.submitTime;
        oss << "cancel " << formatDurationFromNow(startedAt);
    } else if (order.localState == LocalOrderState::PartiallyFilled && order.watchdogs.partialFillQuietArmed) {
        oss << "quiet " << formatDurationFromNow(order.watchdogs.lastBrokerCallback);
    } else if (order.localState == LocalOrderState::NeedsReconciliation) {
        oss << "retry " << order.watchdogs.reconciliationAttempts;
    } else if (order.localState == LocalOrderState::NeedsManualReview) {
        oss << "manual review";
        if (order.manualReviewAcknowledged) {
            oss << " ack";
        }
    } else if (order.watchdogs.lastBrokerCallback.time_since_epoch().count() != 0 && !order.isTerminal()) {
        oss << "callback " << formatDurationFromNow(order.watchdogs.lastBrokerCallback) << " ago";
    } else {
        return formatOrderTimingText(order);
    }

    if (!order.lastReconciliationReason.empty() &&
        (order.localState == LocalOrderState::NeedsReconciliation ||
         order.localState == LocalOrderState::NeedsManualReview)) {
        oss << " · " << order.lastReconciliationReason;
    }
    return oss.str();
}

std::string formatOrderTimingText(const OrderInfo& order) {
    std::ostringstream oss;
    if (order.fillDurationMs >= 0.0) {
        if (order.fillDurationMs >= 1000.0) {
            oss << std::fixed << std::setprecision(2) << (order.fillDurationMs / 1000.0) << " s";
        } else {
            oss << std::fixed << std::setprecision(0) << order.fillDurationMs << " ms";
        }
    } else if (order.submitTime.time_since_epoch().count() > 0 && !order.isTerminal()) {
        const auto elapsed = std::chrono::steady_clock::now() - order.submitTime;
        const double elapsedMs = std::chrono::duration<double, std::milli>(elapsed).count();
        if (elapsedMs >= 1000.0) {
            oss << std::fixed << std::setprecision(1) << (elapsedMs / 1000.0) << " s...";
        } else {
            oss << std::fixed << std::setprecision(0) << elapsedMs << " ms...";
        }
    } else {
        oss << "--";
    }
    return oss.str();
}

std::string formatTradeTraceDetailsText(const TradeTraceSnapshot& snapshot) {
    if (!snapshot.found) {
        return "No trade trace selected.";
    }

    const TradeTrace& trace = snapshot.trace;
    std::ostringstream oss;
    oss << "Trace " << trace.traceId;
    if (trace.orderId > 0) {
        oss << "  |  Order " << static_cast<long long>(trace.orderId);
    } else {
        oss << "  |  No order ID assigned";
    }
    if (trace.permId > 0) {
        oss << "  |  PermId " << trace.permId;
    }
    oss << "\n\n";

    oss << "Source: " << (trace.source.empty() ? "<unknown>" : trace.source) << '\n';
    oss << "Symbol: " << (trace.symbol.empty() ? "<none>" : trace.symbol) << '\n';
    oss << "Side: " << (trace.side.empty() ? "<none>" : trace.side) << '\n';
    oss << "Requested: " << trace.requestedQty << " @ " << std::fixed << std::setprecision(2) << trace.limitPrice << '\n';
    oss << "Close-only: " << (trace.closeOnly ? "yes" : "no") << '\n';
    if (!trace.latestStatus.empty()) {
        oss << "Latest status: " << trace.latestStatus << '\n';
    }
    if (!trace.terminalStatus.empty()) {
        oss << "Terminal status: " << trace.terminalStatus << '\n';
    }
    if (!trace.latestError.empty()) {
        oss << "Latest error: " << trace.latestError << '\n';
    }
    oss << '\n';

    oss << "Decision snapshot\n";
    oss << "  bid=" << std::fixed << std::setprecision(2) << trace.decisionBid
        << " ask=" << trace.decisionAsk
        << " last=" << trace.decisionLast
        << " sweep=" << trace.sweepEstimate
        << " buffer=" << trace.priceBuffer << '\n';
    if (!trace.bookSummary.empty()) {
        oss << "  book: " << trace.bookSummary << '\n';
    }
    if (!trace.notes.empty()) {
        oss << "  notes: " << trace.notes << '\n';
    }
    oss << '\n';

    oss << "Latency breakdown\n";
    appendLatencyLine(oss, "Validation", durationMs(trace.validationStartMono, trace.validationEndMono));
    appendLatencyLine(oss, "Trigger -> placeOrder return", durationMs(trace.triggerMono, trace.placeCallEndMono));
    appendLatencyLine(oss, "placeOrder return -> openOrder", durationMs(trace.placeCallEndMono, trace.firstOpenOrderMono));
    appendLatencyLine(oss, "placeOrder return -> first orderStatus", durationMs(trace.placeCallEndMono, trace.firstStatusMono));
    appendLatencyLine(oss, "placeOrder return -> first exec", durationMs(trace.placeCallEndMono, trace.firstExecMono));
    appendLatencyLine(oss, "Trigger -> first exec", durationMs(trace.triggerMono, trace.firstExecMono));
    appendLatencyLine(oss, "First exec -> full fill", durationMs(trace.firstExecMono, trace.fullFillMono));
    appendLatencyLine(oss, "Trigger -> full fill", durationMs(trace.triggerMono, trace.fullFillMono));
    oss << '\n';

    if (!trace.fills.empty()) {
        oss << "Fill slices\n";
        for (const auto& fill : trace.fills) {
            oss << "  " << fill.execId
                << "  shares=" << fill.shares
                << "  price=" << std::fixed << std::setprecision(2) << fill.price
                << "  cum=" << fill.cumQty
                << "  exch=" << fill.exchange;
            if (fill.commissionKnown) {
                oss << "  commission=" << std::fixed << std::setprecision(4)
                    << fill.commission << ' ' << fill.commissionCurrency;
            }
            oss << '\n';
        }
        if (!trace.commissionCurrency.empty()) {
            oss << "  total commission=" << std::fixed << std::setprecision(4)
                << trace.totalCommission << ' ' << trace.commissionCurrency << '\n';
        }
        oss << '\n';
    }

    oss << "Timeline\n";
    for (const auto& event : trace.events) {
        oss << "  " << formatWallTime(event.wallTs) << "  ";
        const double sinceTriggerMs = durationMs(trace.triggerMono, event.monoTs);
        if (sinceTriggerMs >= 0.0) {
            oss << std::fixed << std::setprecision(1) << sinceTriggerMs << " ms  ";
        } else {
            oss << "--  ";
        }
        oss << event.stage << "  " << event.details << '\n';
    }

    return oss.str();
}
