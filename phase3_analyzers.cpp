#include "phase3_analyzers.h"

#include <algorithm>
#include <cmath>
#include <iomanip>
#include <limits>
#include <memory>
#include <sstream>
#include <utility>

namespace tape_engine {

namespace {

constexpr double kSpreadWideningMinAbsolute = 0.01;
constexpr double kLiquidityChangeMinShares = 100.0;
constexpr double kLiquidityChangeMinRatio = 0.50;
constexpr double kTouchTradeTolerance = 1e-6;
constexpr double kMarketImpactMinMidMove = 0.02;

bool hasAnchorIdentity(const BridgeAnchorIdentity& anchor) {
    return anchor.traceId > 0 || anchor.orderId > 0 || anchor.permId > 0 || !anchor.execId.empty();
}

double numberOrDefault(const json& payload, const char* key, double fallback) {
    return payload.contains(key) && payload[key].is_number() && std::isfinite(payload[key].get<double>())
        ? payload[key].get<double>()
        : fallback;
}

std::optional<bool> inferSide(const std::string& side) {
    if (side == "BOT" || side == "BUY") {
        return true;
    }
    if (side == "SLD" || side == "SELL") {
        return false;
    }
    return std::nullopt;
}

class Phase3Analyzer {
public:
    virtual ~Phase3Analyzer() = default;
    virtual const char* name() const = 0;
    virtual AnalyzerLane lane() const = 0;
    virtual void analyzeHot(const HotAnalyzerInput& input,
                            std::vector<AnalyzerFindingSpec>* findings) const {
        (void)input;
        (void)findings;
    }
    virtual void analyzeDeferred(const DeferredAnalyzerInput& input,
                                 std::vector<AnalyzerFindingSpec>* findings) const {
        (void)input;
        (void)findings;
    }
};

class SourceContinuityAnalyzer final : public Phase3Analyzer {
public:
    const char* name() const override { return "source_continuity"; }
    AnalyzerLane lane() const override { return AnalyzerLane::Hot; }

    void analyzeHot(const HotAnalyzerInput& input,
                    std::vector<AnalyzerFindingSpec>* findings) const override {
        if (input.event.eventKind != "gap_marker" && input.event.eventKind != "reset_marker") {
            return;
        }

        AnalyzerFindingSpec finding;
        finding.kind = input.event.eventKind == "gap_marker" ? "source_gap" : "source_reset";
        finding.severity = "warning";
        finding.confidence = 0.95;
        finding.firstSessionSeq = input.event.sessionSeq;
        finding.lastSessionSeq = input.event.sessionSeq;
        finding.tsEngineNs = input.event.tsEngineNs;
        finding.instrumentId = input.event.instrumentId;
        finding.title = input.event.eventKind == "gap_marker"
            ? "Feed gap detected"
            : "Source sequence reset detected";
        finding.summary = input.event.eventKind == "gap_marker"
            ? "A discontinuity was recorded in the bridge source sequence."
            : "An out-of-order bridge source sequence forced a reset marker.";
        finding.overlapsOrder = input.overlapsOrder;
        finding.overlappingAnchor = input.overlappingAnchor;
        findings->push_back(std::move(finding));
    }
};

class SpreadWideningAnalyzer final : public Phase3Analyzer {
public:
    const char* name() const override { return "spread_widening"; }
    AnalyzerLane lane() const override { return AnalyzerLane::Hot; }

    void analyzeHot(const HotAnalyzerInput& input,
                    std::vector<AnalyzerFindingSpec>* findings) const override {
        if ((input.event.eventKind != "market_tick" && input.event.eventKind != "market_depth") ||
            !input.hadInside || !input.hasInside) {
            return;
        }

        const double previousSpread = input.previousAsk - input.previousBid;
        const double currentSpread = input.effectiveAsk - input.effectiveBid;
        if (currentSpread + 1e-9 < previousSpread + kSpreadWideningMinAbsolute) {
            return;
        }

        std::ostringstream title;
        title << "Spread widened to " << std::fixed << std::setprecision(2) << currentSpread;
        std::ostringstream summary;
        summary << "Inside spread widened from " << std::fixed << std::setprecision(2)
                << previousSpread << " to " << currentSpread << ".";

        AnalyzerFindingSpec finding;
        finding.kind = "spread_widened";
        finding.severity = "info";
        finding.confidence = 0.82;
        finding.firstSessionSeq = input.event.sessionSeq;
        finding.lastSessionSeq = input.event.sessionSeq;
        finding.tsEngineNs = input.event.tsEngineNs;
        finding.instrumentId = input.event.instrumentId;
        finding.title = title.str();
        finding.summary = summary.str();
        finding.overlapsOrder = input.overlapsOrder;
        finding.overlappingAnchor = input.overlappingAnchor;
        findings->push_back(std::move(finding));
    }
};

class InsideLiquidityAnalyzer final : public Phase3Analyzer {
public:
    const char* name() const override { return "inside_liquidity"; }
    AnalyzerLane lane() const override { return AnalyzerLane::Hot; }

    void analyzeHot(const HotAnalyzerInput& input,
                    std::vector<AnalyzerFindingSpec>* findings) const override {
        if (input.event.eventKind != "market_depth" || !input.hadInside || !input.hasInside) {
            return;
        }

        auto maybeRecord = [&](const std::string& kind,
                               const std::string& sideLabel,
                               double previousSize,
                               double currentSize,
                               double price,
                               bool refill) {
            if (previousSize < kLiquidityChangeMinShares || currentSize <= 0.0 || price <= 0.0) {
                return;
            }
            const double delta = currentSize - previousSize;
            if (!refill && delta > -kLiquidityChangeMinShares) {
                return;
            }
            if (refill && delta < kLiquidityChangeMinShares) {
                return;
            }
            const double ratio = std::fabs(delta) / previousSize;
            if (ratio < kLiquidityChangeMinRatio) {
                return;
            }

            std::ostringstream title;
            title << "Inside " << sideLabel << (refill ? " liquidity refilled at " : " liquidity thinned at ")
                  << std::fixed << std::setprecision(2) << price;
            std::ostringstream summary;
            summary << "Inside " << sideLabel << " size moved from "
                    << std::fixed << std::setprecision(0) << previousSize
                    << " to " << currentSize << " at the same inside price.";

            AnalyzerFindingSpec finding;
            finding.kind = kind;
            finding.severity = input.overlapsOrder ? "warning" : "info";
            finding.confidence = refill ? 0.76 : 0.79;
            finding.firstSessionSeq = input.event.sessionSeq;
            finding.lastSessionSeq = input.event.sessionSeq;
            finding.tsEngineNs = input.event.tsEngineNs;
            finding.instrumentId = input.event.instrumentId;
            finding.title = title.str();
            finding.summary = summary.str();
            finding.overlapsOrder = input.overlapsOrder;
            finding.overlappingAnchor = input.overlappingAnchor;
            findings->push_back(std::move(finding));
        };

        if (input.previousAsk > 0.0 && input.effectiveAsk == input.previousAsk) {
            maybeRecord("ask_liquidity_thinned", "ask", input.previousAskSize, input.effectiveAskSize, input.effectiveAsk, false);
            maybeRecord("ask_liquidity_refilled", "ask", input.previousAskSize, input.effectiveAskSize, input.effectiveAsk, true);
        }
        if (input.previousBid > 0.0 && input.effectiveBid == input.previousBid) {
            maybeRecord("bid_liquidity_thinned", "bid", input.previousBidSize, input.effectiveBidSize, input.effectiveBid, false);
            maybeRecord("bid_liquidity_refilled", "bid", input.previousBidSize, input.effectiveBidSize, input.effectiveBid, true);
        }
    }
};

class TradePressureAnalyzer final : public Phase3Analyzer {
public:
    const char* name() const override { return "trade_pressure"; }
    AnalyzerLane lane() const override { return AnalyzerLane::Hot; }

    void analyzeHot(const HotAnalyzerInput& input,
                    std::vector<AnalyzerFindingSpec>* findings) const override {
        if (!input.tradePressureTriggered || input.tradePressureKind.empty()) {
            return;
        }

        const bool buyPressure = input.tradePressureKind == "buy_trade_pressure";
        std::ostringstream title;
        title << (buyPressure ? "Buy-side trade pressure at " : "Sell-side trade pressure at ")
              << std::fixed << std::setprecision(2) << input.tradePressureReferencePrice;

        std::ostringstream summary;
        summary << input.tradePressureStreakCount
                << " consecutive last-trade prints matched the "
                << (buyPressure ? "ask" : "bid")
                << " from session_seq " << input.tradePressureFirstSessionSeq
                << " through " << input.event.sessionSeq << ".";

        AnalyzerFindingSpec finding;
        finding.kind = input.tradePressureKind;
        finding.severity = input.overlapsOrder ? "warning" : "info";
        finding.confidence = input.tradePressureStreakCount >= 4 ? 0.82 : 0.76;
        finding.firstSessionSeq = input.tradePressureFirstSessionSeq;
        finding.lastSessionSeq = input.event.sessionSeq;
        finding.tsEngineNs = input.event.tsEngineNs;
        finding.instrumentId = input.event.instrumentId;
        finding.title = title.str();
        finding.summary = summary.str();
        finding.overlapsOrder = input.overlapsOrder;
        finding.overlappingAnchor = input.overlappingAnchor;
        findings->push_back(std::move(finding));
    }
};

class DisplayInstabilityAnalyzer final : public Phase3Analyzer {
public:
    const char* name() const override { return "display_instability"; }
    AnalyzerLane lane() const override { return AnalyzerLane::Hot; }

    void analyzeHot(const HotAnalyzerInput& input,
                    std::vector<AnalyzerFindingSpec>* findings) const override {
        if (!input.displayInstabilityTriggered || input.displayInstabilityKind.empty()) {
            return;
        }

        const bool askSide = input.displayInstabilityKind == "ask_display_instability";
        std::ostringstream title;
        title << (askSide ? "Ask" : "Bid")
              << " display instability at "
              << std::fixed << std::setprecision(2) << input.displayInstabilityPrice;

        std::ostringstream summary;
        summary << input.displayInstabilityCycles
                << " rapid remove/readd cycles occurred at the same inside "
                << (askSide ? "ask" : "bid")
                << " price from session_seq " << input.displayInstabilityFirstSessionSeq
                << " through " << input.event.sessionSeq << ".";

        AnalyzerFindingSpec finding;
        finding.kind = input.displayInstabilityKind;
        finding.severity = input.overlapsOrder ? "warning" : "info";
        finding.confidence = input.displayInstabilityCycles >= 3 ? 0.84 : 0.79;
        finding.firstSessionSeq = input.displayInstabilityFirstSessionSeq;
        finding.lastSessionSeq = input.event.sessionSeq;
        finding.tsEngineNs = input.event.tsEngineNs;
        finding.instrumentId = input.event.instrumentId;
        finding.title = title.str();
        finding.summary = summary.str();
        finding.overlapsOrder = input.overlapsOrder;
        finding.overlappingAnchor = input.overlappingAnchor;
        findings->push_back(std::move(finding));
    }
};

class FillInvalidationAnalyzer final : public Phase3Analyzer {
public:
    const char* name() const override { return "fill_invalidation"; }
    AnalyzerLane lane() const override { return AnalyzerLane::Hot; }

    void analyzeHot(const HotAnalyzerInput& input,
                    std::vector<AnalyzerFindingSpec>* findings) const override {
        if (!input.fillInvalidationTriggered || input.fillInvalidationKind.empty()) {
            return;
        }

        const bool buySide = input.fillInvalidationKind == "buy_fill_invalidation";
        std::ostringstream title;
        title << (buySide ? "Buy fill invalidated below " : "Sell fill invalidated above ")
              << std::fixed << std::setprecision(2) << input.fillInvalidationObservedPrice;

        std::ostringstream summary;
        summary << "After the "
                << (buySide ? "buy" : "sell")
                << " fill at " << std::fixed << std::setprecision(2) << input.fillInvalidationFillPrice
                << ", the inside "
                << (buySide ? "bid" : "ask")
                << " moved to " << input.fillInvalidationObservedPrice
                << " by session_seq " << input.event.sessionSeq << ".";

        AnalyzerFindingSpec finding;
        finding.kind = input.fillInvalidationKind;
        finding.severity = "warning";
        finding.confidence = 0.86;
        finding.firstSessionSeq = input.fillInvalidationFirstSessionSeq;
        finding.lastSessionSeq = input.event.sessionSeq;
        finding.tsEngineNs = input.event.tsEngineNs;
        finding.instrumentId = input.event.instrumentId;
        finding.title = title.str();
        finding.summary = summary.str();
        finding.overlapsOrder = true;
        finding.overlappingAnchor = input.overlappingAnchor;
        findings->push_back(std::move(finding));
    }
};

class PullFollowThroughAnalyzer final : public Phase3Analyzer {
public:
    const char* name() const override { return "pull_follow_through"; }
    AnalyzerLane lane() const override { return AnalyzerLane::Hot; }

    void analyzeHot(const HotAnalyzerInput& input,
                    std::vector<AnalyzerFindingSpec>* findings) const override {
        if (!input.pullFollowThroughTriggered || input.pullFollowThroughKind.empty()) {
            return;
        }

        const bool askSide = input.pullFollowThroughKind == "ask_pull_follow_through";
        std::ostringstream title;
        title << (askSide ? "Ask pull followed through to " : "Bid pull followed through to ")
              << std::fixed << std::setprecision(2) << input.pullFollowThroughObservedPrice;

        std::ostringstream summary;
        summary << "After inside " << (askSide ? "ask" : "bid")
                << " liquidity pulled at " << std::fixed << std::setprecision(2)
                << input.pullFollowThroughReferencePrice
                << ", the touch moved to " << input.pullFollowThroughObservedPrice
                << " by session_seq " << input.event.sessionSeq << ".";

        AnalyzerFindingSpec finding;
        finding.kind = input.pullFollowThroughKind;
        finding.severity = input.overlapsOrder ? "warning" : "info";
        finding.confidence = 0.84;
        finding.firstSessionSeq = input.pullFollowThroughFirstSessionSeq;
        finding.lastSessionSeq = input.event.sessionSeq;
        finding.tsEngineNs = input.event.tsEngineNs;
        finding.instrumentId = input.event.instrumentId;
        finding.title = title.str();
        finding.summary = summary.str();
        finding.overlapsOrder = input.overlapsOrder;
        finding.overlappingAnchor = input.overlappingAnchor;
        findings->push_back(std::move(finding));
    }
};

class QuoteFlickerAnalyzer final : public Phase3Analyzer {
public:
    const char* name() const override { return "quote_flicker"; }
    AnalyzerLane lane() const override { return AnalyzerLane::Hot; }

    void analyzeHot(const HotAnalyzerInput& input,
                    std::vector<AnalyzerFindingSpec>* findings) const override {
        if (!input.quoteFlickerTriggered || input.quoteFlickerKind.empty()) {
            return;
        }

        const bool askSide = input.quoteFlickerKind == "ask_quote_flicker";
        std::ostringstream title;
        title << (askSide ? "Ask" : "Bid")
              << " quote flicker at "
              << std::fixed << std::setprecision(2) << input.quoteFlickerObservedPrice;

        std::ostringstream summary;
        summary << input.quoteFlickerChangeCount
                << " inside " << (askSide ? "ask" : "bid")
                << " price changes occurred from session_seq "
                << input.quoteFlickerFirstSessionSeq
                << " through " << input.event.sessionSeq
                << ", indicating unstable touch repricing.";

        AnalyzerFindingSpec finding;
        finding.kind = input.quoteFlickerKind;
        finding.severity = input.overlapsOrder ? "warning" : "info";
        finding.confidence = input.quoteFlickerChangeCount >= 4 ? 0.83 : 0.77;
        finding.firstSessionSeq = input.quoteFlickerFirstSessionSeq;
        finding.lastSessionSeq = input.event.sessionSeq;
        finding.tsEngineNs = input.event.tsEngineNs;
        finding.instrumentId = input.event.instrumentId;
        finding.title = title.str();
        finding.summary = summary.str();
        finding.overlapsOrder = input.overlapsOrder;
        finding.overlappingAnchor = input.overlappingAnchor;
        findings->push_back(std::move(finding));
    }
};

class TradeAfterDepletionAnalyzer final : public Phase3Analyzer {
public:
    const char* name() const override { return "trade_after_depletion"; }
    AnalyzerLane lane() const override { return AnalyzerLane::Hot; }

    void analyzeHot(const HotAnalyzerInput& input,
                    std::vector<AnalyzerFindingSpec>* findings) const override {
        if (!input.tradeAfterDepletionTriggered || input.tradeAfterDepletionKind.empty()) {
            return;
        }

        const bool askSide = input.tradeAfterDepletionKind == "ask_trade_after_depletion";
        std::ostringstream title;
        title << (askSide ? "Trade lifted depleted ask at " : "Trade hit depleted bid at ")
              << std::fixed << std::setprecision(2) << input.tradeAfterDepletionTradePrice;

        std::ostringstream summary;
        summary << "After inside " << (askSide ? "ask" : "bid")
                << " liquidity thinned at " << std::fixed << std::setprecision(2)
                << input.tradeAfterDepletionReferencePrice
                << ", a trade printed at " << input.tradeAfterDepletionTradePrice
                << " by session_seq " << input.event.sessionSeq << ".";

        AnalyzerFindingSpec finding;
        finding.kind = input.tradeAfterDepletionKind;
        finding.severity = input.overlapsOrder ? "warning" : "info";
        finding.confidence = 0.82;
        finding.firstSessionSeq = input.tradeAfterDepletionFirstSessionSeq;
        finding.lastSessionSeq = input.event.sessionSeq;
        finding.tsEngineNs = input.event.tsEngineNs;
        finding.instrumentId = input.event.instrumentId;
        finding.title = title.str();
        finding.summary = summary.str();
        finding.overlapsOrder = input.overlapsOrder;
        finding.overlappingAnchor = input.overlappingAnchor;
        findings->push_back(std::move(finding));
    }
};

class AbsorptionPersistenceAnalyzer final : public Phase3Analyzer {
public:
    const char* name() const override { return "absorption_persistence"; }
    AnalyzerLane lane() const override { return AnalyzerLane::Hot; }

    void analyzeHot(const HotAnalyzerInput& input,
                    std::vector<AnalyzerFindingSpec>* findings) const override {
        if (!input.absorptionPersistenceTriggered || input.absorptionPersistenceKind.empty()) {
            return;
        }

        const bool askSide = input.absorptionPersistenceKind == "ask_absorption_persistence";
        std::ostringstream title;
        title << (askSide ? "Ask" : "Bid")
              << " refill persisted at "
              << std::fixed << std::setprecision(2) << input.absorptionPersistencePrice;

        std::ostringstream summary;
        summary << "Inside " << (askSide ? "ask" : "bid")
                << " liquidity refilled and held for "
                << input.absorptionPersistenceStableUpdates
                << " follow-on updates";
        if (input.absorptionPersistenceTouchTrades > 0) {
            summary << " while " << input.absorptionPersistenceTouchTrades
                    << " touch-aligned trade"
                    << (input.absorptionPersistenceTouchTrades == 1 ? "" : "s")
                    << " printed";
        }
        summary << " from session_seq " << input.absorptionPersistenceFirstSessionSeq
                << " through " << input.event.sessionSeq << ".";

        AnalyzerFindingSpec finding;
        finding.kind = input.absorptionPersistenceKind;
        finding.severity = input.overlapsOrder ? "warning" : "info";
        finding.confidence = input.absorptionPersistenceTouchTrades > 0 ? 0.86 : 0.8;
        finding.firstSessionSeq = input.absorptionPersistenceFirstSessionSeq;
        finding.lastSessionSeq = input.event.sessionSeq;
        finding.tsEngineNs = input.event.tsEngineNs;
        finding.instrumentId = input.event.instrumentId;
        finding.title = title.str();
        finding.summary = summary.str();
        finding.overlapsOrder = input.overlapsOrder;
        finding.overlappingAnchor = input.overlappingAnchor;
        findings->push_back(std::move(finding));
    }
};

class GenuineRefillAnalyzer final : public Phase3Analyzer {
public:
    const char* name() const override { return "genuine_refill"; }
    AnalyzerLane lane() const override { return AnalyzerLane::Hot; }

    void analyzeHot(const HotAnalyzerInput& input,
                    std::vector<AnalyzerFindingSpec>* findings) const override {
        if (!input.genuineRefillTriggered || input.genuineRefillKind.empty()) {
            return;
        }

        const bool askSide = input.genuineRefillKind == "ask_genuine_refill";
        AnalyzerFindingSpec finding;
        finding.kind = input.genuineRefillKind;
        finding.severity = input.overlapsOrder ? "warning" : "info";
        finding.confidence = 0.79;
        finding.firstSessionSeq = input.genuineRefillFirstSessionSeq;
        finding.lastSessionSeq = input.event.sessionSeq;
        finding.tsEngineNs = input.event.tsEngineNs;
        finding.instrumentId = input.event.instrumentId;
        finding.overlapsOrder = input.overlapsOrder;
        finding.overlappingAnchor = input.overlappingAnchor;

        std::ostringstream title;
        title << (askSide ? "Ask" : "Bid") << " refill held without confirming absorption at "
              << std::fixed << std::setprecision(2) << input.genuineRefillPrice;
        finding.title = title.str();

        std::ostringstream summary;
        summary << "Inside " << (askSide ? "ask" : "bid")
                << " liquidity refilled at " << std::fixed << std::setprecision(2)
                << input.genuineRefillPrice
                << " and held for " << input.genuineRefillStableUpdates
                << " follow-on updates without touch-aligned trades. "
                << "That looks more like a genuine displayed refill than an absorption event.";
        finding.summary = summary.str();
        findings->push_back(std::move(finding));
    }
};

class DepletionAfterTradeAnalyzer final : public Phase3Analyzer {
public:
    const char* name() const override { return "depletion_after_trade"; }
    AnalyzerLane lane() const override { return AnalyzerLane::Hot; }

    void analyzeHot(const HotAnalyzerInput& input,
                    std::vector<AnalyzerFindingSpec>* findings) const override {
        if (!input.depletionAfterTradeTriggered || input.depletionAfterTradeKind.empty()) {
            return;
        }

        const bool askSide = input.depletionAfterTradeKind == "ask_depletion_after_trade";
        AnalyzerFindingSpec finding;
        finding.kind = input.depletionAfterTradeKind;
        finding.severity = input.overlapsOrder ? "warning" : "info";
        finding.confidence = 0.81;
        finding.firstSessionSeq = input.depletionAfterTradeFirstSessionSeq;
        finding.lastSessionSeq = input.event.sessionSeq;
        finding.tsEngineNs = input.event.tsEngineNs;
        finding.instrumentId = input.event.instrumentId;
        finding.overlapsOrder = input.overlapsOrder;
        finding.overlappingAnchor = input.overlappingAnchor;

        std::ostringstream title;
        title << (askSide ? "Ask" : "Bid") << " depleted after touch trade at "
              << std::fixed << std::setprecision(2) << input.depletionAfterTradeTradePrice;
        finding.title = title.str();

        std::ostringstream summary;
        summary << "A touch-aligned trade printed at " << std::fixed << std::setprecision(2)
                << input.depletionAfterTradeTradePrice
                << ", then displayed " << (askSide ? "ask" : "bid")
                << " size depleted to " << input.depletionAfterTradeRemainingSize
                << " by session_seq " << input.event.sessionSeq
                << ". This is the opposite ordering from trade-after-depletion and can point to queue drain or follow-on impact after the trade.";
        finding.summary = summary.str();
        findings->push_back(std::move(finding));
    }
};

class OrderFlowTimelineAnalyzer final : public Phase3Analyzer {
public:
    const char* name() const override { return "order_flow_timeline"; }
    AnalyzerLane lane() const override { return AnalyzerLane::Deferred; }

    void analyzeDeferred(const DeferredAnalyzerInput& input,
                         std::vector<AnalyzerFindingSpec>* findings) const override {
        if (!hasAnchorIdentity(input.protectedWindow.anchor) || input.windowEvents.empty()) {
            return;
        }

        std::size_t orderLifecycleCount = 0;
        std::size_t fillCount = 0;
        std::size_t errorCount = 0;
        std::size_t statusCount = 0;
        std::uint64_t firstSessionSeq = 0;
        std::uint64_t lastSessionSeq = 0;
        std::uint64_t tsEngineNs = 0;

        for (const auto& event : input.windowEvents) {
            const std::string kind = event.value("event_kind", std::string());
            const std::uint64_t sessionSeq = event.value("session_seq", 0ULL);
            if (firstSessionSeq == 0 || sessionSeq < firstSessionSeq) {
                firstSessionSeq = sessionSeq;
            }
            lastSessionSeq = std::max(lastSessionSeq, sessionSeq);
            tsEngineNs = std::max(tsEngineNs, event.value("ts_engine_ns", 0ULL));

            if (kind == "fill_execution") {
                ++fillCount;
            }
            if (kind == "order_intent" || kind == "open_order" || kind == "order_status" ||
                kind == "cancel_request" || kind == "order_reject" || kind == "broker_error") {
                ++orderLifecycleCount;
            }
            if (kind == "order_status" || kind == "open_order") {
                ++statusCount;
            }
            if (kind == "order_reject" || kind == "broker_error") {
                ++errorCount;
            }
        }

        if (orderLifecycleCount == 0 && fillCount == 0) {
            return;
        }

        AnalyzerFindingSpec finding;
        finding.kind = "order_flow_timeline";
        finding.severity = errorCount > 0 ? "warning" : "info";
        finding.confidence = errorCount > 0 ? 0.82 : (fillCount > 0 ? 0.78 : 0.72);
        finding.firstSessionSeq = firstSessionSeq;
        finding.lastSessionSeq = lastSessionSeq;
        finding.tsEngineNs = tsEngineNs;
        finding.instrumentId = input.protectedWindow.instrumentId;
        std::ostringstream title;
        title << "Order window captured " << orderLifecycleCount << " lifecycle events";
        if (fillCount > 0) {
            title << " and " << fillCount << " fills";
        }
        finding.title = title.str();
        std::ostringstream summary;
        summary << "Protected order window " << input.protectedWindow.windowId
                << " spans session_seq " << firstSessionSeq << " to " << lastSessionSeq
                << " with " << orderLifecycleCount << " order lifecycle events, "
                << fillCount << " fills, and " << statusCount << " order-status/open-order updates.";
        if (errorCount > 0) {
            summary << " The window also contains " << errorCount << " error/reject events.";
        }
        finding.summary = summary.str();
        finding.overlapsOrder = true;
        finding.overlappingAnchor = input.protectedWindow.anchor;
        findings->push_back(std::move(finding));
    }
};

class OrderFillContextAnalyzer final : public Phase3Analyzer {
public:
    const char* name() const override { return "order_fill_context"; }
    AnalyzerLane lane() const override { return AnalyzerLane::Deferred; }

    void analyzeDeferred(const DeferredAnalyzerInput& input,
                         std::vector<AnalyzerFindingSpec>* findings) const override {
        if (!hasAnchorIdentity(input.protectedWindow.anchor) || input.windowEvents.empty()) {
            return;
        }

        std::size_t fillCount = 0;
        std::size_t marketEventCount = 0;
        std::size_t errorCount = 0;
        std::uint64_t firstSessionSeq = 0;
        std::uint64_t lastSessionSeq = 0;
        std::uint64_t tsEngineNs = 0;
        double firstFillPrice = std::numeric_limits<double>::quiet_NaN();
        double lastFillPrice = std::numeric_limits<double>::quiet_NaN();
        double bestBidLow = std::numeric_limits<double>::quiet_NaN();
        double bestBidHigh = std::numeric_limits<double>::quiet_NaN();
        double bestAskLow = std::numeric_limits<double>::quiet_NaN();
        double bestAskHigh = std::numeric_limits<double>::quiet_NaN();
        double firstMid = std::numeric_limits<double>::quiet_NaN();
        double lastMid = std::numeric_limits<double>::quiet_NaN();
        std::optional<bool> firstFillIsBuy;
        double adverseExcursion = 0.0;
        double favorableExcursion = 0.0;

        for (const auto& event : input.windowEvents) {
            const std::string kind = event.value("event_kind", std::string());
            const std::uint64_t sessionSeq = event.value("session_seq", 0ULL);
            if (firstSessionSeq == 0 || sessionSeq < firstSessionSeq) {
                firstSessionSeq = sessionSeq;
            }
            lastSessionSeq = std::max(lastSessionSeq, sessionSeq);
            tsEngineNs = std::max(tsEngineNs, event.value("ts_engine_ns", 0ULL));

            if (kind == "fill_execution") {
                ++fillCount;
                const double fillPrice = numberOrDefault(event, "price", std::numeric_limits<double>::quiet_NaN());
                if (std::isfinite(fillPrice)) {
                    if (!std::isfinite(firstFillPrice)) {
                        firstFillPrice = fillPrice;
                        const std::string side = event.value("side", std::string());
                        if (side == "BOT" || side == "BUY") {
                            firstFillIsBuy = true;
                        } else if (side == "SLD" || side == "SELL") {
                            firstFillIsBuy = false;
                        }
                    }
                    lastFillPrice = fillPrice;
                }
            } else if (kind == "market_tick" || kind == "market_depth") {
                ++marketEventCount;
            } else if (kind == "order_reject" || kind == "broker_error") {
                ++errorCount;
            }

            const int marketField = event.value("market_field", -1);
            const double price = numberOrDefault(event, "price", std::numeric_limits<double>::quiet_NaN());
            if (!std::isfinite(price)) {
                continue;
            }
            if (kind == "market_tick" && marketField == 1) {
                bestBidLow = std::isfinite(bestBidLow) ? std::min(bestBidLow, price) : price;
                bestBidHigh = std::isfinite(bestBidHigh) ? std::max(bestBidHigh, price) : price;
            } else if (kind == "market_tick" && marketField == 2) {
                bestAskLow = std::isfinite(bestAskLow) ? std::min(bestAskLow, price) : price;
                bestAskHigh = std::isfinite(bestAskHigh) ? std::max(bestAskHigh, price) : price;
            }
            if (std::isfinite(bestBidLow) && std::isfinite(bestAskLow)) {
                const double mid = (bestBidLow + bestAskLow) * 0.5;
                if (!std::isfinite(firstMid)) {
                    firstMid = mid;
                }
                lastMid = mid;
            }
        }

        if (fillCount > 0 && std::isfinite(firstFillPrice) && firstFillIsBuy.has_value()) {
            if (*firstFillIsBuy) {
                if (std::isfinite(bestBidLow)) {
                    adverseExcursion = std::max(0.0, firstFillPrice - bestBidLow);
                }
                if (std::isfinite(bestAskHigh)) {
                    favorableExcursion = std::max(0.0, bestAskHigh - firstFillPrice);
                }
            } else {
                if (std::isfinite(bestAskHigh)) {
                    adverseExcursion = std::max(0.0, bestAskHigh - firstFillPrice);
                }
                if (std::isfinite(bestBidLow)) {
                    favorableExcursion = std::max(0.0, firstFillPrice - bestBidLow);
                }
            }
        }

        if (fillCount == 0 && marketEventCount == 0) {
            return;
        }

        AnalyzerFindingSpec finding;
        finding.kind = "order_fill_context";
        finding.severity = (errorCount > 0 || adverseExcursion >= kMarketImpactMinMidMove)
            ? "warning"
            : (fillCount > 0 ? "info" : "warning");
        finding.confidence = fillCount > 0 && std::isfinite(firstMid) && std::isfinite(lastMid) ? 0.88
            : (fillCount > 0 ? 0.84 : 0.73);
        finding.firstSessionSeq = firstSessionSeq;
        finding.lastSessionSeq = lastSessionSeq;
        finding.tsEngineNs = tsEngineNs;
        finding.instrumentId = input.protectedWindow.instrumentId;
        finding.overlapsOrder = true;
        finding.overlappingAnchor = input.protectedWindow.anchor;

        std::ostringstream title;
        title << "Order/fill context captured " << marketEventCount << " market updates";
        if (fillCount > 0) {
            title << " around " << fillCount << " fills";
        }
        finding.title = title.str();

        std::ostringstream summary;
        summary << "Protected order window " << input.protectedWindow.windowId
                << " spans session_seq " << firstSessionSeq << " to " << lastSessionSeq
                << " with " << marketEventCount << " market updates";
        if (fillCount > 0) {
            summary << ", " << fillCount << " fills";
            if (std::isfinite(firstFillPrice)) {
                summary << ", first fill " << std::fixed << std::setprecision(2) << firstFillPrice;
            }
            if (std::isfinite(lastFillPrice) &&
                (!std::isfinite(firstFillPrice) || std::fabs(lastFillPrice - firstFillPrice) > kTouchTradeTolerance)) {
                summary << ", last fill " << std::fixed << std::setprecision(2) << lastFillPrice;
            }
        }
        if (std::isfinite(bestBidLow) && std::isfinite(bestBidHigh)) {
            summary << ", bid range " << std::fixed << std::setprecision(2) << bestBidLow
                    << "-" << bestBidHigh;
        }
        if (std::isfinite(bestAskLow) && std::isfinite(bestAskHigh)) {
            summary << ", ask range " << std::fixed << std::setprecision(2) << bestAskLow
                    << "-" << bestAskHigh;
        }
        if (std::isfinite(firstMid) && std::isfinite(lastMid)) {
            summary << ", mid " << std::fixed << std::setprecision(2) << firstMid
                    << " -> " << lastMid;
        }
        if (fillCount > 0 && std::isfinite(firstFillPrice)) {
            summary << ", adverse excursion " << std::fixed << std::setprecision(2) << adverseExcursion
                    << ", favorable excursion " << favorableExcursion;
        }
        if (errorCount > 0) {
            summary << ". The window also contains " << errorCount << " error/reject events.";
        } else {
            summary << '.';
        }
        finding.summary = summary.str();
        findings->push_back(std::move(finding));
    }
};

class OrderWindowMarketImpactAnalyzer final : public Phase3Analyzer {
public:
    const char* name() const override { return "order_window_market_impact"; }
    AnalyzerLane lane() const override { return AnalyzerLane::Deferred; }

    void analyzeDeferred(const DeferredAnalyzerInput& input,
                         std::vector<AnalyzerFindingSpec>* findings) const override {
        if (!hasAnchorIdentity(input.protectedWindow.anchor) || input.windowEvents.empty()) {
            return;
        }

        std::uint64_t firstSessionSeq = 0;
        std::uint64_t lastSessionSeq = 0;
        std::uint64_t tsEngineNs = 0;
        double firstBid = std::numeric_limits<double>::quiet_NaN();
        double firstAsk = std::numeric_limits<double>::quiet_NaN();
        double lastBid = std::numeric_limits<double>::quiet_NaN();
        double lastAsk = std::numeric_limits<double>::quiet_NaN();
        double firstFillPrice = std::numeric_limits<double>::quiet_NaN();
        std::string firstFillSide;
        std::size_t fillCount = 0;

        for (const auto& event : input.windowEvents) {
            const std::string kind = event.value("event_kind", std::string());
            const std::uint64_t sessionSeq = event.value("session_seq", 0ULL);
            if (firstSessionSeq == 0 || sessionSeq < firstSessionSeq) {
                firstSessionSeq = sessionSeq;
            }
            lastSessionSeq = std::max(lastSessionSeq, sessionSeq);
            tsEngineNs = std::max(tsEngineNs, event.value("ts_engine_ns", 0ULL));

            if (kind == "market_tick") {
                const int marketField = event.value("market_field", -1);
                const double price = numberOrDefault(event, "price", std::numeric_limits<double>::quiet_NaN());
                if (!std::isfinite(price)) {
                    continue;
                }
                if (marketField == 1) {
                    if (!std::isfinite(firstBid)) {
                        firstBid = price;
                    }
                    lastBid = price;
                } else if (marketField == 2) {
                    if (!std::isfinite(firstAsk)) {
                        firstAsk = price;
                    }
                    lastAsk = price;
                }
            } else if (kind == "fill_execution") {
                ++fillCount;
                const double fillPrice = numberOrDefault(event, "price", std::numeric_limits<double>::quiet_NaN());
                if (!std::isfinite(firstFillPrice) && std::isfinite(fillPrice)) {
                    firstFillPrice = fillPrice;
                    firstFillSide = event.value("side", std::string());
                }
            }
        }

        const bool hasInitialInside = std::isfinite(firstBid) && std::isfinite(firstAsk) && firstAsk >= firstBid;
        const bool hasFinalInside = std::isfinite(lastBid) && std::isfinite(lastAsk) && lastAsk >= lastBid;
        if (!hasInitialInside && !hasFinalInside) {
            return;
        }

        const double firstMid = hasInitialInside ? (firstBid + firstAsk) * 0.5 : std::numeric_limits<double>::quiet_NaN();
        const double lastMid = hasFinalInside ? (lastBid + lastAsk) * 0.5 : std::numeric_limits<double>::quiet_NaN();
        const double firstSpread = hasInitialInside ? (firstAsk - firstBid) : std::numeric_limits<double>::quiet_NaN();
        const double lastSpread = hasFinalInside ? (lastAsk - lastBid) : std::numeric_limits<double>::quiet_NaN();
        const double midMove = (std::isfinite(firstMid) && std::isfinite(lastMid)) ? (lastMid - firstMid) : 0.0;
        const double spreadChange = (std::isfinite(firstSpread) && std::isfinite(lastSpread)) ? (lastSpread - firstSpread) : 0.0;

        bool adverseMove = false;
        if (std::isfinite(firstFillPrice)) {
            if (firstFillSide == "BOT" || firstFillSide == "BUY") {
                adverseMove = std::isfinite(lastMid) && lastMid <= firstFillPrice - kMarketImpactMinMidMove;
            } else if (firstFillSide == "SLD" || firstFillSide == "SELL") {
                adverseMove = std::isfinite(lastMid) && lastMid >= firstFillPrice + kMarketImpactMinMidMove;
            }
        }

        if (!adverseMove && spreadChange < 0.01 && std::fabs(midMove) < kMarketImpactMinMidMove) {
            return;
        }

        AnalyzerFindingSpec finding;
        finding.kind = "order_window_market_impact";
        finding.severity = adverseMove ? "warning" : "info";
        finding.confidence = adverseMove ? 0.85 : 0.76;
        finding.firstSessionSeq = firstSessionSeq;
        finding.lastSessionSeq = lastSessionSeq;
        finding.tsEngineNs = tsEngineNs;
        finding.instrumentId = input.protectedWindow.instrumentId;
        finding.overlapsOrder = true;
        finding.overlappingAnchor = input.protectedWindow.anchor;

        std::ostringstream title;
        if (adverseMove) {
            title << "Order window moved against the fill";
        } else {
            title << "Order window ended with changed market structure";
        }
        finding.title = title.str();

        std::ostringstream summary;
        summary << "Protected order window " << input.protectedWindow.windowId
                << " spans session_seq " << firstSessionSeq << " to " << lastSessionSeq;
        if (std::isfinite(firstMid) && std::isfinite(lastMid)) {
            summary << ", mid " << std::fixed << std::setprecision(2) << firstMid
                    << " -> " << lastMid;
        }
        if (std::isfinite(firstSpread) && std::isfinite(lastSpread)) {
            summary << ", spread " << std::fixed << std::setprecision(2) << firstSpread
                    << " -> " << lastSpread;
        }
        if (fillCount > 0 && std::isfinite(firstFillPrice)) {
            summary << ", first fill " << std::fixed << std::setprecision(2) << firstFillPrice;
        }
        summary << '.';
        finding.summary = summary.str();
        findings->push_back(std::move(finding));
    }
};

class PassiveFillQueueProxyAnalyzer final : public Phase3Analyzer {
public:
    const char* name() const override { return "passive_fill_queue_proxy"; }
    AnalyzerLane lane() const override { return AnalyzerLane::Deferred; }

    void analyzeDeferred(const DeferredAnalyzerInput& input,
                         std::vector<AnalyzerFindingSpec>* findings) const override {
        if (!hasAnchorIdentity(input.protectedWindow.anchor) || input.windowEvents.empty()) {
            return;
        }

        std::size_t fillCount = 0;
        std::size_t touchStableUpdates = 0;
        std::size_t refillEvents = 0;
        std::uint64_t firstSessionSeq = 0;
        std::uint64_t lastSessionSeq = 0;
        std::uint64_t tsEngineNs = 0;
        double fillPrice = std::numeric_limits<double>::quiet_NaN();
        std::string fillSide;
        double touchPrice = std::numeric_limits<double>::quiet_NaN();
        double touchSizeBeforeFill = std::numeric_limits<double>::quiet_NaN();
        double touchSizeAfterFill = std::numeric_limits<double>::quiet_NaN();
        double bestBid = std::numeric_limits<double>::quiet_NaN();
        double bestAsk = std::numeric_limits<double>::quiet_NaN();

        for (const auto& event : input.windowEvents) {
            const std::string kind = event.value("event_kind", std::string());
            const std::uint64_t sessionSeq = event.value("session_seq", 0ULL);
            if (firstSessionSeq == 0 || sessionSeq < firstSessionSeq) {
                firstSessionSeq = sessionSeq;
            }
            lastSessionSeq = std::max(lastSessionSeq, sessionSeq);
            tsEngineNs = std::max(tsEngineNs, event.value("ts_engine_ns", 0ULL));

            if (kind == "market_tick") {
                const int marketField = event.value("market_field", -1);
                const double price = numberOrDefault(event, "price", std::numeric_limits<double>::quiet_NaN());
                if (marketField == 1 && std::isfinite(price)) {
                    bestBid = price;
                } else if (marketField == 2 && std::isfinite(price)) {
                    bestAsk = price;
                }
            } else if (kind == "fill_execution") {
                ++fillCount;
                if (!std::isfinite(fillPrice)) {
                    fillPrice = numberOrDefault(event, "price", std::numeric_limits<double>::quiet_NaN());
                    fillSide = event.value("side", std::string());
                    if (fillSide == "BOT" || fillSide == "BUY") {
                        touchPrice = bestBid;
                    } else if (fillSide == "SLD" || fillSide == "SELL") {
                        touchPrice = bestAsk;
                    }
                }
            } else if (kind == "market_depth") {
                const double price = numberOrDefault(event, "price", std::numeric_limits<double>::quiet_NaN());
                const double size = numberOrDefault(event, "size", std::numeric_limits<double>::quiet_NaN());
                if (!std::isfinite(touchPrice) || !std::isfinite(price) || std::fabs(price - touchPrice) > kTouchTradeTolerance) {
                    continue;
                }
                if (!std::isfinite(touchSizeBeforeFill) && fillCount == 0) {
                    touchSizeBeforeFill = size;
                } else if (fillCount > 0) {
                    if (!std::isfinite(touchSizeAfterFill)) {
                        touchSizeAfterFill = size;
                    }
                    ++touchStableUpdates;
                    if (std::isfinite(touchSizeBeforeFill) && size >= touchSizeBeforeFill * 0.8) {
                        ++refillEvents;
                    }
                }
            }
        }

        if (fillCount == 0 || !std::isfinite(fillPrice)) {
            return;
        }

        const bool passiveContextLooksStrong = touchStableUpdates >= 2 && refillEvents >= 1;
        const double queueProxyScore = passiveContextLooksStrong ? 0.84 : (touchStableUpdates > 0 ? 0.71 : 0.58);

        AnalyzerFindingSpec finding;
        finding.kind = "passive_fill_queue_proxy";
        finding.severity = passiveContextLooksStrong ? "info" : "warning";
        finding.confidence = queueProxyScore;
        finding.firstSessionSeq = firstSessionSeq;
        finding.lastSessionSeq = lastSessionSeq;
        finding.tsEngineNs = tsEngineNs;
        finding.instrumentId = input.protectedWindow.instrumentId;
        finding.overlapsOrder = true;
        finding.overlappingAnchor = input.protectedWindow.anchor;

        std::ostringstream title;
        title << "Passive-fill queue proxy is " << (passiveContextLooksStrong ? "supportive" : "fragile");
        finding.title = title.str();

        std::ostringstream summary;
        summary << "The fill window captured " << fillCount << " fills around touch price "
                << std::fixed << std::setprecision(2) << touchPrice
                << " with " << touchStableUpdates << " post-fill touch updates";
        if (std::isfinite(touchSizeBeforeFill)) {
            summary << ", touch size before fill " << touchSizeBeforeFill;
        }
        if (std::isfinite(touchSizeAfterFill)) {
            summary << ", first observed post-fill touch size " << touchSizeAfterFill;
        }
        summary << ", and " << refillEvents << " supportive refill-style updates. "
                << "This is only a queue-position proxy, not a true queue estimate.";
        finding.summary = summary.str();
        findings->push_back(std::move(finding));
    }
};

class PostFillAdverseSelectionAnalyzer final : public Phase3Analyzer {
public:
    const char* name() const override { return "post_fill_adverse_selection"; }
    AnalyzerLane lane() const override { return AnalyzerLane::Deferred; }

    void analyzeDeferred(const DeferredAnalyzerInput& input,
                         std::vector<AnalyzerFindingSpec>* findings) const override {
        if (!hasAnchorIdentity(input.protectedWindow.anchor) || input.windowEvents.empty()) {
            return;
        }

        std::optional<bool> firstFillIsBuy;
        std::uint64_t firstSessionSeq = 0;
        std::uint64_t lastSessionSeq = 0;
        std::uint64_t tsEngineNs = 0;
        double fillPrice = std::numeric_limits<double>::quiet_NaN();
        double bestBidLow = std::numeric_limits<double>::quiet_NaN();
        double bestAskHigh = std::numeric_limits<double>::quiet_NaN();
        double firstMid = std::numeric_limits<double>::quiet_NaN();
        double lastMid = std::numeric_limits<double>::quiet_NaN();
        double currentBid = std::numeric_limits<double>::quiet_NaN();
        double currentAsk = std::numeric_limits<double>::quiet_NaN();

        for (const auto& event : input.windowEvents) {
            const std::string kind = event.value("event_kind", std::string());
            const std::uint64_t sessionSeq = event.value("session_seq", 0ULL);
            if (firstSessionSeq == 0 || sessionSeq < firstSessionSeq) {
                firstSessionSeq = sessionSeq;
            }
            lastSessionSeq = std::max(lastSessionSeq, sessionSeq);
            tsEngineNs = std::max(tsEngineNs, event.value("ts_engine_ns", 0ULL));

            if (kind == "fill_execution" && !std::isfinite(fillPrice)) {
                fillPrice = numberOrDefault(event, "price", std::numeric_limits<double>::quiet_NaN());
                const std::string side = event.value("side", std::string());
                if (side == "BOT" || side == "BUY") {
                    firstFillIsBuy = true;
                } else if (side == "SLD" || side == "SELL") {
                    firstFillIsBuy = false;
                }
            } else if (kind == "market_tick") {
                const int marketField = event.value("market_field", -1);
                const double price = numberOrDefault(event, "price", std::numeric_limits<double>::quiet_NaN());
                if (!std::isfinite(price)) {
                    continue;
                }
                if (marketField == 1) {
                    currentBid = price;
                    bestBidLow = std::isfinite(bestBidLow) ? std::min(bestBidLow, price) : price;
                } else if (marketField == 2) {
                    currentAsk = price;
                    bestAskHigh = std::isfinite(bestAskHigh) ? std::max(bestAskHigh, price) : price;
                }
                if (std::isfinite(currentBid) && std::isfinite(currentAsk)) {
                    const double mid = (currentBid + currentAsk) * 0.5;
                    if (!std::isfinite(firstMid)) {
                        firstMid = mid;
                    }
                    lastMid = mid;
                }
            }
        }

        if (!std::isfinite(fillPrice) || !firstFillIsBuy.has_value()) {
            return;
        }

        double adverseMove = 0.0;
        if (*firstFillIsBuy) {
            if (std::isfinite(bestBidLow)) {
                adverseMove = std::max(0.0, fillPrice - bestBidLow);
            }
        } else if (std::isfinite(bestAskHigh)) {
            adverseMove = std::max(0.0, bestAskHigh - fillPrice);
        }

        if (adverseMove < kMarketImpactMinMidMove && (!std::isfinite(firstMid) || !std::isfinite(lastMid))) {
            return;
        }

        AnalyzerFindingSpec finding;
        finding.kind = "post_fill_adverse_selection";
        finding.severity = adverseMove >= kMarketImpactMinMidMove ? "warning" : "info";
        finding.confidence = adverseMove >= kMarketImpactMinMidMove ? 0.89 : 0.78;
        finding.firstSessionSeq = firstSessionSeq;
        finding.lastSessionSeq = lastSessionSeq;
        finding.tsEngineNs = tsEngineNs;
        finding.instrumentId = input.protectedWindow.instrumentId;
        finding.overlapsOrder = true;
        finding.overlappingAnchor = input.protectedWindow.anchor;

        std::ostringstream title;
        title << "Post-fill adverse selection "
              << (adverseMove >= kMarketImpactMinMidMove ? "was detected" : "was limited");
        finding.title = title.str();

        std::ostringstream summary;
        summary << "After the first fill at " << std::fixed << std::setprecision(2) << fillPrice
                << ", the market moved " << adverseMove
                << " against the fill";
        if (std::isfinite(firstMid) && std::isfinite(lastMid)) {
            summary << " with mid " << firstMid << " -> " << lastMid;
        }
        summary << ". This is a fill-specific adverse-selection read, not just a generic order-window impact measure.";
        finding.summary = summary.str();
        findings->push_back(std::move(finding));
    }
};

class PassiveQueueLossAnalyzer final : public Phase3Analyzer {
public:
    const char* name() const override { return "passive_queue_loss"; }
    AnalyzerLane lane() const override { return AnalyzerLane::Deferred; }

    void analyzeDeferred(const DeferredAnalyzerInput& input,
                         std::vector<AnalyzerFindingSpec>* findings) const override {
        if (!hasAnchorIdentity(input.protectedWindow.anchor) || input.windowEvents.empty()) {
            return;
        }

        std::optional<bool> orderIsBuy;
        std::uint64_t firstSessionSeq = 0;
        std::uint64_t lastSessionSeq = 0;
        std::uint64_t tsEngineNs = 0;
        double currentBid = std::numeric_limits<double>::quiet_NaN();
        double currentAsk = std::numeric_limits<double>::quiet_NaN();
        double referencePassivePrice = std::numeric_limits<double>::quiet_NaN();
        double movedAwayPrice = std::numeric_limits<double>::quiet_NaN();
        std::size_t touchTradeCount = 0;
        std::size_t fillCount = 0;
        bool cancelSeen = false;
        bool cutThroughSeen = false;

        for (const auto& event : input.windowEvents) {
            const std::string kind = event.value("event_kind", std::string());
            const std::uint64_t sessionSeq = event.value("session_seq", 0ULL);
            if (firstSessionSeq == 0 || sessionSeq < firstSessionSeq) {
                firstSessionSeq = sessionSeq;
            }
            lastSessionSeq = std::max(lastSessionSeq, sessionSeq);
            tsEngineNs = std::max(tsEngineNs, event.value("ts_engine_ns", 0ULL));

            if (!orderIsBuy.has_value()) {
                orderIsBuy = inferSide(event.value("side", std::string()));
            }

            if (kind == "market_tick") {
                const int field = event.value("market_field", -1);
                const double price = numberOrDefault(event, "price", std::numeric_limits<double>::quiet_NaN());
                if (!std::isfinite(price)) {
                    continue;
                }
                if (field == 1) {
                    currentBid = price;
                    if (!std::isfinite(referencePassivePrice) && orderIsBuy == true) {
                        referencePassivePrice = price;
                    }
                    if (orderIsBuy == true && std::isfinite(referencePassivePrice) &&
                        price < referencePassivePrice - kTouchTradeTolerance) {
                        movedAwayPrice = price;
                    }
                } else if (field == 2) {
                    currentAsk = price;
                    if (!std::isfinite(referencePassivePrice) && orderIsBuy == false) {
                        referencePassivePrice = price;
                    }
                    if (orderIsBuy == false && std::isfinite(referencePassivePrice) &&
                        price > referencePassivePrice + kTouchTradeTolerance) {
                        movedAwayPrice = price;
                    }
                } else if (field == 4 && orderIsBuy.has_value() && std::isfinite(referencePassivePrice)) {
                    const bool touchedPassivePrice = *orderIsBuy
                        ? price <= referencePassivePrice + kTouchTradeTolerance
                        : price >= referencePassivePrice - kTouchTradeTolerance;
                    if (touchedPassivePrice) {
                        ++touchTradeCount;
                    }
                    const bool printedThroughPassivePrice = *orderIsBuy
                        ? price < referencePassivePrice - kTouchTradeTolerance
                        : price > referencePassivePrice + kTouchTradeTolerance;
                    cutThroughSeen = cutThroughSeen || printedThroughPassivePrice;
                }
            } else if (kind == "fill_execution") {
                ++fillCount;
            } else if (kind == "cancel_request") {
                cancelSeen = true;
            } else if (kind == "order_status") {
                const std::string status = event.value("status", std::string());
                cancelSeen = cancelSeen ||
                    status == "Cancelled" ||
                    status == "ApiCancelled" ||
                    status == "Inactive";
            }
        }

        if (!orderIsBuy.has_value() || !std::isfinite(referencePassivePrice) || touchTradeCount == 0 || fillCount > 0) {
            return;
        }

        if (cancelSeen && touchTradeCount >= 2) {
            AnalyzerFindingSpec finding;
            finding.kind = "passive_queue_loss_proxy";
            finding.severity = "warning";
            finding.confidence = touchTradeCount >= 3 ? 0.83 : 0.76;
            finding.firstSessionSeq = firstSessionSeq;
            finding.lastSessionSeq = lastSessionSeq;
            finding.tsEngineNs = tsEngineNs;
            finding.instrumentId = input.protectedWindow.instrumentId;
            finding.overlapsOrder = true;
            finding.overlappingAnchor = input.protectedWindow.anchor;

            finding.title = orderIsBuy == true
                ? "Passive buy likely lost queue position"
                : "Passive sell likely lost queue position";
            std::ostringstream summary;
            summary << touchTradeCount << " touch trades printed at passive price "
                    << std::fixed << std::setprecision(2) << referencePassivePrice
                    << " before the order window canceled without a fill. "
                    << "This is a queue-loss proxy, not a direct queue-position measurement.";
            finding.summary = summary.str();
            findings->push_back(std::move(finding));
        }

        if ((cutThroughSeen || std::isfinite(movedAwayPrice)) && touchTradeCount >= 1) {
            AnalyzerFindingSpec finding;
            finding.kind = "passive_cut_through_proxy";
            finding.severity = "warning";
            finding.confidence = cutThroughSeen ? 0.86 : 0.78;
            finding.firstSessionSeq = firstSessionSeq;
            finding.lastSessionSeq = lastSessionSeq;
            finding.tsEngineNs = tsEngineNs;
            finding.instrumentId = input.protectedWindow.instrumentId;
            finding.overlapsOrder = true;
            finding.overlappingAnchor = input.protectedWindow.anchor;
            finding.title = orderIsBuy == true
                ? "Passive buy was cut through"
                : "Passive sell was cut through";

            std::ostringstream summary;
            summary << "The market traded at passive price "
                    << std::fixed << std::setprecision(2) << referencePassivePrice;
            if (cutThroughSeen) {
                summary << " and then printed through it";
            }
            if (std::isfinite(movedAwayPrice)) {
                summary << ", with the touch later moving to " << movedAwayPrice;
            }
            summary << " without the order window recording a fill.";
            finding.summary = summary.str();
            findings->push_back(std::move(finding));
        }
    }
};

class SweepVsFadeAnalyzer final : public Phase3Analyzer {
public:
    const char* name() const override { return "sweep_vs_fade"; }
    AnalyzerLane lane() const override { return AnalyzerLane::Deferred; }

    void analyzeDeferred(const DeferredAnalyzerInput& input,
                         std::vector<AnalyzerFindingSpec>* findings) const override {
        if (input.windowEvents.empty()) {
            return;
        }

        std::uint64_t firstSessionSeq = 0;
        std::uint64_t lastSessionSeq = 0;
        std::uint64_t tsEngineNs = 0;
        double currentBid = std::numeric_limits<double>::quiet_NaN();
        double currentAsk = std::numeric_limits<double>::quiet_NaN();
        double firstMid = std::numeric_limits<double>::quiet_NaN();
        double lastMid = std::numeric_limits<double>::quiet_NaN();
        std::size_t askTouchTrades = 0;
        std::size_t bidTouchTrades = 0;

        for (const auto& event : input.windowEvents) {
            const std::string kind = event.value("event_kind", std::string());
            const std::uint64_t sessionSeq = event.value("session_seq", 0ULL);
            if (firstSessionSeq == 0 || sessionSeq < firstSessionSeq) {
                firstSessionSeq = sessionSeq;
            }
            lastSessionSeq = std::max(lastSessionSeq, sessionSeq);
            tsEngineNs = std::max(tsEngineNs, event.value("ts_engine_ns", 0ULL));

            if (kind != "market_tick") {
                continue;
            }
            const int field = event.value("market_field", -1);
            const double price = numberOrDefault(event, "price", std::numeric_limits<double>::quiet_NaN());
            if (!std::isfinite(price)) {
                continue;
            }
            if (field == 1) {
                currentBid = price;
            } else if (field == 2) {
                currentAsk = price;
            } else if (field == 4 && std::isfinite(currentBid) && std::isfinite(currentAsk)) {
                if (price >= currentAsk - kTouchTradeTolerance) {
                    ++askTouchTrades;
                }
                if (price <= currentBid + kTouchTradeTolerance) {
                    ++bidTouchTrades;
                }
            }

            if (std::isfinite(currentBid) && std::isfinite(currentAsk)) {
                const double mid = (currentBid + currentAsk) * 0.5;
                if (!std::isfinite(firstMid)) {
                    firstMid = mid;
                }
                lastMid = mid;
            }
        }

        if (!std::isfinite(firstMid) || !std::isfinite(lastMid)) {
            return;
        }
        const double midMove = lastMid - firstMid;
        if (std::fabs(midMove) < kMarketImpactMinMidMove) {
            return;
        }

        const bool upwardMove = midMove > 0.0;
        const std::size_t confirmingTrades = upwardMove ? askTouchTrades : bidTouchTrades;
        AnalyzerFindingSpec finding;
        finding.kind = upwardMove
            ? (confirmingTrades >= 2 ? "buy_sweep_sequence" : "buy_fade_sequence")
            : (confirmingTrades >= 2 ? "sell_sweep_sequence" : "sell_fade_sequence");
        finding.severity = confirmingTrades >= 2 ? "warning" : "info";
        finding.confidence = confirmingTrades >= 2 ? 0.82 : 0.73;
        finding.firstSessionSeq = firstSessionSeq;
        finding.lastSessionSeq = lastSessionSeq;
        finding.tsEngineNs = tsEngineNs;
        finding.instrumentId = input.protectedWindow.instrumentId;
        finding.overlapsOrder = hasAnchorIdentity(input.protectedWindow.anchor);
        finding.overlappingAnchor = input.protectedWindow.anchor;

        if (confirmingTrades >= 2) {
            finding.title = upwardMove ? "Up move behaved like a sweep" : "Down move behaved like a sweep";
        } else {
            finding.title = upwardMove ? "Up move behaved more like a fade" : "Down move behaved more like a fade";
        }

        std::ostringstream summary;
        summary << "Mid moved from " << std::fixed << std::setprecision(2) << firstMid
                << " to " << lastMid << " across the protected window";
        if (upwardMove) {
            summary << " with " << askTouchTrades << " ask-side touch trades";
        } else {
            summary << " with " << bidTouchTrades << " bid-side touch trades";
        }
        summary << ". The sequence is classified as "
                << (confirmingTrades >= 2 ? "trade-confirmed sweep pressure." : "fading liquidity rather than a clean sweep.");
        finding.summary = summary.str();
        findings->push_back(std::move(finding));
    }
};

class FillOutcomeChainAnalyzer final : public Phase3Analyzer {
public:
    const char* name() const override { return "fill_outcome_chain"; }
    AnalyzerLane lane() const override { return AnalyzerLane::Deferred; }

    void analyzeDeferred(const DeferredAnalyzerInput& input,
                         std::vector<AnalyzerFindingSpec>* findings) const override {
        if (!hasAnchorIdentity(input.protectedWindow.anchor) || input.windowEvents.empty()) {
            return;
        }

        std::optional<bool> fillIsBuy;
        std::uint64_t firstSessionSeq = 0;
        std::uint64_t lastSessionSeq = 0;
        std::uint64_t tsEngineNs = 0;
        std::uint64_t firstFillSessionSeq = 0;
        std::uint64_t cancelSessionSeq = 0;
        double fillPrice = std::numeric_limits<double>::quiet_NaN();
        double currentBid = std::numeric_limits<double>::quiet_NaN();
        double currentAsk = std::numeric_limits<double>::quiet_NaN();
        double adversePrice = std::numeric_limits<double>::quiet_NaN();

        for (const auto& event : input.windowEvents) {
            const std::string kind = event.value("event_kind", std::string());
            const std::uint64_t sessionSeq = event.value("session_seq", 0ULL);
            if (firstSessionSeq == 0 || sessionSeq < firstSessionSeq) {
                firstSessionSeq = sessionSeq;
            }
            lastSessionSeq = std::max(lastSessionSeq, sessionSeq);
            tsEngineNs = std::max(tsEngineNs, event.value("ts_engine_ns", 0ULL));

            if (kind == "market_tick") {
                const int field = event.value("market_field", -1);
                const double price = numberOrDefault(event, "price", std::numeric_limits<double>::quiet_NaN());
                if (!std::isfinite(price)) {
                    continue;
                }
                if (field == 1) {
                    currentBid = price;
                } else if (field == 2) {
                    currentAsk = price;
                }
                if (firstFillSessionSeq > 0) {
                    if (fillIsBuy == true && field == 1) {
                        adversePrice = !std::isfinite(adversePrice) ? price : std::min(adversePrice, price);
                    } else if (fillIsBuy == false && field == 2) {
                        adversePrice = !std::isfinite(adversePrice) ? price : std::max(adversePrice, price);
                    }
                }
            } else if (kind == "fill_execution" && !std::isfinite(fillPrice)) {
                fillPrice = numberOrDefault(event, "price", std::numeric_limits<double>::quiet_NaN());
                fillIsBuy = inferSide(event.value("side", std::string()));
                firstFillSessionSeq = sessionSeq;
            } else if (firstFillSessionSeq > 0 && (kind == "cancel_request" || kind == "order_reject")) {
                cancelSessionSeq = sessionSeq;
            } else if (firstFillSessionSeq > 0 && kind == "order_status") {
                const std::string status = event.value("status", std::string());
                if (status == "Cancelled" || status == "ApiCancelled" || status == "Inactive") {
                    cancelSessionSeq = sessionSeq;
                }
            }
        }

        if (!std::isfinite(fillPrice) || !fillIsBuy.has_value()) {
            return;
        }

        if (cancelSessionSeq > firstFillSessionSeq) {
            AnalyzerFindingSpec finding;
            finding.kind = "fill_to_cancel_chain";
            finding.severity = "warning";
            finding.confidence = 0.84;
            finding.firstSessionSeq = firstFillSessionSeq;
            finding.lastSessionSeq = cancelSessionSeq;
            finding.tsEngineNs = tsEngineNs;
            finding.instrumentId = input.protectedWindow.instrumentId;
            finding.overlapsOrder = true;
            finding.overlappingAnchor = input.protectedWindow.anchor;
            finding.title = "Fill-to-cancel chain detected";

            std::ostringstream summary;
            summary << "A fill at " << std::fixed << std::setprecision(2) << fillPrice
                    << " was followed by cancellation activity before the protected window ended.";
            finding.summary = summary.str();
            findings->push_back(std::move(finding));
        }

        double adverseMove = 0.0;
        if (fillIsBuy == true && std::isfinite(adversePrice)) {
            adverseMove = std::max(0.0, fillPrice - adversePrice);
        } else if (fillIsBuy == false && std::isfinite(adversePrice)) {
            adverseMove = std::max(0.0, adversePrice - fillPrice);
        }
        if (adverseMove >= kMarketImpactMinMidMove) {
            AnalyzerFindingSpec finding;
            finding.kind = "fill_to_adverse_move_chain";
            finding.severity = "warning";
            finding.confidence = 0.87;
            finding.firstSessionSeq = firstFillSessionSeq;
            finding.lastSessionSeq = lastSessionSeq;
            finding.tsEngineNs = tsEngineNs;
            finding.instrumentId = input.protectedWindow.instrumentId;
            finding.overlapsOrder = true;
            finding.overlappingAnchor = input.protectedWindow.anchor;
            finding.title = "Fill-to-adverse-move chain detected";

            std::ostringstream summary;
            summary << "After the fill at " << std::fixed << std::setprecision(2) << fillPrice
                    << ", the market moved " << adverseMove << " against the fill";
            if (fillIsBuy == true && std::isfinite(currentBid)) {
                summary << " with the best bid reaching " << adversePrice;
            } else if (fillIsBuy == false && std::isfinite(currentAsk)) {
                summary << " with the best ask reaching " << adversePrice;
            }
            summary << '.';
            finding.summary = summary.str();
            findings->push_back(std::move(finding));
        }
    }
};

} // namespace

struct AnalyzerRuntime::Impl {
    std::vector<std::unique_ptr<Phase3Analyzer>> analyzers;
    std::vector<const Phase3Analyzer*> hotAnalyzers;
    std::vector<const Phase3Analyzer*> deferredAnalyzers;

    void registerAnalyzer(std::unique_ptr<Phase3Analyzer> analyzer) {
        const Phase3Analyzer* raw = analyzer.get();
        if (raw->lane() == AnalyzerLane::Hot) {
            hotAnalyzers.push_back(raw);
        } else {
            deferredAnalyzers.push_back(raw);
        }
        analyzers.push_back(std::move(analyzer));
    }
};

AnalyzerRuntime::AnalyzerRuntime()
    : impl_(std::make_unique<Impl>()) {
    impl_->registerAnalyzer(std::make_unique<SourceContinuityAnalyzer>());
    impl_->registerAnalyzer(std::make_unique<SpreadWideningAnalyzer>());
    impl_->registerAnalyzer(std::make_unique<InsideLiquidityAnalyzer>());
    impl_->registerAnalyzer(std::make_unique<TradePressureAnalyzer>());
    impl_->registerAnalyzer(std::make_unique<DisplayInstabilityAnalyzer>());
    impl_->registerAnalyzer(std::make_unique<FillInvalidationAnalyzer>());
    impl_->registerAnalyzer(std::make_unique<TradeAfterDepletionAnalyzer>());
    impl_->registerAnalyzer(std::make_unique<DepletionAfterTradeAnalyzer>());
    impl_->registerAnalyzer(std::make_unique<AbsorptionPersistenceAnalyzer>());
    impl_->registerAnalyzer(std::make_unique<GenuineRefillAnalyzer>());
    impl_->registerAnalyzer(std::make_unique<PullFollowThroughAnalyzer>());
    impl_->registerAnalyzer(std::make_unique<QuoteFlickerAnalyzer>());
    impl_->registerAnalyzer(std::make_unique<OrderFlowTimelineAnalyzer>());
    impl_->registerAnalyzer(std::make_unique<OrderFillContextAnalyzer>());
    impl_->registerAnalyzer(std::make_unique<PassiveFillQueueProxyAnalyzer>());
    impl_->registerAnalyzer(std::make_unique<PostFillAdverseSelectionAnalyzer>());
    impl_->registerAnalyzer(std::make_unique<PassiveQueueLossAnalyzer>());
    impl_->registerAnalyzer(std::make_unique<SweepVsFadeAnalyzer>());
    impl_->registerAnalyzer(std::make_unique<FillOutcomeChainAnalyzer>());
    impl_->registerAnalyzer(std::make_unique<OrderWindowMarketImpactAnalyzer>());
}

AnalyzerRuntime::~AnalyzerRuntime() = default;

void AnalyzerRuntime::analyzeHot(const HotAnalyzerInput& input,
                                 std::vector<AnalyzerFindingSpec>* findings) const {
    for (const Phase3Analyzer* analyzer : impl_->hotAnalyzers) {
        analyzer->analyzeHot(input, findings);
    }
}

void AnalyzerRuntime::analyzeDeferred(std::string_view analyzerName,
                                      const DeferredAnalyzerInput& input,
                                      std::vector<AnalyzerFindingSpec>* findings) const {
    for (const Phase3Analyzer* analyzer : impl_->deferredAnalyzers) {
        if (!analyzerName.empty() && analyzerName != analyzer->name()) {
            continue;
        }
        analyzer->analyzeDeferred(input, findings);
    }
}

} // namespace tape_engine
