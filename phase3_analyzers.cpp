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

bool hasAnchorIdentity(const BridgeAnchorIdentity& anchor) {
    return anchor.traceId > 0 || anchor.orderId > 0 || anchor.permId > 0 || !anchor.execId.empty();
}

double numberOrDefault(const json& payload, const char* key, double fallback) {
    return payload.contains(key) && payload[key].is_number() && std::isfinite(payload[key].get<double>())
        ? payload[key].get<double>()
        : fallback;
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
        }

        if (fillCount == 0 && marketEventCount == 0) {
            return;
        }

        AnalyzerFindingSpec finding;
        finding.kind = "order_fill_context";
        finding.severity = errorCount > 0 ? "warning" : (fillCount > 0 ? "info" : "warning");
        finding.confidence = fillCount > 0 ? 0.84 : 0.73;
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
        if (errorCount > 0) {
            summary << ". The window also contains " << errorCount << " error/reject events.";
        } else {
            summary << '.';
        }
        finding.summary = summary.str();
        findings->push_back(std::move(finding));
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
    impl_->registerAnalyzer(std::make_unique<OrderFlowTimelineAnalyzer>());
    impl_->registerAnalyzer(std::make_unique<OrderFillContextAnalyzer>());
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
