#pragma once

#include "tape_engine.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace tape_engine {

enum class AnalyzerLane {
    Hot,
    Deferred,
};

struct AnalyzerFindingSpec {
    std::string kind;
    std::string severity;
    double confidence = 0.0;
    std::uint64_t firstSessionSeq = 0;
    std::uint64_t lastSessionSeq = 0;
    std::uint64_t tsEngineNs = 0;
    std::string instrumentId;
    std::string title;
    std::string summary;
    bool overlapsOrder = false;
    BridgeAnchorIdentity overlappingAnchor;
};

struct HotAnalyzerInput {
    const EngineEvent& event;
    double previousBid = 0.0;
    double previousAsk = 0.0;
    double previousBidSize = 0.0;
    double previousAskSize = 0.0;
    bool hadInside = false;
    double effectiveBid = 0.0;
    double effectiveAsk = 0.0;
    double effectiveBidSize = 0.0;
    double effectiveAskSize = 0.0;
    bool hasInside = false;
    bool overlapsOrder = false;
    BridgeAnchorIdentity overlappingAnchor;
    bool tradePressureTriggered = false;
    std::string tradePressureKind;
    std::size_t tradePressureStreakCount = 0;
    std::uint64_t tradePressureFirstSessionSeq = 0;
    double tradePressureReferencePrice = 0.0;
    bool displayInstabilityTriggered = false;
    std::string displayInstabilityKind;
    std::size_t displayInstabilityCycles = 0;
    std::uint64_t displayInstabilityFirstSessionSeq = 0;
    double displayInstabilityPrice = 0.0;
    bool fillInvalidationTriggered = false;
    std::string fillInvalidationKind;
    std::uint64_t fillInvalidationFirstSessionSeq = 0;
    double fillInvalidationFillPrice = 0.0;
    double fillInvalidationObservedPrice = 0.0;
    bool pullFollowThroughTriggered = false;
    std::string pullFollowThroughKind;
    std::uint64_t pullFollowThroughFirstSessionSeq = 0;
    double pullFollowThroughReferencePrice = 0.0;
    double pullFollowThroughObservedPrice = 0.0;
    bool quoteFlickerTriggered = false;
    std::string quoteFlickerKind;
    std::uint64_t quoteFlickerFirstSessionSeq = 0;
    std::size_t quoteFlickerChangeCount = 0;
    double quoteFlickerObservedPrice = 0.0;
    bool tradeAfterDepletionTriggered = false;
    std::string tradeAfterDepletionKind;
    std::uint64_t tradeAfterDepletionFirstSessionSeq = 0;
    double tradeAfterDepletionReferencePrice = 0.0;
    double tradeAfterDepletionTradePrice = 0.0;
    bool absorptionPersistenceTriggered = false;
    std::string absorptionPersistenceKind;
    std::uint64_t absorptionPersistenceFirstSessionSeq = 0;
    std::size_t absorptionPersistenceStableUpdates = 0;
    std::size_t absorptionPersistenceTouchTrades = 0;
    double absorptionPersistencePrice = 0.0;
};

struct DeferredAnalyzerInput {
    std::uint64_t sourceRevisionId = 0;
    ProtectedWindowRecord protectedWindow;
    std::vector<json> windowEvents;
};

class AnalyzerRuntime {
public:
    AnalyzerRuntime();
    ~AnalyzerRuntime();

    AnalyzerRuntime(const AnalyzerRuntime&) = delete;
    AnalyzerRuntime& operator=(const AnalyzerRuntime&) = delete;

    void analyzeHot(const HotAnalyzerInput& input,
                    std::vector<AnalyzerFindingSpec>* findings) const;
    void analyzeDeferred(std::string_view analyzerName,
                         const DeferredAnalyzerInput& input,
                         std::vector<AnalyzerFindingSpec>* findings) const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace tape_engine
