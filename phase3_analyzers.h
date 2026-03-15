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
