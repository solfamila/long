#include "tape_engine.h"
#include "runtime_qos.h"

#include <CommonCrypto/CommonDigest.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <system_error>
#include <tuple>
#include <unordered_set>

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

namespace tape_engine {

namespace {

std::string errnoMessage(const std::string& prefix) {
    return prefix + ": " + std::strerror(errno);
}

std::uint64_t nowEngineNs() {
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
}

std::string sha256Hex(const std::uint8_t* data, std::size_t size) {
    unsigned char digest[CC_SHA256_DIGEST_LENGTH];
    CC_SHA256(data, static_cast<CC_LONG>(size), digest);

    static constexpr char kHex[] = "0123456789abcdef";
    std::string output;
    output.reserve(CC_SHA256_DIGEST_LENGTH * 2);
    for (unsigned char byte : digest) {
        output.push_back(kHex[(byte >> 4) & 0x0fU]);
        output.push_back(kHex[byte & 0x0fU]);
    }
    return output;
}

std::string sha256Hex(const std::string& input) {
    return sha256Hex(reinterpret_cast<const std::uint8_t*>(input.data()), input.size());
}

std::string sha256Hex(const std::vector<std::uint8_t>& input) {
    return sha256Hex(input.data(), input.size());
}

bool hasFiniteNumber(const json& payload, const char* key) {
    return payload.contains(key) && payload[key].is_number() && std::isfinite(payload[key].get<double>());
}

double numberOrDefault(const json& payload, const char* key, double fallback) {
    return hasFiniteNumber(payload, key) ? payload[key].get<double>() : fallback;
}

void applyDepthDelta(std::vector<BookLevel>& book, int position, int operation, double price, double size) {
    if (position < 0) {
        return;
    }

    if (operation == 0) {
        BookLevel level{price, size};
        if (position >= static_cast<int>(book.size())) {
            book.push_back(level);
        } else {
            book.insert(book.begin() + position, level);
        }
    } else if (operation == 1) {
        if (position < static_cast<int>(book.size())) {
            book[position].price = price;
            book[position].size = size;
        }
    } else if (operation == 2) {
        if (position < static_cast<int>(book.size())) {
            book.erase(book.begin() + position);
        }
    }
}

json bookToJson(const std::vector<BookLevel>& book, std::size_t depthLimit) {
    json levels = json::array();
    const std::size_t limit = depthLimit == 0 ? book.size() : std::min(depthLimit, book.size());
    for (std::size_t i = 0; i < limit; ++i) {
        levels.push_back({
            {"position", i},
            {"price", book[i].price},
            {"size", book[i].size}
        });
    }
    return levels;
}

struct ReplayBookState {
    double bidPrice = 0.0;
    double askPrice = 0.0;
    double lastPrice = 0.0;
    std::vector<BookLevel> bidBook;
    std::vector<BookLevel> askBook;
    std::uint64_t replayedThroughSessionSeq = 0;
    std::size_t appliedEvents = 0;
    std::size_t gapMarkers = 0;
};

constexpr std::uint64_t kProtectedWindowPreNs = 30ULL * 1000ULL * 1000ULL * 1000ULL;
constexpr std::uint64_t kProtectedWindowPostNs = 90ULL * 1000ULL * 1000ULL * 1000ULL;
constexpr std::uint64_t kIncidentMergeWindowNs = 5ULL * 1000ULL * 1000ULL * 1000ULL;
constexpr double kSpreadWideningMinAbsolute = 0.01;
constexpr double kLiquidityChangeMinShares = 100.0;
constexpr double kLiquidityChangeMinRatio = 0.50;

bool hasAnchorIdentity(const BridgeAnchorIdentity& anchor) {
    return anchor.traceId > 0 || anchor.orderId > 0 || anchor.permId > 0 || !anchor.execId.empty();
}

bool sameAnchorIdentity(const BridgeAnchorIdentity& left, const BridgeAnchorIdentity& right) {
    return left.traceId == right.traceId &&
           left.orderId == right.orderId &&
           left.permId == right.permId &&
           left.execId == right.execId;
}

int severityRank(const std::string& severity) {
    if (severity == "critical") {
        return 4;
    }
    if (severity == "error") {
        return 3;
    }
    if (severity == "warning") {
        return 2;
    }
    if (severity == "info") {
        return 1;
    }
    return 0;
}

double incidentScoreContribution(const FindingRecord& finding) {
    const double severityWeight = finding.severity == "warning" ? 2.5 : 1.0;
    const double overlapWeight = finding.overlapsOrder ? 1.75 : 1.0;
    return severityWeight * std::max(0.25, finding.confidence) * overlapWeight;
}

std::string incidentWhyItMatters(const IncidentRecord& incident) {
    if (incident.kind == "spread_widened") {
        return incident.overlapsOrder
            ? "The inside market widened while your order anchor was active, which can change fill odds and queue quality."
            : "The inside market widened, which usually signals a more fragile execution environment.";
    }
    if (incident.kind == "ask_liquidity_thinned" || incident.kind == "bid_liquidity_thinned") {
        return "Displayed inside liquidity pulled back at the touch, which can precede price travel or weaker passive fills.";
    }
    if (incident.kind == "ask_liquidity_refilled" || incident.kind == "bid_liquidity_refilled") {
        return "Displayed inside liquidity refilled at the touch, which can signal absorption or support/resistance rebuilding.";
    }
    if (incident.kind == "source_gap" || incident.kind == "source_reset") {
        return "Source continuity changed, so any interpretation around this window needs to account for data-quality risk.";
    }
    return "This incident groups correlated evidence into one revision-pinned investigation unit.";
}

json incidentScoreBreakdown(const IncidentRecord& incident) {
    const double findingCount = std::max<double>(1.0, static_cast<double>(incident.findingCount));
    return json{
        {"score", incident.score},
        {"finding_count", incident.findingCount},
        {"average_score_per_finding", incident.score / findingCount},
        {"confidence", incident.confidence},
        {"severity", incident.severity},
        {"overlaps_order", incident.overlapsOrder},
        {"score_model", "severity_weight * confidence * overlap_weight, accumulated per finding"}
    };
}

std::vector<IncidentRecord> collapseLatestIncidentRevisions(const std::vector<IncidentRecord>& records) {
    std::map<std::uint64_t, IncidentRecord> latestByLogicalIncident;
    for (const auto& record : records) {
        auto found = latestByLogicalIncident.find(record.logicalIncidentId);
        if (found == latestByLogicalIncident.end() ||
            found->second.incidentRevisionId < record.incidentRevisionId) {
            latestByLogicalIncident[record.logicalIncidentId] = record;
        }
    }
    std::vector<IncidentRecord> collapsed;
    collapsed.reserve(latestByLogicalIncident.size());
    for (const auto& entry : latestByLogicalIncident) {
        collapsed.push_back(entry.second);
    }
    std::sort(collapsed.begin(), collapsed.end(), [](const IncidentRecord& left,
                                                     const IncidentRecord& right) {
        if (left.score != right.score) {
            return left.score > right.score;
        }
        if (left.tsEngineNs != right.tsEngineNs) {
            return left.tsEngineNs > right.tsEngineNs;
        }
        return left.logicalIncidentId > right.logicalIncidentId;
    });
    return collapsed;
}

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
};

struct DeferredAnalyzerInput {
    std::uint64_t sourceRevisionId = 0;
    ProtectedWindowRecord protectedWindow;
    std::vector<json> windowEvents;
};

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

            std::ostringstream liquidityTitle;
            liquidityTitle << "Inside " << sideLabel << (refill ? " liquidity refilled at " : " liquidity thinned at ")
                           << std::fixed << std::setprecision(2) << price;
            std::ostringstream liquiditySummary;
            liquiditySummary << "Inside " << sideLabel << " size moved from "
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
            finding.title = liquidityTitle.str();
            finding.summary = liquiditySummary.str();
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

const std::vector<std::unique_ptr<Phase3Analyzer>>& allPhase3Analyzers() {
    static const std::vector<std::unique_ptr<Phase3Analyzer>> analyzers = [] {
        std::vector<std::unique_ptr<Phase3Analyzer>> built;
        built.push_back(std::make_unique<SourceContinuityAnalyzer>());
        built.push_back(std::make_unique<SpreadWideningAnalyzer>());
        built.push_back(std::make_unique<InsideLiquidityAnalyzer>());
        built.push_back(std::make_unique<OrderFlowTimelineAnalyzer>());
        return built;
    }();
    return analyzers;
}

json anchorToJson(const BridgeAnchorIdentity& anchor) {
    json payload = json::object();
    if (anchor.traceId > 0) {
        payload["trace_id"] = anchor.traceId;
    }
    if (anchor.orderId > 0) {
        payload["order_id"] = static_cast<long long>(anchor.orderId);
    }
    if (anchor.permId > 0) {
        payload["perm_id"] = anchor.permId;
    }
    if (!anchor.execId.empty()) {
        payload["exec_id"] = anchor.execId;
    }
    return payload;
}

BridgeAnchorIdentity anchorFromJson(const json& payload) {
    BridgeAnchorIdentity anchor;
    anchor.traceId = payload.value("trace_id", 0ULL);
    anchor.orderId = static_cast<OrderId>(payload.value("order_id", 0LL));
    anchor.permId = payload.value("perm_id", 0LL);
    anchor.execId = payload.value("exec_id", std::string());
    return anchor;
}

json orderAnchorToJson(const OrderAnchorRecord& record) {
    return {
        {"anchor_id", record.anchorId},
        {"revision_id", record.revisionId},
        {"session_seq", record.sessionSeq},
        {"ts_engine_ns", record.tsEngineNs},
        {"event_kind", record.eventKind},
        {"instrument_id", record.instrumentId},
        {"note", record.note},
        {"anchor", anchorToJson(record.anchor)}
    };
}

json protectedWindowToJson(const ProtectedWindowRecord& record) {
    return {
        {"window_id", record.windowId},
        {"revision_id", record.revisionId},
        {"logical_incident_id", record.logicalIncidentId},
        {"anchor_session_seq", record.anchorSessionSeq},
        {"start_engine_ns", record.startEngineNs},
        {"end_engine_ns", record.endEngineNs},
        {"reason", record.reason},
        {"instrument_id", record.instrumentId},
        {"anchor", anchorToJson(record.anchor)}
    };
}

json findingToJson(const FindingRecord& record) {
    return {
        {"finding_id", record.findingId},
        {"revision_id", record.revisionId},
        {"logical_incident_id", record.logicalIncidentId},
        {"incident_revision_id", record.incidentRevisionId},
        {"kind", record.kind},
        {"severity", record.severity},
        {"confidence", record.confidence},
        {"first_session_seq", record.firstSessionSeq},
        {"last_session_seq", record.lastSessionSeq},
        {"ts_engine_ns", record.tsEngineNs},
        {"instrument_id", record.instrumentId},
        {"title", record.title},
        {"summary", record.summary},
        {"overlaps_order", record.overlapsOrder},
        {"overlapping_anchor", anchorToJson(record.overlappingAnchor)}
    };
}

json incidentToJson(const IncidentRecord& record) {
    return {
        {"logical_incident_id", record.logicalIncidentId},
        {"incident_revision_id", record.incidentRevisionId},
        {"revision_id", record.revisionId},
        {"kind", record.kind},
        {"severity", record.severity},
        {"confidence", record.confidence},
        {"score", record.score},
        {"first_session_seq", record.firstSessionSeq},
        {"last_session_seq", record.lastSessionSeq},
        {"promoted_by_finding_id", record.promotedByFindingId},
        {"latest_finding_id", record.latestFindingId},
        {"finding_count", record.findingCount},
        {"ts_engine_ns", record.tsEngineNs},
        {"instrument_id", record.instrumentId},
        {"title", record.title},
        {"summary", record.summary},
        {"overlaps_order", record.overlapsOrder},
        {"overlapping_anchor", anchorToJson(record.overlappingAnchor)}
    };
}

OrderAnchorRecord orderAnchorFromJson(const json& payload) {
    OrderAnchorRecord record;
    record.anchorId = payload.value("anchor_id", 0ULL);
    record.revisionId = payload.value("revision_id", 0ULL);
    record.sessionSeq = payload.value("session_seq", 0ULL);
    record.tsEngineNs = payload.value("ts_engine_ns", 0ULL);
    record.eventKind = payload.value("event_kind", std::string());
    record.instrumentId = payload.value("instrument_id", std::string());
    record.note = payload.value("note", std::string());
    record.anchor = anchorFromJson(payload.value("anchor", json::object()));
    return record;
}

ProtectedWindowRecord protectedWindowFromJson(const json& payload) {
    ProtectedWindowRecord record;
    record.windowId = payload.value("window_id", 0ULL);
    record.revisionId = payload.value("revision_id", 0ULL);
    record.logicalIncidentId = payload.value("logical_incident_id", 0ULL);
    record.anchorSessionSeq = payload.value("anchor_session_seq", 0ULL);
    record.startEngineNs = payload.value("start_engine_ns", 0ULL);
    record.endEngineNs = payload.value("end_engine_ns", 0ULL);
    record.reason = payload.value("reason", std::string());
    record.instrumentId = payload.value("instrument_id", std::string());
    record.anchor = anchorFromJson(payload.value("anchor", json::object()));
    return record;
}

FindingRecord findingFromJson(const json& payload) {
    FindingRecord record;
    record.findingId = payload.value("finding_id", 0ULL);
    record.revisionId = payload.value("revision_id", 0ULL);
    record.logicalIncidentId = payload.value("logical_incident_id", 0ULL);
    record.incidentRevisionId = payload.value("incident_revision_id", 0ULL);
    record.kind = payload.value("kind", std::string());
    record.severity = payload.value("severity", std::string());
    record.confidence = payload.value("confidence", 0.0);
    record.firstSessionSeq = payload.value("first_session_seq", 0ULL);
    record.lastSessionSeq = payload.value("last_session_seq", 0ULL);
    record.tsEngineNs = payload.value("ts_engine_ns", 0ULL);
    record.instrumentId = payload.value("instrument_id", std::string());
    record.title = payload.value("title", std::string());
    record.summary = payload.value("summary", std::string());
    record.overlapsOrder = payload.value("overlaps_order", false);
    record.overlappingAnchor = anchorFromJson(payload.value("overlapping_anchor", json::object()));
    return record;
}

IncidentRecord incidentFromJson(const json& payload) {
    IncidentRecord record;
    record.logicalIncidentId = payload.value("logical_incident_id", 0ULL);
    record.incidentRevisionId = payload.value("incident_revision_id", 0ULL);
    record.revisionId = payload.value("revision_id", 0ULL);
    record.kind = payload.value("kind", std::string());
    record.severity = payload.value("severity", std::string());
    record.confidence = payload.value("confidence", 0.0);
    record.score = payload.value("score", std::max(0.25, record.confidence));
    record.firstSessionSeq = payload.value("first_session_seq", 0ULL);
    record.lastSessionSeq = payload.value("last_session_seq", 0ULL);
    record.promotedByFindingId = payload.value("promoted_by_finding_id", 0ULL);
    record.latestFindingId = payload.value("latest_finding_id", record.promotedByFindingId);
    record.findingCount = payload.value("finding_count", 1ULL);
    record.tsEngineNs = payload.value("ts_engine_ns", 0ULL);
    record.instrumentId = payload.value("instrument_id", std::string());
    record.title = payload.value("title", std::string());
    record.summary = payload.value("summary", std::string());
    record.overlapsOrder = payload.value("overlaps_order", false);
    record.overlappingAnchor = anchorFromJson(payload.value("overlapping_anchor", json::object()));
    return record;
}

json eventToJson(const EngineEvent& event) {
    json payload{
        {"adapter_id", event.adapterId},
        {"connection_id", event.connectionId},
        {"event_kind", event.eventKind},
        {"instrument_id", event.instrumentId},
        {"revision_id", event.revisionId},
        {"session_seq", event.sessionSeq},
        {"source_seq", event.sourceSeq},
        {"ts_engine_ns", event.tsEngineNs}
    };

    if (event.eventKind == "gap_marker") {
        payload["gap_start_source_seq"] = event.gapStartSourceSeq;
        payload["gap_end_source_seq"] = event.gapEndSourceSeq;
        return payload;
    }
    if (event.eventKind == "reset_marker") {
        payload["reset_previous_source_seq"] = event.resetPreviousSourceSeq;
        payload["reset_source_seq"] = event.resetSourceSeq;
        return payload;
    }

    const json recordPayload = bridge_batch::recordToJson(event.bridgeRecord);
    for (auto it = recordPayload.begin(); it != recordPayload.end(); ++it) {
        payload[it.key()] = it.value();
    }
    return payload;
}

std::vector<std::uint8_t> readExact(int fd, std::size_t bytes) {
    std::vector<std::uint8_t> buffer(bytes);
    std::size_t offset = 0;
    while (offset < bytes) {
        const ssize_t readCount = ::read(fd, buffer.data() + offset, bytes - offset);
        if (readCount == 0) {
            throw std::runtime_error("unexpected EOF while reading framed message");
        }
        if (readCount < 0) {
            throw std::runtime_error(errnoMessage("read"));
        }
        offset += static_cast<std::size_t>(readCount);
    }
    return buffer;
}

std::vector<std::uint8_t> readFramedMessage(int fd) {
    const std::vector<std::uint8_t> prefix = readExact(fd, 4);
    const std::uint32_t payloadSize =
        (static_cast<std::uint32_t>(prefix[0]) << 24) |
        (static_cast<std::uint32_t>(prefix[1]) << 16) |
        (static_cast<std::uint32_t>(prefix[2]) << 8) |
        static_cast<std::uint32_t>(prefix[3]);
    std::vector<std::uint8_t> frame = prefix;
    const std::vector<std::uint8_t> payload = readExact(fd, payloadSize);
    frame.insert(frame.end(), payload.begin(), payload.end());
    return frame;
}

void writeAll(int fd, const std::vector<std::uint8_t>& data) {
    std::size_t offset = 0;
    while (offset < data.size()) {
        const ssize_t wrote = ::write(fd, data.data() + offset, data.size() - offset);
        if (wrote < 0) {
            throw std::runtime_error(errnoMessage("write"));
        }
        offset += static_cast<std::size_t>(wrote);
    }
}

std::string connectionKey(const std::string& adapterId, const std::string& connectionId) {
    return adapterId + "|" + connectionId;
}

RequestKind classifyRequestKind(const std::vector<std::uint8_t>& frame) {
    const json payload = decodeFramedJson(frame);
    const std::string schema = payload.value("schema", std::string());
    if (schema == bridge_batch::kSchemaName) {
        return RequestKind::Ingest;
    }
    if (schema == kQueryRequestSchema) {
        return RequestKind::Query;
    }
    throw std::runtime_error("unknown tape-engine request schema");
}

} // namespace

Server::Server(EngineConfig config)
    : config_(std::move(config)) {}

Server::~Server() {
    stop();
}

bool Server::restoreFrozenState(std::string* error) {
    std::lock_guard<std::mutex> lock(stateMutex_);
    liveRing_.clear();
    cursors_.clear();
    segments_.clear();
    orderAnchors_.clear();
    protectedWindows_.clear();
    findings_.clear();
    incidents_.clear();
    writerFailure_.clear();
    lastManifestHash_.clear();
    analyzerBookState_ = {};
    nextSessionSeq_ = 1;
    nextSegmentId_ = 1;
    nextRevisionId_ = 1;
    latestFrozenRevisionId_ = 0;
    latestFrozenSessionSeq_ = 0;
    nextOrderAnchorId_ = 1;
    nextProtectedWindowId_ = 1;
    nextFindingId_ = 1;
    nextLogicalIncidentId_ = 1;
    nextIncidentRevisionId_ = 1;

    const std::filesystem::path manifestPath = config_.dataDir / "manifest.jsonl";
    if (!std::filesystem::exists(manifestPath)) {
        return true;
    }

    std::ifstream manifestIn(manifestPath);
    if (!manifestIn.is_open()) {
        if (error != nullptr) {
            *error = "failed to open tape-engine manifest for restore";
        }
        return false;
    }

    std::string line;
    while (std::getline(manifestIn, line)) {
        if (line.empty()) {
            continue;
        }

        const json entry = json::parse(line, nullptr, false);
        if (entry.is_discarded()) {
            if (error != nullptr) {
                *error = "failed to parse tape-engine manifest during restore";
            }
            return false;
        }

        SegmentInfo info;
        info.segmentId = entry.value("segment_id", 0ULL);
        info.revisionId = entry.value("revision_id", 0ULL);
        info.firstSessionSeq = entry.value("first_session_seq", 0ULL);
        info.lastSessionSeq = entry.value("last_session_seq", 0ULL);
        info.eventCount = entry.value("event_count", 0ULL);
        info.fileName = entry.value("file_name", std::string());
        info.metadataFileName = entry.value("metadata_file_name", std::string());
        info.artifactsFileName = entry.value("artifacts_file_name", std::string());
        info.payloadSha256 = entry.value("payload_sha256", std::string());
        info.prevManifestHash = entry.value("prev_manifest_hash", std::string());
        info.manifestHash = entry.value("manifest_hash", std::string());
        segments_.push_back(info);

        lastManifestHash_ = info.manifestHash;
        latestFrozenRevisionId_ = std::max(latestFrozenRevisionId_, info.revisionId);
        latestFrozenSessionSeq_ = std::max(latestFrozenSessionSeq_, info.lastSessionSeq);
        nextSessionSeq_ = std::max(nextSessionSeq_, info.lastSessionSeq + 1);
        nextSegmentId_ = std::max(nextSegmentId_, info.segmentId + 1);
        nextRevisionId_ = std::max(nextRevisionId_, info.revisionId + 1);

        if (info.artifactsFileName.empty()) {
            continue;
        }

        const std::filesystem::path artifactsPath = config_.dataDir / "segments" / info.artifactsFileName;
        std::ifstream artifactsIn(artifactsPath, std::ios::binary);
        if (!artifactsIn.is_open()) {
            continue;
        }

        const std::vector<std::uint8_t> bytes((std::istreambuf_iterator<char>(artifactsIn)),
                                              std::istreambuf_iterator<char>());
        if (bytes.empty()) {
            continue;
        }

        const json artifacts = json::from_msgpack(bytes, true, false);
        for (const auto& item : artifacts.value("order_anchors", json::array())) {
            OrderAnchorRecord record = orderAnchorFromJson(item);
            orderAnchors_.push_back(record);
            nextOrderAnchorId_ = std::max(nextOrderAnchorId_, record.anchorId + 1);
        }
        for (const auto& item : artifacts.value("protected_windows", json::array())) {
            ProtectedWindowRecord record = protectedWindowFromJson(item);
            protectedWindows_.push_back(record);
            nextProtectedWindowId_ = std::max(nextProtectedWindowId_, record.windowId + 1);
        }
        for (const auto& item : artifacts.value("findings", json::array())) {
            FindingRecord record = findingFromJson(item);
            findings_.push_back(record);
            nextFindingId_ = std::max(nextFindingId_, record.findingId + 1);
        }
        for (const auto& item : artifacts.value("incidents", json::array())) {
            IncidentRecord record = incidentFromJson(item);
            incidents_.push_back(record);
            nextLogicalIncidentId_ = std::max(nextLogicalIncidentId_, record.logicalIncidentId + 1);
            nextIncidentRevisionId_ = std::max(nextIncidentRevisionId_, record.incidentRevisionId + 1);
        }
    }

    return true;
}

bool Server::start(std::string* error) {
    if (running_.load(std::memory_order_acquire)) {
        return true;
    }

    std::error_code ec;
    std::filesystem::create_directories(config_.dataDir / "segments", ec);
    if (ec) {
        if (error != nullptr) {
            *error = "failed to create tape-engine data dir: " + ec.message();
        }
        return false;
    }

    if (!restoreFrozenState(error)) {
        return false;
    }

    if (config_.socketPath.size() >= sizeof(sockaddr_un{}.sun_path)) {
        if (error != nullptr) {
            *error = "tape-engine socket path is too long";
        }
        return false;
    }

    std::filesystem::remove(config_.socketPath, ec);

    serverFd_ = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (serverFd_ < 0) {
        if (error != nullptr) {
            *error = errnoMessage("socket");
        }
        return false;
    }

    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    std::strncpy(address.sun_path, config_.socketPath.c_str(), sizeof(address.sun_path) - 1);
    if (::bind(serverFd_, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0) {
        if (error != nullptr) {
            *error = errnoMessage("bind");
        }
        ::close(serverFd_);
        serverFd_ = -1;
        return false;
    }
    if (::listen(serverFd_, 16) != 0) {
        if (error != nullptr) {
            *error = errnoMessage("listen");
        }
        ::close(serverFd_);
        serverFd_ = -1;
        return false;
    }

    running_.store(true, std::memory_order_release);
    writerThread_ = std::thread(&Server::writerLoop, this);
    deferredAnalyzerThread_ = std::thread(&Server::deferredAnalyzerLoop, this);
    replayThread_ = std::thread(&Server::replayLoop, this);
    sequencerThread_ = std::thread(&Server::sequencerLoop, this);
    acceptThread_ = std::thread(&Server::acceptLoop, this);
    return true;
}

void Server::stop() {
    if (!running_.exchange(false, std::memory_order_acq_rel)) {
        return;
    }

    if (serverFd_ >= 0) {
        ::shutdown(serverFd_, SHUT_RDWR);
        ::close(serverFd_);
        serverFd_ = -1;
    }
    ingestQueueCv_.notify_all();
    queryQueueCv_.notify_all();
    writerCv_.notify_all();
    deferredAnalyzerCv_.notify_all();

    if (acceptThread_.joinable()) {
        acceptThread_.join();
    }
    std::vector<std::thread> clientThreads;
    {
        std::lock_guard<std::mutex> lock(clientThreadsMutex_);
        clientThreads.swap(clientThreads_);
    }
    for (auto& thread : clientThreads) {
        if (thread.joinable()) {
            thread.join();
        }
    }
    if (sequencerThread_.joinable()) {
        sequencerThread_.join();
    }
    if (replayThread_.joinable()) {
        replayThread_.join();
    }
    if (deferredAnalyzerThread_.joinable()) {
        deferredAnalyzerThread_.join();
    }
    if (writerThread_.joinable()) {
        writerThread_.join();
    }

    std::error_code ec;
    std::filesystem::remove(config_.socketPath, ec);
}

EngineSnapshot Server::snapshot() const {
    const std::size_t writerBacklog = [&]() {
        std::lock_guard<std::mutex> writerLock(writerMutex_);
        return writerQueue_.size();
    }();
    std::lock_guard<std::mutex> lock(stateMutex_);
    EngineSnapshot snapshot;
    snapshot.nextSessionSeq = nextSessionSeq_;
    snapshot.nextSegmentId = nextSegmentId_;
    snapshot.nextRevisionId = nextRevisionId_;
    snapshot.latestFrozenRevisionId = latestFrozenRevisionId_;
    snapshot.latestFrozenSessionSeq = latestFrozenSessionSeq_;
    snapshot.writerBacklogSegments = writerBacklog;
    snapshot.liveEvents.assign(liveRing_.begin(), liveRing_.end());
    snapshot.segments = segments_;
    return snapshot;
}

const EngineConfig& Server::config() const {
    return config_;
}

void Server::acceptLoop() {
    runtime_qos::applyCurrentThreadSpec(runtime_registry::QueueId::EngineAcceptLoop);
    while (running_.load(std::memory_order_acquire)) {
        const int clientFd = ::accept(serverFd_, nullptr, nullptr);
        if (clientFd < 0) {
            if (!running_.load(std::memory_order_acquire)) {
                break;
            }
            if (errno == EINTR) {
                continue;
            }
            continue;
        }

        try {
            std::lock_guard<std::mutex> lock(clientThreadsMutex_);
            clientThreads_.emplace_back(&Server::handleClientConnection, this, clientFd);
        } catch (...) {
            ::close(clientFd);
        }
    }
}

void Server::handleClientConnection(int clientFd) {
    runtime_qos::applyCurrentThreadSpec(runtime_registry::QueueId::EngineAcceptLoop);
    try {
        auto request = std::make_shared<PendingRequest>();
        request->frame = readFramedMessage(clientFd);
        request->kind = classifyRequestKind(request->frame);
        auto future = request->promise.get_future();

        if (request->kind == RequestKind::Ingest) {
            {
                std::lock_guard<std::mutex> lock(ingestQueueMutex_);
                ingestQueue_.push_back(std::move(request));
            }
            ingestQueueCv_.notify_one();
        } else {
            {
                std::lock_guard<std::mutex> lock(queryQueueMutex_);
                queryQueue_.push_back(std::move(request));
            }
            queryQueueCv_.notify_one();
        }

        writeAll(clientFd, future.get());
    } catch (const std::exception& error) {
        QueryRequest request;
        request.operation = "unknown";
        try {
            writeAll(clientFd, encodeQueryResponseFrame(rejectResponse(request, error.what())));
        } catch (...) {
        }
    } catch (...) {
    }

    ::close(clientFd);
}

void Server::sequencerLoop() {
    runtime_qos::applyCurrentThreadSpec(runtime_registry::QueueId::EngineSequencer);
    while (true) {
        std::shared_ptr<PendingRequest> request;
        {
            std::unique_lock<std::mutex> lock(ingestQueueMutex_);
            ingestQueueCv_.wait(lock, [&]() {
                return !running_.load(std::memory_order_acquire) || !ingestQueue_.empty();
            });
            if (!running_.load(std::memory_order_acquire) && ingestQueue_.empty()) {
                break;
            }
            request = std::move(ingestQueue_.front());
            ingestQueue_.pop_front();
        }

        try {
            request->promise.set_value(encodeAckFrame(processIngestFrame(request->frame)));
        } catch (const std::exception& error) {
            request->promise.set_value(encodeAckFrame(rejectAck(0, "", "", error.what())));
        }
    }
}

void Server::replayLoop() {
    runtime_qos::applyCurrentThreadSpec(runtime_registry::QueueId::EngineReplay);
    while (true) {
        std::shared_ptr<PendingRequest> request;
        {
            std::unique_lock<std::mutex> lock(queryQueueMutex_);
            queryQueueCv_.wait(lock, [&]() {
                return !running_.load(std::memory_order_acquire) || !queryQueue_.empty();
            });
            if (!running_.load(std::memory_order_acquire) && queryQueue_.empty()) {
                break;
            }
            request = std::move(queryQueue_.front());
            queryQueue_.pop_front();
        }

        try {
            request->promise.set_value(encodeQueryResponseFrame(processQueryFrame(request->frame)));
        } catch (const std::exception& error) {
            QueryRequest failedRequest;
            failedRequest.operation = "unknown";
            request->promise.set_value(encodeQueryResponseFrame(rejectResponse(failedRequest, error.what())));
        }
    }
}

void Server::writerLoop() {
    runtime_qos::applyCurrentThreadSpec(runtime_registry::QueueId::EngineSegmentWriter);
    while (true) {
        PendingSegment segment;
        {
            std::unique_lock<std::mutex> lock(writerMutex_);
            writerCv_.wait(lock, [&]() {
                return !running_.load(std::memory_order_acquire) || !writerQueue_.empty();
            });
            if (!running_.load(std::memory_order_acquire) && writerQueue_.empty()) {
                break;
            }
            segment = std::move(writerQueue_.front());
            writerQueue_.pop_front();
        }

        try {
            writeSegment(segment);
        } catch (const std::exception& error) {
            std::lock_guard<std::mutex> lock(stateMutex_);
            writerFailure_ = error.what();
        }
    }
}

void Server::deferredAnalyzerLoop() {
    runtime_qos::applyCurrentThreadSpec(runtime_registry::QueueId::EngineAnalyzerDeferred);
    while (true) {
        DeferredAnalyzerTask task;
        {
            std::unique_lock<std::mutex> lock(deferredAnalyzerMutex_);
            deferredAnalyzerCv_.wait(lock, [&]() {
                return !running_.load(std::memory_order_acquire) || !deferredAnalyzerQueue_.empty();
            });
            if (!running_.load(std::memory_order_acquire) && deferredAnalyzerQueue_.empty()) {
                break;
            }
            task = std::move(deferredAnalyzerQueue_.front());
            deferredAnalyzerQueue_.pop_front();
        }

        QuerySnapshot snapshot = captureQuerySnapshot();
        const std::uint64_t frozenRevisionId = snapshot.latestFrozenRevisionId;
        const FrozenArtifacts frozenArtifacts = loadFrozenArtifacts(snapshot, frozenRevisionId);

        std::vector<ProtectedWindowRecord> windows = frozenArtifacts.protectedWindows;
        for (const auto& record : snapshot.protectedWindows) {
            if (record.revisionId > frozenRevisionId) {
                windows.push_back(record);
            }
        }

        std::optional<ProtectedWindowRecord> selectedWindow;
        for (const auto& window : windows) {
            if (window.windowId == task.protectedWindowId) {
                if (!selectedWindow.has_value() || selectedWindow->revisionId < window.revisionId) {
                    selectedWindow = window;
                }
            }
        }
        if (!selectedWindow.has_value()) {
            continue;
        }

        json ignoredSummary = json::object();
        std::vector<json> windowEvents;
        try {
            windowEvents = filterEventsByProtectedWindow(snapshot,
                                                        selectedWindow->windowId,
                                                        0,
                                                        frozenRevisionId,
                                                        true,
                                                        &ignoredSummary);
        } catch (...) {
            continue;
        }

        DeferredAnalyzerInput input;
        input.sourceRevisionId = task.sourceRevisionId;
        input.protectedWindow = *selectedWindow;
        input.windowEvents = std::move(windowEvents);

        std::vector<AnalyzerFindingSpec> findings;
        for (const auto& analyzer : allPhase3Analyzers()) {
            if (analyzer->lane() != AnalyzerLane::Deferred) {
                continue;
            }
            if (!task.analyzerName.empty() && task.analyzerName != analyzer->name()) {
                continue;
            }
            analyzer->analyzeDeferred(input, &findings);
        }
        if (findings.empty()) {
            continue;
        }

        PendingSegment pendingSegment;
        {
            std::lock_guard<std::mutex> lock(stateMutex_);
            const std::uint64_t revisionId = nextRevisionId_++;
            pendingSegment.revisionId = revisionId;
            for (const auto& finding : findings) {
                recordFindingRangeUnlocked(revisionId,
                                           finding.kind,
                                           finding.severity,
                                           finding.confidence,
                                           finding.firstSessionSeq,
                                           finding.lastSessionSeq,
                                           finding.tsEngineNs,
                                           finding.instrumentId.empty() ? selectedWindow->instrumentId : finding.instrumentId,
                                           finding.title,
                                           finding.summary,
                                           finding.overlappingAnchor,
                                           finding.overlapsOrder);
                EngineEvent syntheticEvent;
                syntheticEvent.revisionId = revisionId;
                syntheticEvent.sessionSeq = finding.lastSessionSeq;
                syntheticEvent.tsEngineNs = finding.tsEngineNs;
                syntheticEvent.instrumentId = finding.instrumentId.empty() ? selectedWindow->instrumentId : finding.instrumentId;
                recordIncidentUnlocked(syntheticEvent, findings_.back(), finding.overlappingAnchor, finding.overlapsOrder);
            }

            for (const auto& record : orderAnchors_) {
                if (record.revisionId == revisionId) {
                    pendingSegment.orderAnchors.push_back(record);
                }
            }
            for (const auto& record : protectedWindows_) {
                if (record.revisionId == revisionId) {
                    pendingSegment.protectedWindows.push_back(record);
                }
            }
            for (const auto& record : findings_) {
                if (record.revisionId == revisionId) {
                    pendingSegment.findings.push_back(record);
                }
            }
            for (const auto& record : incidents_) {
                if (record.revisionId == revisionId) {
                    pendingSegment.incidents.push_back(record);
                }
            }
        }

        enqueueSegment(std::move(pendingSegment));
    }
}

std::string Server::resolveInstrumentId(const BridgeOutboxRecord& record) const {
    if (!record.instrumentId.empty()) {
        return record.instrumentId;
    }
    if (!record.symbol.empty()) {
        return "ib:STK:SMART:USD:" + record.symbol;
    }
    return config_.instrumentId;
}

void Server::rememberSourceSeqUnlocked(ConnectionCursor& cursor, std::uint64_t sourceSeq) {
    cursor.recentSourceSeqs.push_back(sourceSeq);
    cursor.recentSourceSeqSet.insert(sourceSeq);
    while (cursor.recentSourceSeqs.size() > config_.dedupeWindowSize) {
        const std::uint64_t evicted = cursor.recentSourceSeqs.front();
        cursor.recentSourceSeqs.pop_front();
        cursor.recentSourceSeqSet.erase(evicted);
    }
}

void Server::resetSourceSeqWindowUnlocked(ConnectionCursor& cursor) {
    cursor.recentSourceSeqs.clear();
    cursor.recentSourceSeqSet.clear();
}

void Server::recordOrderAnchorUnlocked(const EngineEvent& event) {
    if (!hasAnchorIdentity(event.bridgeRecord.anchor)) {
        return;
    }

    OrderAnchorRecord record;
    record.anchorId = nextOrderAnchorId_++;
    record.revisionId = event.revisionId;
    record.sessionSeq = event.sessionSeq;
    record.tsEngineNs = event.tsEngineNs;
    record.eventKind = event.eventKind;
    record.instrumentId = event.instrumentId;
    record.anchor = event.bridgeRecord.anchor;
    record.note = event.bridgeRecord.note;
    orderAnchors_.push_back(std::move(record));
}

void Server::addProtectedWindowUnlocked(const EngineEvent& event,
                                        const std::string& reason,
                                        const BridgeAnchorIdentity& anchor,
                                        std::uint64_t logicalIncidentId) {
    ProtectedWindowRecord record;
    record.windowId = nextProtectedWindowId_++;
    record.revisionId = event.revisionId;
    record.logicalIncidentId = logicalIncidentId;
    record.anchorSessionSeq = event.sessionSeq;
    record.startEngineNs = event.tsEngineNs > kProtectedWindowPreNs ? event.tsEngineNs - kProtectedWindowPreNs : 0;
    record.endEngineNs = event.tsEngineNs + kProtectedWindowPostNs;
    record.reason = reason;
    record.instrumentId = event.instrumentId;
    record.anchor = anchor;
    protectedWindows_.push_back(std::move(record));

    if (logicalIncidentId == 0 &&
        hasAnchorIdentity(anchor) &&
        (reason == "order_intent" ||
         reason == "open_order" ||
         reason == "order_status" ||
         reason == "fill_execution" ||
         reason == "cancel_request" ||
         reason == "order_reject" ||
         reason == "broker_error")) {
        DeferredAnalyzerTask task;
        task.analyzerName = "order_flow_timeline";
        task.sourceRevisionId = event.revisionId;
        task.protectedWindowId = protectedWindows_.back().windowId;
        task.instrumentId = event.instrumentId;
        task.reason = reason;
        task.anchor = anchor;
        enqueueDeferredAnalyzerTask(std::move(task));
    }
}

BridgeAnchorIdentity Server::findOverlappingOrderAnchorUnlocked(std::uint64_t tsEngineNs) const {
    for (auto it = protectedWindows_.rbegin(); it != protectedWindows_.rend(); ++it) {
        if (!hasAnchorIdentity(it->anchor)) {
            continue;
        }
        if (tsEngineNs >= it->startEngineNs && tsEngineNs <= it->endEngineNs) {
            return it->anchor;
        }
    }
    return {};
}

void Server::recordFindingUnlocked(const EngineEvent& event,
                                   const std::string& kind,
                                   const std::string& severity,
                                   double confidence,
                                   const std::string& title,
                                   const std::string& summary,
                                   const BridgeAnchorIdentity& overlappingAnchor,
                                   bool overlapsOrder) {
    recordFindingRangeUnlocked(event.revisionId,
                               kind,
                               severity,
                               confidence,
                               event.sessionSeq,
                               event.sessionSeq,
                               event.tsEngineNs,
                               event.instrumentId,
                               title,
                               summary,
                               overlappingAnchor,
                               overlapsOrder);
}

void Server::recordFindingRangeUnlocked(std::uint64_t revisionId,
                                        const std::string& kind,
                                        const std::string& severity,
                                        double confidence,
                                        std::uint64_t firstSessionSeq,
                                        std::uint64_t lastSessionSeq,
                                        std::uint64_t tsEngineNs,
                                        const std::string& instrumentId,
                                        const std::string& title,
                                        const std::string& summary,
                                        const BridgeAnchorIdentity& overlappingAnchor,
                                        bool overlapsOrder) {
    FindingRecord finding;
    finding.findingId = nextFindingId_++;
    finding.revisionId = revisionId;
    finding.kind = kind;
    finding.severity = severity;
    finding.confidence = confidence;
    finding.firstSessionSeq = firstSessionSeq;
    finding.lastSessionSeq = lastSessionSeq;
    finding.tsEngineNs = tsEngineNs;
    finding.instrumentId = instrumentId;
    finding.title = title;
    finding.summary = summary;
    finding.overlapsOrder = overlapsOrder;
    finding.overlappingAnchor = overlappingAnchor;
    findings_.push_back(std::move(finding));
}

void Server::recordIncidentUnlocked(const EngineEvent& event,
                                    const FindingRecord& finding,
                                    const BridgeAnchorIdentity& overlappingAnchor,
                                    bool overlapsOrder) {
    for (auto it = incidents_.rbegin(); it != incidents_.rend(); ++it) {
        if (it->kind != finding.kind || it->instrumentId != finding.instrumentId) {
            continue;
        }
        if (it->overlapsOrder != overlapsOrder || !sameAnchorIdentity(it->overlappingAnchor, overlappingAnchor)) {
            continue;
        }
        if (finding.tsEngineNs > it->tsEngineNs + kIncidentMergeWindowNs) {
            continue;
        }

        IncidentRecord incident = *it;
        incident.incidentRevisionId = nextIncidentRevisionId_++;
        incident.revisionId = finding.revisionId;
        incident.severity = severityRank(finding.severity) > severityRank(incident.severity)
            ? finding.severity
            : incident.severity;
        incident.confidence = std::max(incident.confidence, finding.confidence);
        incident.score += incidentScoreContribution(finding);
        incident.lastSessionSeq = finding.lastSessionSeq;
        incident.promotedByFindingId = finding.findingId;
        incident.latestFindingId = finding.findingId;
        incident.findingCount = std::max<std::uint64_t>(1, incident.findingCount) + 1;
        incident.tsEngineNs = finding.tsEngineNs;
        incident.title = finding.title;
        std::ostringstream summary;
        summary << incident.findingCount << " correlated " << finding.kind
                << " findings from session_seq " << incident.firstSessionSeq
                << " through " << incident.lastSessionSeq << ".";
        incident.summary = summary.str();
        incidents_.push_back(std::move(incident));
        if (!findings_.empty() && findings_.back().findingId == finding.findingId) {
            findings_.back().logicalIncidentId = incidents_.back().logicalIncidentId;
            findings_.back().incidentRevisionId = incidents_.back().incidentRevisionId;
        }
        return;
    }

    IncidentRecord incident;
    incident.logicalIncidentId = nextLogicalIncidentId_++;
    incident.incidentRevisionId = nextIncidentRevisionId_++;
    incident.revisionId = finding.revisionId;
    incident.kind = finding.kind;
    incident.severity = finding.severity;
    incident.confidence = finding.confidence;
    incident.score = incidentScoreContribution(finding);
    incident.firstSessionSeq = finding.firstSessionSeq;
    incident.lastSessionSeq = finding.lastSessionSeq;
    incident.promotedByFindingId = finding.findingId;
    incident.latestFindingId = finding.findingId;
    incident.findingCount = 1;
    incident.tsEngineNs = finding.tsEngineNs;
    incident.instrumentId = finding.instrumentId;
    incident.title = finding.title;
    incident.summary = finding.summary;
    incident.overlapsOrder = overlapsOrder;
    incident.overlappingAnchor = overlappingAnchor;
    incidents_.push_back(std::move(incident));
    if (!findings_.empty() && findings_.back().findingId == finding.findingId) {
        findings_.back().logicalIncidentId = incidents_.back().logicalIncidentId;
        findings_.back().incidentRevisionId = incidents_.back().incidentRevisionId;
    }

    addProtectedWindowUnlocked(event, "incident_promotion", overlappingAnchor, incidents_.back().logicalIncidentId);
}

void Server::updateAnalyzerBookUnlocked(const EngineEvent& event) {
    if (event.eventKind == "market_tick") {
        const int marketField = event.bridgeRecord.marketField;
        const double price = event.bridgeRecord.price;
        if (std::isfinite(price)) {
            if (marketField == 1) {
                analyzerBookState_.bidTickPrice = price;
            } else if (marketField == 2) {
                analyzerBookState_.askTickPrice = price;
            } else if (marketField == 4) {
                analyzerBookState_.lastTradePrice = price;
            }
        }
        return;
    }

    if (event.eventKind == "market_depth") {
        const int bookSide = event.bridgeRecord.bookSide;
        const int position = event.bridgeRecord.bookPosition;
        const int operation = event.bridgeRecord.bookOperation;
        const double price = event.bridgeRecord.price;
        const double size = event.bridgeRecord.size;
        if (bookSide == 0) {
            applyDepthDelta(analyzerBookState_.askBook, position, operation, price, size);
        } else if (bookSide == 1) {
            applyDepthDelta(analyzerBookState_.bidBook, position, operation, price, size);
        }
    }
}

void Server::updatePhase3StateUnlocked(const EngineEvent& event) {
    if (hasAnchorIdentity(event.bridgeRecord.anchor)) {
        recordOrderAnchorUnlocked(event);

        if (event.eventKind == "order_intent" ||
            event.eventKind == "open_order" ||
            event.eventKind == "order_status" ||
            event.eventKind == "fill_execution" ||
            event.eventKind == "cancel_request" ||
            event.eventKind == "order_reject" ||
            event.eventKind == "broker_error") {
            addProtectedWindowUnlocked(event, event.eventKind, event.bridgeRecord.anchor);
        }
    }

    const double previousBid = analyzerBookState_.lastEffectiveBid;
    const double previousAsk = analyzerBookState_.lastEffectiveAsk;
    const double previousBidSize = analyzerBookState_.lastEffectiveBidSize;
    const double previousAskSize = analyzerBookState_.lastEffectiveAskSize;
    const bool hadInside = analyzerBookState_.hasInside;
    updateAnalyzerBookUnlocked(event);

    const double effectiveBid = !analyzerBookState_.bidBook.empty()
        ? analyzerBookState_.bidBook.front().price
        : analyzerBookState_.bidTickPrice;
    const double effectiveAsk = !analyzerBookState_.askBook.empty()
        ? analyzerBookState_.askBook.front().price
        : analyzerBookState_.askTickPrice;
    const double effectiveBidSize = !analyzerBookState_.bidBook.empty()
        ? analyzerBookState_.bidBook.front().size
        : 0.0;
    const double effectiveAskSize = !analyzerBookState_.askBook.empty()
        ? analyzerBookState_.askBook.front().size
        : 0.0;
    const bool hasInside = effectiveBid > 0.0 && effectiveAsk > 0.0 && effectiveAsk >= effectiveBid;

    analyzerBookState_.lastEffectiveBid = effectiveBid;
    analyzerBookState_.lastEffectiveAsk = effectiveAsk;
    analyzerBookState_.lastEffectiveBidSize = effectiveBidSize;
    analyzerBookState_.lastEffectiveAskSize = effectiveAskSize;
    analyzerBookState_.hasInside = hasInside;

    const BridgeAnchorIdentity overlap = findOverlappingOrderAnchorUnlocked(event.tsEngineNs);
    const bool overlapsOrder = hasAnchorIdentity(overlap);

    HotAnalyzerInput input{
        .event = event,
        .previousBid = previousBid,
        .previousAsk = previousAsk,
        .previousBidSize = previousBidSize,
        .previousAskSize = previousAskSize,
        .hadInside = hadInside,
        .effectiveBid = effectiveBid,
        .effectiveAsk = effectiveAsk,
        .effectiveBidSize = effectiveBidSize,
        .effectiveAskSize = effectiveAskSize,
        .hasInside = hasInside,
        .overlapsOrder = overlapsOrder,
        .overlappingAnchor = overlap
    };

    std::vector<AnalyzerFindingSpec> analyzerFindings;
    for (const auto& analyzer : allPhase3Analyzers()) {
        if (analyzer->lane() != AnalyzerLane::Hot) {
            continue;
        }
        analyzer->analyzeHot(input, &analyzerFindings);
    }

    for (const auto& finding : analyzerFindings) {
        recordFindingRangeUnlocked(event.revisionId,
                                   finding.kind,
                                   finding.severity,
                                   finding.confidence,
                                   finding.firstSessionSeq,
                                   finding.lastSessionSeq,
                                   finding.tsEngineNs,
                                   finding.instrumentId.empty() ? event.instrumentId : finding.instrumentId,
                                   finding.title,
                                   finding.summary,
                                   finding.overlappingAnchor,
                                   finding.overlapsOrder);
        recordIncidentUnlocked(event, findings_.back(), finding.overlappingAnchor, finding.overlapsOrder);
    }
}

IngestAck Server::rejectAck(std::uint64_t batchSeq,
                            const std::string& adapterId,
                            const std::string& connectionId,
                            const std::string& error) const {
    IngestAck ack;
    ack.status = "rejected";
    ack.batchSeq = batchSeq;
    ack.adapterId = adapterId;
    ack.connectionId = connectionId;
    ack.error = error;
    return ack;
}

void Server::appendLiveEvent(const EngineEvent& event) {
    if (config_.ringCapacity == 0) {
        return;
    }
    if (liveRing_.size() >= config_.ringCapacity) {
        liveRing_.pop_front();
    }
    liveRing_.push_back(event);
}

void Server::enqueueSegment(PendingSegment segment) {
    if (segment.events.empty() &&
        segment.orderAnchors.empty() &&
        segment.protectedWindows.empty() &&
        segment.findings.empty() &&
        segment.incidents.empty()) {
        return;
    }
    {
        std::lock_guard<std::mutex> lock(writerMutex_);
        writerQueue_.push_back(std::move(segment));
    }
    writerCv_.notify_one();
}

void Server::enqueueDeferredAnalyzerTask(DeferredAnalyzerTask task) {
    {
        std::lock_guard<std::mutex> lock(deferredAnalyzerMutex_);
        deferredAnalyzerQueue_.push_back(std::move(task));
    }
    deferredAnalyzerCv_.notify_one();
}

void Server::writeSegment(const PendingSegment& segment) {
    if (segment.events.empty() &&
        segment.orderAnchors.empty() &&
        segment.protectedWindows.empty() &&
        segment.findings.empty() &&
        segment.incidents.empty()) {
        return;
    }

    std::uint64_t segmentId = 0;
    std::string previousManifestHash;
    {
        std::lock_guard<std::mutex> lock(stateMutex_);
        segmentId = nextSegmentId_++;
        previousManifestHash = lastManifestHash_;
    }

    std::ostringstream name;
    name << "segment-" << std::setw(6) << std::setfill('0') << segmentId;
    const std::string baseName = name.str();
    const std::filesystem::path segmentPath = config_.dataDir / "segments" / (baseName + ".events.msgpack");
    const std::filesystem::path metadataPath = config_.dataDir / "segments" / (baseName + ".meta.json");
    const std::filesystem::path artifactsPath = config_.dataDir / "segments" / (baseName + ".artifacts.msgpack");
    const std::filesystem::path manifestPath = config_.dataDir / "manifest.jsonl";

    json payloadJson = json::array();
    for (const auto& event : segment.events) {
        payloadJson.push_back(eventToJson(event));
    }
    const std::vector<std::uint8_t> payload = json::to_msgpack(payloadJson);

    {
        std::ofstream out(segmentPath, std::ios::binary);
        if (!out.is_open()) {
            throw std::runtime_error("failed to open tape-engine segment for write");
        }
        out.write(reinterpret_cast<const char*>(payload.data()), static_cast<std::streamsize>(payload.size()));
    }

    json artifactsJson{
        {"findings", json::array()},
        {"incidents", json::array()},
        {"order_anchors", json::array()},
        {"protected_windows", json::array()}
    };
    for (const auto& record : segment.orderAnchors) {
        artifactsJson["order_anchors"].push_back(orderAnchorToJson(record));
    }
    for (const auto& record : segment.protectedWindows) {
        artifactsJson["protected_windows"].push_back(protectedWindowToJson(record));
    }
    for (const auto& record : segment.findings) {
        artifactsJson["findings"].push_back(findingToJson(record));
    }
    for (const auto& record : segment.incidents) {
        artifactsJson["incidents"].push_back(incidentToJson(record));
    }
    const std::vector<std::uint8_t> artifactsPayload = json::to_msgpack(artifactsJson);

    {
        std::ofstream artifactsOut(artifactsPath, std::ios::binary);
        if (!artifactsOut.is_open()) {
            throw std::runtime_error("failed to open tape-engine segment artifacts for write");
        }
        artifactsOut.write(reinterpret_cast<const char*>(artifactsPayload.data()),
                           static_cast<std::streamsize>(artifactsPayload.size()));
    }

    SegmentInfo info;
    info.segmentId = segmentId;
    info.revisionId = segment.revisionId;
    std::uint64_t firstSessionSeq = std::numeric_limits<std::uint64_t>::max();
    std::uint64_t lastSessionSeq = 0;
    for (const auto& event : segment.events) {
        firstSessionSeq = std::min(firstSessionSeq, event.sessionSeq);
        lastSessionSeq = std::max(lastSessionSeq, event.sessionSeq);
    }
    for (const auto& record : segment.orderAnchors) {
        firstSessionSeq = std::min(firstSessionSeq, record.sessionSeq);
        lastSessionSeq = std::max(lastSessionSeq, record.sessionSeq);
    }
    for (const auto& record : segment.protectedWindows) {
        firstSessionSeq = std::min(firstSessionSeq, record.anchorSessionSeq);
        lastSessionSeq = std::max(lastSessionSeq, record.anchorSessionSeq);
    }
    for (const auto& record : segment.findings) {
        firstSessionSeq = std::min(firstSessionSeq, record.firstSessionSeq);
        lastSessionSeq = std::max(lastSessionSeq, record.lastSessionSeq);
    }
    for (const auto& record : segment.incidents) {
        firstSessionSeq = std::min(firstSessionSeq, record.firstSessionSeq);
        lastSessionSeq = std::max(lastSessionSeq, record.lastSessionSeq);
    }
    if (firstSessionSeq == std::numeric_limits<std::uint64_t>::max()) {
        return;
    }
    info.firstSessionSeq = firstSessionSeq;
    info.lastSessionSeq = lastSessionSeq;
    info.eventCount = static_cast<std::uint64_t>(segment.events.size());
    info.fileName = segmentPath.filename().string();
    info.metadataFileName = metadataPath.filename().string();
    info.artifactsFileName = artifactsPath.filename().string();
    info.payloadSha256 = sha256Hex(payload);
    info.prevManifestHash = previousManifestHash;

    json manifestEntry{
        {"event_count", info.eventCount},
        {"file_name", info.fileName},
        {"first_session_seq", info.firstSessionSeq},
        {"last_session_seq", info.lastSessionSeq},
        {"artifacts_file_name", info.artifactsFileName},
        {"metadata_file_name", info.metadataFileName},
        {"payload_sha256", info.payloadSha256},
        {"prev_manifest_hash", info.prevManifestHash},
        {"revision_id", info.revisionId},
        {"segment_id", info.segmentId}
    };
    info.manifestHash = sha256Hex(manifestEntry.dump());
    manifestEntry["manifest_hash"] = info.manifestHash;

    {
        std::ofstream metadataOut(metadataPath);
        if (!metadataOut.is_open()) {
            throw std::runtime_error("failed to open tape-engine segment metadata for write");
        }
        metadataOut << manifestEntry.dump(2) << '\n';
    }
    {
        std::ofstream manifestOut(manifestPath, std::ios::app);
        if (!manifestOut.is_open()) {
            throw std::runtime_error("failed to open tape-engine manifest for append");
        }
        manifestOut << manifestEntry.dump() << '\n';
    }

    {
        std::lock_guard<std::mutex> lock(stateMutex_);
        lastManifestHash_ = info.manifestHash;
        latestFrozenRevisionId_ = std::max(latestFrozenRevisionId_, segment.revisionId);
        latestFrozenSessionSeq_ = std::max(latestFrozenSessionSeq_, info.lastSessionSeq);
        segments_.push_back(std::move(info));
    }
}

Server::QuerySnapshot Server::captureQuerySnapshot() const {
    std::lock_guard<std::mutex> lock(stateMutex_);
    QuerySnapshot snapshot;
    snapshot.dataDir = config_.dataDir;
    snapshot.socketPath = config_.socketPath;
    snapshot.instrumentId = config_.instrumentId;
    snapshot.lastManifestHash = lastManifestHash_;
    snapshot.writerFailure = writerFailure_;
    snapshot.nextSessionSeq = nextSessionSeq_;
    snapshot.nextSegmentId = nextSegmentId_;
    snapshot.nextRevisionId = nextRevisionId_;
    snapshot.latestFrozenRevisionId = latestFrozenRevisionId_;
    snapshot.latestFrozenSessionSeq = latestFrozenSessionSeq_;
    snapshot.segments = segments_;
    snapshot.liveEvents.assign(liveRing_.begin(), liveRing_.end());
    snapshot.orderAnchors = orderAnchors_;
    snapshot.protectedWindows = protectedWindows_;
    snapshot.findings = findings_;
    snapshot.incidents = incidents_;
    return snapshot;
}

std::uint64_t Server::resolveFrozenRevision(const QuerySnapshot& snapshot,
                                            std::uint64_t requestedRevisionId) const {
    if (requestedRevisionId == 0) {
        return snapshot.latestFrozenRevisionId;
    }
    if (requestedRevisionId > snapshot.latestFrozenRevisionId) {
        throw std::runtime_error("requested revision_id is not frozen yet");
    }
    return requestedRevisionId;
}

Server::FrozenArtifacts Server::loadFrozenArtifacts(const QuerySnapshot& snapshot,
                                                    std::uint64_t frozenRevisionId) const {
    FrozenArtifacts artifacts;
    for (const auto& segment : snapshot.segments) {
        if (segment.revisionId > frozenRevisionId || segment.artifactsFileName.empty()) {
            continue;
        }

        const std::filesystem::path artifactsPath = snapshot.dataDir / "segments" / segment.artifactsFileName;
        std::ifstream in(artifactsPath, std::ios::binary);
        if (!in.is_open()) {
            continue;
        }

        const std::vector<std::uint8_t> bytes((std::istreambuf_iterator<char>(in)),
                                              std::istreambuf_iterator<char>());
        if (bytes.empty()) {
            continue;
        }

        const json payload = json::from_msgpack(bytes, true, false);
        for (const auto& item : payload.value("order_anchors", json::array())) {
            artifacts.orderAnchors.push_back(orderAnchorFromJson(item));
        }
        for (const auto& item : payload.value("protected_windows", json::array())) {
            artifacts.protectedWindows.push_back(protectedWindowFromJson(item));
        }
        for (const auto& item : payload.value("findings", json::array())) {
            artifacts.findings.push_back(findingFromJson(item));
        }
        for (const auto& item : payload.value("incidents", json::array())) {
            artifacts.incidents.push_back(incidentFromJson(item));
        }
    }

    std::sort(artifacts.orderAnchors.begin(), artifacts.orderAnchors.end(), [](const OrderAnchorRecord& left,
                                                                                const OrderAnchorRecord& right) {
        return std::tie(left.revisionId, left.anchorId) < std::tie(right.revisionId, right.anchorId);
    });
    std::sort(artifacts.protectedWindows.begin(), artifacts.protectedWindows.end(), [](const ProtectedWindowRecord& left,
                                                                                        const ProtectedWindowRecord& right) {
        return std::tie(left.revisionId, left.windowId) < std::tie(right.revisionId, right.windowId);
    });
    std::sort(artifacts.findings.begin(), artifacts.findings.end(), [](const FindingRecord& left,
                                                                        const FindingRecord& right) {
        return std::tie(left.revisionId, left.findingId) < std::tie(right.revisionId, right.findingId);
    });
    std::sort(artifacts.incidents.begin(), artifacts.incidents.end(), [](const IncidentRecord& left,
                                                                          const IncidentRecord& right) {
        return std::tie(left.revisionId, left.incidentRevisionId) < std::tie(right.revisionId, right.incidentRevisionId);
    });
    return artifacts;
}

std::vector<json> Server::loadEvents(const QuerySnapshot& snapshot,
                                     std::uint64_t frozenRevisionId,
                                     std::uint64_t fromSessionSeq,
                                     std::uint64_t throughSessionSeq) const {
    std::vector<json> events;
    for (const auto& segment : snapshot.segments) {
        if (segment.revisionId > frozenRevisionId) {
            continue;
        }

        if (fromSessionSeq > 0 && segment.lastSessionSeq < fromSessionSeq) {
            continue;
        }
        if (throughSessionSeq > 0 && segment.firstSessionSeq > throughSessionSeq) {
            continue;
        }

        const std::filesystem::path segmentPath = snapshot.dataDir / "segments" / segment.fileName;
        std::ifstream in(segmentPath, std::ios::binary);
        if (!in.is_open()) {
            continue;
        }

        if (segmentPath.extension() == ".msgpack") {
            const std::vector<std::uint8_t> bytes((std::istreambuf_iterator<char>(in)),
                                                  std::istreambuf_iterator<char>());
            if (bytes.empty()) {
                continue;
            }
            json parsed = json::from_msgpack(bytes, true, false);
            if (parsed.is_array()) {
                for (auto& entry : parsed) {
                    if (entry.is_object()) {
                        events.push_back(std::move(entry));
                    }
                }
            }
            continue;
        }

        std::string line;
        while (std::getline(in, line)) {
            if (line.empty()) {
                continue;
            }
            json parsed = json::parse(line, nullptr, false);
            if (!parsed.is_discarded()) {
                events.push_back(std::move(parsed));
            }
        }
    }
    return events;
}

std::vector<json> Server::mergedEvents(const QuerySnapshot& snapshot,
                                       std::uint64_t frozenRevisionId,
                                       bool includeLiveTail,
                                       std::uint64_t fromSessionSeq,
                                       std::uint64_t throughSessionSeq) const {
    std::vector<json> events = loadEvents(snapshot, frozenRevisionId, fromSessionSeq, throughSessionSeq);
    if (!includeLiveTail) {
        return events;
    }

    std::unordered_set<std::uint64_t> seenSessionSeq;
    seenSessionSeq.reserve(events.size() + snapshot.liveEvents.size());
    for (const auto& event : events) {
        seenSessionSeq.insert(event.value("session_seq", 0ULL));
    }
    for (const auto& event : snapshot.liveEvents) {
        if (event.revisionId > 0 && event.revisionId <= frozenRevisionId) {
            continue;
        }
        if (fromSessionSeq > 0 && event.sessionSeq < fromSessionSeq) {
            continue;
        }
        if (throughSessionSeq > 0 && event.sessionSeq > throughSessionSeq) {
            continue;
        }
        if (seenSessionSeq.insert(event.sessionSeq).second) {
            events.push_back(eventToJson(event));
        }
    }

    std::sort(events.begin(), events.end(), [](const json& left, const json& right) {
        return left.value("session_seq", 0ULL) < right.value("session_seq", 0ULL);
    });
    return events;
}

std::vector<json> Server::filterEventsByRange(const QuerySnapshot& snapshot,
                                              std::uint64_t fromSessionSeq,
                                              std::uint64_t toSessionSeq,
                                              std::size_t limit,
                                              std::uint64_t frozenRevisionId,
                                              bool includeLiveTail) const {
    std::vector<json> results;
    const auto allEvents = mergedEvents(snapshot, frozenRevisionId, includeLiveTail, fromSessionSeq, toSessionSeq);
    for (const auto& event : allEvents) {
        const std::uint64_t sessionSeq = event.value("session_seq", 0ULL);
        if (sessionSeq < fromSessionSeq) {
            continue;
        }
        if (toSessionSeq > 0 && sessionSeq > toSessionSeq) {
            continue;
        }
        results.push_back(event);
        if (limit > 0 && results.size() >= limit) {
            return results;
        }
    }

    return results;
}

std::vector<json> Server::filterEventsByAnchor(const QuerySnapshot& snapshot,
                                               std::uint64_t traceId,
                                               long long orderId,
                                               long long permId,
                                               const std::string& execId,
                                               std::size_t limit,
                                               std::uint64_t frozenRevisionId,
                                               bool includeLiveTail) const {
    std::vector<json> results;
    const auto allEvents = mergedEvents(snapshot, frozenRevisionId, includeLiveTail, 0, 0);
    for (const auto& event : allEvents) {
        const json anchor = event.value("anchor", json::object());
        const bool matchesTrace = traceId > 0 && anchor.value("trace_id", 0ULL) == traceId;
        const bool matchesOrder = orderId > 0 && anchor.value("order_id", 0LL) == orderId;
        const bool matchesPerm = permId > 0 && anchor.value("perm_id", 0LL) == permId;
        const bool matchesExec = !execId.empty() && anchor.value("exec_id", std::string()) == execId;
        if (!matchesTrace && !matchesOrder && !matchesPerm && !matchesExec) {
            continue;
        }
        results.push_back(event);
        if (limit > 0 && results.size() >= limit) {
            break;
        }
    }
    return results;
}

std::vector<json> Server::filterEventsByProtectedWindow(const QuerySnapshot& snapshot,
                                                        std::uint64_t windowId,
                                                        std::size_t limit,
                                                        std::uint64_t frozenRevisionId,
                                                        bool includeLiveTail,
                                                        json* selectedWindowSummary) const {
    std::optional<ProtectedWindowRecord> selectedWindow;
    const FrozenArtifacts frozenArtifacts = loadFrozenArtifacts(snapshot, frozenRevisionId);
    for (const auto& record : frozenArtifacts.protectedWindows) {
        if (record.windowId == windowId) {
            selectedWindow = record;
        }
    }
    if (includeLiveTail) {
        for (const auto& record : snapshot.protectedWindows) {
            if (record.revisionId > frozenRevisionId && record.windowId == windowId) {
                selectedWindow = record;
            }
        }
    }

    if (!selectedWindow.has_value()) {
        throw std::runtime_error("protected window not found");
    }

    if (selectedWindowSummary != nullptr) {
        *selectedWindowSummary = protectedWindowToJson(*selectedWindow);
    }

    std::vector<json> results;
    const std::vector<json> allEvents = mergedEvents(snapshot, frozenRevisionId, includeLiveTail, 0, 0);
    for (const auto& event : allEvents) {
        const std::uint64_t tsEngineNs = event.value("ts_engine_ns", 0ULL);
        if (tsEngineNs < selectedWindow->startEngineNs || tsEngineNs > selectedWindow->endEngineNs) {
            continue;
        }
        if (!selectedWindow->instrumentId.empty() &&
            event.value("instrument_id", std::string()) != selectedWindow->instrumentId) {
            continue;
        }
        results.push_back(event);
        if (limit > 0 && results.size() >= limit) {
            break;
        }
    }
    return results;
}

json Server::buildReplaySnapshot(const QuerySnapshot& snapshot,
                                 std::uint64_t targetSessionSeq,
                                 std::size_t depthLimit,
                                 std::uint64_t frozenRevisionId,
                                 bool includeLiveTail) const {
    const std::vector<json> allEvents = mergedEvents(snapshot,
                                                     frozenRevisionId,
                                                     includeLiveTail,
                                                     0,
                                                     targetSessionSeq);
    if (targetSessionSeq == 0 && !allEvents.empty()) {
        targetSessionSeq = allEvents.back().value("session_seq", 0ULL);
    }

    ReplayBookState replay;
    for (const auto& event : allEvents) {
        const std::uint64_t sessionSeq = event.value("session_seq", 0ULL);
        if (sessionSeq == 0) {
            continue;
        }
        if (targetSessionSeq > 0 && sessionSeq > targetSessionSeq) {
            break;
        }

        replay.replayedThroughSessionSeq = sessionSeq;
        const std::string eventKind = event.value("event_kind", std::string());
        if (eventKind == "gap_marker") {
            ++replay.gapMarkers;
            continue;
        }

        if (eventKind == "market_tick") {
            const int marketField = event.value("market_field", -1);
            const double price = numberOrDefault(event, "price", std::numeric_limits<double>::quiet_NaN());
            if (std::isfinite(price)) {
                if (marketField == 1) {
                    replay.bidPrice = price;
                } else if (marketField == 2) {
                    replay.askPrice = price;
                } else if (marketField == 4) {
                    replay.lastPrice = price;
                }
            }
            ++replay.appliedEvents;
            continue;
        }

        if (eventKind == "market_depth") {
            const int bookSide = event.value("book_side", -1);
            const int position = event.value("book_position", -1);
            const int operation = event.value("book_operation", -1);
            const double price = numberOrDefault(event, "price", std::numeric_limits<double>::quiet_NaN());
            const double size = numberOrDefault(event, "size", std::numeric_limits<double>::quiet_NaN());
            if (bookSide == 0) {
                applyDepthDelta(replay.askBook, position, operation, price, size);
            } else if (bookSide == 1) {
                applyDepthDelta(replay.bidBook, position, operation, price, size);
            }
            ++replay.appliedEvents;
        }
    }

    const double effectiveBid = !replay.bidBook.empty()
        ? replay.bidBook.front().price
        : replay.bidPrice;
    const double effectiveAsk = !replay.askBook.empty()
        ? replay.askBook.front().price
        : replay.askPrice;

    return json{
        {"applied_event_count", replay.appliedEvents},
        {"ask_book", bookToJson(replay.askBook, depthLimit)},
        {"ask_price", effectiveAsk},
        {"bid_book", bookToJson(replay.bidBook, depthLimit)},
        {"bid_price", effectiveBid},
        {"gap_markers_encountered", replay.gapMarkers},
        {"includes_mutable_tail", includeLiveTail},
        {"last_price", replay.lastPrice},
        {"replayed_through_session_seq", replay.replayedThroughSessionSeq},
        {"target_session_seq", targetSessionSeq}
    };
}

IngestAck Server::processIngestFrame(const std::vector<std::uint8_t>& frame) {
    const bridge_batch::Batch batch = bridge_batch::decodeFrame(frame);
    IngestAck ack;
    ack.batchSeq = batch.header.batchSeq;
    ack.adapterId = batch.header.producer;
    ack.connectionId = batch.header.runtimeSessionId;
    ack.firstSourceSeq = batch.header.firstSourceSeq;
    ack.lastSourceSeq = batch.header.lastSourceSeq;

    if (batch.header.runtimeSessionId.empty()) {
        return rejectAck(batch.header.batchSeq, batch.header.producer, batch.header.runtimeSessionId,
                         "bridge batch runtime_session_id is required");
    }
    if (batch.header.recordCount != batch.records.size()) {
        return rejectAck(batch.header.batchSeq, batch.header.producer, batch.header.runtimeSessionId,
                         "bridge batch record_count does not match payload");
    }
    if (!batch.records.empty()) {
        if (batch.header.firstSourceSeq != batch.records.front().sourceSeq ||
            batch.header.lastSourceSeq != batch.records.back().sourceSeq) {
            return rejectAck(batch.header.batchSeq, batch.header.producer, batch.header.runtimeSessionId,
                             "bridge batch source_seq bounds do not match payload");
        }
        for (std::size_t i = 1; i < batch.records.size(); ++i) {
            if (batch.records[i - 1].sourceSeq >= batch.records[i].sourceSeq) {
                return rejectAck(batch.header.batchSeq, batch.header.producer, batch.header.runtimeSessionId,
                                 "bridge batch source_seq values must be strictly increasing");
            }
        }
    }

    PendingSegment pendingSegment;
    std::uint64_t assignedRevisionId = 0;

    {
        std::lock_guard<std::mutex> lock(stateMutex_);
        ConnectionCursor& cursor = cursors_[connectionKey(batch.header.producer, batch.header.runtimeSessionId)];
        const auto ensureRevisionId = [&]() -> std::uint64_t {
            if (assignedRevisionId == 0) {
                assignedRevisionId = nextRevisionId_++;
            }
            return assignedRevisionId;
        };

        for (const auto& record : batch.records) {
            if (cursor.recentSourceSeqSet.count(record.sourceSeq) > 0) {
                ++ack.duplicateRecords;
                continue;
            }

            if (cursor.hasLastAccepted && record.sourceSeq <= cursor.lastAcceptedSourceSeq) {
                EngineEvent reset;
                reset.sessionSeq = nextSessionSeq_++;
                reset.revisionId = ensureRevisionId();
                reset.sourceSeq = record.sourceSeq;
                reset.eventKind = "reset_marker";
                reset.adapterId = batch.header.producer;
                reset.connectionId = batch.header.runtimeSessionId;
                reset.instrumentId = resolveInstrumentId(record);
                reset.tsEngineNs = nowEngineNs();
                reset.resetPreviousSourceSeq = cursor.lastAcceptedSourceSeq;
                reset.resetSourceSeq = record.sourceSeq;
                appendLiveEvent(reset);
                pendingSegment.events.push_back(reset);
                updatePhase3StateUnlocked(reset);
                resetSourceSeqWindowUnlocked(cursor);
                cursor.lastAcceptedSourceSeq = 0;
                cursor.hasLastAccepted = false;
                if (ack.firstSessionSeq == 0) {
                    ack.firstSessionSeq = reset.sessionSeq;
                }
                ack.lastSessionSeq = reset.sessionSeq;
            }

            if (cursor.hasLastAccepted && record.sourceSeq > cursor.lastAcceptedSourceSeq + 1) {
                EngineEvent gap;
                gap.sessionSeq = nextSessionSeq_++;
                gap.revisionId = ensureRevisionId();
                gap.sourceSeq = cursor.lastAcceptedSourceSeq + 1;
                gap.eventKind = "gap_marker";
                gap.adapterId = batch.header.producer;
                gap.connectionId = batch.header.runtimeSessionId;
                gap.instrumentId = resolveInstrumentId(record);
                gap.tsEngineNs = nowEngineNs();
                gap.gapStartSourceSeq = cursor.lastAcceptedSourceSeq + 1;
                gap.gapEndSourceSeq = record.sourceSeq - 1;
                appendLiveEvent(gap);
                pendingSegment.events.push_back(gap);
                updatePhase3StateUnlocked(gap);
                ++ack.gapMarkers;
                if (ack.firstSessionSeq == 0) {
                    ack.firstSessionSeq = gap.sessionSeq;
                }
                ack.lastSessionSeq = gap.sessionSeq;
            }

            EngineEvent event;
            event.sessionSeq = nextSessionSeq_++;
            event.revisionId = ensureRevisionId();
            event.sourceSeq = record.sourceSeq;
            event.eventKind = record.recordType;
            event.adapterId = batch.header.producer;
            event.connectionId = batch.header.runtimeSessionId;
            event.instrumentId = resolveInstrumentId(record);
            event.tsEngineNs = nowEngineNs();
            event.bridgeRecord = record;

            appendLiveEvent(event);
            pendingSegment.events.push_back(event);
            updatePhase3StateUnlocked(event);
            rememberSourceSeqUnlocked(cursor, record.sourceSeq);
            cursor.lastAcceptedSourceSeq = std::max(cursor.lastAcceptedSourceSeq, record.sourceSeq);
            cursor.hasLastAccepted = true;

            ++ack.acceptedRecords;
            if (ack.firstSessionSeq == 0) {
                ack.firstSessionSeq = event.sessionSeq;
            }
            ack.lastSessionSeq = event.sessionSeq;
        }
        pendingSegment.revisionId = assignedRevisionId;
        if (assignedRevisionId > 0) {
            for (const auto& record : orderAnchors_) {
                if (record.revisionId == assignedRevisionId) {
                    pendingSegment.orderAnchors.push_back(record);
                }
            }
            for (const auto& record : protectedWindows_) {
                if (record.revisionId == assignedRevisionId) {
                    pendingSegment.protectedWindows.push_back(record);
                }
            }
            for (const auto& record : findings_) {
                if (record.revisionId == assignedRevisionId) {
                    pendingSegment.findings.push_back(record);
                }
            }
            for (const auto& record : incidents_) {
                if (record.revisionId == assignedRevisionId) {
                    pendingSegment.incidents.push_back(record);
                }
            }
        }
        ack.assignedRevisionId = assignedRevisionId;
    }

    enqueueSegment(std::move(pendingSegment));

    return ack;
}

QueryResponse Server::rejectResponse(const QueryRequest& request,
                                     const std::string& error) const {
    QueryResponse response;
    response.requestId = request.requestId;
    response.operation = request.operation;
    response.status = "error";
    response.error = error;
    return response;
}

QueryResponse Server::processQueryFrame(const std::vector<std::uint8_t>& frame) {
    const QueryRequest request = decodeQueryRequestFrame(frame);
    QueryResponse response;
    response.requestId = request.requestId;
    response.operation = request.operation;

    const std::size_t writerBacklog = [&]() {
        std::lock_guard<std::mutex> lock(writerMutex_);
        return writerQueue_.size();
    }();
    const QuerySnapshot snapshot = captureQuerySnapshot();
    if (request.operation == "status") {
        response.summary = {
            {"data_dir", snapshot.dataDir.string()},
            {"instrument_id", snapshot.instrumentId},
            {"last_manifest_hash", snapshot.lastManifestHash},
            {"latest_frozen_revision_id", snapshot.latestFrozenRevisionId},
            {"latest_frozen_session_seq", snapshot.latestFrozenSessionSeq},
            {"latest_session_seq", snapshot.nextSessionSeq > 0 ? snapshot.nextSessionSeq - 1 : 0},
            {"live_event_count", snapshot.liveEvents.size()},
            {"next_revision_id", snapshot.nextRevisionId},
            {"next_segment_id", snapshot.nextSegmentId},
            {"next_session_seq", snapshot.nextSessionSeq},
            {"segment_count", snapshot.segments.size()},
            {"socket_path", snapshot.socketPath},
            {"writer_backlog_segments", writerBacklog}
        };
        if (!snapshot.writerFailure.empty()) {
            response.summary["writer_error"] = snapshot.writerFailure;
        }
        return response;
    }

    if (request.operation == "read_live_tail") {
        const std::size_t limit = request.limit == 0 ? 50 : request.limit;
        response.summary = {
            {"base_revision_id", snapshot.latestFrozenRevisionId},
            {"includes_mutable_tail", true},
            {"live_tail_high_water_seq", snapshot.liveEvents.empty() ? 0 : snapshot.liveEvents.back().sessionSeq},
            {"returned_events", 0}
        };
        std::size_t start = snapshot.liveEvents.size() > limit ? snapshot.liveEvents.size() - limit : 0;
        response.events = json::array();
        for (std::size_t i = start; i < snapshot.liveEvents.size(); ++i) {
            response.events.push_back(eventToJson(snapshot.liveEvents[i]));
        }
        response.summary["returned_events"] = response.events.size();
        return response;
    }

    if (request.operation == "list_order_anchors" ||
        request.operation == "list_protected_windows" ||
        request.operation == "list_findings" ||
        request.operation == "list_incidents") {
        std::uint64_t frozenRevisionId = 0;
        try {
            frozenRevisionId = resolveFrozenRevision(snapshot, request.revisionId);
        } catch (const std::exception& error) {
            return rejectResponse(request, error.what());
        }

        const std::size_t limit = request.limit == 0 ? 100 : request.limit;
        const FrozenArtifacts frozenArtifacts = loadFrozenArtifacts(snapshot, frozenRevisionId);
        response.events = json::array();

        if (request.operation == "list_order_anchors") {
            std::vector<OrderAnchorRecord> records = frozenArtifacts.orderAnchors;
            if (request.includeLiveTail) {
                for (const auto& record : snapshot.orderAnchors) {
                    if (record.revisionId > frozenRevisionId) {
                        records.push_back(record);
                    }
                }
            }
            for (auto it = records.rbegin();
                 it != records.rend() && response.events.size() < limit;
                 ++it) {
                response.events.push_back(orderAnchorToJson(*it));
            }
        } else if (request.operation == "list_protected_windows") {
            std::vector<ProtectedWindowRecord> records = frozenArtifacts.protectedWindows;
            if (request.includeLiveTail) {
                for (const auto& record : snapshot.protectedWindows) {
                    if (record.revisionId > frozenRevisionId) {
                        records.push_back(record);
                    }
                }
            }
            for (auto it = records.rbegin();
                 it != records.rend() && response.events.size() < limit;
                 ++it) {
                response.events.push_back(protectedWindowToJson(*it));
            }
        } else if (request.operation == "list_findings") {
            std::vector<FindingRecord> records = frozenArtifacts.findings;
            if (request.includeLiveTail) {
                for (const auto& record : snapshot.findings) {
                    if (record.revisionId > frozenRevisionId) {
                        records.push_back(record);
                    }
                }
            }
            for (auto it = records.rbegin();
                 it != records.rend() && response.events.size() < limit;
                 ++it) {
                response.events.push_back(findingToJson(*it));
            }
        } else if (request.operation == "list_incidents") {
            std::vector<IncidentRecord> records = frozenArtifacts.incidents;
            if (request.includeLiveTail) {
                for (const auto& record : snapshot.incidents) {
                    if (record.revisionId > frozenRevisionId) {
                        records.push_back(record);
                    }
                }
            }
            const std::vector<IncidentRecord> collapsed = collapseLatestIncidentRevisions(records);
            for (const auto& record : collapsed) {
                if (response.events.size() >= limit) {
                    break;
                }
                response.events.push_back(incidentToJson(record));
            }
        }

        response.summary = {
            {"collapsed_logical_incidents", request.operation == "list_incidents"},
            {"includes_mutable_tail", request.includeLiveTail},
            {"returned_events", response.events.size()},
            {"served_revision_id", frozenRevisionId}
        };
        return response;
    }

    if (request.operation == "read_incident") {
        if (request.logicalIncidentId == 0) {
            return rejectResponse(request, "logical_incident_id is required");
        }

        std::uint64_t frozenRevisionId = 0;
        try {
            frozenRevisionId = resolveFrozenRevision(snapshot, request.revisionId);
        } catch (const std::exception& error) {
            return rejectResponse(request, error.what());
        }

        const FrozenArtifacts frozenArtifacts = loadFrozenArtifacts(snapshot, frozenRevisionId);
        std::vector<IncidentRecord> incidentRevisions = frozenArtifacts.incidents;
        std::vector<FindingRecord> relatedFindings = frozenArtifacts.findings;
        std::vector<ProtectedWindowRecord> protectedWindows = frozenArtifacts.protectedWindows;
        if (request.includeLiveTail) {
            for (const auto& record : snapshot.incidents) {
                if (record.revisionId > frozenRevisionId) {
                    incidentRevisions.push_back(record);
                }
            }
            for (const auto& record : snapshot.findings) {
                if (record.revisionId > frozenRevisionId) {
                    relatedFindings.push_back(record);
                }
            }
            for (const auto& record : snapshot.protectedWindows) {
                if (record.revisionId > frozenRevisionId) {
                    protectedWindows.push_back(record);
                }
            }
        }

        incidentRevisions.erase(std::remove_if(incidentRevisions.begin(),
                                               incidentRevisions.end(),
                                               [&](const IncidentRecord& record) {
                                                   return record.logicalIncidentId != request.logicalIncidentId;
                                               }),
                                incidentRevisions.end());
        if (incidentRevisions.empty()) {
            return rejectResponse(request, "logical_incident_id not found");
        }
        std::sort(incidentRevisions.begin(), incidentRevisions.end(), [](const IncidentRecord& left,
                                                                         const IncidentRecord& right) {
            return left.incidentRevisionId < right.incidentRevisionId;
        });
        const IncidentRecord& latestIncident = incidentRevisions.back();

        relatedFindings.erase(std::remove_if(relatedFindings.begin(),
                                             relatedFindings.end(),
                                             [&](const FindingRecord& record) {
                                                 return record.logicalIncidentId != latestIncident.logicalIncidentId;
                                             }),
                              relatedFindings.end());
        std::sort(relatedFindings.begin(), relatedFindings.end(), [](const FindingRecord& left,
                                                                     const FindingRecord& right) {
            if (left.lastSessionSeq != right.lastSessionSeq) {
                return left.lastSessionSeq > right.lastSessionSeq;
            }
            return left.findingId > right.findingId;
        });

        std::optional<ProtectedWindowRecord> incidentWindow;
        for (const auto& record : protectedWindows) {
            if (record.logicalIncidentId == latestIncident.logicalIncidentId) {
                if (!incidentWindow.has_value() || incidentWindow->revisionId < record.revisionId) {
                    incidentWindow = record;
                }
            }
        }

        const std::size_t limit = request.limit == 0 ? relatedFindings.size() : request.limit;
        response.events = json::array();
        for (std::size_t i = 0; i < relatedFindings.size() && i < limit; ++i) {
            response.events.push_back(findingToJson(relatedFindings[i]));
        }

        json revisionsJson = json::array();
        for (const auto& record : incidentRevisions) {
            revisionsJson.push_back(incidentToJson(record));
        }

        json reportSummary{
            {"headline", latestIncident.title},
            {"summary", latestIncident.summary},
            {"why_it_matters", incidentWhyItMatters(latestIncident)},
            {"evidence_from_session_seq", latestIncident.firstSessionSeq},
            {"evidence_to_session_seq", latestIncident.lastSessionSeq}
        };
        if (incidentWindow.has_value()) {
            reportSummary["protected_window_id"] = incidentWindow->windowId;
        }

        response.summary = {
            {"includes_mutable_tail", request.includeLiveTail},
            {"logical_incident_id", latestIncident.logicalIncidentId},
            {"served_revision_id", frozenRevisionId},
            {"latest_incident", incidentToJson(latestIncident)},
            {"incident_revision_count", incidentRevisions.size()},
            {"score_breakdown", incidentScoreBreakdown(latestIncident)},
            {"why_it_matters", incidentWhyItMatters(latestIncident)},
            {"report_summary", reportSummary},
            {"returned_events", response.events.size()},
            {"related_finding_count", relatedFindings.size()},
            {"incident_revisions", revisionsJson}
        };
        if (incidentWindow.has_value()) {
            response.summary["protected_window"] = protectedWindowToJson(*incidentWindow);
        }
        return response;
    }

    if (request.operation == "read_protected_window") {
        std::uint64_t frozenRevisionId = 0;
        try {
            frozenRevisionId = resolveFrozenRevision(snapshot, request.revisionId);
        } catch (const std::exception& error) {
            return rejectResponse(request, error.what());
        }

        const std::size_t limit = request.limit == 0 ? 200 : request.limit;
        json selectedWindowSummary = json::object();
        std::vector<json> events;
        try {
            events = filterEventsByProtectedWindow(snapshot,
                                                  request.windowId,
                                                  limit,
                                                  frozenRevisionId,
                                                  request.includeLiveTail,
                                                  &selectedWindowSummary);
        } catch (const std::exception& error) {
            return rejectResponse(request, error.what());
        }

        response.events = json::array();
        for (const auto& event : events) {
            response.events.push_back(event);
        }
        response.summary = {
            {"includes_mutable_tail", request.includeLiveTail},
            {"protected_window", selectedWindowSummary},
            {"returned_events", response.events.size()},
            {"served_revision_id", frozenRevisionId},
            {"window_id", request.windowId}
        };
        return response;
    }

    if (request.operation == "read_range") {
        std::uint64_t frozenRevisionId = 0;
        try {
            frozenRevisionId = resolveFrozenRevision(snapshot, request.revisionId);
        } catch (const std::exception& error) {
            return rejectResponse(request, error.what());
        }
        const std::uint64_t from = request.fromSessionSeq == 0 ? 1 : request.fromSessionSeq;
        const std::uint64_t to = request.toSessionSeq;
        const std::size_t limit = request.limit == 0 ? 200 : request.limit;
        const std::vector<json> events = filterEventsByRange(snapshot,
                                                             from,
                                                             to,
                                                             limit,
                                                             frozenRevisionId,
                                                             request.includeLiveTail);
        response.events = json::array();
        for (const auto& event : events) {
            response.events.push_back(event);
        }
        response.summary = {
            {"from_session_seq", from},
            {"includes_mutable_tail", request.includeLiveTail},
            {"returned_events", response.events.size()},
            {"served_revision_id", frozenRevisionId},
            {"to_session_seq", to}
        };
        if (request.includeLiveTail) {
            response.summary["live_tail_high_water_seq"] = snapshot.liveEvents.empty() ? 0 : snapshot.liveEvents.back().sessionSeq;
        }
        return response;
    }

    if (request.operation == "find_order_anchor") {
        std::uint64_t frozenRevisionId = 0;
        try {
            frozenRevisionId = resolveFrozenRevision(snapshot, request.revisionId);
        } catch (const std::exception& error) {
            return rejectResponse(request, error.what());
        }
        const std::size_t limit = request.limit == 0 ? 100 : request.limit;
        const std::vector<json> events =
            filterEventsByAnchor(snapshot,
                                 request.traceId,
                                 request.orderId,
                                 request.permId,
                                 request.execId,
                                 limit,
                                 frozenRevisionId,
                                 request.includeLiveTail);
        response.events = json::array();
        for (const auto& event : events) {
            response.events.push_back(event);
        }
        response.summary = {
            {"exec_id", request.execId},
            {"includes_mutable_tail", request.includeLiveTail},
            {"order_id", request.orderId},
            {"perm_id", request.permId},
            {"returned_events", response.events.size()},
            {"served_revision_id", frozenRevisionId},
            {"trace_id", request.traceId}
        };
        return response;
    }

    if (request.operation == "seek_order_anchor") {
        std::uint64_t frozenRevisionId = 0;
        try {
            frozenRevisionId = resolveFrozenRevision(snapshot, request.revisionId);
        } catch (const std::exception& error) {
            return rejectResponse(request, error.what());
        }

        const std::size_t limit = request.limit == 0 ? 100 : request.limit;
        const std::vector<json> events =
            filterEventsByAnchor(snapshot,
                                 request.traceId,
                                 request.orderId,
                                 request.permId,
                                 request.execId,
                                 limit,
                                 frozenRevisionId,
                                 request.includeLiveTail);
        if (events.empty()) {
            return rejectResponse(request, "order/fill anchor not found");
        }

        std::uint64_t firstSessionSeq = 0;
        std::uint64_t lastSessionSeq = 0;
        std::uint64_t firstFillSessionSeq = 0;
        std::uint64_t lastFillSessionSeq = 0;
        std::uint64_t latestStatusSessionSeq = 0;
        std::uint64_t latestOpenOrderSessionSeq = 0;
        for (const auto& event : events) {
            const std::uint64_t sessionSeq = event.value("session_seq", 0ULL);
            if (firstSessionSeq == 0 || sessionSeq < firstSessionSeq) {
                firstSessionSeq = sessionSeq;
            }
            lastSessionSeq = std::max(lastSessionSeq, sessionSeq);
            const std::string kind = event.value("event_kind", std::string());
            if (kind == "fill_execution") {
                if (firstFillSessionSeq == 0 || sessionSeq < firstFillSessionSeq) {
                    firstFillSessionSeq = sessionSeq;
                }
                lastFillSessionSeq = std::max(lastFillSessionSeq, sessionSeq);
            } else if (kind == "order_status") {
                latestStatusSessionSeq = std::max(latestStatusSessionSeq, sessionSeq);
            } else if (kind == "open_order") {
                latestOpenOrderSessionSeq = std::max(latestOpenOrderSessionSeq, sessionSeq);
            }
        }

        const json anchor = events.front().value("anchor", json::object());
        std::vector<ProtectedWindowRecord> windows = loadFrozenArtifacts(snapshot, frozenRevisionId).protectedWindows;
        if (request.includeLiveTail) {
            for (const auto& record : snapshot.protectedWindows) {
                if (record.revisionId > frozenRevisionId) {
                    windows.push_back(record);
                }
            }
        }

        std::optional<ProtectedWindowRecord> selectedWindow;
        for (const auto& window : windows) {
            const bool matchesTrace = request.traceId > 0 && window.anchor.traceId == request.traceId;
            const bool matchesOrder = request.orderId > 0 && window.anchor.orderId == request.orderId;
            const bool matchesPerm = request.permId > 0 && window.anchor.permId == request.permId;
            const bool matchesExec = !request.execId.empty() && window.anchor.execId == request.execId;
            if (!matchesTrace && !matchesOrder && !matchesPerm && !matchesExec) {
                continue;
            }
            if (!selectedWindow.has_value() || selectedWindow->revisionId < window.revisionId) {
                selectedWindow = window;
            }
        }

        const std::uint64_t replayTargetSessionSeq = lastFillSessionSeq > 0
            ? lastFillSessionSeq
            : (latestStatusSessionSeq > 0 ? latestStatusSessionSeq
                                          : (latestOpenOrderSessionSeq > 0 ? latestOpenOrderSessionSeq : lastSessionSeq));
        const std::uint64_t replayFromSessionSeq = firstSessionSeq > 5 ? firstSessionSeq - 5 : 1;
        const std::uint64_t replayToSessionSeq = std::max(lastSessionSeq, replayTargetSessionSeq + 5);

        response.events = json::array();
        for (const auto& event : events) {
            response.events.push_back(event);
        }
        response.summary = {
            {"anchor", anchor},
            {"first_fill_session_seq", firstFillSessionSeq},
            {"first_session_seq", firstSessionSeq},
            {"includes_mutable_tail", request.includeLiveTail},
            {"last_fill_session_seq", lastFillSessionSeq},
            {"last_session_seq", lastSessionSeq},
            {"latest_open_order_session_seq", latestOpenOrderSessionSeq},
            {"latest_order_status_session_seq", latestStatusSessionSeq},
            {"replay_from_session_seq", replayFromSessionSeq},
            {"replay_target_session_seq", replayTargetSessionSeq},
            {"replay_to_session_seq", replayToSessionSeq},
            {"returned_events", response.events.size()},
            {"served_revision_id", frozenRevisionId}
        };
        if (selectedWindow.has_value()) {
            response.summary["protected_window"] = protectedWindowToJson(*selectedWindow);
        }
        return response;
    }

    if (request.operation == "replay_snapshot") {
        std::uint64_t frozenRevisionId = 0;
        try {
            frozenRevisionId = resolveFrozenRevision(snapshot, request.revisionId);
        } catch (const std::exception& error) {
            return rejectResponse(request, error.what());
        }
        const std::size_t depthLimit = request.limit == 0 ? 5 : request.limit;
        response.summary = buildReplaySnapshot(snapshot,
                                              request.targetSessionSeq,
                                              depthLimit,
                                              frozenRevisionId,
                                              request.includeLiveTail);
        response.summary["served_revision_id"] = frozenRevisionId;
        response.events = json::array();
        return response;
    }

    return rejectResponse(request, "unknown tape-engine operation");
}

} // namespace tape_engine
