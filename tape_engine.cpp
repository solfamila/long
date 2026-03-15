#include "tape_engine.h"
#include "phase3_analyzers.h"
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
constexpr std::size_t kTradePressureMinStreak = 3;
constexpr double kTouchTradeTolerance = 1e-6;
constexpr std::size_t kDisplayInstabilityMinCycles = 2;
constexpr std::uint64_t kDisplayInstabilitySeqWindow = 8;
constexpr double kFillInvalidationMinMove = 0.02;

bool hasAnchorIdentity(const BridgeAnchorIdentity& anchor) {
    return anchor.traceId > 0 || anchor.orderId > 0 || anchor.permId > 0 || !anchor.execId.empty();
}

bool sameAnchorIdentity(const BridgeAnchorIdentity& left, const BridgeAnchorIdentity& right) {
    return left.traceId == right.traceId &&
           left.orderId == right.orderId &&
           left.permId == right.permId &&
           left.execId == right.execId;
}

bool anchorsShareIdentity(const BridgeAnchorIdentity& left, const BridgeAnchorIdentity& right) {
    return (left.traceId > 0 && right.traceId > 0 && left.traceId == right.traceId) ||
           (left.orderId > 0 && right.orderId > 0 && left.orderId == right.orderId) ||
           (left.permId > 0 && right.permId > 0 && left.permId == right.permId) ||
           (!left.execId.empty() && !right.execId.empty() && left.execId == right.execId);
}

bool matchesAnchorSelector(const BridgeAnchorIdentity& anchor,
                           std::uint64_t traceId,
                           long long orderId,
                           long long permId,
                           const std::string& execId) {
    const bool matchesTrace = traceId > 0 && anchor.traceId == traceId;
    const bool matchesOrder = orderId > 0 && anchor.orderId == orderId;
    const bool matchesPerm = permId > 0 && anchor.permId == permId;
    const bool matchesExec = !execId.empty() && anchor.execId == execId;
    return matchesTrace || matchesOrder || matchesPerm || matchesExec;
}

std::vector<std::string> anchorSelectorKeys(const BridgeAnchorIdentity& anchor) {
    std::vector<std::string> keys;
    if (anchor.traceId > 0) {
        keys.push_back("trace:" + std::to_string(anchor.traceId));
    }
    if (anchor.orderId > 0) {
        keys.push_back("order:" + std::to_string(static_cast<long long>(anchor.orderId)));
    }
    if (anchor.permId > 0) {
        keys.push_back("perm:" + std::to_string(anchor.permId));
    }
    if (!anchor.execId.empty()) {
        keys.push_back("exec:" + anchor.execId);
    }
    return keys;
}

std::vector<std::string> anchorSelectorKeys(std::uint64_t traceId,
                                            long long orderId,
                                            long long permId,
                                            const std::string& execId) {
    BridgeAnchorIdentity anchor;
    anchor.traceId = traceId;
    anchor.orderId = static_cast<OrderId>(orderId);
    anchor.permId = permId;
    anchor.execId = execId;
    return anchorSelectorKeys(anchor);
}

bool isStrongInstrumentId(const std::string& instrumentId) {
    return instrumentId.rfind("ib:conid:", 0) == 0;
}

int classifyTradePressureSide(const EngineEvent& event,
                              double effectiveBid,
                              double effectiveAsk,
                              bool hasInside) {
    if (event.eventKind != "market_tick" ||
        event.bridgeRecord.marketField != 4 ||
        !std::isfinite(event.bridgeRecord.price) ||
        !hasInside) {
        return 0;
    }
    if (event.bridgeRecord.price >= effectiveAsk - kTouchTradeTolerance) {
        return 1;
    }
    if (event.bridgeRecord.price <= effectiveBid + kTouchTradeTolerance) {
        return -1;
    }
    return 0;
}

std::optional<bool> classifyFillSide(const std::string& side) {
    if (side == "BOT" || side == "BUY") {
        return true;
    }
    if (side == "SLD" || side == "SELL") {
        return false;
    }
    return std::nullopt;
}

struct DisplayInstabilityResult {
    bool triggered = false;
    std::size_t completedCycles = 0;
    std::uint64_t firstCycleSessionSeq = 0;
    double price = 0.0;
};

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
    if (incident.kind == "buy_trade_pressure" || incident.kind == "sell_trade_pressure") {
        return incident.kind == "buy_trade_pressure"
            ? "Repeated prints lifted the ask, which can indicate buyers pressing the offer and raising short-term execution risk for passive buys."
            : "Repeated prints hit the bid, which can indicate sellers pressing the bid and weakening short-term support for passive sells.";
    }
    if (incident.kind == "ask_display_instability" || incident.kind == "bid_display_instability") {
        return "Displayed touch liquidity repeatedly disappeared and reappeared at the same price, which can make the visible book less trustworthy for queue and fill decisions.";
    }
    if (incident.kind == "buy_fill_invalidation" || incident.kind == "sell_fill_invalidation") {
        return incident.kind == "buy_fill_invalidation"
            ? "After the buy fill, the best bid moved materially lower, which is a strong sign that the immediate execution thesis failed."
            : "After the sell fill, the best ask moved materially higher, which is a strong sign that the immediate execution thesis failed.";
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

std::string eventTimelineStage(const std::string& kind) {
    if (kind == "order_intent") {
        return "intent";
    }
    if (kind == "open_order") {
        return "open";
    }
    if (kind == "order_status") {
        return "status";
    }
    if (kind == "fill_execution") {
        return "fill";
    }
    if (kind == "cancel_request") {
        return "cancel";
    }
    if (kind == "order_reject" || kind == "broker_error") {
        return "error";
    }
    if (kind == "market_tick") {
        return "market_tick";
    }
    if (kind == "market_depth") {
        return "market_depth";
    }
    if (kind == "gap_marker" || kind == "reset_marker") {
        return "data_quality";
    }
    return "event";
}

std::string eventTimelineHeadline(const json& event) {
    const std::string kind = event.value("event_kind", std::string());
    if (kind == "order_intent") {
        return "Order intent submitted";
    }
    if (kind == "open_order") {
        return "Broker open-order update";
    }
    if (kind == "order_status") {
        return "Order status update";
    }
    if (kind == "fill_execution") {
        return "Fill execution";
    }
    if (kind == "cancel_request") {
        return "Cancel requested";
    }
    if (kind == "order_reject") {
        return "Order rejected";
    }
    if (kind == "broker_error") {
        return "Broker error";
    }
    if (kind == "market_tick") {
        return "Market tick";
    }
    if (kind == "market_depth") {
        return "Market depth update";
    }
    if (kind == "gap_marker") {
        return "Feed gap marker";
    }
    if (kind == "reset_marker") {
        return "Feed reset marker";
    }
    return kind.empty() ? "Event" : kind;
}

json timelineEntryFromEvent(const json& event) {
    json entry{
        {"entry_type", "event"},
        {"stage", eventTimelineStage(event.value("event_kind", std::string()))},
        {"headline", eventTimelineHeadline(event)},
        {"kind", event.value("event_kind", std::string())},
        {"session_seq", event.value("session_seq", 0ULL)},
        {"ts_engine_ns", event.value("ts_engine_ns", 0ULL)}
    };
    const std::string note = event.value("note", std::string());
    if (!note.empty()) {
        entry["summary"] = note;
    }
    if (event.contains("anchor")) {
        entry["anchor"] = event["anchor"];
    }
    if (event.contains("price")) {
        entry["price"] = event["price"];
    }
    if (event.contains("size")) {
        entry["size"] = event["size"];
    }
    return entry;
}

json timelineEntryFromFinding(const FindingRecord& finding) {
    return {
        {"entry_type", "finding"},
        {"stage", "finding"},
        {"headline", finding.title},
        {"summary", finding.summary},
        {"kind", finding.kind},
        {"severity", finding.severity},
        {"confidence", finding.confidence},
        {"session_seq", finding.lastSessionSeq},
        {"session_seq_from", finding.firstSessionSeq},
        {"ts_engine_ns", finding.tsEngineNs},
        {"finding_id", finding.findingId},
        {"logical_incident_id", finding.logicalIncidentId}
    };
}

json timelineEntryFromIncident(const IncidentRecord& incident) {
    return {
        {"entry_type", "incident"},
        {"stage", "incident"},
        {"headline", incident.title},
        {"summary", incident.summary},
        {"kind", incident.kind},
        {"severity", incident.severity},
        {"confidence", incident.confidence},
        {"score", incident.score},
        {"session_seq", incident.lastSessionSeq},
        {"session_seq_from", incident.firstSessionSeq},
        {"ts_engine_ns", incident.tsEngineNs},
        {"logical_incident_id", incident.logicalIncidentId},
        {"incident_revision_id", incident.incidentRevisionId}
    };
}

json buildTimelineSummary(const json& timeline) {
    std::size_t eventCount = 0;
    std::size_t findingCount = 0;
    std::size_t incidentCount = 0;
    std::size_t fillCount = 0;
    std::size_t errorCount = 0;
    std::size_t marketCount = 0;
    std::size_t dataQualityCount = 0;
    for (const auto& entry : timeline) {
        const std::string entryType = entry.value("entry_type", std::string());
        const std::string kind = entry.value("kind", std::string());
        if (entryType == "event") {
            ++eventCount;
            if (kind == "fill_execution") {
                ++fillCount;
            } else if (kind == "order_reject" || kind == "broker_error") {
                ++errorCount;
            } else if (kind == "market_tick" || kind == "market_depth") {
                ++marketCount;
            } else if (kind == "gap_marker" || kind == "reset_marker") {
                ++dataQualityCount;
            }
        } else if (entryType == "finding") {
            ++findingCount;
        } else if (entryType == "incident") {
            ++incidentCount;
        }
    }
    return {
        {"timeline_entry_count", timeline.size()},
        {"event_count", eventCount},
        {"finding_count", findingCount},
        {"incident_count", incidentCount},
        {"fill_count", fillCount},
        {"error_count", errorCount},
        {"market_event_count", marketCount},
        {"data_quality_event_count", dataQualityCount}
    };
}

json sortAndTrimTimeline(json timeline, std::size_t limit) {
    std::vector<json> entries = timeline.get<std::vector<json>>();
    std::sort(entries.begin(), entries.end(), [](const json& left, const json& right) {
        const std::uint64_t leftSeq = left.value("session_seq", 0ULL);
        const std::uint64_t rightSeq = right.value("session_seq", 0ULL);
        if (leftSeq != rightSeq) {
            return leftSeq < rightSeq;
        }
        const std::uint64_t leftTs = left.value("ts_engine_ns", 0ULL);
        const std::uint64_t rightTs = right.value("ts_engine_ns", 0ULL);
        if (leftTs != rightTs) {
            return leftTs < rightTs;
        }
        return left.value("entry_type", std::string()) < right.value("entry_type", std::string());
    });
    if (limit > 0 && entries.size() > limit) {
        entries.erase(entries.begin(), entries.end() - static_cast<std::ptrdiff_t>(limit));
    }
    json result = json::array();
    for (const auto& entry : entries) {
        result.push_back(entry);
    }
    return result;
}

json buildTimelineHighlights(const json& timeline, std::size_t limit) {
    json highlights = json::array();
    if (!timeline.is_array() || limit == 0) {
        return highlights;
    }
    const std::size_t start = timeline.size() > limit ? timeline.size() - limit : 0;
    for (std::size_t i = start; i < timeline.size(); ++i) {
        const auto& entry = timeline.at(i);
        highlights.push_back({
            {"entry_type", entry.value("entry_type", std::string())},
            {"headline", entry.value("headline", std::string())},
            {"kind", entry.value("kind", std::string())},
            {"session_seq", entry.value("session_seq", 0ULL)}
        });
    }
    return highlights;
}

json buildDataQualitySummary(const std::vector<json>& events,
                             bool includesMutableTail,
                             const std::string& expectedInstrumentId = {}) {
    std::size_t totalEvents = 0;
    std::size_t marketEventCount = 0;
    std::size_t lifecycleEventCount = 0;
    std::size_t gapMarkerCount = 0;
    std::size_t resetMarkerCount = 0;
    std::size_t weakInstrumentIdentityCount = 0;
    std::size_t missingReceiveTimestampCount = 0;
    std::size_t missingExchangeTimestampCount = 0;
    std::size_t missingVendorSequenceCount = 0;
    std::uint64_t firstSessionSeq = 0;
    std::uint64_t lastSessionSeq = 0;

    for (const auto& event : events) {
        const std::string kind = event.value("event_kind", std::string());
        if (kind.empty()) {
            continue;
        }

        ++totalEvents;
        const std::uint64_t sessionSeq = event.value("session_seq", 0ULL);
        if (firstSessionSeq == 0 || (sessionSeq > 0 && sessionSeq < firstSessionSeq)) {
            firstSessionSeq = sessionSeq;
        }
        lastSessionSeq = std::max(lastSessionSeq, sessionSeq);

        if (kind == "gap_marker") {
            ++gapMarkerCount;
            continue;
        }
        if (kind == "reset_marker") {
            ++resetMarkerCount;
            continue;
        }

        const bool marketEvent = kind == "market_tick" || kind == "market_depth";
        if (marketEvent) {
            ++marketEventCount;
        } else {
            ++lifecycleEventCount;
        }

        const std::string instrumentId = event.value("instrument_id", std::string());
        if (instrumentId.empty() ||
            (!expectedInstrumentId.empty() && instrumentId != expectedInstrumentId) ||
            !isStrongInstrumentId(instrumentId)) {
            ++weakInstrumentIdentityCount;
        }
        if (event.value("ts_receive_ns", 0ULL) == 0ULL) {
            ++missingReceiveTimestampCount;
        }
        if (marketEvent && event.value("ts_exchange_ns", 0ULL) == 0ULL) {
            ++missingExchangeTimestampCount;
        }
        if (marketEvent && event.value("vendor_seq", 0ULL) == 0ULL) {
            ++missingVendorSequenceCount;
        }
    }

    const double marketDenominator = marketEventCount == 0 ? 1.0 : static_cast<double>(marketEventCount);
    double score = 100.0;
    score -= std::min<std::size_t>(2, gapMarkerCount) * 18.0;
    score -= std::min<std::size_t>(2, resetMarkerCount) * 22.0;
    if (weakInstrumentIdentityCount > 0) {
        score -= 14.0;
    }
    score -= 12.0 * (static_cast<double>(missingReceiveTimestampCount) / std::max<double>(1.0, static_cast<double>(totalEvents)));
    score -= 18.0 * (static_cast<double>(missingExchangeTimestampCount) / marketDenominator);
    score -= 10.0 * (static_cast<double>(missingVendorSequenceCount) / marketDenominator);
    if (includesMutableTail) {
        score -= 3.0;
    }
    score = std::clamp(score, 0.0, 100.0);

    std::string grade = "excellent";
    if (score < 90.0) {
        grade = "good";
    }
    if (score < 75.0) {
        grade = "degraded";
    }
    if (score < 55.0) {
        grade = "poor";
    }

    json issues = json::array();
    if (gapMarkerCount > 0) {
        issues.push_back("Feed gaps were recorded in this evidence window.");
    }
    if (resetMarkerCount > 0) {
        issues.push_back("Source resets were recorded in this evidence window.");
    }
    if (weakInstrumentIdentityCount > 0) {
        issues.push_back("At least one event relied on a weak or mismatched instrument identity.");
    }
    if (missingReceiveTimestampCount > 0) {
        issues.push_back("Some events are missing receive timestamps.");
    }
    if (missingExchangeTimestampCount > 0) {
        issues.push_back("Some market events are missing exchange timestamps.");
    }
    if (missingVendorSequenceCount > 0) {
        issues.push_back("Some market events are missing vendor sequence numbers.");
    }
    if (includesMutableTail) {
        issues.push_back("This read includes mutable live-tail evidence in addition to frozen revisions.");
    }

    return {
        {"score", score},
        {"grade", grade},
        {"total_event_count", totalEvents},
        {"market_event_count", marketEventCount},
        {"lifecycle_event_count", lifecycleEventCount},
        {"first_session_seq", firstSessionSeq},
        {"last_session_seq", lastSessionSeq},
        {"gap_marker_count", gapMarkerCount},
        {"reset_marker_count", resetMarkerCount},
        {"weak_instrument_identity_count", weakInstrumentIdentityCount},
        {"missing_receive_timestamp_count", missingReceiveTimestampCount},
        {"missing_exchange_timestamp_count", missingExchangeTimestampCount},
        {"missing_vendor_sequence_count", missingVendorSequenceCount},
        {"includes_mutable_tail", includesMutableTail},
        {"issues", issues}
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

bool rangeOverlaps(std::uint64_t firstSessionSeq,
                   std::uint64_t lastSessionSeq,
                   std::uint64_t fromSessionSeq,
                   std::uint64_t toSessionSeq) {
    if (toSessionSeq != 0 && firstSessionSeq > toSessionSeq) {
        return false;
    }
    if (fromSessionSeq != 0 && lastSessionSeq < fromSessionSeq) {
        return false;
    }
    return true;
}

std::vector<ProtectedWindowRecord> collapseLatestProtectedWindowRevisions(const std::vector<ProtectedWindowRecord>& records) {
    std::map<std::uint64_t, ProtectedWindowRecord> latestByWindowId;
    for (const auto& record : records) {
        auto found = latestByWindowId.find(record.windowId);
        if (found == latestByWindowId.end() ||
            found->second.revisionId < record.revisionId) {
            latestByWindowId[record.windowId] = record;
        }
    }
    std::vector<ProtectedWindowRecord> collapsed;
    collapsed.reserve(latestByWindowId.size());
    for (const auto& entry : latestByWindowId) {
        collapsed.push_back(entry.second);
    }
    std::sort(collapsed.begin(), collapsed.end(), [](const ProtectedWindowRecord& left,
                                                     const ProtectedWindowRecord& right) {
        if (left.lastSessionSeq != right.lastSessionSeq) {
            return left.lastSessionSeq > right.lastSessionSeq;
        }
        return left.windowId > right.windowId;
    });
    return collapsed;
}

template <typename Records, typename KeyFn>
json buildCountSummary(const Records& records, KeyFn&& keyFn) {
    std::map<std::string, std::uint64_t> counts;
    for (const auto& record : records) {
        const std::string key = keyFn(record);
        if (!key.empty()) {
            counts[key] += 1;
        }
    }
    json payload = json::object();
    for (const auto& [key, count] : counts) {
        payload[key] = count;
    }
    return payload;
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
        {"first_session_seq", record.firstSessionSeq},
        {"last_session_seq", record.lastSessionSeq},
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
    record.firstSessionSeq = payload.value("first_session_seq", record.anchorSessionSeq);
    record.lastSessionSeq = payload.value("last_session_seq", record.anchorSessionSeq);
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
    : config_(std::move(config)),
      analyzerRuntime_(std::make_unique<AnalyzerRuntime>()) {}

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
        const QueryArtifacts artifacts = buildQueryArtifacts(snapshot, frozenRevisionId, true);

        std::optional<ProtectedWindowRecord> selectedWindow;
        const auto windowIt = artifacts.latestProtectedWindowById.find(task.protectedWindowId);
        if (windowIt != artifacts.latestProtectedWindowById.end()) {
            selectedWindow = artifacts.protectedWindows[windowIt->second];
        }
        if (!selectedWindow.has_value()) {
            continue;
        }

        json ignoredSummary = json::object();
        std::vector<json> windowEvents;
        try {
            windowEvents = filterEventsByProtectedWindow(snapshot,
                                                        artifacts,
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
        analyzerRuntime_->analyzeDeferred(task.analyzerName, input, &findings);
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
    if (!config_.instrumentId.empty()) {
        if (record.symbol.empty()) {
            return config_.instrumentId;
        }
        const std::string configured = config_.instrumentId;
        const std::string symbol = record.symbol;
        if (isStrongInstrumentId(configured) ||
            configured.size() >= symbol.size() + 1 &&
                configured.compare(configured.size() - symbol.size(), symbol.size(), symbol) == 0) {
            return configured;
        }
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
    record.firstSessionSeq = event.sessionSeq;
    record.lastSessionSeq = event.sessionSeq;
    record.startEngineNs = event.tsEngineNs > kProtectedWindowPreNs ? event.tsEngineNs - kProtectedWindowPreNs : 0;
    record.endEngineNs = event.tsEngineNs + kProtectedWindowPostNs;
    record.reason = reason;
    record.instrumentId = event.instrumentId;
    record.anchor = anchor;

    for (auto it = liveRing_.rbegin(); it != liveRing_.rend(); ++it) {
        if (!record.instrumentId.empty() && it->instrumentId != record.instrumentId) {
            continue;
        }
        if (it->tsEngineNs < record.startEngineNs) {
            break;
        }
        if (it->tsEngineNs <= record.endEngineNs) {
            record.firstSessionSeq = std::min(record.firstSessionSeq, it->sessionSeq);
            record.lastSessionSeq = std::max(record.lastSessionSeq, it->sessionSeq);
        }
    }
    protectedWindows_.push_back(std::move(record));

}

std::optional<std::uint64_t> Server::findReusableLogicalIncidentIdUnlocked(const EngineEvent& event,
                                                                           const FindingRecord& finding,
                                                                           const BridgeAnchorIdentity& overlappingAnchor,
                                                                           bool overlapsOrder) const {
    std::unordered_set<std::uint64_t> seenLogicalIncidents;
    for (auto it = incidents_.rbegin(); it != incidents_.rend(); ++it) {
        if (!seenLogicalIncidents.insert(it->logicalIncidentId).second) {
            continue;
        }
        if (it->kind != finding.kind || it->instrumentId != finding.instrumentId) {
            continue;
        }
        if (it->overlapsOrder != overlapsOrder) {
            continue;
        }
        if (overlapsOrder) {
            if (hasAnchorIdentity(overlappingAnchor) &&
                anchorsShareIdentity(it->overlappingAnchor, overlappingAnchor)) {
                return it->logicalIncidentId;
            }
            continue;
        }
        if (finding.tsEngineNs <= it->tsEngineNs + kIncidentMergeWindowNs) {
            return it->logicalIncidentId;
        }
        for (auto windowIt = protectedWindows_.rbegin(); windowIt != protectedWindows_.rend(); ++windowIt) {
            if (windowIt->logicalIncidentId != it->logicalIncidentId ||
                windowIt->reason != "incident_promotion") {
                continue;
            }
            if (!windowIt->instrumentId.empty() && windowIt->instrumentId != event.instrumentId) {
                continue;
            }
            if ((event.sessionSeq >= windowIt->firstSessionSeq && event.sessionSeq <= windowIt->lastSessionSeq) ||
                (event.tsEngineNs >= windowIt->startEngineNs && event.tsEngineNs <= windowIt->endEngineNs)) {
                return it->logicalIncidentId;
            }
            break;
        }
    }
    return std::nullopt;
}

void Server::upsertIncidentProtectedWindowUnlocked(const EngineEvent& event,
                                                   const BridgeAnchorIdentity& anchor,
                                                   std::uint64_t logicalIncidentId) {
    ProtectedWindowRecord* latestWindow = nullptr;
    for (auto it = protectedWindows_.rbegin(); it != protectedWindows_.rend(); ++it) {
        if (it->logicalIncidentId == logicalIncidentId && it->reason == "incident_promotion") {
            latestWindow = &(*it);
            break;
        }
    }
    if (latestWindow == nullptr) {
        addProtectedWindowUnlocked(event, "incident_promotion", anchor, logicalIncidentId);
        return;
    }

    const std::uint64_t eventStartEngineNs =
        event.tsEngineNs > kProtectedWindowPreNs ? event.tsEngineNs - kProtectedWindowPreNs : 0;
    const std::uint64_t eventEndEngineNs = event.tsEngineNs + kProtectedWindowPostNs;
    const bool alreadyCovered =
        event.sessionSeq >= latestWindow->firstSessionSeq &&
        event.sessionSeq <= latestWindow->lastSessionSeq &&
        event.tsEngineNs >= latestWindow->startEngineNs &&
        event.tsEngineNs <= latestWindow->endEngineNs;
    if (alreadyCovered) {
        return;
    }

    ProtectedWindowRecord updated = *latestWindow;
    updated.revisionId = event.revisionId;
    updated.anchorSessionSeq = updated.anchorSessionSeq == 0
        ? event.sessionSeq
        : std::min(updated.anchorSessionSeq, event.sessionSeq);
    updated.firstSessionSeq = updated.firstSessionSeq == 0
        ? event.sessionSeq
        : std::min(updated.firstSessionSeq, event.sessionSeq);
    updated.lastSessionSeq = std::max(updated.lastSessionSeq, event.sessionSeq);
    updated.startEngineNs = std::min(updated.startEngineNs, eventStartEngineNs);
    updated.endEngineNs = std::max(updated.endEngineNs, eventEndEngineNs);
    if (!hasAnchorIdentity(updated.anchor) && hasAnchorIdentity(anchor)) {
        updated.anchor = anchor;
    }

    for (auto it = liveRing_.rbegin(); it != liveRing_.rend(); ++it) {
        if (!updated.instrumentId.empty() && it->instrumentId != updated.instrumentId) {
            continue;
        }
        if (it->tsEngineNs < updated.startEngineNs) {
            break;
        }
        if (it->tsEngineNs <= updated.endEngineNs) {
            updated.firstSessionSeq = std::min(updated.firstSessionSeq, it->sessionSeq);
            updated.lastSessionSeq = std::max(updated.lastSessionSeq, it->sessionSeq);
        }
    }
    protectedWindows_.push_back(std::move(updated));
}

void Server::updateProtectedWindowBoundsUnlocked(const EngineEvent& event) {
    std::unordered_set<std::uint64_t> seenWindowIds;
    std::vector<ProtectedWindowRecord> updatedRecords;
    for (auto it = protectedWindows_.rbegin(); it != protectedWindows_.rend(); ++it) {
        if (!seenWindowIds.insert(it->windowId).second) {
            continue;
        }
        if (!it->instrumentId.empty() && it->instrumentId != event.instrumentId) {
            continue;
        }
        if (event.tsEngineNs > it->endEngineNs) {
            if (it->instrumentId == event.instrumentId) {
                break;
            }
            continue;
        }
        if (event.tsEngineNs < it->startEngineNs) {
            continue;
        }
        const std::uint64_t updatedFirstSessionSeq =
            it->firstSessionSeq == 0 ? event.sessionSeq : std::min(it->firstSessionSeq, event.sessionSeq);
        const std::uint64_t updatedLastSessionSeq = std::max(it->lastSessionSeq, event.sessionSeq);
        if (updatedFirstSessionSeq == it->firstSessionSeq && updatedLastSessionSeq == it->lastSessionSeq) {
            continue;
        }

        ProtectedWindowRecord updated = *it;
        const std::uint64_t previousLastSessionSeq = updated.lastSessionSeq;
        updated.revisionId = event.revisionId;
        updated.firstSessionSeq = updatedFirstSessionSeq;
        updated.lastSessionSeq = updatedLastSessionSeq;
        updatedRecords.push_back(updated);

        if (updated.logicalIncidentId == 0 &&
            hasAnchorIdentity(updated.anchor) &&
            previousLastSessionSeq <= updated.anchorSessionSeq &&
            updated.lastSessionSeq > updated.anchorSessionSeq &&
            (updated.reason == "order_intent" ||
             updated.reason == "open_order" ||
             updated.reason == "order_status" ||
             updated.reason == "fill_execution" ||
             updated.reason == "cancel_request" ||
             updated.reason == "order_reject" ||
             updated.reason == "broker_error")) {
            DeferredAnalyzerTask task;
            task.sourceRevisionId = event.revisionId;
            task.protectedWindowId = updated.windowId;
            task.logicalIncidentId = updated.logicalIncidentId;
            task.instrumentId = updated.instrumentId;
            task.reason = updated.reason;
            task.anchor = updated.anchor;
            enqueueDeferredAnalyzerTask(std::move(task));
        }
    }
    for (auto& updated : updatedRecords) {
        protectedWindows_.push_back(std::move(updated));
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
    const std::optional<std::uint64_t> reusableLogicalIncidentId =
        findReusableLogicalIncidentIdUnlocked(event, finding, overlappingAnchor, overlapsOrder);
    if (reusableLogicalIncidentId.has_value()) {
        for (auto it = incidents_.rbegin(); it != incidents_.rend(); ++it) {
            if (it->logicalIncidentId != *reusableLogicalIncidentId) {
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
            incident.firstSessionSeq = incident.firstSessionSeq == 0
                ? finding.firstSessionSeq
                : std::min(incident.firstSessionSeq, finding.firstSessionSeq);
            incident.lastSessionSeq = std::max(incident.lastSessionSeq, finding.lastSessionSeq);
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
            upsertIncidentProtectedWindowUnlocked(event, overlappingAnchor, incidents_.back().logicalIncidentId);
            return;
        }
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

    upsertIncidentProtectedWindowUnlocked(event, overlappingAnchor, incidents_.back().logicalIncidentId);
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

    updateProtectedWindowBoundsUnlocked(event);

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

    bool fillInvalidationTriggered = false;
    std::string fillInvalidationKind;
    std::uint64_t fillInvalidationFirstSessionSeq = 0;
    double fillInvalidationFillPrice = 0.0;
    double fillInvalidationObservedPrice = 0.0;
    if (analyzerBookState_.activeFillWatch.has_value()) {
        const auto& watch = *analyzerBookState_.activeFillWatch;
        if (event.tsEngineNs > watch.expiryTsEngineNs) {
            analyzerBookState_.activeFillWatch.reset();
        } else if (event.sessionSeq > watch.fillSessionSeq &&
                   hasInside &&
                   (event.eventKind == "market_tick" || event.eventKind == "market_depth")) {
            if (watch.isBuy &&
                effectiveBid > 0.0 &&
                effectiveBid <= watch.fillPrice - kFillInvalidationMinMove) {
                fillInvalidationTriggered = true;
                fillInvalidationKind = "buy_fill_invalidation";
                fillInvalidationFirstSessionSeq = watch.fillSessionSeq;
                fillInvalidationFillPrice = watch.fillPrice;
                fillInvalidationObservedPrice = effectiveBid;
                analyzerBookState_.activeFillWatch.reset();
            } else if (!watch.isBuy &&
                       effectiveAsk > 0.0 &&
                       effectiveAsk >= watch.fillPrice + kFillInvalidationMinMove) {
                fillInvalidationTriggered = true;
                fillInvalidationKind = "sell_fill_invalidation";
                fillInvalidationFirstSessionSeq = watch.fillSessionSeq;
                fillInvalidationFillPrice = watch.fillPrice;
                fillInvalidationObservedPrice = effectiveAsk;
                analyzerBookState_.activeFillWatch.reset();
            }
        }
    }

    bool tradePressureTriggered = false;
    std::string tradePressureKind;
    const int tradePressureSide = classifyTradePressureSide(event, effectiveBid, effectiveAsk, hasInside);
    if (tradePressureSide == 0) {
        analyzerBookState_.tradePressureSide = 0;
        analyzerBookState_.tradePressureStreakCount = 0;
        analyzerBookState_.tradePressureFirstSessionSeq = 0;
        analyzerBookState_.tradePressureLastSessionSeq = 0;
        analyzerBookState_.tradePressureReferencePrice = 0.0;
    } else {
        if (analyzerBookState_.tradePressureSide == tradePressureSide) {
            ++analyzerBookState_.tradePressureStreakCount;
        } else {
            analyzerBookState_.tradePressureSide = tradePressureSide;
            analyzerBookState_.tradePressureStreakCount = 1;
            analyzerBookState_.tradePressureFirstSessionSeq = event.sessionSeq;
        }
        analyzerBookState_.tradePressureLastSessionSeq = event.sessionSeq;
        analyzerBookState_.tradePressureReferencePrice = event.bridgeRecord.price;
        if (analyzerBookState_.tradePressureStreakCount == kTradePressureMinStreak) {
            tradePressureTriggered = true;
            tradePressureKind = tradePressureSide > 0 ? "buy_trade_pressure" : "sell_trade_pressure";
        }
    }

    auto resetDisplaySide = [](auto* state, double trackedPrice) {
        state->trackedPrice = trackedPrice;
        state->sawRecentRemoval = false;
        state->removalSessionSeq = 0;
        state->firstCycleSessionSeq = 0;
        state->completedCycles = 0;
    };

    auto updateDisplaySide = [&](auto* state,
                                 const std::string& kind,
                                 double previousPrice,
                                 double currentPrice,
                                 double previousSize,
                                 double currentSize) {
        DisplayInstabilityResult result;
        if (previousPrice <= 0.0 || currentPrice <= 0.0) {
            resetDisplaySide(state, currentPrice);
            return std::pair<std::string, DisplayInstabilityResult>{std::string(), result};
        }
        if (state->trackedPrice <= 0.0 || std::fabs(state->trackedPrice - currentPrice) > kTouchTradeTolerance) {
            resetDisplaySide(state, currentPrice);
        }
        if (std::fabs(previousPrice - currentPrice) > kTouchTradeTolerance) {
            resetDisplaySide(state, currentPrice);
            return std::pair<std::string, DisplayInstabilityResult>{std::string(), result};
        }

        const double delta = currentSize - previousSize;
        const double ratio = previousSize > 0.0 ? std::fabs(delta) / previousSize : 0.0;
        const bool removal = previousSize >= kLiquidityChangeMinShares &&
                             delta <= -kLiquidityChangeMinShares &&
                             ratio >= kLiquidityChangeMinRatio;
        const bool refill = previousSize > 0.0 &&
                            delta >= kLiquidityChangeMinShares &&
                            ratio >= kLiquidityChangeMinRatio;

        if (removal) {
            state->sawRecentRemoval = true;
            state->removalSessionSeq = event.sessionSeq;
            if (state->firstCycleSessionSeq == 0) {
                state->firstCycleSessionSeq = event.sessionSeq;
            }
            return std::pair<std::string, DisplayInstabilityResult>{std::string(), result};
        }

        if (state->sawRecentRemoval &&
            refill &&
            event.sessionSeq >= state->removalSessionSeq &&
            event.sessionSeq - state->removalSessionSeq <= kDisplayInstabilitySeqWindow) {
            state->completedCycles += 1;
            state->sawRecentRemoval = false;
            if (state->firstCycleSessionSeq == 0) {
                state->firstCycleSessionSeq = state->removalSessionSeq;
            }
            if (state->completedCycles >= kDisplayInstabilityMinCycles) {
                result.triggered = true;
                result.completedCycles = state->completedCycles;
                result.firstCycleSessionSeq = state->firstCycleSessionSeq;
                result.price = currentPrice;
                return std::pair<std::string, DisplayInstabilityResult>{kind, result};
            }
            return std::pair<std::string, DisplayInstabilityResult>{std::string(), result};
        }

        if (state->sawRecentRemoval &&
            event.sessionSeq > state->removalSessionSeq + kDisplayInstabilitySeqWindow) {
            state->sawRecentRemoval = false;
        }
        return std::pair<std::string, DisplayInstabilityResult>{std::string(), result};
    };

    bool displayInstabilityTriggered = false;
    std::string displayInstabilityKind;
    std::size_t displayInstabilityCycles = 0;
    std::uint64_t displayInstabilityFirstSessionSeq = 0;
    double displayInstabilityPrice = 0.0;
    if (event.eventKind == "market_depth" && hadInside && hasInside) {
        if (previousAsk > 0.0 && std::fabs(previousAsk - effectiveAsk) <= kTouchTradeTolerance) {
            const auto [kind, result] = updateDisplaySide(&analyzerBookState_.askDisplayInstability,
                                                          "ask_display_instability",
                                                          previousAsk,
                                                          effectiveAsk,
                                                          previousAskSize,
                                                          effectiveAskSize);
            if (result.triggered) {
                displayInstabilityTriggered = true;
                displayInstabilityKind = kind;
                displayInstabilityCycles = result.completedCycles;
                displayInstabilityFirstSessionSeq = result.firstCycleSessionSeq;
                displayInstabilityPrice = result.price;
            }
        } else if (effectiveAsk > 0.0) {
            resetDisplaySide(&analyzerBookState_.askDisplayInstability, effectiveAsk);
        }

        if (!displayInstabilityTriggered &&
            previousBid > 0.0 &&
            std::fabs(previousBid - effectiveBid) <= kTouchTradeTolerance) {
            const auto [kind, result] = updateDisplaySide(&analyzerBookState_.bidDisplayInstability,
                                                          "bid_display_instability",
                                                          previousBid,
                                                          effectiveBid,
                                                          previousBidSize,
                                                          effectiveBidSize);
            if (result.triggered) {
                displayInstabilityTriggered = true;
                displayInstabilityKind = kind;
                displayInstabilityCycles = result.completedCycles;
                displayInstabilityFirstSessionSeq = result.firstCycleSessionSeq;
                displayInstabilityPrice = result.price;
            }
        } else if (effectiveBid > 0.0) {
            resetDisplaySide(&analyzerBookState_.bidDisplayInstability, effectiveBid);
        }
    }

    analyzerBookState_.lastEffectiveBid = effectiveBid;
    analyzerBookState_.lastEffectiveAsk = effectiveAsk;
    analyzerBookState_.lastEffectiveBidSize = effectiveBidSize;
    analyzerBookState_.lastEffectiveAskSize = effectiveAskSize;
    analyzerBookState_.hasInside = hasInside;

    const BridgeAnchorIdentity overlap = findOverlappingOrderAnchorUnlocked(event.tsEngineNs);
    BridgeAnchorIdentity effectiveOverlap = overlap;
    if (fillInvalidationTriggered && !hasAnchorIdentity(effectiveOverlap) && hasAnchorIdentity(event.bridgeRecord.anchor)) {
        effectiveOverlap = event.bridgeRecord.anchor;
    }
    const bool overlapsOrder = hasAnchorIdentity(effectiveOverlap);

    if (event.eventKind == "fill_execution" &&
        hasAnchorIdentity(event.bridgeRecord.anchor) &&
        std::isfinite(event.bridgeRecord.price)) {
        const std::optional<bool> fillSide = classifyFillSide(event.bridgeRecord.side);
        if (fillSide.has_value()) {
            AnalyzerBookState::ActiveFillWatch watch;
            watch.anchor = event.bridgeRecord.anchor;
            watch.instrumentId = event.instrumentId;
            watch.isBuy = *fillSide;
            watch.fillPrice = event.bridgeRecord.price;
            watch.fillSessionSeq = event.sessionSeq;
            watch.fillTsEngineNs = event.tsEngineNs;
            watch.expiryTsEngineNs = event.tsEngineNs + kProtectedWindowPostNs;
            analyzerBookState_.activeFillWatch = std::move(watch);
        }
    }

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
        .overlappingAnchor = effectiveOverlap,
        .tradePressureTriggered = tradePressureTriggered,
        .tradePressureKind = tradePressureKind,
        .tradePressureStreakCount = analyzerBookState_.tradePressureStreakCount,
        .tradePressureFirstSessionSeq = analyzerBookState_.tradePressureFirstSessionSeq,
        .tradePressureReferencePrice = analyzerBookState_.tradePressureReferencePrice,
        .displayInstabilityTriggered = displayInstabilityTriggered,
        .displayInstabilityKind = displayInstabilityKind,
        .displayInstabilityCycles = displayInstabilityCycles,
        .displayInstabilityFirstSessionSeq = displayInstabilityFirstSessionSeq,
        .displayInstabilityPrice = displayInstabilityPrice,
        .fillInvalidationTriggered = fillInvalidationTriggered,
        .fillInvalidationKind = fillInvalidationKind,
        .fillInvalidationFirstSessionSeq = fillInvalidationFirstSessionSeq,
        .fillInvalidationFillPrice = fillInvalidationFillPrice,
        .fillInvalidationObservedPrice = fillInvalidationObservedPrice
    };

    std::vector<AnalyzerFindingSpec> analyzerFindings;
    analyzerRuntime_->analyzeHot(input, &analyzerFindings);

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
        if (record.firstSessionSeq > 0) {
            firstSessionSeq = std::min(firstSessionSeq, record.firstSessionSeq);
        }
        if (record.lastSessionSeq > 0) {
            lastSessionSeq = std::max(lastSessionSeq, record.lastSessionSeq);
        }
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

Server::QueryArtifacts Server::buildQueryArtifacts(const QuerySnapshot& snapshot,
                                                   std::uint64_t frozenRevisionId,
                                                   bool includeLiveTail) const {
    QueryArtifacts artifacts;

    for (const auto& record : snapshot.orderAnchors) {
        if (record.revisionId <= frozenRevisionId || (includeLiveTail && record.revisionId > frozenRevisionId)) {
            artifacts.orderAnchors.push_back(record);
        }
    }
    for (const auto& record : snapshot.protectedWindows) {
        if (record.revisionId <= frozenRevisionId || (includeLiveTail && record.revisionId > frozenRevisionId)) {
            artifacts.protectedWindows.push_back(record);
        }
    }
    for (const auto& record : snapshot.findings) {
        if (record.revisionId <= frozenRevisionId || (includeLiveTail && record.revisionId > frozenRevisionId)) {
            artifacts.findings.push_back(record);
        }
    }
    for (const auto& record : snapshot.incidents) {
        if (record.revisionId <= frozenRevisionId || (includeLiveTail && record.revisionId > frozenRevisionId)) {
            artifacts.incidents.push_back(record);
        }
    }

    std::sort(artifacts.orderAnchors.begin(), artifacts.orderAnchors.end(), [](const OrderAnchorRecord& left,
                                                                                const OrderAnchorRecord& right) {
        return std::tie(left.sessionSeq, left.anchorId) < std::tie(right.sessionSeq, right.anchorId);
    });
    std::sort(artifacts.protectedWindows.begin(), artifacts.protectedWindows.end(), [](const ProtectedWindowRecord& left,
                                                                                        const ProtectedWindowRecord& right) {
        return std::tie(left.firstSessionSeq, left.revisionId, left.windowId) <
               std::tie(right.firstSessionSeq, right.revisionId, right.windowId);
    });
    std::sort(artifacts.findings.begin(), artifacts.findings.end(), [](const FindingRecord& left,
                                                                        const FindingRecord& right) {
        return std::tie(left.lastSessionSeq, left.findingId) < std::tie(right.lastSessionSeq, right.findingId);
    });
    std::sort(artifacts.incidents.begin(), artifacts.incidents.end(), [](const IncidentRecord& left,
                                                                          const IncidentRecord& right) {
        return std::tie(left.lastSessionSeq, left.incidentRevisionId) <
               std::tie(right.lastSessionSeq, right.incidentRevisionId);
    });

    for (std::size_t i = 0; i < artifacts.orderAnchors.size(); ++i) {
        for (const auto& key : anchorSelectorKeys(artifacts.orderAnchors[i].anchor)) {
            artifacts.orderAnchorsBySelector.emplace(key, i);
        }
    }
    for (std::size_t i = 0; i < artifacts.protectedWindows.size(); ++i) {
        const auto& window = artifacts.protectedWindows[i];
        for (const auto& key : anchorSelectorKeys(window.anchor)) {
            artifacts.protectedWindowsBySelector.emplace(key, i);
        }
        auto existing = artifacts.latestProtectedWindowById.find(window.windowId);
        if (existing == artifacts.latestProtectedWindowById.end() ||
            artifacts.protectedWindows[existing->second].revisionId < window.revisionId) {
            artifacts.latestProtectedWindowById[window.windowId] = i;
        }
    }
    for (std::size_t i = 0; i < artifacts.findings.size(); ++i) {
        if (artifacts.findings[i].logicalIncidentId > 0) {
            artifacts.findingsByIncident.emplace(artifacts.findings[i].logicalIncidentId, i);
        }
    }
    for (std::size_t i = 0; i < artifacts.incidents.size(); ++i) {
        if (artifacts.incidents[i].logicalIncidentId > 0) {
            artifacts.incidentsByLogicalIncident.emplace(artifacts.incidents[i].logicalIncidentId, i);
        }
    }

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
                                               const QueryArtifacts& artifacts,
                                               std::uint64_t traceId,
                                               long long orderId,
                                               long long permId,
                                               const std::string& execId,
                                               std::size_t limit,
                                               std::uint64_t frozenRevisionId,
                                               bool includeLiveTail) const {
    const std::vector<std::string> selectorKeys = anchorSelectorKeys(traceId, orderId, permId, execId);
    if (selectorKeys.empty()) {
        return {};
    }

    std::unordered_set<std::size_t> matchedAnchorIndexes;
    std::uint64_t fromSessionSeq = 0;
    std::uint64_t throughSessionSeq = 0;
    for (const auto& key : selectorKeys) {
        const auto range = artifacts.orderAnchorsBySelector.equal_range(key);
        for (auto it = range.first; it != range.second; ++it) {
            const OrderAnchorRecord& record = artifacts.orderAnchors[it->second];
            if (!matchesAnchorSelector(record.anchor, traceId, orderId, permId, execId) ||
                !matchedAnchorIndexes.insert(it->second).second) {
                continue;
            }
            fromSessionSeq = fromSessionSeq == 0 ? record.sessionSeq : std::min(fromSessionSeq, record.sessionSeq);
            throughSessionSeq = std::max(throughSessionSeq, record.sessionSeq);
        }
    }

    std::unordered_set<std::size_t> matchedWindowIndexes;
    for (const auto& key : selectorKeys) {
        const auto range = artifacts.protectedWindowsBySelector.equal_range(key);
        for (auto it = range.first; it != range.second; ++it) {
            const ProtectedWindowRecord& window = artifacts.protectedWindows[it->second];
            if (!matchesAnchorSelector(window.anchor, traceId, orderId, permId, execId) ||
                !matchedWindowIndexes.insert(it->second).second) {
                continue;
            }
            const std::uint64_t windowFirst = window.firstSessionSeq > 0 ? window.firstSessionSeq : window.anchorSessionSeq;
            const std::uint64_t windowLast = window.lastSessionSeq > 0 ? window.lastSessionSeq : window.anchorSessionSeq;
            fromSessionSeq = fromSessionSeq == 0 ? windowFirst : std::min(fromSessionSeq, windowFirst);
            throughSessionSeq = std::max(throughSessionSeq, windowLast);
        }
    }

    if (matchedAnchorIndexes.empty() && matchedWindowIndexes.empty()) {
        return {};
    }

    std::vector<json> results;
    const auto allEvents = mergedEvents(snapshot,
                                        frozenRevisionId,
                                        includeLiveTail,
                                        fromSessionSeq,
                                        throughSessionSeq);
    for (const auto& event : allEvents) {
        const json anchor = event.value("anchor", json::object());
        if (!matchesAnchorSelector(anchorFromJson(anchor), traceId, orderId, permId, execId)) {
            continue;
        }
        results.push_back(event);
        if (limit > 0 && results.size() >= limit) {
            break;
        }
    }
    return results;
}

json Server::buildOrderCaseSummary(const QuerySnapshot& snapshot,
                                   const QueryArtifacts& artifacts,
                                   const std::vector<json>& events,
                                   std::uint64_t traceId,
                                   long long orderId,
                                   long long permId,
                                   const std::string& execId,
                                   std::uint64_t frozenRevisionId,
                                   bool includeLiveTail) const {
    if (events.empty()) {
        throw std::runtime_error("order/fill anchor not found");
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
    std::optional<ProtectedWindowRecord> selectedWindow;
    const std::vector<std::string> selectorKeys = anchorSelectorKeys(traceId, orderId, permId, execId);
    std::unordered_set<std::size_t> matchedWindowIndexes;
    for (const auto& key : selectorKeys) {
        const auto range = artifacts.protectedWindowsBySelector.equal_range(key);
        for (auto it = range.first; it != range.second; ++it) {
            if (!matchedWindowIndexes.insert(it->second).second) {
                continue;
            }
            const auto& window = artifacts.protectedWindows[it->second];
            if (!matchesAnchorSelector(window.anchor, traceId, orderId, permId, execId)) {
                continue;
            }
            if (!selectedWindow.has_value() || selectedWindow->revisionId < window.revisionId) {
                selectedWindow = window;
            }
        }
    }

    const std::uint64_t replayTargetSessionSeq = lastFillSessionSeq > 0
        ? lastFillSessionSeq
        : (latestStatusSessionSeq > 0 ? latestStatusSessionSeq
                                      : (latestOpenOrderSessionSeq > 0 ? latestOpenOrderSessionSeq : lastSessionSeq));
    const std::uint64_t replayFromSessionSeq = firstSessionSeq > 5 ? firstSessionSeq - 5 : 1;
    const std::uint64_t replayToSessionSeq = std::max(lastSessionSeq, replayTargetSessionSeq + 5);

    json summary = {
        {"anchor", anchor},
        {"first_fill_session_seq", firstFillSessionSeq},
        {"first_session_seq", firstSessionSeq},
        {"includes_mutable_tail", includeLiveTail},
        {"last_fill_session_seq", lastFillSessionSeq},
        {"last_session_seq", lastSessionSeq},
        {"latest_open_order_session_seq", latestOpenOrderSessionSeq},
        {"latest_order_status_session_seq", latestStatusSessionSeq},
        {"replay_from_session_seq", replayFromSessionSeq},
        {"replay_target_session_seq", replayTargetSessionSeq},
        {"replay_to_session_seq", replayToSessionSeq},
        {"returned_events", events.size()},
        {"served_revision_id", frozenRevisionId}
    };
    if (selectedWindow.has_value()) {
        summary["protected_window"] = protectedWindowToJson(*selectedWindow);
    }
    return summary;
}

std::vector<json> Server::filterEventsByProtectedWindow(const QuerySnapshot& snapshot,
                                                        const QueryArtifacts& artifacts,
                                                        std::uint64_t windowId,
                                                        std::size_t limit,
                                                        std::uint64_t frozenRevisionId,
                                                        bool includeLiveTail,
                                                        json* selectedWindowSummary) const {
    std::optional<ProtectedWindowRecord> selectedWindow;
    const auto it = artifacts.latestProtectedWindowById.find(windowId);
    if (it != artifacts.latestProtectedWindowById.end()) {
        selectedWindow = artifacts.protectedWindows[it->second];
    }

    if (!selectedWindow.has_value()) {
        throw std::runtime_error("protected window not found");
    }

    if (selectedWindowSummary != nullptr) {
        *selectedWindowSummary = protectedWindowToJson(*selectedWindow);
    }

    std::vector<json> results;
    const std::uint64_t fromSessionSeq = selectedWindow->firstSessionSeq > 0
        ? selectedWindow->firstSessionSeq
        : selectedWindow->anchorSessionSeq;
    const std::uint64_t throughSessionSeq = selectedWindow->lastSessionSeq > 0
        ? selectedWindow->lastSessionSeq
        : selectedWindow->anchorSessionSeq;
    const std::vector<json> allEvents = mergedEvents(snapshot,
                                                     frozenRevisionId,
                                                     includeLiveTail,
                                                     fromSessionSeq,
                                                     throughSessionSeq);
    for (const auto& event : allEvents) {
        const std::uint64_t sessionSeq = event.value("session_seq", 0ULL);
        if (selectedWindow->firstSessionSeq > 0 && sessionSeq < selectedWindow->firstSessionSeq) {
            continue;
        }
        if (selectedWindow->lastSessionSeq > 0 && sessionSeq > selectedWindow->lastSessionSeq) {
            continue;
        }
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
        {"data_quality", buildDataQualitySummary(allEvents, includeLiveTail, snapshot.instrumentId)},
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

    if (request.operation == "read_session_quality") {
        std::uint64_t frozenRevisionId = 0;
        try {
            frozenRevisionId = resolveFrozenRevision(snapshot, request.revisionId);
        } catch (const std::exception& error) {
            return rejectResponse(request, error.what());
        }
        const std::uint64_t from = request.fromSessionSeq;
        const std::uint64_t to = request.toSessionSeq;
        const std::vector<json> events = mergedEvents(snapshot,
                                                      frozenRevisionId,
                                                      request.includeLiveTail,
                                                      from,
                                                      to);
        response.summary = {
            {"from_session_seq", from},
            {"to_session_seq", to},
            {"served_revision_id", frozenRevisionId},
            {"data_quality", buildDataQualitySummary(events, request.includeLiveTail, snapshot.instrumentId)}
        };
        return response;
    }

    if (request.operation == "read_session_overview") {
        std::uint64_t frozenRevisionId = 0;
        try {
            frozenRevisionId = resolveFrozenRevision(snapshot, request.revisionId);
        } catch (const std::exception& error) {
            return rejectResponse(request, error.what());
        }

        const std::uint64_t from = request.fromSessionSeq;
        const std::uint64_t to = request.toSessionSeq == 0
            ? (snapshot.nextSessionSeq == 0 ? 0 : snapshot.nextSessionSeq - 1)
            : request.toSessionSeq;
        const std::size_t limit = request.limit == 0 ? 5 : request.limit;
        const QueryArtifacts artifacts = buildQueryArtifacts(snapshot, frozenRevisionId, request.includeLiveTail);
        const std::vector<json> allEvents = mergedEvents(snapshot,
                                                         frozenRevisionId,
                                                         request.includeLiveTail,
                                                         from,
                                                         to);
        const std::vector<json> timelineEvents = filterEventsByRange(snapshot,
                                                                     from == 0 ? 1 : from,
                                                                     to,
                                                                     std::min<std::size_t>(32, std::max<std::size_t>(12, limit * 4)),
                                                                     frozenRevisionId,
                                                                     request.includeLiveTail);

        std::vector<IncidentRecord> topIncidents;
        for (const auto& incident : collapseLatestIncidentRevisions(artifacts.incidents)) {
            if (rangeOverlaps(incident.firstSessionSeq, incident.lastSessionSeq, from, to)) {
                topIncidents.push_back(incident);
            }
        }

        std::vector<FindingRecord> topFindings;
        for (const auto& finding : artifacts.findings) {
            if (rangeOverlaps(finding.firstSessionSeq, finding.lastSessionSeq, from, to)) {
                topFindings.push_back(finding);
            }
        }
        std::sort(topFindings.begin(), topFindings.end(), [](const FindingRecord& left,
                                                             const FindingRecord& right) {
            if (left.lastSessionSeq != right.lastSessionSeq) {
                return left.lastSessionSeq > right.lastSessionSeq;
            }
            return left.findingId > right.findingId;
        });

        std::vector<ProtectedWindowRecord> protectedWindows;
        for (const auto& window : collapseLatestProtectedWindowRevisions(artifacts.protectedWindows)) {
            if (rangeOverlaps(window.firstSessionSeq, window.lastSessionSeq, from, to)) {
                protectedWindows.push_back(window);
            }
        }

        std::vector<OrderAnchorRecord> orderAnchors;
        for (const auto& anchor : artifacts.orderAnchors) {
            if (rangeOverlaps(anchor.sessionSeq, anchor.sessionSeq, from, to)) {
                orderAnchors.push_back(anchor);
            }
        }
        std::sort(orderAnchors.begin(), orderAnchors.end(), [](const OrderAnchorRecord& left,
                                                               const OrderAnchorRecord& right) {
            if (left.sessionSeq != right.sessionSeq) {
                return left.sessionSeq > right.sessionSeq;
            }
            return left.anchorId > right.anchorId;
        });

        json timeline = json::array();
        for (const auto& event : timelineEvents) {
            timeline.push_back(timelineEntryFromEvent(event));
        }
        for (std::size_t i = 0; i < topFindings.size() && i < limit; ++i) {
            timeline.push_back(timelineEntryFromFinding(topFindings[i]));
        }
        for (std::size_t i = 0; i < topIncidents.size() && i < limit; ++i) {
            timeline.push_back(timelineEntryFromIncident(topIncidents[i]));
        }
        timeline = sortAndTrimTimeline(std::move(timeline), 24);
        const json timelineSummary = buildTimelineSummary(timeline);

        response.events = json::array();
        for (std::size_t i = 0; i < topIncidents.size() && i < limit; ++i) {
            response.events.push_back(incidentToJson(topIncidents[i]));
        }

        json topFindingsJson = json::array();
        for (std::size_t i = 0; i < topFindings.size() && i < limit; ++i) {
            topFindingsJson.push_back(findingToJson(topFindings[i]));
        }

        json topWindowsJson = json::array();
        for (std::size_t i = 0; i < protectedWindows.size() && i < limit; ++i) {
            topWindowsJson.push_back(protectedWindowToJson(protectedWindows[i]));
        }

        json topAnchorsJson = json::array();
        for (std::size_t i = 0; i < orderAnchors.size() && i < limit; ++i) {
            topAnchorsJson.push_back(orderAnchorToJson(orderAnchors[i]));
        }

        json reportSummary{
            {"headline", "Session overview"},
            {"summary", "Ranked incidents, findings, protected windows, and data-quality scoring for the requested session range."},
            {"timeline_highlights", buildTimelineHighlights(timeline, 5)},
            {"top_incident_kind", topIncidents.empty() ? std::string() : topIncidents.front().kind},
            {"top_incident_title", topIncidents.empty() ? std::string() : topIncidents.front().title}
        };

        response.summary = {
            {"from_session_seq", from},
            {"to_session_seq", to},
            {"served_revision_id", frozenRevisionId},
            {"includes_mutable_tail", request.includeLiveTail},
            {"returned_events", response.events.size()},
            {"incident_count", topIncidents.size()},
            {"finding_count", topFindings.size()},
            {"protected_window_count", protectedWindows.size()},
            {"order_anchor_count", orderAnchors.size()},
            {"incident_kind_counts", buildCountSummary(topIncidents, [](const IncidentRecord& record) { return record.kind; })},
            {"finding_kind_counts", buildCountSummary(topFindings, [](const FindingRecord& record) { return record.kind; })},
            {"protected_window_reason_counts", buildCountSummary(protectedWindows, [](const ProtectedWindowRecord& record) { return record.reason; })},
            {"top_findings", topFindingsJson},
            {"top_protected_windows", topWindowsJson},
            {"top_order_anchors", topAnchorsJson},
            {"timeline", timeline},
            {"timeline_summary", timelineSummary},
            {"report_summary", reportSummary},
            {"data_quality", buildDataQualitySummary(allEvents, request.includeLiveTail, snapshot.instrumentId)}
        };
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
        const QueryArtifacts artifacts = buildQueryArtifacts(snapshot, frozenRevisionId, request.includeLiveTail);
        response.events = json::array();

        if (request.operation == "list_order_anchors") {
            const std::vector<OrderAnchorRecord>& records = artifacts.orderAnchors;
            for (auto it = records.rbegin();
                 it != records.rend() && response.events.size() < limit;
                 ++it) {
                response.events.push_back(orderAnchorToJson(*it));
            }
        } else if (request.operation == "list_protected_windows") {
            const std::vector<ProtectedWindowRecord>& records = artifacts.protectedWindows;
            for (auto it = records.rbegin();
                 it != records.rend() && response.events.size() < limit;
                 ++it) {
                response.events.push_back(protectedWindowToJson(*it));
            }
        } else if (request.operation == "list_findings") {
            const std::vector<FindingRecord>& records = artifacts.findings;
            for (auto it = records.rbegin();
                 it != records.rend() && response.events.size() < limit;
                 ++it) {
                response.events.push_back(findingToJson(*it));
            }
        } else if (request.operation == "list_incidents") {
            const std::vector<IncidentRecord>& records = artifacts.incidents;
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

        const QueryArtifacts artifacts = buildQueryArtifacts(snapshot, frozenRevisionId, request.includeLiveTail);
        std::vector<IncidentRecord> incidentRevisions;
        std::vector<FindingRecord> relatedFindings;
        std::vector<ProtectedWindowRecord> protectedWindows = artifacts.protectedWindows;
        const auto incidentRange = artifacts.incidentsByLogicalIncident.equal_range(request.logicalIncidentId);
        for (auto it = incidentRange.first; it != incidentRange.second; ++it) {
            incidentRevisions.push_back(artifacts.incidents[it->second]);
        }
        const auto findingRange = artifacts.findingsByIncident.equal_range(request.logicalIncidentId);
        for (auto it = findingRange.first; it != findingRange.second; ++it) {
            relatedFindings.push_back(artifacts.findings[it->second]);
        }

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
        json timeline = json::array();
        std::vector<json> dataQualityEvents;
        if (incidentWindow.has_value()) {
            std::vector<json> windowEvents = filterEventsByProtectedWindow(snapshot,
                                                                          artifacts,
                                                                          incidentWindow->windowId,
                                                                          32,
                                                                          frozenRevisionId,
                                                                          request.includeLiveTail,
                                                                          nullptr);
            dataQualityEvents = windowEvents;
            for (const auto& event : windowEvents) {
                timeline.push_back(timelineEntryFromEvent(event));
            }
            reportSummary["protected_window_id"] = incidentWindow->windowId;
        }
        if (dataQualityEvents.empty()) {
            dataQualityEvents = mergedEvents(snapshot,
                                             frozenRevisionId,
                                             request.includeLiveTail,
                                             latestIncident.firstSessionSeq,
                                             latestIncident.lastSessionSeq);
        }
        for (const auto& finding : relatedFindings) {
            timeline.push_back(timelineEntryFromFinding(finding));
        }
        timeline.push_back(timelineEntryFromIncident(latestIncident));
        timeline = sortAndTrimTimeline(std::move(timeline), 24);
        const json timelineSummary = buildTimelineSummary(timeline);
        reportSummary["timeline_highlights"] = buildTimelineHighlights(timeline, 3);

        response.summary = {
            {"includes_mutable_tail", request.includeLiveTail},
            {"logical_incident_id", latestIncident.logicalIncidentId},
            {"served_revision_id", frozenRevisionId},
            {"latest_incident", incidentToJson(latestIncident)},
            {"incident_revision_count", incidentRevisions.size()},
            {"score_breakdown", incidentScoreBreakdown(latestIncident)},
            {"why_it_matters", incidentWhyItMatters(latestIncident)},
            {"report_summary", reportSummary},
            {"timeline", timeline},
            {"timeline_summary", timelineSummary},
            {"data_quality", buildDataQualitySummary(dataQualityEvents, request.includeLiveTail, latestIncident.instrumentId)},
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
        const QueryArtifacts artifacts = buildQueryArtifacts(snapshot, frozenRevisionId, request.includeLiveTail);
        json selectedWindowSummary = json::object();
        std::vector<json> events;
        try {
            events = filterEventsByProtectedWindow(snapshot,
                                                  artifacts,
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
            {"data_quality", buildDataQualitySummary(events,
                                                    request.includeLiveTail,
                                                    selectedWindowSummary.value("instrument_id", std::string()))},
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
        const QueryArtifacts artifacts = buildQueryArtifacts(snapshot, frozenRevisionId, request.includeLiveTail);
        const std::vector<json> events =
            filterEventsByAnchor(snapshot,
                                 artifacts,
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

    if (request.operation == "read_order_case") {
        std::uint64_t frozenRevisionId = 0;
        try {
            frozenRevisionId = resolveFrozenRevision(snapshot, request.revisionId);
        } catch (const std::exception& error) {
            return rejectResponse(request, error.what());
        }

        const std::size_t limit = request.limit == 0 ? 200 : request.limit;
        const QueryArtifacts artifacts = buildQueryArtifacts(snapshot, frozenRevisionId, request.includeLiveTail);
        const std::vector<json> events =
            filterEventsByAnchor(snapshot,
                                 artifacts,
                                 request.traceId,
                                 request.orderId,
                                 request.permId,
                                 request.execId,
                                 limit,
                                 frozenRevisionId,
                                 request.includeLiveTail);
        json orderCaseSummary = json::object();
        try {
            orderCaseSummary = buildOrderCaseSummary(snapshot,
                                                    artifacts,
                                                    events,
                                                    request.traceId,
                                                    request.orderId,
                                                    request.permId,
                                                    request.execId,
                                                    frozenRevisionId,
                                                    request.includeLiveTail);
        } catch (const std::exception& error) {
            return rejectResponse(request, error.what());
        }

        const json anchorJson = orderCaseSummary.value("anchor", json::object());
        const BridgeAnchorIdentity anchor = anchorFromJson(anchorJson);
        std::vector<FindingRecord> relatedFindings = artifacts.findings;
        std::vector<IncidentRecord> relatedIncidents = artifacts.incidents;

        relatedFindings.erase(std::remove_if(relatedFindings.begin(),
                                             relatedFindings.end(),
                                             [&](const FindingRecord& record) {
                                                 return !anchorsShareIdentity(record.overlappingAnchor, anchor);
                                             }),
                              relatedFindings.end());
        std::sort(relatedFindings.begin(), relatedFindings.end(), [](const FindingRecord& left,
                                                                     const FindingRecord& right) {
            if (left.lastSessionSeq != right.lastSessionSeq) {
                return left.lastSessionSeq > right.lastSessionSeq;
            }
            return left.findingId > right.findingId;
        });

        relatedIncidents.erase(std::remove_if(relatedIncidents.begin(),
                                              relatedIncidents.end(),
                                              [&](const IncidentRecord& record) {
                                                  return !anchorsShareIdentity(record.overlappingAnchor, anchor);
                                              }),
                               relatedIncidents.end());
        const std::vector<IncidentRecord> collapsedIncidents = collapseLatestIncidentRevisions(relatedIncidents);

        json findingsJson = json::array();
        for (const auto& record : relatedFindings) {
            findingsJson.push_back(findingToJson(record));
        }

        json incidentsJson = json::array();
        for (const auto& record : collapsedIncidents) {
            incidentsJson.push_back(incidentToJson(record));
        }

        std::string headline = "Order case";
        if (anchor.orderId > 0) {
            headline = "Order case for order " + std::to_string(anchor.orderId);
        } else if (!anchor.execId.empty()) {
            headline = "Order case for fill " + anchor.execId;
        } else if (anchor.traceId > 0) {
            headline = "Order case for trace " + std::to_string(anchor.traceId);
        }

        std::ostringstream narrative;
        narrative << "Anchor evidence spans session_seq "
                  << orderCaseSummary.value("first_session_seq", 0ULL)
                  << " through " << orderCaseSummary.value("last_session_seq", 0ULL)
                  << " with " << relatedFindings.size() << " related findings and "
                  << collapsedIncidents.size() << " ranked incidents.";
        if (orderCaseSummary.value("last_fill_session_seq", 0ULL) > 0) {
            narrative << " Replay target is the latest fill at session_seq "
                      << orderCaseSummary.value("last_fill_session_seq", 0ULL) << ".";
        }

        json timeline = json::array();
        std::vector<json> evidenceEvents = events;
        if (orderCaseSummary.contains("protected_window")) {
            const std::uint64_t protectedWindowId = orderCaseSummary["protected_window"].value("window_id", 0ULL);
            if (protectedWindowId > 0) {
                std::vector<json> windowEvents = filterEventsByProtectedWindow(snapshot,
                                                                              artifacts,
                                                                              protectedWindowId,
                                                                              48,
                                                                              frozenRevisionId,
                                                                              request.includeLiveTail,
                                                                              nullptr);
                evidenceEvents = windowEvents;
                for (const auto& event : windowEvents) {
                    timeline.push_back(timelineEntryFromEvent(event));
                }
                orderCaseSummary["protected_window_event_count"] = windowEvents.size();
            }
        }
        if (timeline.empty()) {
            for (const auto& event : events) {
                timeline.push_back(timelineEntryFromEvent(event));
            }
        }
        for (const auto& record : relatedFindings) {
            timeline.push_back(timelineEntryFromFinding(record));
        }
        for (const auto& record : collapsedIncidents) {
            timeline.push_back(timelineEntryFromIncident(record));
        }
        timeline = sortAndTrimTimeline(std::move(timeline), 32);
        const json timelineSummary = buildTimelineSummary(timeline);

        response.events = json::array();
        for (const auto& event : events) {
            response.events.push_back(event);
        }
        orderCaseSummary["related_finding_count"] = relatedFindings.size();
        orderCaseSummary["related_incident_count"] = collapsedIncidents.size();
        orderCaseSummary["related_findings"] = findingsJson;
        orderCaseSummary["related_incidents"] = incidentsJson;
        orderCaseSummary["timeline"] = timeline;
        orderCaseSummary["timeline_summary"] = timelineSummary;
        orderCaseSummary["data_quality"] = buildDataQualitySummary(evidenceEvents,
                                                                   request.includeLiveTail,
                                                                   events.front().value("instrument_id", std::string()));
        orderCaseSummary["case_report"] = {
            {"headline", headline},
            {"summary", narrative.str()},
            {"timeline_highlights", buildTimelineHighlights(timeline, 4)},
            {"replay_target_session_seq", orderCaseSummary.value("replay_target_session_seq", 0ULL)},
            {"replay_from_session_seq", orderCaseSummary.value("replay_from_session_seq", 0ULL)},
            {"replay_to_session_seq", orderCaseSummary.value("replay_to_session_seq", 0ULL)}
        };
        response.summary = std::move(orderCaseSummary);
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
        const QueryArtifacts artifacts = buildQueryArtifacts(snapshot, frozenRevisionId, request.includeLiveTail);
        const std::vector<json> events =
            filterEventsByAnchor(snapshot,
                                 artifacts,
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

        response.events = json::array();
        for (const auto& event : events) {
            response.events.push_back(event);
        }
        response.summary = buildOrderCaseSummary(snapshot,
                                                artifacts,
                                                events,
                                                request.traceId,
                                                request.orderId,
                                                request.permId,
                                                request.execId,
                                                frozenRevisionId,
                                                request.includeLiveTail);
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
