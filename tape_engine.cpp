#include "tape_engine.h"
#include "tape_bundle_inspection.h"
#include "phase3_analyzers.h"
#include "runtime_qos.h"

#include <CommonCrypto/CommonDigest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <system_error>
#include <tuple>
#include <unordered_set>

#include <fcntl.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

namespace tape_engine {

namespace {

constexpr const char* kImportedCaseManifestSchema = "com.foxy.tape-engine.imported-case-bundle";
constexpr std::uint32_t kImportedCaseManifestVersion = 1;

json investigationResultToJson(const QueryResponse& response);

std::filesystem::path atomicWriteTempPathFor(const std::filesystem::path& path) {
    static std::atomic<std::uint64_t> counter{1};
    std::ostringstream name;
    name << path.filename().string() << ".tmp." << ::getpid() << '.'
         << counter.fetch_add(1, std::memory_order_relaxed);
    return path.parent_path() / name.str();
}

void syncDirectoryBestEffort(const std::filesystem::path& path) {
    if (path.empty()) {
        return;
    }

    const int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        return;
    }
    (void)::fsync(fd);
    ::close(fd);
}

bool writeFileAtomically(const std::filesystem::path& path,
                         const std::uint8_t* data,
                         std::size_t size,
                         std::string* error) {
    auto localErrnoMessage = [](const std::string& prefix) {
        return prefix + ": " + std::strerror(errno);
    };

    if (data == nullptr && size > 0) {
        if (error != nullptr) {
            *error = "atomic write payload pointer is null";
        }
        return false;
    }

    std::error_code ec;
    if (!path.parent_path().empty()) {
        std::filesystem::create_directories(path.parent_path(), ec);
        if (ec) {
            if (error != nullptr) {
                *error = "failed to create parent directory for atomic write: " + ec.message();
            }
            return false;
        }
    }

    const std::filesystem::path tempPath = atomicWriteTempPathFor(path);
    int fd = ::open(tempPath.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        if (error != nullptr) {
            *error = localErrnoMessage("open");
        }
        return false;
    }

    auto cleanupTemp = [&]() {
        std::error_code removeEc;
        std::filesystem::remove(tempPath, removeEc);
    };
    auto closeFd = [&]() {
        if (fd >= 0) {
            while (::close(fd) != 0 && errno == EINTR) {
            }
            fd = -1;
        }
    };

    std::size_t offset = 0;
    while (offset < size) {
        const ssize_t wrote = ::write(fd, data + offset, size - offset);
        if (wrote < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (error != nullptr) {
                *error = localErrnoMessage("write");
            }
            closeFd();
            cleanupTemp();
            return false;
        }
        offset += static_cast<std::size_t>(wrote);
    }

    if (::fsync(fd) != 0) {
        if (error != nullptr) {
            *error = localErrnoMessage("fsync");
        }
        closeFd();
        cleanupTemp();
        return false;
    }

    closeFd();

    std::filesystem::rename(tempPath, path, ec);
    if (ec) {
        if (error != nullptr) {
            *error = "failed to publish atomic write: " + ec.message();
        }
        cleanupTemp();
        return false;
    }

    syncDirectoryBestEffort(path.parent_path());
    return true;
}

bool writeBinaryFileAtomically(const std::filesystem::path& path,
                               const std::vector<std::uint8_t>& bytes,
                               std::string* error) {
    return writeFileAtomically(path, bytes.data(), bytes.size(), error);
}

bool writeTextFileAtomically(const std::filesystem::path& path,
                             const std::string& text,
                             std::string* error) {
    return writeFileAtomically(path,
                               reinterpret_cast<const std::uint8_t*>(text.data()),
                               text.size(),
                               error);
}

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

std::vector<BookLevel> bookFromJson(const json& payload) {
    std::vector<BookLevel> book;
    if (!payload.is_array()) {
        return book;
    }
    for (const auto& level : payload) {
        if (!level.is_object()) {
            continue;
        }
        book.push_back(BookLevel{
            .price = numberOrDefault(level, "price", 0.0),
            .size = numberOrDefault(level, "size", 0.0)
        });
    }
    return book;
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

void applyEventToReplayState(ReplayBookState* replay, const json& event) {
    const std::uint64_t sessionSeq = event.value("session_seq", 0ULL);
    if (sessionSeq == 0) {
        return;
    }

    replay->replayedThroughSessionSeq = sessionSeq;
    const std::string eventKind = event.value("event_kind", std::string());
    if (eventKind == "gap_marker") {
        ++replay->gapMarkers;
        return;
    }

    if (eventKind == "market_tick") {
        const int marketField = event.value("market_field", -1);
        const double price = numberOrDefault(event, "price", std::numeric_limits<double>::quiet_NaN());
        if (std::isfinite(price)) {
            if (marketField == 1) {
                replay->bidPrice = price;
            } else if (marketField == 2) {
                replay->askPrice = price;
            } else if (marketField == 4) {
                replay->lastPrice = price;
            }
        }
        ++replay->appliedEvents;
        return;
    }

    if (eventKind == "market_depth") {
        const int bookSide = event.value("book_side", -1);
        const int position = event.value("book_position", -1);
        const int operation = event.value("book_operation", -1);
        const double price = numberOrDefault(event, "price", std::numeric_limits<double>::quiet_NaN());
        const double size = numberOrDefault(event, "size", std::numeric_limits<double>::quiet_NaN());
        if (bookSide == 0) {
            applyDepthDelta(replay->askBook, position, operation, price, size);
        } else if (bookSide == 1) {
            applyDepthDelta(replay->bidBook, position, operation, price, size);
        }
        ++replay->appliedEvents;
    }
}

json replayCheckpointToJson(const ReplayBookState& replay, std::uint64_t revisionId) {
    return {
        {"revision_id", revisionId},
        {"session_seq", replay.replayedThroughSessionSeq},
        {"bid_price", replay.bidPrice},
        {"ask_price", replay.askPrice},
        {"last_price", replay.lastPrice},
        {"bid_book", bookToJson(replay.bidBook, 0)},
        {"ask_book", bookToJson(replay.askBook, 0)},
        {"applied_event_count", replay.appliedEvents},
        {"gap_markers", replay.gapMarkers}
    };
}

ReplayBookState replayCheckpointFromJson(const json& payload) {
    ReplayBookState replay;
    replay.replayedThroughSessionSeq = payload.value("session_seq", 0ULL);
    replay.bidPrice = payload.value("bid_price", 0.0);
    replay.askPrice = payload.value("ask_price", 0.0);
    replay.lastPrice = payload.value("last_price", 0.0);
    replay.bidBook = bookFromJson(payload.value("bid_book", json::array()));
    replay.askBook = bookFromJson(payload.value("ask_book", json::array()));
    replay.appliedEvents = payload.value("applied_event_count", static_cast<std::size_t>(0));
    replay.gapMarkers = payload.value("gap_markers", static_cast<std::size_t>(0));
    return replay;
}

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
constexpr std::uint64_t kLiquidityFollowThroughSeqWindow = 6;
constexpr std::uint64_t kQuoteFlickerSeqWindow = 8;
constexpr std::size_t kQuoteFlickerMinChanges = 3;

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

std::string makeWeakFallbackInstrumentId(const std::string& symbol) {
    return "ib:heuristic:STK:SMART:USD:" + symbol;
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

bool selectorKeySetContainsAnchor(const std::unordered_set<std::string>& selectorKeys,
                                  const BridgeAnchorIdentity& anchor) {
    for (const auto& key : anchorSelectorKeys(anchor)) {
        if (selectorKeys.count(key) > 0) {
            return true;
        }
    }
    return false;
}

bool isStrongInstrumentId(const std::string& instrumentId) {
    return instrumentId.rfind("ib:conid:", 0) == 0;
}

bool isHeuristicInstrumentId(const std::string& instrumentId) {
    return instrumentId.rfind("ib:heuristic:", 0) == 0;
}

std::string instrumentIdSymbolSuffix(const std::string& instrumentId);

bool symbolMatchesInstrumentId(const std::string& instrumentId, const std::string& symbol) {
    if (instrumentId.empty() || symbol.empty()) {
        return true;
    }
    const std::string suffix = instrumentIdSymbolSuffix(instrumentId);
    return suffix.empty() || suffix == symbol;
}

std::string instrumentIdSymbolSuffix(const std::string& instrumentId) {
    const std::size_t lastColon = instrumentId.find_last_of(':');
    if (lastColon == std::string::npos || lastColon + 1 >= instrumentId.size()) {
        return std::string();
    }
    return instrumentId.substr(lastColon + 1);
}

struct ResolvedInstrumentIdentity {
    std::string resolvedInstrumentId;
    std::string sourceInstrumentId;
    std::string source;
    std::string policy;
};

ResolvedInstrumentIdentity resolveInstrumentIdentity(const EngineConfig& config,
                                                     const BridgeOutboxRecord& record) {
    ResolvedInstrumentIdentity result;
    result.sourceInstrumentId = record.instrumentId;
    result.source = "configured";
    result.policy = "accepted";

    const std::string symbol = record.symbol;
    const std::string configured = config.instrumentId;
    const bool configuredMatches = !configured.empty() && symbolMatchesInstrumentId(configured, symbol);
    const bool recordMatches = !record.instrumentId.empty() && symbolMatchesInstrumentId(record.instrumentId, symbol);

    if (!record.instrumentId.empty()) {
        if (isStrongInstrumentId(record.instrumentId)) {
            if (recordMatches) {
                result.resolvedInstrumentId = record.instrumentId;
                result.source = "bridge";
                return result;
            }
            if (!configured.empty() && isStrongInstrumentId(configured) && configuredMatches) {
                result.resolvedInstrumentId = configured;
                result.source = "configured";
                result.policy = "coerced_from_mismatch";
                return result;
            }
            if (!symbol.empty()) {
                result.resolvedInstrumentId = makeWeakFallbackInstrumentId(symbol);
                result.source = "heuristic";
                result.policy = "coerced_from_mismatch";
                return result;
            }
            result.resolvedInstrumentId = record.instrumentId;
            result.source = "bridge";
            result.policy = "mismatch_unresolved";
            return result;
        }
        if (record.instrumentId.rfind("ib:", 0) == 0) {
            if (recordMatches) {
                result.resolvedInstrumentId = record.instrumentId;
                result.source = "bridge";
                return result;
            }
            if (!configured.empty() && configuredMatches) {
                result.resolvedInstrumentId = configured;
                result.source = isStrongInstrumentId(configured) ? "configured" : "configured_weak";
                result.policy = "coerced_from_mismatch";
                return result;
            }
        }
    }

    if (!configured.empty()) {
        if (isStrongInstrumentId(configured)) {
            if (configuredMatches) {
                result.resolvedInstrumentId = configured;
                result.source = "configured";
                return result;
            }
        } else if (configuredMatches || symbol.empty()) {
            result.resolvedInstrumentId = configured;
            result.source = "configured_weak";
            return result;
        }
    }

    if (!symbol.empty()) {
        result.resolvedInstrumentId = makeWeakFallbackInstrumentId(symbol);
        result.source = "heuristic";
        result.policy = "weak_fallback";
        return result;
    }

    result.resolvedInstrumentId = configured;
    result.source = configured.empty() ? "missing" : "configured";
    result.policy = configured.empty() ? "missing" : "accepted";
    return result;
}

std::string instrumentIdentityStrength(const std::string& instrumentId) {
    if (isStrongInstrumentId(instrumentId)) {
        return "strong";
    }
    if (isHeuristicInstrumentId(instrumentId)) {
        return "heuristic";
    }
    if (instrumentId.rfind("ib:", 0) == 0) {
        return "canonical";
    }
    return "unknown";
}

std::string instrumentIdentityStatus(const std::string& instrumentId,
                                     const std::string& symbol) {
    if (instrumentId.empty()) {
        return "missing";
    }
    if (!symbol.empty()) {
        const std::string suffix = instrumentIdSymbolSuffix(instrumentId);
        if (!suffix.empty() && suffix != symbol) {
            return "mismatch";
        }
    }
    if (isHeuristicInstrumentId(instrumentId)) {
        return "heuristic";
    }
    if (isStrongInstrumentId(instrumentId)) {
        return "matched";
    }
    if (instrumentId.rfind("ib:", 0) == 0) {
        return "canonical";
    }
    return "unknown";
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

double severityScoreWeight(const std::string& severity) {
    if (severity == "critical") {
        return 3.0;
    }
    if (severity == "error") {
        return 2.75;
    }
    if (severity == "warning") {
        return 2.5;
    }
    if (severity == "info") {
        return 1.0;
    }
    return 0.9;
}

double incidentKindWeight(std::string_view kind) {
    if (kind == "fill_to_adverse_move_chain") {
        return 2.3;
    }
    if (kind == "buy_fill_invalidation" || kind == "sell_fill_invalidation") {
        return 2.35;
    }
    if (kind == "fill_to_cancel_chain") {
        return 2.15;
    }
    if (kind == "post_fill_adverse_selection") {
        return 2.25;
    }
    if (kind == "passive_cut_through_proxy") {
        return 2.05;
    }
    if (kind == "buy_sweep_sequence" || kind == "sell_sweep_sequence") {
        return 1.95;
    }
    if (kind == "ask_pull_follow_through" || kind == "bid_pull_follow_through") {
        return 2.1;
    }
    if (kind == "ask_depletion_after_trade" || kind == "bid_depletion_after_trade") {
        return 2.0;
    }
    if (kind == "order_window_market_impact") {
        return 1.95;
    }
    if (kind == "source_gap" || kind == "source_reset") {
        return 1.9;
    }
    if (kind == "buy_trade_pressure" || kind == "sell_trade_pressure") {
        return 1.75;
    }
    if (kind == "ask_quote_flicker" || kind == "bid_quote_flicker" ||
        kind == "ask_display_instability" || kind == "bid_display_instability") {
        return 1.6;
    }
    if (kind == "ask_liquidity_thinned" || kind == "bid_liquidity_thinned") {
        return 1.35;
    }
    if (kind == "spread_widened") {
        return 1.25;
    }
    if (kind == "ask_liquidity_refilled" || kind == "bid_liquidity_refilled") {
        return 1.15;
    }
    if (kind == "ask_genuine_refill" || kind == "bid_genuine_refill") {
        return 1.18;
    }
    if (kind == "buy_fade_sequence" || kind == "sell_fade_sequence") {
        return 1.45;
    }
    if (kind == "passive_queue_loss_proxy") {
        return 1.4;
    }
    if (kind == "passive_fill_queue_proxy") {
        return 1.25;
    }
    if (kind == "order_fill_context" || kind == "order_flow_timeline") {
        return 0.95;
    }
    return 1.0;
}

double spanScoreWeight(std::uint64_t firstSessionSeq, std::uint64_t lastSessionSeq) {
    const std::uint64_t span = lastSessionSeq > firstSessionSeq ? (lastSessionSeq - firstSessionSeq) : 0;
    return 1.0 + std::min(0.35, static_cast<double>(span) * 0.035);
}

double incidentQualityPenaltyFactor(const json& dataQuality) {
    const double qualityScore = std::clamp(dataQuality.value("score", 100.0), 0.0, 100.0);
    const double scoreFactor = 0.55 + (qualityScore / 100.0) * 0.45;
    const std::size_t issueCount = dataQuality.value("issues", json::array()).size();
    const double issueFactor = std::max(0.72, 1.0 - static_cast<double>(std::min<std::size_t>(3, issueCount)) * 0.08);
    return std::clamp(scoreFactor * issueFactor, 0.5, 1.0);
}

double incidentScoreContribution(const FindingRecord& finding) {
    const double severityWeight = severityScoreWeight(finding.severity);
    const double overlapWeight = finding.overlapsOrder ? 1.75 : 1.0;
    const double kindWeight = incidentKindWeight(finding.kind);
    const double rangeWeight = spanScoreWeight(finding.firstSessionSeq, finding.lastSessionSeq);
    return severityWeight * std::max(0.25, finding.confidence) * overlapWeight * kindWeight * rangeWeight;
}

std::size_t distinctIncidentFindingKinds(const std::vector<FindingRecord>& findings);
double incidentEvidenceBreadthFactor(const std::vector<FindingRecord>& findings);
double incidentCorroborationFactor(const std::vector<FindingRecord>& findings);

std::string sessionReportArtifactId(std::uint64_t reportId) {
    return "session-report:" + std::to_string(reportId);
}

std::string caseReportArtifactId(std::uint64_t reportId) {
    return "case-report:" + std::to_string(reportId);
}

std::string sessionBundleArtifactId(std::uint64_t reportId) {
    return "session-bundle:report:" + std::to_string(reportId);
}

std::string caseBundleArtifactId(std::uint64_t reportId) {
    return "case-bundle:report:" + std::to_string(reportId);
}

std::string importedCaseArtifactId(std::uint64_t importedCaseId) {
    return "imported-case:" + std::to_string(importedCaseId);
}

std::string incidentArtifactId(std::uint64_t logicalIncidentId) {
    return "incident:" + std::to_string(logicalIncidentId);
}

std::string protectedWindowArtifactId(std::uint64_t windowId) {
    return "window:" + std::to_string(windowId);
}

std::string findingArtifactId(std::uint64_t findingId) {
    return "finding:" + std::to_string(findingId);
}

std::string anchorArtifactId(std::uint64_t anchorId) {
    return "anchor:" + std::to_string(anchorId);
}

std::string anchorSelectorArtifactId(const BridgeAnchorIdentity& anchor) {
    if (!anchor.execId.empty()) {
        return "order-case:exec:" + anchor.execId;
    }
    if (anchor.orderId > 0) {
        return "order-case:order:" + std::to_string(anchor.orderId);
    }
    if (anchor.permId > 0) {
        return "order-case:perm:" + std::to_string(anchor.permId);
    }
    if (anchor.traceId > 0) {
        return "order-case:trace:" + std::to_string(anchor.traceId);
    }
    return "order-case:unknown";
}

json evidenceCitation(std::string type,
                      std::string artifactId,
                      std::uint64_t firstSessionSeq,
                      std::uint64_t lastSessionSeq,
                      std::string headline) {
    return {
        {"artifact_id", artifactId},
        {"first_session_seq", firstSessionSeq},
        {"headline", headline},
        {"last_session_seq", lastSessionSeq},
        {"type", type}
    };
}

std::string firstTimelineHeadline(const json& timeline) {
    if (!timeline.is_array()) {
        return std::string();
    }
    for (auto it = timeline.rbegin(); it != timeline.rend(); ++it) {
        if (it->is_object()) {
            const std::string headline = it->value("headline", std::string());
            if (!headline.empty()) {
                return headline;
            }
        }
    }
    return std::string();
}

std::string reportUncertaintySummary(const json& dataQuality,
                                     std::size_t evidenceCount,
                                     std::size_t corroboratingKinds) {
    const double qualityScore = dataQuality.value("score", 100.0);
    const std::size_t issueCount = dataQuality.value("issues", json::array()).size();
    if (qualityScore < 60.0 || issueCount >= 3) {
        return "Evidence quality is materially degraded, so confidence should be treated cautiously.";
    }
    if (corroboratingKinds <= 1 && evidenceCount <= 1) {
        return "This conclusion relies on a narrow evidence set with limited corroboration.";
    }
    if (qualityScore < 80.0 || issueCount > 0) {
        return "The evidence is usable but carries some data-quality caveats.";
    }
    return "Confidence is supported by both data quality and corroborating evidence.";
}

json buildEvidenceSection(const json& timeline,
                          const json& timelineSummary,
                          const json& citations,
                          const json& dataQuality) {
    return {
        {"citations", citations},
        {"data_quality", dataQuality},
        {"timeline", timeline},
        {"timeline_summary", timelineSummary}
    };
}

json buildReportEnvelope(const json& artifact,
                         const json& entity,
                         const json& report,
                         const json& evidence) {
    return {
        {"artifact", artifact},
        {"entity", entity},
        {"evidence", evidence},
        {"report", report}
    };
}

struct SessionOverviewArtifactRef {
    std::uint64_t revisionId = 0;
    std::uint64_t fromSessionSeq = 0;
    std::uint64_t toSessionSeq = 0;
};

std::optional<SessionOverviewArtifactRef> parseSessionOverviewArtifactId(const std::string& artifactId) {
    if (artifactId.rfind("session-overview:", 0) != 0) {
        return std::nullopt;
    }
    const std::string suffix = artifactId.substr(std::strlen("session-overview:"));
    std::vector<std::string> parts;
    std::size_t start = 0;
    while (start <= suffix.size()) {
        const std::size_t colon = suffix.find(':', start);
        if (colon == std::string::npos) {
            parts.push_back(suffix.substr(start));
            break;
        }
        parts.push_back(suffix.substr(start, colon - start));
        start = colon + 1;
    }
    if (parts.size() != 3) {
        return std::nullopt;
    }
    SessionOverviewArtifactRef result;
    char* end = nullptr;
    result.revisionId = std::strtoull(parts[0].c_str(), &end, 10);
    if (end == nullptr || *end != '\0') {
        return std::nullopt;
    }
    result.fromSessionSeq = std::strtoull(parts[1].c_str(), &end, 10);
    if (end == nullptr || *end != '\0') {
        return std::nullopt;
    }
    result.toSessionSeq = std::strtoull(parts[2].c_str(), &end, 10);
    if (end == nullptr || *end != '\0') {
        return std::nullopt;
    }
    return result;
}

std::optional<BridgeAnchorIdentity> parseOrderCaseArtifactId(const std::string& artifactId) {
    if (artifactId.rfind("order-case:", 0) != 0) {
        return std::nullopt;
    }
    const std::string suffix = artifactId.substr(std::strlen("order-case:"));
    const std::size_t colon = suffix.find(':');
    if (colon == std::string::npos) {
        return std::nullopt;
    }
    const std::string selector = suffix.substr(0, colon);
    const std::string value = suffix.substr(colon + 1);
    if (value.empty()) {
        return std::nullopt;
    }
    BridgeAnchorIdentity anchor;
    char* end = nullptr;
    if (selector == "trace") {
        anchor.traceId = std::strtoull(value.c_str(), &end, 10);
        if (end == nullptr || *end != '\0') {
            return std::nullopt;
        }
    } else if (selector == "order") {
        anchor.orderId = static_cast<OrderId>(std::strtoll(value.c_str(), &end, 10));
        if (end == nullptr || *end != '\0') {
            return std::nullopt;
        }
    } else if (selector == "perm") {
        anchor.permId = std::strtoll(value.c_str(), &end, 10);
        if (end == nullptr || *end != '\0') {
            return std::nullopt;
        }
    } else if (selector == "exec") {
        anchor.execId = value;
    } else {
        return std::nullopt;
    }
    return anchor;
}

std::optional<std::pair<std::string, std::uint64_t>> parseNumericArtifactId(const std::string& artifactId) {
    const std::size_t colon = artifactId.find(':');
    if (colon == std::string::npos || colon + 1 >= artifactId.size()) {
        return std::nullopt;
    }
    const std::string prefix = artifactId.substr(0, colon);
    const std::string value = artifactId.substr(colon + 1);
    if (value.empty()) {
        return std::nullopt;
    }
    char* end = nullptr;
    const unsigned long long parsed = std::strtoull(value.c_str(), &end, 10);
    if (end == nullptr || *end != '\0') {
        return std::nullopt;
    }
    return std::make_pair(prefix, static_cast<std::uint64_t>(parsed));
}

std::string renderReportMarkdown(const QueryResponse& response) {
    const json artifact = response.summary.value("artifact", json::object());
    const json report = response.summary.value("report", response.summary.value("report_summary", json::object()));
    const json evidence = response.summary.value("evidence", json::object());
    std::ostringstream out;
    out << "# " << report.value("headline", std::string("Tape Engine Report")) << "\n\n";
    if (!artifact.empty()) {
        out << "- Artifact: `" << artifact.value("artifact_id", std::string()) << "`\n";
        out << "- Revision: `" << artifact.value("revision_id", response.summary.value("served_revision_id", 0ULL)) << "`\n";
        if (artifact.contains("first_session_seq")) {
            out << "- Session range: `" << artifact.value("first_session_seq", 0ULL)
                << " -> " << artifact.value("last_session_seq", 0ULL) << "`\n";
        } else if (artifact.contains("from_session_seq")) {
            out << "- Session range: `" << artifact.value("from_session_seq", 0ULL)
                << " -> " << artifact.value("to_session_seq", 0ULL) << "`\n";
        }
        out << "\n";
    }
    const std::string summaryText = report.value("summary", std::string());
    if (!summaryText.empty()) {
        out << summaryText << "\n\n";
    }
    const std::string why = report.value("why_it_matters", response.summary.value("why_it_matters", std::string()));
    if (!why.empty()) {
        out << "## Why It Matters\n" << why << "\n\n";
    }
    const std::string firstChange = report.value("what_changed_first", std::string());
    if (!firstChange.empty()) {
        out << "## What Changed First\n" << firstChange << "\n\n";
    }
    const std::string uncertainty = report.value("uncertainty", response.summary.value("uncertainty_summary", std::string()));
    if (!uncertainty.empty()) {
        out << "## Uncertainty\n" << uncertainty << "\n\n";
    }
    const json citations = evidence.value("citations", json::array());
    if (citations.is_array() && !citations.empty()) {
        out << "## Evidence\n";
        for (const auto& citation : citations) {
            out << "- `" << citation.value("artifact_id", std::string()) << "` "
                << citation.value("headline", std::string())
                << " (`" << citation.value("first_session_seq", 0ULL) << " -> "
                << citation.value("last_session_seq", 0ULL) << "`)\n";
        }
        out << "\n";
    }
    const json highlights = report.value("timeline_highlights", json::array());
    if (highlights.is_array() && !highlights.empty()) {
        out << "## Highlights\n";
        for (const auto& item : highlights) {
            if (item.is_string()) {
                out << "- " << item.get<std::string>() << '\n';
            } else if (item.is_object()) {
                out << "- " << item.value("headline", std::string());
                if (item.value("session_seq", 0ULL) > 0) {
                    out << " (`" << item.value("session_seq", 0ULL) << "`)";
                }
                out << '\n';
            }
        }
        out << '\n';
    }
    return out.str();
}

json buildInvestigationApiSummary(const QueryResponse& response,
                                  std::uint64_t servedRevisionId,
                                  bool includesMutableTail,
                                  std::string responseKind) {
    return {
        {"operation", response.operation},
        {"response_kind", std::move(responseKind)},
        {"served_revision_id", servedRevisionId},
        {"wire_schema", response.schema},
        {"wire_version", response.version},
        {"envelope_schema", kInvestigationEnvelopeSchema},
        {"envelope_version", kInvestigationEnvelopeVersion},
        {"includes_mutable_tail", includesMutableTail}
    };
}

void annotateInvestigationEnvelope(QueryResponse* response,
                                   std::uint64_t servedRevisionId,
                                   bool includesMutableTail,
                                   const std::string& responseKind,
                                   const std::string& reportType) {
    if (response == nullptr) {
        return;
    }
    response->summary["api"] = buildInvestigationApiSummary(*response,
                                                            servedRevisionId,
                                                            includesMutableTail,
                                                            responseKind);
    response->summary["served_revision_id"] = servedRevisionId;
    response->summary["includes_mutable_tail"] = includesMutableTail;
    if (response->summary.contains("artifact") && response->summary["artifact"].is_object()) {
        json artifact = response->summary["artifact"];
        artifact["schema_version"] = kInvestigationEnvelopeVersion;
        if (!artifact.contains("artifact_scope")) {
            artifact["artifact_scope"] = response->summary.value("is_durable_report", false) ? "durable" : "ephemeral";
        }
        if (!artifact.contains("resolved_revision_id")) {
            artifact["resolved_revision_id"] = servedRevisionId;
        }
        response->summary["artifact"] = std::move(artifact);
    }
    if (response->summary.contains("entity") && response->summary["entity"].is_object()) {
        json entity = response->summary["entity"];
        if (!entity.contains("entity_type")) {
            entity["entity_type"] = entity.value("type", std::string());
        }
        entity["schema_version"] = kInvestigationEnvelopeVersion;
        response->summary["entity"] = std::move(entity);
    }
    if (response->summary.contains("report") && response->summary["report"].is_object()) {
        json report = response->summary["report"];
        report["schema_version"] = kInvestigationEnvelopeVersion;
        if (!reportType.empty() && !report.contains("report_type")) {
            report["report_type"] = reportType;
        }
        if (response->summary.contains("artifact") &&
            response->summary["artifact"].is_object() &&
            !report.contains("artifact_id")) {
            report["artifact_id"] = response->summary["artifact"].value("artifact_id", std::string());
        }
        response->summary["report"] = std::move(report);
        response->summary["report_summary"] = response->summary["report"];
    }
    if (response->summary.contains("evidence") && response->summary["evidence"].is_object()) {
        json evidence = response->summary["evidence"];
        evidence["schema_version"] = kInvestigationEnvelopeVersion;
        const json citations = evidence.value("citations", json::array());
        evidence["citation_count"] = citations.is_array() ? citations.size() : 0;
        const json timeline = evidence.value("timeline", response->summary.value("timeline", json::array()));
        evidence["timeline_entry_count"] = timeline.is_array() ? timeline.size() : 0;
        response->summary["evidence"] = std::move(evidence);
    }
    response->result = investigationResultToJson(*response);
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
    if (incident.kind == "ask_absorption_persistence" || incident.kind == "bid_absorption_persistence") {
        return "Refilled touch liquidity stayed in place long enough to matter, which is a stronger absorption cue than a one-off refill print.";
    }
    if (incident.kind == "ask_genuine_refill" || incident.kind == "bid_genuine_refill") {
        return "Displayed touch liquidity rebuilt and held without touch-trade confirmation, which looks more like a genuine refill than absorption.";
    }
    if (incident.kind == "buy_trade_pressure" || incident.kind == "sell_trade_pressure") {
        return incident.kind == "buy_trade_pressure"
            ? "Repeated prints lifted the ask, which can indicate buyers pressing the offer and raising short-term execution risk for passive buys."
            : "Repeated prints hit the bid, which can indicate sellers pressing the bid and weakening short-term support for passive sells.";
    }
    if (incident.kind == "ask_depletion_after_trade" || incident.kind == "bid_depletion_after_trade") {
        return incident.kind == "ask_depletion_after_trade"
            ? "Displayed ask size drained immediately after a touch trade, which can indicate queue exhaustion or follow-on pressure after the trade."
            : "Displayed bid size drained immediately after a touch trade, which can indicate queue exhaustion or follow-on pressure after the trade.";
    }
    if (incident.kind == "ask_trade_after_depletion" || incident.kind == "bid_trade_after_depletion") {
        return incident.kind == "ask_trade_after_depletion"
            ? "A trade printed through the depleted ask immediately after liquidity thinned, which is a stronger continuation clue than depletion alone."
            : "A trade printed through the depleted bid immediately after liquidity thinned, which is a stronger continuation clue than depletion alone.";
    }
    if (incident.kind == "ask_pull_follow_through" || incident.kind == "bid_pull_follow_through") {
        return incident.kind == "ask_pull_follow_through"
            ? "Ask liquidity pulled and the market followed upward immediately after, which is stronger than a simple displayed-size change."
            : "Bid liquidity pulled and the market followed downward immediately after, which is stronger than a simple displayed-size change.";
    }
    if (incident.kind == "ask_quote_flicker" || incident.kind == "bid_quote_flicker") {
        return "The touch repriced repeatedly over a short sequence window, which makes the visible inside less stable for queue and timing decisions.";
    }
    if (incident.kind == "ask_display_instability" || incident.kind == "bid_display_instability") {
        return "Displayed touch liquidity repeatedly disappeared and reappeared at the same price, which can make the visible book less trustworthy for queue and fill decisions.";
    }
    if (incident.kind == "buy_fill_invalidation" || incident.kind == "sell_fill_invalidation") {
        return incident.kind == "buy_fill_invalidation"
            ? "After the buy fill, the best bid moved materially lower, which is a strong sign that the immediate execution thesis failed."
            : "After the sell fill, the best ask moved materially higher, which is a strong sign that the immediate execution thesis failed.";
    }
    if (incident.kind == "fill_to_cancel_chain") {
        return "A fill was followed by cancellation activity in the same protected window, which usually points to a deteriorating execution setup or incomplete fill thesis.";
    }
    if (incident.kind == "fill_to_adverse_move_chain") {
        return "A fill was followed by a measurable adverse move in the same protected window, which is stronger than a generic post-fill warning because it preserves the sequence.";
    }
    if (incident.kind == "post_fill_adverse_selection") {
        return "The market moved against the fill after execution, which is a direct adverse-selection signal rather than a generic order-window summary.";
    }
    if (incident.kind == "passive_queue_loss_proxy") {
        return "The order window saw touch trading but no fill before cancellation, which is a useful proxy for passive queue loss or stale queue position.";
    }
    if (incident.kind == "passive_cut_through_proxy") {
        return "The market traded at or through the passive price without filling the order window, which is a strong proxy for being cut through.";
    }
    if (incident.kind == "passive_fill_queue_proxy") {
        return "The fill window contains enough touch-size behavior to form a queue-position proxy, which helps explain whether the passive fill looked supported or fragile.";
    }
    if (incident.kind == "buy_sweep_sequence" || incident.kind == "sell_sweep_sequence") {
        return "The protected window moved with repeated touch-trade confirmation, which looks more like a true sweep than a simple fade in displayed liquidity.";
    }
    if (incident.kind == "buy_fade_sequence" || incident.kind == "sell_fade_sequence") {
        return "The protected window moved with limited trade confirmation, which looks more like fading displayed liquidity than a decisive sweep.";
    }
    if (incident.kind == "order_window_market_impact") {
        return "The order window ended with a measurable mid/spread change, which is a useful proxy for whether the market moved with or against the execution.";
    }
    if (incident.kind == "source_gap" || incident.kind == "source_reset") {
        return "Source continuity changed, so any interpretation around this window needs to account for data-quality risk.";
    }
    return "This incident groups correlated evidence into one revision-pinned investigation unit.";
}

json incidentScoreBreakdown(const IncidentRecord& incident,
                           const std::vector<FindingRecord>& relatedFindings,
                           const json* dataQuality = nullptr) {
    const double findingCount = std::max<double>(1.0, static_cast<double>(incident.findingCount));
    const double severityWeight = severityScoreWeight(incident.severity);
    const double overlapWeight = incident.overlapsOrder ? 1.75 : 1.0;
    const double kindWeight = incidentKindWeight(incident.kind);
    const double rangeWeight = spanScoreWeight(incident.firstSessionSeq, incident.lastSessionSeq);
    const double evidenceBreadthFactor = incidentEvidenceBreadthFactor(relatedFindings);
    const double corroborationFactor = incidentCorroborationFactor(relatedFindings);
    json payload{
        {"score", incident.score},
        {"finding_count", incident.findingCount},
        {"average_score_per_finding", incident.score / findingCount},
        {"confidence", incident.confidence},
        {"severity", incident.severity},
        {"overlaps_order", incident.overlapsOrder},
        {"severity_weight", severityWeight},
        {"overlap_weight", overlapWeight},
        {"kind_weight", kindWeight},
        {"range_weight", rangeWeight},
        {"evidence_breadth_factor", evidenceBreadthFactor},
        {"corroboration_factor", corroborationFactor},
        {"corroborating_kind_count", distinctIncidentFindingKinds(relatedFindings)},
        {"score_model", "severity_weight * confidence * overlap_weight * kind_weight * range_weight * evidence_breadth_factor * corroboration_factor, accumulated per finding and then quality-adjusted"}
    };
    if (dataQuality != nullptr) {
        payload["data_quality_penalty_factor"] = incidentQualityPenaltyFactor(*dataQuality);
        payload["data_quality_score"] = dataQuality->value("score", 100.0);
        payload["uncertainty_summary"] = reportUncertaintySummary(*dataQuality,
                                                                  relatedFindings.size(),
                                                                  distinctIncidentFindingKinds(relatedFindings));
    }
    return payload;
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
    std::size_t heuristicInstrumentIdentityCount = 0;
    std::size_t canonicalInstrumentIdentityCount = 0;
    std::size_t strongInstrumentIdentityCount = 0;
    std::size_t mismatchedInstrumentIdentityCount = 0;
    std::size_t sourceStrongInstrumentIdentityCount = 0;
    std::size_t identityPolicyOverrideCount = 0;
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
        const std::string identityStrength = event.value("instrument_identity_strength",
                                                         instrumentIdentityStrength(instrumentId));
        const std::string identityStatus = event.value("instrument_identity_status",
                                                       instrumentIdentityStatus(instrumentId,
                                                                                event.value("symbol", std::string())));
        const std::string sourceIdentityStrength = event.value("source_instrument_identity_strength",
                                                               instrumentIdentityStrength(event.value("source_instrument_id", std::string())));
        const std::string identityPolicy = event.value("instrument_identity_policy", std::string("accepted"));
        if (instrumentId.empty() ||
            (!expectedInstrumentId.empty() && instrumentId != expectedInstrumentId) ||
            !isStrongInstrumentId(instrumentId)) {
            ++weakInstrumentIdentityCount;
        }
        if (identityStatus == "mismatch") {
            ++mismatchedInstrumentIdentityCount;
        }
        if (identityStrength == "heuristic") {
            ++heuristicInstrumentIdentityCount;
        } else if (identityStrength == "canonical") {
            ++canonicalInstrumentIdentityCount;
        } else if (identityStrength == "strong") {
            ++strongInstrumentIdentityCount;
        }
        if (sourceIdentityStrength == "strong") {
            ++sourceStrongInstrumentIdentityCount;
        }
        if (identityPolicy == "coerced_from_mismatch") {
            ++identityPolicyOverrideCount;
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
    if (mismatchedInstrumentIdentityCount > 0) {
        score -= 8.0;
    }
    if (identityPolicyOverrideCount > 0) {
        score -= 6.0;
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
    if (mismatchedInstrumentIdentityCount > 0) {
        issues.push_back("At least one event carried an instrument identity whose symbol suffix did not match the bridged symbol.");
    }
    if (identityPolicyOverrideCount > 0) {
        issues.push_back("The engine had to override at least one bridged instrument identity to keep canonical evidence consistent.");
    }
    if (heuristicInstrumentIdentityCount > 0) {
        issues.push_back("At least one event relied on heuristic instrument identity fallback instead of a strong canonical ID.");
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
        {"mismatched_instrument_identity_count", mismatchedInstrumentIdentityCount},
        {"heuristic_instrument_identity_count", heuristicInstrumentIdentityCount},
        {"canonical_instrument_identity_count", canonicalInstrumentIdentityCount},
        {"strong_instrument_identity_count", strongInstrumentIdentityCount},
        {"source_strong_instrument_identity_count", sourceStrongInstrumentIdentityCount},
        {"identity_policy_override_count", identityPolicyOverrideCount},
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

template <typename QueryArtifactsLike>
std::vector<FindingRecord> relatedIncidentFindings(const QueryArtifactsLike& artifacts,
                                                   std::uint64_t logicalIncidentId) {
    std::vector<FindingRecord> related;
    const auto range = artifacts.findingsByIncident.equal_range(logicalIncidentId);
    for (auto it = range.first; it != range.second; ++it) {
        related.push_back(artifacts.findings[it->second]);
    }
    std::sort(related.begin(), related.end(), [](const FindingRecord& left, const FindingRecord& right) {
        if (left.lastSessionSeq != right.lastSessionSeq) {
            return left.lastSessionSeq > right.lastSessionSeq;
        }
        return left.findingId > right.findingId;
    });
    return related;
}

std::size_t distinctIncidentFindingKinds(const std::vector<FindingRecord>& findings) {
    std::unordered_set<std::string> kinds;
    for (const auto& finding : findings) {
        kinds.insert(finding.kind);
    }
    return kinds.size();
}

double incidentEvidenceBreadthFactor(const std::vector<FindingRecord>& findings) {
    if (findings.empty()) {
        return 1.0;
    }
    return 1.0 + std::min(0.25, static_cast<double>(findings.size() - 1) * 0.05);
}

double incidentCorroborationFactor(const std::vector<FindingRecord>& findings) {
    const std::size_t distinctKinds = distinctIncidentFindingKinds(findings);
    if (distinctKinds <= 1) {
        return 1.0;
    }
    return 1.0 + std::min(0.25, static_cast<double>(distinctKinds - 1) * 0.08);
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

bool protectedWindowIsNewer(const ProtectedWindowRecord& candidate,
                            const ProtectedWindowRecord& current) {
    if (candidate.revisionId != current.revisionId) {
        return candidate.revisionId > current.revisionId;
    }
    if (candidate.lastSessionSeq != current.lastSessionSeq) {
        return candidate.lastSessionSeq > current.lastSessionSeq;
    }
    if (candidate.firstSessionSeq != current.firstSessionSeq) {
        return candidate.firstSessionSeq < current.firstSessionSeq;
    }
    if (candidate.endEngineNs != current.endEngineNs) {
        return candidate.endEngineNs > current.endEngineNs;
    }
    if (candidate.startEngineNs != current.startEngineNs) {
        return candidate.startEngineNs < current.startEngineNs;
    }
    return candidate.windowId > current.windowId;
}

std::vector<ProtectedWindowRecord> collapseLatestProtectedWindowRevisions(const std::vector<ProtectedWindowRecord>& records) {
    std::map<std::uint64_t, ProtectedWindowRecord> latestByWindowId;
    for (const auto& record : records) {
        auto found = latestByWindowId.find(record.windowId);
        if (found == latestByWindowId.end() ||
            protectedWindowIsNewer(record, found->second)) {
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
        {"artifact_id", anchorArtifactId(record.anchorId)},
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
        {"artifact_id", protectedWindowArtifactId(record.windowId)},
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
        {"artifact_id", findingArtifactId(record.findingId)},
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
        {"artifact_id", incidentArtifactId(record.logicalIncidentId)},
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

json sessionReportToJson(const SessionReportRecord& record) {
    return {
        {"artifact_id", sessionReportArtifactId(record.reportId)},
        {"report_id", record.reportId},
        {"revision_id", record.revisionId},
        {"from_session_seq", record.fromSessionSeq},
        {"to_session_seq", record.toSessionSeq},
        {"created_ts_engine_ns", record.createdTsEngineNs},
        {"incident_count", record.incidentCount},
        {"instrument_id", record.instrumentId},
        {"headline", record.headline},
        {"file_name", record.fileName},
        {"payload_sha256", record.payloadSha256}
    };
}

json caseReportToJson(const CaseReportRecord& record) {
    return {
        {"artifact_id", caseReportArtifactId(record.reportId)},
        {"report_id", record.reportId},
        {"revision_id", record.revisionId},
        {"report_type", record.reportType},
        {"logical_incident_id", record.logicalIncidentId},
        {"anchor", anchorToJson(record.anchor)},
        {"first_session_seq", record.firstSessionSeq},
        {"last_session_seq", record.lastSessionSeq},
        {"created_ts_engine_ns", record.createdTsEngineNs},
        {"instrument_id", record.instrumentId},
        {"headline", record.headline},
        {"file_name", record.fileName},
        {"payload_sha256", record.payloadSha256}
    };
}

json importedCaseToJson(const ImportedCaseRecord& record) {
    return {
        {"artifact_id", importedCaseArtifactId(record.importedCaseId)},
        {"imported_case_id", record.importedCaseId},
        {"imported_ts_engine_ns", record.importedTsEngineNs},
        {"bundle_id", record.bundleId},
        {"bundle_type", record.bundleType},
        {"source_artifact_id", record.sourceArtifactId},
        {"source_report_id", record.sourceReportId},
        {"source_revision_id", record.sourceRevisionId},
        {"first_session_seq", record.firstSessionSeq},
        {"last_session_seq", record.lastSessionSeq},
        {"instrument_id", record.instrumentId},
        {"headline", record.headline},
        {"file_name", record.fileName},
        {"source_bundle_path", record.sourceBundlePath},
        {"payload_sha256", record.payloadSha256}
    };
}

json bundleExportResultToJson(const json& artifact,
                              const json& bundle,
                              const json& sourceArtifact,
                              const json& sourceReport,
                              std::uint64_t servedRevisionId,
                              const std::string& exportStatus) {
    return {
        {"schema", kBundleExportResultSchema},
        {"version", kBundleExportResultVersion},
        {"artifact", artifact},
        {"bundle", bundle},
        {"source_artifact", sourceArtifact},
        {"source_report", sourceReport},
        {"served_revision_id", servedRevisionId},
        {"export_status", exportStatus}
    };
}

json bundleVerifyResultToJson(const json& artifact,
                              const json& bundle,
                              const json& sourceArtifact,
                              const json& sourceReport,
                              const json& reportSummary,
                              const std::string& reportMarkdown,
                              const std::string& verifyStatus,
                              bool importSupported,
                              bool alreadyImported,
                              bool canImport,
                              const std::string& importReason,
                              std::uint64_t servedRevisionId,
                              const std::optional<ImportedCaseRecord>& importedCase) {
    json result{
        {"schema", kBundleVerifyResultSchema},
        {"version", kBundleVerifyResultVersion},
        {"artifact", artifact},
        {"bundle", bundle},
        {"source_artifact", sourceArtifact},
        {"source_report", sourceReport},
        {"report_summary", reportSummary},
        {"report_markdown", reportMarkdown},
        {"verify_status", verifyStatus},
        {"import_supported", importSupported},
        {"already_imported", alreadyImported},
        {"can_import", canImport},
        {"import_reason", importReason},
        {"served_revision_id", servedRevisionId}
    };
    if (importedCase.has_value()) {
        result["imported_case"] = importedCaseToJson(*importedCase);
    }
    return result;
}

json caseBundleImportResultToJson(const json& artifact,
                                  const ImportedCaseRecord& importedCase,
                                  const std::string& importStatus,
                                  bool duplicateImport) {
    return {
        {"schema", kCaseBundleImportResultSchema},
        {"version", kCaseBundleImportResultVersion},
        {"artifact", artifact},
        {"imported_case", importedCaseToJson(importedCase)},
        {"import_status", importStatus},
        {"duplicate_import", duplicateImport}
    };
}

json importedCaseInventoryResultToJson(const json& importedCases,
                                       std::size_t returnedCount) {
    return {
        {"schema", kImportedCaseInventoryResultSchema},
        {"version", kImportedCaseInventoryResultVersion},
        {"returned_count", returnedCount},
        {"imported_cases", importedCases}
    };
}

std::string firstStringValue(const json& payload,
                             std::initializer_list<const char*> keys) {
    if (!payload.is_object()) {
        return {};
    }
    for (const char* key : keys) {
        const auto it = payload.find(key);
        if (it != payload.end() && it->is_string()) {
            return it->get<std::string>();
        }
    }
    return {};
}

json maybeReplayRangeToJson(const json& summary) {
    if (summary.contains("replay_from_session_seq") || summary.contains("replay_to_session_seq")) {
        return {
            {"first_session_seq", summary.value("replay_from_session_seq", 0ULL)},
            {"last_session_seq", summary.value("replay_to_session_seq", 0ULL)}
        };
    }
    return nullptr;
}

json statusResultToJson(const json& summary) {
    json result = summary.is_object() ? summary : json::object();
    result["schema"] = kStatusResultSchema;
    result["version"] = kStatusResultVersion;
    return result;
}

json eventListResultToJson(const json& summary,
                           const json& events) {
    return {
        {"schema", kEventListResultSchema},
        {"version", kEventListResultVersion},
        {"served_revision_id", summary.value("served_revision_id", 0ULL)},
        {"includes_mutable_tail", summary.value("includes_mutable_tail", false)},
        {"returned_count", events.is_array() ? events.size() : 0},
        {"base_revision_id", summary.value("base_revision_id", 0ULL)},
        {"live_tail_high_water_seq", summary.value("live_tail_high_water_seq", 0ULL)},
        {"from_session_seq", summary.value("from_session_seq", 0ULL)},
        {"to_session_seq", summary.value("to_session_seq", 0ULL)},
        {"trace_id", summary.value("trace_id", 0ULL)},
        {"order_id", summary.value("order_id", 0LL)},
        {"perm_id", summary.value("perm_id", 0LL)},
        {"exec_id", summary.value("exec_id", std::string())},
        {"events", events.is_array() ? events : json::array()}
    };
}

json sessionQualityResultToJson(const json& summary) {
    return {
        {"schema", kSessionQualityResultSchema},
        {"version", kSessionQualityResultVersion},
        {"served_revision_id", summary.value("served_revision_id", 0ULL)},
        {"includes_mutable_tail", summary.value("includes_mutable_tail", false)},
        {"first_session_seq", summary.value("from_session_seq", 0ULL)},
        {"last_session_seq", summary.value("to_session_seq", 0ULL)},
        {"data_quality", summary.value("data_quality", json::object())}
    };
}

json investigationIncidentRows(const QueryResponse& response) {
    const json& summary = response.summary;
    if (summary.contains("related_incidents") && summary["related_incidents"].is_array()) {
        return summary["related_incidents"];
    }
    if (summary.contains("top_incidents") && summary["top_incidents"].is_array()) {
        return summary["top_incidents"];
    }
    if (summary.contains("incident_revisions") && summary["incident_revisions"].is_array()) {
        return summary["incident_revisions"];
    }
    if (summary.contains("latest_incident") && summary["latest_incident"].is_object() && !summary["latest_incident"].empty()) {
        return json::array({summary["latest_incident"]});
    }
    if (summary.contains("incident") && summary["incident"].is_object() && !summary["incident"].empty()) {
        return json::array({summary["incident"]});
    }
    if (response.events.is_array()) {
        bool allIncidents = !response.events.empty();
        for (const auto& item : response.events) {
            if (!item.is_object() || !item.contains("logical_incident_id")) {
                allIncidents = false;
                break;
            }
        }
        if (allIncidents) {
            return response.events;
        }
    }
    return json::array();
}

json investigationResultToJson(const QueryResponse& response) {
    const json artifact = response.summary.value("artifact", json::object());
    const json report = response.summary.value("report",
        response.summary.value("report_summary", json::object()));
    const json evidence = response.summary.value("evidence", json::object());
    const json citationRows = evidence.value("citations", json::array());
    const std::string headline = firstStringValue(report, {"headline", "title", "summary"});
    std::string detail = firstStringValue(report, {"summary", "why_it_matters"});
    if (detail.empty()) {
        detail = firstStringValue(response.summary, {"what_changed_first", "why_it_matters", "headline"});
    }
    return {
        {"schema", kInvestigationResultSchema},
        {"version", kInvestigationResultVersion},
        {"artifact_id", artifact.value("artifact_id", std::string())},
        {"artifact_kind", artifact.value("artifact_type", artifact.value("artifact_kind", std::string()))},
        {"headline", headline},
        {"detail", detail},
        {"served_revision_id", response.summary.value("served_revision_id", 0ULL)},
        {"includes_mutable_tail", response.summary.value("includes_mutable_tail", false)},
        {"artifact", artifact},
        {"entity", response.summary.value("entity", json::object())},
        {"report", report},
        {"evidence", evidence},
        {"data_quality", response.summary.value("data_quality", json::object())},
        {"replay_range", maybeReplayRangeToJson(response.summary)},
        {"incident_rows", investigationIncidentRows(response)},
        {"citation_rows", citationRows.is_array() ? citationRows : json::array()},
        {"events", response.events.is_array() ? response.events : json::array()}
    };
}

json collectionResultToJson(const json& rows,
                            std::string collectionKind,
                            std::uint64_t servedRevisionId,
                            bool includesMutableTail,
                            std::size_t totalCount) {
    return {
        {"schema", kCollectionResultSchema},
        {"version", kCollectionResultVersion},
        {"collection_kind", std::move(collectionKind)},
        {"served_revision_id", servedRevisionId},
        {"includes_mutable_tail", includesMutableTail},
        {"returned_count", rows.is_array() ? rows.size() : 0},
        {"total_count", totalCount},
        {"rows", rows.is_array() ? rows : json::array()}
    };
}

json seekOrderResultToJson(const json& summary) {
    return {
        {"schema", kSeekOrderResultSchema},
        {"version", kSeekOrderResultVersion},
        {"served_revision_id", summary.value("served_revision_id", 0ULL)},
        {"includes_mutable_tail", summary.value("includes_mutable_tail", false)},
        {"replay_target_session_seq", summary.value("replay_target_session_seq", 0ULL)},
        {"first_session_seq", summary.value("first_session_seq", 0ULL)},
        {"last_session_seq", summary.value("last_session_seq", 0ULL)},
        {"last_fill_session_seq", summary.value("last_fill_session_seq", 0ULL)},
        {"replay_range", maybeReplayRangeToJson(summary)},
        {"anchor", summary.value("anchor", json::object())},
        {"protected_window", summary.value("protected_window", json::object())}
    };
}

json artifactExportResultToJson(const json& summary) {
    return {
        {"schema", kArtifactExportResultSchema},
        {"version", kArtifactExportResultVersion},
        {"artifact_id", summary.value("artifact_id", std::string())},
        {"format", summary.value("export_format", std::string())},
        {"served_revision_id", summary.value("served_revision_id", 0ULL)},
        {"artifact_export", summary.value("artifact_export", json::object())},
        {"markdown", summary.value("markdown", std::string())},
        {"bundle", summary.value("bundle", json::object())}
    };
}

json replaySnapshotResultToJson(const json& summary) {
    json result = summary.is_object() ? summary : json::object();
    result["schema"] = kReplaySnapshotResultSchema;
    result["version"] = kReplaySnapshotResultVersion;
    return result;
}

std::string importedCasesManifestContents(const std::vector<ImportedCaseRecord>& records) {
    std::ostringstream out;
    for (const auto& record : records) {
        out << importedCaseToJson(record).dump() << '\n';
    }
    return out.str();
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

SessionReportRecord sessionReportFromJson(const json& payload) {
    SessionReportRecord record;
    record.reportId = payload.value("report_id", 0ULL);
    record.revisionId = payload.value("revision_id", 0ULL);
    record.fromSessionSeq = payload.value("from_session_seq", 0ULL);
    record.toSessionSeq = payload.value("to_session_seq", 0ULL);
    record.createdTsEngineNs = payload.value("created_ts_engine_ns", 0ULL);
    record.incidentCount = payload.value("incident_count", static_cast<std::size_t>(0));
    record.instrumentId = payload.value("instrument_id", std::string());
    record.headline = payload.value("headline", std::string());
    record.fileName = payload.value("file_name", std::string());
    record.payloadSha256 = payload.value("payload_sha256", std::string());
    return record;
}

CaseReportRecord caseReportFromJson(const json& payload) {
    CaseReportRecord record;
    record.reportId = payload.value("report_id", 0ULL);
    record.revisionId = payload.value("revision_id", 0ULL);
    record.reportType = payload.value("report_type", std::string());
    record.logicalIncidentId = payload.value("logical_incident_id", 0ULL);
    record.anchor = anchorFromJson(payload.value("anchor", json::object()));
    record.firstSessionSeq = payload.value("first_session_seq", 0ULL);
    record.lastSessionSeq = payload.value("last_session_seq", 0ULL);
    record.createdTsEngineNs = payload.value("created_ts_engine_ns", 0ULL);
    record.instrumentId = payload.value("instrument_id", std::string());
    record.headline = payload.value("headline", std::string());
    record.fileName = payload.value("file_name", std::string());
    record.payloadSha256 = payload.value("payload_sha256", std::string());
    return record;
}

ImportedCaseRecord importedCaseFromJson(const json& payload) {
    ImportedCaseRecord record;
    record.importedCaseId = payload.value("imported_case_id", 0ULL);
    record.importedTsEngineNs = payload.value("imported_ts_engine_ns", 0ULL);
    record.bundleId = payload.value("bundle_id", std::string());
    record.bundleType = payload.value("bundle_type", std::string());
    record.sourceArtifactId = payload.value("source_artifact_id", std::string());
    record.sourceReportId = payload.value("source_report_id", 0ULL);
    record.sourceRevisionId = payload.value("source_revision_id", 0ULL);
    record.firstSessionSeq = payload.value("first_session_seq", 0ULL);
    record.lastSessionSeq = payload.value("last_session_seq", 0ULL);
    record.instrumentId = payload.value("instrument_id", std::string());
    record.headline = payload.value("headline", std::string());
    record.fileName = payload.value("file_name", std::string());
    record.sourceBundlePath = payload.value("source_bundle_path", std::string());
    record.payloadSha256 = payload.value("payload_sha256", std::string());
    return record;
}

json artifactLookupIndexToJson(const Server::ArtifactLookupIndex& index) {
    auto appendSorted = [](json* target,
                           const auto& map,
                           const auto& toJsonFn,
                           const auto& lessFn) {
        using ValueType = typename std::decay_t<decltype(map)>::mapped_type;
        std::vector<ValueType> values;
        values.reserve(map.size());
        for (const auto& entry : map) {
            values.push_back(entry.second);
        }
        std::sort(values.begin(), values.end(), lessFn);
        for (const auto& value : values) {
            target->push_back(toJsonFn(value));
        }
    };

    json payload{
        {"schema", "com.foxy.tape-engine.artifact-lookup"},
        {"version", 1},
        {"session_reports", json::array()},
        {"case_reports", json::array()},
        {"imported_cases", json::array()},
        {"order_anchors", json::array()},
        {"protected_windows", json::array()},
        {"findings", json::array()},
        {"incidents", json::array()}
    };

    appendSorted(&payload["session_reports"],
                 index.sessionReportsById,
                 sessionReportToJson,
                 [](const SessionReportRecord& left, const SessionReportRecord& right) {
                     return left.reportId < right.reportId;
                 });
    appendSorted(&payload["case_reports"],
                 index.caseReportsById,
                 caseReportToJson,
                 [](const CaseReportRecord& left, const CaseReportRecord& right) {
                     return left.reportId < right.reportId;
                 });
    appendSorted(&payload["imported_cases"],
                 index.importedCasesById,
                 importedCaseToJson,
                 [](const ImportedCaseRecord& left, const ImportedCaseRecord& right) {
                     return left.importedCaseId < right.importedCaseId;
                 });
    appendSorted(&payload["order_anchors"],
                 index.orderAnchorsById,
                 orderAnchorToJson,
                 [](const OrderAnchorRecord& left, const OrderAnchorRecord& right) {
                     return left.anchorId < right.anchorId;
                 });
    appendSorted(&payload["protected_windows"],
                 index.protectedWindowsById,
                 protectedWindowToJson,
                 [](const ProtectedWindowRecord& left, const ProtectedWindowRecord& right) {
                     return left.windowId < right.windowId;
                 });
    appendSorted(&payload["findings"],
                 index.findingsById,
                 findingToJson,
                 [](const FindingRecord& left, const FindingRecord& right) {
                     return left.findingId < right.findingId;
                 });
    appendSorted(&payload["incidents"],
                 index.incidentsByLogicalIncident,
                 incidentToJson,
                 [](const IncidentRecord& left, const IncidentRecord& right) {
                     return left.logicalIncidentId < right.logicalIncidentId;
                 });
    return payload;
}

Server::ArtifactLookupIndex artifactLookupIndexFromJson(const json& payload) {
    Server::ArtifactLookupIndex index;
    for (const auto& item : payload.value("session_reports", json::array())) {
        SessionReportRecord record = sessionReportFromJson(item);
        index.sessionReportsById[record.reportId] = std::move(record);
    }
    for (const auto& item : payload.value("case_reports", json::array())) {
        CaseReportRecord record = caseReportFromJson(item);
        index.caseReportsById[record.reportId] = std::move(record);
    }
    for (const auto& item : payload.value("imported_cases", json::array())) {
        ImportedCaseRecord record = importedCaseFromJson(item);
        index.importedCasesById[record.importedCaseId] = std::move(record);
    }
    for (const auto& item : payload.value("order_anchors", json::array())) {
        OrderAnchorRecord record = orderAnchorFromJson(item);
        index.orderAnchorsById[record.anchorId] = std::move(record);
    }
    for (const auto& item : payload.value("protected_windows", json::array())) {
        ProtectedWindowRecord record = protectedWindowFromJson(item);
        auto found = index.protectedWindowsById.find(record.windowId);
        if (found == index.protectedWindowsById.end() ||
            protectedWindowIsNewer(record, found->second)) {
            index.protectedWindowsById[record.windowId] = std::move(record);
        }
    }
    for (const auto& item : payload.value("findings", json::array())) {
        FindingRecord record = findingFromJson(item);
        index.findingsById[record.findingId] = std::move(record);
    }
    for (const auto& item : payload.value("incidents", json::array())) {
        IncidentRecord record = incidentFromJson(item);
        auto found = index.incidentsByLogicalIncident.find(record.logicalIncidentId);
        if (found == index.incidentsByLogicalIncident.end() ||
            found->second.incidentRevisionId < record.incidentRevisionId) {
            index.incidentsByLogicalIncident[record.logicalIncidentId] = std::move(record);
        }
    }
    return index;
}

json eventToJson(const EngineEvent& event) {
    const std::string sourceInstrumentId = event.sourceInstrumentId.empty() ? event.bridgeRecord.instrumentId
                                                                            : event.sourceInstrumentId;
    const std::string resolvedStatus = instrumentIdentityStatus(event.instrumentId, event.bridgeRecord.symbol);
    const std::string sourceStatus = instrumentIdentityStatus(sourceInstrumentId, event.bridgeRecord.symbol);
    json payload{
        {"adapter_id", event.adapterId},
        {"connection_id", event.connectionId},
        {"event_kind", event.eventKind},
        {"instrument_id", event.instrumentId},
        {"resolved_instrument_id", event.instrumentId},
        {"source_instrument_id", sourceInstrumentId},
        {"instrument_identity_strength", instrumentIdentityStrength(event.instrumentId)},
        {"instrument_identity_status", event.instrumentIdentityPolicy == "coerced_from_mismatch" ? "mismatch" : resolvedStatus},
        {"resolved_instrument_identity_status", resolvedStatus},
        {"source_instrument_identity_status", sourceStatus},
        {"source_instrument_identity_strength", instrumentIdentityStrength(sourceInstrumentId)},
        {"instrument_identity_source", event.instrumentIdentitySource},
        {"instrument_identity_policy", event.instrumentIdentityPolicy},
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
        if (it.key() == "instrument_id") {
            payload["bridge_instrument_id"] = it.value();
            continue;
        }
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

Server::ArtifactLookupIndex Server::rebuildArtifactLookupIndexUnlocked() const {
    ArtifactLookupIndex index;
    for (const auto& record : sessionReports_) {
        index.sessionReportsById[record.reportId] = record;
    }
    for (const auto& record : caseReports_) {
        index.caseReportsById[record.reportId] = record;
    }
    for (const auto& record : importedCases_) {
        index.importedCasesById[record.importedCaseId] = record;
    }
    for (const auto& record : orderAnchors_) {
        index.orderAnchorsById[record.anchorId] = record;
    }
    for (const auto& record : protectedWindows_) {
        auto found = index.protectedWindowsById.find(record.windowId);
        if (found == index.protectedWindowsById.end() ||
            protectedWindowIsNewer(record, found->second)) {
            index.protectedWindowsById[record.windowId] = record;
        }
    }
    for (const auto& record : findings_) {
        index.findingsById[record.findingId] = record;
    }
    for (const auto& record : incidents_) {
        auto found = index.incidentsByLogicalIncident.find(record.logicalIncidentId);
        if (found == index.incidentsByLogicalIncident.end() ||
            found->second.incidentRevisionId < record.incidentRevisionId) {
            index.incidentsByLogicalIncident[record.logicalIncidentId] = record;
        }
    }
    return index;
}

void Server::upsertArtifactLookupIndexUnlocked(const PendingSegment& segment) {
    for (const auto& record : segment.orderAnchors) {
        artifactLookupIndex_.orderAnchorsById[record.anchorId] = record;
    }
    for (const auto& record : segment.protectedWindows) {
        auto found = artifactLookupIndex_.protectedWindowsById.find(record.windowId);
        if (found == artifactLookupIndex_.protectedWindowsById.end() ||
            protectedWindowIsNewer(record, found->second)) {
            artifactLookupIndex_.protectedWindowsById[record.windowId] = record;
        }
    }
    for (const auto& record : segment.findings) {
        artifactLookupIndex_.findingsById[record.findingId] = record;
    }
    for (const auto& record : segment.incidents) {
        auto found = artifactLookupIndex_.incidentsByLogicalIncident.find(record.logicalIncidentId);
        if (found == artifactLookupIndex_.incidentsByLogicalIncident.end() ||
            found->second.incidentRevisionId < record.incidentRevisionId) {
            artifactLookupIndex_.incidentsByLogicalIncident[record.logicalIncidentId] = record;
        }
    }
}

void Server::upsertArtifactLookupIndexUnlocked(const SessionReportRecord& record) {
    artifactLookupIndex_.sessionReportsById[record.reportId] = record;
}

void Server::upsertArtifactLookupIndexUnlocked(const CaseReportRecord& record) {
    artifactLookupIndex_.caseReportsById[record.reportId] = record;
}

void Server::upsertArtifactLookupIndexUnlocked(const ImportedCaseRecord& record) {
    artifactLookupIndex_.importedCasesById[record.importedCaseId] = record;
}

bool Server::artifactLookupMatchesStateUnlocked(const ArtifactLookupIndex& index) const {
    return artifactLookupIndexToJson(index) == artifactLookupIndexToJson(rebuildArtifactLookupIndexUnlocked());
}

bool Server::persistArtifactLookupIndex(const ArtifactLookupIndex& index, std::string* error) const {
    const std::filesystem::path lookupPath = config_.dataDir / "artifact-lookup.msgpack";
    const std::vector<std::uint8_t> payload = json::to_msgpack(artifactLookupIndexToJson(index));
    if (!writeBinaryFileAtomically(lookupPath, payload, error)) {
        if (error != nullptr && error->rfind("failed to ", 0) != 0) {
            *error = "failed to publish tape-engine artifact lookup index: " + *error;
        } else if (error != nullptr && error->empty()) {
            *error = "failed to publish tape-engine artifact lookup index";
        }
        return false;
    }
    return true;
}

bool Server::restoreArtifactLookupIndex(const std::filesystem::path& path,
                                        ArtifactLookupIndex* index,
                                        std::string* error) const {
    if (index == nullptr) {
        if (error != nullptr) {
            *error = "artifact lookup restore target is null";
        }
        return false;
    }
    std::ifstream in(path, std::ios::binary);
    if (!in.is_open()) {
        if (error != nullptr) {
            *error = "failed to open tape-engine artifact lookup index";
        }
        return false;
    }
    const std::vector<std::uint8_t> bytes((std::istreambuf_iterator<char>(in)),
                                          std::istreambuf_iterator<char>());
    if (bytes.empty()) {
        if (error != nullptr) {
            *error = "tape-engine artifact lookup index is empty";
        }
        return false;
    }
    const json payload = json::from_msgpack(bytes, true, false);
    if (payload.is_discarded() ||
        payload.value("schema", std::string()) != "com.foxy.tape-engine.artifact-lookup") {
        if (error != nullptr) {
            *error = "failed to parse tape-engine artifact lookup index";
        }
        return false;
    }
    *index = artifactLookupIndexFromJson(payload);
    return true;
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
    sessionReports_.clear();
    caseReports_.clear();
    importedCases_.clear();
    writerFailure_.clear();
    lastManifestHash_.clear();
    segmentEventCache_.clear();
    segmentIndexCache_.clear();
    replayCheckpointCache_.clear();
    reportResponseCache_.clear();
    artifactLookupIndex_ = {};
    analyzerBookState_ = {};
    frozenReplayCheckpointState_ = {};
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
    nextSessionReportId_ = 1;
    nextCaseReportId_ = 1;
    nextImportedCaseId_ = 1;

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
        info.indexFileName = entry.value("index_file_name", std::string());
        info.checkpointFileName = entry.value("checkpoint_file_name", std::string());
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

    const std::filesystem::path reportsManifestPath = config_.dataDir / "session-reports.jsonl";
    if (std::filesystem::exists(reportsManifestPath)) {
        std::ifstream reportsIn(reportsManifestPath);
        if (!reportsIn.is_open()) {
            if (error != nullptr) {
                *error = "failed to open tape-engine session reports manifest for restore";
            }
            return false;
        }

        while (std::getline(reportsIn, line)) {
            if (line.empty()) {
                continue;
            }

            const json entry = json::parse(line, nullptr, false);
            if (entry.is_discarded()) {
                if (error != nullptr) {
                    *error = "failed to parse tape-engine session reports manifest during restore";
                }
                return false;
            }

            SessionReportRecord record = sessionReportFromJson(entry);
            sessionReports_.push_back(record);
            nextSessionReportId_ = std::max(nextSessionReportId_, record.reportId + 1);
        }
    }

    const std::filesystem::path caseReportsManifestPath = config_.dataDir / "case-reports.jsonl";
    if (std::filesystem::exists(caseReportsManifestPath)) {
        std::ifstream caseReportsIn(caseReportsManifestPath);
        if (!caseReportsIn.is_open()) {
            if (error != nullptr) {
                *error = "failed to open tape-engine case reports manifest for restore";
            }
            return false;
        }

        while (std::getline(caseReportsIn, line)) {
            if (line.empty()) {
                continue;
            }

            const json entry = json::parse(line, nullptr, false);
            if (entry.is_discarded()) {
                if (error != nullptr) {
                    *error = "failed to parse tape-engine case reports manifest during restore";
                }
                return false;
            }

            CaseReportRecord record = caseReportFromJson(entry);
            caseReports_.push_back(record);
            nextCaseReportId_ = std::max(nextCaseReportId_, record.reportId + 1);
        }
    }

    const std::filesystem::path importedCasesManifestPath = config_.dataDir / "imported-case-bundles.jsonl";
    if (std::filesystem::exists(importedCasesManifestPath) &&
        !restoreImportedCasesManifest(importedCasesManifestPath, error)) {
        return false;
    }

    if (!segments_.empty()) {
        const auto& lastSegment = segments_.back();
        if (!lastSegment.checkpointFileName.empty()) {
            const std::filesystem::path checkpointPath = config_.dataDir / "segments" / lastSegment.checkpointFileName;
            std::ifstream checkpointIn(checkpointPath, std::ios::binary);
            if (checkpointIn.is_open()) {
                const std::vector<std::uint8_t> bytes((std::istreambuf_iterator<char>(checkpointIn)),
                                                      std::istreambuf_iterator<char>());
                if (!bytes.empty()) {
                    const json payload = json::from_msgpack(bytes, true, false);
                    const ReplayBookState replay = replayCheckpointFromJson(payload);
                    frozenReplayCheckpointState_ = ReplayCheckpointRecord{
                        .revisionId = payload.value("revision_id", lastSegment.revisionId),
                        .sessionSeq = replay.replayedThroughSessionSeq,
                        .bidPrice = replay.bidPrice,
                        .askPrice = replay.askPrice,
                        .lastPrice = replay.lastPrice,
                        .bidBook = replay.bidBook,
                        .askBook = replay.askBook,
                        .appliedEvents = replay.appliedEvents,
                        .gapMarkers = replay.gapMarkers
                    };
                    replayCheckpointCache_[lastSegment.checkpointFileName] = frozenReplayCheckpointState_;
                }
            }
        }
    }

    bool shouldPersistLookupIndex = false;
    const std::filesystem::path lookupPath = config_.dataDir / "artifact-lookup.msgpack";
    if (std::filesystem::exists(lookupPath)) {
        std::string lookupError;
        ArtifactLookupIndex restoredLookup;
        if (restoreArtifactLookupIndex(lookupPath, &restoredLookup, &lookupError)) {
            artifactLookupIndex_ = std::move(restoredLookup);
            if (!artifactLookupMatchesStateUnlocked(artifactLookupIndex_)) {
                artifactLookupIndex_ = rebuildArtifactLookupIndexUnlocked();
                shouldPersistLookupIndex = true;
            }
        } else {
            artifactLookupIndex_ = rebuildArtifactLookupIndexUnlocked();
            shouldPersistLookupIndex = true;
        }
    } else {
        artifactLookupIndex_ = rebuildArtifactLookupIndexUnlocked();
        shouldPersistLookupIndex = true;
    }
    if (shouldPersistLookupIndex) {
        std::string persistError;
        if (!persistArtifactLookupIndex(artifactLookupIndex_, &persistError)) {
            if (error != nullptr) {
                *error = persistError;
            }
            return false;
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
    std::filesystem::create_directories(config_.dataDir / "reports", ec);
    if (ec) {
        if (error != nullptr) {
            *error = "failed to create tape-engine reports dir: " + ec.message();
        }
        return false;
    }
    std::filesystem::create_directories(config_.dataDir / "bundles", ec);
    if (ec) {
        if (error != nullptr) {
            *error = "failed to create tape-engine bundles dir: " + ec.message();
        }
        return false;
    }
    std::filesystem::create_directories(config_.dataDir / "imports", ec);
    if (ec) {
        if (error != nullptr) {
            *error = "failed to create tape-engine imports dir: " + ec.message();
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
    snapshot.sessionReports = sessionReports_;
    snapshot.caseReports = caseReports_;
    snapshot.importedCases = importedCases_;
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
        request.operationKind = QueryOperation::Unknown;
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
            failedRequest.operationKind = QueryOperation::Unknown;
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
    return resolveInstrumentIdentity(config_, record).resolvedInstrumentId;
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

        if (hasAnchorIdentity(updated.anchor) &&
            updated.lastSessionSeq > previousLastSessionSeq &&
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

    auto qualifiesLiquidityThinning = [](double previousSize, double currentSize) {
        if (previousSize < kLiquidityChangeMinShares || currentSize <= 0.0) {
            return false;
        }
        const double delta = currentSize - previousSize;
        const double ratio = previousSize > 0.0 ? std::fabs(delta) / previousSize : 0.0;
        return delta <= -kLiquidityChangeMinShares && ratio >= kLiquidityChangeMinRatio;
    };
    auto qualifiesLiquidityRefill = [](double previousSize, double currentSize) {
        if (previousSize < kLiquidityChangeMinShares || currentSize <= 0.0) {
            return false;
        }
        const double delta = currentSize - previousSize;
        const double ratio = previousSize > 0.0 ? std::fabs(delta) / previousSize : 0.0;
        return delta >= kLiquidityChangeMinShares && ratio >= kLiquidityChangeMinRatio;
    };

    const bool askThinDetected = event.eventKind == "market_depth" &&
        hadInside && hasInside &&
        previousAsk > 0.0 &&
        std::fabs(previousAsk - effectiveAsk) <= kTouchTradeTolerance &&
        qualifiesLiquidityThinning(previousAskSize, effectiveAskSize);
    const bool bidThinDetected = event.eventKind == "market_depth" &&
        hadInside && hasInside &&
        previousBid > 0.0 &&
        std::fabs(previousBid - effectiveBid) <= kTouchTradeTolerance &&
        qualifiesLiquidityThinning(previousBidSize, effectiveBidSize);
    const bool askRefillDetected = event.eventKind == "market_depth" &&
        hadInside && hasInside &&
        previousAsk > 0.0 &&
        std::fabs(previousAsk - effectiveAsk) <= kTouchTradeTolerance &&
        qualifiesLiquidityRefill(previousAskSize, effectiveAskSize);
    const bool bidRefillDetected = event.eventKind == "market_depth" &&
        hadInside && hasInside &&
        previousBid > 0.0 &&
        std::fabs(previousBid - effectiveBid) <= kTouchTradeTolerance &&
        qualifiesLiquidityRefill(previousBidSize, effectiveBidSize);

    auto rememberTouchTrade = [&](std::optional<AnalyzerBookState::RecentTouchTrade>& watch,
                                  double touchPrice) {
        if (watch.has_value() && event.sessionSeq > watch->sessionSeq + kLiquidityFollowThroughSeqWindow) {
            watch.reset();
        }
        if (event.eventKind != "market_tick" ||
            event.bridgeRecord.marketField != 4 ||
            !hasInside ||
            !std::isfinite(event.bridgeRecord.price) ||
            touchPrice <= 0.0 ||
            std::fabs(event.bridgeRecord.price - touchPrice) > kTouchTradeTolerance) {
            return;
        }
        AnalyzerBookState::RecentTouchTrade trade;
        trade.price = touchPrice;
        trade.sessionSeq = event.sessionSeq;
        trade.tsEngineNs = event.tsEngineNs;
        watch = std::move(trade);
    };
    rememberTouchTrade(analyzerBookState_.recentAskTouchTrade, effectiveAsk);
    rememberTouchTrade(analyzerBookState_.recentBidTouchTrade, effectiveBid);

    if (askThinDetected) {
        AnalyzerBookState::RecentTouchLiquidityShift shift;
        shift.price = effectiveAsk;
        shift.previousSize = previousAskSize;
        shift.currentSize = effectiveAskSize;
        shift.sessionSeq = event.sessionSeq;
        shift.tsEngineNs = event.tsEngineNs;
        analyzerBookState_.recentAskThinning = std::move(shift);
    }
    if (bidThinDetected) {
        AnalyzerBookState::RecentTouchLiquidityShift shift;
        shift.price = effectiveBid;
        shift.previousSize = previousBidSize;
        shift.currentSize = effectiveBidSize;
        shift.sessionSeq = event.sessionSeq;
        shift.tsEngineNs = event.tsEngineNs;
        analyzerBookState_.recentBidThinning = std::move(shift);
    }
    if (askRefillDetected) {
        AnalyzerBookState::RecentTouchRefillWatch watch;
        watch.price = effectiveAsk;
        watch.refillSize = effectiveAskSize;
        watch.sessionSeq = event.sessionSeq;
        watch.tsEngineNs = event.tsEngineNs;
        analyzerBookState_.recentAskRefill = std::move(watch);
    }
    if (bidRefillDetected) {
        AnalyzerBookState::RecentTouchRefillWatch watch;
        watch.price = effectiveBid;
        watch.refillSize = effectiveBidSize;
        watch.sessionSeq = event.sessionSeq;
        watch.tsEngineNs = event.tsEngineNs;
        analyzerBookState_.recentBidRefill = std::move(watch);
    }

    bool depletionAfterTradeTriggered = false;
    std::string depletionAfterTradeKind;
    std::uint64_t depletionAfterTradeFirstSessionSeq = 0;
    double depletionAfterTradeTradePrice = 0.0;
    double depletionAfterTradeRemainingSize = 0.0;
    auto updateDepletionAfterTrade = [&](const std::optional<AnalyzerBookState::RecentTouchTrade>& tradeWatch,
                                         bool thinDetected,
                                         double currentPrice,
                                         double currentSize,
                                         bool askSide) {
        if (!thinDetected || !tradeWatch.has_value()) {
            return;
        }
        if (tradeWatch->sessionSeq + kLiquidityFollowThroughSeqWindow < event.sessionSeq) {
            return;
        }
        if (currentPrice <= 0.0 || std::fabs(currentPrice - tradeWatch->price) > kTouchTradeTolerance) {
            return;
        }
        depletionAfterTradeTriggered = true;
        depletionAfterTradeKind = askSide ? "ask_depletion_after_trade" : "bid_depletion_after_trade";
        depletionAfterTradeFirstSessionSeq = tradeWatch->sessionSeq;
        depletionAfterTradeTradePrice = tradeWatch->price;
        depletionAfterTradeRemainingSize = currentSize;
    };
    updateDepletionAfterTrade(analyzerBookState_.recentAskTouchTrade, askThinDetected, effectiveAsk, effectiveAskSize, true);
    if (!depletionAfterTradeTriggered) {
        updateDepletionAfterTrade(analyzerBookState_.recentBidTouchTrade, bidThinDetected, effectiveBid, effectiveBidSize, false);
    }
    if (depletionAfterTradeTriggered) {
        if (depletionAfterTradeKind == "ask_depletion_after_trade") {
            analyzerBookState_.recentAskTouchTrade.reset();
        } else if (depletionAfterTradeKind == "bid_depletion_after_trade") {
            analyzerBookState_.recentBidTouchTrade.reset();
        }
    }

    bool tradeAfterDepletionTriggered = false;
    std::string tradeAfterDepletionKind;
    std::uint64_t tradeAfterDepletionFirstSessionSeq = 0;
    double tradeAfterDepletionReferencePrice = 0.0;
    double tradeAfterDepletionTradePrice = 0.0;
    auto updateTradeAfterDepletion = [&](std::optional<AnalyzerBookState::RecentTouchLiquidityShift>& watch,
                                         bool askSide) {
        if (!watch.has_value()) {
            return;
        }
        if (event.sessionSeq > watch->sessionSeq + kLiquidityFollowThroughSeqWindow) {
            watch.reset();
            return;
        }
        if (event.eventKind != "market_tick" ||
            event.bridgeRecord.marketField != 4 ||
            !std::isfinite(event.bridgeRecord.price) ||
            watch->sawTradeAfterDepletion) {
            return;
        }
        const bool touchedDepletedSide = askSide
            ? event.bridgeRecord.price >= watch->price - kTouchTradeTolerance
            : event.bridgeRecord.price <= watch->price + kTouchTradeTolerance;
        if (!touchedDepletedSide) {
            return;
        }
        tradeAfterDepletionTriggered = true;
        tradeAfterDepletionKind = askSide ? "ask_trade_after_depletion" : "bid_trade_after_depletion";
        tradeAfterDepletionFirstSessionSeq = watch->sessionSeq;
        tradeAfterDepletionReferencePrice = watch->price;
        tradeAfterDepletionTradePrice = event.bridgeRecord.price;
        watch->sawTradeAfterDepletion = true;
    };
    updateTradeAfterDepletion(analyzerBookState_.recentAskThinning, true);
    if (!tradeAfterDepletionTriggered) {
        updateTradeAfterDepletion(analyzerBookState_.recentBidThinning, false);
    }

    bool pullFollowThroughTriggered = false;
    std::string pullFollowThroughKind;
    std::uint64_t pullFollowThroughFirstSessionSeq = 0;
    double pullFollowThroughReferencePrice = 0.0;
    double pullFollowThroughObservedPrice = 0.0;
    auto updatePullFollowThrough = [&](std::optional<AnalyzerBookState::RecentTouchLiquidityShift>& watch,
                                       bool askSide,
                                       double currentPrice,
                                       double currentSize) {
        if (!watch.has_value()) {
            return;
        }
        if (event.sessionSeq <= watch->sessionSeq) {
            return;
        }
        if (event.sessionSeq > watch->sessionSeq + kLiquidityFollowThroughSeqWindow) {
            watch.reset();
            return;
        }

        const bool tradeFollowThrough = event.eventKind == "market_tick" &&
            event.bridgeRecord.marketField == 4 &&
            std::isfinite(event.bridgeRecord.price) &&
            (askSide
                ? event.bridgeRecord.price >= watch->price - kTouchTradeTolerance
                : event.bridgeRecord.price <= watch->price + kTouchTradeTolerance);
        const bool touchMoved = currentPrice > 0.0 &&
            (askSide
                ? currentPrice > watch->price + kTouchTradeTolerance
                : currentPrice < watch->price - kTouchTradeTolerance);
        const bool replenished = currentPrice > 0.0 &&
            std::fabs(currentPrice - watch->price) <= kTouchTradeTolerance &&
            currentSize >= watch->previousSize * 0.8;
        if (tradeFollowThrough || touchMoved) {
            pullFollowThroughTriggered = true;
            pullFollowThroughKind = askSide ? "ask_pull_follow_through" : "bid_pull_follow_through";
            pullFollowThroughFirstSessionSeq = watch->sessionSeq;
            pullFollowThroughReferencePrice = watch->price;
            pullFollowThroughObservedPrice = tradeFollowThrough ? event.bridgeRecord.price : currentPrice;
            watch.reset();
        } else if (replenished) {
            watch.reset();
        }
    };
    updatePullFollowThrough(analyzerBookState_.recentAskThinning, true, effectiveAsk, effectiveAskSize);
    if (!pullFollowThroughTriggered) {
        updatePullFollowThrough(analyzerBookState_.recentBidThinning, false, effectiveBid, effectiveBidSize);
    }

    bool absorptionPersistenceTriggered = false;
    std::string absorptionPersistenceKind;
    std::uint64_t absorptionPersistenceFirstSessionSeq = 0;
    std::size_t absorptionPersistenceStableUpdates = 0;
    std::size_t absorptionPersistenceTouchTrades = 0;
    double absorptionPersistencePrice = 0.0;
    bool genuineRefillTriggered = false;
    std::string genuineRefillKind;
    std::uint64_t genuineRefillFirstSessionSeq = 0;
    std::size_t genuineRefillStableUpdates = 0;
    double genuineRefillPrice = 0.0;
    auto updateAbsorptionPersistence = [&](std::optional<AnalyzerBookState::RecentTouchRefillWatch>& watch,
                                           bool askSide,
                                           double currentPrice,
                                           double currentSize) {
        if (!watch.has_value()) {
            return;
        }
        if (event.sessionSeq <= watch->sessionSeq) {
            return;
        }
        if (event.sessionSeq > watch->sessionSeq + kLiquidityFollowThroughSeqWindow) {
            watch.reset();
            return;
        }
        if (currentPrice <= 0.0 || std::fabs(currentPrice - watch->price) > kTouchTradeTolerance) {
            watch.reset();
            return;
        }
        if (currentSize < watch->refillSize * 0.6) {
            watch.reset();
            return;
        }
        watch->stableUpdateCount += 1;
        if (event.eventKind == "market_tick" &&
            event.bridgeRecord.marketField == 4 &&
            std::isfinite(event.bridgeRecord.price) &&
            (askSide
                ? event.bridgeRecord.price >= watch->price - kTouchTradeTolerance
                : event.bridgeRecord.price <= watch->price + kTouchTradeTolerance)) {
            watch->touchTradeCount += 1;
        }
        if (watch->stableUpdateCount >= 2 && watch->touchTradeCount >= 1) {
            absorptionPersistenceTriggered = true;
            absorptionPersistenceKind = askSide ? "ask_absorption_persistence" : "bid_absorption_persistence";
            absorptionPersistenceFirstSessionSeq = watch->sessionSeq;
            absorptionPersistenceStableUpdates = watch->stableUpdateCount;
            absorptionPersistenceTouchTrades = watch->touchTradeCount;
            absorptionPersistencePrice = watch->price;
            watch.reset();
        } else if (watch->stableUpdateCount >= 3 && watch->touchTradeCount == 0) {
            genuineRefillTriggered = true;
            genuineRefillKind = askSide ? "ask_genuine_refill" : "bid_genuine_refill";
            genuineRefillFirstSessionSeq = watch->sessionSeq;
            genuineRefillStableUpdates = watch->stableUpdateCount;
            genuineRefillPrice = watch->price;
            watch.reset();
        }
    };
    updateAbsorptionPersistence(analyzerBookState_.recentAskRefill, true, effectiveAsk, effectiveAskSize);
    if (!absorptionPersistenceTriggered && !genuineRefillTriggered) {
        updateAbsorptionPersistence(analyzerBookState_.recentBidRefill, false, effectiveBid, effectiveBidSize);
    }

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

    auto resetQuoteFlicker = [](auto* state, double price) {
        state->lastPrice = price;
        state->firstChangeSessionSeq = 0;
        state->lastChangeSessionSeq = 0;
        state->changeCount = 0;
    };

    auto updateQuoteFlicker = [&](auto* state,
                                  const std::string& kind,
                                  double previousPrice,
                                  double currentPrice) {
        if (previousPrice <= 0.0 || currentPrice <= 0.0) {
            resetQuoteFlicker(state, currentPrice);
            return std::tuple<bool, std::string, std::size_t, std::uint64_t, double>{
                false, std::string(), 0, 0, 0.0
            };
        }
        if (std::fabs(previousPrice - currentPrice) <= kTouchTradeTolerance) {
            if (state->lastChangeSessionSeq > 0 &&
                event.sessionSeq > state->lastChangeSessionSeq + kQuoteFlickerSeqWindow) {
                resetQuoteFlicker(state, currentPrice);
            }
            return std::tuple<bool, std::string, std::size_t, std::uint64_t, double>{
                false, std::string(), 0, 0, 0.0
            };
        }
        if (state->lastChangeSessionSeq == 0 ||
            event.sessionSeq > state->lastChangeSessionSeq + kQuoteFlickerSeqWindow) {
            state->firstChangeSessionSeq = event.sessionSeq;
            state->changeCount = 1;
        } else {
            state->changeCount += 1;
        }
        state->lastChangeSessionSeq = event.sessionSeq;
        state->lastPrice = currentPrice;
        if (state->changeCount >= kQuoteFlickerMinChanges) {
            const auto firstSessionSeq = state->firstChangeSessionSeq;
            const auto changeCount = state->changeCount;
            resetQuoteFlicker(state, currentPrice);
            return std::tuple<bool, std::string, std::size_t, std::uint64_t, double>{
                true, kind, changeCount, firstSessionSeq, currentPrice
            };
        }
        return std::tuple<bool, std::string, std::size_t, std::uint64_t, double>{
            false, std::string(), 0, 0, 0.0
        };
    };

    bool quoteFlickerTriggered = false;
    std::string quoteFlickerKind;
    std::size_t quoteFlickerChangeCount = 0;
    std::uint64_t quoteFlickerFirstSessionSeq = 0;
    double quoteFlickerObservedPrice = 0.0;
    if (hadInside && hasInside &&
        (event.eventKind == "market_tick" || event.eventKind == "market_depth")) {
        if (previousAsk > 0.0) {
            auto [triggered, kind, changeCount, firstChangeSessionSeq, observedPrice] =
                updateQuoteFlicker(&analyzerBookState_.askQuoteFlicker,
                                   "ask_quote_flicker",
                                   previousAsk,
                                   effectiveAsk);
            if (triggered) {
                quoteFlickerTriggered = true;
                quoteFlickerKind = std::move(kind);
                quoteFlickerChangeCount = changeCount;
                quoteFlickerFirstSessionSeq = firstChangeSessionSeq;
                quoteFlickerObservedPrice = observedPrice;
            }
        }
        if (!quoteFlickerTriggered && previousBid > 0.0) {
            auto [triggered, kind, changeCount, firstChangeSessionSeq, observedPrice] =
                updateQuoteFlicker(&analyzerBookState_.bidQuoteFlicker,
                                   "bid_quote_flicker",
                                   previousBid,
                                   effectiveBid);
            if (triggered) {
                quoteFlickerTriggered = true;
                quoteFlickerKind = std::move(kind);
                quoteFlickerChangeCount = changeCount;
                quoteFlickerFirstSessionSeq = firstChangeSessionSeq;
                quoteFlickerObservedPrice = observedPrice;
            }
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
        .fillInvalidationObservedPrice = fillInvalidationObservedPrice,
        .tradeAfterDepletionTriggered = tradeAfterDepletionTriggered,
        .tradeAfterDepletionKind = tradeAfterDepletionKind,
        .tradeAfterDepletionFirstSessionSeq = tradeAfterDepletionFirstSessionSeq,
        .tradeAfterDepletionReferencePrice = tradeAfterDepletionReferencePrice,
        .tradeAfterDepletionTradePrice = tradeAfterDepletionTradePrice,
        .absorptionPersistenceTriggered = absorptionPersistenceTriggered,
        .absorptionPersistenceKind = absorptionPersistenceKind,
        .absorptionPersistenceFirstSessionSeq = absorptionPersistenceFirstSessionSeq,
        .absorptionPersistenceStableUpdates = absorptionPersistenceStableUpdates,
        .absorptionPersistenceTouchTrades = absorptionPersistenceTouchTrades,
        .absorptionPersistencePrice = absorptionPersistencePrice,
        .genuineRefillTriggered = genuineRefillTriggered,
        .genuineRefillKind = genuineRefillKind,
        .genuineRefillFirstSessionSeq = genuineRefillFirstSessionSeq,
        .genuineRefillStableUpdates = genuineRefillStableUpdates,
        .genuineRefillPrice = genuineRefillPrice,
        .depletionAfterTradeTriggered = depletionAfterTradeTriggered,
        .depletionAfterTradeKind = depletionAfterTradeKind,
        .depletionAfterTradeFirstSessionSeq = depletionAfterTradeFirstSessionSeq,
        .depletionAfterTradeTradePrice = depletionAfterTradeTradePrice,
        .depletionAfterTradeRemainingSize = depletionAfterTradeRemainingSize,
        .pullFollowThroughTriggered = pullFollowThroughTriggered,
        .pullFollowThroughKind = pullFollowThroughKind,
        .pullFollowThroughFirstSessionSeq = pullFollowThroughFirstSessionSeq,
        .pullFollowThroughReferencePrice = pullFollowThroughReferencePrice,
        .pullFollowThroughObservedPrice = pullFollowThroughObservedPrice,
        .quoteFlickerTriggered = quoteFlickerTriggered,
        .quoteFlickerKind = quoteFlickerKind,
        .quoteFlickerFirstSessionSeq = quoteFlickerFirstSessionSeq,
        .quoteFlickerChangeCount = quoteFlickerChangeCount,
        .quoteFlickerObservedPrice = quoteFlickerObservedPrice
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
    const std::filesystem::path indexPath = config_.dataDir / "segments" / (baseName + ".index.msgpack");
    const std::filesystem::path checkpointPath = config_.dataDir / "segments" / (baseName + ".checkpoint.msgpack");
    const std::filesystem::path metadataPath = config_.dataDir / "segments" / (baseName + ".meta.json");
    const std::filesystem::path artifactsPath = config_.dataDir / "segments" / (baseName + ".artifacts.msgpack");
    const std::filesystem::path manifestPath = config_.dataDir / "manifest.jsonl";

    json payloadJson = json::array();
    std::vector<json> payloadEvents;
    std::unordered_set<std::string> eventAnchorSelectors;
    ReplayBookState replayCheckpointState;
    {
        std::lock_guard<std::mutex> lock(stateMutex_);
        replayCheckpointState.bidPrice = frozenReplayCheckpointState_.bidPrice;
        replayCheckpointState.askPrice = frozenReplayCheckpointState_.askPrice;
        replayCheckpointState.lastPrice = frozenReplayCheckpointState_.lastPrice;
        replayCheckpointState.bidBook = frozenReplayCheckpointState_.bidBook;
        replayCheckpointState.askBook = frozenReplayCheckpointState_.askBook;
        replayCheckpointState.replayedThroughSessionSeq = frozenReplayCheckpointState_.sessionSeq;
        replayCheckpointState.appliedEvents = frozenReplayCheckpointState_.appliedEvents;
        replayCheckpointState.gapMarkers = frozenReplayCheckpointState_.gapMarkers;
    }
    for (const auto& event : segment.events) {
        json eventJson = eventToJson(event);
        payloadJson.push_back(eventJson);
        payloadEvents.push_back(eventJson);
        const BridgeAnchorIdentity eventAnchor = anchorFromJson(eventJson.value("anchor", json::object()));
        for (const auto& key : anchorSelectorKeys(eventAnchor)) {
            eventAnchorSelectors.insert(key);
        }
        applyEventToReplayState(&replayCheckpointState, eventJson);
    }
    const std::vector<std::uint8_t> payload = json::to_msgpack(payloadJson);

    {
        std::ofstream out(segmentPath, std::ios::binary);
        if (!out.is_open()) {
            throw std::runtime_error("failed to open tape-engine segment for write");
        }
        out.write(reinterpret_cast<const char*>(payload.data()), static_cast<std::streamsize>(payload.size()));
    }

    json indexJson{
        {"event_anchor_selectors", json::array()},
        {"first_session_seq", 0ULL},
        {"last_session_seq", 0ULL},
        {"revision_id", segment.revisionId}
    };
    for (const auto& key : eventAnchorSelectors) {
        indexJson["event_anchor_selectors"].push_back(key);
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
    info.indexFileName = indexPath.filename().string();
    info.checkpointFileName = checkpointPath.filename().string();
    info.metadataFileName = metadataPath.filename().string();
    info.artifactsFileName = artifactsPath.filename().string();
    info.payloadSha256 = sha256Hex(payload);
    info.prevManifestHash = previousManifestHash;
    indexJson["first_session_seq"] = info.firstSessionSeq;
    indexJson["last_session_seq"] = info.lastSessionSeq;
    const std::vector<std::uint8_t> indexPayload = json::to_msgpack(indexJson);

    {
        std::ofstream indexOut(indexPath, std::ios::binary);
        if (!indexOut.is_open()) {
            throw std::runtime_error("failed to open tape-engine segment index for write");
        }
        indexOut.write(reinterpret_cast<const char*>(indexPayload.data()),
                       static_cast<std::streamsize>(indexPayload.size()));
    }

    const json checkpointJson = replayCheckpointToJson(replayCheckpointState, segment.revisionId);
    const std::vector<std::uint8_t> checkpointPayload = json::to_msgpack(checkpointJson);
    {
        std::ofstream checkpointOut(checkpointPath, std::ios::binary);
        if (!checkpointOut.is_open()) {
            throw std::runtime_error("failed to open tape-engine replay checkpoint for write");
        }
        checkpointOut.write(reinterpret_cast<const char*>(checkpointPayload.data()),
                            static_cast<std::streamsize>(checkpointPayload.size()));
    }

    json manifestEntry{
        {"event_count", info.eventCount},
        {"file_name", info.fileName},
        {"first_session_seq", info.firstSessionSeq},
        {"last_session_seq", info.lastSessionSeq},
        {"index_file_name", info.indexFileName},
        {"checkpoint_file_name", info.checkpointFileName},
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

    ArtifactLookupIndex lookupIndexSnapshot;
    {
        std::lock_guard<std::mutex> lock(stateMutex_);
        lastManifestHash_ = info.manifestHash;
        latestFrozenRevisionId_ = std::max(latestFrozenRevisionId_, segment.revisionId);
        latestFrozenSessionSeq_ = std::max(latestFrozenSessionSeq_, info.lastSessionSeq);
        frozenReplayCheckpointState_ = ReplayCheckpointRecord{
            .revisionId = segment.revisionId,
            .sessionSeq = replayCheckpointState.replayedThroughSessionSeq,
            .bidPrice = replayCheckpointState.bidPrice,
            .askPrice = replayCheckpointState.askPrice,
            .lastPrice = replayCheckpointState.lastPrice,
            .bidBook = replayCheckpointState.bidBook,
            .askBook = replayCheckpointState.askBook,
            .appliedEvents = replayCheckpointState.appliedEvents,
            .gapMarkers = replayCheckpointState.gapMarkers
        };
        segments_.push_back(std::move(info));
        upsertArtifactLookupIndexUnlocked(segment);
        lookupIndexSnapshot = artifactLookupIndex_;
    }
    {
        std::string lookupError;
        if (!persistArtifactLookupIndex(lookupIndexSnapshot, &lookupError)) {
            throw std::runtime_error(lookupError);
        }
    }
    {
        std::lock_guard<std::mutex> lock(segmentCacheMutex_);
        segmentEventCache_[segmentPath.filename().string()] = payloadEvents;
        segmentIndexCache_[indexPath.filename().string()] = SegmentArtifactIndex{
            .revisionId = segment.revisionId,
            .firstSessionSeq = info.firstSessionSeq,
            .lastSessionSeq = info.lastSessionSeq,
            .eventAnchorSelectors = std::move(eventAnchorSelectors)
        };
        replayCheckpointCache_[checkpointPath.filename().string()] = ReplayCheckpointRecord{
            .revisionId = segment.revisionId,
            .sessionSeq = replayCheckpointState.replayedThroughSessionSeq,
            .bidPrice = replayCheckpointState.bidPrice,
            .askPrice = replayCheckpointState.askPrice,
            .lastPrice = replayCheckpointState.lastPrice,
            .bidBook = replayCheckpointState.bidBook,
            .askBook = replayCheckpointState.askBook,
            .appliedEvents = replayCheckpointState.appliedEvents,
            .gapMarkers = replayCheckpointState.gapMarkers
        };
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
    snapshot.sessionReports = sessionReports_;
    snapshot.caseReports = caseReports_;
    snapshot.importedCases = importedCases_;
    snapshot.sessionReportsById = artifactLookupIndex_.sessionReportsById;
    snapshot.caseReportsById = artifactLookupIndex_.caseReportsById;
    snapshot.importedCasesById = artifactLookupIndex_.importedCasesById;
    snapshot.orderAnchorsById = artifactLookupIndex_.orderAnchorsById;
    snapshot.protectedWindowsById = artifactLookupIndex_.protectedWindowsById;
    snapshot.findingsById = artifactLookupIndex_.findingsById;
    snapshot.incidentsByLogicalIncident = artifactLookupIndex_.incidentsByLogicalIncident;
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
        artifacts.orderAnchorsById[artifacts.orderAnchors[i].anchorId] = i;
        for (const auto& key : anchorSelectorKeys(artifacts.orderAnchors[i].anchor)) {
            artifacts.orderAnchorsBySelector.emplace(key, i);
        }
    }
    for (std::size_t i = 0; i < artifacts.protectedWindows.size(); ++i) {
        const auto& window = artifacts.protectedWindows[i];
        auto existingById = artifacts.protectedWindowsById.find(window.windowId);
        if (existingById == artifacts.protectedWindowsById.end() ||
            protectedWindowIsNewer(window, artifacts.protectedWindows[existingById->second])) {
            artifacts.protectedWindowsById[window.windowId] = i;
        }
        for (const auto& key : anchorSelectorKeys(window.anchor)) {
            artifacts.protectedWindowsBySelector.emplace(key, i);
        }
        if (window.logicalIncidentId > 0) {
            artifacts.protectedWindowsByIncident.emplace(window.logicalIncidentId, i);
        }
        auto existing = artifacts.latestProtectedWindowById.find(window.windowId);
        if (existing == artifacts.latestProtectedWindowById.end() ||
            protectedWindowIsNewer(window, artifacts.protectedWindows[existing->second])) {
            artifacts.latestProtectedWindowById[window.windowId] = i;
        }
    }
    for (std::size_t i = 0; i < artifacts.findings.size(); ++i) {
        artifacts.findingsById[artifacts.findings[i].findingId] = i;
        if (artifacts.findings[i].logicalIncidentId > 0) {
            artifacts.findingsByIncident.emplace(artifacts.findings[i].logicalIncidentId, i);
        }
        for (const auto& key : anchorSelectorKeys(artifacts.findings[i].overlappingAnchor)) {
            artifacts.findingsBySelector.emplace(key, i);
        }
    }
    for (std::size_t i = 0; i < artifacts.incidents.size(); ++i) {
        if (artifacts.incidents[i].logicalIncidentId > 0) {
            artifacts.incidentsByLogicalIncident.emplace(artifacts.incidents[i].logicalIncidentId, i);
            auto existing = artifacts.latestIncidentByLogicalIncident.find(artifacts.incidents[i].logicalIncidentId);
            if (existing == artifacts.latestIncidentByLogicalIncident.end() ||
                artifacts.incidents[existing->second].incidentRevisionId < artifacts.incidents[i].incidentRevisionId) {
                artifacts.latestIncidentByLogicalIncident[artifacts.incidents[i].logicalIncidentId] = i;
            }
        }
        for (const auto& key : anchorSelectorKeys(artifacts.incidents[i].overlappingAnchor)) {
            artifacts.incidentsBySelector.emplace(key, i);
        }
    }

    return artifacts;
}

Server::SegmentArtifactIndex Server::loadSegmentArtifactIndex(const QuerySnapshot& snapshot,
                                                              const SegmentInfo& segment) const {
    if (segment.indexFileName.empty()) {
        return SegmentArtifactIndex{
            .revisionId = segment.revisionId,
            .firstSessionSeq = segment.firstSessionSeq,
            .lastSessionSeq = segment.lastSessionSeq
        };
    }

    {
        std::lock_guard<std::mutex> lock(segmentCacheMutex_);
        const auto found = segmentIndexCache_.find(segment.indexFileName);
        if (found != segmentIndexCache_.end()) {
            return found->second;
        }
    }

    SegmentArtifactIndex index{
        .revisionId = segment.revisionId,
        .firstSessionSeq = segment.firstSessionSeq,
        .lastSessionSeq = segment.lastSessionSeq
    };
    const std::filesystem::path indexPath = snapshot.dataDir / "segments" / segment.indexFileName;
    std::ifstream in(indexPath, std::ios::binary);
    if (in.is_open()) {
        const std::vector<std::uint8_t> bytes((std::istreambuf_iterator<char>(in)),
                                              std::istreambuf_iterator<char>());
        if (!bytes.empty()) {
            const json payload = json::from_msgpack(bytes, true, false);
            index.revisionId = payload.value("revision_id", segment.revisionId);
            index.firstSessionSeq = payload.value("first_session_seq", segment.firstSessionSeq);
            index.lastSessionSeq = payload.value("last_session_seq", segment.lastSessionSeq);
            for (const auto& item : payload.value("event_anchor_selectors", json::array())) {
                if (item.is_string()) {
                    index.eventAnchorSelectors.insert(item.get<std::string>());
                }
            }
        }
    }

    {
        std::lock_guard<std::mutex> lock(segmentCacheMutex_);
        segmentIndexCache_[segment.indexFileName] = index;
    }
    return index;
}

std::optional<Server::ReplayCheckpointRecord> Server::loadReplayCheckpoint(const QuerySnapshot& snapshot,
                                                                           const SegmentInfo& segment) const {
    if (segment.checkpointFileName.empty()) {
        return std::nullopt;
    }

    {
        std::lock_guard<std::mutex> lock(segmentCacheMutex_);
        const auto found = replayCheckpointCache_.find(segment.checkpointFileName);
        if (found != replayCheckpointCache_.end()) {
            return found->second;
        }
    }

    const std::filesystem::path checkpointPath = snapshot.dataDir / "segments" / segment.checkpointFileName;
    std::ifstream in(checkpointPath, std::ios::binary);
    if (!in.is_open()) {
        return std::nullopt;
    }

    const std::vector<std::uint8_t> bytes((std::istreambuf_iterator<char>(in)),
                                          std::istreambuf_iterator<char>());
    if (bytes.empty()) {
        return std::nullopt;
    }

    const json payload = json::from_msgpack(bytes, true, false);
    ReplayCheckpointRecord checkpoint{
        .revisionId = payload.value("revision_id", segment.revisionId),
        .sessionSeq = payload.value("session_seq", 0ULL),
        .bidPrice = payload.value("bid_price", 0.0),
        .askPrice = payload.value("ask_price", 0.0),
        .lastPrice = payload.value("last_price", 0.0),
        .bidBook = bookFromJson(payload.value("bid_book", json::array())),
        .askBook = bookFromJson(payload.value("ask_book", json::array())),
        .appliedEvents = payload.value("applied_event_count", static_cast<std::size_t>(0)),
        .gapMarkers = payload.value("gap_markers", static_cast<std::size_t>(0))
    };

    {
        std::lock_guard<std::mutex> lock(segmentCacheMutex_);
        replayCheckpointCache_[segment.checkpointFileName] = checkpoint;
    }
    return checkpoint;
}

QueryResponse Server::buildSessionOverviewResponse(const QueryRequest& request,
                                                   const QuerySnapshot& snapshot,
                                                   std::uint64_t frozenRevisionId) const {
    QueryResponse response;
    response.requestId = request.requestId;
    response.operation = request.operation;

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
    for (const auto& incident : collapseAdjustedIncidents(snapshot,
                                                          artifacts,
                                                          artifacts.incidents,
                                                          frozenRevisionId,
                                                          request.includeLiveTail)) {
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
    json citations = json::array();
    for (std::size_t i = 0; i < topIncidents.size() && i < limit; ++i) {
        citations.push_back(evidenceCitation("incident",
                                             incidentArtifactId(topIncidents[i].logicalIncidentId),
                                             topIncidents[i].firstSessionSeq,
                                             topIncidents[i].lastSessionSeq,
                                             topIncidents[i].title));
    }
    for (std::size_t i = 0; i < topFindings.size() && i < std::min<std::size_t>(3, limit); ++i) {
        citations.push_back(evidenceCitation("finding",
                                             findingArtifactId(topFindings[i].findingId),
                                             topFindings[i].firstSessionSeq,
                                             topFindings[i].lastSessionSeq,
                                             topFindings[i].title));
    }
    for (std::size_t i = 0; i < protectedWindows.size() && i < std::min<std::size_t>(2, limit); ++i) {
        citations.push_back(evidenceCitation("protected_window",
                                             protectedWindowArtifactId(protectedWindows[i].windowId),
                                             protectedWindows[i].firstSessionSeq,
                                             protectedWindows[i].lastSessionSeq,
                                             protectedWindows[i].reason));
    }
    const json dataQuality = buildDataQualitySummary(allEvents, request.includeLiveTail, snapshot.instrumentId);

    std::string topIncidentWhy;
    std::string topIncidentUncertainty;
    std::string topIncidentRankingReason;
    if (!topIncidents.empty()) {
        const std::vector<FindingRecord> topIncidentFindings =
            relatedIncidentFindings(artifacts, topIncidents.front().logicalIncidentId);
        const json topIncidentDataQuality =
            buildIncidentDataQualitySummary(snapshot,
                                            artifacts,
                                            topIncidents.front(),
                                            frozenRevisionId,
                                            request.includeLiveTail);
        topIncidentWhy = incidentWhyItMatters(topIncidents.front());
        topIncidentUncertainty = reportUncertaintySummary(topIncidentDataQuality,
                                                          topIncidentFindings.size(),
                                                          distinctIncidentFindingKinds(topIncidentFindings));
        std::ostringstream ranking;
        ranking << "Top-ranked because it carries score " << std::fixed << std::setprecision(2)
                << topIncidents.front().score
                << " across " << topIncidents.front().findingCount << " findings";
        const std::size_t corroboratingKinds = distinctIncidentFindingKinds(topIncidentFindings);
        if (corroboratingKinds > 1) {
            ranking << " from " << corroboratingKinds << " corroborating signal families";
        }
        if (topIncidents.front().overlapsOrder) {
            ranking << " and overlaps an active order/fill anchor";
        }
        ranking << '.';
        topIncidentRankingReason = ranking.str();
    }

    json reportSummary{
        {"headline", "Session overview"},
        {"summary", "Ranked incidents, findings, protected windows, and data-quality scoring for the requested session range."},
        {"timeline_highlights", buildTimelineHighlights(timeline, 5)},
        {"top_incident_kind", topIncidents.empty() ? std::string() : topIncidents.front().kind},
        {"top_incident_title", topIncidents.empty() ? std::string() : topIncidents.front().title},
        {"top_incident_why_it_matters", topIncidentWhy},
        {"top_incident_ranking_reason", topIncidentRankingReason},
        {"top_incident_uncertainty", topIncidentUncertainty},
        {"what_changed_first", firstTimelineHeadline(timeline)}
    };
    const json artifact = {
        {"artifact_id", "session-overview:" + std::to_string(frozenRevisionId) + ":" + std::to_string(from) + ":" + std::to_string(to)},
        {"artifact_type", "session_overview"},
        {"from_session_seq", from},
        {"revision_id", frozenRevisionId},
        {"to_session_seq", to}
    };
    const json entity = {
        {"from_session_seq", from},
        {"served_revision_id", frozenRevisionId},
        {"to_session_seq", to},
        {"type", "session_range"}
    };
    const json evidence = buildEvidenceSection(timeline, timelineSummary, citations, dataQuality);

    response.summary = {
        {"artifact", artifact},
        {"entity", entity},
        {"evidence", evidence},
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
        {"data_quality", dataQuality},
        {"report", reportSummary}
    };
    annotateInvestigationEnvelope(&response,
                                  frozenRevisionId,
                                  request.includeLiveTail,
                                  "session_overview",
                                  "session_overview");
    return response;
}

std::optional<SessionReportRecord> Server::findSessionReport(const QuerySnapshot& snapshot,
                                                             std::uint64_t revisionId,
                                                             std::uint64_t fromSessionSeq,
                                                             std::uint64_t toSessionSeq) const {
    for (auto it = snapshot.sessionReports.rbegin(); it != snapshot.sessionReports.rend(); ++it) {
        if (it->revisionId == revisionId &&
            it->fromSessionSeq == fromSessionSeq &&
            it->toSessionSeq == toSessionSeq) {
            return *it;
        }
    }
    return std::nullopt;
}

QueryResponse Server::loadSessionReportResponse(const QuerySnapshot& snapshot,
                                                const SessionReportRecord& report) const {
    {
        std::lock_guard<std::mutex> lock(segmentCacheMutex_);
        const auto found = reportResponseCache_.find(report.fileName);
        if (found != reportResponseCache_.end()) {
            QueryResponse cached = found->second;
            cached.summary["session_report"] = sessionReportToJson(report);
            cached.summary["artifact"] = {
                {"artifact_id", sessionReportArtifactId(report.reportId)},
                {"artifact_type", "session_report"},
                {"from_session_seq", report.fromSessionSeq},
                {"revision_id", report.revisionId},
                {"to_session_seq", report.toSessionSeq}
            };
            cached.summary["is_durable_report"] = true;
            annotateInvestigationEnvelope(&cached,
                                          report.revisionId,
                                          false,
                                          "session_report",
                                          "session_overview");
            return cached;
        }
    }

    const std::filesystem::path reportPath = snapshot.dataDir / "reports" / report.fileName;
    std::ifstream in(reportPath, std::ios::binary);
    if (!in.is_open()) {
        throw std::runtime_error("failed to open session report artifact");
    }

    const std::vector<std::uint8_t> bytes((std::istreambuf_iterator<char>(in)),
                                          std::istreambuf_iterator<char>());
    if (bytes.empty()) {
        throw std::runtime_error("session report artifact is empty");
    }

    QueryResponse response = queryResponseFromJson(json::from_msgpack(bytes, true, false));
    {
        std::lock_guard<std::mutex> lock(segmentCacheMutex_);
        reportResponseCache_[report.fileName] = response;
    }
    response.summary["session_report"] = sessionReportToJson(report);
    response.summary["artifact"] = {
        {"artifact_id", sessionReportArtifactId(report.reportId)},
        {"artifact_type", "session_report"},
        {"from_session_seq", report.fromSessionSeq},
        {"revision_id", report.revisionId},
        {"to_session_seq", report.toSessionSeq}
    };
    response.summary["is_durable_report"] = true;
    annotateInvestigationEnvelope(&response,
                                  report.revisionId,
                                  false,
                                  "session_report",
                                  "session_overview");
    return response;
}

SessionReportRecord Server::persistSessionReport(const QuerySnapshot& snapshot,
                                                 std::uint64_t revisionId,
                                                 std::uint64_t fromSessionSeq,
                                                 std::uint64_t toSessionSeq,
                                                 const QueryResponse& response) {
    std::lock_guard<std::mutex> persistLock(reportPersistMutex_);

    {
        std::lock_guard<std::mutex> lock(stateMutex_);
        for (auto it = sessionReports_.rbegin(); it != sessionReports_.rend(); ++it) {
            if (it->revisionId == revisionId &&
                it->fromSessionSeq == fromSessionSeq &&
                it->toSessionSeq == toSessionSeq) {
                return *it;
            }
        }
    }

    SessionReportRecord record;
    {
        std::lock_guard<std::mutex> lock(stateMutex_);
        record.reportId = nextSessionReportId_++;
    }
    record.revisionId = revisionId;
    record.fromSessionSeq = fromSessionSeq;
    record.toSessionSeq = toSessionSeq;
    record.createdTsEngineNs = nowEngineNs();
    record.incidentCount = response.summary.value("incident_count", static_cast<std::size_t>(0));
    record.instrumentId = snapshot.instrumentId;
    record.headline = response.summary.value("report_summary", json::object()).value("headline", std::string("Session report"));

    std::ostringstream fileName;
    fileName << "session-report-" << std::setw(6) << std::setfill('0') << record.reportId << ".msgpack";
    record.fileName = fileName.str();

    QueryResponse payloadResponse = response;
    payloadResponse.summary["session_report"] = sessionReportToJson(record);
    payloadResponse.summary["is_durable_report"] = true;
    const std::vector<std::uint8_t> payload = json::to_msgpack(queryResponseToJson(payloadResponse));
    record.payloadSha256 = sha256Hex(payload);

    const std::filesystem::path reportPath = snapshot.dataDir / "reports" / record.fileName;
    {
        std::ofstream out(reportPath, std::ios::binary);
        if (!out.is_open()) {
            throw std::runtime_error("failed to open session report artifact for write");
        }
        out.write(reinterpret_cast<const char*>(payload.data()), static_cast<std::streamsize>(payload.size()));
    }

    const std::filesystem::path manifestPath = snapshot.dataDir / "session-reports.jsonl";
    {
        std::ofstream manifestOut(manifestPath, std::ios::app);
        if (!manifestOut.is_open()) {
            throw std::runtime_error("failed to open tape-engine session report manifest for append");
        }
        manifestOut << sessionReportToJson(record).dump() << '\n';
    }

    ArtifactLookupIndex lookupIndexSnapshot;
    {
        std::lock_guard<std::mutex> lock(stateMutex_);
        sessionReports_.push_back(record);
        upsertArtifactLookupIndexUnlocked(record);
        lookupIndexSnapshot = artifactLookupIndex_;
    }
    {
        std::lock_guard<std::mutex> lock(segmentCacheMutex_);
        reportResponseCache_[record.fileName] = payloadResponse;
    }
    std::string lookupError;
    if (!persistArtifactLookupIndex(lookupIndexSnapshot, &lookupError)) {
        throw std::runtime_error(lookupError);
    }
    return record;
}

std::optional<CaseReportRecord> Server::findCaseReport(const QuerySnapshot& snapshot,
                                                       std::uint64_t reportId) const {
    const auto found = snapshot.caseReportsById.find(reportId);
    if (found != snapshot.caseReportsById.end()) {
        return found->second;
    }
    return std::nullopt;
}

std::optional<CaseReportRecord> Server::findOrderCaseReport(const QuerySnapshot& snapshot,
                                                            std::uint64_t revisionId,
                                                            const BridgeAnchorIdentity& anchor) const {
    for (auto it = snapshot.caseReports.rbegin(); it != snapshot.caseReports.rend(); ++it) {
        if (it->reportType == "order_case" &&
            it->revisionId == revisionId &&
            sameAnchorIdentity(it->anchor, anchor)) {
            return *it;
        }
    }
    return std::nullopt;
}

std::optional<CaseReportRecord> Server::findIncidentCaseReport(const QuerySnapshot& snapshot,
                                                               std::uint64_t revisionId,
                                                               std::uint64_t logicalIncidentId) const {
    for (auto it = snapshot.caseReports.rbegin(); it != snapshot.caseReports.rend(); ++it) {
        if (it->reportType == "incident_case" &&
            it->revisionId == revisionId &&
            it->logicalIncidentId == logicalIncidentId) {
            return *it;
        }
    }
    return std::nullopt;
}

QueryResponse Server::loadCaseReportResponse(const QuerySnapshot& snapshot,
                                             const CaseReportRecord& report) const {
    {
        std::lock_guard<std::mutex> lock(segmentCacheMutex_);
        const auto found = reportResponseCache_.find(report.fileName);
        if (found != reportResponseCache_.end()) {
            QueryResponse cached = found->second;
            cached.summary["case_report_artifact"] = caseReportToJson(report);
            cached.summary["artifact"] = {
                {"artifact_id", caseReportArtifactId(report.reportId)},
                {"artifact_type", "case_report"},
                {"first_session_seq", report.firstSessionSeq},
                {"last_session_seq", report.lastSessionSeq},
                {"revision_id", report.revisionId}
            };
            cached.summary["is_durable_report"] = true;
            annotateInvestigationEnvelope(&cached,
                                          report.revisionId,
                                          false,
                                          "case_report",
                                          report.reportType);
            return cached;
        }
    }

    const std::filesystem::path reportPath = snapshot.dataDir / "reports" / report.fileName;
    std::ifstream in(reportPath, std::ios::binary);
    if (!in.is_open()) {
        throw std::runtime_error("failed to open case report artifact");
    }

    const std::vector<std::uint8_t> bytes((std::istreambuf_iterator<char>(in)),
                                          std::istreambuf_iterator<char>());
    if (bytes.empty()) {
        throw std::runtime_error("case report artifact is empty");
    }

    QueryResponse response = queryResponseFromJson(json::from_msgpack(bytes, true, false));
    {
        std::lock_guard<std::mutex> lock(segmentCacheMutex_);
        reportResponseCache_[report.fileName] = response;
    }
    response.summary["case_report_artifact"] = caseReportToJson(report);
    response.summary["artifact"] = {
        {"artifact_id", caseReportArtifactId(report.reportId)},
        {"artifact_type", "case_report"},
        {"first_session_seq", report.firstSessionSeq},
        {"last_session_seq", report.lastSessionSeq},
        {"revision_id", report.revisionId}
    };
    response.summary["is_durable_report"] = true;
    annotateInvestigationEnvelope(&response,
                                  report.revisionId,
                                  false,
                                  "case_report",
                                  report.reportType);
    return response;
}

CaseReportRecord Server::persistCaseReport(const QuerySnapshot& snapshot,
                                           std::uint64_t revisionId,
                                           const std::string& reportType,
                                           std::uint64_t logicalIncidentId,
                                           const BridgeAnchorIdentity& anchor,
                                           std::uint64_t firstSessionSeq,
                                           std::uint64_t lastSessionSeq,
                                           const QueryResponse& response) {
    std::lock_guard<std::mutex> persistLock(reportPersistMutex_);

    {
        std::lock_guard<std::mutex> lock(stateMutex_);
        for (auto it = caseReports_.rbegin(); it != caseReports_.rend(); ++it) {
            if (it->reportType == reportType &&
                it->revisionId == revisionId &&
                it->logicalIncidentId == logicalIncidentId &&
                sameAnchorIdentity(it->anchor, anchor)) {
                return *it;
            }
        }
    }

    CaseReportRecord record;
    {
        std::lock_guard<std::mutex> lock(stateMutex_);
        record.reportId = nextCaseReportId_++;
    }
    record.revisionId = revisionId;
    record.reportType = reportType;
    record.logicalIncidentId = logicalIncidentId;
    record.anchor = anchor;
    record.firstSessionSeq = firstSessionSeq;
    record.lastSessionSeq = lastSessionSeq;
    record.createdTsEngineNs = nowEngineNs();
    record.instrumentId = response.summary.value("latest_incident", json::object()).value("instrument_id",
                        response.summary.value("anchor", json::object()).value("instrument_id", snapshot.instrumentId));
    record.headline = response.summary.value("report_summary", json::object()).value("headline",
                      response.summary.value("case_report", json::object()).value("headline", std::string("Case report")));

    std::ostringstream fileName;
    fileName << "case-report-" << std::setw(6) << std::setfill('0') << record.reportId << ".msgpack";
    record.fileName = fileName.str();

    QueryResponse payloadResponse = response;
    payloadResponse.summary["case_report_artifact"] = caseReportToJson(record);
    payloadResponse.summary["is_durable_report"] = true;
    const std::vector<std::uint8_t> payload = json::to_msgpack(queryResponseToJson(payloadResponse));
    record.payloadSha256 = sha256Hex(payload);

    const std::filesystem::path reportPath = snapshot.dataDir / "reports" / record.fileName;
    {
        std::ofstream out(reportPath, std::ios::binary);
        if (!out.is_open()) {
            throw std::runtime_error("failed to open case report artifact for write");
        }
        out.write(reinterpret_cast<const char*>(payload.data()), static_cast<std::streamsize>(payload.size()));
    }

    const std::filesystem::path manifestPath = snapshot.dataDir / "case-reports.jsonl";
    {
        std::ofstream manifestOut(manifestPath, std::ios::app);
        if (!manifestOut.is_open()) {
            throw std::runtime_error("failed to open tape-engine case report manifest for append");
        }
        manifestOut << caseReportToJson(record).dump() << '\n';
    }

    ArtifactLookupIndex lookupIndexSnapshot;
    {
        std::lock_guard<std::mutex> lock(stateMutex_);
        caseReports_.push_back(record);
        upsertArtifactLookupIndexUnlocked(record);
        lookupIndexSnapshot = artifactLookupIndex_;
    }
    {
        std::lock_guard<std::mutex> lock(segmentCacheMutex_);
        reportResponseCache_[record.fileName] = payloadResponse;
    }
    std::string lookupError;
    if (!persistArtifactLookupIndex(lookupIndexSnapshot, &lookupError)) {
        throw std::runtime_error(lookupError);
    }
    return record;
}

bool Server::restoreImportedCasesManifest(const std::filesystem::path& path, std::string* error) {
    std::ifstream in(path);
    if (!in.is_open()) {
        if (error != nullptr) {
            *error = "failed to open tape-engine imported case manifest for restore";
        }
        return false;
    }

    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) {
            continue;
        }

        const json entry = json::parse(line, nullptr, false);
        if (entry.is_discarded()) {
            if (error != nullptr) {
                *error = "failed to parse tape-engine imported case manifest during restore";
            }
            return false;
        }

        ImportedCaseRecord record = importedCaseFromJson(entry);
        importedCases_.push_back(record);
        nextImportedCaseId_ = std::max(nextImportedCaseId_, record.importedCaseId + 1);
    }

    return true;
}

bool Server::persistImportedCasesManifest(const std::vector<ImportedCaseRecord>& records, std::string* error) const {
    const std::filesystem::path manifestPath = config_.dataDir / "imported-case-bundles.jsonl";
    if (records.empty()) {
        std::error_code ec;
        std::filesystem::remove(manifestPath, ec);
        return true;
    }
    std::string manifest = importedCasesManifestContents(records);
    if (!writeTextFileAtomically(manifestPath, manifest, error)) {
        if (error != nullptr && error->rfind("failed to ", 0) != 0) {
            *error = "failed to persist imported case bundle manifest: " + *error;
        } else if (error != nullptr && error->empty()) {
            *error = "failed to persist imported case bundle manifest";
        }
        return false;
    }
    return true;
}

std::optional<ImportedCaseRecord> Server::findImportedCaseByPayloadSha(const QuerySnapshot& snapshot,
                                                                       const std::string& payloadSha256) const {
    if (payloadSha256.empty()) {
        return std::nullopt;
    }
    for (auto it = snapshot.importedCases.rbegin(); it != snapshot.importedCases.rend(); ++it) {
        if (it->payloadSha256 == payloadSha256) {
            return *it;
        }
    }
    return std::nullopt;
}

ImportedCaseRecord Server::persistImportedCaseRecord(const QuerySnapshot& snapshot,
                                                     const json& bundle,
                                                     const std::filesystem::path& sourcePath,
                                                     const std::vector<std::uint8_t>& bytes) {
    std::lock_guard<std::mutex> persistLock(reportPersistMutex_);

    const json sourceArtifact = bundle.value("source_artifact", json::object());
    const json sourceReport = bundle.value("source_report", json::object());
    const json reportBundle = bundle.value("report_bundle", json::object());
    const json reportSummary = reportBundle.value("summary", json::object())
        .value("report", reportBundle.value("summary", json::object()).value("report_summary", json::object()));

    ImportedCaseRecord record;
    {
        std::lock_guard<std::mutex> lock(stateMutex_);
        record.importedCaseId = nextImportedCaseId_;
    }
    record.importedTsEngineNs = nowEngineNs();
    record.bundleId = bundle.value("bundle_id", std::string());
    record.bundleType = bundle.value("bundle_type", std::string());
    record.sourceArtifactId = bundle.value("source_artifact_id",
                                           sourceArtifact.value("artifact_id", std::string()));
    record.sourceReportId = bundle.value("source_report_id",
                                         sourceReport.value("report_id", 0ULL));
    record.sourceRevisionId = bundle.value("source_revision_id",
                                           sourceReport.value("revision_id",
                                                              sourceArtifact.value("revision_id", 0ULL)));
    record.firstSessionSeq = bundle.value("first_session_seq",
                                          sourceArtifact.value("first_session_seq",
                                                               sourceArtifact.value("from_session_seq", 0ULL)));
    record.lastSessionSeq = bundle.value("last_session_seq",
                                         sourceArtifact.value("last_session_seq",
                                                              sourceArtifact.value("to_session_seq", 0ULL)));
    record.instrumentId = bundle.value("instrument_id",
                                       reportBundle.value("summary", json::object()).value("instrument_id",
                                                                                           snapshot.instrumentId));
    record.headline = bundle.value("headline",
                                   reportSummary.value("headline", std::string("Imported case bundle")));
    record.sourceBundlePath = sourcePath.string();
    record.payloadSha256 = sha256Hex(bytes);

    std::ostringstream fileName;
    fileName << "imported-case-" << std::setw(6) << std::setfill('0') << record.importedCaseId << ".msgpack";
    record.fileName = fileName.str();

    const std::filesystem::path bundlePath = snapshot.dataDir / "imports" / record.fileName;
    std::string persistError;
    if (!writeBinaryFileAtomically(bundlePath, bytes, &persistError)) {
        throw std::runtime_error("failed to persist imported case bundle: " + persistError);
    }

    std::vector<ImportedCaseRecord> manifestRecords = snapshot.importedCases;
    manifestRecords.push_back(record);
    if (!persistImportedCasesManifest(manifestRecords, &persistError)) {
        std::error_code removeEc;
        std::filesystem::remove(bundlePath, removeEc);
        throw std::runtime_error(persistError);
    }

    ArtifactLookupIndex lookupIndexSnapshot;
    {
        std::lock_guard<std::mutex> lock(stateMutex_);
        lookupIndexSnapshot = artifactLookupIndex_;
    }
    lookupIndexSnapshot.importedCasesById[record.importedCaseId] = record;

    std::string lookupError;
    if (!persistArtifactLookupIndex(lookupIndexSnapshot, &lookupError)) {
        std::string rollbackError;
        const bool manifestRolledBack = persistImportedCasesManifest(snapshot.importedCases, &rollbackError);
        std::error_code removeEc;
        std::filesystem::remove(bundlePath, removeEc);

        if (!manifestRolledBack) {
            throw std::runtime_error(lookupError + "; rollback failed: " + rollbackError);
        }
        throw std::runtime_error(lookupError);
    }

    {
        std::lock_guard<std::mutex> lock(stateMutex_);
        nextImportedCaseId_ = std::max(nextImportedCaseId_, record.importedCaseId + 1);
        importedCases_.push_back(record);
        artifactLookupIndex_ = lookupIndexSnapshot;
    }
    return record;
}

std::vector<json> Server::loadEvents(const QuerySnapshot& snapshot,
                                     std::uint64_t frozenRevisionId,
                                     std::uint64_t fromSessionSeq,
                                     std::uint64_t throughSessionSeq,
                                     const std::unordered_set<std::string>* selectorFilter) const {
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
        if (selectorFilter != nullptr && !selectorFilter->empty()) {
            const SegmentArtifactIndex index = loadSegmentArtifactIndex(snapshot, segment);
            bool selectorOverlap = false;
            for (const auto& key : *selectorFilter) {
                if (index.eventAnchorSelectors.count(key) > 0) {
                    selectorOverlap = true;
                    break;
                }
            }
            if (!selectorOverlap) {
                continue;
            }
        }

        const std::filesystem::path segmentPath = snapshot.dataDir / "segments" / segment.fileName;
        std::vector<json> segmentEvents;
        bool cached = false;
        {
            std::lock_guard<std::mutex> lock(segmentCacheMutex_);
            const auto found = segmentEventCache_.find(segment.fileName);
            if (found != segmentEventCache_.end()) {
                segmentEvents = found->second;
                cached = true;
            }
        }

        if (!cached) {
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
                            segmentEvents.push_back(std::move(entry));
                        }
                    }
                }
            } else {
                std::string line;
                while (std::getline(in, line)) {
                    if (line.empty()) {
                        continue;
                    }
                    json parsed = json::parse(line, nullptr, false);
                    if (!parsed.is_discarded()) {
                        segmentEvents.push_back(std::move(parsed));
                    }
                }
            }
            std::lock_guard<std::mutex> lock(segmentCacheMutex_);
            segmentEventCache_[segment.fileName] = segmentEvents;
        }

        for (const auto& event : segmentEvents) {
            const std::uint64_t sessionSeq = event.value("session_seq", 0ULL);
            if (fromSessionSeq > 0 && sessionSeq < fromSessionSeq) {
                continue;
            }
            if (throughSessionSeq > 0 && sessionSeq > throughSessionSeq) {
                continue;
            }
            if (selectorFilter != nullptr && !selectorFilter->empty()) {
                const BridgeAnchorIdentity anchor = anchorFromJson(event.value("anchor", json::object()));
                if (!selectorKeySetContainsAnchor(*selectorFilter, anchor)) {
                    continue;
                }
            }
            events.push_back(event);
        }
    }
    return events;
}

std::vector<json> Server::mergedEvents(const QuerySnapshot& snapshot,
                                       std::uint64_t frozenRevisionId,
                                       bool includeLiveTail,
                                       std::uint64_t fromSessionSeq,
                                       std::uint64_t throughSessionSeq,
                                       const std::unordered_set<std::string>* selectorFilter) const {
    std::vector<json> events = loadEvents(snapshot,
                                          frozenRevisionId,
                                          fromSessionSeq,
                                          throughSessionSeq,
                                          selectorFilter);
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
        if (selectorFilter != nullptr &&
            !selectorFilter->empty() &&
            !selectorKeySetContainsAnchor(*selectorFilter, event.bridgeRecord.anchor)) {
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

    const std::unordered_set<std::string> selectorSet(selectorKeys.begin(), selectorKeys.end());
    std::vector<json> results;
    const auto allEvents = mergedEvents(snapshot,
                                        frozenRevisionId,
                                        includeLiveTail,
                                        fromSessionSeq,
                                        throughSessionSeq,
                                        &selectorSet);
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

std::optional<ProtectedWindowRecord> Server::latestIncidentProtectedWindow(const QueryArtifacts& artifacts,
                                                                           std::uint64_t logicalIncidentId) const {
    std::optional<ProtectedWindowRecord> selectedWindow;
    const auto range = artifacts.protectedWindowsByIncident.equal_range(logicalIncidentId);
    for (auto it = range.first; it != range.second; ++it) {
        const auto& record = artifacts.protectedWindows[it->second];
        if (!selectedWindow.has_value() || selectedWindow->revisionId < record.revisionId) {
            selectedWindow = record;
        }
    }
    return selectedWindow;
}

json Server::buildIncidentDataQualitySummary(const QuerySnapshot& snapshot,
                                             const QueryArtifacts& artifacts,
                                             const IncidentRecord& incident,
                                             std::uint64_t frozenRevisionId,
                                             bool includeLiveTail) const {
    std::vector<json> events;
    const auto incidentWindow = latestIncidentProtectedWindow(artifacts, incident.logicalIncidentId);
    if (incidentWindow.has_value()) {
        events = filterEventsByProtectedWindow(snapshot,
                                               artifacts,
                                               incidentWindow->windowId,
                                               64,
                                               frozenRevisionId,
                                               includeLiveTail,
                                               nullptr);
    }
    if (events.empty()) {
        events = mergedEvents(snapshot,
                              frozenRevisionId,
                              includeLiveTail,
                              incident.firstSessionSeq,
                              incident.lastSessionSeq);
    }
    return buildDataQualitySummary(events, includeLiveTail, incident.instrumentId);
}

IncidentRecord Server::applyIncidentDataQualityPenalty(const IncidentRecord& incident,
                                                       const json& dataQuality,
                                                       const std::vector<FindingRecord>& relatedFindings) const {
    IncidentRecord adjusted = incident;
    const double penaltyFactor = incidentQualityPenaltyFactor(dataQuality);
    const double evidenceBreadthFactor = incidentEvidenceBreadthFactor(relatedFindings);
    const double corroborationFactor = incidentCorroborationFactor(relatedFindings);
    adjusted.confidence = std::clamp(adjusted.confidence * penaltyFactor *
                                     std::sqrt(evidenceBreadthFactor * corroborationFactor), 0.0, 1.0);
    adjusted.score *= evidenceBreadthFactor * corroborationFactor * (0.7 + 0.3 * penaltyFactor);
    return adjusted;
}

std::vector<IncidentRecord> Server::collapseAdjustedIncidents(const QuerySnapshot& snapshot,
                                                              const QueryArtifacts& artifacts,
                                                              const std::vector<IncidentRecord>& records,
                                                              std::uint64_t frozenRevisionId,
                                                              bool includeLiveTail) const {
    std::vector<IncidentRecord> adjusted;
    adjusted.reserve(records.size());
    for (const auto& incident : collapseLatestIncidentRevisions(records)) {
        const std::vector<FindingRecord> relatedFindings = relatedIncidentFindings(artifacts, incident.logicalIncidentId);
        adjusted.push_back(applyIncidentDataQualityPenalty(
            incident,
            buildIncidentDataQualitySummary(snapshot, artifacts, incident, frozenRevisionId, includeLiveTail),
            relatedFindings));
    }
    std::sort(adjusted.begin(), adjusted.end(), [](const IncidentRecord& left,
                                                   const IncidentRecord& right) {
        if (left.score != right.score) {
            return left.score > right.score;
        }
        if (left.confidence != right.confidence) {
            return left.confidence > right.confidence;
        }
        if (left.tsEngineNs != right.tsEngineNs) {
            return left.tsEngineNs > right.tsEngineNs;
        }
        return left.logicalIncidentId > right.logicalIncidentId;
    });
    return adjusted;
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
    if (targetSessionSeq == 0) {
        targetSessionSeq = snapshot.nextSessionSeq > 0 ? snapshot.nextSessionSeq - 1 : 0;
    }

    std::optional<ReplayCheckpointRecord> selectedCheckpoint;
    for (const auto& segment : snapshot.segments) {
        if (segment.revisionId > frozenRevisionId || segment.checkpointFileName.empty()) {
            continue;
        }
        const auto checkpoint = loadReplayCheckpoint(snapshot, segment);
        if (!checkpoint.has_value() || checkpoint->sessionSeq > targetSessionSeq) {
            continue;
        }
        if (!selectedCheckpoint.has_value() || selectedCheckpoint->sessionSeq < checkpoint->sessionSeq) {
            selectedCheckpoint = checkpoint;
        }
    }

    ReplayBookState replay;
    if (selectedCheckpoint.has_value()) {
        replay.bidPrice = selectedCheckpoint->bidPrice;
        replay.askPrice = selectedCheckpoint->askPrice;
        replay.lastPrice = selectedCheckpoint->lastPrice;
        replay.bidBook = selectedCheckpoint->bidBook;
        replay.askBook = selectedCheckpoint->askBook;
        replay.replayedThroughSessionSeq = selectedCheckpoint->sessionSeq;
        replay.appliedEvents = selectedCheckpoint->appliedEvents;
        replay.gapMarkers = selectedCheckpoint->gapMarkers;
    }

    const std::uint64_t replayFromSessionSeq = replay.replayedThroughSessionSeq > 0
        ? replay.replayedThroughSessionSeq + 1
        : 0;
    const std::vector<json> replayEvents = mergedEvents(snapshot,
                                                        frozenRevisionId,
                                                        includeLiveTail,
                                                        replayFromSessionSeq,
                                                        targetSessionSeq);
    for (const auto& event : replayEvents) {
        const std::uint64_t sessionSeq = event.value("session_seq", 0ULL);
        if (sessionSeq == 0) {
            continue;
        }
        if (targetSessionSeq > 0 && sessionSeq > targetSessionSeq) {
            break;
        }
        applyEventToReplayState(&replay, event);
    }
    if (targetSessionSeq == 0) {
        targetSessionSeq = replay.replayedThroughSessionSeq;
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
        {"checkpoint_revision_id", selectedCheckpoint.has_value() ? selectedCheckpoint->revisionId : 0ULL},
        {"checkpoint_session_seq", selectedCheckpoint.has_value() ? selectedCheckpoint->sessionSeq : 0ULL},
        {"checkpoint_used", selectedCheckpoint.has_value()},
        {"data_quality", buildDataQualitySummary(replayEvents, includeLiveTail, snapshot.instrumentId)},
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
            const ResolvedInstrumentIdentity identity = resolveInstrumentIdentity(config_, record);
            if (config_.rejectMismatchedStrongInstrumentIds &&
                identity.policy == "coerced_from_mismatch" &&
                isStrongInstrumentId(record.instrumentId)) {
                return rejectAck(batch.header.batchSeq,
                                 batch.header.producer,
                                 batch.header.runtimeSessionId,
                                 "bridge batch contains a mismatched strong instrument_id for symbol " + record.symbol);
            }

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
                reset.instrumentId = identity.resolvedInstrumentId;
                reset.sourceInstrumentId = identity.sourceInstrumentId;
                reset.instrumentIdentitySource = identity.source;
                reset.instrumentIdentityPolicy = identity.policy;
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
                gap.instrumentId = identity.resolvedInstrumentId;
                gap.sourceInstrumentId = identity.sourceInstrumentId;
                gap.instrumentIdentitySource = identity.source;
                gap.instrumentIdentityPolicy = identity.policy;
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
            event.instrumentId = identity.resolvedInstrumentId;
            event.sourceInstrumentId = identity.sourceInstrumentId;
            event.instrumentIdentitySource = identity.source;
            event.instrumentIdentityPolicy = identity.policy;
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
    response.operation = request.operation.empty()
        ? std::string(queryOperationName(request.operationKind))
        : request.operation;
    response.status = "error";
    response.error = error;
    response.summary = {
        {"api", buildInvestigationApiSummary(response, request.revisionId, request.includeLiveTail, "error")},
        {"includes_mutable_tail", request.includeLiveTail},
        {"served_revision_id", request.revisionId}
    };
    return response;
}

QueryResponse Server::processQueryFrame(const std::vector<std::uint8_t>& frame) {
    QueryRequest request = decodeQueryRequestFrame(frame);
    if (request.operationKind == QueryOperation::Unknown) {
        request.operationKind = queryOperationFromString(request.operation);
    }
    if (request.operationKind != QueryOperation::Unknown) {
        request.operation = queryOperationName(request.operationKind);
    } else {
        request.operation = canonicalizeQueryOperationName(request.operation);
    }
    QueryResponse response;
    response.requestId = request.requestId;
    response.operation = request.operation;
    const QueryOperation operation = request.operationKind;

    const std::size_t writerBacklog = [&]() {
        std::lock_guard<std::mutex> lock(writerMutex_);
        return writerQueue_.size();
    }();
    const QuerySnapshot snapshot = captureQuerySnapshot();
    if (operation == QueryOperation::Status) {
        response.summary = {
            {"data_dir", snapshot.dataDir.string()},
            {"instrument_id", snapshot.instrumentId},
            {"last_manifest_hash", snapshot.lastManifestHash},
            {"latest_frozen_revision_id", snapshot.latestFrozenRevisionId},
            {"latest_frozen_session_seq", snapshot.latestFrozenSessionSeq},
            {"latest_session_seq", snapshot.nextSessionSeq > 0 ? snapshot.nextSessionSeq - 1 : 0},
            {"live_event_count", snapshot.liveEvents.size()},
            {"case_report_count", snapshot.caseReports.size()},
            {"imported_case_count", snapshot.importedCases.size()},
            {"next_revision_id", snapshot.nextRevisionId},
            {"next_case_report_id", snapshot.caseReports.empty() ? 1ULL : snapshot.caseReports.back().reportId + 1},
            {"next_imported_case_id", snapshot.importedCases.empty() ? 1ULL : snapshot.importedCases.back().importedCaseId + 1},
            {"next_session_report_id", snapshot.sessionReports.empty() ? 1ULL : snapshot.sessionReports.back().reportId + 1},
            {"next_segment_id", snapshot.nextSegmentId},
            {"next_session_seq", snapshot.nextSessionSeq},
            {"session_report_count", snapshot.sessionReports.size()},
            {"segment_count", snapshot.segments.size()},
            {"socket_path", snapshot.socketPath},
            {"writer_backlog_segments", writerBacklog}
        };
        if (!snapshot.writerFailure.empty()) {
            response.summary["writer_error"] = snapshot.writerFailure;
        }
        response.result = statusResultToJson(response.summary);
        return response;
    }

    if (operation == QueryOperation::ReadLiveTail) {
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
        response.result = eventListResultToJson(response.summary, response.events);
        return response;
    }

    if (operation == QueryOperation::ReadSessionQuality) {
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
            {"includes_mutable_tail", request.includeLiveTail},
            {"to_session_seq", to},
            {"served_revision_id", frozenRevisionId},
            {"data_quality", buildDataQualitySummary(events, request.includeLiveTail, snapshot.instrumentId)}
        };
        response.result = sessionQualityResultToJson(response.summary);
        return response;
    }

    if (operation == QueryOperation::ReadSessionOverview) {
        std::uint64_t frozenRevisionId = 0;
        try {
            frozenRevisionId = resolveFrozenRevision(snapshot, request.revisionId);
        } catch (const std::exception& error) {
            return rejectResponse(request, error.what());
        }
        return buildSessionOverviewResponse(request, snapshot, frozenRevisionId);
    }

    if (operation == QueryOperation::ScanSessionReport) {
        if (request.includeLiveTail) {
            return rejectResponse(request, "scan_session_report only supports frozen revision evidence");
        }

        std::uint64_t frozenRevisionId = 0;
        try {
            frozenRevisionId = resolveFrozenRevision(snapshot, request.revisionId);
        } catch (const std::exception& error) {
            return rejectResponse(request, error.what());
        }

        QueryRequest overviewRequest = request;
        overviewRequest.operation = queryOperationName(QueryOperation::ScanSessionReport);
        overviewRequest.operationKind = QueryOperation::ScanSessionReport;
        QueryResponse reportResponse = buildSessionOverviewResponse(overviewRequest, snapshot, frozenRevisionId);
        const std::uint64_t from = reportResponse.summary.value("from_session_seq", 0ULL);
        const std::uint64_t to = reportResponse.summary.value("to_session_seq", 0ULL);
        const SessionReportRecord report = persistSessionReport(snapshot, frozenRevisionId, from, to, reportResponse);
        reportResponse.summary["source_artifact"] = reportResponse.summary.value("artifact", json::object());
        reportResponse.summary["session_report"] = sessionReportToJson(report);
        reportResponse.summary["artifact"] = {
            {"artifact_id", sessionReportArtifactId(report.reportId)},
            {"artifact_type", "session_report"},
            {"artifact_scope", "durable"},
            {"from_session_seq", report.fromSessionSeq},
            {"revision_id", report.revisionId},
            {"to_session_seq", report.toSessionSeq}
        };
        reportResponse.summary["is_durable_report"] = true;
        reportResponse.summary["scan_status"] = "persisted";
        annotateInvestigationEnvelope(&reportResponse,
                                      frozenRevisionId,
                                      false,
                                      "session_report",
                                      "session_overview");
        return reportResponse;
    }

    if (operation == QueryOperation::ReadSessionReport) {
        std::optional<SessionReportRecord> report;
        if (request.reportId > 0) {
            const auto found = snapshot.sessionReportsById.find(request.reportId);
            if (found != snapshot.sessionReportsById.end()) {
                report = found->second;
            }
        } else {
            std::uint64_t frozenRevisionId = 0;
            try {
                frozenRevisionId = resolveFrozenRevision(snapshot, request.revisionId);
            } catch (const std::exception& error) {
                return rejectResponse(request, error.what());
            }
            const std::uint64_t from = request.fromSessionSeq;
            const std::uint64_t to = request.toSessionSeq == 0
                ? (snapshot.latestFrozenSessionSeq == 0 ? 0 : snapshot.latestFrozenSessionSeq)
                : request.toSessionSeq;
            report = findSessionReport(snapshot, frozenRevisionId, from, to);
        }
        if (!report.has_value()) {
            return rejectResponse(request, "session report not found");
        }
        try {
            QueryResponse stored = loadSessionReportResponse(snapshot, *report);
            stored.requestId = request.requestId;
            stored.operation = request.operation;
            annotateInvestigationEnvelope(&stored,
                                          stored.summary.value("served_revision_id", report->revisionId),
                                          false,
                                          "session_report",
                                          "session_overview");
            return stored;
        } catch (const std::exception& error) {
            return rejectResponse(request, error.what());
        }
    }

    if (operation == QueryOperation::ListSessionReports) {
        response.events = json::array();
        const std::size_t limit = request.limit == 0 ? 20 : request.limit;
        for (auto it = snapshot.sessionReports.rbegin();
             it != snapshot.sessionReports.rend() && response.events.size() < limit;
             ++it) {
            if (request.revisionId > 0 && it->revisionId != request.revisionId) {
                continue;
            }
            response.events.push_back(sessionReportToJson(*it));
        }
        response.summary = {
            {"returned_events", response.events.size()},
            {"session_report_count", snapshot.sessionReports.size()}
        };
        response.result = collectionResultToJson(response.events,
                                                 "session_reports",
                                                 request.revisionId,
                                                 false,
                                                 snapshot.sessionReports.size());
        return response;
    }

    if (operation == QueryOperation::ReadCaseReport) {
        std::optional<CaseReportRecord> report = findCaseReport(snapshot, request.reportId);
        if (!report.has_value()) {
            return rejectResponse(request, "case report not found");
        }
        try {
            QueryResponse stored = loadCaseReportResponse(snapshot, *report);
            stored.requestId = request.requestId;
            stored.operation = request.operation;
            annotateInvestigationEnvelope(&stored,
                                          stored.summary.value("served_revision_id", report->revisionId),
                                          false,
                                          "case_report",
                                          report->reportType);
            return stored;
        } catch (const std::exception& error) {
            return rejectResponse(request, error.what());
        }
    }

    if (operation == QueryOperation::ListCaseReports) {
        response.events = json::array();
        const std::size_t limit = request.limit == 0 ? 20 : request.limit;
        for (auto it = snapshot.caseReports.rbegin();
             it != snapshot.caseReports.rend() && response.events.size() < limit;
             ++it) {
            if (request.revisionId > 0 && it->revisionId != request.revisionId) {
                continue;
            }
            response.events.push_back(caseReportToJson(*it));
        }
        response.summary = {
            {"returned_events", response.events.size()},
            {"case_report_count", snapshot.caseReports.size()}
        };
        response.result = collectionResultToJson(response.events,
                                                 "case_reports",
                                                 request.revisionId,
                                                 false,
                                                 snapshot.caseReports.size());
        return response;
    }

    if (operation == QueryOperation::ExportSessionBundle ||
        operation == QueryOperation::ExportCaseBundle) {
        if (request.reportId == 0) {
            return rejectResponse(request, "report_id is required");
        }

        const bool isSessionBundle = operation == QueryOperation::ExportSessionBundle;
        QueryResponse sourceResponse;
        json sourceReport = json::object();
        json sourceArtifact = json::object();
        std::string bundleId;
        std::string bundleType;
        std::string filePrefix;
        std::uint64_t sourceRevisionId = 0;
        std::uint64_t firstSessionSeq = 0;
        std::uint64_t lastSessionSeq = 0;
        std::string instrumentId = snapshot.instrumentId;
        std::string headline;

        try {
            if (isSessionBundle) {
                const auto found = snapshot.sessionReportsById.find(request.reportId);
                if (found == snapshot.sessionReportsById.end()) {
                    return rejectResponse(request, "session report not found");
                }
                sourceResponse = loadSessionReportResponse(snapshot, found->second);
                sourceReport = sessionReportToJson(found->second);
                sourceArtifact = sourceResponse.summary.value("artifact", json::object());
                bundleId = sessionBundleArtifactId(found->second.reportId);
                bundleType = "session_bundle";
                filePrefix = "session-bundle-report-";
                sourceRevisionId = found->second.revisionId;
                firstSessionSeq = found->second.fromSessionSeq;
                lastSessionSeq = found->second.toSessionSeq;
                instrumentId = found->second.instrumentId;
                headline = found->second.headline;
            } else {
                const auto found = snapshot.caseReportsById.find(request.reportId);
                if (found == snapshot.caseReportsById.end()) {
                    return rejectResponse(request, "case report not found");
                }
                sourceResponse = loadCaseReportResponse(snapshot, found->second);
                sourceReport = caseReportToJson(found->second);
                sourceArtifact = sourceResponse.summary.value("artifact", json::object());
                bundleId = caseBundleArtifactId(found->second.reportId);
                bundleType = "case_bundle";
                filePrefix = "case-bundle-report-";
                sourceRevisionId = found->second.revisionId;
                firstSessionSeq = found->second.firstSessionSeq;
                lastSessionSeq = found->second.lastSessionSeq;
                instrumentId = found->second.instrumentId;
                headline = found->second.headline;
            }
        } catch (const std::exception& error) {
            return rejectResponse(request, error.what());
        }

        const json reportSummary = sourceResponse.summary.value("report",
            sourceResponse.summary.value("report_summary", json::object()));
        if (headline.empty()) {
            headline = reportSummary.value("headline", std::string("Tape engine bundle"));
        }
        if (instrumentId.empty()) {
            instrumentId = sourceResponse.summary.value("entity", json::object()).value("instrument_id",
                         sourceResponse.summary.value("instrument_id", snapshot.instrumentId));
        }

        json bundle{
            {"schema", kReportBundleSchema},
            {"version", kReportBundleVersion},
            {"bundle_id", bundleId},
            {"bundle_type", bundleType},
            {"exported_ts_engine_ns", nowEngineNs()},
            {"source_artifact_id", sourceArtifact.value("artifact_id", std::string())},
            {"source_report_id", request.reportId},
            {"source_revision_id", sourceRevisionId},
            {"first_session_seq", firstSessionSeq},
            {"last_session_seq", lastSessionSeq},
            {"instrument_id", instrumentId},
            {"headline", headline},
            {"source_artifact", sourceArtifact},
            {"source_report", sourceReport},
            {"report_markdown", renderReportMarkdown(sourceResponse)},
            {"report_bundle", queryResponseToJson(sourceResponse)}
        };

        std::ostringstream fileName;
        fileName << filePrefix << std::setw(6) << std::setfill('0') << request.reportId << ".msgpack";
        const std::filesystem::path bundlePath = snapshot.dataDir / "bundles" / fileName.str();
        const std::vector<std::uint8_t> payload = json::to_msgpack(bundle);
        std::string writeError;
        if (!writeBinaryFileAtomically(bundlePath, payload, &writeError)) {
            return rejectResponse(request, "failed to write report bundle: " + writeError);
        }

        response.summary = {
            {"artifact", {
                {"artifact_id", bundleId},
                {"artifact_type", bundleType},
                {"artifact_scope", "portable"},
                {"first_session_seq", firstSessionSeq},
                {"last_session_seq", lastSessionSeq},
                {"revision_id", sourceRevisionId}
            }},
            {"bundle", {
                {"bundle_id", bundleId},
                {"bundle_type", bundleType},
                {"bundle_path", bundlePath.string()},
                {"file_name", fileName.str()},
                {"payload_sha256", sha256Hex(payload)},
                {"schema", kReportBundleSchema},
                {"version", kReportBundleVersion}
            }},
            {"source_artifact", sourceArtifact},
            {"source_report", sourceReport},
            {"served_revision_id", sourceRevisionId},
            {"export_status", "written"}
        };
        response.result = bundleExportResultToJson(response.summary.value("artifact", json::object()),
                                                   response.summary.value("bundle", json::object()),
                                                   sourceArtifact,
                                                   sourceReport,
                                                   sourceRevisionId,
                                                   "written");
        response.events = json::array();
        return response;
    }

    if (operation == QueryOperation::VerifyBundle) {
        if (request.bundlePath.empty()) {
            return rejectResponse(request, "bundle_path is required");
        }

        tape_bundle::PortableBundleInspection inspection;
        std::string inspectError;
        if (!tape_bundle::inspectPortableBundle(request.bundlePath, &inspection, &inspectError)) {
            return rejectResponse(request, inspectError);
        }

        const bool importSupported = (inspection.bundleType == "case_bundle");
        const auto existing = findImportedCaseByPayloadSha(snapshot, inspection.payloadSha256);
        const bool alreadyImported = existing.has_value();

        std::string verifyStatus = "valid";
        std::string importReason = importSupported
            ? (alreadyImported ? "payload_already_imported" : "case_bundle_can_be_imported")
            : "bundle_type_not_importable";
        if (inspection.bundleType != "session_bundle" && inspection.bundleType != "case_bundle") {
            verifyStatus = "unknown_bundle_type";
        }

        response.summary = {
            {"artifact", {
                {"artifact_id", inspection.bundleId},
                {"artifact_type", inspection.bundleType},
                {"artifact_scope", "portable"},
                {"first_session_seq", inspection.firstSessionSeq},
                {"last_session_seq", inspection.lastSessionSeq},
                {"revision_id", inspection.sourceRevisionId}
            }},
            {"bundle", {
                {"bundle_id", inspection.bundleId},
                {"bundle_type", inspection.bundleType},
                {"bundle_path", inspection.bundlePath.string()},
                {"file_name", inspection.fileName},
                {"payload_sha256", inspection.payloadSha256},
                {"schema", inspection.bundle.value("schema", std::string())},
                {"version", inspection.bundle.value("version", 0U)}
            }},
            {"source_artifact", inspection.sourceArtifact},
            {"source_report", inspection.sourceReport},
            {"report_summary", inspection.reportSummary},
            {"report_markdown", inspection.reportMarkdown},
            {"verify_status", verifyStatus},
            {"import_supported", importSupported},
            {"already_imported", alreadyImported},
            {"can_import", importSupported && !alreadyImported},
            {"import_reason", importReason},
            {"served_revision_id", inspection.sourceRevisionId}
        };
        if (alreadyImported) {
            response.summary["imported_case"] = importedCaseToJson(*existing);
        }
        response.result = bundleVerifyResultToJson(response.summary.value("artifact", json::object()),
                                                   response.summary.value("bundle", json::object()),
                                                   inspection.sourceArtifact,
                                                   inspection.sourceReport,
                                                   inspection.reportSummary,
                                                   inspection.reportMarkdown,
                                                   verifyStatus,
                                                   importSupported,
                                                   alreadyImported,
                                                   importSupported && !alreadyImported,
                                                   importReason,
                                                   inspection.sourceRevisionId,
                                                   existing);
        response.events = json::array();
        return response;
    }

    if (operation == QueryOperation::ImportCaseBundle) {
        if (request.bundlePath.empty()) {
            return rejectResponse(request, "bundle_path is required");
        }

        tape_bundle::PortableBundleInspection inspection;
        std::string inspectError;
        if (!tape_bundle::inspectPortableBundle(request.bundlePath, &inspection, &inspectError)) {
            return rejectResponse(request, inspectError);
        }
        if (inspection.bundleType != "case_bundle") {
            return rejectResponse(request, "import_case_bundle only supports case_bundle payloads");
        }
        if (!inspection.reportBundle.is_object()) {
            return rejectResponse(request, "case bundle is missing report_bundle payload");
        }

        if (const auto existing = findImportedCaseByPayloadSha(snapshot, inspection.payloadSha256); existing.has_value()) {
            response.summary = {
                {"artifact", {
                    {"artifact_id", importedCaseArtifactId(existing->importedCaseId)},
                    {"artifact_type", "imported_case_bundle"},
                    {"artifact_scope", "imported"},
                    {"first_session_seq", existing->firstSessionSeq},
                    {"last_session_seq", existing->lastSessionSeq},
                    {"revision_id", existing->sourceRevisionId}
                }},
                {"import_status", "existing"},
                {"duplicate_import", true},
                {"imported_case", importedCaseToJson(*existing)}
            };
            response.result = caseBundleImportResultToJson(response.summary.value("artifact", json::object()),
                                                           *existing,
                                                           "existing",
                                                           true);
            response.events = json::array();
            return response;
        }

        ImportedCaseRecord record;
        try {
            record = persistImportedCaseRecord(snapshot, inspection.bundle, inspection.bundlePath, inspection.bytes);
        } catch (const std::exception& error) {
            return rejectResponse(request, error.what());
        }

        response.summary = {
            {"artifact", {
                {"artifact_id", importedCaseArtifactId(record.importedCaseId)},
                {"artifact_type", "imported_case_bundle"},
                {"artifact_scope", "imported"},
                {"first_session_seq", record.firstSessionSeq},
                {"last_session_seq", record.lastSessionSeq},
                {"revision_id", record.sourceRevisionId}
            }},
            {"import_status", "imported"},
            {"duplicate_import", false},
            {"imported_case", importedCaseToJson(record)}
        };
        response.result = caseBundleImportResultToJson(response.summary.value("artifact", json::object()),
                                                       record,
                                                       "imported",
                                                       false);
        response.events = json::array();
        return response;
    }

    if (operation == QueryOperation::ListImportedCases) {
        response.events = json::array();
        const std::size_t limit = request.limit == 0 ? 20 : request.limit;
        for (auto it = snapshot.importedCases.rbegin();
             it != snapshot.importedCases.rend() && response.events.size() < limit;
             ++it) {
            response.events.push_back(importedCaseToJson(*it));
        }
        response.summary = {
            {"imported_case_count", snapshot.importedCases.size()},
            {"returned_events", response.events.size()},
            {"schema", kImportedCaseListSchema},
            {"version", kImportedCaseListVersion}
        };
        response.result = importedCaseInventoryResultToJson(response.events, response.events.size());
        return response;
    }

    if (operation == QueryOperation::ReadArtifact || operation == QueryOperation::ExportArtifact) {
        if (request.artifactId.empty()) {
            return rejectResponse(request, "artifact_id is required");
        }
        QueryRequest delegated = request;
        delegated.artifactId.clear();
        delegated.exportFormat.clear();
        std::optional<QueryResponse> importedArtifactResponse;
        if (const auto overview = parseSessionOverviewArtifactId(request.artifactId); overview.has_value()) {
            delegated.operation = queryOperationName(QueryOperation::ReadSessionOverview);
            delegated.operationKind = QueryOperation::ReadSessionOverview;
            delegated.revisionId = overview->revisionId;
            delegated.fromSessionSeq = overview->fromSessionSeq;
            delegated.toSessionSeq = overview->toSessionSeq;
        } else if (const auto orderCase = parseOrderCaseArtifactId(request.artifactId); orderCase.has_value()) {
            delegated.operation = queryOperationName(QueryOperation::ReadOrderCase);
            delegated.operationKind = QueryOperation::ReadOrderCase;
            delegated.traceId = orderCase->traceId;
            delegated.orderId = orderCase->orderId;
            delegated.permId = orderCase->permId;
            delegated.execId = orderCase->execId;
        } else {
            const auto parsed = parseNumericArtifactId(request.artifactId);
            if (!parsed.has_value()) {
                return rejectResponse(request, "artifact_id must use a supported session-overview, order-case, or numeric artifact form");
            }
            if (parsed->first == "session-report") {
                delegated.operation = queryOperationName(QueryOperation::ReadSessionReport);
                delegated.operationKind = QueryOperation::ReadSessionReport;
                delegated.reportId = parsed->second;
            } else if (parsed->first == "case-report") {
                delegated.operation = queryOperationName(QueryOperation::ReadCaseReport);
                delegated.operationKind = QueryOperation::ReadCaseReport;
                delegated.reportId = parsed->second;
            } else if (parsed->first == "incident") {
                delegated.operation = queryOperationName(QueryOperation::ReadIncident);
                delegated.operationKind = QueryOperation::ReadIncident;
                delegated.logicalIncidentId = parsed->second;
            } else if (parsed->first == "window") {
                delegated.operation = queryOperationName(QueryOperation::ReadProtectedWindow);
                delegated.operationKind = QueryOperation::ReadProtectedWindow;
                delegated.windowId = parsed->second;
            } else if (parsed->first == "finding") {
                delegated.operation = queryOperationName(QueryOperation::ReadFinding);
                delegated.operationKind = QueryOperation::ReadFinding;
                delegated.findingId = parsed->second;
            } else if (parsed->first == "anchor") {
                delegated.operation = queryOperationName(QueryOperation::ReadOrderAnchor);
                delegated.operationKind = QueryOperation::ReadOrderAnchor;
                delegated.anchorId = parsed->second;
            } else if (parsed->first == "imported-case") {
                delegated.operation = "read_imported_case_bundle";
                delegated.operationKind = QueryOperation::Unknown;
                const auto found = snapshot.importedCasesById.find(parsed->second);
                if (found == snapshot.importedCasesById.end()) {
                    return rejectResponse(request, "imported case not found");
                }

                const std::filesystem::path bundlePath = snapshot.dataDir / "imports" / found->second.fileName;
                tape_bundle::PortableBundleInspection inspection;
                std::string inspectError;
                if (!tape_bundle::inspectPortableBundle(bundlePath, &inspection, &inspectError) ||
                    inspection.bundleType != "case_bundle") {
                    return rejectResponse(request, "imported case bundle is malformed");
                }
                QueryResponse importedResponse = queryResponseFromJson(inspection.reportBundle);
                importedResponse.summary["source_artifact"] = importedResponse.summary.value("artifact", json::object());
                importedResponse.summary["artifact"] = {
                    {"artifact_id", importedCaseArtifactId(found->second.importedCaseId)},
                    {"artifact_type", "imported_case_bundle"},
                    {"artifact_scope", "imported"},
                    {"first_session_seq", found->second.firstSessionSeq},
                    {"last_session_seq", found->second.lastSessionSeq},
                    {"revision_id", found->second.sourceRevisionId}
                };
                importedResponse.summary["imported_case"] = importedCaseToJson(found->second);
                importedResponse.summary["bundle"] = {
                    {"bundle_id", found->second.bundleId},
                    {"bundle_type", found->second.bundleType},
                    {"bundle_path", bundlePath.string()}
                };
                importedArtifactResponse = std::move(importedResponse);
            } else {
                return rejectResponse(request, "artifact_id type is not supported");
            }
        }

        QueryResponse artifactResponse;
        if (importedArtifactResponse.has_value()) {
            artifactResponse = *importedArtifactResponse;
        } else {
            artifactResponse = processQueryFrame(encodeQueryRequestFrame(delegated));
        }
        artifactResponse.requestId = request.requestId;
        if (operation == QueryOperation::ReadArtifact) {
            artifactResponse.operation = request.operation;
            artifactResponse.summary["resolved_artifact_id"] = request.artifactId;
            artifactResponse.summary["artifact_resolution"] = {
                {"requested_artifact_id", request.artifactId},
                {"resolved_operation", delegated.operation},
                {"resolved_revision_id", artifactResponse.summary.value("served_revision_id", 0ULL)}
            };
            annotateInvestigationEnvelope(&artifactResponse,
                                          artifactResponse.summary.value("served_revision_id", 0ULL),
                                          artifactResponse.summary.value("includes_mutable_tail", request.includeLiveTail),
                                          "artifact_read",
                                          artifactResponse.summary.value("report", json::object()).value("report_type", std::string()));
            return artifactResponse;
        }

        if (request.exportFormat != "markdown" && request.exportFormat != "json-bundle") {
            return rejectResponse(request, "export_artifact requires export_format of markdown or json-bundle");
        }

        QueryResponse exportResponse;
        exportResponse.requestId = request.requestId;
        exportResponse.operation = request.operation;
        exportResponse.summary = {
            {"artifact_id", request.artifactId},
            {"export_format", request.exportFormat},
            {"served_revision_id", artifactResponse.summary.value("served_revision_id", 0ULL)},
            {"artifact_export", {
                {"artifact_id", request.artifactId},
                {"schema", kArtifactExportSchema},
                {"version", kArtifactExportVersion},
                {"format", request.exportFormat}
            }}
        };
        if (request.exportFormat == "markdown") {
            exportResponse.summary["markdown"] = renderReportMarkdown(artifactResponse);
        } else {
            exportResponse.summary["bundle"] = queryResponseToJson(artifactResponse);
        }
        exportResponse.events = json::array();
        exportResponse.summary["api"] = {
            {"operation", exportResponse.operation},
            {"response_kind", "artifact_export"},
            {"served_revision_id", artifactResponse.summary.value("served_revision_id", 0ULL)},
            {"wire_schema", exportResponse.schema},
            {"wire_version", exportResponse.version},
            {"envelope_schema", kArtifactExportSchema},
            {"envelope_version", kArtifactExportVersion},
            {"includes_mutable_tail", artifactResponse.summary.value("includes_mutable_tail", request.includeLiveTail)}
        };
        exportResponse.result = artifactExportResultToJson(exportResponse.summary);
        return exportResponse;
    }

    if (operation == QueryOperation::ListOrderAnchors ||
        operation == QueryOperation::ListProtectedWindows ||
        operation == QueryOperation::ListFindings ||
        operation == QueryOperation::ListIncidents) {
        std::uint64_t frozenRevisionId = 0;
        try {
            frozenRevisionId = resolveFrozenRevision(snapshot, request.revisionId);
        } catch (const std::exception& error) {
            return rejectResponse(request, error.what());
        }

        const std::size_t limit = request.limit == 0 ? 100 : request.limit;
        const QueryArtifacts artifacts = buildQueryArtifacts(snapshot, frozenRevisionId, request.includeLiveTail);
        response.events = json::array();

        if (operation == QueryOperation::ListOrderAnchors) {
            const std::vector<OrderAnchorRecord>& records = artifacts.orderAnchors;
            for (auto it = records.rbegin();
                 it != records.rend() && response.events.size() < limit;
                 ++it) {
                response.events.push_back(orderAnchorToJson(*it));
            }
        } else if (operation == QueryOperation::ListProtectedWindows) {
            const std::vector<ProtectedWindowRecord>& records = artifacts.protectedWindows;
            for (auto it = records.rbegin();
                 it != records.rend() && response.events.size() < limit;
                 ++it) {
                response.events.push_back(protectedWindowToJson(*it));
            }
        } else if (operation == QueryOperation::ListFindings) {
            const std::vector<FindingRecord>& records = artifacts.findings;
            for (auto it = records.rbegin();
                 it != records.rend() && response.events.size() < limit;
                 ++it) {
                response.events.push_back(findingToJson(*it));
            }
        } else if (operation == QueryOperation::ListIncidents) {
            const std::vector<IncidentRecord>& records = artifacts.incidents;
            const std::vector<IncidentRecord> collapsed = collapseAdjustedIncidents(snapshot,
                                                                                   artifacts,
                                                                                   records,
                                                                                   frozenRevisionId,
                                                                                   request.includeLiveTail);
            for (const auto& record : collapsed) {
                if (response.events.size() >= limit) {
                    break;
                }
                response.events.push_back(incidentToJson(record));
            }
        }

        response.summary = {
            {"collapsed_logical_incidents", operation == QueryOperation::ListIncidents},
            {"includes_mutable_tail", request.includeLiveTail},
            {"returned_events", response.events.size()},
            {"served_revision_id", frozenRevisionId}
        };
        response.result = collectionResultToJson(response.events,
                                                 operation == QueryOperation::ListOrderAnchors
                                                     ? "order_anchors"
                                                     : operation == QueryOperation::ListProtectedWindows
                                                         ? "protected_windows"
                                                         : operation == QueryOperation::ListFindings
                                                             ? "findings"
                                                             : "incidents",
                                                 frozenRevisionId,
                                                 request.includeLiveTail,
                                                 operation == QueryOperation::ListOrderAnchors
                                                     ? artifacts.orderAnchors.size()
                                                     : operation == QueryOperation::ListProtectedWindows
                                                         ? artifacts.protectedWindows.size()
                                                         : operation == QueryOperation::ListFindings
                                                             ? artifacts.findings.size()
                                                             : collapseAdjustedIncidents(snapshot,
                                                                                        artifacts,
                                                                                        artifacts.incidents,
                                                                                        frozenRevisionId,
                                                                                        request.includeLiveTail).size());
        return response;
    }

    if (operation == QueryOperation::ReadFinding) {
        if (request.findingId == 0) {
            return rejectResponse(request, "finding_id is required");
        }

        std::uint64_t frozenRevisionId = 0;
        try {
            frozenRevisionId = resolveFrozenRevision(snapshot, request.revisionId);
        } catch (const std::exception& error) {
            return rejectResponse(request, error.what());
        }

        std::optional<FindingRecord> finding;
        const auto directFinding = snapshot.findingsById.find(request.findingId);
        if (directFinding != snapshot.findingsById.end()) {
            finding = directFinding->second;
        }
        if (!finding.has_value()) {
            return rejectResponse(request, "finding_id not found");
        }
        const QueryArtifacts artifacts = buildQueryArtifacts(snapshot, frozenRevisionId, request.includeLiveTail);

        std::optional<IncidentRecord> incident;
        if (finding->logicalIncidentId > 0) {
            const auto range = artifacts.incidentsByLogicalIncident.equal_range(finding->logicalIncidentId);
            for (auto it = range.first; it != range.second; ++it) {
                const auto& record = artifacts.incidents[it->second];
                if (!incident.has_value() || incident->incidentRevisionId < record.incidentRevisionId) {
                    incident = record;
                }
            }
        }
        const auto incidentWindow = incident.has_value()
            ? latestIncidentProtectedWindow(artifacts, incident->logicalIncidentId)
            : std::optional<ProtectedWindowRecord>();

        const std::size_t limit = request.limit == 0 ? 64 : request.limit;
        std::vector<json> events;
        if (incidentWindow.has_value()) {
            events = filterEventsByProtectedWindow(snapshot,
                                                  artifacts,
                                                  incidentWindow->windowId,
                                                  limit,
                                                  frozenRevisionId,
                                                  request.includeLiveTail,
                                                  nullptr);
        } else {
            events = filterEventsByRange(snapshot,
                                         finding->firstSessionSeq,
                                         finding->lastSessionSeq,
                                         limit,
                                         frozenRevisionId,
                                         request.includeLiveTail);
        }

        response.events = json::array();
        for (const auto& event : events) {
            response.events.push_back(event);
        }

        json timeline = json::array();
        for (const auto& event : events) {
            timeline.push_back(timelineEntryFromEvent(event));
        }
        timeline.push_back(timelineEntryFromFinding(*finding));
        if (incident.has_value()) {
            timeline.push_back(timelineEntryFromIncident(*incident));
        }
        timeline = sortAndTrimTimeline(std::move(timeline), 24);
        const json timelineSummary = buildTimelineSummary(timeline);
        const json dataQuality = buildDataQualitySummary(events,
                                                         request.includeLiveTail,
                                                         finding->instrumentId);
        const std::string uncertaintySummary = reportUncertaintySummary(dataQuality,
                                                                        incident.has_value() ? 2 : 1,
                                                                        incident.has_value() ? 2 : 1);
        json citations = json::array();
        citations.push_back(evidenceCitation("finding",
                                             findingArtifactId(finding->findingId),
                                             finding->firstSessionSeq,
                                             finding->lastSessionSeq,
                                             finding->title));
        if (incident.has_value()) {
            citations.push_back(evidenceCitation("incident",
                                                 incidentArtifactId(incident->logicalIncidentId),
                                                 incident->firstSessionSeq,
                                                 incident->lastSessionSeq,
                                                 incident->title));
        }
        if (incidentWindow.has_value()) {
            citations.push_back(evidenceCitation("protected_window",
                                                 protectedWindowArtifactId(incidentWindow->windowId),
                                                 incidentWindow->firstSessionSeq,
                                                 incidentWindow->lastSessionSeq,
                                                 incidentWindow->reason));
        }

        const json reportSummary = {
            {"headline", finding->title},
            {"summary", finding->summary},
            {"why_it_matters", incident.has_value() ? incidentWhyItMatters(*incident) : finding->summary},
            {"timeline_highlights", buildTimelineHighlights(timeline, 3)},
            {"what_changed_first", firstTimelineHeadline(timeline)},
            {"uncertainty", uncertaintySummary}
        };
        response.summary = {
            {"artifact", {
                {"artifact_id", findingArtifactId(finding->findingId)},
                {"artifact_type", "finding"},
                {"first_session_seq", finding->firstSessionSeq},
                {"last_session_seq", finding->lastSessionSeq},
                {"revision_id", frozenRevisionId}
            }},
            {"entity", {
                {"type", "finding"},
                {"finding_id", finding->findingId},
                {"logical_incident_id", finding->logicalIncidentId}
            }},
            {"finding", findingToJson(*finding)},
            {"incident", incident.has_value() ? incidentToJson(*incident) : json::object()},
            {"protected_window", incidentWindow.has_value() ? protectedWindowToJson(*incidentWindow) : json::object()},
            {"evidence", buildEvidenceSection(timeline, timelineSummary, citations, dataQuality)},
            {"report", reportSummary},
            {"report_summary", reportSummary},
            {"timeline", timeline},
            {"timeline_summary", timelineSummary},
            {"data_quality", dataQuality},
            {"returned_events", response.events.size()}
        };
        annotateInvestigationEnvelope(&response,
                                      frozenRevisionId,
                                      request.includeLiveTail,
                                      "finding",
                                      "finding");
        return response;
    }

    if (operation == QueryOperation::ReadOrderAnchor) {
        if (request.anchorId == 0) {
            return rejectResponse(request, "anchor_id is required");
        }

        std::uint64_t frozenRevisionId = 0;
        try {
            frozenRevisionId = resolveFrozenRevision(snapshot, request.revisionId);
        } catch (const std::exception& error) {
            return rejectResponse(request, error.what());
        }

        std::optional<OrderAnchorRecord> anchorRecord;
        const auto directAnchor = snapshot.orderAnchorsById.find(request.anchorId);
        if (directAnchor != snapshot.orderAnchorsById.end()) {
            anchorRecord = directAnchor->second;
        }
        if (!anchorRecord.has_value()) {
            return rejectResponse(request, "anchor_id not found");
        }

        QueryRequest delegated = request;
        delegated.operation = queryOperationName(QueryOperation::ReadOrderCase);
        delegated.operationKind = QueryOperation::ReadOrderCase;
        delegated.traceId = anchorRecord->anchor.traceId;
        delegated.orderId = anchorRecord->anchor.orderId;
        delegated.permId = anchorRecord->anchor.permId;
        delegated.execId = anchorRecord->anchor.execId;
        delegated.anchorId = 0;
        QueryResponse anchorResponse = processQueryFrame(encodeQueryRequestFrame(delegated));
        anchorResponse.requestId = request.requestId;
        anchorResponse.operation = request.operation;
        if (anchorResponse.status != "ok") {
            return anchorResponse;
        }

        json citations = anchorResponse.summary.value("evidence", json::object()).value("citations", json::array());
        citations.insert(citations.begin(),
                         evidenceCitation("order_anchor",
                                          anchorArtifactId(anchorRecord->anchorId),
                                          anchorRecord->sessionSeq,
                                          anchorRecord->sessionSeq,
                                          anchorRecord->eventKind));
        anchorResponse.summary["order_anchor"] = orderAnchorToJson(*anchorRecord);
        anchorResponse.summary["artifact"] = {
            {"artifact_id", anchorArtifactId(anchorRecord->anchorId)},
            {"artifact_type", "order_anchor"},
            {"first_session_seq", anchorRecord->sessionSeq},
            {"last_session_seq", anchorRecord->sessionSeq},
            {"revision_id", frozenRevisionId}
        };
        anchorResponse.summary["entity"] = {
            {"type", "order_anchor"},
            {"anchor_id", anchorRecord->anchorId},
            {"anchor", anchorToJson(anchorRecord->anchor)}
        };
        anchorResponse.summary["evidence"]["citations"] = citations;
        anchorResponse.summary["report"]["headline"] = "Order anchor " + std::to_string(anchorRecord->anchorId);
        anchorResponse.summary["report"]["summary"] =
            "Anchor event " + anchorRecord->eventKind + " at session_seq " + std::to_string(anchorRecord->sessionSeq) +
            " with related order-case evidence.";
        anchorResponse.summary["report_summary"] = anchorResponse.summary["report"];
        annotateInvestigationEnvelope(&anchorResponse,
                                      frozenRevisionId,
                                      request.includeLiveTail,
                                      "order_anchor",
                                      "order_anchor");
        return anchorResponse;
    }

    if (operation == QueryOperation::ReadIncident || operation == QueryOperation::ScanIncidentReport) {
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
        IncidentRecord latestIncident = incidentRevisions.back();

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

        std::optional<ProtectedWindowRecord> incidentWindow = latestIncidentProtectedWindow(artifacts, latestIncident.logicalIncidentId);

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
        const json incidentDataQuality = buildDataQualitySummary(dataQualityEvents,
                                                                 request.includeLiveTail,
                                                                 latestIncident.instrumentId);
        latestIncident = applyIncidentDataQualityPenalty(latestIncident, incidentDataQuality, relatedFindings);
        for (const auto& finding : relatedFindings) {
            timeline.push_back(timelineEntryFromFinding(finding));
        }
        timeline.push_back(timelineEntryFromIncident(latestIncident));
        timeline = sortAndTrimTimeline(std::move(timeline), 24);
        const json timelineSummary = buildTimelineSummary(timeline);
        const std::string uncertaintySummary = reportUncertaintySummary(incidentDataQuality,
                                                                        relatedFindings.size(),
                                                                        distinctIncidentFindingKinds(relatedFindings));
        reportSummary["timeline_highlights"] = buildTimelineHighlights(timeline, 3);
        reportSummary["what_changed_first"] = firstTimelineHeadline(timeline);
        reportSummary["uncertainty"] = uncertaintySummary;
        json citations = json::array();
        citations.push_back(evidenceCitation("incident",
                                             incidentArtifactId(latestIncident.logicalIncidentId),
                                             latestIncident.firstSessionSeq,
                                             latestIncident.lastSessionSeq,
                                             latestIncident.title));
        if (incidentWindow.has_value()) {
            citations.push_back(evidenceCitation("protected_window",
                                                 protectedWindowArtifactId(incidentWindow->windowId),
                                                 incidentWindow->firstSessionSeq,
                                                 incidentWindow->lastSessionSeq,
                                                 incidentWindow->reason));
        }
        for (std::size_t i = 0; i < relatedFindings.size() && i < 4; ++i) {
            citations.push_back(evidenceCitation("finding",
                                                 findingArtifactId(relatedFindings[i].findingId),
                                                 relatedFindings[i].firstSessionSeq,
                                                 relatedFindings[i].lastSessionSeq,
                                                 relatedFindings[i].title));
        }
        const json artifact = {
            {"artifact_id", incidentArtifactId(latestIncident.logicalIncidentId)},
            {"artifact_type", "incident"},
            {"first_session_seq", latestIncident.firstSessionSeq},
            {"last_session_seq", latestIncident.lastSessionSeq},
            {"revision_id", frozenRevisionId}
        };
        const json entity = {
            {"logical_incident_id", latestIncident.logicalIncidentId},
            {"type", "incident"}
        };
        const json evidence = buildEvidenceSection(timeline, timelineSummary, citations, incidentDataQuality);

        response.summary = {
            {"artifact", artifact},
            {"entity", entity},
            {"evidence", evidence},
            {"includes_mutable_tail", request.includeLiveTail},
            {"logical_incident_id", latestIncident.logicalIncidentId},
            {"served_revision_id", frozenRevisionId},
            {"latest_incident", incidentToJson(latestIncident)},
            {"incident_revision_count", incidentRevisions.size()},
            {"score_breakdown", incidentScoreBreakdown(latestIncident, relatedFindings, &incidentDataQuality)},
            {"why_it_matters", incidentWhyItMatters(latestIncident)},
            {"uncertainty_summary", uncertaintySummary},
            {"report_summary", reportSummary},
            {"timeline", timeline},
            {"timeline_summary", timelineSummary},
            {"data_quality", incidentDataQuality},
            {"returned_events", response.events.size()},
            {"related_finding_count", relatedFindings.size()},
            {"incident_revisions", revisionsJson},
            {"report", reportSummary}
        };
        if (incidentWindow.has_value()) {
            response.summary["protected_window"] = protectedWindowToJson(*incidentWindow);
        }
        if (operation == QueryOperation::ScanIncidentReport) {
            if (request.includeLiveTail) {
                return rejectResponse(request, "scan_incident_report only supports frozen revision evidence");
            }
            const CaseReportRecord report = persistCaseReport(snapshot,
                                                              frozenRevisionId,
                                                              "incident_case",
                                                              latestIncident.logicalIncidentId,
                                                              latestIncident.overlappingAnchor,
                                                              latestIncident.firstSessionSeq,
                                                              latestIncident.lastSessionSeq,
                                                              response);
            response.summary["source_artifact"] = response.summary.value("artifact", json::object());
            response.summary["artifact"] = {
                {"artifact_id", caseReportArtifactId(report.reportId)},
                {"artifact_type", "case_report"},
                {"artifact_scope", "durable"},
                {"first_session_seq", report.firstSessionSeq},
                {"last_session_seq", report.lastSessionSeq},
                {"revision_id", report.revisionId}
            };
            response.summary["case_report_artifact"] = caseReportToJson(report);
            response.summary["is_durable_report"] = true;
            response.summary["scan_status"] = "persisted";
        }
        annotateInvestigationEnvelope(&response,
                                      frozenRevisionId,
                                      request.includeLiveTail,
                                      operation == QueryOperation::ScanIncidentReport ? "incident_report" : "incident",
                                      "incident");
        return response;
    }

    if (operation == QueryOperation::ReadProtectedWindow) {
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
        const json timeline = sortAndTrimTimeline([&]() {
            json items = json::array();
            for (const auto& event : events) {
                items.push_back(timelineEntryFromEvent(event));
            }
            return items;
        }(), 24);
        const json timelineSummary = buildTimelineSummary(timeline);
        json citations = json::array();
        citations.push_back(evidenceCitation("protected_window",
                                             protectedWindowArtifactId(request.windowId),
                                             selectedWindowSummary.value("first_session_seq", 0ULL),
                                             selectedWindowSummary.value("last_session_seq", 0ULL),
                                             selectedWindowSummary.value("reason", std::string())));
        response.summary = {
            {"artifact", {
                {"artifact_id", protectedWindowArtifactId(request.windowId)},
                {"artifact_type", "protected_window"},
                {"first_session_seq", selectedWindowSummary.value("first_session_seq", 0ULL)},
                {"last_session_seq", selectedWindowSummary.value("last_session_seq", 0ULL)},
                {"revision_id", frozenRevisionId}
            }},
            {"entity", {
                {"type", "protected_window"},
                {"window_id", request.windowId}
            }},
            {"data_quality", buildDataQualitySummary(events,
                                                    request.includeLiveTail,
                                                    selectedWindowSummary.value("instrument_id", std::string()))},
            {"evidence", buildEvidenceSection(timeline,
                                             timelineSummary,
                                             citations,
                                             buildDataQualitySummary(events,
                                                                     request.includeLiveTail,
                                                                     selectedWindowSummary.value("instrument_id", std::string())))},
            {"includes_mutable_tail", request.includeLiveTail},
            {"protected_window", selectedWindowSummary},
            {"returned_events", response.events.size()},
            {"served_revision_id", frozenRevisionId},
            {"timeline", timeline},
            {"timeline_summary", timelineSummary},
            {"window_id", request.windowId}
        };
        response.summary["report"] = {
            {"headline", selectedWindowSummary.value("reason", std::string("Protected window"))},
            {"summary", "Protected-window evidence slice pinned to the requested forensic window."},
            {"timeline_highlights", buildTimelineHighlights(timeline, 3)},
            {"what_changed_first", firstTimelineHeadline(timeline)},
            {"report_type", "protected_window"},
            {"uncertainty", reportUncertaintySummary(response.summary["data_quality"],
                                                     response.events.size(),
                                                     1)}
        };
        response.summary["report_summary"] = response.summary["report"];
        annotateInvestigationEnvelope(&response,
                                      frozenRevisionId,
                                      request.includeLiveTail,
                                      "protected_window",
                                      "protected_window");
        return response;
    }

    if (operation == QueryOperation::ReadRange) {
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
        response.result = eventListResultToJson(response.summary, response.events);
        return response;
    }

    if (operation == QueryOperation::FindOrderAnchor) {
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
        response.result = eventListResultToJson(response.summary, response.events);
        return response;
    }

    if (operation == QueryOperation::ReadOrderCase || operation == QueryOperation::ScanOrderCaseReport) {
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
        std::vector<std::string> selectorKeys = anchorSelectorKeys(anchor);
        std::vector<FindingRecord> relatedFindings;
        std::unordered_set<std::size_t> matchedFindingIndexes;
        for (const auto& key : selectorKeys) {
            const auto range = artifacts.findingsBySelector.equal_range(key);
            for (auto it = range.first; it != range.second; ++it) {
                if (!matchedFindingIndexes.insert(it->second).second) {
                    continue;
                }
                const auto& record = artifacts.findings[it->second];
                if (anchorsShareIdentity(record.overlappingAnchor, anchor)) {
                    relatedFindings.push_back(record);
                }
            }
        }
        std::sort(relatedFindings.begin(), relatedFindings.end(), [](const FindingRecord& left,
                                                                     const FindingRecord& right) {
            if (left.lastSessionSeq != right.lastSessionSeq) {
                return left.lastSessionSeq > right.lastSessionSeq;
            }
            return left.findingId > right.findingId;
        });

        std::vector<IncidentRecord> relatedIncidents;
        std::unordered_set<std::size_t> matchedIncidentIndexes;
        for (const auto& key : selectorKeys) {
            const auto range = artifacts.incidentsBySelector.equal_range(key);
            for (auto it = range.first; it != range.second; ++it) {
                if (!matchedIncidentIndexes.insert(it->second).second) {
                    continue;
                }
                const auto& record = artifacts.incidents[it->second];
                if (anchorsShareIdentity(record.overlappingAnchor, anchor)) {
                    relatedIncidents.push_back(record);
                }
            }
        }
        const std::vector<IncidentRecord> collapsedIncidents = collapseAdjustedIncidents(snapshot,
                                                                                         artifacts,
                                                                                         relatedIncidents,
                                                                                         frozenRevisionId,
                                                                                         request.includeLiveTail);

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
        const json dataQuality = buildDataQualitySummary(evidenceEvents,
                                                         request.includeLiveTail,
                                                         events.front().value("instrument_id", std::string()));
        const std::string uncertaintySummary = reportUncertaintySummary(
            dataQuality,
            relatedFindings.size() + collapsedIncidents.size(),
            std::max<std::size_t>(1, collapsedIncidents.size()));
        json citations = json::array();
        if (anchor.orderId > 0 || anchor.traceId > 0 || anchor.permId > 0 || !anchor.execId.empty()) {
            citations.push_back(evidenceCitation("order_case",
                                                 anchorSelectorArtifactId(anchor),
                                                 orderCaseSummary.value("first_session_seq", 0ULL),
                                                 orderCaseSummary.value("last_session_seq", 0ULL),
                                                 headline));
        }
        if (orderCaseSummary.contains("protected_window")) {
            const auto& window = orderCaseSummary["protected_window"];
            citations.push_back(evidenceCitation("protected_window",
                                                 protectedWindowArtifactId(window.value("window_id", 0ULL)),
                                                 window.value("first_session_seq", 0ULL),
                                                 window.value("last_session_seq", 0ULL),
                                                 window.value("reason", std::string())));
        }
        for (std::size_t i = 0; i < relatedFindings.size() && i < 4; ++i) {
            citations.push_back(evidenceCitation("finding",
                                                 findingArtifactId(relatedFindings[i].findingId),
                                                 relatedFindings[i].firstSessionSeq,
                                                 relatedFindings[i].lastSessionSeq,
                                                 relatedFindings[i].title));
        }
        for (std::size_t i = 0; i < collapsedIncidents.size() && i < 3; ++i) {
            citations.push_back(evidenceCitation("incident",
                                                 incidentArtifactId(collapsedIncidents[i].logicalIncidentId),
                                                 collapsedIncidents[i].firstSessionSeq,
                                                 collapsedIncidents[i].lastSessionSeq,
                                                 collapsedIncidents[i].title));
        }

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
        orderCaseSummary["data_quality"] = dataQuality;
        orderCaseSummary["uncertainty_summary"] = uncertaintySummary;
        orderCaseSummary["case_report"] = {
            {"headline", headline},
            {"summary", narrative.str()},
            {"timeline_highlights", buildTimelineHighlights(timeline, 4)},
            {"what_changed_first", firstTimelineHeadline(timeline)},
            {"uncertainty", uncertaintySummary},
            {"replay_target_session_seq", orderCaseSummary.value("replay_target_session_seq", 0ULL)},
            {"replay_from_session_seq", orderCaseSummary.value("replay_from_session_seq", 0ULL)},
            {"replay_to_session_seq", orderCaseSummary.value("replay_to_session_seq", 0ULL)}
        };
        orderCaseSummary["artifact"] = {
            {"artifact_id", anchorSelectorArtifactId(anchor)},
            {"artifact_type", "order_case"},
            {"first_session_seq", orderCaseSummary.value("first_session_seq", 0ULL)},
            {"last_session_seq", orderCaseSummary.value("last_session_seq", 0ULL)},
            {"revision_id", frozenRevisionId}
        };
        orderCaseSummary["entity"] = {
            {"anchor", anchorToJson(anchor)},
            {"type", "order_case"}
        };
        orderCaseSummary["evidence"] = buildEvidenceSection(timeline, timelineSummary, citations, dataQuality);
        orderCaseSummary["report"] = orderCaseSummary["case_report"];
        response.summary = std::move(orderCaseSummary);
        if (operation == QueryOperation::ScanOrderCaseReport) {
            if (request.includeLiveTail) {
                return rejectResponse(request, "scan_order_case_report only supports frozen revision evidence");
            }
            const CaseReportRecord report = persistCaseReport(snapshot,
                                                              frozenRevisionId,
                                                              "order_case",
                                                              0,
                                                              anchor,
                                                              response.summary.value("first_session_seq", 0ULL),
                                                              response.summary.value("last_session_seq", 0ULL),
                                                              response);
            response.summary["source_artifact"] = response.summary.value("artifact", json::object());
            response.summary["artifact"] = {
                {"artifact_id", caseReportArtifactId(report.reportId)},
                {"artifact_type", "case_report"},
                {"artifact_scope", "durable"},
                {"first_session_seq", report.firstSessionSeq},
                {"last_session_seq", report.lastSessionSeq},
                {"revision_id", report.revisionId}
            };
            response.summary["case_report_artifact"] = caseReportToJson(report);
            response.summary["is_durable_report"] = true;
            response.summary["scan_status"] = "persisted";
        }
        annotateInvestigationEnvelope(&response,
                                      frozenRevisionId,
                                      request.includeLiveTail,
                                      operation == QueryOperation::ScanOrderCaseReport ? "order_case_report" : "order_case",
                                      "order_case");
        return response;
    }

    if (operation == QueryOperation::SeekOrderAnchor) {
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
        response.result = seekOrderResultToJson(response.summary);
        return response;
    }

    if (operation == QueryOperation::ReplaySnapshot) {
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
        response.result = replaySnapshotResultToJson(response.summary);
        return response;
    }

    return rejectResponse(request, "unknown tape-engine operation");
}

} // namespace tape_engine
