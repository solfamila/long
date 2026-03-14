#include "app_shared.h"
#include "trace_exporter.h"
#include "mac_observability.h"

#include <random>
#include <atomic>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <system_error>
#include <unordered_map>

#define g_data appState()

namespace {
constexpr std::size_t kMaxTraceEvents = 512;
constexpr std::size_t kMaxTraceFills = 256;
constexpr std::size_t kMaxBridgeOutboxRecords = 512;
constexpr auto kRecentSubmitWindow = std::chrono::milliseconds(750);
constexpr auto kRecentWebSocketOrderWindow = std::chrono::seconds(2);
constexpr int kMaxWebSocketOrdersPerWindow = 5;
constexpr auto kWebSocketIdempotencyWindow = std::chrono::minutes(10);
constexpr int kDefaultBrokerEchoTimeoutMs = 2000;
constexpr int kDefaultCancelAckTimeoutMs = 5000;
constexpr int kDefaultPartialFillQuietTimeoutMs = 15000;
constexpr auto kReconciliationRetryDelayShort = std::chrono::seconds(2);
constexpr auto kReconciliationRetryDelayLong = std::chrono::seconds(5);
constexpr int kMaxReconciliationAttempts = 3;

const std::string& resolvedAppDataDirectory() {
    static const std::string path = []() -> std::string {
        namespace fs = std::filesystem;

        fs::path basePath;
        if (const char* overridePath = std::getenv("TWS_GUI_DATA_DIR");
            overridePath != nullptr && *overridePath != '\0') {
            basePath = fs::path(overridePath);
        } else if (const char* home = std::getenv("HOME");
                   home != nullptr && *home != '\0') {
            basePath = fs::path(home) / "Library" / "Application Support" / "TWS Trading GUI";
        } else {
            basePath = fs::current_path() / "tws_gui_data";
        }

        std::error_code ec;
        fs::create_directories(basePath, ec);
        return basePath.string();
    }();
    return path;
}

void reserveTraceIdFloor(std::atomic<std::uint64_t>& nextTraceId, std::uint64_t traceId) {
    std::uint64_t next = nextTraceId.load(std::memory_order_relaxed);
    while (next <= traceId &&
           !nextTraceId.compare_exchange_weak(next, traceId + 1, std::memory_order_relaxed)) {
    }
}

std::uint64_t recoverHighestTraceIdFromLog(const std::string& logPath) {
    std::ifstream in(logPath);
    if (!in.is_open()) {
        return 0;
    }

    std::uint64_t highestTraceId = 0;
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) {
            continue;
        }

        const json parsed = json::parse(line, nullptr, false);
        if (parsed.is_discarded()) {
            continue;
        }

        highestTraceId = std::max(highestTraceId, parsed.value("traceId", 0ULL));
    }
    return highestTraceId;
}

std::uint64_t recoverHighestBridgeSourceSeqFromJournal(const std::string& logPath) {
    std::ifstream in(logPath);
    if (!in.is_open()) {
        return 0;
    }

    std::uint64_t highestSourceSeq = 0;
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) {
            continue;
        }
        const json parsed = json::parse(line, nullptr, false);
        if (parsed.is_discarded()) {
            continue;
        }
        if (!parsed.contains("details") || !parsed["details"].is_object()) {
            continue;
        }
        const json& details = parsed["details"];
        highestSourceSeq = std::max(highestSourceSeq, details.value("sourceSeq", 0ULL));
        highestSourceSeq = std::max(highestSourceSeq, details.value("droppedSourceSeq", 0ULL));
    }
    return highestSourceSeq;
}

SharedData& bootstrapSharedData() {
    static SharedData data;
    return data;
}

std::atomic<SharedData*>& activeSharedDataSlot() {
    static std::atomic<SharedData*> slot{&bootstrapSharedData()};
    return slot;
}

void copySharedDataState(const SharedData& src, SharedData& dst) {
    dst.connected = src.connected;
    dst.sessionReady = src.sessionReady;
    dst.sessionState = src.sessionState;
    dst.managedAccounts = src.managedAccounts;
    dst.selectedAccount = src.selectedAccount;

    dst.currentSymbol = src.currentSymbol;
    dst.bidPrice = src.bidPrice;
    dst.askPrice = src.askPrice;
    dst.lastPrice = src.lastPrice;
    dst.askBook = src.askBook;
    dst.bidBook = src.bidBook;
    dst.depthSubscribed = src.depthSubscribed;

    dst.nextReqId.store(src.nextReqId.load(std::memory_order_relaxed), std::memory_order_relaxed);
    dst.activeMktDataReqId = src.activeMktDataReqId;
    dst.activeDepthReqId = src.activeDepthReqId;
    dst.suppressedMktDataCancelIds = src.suppressedMktDataCancelIds;
    dst.suppressedMktDepthCancelIds = src.suppressedMktDepthCancelIds;

    dst.currentQuantity = src.currentQuantity;
    dst.priceBuffer = src.priceBuffer;
    dst.maxPositionDollars = src.maxPositionDollars;
    dst.maxOrderNotional = src.maxOrderNotional;
    dst.maxOpenNotional = src.maxOpenNotional;
    dst.staleQuoteThresholdMs = src.staleQuoteThresholdMs;
    dst.brokerEchoTimeoutMs = src.brokerEchoTimeoutMs;
    dst.cancelAckTimeoutMs = src.cancelAckTimeoutMs;
    dst.partialFillQuietTimeoutMs = src.partialFillQuietTimeoutMs;
    dst.controllerArmMode = src.controllerArmMode;
    dst.controllerArmed = src.controllerArmed;
    dst.tradingKillSwitch = src.tradingKillSwitch;

    dst.twsHost = src.twsHost;
    dst.twsPort = src.twsPort;
    dst.twsClientId = src.twsClientId;
    dst.websocketAuthToken = src.websocketAuthToken;
    dst.websocketEnabled = src.websocketEnabled;
    dst.controllerEnabled = src.controllerEnabled;
    dst.appSessionId = src.appSessionId;
    dst.runtimeSessionId = src.runtimeSessionId;
    dst.startupRecoveryBanner = src.startupRecoveryBanner;

    dst.pendingSubscribeSymbol = src.pendingSubscribeSymbol;
    dst.hasPendingSubscribe = src.hasPendingSubscribe;
    dst.pendingWSQuantityCalc = src.pendingWSQuantityCalc;
    dst.wsQuantityUpdated = src.wsQuantityUpdated;

    dst.lastWsRequestedSymbol = src.lastWsRequestedSymbol;
    dst.lastWsSubscribeRequest = src.lastWsSubscribeRequest;
    dst.recentWsIdempotencyKeys = src.recentWsIdempotencyKeys;
    dst.recentWsOrderTimestamps = src.recentWsOrderTimestamps;

    dst.nextOrderId.store(src.nextOrderId.load(std::memory_order_relaxed), std::memory_order_relaxed);
    dst.orders = src.orders;

    dst.positions = src.positions;
    dst.positionsLoaded = src.positionsLoaded;
    dst.executionsLoaded = src.executionsLoaded;

    dst.wsServerRunning.store(src.wsServerRunning.load(std::memory_order_relaxed), std::memory_order_relaxed);
    dst.wsConnectedClients.store(src.wsConnectedClients.load(std::memory_order_relaxed), std::memory_order_relaxed);

    dst.controllerConnected.store(src.controllerConnected.load(std::memory_order_relaxed), std::memory_order_relaxed);
    dst.controllerDeviceName = src.controllerDeviceName;
    dst.controllerLockedDeviceName = src.controllerLockedDeviceName;
    dst.lastQuoteUpdate = src.lastQuoteUpdate;
    dst.lastSubmitFingerprint = src.lastSubmitFingerprint;
    dst.lastSubmitTime = src.lastSubmitTime;

    dst.messages = src.messages;
    dst.messagesCache = src.messagesCache;
    dst.messagesVersion = src.messagesVersion;
    dst.messagesCacheVersion = src.messagesCacheVersion;

    dst.nextTraceId.store(src.nextTraceId.load(std::memory_order_relaxed), std::memory_order_relaxed);
    dst.traces = src.traces;
    dst.traceRecency = src.traceRecency;
    dst.traceIdByOrderId = src.traceIdByOrderId;
    dst.traceIdByPermId = src.traceIdByPermId;
    dst.traceIdByExecId = src.traceIdByExecId;
    dst.latestTraceId = src.latestTraceId;
    dst.nextBridgeSourceSeq.store(src.nextBridgeSourceSeq.load(std::memory_order_relaxed), std::memory_order_relaxed);
    dst.bridgeOutbox = src.bridgeOutbox;
    dst.bridgeOutboxLossCount = src.bridgeOutboxLossCount;
    dst.lastBridgeSourceSeq = src.lastBridgeSourceSeq;
    dst.bridgeRecoveredPendingCount = src.bridgeRecoveredPendingCount;
    dst.bridgeRecoveredLossCount = src.bridgeRecoveredLossCount;
    dst.bridgeRecoveredLastSourceSeq = src.bridgeRecoveredLastSourceSeq;
    dst.bridgeFallbackState = src.bridgeFallbackState;
    dst.bridgeFallbackReason = src.bridgeFallbackReason;
    dst.bridgeRecoveryRequired = src.bridgeRecoveryRequired;
}

std::mutex& uiInvalidationCallbackMutex() {
    static std::mutex m;
    return m;
}

UiInvalidationCallback& uiInvalidationCallbackSlot() {
    static UiInvalidationCallback callback;
    return callback;
}

std::mutex& sharedDataMutationDispatcherMutex() {
    static std::mutex m;
    return m;
}

SharedDataMutationDispatcher& sharedDataMutationDispatcherSlot() {
    static SharedDataMutationDispatcher dispatcher;
    return dispatcher;
}

std::mutex& tradeTraceFileMutex() {
    static std::mutex m;
    return m;
}

std::mutex& runtimeJournalFileMutex() {
    static std::mutex m;
    return m;
}

struct ImmutableSharedDataSnapshot {
    bool connected = false;
    bool sessionReady = false;
    RuntimeSessionState sessionState = RuntimeSessionState::Disconnected;
    std::string managedAccounts;
    std::string selectedAccount;

    std::string currentSymbol;
    double bidPrice = 0.0;
    double askPrice = 0.0;
    double lastPrice = 0.0;
    std::vector<BookLevel> askBook;
    std::vector<BookLevel> bidBook;
    bool depthSubscribed = false;

    int activeMktDataReqId = 0;
    int activeDepthReqId = 0;

    int currentQuantity = 1;
    double priceBuffer = 0.01;
    double maxPositionDollars = 40000.0;
    double maxOrderNotional = 15000.0;
    double maxOpenNotional = 50000.0;
    int staleQuoteThresholdMs = 1500;
    int brokerEchoTimeoutMs = 2000;
    int cancelAckTimeoutMs = 5000;
    int partialFillQuietTimeoutMs = 15000;
    ControllerArmMode controllerArmMode = ControllerArmMode::OneShot;
    bool controllerArmed = false;
    bool tradingKillSwitch = false;

    std::string twsHost = DEFAULT_HOST;
    int twsPort = DEFAULT_PORT;
    int twsClientId = DEFAULT_CLIENT_ID;
    std::string websocketAuthToken;
    bool websocketEnabled = true;
    bool controllerEnabled = true;
    std::string appSessionId;
    std::string runtimeSessionId;
    std::string startupRecoveryBanner;

    std::string pendingSubscribeSymbol;
    bool hasPendingSubscribe = false;
    bool pendingWSQuantityCalc = false;
    bool wsQuantityUpdated = false;

    bool positionsLoaded = false;
    bool executionsLoaded = false;
    bool wsServerRunning = false;
    int wsConnectedClients = 0;
    bool controllerConnected = false;
    std::string controllerDeviceName;
    std::string controllerLockedDeviceName;
    std::chrono::steady_clock::time_point lastQuoteUpdate{};

    std::map<OrderId, OrderInfo> orders;
    std::map<std::string, PositionInfo> positions;

    std::string messagesText;
    std::uint64_t messagesVersion = 0;

    std::map<std::uint64_t, TradeTrace> traces;
    std::deque<std::uint64_t> traceRecency;
    std::map<OrderId, std::uint64_t> traceIdByOrderId;
    std::uint64_t latestTraceId = 0;
    std::deque<BridgeOutboxRecord> bridgeOutbox;
    std::uint64_t bridgeOutboxLossCount = 0;
    std::uint64_t lastBridgeSourceSeq = 0;
    int bridgeRecoveredPendingCount = 0;
    int bridgeRecoveredLossCount = 0;
    std::uint64_t bridgeRecoveredLastSourceSeq = 0;
    std::string bridgeFallbackState;
    std::string bridgeFallbackReason;
    bool bridgeRecoveryRequired = false;
};

std::shared_ptr<const ImmutableSharedDataSnapshot>& publishedSharedDataSnapshotSlot() {
    static std::shared_ptr<const ImmutableSharedDataSnapshot> snapshot;
    return snapshot;
}

std::uint64_t findTraceIdLocked(OrderId orderId, long long permId, const std::string& execId);
json makeTraceEventLogLine(const TradeTrace& trace, const TraceEvent& event);
void emitMacTraceObservation(std::uint64_t traceId,
                             TradeEventType type,
                             const std::string& stage,
                             const std::string& details);

template <typename Fn>
auto invokeSharedDataMutation(Fn&& fn) -> decltype(fn()) {
    SharedDataMutationDispatcher dispatcher;
    {
        std::lock_guard<std::mutex> lock(sharedDataMutationDispatcherMutex());
        dispatcher = sharedDataMutationDispatcherSlot();
    }
    if (!dispatcher) {
        if constexpr (std::is_void_v<decltype(fn())>) {
            fn();
            publishSharedDataSnapshot();
            return;
        } else {
            auto result = fn();
            publishSharedDataSnapshot();
            return result;
        }
    }

    using Result = decltype(fn());
    auto promise = std::make_shared<std::promise<Result>>();
    auto future = promise->get_future();
    dispatcher([promise, fn = std::forward<Fn>(fn)]() mutable {
        try {
            if constexpr (std::is_void_v<Result>) {
                fn();
                promise->set_value();
            } else {
                promise->set_value(fn());
            }
        } catch (...) {
            promise->set_exception(std::current_exception());
        }
    });
    if constexpr (std::is_void_v<Result>) {
        future.get();
        publishSharedDataSnapshot();
        return;
    } else {
        auto result = future.get();
        publishSharedDataSnapshot();
        return result;
    }
}

bool hasTime(std::chrono::steady_clock::time_point tp) {
    return tp.time_since_epoch().count() != 0;
}

bool hasTime(std::chrono::system_clock::time_point tp) {
    return tp.time_since_epoch().count() != 0;
}

std::string trimCopy(const std::string& s) {
    const auto begin = std::find_if_not(s.begin(), s.end(),
        [](unsigned char c) { return std::isspace(c) != 0; });
    if (begin == s.end()) return {};
    const auto end = std::find_if_not(s.rbegin(), s.rend(),
        [](unsigned char c) { return std::isspace(c) != 0; }).base();
    return std::string(begin, end);
}

std::vector<std::string> splitCsv(const std::string& csv) {
    std::vector<std::string> out;
    std::string current;
    for (char ch : csv) {
        if (ch == ',') {
            std::string item = trimCopy(current);
            if (!item.empty()) out.push_back(item);
            current.clear();
        } else {
            current.push_back(ch);
        }
    }
    std::string tail = trimCopy(current);
    if (!tail.empty()) out.push_back(tail);
    return out;
}

std::string summarizeBookSide(const std::vector<BookLevel>& book, std::size_t maxLevels, const char* label) {
    std::ostringstream oss;
    oss << label << "[";
    const std::size_t count = std::min(maxLevels, book.size());
    for (std::size_t i = 0; i < count; ++i) {
        if (i != 0) oss << " | ";
        oss << std::fixed << std::setprecision(2) << book[i].price << "x" << std::setprecision(0) << book[i].size;
    }
    oss << "]";
    return oss.str();
}

std::string buildBookSummary(const std::vector<BookLevel>& askBook, const std::vector<BookLevel>& bidBook) {
    return summarizeBookSide(askBook, 3, "ask") + " " + summarizeBookSide(bidBook, 3, "bid");
}

std::string randomHexToken(std::size_t bytes) {
    static constexpr char kHex[] = "0123456789abcdef";
    std::random_device rd;
    std::uniform_int_distribution<int> dist(0, 255);
    std::string token;
    token.reserve(bytes * 2);
    for (std::size_t i = 0; i < bytes; ++i) {
        const int value = dist(rd);
        token.push_back(kHex[(value >> 4) & 0xF]);
        token.push_back(kHex[value & 0xF]);
    }
    return token;
}

void pruneWebSocketStateLocked(SharedData& state, std::chrono::steady_clock::time_point now) {
    while (!state.recentWsOrderTimestamps.empty() &&
           (now - state.recentWsOrderTimestamps.front()) > kRecentWebSocketOrderWindow) {
        state.recentWsOrderTimestamps.pop_front();
    }

    for (auto it = state.recentWsIdempotencyKeys.begin(); it != state.recentWsIdempotencyKeys.end();) {
        if ((now - it->second) > kWebSocketIdempotencyWindow) {
            it = state.recentWsIdempotencyKeys.erase(it);
        } else {
            ++it;
        }
    }
}

json makeRuntimeJournalLine(const std::string& event, const json& details) {
    json line;
    line["event"] = event;
    line["wallTime"] = formatWallTime(std::chrono::system_clock::now());
    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        if (!g_data.appSessionId.empty()) {
            line["appSessionId"] = g_data.appSessionId;
        }
        if (!g_data.runtimeSessionId.empty()) {
            line["runtimeSessionId"] = g_data.runtimeSessionId;
        }
    }
    if (!details.is_null() && !details.empty()) {
        line["details"] = details;
    }
    return line;
}

std::string makeOrderFingerprint(const SubmitIntent& intent) {
    std::ostringstream oss;
    oss << intent.source << '|'
        << intent.symbol << '|'
        << intent.side << '|'
        << intent.requestedQty << '|'
        << std::fixed << std::setprecision(4) << intent.limitPrice << '|'
        << (intent.closeOnly ? '1' : '0');
    return oss.str();
}

std::chrono::steady_clock::duration reconciliationRetryDelayForAttempt(int attempts) {
    if (attempts <= 1) {
        return kReconciliationRetryDelayShort;
    }
    return kReconciliationRetryDelayLong;
}

void disarmOrderWatchdogs(OrderInfo& order) {
    order.watchdogs.brokerEchoArmed = false;
    order.watchdogs.cancelAckArmed = false;
    order.watchdogs.partialFillQuietArmed = false;
}

void noteOrderBrokerCallback(OrderInfo& order, std::chrono::steady_clock::time_point now) {
    order.watchdogs.lastBrokerCallback = now;
    order.watchdogs.brokerEchoArmed = false;
}

void armBrokerEchoWatchdog(OrderInfo& order,
                           std::chrono::steady_clock::time_point now,
                           int timeoutMs) {
    order.localState = LocalOrderState::AwaitingBrokerEcho;
    order.watchdogs.brokerEchoArmed = true;
    order.watchdogs.brokerEchoDeadline = now + std::chrono::milliseconds(std::max(250, timeoutMs));
    order.watchdogs.cancelAckArmed = false;
    order.watchdogs.partialFillQuietArmed = false;
}

void armCancelAckWatchdog(OrderInfo& order,
                          std::chrono::steady_clock::time_point now,
                          int timeoutMs) {
    order.localState = LocalOrderState::AwaitingCancelAck;
    order.cancelPending = true;
    order.watchdogs.cancelAckArmed = true;
    order.watchdogs.cancelAckDeadline = now + std::chrono::milliseconds(std::max(500, timeoutMs));
    order.watchdogs.brokerEchoArmed = false;
    order.watchdogs.partialFillQuietArmed = false;
}

void armPartialFillQuietWatchdog(OrderInfo& order,
                                 std::chrono::steady_clock::time_point now,
                                 int timeoutMs) {
    order.localState = LocalOrderState::PartiallyFilled;
    order.watchdogs.partialFillQuietArmed = true;
    order.watchdogs.partialFillQuietDeadline = now + std::chrono::milliseconds(std::max(1000, timeoutMs));
    order.watchdogs.brokerEchoArmed = false;
    order.watchdogs.cancelAckArmed = false;
}

void transitionOrderToResolvedState(OrderInfo& order,
                                    LocalOrderState localState,
                                    std::chrono::steady_clock::time_point now) {
    order.localState = localState;
    order.watchdogs.lastBrokerCallback = now;
    disarmOrderWatchdogs(order);
}

int beginOrderReconciliation(OrderInfo& order,
                             const std::string& reason,
                             std::chrono::steady_clock::time_point now,
                             bool incrementAttempts) {
    order.localState = LocalOrderState::NeedsReconciliation;
    order.lastReconciliationReason = reason;
    order.lastReconciliationTime = now;
    order.manualReviewAcknowledged = false;
    if (incrementAttempts) {
        order.watchdogs.reconciliationAttempts += 1;
    } else if (order.watchdogs.reconciliationAttempts < 1) {
        order.watchdogs.reconciliationAttempts = 1;
    }
    order.watchdogs.brokerEchoArmed = true;
    order.watchdogs.brokerEchoDeadline = now + reconciliationRetryDelayForAttempt(order.watchdogs.reconciliationAttempts);
    order.watchdogs.cancelAckArmed = false;
    order.watchdogs.partialFillQuietArmed = false;
    return order.watchdogs.reconciliationAttempts;
}

void floorNextOrderIdLocked(SharedData& state, OrderId seenOrderId) {
    if (seenOrderId <= 0) {
        return;
    }
    OrderId next = state.nextOrderId.load(std::memory_order_relaxed);
    while (next <= seenOrderId &&
           !state.nextOrderId.compare_exchange_weak(next, seenOrderId + 1, std::memory_order_relaxed)) {
    }
}

std::uint64_t makeRecoveredTraceId(SharedData& state) {
    return state.nextTraceId.fetch_add(1, std::memory_order_relaxed);
}

void reserveTraceIdLocked(SharedData& state, std::uint64_t traceId) {
    reserveTraceIdFloor(state.nextTraceId, traceId);
}

std::uint64_t registerRecoveredTraceLocked(SharedData& state,
                                           const TradeTrace& sourceTrace,
                                           OrderId orderIdHint,
                                           long long permIdHint,
                                           const std::string& execIdHint) {
    TradeTrace trace = sourceTrace;
    if (trace.traceId == 0) {
        trace.traceId = makeRecoveredTraceId(state);
    } else {
        reserveTraceIdLocked(state, trace.traceId);
    }

    if (trace.orderId <= 0) {
        trace.orderId = orderIdHint;
    }
    if (trace.permId <= 0) {
        trace.permId = permIdHint;
    }

    state.traces[trace.traceId] = trace;
    state.traceRecency.push_back(trace.traceId);
    state.latestTraceId = trace.traceId;
    if (trace.orderId > 0) {
        state.traceIdByOrderId[trace.orderId] = trace.traceId;
    }
    if (trace.permId > 0) {
        state.traceIdByPermId[trace.permId] = trace.traceId;
    }
    for (const auto& fill : trace.fills) {
        if (!fill.execId.empty()) {
            state.traceIdByExecId[fill.execId] = trace.traceId;
        }
    }
    if (!execIdHint.empty()) {
        state.traceIdByExecId[execIdHint] = trace.traceId;
    }
    return trace.traceId;
}

std::uint64_t ensureRecoveredTraceLocked(SharedData& state,
                                         OrderId orderId,
                                         long long permId,
                                         const std::string& execId,
                                         const std::string& source,
                                         const std::string& symbol,
                                         const std::string& side,
                                         int requestedQty,
                                         double limitPrice,
                                         const std::string& account,
                                         const std::string& note) {
    std::uint64_t traceId = findTraceIdLocked(orderId, permId, execId);
    if (traceId != 0) {
        return traceId;
    }

    TradeTraceSnapshot replayedSnapshot;
    if (replayTradeTraceSnapshotByIdentityFromLog(orderId,
                                                  permId,
                                                  execId,
                                                  &replayedSnapshot,
                                                  nullptr,
                                                  tradeTraceLogPath()) &&
        replayedSnapshot.found) {
        traceId = registerRecoveredTraceLocked(state, replayedSnapshot.trace, orderId, permId, execId);
        appendRuntimeJournalEvent("recovered_trace_hydrated", {
            {"traceId", static_cast<unsigned long long>(traceId)},
            {"orderId", static_cast<long long>(orderId)},
            {"permId", permId},
            {"execId", execId},
            {"symbol", replayedSnapshot.trace.symbol},
            {"side", replayedSnapshot.trace.side}
        });
        return traceId;
    }

    TradeTrace trace;
    trace.traceId = makeRecoveredTraceId(state);
    trace.orderId = orderId;
    trace.permId = permId;
    trace.source = source;
    trace.symbol = symbol;
    trace.side = side;
    trace.account = account;
    trace.requestedQty = requestedQty;
    trace.limitPrice = limitPrice;
    trace.notes = note;
    trace.triggerMono = std::chrono::steady_clock::now();
    trace.triggerWall = std::chrono::system_clock::now();

    TraceEvent event;
    event.type = TradeEventType::Note;
    event.monoTs = trace.triggerMono;
    event.wallTs = trace.triggerWall;
    event.stage = "Recovered";
    event.details = note;
    trace.events.push_back(event);

    state.traces[trace.traceId] = trace;
    state.traceRecency.push_back(trace.traceId);
    state.latestTraceId = trace.traceId;
    if (orderId > 0) {
        state.traceIdByOrderId[orderId] = trace.traceId;
    }
    if (permId > 0) {
        state.traceIdByPermId[permId] = trace.traceId;
    }
    if (!execId.empty()) {
        state.traceIdByExecId[execId] = trace.traceId;
    }
    appendTradeTraceLogLine(makeTraceEventLogLine(trace, event));
    emitMacTraceObservation(trace.traceId, TradeEventType::Note, event.stage, event.details);
    appendRuntimeJournalEvent("recovered_trace_created", {
        {"traceId", static_cast<unsigned long long>(trace.traceId)},
        {"orderId", static_cast<long long>(orderId)},
        {"permId", permId},
        {"execId", execId},
        {"symbol", symbol},
        {"side", side}
    });
    return trace.traceId;
}

json makeTraceEventLogLine(const TradeTrace& trace, const TraceEvent& event) {
    json line;
    line["traceId"] = static_cast<unsigned long long>(trace.traceId);
    line["orderId"] = static_cast<long long>(trace.orderId);
    line["permId"] = trace.permId;
    line["source"] = trace.source;
    line["symbol"] = trace.symbol;
    line["side"] = trace.side;
    line["requestedQty"] = trace.requestedQty;
    line["limitPrice"] = trace.limitPrice;
    line["closeOnly"] = trace.closeOnly;
    line["eventType"] = tradeEventTypeToString(event.type);
    line["stage"] = event.stage;
    line["details"] = event.details;
    line["wallTime"] = formatWallTime(event.wallTs);
    if (hasTime(trace.triggerMono) && hasTime(event.monoTs)) {
        line["sinceTriggerMs"] = durationMs(trace.triggerMono, event.monoTs);
    }
    if (event.cumFilled >= 0.0) line["cumFilled"] = event.cumFilled;
    if (event.remaining >= 0.0) line["remaining"] = event.remaining;
    if (event.price > 0.0) line["price"] = event.price;
    if (event.shares > 0) line["shares"] = event.shares;
    if (event.errorCode != 0) line["errorCode"] = event.errorCode;
    if (!trace.notes.empty()) line["notes"] = trace.notes;
    if (!trace.bookSummary.empty()) line["bookSummary"] = trace.bookSummary;
    return line;
}

void emitMacTraceObservation(std::uint64_t traceId,
                             TradeEventType type,
                             const std::string& stage,
                             const std::string& details) {
    switch (type) {
        case TradeEventType::Trigger:
            macTraceBegin(traceId, "trade_lifecycle", details);
            macTraceEvent(traceId, "trigger", details);
            break;
        case TradeEventType::ValidationStart:
            macTraceBegin(traceId, "validation", details);
            break;
        case TradeEventType::ValidationOk:
        case TradeEventType::ValidationFailed:
            macTraceEnd(traceId, "validation", details);
            break;
        case TradeEventType::PlaceOrderCallStart:
            macTraceBegin(traceId, "place_order", details);
            break;
        case TradeEventType::PlaceOrderCallEnd:
            macTraceEnd(traceId, "place_order", details);
            break;
        case TradeEventType::OpenOrderSeen:
            macTraceEvent(traceId, "open_order", details);
            break;
        case TradeEventType::OrderStatusSeen:
            macTraceEvent(traceId, "order_status", details);
            break;
        case TradeEventType::ExecDetailsSeen:
            macTraceEvent(traceId, "exec_details", details);
            break;
        case TradeEventType::CommissionSeen:
            macTraceEvent(traceId, "commission", details);
            break;
        case TradeEventType::ErrorSeen:
            macLogError("orders", "Trace " + std::to_string(static_cast<unsigned long long>(traceId)) + ": " + details);
            macTraceEvent(traceId, "error", details);
            break;
        case TradeEventType::CancelRequestSent:
            macTraceBegin(traceId, "cancel", details);
            break;
        case TradeEventType::CancelAck:
            macTraceEnd(traceId, "cancel", details);
            break;
        case TradeEventType::FinalState:
            macTraceEnd(traceId, "trade_lifecycle", details);
            break;
        case TradeEventType::Note:
            macTraceEvent(traceId, "note", details);
            break;
        default:
            break;
    }
}

void setTraceTerminalFields(TradeTrace& trace, const std::string& terminalStatus, const std::string& reason) {
    trace.terminalStatus = terminalStatus;
    if (!reason.empty()) {
        trace.latestError = reason;
    }
    if (terminalStatus == "Filled" && !hasTime(trace.fullFillMono)) {
        trace.fullFillMono = std::chrono::steady_clock::now();
    }
}

std::uint64_t findTraceIdLocked(OrderId orderId, long long permId = 0, const std::string& execId = {}) {
    if (orderId > 0) {
        const auto byOrder = g_data.traceIdByOrderId.find(orderId);
        if (byOrder != g_data.traceIdByOrderId.end()) return byOrder->second;
    }
    if (permId > 0) {
        const auto byPerm = g_data.traceIdByPermId.find(permId);
        if (byPerm != g_data.traceIdByPermId.end()) return byPerm->second;
    }
    if (!execId.empty()) {
        const auto byExec = g_data.traceIdByExecId.find(execId);
        if (byExec != g_data.traceIdByExecId.end()) return byExec->second;
    }
    return 0;
}

} // namespace

std::shared_ptr<const ImmutableSharedDataSnapshot> ensurePublishedSharedDataSnapshot();
void publishSharedDataSnapshotLocked(SharedData& state);

SharedData& appState() {
    SharedData* active = activeSharedDataSlot().load(std::memory_order_acquire);
    return *(active != nullptr ? active : &bootstrapSharedData());
}

std::string appDataDirectory() {
    return resolvedAppDataDirectory();
}

std::string tradeTraceLogPath() {
    return (std::filesystem::path(resolvedAppDataDirectory()) / TRADE_TRACE_LOG_FILENAME).string();
}

std::string runtimeJournalLogPath() {
    return (std::filesystem::path(resolvedAppDataDirectory()) / RUNTIME_JOURNAL_LOG_FILENAME).string();
}

void bindSharedDataOwner(SharedData* owner) {
    if (owner == nullptr) {
        return;
    }
    SharedData* current = activeSharedDataSlot().load(std::memory_order_acquire);
    if (current == owner) {
        return;
    }
    {
        std::scoped_lock lock(current->mutex, owner->mutex);
        copySharedDataState(*current, *owner);
    }
    reserveTraceIdFloor(owner->nextTraceId, recoverHighestTraceIdFromLog(tradeTraceLogPath()));
    reserveTraceIdFloor(owner->nextBridgeSourceSeq, recoverHighestBridgeSourceSeqFromJournal(runtimeJournalLogPath()));
    activeSharedDataSlot().store(owner, std::memory_order_release);
    publishSharedDataSnapshot();
}

void unbindSharedDataOwner(SharedData* owner) {
    SharedData* current = activeSharedDataSlot().load(std::memory_order_acquire);
    if (owner == nullptr || current != owner) {
        return;
    }
    SharedData& bootstrap = bootstrapSharedData();
    {
        std::scoped_lock lock(owner->mutex, bootstrap.mutex);
        copySharedDataState(*owner, bootstrap);
    }
    activeSharedDataSlot().store(&bootstrap, std::memory_order_release);
    publishSharedDataSnapshot();
}

void setUiInvalidationCallback(UiInvalidationCallback callback) {
    std::lock_guard<std::mutex> lock(uiInvalidationCallbackMutex());
    uiInvalidationCallbackSlot() = std::move(callback);
}

void clearUiInvalidationCallback() {
    std::lock_guard<std::mutex> lock(uiInvalidationCallbackMutex());
    uiInvalidationCallbackSlot() = nullptr;
}

void setSharedDataMutationDispatcher(SharedDataMutationDispatcher dispatcher) {
    std::lock_guard<std::mutex> lock(sharedDataMutationDispatcherMutex());
    sharedDataMutationDispatcherSlot() = std::move(dispatcher);
}

void clearSharedDataMutationDispatcher() {
    std::lock_guard<std::mutex> lock(sharedDataMutationDispatcherMutex());
    sharedDataMutationDispatcherSlot() = nullptr;
}

void publishSharedDataSnapshot() {
    SharedData& state = appState();
    std::lock_guard<std::recursive_mutex> lock(state.mutex);
    publishSharedDataSnapshotLocked(state);
}

void requestUiInvalidation() {
    UiInvalidationCallback callback;
    {
        std::lock_guard<std::mutex> lock(uiInvalidationCallbackMutex());
        callback = uiInvalidationCallbackSlot();
    }
    if (callback) {
        callback();
    }
}

namespace trading_engine {

void reduce(SharedData& state, const RuntimeBootstrapEvent& event) {
    std::lock_guard<std::recursive_mutex> lock(state.mutex);
    state.appSessionId = event.appSessionId;
    state.runtimeSessionId = event.runtimeSessionId;
    state.startupRecoveryBanner = event.startupRecoveryBanner;
    state.wsServerRunning.store(false, std::memory_order_relaxed);
    state.wsConnectedClients.store(0, std::memory_order_relaxed);
}

void reduce(SharedData& state, const RuntimeMessageEvent& event) {
    if (!event.message.empty()) {
        state.addMessage(event.message);
    }
}

void reduce(SharedData& state, const GuiInputsSyncedEvent& event) {
    std::lock_guard<std::recursive_mutex> lock(state.mutex);
    state.currentQuantity = event.quantityInput;
    state.priceBuffer = event.priceBuffer;
    state.maxPositionDollars = event.maxPositionDollars;
}

void reduce(SharedData& state, const ConnectionConfigUpdatedEvent& event) {
    std::lock_guard<std::recursive_mutex> lock(state.mutex);
    state.twsHost = event.config.host.empty() ? DEFAULT_HOST : event.config.host;
    state.twsPort = event.config.port > 0 ? event.config.port : DEFAULT_PORT;
    state.twsClientId = event.config.clientId > 0 ? event.config.clientId : DEFAULT_CLIENT_ID;
    state.websocketAuthToken = event.config.websocketAuthToken;
    state.websocketEnabled = event.config.websocketEnabled;
    state.controllerEnabled = event.config.controllerEnabled;
}

void reduce(SharedData& state, const RiskControlsUpdatedEvent& event) {
    std::lock_guard<std::recursive_mutex> lock(state.mutex);
    state.staleQuoteThresholdMs = std::max(250, event.staleQuoteThresholdMs);
    state.brokerEchoTimeoutMs = std::max(250, event.brokerEchoTimeoutMs);
    state.cancelAckTimeoutMs = std::max(500, event.cancelAckTimeoutMs);
    state.partialFillQuietTimeoutMs = std::max(1000, event.partialFillQuietTimeoutMs);
    state.maxOrderNotional = std::max(100.0, event.maxOrderNotional);
    state.maxOpenNotional = std::max(state.maxOrderNotional, event.maxOpenNotional);
    state.controllerArmMode = event.controllerArmMode;
}

bool reduce(SharedData& state, const ControllerArmedChangedEvent& event) {
    std::lock_guard<std::recursive_mutex> lock(state.mutex);
    if (state.controllerArmed == event.armed) {
        return false;
    }
    state.controllerArmed = event.armed;
    return true;
}

bool reduce(SharedData& state, const TradingKillSwitchChangedEvent& event) {
    std::lock_guard<std::recursive_mutex> lock(state.mutex);
    if (state.tradingKillSwitch == event.enabled) {
        return false;
    }
    state.tradingKillSwitch = event.enabled;
    return true;
}

bool reduce(SharedData& state, const ControllerConnectionStateUpdatedEvent& event) {
    std::lock_guard<std::recursive_mutex> lock(state.mutex);
    const bool connectedChanged = state.controllerConnected.load(std::memory_order_relaxed) != event.connected;
    const bool nameChanged = state.controllerDeviceName != event.deviceName;
    if (!connectedChanged && !nameChanged) {
        return false;
    }
    state.controllerConnected.store(event.connected, std::memory_order_relaxed);
    state.controllerDeviceName = event.deviceName;
    return true;
}

bool reduce(SharedData& state, const ControllerLockedDeviceUpdatedEvent& event) {
    std::lock_guard<std::recursive_mutex> lock(state.mutex);
    if (state.controllerLockedDeviceName == event.deviceName) {
        return false;
    }
    state.controllerLockedDeviceName = event.deviceName;
    return true;
}

bool reduce(SharedData& state, const WebSocketServerRunningChangedEvent& event) {
    const bool previous = state.wsServerRunning.load(std::memory_order_relaxed);
    if (previous == event.running) {
        return false;
    }
    state.wsServerRunning.store(event.running, std::memory_order_relaxed);
    return true;
}

int reduce(SharedData& state, const WebSocketClientDeltaEvent& event) {
    int next = state.wsConnectedClients.load(std::memory_order_relaxed) + event.delta;
    if (next < 0) {
        next = 0;
    }
    state.wsConnectedClients.store(next, std::memory_order_relaxed);
    return next;
}

WebSocketSubscribeDecision reduce(SharedData& state, const WebSocketSubscribeRequestedEvent& event) {
    std::lock_guard<std::recursive_mutex> lock(state.mutex);
    if (!(state.connected && state.sessionReady)) {
        return WebSocketSubscribeDecision::SessionNotReady;
    }
    if (state.lastWsRequestedSymbol == event.symbol &&
        (event.requestTime - state.lastWsSubscribeRequest) < std::chrono::milliseconds(300)) {
        return WebSocketSubscribeDecision::DuplicateIgnored;
    }
    state.lastWsRequestedSymbol = event.symbol;
    state.lastWsSubscribeRequest = event.requestTime;
    if (state.currentSymbol == event.symbol &&
        state.activeMktDataReqId != 0 &&
        state.activeDepthReqId != 0) {
        return WebSocketSubscribeDecision::AlreadySubscribed;
    }
    return WebSocketSubscribeDecision::Proceed;
}

void reduce(SharedData& state, const MarketSubscriptionClearedEvent& event) {
    std::lock_guard<std::recursive_mutex> lock(state.mutex);
    if (event.marketDataRequestId != 0) {
        state.suppressedMktDataCancelIds.insert(event.marketDataRequestId);
        if (state.activeMktDataReqId == event.marketDataRequestId) {
            state.activeMktDataReqId = 0;
        }
    }
    if (event.depthRequestId != 0) {
        state.suppressedMktDepthCancelIds.insert(event.depthRequestId);
        if (state.activeDepthReqId == event.depthRequestId) {
            state.activeDepthReqId = 0;
        }
    }
    state.depthSubscribed = false;
    state.bidPrice = 0.0;
    state.askPrice = 0.0;
    state.lastPrice = 0.0;
    state.askBook.clear();
    state.bidBook.clear();
}

void reduce(SharedData& state, const MarketSubscriptionStartedEvent& event) {
    std::lock_guard<std::recursive_mutex> lock(state.mutex);
    state.activeMktDataReqId = event.marketDataRequestId;
    state.activeDepthReqId = event.depthRequestId;
    state.depthSubscribed = true;
    state.currentSymbol = event.symbol;
    state.bidPrice = 0.0;
    state.askPrice = 0.0;
    state.lastPrice = 0.0;
    state.askBook.clear();
    state.bidBook.clear();
    state.pendingSubscribeSymbol = event.symbol;
    state.hasPendingSubscribe = true;
    state.pendingWSQuantityCalc = event.recalcQtyFromFirstAsk;
    state.wsQuantityUpdated = false;
    if (event.recalcQtyFromFirstAsk) {
        state.currentQuantity = 1;
    }
}

void reduce(SharedData& state, const BrokerConnectAckEvent&) {
    std::string host;
    int port = DEFAULT_PORT;
    {
        std::lock_guard<std::recursive_mutex> lock(state.mutex);
        state.connected = true;
        state.sessionReady = false;
        state.sessionState = RuntimeSessionState::SocketConnected;
        host = state.twsHost;
        port = state.twsPort;
    }
    appendRuntimeJournalEvent("tws_connect_ack", {{"host", host}, {"port", port}});
    state.addMessage("Connected to TWS (awaiting nextValidId)");
}

void reduce(SharedData& state, const BrokerConnectionClosedEvent&) {
    {
        std::lock_guard<std::recursive_mutex> lock(state.mutex);
        state.connected = false;
        state.sessionReady = false;
        state.sessionState = RuntimeSessionState::Disconnected;
        state.nextOrderId.store(0, std::memory_order_relaxed);
        state.activeMktDataReqId = 0;
        state.activeDepthReqId = 0;
        state.depthSubscribed = false;
        state.positionsLoaded = false;
        state.executionsLoaded = false;
        state.bidPrice = 0.0;
        state.askPrice = 0.0;
        state.lastPrice = 0.0;
        state.askBook.clear();
        state.bidBook.clear();
        state.pendingWSQuantityCalc = false;
        state.wsQuantityUpdated = false;
        for (auto& [orderId, order] : state.orders) {
            (void)orderId;
            if (!order.isTerminal()) {
                order.localState = LocalOrderState::NeedsReconciliation;
                disarmOrderWatchdogs(order);
            }
        }
    }
    appendRuntimeJournalEvent("tws_connection_closed");
    state.addMessage("Disconnected from TWS");
}

void reduce(SharedData& state, const BrokerNextValidIdEvent& event) {
    {
        std::lock_guard<std::recursive_mutex> lock(state.mutex);
        state.connected = true;
        state.sessionReady = false;
        state.sessionState = RuntimeSessionState::Reconciling;
        state.positionsLoaded = false;
        state.executionsLoaded = false;
        state.nextOrderId.store(event.orderId, std::memory_order_relaxed);
    }
    appendRuntimeJournalEvent("tws_session_ready", {{"nextOrderId", static_cast<long long>(event.orderId)}});
    state.addMessage("Next valid order ID: " + std::to_string(event.orderId));
}

void reduce(SharedData& state, const BrokerManagedAccountsEvent& event) {
    std::string message;
    std::string selectedAccount;
    {
        std::lock_guard<std::recursive_mutex> lock(state.mutex);
        state.managedAccounts = event.accountsList;
        state.selectedAccount = chooseConfiguredAccount(event.accountsList);
        selectedAccount = state.selectedAccount;

        if (!state.selectedAccount.empty()) {
            message = "Managed accounts: " + event.accountsList + " | using account " + state.selectedAccount;
        } else {
            message = "Managed accounts: " + event.accountsList +
                      " | configured account not found: " + std::string(HARDCODED_ACCOUNT);
        }
    }
    appendRuntimeJournalEvent("managed_accounts", {{"accounts", event.accountsList}, {"selectedAccount", selectedAccount}});
    state.addMessage(message);
}

void reduce(SharedData& state, const BrokerTickPriceEvent& event) {
    std::string autoQtyMsg;
    {
        std::lock_guard<std::recursive_mutex> lock(state.mutex);
        if (event.tickerId != state.activeMktDataReqId) {
            return;
        }

        switch (event.field) {
            case 1:
                state.bidPrice = event.price;
                state.lastQuoteUpdate = std::chrono::steady_clock::now();
                break;
            case 2: {
                state.askPrice = event.price;
                state.lastQuoteUpdate = std::chrono::steady_clock::now();
                if (state.pendingWSQuantityCalc && event.price > 0.0) {
                    int maxQty = static_cast<int>(std::floor(state.maxPositionDollars / event.price));
                    if (maxQty < 1) maxQty = 1;
                    state.currentQuantity = maxQty;
                    state.pendingWSQuantityCalc = false;
                    state.wsQuantityUpdated = true;

                    char buf[160];
                    std::snprintf(buf, sizeof(buf),
                                  "WS: Subscribed to %s, quantity set to %d shares ($%.0f / $%.2f)",
                                  state.currentSymbol.c_str(), maxQty,
                                  state.maxPositionDollars, event.price);
                    autoQtyMsg = buf;
                }
                break;
            }
            case 4:
                state.lastPrice = event.price;
                state.lastQuoteUpdate = std::chrono::steady_clock::now();
                break;
            default:
                break;
        }
    }
    if (!autoQtyMsg.empty()) {
        state.addMessage(autoQtyMsg);
    }
}

void reduce(SharedData& state, const BrokerMarketDepthEvent& event) {
    std::lock_guard<std::recursive_mutex> lock(state.mutex);
    if (event.requestId != state.activeDepthReqId) {
        return;
    }

    std::vector<BookLevel>& book = (event.side == 0) ? state.askBook : state.bidBook;
    if (event.operation == 0) {
        BookLevel level{event.price, event.size};
        if (event.position >= static_cast<int>(book.size())) {
            book.push_back(level);
        } else {
            book.insert(book.begin() + event.position, level);
        }
    } else if (event.operation == 1) {
        if (event.position < static_cast<int>(book.size())) {
            book[event.position].price = event.price;
            book[event.position].size = event.size;
        }
    } else if (event.operation == 2) {
        if (event.position < static_cast<int>(book.size())) {
            book.erase(book.begin() + event.position);
        }
    }
}

void reduce(SharedData& state, const BrokerOrderStatusEvent& event) {
    std::string msg;
    std::string reconciliationJournalEvent;
    json reconciliationJournalDetails;
    {
        std::lock_guard<std::recursive_mutex> lock(state.mutex);
        floorNextOrderIdLocked(state, event.orderId);
        auto it = state.orders.find(event.orderId);
        if (it == state.orders.end()) {
            OrderInfo info;
            info.orderId = event.orderId;
            info.status = event.status;
            info.filledQty = event.filled;
            info.remainingQty = event.remaining;
            info.avgFillPrice = event.avgFillPrice;
            info.quantity = std::max(event.filled + event.remaining, info.quantity);
            info.localState = LocalOrderState::NeedsReconciliation;
            state.orders.emplace(event.orderId, info);
            it = state.orders.find(event.orderId);
        }

        OrderInfo& ord = it->second;
        const LocalOrderState previousLocalState = ord.localState;
        const int previousReconciliationAttempts = ord.watchdogs.reconciliationAttempts;
        const std::string previousReconciliationReason = ord.lastReconciliationReason;
        if (ord.isTerminal() && !isTerminalStatus(event.status) && event.status != ord.status) {
            return;
        }
        if (event.status == ord.status &&
            std::abs(event.filled - ord.filledQty) < 1e-9 &&
            std::abs(event.remaining - ord.remainingQty) < 1e-9 &&
            std::abs(event.avgFillPrice - ord.avgFillPrice) < 1e-9) {
            return;
        }
        if (event.filled + 1e-9 < ord.filledQty) {
            return;
        }
        if (std::abs(event.filled - ord.filledQty) < 1e-9 &&
            event.remaining > ord.remainingQty + 1e-9 &&
            !isTerminalStatus(event.status)) {
            return;
        }

        const auto now = std::chrono::steady_clock::now();
        ord.status = event.status;
        ord.quantity = std::max(ord.quantity, event.filled + event.remaining);
        ord.filledQty = event.filled;
        ord.remainingQty = event.remaining;
        noteOrderBrokerCallback(ord, now);
        if (ord.filledQty > 0.0) {
            ord.avgFillPrice = event.avgFillPrice;
            if (ord.firstFillDurationMs < 0.0 && ord.submitTime.time_since_epoch().count() > 0) {
                ord.firstFillDurationMs = std::chrono::duration<double, std::milli>(now - ord.submitTime).count();
            }
        }
        if (ord.isTerminal()) {
            ord.cancelPending = false;
        }
        if (ord.status == "Filled" && ord.fillDurationMs < 0.0 && ord.submitTime.time_since_epoch().count() > 0) {
            ord.fillDurationMs = std::chrono::duration<double, std::milli>(now - ord.submitTime).count();
        }

        if (event.status == "Cancelled" || event.status == "ApiCancelled") {
            ord.cancelPending = false;
            transitionOrderToResolvedState(ord, LocalOrderState::Cancelled, now);
        } else if (event.status == "Rejected") {
            transitionOrderToResolvedState(ord, LocalOrderState::Rejected, now);
        } else if (event.status == "Inactive") {
            transitionOrderToResolvedState(ord, LocalOrderState::Inactive, now);
        } else if (event.status == "Filled" || (event.filled > 0.0 && event.remaining <= 1e-9)) {
            transitionOrderToResolvedState(ord, LocalOrderState::Filled, now);
        } else if (ord.cancelPending || previousLocalState == LocalOrderState::CancelRequested ||
                   previousLocalState == LocalOrderState::AwaitingCancelAck) {
            armCancelAckWatchdog(ord, now, state.cancelAckTimeoutMs);
        } else if (event.filled > 0.0 && event.remaining > 0.0) {
            armPartialFillQuietWatchdog(ord, now, state.partialFillQuietTimeoutMs);
        } else {
            transitionOrderToResolvedState(ord, LocalOrderState::Working, now);
        }

        char buf[256];
        if (ord.fillDurationMs >= 0.0 && ord.firstFillDurationMs >= 0.0) {
            std::snprintf(buf, sizeof(buf),
                          "Order %lld: %s (filled: %.0f @ $%.2f) [first fill: %.0f ms, final fill: %.0f ms]",
                          static_cast<long long>(event.orderId), ord.status.c_str(),
                          ord.filledQty, ord.avgFillPrice, ord.firstFillDurationMs, ord.fillDurationMs);
        } else if (ord.fillDurationMs >= 0.0) {
            std::snprintf(buf, sizeof(buf),
                          "Order %lld: %s (filled: %.0f @ $%.2f) [%.0f ms]",
                          static_cast<long long>(event.orderId), ord.status.c_str(),
                          ord.filledQty, ord.avgFillPrice, ord.fillDurationMs);
        } else if (ord.filledQty > 0.0 && ord.firstFillDurationMs >= 0.0) {
            std::snprintf(buf, sizeof(buf),
                          "Order %lld: %s (filled: %.0f @ $%.2f) [first fill: %.0f ms]",
                          static_cast<long long>(event.orderId), ord.status.c_str(),
                          ord.filledQty, ord.avgFillPrice, ord.firstFillDurationMs);
        } else {
            std::snprintf(buf, sizeof(buf),
                          "Order %lld: %s (filled: %.0f @ $%.2f)",
                          static_cast<long long>(event.orderId), ord.status.c_str(),
                          ord.filledQty, ord.avgFillPrice);
        }
        msg = buf;

        const bool wasAwaitingOrReconciling =
            previousLocalState == LocalOrderState::AwaitingBrokerEcho ||
            previousLocalState == LocalOrderState::AwaitingCancelAck ||
            previousLocalState == LocalOrderState::NeedsReconciliation;
        if (wasAwaitingOrReconciling) {
            if (ord.localState == LocalOrderState::Working || ord.localState == LocalOrderState::PartiallyFilled) {
                reconciliationJournalEvent = "reconcile_resolved_working";
            } else if (ord.localState == LocalOrderState::Filled) {
                reconciliationJournalEvent = "reconcile_resolved_filled";
            } else if (ord.localState == LocalOrderState::Cancelled) {
                reconciliationJournalEvent = "reconcile_resolved_cancelled";
            } else if (ord.localState == LocalOrderState::Rejected) {
                reconciliationJournalEvent = "reconcile_resolved_rejected";
            } else if (ord.localState == LocalOrderState::Inactive) {
                reconciliationJournalEvent = "reconcile_resolved_inactive";
            }
            if (!reconciliationJournalEvent.empty()) {
                reconciliationJournalDetails = {
                    {"orderId", static_cast<long long>(event.orderId)},
                    {"localState", localOrderStateToString(ord.localState)},
                    {"brokerStatus", ord.status},
                    {"reconciliationAttempts", previousReconciliationAttempts},
                    {"reason", previousReconciliationReason}
                };
            }
        }
    }

    state.addMessage(msg);
    if (!reconciliationJournalEvent.empty()) {
        appendRuntimeJournalEvent(reconciliationJournalEvent, reconciliationJournalDetails);
    }
    recordTraceOrderStatus(event.orderId, event.status, event.filled, event.remaining, event.avgFillPrice,
                           event.permId, event.lastFillPrice, event.mktCapPrice);
}

void reduce(SharedData& state, const BrokerOpenOrderEvent& event) {
    std::string reconciliationJournalEvent;
    json reconciliationJournalDetails;
    {
        std::lock_guard<std::recursive_mutex> lock(state.mutex);
        floorNextOrderIdLocked(state, event.orderId);
        auto it = state.orders.find(event.orderId);
        if (it == state.orders.end()) {
            OrderInfo info;
            info.orderId = event.orderId;
            info.account = event.order.account;
            info.symbol = event.contract.symbol;
            info.side = event.order.action;
            info.quantity = DecimalFunctions::decimalToDouble(event.order.totalQuantity);
            info.limitPrice = event.order.lmtPrice;
            info.status = event.orderState.status.empty() ? "Unknown" : event.orderState.status;
            info.filledQty = 0.0;
            info.remainingQty = info.quantity;
            info.localState = LocalOrderState::NeedsReconciliation;
            state.orders.emplace(event.orderId, info);
        }

        it = state.orders.find(event.orderId);
        OrderInfo& ord = it->second;
        const LocalOrderState previousLocalState = ord.localState;
        const int previousReconciliationAttempts = ord.watchdogs.reconciliationAttempts;
        const std::string previousReconciliationReason = ord.lastReconciliationReason;
        const auto now = std::chrono::steady_clock::now();

        if (!ord.isTerminal()) {
            ord.account = event.order.account;
            ord.symbol = event.contract.symbol;
            ord.side = event.order.action;
            ord.quantity = DecimalFunctions::decimalToDouble(event.order.totalQuantity);
            ord.limitPrice = event.order.lmtPrice;
            ord.status = event.orderState.status.empty() ? ord.status : event.orderState.status;
            noteOrderBrokerCallback(ord, now);
            if (ord.cancelPending || previousLocalState == LocalOrderState::CancelRequested ||
                previousLocalState == LocalOrderState::AwaitingCancelAck) {
                armCancelAckWatchdog(ord, now, state.cancelAckTimeoutMs);
            } else if (ord.filledQty > 0.0 && ord.remainingQty > 0.0) {
                armPartialFillQuietWatchdog(ord, now, state.partialFillQuietTimeoutMs);
            } else {
                transitionOrderToResolvedState(ord, LocalOrderState::Working, now);
            }
        }

        const bool wasAwaitingOrReconciling =
            previousLocalState == LocalOrderState::AwaitingBrokerEcho ||
            previousLocalState == LocalOrderState::AwaitingCancelAck ||
            previousLocalState == LocalOrderState::NeedsReconciliation;
        if (wasAwaitingOrReconciling &&
            (ord.localState == LocalOrderState::Working || ord.localState == LocalOrderState::PartiallyFilled)) {
            reconciliationJournalEvent = "reconcile_resolved_working";
            reconciliationJournalDetails = {
                {"orderId", static_cast<long long>(event.orderId)},
                {"localState", localOrderStateToString(ord.localState)},
                {"brokerStatus", ord.status},
                {"reconciliationAttempts", previousReconciliationAttempts},
                {"reason", previousReconciliationReason}
            };
        }
    }

    recordTraceOpenOrder(event.orderId, event.contract, event.order, event.orderState);

    char msg[256];
    std::snprintf(msg, sizeof(msg), "Open order %lld: %s %s %.0f @ %.2f - %s",
                  static_cast<long long>(event.orderId),
                  event.order.action.c_str(),
                  event.contract.symbol.c_str(),
                  DecimalFunctions::decimalToDouble(event.order.totalQuantity),
                  event.order.lmtPrice,
                  event.orderState.status.c_str());
    state.addMessage(msg);
    if (!reconciliationJournalEvent.empty()) {
        appendRuntimeJournalEvent(reconciliationJournalEvent, reconciliationJournalDetails);
    }
}

void reduce(SharedData& state, const BrokerExecutionEvent& event) {
    std::string reconciliationJournalEvent;
    json reconciliationJournalDetails;
    {
        std::lock_guard<std::recursive_mutex> lock(state.mutex);
        const OrderId orderId = static_cast<OrderId>(event.execution.orderId);
        floorNextOrderIdLocked(state, orderId);
        auto [it, inserted] = state.orders.try_emplace(orderId);
        OrderInfo& ord = it->second;
        if (inserted) {
            ord.orderId = orderId;
            ord.symbol = event.contract.symbol;
            ord.side = event.execution.side;
            ord.quantity = DecimalFunctions::decimalToDouble(event.execution.cumQty);
            ord.limitPrice = event.execution.price;
            ord.localState = LocalOrderState::NeedsReconciliation;
        }

        if (!event.execution.execId.empty() && !ord.seenExecIds.insert(event.execution.execId).second) {
            return;
        }

        const LocalOrderState previousLocalState = ord.localState;
        const int previousReconciliationAttempts = ord.watchdogs.reconciliationAttempts;
        const std::string previousReconciliationReason = ord.lastReconciliationReason;
        const auto now = std::chrono::steady_clock::now();
        noteOrderBrokerCallback(ord, now);

        ord.symbol = event.contract.symbol;
        ord.side = event.execution.side;
        ord.filledQty = std::max(ord.filledQty, DecimalFunctions::decimalToDouble(event.execution.cumQty));
        if (ord.quantity < ord.filledQty) {
            ord.quantity = ord.filledQty;
        }
        ord.remainingQty = std::max(0.0, ord.quantity - ord.filledQty);
        if (event.execution.avgPrice > 0.0) {
            ord.avgFillPrice = event.execution.avgPrice;
        } else if (event.execution.price > 0.0) {
            ord.avgFillPrice = event.execution.price;
        }
        if (ord.firstFillDurationMs < 0.0 && ord.submitTime.time_since_epoch().count() > 0) {
            ord.firstFillDurationMs = std::chrono::duration<double, std::milli>(now - ord.submitTime).count();
        }
        if (ord.cancelPending) {
            armCancelAckWatchdog(ord, now, state.cancelAckTimeoutMs);
        } else if (ord.remainingQty > 0.0) {
            ord.status = "PartiallyFilled";
            armPartialFillQuietWatchdog(ord, now, state.partialFillQuietTimeoutMs);
        } else {
            ord.status = "Filled";
            if (ord.fillDurationMs < 0.0 && ord.submitTime.time_since_epoch().count() > 0) {
                ord.fillDurationMs = std::chrono::duration<double, std::milli>(now - ord.submitTime).count();
            }
            transitionOrderToResolvedState(ord, LocalOrderState::Filled, now);
        }

        const bool wasAwaitingOrReconciling =
            previousLocalState == LocalOrderState::AwaitingBrokerEcho ||
            previousLocalState == LocalOrderState::AwaitingCancelAck ||
            previousLocalState == LocalOrderState::NeedsReconciliation;
        if (wasAwaitingOrReconciling) {
            reconciliationJournalEvent = (ord.localState == LocalOrderState::Filled)
                ? "reconcile_resolved_filled"
                : "reconcile_resolved_working";
            reconciliationJournalDetails = {
                {"orderId", static_cast<long long>(orderId)},
                {"localState", localOrderStateToString(ord.localState)},
                {"reconciliationAttempts", previousReconciliationAttempts},
                {"reason", previousReconciliationReason}
            };
        }
    }

    recordTraceExecution(event.contract, event.execution);
    appendRuntimeJournalEvent("execution_seen", {
        {"orderId", static_cast<long long>(event.execution.orderId)},
        {"symbol", event.contract.symbol},
        {"execId", event.execution.execId},
        {"shares", DecimalFunctions::decimalToDouble(event.execution.shares)},
        {"price", event.execution.price}
    });

    char msg[256];
    std::snprintf(msg, sizeof(msg),
                  "Execution %s: order %lld %s %.0f @ %.2f (cum %.0f)",
                  event.execution.execId.c_str(), static_cast<long long>(event.execution.orderId),
                  event.contract.symbol.c_str(),
                  DecimalFunctions::decimalToDouble(event.execution.shares), event.execution.price,
                  DecimalFunctions::decimalToDouble(event.execution.cumQty));
    state.addMessage(msg);
    if (!reconciliationJournalEvent.empty()) {
        appendRuntimeJournalEvent(reconciliationJournalEvent, reconciliationJournalDetails);
    }
}

void reduce(SharedData& state, const BrokerExecutionsLoadedEvent&) {
    bool sessionReadyNow = false;
    {
        std::lock_guard<std::recursive_mutex> lock(state.mutex);
        state.executionsLoaded = true;
        if (state.positionsLoaded) {
            state.sessionReady = true;
            state.sessionState = RuntimeSessionState::SessionReady;
            sessionReadyNow = true;
        }
    }
    appendRuntimeJournalEvent("executions_loaded");
    if (sessionReadyNow) {
        state.addMessage("TWS session is ready after reconciliation");
    }
}

void reduce(SharedData& state, const BrokerCommissionEvent& event) {
    recordTraceCommission(event.commissionReport);
    appendRuntimeJournalEvent("commission_seen", {
        {"execId", event.commissionReport.execId},
        {"commission", commissionValue(event.commissionReport)},
        {"currency", event.commissionReport.currency}
    });

    char msg[256];
    std::snprintf(msg, sizeof(msg), "Commission %s: %.4f %s",
                  event.commissionReport.execId.c_str(), commissionValue(event.commissionReport),
                  event.commissionReport.currency.c_str());
    state.addMessage(msg);
}

void reduce(SharedData& state, const BrokerErrorEvent& event) {
    std::string reconciliationJournalEvent;
    json reconciliationJournalDetails;
    {
        std::lock_guard<std::recursive_mutex> lock(state.mutex);
        if (event.errorCode == 300 && state.suppressedMktDataCancelIds.erase(event.id) > 0) return;
        if (event.errorCode == 310 && state.suppressedMktDepthCancelIds.erase(event.id) > 0) return;
    }

    char msg[512];
    const bool isInfo = (event.errorCode >= 2100 && event.errorCode <= 2199);
    const bool isWarning = (event.errorCode == 399);
    if (isInfo) {
        std::snprintf(msg, sizeof(msg), "Info [%d] code=%d: %s", event.id, event.errorCode, event.errorString.c_str());
    } else if (isWarning) {
        std::snprintf(msg, sizeof(msg), "Warning [%d] code=%d: %s", event.id, event.errorCode, event.errorString.c_str());
    } else {
        std::snprintf(msg, sizeof(msg), "Error [%d] code=%d: %s", event.id, event.errorCode, event.errorString.c_str());
    }

    if (event.errorCode == 200) {
        std::string symbolContext;
        {
            std::lock_guard<std::recursive_mutex> lock(state.mutex);
            symbolContext = state.currentSymbol;
            if (event.id == state.activeMktDataReqId) {
                state.activeMktDataReqId = 0;
                state.bidPrice = 0.0;
                state.askPrice = 0.0;
                state.lastPrice = 0.0;
            }
            if (event.id == state.activeDepthReqId) {
                state.activeDepthReqId = 0;
                state.depthSubscribed = false;
                state.askBook.clear();
                state.bidBook.clear();
            }
        }
        state.addMessage(std::string(msg) + " (symbol: " + symbolContext + ")");
    } else {
        state.addMessage(msg);
    }

    if (event.id > 0 && (event.errorCode == 201 || event.errorCode == 202 || event.errorCode == 10147)) {
        std::lock_guard<std::recursive_mutex> lock(state.mutex);
        auto it = state.orders.find(static_cast<OrderId>(event.id));
        if (it != state.orders.end()) {
            const LocalOrderState previousLocalState = it->second.localState;
            const int previousReconciliationAttempts = it->second.watchdogs.reconciliationAttempts;
            const std::string previousReconciliationReason = it->second.lastReconciliationReason;
            const auto now = std::chrono::steady_clock::now();
            if (event.errorCode == 201) {
                it->second.status = "Rejected";
                transitionOrderToResolvedState(it->second, LocalOrderState::Rejected, now);
                reconciliationJournalEvent = "reconcile_resolved_rejected";
            } else {
                it->second.status = "Cancelled";
                it->second.cancelPending = false;
                transitionOrderToResolvedState(it->second, LocalOrderState::Cancelled, now);
                reconciliationJournalEvent = "reconcile_resolved_cancelled";
            }
            const bool wasAwaitingOrReconciling =
                previousLocalState == LocalOrderState::AwaitingBrokerEcho ||
                previousLocalState == LocalOrderState::AwaitingCancelAck ||
                previousLocalState == LocalOrderState::NeedsReconciliation;
            if (!wasAwaitingOrReconciling) {
                reconciliationJournalEvent.clear();
            } else if (!reconciliationJournalEvent.empty()) {
                reconciliationJournalDetails = {
                    {"orderId", static_cast<long long>(event.id)},
                    {"localState", localOrderStateToString(it->second.localState)},
                    {"reconciliationAttempts", previousReconciliationAttempts},
                    {"reason", previousReconciliationReason}
                };
            }
        }
    }

    recordTraceError(event.id, event.errorCode, event.errorString);
    appendRuntimeJournalEvent("broker_error", {
        {"id", event.id},
        {"code", event.errorCode},
        {"message", event.errorString}
    });
    if (!reconciliationJournalEvent.empty()) {
        appendRuntimeJournalEvent(reconciliationJournalEvent, reconciliationJournalDetails);
    }
}

void reduce(SharedData& state, const BrokerPositionEvent& event) {
    {
        std::lock_guard<std::recursive_mutex> lock(state.mutex);
        if (event.quantity != 0.0) {
            PositionInfo info;
            info.account = event.account;
            info.symbol = event.contract.symbol;
            info.quantity = event.quantity;
            info.avgCost = event.avgCost;
            state.positions[makePositionKey(event.account, event.contract.symbol)] = info;
        } else {
            state.positions.erase(makePositionKey(event.account, event.contract.symbol));
        }
    }
}

void reduce(SharedData& state, const BrokerPositionsLoadedEvent&) {
    bool sessionReadyNow = false;
    {
        std::lock_guard<std::recursive_mutex> lock(state.mutex);
        state.positionsLoaded = true;
        if (state.executionsLoaded) {
            state.sessionReady = true;
            state.sessionState = RuntimeSessionState::SessionReady;
            sessionReadyNow = true;
        }
    }
    appendRuntimeJournalEvent("positions_loaded");
    state.addMessage("Positions loaded");
    if (sessionReadyNow) {
        state.addMessage("TWS session is ready after reconciliation");
    }
}

struct RuntimeSessionStateChangedEvent {
    RuntimeSessionState state = RuntimeSessionState::Disconnected;
};

bool reduce(SharedData& state, const RuntimeSessionStateChangedEvent& event) {
    std::lock_guard<std::recursive_mutex> lock(state.mutex);
    if (state.sessionState == event.state) {
        return false;
    }
    state.sessionState = event.state;
    return true;
}

struct WebSocketAuthTokenEnsuredEvent {};

std::string reduce(SharedData& state, const WebSocketAuthTokenEnsuredEvent&) {
    std::lock_guard<std::recursive_mutex> lock(state.mutex);
    if (state.websocketAuthToken.empty()) {
        state.websocketAuthToken = randomHexToken(16);
    }
    return state.websocketAuthToken;
}

struct WebSocketOrderRateLimitConsumedEvent {
    std::chrono::steady_clock::time_point requestTime{};
};

struct WebSocketOrderRateLimitResult {
    bool allowed = false;
    std::string error;
};

WebSocketOrderRateLimitResult reduce(SharedData& state, const WebSocketOrderRateLimitConsumedEvent& event) {
    std::lock_guard<std::recursive_mutex> lock(state.mutex);
    pruneWebSocketStateLocked(state, event.requestTime);
    if (static_cast<int>(state.recentWsOrderTimestamps.size()) >= kMaxWebSocketOrdersPerWindow) {
        return {false, "Too many WebSocket order requests; slow down"};
    }
    state.recentWsOrderTimestamps.push_back(event.requestTime);
    return {true, {}};
}

struct WebSocketIdempotencyKeyReservedEvent {
    std::string key;
    std::chrono::steady_clock::time_point requestTime{};
};

struct WebSocketIdempotencyReservationResult {
    bool reserved = false;
    std::string error;
};

WebSocketIdempotencyReservationResult reduce(SharedData& state,
                                             const WebSocketIdempotencyKeyReservedEvent& event) {
    if (event.key.empty()) {
        return {false, "Missing required string field: idempotencyKey"};
    }

    std::lock_guard<std::recursive_mutex> lock(state.mutex);
    pruneWebSocketStateLocked(state, event.requestTime);
    const auto it = state.recentWsIdempotencyKeys.find(event.key);
    if (it != state.recentWsIdempotencyKeys.end()) {
        return {false, "Duplicate idempotencyKey"};
    }
    state.recentWsIdempotencyKeys[event.key] = event.requestTime;
    return {true, {}};
}

struct OrdersPendingCancelMarkedEvent {
    std::vector<OrderId> orderIds;
};

std::vector<OrderId> reduce(SharedData& state, const OrdersPendingCancelMarkedEvent& event) {
    std::vector<OrderId> marked;
    std::lock_guard<std::recursive_mutex> lock(state.mutex);
    marked.reserve(event.orderIds.size());
    for (OrderId id : event.orderIds) {
        auto it = state.orders.find(id);
        if (it != state.orders.end() && !it->second.isTerminal() && !it->second.cancelPending) {
            it->second.cancelPending = true;
            it->second.localState = LocalOrderState::CancelRequested;
            marked.push_back(id);
        }
    }
    return marked;
}

struct AllPendingOrdersMarkedForCancelEvent {};

std::vector<OrderId> reduce(SharedData& state, const AllPendingOrdersMarkedForCancelEvent&) {
    std::vector<OrderId> pendingOrders;
    std::lock_guard<std::recursive_mutex> lock(state.mutex);
    pendingOrders.reserve(state.orders.size());
    for (auto& [id, order] : state.orders) {
        if (!order.isTerminal() && !order.cancelPending) {
            order.cancelPending = true;
            order.localState = LocalOrderState::CancelRequested;
            pendingOrders.push_back(id);
        }
    }
    return pendingOrders;
}

struct CancelRequestsSentEvent {
    std::vector<OrderId> orderIds;
    std::chrono::steady_clock::time_point requestTime{};
};

void reduce(SharedData& state, const CancelRequestsSentEvent& event) {
    std::lock_guard<std::recursive_mutex> lock(state.mutex);
    for (const OrderId id : event.orderIds) {
        auto it = state.orders.find(id);
        if (it == state.orders.end() || it->second.isTerminal()) {
            continue;
        }
        armCancelAckWatchdog(it->second, event.requestTime, state.cancelAckTimeoutMs);
    }
}

struct SubmitOrderStateReservedEvent {
    std::string fingerprint;
    std::chrono::steady_clock::time_point requestTime{};
};

struct SubmitOrderStateReservationResult {
    bool allowed = false;
    std::string error;
    OrderId orderId = 0;
};

SubmitOrderStateReservationResult reduce(SharedData& state, const SubmitOrderStateReservedEvent& event) {
    std::lock_guard<std::recursive_mutex> lock(state.mutex);
    if (state.lastSubmitFingerprint == event.fingerprint &&
        hasTime(state.lastSubmitTime) &&
        (event.requestTime - state.lastSubmitTime) <= kRecentSubmitWindow) {
        return {false, "Duplicate order suppressed", 0};
    }

    const OrderId nextId = state.nextOrderId.load(std::memory_order_relaxed);
    if (nextId <= 0) {
        return {false, "No valid order ID yet", 0};
    }

    const OrderId orderId = state.nextOrderId.fetch_add(1, std::memory_order_relaxed);
    if (orderId <= 0) {
        return {false, "Failed to allocate order ID", 0};
    }

    state.lastSubmitFingerprint = event.fingerprint;
    state.lastSubmitTime = event.requestTime;
    return {true, {}, orderId};
}

struct LocalOrderStoredEvent {
    OrderId orderId = 0;
    std::string account;
    std::string symbol;
    std::string side;
    double quantity = 0.0;
    double limitPrice = 0.0;
    std::chrono::steady_clock::time_point submitTime{};
};

void reduce(SharedData& state, const LocalOrderStoredEvent& event) {
    std::lock_guard<std::recursive_mutex> lock(state.mutex);
    OrderInfo info;
    info.orderId = event.orderId;
    info.account = event.account;
    info.symbol = event.symbol;
    info.side = event.side;
    info.quantity = event.quantity;
    info.limitPrice = event.limitPrice;
    info.status = "Submitted";
    info.submitTime = event.submitTime;
    info.filledQty = 0.0;
    info.remainingQty = event.quantity;
    info.localState = LocalOrderState::AwaitingBrokerEcho;
    armBrokerEchoWatchdog(info, event.submitTime, state.brokerEchoTimeoutMs);
    state.orders[event.orderId] = info;
}

struct OrderWatchdogSweepEvent {
    std::chrono::steady_clock::time_point now{};
};

OrderWatchdogSweepResult reduce(SharedData& state, const OrderWatchdogSweepEvent& event) {
    OrderWatchdogSweepResult result;
    std::lock_guard<std::recursive_mutex> lock(state.mutex);
    for (auto& [orderId, order] : state.orders) {
        if (order.isTerminal()) {
            continue;
        }

        auto beginReconciliation = [&](const std::string& reason) {
            const int attempts = beginOrderReconciliation(order, reason, event.now, true);
            result.reconciliationOrders.push_back({orderId, reason, attempts});
        };

        auto requireManualReview = [&](const std::string& reason) {
            order.localState = LocalOrderState::NeedsManualReview;
            order.lastReconciliationReason = reason;
            order.lastReconciliationTime = event.now;
            disarmOrderWatchdogs(order);
            result.manualReviewOrders.push_back({orderId, reason, order.watchdogs.reconciliationAttempts});
        };

        if (order.localState == LocalOrderState::AwaitingBrokerEcho &&
            order.watchdogs.brokerEchoArmed &&
            event.now >= order.watchdogs.brokerEchoDeadline) {
            beginReconciliation("broker_echo_timeout");
            continue;
        }

        if (order.localState == LocalOrderState::AwaitingCancelAck &&
            order.watchdogs.cancelAckArmed &&
            event.now >= order.watchdogs.cancelAckDeadline) {
            beginReconciliation("cancel_ack_timeout");
            continue;
        }

        if (order.localState == LocalOrderState::PartiallyFilled &&
            order.watchdogs.partialFillQuietArmed &&
            event.now >= order.watchdogs.partialFillQuietDeadline) {
            beginReconciliation("partial_fill_quiet_timeout");
            continue;
        }

        if (order.localState == LocalOrderState::NeedsReconciliation &&
            order.watchdogs.brokerEchoArmed &&
            event.now >= order.watchdogs.brokerEchoDeadline) {
            if (order.watchdogs.reconciliationAttempts >= kMaxReconciliationAttempts) {
                requireManualReview("reconciliation_retry_exhausted");
            } else {
                beginReconciliation("reconciliation_retry_timeout");
            }
        }
    }
    return result;
}

struct ManualReconciliationRequestedEvent {
    std::vector<OrderId> orderIds;
    std::string reason;
    std::chrono::steady_clock::time_point requestTime{};
};

OrderWatchdogSweepResult reduce(SharedData& state, const ManualReconciliationRequestedEvent& event) {
    OrderWatchdogSweepResult result;
    std::lock_guard<std::recursive_mutex> lock(state.mutex);
    for (const OrderId orderId : event.orderIds) {
        auto it = state.orders.find(orderId);
        if (it == state.orders.end() || it->second.isTerminal()) {
            continue;
        }

        const int attempts = beginOrderReconciliation(it->second, event.reason, event.requestTime, true);
        result.reconciliationOrders.push_back({orderId, event.reason, attempts});
    }
    return result;
}

struct ManualReviewAcknowledgedEvent {
    std::vector<OrderId> orderIds;
    std::chrono::steady_clock::time_point acknowledgeTime{};
};

std::vector<OrderId> reduce(SharedData& state, const ManualReviewAcknowledgedEvent& event) {
    std::vector<OrderId> acknowledged;
    std::lock_guard<std::recursive_mutex> lock(state.mutex);
    for (const OrderId orderId : event.orderIds) {
        auto it = state.orders.find(orderId);
        if (it == state.orders.end() || it->second.localState != LocalOrderState::NeedsManualReview) {
            continue;
        }
        it->second.manualReviewAcknowledged = true;
        it->second.manualReviewAcknowledgedTime = event.acknowledgeTime;
        acknowledged.push_back(orderId);
    }
    return acknowledged;
}

} // namespace trading_engine

void SharedData::addMessage(const std::string& msg) {
    invokeSharedDataMutation([&]() {
        std::lock_guard<std::recursive_mutex> lock(mutex);
        messages.push_back(msg);
        while (messages.size() > MAX_MESSAGES) {
            messages.pop_front();
        }
        ++messagesVersion;
    });
    requestUiInvalidation();
}

std::string toUpperCase(const std::string& str) {
    std::string result = str;
    std::transform(result.begin(), result.end(), result.begin(),
                   [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
    return result;
}

std::string chooseConfiguredAccount(const std::string& accountsCsv) {
    const auto accounts = splitCsv(accountsCsv);
    for (const auto& acct : accounts) {
        if (acct == HARDCODED_ACCOUNT) return acct;
    }
    return {};
}

std::string makePositionKey(const std::string& account, const std::string& symbol) {
    return account + "|" + symbol;
}

std::string runtimeSessionStateToString(RuntimeSessionState state) {
    switch (state) {
        case RuntimeSessionState::Disconnected: return "Disconnected";
        case RuntimeSessionState::Connecting: return "Connecting";
        case RuntimeSessionState::SocketConnected: return "Socket connected";
        case RuntimeSessionState::Reconciling: return "Reconciling";
        case RuntimeSessionState::SessionReady: return "Ready";
        default: return "Unknown";
    }
}

std::string localOrderStateToString(LocalOrderState state) {
    switch (state) {
        case LocalOrderState::IntentAccepted: return "Intent accepted";
        case LocalOrderState::SentToBroker: return "Sent to broker";
        case LocalOrderState::AwaitingBrokerEcho: return "Awaiting broker echo";
        case LocalOrderState::Working: return "Working";
        case LocalOrderState::PartiallyFilled: return "Partially filled";
        case LocalOrderState::CancelRequested: return "Cancel requested";
        case LocalOrderState::AwaitingCancelAck: return "Awaiting cancel ack";
        case LocalOrderState::Filled: return "Filled";
        case LocalOrderState::Cancelled: return "Cancelled";
        case LocalOrderState::Rejected: return "Rejected";
        case LocalOrderState::Inactive: return "Inactive";
        case LocalOrderState::NeedsReconciliation: return "Reconciling";
        case LocalOrderState::NeedsManualReview: return "Manual review";
        default: return "Unknown";
    }
}

void setRuntimeSessionState(RuntimeSessionState state) {
    const bool changed = invokeSharedDataMutation([&]() {
        return trading_engine::reduce(appState(), trading_engine::RuntimeSessionStateChangedEvent{state});
    });
    if (changed) {
        macLogInfo("runtime", "Session state changed: " + runtimeSessionStateToString(state));
        appendRuntimeJournalEvent("session_state_changed", {{"state", runtimeSessionStateToString(state)}});
        requestUiInvalidation();
    }
}

int allocateReqId() {
    SharedData& state = appState();
    return state.nextReqId.fetch_add(1, std::memory_order_relaxed);
}

int toShareCount(double qty) {
    return static_cast<int>(std::llround(qty));
}

bool isTerminalStatus(const std::string& status) {
    return (status == "Filled" || status == "Cancelled" || status == "ApiCancelled" ||
            status == "Rejected" || status == "Inactive");
}

double outstandingOrderQty(const OrderInfo& order) {
    if (order.remainingQty > 0.0) return order.remainingQty;
    return std::max(0.0, order.quantity - order.filledQty);
}

double availableLongToCloseUnlocked(const SharedData& state, const std::string& account, const std::string& symbol) {
    double longPos = 0.0;
    auto posIt = state.positions.find(makePositionKey(account, symbol));
    if (posIt != state.positions.end()) {
        longPos = std::max(0.0, posIt->second.quantity);
    }

    double workingSellQty = 0.0;
    for (const auto& [id, ord] : state.orders) {
        (void)id;
        if (ord.symbol != symbol) continue;
        if (ord.side != "SELL") continue;
        if (ord.isTerminal()) continue;
        if (!ord.account.empty() && ord.account != account) continue;
        workingSellQty += outstandingOrderQty(ord);
    }

    const double available = longPos - workingSellQty;
    return available > 0.0 ? available : 0.0;
}

double calculateOpenBuyExposureUnlocked(const SharedData& state, const std::string& account) {
    double exposure = 0.0;
    for (const auto& [id, ord] : state.orders) {
        (void)id;
        if (!ord.account.empty() && ord.account != account) continue;
        if (ord.side != "BUY") continue;
        if (ord.isTerminal()) continue;
        exposure += outstandingOrderQty(ord) * ord.limitPrice;
    }
    return exposure;
}

double calculatePositionMarketValueUnlocked(const SharedData& state, const std::string& account, const std::string& symbol) {
    const auto posIt = state.positions.find(makePositionKey(account, symbol));
    if (posIt == state.positions.end()) {
        return 0.0;
    }
    const double qty = std::max(0.0, posIt->second.quantity);
    if (qty <= 0.0) {
        return 0.0;
    }

    double price = 0.0;
    if (symbol == state.currentSymbol) {
        if (state.lastPrice > 0.0) {
            price = state.lastPrice;
        } else if (state.askPrice > 0.0) {
            price = state.askPrice;
        } else if (state.bidPrice > 0.0) {
            price = state.bidPrice;
        }
    }
    if (price <= 0.0) {
        price = posIt->second.avgCost;
    }
    return qty * price;
}

double calculatePositionMarketValueUnlocked(const ImmutableSharedDataSnapshot& state,
                                            const std::string& account,
                                            const std::string& symbol) {
    const auto posIt = state.positions.find(makePositionKey(account, symbol));
    if (posIt == state.positions.end()) {
        return 0.0;
    }
    const double qty = std::max(0.0, posIt->second.quantity);
    if (qty <= 0.0) {
        return 0.0;
    }

    double price = 0.0;
    if (symbol == state.currentSymbol) {
        if (state.lastPrice > 0.0) {
            price = state.lastPrice;
        } else if (state.askPrice > 0.0) {
            price = state.askPrice;
        } else if (state.bidPrice > 0.0) {
            price = state.bidPrice;
        }
    }
    if (price <= 0.0) {
        price = posIt->second.avgCost;
    }
    return qty * price;
}

double availableLongToCloseUnlocked(const ImmutableSharedDataSnapshot& state,
                                    const std::string& account,
                                    const std::string& symbol) {
    double longPos = 0.0;
    auto posIt = state.positions.find(makePositionKey(account, symbol));
    if (posIt != state.positions.end()) {
        longPos = std::max(0.0, posIt->second.quantity);
    }

    double workingSellQty = 0.0;
    for (const auto& [id, ord] : state.orders) {
        (void)id;
        if (ord.symbol != symbol) continue;
        if (ord.side != "SELL") continue;
        if (ord.isTerminal()) continue;
        if (!ord.account.empty() && ord.account != account) continue;
        workingSellQty += outstandingOrderQty(ord);
    }

    const double available = longPos - workingSellQty;
    return available > 0.0 ? available : 0.0;
}

double calculateOpenBuyExposureUnlocked(const ImmutableSharedDataSnapshot& state, const std::string& account) {
    double exposure = 0.0;
    for (const auto& [id, ord] : state.orders) {
        (void)id;
        if (!ord.account.empty() && ord.account != account) continue;
        if (ord.side != "BUY") continue;
        if (ord.isTerminal()) continue;
        exposure += outstandingOrderQty(ord) * ord.limitPrice;
    }
    return exposure;
}

void refreshMessagesCacheLocked(SharedData& state) {
    if (state.messagesCacheVersion == state.messagesVersion) {
        return;
    }

    size_t totalChars = 0;
    for (const auto& message : state.messages) {
        totalChars += message.size() + 1;
    }
    state.messagesCache.clear();
    state.messagesCache.reserve(totalChars);
    for (auto it = state.messages.rbegin(); it != state.messages.rend(); ++it) {
        state.messagesCache += *it;
        state.messagesCache.push_back('\n');
    }
    state.messagesCacheVersion = state.messagesVersion;
}

std::shared_ptr<const ImmutableSharedDataSnapshot> buildImmutableSharedDataSnapshotLocked(SharedData& state) {
    refreshMessagesCacheLocked(state);

    auto snapshot = std::make_shared<ImmutableSharedDataSnapshot>();
    snapshot->connected = state.connected;
    snapshot->sessionReady = state.sessionReady;
    snapshot->sessionState = state.sessionState;
    snapshot->managedAccounts = state.managedAccounts;
    snapshot->selectedAccount = state.selectedAccount;

    snapshot->currentSymbol = state.currentSymbol;
    snapshot->bidPrice = state.bidPrice;
    snapshot->askPrice = state.askPrice;
    snapshot->lastPrice = state.lastPrice;
    snapshot->askBook = state.askBook;
    snapshot->bidBook = state.bidBook;
    snapshot->depthSubscribed = state.depthSubscribed;

    snapshot->activeMktDataReqId = state.activeMktDataReqId;
    snapshot->activeDepthReqId = state.activeDepthReqId;

    snapshot->currentQuantity = state.currentQuantity;
    snapshot->priceBuffer = state.priceBuffer;
    snapshot->maxPositionDollars = state.maxPositionDollars;
    snapshot->maxOrderNotional = state.maxOrderNotional;
    snapshot->maxOpenNotional = state.maxOpenNotional;
    snapshot->staleQuoteThresholdMs = state.staleQuoteThresholdMs;
    snapshot->brokerEchoTimeoutMs = state.brokerEchoTimeoutMs;
    snapshot->cancelAckTimeoutMs = state.cancelAckTimeoutMs;
    snapshot->partialFillQuietTimeoutMs = state.partialFillQuietTimeoutMs;
    snapshot->controllerArmMode = state.controllerArmMode;
    snapshot->controllerArmed = state.controllerArmed;
    snapshot->tradingKillSwitch = state.tradingKillSwitch;

    snapshot->twsHost = state.twsHost;
    snapshot->twsPort = state.twsPort;
    snapshot->twsClientId = state.twsClientId;
    snapshot->websocketAuthToken = state.websocketAuthToken;
    snapshot->websocketEnabled = state.websocketEnabled;
    snapshot->controllerEnabled = state.controllerEnabled;
    snapshot->appSessionId = state.appSessionId;
    snapshot->runtimeSessionId = state.runtimeSessionId;
    snapshot->startupRecoveryBanner = state.startupRecoveryBanner;

    snapshot->pendingSubscribeSymbol = state.pendingSubscribeSymbol;
    snapshot->hasPendingSubscribe = state.hasPendingSubscribe;
    snapshot->pendingWSQuantityCalc = state.pendingWSQuantityCalc;
    snapshot->wsQuantityUpdated = state.wsQuantityUpdated;

    snapshot->positionsLoaded = state.positionsLoaded;
    snapshot->executionsLoaded = state.executionsLoaded;
    snapshot->wsServerRunning = state.wsServerRunning.load(std::memory_order_relaxed);
    snapshot->wsConnectedClients = state.wsConnectedClients.load(std::memory_order_relaxed);
    snapshot->controllerConnected = state.controllerConnected.load(std::memory_order_relaxed);
    snapshot->controllerDeviceName = state.controllerDeviceName;
    snapshot->controllerLockedDeviceName = state.controllerLockedDeviceName;
    snapshot->lastQuoteUpdate = state.lastQuoteUpdate;

    snapshot->orders = state.orders;
    snapshot->positions = state.positions;
    snapshot->messagesText = state.messagesCache;
    snapshot->messagesVersion = state.messagesVersion;
    snapshot->traces = state.traces;
    snapshot->traceRecency = state.traceRecency;
    snapshot->traceIdByOrderId = state.traceIdByOrderId;
    snapshot->latestTraceId = state.latestTraceId;
    snapshot->bridgeOutbox = state.bridgeOutbox;
    snapshot->bridgeOutboxLossCount = state.bridgeOutboxLossCount;
    snapshot->lastBridgeSourceSeq = state.lastBridgeSourceSeq;
    snapshot->bridgeRecoveredPendingCount = state.bridgeRecoveredPendingCount;
    snapshot->bridgeRecoveredLossCount = state.bridgeRecoveredLossCount;
    snapshot->bridgeRecoveredLastSourceSeq = state.bridgeRecoveredLastSourceSeq;
    snapshot->bridgeFallbackState = state.bridgeFallbackState;
    snapshot->bridgeFallbackReason = state.bridgeFallbackReason;
    snapshot->bridgeRecoveryRequired = state.bridgeRecoveryRequired;
    return snapshot;
}

void storePublishedSharedDataSnapshot(std::shared_ptr<const ImmutableSharedDataSnapshot> snapshot) {
    std::atomic_store_explicit(&publishedSharedDataSnapshotSlot(), std::move(snapshot), std::memory_order_release);
}

std::shared_ptr<const ImmutableSharedDataSnapshot> loadPublishedSharedDataSnapshot() {
    return std::atomic_load_explicit(&publishedSharedDataSnapshotSlot(), std::memory_order_acquire);
}

void publishSharedDataSnapshotLocked(SharedData& state) {
    storePublishedSharedDataSnapshot(buildImmutableSharedDataSnapshotLocked(state));
}

std::shared_ptr<const ImmutableSharedDataSnapshot> ensurePublishedSharedDataSnapshot() {
    auto snapshot = loadPublishedSharedDataSnapshot();
    if (snapshot) {
        return snapshot;
    }

    SharedData& state = appState();
    std::lock_guard<std::recursive_mutex> lock(state.mutex);
    publishSharedDataSnapshotLocked(state);
    return loadPublishedSharedDataSnapshot();
}

TradeTraceListItem makeTradeTraceListItem(const TradeTrace& trace) {
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
    oss << " | " << (trace.source.empty() ? "Unknown" : trace.source)
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

double calculateOpenBuyExposureUnlocked(const std::string& account) {
    const auto published = ensurePublishedSharedDataSnapshot();
    return calculateOpenBuyExposureUnlocked(*published, account);
}

double calculatePositionMarketValueUnlocked(const std::string& account, const std::string& symbol) {
    const auto published = ensurePublishedSharedDataSnapshot();
    return calculatePositionMarketValueUnlocked(*published, account, symbol);
}

double availableLongToCloseUnlocked(const std::string& account, const std::string& symbol) {
    const auto published = ensurePublishedSharedDataSnapshot();
    return availableLongToCloseUnlocked(*published, account, symbol);
}

UiStatusSnapshot captureUiStatusSnapshot() {
    UiStatusSnapshot snapshot;
    const auto published = ensurePublishedSharedDataSnapshot();
    snapshot.connected = published->connected;
    snapshot.sessionReady = published->sessionReady;
    snapshot.sessionStateText = runtimeSessionStateToString(published->sessionState);
    snapshot.wsServerRunning = published->wsServerRunning;
    snapshot.websocketEnabled = published->websocketEnabled;
    snapshot.controllerConnected = published->controllerConnected;
    snapshot.controllerEnabled = published->controllerEnabled;
    snapshot.controllerArmed = published->controllerArmed;
    snapshot.tradingKillSwitch = published->tradingKillSwitch;
    snapshot.wsConnectedClients = published->wsConnectedClients;
    snapshot.controllerDeviceName = published->controllerDeviceName;
    snapshot.controllerLockedDeviceName = published->controllerLockedDeviceName;
    snapshot.websocketAuthToken = published->websocketAuthToken;
    snapshot.startupRecoveryBanner = published->startupRecoveryBanner;

    if (!published->selectedAccount.empty()) {
        snapshot.accountText = published->selectedAccount;
    } else if (!published->managedAccounts.empty()) {
        snapshot.accountText = "<configured account not found>";
    } else {
        snapshot.accountText = "<waiting for managedAccounts>";
    }
    return snapshot;
}

PendingUiSyncUpdate consumePendingUiSyncUpdate() {
    return invokeSharedDataMutation([&]() {
        PendingUiSyncUpdate update;
        SharedData& state = appState();
        std::lock_guard<std::recursive_mutex> lock(state.mutex);
        if (state.hasPendingSubscribe) {
            update.hasPendingSubscribe = true;
            update.pendingSubscribeSymbol = state.pendingSubscribeSymbol;
            update.quantityInput = state.currentQuantity;
            state.hasPendingSubscribe = false;
        }
        if (state.wsQuantityUpdated) {
            update.quantityUpdated = true;
            update.quantityInput = state.currentQuantity;
            state.wsQuantityUpdated = false;
        }
        return update;
    });
}

void consumeGuiSyncUpdates(std::string& symbolInput,
                           std::string& subscribedSymbol,
                           bool& subscribed,
                           int& quantityInput) {
    const PendingUiSyncUpdate update = consumePendingUiSyncUpdate();
    if (update.hasPendingSubscribe) {
        symbolInput = update.pendingSubscribeSymbol;
        subscribedSymbol = update.pendingSubscribeSymbol;
        subscribed = true;
        quantityInput = update.quantityInput;
    }
    if (update.quantityUpdated) {
        quantityInput = update.quantityInput;
    }
}

void syncSharedGuiInputs(int quantityInput, double priceBuffer, double maxPositionDollars) {
    invokeSharedDataMutation([&]() {
        trading_engine::reduce(appState(), trading_engine::GuiInputsSyncedEvent{
            quantityInput,
            priceBuffer,
            maxPositionDollars
        });
    });
}

SymbolUiSnapshot captureSymbolUiSnapshot(const std::string& subscribedSymbol) {
    SymbolUiSnapshot snapshot;
    const auto published = ensurePublishedSharedDataSnapshot();

    snapshot.canTrade = published->connected && published->sessionReady && !published->selectedAccount.empty();
    snapshot.bidPrice = published->bidPrice;
    snapshot.askPrice = published->askPrice;
    snapshot.lastPrice = published->lastPrice;
    snapshot.askBook = published->askBook;
    snapshot.bidBook = published->bidBook;
    snapshot.openBuyExposure = calculateOpenBuyExposureUnlocked(*published, published->selectedAccount);
    if (hasTime(published->lastQuoteUpdate)) {
        snapshot.quoteAgeMs = std::chrono::duration<double, std::milli>(
            std::chrono::steady_clock::now() - published->lastQuoteUpdate).count();
        snapshot.hasFreshQuote = snapshot.quoteAgeMs <= static_cast<double>(published->staleQuoteThresholdMs);
    }

    if (!published->selectedAccount.empty() && !subscribedSymbol.empty()) {
        auto posIt = published->positions.find(makePositionKey(published->selectedAccount, subscribedSymbol));
        if (posIt != published->positions.end()) {
            snapshot.currentPositionQty = posIt->second.quantity;
            snapshot.currentPositionAvgCost = posIt->second.avgCost;
            snapshot.hasPosition = true;
        }
        snapshot.availableLongToClose = availableLongToCloseUnlocked(*published, published->selectedAccount, subscribedSymbol);
    }

    return snapshot;
}

RuntimePresentationSnapshot captureRuntimePresentationSnapshot(const std::string& subscribedSymbol, std::size_t maxTraceItems) {
    RuntimePresentationSnapshot snapshot;
    const auto published = ensurePublishedSharedDataSnapshot();

    snapshot.status.connected = published->connected;
    snapshot.status.sessionReady = published->sessionReady;
    snapshot.status.sessionStateText = runtimeSessionStateToString(published->sessionState);
    snapshot.status.wsServerRunning = published->wsServerRunning;
    snapshot.status.websocketEnabled = published->websocketEnabled;
    snapshot.status.controllerConnected = published->controllerConnected;
    snapshot.status.controllerEnabled = published->controllerEnabled;
    snapshot.status.controllerArmed = published->controllerArmed;
    snapshot.status.tradingKillSwitch = published->tradingKillSwitch;
    snapshot.status.wsConnectedClients = published->wsConnectedClients;
    snapshot.status.controllerDeviceName = published->controllerDeviceName;
    snapshot.status.controllerLockedDeviceName = published->controllerLockedDeviceName;
    snapshot.status.websocketAuthToken = published->websocketAuthToken;
    snapshot.status.startupRecoveryBanner = published->startupRecoveryBanner;
    if (!published->selectedAccount.empty()) {
        snapshot.status.accountText = published->selectedAccount;
    } else if (!published->managedAccounts.empty()) {
        snapshot.status.accountText = "<configured account not found>";
    } else {
        snapshot.status.accountText = "<waiting for managedAccounts>";
    }

    snapshot.symbol.canTrade = published->connected && published->sessionReady && !published->selectedAccount.empty();
    snapshot.symbol.bidPrice = published->bidPrice;
    snapshot.symbol.askPrice = published->askPrice;
    snapshot.symbol.lastPrice = published->lastPrice;
    snapshot.symbol.askBook = published->askBook;
    snapshot.symbol.bidBook = published->bidBook;
    snapshot.symbol.openBuyExposure = calculateOpenBuyExposureUnlocked(*published, published->selectedAccount);
    if (hasTime(published->lastQuoteUpdate)) {
        snapshot.symbol.quoteAgeMs = std::chrono::duration<double, std::milli>(
            std::chrono::steady_clock::now() - published->lastQuoteUpdate).count();
        snapshot.symbol.hasFreshQuote = snapshot.symbol.quoteAgeMs <= static_cast<double>(published->staleQuoteThresholdMs);
    }
    if (!published->selectedAccount.empty() && !subscribedSymbol.empty()) {
        const auto posIt = published->positions.find(makePositionKey(published->selectedAccount, subscribedSymbol));
        if (posIt != published->positions.end()) {
            snapshot.symbol.currentPositionQty = posIt->second.quantity;
            snapshot.symbol.currentPositionAvgCost = posIt->second.avgCost;
            snapshot.symbol.hasPosition = true;
        }
        snapshot.symbol.availableLongToClose = availableLongToCloseUnlocked(*published, published->selectedAccount, subscribedSymbol);
    }

    snapshot.risk.staleQuoteThresholdMs = published->staleQuoteThresholdMs;
    snapshot.risk.brokerEchoTimeoutMs = published->brokerEchoTimeoutMs;
    snapshot.risk.cancelAckTimeoutMs = published->cancelAckTimeoutMs;
    snapshot.risk.partialFillQuietTimeoutMs = published->partialFillQuietTimeoutMs;
    snapshot.risk.maxOrderNotional = published->maxOrderNotional;
    snapshot.risk.maxOpenNotional = published->maxOpenNotional;
    snapshot.risk.controllerArmMode = published->controllerArmMode;
    snapshot.risk.controllerArmed = published->controllerArmed;
    snapshot.risk.tradingKillSwitch = published->tradingKillSwitch;

    snapshot.connection.host = published->twsHost;
    snapshot.connection.port = published->twsPort;
    snapshot.connection.clientId = published->twsClientId;
    snapshot.connection.websocketAuthToken = published->websocketAuthToken;
    snapshot.connection.websocketEnabled = published->websocketEnabled;
    snapshot.connection.controllerEnabled = published->controllerEnabled;
    snapshot.activeSymbol = published->currentSymbol;
    snapshot.currentQuantity = published->currentQuantity;
    snapshot.priceBuffer = published->priceBuffer;
    snapshot.maxPositionDollars = published->maxPositionDollars;
    snapshot.subscriptionActive = published->activeMktDataReqId != 0 && published->activeDepthReqId != 0;

    snapshot.orders.reserve(published->orders.size());
    for (const auto& kv : published->orders) {
        snapshot.orders.push_back(kv);
    }

    snapshot.latestTraceId = published->latestTraceId;
    if (maxTraceItems > 0) {
        snapshot.traceItems.reserve(std::min(maxTraceItems, published->traceRecency.size()));
        for (auto it = published->traceRecency.rbegin();
             it != published->traceRecency.rend() && snapshot.traceItems.size() < maxTraceItems;
             ++it) {
            const auto traceIt = published->traces.find(*it);
            if (traceIt == published->traces.end()) {
                continue;
            }
            snapshot.traceItems.push_back(makeTradeTraceListItem(traceIt->second));
        }
    }

    snapshot.messagesVersion = published->messagesVersion;
    snapshot.messagesText = published->messagesText;
    return snapshot;
}

void appendSharedMessage(const std::string& message) {
    if (message.empty()) {
        return;
    }
    appState().addMessage(message);
}

RuntimeConnectionConfig captureRuntimeConnectionConfig() {
    RuntimeConnectionConfig config;
    const auto published = ensurePublishedSharedDataSnapshot();
    config.host = published->twsHost;
    config.port = published->twsPort;
    config.clientId = published->twsClientId;
    config.websocketAuthToken = published->websocketAuthToken;
    config.websocketEnabled = published->websocketEnabled;
    config.controllerEnabled = published->controllerEnabled;
    return config;
}

void updateRuntimeConnectionConfig(const RuntimeConnectionConfig& config) {
    invokeSharedDataMutation([&]() {
        trading_engine::reduce(appState(), trading_engine::ConnectionConfigUpdatedEvent{config});
    });
    requestUiInvalidation();
}

RiskControlsSnapshot captureRiskControlsSnapshot() {
    RiskControlsSnapshot snapshot;
    const auto published = ensurePublishedSharedDataSnapshot();
    snapshot.staleQuoteThresholdMs = published->staleQuoteThresholdMs;
    snapshot.brokerEchoTimeoutMs = published->brokerEchoTimeoutMs;
    snapshot.cancelAckTimeoutMs = published->cancelAckTimeoutMs;
    snapshot.partialFillQuietTimeoutMs = published->partialFillQuietTimeoutMs;
    snapshot.maxOrderNotional = published->maxOrderNotional;
    snapshot.maxOpenNotional = published->maxOpenNotional;
    snapshot.controllerArmMode = published->controllerArmMode;
    snapshot.controllerArmed = published->controllerArmed;
    snapshot.tradingKillSwitch = published->tradingKillSwitch;
    return snapshot;
}

void updateRiskControls(int staleQuoteThresholdMs,
                        int brokerEchoTimeoutMs,
                        int cancelAckTimeoutMs,
                        int partialFillQuietTimeoutMs,
                        double maxOrderNotional,
                        double maxOpenNotional) {
    updateRiskControls(staleQuoteThresholdMs,
                       brokerEchoTimeoutMs,
                       cancelAckTimeoutMs,
                       partialFillQuietTimeoutMs,
                       maxOrderNotional,
                       maxOpenNotional,
                       captureRiskControlsSnapshot().controllerArmMode);
}

void updateRiskControls(int staleQuoteThresholdMs,
                        int brokerEchoTimeoutMs,
                        int cancelAckTimeoutMs,
                        int partialFillQuietTimeoutMs,
                        double maxOrderNotional,
                        double maxOpenNotional,
                        ControllerArmMode controllerArmMode) {
    invokeSharedDataMutation([&]() {
        trading_engine::reduce(appState(), trading_engine::RiskControlsUpdatedEvent{
            staleQuoteThresholdMs,
            brokerEchoTimeoutMs,
            cancelAckTimeoutMs,
            partialFillQuietTimeoutMs,
            maxOrderNotional,
            maxOpenNotional,
            controllerArmMode
        });
    });
    requestUiInvalidation();
}

void setControllerArmed(bool armed) {
    const bool changed = invokeSharedDataMutation([&]() {
        return trading_engine::reduce(appState(), trading_engine::ControllerArmedChangedEvent{armed});
    });
    if (changed) {
        appendRuntimeJournalEvent("controller_armed_changed", {{"armed", armed}});
        requestUiInvalidation();
    }
}

void setTradingKillSwitch(bool enabled) {
    const bool changed = invokeSharedDataMutation([&]() {
        return trading_engine::reduce(appState(), trading_engine::TradingKillSwitchChangedEvent{enabled});
    });
    if (changed) {
        appendRuntimeJournalEvent("trading_kill_switch_changed", {{"enabled", enabled}});
        requestUiInvalidation();
    }
}

void updateControllerConnectionState(bool connected, const std::string& deviceName) {
    const bool changed = invokeSharedDataMutation([&]() {
        return trading_engine::reduce(appState(), trading_engine::ControllerConnectionStateUpdatedEvent{connected, deviceName});
    });
    if (changed) {
        requestUiInvalidation();
    }
}

void updateControllerLockedDeviceName(const std::string& deviceName) {
    const bool changed = invokeSharedDataMutation([&]() {
        return trading_engine::reduce(appState(), trading_engine::ControllerLockedDeviceUpdatedEvent{deviceName});
    });
    if (changed) {
        requestUiInvalidation();
    }
}

void setWebSocketServerRunning(bool running) {
    const bool changed = invokeSharedDataMutation([&]() {
        return trading_engine::reduce(appState(), trading_engine::WebSocketServerRunningChangedEvent{running});
    });
    if (changed) {
        requestUiInvalidation();
    }
}

int adjustWebSocketConnectedClients(int delta) {
    const int total = invokeSharedDataMutation([&]() {
        return trading_engine::reduce(appState(), trading_engine::WebSocketClientDeltaEvent{delta});
    });
    requestUiInvalidation();
    return total;
}

std::string ensureWebSocketAuthToken() {
    return invokeSharedDataMutation([&]() {
        return trading_engine::reduce(appState(), trading_engine::WebSocketAuthTokenEnsuredEvent{});
    });
}

bool consumeWebSocketOrderRateLimit(std::string* error) {
    const auto result = invokeSharedDataMutation([&]() {
        return trading_engine::reduce(appState(), trading_engine::WebSocketOrderRateLimitConsumedEvent{
            std::chrono::steady_clock::now()
        });
    });
    if (error) {
        *error = result.error;
    }
    return result.allowed;
}

bool reserveWebSocketIdempotencyKey(const std::string& key, std::string* error) {
    const auto result = invokeSharedDataMutation([&]() {
        return trading_engine::reduce(appState(), trading_engine::WebSocketIdempotencyKeyReservedEvent{
            key,
            std::chrono::steady_clock::now()
        });
    });
    if (error) {
        *error = result.error;
    }
    return result.reserved;
}

BridgeOutboxEnqueueResult enqueueBridgeOutboxRecord(const BridgeOutboxRecordInput& input) {
    return invokeSharedDataMutation([&]() {
        BridgeOutboxEnqueueResult result;
        SharedData& state = appState();
        std::lock_guard<std::recursive_mutex> lock(state.mutex);

        BridgeOutboxRecord record;
        record.sourceSeq = state.nextBridgeSourceSeq.fetch_add(1, std::memory_order_relaxed);
        record.recordType = input.recordType;
        record.source = input.source;
        record.symbol = toUpperCase(input.symbol);
        record.side = input.side;
        record.anchor.traceId = input.traceId;
        record.anchor.orderId = input.orderId;
        record.anchor.permId = input.permId;
        record.anchor.execId = input.execId;
        record.note = input.note;
        record.wallTime = formatWallTime(std::chrono::system_clock::now());

        state.lastBridgeSourceSeq = record.sourceSeq;
        state.bridgeFallbackState = "queued_for_recovery";
        state.bridgeFallbackReason = "engine_unavailable";
        record.fallbackState = state.bridgeFallbackState;
        record.fallbackReason = state.bridgeFallbackReason;
        state.bridgeOutbox.push_back(record);

        result.sourceSeq = record.sourceSeq;
        result.queued = true;
        result.fallbackState = record.fallbackState;
        result.fallbackReason = record.fallbackReason;

        json queuedDetails = {
            {"sourceSeq", static_cast<unsigned long long>(record.sourceSeq)},
            {"recordType", record.recordType},
            {"fallbackState", record.fallbackState},
            {"fallbackReason", record.fallbackReason}
        };
        if (!record.source.empty()) {
            queuedDetails["source"] = record.source;
        }
        if (!record.symbol.empty()) {
            queuedDetails["symbol"] = record.symbol;
        }
        if (!record.side.empty()) {
            queuedDetails["side"] = record.side;
        }
        if (record.anchor.traceId > 0) {
            queuedDetails["traceId"] = static_cast<unsigned long long>(record.anchor.traceId);
        }
        if (record.anchor.orderId > 0) {
            queuedDetails["orderId"] = static_cast<long long>(record.anchor.orderId);
        }
        if (record.anchor.permId > 0) {
            queuedDetails["permId"] = record.anchor.permId;
        }
        if (!record.anchor.execId.empty()) {
            queuedDetails["execId"] = record.anchor.execId;
        }
        if (!record.note.empty()) {
            queuedDetails["note"] = record.note;
        }
        appendRuntimeJournalEvent("bridge_outbox_queued", queuedDetails);

        if (state.bridgeOutbox.size() > kMaxBridgeOutboxRecords) {
            const BridgeOutboxRecord dropped = state.bridgeOutbox.front();
            state.bridgeOutbox.pop_front();
            ++state.bridgeOutboxLossCount;
            result.lossMarked = true;

            json lossDetails = {
                {"reason", "queue_overflow"},
                {"recordType", dropped.recordType},
                {"droppedSourceSeq", static_cast<unsigned long long>(dropped.sourceSeq)}
            };
            if (dropped.anchor.traceId > 0) {
                lossDetails["traceId"] = static_cast<unsigned long long>(dropped.anchor.traceId);
            }
            if (dropped.anchor.orderId > 0) {
                lossDetails["orderId"] = static_cast<long long>(dropped.anchor.orderId);
            }
            if (dropped.anchor.permId > 0) {
                lossDetails["permId"] = dropped.anchor.permId;
            }
            if (!dropped.anchor.execId.empty()) {
                lossDetails["execId"] = dropped.anchor.execId;
            }
            appendRuntimeJournalEvent("bridge_outbox_loss", lossDetails);
        }

        state.bridgeRecoveryRequired = (state.bridgeRecoveredPendingCount + static_cast<int>(state.bridgeOutbox.size())) > 0 ||
                                       (state.bridgeRecoveredLossCount + static_cast<int>(state.bridgeOutboxLossCount)) > 0;
        result.recoveryRequired = state.bridgeRecoveryRequired;
        return result;
    });
}

void seedBridgeOutboxRecoveryState(const RuntimeRecoverySnapshot& recovery) {
    invokeSharedDataMutation([&]() {
        SharedData& state = appState();
        std::lock_guard<std::recursive_mutex> lock(state.mutex);
        state.bridgeRecoveredPendingCount = std::max(0, recovery.pendingOutboxCount);
        state.bridgeRecoveredLossCount = std::max(0, recovery.outboxLossCount);
        state.bridgeRecoveredLastSourceSeq = recovery.lastOutboxSourceSeq;
        state.lastBridgeSourceSeq = std::max(state.lastBridgeSourceSeq, recovery.lastOutboxSourceSeq);
        if (state.bridgeFallbackState.empty()) {
            state.bridgeFallbackState = "queued_for_recovery";
        }
        if (state.bridgeFallbackReason.empty()) {
            state.bridgeFallbackReason = "engine_unavailable";
        }
        state.bridgeRecoveryRequired = (state.bridgeRecoveredPendingCount + static_cast<int>(state.bridgeOutbox.size())) > 0 ||
                                       (state.bridgeRecoveredLossCount + static_cast<int>(state.bridgeOutboxLossCount)) > 0;
    });
}

BridgeOutboxSnapshot captureBridgeOutboxSnapshot(std::size_t maxItems) {
    BridgeOutboxSnapshot snapshot;
    const auto published = ensurePublishedSharedDataSnapshot();
    snapshot.fallbackState = published->bridgeFallbackState;
    snapshot.fallbackReason = published->bridgeFallbackReason;
    snapshot.recoveryRequired = published->bridgeRecoveryRequired;
    snapshot.pendingCount = published->bridgeRecoveredPendingCount + static_cast<int>(published->bridgeOutbox.size());
    snapshot.lossCount = published->bridgeRecoveredLossCount + static_cast<int>(published->bridgeOutboxLossCount);
    snapshot.lastSourceSeq = std::max(published->lastBridgeSourceSeq, published->bridgeRecoveredLastSourceSeq);

    if (maxItems > 0) {
        snapshot.records.reserve(std::min(maxItems, published->bridgeOutbox.size()));
        for (auto it = published->bridgeOutbox.rbegin();
             it != published->bridgeOutbox.rend() && snapshot.records.size() < maxItems;
             ++it) {
            snapshot.records.push_back(*it);
        }
    }
    return snapshot;
}

std::vector<std::pair<OrderId, OrderInfo>> captureOrdersSnapshot() {
    std::vector<std::pair<OrderId, OrderInfo>> ordersSnapshot;
    const auto published = ensurePublishedSharedDataSnapshot();
    ordersSnapshot.reserve(published->orders.size());
    for (const auto& kv : published->orders) {
        ordersSnapshot.push_back(kv);
    }
    return ordersSnapshot;
}

std::vector<OrderId> markOrdersPendingCancel(const std::vector<OrderId>& orderIds) {
    return invokeSharedDataMutation([&]() {
        return trading_engine::reduce(appState(), trading_engine::OrdersPendingCancelMarkedEvent{orderIds});
    });
}

std::vector<OrderId> markAllPendingOrdersForCancel() {
    return invokeSharedDataMutation([&]() {
        return trading_engine::reduce(appState(), trading_engine::AllPendingOrdersMarkedForCancelEvent{});
    });
}

void noteCancelRequestsSent(const std::vector<OrderId>& orderIds,
                            std::chrono::steady_clock::time_point requestTime) {
    if (orderIds.empty()) {
        return;
    }
    invokeSharedDataMutation([&]() {
        trading_engine::reduce(appState(), trading_engine::CancelRequestsSentEvent{orderIds, requestTime});
    });
}

OrderWatchdogSweepResult requestOrderReconciliation(const std::vector<OrderId>& orderIds,
                                                    const std::string& reason,
                                                    std::chrono::steady_clock::time_point requestTime) {
    if (orderIds.empty()) {
        return {};
    }
    return invokeSharedDataMutation([&]() {
        return trading_engine::reduce(appState(),
                                      trading_engine::ManualReconciliationRequestedEvent{orderIds, reason, requestTime});
    });
}

std::vector<OrderId> acknowledgeManualReviewOrders(const std::vector<OrderId>& orderIds,
                                                   std::chrono::steady_clock::time_point acknowledgeTime) {
    if (orderIds.empty()) {
        return {};
    }
    return invokeSharedDataMutation([&]() {
        return trading_engine::reduce(appState(),
                                      trading_engine::ManualReviewAcknowledgedEvent{orderIds, acknowledgeTime});
    });
}

OrderWatchdogSweepResult sweepOrderWatchdogs(std::chrono::steady_clock::time_point now) {
    return invokeSharedDataMutation([&]() {
        return trading_engine::reduce(appState(), trading_engine::OrderWatchdogSweepEvent{now});
    });
}

OrderWatchdogSweepResult sweepOrderWatchdogsOnReducerThread(std::chrono::steady_clock::time_point now) {
    return trading_engine::reduce(appState(), trading_engine::OrderWatchdogSweepEvent{now});
}

std::vector<bool> sendCancelRequests(EClientSocket* client, const std::vector<OrderId>& orderIds) {
    std::vector<bool> sent(orderIds.size(), false);
    if (orderIds.empty()) return sent;

    {
        std::lock_guard<std::recursive_mutex> clientLock(g_data.clientMutex);
        if (!client->isConnected()) {
            return sent;
        }

        for (size_t i = 0; i < orderIds.size(); ++i) {
#if defined(TWS_HAS_ORDER_CANCEL_OBJECT)
            OrderCancel orderCancel;
            client->cancelOrder(orderIds[i], orderCancel);
#else
            client->cancelOrder(orderIds[i], "");
#endif
            sent[i] = true;
        }
    }

    std::vector<OrderId> sentOrderIds;
    sentOrderIds.reserve(orderIds.size());
    for (size_t i = 0; i < orderIds.size(); ++i) {
        if (sent[i]) {
            sentOrderIds.push_back(orderIds[i]);
            recordTraceCancelRequest(orderIds[i]);
            appendRuntimeJournalEvent("cancel_request_sent", {{"orderId", static_cast<long long>(orderIds[i])}});
        }
    }
    noteCancelRequestsSent(sentOrderIds, std::chrono::steady_clock::now());

    return sent;
}

int computeMaxQuantityFromAsk(double currentAsk, double maxPositionDollars) {
    if (currentAsk <= 0.0) return 1;
    const int maxQty = static_cast<int>(std::floor(maxPositionDollars / currentAsk));
    return maxQty < 1 ? 1 : maxQty;
}

void cancelActiveSubscription(EClientSocket* client) {
    const auto published = ensurePublishedSharedDataSnapshot();
    const int mktDataReqId = published->activeMktDataReqId;
    const int depthReqId = published->activeDepthReqId;

    invokeSharedDataMutation([&]() {
        trading_engine::reduce(appState(), trading_engine::MarketSubscriptionClearedEvent{
            mktDataReqId,
            depthReqId
        });
    });

    std::lock_guard<std::recursive_mutex> clientLock(g_data.clientMutex);
    if (!client->isConnected()) return;
    if (mktDataReqId != 0) client->cancelMktData(mktDataReqId);
    if (depthReqId != 0) client->cancelMktDepth(depthReqId, true);
}

bool requestSymbolSubscription(EClientSocket* client,
                               const std::string& rawSymbol,
                               bool recalcQtyFromFirstAsk,
                               std::string* error) {
    const std::string symbol = toUpperCase(rawSymbol);
    if (symbol.empty()) {
        if (error) *error = "Symbol cannot be empty";
        return false;
    }

    const auto published = ensurePublishedSharedDataSnapshot();
    if (!(published->connected && published->sessionReady)) {
        if (error) *error = "TWS session not ready";
        return false;
    }
    if (published->currentSymbol == symbol &&
        published->activeMktDataReqId != 0 &&
        published->activeDepthReqId != 0) {
        return true;
    }

    cancelActiveSubscription(client);

    const int mktDataReqId = allocateReqId();
    const int depthReqId = allocateReqId();

    Contract contract;
    contract.symbol = symbol;
    contract.secType = "STK";
    contract.exchange = "SMART";
    contract.currency = "USD";

    {
        std::lock_guard<std::recursive_mutex> clientLock(g_data.clientMutex);
        if (!client->isConnected()) {
            if (error) *error = "TWS socket not connected";
            return false;
        }
        client->reqMktData(mktDataReqId, contract, "", false, false, TagValueListSPtr());
        client->reqMktDepth(depthReqId, contract, MARKET_DEPTH_NUM_ROWS, true, TagValueListSPtr());
    }

    invokeSharedDataMutation([&]() {
        trading_engine::reduce(appState(), trading_engine::MarketSubscriptionStartedEvent{
            symbol,
            mktDataReqId,
            depthReqId,
            recalcQtyFromFirstAsk
        });
    });

    appendSharedMessage("Subscription request sent for " + symbol);
    appendRuntimeJournalEvent("subscribe_request_sent", {
        {"symbol", symbol},
        {"recalculateQuantity", recalcQtyFromFirstAsk}
    });
    return true;
}

SubmitIntent captureSubmitIntent(const std::string& source,
                                 const std::string& symbol,
                                 const std::string& side,
                                 int requestedQty,
                                 double limitPrice,
                                 bool closeOnly,
                                 double priceBuffer,
                                 double sweepEstimate,
                                 const std::string& notes) {
    SubmitIntent intent;
    intent.source = source;
    intent.symbol = toUpperCase(symbol);
    intent.side = side;
    intent.requestedQty = requestedQty;
    intent.limitPrice = limitPrice;
    intent.closeOnly = closeOnly;
    intent.priceBuffer = priceBuffer;
    intent.sweepEstimate = sweepEstimate;
    intent.notes = notes;
    intent.triggerMono = std::chrono::steady_clock::now();
    intent.triggerWall = std::chrono::system_clock::now();

    const auto published = ensurePublishedSharedDataSnapshot();
    if (intent.symbol == published->currentSymbol) {
        intent.decisionBid = published->bidPrice;
        intent.decisionAsk = published->askPrice;
        intent.decisionLast = published->lastPrice;
        intent.decisionAskLevels = static_cast<int>(published->askBook.size());
        intent.decisionBidLevels = static_cast<int>(published->bidBook.size());
        intent.bookSummary = buildBookSummary(published->askBook, published->bidBook);
    } else {
        intent.bookSummary = "no active book snapshot for requested symbol";
    }
    return intent;
}

bool submitLimitOrder(EClientSocket* client,
                      const std::string& rawSymbol,
                      const std::string& action,
                      double quantity,
                      double limitPrice,
                      bool closeOnly,
                      const SubmitIntent* intent,
                      std::string* error,
                      OrderId* outOrderId,
                      std::uint64_t* outTraceId) {
    const std::string symbol = toUpperCase(rawSymbol);
    SubmitIntent effectiveIntent = intent ? *intent : captureSubmitIntent(
        "Internal", symbol, action, toShareCount(quantity), limitPrice, closeOnly, 0.0, 0.0, "submitLimitOrder");
    effectiveIntent.symbol = symbol;
    effectiveIntent.side = action;
    effectiveIntent.requestedQty = toShareCount(quantity);
    effectiveIntent.limitPrice = limitPrice;
    effectiveIntent.closeOnly = closeOnly;

    const std::uint64_t traceId = beginTradeTrace(effectiveIntent);
    if (outTraceId) *outTraceId = traceId;

    appendTraceEventByTraceId(traceId, TradeEventType::ValidationStart,
                              "Validation", "Starting local order validation");

    auto failValidation = [&](const std::string& reason) {
        if (error) *error = reason;
        markTraceValidationFailed(traceId, reason);
        return false;
    };

    if (symbol.empty()) {
        return failValidation("Symbol cannot be empty");
    }
    if (action != "BUY" && action != "SELL") {
        return failValidation("Action must be BUY or SELL");
    }
    if (quantity <= 0.0) {
        return failValidation("Quantity must be positive");
    }
    if (limitPrice <= 0.0) {
        return failValidation("Limit price must be positive");
    }

    std::string account;
    bool ready = false;
    double availableToClose = 0.0;
    double quoteAgeMs = -1.0;
    bool quoteMatchesCurrentSymbol = false;
    int staleQuoteThresholdMs = 0;
    double maxOrderNotional = 0.0;
    double maxOpenNotional = 0.0;
    double openBuyExposure = 0.0;
    double currentPositionValue = 0.0;
    ControllerArmMode controllerArmMode = ControllerArmMode::OneShot;
    bool controllerArmed = false;
    bool tradingKillSwitch = false;
    std::string duplicateFingerprint;

    {
        const auto published = ensurePublishedSharedDataSnapshot();
        ready = published->connected && published->sessionReady;
        account = published->selectedAccount;
        staleQuoteThresholdMs = published->staleQuoteThresholdMs;
        maxOrderNotional = published->maxOrderNotional;
        maxOpenNotional = published->maxOpenNotional;
        controllerArmMode = published->controllerArmMode;
        controllerArmed = published->controllerArmed;
        tradingKillSwitch = published->tradingKillSwitch;
        openBuyExposure = calculateOpenBuyExposureUnlocked(*published, account);
        currentPositionValue = calculatePositionMarketValueUnlocked(*published, account, symbol);
        quoteMatchesCurrentSymbol = (symbol == published->currentSymbol);
        if (hasTime(published->lastQuoteUpdate) && quoteMatchesCurrentSymbol) {
            quoteAgeMs = std::chrono::duration<double, std::milli>(
                std::chrono::steady_clock::now() - published->lastQuoteUpdate).count();
        }
        if (closeOnly) {
            availableToClose = availableLongToCloseUnlocked(*published, account, symbol);
        }
    }

    if (!ready) {
        return failValidation("TWS session not ready");
    }
    if (account.empty()) {
        return failValidation("Configured account is not present in managedAccounts");
    }
    if (tradingKillSwitch) {
        return failValidation("Trading is halted by the kill switch");
    }
    if (effectiveIntent.source.find("Controller") != std::string::npos && !controllerArmed) {
        return failValidation("Controller trading is not armed");
    }
    if (closeOnly) {
        if (availableToClose <= 0.0) {
            return failValidation("No long shares available to close");
        }
        if (quantity > availableToClose + 1e-9) {
            return failValidation("Requested sell quantity exceeds available long shares");
        }
    } else {
        const double orderNotional = quantity * limitPrice;
        if (maxOrderNotional > 0.0 && orderNotional > maxOrderNotional + 1e-9) {
            std::ostringstream oss;
            oss << "Order notional $" << std::fixed << std::setprecision(2) << orderNotional
                << " exceeds max order notional $" << maxOrderNotional;
            return failValidation(oss.str());
        }
        const double projectedNotional = openBuyExposure + currentPositionValue + orderNotional;
        if (maxOpenNotional > 0.0 && projectedNotional > maxOpenNotional + 1e-9) {
            std::ostringstream oss;
            oss << "Projected open notional $" << std::fixed << std::setprecision(2) << projectedNotional
                << " exceeds max open notional $" << maxOpenNotional;
            return failValidation(oss.str());
        }
    }
    if (quoteMatchesCurrentSymbol && staleQuoteThresholdMs > 0) {
        if (quoteAgeMs < 0.0) {
            return failValidation("No quote has been received for the active symbol yet");
        }
        if (quoteAgeMs > static_cast<double>(staleQuoteThresholdMs)) {
            std::ostringstream oss;
            oss << "Quote is stale (" << std::fixed << std::setprecision(0) << quoteAgeMs
                << " ms old; threshold " << staleQuoteThresholdMs << " ms)";
            return failValidation(oss.str());
        }
    }

    duplicateFingerprint = makeOrderFingerprint(effectiveIntent);
    const auto reservation = invokeSharedDataMutation([&]() {
        return trading_engine::reduce(appState(), trading_engine::SubmitOrderStateReservedEvent{
            duplicateFingerprint,
            std::chrono::steady_clock::now()
        });
    });
    if (!reservation.allowed) {
        return failValidation(reservation.error);
    }
    const OrderId orderId = reservation.orderId;

    bindTraceToOrder(traceId, orderId);
    appendTraceEventByTraceId(traceId, TradeEventType::ValidationOk,
                              "Validation", "Local validation passed");

    Contract contract;
    contract.symbol = symbol;
    contract.secType = "STK";
    contract.exchange = "SMART";
    contract.currency = "USD";

    Order order;
    order.orderId = static_cast<long>(orderId);
    order.action = action;
    order.totalQuantity = DecimalFunctions::doubleToDecimal(quantity);
    order.orderType = "LMT";
    order.lmtPrice = limitPrice;
    order.tif = "DAY";
    order.account = account;
    order.outsideRth = true;
    order.transmit = true;

    appendTraceEventByTraceId(traceId, TradeEventType::PlaceOrderCallStart,
                              "placeOrder", "Calling EClientSocket::placeOrder()");
    appendRuntimeJournalEvent("submit_order_requested", {
        {"traceId", static_cast<unsigned long long>(traceId)},
        {"symbol", symbol},
        {"action", action},
        {"quantity", quantity},
        {"limitPrice", limitPrice},
        {"source", effectiveIntent.source}
    });

    {
        std::lock_guard<std::recursive_mutex> clientLock(g_data.clientMutex);
        if (!client->isConnected()) {
            appendTraceEventByTraceId(traceId, TradeEventType::ErrorSeen,
                                      "placeOrder", "TWS socket not connected", -1.0, -1.0, 0.0, 0, -1);
            markTraceTerminalByOrderId(orderId, "FailedBeforeSubmit", "TWS socket not connected");
            if (error) *error = "TWS socket not connected";
            return false;
        }
        client->placeOrder(orderId, contract, order);
    }

    appendTraceEventByTraceId(traceId, TradeEventType::PlaceOrderCallEnd,
                              "placeOrder", "Returned from EClientSocket::placeOrder()");

    invokeSharedDataMutation([&]() {
        trading_engine::reduce(appState(), trading_engine::LocalOrderStoredEvent{
            orderId,
            account,
            symbol,
            action,
            quantity,
            limitPrice,
            std::chrono::steady_clock::now()
        });
    });

    markTraceSubmitted(traceId);

    char msg[256];
    std::snprintf(msg, sizeof(msg), "%s %.0f %s @ %.2f (ID: %lld, acct: %s)",
                  action.c_str(), quantity, symbol.c_str(), limitPrice,
                  static_cast<long long>(orderId), account.c_str());
    appendSharedMessage(msg);

    if (outOrderId) *outOrderId = orderId;
    if (effectiveIntent.source.find("Controller") != std::string::npos &&
        controllerArmMode == ControllerArmMode::OneShot) {
        setControllerArmed(false);
    }
    return true;
}

double calculateSweepPrice(const std::vector<BookLevel>& book, int quantity, double safetyBuffer, bool isBuy) {
    if (book.empty() || quantity <= 0) return 0.0;

    double remaining = static_cast<double>(quantity);
    double sweepPrice = 0.0;

    for (const auto& level : book) {
        if (level.size <= 0.0) continue;
        sweepPrice = level.price;
        remaining -= level.size;
        if (remaining <= 0.0) break;
    }

    if (isBuy) {
        return sweepPrice + safetyBuffer;
    } else {
        double result = sweepPrice - safetyBuffer;
        return (result < 0.01) ? 0.01 : result;
    }
}

void readerLoop(EReaderOSSignal* osSignal, EReader* reader, EClientSocket* client, std::atomic<bool>* running) {
    (void)client;
    while (running->load()) {
        osSignal->waitForSignal();
        if (!running->load()) break;
        reader->processMsgs();
    }
}

std::string formatWallTime(std::chrono::system_clock::time_point tp) {
    if (!hasTime(tp)) return {};

    const auto timeT = std::chrono::system_clock::to_time_t(tp);
    std::tm tmLocal{};
#if defined(_WIN32)
    localtime_s(&tmLocal, &timeT);
#else
    localtime_r(&timeT, &tmLocal);
#endif
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()) % 1000;

    std::ostringstream oss;
    oss << std::put_time(&tmLocal, "%H:%M:%S") << '.' << std::setfill('0') << std::setw(3) << ms.count();
    return oss.str();
}

double durationMs(std::chrono::steady_clock::time_point start, std::chrono::steady_clock::time_point end) {
    if (!hasTime(start) || !hasTime(end)) return -1.0;
    return std::chrono::duration<double, std::milli>(end - start).count();
}

std::string tradeEventTypeToString(TradeEventType type) {
    switch (type) {
        case TradeEventType::Trigger: return "Trigger";
        case TradeEventType::ValidationStart: return "ValidationStart";
        case TradeEventType::ValidationOk: return "ValidationOk";
        case TradeEventType::ValidationFailed: return "ValidationFailed";
        case TradeEventType::PlaceOrderCallStart: return "PlaceOrderCallStart";
        case TradeEventType::PlaceOrderCallEnd: return "PlaceOrderCallEnd";
        case TradeEventType::OpenOrderSeen: return "OpenOrderSeen";
        case TradeEventType::OrderStatusSeen: return "OrderStatusSeen";
        case TradeEventType::ExecDetailsSeen: return "ExecDetailsSeen";
        case TradeEventType::CommissionSeen: return "CommissionSeen";
        case TradeEventType::ErrorSeen: return "ErrorSeen";
        case TradeEventType::CancelRequestSent: return "CancelRequestSent";
        case TradeEventType::CancelAck: return "CancelAck";
        case TradeEventType::FinalState: return "FinalState";
        case TradeEventType::Note: return "Note";
        default: return "Unknown";
    }
}

void appendTradeTraceLogLine(const json& line) {
    std::lock_guard<std::mutex> lock(tradeTraceFileMutex());
    std::ofstream out(tradeTraceLogPath(), std::ios::app);
    if (!out.is_open()) return;
    out << line.dump() << '\n';
}

void appendRuntimeJournalLine(const json& line) {
    std::lock_guard<std::mutex> lock(runtimeJournalFileMutex());
    std::ofstream out(runtimeJournalLogPath(), std::ios::app);
    if (!out.is_open()) return;
    out << line.dump() << '\n';
}

void appendRuntimeJournalEvent(const std::string& event, const json& details) {
    appendRuntimeJournalLine(makeRuntimeJournalLine(event, details));
}

std::vector<std::string> recoverUnfinishedTraceSummariesFromLog(std::size_t maxItems) {
    std::ifstream in(tradeTraceLogPath());
    if (!in.is_open()) {
        return {};
    }

    struct PendingInfo {
        bool terminal = false;
        std::string summary;
    };

    std::unordered_map<std::uint64_t, PendingInfo> traces;
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) continue;
        json parsed = json::parse(line, nullptr, false);
        if (parsed.is_discarded()) continue;
        const std::uint64_t traceId = parsed.value("traceId", 0ULL);
        if (traceId == 0) continue;

        auto& info = traces[traceId];
        std::ostringstream oss;
        oss << "T" << traceId
            << " | " << parsed.value("source", std::string("Unknown"))
            << " | " << parsed.value("side", std::string("?"))
            << ' ' << parsed.value("symbol", std::string("<none>"))
            << ' ' << parsed.value("requestedQty", 0)
            << " @ " << std::fixed << std::setprecision(2)
            << parsed.value("limitPrice", 0.0);
        info.summary = oss.str();
        if (parsed.value("eventType", std::string()) == "FinalState") {
            info.terminal = true;
        }
    }

    std::vector<std::string> summaries;
    summaries.reserve(std::min(maxItems, traces.size()));
    for (const auto& [traceId, info] : traces) {
        if (!info.terminal) {
            summaries.push_back(info.summary);
        }
    }
    if (summaries.size() > maxItems) {
        summaries.resize(maxItems);
    }
    return summaries;
}

RuntimeRecoverySnapshot recoverRuntimeRecoverySnapshot(std::size_t maxTraceItems) {
    RuntimeRecoverySnapshot snapshot;
    snapshot.unfinishedTraceSummaries = recoverUnfinishedTraceSummariesFromLog(maxTraceItems);
    snapshot.unfinishedTraceCount = static_cast<int>(snapshot.unfinishedTraceSummaries.size());

    std::ifstream in(runtimeJournalLogPath());
    if (!in.is_open()) {
        return snapshot;
    }

    struct SessionInfo {
        bool started = false;
        bool cleanShutdown = false;
        std::string appSessionId;
        std::string runtimeSessionId;
        int outboxQueued = 0;
        int outboxDelivered = 0;
        int outboxLoss = 0;
        std::uint64_t lastOutboxSourceSeq = 0;
    };

    std::map<std::string, SessionInfo> sessions;
    std::string lastAppSessionId;
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) {
            continue;
        }
        json parsed = json::parse(line, nullptr, false);
        if (parsed.is_discarded()) {
            continue;
        }

        const std::string appSessionId = parsed.value("appSessionId", std::string());
        if (appSessionId.empty()) {
            continue;
        }
        SessionInfo& info = sessions[appSessionId];
        info.appSessionId = appSessionId;
        info.runtimeSessionId = parsed.value("runtimeSessionId", info.runtimeSessionId);
        lastAppSessionId = appSessionId;

        const std::string event = parsed.value("event", std::string());
        const json details = parsed.contains("details") && parsed["details"].is_object()
            ? parsed["details"]
            : json::object();
        if (event == "runtime_start") {
            info.started = true;
        } else if (event == "runtime_shutdown") {
            info.cleanShutdown = true;
        } else if (event == "bridge_outbox_queued") {
            ++info.outboxQueued;
            info.lastOutboxSourceSeq = std::max(info.lastOutboxSourceSeq,
                                                details.value("sourceSeq", 0ULL));
        } else if (event == "bridge_outbox_delivered") {
            ++info.outboxDelivered;
            info.lastOutboxSourceSeq = std::max(info.lastOutboxSourceSeq,
                                                details.value("sourceSeq", 0ULL));
        } else if (event == "bridge_outbox_loss") {
            ++info.outboxLoss;
            info.lastOutboxSourceSeq = std::max(info.lastOutboxSourceSeq,
                                                details.value("droppedSourceSeq", 0ULL));
        }
    }

    if (lastAppSessionId.empty()) {
        return snapshot;
    }

    const auto it = sessions.find(lastAppSessionId);
    if (it == sessions.end()) {
        return snapshot;
    }

    snapshot.priorAppSessionId = it->second.appSessionId;
    snapshot.priorRuntimeSessionId = it->second.runtimeSessionId;
    snapshot.priorSessionAbnormal = it->second.started && !it->second.cleanShutdown;
    snapshot.pendingOutboxCount = std::max(0, it->second.outboxQueued - it->second.outboxDelivered - it->second.outboxLoss);
    snapshot.outboxLossCount = it->second.outboxLoss;
    snapshot.lastOutboxSourceSeq = it->second.lastOutboxSourceSeq;
    snapshot.bridgeRecoveryRequired = snapshot.pendingOutboxCount > 0 || snapshot.outboxLossCount > 0;
    if (snapshot.priorSessionAbnormal) {
        std::ostringstream oss;
        oss << "Previous session ended unexpectedly";
        if (snapshot.unfinishedTraceCount > 0) {
            oss << " with " << snapshot.unfinishedTraceCount << " unfinished trace";
            if (snapshot.unfinishedTraceCount != 1) {
                oss << 's';
            }
        }
        snapshot.bannerText = oss.str();
    }
    if (snapshot.bridgeRecoveryRequired) {
        std::ostringstream bridge;
        bridge << "Bridge recovery pending: " << snapshot.pendingOutboxCount << " queued intent";
        if (snapshot.pendingOutboxCount != 1) {
            bridge << 's';
        }
        if (snapshot.outboxLossCount > 0) {
            bridge << ", " << snapshot.outboxLossCount << " loss marker";
            if (snapshot.outboxLossCount != 1) {
                bridge << 's';
            }
        }
        if (snapshot.lastOutboxSourceSeq > 0) {
            bridge << " (last source_seq=" << snapshot.lastOutboxSourceSeq << ")";
        }
        if (!snapshot.bannerText.empty()) {
            snapshot.bannerText += ". ";
            snapshot.bannerText += bridge.str();
        } else {
            snapshot.bannerText = bridge.str();
        }
    }
    return snapshot;
}

namespace trading_engine {

struct TraceMutationResult {
    bool emitted = false;
    std::uint64_t traceId = 0;
    TradeEventType type = TradeEventType::Note;
    std::string stage;
    std::string details;
    json line;
};

struct TraceStatusRecordResult {
    TraceMutationResult mutation;
    bool appendCancelAck = false;
    bool appendTerminal = false;
    std::string terminalReason;
    double filledQty = -1.0;
    double remainingQty = -1.0;
    double avgFillPrice = 0.0;
    OrderId orderId = 0;
};

struct TraceErrorRecordResult {
    TraceMutationResult mutation;
    bool appendCancelAck = false;
    bool appendTerminal = false;
    std::string terminalStatus;
    int errorCode = 0;
    OrderId orderId = 0;
};

struct BeginTradeTraceEvent {
    SubmitIntent intent;
};

struct TraceBoundToOrderEvent {
    std::uint64_t traceId = 0;
    OrderId orderId = 0;
};

struct TraceBoundToPermIdEvent {
    OrderId orderId = 0;
    long long permId = 0;
};

struct TraceEventAppendedEvent {
    std::uint64_t traceId = 0;
    TradeEventType type = TradeEventType::Note;
    std::string stage;
    std::string details;
    double cumFilled = -1.0;
    double remaining = -1.0;
    double price = 0.0;
    int shares = 0;
    int errorCode = 0;
};

struct TraceValidationFailedMarkedEvent {
    std::uint64_t traceId = 0;
    std::string reason;
};

struct TraceTerminalMarkedByOrderEvent {
    OrderId orderId = 0;
    std::string terminalStatus;
    std::string reason;
};

struct TraceOpenOrderRecordedEvent {
    OrderId orderId = 0;
    Contract contract;
    Order order;
    OrderState orderState;
};

struct TraceOrderStatusRecordedEvent {
    OrderId orderId = 0;
    std::string status;
    double filledQty = 0.0;
    double remainingQty = 0.0;
    double avgFillPrice = 0.0;
    long long permId = 0;
    double lastFillPrice = 0.0;
    double mktCapPrice = 0.0;
};

struct TraceExecutionRecordedEvent {
    Contract contract;
    Execution execution;
};

struct TraceCommissionRecordedEvent {
    CommissionReport commissionReport;
};

struct TraceErrorRecordedEvent {
    int id = 0;
    int errorCode = 0;
    std::string errorString;
};

TraceMutationResult reduce(SharedData& state, const BeginTradeTraceEvent& event) {
    TradeTrace trace;
    trace.traceId = state.nextTraceId.fetch_add(1, std::memory_order_relaxed);
    trace.source = event.intent.source;
    trace.symbol = event.intent.symbol;
    trace.side = event.intent.side;
    trace.requestedQty = event.intent.requestedQty;
    trace.limitPrice = event.intent.limitPrice;
    trace.closeOnly = event.intent.closeOnly;
    trace.decisionBid = event.intent.decisionBid;
    trace.decisionAsk = event.intent.decisionAsk;
    trace.decisionLast = event.intent.decisionLast;
    trace.sweepEstimate = event.intent.sweepEstimate;
    trace.priceBuffer = event.intent.priceBuffer;
    trace.decisionAskLevels = event.intent.decisionAskLevels;
    trace.decisionBidLevels = event.intent.decisionBidLevels;
    trace.bookSummary = event.intent.bookSummary;
    trace.notes = event.intent.notes;
    trace.triggerMono = event.intent.triggerMono;
    trace.triggerWall = event.intent.triggerWall;

    TraceEvent triggerEvent;
    triggerEvent.type = TradeEventType::Trigger;
    triggerEvent.monoTs = event.intent.triggerMono;
    triggerEvent.wallTs = event.intent.triggerWall;
    triggerEvent.stage = event.intent.source;
    std::ostringstream details;
    details << event.intent.side << ' ' << event.intent.requestedQty << ' ' << event.intent.symbol
            << " @ " << std::fixed << std::setprecision(2) << event.intent.limitPrice;
    if (!event.intent.notes.empty()) {
        details << " | " << event.intent.notes;
    }
    triggerEvent.details = details.str();
    trace.events.push_back(triggerEvent);

    {
        std::lock_guard<std::recursive_mutex> lock(state.mutex);
        state.traces[trace.traceId] = trace;
        state.traceRecency.push_back(trace.traceId);
        state.latestTraceId = trace.traceId;
    }

    return {
        true,
        trace.traceId,
        triggerEvent.type,
        triggerEvent.stage,
        triggerEvent.details,
        makeTraceEventLogLine(trace, triggerEvent)
    };
}

void reduce(SharedData& state, const TraceBoundToOrderEvent& event) {
    std::lock_guard<std::recursive_mutex> lock(state.mutex);
    auto it = state.traces.find(event.traceId);
    if (it == state.traces.end()) {
        return;
    }
    it->second.orderId = event.orderId;
    state.traceIdByOrderId[event.orderId] = event.traceId;
}

void reduce(SharedData& state, const TraceBoundToPermIdEvent& event) {
    if (event.permId <= 0) {
        return;
    }
    std::lock_guard<std::recursive_mutex> lock(state.mutex);
    const auto traceId = findTraceIdLocked(event.orderId);
    if (traceId == 0) {
        return;
    }
    auto it = state.traces.find(traceId);
    if (it == state.traces.end()) {
        return;
    }
    it->second.permId = event.permId;
    state.traceIdByPermId[event.permId] = traceId;
}

TraceMutationResult reduce(SharedData& state, const TraceEventAppendedEvent& eventRequest) {
    std::lock_guard<std::recursive_mutex> lock(state.mutex);
    auto it = state.traces.find(eventRequest.traceId);
    if (it == state.traces.end()) {
        return {};
    }

    TraceEvent event;
    event.type = eventRequest.type;
    event.monoTs = std::chrono::steady_clock::now();
    event.wallTs = std::chrono::system_clock::now();
    event.stage = eventRequest.stage;
    event.details = eventRequest.details;
    event.cumFilled = eventRequest.cumFilled;
    event.remaining = eventRequest.remaining;
    event.price = eventRequest.price;
    event.shares = eventRequest.shares;
    event.errorCode = eventRequest.errorCode;

    TradeTrace& trace = it->second;
    if (trace.events.size() >= kMaxTraceEvents) {
        trace.events.erase(trace.events.begin(), trace.events.begin() + (trace.events.size() - kMaxTraceEvents + 1));
    }
    trace.events.push_back(event);
    state.latestTraceId = trace.traceId;

    if (event.type == TradeEventType::ValidationStart && !hasTime(trace.validationStartMono)) {
        trace.validationStartMono = event.monoTs;
    } else if ((event.type == TradeEventType::ValidationOk || event.type == TradeEventType::ValidationFailed) &&
               !hasTime(trace.validationEndMono)) {
        trace.validationEndMono = event.monoTs;
    } else if (event.type == TradeEventType::PlaceOrderCallStart && !hasTime(trace.placeCallStartMono)) {
        trace.placeCallStartMono = event.monoTs;
    } else if (event.type == TradeEventType::PlaceOrderCallEnd && !hasTime(trace.placeCallEndMono)) {
        trace.placeCallEndMono = event.monoTs;
    } else if (event.type == TradeEventType::CancelRequestSent && !hasTime(trace.cancelReqMono)) {
        trace.cancelReqMono = event.monoTs;
    }

    return {
        true,
        trace.traceId,
        event.type,
        event.stage,
        event.details,
        makeTraceEventLogLine(trace, event)
    };
}

void reduce(SharedData& state, const TraceValidationFailedMarkedEvent& event) {
    std::lock_guard<std::recursive_mutex> lock(state.mutex);
    auto it = state.traces.find(event.traceId);
    if (it == state.traces.end()) {
        return;
    }
    it->second.failedBeforeSubmit = true;
    it->second.latestError = event.reason;
    it->second.terminalStatus = "FailedBeforeSubmit";
}

TraceMutationResult reduce(SharedData& state, const TraceTerminalMarkedByOrderEvent& eventRequest) {
    std::lock_guard<std::recursive_mutex> lock(state.mutex);
    const auto traceId = findTraceIdLocked(eventRequest.orderId);
    if (traceId == 0) {
        return {};
    }
    auto it = state.traces.find(traceId);
    if (it == state.traces.end()) {
        return {};
    }
    TradeTrace& trace = it->second;
    if (trace.terminalStatus == eventRequest.terminalStatus && eventRequest.reason.empty()) {
        return {};
    }
    setTraceTerminalFields(trace, eventRequest.terminalStatus, eventRequest.reason);

    TraceEvent event;
    event.type = TradeEventType::FinalState;
    event.monoTs = std::chrono::steady_clock::now();
    event.wallTs = std::chrono::system_clock::now();
    event.stage = "Terminal";
    event.details = eventRequest.reason.empty()
        ? eventRequest.terminalStatus
        : eventRequest.terminalStatus + ": " + eventRequest.reason;
    if (trace.events.size() >= kMaxTraceEvents) {
        trace.events.erase(trace.events.begin(), trace.events.begin() + (trace.events.size() - kMaxTraceEvents + 1));
    }
    trace.events.push_back(event);

    return {
        true,
        traceId,
        event.type,
        event.stage,
        event.details,
        makeTraceEventLogLine(trace, event)
    };
}

TraceMutationResult reduce(SharedData& state, const TraceOpenOrderRecordedEvent& eventRequest) {
    std::lock_guard<std::recursive_mutex> lock(state.mutex);
    std::uint64_t traceId = findTraceIdLocked(eventRequest.orderId, static_cast<long long>(eventRequest.order.permId));
    if (traceId == 0) {
        traceId = ensureRecoveredTraceLocked(state,
                                             eventRequest.orderId,
                                             static_cast<long long>(eventRequest.order.permId),
                                             {},
                                             "Broker Reconcile",
                                             eventRequest.contract.symbol,
                                             eventRequest.order.action,
                                             toShareCount(DecimalFunctions::decimalToDouble(eventRequest.order.totalQuantity)),
                                             eventRequest.order.lmtPrice,
                                             eventRequest.order.account,
                                             "Recovered open order during startup reconciliation");
    }

    auto it = state.traces.find(traceId);
    if (it == state.traces.end()) {
        return {};
    }
    TradeTrace& trace = it->second;
    trace.symbol = eventRequest.contract.symbol;
    trace.side = eventRequest.order.action;
    trace.account = eventRequest.order.account;
    trace.limitPrice = eventRequest.order.lmtPrice;
    trace.requestedQty = toShareCount(DecimalFunctions::decimalToDouble(eventRequest.order.totalQuantity));
    if (eventRequest.order.permId > 0) {
        trace.permId = static_cast<long long>(eventRequest.order.permId);
        state.traceIdByPermId[trace.permId] = traceId;
    }
    if (!hasTime(trace.firstOpenOrderMono)) {
        trace.firstOpenOrderMono = std::chrono::steady_clock::now();
    }
    trace.latestStatus = eventRequest.orderState.status;

    TraceEvent event;
    event.type = TradeEventType::OpenOrderSeen;
    event.monoTs = std::chrono::steady_clock::now();
    event.wallTs = std::chrono::system_clock::now();
    event.stage = eventRequest.orderState.status.empty() ? "OpenOrder" : eventRequest.orderState.status;
    std::ostringstream oss;
    oss << eventRequest.order.action << ' ' << eventRequest.contract.symbol << ' ' << trace.requestedQty
        << " @ " << std::fixed << std::setprecision(2) << eventRequest.order.lmtPrice;
    event.details = oss.str();
    if (trace.events.size() >= kMaxTraceEvents) {
        trace.events.erase(trace.events.begin(), trace.events.begin() + (trace.events.size() - kMaxTraceEvents + 1));
    }
    trace.events.push_back(event);

    return {
        true,
        traceId,
        event.type,
        event.stage,
        event.details,
        makeTraceEventLogLine(trace, event)
    };
}

TraceStatusRecordResult reduce(SharedData& state, const TraceOrderStatusRecordedEvent& eventRequest) {
    std::lock_guard<std::recursive_mutex> lock(state.mutex);
    TraceStatusRecordResult result;
    result.orderId = eventRequest.orderId;
    result.filledQty = eventRequest.filledQty;
    result.remainingQty = eventRequest.remainingQty;
    result.avgFillPrice = eventRequest.avgFillPrice;

    std::uint64_t traceId = findTraceIdLocked(eventRequest.orderId, eventRequest.permId);
    if (traceId == 0) {
        std::string symbol;
        std::string side;
        int requestedQty = 0;
        double limitPrice = 0.0;
        std::string account;
        const auto orderIt = state.orders.find(eventRequest.orderId);
        if (orderIt != state.orders.end()) {
            symbol = orderIt->second.symbol;
            side = orderIt->second.side;
            requestedQty = toShareCount(orderIt->second.quantity);
            limitPrice = orderIt->second.limitPrice;
            account = orderIt->second.account;
        }
        traceId = ensureRecoveredTraceLocked(state,
                                             eventRequest.orderId,
                                             eventRequest.permId,
                                             {},
                                             "Broker Reconcile",
                                             symbol,
                                             side,
                                             requestedQty,
                                             limitPrice,
                                             account,
                                             "Recovered order status during startup reconciliation");
    }
    auto it = state.traces.find(traceId);
    if (it == state.traces.end()) {
        return result;
    }
    TradeTrace& trace = it->second;
    if (eventRequest.permId > 0) {
        trace.permId = eventRequest.permId;
        state.traceIdByPermId[eventRequest.permId] = traceId;
    }
    if (!hasTime(trace.firstStatusMono)) {
        trace.firstStatusMono = std::chrono::steady_clock::now();
    }
    trace.latestStatus = eventRequest.status;
    if (eventRequest.status == "Filled" && !hasTime(trace.fullFillMono)) {
        trace.fullFillMono = std::chrono::steady_clock::now();
    }

    TraceEvent event;
    event.type = TradeEventType::OrderStatusSeen;
    event.monoTs = std::chrono::steady_clock::now();
    event.wallTs = std::chrono::system_clock::now();
    event.stage = eventRequest.status;
    std::ostringstream oss;
    oss << "filled=" << std::fixed << std::setprecision(0) << eventRequest.filledQty
        << " remaining=" << eventRequest.remainingQty
        << " avgFill=" << std::setprecision(2) << eventRequest.avgFillPrice;
    if (eventRequest.lastFillPrice > 0.0) {
        oss << " lastFill=" << std::setprecision(2) << eventRequest.lastFillPrice;
    }
    if (eventRequest.mktCapPrice > 0.0) {
        oss << " mktCap=" << std::setprecision(2) << eventRequest.mktCapPrice;
    }
    event.details = oss.str();
    event.cumFilled = eventRequest.filledQty;
    event.remaining = eventRequest.remainingQty;
    event.price = eventRequest.avgFillPrice;
    if (trace.events.size() >= kMaxTraceEvents) {
        trace.events.erase(trace.events.begin(), trace.events.begin() + (trace.events.size() - kMaxTraceEvents + 1));
    }
    trace.events.push_back(event);

    result.mutation = {
        true,
        traceId,
        event.type,
        event.stage,
        event.details,
        makeTraceEventLogLine(trace, event)
    };

    if (eventRequest.status == "Cancelled" || eventRequest.status == "ApiCancelled") {
        result.appendCancelAck = true;
        result.appendTerminal = true;
        result.terminalReason = eventRequest.status;
        setTraceTerminalFields(trace, eventRequest.status, {});
    } else if (eventRequest.status == "Filled" || eventRequest.status == "Rejected" || eventRequest.status == "Inactive") {
        result.appendTerminal = true;
        result.terminalReason = eventRequest.status;
        setTraceTerminalFields(trace, eventRequest.status, {});
    }

    return result;
}

TraceMutationResult reduce(SharedData& state, const TraceExecutionRecordedEvent& eventRequest) {
    std::lock_guard<std::recursive_mutex> lock(state.mutex);
    const OrderId orderId = static_cast<OrderId>(eventRequest.execution.orderId);
    const long long permId = static_cast<long long>(eventRequest.execution.permId);
    std::uint64_t traceId = findTraceIdLocked(orderId, permId);
    if (traceId == 0) {
        traceId = ensureRecoveredTraceLocked(state,
                                             orderId,
                                             permId,
                                             eventRequest.execution.execId,
                                             "Broker Reconcile",
                                             eventRequest.contract.symbol,
                                             eventRequest.execution.side,
                                             toShareCount(DecimalFunctions::decimalToDouble(eventRequest.execution.cumQty)),
                                             eventRequest.execution.price,
                                             {},
                                             "Recovered execution during startup reconciliation");
    }

    auto it = state.traces.find(traceId);
    if (it == state.traces.end()) {
        return {};
    }
    TradeTrace& trace = it->second;

    if (permId > 0) {
        trace.permId = permId;
        state.traceIdByPermId[permId] = traceId;
    }
    if (!hasTime(trace.firstExecMono)) {
        trace.firstExecMono = std::chrono::steady_clock::now();
    }

    if (!eventRequest.execution.execId.empty()) {
        const auto existing = state.traceIdByExecId.find(eventRequest.execution.execId);
        if (existing != state.traceIdByExecId.end() && existing->second == traceId) {
            return {};
        }
    }

    FillSlice fill;
    fill.execId = eventRequest.execution.execId;
    fill.shares = toShareCount(DecimalFunctions::decimalToDouble(eventRequest.execution.shares));
    fill.price = eventRequest.execution.price;
    fill.exchange = eventRequest.execution.exchange;
    fill.liquidity = eventRequest.execution.lastLiquidity;
    fill.cumQty = DecimalFunctions::decimalToDouble(eventRequest.execution.cumQty);
    fill.avgPrice = eventRequest.execution.avgPrice;
    fill.execTimeText = eventRequest.execution.time;

    if (trace.fills.size() >= kMaxTraceFills) {
        trace.fills.erase(trace.fills.begin(), trace.fills.begin() + (trace.fills.size() - kMaxTraceFills + 1));
    }
    trace.fills.push_back(fill);
    state.traceIdByExecId[fill.execId] = traceId;

    TraceEvent event;
    event.type = TradeEventType::ExecDetailsSeen;
    event.monoTs = std::chrono::steady_clock::now();
    event.wallTs = std::chrono::system_clock::now();
    event.stage = "execDetails";
    std::ostringstream oss;
    oss << fill.shares << " @ " << std::fixed << std::setprecision(2) << fill.price
        << " exch=" << fill.exchange;
    if (!fill.execId.empty()) {
        oss << " execId=" << fill.execId;
    }
    if (!fill.execTimeText.empty()) {
        oss << " time=" << fill.execTimeText;
    }
    event.details = oss.str();
    event.cumFilled = fill.cumQty;
    event.price = fill.price;
    event.shares = fill.shares;
    if (trace.events.size() >= kMaxTraceEvents) {
        trace.events.erase(trace.events.begin(), trace.events.begin() + (trace.events.size() - kMaxTraceEvents + 1));
    }
    trace.events.push_back(event);

    if (trace.requestedQty > 0 && fill.cumQty >= static_cast<double>(trace.requestedQty) && !hasTime(trace.fullFillMono)) {
        trace.fullFillMono = event.monoTs;
    }
    trace.symbol = eventRequest.contract.symbol;

    return {
        true,
        traceId,
        event.type,
        event.stage,
        event.details,
        makeTraceEventLogLine(trace, event)
    };
}

TraceMutationResult reduce(SharedData& state, const TraceCommissionRecordedEvent& eventRequest) {
    std::lock_guard<std::recursive_mutex> lock(state.mutex);
    const double commission = commissionValue(eventRequest.commissionReport);
    const std::uint64_t traceId = findTraceIdLocked(0, 0, eventRequest.commissionReport.execId);
    if (traceId == 0) {
        return {};
    }
    auto it = state.traces.find(traceId);
    if (it == state.traces.end()) {
        return {};
    }
    TradeTrace& trace = it->second;

    trace.totalCommission += commission;
    trace.commissionCurrency = eventRequest.commissionReport.currency;
    for (auto rit = trace.fills.rbegin(); rit != trace.fills.rend(); ++rit) {
        if (rit->execId == eventRequest.commissionReport.execId) {
            rit->commission = commission;
            rit->commissionKnown = true;
            rit->commissionCurrency = eventRequest.commissionReport.currency;
            break;
        }
    }

    TraceEvent event;
    event.type = TradeEventType::CommissionSeen;
    event.monoTs = std::chrono::steady_clock::now();
    event.wallTs = std::chrono::system_clock::now();
    event.stage = "commissionReport";
    std::ostringstream oss;
    oss << eventRequest.commissionReport.execId << " commission=" << std::fixed << std::setprecision(4)
        << commission << ' ' << eventRequest.commissionReport.currency;
    event.details = oss.str();
    if (trace.events.size() >= kMaxTraceEvents) {
        trace.events.erase(trace.events.begin(), trace.events.begin() + (trace.events.size() - kMaxTraceEvents + 1));
    }
    trace.events.push_back(event);

    return {
        true,
        traceId,
        event.type,
        event.stage,
        event.details,
        makeTraceEventLogLine(trace, event)
    };
}

TraceErrorRecordResult reduce(SharedData& state, const TraceErrorRecordedEvent& eventRequest) {
    TraceErrorRecordResult result;
    if (eventRequest.id <= 0) {
        return result;
    }

    std::lock_guard<std::recursive_mutex> lock(state.mutex);
    const OrderId orderId = static_cast<OrderId>(eventRequest.id);
    const std::uint64_t traceId = findTraceIdLocked(orderId);
    if (traceId == 0) {
        return result;
    }
    auto it = state.traces.find(traceId);
    if (it == state.traces.end()) {
        return result;
    }
    TradeTrace& trace = it->second;
    trace.latestError = eventRequest.errorString;

    TraceEvent event;
    event.type = TradeEventType::ErrorSeen;
    event.monoTs = std::chrono::steady_clock::now();
    event.wallTs = std::chrono::system_clock::now();
    event.stage = "Error";
    std::ostringstream oss;
    oss << "code=" << eventRequest.errorCode << " msg=" << eventRequest.errorString;
    event.details = oss.str();
    event.errorCode = eventRequest.errorCode;
    if (trace.events.size() >= kMaxTraceEvents) {
        trace.events.erase(trace.events.begin(), trace.events.begin() + (trace.events.size() - kMaxTraceEvents + 1));
    }
    trace.events.push_back(event);

    result.mutation = {
        true,
        traceId,
        event.type,
        event.stage,
        event.details,
        makeTraceEventLogLine(trace, event)
    };
    result.orderId = orderId;
    result.errorCode = eventRequest.errorCode;

    if (eventRequest.errorCode == 202 || eventRequest.errorCode == 10147) {
        result.appendCancelAck = true;
        result.appendTerminal = true;
        result.terminalStatus = "Cancelled";
        setTraceTerminalFields(trace, result.terminalStatus, eventRequest.errorString);
    } else if (eventRequest.errorCode == 201) {
        result.appendTerminal = true;
        result.terminalStatus = "Rejected";
        setTraceTerminalFields(trace, result.terminalStatus, eventRequest.errorString);
    }

    return result;
}

} // namespace trading_engine

void flushTraceMutationResult(const trading_engine::TraceMutationResult& result) {
    if (!result.emitted) {
        return;
    }
    appendTradeTraceLogLine(result.line);
    emitMacTraceObservation(result.traceId, result.type, result.stage, result.details);
}

std::uint64_t beginTradeTrace(const SubmitIntent& intent) {
    auto result = invokeSharedDataMutation([&]() {
        return trading_engine::reduce(appState(), trading_engine::BeginTradeTraceEvent{intent});
    });
    flushTraceMutationResult(result);
    return result.traceId;
}

void bindTraceToOrder(std::uint64_t traceId, OrderId orderId) {
    invokeSharedDataMutation([&]() {
        trading_engine::reduce(appState(), trading_engine::TraceBoundToOrderEvent{traceId, orderId});
    });
}

void bindTraceToPermId(OrderId orderId, long long permId) {
    invokeSharedDataMutation([&]() {
        trading_engine::reduce(appState(), trading_engine::TraceBoundToPermIdEvent{orderId, permId});
    });
}

void appendTraceEventByTraceId(std::uint64_t traceId,
                               TradeEventType type,
                               const std::string& stage,
                               const std::string& details,
                               double cumFilled,
                               double remaining,
                               double price,
                               int shares,
                               int errorCode) {
    auto result = invokeSharedDataMutation([&]() {
        return trading_engine::reduce(appState(), trading_engine::TraceEventAppendedEvent{
            traceId, type, stage, details, cumFilled, remaining, price, shares, errorCode
        });
    });
    flushTraceMutationResult(result);
}

void appendTraceEventByOrderId(OrderId orderId,
                               TradeEventType type,
                               const std::string& stage,
                               const std::string& details,
                               double cumFilled,
                               double remaining,
                               double price,
                               int shares,
                               int errorCode) {
    const std::uint64_t traceId = findTraceIdByOrderId(orderId);
    if (traceId == 0) return;
    appendTraceEventByTraceId(traceId, type, stage, details, cumFilled, remaining, price, shares, errorCode);
}

void markTraceValidationFailed(std::uint64_t traceId, const std::string& reason) {
    invokeSharedDataMutation([&]() {
        trading_engine::reduce(appState(), trading_engine::TraceValidationFailedMarkedEvent{traceId, reason});
    });
    appendTraceEventByTraceId(traceId, TradeEventType::ValidationFailed,
                              "Validation", reason, -1.0, -1.0, 0.0, 0, -1);
    appendTraceEventByTraceId(traceId, TradeEventType::FinalState,
                              "Terminal", "FailedBeforeSubmit: " + reason);
}

void markTraceSubmitted(std::uint64_t traceId) {
    appendTraceEventByTraceId(traceId, TradeEventType::Note,
                              "LocalState", "Order stored locally and awaiting broker callbacks");
}

void markTraceTerminalByOrderId(OrderId orderId, const std::string& terminalStatus, const std::string& reason) {
    auto result = invokeSharedDataMutation([&]() {
        return trading_engine::reduce(appState(), trading_engine::TraceTerminalMarkedByOrderEvent{
            orderId, terminalStatus, reason
        });
    });
    flushTraceMutationResult(result);
}

void recordTraceOpenOrder(OrderId orderId, const Contract& contract, const Order& order, const OrderState& orderState) {
    auto result = invokeSharedDataMutation([&]() {
        return trading_engine::reduce(appState(), trading_engine::TraceOpenOrderRecordedEvent{
            orderId, contract, order, orderState
        });
    });
    flushTraceMutationResult(result);
}

void recordTraceOrderStatus(OrderId orderId,
                            const std::string& status,
                            double filledQty,
                            double remainingQty,
                            double avgFillPrice,
                            long long permId,
                            double lastFillPrice,
                            double mktCapPrice) {
    auto result = invokeSharedDataMutation([&]() {
        return trading_engine::reduce(appState(), trading_engine::TraceOrderStatusRecordedEvent{
            orderId, status, filledQty, remainingQty, avgFillPrice, permId, lastFillPrice, mktCapPrice
        });
    });
    flushTraceMutationResult(result.mutation);
    if (result.appendCancelAck) {
        appendTraceEventByOrderId(result.orderId, TradeEventType::CancelAck,
                                  "Cancel", "Broker acknowledged cancellation",
                                  result.filledQty, result.remainingQty, result.avgFillPrice);
    }
    if (result.appendTerminal) {
        markTraceTerminalByOrderId(result.orderId, result.terminalReason);
    }
}

void recordTraceExecution(const Contract& contract, const Execution& execution) {
    auto result = invokeSharedDataMutation([&]() {
        return trading_engine::reduce(appState(), trading_engine::TraceExecutionRecordedEvent{contract, execution});
    });
    flushTraceMutationResult(result);
    if (result.traceId != 0) {
        BridgeOutboxRecordInput bridgeRecord;
        bridgeRecord.recordType = "fill_execution";
        bridgeRecord.source = "BrokerExecution";
        bridgeRecord.symbol = contract.symbol;
        bridgeRecord.side = execution.side;
        bridgeRecord.traceId = result.traceId;
        bridgeRecord.orderId = static_cast<OrderId>(execution.orderId);
        bridgeRecord.permId = static_cast<long long>(execution.permId);
        bridgeRecord.execId = execution.execId;
        bridgeRecord.note = "execution details observed";
        enqueueBridgeOutboxRecord(bridgeRecord);
    }
}

void recordTraceCommission(const CommissionReport& commissionReport) {
    auto result = invokeSharedDataMutation([&]() {
        return trading_engine::reduce(appState(), trading_engine::TraceCommissionRecordedEvent{commissionReport});
    });
    flushTraceMutationResult(result);
}

void recordTraceError(int id, int errorCode, const std::string& errorString) {
    auto result = invokeSharedDataMutation([&]() {
        return trading_engine::reduce(appState(), trading_engine::TraceErrorRecordedEvent{id, errorCode, errorString});
    });
    flushTraceMutationResult(result.mutation);
    if (result.appendCancelAck) {
        appendTraceEventByOrderId(result.orderId, TradeEventType::CancelAck,
                                  "Cancel", errorString, -1.0, -1.0, 0.0, 0, result.errorCode);
    }
    if (result.appendTerminal) {
        markTraceTerminalByOrderId(result.orderId, result.terminalStatus, errorString);
    }
}

void recordTraceCancelRequest(OrderId orderId) {
    appendTraceEventByOrderId(orderId, TradeEventType::CancelRequestSent,
                              "Cancel", "Cancel request sent to TWS");
}

std::uint64_t findTraceIdByOrderId(OrderId orderId) {
    const auto published = ensurePublishedSharedDataSnapshot();
    if (orderId > 0) {
        const auto byOrder = published->traceIdByOrderId.find(orderId);
        if (byOrder != published->traceIdByOrderId.end()) {
            return byOrder->second;
        }
    }
    return 0;
}

std::uint64_t latestTradeTraceId() {
    return ensurePublishedSharedDataSnapshot()->latestTraceId;
}

std::vector<TradeTraceListItem> captureTradeTraceListItems(std::size_t maxItems) {
    std::vector<TradeTraceListItem> items;
    const auto published = ensurePublishedSharedDataSnapshot();
    items.reserve(std::min(maxItems, published->traceRecency.size()));
    for (auto it = published->traceRecency.rbegin();
         it != published->traceRecency.rend() && items.size() < maxItems;
         ++it) {
        const auto traceIt = published->traces.find(*it);
        if (traceIt == published->traces.end()) continue;
        items.push_back(makeTradeTraceListItem(traceIt->second));
    }
    return items;
}

TradeTraceSnapshot captureTradeTraceSnapshot(std::uint64_t traceId) {
    TradeTraceSnapshot snapshot;
    const auto published = ensurePublishedSharedDataSnapshot();
    auto it = published->traces.find(traceId);
    if (it == published->traces.end()) return snapshot;
    snapshot.found = true;
    snapshot.trace = it->second;
    return snapshot;
}

void resetSharedDataForTesting() {
    clearUiInvalidationCallback();
    clearSharedDataMutationDispatcher();

    SharedData fresh;
    SharedData& bootstrap = bootstrapSharedData();
    SharedData* active = activeSharedDataSlot().load(std::memory_order_acquire);
    if (active == nullptr) {
        active = &bootstrap;
    }

    if (active == &bootstrap) {
        std::scoped_lock lock(bootstrap.mutex);
        copySharedDataState(fresh, bootstrap);
    } else {
        std::scoped_lock lock(active->mutex, bootstrap.mutex);
        copySharedDataState(fresh, *active);
        copySharedDataState(fresh, bootstrap);
    }

    activeSharedDataSlot().store(&bootstrap, std::memory_order_release);
    publishSharedDataSnapshot();
}
