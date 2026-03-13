#include "app_shared.h"
#include "mac_observability.h"

#include <random>
#include <atomic>
#include <unordered_map>

namespace {
constexpr std::size_t kMaxTraceEvents = 512;
constexpr std::size_t kMaxTraceFills = 256;
constexpr auto kRecentSubmitWindow = std::chrono::milliseconds(750);
constexpr auto kRecentWebSocketOrderWindow = std::chrono::seconds(2);
constexpr int kMaxWebSocketOrdersPerWindow = 5;
constexpr auto kWebSocketIdempotencyWindow = std::chrono::minutes(10);
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
}

std::mutex& uiInvalidationCallbackMutex() {
    static std::mutex m;
    return m;
}

UiInvalidationCallback& uiInvalidationCallbackSlot() {
    static UiInvalidationCallback callback;
    return callback;
}

std::mutex& tradeTraceFileMutex() {
    static std::mutex m;
    return m;
}

std::mutex& runtimeJournalFileMutex() {
    static std::mutex m;
    return m;
}

std::uint64_t findTraceIdLocked(OrderId orderId, long long permId, const std::string& execId);
json makeTraceEventLogLine(const TradeTrace& trace, const TraceEvent& event);
void emitMacTraceObservation(std::uint64_t traceId,
                             TradeEventType type,
                             const std::string& stage,
                             const std::string& details);

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

void pruneWebSocketStateLocked(std::chrono::steady_clock::time_point now) {
    while (!g_data.recentWsOrderTimestamps.empty() &&
           (now - g_data.recentWsOrderTimestamps.front()) > kRecentWebSocketOrderWindow) {
        g_data.recentWsOrderTimestamps.pop_front();
    }

    for (auto it = g_data.recentWsIdempotencyKeys.begin(); it != g_data.recentWsIdempotencyKeys.end();) {
        if ((now - it->second) > kWebSocketIdempotencyWindow) {
            it = g_data.recentWsIdempotencyKeys.erase(it);
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

std::uint64_t makeRecoveredTraceId() {
    return g_data.nextTraceId.fetch_add(1, std::memory_order_relaxed);
}

std::uint64_t ensureRecoveredTraceLocked(OrderId orderId,
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

    TradeTrace trace;
    trace.traceId = makeRecoveredTraceId();
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

    g_data.traces[trace.traceId] = trace;
    g_data.traceRecency.push_back(trace.traceId);
    g_data.latestTraceId = trace.traceId;
    if (orderId > 0) {
        g_data.traceIdByOrderId[orderId] = trace.traceId;
    }
    if (permId > 0) {
        g_data.traceIdByPermId[permId] = trace.traceId;
    }
    if (!execId.empty()) {
        g_data.traceIdByExecId[execId] = trace.traceId;
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

SharedData& appState() {
    SharedData* active = activeSharedDataSlot().load(std::memory_order_acquire);
    return *(active != nullptr ? active : &bootstrapSharedData());
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
    activeSharedDataSlot().store(owner, std::memory_order_release);
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
}

void setUiInvalidationCallback(UiInvalidationCallback callback) {
    std::lock_guard<std::mutex> lock(uiInvalidationCallbackMutex());
    uiInvalidationCallbackSlot() = std::move(callback);
}

void clearUiInvalidationCallback() {
    std::lock_guard<std::mutex> lock(uiInvalidationCallbackMutex());
    uiInvalidationCallbackSlot() = nullptr;
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

void setRuntimeSessionState(RuntimeSessionState state) {
    bool changed = false;
    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        if (g_data.sessionState != state) {
            g_data.sessionState = state;
            changed = true;
        }
    }
    if (changed) {
        macLogInfo("runtime", "Session state changed: " + runtimeSessionStateToString(state));
        appendRuntimeJournalEvent("session_state_changed", {{"state", runtimeSessionStateToString(state)}});
        requestUiInvalidation();
    }
}

int allocateReqId() {
    return g_data.nextReqId.fetch_add(1);
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

double availableLongToCloseUnlocked(const std::string& account, const std::string& symbol) {
    double longPos = 0.0;
    auto posIt = g_data.positions.find(makePositionKey(account, symbol));
    if (posIt != g_data.positions.end()) {
        longPos = std::max(0.0, posIt->second.quantity);
    }

    double workingSellQty = 0.0;
    for (const auto& [id, ord] : g_data.orders) {
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

double calculateOpenBuyExposureUnlocked(const std::string& account) {
    double exposure = 0.0;
    for (const auto& [id, ord] : g_data.orders) {
        (void)id;
        if (!ord.account.empty() && ord.account != account) continue;
        if (ord.side != "BUY") continue;
        if (ord.isTerminal()) continue;
        exposure += outstandingOrderQty(ord) * ord.limitPrice;
    }
    return exposure;
}

double calculatePositionMarketValueUnlocked(const std::string& account, const std::string& symbol) {
    const auto posIt = g_data.positions.find(makePositionKey(account, symbol));
    if (posIt == g_data.positions.end()) {
        return 0.0;
    }
    const double qty = std::max(0.0, posIt->second.quantity);
    if (qty <= 0.0) {
        return 0.0;
    }

    double price = 0.0;
    if (symbol == g_data.currentSymbol) {
        if (g_data.lastPrice > 0.0) {
            price = g_data.lastPrice;
        } else if (g_data.askPrice > 0.0) {
            price = g_data.askPrice;
        } else if (g_data.bidPrice > 0.0) {
            price = g_data.bidPrice;
        }
    }
    if (price <= 0.0) {
        price = posIt->second.avgCost;
    }
    return qty * price;
}

UiStatusSnapshot captureUiStatusSnapshot() {
    UiStatusSnapshot snapshot;
    std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
    snapshot.connected = g_data.connected;
    snapshot.sessionReady = g_data.sessionReady;
    snapshot.sessionStateText = runtimeSessionStateToString(g_data.sessionState);
    snapshot.wsServerRunning = g_data.wsServerRunning.load();
    snapshot.websocketEnabled = g_data.websocketEnabled;
    snapshot.controllerConnected = g_data.controllerConnected.load();
    snapshot.controllerEnabled = g_data.controllerEnabled;
    snapshot.controllerArmed = g_data.controllerArmed;
    snapshot.tradingKillSwitch = g_data.tradingKillSwitch;
    snapshot.wsConnectedClients = g_data.wsConnectedClients.load();
    snapshot.controllerDeviceName = g_data.controllerDeviceName;
    snapshot.controllerLockedDeviceName = g_data.controllerLockedDeviceName;
    snapshot.websocketAuthToken = g_data.websocketAuthToken;
    snapshot.startupRecoveryBanner = g_data.startupRecoveryBanner;

    if (!g_data.selectedAccount.empty()) {
        snapshot.accountText = g_data.selectedAccount;
    } else if (!g_data.managedAccounts.empty()) {
        snapshot.accountText = "<configured account not found>";
    } else {
        snapshot.accountText = "<waiting for managedAccounts>";
    }
    return snapshot;
}

void consumeGuiSyncUpdates(std::string& symbolInput,
                           std::string& subscribedSymbol,
                           bool& subscribed,
                           int& quantityInput) {
    std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
    if (g_data.hasPendingSubscribe) {
        symbolInput = g_data.pendingSubscribeSymbol;
        subscribedSymbol = g_data.pendingSubscribeSymbol;
        subscribed = true;
        quantityInput = g_data.currentQuantity;
        g_data.hasPendingSubscribe = false;
    }
    if (g_data.wsQuantityUpdated) {
        quantityInput = g_data.currentQuantity;
        g_data.wsQuantityUpdated = false;
    }
}

void syncSharedGuiInputs(int quantityInput, double priceBuffer, double maxPositionDollars) {
    std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
    g_data.currentQuantity = quantityInput;
    g_data.priceBuffer = priceBuffer;
    g_data.maxPositionDollars = maxPositionDollars;
}

SymbolUiSnapshot captureSymbolUiSnapshot(const std::string& subscribedSymbol) {
    SymbolUiSnapshot snapshot;
    std::lock_guard<std::recursive_mutex> lock(g_data.mutex);

    snapshot.canTrade = g_data.connected && g_data.sessionReady && !g_data.selectedAccount.empty();
    snapshot.bidPrice = g_data.bidPrice;
    snapshot.askPrice = g_data.askPrice;
    snapshot.lastPrice = g_data.lastPrice;
    snapshot.askBook = g_data.askBook;
    snapshot.bidBook = g_data.bidBook;
    snapshot.openBuyExposure = calculateOpenBuyExposureUnlocked(g_data.selectedAccount);
    if (hasTime(g_data.lastQuoteUpdate)) {
        snapshot.quoteAgeMs = std::chrono::duration<double, std::milli>(
            std::chrono::steady_clock::now() - g_data.lastQuoteUpdate).count();
        snapshot.hasFreshQuote = snapshot.quoteAgeMs <= static_cast<double>(g_data.staleQuoteThresholdMs);
    }

    if (!g_data.selectedAccount.empty() && !subscribedSymbol.empty()) {
        auto posIt = g_data.positions.find(makePositionKey(g_data.selectedAccount, subscribedSymbol));
        if (posIt != g_data.positions.end()) {
            snapshot.currentPositionQty = posIt->second.quantity;
            snapshot.currentPositionAvgCost = posIt->second.avgCost;
            snapshot.hasPosition = true;
        }
        snapshot.availableLongToClose = availableLongToCloseUnlocked(g_data.selectedAccount, subscribedSymbol);
    }

    return snapshot;
}

RuntimeConnectionConfig captureRuntimeConnectionConfig() {
    RuntimeConnectionConfig config;
    std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
    config.host = g_data.twsHost;
    config.port = g_data.twsPort;
    config.clientId = g_data.twsClientId;
    config.websocketAuthToken = g_data.websocketAuthToken;
    config.websocketEnabled = g_data.websocketEnabled;
    config.controllerEnabled = g_data.controllerEnabled;
    return config;
}

void updateRuntimeConnectionConfig(const RuntimeConnectionConfig& config) {
    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        g_data.twsHost = config.host.empty() ? DEFAULT_HOST : config.host;
        g_data.twsPort = config.port > 0 ? config.port : DEFAULT_PORT;
        g_data.twsClientId = config.clientId > 0 ? config.clientId : DEFAULT_CLIENT_ID;
        g_data.websocketAuthToken = config.websocketAuthToken;
        g_data.websocketEnabled = config.websocketEnabled;
        g_data.controllerEnabled = config.controllerEnabled;
    }
    requestUiInvalidation();
}

RiskControlsSnapshot captureRiskControlsSnapshot() {
    RiskControlsSnapshot snapshot;
    std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
    snapshot.staleQuoteThresholdMs = g_data.staleQuoteThresholdMs;
    snapshot.maxOrderNotional = g_data.maxOrderNotional;
    snapshot.maxOpenNotional = g_data.maxOpenNotional;
    snapshot.controllerArmed = g_data.controllerArmed;
    snapshot.tradingKillSwitch = g_data.tradingKillSwitch;
    return snapshot;
}

void updateRiskControls(int staleQuoteThresholdMs, double maxOrderNotional, double maxOpenNotional) {
    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        g_data.staleQuoteThresholdMs = std::max(250, staleQuoteThresholdMs);
        g_data.maxOrderNotional = std::max(100.0, maxOrderNotional);
        g_data.maxOpenNotional = std::max(g_data.maxOrderNotional, maxOpenNotional);
    }
    requestUiInvalidation();
}

void setControllerArmed(bool armed) {
    bool changed = false;
    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        if (g_data.controllerArmed != armed) {
            g_data.controllerArmed = armed;
            changed = true;
        }
    }
    if (changed) {
        appendRuntimeJournalEvent("controller_armed_changed", {{"armed", armed}});
        requestUiInvalidation();
    }
}

void setTradingKillSwitch(bool enabled) {
    bool changed = false;
    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        if (g_data.tradingKillSwitch != enabled) {
            g_data.tradingKillSwitch = enabled;
            changed = true;
        }
    }
    if (changed) {
        appendRuntimeJournalEvent("trading_kill_switch_changed", {{"enabled", enabled}});
        requestUiInvalidation();
    }
}

std::string ensureWebSocketAuthToken() {
    std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
    if (g_data.websocketAuthToken.empty()) {
        g_data.websocketAuthToken = randomHexToken(16);
    }
    return g_data.websocketAuthToken;
}

bool consumeWebSocketOrderRateLimit(std::string* error) {
    std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
    const auto now = std::chrono::steady_clock::now();
    pruneWebSocketStateLocked(now);
    if (static_cast<int>(g_data.recentWsOrderTimestamps.size()) >= kMaxWebSocketOrdersPerWindow) {
        if (error) {
            *error = "Too many WebSocket order requests; slow down";
        }
        return false;
    }
    g_data.recentWsOrderTimestamps.push_back(now);
    return true;
}

bool reserveWebSocketIdempotencyKey(const std::string& key, std::string* error) {
    if (key.empty()) {
        if (error) {
            *error = "Missing required string field: idempotencyKey";
        }
        return false;
    }

    std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
    const auto now = std::chrono::steady_clock::now();
    pruneWebSocketStateLocked(now);
    const auto it = g_data.recentWsIdempotencyKeys.find(key);
    if (it != g_data.recentWsIdempotencyKeys.end()) {
        if (error) {
            *error = "Duplicate idempotencyKey";
        }
        return false;
    }
    g_data.recentWsIdempotencyKeys[key] = now;
    return true;
}

std::vector<std::pair<OrderId, OrderInfo>> captureOrdersSnapshot() {
    std::vector<std::pair<OrderId, OrderInfo>> ordersSnapshot;
    std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
    ordersSnapshot.reserve(g_data.orders.size());
    for (const auto& kv : g_data.orders) {
        ordersSnapshot.push_back(kv);
    }
    return ordersSnapshot;
}

std::vector<OrderId> markOrdersPendingCancel(const std::vector<OrderId>& orderIds) {
    std::vector<OrderId> marked;
    std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
    marked.reserve(orderIds.size());
    for (OrderId id : orderIds) {
        auto it = g_data.orders.find(id);
        if (it != g_data.orders.end() && !it->second.isTerminal() && !it->second.cancelPending) {
            it->second.cancelPending = true;
            marked.push_back(id);
        }
    }
    return marked;
}

std::vector<OrderId> markAllPendingOrdersForCancel() {
    std::vector<OrderId> pendingOrders;
    std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
    pendingOrders.reserve(g_data.orders.size());
    for (auto& [id, order] : g_data.orders) {
        if (!order.isTerminal() && !order.cancelPending) {
            order.cancelPending = true;
            pendingOrders.push_back(id);
        }
    }
    return pendingOrders;
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

    for (size_t i = 0; i < orderIds.size(); ++i) {
        if (sent[i]) {
            recordTraceCancelRequest(orderIds[i]);
            appendRuntimeJournalEvent("cancel_request_sent", {{"orderId", static_cast<long long>(orderIds[i])}});
        }
    }

    return sent;
}

int computeMaxQuantityFromAsk(double currentAsk, double maxPositionDollars) {
    if (currentAsk <= 0.0) return 1;
    const int maxQty = static_cast<int>(std::floor(maxPositionDollars / currentAsk));
    return maxQty < 1 ? 1 : maxQty;
}

void cancelActiveSubscription(EClientSocket* client) {
    int mktDataReqId = 0;
    int depthReqId = 0;

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        mktDataReqId = g_data.activeMktDataReqId;
        depthReqId = g_data.activeDepthReqId;

        if (mktDataReqId != 0) {
            g_data.suppressedMktDataCancelIds.insert(mktDataReqId);
            g_data.activeMktDataReqId = 0;
        }
        if (depthReqId != 0) {
            g_data.suppressedMktDepthCancelIds.insert(depthReqId);
            g_data.activeDepthReqId = 0;
        }

        g_data.depthSubscribed = false;
        g_data.bidPrice = 0.0;
        g_data.askPrice = 0.0;
        g_data.lastPrice = 0.0;
        g_data.askBook.clear();
        g_data.bidBook.clear();
    }

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

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        if (!(g_data.connected && g_data.sessionReady)) {
            if (error) *error = "TWS session not ready";
            return false;
        }

        if (g_data.currentSymbol == symbol &&
            g_data.activeMktDataReqId != 0 &&
            g_data.activeDepthReqId != 0) {
            return true;
        }
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

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        g_data.activeMktDataReqId = mktDataReqId;
        g_data.activeDepthReqId = depthReqId;
        g_data.depthSubscribed = true;
        g_data.currentSymbol = symbol;
        g_data.bidPrice = 0.0;
        g_data.askPrice = 0.0;
        g_data.lastPrice = 0.0;
        g_data.askBook.clear();
        g_data.bidBook.clear();
        g_data.pendingSubscribeSymbol = symbol;
        g_data.hasPendingSubscribe = true;
        g_data.pendingWSQuantityCalc = recalcQtyFromFirstAsk;
        g_data.wsQuantityUpdated = false;
        if (recalcQtyFromFirstAsk) {
            g_data.currentQuantity = 1;
        }
    }

    g_data.addMessage("Subscription request sent for " + symbol);
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

    std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
    if (intent.symbol == g_data.currentSymbol) {
        intent.decisionBid = g_data.bidPrice;
        intent.decisionAsk = g_data.askPrice;
        intent.decisionLast = g_data.lastPrice;
        intent.decisionAskLevels = static_cast<int>(g_data.askBook.size());
        intent.decisionBidLevels = static_cast<int>(g_data.bidBook.size());
        intent.bookSummary = buildBookSummary(g_data.askBook, g_data.bidBook);
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
    bool controllerArmed = false;
    bool tradingKillSwitch = false;
    std::string duplicateFingerprint;

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        ready = g_data.connected && g_data.sessionReady;
        account = g_data.selectedAccount;
        staleQuoteThresholdMs = g_data.staleQuoteThresholdMs;
        maxOrderNotional = g_data.maxOrderNotional;
        maxOpenNotional = g_data.maxOpenNotional;
        controllerArmed = g_data.controllerArmed;
        tradingKillSwitch = g_data.tradingKillSwitch;
        openBuyExposure = calculateOpenBuyExposureUnlocked(account);
        currentPositionValue = calculatePositionMarketValueUnlocked(account, symbol);
        quoteMatchesCurrentSymbol = (symbol == g_data.currentSymbol);
        if (hasTime(g_data.lastQuoteUpdate) && quoteMatchesCurrentSymbol) {
            quoteAgeMs = std::chrono::duration<double, std::milli>(
                std::chrono::steady_clock::now() - g_data.lastQuoteUpdate).count();
        }
        if (closeOnly) {
            availableToClose = availableLongToCloseUnlocked(account, symbol);
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
    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        if (g_data.lastSubmitFingerprint == duplicateFingerprint &&
            hasTime(g_data.lastSubmitTime) &&
            (std::chrono::steady_clock::now() - g_data.lastSubmitTime) <= kRecentSubmitWindow) {
            return failValidation("Duplicate order suppressed");
        }
        g_data.lastSubmitFingerprint = duplicateFingerprint;
        g_data.lastSubmitTime = std::chrono::steady_clock::now();
    }

    const OrderId nextId = g_data.nextOrderId.load(std::memory_order_relaxed);
    if (nextId <= 0) {
        return failValidation("No valid order ID yet");
    }

    const OrderId orderId = g_data.nextOrderId.fetch_add(1);
    if (orderId <= 0) {
        return failValidation("Failed to allocate order ID");
    }

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

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        OrderInfo info;
        info.orderId = orderId;
        info.account = account;
        info.symbol = symbol;
        info.side = action;
        info.quantity = quantity;
        info.limitPrice = limitPrice;
        info.status = "Submitted";
        info.submitTime = std::chrono::steady_clock::now();
        info.filledQty = 0.0;
        info.remainingQty = quantity;
        g_data.orders[orderId] = info;
    }

    markTraceSubmitted(traceId);

    char msg[256];
    std::snprintf(msg, sizeof(msg), "%s %.0f %s @ %.2f (ID: %lld, acct: %s)",
                  action.c_str(), quantity, symbol.c_str(), limitPrice,
                  static_cast<long long>(orderId), account.c_str());
    g_data.addMessage(msg);

    if (outOrderId) *outOrderId = orderId;
    if (effectiveIntent.source.find("Controller") != std::string::npos) {
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
    std::ofstream out(TRADE_TRACE_LOG_FILENAME, std::ios::app);
    if (!out.is_open()) return;
    out << line.dump() << '\n';
}

void appendRuntimeJournalLine(const json& line) {
    std::lock_guard<std::mutex> lock(runtimeJournalFileMutex());
    std::ofstream out(RUNTIME_JOURNAL_LOG_FILENAME, std::ios::app);
    if (!out.is_open()) return;
    out << line.dump() << '\n';
}

void appendRuntimeJournalEvent(const std::string& event, const json& details) {
    appendRuntimeJournalLine(makeRuntimeJournalLine(event, details));
}

std::vector<std::string> recoverUnfinishedTraceSummariesFromLog(std::size_t maxItems) {
    std::ifstream in(TRADE_TRACE_LOG_FILENAME);
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

    std::ifstream in(RUNTIME_JOURNAL_LOG_FILENAME);
    if (!in.is_open()) {
        return snapshot;
    }

    struct SessionInfo {
        bool started = false;
        bool cleanShutdown = false;
        std::string appSessionId;
        std::string runtimeSessionId;
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
        if (event == "runtime_start") {
            info.started = true;
        } else if (event == "runtime_shutdown") {
            info.cleanShutdown = true;
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
    return snapshot;
}

std::uint64_t beginTradeTrace(const SubmitIntent& intent) {
    TradeTrace trace;
    trace.traceId = g_data.nextTraceId.fetch_add(1, std::memory_order_relaxed);
    trace.source = intent.source;
    trace.symbol = intent.symbol;
    trace.side = intent.side;
    trace.requestedQty = intent.requestedQty;
    trace.limitPrice = intent.limitPrice;
    trace.closeOnly = intent.closeOnly;
    trace.decisionBid = intent.decisionBid;
    trace.decisionAsk = intent.decisionAsk;
    trace.decisionLast = intent.decisionLast;
    trace.sweepEstimate = intent.sweepEstimate;
    trace.priceBuffer = intent.priceBuffer;
    trace.decisionAskLevels = intent.decisionAskLevels;
    trace.decisionBidLevels = intent.decisionBidLevels;
    trace.bookSummary = intent.bookSummary;
    trace.notes = intent.notes;
    trace.triggerMono = intent.triggerMono;
    trace.triggerWall = intent.triggerWall;

    TraceEvent triggerEvent;
    triggerEvent.type = TradeEventType::Trigger;
    triggerEvent.monoTs = intent.triggerMono;
    triggerEvent.wallTs = intent.triggerWall;
    triggerEvent.stage = intent.source;
    std::ostringstream details;
    details << intent.side << ' ' << intent.requestedQty << ' ' << intent.symbol
            << " @ " << std::fixed << std::setprecision(2) << intent.limitPrice;
    if (!intent.notes.empty()) {
        details << " | " << intent.notes;
    }
    triggerEvent.details = details.str();
    trace.events.push_back(triggerEvent);

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        g_data.traces[trace.traceId] = trace;
        g_data.traceRecency.push_back(trace.traceId);
        g_data.latestTraceId = trace.traceId;
    }

    appendTradeTraceLogLine(makeTraceEventLogLine(trace, triggerEvent));
    emitMacTraceObservation(trace.traceId, triggerEvent.type, triggerEvent.stage, triggerEvent.details);
    return trace.traceId;
}

void bindTraceToOrder(std::uint64_t traceId, OrderId orderId) {
    std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
    auto it = g_data.traces.find(traceId);
    if (it == g_data.traces.end()) return;
    it->second.orderId = orderId;
    g_data.traceIdByOrderId[orderId] = traceId;
}

void bindTraceToPermId(OrderId orderId, long long permId) {
    if (permId <= 0) return;
    std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
    const auto traceId = findTraceIdLocked(orderId);
    if (traceId == 0) return;
    auto it = g_data.traces.find(traceId);
    if (it == g_data.traces.end()) return;
    it->second.permId = permId;
    g_data.traceIdByPermId[permId] = traceId;
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
    json line;
    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        auto it = g_data.traces.find(traceId);
        if (it == g_data.traces.end()) return;

        TraceEvent event;
        event.type = type;
        event.monoTs = std::chrono::steady_clock::now();
        event.wallTs = std::chrono::system_clock::now();
        event.stage = stage;
        event.details = details;
        event.cumFilled = cumFilled;
        event.remaining = remaining;
        event.price = price;
        event.shares = shares;
        event.errorCode = errorCode;

        TradeTrace& trace = it->second;
        if (trace.events.size() >= kMaxTraceEvents) {
            trace.events.erase(trace.events.begin(), trace.events.begin() + (trace.events.size() - kMaxTraceEvents + 1));
        }
        trace.events.push_back(event);
        g_data.latestTraceId = trace.traceId;

        if (type == TradeEventType::ValidationStart && !hasTime(trace.validationStartMono)) {
            trace.validationStartMono = event.monoTs;
        } else if ((type == TradeEventType::ValidationOk || type == TradeEventType::ValidationFailed) && !hasTime(trace.validationEndMono)) {
            trace.validationEndMono = event.monoTs;
        } else if (type == TradeEventType::PlaceOrderCallStart && !hasTime(trace.placeCallStartMono)) {
            trace.placeCallStartMono = event.monoTs;
        } else if (type == TradeEventType::PlaceOrderCallEnd && !hasTime(trace.placeCallEndMono)) {
            trace.placeCallEndMono = event.monoTs;
        } else if (type == TradeEventType::CancelRequestSent && !hasTime(trace.cancelReqMono)) {
            trace.cancelReqMono = event.monoTs;
        }

        line = makeTraceEventLogLine(trace, event);
    }
    appendTradeTraceLogLine(line);
    emitMacTraceObservation(traceId, type, stage, details);
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
    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        auto it = g_data.traces.find(traceId);
        if (it == g_data.traces.end()) return;
        it->second.failedBeforeSubmit = true;
        it->second.latestError = reason;
        it->second.terminalStatus = "FailedBeforeSubmit";
    }
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
    json line;
    bool shouldLog = false;
    std::uint64_t traceId = 0;
    std::string finalDetails;
    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        traceId = findTraceIdLocked(orderId);
        if (traceId == 0) return;
        auto it = g_data.traces.find(traceId);
        if (it == g_data.traces.end()) return;
        TradeTrace& trace = it->second;
        if (trace.terminalStatus == terminalStatus && reason.empty()) {
            return;
        }
        setTraceTerminalFields(trace, terminalStatus, reason);

        TraceEvent event;
        event.type = TradeEventType::FinalState;
        event.monoTs = std::chrono::steady_clock::now();
        event.wallTs = std::chrono::system_clock::now();
        event.stage = "Terminal";
        event.details = reason.empty() ? terminalStatus : terminalStatus + ": " + reason;
        finalDetails = event.details;
        if (trace.events.size() >= kMaxTraceEvents) {
            trace.events.erase(trace.events.begin(), trace.events.begin() + (trace.events.size() - kMaxTraceEvents + 1));
        }
        trace.events.push_back(event);
        line = makeTraceEventLogLine(trace, event);
        shouldLog = true;
    }
    if (shouldLog) {
        appendTradeTraceLogLine(line);
        emitMacTraceObservation(traceId, TradeEventType::FinalState, "Terminal", finalDetails);
    }
}

void recordTraceOpenOrder(OrderId orderId, const Contract& contract, const Order& order, const OrderState& orderState) {
    json line;
    std::uint64_t traceId = 0;
    std::string eventStage;
    std::string eventDetails;
    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        traceId = findTraceIdLocked(orderId, static_cast<long long>(order.permId));
        if (traceId == 0) {
            traceId = ensureRecoveredTraceLocked(orderId,
                                                 static_cast<long long>(order.permId),
                                                 {},
                                                 "Broker Reconcile",
                                                 contract.symbol,
                                                 order.action,
                                                 toShareCount(DecimalFunctions::decimalToDouble(order.totalQuantity)),
                                                 order.lmtPrice,
                                                 order.account,
                                                 "Recovered open order during startup reconciliation");
        }
        auto it = g_data.traces.find(traceId);
        if (it == g_data.traces.end()) return;
        TradeTrace& trace = it->second;
        trace.symbol = contract.symbol;
        trace.side = order.action;
        trace.account = order.account;
        trace.limitPrice = order.lmtPrice;
        trace.requestedQty = toShareCount(DecimalFunctions::decimalToDouble(order.totalQuantity));
        if (order.permId > 0) {
            trace.permId = static_cast<long long>(order.permId);
            g_data.traceIdByPermId[trace.permId] = traceId;
        }
        if (!hasTime(trace.firstOpenOrderMono)) {
            trace.firstOpenOrderMono = std::chrono::steady_clock::now();
        }
        trace.latestStatus = orderState.status;

        TraceEvent event;
        event.type = TradeEventType::OpenOrderSeen;
        event.monoTs = std::chrono::steady_clock::now();
        event.wallTs = std::chrono::system_clock::now();
        event.stage = orderState.status.empty() ? "OpenOrder" : orderState.status;
        std::ostringstream oss;
        oss << order.action << ' ' << contract.symbol << ' ' << trace.requestedQty
            << " @ " << std::fixed << std::setprecision(2) << order.lmtPrice;
        event.details = oss.str();
        if (trace.events.size() >= kMaxTraceEvents) {
            trace.events.erase(trace.events.begin(), trace.events.begin() + (trace.events.size() - kMaxTraceEvents + 1));
        }
        trace.events.push_back(event);
        eventStage = event.stage;
        eventDetails = event.details;
        line = makeTraceEventLogLine(trace, event);
    }
    appendTradeTraceLogLine(line);
    emitMacTraceObservation(traceId, TradeEventType::OpenOrderSeen, eventStage, eventDetails);
}

void recordTraceOrderStatus(OrderId orderId,
                            const std::string& status,
                            double filledQty,
                            double remainingQty,
                            double avgFillPrice,
                            long long permId,
                            double lastFillPrice,
                            double mktCapPrice) {
    json line;
    bool appendCancelAck = false;
    bool appendTerminal = false;
    std::string terminalReason;
    std::uint64_t traceId = 0;
    std::string eventDetails;

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        traceId = findTraceIdLocked(orderId, permId);
        if (traceId == 0) {
            std::string symbol;
            std::string side;
            int requestedQty = 0;
            double limitPrice = 0.0;
            std::string account;
            const auto orderIt = g_data.orders.find(orderId);
            if (orderIt != g_data.orders.end()) {
                symbol = orderIt->second.symbol;
                side = orderIt->second.side;
                requestedQty = toShareCount(orderIt->second.quantity);
                limitPrice = orderIt->second.limitPrice;
                account = orderIt->second.account;
            }
            traceId = ensureRecoveredTraceLocked(orderId,
                                                 permId,
                                                 {},
                                                 "Broker Reconcile",
                                                 symbol,
                                                 side,
                                                 requestedQty,
                                                 limitPrice,
                                                 account,
                                                 "Recovered order status during startup reconciliation");
        }
        auto it = g_data.traces.find(traceId);
        if (it == g_data.traces.end()) return;
        TradeTrace& trace = it->second;
        if (permId > 0) {
            trace.permId = permId;
            g_data.traceIdByPermId[permId] = traceId;
        }
        if (!hasTime(trace.firstStatusMono)) {
            trace.firstStatusMono = std::chrono::steady_clock::now();
        }
        trace.latestStatus = status;
        if (status == "Filled" && !hasTime(trace.fullFillMono)) {
            trace.fullFillMono = std::chrono::steady_clock::now();
        }

        TraceEvent event;
        event.type = TradeEventType::OrderStatusSeen;
        event.monoTs = std::chrono::steady_clock::now();
        event.wallTs = std::chrono::system_clock::now();
        event.stage = status;
        std::ostringstream oss;
        oss << "filled=" << std::fixed << std::setprecision(0) << filledQty
            << " remaining=" << remainingQty
            << " avgFill=" << std::setprecision(2) << avgFillPrice;
        if (lastFillPrice > 0.0) {
            oss << " lastFill=" << std::setprecision(2) << lastFillPrice;
        }
        if (mktCapPrice > 0.0) {
            oss << " mktCap=" << std::setprecision(2) << mktCapPrice;
        }
        event.details = oss.str();
        event.cumFilled = filledQty;
        event.remaining = remainingQty;
        event.price = avgFillPrice;
        if (trace.events.size() >= kMaxTraceEvents) {
            trace.events.erase(trace.events.begin(), trace.events.begin() + (trace.events.size() - kMaxTraceEvents + 1));
        }
        trace.events.push_back(event);
        eventDetails = event.details;
        line = makeTraceEventLogLine(trace, event);

        if (status == "Cancelled" || status == "ApiCancelled") {
            appendCancelAck = true;
            terminalReason = status;
            setTraceTerminalFields(trace, status, {});
            appendTerminal = true;
        } else if (status == "Filled" || status == "Rejected" || status == "Inactive") {
            terminalReason = status;
            setTraceTerminalFields(trace, status, {});
            appendTerminal = true;
        }
    }

    appendTradeTraceLogLine(line);
    emitMacTraceObservation(traceId, TradeEventType::OrderStatusSeen, status, eventDetails);

    if (appendCancelAck) {
        appendTraceEventByOrderId(orderId, TradeEventType::CancelAck,
                                  "Cancel", "Broker acknowledged cancellation", filledQty, remainingQty, avgFillPrice);
    }
    if (appendTerminal) {
        markTraceTerminalByOrderId(orderId, terminalReason);
    }
}

void recordTraceExecution(const Contract& contract, const Execution& execution) {
    json line;
    std::uint64_t traceId = 0;
    std::string eventDetails;
    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        const OrderId orderId = static_cast<OrderId>(execution.orderId);
        const long long permId = static_cast<long long>(execution.permId);
        traceId = findTraceIdLocked(orderId, permId);
        if (traceId == 0) {
            traceId = ensureRecoveredTraceLocked(orderId,
                                                 permId,
                                                 execution.execId,
                                                 "Broker Reconcile",
                                                 contract.symbol,
                                                 execution.side,
                                                 toShareCount(DecimalFunctions::decimalToDouble(execution.cumQty)),
                                                 execution.price,
                                                 {},
                                                 "Recovered execution during startup reconciliation");
        }
        auto it = g_data.traces.find(traceId);
        if (it == g_data.traces.end()) return;
        TradeTrace& trace = it->second;

        if (permId > 0) {
            trace.permId = permId;
            g_data.traceIdByPermId[permId] = traceId;
        }
        if (!hasTime(trace.firstExecMono)) {
            trace.firstExecMono = std::chrono::steady_clock::now();
        }

        FillSlice fill;
        fill.execId = execution.execId;
        fill.shares = toShareCount(DecimalFunctions::decimalToDouble(execution.shares));
        fill.price = execution.price;
        fill.exchange = execution.exchange;
        fill.liquidity = execution.lastLiquidity;
        fill.cumQty = DecimalFunctions::decimalToDouble(execution.cumQty);
        fill.avgPrice = execution.avgPrice;
        fill.execTimeText = execution.time;

        if (trace.fills.size() >= kMaxTraceFills) {
            trace.fills.erase(trace.fills.begin(), trace.fills.begin() + (trace.fills.size() - kMaxTraceFills + 1));
        }
        trace.fills.push_back(fill);
        g_data.traceIdByExecId[fill.execId] = traceId;

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
        eventDetails = event.details;
        line = makeTraceEventLogLine(trace, event);

        if (trace.requestedQty > 0 && fill.cumQty >= static_cast<double>(trace.requestedQty) && !hasTime(trace.fullFillMono)) {
            trace.fullFillMono = event.monoTs;
        }
        trace.symbol = contract.symbol;
    }

    appendTradeTraceLogLine(line);
    emitMacTraceObservation(traceId, TradeEventType::ExecDetailsSeen, "execDetails", eventDetails);
}

void recordTraceCommission(const CommissionReport& commissionReport) {
    json line;
    std::uint64_t traceId = 0;
    std::string eventDetails;
    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        const double commission = commissionValue(commissionReport);
        traceId = findTraceIdLocked(0, 0, commissionReport.execId);
        if (traceId == 0) return;
        auto it = g_data.traces.find(traceId);
        if (it == g_data.traces.end()) return;
        TradeTrace& trace = it->second;

        trace.totalCommission += commission;
        trace.commissionCurrency = commissionReport.currency;
        for (auto rit = trace.fills.rbegin(); rit != trace.fills.rend(); ++rit) {
            if (rit->execId == commissionReport.execId) {
                rit->commission = commission;
                rit->commissionKnown = true;
                rit->commissionCurrency = commissionReport.currency;
                break;
            }
        }

        TraceEvent event;
        event.type = TradeEventType::CommissionSeen;
        event.monoTs = std::chrono::steady_clock::now();
        event.wallTs = std::chrono::system_clock::now();
        event.stage = "commissionReport";
        std::ostringstream oss;
        oss << commissionReport.execId << " commission=" << std::fixed << std::setprecision(4)
            << commission << ' ' << commissionReport.currency;
        event.details = oss.str();
        if (trace.events.size() >= kMaxTraceEvents) {
            trace.events.erase(trace.events.begin(), trace.events.begin() + (trace.events.size() - kMaxTraceEvents + 1));
        }
        trace.events.push_back(event);
        eventDetails = event.details;
        line = makeTraceEventLogLine(trace, event);
    }
    appendTradeTraceLogLine(line);
    emitMacTraceObservation(traceId, TradeEventType::CommissionSeen, "commissionReport", eventDetails);
}

void recordTraceError(int id, int errorCode, const std::string& errorString) {
    if (id <= 0) return;

    json line;
    bool markCancel = false;
    bool markTerminal = false;
    std::string terminalStatus;
    std::uint64_t traceId = 0;
    std::string eventDetails;

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        traceId = findTraceIdLocked(static_cast<OrderId>(id));
        if (traceId == 0) return;
        auto it = g_data.traces.find(traceId);
        if (it == g_data.traces.end()) return;
        TradeTrace& trace = it->second;
        trace.latestError = errorString;

        TraceEvent event;
        event.type = TradeEventType::ErrorSeen;
        event.monoTs = std::chrono::steady_clock::now();
        event.wallTs = std::chrono::system_clock::now();
        event.stage = "Error";
        std::ostringstream oss;
        oss << "code=" << errorCode << " msg=" << errorString;
        event.details = oss.str();
        event.errorCode = errorCode;
        if (trace.events.size() >= kMaxTraceEvents) {
            trace.events.erase(trace.events.begin(), trace.events.begin() + (trace.events.size() - kMaxTraceEvents + 1));
        }
        trace.events.push_back(event);
        eventDetails = event.details;
        line = makeTraceEventLogLine(trace, event);

        if (errorCode == 202 || errorCode == 10147) {
            markCancel = true;
            markTerminal = true;
            terminalStatus = "Cancelled";
            setTraceTerminalFields(trace, terminalStatus, errorString);
        } else if (errorCode == 201) {
            markTerminal = true;
            terminalStatus = "Rejected";
            setTraceTerminalFields(trace, terminalStatus, errorString);
        }
    }

    appendTradeTraceLogLine(line);
    emitMacTraceObservation(traceId, TradeEventType::ErrorSeen, "Error", eventDetails);

    if (markCancel) {
        appendTraceEventByOrderId(static_cast<OrderId>(id), TradeEventType::CancelAck,
                                  "Cancel", errorString, -1.0, -1.0, 0.0, 0, errorCode);
    }
    if (markTerminal) {
        markTraceTerminalByOrderId(static_cast<OrderId>(id), terminalStatus, errorString);
    }
}

void recordTraceCancelRequest(OrderId orderId) {
    appendTraceEventByOrderId(orderId, TradeEventType::CancelRequestSent,
                              "Cancel", "Cancel request sent to TWS");
}

std::uint64_t findTraceIdByOrderId(OrderId orderId) {
    std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
    return findTraceIdLocked(orderId);
}

std::uint64_t latestTradeTraceId() {
    std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
    return g_data.latestTraceId;
}

std::vector<TradeTraceListItem> captureTradeTraceListItems(std::size_t maxItems) {
    std::vector<TradeTraceListItem> items;
    std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
    items.reserve(std::min(maxItems, g_data.traceRecency.size()));
    for (auto it = g_data.traceRecency.rbegin(); it != g_data.traceRecency.rend() && items.size() < maxItems; ++it) {
        const auto traceIt = g_data.traces.find(*it);
        if (traceIt == g_data.traces.end()) continue;
        const TradeTrace& trace = traceIt->second;
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
        items.push_back(std::move(item));
    }
    return items;
}

TradeTraceSnapshot captureTradeTraceSnapshot(std::uint64_t traceId) {
    TradeTraceSnapshot snapshot;
    std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
    auto it = g_data.traces.find(traceId);
    if (it == g_data.traces.end()) return snapshot;
    snapshot.found = true;
    snapshot.trace = it->second;
    return snapshot;
}
