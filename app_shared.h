#pragma once

#ifndef NOMINMAX
#define NOMINMAX
#endif

#include "DefaultEWrapper.h"
#include "EReaderOSSignal.h"
#include "EReader.h"
#include "EClientSocket.h"
#include "Contract.h"
#include "Order.h"
#include "OrderState.h"
#if defined(TWS_HAS_ORDER_CANCEL_OBJECT)
#include "OrderCancel.h"
#endif
#include "Execution.h"
#if defined(TWS_HAS_COMMISSION_AND_FEES_REPORT)
#include "CommissionAndFeesReport.h"
using CommissionReport = CommissionAndFeesReport;

inline double commissionValue(const CommissionReport& report) {
    return report.commissionAndFees;
}
#else
#include "CommissionReport.h"

inline double commissionValue(const CommissionReport& report) {
    return report.commission;
}
#endif
#include "Decimal.h"
#if defined(TWS_HAS_PROTOBUF_API)
#include "ErrorMessage.pb.h"
#include "ManagedAccounts.pb.h"
#include "TickPrice.pb.h"
#include "Position.pb.h"
#include "PositionEnd.pb.h"
#endif

// Standard library
#include <iostream>
#include <string>
#include <thread>
#include <atomic>
#include <mutex>
#include <vector>
#include <map>
#include <set>
#include <deque>
#include <cstdint>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <utility>
#include <iterator>
#include <algorithm>
#include <cctype>
#include <functional>
#include <cmath>
#include <fstream>
#include <sstream>
#include <iomanip>

// WebSocket server
#include <ixwebsocket/IXWebSocketServer.h>
#include <ixwebsocket/IXNetSystem.h>

// JSON parsing
#include <nlohmann/json.hpp>
using json = nlohmann::json;

#if defined(TWS_NEEDS_DECIMAL_FUNCTIONS_SHIM)
namespace DecimalFunctions {
inline double decimalToDouble(Decimal value) {
    return ::decimalToDouble(value);
}

inline Decimal doubleToDecimal(double value) {
    return ::doubleToDecimal(value);
}
} // namespace DecimalFunctions
#endif

std::string toUpperCase(const std::string& str);

inline constexpr const char* DEFAULT_HOST = "127.0.0.1";
inline constexpr int DEFAULT_PORT = 7496;
inline constexpr int DEFAULT_CLIENT_ID = 101;
inline constexpr const char* DEFAULT_SYMBOL = "QBTS";
inline constexpr const char* HARDCODED_ACCOUNT = "U23154741";
inline constexpr int FIRST_DYNAMIC_REQ_ID = 1001;
inline constexpr int MARKET_DEPTH_NUM_ROWS = 20;
inline constexpr int WEBSOCKET_PORT = 8080;
inline constexpr const char* WEBSOCKET_HOST = "127.0.0.1"; // localhost only
inline constexpr const char* TRADE_TRACE_LOG_FILENAME = "trade_trace_events.jsonl";
inline constexpr const char* RUNTIME_JOURNAL_LOG_FILENAME = "trade_runtime_journal.jsonl";

enum class ControllerArmMode {
    OneShot = 0,
    Manual = 1,
};

using UiInvalidationCallback = std::function<void()>;
using SharedDataMutationDispatcher = std::function<void(std::function<void()>)>;

void setUiInvalidationCallback(UiInvalidationCallback callback);
void clearUiInvalidationCallback();
void requestUiInvalidation();
void setSharedDataMutationDispatcher(SharedDataMutationDispatcher dispatcher);
void clearSharedDataMutationDispatcher();
void publishSharedDataSnapshot();

enum class LocalOrderState {
    IntentAccepted = 0,
    SentToBroker,
    AwaitingBrokerEcho,
    Working,
    PartiallyFilled,
    CancelRequested,
    AwaitingCancelAck,
    Filled,
    Cancelled,
    Rejected,
    Inactive,
    NeedsReconciliation,
    NeedsManualReview
};

struct OrderWatchdogs {
    std::chrono::steady_clock::time_point brokerEchoDeadline{};
    std::chrono::steady_clock::time_point cancelAckDeadline{};
    std::chrono::steady_clock::time_point partialFillQuietDeadline{};

    bool brokerEchoArmed = false;
    bool cancelAckArmed = false;
    bool partialFillQuietArmed = false;

    int reconciliationAttempts = 0;
    std::chrono::steady_clock::time_point lastBrokerCallback{};
};

struct OrderInfo {
    OrderId orderId = 0;
    std::string account;
    std::string symbol;
    std::string side;  // "BUY" or "SELL"
    double quantity = 0.0;
    double limitPrice = 0.0;
    std::string status;
    double filledQty = 0.0;
    double remainingQty = 0.0;
    double avgFillPrice = 0.0;
    LocalOrderState localState = LocalOrderState::NeedsReconciliation;
    bool cancelPending = false;
    OrderWatchdogs watchdogs;
    std::string lastReconciliationReason;
    std::chrono::steady_clock::time_point lastReconciliationTime{};
    bool manualReviewAcknowledged = false;
    std::chrono::steady_clock::time_point manualReviewAcknowledgedTime{};
    std::set<std::string> seenExecIds;

    std::chrono::steady_clock::time_point submitTime{};
    double firstFillDurationMs = -1.0;
    double fillDurationMs = -1.0;

    bool isTerminal() const {
        return (status == "Filled" || status == "Cancelled" || status == "ApiCancelled" ||
                status == "Rejected" || status == "Inactive");
    }
};

struct OrderWatchdogAction {
    OrderId orderId = 0;
    std::string reason;
    int reconciliationAttempts = 0;
};

struct OrderWatchdogSweepResult {
    std::vector<OrderWatchdogAction> reconciliationOrders;
    std::vector<OrderWatchdogAction> manualReviewOrders;
};

struct PositionInfo {
    std::string account;
    std::string symbol;
    double quantity = 0.0;
    double avgCost = 0.0;
};

struct BookLevel {
    double price = 0.0;
    double size = 0.0;
};

enum class TradeEventType {
    Trigger,
    ValidationStart,
    ValidationOk,
    ValidationFailed,
    PlaceOrderCallStart,
    PlaceOrderCallEnd,
    OpenOrderSeen,
    OrderStatusSeen,
    ExecDetailsSeen,
    CommissionSeen,
    ErrorSeen,
    CancelRequestSent,
    CancelAck,
    FinalState,
    Note
};

struct TraceEvent {
    TradeEventType type = TradeEventType::Note;
    std::chrono::steady_clock::time_point monoTs{};
    std::chrono::system_clock::time_point wallTs{};
    std::string stage;
    std::string details;
    double cumFilled = -1.0;
    double remaining = -1.0;
    double price = 0.0;
    int shares = 0;
    int errorCode = 0;
};

struct FillSlice {
    std::string execId;
    int shares = 0;
    double price = 0.0;
    std::string exchange;
    int liquidity = 0;
    double cumQty = 0.0;
    double avgPrice = 0.0;
    std::string execTimeText;
    double commission = 0.0;
    bool commissionKnown = false;
    std::string commissionCurrency;
};

enum class RuntimeSessionState {
    Disconnected,
    Connecting,
    SocketConnected,
    Reconciling,
    SessionReady
};

struct SubmitIntent {
    std::string source;
    std::string symbol;
    std::string side;
    int requestedQty = 0;
    double limitPrice = 0.0;
    bool closeOnly = false;

    double decisionBid = 0.0;
    double decisionAsk = 0.0;
    double decisionLast = 0.0;
    double sweepEstimate = 0.0;
    double priceBuffer = 0.0;
    int decisionAskLevels = 0;
    int decisionBidLevels = 0;
    std::string bookSummary;
    std::string notes;

    std::chrono::steady_clock::time_point triggerMono{std::chrono::steady_clock::now()};
    std::chrono::system_clock::time_point triggerWall{std::chrono::system_clock::now()};
};

struct TradeTrace {
    std::uint64_t traceId = 0;
    OrderId orderId = 0;
    long long permId = 0;
    std::string source;
    std::string symbol;
    std::string side;
    std::string account;
    int requestedQty = 0;
    double limitPrice = 0.0;
    bool closeOnly = false;

    double decisionBid = 0.0;
    double decisionAsk = 0.0;
    double decisionLast = 0.0;
    double sweepEstimate = 0.0;
    double priceBuffer = 0.0;
    int decisionAskLevels = 0;
    int decisionBidLevels = 0;
    std::string bookSummary;
    std::string notes;

    std::chrono::steady_clock::time_point triggerMono{};
    std::chrono::system_clock::time_point triggerWall{};
    std::chrono::steady_clock::time_point validationStartMono{};
    std::chrono::steady_clock::time_point validationEndMono{};
    std::chrono::steady_clock::time_point placeCallStartMono{};
    std::chrono::steady_clock::time_point placeCallEndMono{};
    std::chrono::steady_clock::time_point firstOpenOrderMono{};
    std::chrono::steady_clock::time_point firstStatusMono{};
    std::chrono::steady_clock::time_point firstExecMono{};
    std::chrono::steady_clock::time_point fullFillMono{};
    std::chrono::steady_clock::time_point cancelReqMono{};

    std::vector<TraceEvent> events;
    std::vector<FillSlice> fills;

    std::string latestStatus;
    std::string latestError;
    std::string terminalStatus;
    bool failedBeforeSubmit = false;
    double totalCommission = 0.0;
    std::string commissionCurrency;
};

struct SharedData {
    std::recursive_mutex mutex;
    std::recursive_mutex clientMutex;

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

    std::atomic<int> nextReqId{FIRST_DYNAMIC_REQ_ID};
    int activeMktDataReqId = 0;
    int activeDepthReqId = 0;
    std::set<int> suppressedMktDataCancelIds;
    std::set<int> suppressedMktDepthCancelIds;

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

    std::string lastWsRequestedSymbol;
    std::chrono::steady_clock::time_point lastWsSubscribeRequest{};
    std::map<std::string, std::chrono::steady_clock::time_point> recentWsIdempotencyKeys;
    std::deque<std::chrono::steady_clock::time_point> recentWsOrderTimestamps;

    std::atomic<OrderId> nextOrderId{0};
    std::map<OrderId, OrderInfo> orders;

    std::map<std::string, PositionInfo> positions;
    bool positionsLoaded = false;
    bool executionsLoaded = false;

    std::atomic<bool> wsServerRunning{false};
    std::atomic<int> wsConnectedClients{0};

    std::atomic<bool> controllerConnected{false};
    std::string controllerDeviceName;
    std::string controllerLockedDeviceName;
    std::chrono::steady_clock::time_point lastQuoteUpdate{};
    std::string lastSubmitFingerprint;
    std::chrono::steady_clock::time_point lastSubmitTime{};

    std::deque<std::string> messages;
    std::string messagesCache;
    std::uint64_t messagesVersion = 0;
    std::uint64_t messagesCacheVersion = 0;
    static const size_t MAX_MESSAGES = 200;

    std::atomic<std::uint64_t> nextTraceId{1};
    std::map<std::uint64_t, TradeTrace> traces;
    std::deque<std::uint64_t> traceRecency;
    std::map<OrderId, std::uint64_t> traceIdByOrderId;
    std::map<long long, std::uint64_t> traceIdByPermId;
    std::map<std::string, std::uint64_t> traceIdByExecId;
    std::uint64_t latestTraceId = 0;

    void addMessage(const std::string& msg);

    void copyMessagesTextIfChanged(std::string& out, std::uint64_t& seenVersion) {
        std::lock_guard<std::recursive_mutex> lock(mutex);
        if (messagesCacheVersion != messagesVersion) {
            size_t totalChars = 0;
            for (const auto& message : messages) {
                totalChars += message.size() + 1;
            }
            messagesCache.clear();
            messagesCache.reserve(totalChars);
            for (auto it = messages.rbegin(); it != messages.rend(); ++it) {
                messagesCache += *it;
                messagesCache.push_back('\n');
            }
            messagesCacheVersion = messagesVersion;
        }
        if (seenVersion != messagesVersion) {
            out = messagesCache;
            seenVersion = messagesVersion;
        }
    }
};

SharedData& appState();
void bindSharedDataOwner(SharedData* owner);
void unbindSharedDataOwner(SharedData* owner);

struct UiStatusSnapshot {
    bool connected = false;
    bool sessionReady = false;
    bool wsServerRunning = false;
    bool websocketEnabled = true;
    bool controllerConnected = false;
    bool controllerEnabled = true;
    bool controllerArmed = false;
    bool tradingKillSwitch = false;
    int wsConnectedClients = 0;
    std::string sessionStateText;
    std::string accountText;
    std::string controllerDeviceName;
    std::string controllerLockedDeviceName;
    std::string websocketAuthToken;
    std::string startupRecoveryBanner;
};

struct SymbolUiSnapshot {
    bool canTrade = false;
    bool hasPosition = false;
    bool hasFreshQuote = false;
    double bidPrice = 0.0;
    double askPrice = 0.0;
    double lastPrice = 0.0;
    double currentPositionQty = 0.0;
    double currentPositionAvgCost = 0.0;
    double availableLongToClose = 0.0;
    double quoteAgeMs = -1.0;
    double openBuyExposure = 0.0;
    std::vector<BookLevel> askBook;
    std::vector<BookLevel> bidBook;
};

struct RuntimeConnectionConfig {
    std::string host = DEFAULT_HOST;
    int port = DEFAULT_PORT;
    int clientId = DEFAULT_CLIENT_ID;
    std::string websocketAuthToken;
    bool websocketEnabled = true;
    bool controllerEnabled = true;
};

struct RiskControlsSnapshot {
    int staleQuoteThresholdMs = 1500;
    int brokerEchoTimeoutMs = 2000;
    int cancelAckTimeoutMs = 5000;
    int partialFillQuietTimeoutMs = 15000;
    double maxOrderNotional = 15000.0;
    double maxOpenNotional = 50000.0;
    ControllerArmMode controllerArmMode = ControllerArmMode::OneShot;
    bool controllerArmed = false;
    bool tradingKillSwitch = false;
};

struct TradeTraceListItem {
    std::uint64_t traceId = 0;
    OrderId orderId = 0;
    bool terminal = false;
    bool failed = false;
    std::string summary;
};

struct TradeTraceSnapshot {
    bool found = false;
    TradeTrace trace;
};

struct PendingUiSyncUpdate {
    bool hasPendingSubscribe = false;
    bool quantityUpdated = false;
    std::string pendingSubscribeSymbol;
    int quantityInput = 1;
};

struct RuntimePresentationSnapshot {
    UiStatusSnapshot status;
    SymbolUiSnapshot symbol;
    RiskControlsSnapshot risk;
    RuntimeConnectionConfig connection;
    std::string activeSymbol;
    int currentQuantity = 1;
    double priceBuffer = 0.01;
    double maxPositionDollars = 40000.0;
    bool subscriptionActive = false;
    std::vector<std::pair<OrderId, OrderInfo>> orders;
    std::vector<TradeTraceListItem> traceItems;
    std::uint64_t latestTraceId = 0;
    std::uint64_t messagesVersion = 0;
    std::string messagesText;
};

struct RuntimeRecoverySnapshot {
    bool priorSessionAbnormal = false;
    std::string priorAppSessionId;
    std::string priorRuntimeSessionId;
    int unfinishedTraceCount = 0;
    std::vector<std::string> unfinishedTraceSummaries;
    std::string bannerText;
};

namespace trading_engine {

struct RuntimeBootstrapEvent {
    std::string appSessionId;
    std::string runtimeSessionId;
    std::string startupRecoveryBanner;
};

struct RuntimeMessageEvent {
    std::string message;
};

struct GuiInputsSyncedEvent {
    int quantityInput = 1;
    double priceBuffer = 0.01;
    double maxPositionDollars = 40000.0;
};

struct ConnectionConfigUpdatedEvent {
    RuntimeConnectionConfig config;
};

struct RiskControlsUpdatedEvent {
    int staleQuoteThresholdMs = 1500;
    int brokerEchoTimeoutMs = 2000;
    int cancelAckTimeoutMs = 5000;
    int partialFillQuietTimeoutMs = 15000;
    double maxOrderNotional = 15000.0;
    double maxOpenNotional = 50000.0;
    ControllerArmMode controllerArmMode = ControllerArmMode::OneShot;
};

struct ControllerArmedChangedEvent {
    bool armed = false;
};

struct TradingKillSwitchChangedEvent {
    bool enabled = false;
};

struct ControllerConnectionStateUpdatedEvent {
    bool connected = false;
    std::string deviceName;
};

struct ControllerLockedDeviceUpdatedEvent {
    std::string deviceName;
};

struct WebSocketServerRunningChangedEvent {
    bool running = false;
};

struct WebSocketClientDeltaEvent {
    int delta = 0;
};

enum class WebSocketSubscribeDecision {
    Proceed,
    DuplicateIgnored,
    AlreadySubscribed,
    SessionNotReady,
};

struct WebSocketSubscribeRequestedEvent {
    std::string symbol;
    std::chrono::steady_clock::time_point requestTime{};
};

struct MarketSubscriptionClearedEvent {
    int marketDataRequestId = 0;
    int depthRequestId = 0;
};

struct MarketSubscriptionStartedEvent {
    std::string symbol;
    int marketDataRequestId = 0;
    int depthRequestId = 0;
    bool recalcQtyFromFirstAsk = false;
};

struct BrokerConnectAckEvent {};
struct BrokerConnectionClosedEvent {};

struct BrokerNextValidIdEvent {
    OrderId orderId = 0;
};

struct BrokerManagedAccountsEvent {
    std::string accountsList;
};

struct BrokerTickPriceEvent {
    TickerId tickerId = 0;
    TickType field = static_cast<TickType>(0);
    double price = 0.0;
};

struct BrokerMarketDepthEvent {
    TickerId requestId = 0;
    int position = 0;
    int operation = 0;
    int side = 0;
    double price = 0.0;
    double size = 0.0;
};

struct BrokerOrderStatusEvent {
    OrderId orderId = 0;
    std::string status;
    double filled = 0.0;
    double remaining = 0.0;
    double avgFillPrice = 0.0;
    long long permId = 0;
    double lastFillPrice = 0.0;
    double mktCapPrice = 0.0;
};

struct BrokerOpenOrderEvent {
    OrderId orderId = 0;
    Contract contract;
    Order order;
    OrderState orderState;
};

struct BrokerExecutionEvent {
    Contract contract;
    Execution execution;
};

struct BrokerExecutionsLoadedEvent {};

struct BrokerCommissionEvent {
    CommissionReport commissionReport;
};

struct BrokerErrorEvent {
    int id = 0;
    int errorCode = 0;
    std::string errorString;
};

struct BrokerPositionEvent {
    std::string account;
    Contract contract;
    double quantity = 0.0;
    double avgCost = 0.0;
};

struct BrokerPositionsLoadedEvent {};

void reduce(SharedData& state, const RuntimeBootstrapEvent& event);
void reduce(SharedData& state, const RuntimeMessageEvent& event);
void reduce(SharedData& state, const GuiInputsSyncedEvent& event);
void reduce(SharedData& state, const ConnectionConfigUpdatedEvent& event);
void reduce(SharedData& state, const RiskControlsUpdatedEvent& event);
bool reduce(SharedData& state, const ControllerArmedChangedEvent& event);
bool reduce(SharedData& state, const TradingKillSwitchChangedEvent& event);
bool reduce(SharedData& state, const ControllerConnectionStateUpdatedEvent& event);
bool reduce(SharedData& state, const ControllerLockedDeviceUpdatedEvent& event);
bool reduce(SharedData& state, const WebSocketServerRunningChangedEvent& event);
int reduce(SharedData& state, const WebSocketClientDeltaEvent& event);
WebSocketSubscribeDecision reduce(SharedData& state, const WebSocketSubscribeRequestedEvent& event);
void reduce(SharedData& state, const MarketSubscriptionClearedEvent& event);
void reduce(SharedData& state, const MarketSubscriptionStartedEvent& event);
void reduce(SharedData& state, const BrokerConnectAckEvent& event);
void reduce(SharedData& state, const BrokerConnectionClosedEvent& event);
void reduce(SharedData& state, const BrokerNextValidIdEvent& event);
void reduce(SharedData& state, const BrokerManagedAccountsEvent& event);
void reduce(SharedData& state, const BrokerTickPriceEvent& event);
void reduce(SharedData& state, const BrokerMarketDepthEvent& event);
void reduce(SharedData& state, const BrokerOrderStatusEvent& event);
void reduce(SharedData& state, const BrokerOpenOrderEvent& event);
void reduce(SharedData& state, const BrokerExecutionEvent& event);
void reduce(SharedData& state, const BrokerExecutionsLoadedEvent& event);
void reduce(SharedData& state, const BrokerCommissionEvent& event);
void reduce(SharedData& state, const BrokerErrorEvent& event);
void reduce(SharedData& state, const BrokerPositionEvent& event);
void reduce(SharedData& state, const BrokerPositionsLoadedEvent& event);

} // namespace trading_engine

std::string chooseConfiguredAccount(const std::string& accountsCsv);
std::string makePositionKey(const std::string& account, const std::string& symbol);
std::string runtimeSessionStateToString(RuntimeSessionState state);
std::string localOrderStateToString(LocalOrderState state);
std::string appDataDirectory();
std::string tradeTraceLogPath();
std::string runtimeJournalLogPath();
void setRuntimeSessionState(RuntimeSessionState state);
int allocateReqId();
int toShareCount(double qty);
bool isTerminalStatus(const std::string& status);
double outstandingOrderQty(const OrderInfo& order);
double availableLongToCloseUnlocked(const std::string& account, const std::string& symbol);
UiStatusSnapshot captureUiStatusSnapshot();
PendingUiSyncUpdate consumePendingUiSyncUpdate();
void consumeGuiSyncUpdates(std::string& symbolInput, std::string& subscribedSymbol, bool& subscribed, int& quantityInput);
void syncSharedGuiInputs(int quantityInput, double priceBuffer, double maxPositionDollars);
SymbolUiSnapshot captureSymbolUiSnapshot(const std::string& subscribedSymbol);
RuntimePresentationSnapshot captureRuntimePresentationSnapshot(const std::string& subscribedSymbol, std::size_t maxTraceItems = 100);
void appendSharedMessage(const std::string& message);
RuntimeConnectionConfig captureRuntimeConnectionConfig();
void updateRuntimeConnectionConfig(const RuntimeConnectionConfig& config);
RiskControlsSnapshot captureRiskControlsSnapshot();
void updateRiskControls(int staleQuoteThresholdMs,
                        int brokerEchoTimeoutMs,
                        int cancelAckTimeoutMs,
                        int partialFillQuietTimeoutMs,
                        double maxOrderNotional,
                        double maxOpenNotional);
void updateRiskControls(int staleQuoteThresholdMs,
                        int brokerEchoTimeoutMs,
                        int cancelAckTimeoutMs,
                        int partialFillQuietTimeoutMs,
                        double maxOrderNotional,
                        double maxOpenNotional,
                        ControllerArmMode controllerArmMode);
void setControllerArmed(bool armed);
void setTradingKillSwitch(bool enabled);
void updateControllerConnectionState(bool connected, const std::string& deviceName);
void updateControllerLockedDeviceName(const std::string& deviceName);
void setWebSocketServerRunning(bool running);
int adjustWebSocketConnectedClients(int delta);
std::string ensureWebSocketAuthToken();
bool consumeWebSocketOrderRateLimit(std::string* error = nullptr);
bool reserveWebSocketIdempotencyKey(const std::string& key, std::string* error = nullptr);
double calculateOpenBuyExposureUnlocked(const std::string& account);
double calculatePositionMarketValueUnlocked(const std::string& account, const std::string& symbol);
std::vector<std::pair<OrderId, OrderInfo>> captureOrdersSnapshot();
std::vector<OrderId> markOrdersPendingCancel(const std::vector<OrderId>& orderIds);
std::vector<OrderId> markAllPendingOrdersForCancel();
void noteCancelRequestsSent(const std::vector<OrderId>& orderIds,
                            std::chrono::steady_clock::time_point requestTime = std::chrono::steady_clock::now());
OrderWatchdogSweepResult requestOrderReconciliation(const std::vector<OrderId>& orderIds,
                                                    const std::string& reason,
                                                    std::chrono::steady_clock::time_point requestTime = std::chrono::steady_clock::now());
std::vector<OrderId> acknowledgeManualReviewOrders(const std::vector<OrderId>& orderIds,
                                                   std::chrono::steady_clock::time_point acknowledgeTime = std::chrono::steady_clock::now());
OrderWatchdogSweepResult sweepOrderWatchdogs(std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now());
OrderWatchdogSweepResult sweepOrderWatchdogsOnReducerThread(std::chrono::steady_clock::time_point now);
std::vector<bool> sendCancelRequests(EClientSocket* client, const std::vector<OrderId>& orderIds);
int computeMaxQuantityFromAsk(double currentAsk, double maxPositionDollars);
void cancelActiveSubscription(EClientSocket* client);
bool requestSymbolSubscription(EClientSocket* client, const std::string& rawSymbol, bool recalcQtyFromFirstAsk, std::string* error = nullptr);
SubmitIntent captureSubmitIntent(const std::string& source,
                                 const std::string& symbol,
                                 const std::string& side,
                                 int requestedQty,
                                 double limitPrice,
                                 bool closeOnly,
                                 double priceBuffer,
                                 double sweepEstimate,
                                 const std::string& notes);
bool submitLimitOrder(EClientSocket* client,
                      const std::string& rawSymbol,
                      const std::string& action,
                      double quantity,
                      double limitPrice,
                      bool closeOnly,
                      const SubmitIntent* intent = nullptr,
                      std::string* error = nullptr,
                      OrderId* outOrderId = nullptr,
                      std::uint64_t* outTraceId = nullptr);
double calculateSweepPrice(const std::vector<BookLevel>& book, int quantity, double safetyBuffer, bool isBuy);
void readerLoop(EReaderOSSignal* osSignal, EReader* reader, EClientSocket* client, std::atomic<bool>* running);

std::string formatWallTime(std::chrono::system_clock::time_point tp);
double durationMs(std::chrono::steady_clock::time_point start, std::chrono::steady_clock::time_point end);
std::string tradeEventTypeToString(TradeEventType type);

std::uint64_t beginTradeTrace(const SubmitIntent& intent);
void bindTraceToOrder(std::uint64_t traceId, OrderId orderId);
void bindTraceToPermId(OrderId orderId, long long permId);
void appendTraceEventByTraceId(std::uint64_t traceId,
                               TradeEventType type,
                               const std::string& stage,
                               const std::string& details,
                               double cumFilled = -1.0,
                               double remaining = -1.0,
                               double price = 0.0,
                               int shares = 0,
                               int errorCode = 0);
void appendTraceEventByOrderId(OrderId orderId,
                               TradeEventType type,
                               const std::string& stage,
                               const std::string& details,
                               double cumFilled = -1.0,
                               double remaining = -1.0,
                               double price = 0.0,
                               int shares = 0,
                               int errorCode = 0);
void markTraceValidationFailed(std::uint64_t traceId, const std::string& reason);
void markTraceSubmitted(std::uint64_t traceId);
void markTraceTerminalByOrderId(OrderId orderId, const std::string& terminalStatus, const std::string& reason = {});
void recordTraceOpenOrder(OrderId orderId, const Contract& contract, const Order& order, const OrderState& orderState);
void recordTraceOrderStatus(OrderId orderId,
                            const std::string& status,
                            double filledQty,
                            double remainingQty,
                            double avgFillPrice,
                            long long permId,
                            double lastFillPrice,
                            double mktCapPrice);
void recordTraceExecution(const Contract& contract, const Execution& execution);
void recordTraceCommission(const CommissionReport& commissionReport);
void recordTraceError(int id, int errorCode, const std::string& errorString);
void recordTraceCancelRequest(OrderId orderId);
std::uint64_t findTraceIdByOrderId(OrderId orderId);
std::uint64_t latestTradeTraceId();
std::vector<TradeTraceListItem> captureTradeTraceListItems(std::size_t maxItems = 100);
TradeTraceSnapshot captureTradeTraceSnapshot(std::uint64_t traceId);
void resetSharedDataForTesting();
void appendTradeTraceLogLine(const json& line);
void appendRuntimeJournalLine(const json& line);
void appendRuntimeJournalEvent(const std::string& event, const json& details = {});
std::vector<std::string> recoverUnfinishedTraceSummariesFromLog(std::size_t maxItems = 20);
RuntimeRecoverySnapshot recoverRuntimeRecoverySnapshot(std::size_t maxTraceItems = 20);
