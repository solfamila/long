#pragma once

#ifndef NOMINMAX
#define NOMINMAX
#endif

#ifdef IB_API_AVAILABLE
#include "DefaultEWrapper.h"
#include "EReaderOSSignal.h"
#include "EReader.h"
#include "EClientSocket.h"
#include "Contract.h"
#include "Order.h"
#include "OrderState.h"
#include "OrderCancel.h"
#include "Execution.h"
#include "CommissionReport.h"
#include "Decimal.h"
#include "ErrorMessage.pb.h"
#include "ManagedAccounts.pb.h"
#include "TickPrice.pb.h"
#include "Position.pb.h"
#include "PositionEnd.pb.h"
#endif

#include <imgui.h>
#include <imgui_stdlib.h>

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
    bool cancelPending = false;

    std::chrono::steady_clock::time_point submitTime{};
    double firstFillDurationMs = -1.0;
    double fillDurationMs = -1.0;

    bool isTerminal() const {
        return (status == "Filled" || status == "Cancelled" || status == "ApiCancelled" ||
                status == "Rejected" || status == "Inactive");
    }
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

    std::string pendingSubscribeSymbol;
    bool hasPendingSubscribe = false;
    bool pendingWSQuantityCalc = false;
    bool wsQuantityUpdated = false;

    std::string lastWsRequestedSymbol;
    std::chrono::steady_clock::time_point lastWsSubscribeRequest{};

    std::atomic<OrderId> nextOrderId{0};
    std::map<OrderId, OrderInfo> orders;

    std::map<std::string, PositionInfo> positions;
    bool positionsLoaded = false;

    std::atomic<bool> wsServerRunning{false};
    std::atomic<int> wsConnectedClients{0};

    std::atomic<bool> controllerConnected{false};
    std::string controllerDeviceName;

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

    void addMessage(const std::string& msg) {
        std::lock_guard<std::recursive_mutex> lock(mutex);
        messages.push_back(msg);
        while (messages.size() > MAX_MESSAGES) {
            messages.pop_front();
        }
        ++messagesVersion;
    }

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

extern SharedData g_data;

struct UiStatusSnapshot {
    bool connected = false;
    bool sessionReady = false;
    bool wsServerRunning = false;
    bool controllerConnected = false;
    int wsConnectedClients = 0;
    std::string accountText;
};

struct SymbolUiSnapshot {
    bool canTrade = false;
    bool hasPosition = false;
    double bidPrice = 0.0;
    double askPrice = 0.0;
    double lastPrice = 0.0;
    double currentPositionQty = 0.0;
    double currentPositionAvgCost = 0.0;
    double availableLongToClose = 0.0;
    std::vector<BookLevel> askBook;
    std::vector<BookLevel> bidBook;
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

std::string chooseConfiguredAccount(const std::string& accountsCsv);
std::string makePositionKey(const std::string& account, const std::string& symbol);
int allocateReqId();
int toShareCount(double qty);
bool isTerminalStatus(const std::string& status);
double outstandingOrderQty(const OrderInfo& order);
double availableLongToCloseUnlocked(const std::string& account, const std::string& symbol);
UiStatusSnapshot captureUiStatusSnapshot();
void consumeGuiSyncUpdates(std::string& symbolInput, std::string& subscribedSymbol, bool& subscribed, int& quantityInput);
void syncSharedGuiInputs(int quantityInput, double priceBuffer, double maxPositionDollars);
SymbolUiSnapshot captureSymbolUiSnapshot(const std::string& subscribedSymbol);
std::vector<std::pair<OrderId, OrderInfo>> captureOrdersSnapshot();
std::vector<OrderId> markOrdersPendingCancel(const std::vector<OrderId>& orderIds);
std::vector<OrderId> markAllPendingOrdersForCancel();
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
void appendTradeTraceLogLine(const json& line);
