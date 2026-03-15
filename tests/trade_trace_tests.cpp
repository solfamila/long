#include "app_shared.h"
#include "trace_exporter.h"
#include "trading_ui_format.h"
#include "trading_wrapper.h"

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <unistd.h>

#define g_data appState()

namespace fs = std::filesystem;

namespace {

void expect(bool condition, const std::string& message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

void expectContains(const std::string& text, const std::string& needle, const std::string& message) {
    expect(text.find(needle) != std::string::npos, message + " (missing: " + needle + ")");
}

const fs::path& testDataDir() {
    static const fs::path path = [] {
        char pattern[] = "/tmp/tws_gui_tests.XXXXXX";
        char* created = mkdtemp(pattern);
        if (created == nullptr) {
            throw std::runtime_error("mkdtemp failed");
        }
        setenv("TWS_GUI_DATA_DIR", created, 1);
        return fs::path(created);
    }();
    return path;
}

void clearTestFiles() {
    resetSharedDataForTesting();
    std::error_code ec;
    fs::create_directories(testDataDir(), ec);
    fs::remove(tradeTraceLogPath(), ec);
    fs::remove(runtimeJournalLogPath(), ec);
}

json makeTraceLine(std::uint64_t traceId,
                   OrderId orderId,
                   long long permId,
                   const std::string& source,
                   const std::string& symbol,
                   const std::string& side,
                   const std::string& eventType,
                   const std::string& stage,
                   const std::string& details,
                   double sinceTriggerMs,
                   double price = 0.0,
                   int shares = 0,
                   double cumFilled = -1.0) {
    json line;
    line["traceId"] = traceId;
    line["orderId"] = static_cast<long long>(orderId);
    line["permId"] = permId;
    line["source"] = source;
    line["symbol"] = symbol;
    line["side"] = side;
    line["requestedQty"] = 1;
    line["limitPrice"] = 45.64;
    line["closeOnly"] = false;
    line["eventType"] = eventType;
    line["stage"] = stage;
    line["details"] = details;
    line["wallTime"] = "09:30:00.000";
    line["sinceTriggerMs"] = sinceTriggerMs;
    if (price > 0.0) {
        line["price"] = price;
    }
    if (shares > 0) {
        line["shares"] = shares;
    }
    if (cumFilled >= 0.0) {
        line["cumFilled"] = cumFilled;
    }
    return line;
}

void appendTraceLine(const json& line) {
    std::ofstream out(tradeTraceLogPath(), std::ios::app);
    expect(out.is_open(), "failed to open trace log for append");
    out << line.dump() << '\n';
}

void appendJournalLine(const json& line) {
    std::ofstream out(runtimeJournalLogPath(), std::ios::app);
    expect(out.is_open(), "failed to open runtime journal for append");
    out << line.dump() << '\n';
}

void connectReadyMockSession(TradingWrapper& wrapper, EClientSocket& client, int clientId = 11) {
    wrapper.setClient(&client);
    expect(client.eConnect("127.0.0.1", 7496, clientId), "mock socket connect should succeed");
    wrapper.positionEnd();
    wrapper.execDetailsEnd(1);
}

void testReplayPrefersRichLiveTrace() {
    clearTestFiles();

    appendTraceLine(makeTraceLine(7, 113, 5001, "GUI Button", "INTC", "BUY",
                                  "Trigger", "GUI Button", "BUY 1 INTC @ 45.64", 0.0));
    appendTraceLine(makeTraceLine(7, 113, 5001, "GUI Button", "INTC", "BUY",
                                  "ValidationStart", "Validation", "Starting local order validation", 1.0));
    appendTraceLine(makeTraceLine(7, 113, 5001, "GUI Button", "INTC", "BUY",
                                  "ValidationOk", "Validation", "Validation passed", 4.0));
    appendTraceLine(makeTraceLine(7, 113, 5001, "GUI Button", "INTC", "BUY",
                                  "PlaceOrderCallStart", "placeOrder", "placeOrder start", 7.0));
    appendTraceLine(makeTraceLine(7, 113, 5001, "GUI Button", "INTC", "BUY",
                                  "PlaceOrderCallEnd", "placeOrder", "placeOrder return", 10.0));
    appendTraceLine(makeTraceLine(7, 113, 5001, "GUI Button", "INTC", "BUY",
                                  "OrderStatusSeen", "Submitted", "submitted", 28.0));
    appendTraceLine(makeTraceLine(7, 113, 5001, "GUI Button", "INTC", "BUY",
                                  "ExecDetailsSeen", "execDetails", "exch=SMART execId=E1 time=20260313 09:30:02",
                                  2930.0, 45.64, 1, 1.0));
    appendTraceLine(makeTraceLine(7, 113, 5001, "GUI Button", "INTC", "BUY",
                                  "CommissionSeen", "commissionReport", "E1 commission=0.4564 USD", 2935.0));
    appendTraceLine(makeTraceLine(7, 113, 5001, "GUI Button", "INTC", "BUY",
                                  "FinalState", "Terminal", "Filled: order complete", 2936.0));

    appendTraceLine(makeTraceLine(100, 113, 5001, "Broker Reconcile", "INTC", "BUY",
                                  "Note", "Recovered", "Recovered execution during startup reconciliation", 0.0));
    appendTraceLine(makeTraceLine(100, 113, 5001, "Broker Reconcile", "INTC", "BUY",
                                  "ExecDetailsSeen", "execDetails", "exch=SMART execId=E1 time=20260313 09:30:02",
                                  0.05, 45.64, 1, 1.0));

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        TradeTrace recovered;
        recovered.traceId = 100;
        recovered.orderId = 113;
        recovered.permId = 5001;
        recovered.source = "Broker Reconcile";
        recovered.symbol = "INTC";
        recovered.side = "BUY";
        recovered.requestedQty = 1;
        recovered.limitPrice = 45.64;
        FillSlice fill;
        fill.execId = "E1";
        fill.shares = 1;
        fill.price = 45.64;
        recovered.fills.push_back(fill);
        g_data.traces[recovered.traceId] = recovered;
        g_data.traceRecency.push_back(recovered.traceId);
        g_data.latestTraceId = recovered.traceId;
        g_data.traceIdByOrderId[recovered.orderId] = recovered.traceId;
        g_data.traceIdByPermId[recovered.permId] = recovered.traceId;
        g_data.traceIdByExecId["E1"] = recovered.traceId;
    }
    publishSharedDataSnapshot();

    TradeTraceSnapshot replayed;
    expect(replayTradeTraceSnapshotByIdentityFromLog(113, 5001, "E1", &replayed),
           "replay by identity should succeed");
    expect(replayed.found, "replayed snapshot should be found");
    expect(replayed.trace.traceId == 7, "replay should prefer the richer live trace");
    expect(replayed.trace.source == "GUI Button", "replay should use live source");

    TraceExportBundle bundle;
    std::string error;
    expect(buildTraceExportBundle(100, &bundle, &error), "export bundle should succeed: " + error);
    expectContains(bundle.summaryCsv, "GUI Button", "summary should be enriched from live trace");
    expectContains(bundle.summaryCsv, "2936", "summary should contain full fill latency");
    expectContains(bundle.timelineCsv, "ValidationStart", "timeline should include live validation stages");
}

void testTraceIdFloorRecoversFromLog() {
    clearTestFiles();
    appendTraceLine(makeTraceLine(42, 0, 0, "GUI Button", "AAPL", "BUY",
                                  "Trigger", "GUI Button", "BUY 1 AAPL @ 100.00", 0.0));

    SharedData owner;
    bindSharedDataOwner(&owner);

    SubmitIntent intent;
    intent.source = "Test";
    intent.symbol = "MSFT";
    intent.side = "BUY";
    intent.requestedQty = 1;
    intent.limitPrice = 10.0;
    intent.triggerMono = std::chrono::steady_clock::now();
    intent.triggerWall = std::chrono::system_clock::now();

    const std::uint64_t traceId = beginTradeTrace(intent);
    expect(traceId > 42, "next trace id should advance beyond recovered log ids");

    unbindSharedDataOwner(&owner);
    resetSharedDataForTesting();
}

void testReplayHandlesPartialFillsAndCommission() {
    clearTestFiles();

    appendTraceLine(makeTraceLine(9, 114, 6001, "Controller", "INTC", "SELL",
                                  "Trigger", "Controller", "SELL 2 INTC @ 45.65", 0.0));
    appendTraceLine(makeTraceLine(9, 114, 6001, "Controller", "INTC", "SELL",
                                  "PlaceOrderCallEnd", "placeOrder", "placeOrder return", 12.0));
    appendTraceLine(makeTraceLine(9, 114, 6001, "Controller", "INTC", "SELL",
                                  "OrderStatusSeen", "Submitted", "submitted", 20.0));
    appendTraceLine(makeTraceLine(9, 114, 6001, "Controller", "INTC", "SELL",
                                  "ExecDetailsSeen", "execDetails", "exch=SMART execId=E2 time=20260313 09:31:01",
                                  800.0, 45.65, 1, 1.0));
    appendTraceLine(makeTraceLine(9, 114, 6001, "Controller", "INTC", "SELL",
                                  "CommissionSeen", "commissionReport", "E2 commission=0.3000 USD", 801.0));
    appendTraceLine(makeTraceLine(9, 114, 6001, "Controller", "INTC", "SELL",
                                  "ExecDetailsSeen", "execDetails", "exch=SMART execId=E3 time=20260313 09:31:02",
                                  1738.0, 45.66, 1, 2.0));
    appendTraceLine(makeTraceLine(9, 114, 6001, "Controller", "INTC", "SELL",
                                  "CommissionSeen", "commissionReport", "E3 commission=0.1567 USD", 1739.0));
    appendTraceLine(makeTraceLine(9, 114, 6001, "Controller", "INTC", "SELL",
                                  "FinalState", "Terminal", "Filled: order complete", 1740.0));

    TradeTraceSnapshot snapshot;
    std::string error;
    expect(replayTradeTraceSnapshotFromLog(9, &snapshot, &error), "replay should succeed: " + error);
    expect(snapshot.found, "snapshot should be found");
    expect(snapshot.trace.fills.size() == 2, "replay should preserve partial fills");
    expect(snapshot.trace.totalCommission > 0.45, "replay should accumulate commission");

    TraceExportBundle bundle;
    expect(buildTraceExportBundle(9, &bundle, &error), "export should succeed: " + error);
    expectContains(bundle.fillsCsv, "E2", "fills csv should contain first fill");
    expectContains(bundle.fillsCsv, "E3", "fills csv should contain second fill");
    expectContains(bundle.summaryCsv, "1740", "summary should contain trigger-to-fill latency");
}

void testWebSocketRuntimeGuards() {
    clearTestFiles();

    std::string error;
    expect(reserveWebSocketIdempotencyKey("abc-123", &error), "first idempotency key reserve should succeed");
    expect(!reserveWebSocketIdempotencyKey("abc-123", &error), "duplicate idempotency key should fail");
    expectContains(error, "Duplicate", "duplicate idempotency error should be descriptive");

    for (int i = 0; i < 5; ++i) {
        expect(consumeWebSocketOrderRateLimit(&error), "rate limit should allow initial order burst");
    }
    expect(!consumeWebSocketOrderRateLimit(&error), "rate limit should reject excessive order burst");
    expectContains(error, "Too many WebSocket order requests", "rate limit error should be descriptive");
}

void testRecoverySnapshotReportsAbnormalShutdown() {
    clearTestFiles();

    appendTraceLine(makeTraceLine(21, 115, 0, "Controller", "INTC", "BUY",
                                  "Trigger", "Controller", "BUY 1 INTC @ 45.64", 0.0));
    appendJournalLine(json{
        {"event", "runtime_start"},
        {"wallTime", "2026-03-13T09:30:00.000"},
        {"appSessionId", "app-test-session"},
        {"runtimeSessionId", "runtime-test-session"}
    });

    const RuntimeRecoverySnapshot snapshot = recoverRuntimeRecoverySnapshot(5);
    expect(snapshot.priorSessionAbnormal, "recovery snapshot should flag abnormal prior session");
    expect(snapshot.priorAppSessionId == "app-test-session", "recovery snapshot should preserve prior app session id");
    expect(snapshot.priorRuntimeSessionId == "runtime-test-session", "recovery snapshot should preserve prior runtime session id");
    expect(snapshot.unfinishedTraceCount == 1, "recovery snapshot should count unfinished traces");
    expectContains(snapshot.bannerText, "Previous session ended unexpectedly", "recovery banner should mention abnormal shutdown");
    expectContains(snapshot.bannerText, "1 unfinished trace", "recovery banner should mention unfinished trace count");
}

void testTradingWrapperSessionReadyAndReconnect() {
    clearTestFiles();

    SharedData owner;
    bindSharedDataOwner(&owner);

    TradingWrapper wrapper;
    wrapper.connectAck();
    wrapper.managedAccounts("U11111111,U23164862");
    wrapper.nextValidId(113);

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        expect(g_data.connected, "wrapper should mark session connected after connectAck/nextValidId");
        expect(!g_data.sessionReady, "session should not be ready until reconciliation completes");
        expect(g_data.sessionState == RuntimeSessionState::Reconciling, "session should enter reconciling after nextValidId");
        expect(g_data.selectedAccount == "U23164862", "managedAccounts should select configured account");
        expect(g_data.nextOrderId.load(std::memory_order_relaxed) == 113, "nextValidId should update next order id");
    }

    wrapper.positionEnd();
    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        expect(g_data.positionsLoaded, "positionEnd should mark positions loaded");
        expect(!g_data.sessionReady, "session should wait for executions before becoming ready");
    }

    wrapper.execDetailsEnd(1);
    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        expect(g_data.executionsLoaded, "execDetailsEnd should mark executions loaded");
        expect(g_data.sessionReady, "session should become ready after positions and executions load");
        expect(g_data.sessionState == RuntimeSessionState::SessionReady, "session should transition to ready after reconciliation");
        expect(!g_data.messages.empty(), "wrapper should have appended status messages");
    }

    wrapper.connectionClosed();
    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        expect(!g_data.connected, "connectionClosed should clear connected state");
        expect(!g_data.sessionReady, "connectionClosed should clear ready state");
        expect(g_data.sessionState == RuntimeSessionState::Disconnected, "connectionClosed should return session to disconnected");
        expect(!g_data.positionsLoaded, "connectionClosed should clear reconciliation flags");
        expect(!g_data.executionsLoaded, "connectionClosed should clear reconciliation flags");
        expect(g_data.bidPrice == 0.0 && g_data.askPrice == 0.0 && g_data.lastPrice == 0.0,
               "connectionClosed should clear quote state");
    }

    unbindSharedDataOwner(&owner);
    resetSharedDataForTesting();
}

void testTradingWrapperIgnoresDuplicateOrderStatus() {
    clearTestFiles();

    SharedData owner;
    bindSharedDataOwner(&owner);

    TradingWrapper wrapper;
#if defined(TWS_ORDER_STATUS_PERMID_IS_INT)
    const int permId = 7001;
#else
    const long long permId = 7001;
#endif
    const Decimal zero = DecimalFunctions::doubleToDecimal(0.0);
    const Decimal one = DecimalFunctions::doubleToDecimal(1.0);
    wrapper.orderStatus(113, "Submitted", zero, one, 0.0, permId, 0, 0.0, 0, "", 0.0);

    std::size_t firstMessageCount = 0;
    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        const auto orderIt = g_data.orders.find(113);
        expect(orderIt != g_data.orders.end(), "first orderStatus should create order state");
        expect(orderIt->second.status == "Submitted", "first orderStatus should record status");
        firstMessageCount = g_data.messages.size();
    }

    wrapper.orderStatus(113, "Submitted", zero, one, 0.0, permId, 0, 0.0, 0, "", 0.0);

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        expect(g_data.messages.size() == firstMessageCount, "duplicate orderStatus should not add a second message");
        const auto orderIt = g_data.orders.find(113);
        expect(orderIt != g_data.orders.end(), "duplicate orderStatus should preserve order state");
        expect(orderIt->second.remainingQty == 1.0, "duplicate orderStatus should preserve remaining quantity");
    }

    unbindSharedDataOwner(&owner);
    resetSharedDataForTesting();
}

void testSymbolSubscriptionRequestsBorrowTicks() {
#if !defined(TWS_GUI_MOCK_IBAPI)
    return;
#else
    clearTestFiles();

    SharedData owner;
    bindSharedDataOwner(&owner);

    TradingWrapper wrapper;
    EReaderOSSignal signal(50);
    EClientSocket client(&wrapper, &signal);
    wrapper.setClient(&client);
    expect(client.eConnect("127.0.0.1", 7496, 11), "mock socket connect should succeed");

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        g_data.connected = true;
        g_data.sessionReady = true;
    }
    publishSharedDataSnapshot();

    std::string error;
    expect(requestSymbolSubscription(&client, "INTC", false, &error),
           "symbol subscription should succeed: " + error);
    expectContains(client.lastReqMktDataGenericTickList(),
                   "236",
                   "symbol subscription should request shortability generic tick list");

    const RuntimePresentationSnapshot snapshot = captureRuntimePresentationSnapshot("INTC", 0);
    expect(snapshot.symbol.borrowAvailability == BorrowAvailability::Borrowable,
           "borrow status should resolve immediately when subscribe-time borrow ticks arrive");
    expect(snapshot.symbol.borrowRateKnown,
           "subscribe-time borrow ticks should populate borrow-rate state");
    expect(std::abs(snapshot.symbol.borrowRate - 0.0125) < 1e-9,
           "subscribe-time borrow ticks should preserve the borrow rate");
    expectContains(snapshot.messagesText,
                   "INTC: Borrowable",
                   "subscription status messaging should surface the resolved borrowable state");
    expect(snapshot.messagesText.find("borrow status pending") == std::string::npos,
           "subscription status message should not stay pending once borrow ticks resolved inline");

    unbindSharedDataOwner(&owner);
    resetSharedDataForTesting();
#endif
}

void testBorrowTickCallbacksUpdateSnapshotState() {
    clearTestFiles();

    SharedData owner;
    bindSharedDataOwner(&owner);

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        g_data.connected = true;
        g_data.sessionReady = true;
        g_data.selectedAccount = "U23164862";
        g_data.currentSymbol = "INTC";
        g_data.activeMktDataReqId = 7001;
    }
    publishSharedDataSnapshot();

    TradingWrapper wrapper;
    wrapper.tickGeneric(7001, static_cast<TickType>(46), 1.0);
    publishSharedDataSnapshot();

    RuntimePresentationSnapshot noShares = captureRuntimePresentationSnapshot("INTC", 0);
    expect(noShares.symbol.borrowAvailability == BorrowAvailability::NoSharesToBorrow,
           "shortability metric should surface a no-shares-to-borrow state");
    expectContains(noShares.symbol.borrowStatusText,
                   "No shares to borrow",
                   "symbol snapshot should expose a clear no-shares state");

    wrapper.tickString(7001, static_cast<TickType>(87), "0.0175");
    wrapper.tickSize(7001, static_cast<TickType>(89), 15000.0);
    publishSharedDataSnapshot();

    const RuntimePresentationSnapshot borrowable = captureRuntimePresentationSnapshot("INTC", 0);
    expect(borrowable.symbol.borrowAvailability == BorrowAvailability::Borrowable,
           "positive shortable shares should mark symbol borrowable");
    expect(borrowable.symbol.borrowRateKnown,
           "borrow-rate callback should mark borrow rate as known");
    expect(std::abs(borrowable.symbol.borrowRate - 0.0175) < 1e-9,
           "borrow-rate callback should populate borrow rate");
    expectContains(borrowable.symbol.borrowStatusText,
                   "Borrowable",
                   "borrowable snapshot should include borrowable status text");

    expectContains(borrowable.messagesText,
                   "INTC: No shares to borrow",
                   "status messaging should include no-shares state");
    expectContains(borrowable.messagesText,
                   "INTC: Borrowable",
                   "status messaging should include borrowable state");

    unbindSharedDataOwner(&owner);
    resetSharedDataForTesting();
}

void testRuntimePresentationSnapshotCapturesConsistentState() {
    clearTestFiles();

    SharedData owner;
    bindSharedDataOwner(&owner);

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        g_data.connected = true;
        g_data.sessionReady = true;
        g_data.sessionState = RuntimeSessionState::SessionReady;
        g_data.selectedAccount = "U23164862";
        g_data.twsHost = "127.0.0.1";
        g_data.twsPort = 7496;
        g_data.twsClientId = 7;
        g_data.websocketAuthToken = "token-123";
        g_data.websocketEnabled = true;
        g_data.controllerEnabled = true;
        g_data.wsServerRunning.store(true, std::memory_order_relaxed);
        g_data.wsConnectedClients.store(2, std::memory_order_relaxed);
        g_data.controllerConnected.store(true, std::memory_order_relaxed);
        g_data.controllerDeviceName = "DualSense";
        g_data.controllerLockedDeviceName = "DualSense";
        g_data.currentSymbol = "INTC";
        g_data.activeMktDataReqId = 101;
        g_data.activeDepthReqId = 202;
        g_data.currentQuantity = 9;
        g_data.priceBuffer = 0.05;
        g_data.maxPositionDollars = 42000.0;
        g_data.bidPrice = 45.64;
        g_data.askPrice = 45.65;
        g_data.lastPrice = 45.645;
        g_data.lastQuoteUpdate = std::chrono::steady_clock::now();
        g_data.maxOrderNotional = 25000.0;
        g_data.maxOpenNotional = 60000.0;
        g_data.staleQuoteThresholdMs = 1500;
        g_data.controllerArmMode = ControllerArmMode::Manual;
        g_data.controllerArmed = true;
        g_data.tradingKillSwitch = false;

        OrderInfo order;
        order.orderId = 200;
        order.account = "U23164862";
        order.symbol = "INTC";
        order.side = "BUY";
        order.quantity = 2.0;
        order.remainingQty = 2.0;
        order.limitPrice = 45.65;
        order.status = "Submitted";
        g_data.orders[200] = order;

        PositionInfo position;
        position.account = "U23164862";
        position.symbol = "INTC";
        position.quantity = 5.0;
        position.avgCost = 45.10;
        g_data.positions[makePositionKey("U23164862", "INTC")] = position;

        TradeTrace trace;
        trace.traceId = 44;
        trace.orderId = 200;
        trace.source = "Controller";
        trace.symbol = "INTC";
        trace.side = "BUY";
        trace.requestedQty = 2;
        trace.limitPrice = 45.65;
        trace.latestStatus = "Submitted";
        g_data.traces[44] = trace;
        g_data.traceRecency.push_back(44);
        g_data.latestTraceId = 44;

        g_data.messages.push_back("hello snapshot");
        ++g_data.messagesVersion;
    }
    publishSharedDataSnapshot();

    const RuntimePresentationSnapshot snapshot = captureRuntimePresentationSnapshot("INTC", 10);
    expect(snapshot.status.connected, "presentation snapshot should include connection status");
    expect(snapshot.status.accountText == "U23164862", "presentation snapshot should include selected account");
    expect(snapshot.connection.clientId == 7, "presentation snapshot should include runtime connection");
    expect(snapshot.activeSymbol == "INTC", "presentation snapshot should include active symbol");
    expect(snapshot.subscriptionActive, "presentation snapshot should include subscription state");
    expect(snapshot.currentQuantity == 9, "presentation snapshot should include current quantity");
    expect(snapshot.priceBuffer == 0.05, "presentation snapshot should include price buffer");
    expect(snapshot.maxPositionDollars == 42000.0, "presentation snapshot should include max position");
    expect(snapshot.symbol.hasPosition, "presentation snapshot should include current position");
    expect(snapshot.symbol.availableLongToClose == 5.0, "presentation snapshot should compute closeable shares");
    expect(snapshot.risk.controllerArmed, "presentation snapshot should include risk state");
    expect(snapshot.orders.size() == 1, "presentation snapshot should include orders");
    expect(snapshot.traceItems.size() == 1, "presentation snapshot should include trace list");
    expect(snapshot.latestTraceId == 44, "presentation snapshot should include latest trace id");
    expectContains(snapshot.messagesText, "hello snapshot", "presentation snapshot should include cached messages");

    unbindSharedDataOwner(&owner);
    resetSharedDataForTesting();
}

void testPendingUiSyncUpdateConsumesFlags() {
    clearTestFiles();

    SharedData owner;
    bindSharedDataOwner(&owner);

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        g_data.hasPendingSubscribe = true;
        g_data.pendingSubscribeSymbol = "QBTS";
        g_data.wsQuantityUpdated = true;
        g_data.currentQuantity = 17;
    }
    publishSharedDataSnapshot();

    const PendingUiSyncUpdate update = consumePendingUiSyncUpdate();
    expect(update.hasPendingSubscribe, "pending subscribe should be surfaced");
    expect(update.quantityUpdated, "pending quantity update should be surfaced");
    expect(update.pendingSubscribeSymbol == "QBTS", "pending subscribe symbol should be preserved");
    expect(update.quantityInput == 17, "pending quantity should be preserved");

    const PendingUiSyncUpdate secondUpdate = consumePendingUiSyncUpdate();
    expect(!secondUpdate.hasPendingSubscribe, "pending subscribe should be consumed");
    expect(!secondUpdate.quantityUpdated, "pending quantity update should be consumed");

    unbindSharedDataOwner(&owner);
    resetSharedDataForTesting();
}

void testRuntimePresentationSnapshotTracksQuoteFreshnessAndCancelMarking() {
    clearTestFiles();

    SharedData owner;
    bindSharedDataOwner(&owner);

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        g_data.connected = true;
        g_data.sessionReady = true;
        g_data.selectedAccount = "U23164862";
        g_data.currentSymbol = "INTC";
        g_data.askPrice = 45.65;
        g_data.bidPrice = 45.64;
        g_data.lastPrice = 45.645;
        g_data.staleQuoteThresholdMs = 500;
        g_data.lastQuoteUpdate = std::chrono::steady_clock::now() - std::chrono::milliseconds(1200);

        OrderInfo order;
        order.orderId = 200;
        order.account = "U23164862";
        order.symbol = "INTC";
        order.side = "BUY";
        order.quantity = 1.0;
        order.remainingQty = 1.0;
        order.limitPrice = 45.65;
        order.status = "Submitted";
        g_data.orders[200] = order;
    }
    publishSharedDataSnapshot();

    RuntimePresentationSnapshot staleSnapshot = captureRuntimePresentationSnapshot("INTC", 10);
    expect(!staleSnapshot.symbol.hasFreshQuote, "old quote should be marked stale in presentation snapshot");
    expect(staleSnapshot.orders.size() == 1, "working order should appear in presentation snapshot");
    expect(!staleSnapshot.orders.front().second.cancelPending, "working order should not start as cancel-pending");

    const auto marked = markAllPendingOrdersForCancel();
    expect(marked.size() == 1 && marked.front() == 200, "markAllPendingOrdersForCancel should mark the working order");

    RuntimePresentationSnapshot pendingCancelSnapshot = captureRuntimePresentationSnapshot("INTC", 10);
    expect(pendingCancelSnapshot.orders.size() == 1, "presentation snapshot should still include cancel-pending order");
    expect(pendingCancelSnapshot.orders.front().second.cancelPending, "presentation snapshot should preserve cancel-pending state");

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        g_data.lastQuoteUpdate = std::chrono::steady_clock::now();
    }
    publishSharedDataSnapshot();

    RuntimePresentationSnapshot freshSnapshot = captureRuntimePresentationSnapshot("INTC", 10);
    expect(freshSnapshot.symbol.hasFreshQuote, "recent quote should be marked fresh in presentation snapshot");

    unbindSharedDataOwner(&owner);
    resetSharedDataForTesting();
}

void testRuntimePresentationSnapshotCapturesShortPositionState() {
    clearTestFiles();

    SharedData owner;
    bindSharedDataOwner(&owner);

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        g_data.connected = true;
        g_data.sessionReady = true;
        g_data.sessionState = RuntimeSessionState::SessionReady;
        g_data.selectedAccount = "U23164862";
        g_data.currentSymbol = "INTC";
        g_data.askPrice = 45.30;
        g_data.bidPrice = 45.20;
        g_data.lastPrice = 45.25;
        g_data.lastQuoteUpdate = std::chrono::steady_clock::now();
        g_data.borrowAvailability = BorrowAvailability::Borrowable;
        g_data.borrowRateKnown = true;
        g_data.borrowRate = 0.0175;

        PositionInfo shortPosition;
        shortPosition.account = "U23164862";
        shortPosition.symbol = "INTC";
        shortPosition.quantity = -5.0;
        shortPosition.avgCost = 44.80;
        g_data.positions[makePositionKey("U23164862", "INTC")] = shortPosition;

        OrderInfo workingShort;
        workingShort.orderId = 801;
        workingShort.account = "U23164862";
        workingShort.symbol = "INTC";
        workingShort.side = "SELL";
        workingShort.quantity = 2.0;
        workingShort.remainingQty = 2.0;
        workingShort.limitPrice = 45.10;
        workingShort.status = "Submitted";
        g_data.orders[801] = workingShort;
    }
    publishSharedDataSnapshot();

    const RuntimePresentationSnapshot snapshot = captureRuntimePresentationSnapshot("INTC", 10);
    expect(snapshot.status.accountText == "U23164862", "short-position snapshot should preserve the selected account");
    expect(snapshot.symbol.hasPosition, "short-position snapshot should report the current short position");
    expect(snapshot.symbol.currentPositionQty == -5.0, "short-position snapshot should preserve the negative quantity");
    expect(snapshot.symbol.availableLongToClose == 0.0, "short-position snapshot should not report long shares available to close");
    expect(std::abs(snapshot.symbol.openBuyExposure - 90.20) < 1e-9,
           "short-position snapshot should include open short exposure from working sell orders");
    expect(snapshot.symbol.borrowAvailability == BorrowAvailability::Borrowable,
           "short-position snapshot should preserve borrow availability");
    expect(snapshot.symbol.borrowRateKnown, "short-position snapshot should preserve borrow-rate state");
    expect(std::abs(snapshot.symbol.borrowRate - 0.0175) < 1e-9,
           "short-position snapshot should preserve the borrow rate");
    expectContains(snapshot.symbol.borrowStatusText,
                   "Borrowable (1.75%)",
                   "short-position snapshot should render borrow status text with the borrow rate");
    expect(snapshot.symbol.hasFreshQuote, "short-position snapshot should preserve fresh-quote state");

    unbindSharedDataOwner(&owner);
    resetSharedDataForTesting();
}

void testShortAppConnectionDefaultsAndFallback() {
    clearTestFiles();

    SharedData owner;
    bindSharedDataOwner(&owner);

    constexpr int kLongAppDefaultClientId = 101;
    const RuntimeConnectionConfig initial = captureRuntimeConnectionConfig();
    expect(DEFAULT_CLIENT_ID == 102, "short app default client id should be 102");
    expect(initial.clientId == DEFAULT_CLIENT_ID,
           "runtime connection config should start from the short app default client id");
    expect(initial.clientId != kLongAppDefaultClientId,
           "short app default client id should differ from long app default 101");

    RuntimeConnectionConfig overrideConfig = initial;
    overrideConfig.clientId = 777;
    updateRuntimeConnectionConfig(overrideConfig);
    const RuntimeConnectionConfig overrideSnapshot = captureRuntimeConnectionConfig();
    expect(overrideSnapshot.clientId == 777, "explicit client id override should be preserved");

    RuntimeConnectionConfig fallbackConfig = overrideSnapshot;
    fallbackConfig.clientId = 0;
    updateRuntimeConnectionConfig(fallbackConfig);
    const RuntimeConnectionConfig fallbackSnapshot = captureRuntimeConnectionConfig();
    expect(fallbackSnapshot.clientId == DEFAULT_CLIENT_ID,
           "invalid client id should fall back to the short app default client id");

    unbindSharedDataOwner(&owner);
    resetSharedDataForTesting();
}

void testShortOpenSubmitSucceeds() {
    clearTestFiles();

    SharedData owner;
    bindSharedDataOwner(&owner);

    TradingWrapper wrapper;
    EReaderOSSignal signal(50);
    EClientSocket client(&wrapper, &signal);
    connectReadyMockSession(wrapper, client, 21);

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        g_data.currentSymbol = "INTC";
        g_data.askPrice = 100.05;
        g_data.bidPrice = 99.95;
        g_data.lastPrice = 100.00;
        g_data.lastQuoteUpdate = std::chrono::steady_clock::now();
        g_data.borrowAvailability = BorrowAvailability::Borrowable;
        g_data.borrowRateKnown = true;
        g_data.borrowRate = 0.0125;
    }
    publishSharedDataSnapshot();

    std::string error;
    OrderId orderId = 0;
    std::uint64_t traceId = 0;
    const bool submitted = submitLimitOrder(&client,
                                            "INTC",
                                            "SELL",
                                            3.0,
                                            100.00,
                                            false,
                                            nullptr,
                                            &error,
                                            &orderId,
                                            &traceId);
    expect(submitted, "open short should succeed when borrow is available");
    expect(error.empty(), "open short success should not populate an error");
    expect(orderId > 0, "open short success should allocate an order id");
    expect(traceId > 0, "open short success should allocate a trace id");

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        const auto orderIt = g_data.orders.find(orderId);
        expect(orderIt != g_data.orders.end(), "open short success should store the order");
        expect(orderIt->second.account == "U23164862", "open short should use the configured short account");
        expect(orderIt->second.symbol == "INTC", "open short should preserve the submitted symbol");
        expect(orderIt->second.side == "SELL", "open short should store a sell order");
        expect(orderIt->second.quantity == 3.0, "open short should preserve the submitted quantity");
        expect(orderIt->second.remainingQty == 3.0, "open short should preserve the remaining quantity");
        expect(orderIt->second.status == "Submitted", "open short should record submitted status");

        const auto traceIt = g_data.traces.find(traceId);
        expect(traceIt != g_data.traces.end(), "open short success should record a trade trace");
        expect(traceIt->second.orderId == orderId, "open short trace should bind to the submitted order");
        expect(traceIt->second.account == "U23164862", "open short trace should record the configured short account");
        expect(traceIt->second.side == "SELL", "open short trace should record a sell side");
        expect(!traceIt->second.closeOnly, "open short trace should not be marked close-only");
        expect(traceIt->second.latestStatus == "Submitted", "open short trace should record the submitted broker status");
    }

    unbindSharedDataOwner(&owner);
    resetSharedDataForTesting();
}

void testBuyToCoverSubmitSucceedsWhenBorrowUnavailable() {
    clearTestFiles();

    SharedData owner;
    bindSharedDataOwner(&owner);

    TradingWrapper wrapper;
    EReaderOSSignal signal(50);
    EClientSocket client(&wrapper, &signal);
    connectReadyMockSession(wrapper, client, 22);

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        g_data.currentSymbol = "INTC";
        g_data.askPrice = 100.10;
        g_data.bidPrice = 100.00;
        g_data.lastPrice = 100.05;
        g_data.lastQuoteUpdate = std::chrono::steady_clock::now();
        g_data.borrowAvailability = BorrowAvailability::NoSharesToBorrow;

        PositionInfo shortPosition;
        shortPosition.account = "U23164862";
        shortPosition.symbol = "INTC";
        shortPosition.quantity = -5.0;
        shortPosition.avgCost = 99.50;
        g_data.positions[makePositionKey("U23164862", "INTC")] = shortPosition;

        OrderInfo existingCover;
        existingCover.orderId = 910;
        existingCover.account = "U23164862";
        existingCover.symbol = "INTC";
        existingCover.side = "BUY";
        existingCover.quantity = 2.0;
        existingCover.remainingQty = 2.0;
        existingCover.limitPrice = 100.20;
        existingCover.status = "Submitted";
        g_data.orders[910] = existingCover;
    }
    publishSharedDataSnapshot();

    std::string error;
    OrderId orderId = 0;
    std::uint64_t traceId = 0;
    const bool submitted = submitLimitOrder(&client,
                                            "INTC",
                                            "BUY",
                                            3.0,
                                            100.10,
                                            true,
                                            nullptr,
                                            &error,
                                            &orderId,
                                            &traceId);
    expect(submitted, "buy to cover should succeed for the remaining short quantity even when new borrow is unavailable");
    expect(error.empty(), "buy-to-cover success should not populate an error");

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        const auto orderIt = g_data.orders.find(orderId);
        expect(orderIt != g_data.orders.end(), "buy-to-cover success should store the order");
        expect(orderIt->second.account == "U23164862", "buy-to-cover should use the configured short account");
        expect(orderIt->second.side == "BUY", "buy-to-cover should store a buy order");
        expect(orderIt->second.quantity == 3.0, "buy-to-cover should preserve the remaining cover quantity");

        const auto traceIt = g_data.traces.find(traceId);
        expect(traceIt != g_data.traces.end(), "buy-to-cover success should record a trade trace");
        expect(traceIt->second.account == "U23164862", "buy-to-cover trace should record the configured short account");
        expect(traceIt->second.side == "BUY", "buy-to-cover trace should record a buy side");
        expect(traceIt->second.closeOnly, "buy-to-cover trace should be marked close-only");
        expect(traceIt->second.latestStatus == "Submitted", "buy-to-cover trace should record the submitted broker status");
    }

    unbindSharedDataOwner(&owner);
    resetSharedDataForTesting();
}

void testShortOpenRejectsWhenBorrowIsUnavailable() {
    clearTestFiles();

    SharedData owner;
    bindSharedDataOwner(&owner);

    TradingWrapper wrapper;
    EReaderOSSignal signal(50);
    EClientSocket client(&wrapper, &signal);
    connectReadyMockSession(wrapper, client, 23);

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        g_data.currentSymbol = "INTC";
        g_data.askPrice = 100.05;
        g_data.bidPrice = 99.95;
        g_data.lastPrice = 100.00;
        g_data.lastQuoteUpdate = std::chrono::steady_clock::now();
    }
    publishSharedDataSnapshot();

    std::string error;
    std::uint64_t traceId = 0;

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        g_data.borrowAvailability = BorrowAvailability::Unknown;
    }
    publishSharedDataSnapshot();

    expect(!submitLimitOrder(&client,
                             "INTC",
                             "SELL",
                             1.0,
                             100.00,
                             false,
                             nullptr,
                             &error,
                             nullptr,
                             &traceId),
           "open short should fail while borrow availability is still pending");
    expectContains(error,
                   "Borrow availability is pending for this symbol",
                   "pending borrow state should block opening a short");

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        expect(g_data.orders.empty(), "rejected short-open should not store an order");
        const auto traceIt = g_data.traces.find(traceId);
        expect(traceIt != g_data.traces.end(), "pending-borrow rejection should still record a trace");
        expect(traceIt->second.failedBeforeSubmit, "pending-borrow rejection should mark the trace as failed before submit");
        expect(traceIt->second.terminalStatus == "FailedBeforeSubmit",
               "pending-borrow rejection should end the trace before broker submit");
    }

    error.clear();
    traceId = 0;
    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        g_data.borrowAvailability = BorrowAvailability::NoSharesToBorrow;
    }
    publishSharedDataSnapshot();

    expect(!submitLimitOrder(&client,
                             "INTC",
                             "SELL",
                             1.0,
                             100.00,
                             false,
                             nullptr,
                             &error,
                             nullptr,
                             &traceId),
           "open short should fail when there are no shares available to borrow");
    expectContains(error,
                   "No shares available to borrow for shorting",
                   "no-shares borrow state should block opening a short");

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        expect(g_data.orders.empty(), "no-shares rejection should still avoid storing an order");
        const auto traceIt = g_data.traces.find(traceId);
        expect(traceIt != g_data.traces.end(), "no-shares rejection should still record a trace");
        expect(traceIt->second.failedBeforeSubmit, "no-shares rejection should mark the trace as failed before submit");
        expect(traceIt->second.terminalStatus == "FailedBeforeSubmit",
               "no-shares rejection should end the trace before broker submit");
    }

    unbindSharedDataOwner(&owner);
    resetSharedDataForTesting();
}

void testShortOpenValidationUsesShortExposureForMaxOpenNotional() {
    clearTestFiles();

    SharedData owner;
    bindSharedDataOwner(&owner);

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        g_data.connected = true;
        g_data.sessionReady = true;
        g_data.selectedAccount = "U23164862";
        g_data.currentSymbol = "INTC";
        g_data.maxOrderNotional = 20000.0;
        g_data.maxOpenNotional = 1000.0;
        g_data.borrowAvailability = BorrowAvailability::Borrowable;

        PositionInfo position;
        position.account = "U23164862";
        position.symbol = "INTC";
        position.quantity = -5.0;
        position.avgCost = 100.0;
        g_data.positions[makePositionKey("U23164862", "INTC")] = position;

        OrderInfo workingShort;
        workingShort.orderId = 701;
        workingShort.account = "U23164862";
        workingShort.symbol = "INTC";
        workingShort.side = "SELL";
        workingShort.quantity = 3.0;
        workingShort.remainingQty = 3.0;
        workingShort.limitPrice = 100.0;
        workingShort.status = "Submitted";
        g_data.orders[701] = workingShort;
    }
    publishSharedDataSnapshot();

    EReaderOSSignal signal(1000);
    EClientSocket client(nullptr, &signal);
    (void)client.eConnect("127.0.0.1", 7496, 77);

    std::string error;
    const bool submitted = submitLimitOrder(&client,
                                            "INTC",
                                            "SELL",
                                            3.0,
                                            100.0,
                                            false,
                                            nullptr,
                                            &error,
                                            nullptr,
                                            nullptr);
    expect(!submitted, "open short should fail when projected short exposure exceeds max open notional");
    expectContains(error,
                   "Projected open notional $1100.00 exceeds max open notional $1000.00",
                   "max-open validation should include existing short position and working short-open exposure");

    unbindSharedDataOwner(&owner);
    resetSharedDataForTesting();
}

void testOrderWatchdogEscalatesToManualReview() {
    clearTestFiles();

    SharedData owner;
    bindSharedDataOwner(&owner);

    const auto now = std::chrono::steady_clock::now();
    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        OrderInfo order;
        order.orderId = 301;
        order.account = "U23164862";
        order.symbol = "INTC";
        order.side = "BUY";
        order.quantity = 1.0;
        order.remainingQty = 1.0;
        order.limitPrice = 45.64;
        order.status = "Submitted";
        order.submitTime = now - std::chrono::seconds(3);
        order.localState = LocalOrderState::AwaitingBrokerEcho;
        order.watchdogs.brokerEchoArmed = true;
        order.watchdogs.brokerEchoDeadline = now - std::chrono::milliseconds(1);
        g_data.orders[301] = order;
    }
    publishSharedDataSnapshot();

    const OrderWatchdogSweepResult first = sweepOrderWatchdogs(now);
    expect(first.reconciliationOrders.size() == 1, "first watchdog sweep should begin reconciliation");
    expect(first.reconciliationOrders.front().reason == "broker_echo_timeout", "first watchdog reason should be broker echo timeout");

    const OrderWatchdogSweepResult second = sweepOrderWatchdogs(now + std::chrono::seconds(3));
    expect(second.reconciliationOrders.size() == 1, "second watchdog sweep should retry reconciliation");
    expect(second.reconciliationOrders.front().reconciliationAttempts == 2, "second watchdog sweep should increment reconciliation attempts");

    const OrderWatchdogSweepResult third = sweepOrderWatchdogs(now + std::chrono::seconds(9));
    expect(third.reconciliationOrders.size() == 1, "third watchdog sweep should issue final reconciliation retry");
    expect(third.reconciliationOrders.front().reconciliationAttempts == 3, "third watchdog sweep should increment reconciliation attempts to three");

    const OrderWatchdogSweepResult fourth = sweepOrderWatchdogs(now + std::chrono::seconds(15));
    expect(fourth.manualReviewOrders.size() == 1, "fourth watchdog sweep should escalate to manual review");
    expect(fourth.manualReviewOrders.front().reason == "reconciliation_retry_exhausted",
           "manual review should record reconciliation exhaustion");

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        const auto it = g_data.orders.find(301);
        expect(it != g_data.orders.end(), "watchdog test order should still exist");
        expect(it->second.localState == LocalOrderState::NeedsManualReview, "order should end in manual review");
        expect(it->second.watchdogs.reconciliationAttempts == 3, "manual review should preserve reconciliation attempt count");
    }

    unbindSharedDataOwner(&owner);
    resetSharedDataForTesting();
}

void testCancelAndPartialFillWatchdogs() {
    clearTestFiles();

    SharedData owner;
    bindSharedDataOwner(&owner);

    const auto now = std::chrono::steady_clock::now();
    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        OrderInfo cancelOrder;
        cancelOrder.orderId = 401;
        cancelOrder.account = "U23164862";
        cancelOrder.symbol = "INTC";
        cancelOrder.side = "BUY";
        cancelOrder.quantity = 1.0;
        cancelOrder.remainingQty = 1.0;
        cancelOrder.limitPrice = 45.64;
        cancelOrder.status = "Submitted";
        cancelOrder.submitTime = now - std::chrono::seconds(1);
        cancelOrder.localState = LocalOrderState::Working;
        g_data.orders[401] = cancelOrder;

        OrderInfo partialOrder;
        partialOrder.orderId = 402;
        partialOrder.account = "U23164862";
        partialOrder.symbol = "INTC";
        partialOrder.side = "BUY";
        partialOrder.quantity = 2.0;
        partialOrder.remainingQty = 2.0;
        partialOrder.limitPrice = 45.65;
        partialOrder.status = "Submitted";
        partialOrder.submitTime = now - std::chrono::seconds(1);
        partialOrder.localState = LocalOrderState::Working;
        g_data.orders[402] = partialOrder;
    }
    publishSharedDataSnapshot();

    noteCancelRequestsSent({401}, now);
    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        const auto it = g_data.orders.find(401);
        expect(it != g_data.orders.end(), "cancel test order should exist");
        expect(it->second.localState == LocalOrderState::AwaitingCancelAck, "cancel request should arm cancel-ack watchdog");
        expect(it->second.watchdogs.cancelAckArmed, "cancel-ack watchdog should be armed");
    }

    const OrderWatchdogSweepResult cancelSweep = sweepOrderWatchdogs(now + std::chrono::seconds(6));
    expect(cancelSweep.reconciliationOrders.size() == 1, "cancel timeout should begin reconciliation");
    expect(cancelSweep.reconciliationOrders.front().orderId == 401, "cancel timeout should target the cancel order");
    expect(cancelSweep.reconciliationOrders.front().reason == "cancel_ack_timeout", "cancel timeout should record the right reason");

    Contract contract;
    contract.symbol = "INTC";

    Execution execution;
    execution.orderId = 402;
    execution.permId = 9001;
    execution.execId = "EX-1";
    execution.side = "BOT";
    execution.shares = DecimalFunctions::doubleToDecimal(1.0);
    execution.cumQty = DecimalFunctions::doubleToDecimal(1.0);
    execution.price = 45.65;
    execution.avgPrice = 45.65;
    execution.exchange = "SMART";
    execution.time = "20260313 10:15:00";

    trading_engine::reduce(g_data, trading_engine::BrokerExecutionEvent{contract, execution});
    publishSharedDataSnapshot();
    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        const auto it = g_data.orders.find(402);
        expect(it != g_data.orders.end(), "partial-fill order should exist");
        expect(it->second.localState == LocalOrderState::PartiallyFilled, "execution should move order into partially filled");
        expect(std::abs(it->second.filledQty - 1.0) < 1e-9, "execution should update filled quantity");
        expect(it->second.seenExecIds.size() == 1, "first execution should be recorded once");
    }

    trading_engine::reduce(g_data, trading_engine::BrokerExecutionEvent{contract, execution});
    publishSharedDataSnapshot();
    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        const auto it = g_data.orders.find(402);
        expect(it->second.seenExecIds.size() == 1, "duplicate execution should be deduped");
        expect(std::abs(it->second.filledQty - 1.0) < 1e-9, "duplicate execution should not change filled quantity");
    }

    const OrderWatchdogSweepResult partialSweep = sweepOrderWatchdogs(now + std::chrono::seconds(20));
    const auto partialIt = std::find_if(partialSweep.reconciliationOrders.begin(),
                                        partialSweep.reconciliationOrders.end(),
                                        [](const OrderWatchdogAction& action) {
                                            return action.orderId == 402 &&
                                                   action.reason == "partial_fill_quiet_timeout";
                                        });
    expect(partialIt != partialSweep.reconciliationOrders.end(),
           "quiet partial fill should begin reconciliation for the partial order");

    unbindSharedDataOwner(&owner);
    resetSharedDataForTesting();
}

void testOpenOrderResolvesReconcilingStateAndFloorsOrderIds() {
    clearTestFiles();

    SharedData owner;
    bindSharedDataOwner(&owner);

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        OrderInfo order;
        order.orderId = 501;
        order.account = "U23164862";
        order.symbol = "INTC";
        order.side = "BUY";
        order.quantity = 1.0;
        order.remainingQty = 1.0;
        order.limitPrice = 45.70;
        order.status = "Submitted";
        order.localState = LocalOrderState::NeedsReconciliation;
        order.watchdogs.reconciliationAttempts = 2;
        order.watchdogs.brokerEchoArmed = true;
        order.lastReconciliationReason = "broker_echo_timeout";
        g_data.orders[501] = order;
        g_data.nextOrderId.store(10, std::memory_order_relaxed);
    }

    Contract contract;
    contract.symbol = "INTC";

    Order brokerOrder;
    brokerOrder.orderId = 501;
    brokerOrder.action = "BUY";
    brokerOrder.totalQuantity = DecimalFunctions::doubleToDecimal(1.0);
    brokerOrder.lmtPrice = 45.70;
    brokerOrder.account = "U23164862";
    brokerOrder.permId = 9101;

    OrderState brokerState;
    brokerState.status = "Submitted";

    trading_engine::reduce(g_data, trading_engine::BrokerOpenOrderEvent{501, contract, brokerOrder, brokerState});
    publishSharedDataSnapshot();

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        const auto it = g_data.orders.find(501);
        expect(it != g_data.orders.end(), "open-order test order should still exist");
        expect(it->second.localState == LocalOrderState::Working, "openOrder should resolve reconciling order into working");
        expect(!it->second.watchdogs.brokerEchoArmed, "openOrder should disarm broker-echo watchdog");
        expect(g_data.nextOrderId.load(std::memory_order_relaxed) > 501, "openOrder should floor the next order id");
    }

    unbindSharedDataOwner(&owner);
    resetSharedDataForTesting();
}

void testManualReconcileAndAcknowledgeFlow() {
    clearTestFiles();

    SharedData owner;
    bindSharedDataOwner(&owner);

    const auto now = std::chrono::steady_clock::now();
    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);

        OrderInfo reconcilable;
        reconcilable.orderId = 601;
        reconcilable.account = "U23164862";
        reconcilable.symbol = "INTC";
        reconcilable.side = "BUY";
        reconcilable.quantity = 1.0;
        reconcilable.remainingQty = 1.0;
        reconcilable.limitPrice = 45.80;
        reconcilable.status = "Submitted";
        reconcilable.localState = LocalOrderState::Working;
        g_data.orders[601] = reconcilable;

        OrderInfo manualReview;
        manualReview.orderId = 602;
        manualReview.account = "U23164862";
        manualReview.symbol = "INTC";
        manualReview.side = "SELL";
        manualReview.quantity = 1.0;
        manualReview.remainingQty = 1.0;
        manualReview.limitPrice = 45.81;
        manualReview.status = "Submitted";
        manualReview.localState = LocalOrderState::NeedsManualReview;
        manualReview.lastReconciliationReason = "broker_echo_timeout";
        manualReview.watchdogs.reconciliationAttempts = 3;
        g_data.orders[602] = manualReview;
    }
    publishSharedDataSnapshot();

    const OrderWatchdogSweepResult reconcile =
        requestOrderReconciliation({601, 602}, "manual_reconcile", now);
    expect(reconcile.reconciliationOrders.size() == 2, "manual reconcile should target both selected active orders");

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        const auto working = g_data.orders.find(601);
        expect(working != g_data.orders.end(), "manual reconcile working order should exist");
        expect(working->second.localState == LocalOrderState::NeedsReconciliation,
               "manual reconcile should move working order into reconciling");
        expect(working->second.lastReconciliationReason == "manual_reconcile",
               "manual reconcile should record the operator reason");

        const auto review = g_data.orders.find(602);
        expect(review != g_data.orders.end(), "manual review order should exist");
        expect(review->second.localState == LocalOrderState::NeedsReconciliation,
               "manual reconcile should let manual-review orders retry");
        expect(review->second.watchdogs.reconciliationAttempts == 4,
               "manual reconcile should advance the reconciliation attempt count");
    }

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        g_data.orders[602].localState = LocalOrderState::NeedsManualReview;
        g_data.orders[602].manualReviewAcknowledged = false;
    }
    publishSharedDataSnapshot();

    const std::vector<OrderId> acknowledged = acknowledgeManualReviewOrders({601, 602}, now);
    expect(acknowledged.size() == 1 && acknowledged.front() == 602,
           "acknowledge should only mark manual-review orders");

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        const auto review = g_data.orders.find(602);
        expect(review->second.manualReviewAcknowledged, "manual review acknowledgement should persist");
        expect(formatOrderLocalStateText(review->second).find("ack") != std::string::npos,
               "local state text should surface acknowledged manual review");
        expect(formatOrderWatchdogText(review->second).find("ack") != std::string::npos,
               "watchdog text should surface acknowledged manual review");
    }

    unbindSharedDataOwner(&owner);
    resetSharedDataForTesting();
}

} // namespace

int main() {
    try {
        testDataDir();
        testReplayPrefersRichLiveTrace();
        testTraceIdFloorRecoversFromLog();
        testReplayHandlesPartialFillsAndCommission();
        testWebSocketRuntimeGuards();
        testRecoverySnapshotReportsAbnormalShutdown();
        testTradingWrapperSessionReadyAndReconnect();
        testTradingWrapperIgnoresDuplicateOrderStatus();
        testSymbolSubscriptionRequestsBorrowTicks();
        testBorrowTickCallbacksUpdateSnapshotState();
        testRuntimePresentationSnapshotCapturesConsistentState();
        testPendingUiSyncUpdateConsumesFlags();
        testRuntimePresentationSnapshotTracksQuoteFreshnessAndCancelMarking();
        testRuntimePresentationSnapshotCapturesShortPositionState();
        testShortAppConnectionDefaultsAndFallback();
        testShortOpenSubmitSucceeds();
        testBuyToCoverSubmitSucceedsWhenBorrowUnavailable();
        testShortOpenRejectsWhenBorrowIsUnavailable();
        testShortOpenValidationUsesShortExposureForMaxOpenNotional();
        testOrderWatchdogEscalatesToManualReview();
        testCancelAndPartialFillWatchdogs();
        testOpenOrderResolvesReconcilingStateAndFloorsOrderIds();
        testManualReconcileAndAcknowledgeFlow();
        std::cout << "All trace tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
