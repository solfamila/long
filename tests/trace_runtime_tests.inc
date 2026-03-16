void testTradingWrapperSessionReadyAndReconnect() {
    clearTestFiles();

    SharedData owner;
    bindSharedDataOwner(&owner);

    TradingWrapper wrapper;
    wrapper.connectAck();
    wrapper.managedAccounts("U23154741,U23164862");
    wrapper.nextValidId(113);

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        expect(g_data.connected, "wrapper should mark session connected after connectAck/nextValidId");
        expect(!g_data.sessionReady, "session should not be ready until reconciliation completes");
        expect(g_data.sessionState == RuntimeSessionState::Reconciling, "session should enter reconciling after nextValidId");
        expect(g_data.selectedAccount == "U23154741", "managedAccounts should select configured account");
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

void testSymbolSubscriptionCapturesImmediateMarketDataCallbacks() {
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

    const RuntimePresentationSnapshot snapshot = captureRuntimePresentationSnapshot("INTC", 0);
    expect(snapshot.subscriptionActive, "subscription should stay active after subscribe");
    expect(snapshot.activeSymbol == "INTC", "subscription should set the active symbol");
    expect(std::abs(snapshot.symbol.bidPrice - 99.95) < 1e-9,
           "immediate bid callback should populate the subscribed symbol");
    expect(std::abs(snapshot.symbol.askPrice - 100.05) < 1e-9,
           "immediate ask callback should populate the subscribed symbol");
    expect(std::abs(snapshot.symbol.lastPrice - 100.0) < 1e-9,
           "immediate last-price callback should populate the subscribed symbol");
    expect(!snapshot.symbol.askBook.empty(),
           "immediate depth callbacks should populate the ask book");
    expect(!snapshot.symbol.bidBook.empty(),
           "immediate depth callbacks should populate the bid book");

    unbindSharedDataOwner(&owner);
    resetSharedDataForTesting();
#endif
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
        g_data.selectedAccount = "U23154741";
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
        order.account = "U23154741";
        order.symbol = "INTC";
        order.side = "BUY";
        order.quantity = 2.0;
        order.remainingQty = 2.0;
        order.limitPrice = 45.65;
        order.status = "Submitted";
        g_data.orders[200] = order;

        PositionInfo position;
        position.account = "U23154741";
        position.symbol = "INTC";
        position.quantity = 5.0;
        position.avgCost = 45.10;
        g_data.positions[makePositionKey("U23154741", "INTC")] = position;

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
    expect(snapshot.status.accountText == "U23154741", "presentation snapshot should include selected account");
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
        g_data.selectedAccount = "U23154741";
        g_data.currentSymbol = "INTC";
        g_data.askPrice = 45.65;
        g_data.bidPrice = 45.64;
        g_data.lastPrice = 45.645;
        g_data.staleQuoteThresholdMs = 500;
        g_data.lastQuoteUpdate = std::chrono::steady_clock::now() - std::chrono::milliseconds(1200);

        OrderInfo order;
        order.orderId = 200;
        order.account = "U23154741";
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

void testOrderWatchdogEscalatesToManualReview() {
    clearTestFiles();

    SharedData owner;
    bindSharedDataOwner(&owner);

    const auto now = std::chrono::steady_clock::now();
    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        OrderInfo order;
        order.orderId = 301;
        order.account = "U23154741";
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
        cancelOrder.account = "U23154741";
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
        partialOrder.account = "U23154741";
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
        order.account = "U23154741";
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
    brokerOrder.account = "U23154741";
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
        reconcilable.account = "U23154741";
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
        manualReview.account = "U23154741";
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
