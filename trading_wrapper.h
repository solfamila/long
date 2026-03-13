#pragma once

#include "app_shared.h"

class TradingWrapper : public DefaultEWrapper {
private:
    using BrokerEventDispatcher = std::function<void(std::function<void()>)>;

    EClientSocket* m_client = nullptr;
    BrokerEventDispatcher eventDispatcher_;

    template <typename Fn>
    void dispatchBrokerEvent(Fn&& fn) {
        if (eventDispatcher_) {
            eventDispatcher_(std::function<void()>(std::forward<Fn>(fn)));
            return;
        }
        fn();
    }

public:
    void setClient(EClientSocket* client) { m_client = client; }
    void setEventDispatcher(BrokerEventDispatcher dispatcher) { eventDispatcher_ = std::move(dispatcher); }

    void connectAck() override {
        dispatchBrokerEvent([this]() {
            std::string host;
            int port = DEFAULT_PORT;
            {
                std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
                g_data.connected = true;
                g_data.sessionReady = false;
                g_data.sessionState = RuntimeSessionState::SocketConnected;
                host = g_data.twsHost;
                port = g_data.twsPort;
            }
            appendRuntimeJournalEvent("tws_connect_ack", {{"host", host}, {"port", port}});
            g_data.addMessage("Connected to TWS (awaiting nextValidId)");
            std::cout << "[Connected to TWS]" << std::endl;
            requestUiInvalidation();
        });
    }

    void connectionClosed() override {
        dispatchBrokerEvent([this]() {
            {
                std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
                g_data.connected = false;
                g_data.sessionReady = false;
                g_data.sessionState = RuntimeSessionState::Disconnected;
                g_data.nextOrderId.store(0, std::memory_order_relaxed);
                g_data.activeMktDataReqId = 0;
                g_data.activeDepthReqId = 0;
                g_data.depthSubscribed = false;
                g_data.positionsLoaded = false;
                g_data.executionsLoaded = false;
                g_data.bidPrice = 0.0;
                g_data.askPrice = 0.0;
                g_data.lastPrice = 0.0;
                g_data.askBook.clear();
                g_data.bidBook.clear();
                g_data.pendingWSQuantityCalc = false;
                g_data.wsQuantityUpdated = false;
            }
            appendRuntimeJournalEvent("tws_connection_closed");
            g_data.addMessage("Disconnected from TWS");
            std::cout << "[Disconnected from TWS]" << std::endl;
            requestUiInvalidation();
        });
    }

    void nextValidId(OrderId orderId) override {
        dispatchBrokerEvent([this, orderId]() {
            {
                std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
                g_data.connected = true;
                g_data.sessionReady = false;
                g_data.sessionState = RuntimeSessionState::Reconciling;
                g_data.positionsLoaded = false;
                g_data.executionsLoaded = false;
                g_data.nextOrderId.store(orderId, std::memory_order_relaxed);
            }
            appendRuntimeJournalEvent("tws_session_ready", {{"nextOrderId", static_cast<long long>(orderId)}});

            g_data.addMessage("Next valid order ID: " + std::to_string(orderId));
            std::cout << "[Next valid order ID: " << orderId << "]" << std::endl;

            if (m_client) {
                {
                    std::lock_guard<std::recursive_mutex> clientLock(g_data.clientMutex);
                    if (m_client->isConnected()) {
                        m_client->reqPositions();
                        m_client->reqOpenOrders();
                        ExecutionFilter executionFilter;
                        m_client->reqExecutions(allocateReqId(), executionFilter);
                    }
                }
                g_data.addMessage("Requested positions, open orders, and executions...");
                std::cout << "[Requested positions, open orders, and executions]" << std::endl;
            }
            requestUiInvalidation();
        });
    }

    void managedAccounts(const std::string& accountsList) override {
        dispatchBrokerEvent([this, accountsList]() {
            std::string message;
            std::string selectedAccount;
            {
                std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
                g_data.managedAccounts = accountsList;
                g_data.selectedAccount = chooseConfiguredAccount(accountsList);
                selectedAccount = g_data.selectedAccount;

                if (!g_data.selectedAccount.empty()) {
                    message = "Managed accounts: " + accountsList + " | using account " + g_data.selectedAccount;
                } else {
                    message = "Managed accounts: " + accountsList +
                              " | configured account not found: " + std::string(HARDCODED_ACCOUNT);
                }
            }

            appendRuntimeJournalEvent("managed_accounts", {{"accounts", accountsList}, {"selectedAccount", selectedAccount}});
            g_data.addMessage(message);
            std::cout << "[" << message << "]" << std::endl;
            requestUiInvalidation();
        });
    }

    void tickPrice(TickerId tickerId, TickType field, double price, const TickAttrib& attrib) override {
        (void)attrib;
        dispatchBrokerEvent([this, tickerId, field, price]() {
            std::string autoQtyMsg;

            {
                std::lock_guard<std::recursive_mutex> lock(g_data.mutex);

                if (tickerId != g_data.activeMktDataReqId) {
                    return;
                }

                switch (field) {
                    case 1:
                        g_data.bidPrice = price;
                        g_data.lastQuoteUpdate = std::chrono::steady_clock::now();
                        break;
                    case 2: {
                        g_data.askPrice = price;
                        g_data.lastQuoteUpdate = std::chrono::steady_clock::now();
                        if (g_data.pendingWSQuantityCalc && price > 0.0) {
                            int maxQty = static_cast<int>(std::floor(g_data.maxPositionDollars / price));
                            if (maxQty < 1) maxQty = 1;
                            g_data.currentQuantity = maxQty;
                            g_data.pendingWSQuantityCalc = false;
                            g_data.wsQuantityUpdated = true;

                            char buf[160];
                            std::snprintf(buf, sizeof(buf),
                                          "WS: Subscribed to %s, quantity set to %d shares ($%.0f / $%.2f)",
                                          g_data.currentSymbol.c_str(), maxQty,
                                          g_data.maxPositionDollars, price);
                            autoQtyMsg = buf;
                        }
                        break;
                    }
                    case 4:
                        g_data.lastPrice = price;
                        g_data.lastQuoteUpdate = std::chrono::steady_clock::now();
                        break;
                    default:
                        break;
                }
            }

            if (!autoQtyMsg.empty()) {
                g_data.addMessage(autoQtyMsg);
            }
            requestUiInvalidation();
        });
    }

    void updateMktDepthL2(TickerId id, int position, const std::string& marketMaker, int operation,
                          int side, double price, Decimal size, bool isSmartDepth) override {
        (void)marketMaker;
        (void)isSmartDepth;
        dispatchBrokerEvent([this, id, position, operation, side, price, size]() {
            std::lock_guard<std::recursive_mutex> lock(g_data.mutex);

            if (id != g_data.activeDepthReqId) {
                return;
            }

            double sizeDouble = DecimalFunctions::decimalToDouble(size);
            std::vector<BookLevel>& book = (side == 0) ? g_data.askBook : g_data.bidBook;

            if (operation == 0) {
                BookLevel level;
                level.price = price;
                level.size = sizeDouble;
                if (position >= static_cast<int>(book.size())) {
                    book.push_back(level);
                } else {
                    book.insert(book.begin() + position, level);
                }
            } else if (operation == 1) {
                if (position < static_cast<int>(book.size())) {
                    book[position].price = price;
                    book[position].size = sizeDouble;
                }
            } else if (operation == 2) {
                if (position < static_cast<int>(book.size())) {
                    book.erase(book.begin() + position);
                }
            }
            requestUiInvalidation();
        });
    }

    void updateMktDepth(TickerId id, int position, int operation, int side,
                        double price, Decimal size) override {
        updateMktDepthL2(id, position, "", operation, side, price, size, false);
    }

    void orderStatus(OrderId orderId, const std::string& status, Decimal filled,
                     Decimal remaining, double avgFillPrice,
#if defined(TWS_ORDER_STATUS_PERMID_IS_INT)
                     int permId,
#else
                     long long permId,
#endif
                     int parentId, double lastFillPrice, int clientId,
                     const std::string& whyHeld, double mktCapPrice) override {
        (void)parentId;
        (void)clientId;
        (void)whyHeld;
        dispatchBrokerEvent([this, orderId, status, filled, remaining, avgFillPrice, permId, lastFillPrice, mktCapPrice]() {
            std::string msg;
            bool shouldTrace = false;
            double traceFilled = 0.0;
            double traceRemaining = 0.0;
            double traceAvgFill = 0.0;
            std::string traceStatus;

            {
                std::lock_guard<std::recursive_mutex> lock(g_data.mutex);

                const double incomingFilled = DecimalFunctions::decimalToDouble(filled);
                const double incomingRemaining = DecimalFunctions::decimalToDouble(remaining);

                auto it = g_data.orders.find(orderId);
                if (it == g_data.orders.end()) {
                    OrderInfo info;
                    info.orderId = orderId;
                    info.status = status;
                    info.filledQty = incomingFilled;
                    info.remainingQty = incomingRemaining;
                    info.avgFillPrice = avgFillPrice;
                    g_data.orders.emplace(orderId, info);
                    it = g_data.orders.find(orderId);
                }

                OrderInfo& ord = it->second;

                if (ord.isTerminal() && !isTerminalStatus(status) && status != ord.status) {
                    return;
                }

                if (status == ord.status &&
                    std::abs(incomingFilled - ord.filledQty) < 1e-9 &&
                    std::abs(incomingRemaining - ord.remainingQty) < 1e-9 &&
                    std::abs(avgFillPrice - ord.avgFillPrice) < 1e-9) {
                    return;
                }

                if (incomingFilled + 1e-9 < ord.filledQty) {
                    return;
                }

                if (std::abs(incomingFilled - ord.filledQty) < 1e-9 &&
                    incomingRemaining > ord.remainingQty + 1e-9 &&
                    !isTerminalStatus(status)) {
                    return;
                }

                ord.status = status;
                ord.filledQty = incomingFilled;
                ord.remainingQty = incomingRemaining;

                if (ord.filledQty > 0.0) {
                    ord.avgFillPrice = avgFillPrice;
                    if (ord.firstFillDurationMs < 0.0 &&
                        ord.submitTime.time_since_epoch().count() > 0) {
                        auto now = std::chrono::steady_clock::now();
                        ord.firstFillDurationMs =
                            std::chrono::duration<double, std::milli>(now - ord.submitTime).count();
                    }
                }

                if (ord.isTerminal()) {
                    ord.cancelPending = false;
                }

                if (ord.status == "Filled" &&
                    ord.fillDurationMs < 0.0 &&
                    ord.submitTime.time_since_epoch().count() > 0) {
                    auto now = std::chrono::steady_clock::now();
                    ord.fillDurationMs =
                        std::chrono::duration<double, std::milli>(now - ord.submitTime).count();
                }

                char buf[256];
                if (ord.fillDurationMs >= 0.0 && ord.firstFillDurationMs >= 0.0) {
                    std::snprintf(buf, sizeof(buf),
                                  "Order %lld: %s (filled: %.0f @ $%.2f) [first fill: %.0f ms, final fill: %.0f ms]",
                                  static_cast<long long>(orderId), ord.status.c_str(),
                                  ord.filledQty, ord.avgFillPrice,
                                  ord.firstFillDurationMs, ord.fillDurationMs);
                } else if (ord.fillDurationMs >= 0.0) {
                    std::snprintf(buf, sizeof(buf),
                                  "Order %lld: %s (filled: %.0f @ $%.2f) [%.0f ms]",
                                  static_cast<long long>(orderId), ord.status.c_str(),
                                  ord.filledQty, ord.avgFillPrice, ord.fillDurationMs);
                } else if (ord.filledQty > 0.0 && ord.firstFillDurationMs >= 0.0) {
                    std::snprintf(buf, sizeof(buf),
                                  "Order %lld: %s (filled: %.0f @ $%.2f) [first fill: %.0f ms]",
                                  static_cast<long long>(orderId), ord.status.c_str(),
                                  ord.filledQty, ord.avgFillPrice, ord.firstFillDurationMs);
                } else {
                    std::snprintf(buf, sizeof(buf),
                                  "Order %lld: %s (filled: %.0f @ $%.2f)",
                                  static_cast<long long>(orderId), ord.status.c_str(),
                                  ord.filledQty, ord.avgFillPrice);
                }
                msg = buf;
                shouldTrace = true;
                traceFilled = ord.filledQty;
                traceRemaining = ord.remainingQty;
                traceAvgFill = ord.avgFillPrice;
                traceStatus = ord.status;
            }

            g_data.addMessage(msg);
            std::cout << "[" << msg << "]" << std::endl;

            if (shouldTrace) {
                recordTraceOrderStatus(orderId, traceStatus, traceFilled, traceRemaining, traceAvgFill,
                                       permId, lastFillPrice, mktCapPrice);
            }
            requestUiInvalidation();
        });
    }

    void openOrder(OrderId orderId, const Contract& contract,
                   const Order& order, const OrderState& orderState) override {
        dispatchBrokerEvent([this, orderId, contract, order, orderState]() {
            {
                std::lock_guard<std::recursive_mutex> lock(g_data.mutex);

                auto it = g_data.orders.find(orderId);
                if (it == g_data.orders.end()) {
                    OrderInfo info;
                    info.orderId = orderId;
                    info.account = order.account;
                    info.symbol = contract.symbol;
                    info.side = order.action;
                    info.quantity = DecimalFunctions::decimalToDouble(order.totalQuantity);
                    info.limitPrice = order.lmtPrice;
                    info.status = orderState.status.empty() ? "Unknown" : orderState.status;
                    info.filledQty = 0.0;
                    info.remainingQty = info.quantity;
                    g_data.orders.emplace(orderId, info);
                } else if (!it->second.isTerminal()) {
                    it->second.account = order.account;
                    it->second.symbol = contract.symbol;
                    it->second.side = order.action;
                    it->second.quantity = DecimalFunctions::decimalToDouble(order.totalQuantity);
                    it->second.limitPrice = order.lmtPrice;
                    it->second.status = orderState.status;
                }
            }

            recordTraceOpenOrder(orderId, contract, order, orderState);

            char msg[256];
            std::snprintf(msg, sizeof(msg), "Open order %lld: %s %s %.0f @ %.2f - %s",
                          static_cast<long long>(orderId),
                          order.action.c_str(),
                          contract.symbol.c_str(),
                          DecimalFunctions::decimalToDouble(order.totalQuantity),
                          order.lmtPrice,
                          orderState.status.c_str());
            g_data.addMessage(msg);
            std::cout << "[" << msg << "]" << std::endl;
            requestUiInvalidation();
        });
    }

    void execDetails(int reqId, const Contract& contract, const Execution& execution) override {
        (void)reqId;
        dispatchBrokerEvent([this, contract, execution]() {
            recordTraceExecution(contract, execution);
            appendRuntimeJournalEvent("execution_seen", {
                {"orderId", static_cast<long long>(execution.orderId)},
                {"symbol", contract.symbol},
                {"execId", execution.execId},
                {"shares", DecimalFunctions::decimalToDouble(execution.shares)},
                {"price", execution.price}
            });

            char msg[256];
            std::snprintf(msg, sizeof(msg),
                          "Execution %s: order %lld %s %.0f @ %.2f (cum %.0f)",
                          execution.execId.c_str(), static_cast<long long>(execution.orderId), contract.symbol.c_str(),
                          DecimalFunctions::decimalToDouble(execution.shares), execution.price,
                          DecimalFunctions::decimalToDouble(execution.cumQty));
            g_data.addMessage(msg);
            std::cout << "[" << msg << "]" << std::endl;
            requestUiInvalidation();
        });
    }

    void execDetailsEnd(int reqId) override {
        (void)reqId;
        dispatchBrokerEvent([this]() {
            bool sessionReadyNow = false;
            {
                std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
                g_data.executionsLoaded = true;
                if (g_data.positionsLoaded) {
                    g_data.sessionReady = true;
                    g_data.sessionState = RuntimeSessionState::SessionReady;
                    sessionReadyNow = true;
                }
            }
            appendRuntimeJournalEvent("executions_loaded");
            if (sessionReadyNow) {
                g_data.addMessage("TWS session is ready after reconciliation");
            }
            requestUiInvalidation();
        });
    }

    #if defined(TWS_HAS_COMMISSION_AND_FEES_REPORT)
    void commissionAndFeesReport(const CommissionReport& commissionReport) override {
    #else
    void commissionReport(const CommissionReport& commissionReport) override {
    #endif
        dispatchBrokerEvent([this, commissionReport]() {
            recordTraceCommission(commissionReport);
            appendRuntimeJournalEvent("commission_seen", {
                {"execId", commissionReport.execId},
                {"commission", commissionValue(commissionReport)},
                {"currency", commissionReport.currency}
            });

            char msg[256];
            std::snprintf(msg, sizeof(msg), "Commission %s: %.4f %s",
                          commissionReport.execId.c_str(), commissionValue(commissionReport),
                          commissionReport.currency.c_str());
            g_data.addMessage(msg);
            std::cout << "[" << msg << "]" << std::endl;
            requestUiInvalidation();
        });
    }

    void error(int id,
#if defined(TWS_ERROR_HAS_TIMESTAMP)
               time_t errorTime,
#endif
               int errorCode, const std::string& errorString,
               const std::string& advancedOrderRejectJson) override {
#if defined(TWS_ERROR_HAS_TIMESTAMP)
        (void)errorTime;
#endif
        (void)advancedOrderRejectJson;
        dispatchBrokerEvent([this, id, errorCode, errorString]() {
            {
                std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
                if (errorCode == 300 && g_data.suppressedMktDataCancelIds.erase(id) > 0) return;
                if (errorCode == 310 && g_data.suppressedMktDepthCancelIds.erase(id) > 0) return;
            }

            char msg[512];
            const bool isInfo = (errorCode >= 2100 && errorCode <= 2199);
            const bool isWarning = (errorCode == 399);

            if (isInfo) {
                std::snprintf(msg, sizeof(msg), "Info [%d] code=%d: %s", id, errorCode, errorString.c_str());
            } else if (isWarning) {
                std::snprintf(msg, sizeof(msg), "Warning [%d] code=%d: %s", id, errorCode, errorString.c_str());
            } else {
                std::snprintf(msg, sizeof(msg), "Error [%d] code=%d: %s", id, errorCode, errorString.c_str());
            }

            if (errorCode == 200) {
                std::string symbolContext;
                {
                    std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
                    symbolContext = g_data.currentSymbol;

                    if (id == g_data.activeMktDataReqId) {
                        g_data.activeMktDataReqId = 0;
                        g_data.bidPrice = 0.0;
                        g_data.askPrice = 0.0;
                        g_data.lastPrice = 0.0;
                    }
                    if (id == g_data.activeDepthReqId) {
                        g_data.activeDepthReqId = 0;
                        g_data.depthSubscribed = false;
                        g_data.askBook.clear();
                        g_data.bidBook.clear();
                    }
                }
                std::string withContext = std::string(msg) + " (symbol: " + symbolContext + ")";
                g_data.addMessage(withContext);
                std::cout << "[" << withContext << "]" << std::endl;
            } else {
                g_data.addMessage(msg);
                std::cout << "[" << msg << "]" << std::endl;
            }

            if (id > 0 && (errorCode == 201 || errorCode == 202 || errorCode == 10147)) {
                std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
                auto it = g_data.orders.find(static_cast<OrderId>(id));
                if (it != g_data.orders.end()) {
                    if (errorCode == 201) {
                        it->second.status = "Rejected";
                    } else {
                        it->second.status = "Cancelled";
                        it->second.cancelPending = false;
                    }
                }
            }

            recordTraceError(id, errorCode, errorString);
            appendRuntimeJournalEvent("broker_error", {
                {"id", id},
                {"code", errorCode},
                {"message", errorString}
            });
            requestUiInvalidation();
        });
    }

    void position(const std::string& account, const Contract& contract,
                  Decimal pos, double avgCost) override {
        dispatchBrokerEvent([this, account, contract, pos, avgCost]() {
            const double posQty = DecimalFunctions::decimalToDouble(pos);

            {
                std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
                if (posQty != 0.0) {
                    PositionInfo info;
                    info.account = account;
                    info.symbol = contract.symbol;
                    info.quantity = posQty;
                    info.avgCost = avgCost;
                    g_data.positions[makePositionKey(account, contract.symbol)] = info;
                } else {
                    g_data.positions.erase(makePositionKey(account, contract.symbol));
                }
            }

            char msg[256];
            std::snprintf(msg, sizeof(msg), "Position: %s %.0f @ %.2f (account: %s)",
                          contract.symbol.c_str(), posQty, avgCost, account.c_str());
            std::cout << "[" << msg << "]" << std::endl;
            requestUiInvalidation();
        });
    }

    void positionEnd() override {
        dispatchBrokerEvent([this]() {
            bool sessionReadyNow = false;
            {
                std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
                g_data.positionsLoaded = true;
                if (g_data.executionsLoaded) {
                    g_data.sessionReady = true;
                    g_data.sessionState = RuntimeSessionState::SessionReady;
                    sessionReadyNow = true;
                }
            }
            appendRuntimeJournalEvent("positions_loaded");
            g_data.addMessage("Positions loaded");
            if (sessionReadyNow) {
                g_data.addMessage("TWS session is ready after reconciliation");
            }
            std::cout << "[Positions loaded]" << std::endl;
            requestUiInvalidation();
        });
    }

#if defined(TWS_HAS_PROTOBUF_API)
    void errorProtoBuf(const protobuf::ErrorMessage& errorProto) override { (void)errorProto; }
    void managedAccountsProtoBuf(const protobuf::ManagedAccounts& managedAccountsProto) override { (void)managedAccountsProto; }
    void tickPriceProtoBuf(const protobuf::TickPrice& tickPriceProto) override { (void)tickPriceProto; }
    void positionProtoBuf(const protobuf::Position& positionProto) override { (void)positionProto; }
    void positionEndProtoBuf(const protobuf::PositionEnd& positionEndProto) override { (void)positionEndProto; }
    void updateMarketDepthProtoBuf(const protobuf::MarketDepth& marketDepthProto) override { (void)marketDepthProto; }
    void updateMarketDepthL2ProtoBuf(const protobuf::MarketDepthL2& marketDepthL2Proto) override { (void)marketDepthL2Proto; }
#endif
};
