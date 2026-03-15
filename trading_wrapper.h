#pragma once

#include "app_shared.h"

class TradingWrapper : public DefaultEWrapper {
private:
    using BrokerEventDispatcher = std::function<void(std::function<void()>)>;

    mutable std::mutex stateMutex_;
    EClientSocket* m_client = nullptr;
    BrokerEventDispatcher eventDispatcher_;

    BrokerEventDispatcher currentEventDispatcher() const {
        std::lock_guard<std::mutex> lock(stateMutex_);
        return eventDispatcher_;
    }

    EClientSocket* currentClient() const {
        std::lock_guard<std::mutex> lock(stateMutex_);
        return m_client;
    }

    template <typename Fn>
    void dispatchBrokerEvent(Fn&& fn) {
        BrokerEventDispatcher dispatcher = currentEventDispatcher();
        if (dispatcher) {
            dispatcher(std::function<void()>(std::forward<Fn>(fn)));
            return;
        }
        fn();
    }

public:
    void setClient(EClientSocket* client) {
        std::lock_guard<std::mutex> lock(stateMutex_);
        m_client = client;
    }

    void setEventDispatcher(BrokerEventDispatcher dispatcher) {
        std::lock_guard<std::mutex> lock(stateMutex_);
        eventDispatcher_ = std::move(dispatcher);
    }

    void connectAck() override {
        dispatchBrokerEvent([this]() {
            SharedData& state = appState();
            trading_engine::reduce(state, trading_engine::BrokerConnectAckEvent{});
            std::cout << "[Connected to TWS]" << std::endl;
            requestUiInvalidation();
        });
    }

    void connectionClosed() override {
        dispatchBrokerEvent([this]() {
            SharedData& state = appState();
            trading_engine::reduce(state, trading_engine::BrokerConnectionClosedEvent{});
            std::cout << "[Disconnected from TWS]" << std::endl;
            requestUiInvalidation();
        });
    }

    void nextValidId(OrderId orderId) override {
        dispatchBrokerEvent([this, orderId]() {
            SharedData& state = appState();
            trading_engine::reduce(state, trading_engine::BrokerNextValidIdEvent{orderId});
            std::cout << "[Next valid order ID: " << orderId << "]" << std::endl;

            if (EClientSocket* client = currentClient()) {
                {
                    std::lock_guard<std::recursive_mutex> clientLock(state.clientMutex);
                    if (client == currentClient() && client->isConnected()) {
#if !defined(TWS_GUI_MOCK_IBAPI)
                        client->reqMarketDataType(2); // Request frozen data when the market is closed.
                        appendRuntimeJournalEvent("market_data_type_requested", {
                            {"type", 2},
                            {"label", "frozen"},
                            {"phase", "post_next_valid_id"}
                        });
#endif
                        client->reqPositions();
                        client->reqOpenOrders();
                        ExecutionFilter executionFilter;
                        client->reqExecutions(allocateReqId(), executionFilter);
                    }
                }
                trading_engine::reduce(state, trading_engine::RuntimeMessageEvent{
                    "Requested positions, open orders, and executions..."
                });
                std::cout << "[Requested positions, open orders, and executions]" << std::endl;
            }
            requestUiInvalidation();
        });
    }

    void managedAccounts(const std::string& accountsList) override {
        dispatchBrokerEvent([this, accountsList]() {
            SharedData& state = appState();
            trading_engine::reduce(state, trading_engine::BrokerManagedAccountsEvent{accountsList});
            std::cout << "[Managed accounts updated]" << std::endl;
            requestUiInvalidation();
        });
    }

    void tickPrice(TickerId tickerId, TickType field, double price, const TickAttrib& attrib) override {
        (void)attrib;
        dispatchBrokerEvent([this, tickerId, field, price]() {
            SharedData& state = appState();
            trading_engine::reduce(state, trading_engine::BrokerTickPriceEvent{tickerId, field, price});
            requestUiInvalidation();
        });
    }

    void tickSize(TickerId tickerId, TickType field, Decimal size) override {
        dispatchBrokerEvent([this, tickerId, field, size]() {
            SharedData& state = appState();
            trading_engine::reduce(state, trading_engine::BrokerTickSizeEvent{
                tickerId,
                field,
                DecimalFunctions::decimalToDouble(size)
            });
            requestUiInvalidation();
        });
    }

    void tickGeneric(TickerId tickerId, TickType field, double value) override {
        dispatchBrokerEvent([this, tickerId, field, value]() {
            SharedData& state = appState();
            trading_engine::reduce(state, trading_engine::BrokerTickGenericEvent{tickerId, field, value});
            requestUiInvalidation();
        });
    }

    void tickString(TickerId tickerId, TickType field, const std::string& value) override {
        dispatchBrokerEvent([this, tickerId, field, value]() {
            SharedData& state = appState();
            trading_engine::reduce(state, trading_engine::BrokerTickStringEvent{tickerId, field, value});
            requestUiInvalidation();
        });
    }

    void updateMktDepthL2(TickerId id, int position, const std::string& marketMaker, int operation,
                          int side, double price, Decimal size, bool isSmartDepth) override {
        (void)marketMaker;
        (void)isSmartDepth;
        dispatchBrokerEvent([this, id, position, operation, side, price, size]() {
            SharedData& state = appState();
            trading_engine::reduce(state, trading_engine::BrokerMarketDepthEvent{
                id,
                position,
                operation,
                side,
                price,
                DecimalFunctions::decimalToDouble(size)
            });
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
            SharedData& state = appState();
            trading_engine::reduce(state, trading_engine::BrokerOrderStatusEvent{
                orderId,
                status,
                DecimalFunctions::decimalToDouble(filled),
                DecimalFunctions::decimalToDouble(remaining),
                avgFillPrice,
                static_cast<long long>(permId),
                lastFillPrice,
                mktCapPrice
            });
            requestUiInvalidation();
        });
    }

    void openOrder(OrderId orderId, const Contract& contract,
                   const Order& order, const OrderState& orderState) override {
        dispatchBrokerEvent([this, orderId, contract, order, orderState]() {
            SharedData& state = appState();
            trading_engine::reduce(state, trading_engine::BrokerOpenOrderEvent{
                orderId,
                contract,
                order,
                orderState
            });
            requestUiInvalidation();
        });
    }

    void execDetails(int reqId, const Contract& contract, const Execution& execution) override {
        (void)reqId;
        dispatchBrokerEvent([this, contract, execution]() {
            SharedData& state = appState();
            trading_engine::reduce(state, trading_engine::BrokerExecutionEvent{contract, execution});
            requestUiInvalidation();
        });
    }

    void execDetailsEnd(int reqId) override {
        (void)reqId;
        dispatchBrokerEvent([this]() {
            SharedData& state = appState();
            trading_engine::reduce(state, trading_engine::BrokerExecutionsLoadedEvent{});
            requestUiInvalidation();
        });
    }

    #if defined(TWS_HAS_COMMISSION_AND_FEES_REPORT)
    void commissionAndFeesReport(const CommissionReport& commissionReport) override {
    #else
    void commissionReport(const CommissionReport& commissionReport) override {
    #endif
        dispatchBrokerEvent([this, commissionReport]() {
            SharedData& state = appState();
            trading_engine::reduce(state, trading_engine::BrokerCommissionEvent{commissionReport});
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
            SharedData& state = appState();
            trading_engine::reduce(state, trading_engine::BrokerErrorEvent{id, errorCode, errorString});
            requestUiInvalidation();
        });
    }

    void position(const std::string& account, const Contract& contract,
                  Decimal pos, double avgCost) override {
        dispatchBrokerEvent([this, account, contract, pos, avgCost]() {
            SharedData& state = appState();
            trading_engine::reduce(state, trading_engine::BrokerPositionEvent{
                account,
                contract,
                DecimalFunctions::decimalToDouble(pos),
                avgCost
            });
            requestUiInvalidation();
        });
    }

    void positionEnd() override {
        dispatchBrokerEvent([this]() {
            SharedData& state = appState();
            trading_engine::reduce(state, trading_engine::BrokerPositionsLoadedEvent{});
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
