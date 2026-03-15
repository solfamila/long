#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <ctime>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

using OrderId = long long;
using TickerId = int;
using TickType = int;
using Decimal = double;

struct TickAttrib {};
struct TagValue {};
using TagValueList = std::vector<TagValue>;
using TagValueListSPtr = std::shared_ptr<TagValueList>;

namespace DecimalFunctions {
inline double decimalToDouble(Decimal value) {
    return value;
}

inline Decimal doubleToDecimal(double value) {
    return value;
}
} // namespace DecimalFunctions

namespace protobuf {
struct ErrorMessage {};
struct ManagedAccounts {};
struct TickPrice {};
struct Position {};
struct PositionEnd {};
struct MarketDepth {};
struct MarketDepthL2 {};
} // namespace protobuf

struct Contract {
    std::string symbol;
    std::string secType;
    std::string exchange;
    std::string currency;
};

struct Order {
    long orderId = 0;
    long permId = 0;
    std::string action;
    Decimal totalQuantity = 0.0;
    std::string orderType;
    double lmtPrice = 0.0;
    std::string tif;
    std::string account;
    bool outsideRth = false;
    bool transmit = true;
};

struct OrderState {
    std::string status;
};

struct OrderCancel {};

struct ExecutionFilter {};

struct Execution {
    int orderId = 0;
    long long permId = 0;
    std::string execId;
    std::string side;
    Decimal shares = 0.0;
    double price = 0.0;
    std::string exchange;
    int lastLiquidity = 0;
    Decimal cumQty = 0.0;
    double avgPrice = 0.0;
    std::string time;
};

struct CommissionReport {
    std::string execId;
    double commission = 0.0;
    std::string currency;
};

class DefaultEWrapper {
public:
    virtual ~DefaultEWrapper() = default;

    virtual void connectAck() {}
    virtual void connectionClosed() {}
    virtual void nextValidId(OrderId) {}
    virtual void managedAccounts(const std::string&) {}
    virtual void tickPrice(TickerId, TickType, double, const TickAttrib&) {}
    virtual void tickSize(TickerId, TickType, Decimal) {}
    virtual void tickGeneric(TickerId, TickType, double) {}
    virtual void tickString(TickerId, TickType, const std::string&) {}
    virtual void updateMktDepthL2(TickerId, int, const std::string&, int, int, double, Decimal, bool) {}
    virtual void updateMktDepth(TickerId, int, int, int, double, Decimal) {}
    virtual void orderStatus(OrderId,
                             const std::string&,
                             Decimal,
                             Decimal,
                             double,
                             long long,
                             int,
                             double,
                             int,
                             const std::string&,
                             double) {}
    virtual void openOrder(OrderId, const Contract&, const Order&, const OrderState&) {}
    virtual void execDetails(int, const Contract&, const Execution&) {}
    virtual void execDetailsEnd(int) {}
    virtual void commissionReport(const CommissionReport&) {}
    virtual void error(int, int, const std::string&, const std::string&) {}
    virtual void error(int, std::time_t, int, const std::string&, const std::string&) {}
    virtual void position(const std::string&, const Contract&, Decimal, double) {}
    virtual void positionEnd() {}
    virtual void errorProtoBuf(const protobuf::ErrorMessage&) {}
    virtual void managedAccountsProtoBuf(const protobuf::ManagedAccounts&) {}
    virtual void tickPriceProtoBuf(const protobuf::TickPrice&) {}
    virtual void positionProtoBuf(const protobuf::Position&) {}
    virtual void positionEndProtoBuf(const protobuf::PositionEnd&) {}
    virtual void updateMarketDepthProtoBuf(const protobuf::MarketDepth&) {}
    virtual void updateMarketDepthL2ProtoBuf(const protobuf::MarketDepthL2&) {}
};

class EReaderOSSignal {
public:
    explicit EReaderOSSignal(int waitTimeoutMs)
        : waitTimeoutMs_(waitTimeoutMs > 0 ? waitTimeoutMs : 1) {}

    void waitForSignal() {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait_for(lock, std::chrono::milliseconds(waitTimeoutMs_), [this] { return signaled_; });
        signaled_ = false;
    }

    void issueSignal() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            signaled_ = true;
        }
        cv_.notify_all();
    }

private:
    int waitTimeoutMs_ = 1;
    std::mutex mutex_;
    std::condition_variable cv_;
    bool signaled_ = false;
};

class EClientSocket;

class EReader {
public:
    EReader(EClientSocket* client, EReaderOSSignal* signal)
        : client_(client), signal_(signal) {}

    void start() {}
    void processMsgs() {}

private:
    EClientSocket* client_ = nullptr;
    EReaderOSSignal* signal_ = nullptr;
};

class EClientSocket {
public:
    EClientSocket(DefaultEWrapper* wrapper, EReaderOSSignal* signal)
        : wrapper_(wrapper), signal_(signal) {}

    bool eConnect(const char*, int, int) {
        connected_ = true;
        if (wrapper_) {
            wrapper_->connectAck();
            wrapper_->managedAccounts("U23164862");
            wrapper_->nextValidId(nextOrderId_.load());
        }
        return true;
    }

    void eDisconnect() {
        if (!connected_) {
            return;
        }
        connected_ = false;
        if (wrapper_) {
            wrapper_->connectionClosed();
        }
    }

    bool isConnected() const {
        return connected_;
    }

    const std::string& lastReqMktDataGenericTickList() const {
        return lastReqMktDataGenericTickList_;
    }

    TickerId lastReqMktDataTickerId() const {
        return lastReqMktDataTickerId_;
    }

    void reqPositions() {
        if (wrapper_) {
            wrapper_->positionEnd();
        }
    }

    void reqOpenOrders() {}

    void reqExecutions(int reqId, const ExecutionFilter&) {
        if (wrapper_) {
            wrapper_->execDetailsEnd(reqId);
        }
    }

    void reqIds(int) {
        if (wrapper_) {
            wrapper_->nextValidId(nextOrderId_.load());
        }
    }

    void reqMktData(TickerId tickerId,
                    const Contract&,
                    const std::string& genericTickList,
                    bool,
                    bool,
                    const TagValueListSPtr&) {
        lastReqMktDataTickerId_ = tickerId;
        lastReqMktDataGenericTickList_ = genericTickList;

        if (!wrapper_) {
            return;
        }
        TickAttrib attrib;
        wrapper_->tickPrice(tickerId, 1, 99.95, attrib);
        wrapper_->tickPrice(tickerId, 2, 100.05, attrib);
        wrapper_->tickPrice(tickerId, 4, 100.00, attrib);

        if (genericTickList.find("236") != std::string::npos) {
            wrapper_->tickGeneric(tickerId, 46, 2.0);
            wrapper_->tickSize(tickerId, 89, 25000.0);
            wrapper_->tickString(tickerId, 87, "0.0125");
        }
    }

    void reqMktDepth(TickerId tickerId,
                     const Contract&,
                     int numRows,
                     bool,
                     const TagValueListSPtr&) {
        if (!wrapper_) {
            return;
        }

        const int rows = numRows > 0 ? (numRows < 3 ? numRows : 3) : 0;
        for (int i = 0; i < rows; ++i) {
            wrapper_->updateMktDepthL2(tickerId, i, "MOCK", 0, 0, 100.05 + (0.05 * i), 100.0 * (i + 1), false);
            wrapper_->updateMktDepthL2(tickerId, i, "MOCK", 0, 1, 99.95 - (0.05 * i), 100.0 * (i + 1), false);
        }
    }

    void cancelMktData(TickerId) {}
    void cancelMktDepth(TickerId, bool) {}

    void placeOrder(OrderId orderId, const Contract& contract, const Order& order) {
        if (!wrapper_) {
            return;
        }

        const long long permId = nextPermId_.fetch_add(1);
        orderPermIds_[orderId] = permId;

        Order acceptedOrder = order;
        acceptedOrder.orderId = static_cast<long>(orderId);
        acceptedOrder.permId = static_cast<long>(permId);

        OrderState state;
        state.status = "Submitted";
        wrapper_->openOrder(orderId, contract, acceptedOrder, state);
        wrapper_->orderStatus(orderId, "Submitted", 0.0, acceptedOrder.totalQuantity, 0.0, permId, 0, 0.0, 0, "", 0.0);
        if (signal_) {
            signal_->issueSignal();
        }
    }

    void cancelOrder(OrderId orderId, const OrderCancel&) {
        if (!wrapper_) {
            return;
        }
        const auto it = orderPermIds_.find(orderId);
        const long long permId = it != orderPermIds_.end() ? it->second : 0;
        wrapper_->orderStatus(orderId, "Cancelled", 0.0, 0.0, 0.0, permId, 0, 0.0, 0, "", 0.0);
    }

private:
    DefaultEWrapper* wrapper_ = nullptr;
    EReaderOSSignal* signal_ = nullptr;
    bool connected_ = false;
    std::map<OrderId, long long> orderPermIds_;
    std::string lastReqMktDataGenericTickList_;
    TickerId lastReqMktDataTickerId_ = 0;

    inline static std::atomic<OrderId> nextOrderId_{1};
    inline static std::atomic<long long> nextPermId_{1000};
};
