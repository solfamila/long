#pragma once

#include <memory>
#include <atomic>
#include <thread>
#include <functional>
#include <variant>
#include <optional>

#include "app_shared.h"

#ifdef IB_API_AVAILABLE
#include "EClientSocket.h"
#include "EReaderOSSignal.h"
#include "EReader.h"
#endif

namespace ix {
class WebSocketServer;
class WebSocket;
}

class TradingWrapper;

struct TradingRuntimeConfig {
    const char* host = "127.0.0.1";
    int port = 7496;
    int clientId = 0;
    const char* account = "DU123456";
    const char* wsHost = "127.0.0.1";
    int wsPort = 8080;
};

enum class TradingRuntimeStatus {
    NotStarted,
    Starting,
    Running,
    Stopping,
    Stopped,
    Failed
};

enum class RuntimeCommandType {
    Subscribe,
    PlaceOrder,
    CancelOrder
};

enum class RuntimeEventType {
    ConnectAck,
    ConnectionClosed,
    NextValidId,
    ManagedAccounts,
    TickPrice,
    UpdateMktDepthL2,
    OrderStatus,
    OpenOrder,
    ExecDetails,
    CommissionReport,
    Error,
    Position,
    PositionEnd
};

struct SubscribeCommand {
    std::string symbol;
    bool recalcQtyFromFirstAsk = true;
};

struct PlaceOrderCommand {
    std::string symbol;
    std::string action;
    int quantity = 0;
    double limitPrice = 0.0;
    bool closeOnly = false;
    std::string source;
    std::optional<std::uint64_t> traceId;
};

struct CancelOrderCommand {
    OrderId orderId;
};

using RuntimeCommand = std::variant<SubscribeCommand, PlaceOrderCommand, CancelOrderCommand>;

struct OrderStatusEvent {
    OrderId orderId;
    std::string status;
    double filled;
    double remaining;
    double avgFillPrice;
    long long permId;
    double lastFillPrice;
    double mktCapPrice;
};

struct OpenOrderEvent {
    OrderId orderId;
    std::string account;
    std::string symbol;
    std::string side;
    double quantity;
    double limitPrice;
    std::string status;
};

struct ExecDetailsEvent {
    int reqId;
    std::string execId;
    OrderId orderId;
    std::string symbol;
    double shares;
    double price;
    double cumQty;
};

struct CommissionReportEvent {
    std::string execId;
    double commission;
    std::string currency;
};

struct ErrorEvent {
    int id;
    int errorCode;
    std::string errorString;
};

struct PositionEvent {
    std::string account;
    std::string symbol;
    double position;
    double avgCost;
};

using RuntimeEvent = std::variant<
    OrderStatusEvent,
    OpenOrderEvent,
    ExecDetailsEvent,
    CommissionReportEvent,
    ErrorEvent,
    PositionEvent
>;

struct CommandResult {
    bool success = false;
    std::string error;
    OrderId orderId = 0;
    std::uint64_t traceId = 0;
    std::string subscribedSymbol;
};

class TradingRuntime {
public:
    TradingRuntime();
    ~TradingRuntime();

    TradingRuntime(const TradingRuntime&) = delete;
    TradingRuntime& operator=(const TradingRuntime&) = delete;

    bool start(const TradingRuntimeConfig& config = {});
    void stop();

    bool isConnected() const;
    bool isWebSocketRunning() const;
    TradingRuntimeStatus status() const;

    EClientSocket* getClient() const;

    void setOnWebSocketMessageCallback(
        std::function<void(std::shared_ptr<ix::ConnectionState>,
                          ix::WebSocket&,
                          const ix::WebSocketMessagePtr&)> callback);

    CommandResult submitSubscribe(const std::string& symbol, bool recalcQtyFromFirstAsk = true);
    CommandResult submitOrder(const std::string& symbol, const std::string& action,
                             int quantity, double limitPrice, bool closeOnly,
                             const std::string& source = "Unknown",
                             std::optional<std::uint64_t> traceId = std::nullopt);
    CommandResult submitCancel(OrderId orderId);

    void submitOrderStatusEvent(OrderId orderId, const std::string& status,
                                double filled, double remaining, double avgFillPrice,
                                long long permId, double lastFillPrice, double mktCapPrice);
    void submitOpenOrderEvent(OrderId orderId, const std::string& account,
                              const std::string& symbol, const std::string& side,
                              double quantity, double limitPrice, const std::string& status);
    void submitExecDetailsEvent(int reqId, const std::string& execId, OrderId orderId,
                                const std::string& symbol, double shares, double price, double cumQty);
    void submitCommissionReportEvent(const std::string& execId, double commission, const std::string& currency);
    void submitErrorEvent(int id, int errorCode, const std::string& errorString);
    void submitPositionEvent(const std::string& account, const std::string& symbol,
                             double position, double avgCost);

    UiStatusSnapshot getStatusSnapshot() const;
    SymbolUiSnapshot getSymbolSnapshot(const std::string& symbol) const;
    std::vector<std::pair<OrderId, OrderInfo>> getOrdersSnapshot() const;
    std::vector<TradeTraceListItem> getTradeTraceListItems(std::size_t maxItems = 100) const;
    TradeTraceSnapshot getTradeTraceSnapshot(std::uint64_t traceId) const;
    std::uint64_t getLatestTraceId() const;
    void addMessage(const std::string& msg);
    void copyMessagesTextIfChanged(std::string& out, std::uint64_t& seenVersion);

    static const char* statusString(TradingRuntimeStatus status);

private:
    void readerLoop(EReaderOSSignal* signal, EReader* reader, std::atomic<bool>* running);
    CommandResult processCommand(const RuntimeCommand& command);

    class Impl;
    std::unique_ptr<Impl> pImpl;
};