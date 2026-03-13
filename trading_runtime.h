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

    static const char* statusString(TradingRuntimeStatus status);

private:
    void readerLoop(EReaderOSSignal* signal, EReader* reader, std::atomic<bool>* running);
    CommandResult processCommand(const RuntimeCommand& command);

    class Impl;
    std::unique_ptr<Impl> pImpl;
};