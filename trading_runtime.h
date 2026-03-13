#pragma once

#include <memory>
#include <atomic>
#include <thread>
#include <functional>

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

    static const char* statusString(TradingRuntimeStatus status);

private:
    void readerLoop(EReaderOSSignal* signal, EReader* reader, std::atomic<bool>* running);

    class Impl;
    std::unique_ptr<Impl> pImpl;
};