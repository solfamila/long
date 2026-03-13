#include "trading_runtime.h"
#include "trading_wrapper.h"
#include "app_shared.h"
#include "websocket_handlers.h"

#include <ixwebsocket/IXWebSocketServer.h>
#include <ixwebsocket/IXWebSocket.h>
#include <ixwebsocket/IXNetSystem.h>

#include <iostream>
#include <memory>
#include <thread>
#include <atomic>
#include <functional>

class TradingRuntime::Impl {
public:
    TradingWrapper* wrapper = nullptr;
    EClientSocket* client = nullptr;
    EReaderOSSignal* osSignal = nullptr;
    EReader* reader = nullptr;
    ix::WebSocketServer* wsServer = nullptr;
    std::thread readerThread;
    std::atomic<bool> readerRunning{false};
    std::atomic<TradingRuntimeStatus> status{TradingRuntimeStatus::NotStarted};

    std::function<void(std::shared_ptr<ix::ConnectionState>,
                      ix::WebSocket&,
                      const ix::WebSocketMessagePtr&)> wsMessageCallback;

    bool start(const TradingRuntimeConfig& config) {
        status.store(TradingRuntimeStatus::Starting);

        std::cout << "=== Trading Runtime Starting ===" << std::endl;
        std::cout << "TWS: " << config.host << ":" << config.port << std::endl;
        std::cout << "Account: " << config.account << std::endl;

        wrapper = new TradingWrapper();
        osSignal = new EReaderOSSignal(2000);
        client = new EClientSocket(wrapper, osSignal);
        wrapper->setClient(client);

        bool twsConnected = client->eConnect(config.host, config.port, config.clientId);
        if (!twsConnected) {
            std::cerr << "Failed to connect to TWS" << std::endl;
            g_data.addMessage("Failed to connect to TWS");
            status.store(TradingRuntimeStatus::Failed);
            return false;
        }

        std::cout << "Connected to TWS socket" << std::endl;
        readerRunning.store(twsConnected);

        if (twsConnected) {
            reader = new EReader(client, osSignal);
            reader->start();

            EReaderOSSignal* sig = osSignal;
            EReader* r = reader;
            std::atomic<bool>* running = &readerRunning;

            readerThread = std::thread([this, sig, r, running]() {
                while (running->load()) {
                    sig->waitForSignal();
                    if (!running->load()) break;
                    r->processMsgs();
                }
            });
        }

        ix::initNetSystem();
        wsServer = new ix::WebSocketServer(config.wsPort, config.wsHost);

        wsServer->setOnClientMessageCallback(
            [this](std::shared_ptr<ix::ConnectionState> connectionState,
                  ix::WebSocket& webSocket,
                  const ix::WebSocketMessagePtr& msg) {
                if (wsMessageCallback) {
                    wsMessageCallback(connectionState, webSocket, msg);
                } else {
                    if (msg->type == ix::WebSocketMessageType::Message) {
                        handleWebSocketMessage(msg->str, webSocket, client);
                    } else if (msg->type == ix::WebSocketMessageType::Open) {
                        const int total = g_data.wsConnectedClients.fetch_add(1) + 1;
                        g_data.addMessage("WebSocket client connected (total: " + std::to_string(total) + ")");
                        std::cout << "[WebSocket client connected]" << std::endl;
                    } else if (msg->type == ix::WebSocketMessageType::Close) {
                        int observed = g_data.wsConnectedClients.load();
                        int total = 0;
                        do {
                            total = observed > 0 ? (observed - 1) : 0;
                        } while (!g_data.wsConnectedClients.compare_exchange_weak(observed, total));
                        g_data.addMessage("WebSocket client disconnected (total: " + std::to_string(total) + ")");
                        std::cout << "[WebSocket client disconnected]" << std::endl;
                    } else if (msg->type == ix::WebSocketMessageType::Error) {
                        g_data.addMessage("WebSocket error: " + msg->errorInfo.reason);
                        std::cout << "[WebSocket error: " << msg->errorInfo.reason << "]" << std::endl;
                    }
                }
            }
        );

        bool wsStarted = wsServer->listenAndStart();
        if (wsStarted) {
            g_data.wsServerRunning.store(true);
            g_data.addMessage("WebSocket server started on localhost port " + std::to_string(config.wsPort));
            std::cout << "[WebSocket server started on localhost port " << config.wsPort << "]" << std::endl;
        } else {
            g_data.addMessage("Failed to start WebSocket server on port " + std::to_string(config.wsPort));
            std::cerr << "Failed to start WebSocket server on port " << config.wsPort << std::endl;
        }

        status.store(TradingRuntimeStatus::Running);
        std::cout << "=== Trading Runtime Started ===" << std::endl;
        return true;
    }

    void stop() {
        status.store(TradingRuntimeStatus::Stopping);
        std::cout << "=== Trading Runtime Stopping ===" << std::endl;

        if (g_data.wsServerRunning.load()) {
            std::cout << "Stopping WebSocket server..." << std::endl;
            if (wsServer) {
                wsServer->stop();
            }
            g_data.wsServerRunning.store(false);
        }
        ix::uninitNetSystem();

        if (client && client->isConnected()) {
            cancelActiveSubscription(client);
        }

        readerRunning.store(false);
        if (readerThread.joinable()) {
            readerThread.join();
        }

        if (client && client->isConnected()) {
            client->eDisconnect();
        }

        delete wsServer;
        delete reader;
        delete client;
        delete osSignal;
        delete wrapper;

        wsServer = nullptr;
        reader = nullptr;
        client = nullptr;
        osSignal = nullptr;
        wrapper = nullptr;

        status.store(TradingRuntimeStatus::Stopped);
        std::cout << "=== Trading Runtime Stopped ===" << std::endl;
    }
};

TradingRuntime::TradingRuntime() : pImpl(std::make_unique<Impl>()) {}

TradingRuntime::~TradingRuntime() {
    if (status() == TradingRuntimeStatus::Running) {
        stop();
    }
}

bool TradingRuntime::start(const TradingRuntimeConfig& config) {
    return pImpl->start(config);
}

void TradingRuntime::stop() {
    pImpl->stop();
}

bool TradingRuntime::isConnected() const {
    return pImpl->client && pImpl->client->isConnected();
}

bool TradingRuntime::isWebSocketRunning() const {
    return g_data.wsServerRunning.load();
}

TradingRuntimeStatus TradingRuntime::status() const {
    return pImpl->status.load();
}

EClientSocket* TradingRuntime::getClient() const {
    return pImpl->client;
}

void TradingRuntime::setOnWebSocketMessageCallback(
    std::function<void(std::shared_ptr<ix::ConnectionState>,
                      ix::WebSocket&,
                      const ix::WebSocketMessagePtr&)> callback) {
    pImpl->wsMessageCallback = std::move(callback);
}

const char* TradingRuntime::statusString(TradingRuntimeStatus status) {
    switch (status) {
        case TradingRuntimeStatus::NotStarted: return "NotStarted";
        case TradingRuntimeStatus::Starting: return "Starting";
        case TradingRuntimeStatus::Running: return "Running";
        case TradingRuntimeStatus::Stopping: return "Stopping";
        case TradingRuntimeStatus::Stopped: return "Stopped";
        case TradingRuntimeStatus::Failed: return "Failed";
        default: return "Unknown";
    }
}