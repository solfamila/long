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
#include <queue>
#include <variant>
#include <mutex>
#include <condition_variable>

class TradingRuntime::Impl {
public:
    TradingRuntime* outer = nullptr;
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

    std::queue<RuntimeCommand> commandQueue;
    std::mutex commandQueueMutex;
    std::condition_variable command_cv;
    std::atomic<bool> commandThreadRunning{false};
    std::thread commandThread;

    void startCommandProcessor() {
        commandThreadRunning.store(true);
        commandThread = std::thread([this]() {
            while (commandThreadRunning.load()) {
                RuntimeCommand cmd;
                bool haveCommand = false;
                {
                    std::unique_lock<std::mutex> lock(commandQueueMutex);
                    command_cv.wait_for(lock, std::chrono::milliseconds(100), [this]() {
                        return !commandQueue.empty() || !commandThreadRunning.load();
                    });
                    if (!commandQueue.empty()) {
                        cmd = std::move(commandQueue.front());
                        commandQueue.pop();
                        haveCommand = true;
                    }
                }
                if (haveCommand) {
                    processCommand(cmd);
                }
            }
        });
    }

    void stopCommandProcessor() {
        commandThreadRunning.store(false);
        command_cv.notify_all();
        if (commandThread.joinable()) {
            commandThread.join();
        }
    }

    void enqueueCommand(RuntimeCommand&& cmd) {
        {
            std::lock_guard<std::mutex> lock(commandQueueMutex);
            commandQueue.push(std::move(cmd));
        }
        command_cv.notify_one();
    }

    CommandResult processCommand(const RuntimeCommand& cmd) {
        CommandResult result;
        result.success = false;

        std::visit([this, &result](auto&& c) {
            using T = std::decay_t<decltype(c)>;
            if constexpr (std::is_same_v<T, SubscribeCommand>) {
                std::string error;
                result.success = requestSymbolSubscription(client, c.symbol, c.recalcQtyFromFirstAsk, &error);
                result.error = error;
                result.subscribedSymbol = c.symbol;
            } else if constexpr (std::is_same_v<T, PlaceOrderCommand>) {
                SubmitIntent intent = captureSubmitIntent(
                    c.source, c.symbol, c.action, c.quantity, c.limitPrice,
                    c.closeOnly, 0.01, 0.0, c.source);
                if (c.traceId) {
                    intent.triggerMono = std::chrono::steady_clock::now();
                    intent.triggerWall = std::chrono::system_clock::now();
                }
                std::string submitError;
                OrderId orderId = 0;
                std::uint64_t traceId = 0;
                result.success = submitLimitOrder(client, c.symbol, c.action,
                    static_cast<double>(c.quantity), c.limitPrice, c.closeOnly,
                    &intent, &submitError, &orderId, &traceId);
                result.error = submitError;
                result.orderId = orderId;
                result.traceId = traceId;
            } else if constexpr (std::is_same_v<T, CancelOrderCommand>) {
                OrderCancel cancel;
                result.success = client->cancelOrder(c.orderId, cancel);
                if (!result.success) {
                    result.error = "Failed to send cancel request";
                }
            }
        }, cmd);

        return result;
    }

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

        startCommandProcessor();

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
                        handleWebSocketMessage(msg->str, webSocket, outer);
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

        stopCommandProcessor();

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

TradingRuntime::TradingRuntime() : pImpl(std::make_unique<Impl>()) {
    pImpl->outer = this;
}

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

CommandResult TradingRuntime::submitSubscribe(const std::string& symbol, bool recalcQtyFromFirstAsk) {
    SubscribeCommand cmd;
    cmd.symbol = symbol;
    cmd.recalcQtyFromFirstAsk = recalcQtyFromFirstAsk;
    pImpl->enqueueCommand(std::move(cmd));
    return CommandResult{true, {}, 0, 0, symbol};
}

CommandResult TradingRuntime::submitOrder(const std::string& symbol, const std::string& action,
                                           int quantity, double limitPrice, bool closeOnly,
                                           const std::string& source,
                                           std::optional<std::uint64_t> traceId) {
    PlaceOrderCommand cmd;
    cmd.symbol = symbol;
    cmd.action = action;
    cmd.quantity = quantity;
    cmd.limitPrice = limitPrice;
    cmd.closeOnly = closeOnly;
    cmd.source = source;
    cmd.traceId = traceId;
    pImpl->enqueueCommand(std::move(cmd));
    return CommandResult{true, {}, 0, 0, {}};
}

CommandResult TradingRuntime::submitCancel(OrderId orderId) {
    CancelOrderCommand cmd;
    cmd.orderId = orderId;
    pImpl->enqueueCommand(std::move(cmd));
    return CommandResult{true, {}, orderId, 0, {}};
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