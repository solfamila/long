#include "trading_runtime.h"

#include "app_shared.h"
#include "controller.h"
#include "mac_observability.h"
#include "trading_wrapper.h"
#include "websocket_handlers.h"

#include <chrono>
#include <iomanip>
#include <mutex>
#include <random>
#include <sstream>
#include <thread>
#include <utility>
#include <vector>

namespace {

constexpr auto kControllerPollInterval = std::chrono::milliseconds(16);

std::string makeSessionIdentifier(const char* prefix) {
    static constexpr char kHex[] = "0123456789abcdef";
    std::random_device rd;
    std::uniform_int_distribution<int> dist(0, 255);

    std::ostringstream oss;
    oss << prefix << '-';
    for (int i = 0; i < 8; ++i) {
        const int value = dist(rd);
        oss << kHex[(value >> 4) & 0xF] << kHex[value & 0xF];
    }
    return oss.str();
}

} // namespace

struct TradingRuntime::Impl {
    SharedData sharedData;
    TradingWrapper wrapper;
    std::unique_ptr<EReaderOSSignal> osSignal;
    std::unique_ptr<EClientSocket> client;
    std::unique_ptr<EReader> reader;
    std::unique_ptr<ix::WebSocketServer> wsServer;
    std::thread readerThread;
    std::thread controllerThread;
    std::atomic<bool> readerRunning{false};
    std::atomic<bool> controllerRunning{false};
    mutable std::mutex callbackMutex;
    std::mutex controllerMutex;
    UiInvalidationCallback uiInvalidationCallback;
    ControllerActionCallback controllerActionCallback;
    ControllerState controllerState;
    bool started = false;

    void installUiInvalidationCallback() {
        UiInvalidationCallback callback;
        {
            std::lock_guard<std::mutex> lock(callbackMutex);
            callback = uiInvalidationCallback;
        }
        ::setUiInvalidationCallback(std::move(callback));
    }

    void invokeControllerAction(TradingRuntimeControllerAction action) {
        ControllerActionCallback callback;
        {
            std::lock_guard<std::mutex> lock(callbackMutex);
            callback = controllerActionCallback;
        }
        if (callback) {
            callback(action);
        }
    }

    void controllerLoop() {
        while (controllerRunning.load(std::memory_order_relaxed)) {
            std::vector<TradingRuntimeControllerAction> actions;
            {
                std::lock_guard<std::mutex> lock(controllerMutex);
                controllerPoll(controllerState);
                if (controllerIsConnected(controllerState)) {
                    const auto now = std::chrono::steady_clock::now();
                    if (controllerConsumeDebouncedPress(controllerState, CONTROLLER_BUTTON_SQUARE, now)) {
                        actions.push_back(TradingRuntimeControllerAction::Buy);
                    }
                    if (controllerConsumeDebouncedPress(controllerState, CONTROLLER_BUTTON_CROSS, now)) {
                        actions.push_back(TradingRuntimeControllerAction::ToggleQuantity);
                    }
                    if (controllerConsumeDebouncedPress(controllerState, CONTROLLER_BUTTON_CIRCLE, now)) {
                        actions.push_back(TradingRuntimeControllerAction::Close);
                    }
                    if (controllerConsumeDebouncedPress(controllerState, CONTROLLER_BUTTON_TRIANGLE, now)) {
                        actions.push_back(TradingRuntimeControllerAction::CancelAll);
                    }
                }
            }

            for (const auto action : actions) {
                invokeControllerAction(action);
            }

            std::this_thread::sleep_for(kControllerPollInterval);
        }
    }
};

TradingRuntime::TradingRuntime()
    : impl_(std::make_unique<Impl>()) {}

TradingRuntime::~TradingRuntime() {
    shutdown();
}

void TradingRuntime::setUiInvalidationCallback(UiInvalidationCallback callback) {
    {
        std::lock_guard<std::mutex> lock(impl_->callbackMutex);
        impl_->uiInvalidationCallback = std::move(callback);
    }
    if (impl_->started) {
        impl_->installUiInvalidationCallback();
    }
}

void TradingRuntime::setControllerActionCallback(ControllerActionCallback callback) {
    std::lock_guard<std::mutex> lock(impl_->callbackMutex);
    impl_->controllerActionCallback = std::move(callback);
}

bool TradingRuntime::start() {
    if (impl_->started) {
        return impl_->client && impl_->client->isConnected();
    }

    bindSharedDataOwner(&impl_->sharedData);
    impl_->installUiInvalidationCallback();

    const RuntimeConnectionConfig connectionConfig = captureRuntimeConnectionConfig();
    const std::string websocketToken = ensureWebSocketAuthToken();
    const auto unfinishedTraces = recoverUnfinishedTraceSummariesFromLog(5);
    const RuntimeRecoverySnapshot recovery = recoverRuntimeRecoverySnapshot(5);
    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        g_data.appSessionId = makeSessionIdentifier("app");
        g_data.runtimeSessionId = makeSessionIdentifier("runtime");
        g_data.startupRecoveryBanner = recovery.bannerText;
        g_data.wsServerRunning.store(false, std::memory_order_relaxed);
        g_data.wsConnectedClients.store(0, std::memory_order_relaxed);
    }

    std::cout << "=== TWS Trading GUI ===" << std::endl;
    std::cout << "Connecting to TWS at " << connectionConfig.host << ":" << connectionConfig.port << std::endl;
    std::cout << "Configured account: " << HARDCODED_ACCOUNT << std::endl;
    std::cout << "Using platform: AppKit native views" << std::endl;
    macLogInfo("runtime", "Starting runtime for " + connectionConfig.host + ":" + std::to_string(connectionConfig.port));
    appendRuntimeJournalEvent("runtime_start", {
        {"twsHost", connectionConfig.host},
        {"twsPort", connectionConfig.port},
        {"twsClientId", connectionConfig.clientId},
        {"websocketEnabled", connectionConfig.websocketEnabled},
        {"controllerEnabled", connectionConfig.controllerEnabled}
    });
    setRuntimeSessionState(RuntimeSessionState::Connecting);

    impl_->osSignal = std::make_unique<EReaderOSSignal>(2000);
    impl_->client = std::make_unique<EClientSocket>(&impl_->wrapper, impl_->osSignal.get());
    impl_->wrapper.setClient(impl_->client.get());

    const bool twsConnected = impl_->client->eConnect(connectionConfig.host.c_str(),
                                                      connectionConfig.port,
                                                      connectionConfig.clientId);
    if (!twsConnected) {
        std::cerr << "Failed to connect to TWS" << std::endl;
        g_data.addMessage("Failed to connect to TWS");
        macLogError("runtime", "Failed to connect to TWS at " + connectionConfig.host + ":" + std::to_string(connectionConfig.port));
        appendRuntimeJournalEvent("runtime_connect_failed", {
            {"twsHost", connectionConfig.host},
            {"twsPort", connectionConfig.port}
        });
        setRuntimeSessionState(RuntimeSessionState::Disconnected);
    } else {
        std::cout << "Connected to TWS socket" << std::endl;
        macLogInfo("runtime", "Connected to TWS socket");
        appendRuntimeJournalEvent("runtime_connect_started");
    }

    if (twsConnected) {
        impl_->reader = std::make_unique<EReader>(impl_->client.get(), impl_->osSignal.get());
        impl_->readerRunning.store(true, std::memory_order_relaxed);
        impl_->reader->start();
        impl_->readerThread = std::thread(readerLoop,
                                          impl_->osSignal.get(),
                                          impl_->reader.get(),
                                          impl_->client.get(),
                                          &impl_->readerRunning);
    }

    if (connectionConfig.websocketEnabled) {
        ix::initNetSystem();
        impl_->wsServer = std::make_unique<ix::WebSocketServer>(WEBSOCKET_PORT, WEBSOCKET_HOST);
        EClientSocket* const client = impl_->client.get();
        impl_->wsServer->setOnClientMessageCallback(
            [client](std::shared_ptr<ix::ConnectionState> connectionState,
                     ix::WebSocket& webSocket,
                     const ix::WebSocketMessagePtr& msg) {
                (void)connectionState;

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
            });

        if (impl_->wsServer->listenAndStart()) {
            g_data.wsServerRunning.store(true);
            g_data.addMessage("WebSocket server started on localhost port " + std::to_string(WEBSOCKET_PORT));
            g_data.addMessage("WebSocket token: " + websocketToken);
            std::cout << "[WebSocket server started on localhost port " << WEBSOCKET_PORT << "]" << std::endl;
            macLogInfo("ipc", "WebSocket server started on localhost:" + std::to_string(WEBSOCKET_PORT));
            appendRuntimeJournalEvent("ws_server_started", {
                {"host", WEBSOCKET_HOST},
                {"port", WEBSOCKET_PORT}
            });
        } else {
            g_data.addMessage("Failed to start WebSocket server on port " + std::to_string(WEBSOCKET_PORT));
            std::cerr << "Failed to start WebSocket server on port " << WEBSOCKET_PORT << std::endl;
            macLogError("ipc", "Failed to start WebSocket server on localhost:" + std::to_string(WEBSOCKET_PORT));
            appendRuntimeJournalEvent("ws_server_failed", {{"port", WEBSOCKET_PORT}});
        }
    } else {
        g_data.addMessage("WebSocket server is disabled in settings");
        appendRuntimeJournalEvent("ws_server_disabled");
    }

    if (connectionConfig.controllerEnabled) {
        {
            std::lock_guard<std::mutex> lock(impl_->controllerMutex);
            controllerInitialize(impl_->controllerState);
        }
        impl_->controllerRunning.store(true, std::memory_order_relaxed);
        impl_->controllerThread = std::thread(&Impl::controllerLoop, impl_.get());
    } else {
        g_data.controllerConnected.store(false);
        g_data.addMessage("Controller input is disabled in settings");
        appendRuntimeJournalEvent("controller_disabled");
    }

    impl_->started = true;
    if (recovery.priorSessionAbnormal) {
        g_data.addMessage(recovery.bannerText);
    }
    for (const auto& summary : unfinishedTraces) {
        g_data.addMessage("Recovered unfinished trace from prior run: " + summary);
    }
    requestUiInvalidation();
    return twsConnected;
}

void TradingRuntime::shutdown() {
    if (!impl_ || !impl_->started) {
        return;
    }

    impl_->controllerRunning.store(false, std::memory_order_relaxed);
    if (impl_->controllerThread.joinable()) {
        impl_->controllerThread.join();
    }
    {
        std::lock_guard<std::mutex> lock(impl_->controllerMutex);
        controllerCleanup(impl_->controllerState);
    }

    if (impl_->wsServer) {
        if (g_data.wsServerRunning.load()) {
            std::cout << "Stopping WebSocket server..." << std::endl;
            impl_->wsServer->stop();
            g_data.wsServerRunning.store(false);
            requestUiInvalidation();
        }
        impl_->wsServer.reset();
        ix::uninitNetSystem();
    }

    if (impl_->client) {
        cancelActiveSubscription(impl_->client.get());
    }

    impl_->readerRunning.store(false, std::memory_order_relaxed);
    if (impl_->osSignal) {
        impl_->osSignal->issueSignal();
    }

    if (impl_->readerThread.joinable()) {
        impl_->readerThread.join();
    }

    impl_->reader.reset();

    if (impl_->client) {
        if (impl_->client->isConnected()) {
            impl_->client->eDisconnect();
        }
        impl_->client.reset();
    }

    impl_->wrapper.setClient(nullptr);
    impl_->osSignal.reset();
    appendRuntimeJournalEvent("runtime_shutdown");
    setRuntimeSessionState(RuntimeSessionState::Disconnected);
    unbindSharedDataOwner(&impl_->sharedData);
    clearUiInvalidationCallback();
    impl_->started = false;
    macLogInfo("runtime", "Runtime shutdown complete");
}

bool TradingRuntime::isStarted() const {
    return impl_ && impl_->started;
}

void TradingRuntime::setControllerVibration(bool enable) {
    if (!impl_) {
        return;
    }

    std::lock_guard<std::mutex> lock(impl_->controllerMutex);
    if (impl_->controllerState.vibrating == enable) {
        return;
    }
    controllerSetVibration(impl_->controllerState, enable);
}

bool TradingRuntime::requestSymbolSubscription(const std::string& symbol, bool recalcQtyFromFirstAsk, std::string* error) {
    if (!impl_ || !impl_->client) {
        if (error) {
            *error = "Trading runtime is not started";
        }
        return false;
    }
    return ::requestSymbolSubscription(impl_->client.get(), symbol, recalcQtyFromFirstAsk, error);
}

bool TradingRuntime::submitOrderIntent(const SubmitIntent& intent,
                                       double quantity,
                                       double limitPrice,
                                       bool closeOnly,
                                       std::string* error,
                                       std::uint64_t* outTraceId) {
    if (!impl_ || !impl_->client) {
        if (error) {
            *error = "Trading runtime is not started";
        }
        return false;
    }
    return submitLimitOrder(impl_->client.get(),
                            intent.symbol,
                            intent.side,
                            quantity,
                            limitPrice,
                            closeOnly,
                            &intent,
                            error,
                            nullptr,
                            outTraceId);
}

std::vector<bool> TradingRuntime::requestCancelOrders(const std::vector<OrderId>& orderIds) {
    if (!impl_ || !impl_->client) {
        return std::vector<bool>(orderIds.size(), false);
    }
    return sendCancelRequests(impl_->client.get(), orderIds);
}
