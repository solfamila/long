#include "trading_runtime.h"

#include "app_shared.h"
#include "controller.h"
#include "trading_wrapper.h"
#include "websocket_handlers.h"

#include <chrono>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

namespace {

constexpr auto kControllerPollInterval = std::chrono::milliseconds(16);

} // namespace

struct TradingRuntime::Impl {
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

    impl_->installUiInvalidationCallback();

    std::cout << "=== TWS Trading GUI ===" << std::endl;
    std::cout << "Connecting to TWS at " << DEFAULT_HOST << ":" << DEFAULT_PORT << std::endl;
    std::cout << "Configured account: " << HARDCODED_ACCOUNT << std::endl;
    std::cout << "Using platform: AppKit native views" << std::endl;

    impl_->osSignal = std::make_unique<EReaderOSSignal>(2000);
    impl_->client = std::make_unique<EClientSocket>(&impl_->wrapper, impl_->osSignal.get());
    impl_->wrapper.setClient(impl_->client.get());

    const bool twsConnected = impl_->client->eConnect(DEFAULT_HOST, DEFAULT_PORT, DEFAULT_CLIENT_ID);
    if (!twsConnected) {
        std::cerr << "Failed to connect to TWS" << std::endl;
        g_data.addMessage("Failed to connect to TWS");
    } else {
        std::cout << "Connected to TWS socket" << std::endl;
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
        std::cout << "[WebSocket server started on localhost port " << WEBSOCKET_PORT << "]" << std::endl;
    } else {
        g_data.addMessage("Failed to start WebSocket server on port " + std::to_string(WEBSOCKET_PORT));
        std::cerr << "Failed to start WebSocket server on port " << WEBSOCKET_PORT << std::endl;
    }

    {
        std::lock_guard<std::mutex> lock(impl_->controllerMutex);
        controllerInitialize(impl_->controllerState);
    }
    impl_->controllerRunning.store(true, std::memory_order_relaxed);
    impl_->controllerThread = std::thread(&Impl::controllerLoop, impl_.get());

    impl_->started = true;
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
    }
    ix::uninitNetSystem();

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
    clearUiInvalidationCallback();
    impl_->started = false;
}

EClientSocket* TradingRuntime::client() const {
    return impl_ ? impl_->client.get() : nullptr;
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
