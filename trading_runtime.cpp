#include "trading_runtime.h"

#include "app_shared.h"
#include "bridge_batch_transport.h"
#include "controller.h"
#include "mac_observability.h"
#include "trading_wrapper.h"
#include "websocket_handlers.h"

#include <chrono>
#include <condition_variable>
#include <deque>
#include <future>
#include <iomanip>
#include <mutex>
#include <random>
#include <sstream>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace {

constexpr auto kControllerPollInterval = std::chrono::milliseconds(16);
constexpr auto kOrderWatchdogPollInterval = std::chrono::milliseconds(250);
constexpr auto kBridgeFlushInterval = std::chrono::milliseconds(1);
constexpr auto kBridgeRetryInterval = std::chrono::milliseconds(250);
constexpr char kBridgeSocketEnv[] = "LONG_TAPE_ENGINE_SOCKET";
constexpr char kDefaultBridgeSocketPath[] = "/tmp/tape-engine.sock";
thread_local void* tlsActionLoopOwner = nullptr;

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

std::string resolveBridgeSocketPath() {
    const char* const path = std::getenv(kBridgeSocketEnv);
    if (path != nullptr && path[0] != '\0') {
        return path;
    }
    return kDefaultBridgeSocketPath;
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
    std::thread actionThread;
    std::thread bridgeThread;
    std::atomic<bool> readerRunning{false};
    std::atomic<bool> controllerRunning{false};
    mutable std::mutex callbackMutex;
    std::mutex controllerMutex;
    std::mutex actionMutex;
    std::condition_variable actionCv;
    std::deque<std::function<void()>> actionQueue;
    bool stopActionThread = false;
    std::mutex bridgeMutex;
    std::condition_variable bridgeCv;
    bool stopBridgeThread = false;
    bool bridgeWakeRequested = false;
    UiInvalidationCallback uiInvalidationCallback;
    ControllerActionCallback controllerActionCallback;
    ControllerState controllerState;
    bool started = false;

    void issueReconciliationRefreshes(const OrderWatchdogSweepResult& sweep) {
        if (sweep.reconciliationOrders.empty() && sweep.manualReviewOrders.empty()) {
            return;
        }

        for (const auto& item : sweep.reconciliationOrders) {
            appendRuntimeJournalEvent(item.reason, {
                {"orderId", static_cast<long long>(item.orderId)},
                {"reconciliationAttempts", item.reconciliationAttempts}
            });
            appendRuntimeJournalEvent("reconcile_begin", {
                {"orderId", static_cast<long long>(item.orderId)},
                {"reason", item.reason},
                {"reconciliationAttempts", item.reconciliationAttempts}
            });
            appendTraceEventByOrderId(item.orderId,
                                      TradeEventType::Note,
                                      "Watchdog",
                                      "Reconciliation started: " + item.reason);
            appendSharedMessage("Order " + std::to_string(static_cast<long long>(item.orderId)) +
                                " reconciling after " + item.reason +
                                " (attempt " + std::to_string(item.reconciliationAttempts) + ")");
        }

        for (const auto& item : sweep.manualReviewOrders) {
            appendRuntimeJournalEvent("manual_review_required", {
                {"orderId", static_cast<long long>(item.orderId)},
                {"reason", item.reason},
                {"reconciliationAttempts", item.reconciliationAttempts}
            });
            appendTraceEventByOrderId(item.orderId,
                                      TradeEventType::Note,
                                      "ManualReview",
                                      "Manual review required: " + item.reason);
            appendSharedMessage("Order " + std::to_string(static_cast<long long>(item.orderId)) +
                                " requires manual review (" + item.reason + ")");
        }

        if (!sweep.reconciliationOrders.empty() && client) {
            SharedData& state = appState();
            std::lock_guard<std::recursive_mutex> clientLock(state.clientMutex);
            if (client->isConnected()) {
                client->reqOpenOrders();
                appendRuntimeJournalEvent("reconcile_open_orders_requested", {
                    {"orderCount", static_cast<int>(sweep.reconciliationOrders.size())}
                });

                ExecutionFilter executionFilter;
                client->reqExecutions(allocateReqId(), executionFilter);
                appendRuntimeJournalEvent("reconcile_executions_requested", {
                    {"orderCount", static_cast<int>(sweep.reconciliationOrders.size())}
                });

                client->reqIds(-1);
                appendRuntimeJournalEvent("reconcile_order_ids_requested", {
                    {"orderCount", static_cast<int>(sweep.reconciliationOrders.size())}
                });
            } else {
                appendRuntimeJournalEvent("reconcile_refresh_skipped", {
                    {"reason", "socket_not_connected"},
                    {"orderCount", static_cast<int>(sweep.reconciliationOrders.size())}
                });
            }
        }

        requestUiInvalidation();
    }

    void checkOrderWatchdogs() {
        const OrderWatchdogSweepResult sweep =
            sweepOrderWatchdogsOnReducerThread(std::chrono::steady_clock::now());
        if (!sweep.reconciliationOrders.empty() || !sweep.manualReviewOrders.empty()) {
            publishSharedDataSnapshot();
            issueReconciliationRefreshes(sweep);
        }
    }

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

    void actionLoop() {
        tlsActionLoopOwner = this;

        while (true) {
            std::function<void()> action;
            bool ranAction = false;
            {
                std::unique_lock<std::mutex> lock(actionMutex);
                actionCv.wait_for(lock, kOrderWatchdogPollInterval, [&] {
                    return stopActionThread || !actionQueue.empty();
                });
                if (stopActionThread && actionQueue.empty()) {
                    break;
                }
                if (!actionQueue.empty()) {
                    action = std::move(actionQueue.front());
                    actionQueue.pop_front();
                    ranAction = true;
                }
            }
            if (ranAction && action) {
                action();
                publishSharedDataSnapshot();
                notifyBridgeThread();
            }
            checkOrderWatchdogs();
        }

        tlsActionLoopOwner = nullptr;
    }

    void postOnActionThread(std::function<void()> action) {
        if (!action) {
            return;
        }
        if (tlsActionLoopOwner == this) {
            action();
            return;
        }
        {
            std::lock_guard<std::mutex> lock(actionMutex);
            if (stopActionThread) {
                return;
            }
            actionQueue.emplace_back(std::move(action));
        }
        actionCv.notify_one();
    }

    void startActionLoop() {
        {
            std::lock_guard<std::mutex> lock(actionMutex);
            stopActionThread = false;
            actionQueue.clear();
        }
        actionThread = std::thread(&Impl::actionLoop, this);
    }

    void notifyBridgeThread() {
        {
            std::lock_guard<std::mutex> lock(bridgeMutex);
            bridgeWakeRequested = true;
        }
        bridgeCv.notify_one();
    }

    void bridgeLoop() {
        const std::string socketPath = resolveBridgeSocketPath();
        bridge_batch::UnixDomainSocketTransport transport(socketPath);
        bridge_batch::Sender sender(transport);
        bridge_batch::BatchPolicy policy;
        std::uint64_t nextBatchSeq = 1;
        std::chrono::steady_clock::time_point flushDeadline{};
        bool flushDeadlineArmed = false;

        appendRuntimeJournalEvent("bridge_sender_started", {
            {"socketPath", socketPath},
            {"flushIntervalMs", static_cast<int>(kBridgeFlushInterval.count())},
            {"retryIntervalMs", static_cast<int>(kBridgeRetryInterval.count())},
            {"maxRecords", static_cast<int>(policy.maxRecords)},
            {"maxPayloadBytes", static_cast<int>(policy.maxPayloadBytes)}
        });

        while (true) {
            if (sender.pendingBatchCount() > 0) {
                const bridge_batch::DrainResult drain = sender.drainPending();
                if (!drain.deliveredBatches.empty()) {
                    for (const auto& delivered : drain.deliveredBatches) {
                        const std::size_t removed = acknowledgeDeliveredBridgeRecords(delivered.records);
                        if (removed != delivered.records.size()) {
                            appendRuntimeJournalEvent("bridge_outbox_ack_mismatch", {
                                {"expected", static_cast<int>(delivered.records.size())},
                                {"removed", static_cast<int>(removed)},
                                {"batchSeq", static_cast<unsigned long long>(delivered.header.batchSeq)}
                            });
                        }
                    }
                    flushDeadlineArmed = false;
                    continue;
                }
                if (drain.blocked) {
                    noteBridgeTransportUnavailable(drain.error);
                    std::unique_lock<std::mutex> lock(bridgeMutex);
                    bridgeWakeRequested = false;
                    bridgeCv.wait_for(lock, kBridgeRetryInterval, [&]() {
                        return stopBridgeThread;
                    });
                    if (stopBridgeThread) {
                        break;
                    }
                    continue;
                }
            }

            const BridgeDispatchSnapshot dispatch = captureBridgeDispatchSnapshot(policy.maxRecords + 1);
            if (dispatch.records.empty()) {
                flushDeadlineArmed = false;
                std::unique_lock<std::mutex> lock(bridgeMutex);
                bridgeCv.wait(lock, [&]() {
                    return stopBridgeThread || bridgeWakeRequested;
                });
                if (stopBridgeThread) {
                    break;
                }
                bridgeWakeRequested = false;
                continue;
            }

            bridge_batch::BuildOptions options;
            options.appSessionId = dispatch.appSessionId;
            options.runtimeSessionId = dispatch.runtimeSessionId;
            options.batchSeq = nextBatchSeq;

            const bridge_batch::PreparedBatch prepared =
                bridge_batch::prepareBatch(dispatch.records, options, policy);
            if (prepared.batch.records.empty()) {
                std::unique_lock<std::mutex> lock(bridgeMutex);
                bridgeCv.wait_for(lock, kBridgeFlushInterval, [&]() {
                    return stopBridgeThread || bridgeWakeRequested;
                });
                if (stopBridgeThread) {
                    break;
                }
                bridgeWakeRequested = false;
                continue;
            }

            const bool shouldFlushNow = prepared.immediateFlush ||
                                        prepared.reachedRecordLimit ||
                                        prepared.reachedPayloadLimit;
            if (!shouldFlushNow) {
                const auto now = std::chrono::steady_clock::now();
                if (!flushDeadlineArmed) {
                    flushDeadline = now + kBridgeFlushInterval;
                    flushDeadlineArmed = true;
                }
                if (now < flushDeadline) {
                    std::unique_lock<std::mutex> lock(bridgeMutex);
                    bridgeCv.wait_until(lock, flushDeadline, [&]() {
                        return stopBridgeThread || bridgeWakeRequested;
                    });
                    if (stopBridgeThread) {
                        break;
                    }
                    bridgeWakeRequested = false;
                    continue;
                }
            }

            options.flushReason = prepared.immediateFlush
                ? bridge_batch::FlushReason::ImmediateLifecycle
                : (prepared.reachedPayloadLimit
                    ? bridge_batch::FlushReason::ThresholdBytes
                    : (prepared.reachedRecordLimit
                        ? bridge_batch::FlushReason::ThresholdRecords
                        : bridge_batch::FlushReason::TimerElapsed));
            const bridge_batch::PreparedBatch publishPrepared =
                bridge_batch::prepareBatch(dispatch.records, options, policy);

            const bridge_batch::PublishResult publish = sender.publish(publishPrepared.batch);
            ++nextBatchSeq;
            flushDeadlineArmed = false;
            if (publish.delivered) {
                const std::size_t removed = acknowledgeDeliveredBridgeRecords(publishPrepared.batch.records);
                if (removed != publishPrepared.batch.records.size()) {
                    appendRuntimeJournalEvent("bridge_outbox_ack_mismatch", {
                        {"expected", static_cast<int>(publishPrepared.batch.records.size())},
                        {"removed", static_cast<int>(removed)},
                        {"batchSeq", static_cast<unsigned long long>(publishPrepared.batch.header.batchSeq)}
                    });
                }
                continue;
            }

            if (publish.queuedForRetry) {
                noteBridgeTransportUnavailable(publish.error);
                std::unique_lock<std::mutex> lock(bridgeMutex);
                bridgeWakeRequested = false;
                bridgeCv.wait_for(lock, kBridgeRetryInterval, [&]() {
                    return stopBridgeThread;
                });
                if (stopBridgeThread) {
                    break;
                }
            }
        }
    }

    void startBridgeLoop() {
        {
            std::lock_guard<std::mutex> lock(bridgeMutex);
            stopBridgeThread = false;
            bridgeWakeRequested = false;
        }
        bridgeThread = std::thread(&Impl::bridgeLoop, this);
    }

    void stopBridgeLoop() {
        {
            std::lock_guard<std::mutex> lock(bridgeMutex);
            stopBridgeThread = true;
            bridgeWakeRequested = true;
        }
        bridgeCv.notify_all();
        if (bridgeThread.joinable()) {
            bridgeThread.join();
        }
    }

    void stopActionLoop() {
        {
            std::lock_guard<std::mutex> lock(actionMutex);
            stopActionThread = true;
        }
        actionCv.notify_all();
        if (actionThread.joinable()) {
            actionThread.join();
        }
        {
            std::lock_guard<std::mutex> lock(actionMutex);
            actionQueue.clear();
        }
    }

    void drainActionQueue() {
        invokeOnActionThread([] {});
    }

    template <typename Fn>
    auto invokeOnActionThread(Fn&& fn) -> decltype(fn()) {
        using Result = decltype(fn());
        if (tlsActionLoopOwner == this) {
            return fn();
        }

        bool runInline = false;
        auto promise = std::make_shared<std::promise<Result>>();
        auto future = promise->get_future();
        {
            std::lock_guard<std::mutex> lock(actionMutex);
            if (stopActionThread) {
                runInline = true;
            } else {
                actionQueue.emplace_back([promise, fn = std::forward<Fn>(fn)]() mutable {
                    try {
                        if constexpr (std::is_void_v<Result>) {
                            fn();
                            promise->set_value();
                        } else {
                            promise->set_value(fn());
                        }
                    } catch (...) {
                        promise->set_exception(std::current_exception());
                    }
                });
            }
        }
        if (runInline) {
            if constexpr (std::is_void_v<Result>) {
                fn();
                return;
            } else {
                return fn();
            }
        }
        actionCv.notify_one();
        return future.get();
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
    impl_->startActionLoop();
    setSharedDataMutationDispatcher([impl = impl_.get()](std::function<void()> mutation) {
        impl->invokeOnActionThread([mutation = std::move(mutation)]() mutable {
            mutation();
        });
    });

    const RuntimeConnectionConfig connectionConfig = captureRuntimeConnectionConfig();
    const std::string websocketToken = ensureWebSocketAuthToken();
    const auto unfinishedTraces = recoverUnfinishedTraceSummariesFromLog(5);
    const RuntimeRecoverySnapshot recovery = recoverRuntimeRecoverySnapshot(5);
    seedBridgeOutboxRecoveryState(recovery);
    impl_->invokeOnActionThread([&]() {
        trading_engine::reduce(appState(), trading_engine::RuntimeBootstrapEvent{
            makeSessionIdentifier("app"),
            makeSessionIdentifier("runtime"),
            recovery.bannerText
        });
    });
    impl_->startBridgeLoop();

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
    if (recovery.bridgeRecoveryRequired) {
        appendRuntimeJournalEvent("bridge_recovery_pending", {
            {"pendingCount", recovery.pendingOutboxCount},
            {"lossCount", recovery.outboxLossCount},
            {"lastSourceSeq", static_cast<unsigned long long>(recovery.lastOutboxSourceSeq)}
        });
    }
    setRuntimeSessionState(RuntimeSessionState::Connecting);

    impl_->osSignal = std::make_unique<EReaderOSSignal>(2000);
    impl_->client = std::make_unique<EClientSocket>(&impl_->wrapper, impl_->osSignal.get());
    impl_->wrapper.setClient(impl_->client.get());
    impl_->wrapper.setEventDispatcher([impl = impl_.get()](std::function<void()> event) {
        impl->postOnActionThread(std::move(event));
    });

    const bool twsConnected = impl_->client->eConnect(connectionConfig.host.c_str(),
                                                      connectionConfig.port,
                                                      connectionConfig.clientId);
    if (!twsConnected) {
        std::cerr << "Failed to connect to TWS" << std::endl;
        appendSharedMessage("Failed to connect to TWS");
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
        TradingRuntime* const runtime = this;
        Impl* const impl = impl_.get();
        impl_->wsServer->setOnClientMessageCallback(
            [runtime, impl](std::shared_ptr<ix::ConnectionState> connectionState,
                            ix::WebSocket& webSocket,
                            const ix::WebSocketMessagePtr& msg) {
                (void)connectionState;

                if (msg->type == ix::WebSocketMessageType::Message) {
                    handleWebSocketMessage(msg->str, webSocket, runtime);
                } else if (msg->type == ix::WebSocketMessageType::Open) {
                    impl->postOnActionThread([]() {
                        const int total = adjustWebSocketConnectedClients(1);
                        appendSharedMessage("WebSocket client connected (total: " + std::to_string(total) + ")");
                        std::cout << "[WebSocket client connected]" << std::endl;
                    });
                } else if (msg->type == ix::WebSocketMessageType::Close) {
                    impl->postOnActionThread([]() {
                        const int total = adjustWebSocketConnectedClients(-1);
                        appendSharedMessage("WebSocket client disconnected (total: " + std::to_string(total) + ")");
                        std::cout << "[WebSocket client disconnected]" << std::endl;
                    });
                } else if (msg->type == ix::WebSocketMessageType::Error) {
                    const std::string reason = msg->errorInfo.reason;
                    impl->postOnActionThread([reason]() {
                        appendSharedMessage("WebSocket error: " + reason);
                        std::cout << "[WebSocket error: " << reason << "]" << std::endl;
                    });
                }
            });

        if (impl_->wsServer->listenAndStart()) {
            setWebSocketServerRunning(true);
            appendSharedMessage("WebSocket server started on localhost port " + std::to_string(WEBSOCKET_PORT));
            appendSharedMessage("WebSocket token: " + websocketToken);
            std::cout << "[WebSocket server started on localhost port " << WEBSOCKET_PORT << "]" << std::endl;
            macLogInfo("ipc", "WebSocket server started on localhost:" + std::to_string(WEBSOCKET_PORT));
            appendRuntimeJournalEvent("ws_server_started", {
                {"host", WEBSOCKET_HOST},
                {"port", WEBSOCKET_PORT}
            });
        } else {
            appendSharedMessage("Failed to start WebSocket server on port " + std::to_string(WEBSOCKET_PORT));
            std::cerr << "Failed to start WebSocket server on port " << WEBSOCKET_PORT << std::endl;
            macLogError("ipc", "Failed to start WebSocket server on localhost:" + std::to_string(WEBSOCKET_PORT));
            appendRuntimeJournalEvent("ws_server_failed", {{"port", WEBSOCKET_PORT}});
        }
    } else {
        appendSharedMessage("WebSocket server is disabled in settings");
        appendRuntimeJournalEvent("ws_server_disabled");
    }

    if (connectionConfig.controllerEnabled) {
        controllerInitialize(impl_->controllerState);
        impl_->controllerRunning.store(true, std::memory_order_relaxed);
        impl_->controllerThread = std::thread(&Impl::controllerLoop, impl_.get());
    } else {
        updateControllerConnectionState(false, "");
        appendSharedMessage("Controller input is disabled in settings");
        appendRuntimeJournalEvent("controller_disabled");
    }

    impl_->started = true;
    if (!recovery.bannerText.empty()) {
        appendSharedMessage(recovery.bannerText);
    }
    for (const auto& summary : unfinishedTraces) {
        appendSharedMessage("Recovered unfinished trace from prior run: " + summary);
    }
    requestUiInvalidation();
    return twsConnected;
}

void TradingRuntime::shutdown() {
    if (!impl_ || !impl_->started) {
        return;
    }

    impl_->started = false;
    SharedData& state = appState();
    impl_->stopBridgeLoop();

    impl_->controllerRunning.store(false, std::memory_order_relaxed);
    if (impl_->controllerThread.joinable()) {
        impl_->controllerThread.join();
    }
    controllerCleanup(impl_->controllerState);

    if (impl_->wsServer) {
        if (state.wsServerRunning.load()) {
            std::cout << "Stopping WebSocket server..." << std::endl;
            impl_->wsServer->stop();
            setWebSocketServerRunning(false);
        }
        impl_->wsServer.reset();
        ix::uninitNetSystem();
    }

    impl_->wrapper.setEventDispatcher({});

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
    impl_->drainActionQueue();

    if (impl_->client) {
        if (impl_->client->isConnected()) {
            impl_->client->eDisconnect();
        }
    }

    impl_->wrapper.setClient(nullptr);
    impl_->client.reset();
    impl_->osSignal.reset();
    appendRuntimeJournalEvent("runtime_shutdown");
    setRuntimeSessionState(RuntimeSessionState::Disconnected);
    clearSharedDataMutationDispatcher();
    impl_->stopActionLoop();
    unbindSharedDataOwner(&impl_->sharedData);
    clearUiInvalidationCallback();
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

void TradingRuntime::syncGuiInputs(int quantityInput, double priceBuffer, double maxPositionDollars) {
    if (!impl_ || !impl_->started) {
        ::syncSharedGuiInputs(quantityInput, priceBuffer, maxPositionDollars);
        return;
    }
    impl_->invokeOnActionThread([quantityInput, priceBuffer, maxPositionDollars]() {
        ::syncSharedGuiInputs(quantityInput, priceBuffer, maxPositionDollars);
    });
}

RuntimePresentationSnapshot TradingRuntime::capturePresentationSnapshot(const std::string& subscribedSymbol,
                                                                        std::size_t maxTraceItems) const {
    return ::captureRuntimePresentationSnapshot(subscribedSymbol, maxTraceItems);
}

PendingUiSyncUpdate TradingRuntime::consumePendingUiSyncUpdate() {
    if (!impl_ || !impl_->started) {
        return ::consumePendingUiSyncUpdate();
    }
    return impl_->invokeOnActionThread([]() {
        return ::consumePendingUiSyncUpdate();
    });
}

RuntimeConnectionConfig TradingRuntime::captureConnectionConfig() const {
    return ::captureRuntimeConnectionConfig();
}

void TradingRuntime::updateConnectionConfig(const RuntimeConnectionConfig& config) {
    if (!impl_ || !impl_->started) {
        ::updateRuntimeConnectionConfig(config);
        return;
    }
    impl_->invokeOnActionThread([config]() {
        ::updateRuntimeConnectionConfig(config);
    });
}

RiskControlsSnapshot TradingRuntime::captureRiskControls() const {
    return ::captureRiskControlsSnapshot();
}

void TradingRuntime::updateRiskControls(const RiskControlsSnapshot& risk) {
    if (!impl_ || !impl_->started) {
        ::updateRiskControls(risk.staleQuoteThresholdMs,
                             risk.brokerEchoTimeoutMs,
                             risk.cancelAckTimeoutMs,
                             risk.partialFillQuietTimeoutMs,
                             risk.maxOrderNotional,
                             risk.maxOpenNotional,
                             risk.controllerArmMode);
        return;
    }
    impl_->invokeOnActionThread([risk]() {
        ::updateRiskControls(risk.staleQuoteThresholdMs,
                             risk.brokerEchoTimeoutMs,
                             risk.cancelAckTimeoutMs,
                             risk.partialFillQuietTimeoutMs,
                             risk.maxOrderNotional,
                             risk.maxOpenNotional,
                             risk.controllerArmMode);
    });
}

void TradingRuntime::setControllerArmed(bool armed) {
    if (!impl_ || !impl_->started) {
        ::setControllerArmed(armed);
        return;
    }
    impl_->invokeOnActionThread([armed]() {
        ::setControllerArmed(armed);
    });
}

void TradingRuntime::setTradingKillSwitch(bool enabled) {
    if (!impl_ || !impl_->started) {
        ::setTradingKillSwitch(enabled);
        return;
    }
    impl_->invokeOnActionThread([enabled]() {
        ::setTradingKillSwitch(enabled);
    });
}

std::string TradingRuntime::ensureWebSocketAuthToken() {
    if (!impl_ || !impl_->started) {
        return ::ensureWebSocketAuthToken();
    }
    return impl_->invokeOnActionThread([]() {
        return ::ensureWebSocketAuthToken();
    });
}

TradeTraceSnapshot TradingRuntime::captureTradeTraceSnapshot(std::uint64_t traceId) const {
    return ::captureTradeTraceSnapshot(traceId);
}

BridgeOutboxSnapshot TradingRuntime::captureBridgeOutboxSnapshot(std::size_t maxItems) const {
    return ::captureBridgeOutboxSnapshot(maxItems);
}

std::uint64_t TradingRuntime::findTradeTraceIdByOrderId(OrderId orderId) const {
    return ::findTraceIdByOrderId(orderId);
}

void TradingRuntime::appendMessage(const std::string& message) {
    if (message.empty()) {
        return;
    }
    if (!impl_ || !impl_->started) {
        ::appendSharedMessage(message);
        return;
    }
    impl_->postOnActionThread([message]() {
        ::appendSharedMessage(message);
    });
}

std::vector<OrderId> TradingRuntime::markOrdersPendingCancel(const std::vector<OrderId>& orderIds) {
    if (!impl_ || !impl_->started) {
        return ::markOrdersPendingCancel(orderIds);
    }
    return impl_->invokeOnActionThread([orderIds]() {
        return ::markOrdersPendingCancel(orderIds);
    });
}

std::vector<OrderId> TradingRuntime::markAllPendingOrdersForCancel() {
    if (!impl_ || !impl_->started) {
        return ::markAllPendingOrdersForCancel();
    }
    return impl_->invokeOnActionThread([]() {
        return ::markAllPendingOrdersForCancel();
    });
}

std::vector<OrderId> TradingRuntime::acknowledgeManualReviewOrders(const std::vector<OrderId>& orderIds) {
    if (!impl_ || !impl_->started) {
        const auto acknowledged = ::acknowledgeManualReviewOrders(orderIds);
        if (!acknowledged.empty()) {
            requestUiInvalidation();
        }
        return acknowledged;
    }
    return impl_->invokeOnActionThread([orderIds]() {
        const auto acknowledged = ::acknowledgeManualReviewOrders(orderIds);
        for (const OrderId orderId : acknowledged) {
            appendRuntimeJournalEvent("manual_review_acknowledged", {
                {"orderId", static_cast<long long>(orderId)}
            });
            appendTraceEventByOrderId(orderId,
                                      TradeEventType::Note,
                                      "ManualReview",
                                      "Manual review acknowledged");
            appendSharedMessage("Manual review acknowledged for order " +
                                std::to_string(static_cast<long long>(orderId)));
        }
        return acknowledged;
    });
}

std::vector<OrderId> TradingRuntime::requestOrderReconciliation(const std::vector<OrderId>& orderIds,
                                                                const std::string& reason) {
    if (orderIds.empty()) {
        return {};
    }
    if (!impl_ || !impl_->started) {
        const auto sweep = ::requestOrderReconciliation(orderIds, reason);
        if (!sweep.reconciliationOrders.empty()) {
            requestUiInvalidation();
        }
        std::vector<OrderId> accepted;
        accepted.reserve(sweep.reconciliationOrders.size());
        for (const auto& item : sweep.reconciliationOrders) {
            accepted.push_back(item.orderId);
        }
        return accepted;
    }
    return impl_->invokeOnActionThread([this, orderIds, reason]() {
        const auto sweep = ::requestOrderReconciliation(orderIds, reason);
        publishSharedDataSnapshot();
        impl_->issueReconciliationRefreshes(sweep);

        std::vector<OrderId> accepted;
        accepted.reserve(sweep.reconciliationOrders.size());
        for (const auto& item : sweep.reconciliationOrders) {
            accepted.push_back(item.orderId);
        }
        return accepted;
    });
}

bool TradingRuntime::requestWebSocketSubscription(const std::string& symbol,
                                                  std::string* error,
                                                  bool* duplicateIgnored,
                                                  bool* alreadySubscribed) {
    if (duplicateIgnored) {
        *duplicateIgnored = false;
    }
    if (alreadySubscribed) {
        *alreadySubscribed = false;
    }
    if (!impl_ || !impl_->started) {
        if (error) {
            *error = "Trading runtime is not started";
        }
        return false;
    }

    return impl_->invokeOnActionThread([this, symbol, error, duplicateIgnored, alreadySubscribed]() {
        if (!impl_ || !impl_->client) {
            if (error) {
                *error = "Trading runtime is not started";
            }
            return false;
        }

        const std::string normalizedSymbol = toUpperCase(symbol);
        if (normalizedSymbol.empty()) {
            if (error) {
                *error = "Symbol cannot be empty";
            }
            return false;
        }

        SharedData& state = appState();
        switch (trading_engine::reduce(state, trading_engine::WebSocketSubscribeRequestedEvent{
            normalizedSymbol,
            std::chrono::steady_clock::now()
        })) {
            case trading_engine::WebSocketSubscribeDecision::SessionNotReady:
                if (error) {
                    *error = "TWS session not ready";
                }
                return false;
            case trading_engine::WebSocketSubscribeDecision::DuplicateIgnored:
                if (duplicateIgnored) {
                    *duplicateIgnored = true;
                }
                return true;
            case trading_engine::WebSocketSubscribeDecision::AlreadySubscribed:
                if (alreadySubscribed) {
                    *alreadySubscribed = true;
                }
                return true;
            case trading_engine::WebSocketSubscribeDecision::Proceed:
                break;
        }

        return ::requestSymbolSubscription(impl_->client.get(), normalizedSymbol, true, error);
    });
}

bool TradingRuntime::requestSymbolSubscription(const std::string& symbol, bool recalcQtyFromFirstAsk, std::string* error) {
    if (!impl_ || !impl_->started) {
        if (error) {
            *error = "Trading runtime is not started";
        }
        return false;
    }
    return impl_->invokeOnActionThread([this, symbol, recalcQtyFromFirstAsk, error]() {
        if (!impl_ || !impl_->client) {
            if (error) {
                *error = "Trading runtime is not started";
            }
            return false;
        }
        return ::requestSymbolSubscription(impl_->client.get(), symbol, recalcQtyFromFirstAsk, error);
    });
}

bool TradingRuntime::submitOrderIntent(const SubmitIntent& intent,
                                       double quantity,
                                       double limitPrice,
                                       bool closeOnly,
                                       std::string* error,
                                       std::uint64_t* outTraceId,
                                       OrderId* outOrderId,
                                       BridgeOutboxEnqueueResult* outBridgeOutbox) {
    if (!impl_ || !impl_->started) {
        if (error) {
            *error = "Trading runtime is not started";
        }
        return false;
    }
    return impl_->invokeOnActionThread([this, intent, quantity, limitPrice, closeOnly, error, outTraceId, outOrderId, outBridgeOutbox]() {
        if (!impl_ || !impl_->client) {
            if (error) {
                *error = "Trading runtime is not started";
            }
            return false;
        }

        OrderId orderId = 0;
        std::uint64_t traceId = 0;
        const bool submitted = submitLimitOrder(impl_->client.get(),
                                                intent.symbol,
                                                intent.side,
                                                quantity,
                                                limitPrice,
                                                closeOnly,
                                                &intent,
                                                error,
                                                &orderId,
                                                &traceId);
        if (outOrderId) {
            *outOrderId = orderId;
        }
        if (outTraceId) {
            *outTraceId = traceId;
        }
        if (!submitted) {
            if (outBridgeOutbox) {
                *outBridgeOutbox = {};
            }
            return false;
        }

        BridgeOutboxRecordInput bridgeRecord;
        bridgeRecord.recordType = "order_intent";
        bridgeRecord.source = intent.source;
        bridgeRecord.symbol = intent.symbol;
        bridgeRecord.side = intent.side;
        bridgeRecord.traceId = traceId;
        bridgeRecord.orderId = orderId;
        bridgeRecord.note = intent.notes;
        const BridgeOutboxEnqueueResult bridgeResult = enqueueBridgeOutboxRecord(bridgeRecord);
        if (outBridgeOutbox) {
            *outBridgeOutbox = bridgeResult;
        }
        if (traceId != 0) {
            std::ostringstream details;
            details << "Queued bridge outbox source_seq="
                    << static_cast<unsigned long long>(bridgeResult.sourceSeq)
                    << " state=" << bridgeResult.fallbackState
                    << " reason=" << bridgeResult.fallbackReason;
            appendTraceEventByTraceId(traceId,
                                      TradeEventType::Note,
                                      "BridgeOutbox",
                                      details.str());
        }
        return true;
    });
}

std::vector<bool> TradingRuntime::requestCancelOrders(const std::vector<OrderId>& orderIds) {
    if (!impl_ || !impl_->started) {
        return std::vector<bool>(orderIds.size(), false);
    }
    return impl_->invokeOnActionThread([this, orderIds]() {
        if (!impl_ || !impl_->client) {
            return std::vector<bool>(orderIds.size(), false);
        }
        return sendCancelRequests(impl_->client.get(), orderIds);
    });
}
