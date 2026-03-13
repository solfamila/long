#include "app_shared.h"
#include "app_platform.h"
#if defined(_WIN32)
#include "platform_win32.h"
#endif
#include "controller.h"
#include "websocket_handlers.h"
#include "trading_wrapper.h"
#include "trading_panel.h"

int main() {
    std::cout << "=== TWS Trading GUI ===" << std::endl;
    std::cout << "Connecting to TWS at " << DEFAULT_HOST << ":" << DEFAULT_PORT << std::endl;
    std::cout << "Configured account: " << HARDCODED_ACCOUNT << std::endl;

#if defined(_WIN32)
    std::unique_ptr<IPlatformFactory> factory = CreateWin32Factory();
#elif defined(__APPLE__)
    std::unique_ptr<IPlatformFactory> factory = CreateMacOSFactory();
#else
    std::unique_ptr<IPlatformFactory> factory = CreateDefaultFactory();
#endif
    if (!factory) {
        std::cerr << "Failed to create platform factory" << std::endl;
        return 1;
    }

    std::cout << "Using platform: " << factory->getPlatformName() << std::endl;

    PlatformWindowInfo windowInfo;
    windowInfo.width = 1200;
    windowInfo.height = 900;
    windowInfo.title = "TWS Trading GUI";

    auto backend = factory->createBackend();
    if (!backend) {
        std::cerr << "Failed to create platform backend" << std::endl;
        return 1;
    }

    if (!backend->initialize(windowInfo)) {
        std::cerr << "Failed to initialize platform" << std::endl;
        return 1;
    }

    TradingWrapper wrapper;
    EReaderOSSignal osSignal(2000);
    EClientSocket client(&wrapper, &osSignal);
    wrapper.setClient(&client);

    bool twsConnected = client.eConnect(DEFAULT_HOST, DEFAULT_PORT, DEFAULT_CLIENT_ID);
    if (!twsConnected) {
        std::cerr << "Failed to connect to TWS" << std::endl;
        g_data.addMessage("Failed to connect to TWS");
    } else {
        std::cout << "Connected to TWS socket" << std::endl;
    }

    std::atomic<bool> readerRunning{twsConnected};
    EReader* reader = nullptr;
    std::thread readerThread;

    if (twsConnected) {
        reader = new EReader(&client, &osSignal);
        reader->start();
        readerThread = std::thread(readerLoop, &osSignal, reader, &client, &readerRunning);
    }

    ix::initNetSystem();
    ix::WebSocketServer wsServer(WEBSOCKET_PORT, WEBSOCKET_HOST);

    wsServer.setOnClientMessageCallback(
        [&client](std::shared_ptr<ix::ConnectionState> connectionState,
                  ix::WebSocket& webSocket,
                  const ix::WebSocketMessagePtr& msg) {
            (void)connectionState;

            if (msg->type == ix::WebSocketMessageType::Message) {
                handleWebSocketMessage(msg->str, webSocket, &client);
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
    );

    bool wsStarted = wsServer.listenAndStart();
    if (wsStarted) {
        g_data.wsServerRunning.store(true);
        g_data.addMessage("WebSocket server started on localhost port " + std::to_string(WEBSOCKET_PORT));
        std::cout << "[WebSocket server started on localhost port " << WEBSOCKET_PORT << "]" << std::endl;
    } else {
        g_data.addMessage("Failed to start WebSocket server on port " + std::to_string(WEBSOCKET_PORT));
        std::cerr << "Failed to start WebSocket server on port " << WEBSOCKET_PORT << std::endl;
    }

    ControllerState dsState;
#if defined(_WIN32)
    void* hwnd = backend->getNativeWindowHandle();
    void* hInstance = reinterpret_cast<void*>(GetModuleHandle(nullptr));
    controllerInitialize(dsState, hInstance, hwnd);
#else
    controllerInitialize(dsState, nullptr, nullptr);
#endif

    TradingPanelUiState uiState;
    ImVec4 clearColor = ImVec4(0.1f, 0.1f, 0.1f, 1.0f);

    while (backend->isRunning()) {
        if (!backend->pumpEvents()) {
            break;
        }

        backend->beginFrame();

        RenderTradingPanel(ImGui::GetIO(), &client, dsState, uiState);

        backend->endFrame(clearColor);
    }

    std::cout << "Shutting down..." << std::endl;

    if (g_data.wsServerRunning.load()) {
        std::cout << "Stopping WebSocket server..." << std::endl;
        wsServer.stop();
        g_data.wsServerRunning.store(false);
    }
    ix::uninitNetSystem();

    cancelActiveSubscription(&client);

    readerRunning.store(false);
    osSignal.issueSignal();

    if (readerThread.joinable()) {
        readerThread.join();
    }

    if (reader) {
        delete reader;
        reader = nullptr;
    }

    if (twsConnected) {
        client.eDisconnect();
    }

    controllerCleanup(dsState);

    backend->shutdown();

    std::cout << "Goodbye!" << std::endl;
    return 0;
}
