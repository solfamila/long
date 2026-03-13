#include "app_shared.h"
#include "app_platform.h"
#if defined(_WIN32)
#include "platform_win32.h"
#endif
#include "controller.h"
#include "trading_panel.h"
#include "trading_runtime.h"

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

    TradingRuntime runtime;
    TradingRuntimeConfig config;
    config.host = DEFAULT_HOST;
    config.port = DEFAULT_PORT;
    config.clientId = DEFAULT_CLIENT_ID;
    config.account = HARDCODED_ACCOUNT;
    config.wsHost = WEBSOCKET_HOST;
    config.wsPort = WEBSOCKET_PORT;
    runtime.start(config);

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

        RenderTradingPanel(ImGui::GetIO(), &runtime, dsState, uiState);

        backend->endFrame(clearColor);
    }

    std::cout << "Shutting down..." << std::endl;

    controllerCleanup(dsState);
    runtime.stop();

    backend->shutdown();

    std::cout << "Goodbye!" << std::endl;
    return 0;
}
