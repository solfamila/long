#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>

struct ImGuiIO;
struct ImVec2;
struct ImVec4;

enum class PlatformType {
    Windows_D3D11,
    MacOS_Metal
};

struct PlatformWindowInfo {
    void* nativeWindowHandle = nullptr;
    int width = 1200;
    int height = 900;
    std::string title = "TWS Trading GUI";
};

struct RenderCallbacks {
    std::function<void()> onNewFrame;
    std::function<void()> onRender;
    std::function<void(ImVec4 clearColor)> onPresent;
};

class IPlatformBackend {
public:
    virtual ~IPlatformBackend() = default;
    
    virtual PlatformType getType() const = 0;
    virtual bool initialize(const PlatformWindowInfo& windowInfo) = 0;
    virtual void shutdown() = 0;
    
    virtual void* getNativeWindowHandle() const = 0;
    virtual ImGuiIO& getImGuiIO() = 0;
    virtual bool pumpEvents() = 0;
    virtual void beginFrame() = 0;
    virtual void endFrame(ImVec4 clearColor) = 0;
    
    virtual bool isRunning() const = 0;
    virtual void requestShutdown() = 0;
};

class IPlatformFactory {
public:
    virtual ~IPlatformFactory() = default;
    virtual PlatformType getPlatformType() const = 0;
    virtual std::unique_ptr<IPlatformBackend> createBackend() = 0;
    virtual std::string getPlatformName() const = 0;
};

#if defined(_WIN32)
PlatformType DetectPlatform();
std::unique_ptr<IPlatformFactory> CreateWin32Factory();
#elif defined(__APPLE__)
PlatformType DetectPlatform();
std::unique_ptr<IPlatformFactory> CreateMacOSFactory();
#else
PlatformType DetectPlatform();
std::unique_ptr<IPlatformFactory> CreateDefaultFactory();
#endif
