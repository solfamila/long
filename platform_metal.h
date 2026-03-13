#pragma once

#include "app_platform.h"

#if defined(__APPLE__)

#include <GLFW/glfw3.h>
#include <Metal/Metal.h>
#include <imgui.h>
#include <imgui_impl_glfw.h>
#include <imgui_impl_metal.h>

class MacOSPlatformBackend : public IPlatformBackend {
public:
    MacOSPlatformBackend();
    ~MacOSPlatformBackend() override;

    PlatformType getType() const override { return PlatformType::MacOS_Metal; }
    bool initialize(const PlatformWindowInfo& windowInfo) override;
    void shutdown() override;

    void* getNativeWindowHandle() const override { return reinterpret_cast<void*>(m_window); }
    ImGuiIO& getImGuiIO() override { return ImGui::GetIO(); }
    bool pumpEvents() override;
    void beginFrame() override;
    void endFrame(ImVec4 clearColor) override;

    bool isRunning() const override { return m_running && !glfwWindowShouldClose(m_window); }
    void requestShutdown() override;

    GLFWwindow* getWindow() const { return m_window; }
    CAMetalLayer* getMetalLayer() const { return m_metalLayer; }

private:
    GLFWwindow* m_window = nullptr;
    CAMetalLayer* m_metalLayer = nullptr;
    id<MTLDevice> m_mtlDevice = nil;
    id<MTLCommandQueue> m_mtlCommandQueue = nil;
    bool m_running = false;
};

class MacOSPlatformFactory : public IPlatformFactory {
public:
    PlatformType getPlatformType() const override { return PlatformType::MacOS_Metal; }
    std::unique_ptr<IPlatformBackend> createBackend() override;
    std::string getPlatformName() const override { return "macOS (Metal)"; }
};

PlatformType DetectPlatform();
std::unique_ptr<IPlatformFactory> CreateMacOSFactory();

#endif
