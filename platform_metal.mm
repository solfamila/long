#include "platform_metal.h"

#if defined(__APPLE__)

#include <iostream>
#include <cassert>

#ifndef __OBJC__
#define __OBJC__
#endif

#import <Cocoa/Cocoa.h>
#import <Metal/Metal.h>
#import <QuartzCore/QuartzCore.h>

MacOSPlatformBackend::MacOSPlatformBackend()
    : m_window(nullptr)
    , m_metalLayer(nullptr)
    , m_mtlDevice(nullptr)
    , m_mtlCommandQueue(nullptr)
    , m_running(false)
{
}

MacOSPlatformBackend::~MacOSPlatformBackend() {
    shutdown();
}

bool MacOSPlatformBackend::initialize(const PlatformWindowInfo& windowInfo) {
    if (!glfwInit()) {
        std::cerr << "Failed to initialize GLFW" << std::endl;
        return false;
    }

    glfwWindowHint(GLFW_CLIENT_API, GLFW_NO_API);
    glfwWindowHint(GLFW_RESIZABLE, GLFW_TRUE);

    int width = windowInfo.width > 0 ? windowInfo.width : 1200;
    int height = windowInfo.height > 0 ? windowInfo.height : 900;

    m_window = glfwCreateWindow(width, height, 
        windowInfo.title.c_str(), nullptr, nullptr);
    
    if (!m_window) {
        std::cerr << "Failed to create GLFW window" << std::endl;
        glfwTerminate();
        return false;
    }

    id<MTLDevice> mtlDevice = MTLCreateSystemDefaultDevice();
    if (!mtlDevice) {
        std::cerr << "Metal is not supported on this device" << std::endl;
        glfwDestroyWindow(m_window);
        glfwTerminate();
        return false;
    }

    id<MTLCommandQueue> mtlCommandQueue = [mtlDevice newCommandQueue];
    if (!mtlCommandQueue) {
        std::cerr << "Failed to create Metal command queue" << std::endl;
        glfwDestroyWindow(m_window);
        glfwTerminate();
        return false;
    }

#if GLFW_VERSION_MAJOR >= 3 && GLFW_VERSION_MINOR >= 3
    NSWindow* nsWindow = glfwGetCocoaWindow(m_window);
    if (nsWindow) {
        CAMetalLayer* metalLayer = [CAMetalLayer layer];
        metalLayer.device = mtlDevice;
        metalLayer.pixelFormat = MTLPixelFormatBGRA8Unorm;
        metalLayer.framebufferOnly = YES;
        metalLayer.drawableSize = CGSizeMake(width, height);
        
        nsWindow.contentView.layer = metalLayer;
        nsWindow.contentView.wantsLayer = YES;
        m_metalLayer = (__bridge void*)metalLayer;
    }
#endif

    m_mtlDevice = (__bridge void*)mtlDevice;
    m_mtlCommandQueue = (__bridge void*)mtlCommandQueue;

    IMGUI_CHECKVERSION();
    ImGui::CreateContext();
    ImGuiIO& io = ImGui::GetIO();
    io.ConfigFlags |= ImGuiConfigFlags_NavEnableKeyboard;
    ImGui::StyleColorsDark();

    ImGui_ImplGlfw_InitForOther(m_window, true);
    ImGui_ImplMetal_Init(mtlDevice);

    m_running = true;
    return true;
}

void MacOSPlatformBackend::shutdown() {
    if (!m_running) return;

    ImGui_ImplMetal_Shutdown();
    ImGui_ImplGlfw_Shutdown();
    ImGui::DestroyContext();

    id<MTLCommandQueue> cmdQueue = (__bridge id<MTLCommandQueue>)m_mtlCommandQueue;
    (void)cmdQueue;
    
    m_mtlCommandQueue = nullptr;
    m_mtlDevice = nullptr;

    if (m_window) {
        glfwDestroyWindow(m_window);
        m_window = nullptr;
    }

    glfwTerminate();
    m_running = false;
}

bool MacOSPlatformBackend::pumpEvents() {
    glfwPollEvents();
    return isRunning();
}

void MacOSPlatformBackend::beginFrame() {
    MTLRenderPassDescriptor* passDesc = [[MTLRenderPassDescriptor alloc] init];
    ImGui_ImplMetal_NewFrame(passDesc);
    ImGui_ImplGlfw_NewFrame();
    ImGui::NewFrame();
}

void MacOSPlatformBackend::endFrame(ImVec4 clearColor) {
    ImGui::Render();

    if (!m_metalLayer) return;

    CAMetalLayer* metalLayer = (__bridge CAMetalLayer*)m_metalLayer;
    CAMetalDrawable* drawable = [metalLayer nextDrawable];
    if (!drawable) return;

    MTLRenderPassDescriptor* passDesc = [[MTLRenderPassDescriptor alloc] init];
    passDesc.colorAttachments[0].texture = drawable.texture;
    passDesc.colorAttachments[0].loadAction = MTLLoadActionClear;
    passDesc.colorAttachments[0].storeAction = MTLStoreActionStore;
    passDesc.colorAttachments[0].clearColor = MTLClearColorMake(
        clearColor.x, clearColor.y, clearColor.z, clearColor.w);

    id<MTLCommandQueue> cmdQueue = (__bridge id<MTLCommandQueue>)m_mtlCommandQueue;
    id<MTLCommandBuffer> cmdBuffer = [cmdQueue commandBuffer];
    id<MTLRenderCommandEncoder> encoder = [cmdBuffer renderCommandEncoderWithDescriptor:passDesc];
    
    ImGui_ImplMetal_RenderDrawData(ImGui::GetDrawData(), cmdBuffer, encoder);
    
    [encoder endEncoding];
    [cmdBuffer presentDrawable:drawable];
    [cmdBuffer commit];
}

void MacOSPlatformBackend::requestShutdown() {
    m_running = false;
    if (m_window) {
        glfwSetWindowShouldClose(m_window, GLFW_TRUE);
    }
}

std::unique_ptr<IPlatformBackend> MacOSPlatformFactory::createBackend() {
    return std::unique_ptr<IPlatformBackend>(new MacOSPlatformBackend());
}

PlatformType DetectPlatform() {
    return PlatformType::MacOS_Metal;
}

std::unique_ptr<IPlatformFactory> CreateMacOSFactory() {
    return std::unique_ptr<IPlatformFactory>(new MacOSPlatformFactory());
}

#if !defined(_WIN32) && !defined(__APPLE__)
std::unique_ptr<IPlatformFactory> CreateDefaultFactory() {
#if defined(__APPLE__)
    return CreateMacOSFactory();
#else
    return nullptr;
#endif
}
#endif

#endif
