#include "platform_metal.h"

#if defined(__APPLE__)

#include <iostream>
#include <cassert>

MacOSPlatformBackend::MacOSPlatformBackend()
    : m_window(nullptr)
    , m_metalLayer(nullptr)
    , m_mtlDevice(nil)
    , m_mtlCommandQueue(nil)
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

    m_mtlDevice = MTLCreateSystemDefaultDevice();
    if (!m_mtlDevice) {
        std::cerr << "Metal is not supported on this device" << std::endl;
        glfwDestroyWindow(m_window);
        glfwTerminate();
        return false;
    }

    m_mtlCommandQueue = [m_mtlDevice newCommandQueue];
    if (!m_mtlCommandQueue) {
        std::cerr << "Failed to create Metal command queue" << std::endl;
        m_mtlDevice = nil;
        glfwDestroyWindow(m_window);
        glfwTerminate();
        return false;
    }

    NSWindow* nsWindow = glfwGetCocoaWindow(m_window);
    if (nsWindow) {
        m_metalLayer = [CAMetalLayer layer];
        m_metalLayer.device = m_mtlDevice;
        m_metalLayer.pixelFormat = MTLPixelFormatBGRA8Unorm;
        m_metalLayer.framebufferOnly = YES;
        m_metalLayer.drawableSize = CGSizeMake(width, height);
        
        nsWindow.contentView.layer = m_metalLayer;
        nsWindow.contentView.wantsLayer = YES;
    }

    IMGUI_CHECKVERSION();
    ImGui::CreateContext();
    ImGuiIO& io = ImGui::GetIO();
    io.ConfigFlags |= ImGuiConfigFlags_NavEnableKeyboard;
    io.ConfigFlags |= ImGuiConfigFlags_DockingEnable;
    ImGui::StyleColorsDark();

    ImGui_ImplGlfw_InitForOther(m_window, true);
    ImGui_ImplMetal_Init(m_mtlDevice);

    m_running = true;
    return true;
}

void MacOSPlatformBackend::shutdown() {
    if (!m_running) return;

    ImGui_ImplMetal_Shutdown();
    ImGui_ImplGlfw_Shutdown();
    ImGui::DestroyContext();

    if (m_mtlCommandQueue) {
        m_mtlCommandQueue = nil;
    }
    if (m_mtlDevice) {
        m_mtlDevice = nil;
    }

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
    MTLRenderPassDescriptor* passDesc = nullptr;
    ImGui_ImplMetal_NewFrame(passDesc);
    ImGui_ImplGlfw_NewFrame();
    ImGui::NewFrame();
}

void MacOSPlatformBackend::endFrame(ImVec4 clearColor) {
    ImGui::Render();

    if (!m_metalLayer) return;

    CAMetalDrawable* drawable = [m_metalLayer nextDrawable];
    if (!drawable) return;

    MTLRenderPassDescriptor* passDesc = [MTLRenderPassDescriptor renderPassDescriptor];
    passDesc.colorAttachments[0].texture = drawable.texture;
    passDesc.colorAttachments[0].loadAction = MTLLoadActionClear;
    passDesc.colorAttachments[0].storeAction = MTLStoreActionStore;
    passDesc.colorAttachments[0].clearColor = MTLClearColorMake(
        clearColor.x, clearColor.y, clearColor.z, clearColor.w);

    id<MTLCommandBuffer> cmdBuffer = [m_mtlCommandQueue commandBuffer];
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
    return std::make_unique<MacOSPlatformBackend>();
}

PlatformType DetectPlatform() {
    return PlatformType::MacOS_Metal;
}

std::unique_ptr<IPlatformFactory> CreateMacOSFactory() {
    return std::make_unique<MacOSPlatformFactory>();
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
