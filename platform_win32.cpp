#include "platform_win32.h"

#if defined(_WIN32)

#include <iostream>

extern IMGUI_IMPL_API LRESULT ImGui_ImplWin32_WndProcHandler(HWND hWnd, UINT msg, WPARAM wParam, LPARAM lParam);

Win32PlatformBackend::Win32PlatformBackend()
    : m_hwnd(nullptr)
    , m_hInstance(nullptr)
    , m_pd3dDevice(nullptr)
    , m_pd3dContext(nullptr)
    , m_pSwapChain(nullptr)
    , m_pRenderTargetView(nullptr)
    , m_running(false)
{
}

Win32PlatformBackend::~Win32PlatformBackend() {
    shutdown();
}

bool Win32PlatformBackend::initialize(const PlatformWindowInfo& windowInfo) {
    if (!createWindow(windowInfo)) {
        return false;
    }

    if (!createDeviceD3D()) {
        std::cerr << "Failed to create D3D11 device" << std::endl;
        cleanupDeviceD3D();
        DestroyWindow(m_hwnd);
        UnregisterClassW(L"TwsTradingGui", m_hInstance);
        return false;
    }

    ShowWindow(m_hwnd, SW_SHOWDEFAULT);
    UpdateWindow(m_hwnd);

    IMGUI_CHECKVERSION();
    ImGui::CreateContext();
    ImGuiIO& io = ImGui::GetIO();
    io.ConfigFlags |= ImGuiConfigFlags_NavEnableKeyboard;
    ImGui::StyleColorsDark();

    ImGui_ImplWin32_Init(m_hwnd);
    ImGui_ImplDX11_Init(m_pd3dDevice, m_pd3dContext);

    m_running = true;
    return true;
}

void Win32PlatformBackend::shutdown() {
    if (!m_running) return;

    ImGui_ImplDX11_Shutdown();
    ImGui_ImplWin32_Shutdown();
    ImGui::DestroyContext();

    cleanupDeviceD3D();

    if (m_hwnd) {
        DestroyWindow(m_hwnd);
        m_hwnd = nullptr;
    }

    if (m_hInstance) {
        UnregisterClassW(L"TwsTradingGui", m_hInstance);
        m_hInstance = nullptr;
    }

    m_running = false;
}

bool Win32PlatformBackend::createWindow(const PlatformWindowInfo& windowInfo) {
    m_hInstance = GetModuleHandle(nullptr);

    WNDCLASSEXW wc = {};
    wc.cbSize = sizeof(WNDCLASSEXW);
    wc.style = CS_HREDRAW | CS_VREDRAW;
    wc.lpfnWndProc = wndProc;
    wc.hInstance = m_hInstance;
    wc.hCursor = LoadCursor(nullptr, IDC_ARROW);
    wc.lpszClassName = L"TwsTradingGui";

    if (!RegisterClassExW(&wc)) {
        std::cerr << "Failed to register window class" << std::endl;
        return false;
    }

    m_hwnd = CreateWindowExW(
        0, L"TwsTradingGui",
        windowInfo.title.empty() ? L"TWS Trading GUI" : 
            std::wstring(windowInfo.title.begin(), windowInfo.title.end()).c_str(),
        WS_OVERLAPPEDWINDOW,
        CW_USEDEFAULT, CW_USEDEFAULT, 
        windowInfo.width > 0 ? windowInfo.width : 1200,
        windowInfo.height > 0 ? windowInfo.height : 900,
        nullptr, nullptr, m_hInstance, this
    );

    if (!m_hwnd) {
        std::cerr << "Failed to create window" << std::endl;
        return false;
    }

    return true;
}

bool Win32PlatformBackend::createDeviceD3D() {
    DXGI_SWAP_CHAIN_DESC sd = {};
    sd.BufferCount = 2;
    sd.BufferDesc.Width = 0;
    sd.BufferDesc.Height = 0;
    sd.BufferDesc.Format = DXGI_FORMAT_R8G8B8A8_UNORM;
    sd.BufferDesc.RefreshRate.Numerator = 60;
    sd.BufferDesc.RefreshRate.Denominator = 1;
    sd.Flags = 0;
    sd.BufferUsage = DXGI_USAGE_RENDER_TARGET_OUTPUT;
    sd.OutputWindow = m_hwnd;
    sd.SampleDesc.Count = 1;
    sd.SampleDesc.Quality = 0;
    sd.Windowed = TRUE;
    sd.SwapEffect = DXGI_SWAP_EFFECT_DISCARD;

    D3D_FEATURE_LEVEL featureLevel;
    const D3D_FEATURE_LEVEL featureLevels[] = {
        D3D_FEATURE_LEVEL_11_1,
        D3D_FEATURE_LEVEL_11_0,
        D3D_FEATURE_LEVEL_10_1,
        D3D_FEATURE_LEVEL_10_0
    };

    HRESULT hr = D3D11CreateDeviceAndSwapChain(
        nullptr, D3D_DRIVER_TYPE_HARDWARE, nullptr, 0,
        featureLevels, static_cast<UINT>(std::size(featureLevels)),
        D3D11_SDK_VERSION, &sd,
        &m_pSwapChain, &m_pd3dDevice, &featureLevel, &m_pd3dContext
    );

    if (FAILED(hr)) {
        hr = D3D11CreateDeviceAndSwapChain(
            nullptr, D3D_DRIVER_TYPE_WARP, nullptr, 0,
            featureLevels, static_cast<UINT>(std::size(featureLevels)),
            D3D11_SDK_VERSION, &sd,
            &m_pSwapChain, &m_pd3dDevice, &featureLevel, &m_pd3dContext
        );
        if (FAILED(hr)) return false;
    }

    createRenderTarget();
    return true;
}

void Win32PlatformBackend::cleanupDeviceD3D() {
    if (m_pRenderTargetView) { m_pRenderTargetView->Release(); m_pRenderTargetView = nullptr; }
    if (m_pSwapChain) { m_pSwapChain->Release(); m_pSwapChain = nullptr; }
    if (m_pd3dContext) { m_pd3dContext->Release(); m_pd3dContext = nullptr; }
    if (m_pd3dDevice) { m_pd3dDevice->Release(); m_pd3dDevice = nullptr; }
}

void Win32PlatformBackend::createRenderTarget() {
    ID3D11Texture2D* pBackBuffer = nullptr;
    m_pSwapChain->GetBuffer(0, __uuidof(ID3D11Texture2D), (void**)&pBackBuffer);
    if (pBackBuffer) {
        m_pd3dDevice->CreateRenderTargetView(pBackBuffer, nullptr, &m_pRenderTargetView);
        pBackBuffer->Release();
    }
}

void Win32PlatformBackend::cleanupRenderTarget() {
    if (m_pRenderTargetView) {
        m_pRenderTargetView->Release();
        m_pRenderTargetView = nullptr;
    }
}

LRESULT WINAPI Win32PlatformBackend::wndProc(HWND hWnd, UINT msg, WPARAM wParam, LPARAM lParam) {
    if (ImGui_ImplWin32_WndProcHandler(hWnd, msg, wParam, lParam)) {
        return true;
    }

    switch (msg) {
        case WM_SIZE:
            if (Win32PlatformBackend* self = reinterpret_cast<Win32PlatformBackend*>(GetWindowLongPtr(hWnd, GWLP_USERDATA))) {
                if (self->m_pd3dDevice && wParam != SIZE_MINIMIZED) {
                    self->cleanupRenderTarget();
                    self->m_pSwapChain->ResizeBuffers(0, LOWORD(lParam), HIWORD(lParam), DXGI_FORMAT_UNKNOWN, 0);
                    self->createRenderTarget();
                }
            }
            return 0;
        case WM_DESTROY:
            PostQuitMessage(0);
            return 0;
        default:
            return DefWindowProc(hWnd, msg, wParam, lParam);
    }
}

bool Win32PlatformBackend::pumpEvents() {
    MSG msg = {};
    while (PeekMessage(&msg, nullptr, 0, 0, PM_REMOVE)) {
        TranslateMessage(&msg);
        DispatchMessage(&msg);
        if (msg.message == WM_QUIT) {
            m_running = false;
            return false;
        }
    }
    return m_running;
}

void Win32PlatformBackend::beginFrame() {
    ImGui_ImplDX11_NewFrame();
    ImGui_ImplWin32_NewFrame();
    ImGui::NewFrame();
}

void Win32PlatformBackend::endFrame(ImVec4 clearColor) {
    ImGui::Render();
    const float clearColorArray[4] = { clearColor.x, clearColor.y, clearColor.z, clearColor.w };
    m_pd3dContext->OMSetRenderTargets(1, &m_pRenderTargetView, nullptr);
    m_pd3dContext->ClearRenderTargetView(m_pRenderTargetView, clearColorArray);
    ImGui_ImplDX11_RenderDrawData(ImGui::GetDrawData());
    m_pSwapChain->Present(1, 0);
}

void Win32PlatformBackend::requestShutdown() {
    m_running = false;
    PostMessage(m_hwnd, WM_CLOSE, 0, 0);
}

std::unique_ptr<IPlatformBackend> Win32PlatformFactory::createBackend() {
    return std::make_unique<Win32PlatformBackend>();
}

PlatformType DetectPlatform() {
    return PlatformType::Windows_D3D11;
}

std::unique_ptr<IPlatformFactory> CreateWin32Factory() {
    return std::make_unique<Win32PlatformFactory>();
}

#endif
