#pragma once

#include "app_platform.h"

#if defined(_WIN32)

#include <d3d11.h>
#include <dxgi.h>

#include <imgui.h>
#include <imgui_impl_win32.h>
#include <imgui_impl_dx11.h>

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>

class Win32PlatformBackend : public IPlatformBackend {
public:
    Win32PlatformBackend();
    ~Win32PlatformBackend() override;

    PlatformType getType() const override { return PlatformType::Windows_D3D11; }
    bool initialize(const PlatformWindowInfo& windowInfo) override;
    void shutdown() override;

    void* getNativeWindowHandle() const override { return reinterpret_cast<void*>(m_hwnd); }
    ImGuiIO& getImGuiIO() override { return ImGui::GetIO(); }
    bool pumpEvents() override;
    void beginFrame() override;
    void endFrame(ImVec4 clearColor) override;

    bool isRunning() const override { return m_running; }
    void requestShutdown() override;

    HWND getHwnd() const { return m_hwnd; }
    ID3D11Device* getDevice() const { return m_pd3dDevice; }
    ID3D11DeviceContext* getContext() const { return m_pd3dContext; }

private:
    bool createWindow(const PlatformWindowInfo& windowInfo);
    bool createDeviceD3D();
    void cleanupDeviceD3D();
    void createRenderTarget();
    void cleanupRenderTarget();

    static LRESULT WINAPI wndProc(HWND hWnd, UINT msg, WPARAM wParam, LPARAM lParam);

    HWND m_hwnd = nullptr;
    HINSTANCE m_hInstance = nullptr;
    ID3D11Device* m_pd3dDevice = nullptr;
    ID3D11DeviceContext* m_pd3dContext = nullptr;
    IDXGISwapChain* m_pSwapChain = nullptr;
    ID3D11RenderTargetView* m_pRenderTargetView = nullptr;
    bool m_running = false;
};

class Win32PlatformFactory : public IPlatformFactory {
public:
    PlatformType getPlatformType() const override { return PlatformType::Windows_D3D11; }
    std::unique_ptr<IPlatformBackend> createBackend() override;
    std::string getPlatformName() const override { return "Windows (Direct3D 11)"; }
};

PlatformType DetectPlatform();
std::unique_ptr<IPlatformFactory> CreateWin32Factory();

#endif
