#import <Cocoa/Cocoa.h>
#import <Metal/Metal.h>
#import <MetalKit/MetalKit.h>

#include <iostream>
#include <memory>

#include "imgui.h"
#include "imgui_impl_metal.h"
#include "imgui_impl_osx.h"

#include "app_shared.h"
#include "app_platform.h"
#include "trading_runtime.h"
#include "websocket_handlers.h"
#include "controller.h"
#include "trading_panel.h"

@interface AppViewController : NSViewController <MTKViewDelegate, NSWindowDelegate>
@property (nonatomic, strong) MTKView* mtkView;
@property (nonatomic, strong) id<MTLDevice> device;
@property (nonatomic, strong) id<MTLCommandQueue> commandQueue;
@end

@implementation AppViewController {
    TradingRuntime* _tradingRuntime;
    bool _applicationShouldQuit;
}

- (instancetype)initWithNibName:(NSString*)nibNameOrNil bundle:(NSBundle*)nibBundleOrNil {
    self = [super initWithNibName:nibNameOrNil bundle:nibBundleOrNil];
    
    _device = MTLCreateSystemDefaultDevice();
    _commandQueue = [_device newCommandQueue];
    _applicationShouldQuit = false;
    _tradingRuntime = new TradingRuntime();
    
    if (!self.device) {
        NSLog(@"Metal is not supported");
        abort();
    }
    
    IMGUI_CHECKVERSION();
    ImGui::CreateContext();
    ImGuiIO& io = ImGui::GetIO();
    io.ConfigFlags |= ImGuiConfigFlags_NavEnableKeyboard;
    ImGui::StyleColorsDark();
    
    ImGui_ImplMetal_Init(_device);
    
    return self;
}

- (void)loadView {
    self.view = [[MTKView alloc] initWithFrame:CGRectMake(0, 0, 1200, 900)];
}

- (void)viewDidLoad {
    [super viewDidLoad];
    
    self.mtkView = (MTKView*)self.view;
    self.mtkView.device = self.device;
    self.mtkView.delegate = self;
    self.mtkView.enableSetNeedsDisplay = YES;
    self.mtkView.framebufferOnly = NO;
    
    ImGui_ImplOSX_Init(self.view);
    [NSApp activateIgnoringOtherApps:YES];
    
    TradingRuntimeConfig config;
    config.host = DEFAULT_HOST;
    config.port = DEFAULT_PORT;
    config.clientId = DEFAULT_CLIENT_ID;
    config.account = HARDCODED_ACCOUNT;
    config.wsHost = WEBSOCKET_HOST;
    config.wsPort = WEBSOCKET_PORT;
    
    bool started = _tradingRuntime->start(config);
    if (!started) {
        std::cerr << "Failed to start trading runtime" << std::endl;
        g_data.addMessage("Failed to start trading runtime");
    }
}

- (void)drawInMTKView:(MTKView*)view {
    if (_applicationShouldQuit) {
        [self shutdown];
        [NSApp terminate:nil];
        return;
    }
    
    ImGuiIO& io = ImGui::GetIO();
    io.DisplaySize.x = view.bounds.size.width;
    io.DisplaySize.y = view.bounds.size.height;
    
    CGFloat framebufferScale = view.window.screen.backingScaleFactor ?: NSScreen.mainScreen.backingScaleFactor;
    io.DisplayFramebufferScale = ImVec2(framebufferScale, framebufferScale);
    
    id<MTLCommandBuffer> commandBuffer = [self.commandQueue commandBuffer];
    
    MTLRenderPassDescriptor* renderPassDescriptor = view.currentRenderPassDescriptor;
    if (renderPassDescriptor == nil) {
        [commandBuffer commit];
        return;
    }
    
    ImGui_ImplMetal_NewFrame(renderPassDescriptor);
    ImGui_ImplOSX_NewFrame(view);
    ImGui::NewFrame();
    
    ImVec4 clearColor = ImVec4(0.1f, 0.1f, 0.1f, 1.0f);
    
    ControllerState dsState;
    TradingPanelUiState uiState;
    RenderTradingPanel(ImGui::GetIO(), _tradingRuntime->getClient(), dsState, uiState);
    
    ImGui::Render();
    
    renderPassDescriptor.colorAttachments[0].clearColor = MTLClearColorMake(
        clearColor.x * clearColor.w,
        clearColor.y * clearColor.w,
        clearColor.z * clearColor.w,
        clearColor.w
    );
    
    id<MTLRenderCommandEncoder> renderEncoder = [commandBuffer renderCommandEncoderWithDescriptor:renderPassDescriptor];
    ImGui_ImplMetal_RenderDrawData(ImGui::GetDrawData(), commandBuffer, renderEncoder);
    [renderEncoder endEncoding];
    
    [commandBuffer presentDrawable:view.currentDrawable];
    [commandBuffer commit];
    
    [self.mtkView setNeedsDisplay:YES];
}

- (void)mtkView:(MTKView*)view drawableSizeWillChange:(CGSize)size {
}

- (void)viewWillAppear {
    [super viewWillAppear];
    self.view.window.delegate = self;
}

- (void)windowWillClose:(NSNotification*)notification {
    _applicationShouldQuit = true;
}

- (void)shutdown {
    std::cout << "Shutting down..." << std::endl;
    
    _tradingRuntime->stop();
    delete _tradingRuntime;
    _tradingRuntime = nullptr;
    
    ImGui_ImplMetal_Shutdown();
    ImGui_ImplOSX_Shutdown();
    ImGui::DestroyContext();
    
    std::cout << "Goodbye!" << std::endl;
}

@end

@interface AppDelegate : NSObject <NSApplicationDelegate>
@property (nonatomic, strong) NSWindow* window;
@end

@implementation AppDelegate

- (BOOL)applicationShouldTerminateAfterLastWindowClosed:(NSApplication*)sender {
    return YES;
}

- (instancetype)init {
    if (self = [super init]) {
        AppViewController* rootViewController = [[AppViewController alloc] initWithNibName:nil bundle:nil];
        
        self.window = [[NSWindow alloc] initWithContentRect:NSZeroRect
                                                  styleMask:NSWindowStyleMaskTitled | NSWindowStyleMaskClosable | NSWindowStyleMaskResizable | NSWindowStyleMaskMiniaturizable
                                                    backing:NSBackingStoreBuffered
                                                      defer:NO];
        
        self.window.contentViewController = rootViewController;
        self.window.title = @"TWS Trading GUI";
        
        [self.window center];
        [self.window makeKeyAndOrderFront:self];
    }
    return self;
}

@end

int main(int, const char**) {
    @autoreleasepool {
        [NSApplication sharedApplication];
        [NSApp setActivationPolicy:NSApplicationActivationPolicyRegular];
        
        AppDelegate* appDelegate = [[AppDelegate alloc] init];
        [NSApp setDelegate:appDelegate];
        
        [NSApp activateIgnoringOtherApps:YES];
        [NSApp run];
    }
    return 0;
}
