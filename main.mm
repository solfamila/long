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

enum class ThermalState {
    Nominal,
    Fair,
    Serious,
    Critical
};

enum class AppActivityState {
    Active,
    Inactive
};

@interface AppViewController : NSViewController <MTKViewDelegate, NSWindowDelegate>
@property (nonatomic, strong) MTKView* mtkView;
@property (nonatomic, strong) id<MTLDevice> device;
@property (nonatomic, strong) id<MTLCommandQueue> commandQueue;
@property (nonatomic, assign) ThermalState thermalState;
@property (nonatomic, assign) AppActivityState activityState;
@property (nonatomic, assign) BOOL needsRedraw;
@end

@implementation AppViewController {
    TradingRuntime* _tradingRuntime;
    ControllerState _controllerState;
    TradingPanelUiState _uiState;
    bool _applicationShouldQuit;
    bool _controllerInitialized;
    id _inputEventMonitor;
}

- (instancetype)initWithNibName:(NSString*)nibNameOrNil bundle:(NSBundle*)nibBundleOrNil {
    self = [super initWithNibName:nibNameOrNil bundle:nibBundleOrNil];
    
    _device = MTLCreateSystemDefaultDevice();
    _commandQueue = [_device newCommandQueue];
    _applicationShouldQuit = false;
    _controllerInitialized = false;
    _inputEventMonitor = nil;
    _tradingRuntime = new TradingRuntime();
    _thermalState = [self thermalStateFromProcessInfo];
    _activityState = AppActivityState::Active;
    _needsRedraw = YES;
    
    if (!self.device) {
        NSLog(@"Metal is not supported");
        abort();
    }
    
    [self registerForNotifications];
    
    IMGUI_CHECKVERSION();
    ImGui::CreateContext();
    ImGuiIO& io = ImGui::GetIO();
    io.ConfigFlags |= ImGuiConfigFlags_NavEnableKeyboard;
    ImGui::StyleColorsDark();
    
    ImGui_ImplMetal_Init(_device);
    
    return self;
}

- (ThermalState)thermalStateFromProcessInfo {
    NSProcessInfoThermalState processThermalState = [[NSProcessInfo processInfo] thermalState];
    switch (processThermalState) {
        case NSProcessInfoThermalStateNominal:
            return ThermalState::Nominal;
        case NSProcessInfoThermalStateFair:
            return ThermalState::Fair;
        case NSProcessInfoThermalStateSerious:
            return ThermalState::Serious;
        case NSProcessInfoThermalStateCritical:
            return ThermalState::Critical;
        default:
            return ThermalState::Nominal;
    }
}

- (void)registerForNotifications {
    [[NSNotificationCenter defaultCenter] addObserver:self
                                             selector:@selector(applicationDidBecomeActive:)
                                                 name:NSApplicationDidBecomeActiveNotification
                                               object:nil];
    
    [[NSNotificationCenter defaultCenter] addObserver:self
                                             selector:@selector(applicationWillResignActive:)
                                                 name:NSApplicationWillResignActiveNotification
                                               object:nil];
    
    [[NSNotificationCenter defaultCenter] addObserver:self
                                             selector:@selector(thermalStateDidChange:)
                                                 name:NSProcessInfoThermalStateDidChangeNotification
                                               object:nil];

    __weak AppViewController* weakSelf = self;
    _inputEventMonitor = [NSEvent addLocalMonitorForEventsMatchingMask:
        (NSEventMaskLeftMouseDown |
         NSEventMaskLeftMouseUp |
         NSEventMaskRightMouseDown |
         NSEventMaskRightMouseUp |
         NSEventMaskOtherMouseDown |
         NSEventMaskOtherMouseUp |
         NSEventMaskMouseMoved |
         NSEventMaskLeftMouseDragged |
         NSEventMaskRightMouseDragged |
         NSEventMaskOtherMouseDragged |
         NSEventMaskScrollWheel |
         NSEventMaskKeyDown |
         NSEventMaskKeyUp |
         NSEventMaskFlagsChanged)
        handler:^NSEvent* (NSEvent* event) {
            AppViewController* strongSelf = weakSelf;
            if (strongSelf != nil) {
                [strongSelf requestRedraw];
            }
            return event;
        }];
}

- (void)applicationDidBecomeActive:(NSNotification*)notification {
    (void)notification;
    _activityState = AppActivityState::Active;
    [self updateFrameRateForConditions];
    [self requestRedraw];
    NSLog(@"App became active - full frame rate");
}

- (void)applicationWillResignActive:(NSNotification*)notification {
    (void)notification;
    _activityState = AppActivityState::Inactive;
    [self updateFrameRateForConditions];
    [self requestRedraw];
    NSLog(@"App became inactive - reduced frame rate");
}

- (void)thermalStateDidChange:(NSNotification*)notification {
    (void)notification;
    _thermalState = [self thermalStateFromProcessInfo];
    [self updateFrameRateForConditions];
    NSLog(@"Thermal state changed: %ld", (long)_thermalState);
    [self requestRedraw];
}

- (double)targetFrameInterval {
    if (_activityState == AppActivityState::Inactive) {
        return 1.0 / 2.0;
    }
    
    switch (_thermalState) {
        case ThermalState::Nominal:
            return 1.0 / 60.0;
        case ThermalState::Fair:
            return 1.0 / 30.0;
        case ThermalState::Serious:
            return 1.0 / 15.0;
        case ThermalState::Critical:
            return 1.0 / 5.0;
        default:
            return 1.0 / 60.0;
    }
}

- (BOOL)shouldDraw {
    return _needsRedraw;
}

- (void)requestRedraw {
    _needsRedraw = YES;
    if (self.mtkView != nil) {
        [self.mtkView setNeedsDisplay:YES];
    }
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
    self.mtkView.paused = YES;
    self.mtkView.framebufferOnly = NO;
    self.mtkView.preferredFramesPerSecond = 60;
    
    ImGui_ImplOSX_Init(self.view);
    [NSApp activateIgnoringOtherApps:YES];
    
    _controllerInitialized = controllerInitialize(_controllerState, nullptr, nullptr);
    if (!_controllerInitialized) {
        std::cerr << "Failed to initialize controller manager" << std::endl;
        g_data.addMessage("Failed to initialize controller manager");
    }

    __weak AppViewController* weakSelf = self;
    setUiInvalidationCallback([weakSelf]() {
        dispatch_async(dispatch_get_main_queue(), ^{
            AppViewController* strongSelf = weakSelf;
            if (strongSelf != nil) {
                [strongSelf requestRedraw];
            }
        });
    });

    [self updateFrameRateForConditions];
    [self requestRedraw];
    
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

- (void)updateFrameRateForConditions {
    double interval = [self targetFrameInterval];
    int fps = (int)(1.0 / interval);
    if (self.mtkView.preferredFramesPerSecond != fps) {
        self.mtkView.preferredFramesPerSecond = fps;
        NSLog(@"Frame rate set to %d fps (thermal: %ld, active: %d)", fps, (long)_thermalState, _activityState == AppActivityState::Active);
    }
}

- (void)drawInMTKView:(MTKView*)view {
    if (_applicationShouldQuit) {
        [self shutdown];
        [NSApp terminate:nil];
        return;
    }
    
    if (![self shouldDraw]) {
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
    
    RenderTradingPanel(ImGui::GetIO(), _tradingRuntime, _controllerState, _uiState);
    
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
    
    _needsRedraw = NO;
}

- (void)mtkView:(MTKView*)view drawableSizeWillChange:(CGSize)size {
    (void)size;
    [self requestRedraw];
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
    
    [[NSNotificationCenter defaultCenter] removeObserver:self];
    setUiInvalidationCallback({});

    if (_inputEventMonitor != nil) {
        [NSEvent removeMonitor:_inputEventMonitor];
        _inputEventMonitor = nil;
    }

    if (_controllerInitialized) {
        controllerCleanup(_controllerState);
        _controllerInitialized = false;
    }
    
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
