#import <AppKit/AppKit.h>

#include "app_shared.h"
#include "trading_export_panels.h"
#include "trading_actions.h"
#include "trading_refresh_scheduler.h"
#include "trading_runtime.h"
#include "trading_runtime_host.h"
#include "trading_settings_sheet.h"
#include "trading_ui_format.h"
#include "trading_view_model.h"
#include "trading_wrapper.h"

namespace {

NSString* const kWebSocketTokenDefaultsKey = @"websocketToken";

NSString* ToNSString(const std::string& value) {
    return [NSString stringWithUTF8String:value.c_str()];
}

std::string ToStdString(NSString* value) {
    if (value == nil) {
        return {};
    }
    return std::string([value UTF8String]);
}

NSTextField* MakeLabel(NSString* text, NSFont* font, NSColor* color) {
    NSTextField* label = [NSTextField labelWithString:text ?: @""];
    label.font = font;
    label.textColor = color ?: [NSColor labelColor];
    label.lineBreakMode = NSLineBreakByTruncatingTail;
    label.maximumNumberOfLines = 1;
    return label;
}

NSTextField* MakeValueLabel() {
    return MakeLabel(@"--", [NSFont monospacedSystemFontOfSize:13.0 weight:NSFontWeightMedium], [NSColor labelColor]);
}

NSColor* AppBackgroundColor() {
    return [NSColor colorWithCalibratedWhite:0.94 alpha:1.0];
}

NSColor* PanelBackgroundColor() {
    return [NSColor colorWithCalibratedWhite:0.985 alpha:1.0];
}

NSColor* PanelBorderColor() {
    return [NSColor colorWithCalibratedWhite:0.82 alpha:1.0];
}

NSTextField* MakeWrappingLabel(NSString* text, NSFont* font, NSColor* color) {
    NSTextField* label = [NSTextField wrappingLabelWithString:text ?: @""];
    label.font = font;
    label.textColor = color ?: [NSColor labelColor];
    return label;
}

NSTextField* MakeInputField(NSString* text, CGFloat width, id target, SEL action, id delegate) {
    NSTextField* field = [[NSTextField alloc] initWithFrame:NSZeroRect];
    field.stringValue = text ?: @"";
    field.font = [NSFont monospacedSystemFontOfSize:13.0 weight:NSFontWeightRegular];
    field.bordered = YES;
    field.bezelStyle = NSTextFieldRoundedBezel;
    field.backgroundColor = [NSColor whiteColor];
    field.textColor = [NSColor labelColor];
    field.delegate = delegate;
    field.target = target;
    field.action = action;
    [field.widthAnchor constraintEqualToConstant:width].active = YES;
    return field;
}

NSButton* MakeButton(NSString* title, id target, SEL action) {
    NSButton* button = [NSButton buttonWithTitle:title target:target action:action];
    button.bezelStyle = NSBezelStyleRounded;
    button.bordered = YES;
    button.buttonType = NSButtonTypeMomentaryPushIn;
    button.controlSize = NSControlSizeLarge;
    button.font = [NSFont systemFontOfSize:13.0 weight:NSFontWeightSemibold];
    button.translatesAutoresizingMaskIntoConstraints = NO;
    [button.heightAnchor constraintEqualToConstant:32.0].active = YES;
    return button;
}

void SetButtonEnabledTint(NSButton* button, bool enabled, NSColor* enabledColor, NSColor* disabledColor) {
    button.enabled = enabled;
    button.bezelColor = enabled ? enabledColor : disabledColor;
    button.contentTintColor = enabled ? [NSColor whiteColor] : [NSColor secondaryLabelColor];
}

void SetActionButtonAvailability(NSButton* button,
                                 bool available,
                                 NSColor* activeColor,
                                 NSString* unavailableReason) {
    button.enabled = YES;
    button.bezelColor = available ? activeColor : [NSColor colorWithCalibratedWhite:0.55 alpha:1.0];
    button.contentTintColor = [NSColor whiteColor];
    button.toolTip = available ? nil : unavailableReason;
}

NSInteger ControllerArmModePopupIndex(ControllerArmMode mode) {
    return mode == ControllerArmMode::Manual ? 1 : 0;
}

ControllerArmMode ControllerArmModeFromPopupIndex(NSInteger index) {
    return index == 1 ? ControllerArmMode::Manual : ControllerArmMode::OneShot;
}

NSString* ControllerArmSafetyText(const RiskControlsSnapshot& risk) {
    if (!risk.controllerArmed) {
        return @"controller disarmed";
    }
    return risk.controllerArmMode == ControllerArmMode::Manual
        ? @"controller armed (manual)"
        : @"controller armed (1-shot)";
}

void StyleTintedButton(NSButton* button, NSColor* bezelColor, NSColor* tintColor) {
    button.bezelColor = bezelColor;
    button.contentTintColor = tintColor;
}

NSStackView* MakeRowStack() {
    NSStackView* stack = [[NSStackView alloc] initWithFrame:NSZeroRect];
    stack.orientation = NSUserInterfaceLayoutOrientationHorizontal;
    stack.alignment = NSLayoutAttributeCenterY;
    stack.spacing = 10.0;
    stack.translatesAutoresizingMaskIntoConstraints = NO;
    return stack;
}

NSStackView* MakeColumnStack() {
    NSStackView* stack = [[NSStackView alloc] initWithFrame:NSZeroRect];
    stack.orientation = NSUserInterfaceLayoutOrientationVertical;
    stack.alignment = NSLayoutAttributeLeading;
    stack.spacing = 12.0;
    stack.translatesAutoresizingMaskIntoConstraints = NO;
    return stack;
}

NSView* MakeFlexibleSpacer() {
    NSView* spacer = [[NSView alloc] initWithFrame:NSZeroRect];
    spacer.translatesAutoresizingMaskIntoConstraints = NO;
    [spacer setContentHuggingPriority:NSLayoutPriorityDefaultLow forOrientation:NSLayoutConstraintOrientationHorizontal];
    [spacer setContentCompressionResistancePriority:NSLayoutPriorityDefaultLow forOrientation:NSLayoutConstraintOrientationHorizontal];
    return spacer;
}

NSTextView* MakeReadOnlyTextView() {
    NSTextView* textView = [[NSTextView alloc] initWithFrame:NSZeroRect];
    textView.editable = NO;
    textView.selectable = YES;
    textView.richText = NO;
    textView.automaticQuoteSubstitutionEnabled = NO;
    textView.automaticDashSubstitutionEnabled = NO;
    textView.automaticTextReplacementEnabled = NO;
    textView.font = [NSFont monospacedSystemFontOfSize:12.0 weight:NSFontWeightRegular];
    textView.textColor = [NSColor labelColor];
    textView.backgroundColor = [NSColor whiteColor];
    return textView;
}

NSScrollView* MakeScrollView(NSView* documentView, CGFloat minHeight) {
    NSScrollView* scrollView = [[NSScrollView alloc] initWithFrame:NSZeroRect];
    scrollView.translatesAutoresizingMaskIntoConstraints = NO;
    scrollView.hasVerticalScroller = YES;
    scrollView.hasHorizontalScroller = NO;
    scrollView.borderType = NSBezelBorder;
    scrollView.documentView = documentView;
    [scrollView.heightAnchor constraintGreaterThanOrEqualToConstant:minHeight].active = YES;
    return scrollView;
}

NSString* FormatPrice(double value) {
    if (value <= 0.0) {
        return @"--";
    }
    return [NSString stringWithFormat:@"$%.2f", value];
}

NSString* FormatSignedCurrency(double value) {
    if (value > 0.0) {
        return [NSString stringWithFormat:@"+$%.2f", value];
    }
    if (value < 0.0) {
        return [NSString stringWithFormat:@"-$%.2f", std::abs(value)];
    }
    return @"$0.00";
}

NSString* BuyUnavailableReason(const TradingPanelState& state, bool subscribed) {
    if (!state.status.connected) return @"TWS is disconnected.";
    if (!state.status.sessionReady) return @"TWS session is still initializing.";
    if (!subscribed) return @"Subscribe to a symbol first.";
    if (!state.symbol.hasFreshQuote) return @"Waiting for a fresh quote.";
    if (state.status.tradingKillSwitch) return @"Kill switch is enabled.";
    if (state.risk.maxOrderNotional > 0.0 && state.orderNotional > state.risk.maxOrderNotional) {
        return @"Order exceeds the max order notional limit.";
    }
    if (state.risk.maxOpenNotional > 0.0 && state.projectedOpenNotional > state.risk.maxOpenNotional) {
        return @"Projected exposure exceeds the max open notional limit.";
    }
    return @"Buy is not currently available.";
}

NSString* CloseUnavailableReason(const TradingPanelState& state, bool subscribed) {
    if (!state.status.connected) return @"TWS is disconnected.";
    if (!state.status.sessionReady) return @"TWS session is still initializing.";
    if (!subscribed) return @"Subscribe to a symbol first.";
    if (!state.symbol.hasFreshQuote) return @"Waiting for a fresh quote.";
    if (state.status.tradingKillSwitch) return @"Kill switch is enabled.";
    if (state.symbol.availableLongToClose <= 0.0) return @"There is no long position to close.";
    return @"Close is not currently available.";
}

NSString* CancelUnavailableReason(const TradingPanelState& state) {
    if (!state.status.connected) return @"TWS is disconnected.";
    if (!state.hasCancelableOrders) return @"There are no working orders to cancel.";
    return @"Cancel All is not currently available.";
}

enum {
    OrderColumnId = 1,
    OrderColumnSymbol,
    OrderColumnSide,
    OrderColumnQty,
    OrderColumnPrice,
    OrderColumnLocalState,
    OrderColumnStatus,
    OrderColumnWatchdog
};

void StylePanel(NSView* view) {
    view.wantsLayer = YES;
    view.layer.backgroundColor = PanelBackgroundColor().CGColor;
    view.layer.cornerRadius = 14.0;
    view.layer.borderWidth = 1.0;
    view.layer.borderColor = PanelBorderColor().CGColor;
}

} // namespace

@interface TradingWindowController : NSWindowController <NSWindowDelegate, NSTableViewDataSource, NSTableViewDelegate, NSTextFieldDelegate> {
@private
    std::unique_ptr<TradingRuntimeHost> _runtimeHost;
    std::unique_ptr<TradingRefreshScheduler> _refreshScheduler;

    std::string _symbolInput;
    std::string _subscribedSymbol;
    bool _subscribed;
    int _quantityInput;
    double _priceBuffer;
    double _maxPositionDollars;
    std::uint64_t _selectedTraceId;
    std::uint64_t _messagesVersionSeen;
    std::string _messagesText;
    bool _shuttingDown;

    std::vector<std::pair<OrderId, OrderInfo>> _ordersSnapshot;
    std::vector<TradeTraceListItem> _traceItems;

    bool _appActive;
    NSProcessInfoThermalState _thermalState;

    NSTextField* _twsStatusLabel;
    NSTextField* _accountStatusLabel;
    NSTextField* _websocketStatusLabel;
    NSTextField* _recoveryBannerLabel;

    NSTextField* _symbolField;
    NSTextField* _marketHeaderLabel;
    NSTextField* _bidLabel;
    NSTextField* _askLabel;
    NSTextField* _lastLabel;
    NSTextField* _positionLabel;
    NSTextField* _pnlLabel;
    NSTextField* _bookDepthLabel;
    NSTextField* _pricePreviewLabel;
    NSTextField* _safetyStatusLabel;
    NSTextField* _controllerHintLabel;

    NSTextField* _quantityField;
    NSTextField* _bufferField;
    NSTextField* _maxPositionField;

    NSButton* _subscribeButton;
    NSButton* _buyButton;
    NSButton* _closeButton;
    NSButton* _cancelSelectedButton;
    NSButton* _reconcileSelectedButton;
    NSButton* _acknowledgeSelectedButton;
    NSButton* _cancelAllButton;
    NSButton* _armControllerButton;
    NSButton* _killSwitchButton;
    NSButton* _settingsButton;
    NSButton* _exportTraceButton;
    NSButton* _exportAllButton;

    NSTableView* _ordersTable;
    NSPopUpButton* _tracePopup;
    NSTextView* _traceTextView;
    NSTextView* _messagesTextView;
}

- (void)startRuntime;
- (void)shutdownRuntime;
- (void)scheduleRefresh;
- (void)handleControllerAction:(TradingRuntimeControllerAction)action;
- (void)applyAppActive:(BOOL)active;
- (void)applyThermalState:(NSProcessInfoThermalState)thermalState;
- (void)loadPreferences;
- (void)persistPreferences;
- (BOOL)isEditingField:(NSTextField*)field;
- (void)syncGuiInputsToRuntime;
- (RuntimeConnectionConfig)currentConnectionConfig;
- (RiskControlsSnapshot)currentRiskControls;
- (void)applyConnectionConfig:(const RuntimeConnectionConfig&)config;
- (void)applyRiskControls:(const RiskControlsSnapshot&)risk;
- (std::string)ensureCurrentWebSocketToken;
- (void)setControllerArmedEnabled:(BOOL)armed;
- (void)setTradingKillSwitchEnabled:(BOOL)enabled;
- (RuntimePresentationSnapshot)currentPresentationSnapshot;
- (PendingUiSyncUpdate)consumeCurrentPendingUiSyncUpdate;
- (TradingPanelState)currentPanelState;
- (void)appendAppMessage:(const std::string&)message;
- (std::vector<OrderId>)selectedOrderIds;

@end

@implementation TradingWindowController

- (instancetype)init {
    NSRect frame = NSMakeRect(0.0, 0.0, 1540.0, 980.0);
    NSWindow* window = [[NSWindow alloc] initWithContentRect:frame
                                                   styleMask:(NSWindowStyleMaskTitled |
                                                              NSWindowStyleMaskClosable |
                                                              NSWindowStyleMaskResizable |
                                                              NSWindowStyleMaskMiniaturizable)
                                                     backing:NSBackingStoreBuffered
                                                       defer:NO];
    window.title = @"TWS Short Trading GUI";
    window.releasedWhenClosed = NO;
    [window center];

    self = [super initWithWindow:window];
    if (self != nil) {
        _symbolInput = DEFAULT_SYMBOL;
        _quantityInput = 1;
        _priceBuffer = 0.01;
        _maxPositionDollars = 40000.0;
        _selectedTraceId = 0;
        _messagesVersionSeen = 0;
        _subscribed = false;
        _shuttingDown = false;
        _appActive = NSApp == nil ? true : NSApp.isActive;
        _thermalState = NSProcessInfo.processInfo.thermalState;
        _refreshScheduler = std::make_unique<TradingRefreshScheduler>();
        __weak TradingWindowController* weakSelf = self;
        _refreshScheduler->setCallback([weakSelf]() {
            dispatch_async(dispatch_get_main_queue(), ^{
                TradingWindowController* strongSelf = weakSelf;
                if (strongSelf != nil && !strongSelf->_shuttingDown) {
                    [strongSelf refreshInterface];
                }
            });
        });
        _refreshScheduler->setAppActive(_appActive);
        _refreshScheduler->setThermalState(_thermalState);
        [self loadPreferences];
        [self buildInterface];
        self.window.delegate = self;
    }
    return self;
}

- (void)dealloc {
    [self shutdownRuntime];
}

- (void)loadPreferences {
    NSUserDefaults* defaults = [NSUserDefaults standardUserDefaults];

    NSString* savedSymbol = [defaults stringForKey:@"defaultSymbol"];
    if (savedSymbol.length > 0) {
        _symbolInput = toUpperCase(ToStdString(savedSymbol));
    }

    if ([defaults objectForKey:@"quantityInput"] != nil) {
        _quantityInput = std::max(1, static_cast<int>([defaults integerForKey:@"quantityInput"]));
    }
    if ([defaults objectForKey:@"priceBuffer"] != nil) {
        _priceBuffer = std::max(0.0, [defaults doubleForKey:@"priceBuffer"]);
    }
    if ([defaults objectForKey:@"maxPositionDollars"] != nil) {
        _maxPositionDollars = std::max(1000.0, [defaults doubleForKey:@"maxPositionDollars"]);
    }

    RuntimeConnectionConfig connection = [self currentConnectionConfig];
    if (NSString* host = [defaults stringForKey:@"twsHost"]) {
        connection.host = ToStdString(host);
    }
    if ([defaults objectForKey:@"twsPort"] != nil) {
        connection.port = std::max(1, static_cast<int>([defaults integerForKey:@"twsPort"]));
    }
    if ([defaults objectForKey:@"twsClientId"] != nil) {
        connection.clientId = std::max(1, static_cast<int>([defaults integerForKey:@"twsClientId"]));
    }
    if (NSString* token = [defaults stringForKey:kWebSocketTokenDefaultsKey]) {
        connection.websocketAuthToken = ToStdString(token);
    }
    if (connection.websocketAuthToken.empty()) {
        connection.websocketAuthToken = [self ensureCurrentWebSocketToken];
    }
    if ([defaults objectForKey:@"websocketEnabled"] != nil) {
        connection.websocketEnabled = [defaults boolForKey:@"websocketEnabled"];
    }
    if ([defaults objectForKey:@"controllerEnabled"] != nil) {
        connection.controllerEnabled = [defaults boolForKey:@"controllerEnabled"];
    }
    [self applyConnectionConfig:connection];

    RiskControlsSnapshot risk = [self currentRiskControls];
    if ([defaults objectForKey:@"staleQuoteThresholdMs"] != nil) {
        risk.staleQuoteThresholdMs = std::max(250, static_cast<int>([defaults integerForKey:@"staleQuoteThresholdMs"]));
    }
    if ([defaults objectForKey:@"brokerEchoTimeoutMs"] != nil) {
        risk.brokerEchoTimeoutMs = std::max(250, static_cast<int>([defaults integerForKey:@"brokerEchoTimeoutMs"]));
    }
    if ([defaults objectForKey:@"cancelAckTimeoutMs"] != nil) {
        risk.cancelAckTimeoutMs = std::max(500, static_cast<int>([defaults integerForKey:@"cancelAckTimeoutMs"]));
    }
    if ([defaults objectForKey:@"partialFillQuietTimeoutMs"] != nil) {
        risk.partialFillQuietTimeoutMs = std::max(1000, static_cast<int>([defaults integerForKey:@"partialFillQuietTimeoutMs"]));
    }
    if ([defaults objectForKey:@"maxOrderNotional"] != nil) {
        risk.maxOrderNotional = std::max(100.0, [defaults doubleForKey:@"maxOrderNotional"]);
    }
    if ([defaults objectForKey:@"maxOpenNotional"] != nil) {
        risk.maxOpenNotional = std::max(risk.maxOrderNotional, [defaults doubleForKey:@"maxOpenNotional"]);
    }
    if ([defaults objectForKey:@"controllerArmMode"] != nil) {
        risk.controllerArmMode = ControllerArmModeFromPopupIndex([defaults integerForKey:@"controllerArmMode"]);
    }
    [self applyRiskControls:risk];
    [defaults setObject:ToNSString(connection.websocketAuthToken) forKey:kWebSocketTokenDefaultsKey];
}

- (void)persistPreferences {
    NSUserDefaults* defaults = [NSUserDefaults standardUserDefaults];
    [defaults setObject:ToNSString(_symbolInput) forKey:@"defaultSymbol"];
    [defaults setInteger:_quantityInput forKey:@"quantityInput"];
    [defaults setDouble:_priceBuffer forKey:@"priceBuffer"];
    [defaults setDouble:_maxPositionDollars forKey:@"maxPositionDollars"];

    const RuntimeConnectionConfig connection = [self currentConnectionConfig];
    [defaults setObject:ToNSString(connection.host) forKey:@"twsHost"];
    [defaults setInteger:connection.port forKey:@"twsPort"];
    [defaults setInteger:connection.clientId forKey:@"twsClientId"];
    [defaults setObject:ToNSString(connection.websocketAuthToken) forKey:kWebSocketTokenDefaultsKey];
    [defaults setBool:connection.websocketEnabled forKey:@"websocketEnabled"];
    [defaults setBool:connection.controllerEnabled forKey:@"controllerEnabled"];

    const RiskControlsSnapshot risk = [self currentRiskControls];
    [defaults setInteger:risk.staleQuoteThresholdMs forKey:@"staleQuoteThresholdMs"];
    [defaults setInteger:risk.brokerEchoTimeoutMs forKey:@"brokerEchoTimeoutMs"];
    [defaults setInteger:risk.cancelAckTimeoutMs forKey:@"cancelAckTimeoutMs"];
    [defaults setInteger:risk.partialFillQuietTimeoutMs forKey:@"partialFillQuietTimeoutMs"];
    [defaults setDouble:risk.maxOrderNotional forKey:@"maxOrderNotional"];
    [defaults setDouble:risk.maxOpenNotional forKey:@"maxOpenNotional"];
    [defaults setInteger:ControllerArmModePopupIndex(risk.controllerArmMode) forKey:@"controllerArmMode"];
}

- (void)buildInterface {
    [NSApp setAppearance:[NSAppearance appearanceNamed:NSAppearanceNameAqua]];
    self.window.appearance = [NSAppearance appearanceNamed:NSAppearanceNameAqua];
    NSView* contentView = self.window.contentView;
    contentView.wantsLayer = YES;
    contentView.layer.backgroundColor = AppBackgroundColor().CGColor;

    NSStackView* rootStack = MakeColumnStack();
    rootStack.spacing = 16.0;
    rootStack.edgeInsets = NSEdgeInsetsMake(16.0, 16.0, 16.0, 16.0);
    [contentView addSubview:rootStack];

    [NSLayoutConstraint activateConstraints:@[
        [rootStack.leadingAnchor constraintEqualToAnchor:contentView.leadingAnchor],
        [rootStack.trailingAnchor constraintEqualToAnchor:contentView.trailingAnchor],
        [rootStack.topAnchor constraintEqualToAnchor:contentView.topAnchor],
        [rootStack.bottomAnchor constraintEqualToAnchor:contentView.bottomAnchor],
    ]];

    NSStackView* statusRow = MakeRowStack();
    statusRow.distribution = NSStackViewDistributionFill;
    NSStackView* statusLabels = MakeRowStack();
    statusLabels.spacing = 18.0;
    statusLabels.distribution = NSStackViewDistributionFillProportionally;
    _twsStatusLabel = MakeLabel(@"TWS: Disconnected", [NSFont systemFontOfSize:13.0 weight:NSFontWeightSemibold], [NSColor systemRedColor]);
    _accountStatusLabel = MakeLabel(@"Account: --", [NSFont systemFontOfSize:13.0 weight:NSFontWeightMedium], [NSColor labelColor]);
    _websocketStatusLabel = MakeLabel(@"WS: Not running", [NSFont systemFontOfSize:13.0 weight:NSFontWeightMedium], [NSColor secondaryLabelColor]);
    [statusLabels addArrangedSubview:_twsStatusLabel];
    [statusLabels addArrangedSubview:_accountStatusLabel];
    [statusLabels addArrangedSubview:_websocketStatusLabel];

    NSStackView* statusControls = MakeRowStack();
    statusControls.spacing = 12.0;
    _armControllerButton = MakeButton(@"Arm Controller", self, @selector(toggleControllerArmed:));
    [_armControllerButton.widthAnchor constraintEqualToConstant:150.0].active = YES;
    _killSwitchButton = MakeButton(@"Enable Kill Switch", self, @selector(toggleKillSwitch:));
    [_killSwitchButton.widthAnchor constraintEqualToConstant:180.0].active = YES;
    _settingsButton = MakeButton(@"Settings", self, @selector(openSettings:));
    [_settingsButton.widthAnchor constraintEqualToConstant:118.0].active = YES;
    [statusControls addArrangedSubview:_armControllerButton];
    [statusControls addArrangedSubview:_killSwitchButton];
    [statusControls addArrangedSubview:_settingsButton];

    [statusRow addArrangedSubview:statusLabels];
    [statusRow addArrangedSubview:MakeFlexibleSpacer()];
    [statusRow addArrangedSubview:statusControls];
    [rootStack addArrangedSubview:statusRow];
    [statusRow.widthAnchor constraintEqualToAnchor:rootStack.widthAnchor].active = YES;

    _recoveryBannerLabel = MakeLabel(@"", [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold], [NSColor systemRedColor]);
    _recoveryBannerLabel.hidden = YES;
    [rootStack addArrangedSubview:_recoveryBannerLabel];
    [_recoveryBannerLabel.widthAnchor constraintEqualToAnchor:rootStack.widthAnchor].active = YES;

    NSSplitView* bodySplit = [[NSSplitView alloc] initWithFrame:NSZeroRect];
    bodySplit.translatesAutoresizingMaskIntoConstraints = NO;
    bodySplit.vertical = YES;
    bodySplit.dividerStyle = NSSplitViewDividerStyleThin;
    [rootStack addArrangedSubview:bodySplit];
    [bodySplit.widthAnchor constraintEqualToAnchor:rootStack.widthAnchor].active = YES;
    [bodySplit.heightAnchor constraintGreaterThanOrEqualToConstant:760.0].active = YES;

    NSView* leftPanel = [[NSView alloc] initWithFrame:NSZeroRect];
    NSView* rightPanel = [[NSView alloc] initWithFrame:NSZeroRect];
    leftPanel.translatesAutoresizingMaskIntoConstraints = NO;
    rightPanel.translatesAutoresizingMaskIntoConstraints = NO;
    StylePanel(leftPanel);
    StylePanel(rightPanel);
    [bodySplit addSubview:leftPanel];
    [bodySplit addSubview:rightPanel];
    [leftPanel.widthAnchor constraintGreaterThanOrEqualToConstant:700.0].active = YES;
    [rightPanel.widthAnchor constraintGreaterThanOrEqualToConstant:520.0].active = YES;

    NSStackView* leftStack = MakeColumnStack();
    leftStack.spacing = 14.0;
    [leftPanel addSubview:leftStack];
    [NSLayoutConstraint activateConstraints:@[
        [leftStack.leadingAnchor constraintEqualToAnchor:leftPanel.leadingAnchor constant:18.0],
        [leftStack.trailingAnchor constraintEqualToAnchor:leftPanel.trailingAnchor constant:-18.0],
        [leftStack.topAnchor constraintEqualToAnchor:leftPanel.topAnchor constant:18.0],
        [leftStack.bottomAnchor constraintEqualToAnchor:leftPanel.bottomAnchor constant:-18.0],
    ]];

    NSStackView* rightStack = MakeColumnStack();
    rightStack.spacing = 14.0;
    [rightPanel addSubview:rightStack];
    [NSLayoutConstraint activateConstraints:@[
        [rightStack.leadingAnchor constraintEqualToAnchor:rightPanel.leadingAnchor constant:18.0],
        [rightStack.trailingAnchor constraintEqualToAnchor:rightPanel.trailingAnchor constant:-18.0],
        [rightStack.topAnchor constraintEqualToAnchor:rightPanel.topAnchor constant:18.0],
        [rightStack.bottomAnchor constraintEqualToAnchor:rightPanel.bottomAnchor constant:-18.0],
    ]];

    NSStackView* symbolRow = MakeRowStack();
    [symbolRow addArrangedSubview:MakeLabel(@"Symbol", [NSFont systemFontOfSize:13.0 weight:NSFontWeightSemibold], [NSColor secondaryLabelColor])];
    _symbolField = MakeInputField(ToNSString(_symbolInput), 120.0, self, @selector(subscribeAction:), self);
    [symbolRow addArrangedSubview:_symbolField];
    _subscribeButton = MakeButton(@"Subscribe", self, @selector(subscribeAction:));
    StyleTintedButton(_subscribeButton, [NSColor colorWithCalibratedRed:0.15 green:0.45 blue:0.95 alpha:1.0], [NSColor whiteColor]);
    [_subscribeButton.widthAnchor constraintEqualToConstant:116.0].active = YES;
    [symbolRow addArrangedSubview:_subscribeButton];
    [leftStack addArrangedSubview:symbolRow];
    [symbolRow.widthAnchor constraintEqualToAnchor:leftStack.widthAnchor].active = YES;

    _marketHeaderLabel = MakeLabel(@"Market Data: waiting for a subscription", [NSFont systemFontOfSize:15.0 weight:NSFontWeightSemibold], [NSColor labelColor]);
    [leftStack addArrangedSubview:_marketHeaderLabel];
    [_marketHeaderLabel.widthAnchor constraintEqualToAnchor:leftStack.widthAnchor].active = YES;

    NSGridView* marketGrid = [NSGridView gridViewWithNumberOfColumns:4 rows:3];
    marketGrid.rowSpacing = 8.0;
    marketGrid.columnSpacing = 12.0;
    marketGrid.xPlacement = NSGridCellPlacementLeading;
    marketGrid.yPlacement = NSGridCellPlacementCenter;

    [marketGrid cellAtColumnIndex:0 rowIndex:0].contentView = MakeLabel(@"Bid", [NSFont systemFontOfSize:12.0 weight:NSFontWeightMedium], [NSColor secondaryLabelColor]);
    _bidLabel = MakeValueLabel();
    [marketGrid cellAtColumnIndex:1 rowIndex:0].contentView = _bidLabel;
    [marketGrid cellAtColumnIndex:2 rowIndex:0].contentView = MakeLabel(@"Ask", [NSFont systemFontOfSize:12.0 weight:NSFontWeightMedium], [NSColor secondaryLabelColor]);
    _askLabel = MakeValueLabel();
    [marketGrid cellAtColumnIndex:3 rowIndex:0].contentView = _askLabel;

    [marketGrid cellAtColumnIndex:0 rowIndex:1].contentView = MakeLabel(@"Last", [NSFont systemFontOfSize:12.0 weight:NSFontWeightMedium], [NSColor secondaryLabelColor]);
    _lastLabel = MakeValueLabel();
    [marketGrid cellAtColumnIndex:1 rowIndex:1].contentView = _lastLabel;
    [marketGrid cellAtColumnIndex:2 rowIndex:1].contentView = MakeLabel(@"Book", [NSFont systemFontOfSize:12.0 weight:NSFontWeightMedium], [NSColor secondaryLabelColor]);
    _bookDepthLabel = MakeValueLabel();
    [marketGrid cellAtColumnIndex:3 rowIndex:1].contentView = _bookDepthLabel;

    [marketGrid cellAtColumnIndex:0 rowIndex:2].contentView = MakeLabel(@"Position", [NSFont systemFontOfSize:12.0 weight:NSFontWeightMedium], [NSColor secondaryLabelColor]);
    _positionLabel = MakeValueLabel();
    [marketGrid cellAtColumnIndex:1 rowIndex:2].contentView = _positionLabel;
    [marketGrid cellAtColumnIndex:2 rowIndex:2].contentView = MakeLabel(@"P&L", [NSFont systemFontOfSize:12.0 weight:NSFontWeightMedium], [NSColor secondaryLabelColor]);
    _pnlLabel = MakeValueLabel();
    [marketGrid cellAtColumnIndex:3 rowIndex:2].contentView = _pnlLabel;
    [leftStack addArrangedSubview:marketGrid];
    [marketGrid.widthAnchor constraintEqualToAnchor:leftStack.widthAnchor].active = YES;

    NSStackView* inputRow = MakeRowStack();
    [inputRow addArrangedSubview:MakeLabel(@"Qty", [NSFont systemFontOfSize:12.0 weight:NSFontWeightMedium], [NSColor secondaryLabelColor])];
    _quantityField = MakeInputField(@"1", 72.0, self, @selector(inputFieldAction:), self);
    [inputRow addArrangedSubview:_quantityField];
    [inputRow addArrangedSubview:MakeLabel(@"Buffer", [NSFont systemFontOfSize:12.0 weight:NSFontWeightMedium], [NSColor secondaryLabelColor])];
    _bufferField = MakeInputField(@"0.01", 80.0, self, @selector(inputFieldAction:), self);
    [inputRow addArrangedSubview:_bufferField];
    [inputRow addArrangedSubview:MakeLabel(@"Max Position $", [NSFont systemFontOfSize:12.0 weight:NSFontWeightMedium], [NSColor secondaryLabelColor])];
    _maxPositionField = MakeInputField(@"40000", 110.0, self, @selector(inputFieldAction:), self);
    [inputRow addArrangedSubview:_maxPositionField];
    [leftStack addArrangedSubview:inputRow];
    [inputRow.widthAnchor constraintEqualToAnchor:leftStack.widthAnchor].active = YES;

    _pricePreviewLabel = MakeLabel(@"Prices: buy --  |  sell --", [NSFont monospacedSystemFontOfSize:13.0 weight:NSFontWeightMedium], [NSColor labelColor]);
    [leftStack addArrangedSubview:_pricePreviewLabel];
    [_pricePreviewLabel.widthAnchor constraintEqualToAnchor:leftStack.widthAnchor].active = YES;

    _safetyStatusLabel = MakeLabel(@"Safety: quote waiting  |  controller disarmed  |  kill switch off",
                                   [NSFont systemFontOfSize:12.0 weight:NSFontWeightMedium],
                                   [NSColor secondaryLabelColor]);
    [leftStack addArrangedSubview:_safetyStatusLabel];
    [_safetyStatusLabel.widthAnchor constraintEqualToAnchor:leftStack.widthAnchor].active = YES;

    NSStackView* actionRow = MakeRowStack();
    _buyButton = MakeButton(@"Buy Limit", self, @selector(buyAction:));
    StyleTintedButton(_buyButton, [NSColor colorWithCalibratedRed:0.13 green:0.64 blue:0.32 alpha:1.0], [NSColor whiteColor]);
    [_buyButton.widthAnchor constraintEqualToConstant:170.0].active = YES;
    _closeButton = MakeButton(@"Close Long", self, @selector(closeAction:));
    StyleTintedButton(_closeButton, [NSColor colorWithCalibratedRed:0.96 green:0.60 blue:0.18 alpha:1.0], [NSColor whiteColor]);
    [_closeButton.widthAnchor constraintEqualToConstant:210.0].active = YES;
    _cancelAllButton = MakeButton(@"Cancel All", self, @selector(cancelAllAction:));
    StyleTintedButton(_cancelAllButton, [NSColor colorWithCalibratedRed:0.90 green:0.27 blue:0.22 alpha:1.0], [NSColor whiteColor]);
    [_cancelAllButton.widthAnchor constraintEqualToConstant:130.0].active = YES;
    [actionRow addArrangedSubview:_buyButton];
    [actionRow addArrangedSubview:_closeButton];
    [actionRow addArrangedSubview:_cancelAllButton];
    [leftStack addArrangedSubview:actionRow];
    [actionRow.widthAnchor constraintEqualToAnchor:leftStack.widthAnchor].active = YES;

    _controllerHintLabel = MakeLabel(@"Controller: Square buy  |  Circle close  |  Triangle cancel all  |  Cross toggle qty", [NSFont systemFontOfSize:12.0 weight:NSFontWeightMedium], [NSColor secondaryLabelColor]);
    [leftStack addArrangedSubview:_controllerHintLabel];
    [_controllerHintLabel.widthAnchor constraintEqualToAnchor:leftStack.widthAnchor].active = YES;

    [leftStack addArrangedSubview:MakeLabel(@"Working Orders", [NSFont systemFontOfSize:15.0 weight:NSFontWeightSemibold], [NSColor labelColor])];

    _ordersTable = [[NSTableView alloc] initWithFrame:NSZeroRect];
    _ordersTable.delegate = self;
    _ordersTable.dataSource = self;
    _ordersTable.usesAlternatingRowBackgroundColors = YES;
    _ordersTable.allowsMultipleSelection = YES;
    _ordersTable.rowHeight = 24.0;

    NSArray<NSDictionary*>* columns = @[
        @{@"id": @"id", @"title": @"ID", @"width": @70, @"tag": @(OrderColumnId)},
        @{@"id": @"symbol", @"title": @"Symbol", @"width": @90, @"tag": @(OrderColumnSymbol)},
        @{@"id": @"side", @"title": @"Side", @"width": @60, @"tag": @(OrderColumnSide)},
        @{@"id": @"qty", @"title": @"Qty", @"width": @70, @"tag": @(OrderColumnQty)},
        @{@"id": @"price", @"title": @"Price", @"width": @90, @"tag": @(OrderColumnPrice)},
        @{@"id": @"local", @"title": @"Local", @"width": @150, @"tag": @(OrderColumnLocalState)},
        @{@"id": @"status", @"title": @"Broker", @"width": @110, @"tag": @(OrderColumnStatus)},
        @{@"id": @"watchdog", @"title": @"Watchdog", @"width": @180, @"tag": @(OrderColumnWatchdog)},
    ];
    for (NSDictionary* info in columns) {
        NSTableColumn* column = [[NSTableColumn alloc] initWithIdentifier:info[@"id"]];
        column.title = info[@"title"];
        column.width = [info[@"width"] doubleValue];
        column.resizingMask = NSTableColumnAutoresizingMask;
        column.headerCell.alignment = NSTextAlignmentLeft;
        column.headerCell.tag = [info[@"tag"] intValue];
        [_ordersTable addTableColumn:column];
    }

    NSScrollView* ordersScrollView = MakeScrollView(_ordersTable, 280.0);
    [leftStack addArrangedSubview:ordersScrollView];
    [ordersScrollView.widthAnchor constraintEqualToAnchor:leftStack.widthAnchor].active = YES;

    NSStackView* orderButtons = MakeRowStack();
    _cancelSelectedButton = MakeButton(@"Cancel Selected", self, @selector(cancelSelectedAction:));
    StyleTintedButton(_cancelSelectedButton, [NSColor colorWithCalibratedRed:0.90 green:0.27 blue:0.22 alpha:1.0], [NSColor whiteColor]);
    [_cancelSelectedButton.widthAnchor constraintEqualToConstant:150.0].active = YES;
    _reconcileSelectedButton = MakeButton(@"Reconcile Selected", self, @selector(reconcileSelectedAction:));
    StyleTintedButton(_reconcileSelectedButton,
                      [NSColor colorWithCalibratedRed:0.22 green:0.52 blue:0.92 alpha:1.0],
                      [NSColor whiteColor]);
    [_reconcileSelectedButton.widthAnchor constraintEqualToConstant:170.0].active = YES;
    _acknowledgeSelectedButton = MakeButton(@"Acknowledge", self, @selector(acknowledgeSelectedAction:));
    StyleTintedButton(_acknowledgeSelectedButton,
                      [NSColor colorWithCalibratedRed:0.78 green:0.45 blue:0.15 alpha:1.0],
                      [NSColor whiteColor]);
    [_acknowledgeSelectedButton.widthAnchor constraintEqualToConstant:150.0].active = YES;
    [orderButtons addArrangedSubview:_cancelSelectedButton];
    [orderButtons addArrangedSubview:_reconcileSelectedButton];
    [orderButtons addArrangedSubview:_acknowledgeSelectedButton];
    [leftStack addArrangedSubview:orderButtons];
    [orderButtons.widthAnchor constraintEqualToAnchor:leftStack.widthAnchor].active = YES;

    [rightStack addArrangedSubview:MakeLabel(@"Trade Trace", [NSFont systemFontOfSize:15.0 weight:NSFontWeightSemibold], [NSColor labelColor])];
    _tracePopup = [[NSPopUpButton alloc] initWithFrame:NSZeroRect pullsDown:NO];
    _tracePopup.target = self;
    _tracePopup.action = @selector(traceSelectionChanged:);
    [rightStack addArrangedSubview:_tracePopup];
    [_tracePopup.widthAnchor constraintEqualToAnchor:rightStack.widthAnchor].active = YES;

    NSStackView* exportRow = MakeRowStack();
    _exportTraceButton = MakeButton(@"Export Trace", self, @selector(exportSelectedTrace:));
    [_exportTraceButton.widthAnchor constraintEqualToConstant:150.0].active = YES;
    _exportAllButton = MakeButton(@"Export All CSV", self, @selector(exportAllTracesSummary:));
    [_exportAllButton.widthAnchor constraintEqualToConstant:150.0].active = YES;
    [exportRow addArrangedSubview:_exportTraceButton];
    [exportRow addArrangedSubview:_exportAllButton];
    [rightStack addArrangedSubview:exportRow];
    [exportRow.widthAnchor constraintEqualToAnchor:rightStack.widthAnchor].active = YES;

    _traceTextView = MakeReadOnlyTextView();
    NSScrollView* traceScrollView = MakeScrollView(_traceTextView, 320.0);
    [rightStack addArrangedSubview:traceScrollView];
    [traceScrollView.widthAnchor constraintEqualToAnchor:rightStack.widthAnchor].active = YES;

    [rightStack addArrangedSubview:MakeLabel(@"Messages", [NSFont systemFontOfSize:15.0 weight:NSFontWeightSemibold], [NSColor labelColor])];
    _messagesTextView = MakeReadOnlyTextView();
    NSScrollView* messagesScrollView = MakeScrollView(_messagesTextView, 240.0);
    [rightStack addArrangedSubview:messagesScrollView];
    [messagesScrollView.widthAnchor constraintEqualToAnchor:rightStack.widthAnchor].active = YES;

    [self updateInputFieldsFromState];
}

- (void)showWindowAndStart {
    [self showWindow:nil];
    [self.window makeKeyAndOrderFront:nil];
    [NSApp activateIgnoringOtherApps:YES];
    [self startRuntime];
}

- (void)startRuntime {
    if (_runtimeHost) {
        return;
    }

    [self syncGuiInputsToRuntime];
    _runtimeHost = std::make_unique<TradingRuntimeHost>();

    __weak TradingWindowController* weakSelf = self;
    _runtimeHost->start([weakSelf]() {
        dispatch_async(dispatch_get_main_queue(), ^{
            TradingWindowController* strongSelf = weakSelf;
            if (strongSelf != nil) {
                [strongSelf scheduleRefresh];
            }
        });
    }, [weakSelf](TradingRuntimeControllerAction action) {
        dispatch_async(dispatch_get_main_queue(), ^{
            TradingWindowController* strongSelf = weakSelf;
            if (strongSelf != nil) {
                [strongSelf handleControllerAction:action];
            }
        });
    });
    [self scheduleRefresh];
}

- (void)shutdownRuntime {
    if (_shuttingDown) {
        return;
    }
    _shuttingDown = true;
    if (_refreshScheduler) {
        _refreshScheduler->cancel();
    }
    if (_runtimeHost) {
        _runtimeHost->shutdown();
        _runtimeHost.reset();
    }
}

- (void)windowWillClose:(NSNotification*)notification {
    (void)notification;
    [self shutdownRuntime];
    [NSApp terminate:nil];
}

- (void)scheduleRefresh {
    if (_shuttingDown) {
        return;
    }

    if (![NSThread isMainThread]) {
        __weak TradingWindowController* weakSelf = self;
        dispatch_async(dispatch_get_main_queue(), ^{
            TradingWindowController* strongSelf = weakSelf;
            if (strongSelf != nil) {
                [strongSelf scheduleRefresh];
            }
        });
        return;
    }

    if (_refreshScheduler) {
        _refreshScheduler->schedule();
    }
}

- (void)inputFieldAction:(id)sender {
    (void)sender;
    [self syncInputsFromFields];
    [self scheduleRefresh];
}

- (void)controlTextDidEndEditing:(NSNotification*)notification {
    (void)notification;
    [self syncInputsFromFields];
    [self scheduleRefresh];
}

- (void)controlTextDidChange:(NSNotification*)notification {
    id object = notification.object;
    if (object == _symbolField) {
        _symbolInput = ToStdString(_symbolField.stringValue);
    }
}

- (BOOL)isEditingField:(NSTextField*)field {
    if (field == nil || field.window == nil) {
        return NO;
    }
    NSText* editor = field.currentEditor;
    return editor != nil && field.window.firstResponder == editor;
}

- (void)syncInputsFromFields {
    _symbolInput = ToStdString(_symbolField.stringValue);
    const int parsedQuantity = _quantityField.intValue;
    _quantityInput = std::max(1, parsedQuantity);
    _priceBuffer = std::max(0.0, _bufferField.doubleValue);
    _maxPositionDollars = std::max(1000.0, _maxPositionField.doubleValue);
    [self syncGuiInputsToRuntime];
    [self persistPreferences];
    [self updateInputFieldsFromState];
}

- (void)syncGuiInputsToRuntime {
    if (_runtimeHost && _runtimeHost->runtime()) {
        _runtimeHost->runtime()->syncGuiInputs(_quantityInput, _priceBuffer, _maxPositionDollars);
    } else {
        syncSharedGuiInputs(_quantityInput, _priceBuffer, _maxPositionDollars);
    }
}

- (RuntimeConnectionConfig)currentConnectionConfig {
    if (_runtimeHost && _runtimeHost->runtime()) {
        return _runtimeHost->runtime()->captureConnectionConfig();
    }
    return captureRuntimeConnectionConfig();
}

- (RiskControlsSnapshot)currentRiskControls {
    if (_runtimeHost && _runtimeHost->runtime()) {
        return _runtimeHost->runtime()->captureRiskControls();
    }
    return captureRiskControlsSnapshot();
}

- (void)applyConnectionConfig:(const RuntimeConnectionConfig&)config {
    if (_runtimeHost && _runtimeHost->runtime()) {
        _runtimeHost->runtime()->updateConnectionConfig(config);
    } else {
        updateRuntimeConnectionConfig(config);
    }
}

- (void)applyRiskControls:(const RiskControlsSnapshot&)risk {
    if (_runtimeHost && _runtimeHost->runtime()) {
        _runtimeHost->runtime()->updateRiskControls(risk);
    } else {
        updateRiskControls(risk.staleQuoteThresholdMs,
                           risk.brokerEchoTimeoutMs,
                           risk.cancelAckTimeoutMs,
                           risk.partialFillQuietTimeoutMs,
                           risk.maxOrderNotional,
                           risk.maxOpenNotional,
                           risk.controllerArmMode);
    }
}

- (std::string)ensureCurrentWebSocketToken {
    if (_runtimeHost && _runtimeHost->runtime()) {
        return _runtimeHost->runtime()->ensureWebSocketAuthToken();
    }
    return ensureWebSocketAuthToken();
}

- (void)setControllerArmedEnabled:(BOOL)armed {
    if (_runtimeHost && _runtimeHost->runtime()) {
        _runtimeHost->runtime()->setControllerArmed(armed);
    } else {
        setControllerArmed(armed);
    }
}

- (void)setTradingKillSwitchEnabled:(BOOL)enabled {
    if (_runtimeHost && _runtimeHost->runtime()) {
        _runtimeHost->runtime()->setTradingKillSwitch(enabled);
    } else {
        setTradingKillSwitch(enabled);
    }
}

- (RuntimePresentationSnapshot)currentPresentationSnapshot {
    const std::string activeSymbol = _subscribed ? _subscribedSymbol : std::string();
    if (_runtimeHost && _runtimeHost->runtime()) {
        return _runtimeHost->runtime()->capturePresentationSnapshot(activeSymbol, 150);
    }
    return captureRuntimePresentationSnapshot(activeSymbol, 150);
}

- (PendingUiSyncUpdate)consumeCurrentPendingUiSyncUpdate {
    if (_runtimeHost && _runtimeHost->runtime()) {
        return _runtimeHost->runtime()->consumePendingUiSyncUpdate();
    }
    return consumePendingUiSyncUpdate();
}

- (TradingPanelState)currentPanelState {
    return buildTradingPanelState([self currentPresentationSnapshot],
                                  _subscribed,
                                  _quantityInput,
                                  _priceBuffer,
                                  _maxPositionDollars);
}

- (void)appendAppMessage:(const std::string&)message {
    if (message.empty()) {
        return;
    }
    if (_runtimeHost && _runtimeHost->runtime()) {
        _runtimeHost->runtime()->appendMessage(message);
    } else {
        appendSharedMessage(message);
    }
}

- (void)updateInputFieldsFromState {
    if (![self isEditingField:_quantityField]) {
        _quantityField.stringValue = [NSString stringWithFormat:@"%d", _quantityInput];
    }
    if (![self isEditingField:_bufferField]) {
        _bufferField.stringValue = [NSString stringWithFormat:@"%.2f", _priceBuffer];
    }
    if (![self isEditingField:_maxPositionField]) {
        _maxPositionField.stringValue = [NSString stringWithFormat:@"%.0f", _maxPositionDollars];
    }
    if (![self isEditingField:_symbolField] && !_symbolInput.empty()) {
        _symbolField.stringValue = ToNSString(_symbolInput);
    }
}

- (void)subscribeAction:(id)sender {
    (void)sender;
    [self syncInputsFromFields];
    std::string requestedSymbol;
    std::string error;
    if (!requestSubscriptionAction(_runtimeHost ? _runtimeHost->runtime() : nullptr,
                                   ToStdString(_symbolField.stringValue),
                                   false,
                                   &requestedSymbol,
                                   &error)) {
        [self appendAppMessage:"Subscribe failed: " + error];
        [self scheduleRefresh];
        return;
    }

    _symbolInput = requestedSymbol;
    _subscribed = true;
    _subscribedSymbol = requestedSymbol;
    [self updateInputFieldsFromState];
    [self scheduleRefresh];
}

- (void)buyAction:(id)sender {
    (void)sender;
    [self syncInputsFromFields];
    const TradingPanelState state = [self currentPanelState];
    const TradingSubmitResult result = submitBuyAction(_runtimeHost ? _runtimeHost->runtime() : nullptr,
                                                       state,
                                                       _subscribedSymbol,
                                                       _quantityInput,
                                                       _priceBuffer,
                                                       "GUI Button",
                                                       "Buy Limit button pressed");
    if (!result.submitted && !result.error.empty()) {
        [self appendAppMessage:"Buy failed: " + result.error];
    }
    if (result.traceId != 0) {
        _selectedTraceId = result.traceId;
    }
    [self scheduleRefresh];
}

- (void)closeAction:(id)sender {
    (void)sender;
    [self syncInputsFromFields];
    const TradingPanelState state = [self currentPanelState];
    const TradingSubmitResult result = submitCloseAction(_runtimeHost ? _runtimeHost->runtime() : nullptr,
                                                         state,
                                                         _subscribedSymbol,
                                                         _priceBuffer,
                                                         "GUI Button",
                                                         "Close Long button pressed");
    if (!result.submitted && !result.error.empty()) {
        [self appendAppMessage:"Close failed: " + result.error];
    }
    if (result.traceId != 0) {
        _selectedTraceId = result.traceId;
    }
    [self scheduleRefresh];
}

- (std::vector<OrderId>)selectedOrderIds {
    __block std::vector<OrderId> orderIds;
    NSIndexSet* selectedRows = _ordersTable.selectedRowIndexes;
    [selectedRows enumerateIndexesUsingBlock:^(NSUInteger idx, BOOL* stop) {
        (void)stop;
        if (idx < _ordersSnapshot.size()) {
            orderIds.push_back(_ordersSnapshot[idx].first);
        }
    }];
    return orderIds;
}

- (void)cancelSelectedAction:(id)sender {
    (void)sender;
    if (!_runtimeHost || !_runtimeHost->runtime()) {
        return;
    }

    const std::vector<OrderId> selectedOrderIds = [self selectedOrderIds];
    if (selectedOrderIds.empty()) {
        [self appendAppMessage:"No orders selected for cancellation"];
        [self scheduleRefresh];
        return;
    }

    const TradingCancelResult result = cancelSelectedOrdersAction(_runtimeHost->runtime(), selectedOrderIds);
    for (std::size_t i = 0; i < result.orderIds.size(); ++i) {
        if (i < result.sent.size() && result.sent[i]) {
            [self appendAppMessage:"Cancel request sent for order " + std::to_string(result.orderIds[i])];
        } else {
            [self appendAppMessage:"Cancel failed (not connected) for order " + std::to_string(result.orderIds[i])];
        }
    }
    [self scheduleRefresh];
}

- (void)reconcileSelectedAction:(id)sender {
    (void)sender;
    if (!_runtimeHost || !_runtimeHost->runtime()) {
        return;
    }

    const std::vector<OrderId> selectedOrderIds = [self selectedOrderIds];
    if (selectedOrderIds.empty()) {
        [self appendAppMessage:"No orders selected for reconciliation"];
        [self scheduleRefresh];
        return;
    }

    const std::vector<OrderId> accepted = _runtimeHost->runtime()->requestOrderReconciliation(selectedOrderIds);
    if (accepted.empty()) {
        [self appendAppMessage:"Selected orders do not need reconciliation right now"];
    } else {
        for (const OrderId orderId : accepted) {
            [self appendAppMessage:"Manual reconcile requested for order " + std::to_string(static_cast<long long>(orderId))];
        }
    }
    [self scheduleRefresh];
}

- (void)acknowledgeSelectedAction:(id)sender {
    (void)sender;
    if (!_runtimeHost || !_runtimeHost->runtime()) {
        return;
    }

    const std::vector<OrderId> selectedOrderIds = [self selectedOrderIds];
    if (selectedOrderIds.empty()) {
        [self appendAppMessage:"No orders selected for acknowledgement"];
        [self scheduleRefresh];
        return;
    }

    const std::vector<OrderId> acknowledged = _runtimeHost->runtime()->acknowledgeManualReviewOrders(selectedOrderIds);
    if (acknowledged.empty()) {
        [self appendAppMessage:"Selected orders do not require manual review acknowledgement"];
    }
    [self scheduleRefresh];
}

- (void)cancelAllAction:(id)sender {
    (void)sender;
    if (!_runtimeHost || !_runtimeHost->runtime()) {
        return;
    }

    NSAlert* confirm = [[NSAlert alloc] init];
    confirm.messageText = @"Cancel All Orders?";
    confirm.informativeText = @"This will send cancel requests for every working order.";
    confirm.alertStyle = NSAlertStyleWarning;
    [confirm addButtonWithTitle:@"Cancel All"];
    [confirm addButtonWithTitle:@"Keep Orders"];
    if ([confirm runModal] != NSAlertFirstButtonReturn) {
        return;
    }

    const TradingCancelResult result = cancelAllOrdersAction(_runtimeHost->runtime());
    if (result.orderIds.empty()) {
        [self appendAppMessage:"No pending orders to cancel"];
        [self scheduleRefresh];
        return;
    }

    const int sentCount = static_cast<int>(std::count(result.sent.begin(), result.sent.end(), true));
    if (sentCount != static_cast<int>(result.orderIds.size())) {
        [self appendAppMessage:"Some cancel requests could not be sent"];
    }
    [self appendAppMessage:"Cancel requested for " + std::to_string(sentCount) + " order(s)"];
    [self scheduleRefresh];
}

- (void)toggleControllerArmed:(id)sender {
    (void)sender;
    const bool armed = ![self currentRiskControls].controllerArmed;
    [self setControllerArmedEnabled:armed];
    [self appendAppMessage:(armed ? "Controller trading armed" : "Controller trading disarmed")];
    [self scheduleRefresh];
}

- (void)toggleKillSwitch:(id)sender {
    (void)sender;
    const bool enabled = ![self currentRiskControls].tradingKillSwitch;
    [self setTradingKillSwitchEnabled:enabled];
    if (enabled) {
        [self setControllerArmedEnabled:NO];
    }
    [self appendAppMessage:(enabled ? "Kill switch enabled: trading halted" : "Kill switch disabled: trading may resume")];
    [self scheduleRefresh];
}

- (void)openSettings:(id)sender {
    (void)sender;

    const RuntimeConnectionConfig currentConnection = [self currentConnectionConfig];
    const RiskControlsSnapshot currentRisk = [self currentRiskControls];
    RuntimeConnectionConfig updatedConnection = currentConnection;
    RiskControlsSnapshot updatedRisk = currentRisk;
    if (!RunTradingSettingsSheet(self.window, currentConnection, currentRisk, &updatedConnection, &updatedRisk)) {
        return;
    }

    [self applyConnectionConfig:updatedConnection];
    const std::string ensuredToken = [self ensureCurrentWebSocketToken];
    updatedConnection.websocketAuthToken = ensuredToken;
    [self applyConnectionConfig:updatedConnection];
    [self applyRiskControls:updatedRisk];

    [self persistPreferences];
    [self appendAppMessage:"Settings saved"];
    if (updatedConnection.host != currentConnection.host ||
        updatedConnection.port != currentConnection.port ||
        updatedConnection.clientId != currentConnection.clientId ||
        updatedConnection.websocketEnabled != currentConnection.websocketEnabled ||
        updatedConnection.controllerEnabled != currentConnection.controllerEnabled) {
        [self appendAppMessage:"Restart the app to apply new connection/device settings"];
    }
    [self scheduleRefresh];
}

- (void)exportSelectedTrace:(id)sender {
    (void)sender;
    std::string successMessage;
    if (RunSelectedTraceExportPanel(self.window, _selectedTraceId, &successMessage)) {
        [self appendAppMessage:successMessage];
        [self scheduleRefresh];
    }
}

- (void)exportAllTracesSummary:(id)sender {
    (void)sender;
    std::string successMessage;
    if (RunAllTradesSummaryExportPanel(self.window, &successMessage)) {
        [self appendAppMessage:successMessage];
        [self scheduleRefresh];
    }
}

- (void)traceSelectionChanged:(id)sender {
    NSInteger selectedIndex = _tracePopup.indexOfSelectedItem;
    if (selectedIndex >= 0 && static_cast<std::size_t>(selectedIndex) < _traceItems.size()) {
        _selectedTraceId = _traceItems[static_cast<std::size_t>(selectedIndex)].traceId;
    }
    [self scheduleRefresh];
}

- (void)handleControllerAction:(TradingRuntimeControllerAction)action {
    if (_shuttingDown) {
        return;
    }

    const TradingPanelState state = [self currentPanelState];
    const TradingControllerActionResult result = handleControllerActionIntent(_runtimeHost ? _runtimeHost->runtime() : nullptr,
                                                                              action,
                                                                              state,
                                                                              _subscribedSymbol,
                                                                              _quantityInput,
                                                                              _priceBuffer,
                                                                              _maxPositionDollars);
    if (result.quantityChanged) {
        _quantityInput = result.quantityInput;
        [self updateInputFieldsFromState];
    }
    if (result.traceId != 0) {
        _selectedTraceId = result.traceId;
    }
    for (const auto& message : result.messages) {
        [self appendAppMessage:message];
    }

    [self scheduleRefresh];
}

- (void)applyAppActive:(BOOL)active {
    _appActive = active;
    if (_refreshScheduler) {
        _refreshScheduler->setAppActive(active);
    }
    [self scheduleRefresh];
}

- (void)applyThermalState:(NSProcessInfoThermalState)thermalState {
    if (_thermalState == thermalState) {
        return;
    }

    const NSProcessInfoThermalState previousState = _thermalState;
    _thermalState = thermalState;
    if (_refreshScheduler) {
        _refreshScheduler->setThermalState(thermalState);
    }

    if (thermalState == NSProcessInfoThermalStateSerious) {
        [self appendAppMessage:"macOS thermal state is serious: slowing UI refresh"];
    } else if (thermalState == NSProcessInfoThermalStateCritical) {
        [self appendAppMessage:"macOS thermal state is critical: strongly throttling UI refresh"];
    } else if ((previousState == NSProcessInfoThermalStateSerious ||
                previousState == NSProcessInfoThermalStateCritical) &&
               thermalState == NSProcessInfoThermalStateNominal) {
        [self appendAppMessage:"macOS thermal state returned to nominal"];
    }

    [self scheduleRefresh];
}

- (void)refreshInterface {
    TradingViewModelInput input;
    input.symbolInput = _symbolInput;
    input.subscribedSymbol = _subscribedSymbol;
    input.subscribed = _subscribed;
    input.quantityInput = _quantityInput;
    input.priceBuffer = _priceBuffer;
    input.maxPositionDollars = _maxPositionDollars;
    input.selectedTraceId = _selectedTraceId;
    input.pendingUiSync = [self consumeCurrentPendingUiSyncUpdate];
    input.presentation = [self currentPresentationSnapshot];
    const TradingViewModel model = buildTradingViewModel(input);

    _symbolInput = model.symbolInput;
    _subscribedSymbol = model.subscribedSymbol;
    _subscribed = model.subscribed;
    _quantityInput = model.quantityInput;
    _selectedTraceId = model.selectedTraceId;
    _messagesVersionSeen = model.messagesVersionSeen;
    _messagesText = model.messagesText;
    _ordersSnapshot = model.orders;
    _traceItems = model.traceItems;
    [self updateInputFieldsFromState];

    if (_runtimeHost) {
        _runtimeHost->setControllerVibration(model.shouldVibrate);
    }
    [self refreshStatusLabels:model.panel];
    [self refreshMarketSection:model.panel];
    [self refreshOrders];
    [self refreshTracePopup];
    _traceTextView.string = ToNSString(model.traceDetailsText);
    _exportTraceButton.enabled = model.canExportSelectedTrace;
    _exportAllButton.enabled = model.canExportAllTraces;
    [self refreshMessages];
}

- (void)refreshStatusLabels:(const TradingPanelState&)state {
    if (state.status.connected && state.status.sessionReady) {
        _twsStatusLabel.stringValue = @"TWS: Ready";
        _twsStatusLabel.textColor = [NSColor systemGreenColor];
    } else if (state.status.connected) {
        _twsStatusLabel.stringValue = [NSString stringWithFormat:@"TWS: %@", ToNSString(state.status.sessionStateText)];
        _twsStatusLabel.textColor = [NSColor systemOrangeColor];
    } else {
        _twsStatusLabel.stringValue = @"TWS: Disconnected";
        _twsStatusLabel.textColor = [NSColor systemRedColor];
    }

    _accountStatusLabel.stringValue = [NSString stringWithFormat:@"Account: %@", ToNSString(state.status.accountText)];

    if (!state.status.websocketEnabled) {
        _websocketStatusLabel.stringValue = @"WS: Disabled";
        _websocketStatusLabel.textColor = [NSColor secondaryLabelColor];
    } else if (state.status.wsServerRunning) {
        _websocketStatusLabel.stringValue = [NSString stringWithFormat:@"WS: localhost:%d (%d clients)", WEBSOCKET_PORT, state.status.wsConnectedClients];
        _websocketStatusLabel.textColor = [NSColor systemGreenColor];
    } else {
        _websocketStatusLabel.stringValue = @"WS: Not running";
        _websocketStatusLabel.textColor = [NSColor systemRedColor];
    }

    _armControllerButton.title = state.status.controllerArmed ? @"Disarm Controller" : @"Arm Controller";
    SetButtonEnabledTint(_armControllerButton,
                         state.status.controllerEnabled && state.status.controllerConnected,
                         state.status.controllerArmed ? [NSColor systemOrangeColor] : [NSColor systemBlueColor],
                         [NSColor colorWithCalibratedWhite:0.86 alpha:1.0]);

    _killSwitchButton.title = state.status.tradingKillSwitch ? @"Disable Kill Switch" : @"Enable Kill Switch";
    SetButtonEnabledTint(_killSwitchButton,
                         true,
                         state.status.tradingKillSwitch ? [NSColor systemRedColor] : [NSColor colorWithCalibratedRed:0.40 green:0.40 blue:0.40 alpha:1.0],
                         [NSColor colorWithCalibratedWhite:0.86 alpha:1.0]);

    StyleTintedButton(_settingsButton, [NSColor colorWithCalibratedRed:0.25 green:0.45 blue:0.70 alpha:1.0], [NSColor whiteColor]);

    if (!state.status.startupRecoveryBanner.empty()) {
        _recoveryBannerLabel.hidden = NO;
        _recoveryBannerLabel.stringValue = ToNSString(state.status.startupRecoveryBanner);
        _recoveryBannerLabel.textColor = [NSColor systemRedColor];
    } else {
        _recoveryBannerLabel.hidden = YES;
        _recoveryBannerLabel.stringValue = @"";
    }
}

- (void)refreshMarketSection:(const TradingPanelState&)state {
    if (_subscribed && !_subscribedSymbol.empty()) {
        _marketHeaderLabel.stringValue = [NSString stringWithFormat:@"Market Data: %@", ToNSString(_subscribedSymbol)];
    } else {
        _marketHeaderLabel.stringValue = @"Market Data: waiting for a subscription";
    }

    _bidLabel.stringValue = FormatPrice(state.symbol.bidPrice);
    _askLabel.stringValue = FormatPrice(state.symbol.askPrice);
    _lastLabel.stringValue = FormatPrice(state.symbol.lastPrice);
    _bookDepthLabel.stringValue = [NSString stringWithFormat:@"%d ask / %d bid", state.askLevels, state.bidLevels];

    if (state.symbol.hasPosition && state.symbol.currentPositionQty != 0.0) {
        if (state.symbol.currentPositionQty > 0.0) {
            _positionLabel.stringValue = [NSString stringWithFormat:@"%.0f LONG", state.symbol.currentPositionQty];
            _positionLabel.textColor = [NSColor systemGreenColor];
        } else {
            _positionLabel.stringValue = [NSString stringWithFormat:@"%.0f SHORT", -state.symbol.currentPositionQty];
            _positionLabel.textColor = [NSColor systemRedColor];
        }

        double currentPrice = state.symbol.lastPrice > 0.0
            ? state.symbol.lastPrice
            : (state.symbol.currentPositionQty > 0.0 ? state.symbol.bidPrice : state.symbol.askPrice);
        if (currentPrice > 0.0 && state.symbol.currentPositionAvgCost > 0.0) {
            const double pnl = (currentPrice - state.symbol.currentPositionAvgCost) * state.symbol.currentPositionQty;
            _pnlLabel.stringValue = FormatSignedCurrency(pnl);
            _pnlLabel.textColor = pnl > 0.0 ? [NSColor systemGreenColor]
                                            : (pnl < 0.0 ? [NSColor systemRedColor] : [NSColor secondaryLabelColor]);
        } else {
            _pnlLabel.stringValue = @"--";
            _pnlLabel.textColor = [NSColor secondaryLabelColor];
        }
    } else {
        _positionLabel.stringValue = @"FLAT";
        _positionLabel.textColor = [NSColor secondaryLabelColor];
        _pnlLabel.stringValue = @"--";
        _pnlLabel.textColor = [NSColor secondaryLabelColor];
    }

    const NSString* buySegment = state.buySweepAvailable
        ? [NSString stringWithFormat:@"buy %@ (sweep+%.2f)", FormatPrice(state.buyPrice), _priceBuffer]
        : [NSString stringWithFormat:@"buy %@ (ask+%.2f)", FormatPrice(state.buyPrice), _priceBuffer];
    const NSString* sellSegment = state.sellSweepAvailable
        ? [NSString stringWithFormat:@"sell %@ (sweep-%.2f)", FormatPrice(state.sellPrice), _priceBuffer]
        : [NSString stringWithFormat:@"sell %@ (bid-%.2f)", FormatPrice(state.sellPrice), _priceBuffer];
    _pricePreviewLabel.stringValue = [NSString stringWithFormat:@"Prices: %@  |  %@", buySegment, sellSegment];
    NSString* quoteSegment = state.symbol.quoteAgeMs >= 0.0
        ? [NSString stringWithFormat:@"quote %.0f ms", state.symbol.quoteAgeMs]
        : @"quote waiting";
    NSString* armSegment = ControllerArmSafetyText(state.risk);
    NSString* killSegment = state.status.tradingKillSwitch ? @"kill switch ON" : @"kill switch off";
    NSString* exposureSegment = [NSString stringWithFormat:@"open $%.0f / cap $%.0f",
                                 state.projectedOpenNotional,
                                 state.risk.maxOpenNotional];
    _safetyStatusLabel.stringValue = [NSString stringWithFormat:@"Safety: %@  |  %@  |  %@  |  %@",
                                      quoteSegment, armSegment, killSegment, exposureSegment];
    if (state.status.tradingKillSwitch) {
        _safetyStatusLabel.textColor = [NSColor systemRedColor];
    } else if (!state.symbol.hasFreshQuote) {
        _safetyStatusLabel.textColor = [NSColor systemOrangeColor];
    } else {
        _safetyStatusLabel.textColor = [NSColor secondaryLabelColor];
    }

    SetActionButtonAvailability(_buyButton,
                                state.canBuy,
                                [NSColor colorWithCalibratedRed:0.13 green:0.64 blue:0.32 alpha:1.0],
                                BuyUnavailableReason(state, _subscribed));
    SetActionButtonAvailability(_closeButton,
                                state.canClosePosition,
                                [NSColor colorWithCalibratedRed:0.96 green:0.60 blue:0.18 alpha:1.0],
                                CloseUnavailableReason(state, _subscribed));
    SetActionButtonAvailability(_cancelAllButton,
                                state.hasCancelableOrders,
                                [NSColor colorWithCalibratedRed:0.90 green:0.27 blue:0.22 alpha:1.0],
                                CancelUnavailableReason(state));

    if (state.buyPrice > 0.0) {
        _buyButton.title = [NSString stringWithFormat:@"Buy Limit @ %.2f", state.buyPrice];
    } else {
        _buyButton.title = @"Buy Limit";
    }

    if (state.symbol.availableLongToClose > 0.0 && state.sellPrice > 0.0) {
        _closeButton.title = [NSString stringWithFormat:@"Close Long (%.0f) @ %.2f",
                              state.symbol.availableLongToClose, state.sellPrice];
    } else if (state.symbol.availableLongToClose > 0.0) {
        _closeButton.title = [NSString stringWithFormat:@"Close Long (%.0f)", state.symbol.availableLongToClose];
    } else {
        _closeButton.title = @"Close Long";
    }

    if (!state.status.controllerEnabled) {
        _controllerHintLabel.stringValue = @"Controller input is disabled in Settings.";
    } else {
        _controllerHintLabel.stringValue = @"Controller: Square buy  |  Circle close  |  Triangle cancel all  |  Cross toggle qty";
    }
}

- (void)refreshOrders {
    [_ordersTable reloadData];
    __block bool hasSelectedOrders = false;
    __block bool hasSelectedActiveOrders = false;
    __block bool hasSelectedManualReviewOrders = false;
    NSIndexSet* selectedRows = _ordersTable.selectedRowIndexes;
    [selectedRows enumerateIndexesUsingBlock:^(NSUInteger idx, BOOL* stop) {
        (void)stop;
        if (idx >= _ordersSnapshot.size()) {
            return;
        }
        hasSelectedOrders = true;
        const OrderInfo& order = _ordersSnapshot[idx].second;
        if (!order.isTerminal()) {
            hasSelectedActiveOrders = true;
        }
        if (order.localState == LocalOrderState::NeedsManualReview && !order.manualReviewAcknowledged) {
            hasSelectedManualReviewOrders = true;
        }
    }];

    SetButtonEnabledTint(_cancelSelectedButton,
                         hasSelectedOrders,
                         [NSColor colorWithCalibratedRed:0.90 green:0.27 blue:0.22 alpha:1.0],
                         [NSColor colorWithCalibratedWhite:0.86 alpha:1.0]);
    SetButtonEnabledTint(_reconcileSelectedButton,
                         hasSelectedActiveOrders,
                         [NSColor colorWithCalibratedRed:0.22 green:0.52 blue:0.92 alpha:1.0],
                         [NSColor colorWithCalibratedWhite:0.86 alpha:1.0]);
    SetButtonEnabledTint(_acknowledgeSelectedButton,
                         hasSelectedManualReviewOrders,
                         [NSColor colorWithCalibratedRed:0.78 green:0.45 blue:0.15 alpha:1.0],
                         [NSColor colorWithCalibratedWhite:0.86 alpha:1.0]);
}

- (void)refreshTracePopup {
    if (!_traceItems.empty() && _selectedTraceId == 0) {
        _selectedTraceId = _traceItems.front().traceId;
    }

    if (_traceItems.empty()) {
        [_tracePopup removeAllItems];
        [_tracePopup addItemWithTitle:@"No trade trace yet"];
        _tracePopup.enabled = NO;
        _exportTraceButton.enabled = NO;
        _exportAllButton.enabled = NO;
        return;
    }

    [_tracePopup removeAllItems];
    _tracePopup.enabled = YES;
    NSInteger selectedIndex = 0;
    for (std::size_t i = 0; i < _traceItems.size(); ++i) {
        [_tracePopup addItemWithTitle:ToNSString(_traceItems[i].summary)];
        if (_traceItems[i].traceId == _selectedTraceId) {
            selectedIndex = static_cast<NSInteger>(i);
        }
    }
    [_tracePopup selectItemAtIndex:selectedIndex];
    _selectedTraceId = _traceItems[static_cast<std::size_t>(selectedIndex)].traceId;
}

- (void)refreshMessages {
    _messagesTextView.string = ToNSString(_messagesText);
}

- (NSInteger)numberOfRowsInTableView:(NSTableView*)tableView {
    if (tableView == _ordersTable) {
        return static_cast<NSInteger>(_ordersSnapshot.size());
    }
    return 0;
}

- (NSView*)tableView:(NSTableView*)tableView viewForTableColumn:(NSTableColumn*)tableColumn row:(NSInteger)row {
    if (tableView != _ordersTable || row < 0 || static_cast<std::size_t>(row) >= _ordersSnapshot.size()) {
        return nil;
    }

    NSTextField* label = [tableView makeViewWithIdentifier:tableColumn.identifier owner:self];
    if (label == nil) {
        label = MakeLabel(@"", [NSFont monospacedSystemFontOfSize:12.0 weight:NSFontWeightRegular], [NSColor labelColor]);
        label.identifier = tableColumn.identifier;
    }

    const OrderInfo& order = _ordersSnapshot[static_cast<std::size_t>(row)].second;
    switch (tableColumn.headerCell.tag) {
        case OrderColumnId:
            label.stringValue = [NSString stringWithFormat:@"%lld", static_cast<long long>(_ordersSnapshot[static_cast<std::size_t>(row)].first)];
            label.textColor = [NSColor labelColor];
            break;
        case OrderColumnSymbol:
            label.stringValue = ToNSString(order.symbol);
            label.textColor = [NSColor labelColor];
            break;
        case OrderColumnSide:
            label.stringValue = ToNSString(order.side);
            label.textColor = (order.side == "BUY") ? [NSColor systemGreenColor] : [NSColor systemRedColor];
            break;
        case OrderColumnQty:
            label.stringValue = [NSString stringWithFormat:@"%.0f", order.quantity];
            label.textColor = [NSColor labelColor];
            break;
        case OrderColumnPrice:
            label.stringValue = [NSString stringWithFormat:@"$%.2f", order.avgFillPrice > 0.0 ? order.avgFillPrice : order.limitPrice];
            label.textColor = (order.avgFillPrice > 0.0) ? [NSColor systemOrangeColor] : [NSColor labelColor];
            break;
        case OrderColumnLocalState:
            label.stringValue = ToNSString(formatOrderLocalStateText(order));
            switch (order.localState) {
                case LocalOrderState::AwaitingBrokerEcho:
                case LocalOrderState::AwaitingCancelAck:
                case LocalOrderState::NeedsReconciliation:
                    label.textColor = [NSColor systemOrangeColor];
                    break;
                case LocalOrderState::NeedsManualReview:
                    label.textColor = [NSColor systemRedColor];
                    break;
                case LocalOrderState::Filled:
                    label.textColor = [NSColor systemGreenColor];
                    break;
                case LocalOrderState::Cancelled:
                case LocalOrderState::Rejected:
                case LocalOrderState::Inactive:
                    label.textColor = [NSColor secondaryLabelColor];
                    break;
                default:
                    label.textColor = [NSColor labelColor];
                    break;
            }
            break;
        case OrderColumnStatus:
            label.stringValue = ToNSString(order.status);
            label.textColor = [NSColor labelColor];
            break;
        case OrderColumnWatchdog:
            label.stringValue = ToNSString(formatOrderWatchdogText(order));
            label.textColor = order.localState == LocalOrderState::NeedsManualReview
                ? [NSColor systemRedColor]
                : ((order.localState == LocalOrderState::NeedsReconciliation ||
                    order.localState == LocalOrderState::AwaitingBrokerEcho ||
                    order.localState == LocalOrderState::AwaitingCancelAck ||
                    order.localState == LocalOrderState::PartiallyFilled)
                       ? [NSColor systemOrangeColor]
                       : (order.fillDurationMs >= 0.0 ? [NSColor systemGreenColor]
                                                      : [NSColor secondaryLabelColor]));
            break;
        default:
            label.stringValue = @"";
            break;
    }

    return label;
}

- (void)tableViewSelectionDidChange:(NSNotification*)notification {
    NSTableView* tableView = notification.object;
    if (tableView != _ordersTable) {
        return;
    }

    [self refreshOrders];

    NSInteger selectedRow = _ordersTable.selectedRow;
    if (selectedRow >= 0 && static_cast<std::size_t>(selectedRow) < _ordersSnapshot.size()) {
        const OrderId orderId = _ordersSnapshot[static_cast<std::size_t>(selectedRow)].first;
        const std::uint64_t traceId = (_runtimeHost && _runtimeHost->runtime())
            ? _runtimeHost->runtime()->findTradeTraceIdByOrderId(orderId)
            : findTraceIdByOrderId(orderId);
        if (traceId != 0) {
            _selectedTraceId = traceId;
            [self scheduleRefresh];
        }
    }
}

@end

@interface AppDelegate : NSObject <NSApplicationDelegate>
@property(nonatomic, strong) TradingWindowController* windowController;
@end

@implementation AppDelegate

- (void)applicationDidFinishLaunching:(NSNotification*)notification {
    (void)notification;

    NSMenu* mainMenu = [[NSMenu alloc] initWithTitle:@""];
    NSMenuItem* appMenuItem = [[NSMenuItem alloc] initWithTitle:@"" action:nil keyEquivalent:@""];
    [mainMenu addItem:appMenuItem];

    NSMenu* appMenu = [[NSMenu alloc] initWithTitle:@"TWS Short Trading GUI"];
    NSString* appName = NSProcessInfo.processInfo.processName;
    NSMenuItem* quitItem = [[NSMenuItem alloc] initWithTitle:[NSString stringWithFormat:@"Quit %@", appName]
                                                      action:@selector(terminate:)
                                               keyEquivalent:@"q"];
    [appMenu addItem:quitItem];
    [appMenuItem setSubmenu:appMenu];
    NSApp.mainMenu = mainMenu;

    self.windowController = [[TradingWindowController alloc] init];
    [self.windowController showWindowAndStart];
    [[NSNotificationCenter defaultCenter] addObserver:self
                                             selector:@selector(processThermalStateDidChange:)
                                                 name:NSProcessInfoThermalStateDidChangeNotification
                                               object:nil];
    [self.windowController applyAppActive:NSApp.isActive];
    [self.windowController applyThermalState:NSProcessInfo.processInfo.thermalState];
}

- (BOOL)applicationShouldTerminateAfterLastWindowClosed:(NSApplication*)sender {
    (void)sender;
    return YES;
}

- (void)applicationDidBecomeActive:(NSNotification*)notification {
    (void)notification;
    [self.windowController applyAppActive:YES];
}

- (void)applicationDidResignActive:(NSNotification*)notification {
    (void)notification;
    [self.windowController applyAppActive:NO];
}

- (void)processThermalStateDidChange:(NSNotification*)notification {
    (void)notification;
    [self.windowController applyThermalState:NSProcessInfo.processInfo.thermalState];
}

- (void)applicationWillTerminate:(NSNotification*)notification {
    (void)notification;
    [[NSNotificationCenter defaultCenter] removeObserver:self
                                                    name:NSProcessInfoThermalStateDidChangeNotification
                                                  object:nil];
    [self.windowController shutdownRuntime];
}

@end

int main(int argc, const char* argv[]) {
    (void)argc;
    (void)argv;

    @autoreleasepool {
        NSApplication* application = [NSApplication sharedApplication];
        application.activationPolicy = NSApplicationActivationPolicyRegular;

        AppDelegate* delegate = [[AppDelegate alloc] init];
        application.delegate = delegate;
        [application run];
    }

    return 0;
}
