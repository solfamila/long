#import <AppKit/AppKit.h>
#import <Security/Security.h>

#include "app_shared.h"
#include "trace_exporter.h"
#include "trading_actions.h"
#include "trading_runtime.h"
#include "trading_view_model.h"
#include "trading_wrapper.h"

namespace {

NSString* const kWebSocketTokenService = @"com.foxy.twstradinggui";
NSString* const kWebSocketTokenAccount = @"websocket-token";

NSString* ToNSString(const std::string& value) {
    return [NSString stringWithUTF8String:value.c_str()];
}

std::string ToStdString(NSString* value) {
    if (value == nil) {
        return {};
    }
    return std::string([value UTF8String]);
}

std::string LoadKeychainString(NSString* service, NSString* account) {
    NSDictionary* query = @{
        (__bridge id)kSecClass: (__bridge id)kSecClassGenericPassword,
        (__bridge id)kSecAttrService: service,
        (__bridge id)kSecAttrAccount: account,
        (__bridge id)kSecReturnData: @YES,
        (__bridge id)kSecMatchLimit: (__bridge id)kSecMatchLimitOne,
    };

    CFTypeRef result = nullptr;
    const OSStatus status = SecItemCopyMatching((__bridge CFDictionaryRef)query, &result);
    if (status != errSecSuccess || result == nullptr) {
        return {};
    }

    NSData* data = CFBridgingRelease(result);
    NSString* value = [[NSString alloc] initWithData:data encoding:NSUTF8StringEncoding];
    return ToStdString(value);
}

void SaveKeychainString(NSString* service, NSString* account, const std::string& value) {
    if (value.empty()) {
        return;
    }

    NSData* data = [NSData dataWithBytes:value.data() length:value.size()];
    NSDictionary* query = @{
        (__bridge id)kSecClass: (__bridge id)kSecClassGenericPassword,
        (__bridge id)kSecAttrService: service,
        (__bridge id)kSecAttrAccount: account,
    };
    NSDictionary* update = @{
        (__bridge id)kSecValueData: data,
    };

    OSStatus status = SecItemUpdate((__bridge CFDictionaryRef)query, (__bridge CFDictionaryRef)update);
    if (status == errSecItemNotFound) {
        NSMutableDictionary* add = [query mutableCopy];
        add[(__bridge id)kSecValueData] = data;
        status = SecItemAdd((__bridge CFDictionaryRef)add, nullptr);
    }
    (void)status;
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

bool WriteStringToURL(const std::string& content, NSURL* url, NSError** error) {
    NSString* text = [[NSString alloc] initWithBytes:content.data()
                                              length:content.size()
                                            encoding:NSUTF8StringEncoding];
    if (text == nil) {
        if (error != nullptr) {
            *error = [NSError errorWithDomain:NSCocoaErrorDomain
                                         code:NSFileWriteInapplicableStringEncodingError
                                     userInfo:@{NSLocalizedDescriptionKey: @"Failed to encode UTF-8 text"}];
        }
        return false;
    }
    return [text writeToURL:url atomically:YES encoding:NSUTF8StringEncoding error:error];
}

void ShowAlert(NSWindow* window, NSString* title, NSString* message, NSAlertStyle style) {
    NSAlert* alert = [[NSAlert alloc] init];
    alert.messageText = title ?: @"Notice";
    alert.informativeText = message ?: @"";
    alert.alertStyle = style;
    [alert beginSheetModalForWindow:window completionHandler:nil];
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

std::string FormatOrderTiming(const OrderInfo& order) {
    std::ostringstream oss;
    if (order.fillDurationMs >= 0.0) {
        if (order.fillDurationMs >= 1000.0) {
            oss << std::fixed << std::setprecision(2) << (order.fillDurationMs / 1000.0) << " s";
        } else {
            oss << std::fixed << std::setprecision(0) << order.fillDurationMs << " ms";
        }
    } else if (order.submitTime.time_since_epoch().count() > 0 && !order.isTerminal()) {
        const auto elapsed = std::chrono::steady_clock::now() - order.submitTime;
        const double elapsedMs = std::chrono::duration<double, std::milli>(elapsed).count();
        if (elapsedMs >= 1000.0) {
            oss << std::fixed << std::setprecision(1) << (elapsedMs / 1000.0) << " s...";
        } else {
            oss << std::fixed << std::setprecision(0) << elapsedMs << " ms...";
        }
    } else {
        oss << "--";
    }
    return oss.str();
}

void AppendLatencyLine(std::ostringstream& oss, const char* label, double valueMs) {
    oss << label << ": ";
    if (valueMs >= 0.0) {
        if (valueMs >= 1000.0) {
            oss << std::fixed << std::setprecision(3) << (valueMs / 1000.0) << " s";
        } else {
            oss << std::fixed << std::setprecision(1) << valueMs << " ms";
        }
    } else {
        oss << "--";
    }
    oss << '\n';
}

std::string FormatTraceDetails(const TradeTraceSnapshot& snapshot) {
    if (!snapshot.found) {
        return "No trade trace selected.";
    }

    const TradeTrace& trace = snapshot.trace;
    std::ostringstream oss;
    oss << "Trace " << trace.traceId;
    if (trace.orderId > 0) {
        oss << "  |  Order " << static_cast<long long>(trace.orderId);
    } else {
        oss << "  |  No order ID assigned";
    }
    if (trace.permId > 0) {
        oss << "  |  PermId " << trace.permId;
    }
    oss << "\n\n";

    oss << "Source: " << (trace.source.empty() ? "<unknown>" : trace.source) << '\n';
    oss << "Symbol: " << (trace.symbol.empty() ? "<none>" : trace.symbol) << '\n';
    oss << "Side: " << (trace.side.empty() ? "<none>" : trace.side) << '\n';
    oss << "Requested: " << trace.requestedQty << " @ " << std::fixed << std::setprecision(2) << trace.limitPrice << '\n';
    oss << "Close-only: " << (trace.closeOnly ? "yes" : "no") << '\n';
    if (!trace.latestStatus.empty()) {
        oss << "Latest status: " << trace.latestStatus << '\n';
    }
    if (!trace.terminalStatus.empty()) {
        oss << "Terminal status: " << trace.terminalStatus << '\n';
    }
    if (!trace.latestError.empty()) {
        oss << "Latest error: " << trace.latestError << '\n';
    }
    oss << '\n';

    oss << "Decision snapshot\n";
    oss << "  bid=" << std::fixed << std::setprecision(2) << trace.decisionBid
        << " ask=" << trace.decisionAsk
        << " last=" << trace.decisionLast
        << " sweep=" << trace.sweepEstimate
        << " buffer=" << trace.priceBuffer << '\n';
    if (!trace.bookSummary.empty()) {
        oss << "  book: " << trace.bookSummary << '\n';
    }
    if (!trace.notes.empty()) {
        oss << "  notes: " << trace.notes << '\n';
    }
    oss << '\n';

    oss << "Latency breakdown\n";
    AppendLatencyLine(oss, "Validation", durationMs(trace.validationStartMono, trace.validationEndMono));
    AppendLatencyLine(oss, "Trigger -> placeOrder return", durationMs(trace.triggerMono, trace.placeCallEndMono));
    AppendLatencyLine(oss, "placeOrder return -> openOrder", durationMs(trace.placeCallEndMono, trace.firstOpenOrderMono));
    AppendLatencyLine(oss, "placeOrder return -> first orderStatus", durationMs(trace.placeCallEndMono, trace.firstStatusMono));
    AppendLatencyLine(oss, "placeOrder return -> first exec", durationMs(trace.placeCallEndMono, trace.firstExecMono));
    AppendLatencyLine(oss, "Trigger -> first exec", durationMs(trace.triggerMono, trace.firstExecMono));
    AppendLatencyLine(oss, "First exec -> full fill", durationMs(trace.firstExecMono, trace.fullFillMono));
    AppendLatencyLine(oss, "Trigger -> full fill", durationMs(trace.triggerMono, trace.fullFillMono));
    oss << '\n';

    if (!trace.fills.empty()) {
        oss << "Fill slices\n";
        for (const auto& fill : trace.fills) {
            oss << "  " << fill.execId
                << "  shares=" << fill.shares
                << "  price=" << std::fixed << std::setprecision(2) << fill.price
                << "  cum=" << fill.cumQty
                << "  exch=" << fill.exchange;
            if (fill.commissionKnown) {
                oss << "  commission=" << std::fixed << std::setprecision(4)
                    << fill.commission << ' ' << fill.commissionCurrency;
            }
            oss << '\n';
        }
        if (!trace.commissionCurrency.empty()) {
            oss << "  total commission=" << std::fixed << std::setprecision(4)
                << trace.totalCommission << ' ' << trace.commissionCurrency << '\n';
        }
        oss << '\n';
    }

    oss << "Timeline\n";
    for (const auto& event : trace.events) {
        oss << "  " << formatWallTime(event.wallTs) << "  ";
        const double sinceTriggerMs = durationMs(trace.triggerMono, event.monoTs);
        if (sinceTriggerMs >= 0.0) {
            oss << std::fixed << std::setprecision(1) << sinceTriggerMs << " ms  ";
        } else {
            oss << "--  ";
        }
        oss << event.stage << "  " << event.details << '\n';
    }

    return oss.str();
}

bool TraceItemsEqual(const std::vector<TradeTraceListItem>& lhs,
                     const std::vector<TradeTraceListItem>& rhs) {
    if (lhs.size() != rhs.size()) {
        return false;
    }
    for (std::size_t i = 0; i < lhs.size(); ++i) {
        if (lhs[i].traceId != rhs[i].traceId || lhs[i].summary != rhs[i].summary) {
            return false;
        }
    }
    return true;
}

enum {
    OrderColumnId = 1,
    OrderColumnSymbol,
    OrderColumnSide,
    OrderColumnQty,
    OrderColumnPrice,
    OrderColumnStatus,
    OrderColumnTime
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
    std::unique_ptr<TradingRuntime> _runtime;

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

    bool _refreshScheduled;
    bool _traceItemsFromReplayLog;

    NSTextField* _twsStatusLabel;
    NSTextField* _accountStatusLabel;
    NSTextField* _websocketStatusLabel;
    NSTextField* _controllerStatusLabel;
    NSTextField* _controllerLockLabel;
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
- (void)loadPreferences;
- (void)persistPreferences;

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
    window.title = @"TWS Trading GUI";
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
        _refreshScheduled = false;
        _traceItemsFromReplayLog = false;
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

    RuntimeConnectionConfig connection = captureRuntimeConnectionConfig();
    if (NSString* host = [defaults stringForKey:@"twsHost"]) {
        connection.host = ToStdString(host);
    }
    if ([defaults objectForKey:@"twsPort"] != nil) {
        connection.port = std::max(1, static_cast<int>([defaults integerForKey:@"twsPort"]));
    }
    if ([defaults objectForKey:@"twsClientId"] != nil) {
        connection.clientId = std::max(1, static_cast<int>([defaults integerForKey:@"twsClientId"]));
    }
    const std::string keychainToken = LoadKeychainString(kWebSocketTokenService, kWebSocketTokenAccount);
    if (!keychainToken.empty()) {
        connection.websocketAuthToken = keychainToken;
    } else if (NSString* token = [defaults stringForKey:@"websocketToken"]) {
        connection.websocketAuthToken = ToStdString(token);
        SaveKeychainString(kWebSocketTokenService, kWebSocketTokenAccount, connection.websocketAuthToken);
        [defaults removeObjectForKey:@"websocketToken"];
    }
    if ([defaults objectForKey:@"websocketEnabled"] != nil) {
        connection.websocketEnabled = [defaults boolForKey:@"websocketEnabled"];
    }
    if ([defaults objectForKey:@"controllerEnabled"] != nil) {
        connection.controllerEnabled = [defaults boolForKey:@"controllerEnabled"];
    }
    updateRuntimeConnectionConfig(connection);

    RiskControlsSnapshot risk = captureRiskControlsSnapshot();
    if ([defaults objectForKey:@"staleQuoteThresholdMs"] != nil) {
        risk.staleQuoteThresholdMs = std::max(250, static_cast<int>([defaults integerForKey:@"staleQuoteThresholdMs"]));
    }
    if ([defaults objectForKey:@"maxOrderNotional"] != nil) {
        risk.maxOrderNotional = std::max(100.0, [defaults doubleForKey:@"maxOrderNotional"]);
    }
    if ([defaults objectForKey:@"maxOpenNotional"] != nil) {
        risk.maxOpenNotional = std::max(risk.maxOrderNotional, [defaults doubleForKey:@"maxOpenNotional"]);
    }
    updateRiskControls(risk.staleQuoteThresholdMs, risk.maxOrderNotional, risk.maxOpenNotional);

    NSString* ensuredToken = ToNSString(ensureWebSocketAuthToken());
    SaveKeychainString(kWebSocketTokenService, kWebSocketTokenAccount, ToStdString(ensuredToken));
    [defaults removeObjectForKey:@"websocketToken"];
}

- (void)persistPreferences {
    NSUserDefaults* defaults = [NSUserDefaults standardUserDefaults];
    [defaults setObject:ToNSString(_symbolInput) forKey:@"defaultSymbol"];
    [defaults setInteger:_quantityInput forKey:@"quantityInput"];
    [defaults setDouble:_priceBuffer forKey:@"priceBuffer"];
    [defaults setDouble:_maxPositionDollars forKey:@"maxPositionDollars"];

    const RuntimeConnectionConfig connection = captureRuntimeConnectionConfig();
    [defaults setObject:ToNSString(connection.host) forKey:@"twsHost"];
    [defaults setInteger:connection.port forKey:@"twsPort"];
    [defaults setInteger:connection.clientId forKey:@"twsClientId"];
    SaveKeychainString(kWebSocketTokenService, kWebSocketTokenAccount, connection.websocketAuthToken);
    [defaults removeObjectForKey:@"websocketToken"];
    [defaults setBool:connection.websocketEnabled forKey:@"websocketEnabled"];
    [defaults setBool:connection.controllerEnabled forKey:@"controllerEnabled"];

    const RiskControlsSnapshot risk = captureRiskControlsSnapshot();
    [defaults setInteger:risk.staleQuoteThresholdMs forKey:@"staleQuoteThresholdMs"];
    [defaults setDouble:risk.maxOrderNotional forKey:@"maxOrderNotional"];
    [defaults setDouble:risk.maxOpenNotional forKey:@"maxOpenNotional"];
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
    statusRow.distribution = NSStackViewDistributionFillProportionally;
    _twsStatusLabel = MakeLabel(@"TWS: Disconnected", [NSFont systemFontOfSize:13.0 weight:NSFontWeightSemibold], [NSColor systemRedColor]);
    _accountStatusLabel = MakeLabel(@"Account: --", [NSFont systemFontOfSize:13.0 weight:NSFontWeightMedium], [NSColor labelColor]);
    _websocketStatusLabel = MakeLabel(@"WS: Not running", [NSFont systemFontOfSize:13.0 weight:NSFontWeightMedium], [NSColor secondaryLabelColor]);
    _controllerStatusLabel = MakeLabel(@"Controller: Not found", [NSFont systemFontOfSize:13.0 weight:NSFontWeightMedium], [NSColor secondaryLabelColor]);
    _controllerLockLabel = MakeLabel(@"Controller Lock: Waiting to lock", [NSFont systemFontOfSize:13.0 weight:NSFontWeightMedium], [NSColor secondaryLabelColor]);
    [statusRow addArrangedSubview:_twsStatusLabel];
    [statusRow addArrangedSubview:_accountStatusLabel];
    [statusRow addArrangedSubview:_websocketStatusLabel];
    [statusRow addArrangedSubview:_controllerStatusLabel];
    [statusRow addArrangedSubview:_controllerLockLabel];
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

    NSStackView* safetyRow = MakeRowStack();
    _armControllerButton = MakeButton(@"Arm Controller", self, @selector(toggleControllerArmed:));
    [_armControllerButton.widthAnchor constraintEqualToConstant:170.0].active = YES;
    _killSwitchButton = MakeButton(@"Enable Kill Switch", self, @selector(toggleKillSwitch:));
    [_killSwitchButton.widthAnchor constraintEqualToConstant:190.0].active = YES;
    _settingsButton = MakeButton(@"Settings", self, @selector(openSettings:));
    [_settingsButton.widthAnchor constraintEqualToConstant:120.0].active = YES;
    [safetyRow addArrangedSubview:_armControllerButton];
    [safetyRow addArrangedSubview:_killSwitchButton];
    [safetyRow addArrangedSubview:_settingsButton];
    [leftStack addArrangedSubview:safetyRow];
    [safetyRow.widthAnchor constraintEqualToAnchor:leftStack.widthAnchor].active = YES;

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
        @{@"id": @"status", @"title": @"Status", @"width": @110, @"tag": @(OrderColumnStatus)},
        @{@"id": @"time", @"title": @"Time", @"width": @100, @"tag": @(OrderColumnTime)},
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
    [_cancelSelectedButton.widthAnchor constraintEqualToConstant:160.0].active = YES;
    [orderButtons addArrangedSubview:_cancelSelectedButton];
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
    if (_runtime) {
        return;
    }

    syncSharedGuiInputs(_quantityInput, _priceBuffer, _maxPositionDollars);
    _runtime = std::make_unique<TradingRuntime>();

    __weak TradingWindowController* weakSelf = self;
    _runtime->setUiInvalidationCallback([weakSelf]() {
        dispatch_async(dispatch_get_main_queue(), ^{
            TradingWindowController* strongSelf = weakSelf;
            if (strongSelf != nil) {
                [strongSelf scheduleRefresh];
            }
        });
    });
    _runtime->setControllerActionCallback([weakSelf](TradingRuntimeControllerAction action) {
        dispatch_async(dispatch_get_main_queue(), ^{
            TradingWindowController* strongSelf = weakSelf;
            if (strongSelf != nil) {
                [strongSelf handleControllerAction:action];
            }
        });
    });
    _runtime->start();
    [self scheduleRefresh];
}

- (void)shutdownRuntime {
    if (_shuttingDown) {
        return;
    }
    _shuttingDown = true;
    _refreshScheduled = false;
    if (_runtime) {
        _runtime->shutdown();
        _runtime.reset();
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

    if (_refreshScheduled) {
        return;
    }

    _refreshScheduled = true;
    __weak TradingWindowController* weakSelf = self;
    dispatch_async(dispatch_get_main_queue(), ^{
        TradingWindowController* strongSelf = weakSelf;
        if (strongSelf == nil) {
            return;
        }
        strongSelf->_refreshScheduled = false;
        if (strongSelf->_shuttingDown) {
            return;
        }
        [strongSelf refreshInterface];
    });
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

- (void)syncInputsFromFields {
    const int parsedQuantity = _quantityField.intValue;
    _quantityInput = std::max(1, parsedQuantity);
    _priceBuffer = std::max(0.0, _bufferField.doubleValue);
    _maxPositionDollars = std::max(1000.0, _maxPositionField.doubleValue);
    syncSharedGuiInputs(_quantityInput, _priceBuffer, _maxPositionDollars);
    [self persistPreferences];
    [self updateInputFieldsFromState];
}

- (void)updateInputFieldsFromState {
    _quantityField.stringValue = [NSString stringWithFormat:@"%d", _quantityInput];
    _bufferField.stringValue = [NSString stringWithFormat:@"%.2f", _priceBuffer];
    _maxPositionField.stringValue = [NSString stringWithFormat:@"%.0f", _maxPositionDollars];
    if (!_symbolInput.empty()) {
        _symbolField.stringValue = ToNSString(_symbolInput);
    }
}

- (void)subscribeAction:(id)sender {
    (void)sender;
    [self syncInputsFromFields];
    std::string requestedSymbol;
    std::string error;
    if (!requestSubscriptionAction(_runtime.get(),
                                   ToStdString(_symbolField.stringValue),
                                   false,
                                   &requestedSymbol,
                                   &error)) {
        g_data.addMessage("Subscribe failed: " + error);
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
    const TradingPanelState state = buildTradingPanelState(_subscribedSymbol, _subscribed, _quantityInput, _priceBuffer, _maxPositionDollars);
    const TradingSubmitResult result = submitBuyAction(_runtime.get(),
                                                       state,
                                                       _subscribedSymbol,
                                                       _quantityInput,
                                                       _priceBuffer,
                                                       "GUI Button",
                                                       "Buy Limit button pressed");
    if (!result.submitted && !result.error.empty()) {
        g_data.addMessage("Buy failed: " + result.error);
    }
    if (result.traceId != 0) {
        _selectedTraceId = result.traceId;
    }
    [self scheduleRefresh];
}

- (void)closeAction:(id)sender {
    (void)sender;
    [self syncInputsFromFields];
    const TradingPanelState state = buildTradingPanelState(_subscribedSymbol, _subscribed, _quantityInput, _priceBuffer, _maxPositionDollars);
    const TradingSubmitResult result = submitCloseAction(_runtime.get(),
                                                         state,
                                                         _subscribedSymbol,
                                                         _priceBuffer,
                                                         "GUI Button",
                                                         "Close Long button pressed");
    if (!result.submitted && !result.error.empty()) {
        g_data.addMessage("Close failed: " + result.error);
    }
    if (result.traceId != 0) {
        _selectedTraceId = result.traceId;
    }
    [self scheduleRefresh];
}

- (void)cancelSelectedAction:(id)sender {
    (void)sender;
    if (!_runtime) {
        return;
    }

    NSIndexSet* selectedRows = _ordersTable.selectedRowIndexes;
    if (selectedRows.count == 0) {
        g_data.addMessage("No orders selected for cancellation");
        [self scheduleRefresh];
        return;
    }

    __block std::vector<OrderId> selectedOrderIds;
    [selectedRows enumerateIndexesUsingBlock:^(NSUInteger idx, BOOL* stop) {
        (void)stop;
        if (idx < _ordersSnapshot.size()) {
            selectedOrderIds.push_back(_ordersSnapshot[idx].first);
        }
    }];

    const TradingCancelResult result = cancelSelectedOrdersAction(_runtime.get(), selectedOrderIds);
    for (std::size_t i = 0; i < result.orderIds.size(); ++i) {
        if (i < result.sent.size() && result.sent[i]) {
            g_data.addMessage("Cancel request sent for order " + std::to_string(result.orderIds[i]));
        } else {
            g_data.addMessage("Cancel failed (not connected) for order " + std::to_string(result.orderIds[i]));
        }
    }
    [self scheduleRefresh];
}

- (void)cancelAllAction:(id)sender {
    (void)sender;
    if (!_runtime) {
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

    const TradingCancelResult result = cancelAllOrdersAction(_runtime.get());
    if (result.orderIds.empty()) {
        g_data.addMessage("No pending orders to cancel");
        [self scheduleRefresh];
        return;
    }

    const int sentCount = static_cast<int>(std::count(result.sent.begin(), result.sent.end(), true));
    if (sentCount != static_cast<int>(result.orderIds.size())) {
        g_data.addMessage("Some cancel requests could not be sent");
    }
    g_data.addMessage("Cancel requested for " + std::to_string(sentCount) + " order(s)");
    [self scheduleRefresh];
}

- (void)toggleControllerArmed:(id)sender {
    (void)sender;
    const bool armed = !captureRiskControlsSnapshot().controllerArmed;
    setControllerArmed(armed);
    g_data.addMessage(armed ? "Controller trading armed" : "Controller trading disarmed");
    [self scheduleRefresh];
}

- (void)toggleKillSwitch:(id)sender {
    (void)sender;
    const bool enabled = !captureRiskControlsSnapshot().tradingKillSwitch;
    setTradingKillSwitch(enabled);
    if (enabled) {
        setControllerArmed(false);
    }
    g_data.addMessage(enabled ? "Kill switch enabled: trading halted" : "Kill switch disabled: trading may resume");
    [self scheduleRefresh];
}

- (void)openSettings:(id)sender {
    (void)sender;

    const RuntimeConnectionConfig currentConnection = captureRuntimeConnectionConfig();
    const RiskControlsSnapshot currentRisk = captureRiskControlsSnapshot();

    NSAlert* alert = [[NSAlert alloc] init];
    alert.messageText = @"Trading Settings";
    alert.informativeText = @"Connection changes apply on next launch. Risk and WebSocket token changes apply immediately.";
    [alert addButtonWithTitle:@"Save"];
    [alert addButtonWithTitle:@"Cancel"];

    NSStackView* accessory = MakeColumnStack();
    accessory.spacing = 10.0;
    accessory.translatesAutoresizingMaskIntoConstraints = NO;

    auto addRow = ^(NSString* title, NSTextField* field) {
        NSStackView* row = MakeRowStack();
        NSTextField* label = MakeLabel(title, [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold], [NSColor secondaryLabelColor]);
        [label.widthAnchor constraintEqualToConstant:170.0].active = YES;
        [row addArrangedSubview:label];
        [row addArrangedSubview:field];
        [accessory addArrangedSubview:row];
        [row.widthAnchor constraintEqualToAnchor:accessory.widthAnchor].active = YES;
    };

    NSTextField* hostField = MakeInputField(ToNSString(currentConnection.host), 250.0, nil, nil, nil);
    NSTextField* portField = MakeInputField([NSString stringWithFormat:@"%d", currentConnection.port], 90.0, nil, nil, nil);
    NSTextField* clientIdField = MakeInputField([NSString stringWithFormat:@"%d", currentConnection.clientId], 90.0, nil, nil, nil);
    NSTextField* tokenField = MakeInputField(ToNSString(currentConnection.websocketAuthToken), 300.0, nil, nil, nil);
    NSTextField* staleQuoteField = MakeInputField([NSString stringWithFormat:@"%d", currentRisk.staleQuoteThresholdMs], 90.0, nil, nil, nil);
    NSTextField* maxOrderField = MakeInputField([NSString stringWithFormat:@"%.0f", currentRisk.maxOrderNotional], 110.0, nil, nil, nil);
    NSTextField* maxOpenField = MakeInputField([NSString stringWithFormat:@"%.0f", currentRisk.maxOpenNotional], 110.0, nil, nil, nil);
    NSButton* websocketCheckbox = [NSButton checkboxWithTitle:@"Enable localhost WebSocket automation" target:nil action:nil];
    websocketCheckbox.state = currentConnection.websocketEnabled ? NSControlStateValueOn : NSControlStateValueOff;
    NSButton* controllerCheckbox = [NSButton checkboxWithTitle:@"Enable controller input" target:nil action:nil];
    controllerCheckbox.state = currentConnection.controllerEnabled ? NSControlStateValueOn : NSControlStateValueOff;

    addRow(@"TWS Host", hostField);
    addRow(@"TWS Port", portField);
    addRow(@"TWS Client ID", clientIdField);
    addRow(@"WebSocket Token", tokenField);
    addRow(@"Stale Quote (ms)", staleQuoteField);
    addRow(@"Max Order Notional", maxOrderField);
    addRow(@"Max Open Notional", maxOpenField);
    [accessory addArrangedSubview:websocketCheckbox];
    [accessory addArrangedSubview:controllerCheckbox];
    [accessory.widthAnchor constraintEqualToConstant:520.0].active = YES;

    alert.accessoryView = accessory;

    if ([alert runModal] != NSAlertFirstButtonReturn) {
        return;
    }

    RuntimeConnectionConfig updatedConnection = currentConnection;
    updatedConnection.host = ToStdString(hostField.stringValue);
    updatedConnection.port = std::max(1, portField.intValue);
    updatedConnection.clientId = std::max(1, clientIdField.intValue);
    updatedConnection.websocketAuthToken = ToStdString(tokenField.stringValue);
    updatedConnection.websocketEnabled = (websocketCheckbox.state == NSControlStateValueOn);
    updatedConnection.controllerEnabled = (controllerCheckbox.state == NSControlStateValueOn);

    updateRuntimeConnectionConfig(updatedConnection);
    const std::string ensuredToken = ensureWebSocketAuthToken();
    updatedConnection.websocketAuthToken = ensuredToken;
    updateRuntimeConnectionConfig(updatedConnection);

    updateRiskControls(std::max(250, staleQuoteField.intValue),
                       std::max(100.0, maxOrderField.doubleValue),
                       std::max(maxOrderField.doubleValue, maxOpenField.doubleValue));

    [self persistPreferences];
    g_data.addMessage("Settings saved");
    if (updatedConnection.host != currentConnection.host ||
        updatedConnection.port != currentConnection.port ||
        updatedConnection.clientId != currentConnection.clientId ||
        updatedConnection.websocketEnabled != currentConnection.websocketEnabled ||
        updatedConnection.controllerEnabled != currentConnection.controllerEnabled) {
        g_data.addMessage("Restart the app to apply new connection/device settings");
    }
    [self scheduleRefresh];
}

- (void)exportSelectedTrace:(id)sender {
    (void)sender;
    if (_selectedTraceId == 0) {
        ShowAlert(self.window, @"No Trace Selected", @"Select a trade trace before exporting.", NSAlertStyleWarning);
        return;
    }

    TraceExportBundle bundle;
    std::string error;
    if (!buildTraceExportBundle(_selectedTraceId, &bundle, &error)) {
        ShowAlert(self.window, @"Export Failed", ToNSString(error), NSAlertStyleCritical);
        return;
    }

    NSOpenPanel* panel = [NSOpenPanel openPanel];
    panel.canChooseDirectories = YES;
    panel.canChooseFiles = NO;
    panel.canCreateDirectories = YES;
    panel.prompt = @"Export";
    panel.message = @"Choose a folder for the selected trace export.";
    if ([panel runModal] != NSModalResponseOK) {
        return;
    }

    NSURL* directoryURL = panel.URL;
    NSError* writeError = nil;
    const std::string base = bundle.baseName;
    const bool wroteReport = WriteStringToURL(bundle.reportText,
        [directoryURL URLByAppendingPathComponent:ToNSString(base + "-report.txt")], &writeError);
    const bool wroteSummary = wroteReport && WriteStringToURL(bundle.summaryCsv,
        [directoryURL URLByAppendingPathComponent:ToNSString(base + "-summary.csv")], &writeError);
    const bool wroteFills = wroteSummary && WriteStringToURL(bundle.fillsCsv,
        [directoryURL URLByAppendingPathComponent:ToNSString(base + "-fills.csv")], &writeError);
    const bool wroteTimeline = wroteFills && WriteStringToURL(bundle.timelineCsv,
        [directoryURL URLByAppendingPathComponent:ToNSString(base + "-timeline.csv")], &writeError);

    if (!wroteTimeline) {
        ShowAlert(self.window, @"Export Failed", writeError.localizedDescription ?: @"Failed to write export files.", NSAlertStyleCritical);
        return;
    }

    g_data.addMessage("Exported trace bundle to " + ToStdString(directoryURL.path));
    [self scheduleRefresh];
}

- (void)exportAllTracesSummary:(id)sender {
    (void)sender;
    NSSavePanel* panel = [NSSavePanel savePanel];
    panel.nameFieldStringValue = @"all-trades-summary.csv";
    panel.prompt = @"Save CSV";
    if ([panel runModal] != NSModalResponseOK) {
        return;
    }

    NSError* writeError = nil;
    const std::string csv = buildAllTradesSummaryCsv();
    if (!WriteStringToURL(csv, panel.URL, &writeError)) {
        ShowAlert(self.window, @"Export Failed", writeError.localizedDescription ?: @"Failed to save CSV.", NSAlertStyleCritical);
        return;
    }

    g_data.addMessage("Exported trade summary CSV to " + ToStdString(panel.URL.path));
    [self scheduleRefresh];
}

- (void)traceSelectionChanged:(id)sender {
    NSInteger selectedIndex = _tracePopup.indexOfSelectedItem;
    if (selectedIndex >= 0 && static_cast<std::size_t>(selectedIndex) < _traceItems.size()) {
        _selectedTraceId = _traceItems[static_cast<std::size_t>(selectedIndex)].traceId;
    }
    [self refreshTraceDetails];
}

- (void)handleControllerAction:(TradingRuntimeControllerAction)action {
    if (_shuttingDown) {
        return;
    }

    const TradingPanelState state = buildTradingPanelState(_subscribedSymbol, _subscribed, _quantityInput, _priceBuffer, _maxPositionDollars);
    const TradingControllerActionResult result = handleControllerActionIntent(_runtime.get(),
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
        g_data.addMessage(message);
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
    input.messagesVersionSeen = _messagesVersionSeen;
    input.messagesText = _messagesText;
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
    _traceItemsFromReplayLog = model.traceItemsFromReplayLog;
    [self updateInputFieldsFromState];

    if (_runtime) {
        _runtime->setControllerVibration(model.shouldVibrate);
    }
    [self refreshStatusLabels:model.panel];
    [self refreshMarketSection:model.panel];
    [self refreshOrders];
    [self refreshTracePopup];
    [self refreshTraceDetails];
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

    if (!state.status.controllerEnabled) {
        _controllerStatusLabel.stringValue = @"Controller: Disabled";
        _controllerStatusLabel.textColor = [NSColor secondaryLabelColor];
    } else if (state.status.controllerConnected) {
        const std::string label = state.status.controllerDeviceName.empty()
            ? std::string("Connected")
            : state.status.controllerDeviceName;
        _controllerStatusLabel.stringValue = [NSString stringWithFormat:@"Controller: %@", ToNSString(label)];
        _controllerStatusLabel.textColor = [NSColor systemGreenColor];
    } else {
        _controllerStatusLabel.stringValue = @"Controller: Not found";
        _controllerStatusLabel.textColor = [NSColor secondaryLabelColor];
    }

    if (!state.status.controllerLockedDeviceName.empty()) {
        _controllerLockLabel.stringValue = [NSString stringWithFormat:@"Controller Lock: %@", ToNSString(state.status.controllerLockedDeviceName)];
        _controllerLockLabel.textColor = state.status.controllerConnected ? [NSColor systemBlueColor] : [NSColor systemOrangeColor];
    } else {
        _controllerLockLabel.stringValue = @"Controller Lock: Waiting to lock";
        _controllerLockLabel.textColor = [NSColor secondaryLabelColor];
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
    NSString* armSegment = state.status.controllerArmed ? @"controller armed" : @"controller disarmed";
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

    SetButtonEnabledTint(_buyButton,
                         state.canBuy,
                         [NSColor colorWithCalibratedRed:0.13 green:0.64 blue:0.32 alpha:1.0],
                         [NSColor colorWithCalibratedWhite:0.86 alpha:1.0]);
    SetButtonEnabledTint(_closeButton,
                         state.canClosePosition,
                         [NSColor colorWithCalibratedRed:0.96 green:0.60 blue:0.18 alpha:1.0],
                         [NSColor colorWithCalibratedWhite:0.86 alpha:1.0]);
    SetButtonEnabledTint(_cancelAllButton,
                         state.hasCancelableOrders,
                         [NSColor colorWithCalibratedRed:0.90 green:0.27 blue:0.22 alpha:1.0],
                         [NSColor colorWithCalibratedWhite:0.86 alpha:1.0]);

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
    SetButtonEnabledTint(_cancelSelectedButton,
                         (_ordersTable.numberOfSelectedRows > 0),
                         [NSColor colorWithCalibratedRed:0.90 green:0.27 blue:0.22 alpha:1.0],
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
    _exportTraceButton.enabled = (_selectedTraceId != 0 && !_traceItems.empty());
    _exportAllButton.enabled = !_traceItems.empty();
}

- (void)refreshTraceDetails {
    if (_traceItems.empty()) {
        _traceTextView.string = @"No trade trace yet.";
        return;
    }

    if (_selectedTraceId == 0) {
        _selectedTraceId = _traceItems.front().traceId;
    }

    TradeTraceSnapshot snapshot = captureTradeTraceSnapshot(_selectedTraceId);
    if (!snapshot.found) {
        std::string replayError;
        replayTradeTraceSnapshotFromLog(_selectedTraceId, &snapshot, &replayError);
        if (!snapshot.found && _traceItemsFromReplayLog) {
            _traceTextView.string = ToNSString(replayError.empty() ? "Replay trace not available." : replayError);
            return;
        }
    }
    _traceTextView.string = ToNSString(FormatTraceDetails(snapshot));
}

- (void)refreshMessages {
    g_data.copyMessagesTextIfChanged(_messagesText, _messagesVersionSeen);
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
        case OrderColumnStatus:
            label.stringValue = ToNSString(order.status);
            label.textColor = [NSColor labelColor];
            break;
        case OrderColumnTime:
            label.stringValue = ToNSString(FormatOrderTiming(order));
            label.textColor = order.fillDurationMs >= 0.0 ? [NSColor systemGreenColor]
                                                          : (!order.isTerminal() ? [NSColor systemOrangeColor] : [NSColor secondaryLabelColor]);
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

    _cancelSelectedButton.enabled = (_ordersTable.numberOfSelectedRows > 0);

    NSInteger selectedRow = _ordersTable.selectedRow;
    if (selectedRow >= 0 && static_cast<std::size_t>(selectedRow) < _ordersSnapshot.size()) {
        const OrderId orderId = _ordersSnapshot[static_cast<std::size_t>(selectedRow)].first;
        const std::uint64_t traceId = findTraceIdByOrderId(orderId);
        if (traceId != 0) {
            _selectedTraceId = traceId;
            [self refreshTracePopup];
            [self refreshTraceDetails];
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

    NSMenu* appMenu = [[NSMenu alloc] initWithTitle:@"TWS Trading GUI"];
    NSString* appName = NSProcessInfo.processInfo.processName;
    NSMenuItem* quitItem = [[NSMenuItem alloc] initWithTitle:[NSString stringWithFormat:@"Quit %@", appName]
                                                      action:@selector(terminate:)
                                               keyEquivalent:@"q"];
    [appMenu addItem:quitItem];
    [appMenuItem setSubmenu:appMenu];
    NSApp.mainMenu = mainMenu;

    self.windowController = [[TradingWindowController alloc] init];
    [self.windowController showWindowAndStart];
}

- (BOOL)applicationShouldTerminateAfterLastWindowClosed:(NSApplication*)sender {
    (void)sender;
    return YES;
}

- (void)applicationWillTerminate:(NSNotification*)notification {
    (void)notification;
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
