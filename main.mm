#import <AppKit/AppKit.h>

#include "app_shared.h"
#include "controller.h"
#include "trading_wrapper.h"
#include "websocket_handlers.h"

namespace {

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

struct PanelDerivedState {
    UiStatusSnapshot status;
    SymbolUiSnapshot symbol;
    double buyPrice = 0.0;
    double sellPrice = 0.0;
    bool buySweepAvailable = false;
    bool sellSweepAvailable = false;
    bool canTrade = false;
    bool canBuy = false;
    bool canClosePosition = false;
    bool hasCancelableOrders = false;
    int askLevels = 0;
    int bidLevels = 0;
    int maxToggleQuantity = 1;
};

PanelDerivedState BuildPanelState(const std::string& subscribedSymbol,
                                  bool subscribed,
                                  int quantityInput,
                                  double priceBuffer,
                                  double maxPositionDollars) {
    PanelDerivedState state;
    state.status = captureUiStatusSnapshot();
    state.symbol = captureSymbolUiSnapshot(subscribed ? subscribedSymbol : std::string());
    state.canTrade = state.symbol.canTrade;
    state.askLevels = static_cast<int>(state.symbol.askBook.size());
    state.bidLevels = static_cast<int>(state.symbol.bidBook.size());
    state.maxToggleQuantity = computeMaxQuantityFromAsk(state.symbol.askPrice, maxPositionDollars);

    if (subscribed) {
        if (!state.symbol.askBook.empty()) {
            const double sweep = calculateSweepPrice(state.symbol.askBook, quantityInput, priceBuffer, true);
            if (sweep > 0.0) {
                state.buyPrice = sweep;
                state.buySweepAvailable = true;
            }
        }

        if (!state.symbol.bidBook.empty()) {
            const double sweep = calculateSweepPrice(state.symbol.bidBook, quantityInput, priceBuffer, false);
            if (sweep > 0.0) {
                state.sellPrice = sweep;
                state.sellSweepAvailable = true;
            }
        }

        if (state.buyPrice <= 0.0 && state.symbol.askPrice > 0.0) {
            state.buyPrice = state.symbol.askPrice + priceBuffer;
        }
        if (state.sellPrice <= 0.0 && state.symbol.bidPrice > 0.0) {
            state.sellPrice = std::max(0.01, state.symbol.bidPrice - priceBuffer);
        }

        if (state.buyPrice > 0.0 && state.symbol.askPrice > 0.0) {
            state.buyPrice = std::max(state.buyPrice, state.symbol.askPrice + priceBuffer);
        }
        if (state.sellPrice > 0.0 && state.symbol.bidPrice > 0.0) {
            state.sellPrice = std::min(state.sellPrice, std::max(0.01, state.symbol.bidPrice - priceBuffer));
        }
    }

    state.canBuy = state.canTrade && subscribed && quantityInput > 0 && state.buyPrice > 0.0;
    state.canClosePosition = state.canTrade && subscribed &&
                             state.symbol.availableLongToClose > 0.0 &&
                             state.sellPrice > 0.0;

    const auto orders = captureOrdersSnapshot();
    for (const auto& [id, order] : orders) {
        (void)id;
        if (!order.isTerminal() && !order.cancelPending) {
            state.hasCancelableOrders = true;
            break;
        }
    }

    return state;
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
    TradingWrapper _wrapper;
    std::unique_ptr<EReaderOSSignal> _osSignal;
    std::unique_ptr<EClientSocket> _client;
    std::unique_ptr<EReader> _reader;
    std::unique_ptr<ix::WebSocketServer> _wsServer;
    std::thread _readerThread;
    std::atomic<bool> _readerRunning;
    ControllerState _controllerState;

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

    NSTimer* _refreshTimer;

    NSTextField* _twsStatusLabel;
    NSTextField* _accountStatusLabel;
    NSTextField* _websocketStatusLabel;
    NSTextField* _controllerStatusLabel;
    NSTextField* _controllerLockLabel;

    NSTextField* _symbolField;
    NSTextField* _marketHeaderLabel;
    NSTextField* _bidLabel;
    NSTextField* _askLabel;
    NSTextField* _lastLabel;
    NSTextField* _positionLabel;
    NSTextField* _pnlLabel;
    NSTextField* _bookDepthLabel;
    NSTextField* _pricePreviewLabel;
    NSTextField* _controllerHintLabel;

    NSTextField* _quantityField;
    NSTextField* _bufferField;
    NSTextField* _maxPositionField;

    NSButton* _subscribeButton;
    NSButton* _buyButton;
    NSButton* _closeButton;
    NSButton* _cancelSelectedButton;
    NSButton* _cancelAllButton;

    NSTableView* _ordersTable;
    NSPopUpButton* _tracePopup;
    NSTextView* _traceTextView;
    NSTextView* _messagesTextView;
}

- (void)startRuntime;
- (void)shutdownRuntime;

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
        _readerRunning.store(false);
        _symbolInput = DEFAULT_SYMBOL;
        _quantityInput = 1;
        _priceBuffer = 0.01;
        _maxPositionDollars = 40000.0;
        _selectedTraceId = 0;
        _messagesVersionSeen = 0;
        _subscribed = false;
        _shuttingDown = false;
        [self buildInterface];
        self.window.delegate = self;
    }
    return self;
}

- (void)dealloc {
    [self shutdownRuntime];
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
    if (_client) {
        return;
    }

    syncSharedGuiInputs(_quantityInput, _priceBuffer, _maxPositionDollars);

    std::cout << "=== TWS Trading GUI ===" << std::endl;
    std::cout << "Connecting to TWS at " << DEFAULT_HOST << ":" << DEFAULT_PORT << std::endl;
    std::cout << "Configured account: " << HARDCODED_ACCOUNT << std::endl;
    std::cout << "Using platform: AppKit native views" << std::endl;

    _osSignal = std::make_unique<EReaderOSSignal>(2000);
    _client = std::make_unique<EClientSocket>(&_wrapper, _osSignal.get());
    _wrapper.setClient(_client.get());

    const bool twsConnected = _client->eConnect(DEFAULT_HOST, DEFAULT_PORT, DEFAULT_CLIENT_ID);
    if (!twsConnected) {
        std::cerr << "Failed to connect to TWS" << std::endl;
        g_data.addMessage("Failed to connect to TWS");
    } else {
        std::cout << "Connected to TWS socket" << std::endl;
    }

    if (twsConnected) {
        _reader = std::make_unique<EReader>(_client.get(), _osSignal.get());
        _readerRunning.store(true);
        _reader->start();
        _readerThread = std::thread(readerLoop, _osSignal.get(), _reader.get(), _client.get(), &_readerRunning);
    }

    ix::initNetSystem();
    _wsServer = std::make_unique<ix::WebSocketServer>(WEBSOCKET_PORT, WEBSOCKET_HOST);
    EClientSocket* const client = _client.get();
    _wsServer->setOnClientMessageCallback(
        [client](std::shared_ptr<ix::ConnectionState> connectionState,
                 ix::WebSocket& webSocket,
                 const ix::WebSocketMessagePtr& msg) {
            (void)connectionState;

            if (msg->type == ix::WebSocketMessageType::Message) {
                handleWebSocketMessage(msg->str, webSocket, client);
            } else if (msg->type == ix::WebSocketMessageType::Open) {
                const int total = g_data.wsConnectedClients.fetch_add(1) + 1;
                g_data.addMessage("WebSocket client connected (total: " + std::to_string(total) + ")");
                std::cout << "[WebSocket client connected]" << std::endl;
            } else if (msg->type == ix::WebSocketMessageType::Close) {
                int observed = g_data.wsConnectedClients.load();
                int total = 0;
                do {
                    total = observed > 0 ? (observed - 1) : 0;
                } while (!g_data.wsConnectedClients.compare_exchange_weak(observed, total));
                g_data.addMessage("WebSocket client disconnected (total: " + std::to_string(total) + ")");
                std::cout << "[WebSocket client disconnected]" << std::endl;
            } else if (msg->type == ix::WebSocketMessageType::Error) {
                g_data.addMessage("WebSocket error: " + msg->errorInfo.reason);
                std::cout << "[WebSocket error: " << msg->errorInfo.reason << "]" << std::endl;
            }
        });

    if (_wsServer->listenAndStart()) {
        g_data.wsServerRunning.store(true);
        g_data.addMessage("WebSocket server started on localhost port " + std::to_string(WEBSOCKET_PORT));
        std::cout << "[WebSocket server started on localhost port " << WEBSOCKET_PORT << "]" << std::endl;
    } else {
        g_data.addMessage("Failed to start WebSocket server on port " + std::to_string(WEBSOCKET_PORT));
        std::cerr << "Failed to start WebSocket server on port " << WEBSOCKET_PORT << std::endl;
    }

    controllerInitialize(_controllerState);

    _refreshTimer = [NSTimer timerWithTimeInterval:0.15
                                            target:self
                                          selector:@selector(refreshTick:)
                                          userInfo:nil
                                           repeats:YES];
    [[NSRunLoop mainRunLoop] addTimer:_refreshTimer forMode:NSRunLoopCommonModes];

    [self refreshInterface];
}

- (void)shutdownRuntime {
    if (_shuttingDown) {
        return;
    }
    _shuttingDown = true;

    [_refreshTimer invalidate];
    _refreshTimer = nil;

    if (_wsServer) {
        if (g_data.wsServerRunning.load()) {
            std::cout << "Stopping WebSocket server..." << std::endl;
            _wsServer->stop();
            g_data.wsServerRunning.store(false);
        }
        _wsServer.reset();
    }
    ix::uninitNetSystem();

    if (_client) {
        cancelActiveSubscription(_client.get());
    }

    _readerRunning.store(false);
    if (_osSignal) {
        _osSignal->issueSignal();
    }

    if (_readerThread.joinable()) {
        _readerThread.join();
    }

    _reader.reset();

    if (_client) {
        if (_client->isConnected()) {
            _client->eDisconnect();
        }
        _client.reset();
    }

    controllerCleanup(_controllerState);
    _osSignal.reset();
}

- (void)windowWillClose:(NSNotification*)notification {
    (void)notification;
    [self shutdownRuntime];
    [NSApp terminate:nil];
}

- (void)refreshTick:(NSTimer*)timer {
    (void)timer;
    if (_shuttingDown) {
        return;
    }

    controllerPoll(_controllerState);
    const PanelDerivedState state = BuildPanelState(_subscribedSymbol, _subscribed, _quantityInput, _priceBuffer, _maxPositionDollars);
    [self handleControllerWithState:state];
    [self refreshInterface];
}

- (void)inputFieldAction:(id)sender {
    (void)sender;
    [self syncInputsFromFields];
    [self refreshInterface];
}

- (void)controlTextDidEndEditing:(NSNotification*)notification {
    (void)notification;
    [self syncInputsFromFields];
    [self refreshInterface];
}

- (void)syncInputsFromFields {
    const int parsedQuantity = _quantityField.intValue;
    _quantityInput = std::max(1, parsedQuantity);
    _priceBuffer = std::max(0.0, _bufferField.doubleValue);
    _maxPositionDollars = std::max(1000.0, _maxPositionField.doubleValue);
    syncSharedGuiInputs(_quantityInput, _priceBuffer, _maxPositionDollars);
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
    const std::string requestedSymbol = toUpperCase(ToStdString(_symbolField.stringValue));
    if (requestedSymbol.empty()) {
        g_data.addMessage("Subscribe failed: Symbol cannot be empty");
        [self refreshInterface];
        return;
    }

    std::string error;
    if (!_client || !requestSymbolSubscription(_client.get(), requestedSymbol, false, &error)) {
        g_data.addMessage("Subscribe failed: " + error);
        [self refreshInterface];
        return;
    }

    _symbolInput = requestedSymbol;
    _subscribed = true;
    _subscribedSymbol = requestedSymbol;
    [self updateInputFieldsFromState];
    [self refreshInterface];
}

- (void)buyAction:(id)sender {
    (void)sender;
    [self syncInputsFromFields];
    const PanelDerivedState state = BuildPanelState(_subscribedSymbol, _subscribed, _quantityInput, _priceBuffer, _maxPositionDollars);
    [self submitBuyWithState:state source:"GUI Button" note:"Buy Limit button pressed" failurePrefix:"Buy failed: "];
    [self refreshInterface];
}

- (void)closeAction:(id)sender {
    (void)sender;
    [self syncInputsFromFields];
    const PanelDerivedState state = BuildPanelState(_subscribedSymbol, _subscribed, _quantityInput, _priceBuffer, _maxPositionDollars);
    [self submitCloseWithState:state source:"GUI Button" note:"Close Long button pressed" failurePrefix:"Close failed: "];
    [self refreshInterface];
}

- (void)cancelSelectedAction:(id)sender {
    (void)sender;
    if (!_client) {
        return;
    }

    NSIndexSet* selectedRows = _ordersTable.selectedRowIndexes;
    if (selectedRows.count == 0) {
        g_data.addMessage("No orders selected for cancellation");
        [self refreshInterface];
        return;
    }

    __block std::vector<OrderId> selectedOrderIds;
    [selectedRows enumerateIndexesUsingBlock:^(NSUInteger idx, BOOL* stop) {
        (void)stop;
        if (idx < _ordersSnapshot.size()) {
            selectedOrderIds.push_back(_ordersSnapshot[idx].first);
        }
    }];

    const std::vector<OrderId> markedForCancel = markOrdersPendingCancel(selectedOrderIds);
    const std::vector<bool> cancelSent = sendCancelRequests(_client.get(), markedForCancel);
    for (std::size_t i = 0; i < markedForCancel.size(); ++i) {
        if (cancelSent[i]) {
            g_data.addMessage("Cancel request sent for order " + std::to_string(markedForCancel[i]));
        } else {
            g_data.addMessage("Cancel failed (not connected) for order " + std::to_string(markedForCancel[i]));
        }
    }
    [self refreshInterface];
}

- (void)cancelAllAction:(id)sender {
    (void)sender;
    if (!_client) {
        return;
    }

    const std::vector<OrderId> pendingOrders = markAllPendingOrdersForCancel();
    if (pendingOrders.empty()) {
        g_data.addMessage("No pending orders to cancel");
        [self refreshInterface];
        return;
    }

    const std::vector<bool> cancelSent = sendCancelRequests(_client.get(), pendingOrders);
    const int sentCount = static_cast<int>(std::count(cancelSent.begin(), cancelSent.end(), true));
    if (sentCount != static_cast<int>(pendingOrders.size())) {
        g_data.addMessage("Some cancel requests could not be sent");
    }
    g_data.addMessage("Cancel requested for " + std::to_string(sentCount) + " order(s)");
    [self refreshInterface];
}

- (void)traceSelectionChanged:(id)sender {
    NSInteger selectedIndex = _tracePopup.indexOfSelectedItem;
    if (selectedIndex >= 0 && static_cast<std::size_t>(selectedIndex) < _traceItems.size()) {
        _selectedTraceId = _traceItems[static_cast<std::size_t>(selectedIndex)].traceId;
    }
    [self refreshTraceDetails];
}

- (void)handleControllerWithState:(const PanelDerivedState&)state {
    if (!controllerIsConnected(_controllerState)) {
        return;
    }

    const auto now = std::chrono::steady_clock::now();

    if (controllerConsumeDebouncedPress(_controllerState, CONTROLLER_BUTTON_SQUARE, now)) {
        [self submitBuyWithState:state
                          source:"Controller"
                            note:"Controller Square button"
                   failurePrefix:"[Controller] Buy failed: "];
    }

    if (controllerConsumeDebouncedPress(_controllerState, CONTROLLER_BUTTON_CIRCLE, now)) {
        [self submitCloseWithState:state
                            source:"Controller"
                              note:"Controller Circle button"
                     failurePrefix:"[Controller] Close failed: "];
    }

    if (controllerConsumeDebouncedPress(_controllerState, CONTROLLER_BUTTON_TRIANGLE, now)) {
        if (!_client) {
            return;
        }

        const std::vector<OrderId> pendingOrders = markAllPendingOrdersForCancel();
        if (!pendingOrders.empty()) {
            const std::vector<bool> cancelSent = sendCancelRequests(_client.get(), pendingOrders);
            const int sentCount = static_cast<int>(std::count(cancelSent.begin(), cancelSent.end(), true));
            if (sentCount != static_cast<int>(pendingOrders.size())) {
                g_data.addMessage("[Controller] Some cancel requests could not be sent");
            }
            g_data.addMessage("[Controller] Cancel requested for " + std::to_string(sentCount) + " order(s)");
        } else {
            g_data.addMessage("[Controller] No pending orders to cancel");
        }
    }

    if (controllerConsumeDebouncedPress(_controllerState, CONTROLLER_BUTTON_CROSS, now)) {
        if (state.canTrade && _subscribed) {
            _quantityInput = (_quantityInput == 1) ? state.maxToggleQuantity : 1;
            syncSharedGuiInputs(_quantityInput, _priceBuffer, _maxPositionDollars);
            [self updateInputFieldsFromState];
            g_data.addMessage("[Controller] Quantity toggled to " + std::to_string(_quantityInput) + " shares");
        }
    }

    const bool shouldVibrate = state.symbol.hasPosition && state.symbol.currentPositionQty != 0.0;
    controllerSetVibration(_controllerState, shouldVibrate);
}

- (void)submitBuyWithState:(const PanelDerivedState&)state
                    source:(const std::string&)source
                      note:(const std::string&)note
             failurePrefix:(const std::string&)failurePrefix {
    if (!_client || !state.canBuy) {
        return;
    }

    std::string error;
    std::uint64_t traceId = 0;
    SubmitIntent intent = captureSubmitIntent(source, _subscribedSymbol, "BUY",
                                              _quantityInput, state.buyPrice, false,
                                              _priceBuffer,
                                              state.buySweepAvailable ? state.buyPrice : 0.0,
                                              note);

    if (!submitLimitOrder(_client.get(), _subscribedSymbol, "BUY",
                          static_cast<double>(_quantityInput), state.buyPrice,
                          false, &intent, &error, nullptr, &traceId)) {
        g_data.addMessage(failurePrefix + error);
    }

    if (traceId != 0) {
        _selectedTraceId = traceId;
    }
}

- (void)submitCloseWithState:(const PanelDerivedState&)state
                      source:(const std::string&)source
                        note:(const std::string&)note
               failurePrefix:(const std::string&)failurePrefix {
    if (!_client || !state.canClosePosition) {
        return;
    }

    std::string error;
    std::uint64_t traceId = 0;
    SubmitIntent intent = captureSubmitIntent(source, _subscribedSymbol, "SELL",
                                              toShareCount(state.symbol.availableLongToClose),
                                              state.sellPrice, true,
                                              _priceBuffer,
                                              state.sellSweepAvailable ? state.sellPrice : 0.0,
                                              note);

    if (!submitLimitOrder(_client.get(), _subscribedSymbol, "SELL",
                          state.symbol.availableLongToClose, state.sellPrice,
                          true, &intent, &error, nullptr, &traceId)) {
        g_data.addMessage(failurePrefix + error);
    }

    if (traceId != 0) {
        _selectedTraceId = traceId;
    }
}

- (void)refreshInterface {
    std::string syncedSymbolInput = _symbolInput;
    std::string syncedSubscribedSymbol = _subscribedSymbol;
    bool syncedSubscribed = _subscribed;
    int syncedQuantity = _quantityInput;
    consumeGuiSyncUpdates(syncedSymbolInput, syncedSubscribedSymbol, syncedSubscribed, syncedQuantity);
    if (syncedSymbolInput != _symbolInput ||
        syncedSubscribedSymbol != _subscribedSymbol ||
        syncedSubscribed != _subscribed ||
        syncedQuantity != _quantityInput) {
        _symbolInput = syncedSymbolInput;
        _subscribedSymbol = syncedSubscribedSymbol;
        _subscribed = syncedSubscribed;
        _quantityInput = syncedQuantity;
        [self updateInputFieldsFromState];
    }

    if (_selectedTraceId == 0) {
        _selectedTraceId = latestTradeTraceId();
    }

    const PanelDerivedState state = BuildPanelState(_subscribedSymbol, _subscribed, _quantityInput, _priceBuffer, _maxPositionDollars);
    [self refreshStatusLabels:state];
    [self refreshMarketSection:state];
    [self refreshOrders];
    [self refreshTracePopup];
    [self refreshTraceDetails];
    [self refreshMessages];
}

- (void)refreshStatusLabels:(const PanelDerivedState&)state {
    if (state.status.connected && state.status.sessionReady) {
        _twsStatusLabel.stringValue = @"TWS: Ready";
        _twsStatusLabel.textColor = [NSColor systemGreenColor];
    } else if (state.status.connected) {
        _twsStatusLabel.stringValue = @"TWS: Connected (initializing)";
        _twsStatusLabel.textColor = [NSColor systemOrangeColor];
    } else {
        _twsStatusLabel.stringValue = @"TWS: Disconnected";
        _twsStatusLabel.textColor = [NSColor systemRedColor];
    }

    _accountStatusLabel.stringValue = [NSString stringWithFormat:@"Account: %@", ToNSString(state.status.accountText)];

    if (state.status.wsServerRunning) {
        _websocketStatusLabel.stringValue = [NSString stringWithFormat:@"WS: localhost:%d (%d clients)", WEBSOCKET_PORT, state.status.wsConnectedClients];
        _websocketStatusLabel.textColor = [NSColor systemGreenColor];
    } else {
        _websocketStatusLabel.stringValue = @"WS: Not running";
        _websocketStatusLabel.textColor = [NSColor systemRedColor];
    }

    if (state.status.controllerConnected) {
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
}

- (void)refreshMarketSection:(const PanelDerivedState&)state {
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

    _buyButton.enabled = state.canBuy;
    _closeButton.enabled = state.canClosePosition;
    _cancelAllButton.enabled = state.hasCancelableOrders;

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
}

- (void)refreshOrders {
    _ordersSnapshot = captureOrdersSnapshot();
    [_ordersTable reloadData];
    _cancelSelectedButton.enabled = (_ordersTable.numberOfSelectedRows > 0);
}

- (void)refreshTracePopup {
    const std::vector<TradeTraceListItem> newItems = captureTradeTraceListItems(150);
    if (!newItems.empty() && _selectedTraceId == 0) {
        _selectedTraceId = newItems.front().traceId;
    }

    if (!TraceItemsEqual(_traceItems, newItems)) {
        _traceItems = newItems;
        [_tracePopup removeAllItems];
        if (_traceItems.empty()) {
            [_tracePopup addItemWithTitle:@"No trade trace yet"];
            _tracePopup.enabled = NO;
            return;
        }

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
    } else if (!_traceItems.empty()) {
        for (std::size_t i = 0; i < _traceItems.size(); ++i) {
            if (_traceItems[i].traceId == _selectedTraceId) {
                [_tracePopup selectItemAtIndex:static_cast<NSInteger>(i)];
                break;
            }
        }
    }
}

- (void)refreshTraceDetails {
    if (_traceItems.empty()) {
        _traceTextView.string = @"No trade trace yet.";
        return;
    }

    if (_selectedTraceId == 0) {
        _selectedTraceId = _traceItems.front().traceId;
    }

    const TradeTraceSnapshot snapshot = captureTradeTraceSnapshot(_selectedTraceId);
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
