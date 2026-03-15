#import <AppKit/AppKit.h>

#include "tapescope_client.h"

#include <dispatch/dispatch.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace {

using tapescope::json;

struct ProbeSnapshot {
    tapescope::QueryResult<tapescope::StatusSnapshot> status;
    tapescope::QueryResult<std::vector<json>> liveTail;
    tapescope::QueryResult<std::vector<json>> range;
    std::string orderPlaceholder;
};

NSString* ToNSString(const std::string& value) {
    if (value.empty()) {
        return @"";
    }
    return [NSString stringWithUTF8String:value.c_str()];
}

NSTextField* MakeLabel(NSString* text, NSFont* font, NSColor* color) {
    NSTextField* label = [NSTextField labelWithString:text ?: @""];
    label.font = font;
    label.textColor = color ?: [NSColor labelColor];
    label.lineBreakMode = NSLineBreakByTruncatingTail;
    return label;
}

NSTextField* MakeValueLabel() {
    return MakeLabel(@"--",
                     [NSFont monospacedSystemFontOfSize:12.5 weight:NSFontWeightMedium],
                     [NSColor labelColor]);
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
    textView.backgroundColor = [NSColor colorWithCalibratedWhite:0.995 alpha:1.0];
    return textView;
}

NSScrollView* MakeScrollView(NSTextView* textView) {
    NSScrollView* scrollView = [[NSScrollView alloc] initWithFrame:NSMakeRect(0, 0, 800, 420)];
    scrollView.hasVerticalScroller = YES;
    scrollView.hasHorizontalScroller = NO;
    scrollView.borderType = NSBezelBorder;
    scrollView.autoresizingMask = NSViewWidthSizable | NSViewHeightSizable;
    scrollView.documentView = textView;
    return scrollView;
}

NSStackView* MakeColumnStack(CGFloat spacing) {
    NSStackView* stack = [[NSStackView alloc] initWithFrame:NSZeroRect];
    stack.orientation = NSUserInterfaceLayoutOrientationVertical;
    stack.alignment = NSLayoutAttributeLeading;
    stack.spacing = spacing;
    stack.translatesAutoresizingMaskIntoConstraints = NO;
    return stack;
}

NSColor* BannerColorForProbe(const ProbeSnapshot& probe) {
    if (probe.status.ok()) {
        return [NSColor systemGreenColor];
    }
    switch (probe.status.error.kind) {
        case tapescope::QueryErrorKind::Transport:
            return [NSColor systemRedColor];
        case tapescope::QueryErrorKind::SeamUnavailable:
            return [NSColor systemOrangeColor];
        case tapescope::QueryErrorKind::MalformedResponse:
        case tapescope::QueryErrorKind::Remote:
            return [NSColor systemYellowColor];
        case tapescope::QueryErrorKind::None:
            break;
    }
    return [NSColor secondaryLabelColor];
}

std::string PrettyPrintJson(const json& payload) {
    return payload.dump(2);
}

json StatusToJson(const tapescope::StatusSnapshot& status) {
    return json{
        {"socket_path", status.socketPath},
        {"data_dir", status.dataDir},
        {"instrument", status.instrumentId},
        {"latest_session_seq", status.latestSessionSeq},
        {"live_event_count", status.liveEventCount},
        {"segment_count", status.segmentCount},
        {"manifest_hash", status.manifestHash}
    };
}

json EventsToJson(const std::vector<json>& events) {
    json payload = json::array();
    for (const auto& event : events) {
        payload.push_back(event);
    }
    return payload;
}

std::string DescribeResult(const tapescope::QueryResult<std::vector<json>>& result,
                           const std::string& emptyMessage) {
    if (!result.ok()) {
        return tapescope::QueryClient::describeError(result.error);
    }
    if (result.value.empty()) {
        return emptyMessage;
    }
    return PrettyPrintJson(EventsToJson(result.value));
}

std::string OrderPlaceholderText(const ProbeSnapshot& probe) {
    std::string text =
        "Wave 1 scaffold only.\n\n"
        "The app-side client already exposes the `find_order_anchor` command, "
        "but this build intentionally stops short of the lookup form and result rendering. "
        "That keeps the first wave limited to the bundle scaffold, query transport boundary, "
        "polling health, and safe placeholder states.\n";

    if (!probe.status.ok()) {
        text += "\nCurrent engine state: ";
        text += tapescope::QueryClient::describeError(probe.status.error);
    }
    return text;
}

ProbeSnapshot CaptureProbe(const tapescope::QueryClient& client) {
    ProbeSnapshot probe;
    probe.status = client.status();

    if (!probe.status.ok()) {
        probe.liveTail.error = probe.status.error;
        probe.range.error = probe.status.error;
        probe.orderPlaceholder = OrderPlaceholderText(probe);
        return probe;
    }

    probe.liveTail = client.readLiveTail(32);

    tapescope::RangeQuery rangeQuery;
    rangeQuery.firstSessionSeq = 1;
    rangeQuery.lastSessionSeq = 64;
    probe.range = client.readRange(rangeQuery);
    probe.orderPlaceholder = OrderPlaceholderText(probe);
    return probe;
}

NSString* BannerTextForProbe(const ProbeSnapshot& probe) {
    if (probe.status.ok()) {
        return @"Connected to the engine query seam";
    }
    switch (probe.status.error.kind) {
        case tapescope::QueryErrorKind::Transport:
            return @"Engine socket is unreachable";
        case tapescope::QueryErrorKind::SeamUnavailable:
            return @"Engine reachable, but the query seam is not available on this branch";
        case tapescope::QueryErrorKind::MalformedResponse:
            return @"Engine returned a malformed query payload";
        case tapescope::QueryErrorKind::Remote:
            return @"Engine reported a query error";
        case tapescope::QueryErrorKind::None:
            break;
    }
    return @"Waiting for the engine";
}

NSString* UInt64String(std::uint64_t value) {
    return [NSString stringWithFormat:@"%llu", static_cast<unsigned long long>(value)];
}

} // namespace

@interface TapeScopeWindowController : NSWindowController <NSWindowDelegate> {
@private
    std::unique_ptr<tapescope::QueryClient> _client;
    dispatch_queue_t _pollQueue;
    NSTimer* _pollTimer;
    BOOL _pollInFlight;

    NSTextField* _bannerLabel;
    NSTextField* _socketValue;
    NSTextField* _dataDirValue;
    NSTextField* _instrumentValue;
    NSTextField* _latestSeqValue;
    NSTextField* _liveCountValue;
    NSTextField* _segmentCountValue;
    NSTextField* _manifestHashValue;

    NSTextView* _statusTextView;
    NSTextView* _liveTextView;
    NSTextView* _rangeTextView;
    NSTextView* _orderTextView;
}
@end

@implementation TapeScopeWindowController

- (instancetype)init {
    NSRect frame = NSMakeRect(0, 0, 1120, 760);
    NSWindow* window = [[NSWindow alloc] initWithContentRect:frame
                                                   styleMask:(NSWindowStyleMaskTitled |
                                                              NSWindowStyleMaskClosable |
                                                              NSWindowStyleMaskResizable |
                                                              NSWindowStyleMaskMiniaturizable)
                                                     backing:NSBackingStoreBuffered
                                                       defer:NO];
    self = [super initWithWindow:window];
    if (self == nil) {
        return nil;
    }

    window.title = @"TapeScope";
    window.delegate = self;
    window.minSize = NSMakeSize(920, 620);
    window.contentView.wantsLayer = YES;
    window.contentView.layer.backgroundColor = [NSColor colorWithCalibratedRed:0.96 green:0.95 blue:0.92 alpha:1.0].CGColor;

    tapescope::ClientConfig config;
    config.socketPath = tapescope::defaultSocketPath();
    _client = std::make_unique<tapescope::QueryClient>(std::move(config));
    _pollQueue = dispatch_queue_create("com.foxy.tapescope.poll", DISPATCH_QUEUE_SERIAL);

    [self buildInterface];
    return self;
}

- (NSTabViewItem*)tabItemWithLabel:(NSString*)label textView:(NSTextView* __strong *)outTextView {
    NSTextView* textView = MakeReadOnlyTextView();
    NSScrollView* scrollView = MakeScrollView(textView);

    NSTabViewItem* item = [[NSTabViewItem alloc] initWithIdentifier:label];
    item.label = label;
    item.view = scrollView;

    if (outTextView != nullptr) {
        *outTextView = textView;
    }
    return item;
}

- (void)buildInterface {
    NSView* contentView = self.window.contentView;
    NSStackView* root = MakeColumnStack(16.0);
    [contentView addSubview:root];

    [NSLayoutConstraint activateConstraints:@[
        [root.leadingAnchor constraintEqualToAnchor:contentView.leadingAnchor constant:22.0],
        [root.trailingAnchor constraintEqualToAnchor:contentView.trailingAnchor constant:-22.0],
        [root.topAnchor constraintEqualToAnchor:contentView.topAnchor constant:20.0],
        [root.bottomAnchor constraintEqualToAnchor:contentView.bottomAnchor constant:-20.0]
    ]];

    NSTextField* title = MakeLabel(@"TapeScope",
                                   [NSFont systemFontOfSize:28.0 weight:NSFontWeightBold],
                                   [NSColor labelColor]);
    [root addArrangedSubview:title];

    NSTextField* subtitle = MakeLabel(
        @"Wave 1 scaffold: one app-side query client, health polling, and safe placeholders while the engine query seam lands.",
        [NSFont systemFontOfSize:13.0 weight:NSFontWeightMedium],
        [NSColor secondaryLabelColor]);
    subtitle.lineBreakMode = NSLineBreakByWordWrapping;
    subtitle.maximumNumberOfLines = 2;
    [root addArrangedSubview:subtitle];

    _bannerLabel = MakeLabel(@"Waiting for the engine",
                             [NSFont systemFontOfSize:14.0 weight:NSFontWeightSemibold],
                             [NSColor secondaryLabelColor]);
    [root addArrangedSubview:_bannerLabel];

    NSGridView* summaryGrid = [NSGridView gridViewWithViews:@[
        @[MakeLabel(@"Socket", [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold], [NSColor secondaryLabelColor]), (_socketValue = MakeValueLabel())],
        @[MakeLabel(@"Data Dir", [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold], [NSColor secondaryLabelColor]), (_dataDirValue = MakeValueLabel())],
        @[MakeLabel(@"Instrument", [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold], [NSColor secondaryLabelColor]), (_instrumentValue = MakeValueLabel())],
        @[MakeLabel(@"Latest Session Seq", [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold], [NSColor secondaryLabelColor]), (_latestSeqValue = MakeValueLabel())],
        @[MakeLabel(@"Live Event Count", [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold], [NSColor secondaryLabelColor]), (_liveCountValue = MakeValueLabel())],
        @[MakeLabel(@"Segment Count", [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold], [NSColor secondaryLabelColor]), (_segmentCountValue = MakeValueLabel())],
        @[MakeLabel(@"Manifest Hash", [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold], [NSColor secondaryLabelColor]), (_manifestHashValue = MakeValueLabel())]
    ]];
    summaryGrid.translatesAutoresizingMaskIntoConstraints = NO;
    summaryGrid.rowSpacing = 8.0;
    summaryGrid.columnSpacing = 18.0;
    [summaryGrid.widthAnchor constraintEqualToAnchor:root.widthAnchor].active = YES;
    [root addArrangedSubview:summaryGrid];

    NSTabView* tabView = [[NSTabView alloc] initWithFrame:NSZeroRect];
    tabView.translatesAutoresizingMaskIntoConstraints = NO;
    [tabView addTabViewItem:[self tabItemWithLabel:@"StatusPane" textView:&_statusTextView]];
    [tabView addTabViewItem:[self tabItemWithLabel:@"LiveEventsPane" textView:&_liveTextView]];
    [tabView addTabViewItem:[self tabItemWithLabel:@"RangePane" textView:&_rangeTextView]];
    [tabView addTabViewItem:[self tabItemWithLabel:@"OrderLookupPane" textView:&_orderTextView]];
    [tabView.heightAnchor constraintGreaterThanOrEqualToConstant:430.0].active = YES;
    [root addArrangedSubview:tabView];
}

- (void)showWindowAndStart {
    [self showWindow:nil];
    [self.window makeKeyAndOrderFront:nil];
    [NSApp activateIgnoringOtherApps:YES];
    [self startPolling];
}

- (void)startPolling {
    [self refresh:nil];
    _pollTimer = [NSTimer scheduledTimerWithTimeInterval:2.0
                                                  target:self
                                                selector:@selector(refresh:)
                                                userInfo:nil
                                                 repeats:YES];
}

- (void)shutdown {
    [_pollTimer invalidate];
    _pollTimer = nil;
}

- (void)refresh:(id)sender {
    (void)sender;
    if (_pollInFlight || !_client) {
        return;
    }

    _pollInFlight = YES;
    _bannerLabel.stringValue = @"Probing tape_engine…";
    _bannerLabel.textColor = [NSColor systemOrangeColor];

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_pollQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }

        ProbeSnapshot probe = CaptureProbe(*strongSelf->_client);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil) {
                return;
            }
            innerSelf->_pollInFlight = NO;
            [innerSelf applyProbe:probe];
        });
    });
}

- (void)applyProbe:(const ProbeSnapshot&)probe {
    _bannerLabel.stringValue = BannerTextForProbe(probe);
    _bannerLabel.textColor = BannerColorForProbe(probe);

    const std::string configuredSocket = _client ? _client->config().socketPath : tapescope::defaultSocketPath();
    _socketValue.stringValue = ToNSString(probe.status.ok() && !probe.status.value.socketPath.empty()
                                              ? probe.status.value.socketPath
                                              : configuredSocket);

    if (probe.status.ok()) {
        _dataDirValue.stringValue = probe.status.value.dataDir.empty() ? @"--" : ToNSString(probe.status.value.dataDir);
        _instrumentValue.stringValue = probe.status.value.instrumentId.empty() ? @"--" : ToNSString(probe.status.value.instrumentId);
        _latestSeqValue.stringValue = UInt64String(probe.status.value.latestSessionSeq);
        _liveCountValue.stringValue = UInt64String(probe.status.value.liveEventCount);
        _segmentCountValue.stringValue = UInt64String(probe.status.value.segmentCount);
        _manifestHashValue.stringValue = probe.status.value.manifestHash.empty() ? @"--" : ToNSString(probe.status.value.manifestHash);
        _statusTextView.string = ToNSString(PrettyPrintJson(StatusToJson(probe.status.value)));
    } else {
        _dataDirValue.stringValue = @"--";
        _instrumentValue.stringValue = @"--";
        _latestSeqValue.stringValue = @"--";
        _liveCountValue.stringValue = @"--";
        _segmentCountValue.stringValue = @"--";
        _manifestHashValue.stringValue = @"--";
        _statusTextView.string = ToNSString(tapescope::QueryClient::describeError(probe.status.error));
    }

    _liveTextView.string = ToNSString(DescribeResult(probe.liveTail, "No live events returned"));
    _rangeTextView.string = ToNSString(DescribeResult(probe.range, "No range events returned"));
    _orderTextView.string = ToNSString(probe.orderPlaceholder);
}

- (void)windowWillClose:(NSNotification*)notification {
    (void)notification;
    [self shutdown];
    [NSApp terminate:nil];
}

@end

@interface AppDelegate : NSObject <NSApplicationDelegate>
@property(nonatomic, strong) TapeScopeWindowController* windowController;
@end

@implementation AppDelegate

- (void)applicationDidFinishLaunching:(NSNotification*)notification {
    (void)notification;

    NSMenu* mainMenu = [[NSMenu alloc] initWithTitle:@""];
    NSMenuItem* appMenuItem = [[NSMenuItem alloc] initWithTitle:@"" action:nil keyEquivalent:@""];
    [mainMenu addItem:appMenuItem];

    NSMenu* appMenu = [[NSMenu alloc] initWithTitle:@"TapeScope"];
    NSString* appName = NSProcessInfo.processInfo.processName;
    NSMenuItem* quitItem = [[NSMenuItem alloc] initWithTitle:[NSString stringWithFormat:@"Quit %@", appName]
                                                      action:@selector(terminate:)
                                               keyEquivalent:@"q"];
    [appMenu addItem:quitItem];
    [appMenuItem setSubmenu:appMenu];
    NSApp.mainMenu = mainMenu;

    self.windowController = [[TapeScopeWindowController alloc] init];
    [self.windowController showWindowAndStart];
}

- (BOOL)applicationShouldTerminateAfterLastWindowClosed:(NSApplication*)sender {
    (void)sender;
    return YES;
}

- (void)applicationWillTerminate:(NSNotification*)notification {
    (void)notification;
    [self.windowController shutdown];
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
