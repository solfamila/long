#import <AppKit/AppKit.h>

#include "tapescope_client.h"

#include <dispatch/dispatch.h>

#include <cstdint>
#include <initializer_list>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace {

using tapescope::json;

constexpr NSTimeInterval kPollIntervalSeconds = 2.0;
constexpr std::size_t kLiveTailLimit = 32;

struct ProbeSnapshot {
    tapescope::QueryResult<tapescope::StatusSnapshot> status;
    tapescope::QueryResult<std::vector<json>> liveTail;
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

std::string FirstPresentString(const json& payload,
                               std::initializer_list<const char*> keys) {
    if (!payload.is_object()) {
        return {};
    }
    for (const char* key : keys) {
        const auto it = payload.find(key);
        if (it != payload.end() && it->is_string()) {
            return it->get<std::string>();
        }
    }
    return {};
}

std::uint64_t FirstPresentUInt64(const json& payload,
                                 std::initializer_list<const char*> keys) {
    if (!payload.is_object()) {
        return 0;
    }
    for (const char* key : keys) {
        const auto it = payload.find(key);
        if (it == payload.end()) {
            continue;
        }
        if (it->is_number_unsigned()) {
            return it->get<std::uint64_t>();
        }
        if (it->is_number_integer()) {
            const long long value = it->get<long long>();
            if (value >= 0) {
                return static_cast<std::uint64_t>(value);
            }
        }
    }
    return 0;
}

std::string DescribeStatusPane(const ProbeSnapshot& probe, const std::string& configuredSocketPath) {
    std::ostringstream out;
    out << "StatusPane (mutable/live)\n";
    out << "Source command: status\n\n";

    if (!probe.status.ok()) {
        out << tapescope::QueryClient::describeError(probe.status.error) << '\n';
        out << "\nThis pane remains readable while the engine is unavailable or "
               "the query seam has not landed yet.";
        return out.str();
    }

    const tapescope::StatusSnapshot& status = probe.status.value;
    out << "socket_path: "
        << (status.socketPath.empty() ? configuredSocketPath : status.socketPath) << '\n';
    out << "data_dir: " << (status.dataDir.empty() ? "--" : status.dataDir) << '\n';
    out << "instrument: " << (status.instrumentId.empty() ? "--" : status.instrumentId) << '\n';
    out << "latest_session_seq: " << status.latestSessionSeq << '\n';
    out << "live_event_count: " << status.liveEventCount << '\n';
    out << "segment_count: " << status.segmentCount << '\n';
    out << "manifest_hash: " << (status.manifestHash.empty() ? "--" : status.manifestHash) << '\n';
    return out.str();
}

std::string DescribeLiveEvent(const json& event, std::size_t index) {
    std::ostringstream out;
    out << '[' << (index + 1) << "] ";
    if (!event.is_object()) {
        out << event.dump();
        return out.str();
    }

    const std::uint64_t sessionSeq = FirstPresentUInt64(event, {"session_seq", "sessionSeq"});
    if (sessionSeq > 0) {
        out << "session_seq=" << sessionSeq << ' ';
    }

    const std::uint64_t sourceSeq = FirstPresentUInt64(event, {"source_seq", "sourceSeq"});
    if (sourceSeq > 0) {
        out << "source_seq=" << sourceSeq << ' ';
    }

    const std::string eventKind = FirstPresentString(event, {"event_kind", "eventType", "kind", "type"});
    if (!eventKind.empty()) {
        out << "kind=" << eventKind << ' ';
    }

    const std::string wallTime = FirstPresentString(event, {"wall_time", "wallTime", "timestamp"});
    if (!wallTime.empty()) {
        out << "time=" << wallTime << ' ';
    }

    out << '\n' << event.dump(2);
    return out.str();
}

std::string DescribeLiveEventsPane(const tapescope::QueryResult<std::vector<json>>& liveTail) {
    std::ostringstream out;
    out << "LiveEventsPane (mutable/live)\n";
    out << "Source command: read_live_tail\n";
    out << "Requested limit: " << kLiveTailLimit << " events\n\n";

    if (!liveTail.ok()) {
        out << tapescope::QueryClient::describeError(liveTail.error) << '\n';
        out << "\nThis list updates automatically when the seam becomes available.";
        return out.str();
    }

    if (liveTail.value.empty()) {
        out << "No live events returned yet.\n";
        out << "This can happen at startup before the first accepted bridge batch.";
        return out.str();
    }

    out << "Most recent events from the mutable tail:\n\n";
    for (std::size_t i = 0; i < liveTail.value.size(); ++i) {
        out << DescribeLiveEvent(liveTail.value[i], i);
        if (i + 1 != liveTail.value.size()) {
            out << "\n\n";
        }
    }
    return out.str();
}

std::string RangePlaceholderText() {
    return
        "RangePane is reserved for Wave 3.\n\n"
        "This pane is intentionally left as a placeholder in Wave 2 while "
        "StatusPane and LiveEventsPane are being implemented.";
}

std::string OrderPlaceholderText() {
    return
        "OrderLookupPane is reserved for Wave 4.\n\n"
        "This pane is intentionally left as a placeholder in Wave 2.";
}

NSString* PollMetaTextForProbe(const ProbeSnapshot& probe) {
    NSString* tailState = @"live tail unavailable";
    if (probe.liveTail.ok()) {
        tailState = [NSString stringWithFormat:@"live tail: %llu events",
                                               static_cast<unsigned long long>(probe.liveTail.value.size())];
    } else {
        switch (probe.liveTail.error.kind) {
            case tapescope::QueryErrorKind::SeamUnavailable:
                tailState = @"live tail: seam unavailable";
                break;
            case tapescope::QueryErrorKind::Transport:
                tailState = @"live tail: socket unavailable";
                break;
            case tapescope::QueryErrorKind::MalformedResponse:
                tailState = @"live tail: malformed response";
                break;
            case tapescope::QueryErrorKind::Remote:
                tailState = @"live tail: remote error";
                break;
            case tapescope::QueryErrorKind::None:
                break;
        }
    }
    return [NSString stringWithFormat:@"Mutable/live refresh every %.1fs (%@)",
                                      kPollIntervalSeconds,
                                      tailState];
}

ProbeSnapshot CaptureProbe(const tapescope::QueryClient& client) {
    ProbeSnapshot probe;
    probe.status = client.status();

    if (!probe.status.ok()) {
        probe.liveTail.error = probe.status.error;
        return probe;
    }

    probe.liveTail = client.readLiveTail(kLiveTailLimit);
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
    NSTextField* _pollMetaLabel;
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
        @"Wave 2: status and mutable live-tail panes backed only by QueryClient status/read_live_tail.",
        [NSFont systemFontOfSize:13.0 weight:NSFontWeightMedium],
        [NSColor secondaryLabelColor]);
    subtitle.lineBreakMode = NSLineBreakByWordWrapping;
    subtitle.maximumNumberOfLines = 2;
    [root addArrangedSubview:subtitle];

    _bannerLabel = MakeLabel(@"Waiting for the engine",
                             [NSFont systemFontOfSize:14.0 weight:NSFontWeightSemibold],
                             [NSColor secondaryLabelColor]);
    [root addArrangedSubview:_bannerLabel];

    _pollMetaLabel = MakeLabel(@"Mutable/live refresh every 2.0s",
                               [NSFont systemFontOfSize:12.0 weight:NSFontWeightMedium],
                               [NSColor tertiaryLabelColor]);
    [root addArrangedSubview:_pollMetaLabel];

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

    _statusTextView.string = @"Waiting for first status response…";
    _liveTextView.string = @"Waiting for first live-tail response…";
    _rangeTextView.string = ToNSString(RangePlaceholderText());
    _orderTextView.string = ToNSString(OrderPlaceholderText());
}

- (void)showWindowAndStart {
    [self showWindow:nil];
    [self.window makeKeyAndOrderFront:nil];
    [NSApp activateIgnoringOtherApps:YES];
    [self startPolling];
}

- (void)startPolling {
    [self refresh:nil];
    _pollTimer = [NSTimer scheduledTimerWithTimeInterval:kPollIntervalSeconds
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
    _pollMetaLabel.stringValue = @"Refreshing mutable/live panes…";

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
        _statusTextView.string = ToNSString(DescribeStatusPane(probe, configuredSocket));
    } else {
        _dataDirValue.stringValue = @"--";
        _instrumentValue.stringValue = @"--";
        _latestSeqValue.stringValue = @"--";
        _liveCountValue.stringValue = @"--";
        _segmentCountValue.stringValue = @"--";
        _manifestHashValue.stringValue = @"--";
        _statusTextView.string = ToNSString(DescribeStatusPane(probe, configuredSocket));
    }

    _liveTextView.string = ToNSString(DescribeLiveEventsPane(probe.liveTail));
    _pollMetaLabel.stringValue = PollMetaTextForProbe(probe);
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
