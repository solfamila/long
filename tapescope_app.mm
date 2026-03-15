#import <AppKit/AppKit.h>

#include "tapescope_client.h"

#include <dispatch/dispatch.h>

#include <algorithm>
#include <cerrno>
#include <cctype>
#include <cstdint>
#include <initializer_list>
#include <limits>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace {

using tapescope::json;

constexpr NSTimeInterval kPollIntervalSeconds = 2.0;
constexpr std::size_t kLiveTailLimit = 32;
constexpr std::uint64_t kDefaultRangeFirstSessionSeq = 1;
constexpr std::uint64_t kDefaultRangeLastSessionSeq = 128;
constexpr std::size_t kRangeSummaryMaxChars = 140;

struct ProbeSnapshot {
    tapescope::QueryResult<tapescope::StatusSnapshot> status;
    tapescope::QueryResult<std::vector<json>> liveTail;
};

struct RangeRow {
    std::string sessionSeq;
    std::string sourceSeq;
    std::string eventKind;
    std::string wallTime;
    std::string summary;
};

enum class OrderAnchorType {
    TraceId = 0,
    OrderId = 1,
    PermId = 2,
    ExecId = 3
};

std::string DescribeLiveEvent(const json& event, std::size_t index);

NSString* ToNSString(const std::string& value) {
    if (value.empty()) {
        return @"";
    }
    return [NSString stringWithUTF8String:value.c_str()];
}

std::string ToStdString(NSString* value) {
    if (value == nil) {
        return {};
    }
    const char* utf8 = [value UTF8String];
    return utf8 == nullptr ? std::string() : std::string(utf8);
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

std::string TrimAscii(std::string text) {
    const auto begin = std::find_if_not(text.begin(), text.end(), [](unsigned char ch) {
        return std::isspace(ch) != 0;
    });
    const auto end = std::find_if_not(text.rbegin(), text.rend(), [](unsigned char ch) {
        return std::isspace(ch) != 0;
    }).base();

    if (begin >= end) {
        return {};
    }
    return std::string(begin, end);
}

bool ParsePositiveUInt64(const std::string& raw, std::uint64_t* parsed) {
    if (parsed == nullptr) {
        return false;
    }

    std::string text = TrimAscii(raw);
    if (text.empty()) {
        return false;
    }

    if (text.front() == '+') {
        text.erase(text.begin());
    }
    if (text.empty()) {
        return false;
    }

    const bool allDigits = std::all_of(text.begin(), text.end(), [](unsigned char ch) {
        return std::isdigit(ch) != 0;
    });
    if (!allDigits) {
        return false;
    }

    errno = 0;
    char* consumed = nullptr;
    const unsigned long long value = std::strtoull(text.c_str(), &consumed, 10);
    if (errno == ERANGE || consumed == nullptr || *consumed != '\0') {
        return false;
    }

    if (value == 0 || value > std::numeric_limits<std::uint64_t>::max()) {
        return false;
    }

    *parsed = static_cast<std::uint64_t>(value);
    return true;
}

bool ParsePositiveInt64(const std::string& raw, long long* parsed) {
    if (parsed == nullptr) {
        return false;
    }

    std::uint64_t unsignedValue = 0;
    if (!ParsePositiveUInt64(raw, &unsignedValue) ||
        unsignedValue > static_cast<std::uint64_t>(std::numeric_limits<long long>::max())) {
        return false;
    }

    *parsed = static_cast<long long>(unsignedValue);
    return true;
}

std::string ToLowerAscii(std::string text) {
    std::transform(text.begin(), text.end(), text.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
    return text;
}

bool ContainsCaseInsensitive(const std::string& haystack, const std::string& needle) {
    return ToLowerAscii(haystack).find(ToLowerAscii(needle)) != std::string::npos;
}

OrderAnchorType OrderAnchorTypeFromIndex(NSInteger index) {
    switch (index) {
        case 1:
            return OrderAnchorType::OrderId;
        case 2:
            return OrderAnchorType::PermId;
        case 3:
            return OrderAnchorType::ExecId;
        case 0:
        default:
            return OrderAnchorType::TraceId;
    }
}

NSString* PlaceholderForOrderAnchorType(OrderAnchorType type) {
    switch (type) {
        case OrderAnchorType::TraceId:
            return @"e.g. 71";
        case OrderAnchorType::OrderId:
            return @"e.g. 501";
        case OrderAnchorType::PermId:
            return @"e.g. 9501";
        case OrderAnchorType::ExecId:
            return @"e.g. EXEC-71";
    }
    return @"";
}

std::string TruncateString(const std::string& value, std::size_t maxChars) {
    if (value.size() <= maxChars) {
        return value;
    }
    if (maxChars <= 3) {
        return value.substr(0, maxChars);
    }
    return value.substr(0, maxChars - 3) + "...";
}

std::string UInt64OrDash(std::uint64_t value) {
    return value == 0 ? std::string("--") : std::to_string(value);
}

std::string DescribeRangeSummary(const json& event) {
    if (!event.is_object()) {
        return TruncateString(event.dump(), kRangeSummaryMaxChars);
    }

    const std::string detail = FirstPresentString(event, {"details", "note", "message", "reason"});
    if (!detail.empty()) {
        return TruncateString(detail, kRangeSummaryMaxChars);
    }

    std::ostringstream out;

    const std::uint64_t traceId = FirstPresentUInt64(event, {"trace_id", "traceId"});
    if (traceId > 0) {
        out << "trace_id=" << traceId << ' ';
    }

    const std::uint64_t orderId = FirstPresentUInt64(event, {"order_id", "orderId"});
    if (orderId > 0) {
        out << "order_id=" << orderId << ' ';
    }

    const std::uint64_t permId = FirstPresentUInt64(event, {"perm_id", "permId"});
    if (permId > 0) {
        out << "perm_id=" << permId << ' ';
    }

    const std::string execId = FirstPresentString(event, {"exec_id", "execId"});
    if (!execId.empty()) {
        out << "exec_id=" << execId << ' ';
    }

    const std::string symbol = FirstPresentString(event, {"symbol", "instrument", "instrument_id"});
    if (!symbol.empty()) {
        out << "symbol=" << symbol << ' ';
    }

    const std::string side = FirstPresentString(event, {"side"});
    if (!side.empty()) {
        out << "side=" << side << ' ';
    }

    const std::string summary = TrimAscii(out.str());
    if (!summary.empty()) {
        return TruncateString(summary, kRangeSummaryMaxChars);
    }

    return TruncateString(event.dump(), kRangeSummaryMaxChars);
}

RangeRow BuildRangeRow(const json& event) {
    RangeRow row;
    row.sessionSeq = UInt64OrDash(FirstPresentUInt64(event, {"session_seq", "sessionSeq"}));
    row.sourceSeq = UInt64OrDash(FirstPresentUInt64(event, {"source_seq", "sourceSeq"}));
    row.eventKind = FirstPresentString(event, {"event_kind", "eventType", "kind", "type"});
    row.wallTime = FirstPresentString(event, {"wall_time", "wallTime", "timestamp"});
    row.summary = DescribeRangeSummary(event);

    if (row.eventKind.empty()) {
        row.eventKind = event.is_object() ? "--" : "raw";
    }
    if (row.wallTime.empty()) {
        row.wallTime = "--";
    }
    return row;
}

NSColor* RangeErrorColor(tapescope::QueryErrorKind kind) {
    switch (kind) {
        case tapescope::QueryErrorKind::None:
            return [NSColor secondaryLabelColor];
        case tapescope::QueryErrorKind::Transport:
            return [NSColor systemRedColor];
        case tapescope::QueryErrorKind::SeamUnavailable:
            return [NSColor systemOrangeColor];
        case tapescope::QueryErrorKind::MalformedResponse:
        case tapescope::QueryErrorKind::Remote:
            return [NSColor systemYellowColor];
    }
    return [NSColor secondaryLabelColor];
}

std::vector<json> ExtractOrderContextEvents(const json& payload) {
    std::vector<json> events;
    auto fromArray = [](const json& arrayPayload) {
        std::vector<json> values;
        values.reserve(arrayPayload.size());
        for (const auto& item : arrayPayload) {
            values.push_back(item);
        }
        return values;
    };

    if (payload.is_array()) {
        return fromArray(payload);
    }
    if (!payload.is_object()) {
        return events;
    }

    for (const char* key : {"events", "records", "matches", "context"}) {
        const auto it = payload.find(key);
        if (it != payload.end() && it->is_array()) {
            return fromArray(*it);
        }
    }
    for (const char* key : {"event", "record", "match"}) {
        const auto it = payload.find(key);
        if (it != payload.end() && it->is_object()) {
            events.push_back(*it);
            return events;
        }
    }
    return events;
}

std::string DescribeOrderAnchor(const json& payload) {
    if (!payload.is_object()) {
        return {};
    }

    const json* anchorObject = &payload;
    const auto anchorIt = payload.find("anchor");
    if (anchorIt != payload.end() && anchorIt->is_object()) {
        anchorObject = &(*anchorIt);
    }

    std::ostringstream out;
    const std::uint64_t traceId = FirstPresentUInt64(*anchorObject, {"trace_id", "traceId"});
    const std::uint64_t orderId = FirstPresentUInt64(*anchorObject, {"order_id", "orderId"});
    const std::uint64_t permId = FirstPresentUInt64(*anchorObject, {"perm_id", "permId"});
    const std::string execId = FirstPresentString(*anchorObject, {"exec_id", "execId"});

    if (traceId > 0) {
        out << "trace_id=" << traceId << ' ';
    }
    if (orderId > 0) {
        out << "order_id=" << orderId << ' ';
    }
    if (permId > 0) {
        out << "perm_id=" << permId << ' ';
    }
    if (!execId.empty()) {
        out << "exec_id=" << execId << ' ';
    }

    return TrimAscii(out.str());
}

bool IsOrderLookupNotFoundPayload(const json& payload) {
    if (payload.is_null()) {
        return true;
    }
    if (payload.is_array()) {
        return payload.empty();
    }
    if (!payload.is_object()) {
        return false;
    }
    if (payload.empty()) {
        return true;
    }

    const auto foundIt = payload.find("found");
    if (foundIt != payload.end() && foundIt->is_boolean() && !foundIt->get<bool>()) {
        return true;
    }

    const std::string status = ToLowerAscii(FirstPresentString(payload, {"status", "lookup_status", "result_status"}));
    if (status == "not_found" || status == "not-found" || status == "not found" || status == "missing") {
        return true;
    }

    const std::string message = FirstPresentString(payload, {"error", "message", "reason"});
    if (ContainsCaseInsensitive(message, "not found")) {
        return true;
    }

    const bool hasContextCollection = payload.contains("events") ||
                                      payload.contains("records") ||
                                      payload.contains("matches") ||
                                      payload.contains("context");
    if (!hasContextCollection) {
        return false;
    }

    const std::vector<json> events = ExtractOrderContextEvents(payload);
    if (!events.empty()) {
        return false;
    }
    return DescribeOrderAnchor(payload).empty();
}

bool IsOrderLookupNotFoundError(const tapescope::QueryError& error) {
    return error.kind == tapescope::QueryErrorKind::Remote &&
           ContainsCaseInsensitive(error.message, "not found");
}

std::string DescribeOrderLookupResult(const json& payload, const std::string& descriptor) {
    std::ostringstream out;
    out << "OrderLookupPane (frozen query)\n";
    out << "Source command: find_order_anchor\n";
    out << "Lookup: " << descriptor << "\n\n";

    const std::string resolvedAnchor = DescribeOrderAnchor(payload);
    if (!resolvedAnchor.empty()) {
        out << "Resolved anchor: " << resolvedAnchor << "\n\n";
    }

    const std::vector<json> contextEvents = ExtractOrderContextEvents(payload);
    if (contextEvents.empty()) {
        out << "No event context array returned for this lookup.\n";
    } else {
        out << "Matching event context (" << contextEvents.size() << " events):\n\n";
        for (std::size_t i = 0; i < contextEvents.size(); ++i) {
            out << DescribeLiveEvent(contextEvents[i], i);
            if (i + 1 != contextEvents.size()) {
                out << "\n\n";
            }
        }
    }

    out << "\n\nRaw payload:\n" << payload.dump(2);
    return out.str();
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

@interface TapeScopeWindowController : NSWindowController <NSWindowDelegate, NSTableViewDataSource, NSTableViewDelegate> {
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
    NSTextView* _orderTextView;
    NSPopUpButton* _orderAnchorTypePopup;
    NSTextField* _orderAnchorInputField;
    NSButton* _orderLookupButton;
    NSTextField* _orderStateLabel;
    NSTextField* _orderSummaryLabel;
    BOOL _orderLookupInFlight;

    NSTextField* _rangeFirstField;
    NSTextField* _rangeLastField;
    NSButton* _rangeFetchButton;
    NSButton* _rangePrevButton;
    NSButton* _rangeNextButton;
    NSTextField* _rangeStateLabel;
    NSTextField* _rangeSummaryLabel;
    NSTableView* _rangeTableView;
    NSTextView* _rangeDetailTextView;

    std::vector<json> _rangeEvents;
    std::vector<RangeRow> _rangeRows;
    tapescope::RangeQuery _activeRangeQuery;
    BOOL _hasActiveRangeQuery;
    BOOL _rangeRequestInFlight;
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

- (NSTextField*)rangeInputFieldWithDefaultValue:(std::uint64_t)value {
    NSTextField* field = [[NSTextField alloc] initWithFrame:NSMakeRect(0, 0, 130, 24)];
    field.font = [NSFont monospacedSystemFontOfSize:12.0 weight:NSFontWeightMedium];
    field.stringValue = UInt64String(value);
    field.alignment = NSTextAlignmentRight;
    field.target = self;
    field.action = @selector(fetchRange:);
    return field;
}

- (NSTabViewItem*)rangeTabItem {
    NSView* pane = [[NSView alloc] initWithFrame:NSZeroRect];

    NSStackView* stack = MakeColumnStack(10.0);
    [pane addSubview:stack];
    [NSLayoutConstraint activateConstraints:@[
        [stack.leadingAnchor constraintEqualToAnchor:pane.leadingAnchor constant:10.0],
        [stack.trailingAnchor constraintEqualToAnchor:pane.trailingAnchor constant:-10.0],
        [stack.topAnchor constraintEqualToAnchor:pane.topAnchor constant:10.0],
        [stack.bottomAnchor constraintEqualToAnchor:pane.bottomAnchor constant:-10.0]
    ]];

    NSTextField* intro = MakeLabel(
        @"Replay window (frozen snapshot): choose first/last session_seq and fetch read_range results.",
        [NSFont systemFontOfSize:12.5 weight:NSFontWeightMedium],
        [NSColor secondaryLabelColor]);
    intro.lineBreakMode = NSLineBreakByWordWrapping;
    intro.maximumNumberOfLines = 2;
    [stack addArrangedSubview:intro];

    NSStackView* controls = [[NSStackView alloc] initWithFrame:NSZeroRect];
    controls.orientation = NSUserInterfaceLayoutOrientationHorizontal;
    controls.alignment = NSLayoutAttributeCenterY;
    controls.spacing = 8.0;
    controls.translatesAutoresizingMaskIntoConstraints = NO;

    [controls addArrangedSubview:MakeLabel(@"first_session_seq",
                                           [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                           [NSColor secondaryLabelColor])];
    _rangeFirstField = [self rangeInputFieldWithDefaultValue:kDefaultRangeFirstSessionSeq];
    [controls addArrangedSubview:_rangeFirstField];

    [controls addArrangedSubview:MakeLabel(@"last_session_seq",
                                           [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                           [NSColor secondaryLabelColor])];
    _rangeLastField = [self rangeInputFieldWithDefaultValue:kDefaultRangeLastSessionSeq];
    [controls addArrangedSubview:_rangeLastField];

    _rangeFetchButton = [NSButton buttonWithTitle:@"Fetch Range" target:self action:@selector(fetchRange:)];
    _rangePrevButton = [NSButton buttonWithTitle:@"Prev Window" target:self action:@selector(fetchPreviousRange:)];
    _rangeNextButton = [NSButton buttonWithTitle:@"Next Window" target:self action:@selector(fetchNextRange:)];
    [controls addArrangedSubview:_rangeFetchButton];
    [controls addArrangedSubview:_rangePrevButton];
    [controls addArrangedSubview:_rangeNextButton];
    [stack addArrangedSubview:controls];

    _rangeStateLabel = MakeLabel(@"Enter a replay window and click Fetch Range.",
                                 [NSFont systemFontOfSize:12.0 weight:NSFontWeightMedium],
                                 [NSColor secondaryLabelColor]);
    _rangeStateLabel.lineBreakMode = NSLineBreakByWordWrapping;
    _rangeStateLabel.maximumNumberOfLines = 2;
    [stack addArrangedSubview:_rangeStateLabel];

    _rangeSummaryLabel = MakeLabel(@"No replay window loaded yet.",
                                   [NSFont systemFontOfSize:12.0 weight:NSFontWeightRegular],
                                   [NSColor tertiaryLabelColor]);
    [stack addArrangedSubview:_rangeSummaryLabel];

    _rangeTableView = [[NSTableView alloc] initWithFrame:NSZeroRect];
    _rangeTableView.usesAlternatingRowBackgroundColors = YES;
    _rangeTableView.allowsMultipleSelection = NO;
    _rangeTableView.allowsEmptySelection = YES;
    _rangeTableView.delegate = self;
    _rangeTableView.dataSource = self;

    auto addColumn = ^(NSString* identifier, NSString* title, CGFloat width) {
        NSTableColumn* column = [[NSTableColumn alloc] initWithIdentifier:identifier];
        column.title = title;
        column.width = width;
        column.minWidth = 90.0;
        [_rangeTableView addTableColumn:column];
    };
    addColumn(@"session_seq", @"session_seq", 120.0);
    addColumn(@"source_seq", @"source_seq", 120.0);
    addColumn(@"event_kind", @"event_kind", 190.0);
    addColumn(@"wall_time", @"wall_time", 210.0);
    addColumn(@"summary", @"summary", 520.0);

    NSScrollView* tableScroll = [[NSScrollView alloc] initWithFrame:NSMakeRect(0, 0, 900, 280)];
    tableScroll.hasVerticalScroller = YES;
    tableScroll.hasHorizontalScroller = YES;
    tableScroll.borderType = NSBezelBorder;
    tableScroll.autoresizingMask = NSViewWidthSizable | NSViewHeightSizable;
    tableScroll.documentView = _rangeTableView;
    [tableScroll.heightAnchor constraintGreaterThanOrEqualToConstant:250.0].active = YES;
    [stack addArrangedSubview:tableScroll];

    NSTextField* detailLabel = MakeLabel(@"Decoded Event",
                                         [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                         [NSColor secondaryLabelColor]);
    [stack addArrangedSubview:detailLabel];

    _rangeDetailTextView = MakeReadOnlyTextView();
    _rangeDetailTextView.string = @"Select a row to inspect the decoded event payload.";
    NSScrollView* detailScroll = MakeScrollView(_rangeDetailTextView);
    detailScroll.hasHorizontalScroller = YES;
    [detailScroll.heightAnchor constraintGreaterThanOrEqualToConstant:150.0].active = YES;
    [stack addArrangedSubview:detailScroll];

    _activeRangeQuery.firstSessionSeq = kDefaultRangeFirstSessionSeq;
    _activeRangeQuery.lastSessionSeq = kDefaultRangeLastSessionSeq;
    _hasActiveRangeQuery = NO;
    _rangeRequestInFlight = NO;
    [self updateRangeControls];

    NSTabViewItem* item = [[NSTabViewItem alloc] initWithIdentifier:@"RangePane"];
    item.label = @"RangePane";
    item.view = pane;
    return item;
}

- (void)updateRangeControls {
    const BOOL controlsEnabled = !_rangeRequestInFlight;
    _rangeFirstField.enabled = controlsEnabled;
    _rangeLastField.enabled = controlsEnabled;
    _rangeFetchButton.enabled = controlsEnabled;
    _rangePrevButton.enabled = controlsEnabled && _hasActiveRangeQuery && _activeRangeQuery.firstSessionSeq > 1;
    _rangeNextButton.enabled = controlsEnabled && _hasActiveRangeQuery;
}

- (BOOL)parseRangeInputs:(tapescope::RangeQuery*)outQuery error:(std::string*)outError {
    if (outQuery == nullptr || outError == nullptr) {
        return NO;
    }

    std::uint64_t firstSessionSeq = 0;
    std::uint64_t lastSessionSeq = 0;
    if (!ParsePositiveUInt64(ToStdString(_rangeFirstField.stringValue), &firstSessionSeq)) {
        *outError = "first_session_seq must be a positive integer.";
        return NO;
    }

    if (!ParsePositiveUInt64(ToStdString(_rangeLastField.stringValue), &lastSessionSeq)) {
        *outError = "last_session_seq must be a positive integer.";
        return NO;
    }

    if (firstSessionSeq > lastSessionSeq) {
        *outError = "first_session_seq must be less than or equal to last_session_seq.";
        return NO;
    }

    outQuery->firstSessionSeq = firstSessionSeq;
    outQuery->lastSessionSeq = lastSessionSeq;
    return YES;
}

- (void)updateRangeDetailForSelection {
    const NSInteger selected = _rangeTableView.selectedRow;
    if (selected < 0 || static_cast<std::size_t>(selected) >= _rangeEvents.size()) {
        if (_rangeEvents.empty()) {
            _rangeDetailTextView.string = @"No decoded events to inspect for the current window.";
        } else {
            _rangeDetailTextView.string = @"Select a row to inspect the decoded event payload.";
        }
        return;
    }

    const json& event = _rangeEvents.at(static_cast<std::size_t>(selected));
    std::ostringstream out;
    out << "event_index: " << (selected + 1) << '\n';
    out << "event_count: " << _rangeEvents.size() << "\n\n";
    out << event.dump(2);
    _rangeDetailTextView.string = ToNSString(out.str());
}

- (void)queueRangeRequest:(const tapescope::RangeQuery&)query {
    if (_rangeRequestInFlight || !_client) {
        return;
    }

    _rangeRequestInFlight = YES;
    _activeRangeQuery = query;
    _hasActiveRangeQuery = YES;
    _rangeFirstField.stringValue = UInt64String(query.firstSessionSeq);
    _rangeLastField.stringValue = UInt64String(query.lastSessionSeq);
    _rangeStateLabel.stringValue = @"Requesting replay window…";
    _rangeStateLabel.textColor = [NSColor systemOrangeColor];
    _rangeSummaryLabel.stringValue = [NSString stringWithFormat:@"Fetching read_range [%llu, %llu]…",
                                                                static_cast<unsigned long long>(query.firstSessionSeq),
                                                                static_cast<unsigned long long>(query.lastSessionSeq)];
    [self updateRangeControls];

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_pollQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }

        const tapescope::QueryResult<std::vector<json>> rangeResult = strongSelf->_client->readRange(query);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil) {
                return;
            }
            [innerSelf applyRangeResult:rangeResult forQuery:query];
        });
    });
}

- (void)applyRangeResult:(const tapescope::QueryResult<std::vector<json>>&)result
                forQuery:(const tapescope::RangeQuery&)query {
    _rangeRequestInFlight = NO;
    _activeRangeQuery = query;
    _hasActiveRangeQuery = YES;

    _rangeEvents.clear();
    _rangeRows.clear();

    if (!result.ok()) {
        _rangeStateLabel.stringValue = ToNSString(tapescope::QueryClient::describeError(result.error));
        _rangeStateLabel.textColor = RangeErrorColor(result.error.kind);
        _rangeSummaryLabel.stringValue = [NSString stringWithFormat:@"read_range [%llu, %llu] failed.",
                                                                    static_cast<unsigned long long>(query.firstSessionSeq),
                                                                    static_cast<unsigned long long>(query.lastSessionSeq)];
        _rangeDetailTextView.string = @"Range window unavailable. Fix input or engine state, then retry.";
        [_rangeTableView reloadData];
        [self updateRangeControls];
        return;
    }

    _rangeEvents = result.value;
    _rangeRows.reserve(_rangeEvents.size());
    for (const json& event : _rangeEvents) {
        _rangeRows.push_back(BuildRangeRow(event));
    }
    [_rangeTableView reloadData];

    if (_rangeRows.empty()) {
        _rangeStateLabel.stringValue = @"read_range returned no events for this window.";
        _rangeStateLabel.textColor = [NSColor secondaryLabelColor];
        _rangeSummaryLabel.stringValue = [NSString stringWithFormat:@"Window [%llu, %llu] is empty.",
                                                                    static_cast<unsigned long long>(query.firstSessionSeq),
                                                                    static_cast<unsigned long long>(query.lastSessionSeq)];
        _rangeDetailTextView.string = @"No decoded events were returned for the requested window.";
        [self updateRangeControls];
        return;
    }

    _rangeStateLabel.stringValue = @"Replay window loaded.";
    _rangeStateLabel.textColor = [NSColor systemGreenColor];
    _rangeSummaryLabel.stringValue = [NSString stringWithFormat:@"Window [%llu, %llu] returned %llu events.",
                                                                static_cast<unsigned long long>(query.firstSessionSeq),
                                                                static_cast<unsigned long long>(query.lastSessionSeq),
                                                                static_cast<unsigned long long>(_rangeRows.size())];
    [_rangeTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0] byExtendingSelection:NO];
    [_rangeTableView scrollRowToVisible:0];
    [self updateRangeDetailForSelection];
    [self updateRangeControls];
}

- (void)showRangeValidationError:(const std::string&)errorMessage {
    _rangeStateLabel.stringValue = ToNSString(errorMessage);
    _rangeStateLabel.textColor = [NSColor systemRedColor];
    _rangeSummaryLabel.stringValue = @"Fix range inputs, then request again.";
}

- (void)fetchRange:(id)sender {
    (void)sender;
    tapescope::RangeQuery query;
    std::string validationError;
    if (![self parseRangeInputs:&query error:&validationError]) {
        [self showRangeValidationError:validationError];
        return;
    }
    [self queueRangeRequest:query];
}

- (void)fetchPreviousRange:(id)sender {
    (void)sender;
    tapescope::RangeQuery query;
    std::string validationError;
    if (![self parseRangeInputs:&query error:&validationError]) {
        [self showRangeValidationError:validationError];
        return;
    }

    if (query.firstSessionSeq == 1) {
        [self showRangeValidationError:"Already at the earliest replay window."];
        return;
    }

    const std::uint64_t span = query.lastSessionSeq - query.firstSessionSeq + 1;
    if (query.firstSessionSeq <= span) {
        query.firstSessionSeq = 1;
        query.lastSessionSeq = span;
    } else {
        query.firstSessionSeq -= span;
        query.lastSessionSeq -= span;
    }
    [self queueRangeRequest:query];
}

- (void)fetchNextRange:(id)sender {
    (void)sender;
    tapescope::RangeQuery query;
    std::string validationError;
    if (![self parseRangeInputs:&query error:&validationError]) {
        [self showRangeValidationError:validationError];
        return;
    }

    const std::uint64_t span = query.lastSessionSeq - query.firstSessionSeq + 1;
    if (query.lastSessionSeq > std::numeric_limits<std::uint64_t>::max() - span) {
        [self showRangeValidationError:"Cannot advance window past uint64 session_seq bounds."];
        return;
    }
    query.firstSessionSeq += span;
    query.lastSessionSeq += span;
    [self queueRangeRequest:query];
}

- (NSTabViewItem*)orderTabItem {
    NSView* pane = [[NSView alloc] initWithFrame:NSZeroRect];

    NSStackView* stack = MakeColumnStack(10.0);
    [pane addSubview:stack];
    [NSLayoutConstraint activateConstraints:@[
        [stack.leadingAnchor constraintEqualToAnchor:pane.leadingAnchor constant:10.0],
        [stack.trailingAnchor constraintEqualToAnchor:pane.trailingAnchor constant:-10.0],
        [stack.topAnchor constraintEqualToAnchor:pane.topAnchor constant:10.0],
        [stack.bottomAnchor constraintEqualToAnchor:pane.bottomAnchor constant:-10.0]
    ]];

    NSTextField* intro = MakeLabel(
        @"Order anchor lookup: choose one anchor selector and query find_order_anchor for matching context.",
        [NSFont systemFontOfSize:12.5 weight:NSFontWeightMedium],
        [NSColor secondaryLabelColor]);
    intro.lineBreakMode = NSLineBreakByWordWrapping;
    intro.maximumNumberOfLines = 2;
    [stack addArrangedSubview:intro];

    NSStackView* controls = [[NSStackView alloc] initWithFrame:NSZeroRect];
    controls.orientation = NSUserInterfaceLayoutOrientationHorizontal;
    controls.alignment = NSLayoutAttributeCenterY;
    controls.spacing = 8.0;
    controls.translatesAutoresizingMaskIntoConstraints = NO;

    [controls addArrangedSubview:MakeLabel(@"anchor_type",
                                           [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                           [NSColor secondaryLabelColor])];
    _orderAnchorTypePopup = [[NSPopUpButton alloc] initWithFrame:NSMakeRect(0, 0, 120, 24) pullsDown:NO];
    [_orderAnchorTypePopup addItemsWithTitles:@[@"traceId", @"orderId", @"permId", @"execId"]];
    _orderAnchorTypePopup.target = self;
    _orderAnchorTypePopup.action = @selector(orderAnchorTypeChanged:);
    [controls addArrangedSubview:_orderAnchorTypePopup];

    [controls addArrangedSubview:MakeLabel(@"anchor_value",
                                           [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                           [NSColor secondaryLabelColor])];
    _orderAnchorInputField = [[NSTextField alloc] initWithFrame:NSMakeRect(0, 0, 200, 24)];
    _orderAnchorInputField.font = [NSFont monospacedSystemFontOfSize:12.0 weight:NSFontWeightMedium];
    _orderAnchorInputField.placeholderString = PlaceholderForOrderAnchorType(OrderAnchorType::TraceId);
    _orderAnchorInputField.target = self;
    _orderAnchorInputField.action = @selector(performOrderLookup:);
    [controls addArrangedSubview:_orderAnchorInputField];

    _orderLookupButton = [NSButton buttonWithTitle:@"Find Order Anchor"
                                            target:self
                                            action:@selector(performOrderLookup:)];
    [controls addArrangedSubview:_orderLookupButton];
    [stack addArrangedSubview:controls];

    _orderStateLabel = MakeLabel(@"Enter one anchor value and click Find Order Anchor.",
                                 [NSFont systemFontOfSize:12.0 weight:NSFontWeightMedium],
                                 [NSColor secondaryLabelColor]);
    _orderStateLabel.lineBreakMode = NSLineBreakByWordWrapping;
    _orderStateLabel.maximumNumberOfLines = 2;
    [stack addArrangedSubview:_orderStateLabel];

    _orderSummaryLabel = MakeLabel(@"No lookup issued yet.",
                                   [NSFont systemFontOfSize:12.0 weight:NSFontWeightRegular],
                                   [NSColor tertiaryLabelColor]);
    [stack addArrangedSubview:_orderSummaryLabel];

    _orderTextView = MakeReadOnlyTextView();
    _orderTextView.string = @"Lookup results will appear here.";
    NSScrollView* resultScroll = MakeScrollView(_orderTextView);
    resultScroll.hasHorizontalScroller = YES;
    [resultScroll.heightAnchor constraintGreaterThanOrEqualToConstant:320.0].active = YES;
    [stack addArrangedSubview:resultScroll];

    _orderLookupInFlight = NO;
    [self updateOrderLookupControls];

    NSTabViewItem* item = [[NSTabViewItem alloc] initWithIdentifier:@"OrderLookupPane"];
    item.label = @"OrderLookupPane";
    item.view = pane;
    return item;
}

- (void)updateOrderLookupControls {
    const BOOL controlsEnabled = !_orderLookupInFlight;
    _orderAnchorTypePopup.enabled = controlsEnabled;
    _orderAnchorInputField.enabled = controlsEnabled;
    _orderLookupButton.enabled = controlsEnabled;
}

- (void)orderAnchorTypeChanged:(id)sender {
    (void)sender;
    const OrderAnchorType type = OrderAnchorTypeFromIndex(_orderAnchorTypePopup.indexOfSelectedItem);
    _orderAnchorInputField.placeholderString = PlaceholderForOrderAnchorType(type);
}

- (BOOL)buildOrderLookupQuery:(tapescope::OrderAnchorQuery*)outQuery
                   descriptor:(std::string*)outDescriptor
                        error:(std::string*)outError {
    if (outQuery == nullptr || outDescriptor == nullptr || outError == nullptr) {
        return NO;
    }

    *outQuery = tapescope::OrderAnchorQuery{};
    const OrderAnchorType type = OrderAnchorTypeFromIndex(_orderAnchorTypePopup.indexOfSelectedItem);
    const std::string rawValue = TrimAscii(ToStdString(_orderAnchorInputField.stringValue));
    if (rawValue.empty()) {
        *outError = "anchor_value is required.";
        return NO;
    }

    switch (type) {
        case OrderAnchorType::TraceId: {
            std::uint64_t traceId = 0;
            if (!ParsePositiveUInt64(rawValue, &traceId)) {
                *outError = "traceId must be a positive integer.";
                return NO;
            }
            outQuery->traceId = traceId;
            *outDescriptor = "traceId=" + std::to_string(traceId);
            return YES;
        }
        case OrderAnchorType::OrderId: {
            long long orderId = 0;
            if (!ParsePositiveInt64(rawValue, &orderId)) {
                *outError = "orderId must be a positive integer.";
                return NO;
            }
            outQuery->orderId = orderId;
            *outDescriptor = "orderId=" + std::to_string(orderId);
            return YES;
        }
        case OrderAnchorType::PermId: {
            long long permId = 0;
            if (!ParsePositiveInt64(rawValue, &permId)) {
                *outError = "permId must be a positive integer.";
                return NO;
            }
            outQuery->permId = permId;
            *outDescriptor = "permId=" + std::to_string(permId);
            return YES;
        }
        case OrderAnchorType::ExecId:
            outQuery->execId = rawValue;
            *outDescriptor = "execId=" + rawValue;
            return YES;
    }

    *outError = "Unsupported anchor selector.";
    return NO;
}

- (void)showOrderValidationError:(const std::string&)errorMessage {
    _orderStateLabel.stringValue = ToNSString(errorMessage);
    _orderStateLabel.textColor = [NSColor systemRedColor];
    _orderSummaryLabel.stringValue = @"Fix lookup input, then retry.";
}

- (void)performOrderLookup:(id)sender {
    (void)sender;
    if (_orderLookupInFlight || !_client) {
        return;
    }

    tapescope::OrderAnchorQuery query;
    std::string descriptor;
    std::string validationError;
    if (![self buildOrderLookupQuery:&query descriptor:&descriptor error:&validationError]) {
        [self showOrderValidationError:validationError];
        return;
    }
    [self queueOrderLookup:query descriptor:descriptor];
}

- (void)queueOrderLookup:(const tapescope::OrderAnchorQuery&)query descriptor:(const std::string&)descriptor {
    if (_orderLookupInFlight || !_client) {
        return;
    }

    _orderLookupInFlight = YES;
    _orderStateLabel.stringValue = @"Requesting order anchor…";
    _orderStateLabel.textColor = [NSColor systemOrangeColor];
    _orderSummaryLabel.stringValue = ToNSString(std::string("Running find_order_anchor for ") + descriptor + "...");
    [self updateOrderLookupControls];

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_pollQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }

        const tapescope::QueryResult<json> lookupResult = strongSelf->_client->findOrderAnchor(query);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil) {
                return;
            }
            [innerSelf applyOrderLookupResult:lookupResult descriptor:descriptor];
        });
    });
}

- (void)applyOrderLookupResult:(const tapescope::QueryResult<json>&)result
                    descriptor:(const std::string&)descriptor {
    _orderLookupInFlight = NO;

    if (!result.ok()) {
        if (IsOrderLookupNotFoundError(result.error)) {
            _orderStateLabel.stringValue = @"No matching order anchor found.";
            _orderStateLabel.textColor = [NSColor secondaryLabelColor];
            _orderSummaryLabel.stringValue = ToNSString(std::string("Lookup for ") + descriptor + " returned no match.");
            _orderTextView.string = ToNSString(std::string("Engine reported no matching order anchor for ") + descriptor + '.');
            [self updateOrderLookupControls];
            return;
        }

        _orderStateLabel.stringValue = ToNSString(tapescope::QueryClient::describeError(result.error));
        _orderStateLabel.textColor = RangeErrorColor(result.error.kind);
        _orderSummaryLabel.stringValue = ToNSString(std::string("Lookup failed for ") + descriptor + '.');
        _orderTextView.string = ToNSString(std::string("Order lookup failed.\n\n") + tapescope::QueryClient::describeError(result.error));
        [self updateOrderLookupControls];
        return;
    }

    if (IsOrderLookupNotFoundPayload(result.value)) {
        _orderStateLabel.stringValue = @"No matching order anchor found.";
        _orderStateLabel.textColor = [NSColor secondaryLabelColor];
        _orderSummaryLabel.stringValue = ToNSString(std::string("Lookup for ") + descriptor + " returned no match.");
        std::string details = "No order anchor matched the requested selector.\n";
        if (!result.value.is_null()) {
            details += "\nRaw payload:\n";
            details += result.value.dump(2);
        }
        _orderTextView.string = ToNSString(details);
        [self updateOrderLookupControls];
        return;
    }

    const std::vector<json> contextEvents = ExtractOrderContextEvents(result.value);
    _orderStateLabel.stringValue = @"Order anchor lookup completed.";
    _orderStateLabel.textColor = [NSColor systemGreenColor];
    if (contextEvents.empty()) {
        _orderSummaryLabel.stringValue = ToNSString(std::string("Lookup for ") + descriptor + " returned an anchor payload.");
    } else {
        _orderSummaryLabel.stringValue =
            ToNSString(std::string("Lookup for ") + descriptor + " returned " + std::to_string(contextEvents.size()) + " context events.");
    }
    _orderTextView.string = ToNSString(DescribeOrderLookupResult(result.value, descriptor));
    [self updateOrderLookupControls];
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
        @"Wave 4: status, mutable live-tail, replay range, and order anchor lookup panes backed only by QueryClient seam commands.",
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
    [tabView addTabViewItem:[self rangeTabItem]];
    [tabView addTabViewItem:[self orderTabItem]];
    [tabView.heightAnchor constraintGreaterThanOrEqualToConstant:430.0].active = YES;
    [root addArrangedSubview:tabView];

    _statusTextView.string = @"Waiting for first status response…";
    _liveTextView.string = @"Waiting for first live-tail response…";
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

- (NSInteger)numberOfRowsInTableView:(NSTableView*)tableView {
    if (tableView == _rangeTableView) {
        return static_cast<NSInteger>(_rangeRows.size());
    }
    return 0;
}

- (NSView*)tableView:(NSTableView*)tableView
   viewForTableColumn:(NSTableColumn*)tableColumn
                  row:(NSInteger)row {
    if (tableView != _rangeTableView || tableColumn == nil) {
        return nil;
    }
    if (row < 0 || static_cast<std::size_t>(row) >= _rangeRows.size()) {
        return nil;
    }

    NSString* columnId = tableColumn.identifier ?: @"";
    NSTableCellView* cell = [tableView makeViewWithIdentifier:columnId owner:self];
    if (cell == nil) {
        cell = [[NSTableCellView alloc] initWithFrame:NSZeroRect];
        cell.identifier = columnId;

        NSTextField* label = [NSTextField labelWithString:@""];
        label.translatesAutoresizingMaskIntoConstraints = NO;
        label.lineBreakMode = NSLineBreakByTruncatingTail;
        [cell addSubview:label];
        [NSLayoutConstraint activateConstraints:@[
            [label.leadingAnchor constraintEqualToAnchor:cell.leadingAnchor constant:4.0],
            [label.trailingAnchor constraintEqualToAnchor:cell.trailingAnchor constant:-4.0],
            [label.centerYAnchor constraintEqualToAnchor:cell.centerYAnchor]
        ]];
        cell.textField = label;
    }

    const RangeRow& rangeRow = _rangeRows.at(static_cast<std::size_t>(row));
    std::string value;
    if ([columnId isEqualToString:@"session_seq"]) {
        value = rangeRow.sessionSeq;
    } else if ([columnId isEqualToString:@"source_seq"]) {
        value = rangeRow.sourceSeq;
    } else if ([columnId isEqualToString:@"event_kind"]) {
        value = rangeRow.eventKind;
    } else if ([columnId isEqualToString:@"wall_time"]) {
        value = rangeRow.wallTime;
    } else {
        value = rangeRow.summary;
    }

    cell.textField.font = [columnId isEqualToString:@"summary"]
                              ? [NSFont systemFontOfSize:12.0 weight:NSFontWeightRegular]
                              : [NSFont monospacedSystemFontOfSize:11.5 weight:NSFontWeightRegular];
    cell.textField.stringValue = ToNSString(value);
    return cell;
}

- (void)tableViewSelectionDidChange:(NSNotification*)notification {
    if (notification.object == _rangeTableView) {
        [self updateRangeDetailForSelection];
    }
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
