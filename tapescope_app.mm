#import <AppKit/AppKit.h>

#include "tapescope_client.h"

#include <dispatch/dispatch.h>

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

@interface ActionTableView : NSTableView
@property(nonatomic, weak) id primaryActionTarget;
@property(nonatomic) SEL primaryAction;
@end

@implementation ActionTableView

- (void)performPrimaryActionFrom:(id)sender {
    if (self.primaryActionTarget == nil || self.primaryAction == nullptr || self.selectedRow < 0) {
        return;
    }
    [NSApp sendAction:self.primaryAction to:self.primaryActionTarget from:sender];
}

- (void)keyDown:(NSEvent*)event {
    NSString* chars = event.charactersIgnoringModifiers ?: @"";
    if (([chars isEqualToString:@"\r"] || [chars isEqualToString:@" "] || [chars isEqualToString:@"\n"]) &&
        self.selectedRow >= 0 && self.primaryAction != nullptr) {
        [self performPrimaryActionFrom:self];
        return;
    }
    [super keyDown:event];
}

@end

namespace {

using tapescope::json;

constexpr NSTimeInterval kPollIntervalSeconds = 2.0;
constexpr std::size_t kLiveTailLimit = 32;
constexpr std::size_t kRecentHistoryLimit = 24;
NSString* const kTapeScopeStateDefaultsKey = @"TapeScopeStateV1";

NSColor* TapeBackgroundColor() {
    return [NSColor colorWithCalibratedRed:0.953 green:0.941 blue:0.914 alpha:1.0];
}

NSColor* TapeCardColor() {
    return [NSColor colorWithCalibratedRed:0.986 green:0.979 blue:0.965 alpha:0.96];
}

NSColor* TapeCardBorderColor() {
    return [NSColor colorWithCalibratedRed:0.835 green:0.792 blue:0.706 alpha:0.9];
}

NSColor* TapePanelFillColor() {
    return [NSColor colorWithCalibratedRed:0.994 green:0.991 blue:0.982 alpha:1.0];
}

NSColor* TapePanelBorderColor() {
    return [NSColor colorWithCalibratedRed:0.869 green:0.833 blue:0.761 alpha:0.95];
}

NSColor* TapeInkMutedColor() {
    return [NSColor colorWithCalibratedRed:0.345 green:0.333 blue:0.302 alpha:1.0];
}

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

std::string TrimAscii(std::string value) {
    const auto begin = std::find_if_not(value.begin(), value.end(), [](unsigned char ch) {
        return std::isspace(ch) != 0;
    });
    const auto end = std::find_if_not(value.rbegin(), value.rend(), [](unsigned char ch) {
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
    const std::string value = TrimAscii(raw);
    if (value.empty()) {
        return false;
    }
    char* consumed = nullptr;
    errno = 0;
    const unsigned long long converted = std::strtoull(value.c_str(), &consumed, 10);
    if (errno == ERANGE || consumed == nullptr || *consumed != '\0' || converted == 0) {
        return false;
    }
    *parsed = static_cast<std::uint64_t>(converted);
    return true;
}

bool ParsePositiveInt64(const std::string& raw, long long* parsed) {
    if (parsed == nullptr) {
        return false;
    }
    std::uint64_t value = 0;
    if (!ParsePositiveUInt64(raw, &value) ||
        value > static_cast<std::uint64_t>(std::numeric_limits<long long>::max())) {
        return false;
    }
    *parsed = static_cast<long long>(value);
    return true;
}

NSString* UInt64String(std::uint64_t value) {
    return [NSString stringWithFormat:@"%llu", static_cast<unsigned long long>(value)];
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
                     TapeInkMutedColor());
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
    textView.backgroundColor = TapePanelFillColor();
    textView.textContainerInset = NSMakeSize(12.0, 12.0);
    return textView;
}

NSScrollView* MakeScrollView(NSTextView* textView, CGFloat minHeight) {
    NSScrollView* scrollView = [[NSScrollView alloc] initWithFrame:NSMakeRect(0, 0, 800, minHeight)];
    scrollView.hasVerticalScroller = YES;
    scrollView.hasHorizontalScroller = YES;
    scrollView.borderType = NSLineBorder;
    scrollView.backgroundColor = TapePanelFillColor();
    scrollView.autoresizingMask = NSViewWidthSizable | NSViewHeightSizable;
    scrollView.documentView = textView;
    [scrollView.heightAnchor constraintGreaterThanOrEqualToConstant:minHeight].active = YES;
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

NSBox* MakeCardBox(CGFloat cornerRadius = 18.0) {
    NSBox* box = [[NSBox alloc] initWithFrame:NSZeroRect];
    box.boxType = NSBoxCustom;
    box.cornerRadius = cornerRadius;
    box.borderWidth = 1.0;
    box.fillColor = TapeCardColor();
    box.borderColor = TapeCardBorderColor();
    box.translatesAutoresizingMaskIntoConstraints = NO;
    return box;
}

NSView* MakePaneWithStack(NSStackView* __strong* outStack) {
    NSView* pane = [[NSView alloc] initWithFrame:NSZeroRect];
    NSStackView* stack = MakeColumnStack(10.0);
    [pane addSubview:stack];
    [NSLayoutConstraint activateConstraints:@[
        [stack.leadingAnchor constraintEqualToAnchor:pane.leadingAnchor constant:10.0],
        [stack.trailingAnchor constraintEqualToAnchor:pane.trailingAnchor constant:-10.0],
        [stack.topAnchor constraintEqualToAnchor:pane.topAnchor constant:10.0],
        [stack.bottomAnchor constraintEqualToAnchor:pane.bottomAnchor constant:-10.0]
    ]];
    if (outStack != nullptr) {
        *outStack = stack;
    }
    return pane;
}

NSBox* MakeCardWithStack(NSStackView* __strong* outStack, CGFloat spacing = 10.0) {
    NSBox* card = MakeCardBox();
    NSStackView* stack = MakeColumnStack(spacing);
    [card addSubview:stack];
    [NSLayoutConstraint activateConstraints:@[
        [stack.leadingAnchor constraintEqualToAnchor:card.leadingAnchor constant:16.0],
        [stack.trailingAnchor constraintEqualToAnchor:card.trailingAnchor constant:-16.0],
        [stack.topAnchor constraintEqualToAnchor:card.topAnchor constant:16.0],
        [stack.bottomAnchor constraintEqualToAnchor:card.bottomAnchor constant:-16.0]
    ]];
    if (outStack != nullptr) {
        *outStack = stack;
    }
    return card;
}

NSTextField* MakeIntroLabel(NSString* text, NSInteger lines = 1) {
    NSTextField* label = MakeLabel(text,
                                   [NSFont systemFontOfSize:12.5 weight:NSFontWeightMedium],
                                   TapeInkMutedColor());
    label.lineBreakMode = NSLineBreakByWordWrapping;
    label.maximumNumberOfLines = lines;
    return label;
}

NSTextField* MakeSectionLabel(NSString* text) {
    return MakeLabel(text,
                     [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                     TapeInkMutedColor());
}

NSStackView* MakeControlRow() {
    NSStackView* controls = [[NSStackView alloc] initWithFrame:NSZeroRect];
    controls.orientation = NSUserInterfaceLayoutOrientationHorizontal;
    controls.alignment = NSLayoutAttributeCenterY;
    controls.spacing = 8.0;
    controls.translatesAutoresizingMaskIntoConstraints = NO;
    return controls;
}

NSTextField* MakeMonospacedField(CGFloat width,
                                 NSString* initialValue = nil,
                                 NSString* placeholder = nil) {
    NSTextField* field = [[NSTextField alloc] initWithFrame:NSMakeRect(0, 0, width, 24)];
    field.font = [NSFont monospacedSystemFontOfSize:12.0 weight:NSFontWeightMedium];
    if (initialValue != nil) {
        field.stringValue = initialValue;
    }
    if (placeholder != nil) {
        field.placeholderString = placeholder;
    }
    return field;
}

NSTableView* MakeStandardTableView(id delegate, id dataSource) {
    NSTableView* tableView = [[ActionTableView alloc] initWithFrame:NSZeroRect];
    tableView.usesAlternatingRowBackgroundColors = NO;
    tableView.allowsEmptySelection = YES;
    tableView.allowsMultipleSelection = NO;
    tableView.rowHeight = 28.0;
    tableView.intercellSpacing = NSMakeSize(6.0, 4.0);
    tableView.selectionHighlightStyle = NSTableViewSelectionHighlightStyleRegular;
    tableView.gridStyleMask = NSTableViewSolidHorizontalGridLineMask;
    tableView.gridColor = [TapePanelBorderColor() colorWithAlphaComponent:0.45];
    tableView.backgroundColor = TapePanelFillColor();
    tableView.delegate = delegate;
    tableView.dataSource = dataSource;
    if (@available(macOS 11.0, *)) {
        tableView.style = NSTableViewStyleInset;
    }
    return tableView;
}

void ConfigureTablePrimaryAction(NSTableView* tableView, id target, SEL action) {
    if (tableView == nil || action == nullptr) {
        return;
    }
    tableView.target = target;
    tableView.doubleAction = action;
    if ([tableView isKindOfClass:[ActionTableView class]]) {
        ActionTableView* actionTableView = (ActionTableView*)tableView;
        actionTableView.primaryActionTarget = target;
        actionTableView.primaryAction = action;
    }
}

void AddTableColumn(NSTableView* tableView,
                    NSString* identifier,
                    NSString* title,
                    CGFloat width) {
    NSTableColumn* column = [[NSTableColumn alloc] initWithIdentifier:identifier];
    column.title = title;
    column.width = width;
    column.minWidth = 90.0;
    [tableView addTableColumn:column];
}

NSScrollView* MakeTableScrollView(NSTableView* tableView, CGFloat minHeight) {
    NSScrollView* scrollView = [[NSScrollView alloc] initWithFrame:NSMakeRect(0, 0, 900, minHeight)];
    scrollView.hasVerticalScroller = YES;
    scrollView.hasHorizontalScroller = YES;
    scrollView.borderType = NSLineBorder;
    scrollView.backgroundColor = TapePanelFillColor();
    scrollView.autoresizingMask = NSViewWidthSizable | NSViewHeightSizable;
    scrollView.documentView = tableView;
    [scrollView.heightAnchor constraintGreaterThanOrEqualToConstant:minHeight].active = YES;
    return scrollView;
}

NSTableView* MakeEvidenceTableView(id delegate, id dataSource) {
    NSTableView* tableView = MakeStandardTableView(delegate, dataSource);
    AddTableColumn(tableView, @"kind", @"kind", 130.0);
    AddTableColumn(tableView, @"artifact_id", @"artifact_id", 260.0);
    AddTableColumn(tableView, @"label", @"label", 360.0);
    return tableView;
}

NSTableCellView* MakeOrReuseTableCell(NSTableView* tableView,
                                      NSString* identifier,
                                      NSFont* font) {
    NSTableCellView* cell = [tableView makeViewWithIdentifier:identifier owner:nil];
    if (cell == nil) {
        cell = [[NSTableCellView alloc] initWithFrame:NSZeroRect];
        cell.identifier = identifier;

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
    cell.textField.font = font;
    return cell;
}

NSColor* ErrorColorForKind(tapescope::QueryErrorKind kind) {
    switch (kind) {
        case tapescope::QueryErrorKind::Transport:
            return [NSColor systemRedColor];
        case tapescope::QueryErrorKind::Remote:
            return [NSColor systemOrangeColor];
        case tapescope::QueryErrorKind::MalformedResponse:
            return [NSColor systemYellowColor];
        case tapescope::QueryErrorKind::None:
            break;
    }
    return [NSColor secondaryLabelColor];
}

template <typename T>
std::string DescribeQueryResult(const tapescope::QueryResult<T>& result) {
    if (result.ok()) {
        return {};
    }
    return tapescope::QueryClient::describeError(result.error);
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
        if (it != payload.end() && it->is_number_unsigned()) {
            return it->get<std::uint64_t>();
        }
        if (it != payload.end() && it->is_number_integer()) {
            const long long value = it->get<long long>();
            if (value >= 0) {
                return static_cast<std::uint64_t>(value);
            }
        }
    }
    return 0;
}

std::string DescribeStatusPane(const tapescope::QueryResult<tapescope::StatusSnapshot>& result,
                               const std::string& configuredSocketPath) {
    std::ostringstream out;
    out << "configured_socket_path: " << configuredSocketPath << "\n";
    if (!result.ok()) {
        out << "status: unavailable\n";
        out << "error: " << tapescope::QueryClient::describeError(result.error) << "\n";
        return out.str();
    }

    const auto& status = result.value;
    out << "status: connected\n";
    out << "socket_path: " << status.socketPath << "\n";
    out << "data_dir: " << status.dataDir << "\n";
    out << "instrument_id: " << status.instrumentId << "\n";
    out << "latest_session_seq: " << status.latestSessionSeq << "\n";
    out << "live_event_count: " << status.liveEventCount << "\n";
    out << "segment_count: " << status.segmentCount << "\n";
    out << "manifest_hash: " << status.manifestHash << "\n";
    return out.str();
}

std::string DescribeLiveEventsPane(const tapescope::QueryResult<std::vector<json>>& result) {
    if (!result.ok()) {
        return tapescope::QueryClient::describeError(result.error);
    }
    if (result.value.empty()) {
        return "No live events returned.";
    }

    std::ostringstream out;
    for (std::size_t index = 0; index < result.value.size(); ++index) {
        const json& event = result.value[index];
        out << '[' << index + 1 << "] session_seq=" << event.value("session_seq", 0ULL)
            << " source_seq=" << event.value("source_seq", 0ULL)
            << " kind=" << event.value("event_kind", std::string())
            << " instrument=" << event.value("instrument_id", std::string());
        const std::string side = FirstPresentString(event, {"side"});
        if (!side.empty()) {
            out << " side=" << side;
        }
        const auto priceIt = event.find("price");
        if (priceIt != event.end() && priceIt->is_number()) {
            out << " price=" << priceIt->get<double>();
        }
        out << '\n';
        const std::string note = FirstPresentString(event, {"note", "details", "summary", "message"});
        if (!note.empty()) {
            out << "    " << note << '\n';
        }
        out << event.dump(2) << "\n\n";
    }
    return out.str();
}

std::string DescribeRangeResult(const tapescope::RangeQuery& query,
                                const tapescope::QueryResult<std::vector<json>>& result) {
    std::ostringstream out;
    out << "requested_range: [" << query.firstSessionSeq << ", " << query.lastSessionSeq << "]\n\n";
    if (!result.ok()) {
        out << tapescope::QueryClient::describeError(result.error) << '\n';
        return out.str();
    }
    if (result.value.empty()) {
        out << "No events returned.\n";
        return out.str();
    }
    for (const auto& event : result.value) {
        out << "session_seq=" << event.value("session_seq", 0ULL)
            << " source_seq=" << event.value("source_seq", 0ULL)
            << " kind=" << event.value("event_kind", std::string()) << '\n';
        out << event.dump(2) << "\n\n";
    }
    return out.str();
}

std::string EventSummaryText(const json& event) {
    std::string summary = FirstPresentString(event, {"summary", "note", "details", "message"});
    if (!summary.empty()) {
        return summary;
    }
    const std::string kind = event.value("event_kind", std::string());
    const std::string side = event.value("side", std::string());
    const auto priceIt = event.find("price");
    if (priceIt != event.end() && priceIt->is_number()) {
        std::ostringstream out;
        out << kind;
        if (!side.empty()) {
            out << " " << side;
        }
        out << " @ " << priceIt->get<double>();
        return out.str();
    }
    return kind;
}

std::string DescribeOrderLookupResult(const std::string& descriptor,
                                      const tapescope::QueryResult<json>& result) {
    std::ostringstream out;
    out << "lookup: " << descriptor << "\n\n";
    if (!result.ok()) {
        out << tapescope::QueryClient::describeError(result.error) << '\n';
        return out.str();
    }

    const json& payload = result.value;
    const json summary = payload.value("summary", json::object());
    const json events = payload.value("events", json::array());
    out << "event_count: " << (events.is_array() ? events.size() : 0) << "\n";
    if (!summary.is_null()) {
        out << "\nsummary:\n" << summary.dump(2) << "\n";
    }
    if (events.is_array()) {
        out << "\nanchored_events:\n";
        for (const auto& event : events) {
            out << "- session_seq=" << event.value("session_seq", 0ULL)
                << " kind=" << event.value("event_kind", std::string()) << '\n';
        }
        out << "\nraw_events:\n" << events.dump(2) << '\n';
    }
    return out.str();
}

std::string DescribeInvestigationPayload(const std::string& heading,
                                         const std::string& descriptor,
                                         const tapescope::QueryResult<json>& result) {
    std::ostringstream out;
    out << heading << '\n';
    if (!descriptor.empty()) {
        out << "query: " << descriptor << "\n\n";
    } else {
        out << '\n';
    }
    if (!result.ok()) {
        out << tapescope::QueryClient::describeError(result.error) << '\n';
        return out.str();
    }

    const json& payload = result.value;
    const json summary = payload.value("summary", json::object());
    const json events = payload.value("events", json::array());

    if (summary.is_object()) {
        const json artifact = summary.value("artifact", json::object());
        const json report = summary.value("report", json::object());
        const json evidence = summary.value("evidence", json::object());
        if (!artifact.is_null() && !artifact.empty()) {
            out << "artifact_id: " << artifact.value("id", std::string("--")) << '\n';
            out << "artifact_kind: " << artifact.value("kind", std::string("--")) << '\n';
        }
        if (!report.is_null() && !report.empty()) {
            const auto highlightsIt = report.find("highlights");
            const std::size_t highlightCount = (highlightsIt != report.end() && highlightsIt->is_array())
                                                   ? highlightsIt->size()
                                                   : 0U;
            out << "report_title: " << report.value("title", std::string("--")) << '\n';
            out << "report_highlights: " << highlightCount << '\n';
        }
        if (!evidence.is_null() && !evidence.empty()) {
            out << "evidence_sections: " << evidence.size() << '\n';
        }
    }

    out << "event_count: " << (events.is_array() ? events.size() : 0) << "\n\n";
    out << "summary:\n" << summary.dump(2) << "\n";
    if (events.is_array()) {
        out << "\nevents:\n";
        for (const auto& event : events) {
            out << "- session_seq=" << event.value("session_seq", 0ULL)
                << " kind=" << event.value("event_kind", std::string()) << '\n';
        }
        out << "\nraw_events:\n" << events.dump(2) << '\n';
    }
    return out.str();
}

std::string ExtractArtifactId(const json& payload) {
    if (!payload.is_object()) {
        return {};
    }
    const json summary = payload.value("summary", json::object());
    if (!summary.is_object()) {
        return {};
    }
    const json artifact = summary.value("artifact", json::object());
    if (!artifact.is_object()) {
        return {};
    }
    return artifact.value("artifact_id", artifact.value("id", std::string()));
}

std::string RecentHistoryHeadlineForPayload(const json& payload,
                                           const std::string& fallbackHeadline) {
    if (!payload.is_object()) {
        return fallbackHeadline;
    }
    const json summary = payload.value("summary", json::object());
    const json report = summary.value("report", json::object());
    const json entity = summary.value("entity", json::object());
    const json artifact = summary.value("artifact", json::object());

    std::string headline = FirstPresentString(report, {"title", "headline"});
    if (!headline.empty()) {
        return headline;
    }
    headline = FirstPresentString(summary, {"headline", "title", "why_it_matters"});
    if (!headline.empty()) {
        return headline;
    }
    headline = FirstPresentString(entity, {"headline", "label", "kind"});
    if (!headline.empty()) {
        return headline;
    }
    headline = FirstPresentString(artifact, {"headline", "id", "artifact_id"});
    if (!headline.empty()) {
        return headline;
    }
    return fallbackHeadline;
}

std::string RecentHistoryDetailForPayload(const json& payload,
                                         const std::string& fallbackDetail) {
    if (!payload.is_object()) {
        return fallbackDetail;
    }
    const json summary = payload.value("summary", json::object());
    const json report = summary.value("report", json::object());
    const json timelineSummary = summary.value("timeline_summary", json::object());
    std::string detail = FirstPresentString(report, {"summary", "why_it_matters"});
    if (!detail.empty()) {
        return detail;
    }
    detail = FirstPresentString(summary, {"what_changed_first", "why_it_matters", "headline"});
    if (!detail.empty()) {
        return detail;
    }
    detail = FirstPresentString(timelineSummary, {"headline", "summary", "description"});
    if (!detail.empty()) {
        return detail;
    }
    return fallbackDetail;
}

std::string DescribeRecentHistoryEntry(const json& entry) {
    std::ostringstream out;
    out << "kind: " << entry.value("kind", std::string("--")) << "\n";
    out << "target_id: " << entry.value("target_id", std::string("--")) << "\n";
    const std::string artifactId = entry.value("artifact_id", std::string());
    if (!artifactId.empty()) {
        out << "artifact_id: " << artifactId << "\n";
    }
    const auto firstSessionSeq = entry.value("first_session_seq", 0ULL);
    const auto lastSessionSeq = entry.value("last_session_seq", 0ULL);
    if (firstSessionSeq > 0 && lastSessionSeq >= firstSessionSeq) {
        out << "session_seq: [" << firstSessionSeq << ", " << lastSessionSeq << "]\n";
    }
    const std::string anchorKind = entry.value("anchor_kind", std::string());
    const std::string anchorValue = entry.value("anchor_value", std::string());
    if (!anchorKind.empty() && !anchorValue.empty()) {
        out << "anchor: " << anchorKind << "=" << anchorValue << "\n";
    }
    out << "\nheadline:\n" << entry.value("headline", std::string("--")) << "\n";
    out << "\ndetail:\n" << entry.value("detail", std::string("--")) << "\n";
    return out.str();
}

std::string EvidenceCitationLabel(const json& citation) {
    if (!citation.is_object()) {
        return {};
    }
    const std::string label = citation.value("label", std::string());
    if (!label.empty()) {
        return label;
    }
    const std::string headline = citation.value("headline", std::string());
    if (!headline.empty()) {
        return headline;
    }
    const std::string artifactId = citation.value("artifact_id", std::string());
    if (!artifactId.empty()) {
        return artifactId;
    }
    const std::string type = citation.value("type", citation.value("kind", std::string()));
    if (!type.empty()) {
        return type;
    }
    return citation.dump();
}

json ExtractEvidenceCitations(const json& payload) {
    if (!payload.is_object()) {
        return json::array();
    }
    const json summary = payload.value("summary", json::object());
    if (!summary.is_object()) {
        return json::array();
    }
    const json evidence = summary.value("evidence", json::object());
    if (!evidence.is_object()) {
        return json::array();
    }
    const json citations = evidence.value("citations", json::array());
    if (!citations.is_array()) {
        return json::array();
    }
    json normalized = json::array();
    for (const auto& citation : citations) {
        if (!citation.is_object()) {
            continue;
        }
        json row = citation;
        if (!row.contains("kind")) {
            row["kind"] = citation.value("type", citation.value("kind", std::string()));
        }
        if (!row.contains("label")) {
            row["label"] = EvidenceCitationLabel(citation);
        }
        normalized.push_back(std::move(row));
    }
    return normalized;
}

std::string DescribeSeekOrderResult(const std::string& descriptor,
                                    const tapescope::QueryResult<json>& result) {
    std::ostringstream out;
    out << "seek_order_anchor\n";
    out << "query: " << descriptor << "\n\n";
    if (!result.ok()) {
        out << tapescope::QueryClient::describeError(result.error) << '\n';
        return out.str();
    }

    const json summary = result.value.value("summary", json::object());
    out << "replay_target_session_seq: " << summary.value("replay_target_session_seq", 0ULL) << '\n';
    out << "first_session_seq: " << summary.value("first_session_seq", 0ULL) << '\n';
    out << "last_session_seq: " << summary.value("last_session_seq", 0ULL) << '\n';
    out << "last_fill_session_seq: " << summary.value("last_fill_session_seq", 0ULL) << '\n';
    if (summary.contains("protected_window")) {
        out << "protected_window:\n" << summary.value("protected_window", json::object()).dump(2) << '\n';
    }
    out << "\nsummary:\n" << summary.dump(2) << '\n';
    return out.str();
}

std::string DescribeReportInventoryResult(const tapescope::QueryResult<json>& sessionReports,
                                          const tapescope::QueryResult<json>& caseReports) {
    std::ostringstream out;
    out << "report_inventory\n\n";

    out << "session_reports:\n";
    if (!sessionReports.ok()) {
        out << tapescope::QueryClient::describeError(sessionReports.error) << "\n\n";
    } else {
        const json rows = sessionReports.value.value("events", json::array());
        if (!rows.is_array() || rows.empty()) {
            out << "(none)\n\n";
        } else {
            for (const auto& row : rows) {
                out << "- report_id=" << row.value("report_id", 0ULL)
                    << " artifact_id=" << row.value("artifact_id", std::string())
                    << " revision_id=" << row.value("revision_id", 0ULL)
                    << " headline=" << row.value("headline", std::string()) << '\n';
            }
            out << '\n';
        }
    }

    out << "case_reports:\n";
    if (!caseReports.ok()) {
        out << tapescope::QueryClient::describeError(caseReports.error) << '\n';
    } else {
        const json rows = caseReports.value.value("events", json::array());
        if (!rows.is_array() || rows.empty()) {
            out << "(none)\n";
        } else {
            for (const auto& row : rows) {
                out << "- report_id=" << row.value("report_id", 0ULL)
                    << " artifact_id=" << row.value("artifact_id", std::string())
                    << " report_type=" << row.value("report_type", std::string())
                    << " headline=" << row.value("headline", std::string()) << '\n';
            }
        }
    }

    return out.str();
}

std::string DescribeArtifactExportResult(const std::string& artifactId,
                                         const std::string& exportFormat,
                                         const tapescope::QueryResult<json>& result) {
    std::ostringstream out;
    out << "artifact_export\n";
    out << "artifact_id: " << artifactId << '\n';
    out << "format: " << exportFormat << "\n\n";
    if (!result.ok()) {
        out << tapescope::QueryClient::describeError(result.error) << '\n';
        return out.str();
    }

    const json summary = result.value.value("summary", json::object());
    out << "summary:\n" << summary.dump(2) << "\n\n";
    if (exportFormat == "markdown") {
        out << "markdown:\n" << summary.value("markdown", std::string()) << '\n';
    } else {
        out << "bundle:\n" << summary.value("bundle", json::object()).dump(2) << '\n';
    }
    return out.str();
}

std::string DescribeSessionQualityResult(const tapescope::RangeQuery& query,
                                         bool includeLiveTail,
                                         const tapescope::QueryResult<json>& result) {
    std::ostringstream out;
    out << "session_quality\n";
    out << "session_seq=[" << query.firstSessionSeq << ", " << query.lastSessionSeq << "]\n";
    out << "include_live_tail: " << (includeLiveTail ? "true" : "false") << "\n\n";
    if (!result.ok()) {
        out << tapescope::QueryClient::describeError(result.error) << '\n';
        return out.str();
    }

    const json summary = result.value.value("summary", json::object());
    out << "summary:\n" << summary.dump(2) << '\n';
    return out.str();
}

bool ReplayRangeFromSeekSummary(const json& summary, tapescope::RangeQuery* query) {
    if (query == nullptr || !summary.is_object()) {
        return false;
    }
    const std::uint64_t replayFrom = summary.value("replay_from_session_seq", 0ULL);
    const std::uint64_t replayTo = summary.value("replay_to_session_seq", 0ULL);
    if (replayFrom > 0 && replayTo >= replayFrom) {
        query->firstSessionSeq = replayFrom;
        query->lastSessionSeq = replayTo;
        return true;
    }

    const json window = summary.value("protected_window", json::object());
    if (window.is_object()) {
        const std::uint64_t first = window.value("first_session_seq", 0ULL);
        const std::uint64_t last = window.value("last_session_seq", 0ULL);
        if (first > 0 && last >= first) {
            query->firstSessionSeq = first;
            query->lastSessionSeq = last;
            return true;
        }
    }

    const std::uint64_t target = summary.value("replay_target_session_seq", 0ULL);
    if (target == 0) {
        return false;
    }
    query->firstSessionSeq = target > 16 ? target - 16 : 1;
    query->lastSessionSeq = target + 16;
    return true;
}

bool ReplayRangeFromInvestigationSummary(const json& summary, tapescope::RangeQuery* query) {
    if (query == nullptr || !summary.is_object()) {
        return false;
    }
    const std::uint64_t from = summary.value("from_session_seq",
                                             summary.value("first_session_seq", 0ULL));
    const std::uint64_t to = summary.value("to_session_seq",
                                           summary.value("last_session_seq", 0ULL));
    if (from > 0 && to >= from) {
        query->firstSessionSeq = from;
        query->lastSessionSeq = to;
        return true;
    }
    return ReplayRangeFromSeekSummary(summary, query);
}

enum class OrderAnchorType {
    TraceId = 0,
    OrderId = 1,
    PermId = 2,
    ExecId = 3
};

std::string OrderAnchorTypeKey(OrderAnchorType type) {
    switch (type) {
        case OrderAnchorType::TraceId:
            return "traceId";
        case OrderAnchorType::OrderId:
            return "orderId";
        case OrderAnchorType::PermId:
            return "permId";
        case OrderAnchorType::ExecId:
            return "execId";
    }
    return "traceId";
}

NSInteger OrderAnchorTypeIndexForKey(const std::string& key) {
    if (key == "orderId") {
        return 1;
    }
    if (key == "permId") {
        return 2;
    }
    if (key == "execId") {
        return 3;
    }
    return 0;
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

struct ProbeSnapshot {
    tapescope::QueryResult<tapescope::StatusSnapshot> status;
    tapescope::QueryResult<std::vector<json>> liveTail;
};

struct InvestigationPaneModel {
    NSTextField* stateLabel = nil;
    NSTextView* detailView = nil;
    NSTableView* evidenceTableView = nil;
    NSButton* openEvidenceButton = nil;
    NSButton* loadReplayButton = nil;
    std::vector<json>* evidenceItems = nullptr;
    BOOL* hasReplayRange = nullptr;
    tapescope::RangeQuery* replayRange = nullptr;
    NSString* emptyEvidenceMessage = nil;
    NSString* missingSelectionMessage = nil;
    NSString* missingArtifactMessage = nil;
    NSString* missingReplayMessage = nil;
};

} // namespace

@interface TapeScopeWindowController : NSWindowController <NSTableViewDataSource, NSTableViewDelegate> {
@private
    std::unique_ptr<tapescope::QueryClient> _client;
    dispatch_queue_t _pollQueue;
    NSTimer* _pollTimer;
    BOOL _pollInFlight;
    BOOL _pollingPaused;
    NSDate* _lastProbeAt;

    NSBox* _bannerBox;
    NSTextField* _bannerLabel;
    NSTextField* _pollMetaLabel;
    NSTextField* _lastProbeLabel;
    NSButton* _refreshNowButton;
    NSButton* _pollingToggleButton;
    NSTextField* _socketValue;
    NSTextField* _dataDirValue;
    NSTextField* _instrumentValue;
    NSTextField* _latestSeqValue;
    NSTextField* _liveCountValue;
    NSTextField* _segmentCountValue;
    NSTextField* _manifestHashValue;
    NSTabView* _tabView;

    NSTextView* _statusTextView;
    NSTextView* _liveTextView;
    NSTableView* _liveTableView;
    std::vector<json> _liveEvents;

    NSButton* _recentOpenButton;
    NSButton* _recentClearButton;
    NSTextField* _recentStateLabel;
    NSTableView* _recentTableView;
    NSTextView* _recentTextView;
    std::vector<json> _recentHistoryItems;

    NSTextField* _overviewFirstField;
    NSTextField* _overviewLastField;
    NSButton* _overviewFetchButton;
    NSButton* _overviewScanButton;
    NSButton* _overviewLoadReplayButton;
    NSButton* _overviewOpenSelectedIncidentButton;
    NSButton* _overviewOpenSelectedEvidenceButton;
    NSTextField* _overviewStateLabel;
    NSTableView* _overviewIncidentTableView;
    NSTableView* _overviewEvidenceTableView;
    NSTextView* _overviewTextView;
    BOOL _overviewInFlight;
    BOOL _hasOverviewReplayRange;
    tapescope::RangeQuery _lastOverviewQuery;
    tapescope::RangeQuery _overviewReplayRange;
    std::vector<json> _overviewIncidents;
    std::vector<json> _overviewEvidenceItems;

    NSTextField* _rangeFirstField;
    NSTextField* _rangeLastField;
    NSButton* _rangeFetchButton;
    NSTextField* _rangeStateLabel;
    NSTableView* _rangeTableView;
    NSTextView* _rangeTextView;
    BOOL _rangeInFlight;
    tapescope::RangeQuery _lastRangeQuery;
    std::vector<json> _rangeEvents;

    NSTextField* _qualityFirstField;
    NSTextField* _qualityLastField;
    NSButton* _qualityFetchButton;
    NSButton* _qualityIncludeLiveTailButton;
    NSTextField* _qualityStateLabel;
    NSTextView* _qualityTextView;
    BOOL _qualityInFlight;
    tapescope::RangeQuery _lastQualityQuery;

    NSTextField* _findingIdField;
    NSButton* _findingFetchButton;
    NSButton* _findingLoadReplayButton;
    NSButton* _findingOpenSelectedEvidenceButton;
    NSTextField* _findingStateLabel;
    NSTableView* _findingEvidenceTableView;
    NSTextView* _findingTextView;
    BOOL _findingInFlight;
    BOOL _hasFindingReplayRange;
    tapescope::RangeQuery _findingReplayRange;
    std::vector<json> _findingEvidenceItems;

    NSTextField* _anchorIdField;
    NSButton* _anchorFetchButton;
    NSButton* _anchorLoadReplayButton;
    NSButton* _anchorOpenSelectedEvidenceButton;
    NSTextField* _anchorStateLabel;
    NSTableView* _anchorEvidenceTableView;
    NSTextView* _anchorTextView;
    BOOL _anchorInFlight;
    BOOL _hasAnchorReplayRange;
    tapescope::RangeQuery _anchorReplayRange;
    std::vector<json> _anchorEvidenceItems;

    NSPopUpButton* _orderAnchorTypePopup;
    NSTextField* _orderAnchorInputField;
    NSButton* _orderLookupButton;
    NSTextField* _orderStateLabel;
    NSTableView* _orderTableView;
    NSTextView* _orderTextView;
    BOOL _orderLookupInFlight;
    std::vector<json> _orderEvents;

    NSPopUpButton* _orderCaseAnchorTypePopup;
    NSTextField* _orderCaseAnchorInputField;
    NSButton* _orderCaseFetchButton;
    NSButton* _orderCaseScanButton;
    NSButton* _orderCaseLoadReplayButton;
    NSButton* _orderCaseOpenSelectedEvidenceButton;
    NSTextField* _orderCaseStateLabel;
    NSTableView* _orderCaseEvidenceTableView;
    NSTextView* _orderCaseTextView;
    BOOL _orderCaseInFlight;
    BOOL _hasOrderCaseReplayRange;
    tapescope::RangeQuery _orderCaseReplayRange;
    std::vector<json> _orderCaseEvidenceItems;

    NSPopUpButton* _seekAnchorTypePopup;
    NSTextField* _seekAnchorInputField;
    NSButton* _seekFetchButton;
    NSButton* _seekLoadRangeButton;
    NSTextField* _seekStateLabel;
    NSTextView* _seekTextView;
    BOOL _seekInFlight;
    BOOL _hasSeekReplayRange;
    tapescope::RangeQuery _seekReplayRange;

    NSTextField* _incidentIdField;
    NSButton* _incidentFetchButton;
    NSButton* _incidentRefreshButton;
    NSButton* _incidentLoadReplayButton;
    NSButton* _incidentOpenSelectedButton;
    NSButton* _incidentOpenSelectedEvidenceButton;
    NSTextField* _incidentStateLabel;
    NSTableView* _incidentTableView;
    NSTableView* _incidentEvidenceTableView;
    NSTextView* _incidentTextView;
    BOOL _incidentInFlight;
    BOOL _hasIncidentReplayRange;
    tapescope::RangeQuery _incidentReplayRange;
    std::vector<json> _latestIncidents;
    std::vector<json> _incidentEvidenceItems;

    NSTextField* _artifactIdField;
    NSButton* _artifactFetchButton;
    NSPopUpButton* _artifactExportFormatPopup;
    NSButton* _artifactExportButton;
    NSButton* _artifactOpenSelectedEvidenceButton;
    NSTextField* _artifactStateLabel;
    NSTableView* _artifactEvidenceTableView;
    NSTextView* _artifactTextView;
    BOOL _artifactInFlight;
    std::vector<json> _artifactEvidenceItems;

    NSButton* _reportInventoryRefreshButton;
    NSButton* _reportInventoryOpenSessionButton;
    NSButton* _reportInventoryOpenCaseButton;
    NSTextField* _reportInventoryStateLabel;
    NSTableView* _sessionReportTableView;
    NSTableView* _caseReportTableView;
    NSTextView* _reportInventoryTextView;
    BOOL _reportInventoryInFlight;
    std::vector<json> _latestSessionReports;
    std::vector<json> _latestCaseReports;
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
    window.minSize = NSMakeSize(900, 620);
    if (@available(macOS 11.0, *)) {
        window.toolbarStyle = NSWindowToolbarStyleUnified;
    }
    window.titleVisibility = NSWindowTitleHidden;
    window.titlebarAppearsTransparent = YES;
    window.contentView.wantsLayer = YES;
    window.contentView.layer.backgroundColor = TapeBackgroundColor().CGColor;

    tapescope::ClientConfig config;
    config.socketPath = tapescope::defaultSocketPath();
    _client = std::make_unique<tapescope::QueryClient>(std::move(config));
    _pollQueue = dispatch_queue_create("com.foxy.tapescope.poll", DISPATCH_QUEUE_SERIAL);
    _lastOverviewQuery.firstSessionSeq = 1;
    _lastOverviewQuery.lastSessionSeq = 128;
    _lastRangeQuery.firstSessionSeq = 1;
    _lastRangeQuery.lastSessionSeq = 128;
    _lastQualityQuery.firstSessionSeq = 1;
    _lastQualityQuery.lastSessionSeq = 128;

    [self buildInterface];
    [self restoreApplicationState];
    return self;
}

- (NSTabViewItem*)textTabItemWithLabel:(NSString*)label textView:(NSTextView* __strong*)outTextView {
    NSTextView* textView = MakeReadOnlyTextView();
    NSScrollView* scrollView = MakeScrollView(textView, 420.0);

    NSTabViewItem* item = [[NSTabViewItem alloc] initWithIdentifier:label];
    item.label = label;
    item.view = scrollView;
    if (outTextView != nullptr) {
        *outTextView = textView;
    }
    return item;
}

- (NSTabViewItem*)liveEventsTabItem {
    NSStackView* stack = nil;
    NSView* pane = MakePaneWithStack(&stack);

    [stack addArrangedSubview:MakeIntroLabel(@"Live tape: recent mutable-tail events from tape_engine with row-level drilldown.")];

    _liveTableView = MakeStandardTableView(self, self);
    AddTableColumn(_liveTableView, @"session_seq", @"session_seq", 120.0);
    AddTableColumn(_liveTableView, @"source_seq", @"source_seq", 120.0);
    AddTableColumn(_liveTableView, @"event_kind", @"event_kind", 180.0);
    AddTableColumn(_liveTableView, @"summary", @"summary", 460.0);
    [stack addArrangedSubview:MakeTableScrollView(_liveTableView, 170.0)];

    [stack addArrangedSubview:MakeSectionLabel(@"Selected Live Event")];

    _liveTextView = MakeReadOnlyTextView();
    _liveTextView.string = @"Waiting for the first live-tail response…";
    [stack addArrangedSubview:MakeScrollView(_liveTextView, 240.0)];

    NSTabViewItem* item = [[NSTabViewItem alloc] initWithIdentifier:@"LiveEventsPane"];
    item.label = @"LiveEventsPane";
    item.view = pane;
    return item;
}

- (NSTabViewItem*)recentHistoryTabItem {
    NSStackView* stack = nil;
    NSView* pane = MakePaneWithStack(&stack);
    [stack addArrangedSubview:MakeIntroLabel(@"Recent history: reopen recently viewed incidents, findings, anchors, order cases, artifacts, and overview ranges without retyping ids.",
                                             2)];

    NSStackView* controls = MakeControlRow();
    _recentOpenButton = [NSButton buttonWithTitle:@"Open Selected"
                                           target:self
                                           action:@selector(openSelectedRecentHistory:)];
    _recentOpenButton.enabled = NO;
    [controls addArrangedSubview:_recentOpenButton];

    _recentClearButton = [NSButton buttonWithTitle:@"Clear History"
                                            target:self
                                            action:@selector(clearRecentHistory:)];
    _recentClearButton.enabled = NO;
    [controls addArrangedSubview:_recentClearButton];
    [stack addArrangedSubview:controls];

    _recentStateLabel = MakeLabel(@"Recent history will populate as you open investigations.",
                                  [NSFont systemFontOfSize:12.0 weight:NSFontWeightMedium],
                                  [NSColor secondaryLabelColor]);
    [stack addArrangedSubview:_recentStateLabel];

    _recentTableView = MakeStandardTableView(self, self);
    AddTableColumn(_recentTableView, @"kind", @"kind", 140.0);
    AddTableColumn(_recentTableView, @"target_id", @"target_id", 220.0);
    AddTableColumn(_recentTableView, @"headline", @"headline", 430.0);
    ConfigureTablePrimaryAction(_recentTableView, self, @selector(openSelectedRecentHistory:));
    [stack addArrangedSubview:MakeTableScrollView(_recentTableView, 180.0)];

    [stack addArrangedSubview:MakeSectionLabel(@"Selected Recent Entry")];

    _recentTextView = MakeReadOnlyTextView();
    _recentTextView.string = @"Open findings, incidents, order cases, anchors, artifacts, or overview ranges and they will appear here.";
    [stack addArrangedSubview:MakeScrollView(_recentTextView, 250.0)];

    NSTabViewItem* item = [[NSTabViewItem alloc] initWithIdentifier:@"RecentPane"];
    item.label = @"RecentPane";
    item.view = pane;
    return item;
}

- (NSTabViewItem*)overviewTabItem {
    NSStackView* stack = nil;
    NSView* pane = MakePaneWithStack(&stack);
    [stack addArrangedSubview:MakeIntroLabel(@"Session overview: summarize the major incidents and evidence for a frozen session_seq window.",
                                             2)];

    NSStackView* controls = MakeControlRow();
    [controls addArrangedSubview:MakeLabel(@"first_session_seq",
                                           [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                           [NSColor secondaryLabelColor])];
    _overviewFirstField = MakeMonospacedField(130.0, UInt64String(_lastOverviewQuery.firstSessionSeq));
    [controls addArrangedSubview:_overviewFirstField];

    [controls addArrangedSubview:MakeLabel(@"last_session_seq",
                                           [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                           [NSColor secondaryLabelColor])];
    _overviewLastField = MakeMonospacedField(130.0, UInt64String(_lastOverviewQuery.lastSessionSeq));
    [controls addArrangedSubview:_overviewLastField];

    _overviewFetchButton = [NSButton buttonWithTitle:@"Read Overview"
                                              target:self
                                              action:@selector(fetchOverview:)];
    [controls addArrangedSubview:_overviewFetchButton];
    _overviewScanButton = [NSButton buttonWithTitle:@"Scan Report"
                                             target:self
                                             action:@selector(scanOverviewReport:)];
    [controls addArrangedSubview:_overviewScanButton];
    _overviewLoadReplayButton = [NSButton buttonWithTitle:@"Load Range"
                                                   target:self
                                                   action:@selector(loadReplayWindowFromOverview:)];
    _overviewLoadReplayButton.enabled = NO;
    [controls addArrangedSubview:_overviewLoadReplayButton];
    _overviewOpenSelectedIncidentButton = [NSButton buttonWithTitle:@"Open Selected Incident"
                                                             target:self
                                                             action:@selector(openSelectedOverviewIncident:)];
    _overviewOpenSelectedIncidentButton.enabled = NO;
    [controls addArrangedSubview:_overviewOpenSelectedIncidentButton];
    _overviewOpenSelectedEvidenceButton = [NSButton buttonWithTitle:@"Open Selected Evidence"
                                                             target:self
                                                             action:@selector(openSelectedOverviewEvidence:)];
    _overviewOpenSelectedEvidenceButton.enabled = NO;
    [controls addArrangedSubview:_overviewOpenSelectedEvidenceButton];
    [stack addArrangedSubview:controls];

    _overviewStateLabel = MakeLabel(@"No session overview loaded yet.",
                                    [NSFont systemFontOfSize:12.0 weight:NSFontWeightMedium],
                                    [NSColor secondaryLabelColor]);
    [stack addArrangedSubview:_overviewStateLabel];

    [stack addArrangedSubview:MakeSectionLabel(@"Overview Summary")];

    _overviewTextView = MakeReadOnlyTextView();
    _overviewTextView.string = @"Read a session overview to inspect ranked incidents and evidence.";
    [stack addArrangedSubview:MakeScrollView(_overviewTextView, 170.0)];

    [stack addArrangedSubview:MakeSectionLabel(@"Top Incidents")];

    _overviewIncidentTableView = MakeStandardTableView(self, self);
    AddTableColumn(_overviewIncidentTableView, @"logical_incident_id", @"logical_incident_id", 150.0);
    AddTableColumn(_overviewIncidentTableView, @"kind", @"kind", 180.0);
    AddTableColumn(_overviewIncidentTableView, @"score", @"score", 100.0);
    AddTableColumn(_overviewIncidentTableView, @"summary", @"summary", 430.0);
    ConfigureTablePrimaryAction(_overviewIncidentTableView, self, @selector(openSelectedOverviewIncident:));
    [stack addArrangedSubview:MakeTableScrollView(_overviewIncidentTableView, 160.0)];

    [stack addArrangedSubview:MakeSectionLabel(@"Evidence Citations")];

    _overviewEvidenceTableView = MakeEvidenceTableView(self, self);
    ConfigureTablePrimaryAction(_overviewEvidenceTableView, self, @selector(openSelectedOverviewEvidence:));
    [stack addArrangedSubview:MakeTableScrollView(_overviewEvidenceTableView, 140.0)];

    NSTabViewItem* item = [[NSTabViewItem alloc] initWithIdentifier:@"SessionOverviewPane"];
    item.label = @"SessionOverviewPane";
    item.view = pane;
    return item;
}

- (NSTabViewItem*)rangeTabItem {
    NSStackView* stack = nil;
    NSView* pane = MakePaneWithStack(&stack);
    [stack addArrangedSubview:MakeIntroLabel(@"Replay window: fetch a frozen session_seq range from tape_engine.")];

    NSStackView* controls = MakeControlRow();
    [controls addArrangedSubview:MakeLabel(@"first_session_seq",
                                           [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                           [NSColor secondaryLabelColor])];
    _rangeFirstField = MakeMonospacedField(130.0, UInt64String(_lastRangeQuery.firstSessionSeq));
    [controls addArrangedSubview:_rangeFirstField];

    [controls addArrangedSubview:MakeLabel(@"last_session_seq",
                                           [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                           [NSColor secondaryLabelColor])];
    _rangeLastField = MakeMonospacedField(130.0, UInt64String(_lastRangeQuery.lastSessionSeq));
    [controls addArrangedSubview:_rangeLastField];

    _rangeFetchButton = [NSButton buttonWithTitle:@"Fetch Range" target:self action:@selector(fetchRange:)];
    [controls addArrangedSubview:_rangeFetchButton];
    NSButton* rangeQualityButton = [NSButton buttonWithTitle:@"Read Quality"
                                                      target:self
                                                      action:@selector(loadQualityFromRange:)];
    [controls addArrangedSubview:rangeQualityButton];
    [stack addArrangedSubview:controls];

    _rangeStateLabel = MakeLabel(@"No replay window loaded yet.",
                                 [NSFont systemFontOfSize:12.0 weight:NSFontWeightMedium],
                                 [NSColor secondaryLabelColor]);
    [stack addArrangedSubview:_rangeStateLabel];

    _rangeTableView = MakeStandardTableView(self, self);
    AddTableColumn(_rangeTableView, @"session_seq", @"session_seq", 120.0);
    AddTableColumn(_rangeTableView, @"source_seq", @"source_seq", 120.0);
    AddTableColumn(_rangeTableView, @"event_kind", @"event_kind", 180.0);
    AddTableColumn(_rangeTableView, @"summary", @"summary", 460.0);
    [stack addArrangedSubview:MakeTableScrollView(_rangeTableView, 170.0)];

    [stack addArrangedSubview:MakeSectionLabel(@"Decoded Event")];

    _rangeTextView = MakeReadOnlyTextView();
    _rangeTextView.string = @"Fetch a replay window, then select a row to inspect the decoded event payload.";
    [stack addArrangedSubview:MakeScrollView(_rangeTextView, 220.0)];

    NSTabViewItem* item = [[NSTabViewItem alloc] initWithIdentifier:@"RangePane"];
    item.label = @"RangePane";
    item.view = pane;
    return item;
}

- (NSTabViewItem*)qualityTabItem {
    NSStackView* stack = nil;
    NSView* pane = MakePaneWithStack(&stack);
    [stack addArrangedSubview:MakeIntroLabel(@"Session quality: inspect evidence trust, identity quality, and feed caveats for a session_seq window.",
                                             2)];

    NSStackView* controls = MakeControlRow();
    [controls addArrangedSubview:MakeLabel(@"first_session_seq",
                                           [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                           [NSColor secondaryLabelColor])];
    _qualityFirstField = MakeMonospacedField(130.0, UInt64String(_lastQualityQuery.firstSessionSeq));
    [controls addArrangedSubview:_qualityFirstField];

    [controls addArrangedSubview:MakeLabel(@"last_session_seq",
                                           [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                           [NSColor secondaryLabelColor])];
    _qualityLastField = MakeMonospacedField(130.0, UInt64String(_lastQualityQuery.lastSessionSeq));
    [controls addArrangedSubview:_qualityLastField];

    _qualityIncludeLiveTailButton = [NSButton checkboxWithTitle:@"Include Live Tail"
                                                         target:nil
                                                         action:nil];
    _qualityIncludeLiveTailButton.state = NSControlStateValueOff;
    [controls addArrangedSubview:_qualityIncludeLiveTailButton];

    _qualityFetchButton = [NSButton buttonWithTitle:@"Read Quality"
                                             target:self
                                             action:@selector(fetchSessionQuality:)];
    [controls addArrangedSubview:_qualityFetchButton];
    [stack addArrangedSubview:controls];

    _qualityStateLabel = MakeLabel(@"No session quality loaded yet.",
                                   [NSFont systemFontOfSize:12.0 weight:NSFontWeightMedium],
                                   [NSColor secondaryLabelColor]);
    [stack addArrangedSubview:_qualityStateLabel];

    _qualityTextView = MakeReadOnlyTextView();
    _qualityTextView.string = @"Read a session-quality window to inspect evidence trust and provenance caveats.";
    [stack addArrangedSubview:MakeScrollView(_qualityTextView, 420.0)];

    NSTabViewItem* item = [[NSTabViewItem alloc] initWithIdentifier:@"QualityPane"];
    item.label = @"QualityPane";
    item.view = pane;
    return item;
}

- (NSTabViewItem*)findingTabItem {
    NSStackView* stack = nil;
    NSView* pane = MakePaneWithStack(&stack);
    [stack addArrangedSubview:MakeIntroLabel(@"Finding drilldown: reopen one finding directly by stable finding_id.",
                                             2)];

    NSStackView* controls = MakeControlRow();
    [controls addArrangedSubview:MakeLabel(@"finding_id",
                                           [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                           [NSColor secondaryLabelColor])];
    _findingIdField = MakeMonospacedField(180.0, nil, @"e.g. 1");
    [controls addArrangedSubview:_findingIdField];

    _findingFetchButton = [NSButton buttonWithTitle:@"Read Finding"
                                             target:self
                                             action:@selector(fetchFinding:)];
    [controls addArrangedSubview:_findingFetchButton];
    _findingLoadReplayButton = [NSButton buttonWithTitle:@"Load Replay Window"
                                                  target:self
                                                  action:@selector(loadReplayWindowFromFinding:)];
    _findingLoadReplayButton.enabled = NO;
    [controls addArrangedSubview:_findingLoadReplayButton];
    _findingOpenSelectedEvidenceButton = [NSButton buttonWithTitle:@"Open Selected Evidence"
                                                            target:self
                                                            action:@selector(openSelectedFindingEvidence:)];
    _findingOpenSelectedEvidenceButton.enabled = NO;
    [controls addArrangedSubview:_findingOpenSelectedEvidenceButton];
    [stack addArrangedSubview:controls];

    _findingStateLabel = MakeLabel(@"No finding loaded yet.",
                                   [NSFont systemFontOfSize:12.0 weight:NSFontWeightMedium],
                                   [NSColor secondaryLabelColor]);
    [stack addArrangedSubview:_findingStateLabel];

    [stack addArrangedSubview:MakeSectionLabel(@"Evidence Citations")];

    _findingEvidenceTableView = MakeEvidenceTableView(self, self);
    ConfigureTablePrimaryAction(_findingEvidenceTableView, self, @selector(openSelectedFindingEvidence:));
    [stack addArrangedSubview:MakeTableScrollView(_findingEvidenceTableView, 150.0)];

    _findingTextView = MakeReadOnlyTextView();
    _findingTextView.string = @"Read a finding to inspect its report, timeline, and linked evidence.";
    [stack addArrangedSubview:MakeScrollView(_findingTextView, 220.0)];

    NSTabViewItem* item = [[NSTabViewItem alloc] initWithIdentifier:@"FindingPane"];
    item.label = @"FindingPane";
    item.view = pane;
    return item;
}

- (NSTabViewItem*)anchorTabItem {
    NSStackView* stack = nil;
    NSView* pane = MakePaneWithStack(&stack);
    [stack addArrangedSubview:MakeIntroLabel(@"Order anchor drilldown: reopen one persisted order anchor by stable anchor_id.",
                                             2)];

    NSStackView* controls = MakeControlRow();
    [controls addArrangedSubview:MakeLabel(@"anchor_id",
                                           [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                           [NSColor secondaryLabelColor])];
    _anchorIdField = MakeMonospacedField(180.0, nil, @"e.g. 1");
    [controls addArrangedSubview:_anchorIdField];

    _anchorFetchButton = [NSButton buttonWithTitle:@"Read Anchor"
                                            target:self
                                            action:@selector(fetchOrderAnchorById:)];
    [controls addArrangedSubview:_anchorFetchButton];
    _anchorLoadReplayButton = [NSButton buttonWithTitle:@"Load Replay Window"
                                                 target:self
                                                 action:@selector(loadReplayWindowFromAnchor:)];
    _anchorLoadReplayButton.enabled = NO;
    [controls addArrangedSubview:_anchorLoadReplayButton];
    _anchorOpenSelectedEvidenceButton = [NSButton buttonWithTitle:@"Open Selected Evidence"
                                                           target:self
                                                           action:@selector(openSelectedAnchorEvidence:)];
    _anchorOpenSelectedEvidenceButton.enabled = NO;
    [controls addArrangedSubview:_anchorOpenSelectedEvidenceButton];
    [stack addArrangedSubview:controls];

    _anchorStateLabel = MakeLabel(@"No order anchor loaded yet.",
                                  [NSFont systemFontOfSize:12.0 weight:NSFontWeightMedium],
                                  [NSColor secondaryLabelColor]);
    [stack addArrangedSubview:_anchorStateLabel];

    [stack addArrangedSubview:MakeSectionLabel(@"Evidence Citations")];

    _anchorEvidenceTableView = MakeEvidenceTableView(self, self);
    ConfigureTablePrimaryAction(_anchorEvidenceTableView, self, @selector(openSelectedAnchorEvidence:));
    [stack addArrangedSubview:MakeTableScrollView(_anchorEvidenceTableView, 150.0)];

    _anchorTextView = MakeReadOnlyTextView();
    _anchorTextView.string = @"Read an order anchor to inspect the anchor event and linked order-case evidence.";
    [stack addArrangedSubview:MakeScrollView(_anchorTextView, 220.0)];

    NSTabViewItem* item = [[NSTabViewItem alloc] initWithIdentifier:@"AnchorPane"];
    item.label = @"AnchorPane";
    item.view = pane;
    return item;
}

- (NSTabViewItem*)orderTabItem {
    NSStackView* stack = nil;
    NSView* pane = MakePaneWithStack(&stack);
    [stack addArrangedSubview:MakeIntroLabel(@"Order anchor lookup: query anchored lifecycle context by trace/order/perm/exec id.")];

    NSStackView* controls = MakeControlRow();

    _orderAnchorTypePopup = [[NSPopUpButton alloc] initWithFrame:NSMakeRect(0, 0, 120, 24) pullsDown:NO];
    [_orderAnchorTypePopup addItemsWithTitles:@[@"traceId", @"orderId", @"permId", @"execId"]];
    _orderAnchorTypePopup.target = self;
    _orderAnchorTypePopup.action = @selector(orderAnchorTypeChanged:);
    [controls addArrangedSubview:_orderAnchorTypePopup];

    _orderAnchorInputField = MakeMonospacedField(220.0, nil, PlaceholderForOrderAnchorType(OrderAnchorType::TraceId));
    [controls addArrangedSubview:_orderAnchorInputField];

    _orderLookupButton = [NSButton buttonWithTitle:@"Find Order Anchor"
                                            target:self
                                            action:@selector(performOrderLookup:)];
    [controls addArrangedSubview:_orderLookupButton];
    [stack addArrangedSubview:controls];

    _orderStateLabel = MakeLabel(@"No order lookup issued yet.",
                                 [NSFont systemFontOfSize:12.0 weight:NSFontWeightMedium],
                                 [NSColor secondaryLabelColor]);
    [stack addArrangedSubview:_orderStateLabel];

    _orderTableView = MakeStandardTableView(self, self);
    AddTableColumn(_orderTableView, @"session_seq", @"session_seq", 120.0);
    AddTableColumn(_orderTableView, @"event_kind", @"event_kind", 180.0);
    AddTableColumn(_orderTableView, @"side", @"side", 100.0);
    AddTableColumn(_orderTableView, @"summary", @"summary", 460.0);
    [stack addArrangedSubview:MakeTableScrollView(_orderTableView, 170.0)];

    [stack addArrangedSubview:MakeSectionLabel(@"Anchored Event")];

    _orderTextView = MakeReadOnlyTextView();
    _orderTextView.string = @"Lookup an anchor, then select a row to inspect the decoded event payload.";
    [stack addArrangedSubview:MakeScrollView(_orderTextView, 220.0)];

    NSTabViewItem* item = [[NSTabViewItem alloc] initWithIdentifier:@"OrderLookupPane"];
    item.label = @"OrderLookupPane";
    item.view = pane;
    return item;
}

- (NSTabViewItem*)orderCaseTabItem {
    NSStackView* stack = nil;
    NSView* pane = MakePaneWithStack(&stack);
    [stack addArrangedSubview:MakeIntroLabel(@"Order case: load the report-style investigation summary for one trace/order/perm/exec anchor.",
                                             2)];

    NSStackView* controls = MakeControlRow();

    _orderCaseAnchorTypePopup = [[NSPopUpButton alloc] initWithFrame:NSMakeRect(0, 0, 120, 24) pullsDown:NO];
    [_orderCaseAnchorTypePopup addItemsWithTitles:@[@"traceId", @"orderId", @"permId", @"execId"]];
    _orderCaseAnchorTypePopup.target = self;
    _orderCaseAnchorTypePopup.action = @selector(orderCaseAnchorTypeChanged:);
    [controls addArrangedSubview:_orderCaseAnchorTypePopup];

    _orderCaseAnchorInputField = MakeMonospacedField(220.0, nil, PlaceholderForOrderAnchorType(OrderAnchorType::TraceId));
    [controls addArrangedSubview:_orderCaseAnchorInputField];

    _orderCaseFetchButton = [NSButton buttonWithTitle:@"Read Order Case"
                                               target:self
                                               action:@selector(fetchOrderCase:)];
    [controls addArrangedSubview:_orderCaseFetchButton];
    _orderCaseScanButton = [NSButton buttonWithTitle:@"Scan Report"
                                              target:self
                                              action:@selector(scanOrderCaseReport:)];
    [controls addArrangedSubview:_orderCaseScanButton];
    [stack addArrangedSubview:controls];

    _orderCaseStateLabel = MakeLabel(@"No order case loaded yet.",
                                     [NSFont systemFontOfSize:12.0 weight:NSFontWeightMedium],
                                     [NSColor secondaryLabelColor]);
    [stack addArrangedSubview:_orderCaseStateLabel];

    NSStackView* actions = MakeControlRow();
    _orderCaseLoadReplayButton = [NSButton buttonWithTitle:@"Load Replay Window"
                                                    target:self
                                                    action:@selector(loadReplayWindowFromOrderCase:)];
    _orderCaseLoadReplayButton.enabled = NO;
    [actions addArrangedSubview:_orderCaseLoadReplayButton];
    _orderCaseOpenSelectedEvidenceButton = [NSButton buttonWithTitle:@"Open Selected Evidence"
                                                              target:self
                                                              action:@selector(openSelectedOrderCaseEvidence:)];
    _orderCaseOpenSelectedEvidenceButton.enabled = NO;
    [actions addArrangedSubview:_orderCaseOpenSelectedEvidenceButton];
    [stack addArrangedSubview:actions];

    [stack addArrangedSubview:MakeSectionLabel(@"Order Case Summary")];

    _orderCaseTextView = MakeReadOnlyTextView();
    _orderCaseTextView.string = @"Read an order case to inspect replay targets, findings, incidents, and evidence.";
    [stack addArrangedSubview:MakeScrollView(_orderCaseTextView, 170.0)];

    [stack addArrangedSubview:MakeSectionLabel(@"Evidence Citations")];

    _orderCaseEvidenceTableView = MakeEvidenceTableView(self, self);
    ConfigureTablePrimaryAction(_orderCaseEvidenceTableView, self, @selector(openSelectedOrderCaseEvidence:));
    [stack addArrangedSubview:MakeTableScrollView(_orderCaseEvidenceTableView, 160.0)];

    NSTabViewItem* item = [[NSTabViewItem alloc] initWithIdentifier:@"OrderCasePane"];
    item.label = @"OrderCasePane";
    item.view = pane;
    return item;
}

- (NSTabViewItem*)seekTabItem {
    NSStackView* stack = nil;
    NSView* pane = MakePaneWithStack(&stack);
    [stack addArrangedSubview:MakeIntroLabel(@"Replay target: compute the best replay jump around an order/fill anchor.",
                                             2)];

    NSStackView* controls = MakeControlRow();

    _seekAnchorTypePopup = [[NSPopUpButton alloc] initWithFrame:NSMakeRect(0, 0, 120, 24) pullsDown:NO];
    [_seekAnchorTypePopup addItemsWithTitles:@[@"traceId", @"orderId", @"permId", @"execId"]];
    _seekAnchorTypePopup.target = self;
    _seekAnchorTypePopup.action = @selector(seekAnchorTypeChanged:);
    [controls addArrangedSubview:_seekAnchorTypePopup];

    _seekAnchorInputField = MakeMonospacedField(220.0, nil, PlaceholderForOrderAnchorType(OrderAnchorType::TraceId));
    [controls addArrangedSubview:_seekAnchorInputField];

    _seekFetchButton = [NSButton buttonWithTitle:@"Seek Replay Target"
                                          target:self
                                          action:@selector(fetchSeekOrder:)];
    [controls addArrangedSubview:_seekFetchButton];
    _seekLoadRangeButton = [NSButton buttonWithTitle:@"Load Replay Window"
                                              target:self
                                              action:@selector(loadReplayWindowFromSeek:)];
    _seekLoadRangeButton.enabled = NO;
    [controls addArrangedSubview:_seekLoadRangeButton];
    [stack addArrangedSubview:controls];

    _seekStateLabel = MakeLabel(@"No replay target lookup issued yet.",
                                [NSFont systemFontOfSize:12.0 weight:NSFontWeightMedium],
                                [NSColor secondaryLabelColor]);
    [stack addArrangedSubview:_seekStateLabel];

    _seekTextView = MakeReadOnlyTextView();
    _seekTextView.string = @"Seek results will show the replay target session_seq and protected-window context.";
    [stack addArrangedSubview:MakeScrollView(_seekTextView, 320.0)];

    NSTabViewItem* item = [[NSTabViewItem alloc] initWithIdentifier:@"ReplayTargetPane"];
    item.label = @"ReplayTargetPane";
    item.view = pane;
    return item;
}

- (NSTabViewItem*)incidentTabItem {
    NSStackView* stack = nil;
    NSView* pane = MakePaneWithStack(&stack);
    [stack addArrangedSubview:MakeIntroLabel(@"Incident drilldown: reopen a ranked logical incident by stable logical_incident_id.",
                                             2)];

    NSStackView* controls = MakeControlRow();
    [controls addArrangedSubview:MakeLabel(@"logical_incident_id",
                                           [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                           [NSColor secondaryLabelColor])];
    _incidentIdField = MakeMonospacedField(180.0, nil, @"e.g. 1");
    [controls addArrangedSubview:_incidentIdField];

    _incidentRefreshButton = [NSButton buttonWithTitle:@"Refresh List"
                                                target:self
                                                action:@selector(refreshIncidentList:)];
    [controls addArrangedSubview:_incidentRefreshButton];

    _incidentFetchButton = [NSButton buttonWithTitle:@"Read Incident"
                                              target:self
                                              action:@selector(fetchIncident:)];
    [controls addArrangedSubview:_incidentFetchButton];
    _incidentLoadReplayButton = [NSButton buttonWithTitle:@"Load Replay Window"
                                                   target:self
                                                   action:@selector(loadReplayWindowFromIncident:)];
    _incidentLoadReplayButton.enabled = NO;
    [controls addArrangedSubview:_incidentLoadReplayButton];

    _incidentOpenSelectedButton = [NSButton buttonWithTitle:@"Open Selected"
                                                     target:self
                                                     action:@selector(openSelectedIncident:)];
    _incidentOpenSelectedButton.enabled = NO;
    [controls addArrangedSubview:_incidentOpenSelectedButton];
    _incidentOpenSelectedEvidenceButton = [NSButton buttonWithTitle:@"Open Selected Evidence"
                                                             target:self
                                                             action:@selector(openSelectedIncidentEvidence:)];
    _incidentOpenSelectedEvidenceButton.enabled = NO;
    [controls addArrangedSubview:_incidentOpenSelectedEvidenceButton];
    [stack addArrangedSubview:controls];

    _incidentStateLabel = MakeLabel(@"No incident loaded yet.",
                                    [NSFont systemFontOfSize:12.0 weight:NSFontWeightMedium],
                                    [NSColor secondaryLabelColor]);
    [stack addArrangedSubview:_incidentStateLabel];

    _incidentTableView = MakeStandardTableView(self, self);
    AddTableColumn(_incidentTableView, @"logical_incident_id", @"logical_incident_id", 150.0);
    AddTableColumn(_incidentTableView, @"kind", @"kind", 180.0);
    AddTableColumn(_incidentTableView, @"score", @"score", 90.0);
    AddTableColumn(_incidentTableView, @"headline", @"headline", 420.0);
    ConfigureTablePrimaryAction(_incidentTableView, self, @selector(openSelectedIncident:));
    [stack addArrangedSubview:MakeTableScrollView(_incidentTableView, 170.0)];

    [stack addArrangedSubview:MakeSectionLabel(@"Evidence Citations")];

    _incidentEvidenceTableView = MakeEvidenceTableView(self, self);
    ConfigureTablePrimaryAction(_incidentEvidenceTableView, self, @selector(openSelectedIncidentEvidence:));
    [stack addArrangedSubview:MakeTableScrollView(_incidentEvidenceTableView, 140.0)];

    _incidentTextView = MakeReadOnlyTextView();
    _incidentTextView.string = @"Incident drilldown will show score breakdown, findings, protected windows, and narrative summary.";
    [stack addArrangedSubview:MakeScrollView(_incidentTextView, 180.0)];

    NSTabViewItem* item = [[NSTabViewItem alloc] initWithIdentifier:@"IncidentPane"];
    item.label = @"IncidentPane";
    item.view = pane;
    return item;
}

- (NSTabViewItem*)artifactTabItem {
    NSStackView* stack = nil;
    NSView* pane = MakePaneWithStack(&stack);
    [stack addArrangedSubview:MakeIntroLabel(@"Artifact read: reopen durable reports or selector artifacts by stable artifact id.",
                                             2)];

    NSStackView* controls = MakeControlRow();
    [controls addArrangedSubview:MakeLabel(@"artifact_id",
                                           [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                           [NSColor secondaryLabelColor])];
    _artifactIdField = MakeMonospacedField(340.0, nil, @"e.g. session-report:1 or order-case:order:7401");
    [controls addArrangedSubview:_artifactIdField];

    _artifactFetchButton = [NSButton buttonWithTitle:@"Read Artifact"
                                              target:self
                                              action:@selector(fetchArtifact:)];
    [controls addArrangedSubview:_artifactFetchButton];

    _artifactExportFormatPopup = [[NSPopUpButton alloc] initWithFrame:NSMakeRect(0, 0, 140, 24) pullsDown:NO];
    [_artifactExportFormatPopup addItemsWithTitles:@[@"markdown", @"json-bundle"]];
    [controls addArrangedSubview:_artifactExportFormatPopup];

    _artifactExportButton = [NSButton buttonWithTitle:@"Preview Export"
                                               target:self
                                               action:@selector(exportArtifactPreview:)];
    [controls addArrangedSubview:_artifactExportButton];
    _artifactOpenSelectedEvidenceButton = [NSButton buttonWithTitle:@"Open Selected Evidence"
                                                             target:self
                                                             action:@selector(openSelectedArtifactEvidence:)];
    _artifactOpenSelectedEvidenceButton.enabled = NO;
    [controls addArrangedSubview:_artifactOpenSelectedEvidenceButton];
    [stack addArrangedSubview:controls];

    _artifactStateLabel = MakeLabel(@"No artifact loaded yet.",
                                    [NSFont systemFontOfSize:12.0 weight:NSFontWeightMedium],
                                    [NSColor secondaryLabelColor]);
    [stack addArrangedSubview:_artifactStateLabel];

    [stack addArrangedSubview:MakeSectionLabel(@"Evidence Citations")];

    _artifactEvidenceTableView = MakeEvidenceTableView(self, self);
    ConfigureTablePrimaryAction(_artifactEvidenceTableView, self, @selector(openSelectedArtifactEvidence:));
    [stack addArrangedSubview:MakeTableScrollView(_artifactEvidenceTableView, 160.0)];

    [stack addArrangedSubview:MakeSectionLabel(@"Artifact Detail")];

    _artifactTextView = MakeReadOnlyTextView();
    _artifactTextView.string = @"Artifact reads will show the normalized investigation envelope for durable reports and selector artifacts.";
    [stack addArrangedSubview:MakeScrollView(_artifactTextView, 180.0)];

    NSTabViewItem* item = [[NSTabViewItem alloc] initWithIdentifier:@"ArtifactPane"];
    item.label = @"ArtifactPane";
    item.view = pane;
    return item;
}

- (NSTabViewItem*)reportInventoryTabItem {
    NSStackView* stack = nil;
    NSView* pane = MakePaneWithStack(&stack);
    [stack addArrangedSubview:MakeIntroLabel(@"Durable reports: browse session and case report artifacts already persisted by tape_engine.",
                                             2)];

    NSStackView* controls = MakeControlRow();

    _reportInventoryRefreshButton = [NSButton buttonWithTitle:@"Refresh Reports"
                                                       target:self
                                                       action:@selector(refreshReportInventory:)];
    [controls addArrangedSubview:_reportInventoryRefreshButton];

    _reportInventoryOpenSessionButton = [NSButton buttonWithTitle:@"Open Selected Session"
                                                           target:self
                                                           action:@selector(openSelectedSessionReport:)];
    _reportInventoryOpenSessionButton.enabled = NO;
    [controls addArrangedSubview:_reportInventoryOpenSessionButton];

    _reportInventoryOpenCaseButton = [NSButton buttonWithTitle:@"Open Selected Case"
                                                        target:self
                                                        action:@selector(openSelectedCaseReport:)];
    _reportInventoryOpenCaseButton.enabled = NO;
    [controls addArrangedSubview:_reportInventoryOpenCaseButton];

    [stack addArrangedSubview:controls];

    _reportInventoryStateLabel = MakeLabel(@"No report inventory loaded yet.",
                                           [NSFont systemFontOfSize:12.0 weight:NSFontWeightMedium],
                                           [NSColor secondaryLabelColor]);
    [stack addArrangedSubview:_reportInventoryStateLabel];

    [stack addArrangedSubview:MakeSectionLabel(@"Session Reports")];

    _sessionReportTableView = MakeStandardTableView(self, self);
    AddTableColumn(_sessionReportTableView, @"report_id", @"report_id", 110.0);
    AddTableColumn(_sessionReportTableView, @"revision_id", @"revision_id", 110.0);
    AddTableColumn(_sessionReportTableView, @"artifact_id", @"artifact_id", 220.0);
    AddTableColumn(_sessionReportTableView, @"headline", @"headline", 360.0);
    ConfigureTablePrimaryAction(_sessionReportTableView, self, @selector(openSelectedSessionReport:));
    [stack addArrangedSubview:MakeTableScrollView(_sessionReportTableView, 140.0)];

    [stack addArrangedSubview:MakeSectionLabel(@"Case Reports")];

    _caseReportTableView = MakeStandardTableView(self, self);
    AddTableColumn(_caseReportTableView, @"report_id", @"report_id", 110.0);
    AddTableColumn(_caseReportTableView, @"report_type", @"report_type", 150.0);
    AddTableColumn(_caseReportTableView, @"artifact_id", @"artifact_id", 220.0);
    AddTableColumn(_caseReportTableView, @"headline", @"headline", 320.0);
    ConfigureTablePrimaryAction(_caseReportTableView, self, @selector(openSelectedCaseReport:));
    [stack addArrangedSubview:MakeTableScrollView(_caseReportTableView, 140.0)];

    _reportInventoryTextView = MakeReadOnlyTextView();
    _reportInventoryTextView.string = @"Refresh report inventory, then select a row to inspect its metadata or open it in ArtifactPane.";
    [stack addArrangedSubview:MakeScrollView(_reportInventoryTextView, 220.0)];

    NSTabViewItem* item = [[NSTabViewItem alloc] initWithIdentifier:@"ReportInventoryPane"];
    item.label = @"ReportInventoryPane";
    item.view = pane;
    return item;
}

- (void)buildInterface {
    NSView* contentView = self.window.contentView;
    NSStackView* root = MakeColumnStack(18.0);
    [contentView addSubview:root];
    [NSLayoutConstraint activateConstraints:@[
        [root.leadingAnchor constraintEqualToAnchor:contentView.leadingAnchor constant:22.0],
        [root.trailingAnchor constraintEqualToAnchor:contentView.trailingAnchor constant:-22.0],
        [root.topAnchor constraintEqualToAnchor:contentView.topAnchor constant:20.0],
        [root.bottomAnchor constraintEqualToAnchor:contentView.bottomAnchor constant:-20.0]
    ]];

    NSStackView* headerStack = nil;
    NSBox* headerCard = MakeCardWithStack(&headerStack, 10.0);

    NSTextField* title = MakeLabel(@"TapeScope",
                                   [NSFont systemFontOfSize:30.0 weight:NSFontWeightBlack],
                                   [NSColor labelColor]);
    [headerStack addArrangedSubview:title];

    NSTextField* subtitle = MakeLabel(@"Phase 4: native status, live-tail, overview, incident, replay-target, range, quality, finding, anchor, order case, report inventory, and artifact/export panes backed by the engine query seam.",
                                      [NSFont systemFontOfSize:13.0 weight:NSFontWeightMedium],
                                      TapeInkMutedColor());
    subtitle.lineBreakMode = NSLineBreakByWordWrapping;
    subtitle.maximumNumberOfLines = 2;
    [headerStack addArrangedSubview:subtitle];

    NSStackView* bannerStack = nil;
    _bannerBox = MakeCardWithStack(&bannerStack, 4.0);
    _bannerBox.fillColor = [[NSColor secondaryLabelColor] colorWithAlphaComponent:0.10];
    _bannerBox.borderColor = [[NSColor secondaryLabelColor] colorWithAlphaComponent:0.18];

    _bannerLabel = MakeLabel(@"Waiting for tape_engine",
                             [NSFont systemFontOfSize:14.0 weight:NSFontWeightSemibold],
                             [NSColor secondaryLabelColor]);
    [bannerStack addArrangedSubview:_bannerLabel];

    NSStackView* bannerControls = MakeControlRow();
    _pollMetaLabel = MakeLabel(@"Polling engine every 2s",
                               [NSFont systemFontOfSize:12.0 weight:NSFontWeightMedium],
                               TapeInkMutedColor());
    [bannerControls addArrangedSubview:_pollMetaLabel];

    _lastProbeLabel = MakeLabel(@"No probe yet",
                                [NSFont monospacedSystemFontOfSize:11.5 weight:NSFontWeightMedium],
                                TapeInkMutedColor());
    [bannerControls addArrangedSubview:_lastProbeLabel];

    _refreshNowButton = [NSButton buttonWithTitle:@"Refresh Now"
                                           target:self
                                           action:@selector(refreshNow:)];
    [bannerControls addArrangedSubview:_refreshNowButton];

    _pollingToggleButton = [NSButton buttonWithTitle:@"Pause Polling"
                                              target:self
                                              action:@selector(togglePolling:)];
    [bannerControls addArrangedSubview:_pollingToggleButton];
    [bannerStack addArrangedSubview:bannerControls];
    [headerStack addArrangedSubview:_bannerBox];
    [root addArrangedSubview:headerCard];

    NSGridView* summaryGrid = [NSGridView gridViewWithViews:@[
        @[MakeSectionLabel(@"Socket"), (_socketValue = MakeValueLabel())],
        @[MakeSectionLabel(@"Data Dir"), (_dataDirValue = MakeValueLabel())],
        @[MakeSectionLabel(@"Instrument"), (_instrumentValue = MakeValueLabel())],
        @[MakeSectionLabel(@"Latest Session Seq"), (_latestSeqValue = MakeValueLabel())],
        @[MakeSectionLabel(@"Live Event Count"), (_liveCountValue = MakeValueLabel())],
        @[MakeSectionLabel(@"Segment Count"), (_segmentCountValue = MakeValueLabel())],
        @[MakeSectionLabel(@"Manifest Hash"), (_manifestHashValue = MakeValueLabel())]
    ]];
    summaryGrid.translatesAutoresizingMaskIntoConstraints = NO;
    summaryGrid.rowSpacing = 8.0;
    summaryGrid.columnSpacing = 18.0;

    NSStackView* summaryStack = nil;
    NSBox* summaryCard = MakeCardWithStack(&summaryStack, 12.0);
    [summaryStack addArrangedSubview:MakeSectionLabel(@"Engine Snapshot")];
    [summaryStack addArrangedSubview:summaryGrid];
    [summaryGrid.widthAnchor constraintEqualToAnchor:summaryStack.widthAnchor].active = YES;
    [root addArrangedSubview:summaryCard];

    _tabView = [[NSTabView alloc] initWithFrame:NSZeroRect];
    _tabView.translatesAutoresizingMaskIntoConstraints = NO;
    [_tabView addTabViewItem:[self textTabItemWithLabel:@"StatusPane" textView:&_statusTextView]];
    [_tabView addTabViewItem:[self liveEventsTabItem]];
    [_tabView addTabViewItem:[self recentHistoryTabItem]];
    [_tabView addTabViewItem:[self overviewTabItem]];
    [_tabView addTabViewItem:[self incidentTabItem]];
    [_tabView addTabViewItem:[self seekTabItem]];
    [_tabView addTabViewItem:[self rangeTabItem]];
    [_tabView addTabViewItem:[self qualityTabItem]];
    [_tabView addTabViewItem:[self findingTabItem]];
    [_tabView addTabViewItem:[self anchorTabItem]];
    [_tabView addTabViewItem:[self orderTabItem]];
    [_tabView addTabViewItem:[self orderCaseTabItem]];
    [_tabView addTabViewItem:[self reportInventoryTabItem]];
    [_tabView addTabViewItem:[self artifactTabItem]];
    [_tabView.heightAnchor constraintGreaterThanOrEqualToConstant:520.0].active = YES;

    NSStackView* tabStack = nil;
    NSBox* tabCard = MakeCardWithStack(&tabStack, 12.0);
    [tabStack addArrangedSubview:MakeSectionLabel(@"Investigation Surface")];
    [tabStack addArrangedSubview:_tabView];
    [root addArrangedSubview:tabCard];

    _statusTextView.string = @"Waiting for the first status response…";
    _liveTextView.string = @"Waiting for the first live-tail response…";
    [self updateBannerAppearanceWithColor:[NSColor secondaryLabelColor]];
}

- (void)showWindowAndStart {
    [self showWindow:nil];
    [self.window makeKeyAndOrderFront:nil];
    [NSApp activateIgnoringOtherApps:YES];
    [self.window makeFirstResponder:_overviewFirstField];
    [self startPolling];
    [self refreshIncidentList:nil];
    [self refreshReportInventory:nil];
}

- (void)updateBannerAppearanceWithColor:(NSColor*)color {
    NSColor* tone = color ?: [NSColor secondaryLabelColor];
    _bannerLabel.textColor = tone;
    if (_bannerBox != nil) {
        _bannerBox.fillColor = [tone colorWithAlphaComponent:0.12];
        _bannerBox.borderColor = [tone colorWithAlphaComponent:0.24];
    }
}

- (void)updatePollingStatusText {
    NSString* pollingState =
        _pollingPaused ? [NSString stringWithFormat:@"Polling paused (manual refresh only, %.1fs baseline)", kPollIntervalSeconds]
                       : [NSString stringWithFormat:@"Polling engine every %.1fs", kPollIntervalSeconds];
    _pollMetaLabel.stringValue = pollingState;
    if (_lastProbeAt != nil) {
        NSDateFormatter* formatter = [[NSDateFormatter alloc] init];
        formatter.timeStyle = NSDateFormatterMediumStyle;
        formatter.dateStyle = NSDateFormatterNoStyle;
        _lastProbeLabel.stringValue =
            [NSString stringWithFormat:@"Last updated %@", [formatter stringFromDate:_lastProbeAt]];
    } else {
        _lastProbeLabel.stringValue = @"No probe yet";
    }
    _pollingToggleButton.title = _pollingPaused ? @"Resume Polling" : @"Pause Polling";
}

- (json)capturePersistentState {
    json state = json::object();
    if (_tabView.selectedTabViewItem.identifier != nil &&
        [_tabView.selectedTabViewItem.identifier isKindOfClass:[NSString class]]) {
        state["selected_tab"] = ToStdString((NSString*)_tabView.selectedTabViewItem.identifier);
    }
    state["polling_paused"] = (_pollingPaused == YES);
    state["recent_history"] = _recentHistoryItems;
    state["overview"] = json{{"first_session_seq", ToStdString(_overviewFirstField.stringValue)},
                             {"last_session_seq", ToStdString(_overviewLastField.stringValue)}};
    state["range"] = json{{"first_session_seq", ToStdString(_rangeFirstField.stringValue)},
                          {"last_session_seq", ToStdString(_rangeLastField.stringValue)}};
    state["quality"] = json{{"first_session_seq", ToStdString(_qualityFirstField.stringValue)},
                            {"last_session_seq", ToStdString(_qualityLastField.stringValue)},
                            {"include_live_tail", _qualityIncludeLiveTailButton.state == NSControlStateValueOn}};
    state["finding"] = json{{"finding_id", ToStdString(_findingIdField.stringValue)}};
    state["anchor"] = json{{"anchor_id", ToStdString(_anchorIdField.stringValue)}};
    state["order_lookup"] = json{{"anchor_kind",
                                  OrderAnchorTypeKey(OrderAnchorTypeFromIndex(_orderAnchorTypePopup.indexOfSelectedItem))},
                                 {"anchor_value", ToStdString(_orderAnchorInputField.stringValue)}};
    state["order_case"] = json{{"anchor_kind",
                                OrderAnchorTypeKey(OrderAnchorTypeFromIndex(_orderCaseAnchorTypePopup.indexOfSelectedItem))},
                               {"anchor_value", ToStdString(_orderCaseAnchorInputField.stringValue)}};
    state["seek"] = json{{"anchor_kind",
                          OrderAnchorTypeKey(OrderAnchorTypeFromIndex(_seekAnchorTypePopup.indexOfSelectedItem))},
                         {"anchor_value", ToStdString(_seekAnchorInputField.stringValue)}};
    state["incident"] = json{{"logical_incident_id", ToStdString(_incidentIdField.stringValue)}};
    state["artifact"] = json{{"artifact_id", ToStdString(_artifactIdField.stringValue)},
                             {"export_format", ToStdString(_artifactExportFormatPopup.titleOfSelectedItem)}};
    return state;
}

- (void)persistApplicationState {
    const json state = [self capturePersistentState];
    NSUserDefaults* defaults = [NSUserDefaults standardUserDefaults];
    [defaults setObject:ToNSString(state.dump()) forKey:kTapeScopeStateDefaultsKey];
}

- (void)restoreApplicationState {
    NSUserDefaults* defaults = [NSUserDefaults standardUserDefaults];
    NSString* raw = [defaults stringForKey:kTapeScopeStateDefaultsKey];
    if (raw == nil || raw.length == 0) {
        return;
    }

    json state;
    try {
        state = json::parse(ToStdString(raw));
    } catch (const std::exception&) {
        return;
    }
    if (!state.is_object()) {
        return;
    }

    _pollingPaused = state.value("polling_paused", false);

    const json overview = state.value("overview", json::object());
    _overviewFirstField.stringValue = ToNSString(overview.value("first_session_seq", std::string()));
    _overviewLastField.stringValue = ToNSString(overview.value("last_session_seq", std::string()));

    const json range = state.value("range", json::object());
    _rangeFirstField.stringValue = ToNSString(range.value("first_session_seq", std::string()));
    _rangeLastField.stringValue = ToNSString(range.value("last_session_seq", std::string()));

    const json quality = state.value("quality", json::object());
    _qualityFirstField.stringValue = ToNSString(quality.value("first_session_seq", std::string()));
    _qualityLastField.stringValue = ToNSString(quality.value("last_session_seq", std::string()));
    _qualityIncludeLiveTailButton.state = quality.value("include_live_tail", false)
                                              ? NSControlStateValueOn
                                              : NSControlStateValueOff;

    const json finding = state.value("finding", json::object());
    _findingIdField.stringValue = ToNSString(finding.value("finding_id", std::string()));

    const json anchor = state.value("anchor", json::object());
    _anchorIdField.stringValue = ToNSString(anchor.value("anchor_id", std::string()));

    const json orderLookup = state.value("order_lookup", json::object());
    [_orderAnchorTypePopup selectItemAtIndex:OrderAnchorTypeIndexForKey(orderLookup.value("anchor_kind", std::string("traceId")))];
    [self orderAnchorTypeChanged:nil];
    _orderAnchorInputField.stringValue = ToNSString(orderLookup.value("anchor_value", std::string()));

    const json orderCase = state.value("order_case", json::object());
    [_orderCaseAnchorTypePopup selectItemAtIndex:OrderAnchorTypeIndexForKey(orderCase.value("anchor_kind", std::string("traceId")))];
    [self orderCaseAnchorTypeChanged:nil];
    _orderCaseAnchorInputField.stringValue = ToNSString(orderCase.value("anchor_value", std::string()));

    const json seek = state.value("seek", json::object());
    [_seekAnchorTypePopup selectItemAtIndex:OrderAnchorTypeIndexForKey(seek.value("anchor_kind", std::string("traceId")))];
    [self seekAnchorTypeChanged:nil];
    _seekAnchorInputField.stringValue = ToNSString(seek.value("anchor_value", std::string()));

    const json incident = state.value("incident", json::object());
    _incidentIdField.stringValue = ToNSString(incident.value("logical_incident_id", std::string()));

    const json artifact = state.value("artifact", json::object());
    _artifactIdField.stringValue = ToNSString(artifact.value("artifact_id", std::string()));
    const std::string exportFormat = artifact.value("export_format", std::string("markdown"));
    if (!exportFormat.empty()) {
        [_artifactExportFormatPopup selectItemWithTitle:ToNSString(exportFormat)];
    }

    _recentHistoryItems.clear();
    const json recentHistory = state.value("recent_history", json::array());
    if (recentHistory.is_array()) {
        for (const auto& item : recentHistory) {
            if (item.is_object()) {
                _recentHistoryItems.push_back(item);
            }
        }
    }
    [_recentTableView reloadData];
    _recentClearButton.enabled = !_recentHistoryItems.empty();
    _recentOpenButton.enabled = NO;
    if (!_recentHistoryItems.empty()) {
        _recentStateLabel.stringValue = @"Restored recent history from the last TapeScope session.";
        _recentStateLabel.textColor = TapeInkMutedColor();
        [_recentTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0] byExtendingSelection:NO];
    }

    const std::string selectedTab = state.value("selected_tab", std::string());
    if (!selectedTab.empty()) {
        [_tabView selectTabViewItemWithIdentifier:ToNSString(selectedTab)];
    }
    [self updatePollingStatusText];
}

- (void)recordRecentHistoryEntry:(json)entry {
    if (!entry.is_object()) {
        return;
    }

    const std::string key = entry.value("kind", std::string()) + "|" + entry.value("target_id", std::string());
    _recentHistoryItems.erase(std::remove_if(_recentHistoryItems.begin(),
                                             _recentHistoryItems.end(),
                                             [&](const json& item) {
                                                 return (item.value("kind", std::string()) + "|" +
                                                         item.value("target_id", std::string())) == key;
                                             }),
                              _recentHistoryItems.end());
    _recentHistoryItems.insert(_recentHistoryItems.begin(), std::move(entry));
    if (_recentHistoryItems.size() > kRecentHistoryLimit) {
        _recentHistoryItems.resize(kRecentHistoryLimit);
    }
    [_recentTableView reloadData];
    _recentClearButton.enabled = !_recentHistoryItems.empty();
    if (_recentHistoryItems.empty()) {
        _recentStateLabel.stringValue = @"Recent history is empty.";
        _recentStateLabel.textColor = TapeInkMutedColor();
        _recentOpenButton.enabled = NO;
        _recentTextView.string = @"Open investigations to build recent history.";
        return;
    }
    _recentStateLabel.stringValue = @"Recent history updated.";
    _recentStateLabel.textColor = [NSColor systemGreenColor];
    [_recentTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0] byExtendingSelection:NO];
    [self persistApplicationState];
}

- (void)recordRecentHistoryForPayloadKind:(const std::string&)kind
                                 targetId:(const std::string&)targetId
                                  payload:(const json&)payload
                            fallbackTitle:(const std::string&)fallbackTitle
                           fallbackDetail:(const std::string&)fallbackDetail
                                 metadata:(const json&)metadata {
    json entry = json::object();
    entry["kind"] = kind;
    entry["target_id"] = targetId;
    entry["headline"] = RecentHistoryHeadlineForPayload(payload, fallbackTitle);
    entry["detail"] = RecentHistoryDetailForPayload(payload, fallbackDetail);
    const std::string artifactId = ExtractArtifactId(payload);
    if (!artifactId.empty()) {
        entry["artifact_id"] = artifactId;
    }
    const json summary = payload.value("summary", json::object());
    const std::uint64_t firstSessionSeq =
        summary.value("first_session_seq", summary.value("from_session_seq", 0ULL));
    const std::uint64_t lastSessionSeq =
        summary.value("last_session_seq", summary.value("to_session_seq", 0ULL));
    if (firstSessionSeq > 0 && lastSessionSeq >= firstSessionSeq) {
        entry["first_session_seq"] = firstSessionSeq;
        entry["last_session_seq"] = lastSessionSeq;
    }
    if (metadata.is_object()) {
        for (auto it = metadata.begin(); it != metadata.end(); ++it) {
            entry[it.key()] = it.value();
        }
    }
    [self recordRecentHistoryEntry:std::move(entry)];
}

- (void)openRecentHistoryEntry:(const json&)entry {
    if (!entry.is_object()) {
        return;
    }

    const std::string kind = entry.value("kind", std::string());
    if (kind == "overview") {
        const std::uint64_t firstSessionSeq = entry.value("first_session_seq", 0ULL);
        const std::uint64_t lastSessionSeq = entry.value("last_session_seq", 0ULL);
        if (firstSessionSeq > 0 && lastSessionSeq >= firstSessionSeq) {
            _overviewFirstField.stringValue = UInt64String(firstSessionSeq);
            _overviewLastField.stringValue = UInt64String(lastSessionSeq);
            [_tabView selectTabViewItemWithIdentifier:@"SessionOverviewPane"];
            [self fetchOverview:nil];
        }
        return;
    }
    if (kind == "incident") {
        const std::uint64_t logicalIncidentId = entry.value("logical_incident_id", 0ULL);
        if (logicalIncidentId > 0) {
            _incidentIdField.stringValue = UInt64String(logicalIncidentId);
            [_tabView selectTabViewItemWithIdentifier:@"IncidentPane"];
            [self fetchIncident:nil];
        }
        return;
    }
    if (kind == "finding") {
        const std::uint64_t findingId = entry.value("finding_id", 0ULL);
        if (findingId > 0) {
            _findingIdField.stringValue = UInt64String(findingId);
            [_tabView selectTabViewItemWithIdentifier:@"FindingPane"];
            [self fetchFinding:nil];
        }
        return;
    }
    if (kind == "anchor") {
        const std::uint64_t anchorId = entry.value("anchor_id", 0ULL);
        if (anchorId > 0) {
            _anchorIdField.stringValue = UInt64String(anchorId);
            [_tabView selectTabViewItemWithIdentifier:@"AnchorPane"];
            [self fetchOrderAnchorById:nil];
        }
        return;
    }
    if (kind == "order_case") {
        const std::string anchorKind = entry.value("anchor_kind", std::string("traceId"));
        const std::string anchorValue = entry.value("anchor_value", std::string());
        if (!anchorValue.empty()) {
            [_orderCaseAnchorTypePopup selectItemAtIndex:OrderAnchorTypeIndexForKey(anchorKind)];
            [self orderCaseAnchorTypeChanged:nil];
            _orderCaseAnchorInputField.stringValue = ToNSString(anchorValue);
            [_tabView selectTabViewItemWithIdentifier:@"OrderCasePane"];
            [self fetchOrderCase:nil];
        }
        return;
    }
    if (kind == "artifact") {
        const std::string artifactId = entry.value("artifact_id", entry.value("target_id", std::string()));
        if (!artifactId.empty()) {
            _artifactIdField.stringValue = ToNSString(artifactId);
            [_tabView selectTabViewItemWithIdentifier:@"ArtifactPane"];
            [self fetchArtifact:nil];
        }
        return;
    }
    if (kind == "range") {
        const std::uint64_t firstSessionSeq = entry.value("first_session_seq", 0ULL);
        const std::uint64_t lastSessionSeq = entry.value("last_session_seq", 0ULL);
        if (firstSessionSeq > 0 && lastSessionSeq >= firstSessionSeq) {
            _rangeFirstField.stringValue = UInt64String(firstSessionSeq);
            _rangeLastField.stringValue = UInt64String(lastSessionSeq);
            [_tabView selectTabViewItemWithIdentifier:@"RangePane"];
            [self fetchRange:nil];
        }
    }
}

- (void)openSelectedRecentHistory:(id)sender {
    (void)sender;
    const NSInteger selected = _recentTableView.selectedRow;
    if (selected < 0 || static_cast<std::size_t>(selected) >= _recentHistoryItems.size()) {
        _recentStateLabel.stringValue = @"Select a recent-history row first.";
        _recentStateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    [self openRecentHistoryEntry:_recentHistoryItems.at(static_cast<std::size_t>(selected))];
}

- (void)clearRecentHistory:(id)sender {
    (void)sender;
    _recentHistoryItems.clear();
    [_recentTableView reloadData];
    _recentOpenButton.enabled = NO;
    _recentClearButton.enabled = NO;
    _recentStateLabel.stringValue = @"Recent history cleared.";
    _recentStateLabel.textColor = TapeInkMutedColor();
    _recentTextView.string = @"Open investigations to repopulate recent history.";
    [self persistApplicationState];
}

- (void)refreshNow:(id)sender {
    (void)sender;
    [self refresh:nil];
}

- (void)togglePolling:(id)sender {
    (void)sender;
    _pollingPaused = !_pollingPaused;
    if (_pollingPaused) {
        [_pollTimer invalidate];
        _pollTimer = nil;
    } else if (_pollTimer == nil) {
        _pollTimer = [NSTimer scheduledTimerWithTimeInterval:kPollIntervalSeconds
                                                      target:self
                                                    selector:@selector(refresh:)
                                                    userInfo:nil
                                                     repeats:YES];
    }
    [self updatePollingStatusText];
    [self persistApplicationState];
}

- (InvestigationPaneModel)overviewPaneModel {
    InvestigationPaneModel pane;
    pane.stateLabel = _overviewStateLabel;
    pane.detailView = _overviewTextView;
    pane.evidenceTableView = _overviewEvidenceTableView;
    pane.openEvidenceButton = _overviewOpenSelectedEvidenceButton;
    pane.loadReplayButton = _overviewLoadReplayButton;
    pane.evidenceItems = &_overviewEvidenceItems;
    pane.hasReplayRange = &_hasOverviewReplayRange;
    pane.replayRange = &_overviewReplayRange;
    pane.emptyEvidenceMessage = @"Read or scan a session overview, then select a citation row to inspect or reopen evidence.";
    pane.missingSelectionMessage = @"Select an overview evidence row first.";
    pane.missingArtifactMessage = @"Selected overview evidence is missing artifact_id.";
    pane.missingReplayMessage = @"No overview replay range is ready yet.";
    return pane;
}

- (InvestigationPaneModel)incidentPaneModel {
    InvestigationPaneModel pane;
    pane.stateLabel = _incidentStateLabel;
    pane.detailView = _incidentTextView;
    pane.evidenceTableView = _incidentEvidenceTableView;
    pane.openEvidenceButton = _incidentOpenSelectedEvidenceButton;
    pane.loadReplayButton = _incidentLoadReplayButton;
    pane.evidenceItems = &_incidentEvidenceItems;
    pane.hasReplayRange = &_hasIncidentReplayRange;
    pane.replayRange = &_incidentReplayRange;
    pane.emptyEvidenceMessage = @"Read an incident, then select a citation row to inspect or reopen evidence.";
    pane.missingSelectionMessage = @"Select an incident evidence row first.";
    pane.missingArtifactMessage = @"Selected incident evidence is missing artifact_id.";
    pane.missingReplayMessage = @"No incident replay window is ready yet.";
    return pane;
}

- (InvestigationPaneModel)findingPaneModel {
    InvestigationPaneModel pane;
    pane.stateLabel = _findingStateLabel;
    pane.detailView = _findingTextView;
    pane.evidenceTableView = _findingEvidenceTableView;
    pane.openEvidenceButton = _findingOpenSelectedEvidenceButton;
    pane.loadReplayButton = _findingLoadReplayButton;
    pane.evidenceItems = &_findingEvidenceItems;
    pane.hasReplayRange = &_hasFindingReplayRange;
    pane.replayRange = &_findingReplayRange;
    pane.emptyEvidenceMessage = @"Read a finding, then select a citation row to inspect or reopen evidence.";
    pane.missingSelectionMessage = @"Select a finding evidence row first.";
    pane.missingArtifactMessage = @"Selected finding evidence is missing artifact_id.";
    pane.missingReplayMessage = @"No finding replay window is ready yet.";
    return pane;
}

- (InvestigationPaneModel)anchorPaneModel {
    InvestigationPaneModel pane;
    pane.stateLabel = _anchorStateLabel;
    pane.detailView = _anchorTextView;
    pane.evidenceTableView = _anchorEvidenceTableView;
    pane.openEvidenceButton = _anchorOpenSelectedEvidenceButton;
    pane.loadReplayButton = _anchorLoadReplayButton;
    pane.evidenceItems = &_anchorEvidenceItems;
    pane.hasReplayRange = &_hasAnchorReplayRange;
    pane.replayRange = &_anchorReplayRange;
    pane.emptyEvidenceMessage = @"Read an order anchor, then select a citation row to inspect or reopen evidence.";
    pane.missingSelectionMessage = @"Select an order-anchor evidence row first.";
    pane.missingArtifactMessage = @"Selected order-anchor evidence is missing artifact_id.";
    pane.missingReplayMessage = @"No order-anchor replay window is ready yet.";
    return pane;
}

- (InvestigationPaneModel)orderCasePaneModel {
    InvestigationPaneModel pane;
    pane.stateLabel = _orderCaseStateLabel;
    pane.detailView = _orderCaseTextView;
    pane.evidenceTableView = _orderCaseEvidenceTableView;
    pane.openEvidenceButton = _orderCaseOpenSelectedEvidenceButton;
    pane.loadReplayButton = _orderCaseLoadReplayButton;
    pane.evidenceItems = &_orderCaseEvidenceItems;
    pane.hasReplayRange = &_hasOrderCaseReplayRange;
    pane.replayRange = &_orderCaseReplayRange;
    pane.emptyEvidenceMessage = @"Read an order case, then select a citation row to inspect or reopen evidence.";
    pane.missingSelectionMessage = @"Select an order-case evidence row first.";
    pane.missingArtifactMessage = @"Selected order-case evidence is missing artifact_id.";
    pane.missingReplayMessage = @"No order-case replay window is ready yet.";
    return pane;
}

- (InvestigationPaneModel)artifactPaneModel {
    InvestigationPaneModel pane;
    pane.stateLabel = _artifactStateLabel;
    pane.detailView = _artifactTextView;
    pane.evidenceTableView = _artifactEvidenceTableView;
    pane.openEvidenceButton = _artifactOpenSelectedEvidenceButton;
    pane.evidenceItems = &_artifactEvidenceItems;
    pane.emptyEvidenceMessage = @"Read an artifact, then select a citation row to inspect or reopen evidence.";
    pane.missingSelectionMessage = @"Select an artifact evidence row first.";
    pane.missingArtifactMessage = @"Selected artifact evidence is missing artifact_id.";
    return pane;
}

- (BOOL)getInvestigationPaneModelForEvidenceTable:(NSTableView*)tableView
                                         outModel:(InvestigationPaneModel*)outModel {
    if (outModel == nullptr || tableView == nil) {
        return NO;
    }
    if (tableView == _overviewEvidenceTableView) {
        *outModel = [self overviewPaneModel];
        return YES;
    }
    if (tableView == _incidentEvidenceTableView) {
        *outModel = [self incidentPaneModel];
        return YES;
    }
    if (tableView == _findingEvidenceTableView) {
        *outModel = [self findingPaneModel];
        return YES;
    }
    if (tableView == _anchorEvidenceTableView) {
        *outModel = [self anchorPaneModel];
        return YES;
    }
    if (tableView == _orderCaseEvidenceTableView) {
        *outModel = [self orderCasePaneModel];
        return YES;
    }
    if (tableView == _artifactEvidenceTableView) {
        *outModel = [self artifactPaneModel];
        return YES;
    }
    return NO;
}

- (void)beginInvestigationRequestForPane:(const InvestigationPaneModel&)pane
                                 message:(NSString*)message {
    if (pane.stateLabel != nil) {
        pane.stateLabel.stringValue = message ?: @"Loading…";
        pane.stateLabel.textColor = [NSColor systemOrangeColor];
    }
    if (pane.detailView != nil) {
        pane.detailView.string = @"Loading investigation details…";
    }
    if (pane.hasReplayRange != nullptr) {
        *pane.hasReplayRange = NO;
    }
    if (pane.loadReplayButton != nil) {
        pane.loadReplayButton.enabled = NO;
    }
    [self resetEvidenceItems:pane.evidenceItems
                   tableView:pane.evidenceTableView
                  openButton:pane.openEvidenceButton];
}

- (void)applyInvestigationResult:(const tapescope::QueryResult<json>&)result
                          toPane:(const InvestigationPaneModel&)pane
                     successText:(NSString*)successText
                syncArtifactField:(BOOL)syncArtifactField {
    if (result.ok()) {
        if (pane.stateLabel != nil) {
            pane.stateLabel.stringValue = successText ?: @"Loaded.";
            pane.stateLabel.textColor = [NSColor systemGreenColor];
        }
        [self applyEvidenceItemsFromResult:result
                                     items:pane.evidenceItems
                                 tableView:pane.evidenceTableView
                                openButton:pane.openEvidenceButton];
        [self applyInvestigationReplayRangeFromResult:result
                                                range:pane.replayRange
                                             hasRange:pane.hasReplayRange
                                         actionButton:pane.loadReplayButton];
        if (syncArtifactField) {
            [self applyArtifactFieldFromResult:result];
        }
        return;
    }

    if (pane.stateLabel != nil) {
        pane.stateLabel.stringValue = ToNSString(tapescope::QueryClient::describeError(result.error));
        pane.stateLabel.textColor = ErrorColorForKind(result.error.kind);
    }
    [self applyInvestigationReplayRangeFromResult:result
                                            range:pane.replayRange
                                         hasRange:pane.hasReplayRange
                                     actionButton:pane.loadReplayButton];
    [self resetEvidenceItems:pane.evidenceItems
                   tableView:pane.evidenceTableView
                  openButton:pane.openEvidenceButton];
}

- (void)loadReplayWindowForPane:(const InvestigationPaneModel&)pane {
    const BOOL available = (pane.hasReplayRange != nullptr) ? *pane.hasReplayRange : NO;
    const tapescope::RangeQuery replayRange =
        (pane.replayRange != nullptr) ? *pane.replayRange : tapescope::RangeQuery{};
    [self loadReplayRange:replayRange
                available:available
               stateLabel:pane.stateLabel
           missingMessage:pane.missingReplayMessage ?: @"No replay window is ready yet."];
}

- (void)openSelectedEvidenceForPane:(const InvestigationPaneModel&)pane {
    [self openSelectedEvidenceFromTable:pane.evidenceTableView
                                  items:(pane.evidenceItems != nullptr ? *pane.evidenceItems : std::vector<json>{})
                             stateLabel:pane.stateLabel
                       missingSelection:pane.missingSelectionMessage ?: @"Select an evidence row first."
                        missingArtifact:pane.missingArtifactMessage ?: @"Selected evidence is missing artifact_id."];
}

- (BOOL)renderSelectionForPane:(const InvestigationPaneModel&)pane {
    return [self renderSelectedEvidenceFromTable:pane.evidenceTableView
                                           items:(pane.evidenceItems != nullptr ? *pane.evidenceItems : std::vector<json>{})
                                      detailView:pane.detailView
                                      openButton:pane.openEvidenceButton
                                    emptyMessage:pane.emptyEvidenceMessage ?: @"Select an evidence row to inspect it."];
}

- (void)resetEvidenceItems:(std::vector<json>*)items
                 tableView:(NSTableView*)tableView
                openButton:(NSButton*)openButton {
    if (items != nullptr) {
        items->clear();
    }
    if (tableView != nil) {
        [tableView reloadData];
    }
    if (openButton != nil) {
        openButton.enabled = NO;
    }
}

- (void)applyEvidenceItemsFromResult:(const tapescope::QueryResult<json>&)result
                               items:(std::vector<json>*)items
                           tableView:(NSTableView*)tableView
                          openButton:(NSButton*)openButton {
    [self resetEvidenceItems:items tableView:tableView openButton:openButton];
    if (!result.ok() || items == nullptr || tableView == nil) {
        return;
    }
    const json citations = ExtractEvidenceCitations(result.value);
    if (!citations.is_array()) {
        return;
    }
    items->assign(citations.begin(), citations.end());
    [tableView reloadData];
    const bool hasRows = !items->empty();
    if (openButton != nil) {
        openButton.enabled = hasRows;
    }
    if (hasRows) {
        [tableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0] byExtendingSelection:NO];
    }
}

- (void)applyArtifactFieldFromResult:(const tapescope::QueryResult<json>&)result {
    if (!result.ok() || _artifactIdField == nil) {
        return;
    }
    const std::string artifactId = ExtractArtifactId(result.value);
    if (!artifactId.empty()) {
        _artifactIdField.stringValue = ToNSString(artifactId);
    }
}

- (void)applyInvestigationReplayRangeFromResult:(const tapescope::QueryResult<json>&)result
                                          range:(tapescope::RangeQuery*)range
                                       hasRange:(BOOL*)hasRange
                                   actionButton:(NSButton*)actionButton {
    BOOL available = NO;
    if (result.ok() && range != nullptr) {
        available = ReplayRangeFromInvestigationSummary(result.value.value("summary", json::object()), range);
    }
    if (hasRange != nullptr) {
        *hasRange = available;
    }
    if (actionButton != nil) {
        actionButton.enabled = available;
    }
}

- (void)loadReplayRange:(const tapescope::RangeQuery&)range
              available:(BOOL)available
             stateLabel:(NSTextField*)stateLabel
         missingMessage:(NSString*)missingMessage {
    if (!available) {
        if (stateLabel != nil) {
            stateLabel.stringValue = missingMessage;
            stateLabel.textColor = [NSColor systemRedColor];
        }
        return;
    }
    _rangeFirstField.stringValue = UInt64String(range.firstSessionSeq);
    _rangeLastField.stringValue = UInt64String(range.lastSessionSeq);
    if (_tabView != nil) {
        [_tabView selectTabViewItemWithIdentifier:@"RangePane"];
    }
    [self fetchRange:nil];
}

- (void)openSelectedEvidenceFromTable:(NSTableView*)tableView
                                items:(const std::vector<json>&)items
                           stateLabel:(NSTextField*)stateLabel
                     missingSelection:(NSString*)missingSelection
                      missingArtifact:(NSString*)missingArtifact {
    const NSInteger selected = tableView.selectedRow;
    if (selected < 0 || static_cast<std::size_t>(selected) >= items.size()) {
        if (stateLabel != nil) {
            stateLabel.stringValue = missingSelection;
            stateLabel.textColor = [NSColor systemRedColor];
        }
        return;
    }
    const json& citation = items.at(static_cast<std::size_t>(selected));
    const std::string artifactId = citation.value("artifact_id", std::string());
    if (artifactId.empty()) {
        if (stateLabel != nil) {
            stateLabel.stringValue = missingArtifact;
            stateLabel.textColor = [NSColor systemRedColor];
        }
        return;
    }
    _artifactIdField.stringValue = ToNSString(artifactId);
    if (_tabView != nil) {
        [_tabView selectTabViewItemWithIdentifier:@"ArtifactPane"];
    }
    [self fetchArtifact:nil];
}

- (BOOL)renderSelectedEvidenceFromTable:(NSTableView*)tableView
                                  items:(const std::vector<json>&)items
                             detailView:(NSTextView*)detailView
                             openButton:(NSButton*)openButton
                           emptyMessage:(NSString*)emptyMessage {
    const NSInteger selected = tableView.selectedRow;
    if (openButton != nil) {
        openButton.enabled = (selected >= 0);
    }
    if (selected < 0 || static_cast<std::size_t>(selected) >= items.size()) {
        if (detailView != nil) {
            detailView.string = emptyMessage;
        }
        return NO;
    }
    if (detailView != nil) {
        detailView.string = ToNSString(items.at(static_cast<std::size_t>(selected)).dump(2));
    }
    return YES;
}

- (void)startPolling {
    [self refresh:_refreshNowButton];
    if (!_pollingPaused) {
        _pollTimer = [NSTimer scheduledTimerWithTimeInterval:kPollIntervalSeconds
                                                      target:self
                                                    selector:@selector(refresh:)
                                                    userInfo:nil
                                                     repeats:YES];
    }
    [self updatePollingStatusText];
}

- (void)shutdown {
    [self persistApplicationState];
    [_pollTimer invalidate];
    _pollTimer = nil;
}

- (void)refresh:(id)sender {
    (void)sender;
    if (_pollingPaused && sender != _refreshNowButton) {
        return;
    }
    if (_pollInFlight || !_client) {
        return;
    }

    _pollInFlight = YES;
    _bannerLabel.stringValue = @"Probing tape_engine…";
    [self updateBannerAppearanceWithColor:[NSColor systemOrangeColor]];

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_pollQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }

        ProbeSnapshot probe;
        probe.status = strongSelf->_client->status();
        probe.liveTail = strongSelf->_client->readLiveTail(kLiveTailLimit);

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
    _lastProbeAt = [NSDate date];
    if (probe.status.ok()) {
        _bannerLabel.stringValue = @"Connected to tape_engine";
        [self updateBannerAppearanceWithColor:[NSColor systemGreenColor]];
        _socketValue.stringValue = ToNSString(probe.status.value.socketPath);
        _dataDirValue.stringValue = ToNSString(probe.status.value.dataDir);
        _instrumentValue.stringValue = ToNSString(probe.status.value.instrumentId);
        _latestSeqValue.stringValue = UInt64String(probe.status.value.latestSessionSeq);
        _liveCountValue.stringValue = UInt64String(probe.status.value.liveEventCount);
        _segmentCountValue.stringValue = UInt64String(probe.status.value.segmentCount);
        _manifestHashValue.stringValue = ToNSString(probe.status.value.manifestHash);
    } else {
        _bannerLabel.stringValue = ToNSString(tapescope::QueryClient::describeError(probe.status.error));
        [self updateBannerAppearanceWithColor:ErrorColorForKind(probe.status.error.kind)];
        _socketValue.stringValue = ToNSString(_client ? _client->config().socketPath : tapescope::defaultSocketPath());
        _dataDirValue.stringValue = @"--";
        _instrumentValue.stringValue = @"--";
        _latestSeqValue.stringValue = @"--";
        _liveCountValue.stringValue = @"--";
        _segmentCountValue.stringValue = @"--";
        _manifestHashValue.stringValue = @"--";
    }

    [self updatePollingStatusText];
    _statusTextView.string = ToNSString(DescribeStatusPane(probe.status,
                                                           _client ? _client->config().socketPath : tapescope::defaultSocketPath()));
    if (probe.liveTail.ok()) {
        _liveEvents = probe.liveTail.value;
        [_liveTableView reloadData];
        if (!_liveEvents.empty()) {
            [_liveTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0] byExtendingSelection:NO];
        } else {
            _liveTextView.string = @"No live events returned.";
        }
    } else {
        _liveEvents.clear();
        [_liveTableView reloadData];
        _liveTextView.string = ToNSString(DescribeLiveEventsPane(probe.liveTail));
    }
}

- (void)fetchRange:(id)sender {
    (void)sender;
    if (_rangeInFlight || !_client) {
        return;
    }

    std::uint64_t firstSessionSeq = 0;
    std::uint64_t lastSessionSeq = 0;
    if (!ParsePositiveUInt64(ToStdString(_rangeFirstField.stringValue), &firstSessionSeq) ||
        !ParsePositiveUInt64(ToStdString(_rangeLastField.stringValue), &lastSessionSeq) ||
        firstSessionSeq > lastSessionSeq) {
        _rangeStateLabel.stringValue = @"Range inputs must be positive integers with first <= last.";
        _rangeStateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    _rangeInFlight = YES;
    _rangeFetchButton.enabled = NO;
    _rangeStateLabel.stringValue = @"Fetching replay window…";
    _rangeStateLabel.textColor = [NSColor systemOrangeColor];
    _rangeTextView.string = @"Loading replay window and decoded events…";
    _lastRangeQuery.firstSessionSeq = firstSessionSeq;
    _lastRangeQuery.lastSessionSeq = lastSessionSeq;

    __weak TapeScopeWindowController* weakSelf = self;
    const tapescope::RangeQuery query = _lastRangeQuery;
    dispatch_async(_pollQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->readRange(query);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil) {
                return;
            }
            innerSelf->_rangeInFlight = NO;
            innerSelf->_rangeFetchButton.enabled = YES;
            if (result.ok()) {
                innerSelf->_rangeEvents = result.value;
                [innerSelf->_rangeTableView reloadData];
                if (!innerSelf->_rangeEvents.empty()) {
                    innerSelf->_rangeStateLabel.stringValue = @"Replay window loaded.";
                    innerSelf->_rangeStateLabel.textColor = [NSColor systemGreenColor];
                    [innerSelf->_rangeTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0] byExtendingSelection:NO];
                } else {
                    innerSelf->_rangeStateLabel.stringValue = @"Replay window is empty for that range.";
                    innerSelf->_rangeStateLabel.textColor = TapeInkMutedColor();
                    innerSelf->_rangeTextView.string = @"No decoded events are available for the requested replay window. Try widening the session_seq range or loading a replay target from an order, finding, or incident.";
                }
            } else {
                innerSelf->_rangeStateLabel.stringValue = ToNSString(tapescope::QueryClient::describeError(result.error));
                innerSelf->_rangeStateLabel.textColor = ErrorColorForKind(result.error.kind);
                innerSelf->_rangeEvents.clear();
                [innerSelf->_rangeTableView reloadData];
                innerSelf->_rangeTextView.string = ToNSString(DescribeRangeResult(query, result));
            }
        });
    });
}

- (void)loadQualityFromRange:(id)sender {
    (void)sender;
    _qualityFirstField.stringValue = _rangeFirstField.stringValue;
    _qualityLastField.stringValue = _rangeLastField.stringValue;
    if (_tabView != nil) {
        [_tabView selectTabViewItemWithIdentifier:@"QualityPane"];
    }
    [self fetchSessionQuality:nil];
}

- (void)fetchSessionQuality:(id)sender {
    (void)sender;
    if (_qualityInFlight || !_client) {
        return;
    }

    std::uint64_t firstSessionSeq = 0;
    std::uint64_t lastSessionSeq = 0;
    if (!ParsePositiveUInt64(ToStdString(_qualityFirstField.stringValue), &firstSessionSeq) ||
        !ParsePositiveUInt64(ToStdString(_qualityLastField.stringValue), &lastSessionSeq) ||
        firstSessionSeq > lastSessionSeq) {
        _qualityStateLabel.stringValue = @"Quality inputs must be positive integers with first <= last.";
        _qualityStateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    _qualityInFlight = YES;
    _qualityFetchButton.enabled = NO;
    _qualityStateLabel.stringValue = @"Reading session quality…";
    _qualityStateLabel.textColor = [NSColor systemOrangeColor];
    _qualityTextView.string = @"Loading data-quality summary and provenance coverage…";
    _lastQualityQuery.firstSessionSeq = firstSessionSeq;
    _lastQualityQuery.lastSessionSeq = lastSessionSeq;
    const bool includeLiveTail = (_qualityIncludeLiveTailButton.state == NSControlStateValueOn);

    __weak TapeScopeWindowController* weakSelf = self;
    const tapescope::RangeQuery query = _lastQualityQuery;
    dispatch_async(_pollQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->readSessionQuality(query, includeLiveTail);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil) {
                return;
            }
            innerSelf->_qualityInFlight = NO;
            innerSelf->_qualityFetchButton.enabled = YES;
            if (result.ok()) {
                innerSelf->_qualityStateLabel.stringValue = @"Session quality loaded.";
                innerSelf->_qualityStateLabel.textColor = [NSColor systemGreenColor];
            } else {
                innerSelf->_qualityStateLabel.stringValue = ToNSString(tapescope::QueryClient::describeError(result.error));
                innerSelf->_qualityStateLabel.textColor = ErrorColorForKind(result.error.kind);
            }
            innerSelf->_qualityTextView.string = ToNSString(DescribeSessionQualityResult(query, includeLiveTail, result));
        });
    });
}

- (void)fetchFinding:(id)sender {
    (void)sender;
    if (_findingInFlight || !_client) {
        return;
    }

    std::uint64_t findingId = 0;
    if (!ParsePositiveUInt64(ToStdString(_findingIdField.stringValue), &findingId)) {
        _findingStateLabel.stringValue = @"finding_id must be a positive integer.";
        _findingStateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    const InvestigationPaneModel pane = [self findingPaneModel];
    _findingInFlight = YES;
    _findingFetchButton.enabled = NO;
    [self beginInvestigationRequestForPane:pane message:@"Reading finding…"];

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_pollQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->readFinding(findingId);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil) {
                return;
            }
            innerSelf->_findingInFlight = NO;
            innerSelf->_findingFetchButton.enabled = YES;
            [innerSelf applyInvestigationResult:result
                                         toPane:[innerSelf findingPaneModel]
                                    successText:@"Finding loaded."
                               syncArtifactField:YES];
            if (result.ok()) {
                [innerSelf recordRecentHistoryForPayloadKind:"finding"
                                                    targetId:std::to_string(findingId)
                                                     payload:result.value
                                               fallbackTitle:"Finding " + std::to_string(findingId)
                                              fallbackDetail:"Reopen the persisted finding drilldown."
                                                    metadata:json{{"finding_id", findingId}}];
            }
            innerSelf->_findingTextView.string =
                ToNSString(DescribeInvestigationPayload("finding",
                                                       "finding_id=" + std::to_string(findingId),
                                                       result));
        });
    });
}

- (void)loadReplayWindowFromFinding:(id)sender {
    (void)sender;
    [self loadReplayWindowForPane:[self findingPaneModel]];
}

- (void)openSelectedFindingEvidence:(id)sender {
    (void)sender;
    [self openSelectedEvidenceForPane:[self findingPaneModel]];
}

- (void)fetchOrderAnchorById:(id)sender {
    (void)sender;
    if (_anchorInFlight || !_client) {
        return;
    }

    std::uint64_t anchorId = 0;
    if (!ParsePositiveUInt64(ToStdString(_anchorIdField.stringValue), &anchorId)) {
        _anchorStateLabel.stringValue = @"anchor_id must be a positive integer.";
        _anchorStateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    const InvestigationPaneModel pane = [self anchorPaneModel];
    _anchorInFlight = YES;
    _anchorFetchButton.enabled = NO;
    [self beginInvestigationRequestForPane:pane message:@"Reading order anchor…"];

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_pollQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->readOrderAnchor(anchorId);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil) {
                return;
            }
            innerSelf->_anchorInFlight = NO;
            innerSelf->_anchorFetchButton.enabled = YES;
            [innerSelf applyInvestigationResult:result
                                         toPane:[innerSelf anchorPaneModel]
                                    successText:@"Order anchor loaded."
                               syncArtifactField:YES];
            if (result.ok()) {
                [innerSelf recordRecentHistoryForPayloadKind:"anchor"
                                                    targetId:std::to_string(anchorId)
                                                     payload:result.value
                                               fallbackTitle:"Order anchor " + std::to_string(anchorId)
                                              fallbackDetail:"Reopen the persisted order anchor drilldown."
                                                    metadata:json{{"anchor_id", anchorId}}];
            }
            innerSelf->_anchorTextView.string =
                ToNSString(DescribeInvestigationPayload("order_anchor",
                                                       "anchor_id=" + std::to_string(anchorId),
                                                       result));
        });
    });
}

- (void)loadReplayWindowFromAnchor:(id)sender {
    (void)sender;
    [self loadReplayWindowForPane:[self anchorPaneModel]];
}

- (void)openSelectedAnchorEvidence:(id)sender {
    (void)sender;
    [self openSelectedEvidenceForPane:[self anchorPaneModel]];
}

- (void)fetchOverview:(id)sender {
    (void)sender;
    if (_overviewInFlight || !_client) {
        return;
    }

    std::uint64_t firstSessionSeq = 0;
    std::uint64_t lastSessionSeq = 0;
    if (!ParsePositiveUInt64(ToStdString(_overviewFirstField.stringValue), &firstSessionSeq) ||
        !ParsePositiveUInt64(ToStdString(_overviewLastField.stringValue), &lastSessionSeq) ||
        firstSessionSeq > lastSessionSeq) {
        _overviewStateLabel.stringValue = @"Overview inputs must be positive integers with first <= last.";
        _overviewStateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    const InvestigationPaneModel pane = [self overviewPaneModel];
    _overviewInFlight = YES;
    _overviewFetchButton.enabled = NO;
    _overviewScanButton.enabled = NO;
    _overviewOpenSelectedIncidentButton.enabled = NO;
    [self beginInvestigationRequestForPane:pane message:@"Reading session overview…"];
    _lastOverviewQuery.firstSessionSeq = firstSessionSeq;
    _lastOverviewQuery.lastSessionSeq = lastSessionSeq;

    __weak TapeScopeWindowController* weakSelf = self;
    const tapescope::RangeQuery query = _lastOverviewQuery;
    dispatch_async(_pollQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->readSessionOverview(query);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil) {
                return;
            }
            innerSelf->_overviewInFlight = NO;
            innerSelf->_overviewFetchButton.enabled = YES;
            innerSelf->_overviewScanButton.enabled = YES;
            if (result.ok()) {
                const json incidents = result.value.value("events", json::array());
                innerSelf->_overviewIncidents.clear();
                if (incidents.is_array()) {
                    innerSelf->_overviewIncidents.assign(incidents.begin(), incidents.end());
                }
                [innerSelf->_overviewIncidentTableView reloadData];
                [innerSelf applyInvestigationResult:result
                                             toPane:[innerSelf overviewPaneModel]
                                        successText:@"Session overview loaded."
                                   syncArtifactField:NO];
                [innerSelf recordRecentHistoryForPayloadKind:"overview"
                                                    targetId:std::to_string(query.firstSessionSeq) + "-" +
                                                             std::to_string(query.lastSessionSeq)
                                                     payload:result.value
                                               fallbackTitle:"Session overview " +
                                                             std::to_string(query.firstSessionSeq) + "-" +
                                                             std::to_string(query.lastSessionSeq)
                                              fallbackDetail:"Reopen the recent session-overview range."
                                                    metadata:json{{"first_session_seq", query.firstSessionSeq},
                                                                  {"last_session_seq", query.lastSessionSeq}}];
                innerSelf->_overviewOpenSelectedIncidentButton.enabled = !innerSelf->_overviewIncidents.empty();
                if (!innerSelf->_overviewIncidents.empty()) {
                    [innerSelf->_overviewIncidentTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0]
                                                         byExtendingSelection:NO];
                }
            } else {
                innerSelf->_overviewIncidents.clear();
                [innerSelf->_overviewIncidentTableView reloadData];
                [innerSelf applyInvestigationResult:result
                                             toPane:[innerSelf overviewPaneModel]
                                        successText:nil
                                   syncArtifactField:NO];
                innerSelf->_overviewOpenSelectedIncidentButton.enabled = NO;
            }
            innerSelf->_overviewTextView.string =
                ToNSString(DescribeInvestigationPayload("session_overview",
                                                       "session_seq=[" + std::to_string(query.firstSessionSeq) + ", " +
                                                           std::to_string(query.lastSessionSeq) + "]",
                                                       result));
        });
    });
}

- (void)scanOverviewReport:(id)sender {
    (void)sender;
    if (_overviewInFlight || !_client) {
        return;
    }

    std::uint64_t firstSessionSeq = 0;
    std::uint64_t lastSessionSeq = 0;
    if (!ParsePositiveUInt64(ToStdString(_overviewFirstField.stringValue), &firstSessionSeq) ||
        !ParsePositiveUInt64(ToStdString(_overviewLastField.stringValue), &lastSessionSeq) ||
        firstSessionSeq > lastSessionSeq) {
        _overviewStateLabel.stringValue = @"Overview inputs must be positive integers with first <= last.";
        _overviewStateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    const InvestigationPaneModel pane = [self overviewPaneModel];
    _overviewInFlight = YES;
    _overviewFetchButton.enabled = NO;
    _overviewScanButton.enabled = NO;
    _overviewOpenSelectedIncidentButton.enabled = NO;
    [self beginInvestigationRequestForPane:pane message:@"Scanning durable session report…"];
    _lastOverviewQuery.firstSessionSeq = firstSessionSeq;
    _lastOverviewQuery.lastSessionSeq = lastSessionSeq;

    __weak TapeScopeWindowController* weakSelf = self;
    const tapescope::RangeQuery query = _lastOverviewQuery;
    dispatch_async(_pollQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->scanSessionReport(query);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil) {
                return;
            }
            innerSelf->_overviewInFlight = NO;
            innerSelf->_overviewFetchButton.enabled = YES;
            innerSelf->_overviewScanButton.enabled = YES;
            if (result.ok()) {
                const json incidents = result.value.value("events", json::array());
                innerSelf->_overviewIncidents.clear();
                if (incidents.is_array()) {
                    innerSelf->_overviewIncidents.assign(incidents.begin(), incidents.end());
                }
                [innerSelf->_overviewIncidentTableView reloadData];
                [innerSelf applyInvestigationResult:result
                                             toPane:[innerSelf overviewPaneModel]
                                        successText:@"Durable session report scanned."
                                   syncArtifactField:YES];
                const std::string artifactId = ExtractArtifactId(result.value);
                if (!artifactId.empty()) {
                    [innerSelf recordRecentHistoryForPayloadKind:"artifact"
                                                        targetId:artifactId
                                                         payload:result.value
                                                   fallbackTitle:"Session report " + artifactId
                                                  fallbackDetail:"Reopen the durable scanned session report."
                                                        metadata:json{{"artifact_id", artifactId}}];
                }
                innerSelf->_overviewOpenSelectedIncidentButton.enabled = !innerSelf->_overviewIncidents.empty();
                if (!innerSelf->_overviewIncidents.empty()) {
                    [innerSelf->_overviewIncidentTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0]
                                                         byExtendingSelection:NO];
                }
                [innerSelf refreshReportInventory:nil];
            } else {
                innerSelf->_overviewIncidents.clear();
                [innerSelf->_overviewIncidentTableView reloadData];
                [innerSelf applyInvestigationResult:result
                                             toPane:[innerSelf overviewPaneModel]
                                        successText:nil
                                   syncArtifactField:NO];
                innerSelf->_overviewOpenSelectedIncidentButton.enabled = NO;
            }
            innerSelf->_overviewTextView.string =
                ToNSString(DescribeInvestigationPayload("session_report_scan",
                                                       "session_seq=[" + std::to_string(query.firstSessionSeq) + ", " +
                                                           std::to_string(query.lastSessionSeq) + "]",
                                                       result));
        });
    });
}

- (void)loadReplayWindowFromOverview:(id)sender {
    (void)sender;
    [self loadReplayWindowForPane:[self overviewPaneModel]];
}

- (void)openSelectedOverviewEvidence:(id)sender {
    (void)sender;
    [self openSelectedEvidenceForPane:[self overviewPaneModel]];
}

- (BOOL)buildOrderAnchorQueryFromPopup:(NSPopUpButton*)popup
                            inputField:(NSTextField*)inputField
                             outQuery:(tapescope::OrderAnchorQuery*)outQuery
                           descriptor:(std::string*)descriptor
                                error:(std::string*)error {
    if (outQuery == nullptr || descriptor == nullptr || error == nullptr) {
        return NO;
    }

    const OrderAnchorType type = OrderAnchorTypeFromIndex(popup.indexOfSelectedItem);
    const std::string rawValue = TrimAscii(ToStdString(inputField.stringValue));
    if (rawValue.empty()) {
        *error = "An anchor value is required.";
        return NO;
    }

    *outQuery = tapescope::OrderAnchorQuery{};
    *descriptor = std::string();
    switch (type) {
        case OrderAnchorType::TraceId: {
            std::uint64_t parsed = 0;
            if (!ParsePositiveUInt64(rawValue, &parsed)) {
                *error = "traceId must be a positive integer.";
                return NO;
            }
            outQuery->traceId = parsed;
            *descriptor = "traceId=" + std::to_string(parsed);
            return YES;
        }
        case OrderAnchorType::OrderId: {
            long long parsed = 0;
            if (!ParsePositiveInt64(rawValue, &parsed)) {
                *error = "orderId must be a positive integer.";
                return NO;
            }
            outQuery->orderId = parsed;
            *descriptor = "orderId=" + std::to_string(parsed);
            return YES;
        }
        case OrderAnchorType::PermId: {
            long long parsed = 0;
            if (!ParsePositiveInt64(rawValue, &parsed)) {
                *error = "permId must be a positive integer.";
                return NO;
            }
            outQuery->permId = parsed;
            *descriptor = "permId=" + std::to_string(parsed);
            return YES;
        }
        case OrderAnchorType::ExecId:
            outQuery->execId = rawValue;
            *descriptor = "execId=" + rawValue;
            return YES;
    }
    *error = "Unsupported anchor selector.";
    return NO;
}

- (void)orderAnchorTypeChanged:(id)sender {
    (void)sender;
    _orderAnchorInputField.placeholderString =
        PlaceholderForOrderAnchorType(OrderAnchorTypeFromIndex(_orderAnchorTypePopup.indexOfSelectedItem));
}

- (void)performOrderLookup:(id)sender {
    (void)sender;
    if (_orderLookupInFlight || !_client) {
        return;
    }

    tapescope::OrderAnchorQuery query;
    std::string descriptor;
    std::string errorMessage;
    if (![self buildOrderAnchorQueryFromPopup:_orderAnchorTypePopup
                                   inputField:_orderAnchorInputField
                                    outQuery:&query
                                  descriptor:&descriptor
                                       error:&errorMessage]) {
        _orderStateLabel.stringValue = ToNSString(errorMessage);
        _orderStateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    _orderLookupInFlight = YES;
    _orderLookupButton.enabled = NO;
    _orderStateLabel.stringValue = @"Looking up order anchor…";
    _orderStateLabel.textColor = [NSColor systemOrangeColor];
    _orderTextView.string = @"Looking up anchor-linked events and replay context…";

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_pollQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->findOrderAnchor(query);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil) {
                return;
            }
            innerSelf->_orderLookupInFlight = NO;
            innerSelf->_orderLookupButton.enabled = YES;
            if (result.ok()) {
                const json events = result.value.value("events", json::array());
                innerSelf->_orderEvents.clear();
                if (events.is_array()) {
                    innerSelf->_orderEvents.assign(events.begin(), events.end());
                }
                [innerSelf->_orderTableView reloadData];
                if (!innerSelf->_orderEvents.empty()) {
                    innerSelf->_orderStateLabel.stringValue = @"Order lookup complete.";
                    innerSelf->_orderStateLabel.textColor = [NSColor systemGreenColor];
                    [innerSelf->_orderTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0] byExtendingSelection:NO];
                } else {
                    innerSelf->_orderStateLabel.stringValue = @"No anchor-linked events matched that selector.";
                    innerSelf->_orderStateLabel.textColor = TapeInkMutedColor();
                    innerSelf->_orderTextView.string = ToNSString(DescribeOrderLookupResult(descriptor, result));
                }
            } else {
                innerSelf->_orderStateLabel.stringValue = ToNSString(tapescope::QueryClient::describeError(result.error));
                innerSelf->_orderStateLabel.textColor = ErrorColorForKind(result.error.kind);
                innerSelf->_orderEvents.clear();
                [innerSelf->_orderTableView reloadData];
                innerSelf->_orderTextView.string = ToNSString(DescribeOrderLookupResult(descriptor, result));
            }
        });
    });
}

- (void)orderCaseAnchorTypeChanged:(id)sender {
    (void)sender;
    _orderCaseAnchorInputField.placeholderString =
        PlaceholderForOrderAnchorType(OrderAnchorTypeFromIndex(_orderCaseAnchorTypePopup.indexOfSelectedItem));
}

- (void)seekAnchorTypeChanged:(id)sender {
    (void)sender;
    _seekAnchorInputField.placeholderString =
        PlaceholderForOrderAnchorType(OrderAnchorTypeFromIndex(_seekAnchorTypePopup.indexOfSelectedItem));
}

- (void)fetchOrderCase:(id)sender {
    (void)sender;
    if (_orderCaseInFlight || !_client) {
        return;
    }

    tapescope::OrderAnchorQuery query;
    std::string descriptor;
    std::string errorMessage;
    if (![self buildOrderAnchorQueryFromPopup:_orderCaseAnchorTypePopup
                                   inputField:_orderCaseAnchorInputField
                                    outQuery:&query
                                  descriptor:&descriptor
                                       error:&errorMessage]) {
        _orderCaseStateLabel.stringValue = ToNSString(errorMessage);
        _orderCaseStateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    const InvestigationPaneModel pane = [self orderCasePaneModel];
    _orderCaseInFlight = YES;
    _orderCaseFetchButton.enabled = NO;
    _orderCaseScanButton.enabled = NO;
    [self beginInvestigationRequestForPane:pane message:@"Reading order case…"];

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_pollQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->readOrderCase(query);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil) {
                return;
            }
            innerSelf->_orderCaseInFlight = NO;
            innerSelf->_orderCaseFetchButton.enabled = YES;
            innerSelf->_orderCaseScanButton.enabled = YES;
            [innerSelf applyInvestigationResult:result
                                         toPane:[innerSelf orderCasePaneModel]
                                    successText:@"Order case loaded."
                               syncArtifactField:YES];
            if (result.ok()) {
                [innerSelf recordRecentHistoryForPayloadKind:"order_case"
                                                    targetId:descriptor
                                                     payload:result.value
                                               fallbackTitle:"Order case " + descriptor
                                              fallbackDetail:"Reopen the recent order-case investigation."
                                                    metadata:json{{"anchor_kind",
                                                                    OrderAnchorTypeKey(OrderAnchorTypeFromIndex(innerSelf->_orderCaseAnchorTypePopup.indexOfSelectedItem))},
                                                                  {"anchor_value",
                                                                    ToStdString(innerSelf->_orderCaseAnchorInputField.stringValue)}}];
            }
            innerSelf->_orderCaseTextView.string =
                ToNSString(DescribeInvestigationPayload("order_case", descriptor, result));
        });
    });
}

- (void)scanOrderCaseReport:(id)sender {
    (void)sender;
    if (_orderCaseInFlight || !_client) {
        return;
    }

    tapescope::OrderAnchorQuery query;
    std::string descriptor;
    std::string errorMessage;
    if (![self buildOrderAnchorQueryFromPopup:_orderCaseAnchorTypePopup
                                   inputField:_orderCaseAnchorInputField
                                    outQuery:&query
                                  descriptor:&descriptor
                                       error:&errorMessage]) {
        _orderCaseStateLabel.stringValue = ToNSString(errorMessage);
        _orderCaseStateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    const InvestigationPaneModel pane = [self orderCasePaneModel];
    _orderCaseInFlight = YES;
    _orderCaseFetchButton.enabled = NO;
    _orderCaseScanButton.enabled = NO;
    [self beginInvestigationRequestForPane:pane message:@"Scanning durable order-case report…"];

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_pollQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->scanOrderCaseReport(query);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil) {
                return;
            }
            innerSelf->_orderCaseInFlight = NO;
            innerSelf->_orderCaseFetchButton.enabled = YES;
            innerSelf->_orderCaseScanButton.enabled = YES;
            [innerSelf applyInvestigationResult:result
                                         toPane:[innerSelf orderCasePaneModel]
                                    successText:@"Durable order-case report scanned."
                               syncArtifactField:YES];
            if (result.ok()) {
                const std::string artifactId = ExtractArtifactId(result.value);
                if (!artifactId.empty()) {
                    [innerSelf recordRecentHistoryForPayloadKind:"artifact"
                                                        targetId:artifactId
                                                         payload:result.value
                                                   fallbackTitle:"Order-case report " + artifactId
                                                  fallbackDetail:"Reopen the durable scanned order-case report."
                                                        metadata:json{{"artifact_id", artifactId}}];
                }
                [innerSelf refreshReportInventory:nil];
            }
            innerSelf->_orderCaseTextView.string =
                ToNSString(DescribeInvestigationPayload("order_case_report_scan", descriptor, result));
        });
    });
}

- (void)fetchSeekOrder:(id)sender {
    (void)sender;
    if (_seekInFlight || !_client) {
        return;
    }

    tapescope::OrderAnchorQuery query;
    std::string descriptor;
    std::string errorMessage;
    if (![self buildOrderAnchorQueryFromPopup:_seekAnchorTypePopup
                                   inputField:_seekAnchorInputField
                                    outQuery:&query
                                  descriptor:&descriptor
                                       error:&errorMessage]) {
        _seekStateLabel.stringValue = ToNSString(errorMessage);
        _seekStateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    _seekInFlight = YES;
    _seekFetchButton.enabled = NO;
    _seekLoadRangeButton.enabled = NO;
    _hasSeekReplayRange = NO;
    _seekStateLabel.stringValue = @"Computing replay target…";
    _seekStateLabel.textColor = [NSColor systemOrangeColor];
    _seekTextView.string = @"Computing replay target and protected-window context…";

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_pollQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->seekOrderAnchor(query);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil) {
                return;
            }
            innerSelf->_seekInFlight = NO;
            innerSelf->_seekFetchButton.enabled = YES;
            if (result.ok()) {
                innerSelf->_seekStateLabel.stringValue = @"Replay target ready.";
                innerSelf->_seekStateLabel.textColor = [NSColor systemGreenColor];
                innerSelf->_hasSeekReplayRange = ReplayRangeFromSeekSummary(result.value.value("summary", json::object()),
                                                                           &innerSelf->_seekReplayRange);
                innerSelf->_seekLoadRangeButton.enabled = innerSelf->_hasSeekReplayRange;
            } else {
                innerSelf->_seekStateLabel.stringValue = ToNSString(tapescope::QueryClient::describeError(result.error));
                innerSelf->_seekStateLabel.textColor = ErrorColorForKind(result.error.kind);
                innerSelf->_seekLoadRangeButton.enabled = NO;
            }
            innerSelf->_seekTextView.string = ToNSString(DescribeSeekOrderResult(descriptor, result));
        });
    });
}

- (void)loadReplayWindowFromSeek:(id)sender {
    (void)sender;
    if (!_hasSeekReplayRange) {
        _seekStateLabel.stringValue = @"No replay target window is ready yet.";
        _seekStateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    _rangeFirstField.stringValue = UInt64String(_seekReplayRange.firstSessionSeq);
    _rangeLastField.stringValue = UInt64String(_seekReplayRange.lastSessionSeq);
    if (_tabView != nil) {
        [_tabView selectTabViewItemWithIdentifier:@"RangePane"];
    }
    [self fetchRange:nil];
}

- (void)loadReplayWindowFromOrderCase:(id)sender {
    (void)sender;
    [self loadReplayWindowForPane:[self orderCasePaneModel]];
}

- (void)openSelectedOrderCaseEvidence:(id)sender {
    (void)sender;
    [self openSelectedEvidenceForPane:[self orderCasePaneModel]];
}

- (void)fetchIncident:(id)sender {
    (void)sender;
    if (_incidentInFlight || !_client) {
        return;
    }

    std::uint64_t logicalIncidentId = 0;
    if (!ParsePositiveUInt64(ToStdString(_incidentIdField.stringValue), &logicalIncidentId)) {
        _incidentStateLabel.stringValue = @"logical_incident_id must be a positive integer.";
        _incidentStateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    const InvestigationPaneModel pane = [self incidentPaneModel];
    _incidentInFlight = YES;
    _incidentFetchButton.enabled = NO;
    _incidentRefreshButton.enabled = NO;
    _incidentOpenSelectedButton.enabled = NO;
    [self beginInvestigationRequestForPane:pane message:@"Reading incident drilldown…"];

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_pollQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->readIncident(logicalIncidentId);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil) {
                return;
            }
            innerSelf->_incidentInFlight = NO;
            innerSelf->_incidentFetchButton.enabled = YES;
            innerSelf->_incidentRefreshButton.enabled = YES;
            innerSelf->_incidentOpenSelectedButton.enabled = (innerSelf->_incidentTableView.selectedRow >= 0);
            [innerSelf applyInvestigationResult:result
                                         toPane:[innerSelf incidentPaneModel]
                                    successText:@"Incident loaded."
                               syncArtifactField:YES];
            if (result.ok()) {
                [innerSelf recordRecentHistoryForPayloadKind:"incident"
                                                    targetId:std::to_string(logicalIncidentId)
                                                     payload:result.value
                                               fallbackTitle:"Incident " + std::to_string(logicalIncidentId)
                                              fallbackDetail:"Reopen the incident drilldown."
                                                    metadata:json{{"logical_incident_id", logicalIncidentId}}];
            }
            innerSelf->_incidentTextView.string =
                ToNSString(DescribeInvestigationPayload("incident", "logical_incident_id=" + std::to_string(logicalIncidentId), result));
        });
    });
}

- (void)loadReplayWindowFromIncident:(id)sender {
    (void)sender;
    [self loadReplayWindowForPane:[self incidentPaneModel]];
}

- (void)openSelectedIncidentEvidence:(id)sender {
    (void)sender;
    [self openSelectedEvidenceForPane:[self incidentPaneModel]];
}

- (void)openSelectedOverviewIncident:(id)sender {
    (void)sender;
    const NSInteger selected = _overviewIncidentTableView.selectedRow;
    if (selected < 0 || static_cast<std::size_t>(selected) >= _overviewIncidents.size()) {
        _overviewStateLabel.stringValue = @"Select an overview incident row first.";
        _overviewStateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    const json& row = _overviewIncidents.at(static_cast<std::size_t>(selected));
    const std::uint64_t logicalIncidentId = row.value("logical_incident_id", 0ULL);
    if (logicalIncidentId == 0) {
        _overviewStateLabel.stringValue = @"Selected overview incident is missing logical_incident_id.";
        _overviewStateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    _incidentIdField.stringValue = UInt64String(logicalIncidentId);
    if (_tabView != nil) {
        [_tabView selectTabViewItemWithIdentifier:@"IncidentPane"];
    }
    [self fetchIncident:nil];
}

- (void)refreshIncidentList:(id)sender {
    (void)sender;
    if (_incidentInFlight || !_client) {
        return;
    }

    _incidentInFlight = YES;
    _incidentRefreshButton.enabled = NO;
    _incidentFetchButton.enabled = NO;
    _incidentOpenSelectedButton.enabled = NO;
    _incidentStateLabel.stringValue = @"Refreshing incident list…";
    _incidentStateLabel.textColor = [NSColor systemOrangeColor];
    _incidentTextView.string = @"Refreshing ranked incidents…";

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_pollQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->listIncidents(40);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil) {
                return;
            }
            innerSelf->_incidentInFlight = NO;
            innerSelf->_incidentRefreshButton.enabled = YES;
            innerSelf->_incidentFetchButton.enabled = YES;
            innerSelf->_latestIncidents.clear();
            if (result.ok()) {
                const json rows = result.value.value("events", json::array());
                if (rows.is_array()) {
                    innerSelf->_latestIncidents.assign(rows.begin(), rows.end());
                }
                if (!innerSelf->_latestIncidents.empty()) {
                    innerSelf->_incidentStateLabel.stringValue = @"Incident list loaded.";
                    innerSelf->_incidentStateLabel.textColor = [NSColor systemGreenColor];
                } else {
                    innerSelf->_incidentStateLabel.stringValue = @"No ranked incidents are available yet.";
                    innerSelf->_incidentStateLabel.textColor = TapeInkMutedColor();
                }
            } else {
                innerSelf->_incidentStateLabel.stringValue = ToNSString(tapescope::QueryClient::describeError(result.error));
                innerSelf->_incidentStateLabel.textColor = ErrorColorForKind(result.error.kind);
            }
            [innerSelf->_incidentTableView reloadData];
            if (!innerSelf->_latestIncidents.empty()) {
                [innerSelf->_incidentTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0] byExtendingSelection:NO];
            } else {
                innerSelf->_incidentTextView.string =
                    ToNSString(DescribeInvestigationPayload("incident_list", "", result));
            }
        });
    });
}

- (void)openSelectedIncident:(id)sender {
    (void)sender;
    const NSInteger selected = _incidentTableView.selectedRow;
    if (selected < 0 || static_cast<std::size_t>(selected) >= _latestIncidents.size()) {
        _incidentStateLabel.stringValue = @"Select an incident row first.";
        _incidentStateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    const json& row = _latestIncidents.at(static_cast<std::size_t>(selected));
    const std::uint64_t logicalIncidentId = row.value("logical_incident_id", 0ULL);
    if (logicalIncidentId == 0) {
        _incidentStateLabel.stringValue = @"Selected incident row is missing logical_incident_id.";
        _incidentStateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    _incidentIdField.stringValue = UInt64String(logicalIncidentId);
    [self fetchIncident:nil];
}

- (void)fetchArtifact:(id)sender {
    (void)sender;
    if (_artifactInFlight || !_client) {
        return;
    }

    const std::string artifactId = TrimAscii(ToStdString(_artifactIdField.stringValue));
    if (artifactId.empty()) {
        _artifactStateLabel.stringValue = @"artifact_id is required.";
        _artifactStateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    const InvestigationPaneModel pane = [self artifactPaneModel];
    _artifactInFlight = YES;
    _artifactFetchButton.enabled = NO;
    _artifactExportButton.enabled = NO;
    [self beginInvestigationRequestForPane:pane message:@"Reading artifact…"];

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_pollQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->readArtifact(artifactId);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil) {
                return;
            }
            innerSelf->_artifactInFlight = NO;
            innerSelf->_artifactFetchButton.enabled = YES;
            innerSelf->_artifactExportButton.enabled = YES;
            [innerSelf applyInvestigationResult:result
                                         toPane:[innerSelf artifactPaneModel]
                                    successText:@"Artifact loaded."
                               syncArtifactField:NO];
            if (result.ok()) {
                [innerSelf recordRecentHistoryForPayloadKind:"artifact"
                                                    targetId:artifactId
                                                     payload:result.value
                                               fallbackTitle:"Artifact " + artifactId
                                              fallbackDetail:"Reopen the durable artifact envelope."
                                                    metadata:json{{"artifact_id", artifactId}}];
            }
            innerSelf->_artifactTextView.string =
                ToNSString(DescribeInvestigationPayload("artifact_read", artifactId, result));
        });
    });
}

- (void)exportArtifactPreview:(id)sender {
    (void)sender;
    if (_artifactInFlight || !_client) {
        return;
    }

    const std::string artifactId = TrimAscii(ToStdString(_artifactIdField.stringValue));
    if (artifactId.empty()) {
        _artifactStateLabel.stringValue = @"artifact_id is required.";
        _artifactStateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    const std::string format = ToStdString(_artifactExportFormatPopup.titleOfSelectedItem);

    _artifactInFlight = YES;
    _artifactFetchButton.enabled = NO;
    _artifactExportButton.enabled = NO;
    _artifactOpenSelectedEvidenceButton.enabled = NO;
    _artifactStateLabel.stringValue = @"Exporting artifact preview…";
    _artifactStateLabel.textColor = [NSColor systemOrangeColor];
    _artifactTextView.string = @"Generating export preview…";

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_pollQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->exportArtifact(artifactId, format);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil) {
                return;
            }
            innerSelf->_artifactInFlight = NO;
            innerSelf->_artifactFetchButton.enabled = YES;
            innerSelf->_artifactExportButton.enabled = YES;
            [innerSelf resetEvidenceItems:&innerSelf->_artifactEvidenceItems
                                 tableView:innerSelf->_artifactEvidenceTableView
                                openButton:innerSelf->_artifactOpenSelectedEvidenceButton];
            if (result.ok()) {
                innerSelf->_artifactStateLabel.stringValue = @"Artifact export preview ready.";
                innerSelf->_artifactStateLabel.textColor = [NSColor systemGreenColor];
            } else {
                innerSelf->_artifactStateLabel.stringValue = ToNSString(tapescope::QueryClient::describeError(result.error));
                innerSelf->_artifactStateLabel.textColor = ErrorColorForKind(result.error.kind);
            }
            innerSelf->_artifactTextView.string =
                ToNSString(DescribeArtifactExportResult(artifactId, format, result));
        });
    });
}

- (void)openSelectedArtifactEvidence:(id)sender {
    (void)sender;
    [self openSelectedEvidenceForPane:[self artifactPaneModel]];
}

- (void)refreshReportInventory:(id)sender {
    (void)sender;
    if (_reportInventoryInFlight || !_client) {
        return;
    }

    _reportInventoryInFlight = YES;
    _reportInventoryRefreshButton.enabled = NO;
    _reportInventoryOpenSessionButton.enabled = NO;
    _reportInventoryOpenCaseButton.enabled = NO;
    _reportInventoryStateLabel.stringValue = @"Refreshing report inventory…";
    _reportInventoryStateLabel.textColor = [NSColor systemOrangeColor];
    _reportInventoryTextView.string = @"Refreshing session and case report inventory…";

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_pollQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto sessionReports = strongSelf->_client->listSessionReports(20);
        const auto caseReports = strongSelf->_client->listCaseReports(20);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil) {
                return;
            }
            innerSelf->_reportInventoryInFlight = NO;
            innerSelf->_reportInventoryRefreshButton.enabled = YES;

            innerSelf->_latestSessionReports.clear();
            innerSelf->_latestCaseReports.clear();
            if (sessionReports.ok()) {
                const json rows = sessionReports.value.value("events", json::array());
                if (rows.is_array()) {
                    innerSelf->_latestSessionReports.assign(rows.begin(), rows.end());
                }
            }
            if (caseReports.ok()) {
                const json rows = caseReports.value.value("events", json::array());
                if (rows.is_array()) {
                    innerSelf->_latestCaseReports.assign(rows.begin(), rows.end());
                }
            }
            [innerSelf->_sessionReportTableView reloadData];
            [innerSelf->_caseReportTableView reloadData];
            if (!innerSelf->_latestSessionReports.empty()) {
                [innerSelf->_sessionReportTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0] byExtendingSelection:NO];
            }
            if (!innerSelf->_latestCaseReports.empty()) {
                [innerSelf->_caseReportTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0] byExtendingSelection:NO];
            }
            innerSelf->_reportInventoryOpenSessionButton.enabled =
                (innerSelf->_sessionReportTableView.selectedRow >= 0);
            innerSelf->_reportInventoryOpenCaseButton.enabled =
                (innerSelf->_caseReportTableView.selectedRow >= 0);

            if (sessionReports.ok() || caseReports.ok()) {
                if (!innerSelf->_latestSessionReports.empty() || !innerSelf->_latestCaseReports.empty()) {
                    innerSelf->_reportInventoryStateLabel.stringValue = @"Report inventory loaded.";
                    innerSelf->_reportInventoryStateLabel.textColor = [NSColor systemGreenColor];
                } else {
                    innerSelf->_reportInventoryStateLabel.stringValue = @"No durable reports are available yet.";
                    innerSelf->_reportInventoryStateLabel.textColor = TapeInkMutedColor();
                }
            } else {
                innerSelf->_reportInventoryStateLabel.stringValue = @"Report inventory queries failed.";
                innerSelf->_reportInventoryStateLabel.textColor = [NSColor systemRedColor];
            }
            if (innerSelf->_latestSessionReports.empty() && innerSelf->_latestCaseReports.empty()) {
                innerSelf->_reportInventoryTextView.string =
                    ToNSString(DescribeReportInventoryResult(sessionReports, caseReports));
            }
        });
    });
}

- (void)openSelectedSessionReport:(id)sender {
    (void)sender;
    const NSInteger selected = _sessionReportTableView.selectedRow;
    if (selected < 0 || static_cast<std::size_t>(selected) >= _latestSessionReports.size()) {
        _reportInventoryStateLabel.stringValue = @"Select a session report row first.";
        _reportInventoryStateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    const std::string artifactId = _latestSessionReports.at(static_cast<std::size_t>(selected)).value("artifact_id", std::string());
    if (artifactId.empty()) {
        _reportInventoryStateLabel.stringValue = @"Selected session report is missing an artifact id.";
        _reportInventoryStateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    _artifactIdField.stringValue = ToNSString(artifactId);
    if (_tabView != nil) {
        [_tabView selectTabViewItemWithIdentifier:@"ArtifactPane"];
    }
    [self fetchArtifact:nil];
}

- (void)openSelectedCaseReport:(id)sender {
    (void)sender;
    const NSInteger selected = _caseReportTableView.selectedRow;
    if (selected < 0 || static_cast<std::size_t>(selected) >= _latestCaseReports.size()) {
        _reportInventoryStateLabel.stringValue = @"Select a case report row first.";
        _reportInventoryStateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    const std::string artifactId = _latestCaseReports.at(static_cast<std::size_t>(selected)).value("artifact_id", std::string());
    if (artifactId.empty()) {
        _reportInventoryStateLabel.stringValue = @"Selected case report is missing an artifact id.";
        _reportInventoryStateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    _artifactIdField.stringValue = ToNSString(artifactId);
    if (_tabView != nil) {
        [_tabView selectTabViewItemWithIdentifier:@"ArtifactPane"];
    }
    [self fetchArtifact:nil];
}

- (NSInteger)numberOfRowsInTableView:(NSTableView*)tableView {
    if (tableView == _liveTableView) {
        return static_cast<NSInteger>(_liveEvents.size());
    }
    if (tableView == _recentTableView) {
        return static_cast<NSInteger>(_recentHistoryItems.size());
    }
    if (tableView == _overviewIncidentTableView) {
        return static_cast<NSInteger>(_overviewIncidents.size());
    }
    if (tableView == _overviewEvidenceTableView) {
        return static_cast<NSInteger>(_overviewEvidenceItems.size());
    }
    if (tableView == _rangeTableView) {
        return static_cast<NSInteger>(_rangeEvents.size());
    }
    if (tableView == _orderTableView) {
        return static_cast<NSInteger>(_orderEvents.size());
    }
    if (tableView == _orderCaseEvidenceTableView) {
        return static_cast<NSInteger>(_orderCaseEvidenceItems.size());
    }
    if (tableView == _incidentTableView) {
        return static_cast<NSInteger>(_latestIncidents.size());
    }
    if (tableView == _incidentEvidenceTableView) {
        return static_cast<NSInteger>(_incidentEvidenceItems.size());
    }
    if (tableView == _findingEvidenceTableView) {
        return static_cast<NSInteger>(_findingEvidenceItems.size());
    }
    if (tableView == _anchorEvidenceTableView) {
        return static_cast<NSInteger>(_anchorEvidenceItems.size());
    }
    if (tableView == _artifactEvidenceTableView) {
        return static_cast<NSInteger>(_artifactEvidenceItems.size());
    }
    if (tableView == _sessionReportTableView) {
        return static_cast<NSInteger>(_latestSessionReports.size());
    }
    if (tableView == _caseReportTableView) {
        return static_cast<NSInteger>(_latestCaseReports.size());
    }
    return 0;
}

- (NSView*)tableView:(NSTableView*)tableView
   viewForTableColumn:(NSTableColumn*)tableColumn
                  row:(NSInteger)row {
    if (tableColumn == nil || row < 0) {
        return nil;
    }

    const json* item = nullptr;
    if (tableView == _liveTableView) {
        if (static_cast<std::size_t>(row) >= _liveEvents.size()) {
            return nil;
        }
        item = &_liveEvents.at(static_cast<std::size_t>(row));
    } else if (tableView == _recentTableView) {
        if (static_cast<std::size_t>(row) >= _recentHistoryItems.size()) {
            return nil;
        }
        item = &_recentHistoryItems.at(static_cast<std::size_t>(row));
    } else if (tableView == _overviewIncidentTableView) {
        if (static_cast<std::size_t>(row) >= _overviewIncidents.size()) {
            return nil;
        }
        item = &_overviewIncidents.at(static_cast<std::size_t>(row));
    } else if (tableView == _overviewEvidenceTableView) {
        if (static_cast<std::size_t>(row) >= _overviewEvidenceItems.size()) {
            return nil;
        }
        item = &_overviewEvidenceItems.at(static_cast<std::size_t>(row));
    } else if (tableView == _rangeTableView) {
        if (static_cast<std::size_t>(row) >= _rangeEvents.size()) {
            return nil;
        }
        item = &_rangeEvents.at(static_cast<std::size_t>(row));
    } else if (tableView == _orderTableView) {
        if (static_cast<std::size_t>(row) >= _orderEvents.size()) {
            return nil;
        }
        item = &_orderEvents.at(static_cast<std::size_t>(row));
    } else if (tableView == _orderCaseEvidenceTableView) {
        if (static_cast<std::size_t>(row) >= _orderCaseEvidenceItems.size()) {
            return nil;
        }
        item = &_orderCaseEvidenceItems.at(static_cast<std::size_t>(row));
    } else if (tableView == _incidentTableView) {
        if (static_cast<std::size_t>(row) >= _latestIncidents.size()) {
            return nil;
        }
        item = &_latestIncidents.at(static_cast<std::size_t>(row));
    } else if (tableView == _incidentEvidenceTableView) {
        if (static_cast<std::size_t>(row) >= _incidentEvidenceItems.size()) {
            return nil;
        }
        item = &_incidentEvidenceItems.at(static_cast<std::size_t>(row));
    } else if (tableView == _findingEvidenceTableView) {
        if (static_cast<std::size_t>(row) >= _findingEvidenceItems.size()) {
            return nil;
        }
        item = &_findingEvidenceItems.at(static_cast<std::size_t>(row));
    } else if (tableView == _anchorEvidenceTableView) {
        if (static_cast<std::size_t>(row) >= _anchorEvidenceItems.size()) {
            return nil;
        }
        item = &_anchorEvidenceItems.at(static_cast<std::size_t>(row));
    } else if (tableView == _artifactEvidenceTableView) {
        if (static_cast<std::size_t>(row) >= _artifactEvidenceItems.size()) {
            return nil;
        }
        item = &_artifactEvidenceItems.at(static_cast<std::size_t>(row));
    } else if (tableView == _sessionReportTableView) {
        if (static_cast<std::size_t>(row) >= _latestSessionReports.size()) {
            return nil;
        }
        item = &_latestSessionReports.at(static_cast<std::size_t>(row));
    } else if (tableView == _caseReportTableView) {
        if (static_cast<std::size_t>(row) >= _latestCaseReports.size()) {
            return nil;
        }
        item = &_latestCaseReports.at(static_cast<std::size_t>(row));
    } else {
        return nil;
    }

    NSString* columnId = tableColumn.identifier ?: @"";
    const bool mono = [columnId containsString:@"id"] || [columnId containsString:@"score"] || [columnId containsString:@"revision"];
    NSTableCellView* cell = MakeOrReuseTableCell(tableView,
                                                 columnId,
                                                 mono ? [NSFont monospacedSystemFontOfSize:11.5 weight:NSFontWeightRegular]
                                                      : [NSFont systemFontOfSize:12.0 weight:NSFontWeightRegular]);

    std::string value;
    if (tableView == _liveTableView) {
        if ([columnId isEqualToString:@"session_seq"]) {
            value = std::to_string(item->value("session_seq", 0ULL));
        } else if ([columnId isEqualToString:@"source_seq"]) {
            value = std::to_string(item->value("source_seq", 0ULL));
        } else if ([columnId isEqualToString:@"event_kind"]) {
            value = item->value("event_kind", std::string());
        } else {
            value = EventSummaryText(*item);
        }
    } else if (tableView == _recentTableView) {
        if ([columnId isEqualToString:@"kind"]) {
            value = item->value("kind", std::string());
        } else if ([columnId isEqualToString:@"target_id"]) {
            value = item->value("target_id", std::string());
        } else {
            value = item->value("headline", item->value("detail", std::string()));
        }
    } else if (tableView == _overviewIncidentTableView) {
        if ([columnId isEqualToString:@"logical_incident_id"]) {
            value = std::to_string(item->value("logical_incident_id", 0ULL));
        } else if ([columnId isEqualToString:@"kind"]) {
            value = item->value("kind", std::string());
        } else if ([columnId isEqualToString:@"score"]) {
            std::ostringstream out;
            out.setf(std::ios::fixed);
            out.precision(2);
            out << item->value("score", 0.0);
            value = out.str();
        } else {
            value = item->value("title", item->value("why_it_matters", item->value("kind", std::string())));
        }
    } else if (tableView == _rangeTableView) {
        if ([columnId isEqualToString:@"session_seq"]) {
            value = std::to_string(item->value("session_seq", 0ULL));
        } else if ([columnId isEqualToString:@"source_seq"]) {
            value = std::to_string(item->value("source_seq", 0ULL));
        } else if ([columnId isEqualToString:@"event_kind"]) {
            value = item->value("event_kind", std::string());
        } else {
            value = EventSummaryText(*item);
        }
    } else if (tableView == _orderTableView) {
        if ([columnId isEqualToString:@"session_seq"]) {
            value = std::to_string(item->value("session_seq", 0ULL));
        } else if ([columnId isEqualToString:@"event_kind"]) {
            value = item->value("event_kind", std::string());
        } else if ([columnId isEqualToString:@"side"]) {
            value = item->value("side", std::string());
        } else {
            value = EventSummaryText(*item);
        }
    } else if (tableView == _orderCaseEvidenceTableView || tableView == _artifactEvidenceTableView) {
        if ([columnId isEqualToString:@"kind"]) {
            value = item->value("kind", item->value("type", std::string()));
        } else if ([columnId isEqualToString:@"artifact_id"]) {
            value = item->value("artifact_id", std::string());
        } else {
            value = item->value("label", item->value("headline", item->value("artifact_id", std::string())));
        }
    } else if (tableView == _overviewEvidenceTableView || tableView == _incidentEvidenceTableView ||
               tableView == _findingEvidenceTableView || tableView == _anchorEvidenceTableView) {
        if ([columnId isEqualToString:@"kind"]) {
            value = item->value("kind", item->value("type", std::string()));
        } else if ([columnId isEqualToString:@"artifact_id"]) {
            value = item->value("artifact_id", std::string());
        } else {
            value = item->value("label", item->value("headline", item->value("artifact_id", std::string())));
        }
    } else if (tableView == _incidentTableView) {
        if ([columnId isEqualToString:@"logical_incident_id"]) {
            value = std::to_string(item->value("logical_incident_id", 0ULL));
        } else if ([columnId isEqualToString:@"kind"]) {
            value = item->value("kind", std::string());
        } else if ([columnId isEqualToString:@"score"]) {
            std::ostringstream out;
            out.setf(std::ios::fixed);
            out.precision(2);
            out << item->value("score", 0.0);
            value = out.str();
        } else {
            value = item->value("why_it_matters", item->value("kind", std::string()));
        }
    } else if (tableView == _sessionReportTableView) {
        if ([columnId isEqualToString:@"report_id"]) {
            value = std::to_string(item->value("report_id", 0ULL));
        } else if ([columnId isEqualToString:@"revision_id"]) {
            value = std::to_string(item->value("revision_id", 0ULL));
        } else if ([columnId isEqualToString:@"artifact_id"]) {
            value = item->value("artifact_id", std::string());
        } else {
            value = item->value("headline", std::string());
        }
    } else if (tableView == _caseReportTableView) {
        if ([columnId isEqualToString:@"report_id"]) {
            value = std::to_string(item->value("report_id", 0ULL));
        } else if ([columnId isEqualToString:@"report_type"]) {
            value = item->value("report_type", std::string());
        } else if ([columnId isEqualToString:@"artifact_id"]) {
            value = item->value("artifact_id", std::string());
        } else {
            value = item->value("headline", std::string());
        }
    }

    cell.textField.stringValue = ToNSString(value);
    return cell;
}

- (void)tableViewSelectionDidChange:(NSNotification*)notification {
    NSTableView* tableView = notification.object;
    if (tableView == _liveTableView) {
        const NSInteger selected = _liveTableView.selectedRow;
        if (selected < 0 || static_cast<std::size_t>(selected) >= _liveEvents.size()) {
            _liveTextView.string = @"Select a live event row to inspect the decoded payload.";
            return;
        }
        const json& item = _liveEvents.at(static_cast<std::size_t>(selected));
        _liveTextView.string = ToNSString(item.dump(2));
        return;
    }

    if (tableView == _recentTableView) {
        const NSInteger selected = _recentTableView.selectedRow;
        _recentOpenButton.enabled = (selected >= 0);
        if (selected < 0 || static_cast<std::size_t>(selected) >= _recentHistoryItems.size()) {
            _recentTextView.string = @"Select a recent-history row to inspect its summary and reopen it.";
            return;
        }
        const json& item = _recentHistoryItems.at(static_cast<std::size_t>(selected));
        _recentTextView.string = ToNSString(DescribeRecentHistoryEntry(item));
        return;
    }

    if (tableView == _overviewIncidentTableView) {
        const NSInteger selected = _overviewIncidentTableView.selectedRow;
        _overviewOpenSelectedIncidentButton.enabled = (selected >= 0);
        if (selected < 0 || static_cast<std::size_t>(selected) >= _overviewIncidents.size()) {
            return;
        }
        const json& item = _overviewIncidents.at(static_cast<std::size_t>(selected));
        _overviewTextView.string = ToNSString(item.dump(2));
        return;
    }

    if (tableView == _rangeTableView) {
        const NSInteger selected = _rangeTableView.selectedRow;
        if (selected < 0 || static_cast<std::size_t>(selected) >= _rangeEvents.size()) {
            _rangeTextView.string = @"Select a replay event row to inspect the decoded payload.";
            return;
        }
        const json& item = _rangeEvents.at(static_cast<std::size_t>(selected));
        _rangeTextView.string = ToNSString(item.dump(2));
        return;
    }

    if (tableView == _orderTableView) {
        const NSInteger selected = _orderTableView.selectedRow;
        if (selected < 0 || static_cast<std::size_t>(selected) >= _orderEvents.size()) {
            _orderTextView.string = @"Select an anchored event row to inspect the decoded payload.";
            return;
        }
        const json& item = _orderEvents.at(static_cast<std::size_t>(selected));
        _orderTextView.string = ToNSString(item.dump(2));
        return;
    }

    if (tableView == _incidentTableView) {
        const NSInteger selected = _incidentTableView.selectedRow;
        _incidentOpenSelectedButton.enabled = (selected >= 0);
        if (selected < 0 || static_cast<std::size_t>(selected) >= _latestIncidents.size()) {
            _incidentTextView.string = @"Select an incident row to inspect its summary or open the full drilldown.";
            return;
        }
        const json& item = _latestIncidents.at(static_cast<std::size_t>(selected));
        _incidentIdField.stringValue = UInt64String(item.value("logical_incident_id", 0ULL));
        _incidentTextView.string = ToNSString(item.dump(2));
        return;
    }

    InvestigationPaneModel pane;
    if ([self getInvestigationPaneModelForEvidenceTable:tableView outModel:&pane]) {
        [self renderSelectionForPane:pane];
        return;
    }

    if (tableView == _sessionReportTableView) {
        const NSInteger selected = _sessionReportTableView.selectedRow;
        _reportInventoryOpenSessionButton.enabled = (selected >= 0);
        if (selected < 0 || static_cast<std::size_t>(selected) >= _latestSessionReports.size()) {
            return;
        }
        const json& item = _latestSessionReports.at(static_cast<std::size_t>(selected));
        _reportInventoryTextView.string = ToNSString(item.dump(2));
        return;
    }

    if (tableView == _caseReportTableView) {
        const NSInteger selected = _caseReportTableView.selectedRow;
        _reportInventoryOpenCaseButton.enabled = (selected >= 0);
        if (selected < 0 || static_cast<std::size_t>(selected) >= _latestCaseReports.size()) {
            return;
        }
        const json& item = _latestCaseReports.at(static_cast<std::size_t>(selected));
        _reportInventoryTextView.string = ToNSString(item.dump(2));
    }
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
