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

namespace {

using tapescope::json;

constexpr NSTimeInterval kPollIntervalSeconds = 2.0;
constexpr std::size_t kLiveTailLimit = 32;

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

NSScrollView* MakeScrollView(NSTextView* textView, CGFloat minHeight) {
    NSScrollView* scrollView = [[NSScrollView alloc] initWithFrame:NSMakeRect(0, 0, 800, minHeight)];
    scrollView.hasVerticalScroller = YES;
    scrollView.hasHorizontalScroller = YES;
    scrollView.borderType = NSBezelBorder;
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
    return artifact.value("artifact_id", std::string());
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

enum class OrderAnchorType {
    TraceId = 0,
    OrderId = 1,
    PermId = 2,
    ExecId = 3
};

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

} // namespace

@interface TapeScopeWindowController : NSWindowController <NSTableViewDataSource, NSTableViewDelegate> {
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
    NSTabView* _tabView;

    NSTextView* _statusTextView;
    NSTextView* _liveTextView;
    NSTableView* _liveTableView;
    std::vector<json> _liveEvents;

    NSTextField* _overviewFirstField;
    NSTextField* _overviewLastField;
    NSButton* _overviewFetchButton;
    NSButton* _overviewScanButton;
    NSButton* _overviewOpenSelectedIncidentButton;
    NSTextField* _overviewStateLabel;
    NSTableView* _overviewIncidentTableView;
    NSTextView* _overviewTextView;
    BOOL _overviewInFlight;
    tapescope::RangeQuery _lastOverviewQuery;
    std::vector<json> _overviewIncidents;

    NSTextField* _rangeFirstField;
    NSTextField* _rangeLastField;
    NSButton* _rangeFetchButton;
    NSTextField* _rangeStateLabel;
    NSTableView* _rangeTableView;
    NSTextView* _rangeTextView;
    BOOL _rangeInFlight;
    tapescope::RangeQuery _lastRangeQuery;
    std::vector<json> _rangeEvents;

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
    NSButton* _incidentOpenSelectedButton;
    NSTextField* _incidentStateLabel;
    NSTableView* _incidentTableView;
    NSTextView* _incidentTextView;
    BOOL _incidentInFlight;
    std::vector<json> _latestIncidents;

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
    window.contentView.wantsLayer = YES;
    window.contentView.layer.backgroundColor = [NSColor colorWithCalibratedRed:0.96 green:0.95 blue:0.92 alpha:1.0].CGColor;

    tapescope::ClientConfig config;
    config.socketPath = tapescope::defaultSocketPath();
    _client = std::make_unique<tapescope::QueryClient>(std::move(config));
    _pollQueue = dispatch_queue_create("com.foxy.tapescope.poll", DISPATCH_QUEUE_SERIAL);
    _lastOverviewQuery.firstSessionSeq = 1;
    _lastOverviewQuery.lastSessionSeq = 128;
    _lastRangeQuery.firstSessionSeq = 1;
    _lastRangeQuery.lastSessionSeq = 128;

    [self buildInterface];
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
    NSView* pane = [[NSView alloc] initWithFrame:NSZeroRect];
    NSStackView* stack = MakeColumnStack(10.0);
    [pane addSubview:stack];
    [NSLayoutConstraint activateConstraints:@[
        [stack.leadingAnchor constraintEqualToAnchor:pane.leadingAnchor constant:10.0],
        [stack.trailingAnchor constraintEqualToAnchor:pane.trailingAnchor constant:-10.0],
        [stack.topAnchor constraintEqualToAnchor:pane.topAnchor constant:10.0],
        [stack.bottomAnchor constraintEqualToAnchor:pane.bottomAnchor constant:-10.0]
    ]];

    NSTextField* intro = MakeLabel(@"Live tape: recent mutable-tail events from tape_engine with row-level drilldown.",
                                   [NSFont systemFontOfSize:12.5 weight:NSFontWeightMedium],
                                   [NSColor secondaryLabelColor]);
    [stack addArrangedSubview:intro];

    _liveTableView = [[NSTableView alloc] initWithFrame:NSZeroRect];
    _liveTableView.usesAlternatingRowBackgroundColors = YES;
    _liveTableView.allowsEmptySelection = YES;
    _liveTableView.allowsMultipleSelection = NO;
    _liveTableView.delegate = self;
    _liveTableView.dataSource = self;
    auto addLiveColumn = ^(NSString* identifier, NSString* title, CGFloat width) {
        NSTableColumn* column = [[NSTableColumn alloc] initWithIdentifier:identifier];
        column.title = title;
        column.width = width;
        column.minWidth = 90.0;
        [_liveTableView addTableColumn:column];
    };
    addLiveColumn(@"session_seq", @"session_seq", 120.0);
    addLiveColumn(@"source_seq", @"source_seq", 120.0);
    addLiveColumn(@"event_kind", @"event_kind", 180.0);
    addLiveColumn(@"summary", @"summary", 460.0);
    NSScrollView* liveTableScroll = [[NSScrollView alloc] initWithFrame:NSMakeRect(0, 0, 900, 180)];
    liveTableScroll.hasVerticalScroller = YES;
    liveTableScroll.hasHorizontalScroller = YES;
    liveTableScroll.borderType = NSBezelBorder;
    liveTableScroll.autoresizingMask = NSViewWidthSizable | NSViewHeightSizable;
    liveTableScroll.documentView = _liveTableView;
    [liveTableScroll.heightAnchor constraintGreaterThanOrEqualToConstant:170.0].active = YES;
    [stack addArrangedSubview:liveTableScroll];

    NSTextField* detailLabel = MakeLabel(@"Selected Live Event",
                                         [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                         [NSColor secondaryLabelColor]);
    [stack addArrangedSubview:detailLabel];

    _liveTextView = MakeReadOnlyTextView();
    _liveTextView.string = @"Waiting for the first live-tail response…";
    [stack addArrangedSubview:MakeScrollView(_liveTextView, 240.0)];

    NSTabViewItem* item = [[NSTabViewItem alloc] initWithIdentifier:@"LiveEventsPane"];
    item.label = @"LiveEventsPane";
    item.view = pane;
    return item;
}

- (NSTabViewItem*)overviewTabItem {
    NSView* pane = [[NSView alloc] initWithFrame:NSZeroRect];
    NSStackView* stack = MakeColumnStack(10.0);
    [pane addSubview:stack];
    [NSLayoutConstraint activateConstraints:@[
        [stack.leadingAnchor constraintEqualToAnchor:pane.leadingAnchor constant:10.0],
        [stack.trailingAnchor constraintEqualToAnchor:pane.trailingAnchor constant:-10.0],
        [stack.topAnchor constraintEqualToAnchor:pane.topAnchor constant:10.0],
        [stack.bottomAnchor constraintEqualToAnchor:pane.bottomAnchor constant:-10.0]
    ]];

    NSTextField* intro = MakeLabel(@"Session overview: summarize the major incidents and evidence for a frozen session_seq window.",
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
    _overviewFirstField = [[NSTextField alloc] initWithFrame:NSMakeRect(0, 0, 130, 24)];
    _overviewFirstField.font = [NSFont monospacedSystemFontOfSize:12.0 weight:NSFontWeightMedium];
    _overviewFirstField.stringValue = UInt64String(_lastOverviewQuery.firstSessionSeq);
    [controls addArrangedSubview:_overviewFirstField];

    [controls addArrangedSubview:MakeLabel(@"last_session_seq",
                                           [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                           [NSColor secondaryLabelColor])];
    _overviewLastField = [[NSTextField alloc] initWithFrame:NSMakeRect(0, 0, 130, 24)];
    _overviewLastField.font = [NSFont monospacedSystemFontOfSize:12.0 weight:NSFontWeightMedium];
    _overviewLastField.stringValue = UInt64String(_lastOverviewQuery.lastSessionSeq);
    [controls addArrangedSubview:_overviewLastField];

    _overviewFetchButton = [NSButton buttonWithTitle:@"Read Overview"
                                              target:self
                                              action:@selector(fetchOverview:)];
    [controls addArrangedSubview:_overviewFetchButton];
    _overviewScanButton = [NSButton buttonWithTitle:@"Scan Report"
                                             target:self
                                             action:@selector(scanOverviewReport:)];
    [controls addArrangedSubview:_overviewScanButton];
    _overviewOpenSelectedIncidentButton = [NSButton buttonWithTitle:@"Open Selected Incident"
                                                             target:self
                                                             action:@selector(openSelectedOverviewIncident:)];
    _overviewOpenSelectedIncidentButton.enabled = NO;
    [controls addArrangedSubview:_overviewOpenSelectedIncidentButton];
    [stack addArrangedSubview:controls];

    _overviewStateLabel = MakeLabel(@"No session overview loaded yet.",
                                    [NSFont systemFontOfSize:12.0 weight:NSFontWeightMedium],
                                    [NSColor secondaryLabelColor]);
    [stack addArrangedSubview:_overviewStateLabel];

    NSTextField* overviewSummaryLabel = MakeLabel(@"Overview Summary",
                                                  [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                                  [NSColor secondaryLabelColor]);
    [stack addArrangedSubview:overviewSummaryLabel];

    _overviewTextView = MakeReadOnlyTextView();
    _overviewTextView.string = @"Read a session overview to inspect ranked incidents and evidence.";
    [stack addArrangedSubview:MakeScrollView(_overviewTextView, 170.0)];

    NSTextField* incidentListLabel = MakeLabel(@"Top Incidents",
                                               [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                               [NSColor secondaryLabelColor]);
    [stack addArrangedSubview:incidentListLabel];

    _overviewIncidentTableView = [[NSTableView alloc] initWithFrame:NSZeroRect];
    _overviewIncidentTableView.usesAlternatingRowBackgroundColors = YES;
    _overviewIncidentTableView.allowsEmptySelection = YES;
    _overviewIncidentTableView.allowsMultipleSelection = NO;
    _overviewIncidentTableView.delegate = self;
    _overviewIncidentTableView.dataSource = self;
    auto addOverviewIncidentColumn = ^(NSString* identifier, NSString* title, CGFloat width) {
        NSTableColumn* column = [[NSTableColumn alloc] initWithIdentifier:identifier];
        column.title = title;
        column.width = width;
        column.minWidth = 90.0;
        [_overviewIncidentTableView addTableColumn:column];
    };
    addOverviewIncidentColumn(@"logical_incident_id", @"logical_incident_id", 150.0);
    addOverviewIncidentColumn(@"kind", @"kind", 180.0);
    addOverviewIncidentColumn(@"score", @"score", 100.0);
    addOverviewIncidentColumn(@"summary", @"summary", 430.0);
    NSScrollView* incidentTableScroll = [[NSScrollView alloc] initWithFrame:NSMakeRect(0, 0, 900, 170)];
    incidentTableScroll.hasVerticalScroller = YES;
    incidentTableScroll.hasHorizontalScroller = YES;
    incidentTableScroll.borderType = NSBezelBorder;
    incidentTableScroll.autoresizingMask = NSViewWidthSizable | NSViewHeightSizable;
    incidentTableScroll.documentView = _overviewIncidentTableView;
    [incidentTableScroll.heightAnchor constraintGreaterThanOrEqualToConstant:160.0].active = YES;
    [stack addArrangedSubview:incidentTableScroll];

    NSTabViewItem* item = [[NSTabViewItem alloc] initWithIdentifier:@"SessionOverviewPane"];
    item.label = @"SessionOverviewPane";
    item.view = pane;
    return item;
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

    NSTextField* intro = MakeLabel(@"Replay window: fetch a frozen session_seq range from tape_engine.",
                                   [NSFont systemFontOfSize:12.5 weight:NSFontWeightMedium],
                                   [NSColor secondaryLabelColor]);
    [stack addArrangedSubview:intro];

    NSStackView* controls = [[NSStackView alloc] initWithFrame:NSZeroRect];
    controls.orientation = NSUserInterfaceLayoutOrientationHorizontal;
    controls.alignment = NSLayoutAttributeCenterY;
    controls.spacing = 8.0;
    controls.translatesAutoresizingMaskIntoConstraints = NO;
    [controls addArrangedSubview:MakeLabel(@"first_session_seq",
                                           [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                           [NSColor secondaryLabelColor])];
    _rangeFirstField = [[NSTextField alloc] initWithFrame:NSMakeRect(0, 0, 130, 24)];
    _rangeFirstField.font = [NSFont monospacedSystemFontOfSize:12.0 weight:NSFontWeightMedium];
    _rangeFirstField.stringValue = UInt64String(_lastRangeQuery.firstSessionSeq);
    [controls addArrangedSubview:_rangeFirstField];

    [controls addArrangedSubview:MakeLabel(@"last_session_seq",
                                           [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                           [NSColor secondaryLabelColor])];
    _rangeLastField = [[NSTextField alloc] initWithFrame:NSMakeRect(0, 0, 130, 24)];
    _rangeLastField.font = [NSFont monospacedSystemFontOfSize:12.0 weight:NSFontWeightMedium];
    _rangeLastField.stringValue = UInt64String(_lastRangeQuery.lastSessionSeq);
    [controls addArrangedSubview:_rangeLastField];

    _rangeFetchButton = [NSButton buttonWithTitle:@"Fetch Range" target:self action:@selector(fetchRange:)];
    [controls addArrangedSubview:_rangeFetchButton];
    [stack addArrangedSubview:controls];

    _rangeStateLabel = MakeLabel(@"No replay window loaded yet.",
                                 [NSFont systemFontOfSize:12.0 weight:NSFontWeightMedium],
                                 [NSColor secondaryLabelColor]);
    [stack addArrangedSubview:_rangeStateLabel];

    _rangeTableView = [[NSTableView alloc] initWithFrame:NSZeroRect];
    _rangeTableView.usesAlternatingRowBackgroundColors = YES;
    _rangeTableView.allowsEmptySelection = YES;
    _rangeTableView.allowsMultipleSelection = NO;
    _rangeTableView.delegate = self;
    _rangeTableView.dataSource = self;
    auto addRangeColumn = ^(NSString* identifier, NSString* title, CGFloat width) {
        NSTableColumn* column = [[NSTableColumn alloc] initWithIdentifier:identifier];
        column.title = title;
        column.width = width;
        column.minWidth = 90.0;
        [_rangeTableView addTableColumn:column];
    };
    addRangeColumn(@"session_seq", @"session_seq", 120.0);
    addRangeColumn(@"source_seq", @"source_seq", 120.0);
    addRangeColumn(@"event_kind", @"event_kind", 180.0);
    addRangeColumn(@"summary", @"summary", 460.0);
    NSScrollView* rangeTableScroll = [[NSScrollView alloc] initWithFrame:NSMakeRect(0, 0, 900, 180)];
    rangeTableScroll.hasVerticalScroller = YES;
    rangeTableScroll.hasHorizontalScroller = YES;
    rangeTableScroll.borderType = NSBezelBorder;
    rangeTableScroll.autoresizingMask = NSViewWidthSizable | NSViewHeightSizable;
    rangeTableScroll.documentView = _rangeTableView;
    [rangeTableScroll.heightAnchor constraintGreaterThanOrEqualToConstant:170.0].active = YES;
    [stack addArrangedSubview:rangeTableScroll];

    NSTextField* rangeDetailLabel = MakeLabel(@"Decoded Event",
                                              [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                              [NSColor secondaryLabelColor]);
    [stack addArrangedSubview:rangeDetailLabel];

    _rangeTextView = MakeReadOnlyTextView();
    _rangeTextView.string = @"Fetch a replay window, then select a row to inspect the decoded event payload.";
    [stack addArrangedSubview:MakeScrollView(_rangeTextView, 220.0)];

    NSTabViewItem* item = [[NSTabViewItem alloc] initWithIdentifier:@"RangePane"];
    item.label = @"RangePane";
    item.view = pane;
    return item;
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

    NSTextField* intro = MakeLabel(@"Order anchor lookup: query anchored lifecycle context by trace/order/perm/exec id.",
                                   [NSFont systemFontOfSize:12.5 weight:NSFontWeightMedium],
                                   [NSColor secondaryLabelColor]);
    [stack addArrangedSubview:intro];

    NSStackView* controls = [[NSStackView alloc] initWithFrame:NSZeroRect];
    controls.orientation = NSUserInterfaceLayoutOrientationHorizontal;
    controls.alignment = NSLayoutAttributeCenterY;
    controls.spacing = 8.0;
    controls.translatesAutoresizingMaskIntoConstraints = NO;

    _orderAnchorTypePopup = [[NSPopUpButton alloc] initWithFrame:NSMakeRect(0, 0, 120, 24) pullsDown:NO];
    [_orderAnchorTypePopup addItemsWithTitles:@[@"traceId", @"orderId", @"permId", @"execId"]];
    _orderAnchorTypePopup.target = self;
    _orderAnchorTypePopup.action = @selector(orderAnchorTypeChanged:);
    [controls addArrangedSubview:_orderAnchorTypePopup];

    _orderAnchorInputField = [[NSTextField alloc] initWithFrame:NSMakeRect(0, 0, 220, 24)];
    _orderAnchorInputField.font = [NSFont monospacedSystemFontOfSize:12.0 weight:NSFontWeightMedium];
    _orderAnchorInputField.placeholderString = PlaceholderForOrderAnchorType(OrderAnchorType::TraceId);
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

    _orderTableView = [[NSTableView alloc] initWithFrame:NSZeroRect];
    _orderTableView.usesAlternatingRowBackgroundColors = YES;
    _orderTableView.allowsEmptySelection = YES;
    _orderTableView.allowsMultipleSelection = NO;
    _orderTableView.delegate = self;
    _orderTableView.dataSource = self;
    auto addOrderColumn = ^(NSString* identifier, NSString* title, CGFloat width) {
        NSTableColumn* column = [[NSTableColumn alloc] initWithIdentifier:identifier];
        column.title = title;
        column.width = width;
        column.minWidth = 90.0;
        [_orderTableView addTableColumn:column];
    };
    addOrderColumn(@"session_seq", @"session_seq", 120.0);
    addOrderColumn(@"event_kind", @"event_kind", 180.0);
    addOrderColumn(@"side", @"side", 100.0);
    addOrderColumn(@"summary", @"summary", 460.0);
    NSScrollView* orderTableScroll = [[NSScrollView alloc] initWithFrame:NSMakeRect(0, 0, 900, 180)];
    orderTableScroll.hasVerticalScroller = YES;
    orderTableScroll.hasHorizontalScroller = YES;
    orderTableScroll.borderType = NSBezelBorder;
    orderTableScroll.autoresizingMask = NSViewWidthSizable | NSViewHeightSizable;
    orderTableScroll.documentView = _orderTableView;
    [orderTableScroll.heightAnchor constraintGreaterThanOrEqualToConstant:170.0].active = YES;
    [stack addArrangedSubview:orderTableScroll];

    NSTextField* orderDetailLabel = MakeLabel(@"Anchored Event",
                                              [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                              [NSColor secondaryLabelColor]);
    [stack addArrangedSubview:orderDetailLabel];

    _orderTextView = MakeReadOnlyTextView();
    _orderTextView.string = @"Lookup an anchor, then select a row to inspect the decoded event payload.";
    [stack addArrangedSubview:MakeScrollView(_orderTextView, 220.0)];

    NSTabViewItem* item = [[NSTabViewItem alloc] initWithIdentifier:@"OrderLookupPane"];
    item.label = @"OrderLookupPane";
    item.view = pane;
    return item;
}

- (NSTabViewItem*)orderCaseTabItem {
    NSView* pane = [[NSView alloc] initWithFrame:NSZeroRect];
    NSStackView* stack = MakeColumnStack(10.0);
    [pane addSubview:stack];
    [NSLayoutConstraint activateConstraints:@[
        [stack.leadingAnchor constraintEqualToAnchor:pane.leadingAnchor constant:10.0],
        [stack.trailingAnchor constraintEqualToAnchor:pane.trailingAnchor constant:-10.0],
        [stack.topAnchor constraintEqualToAnchor:pane.topAnchor constant:10.0],
        [stack.bottomAnchor constraintEqualToAnchor:pane.bottomAnchor constant:-10.0]
    ]];

    NSTextField* intro = MakeLabel(@"Order case: load the report-style investigation summary for one trace/order/perm/exec anchor.",
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

    _orderCaseAnchorTypePopup = [[NSPopUpButton alloc] initWithFrame:NSMakeRect(0, 0, 120, 24) pullsDown:NO];
    [_orderCaseAnchorTypePopup addItemsWithTitles:@[@"traceId", @"orderId", @"permId", @"execId"]];
    _orderCaseAnchorTypePopup.target = self;
    _orderCaseAnchorTypePopup.action = @selector(orderCaseAnchorTypeChanged:);
    [controls addArrangedSubview:_orderCaseAnchorTypePopup];

    _orderCaseAnchorInputField = [[NSTextField alloc] initWithFrame:NSMakeRect(0, 0, 220, 24)];
    _orderCaseAnchorInputField.font = [NSFont monospacedSystemFontOfSize:12.0 weight:NSFontWeightMedium];
    _orderCaseAnchorInputField.placeholderString = PlaceholderForOrderAnchorType(OrderAnchorType::TraceId);
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

    NSStackView* actions = [[NSStackView alloc] initWithFrame:NSZeroRect];
    actions.orientation = NSUserInterfaceLayoutOrientationHorizontal;
    actions.alignment = NSLayoutAttributeCenterY;
    actions.spacing = 8.0;
    actions.translatesAutoresizingMaskIntoConstraints = NO;
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

    NSTextField* orderCaseDetailLabel = MakeLabel(@"Order Case Summary",
                                                  [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                                  [NSColor secondaryLabelColor]);
    [stack addArrangedSubview:orderCaseDetailLabel];

    _orderCaseTextView = MakeReadOnlyTextView();
    _orderCaseTextView.string = @"Read an order case to inspect replay targets, findings, incidents, and evidence.";
    [stack addArrangedSubview:MakeScrollView(_orderCaseTextView, 170.0)];

    NSTextField* orderCaseEvidenceLabel = MakeLabel(@"Evidence Citations",
                                                    [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                                    [NSColor secondaryLabelColor]);
    [stack addArrangedSubview:orderCaseEvidenceLabel];

    _orderCaseEvidenceTableView = [[NSTableView alloc] initWithFrame:NSZeroRect];
    _orderCaseEvidenceTableView.usesAlternatingRowBackgroundColors = YES;
    _orderCaseEvidenceTableView.allowsEmptySelection = YES;
    _orderCaseEvidenceTableView.allowsMultipleSelection = NO;
    _orderCaseEvidenceTableView.delegate = self;
    _orderCaseEvidenceTableView.dataSource = self;
    auto addOrderCaseEvidenceColumn = ^(NSString* identifier, NSString* title, CGFloat width) {
        NSTableColumn* column = [[NSTableColumn alloc] initWithIdentifier:identifier];
        column.title = title;
        column.width = width;
        column.minWidth = 90.0;
        [_orderCaseEvidenceTableView addTableColumn:column];
    };
    addOrderCaseEvidenceColumn(@"kind", @"kind", 130.0);
    addOrderCaseEvidenceColumn(@"artifact_id", @"artifact_id", 260.0);
    addOrderCaseEvidenceColumn(@"label", @"label", 360.0);
    NSScrollView* orderCaseEvidenceScroll = [[NSScrollView alloc] initWithFrame:NSMakeRect(0, 0, 900, 170)];
    orderCaseEvidenceScroll.hasVerticalScroller = YES;
    orderCaseEvidenceScroll.hasHorizontalScroller = YES;
    orderCaseEvidenceScroll.borderType = NSBezelBorder;
    orderCaseEvidenceScroll.autoresizingMask = NSViewWidthSizable | NSViewHeightSizable;
    orderCaseEvidenceScroll.documentView = _orderCaseEvidenceTableView;
    [orderCaseEvidenceScroll.heightAnchor constraintGreaterThanOrEqualToConstant:160.0].active = YES;
    [stack addArrangedSubview:orderCaseEvidenceScroll];

    NSTabViewItem* item = [[NSTabViewItem alloc] initWithIdentifier:@"OrderCasePane"];
    item.label = @"OrderCasePane";
    item.view = pane;
    return item;
}

- (NSTabViewItem*)seekTabItem {
    NSView* pane = [[NSView alloc] initWithFrame:NSZeroRect];
    NSStackView* stack = MakeColumnStack(10.0);
    [pane addSubview:stack];
    [NSLayoutConstraint activateConstraints:@[
        [stack.leadingAnchor constraintEqualToAnchor:pane.leadingAnchor constant:10.0],
        [stack.trailingAnchor constraintEqualToAnchor:pane.trailingAnchor constant:-10.0],
        [stack.topAnchor constraintEqualToAnchor:pane.topAnchor constant:10.0],
        [stack.bottomAnchor constraintEqualToAnchor:pane.bottomAnchor constant:-10.0]
    ]];

    NSTextField* intro = MakeLabel(@"Replay target: compute the best replay jump around an order/fill anchor.",
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

    _seekAnchorTypePopup = [[NSPopUpButton alloc] initWithFrame:NSMakeRect(0, 0, 120, 24) pullsDown:NO];
    [_seekAnchorTypePopup addItemsWithTitles:@[@"traceId", @"orderId", @"permId", @"execId"]];
    _seekAnchorTypePopup.target = self;
    _seekAnchorTypePopup.action = @selector(seekAnchorTypeChanged:);
    [controls addArrangedSubview:_seekAnchorTypePopup];

    _seekAnchorInputField = [[NSTextField alloc] initWithFrame:NSMakeRect(0, 0, 220, 24)];
    _seekAnchorInputField.font = [NSFont monospacedSystemFontOfSize:12.0 weight:NSFontWeightMedium];
    _seekAnchorInputField.placeholderString = PlaceholderForOrderAnchorType(OrderAnchorType::TraceId);
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
    NSView* pane = [[NSView alloc] initWithFrame:NSZeroRect];
    NSStackView* stack = MakeColumnStack(10.0);
    [pane addSubview:stack];
    [NSLayoutConstraint activateConstraints:@[
        [stack.leadingAnchor constraintEqualToAnchor:pane.leadingAnchor constant:10.0],
        [stack.trailingAnchor constraintEqualToAnchor:pane.trailingAnchor constant:-10.0],
        [stack.topAnchor constraintEqualToAnchor:pane.topAnchor constant:10.0],
        [stack.bottomAnchor constraintEqualToAnchor:pane.bottomAnchor constant:-10.0]
    ]];

    NSTextField* intro = MakeLabel(@"Incident drilldown: reopen a ranked logical incident by stable logical_incident_id.",
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
    [controls addArrangedSubview:MakeLabel(@"logical_incident_id",
                                           [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                           [NSColor secondaryLabelColor])];
    _incidentIdField = [[NSTextField alloc] initWithFrame:NSMakeRect(0, 0, 180, 24)];
    _incidentIdField.font = [NSFont monospacedSystemFontOfSize:12.0 weight:NSFontWeightMedium];
    _incidentIdField.placeholderString = @"e.g. 1";
    [controls addArrangedSubview:_incidentIdField];

    _incidentRefreshButton = [NSButton buttonWithTitle:@"Refresh List"
                                                target:self
                                                action:@selector(refreshIncidentList:)];
    [controls addArrangedSubview:_incidentRefreshButton];

    _incidentFetchButton = [NSButton buttonWithTitle:@"Read Incident"
                                              target:self
                                              action:@selector(fetchIncident:)];
    [controls addArrangedSubview:_incidentFetchButton];

    _incidentOpenSelectedButton = [NSButton buttonWithTitle:@"Open Selected"
                                                     target:self
                                                     action:@selector(openSelectedIncident:)];
    _incidentOpenSelectedButton.enabled = NO;
    [controls addArrangedSubview:_incidentOpenSelectedButton];
    [stack addArrangedSubview:controls];

    _incidentStateLabel = MakeLabel(@"No incident loaded yet.",
                                    [NSFont systemFontOfSize:12.0 weight:NSFontWeightMedium],
                                    [NSColor secondaryLabelColor]);
    [stack addArrangedSubview:_incidentStateLabel];

    _incidentTableView = [[NSTableView alloc] initWithFrame:NSZeroRect];
    _incidentTableView.usesAlternatingRowBackgroundColors = YES;
    _incidentTableView.allowsEmptySelection = YES;
    _incidentTableView.allowsMultipleSelection = NO;
    _incidentTableView.delegate = self;
    _incidentTableView.dataSource = self;
    auto addIncidentColumn = ^(NSString* identifier, NSString* title, CGFloat width) {
        NSTableColumn* column = [[NSTableColumn alloc] initWithIdentifier:identifier];
        column.title = title;
        column.width = width;
        column.minWidth = 90.0;
        [_incidentTableView addTableColumn:column];
    };
    addIncidentColumn(@"logical_incident_id", @"logical_incident_id", 150.0);
    addIncidentColumn(@"kind", @"kind", 180.0);
    addIncidentColumn(@"score", @"score", 90.0);
    addIncidentColumn(@"headline", @"headline", 420.0);
    NSScrollView* incidentScroll = [[NSScrollView alloc] initWithFrame:NSMakeRect(0, 0, 900, 180)];
    incidentScroll.hasVerticalScroller = YES;
    incidentScroll.hasHorizontalScroller = YES;
    incidentScroll.borderType = NSBezelBorder;
    incidentScroll.autoresizingMask = NSViewWidthSizable | NSViewHeightSizable;
    incidentScroll.documentView = _incidentTableView;
    [incidentScroll.heightAnchor constraintGreaterThanOrEqualToConstant:170.0].active = YES;
    [stack addArrangedSubview:incidentScroll];

    _incidentTextView = MakeReadOnlyTextView();
    _incidentTextView.string = @"Incident drilldown will show score breakdown, findings, protected windows, and narrative summary.";
    [stack addArrangedSubview:MakeScrollView(_incidentTextView, 320.0)];

    NSTabViewItem* item = [[NSTabViewItem alloc] initWithIdentifier:@"IncidentPane"];
    item.label = @"IncidentPane";
    item.view = pane;
    return item;
}

- (NSTabViewItem*)artifactTabItem {
    NSView* pane = [[NSView alloc] initWithFrame:NSZeroRect];
    NSStackView* stack = MakeColumnStack(10.0);
    [pane addSubview:stack];
    [NSLayoutConstraint activateConstraints:@[
        [stack.leadingAnchor constraintEqualToAnchor:pane.leadingAnchor constant:10.0],
        [stack.trailingAnchor constraintEqualToAnchor:pane.trailingAnchor constant:-10.0],
        [stack.topAnchor constraintEqualToAnchor:pane.topAnchor constant:10.0],
        [stack.bottomAnchor constraintEqualToAnchor:pane.bottomAnchor constant:-10.0]
    ]];

    NSTextField* intro = MakeLabel(@"Artifact read: reopen durable reports or selector artifacts by stable artifact id.",
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
    [controls addArrangedSubview:MakeLabel(@"artifact_id",
                                           [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                           [NSColor secondaryLabelColor])];
    _artifactIdField = [[NSTextField alloc] initWithFrame:NSMakeRect(0, 0, 340, 24)];
    _artifactIdField.font = [NSFont monospacedSystemFontOfSize:12.0 weight:NSFontWeightMedium];
    _artifactIdField.placeholderString = @"e.g. session-report:1 or order-case:order:7401";
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

    NSTextField* artifactEvidenceLabel = MakeLabel(@"Evidence Citations",
                                                   [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                                   [NSColor secondaryLabelColor]);
    [stack addArrangedSubview:artifactEvidenceLabel];

    _artifactEvidenceTableView = [[NSTableView alloc] initWithFrame:NSZeroRect];
    _artifactEvidenceTableView.usesAlternatingRowBackgroundColors = YES;
    _artifactEvidenceTableView.allowsEmptySelection = YES;
    _artifactEvidenceTableView.allowsMultipleSelection = NO;
    _artifactEvidenceTableView.delegate = self;
    _artifactEvidenceTableView.dataSource = self;
    auto addArtifactEvidenceColumn = ^(NSString* identifier, NSString* title, CGFloat width) {
        NSTableColumn* column = [[NSTableColumn alloc] initWithIdentifier:identifier];
        column.title = title;
        column.width = width;
        column.minWidth = 90.0;
        [_artifactEvidenceTableView addTableColumn:column];
    };
    addArtifactEvidenceColumn(@"kind", @"kind", 130.0);
    addArtifactEvidenceColumn(@"artifact_id", @"artifact_id", 260.0);
    addArtifactEvidenceColumn(@"label", @"label", 360.0);
    NSScrollView* artifactEvidenceScroll = [[NSScrollView alloc] initWithFrame:NSMakeRect(0, 0, 900, 170)];
    artifactEvidenceScroll.hasVerticalScroller = YES;
    artifactEvidenceScroll.hasHorizontalScroller = YES;
    artifactEvidenceScroll.borderType = NSBezelBorder;
    artifactEvidenceScroll.autoresizingMask = NSViewWidthSizable | NSViewHeightSizable;
    artifactEvidenceScroll.documentView = _artifactEvidenceTableView;
    [artifactEvidenceScroll.heightAnchor constraintGreaterThanOrEqualToConstant:160.0].active = YES;
    [stack addArrangedSubview:artifactEvidenceScroll];

    NSTextField* artifactDetailLabel = MakeLabel(@"Artifact Detail",
                                                 [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                                 [NSColor secondaryLabelColor]);
    [stack addArrangedSubview:artifactDetailLabel];

    _artifactTextView = MakeReadOnlyTextView();
    _artifactTextView.string = @"Artifact reads will show the normalized investigation envelope for durable reports and selector artifacts.";
    [stack addArrangedSubview:MakeScrollView(_artifactTextView, 180.0)];

    NSTabViewItem* item = [[NSTabViewItem alloc] initWithIdentifier:@"ArtifactPane"];
    item.label = @"ArtifactPane";
    item.view = pane;
    return item;
}

- (NSTabViewItem*)reportInventoryTabItem {
    NSView* pane = [[NSView alloc] initWithFrame:NSZeroRect];
    NSStackView* stack = MakeColumnStack(10.0);
    [pane addSubview:stack];
    [NSLayoutConstraint activateConstraints:@[
        [stack.leadingAnchor constraintEqualToAnchor:pane.leadingAnchor constant:10.0],
        [stack.trailingAnchor constraintEqualToAnchor:pane.trailingAnchor constant:-10.0],
        [stack.topAnchor constraintEqualToAnchor:pane.topAnchor constant:10.0],
        [stack.bottomAnchor constraintEqualToAnchor:pane.bottomAnchor constant:-10.0]
    ]];

    NSTextField* intro = MakeLabel(@"Durable reports: browse session and case report artifacts already persisted by tape_engine.",
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

    NSTextField* sessionLabel = MakeLabel(@"Session Reports",
                                          [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                          [NSColor secondaryLabelColor]);
    [stack addArrangedSubview:sessionLabel];

    _sessionReportTableView = [[NSTableView alloc] initWithFrame:NSZeroRect];
    _sessionReportTableView.usesAlternatingRowBackgroundColors = YES;
    _sessionReportTableView.allowsEmptySelection = YES;
    _sessionReportTableView.allowsMultipleSelection = NO;
    _sessionReportTableView.delegate = self;
    _sessionReportTableView.dataSource = self;
    auto addSessionColumn = ^(NSString* identifier, NSString* title, CGFloat width) {
        NSTableColumn* column = [[NSTableColumn alloc] initWithIdentifier:identifier];
        column.title = title;
        column.width = width;
        column.minWidth = 90.0;
        [_sessionReportTableView addTableColumn:column];
    };
    addSessionColumn(@"report_id", @"report_id", 110.0);
    addSessionColumn(@"revision_id", @"revision_id", 110.0);
    addSessionColumn(@"artifact_id", @"artifact_id", 220.0);
    addSessionColumn(@"headline", @"headline", 360.0);
    NSScrollView* sessionScroll = [[NSScrollView alloc] initWithFrame:NSMakeRect(0, 0, 900, 150)];
    sessionScroll.hasVerticalScroller = YES;
    sessionScroll.hasHorizontalScroller = YES;
    sessionScroll.borderType = NSBezelBorder;
    sessionScroll.autoresizingMask = NSViewWidthSizable | NSViewHeightSizable;
    sessionScroll.documentView = _sessionReportTableView;
    [sessionScroll.heightAnchor constraintGreaterThanOrEqualToConstant:140.0].active = YES;
    [stack addArrangedSubview:sessionScroll];

    NSTextField* caseLabel = MakeLabel(@"Case Reports",
                                       [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                       [NSColor secondaryLabelColor]);
    [stack addArrangedSubview:caseLabel];

    _caseReportTableView = [[NSTableView alloc] initWithFrame:NSZeroRect];
    _caseReportTableView.usesAlternatingRowBackgroundColors = YES;
    _caseReportTableView.allowsEmptySelection = YES;
    _caseReportTableView.allowsMultipleSelection = NO;
    _caseReportTableView.delegate = self;
    _caseReportTableView.dataSource = self;
    auto addCaseColumn = ^(NSString* identifier, NSString* title, CGFloat width) {
        NSTableColumn* column = [[NSTableColumn alloc] initWithIdentifier:identifier];
        column.title = title;
        column.width = width;
        column.minWidth = 90.0;
        [_caseReportTableView addTableColumn:column];
    };
    addCaseColumn(@"report_id", @"report_id", 110.0);
    addCaseColumn(@"report_type", @"report_type", 150.0);
    addCaseColumn(@"artifact_id", @"artifact_id", 220.0);
    addCaseColumn(@"headline", @"headline", 320.0);
    NSScrollView* caseScroll = [[NSScrollView alloc] initWithFrame:NSMakeRect(0, 0, 900, 150)];
    caseScroll.hasVerticalScroller = YES;
    caseScroll.hasHorizontalScroller = YES;
    caseScroll.borderType = NSBezelBorder;
    caseScroll.autoresizingMask = NSViewWidthSizable | NSViewHeightSizable;
    caseScroll.documentView = _caseReportTableView;
    [caseScroll.heightAnchor constraintGreaterThanOrEqualToConstant:140.0].active = YES;
    [stack addArrangedSubview:caseScroll];

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

    NSTextField* subtitle = MakeLabel(@"Phase 4: native status, live-tail, overview, incident, replay-target, range, order case, report inventory, and artifact/export panes backed by the engine query seam.",
                                      [NSFont systemFontOfSize:13.0 weight:NSFontWeightMedium],
                                      [NSColor secondaryLabelColor]);
    subtitle.lineBreakMode = NSLineBreakByWordWrapping;
    subtitle.maximumNumberOfLines = 2;
    [root addArrangedSubview:subtitle];

    _bannerLabel = MakeLabel(@"Waiting for tape_engine",
                             [NSFont systemFontOfSize:14.0 weight:NSFontWeightSemibold],
                             [NSColor secondaryLabelColor]);
    [root addArrangedSubview:_bannerLabel];

    _pollMetaLabel = MakeLabel(@"Polling engine every 2s",
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

    _tabView = [[NSTabView alloc] initWithFrame:NSZeroRect];
    _tabView.translatesAutoresizingMaskIntoConstraints = NO;
    [_tabView addTabViewItem:[self textTabItemWithLabel:@"StatusPane" textView:&_statusTextView]];
    [_tabView addTabViewItem:[self liveEventsTabItem]];
    [_tabView addTabViewItem:[self overviewTabItem]];
    [_tabView addTabViewItem:[self incidentTabItem]];
    [_tabView addTabViewItem:[self seekTabItem]];
    [_tabView addTabViewItem:[self rangeTabItem]];
    [_tabView addTabViewItem:[self orderTabItem]];
    [_tabView addTabViewItem:[self orderCaseTabItem]];
    [_tabView addTabViewItem:[self reportInventoryTabItem]];
    [_tabView addTabViewItem:[self artifactTabItem]];
    [_tabView.heightAnchor constraintGreaterThanOrEqualToConstant:520.0].active = YES;
    [root addArrangedSubview:_tabView];

    _statusTextView.string = @"Waiting for the first status response…";
    _liveTextView.string = @"Waiting for the first live-tail response…";
}

- (void)showWindowAndStart {
    [self showWindow:nil];
    [self.window makeKeyAndOrderFront:nil];
    [NSApp activateIgnoringOtherApps:YES];
    [self startPolling];
    [self refreshIncidentList:nil];
    [self refreshReportInventory:nil];
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
    if (probe.status.ok()) {
        _bannerLabel.stringValue = @"Connected to tape_engine";
        _bannerLabel.textColor = [NSColor systemGreenColor];
        _socketValue.stringValue = ToNSString(probe.status.value.socketPath);
        _dataDirValue.stringValue = ToNSString(probe.status.value.dataDir);
        _instrumentValue.stringValue = ToNSString(probe.status.value.instrumentId);
        _latestSeqValue.stringValue = UInt64String(probe.status.value.latestSessionSeq);
        _liveCountValue.stringValue = UInt64String(probe.status.value.liveEventCount);
        _segmentCountValue.stringValue = UInt64String(probe.status.value.segmentCount);
        _manifestHashValue.stringValue = ToNSString(probe.status.value.manifestHash);
    } else {
        _bannerLabel.stringValue = ToNSString(tapescope::QueryClient::describeError(probe.status.error));
        _bannerLabel.textColor = ErrorColorForKind(probe.status.error.kind);
        _socketValue.stringValue = ToNSString(_client ? _client->config().socketPath : tapescope::defaultSocketPath());
        _dataDirValue.stringValue = @"--";
        _instrumentValue.stringValue = @"--";
        _latestSeqValue.stringValue = @"--";
        _liveCountValue.stringValue = @"--";
        _segmentCountValue.stringValue = @"--";
        _manifestHashValue.stringValue = @"--";
    }

    _pollMetaLabel.stringValue = [NSString stringWithFormat:@"Mutable/live refresh every %.1fs", kPollIntervalSeconds];
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
                innerSelf->_rangeStateLabel.stringValue = @"Replay window loaded.";
                innerSelf->_rangeStateLabel.textColor = [NSColor systemGreenColor];
                innerSelf->_rangeEvents = result.value;
                [innerSelf->_rangeTableView reloadData];
                if (!innerSelf->_rangeEvents.empty()) {
                    [innerSelf->_rangeTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0] byExtendingSelection:NO];
                } else {
                    innerSelf->_rangeTextView.string = @"No decoded events are available for the requested replay window.";
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

    _overviewInFlight = YES;
    _overviewFetchButton.enabled = NO;
    _overviewScanButton.enabled = NO;
    _overviewStateLabel.stringValue = @"Reading session overview…";
    _overviewStateLabel.textColor = [NSColor systemOrangeColor];
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
                innerSelf->_overviewStateLabel.stringValue = @"Session overview loaded.";
                innerSelf->_overviewStateLabel.textColor = [NSColor systemGreenColor];
                const json incidents = result.value.value("events", json::array());
                innerSelf->_overviewIncidents.clear();
                if (incidents.is_array()) {
                    innerSelf->_overviewIncidents.assign(incidents.begin(), incidents.end());
                }
                [innerSelf->_overviewIncidentTableView reloadData];
                innerSelf->_overviewOpenSelectedIncidentButton.enabled = !innerSelf->_overviewIncidents.empty();
                if (!innerSelf->_overviewIncidents.empty()) {
                    [innerSelf->_overviewIncidentTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0]
                                                         byExtendingSelection:NO];
                }
            } else {
                innerSelf->_overviewStateLabel.stringValue = ToNSString(tapescope::QueryClient::describeError(result.error));
                innerSelf->_overviewStateLabel.textColor = ErrorColorForKind(result.error.kind);
                innerSelf->_overviewIncidents.clear();
                [innerSelf->_overviewIncidentTableView reloadData];
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

    _overviewInFlight = YES;
    _overviewFetchButton.enabled = NO;
    _overviewScanButton.enabled = NO;
    _overviewStateLabel.stringValue = @"Scanning durable session report…";
    _overviewStateLabel.textColor = [NSColor systemOrangeColor];
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
                innerSelf->_overviewStateLabel.stringValue = @"Durable session report scanned.";
                innerSelf->_overviewStateLabel.textColor = [NSColor systemGreenColor];
                const json incidents = result.value.value("events", json::array());
                innerSelf->_overviewIncidents.clear();
                if (incidents.is_array()) {
                    innerSelf->_overviewIncidents.assign(incidents.begin(), incidents.end());
                }
                [innerSelf->_overviewIncidentTableView reloadData];
                innerSelf->_overviewOpenSelectedIncidentButton.enabled = !innerSelf->_overviewIncidents.empty();
                if (!innerSelf->_overviewIncidents.empty()) {
                    [innerSelf->_overviewIncidentTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0]
                                                         byExtendingSelection:NO];
                }
                const std::string artifactId = ExtractArtifactId(result.value);
                if (!artifactId.empty() && innerSelf->_artifactIdField != nil) {
                    innerSelf->_artifactIdField.stringValue = ToNSString(artifactId);
                }
                [innerSelf refreshReportInventory:nil];
            } else {
                innerSelf->_overviewStateLabel.stringValue = ToNSString(tapescope::QueryClient::describeError(result.error));
                innerSelf->_overviewStateLabel.textColor = ErrorColorForKind(result.error.kind);
                innerSelf->_overviewIncidents.clear();
                [innerSelf->_overviewIncidentTableView reloadData];
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
                innerSelf->_orderStateLabel.stringValue = @"Order lookup complete.";
                innerSelf->_orderStateLabel.textColor = [NSColor systemGreenColor];
                const json events = result.value.value("events", json::array());
                innerSelf->_orderEvents.clear();
                if (events.is_array()) {
                    innerSelf->_orderEvents.assign(events.begin(), events.end());
                }
                [innerSelf->_orderTableView reloadData];
                if (!innerSelf->_orderEvents.empty()) {
                    [innerSelf->_orderTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0] byExtendingSelection:NO];
                } else {
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

    _orderCaseInFlight = YES;
    _orderCaseFetchButton.enabled = NO;
    _orderCaseScanButton.enabled = NO;
    _orderCaseLoadReplayButton.enabled = NO;
    _orderCaseOpenSelectedEvidenceButton.enabled = NO;
    _hasOrderCaseReplayRange = NO;
    _orderCaseStateLabel.stringValue = @"Reading order case…";
    _orderCaseStateLabel.textColor = [NSColor systemOrangeColor];

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
            if (result.ok()) {
                innerSelf->_orderCaseStateLabel.stringValue = @"Order case loaded.";
                innerSelf->_orderCaseStateLabel.textColor = [NSColor systemGreenColor];
                const json citations = ExtractEvidenceCitations(result.value);
                innerSelf->_orderCaseEvidenceItems.clear();
                if (citations.is_array()) {
                    innerSelf->_orderCaseEvidenceItems.assign(citations.begin(), citations.end());
                }
                [innerSelf->_orderCaseEvidenceTableView reloadData];
                innerSelf->_hasOrderCaseReplayRange =
                    ReplayRangeFromSeekSummary(result.value.value("summary", json::object()),
                                              &innerSelf->_orderCaseReplayRange);
                innerSelf->_orderCaseLoadReplayButton.enabled = innerSelf->_hasOrderCaseReplayRange;
                innerSelf->_orderCaseOpenSelectedEvidenceButton.enabled = !innerSelf->_orderCaseEvidenceItems.empty();
                const std::string artifactId = ExtractArtifactId(result.value);
                if (!artifactId.empty() && innerSelf->_artifactIdField != nil) {
                    innerSelf->_artifactIdField.stringValue = ToNSString(artifactId);
                }
                if (!innerSelf->_orderCaseEvidenceItems.empty()) {
                    [innerSelf->_orderCaseEvidenceTableView
                        selectRowIndexes:[NSIndexSet indexSetWithIndex:0]
                  byExtendingSelection:NO];
                }
            } else {
                innerSelf->_orderCaseStateLabel.stringValue = ToNSString(tapescope::QueryClient::describeError(result.error));
                innerSelf->_orderCaseStateLabel.textColor = ErrorColorForKind(result.error.kind);
                innerSelf->_hasOrderCaseReplayRange = NO;
                innerSelf->_orderCaseLoadReplayButton.enabled = NO;
                innerSelf->_orderCaseEvidenceItems.clear();
                [innerSelf->_orderCaseEvidenceTableView reloadData];
                innerSelf->_orderCaseOpenSelectedEvidenceButton.enabled = NO;
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

    _orderCaseInFlight = YES;
    _orderCaseFetchButton.enabled = NO;
    _orderCaseScanButton.enabled = NO;
    _orderCaseLoadReplayButton.enabled = NO;
    _orderCaseOpenSelectedEvidenceButton.enabled = NO;
    _hasOrderCaseReplayRange = NO;
    _orderCaseStateLabel.stringValue = @"Scanning durable order-case report…";
    _orderCaseStateLabel.textColor = [NSColor systemOrangeColor];

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
            if (result.ok()) {
                innerSelf->_orderCaseStateLabel.stringValue = @"Durable order-case report scanned.";
                innerSelf->_orderCaseStateLabel.textColor = [NSColor systemGreenColor];
                const json citations = ExtractEvidenceCitations(result.value);
                innerSelf->_orderCaseEvidenceItems.clear();
                if (citations.is_array()) {
                    innerSelf->_orderCaseEvidenceItems.assign(citations.begin(), citations.end());
                }
                [innerSelf->_orderCaseEvidenceTableView reloadData];
                innerSelf->_hasOrderCaseReplayRange =
                    ReplayRangeFromSeekSummary(result.value.value("summary", json::object()),
                                              &innerSelf->_orderCaseReplayRange);
                innerSelf->_orderCaseLoadReplayButton.enabled = innerSelf->_hasOrderCaseReplayRange;
                innerSelf->_orderCaseOpenSelectedEvidenceButton.enabled = !innerSelf->_orderCaseEvidenceItems.empty();
                const std::string artifactId = ExtractArtifactId(result.value);
                if (!artifactId.empty() && innerSelf->_artifactIdField != nil) {
                    innerSelf->_artifactIdField.stringValue = ToNSString(artifactId);
                }
                [innerSelf refreshReportInventory:nil];
                if (!innerSelf->_orderCaseEvidenceItems.empty()) {
                    [innerSelf->_orderCaseEvidenceTableView
                        selectRowIndexes:[NSIndexSet indexSetWithIndex:0]
                  byExtendingSelection:NO];
                }
            } else {
                innerSelf->_orderCaseStateLabel.stringValue = ToNSString(tapescope::QueryClient::describeError(result.error));
                innerSelf->_orderCaseStateLabel.textColor = ErrorColorForKind(result.error.kind);
                innerSelf->_hasOrderCaseReplayRange = NO;
                innerSelf->_orderCaseLoadReplayButton.enabled = NO;
                innerSelf->_orderCaseEvidenceItems.clear();
                [innerSelf->_orderCaseEvidenceTableView reloadData];
                innerSelf->_orderCaseOpenSelectedEvidenceButton.enabled = NO;
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
    if (!_hasOrderCaseReplayRange) {
        _orderCaseStateLabel.stringValue = @"No order-case replay window is ready yet.";
        _orderCaseStateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    _rangeFirstField.stringValue = UInt64String(_orderCaseReplayRange.firstSessionSeq);
    _rangeLastField.stringValue = UInt64String(_orderCaseReplayRange.lastSessionSeq);
    if (_tabView != nil) {
        [_tabView selectTabViewItemWithIdentifier:@"RangePane"];
    }
    [self fetchRange:nil];
}

- (void)openSelectedOrderCaseEvidence:(id)sender {
    (void)sender;
    const NSInteger selected = _orderCaseEvidenceTableView.selectedRow;
    if (selected < 0 || static_cast<std::size_t>(selected) >= _orderCaseEvidenceItems.size()) {
        _orderCaseStateLabel.stringValue = @"Select an order-case evidence row first.";
        _orderCaseStateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    const json& citation = _orderCaseEvidenceItems.at(static_cast<std::size_t>(selected));
    const std::string artifactId = citation.value("artifact_id", std::string());
    if (artifactId.empty()) {
        _orderCaseStateLabel.stringValue = @"Selected order-case evidence is missing artifact_id.";
        _orderCaseStateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    _artifactIdField.stringValue = ToNSString(artifactId);
    if (_tabView != nil) {
        [_tabView selectTabViewItemWithIdentifier:@"ArtifactPane"];
    }
    [self fetchArtifact:nil];
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

    _incidentInFlight = YES;
    _incidentFetchButton.enabled = NO;
    _incidentRefreshButton.enabled = NO;
    _incidentOpenSelectedButton.enabled = NO;
    _incidentStateLabel.stringValue = @"Reading incident drilldown…";
    _incidentStateLabel.textColor = [NSColor systemOrangeColor];

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
            if (result.ok()) {
                innerSelf->_incidentStateLabel.stringValue = @"Incident loaded.";
                innerSelf->_incidentStateLabel.textColor = [NSColor systemGreenColor];
                const std::string artifactId = ExtractArtifactId(result.value);
                if (!artifactId.empty() && innerSelf->_artifactIdField != nil) {
                    innerSelf->_artifactIdField.stringValue = ToNSString(artifactId);
                }
            } else {
                innerSelf->_incidentStateLabel.stringValue = ToNSString(tapescope::QueryClient::describeError(result.error));
                innerSelf->_incidentStateLabel.textColor = ErrorColorForKind(result.error.kind);
            }
            innerSelf->_incidentTextView.string =
                ToNSString(DescribeInvestigationPayload("incident", "logical_incident_id=" + std::to_string(logicalIncidentId), result));
        });
    });
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
                innerSelf->_incidentStateLabel.stringValue = @"Incident list loaded.";
                innerSelf->_incidentStateLabel.textColor = [NSColor systemGreenColor];
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

    _artifactInFlight = YES;
    _artifactFetchButton.enabled = NO;
    _artifactExportButton.enabled = NO;
    _artifactOpenSelectedEvidenceButton.enabled = NO;
    _artifactStateLabel.stringValue = @"Reading artifact…";
    _artifactStateLabel.textColor = [NSColor systemOrangeColor];

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
            if (result.ok()) {
                innerSelf->_artifactStateLabel.stringValue = @"Artifact loaded.";
                innerSelf->_artifactStateLabel.textColor = [NSColor systemGreenColor];
                const json citations = ExtractEvidenceCitations(result.value);
                innerSelf->_artifactEvidenceItems.clear();
                if (citations.is_array()) {
                    innerSelf->_artifactEvidenceItems.assign(citations.begin(), citations.end());
                }
                [innerSelf->_artifactEvidenceTableView reloadData];
                innerSelf->_artifactOpenSelectedEvidenceButton.enabled = !innerSelf->_artifactEvidenceItems.empty();
                if (!innerSelf->_artifactEvidenceItems.empty()) {
                    [innerSelf->_artifactEvidenceTableView
                        selectRowIndexes:[NSIndexSet indexSetWithIndex:0]
                  byExtendingSelection:NO];
                }
            } else {
                innerSelf->_artifactStateLabel.stringValue = ToNSString(tapescope::QueryClient::describeError(result.error));
                innerSelf->_artifactStateLabel.textColor = ErrorColorForKind(result.error.kind);
                innerSelf->_artifactEvidenceItems.clear();
                [innerSelf->_artifactEvidenceTableView reloadData];
                innerSelf->_artifactOpenSelectedEvidenceButton.enabled = NO;
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
            innerSelf->_artifactEvidenceItems.clear();
            [innerSelf->_artifactEvidenceTableView reloadData];
            innerSelf->_artifactOpenSelectedEvidenceButton.enabled = NO;
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
    const NSInteger selected = _artifactEvidenceTableView.selectedRow;
    if (selected < 0 || static_cast<std::size_t>(selected) >= _artifactEvidenceItems.size()) {
        _artifactStateLabel.stringValue = @"Select an artifact evidence row first.";
        _artifactStateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    const json& citation = _artifactEvidenceItems.at(static_cast<std::size_t>(selected));
    const std::string artifactId = citation.value("artifact_id", std::string());
    if (artifactId.empty()) {
        _artifactStateLabel.stringValue = @"Selected artifact evidence is missing artifact_id.";
        _artifactStateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    _artifactIdField.stringValue = ToNSString(artifactId);
    [self fetchArtifact:nil];
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
                innerSelf->_reportInventoryStateLabel.stringValue = @"Report inventory loaded.";
                innerSelf->_reportInventoryStateLabel.textColor = [NSColor systemGreenColor];
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
    if (tableView == _overviewIncidentTableView) {
        return static_cast<NSInteger>(_overviewIncidents.size());
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
    } else if (tableView == _overviewIncidentTableView) {
        if (static_cast<std::size_t>(row) >= _overviewIncidents.size()) {
            return nil;
        }
        item = &_overviewIncidents.at(static_cast<std::size_t>(row));
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

    if (tableView == _orderCaseEvidenceTableView) {
        const NSInteger selected = _orderCaseEvidenceTableView.selectedRow;
        _orderCaseOpenSelectedEvidenceButton.enabled = (selected >= 0);
        if (selected < 0 || static_cast<std::size_t>(selected) >= _orderCaseEvidenceItems.size()) {
            _orderCaseTextView.string = @"Read an order case, then select a citation row to inspect or reopen evidence.";
            return;
        }
        const json& item = _orderCaseEvidenceItems.at(static_cast<std::size_t>(selected));
        _orderCaseTextView.string = ToNSString(item.dump(2));
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

    if (tableView == _artifactEvidenceTableView) {
        const NSInteger selected = _artifactEvidenceTableView.selectedRow;
        _artifactOpenSelectedEvidenceButton.enabled = (selected >= 0);
        if (selected < 0 || static_cast<std::size_t>(selected) >= _artifactEvidenceItems.size()) {
            _artifactTextView.string = @"Read an artifact, then select a citation row to inspect or reopen evidence.";
            return;
        }
        const json& item = _artifactEvidenceItems.at(static_cast<std::size_t>(selected));
        _artifactTextView.string = ToNSString(item.dump(2));
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
