#pragma once

#import <AppKit/AppKit.h>

#include "tapescope_client.h"

#include <cstdint>
#include <initializer_list>
#include <optional>
#include <string>
#include <vector>

@interface ActionTableView : NSTableView
@property(nonatomic, assign) id primaryActionTarget;
@property(nonatomic) SEL primaryAction;
@end

namespace tapescope_support {

using tapescope::json;

NSColor* TapeBackgroundColor();
NSColor* TapeCardColor();
NSColor* TapeCardBorderColor();
NSColor* TapePanelFillColor();
NSColor* TapePanelBorderColor();
NSColor* TapeInkMutedColor();

NSString* ToNSString(const std::string& value);
std::string ToStdString(NSString* value);
std::string TrimAscii(std::string value);
bool ParsePositiveUInt64(const std::string& raw, std::uint64_t* parsed);
bool ParsePositiveInt64(const std::string& raw, long long* parsed);
NSString* UInt64String(std::uint64_t value);

NSTextField* MakeLabel(NSString* text, NSFont* font, NSColor* color);
NSTextField* MakeValueLabel();
NSTextView* MakeReadOnlyTextView();
NSScrollView* MakeScrollView(NSTextView* textView, CGFloat minHeight);
NSStackView* MakeColumnStack(CGFloat spacing);
NSBox* MakeCardBox(CGFloat cornerRadius = 18.0);
struct ViewWithStack {
    NSView* view;
    NSStackView* stack;
};
struct CardWithStack {
    NSBox* box;
    NSStackView* stack;
};
ViewWithStack MakePaneWithStack();
CardWithStack MakeCardWithStack(CGFloat spacing = 10.0);
NSTextField* MakeIntroLabel(NSString* text, NSInteger lines = 1);
NSTextField* MakeSectionLabel(NSString* text);
NSStackView* MakeControlRow();
NSTextField* MakeMonospacedField(CGFloat width,
                                 NSString* initialValue = nil,
                                 NSString* placeholder = nil);
NSTableView* MakeStandardTableView(id delegate, id dataSource);
void ConfigureTablePrimaryAction(NSTableView* tableView, id target, SEL action);
void AddTableColumn(NSTableView* tableView,
                    NSString* identifier,
                    NSString* title,
                    CGFloat width);
NSScrollView* MakeTableScrollView(NSTableView* tableView, CGFloat minHeight);
NSTableView* MakeEvidenceTableView(id delegate, id dataSource);
NSTableCellView* MakeOrReuseTableCell(NSTableView* tableView,
                                      NSString* identifier,
                                      NSFont* font);

NSColor* ErrorColorForKind(tapescope::QueryErrorKind kind);

template <typename T>
std::string DescribeQueryResult(const tapescope::QueryResult<T>& result) {
    if (result.ok()) {
        return {};
    }
    return tapescope::QueryClient::describeError(result.error);
}

std::string FirstPresentString(const json& payload,
                               std::initializer_list<const char*> keys);
std::uint64_t FirstPresentUInt64(const json& payload,
                                 std::initializer_list<const char*> keys);
std::string DescribeRecentHistoryEntry(const json& entry);
std::string DescribeStatusPane(const tapescope::QueryResult<tapescope::StatusSnapshot>& result,
                               const std::string& configuredSocketPath);
std::string DescribeLiveEventsPane(const tapescope::QueryResult<std::vector<json>>& result);
std::string DescribeLiveEventsPane(const tapescope::QueryResult<std::vector<tapescope::EventRow>>& result);
std::string DescribeRangeResult(const tapescope::RangeQuery& query,
                                const tapescope::QueryResult<std::vector<json>>& result);
std::string DescribeRangeResult(const tapescope::RangeQuery& query,
                                const tapescope::QueryResult<std::vector<tapescope::EventRow>>& result);
std::string EventSummaryText(const json& event);
std::string EventSummaryText(const tapescope::EventRow& event);
std::string DescribeOrderLookupResult(const std::string& descriptor,
                                      const tapescope::QueryResult<json>& result);
std::string DescribeOrderLookupResult(const std::string& descriptor,
                                      const tapescope::QueryResult<tapescope::EventListPayload>& result);
std::string DescribeInvestigationPayload(const std::string& heading,
                                         const std::string& descriptor,
                                         const tapescope::QueryResult<json>& result);
std::string DescribeInvestigationPayload(const std::string& heading,
                                         const std::string& descriptor,
                                         const tapescope::QueryResult<tapescope::InvestigationPayload>& result);
std::string DescribeSeekOrderResult(const std::string& descriptor,
                                    const tapescope::QueryResult<json>& result);
std::string DescribeSeekOrderResult(const std::string& descriptor,
                                    const tapescope::QueryResult<tapescope::SeekOrderPayload>& result);
std::string DescribeReportInventoryResult(const tapescope::QueryResult<json>& sessionReports,
                                          const tapescope::QueryResult<json>& caseReports);
std::string DescribeReportInventoryResult(const tapescope::QueryResult<tapescope::ReportInventoryPayload>& sessionReports,
                                          const tapescope::QueryResult<tapescope::ReportInventoryPayload>& caseReports);
std::string DescribeArtifactExportResult(const std::string& artifactId,
                                         const std::string& exportFormat,
                                         const tapescope::QueryResult<json>& result);
std::string DescribeSessionQualityResult(const tapescope::RangeQuery& query,
                                         bool includeLiveTail,
                                         const tapescope::QueryResult<json>& result);
std::string DescribeSessionQualityResult(const tapescope::RangeQuery& query,
                                         bool includeLiveTail,
                                         const tapescope::QueryResult<tapescope::SessionQualityPayload>& result);
std::string DescribeIncidentListResult(const tapescope::QueryResult<tapescope::IncidentListPayload>& result);
bool ReplayRangeFromSeekSummary(const json& summary, tapescope::RangeQuery* query);
bool ReplayRangeFromInvestigationSummary(const json& summary, tapescope::RangeQuery* query);

enum class OrderAnchorType {
    TraceId = 0,
    OrderId = 1,
    PermId = 2,
    ExecId = 3
};

OrderAnchorType OrderAnchorTypeFromIndex(NSInteger index);
std::string OrderAnchorTypeKey(OrderAnchorType type);
NSInteger OrderAnchorTypeIndexForKey(const std::string& key);
NSString* PlaceholderForOrderAnchorType(OrderAnchorType type);

} // namespace tapescope_support
