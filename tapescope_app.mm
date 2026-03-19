#import <AppKit/AppKit.h>

#include "tapescope_support.h"
#include "tapescope_window_internal.h"

#include <dispatch/dispatch.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

namespace {

using namespace tapescope_support;

NSColor* PaneButtonActiveColor() {
    return [NSColor colorWithCalibratedRed:0.133 green:0.369 blue:0.698 alpha:1.0];
}

NSColor* PaneButtonIdleColor() {
    return [NSColor colorWithCalibratedRed:0.933 green:0.915 blue:0.872 alpha:1.0];
}

NSColor* PaneButtonIdleBorderColor() {
    return [NSColor colorWithCalibratedRed:0.827 green:0.792 blue:0.714 alpha:1.0];
}

} // namespace

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
    window.collectionBehavior = NSWindowCollectionBehaviorMoveToActiveSpace;
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
    _interactiveQueue = dispatch_queue_create("com.foxy.tapescope.interactive", DISPATCH_QUEUE_SERIAL);
    _artifactQueue = dispatch_queue_create("com.foxy.tapescope.artifact", DISPATCH_QUEUE_SERIAL);
    _overviewPane = std::make_unique<tapescope::InvestigationPaneController>();
    _findingPane = std::make_unique<tapescope::InvestigationPaneController>();
    _anchorPane = std::make_unique<tapescope::InvestigationPaneController>();
    _orderCasePane = std::make_unique<tapescope::InvestigationPaneController>();
    _incidentPane = std::make_unique<tapescope::InvestigationPaneController>();
    _artifactPane = std::make_unique<tapescope::InvestigationPaneController>();
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

- (NSTabViewItem*)textTabItemWithIdentifier:(NSString*)identifier
                                     label:(NSString*)label
                                  textView:(NSTextView* __strong*)outTextView {
    NSTextView* textView = MakeReadOnlyTextView();
    NSScrollView* scrollView = MakeScrollView(textView, 420.0);

    NSTabViewItem* item = [[NSTabViewItem alloc] initWithIdentifier:identifier];
    item.label = label;
    item.view = scrollView;
    if (outTextView != nullptr) {
        *outTextView = textView;
    }
    return item;
}

- (NSTabViewItem*)liveEventsTabItem {
    const auto paneWithStack = MakePaneWithStack();
    NSStackView* stack = paneWithStack.stack;
    NSView* pane = paneWithStack.view;

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
    item.label = @"Live";
    item.view = pane;
    return item;
}

- (NSTabViewItem*)overviewTabItem {
    const auto paneWithStack = MakePaneWithStack();
    NSStackView* stack = paneWithStack.stack;
    NSView* pane = paneWithStack.view;
    [stack addArrangedSubview:MakeIntroLabel(@"Session overview: summarize the major incidents and evidence for a frozen session_seq window.",
                                             2)];

    NSStackView* fieldsRow = MakeControlRow();
    [fieldsRow addArrangedSubview:MakeLabel(@"first_session_seq",
                                            [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                            [NSColor secondaryLabelColor])];
    _overviewFirstField = MakeMonospacedField(130.0, UInt64String(_lastOverviewQuery.firstSessionSeq));
    [fieldsRow addArrangedSubview:_overviewFirstField];
    [fieldsRow addArrangedSubview:MakeLabel(@"last_session_seq",
                                            [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                            [NSColor secondaryLabelColor])];
    _overviewLastField = MakeMonospacedField(130.0, UInt64String(_lastOverviewQuery.lastSessionSeq));
    [fieldsRow addArrangedSubview:_overviewLastField];
    [stack addArrangedSubview:fieldsRow];

    NSStackView* primaryActions = MakeControlRow();
    [primaryActions setSpacing:10.0];

    _overviewFetchButton = [NSButton buttonWithTitle:@"Read Overview"
                                              target:self
                                              action:@selector(fetchOverview:)];
    _overviewFetchButton.bezelColor = PaneButtonIdleColor();
    [primaryActions addArrangedSubview:_overviewFetchButton];
    _overviewScanButton = [NSButton buttonWithTitle:@"Scan Report"
                                             target:self
                                             action:@selector(scanOverviewReport:)];
    _overviewScanButton.bezelColor = PaneButtonActiveColor();
    _overviewScanButton.contentTintColor = [NSColor whiteColor];
    [primaryActions addArrangedSubview:_overviewScanButton];
    [stack addArrangedSubview:primaryActions];

    NSStackView* secondaryActions = MakeControlRow();
    [secondaryActions setSpacing:10.0];
    _overviewLoadReplayButton = [NSButton buttonWithTitle:@"Load Range"
                                                   target:self
                                                   action:@selector(loadReplayWindowFromOverview:)];
    _overviewLoadReplayButton.enabled = NO;
    [secondaryActions addArrangedSubview:_overviewLoadReplayButton];
    _overviewOpenSelectedIncidentButton = [NSButton buttonWithTitle:@"Open Selected Incident"
                                                             target:self
                                                             action:@selector(openSelectedOverviewIncident:)];
    _overviewOpenSelectedIncidentButton.enabled = NO;
    [secondaryActions addArrangedSubview:_overviewOpenSelectedIncidentButton];
    _overviewOpenSelectedEvidenceButton = [NSButton buttonWithTitle:@"Open Selected Evidence"
                                                             target:self
                                                             action:@selector(openSelectedOverviewEvidence:)];
    _overviewOpenSelectedEvidenceButton.enabled = NO;
    [secondaryActions addArrangedSubview:_overviewOpenSelectedEvidenceButton];
    [stack addArrangedSubview:secondaryActions];

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
    item.label = @"Overview";
    item.view = pane;
    return item;
}

- (NSTabViewItem*)rangeTabItem {
    const auto paneWithStack = MakePaneWithStack();
    NSStackView* stack = paneWithStack.stack;
    NSView* pane = paneWithStack.view;
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
    item.label = @"Range";
    item.view = pane;
    return item;
}

- (NSTabViewItem*)qualityTabItem {
    const auto paneWithStack = MakePaneWithStack();
    NSStackView* stack = paneWithStack.stack;
    NSView* pane = paneWithStack.view;
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
    item.label = @"Quality";
    item.view = pane;
    return item;
}

- (NSTabViewItem*)findingTabItem {
    const auto paneWithStack = MakePaneWithStack();
    NSStackView* stack = paneWithStack.stack;
    NSView* pane = paneWithStack.view;
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
    item.label = @"Finding";
    item.view = pane;
    return item;
}

- (NSTabViewItem*)anchorTabItem {
    const auto paneWithStack = MakePaneWithStack();
    NSStackView* stack = paneWithStack.stack;
    NSView* pane = paneWithStack.view;
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
    item.label = @"Anchor";
    item.view = pane;
    return item;
}

- (NSTabViewItem*)orderTabItem {
    const auto paneWithStack = MakePaneWithStack();
    NSStackView* stack = paneWithStack.stack;
    NSView* pane = paneWithStack.view;
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
    item.label = @"Orders";
    item.view = pane;
    return item;
}

- (NSTabViewItem*)orderCaseTabItem {
    const auto paneWithStack = MakePaneWithStack();
    NSStackView* stack = paneWithStack.stack;
    NSView* pane = paneWithStack.view;
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
    _orderCaseEnrichButton = [NSButton buttonWithTitle:@"Fast Summary"
                                                target:self
                                                action:@selector(fetchOrderCaseEnrichment:)];
    [controls addArrangedSubview:_orderCaseEnrichButton];
    _orderCaseRefreshContextButton = [NSButton buttonWithTitle:@"Refresh Context"
                                                        target:self
                                                        action:@selector(refreshOrderCaseExternalContext:)];
    [controls addArrangedSubview:_orderCaseRefreshContextButton];
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
    item.label = @"Order Case";
    item.view = pane;
    return item;
}

- (NSTabViewItem*)seekTabItem {
    const auto paneWithStack = MakePaneWithStack();
    NSStackView* stack = paneWithStack.stack;
    NSView* pane = paneWithStack.view;
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
    item.label = @"Replay";
    item.view = pane;
    return item;
}

- (NSTabViewItem*)incidentTabItem {
    const auto paneWithStack = MakePaneWithStack();
    NSStackView* stack = paneWithStack.stack;
    NSView* pane = paneWithStack.view;
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
    _incidentEnrichButton = [NSButton buttonWithTitle:@"Fast Summary"
                                               target:self
                                               action:@selector(fetchIncidentEnrichment:)];
    [controls addArrangedSubview:_incidentEnrichButton];
    _incidentExplainButton = [NSButton buttonWithTitle:@"Deep Explain"
                                                target:self
                                                action:@selector(fetchIncidentExplanation:)];
    [controls addArrangedSubview:_incidentExplainButton];
    _incidentRefreshContextButton = [NSButton buttonWithTitle:@"Refresh Context"
                                                       target:self
                                                       action:@selector(refreshIncidentExternalContext:)];
    [controls addArrangedSubview:_incidentRefreshContextButton];
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
    item.label = @"Incident";
    item.view = pane;
    return item;
}

- (NSTabViewItem*)artifactTabItem {
    const auto paneWithStack = MakePaneWithStack();
    NSStackView* stack = paneWithStack.stack;
    NSView* pane = paneWithStack.view;
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
    _artifactExportBundleButton = [NSButton buttonWithTitle:@"Export Bundle"
                                                     target:self
                                                     action:@selector(exportLoadedArtifactBundle:)];
    _artifactExportBundleButton.enabled = NO;
    [controls addArrangedSubview:_artifactExportBundleButton];
    _artifactRevealBundleButton = [NSButton buttonWithTitle:@"Reveal Bundle"
                                                     target:self
                                                     action:@selector(revealLoadedArtifactBundle:)];
    _artifactRevealBundleButton.enabled = NO;
    [controls addArrangedSubview:_artifactRevealBundleButton];
    _artifactOpenSourceButton = [NSButton buttonWithTitle:@"Open Source Artifact"
                                                   target:self
                                                   action:@selector(openLoadedArtifactSource:)];
    _artifactOpenSourceButton.enabled = NO;
    [controls addArrangedSubview:_artifactOpenSourceButton];
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
    item.label = @"Artifacts";
    item.view = pane;
    return item;
}

- (NSTabViewItem*)reportInventoryTabItem {
    const auto paneWithStack = MakePaneWithStack();
    NSStackView* stack = paneWithStack.stack;
    NSView* pane = paneWithStack.view;
    [stack addArrangedSubview:MakeIntroLabel(@"Durable reports and bundles: browse persisted session/case reports, export portable Phase 6 bundles, import case bundles, and reopen imported cases.",
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

    _reportInventoryExportSessionBundleButton = [NSButton buttonWithTitle:@"Export Session Bundle"
                                                                   target:self
                                                                   action:@selector(exportSelectedSessionBundle:)];
    _reportInventoryExportSessionBundleButton.enabled = NO;
    [controls addArrangedSubview:_reportInventoryExportSessionBundleButton];

    _reportInventoryOpenCaseButton = [NSButton buttonWithTitle:@"Open Selected Case"
                                                        target:self
                                                        action:@selector(openSelectedCaseReport:)];
    _reportInventoryOpenCaseButton.enabled = NO;
    [controls addArrangedSubview:_reportInventoryOpenCaseButton];

    _reportInventoryExportCaseBundleButton = [NSButton buttonWithTitle:@"Export Case Bundle"
                                                                target:self
                                                                action:@selector(exportSelectedCaseBundle:)];
    _reportInventoryExportCaseBundleButton.enabled = NO;
    [controls addArrangedSubview:_reportInventoryExportCaseBundleButton];

    [stack addArrangedSubview:controls];

    NSStackView* importControls = MakeControlRow();
    [importControls addArrangedSubview:MakeLabel(@"bundle_path",
                                                 [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                                 [NSColor secondaryLabelColor])];
    _bundleImportPathField = MakeMonospacedField(360.0, nil, @"Choose a case bundle to import");
    [importControls addArrangedSubview:_bundleImportPathField];

    _bundleChooseImportButton = [NSButton buttonWithTitle:@"Choose Bundle…"
                                                   target:self
                                                   action:@selector(chooseImportBundlePath:)];
    [importControls addArrangedSubview:_bundleChooseImportButton];

    _bundleImportButton = [NSButton buttonWithTitle:@"Import Bundle"
                                             target:self
                                             action:@selector(importSelectedBundlePath:)];
    [importControls addArrangedSubview:_bundleImportButton];

    _bundlePreviewButton = [NSButton buttonWithTitle:@"Preview Bundle"
                                              target:self
                                              action:@selector(previewBundlePath:)];
    [importControls addArrangedSubview:_bundlePreviewButton];

    _bundleRevealPathButton = [NSButton buttonWithTitle:@"Reveal Path"
                                                 target:self
                                                 action:@selector(revealSelectedBundlePath:)];
    [importControls addArrangedSubview:_bundleRevealPathButton];

    _reportInventoryOpenImportedButton = [NSButton buttonWithTitle:@"Open Imported Case"
                                                            target:self
                                                            action:@selector(openSelectedImportedCase:)];
    _reportInventoryOpenImportedButton.enabled = NO;
    [importControls addArrangedSubview:_reportInventoryOpenImportedButton];

    _reportInventoryLoadImportedRangeButton = [NSButton buttonWithTitle:@"Load Imported Range"
                                                                 target:self
                                                                 action:@selector(loadReplayRangeFromImportedCase:)];
    _reportInventoryLoadImportedRangeButton.enabled = NO;
    [importControls addArrangedSubview:_reportInventoryLoadImportedRangeButton];

    _reportInventoryOpenImportedSourceButton = [NSButton buttonWithTitle:@"Open Source Artifact"
                                                                  target:self
                                                                  action:@selector(openSelectedImportedSourceArtifact:)];
    _reportInventoryOpenImportedSourceButton.enabled = NO;
    [importControls addArrangedSubview:_reportInventoryOpenImportedSourceButton];

    [stack addArrangedSubview:importControls];

    NSStackView* previewControls = MakeControlRow();
    _bundlePreviewLoadRangeButton = [NSButton buttonWithTitle:@"Load Preview Range"
                                                       target:self
                                                       action:@selector(loadReplayRangeFromPreviewedBundle:)];
    _bundlePreviewLoadRangeButton.enabled = NO;
    [previewControls addArrangedSubview:_bundlePreviewLoadRangeButton];

    _bundlePreviewOpenSourceButton = [NSButton buttonWithTitle:@"Open Preview Source"
                                                        target:self
                                                        action:@selector(openPreviewBundleSourceArtifact:)];
    _bundlePreviewOpenSourceButton.enabled = NO;
    [previewControls addArrangedSubview:_bundlePreviewOpenSourceButton];

    _bundlePreviewOpenImportedButton = [NSButton buttonWithTitle:@"Open Matching Imported"
                                                          target:self
                                                          action:@selector(openMatchingImportedBundle:)];
    _bundlePreviewOpenImportedButton.enabled = NO;
    [previewControls addArrangedSubview:_bundlePreviewOpenImportedButton];

    [stack addArrangedSubview:previewControls];

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

    [stack addArrangedSubview:MakeSectionLabel(@"Imported Case Bundles")];

    _importedCaseTableView = MakeStandardTableView(self, self);
    AddTableColumn(_importedCaseTableView, @"imported_case_id", @"imported_case_id", 130.0);
    AddTableColumn(_importedCaseTableView, @"artifact_id", @"artifact_id", 210.0);
    AddTableColumn(_importedCaseTableView, @"source_revision_id", @"source_revision_id", 130.0);
    AddTableColumn(_importedCaseTableView, @"headline", @"headline", 330.0);
    ConfigureTablePrimaryAction(_importedCaseTableView, self, @selector(openSelectedImportedCase:));
    [stack addArrangedSubview:MakeTableScrollView(_importedCaseTableView, 140.0)];

    [stack addArrangedSubview:MakeSectionLabel(@"Workflow Summary")];

    _reportInventoryTextView = MakeReadOnlyTextView();
    _reportInventoryTextView.string = @"Refresh report inventory to browse durable reports, export portable bundles, import case bundles, and reopen imported cases in ArtifactPane.";
    [stack addArrangedSubview:MakeScrollView(_reportInventoryTextView, 130.0)];

    [stack addArrangedSubview:MakeSectionLabel(@"Imported Case Detail")];

    _importedCaseTextView = MakeReadOnlyTextView();
    _importedCaseTextView.string = @"Select an imported case row to inspect import diagnostics, source artifact linkage, and replay boundaries.";
    [stack addArrangedSubview:MakeScrollView(_importedCaseTextView, 150.0)];

    [stack addArrangedSubview:MakeSectionLabel(@"Bundle Preview")];

    _bundlePreviewTextView = MakeReadOnlyTextView();
    _bundlePreviewTextView.string = @"Choose or generate a Phase 6 bundle path, then preview it here before import.";
    [stack addArrangedSubview:MakeScrollView(_bundlePreviewTextView, 160.0)];

    NSTabViewItem* item = [[NSTabViewItem alloc] initWithIdentifier:@"ReportInventoryPane"];
    item.label = @"Reports";
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

    const auto headerCardWithStack = MakeCardWithStack(10.0);
    NSStackView* headerStack = headerCardWithStack.stack;
    NSBox* headerCard = headerCardWithStack.box;

    NSTextField* title = MakeLabel(@"TapeScope",
                                   [NSFont systemFontOfSize:30.0 weight:NSFontWeightBlack],
                                   [NSColor labelColor]);
    [headerStack addArrangedSubview:title];

    NSTextField* subtitle = MakeLabel(@"Phase 4/6/7: native status, live-tail, overview, incident, replay-target, range, quality, finding, anchor, order case, report inventory, artifact/export, portable bundle workflows, and local Phase 7 artifact reopen flows backed by the engine and durable artifact seams.",
                                      [NSFont systemFontOfSize:13.0 weight:NSFontWeightMedium],
                                      TapeInkMutedColor());
    subtitle.lineBreakMode = NSLineBreakByWordWrapping;
    subtitle.maximumNumberOfLines = 2;
    [headerStack addArrangedSubview:subtitle];

    const auto bannerCardWithStack = MakeCardWithStack(4.0);
    NSStackView* bannerStack = bannerCardWithStack.stack;
    _bannerBox = bannerCardWithStack.box;
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

    const auto summaryCardWithStack = MakeCardWithStack(12.0);
    NSStackView* summaryStack = summaryCardWithStack.stack;
    NSBox* summaryCard = summaryCardWithStack.box;
    [summaryStack addArrangedSubview:MakeSectionLabel(@"Engine Snapshot")];
    [summaryStack addArrangedSubview:summaryGrid];
    [summaryGrid.widthAnchor constraintEqualToAnchor:summaryStack.widthAnchor].active = YES;
    [root addArrangedSubview:summaryCard];

    _tabView = [[NSTabView alloc] initWithFrame:NSZeroRect];
    _tabView.translatesAutoresizingMaskIntoConstraints = NO;
    _tabView.delegate = self;
    _tabView.tabViewType = NSNoTabsNoBorder;
    [_tabView addTabViewItem:[self textTabItemWithIdentifier:@"StatusPane" label:@"Status" textView:&_statusTextView]];
    [_tabView addTabViewItem:[self liveEventsTabItem]];
    [_tabView addTabViewItem:[self recentHistoryTabItem]];
    [_tabView addTabViewItem:[self bundleHistoryTabItem]];
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
    [_tabView addTabViewItem:[self phase7ArtifactsTabItem]];
    [_tabView addTabViewItem:[self phase8InboxTabItem]];
    [_tabView addTabViewItem:[self artifactTabItem]];
    [_tabView.heightAnchor constraintGreaterThanOrEqualToConstant:520.0].active = YES;

    const auto tabCardWithStack = MakeCardWithStack(12.0);
    NSStackView* tabStack = tabCardWithStack.stack;
    NSBox* tabCard = tabCardWithStack.box;
    [tabStack addArrangedSubview:MakeSectionLabel(@"Investigation Surface")];

    NSStackView* surfaceRow = [[NSStackView alloc] initWithFrame:NSZeroRect];
    surfaceRow.orientation = NSUserInterfaceLayoutOrientationHorizontal;
    surfaceRow.alignment = NSLayoutAttributeTop;
    surfaceRow.spacing = 14.0;
    surfaceRow.translatesAutoresizingMaskIntoConstraints = NO;

    const auto navCardWithStack = MakeCardWithStack(10.0);
    NSBox* navCard = navCardWithStack.box;
    NSStackView* navStack = navCardWithStack.stack;
    [navCard.widthAnchor constraintEqualToConstant:210.0].active = YES;
    [navStack addArrangedSubview:MakeSectionLabel(@"Panes")];
    [navStack addArrangedSubview:MakeIntroLabel(@"Jump between overview, incidents, orders, artifacts, and Phase 7/8 workflows.", 3)];
    _paneButtons = [[NSMutableArray alloc] init];
    for (NSTabViewItem* item in _tabView.tabViewItems) {
        NSButton* button = [self makePaneNavigationButtonWithTitle:item.label ?: @"Pane"
                                                         identifier:(NSString*)item.identifier];
        [_paneButtons addObject:button];
        [navStack addArrangedSubview:button];
    }

    const auto surfaceCardWithStack = MakeCardWithStack(12.0);
    NSBox* surfaceCard = surfaceCardWithStack.box;
    NSStackView* surfaceStack = surfaceCardWithStack.stack;
    _activePaneLabel = MakeLabel(@"Overview",
                                 [NSFont systemFontOfSize:18.0 weight:NSFontWeightBold],
                                 [NSColor labelColor]);
    [surfaceStack addArrangedSubview:_activePaneLabel];
    [surfaceStack addArrangedSubview:MakeIntroLabel(@"Use the pane list on the left to move between investigation surfaces. The overview pane auto-loads a report on startup.",
                                                    2)];
    [surfaceStack addArrangedSubview:_tabView];

    [surfaceRow addArrangedSubview:navCard];
    [surfaceRow addArrangedSubview:surfaceCard];
    [surfaceCard.widthAnchor constraintGreaterThanOrEqualToConstant:760.0].active = YES;
    [tabStack addArrangedSubview:surfaceRow];
    [root addArrangedSubview:tabCard];

    [self selectPaneWithIdentifier:@"SessionOverviewPane"];

    _overviewPane->bind(_overviewStateLabel,
                        _overviewTextView,
                        _overviewEvidenceTableView,
                        _overviewOpenSelectedEvidenceButton,
                        _overviewLoadReplayButton,
                        @"Read or scan a session overview, then select a citation row to inspect or reopen evidence.",
                        @"Select an overview evidence row first.",
                        @"Selected overview evidence is missing artifact_id.",
                        @"No overview replay window is ready yet.");
    _findingPane->bind(_findingStateLabel,
                       _findingTextView,
                       _findingEvidenceTableView,
                       _findingOpenSelectedEvidenceButton,
                       _findingLoadReplayButton,
                       @"Read a finding, then select a citation row to inspect or reopen evidence.",
                       @"Select a finding evidence row first.",
                       @"Selected finding evidence is missing artifact_id.",
                       @"No finding replay window is ready yet.");
    _anchorPane->bind(_anchorStateLabel,
                      _anchorTextView,
                      _anchorEvidenceTableView,
                      _anchorOpenSelectedEvidenceButton,
                      _anchorLoadReplayButton,
                      @"Read an order anchor, then select a citation row to inspect or reopen evidence.",
                      @"Select an anchor evidence row first.",
                      @"Selected anchor evidence is missing artifact_id.",
                      @"No anchor replay window is ready yet.");
    _orderCasePane->bind(_orderCaseStateLabel,
                         _orderCaseTextView,
                         _orderCaseEvidenceTableView,
                         _orderCaseOpenSelectedEvidenceButton,
                         _orderCaseLoadReplayButton,
                         @"Read an order case, then select a citation row to inspect or reopen evidence.",
                         @"Select an order-case evidence row first.",
                         @"Selected order-case evidence is missing artifact_id.",
                         @"No order-case replay window is ready yet.");
    _incidentPane->bind(_incidentStateLabel,
                        _incidentTextView,
                        _incidentEvidenceTableView,
                        _incidentOpenSelectedEvidenceButton,
                        _incidentLoadReplayButton,
                        @"Read an incident, then select a citation row to inspect or reopen evidence.",
                        @"Select an incident evidence row first.",
                        @"Selected incident evidence is missing artifact_id.",
                        @"No incident replay window is ready yet.");
    _artifactPane->bind(_artifactStateLabel,
                        _artifactTextView,
                        _artifactEvidenceTableView,
                        _artifactOpenSelectedEvidenceButton,
                        nil,
                        @"Read an artifact, then select a citation row to inspect or reopen evidence.",
                        @"Select an artifact evidence row first.",
                        @"Selected artifact evidence is missing artifact_id.",
                        @"No artifact replay window is available.");
    _evidencePaneBindings = {
        {_overviewEvidenceTableView, _overviewPane.get()},
        {_findingEvidenceTableView, _findingPane.get()},
        {_anchorEvidenceTableView, _anchorPane.get()},
        {_orderCaseEvidenceTableView, _orderCasePane.get()},
        {_incidentEvidenceTableView, _incidentPane.get()},
        {_artifactEvidenceTableView, _artifactPane.get()}
    };

    _statusTextView.string = @"Waiting for the first status response…";
    _liveTextView.string = @"Waiting for the first live-tail response…";
    [self updateBannerAppearanceWithColor:[NSColor secondaryLabelColor]];
}

- (void)showWindowAndStart {
    [self.window center];
    [self showWindow:nil];
    [self.window deminiaturize:nil];
    [self.window orderFrontRegardless];
    [self.window makeKeyAndOrderFront:nil];
    [NSApp activateIgnoringOtherApps:YES];

    if (_tabView.selectedTabViewItem == nil ||
        [_tabView.selectedTabViewItem.identifier isEqual:@"StatusPane"]) {
        [self selectPaneWithIdentifier:@"SessionOverviewPane"];
        [self persistApplicationState];
    }

    [self.window makeFirstResponder:_overviewFirstField];
    [self startPolling];
    [self refreshIncidentList:nil];
    [self refreshReportInventory:nil];
    if (_tabView.selectedTabViewItem != nil &&
        [_tabView.selectedTabViewItem.identifier isEqual:@"SessionOverviewPane"] &&
        _overviewTextView != nil &&
        [_overviewTextView.string containsString:@"Read a session overview"]) {
        [self scanOverviewReport:nil];
    }
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
        _pollingPaused ? [NSString stringWithFormat:@"Polling paused (manual refresh only, %.1fs baseline)", tapescope_window_internal::kPollIntervalSeconds]
                       : [NSString stringWithFormat:@"Polling engine every %.1fs", tapescope_window_internal::kPollIntervalSeconds];
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

- (NSButton*)makePaneNavigationButtonWithTitle:(NSString*)title identifier:(NSString*)identifier {
    NSButton* button = [NSButton buttonWithTitle:title ?: @"Pane"
                                          target:self
                                          action:@selector(paneNavigationPressed:)];
    button.translatesAutoresizingMaskIntoConstraints = NO;
    button.identifier = identifier;
    button.bordered = YES;
    button.bezelStyle = NSBezelStyleRounded;
    button.buttonType = NSButtonTypeMomentaryPushIn;
    button.font = [NSFont systemFontOfSize:13.0 weight:NSFontWeightSemibold];
    button.alignment = NSTextAlignmentLeft;
    [button.heightAnchor constraintEqualToConstant:30.0].active = YES;
    [button.widthAnchor constraintGreaterThanOrEqualToConstant:170.0].active = YES;
    return button;
}

- (void)syncPaneSelectionChrome {
    if (_tabView == nil || _tabView.selectedTabViewItem == nil) {
        return;
    }
    NSString* identifier = (NSString*)_tabView.selectedTabViewItem.identifier;
    if (_activePaneLabel != nil) {
        _activePaneLabel.stringValue = _tabView.selectedTabViewItem.label ?: @"Pane";
    }
    for (NSButton* button in _paneButtons) {
        const bool selected = identifier != nil && [button.identifier isEqualToString:identifier];
        button.bezelColor = selected ? PaneButtonActiveColor() : PaneButtonIdleColor();
        button.contentTintColor = selected ? [NSColor whiteColor] : [NSColor labelColor];
        button.font = [NSFont systemFontOfSize:13.0 weight:selected ? NSFontWeightBold : NSFontWeightSemibold];
        if (!selected && button.wantsLayer) {
            button.layer.borderColor = PaneButtonIdleBorderColor().CGColor;
        }
    }
}

- (void)selectPaneWithIdentifier:(NSString*)identifier {
    if (_tabView == nil || identifier == nil) {
        return;
    }
    [_tabView selectTabViewItemWithIdentifier:identifier];
    [self syncPaneSelectionChrome];
}

- (void)paneNavigationPressed:(id)sender {
    if ([sender isKindOfClass:[NSButton class]]) {
        NSButton* button = (NSButton*)sender;
        if (button.identifier != nil) {
            [self selectPaneWithIdentifier:button.identifier];
        }
        [self persistApplicationState];
    }
}

- (void)tabView:(NSTabView*)tabView didSelectTabViewItem:(nullable NSTabViewItem*)tabViewItem {
    if (tabView != _tabView || tabViewItem == nil) {
        return;
    }
    [self syncPaneSelectionChrome];
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
        _pollTimer = [NSTimer scheduledTimerWithTimeInterval:tapescope_window_internal::kPollIntervalSeconds
                                                      target:self
                                                    selector:@selector(refresh:)
                                                    userInfo:nil
                                                     repeats:YES];
    }
    [self updatePollingStatusText];
    [self persistApplicationState];
}

- (tapescope::InvestigationPaneController*)paneControllerForEvidenceTable:(NSTableView*)tableView {
    if (tableView == nil) {
        return nullptr;
    }
    for (const auto& binding : _evidencePaneBindings) {
        if (binding.first == tableView) {
            return binding.second;
        }
    }
    return nullptr;
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
