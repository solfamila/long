#pragma once

#import <AppKit/AppKit.h>

#include "tapescope_client.h"
#include "tapescope_investigation_pane.h"

#include <dispatch/dispatch.h>

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace tapescope_window_internal {

inline constexpr NSTimeInterval kPollIntervalSeconds = 2.0;
inline constexpr std::size_t kLiveTailLimit = 32;

struct ProbeSnapshot {
    tapescope::QueryResult<tapescope::StatusSnapshot> status;
    tapescope::QueryResult<std::vector<tapescope::EventRow>> liveTail;
};

}

@interface TapeScopeWindowController : NSWindowController <NSTableViewDataSource, NSTableViewDelegate> {
@protected
    std::unique_ptr<tapescope::QueryClient> _client;
    dispatch_queue_t _pollQueue;
    dispatch_queue_t _interactiveQueue;
    dispatch_queue_t _artifactQueue;
    NSTimer* _pollTimer;
    BOOL _pollInFlight;
    BOOL _pollingPaused;
    NSDate* _lastProbeAt;
    std::uint64_t _rangeRequestToken;
    std::uint64_t _qualityRequestToken;
    std::uint64_t _orderLookupRequestToken;
    std::uint64_t _seekRequestToken;
    std::uint64_t _reportInventoryRequestToken;
    std::uint64_t _artifactExportRequestToken;
    std::uint64_t _bundleWorkflowRequestToken;

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
    std::vector<tapescope::EventRow> _liveEvents;

    NSButton* _recentOpenButton;
    NSButton* _recentClearButton;
    NSTextField* _recentStateLabel;
    NSTableView* _recentTableView;
    NSTextView* _recentTextView;
    std::vector<tapescope::json> _recentHistoryItems;

    NSButton* _bundleHistoryOpenButton;
    NSButton* _bundleHistoryOpenSourceButton;
    NSButton* _bundleHistoryRevealButton;
    NSButton* _bundleHistoryLoadRangeButton;
    NSButton* _bundleHistoryClearButton;
    NSTextField* _bundleHistoryStateLabel;
    NSTableView* _bundleHistoryTableView;
    NSTextView* _bundleHistoryTextView;
    std::vector<tapescope::json> _bundleHistoryItems;

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
    tapescope::RangeQuery _lastOverviewQuery;
    std::vector<tapescope::IncidentListRow> _overviewIncidents;
    std::unique_ptr<tapescope::InvestigationPaneController> _overviewPane;

    NSTextField* _rangeFirstField;
    NSTextField* _rangeLastField;
    NSButton* _rangeFetchButton;
    NSTextField* _rangeStateLabel;
    NSTableView* _rangeTableView;
    NSTextView* _rangeTextView;
    BOOL _rangeInFlight;
    tapescope::RangeQuery _lastRangeQuery;
    std::vector<tapescope::EventRow> _rangeEvents;

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
    std::unique_ptr<tapescope::InvestigationPaneController> _findingPane;

    NSTextField* _anchorIdField;
    NSButton* _anchorFetchButton;
    NSButton* _anchorLoadReplayButton;
    NSButton* _anchorOpenSelectedEvidenceButton;
    NSTextField* _anchorStateLabel;
    NSTableView* _anchorEvidenceTableView;
    NSTextView* _anchorTextView;
    BOOL _anchorInFlight;
    std::unique_ptr<tapescope::InvestigationPaneController> _anchorPane;

    NSPopUpButton* _orderAnchorTypePopup;
    NSTextField* _orderAnchorInputField;
    NSButton* _orderLookupButton;
    NSTextField* _orderStateLabel;
    NSTableView* _orderTableView;
    NSTextView* _orderTextView;
    BOOL _orderLookupInFlight;
    std::vector<tapescope::EventRow> _orderEvents;

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
    std::unique_ptr<tapescope::InvestigationPaneController> _orderCasePane;

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
    std::vector<tapescope::IncidentListRow> _latestIncidents;
    std::unique_ptr<tapescope::InvestigationPaneController> _incidentPane;

    NSTextField* _artifactIdField;
    NSButton* _artifactFetchButton;
    NSPopUpButton* _artifactExportFormatPopup;
    NSButton* _artifactExportButton;
    NSButton* _artifactExportBundleButton;
    NSButton* _artifactRevealBundleButton;
    NSButton* _artifactOpenSourceButton;
    NSButton* _artifactOpenSelectedEvidenceButton;
    NSTextField* _artifactStateLabel;
    NSTableView* _artifactEvidenceTableView;
    NSTextView* _artifactTextView;
    BOOL _artifactInFlight;
    std::uint64_t _loadedArtifactReportId;
    std::string _loadedArtifactKind;
    std::string _loadedArtifactBundlePath;
    std::string _loadedArtifactSourceArtifactId;
    std::unique_ptr<tapescope::InvestigationPaneController> _artifactPane;

    NSButton* _reportInventoryRefreshButton;
    NSButton* _reportInventoryOpenSessionButton;
    NSButton* _reportInventoryExportSessionBundleButton;
    NSButton* _reportInventoryOpenCaseButton;
    NSButton* _reportInventoryExportCaseBundleButton;
    NSTextField* _bundleImportPathField;
    NSButton* _bundleChooseImportButton;
    NSButton* _bundleImportButton;
    NSButton* _bundleRevealPathButton;
    NSButton* _bundlePreviewButton;
    NSButton* _bundlePreviewLoadRangeButton;
    NSButton* _bundlePreviewOpenSourceButton;
    NSButton* _bundlePreviewOpenImportedButton;
    NSButton* _reportInventoryOpenImportedButton;
    NSButton* _reportInventoryLoadImportedRangeButton;
    NSButton* _reportInventoryOpenImportedSourceButton;
    NSTextField* _reportInventoryStateLabel;
    NSTableView* _sessionReportTableView;
    NSTableView* _caseReportTableView;
    NSTableView* _importedCaseTableView;
    NSTextView* _reportInventoryTextView;
    NSTextView* _importedCaseTextView;
    NSTextView* _bundlePreviewTextView;
    BOOL _reportInventoryInFlight;
    BOOL _bundleWorkflowInFlight;
    std::string _lastBundleWorkflowSummary;
    std::string _previewedBundleId;
    std::string _previewedBundleType;
    std::string _previewedBundleSourceArtifactId;
    std::string _previewedImportedArtifactId;
    std::uint64_t _previewedBundleFirstSessionSeq;
    std::uint64_t _previewedBundleLastSessionSeq;
    std::vector<tapescope::ReportInventoryRow> _latestSessionReports;
    std::vector<tapescope::ReportInventoryRow> _latestCaseReports;
    std::vector<tapescope::ImportedCaseRow> _latestImportedCases;
    std::vector<std::pair<NSTableView*, tapescope::InvestigationPaneController*>> _evidencePaneBindings;
}

- (void)updateBannerAppearanceWithColor:(NSColor*)color;
- (void)updatePollingStatusText;
- (tapescope::InvestigationPaneController*)paneControllerForEvidenceTable:(NSTableView*)tableView;

@end

@interface TapeScopeWindowController (RecentHistory)

- (NSTabViewItem*)recentHistoryTabItem;
- (NSTabViewItem*)bundleHistoryTabItem;
- (tapescope::json)capturePersistentState;
- (void)persistApplicationState;
- (void)restoreApplicationState;
- (void)recordRecentHistoryEntry:(tapescope::json)entry;
- (void)recordBundleHistoryEntry:(tapescope::json)entry;
- (void)recordRecentHistoryForKind:(const std::string&)kind
                          targetId:(const std::string&)targetId
                           payload:(const tapescope::InvestigationPayload&)payload
                     fallbackTitle:(const std::string&)fallbackTitle
                    fallbackDetail:(const std::string&)fallbackDetail
                          metadata:(const tapescope::json&)metadata;
- (void)openSelectedRecentHistory:(id)sender;
- (void)clearRecentHistory:(id)sender;
- (void)openSelectedBundleHistory:(id)sender;
- (void)openSelectedBundleHistorySource:(id)sender;
- (void)revealSelectedBundleHistoryPath:(id)sender;
- (void)loadReplayRangeFromBundleHistory:(id)sender;
- (void)clearBundleHistory:(id)sender;

@end

@interface TapeScopeWindowController (Queries)

- (std::uint64_t)issueRequestToken:(std::uint64_t*)storage;
- (BOOL)isRequestTokenCurrent:(std::uint64_t)token storage:(const std::uint64_t*)storage;
- (void)applyInvestigationResult:(const tapescope::QueryResult<tapescope::InvestigationPayload>&)result
                  paneController:(tapescope::InvestigationPaneController*)pane
                     successText:(NSString*)successText
                syncArtifactField:(BOOL)syncArtifactField;
- (void)loadReplayWindowForPane:(tapescope::InvestigationPaneController*)pane;
- (void)openSelectedEvidenceForPane:(tapescope::InvestigationPaneController*)pane;
- (BOOL)renderSelectionForPane:(tapescope::InvestigationPaneController*)pane;
- (void)loadReplayRange:(const tapescope::RangeQuery&)range
              available:(BOOL)available
             stateLabel:(NSTextField*)stateLabel
         missingMessage:(NSString*)missingMessage;
- (void)applyProbe:(const tapescope_window_internal::ProbeSnapshot&)probe;
- (void)startPolling;
- (void)shutdown;
- (void)refresh:(id)sender;

@end

@interface TapeScopeWindowController (SessionQueries)

- (void)fetchRange:(id)sender;
- (void)loadQualityFromRange:(id)sender;
- (void)fetchSessionQuality:(id)sender;
- (void)fetchFinding:(id)sender;
- (void)loadReplayWindowFromFinding:(id)sender;
- (void)openSelectedFindingEvidence:(id)sender;
- (void)fetchOrderAnchorById:(id)sender;
- (void)loadReplayWindowFromAnchor:(id)sender;
- (void)openSelectedAnchorEvidence:(id)sender;
- (void)fetchOverview:(id)sender;
- (void)scanOverviewReport:(id)sender;
- (void)loadReplayWindowFromOverview:(id)sender;
- (void)openSelectedOverviewEvidence:(id)sender;

@end

@interface TapeScopeWindowController (OrderQueries)

- (BOOL)buildOrderAnchorQueryFromPopup:(NSPopUpButton*)popup
                            inputField:(NSTextField*)inputField
                              outQuery:(tapescope::OrderAnchorQuery*)outQuery
                            descriptor:(std::string*)descriptor
                                 error:(std::string*)error;
- (void)orderAnchorTypeChanged:(id)sender;
- (void)performOrderLookup:(id)sender;
- (void)orderCaseAnchorTypeChanged:(id)sender;
- (void)seekAnchorTypeChanged:(id)sender;
- (void)fetchOrderCase:(id)sender;
- (void)scanOrderCaseReport:(id)sender;
- (void)fetchSeekOrder:(id)sender;
- (void)loadReplayWindowFromSeek:(id)sender;
- (void)loadReplayWindowFromOrderCase:(id)sender;
- (void)openSelectedOrderCaseEvidence:(id)sender;

@end

@interface TapeScopeWindowController (ArtifactQueries)

- (void)fetchIncident:(id)sender;
- (void)loadReplayWindowFromIncident:(id)sender;
- (void)openSelectedIncidentEvidence:(id)sender;
- (void)openSelectedOverviewIncident:(id)sender;
- (void)refreshIncidentList:(id)sender;
- (void)openSelectedIncident:(id)sender;
- (void)fetchArtifact:(id)sender;
- (void)exportArtifactPreview:(id)sender;
- (void)exportLoadedArtifactBundle:(id)sender;
- (void)revealLoadedArtifactBundle:(id)sender;
- (void)openLoadedArtifactSource:(id)sender;
- (void)openSelectedArtifactEvidence:(id)sender;
- (void)refreshReportInventory:(id)sender;
- (void)refreshReportInventoryDetailText;
- (void)exportSelectedSessionBundle:(id)sender;
- (void)exportSelectedCaseBundle:(id)sender;
- (void)chooseImportBundlePath:(id)sender;
- (void)importSelectedBundlePath:(id)sender;
- (void)previewBundlePath:(id)sender;
- (void)loadReplayRangeFromPreviewedBundle:(id)sender;
- (void)openPreviewBundleSourceArtifact:(id)sender;
- (void)openMatchingImportedBundle:(id)sender;
- (void)revealSelectedBundlePath:(id)sender;
- (void)loadReplayRangeFromImportedCase:(id)sender;
- (void)openSelectedImportedSourceArtifact:(id)sender;

@end

@interface TapeScopeWindowController (TableBindings)

- (void)openSelectedSessionReport:(id)sender;
- (void)openSelectedCaseReport:(id)sender;
- (void)openSelectedImportedCase:(id)sender;

@end
