#pragma once

#import <AppKit/AppKit.h>

#include "trading_runtime_host.h"
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

@interface TapeScopeWindowController : NSWindowController <NSTableViewDataSource, NSTableViewDelegate, NSTabViewDelegate> {
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
    std::uint64_t _phase7RequestToken;
    std::uint64_t _phase8RequestToken;

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
    NSMutableArray<NSButton*>* _paneButtons;
    NSTextField* _activePaneLabel;
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
    NSButton* _orderCaseEnrichButton;
    NSButton* _orderCaseRefreshContextButton;
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
    NSButton* _incidentEnrichButton;
    NSButton* _incidentExplainButton;
    NSButton* _incidentRefreshContextButton;
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
    std::string _previewedBundlePath;
    std::string _previewedBundleId;
    std::string _previewedBundleType;
    std::string _previewedBundleSourceArtifactId;
    std::string _previewedImportedArtifactId;
    std::uint64_t _previewedBundleFirstSessionSeq;
    std::uint64_t _previewedBundleLastSessionSeq;
    BOOL _previewedBundleImportAllowed;
    std::string _previewedBundleImportStatus;
    std::string _previewedBundleImportDetail;
    std::vector<tapescope::ReportInventoryRow> _latestSessionReports;
    std::vector<tapescope::ReportInventoryRow> _latestCaseReports;
    std::vector<tapescope::ImportedCaseRow> _latestImportedCases;

    NSButton* _phase7RefreshButton;
    NSTextField* _phase7BundlePathField;
    NSPopUpButton* _phase7ProfilePopup;
    NSTextField* _phase7AnalysisSourceFilterField;
    NSPopUpButton* _phase7AnalysisProfileFilterPopup;
    NSPopUpButton* _phase7AnalysisSortPopup;
    NSTextField* _phase7PlaybookAnalysisFilterField;
    NSTextField* _phase7PlaybookSourceFilterField;
    NSPopUpButton* _phase7PlaybookModeFilterPopup;
    NSPopUpButton* _phase7PlaybookSortPopup;
    NSPopUpButton* _phase7LedgerStatusFilterPopup;
    NSPopUpButton* _phase7LedgerSortPopup;
    NSTextField* _phase7JournalLedgerFilterField;
    NSPopUpButton* _phase7JournalStatusFilterPopup;
    NSPopUpButton* _phase7JournalRecoveryFilterPopup;
    NSPopUpButton* _phase7JournalRecoveryStateFilterPopup;
    NSPopUpButton* _phase7JournalResumePolicyFilterPopup;
    NSPopUpButton* _phase7JournalResolutionFilterPopup;
    NSPopUpButton* _phase7JournalSortPopup;
    NSPopUpButton* _phase7ApplyRecoveryFilterPopup;
    NSPopUpButton* _phase7ApplyRecoveryStateFilterPopup;
    NSPopUpButton* _phase7ApplyResumePolicyFilterPopup;
    NSPopUpButton* _phase7ApplyResolutionFilterPopup;
    NSButton* _phase7ClearFiltersButton;
    NSButton* _phase7ChooseBundleButton;
    NSButton* _phase7RunAnalysisButton;
    NSButton* _phase7BuildPlaybookButton;
    NSButton* _phase7BuildLedgerButton;
    NSButton* _phase7StartJournalButton;
    NSButton* _phase7StartApplyButton;
    NSButton* _phase7OpenAnalysisButton;
    NSButton* _phase7OpenPlaybookButton;
    NSButton* _phase7OpenLedgerButton;
    NSButton* _phase7OpenJournalButton;
    NSButton* _phase7OpenApplyButton;
    NSButton* _phase7OpenSourceArtifactButton;
    NSButton* _phase7OpenLinkedAnalysisButton;
    NSButton* _phase7OpenLinkedLedgerButton;
    NSButton* _phase7OpenLinkedJournalButton;
    NSButton* _phase7DispatchJournalButton;
    NSButton* _phase7RuntimeStartButton;
    NSButton* _phase7RuntimeStopButton;
    NSButton* _phase7RuntimeDispatchButton;
    NSButton* _phase7RuntimeReconcileButton;
    NSButton* _phase7RuntimeSweepButton;
    NSButton* _phase7RuntimeSyncApplyButton;
    NSButton* _phase7LoadRangeButton;
    NSTextField* _phase7JournalActorField;
    NSTextField* _phase7ExecutionCapabilityField;
    NSPopUpButton* _phase7ReviewStatusPopup;
    NSTextField* _phase7ReviewActorField;
    NSTextField* _phase7ReviewCommentField;
    NSButton* _phase7RecordReviewButton;
    NSPopUpButton* _phase7ExecutionStatusPopup;
    NSTextField* _phase7ExecutionActorField;
    NSTextField* _phase7ExecutionCommentField;
    NSTextField* _phase7ExecutionFailureCodeField;
    NSTextField* _phase7ExecutionFailureMessageField;
    NSButton* _phase7RecordExecutionButton;
    NSButton* _phase7RecordApplyButton;
    NSTextField* _phase7StateLabel;
    NSTextField* _phase7RuntimeStatusLabel;
    NSTableView* _phase7AnalysisTableView;
    NSTableView* _phase7PlaybookTableView;
    NSTableView* _phase7LedgerTableView;
    NSTableView* _phase7JournalTableView;
    NSTableView* _phase7ApplyTableView;
    NSTableView* _phase7FindingTableView;
    NSTableView* _phase7ActionTableView;
    NSTextView* _phase7ProfilesTextView;
    NSTextView* _phase7TextView;
    BOOL _phase7InFlight;
    BOOL _phase7SelectionIsPlaybook;
    BOOL _phase7SelectionIsLedger;
    BOOL _phase7SelectionIsJournal;
    BOOL _phase7SelectionIsApply;
    tapescope::RangeQuery _phase7ReplayRange;
    std::string _phase7DetailBody;
    std::string _phase7SelectedSourceArtifactId;
    std::string _phase7SelectedLinkedAnalysisArtifactId;
    std::string _phase7SelectedLinkedPlaybookArtifactId;
    std::string _phase7SelectedLinkedLedgerArtifactId;
    std::string _phase7SelectedLinkedJournalArtifactId;
    std::vector<tapescope::Phase7AnalyzerProfile> _latestPhase7Profiles;
    std::vector<tapescope::Phase7AnalysisArtifact> _latestPhase7AnalysisArtifacts;
    std::vector<tapescope::Phase7PlaybookArtifact> _latestPhase7PlaybookArtifacts;
    std::vector<tapescope::Phase7ExecutionLedgerArtifact> _latestPhase7ExecutionLedgers;
    std::vector<tapescope::Phase7ExecutionJournalArtifact> _latestPhase7ExecutionJournals;
    std::vector<tapescope::Phase7ExecutionApplyArtifact> _latestPhase7ExecutionApplies;
    std::vector<tapescope::Phase7FindingRecord> _phase7VisibleFindings;
    std::vector<tapescope::Phase7PlaybookAction> _phase7VisibleActions;
    std::vector<tapescope::Phase7ExecutionLedgerEntry> _phase7VisibleLedgerEntries;
    std::vector<tapescope::Phase7ExecutionJournalEntry> _phase7VisibleJournalEntries;
    std::vector<tapescope::Phase7ExecutionApplyEntry> _phase7VisibleApplyEntries;
    std::unique_ptr<TradingRuntimeHost> _phase7RuntimeHost;
    int _phase7RuntimeClientId;

    NSButton* _phase8RefreshButton;
    NSTextField* _phase8BundlePathField;
    NSTextField* _phase8TitleField;
    NSTextField* _phase8CadenceMinutesField;
    NSTextField* _phase8MinimumFindingCountField;
    NSTextField* _phase8RequiredCategoryField;
    NSPopUpButton* _phase8ProfilePopup;
    NSPopUpButton* _phase8MinimumSeverityPopup;
    NSButton* _phase8EnabledButton;
    NSButton* _phase8ChooseBundleButton;
    NSButton* _phase8CreateWatchButton;
    NSButton* _phase8RunDueButton;
    NSButton* _phase8EvaluateWatchButton;
    NSPopUpButton* _phase8TriggerAttentionStatusPopup;
    NSPopUpButton* _phase8TriggerAttentionOpenPopup;
    NSTextField* _phase8AttentionCommentField;
    NSTextField* _phase8SnoozeMinutesField;
    NSButton* _phase8AcknowledgeButton;
    NSButton* _phase8SnoozeButton;
    NSButton* _phase8ResolveButton;
    NSButton* _phase8OpenAnalysisButton;
    NSButton* _phase8OpenSourceArtifactButton;
    NSPopUpButton* _phase8WatchPopup;
    NSPopUpButton* _phase8TriggerPopup;
    NSPopUpButton* _phase8AttentionPopup;
    NSTextField* _phase8StateLabel;
    NSTextView* _phase8TextView;
    BOOL _phase8InFlight;
    NSInteger _phase8SelectionFocus;
    std::string _phase8SelectedWatchArtifactId;
    std::string _phase8SelectedTriggerArtifactId;
    std::string _phase8SelectedAnalysisArtifactId;
    std::string _phase8SelectedSourceArtifactId;
    std::vector<tapescope::Phase8WatchDefinitionArtifact> _latestPhase8WatchDefinitions;
    std::vector<tapescope::Phase8TriggerRunArtifact> _latestPhase8TriggerRuns;
    std::vector<tapescope::Phase8AttentionInboxItem> _latestPhase8AttentionItems;

    std::vector<std::pair<NSTableView*, tapescope::InvestigationPaneController*>> _evidencePaneBindings;
}

- (void)updateBannerAppearanceWithColor:(NSColor*)color;
- (void)updatePollingStatusText;
- (NSButton*)makePaneNavigationButtonWithTitle:(NSString*)title identifier:(NSString*)identifier;
- (void)syncPaneSelectionChrome;
- (void)selectPaneWithIdentifier:(NSString*)identifier;
- (void)paneNavigationPressed:(id)sender;
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
- (void)applyEnrichmentResult:(const tapescope::QueryResult<tapescope::EnrichmentPayload>&)result
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
- (void)fetchOrderCaseEnrichment:(id)sender;
- (void)refreshOrderCaseExternalContext:(id)sender;
- (void)fetchSeekOrder:(id)sender;
- (void)loadReplayWindowFromSeek:(id)sender;
- (void)loadReplayWindowFromOrderCase:(id)sender;
- (void)openSelectedOrderCaseEvidence:(id)sender;

@end

@interface TapeScopeWindowController (ArtifactQueries)

- (void)fetchIncident:(id)sender;
- (void)fetchIncidentEnrichment:(id)sender;
- (void)fetchIncidentExplanation:(id)sender;
- (void)refreshIncidentExternalContext:(id)sender;
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

@interface TapeScopeWindowController (Phase7Queries)

- (NSTabViewItem*)phase7ArtifactsTabItem;
- (void)refreshPhase7Artifacts:(id)sender;
- (void)refreshPhase7DetailText;
- (void)clearPhase7Filters:(id)sender;
- (void)choosePhase7BundlePath:(id)sender;
- (void)runPhase7Analysis:(id)sender;
- (void)buildSelectedPhase7Playbook:(id)sender;
- (void)buildSelectedPhase7ExecutionLedger:(id)sender;
- (void)startSelectedPhase7ExecutionJournal:(id)sender;
- (void)startSelectedPhase7ExecutionApply:(id)sender;
- (void)openPhase7AnalysisArtifactId:(const std::string&)artifactId;
- (void)openPhase7PlaybookArtifactId:(const std::string&)artifactId;
- (void)openPhase7ExecutionLedgerArtifactId:(const std::string&)artifactId;
- (void)openPhase7ExecutionJournalArtifactId:(const std::string&)artifactId;
- (void)openPhase7ExecutionApplyArtifactId:(const std::string&)artifactId;
- (void)openSelectedPhase7Analysis:(id)sender;
- (void)openSelectedPhase7Playbook:(id)sender;
- (void)openSelectedPhase7ExecutionLedger:(id)sender;
- (void)openSelectedPhase7ExecutionJournal:(id)sender;
- (void)openSelectedPhase7ExecutionApply:(id)sender;
- (void)openSelectedPhase7SourceArtifact:(id)sender;
- (void)openSelectedPhase7LinkedAnalysis:(id)sender;
- (void)openSelectedPhase7LinkedLedger:(id)sender;
- (void)openSelectedPhase7LinkedJournal:(id)sender;
- (void)recordSelectedPhase7LedgerReview:(id)sender;
- (void)dispatchSelectedPhase7JournalEntries:(id)sender;
- (void)recordSelectedPhase7JournalEvent:(id)sender;
- (void)recordSelectedPhase7ApplyEvent:(id)sender;
- (void)loadReplayRangeFromPhase7Selection:(id)sender;

@end

@interface TapeScopeWindowController (Phase7Runtime)

- (void)startPhase7RuntimeHost:(id)sender;
- (void)stopPhase7RuntimeHost:(id)sender;
- (void)dispatchSelectedPhase7JournalEntriesViaRuntime:(id)sender;
- (void)reconcileSelectedPhase7JournalEntriesViaRuntime:(id)sender;
- (void)sweepPhase7ExecutionArtifactsViaRuntime:(id)sender;
- (void)syncSelectedPhase7ApplyFromJournal:(id)sender;
- (void)updatePhase7RuntimeControls;
- (void)shutdownPhase7RuntimeHost;

@end

@interface TapeScopeWindowController (Phase8Queries)

- (NSTabViewItem*)phase8InboxTabItem;
- (void)refreshPhase8Inbox:(id)sender;
- (void)choosePhase8BundlePath:(id)sender;
- (void)createPhase8WatchDefinition:(id)sender;
- (void)runDuePhase8Watches:(id)sender;
- (void)evaluateSelectedPhase8Watch:(id)sender;
- (void)acknowledgeSelectedPhase8Attention:(id)sender;
- (void)snoozeSelectedPhase8Attention:(id)sender;
- (void)resolveSelectedPhase8Attention:(id)sender;
- (void)openSelectedPhase8Analysis:(id)sender;
- (void)openSelectedPhase8SourceArtifact:(id)sender;
- (void)phase8WatchSelectionChanged:(id)sender;
- (void)phase8TriggerSelectionChanged:(id)sender;
- (void)phase8AttentionSelectionChanged:(id)sender;
- (void)refreshPhase8DetailText;

@end

@interface TapeScopeWindowController (TableBindings)

- (void)openSelectedSessionReport:(id)sender;
- (void)openSelectedCaseReport:(id)sender;
- (void)openSelectedImportedCase:(id)sender;

@end
