#import "tapescope_window_internal.h"

#include "tapescope_support.h"

#include <algorithm>
#include <exception>

namespace {

using namespace tapescope_support;

constexpr std::size_t kRecentHistoryLimit = 24;
constexpr std::size_t kBundleHistoryLimit = 32;
NSString* const kTapeScopeStateDefaultsKey = @"TapeScopeStateV1";

std::string InvestigationHeadline(const tapescope::InvestigationPayload& payload,
                                  const std::string& fallbackHeadline) {
    if (!payload.headline.empty()) {
        return payload.headline;
    }
    return FirstPresentString(payload.summary, {"headline", "title", "why_it_matters", "what_changed_first"});
}

std::string InvestigationDetail(const tapescope::InvestigationPayload& payload,
                                const std::string& fallbackDetail) {
    if (!payload.detail.empty()) {
        return payload.detail;
    }
    const std::string detail = FirstPresentString(payload.summary,
                                                  {"what_changed_first", "why_it_matters", "headline"});
    if (!detail.empty()) {
        return detail;
    }
    return fallbackDetail;
}

} // namespace

@implementation TapeScopeWindowController (RecentHistory)

- (NSTabViewItem*)recentHistoryTabItem {
    const auto paneWithStack = MakePaneWithStack();
    NSStackView* stack = paneWithStack.stack;
    NSView* pane = paneWithStack.view;

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

- (NSTabViewItem*)bundleHistoryTabItem {
    const auto paneWithStack = MakePaneWithStack();
    NSStackView* stack = paneWithStack.stack;
    NSView* pane = paneWithStack.view;

    [stack addArrangedSubview:MakeIntroLabel(@"Bundle history: reopen exported and imported Phase 6 bundles, reveal their on-disk locations, jump back to source artifacts, and load their replay windows.",
                                             2)];

    NSStackView* controls = MakeControlRow();
    _bundleHistoryOpenButton = [NSButton buttonWithTitle:@"Open Selected"
                                                  target:self
                                                  action:@selector(openSelectedBundleHistory:)];
    _bundleHistoryOpenButton.enabled = NO;
    [controls addArrangedSubview:_bundleHistoryOpenButton];

    _bundleHistoryOpenSourceButton = [NSButton buttonWithTitle:@"Open Source"
                                                        target:self
                                                        action:@selector(openSelectedBundleHistorySource:)];
    _bundleHistoryOpenSourceButton.enabled = NO;
    [controls addArrangedSubview:_bundleHistoryOpenSourceButton];

    _bundleHistoryRevealButton = [NSButton buttonWithTitle:@"Reveal Path"
                                                    target:self
                                                    action:@selector(revealSelectedBundleHistoryPath:)];
    _bundleHistoryRevealButton.enabled = NO;
    [controls addArrangedSubview:_bundleHistoryRevealButton];

    _bundleHistoryLoadRangeButton = [NSButton buttonWithTitle:@"Load Range"
                                                       target:self
                                                       action:@selector(loadReplayRangeFromBundleHistory:)];
    _bundleHistoryLoadRangeButton.enabled = NO;
    [controls addArrangedSubview:_bundleHistoryLoadRangeButton];

    _bundleHistoryClearButton = [NSButton buttonWithTitle:@"Clear History"
                                                   target:self
                                                   action:@selector(clearBundleHistory:)];
    _bundleHistoryClearButton.enabled = NO;
    [controls addArrangedSubview:_bundleHistoryClearButton];
    [stack addArrangedSubview:controls];

    _bundleHistoryStateLabel = MakeLabel(@"Bundle history will populate as you export or import Phase 6 bundles.",
                                         [NSFont systemFontOfSize:12.0 weight:NSFontWeightMedium],
                                         [NSColor secondaryLabelColor]);
    [stack addArrangedSubview:_bundleHistoryStateLabel];

    _bundleHistoryTableView = MakeStandardTableView(self, self);
    AddTableColumn(_bundleHistoryTableView, @"kind", @"kind", 140.0);
    AddTableColumn(_bundleHistoryTableView, @"bundle_id", @"bundle_id", 180.0);
    AddTableColumn(_bundleHistoryTableView, @"headline", @"headline", 450.0);
    ConfigureTablePrimaryAction(_bundleHistoryTableView, self, @selector(openSelectedBundleHistory:));
    [stack addArrangedSubview:MakeTableScrollView(_bundleHistoryTableView, 180.0)];

    [stack addArrangedSubview:MakeSectionLabel(@"Selected Bundle Entry")];

    _bundleHistoryTextView = MakeReadOnlyTextView();
    _bundleHistoryTextView.string = @"Export or import a Phase 6 bundle and it will appear here with its artifact linkage and replay metadata.";
    [stack addArrangedSubview:MakeScrollView(_bundleHistoryTextView, 250.0)];

    NSTabViewItem* item = [[NSTabViewItem alloc] initWithIdentifier:@"BundleHistoryPane"];
    item.label = @"BundleHistoryPane";
    item.view = pane;
    return item;
}

- (tapescope::json)capturePersistentState {
    tapescope::json state = tapescope::json::object();
    if (_tabView.selectedTabViewItem.identifier != nil &&
        [_tabView.selectedTabViewItem.identifier isKindOfClass:[NSString class]]) {
        state["selected_tab"] = ToStdString((NSString*)_tabView.selectedTabViewItem.identifier);
    }
    state["polling_paused"] = (_pollingPaused == YES);
    state["recent_history"] = _recentHistoryItems;
    state["bundle_history"] = _bundleHistoryItems;
    state["overview"] = tapescope::json{{"first_session_seq", ToStdString(_overviewFirstField.stringValue)},
                                        {"last_session_seq", ToStdString(_overviewLastField.stringValue)}};
    state["range"] = tapescope::json{{"first_session_seq", ToStdString(_rangeFirstField.stringValue)},
                                     {"last_session_seq", ToStdString(_rangeLastField.stringValue)}};
    state["quality"] = tapescope::json{{"first_session_seq", ToStdString(_qualityFirstField.stringValue)},
                                       {"last_session_seq", ToStdString(_qualityLastField.stringValue)},
                                       {"include_live_tail", _qualityIncludeLiveTailButton.state == NSControlStateValueOn}};
    state["finding"] = tapescope::json{{"finding_id", ToStdString(_findingIdField.stringValue)}};
    state["anchor"] = tapescope::json{{"anchor_id", ToStdString(_anchorIdField.stringValue)}};
    state["order_lookup"] = tapescope::json{{"anchor_kind",
                                             OrderAnchorTypeKey(OrderAnchorTypeFromIndex(_orderAnchorTypePopup.indexOfSelectedItem))},
                                            {"anchor_value", ToStdString(_orderAnchorInputField.stringValue)}};
    state["order_case"] = tapescope::json{{"anchor_kind",
                                           OrderAnchorTypeKey(OrderAnchorTypeFromIndex(_orderCaseAnchorTypePopup.indexOfSelectedItem))},
                                          {"anchor_value", ToStdString(_orderCaseAnchorInputField.stringValue)}};
    state["seek"] = tapescope::json{{"anchor_kind",
                                     OrderAnchorTypeKey(OrderAnchorTypeFromIndex(_seekAnchorTypePopup.indexOfSelectedItem))},
                                    {"anchor_value", ToStdString(_seekAnchorInputField.stringValue)}};
    state["incident"] = tapescope::json{{"logical_incident_id", ToStdString(_incidentIdField.stringValue)}};
    state["artifact"] = tapescope::json{{"artifact_id", ToStdString(_artifactIdField.stringValue)},
                                        {"export_format", ToStdString(_artifactExportFormatPopup.titleOfSelectedItem)}};
    state["report_inventory"] = tapescope::json{{"bundle_path", ToStdString(_bundleImportPathField.stringValue)}};
    state["phase7"] = tapescope::json{
        {"bundle_path", ToStdString(_phase7BundlePathField.stringValue)},
        {"analysis_profile", ToStdString(_phase7ProfilePopup.titleOfSelectedItem)},
        {"analysis_source_filter", ToStdString(_phase7AnalysisSourceFilterField.stringValue)},
        {"analysis_profile_filter", ToStdString(_phase7AnalysisProfileFilterPopup.titleOfSelectedItem)},
        {"analysis_sort", ToStdString(_phase7AnalysisSortPopup.titleOfSelectedItem)},
        {"playbook_analysis_filter", ToStdString(_phase7PlaybookAnalysisFilterField.stringValue)},
        {"playbook_source_filter", ToStdString(_phase7PlaybookSourceFilterField.stringValue)},
        {"playbook_mode_filter", ToStdString(_phase7PlaybookModeFilterPopup.titleOfSelectedItem)},
        {"playbook_sort", ToStdString(_phase7PlaybookSortPopup.titleOfSelectedItem)},
        {"ledger_status_filter", ToStdString(_phase7LedgerStatusFilterPopup.titleOfSelectedItem)},
        {"ledger_sort", ToStdString(_phase7LedgerSortPopup.titleOfSelectedItem)},
        {"journal_ledger_filter", ToStdString(_phase7JournalLedgerFilterField.stringValue)},
        {"journal_status_filter", ToStdString(_phase7JournalStatusFilterPopup.titleOfSelectedItem)},
        {"journal_sort", ToStdString(_phase7JournalSortPopup.titleOfSelectedItem)},
        {"journal_actor", ToStdString(_phase7JournalActorField.stringValue)},
        {"execution_capability", ToStdString(_phase7ExecutionCapabilityField.stringValue)},
        {"execution_status", ToStdString(_phase7ExecutionStatusPopup.titleOfSelectedItem)},
        {"execution_actor", ToStdString(_phase7ExecutionActorField.stringValue)}
    };
    return state;
}

- (void)persistApplicationState {
    const tapescope::json state = [self capturePersistentState];
    [[NSUserDefaults standardUserDefaults] setObject:ToNSString(state.dump())
                                              forKey:kTapeScopeStateDefaultsKey];
}

- (void)restoreApplicationState {
    NSString* raw = [[NSUserDefaults standardUserDefaults] stringForKey:kTapeScopeStateDefaultsKey];
    if (raw == nil || raw.length == 0) {
        return;
    }

    tapescope::json state;
    try {
        state = tapescope::json::parse(ToStdString(raw));
    } catch (const std::exception&) {
        return;
    }
    if (!state.is_object()) {
        return;
    }

    _pollingPaused = state.value("polling_paused", false);

    const tapescope::json overview = state.value("overview", tapescope::json::object());
    _overviewFirstField.stringValue = ToNSString(overview.value("first_session_seq", std::string()));
    _overviewLastField.stringValue = ToNSString(overview.value("last_session_seq", std::string()));

    const tapescope::json range = state.value("range", tapescope::json::object());
    _rangeFirstField.stringValue = ToNSString(range.value("first_session_seq", std::string()));
    _rangeLastField.stringValue = ToNSString(range.value("last_session_seq", std::string()));

    const tapescope::json quality = state.value("quality", tapescope::json::object());
    _qualityFirstField.stringValue = ToNSString(quality.value("first_session_seq", std::string()));
    _qualityLastField.stringValue = ToNSString(quality.value("last_session_seq", std::string()));
    _qualityIncludeLiveTailButton.state = quality.value("include_live_tail", false)
                                              ? NSControlStateValueOn
                                              : NSControlStateValueOff;

    const tapescope::json finding = state.value("finding", tapescope::json::object());
    _findingIdField.stringValue = ToNSString(finding.value("finding_id", std::string()));

    const tapescope::json anchor = state.value("anchor", tapescope::json::object());
    _anchorIdField.stringValue = ToNSString(anchor.value("anchor_id", std::string()));

    const tapescope::json orderLookup = state.value("order_lookup", tapescope::json::object());
    [_orderAnchorTypePopup selectItemAtIndex:OrderAnchorTypeIndexForKey(orderLookup.value("anchor_kind", std::string("traceId")))];
    [self orderAnchorTypeChanged:nil];
    _orderAnchorInputField.stringValue = ToNSString(orderLookup.value("anchor_value", std::string()));

    const tapescope::json orderCase = state.value("order_case", tapescope::json::object());
    [_orderCaseAnchorTypePopup selectItemAtIndex:OrderAnchorTypeIndexForKey(orderCase.value("anchor_kind", std::string("traceId")))];
    [self orderCaseAnchorTypeChanged:nil];
    _orderCaseAnchorInputField.stringValue = ToNSString(orderCase.value("anchor_value", std::string()));

    const tapescope::json seek = state.value("seek", tapescope::json::object());
    [_seekAnchorTypePopup selectItemAtIndex:OrderAnchorTypeIndexForKey(seek.value("anchor_kind", std::string("traceId")))];
    [self seekAnchorTypeChanged:nil];
    _seekAnchorInputField.stringValue = ToNSString(seek.value("anchor_value", std::string()));

    const tapescope::json incident = state.value("incident", tapescope::json::object());
    _incidentIdField.stringValue = ToNSString(incident.value("logical_incident_id", std::string()));

    const tapescope::json artifact = state.value("artifact", tapescope::json::object());
    _artifactIdField.stringValue = ToNSString(artifact.value("artifact_id", std::string()));
    const std::string exportFormat = artifact.value("export_format", std::string("markdown"));
    if (!exportFormat.empty()) {
        [_artifactExportFormatPopup selectItemWithTitle:ToNSString(exportFormat)];
    }

    const tapescope::json reportInventory = state.value("report_inventory", tapescope::json::object());
    _bundleImportPathField.stringValue = ToNSString(reportInventory.value("bundle_path", std::string()));

    const tapescope::json phase7 = state.value("phase7", tapescope::json::object());
    _phase7BundlePathField.stringValue = ToNSString(phase7.value("bundle_path", std::string()));
    const std::string phase7Profile = phase7.value("analysis_profile", std::string());
    if (!phase7Profile.empty()) {
        [_phase7ProfilePopup selectItemWithTitle:ToNSString(phase7Profile)];
    }
    _phase7AnalysisSourceFilterField.stringValue = ToNSString(phase7.value("analysis_source_filter", std::string()));
    const std::string phase7AnalysisProfileFilter = phase7.value("analysis_profile_filter", std::string());
    if (!phase7AnalysisProfileFilter.empty()) {
        [_phase7AnalysisProfileFilterPopup selectItemWithTitle:ToNSString(phase7AnalysisProfileFilter)];
    }
    const std::string phase7AnalysisSort = phase7.value("analysis_sort", std::string());
    if (!phase7AnalysisSort.empty()) {
        [_phase7AnalysisSortPopup selectItemWithTitle:ToNSString(phase7AnalysisSort)];
    }
    _phase7PlaybookAnalysisFilterField.stringValue = ToNSString(phase7.value("playbook_analysis_filter", std::string()));
    _phase7PlaybookSourceFilterField.stringValue = ToNSString(phase7.value("playbook_source_filter", std::string()));
    const std::string phase7PlaybookModeFilter = phase7.value("playbook_mode_filter", std::string());
    if (!phase7PlaybookModeFilter.empty()) {
        [_phase7PlaybookModeFilterPopup selectItemWithTitle:ToNSString(phase7PlaybookModeFilter)];
    }
    const std::string phase7PlaybookSort = phase7.value("playbook_sort", std::string());
    if (!phase7PlaybookSort.empty()) {
        [_phase7PlaybookSortPopup selectItemWithTitle:ToNSString(phase7PlaybookSort)];
    }
    const std::string phase7LedgerStatusFilter = phase7.value("ledger_status_filter", std::string());
    if (!phase7LedgerStatusFilter.empty()) {
        [_phase7LedgerStatusFilterPopup selectItemWithTitle:ToNSString(phase7LedgerStatusFilter)];
    }
    const std::string phase7LedgerSort = phase7.value("ledger_sort", std::string());
    if (!phase7LedgerSort.empty()) {
        [_phase7LedgerSortPopup selectItemWithTitle:ToNSString(phase7LedgerSort)];
    }
    _phase7JournalLedgerFilterField.stringValue = ToNSString(phase7.value("journal_ledger_filter", std::string()));
    const std::string phase7JournalStatusFilter = phase7.value("journal_status_filter", std::string());
    if (!phase7JournalStatusFilter.empty()) {
        [_phase7JournalStatusFilterPopup selectItemWithTitle:ToNSString(phase7JournalStatusFilter)];
    }
    const std::string phase7JournalSort = phase7.value("journal_sort", std::string());
    if (!phase7JournalSort.empty()) {
        [_phase7JournalSortPopup selectItemWithTitle:ToNSString(phase7JournalSort)];
    }
    _phase7JournalActorField.stringValue = ToNSString(phase7.value("journal_actor", std::string("tapescope")));
    _phase7ExecutionCapabilityField.stringValue =
        ToNSString(phase7.value("execution_capability", std::string("manual_review")));
    const std::string phase7ExecutionStatus = phase7.value("execution_status", std::string());
    if (!phase7ExecutionStatus.empty()) {
        [_phase7ExecutionStatusPopup selectItemWithTitle:ToNSString(phase7ExecutionStatus)];
    }
    _phase7ExecutionActorField.stringValue = ToNSString(phase7.value("execution_actor", std::string("tapescope")));

    _recentHistoryItems.clear();
    const tapescope::json recentHistory = state.value("recent_history", tapescope::json::array());
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
        _recentTextView.string = ToNSString(DescribeRecentHistoryEntry(_recentHistoryItems.front()));
    }

    _bundleHistoryItems.clear();
    const tapescope::json bundleHistory = state.value("bundle_history", tapescope::json::array());
    if (bundleHistory.is_array()) {
        for (const auto& item : bundleHistory) {
            if (item.is_object()) {
                _bundleHistoryItems.push_back(item);
            }
        }
    }
    [_bundleHistoryTableView reloadData];
    _bundleHistoryClearButton.enabled = !_bundleHistoryItems.empty();
    _bundleHistoryOpenButton.enabled = NO;
    _bundleHistoryOpenSourceButton.enabled = NO;
    _bundleHistoryRevealButton.enabled = NO;
    _bundleHistoryLoadRangeButton.enabled = NO;
    if (!_bundleHistoryItems.empty()) {
        _bundleHistoryStateLabel.stringValue = @"Restored bundle history from the last TapeScope session.";
        _bundleHistoryStateLabel.textColor = TapeInkMutedColor();
        [_bundleHistoryTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0] byExtendingSelection:NO];
        _bundleHistoryTextView.string = ToNSString(DescribeRecentHistoryEntry(_bundleHistoryItems.front()));
    }

    const std::string selectedTab = state.value("selected_tab", std::string());
    if (!selectedTab.empty()) {
        [_tabView selectTabViewItemWithIdentifier:ToNSString(selectedTab)];
    }
    [self updatePollingStatusText];
}

- (void)recordRecentHistoryEntry:(tapescope::json)entry {
    if (!entry.is_object()) {
        return;
    }

    const std::string key = entry.value("kind", std::string()) + "|" + entry.value("target_id", std::string());
    _recentHistoryItems.erase(std::remove_if(_recentHistoryItems.begin(),
                                             _recentHistoryItems.end(),
                                             [&](const tapescope::json& item) {
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
    _recentOpenButton.enabled = YES;
    [_recentTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0] byExtendingSelection:NO];
    _recentTextView.string = ToNSString(DescribeRecentHistoryEntry(_recentHistoryItems.front()));
    [self persistApplicationState];
}

- (void)recordBundleHistoryEntry:(tapescope::json)entry {
    if (!entry.is_object()) {
        return;
    }

    const std::string key = entry.value("kind", std::string()) + "|" + entry.value("target_id", std::string());
    _bundleHistoryItems.erase(std::remove_if(_bundleHistoryItems.begin(),
                                             _bundleHistoryItems.end(),
                                             [&](const tapescope::json& item) {
                                                 return (item.value("kind", std::string()) + "|" +
                                                         item.value("target_id", std::string())) == key;
                                             }),
                              _bundleHistoryItems.end());
    _bundleHistoryItems.insert(_bundleHistoryItems.begin(), std::move(entry));
    if (_bundleHistoryItems.size() > kBundleHistoryLimit) {
        _bundleHistoryItems.resize(kBundleHistoryLimit);
    }
    [_bundleHistoryTableView reloadData];
    _bundleHistoryClearButton.enabled = !_bundleHistoryItems.empty();
    if (_bundleHistoryItems.empty()) {
        _bundleHistoryStateLabel.stringValue = @"Bundle history is empty.";
        _bundleHistoryStateLabel.textColor = TapeInkMutedColor();
        _bundleHistoryOpenButton.enabled = NO;
        _bundleHistoryOpenSourceButton.enabled = NO;
        _bundleHistoryRevealButton.enabled = NO;
        _bundleHistoryLoadRangeButton.enabled = NO;
        _bundleHistoryTextView.string = @"Export or import a Phase 6 bundle to build bundle history.";
        return;
    }
    _bundleHistoryStateLabel.stringValue = @"Bundle history updated.";
    _bundleHistoryStateLabel.textColor = [NSColor systemGreenColor];
    _bundleHistoryOpenButton.enabled = YES;
    _bundleHistoryRevealButton.enabled = YES;
    [_bundleHistoryTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0] byExtendingSelection:NO];
    _bundleHistoryTextView.string = ToNSString(DescribeRecentHistoryEntry(_bundleHistoryItems.front()));
    [self persistApplicationState];
}

- (void)recordRecentHistoryForKind:(const std::string&)kind
                          targetId:(const std::string&)targetId
                           payload:(const tapescope::InvestigationPayload&)payload
                     fallbackTitle:(const std::string&)fallbackTitle
                    fallbackDetail:(const std::string&)fallbackDetail
                          metadata:(const tapescope::json&)metadata {
    tapescope::json entry = tapescope::json::object();
    entry["kind"] = kind;
    entry["target_id"] = targetId;
    entry["headline"] = InvestigationHeadline(payload, fallbackTitle);
    entry["detail"] = InvestigationDetail(payload, fallbackDetail);
    if (!payload.artifactId.empty()) {
        entry["artifact_id"] = payload.artifactId;
    }

    std::uint64_t firstSessionSeq = payload.summary.value("first_session_seq",
                                                          payload.summary.value("from_session_seq", 0ULL));
    std::uint64_t lastSessionSeq = payload.summary.value("last_session_seq",
                                                         payload.summary.value("to_session_seq", 0ULL));
    if ((firstSessionSeq == 0 || lastSessionSeq < firstSessionSeq) && payload.replayRange.has_value()) {
        firstSessionSeq = payload.replayRange->firstSessionSeq;
        lastSessionSeq = payload.replayRange->lastSessionSeq;
    }
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

- (void)openRecentHistoryEntry:(const tapescope::json&)entry {
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
    if (kind == "phase7_analysis") {
        const std::string artifactId = entry.value("artifact_id", entry.value("target_id", std::string()));
        if (!artifactId.empty()) {
            [_tabView selectTabViewItemWithIdentifier:@"Phase7Pane"];
            [self openPhase7AnalysisArtifactId:artifactId];
        }
        return;
    }
    if (kind == "phase7_playbook") {
        const std::string artifactId = entry.value("artifact_id", entry.value("target_id", std::string()));
        if (!artifactId.empty()) {
            [_tabView selectTabViewItemWithIdentifier:@"Phase7Pane"];
            [self openPhase7PlaybookArtifactId:artifactId];
        }
        return;
    }
    if (kind == "phase7_execution_ledger") {
        const std::string artifactId = entry.value("artifact_id", entry.value("target_id", std::string()));
        if (!artifactId.empty()) {
            [_tabView selectTabViewItemWithIdentifier:@"Phase7Pane"];
            [self openPhase7ExecutionLedgerArtifactId:artifactId];
        }
        return;
    }
    if (kind == "phase7_execution_journal") {
        const std::string artifactId = entry.value("artifact_id", entry.value("target_id", std::string()));
        if (!artifactId.empty()) {
            [_tabView selectTabViewItemWithIdentifier:@"Phase7Pane"];
            [self openPhase7ExecutionJournalArtifactId:artifactId];
        }
        return;
    }
    if (kind == "phase7_execution_apply") {
        const std::string artifactId = entry.value("artifact_id", entry.value("target_id", std::string()));
        if (!artifactId.empty()) {
            [_tabView selectTabViewItemWithIdentifier:@"Phase7Pane"];
            [self openPhase7ExecutionApplyArtifactId:artifactId];
        }
        return;
    }
    if (kind == "bundle") {
        const std::string bundlePath = entry.value("bundle_path", std::string());
        if (!bundlePath.empty()) {
            _bundleImportPathField.stringValue = ToNSString(bundlePath);
            [_tabView selectTabViewItemWithIdentifier:@"BundleHistoryPane"];
            [self revealSelectedBundleHistoryPath:nil];
        }
        return;
    }
    if (kind == "session_bundle" || kind == "case_bundle" || kind == "imported_case_bundle") {
        const std::string artifactId = entry.value("artifact_id", std::string());
        if (!artifactId.empty()) {
            _artifactIdField.stringValue = ToNSString(artifactId);
            [_tabView selectTabViewItemWithIdentifier:@"ArtifactPane"];
            [self fetchArtifact:nil];
            return;
        }
        const std::string bundlePath = entry.value("bundle_path", std::string());
        if (!bundlePath.empty()) {
            _bundleImportPathField.stringValue = ToNSString(bundlePath);
            [_tabView selectTabViewItemWithIdentifier:@"BundleHistoryPane"];
            [self revealSelectedBundleHistoryPath:nil];
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

- (void)openSelectedBundleHistory:(id)sender {
    (void)sender;
    const NSInteger selected = _bundleHistoryTableView.selectedRow;
    if (selected < 0 || static_cast<std::size_t>(selected) >= _bundleHistoryItems.size()) {
        _bundleHistoryStateLabel.stringValue = @"Select a bundle-history row first.";
        _bundleHistoryStateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    [self openRecentHistoryEntry:_bundleHistoryItems.at(static_cast<std::size_t>(selected))];
}

- (void)openSelectedBundleHistorySource:(id)sender {
    (void)sender;
    const NSInteger selected = _bundleHistoryTableView.selectedRow;
    if (selected < 0 || static_cast<std::size_t>(selected) >= _bundleHistoryItems.size()) {
        _bundleHistoryStateLabel.stringValue = @"Select a bundle-history row first.";
        _bundleHistoryStateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    const auto& entry = _bundleHistoryItems.at(static_cast<std::size_t>(selected));
    const std::string artifactId = entry.value("source_artifact_id", std::string());
    if (artifactId.empty()) {
        _bundleHistoryStateLabel.stringValue = @"Selected bundle entry is missing a source artifact id.";
        _bundleHistoryStateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    _artifactIdField.stringValue = ToNSString(artifactId);
    [_tabView selectTabViewItemWithIdentifier:@"ArtifactPane"];
    [self fetchArtifact:nil];
}

- (void)revealSelectedBundleHistoryPath:(id)sender {
    (void)sender;
    const NSInteger selected = _bundleHistoryTableView.selectedRow;
    if (selected < 0 || static_cast<std::size_t>(selected) >= _bundleHistoryItems.size()) {
        _bundleHistoryStateLabel.stringValue = @"Select a bundle-history row first.";
        _bundleHistoryStateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    const auto& entry = _bundleHistoryItems.at(static_cast<std::size_t>(selected));
    const std::string bundlePath = entry.value("bundle_path", std::string());
    if (bundlePath.empty()) {
        _bundleHistoryStateLabel.stringValue = @"Selected bundle entry is missing a bundle path.";
        _bundleHistoryStateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    _bundleImportPathField.stringValue = ToNSString(bundlePath);
    [self revealSelectedBundlePath:nil];
    _bundleHistoryStateLabel.stringValue = _reportInventoryStateLabel.stringValue;
    _bundleHistoryStateLabel.textColor = _reportInventoryStateLabel.textColor;
}

- (void)loadReplayRangeFromBundleHistory:(id)sender {
    (void)sender;
    const NSInteger selected = _bundleHistoryTableView.selectedRow;
    if (selected < 0 || static_cast<std::size_t>(selected) >= _bundleHistoryItems.size()) {
        _bundleHistoryStateLabel.stringValue = @"Select a bundle-history row first.";
        _bundleHistoryStateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    const auto& entry = _bundleHistoryItems.at(static_cast<std::size_t>(selected));
    const std::uint64_t firstSessionSeq = entry.value("first_session_seq", 0ULL);
    const std::uint64_t lastSessionSeq = entry.value("last_session_seq", 0ULL);
    tapescope::RangeQuery range;
    range.firstSessionSeq = firstSessionSeq;
    range.lastSessionSeq = lastSessionSeq;
    [self loadReplayRange:range
                available:(firstSessionSeq > 0 && lastSessionSeq >= firstSessionSeq)
               stateLabel:_bundleHistoryStateLabel
           missingMessage:@"Selected bundle entry is missing a replayable session_seq window."];
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

- (void)clearBundleHistory:(id)sender {
    (void)sender;
    _bundleHistoryItems.clear();
    [_bundleHistoryTableView reloadData];
    _bundleHistoryOpenButton.enabled = NO;
    _bundleHistoryOpenSourceButton.enabled = NO;
    _bundleHistoryRevealButton.enabled = NO;
    _bundleHistoryLoadRangeButton.enabled = NO;
    _bundleHistoryClearButton.enabled = NO;
    _bundleHistoryStateLabel.stringValue = @"Bundle history cleared.";
    _bundleHistoryStateLabel.textColor = TapeInkMutedColor();
    _bundleHistoryTextView.string = @"Export or import a Phase 6 bundle to repopulate bundle history.";
    [self persistApplicationState];
}

@end
