#import "tapescope_window_internal.h"

#include "tapescope_support.h"

#include <algorithm>
#include <optional>
#include <sstream>

namespace {

using namespace tapescope_support;

std::optional<tapescope::RangeQuery> ReplayRangeFromPhase7Context(const tapescope::json& replayContext) {
    if (!replayContext.is_object()) {
        return std::nullopt;
    }
    const tapescope::json requestedWindow = replayContext.value("requested_window", tapescope::json::object());
    if (!requestedWindow.is_object()) {
        return std::nullopt;
    }
    const std::uint64_t firstSessionSeq = requestedWindow.value("first_session_seq", 0ULL);
    const std::uint64_t lastSessionSeq = requestedWindow.value("last_session_seq", 0ULL);
    if (firstSessionSeq == 0 || lastSessionSeq < firstSessionSeq) {
        return std::nullopt;
    }
    return tapescope::RangeQuery{firstSessionSeq, lastSessionSeq};
}

std::string DescribePhase7Profiles(const tapescope::QueryResult<std::vector<tapescope::Phase7AnalyzerProfile>>& result) {
    if (!result.ok()) {
        return "Phase 7 profile discovery failed.\n" + tapescope::QueryClient::describeError(result.error);
    }

    std::ostringstream out;
    out << "Supported Phase 7 analyzer profiles: " << result.value.size() << "\n"
        << "Playbook apply stays deferred in TapeScope; this pane only reopens durable analysis and dry-run playbook artifacts.\n";
    for (const auto& profile : result.value) {
        out << "\n- " << profile.analysisProfile;
        if (profile.defaultProfile) {
            out << " (default)";
        }
        out << "\n  " << profile.title
            << "\n  " << profile.summary;
        if (!profile.supportedSourceBundleTypes.empty()) {
            out << "\n  source_bundle_types: ";
            for (std::size_t index = 0; index < profile.supportedSourceBundleTypes.size(); ++index) {
                if (index > 0) {
                    out << ", ";
                }
                out << profile.supportedSourceBundleTypes[index];
            }
        }
        if (!profile.findingCategories.empty()) {
            out << "\n  finding_categories: ";
            for (std::size_t index = 0; index < profile.findingCategories.size(); ++index) {
                if (index > 0) {
                    out << ", ";
                }
                out << profile.findingCategories[index];
            }
        }
        out << "\n";
    }
    return out.str();
}

std::string DescribePhase7AnalysisArtifact(const tapescope::Phase7AnalysisArtifact& artifact) {
    std::ostringstream out;
    out << tape_phase7::analysisArtifactMarkdown(artifact);
    if (!artifact.generatedAtUtc.empty()) {
        out << "\nGenerated at: " << artifact.generatedAtUtc << "\n";
    }
    if (artifact.manifest.is_object()) {
        out << "\nManifest:\n" << artifact.manifest.dump(2) << "\n";
    }
    return out.str();
}

std::string DescribePhase7PlaybookArtifact(const tapescope::Phase7PlaybookArtifact& artifact) {
    std::ostringstream out;
    out << tape_phase7::playbookArtifactMarkdown(artifact);
    out << "\nExecution policy: apply remains deferred; this playbook is a guarded planning artifact only.\n";
    if (!artifact.generatedAtUtc.empty()) {
        out << "Generated at: " << artifact.generatedAtUtc << "\n";
    }
    if (artifact.manifest.is_object()) {
        out << "\nManifest:\n" << artifact.manifest.dump(2) << "\n";
    }
    return out.str();
}

std::string DescribePhase7ExecutionLedgerArtifact(const tapescope::Phase7ExecutionLedgerArtifact& artifact) {
    std::ostringstream out;
    out << tape_phase7::executionLedgerArtifactMarkdown(artifact);
    if (!artifact.generatedAtUtc.empty()) {
        out << "\nGenerated at: " << artifact.generatedAtUtc << "\n";
    }
    if (artifact.manifest.is_object()) {
        out << "\nManifest:\n" << artifact.manifest.dump(2) << "\n";
    }
    return out.str();
}

std::string DescribePhase7AnalysisRun(const std::string& bundlePath,
                                      const tapescope::QueryResult<tapescope::Phase7AnalysisRunPayload>& result) {
    if (!result.ok()) {
        return "Phase 7 analyzer run failed for " + bundlePath + "\n" +
               tapescope::QueryClient::describeError(result.error);
    }
    std::ostringstream out;
    out << "Phase 7 analyzer run for " << bundlePath << "\n"
        << "artifact_status: " << (result.value.created ? "created" : "reused") << "\n\n"
        << DescribePhase7AnalysisArtifact(result.value.artifact);
    return out.str();
}

std::string DescribePhase7PlaybookBuild(const std::string& analysisArtifactId,
                                        const tapescope::QueryResult<tapescope::Phase7PlaybookBuildPayload>& result) {
    if (!result.ok()) {
        return "Phase 7 dry-run playbook build failed for " + analysisArtifactId + "\n" +
               tapescope::QueryClient::describeError(result.error);
    }
    std::ostringstream out;
    out << "Phase 7 dry-run playbook build for " << analysisArtifactId << "\n"
        << "artifact_status: " << (result.value.created ? "created" : "reused") << "\n\n"
        << DescribePhase7PlaybookArtifact(result.value.artifact);
    return out.str();
}

std::string DescribePhase7ExecutionLedgerBuild(
    const std::string& playbookArtifactId,
    const tapescope::QueryResult<tapescope::Phase7ExecutionLedgerBuildPayload>& result) {
    if (!result.ok()) {
        return "Phase 7 execution ledger build failed for " + playbookArtifactId + "\n" +
               tapescope::QueryClient::describeError(result.error);
    }
    std::ostringstream out;
    out << "Phase 7 execution ledger build for " << playbookArtifactId << "\n"
        << "artifact_status: " << (result.value.created ? "created" : "reused") << "\n\n"
        << DescribePhase7ExecutionLedgerArtifact(result.value.artifact);
    return out.str();
}

std::string SourceArtifactIdForPlaybook(const tapescope::Phase7PlaybookArtifact& artifact,
                                        const std::vector<tapescope::Phase7AnalysisArtifact>& analysisArtifacts) {
    for (const auto& analysis : analysisArtifacts) {
        if (analysis.analysisArtifact.artifactId == artifact.analysisArtifact.artifactId) {
            return analysis.sourceArtifact.artifactId;
        }
    }
    return {};
}

std::string Phase7DetailPlaceholder() {
    return "Refresh Phase 7 artifacts to browse durable analyses and dry-run playbooks created from portable bundles.";
}

std::string DescribePhase7Finding(const tapescope::Phase7FindingRecord& finding) {
    std::ostringstream out;
    out << "finding_id: " << finding.findingId << "\n"
        << "severity: " << finding.severity << "\n"
        << "category: " << finding.category << "\n"
        << "summary: " << finding.summary << "\n";
    if (finding.evidenceRefs.is_array() && !finding.evidenceRefs.empty()) {
        out << "evidence_refs:\n" << finding.evidenceRefs.dump(2) << "\n";
    }
    return out.str();
}

std::string DescribePhase7Action(const tapescope::Phase7PlaybookAction& action) {
    std::ostringstream out;
    out << "action_id: " << action.actionId << "\n"
        << "action_type: " << action.actionType << "\n"
        << "finding_id: " << action.findingId << "\n"
        << "title: " << action.title << "\n"
        << "summary: " << action.summary << "\n";
    if (action.suggestedTools.is_array() && !action.suggestedTools.empty()) {
        out << "suggested_tools:\n" << action.suggestedTools.dump(2) << "\n";
    }
    return out.str();
}

std::vector<std::string> SelectedPhase7FindingIds(NSTableView* tableView,
                                                  const std::vector<tapescope::Phase7FindingRecord>& findings) {
    std::vector<std::string> selectedIds;
    if (tableView == nil) {
        return selectedIds;
    }
    NSIndexSet* indexes = tableView.selectedRowIndexes;
    for (NSUInteger idx = indexes.firstIndex; idx != NSNotFound; idx = [indexes indexGreaterThanIndex:idx]) {
        if (idx < findings.size()) {
            selectedIds.push_back(findings.at(idx).findingId);
        }
    }
    return selectedIds;
}

std::vector<std::string> SelectedPhase7ActionIds(NSTableView* tableView,
                                                 const std::vector<tapescope::Phase7PlaybookAction>& actions) {
    std::vector<std::string> selectedIds;
    if (tableView == nil) {
        return selectedIds;
    }
    NSIndexSet* indexes = tableView.selectedRowIndexes;
    for (NSUInteger idx = indexes.firstIndex; idx != NSNotFound; idx = [indexes indexGreaterThanIndex:idx]) {
        if (idx < actions.size()) {
            selectedIds.push_back(actions.at(idx).actionId);
        }
    }
    return selectedIds;
}

NSIndexSet* IndexSetForPhase7FindingIds(const std::vector<std::string>& selectedIds,
                                        const std::vector<tapescope::Phase7FindingRecord>& findings) {
    NSMutableIndexSet* indexes = [NSMutableIndexSet indexSet];
    if (!selectedIds.empty()) {
        for (std::size_t index = 0; index < findings.size(); ++index) {
            if (std::find(selectedIds.begin(), selectedIds.end(), findings[index].findingId) != selectedIds.end()) {
                [indexes addIndex:index];
            }
        }
    }
    return indexes;
}

NSIndexSet* IndexSetForPhase7ActionIds(const std::vector<std::string>& selectedIds,
                                       const std::vector<tapescope::Phase7PlaybookAction>& actions) {
    NSMutableIndexSet* indexes = [NSMutableIndexSet indexSet];
    if (!selectedIds.empty()) {
        for (std::size_t index = 0; index < actions.size(); ++index) {
            if (std::find(selectedIds.begin(), selectedIds.end(), actions[index].actionId) != selectedIds.end()) {
                [indexes addIndex:index];
            }
        }
    }
    return indexes;
}

std::string DescribeSelectedPhase7Findings(const std::vector<tapescope::Phase7FindingRecord>& findings,
                                           NSIndexSet* selectedIndexes) {
    if (findings.empty()) {
        return "No findings were emitted for this analysis artifact.";
    }
    if (selectedIndexes == nil || selectedIndexes.count == 0) {
        std::ostringstream out;
        out << "Findings available: " << findings.size()
            << ". Select one or more finding rows to scope the dry-run playbook; leaving the table unselected will include every finding.\n";
        return out.str();
    }
    std::ostringstream out;
    out << "Selected findings: " << selectedIndexes.count << "\n";
    NSUInteger rendered = 0;
    for (NSUInteger idx = selectedIndexes.firstIndex; idx != NSNotFound; idx = [selectedIndexes indexGreaterThanIndex:idx]) {
        if (idx >= findings.size()) {
            continue;
        }
        if (rendered > 0) {
            out << "\n";
        }
        out << DescribePhase7Finding(findings.at(idx));
        ++rendered;
        if (rendered >= 3 && selectedIndexes.count > 3) {
            out << "\n… " << (selectedIndexes.count - rendered) << " more selected finding(s)";
            break;
        }
    }
    return out.str();
}

std::string DescribeSelectedPhase7Actions(const std::vector<tapescope::Phase7PlaybookAction>& actions,
                                          NSIndexSet* selectedIndexes) {
    if (actions.empty()) {
        return "No guarded actions were planned for this dry-run playbook.";
    }
    if (selectedIndexes == nil || selectedIndexes.count == 0) {
        std::ostringstream out;
        out << "Planned actions available: " << actions.size()
            << ". Select action rows to inspect the guarded plan details.\n";
        return out.str();
    }
    std::ostringstream out;
    out << "Selected actions: " << selectedIndexes.count << "\n";
    NSUInteger rendered = 0;
    for (NSUInteger idx = selectedIndexes.firstIndex; idx != NSNotFound; idx = [selectedIndexes indexGreaterThanIndex:idx]) {
        if (idx >= actions.size()) {
            continue;
        }
        if (rendered > 0) {
            out << "\n";
        }
        out << DescribePhase7Action(actions.at(idx));
        ++rendered;
        if (rendered >= 3 && selectedIndexes.count > 3) {
            out << "\n… " << (selectedIndexes.count - rendered) << " more selected action(s)";
            break;
        }
    }
    return out.str();
}

void UpdatePhase7BuildPlaybookButtonTitle(NSButton* button, std::size_t selectedCount) {
    if (button == nil) {
        return;
    }
    if (selectedCount == 0) {
        button.title = @"Build Dry-Run Playbook (All Findings)";
    } else if (selectedCount == 1) {
        button.title = @"Build Dry-Run Playbook (1 Selected)";
    } else {
        button.title = [NSString stringWithFormat:@"Build Dry-Run Playbook (%lu Selected)",
                                                  static_cast<unsigned long>(selectedCount)];
    }
}

std::string SelectedPhase7AnalysisProfileFilter(NSPopUpButton* popup) {
    if (popup == nil || popup.titleOfSelectedItem == nil) {
        return {};
    }
    const std::string value = ToStdString(popup.titleOfSelectedItem);
    return value == "All Profiles" ? std::string() : value;
}

std::string SelectedPhase7AnalysisSort(NSPopUpButton* popup) {
    if (popup == nil || popup.titleOfSelectedItem == nil) {
        return "generated_at_desc";
    }
    const std::string value = ToStdString(popup.titleOfSelectedItem);
    if (value == "Most Findings") {
        return "finding_count_desc";
    }
    if (value == "Profile") {
        return "analysis_profile_asc";
    }
    if (value == "Source Artifact") {
        return "source_artifact_asc";
    }
    return "generated_at_desc";
}

std::string SelectedPhase7PlaybookModeFilter(NSPopUpButton* popup) {
    if (popup == nil || popup.titleOfSelectedItem == nil) {
        return {};
    }
    const std::string value = ToStdString(popup.titleOfSelectedItem);
    return value == "All Modes" ? std::string() : value;
}

std::string SelectedPhase7PlaybookSort(NSPopUpButton* popup) {
    if (popup == nil || popup.titleOfSelectedItem == nil) {
        return "generated_at_desc";
    }
    const std::string value = ToStdString(popup.titleOfSelectedItem);
    if (value == "Most Actions") {
        return "planned_action_count_desc";
    }
    if (value == "Most Filtered Findings") {
        return "filtered_finding_count_desc";
    }
    if (value == "Mode") {
        return "mode_asc";
    }
    if (value == "Source Artifact") {
        return "source_artifact_asc";
    }
    return "generated_at_desc";
}

std::string DescribePhase7InventoryStatus(const tapescope::Phase7AnalysisInventoryPayload& analyses,
                                          const tapescope::Phase7PlaybookInventoryPayload& playbooks) {
    std::ostringstream out;
    out << "Phase 7 inventory loaded. "
        << "Analyses: " << analyses.artifacts.size() << "/" << analyses.matchedCount
        << " matched. Playbooks: " << playbooks.artifacts.size() << "/" << playbooks.matchedCount
        << " matched. Execution ledgers are prepared from selected playbooks and reopen through recent history.";
    return out.str();
}

} // namespace

@implementation TapeScopeWindowController (Phase7Queries)

- (NSTabViewItem*)phase7ArtifactsTabItem {
    const auto paneWithStack = MakePaneWithStack();
    NSStackView* stack = paneWithStack.stack;
    NSView* pane = paneWithStack.view;

    [stack addArrangedSubview:MakeIntroLabel(@"Phase 7 artifact inventory: reopen durable local analysis and playbook artifacts created from Phase 6 bundles, then prepare review-only execution ledgers from guarded playbooks. TapeScope keeps playbook apply deferred and only surfaces audit/reopen flows here.",
                                             2)];

    NSStackView* controls = MakeControlRow();
    _phase7RefreshButton = [NSButton buttonWithTitle:@"Refresh Phase 7"
                                              target:self
                                              action:@selector(refreshPhase7Artifacts:)];
    [controls addArrangedSubview:_phase7RefreshButton];

    [controls addArrangedSubview:MakeLabel(@"bundle_path",
                                           [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                           [NSColor secondaryLabelColor])];
    _phase7BundlePathField = MakeMonospacedField(320.0, nil, @"Choose a Phase 6 bundle for local analysis");
    [controls addArrangedSubview:_phase7BundlePathField];

    _phase7ChooseBundleButton = [NSButton buttonWithTitle:@"Choose Bundle…"
                                                   target:self
                                                   action:@selector(choosePhase7BundlePath:)];
    [controls addArrangedSubview:_phase7ChooseBundleButton];

    _phase7ProfilePopup = [[NSPopUpButton alloc] initWithFrame:NSMakeRect(0, 0, 220, 24) pullsDown:NO];
    [_phase7ProfilePopup addItemWithTitle:@"phase7.trace_fill_integrity.v1"];
    [controls addArrangedSubview:_phase7ProfilePopup];

    _phase7RunAnalysisButton = [NSButton buttonWithTitle:@"Run Analyzer"
                                                  target:self
                                                  action:@selector(runPhase7Analysis:)];
    [controls addArrangedSubview:_phase7RunAnalysisButton];

    _phase7BuildPlaybookButton = [NSButton buttonWithTitle:@"Build Dry-Run Playbook"
                                                    target:self
                                                    action:@selector(buildSelectedPhase7Playbook:)];
    _phase7BuildPlaybookButton.enabled = NO;
    [controls addArrangedSubview:_phase7BuildPlaybookButton];

    _phase7BuildLedgerButton = [NSButton buttonWithTitle:@"Prepare Execution Ledger"
                                                  target:self
                                                  action:@selector(buildSelectedPhase7ExecutionLedger:)];
    _phase7BuildLedgerButton.enabled = NO;
    [controls addArrangedSubview:_phase7BuildLedgerButton];

    _phase7OpenAnalysisButton = [NSButton buttonWithTitle:@"Open Selected Analysis"
                                                   target:self
                                                   action:@selector(openSelectedPhase7Analysis:)];
    _phase7OpenAnalysisButton.enabled = NO;
    [controls addArrangedSubview:_phase7OpenAnalysisButton];

    _phase7OpenPlaybookButton = [NSButton buttonWithTitle:@"Open Selected Playbook"
                                                   target:self
                                                   action:@selector(openSelectedPhase7Playbook:)];
    _phase7OpenPlaybookButton.enabled = NO;
    [controls addArrangedSubview:_phase7OpenPlaybookButton];

    _phase7OpenSourceArtifactButton = [NSButton buttonWithTitle:@"Open Source Artifact"
                                                         target:self
                                                         action:@selector(openSelectedPhase7SourceArtifact:)];
    _phase7OpenSourceArtifactButton.enabled = NO;
    [controls addArrangedSubview:_phase7OpenSourceArtifactButton];

    _phase7OpenLinkedAnalysisButton = [NSButton buttonWithTitle:@"Open Linked Analysis"
                                                         target:self
                                                         action:@selector(openSelectedPhase7LinkedAnalysis:)];
    _phase7OpenLinkedAnalysisButton.enabled = NO;
    [controls addArrangedSubview:_phase7OpenLinkedAnalysisButton];

    _phase7LoadRangeButton = [NSButton buttonWithTitle:@"Load Replay Range"
                                                target:self
                                                action:@selector(loadReplayRangeFromPhase7Selection:)];
    _phase7LoadRangeButton.enabled = NO;
    [controls addArrangedSubview:_phase7LoadRangeButton];
    [stack addArrangedSubview:controls];

    _phase7StateLabel = MakeLabel(@"No Phase 7 artifacts loaded yet.",
                                  [NSFont systemFontOfSize:12.0 weight:NSFontWeightMedium],
                                  [NSColor secondaryLabelColor]);
    [stack addArrangedSubview:_phase7StateLabel];

    NSStackView* analysisFilters = MakeControlRow();
    [analysisFilters addArrangedSubview:MakeLabel(@"analysis filters",
                                                  [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                                  [NSColor secondaryLabelColor])];
    _phase7AnalysisSourceFilterField = MakeMonospacedField(240.0, nil, @"source_artifact_id");
    [analysisFilters addArrangedSubview:_phase7AnalysisSourceFilterField];
    _phase7AnalysisProfileFilterPopup = [[NSPopUpButton alloc] initWithFrame:NSMakeRect(0, 0, 220, 24) pullsDown:NO];
    [_phase7AnalysisProfileFilterPopup addItemWithTitle:@"All Profiles"];
    [analysisFilters addArrangedSubview:_phase7AnalysisProfileFilterPopup];
    _phase7AnalysisSortPopup = [[NSPopUpButton alloc] initWithFrame:NSMakeRect(0, 0, 180, 24) pullsDown:NO];
    [_phase7AnalysisSortPopup addItemsWithTitles:@[@"Newest First", @"Most Findings", @"Profile", @"Source Artifact"]];
    [analysisFilters addArrangedSubview:_phase7AnalysisSortPopup];
    [stack addArrangedSubview:analysisFilters];

    NSStackView* playbookFilters = MakeControlRow();
    [playbookFilters addArrangedSubview:MakeLabel(@"playbook filters",
                                                  [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                                  [NSColor secondaryLabelColor])];
    _phase7PlaybookAnalysisFilterField = MakeMonospacedField(220.0, nil, @"analysis_artifact_id");
    [playbookFilters addArrangedSubview:_phase7PlaybookAnalysisFilterField];
    _phase7PlaybookSourceFilterField = MakeMonospacedField(220.0, nil, @"source_artifact_id");
    [playbookFilters addArrangedSubview:_phase7PlaybookSourceFilterField];
    _phase7PlaybookModeFilterPopup = [[NSPopUpButton alloc] initWithFrame:NSMakeRect(0, 0, 150, 24) pullsDown:NO];
    [_phase7PlaybookModeFilterPopup addItemsWithTitles:@[@"All Modes", @"dry_run", @"apply"]];
    [playbookFilters addArrangedSubview:_phase7PlaybookModeFilterPopup];
    _phase7PlaybookSortPopup = [[NSPopUpButton alloc] initWithFrame:NSMakeRect(0, 0, 200, 24) pullsDown:NO];
    [_phase7PlaybookSortPopup addItemsWithTitles:@[@"Newest First", @"Most Actions", @"Most Filtered Findings", @"Mode", @"Source Artifact"]];
    [playbookFilters addArrangedSubview:_phase7PlaybookSortPopup];
    _phase7ClearFiltersButton = [NSButton buttonWithTitle:@"Clear Filters"
                                                   target:self
                                                   action:@selector(clearPhase7Filters:)];
    [playbookFilters addArrangedSubview:_phase7ClearFiltersButton];
    [stack addArrangedSubview:playbookFilters];

    [stack addArrangedSubview:MakeSectionLabel(@"Analyzer Profiles")];

    _phase7ProfilesTextView = MakeReadOnlyTextView();
    _phase7ProfilesTextView.string = @"Refresh Phase 7 to inspect the supported analyzer profiles and their source-bundle expectations.";
    [stack addArrangedSubview:MakeScrollView(_phase7ProfilesTextView, 140.0)];

    [stack addArrangedSubview:MakeSectionLabel(@"Analysis Artifacts")];

    _phase7AnalysisTableView = MakeStandardTableView(self, self);
    AddTableColumn(_phase7AnalysisTableView, @"artifact_id", @"artifact_id", 220.0);
    AddTableColumn(_phase7AnalysisTableView, @"analysis_profile", @"analysis_profile", 220.0);
    AddTableColumn(_phase7AnalysisTableView, @"source_artifact_id", @"source_artifact_id", 220.0);
    AddTableColumn(_phase7AnalysisTableView, @"finding_count", @"finding_count", 110.0);
    ConfigureTablePrimaryAction(_phase7AnalysisTableView, self, @selector(openSelectedPhase7Analysis:));
    [stack addArrangedSubview:MakeTableScrollView(_phase7AnalysisTableView, 150.0)];

    [stack addArrangedSubview:MakeSectionLabel(@"Playbook Artifacts")];

    _phase7PlaybookTableView = MakeStandardTableView(self, self);
    AddTableColumn(_phase7PlaybookTableView, @"artifact_id", @"artifact_id", 220.0);
    AddTableColumn(_phase7PlaybookTableView, @"analysis_artifact_id", @"analysis_artifact_id", 220.0);
    AddTableColumn(_phase7PlaybookTableView, @"mode", @"mode", 110.0);
    AddTableColumn(_phase7PlaybookTableView, @"planned_action_count", @"planned_action_count", 140.0);
    ConfigureTablePrimaryAction(_phase7PlaybookTableView, self, @selector(openSelectedPhase7Playbook:));
    [stack addArrangedSubview:MakeTableScrollView(_phase7PlaybookTableView, 150.0)];

    [stack addArrangedSubview:MakeSectionLabel(@"Selected Findings")];

    _phase7FindingTableView = MakeStandardTableView(self, self);
    _phase7FindingTableView.allowsMultipleSelection = YES;
    AddTableColumn(_phase7FindingTableView, @"finding_id", @"finding_id", 220.0);
    AddTableColumn(_phase7FindingTableView, @"severity", @"severity", 100.0);
    AddTableColumn(_phase7FindingTableView, @"category", @"category", 160.0);
    AddTableColumn(_phase7FindingTableView, @"summary", @"summary", 360.0);
    [stack addArrangedSubview:MakeTableScrollView(_phase7FindingTableView, 140.0)];

    [stack addArrangedSubview:MakeSectionLabel(@"Planned Actions")];

    _phase7ActionTableView = MakeStandardTableView(self, self);
    _phase7ActionTableView.allowsMultipleSelection = YES;
    AddTableColumn(_phase7ActionTableView, @"action_id", @"action_id", 220.0);
    AddTableColumn(_phase7ActionTableView, @"action_type", @"action_type", 220.0);
    AddTableColumn(_phase7ActionTableView, @"finding_id", @"finding_id", 220.0);
    AddTableColumn(_phase7ActionTableView, @"title", @"title", 260.0);
    [stack addArrangedSubview:MakeTableScrollView(_phase7ActionTableView, 140.0)];

    [stack addArrangedSubview:MakeSectionLabel(@"Phase 7 Detail")];

    _phase7TextView = MakeReadOnlyTextView();
    _phase7TextView.string = ToNSString(Phase7DetailPlaceholder());
    [stack addArrangedSubview:MakeScrollView(_phase7TextView, 210.0)];

    NSTabViewItem* item = [[NSTabViewItem alloc] initWithIdentifier:@"Phase7Pane"];
    item.label = @"Phase7Pane";
    item.view = pane;
    return item;
}

- (void)refreshPhase7Artifacts:(id)sender {
    (void)sender;
    if (_phase7InFlight || !_client) {
        return;
    }

    tapescope::Phase7AnalysisInventorySelection analysisSelection;
    analysisSelection.sourceArtifactId = TrimAscii(ToStdString(_phase7AnalysisSourceFilterField.stringValue));
    analysisSelection.analysisProfile = SelectedPhase7AnalysisProfileFilter(_phase7AnalysisProfileFilterPopup);
    analysisSelection.sortBy = SelectedPhase7AnalysisSort(_phase7AnalysisSortPopup);
    analysisSelection.limit = 20;

    tapescope::Phase7PlaybookInventorySelection playbookSelection;
    playbookSelection.analysisArtifactId = TrimAscii(ToStdString(_phase7PlaybookAnalysisFilterField.stringValue));
    playbookSelection.sourceArtifactId = TrimAscii(ToStdString(_phase7PlaybookSourceFilterField.stringValue));
    playbookSelection.mode = SelectedPhase7PlaybookModeFilter(_phase7PlaybookModeFilterPopup);
    playbookSelection.sortBy = SelectedPhase7PlaybookSort(_phase7PlaybookSortPopup);
    playbookSelection.limit = 20;

    _phase7InFlight = YES;
    const std::uint64_t token = [self issueRequestToken:&_phase7RequestToken];
    _phase7RefreshButton.enabled = NO;
    _phase7OpenAnalysisButton.enabled = NO;
    _phase7OpenPlaybookButton.enabled = NO;
    _phase7BuildLedgerButton.enabled = NO;
    _phase7OpenSourceArtifactButton.enabled = NO;
    _phase7OpenLinkedAnalysisButton.enabled = NO;
    _phase7LoadRangeButton.enabled = NO;
    _phase7StateLabel.stringValue = @"Refreshing Phase 7 artifact inventory…";
    _phase7StateLabel.textColor = [NSColor systemOrangeColor];
    _phase7TextView.string = @"Refreshing durable Phase 7 analyses and playbooks…";

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_artifactQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto profiles = strongSelf->_client->listAnalysisProfilesPayload();
        const auto analyses = strongSelf->_client->listAnalysisArtifactsPayload(analysisSelection);
        const auto playbooks = strongSelf->_client->listPlaybookArtifactsPayload(playbookSelection);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil || ![innerSelf isRequestTokenCurrent:token storage:&innerSelf->_phase7RequestToken]) {
                return;
            }
            innerSelf->_phase7InFlight = NO;
            innerSelf->_phase7RefreshButton.enabled = YES;
            innerSelf->_latestPhase7Profiles.clear();
            innerSelf->_latestPhase7AnalysisArtifacts.clear();
            innerSelf->_latestPhase7PlaybookArtifacts.clear();
            if (profiles.ok()) {
                innerSelf->_latestPhase7Profiles = profiles.value;
            }
            if (analyses.ok()) {
                innerSelf->_latestPhase7AnalysisArtifacts = analyses.value.artifacts;
            }
            if (playbooks.ok()) {
                innerSelf->_latestPhase7PlaybookArtifacts = playbooks.value.artifacts;
            }
            innerSelf->_phase7ProfilesTextView.string = ToNSString(DescribePhase7Profiles(profiles));
            const std::string requestedFilterProfile = analysisSelection.analysisProfile;
            [innerSelf->_phase7ProfilePopup removeAllItems];
            if (!innerSelf->_latestPhase7Profiles.empty()) {
                for (const auto& profile : innerSelf->_latestPhase7Profiles) {
                    [innerSelf->_phase7ProfilePopup addItemWithTitle:ToNSString(profile.analysisProfile)];
                }
                const auto defaultIt = std::find_if(innerSelf->_latestPhase7Profiles.begin(),
                                                    innerSelf->_latestPhase7Profiles.end(),
                                                    [](const tapescope::Phase7AnalyzerProfile& profile) {
                                                        return profile.defaultProfile;
                                                    });
                if (defaultIt != innerSelf->_latestPhase7Profiles.end()) {
                    [innerSelf->_phase7ProfilePopup selectItemWithTitle:ToNSString(defaultIt->analysisProfile)];
                }
            } else {
                [innerSelf->_phase7ProfilePopup addItemWithTitle:@"phase7.trace_fill_integrity.v1"];
            }
            [innerSelf->_phase7AnalysisProfileFilterPopup removeAllItems];
            [innerSelf->_phase7AnalysisProfileFilterPopup addItemWithTitle:@"All Profiles"];
            for (const auto& profile : innerSelf->_latestPhase7Profiles) {
                [innerSelf->_phase7AnalysisProfileFilterPopup addItemWithTitle:ToNSString(profile.analysisProfile)];
            }
            if (!requestedFilterProfile.empty()) {
                [innerSelf->_phase7AnalysisProfileFilterPopup selectItemWithTitle:ToNSString(requestedFilterProfile)];
            } else {
                [innerSelf->_phase7AnalysisProfileFilterPopup selectItemWithTitle:@"All Profiles"];
            }
            [innerSelf->_phase7AnalysisTableView reloadData];
            [innerSelf->_phase7PlaybookTableView reloadData];
            if (!innerSelf->_latestPhase7AnalysisArtifacts.empty()) {
                innerSelf->_phase7SelectionIsPlaybook = NO;
                [innerSelf->_phase7AnalysisTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0]
                                                byExtendingSelection:NO];
            } else if (!innerSelf->_latestPhase7PlaybookArtifacts.empty()) {
                innerSelf->_phase7SelectionIsPlaybook = YES;
                [innerSelf->_phase7PlaybookTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0]
                                                 byExtendingSelection:NO];
            }

            if (profiles.ok() || analyses.ok() || playbooks.ok()) {
                if (!innerSelf->_latestPhase7AnalysisArtifacts.empty() || !innerSelf->_latestPhase7PlaybookArtifacts.empty()) {
                    innerSelf->_phase7StateLabel.stringValue = ToNSString(DescribePhase7InventoryStatus(
                        analyses.ok() ? analyses.value : tapescope::Phase7AnalysisInventoryPayload{},
                        playbooks.ok() ? playbooks.value : tapescope::Phase7PlaybookInventoryPayload{}));
                    innerSelf->_phase7StateLabel.textColor = [NSColor systemGreenColor];
                } else {
                    innerSelf->_phase7StateLabel.stringValue =
                        (analysisSelection.sourceArtifactId.empty() &&
                         analysisSelection.analysisProfile.empty() &&
                         playbookSelection.analysisArtifactId.empty() &&
                         playbookSelection.sourceArtifactId.empty() &&
                         playbookSelection.mode.empty())
                            ? @"No Phase 7 analysis or playbook artifacts are available yet."
                            : @"Phase 7 filters matched no analysis or playbook artifacts.";
                    innerSelf->_phase7StateLabel.textColor = TapeInkMutedColor();
                }
            } else {
                innerSelf->_phase7StateLabel.stringValue = @"Phase 7 artifact inventory failed.";
                innerSelf->_phase7StateLabel.textColor = [NSColor systemRedColor];
            }
            [innerSelf refreshPhase7DetailText];
        });
    });
}

- (void)clearPhase7Filters:(id)sender {
    (void)sender;
    _phase7AnalysisSourceFilterField.stringValue = @"";
    [_phase7AnalysisProfileFilterPopup selectItemWithTitle:@"All Profiles"];
    [_phase7AnalysisSortPopup selectItemWithTitle:@"Newest First"];
    _phase7PlaybookAnalysisFilterField.stringValue = @"";
    _phase7PlaybookSourceFilterField.stringValue = @"";
    [_phase7PlaybookModeFilterPopup selectItemWithTitle:@"All Modes"];
    [_phase7PlaybookSortPopup selectItemWithTitle:@"Newest First"];
    [self refreshPhase7Artifacts:nil];
}

- (void)choosePhase7BundlePath:(id)sender {
    (void)sender;
    NSOpenPanel* panel = [NSOpenPanel openPanel];
    panel.canChooseFiles = YES;
    panel.canChooseDirectories = NO;
    panel.allowsMultipleSelection = NO;
    panel.message = @"Choose a portable Phase 6 bundle for local Phase 7 analysis.";
    if ([panel runModal] == NSModalResponseOK && panel.URL != nil && panel.URL.path != nil) {
        _phase7BundlePathField.stringValue = panel.URL.path;
    }
}

- (void)refreshPhase7DetailText {
    const std::vector<std::string> selectedFindingIds =
        SelectedPhase7FindingIds(_phase7FindingTableView, _phase7VisibleFindings);
    const std::vector<std::string> selectedActionIds =
        SelectedPhase7ActionIds(_phase7ActionTableView, _phase7VisibleActions);
    _phase7SelectedSourceArtifactId.clear();
    _phase7SelectedLinkedAnalysisArtifactId.clear();
    _phase7ReplayRange = {};
    _phase7VisibleFindings.clear();
    _phase7VisibleActions.clear();

    const NSInteger analysisSelected = _phase7AnalysisTableView.selectedRow;
    if (analysisSelected >= 0 &&
        static_cast<std::size_t>(analysisSelected) < _latestPhase7AnalysisArtifacts.size() &&
        (_phase7SelectionIsPlaybook == NO || _phase7PlaybookTableView.selectedRow < 0)) {
        const auto& artifact = _latestPhase7AnalysisArtifacts.at(static_cast<std::size_t>(analysisSelected));
        _phase7DetailBody = DescribePhase7AnalysisArtifact(artifact);
        _phase7VisibleFindings = artifact.findings;
        _phase7SelectedSourceArtifactId = artifact.sourceArtifact.artifactId;
        _phase7SelectedLinkedAnalysisArtifactId.clear();
        if (const auto replayRange = ReplayRangeFromPhase7Context(artifact.replayContext); replayRange.has_value()) {
            _phase7ReplayRange = *replayRange;
            _phase7LoadRangeButton.enabled = YES;
        } else {
            _phase7LoadRangeButton.enabled = NO;
        }
        _phase7BuildPlaybookButton.enabled = YES;
        _phase7BuildLedgerButton.enabled = NO;
        UpdatePhase7BuildPlaybookButtonTitle(_phase7BuildPlaybookButton, selectedFindingIds.size());
        _phase7OpenAnalysisButton.enabled = YES;
        _phase7OpenPlaybookButton.enabled = NO;
        _phase7OpenSourceArtifactButton.enabled = !_phase7SelectedSourceArtifactId.empty();
        _phase7OpenLinkedAnalysisButton.enabled = NO;
        [_phase7FindingTableView reloadData];
        [_phase7ActionTableView reloadData];
        NSIndexSet* findingSelection = IndexSetForPhase7FindingIds(selectedFindingIds, _phase7VisibleFindings);
        if (findingSelection.count > 0) {
            [_phase7FindingTableView selectRowIndexes:findingSelection byExtendingSelection:NO];
        } else {
            [_phase7FindingTableView deselectAll:nil];
        }
        [_phase7ActionTableView deselectAll:nil];
        _phase7TextView.string = ToNSString(_phase7DetailBody + "\n\n" +
                                            DescribeSelectedPhase7Findings(_phase7VisibleFindings,
                                                                          _phase7FindingTableView.selectedRowIndexes));
        return;
    }

    const NSInteger playbookSelected = _phase7PlaybookTableView.selectedRow;
    if (playbookSelected >= 0 && static_cast<std::size_t>(playbookSelected) < _latestPhase7PlaybookArtifacts.size()) {
        const auto& artifact = _latestPhase7PlaybookArtifacts.at(static_cast<std::size_t>(playbookSelected));
        _phase7DetailBody = DescribePhase7PlaybookArtifact(artifact);
        _phase7VisibleActions = artifact.plannedActions;
        _phase7SelectedLinkedAnalysisArtifactId = artifact.analysisArtifact.artifactId;
        _phase7SelectedSourceArtifactId = SourceArtifactIdForPlaybook(artifact, _latestPhase7AnalysisArtifacts);
        if (_phase7SelectedSourceArtifactId.empty() && _client != nullptr &&
            !_phase7SelectedLinkedAnalysisArtifactId.empty()) {
            const auto linkedAnalysis = _client->readAnalysisArtifactPayload(_phase7SelectedLinkedAnalysisArtifactId);
            if (linkedAnalysis.ok()) {
                _phase7SelectedSourceArtifactId = linkedAnalysis.value.sourceArtifact.artifactId;
            }
        }
        if (const auto replayRange = ReplayRangeFromPhase7Context(artifact.replayContext); replayRange.has_value()) {
            _phase7ReplayRange = *replayRange;
            _phase7LoadRangeButton.enabled = YES;
        } else {
            _phase7LoadRangeButton.enabled = NO;
        }
        _phase7BuildPlaybookButton.enabled = NO;
        _phase7BuildLedgerButton.enabled = YES;
        UpdatePhase7BuildPlaybookButtonTitle(_phase7BuildPlaybookButton, 0);
        _phase7OpenAnalysisButton.enabled = NO;
        _phase7OpenPlaybookButton.enabled = YES;
        _phase7OpenSourceArtifactButton.enabled = !_phase7SelectedSourceArtifactId.empty();
        _phase7OpenLinkedAnalysisButton.enabled = !_phase7SelectedLinkedAnalysisArtifactId.empty();
        [_phase7FindingTableView reloadData];
        [_phase7ActionTableView reloadData];
        [_phase7FindingTableView deselectAll:nil];
        NSIndexSet* actionSelection = IndexSetForPhase7ActionIds(selectedActionIds, _phase7VisibleActions);
        if (actionSelection.count > 0) {
            [_phase7ActionTableView selectRowIndexes:actionSelection byExtendingSelection:NO];
        } else {
            [_phase7ActionTableView deselectAll:nil];
        }
        _phase7TextView.string = ToNSString(_phase7DetailBody + "\n\n" +
                                            DescribeSelectedPhase7Actions(_phase7VisibleActions,
                                                                         _phase7ActionTableView.selectedRowIndexes));
        return;
    }

    _phase7DetailBody = Phase7DetailPlaceholder();
    _phase7OpenAnalysisButton.enabled = NO;
    _phase7OpenPlaybookButton.enabled = NO;
    _phase7BuildPlaybookButton.enabled = NO;
    _phase7BuildLedgerButton.enabled = NO;
    UpdatePhase7BuildPlaybookButtonTitle(_phase7BuildPlaybookButton, 0);
    _phase7OpenSourceArtifactButton.enabled = NO;
    _phase7OpenLinkedAnalysisButton.enabled = NO;
    _phase7LoadRangeButton.enabled = NO;
    [_phase7FindingTableView reloadData];
    [_phase7ActionTableView reloadData];
    _phase7TextView.string = ToNSString(_phase7DetailBody);
}

- (void)runPhase7Analysis:(id)sender {
    (void)sender;
    if (_phase7InFlight || !_client) {
        return;
    }
    const std::string bundlePath = TrimAscii(ToStdString(_phase7BundlePathField.stringValue));
    if (bundlePath.empty()) {
        _phase7StateLabel.stringValue = @"Choose a Phase 6 bundle path first.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    const std::string analysisProfile = ToStdString(_phase7ProfilePopup.titleOfSelectedItem);
    if (analysisProfile.empty()) {
        _phase7StateLabel.stringValue = @"Select a Phase 7 analysis profile first.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    _phase7InFlight = YES;
    const std::uint64_t token = [self issueRequestToken:&_phase7RequestToken];
    _phase7RefreshButton.enabled = NO;
    _phase7ChooseBundleButton.enabled = NO;
    _phase7RunAnalysisButton.enabled = NO;
    _phase7BuildPlaybookButton.enabled = NO;
    _phase7BuildLedgerButton.enabled = NO;
    _phase7StateLabel.stringValue = @"Running Phase 7 analyzer…";
    _phase7StateLabel.textColor = [NSColor systemOrangeColor];
    _phase7TextView.string = @"Running a local Phase 7 analysis against the selected Phase 6 bundle…";

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_artifactQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->runAnalysisPayload(bundlePath, analysisProfile);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil || ![innerSelf isRequestTokenCurrent:token storage:&innerSelf->_phase7RequestToken]) {
                return;
            }
            innerSelf->_phase7InFlight = NO;
            innerSelf->_phase7RefreshButton.enabled = YES;
            innerSelf->_phase7ChooseBundleButton.enabled = YES;
            innerSelf->_phase7RunAnalysisButton.enabled = YES;
            if (!result.ok()) {
                innerSelf->_phase7StateLabel.stringValue = ToNSString(tapescope::QueryClient::describeError(result.error));
                innerSelf->_phase7StateLabel.textColor = ErrorColorForKind(result.error.kind);
                innerSelf->_phase7TextView.string = ToNSString(DescribePhase7AnalysisRun(bundlePath, result));
                return;
            }

            auto it = std::find_if(innerSelf->_latestPhase7AnalysisArtifacts.begin(),
                                   innerSelf->_latestPhase7AnalysisArtifacts.end(),
                                   [&](const tapescope::Phase7AnalysisArtifact& item) {
                                       return item.analysisArtifact.artifactId == result.value.artifact.analysisArtifact.artifactId;
                                   });
            if (it != innerSelf->_latestPhase7AnalysisArtifacts.end()) {
                *it = result.value.artifact;
                const std::size_t index = static_cast<std::size_t>(std::distance(innerSelf->_latestPhase7AnalysisArtifacts.begin(), it));
                innerSelf->_phase7SelectionIsPlaybook = NO;
                [innerSelf->_phase7AnalysisTableView reloadData];
                [innerSelf->_phase7AnalysisTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:index] byExtendingSelection:NO];
            } else {
                innerSelf->_latestPhase7AnalysisArtifacts.insert(innerSelf->_latestPhase7AnalysisArtifacts.begin(),
                                                                 result.value.artifact);
                innerSelf->_phase7SelectionIsPlaybook = NO;
                [innerSelf->_phase7AnalysisTableView reloadData];
                [innerSelf->_phase7AnalysisTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0] byExtendingSelection:NO];
            }
            [innerSelf->_phase7PlaybookTableView deselectAll:nil];
            [innerSelf refreshPhase7DetailText];
            innerSelf->_phase7StateLabel.stringValue = ToNSString(std::string("Phase 7 analysis ") +
                                                                  (result.value.created ? "created." : "reused."));
            innerSelf->_phase7StateLabel.textColor = [NSColor systemGreenColor];
            innerSelf->_phase7TextView.string = ToNSString(DescribePhase7AnalysisRun(bundlePath, result));
            [innerSelf recordRecentHistoryEntry:tapescope::json{
                {"kind", "phase7_analysis"},
                {"target_id", result.value.artifact.analysisArtifact.artifactId},
                {"artifact_id", result.value.artifact.analysisArtifact.artifactId},
                {"source_artifact_id", result.value.artifact.sourceArtifact.artifactId},
                {"headline", result.value.artifact.analysisProfile},
                {"detail", result.value.created ? std::string("Phase 7 analysis created")
                                                : std::string("Phase 7 analysis reused")},
                {"bundle_path", bundlePath},
                {"first_session_seq", result.value.artifact.replayContext.value("requested_window", tapescope::json::object()).value("first_session_seq", 0ULL)},
                {"last_session_seq", result.value.artifact.replayContext.value("requested_window", tapescope::json::object()).value("last_session_seq", 0ULL)}
            }];
        });
    });
}

- (void)buildSelectedPhase7Playbook:(id)sender {
    (void)sender;
    if (_phase7InFlight || !_client) {
        return;
    }
    const NSInteger selected = _phase7AnalysisTableView.selectedRow;
    if (selected < 0 || static_cast<std::size_t>(selected) >= _latestPhase7AnalysisArtifacts.size()) {
        _phase7StateLabel.stringValue = @"Select a Phase 7 analysis row first.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    const std::string analysisArtifactId =
        _latestPhase7AnalysisArtifacts.at(static_cast<std::size_t>(selected)).analysisArtifact.artifactId;
    const std::vector<std::string> selectedFindingIds =
        SelectedPhase7FindingIds(_phase7FindingTableView, _phase7VisibleFindings);
    if (analysisArtifactId.empty()) {
        _phase7StateLabel.stringValue = @"Selected analysis is missing an artifact id.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    _phase7InFlight = YES;
    const std::uint64_t token = [self issueRequestToken:&_phase7RequestToken];
    _phase7RefreshButton.enabled = NO;
    _phase7ChooseBundleButton.enabled = NO;
    _phase7RunAnalysisButton.enabled = NO;
    _phase7BuildPlaybookButton.enabled = NO;
    _phase7BuildLedgerButton.enabled = NO;
    _phase7StateLabel.stringValue = @"Building dry-run playbook…";
    _phase7StateLabel.textColor = [NSColor systemOrangeColor];
    _phase7TextView.string = @"Building a guarded dry-run playbook from the selected Phase 7 analysis…";

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_artifactQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->buildPlaybookPayload(analysisArtifactId, selectedFindingIds);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil || ![innerSelf isRequestTokenCurrent:token storage:&innerSelf->_phase7RequestToken]) {
                return;
            }
            innerSelf->_phase7InFlight = NO;
            innerSelf->_phase7RefreshButton.enabled = YES;
            innerSelf->_phase7ChooseBundleButton.enabled = YES;
            innerSelf->_phase7RunAnalysisButton.enabled = YES;
            if (!result.ok()) {
                innerSelf->_phase7StateLabel.stringValue = ToNSString(tapescope::QueryClient::describeError(result.error));
                innerSelf->_phase7StateLabel.textColor = ErrorColorForKind(result.error.kind);
                innerSelf->_phase7TextView.string = ToNSString(DescribePhase7PlaybookBuild(analysisArtifactId, result));
                return;
            }

            auto it = std::find_if(innerSelf->_latestPhase7PlaybookArtifacts.begin(),
                                   innerSelf->_latestPhase7PlaybookArtifacts.end(),
                                   [&](const tapescope::Phase7PlaybookArtifact& item) {
                                       return item.playbookArtifact.artifactId == result.value.artifact.playbookArtifact.artifactId;
                                   });
            if (it != innerSelf->_latestPhase7PlaybookArtifacts.end()) {
                *it = result.value.artifact;
                const std::size_t index = static_cast<std::size_t>(std::distance(innerSelf->_latestPhase7PlaybookArtifacts.begin(), it));
                innerSelf->_phase7SelectionIsPlaybook = YES;
                [innerSelf->_phase7PlaybookTableView reloadData];
                [innerSelf->_phase7PlaybookTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:index] byExtendingSelection:NO];
            } else {
                innerSelf->_latestPhase7PlaybookArtifacts.insert(innerSelf->_latestPhase7PlaybookArtifacts.begin(),
                                                                 result.value.artifact);
                innerSelf->_phase7SelectionIsPlaybook = YES;
                [innerSelf->_phase7PlaybookTableView reloadData];
                [innerSelf->_phase7PlaybookTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0] byExtendingSelection:NO];
            }
            [innerSelf->_phase7AnalysisTableView deselectAll:nil];
            [innerSelf refreshPhase7DetailText];
            innerSelf->_phase7StateLabel.stringValue = ToNSString(std::string("Phase 7 dry-run playbook ") +
                                                                  (result.value.created ? "created." : "reused."));
            innerSelf->_phase7StateLabel.textColor = [NSColor systemGreenColor];
            innerSelf->_phase7TextView.string = ToNSString(DescribePhase7PlaybookBuild(analysisArtifactId, result));
            [innerSelf recordRecentHistoryEntry:tapescope::json{
                {"kind", "phase7_playbook"},
                {"target_id", result.value.artifact.playbookArtifact.artifactId},
                {"artifact_id", result.value.artifact.playbookArtifact.artifactId},
                {"analysis_artifact_id", result.value.artifact.analysisArtifact.artifactId},
                {"headline", std::string("Phase 7 playbook (dry_run)")},
                {"detail", std::string(result.value.created ? "Dry-run playbook created"
                                                            : "Dry-run playbook reused") +
                               " from " + std::to_string(result.value.artifact.filteredFindingIds.size()) +
                               " finding(s)"},
                {"first_session_seq", result.value.artifact.replayContext.value("requested_window", tapescope::json::object()).value("first_session_seq", 0ULL)},
                {"last_session_seq", result.value.artifact.replayContext.value("requested_window", tapescope::json::object()).value("last_session_seq", 0ULL)}
            }];
        });
    });
}

- (void)buildSelectedPhase7ExecutionLedger:(id)sender {
    (void)sender;
    if (_phase7InFlight || !_client) {
        return;
    }
    const NSInteger selected = _phase7PlaybookTableView.selectedRow;
    if (selected < 0 || static_cast<std::size_t>(selected) >= _latestPhase7PlaybookArtifacts.size()) {
        _phase7StateLabel.stringValue = @"Select a Phase 7 playbook row first.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    const std::string playbookArtifactId =
        _latestPhase7PlaybookArtifacts.at(static_cast<std::size_t>(selected)).playbookArtifact.artifactId;
    if (playbookArtifactId.empty()) {
        _phase7StateLabel.stringValue = @"Selected playbook is missing an artifact id.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    _phase7InFlight = YES;
    const std::uint64_t token = [self issueRequestToken:&_phase7RequestToken];
    _phase7RefreshButton.enabled = NO;
    _phase7ChooseBundleButton.enabled = NO;
    _phase7RunAnalysisButton.enabled = NO;
    _phase7BuildPlaybookButton.enabled = NO;
    _phase7BuildLedgerButton.enabled = NO;
    _phase7StateLabel.stringValue = @"Preparing execution ledger…";
    _phase7StateLabel.textColor = [NSColor systemOrangeColor];
    _phase7TextView.string = @"Preparing a review-only execution ledger from the selected Phase 7 playbook…";

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_artifactQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->buildExecutionLedgerPayload(playbookArtifactId);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil || ![innerSelf isRequestTokenCurrent:token storage:&innerSelf->_phase7RequestToken]) {
                return;
            }
            innerSelf->_phase7InFlight = NO;
            innerSelf->_phase7RefreshButton.enabled = YES;
            innerSelf->_phase7ChooseBundleButton.enabled = YES;
            innerSelf->_phase7RunAnalysisButton.enabled = YES;
            if (!result.ok()) {
                innerSelf->_phase7StateLabel.stringValue = ToNSString(tapescope::QueryClient::describeError(result.error));
                innerSelf->_phase7StateLabel.textColor = ErrorColorForKind(result.error.kind);
                innerSelf->_phase7TextView.string = ToNSString(DescribePhase7ExecutionLedgerBuild(playbookArtifactId, result));
                return;
            }

            innerSelf->_phase7SelectedSourceArtifactId = result.value.artifact.sourceArtifact.artifactId;
            innerSelf->_phase7SelectedLinkedAnalysisArtifactId = result.value.artifact.analysisArtifact.artifactId;
            if (const auto replayRange = ReplayRangeFromPhase7Context(result.value.artifact.replayContext); replayRange.has_value()) {
                innerSelf->_phase7ReplayRange = *replayRange;
                innerSelf->_phase7LoadRangeButton.enabled = YES;
            } else {
                innerSelf->_phase7ReplayRange = {};
                innerSelf->_phase7LoadRangeButton.enabled = NO;
            }
            innerSelf->_phase7StateLabel.stringValue = ToNSString(std::string("Phase 7 execution ledger ") +
                                                                  (result.value.created ? "created." : "reused."));
            innerSelf->_phase7StateLabel.textColor = [NSColor systemGreenColor];
            innerSelf->_phase7TextView.string = ToNSString(DescribePhase7ExecutionLedgerBuild(playbookArtifactId, result));
            [innerSelf recordRecentHistoryEntry:tapescope::json{
                {"kind", "phase7_execution_ledger"},
                {"target_id", result.value.artifact.ledgerArtifact.artifactId},
                {"artifact_id", result.value.artifact.ledgerArtifact.artifactId},
                {"playbook_artifact_id", result.value.artifact.playbookArtifact.artifactId},
                {"analysis_artifact_id", result.value.artifact.analysisArtifact.artifactId},
                {"source_artifact_id", result.value.artifact.sourceArtifact.artifactId},
                {"headline", std::string("Phase 7 execution ledger")},
                {"detail", std::string(result.value.created ? "Execution ledger created"
                                                            : "Execution ledger reused") +
                               " with " + std::to_string(result.value.artifact.entries.size()) +
                               " review entries"},
                {"first_session_seq", result.value.artifact.replayContext.value("requested_window", tapescope::json::object()).value("first_session_seq", 0ULL)},
                {"last_session_seq", result.value.artifact.replayContext.value("requested_window", tapescope::json::object()).value("last_session_seq", 0ULL)}
            }];
        });
    });
}

- (void)openSelectedPhase7Analysis:(id)sender {
    (void)sender;
    const NSInteger selected = _phase7AnalysisTableView.selectedRow;
    if (selected < 0 || static_cast<std::size_t>(selected) >= _latestPhase7AnalysisArtifacts.size()) {
        _phase7StateLabel.stringValue = @"Select a Phase 7 analysis row first.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    [self openPhase7AnalysisArtifactId:_latestPhase7AnalysisArtifacts.at(static_cast<std::size_t>(selected)).analysisArtifact.artifactId];
}

- (void)openSelectedPhase7Playbook:(id)sender {
    (void)sender;
    const NSInteger selected = _phase7PlaybookTableView.selectedRow;
    if (selected < 0 || static_cast<std::size_t>(selected) >= _latestPhase7PlaybookArtifacts.size()) {
        _phase7StateLabel.stringValue = @"Select a Phase 7 playbook row first.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    [self openPhase7PlaybookArtifactId:_latestPhase7PlaybookArtifacts.at(static_cast<std::size_t>(selected)).playbookArtifact.artifactId];
}

- (void)openPhase7AnalysisArtifactId:(const std::string&)artifactId {
    if (_phase7InFlight || !_client) {
        return;
    }
    if (artifactId.empty()) {
        _phase7StateLabel.stringValue = @"Phase 7 analysis artifact id is missing.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    _phase7InFlight = YES;
    const std::uint64_t token = [self issueRequestToken:&_phase7RequestToken];
    _phase7RefreshButton.enabled = NO;
    _phase7StateLabel.stringValue = @"Opening Phase 7 analysis…";
    _phase7StateLabel.textColor = [NSColor systemOrangeColor];

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_artifactQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->readAnalysisArtifactPayload(artifactId);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil || ![innerSelf isRequestTokenCurrent:token storage:&innerSelf->_phase7RequestToken]) {
                return;
            }
            innerSelf->_phase7InFlight = NO;
            innerSelf->_phase7RefreshButton.enabled = YES;
            if (!result.ok()) {
                innerSelf->_phase7StateLabel.stringValue = ToNSString(tapescope::QueryClient::describeError(result.error));
                innerSelf->_phase7StateLabel.textColor = ErrorColorForKind(result.error.kind);
                return;
            }

            auto it = std::find_if(innerSelf->_latestPhase7AnalysisArtifacts.begin(),
                                   innerSelf->_latestPhase7AnalysisArtifacts.end(),
                                   [&](const tapescope::Phase7AnalysisArtifact& item) {
                                       return item.analysisArtifact.artifactId == artifactId;
                                   });
            if (it != innerSelf->_latestPhase7AnalysisArtifacts.end()) {
                *it = result.value;
                const std::size_t index = static_cast<std::size_t>(std::distance(innerSelf->_latestPhase7AnalysisArtifacts.begin(), it));
                innerSelf->_phase7SelectionIsPlaybook = NO;
                [innerSelf->_phase7AnalysisTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:index] byExtendingSelection:NO];
            } else {
                innerSelf->_latestPhase7AnalysisArtifacts.insert(innerSelf->_latestPhase7AnalysisArtifacts.begin(), result.value);
                [innerSelf->_phase7AnalysisTableView reloadData];
                innerSelf->_phase7SelectionIsPlaybook = NO;
                [innerSelf->_phase7AnalysisTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0] byExtendingSelection:NO];
            }
            [innerSelf->_phase7PlaybookTableView deselectAll:nil];
            [innerSelf refreshPhase7DetailText];
            innerSelf->_phase7StateLabel.stringValue = @"Phase 7 analysis reopened.";
            innerSelf->_phase7StateLabel.textColor = [NSColor systemGreenColor];
            [innerSelf recordRecentHistoryEntry:tapescope::json{
                {"kind", "phase7_analysis"},
                {"target_id", artifactId},
                {"artifact_id", artifactId},
                {"source_artifact_id", result.value.sourceArtifact.artifactId},
                {"headline", result.value.analysisProfile},
                {"detail", result.value.findings.empty() ? std::string("No findings recorded.")
                                                          : result.value.findings.front().summary},
                {"first_session_seq", result.value.replayContext.value("requested_window", tapescope::json::object()).value("first_session_seq", 0ULL)},
                {"last_session_seq", result.value.replayContext.value("requested_window", tapescope::json::object()).value("last_session_seq", 0ULL)}
            }];
        });
    });
}

- (void)openPhase7PlaybookArtifactId:(const std::string&)artifactId {
    if (_phase7InFlight || !_client) {
        return;
    }
    if (artifactId.empty()) {
        _phase7StateLabel.stringValue = @"Phase 7 playbook artifact id is missing.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    _phase7InFlight = YES;
    const std::uint64_t token = [self issueRequestToken:&_phase7RequestToken];
    _phase7RefreshButton.enabled = NO;
    _phase7StateLabel.stringValue = @"Opening Phase 7 playbook…";
    _phase7StateLabel.textColor = [NSColor systemOrangeColor];

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_artifactQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->readPlaybookArtifactPayload(artifactId);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil || ![innerSelf isRequestTokenCurrent:token storage:&innerSelf->_phase7RequestToken]) {
                return;
            }
            innerSelf->_phase7InFlight = NO;
            innerSelf->_phase7RefreshButton.enabled = YES;
            if (!result.ok()) {
                innerSelf->_phase7StateLabel.stringValue = ToNSString(tapescope::QueryClient::describeError(result.error));
                innerSelf->_phase7StateLabel.textColor = ErrorColorForKind(result.error.kind);
                return;
            }

            auto it = std::find_if(innerSelf->_latestPhase7PlaybookArtifacts.begin(),
                                   innerSelf->_latestPhase7PlaybookArtifacts.end(),
                                   [&](const tapescope::Phase7PlaybookArtifact& item) {
                                       return item.playbookArtifact.artifactId == artifactId;
                                   });
            if (it != innerSelf->_latestPhase7PlaybookArtifacts.end()) {
                *it = result.value;
                const std::size_t index = static_cast<std::size_t>(std::distance(innerSelf->_latestPhase7PlaybookArtifacts.begin(), it));
                innerSelf->_phase7SelectionIsPlaybook = YES;
                [innerSelf->_phase7PlaybookTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:index] byExtendingSelection:NO];
            } else {
                innerSelf->_latestPhase7PlaybookArtifacts.insert(innerSelf->_latestPhase7PlaybookArtifacts.begin(), result.value);
                [innerSelf->_phase7PlaybookTableView reloadData];
                innerSelf->_phase7SelectionIsPlaybook = YES;
                [innerSelf->_phase7PlaybookTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0] byExtendingSelection:NO];
            }
            [innerSelf->_phase7AnalysisTableView deselectAll:nil];
            [innerSelf refreshPhase7DetailText];
            innerSelf->_phase7StateLabel.stringValue = @"Phase 7 playbook reopened (dry-run only).";
            innerSelf->_phase7StateLabel.textColor = [NSColor systemGreenColor];
            [innerSelf recordRecentHistoryEntry:tapescope::json{
                {"kind", "phase7_playbook"},
                {"target_id", artifactId},
                {"artifact_id", artifactId},
                {"analysis_artifact_id", result.value.analysisArtifact.artifactId},
                {"headline", std::string("Phase 7 playbook (") + result.value.mode + ")"},
                {"detail", std::string("Planned actions: ") + std::to_string(result.value.plannedActions.size())},
                {"first_session_seq", result.value.replayContext.value("requested_window", tapescope::json::object()).value("first_session_seq", 0ULL)},
                {"last_session_seq", result.value.replayContext.value("requested_window", tapescope::json::object()).value("last_session_seq", 0ULL)}
            }];
        });
    });
}

- (void)openPhase7ExecutionLedgerArtifactId:(const std::string&)artifactId {
    if (_phase7InFlight || !_client) {
        return;
    }
    if (artifactId.empty()) {
        _phase7StateLabel.stringValue = @"Phase 7 execution ledger artifact id is missing.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    _phase7InFlight = YES;
    const std::uint64_t token = [self issueRequestToken:&_phase7RequestToken];
    _phase7RefreshButton.enabled = NO;
    _phase7StateLabel.stringValue = @"Opening Phase 7 execution ledger…";
    _phase7StateLabel.textColor = [NSColor systemOrangeColor];

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_artifactQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->readExecutionLedgerArtifactPayload(artifactId);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil || ![innerSelf isRequestTokenCurrent:token storage:&innerSelf->_phase7RequestToken]) {
                return;
            }
            innerSelf->_phase7InFlight = NO;
            innerSelf->_phase7RefreshButton.enabled = YES;
            if (!result.ok()) {
                innerSelf->_phase7StateLabel.stringValue = ToNSString(tapescope::QueryClient::describeError(result.error));
                innerSelf->_phase7StateLabel.textColor = ErrorColorForKind(result.error.kind);
                return;
            }

            innerSelf->_phase7SelectedSourceArtifactId = result.value.sourceArtifact.artifactId;
            innerSelf->_phase7SelectedLinkedAnalysisArtifactId = result.value.analysisArtifact.artifactId;
            if (const auto replayRange = ReplayRangeFromPhase7Context(result.value.replayContext); replayRange.has_value()) {
                innerSelf->_phase7ReplayRange = *replayRange;
                innerSelf->_phase7LoadRangeButton.enabled = YES;
            } else {
                innerSelf->_phase7ReplayRange = {};
                innerSelf->_phase7LoadRangeButton.enabled = NO;
            }
            innerSelf->_phase7StateLabel.stringValue = @"Phase 7 execution ledger reopened.";
            innerSelf->_phase7StateLabel.textColor = [NSColor systemGreenColor];
            innerSelf->_phase7TextView.string = ToNSString(DescribePhase7ExecutionLedgerArtifact(result.value));
            [innerSelf recordRecentHistoryEntry:tapescope::json{
                {"kind", "phase7_execution_ledger"},
                {"target_id", artifactId},
                {"artifact_id", artifactId},
                {"playbook_artifact_id", result.value.playbookArtifact.artifactId},
                {"analysis_artifact_id", result.value.analysisArtifact.artifactId},
                {"source_artifact_id", result.value.sourceArtifact.artifactId},
                {"headline", std::string("Phase 7 execution ledger")},
                {"detail", std::string("Review entries: ") + std::to_string(result.value.entries.size())},
                {"first_session_seq", result.value.replayContext.value("requested_window", tapescope::json::object()).value("first_session_seq", 0ULL)},
                {"last_session_seq", result.value.replayContext.value("requested_window", tapescope::json::object()).value("last_session_seq", 0ULL)}
            }];
        });
    });
}

- (void)openSelectedPhase7SourceArtifact:(id)sender {
    (void)sender;
    if (_phase7SelectedSourceArtifactId.empty()) {
        _phase7StateLabel.stringValue = @"Selected Phase 7 artifact does not expose a source artifact id.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    _artifactIdField.stringValue = ToNSString(_phase7SelectedSourceArtifactId);
    if (_tabView != nil) {
        [_tabView selectTabViewItemWithIdentifier:@"ArtifactPane"];
    }
    [self fetchArtifact:nil];
}

- (void)openSelectedPhase7LinkedAnalysis:(id)sender {
    (void)sender;
    if (_phase7SelectedLinkedAnalysisArtifactId.empty()) {
        _phase7StateLabel.stringValue = @"Selected playbook does not expose a linked analysis artifact.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    [self openPhase7AnalysisArtifactId:_phase7SelectedLinkedAnalysisArtifactId];
}

- (void)loadReplayRangeFromPhase7Selection:(id)sender {
    (void)sender;
    [self loadReplayRange:_phase7ReplayRange
                available:(_phase7ReplayRange.firstSessionSeq > 0 &&
                           _phase7ReplayRange.lastSessionSeq >= _phase7ReplayRange.firstSessionSeq)
               stateLabel:_phase7StateLabel
           missingMessage:@"Selected Phase 7 artifact does not expose a replayable session window."];
}

@end
