#import "tapescope_window_internal.h"

#include "tape_phase7_artifacts.h"
#include "tapescope_support.h"

#include <algorithm>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>

namespace {

using namespace tapescope_support;

constexpr NSInteger kPhase8SelectionFocusAutomatic = 0;
constexpr NSInteger kPhase8SelectionFocusWatch = 1;
constexpr NSInteger kPhase8SelectionFocusTrigger = 2;
constexpr NSInteger kPhase8SelectionFocusAttention = 3;
constexpr const char* kPhase8AttentionActor = "tapescope_operator";

std::string SelectedPopupToken(NSPopUpButton* popup) {
    if (popup == nil || popup.titleOfSelectedItem == nil) {
        return {};
    }
    return TrimAscii(ToStdString(popup.titleOfSelectedItem));
}

std::string CurrentUtcIso8601Now() {
    const auto now = std::chrono::system_clock::now();
    const auto msSinceEpoch = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch());
    const auto msPart = static_cast<long long>(msSinceEpoch.count() % 1000);
    const std::time_t time = std::chrono::system_clock::to_time_t(now);

    std::tm utc{};
    gmtime_r(&time, &utc);

    std::ostringstream out;
    out << std::put_time(&utc, "%Y-%m-%dT%H:%M:%S")
        << '.'
        << std::setw(3) << std::setfill('0') << msPart
        << 'Z';
    return out.str();
}

bool Phase8WatchIsDue(const tapescope::Phase8WatchDefinitionArtifact& artifact) {
    if (!artifact.enabled || artifact.evaluationCadenceMinutes == 0) {
        return false;
    }
    if (artifact.nextEvaluationAtUtc.empty()) {
        return true;
    }
    return artifact.nextEvaluationAtUtc <= CurrentUtcIso8601Now();
}

std::string DescribePhase8WatchDefinition(const tapescope::Phase8WatchDefinitionArtifact& artifact) {
    std::ostringstream out;
    out << tape_phase8::watchDefinitionArtifactMarkdown(artifact);
    if (artifact.manifest.is_object()) {
        out << "\nManifest:\n" << artifact.manifest.dump(2) << "\n";
    }
    return out.str();
}

std::string DescribePhase8TriggerRun(const tapescope::Phase8TriggerRunArtifact& artifact) {
    std::ostringstream out;
    out << tape_phase8::triggerRunArtifactMarkdown(artifact);
    if (artifact.manifest.is_object()) {
        out << "\nManifest:\n" << artifact.manifest.dump(2) << "\n";
    }
    return out.str();
}

std::string DescribePhase8AttentionItem(const tapescope::Phase8AttentionInboxItem& item) {
    std::ostringstream out;
    out << "trigger_artifact_id: " << item.triggerArtifactId << "\n"
        << "watch_artifact_id: " << item.watchArtifactId << "\n"
        << "analysis_artifact_id: " << item.analysisArtifactId << "\n"
        << "source_artifact_id: " << item.sourceArtifactId << "\n"
        << "analysis_profile: " << item.analysisProfile << "\n"
        << "highest_severity: " << item.highestSeverity << "\n"
        << "finding_count: " << item.findingCount << "\n"
        << "attention_status: " << item.attentionStatus << "\n"
        << "attention_open: " << (item.attentionOpen ? "true" : "false") << "\n"
        << "generated_at_utc: " << item.generatedAtUtc << "\n"
        << "title: " << item.title << "\n"
        << "headline: " << item.headline << "\n";
    return out.str();
}

std::vector<std::string> Phase8ProfileTitles() {
    return {
        tape_phase7::kDefaultAnalyzerProfile,
        tape_phase7::kIncidentTriageAnalyzerProfile,
        tape_phase7::kFillQualityAnalyzerProfile,
        tape_phase7::kLiquidityBehaviorAnalyzerProfile,
        tape_phase7::kAdverseSelectionAnalyzerProfile,
        tape_phase7::kOrderImpactAnalyzerProfile,
    };
}

std::vector<std::string> Phase8MinimumSeverityTitles() {
    return {"any", "info", "low", "medium", "high", "critical"};
}

std::vector<std::string> Phase8TriggerAttentionStatusTitles() {
    return {
        "any",
        tape_phase8::kAttentionStatusNew,
        tape_phase8::kAttentionStatusAcknowledged,
        tape_phase8::kAttentionStatusSnoozed,
        tape_phase8::kAttentionStatusResolved,
        tape_phase8::kAttentionStatusSuppressed
    };
}

std::vector<std::string> Phase8TriggerAttentionOpenTitles() {
    return {"any", "open", "closed"};
}

tapescope::Phase8TriggerRunInventorySelection CurrentPhase8TriggerRunSelection(NSString* attentionStatusTitle,
                                                                               NSString* attentionOpenTitle) {
    tapescope::Phase8TriggerRunInventorySelection selection;
    selection.limit = 25;
    const std::string status = TrimAscii(ToStdString(attentionStatusTitle ?: @""));
    if (!status.empty() && status != "any") {
        selection.attentionStatus = status;
    }
    const std::string attentionOpen = TrimAscii(ToStdString(attentionOpenTitle ?: @""));
    if (attentionOpen == "open") {
        selection.attentionOpen = true;
    } else if (attentionOpen == "closed") {
        selection.attentionOpen = false;
    }
    return selection;
}

std::string Phase8WatchPopupTitle(const tapescope::Phase8WatchDefinitionArtifact& artifact) {
    std::ostringstream out;
    out << artifact.watchArtifact.artifactId << "  [" << artifact.analysisProfile << "]";
    if (!artifact.title.empty()) {
        out << "  " << artifact.title;
    }
    if (!artifact.enabled) {
        out << "  (disabled)";
    } else if (Phase8WatchIsDue(artifact)) {
        out << "  (due)";
    }
    return out.str();
}

std::string Phase8AttentionPopupTitle(const tapescope::Phase8AttentionInboxItem& item) {
    std::ostringstream out;
    out << item.highestSeverity << "  " << item.findingCount << " finding";
    if (item.findingCount != 1) {
        out << "s";
    }
    out << "  " << item.title;
    return out.str();
}

std::string Phase8TriggerPopupTitle(const tapescope::Phase8TriggerRunArtifact& artifact) {
    std::ostringstream out;
    out << (artifact.triggerOutcome == tape_phase8::kTriggerOutcomeSuppressed ? "suppressed" : "triggered")
        << "  " << artifact.findingCount << " finding";
    if (artifact.findingCount != 1U) {
        out << "s";
    }
    out << "  " << artifact.title;
    return out.str();
}

} // namespace

@implementation TapeScopeWindowController (Phase8Queries)

- (NSTabViewItem*)phase8InboxTabItem {
    const auto paneWithStack = MakePaneWithStack();
    NSStackView* stack = paneWithStack.stack;
    NSView* pane = paneWithStack.view;

    [stack addArrangedSubview:MakeIntroLabel(@"Phase 8 trigger inbox: define durable bundle watches, schedule them for automatic evaluation, and keep a compact operator inbox of new attention items before they flow into the deeper Phase 7 review/runtime surfaces.",
                                             2)];

    NSStackView* controls = MakeControlRow();
    _phase8RefreshButton = [NSButton buttonWithTitle:@"Refresh Inbox"
                                              target:self
                                              action:@selector(refreshPhase8Inbox:)];
    [controls addArrangedSubview:_phase8RefreshButton];

    [controls addArrangedSubview:MakeLabel(@"bundle_path",
                                           [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                           [NSColor secondaryLabelColor])];
    _phase8BundlePathField = MakeMonospacedField(320.0, nil, @"Choose a Phase 6 case bundle");
    [controls addArrangedSubview:_phase8BundlePathField];

    _phase8ChooseBundleButton = [NSButton buttonWithTitle:@"Choose Bundle…"
                                                   target:self
                                                   action:@selector(choosePhase8BundlePath:)];
    [controls addArrangedSubview:_phase8ChooseBundleButton];

    _phase8ProfilePopup = [[NSPopUpButton alloc] initWithFrame:NSMakeRect(0, 0, 220, 24) pullsDown:NO];
    for (const auto& profile : Phase8ProfileTitles()) {
        [_phase8ProfilePopup addItemWithTitle:ToNSString(profile)];
    }
    [controls addArrangedSubview:_phase8ProfilePopup];

    _phase8EnabledButton = [NSButton checkboxWithTitle:@"Enabled" target:nil action:nil];
    _phase8EnabledButton.state = NSControlStateValueOn;
    [controls addArrangedSubview:_phase8EnabledButton];

    [stack addArrangedSubview:controls];

    NSStackView* watchRules = MakeControlRow();
    [watchRules addArrangedSubview:MakeLabel(@"title",
                                             [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                             [NSColor secondaryLabelColor])];
    _phase8TitleField = MakeMonospacedField(220.0, nil, @"Optional watch title");
    [watchRules addArrangedSubview:_phase8TitleField];

    [watchRules addArrangedSubview:MakeLabel(@"cadence (min)",
                                             [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                             [NSColor secondaryLabelColor])];
    _phase8CadenceMinutesField = MakeMonospacedField(72.0, nil, @"15");
    _phase8CadenceMinutesField.stringValue = ToNSString(std::to_string(tape_phase8::kDefaultEvaluationCadenceMinutes));
    [watchRules addArrangedSubview:_phase8CadenceMinutesField];

    [watchRules addArrangedSubview:MakeLabel(@"min findings",
                                             [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                             [NSColor secondaryLabelColor])];
    _phase8MinimumFindingCountField = MakeMonospacedField(64.0, nil, @"1");
    _phase8MinimumFindingCountField.stringValue = @"1";
    [watchRules addArrangedSubview:_phase8MinimumFindingCountField];

    [watchRules addArrangedSubview:MakeLabel(@"min severity",
                                             [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                             [NSColor secondaryLabelColor])];
    _phase8MinimumSeverityPopup = [[NSPopUpButton alloc] initWithFrame:NSMakeRect(0, 0, 140, 24) pullsDown:NO];
    for (const auto& severity : Phase8MinimumSeverityTitles()) {
        [_phase8MinimumSeverityPopup addItemWithTitle:ToNSString(severity)];
    }
    [watchRules addArrangedSubview:_phase8MinimumSeverityPopup];

    [watchRules addArrangedSubview:MakeLabel(@"required category",
                                             [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                             [NSColor secondaryLabelColor])];
    _phase8RequiredCategoryField = MakeMonospacedField(220.0, nil, @"Optional finding category");
    [watchRules addArrangedSubview:_phase8RequiredCategoryField];

    [stack addArrangedSubview:watchRules];

    NSStackView* actions = MakeControlRow();

    _phase8CreateWatchButton = [NSButton buttonWithTitle:@"Create Watch"
                                                  target:self
                                                  action:@selector(createPhase8WatchDefinition:)];
    [actions addArrangedSubview:_phase8CreateWatchButton];

    _phase8RunDueButton = [NSButton buttonWithTitle:@"Run Due Watches"
                                             target:self
                                             action:@selector(runDuePhase8Watches:)];
    [actions addArrangedSubview:_phase8RunDueButton];

    _phase8EvaluateWatchButton = [NSButton buttonWithTitle:@"Evaluate Selected Watch"
                                                    target:self
                                                    action:@selector(evaluateSelectedPhase8Watch:)];
    _phase8EvaluateWatchButton.enabled = NO;
    [actions addArrangedSubview:_phase8EvaluateWatchButton];

    _phase8OpenAnalysisButton = [NSButton buttonWithTitle:@"Open Linked Analysis"
                                                   target:self
                                                   action:@selector(openSelectedPhase8Analysis:)];
    _phase8OpenAnalysisButton.enabled = NO;
    [actions addArrangedSubview:_phase8OpenAnalysisButton];

    _phase8OpenSourceArtifactButton = [NSButton buttonWithTitle:@"Open Source Artifact"
                                                         target:self
                                                         action:@selector(openSelectedPhase8SourceArtifact:)];
    _phase8OpenSourceArtifactButton.enabled = NO;
    [actions addArrangedSubview:_phase8OpenSourceArtifactButton];

    [stack addArrangedSubview:actions];

    NSStackView* inboxActions = MakeControlRow();
    [inboxActions addArrangedSubview:MakeLabel(@"attention note",
                                               [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                               [NSColor secondaryLabelColor])];
    _phase8AttentionCommentField = MakeMonospacedField(280.0, nil, @"Optional inbox note");
    [inboxActions addArrangedSubview:_phase8AttentionCommentField];

    [inboxActions addArrangedSubview:MakeLabel(@"snooze (min)",
                                               [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                               [NSColor secondaryLabelColor])];
    _phase8SnoozeMinutesField = MakeMonospacedField(72.0, nil, @"60");
    _phase8SnoozeMinutesField.stringValue = ToNSString(std::to_string(tape_phase8::kDefaultAttentionSnoozeMinutes));
    [inboxActions addArrangedSubview:_phase8SnoozeMinutesField];

    _phase8AcknowledgeButton = [NSButton buttonWithTitle:@"Acknowledge"
                                                  target:self
                                                  action:@selector(acknowledgeSelectedPhase8Attention:)];
    _phase8AcknowledgeButton.enabled = NO;
    [inboxActions addArrangedSubview:_phase8AcknowledgeButton];

    _phase8SnoozeButton = [NSButton buttonWithTitle:@"Snooze"
                                             target:self
                                             action:@selector(snoozeSelectedPhase8Attention:)];
    _phase8SnoozeButton.enabled = NO;
    [inboxActions addArrangedSubview:_phase8SnoozeButton];

    _phase8ResolveButton = [NSButton buttonWithTitle:@"Resolve"
                                              target:self
                                              action:@selector(resolveSelectedPhase8Attention:)];
    _phase8ResolveButton.enabled = NO;
    [inboxActions addArrangedSubview:_phase8ResolveButton];

    [stack addArrangedSubview:inboxActions];

    _phase8StateLabel = MakeLabel(@"No Phase 8 watches loaded yet.",
                                  [NSFont systemFontOfSize:12.0 weight:NSFontWeightMedium],
                                  [NSColor secondaryLabelColor]);
    [stack addArrangedSubview:_phase8StateLabel];

    [stack addArrangedSubview:MakeSectionLabel(@"Watch Definitions")];
    _phase8WatchPopup = [[NSPopUpButton alloc] initWithFrame:NSMakeRect(0, 0, 780, 24) pullsDown:NO];
    _phase8WatchPopup.target = self;
    _phase8WatchPopup.action = @selector(phase8WatchSelectionChanged:);
    [_phase8WatchPopup addItemWithTitle:@"No watches yet"];
    _phase8WatchPopup.enabled = NO;
    [stack addArrangedSubview:_phase8WatchPopup];

    [stack addArrangedSubview:MakeSectionLabel(@"Trigger History")];
    NSStackView* triggerFilters = MakeControlRow();
    [triggerFilters addArrangedSubview:MakeLabel(@"attention status",
                                                 [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                                 [NSColor secondaryLabelColor])];
    _phase8TriggerAttentionStatusPopup = [[NSPopUpButton alloc] initWithFrame:NSMakeRect(0, 0, 180, 24) pullsDown:NO];
    for (const auto& title : Phase8TriggerAttentionStatusTitles()) {
        [_phase8TriggerAttentionStatusPopup addItemWithTitle:ToNSString(title)];
    }
    _phase8TriggerAttentionStatusPopup.target = self;
    _phase8TriggerAttentionStatusPopup.action = @selector(refreshPhase8Inbox:);
    [triggerFilters addArrangedSubview:_phase8TriggerAttentionStatusPopup];

    [triggerFilters addArrangedSubview:MakeLabel(@"attention open",
                                                 [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                                 [NSColor secondaryLabelColor])];
    _phase8TriggerAttentionOpenPopup = [[NSPopUpButton alloc] initWithFrame:NSMakeRect(0, 0, 120, 24) pullsDown:NO];
    for (const auto& title : Phase8TriggerAttentionOpenTitles()) {
        [_phase8TriggerAttentionOpenPopup addItemWithTitle:ToNSString(title)];
    }
    _phase8TriggerAttentionOpenPopup.target = self;
    _phase8TriggerAttentionOpenPopup.action = @selector(refreshPhase8Inbox:);
    [triggerFilters addArrangedSubview:_phase8TriggerAttentionOpenPopup];

    [stack addArrangedSubview:triggerFilters];

    _phase8TriggerPopup = [[NSPopUpButton alloc] initWithFrame:NSMakeRect(0, 0, 780, 24) pullsDown:NO];
    _phase8TriggerPopup.target = self;
    _phase8TriggerPopup.action = @selector(phase8TriggerSelectionChanged:);
    [_phase8TriggerPopup addItemWithTitle:@"No trigger runs yet"];
    _phase8TriggerPopup.enabled = NO;
    [stack addArrangedSubview:_phase8TriggerPopup];

    [stack addArrangedSubview:MakeSectionLabel(@"Attention Inbox")];
    _phase8AttentionPopup = [[NSPopUpButton alloc] initWithFrame:NSMakeRect(0, 0, 780, 24) pullsDown:NO];
    _phase8AttentionPopup.target = self;
    _phase8AttentionPopup.action = @selector(phase8AttentionSelectionChanged:);
    [_phase8AttentionPopup addItemWithTitle:@"No attention items yet"];
    _phase8AttentionPopup.enabled = NO;
    [stack addArrangedSubview:_phase8AttentionPopup];

    [stack addArrangedSubview:MakeSectionLabel(@"Phase 8 Detail")];
    _phase8TextView = MakeReadOnlyTextView();
    _phase8TextView.string = @"Refresh Phase 8 to review watch definitions, due watches, trigger history, and auto-analysis attention items.";
    [stack addArrangedSubview:MakeScrollView(_phase8TextView, 360.0)];

    NSTabViewItem* item = [[NSTabViewItem alloc] initWithIdentifier:@"Phase8Pane"];
    item.label = @"Inbox";
    item.view = pane;
    return item;
}

- (void)choosePhase8BundlePath:(id)sender {
    (void)sender;
    NSOpenPanel* panel = [NSOpenPanel openPanel];
    panel.canChooseFiles = YES;
    panel.canChooseDirectories = NO;
    panel.allowsMultipleSelection = NO;
    panel.message = @"Choose a portable Phase 6 case bundle for a Phase 8 watch definition.";
    if ([panel runModal] == NSModalResponseOK && panel.URL != nil && panel.URL.path != nil) {
        _phase8BundlePathField.stringValue = panel.URL.path;
    }
}

- (void)refreshPhase8DetailText {
    _phase8SelectedWatchArtifactId.clear();
    _phase8SelectedTriggerArtifactId.clear();
    _phase8SelectedAnalysisArtifactId.clear();
    _phase8SelectedSourceArtifactId.clear();

    const auto showAttentionSelection = [&]() -> bool {
        const NSInteger attentionSelected = _phase8AttentionPopup.indexOfSelectedItem;
        if (!(_phase8AttentionPopup.enabled &&
              attentionSelected >= 0 &&
              static_cast<std::size_t>(attentionSelected) < _latestPhase8AttentionItems.size())) {
            return false;
        }
        const auto& item = _latestPhase8AttentionItems.at(static_cast<std::size_t>(attentionSelected));
        _phase8SelectedTriggerArtifactId = item.triggerArtifactId;
        _phase8SelectedWatchArtifactId = item.watchArtifactId;
        _phase8SelectedAnalysisArtifactId = item.analysisArtifactId;
        _phase8SelectedSourceArtifactId = item.sourceArtifactId;
        auto triggerIt = std::find_if(_latestPhase8TriggerRuns.begin(),
                                      _latestPhase8TriggerRuns.end(),
                                      [&](const tapescope::Phase8TriggerRunArtifact& trigger) {
                                          return trigger.triggerArtifact.artifactId == item.triggerArtifactId;
                                      });
        if (triggerIt != _latestPhase8TriggerRuns.end()) {
            _phase8TextView.string = ToNSString(DescribePhase8TriggerRun(*triggerIt));
        } else {
            _phase8TextView.string = ToNSString(DescribePhase8AttentionItem(item));
        }
        return true;
    };

    const auto showTriggerSelection = [&]() -> bool {
        const NSInteger triggerSelected = _phase8TriggerPopup.indexOfSelectedItem;
        if (!(_phase8TriggerPopup.enabled &&
              triggerSelected >= 0 &&
              static_cast<std::size_t>(triggerSelected) < _latestPhase8TriggerRuns.size())) {
            return false;
        }
        const auto& artifact = _latestPhase8TriggerRuns.at(static_cast<std::size_t>(triggerSelected));
        _phase8SelectedTriggerArtifactId = artifact.triggerArtifact.artifactId;
        _phase8SelectedWatchArtifactId = artifact.watchArtifact.artifactId;
        _phase8SelectedAnalysisArtifactId = artifact.analysisArtifact.artifactId;
        _phase8SelectedSourceArtifactId = artifact.sourceArtifact.artifactId;
        _phase8TextView.string = ToNSString(DescribePhase8TriggerRun(artifact));
        return true;
    };

    const auto showWatchSelection = [&]() -> bool {
        const NSInteger watchSelected = _phase8WatchPopup.indexOfSelectedItem;
        if (!(_phase8WatchPopup.enabled &&
              watchSelected >= 0 &&
              static_cast<std::size_t>(watchSelected) < _latestPhase8WatchDefinitions.size())) {
            return false;
        }
        const auto& artifact = _latestPhase8WatchDefinitions.at(static_cast<std::size_t>(watchSelected));
        _phase8SelectedWatchArtifactId = artifact.watchArtifact.artifactId;
        _phase8BundlePathField.stringValue = ToNSString(artifact.bundlePath);
        _phase8TitleField.stringValue = ToNSString(artifact.title);
        _phase8CadenceMinutesField.stringValue = ToNSString(std::to_string(artifact.evaluationCadenceMinutes));
        _phase8MinimumFindingCountField.stringValue = ToNSString(std::to_string(artifact.minimumFindingCount));
        const std::string severityChoice = artifact.minimumSeverity.empty() ? "any" : artifact.minimumSeverity;
        [_phase8MinimumSeverityPopup selectItemWithTitle:ToNSString(severityChoice)];
        _phase8RequiredCategoryField.stringValue = ToNSString(artifact.requiredCategory);
        _phase8EnabledButton.state = artifact.enabled ? NSControlStateValueOn : NSControlStateValueOff;
        _phase8TextView.string = ToNSString(DescribePhase8WatchDefinition(artifact));
        return true;
    };

    bool rendered = false;
    bool selectedAttentionActionable = false;
    switch (_phase8SelectionFocus) {
        case kPhase8SelectionFocusAttention:
            rendered = showAttentionSelection() || showTriggerSelection() || showWatchSelection();
            break;
        case kPhase8SelectionFocusTrigger:
            rendered = showTriggerSelection() || showAttentionSelection() || showWatchSelection();
            break;
        case kPhase8SelectionFocusWatch:
            rendered = showWatchSelection() || showAttentionSelection() || showTriggerSelection();
            break;
        default:
            rendered = showAttentionSelection() || showTriggerSelection() || showWatchSelection();
            break;
    }

    if (!rendered) {
        _phase8TextView.string = @"Refresh Phase 8 to review watch definitions, due watches, trigger history, and auto-analysis attention items.";
    }

    if (!_phase8SelectedTriggerArtifactId.empty()) {
        auto triggerIt = std::find_if(_latestPhase8TriggerRuns.begin(),
                                      _latestPhase8TriggerRuns.end(),
                                      [&](const tapescope::Phase8TriggerRunArtifact& trigger) {
                                          return trigger.triggerArtifact.artifactId == _phase8SelectedTriggerArtifactId;
                                      });
        if (triggerIt != _latestPhase8TriggerRuns.end()) {
            selectedAttentionActionable =
                triggerIt->triggerOutcome == tape_phase8::kTriggerOutcomeTriggered && triggerIt->attentionOpen;
        }
    }

    _phase8EvaluateWatchButton.enabled = !_phase8SelectedWatchArtifactId.empty();
    _phase8RunDueButton.enabled = !_phase8InFlight;
    _phase8AcknowledgeButton.enabled = !_phase8InFlight && selectedAttentionActionable;
    _phase8SnoozeButton.enabled = !_phase8InFlight && selectedAttentionActionable;
    _phase8ResolveButton.enabled = !_phase8InFlight && selectedAttentionActionable;
    _phase8OpenAnalysisButton.enabled = !_phase8SelectedAnalysisArtifactId.empty();
    _phase8OpenSourceArtifactButton.enabled = !_phase8SelectedSourceArtifactId.empty();
}

- (void)phase8WatchSelectionChanged:(id)sender {
    (void)sender;
    _phase8SelectionFocus = kPhase8SelectionFocusWatch;
    [self refreshPhase8DetailText];
}

- (void)phase8TriggerSelectionChanged:(id)sender {
    (void)sender;
    _phase8SelectionFocus = kPhase8SelectionFocusTrigger;
    [self refreshPhase8DetailText];
}

- (void)phase8AttentionSelectionChanged:(id)sender {
    (void)sender;
    _phase8SelectionFocus = kPhase8SelectionFocusAttention;
    [self refreshPhase8DetailText];
}

- (void)refreshPhase8Inbox:(id)sender {
    (void)sender;
    if (_phase8InFlight || !_client) {
        return;
    }

    const NSInteger selectedWatchIndex = _phase8WatchPopup.indexOfSelectedItem;
    const std::string selectedWatchArtifactId =
        (selectedWatchIndex >= 0 && static_cast<std::size_t>(selectedWatchIndex) < _latestPhase8WatchDefinitions.size())
            ? _latestPhase8WatchDefinitions.at(static_cast<std::size_t>(selectedWatchIndex)).watchArtifact.artifactId
            : std::string();
    const NSInteger selectedTriggerIndex = _phase8TriggerPopup.indexOfSelectedItem;
    const std::string selectedTriggerFromHistoryId =
        (selectedTriggerIndex >= 0 && static_cast<std::size_t>(selectedTriggerIndex) < _latestPhase8TriggerRuns.size())
            ? _latestPhase8TriggerRuns.at(static_cast<std::size_t>(selectedTriggerIndex)).triggerArtifact.artifactId
            : std::string();
    const NSInteger selectedAttentionIndex = _phase8AttentionPopup.indexOfSelectedItem;
    const std::string selectedTriggerArtifactId =
        (selectedAttentionIndex >= 0 && static_cast<std::size_t>(selectedAttentionIndex) < _latestPhase8AttentionItems.size())
            ? _latestPhase8AttentionItems.at(static_cast<std::size_t>(selectedAttentionIndex)).triggerArtifactId
            : selectedTriggerFromHistoryId;
    const auto triggerSelection = CurrentPhase8TriggerRunSelection(_phase8TriggerAttentionStatusPopup.titleOfSelectedItem,
                                                                   _phase8TriggerAttentionOpenPopup.titleOfSelectedItem);

    _phase8InFlight = YES;
    const std::uint64_t token = [self issueRequestToken:&_phase8RequestToken];
    _phase8RefreshButton.enabled = NO;
    _phase8CreateWatchButton.enabled = NO;
    _phase8RunDueButton.enabled = NO;
    _phase8EvaluateWatchButton.enabled = NO;
    _phase8AcknowledgeButton.enabled = NO;
    _phase8SnoozeButton.enabled = NO;
    _phase8ResolveButton.enabled = NO;
    _phase8StateLabel.stringValue = @"Refreshing Phase 8 inbox…";
    _phase8StateLabel.textColor = [NSColor systemOrangeColor];

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_interactiveQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto watchList = strongSelf->_client->listWatchDefinitionsPayload(25);
        const auto triggerList = strongSelf->_client->listTriggerRunsPayload(triggerSelection);
        const auto attentionList = strongSelf->_client->listAttentionInboxPayload(25);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil || ![innerSelf isRequestTokenCurrent:token storage:&innerSelf->_phase8RequestToken]) {
                return;
            }
            innerSelf->_phase8InFlight = NO;
            innerSelf->_phase8RefreshButton.enabled = YES;
            innerSelf->_phase8CreateWatchButton.enabled = YES;
            innerSelf->_phase8RunDueButton.enabled = YES;
            if (!watchList.ok()) {
                innerSelf->_phase8StateLabel.stringValue = ToNSString(tapescope::QueryClient::describeError(watchList.error));
                innerSelf->_phase8StateLabel.textColor = ErrorColorForKind(watchList.error.kind);
                return;
            }
            if (!triggerList.ok()) {
                innerSelf->_phase8StateLabel.stringValue = ToNSString(tapescope::QueryClient::describeError(triggerList.error));
                innerSelf->_phase8StateLabel.textColor = ErrorColorForKind(triggerList.error.kind);
                return;
            }
            if (!attentionList.ok()) {
                innerSelf->_phase8StateLabel.stringValue = ToNSString(tapescope::QueryClient::describeError(attentionList.error));
                innerSelf->_phase8StateLabel.textColor = ErrorColorForKind(attentionList.error.kind);
                return;
            }

            innerSelf->_latestPhase8WatchDefinitions = watchList.value;
            innerSelf->_latestPhase8TriggerRuns = triggerList.value.artifacts;
            innerSelf->_latestPhase8AttentionItems = attentionList.value;

            [innerSelf->_phase8WatchPopup removeAllItems];
            if (innerSelf->_latestPhase8WatchDefinitions.empty()) {
                [innerSelf->_phase8WatchPopup addItemWithTitle:@"No watches yet"];
                innerSelf->_phase8WatchPopup.enabled = NO;
            } else {
                for (const auto& artifact : innerSelf->_latestPhase8WatchDefinitions) {
                    [innerSelf->_phase8WatchPopup addItemWithTitle:ToNSString(Phase8WatchPopupTitle(artifact))];
                }
                innerSelf->_phase8WatchPopup.enabled = YES;
                std::size_t selectedIndex = 0;
                if (!selectedWatchArtifactId.empty()) {
                    auto it = std::find_if(innerSelf->_latestPhase8WatchDefinitions.begin(),
                                           innerSelf->_latestPhase8WatchDefinitions.end(),
                                           [&](const tapescope::Phase8WatchDefinitionArtifact& artifact) {
                                               return artifact.watchArtifact.artifactId == selectedWatchArtifactId;
                                           });
                    if (it != innerSelf->_latestPhase8WatchDefinitions.end()) {
                        selectedIndex = static_cast<std::size_t>(std::distance(innerSelf->_latestPhase8WatchDefinitions.begin(), it));
                    }
                }
                [innerSelf->_phase8WatchPopup selectItemAtIndex:static_cast<NSInteger>(selectedIndex)];
            }

            [innerSelf->_phase8TriggerPopup removeAllItems];
            if (innerSelf->_latestPhase8TriggerRuns.empty()) {
                [innerSelf->_phase8TriggerPopup addItemWithTitle:@"No trigger runs yet"];
                innerSelf->_phase8TriggerPopup.enabled = NO;
            } else {
                for (const auto& artifact : innerSelf->_latestPhase8TriggerRuns) {
                    [innerSelf->_phase8TriggerPopup addItemWithTitle:ToNSString(Phase8TriggerPopupTitle(artifact))];
                }
                innerSelf->_phase8TriggerPopup.enabled = YES;
                std::size_t selectedIndex = 0;
                if (!selectedTriggerArtifactId.empty()) {
                    auto it = std::find_if(innerSelf->_latestPhase8TriggerRuns.begin(),
                                           innerSelf->_latestPhase8TriggerRuns.end(),
                                           [&](const tapescope::Phase8TriggerRunArtifact& artifact) {
                                               return artifact.triggerArtifact.artifactId == selectedTriggerArtifactId;
                                           });
                    if (it != innerSelf->_latestPhase8TriggerRuns.end()) {
                        selectedIndex = static_cast<std::size_t>(std::distance(innerSelf->_latestPhase8TriggerRuns.begin(), it));
                    }
                }
                [innerSelf->_phase8TriggerPopup selectItemAtIndex:static_cast<NSInteger>(selectedIndex)];
            }

            [innerSelf->_phase8AttentionPopup removeAllItems];
            if (innerSelf->_latestPhase8AttentionItems.empty()) {
                [innerSelf->_phase8AttentionPopup addItemWithTitle:@"No attention items yet"];
                innerSelf->_phase8AttentionPopup.enabled = NO;
            } else {
                for (const auto& item : innerSelf->_latestPhase8AttentionItems) {
                    [innerSelf->_phase8AttentionPopup addItemWithTitle:ToNSString(Phase8AttentionPopupTitle(item))];
                }
                innerSelf->_phase8AttentionPopup.enabled = YES;
                std::size_t selectedIndex = 0;
                if (!selectedTriggerArtifactId.empty()) {
                    auto it = std::find_if(innerSelf->_latestPhase8AttentionItems.begin(),
                                           innerSelf->_latestPhase8AttentionItems.end(),
                                           [&](const tapescope::Phase8AttentionInboxItem& item) {
                                               return item.triggerArtifactId == selectedTriggerArtifactId;
                                           });
                    if (it != innerSelf->_latestPhase8AttentionItems.end()) {
                        selectedIndex = static_cast<std::size_t>(std::distance(innerSelf->_latestPhase8AttentionItems.begin(), it));
                    }
                }
                [innerSelf->_phase8AttentionPopup selectItemAtIndex:static_cast<NSInteger>(selectedIndex)];
            }

            if (innerSelf->_phase8SelectionFocus == kPhase8SelectionFocusAutomatic) {
                innerSelf->_phase8SelectionFocus =
                    innerSelf->_latestPhase8AttentionItems.empty()
                        ? (innerSelf->_latestPhase8TriggerRuns.empty()
                               ? kPhase8SelectionFocusWatch
                               : kPhase8SelectionFocusTrigger)
                        : kPhase8SelectionFocusAttention;
            }
            [innerSelf refreshPhase8DetailText];
            std::size_t openAttentionCount = 0;
            std::size_t suppressedCount = 0;
            std::size_t dueCount = 0;
            const std::string nowUtc = CurrentUtcIso8601Now();
            for (const auto& trigger : innerSelf->_latestPhase8TriggerRuns) {
                if (trigger.attentionOpen) {
                    ++openAttentionCount;
                }
                if (trigger.triggerOutcome == tape_phase8::kTriggerOutcomeSuppressed) {
                    ++suppressedCount;
                }
            }
            for (const auto& watch : innerSelf->_latestPhase8WatchDefinitions) {
                if (watch.enabled && watch.evaluationCadenceMinutes > 0 &&
                    (watch.nextEvaluationAtUtc.empty() || watch.nextEvaluationAtUtc <= nowUtc)) {
                    ++dueCount;
                }
            }
            innerSelf->_phase8StateLabel.stringValue =
                ToNSString("Phase 8 refreshed: " +
                           std::to_string(innerSelf->_latestPhase8WatchDefinitions.size()) + " watch(es), " +
                           std::to_string(dueCount) + " due, " +
                           std::to_string(triggerList.value.matchedCount) + " matched trigger(s), " +
                           std::to_string(innerSelf->_latestPhase8TriggerRuns.size()) + " shown, " +
                           std::to_string(openAttentionCount) + " open attention item(s), " +
                           std::to_string(suppressedCount) + " suppressed trigger(s).");
            innerSelf->_phase8StateLabel.textColor = [NSColor systemGreenColor];
        });
    });
}

- (void)createPhase8WatchDefinition:(id)sender {
    (void)sender;
    if (_phase8InFlight || !_client) {
        return;
    }
    const std::string bundlePath = TrimAscii(ToStdString(_phase8BundlePathField.stringValue));
    if (bundlePath.empty()) {
        _phase8StateLabel.stringValue = @"Choose a Phase 6 case bundle first.";
        _phase8StateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    const std::string title = TrimAscii(ToStdString(_phase8TitleField.stringValue));
    const std::string analysisProfile = SelectedPopupToken(_phase8ProfilePopup);
    const bool enabled = (_phase8EnabledButton.state == NSControlStateValueOn);
    const std::string cadenceMinutesText = TrimAscii(ToStdString(_phase8CadenceMinutesField.stringValue));
    std::size_t evaluationCadenceMinutes = tape_phase8::kDefaultEvaluationCadenceMinutes;
    if (!cadenceMinutesText.empty()) {
        try {
            evaluationCadenceMinutes = static_cast<std::size_t>(std::stoull(cadenceMinutesText));
        } catch (...) {
            _phase8StateLabel.stringValue = @"Cadence must be a non-negative integer number of minutes.";
            _phase8StateLabel.textColor = [NSColor systemRedColor];
            return;
        }
    }
    const std::string minimumFindingCountText = TrimAscii(ToStdString(_phase8MinimumFindingCountField.stringValue));
    std::size_t minimumFindingCount = 1;
    if (!minimumFindingCountText.empty()) {
        try {
            minimumFindingCount = static_cast<std::size_t>(std::stoull(minimumFindingCountText));
        } catch (...) {
            _phase8StateLabel.stringValue = @"Minimum finding count must be a non-negative integer.";
            _phase8StateLabel.textColor = [NSColor systemRedColor];
            return;
        }
    }
    std::string minimumSeverity = SelectedPopupToken(_phase8MinimumSeverityPopup);
    if (minimumSeverity == "any") {
        minimumSeverity.clear();
    }
    const std::string requiredCategory = TrimAscii(ToStdString(_phase8RequiredCategoryField.stringValue));

    _phase8InFlight = YES;
    const std::uint64_t token = [self issueRequestToken:&_phase8RequestToken];
    _phase8RefreshButton.enabled = NO;
    _phase8CreateWatchButton.enabled = NO;
    _phase8RunDueButton.enabled = NO;
    _phase8EvaluateWatchButton.enabled = NO;
    _phase8AcknowledgeButton.enabled = NO;
    _phase8SnoozeButton.enabled = NO;
    _phase8ResolveButton.enabled = NO;
    _phase8StateLabel.stringValue = @"Creating Phase 8 watch definition…";
    _phase8StateLabel.textColor = [NSColor systemOrangeColor];

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_interactiveQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->createWatchDefinitionPayload(bundlePath,
                                                                              analysisProfile,
                                                                              title,
                                                                              enabled,
                                                                              evaluationCadenceMinutes,
                                                                              minimumFindingCount,
                                                                              minimumSeverity,
                                                                              requiredCategory);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil || ![innerSelf isRequestTokenCurrent:token storage:&innerSelf->_phase8RequestToken]) {
                return;
            }
            innerSelf->_phase8InFlight = NO;
            innerSelf->_phase8RefreshButton.enabled = YES;
            innerSelf->_phase8CreateWatchButton.enabled = YES;
            innerSelf->_phase8RunDueButton.enabled = YES;
            if (!result.ok()) {
                innerSelf->_phase8StateLabel.stringValue = ToNSString(tapescope::QueryClient::describeError(result.error));
                innerSelf->_phase8StateLabel.textColor = ErrorColorForKind(result.error.kind);
                [innerSelf refreshPhase8DetailText];
                return;
            }
            innerSelf->_phase8SelectedWatchArtifactId = result.value.artifact.watchArtifact.artifactId;
            innerSelf->_phase8SelectionFocus = kPhase8SelectionFocusWatch;
            innerSelf->_phase8StateLabel.stringValue =
                result.value.created ? @"Phase 8 watch created." : @"Phase 8 watch updated.";
            innerSelf->_phase8StateLabel.textColor = [NSColor systemGreenColor];
            [innerSelf refreshPhase8Inbox:nil];
        });
    });
}

- (void)evaluateSelectedPhase8Watch:(id)sender {
    (void)sender;
    if (_phase8InFlight || !_client) {
        return;
    }
    const NSInteger selected = _phase8WatchPopup.indexOfSelectedItem;
    if (selected < 0 || static_cast<std::size_t>(selected) >= _latestPhase8WatchDefinitions.size()) {
        _phase8StateLabel.stringValue = @"Select a watch definition first.";
        _phase8StateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    const std::string artifactId =
        _latestPhase8WatchDefinitions.at(static_cast<std::size_t>(selected)).watchArtifact.artifactId;
    if (artifactId.empty()) {
        _phase8StateLabel.stringValue = @"Selected watch is missing an artifact id.";
        _phase8StateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    _phase8InFlight = YES;
    const std::uint64_t token = [self issueRequestToken:&_phase8RequestToken];
    _phase8RefreshButton.enabled = NO;
    _phase8CreateWatchButton.enabled = NO;
    _phase8RunDueButton.enabled = NO;
    _phase8EvaluateWatchButton.enabled = NO;
    _phase8AcknowledgeButton.enabled = NO;
    _phase8SnoozeButton.enabled = NO;
    _phase8ResolveButton.enabled = NO;
    _phase8StateLabel.stringValue = @"Evaluating Phase 8 watch…";
    _phase8StateLabel.textColor = [NSColor systemOrangeColor];

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_interactiveQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->evaluateWatchDefinitionPayload(artifactId);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil || ![innerSelf isRequestTokenCurrent:token storage:&innerSelf->_phase8RequestToken]) {
                return;
            }
            innerSelf->_phase8InFlight = NO;
            innerSelf->_phase8RefreshButton.enabled = YES;
            innerSelf->_phase8CreateWatchButton.enabled = YES;
            innerSelf->_phase8RunDueButton.enabled = YES;
            if (!result.ok()) {
                innerSelf->_phase8StateLabel.stringValue = ToNSString(tapescope::QueryClient::describeError(result.error));
                innerSelf->_phase8StateLabel.textColor = ErrorColorForKind(result.error.kind);
                [innerSelf refreshPhase8DetailText];
                return;
            }
            innerSelf->_phase8SelectedTriggerArtifactId = result.value.artifact.triggerArtifact.artifactId;
            innerSelf->_phase8SelectionFocus =
                result.value.artifact.triggerOutcome == tape_phase8::kTriggerOutcomeSuppressed
                    ? kPhase8SelectionFocusTrigger
                    : kPhase8SelectionFocusAttention;
            innerSelf->_phase8StateLabel.stringValue =
                ToNSString(result.value.artifact.triggerOutcome == tape_phase8::kTriggerOutcomeSuppressed
                               ? "Phase 8 watch evaluated without opening attention."
                               : (result.value.created ? "Phase 8 trigger run created."
                                                       : "Phase 8 trigger run reused."));
            innerSelf->_phase8StateLabel.textColor = [NSColor systemGreenColor];
            [innerSelf refreshPhase8Inbox:nil];
        });
    });
}

- (void)runDuePhase8Watches:(id)sender {
    (void)sender;
    if (_phase8InFlight || !_client) {
        return;
    }

    _phase8InFlight = YES;
    const std::uint64_t token = [self issueRequestToken:&_phase8RequestToken];
    _phase8RefreshButton.enabled = NO;
    _phase8CreateWatchButton.enabled = NO;
    _phase8RunDueButton.enabled = NO;
    _phase8EvaluateWatchButton.enabled = NO;
    _phase8AcknowledgeButton.enabled = NO;
    _phase8SnoozeButton.enabled = NO;
    _phase8ResolveButton.enabled = NO;
    _phase8StateLabel.stringValue = @"Running due Phase 8 watches…";
    _phase8StateLabel.textColor = [NSColor systemOrangeColor];

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_interactiveQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->runDueWatchesPayload(25);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil || ![innerSelf isRequestTokenCurrent:token storage:&innerSelf->_phase8RequestToken]) {
                return;
            }
            innerSelf->_phase8InFlight = NO;
            innerSelf->_phase8RefreshButton.enabled = YES;
            innerSelf->_phase8CreateWatchButton.enabled = YES;
            innerSelf->_phase8RunDueButton.enabled = YES;
            if (!result.ok()) {
                innerSelf->_phase8StateLabel.stringValue = ToNSString(tapescope::QueryClient::describeError(result.error));
                innerSelf->_phase8StateLabel.textColor = ErrorColorForKind(result.error.kind);
                [innerSelf refreshPhase8DetailText];
                return;
            }
            if (!result.value.triggerRuns.empty()) {
                innerSelf->_phase8SelectedTriggerArtifactId =
                    result.value.triggerRuns.front().triggerArtifact.artifactId;
                innerSelf->_phase8SelectionFocus =
                    result.value.attentionOpenedCount > 0 ? kPhase8SelectionFocusAttention : kPhase8SelectionFocusTrigger;
            }
            innerSelf->_phase8StateLabel.stringValue =
                ToNSString("Ran " + std::to_string(result.value.evaluatedWatchCount) + " due watch(es): " +
                           std::to_string(result.value.attentionOpenedCount) + " attention item(s), " +
                           std::to_string(result.value.suppressedCount) + " suppressed.");
            innerSelf->_phase8StateLabel.textColor = [NSColor systemGreenColor];
            [innerSelf refreshPhase8Inbox:nil];
        });
    });
}

- (void)acknowledgeSelectedPhase8Attention:(id)sender {
    (void)sender;
    if (_phase8InFlight || !_client) {
        return;
    }
    if (_phase8SelectedTriggerArtifactId.empty()) {
        _phase8StateLabel.stringValue = @"Select an open attention item first.";
        _phase8StateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    auto triggerIt = std::find_if(_latestPhase8TriggerRuns.begin(),
                                  _latestPhase8TriggerRuns.end(),
                                  [&](const tapescope::Phase8TriggerRunArtifact& trigger) {
                                      return trigger.triggerArtifact.artifactId == _phase8SelectedTriggerArtifactId;
                                  });
    if (triggerIt == _latestPhase8TriggerRuns.end() ||
        triggerIt->triggerOutcome != tape_phase8::kTriggerOutcomeTriggered ||
        !triggerIt->attentionOpen) {
        _phase8StateLabel.stringValue = @"Selected trigger is not currently actionable in the inbox.";
        _phase8StateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    const std::string artifactId = _phase8SelectedTriggerArtifactId;
    const std::string comment = TrimAscii(ToStdString(_phase8AttentionCommentField.stringValue));
    _phase8InFlight = YES;
    const std::uint64_t token = [self issueRequestToken:&_phase8RequestToken];
    _phase8RefreshButton.enabled = NO;
    _phase8CreateWatchButton.enabled = NO;
    _phase8RunDueButton.enabled = NO;
    _phase8EvaluateWatchButton.enabled = NO;
    _phase8AcknowledgeButton.enabled = NO;
    _phase8SnoozeButton.enabled = NO;
    _phase8ResolveButton.enabled = NO;
    _phase8StateLabel.stringValue = @"Acknowledging Phase 8 attention…";
    _phase8StateLabel.textColor = [NSColor systemOrangeColor];

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_interactiveQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result =
            strongSelf->_client->acknowledgeAttentionItemPayload(artifactId, kPhase8AttentionActor, comment);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil || ![innerSelf isRequestTokenCurrent:token storage:&innerSelf->_phase8RequestToken]) {
                return;
            }
            innerSelf->_phase8InFlight = NO;
            innerSelf->_phase8RefreshButton.enabled = YES;
            innerSelf->_phase8CreateWatchButton.enabled = YES;
            innerSelf->_phase8RunDueButton.enabled = YES;
            if (!result.ok()) {
                innerSelf->_phase8StateLabel.stringValue = ToNSString(tapescope::QueryClient::describeError(result.error));
                innerSelf->_phase8StateLabel.textColor = ErrorColorForKind(result.error.kind);
                return;
            }
            innerSelf->_phase8SelectedTriggerArtifactId = result.value.triggerArtifact.artifactId;
            innerSelf->_phase8SelectionFocus = kPhase8SelectionFocusTrigger;
            innerSelf->_phase8StateLabel.stringValue = @"Phase 8 attention acknowledged.";
            innerSelf->_phase8StateLabel.textColor = [NSColor systemGreenColor];
            [innerSelf refreshPhase8Inbox:nil];
        });
    });
}

- (void)snoozeSelectedPhase8Attention:(id)sender {
    (void)sender;
    if (_phase8InFlight || !_client) {
        return;
    }
    if (_phase8SelectedTriggerArtifactId.empty()) {
        _phase8StateLabel.stringValue = @"Select an open attention item first.";
        _phase8StateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    auto triggerIt = std::find_if(_latestPhase8TriggerRuns.begin(),
                                  _latestPhase8TriggerRuns.end(),
                                  [&](const tapescope::Phase8TriggerRunArtifact& trigger) {
                                      return trigger.triggerArtifact.artifactId == _phase8SelectedTriggerArtifactId;
                                  });
    if (triggerIt == _latestPhase8TriggerRuns.end() ||
        triggerIt->triggerOutcome != tape_phase8::kTriggerOutcomeTriggered ||
        !triggerIt->attentionOpen) {
        _phase8StateLabel.stringValue = @"Selected trigger is not currently actionable in the inbox.";
        _phase8StateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    const std::string snoozeMinutesText = TrimAscii(ToStdString(_phase8SnoozeMinutesField.stringValue));
    std::size_t snoozeMinutes = tape_phase8::kDefaultAttentionSnoozeMinutes;
    if (!snoozeMinutesText.empty()) {
        try {
            snoozeMinutes = static_cast<std::size_t>(std::stoull(snoozeMinutesText));
        } catch (...) {
            _phase8StateLabel.stringValue = @"Snooze minutes must be a positive integer.";
            _phase8StateLabel.textColor = [NSColor systemRedColor];
            return;
        }
    }
    if (snoozeMinutes == 0) {
        _phase8StateLabel.stringValue = @"Snooze minutes must be greater than zero.";
        _phase8StateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    const std::string artifactId = _phase8SelectedTriggerArtifactId;
    const std::string comment = TrimAscii(ToStdString(_phase8AttentionCommentField.stringValue));
    _phase8InFlight = YES;
    const std::uint64_t token = [self issueRequestToken:&_phase8RequestToken];
    _phase8RefreshButton.enabled = NO;
    _phase8CreateWatchButton.enabled = NO;
    _phase8RunDueButton.enabled = NO;
    _phase8EvaluateWatchButton.enabled = NO;
    _phase8AcknowledgeButton.enabled = NO;
    _phase8SnoozeButton.enabled = NO;
    _phase8ResolveButton.enabled = NO;
    _phase8StateLabel.stringValue = @"Snoozing Phase 8 attention…";
    _phase8StateLabel.textColor = [NSColor systemOrangeColor];

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_interactiveQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result =
            strongSelf->_client->snoozeAttentionItemPayload(artifactId, snoozeMinutes, kPhase8AttentionActor, comment);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil || ![innerSelf isRequestTokenCurrent:token storage:&innerSelf->_phase8RequestToken]) {
                return;
            }
            innerSelf->_phase8InFlight = NO;
            innerSelf->_phase8RefreshButton.enabled = YES;
            innerSelf->_phase8CreateWatchButton.enabled = YES;
            innerSelf->_phase8RunDueButton.enabled = YES;
            if (!result.ok()) {
                innerSelf->_phase8StateLabel.stringValue = ToNSString(tapescope::QueryClient::describeError(result.error));
                innerSelf->_phase8StateLabel.textColor = ErrorColorForKind(result.error.kind);
                return;
            }
            innerSelf->_phase8SelectedTriggerArtifactId = result.value.triggerArtifact.artifactId;
            innerSelf->_phase8SelectionFocus = kPhase8SelectionFocusTrigger;
            innerSelf->_phase8StateLabel.stringValue =
                ToNSString("Phase 8 attention snoozed for " + std::to_string(snoozeMinutes) + " minute(s).");
            innerSelf->_phase8StateLabel.textColor = [NSColor systemGreenColor];
            [innerSelf refreshPhase8Inbox:nil];
        });
    });
}

- (void)resolveSelectedPhase8Attention:(id)sender {
    (void)sender;
    if (_phase8InFlight || !_client) {
        return;
    }
    if (_phase8SelectedTriggerArtifactId.empty()) {
        _phase8StateLabel.stringValue = @"Select an open attention item first.";
        _phase8StateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    auto triggerIt = std::find_if(_latestPhase8TriggerRuns.begin(),
                                  _latestPhase8TriggerRuns.end(),
                                  [&](const tapescope::Phase8TriggerRunArtifact& trigger) {
                                      return trigger.triggerArtifact.artifactId == _phase8SelectedTriggerArtifactId;
                                  });
    if (triggerIt == _latestPhase8TriggerRuns.end() ||
        triggerIt->triggerOutcome != tape_phase8::kTriggerOutcomeTriggered ||
        !triggerIt->attentionOpen) {
        _phase8StateLabel.stringValue = @"Selected trigger is not currently actionable in the inbox.";
        _phase8StateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    const std::string artifactId = _phase8SelectedTriggerArtifactId;
    const std::string comment = TrimAscii(ToStdString(_phase8AttentionCommentField.stringValue));
    _phase8InFlight = YES;
    const std::uint64_t token = [self issueRequestToken:&_phase8RequestToken];
    _phase8RefreshButton.enabled = NO;
    _phase8CreateWatchButton.enabled = NO;
    _phase8RunDueButton.enabled = NO;
    _phase8EvaluateWatchButton.enabled = NO;
    _phase8AcknowledgeButton.enabled = NO;
    _phase8SnoozeButton.enabled = NO;
    _phase8ResolveButton.enabled = NO;
    _phase8StateLabel.stringValue = @"Resolving Phase 8 attention…";
    _phase8StateLabel.textColor = [NSColor systemOrangeColor];

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_interactiveQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result =
            strongSelf->_client->resolveAttentionItemPayload(artifactId, kPhase8AttentionActor, comment);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil || ![innerSelf isRequestTokenCurrent:token storage:&innerSelf->_phase8RequestToken]) {
                return;
            }
            innerSelf->_phase8InFlight = NO;
            innerSelf->_phase8RefreshButton.enabled = YES;
            innerSelf->_phase8CreateWatchButton.enabled = YES;
            innerSelf->_phase8RunDueButton.enabled = YES;
            if (!result.ok()) {
                innerSelf->_phase8StateLabel.stringValue = ToNSString(tapescope::QueryClient::describeError(result.error));
                innerSelf->_phase8StateLabel.textColor = ErrorColorForKind(result.error.kind);
                return;
            }
            innerSelf->_phase8SelectedTriggerArtifactId = result.value.triggerArtifact.artifactId;
            innerSelf->_phase8SelectionFocus = kPhase8SelectionFocusTrigger;
            innerSelf->_phase8StateLabel.stringValue = @"Phase 8 attention resolved.";
            innerSelf->_phase8StateLabel.textColor = [NSColor systemGreenColor];
            [innerSelf refreshPhase8Inbox:nil];
        });
    });
}

- (void)openSelectedPhase8Analysis:(id)sender {
    (void)sender;
    if (_phase8SelectedAnalysisArtifactId.empty()) {
        _phase8StateLabel.stringValue = @"No linked analysis artifact is selected.";
        _phase8StateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    if (_tabView != nil) {
        [_tabView selectTabViewItemWithIdentifier:@"Phase7Pane"];
    }
    [self openPhase7AnalysisArtifactId:_phase8SelectedAnalysisArtifactId];
}

- (void)openSelectedPhase8SourceArtifact:(id)sender {
    (void)sender;
    if (_phase8SelectedSourceArtifactId.empty()) {
        _phase8StateLabel.stringValue = @"No linked source artifact is selected.";
        _phase8StateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    _artifactIdField.stringValue = ToNSString(_phase8SelectedSourceArtifactId);
    if (_tabView != nil) {
        [_tabView selectTabViewItemWithIdentifier:@"ArtifactPane"];
    }
    [self fetchArtifact:nil];
}

@end
