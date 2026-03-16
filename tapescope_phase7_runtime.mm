#import "tapescope_window_internal.h"

#include "tape_phase7_runtime_bridge.h"
#include "tapescope_support.h"

#include <algorithm>
#include <sstream>
#include <utility>
#include <vector>

namespace {

using namespace tapescope_support;

constexpr int kTapeScopeRuntimeClientIdOffset = 700;

tape_phase7::RuntimeRecoveryBacklogSummary summarizeLoadedRuntimeRecoveryBacklog(
    const std::vector<tapescope::Phase7ExecutionJournalArtifact>& journals,
    const std::vector<tapescope::Phase7ExecutionApplyArtifact>& applies) {
    tape_phase7::RuntimeRecoveryBacklogSummary summary;
    summary.journalArtifactCount = journals.size();
    summary.applyArtifactCount = applies.size();
    for (const auto& journal : journals) {
        const auto recovery = tape_phase7::summarizeExecutionJournalRecovery(journal);
        summary.runtimeBackedSubmittedJournalEntryCount += recovery.runtimeBackedSubmittedCount;
        summary.staleRuntimeBackedJournalEntryCount += recovery.staleRuntimeBackedCount;
        if (recovery.recoveryRequired) {
            ++summary.recoveryRequiredJournalCount;
        }
        if (recovery.staleRecoveryRequired) {
            ++summary.staleRecoveryRequiredJournalCount;
        }
    }
    for (const auto& apply : applies) {
        const auto recovery = tape_phase7::summarizeExecutionApplyRecovery(apply);
        summary.runtimeBackedSubmittedApplyEntryCount += recovery.runtimeBackedSubmittedCount;
        summary.staleRuntimeBackedApplyEntryCount += recovery.staleRuntimeBackedCount;
        if (recovery.recoveryRequired) {
            ++summary.recoveryRequiredApplyCount;
        }
        if (recovery.staleRecoveryRequired) {
            ++summary.staleRecoveryRequiredApplyCount;
        }
    }
    summary.recoveryRequired =
        summary.recoveryRequiredJournalCount > 0 || summary.recoveryRequiredApplyCount > 0;
    summary.staleRecoveryRequired =
        summary.staleRecoveryRequiredJournalCount > 0 || summary.staleRecoveryRequiredApplyCount > 0;
    return summary;
}

std::string phase7RuntimeStatusText(TradingRuntimeHost* host,
                                    int preferredClientId,
                                    const tape_phase7::RuntimeRecoveryBacklogSummary& backlog) {
    if (host == nullptr || !host->isStarted() || host->runtime() == nullptr) {
        const RuntimeConnectionConfig current = captureRuntimeConnectionConfig();
        const int suggestedClientId =
            preferredClientId > 0 ? preferredClientId : std::max(1, current.clientId + kTapeScopeRuntimeClientIdOffset);
        std::ostringstream out;
        out << "Local runtime bridge is stopped. Start it to use an isolated TapeScope runtime"
            << " (client_id " << suggestedClientId << ", websocket/controller disabled).";
        return out.str();
    }

    const RuntimePresentationSnapshot snapshot = host->runtime()->capturePresentationSnapshot(std::string(), 0);
    const auto reconcilingCount = static_cast<std::size_t>(std::count_if(
        snapshot.orders.begin(),
        snapshot.orders.end(),
        [](const auto& item) { return item.second.localState == LocalOrderState::NeedsReconciliation; }));
    const auto manualReviewCount = static_cast<std::size_t>(std::count_if(
        snapshot.orders.begin(),
        snapshot.orders.end(),
        [](const auto& item) { return item.second.localState == LocalOrderState::NeedsManualReview; }));

    std::ostringstream out;
    out << "Local runtime bridge active: client_id " << snapshot.connection.clientId << ", broker "
        << (snapshot.status.connected ? "connected" : "disconnected")
        << ", session " << (snapshot.status.sessionReady ? "ready" : "not ready")
        << ", reconciling=" << reconcilingCount
        << ", manual_review=" << manualReviewCount;
    if (backlog.recoveryRequired) {
        out << ", recovery_backlog=" << backlog.recoveryRequiredJournalCount << " journal"
            << (backlog.recoveryRequiredJournalCount == 1 ? "" : "s")
            << "/" << backlog.recoveryRequiredApplyCount << " apply"
            << (backlog.recoveryRequiredApplyCount == 1 ? "" : "s");
        if (backlog.staleRecoveryRequired) {
            out << " (stale)";
        }
    }
    if (!snapshot.activeSymbol.empty()) {
        out << ", active_symbol=" << snapshot.activeSymbol;
    }
    return out.str();
}

std::string phase7StartupRecoverySummary(const tape_phase7::RuntimeStartupRecoveryPlan& plan) {
    if (plan.startupAction == "none") {
        return "No startup recovery is needed.";
    }
    std::ostringstream out;
    out << "Recovery backlog: "
        << plan.backlog.recoveryRequiredJournalCount << " journal"
        << (plan.backlog.recoveryRequiredJournalCount == 1 ? "" : "s")
        << ", " << plan.backlog.recoveryRequiredApplyCount << " apply artifact"
        << (plan.backlog.recoveryRequiredApplyCount == 1 ? "" : "s") << ". ";
    out << plan.detail;
    return out.str();
}

NSColor* phase7RuntimeStatusColor(TradingRuntimeHost* host,
                                  const tape_phase7::RuntimeRecoveryBacklogSummary& backlog) {
    if (host == nullptr || !host->isStarted() || host->runtime() == nullptr) {
        return TapeInkMutedColor();
    }
    const RuntimePresentationSnapshot snapshot = host->runtime()->capturePresentationSnapshot(std::string(), 0);
    if (backlog.staleRecoveryRequired) {
        return [NSColor systemOrangeColor];
    }
    if (backlog.recoveryRequired) {
        return [NSColor systemYellowColor];
    }
    if (snapshot.status.connected && snapshot.status.sessionReady) {
        return [NSColor systemGreenColor];
    }
    if (snapshot.status.connected) {
        return [NSColor systemOrangeColor];
    }
    return [NSColor systemRedColor];
}

RuntimeConnectionConfig tapescopeRuntimeBridgeConfig(int preferredClientId) {
    RuntimeConnectionConfig config = captureRuntimeConnectionConfig();
    config.websocketEnabled = false;
    config.controllerEnabled = false;
    config.clientId =
        preferredClientId > 0 ? preferredClientId : std::max(1, config.clientId + kTapeScopeRuntimeClientIdOffset);
    return config;
}

std::vector<std::string> selectedJournalEntryIdsWithStatus(
    NSTableView* tableView,
    const std::vector<tapescope::Phase7ExecutionJournalEntry>& entries,
    std::string_view requiredStatus) {
    std::vector<std::string> selectedIds;
    if (tableView == nil) {
        return selectedIds;
    }
    NSIndexSet* indexes = tableView.selectedRowIndexes;
    for (NSUInteger idx = indexes.firstIndex; idx != NSNotFound; idx = [indexes indexGreaterThanIndex:idx]) {
        if (idx >= entries.size()) {
            continue;
        }
        const auto& entry = entries.at(idx);
        if (entry.executionStatus == requiredStatus) {
            selectedIds.push_back(entry.journalEntryId);
        }
    }
    return selectedIds;
}

std::vector<std::string> allJournalEntryIdsWithStatus(
    const std::vector<tapescope::Phase7ExecutionJournalEntry>& entries,
    std::string_view requiredStatus) {
    std::vector<std::string> ids;
    for (const auto& entry : entries) {
        if (entry.executionStatus == requiredStatus) {
            ids.push_back(entry.journalEntryId);
        }
    }
    return ids;
}

bool anyJournalEntryWithStatus(const std::vector<tapescope::Phase7ExecutionJournalEntry>& entries,
                               std::string_view requiredStatus) {
    return std::any_of(entries.begin(), entries.end(), [&](const auto& entry) {
        return entry.executionStatus == requiredStatus;
    });
}

NSIndexSet* indexSetForJournalEntryIds(const std::vector<std::string>& selectedIds,
                                       const std::vector<tapescope::Phase7ExecutionJournalEntry>& entries) {
    NSMutableIndexSet* indexes = [NSMutableIndexSet indexSet];
    if (selectedIds.empty()) {
        return indexes;
    }
    for (std::size_t index = 0; index < entries.size(); ++index) {
        if (std::find(selectedIds.begin(), selectedIds.end(), entries[index].journalEntryId) != selectedIds.end()) {
            [indexes addIndex:index];
        }
    }
    return indexes;
}

NSIndexSet* indexSetForApplyEntryIds(const std::vector<std::string>& selectedIds,
                                     const std::vector<tapescope::Phase7ExecutionApplyEntry>& entries) {
    NSMutableIndexSet* indexes = [NSMutableIndexSet indexSet];
    if (selectedIds.empty()) {
        return indexes;
    }
    for (std::size_t index = 0; index < entries.size(); ++index) {
        if (std::find(selectedIds.begin(), selectedIds.end(), entries[index].applyEntryId) != selectedIds.end()) {
            [indexes addIndex:index];
        }
    }
    return indexes;
}

NSInteger rowForJournalArtifactId(const std::string& artifactId,
                                  const std::vector<tapescope::Phase7ExecutionJournalArtifact>& artifacts) {
    if (artifactId.empty()) {
        return -1;
    }
    const auto it = std::find_if(artifacts.begin(), artifacts.end(), [&](const auto& item) {
        return item.journalArtifact.artifactId == artifactId;
    });
    return it == artifacts.end() ? -1 : static_cast<NSInteger>(std::distance(artifacts.begin(), it));
}

NSInteger rowForApplyArtifactId(const std::string& artifactId,
                                const std::vector<tapescope::Phase7ExecutionApplyArtifact>& artifacts) {
    if (artifactId.empty()) {
        return -1;
    }
    const auto it = std::find_if(artifacts.begin(), artifacts.end(), [&](const auto& item) {
        return item.applyArtifact.artifactId == artifactId;
    });
    return it == artifacts.end() ? -1 : static_cast<NSInteger>(std::distance(artifacts.begin(), it));
}

void upsertJournalArtifact(tapescope::Phase7ExecutionJournalArtifact artifact,
                           std::vector<tapescope::Phase7ExecutionJournalArtifact>* artifacts) {
    if (artifacts == nullptr) {
        return;
    }
    const auto it = std::find_if(artifacts->begin(), artifacts->end(), [&](const auto& item) {
        return item.journalArtifact.artifactId == artifact.journalArtifact.artifactId;
    });
    if (it == artifacts->end()) {
        artifacts->insert(artifacts->begin(), std::move(artifact));
    } else {
        *it = std::move(artifact);
    }
}

void upsertApplyArtifact(tapescope::Phase7ExecutionApplyArtifact artifact,
                         std::vector<tapescope::Phase7ExecutionApplyArtifact>* artifacts) {
    if (artifacts == nullptr) {
        return;
    }
    const auto it = std::find_if(artifacts->begin(), artifacts->end(), [&](const auto& item) {
        return item.applyArtifact.artifactId == artifact.applyArtifact.artifactId;
    });
    if (it == artifacts->end()) {
        artifacts->insert(artifacts->begin(), std::move(artifact));
    } else {
        *it = std::move(artifact);
    }
}

} // namespace

@implementation TapeScopeWindowController (Phase7Runtime)

- (void)updatePhase7RuntimeControls {
    const auto backlog = summarizeLoadedRuntimeRecoveryBacklog(_latestPhase7ExecutionJournals,
                                                               _latestPhase7ExecutionApplies);
    if (_phase7RuntimeStatusLabel != nil) {
        _phase7RuntimeStatusLabel.stringValue =
            ToNSString(phase7RuntimeStatusText(_phase7RuntimeHost.get(), _phase7RuntimeClientId, backlog));
        _phase7RuntimeStatusLabel.textColor = phase7RuntimeStatusColor(_phase7RuntimeHost.get(), backlog);
    }

    const BOOL busy = _phase7InFlight;
    const bool runtimeStarted = _phase7RuntimeHost && _phase7RuntimeHost->isStarted() && _phase7RuntimeHost->runtime() != nullptr;
    const bool journalSelected = _phase7SelectionIsJournal == YES &&
        _phase7JournalTableView.selectedRow >= 0 &&
        static_cast<std::size_t>(_phase7JournalTableView.selectedRow) < _latestPhase7ExecutionJournals.size();
    const bool applySelected = _phase7SelectionIsApply == YES &&
        _phase7ApplyTableView.selectedRow >= 0 &&
        static_cast<std::size_t>(_phase7ApplyTableView.selectedRow) < _latestPhase7ExecutionApplies.size();

    const bool hasQueuedJournalEntries = journalSelected &&
        anyJournalEntryWithStatus(_phase7VisibleJournalEntries, tape_phase7::kExecutionEntryStatusQueued);
    const bool hasSubmittedJournalEntries = journalSelected &&
        anyJournalEntryWithStatus(_phase7VisibleJournalEntries, tape_phase7::kExecutionEntryStatusSubmitted);

    _phase7RuntimeStartButton.enabled = !busy && !runtimeStarted;
    _phase7RuntimeStopButton.enabled = !busy && runtimeStarted;
    _phase7RuntimeDispatchButton.enabled = !busy && runtimeStarted && hasQueuedJournalEntries;
    _phase7RuntimeReconcileButton.enabled = !busy && runtimeStarted && hasSubmittedJournalEntries;
    _phase7RuntimeSweepButton.enabled = !busy && runtimeStarted;
    _phase7RuntimeSyncApplyButton.enabled = !busy && applySelected;
    _phase7RuntimeSweepButton.title = backlog.recoveryRequired ? @"Recover Pending" : @"Runtime Sweep";
}

- (void)startPhase7RuntimeHost:(id)sender {
    (void)sender;
    if (_phase7InFlight || _phase7RuntimeHost) {
        return;
    }

    RuntimeConnectionConfig config = tapescopeRuntimeBridgeConfig(_phase7RuntimeClientId);
    _phase7RuntimeClientId = config.clientId;
    updateRuntimeConnectionConfig(config);

    _phase7InFlight = YES;
    _phase7StateLabel.stringValue = @"Starting isolated TapeScope runtime bridge…";
    _phase7StateLabel.textColor = [NSColor systemOrangeColor];
    [self updatePhase7RuntimeControls];

    TapeScopeWindowController* strongSelf = self;
    dispatch_async(_interactiveQueue, ^{
        TradingRuntimeHost* host = new TradingRuntimeHost();
        const bool connected = host->start([strongSelf]() {
            dispatch_async(dispatch_get_main_queue(), ^{
                if (strongSelf != nil) {
                    [strongSelf updatePhase7RuntimeControls];
                }
            });
        }, [](TradingRuntimeControllerAction) {
        });
        host->setControllerVibration(false);

        dispatch_async(dispatch_get_main_queue(), ^{
            if (strongSelf == nil) {
                host->shutdown();
                delete host;
                return;
            }
            strongSelf->_phase7RuntimeHost.reset(host);
            strongSelf->_phase7InFlight = NO;
            [strongSelf updatePhase7RuntimeControls];
            strongSelf->_phase7StateLabel.stringValue =
                connected
                    ? @"Local TapeScope runtime bridge started."
                    : @"Local TapeScope runtime bridge started without a broker connection.";
            strongSelf->_phase7StateLabel.textColor =
                connected ? [NSColor systemGreenColor] : [NSColor systemOrangeColor];
            const std::string startupActor = TrimAscii(ToStdString(strongSelf->_phase7ExecutionActorField.stringValue));

            dispatch_async(strongSelf->_artifactQueue, ^{
                tape_phase7::RuntimeStartupRecoveryPlan startupPlan;
                std::string errorCode;
                std::string errorMessage;
                const bool backlogOk =
                    tape_phase7::planRuntimeRecoveryStartup(!startupActor.empty(),
                                                            &startupPlan,
                                                            &errorCode,
                                                            &errorMessage);

                dispatch_async(dispatch_get_main_queue(), ^{
                    if (strongSelf == nil || strongSelf->_phase7RuntimeHost.get() != host) {
                        return;
                    }
                    [strongSelf updatePhase7RuntimeControls];
                    if (!backlogOk) {
                        strongSelf->_phase7StateLabel.stringValue =
                            connected
                                ? @"Local TapeScope runtime bridge started; recovery backlog inspection failed."
                                : @"Local TapeScope runtime bridge started without a broker connection; recovery backlog inspection failed.";
                        strongSelf->_phase7StateLabel.textColor = [NSColor systemOrangeColor];
                        return;
                    }
                    if (startupPlan.startupAction != "none") {
                        std::string status =
                            connected
                                ? "Local TapeScope runtime bridge started. "
                                : "Local TapeScope runtime bridge started without a broker connection. ";
                        status += phase7StartupRecoverySummary(startupPlan);
                        if (startupPlan.recoverySweepRecommended) {
                            status += " Running startup recovery sweep…";
                            strongSelf->_phase7StateLabel.stringValue = ToNSString(status);
                            strongSelf->_phase7StateLabel.textColor =
                                startupPlan.manualAttentionRecommended
                                    ? [NSColor systemOrangeColor]
                                    : [NSColor systemYellowColor];
                            [strongSelf sweepPhase7ExecutionArtifactsViaRuntime:nil];
                            return;
                        }
                        strongSelf->_phase7StateLabel.stringValue = ToNSString(status);
                        strongSelf->_phase7StateLabel.textColor =
                            startupPlan.manualAttentionRecommended
                                ? [NSColor systemOrangeColor]
                                : [NSColor systemYellowColor];
                        return;
                    }
                    strongSelf->_phase7StateLabel.stringValue =
                        connected
                            ? @"Local TapeScope runtime bridge started."
                            : @"Local TapeScope runtime bridge started without a broker connection.";
                    strongSelf->_phase7StateLabel.textColor =
                        connected ? [NSColor systemGreenColor] : [NSColor systemOrangeColor];
                });
            });
        });
    });
}

- (void)stopPhase7RuntimeHost:(id)sender {
    (void)sender;
    if (_phase7InFlight || !_phase7RuntimeHost) {
        return;
    }

    TradingRuntimeHost* host = _phase7RuntimeHost.release();
    _phase7InFlight = YES;
    _phase7StateLabel.stringValue = @"Stopping local TapeScope runtime bridge…";
    _phase7StateLabel.textColor = [NSColor systemOrangeColor];
    [self updatePhase7RuntimeControls];

    TapeScopeWindowController* strongSelf = self;
    dispatch_async(_interactiveQueue, ^{
        host->shutdown();
        dispatch_async(dispatch_get_main_queue(), ^{
            if (strongSelf == nil) {
                delete host;
                return;
            }
            delete host;
            strongSelf->_phase7InFlight = NO;
            strongSelf->_phase7StateLabel.stringValue = @"Local TapeScope runtime bridge stopped.";
            strongSelf->_phase7StateLabel.textColor = TapeInkMutedColor();
            [strongSelf updatePhase7RuntimeControls];
        });
    });
}

- (void)shutdownPhase7RuntimeHost {
    if (_phase7RuntimeHost) {
        _phase7RuntimeHost->shutdown();
        _phase7RuntimeHost.reset();
    }
}

- (void)dispatchSelectedPhase7JournalEntriesViaRuntime:(id)sender {
    (void)sender;
    if (_phase7InFlight || !_phase7RuntimeHost || !_phase7RuntimeHost->runtime()) {
        return;
    }

    const NSInteger journalSelected = _phase7JournalTableView.selectedRow;
    if (journalSelected < 0 || static_cast<std::size_t>(journalSelected) >= _latestPhase7ExecutionJournals.size()) {
        _phase7StateLabel.stringValue = @"Select a Phase 7 execution journal row first.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    const auto& journal = _latestPhase7ExecutionJournals.at(static_cast<std::size_t>(journalSelected));
    std::vector<std::string> entryIds = selectedJournalEntryIdsWithStatus(
        _phase7ActionTableView,
        _phase7VisibleJournalEntries,
        tape_phase7::kExecutionEntryStatusQueued);
    if (entryIds.empty()) {
        entryIds = allJournalEntryIdsWithStatus(_phase7VisibleJournalEntries, tape_phase7::kExecutionEntryStatusQueued);
    }
    if (entryIds.empty()) {
        _phase7StateLabel.stringValue = @"Select a journal with at least one queued runtime-dispatchable entry.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    const std::string actor = TrimAscii(ToStdString(_phase7ExecutionActorField.stringValue));
    const std::string executionCapability = TrimAscii(ToStdString(_phase7ExecutionCapabilityField.stringValue));
    const std::string comment = TrimAscii(ToStdString(_phase7ExecutionCommentField.stringValue));
    if (actor.empty()) {
        _phase7StateLabel.stringValue = @"Runtime dispatch requires an actor.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    if (executionCapability.empty()) {
        _phase7StateLabel.stringValue = @"Runtime dispatch requires an execution capability.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    TradingRuntime* runtime = _phase7RuntimeHost->runtime();
    _phase7InFlight = YES;
    _phase7RefreshButton.enabled = NO;
    _phase7ChooseBundleButton.enabled = NO;
    _phase7RunAnalysisButton.enabled = NO;
    _phase7BuildPlaybookButton.enabled = NO;
    _phase7BuildLedgerButton.enabled = NO;
    _phase7StartJournalButton.enabled = NO;
    _phase7StartApplyButton.enabled = NO;
    _phase7DispatchJournalButton.enabled = NO;
    _phase7RecordExecutionButton.enabled = NO;
    _phase7RecordApplyButton.enabled = NO;
    _phase7StateLabel.stringValue = @"Dispatching queued entries through the local TapeScope runtime bridge…";
    _phase7StateLabel.textColor = [NSColor systemOrangeColor];
    [self updatePhase7RuntimeControls];

    TapeScopeWindowController* strongSelf = self;
    dispatch_async(_artifactQueue, ^{
        tape_phase7::ExecutionJournalArtifact artifact;
        std::vector<std::string> updatedEntryIds;
        std::string auditEventId;
        std::string errorCode;
        std::string errorMessage;
        const bool ok = tape_phase7::dispatchExecutionJournalEntriesViaRuntime(runtime,
                                                                               std::string(),
                                                                               journal.journalArtifact.artifactId,
                                                                               entryIds,
                                                                               actor,
                                                                               executionCapability,
                                                                               comment,
                                                                               &artifact,
                                                                               &updatedEntryIds,
                                                                               &auditEventId,
                                                                               &errorCode,
                                                                               &errorMessage);
        dispatch_async(dispatch_get_main_queue(), ^{
            if (strongSelf == nil) {
                return;
            }
            strongSelf->_phase7InFlight = NO;
            strongSelf->_phase7RefreshButton.enabled = YES;
            strongSelf->_phase7ChooseBundleButton.enabled = YES;
            strongSelf->_phase7RunAnalysisButton.enabled = YES;
            if (!ok) {
                strongSelf->_phase7StateLabel.stringValue = ToNSString(errorCode.empty()
                                                                          ? errorMessage
                                                                          : errorCode + ": " + errorMessage);
                strongSelf->_phase7StateLabel.textColor = [NSColor systemRedColor];
                [strongSelf updatePhase7RuntimeControls];
                return;
            }

            auto it = std::find_if(strongSelf->_latestPhase7ExecutionJournals.begin(),
                                   strongSelf->_latestPhase7ExecutionJournals.end(),
                                   [&](const tapescope::Phase7ExecutionJournalArtifact& item) {
                                       return item.journalArtifact.artifactId == artifact.journalArtifact.artifactId;
                                   });
            std::size_t selectedIndex = 0;
            if (it != strongSelf->_latestPhase7ExecutionJournals.end()) {
                *it = artifact;
                selectedIndex = static_cast<std::size_t>(std::distance(strongSelf->_latestPhase7ExecutionJournals.begin(), it));
            } else {
                strongSelf->_latestPhase7ExecutionJournals.insert(strongSelf->_latestPhase7ExecutionJournals.begin(), artifact);
                selectedIndex = 0;
            }
            strongSelf->_phase7SelectionIsPlaybook = NO;
            strongSelf->_phase7SelectionIsLedger = NO;
            strongSelf->_phase7SelectionIsJournal = YES;
            strongSelf->_phase7SelectionIsApply = NO;
            [strongSelf->_phase7AnalysisTableView deselectAll:nil];
            [strongSelf->_phase7PlaybookTableView deselectAll:nil];
            [strongSelf->_phase7LedgerTableView deselectAll:nil];
            [strongSelf->_phase7ApplyTableView deselectAll:nil];
            [strongSelf->_phase7JournalTableView reloadData];
            [strongSelf->_phase7JournalTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:selectedIndex]
                                            byExtendingSelection:NO];
            [strongSelf refreshPhase7DetailText];
            NSIndexSet* updatedSelection = indexSetForJournalEntryIds(updatedEntryIds, strongSelf->_phase7VisibleJournalEntries);
            if (updatedSelection.count > 0) {
                [strongSelf->_phase7ActionTableView selectRowIndexes:updatedSelection byExtendingSelection:NO];
                [strongSelf refreshPhase7DetailText];
            }
            strongSelf->_phase7StateLabel.stringValue = ToNSString(
                std::string("Runtime-dispatched ") + std::to_string(updatedEntryIds.size()) +
                " execution entr" + (updatedEntryIds.size() == 1 ? "y." : "ies."));
            strongSelf->_phase7StateLabel.textColor = [NSColor systemGreenColor];
            [strongSelf updatePhase7RuntimeControls];
            [strongSelf recordRecentHistoryEntry:tapescope::json{
                {"kind", "phase7_execution_journal"},
                {"target_id", artifact.journalArtifact.artifactId},
                {"artifact_id", artifact.journalArtifact.artifactId},
                {"ledger_artifact_id", artifact.ledgerArtifact.artifactId},
                {"playbook_artifact_id", artifact.playbookArtifact.artifactId},
                {"analysis_artifact_id", artifact.analysisArtifact.artifactId},
                {"source_artifact_id", artifact.sourceArtifact.artifactId},
                {"headline", std::string("Phase 7 execution journal")},
                {"detail", std::string("Runtime-dispatched ") + std::to_string(updatedEntryIds.size()) + " entr" +
                               (updatedEntryIds.size() == 1 ? "y" : "ies")},
                {"first_session_seq", artifact.replayContext.value("requested_window", tapescope::json::object()).value("first_session_seq", 0ULL)},
                {"last_session_seq", artifact.replayContext.value("requested_window", tapescope::json::object()).value("last_session_seq", 0ULL)}
            }];
        });
    });
}

- (void)reconcileSelectedPhase7JournalEntriesViaRuntime:(id)sender {
    (void)sender;
    if (_phase7InFlight || !_phase7RuntimeHost || !_phase7RuntimeHost->runtime()) {
        return;
    }

    const NSInteger journalSelected = _phase7JournalTableView.selectedRow;
    if (journalSelected < 0 || static_cast<std::size_t>(journalSelected) >= _latestPhase7ExecutionJournals.size()) {
        _phase7StateLabel.stringValue = @"Select a Phase 7 execution journal row first.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    const auto& journal = _latestPhase7ExecutionJournals.at(static_cast<std::size_t>(journalSelected));
    std::vector<std::string> entryIds = selectedJournalEntryIdsWithStatus(
        _phase7ActionTableView,
        _phase7VisibleJournalEntries,
        tape_phase7::kExecutionEntryStatusSubmitted);
    if (entryIds.empty()) {
        entryIds = allJournalEntryIdsWithStatus(_phase7VisibleJournalEntries, tape_phase7::kExecutionEntryStatusSubmitted);
    }
    if (entryIds.empty()) {
        _phase7StateLabel.stringValue = @"Select a journal with at least one submitted execution entry to reconcile.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    const std::string actor = TrimAscii(ToStdString(_phase7ExecutionActorField.stringValue));
    const std::string comment = TrimAscii(ToStdString(_phase7ExecutionCommentField.stringValue));
    if (actor.empty()) {
        _phase7StateLabel.stringValue = @"Runtime reconciliation requires an actor.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    TradingRuntime* runtime = _phase7RuntimeHost->runtime();
    _phase7InFlight = YES;
    _phase7RefreshButton.enabled = NO;
    _phase7ChooseBundleButton.enabled = NO;
    _phase7RunAnalysisButton.enabled = NO;
    _phase7BuildPlaybookButton.enabled = NO;
    _phase7BuildLedgerButton.enabled = NO;
    _phase7StartJournalButton.enabled = NO;
    _phase7StartApplyButton.enabled = NO;
    _phase7DispatchJournalButton.enabled = NO;
    _phase7RecordExecutionButton.enabled = NO;
    _phase7RecordApplyButton.enabled = NO;
    _phase7StateLabel.stringValue = @"Reconciling submitted journal entries through the local TapeScope runtime bridge…";
    _phase7StateLabel.textColor = [NSColor systemOrangeColor];
    [self updatePhase7RuntimeControls];

    TapeScopeWindowController* strongSelf = self;
    dispatch_async(_artifactQueue, ^{
        tape_phase7::ExecutionJournalArtifact artifact;
        std::vector<std::string> updatedEntryIds;
        std::string auditEventId;
        std::string errorCode;
        std::string errorMessage;
        const bool ok = tape_phase7::reconcileExecutionJournalEntriesViaRuntime(runtime,
                                                                                std::string(),
                                                                                journal.journalArtifact.artifactId,
                                                                                entryIds,
                                                                                actor,
                                                                                comment,
                                                                                &artifact,
                                                                                &updatedEntryIds,
                                                                                &auditEventId,
                                                                                &errorCode,
                                                                                &errorMessage);
        dispatch_async(dispatch_get_main_queue(), ^{
            if (strongSelf == nil) {
                return;
            }
            strongSelf->_phase7InFlight = NO;
            strongSelf->_phase7RefreshButton.enabled = YES;
            strongSelf->_phase7ChooseBundleButton.enabled = YES;
            strongSelf->_phase7RunAnalysisButton.enabled = YES;
            if (!ok) {
                strongSelf->_phase7StateLabel.stringValue = ToNSString(errorCode.empty()
                                                                          ? errorMessage
                                                                          : errorCode + ": " + errorMessage);
                strongSelf->_phase7StateLabel.textColor = [NSColor systemRedColor];
                [strongSelf updatePhase7RuntimeControls];
                return;
            }

            auto it = std::find_if(strongSelf->_latestPhase7ExecutionJournals.begin(),
                                   strongSelf->_latestPhase7ExecutionJournals.end(),
                                   [&](const tapescope::Phase7ExecutionJournalArtifact& item) {
                                       return item.journalArtifact.artifactId == artifact.journalArtifact.artifactId;
                                   });
            std::size_t selectedIndex = 0;
            if (it != strongSelf->_latestPhase7ExecutionJournals.end()) {
                *it = artifact;
                selectedIndex = static_cast<std::size_t>(std::distance(strongSelf->_latestPhase7ExecutionJournals.begin(), it));
            } else {
                strongSelf->_latestPhase7ExecutionJournals.insert(strongSelf->_latestPhase7ExecutionJournals.begin(), artifact);
                selectedIndex = 0;
            }
            strongSelf->_phase7SelectionIsPlaybook = NO;
            strongSelf->_phase7SelectionIsLedger = NO;
            strongSelf->_phase7SelectionIsJournal = YES;
            strongSelf->_phase7SelectionIsApply = NO;
            [strongSelf->_phase7AnalysisTableView deselectAll:nil];
            [strongSelf->_phase7PlaybookTableView deselectAll:nil];
            [strongSelf->_phase7LedgerTableView deselectAll:nil];
            [strongSelf->_phase7ApplyTableView deselectAll:nil];
            [strongSelf->_phase7JournalTableView reloadData];
            [strongSelf->_phase7JournalTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:selectedIndex]
                                            byExtendingSelection:NO];
            [strongSelf refreshPhase7DetailText];
            NSIndexSet* updatedSelection = indexSetForJournalEntryIds(updatedEntryIds, strongSelf->_phase7VisibleJournalEntries);
            if (updatedSelection.count > 0) {
                [strongSelf->_phase7ActionTableView selectRowIndexes:updatedSelection byExtendingSelection:NO];
                [strongSelf refreshPhase7DetailText];
            }
            strongSelf->_phase7StateLabel.stringValue =
                updatedEntryIds.empty()
                    ? @"Runtime reconciliation found no terminal journal changes yet."
                    : ToNSString(std::string("Runtime-reconciled ") + std::to_string(updatedEntryIds.size()) +
                                 " execution entr" + (updatedEntryIds.size() == 1 ? "y." : "ies."));
            strongSelf->_phase7StateLabel.textColor =
                updatedEntryIds.empty() ? [NSColor systemOrangeColor] : [NSColor systemGreenColor];
            [strongSelf updatePhase7RuntimeControls];
            [strongSelf recordRecentHistoryEntry:tapescope::json{
                {"kind", "phase7_execution_journal"},
                {"target_id", artifact.journalArtifact.artifactId},
                {"artifact_id", artifact.journalArtifact.artifactId},
                {"ledger_artifact_id", artifact.ledgerArtifact.artifactId},
                {"playbook_artifact_id", artifact.playbookArtifact.artifactId},
                {"analysis_artifact_id", artifact.analysisArtifact.artifactId},
                {"source_artifact_id", artifact.sourceArtifact.artifactId},
                {"headline", std::string("Phase 7 execution journal")},
                {"detail", updatedEntryIds.empty()
                               ? std::string("Runtime reconciliation found no terminal updates")
                               : std::string("Runtime reconciled ") + std::to_string(updatedEntryIds.size()) + " entr" +
                                     (updatedEntryIds.size() == 1 ? "y" : "ies")},
                {"first_session_seq", artifact.replayContext.value("requested_window", tapescope::json::object()).value("first_session_seq", 0ULL)},
                {"last_session_seq", artifact.replayContext.value("requested_window", tapescope::json::object()).value("last_session_seq", 0ULL)}
            }];
        });
    });
}

- (void)sweepPhase7ExecutionArtifactsViaRuntime:(id)sender {
    (void)sender;
    if (_phase7InFlight || !_phase7RuntimeHost || !_phase7RuntimeHost->runtime()) {
        return;
    }

    const std::string actor = TrimAscii(ToStdString(_phase7ExecutionActorField.stringValue));
    const std::string comment = TrimAscii(ToStdString(_phase7ExecutionCommentField.stringValue));
    if (actor.empty()) {
        _phase7StateLabel.stringValue = @"Runtime sweep requires an actor.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    std::string selectedJournalArtifactId;
    if (_phase7SelectionIsJournal == YES &&
        _phase7JournalTableView.selectedRow >= 0 &&
        static_cast<std::size_t>(_phase7JournalTableView.selectedRow) < _latestPhase7ExecutionJournals.size()) {
        selectedJournalArtifactId =
            _latestPhase7ExecutionJournals.at(static_cast<std::size_t>(_phase7JournalTableView.selectedRow))
                .journalArtifact.artifactId;
    }
    std::string selectedApplyArtifactId;
    if (_phase7SelectionIsApply == YES &&
        _phase7ApplyTableView.selectedRow >= 0 &&
        static_cast<std::size_t>(_phase7ApplyTableView.selectedRow) < _latestPhase7ExecutionApplies.size()) {
        selectedApplyArtifactId =
            _latestPhase7ExecutionApplies.at(static_cast<std::size_t>(_phase7ApplyTableView.selectedRow))
                .applyArtifact.artifactId;
    }

    TradingRuntime* runtime = _phase7RuntimeHost->runtime();
    _phase7InFlight = YES;
    _phase7RefreshButton.enabled = NO;
    _phase7ChooseBundleButton.enabled = NO;
    _phase7RunAnalysisButton.enabled = NO;
    _phase7BuildPlaybookButton.enabled = NO;
    _phase7BuildLedgerButton.enabled = NO;
    _phase7StartJournalButton.enabled = NO;
    _phase7StartApplyButton.enabled = NO;
    _phase7DispatchJournalButton.enabled = NO;
    _phase7RecordExecutionButton.enabled = NO;
    _phase7RecordApplyButton.enabled = NO;
    _phase7StateLabel.stringValue = @"Sweeping runtime-backed Phase 7 journals and apply artifacts…";
    _phase7StateLabel.textColor = [NSColor systemOrangeColor];
    [self updatePhase7RuntimeControls];

    TapeScopeWindowController* strongSelf = self;
    dispatch_async(_artifactQueue, ^{
        tape_phase7::RuntimeBridgeSweepResult summary;
        std::string errorCode;
        std::string errorMessage;
        bool ok = tape_phase7::reconcileExecutionArtifactsViaRuntime(runtime,
                                                                     actor,
                                                                     comment,
                                                                     &summary,
                                                                     &errorCode,
                                                                     &errorMessage);

        std::vector<tapescope::Phase7ExecutionJournalArtifact> refreshedJournals;
        std::vector<tapescope::Phase7ExecutionApplyArtifact> refreshedApplies;
        if (ok) {
            for (const auto& artifactId : summary.updatedJournalArtifactIds) {
                tape_phase7::ExecutionJournalArtifact journal;
                if (!tape_phase7::loadExecutionJournalArtifact(std::string(), artifactId, &journal, &errorCode, &errorMessage)) {
                    ok = false;
                    break;
                }
                refreshedJournals.push_back(std::move(journal));
            }
        }
        if (ok) {
            for (const auto& artifactId : summary.updatedApplyArtifactIds) {
                tape_phase7::ExecutionApplyArtifact apply;
                if (!tape_phase7::loadExecutionApplyArtifact(std::string(), artifactId, &apply, &errorCode, &errorMessage)) {
                    ok = false;
                    break;
                }
                refreshedApplies.push_back(std::move(apply));
            }
        }

        dispatch_async(dispatch_get_main_queue(), ^{
            if (strongSelf == nil) {
                return;
            }
            strongSelf->_phase7InFlight = NO;
            strongSelf->_phase7RefreshButton.enabled = YES;
            strongSelf->_phase7ChooseBundleButton.enabled = YES;
            strongSelf->_phase7RunAnalysisButton.enabled = YES;
            if (!ok) {
                strongSelf->_phase7StateLabel.stringValue = ToNSString(errorCode.empty()
                                                                          ? errorMessage
                                                                          : errorCode + ": " + errorMessage);
                strongSelf->_phase7StateLabel.textColor = [NSColor systemRedColor];
                [strongSelf updatePhase7RuntimeControls];
                return;
            }

            for (auto& artifact : refreshedJournals) {
                upsertJournalArtifact(std::move(artifact), &strongSelf->_latestPhase7ExecutionJournals);
            }
            for (auto& artifact : refreshedApplies) {
                upsertApplyArtifact(std::move(artifact), &strongSelf->_latestPhase7ExecutionApplies);
            }

            [strongSelf->_phase7JournalTableView reloadData];
            [strongSelf->_phase7ApplyTableView reloadData];

            NSInteger selectedJournalRow =
                rowForJournalArtifactId(selectedJournalArtifactId, strongSelf->_latestPhase7ExecutionJournals);
            NSInteger selectedApplyRow =
                rowForApplyArtifactId(selectedApplyArtifactId, strongSelf->_latestPhase7ExecutionApplies);
            if (selectedJournalRow >= 0) {
                strongSelf->_phase7SelectionIsPlaybook = NO;
                strongSelf->_phase7SelectionIsLedger = NO;
                strongSelf->_phase7SelectionIsJournal = YES;
                strongSelf->_phase7SelectionIsApply = NO;
                [strongSelf->_phase7AnalysisTableView deselectAll:nil];
                [strongSelf->_phase7PlaybookTableView deselectAll:nil];
                [strongSelf->_phase7LedgerTableView deselectAll:nil];
                [strongSelf->_phase7ApplyTableView deselectAll:nil];
                [strongSelf->_phase7JournalTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:selectedJournalRow]
                                                 byExtendingSelection:NO];
            } else if (selectedApplyRow >= 0) {
                strongSelf->_phase7SelectionIsPlaybook = NO;
                strongSelf->_phase7SelectionIsLedger = NO;
                strongSelf->_phase7SelectionIsJournal = NO;
                strongSelf->_phase7SelectionIsApply = YES;
                [strongSelf->_phase7AnalysisTableView deselectAll:nil];
                [strongSelf->_phase7PlaybookTableView deselectAll:nil];
                [strongSelf->_phase7LedgerTableView deselectAll:nil];
                [strongSelf->_phase7JournalTableView deselectAll:nil];
                [strongSelf->_phase7ApplyTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:selectedApplyRow]
                                               byExtendingSelection:NO];
            }

            [strongSelf refreshPhase7DetailText];
            if (selectedJournalRow >= 0 && !summary.updatedJournalEntryIds.empty()) {
                NSIndexSet* updatedSelection =
                    indexSetForJournalEntryIds(summary.updatedJournalEntryIds, strongSelf->_phase7VisibleJournalEntries);
                if (updatedSelection.count > 0) {
                    [strongSelf->_phase7ActionTableView selectRowIndexes:updatedSelection byExtendingSelection:NO];
                    [strongSelf refreshPhase7DetailText];
                }
            } else if (selectedApplyRow >= 0 && !summary.updatedApplyEntryIds.empty()) {
                NSIndexSet* updatedSelection =
                    indexSetForApplyEntryIds(summary.updatedApplyEntryIds, strongSelf->_phase7VisibleApplyEntries);
                if (updatedSelection.count > 0) {
                    [strongSelf->_phase7ActionTableView selectRowIndexes:updatedSelection byExtendingSelection:NO];
                    [strongSelf refreshPhase7DetailText];
                }
            }

            std::ostringstream status;
            status << "Runtime sweep scanned " << summary.scannedJournalCount << " journal"
                   << (summary.scannedJournalCount == 1 ? "" : "s");
            if (summary.updatedJournalCount == 0 && summary.updatedApplyCount == 0) {
                status << " and found no terminal runtime changes.";
                strongSelf->_phase7StateLabel.textColor = [NSColor systemOrangeColor];
            } else {
                status << ", updated " << summary.updatedJournalCount << " journal"
                       << (summary.updatedJournalCount == 1 ? "" : "s")
                       << " and " << summary.updatedApplyCount << " apply artifact"
                       << (summary.updatedApplyCount == 1 ? "" : "s") << ".";
                strongSelf->_phase7StateLabel.textColor = [NSColor systemGreenColor];
            }
            strongSelf->_phase7StateLabel.stringValue = ToNSString(status.str());
            [strongSelf updatePhase7RuntimeControls];
        });
    });
}

- (void)syncSelectedPhase7ApplyFromJournal:(id)sender {
    (void)sender;
    if (_phase7InFlight) {
        return;
    }

    const NSInteger applySelected = _phase7ApplyTableView.selectedRow;
    if (applySelected < 0 || static_cast<std::size_t>(applySelected) >= _latestPhase7ExecutionApplies.size()) {
        _phase7StateLabel.stringValue = @"Select a Phase 7 execution apply row first.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    const auto& apply = _latestPhase7ExecutionApplies.at(static_cast<std::size_t>(applySelected));
    const std::string actor = TrimAscii(ToStdString(_phase7ExecutionActorField.stringValue));
    const std::string comment = TrimAscii(ToStdString(_phase7ExecutionCommentField.stringValue));
    if (actor.empty()) {
        _phase7StateLabel.stringValue = @"Apply synchronization requires an actor.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    _phase7InFlight = YES;
    _phase7RefreshButton.enabled = NO;
    _phase7ChooseBundleButton.enabled = NO;
    _phase7RunAnalysisButton.enabled = NO;
    _phase7BuildPlaybookButton.enabled = NO;
    _phase7BuildLedgerButton.enabled = NO;
    _phase7StartJournalButton.enabled = NO;
    _phase7StartApplyButton.enabled = NO;
    _phase7DispatchJournalButton.enabled = NO;
    _phase7RecordExecutionButton.enabled = NO;
    _phase7RecordApplyButton.enabled = NO;
    _phase7StateLabel.stringValue = @"Synchronizing controlled apply from the linked execution journal…";
    _phase7StateLabel.textColor = [NSColor systemOrangeColor];
    [self updatePhase7RuntimeControls];

    TapeScopeWindowController* strongSelf = self;
    dispatch_async(_artifactQueue, ^{
        tape_phase7::ExecutionApplyArtifact artifact;
        std::vector<std::string> updatedEntryIds;
        std::string auditEventId;
        std::string errorCode;
        std::string errorMessage;
        const bool ok = tape_phase7::synchronizeExecutionApplyFromJournal(std::string(),
                                                                          apply.applyArtifact.artifactId,
                                                                          actor,
                                                                          comment,
                                                                          &artifact,
                                                                          &updatedEntryIds,
                                                                          &auditEventId,
                                                                          &errorCode,
                                                                          &errorMessage);
        dispatch_async(dispatch_get_main_queue(), ^{
            if (strongSelf == nil) {
                return;
            }
            strongSelf->_phase7InFlight = NO;
            strongSelf->_phase7RefreshButton.enabled = YES;
            strongSelf->_phase7ChooseBundleButton.enabled = YES;
            strongSelf->_phase7RunAnalysisButton.enabled = YES;
            if (!ok) {
                strongSelf->_phase7StateLabel.stringValue = ToNSString(errorCode.empty()
                                                                          ? errorMessage
                                                                          : errorCode + ": " + errorMessage);
                strongSelf->_phase7StateLabel.textColor = [NSColor systemRedColor];
                [strongSelf updatePhase7RuntimeControls];
                return;
            }

            auto it = std::find_if(strongSelf->_latestPhase7ExecutionApplies.begin(),
                                   strongSelf->_latestPhase7ExecutionApplies.end(),
                                   [&](const tapescope::Phase7ExecutionApplyArtifact& item) {
                                       return item.applyArtifact.artifactId == artifact.applyArtifact.artifactId;
                                   });
            std::size_t selectedIndex = 0;
            if (it != strongSelf->_latestPhase7ExecutionApplies.end()) {
                *it = artifact;
                selectedIndex = static_cast<std::size_t>(std::distance(strongSelf->_latestPhase7ExecutionApplies.begin(), it));
            } else {
                strongSelf->_latestPhase7ExecutionApplies.insert(strongSelf->_latestPhase7ExecutionApplies.begin(), artifact);
                selectedIndex = 0;
            }
            strongSelf->_phase7SelectionIsPlaybook = NO;
            strongSelf->_phase7SelectionIsLedger = NO;
            strongSelf->_phase7SelectionIsJournal = NO;
            strongSelf->_phase7SelectionIsApply = YES;
            [strongSelf->_phase7AnalysisTableView deselectAll:nil];
            [strongSelf->_phase7PlaybookTableView deselectAll:nil];
            [strongSelf->_phase7LedgerTableView deselectAll:nil];
            [strongSelf->_phase7JournalTableView deselectAll:nil];
            [strongSelf->_phase7ApplyTableView reloadData];
            [strongSelf->_phase7ApplyTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:selectedIndex]
                                              byExtendingSelection:NO];
            [strongSelf refreshPhase7DetailText];
            if (!updatedEntryIds.empty()) {
                NSIndexSet* updatedSelection = indexSetForApplyEntryIds(updatedEntryIds, strongSelf->_phase7VisibleApplyEntries);
                if (updatedSelection.count > 0) {
                    [strongSelf->_phase7ActionTableView selectRowIndexes:updatedSelection byExtendingSelection:NO];
                    [strongSelf refreshPhase7DetailText];
                }
            }
            strongSelf->_phase7StateLabel.stringValue =
                updatedEntryIds.empty()
                    ? @"Controlled apply already matches the linked journal."
                    : ToNSString(std::string("Synchronized ") + std::to_string(updatedEntryIds.size()) +
                                 " apply entr" + (updatedEntryIds.size() == 1 ? "y." : "ies.") +
                                 " from the linked journal.");
            strongSelf->_phase7StateLabel.textColor =
                updatedEntryIds.empty() ? TapeInkMutedColor() : [NSColor systemGreenColor];
            [strongSelf updatePhase7RuntimeControls];
            [strongSelf recordRecentHistoryEntry:tapescope::json{
                {"kind", "phase7_execution_apply"},
                {"target_id", artifact.applyArtifact.artifactId},
                {"artifact_id", artifact.applyArtifact.artifactId},
                {"journal_artifact_id", artifact.journalArtifact.artifactId},
                {"ledger_artifact_id", artifact.ledgerArtifact.artifactId},
                {"playbook_artifact_id", artifact.playbookArtifact.artifactId},
                {"analysis_artifact_id", artifact.analysisArtifact.artifactId},
                {"source_artifact_id", artifact.sourceArtifact.artifactId},
                {"headline", std::string("Phase 7 execution apply")},
                {"detail", updatedEntryIds.empty()
                               ? std::string("Apply already matched linked journal")
                               : std::string("Synchronized ") + std::to_string(updatedEntryIds.size()) +
                                     " entr" + (updatedEntryIds.size() == 1 ? "y" : "ies") + " from linked journal"},
                {"first_session_seq", artifact.replayContext.value("requested_window", tapescope::json::object()).value("first_session_seq", 0ULL)},
                {"last_session_seq", artifact.replayContext.value("requested_window", tapescope::json::object()).value("last_session_seq", 0ULL)}
            }];
        });
    });
}

@end
