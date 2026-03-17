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
        << "Playbook intent stays guarded in TapeScope; this pane supports dry-run planning plus journal-backed controlled apply artifacts.\n";
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
    const auto reviewSummary = tape_phase7::summarizeExecutionLedgerReviewSummary(artifact);
    const auto latestAudit = tape_phase7::latestExecutionLedgerAuditSummary(artifact);
    std::ostringstream out;
    out << tape_phase7::executionLedgerArtifactMarkdown(artifact);
    out << "\nReview summary detail: pending=" << reviewSummary.pendingReviewCount
        << ", approved=" << reviewSummary.approvedCount
        << ", blocked=" << reviewSummary.blockedCount
        << ", needs_info=" << reviewSummary.needsInfoCount
        << ", not_applicable=" << reviewSummary.notApplicableCount
        << ", waiting_approval=" << reviewSummary.waitingApprovalCount
        << ", ready=" << reviewSummary.readyEntryCount
        << ", actionable=" << reviewSummary.actionableEntryCount << "\n";
    out << "Review policy: distinct_reviewers=" << reviewSummary.distinctReviewerCount
        << ", required_approvals=" << reviewSummary.requiredApprovalCount
        << ", ready_for_execution=" << (reviewSummary.readyForExecution ? "true" : "false") << "\n";
    if (latestAudit.is_object() && latestAudit.contains("message") && latestAudit.at("message").is_string()) {
        out << "Latest audit note: " << latestAudit.at("message").get<std::string>() << "\n";
    }
    if (!artifact.generatedAtUtc.empty()) {
        out << "\nGenerated at: " << artifact.generatedAtUtc << "\n";
    }
    if (artifact.manifest.is_object()) {
        out << "\nManifest:\n" << artifact.manifest.dump(2) << "\n";
    }
    return out.str();
}

std::string DescribePhase7ExecutionJournalArtifact(const tapescope::Phase7ExecutionJournalArtifact& artifact) {
    const auto executionSummary = tape_phase7::summarizeExecutionJournalSummary(artifact);
    const auto recoverySummary = tape_phase7::summarizeExecutionJournalRecovery(artifact);
    const auto latestAudit = tape_phase7::latestExecutionJournalAuditSummary(artifact);
    std::ostringstream out;
    out << tape_phase7::executionJournalArtifactMarkdown(artifact);
    out << "\nExecution summary detail: queued=" << executionSummary.queuedCount
        << ", submitted=" << executionSummary.submittedCount
        << ", succeeded=" << executionSummary.succeededCount
        << ", failed=" << executionSummary.failedCount
        << ", cancelled=" << executionSummary.cancelledCount
        << ", terminal=" << executionSummary.terminalCount
        << ", actionable=" << executionSummary.actionableEntryCount
        << ", all_terminal=" << (executionSummary.allTerminal ? "true" : "false") << "\n";
    out << "Initiated by: " << artifact.initiatedBy
        << "\nExecution capability: " << artifact.executionCapability
        << "\nJournal status: " << artifact.journalStatus
        << "\nRuntime recovery: required=" << (recoverySummary.recoveryRequired ? "true" : "false")
        << ", stale=" << (recoverySummary.staleRecoveryRequired ? "true" : "false")
        << ", submitted_runtime_entries=" << recoverySummary.runtimeBackedSubmittedCount
        << ", stale_runtime_entries=" << recoverySummary.staleRuntimeBackedCount << "\n";
    if (latestAudit.is_object() && latestAudit.contains("message") && latestAudit.at("message").is_string()) {
        out << "Latest audit note: " << latestAudit.at("message").get<std::string>() << "\n";
    }
    if (!artifact.generatedAtUtc.empty()) {
        out << "\nGenerated at: " << artifact.generatedAtUtc << "\n";
    }
    if (artifact.manifest.is_object()) {
        out << "\nManifest:\n" << artifact.manifest.dump(2) << "\n";
    }
    return out.str();
}

std::string DescribePhase7ExecutionApplyArtifact(const tapescope::Phase7ExecutionApplyArtifact& artifact) {
    const auto applySummary = tape_phase7::summarizeExecutionApplySummary(artifact);
    const auto recoverySummary = tape_phase7::summarizeExecutionApplyRecovery(artifact);
    const auto latestAudit = tape_phase7::latestExecutionApplyAuditSummary(artifact);
    std::ostringstream out;
    out << tape_phase7::executionApplyArtifactMarkdown(artifact);
    out << "\nApply summary detail: submitted=" << applySummary.submittedCount
        << ", succeeded=" << applySummary.succeededCount
        << ", failed=" << applySummary.failedCount
        << ", cancelled=" << applySummary.cancelledCount
        << ", terminal=" << applySummary.terminalCount
        << ", actionable=" << applySummary.actionableEntryCount
        << ", all_terminal=" << (applySummary.allTerminal ? "true" : "false") << "\n";
    out << "Initiated by: " << artifact.initiatedBy
        << "\nExecution capability: " << artifact.executionCapability
        << "\nApply status: " << artifact.applyStatus
        << "\nRuntime recovery: required=" << (recoverySummary.recoveryRequired ? "true" : "false")
        << ", stale=" << (recoverySummary.staleRecoveryRequired ? "true" : "false")
        << ", submitted_runtime_entries=" << recoverySummary.runtimeBackedSubmittedCount
        << ", stale_runtime_entries=" << recoverySummary.staleRuntimeBackedCount << "\n";
    if (latestAudit.is_object() && latestAudit.contains("message") && latestAudit.at("message").is_string()) {
        out << "Latest audit note: " << latestAudit.at("message").get<std::string>() << "\n";
    }
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

std::string DescribePhase7ExecutionJournalStart(
    const std::string& executionLedgerArtifactId,
    const tapescope::QueryResult<tapescope::Phase7ExecutionJournalStartPayload>& result) {
    if (!result.ok()) {
        return "Phase 7 execution journal start failed for " + executionLedgerArtifactId + "\n" +
               tapescope::QueryClient::describeError(result.error);
    }
    std::ostringstream out;
    out << "Phase 7 execution journal start for " << executionLedgerArtifactId << "\n"
        << "artifact_status: " << (result.value.created ? "created" : "reused") << "\n\n"
        << DescribePhase7ExecutionJournalArtifact(result.value.artifact);
    return out.str();
}

std::string DescribePhase7ExecutionApplyStart(
    const std::string& executionJournalArtifactId,
    const tapescope::QueryResult<tapescope::Phase7ExecutionApplyStartPayload>& result) {
    if (!result.ok()) {
        return "Phase 7 execution apply start failed for " + executionJournalArtifactId + "\n" +
               tapescope::QueryClient::describeError(result.error);
    }
    std::ostringstream out;
    out << "Phase 7 execution apply start for " << executionJournalArtifactId << "\n"
        << "artifact_status: " << (result.value.created ? "created" : "reused") << "\n\n"
        << DescribePhase7ExecutionApplyArtifact(result.value.artifact);
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
    return "Refresh Phase 7 artifacts to browse durable analyses, dry-run playbooks, ledgers, journals, and controlled apply artifacts created from portable bundles.";
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

std::vector<std::string> SelectedPhase7LedgerEntryIds(NSTableView* tableView,
                                                      const std::vector<tapescope::Phase7ExecutionLedgerEntry>& entries) {
    std::vector<std::string> selectedIds;
    if (tableView == nil) {
        return selectedIds;
    }
    NSIndexSet* indexes = tableView.selectedRowIndexes;
    for (NSUInteger idx = indexes.firstIndex; idx != NSNotFound; idx = [indexes indexGreaterThanIndex:idx]) {
        if (idx < entries.size()) {
            selectedIds.push_back(entries.at(idx).entryId);
        }
    }
    return selectedIds;
}

std::vector<std::string> SelectedPhase7JournalEntryIds(NSTableView* tableView,
                                                       const std::vector<tapescope::Phase7ExecutionJournalEntry>& entries) {
    std::vector<std::string> selectedIds;
    if (tableView == nil) {
        return selectedIds;
    }
    NSIndexSet* indexes = tableView.selectedRowIndexes;
    for (NSUInteger idx = indexes.firstIndex; idx != NSNotFound; idx = [indexes indexGreaterThanIndex:idx]) {
        if (idx < entries.size()) {
            selectedIds.push_back(entries.at(idx).journalEntryId);
        }
    }
    return selectedIds;
}

std::vector<std::string> SelectedPhase7SubmittedJournalEntryIds(
    NSTableView* tableView,
    const std::vector<tapescope::Phase7ExecutionJournalEntry>& entries) {
    std::vector<std::string> selectedIds;
    if (tableView == nil) {
        return selectedIds;
    }
    NSIndexSet* indexes = tableView.selectedRowIndexes;
    for (NSUInteger idx = indexes.firstIndex; idx != NSNotFound; idx = [indexes indexGreaterThanIndex:idx]) {
        if (idx < entries.size() && entries.at(idx).executionStatus == tape_phase7::kExecutionEntryStatusSubmitted) {
            selectedIds.push_back(entries.at(idx).journalEntryId);
        }
    }
    return selectedIds;
}

std::vector<std::string> SelectedPhase7QueuedJournalEntryIds(
    NSTableView* tableView,
    const std::vector<tapescope::Phase7ExecutionJournalEntry>& entries) {
    std::vector<std::string> selectedIds;
    if (tableView == nil) {
        return selectedIds;
    }
    NSIndexSet* indexes = tableView.selectedRowIndexes;
    for (NSUInteger idx = indexes.firstIndex; idx != NSNotFound; idx = [indexes indexGreaterThanIndex:idx]) {
        if (idx < entries.size() && entries.at(idx).executionStatus == tape_phase7::kExecutionEntryStatusQueued) {
            selectedIds.push_back(entries.at(idx).journalEntryId);
        }
    }
    return selectedIds;
}

std::vector<std::string> SelectedPhase7ApplyEntryIds(NSTableView* tableView,
                                                     const std::vector<tapescope::Phase7ExecutionApplyEntry>& entries) {
    std::vector<std::string> selectedIds;
    if (tableView == nil) {
        return selectedIds;
    }
    NSIndexSet* indexes = tableView.selectedRowIndexes;
    for (NSUInteger idx = indexes.firstIndex; idx != NSNotFound; idx = [indexes indexGreaterThanIndex:idx]) {
        if (idx < entries.size()) {
            selectedIds.push_back(entries.at(idx).applyEntryId);
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

NSIndexSet* IndexSetForPhase7LedgerEntryIds(const std::vector<std::string>& selectedIds,
                                            const std::vector<tapescope::Phase7ExecutionLedgerEntry>& entries) {
    NSMutableIndexSet* indexes = [NSMutableIndexSet indexSet];
    if (!selectedIds.empty()) {
        for (std::size_t index = 0; index < entries.size(); ++index) {
            if (std::find(selectedIds.begin(), selectedIds.end(), entries[index].entryId) != selectedIds.end()) {
                [indexes addIndex:index];
            }
        }
    }
    return indexes;
}

NSIndexSet* IndexSetForPhase7JournalEntryIds(const std::vector<std::string>& selectedIds,
                                             const std::vector<tapescope::Phase7ExecutionJournalEntry>& entries) {
    NSMutableIndexSet* indexes = [NSMutableIndexSet indexSet];
    if (!selectedIds.empty()) {
        for (std::size_t index = 0; index < entries.size(); ++index) {
            if (std::find(selectedIds.begin(), selectedIds.end(), entries[index].journalEntryId) != selectedIds.end()) {
                [indexes addIndex:index];
            }
        }
    }
    return indexes;
}

NSIndexSet* IndexSetForPhase7ApplyEntryIds(const std::vector<std::string>& selectedIds,
                                           const std::vector<tapescope::Phase7ExecutionApplyEntry>& entries) {
    NSMutableIndexSet* indexes = [NSMutableIndexSet indexSet];
    if (!selectedIds.empty()) {
        for (std::size_t index = 0; index < entries.size(); ++index) {
            if (std::find(selectedIds.begin(), selectedIds.end(), entries[index].applyEntryId) != selectedIds.end()) {
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

std::string DescribePhase7LedgerEntry(const tapescope::Phase7ExecutionLedgerEntry& entry) {
    std::ostringstream out;
    out << "entry_id: " << entry.entryId << "\n"
        << "action_id: " << entry.actionId << "\n"
        << "action_type: " << entry.actionType << "\n"
        << "finding_id: " << entry.findingId << "\n"
        << "review_status: " << entry.reviewStatus << "\n"
        << "distinct_reviewer_count: " << entry.distinctReviewerCount << "\n"
        << "approval_reviewer_count: " << entry.approvalReviewerCount << "\n"
        << "approval_threshold_met: " << (entry.approvalThresholdMet ? "true" : "false") << "\n"
        << "requires_manual_confirmation: " << (entry.requiresManualConfirmation ? "true" : "false") << "\n"
        << "title: " << entry.title << "\n"
        << "summary: " << entry.summary << "\n";
    if (!entry.reviewedBy.empty()) {
        out << "reviewed_by: " << entry.reviewedBy << "\n";
    }
    if (!entry.reviewedAtUtc.empty()) {
        out << "reviewed_at_utc: " << entry.reviewedAtUtc << "\n";
    }
    if (!entry.reviewComment.empty()) {
        out << "review_comment: " << entry.reviewComment << "\n";
    }
    if (entry.suggestedTools.is_array() && !entry.suggestedTools.empty()) {
        out << "suggested_tools:\n" << entry.suggestedTools.dump(2) << "\n";
    }
    return out.str();
}

std::string DescribePhase7JournalEntry(const tapescope::Phase7ExecutionJournalEntry& entry) {
    std::ostringstream out;
    out << "journal_entry_id: " << entry.journalEntryId << "\n"
        << "ledger_entry_id: " << entry.ledgerEntryId << "\n"
        << "action_id: " << entry.actionId << "\n"
        << "action_type: " << entry.actionType << "\n"
        << "finding_id: " << entry.findingId << "\n"
        << "execution_status: " << entry.executionStatus << "\n"
        << "attempt_count: " << entry.attemptCount << "\n"
        << "terminal: " << (entry.terminal ? "true" : "false") << "\n"
        << "requires_manual_confirmation: " << (entry.requiresManualConfirmation ? "true" : "false") << "\n"
        << "title: " << entry.title << "\n"
        << "summary: " << entry.summary << "\n";
    if (!entry.idempotencyKey.empty()) {
        out << "idempotency_key: " << entry.idempotencyKey << "\n";
    }
    if (!entry.queuedAtUtc.empty()) {
        out << "queued_at_utc: " << entry.queuedAtUtc << "\n";
    }
    if (!entry.startedAtUtc.empty()) {
        out << "started_at_utc: " << entry.startedAtUtc << "\n";
    }
    if (!entry.completedAtUtc.empty()) {
        out << "completed_at_utc: " << entry.completedAtUtc << "\n";
    }
    if (!entry.lastUpdatedAtUtc.empty()) {
        out << "last_updated_at_utc: " << entry.lastUpdatedAtUtc << "\n";
    }
    if (!entry.lastUpdatedBy.empty()) {
        out << "last_updated_by: " << entry.lastUpdatedBy << "\n";
    }
    if (!entry.executionComment.empty()) {
        out << "execution_comment: " << entry.executionComment << "\n";
    }
    if (!entry.failureCode.empty()) {
        out << "failure_code: " << entry.failureCode << "\n";
    }
    if (!entry.failureMessage.empty()) {
        out << "failure_message: " << entry.failureMessage << "\n";
    }
    if (entry.suggestedTools.is_array() && !entry.suggestedTools.empty()) {
        out << "suggested_tools:\n" << entry.suggestedTools.dump(2) << "\n";
    }
    return out.str();
}

std::string DescribePhase7ApplyEntry(const tapescope::Phase7ExecutionApplyEntry& entry) {
    std::ostringstream out;
    out << "apply_entry_id: " << entry.applyEntryId << "\n"
        << "journal_entry_id: " << entry.journalEntryId << "\n"
        << "ledger_entry_id: " << entry.ledgerEntryId << "\n"
        << "action_id: " << entry.actionId << "\n"
        << "action_type: " << entry.actionType << "\n"
        << "finding_id: " << entry.findingId << "\n"
        << "execution_status: " << entry.executionStatus << "\n"
        << "attempt_count: " << entry.attemptCount << "\n"
        << "terminal: " << (entry.terminal ? "true" : "false") << "\n"
        << "requires_manual_confirmation: " << (entry.requiresManualConfirmation ? "true" : "false") << "\n"
        << "title: " << entry.title << "\n"
        << "summary: " << entry.summary << "\n";
    if (!entry.idempotencyKey.empty()) {
        out << "idempotency_key: " << entry.idempotencyKey << "\n";
    }
    if (!entry.submittedAtUtc.empty()) {
        out << "submitted_at_utc: " << entry.submittedAtUtc << "\n";
    }
    if (!entry.completedAtUtc.empty()) {
        out << "completed_at_utc: " << entry.completedAtUtc << "\n";
    }
    if (!entry.lastUpdatedAtUtc.empty()) {
        out << "last_updated_at_utc: " << entry.lastUpdatedAtUtc << "\n";
    }
    if (!entry.lastUpdatedBy.empty()) {
        out << "last_updated_by: " << entry.lastUpdatedBy << "\n";
    }
    if (!entry.executionComment.empty()) {
        out << "execution_comment: " << entry.executionComment << "\n";
    }
    if (!entry.failureCode.empty()) {
        out << "failure_code: " << entry.failureCode << "\n";
    }
    if (!entry.failureMessage.empty()) {
        out << "failure_message: " << entry.failureMessage << "\n";
    }
    if (entry.suggestedTools.is_array() && !entry.suggestedTools.empty()) {
        out << "suggested_tools:\n" << entry.suggestedTools.dump(2) << "\n";
    }
    return out.str();
}

std::string DescribeSelectedPhase7LedgerEntries(const std::vector<tapescope::Phase7ExecutionLedgerEntry>& entries,
                                                NSIndexSet* selectedIndexes) {
    if (entries.empty()) {
        return "No execution-ledger review entries are available for this artifact.";
    }
    if (selectedIndexes == nil || selectedIndexes.count == 0) {
        std::ostringstream out;
        out << "Review entries available: " << entries.size()
            << ". Select one or more review rows, choose a review status, and record an append-only audit decision.\n";
        return out.str();
    }
    std::ostringstream out;
    out << "Selected review entries: " << selectedIndexes.count << "\n";
    NSUInteger rendered = 0;
    for (NSUInteger idx = selectedIndexes.firstIndex; idx != NSNotFound; idx = [selectedIndexes indexGreaterThanIndex:idx]) {
        if (idx >= entries.size()) {
            continue;
        }
        if (rendered > 0) {
            out << "\n";
        }
        out << DescribePhase7LedgerEntry(entries.at(idx));
        ++rendered;
        if (rendered >= 3 && selectedIndexes.count > 3) {
            out << "\n… " << (selectedIndexes.count - rendered) << " more selected ledger entr";
            out << (selectedIndexes.count - rendered == 1 ? "y" : "ies");
            break;
        }
    }
    return out.str();
}

std::string DescribeSelectedPhase7JournalEntries(const std::vector<tapescope::Phase7ExecutionJournalEntry>& entries,
                                                 NSIndexSet* selectedIndexes) {
    if (entries.empty()) {
        return "No execution-journal entries are available for this artifact.";
    }
    if (selectedIndexes == nil || selectedIndexes.count == 0) {
        std::ostringstream out;
        out << "Execution entries available: " << entries.size()
            << ". Select one or more rows to record append-only execution lifecycle events.\n";
        return out.str();
    }
    std::ostringstream out;
    out << "Selected execution entries: " << selectedIndexes.count << "\n";
    NSUInteger rendered = 0;
    for (NSUInteger idx = selectedIndexes.firstIndex; idx != NSNotFound; idx = [selectedIndexes indexGreaterThanIndex:idx]) {
        if (idx >= entries.size()) {
            continue;
        }
        if (rendered > 0) {
            out << "\n";
        }
        out << DescribePhase7JournalEntry(entries.at(idx));
        ++rendered;
        if (rendered >= 3 && selectedIndexes.count > 3) {
            out << "\n… " << (selectedIndexes.count - rendered) << " more selected journal entr";
            out << (selectedIndexes.count - rendered == 1 ? "y" : "ies");
            break;
        }
    }
    return out.str();
}

std::string DescribeSelectedPhase7ApplyEntries(const std::vector<tapescope::Phase7ExecutionApplyEntry>& entries,
                                               NSIndexSet* selectedIndexes) {
    if (entries.empty()) {
        return "No controlled apply entries are available for this artifact.";
    }
    if (selectedIndexes == nil || selectedIndexes.count == 0) {
        std::ostringstream out;
        out << "Apply entries available: " << entries.size()
            << ". Select one or more rows to record append-only apply lifecycle events.\n";
        return out.str();
    }
    std::ostringstream out;
    out << "Selected apply entries: " << selectedIndexes.count << "\n";
    NSUInteger rendered = 0;
    for (NSUInteger idx = selectedIndexes.firstIndex; idx != NSNotFound; idx = [selectedIndexes indexGreaterThanIndex:idx]) {
        if (idx >= entries.size()) {
            continue;
        }
        if (rendered > 0) {
            out << "\n";
        }
        out << DescribePhase7ApplyEntry(entries.at(idx));
        ++rendered;
        if (rendered >= 3 && selectedIndexes.count > 3) {
            out << "\n… " << (selectedIndexes.count - rendered) << " more selected apply entr";
            out << (selectedIndexes.count - rendered == 1 ? "y" : "ies");
            break;
        }
    }
    return out.str();
}

std::string SelectedPhase7ReviewStatus(NSPopUpButton* popup) {
    if (popup == nil || popup.titleOfSelectedItem == nil) {
        return {};
    }
    return TrimAscii(ToStdString(popup.titleOfSelectedItem));
}

std::string SelectedPhase7ExecutionStatus(NSPopUpButton* popup) {
    if (popup == nil || popup.titleOfSelectedItem == nil) {
        return {};
    }
    return TrimAscii(ToStdString(popup.titleOfSelectedItem));
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

std::string SelectedPhase7LedgerStatusFilter(NSPopUpButton* popup) {
    if (popup == nil || popup.titleOfSelectedItem == nil) {
        return {};
    }
    const std::string value = ToStdString(popup.titleOfSelectedItem);
    return value == "All Ledgers" ? std::string() : value;
}

std::string SelectedPhase7LedgerSort(NSPopUpButton* popup) {
    if (popup == nil || popup.titleOfSelectedItem == nil) {
        return "generated_at_desc";
    }
    const std::string value = ToStdString(popup.titleOfSelectedItem);
    if (value == "Needs Attention") {
        return "attention_desc";
    }
    if (value == "Source Artifact") {
        return "source_artifact_asc";
    }
    return "generated_at_desc";
}

std::string SelectedPhase7JournalStatusFilter(NSPopUpButton* popup) {
    if (popup == nil || popup.titleOfSelectedItem == nil) {
        return {};
    }
    const std::string value = ToStdString(popup.titleOfSelectedItem);
    return value == "All Journals" ? std::string() : value;
}

std::string SelectedPhase7JournalSort(NSPopUpButton* popup) {
    if (popup == nil || popup.titleOfSelectedItem == nil) {
        return "generated_at_desc";
    }
    const std::string value = ToStdString(popup.titleOfSelectedItem);
    if (value == "Needs Attention") {
        return "attention_desc";
    }
    if (value == "Source Artifact") {
        return "source_artifact_asc";
    }
    return "generated_at_desc";
}

std::string SelectedPhase7RecoveryFilter(NSPopUpButton* popup) {
    if (popup == nil || popup.titleOfSelectedItem == nil) {
        return {};
    }
    const std::string value = ToStdString(popup.titleOfSelectedItem);
    if (value == "Needs Recovery") {
        return "recovery_required";
    }
    if (value == "Stale Recovery") {
        return "stale_recovery_required";
    }
    return {};
}

std::string SelectedPhase7ResumePolicyFilter(NSPopUpButton* popup) {
    if (popup == nil || popup.titleOfSelectedItem == nil) {
        return {};
    }
    const std::string value = ToStdString(popup.titleOfSelectedItem);
    if (value == "Continue Recovery") {
        return "continue_recovery";
    }
    if (value == "Await Cancel Ack") {
        return "await_cancel_ack";
    }
    if (value == "Completed") {
        return "completed";
    }
    if (value == "Terminal No Resume") {
        return "terminal_no_resume";
    }
    if (value == "Manual Review") {
        return "manual_review_required";
    }
    if (value == "Unknown") {
        return "unknown";
    }
    return {};
}

std::string SelectedPhase7RecoveryStateFilter(NSPopUpButton* popup) {
    if (popup == nil || popup.titleOfSelectedItem == nil) {
        return {};
    }
    const std::string value = ToStdString(popup.titleOfSelectedItem);
    if (value == "Recoverable") {
        return "recoverable";
    }
    if (value == "Await Cancel Ack") {
        return "awaiting_cancel_ack";
    }
    if (value == "Terminal Completed") {
        return "terminal_completed";
    }
    if (value == "Terminal No Resume") {
        return "terminal_no_resume";
    }
    if (value == "Manual Review") {
        return "manual_review_required";
    }
    if (value == "Unknown") {
        return "unknown";
    }
    return {};
}

std::string SelectedPhase7ResolutionFilter(NSPopUpButton* popup) {
    if (popup == nil || popup.titleOfSelectedItem == nil) {
        return {};
    }
    const std::string value = ToStdString(popup.titleOfSelectedItem);
    if (value == "Working") {
        return "resolved_working";
    }
    if (value == "Partially Filled") {
        return "resolved_partially_filled";
    }
    if (value == "Filled") {
        return "resolved_filled";
    }
    if (value == "Cancelled") {
        return "resolved_cancelled";
    }
    if (value == "Rejected") {
        return "resolved_rejected";
    }
    if (value == "Inactive") {
        return "resolved_inactive";
    }
    if (value == "Manual Review") {
        return "manual_review_required";
    }
    return {};
}

std::string DescribePhase7InventoryStatus(const tapescope::Phase7AnalysisInventoryPayload& analyses,
                                          const tapescope::Phase7PlaybookInventoryPayload& playbooks,
                                          const tapescope::Phase7ExecutionLedgerInventoryPayload& ledgers,
                                          const tapescope::Phase7ExecutionJournalInventoryPayload& journals,
                                          const tapescope::Phase7ExecutionApplyInventoryPayload& applies) {
    std::ostringstream out;
    out << "Phase 7 inventory loaded. "
        << "Analyses: " << analyses.artifacts.size() << "/" << analyses.matchedCount
        << " matched. Playbooks: " << playbooks.artifacts.size() << "/" << playbooks.matchedCount
        << " matched. Ledgers: " << ledgers.artifacts.size() << "/" << ledgers.matchedCount
        << " matched. Journals: " << journals.artifacts.size() << "/" << journals.matchedCount
        << " matched. Applies: " << applies.artifacts.size() << "/" << applies.matchedCount
        << " matched. Controlled apply stays journal-backed and append-only.";
    return out.str();
}

} // namespace

@implementation TapeScopeWindowController (Phase7Queries)

- (NSTabViewItem*)phase7ArtifactsTabItem {
    const auto paneWithStack = MakePaneWithStack();
    NSStackView* stack = paneWithStack.stack;
    NSView* pane = paneWithStack.view;

    [stack addArrangedSubview:MakeIntroLabel(@"Phase 7 artifact inventory: reopen durable local analysis and playbook artifacts created from Phase 6 bundles, prepare review-only execution ledgers from guarded playbooks, then move through append-only execution journals and controlled apply artifacts once review thresholds are satisfied. TapeScope can also start an isolated local runtime bridge for live journal dispatch and reconciliation.",
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
    [_phase7ProfilePopup addItemWithTitle:@"phase7.incident_triage.v1"];
    [_phase7ProfilePopup addItemWithTitle:@"phase7.fill_quality_review.v1"];
    [_phase7ProfilePopup addItemWithTitle:@"phase7.liquidity_behavior_review.v1"];
    [_phase7ProfilePopup addItemWithTitle:@"phase7.adverse_selection_review.v1"];
    [_phase7ProfilePopup addItemWithTitle:@"phase7.order_impact_review.v1"];
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

    _phase7StartJournalButton = [NSButton buttonWithTitle:@"Start Execution Journal"
                                                   target:self
                                                   action:@selector(startSelectedPhase7ExecutionJournal:)];
    _phase7StartJournalButton.enabled = NO;
    [controls addArrangedSubview:_phase7StartJournalButton];

    _phase7StartApplyButton = [NSButton buttonWithTitle:@"Start Apply"
                                                 target:self
                                                 action:@selector(startSelectedPhase7ExecutionApply:)];
    _phase7StartApplyButton.enabled = NO;
    [controls addArrangedSubview:_phase7StartApplyButton];

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

    _phase7OpenLedgerButton = [NSButton buttonWithTitle:@"Open Selected Ledger"
                                                 target:self
                                                 action:@selector(openSelectedPhase7ExecutionLedger:)];
    _phase7OpenLedgerButton.enabled = NO;
    [controls addArrangedSubview:_phase7OpenLedgerButton];

    _phase7OpenJournalButton = [NSButton buttonWithTitle:@"Open Selected Journal"
                                                  target:self
                                                  action:@selector(openSelectedPhase7ExecutionJournal:)];
    _phase7OpenJournalButton.enabled = NO;
    [controls addArrangedSubview:_phase7OpenJournalButton];

    _phase7OpenApplyButton = [NSButton buttonWithTitle:@"Open Selected Apply"
                                                target:self
                                                action:@selector(openSelectedPhase7ExecutionApply:)];
    _phase7OpenApplyButton.enabled = NO;
    [controls addArrangedSubview:_phase7OpenApplyButton];

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

    _phase7OpenLinkedLedgerButton = [NSButton buttonWithTitle:@"Open Linked Ledger"
                                                       target:self
                                                       action:@selector(openSelectedPhase7LinkedLedger:)];
    _phase7OpenLinkedLedgerButton.enabled = NO;
    [controls addArrangedSubview:_phase7OpenLinkedLedgerButton];

    _phase7OpenLinkedJournalButton = [NSButton buttonWithTitle:@"Open Linked Journal"
                                                        target:self
                                                        action:@selector(openSelectedPhase7LinkedJournal:)];
    _phase7OpenLinkedJournalButton.enabled = NO;
    [controls addArrangedSubview:_phase7OpenLinkedJournalButton];

    _phase7LoadRangeButton = [NSButton buttonWithTitle:@"Load Replay Range"
                                                target:self
                                                action:@selector(loadReplayRangeFromPhase7Selection:)];
    _phase7LoadRangeButton.enabled = NO;
    [controls addArrangedSubview:_phase7LoadRangeButton];
    [stack addArrangedSubview:controls];

    NSStackView* journalControls = MakeControlRow();
    [journalControls addArrangedSubview:MakeLabel(@"journal start",
                                                  [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                                  [NSColor secondaryLabelColor])];
    _phase7JournalActorField = MakeMonospacedField(160.0, @"tapescope", @"actor");
    [journalControls addArrangedSubview:_phase7JournalActorField];
    _phase7ExecutionCapabilityField = MakeMonospacedField(240.0, @"phase7.execution_operator.v1", @"execution capability");
    [journalControls addArrangedSubview:_phase7ExecutionCapabilityField];
    [stack addArrangedSubview:journalControls];

    NSStackView* reviewControls = MakeControlRow();
    [reviewControls addArrangedSubview:MakeLabel(@"ledger review",
                                                 [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                                 [NSColor secondaryLabelColor])];
    _phase7ReviewStatusPopup = [[NSPopUpButton alloc] initWithFrame:NSMakeRect(0, 0, 150, 24) pullsDown:NO];
    [_phase7ReviewStatusPopup addItemsWithTitles:@[@"approved", @"blocked", @"needs_info", @"not_applicable"]];
    [reviewControls addArrangedSubview:_phase7ReviewStatusPopup];
    _phase7ReviewActorField = MakeMonospacedField(160.0, @"tapescope", @"actor");
    [reviewControls addArrangedSubview:_phase7ReviewActorField];
    _phase7ReviewCommentField = MakeMonospacedField(320.0, nil, @"review comment");
    [reviewControls addArrangedSubview:_phase7ReviewCommentField];
    _phase7RecordReviewButton = [NSButton buttonWithTitle:@"Record Review"
                                                   target:self
                                                   action:@selector(recordSelectedPhase7LedgerReview:)];
    _phase7RecordReviewButton.enabled = NO;
    [reviewControls addArrangedSubview:_phase7RecordReviewButton];
    [stack addArrangedSubview:reviewControls];

    NSStackView* executionControls = MakeControlRow();
    [executionControls addArrangedSubview:MakeLabel(@"execution event",
                                                    [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                                    [NSColor secondaryLabelColor])];
    _phase7ExecutionStatusPopup = [[NSPopUpButton alloc] initWithFrame:NSMakeRect(0, 0, 170, 24) pullsDown:NO];
    [_phase7ExecutionStatusPopup addItemsWithTitles:@[@"queued",
                                                      @"submitted",
                                                      @"succeeded",
                                                      @"failed",
                                                      @"cancelled"]];
    [executionControls addArrangedSubview:_phase7ExecutionStatusPopup];
    _phase7ExecutionActorField = MakeMonospacedField(160.0, @"tapescope", @"actor");
    [executionControls addArrangedSubview:_phase7ExecutionActorField];
    _phase7ExecutionCommentField = MakeMonospacedField(260.0, nil, @"execution comment");
    [executionControls addArrangedSubview:_phase7ExecutionCommentField];
    _phase7ExecutionFailureCodeField = MakeMonospacedField(160.0, nil, @"failure code");
    [executionControls addArrangedSubview:_phase7ExecutionFailureCodeField];
    _phase7ExecutionFailureMessageField = MakeMonospacedField(240.0, nil, @"failure message");
    [executionControls addArrangedSubview:_phase7ExecutionFailureMessageField];
    _phase7DispatchJournalButton = [NSButton buttonWithTitle:@"Dispatch Selected"
                                                      target:self
                                                      action:@selector(dispatchSelectedPhase7JournalEntries:)];
    _phase7DispatchJournalButton.enabled = NO;
    [executionControls addArrangedSubview:_phase7DispatchJournalButton];
    _phase7RecordExecutionButton = [NSButton buttonWithTitle:@"Record Execution Event"
                                                      target:self
                                                      action:@selector(recordSelectedPhase7JournalEvent:)];
    _phase7RecordExecutionButton.enabled = NO;
    [executionControls addArrangedSubview:_phase7RecordExecutionButton];

    _phase7RecordApplyButton = [NSButton buttonWithTitle:@"Record Apply Event"
                                                  target:self
                                                  action:@selector(recordSelectedPhase7ApplyEvent:)];
    _phase7RecordApplyButton.enabled = NO;
    [executionControls addArrangedSubview:_phase7RecordApplyButton];
    [stack addArrangedSubview:executionControls];

    NSStackView* runtimeControls = MakeControlRow();
    [runtimeControls addArrangedSubview:MakeLabel(@"runtime bridge",
                                                  [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                                  [NSColor secondaryLabelColor])];
    _phase7RuntimeStartButton = [NSButton buttonWithTitle:@"Start Local Runtime"
                                                   target:self
                                                   action:@selector(startPhase7RuntimeHost:)];
    [runtimeControls addArrangedSubview:_phase7RuntimeStartButton];
    _phase7RuntimeStopButton = [NSButton buttonWithTitle:@"Stop Runtime"
                                                  target:self
                                                  action:@selector(stopPhase7RuntimeHost:)];
    _phase7RuntimeStopButton.enabled = NO;
    [runtimeControls addArrangedSubview:_phase7RuntimeStopButton];
    _phase7RuntimeDispatchButton = [NSButton buttonWithTitle:@"Runtime Dispatch"
                                                      target:self
                                                      action:@selector(dispatchSelectedPhase7JournalEntriesViaRuntime:)];
    _phase7RuntimeDispatchButton.enabled = NO;
    [runtimeControls addArrangedSubview:_phase7RuntimeDispatchButton];
    _phase7RuntimeReconcileButton = [NSButton buttonWithTitle:@"Runtime Reconcile"
                                                       target:self
                                                       action:@selector(reconcileSelectedPhase7JournalEntriesViaRuntime:)];
    _phase7RuntimeReconcileButton.enabled = NO;
    [runtimeControls addArrangedSubview:_phase7RuntimeReconcileButton];
    _phase7RuntimeSweepButton = [NSButton buttonWithTitle:@"Runtime Sweep"
                                                   target:self
                                                   action:@selector(sweepPhase7ExecutionArtifactsViaRuntime:)];
    _phase7RuntimeSweepButton.enabled = NO;
    [runtimeControls addArrangedSubview:_phase7RuntimeSweepButton];
    _phase7RuntimeSyncApplyButton = [NSButton buttonWithTitle:@"Sync Apply From Journal"
                                                       target:self
                                                       action:@selector(syncSelectedPhase7ApplyFromJournal:)];
    _phase7RuntimeSyncApplyButton.enabled = NO;
    [runtimeControls addArrangedSubview:_phase7RuntimeSyncApplyButton];
    [stack addArrangedSubview:runtimeControls];

    _phase7StateLabel = MakeLabel(@"No Phase 7 artifacts loaded yet.",
                                  [NSFont systemFontOfSize:12.0 weight:NSFontWeightMedium],
                                  [NSColor secondaryLabelColor]);
    [stack addArrangedSubview:_phase7StateLabel];

    _phase7RuntimeStatusLabel = MakeLabel(@"Local runtime bridge is stopped. Starting it uses a TapeScope-specific client id with controller and websocket disabled.",
                                          [NSFont systemFontOfSize:12.0 weight:NSFontWeightMedium],
                                          TapeInkMutedColor());
    [stack addArrangedSubview:_phase7RuntimeStatusLabel];

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

    NSStackView* ledgerFilters = MakeControlRow();
    [ledgerFilters addArrangedSubview:MakeLabel(@"ledger filters",
                                                [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                                [NSColor secondaryLabelColor])];
    _phase7LedgerStatusFilterPopup = [[NSPopUpButton alloc] initWithFrame:NSMakeRect(0, 0, 180, 24) pullsDown:NO];
    [_phase7LedgerStatusFilterPopup addItemsWithTitles:@[@"All Ledgers",
                                                         @"review_pending",
                                                         @"review_in_progress",
                                                         @"review_blocked",
                                                         @"needs_information",
                                                         @"review_waiting_approval",
                                                         @"ready_for_execution",
                                                         @"review_completed"]];
    [ledgerFilters addArrangedSubview:_phase7LedgerStatusFilterPopup];
    _phase7LedgerSortPopup = [[NSPopUpButton alloc] initWithFrame:NSMakeRect(0, 0, 180, 24) pullsDown:NO];
    [_phase7LedgerSortPopup addItemsWithTitles:@[@"Newest First", @"Needs Attention", @"Source Artifact"]];
    [ledgerFilters addArrangedSubview:_phase7LedgerSortPopup];
    [stack addArrangedSubview:ledgerFilters];

    NSStackView* journalFilters = MakeControlRow();
    [journalFilters addArrangedSubview:MakeLabel(@"journal filters",
                                                 [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                                 [NSColor secondaryLabelColor])];
    _phase7JournalLedgerFilterField = MakeMonospacedField(240.0, nil, @"execution_ledger_artifact_id");
    [journalFilters addArrangedSubview:_phase7JournalLedgerFilterField];
    _phase7JournalStatusFilterPopup = [[NSPopUpButton alloc] initWithFrame:NSMakeRect(0, 0, 180, 24) pullsDown:NO];
    [_phase7JournalStatusFilterPopup addItemsWithTitles:@[@"All Journals",
                                                          @"execution_queued",
                                                          @"execution_in_progress",
                                                          @"execution_succeeded",
                                                          @"execution_partially_succeeded",
                                                          @"execution_failed",
                                                          @"execution_cancelled"]];
    [journalFilters addArrangedSubview:_phase7JournalStatusFilterPopup];
    _phase7JournalRecoveryFilterPopup = [[NSPopUpButton alloc] initWithFrame:NSMakeRect(0, 0, 180, 24)
                                                                  pullsDown:NO];
    [_phase7JournalRecoveryFilterPopup addItemsWithTitles:@[@"All Recovery",
                                                            @"Needs Recovery",
                                                            @"Stale Recovery"]];
    [journalFilters addArrangedSubview:_phase7JournalRecoveryFilterPopup];
    _phase7JournalRecoveryStateFilterPopup = [[NSPopUpButton alloc] initWithFrame:NSMakeRect(0, 0, 190, 24)
                                                                        pullsDown:NO];
    [_phase7JournalRecoveryStateFilterPopup addItemsWithTitles:@[@"All Runtime State",
                                                                 @"Recoverable",
                                                                 @"Await Cancel Ack",
                                                                 @"Terminal Completed",
                                                                 @"Terminal No Resume",
                                                                 @"Manual Review",
                                                                 @"Unknown"]];
    [journalFilters addArrangedSubview:_phase7JournalRecoveryStateFilterPopup];
    _phase7JournalResumePolicyFilterPopup = [[NSPopUpButton alloc] initWithFrame:NSMakeRect(0, 0, 170, 24)
                                                                      pullsDown:NO];
    [_phase7JournalResumePolicyFilterPopup addItemsWithTitles:@[@"All Resume",
                                                                @"Continue Recovery",
                                                                @"Await Cancel Ack",
                                                                @"Completed",
                                                                @"Terminal No Resume",
                                                                @"Manual Review",
                                                                @"Unknown"]];
    [journalFilters addArrangedSubview:_phase7JournalResumePolicyFilterPopup];
    _phase7JournalResolutionFilterPopup = [[NSPopUpButton alloc] initWithFrame:NSMakeRect(0, 0, 170, 24)
                                                                    pullsDown:NO];
    [_phase7JournalResolutionFilterPopup addItemsWithTitles:@[@"All Resolution",
                                                              @"Working",
                                                              @"Partially Filled",
                                                              @"Filled",
                                                              @"Cancelled",
                                                              @"Rejected",
                                                              @"Inactive",
                                                              @"Manual Review"]];
    [journalFilters addArrangedSubview:_phase7JournalResolutionFilterPopup];
    _phase7JournalSortPopup = [[NSPopUpButton alloc] initWithFrame:NSMakeRect(0, 0, 180, 24) pullsDown:NO];
    [_phase7JournalSortPopup addItemsWithTitles:@[@"Newest First", @"Needs Attention", @"Source Artifact"]];
    [journalFilters addArrangedSubview:_phase7JournalSortPopup];
    [stack addArrangedSubview:journalFilters];

    NSStackView* applyFilters = MakeControlRow();
    [applyFilters addArrangedSubview:MakeLabel(@"apply filters",
                                               [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                                               [NSColor secondaryLabelColor])];
    _phase7ApplyRecoveryFilterPopup = [[NSPopUpButton alloc] initWithFrame:NSMakeRect(0, 0, 180, 24)
                                                                pullsDown:NO];
    [_phase7ApplyRecoveryFilterPopup addItemsWithTitles:@[@"All Recovery",
                                                          @"Needs Recovery",
                                                          @"Stale Recovery"]];
    [applyFilters addArrangedSubview:_phase7ApplyRecoveryFilterPopup];
    _phase7ApplyRecoveryStateFilterPopup = [[NSPopUpButton alloc] initWithFrame:NSMakeRect(0, 0, 190, 24)
                                                                      pullsDown:NO];
    [_phase7ApplyRecoveryStateFilterPopup addItemsWithTitles:@[@"All Runtime State",
                                                               @"Recoverable",
                                                               @"Await Cancel Ack",
                                                               @"Terminal Completed",
                                                               @"Terminal No Resume",
                                                               @"Manual Review",
                                                               @"Unknown"]];
    [applyFilters addArrangedSubview:_phase7ApplyRecoveryStateFilterPopup];
    _phase7ApplyResumePolicyFilterPopup = [[NSPopUpButton alloc] initWithFrame:NSMakeRect(0, 0, 170, 24)
                                                                    pullsDown:NO];
    [_phase7ApplyResumePolicyFilterPopup addItemsWithTitles:@[@"All Resume",
                                                              @"Continue Recovery",
                                                              @"Await Cancel Ack",
                                                              @"Completed",
                                                              @"Terminal No Resume",
                                                              @"Manual Review",
                                                              @"Unknown"]];
    [applyFilters addArrangedSubview:_phase7ApplyResumePolicyFilterPopup];
    _phase7ApplyResolutionFilterPopup = [[NSPopUpButton alloc] initWithFrame:NSMakeRect(0, 0, 170, 24)
                                                                  pullsDown:NO];
    [_phase7ApplyResolutionFilterPopup addItemsWithTitles:@[@"All Resolution",
                                                            @"Working",
                                                            @"Partially Filled",
                                                            @"Filled",
                                                            @"Cancelled",
                                                            @"Rejected",
                                                            @"Inactive",
                                                            @"Manual Review"]];
    [applyFilters addArrangedSubview:_phase7ApplyResolutionFilterPopup];
    [stack addArrangedSubview:applyFilters];

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

    [stack addArrangedSubview:MakeSectionLabel(@"Execution Ledgers")];

    _phase7LedgerTableView = MakeStandardTableView(self, self);
    AddTableColumn(_phase7LedgerTableView, @"artifact_id", @"artifact_id", 220.0);
    AddTableColumn(_phase7LedgerTableView, @"playbook_artifact_id", @"playbook_artifact_id", 220.0);
    AddTableColumn(_phase7LedgerTableView, @"ledger_status", @"ledger_status", 150.0);
    AddTableColumn(_phase7LedgerTableView, @"pending_review_count", @"pending", 90.0);
    AddTableColumn(_phase7LedgerTableView, @"waiting_approval_count", @"waiting", 90.0);
    AddTableColumn(_phase7LedgerTableView, @"ready_entry_count", @"ready", 90.0);
    AddTableColumn(_phase7LedgerTableView, @"blocked_count", @"blocked", 90.0);
    AddTableColumn(_phase7LedgerTableView, @"distinct_reviewer_count", @"reviewers", 90.0);
    AddTableColumn(_phase7LedgerTableView, @"reviewed_count", @"reviewed", 90.0);
    AddTableColumn(_phase7LedgerTableView, @"latest_audit_event", @"latest_audit", 320.0);
    ConfigureTablePrimaryAction(_phase7LedgerTableView, self, @selector(openSelectedPhase7ExecutionLedger:));
    [stack addArrangedSubview:MakeTableScrollView(_phase7LedgerTableView, 150.0)];

    [stack addArrangedSubview:MakeSectionLabel(@"Execution Journals")];

    _phase7JournalTableView = MakeStandardTableView(self, self);
    AddTableColumn(_phase7JournalTableView, @"artifact_id", @"artifact_id", 220.0);
    AddTableColumn(_phase7JournalTableView, @"ledger_artifact_id", @"ledger_artifact_id", 220.0);
    AddTableColumn(_phase7JournalTableView, @"journal_status", @"journal_status", 170.0);
    AddTableColumn(_phase7JournalTableView, @"restart_recovery_state", @"runtime_state", 170.0);
    AddTableColumn(_phase7JournalTableView, @"restart_resume_policy", @"resume_policy", 160.0);
    AddTableColumn(_phase7JournalTableView, @"latest_execution_resolution", @"resolution", 170.0);
    AddTableColumn(_phase7JournalTableView, @"queued_count", @"queued", 90.0);
    AddTableColumn(_phase7JournalTableView, @"submitted_count", @"submitted", 90.0);
    AddTableColumn(_phase7JournalTableView, @"terminal_count", @"terminal", 90.0);
    AddTableColumn(_phase7JournalTableView, @"latest_audit_event", @"latest_audit", 320.0);
    ConfigureTablePrimaryAction(_phase7JournalTableView, self, @selector(openSelectedPhase7ExecutionJournal:));
    [stack addArrangedSubview:MakeTableScrollView(_phase7JournalTableView, 150.0)];

    [stack addArrangedSubview:MakeSectionLabel(@"Execution Applies")];

    _phase7ApplyTableView = MakeStandardTableView(self, self);
    AddTableColumn(_phase7ApplyTableView, @"artifact_id", @"artifact_id", 220.0);
    AddTableColumn(_phase7ApplyTableView, @"journal_artifact_id", @"journal_artifact_id", 220.0);
    AddTableColumn(_phase7ApplyTableView, @"apply_status", @"apply_status", 170.0);
    AddTableColumn(_phase7ApplyTableView, @"restart_recovery_state", @"runtime_state", 170.0);
    AddTableColumn(_phase7ApplyTableView, @"restart_resume_policy", @"resume_policy", 160.0);
    AddTableColumn(_phase7ApplyTableView, @"latest_execution_resolution", @"resolution", 170.0);
    AddTableColumn(_phase7ApplyTableView, @"submitted_count", @"submitted", 90.0);
    AddTableColumn(_phase7ApplyTableView, @"terminal_count", @"terminal", 90.0);
    AddTableColumn(_phase7ApplyTableView, @"latest_audit_event", @"latest_audit", 320.0);
    ConfigureTablePrimaryAction(_phase7ApplyTableView, self, @selector(openSelectedPhase7ExecutionApply:));
    [stack addArrangedSubview:MakeTableScrollView(_phase7ApplyTableView, 150.0)];

    [stack addArrangedSubview:MakeSectionLabel(@"Selected Findings")];

    _phase7FindingTableView = MakeStandardTableView(self, self);
    _phase7FindingTableView.allowsMultipleSelection = YES;
    AddTableColumn(_phase7FindingTableView, @"finding_id", @"finding_id", 220.0);
    AddTableColumn(_phase7FindingTableView, @"severity", @"severity", 100.0);
    AddTableColumn(_phase7FindingTableView, @"category", @"category", 160.0);
    AddTableColumn(_phase7FindingTableView, @"summary", @"summary", 360.0);
    [stack addArrangedSubview:MakeTableScrollView(_phase7FindingTableView, 140.0)];

    [stack addArrangedSubview:MakeSectionLabel(@"Planned Actions / Review Entries")];

    _phase7ActionTableView = MakeStandardTableView(self, self);
    _phase7ActionTableView.allowsMultipleSelection = YES;
    AddTableColumn(_phase7ActionTableView, @"action_id", @"action_id", 220.0);
    AddTableColumn(_phase7ActionTableView, @"action_type", @"action_type", 220.0);
    AddTableColumn(_phase7ActionTableView, @"finding_id", @"finding_id", 220.0);
    AddTableColumn(_phase7ActionTableView, @"review_status", @"status", 140.0);
    AddTableColumn(_phase7ActionTableView, @"title", @"title", 260.0);
    [stack addArrangedSubview:MakeTableScrollView(_phase7ActionTableView, 140.0)];

    [stack addArrangedSubview:MakeSectionLabel(@"Phase 7 Detail")];

    _phase7TextView = MakeReadOnlyTextView();
    _phase7TextView.string = ToNSString(Phase7DetailPlaceholder());
    [stack addArrangedSubview:MakeScrollView(_phase7TextView, 210.0)];

    NSTabViewItem* item = [[NSTabViewItem alloc] initWithIdentifier:@"Phase7Pane"];
    item.label = @"Phase 7";
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

    tapescope::Phase7ExecutionLedgerInventorySelection ledgerSelection;
    ledgerSelection.analysisArtifactId = playbookSelection.analysisArtifactId;
    ledgerSelection.sourceArtifactId = playbookSelection.sourceArtifactId;
    ledgerSelection.ledgerStatus = SelectedPhase7LedgerStatusFilter(_phase7LedgerStatusFilterPopup);
    ledgerSelection.sortBy = SelectedPhase7LedgerSort(_phase7LedgerSortPopup);
    ledgerSelection.limit = 20;

    tapescope::Phase7ExecutionJournalInventorySelection journalSelection;
    journalSelection.ledgerArtifactId = TrimAscii(ToStdString(_phase7JournalLedgerFilterField.stringValue));
    journalSelection.playbookArtifactId.clear();
    journalSelection.analysisArtifactId = playbookSelection.analysisArtifactId;
    journalSelection.sourceArtifactId = playbookSelection.sourceArtifactId;
    journalSelection.journalStatus = SelectedPhase7JournalStatusFilter(_phase7JournalStatusFilterPopup);
    journalSelection.recoveryState = SelectedPhase7RecoveryFilter(_phase7JournalRecoveryFilterPopup);
    journalSelection.restartRecoveryState = SelectedPhase7RecoveryStateFilter(_phase7JournalRecoveryStateFilterPopup);
    journalSelection.restartResumePolicy = SelectedPhase7ResumePolicyFilter(_phase7JournalResumePolicyFilterPopup);
    journalSelection.latestExecutionResolution = SelectedPhase7ResolutionFilter(_phase7JournalResolutionFilterPopup);
    journalSelection.sortBy = SelectedPhase7JournalSort(_phase7JournalSortPopup);
    journalSelection.limit = 20;

    tapescope::Phase7ExecutionApplyInventorySelection applySelection;
    applySelection.journalArtifactId.clear();
    applySelection.ledgerArtifactId = journalSelection.ledgerArtifactId;
    applySelection.playbookArtifactId.clear();
    applySelection.analysisArtifactId = playbookSelection.analysisArtifactId;
    applySelection.sourceArtifactId = playbookSelection.sourceArtifactId;
    applySelection.recoveryState = SelectedPhase7RecoveryFilter(_phase7ApplyRecoveryFilterPopup);
    applySelection.restartRecoveryState = SelectedPhase7RecoveryStateFilter(_phase7ApplyRecoveryStateFilterPopup);
    applySelection.restartResumePolicy = SelectedPhase7ResumePolicyFilter(_phase7ApplyResumePolicyFilterPopup);
    applySelection.latestExecutionResolution = SelectedPhase7ResolutionFilter(_phase7ApplyResolutionFilterPopup);
    applySelection.sortBy = "generated_at_desc";
    applySelection.limit = 20;

    _phase7InFlight = YES;
    const std::uint64_t token = [self issueRequestToken:&_phase7RequestToken];
    _phase7RefreshButton.enabled = NO;
    _phase7OpenAnalysisButton.enabled = NO;
    _phase7OpenPlaybookButton.enabled = NO;
    _phase7OpenLedgerButton.enabled = NO;
    _phase7OpenJournalButton.enabled = NO;
    _phase7BuildLedgerButton.enabled = NO;
    _phase7StartJournalButton.enabled = NO;
    _phase7StartApplyButton.enabled = NO;
    _phase7OpenSourceArtifactButton.enabled = NO;
    _phase7OpenLinkedAnalysisButton.enabled = NO;
    _phase7OpenLinkedLedgerButton.enabled = NO;
    _phase7OpenLinkedJournalButton.enabled = NO;
    _phase7OpenApplyButton.enabled = NO;
    _phase7LoadRangeButton.enabled = NO;
    _phase7RecordReviewButton.enabled = NO;
    _phase7DispatchJournalButton.enabled = NO;
    _phase7RecordExecutionButton.enabled = NO;
    _phase7RecordApplyButton.enabled = NO;
    [self updatePhase7RuntimeControls];
    _phase7StateLabel.stringValue = @"Refreshing Phase 7 artifact inventory…";
    _phase7StateLabel.textColor = [NSColor systemOrangeColor];
    _phase7TextView.string = @"Refreshing durable Phase 7 analyses, playbooks, ledgers, journals, and controlled apply artifacts…";

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_artifactQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto profiles = strongSelf->_client->listAnalysisProfilesPayload();
        const auto analyses = strongSelf->_client->listAnalysisArtifactsPayload(analysisSelection);
        const auto playbooks = strongSelf->_client->listPlaybookArtifactsPayload(playbookSelection);
        const auto ledgers = strongSelf->_client->listExecutionLedgerArtifactsPayload(ledgerSelection);
        const auto journals = strongSelf->_client->listExecutionJournalArtifactsPayload(journalSelection);
        const auto applies = strongSelf->_client->listExecutionApplyArtifactsPayload(applySelection);
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
            innerSelf->_latestPhase7ExecutionLedgers.clear();
            innerSelf->_latestPhase7ExecutionJournals.clear();
            innerSelf->_latestPhase7ExecutionApplies.clear();
            if (profiles.ok()) {
                innerSelf->_latestPhase7Profiles = profiles.value;
            }
            if (analyses.ok()) {
                innerSelf->_latestPhase7AnalysisArtifacts = analyses.value.artifacts;
            }
            if (playbooks.ok()) {
                innerSelf->_latestPhase7PlaybookArtifacts = playbooks.value.artifacts;
            }
            if (ledgers.ok()) {
                innerSelf->_latestPhase7ExecutionLedgers = ledgers.value.artifacts;
            }
            if (journals.ok()) {
                innerSelf->_latestPhase7ExecutionJournals = journals.value.artifacts;
            }
            if (applies.ok()) {
                innerSelf->_latestPhase7ExecutionApplies = applies.value.artifacts;
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
                [innerSelf->_phase7ProfilePopup addItemWithTitle:@"phase7.incident_triage.v1"];
                [innerSelf->_phase7ProfilePopup addItemWithTitle:@"phase7.fill_quality_review.v1"];
                [innerSelf->_phase7ProfilePopup addItemWithTitle:@"phase7.liquidity_behavior_review.v1"];
                [innerSelf->_phase7ProfilePopup addItemWithTitle:@"phase7.adverse_selection_review.v1"];
                [innerSelf->_phase7ProfilePopup addItemWithTitle:@"phase7.order_impact_review.v1"];
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
            [innerSelf->_phase7LedgerTableView reloadData];
            [innerSelf->_phase7JournalTableView reloadData];
            [innerSelf->_phase7ApplyTableView reloadData];
            if (!innerSelf->_latestPhase7AnalysisArtifacts.empty()) {
                innerSelf->_phase7SelectionIsPlaybook = NO;
                innerSelf->_phase7SelectionIsLedger = NO;
                innerSelf->_phase7SelectionIsJournal = NO;
                innerSelf->_phase7SelectionIsApply = NO;
                [innerSelf->_phase7AnalysisTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0]
                                                byExtendingSelection:NO];
            } else if (!innerSelf->_latestPhase7PlaybookArtifacts.empty()) {
                innerSelf->_phase7SelectionIsPlaybook = YES;
                innerSelf->_phase7SelectionIsLedger = NO;
                innerSelf->_phase7SelectionIsJournal = NO;
                innerSelf->_phase7SelectionIsApply = NO;
                [innerSelf->_phase7PlaybookTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0]
                                                 byExtendingSelection:NO];
            } else if (!innerSelf->_latestPhase7ExecutionLedgers.empty()) {
                innerSelf->_phase7SelectionIsPlaybook = NO;
                innerSelf->_phase7SelectionIsLedger = YES;
                innerSelf->_phase7SelectionIsJournal = NO;
                innerSelf->_phase7SelectionIsApply = NO;
                [innerSelf->_phase7LedgerTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0]
                                               byExtendingSelection:NO];
            } else if (!innerSelf->_latestPhase7ExecutionJournals.empty()) {
                innerSelf->_phase7SelectionIsPlaybook = NO;
                innerSelf->_phase7SelectionIsLedger = NO;
                innerSelf->_phase7SelectionIsJournal = YES;
                innerSelf->_phase7SelectionIsApply = NO;
                [innerSelf->_phase7JournalTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0]
                                                byExtendingSelection:NO];
            } else if (!innerSelf->_latestPhase7ExecutionApplies.empty()) {
                innerSelf->_phase7SelectionIsPlaybook = NO;
                innerSelf->_phase7SelectionIsLedger = NO;
                innerSelf->_phase7SelectionIsJournal = NO;
                innerSelf->_phase7SelectionIsApply = YES;
                [innerSelf->_phase7ApplyTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0]
                                              byExtendingSelection:NO];
            }

            if (profiles.ok() || analyses.ok() || playbooks.ok() || ledgers.ok() || journals.ok() || applies.ok()) {
                if (!innerSelf->_latestPhase7AnalysisArtifacts.empty() ||
                    !innerSelf->_latestPhase7PlaybookArtifacts.empty() ||
                    !innerSelf->_latestPhase7ExecutionLedgers.empty() ||
                    !innerSelf->_latestPhase7ExecutionJournals.empty() ||
                    !innerSelf->_latestPhase7ExecutionApplies.empty()) {
                    innerSelf->_phase7StateLabel.stringValue = ToNSString(DescribePhase7InventoryStatus(
                        analyses.ok() ? analyses.value : tapescope::Phase7AnalysisInventoryPayload{},
                        playbooks.ok() ? playbooks.value : tapescope::Phase7PlaybookInventoryPayload{},
                        ledgers.ok() ? ledgers.value : tapescope::Phase7ExecutionLedgerInventoryPayload{},
                        journals.ok() ? journals.value : tapescope::Phase7ExecutionJournalInventoryPayload{},
                        applies.ok() ? applies.value : tapescope::Phase7ExecutionApplyInventoryPayload{}));
                    innerSelf->_phase7StateLabel.textColor = [NSColor systemGreenColor];
                } else {
                    innerSelf->_phase7StateLabel.stringValue =
                        (analysisSelection.sourceArtifactId.empty() &&
                         analysisSelection.analysisProfile.empty() &&
                         playbookSelection.analysisArtifactId.empty() &&
                         playbookSelection.sourceArtifactId.empty() &&
                         playbookSelection.mode.empty() &&
                         ledgerSelection.ledgerStatus.empty() &&
                         journalSelection.ledgerArtifactId.empty() &&
                         journalSelection.journalStatus.empty() &&
                         journalSelection.recoveryState.empty() &&
                         journalSelection.restartRecoveryState.empty() &&
                         journalSelection.restartResumePolicy.empty() &&
                         journalSelection.latestExecutionResolution.empty() &&
                         applySelection.recoveryState.empty() &&
                         applySelection.restartRecoveryState.empty() &&
                         applySelection.restartResumePolicy.empty() &&
                         applySelection.latestExecutionResolution.empty())
                            ? @"No Phase 7 analysis, playbook, ledger, execution-journal, or controlled-apply artifacts are available yet."
                            : @"Phase 7 filters matched no analysis, playbook, ledger, execution-journal, or controlled-apply artifacts.";
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
    [_phase7LedgerStatusFilterPopup selectItemWithTitle:@"All Ledgers"];
    [_phase7LedgerSortPopup selectItemWithTitle:@"Newest First"];
    _phase7JournalLedgerFilterField.stringValue = @"";
    [_phase7JournalStatusFilterPopup selectItemWithTitle:@"All Journals"];
    [_phase7JournalRecoveryFilterPopup selectItemWithTitle:@"All Recovery"];
    [_phase7JournalRecoveryStateFilterPopup selectItemWithTitle:@"All Runtime State"];
    [_phase7JournalResumePolicyFilterPopup selectItemWithTitle:@"All Resume"];
    [_phase7JournalResolutionFilterPopup selectItemWithTitle:@"All Resolution"];
    [_phase7JournalSortPopup selectItemWithTitle:@"Newest First"];
    [_phase7ApplyRecoveryFilterPopup selectItemWithTitle:@"All Recovery"];
    [_phase7ApplyRecoveryStateFilterPopup selectItemWithTitle:@"All Runtime State"];
    [_phase7ApplyResumePolicyFilterPopup selectItemWithTitle:@"All Resume"];
    [_phase7ApplyResolutionFilterPopup selectItemWithTitle:@"All Resolution"];
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
    const std::vector<std::string> selectedLedgerEntryIds =
        SelectedPhase7LedgerEntryIds(_phase7ActionTableView, _phase7VisibleLedgerEntries);
    const std::vector<std::string> selectedJournalEntryIds =
        SelectedPhase7JournalEntryIds(_phase7ActionTableView, _phase7VisibleJournalEntries);
    const std::vector<std::string> selectedApplyEntryIds =
        SelectedPhase7ApplyEntryIds(_phase7ActionTableView, _phase7VisibleApplyEntries);
    _phase7SelectedSourceArtifactId.clear();
    _phase7SelectedLinkedAnalysisArtifactId.clear();
    _phase7SelectedLinkedPlaybookArtifactId.clear();
    _phase7SelectedLinkedLedgerArtifactId.clear();
    _phase7SelectedLinkedJournalArtifactId.clear();
    _phase7ReplayRange = {};
    _phase7VisibleFindings.clear();
    _phase7VisibleActions.clear();
    _phase7VisibleLedgerEntries.clear();
    _phase7VisibleJournalEntries.clear();
    _phase7VisibleApplyEntries.clear();

    const NSInteger analysisSelected = _phase7AnalysisTableView.selectedRow;
    if (analysisSelected >= 0 &&
        static_cast<std::size_t>(analysisSelected) < _latestPhase7AnalysisArtifacts.size() &&
        _phase7SelectionIsPlaybook == NO &&
        _phase7SelectionIsLedger == NO &&
        _phase7SelectionIsJournal == NO) {
        const auto& artifact = _latestPhase7AnalysisArtifacts.at(static_cast<std::size_t>(analysisSelected));
        _phase7DetailBody = DescribePhase7AnalysisArtifact(artifact);
        _phase7VisibleFindings = artifact.findings;
        _phase7SelectedSourceArtifactId = artifact.sourceArtifact.artifactId;
        _phase7SelectedLinkedAnalysisArtifactId.clear();
        _phase7SelectedLinkedPlaybookArtifactId.clear();
        _phase7SelectedLinkedLedgerArtifactId.clear();
        _phase7SelectedLinkedJournalArtifactId.clear();
        if (const auto replayRange = ReplayRangeFromPhase7Context(artifact.replayContext); replayRange.has_value()) {
            _phase7ReplayRange = *replayRange;
            _phase7LoadRangeButton.enabled = YES;
        } else {
            _phase7LoadRangeButton.enabled = NO;
        }
        _phase7BuildPlaybookButton.enabled = YES;
        _phase7BuildLedgerButton.enabled = NO;
        _phase7StartJournalButton.enabled = NO;
        _phase7StartApplyButton.enabled = NO;
        UpdatePhase7BuildPlaybookButtonTitle(_phase7BuildPlaybookButton, selectedFindingIds.size());
        _phase7OpenAnalysisButton.enabled = YES;
        _phase7OpenPlaybookButton.title = @"Open Selected Playbook";
        _phase7OpenPlaybookButton.enabled = NO;
        _phase7OpenLedgerButton.enabled = NO;
        _phase7OpenJournalButton.enabled = NO;
        _phase7OpenApplyButton.enabled = NO;
        _phase7OpenSourceArtifactButton.enabled = !_phase7SelectedSourceArtifactId.empty();
        _phase7OpenLinkedAnalysisButton.enabled = NO;
        _phase7OpenLinkedLedgerButton.enabled = NO;
        _phase7OpenLinkedJournalButton.enabled = NO;
        _phase7RecordReviewButton.enabled = NO;
        _phase7DispatchJournalButton.enabled = NO;
        _phase7RecordExecutionButton.enabled = NO;
        _phase7RecordApplyButton.enabled = NO;
        [self updatePhase7RuntimeControls];
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
    if (playbookSelected >= 0 &&
        static_cast<std::size_t>(playbookSelected) < _latestPhase7PlaybookArtifacts.size() &&
        _phase7SelectionIsLedger == NO &&
        _phase7SelectionIsJournal == NO) {
        const auto& artifact = _latestPhase7PlaybookArtifacts.at(static_cast<std::size_t>(playbookSelected));
        _phase7DetailBody = DescribePhase7PlaybookArtifact(artifact);
        _phase7VisibleActions = artifact.plannedActions;
        _phase7SelectedLinkedAnalysisArtifactId = artifact.analysisArtifact.artifactId;
        _phase7SelectedLinkedPlaybookArtifactId = artifact.playbookArtifact.artifactId;
        _phase7SelectedLinkedLedgerArtifactId.clear();
        _phase7SelectedLinkedJournalArtifactId.clear();
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
        _phase7StartJournalButton.enabled = NO;
        _phase7StartApplyButton.enabled = NO;
        UpdatePhase7BuildPlaybookButtonTitle(_phase7BuildPlaybookButton, 0);
        _phase7OpenAnalysisButton.enabled = NO;
        _phase7OpenPlaybookButton.title = @"Open Selected Playbook";
        _phase7OpenPlaybookButton.enabled = YES;
        _phase7OpenLedgerButton.enabled = NO;
        _phase7OpenJournalButton.enabled = NO;
        _phase7OpenApplyButton.enabled = NO;
        _phase7OpenSourceArtifactButton.enabled = !_phase7SelectedSourceArtifactId.empty();
        _phase7OpenLinkedAnalysisButton.enabled = !_phase7SelectedLinkedAnalysisArtifactId.empty();
        _phase7OpenLinkedLedgerButton.enabled = NO;
        _phase7OpenLinkedJournalButton.enabled = NO;
        _phase7RecordReviewButton.enabled = NO;
        _phase7DispatchJournalButton.enabled = NO;
        _phase7RecordExecutionButton.enabled = NO;
        _phase7RecordApplyButton.enabled = NO;
        [self updatePhase7RuntimeControls];
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

    const NSInteger ledgerSelected = _phase7LedgerTableView.selectedRow;
    if (ledgerSelected >= 0 &&
        static_cast<std::size_t>(ledgerSelected) < _latestPhase7ExecutionLedgers.size()) {
        const auto& artifact = _latestPhase7ExecutionLedgers.at(static_cast<std::size_t>(ledgerSelected));
        _phase7DetailBody = DescribePhase7ExecutionLedgerArtifact(artifact);
        _phase7VisibleLedgerEntries = artifact.entries;
        _phase7SelectedSourceArtifactId = artifact.sourceArtifact.artifactId;
        _phase7SelectedLinkedAnalysisArtifactId = artifact.analysisArtifact.artifactId;
        _phase7SelectedLinkedPlaybookArtifactId = artifact.playbookArtifact.artifactId;
        _phase7SelectedLinkedLedgerArtifactId = artifact.ledgerArtifact.artifactId;
        _phase7SelectedLinkedJournalArtifactId.clear();
        if (const auto replayRange = ReplayRangeFromPhase7Context(artifact.replayContext); replayRange.has_value()) {
            _phase7ReplayRange = *replayRange;
            _phase7LoadRangeButton.enabled = YES;
        } else {
            _phase7LoadRangeButton.enabled = NO;
        }
        _phase7BuildPlaybookButton.enabled = NO;
        _phase7BuildLedgerButton.enabled = NO;
        _phase7StartJournalButton.enabled =
            (artifact.ledgerStatus == tape_phase7::kLedgerStatusReadyForExecution);
        _phase7StartApplyButton.enabled = NO;
        UpdatePhase7BuildPlaybookButtonTitle(_phase7BuildPlaybookButton, 0);
        _phase7OpenAnalysisButton.enabled = NO;
        _phase7OpenPlaybookButton.title = @"Open Linked Playbook";
        _phase7OpenPlaybookButton.enabled = !_phase7SelectedLinkedPlaybookArtifactId.empty();
        _phase7OpenLedgerButton.enabled = YES;
        _phase7OpenJournalButton.enabled = NO;
        _phase7OpenApplyButton.enabled = NO;
        _phase7OpenSourceArtifactButton.enabled = !_phase7SelectedSourceArtifactId.empty();
        _phase7OpenLinkedAnalysisButton.enabled = !_phase7SelectedLinkedAnalysisArtifactId.empty();
        _phase7OpenLinkedLedgerButton.enabled = NO;
        _phase7OpenLinkedJournalButton.enabled = NO;
        [_phase7FindingTableView reloadData];
        [_phase7ActionTableView reloadData];
        [_phase7FindingTableView deselectAll:nil];
        NSIndexSet* ledgerSelection = IndexSetForPhase7LedgerEntryIds(selectedLedgerEntryIds, _phase7VisibleLedgerEntries);
        if (ledgerSelection.count > 0) {
            [_phase7ActionTableView selectRowIndexes:ledgerSelection byExtendingSelection:NO];
        } else {
            [_phase7ActionTableView deselectAll:nil];
        }
        _phase7RecordReviewButton.enabled = (_phase7ActionTableView.selectedRowIndexes.count > 0);
        _phase7DispatchJournalButton.enabled = NO;
        _phase7RecordExecutionButton.enabled = NO;
        _phase7RecordApplyButton.enabled = NO;
        [self updatePhase7RuntimeControls];
        _phase7TextView.string = ToNSString(_phase7DetailBody + "\n\n" +
                                            DescribeSelectedPhase7LedgerEntries(_phase7VisibleLedgerEntries,
                                                                               _phase7ActionTableView.selectedRowIndexes));
        return;
    }

    const NSInteger journalSelected = _phase7JournalTableView.selectedRow;
    if (journalSelected >= 0 &&
        static_cast<std::size_t>(journalSelected) < _latestPhase7ExecutionJournals.size() &&
        _phase7SelectionIsJournal == YES) {
        const auto& artifact = _latestPhase7ExecutionJournals.at(static_cast<std::size_t>(journalSelected));
        _phase7DetailBody = DescribePhase7ExecutionJournalArtifact(artifact);
        _phase7VisibleJournalEntries = artifact.entries;
        _phase7SelectedSourceArtifactId = artifact.sourceArtifact.artifactId;
        _phase7SelectedLinkedAnalysisArtifactId = artifact.analysisArtifact.artifactId;
        _phase7SelectedLinkedPlaybookArtifactId = artifact.playbookArtifact.artifactId;
        _phase7SelectedLinkedLedgerArtifactId = artifact.ledgerArtifact.artifactId;
        _phase7SelectedLinkedJournalArtifactId = artifact.journalArtifact.artifactId;
        if (const auto replayRange = ReplayRangeFromPhase7Context(artifact.replayContext); replayRange.has_value()) {
            _phase7ReplayRange = *replayRange;
            _phase7LoadRangeButton.enabled = YES;
        } else {
            _phase7LoadRangeButton.enabled = NO;
        }
        _phase7BuildPlaybookButton.enabled = NO;
        _phase7BuildLedgerButton.enabled = NO;
        _phase7StartJournalButton.enabled = NO;
        const std::vector<std::string> selectedSubmittedJournalEntryIds =
            SelectedPhase7SubmittedJournalEntryIds(_phase7ActionTableView, _phase7VisibleJournalEntries);
        const bool anySubmittedJournalEntry =
            std::any_of(_phase7VisibleJournalEntries.begin(),
                        _phase7VisibleJournalEntries.end(),
                        [](const tapescope::Phase7ExecutionJournalEntry& entry) {
                            return entry.executionStatus == tape_phase7::kExecutionEntryStatusSubmitted;
                        });
        const bool hasJournalSelection = (_phase7ActionTableView.selectedRowIndexes.count > 0);
        _phase7StartApplyButton.enabled = hasJournalSelection
                                              ? (!selectedSubmittedJournalEntryIds.empty() &&
                                                 selectedSubmittedJournalEntryIds.size() ==
                                                     selectedJournalEntryIds.size())
                                              : anySubmittedJournalEntry;
        UpdatePhase7BuildPlaybookButtonTitle(_phase7BuildPlaybookButton, 0);
        _phase7OpenAnalysisButton.enabled = NO;
        _phase7OpenPlaybookButton.title = @"Open Linked Playbook";
        _phase7OpenPlaybookButton.enabled = !_phase7SelectedLinkedPlaybookArtifactId.empty();
        _phase7OpenLedgerButton.enabled = NO;
        _phase7OpenJournalButton.enabled = YES;
        _phase7OpenApplyButton.enabled = NO;
        _phase7OpenSourceArtifactButton.enabled = !_phase7SelectedSourceArtifactId.empty();
        _phase7OpenLinkedAnalysisButton.enabled = !_phase7SelectedLinkedAnalysisArtifactId.empty();
        _phase7OpenLinkedLedgerButton.enabled = !_phase7SelectedLinkedLedgerArtifactId.empty();
        _phase7OpenLinkedJournalButton.enabled = NO;
        _phase7RecordReviewButton.enabled = NO;
        [_phase7FindingTableView reloadData];
        [_phase7ActionTableView reloadData];
        [_phase7FindingTableView deselectAll:nil];
        NSIndexSet* journalSelection = IndexSetForPhase7JournalEntryIds(selectedJournalEntryIds, _phase7VisibleJournalEntries);
        if (journalSelection.count > 0) {
            [_phase7ActionTableView selectRowIndexes:journalSelection byExtendingSelection:NO];
        } else {
            [_phase7ActionTableView deselectAll:nil];
        }
        _phase7DispatchJournalButton.enabled =
            !SelectedPhase7QueuedJournalEntryIds(_phase7ActionTableView, _phase7VisibleJournalEntries).empty();
        _phase7RecordExecutionButton.enabled = (_phase7ActionTableView.selectedRowIndexes.count > 0);
        _phase7RecordApplyButton.enabled = NO;
        [self updatePhase7RuntimeControls];
        _phase7TextView.string = ToNSString(_phase7DetailBody + "\n\n" +
                                            DescribeSelectedPhase7JournalEntries(_phase7VisibleJournalEntries,
                                                                                _phase7ActionTableView.selectedRowIndexes));
        return;
    }

    const NSInteger applySelected = _phase7ApplyTableView.selectedRow;
    if (applySelected >= 0 &&
        static_cast<std::size_t>(applySelected) < _latestPhase7ExecutionApplies.size() &&
        _phase7SelectionIsApply == YES) {
        const auto& artifact = _latestPhase7ExecutionApplies.at(static_cast<std::size_t>(applySelected));
        _phase7DetailBody = DescribePhase7ExecutionApplyArtifact(artifact);
        _phase7VisibleApplyEntries = artifact.entries;
        _phase7SelectedSourceArtifactId = artifact.sourceArtifact.artifactId;
        _phase7SelectedLinkedAnalysisArtifactId = artifact.analysisArtifact.artifactId;
        _phase7SelectedLinkedPlaybookArtifactId = artifact.playbookArtifact.artifactId;
        _phase7SelectedLinkedLedgerArtifactId = artifact.ledgerArtifact.artifactId;
        _phase7SelectedLinkedJournalArtifactId = artifact.journalArtifact.artifactId;
        if (const auto replayRange = ReplayRangeFromPhase7Context(artifact.replayContext); replayRange.has_value()) {
            _phase7ReplayRange = *replayRange;
            _phase7LoadRangeButton.enabled = YES;
        } else {
            _phase7LoadRangeButton.enabled = NO;
        }
        _phase7BuildPlaybookButton.enabled = NO;
        _phase7BuildLedgerButton.enabled = NO;
        _phase7StartJournalButton.enabled = NO;
        _phase7StartApplyButton.enabled = NO;
        UpdatePhase7BuildPlaybookButtonTitle(_phase7BuildPlaybookButton, 0);
        _phase7OpenAnalysisButton.enabled = NO;
        _phase7OpenPlaybookButton.title = @"Open Linked Playbook";
        _phase7OpenPlaybookButton.enabled = !_phase7SelectedLinkedPlaybookArtifactId.empty();
        _phase7OpenLedgerButton.enabled = NO;
        _phase7OpenJournalButton.enabled = NO;
        _phase7OpenApplyButton.enabled = YES;
        _phase7OpenSourceArtifactButton.enabled = !_phase7SelectedSourceArtifactId.empty();
        _phase7OpenLinkedAnalysisButton.enabled = !_phase7SelectedLinkedAnalysisArtifactId.empty();
        _phase7OpenLinkedLedgerButton.enabled = !_phase7SelectedLinkedLedgerArtifactId.empty();
        _phase7OpenLinkedJournalButton.enabled = !_phase7SelectedLinkedJournalArtifactId.empty();
        _phase7RecordReviewButton.enabled = NO;
        _phase7DispatchJournalButton.enabled = NO;
        _phase7RecordExecutionButton.enabled = NO;
        [self updatePhase7RuntimeControls];
        [_phase7FindingTableView reloadData];
        [_phase7ActionTableView reloadData];
        [_phase7FindingTableView deselectAll:nil];
        NSIndexSet* applySelection = IndexSetForPhase7ApplyEntryIds(selectedApplyEntryIds, _phase7VisibleApplyEntries);
        if (applySelection.count > 0) {
            [_phase7ActionTableView selectRowIndexes:applySelection byExtendingSelection:NO];
        } else {
            [_phase7ActionTableView deselectAll:nil];
        }
        _phase7RecordApplyButton.enabled = (_phase7ActionTableView.selectedRowIndexes.count > 0);
        _phase7TextView.string = ToNSString(_phase7DetailBody + "\n\n" +
                                            DescribeSelectedPhase7ApplyEntries(_phase7VisibleApplyEntries,
                                                                              _phase7ActionTableView.selectedRowIndexes));
        return;
    }

    _phase7DetailBody = Phase7DetailPlaceholder();
    _phase7OpenAnalysisButton.enabled = NO;
    _phase7OpenPlaybookButton.title = @"Open Selected Playbook";
    _phase7OpenPlaybookButton.enabled = NO;
    _phase7OpenLedgerButton.enabled = NO;
    _phase7OpenJournalButton.enabled = NO;
    _phase7OpenApplyButton.enabled = NO;
    _phase7BuildPlaybookButton.enabled = NO;
    _phase7BuildLedgerButton.enabled = NO;
    _phase7StartJournalButton.enabled = NO;
    _phase7StartApplyButton.enabled = NO;
    UpdatePhase7BuildPlaybookButtonTitle(_phase7BuildPlaybookButton, 0);
    _phase7OpenSourceArtifactButton.enabled = NO;
    _phase7OpenLinkedAnalysisButton.enabled = NO;
    _phase7OpenLinkedLedgerButton.enabled = NO;
    _phase7OpenLinkedJournalButton.enabled = NO;
    _phase7LoadRangeButton.enabled = NO;
    _phase7RecordReviewButton.enabled = NO;
    _phase7DispatchJournalButton.enabled = NO;
    _phase7RecordExecutionButton.enabled = NO;
    _phase7RecordApplyButton.enabled = NO;
    [self updatePhase7RuntimeControls];
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
                innerSelf->_phase7SelectionIsLedger = NO;
                innerSelf->_phase7SelectionIsJournal = NO;
                [innerSelf->_phase7AnalysisTableView reloadData];
                [innerSelf->_phase7AnalysisTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:index] byExtendingSelection:NO];
            } else {
                innerSelf->_latestPhase7AnalysisArtifacts.insert(innerSelf->_latestPhase7AnalysisArtifacts.begin(),
                                                                 result.value.artifact);
                innerSelf->_phase7SelectionIsPlaybook = NO;
                innerSelf->_phase7SelectionIsLedger = NO;
                innerSelf->_phase7SelectionIsJournal = NO;
                [innerSelf->_phase7AnalysisTableView reloadData];
                [innerSelf->_phase7AnalysisTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0] byExtendingSelection:NO];
            }
            [innerSelf->_phase7PlaybookTableView deselectAll:nil];
            [innerSelf->_phase7LedgerTableView deselectAll:nil];
            [innerSelf->_phase7JournalTableView deselectAll:nil];
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
                innerSelf->_phase7SelectionIsLedger = NO;
                innerSelf->_phase7SelectionIsJournal = NO;
                [innerSelf->_phase7PlaybookTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:index] byExtendingSelection:NO];
            } else {
                innerSelf->_latestPhase7PlaybookArtifacts.insert(innerSelf->_latestPhase7PlaybookArtifacts.begin(),
                                                                 result.value.artifact);
                innerSelf->_phase7SelectionIsPlaybook = YES;
                innerSelf->_phase7SelectionIsLedger = NO;
                innerSelf->_phase7SelectionIsJournal = NO;
                [innerSelf->_phase7PlaybookTableView reloadData];
                [innerSelf->_phase7PlaybookTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0] byExtendingSelection:NO];
            }
            [innerSelf->_phase7AnalysisTableView deselectAll:nil];
            [innerSelf->_phase7LedgerTableView deselectAll:nil];
            [innerSelf->_phase7JournalTableView deselectAll:nil];
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

            auto it = std::find_if(innerSelf->_latestPhase7ExecutionLedgers.begin(),
                                   innerSelf->_latestPhase7ExecutionLedgers.end(),
                                   [&](const tapescope::Phase7ExecutionLedgerArtifact& item) {
                                       return item.ledgerArtifact.artifactId == result.value.artifact.ledgerArtifact.artifactId;
                                   });
            if (it != innerSelf->_latestPhase7ExecutionLedgers.end()) {
                *it = result.value.artifact;
                const std::size_t index =
                    static_cast<std::size_t>(std::distance(innerSelf->_latestPhase7ExecutionLedgers.begin(), it));
                [innerSelf->_phase7LedgerTableView reloadData];
                innerSelf->_phase7SelectionIsPlaybook = NO;
                innerSelf->_phase7SelectionIsLedger = YES;
                innerSelf->_phase7SelectionIsJournal = NO;
                [innerSelf->_phase7LedgerTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:index]
                                               byExtendingSelection:NO];
            } else {
                innerSelf->_latestPhase7ExecutionLedgers.insert(innerSelf->_latestPhase7ExecutionLedgers.begin(),
                                                                result.value.artifact);
                [innerSelf->_phase7LedgerTableView reloadData];
                innerSelf->_phase7SelectionIsPlaybook = NO;
                innerSelf->_phase7SelectionIsLedger = YES;
                innerSelf->_phase7SelectionIsJournal = NO;
                [innerSelf->_phase7LedgerTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0]
                                               byExtendingSelection:NO];
            }
            [innerSelf->_phase7AnalysisTableView deselectAll:nil];
            [innerSelf->_phase7PlaybookTableView deselectAll:nil];
            [innerSelf->_phase7JournalTableView deselectAll:nil];
            [innerSelf refreshPhase7DetailText];
            innerSelf->_phase7SelectedSourceArtifactId = result.value.artifact.sourceArtifact.artifactId;
            innerSelf->_phase7SelectedLinkedAnalysisArtifactId = result.value.artifact.analysisArtifact.artifactId;
            innerSelf->_phase7SelectedLinkedPlaybookArtifactId = result.value.artifact.playbookArtifact.artifactId;
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

- (void)startSelectedPhase7ExecutionJournal:(id)sender {
    (void)sender;
    if (_phase7InFlight || !_client) {
        return;
    }
    const NSInteger selected = _phase7LedgerTableView.selectedRow;
    if (selected < 0 || static_cast<std::size_t>(selected) >= _latestPhase7ExecutionLedgers.size()) {
        _phase7StateLabel.stringValue = @"Select a Phase 7 execution ledger row first.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    const auto& ledger = _latestPhase7ExecutionLedgers.at(static_cast<std::size_t>(selected));
    const std::string executionLedgerArtifactId = ledger.ledgerArtifact.artifactId;
    const std::string actor = TrimAscii(ToStdString(_phase7JournalActorField.stringValue));
    const std::string executionCapability = TrimAscii(ToStdString(_phase7ExecutionCapabilityField.stringValue));
    if (executionLedgerArtifactId.empty()) {
        _phase7StateLabel.stringValue = @"Selected execution ledger is missing an artifact id.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    if (actor.empty()) {
        _phase7StateLabel.stringValue = @"Starting an execution journal requires an actor.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    if (executionCapability.empty()) {
        _phase7StateLabel.stringValue = @"Starting an execution journal requires an execution capability.";
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
    _phase7StartJournalButton.enabled = NO;
    _phase7DispatchJournalButton.enabled = NO;
    _phase7StateLabel.stringValue = @"Starting execution journal…";
    _phase7StateLabel.textColor = [NSColor systemOrangeColor];
    _phase7TextView.string = @"Creating or reopening an append-only execution journal for the selected ready ledger…";

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_artifactQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->startExecutionJournalPayload(
            executionLedgerArtifactId,
            actor,
            executionCapability);
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
                innerSelf->_phase7TextView.string = ToNSString(
                    DescribePhase7ExecutionJournalStart(executionLedgerArtifactId, result));
                return;
            }

            auto it = std::find_if(innerSelf->_latestPhase7ExecutionJournals.begin(),
                                   innerSelf->_latestPhase7ExecutionJournals.end(),
                                   [&](const tapescope::Phase7ExecutionJournalArtifact& item) {
                                       return item.journalArtifact.artifactId == result.value.artifact.journalArtifact.artifactId;
                                   });
            if (it != innerSelf->_latestPhase7ExecutionJournals.end()) {
                *it = result.value.artifact;
                const std::size_t index =
                    static_cast<std::size_t>(std::distance(innerSelf->_latestPhase7ExecutionJournals.begin(), it));
                [innerSelf->_phase7JournalTableView reloadData];
                innerSelf->_phase7SelectionIsPlaybook = NO;
                innerSelf->_phase7SelectionIsLedger = NO;
                innerSelf->_phase7SelectionIsJournal = YES;
                [innerSelf->_phase7JournalTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:index]
                                                byExtendingSelection:NO];
            } else {
                innerSelf->_latestPhase7ExecutionJournals.insert(innerSelf->_latestPhase7ExecutionJournals.begin(),
                                                                 result.value.artifact);
                [innerSelf->_phase7JournalTableView reloadData];
                innerSelf->_phase7SelectionIsPlaybook = NO;
                innerSelf->_phase7SelectionIsLedger = NO;
                innerSelf->_phase7SelectionIsJournal = YES;
                [innerSelf->_phase7JournalTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0]
                                                byExtendingSelection:NO];
            }
            [innerSelf->_phase7AnalysisTableView deselectAll:nil];
            [innerSelf->_phase7PlaybookTableView deselectAll:nil];
            [innerSelf->_phase7LedgerTableView deselectAll:nil];
            [innerSelf refreshPhase7DetailText];
            innerSelf->_phase7StateLabel.stringValue = ToNSString(std::string("Phase 7 execution journal ") +
                                                                  (result.value.created ? "created." : "reused."));
            innerSelf->_phase7StateLabel.textColor = [NSColor systemGreenColor];
            innerSelf->_phase7TextView.string = ToNSString(
                DescribePhase7ExecutionJournalStart(executionLedgerArtifactId, result));
            [innerSelf recordRecentHistoryEntry:tapescope::json{
                {"kind", "phase7_execution_journal"},
                {"target_id", result.value.artifact.journalArtifact.artifactId},
                {"artifact_id", result.value.artifact.journalArtifact.artifactId},
                {"ledger_artifact_id", result.value.artifact.ledgerArtifact.artifactId},
                {"playbook_artifact_id", result.value.artifact.playbookArtifact.artifactId},
                {"analysis_artifact_id", result.value.artifact.analysisArtifact.artifactId},
                {"source_artifact_id", result.value.artifact.sourceArtifact.artifactId},
                {"headline", std::string("Phase 7 execution journal")},
                {"detail", std::string(result.value.created ? "Execution journal created"
                                                            : "Execution journal reused") +
                               " with " + std::to_string(result.value.artifact.entries.size()) +
                               " execution entr" +
                               (result.value.artifact.entries.size() == 1 ? "y" : "ies")},
                {"first_session_seq", result.value.artifact.replayContext.value("requested_window", tapescope::json::object()).value("first_session_seq", 0ULL)},
                {"last_session_seq", result.value.artifact.replayContext.value("requested_window", tapescope::json::object()).value("last_session_seq", 0ULL)}
            }];
        });
    });
}

- (void)startSelectedPhase7ExecutionApply:(id)sender {
    (void)sender;
    if (_phase7InFlight || !_client) {
        return;
    }
    const NSInteger selected = _phase7JournalTableView.selectedRow;
    if (selected < 0 || static_cast<std::size_t>(selected) >= _latestPhase7ExecutionJournals.size()) {
        _phase7StateLabel.stringValue = @"Select a Phase 7 execution journal row first.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    const auto& journal = _latestPhase7ExecutionJournals.at(static_cast<std::size_t>(selected));
    const std::string executionJournalArtifactId = journal.journalArtifact.artifactId;
    const std::string actor = TrimAscii(ToStdString(_phase7ExecutionActorField.stringValue));
    const std::string executionCapability = TrimAscii(ToStdString(_phase7ExecutionCapabilityField.stringValue));
    const std::string comment = TrimAscii(ToStdString(_phase7ExecutionCommentField.stringValue));
    if (executionJournalArtifactId.empty()) {
        _phase7StateLabel.stringValue = @"Selected execution journal is missing an artifact id.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    if (actor.empty()) {
        _phase7StateLabel.stringValue = @"Starting controlled apply requires an actor.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    if (executionCapability.empty()) {
        _phase7StateLabel.stringValue = @"Starting controlled apply requires an execution capability.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    const std::vector<std::string> selectedJournalEntryIds =
        SelectedPhase7JournalEntryIds(_phase7ActionTableView, _phase7VisibleJournalEntries);
    const std::vector<std::string> selectedSubmittedJournalEntryIds =
        SelectedPhase7SubmittedJournalEntryIds(_phase7ActionTableView, _phase7VisibleJournalEntries);
    std::vector<std::string> entryIds;
    if (!selectedJournalEntryIds.empty()) {
        if (selectedSubmittedJournalEntryIds.size() != selectedJournalEntryIds.size()) {
            _phase7StateLabel.stringValue = @"Controlled apply only supports submitted execution entries.";
            _phase7StateLabel.textColor = [NSColor systemRedColor];
            return;
        }
        entryIds = selectedSubmittedJournalEntryIds;
    }

    _phase7InFlight = YES;
    const std::uint64_t token = [self issueRequestToken:&_phase7RequestToken];
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
    _phase7StateLabel.stringValue = @"Starting controlled apply…";
    _phase7StateLabel.textColor = [NSColor systemOrangeColor];
    _phase7TextView.string = @"Creating or reopening a controlled apply artifact from the selected submitted journal entries…";

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_artifactQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->startExecutionApplyPayload(
            executionJournalArtifactId,
            entryIds,
            actor,
            executionCapability,
            comment);
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
                innerSelf->_phase7TextView.string = ToNSString(
                    DescribePhase7ExecutionApplyStart(executionJournalArtifactId, result));
                return;
            }

            auto it = std::find_if(innerSelf->_latestPhase7ExecutionApplies.begin(),
                                   innerSelf->_latestPhase7ExecutionApplies.end(),
                                   [&](const tapescope::Phase7ExecutionApplyArtifact& item) {
                                       return item.applyArtifact.artifactId == result.value.artifact.applyArtifact.artifactId;
                                   });
            if (it != innerSelf->_latestPhase7ExecutionApplies.end()) {
                *it = result.value.artifact;
                const std::size_t index =
                    static_cast<std::size_t>(std::distance(innerSelf->_latestPhase7ExecutionApplies.begin(), it));
                [innerSelf->_phase7ApplyTableView reloadData];
                innerSelf->_phase7SelectionIsPlaybook = NO;
                innerSelf->_phase7SelectionIsLedger = NO;
                innerSelf->_phase7SelectionIsJournal = NO;
                innerSelf->_phase7SelectionIsApply = YES;
                [innerSelf->_phase7ApplyTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:index]
                                              byExtendingSelection:NO];
            } else {
                innerSelf->_latestPhase7ExecutionApplies.insert(innerSelf->_latestPhase7ExecutionApplies.begin(),
                                                                result.value.artifact);
                [innerSelf->_phase7ApplyTableView reloadData];
                innerSelf->_phase7SelectionIsPlaybook = NO;
                innerSelf->_phase7SelectionIsLedger = NO;
                innerSelf->_phase7SelectionIsJournal = NO;
                innerSelf->_phase7SelectionIsApply = YES;
                [innerSelf->_phase7ApplyTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0]
                                              byExtendingSelection:NO];
            }
            [innerSelf->_phase7AnalysisTableView deselectAll:nil];
            [innerSelf->_phase7PlaybookTableView deselectAll:nil];
            [innerSelf->_phase7LedgerTableView deselectAll:nil];
            [innerSelf->_phase7JournalTableView deselectAll:nil];
            [innerSelf refreshPhase7DetailText];
            innerSelf->_phase7StateLabel.stringValue = ToNSString(std::string("Phase 7 controlled apply ") +
                                                                  (result.value.created ? "created." : "reused."));
            innerSelf->_phase7StateLabel.textColor = [NSColor systemGreenColor];
            innerSelf->_phase7TextView.string = ToNSString(
                DescribePhase7ExecutionApplyStart(executionJournalArtifactId, result));
            [innerSelf recordRecentHistoryEntry:tapescope::json{
                {"kind", "phase7_execution_apply"},
                {"target_id", result.value.artifact.applyArtifact.artifactId},
                {"artifact_id", result.value.artifact.applyArtifact.artifactId},
                {"journal_artifact_id", result.value.artifact.journalArtifact.artifactId},
                {"ledger_artifact_id", result.value.artifact.ledgerArtifact.artifactId},
                {"playbook_artifact_id", result.value.artifact.playbookArtifact.artifactId},
                {"analysis_artifact_id", result.value.artifact.analysisArtifact.artifactId},
                {"source_artifact_id", result.value.artifact.sourceArtifact.artifactId},
                {"headline", std::string("Phase 7 execution apply")},
                {"detail", std::string(result.value.created ? "Controlled apply created"
                                                            : "Controlled apply reused") +
                               " with " + std::to_string(result.value.artifact.entries.size()) +
                               " apply entr" +
                               (result.value.artifact.entries.size() == 1 ? "y" : "ies")},
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
        if ((_phase7SelectionIsLedger || _phase7SelectionIsJournal || _phase7SelectionIsApply) &&
            !_phase7SelectedLinkedPlaybookArtifactId.empty()) {
            [self openPhase7PlaybookArtifactId:_phase7SelectedLinkedPlaybookArtifactId];
            return;
        }
        _phase7StateLabel.stringValue = @"Select a Phase 7 playbook row first.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    [self openPhase7PlaybookArtifactId:_latestPhase7PlaybookArtifacts.at(static_cast<std::size_t>(selected)).playbookArtifact.artifactId];
}

- (void)openSelectedPhase7ExecutionLedger:(id)sender {
    (void)sender;
    const NSInteger selected = _phase7LedgerTableView.selectedRow;
    if (selected < 0 || static_cast<std::size_t>(selected) >= _latestPhase7ExecutionLedgers.size()) {
        if ((_phase7SelectionIsJournal || _phase7SelectionIsApply) && !_phase7SelectedLinkedLedgerArtifactId.empty()) {
            [self openPhase7ExecutionLedgerArtifactId:_phase7SelectedLinkedLedgerArtifactId];
            return;
        }
        _phase7StateLabel.stringValue = @"Select a Phase 7 execution ledger row first.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    [self openPhase7ExecutionLedgerArtifactId:_latestPhase7ExecutionLedgers.at(static_cast<std::size_t>(selected)).ledgerArtifact.artifactId];
}

- (void)openSelectedPhase7ExecutionJournal:(id)sender {
    (void)sender;
    const NSInteger selected = _phase7JournalTableView.selectedRow;
    if (selected < 0 || static_cast<std::size_t>(selected) >= _latestPhase7ExecutionJournals.size()) {
        _phase7StateLabel.stringValue = @"Select a Phase 7 execution journal row first.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    [self openPhase7ExecutionJournalArtifactId:
              _latestPhase7ExecutionJournals.at(static_cast<std::size_t>(selected)).journalArtifact.artifactId];
}

- (void)openSelectedPhase7ExecutionApply:(id)sender {
    (void)sender;
    const NSInteger selected = _phase7ApplyTableView.selectedRow;
    if (selected < 0 || static_cast<std::size_t>(selected) >= _latestPhase7ExecutionApplies.size()) {
        _phase7StateLabel.stringValue = @"Select a Phase 7 execution apply row first.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    [self openPhase7ExecutionApplyArtifactId:
              _latestPhase7ExecutionApplies.at(static_cast<std::size_t>(selected)).applyArtifact.artifactId];
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
                innerSelf->_phase7SelectionIsLedger = NO;
                innerSelf->_phase7SelectionIsJournal = NO;
                [innerSelf->_phase7AnalysisTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:index] byExtendingSelection:NO];
            } else {
                innerSelf->_latestPhase7AnalysisArtifacts.insert(innerSelf->_latestPhase7AnalysisArtifacts.begin(), result.value);
                [innerSelf->_phase7AnalysisTableView reloadData];
                innerSelf->_phase7SelectionIsPlaybook = NO;
                innerSelf->_phase7SelectionIsLedger = NO;
                innerSelf->_phase7SelectionIsJournal = NO;
                [innerSelf->_phase7AnalysisTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0] byExtendingSelection:NO];
            }
            [innerSelf->_phase7PlaybookTableView deselectAll:nil];
            [innerSelf->_phase7LedgerTableView deselectAll:nil];
            [innerSelf->_phase7JournalTableView deselectAll:nil];
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
                innerSelf->_phase7SelectionIsLedger = NO;
                innerSelf->_phase7SelectionIsJournal = NO;
                [innerSelf->_phase7PlaybookTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:index] byExtendingSelection:NO];
            } else {
                innerSelf->_latestPhase7PlaybookArtifacts.insert(innerSelf->_latestPhase7PlaybookArtifacts.begin(), result.value);
                [innerSelf->_phase7PlaybookTableView reloadData];
                innerSelf->_phase7SelectionIsPlaybook = YES;
                innerSelf->_phase7SelectionIsLedger = NO;
                innerSelf->_phase7SelectionIsJournal = NO;
                [innerSelf->_phase7PlaybookTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0] byExtendingSelection:NO];
            }
            [innerSelf->_phase7AnalysisTableView deselectAll:nil];
            [innerSelf->_phase7LedgerTableView deselectAll:nil];
            [innerSelf->_phase7JournalTableView deselectAll:nil];
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

            auto it = std::find_if(innerSelf->_latestPhase7ExecutionLedgers.begin(),
                                   innerSelf->_latestPhase7ExecutionLedgers.end(),
                                   [&](const tapescope::Phase7ExecutionLedgerArtifact& item) {
                                       return item.ledgerArtifact.artifactId == artifactId;
                                   });
            if (it != innerSelf->_latestPhase7ExecutionLedgers.end()) {
                *it = result.value;
                const std::size_t index =
                    static_cast<std::size_t>(std::distance(innerSelf->_latestPhase7ExecutionLedgers.begin(), it));
                [innerSelf->_phase7LedgerTableView reloadData];
                innerSelf->_phase7SelectionIsPlaybook = NO;
                innerSelf->_phase7SelectionIsLedger = YES;
                innerSelf->_phase7SelectionIsJournal = NO;
                [innerSelf->_phase7LedgerTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:index]
                                               byExtendingSelection:NO];
            } else {
                innerSelf->_latestPhase7ExecutionLedgers.insert(innerSelf->_latestPhase7ExecutionLedgers.begin(), result.value);
                [innerSelf->_phase7LedgerTableView reloadData];
                innerSelf->_phase7SelectionIsPlaybook = NO;
                innerSelf->_phase7SelectionIsLedger = YES;
                innerSelf->_phase7SelectionIsJournal = NO;
                [innerSelf->_phase7LedgerTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0]
                                               byExtendingSelection:NO];
            }
            [innerSelf->_phase7AnalysisTableView deselectAll:nil];
            [innerSelf->_phase7PlaybookTableView deselectAll:nil];
            [innerSelf->_phase7JournalTableView deselectAll:nil];
            innerSelf->_phase7SelectedSourceArtifactId = result.value.sourceArtifact.artifactId;
            innerSelf->_phase7SelectedLinkedAnalysisArtifactId = result.value.analysisArtifact.artifactId;
            innerSelf->_phase7SelectedLinkedPlaybookArtifactId = result.value.playbookArtifact.artifactId;
            innerSelf->_phase7SelectedLinkedLedgerArtifactId = result.value.ledgerArtifact.artifactId;
            if (const auto replayRange = ReplayRangeFromPhase7Context(result.value.replayContext); replayRange.has_value()) {
                innerSelf->_phase7ReplayRange = *replayRange;
                innerSelf->_phase7LoadRangeButton.enabled = YES;
            } else {
                innerSelf->_phase7ReplayRange = {};
                innerSelf->_phase7LoadRangeButton.enabled = NO;
            }
            [innerSelf refreshPhase7DetailText];
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

- (void)openPhase7ExecutionJournalArtifactId:(const std::string&)artifactId {
    if (_phase7InFlight || !_client) {
        return;
    }
    if (artifactId.empty()) {
        _phase7StateLabel.stringValue = @"Phase 7 execution journal artifact id is missing.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    _phase7InFlight = YES;
    const std::uint64_t token = [self issueRequestToken:&_phase7RequestToken];
    _phase7RefreshButton.enabled = NO;
    _phase7StateLabel.stringValue = @"Opening Phase 7 execution journal…";
    _phase7StateLabel.textColor = [NSColor systemOrangeColor];

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_artifactQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->readExecutionJournalArtifactPayload(artifactId);
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

            auto it = std::find_if(innerSelf->_latestPhase7ExecutionJournals.begin(),
                                   innerSelf->_latestPhase7ExecutionJournals.end(),
                                   [&](const tapescope::Phase7ExecutionJournalArtifact& item) {
                                       return item.journalArtifact.artifactId == artifactId;
                                   });
            if (it != innerSelf->_latestPhase7ExecutionJournals.end()) {
                *it = result.value;
                const std::size_t index =
                    static_cast<std::size_t>(std::distance(innerSelf->_latestPhase7ExecutionJournals.begin(), it));
                [innerSelf->_phase7JournalTableView reloadData];
                innerSelf->_phase7SelectionIsPlaybook = NO;
                innerSelf->_phase7SelectionIsLedger = NO;
                innerSelf->_phase7SelectionIsJournal = YES;
                [innerSelf->_phase7JournalTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:index]
                                                byExtendingSelection:NO];
            } else {
                innerSelf->_latestPhase7ExecutionJournals.insert(innerSelf->_latestPhase7ExecutionJournals.begin(), result.value);
                [innerSelf->_phase7JournalTableView reloadData];
                innerSelf->_phase7SelectionIsPlaybook = NO;
                innerSelf->_phase7SelectionIsLedger = NO;
                innerSelf->_phase7SelectionIsJournal = YES;
                [innerSelf->_phase7JournalTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0]
                                                byExtendingSelection:NO];
            }
            [innerSelf->_phase7AnalysisTableView deselectAll:nil];
            [innerSelf->_phase7PlaybookTableView deselectAll:nil];
            [innerSelf->_phase7LedgerTableView deselectAll:nil];
            innerSelf->_phase7SelectedSourceArtifactId = result.value.sourceArtifact.artifactId;
            innerSelf->_phase7SelectedLinkedAnalysisArtifactId = result.value.analysisArtifact.artifactId;
            innerSelf->_phase7SelectedLinkedPlaybookArtifactId = result.value.playbookArtifact.artifactId;
            innerSelf->_phase7SelectedLinkedLedgerArtifactId = result.value.ledgerArtifact.artifactId;
            if (const auto replayRange = ReplayRangeFromPhase7Context(result.value.replayContext); replayRange.has_value()) {
                innerSelf->_phase7ReplayRange = *replayRange;
                innerSelf->_phase7LoadRangeButton.enabled = YES;
            } else {
                innerSelf->_phase7ReplayRange = {};
                innerSelf->_phase7LoadRangeButton.enabled = NO;
            }
            [innerSelf refreshPhase7DetailText];
            innerSelf->_phase7StateLabel.stringValue = @"Phase 7 execution journal reopened.";
            innerSelf->_phase7StateLabel.textColor = [NSColor systemGreenColor];
            innerSelf->_phase7TextView.string = ToNSString(DescribePhase7ExecutionJournalArtifact(result.value));
            [innerSelf recordRecentHistoryEntry:tapescope::json{
                {"kind", "phase7_execution_journal"},
                {"target_id", artifactId},
                {"artifact_id", artifactId},
                {"ledger_artifact_id", result.value.ledgerArtifact.artifactId},
                {"playbook_artifact_id", result.value.playbookArtifact.artifactId},
                {"analysis_artifact_id", result.value.analysisArtifact.artifactId},
                {"source_artifact_id", result.value.sourceArtifact.artifactId},
                {"headline", std::string("Phase 7 execution journal")},
                {"detail", std::string("Execution entries: ") + std::to_string(result.value.entries.size())},
                {"first_session_seq", result.value.replayContext.value("requested_window", tapescope::json::object()).value("first_session_seq", 0ULL)},
                {"last_session_seq", result.value.replayContext.value("requested_window", tapescope::json::object()).value("last_session_seq", 0ULL)}
            }];
        });
    });
}

- (void)openPhase7ExecutionApplyArtifactId:(const std::string&)artifactId {
    if (_phase7InFlight || !_client) {
        return;
    }
    if (artifactId.empty()) {
        _phase7StateLabel.stringValue = @"Phase 7 execution apply artifact id is missing.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    _phase7InFlight = YES;
    const std::uint64_t token = [self issueRequestToken:&_phase7RequestToken];
    _phase7RefreshButton.enabled = NO;
    _phase7StateLabel.stringValue = @"Opening Phase 7 execution apply…";
    _phase7StateLabel.textColor = [NSColor systemOrangeColor];

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_artifactQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->readExecutionApplyArtifactPayload(artifactId);
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

            auto it = std::find_if(innerSelf->_latestPhase7ExecutionApplies.begin(),
                                   innerSelf->_latestPhase7ExecutionApplies.end(),
                                   [&](const tapescope::Phase7ExecutionApplyArtifact& item) {
                                       return item.applyArtifact.artifactId == artifactId;
                                   });
            if (it != innerSelf->_latestPhase7ExecutionApplies.end()) {
                *it = result.value;
                const std::size_t index =
                    static_cast<std::size_t>(std::distance(innerSelf->_latestPhase7ExecutionApplies.begin(), it));
                [innerSelf->_phase7ApplyTableView reloadData];
                innerSelf->_phase7SelectionIsPlaybook = NO;
                innerSelf->_phase7SelectionIsLedger = NO;
                innerSelf->_phase7SelectionIsJournal = NO;
                innerSelf->_phase7SelectionIsApply = YES;
                [innerSelf->_phase7ApplyTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:index]
                                              byExtendingSelection:NO];
            } else {
                innerSelf->_latestPhase7ExecutionApplies.insert(innerSelf->_latestPhase7ExecutionApplies.begin(),
                                                                result.value);
                [innerSelf->_phase7ApplyTableView reloadData];
                innerSelf->_phase7SelectionIsPlaybook = NO;
                innerSelf->_phase7SelectionIsLedger = NO;
                innerSelf->_phase7SelectionIsJournal = NO;
                innerSelf->_phase7SelectionIsApply = YES;
                [innerSelf->_phase7ApplyTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0]
                                              byExtendingSelection:NO];
            }
            [innerSelf->_phase7AnalysisTableView deselectAll:nil];
            [innerSelf->_phase7PlaybookTableView deselectAll:nil];
            [innerSelf->_phase7LedgerTableView deselectAll:nil];
            [innerSelf->_phase7JournalTableView deselectAll:nil];
            [innerSelf refreshPhase7DetailText];
            innerSelf->_phase7StateLabel.stringValue = @"Phase 7 execution apply reopened.";
            innerSelf->_phase7StateLabel.textColor = [NSColor systemGreenColor];
            innerSelf->_phase7TextView.string = ToNSString(DescribePhase7ExecutionApplyArtifact(result.value));
            [innerSelf recordRecentHistoryEntry:tapescope::json{
                {"kind", "phase7_execution_apply"},
                {"target_id", artifactId},
                {"artifact_id", artifactId},
                {"journal_artifact_id", result.value.journalArtifact.artifactId},
                {"ledger_artifact_id", result.value.ledgerArtifact.artifactId},
                {"playbook_artifact_id", result.value.playbookArtifact.artifactId},
                {"analysis_artifact_id", result.value.analysisArtifact.artifactId},
                {"source_artifact_id", result.value.sourceArtifact.artifactId},
                {"headline", std::string("Phase 7 execution apply")},
                {"detail", std::string("Apply entries: ") + std::to_string(result.value.entries.size())},
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

- (void)openSelectedPhase7LinkedLedger:(id)sender {
    (void)sender;
    if (_phase7SelectedLinkedLedgerArtifactId.empty()) {
        _phase7StateLabel.stringValue = @"Selected execution journal does not expose a linked ledger artifact.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    [self openPhase7ExecutionLedgerArtifactId:_phase7SelectedLinkedLedgerArtifactId];
}

- (void)openSelectedPhase7LinkedJournal:(id)sender {
    (void)sender;
    if (_phase7SelectedLinkedJournalArtifactId.empty()) {
        _phase7StateLabel.stringValue = @"Selected controlled apply does not expose a linked journal artifact.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    [self openPhase7ExecutionJournalArtifactId:_phase7SelectedLinkedJournalArtifactId];
}

- (void)recordSelectedPhase7LedgerReview:(id)sender {
    (void)sender;
    if (_phase7InFlight || !_client) {
        return;
    }
    const NSInteger ledgerSelected = _phase7LedgerTableView.selectedRow;
    if (ledgerSelected < 0 || static_cast<std::size_t>(ledgerSelected) >= _latestPhase7ExecutionLedgers.size()) {
        _phase7StateLabel.stringValue = @"Select a Phase 7 execution ledger row first.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    const auto& ledger = _latestPhase7ExecutionLedgers.at(static_cast<std::size_t>(ledgerSelected));
    const std::vector<std::string> entryIds =
        SelectedPhase7LedgerEntryIds(_phase7ActionTableView, _phase7VisibleLedgerEntries);
    if (entryIds.empty()) {
        _phase7StateLabel.stringValue = @"Select one or more review entries first.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    const std::string reviewStatus = SelectedPhase7ReviewStatus(_phase7ReviewStatusPopup);
    if (reviewStatus.empty()) {
        _phase7StateLabel.stringValue = @"Choose a ledger review status first.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    const std::string actor = TrimAscii(ToStdString(_phase7ReviewActorField.stringValue));
    const std::string comment = TrimAscii(ToStdString(_phase7ReviewCommentField.stringValue));
    if (actor.empty()) {
        _phase7StateLabel.stringValue = @"Ledger review requires a reviewer identity.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    if ((reviewStatus == tape_phase7::kLedgerEntryStatusBlocked ||
         reviewStatus == tape_phase7::kLedgerEntryStatusNeedsInfo) &&
        comment.empty()) {
        _phase7StateLabel.stringValue = @"Blocked and needs_info reviews require a review comment.";
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
    _phase7OpenLedgerButton.enabled = NO;
    _phase7RecordReviewButton.enabled = NO;
    _phase7DispatchJournalButton.enabled = NO;
    _phase7StateLabel.stringValue = @"Recording ledger review decision…";
    _phase7StateLabel.textColor = [NSColor systemOrangeColor];
    _phase7TextView.string = @"Appending a review decision to the selected Phase 7 execution-ledger entries…";

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_artifactQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->recordExecutionLedgerReviewPayload(
            ledger.ledgerArtifact.artifactId,
            entryIds,
            reviewStatus,
            actor,
            comment);
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
                return;
            }

            auto it = std::find_if(innerSelf->_latestPhase7ExecutionLedgers.begin(),
                                   innerSelf->_latestPhase7ExecutionLedgers.end(),
                                   [&](const tapescope::Phase7ExecutionLedgerArtifact& item) {
                                       return item.ledgerArtifact.artifactId == result.value.artifact.ledgerArtifact.artifactId;
                                   });
            std::size_t selectedIndex = 0;
            if (it != innerSelf->_latestPhase7ExecutionLedgers.end()) {
                *it = result.value.artifact;
                selectedIndex = static_cast<std::size_t>(std::distance(innerSelf->_latestPhase7ExecutionLedgers.begin(), it));
            } else {
                innerSelf->_latestPhase7ExecutionLedgers.insert(innerSelf->_latestPhase7ExecutionLedgers.begin(),
                                                                result.value.artifact);
                selectedIndex = 0;
            }
            [innerSelf->_phase7LedgerTableView reloadData];
            innerSelf->_phase7SelectionIsPlaybook = NO;
            innerSelf->_phase7SelectionIsLedger = YES;
            [innerSelf->_phase7AnalysisTableView deselectAll:nil];
            [innerSelf->_phase7PlaybookTableView deselectAll:nil];
            [innerSelf->_phase7LedgerTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:selectedIndex]
                                           byExtendingSelection:NO];
            [innerSelf refreshPhase7DetailText];
            NSIndexSet* updatedSelection = IndexSetForPhase7LedgerEntryIds(result.value.updatedEntryIds,
                                                                           innerSelf->_phase7VisibleLedgerEntries);
            if (updatedSelection.count > 0) {
                [innerSelf->_phase7ActionTableView selectRowIndexes:updatedSelection byExtendingSelection:NO];
                innerSelf->_phase7RecordReviewButton.enabled = YES;
                innerSelf->_phase7TextView.string = ToNSString(innerSelf->_phase7DetailBody + "\n\n" +
                                                               DescribeSelectedPhase7LedgerEntries(
                                                                   innerSelf->_phase7VisibleLedgerEntries,
                                                                   innerSelf->_phase7ActionTableView.selectedRowIndexes));
            }
            innerSelf->_phase7StateLabel.stringValue = ToNSString(
                std::string("Recorded `") + reviewStatus + "` for " + std::to_string(result.value.updatedEntryIds.size()) +
                " ledger entr" + (result.value.updatedEntryIds.size() == 1 ? "y." : "ies."));
            innerSelf->_phase7StateLabel.textColor = [NSColor systemGreenColor];
            [innerSelf recordRecentHistoryEntry:tapescope::json{
                {"kind", "phase7_execution_ledger"},
                {"target_id", result.value.artifact.ledgerArtifact.artifactId},
                {"artifact_id", result.value.artifact.ledgerArtifact.artifactId},
                {"playbook_artifact_id", result.value.artifact.playbookArtifact.artifactId},
                {"analysis_artifact_id", result.value.artifact.analysisArtifact.artifactId},
                {"source_artifact_id", result.value.artifact.sourceArtifact.artifactId},
                {"headline", std::string("Phase 7 execution ledger")},
                {"detail", std::string("Recorded ") + reviewStatus + " for " +
                               std::to_string(result.value.updatedEntryIds.size()) + " entr" +
                               (result.value.updatedEntryIds.size() == 1 ? "y" : "ies")},
                {"first_session_seq", result.value.artifact.replayContext.value("requested_window", tapescope::json::object()).value("first_session_seq", 0ULL)},
                {"last_session_seq", result.value.artifact.replayContext.value("requested_window", tapescope::json::object()).value("last_session_seq", 0ULL)}
            }];
        });
    });
}

- (void)dispatchSelectedPhase7JournalEntries:(id)sender {
    (void)sender;
    if (_phase7InFlight || !_client) {
        return;
    }
    const NSInteger journalSelected = _phase7JournalTableView.selectedRow;
    if (journalSelected < 0 || static_cast<std::size_t>(journalSelected) >= _latestPhase7ExecutionJournals.size()) {
        _phase7StateLabel.stringValue = @"Select a Phase 7 execution journal row first.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    const auto& journal = _latestPhase7ExecutionJournals.at(static_cast<std::size_t>(journalSelected));
    const std::vector<std::string> entryIds =
        SelectedPhase7QueuedJournalEntryIds(_phase7ActionTableView, _phase7VisibleJournalEntries);
    if (entryIds.empty()) {
        _phase7StateLabel.stringValue = @"Select one or more queued execution entries first.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    const std::string actor = TrimAscii(ToStdString(_phase7ExecutionActorField.stringValue));
    const std::string executionCapability = TrimAscii(ToStdString(_phase7ExecutionCapabilityField.stringValue));
    const std::string comment = TrimAscii(ToStdString(_phase7ExecutionCommentField.stringValue));
    if (actor.empty()) {
        _phase7StateLabel.stringValue = @"Dispatching queued entries requires an actor.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    if (executionCapability.empty()) {
        _phase7StateLabel.stringValue = @"Dispatching queued entries requires an execution capability.";
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
    _phase7StartJournalButton.enabled = NO;
    _phase7OpenJournalButton.enabled = NO;
    _phase7DispatchJournalButton.enabled = NO;
    _phase7RecordExecutionButton.enabled = NO;
    _phase7StateLabel.stringValue = @"Dispatching queued execution entries…";
    _phase7StateLabel.textColor = [NSColor systemOrangeColor];
    _phase7TextView.string = @"Promoting queued execution entries into the controlled execution journal flow…";

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_artifactQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->dispatchExecutionJournalPayload(
            journal.journalArtifact.artifactId,
            entryIds,
            actor,
            executionCapability,
            comment);
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
                return;
            }

            auto it = std::find_if(innerSelf->_latestPhase7ExecutionJournals.begin(),
                                   innerSelf->_latestPhase7ExecutionJournals.end(),
                                   [&](const tapescope::Phase7ExecutionJournalArtifact& item) {
                                       return item.journalArtifact.artifactId == result.value.artifact.journalArtifact.artifactId;
                                   });
            std::size_t selectedIndex = 0;
            if (it != innerSelf->_latestPhase7ExecutionJournals.end()) {
                *it = result.value.artifact;
                selectedIndex = static_cast<std::size_t>(std::distance(innerSelf->_latestPhase7ExecutionJournals.begin(), it));
            } else {
                innerSelf->_latestPhase7ExecutionJournals.insert(innerSelf->_latestPhase7ExecutionJournals.begin(),
                                                                 result.value.artifact);
                selectedIndex = 0;
            }
            [innerSelf->_phase7JournalTableView reloadData];
            innerSelf->_phase7SelectionIsPlaybook = NO;
            innerSelf->_phase7SelectionIsLedger = NO;
            innerSelf->_phase7SelectionIsJournal = YES;
            [innerSelf->_phase7AnalysisTableView deselectAll:nil];
            [innerSelf->_phase7PlaybookTableView deselectAll:nil];
            [innerSelf->_phase7LedgerTableView deselectAll:nil];
            [innerSelf->_phase7JournalTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:selectedIndex]
                                            byExtendingSelection:NO];
            [innerSelf refreshPhase7DetailText];
            NSIndexSet* updatedSelection = IndexSetForPhase7JournalEntryIds(result.value.updatedEntryIds,
                                                                            innerSelf->_phase7VisibleJournalEntries);
            if (updatedSelection.count > 0) {
                [innerSelf->_phase7ActionTableView selectRowIndexes:updatedSelection byExtendingSelection:NO];
                innerSelf->_phase7DispatchJournalButton.enabled =
                    !SelectedPhase7QueuedJournalEntryIds(innerSelf->_phase7ActionTableView,
                                                         innerSelf->_phase7VisibleJournalEntries).empty();
                innerSelf->_phase7RecordExecutionButton.enabled = YES;
                innerSelf->_phase7TextView.string = ToNSString(innerSelf->_phase7DetailBody + "\n\n" +
                                                               DescribeSelectedPhase7JournalEntries(
                                                                   innerSelf->_phase7VisibleJournalEntries,
                                                                   innerSelf->_phase7ActionTableView.selectedRowIndexes));
            }
            innerSelf->_phase7StateLabel.stringValue = ToNSString(
                std::string("Dispatched ") + std::to_string(result.value.updatedEntryIds.size()) +
                " execution entr" + (result.value.updatedEntryIds.size() == 1 ? "y." : "ies."));
            innerSelf->_phase7StateLabel.textColor = [NSColor systemGreenColor];
            [innerSelf recordRecentHistoryEntry:tapescope::json{
                {"kind", "phase7_execution_journal"},
                {"target_id", result.value.artifact.journalArtifact.artifactId},
                {"artifact_id", result.value.artifact.journalArtifact.artifactId},
                {"ledger_artifact_id", result.value.artifact.ledgerArtifact.artifactId},
                {"playbook_artifact_id", result.value.artifact.playbookArtifact.artifactId},
                {"analysis_artifact_id", result.value.artifact.analysisArtifact.artifactId},
                {"source_artifact_id", result.value.artifact.sourceArtifact.artifactId},
                {"headline", std::string("Phase 7 execution journal")},
                {"detail", std::string("Dispatched ") + std::to_string(result.value.updatedEntryIds.size()) +
                               " queued entr" + (result.value.updatedEntryIds.size() == 1 ? "y" : "ies")},
                {"first_session_seq", result.value.artifact.replayContext.value("requested_window", tapescope::json::object()).value("first_session_seq", 0ULL)},
                {"last_session_seq", result.value.artifact.replayContext.value("requested_window", tapescope::json::object()).value("last_session_seq", 0ULL)}
            }];
        });
    });
}

- (void)recordSelectedPhase7JournalEvent:(id)sender {
    (void)sender;
    if (_phase7InFlight || !_client) {
        return;
    }
    const NSInteger journalSelected = _phase7JournalTableView.selectedRow;
    if (journalSelected < 0 || static_cast<std::size_t>(journalSelected) >= _latestPhase7ExecutionJournals.size()) {
        _phase7StateLabel.stringValue = @"Select a Phase 7 execution journal row first.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    const auto& journal = _latestPhase7ExecutionJournals.at(static_cast<std::size_t>(journalSelected));
    const std::vector<std::string> entryIds =
        SelectedPhase7JournalEntryIds(_phase7ActionTableView, _phase7VisibleJournalEntries);
    if (entryIds.empty()) {
        _phase7StateLabel.stringValue = @"Select one or more execution entries first.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    const std::string executionStatus = SelectedPhase7ExecutionStatus(_phase7ExecutionStatusPopup);
    const std::string actor = TrimAscii(ToStdString(_phase7ExecutionActorField.stringValue));
    const std::string comment = TrimAscii(ToStdString(_phase7ExecutionCommentField.stringValue));
    const std::string failureCode = TrimAscii(ToStdString(_phase7ExecutionFailureCodeField.stringValue));
    const std::string failureMessage = TrimAscii(ToStdString(_phase7ExecutionFailureMessageField.stringValue));
    if (executionStatus.empty()) {
        _phase7StateLabel.stringValue = @"Choose an execution status first.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    if (actor.empty()) {
        _phase7StateLabel.stringValue = @"Execution journal updates require an actor.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    if ((executionStatus == tape_phase7::kExecutionEntryStatusFailed ||
         executionStatus == tape_phase7::kExecutionEntryStatusCancelled) &&
        comment.empty()) {
        _phase7StateLabel.stringValue = @"Failed and cancelled execution updates require a comment.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    if (executionStatus == tape_phase7::kExecutionEntryStatusFailed &&
        failureCode.empty() && failureMessage.empty()) {
        _phase7StateLabel.stringValue = @"Failed execution updates require a failure code or message.";
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
    _phase7StartJournalButton.enabled = NO;
    _phase7OpenJournalButton.enabled = NO;
    _phase7DispatchJournalButton.enabled = NO;
    _phase7RecordExecutionButton.enabled = NO;
    _phase7StateLabel.stringValue = @"Recording execution journal event…";
    _phase7StateLabel.textColor = [NSColor systemOrangeColor];
    _phase7TextView.string = @"Appending an execution lifecycle event to the selected journal entries…";

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_artifactQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->recordExecutionJournalEventPayload(
            journal.journalArtifact.artifactId,
            entryIds,
            executionStatus,
            actor,
            comment,
            failureCode,
            failureMessage);
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
                return;
            }

            auto it = std::find_if(innerSelf->_latestPhase7ExecutionJournals.begin(),
                                   innerSelf->_latestPhase7ExecutionJournals.end(),
                                   [&](const tapescope::Phase7ExecutionJournalArtifact& item) {
                                       return item.journalArtifact.artifactId == result.value.artifact.journalArtifact.artifactId;
                                   });
            std::size_t selectedIndex = 0;
            if (it != innerSelf->_latestPhase7ExecutionJournals.end()) {
                *it = result.value.artifact;
                selectedIndex = static_cast<std::size_t>(std::distance(innerSelf->_latestPhase7ExecutionJournals.begin(), it));
            } else {
                innerSelf->_latestPhase7ExecutionJournals.insert(innerSelf->_latestPhase7ExecutionJournals.begin(),
                                                                 result.value.artifact);
                selectedIndex = 0;
            }
            [innerSelf->_phase7JournalTableView reloadData];
            innerSelf->_phase7SelectionIsPlaybook = NO;
            innerSelf->_phase7SelectionIsLedger = NO;
            innerSelf->_phase7SelectionIsJournal = YES;
            [innerSelf->_phase7AnalysisTableView deselectAll:nil];
            [innerSelf->_phase7PlaybookTableView deselectAll:nil];
            [innerSelf->_phase7LedgerTableView deselectAll:nil];
            [innerSelf->_phase7JournalTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:selectedIndex]
                                            byExtendingSelection:NO];
            [innerSelf refreshPhase7DetailText];
            NSIndexSet* updatedSelection = IndexSetForPhase7JournalEntryIds(result.value.updatedEntryIds,
                                                                            innerSelf->_phase7VisibleJournalEntries);
            if (updatedSelection.count > 0) {
                [innerSelf->_phase7ActionTableView selectRowIndexes:updatedSelection byExtendingSelection:NO];
                innerSelf->_phase7DispatchJournalButton.enabled =
                    !SelectedPhase7QueuedJournalEntryIds(innerSelf->_phase7ActionTableView,
                                                         innerSelf->_phase7VisibleJournalEntries).empty();
                innerSelf->_phase7RecordExecutionButton.enabled = YES;
                innerSelf->_phase7TextView.string = ToNSString(innerSelf->_phase7DetailBody + "\n\n" +
                                                               DescribeSelectedPhase7JournalEntries(
                                                                   innerSelf->_phase7VisibleJournalEntries,
                                                                   innerSelf->_phase7ActionTableView.selectedRowIndexes));
            }
            innerSelf->_phase7StateLabel.stringValue = ToNSString(
                std::string("Recorded `") + executionStatus + "` for " +
                std::to_string(result.value.updatedEntryIds.size()) + " execution entr" +
                (result.value.updatedEntryIds.size() == 1 ? "y." : "ies."));
            innerSelf->_phase7StateLabel.textColor = [NSColor systemGreenColor];
            [innerSelf recordRecentHistoryEntry:tapescope::json{
                {"kind", "phase7_execution_journal"},
                {"target_id", result.value.artifact.journalArtifact.artifactId},
                {"artifact_id", result.value.artifact.journalArtifact.artifactId},
                {"ledger_artifact_id", result.value.artifact.ledgerArtifact.artifactId},
                {"playbook_artifact_id", result.value.artifact.playbookArtifact.artifactId},
                {"analysis_artifact_id", result.value.artifact.analysisArtifact.artifactId},
                {"source_artifact_id", result.value.artifact.sourceArtifact.artifactId},
                {"headline", std::string("Phase 7 execution journal")},
                {"detail", std::string("Recorded ") + executionStatus + " for " +
                               std::to_string(result.value.updatedEntryIds.size()) + " entr" +
                               (result.value.updatedEntryIds.size() == 1 ? "y" : "ies")},
                {"first_session_seq", result.value.artifact.replayContext.value("requested_window", tapescope::json::object()).value("first_session_seq", 0ULL)},
                {"last_session_seq", result.value.artifact.replayContext.value("requested_window", tapescope::json::object()).value("last_session_seq", 0ULL)}
            }];
        });
    });
}

- (void)recordSelectedPhase7ApplyEvent:(id)sender {
    (void)sender;
    if (_phase7InFlight || !_client) {
        return;
    }
    const NSInteger applySelected = _phase7ApplyTableView.selectedRow;
    if (applySelected < 0 || static_cast<std::size_t>(applySelected) >= _latestPhase7ExecutionApplies.size()) {
        _phase7StateLabel.stringValue = @"Select a Phase 7 execution apply row first.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    const auto& apply = _latestPhase7ExecutionApplies.at(static_cast<std::size_t>(applySelected));
    const std::vector<std::string> entryIds =
        SelectedPhase7ApplyEntryIds(_phase7ActionTableView, _phase7VisibleApplyEntries);
    if (entryIds.empty()) {
        _phase7StateLabel.stringValue = @"Select one or more apply entries first.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    const std::string executionStatus = SelectedPhase7ExecutionStatus(_phase7ExecutionStatusPopup);
    const std::string actor = TrimAscii(ToStdString(_phase7ExecutionActorField.stringValue));
    const std::string comment = TrimAscii(ToStdString(_phase7ExecutionCommentField.stringValue));
    const std::string failureCode = TrimAscii(ToStdString(_phase7ExecutionFailureCodeField.stringValue));
    const std::string failureMessage = TrimAscii(ToStdString(_phase7ExecutionFailureMessageField.stringValue));
    if (executionStatus.empty()) {
        _phase7StateLabel.stringValue = @"Choose an apply status first.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    if (actor.empty()) {
        _phase7StateLabel.stringValue = @"Controlled apply updates require an actor.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    if ((executionStatus == tape_phase7::kExecutionEntryStatusFailed ||
         executionStatus == tape_phase7::kExecutionEntryStatusCancelled) &&
        comment.empty()) {
        _phase7StateLabel.stringValue = @"Failed and cancelled apply updates require a comment.";
        _phase7StateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    if (executionStatus == tape_phase7::kExecutionEntryStatusFailed &&
        failureCode.empty() && failureMessage.empty()) {
        _phase7StateLabel.stringValue = @"Failed apply updates require a failure code or message.";
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
    _phase7StartJournalButton.enabled = NO;
    _phase7StartApplyButton.enabled = NO;
    _phase7OpenApplyButton.enabled = NO;
    _phase7RecordApplyButton.enabled = NO;
    _phase7StateLabel.stringValue = @"Recording controlled apply event…";
    _phase7StateLabel.textColor = [NSColor systemOrangeColor];
    _phase7TextView.string = @"Appending a controlled apply lifecycle event to the selected apply entries…";

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_artifactQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->recordExecutionApplyEventPayload(
            apply.applyArtifact.artifactId,
            entryIds,
            executionStatus,
            actor,
            comment,
            failureCode,
            failureMessage);
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
                return;
            }

            auto it = std::find_if(innerSelf->_latestPhase7ExecutionApplies.begin(),
                                   innerSelf->_latestPhase7ExecutionApplies.end(),
                                   [&](const tapescope::Phase7ExecutionApplyArtifact& item) {
                                       return item.applyArtifact.artifactId == result.value.artifact.applyArtifact.artifactId;
                                   });
            std::size_t selectedIndex = 0;
            if (it != innerSelf->_latestPhase7ExecutionApplies.end()) {
                *it = result.value.artifact;
                selectedIndex = static_cast<std::size_t>(std::distance(innerSelf->_latestPhase7ExecutionApplies.begin(), it));
            } else {
                innerSelf->_latestPhase7ExecutionApplies.insert(innerSelf->_latestPhase7ExecutionApplies.begin(),
                                                                result.value.artifact);
                selectedIndex = 0;
            }
            [innerSelf->_phase7ApplyTableView reloadData];
            innerSelf->_phase7SelectionIsPlaybook = NO;
            innerSelf->_phase7SelectionIsLedger = NO;
            innerSelf->_phase7SelectionIsJournal = NO;
            innerSelf->_phase7SelectionIsApply = YES;
            [innerSelf->_phase7AnalysisTableView deselectAll:nil];
            [innerSelf->_phase7PlaybookTableView deselectAll:nil];
            [innerSelf->_phase7LedgerTableView deselectAll:nil];
            [innerSelf->_phase7JournalTableView deselectAll:nil];
            [innerSelf->_phase7ApplyTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:selectedIndex]
                                              byExtendingSelection:NO];
            [innerSelf refreshPhase7DetailText];
            NSIndexSet* updatedSelection = IndexSetForPhase7ApplyEntryIds(result.value.updatedEntryIds,
                                                                          innerSelf->_phase7VisibleApplyEntries);
            if (updatedSelection.count > 0) {
                [innerSelf->_phase7ActionTableView selectRowIndexes:updatedSelection byExtendingSelection:NO];
                innerSelf->_phase7RecordApplyButton.enabled = YES;
                innerSelf->_phase7TextView.string = ToNSString(innerSelf->_phase7DetailBody + "\n\n" +
                                                               DescribeSelectedPhase7ApplyEntries(
                                                                   innerSelf->_phase7VisibleApplyEntries,
                                                                   innerSelf->_phase7ActionTableView.selectedRowIndexes));
            }
            innerSelf->_phase7StateLabel.stringValue = ToNSString(
                std::string("Recorded `") + executionStatus + "` for " +
                std::to_string(result.value.updatedEntryIds.size()) + " apply entr" +
                (result.value.updatedEntryIds.size() == 1 ? "y." : "ies."));
            innerSelf->_phase7StateLabel.textColor = [NSColor systemGreenColor];
            [innerSelf recordRecentHistoryEntry:tapescope::json{
                {"kind", "phase7_execution_apply"},
                {"target_id", result.value.artifact.applyArtifact.artifactId},
                {"artifact_id", result.value.artifact.applyArtifact.artifactId},
                {"journal_artifact_id", result.value.artifact.journalArtifact.artifactId},
                {"ledger_artifact_id", result.value.artifact.ledgerArtifact.artifactId},
                {"playbook_artifact_id", result.value.artifact.playbookArtifact.artifactId},
                {"analysis_artifact_id", result.value.artifact.analysisArtifact.artifactId},
                {"source_artifact_id", result.value.artifact.sourceArtifact.artifactId},
                {"headline", std::string("Phase 7 execution apply")},
                {"detail", std::string("Recorded ") + executionStatus + " for " +
                               std::to_string(result.value.updatedEntryIds.size()) + " entr" +
                               (result.value.updatedEntryIds.size() == 1 ? "y" : "ies")},
                {"first_session_seq", result.value.artifact.replayContext.value("requested_window", tapescope::json::object()).value("first_session_seq", 0ULL)},
                {"last_session_seq", result.value.artifact.replayContext.value("requested_window", tapescope::json::object()).value("last_session_seq", 0ULL)}
            }];
        });
    });
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
