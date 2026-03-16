#import "tapescope_window_internal.h"

#include "tapescope_support.h"

#include <sstream>

namespace {

using tapescope::json;
using namespace tapescope_support;

} // namespace

@implementation TapeScopeWindowController (TableBindings)

- (void)openSelectedSessionReport:(id)sender {
    (void)sender;
    const NSInteger selected = _sessionReportTableView.selectedRow;
    if (selected < 0 || static_cast<std::size_t>(selected) >= _latestSessionReports.size()) {
        _reportInventoryStateLabel.stringValue = @"Select a session report row first.";
        _reportInventoryStateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    const std::string artifactId = _latestSessionReports.at(static_cast<std::size_t>(selected)).artifactId;
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
    const std::string artifactId = _latestCaseReports.at(static_cast<std::size_t>(selected)).artifactId;
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

- (void)openSelectedImportedCase:(id)sender {
    (void)sender;
    const NSInteger selected = _importedCaseTableView.selectedRow;
    if (selected < 0 || static_cast<std::size_t>(selected) >= _latestImportedCases.size()) {
        _reportInventoryStateLabel.stringValue = @"Select an imported case row first.";
        _reportInventoryStateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    const std::string artifactId = _latestImportedCases.at(static_cast<std::size_t>(selected)).artifactId;
    if (artifactId.empty()) {
        _reportInventoryStateLabel.stringValue = @"Selected imported case is missing an artifact id.";
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
    if (tableView == _recentTableView) {
        return static_cast<NSInteger>(_recentHistoryItems.size());
    }
    if (tableView == _bundleHistoryTableView) {
        return static_cast<NSInteger>(_bundleHistoryItems.size());
    }
    if (tableView == _overviewIncidentTableView) {
        return static_cast<NSInteger>(_overviewIncidents.size());
    }
    if (tableView == _overviewEvidenceTableView) {
        return static_cast<NSInteger>(_overviewPane->evidenceItems().size());
    }
    if (tableView == _rangeTableView) {
        return static_cast<NSInteger>(_rangeEvents.size());
    }
    if (tableView == _orderTableView) {
        return static_cast<NSInteger>(_orderEvents.size());
    }
    if (tableView == _orderCaseEvidenceTableView) {
        return static_cast<NSInteger>(_orderCasePane->evidenceItems().size());
    }
    if (tableView == _incidentTableView) {
        return static_cast<NSInteger>(_latestIncidents.size());
    }
    if (tableView == _incidentEvidenceTableView) {
        return static_cast<NSInteger>(_incidentPane->evidenceItems().size());
    }
    if (tableView == _findingEvidenceTableView) {
        return static_cast<NSInteger>(_findingPane->evidenceItems().size());
    }
    if (tableView == _anchorEvidenceTableView) {
        return static_cast<NSInteger>(_anchorPane->evidenceItems().size());
    }
    if (tableView == _artifactEvidenceTableView) {
        return static_cast<NSInteger>(_artifactPane->evidenceItems().size());
    }
    if (tableView == _sessionReportTableView) {
        return static_cast<NSInteger>(_latestSessionReports.size());
    }
    if (tableView == _caseReportTableView) {
        return static_cast<NSInteger>(_latestCaseReports.size());
    }
    if (tableView == _importedCaseTableView) {
        return static_cast<NSInteger>(_latestImportedCases.size());
    }
    if (tableView == _phase7AnalysisTableView) {
        return static_cast<NSInteger>(_latestPhase7AnalysisArtifacts.size());
    }
    if (tableView == _phase7PlaybookTableView) {
        return static_cast<NSInteger>(_latestPhase7PlaybookArtifacts.size());
    }
    if (tableView == _phase7LedgerTableView) {
        return static_cast<NSInteger>(_latestPhase7ExecutionLedgers.size());
    }
    if (tableView == _phase7JournalTableView) {
        return static_cast<NSInteger>(_latestPhase7ExecutionJournals.size());
    }
    if (tableView == _phase7ApplyTableView) {
        return static_cast<NSInteger>(_latestPhase7ExecutionApplies.size());
    }
    if (tableView == _phase7FindingTableView) {
        return static_cast<NSInteger>(_phase7VisibleFindings.size());
    }
    if (tableView == _phase7ActionTableView) {
        if (_phase7SelectionIsApply) {
            return static_cast<NSInteger>(_phase7VisibleApplyEntries.size());
        }
        if (_phase7SelectionIsJournal) {
            return static_cast<NSInteger>(_phase7VisibleJournalEntries.size());
        }
        return static_cast<NSInteger>(_phase7SelectionIsLedger ? _phase7VisibleLedgerEntries.size()
                                                               : _phase7VisibleActions.size());
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
    const tapescope::EventRow* eventItem = nullptr;
    const tapescope::IncidentListRow* incidentItem = nullptr;
    const tapescope::ReportInventoryRow* reportItem = nullptr;
    const tapescope::ImportedCaseRow* importedCaseItem = nullptr;
    const tapescope::Phase7AnalysisArtifact* analysisArtifactItem = nullptr;
    const tapescope::Phase7PlaybookArtifact* playbookArtifactItem = nullptr;
    const tapescope::Phase7ExecutionLedgerArtifact* executionLedgerArtifactItem = nullptr;
    const tapescope::Phase7ExecutionJournalArtifact* executionJournalArtifactItem = nullptr;
    const tapescope::Phase7ExecutionApplyArtifact* executionApplyArtifactItem = nullptr;
    const tapescope::Phase7FindingRecord* phase7FindingItem = nullptr;
    const tapescope::Phase7PlaybookAction* phase7ActionItem = nullptr;
    const tapescope::Phase7ExecutionLedgerEntry* phase7LedgerEntryItem = nullptr;
    const tapescope::Phase7ExecutionJournalEntry* phase7JournalEntryItem = nullptr;
    const tapescope::Phase7ExecutionApplyEntry* phase7ApplyEntryItem = nullptr;
    if (tableView == _liveTableView) {
        if (static_cast<std::size_t>(row) >= _liveEvents.size()) {
            return nil;
        }
        eventItem = &_liveEvents.at(static_cast<std::size_t>(row));
    } else if (tableView == _recentTableView) {
        if (static_cast<std::size_t>(row) >= _recentHistoryItems.size()) {
            return nil;
        }
        item = &_recentHistoryItems.at(static_cast<std::size_t>(row));
    } else if (tableView == _bundleHistoryTableView) {
        if (static_cast<std::size_t>(row) >= _bundleHistoryItems.size()) {
            return nil;
        }
        item = &_bundleHistoryItems.at(static_cast<std::size_t>(row));
    } else if (tableView == _overviewIncidentTableView) {
        if (static_cast<std::size_t>(row) >= _overviewIncidents.size()) {
            return nil;
        }
        incidentItem = &_overviewIncidents.at(static_cast<std::size_t>(row));
    } else if (tableView == _overviewEvidenceTableView) {
        if (static_cast<std::size_t>(row) >= _overviewPane->evidenceItems().size()) {
            return nil;
        }
        item = &_overviewPane->evidenceItems().at(static_cast<std::size_t>(row)).raw;
    } else if (tableView == _rangeTableView) {
        if (static_cast<std::size_t>(row) >= _rangeEvents.size()) {
            return nil;
        }
        eventItem = &_rangeEvents.at(static_cast<std::size_t>(row));
    } else if (tableView == _orderTableView) {
        if (static_cast<std::size_t>(row) >= _orderEvents.size()) {
            return nil;
        }
        eventItem = &_orderEvents.at(static_cast<std::size_t>(row));
    } else if (tableView == _orderCaseEvidenceTableView) {
        if (static_cast<std::size_t>(row) >= _orderCasePane->evidenceItems().size()) {
            return nil;
        }
        item = &_orderCasePane->evidenceItems().at(static_cast<std::size_t>(row)).raw;
    } else if (tableView == _incidentTableView) {
        if (static_cast<std::size_t>(row) >= _latestIncidents.size()) {
            return nil;
        }
        incidentItem = &_latestIncidents.at(static_cast<std::size_t>(row));
    } else if (tableView == _incidentEvidenceTableView) {
        if (static_cast<std::size_t>(row) >= _incidentPane->evidenceItems().size()) {
            return nil;
        }
        item = &_incidentPane->evidenceItems().at(static_cast<std::size_t>(row)).raw;
    } else if (tableView == _findingEvidenceTableView) {
        if (static_cast<std::size_t>(row) >= _findingPane->evidenceItems().size()) {
            return nil;
        }
        item = &_findingPane->evidenceItems().at(static_cast<std::size_t>(row)).raw;
    } else if (tableView == _anchorEvidenceTableView) {
        if (static_cast<std::size_t>(row) >= _anchorPane->evidenceItems().size()) {
            return nil;
        }
        item = &_anchorPane->evidenceItems().at(static_cast<std::size_t>(row)).raw;
    } else if (tableView == _artifactEvidenceTableView) {
        if (static_cast<std::size_t>(row) >= _artifactPane->evidenceItems().size()) {
            return nil;
        }
        item = &_artifactPane->evidenceItems().at(static_cast<std::size_t>(row)).raw;
    } else if (tableView == _sessionReportTableView) {
        if (static_cast<std::size_t>(row) >= _latestSessionReports.size()) {
            return nil;
        }
        reportItem = &_latestSessionReports.at(static_cast<std::size_t>(row));
    } else if (tableView == _caseReportTableView) {
        if (static_cast<std::size_t>(row) >= _latestCaseReports.size()) {
            return nil;
        }
        reportItem = &_latestCaseReports.at(static_cast<std::size_t>(row));
    } else if (tableView == _importedCaseTableView) {
        if (static_cast<std::size_t>(row) >= _latestImportedCases.size()) {
            return nil;
        }
        importedCaseItem = &_latestImportedCases.at(static_cast<std::size_t>(row));
    } else if (tableView == _phase7AnalysisTableView) {
        if (static_cast<std::size_t>(row) >= _latestPhase7AnalysisArtifacts.size()) {
            return nil;
        }
        analysisArtifactItem = &_latestPhase7AnalysisArtifacts.at(static_cast<std::size_t>(row));
    } else if (tableView == _phase7PlaybookTableView) {
        if (static_cast<std::size_t>(row) >= _latestPhase7PlaybookArtifacts.size()) {
            return nil;
        }
        playbookArtifactItem = &_latestPhase7PlaybookArtifacts.at(static_cast<std::size_t>(row));
    } else if (tableView == _phase7LedgerTableView) {
        if (static_cast<std::size_t>(row) >= _latestPhase7ExecutionLedgers.size()) {
            return nil;
        }
        executionLedgerArtifactItem = &_latestPhase7ExecutionLedgers.at(static_cast<std::size_t>(row));
    } else if (tableView == _phase7JournalTableView) {
        if (static_cast<std::size_t>(row) >= _latestPhase7ExecutionJournals.size()) {
            return nil;
        }
        executionJournalArtifactItem = &_latestPhase7ExecutionJournals.at(static_cast<std::size_t>(row));
    } else if (tableView == _phase7ApplyTableView) {
        if (static_cast<std::size_t>(row) >= _latestPhase7ExecutionApplies.size()) {
            return nil;
        }
        executionApplyArtifactItem = &_latestPhase7ExecutionApplies.at(static_cast<std::size_t>(row));
    } else if (tableView == _phase7FindingTableView) {
        if (static_cast<std::size_t>(row) >= _phase7VisibleFindings.size()) {
            return nil;
        }
        phase7FindingItem = &_phase7VisibleFindings.at(static_cast<std::size_t>(row));
    } else if (tableView == _phase7ActionTableView) {
        if (_phase7SelectionIsApply) {
            if (static_cast<std::size_t>(row) >= _phase7VisibleApplyEntries.size()) {
                return nil;
            }
            phase7ApplyEntryItem = &_phase7VisibleApplyEntries.at(static_cast<std::size_t>(row));
        } else if (_phase7SelectionIsJournal) {
            if (static_cast<std::size_t>(row) >= _phase7VisibleJournalEntries.size()) {
                return nil;
            }
            phase7JournalEntryItem = &_phase7VisibleJournalEntries.at(static_cast<std::size_t>(row));
        } else if (_phase7SelectionIsLedger) {
            if (static_cast<std::size_t>(row) >= _phase7VisibleLedgerEntries.size()) {
                return nil;
            }
            phase7LedgerEntryItem = &_phase7VisibleLedgerEntries.at(static_cast<std::size_t>(row));
        } else {
            if (static_cast<std::size_t>(row) >= _phase7VisibleActions.size()) {
                return nil;
            }
            phase7ActionItem = &_phase7VisibleActions.at(static_cast<std::size_t>(row));
        }
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
            value = std::to_string(eventItem->sessionSeq);
        } else if ([columnId isEqualToString:@"source_seq"]) {
            value = std::to_string(eventItem->sourceSeq);
        } else if ([columnId isEqualToString:@"event_kind"]) {
            value = eventItem->eventKind;
        } else {
            value = EventSummaryText(*eventItem);
        }
    } else if (tableView == _recentTableView) {
        if ([columnId isEqualToString:@"kind"]) {
            value = item->value("kind", std::string());
        } else if ([columnId isEqualToString:@"target_id"]) {
            value = item->value("target_id", std::string());
        } else {
            value = item->value("headline", item->value("detail", std::string()));
        }
    } else if (tableView == _bundleHistoryTableView) {
        if ([columnId isEqualToString:@"kind"]) {
            value = item->value("kind", std::string());
        } else if ([columnId isEqualToString:@"bundle_id"]) {
            value = item->value("bundle_id", item->value("target_id", std::string()));
        } else {
            value = item->value("headline", item->value("detail", std::string()));
        }
    } else if (tableView == _overviewIncidentTableView) {
        if ([columnId isEqualToString:@"logical_incident_id"]) {
            value = std::to_string(incidentItem->logicalIncidentId);
        } else if ([columnId isEqualToString:@"kind"]) {
            value = incidentItem->kind;
        } else if ([columnId isEqualToString:@"score"]) {
            std::ostringstream out;
            out.setf(std::ios::fixed);
            out.precision(2);
            out << incidentItem->score;
            value = out.str();
        } else {
            value = incidentItem->title;
        }
    } else if (tableView == _rangeTableView) {
        if ([columnId isEqualToString:@"session_seq"]) {
            value = std::to_string(eventItem->sessionSeq);
        } else if ([columnId isEqualToString:@"source_seq"]) {
            value = std::to_string(eventItem->sourceSeq);
        } else if ([columnId isEqualToString:@"event_kind"]) {
            value = eventItem->eventKind;
        } else {
            value = EventSummaryText(*eventItem);
        }
    } else if (tableView == _orderTableView) {
        if ([columnId isEqualToString:@"session_seq"]) {
            value = std::to_string(eventItem->sessionSeq);
        } else if ([columnId isEqualToString:@"event_kind"]) {
            value = eventItem->eventKind;
        } else if ([columnId isEqualToString:@"side"]) {
            value = eventItem->side;
        } else {
            value = EventSummaryText(*eventItem);
        }
    } else if (tableView == _orderCaseEvidenceTableView || tableView == _artifactEvidenceTableView) {
        if ([columnId isEqualToString:@"kind"]) {
            value = item->value("kind", item->value("type", std::string()));
        } else if ([columnId isEqualToString:@"artifact_id"]) {
            value = item->value("artifact_id", std::string());
        } else {
            value = item->value("label", item->value("headline", item->value("artifact_id", std::string())));
        }
    } else if (tableView == _overviewEvidenceTableView || tableView == _incidentEvidenceTableView ||
               tableView == _findingEvidenceTableView || tableView == _anchorEvidenceTableView) {
        if ([columnId isEqualToString:@"kind"]) {
            value = item->value("kind", item->value("type", std::string()));
        } else if ([columnId isEqualToString:@"artifact_id"]) {
            value = item->value("artifact_id", std::string());
        } else {
            value = item->value("label", item->value("headline", item->value("artifact_id", std::string())));
        }
    } else if (tableView == _incidentTableView) {
        if ([columnId isEqualToString:@"logical_incident_id"]) {
            value = std::to_string(incidentItem->logicalIncidentId);
        } else if ([columnId isEqualToString:@"kind"]) {
            value = incidentItem->kind;
        } else if ([columnId isEqualToString:@"score"]) {
            std::ostringstream out;
            out.setf(std::ios::fixed);
            out.precision(2);
            out << incidentItem->score;
            value = out.str();
        } else {
            value = incidentItem->title;
        }
    } else if (tableView == _sessionReportTableView) {
        if ([columnId isEqualToString:@"report_id"]) {
            value = std::to_string(reportItem->reportId);
        } else if ([columnId isEqualToString:@"revision_id"]) {
            value = std::to_string(reportItem->revisionId);
        } else if ([columnId isEqualToString:@"artifact_id"]) {
            value = reportItem->artifactId;
        } else {
            value = reportItem->headline;
        }
    } else if (tableView == _caseReportTableView) {
        if ([columnId isEqualToString:@"report_id"]) {
            value = std::to_string(reportItem->reportId);
        } else if ([columnId isEqualToString:@"report_type"]) {
            value = reportItem->reportType;
        } else if ([columnId isEqualToString:@"artifact_id"]) {
            value = reportItem->artifactId;
        } else {
            value = reportItem->headline;
        }
    } else if (tableView == _importedCaseTableView) {
        if ([columnId isEqualToString:@"imported_case_id"]) {
            value = std::to_string(importedCaseItem->importedCaseId);
        } else if ([columnId isEqualToString:@"artifact_id"]) {
            value = importedCaseItem->artifactId;
        } else if ([columnId isEqualToString:@"source_revision_id"]) {
            value = std::to_string(importedCaseItem->sourceRevisionId);
        } else {
            value = importedCaseItem->headline;
        }
    } else if (tableView == _phase7AnalysisTableView) {
        if ([columnId isEqualToString:@"artifact_id"]) {
            value = analysisArtifactItem->analysisArtifact.artifactId;
        } else if ([columnId isEqualToString:@"analysis_profile"]) {
            value = analysisArtifactItem->analysisProfile;
        } else if ([columnId isEqualToString:@"source_artifact_id"]) {
            value = analysisArtifactItem->sourceArtifact.artifactId;
        } else {
            value = std::to_string(analysisArtifactItem->findings.size());
        }
    } else if (tableView == _phase7PlaybookTableView) {
        if ([columnId isEqualToString:@"artifact_id"]) {
            value = playbookArtifactItem->playbookArtifact.artifactId;
        } else if ([columnId isEqualToString:@"analysis_artifact_id"]) {
            value = playbookArtifactItem->analysisArtifact.artifactId;
        } else if ([columnId isEqualToString:@"mode"]) {
            value = playbookArtifactItem->mode;
        } else {
            value = std::to_string(playbookArtifactItem->plannedActions.size());
        }
    } else if (tableView == _phase7LedgerTableView) {
        const auto reviewSummary = tape_phase7::summarizeExecutionLedgerReviewSummary(*executionLedgerArtifactItem);
        const auto latestAudit = tape_phase7::latestExecutionLedgerAuditSummary(*executionLedgerArtifactItem);
        if ([columnId isEqualToString:@"artifact_id"]) {
            value = executionLedgerArtifactItem->ledgerArtifact.artifactId;
        } else if ([columnId isEqualToString:@"playbook_artifact_id"]) {
            value = executionLedgerArtifactItem->playbookArtifact.artifactId;
        } else if ([columnId isEqualToString:@"ledger_status"]) {
            value = executionLedgerArtifactItem->ledgerStatus;
        } else if ([columnId isEqualToString:@"pending_review_count"]) {
            value = std::to_string(reviewSummary.pendingReviewCount);
        } else if ([columnId isEqualToString:@"waiting_approval_count"]) {
            value = std::to_string(reviewSummary.waitingApprovalCount);
        } else if ([columnId isEqualToString:@"ready_entry_count"]) {
            value = std::to_string(reviewSummary.readyEntryCount);
        } else if ([columnId isEqualToString:@"blocked_count"]) {
            value = std::to_string(reviewSummary.blockedCount);
        } else if ([columnId isEqualToString:@"distinct_reviewer_count"]) {
            value = std::to_string(reviewSummary.distinctReviewerCount);
        } else if ([columnId isEqualToString:@"reviewed_count"]) {
            value = std::to_string(reviewSummary.reviewedCount);
        } else if ([columnId isEqualToString:@"latest_audit_event"]) {
            if (latestAudit.is_object() && latestAudit.contains("message") && latestAudit.at("message").is_string()) {
                value = latestAudit.at("message").get<std::string>();
            }
        } else {
            value = std::to_string(executionLedgerArtifactItem->entries.size());
        }
    } else if (tableView == _phase7JournalTableView) {
        const auto executionSummary = tape_phase7::summarizeExecutionJournalSummary(*executionJournalArtifactItem);
        const auto latestAudit = tape_phase7::latestExecutionJournalAuditSummary(*executionJournalArtifactItem);
        if ([columnId isEqualToString:@"artifact_id"]) {
            value = executionJournalArtifactItem->journalArtifact.artifactId;
        } else if ([columnId isEqualToString:@"ledger_artifact_id"]) {
            value = executionJournalArtifactItem->ledgerArtifact.artifactId;
        } else if ([columnId isEqualToString:@"journal_status"]) {
            value = executionJournalArtifactItem->journalStatus;
        } else if ([columnId isEqualToString:@"queued_count"]) {
            value = std::to_string(executionSummary.queuedCount);
        } else if ([columnId isEqualToString:@"submitted_count"]) {
            value = std::to_string(executionSummary.submittedCount);
        } else if ([columnId isEqualToString:@"terminal_count"]) {
            value = std::to_string(executionSummary.terminalCount);
        } else if ([columnId isEqualToString:@"latest_audit_event"]) {
            if (latestAudit.is_object() && latestAudit.contains("message") && latestAudit.at("message").is_string()) {
                value = latestAudit.at("message").get<std::string>();
            }
        } else {
            value = std::to_string(executionJournalArtifactItem->entries.size());
        }
    } else if (tableView == _phase7ApplyTableView) {
        const auto applySummary = tape_phase7::summarizeExecutionApplySummary(*executionApplyArtifactItem);
        const auto latestAudit = tape_phase7::latestExecutionApplyAuditSummary(*executionApplyArtifactItem);
        if ([columnId isEqualToString:@"artifact_id"]) {
            value = executionApplyArtifactItem->applyArtifact.artifactId;
        } else if ([columnId isEqualToString:@"journal_artifact_id"]) {
            value = executionApplyArtifactItem->journalArtifact.artifactId;
        } else if ([columnId isEqualToString:@"apply_status"]) {
            value = executionApplyArtifactItem->applyStatus;
        } else if ([columnId isEqualToString:@"submitted_count"]) {
            value = std::to_string(applySummary.submittedCount);
        } else if ([columnId isEqualToString:@"terminal_count"]) {
            value = std::to_string(applySummary.terminalCount);
        } else if ([columnId isEqualToString:@"latest_audit_event"]) {
            if (latestAudit.is_object() && latestAudit.contains("message") && latestAudit.at("message").is_string()) {
                value = latestAudit.at("message").get<std::string>();
            }
        } else {
            value = std::to_string(executionApplyArtifactItem->entries.size());
        }
    } else if (tableView == _phase7FindingTableView) {
        if ([columnId isEqualToString:@"finding_id"]) {
            value = phase7FindingItem->findingId;
        } else if ([columnId isEqualToString:@"severity"]) {
            value = phase7FindingItem->severity;
        } else if ([columnId isEqualToString:@"category"]) {
            value = phase7FindingItem->category;
        } else {
            value = phase7FindingItem->summary;
        }
    } else if (tableView == _phase7ActionTableView) {
        if (_phase7SelectionIsApply) {
            if ([columnId isEqualToString:@"action_id"]) {
                value = phase7ApplyEntryItem->applyEntryId;
            } else if ([columnId isEqualToString:@"action_type"]) {
                value = phase7ApplyEntryItem->actionType;
            } else if ([columnId isEqualToString:@"finding_id"]) {
                value = phase7ApplyEntryItem->findingId;
            } else if ([columnId isEqualToString:@"review_status"]) {
                value = phase7ApplyEntryItem->executionStatus;
            } else {
                value = phase7ApplyEntryItem->title;
            }
        } else if (_phase7SelectionIsJournal) {
            if ([columnId isEqualToString:@"action_id"]) {
                value = phase7JournalEntryItem->journalEntryId;
            } else if ([columnId isEqualToString:@"action_type"]) {
                value = phase7JournalEntryItem->actionType;
            } else if ([columnId isEqualToString:@"finding_id"]) {
                value = phase7JournalEntryItem->findingId;
            } else if ([columnId isEqualToString:@"review_status"]) {
                value = phase7JournalEntryItem->executionStatus;
            } else {
                value = phase7JournalEntryItem->title;
            }
        } else if (_phase7SelectionIsLedger) {
            if ([columnId isEqualToString:@"action_id"]) {
                value = phase7LedgerEntryItem->actionId;
            } else if ([columnId isEqualToString:@"action_type"]) {
                value = phase7LedgerEntryItem->actionType;
            } else if ([columnId isEqualToString:@"finding_id"]) {
                value = phase7LedgerEntryItem->findingId;
            } else if ([columnId isEqualToString:@"review_status"]) {
                value = phase7LedgerEntryItem->reviewStatus;
            } else {
                value = phase7LedgerEntryItem->title;
            }
        } else {
            if ([columnId isEqualToString:@"action_id"]) {
                value = phase7ActionItem->actionId;
            } else if ([columnId isEqualToString:@"action_type"]) {
                value = phase7ActionItem->actionType;
            } else if ([columnId isEqualToString:@"finding_id"]) {
                value = phase7ActionItem->findingId;
            } else if ([columnId isEqualToString:@"review_status"]) {
                value = {};
            } else {
                value = phase7ActionItem->title;
            }
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
        const auto& item = _liveEvents.at(static_cast<std::size_t>(selected));
        _liveTextView.string = ToNSString(item.raw.dump(2));
        return;
    }

    if (tableView == _recentTableView) {
        const NSInteger selected = _recentTableView.selectedRow;
        _recentOpenButton.enabled = (selected >= 0);
        if (selected < 0 || static_cast<std::size_t>(selected) >= _recentHistoryItems.size()) {
            _recentTextView.string = @"Select a recent-history row to inspect its summary and reopen it.";
            return;
        }
        const auto& item = _recentHistoryItems.at(static_cast<std::size_t>(selected));
        _recentTextView.string = ToNSString(DescribeRecentHistoryEntry(item));
        return;
    }

    if (tableView == _bundleHistoryTableView) {
        const NSInteger selected = _bundleHistoryTableView.selectedRow;
        if (selected < 0 || static_cast<std::size_t>(selected) >= _bundleHistoryItems.size()) {
            _bundleHistoryOpenButton.enabled = NO;
            _bundleHistoryOpenSourceButton.enabled = NO;
            _bundleHistoryRevealButton.enabled = NO;
            _bundleHistoryLoadRangeButton.enabled = NO;
            _bundleHistoryTextView.string = @"Select a bundle-history row to inspect its metadata and reopen it.";
            return;
        }
        const auto& item = _bundleHistoryItems.at(static_cast<std::size_t>(selected));
        _bundleHistoryOpenButton.enabled = true;
        _bundleHistoryOpenSourceButton.enabled = !item.value("source_artifact_id", std::string()).empty();
        _bundleHistoryRevealButton.enabled = !item.value("bundle_path", std::string()).empty();
        const std::uint64_t firstSessionSeq = item.value("first_session_seq", 0ULL);
        const std::uint64_t lastSessionSeq = item.value("last_session_seq", 0ULL);
        _bundleHistoryLoadRangeButton.enabled = (firstSessionSeq > 0 && lastSessionSeq >= firstSessionSeq);
        _bundleHistoryTextView.string = ToNSString(DescribeRecentHistoryEntry(item));
        const std::string bundlePath = item.value("bundle_path", std::string());
        if (!bundlePath.empty()) {
            _bundleImportPathField.stringValue = ToNSString(bundlePath);
            [self previewBundlePath:nil];
        }
        return;
    }

    if (tableView == _overviewIncidentTableView) {
        const NSInteger selected = _overviewIncidentTableView.selectedRow;
        _overviewOpenSelectedIncidentButton.enabled = (selected >= 0);
        if (selected < 0 || static_cast<std::size_t>(selected) >= _overviewIncidents.size()) {
            return;
        }
        const auto& item = _overviewIncidents.at(static_cast<std::size_t>(selected));
        _overviewTextView.string = ToNSString(item.raw.dump(2));
        return;
    }

    if (tableView == _rangeTableView) {
        const NSInteger selected = _rangeTableView.selectedRow;
        if (selected < 0 || static_cast<std::size_t>(selected) >= _rangeEvents.size()) {
            _rangeTextView.string = @"Select a replay event row to inspect the decoded payload.";
            return;
        }
        const auto& item = _rangeEvents.at(static_cast<std::size_t>(selected));
        _rangeTextView.string = ToNSString(item.raw.dump(2));
        return;
    }

    if (tableView == _orderTableView) {
        const NSInteger selected = _orderTableView.selectedRow;
        if (selected < 0 || static_cast<std::size_t>(selected) >= _orderEvents.size()) {
            _orderTextView.string = @"Select an anchored event row to inspect the decoded payload.";
            return;
        }
        const auto& item = _orderEvents.at(static_cast<std::size_t>(selected));
        _orderTextView.string = ToNSString(item.raw.dump(2));
        return;
    }

    if (tableView == _incidentTableView) {
        const NSInteger selected = _incidentTableView.selectedRow;
        _incidentOpenSelectedButton.enabled = (selected >= 0);
        if (selected < 0 || static_cast<std::size_t>(selected) >= _latestIncidents.size()) {
            _incidentTextView.string = @"Select an incident row to inspect its summary or open the full drilldown.";
            return;
        }
        const auto& item = _latestIncidents.at(static_cast<std::size_t>(selected));
        _incidentIdField.stringValue = UInt64String(item.logicalIncidentId);
        _incidentTextView.string = ToNSString(item.raw.dump(2));
        return;
    }

    if (tapescope::InvestigationPaneController* pane = [self paneControllerForEvidenceTable:tableView]) {
        [self renderSelectionForPane:pane];
        return;
    }

    if (tableView == _sessionReportTableView) {
        const NSInteger selected = _sessionReportTableView.selectedRow;
        _reportInventoryOpenSessionButton.enabled = (selected >= 0);
        _reportInventoryExportSessionBundleButton.enabled = (selected >= 0);
        [self refreshReportInventoryDetailText];
        return;
    }

    if (tableView == _caseReportTableView) {
        const NSInteger selected = _caseReportTableView.selectedRow;
        _reportInventoryOpenCaseButton.enabled = (selected >= 0);
        _reportInventoryExportCaseBundleButton.enabled = (selected >= 0);
        [self refreshReportInventoryDetailText];
        return;
    }

    if (tableView == _importedCaseTableView) {
        const NSInteger selected = _importedCaseTableView.selectedRow;
        _reportInventoryOpenImportedButton.enabled = (selected >= 0);
        _reportInventoryLoadImportedRangeButton.enabled = (selected >= 0);
        _reportInventoryOpenImportedSourceButton.enabled = (selected >= 0);
        [self refreshReportInventoryDetailText];
        return;
    }

    if (tableView == _phase7AnalysisTableView) {
        const NSInteger selected = _phase7AnalysisTableView.selectedRow;
        _phase7SelectionIsPlaybook = NO;
        _phase7SelectionIsLedger = NO;
        _phase7SelectionIsJournal = NO;
        _phase7SelectionIsApply = NO;
        if (selected >= 0) {
            [_phase7PlaybookTableView deselectAll:nil];
            [_phase7LedgerTableView deselectAll:nil];
            [_phase7JournalTableView deselectAll:nil];
            [_phase7ApplyTableView deselectAll:nil];
        }
        [self refreshPhase7DetailText];
        return;
    }

    if (tableView == _phase7PlaybookTableView) {
        const NSInteger selected = _phase7PlaybookTableView.selectedRow;
        _phase7SelectionIsPlaybook = (selected >= 0);
        _phase7SelectionIsLedger = NO;
        _phase7SelectionIsJournal = NO;
        _phase7SelectionIsApply = NO;
        if (selected >= 0) {
            [_phase7AnalysisTableView deselectAll:nil];
            [_phase7LedgerTableView deselectAll:nil];
            [_phase7JournalTableView deselectAll:nil];
            [_phase7ApplyTableView deselectAll:nil];
        }
        [self refreshPhase7DetailText];
        return;
    }

    if (tableView == _phase7LedgerTableView) {
        const NSInteger selected = _phase7LedgerTableView.selectedRow;
        _phase7SelectionIsPlaybook = NO;
        _phase7SelectionIsLedger = (selected >= 0);
        _phase7SelectionIsJournal = NO;
        _phase7SelectionIsApply = NO;
        if (selected >= 0) {
            [_phase7AnalysisTableView deselectAll:nil];
            [_phase7PlaybookTableView deselectAll:nil];
            [_phase7JournalTableView deselectAll:nil];
            [_phase7ApplyTableView deselectAll:nil];
        }
        [self refreshPhase7DetailText];
        return;
    }

    if (tableView == _phase7JournalTableView) {
        const NSInteger selected = _phase7JournalTableView.selectedRow;
        _phase7SelectionIsPlaybook = NO;
        _phase7SelectionIsLedger = NO;
        _phase7SelectionIsJournal = (selected >= 0);
        _phase7SelectionIsApply = NO;
        if (selected >= 0) {
            [_phase7AnalysisTableView deselectAll:nil];
            [_phase7PlaybookTableView deselectAll:nil];
            [_phase7LedgerTableView deselectAll:nil];
            [_phase7ApplyTableView deselectAll:nil];
        }
        [self refreshPhase7DetailText];
        return;
    }

    if (tableView == _phase7ApplyTableView) {
        const NSInteger selected = _phase7ApplyTableView.selectedRow;
        _phase7SelectionIsPlaybook = NO;
        _phase7SelectionIsLedger = NO;
        _phase7SelectionIsJournal = NO;
        _phase7SelectionIsApply = (selected >= 0);
        if (selected >= 0) {
            [_phase7AnalysisTableView deselectAll:nil];
            [_phase7PlaybookTableView deselectAll:nil];
            [_phase7LedgerTableView deselectAll:nil];
            [_phase7JournalTableView deselectAll:nil];
        }
        [self refreshPhase7DetailText];
        return;
    }

    if (tableView == _phase7FindingTableView || tableView == _phase7ActionTableView) {
        [self refreshPhase7DetailText];
    }
}

@end
