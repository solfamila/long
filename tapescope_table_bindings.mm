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
    }
}

@end
