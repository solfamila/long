#import "tapescope_window_internal.h"

#include "tapescope_support.h"

namespace {

using namespace tapescope_support;

} // namespace

@implementation TapeScopeWindowController (ArtifactQueries)

- (void)fetchIncident:(id)sender {
    (void)sender;
    if (_incidentInFlight || !_client) {
        return;
    }

    std::uint64_t logicalIncidentId = 0;
    if (!ParsePositiveUInt64(ToStdString(_incidentIdField.stringValue), &logicalIncidentId)) {
        _incidentStateLabel.stringValue = @"logical_incident_id must be a positive integer.";
        _incidentStateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    _incidentInFlight = YES;
    _incidentFetchButton.enabled = NO;
    _incidentRefreshButton.enabled = NO;
    _incidentOpenSelectedButton.enabled = NO;
    const std::uint64_t token = _incidentPane->beginRequest(@"Reading incident drilldown…");

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_interactiveQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->readIncidentPayload(logicalIncidentId);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil || !innerSelf->_incidentPane->isCurrent(token)) {
                return;
            }
            innerSelf->_incidentInFlight = NO;
            innerSelf->_incidentFetchButton.enabled = YES;
            innerSelf->_incidentRefreshButton.enabled = YES;
            innerSelf->_incidentOpenSelectedButton.enabled = (innerSelf->_incidentTableView.selectedRow >= 0);
            [innerSelf applyInvestigationResult:result
                                 paneController:innerSelf->_incidentPane.get()
                                    successText:@"Incident loaded."
                               syncArtifactField:YES];
            if (result.ok()) {
                [innerSelf recordRecentHistoryForKind:"incident"
                                             targetId:std::to_string(logicalIncidentId)
                                              payload:result.value
                                        fallbackTitle:"Incident " + std::to_string(logicalIncidentId)
                                       fallbackDetail:"Reopen the incident drilldown."
                                             metadata:tapescope::json{{"logical_incident_id", logicalIncidentId}}];
            }
            innerSelf->_incidentTextView.string =
                ToNSString(DescribeInvestigationPayload("incident", "logical_incident_id=" + std::to_string(logicalIncidentId), result));
        });
    });
}

- (void)loadReplayWindowFromIncident:(id)sender {
    (void)sender;
    [self loadReplayWindowForPane:_incidentPane.get()];
}

- (void)openSelectedIncidentEvidence:(id)sender {
    (void)sender;
    [self openSelectedEvidenceForPane:_incidentPane.get()];
}

- (void)openSelectedOverviewIncident:(id)sender {
    (void)sender;
    const NSInteger selected = _overviewIncidentTableView.selectedRow;
    if (selected < 0 || static_cast<std::size_t>(selected) >= _overviewIncidents.size()) {
        _overviewStateLabel.stringValue = @"Select an overview incident row first.";
        _overviewStateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    const auto& row = _overviewIncidents.at(static_cast<std::size_t>(selected));
    const std::uint64_t logicalIncidentId = row.logicalIncidentId;
    if (logicalIncidentId == 0) {
        _overviewStateLabel.stringValue = @"Selected overview incident is missing logical_incident_id.";
        _overviewStateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    _incidentIdField.stringValue = UInt64String(logicalIncidentId);
    if (_tabView != nil) {
        [_tabView selectTabViewItemWithIdentifier:@"IncidentPane"];
    }
    [self fetchIncident:nil];
}

- (void)refreshIncidentList:(id)sender {
    (void)sender;
    if (_incidentInFlight || !_client) {
        return;
    }

    _incidentInFlight = YES;
    _incidentRefreshButton.enabled = NO;
    _incidentFetchButton.enabled = NO;
    _incidentOpenSelectedButton.enabled = NO;
    _incidentStateLabel.stringValue = @"Refreshing incident list…";
    _incidentStateLabel.textColor = [NSColor systemOrangeColor];
    _incidentTextView.string = @"Refreshing ranked incidents…";

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_pollQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->listIncidentsPayload(40);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil) {
                return;
            }
            innerSelf->_incidentInFlight = NO;
            innerSelf->_incidentRefreshButton.enabled = YES;
            innerSelf->_incidentFetchButton.enabled = YES;
            innerSelf->_latestIncidents.clear();
            if (result.ok()) {
                innerSelf->_latestIncidents = result.value.incidents;
                if (!innerSelf->_latestIncidents.empty()) {
                    innerSelf->_incidentStateLabel.stringValue = @"Incident list loaded.";
                    innerSelf->_incidentStateLabel.textColor = [NSColor systemGreenColor];
                } else {
                    innerSelf->_incidentStateLabel.stringValue = @"No ranked incidents are available yet.";
                    innerSelf->_incidentStateLabel.textColor = TapeInkMutedColor();
                }
            } else {
                innerSelf->_incidentStateLabel.stringValue = ToNSString(tapescope::QueryClient::describeError(result.error));
                innerSelf->_incidentStateLabel.textColor = ErrorColorForKind(result.error.kind);
            }
            [innerSelf->_incidentTableView reloadData];
            if (!innerSelf->_latestIncidents.empty()) {
                [innerSelf->_incidentTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0] byExtendingSelection:NO];
            } else {
                innerSelf->_incidentTextView.string = ToNSString(DescribeIncidentListResult(result));
            }
        });
    });
}

- (void)openSelectedIncident:(id)sender {
    (void)sender;
    const NSInteger selected = _incidentTableView.selectedRow;
    if (selected < 0 || static_cast<std::size_t>(selected) >= _latestIncidents.size()) {
        _incidentStateLabel.stringValue = @"Select an incident row first.";
        _incidentStateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    const auto& row = _latestIncidents.at(static_cast<std::size_t>(selected));
    const std::uint64_t logicalIncidentId = row.logicalIncidentId;
    if (logicalIncidentId == 0) {
        _incidentStateLabel.stringValue = @"Selected incident row is missing logical_incident_id.";
        _incidentStateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    _incidentIdField.stringValue = UInt64String(logicalIncidentId);
    [self fetchIncident:nil];
}

- (void)fetchArtifact:(id)sender {
    (void)sender;
    if (_artifactInFlight || !_client) {
        return;
    }

    const std::string artifactId = TrimAscii(ToStdString(_artifactIdField.stringValue));
    if (artifactId.empty()) {
        _artifactStateLabel.stringValue = @"artifact_id is required.";
        _artifactStateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    _artifactInFlight = YES;
    _artifactFetchButton.enabled = NO;
    _artifactExportButton.enabled = NO;
    const std::uint64_t token = _artifactPane->beginRequest(@"Reading artifact…");

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_artifactQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->readArtifactPayload(artifactId);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil || !innerSelf->_artifactPane->isCurrent(token)) {
                return;
            }
            innerSelf->_artifactInFlight = NO;
            innerSelf->_artifactFetchButton.enabled = YES;
            innerSelf->_artifactExportButton.enabled = YES;
            [innerSelf applyInvestigationResult:result
                                 paneController:innerSelf->_artifactPane.get()
                                    successText:@"Artifact loaded."
                               syncArtifactField:NO];
            if (result.ok()) {
                [innerSelf recordRecentHistoryForKind:"artifact"
                                             targetId:artifactId
                                              payload:result.value
                                        fallbackTitle:"Artifact " + artifactId
                                       fallbackDetail:"Reopen the durable artifact envelope."
                                             metadata:tapescope::json{{"artifact_id", artifactId}}];
            }
            innerSelf->_artifactTextView.string =
                ToNSString(DescribeInvestigationPayload("artifact_read", artifactId, result));
        });
    });
}

- (void)exportArtifactPreview:(id)sender {
    (void)sender;
    if (_artifactInFlight || !_client) {
        return;
    }

    const std::string artifactId = TrimAscii(ToStdString(_artifactIdField.stringValue));
    if (artifactId.empty()) {
        _artifactStateLabel.stringValue = @"artifact_id is required.";
        _artifactStateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    const std::string format = ToStdString(_artifactExportFormatPopup.titleOfSelectedItem);

    _artifactInFlight = YES;
    const std::uint64_t token = [self issueRequestToken:&_artifactExportRequestToken];
    _artifactFetchButton.enabled = NO;
    _artifactExportButton.enabled = NO;
    _artifactOpenSelectedEvidenceButton.enabled = NO;
    _artifactStateLabel.stringValue = @"Exporting artifact preview…";
    _artifactStateLabel.textColor = [NSColor systemOrangeColor];
    _artifactTextView.string = @"Generating export preview…";

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_artifactQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->exportArtifact(artifactId, format);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil || ![innerSelf isRequestTokenCurrent:token storage:&innerSelf->_artifactExportRequestToken]) {
                return;
            }
            innerSelf->_artifactInFlight = NO;
            innerSelf->_artifactFetchButton.enabled = YES;
            innerSelf->_artifactExportButton.enabled = YES;
            innerSelf->_artifactPane->clearEvidence();
            if (result.ok()) {
                innerSelf->_artifactStateLabel.stringValue = @"Artifact export preview ready.";
                innerSelf->_artifactStateLabel.textColor = [NSColor systemGreenColor];
            } else {
                innerSelf->_artifactStateLabel.stringValue = ToNSString(tapescope::QueryClient::describeError(result.error));
                innerSelf->_artifactStateLabel.textColor = ErrorColorForKind(result.error.kind);
            }
            innerSelf->_artifactTextView.string =
                ToNSString(DescribeArtifactExportResult(artifactId, format, result));
        });
    });
}

- (void)openSelectedArtifactEvidence:(id)sender {
    (void)sender;
    [self openSelectedEvidenceForPane:_artifactPane.get()];
}

- (void)refreshReportInventory:(id)sender {
    (void)sender;
    if (_reportInventoryInFlight || !_client) {
        return;
    }

    _reportInventoryInFlight = YES;
    const std::uint64_t token = [self issueRequestToken:&_reportInventoryRequestToken];
    _reportInventoryRefreshButton.enabled = NO;
    _reportInventoryOpenSessionButton.enabled = NO;
    _reportInventoryOpenCaseButton.enabled = NO;
    _reportInventoryStateLabel.stringValue = @"Refreshing report inventory…";
    _reportInventoryStateLabel.textColor = [NSColor systemOrangeColor];
    _reportInventoryTextView.string = @"Refreshing session and case report inventory…";

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_artifactQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto sessionReports = strongSelf->_client->listSessionReportsPayload(20);
        const auto caseReports = strongSelf->_client->listCaseReportsPayload(20);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil || ![innerSelf isRequestTokenCurrent:token storage:&innerSelf->_reportInventoryRequestToken]) {
                return;
            }
            innerSelf->_reportInventoryInFlight = NO;
            innerSelf->_reportInventoryRefreshButton.enabled = YES;

            innerSelf->_latestSessionReports.clear();
            innerSelf->_latestCaseReports.clear();
            if (sessionReports.ok()) {
                innerSelf->_latestSessionReports = sessionReports.value.sessionReports;
            }
            if (caseReports.ok()) {
                innerSelf->_latestCaseReports = caseReports.value.caseReports;
            }
            [innerSelf->_sessionReportTableView reloadData];
            [innerSelf->_caseReportTableView reloadData];
            if (!innerSelf->_latestSessionReports.empty()) {
                [innerSelf->_sessionReportTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0] byExtendingSelection:NO];
            }
            if (!innerSelf->_latestCaseReports.empty()) {
                [innerSelf->_caseReportTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0] byExtendingSelection:NO];
            }
            innerSelf->_reportInventoryOpenSessionButton.enabled =
                (innerSelf->_sessionReportTableView.selectedRow >= 0);
            innerSelf->_reportInventoryOpenCaseButton.enabled =
                (innerSelf->_caseReportTableView.selectedRow >= 0);

            if (sessionReports.ok() || caseReports.ok()) {
                if (!innerSelf->_latestSessionReports.empty() || !innerSelf->_latestCaseReports.empty()) {
                    innerSelf->_reportInventoryStateLabel.stringValue = @"Report inventory loaded.";
                    innerSelf->_reportInventoryStateLabel.textColor = [NSColor systemGreenColor];
                } else {
                    innerSelf->_reportInventoryStateLabel.stringValue = @"No durable reports are available yet.";
                    innerSelf->_reportInventoryStateLabel.textColor = TapeInkMutedColor();
                }
            } else {
                innerSelf->_reportInventoryStateLabel.stringValue = @"Report inventory queries failed.";
                innerSelf->_reportInventoryStateLabel.textColor = [NSColor systemRedColor];
            }
            if (innerSelf->_latestSessionReports.empty() && innerSelf->_latestCaseReports.empty()) {
                innerSelf->_reportInventoryTextView.string =
                    ToNSString(DescribeReportInventoryResult(sessionReports, caseReports));
            }
        });
    });
}

@end
