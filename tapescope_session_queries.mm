#import "tapescope_window_internal.h"

#include "tapescope_support.h"

namespace {

using namespace tapescope_support;

} // namespace

@implementation TapeScopeWindowController (SessionQueries)

- (void)fetchRange:(id)sender {
    (void)sender;
    if (_rangeInFlight || !_client) {
        return;
    }

    std::uint64_t firstSessionSeq = 0;
    std::uint64_t lastSessionSeq = 0;
    if (!ParsePositiveUInt64(ToStdString(_rangeFirstField.stringValue), &firstSessionSeq) ||
        !ParsePositiveUInt64(ToStdString(_rangeLastField.stringValue), &lastSessionSeq) ||
        firstSessionSeq > lastSessionSeq) {
        _rangeStateLabel.stringValue = @"Range inputs must be positive integers with first <= last.";
        _rangeStateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    _rangeInFlight = YES;
    const std::uint64_t token = [self issueRequestToken:&_rangeRequestToken];
    _rangeFetchButton.enabled = NO;
    _rangeStateLabel.stringValue = @"Fetching replay window…";
    _rangeStateLabel.textColor = [NSColor systemOrangeColor];
    _rangeTextView.string = @"Loading replay window and decoded events…";
    _lastRangeQuery.firstSessionSeq = firstSessionSeq;
    _lastRangeQuery.lastSessionSeq = lastSessionSeq;

    __weak TapeScopeWindowController* weakSelf = self;
    const tapescope::RangeQuery query = _lastRangeQuery;
    dispatch_async(_interactiveQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->readRangeRows(query);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil || ![innerSelf isRequestTokenCurrent:token storage:&innerSelf->_rangeRequestToken]) {
                return;
            }
            innerSelf->_rangeInFlight = NO;
            innerSelf->_rangeFetchButton.enabled = YES;
            if (result.ok()) {
                innerSelf->_rangeEvents = result.value;
                [innerSelf->_rangeTableView reloadData];
                if (!innerSelf->_rangeEvents.empty()) {
                    innerSelf->_rangeStateLabel.stringValue = @"Replay window loaded.";
                    innerSelf->_rangeStateLabel.textColor = [NSColor systemGreenColor];
                    [innerSelf->_rangeTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0] byExtendingSelection:NO];
                } else {
                    innerSelf->_rangeStateLabel.stringValue = @"Replay window is empty for that range.";
                    innerSelf->_rangeStateLabel.textColor = TapeInkMutedColor();
                    innerSelf->_rangeTextView.string = @"No decoded events are available for the requested replay window. Try widening the session_seq range or loading a replay target from an order, finding, or incident.";
                }
            } else {
                innerSelf->_rangeStateLabel.stringValue = ToNSString(tapescope::QueryClient::describeError(result.error));
                innerSelf->_rangeStateLabel.textColor = ErrorColorForKind(result.error.kind);
                innerSelf->_rangeEvents.clear();
                [innerSelf->_rangeTableView reloadData];
                innerSelf->_rangeTextView.string = ToNSString(DescribeRangeResult(query, result));
            }
        });
    });
}

- (void)loadQualityFromRange:(id)sender {
    (void)sender;
    _qualityFirstField.stringValue = _rangeFirstField.stringValue;
    _qualityLastField.stringValue = _rangeLastField.stringValue;
    if (_tabView != nil) {
        [_tabView selectTabViewItemWithIdentifier:@"QualityPane"];
    }
    [self fetchSessionQuality:nil];
}

- (void)fetchSessionQuality:(id)sender {
    (void)sender;
    if (_qualityInFlight || !_client) {
        return;
    }

    std::uint64_t firstSessionSeq = 0;
    std::uint64_t lastSessionSeq = 0;
    if (!ParsePositiveUInt64(ToStdString(_qualityFirstField.stringValue), &firstSessionSeq) ||
        !ParsePositiveUInt64(ToStdString(_qualityLastField.stringValue), &lastSessionSeq) ||
        firstSessionSeq > lastSessionSeq) {
        _qualityStateLabel.stringValue = @"Quality inputs must be positive integers with first <= last.";
        _qualityStateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    _qualityInFlight = YES;
    const std::uint64_t token = [self issueRequestToken:&_qualityRequestToken];
    _qualityFetchButton.enabled = NO;
    _qualityStateLabel.stringValue = @"Reading session quality…";
    _qualityStateLabel.textColor = [NSColor systemOrangeColor];
    _qualityTextView.string = @"Loading data-quality summary and provenance coverage…";
    _lastQualityQuery.firstSessionSeq = firstSessionSeq;
    _lastQualityQuery.lastSessionSeq = lastSessionSeq;
    const bool includeLiveTail = (_qualityIncludeLiveTailButton.state == NSControlStateValueOn);

    __weak TapeScopeWindowController* weakSelf = self;
    const tapescope::RangeQuery query = _lastQualityQuery;
    dispatch_async(_interactiveQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->readSessionQualityPayload(query, includeLiveTail);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil || ![innerSelf isRequestTokenCurrent:token storage:&innerSelf->_qualityRequestToken]) {
                return;
            }
            innerSelf->_qualityInFlight = NO;
            innerSelf->_qualityFetchButton.enabled = YES;
            if (result.ok()) {
                innerSelf->_qualityStateLabel.stringValue = @"Session quality loaded.";
                innerSelf->_qualityStateLabel.textColor = [NSColor systemGreenColor];
            } else {
                innerSelf->_qualityStateLabel.stringValue = ToNSString(tapescope::QueryClient::describeError(result.error));
                innerSelf->_qualityStateLabel.textColor = ErrorColorForKind(result.error.kind);
            }
            innerSelf->_qualityTextView.string = ToNSString(DescribeSessionQualityResult(query, includeLiveTail, result));
        });
    });
}

- (void)fetchFinding:(id)sender {
    (void)sender;
    if (_findingInFlight || !_client) {
        return;
    }

    std::uint64_t findingId = 0;
    if (!ParsePositiveUInt64(ToStdString(_findingIdField.stringValue), &findingId)) {
        _findingStateLabel.stringValue = @"finding_id must be a positive integer.";
        _findingStateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    _findingInFlight = YES;
    _findingFetchButton.enabled = NO;
    const std::uint64_t token = _findingPane->beginRequest(@"Reading finding…");

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_interactiveQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->readFindingPayload(findingId);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil || !innerSelf->_findingPane->isCurrent(token)) {
                return;
            }
            innerSelf->_findingInFlight = NO;
            innerSelf->_findingFetchButton.enabled = YES;
            [innerSelf applyInvestigationResult:result
                                 paneController:innerSelf->_findingPane.get()
                                    successText:@"Finding loaded."
                               syncArtifactField:YES];
            if (result.ok()) {
                [innerSelf recordRecentHistoryForKind:"finding"
                                             targetId:std::to_string(findingId)
                                              payload:result.value
                                        fallbackTitle:"Finding " + std::to_string(findingId)
                                       fallbackDetail:"Reopen the persisted finding drilldown."
                                             metadata:tapescope::json{{"finding_id", findingId}}];
            }
            innerSelf->_findingTextView.string =
                ToNSString(DescribeInvestigationPayload("finding",
                                                       "finding_id=" + std::to_string(findingId),
                                                       result));
        });
    });
}

- (void)loadReplayWindowFromFinding:(id)sender {
    (void)sender;
    [self loadReplayWindowForPane:_findingPane.get()];
}

- (void)openSelectedFindingEvidence:(id)sender {
    (void)sender;
    [self openSelectedEvidenceForPane:_findingPane.get()];
}

- (void)fetchOrderAnchorById:(id)sender {
    (void)sender;
    if (_anchorInFlight || !_client) {
        return;
    }

    std::uint64_t anchorId = 0;
    if (!ParsePositiveUInt64(ToStdString(_anchorIdField.stringValue), &anchorId)) {
        _anchorStateLabel.stringValue = @"anchor_id must be a positive integer.";
        _anchorStateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    _anchorInFlight = YES;
    _anchorFetchButton.enabled = NO;
    const std::uint64_t token = _anchorPane->beginRequest(@"Reading order anchor…");

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_interactiveQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->readOrderAnchorPayload(anchorId);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil || !innerSelf->_anchorPane->isCurrent(token)) {
                return;
            }
            innerSelf->_anchorInFlight = NO;
            innerSelf->_anchorFetchButton.enabled = YES;
            [innerSelf applyInvestigationResult:result
                                 paneController:innerSelf->_anchorPane.get()
                                    successText:@"Order anchor loaded."
                               syncArtifactField:YES];
            if (result.ok()) {
                [innerSelf recordRecentHistoryForKind:"anchor"
                                             targetId:std::to_string(anchorId)
                                              payload:result.value
                                        fallbackTitle:"Order anchor " + std::to_string(anchorId)
                                       fallbackDetail:"Reopen the persisted order-anchor drilldown."
                                             metadata:tapescope::json{{"anchor_id", anchorId}}];
            }
            innerSelf->_anchorTextView.string =
                ToNSString(DescribeInvestigationPayload("order_anchor",
                                                       "anchor_id=" + std::to_string(anchorId),
                                                       result));
        });
    });
}

- (void)loadReplayWindowFromAnchor:(id)sender {
    (void)sender;
    [self loadReplayWindowForPane:_anchorPane.get()];
}

- (void)openSelectedAnchorEvidence:(id)sender {
    (void)sender;
    [self openSelectedEvidenceForPane:_anchorPane.get()];
}

- (void)fetchOverview:(id)sender {
    (void)sender;
    if (_overviewInFlight || !_client) {
        return;
    }

    std::uint64_t firstSessionSeq = 0;
    std::uint64_t lastSessionSeq = 0;
    if (!ParsePositiveUInt64(ToStdString(_overviewFirstField.stringValue), &firstSessionSeq) ||
        !ParsePositiveUInt64(ToStdString(_overviewLastField.stringValue), &lastSessionSeq) ||
        firstSessionSeq > lastSessionSeq) {
        _overviewStateLabel.stringValue = @"Overview inputs must be positive integers with first <= last.";
        _overviewStateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    _overviewInFlight = YES;
    _overviewFetchButton.enabled = NO;
    _overviewScanButton.enabled = NO;
    _overviewOpenSelectedIncidentButton.enabled = NO;
    const std::uint64_t token = _overviewPane->beginRequest(@"Reading session overview…");
    _lastOverviewQuery.firstSessionSeq = firstSessionSeq;
    _lastOverviewQuery.lastSessionSeq = lastSessionSeq;

    __weak TapeScopeWindowController* weakSelf = self;
    const tapescope::RangeQuery query = _lastOverviewQuery;
    dispatch_async(_interactiveQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->readSessionOverviewPayload(query);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil || !innerSelf->_overviewPane->isCurrent(token)) {
                return;
            }
            innerSelf->_overviewInFlight = NO;
            innerSelf->_overviewFetchButton.enabled = YES;
            innerSelf->_overviewScanButton.enabled = YES;
            if (result.ok()) {
                innerSelf->_overviewIncidents = result.value.incidents;
                [innerSelf->_overviewIncidentTableView reloadData];
                [innerSelf applyInvestigationResult:result
                                     paneController:innerSelf->_overviewPane.get()
                                        successText:@"Session overview loaded."
                                   syncArtifactField:NO];
                [innerSelf recordRecentHistoryForKind:"overview"
                                             targetId:std::to_string(query.firstSessionSeq) + "-" +
                                                      std::to_string(query.lastSessionSeq)
                                              payload:result.value
                                        fallbackTitle:"Session overview " +
                                                      std::to_string(query.firstSessionSeq) + "-" +
                                                      std::to_string(query.lastSessionSeq)
                                       fallbackDetail:"Reopen the recent session-overview range."
                                             metadata:tapescope::json{{"first_session_seq", query.firstSessionSeq},
                                                                      {"last_session_seq", query.lastSessionSeq}}];
                innerSelf->_overviewOpenSelectedIncidentButton.enabled = !innerSelf->_overviewIncidents.empty();
                if (!innerSelf->_overviewIncidents.empty()) {
                    [innerSelf->_overviewIncidentTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0]
                                                         byExtendingSelection:NO];
                }
            } else {
                innerSelf->_overviewIncidents.clear();
                [innerSelf->_overviewIncidentTableView reloadData];
                [innerSelf applyInvestigationResult:result
                                     paneController:innerSelf->_overviewPane.get()
                                        successText:nil
                                   syncArtifactField:NO];
                innerSelf->_overviewOpenSelectedIncidentButton.enabled = NO;
            }
            innerSelf->_overviewTextView.string =
                ToNSString(DescribeInvestigationPayload("session_overview",
                                                       "session_seq=[" + std::to_string(query.firstSessionSeq) + ", " +
                                                           std::to_string(query.lastSessionSeq) + "]",
                                                       result));
        });
    });
}

- (void)scanOverviewReport:(id)sender {
    (void)sender;
    if (_overviewInFlight || !_client) {
        return;
    }

    std::uint64_t firstSessionSeq = 0;
    std::uint64_t lastSessionSeq = 0;
    if (!ParsePositiveUInt64(ToStdString(_overviewFirstField.stringValue), &firstSessionSeq) ||
        !ParsePositiveUInt64(ToStdString(_overviewLastField.stringValue), &lastSessionSeq) ||
        firstSessionSeq > lastSessionSeq) {
        _overviewStateLabel.stringValue = @"Overview inputs must be positive integers with first <= last.";
        _overviewStateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    _overviewInFlight = YES;
    _overviewFetchButton.enabled = NO;
    _overviewScanButton.enabled = NO;
    _overviewOpenSelectedIncidentButton.enabled = NO;
    const std::uint64_t token = _overviewPane->beginRequest(@"Scanning durable session report…");
    _lastOverviewQuery.firstSessionSeq = firstSessionSeq;
    _lastOverviewQuery.lastSessionSeq = lastSessionSeq;

    __weak TapeScopeWindowController* weakSelf = self;
    const tapescope::RangeQuery query = _lastOverviewQuery;
    dispatch_async(_artifactQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->scanSessionReportPayload(query);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil || !innerSelf->_overviewPane->isCurrent(token)) {
                return;
            }
            innerSelf->_overviewInFlight = NO;
            innerSelf->_overviewFetchButton.enabled = YES;
            innerSelf->_overviewScanButton.enabled = YES;
            if (result.ok()) {
                innerSelf->_overviewIncidents = result.value.incidents;
                [innerSelf->_overviewIncidentTableView reloadData];
                [innerSelf applyInvestigationResult:result
                                     paneController:innerSelf->_overviewPane.get()
                                        successText:@"Durable session report scanned."
                                   syncArtifactField:YES];
                if (!result.value.artifactId.empty()) {
                    [innerSelf recordRecentHistoryForKind:"artifact"
                                                 targetId:result.value.artifactId
                                                  payload:result.value
                                            fallbackTitle:"Session report " + result.value.artifactId
                                           fallbackDetail:"Reopen the durable scanned session report."
                                                 metadata:tapescope::json{{"artifact_id", result.value.artifactId}}];
                }
                innerSelf->_overviewOpenSelectedIncidentButton.enabled = !innerSelf->_overviewIncidents.empty();
                if (!innerSelf->_overviewIncidents.empty()) {
                    [innerSelf->_overviewIncidentTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0]
                                                         byExtendingSelection:NO];
                }
                [innerSelf refreshReportInventory:nil];
            } else {
                innerSelf->_overviewIncidents.clear();
                [innerSelf->_overviewIncidentTableView reloadData];
                [innerSelf applyInvestigationResult:result
                                     paneController:innerSelf->_overviewPane.get()
                                        successText:nil
                                   syncArtifactField:NO];
                innerSelf->_overviewOpenSelectedIncidentButton.enabled = NO;
            }
            innerSelf->_overviewTextView.string =
                ToNSString(DescribeInvestigationPayload("session_report_scan",
                                                       "session_seq=[" + std::to_string(query.firstSessionSeq) + ", " +
                                                           std::to_string(query.lastSessionSeq) + "]",
                                                       result));
        });
    });
}

- (void)loadReplayWindowFromOverview:(id)sender {
    (void)sender;
    [self loadReplayWindowForPane:_overviewPane.get()];
}

- (void)openSelectedOverviewEvidence:(id)sender {
    (void)sender;
    [self openSelectedEvidenceForPane:_overviewPane.get()];
}

@end
