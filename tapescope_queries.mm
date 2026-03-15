#import "tapescope_window_internal.h"

#include "tapescope_support.h"

namespace {

using namespace tapescope_support;

} // namespace

@implementation TapeScopeWindowController (Queries)

- (std::uint64_t)issueRequestToken:(std::uint64_t*)storage {
    if (storage == nullptr) {
        return 0;
    }
    *storage += 1;
    return *storage;
}

- (BOOL)isRequestTokenCurrent:(std::uint64_t)token storage:(const std::uint64_t*)storage {
    if (storage == nullptr) {
        return NO;
    }
    return *storage == token;
}

- (void)applyInvestigationResult:(const tapescope::QueryResult<tapescope::InvestigationPayload>&)result
                  paneController:(tapescope::InvestigationPaneController*)pane
                     successText:(NSString*)successText
                syncArtifactField:(BOOL)syncArtifactField {
    if (pane == nullptr) {
        return;
    }
    pane->applyResult(result, successText);
    if (syncArtifactField && result.ok()) {
        pane->syncArtifactField(_artifactIdField);
    }
}

- (void)loadReplayWindowForPane:(tapescope::InvestigationPaneController*)pane {
    if (pane == nullptr) {
        return;
    }
    [self loadReplayRange:pane->replayRange()
                available:pane->hasReplayRange()
               stateLabel:pane->stateLabel()
           missingMessage:pane->missingReplayMessage()];
}

- (void)openSelectedEvidenceForPane:(tapescope::InvestigationPaneController*)pane {
    if (pane == nullptr) {
        return;
    }
    const std::string artifactId = pane->selectedEvidenceArtifactId();
    if (artifactId.empty()) {
        pane->setStateError(pane->missingSelectionMessage());
        return;
    }
    _artifactIdField.stringValue = ToNSString(artifactId);
    if (_tabView != nil) {
        [_tabView selectTabViewItemWithIdentifier:@"ArtifactPane"];
    }
    [self fetchArtifact:nil];
}

- (BOOL)renderSelectionForPane:(tapescope::InvestigationPaneController*)pane {
    if (pane == nullptr) {
        return NO;
    }
    pane->renderSelection();
    return YES;
}

- (void)loadReplayRange:(const tapescope::RangeQuery&)range
              available:(BOOL)available
             stateLabel:(NSTextField*)stateLabel
         missingMessage:(NSString*)missingMessage {
    if (!available) {
        if (stateLabel != nil) {
            stateLabel.stringValue = missingMessage;
            stateLabel.textColor = [NSColor systemRedColor];
        }
        return;
    }
    _rangeFirstField.stringValue = UInt64String(range.firstSessionSeq);
    _rangeLastField.stringValue = UInt64String(range.lastSessionSeq);
    if (_tabView != nil) {
        [_tabView selectTabViewItemWithIdentifier:@"RangePane"];
    }
    [self fetchRange:nil];
}

- (void)startPolling {
    [self refresh:nil];
    if (!_pollingPaused) {
        _pollTimer = [NSTimer scheduledTimerWithTimeInterval:tapescope_window_internal::kPollIntervalSeconds
                                                      target:self
                                                    selector:@selector(refresh:)
                                                    userInfo:nil
                                                     repeats:YES];
    }
    [self updatePollingStatusText];
}

- (void)shutdown {
    [self persistApplicationState];
    [_pollTimer invalidate];
    _pollTimer = nil;
}

- (void)refresh:(id)sender {
    (void)sender;
    if (_pollingPaused && sender != _refreshNowButton) {
        return;
    }
    if (_pollInFlight || !_client) {
        return;
    }

    _pollInFlight = YES;
    _bannerLabel.stringValue = @"Probing tape_engine…";
    [self updateBannerAppearanceWithColor:[NSColor systemOrangeColor]];

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_interactiveQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }

        tapescope_window_internal::ProbeSnapshot probe;
        probe.status = strongSelf->_client->status();
        probe.liveTail = strongSelf->_client->readLiveTailRows(tapescope_window_internal::kLiveTailLimit);

        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil) {
                return;
            }
            innerSelf->_pollInFlight = NO;
            [innerSelf applyProbe:probe];
        });
    });
}

- (void)applyProbe:(const tapescope_window_internal::ProbeSnapshot&)probe {
    _lastProbeAt = [NSDate date];
    if (probe.status.ok()) {
        _bannerLabel.stringValue = @"Connected to tape_engine";
        [self updateBannerAppearanceWithColor:[NSColor systemGreenColor]];
        _socketValue.stringValue = ToNSString(probe.status.value.socketPath);
        _dataDirValue.stringValue = ToNSString(probe.status.value.dataDir);
        _instrumentValue.stringValue = ToNSString(probe.status.value.instrumentId);
        _latestSeqValue.stringValue = UInt64String(probe.status.value.latestSessionSeq);
        _liveCountValue.stringValue = UInt64String(probe.status.value.liveEventCount);
        _segmentCountValue.stringValue = UInt64String(probe.status.value.segmentCount);
        _manifestHashValue.stringValue = ToNSString(probe.status.value.manifestHash);
    } else {
        _bannerLabel.stringValue = ToNSString(tapescope::QueryClient::describeError(probe.status.error));
        [self updateBannerAppearanceWithColor:ErrorColorForKind(probe.status.error.kind)];
        _socketValue.stringValue = ToNSString(_client ? _client->config().socketPath : tapescope::defaultSocketPath());
        _dataDirValue.stringValue = @"--";
        _instrumentValue.stringValue = @"--";
        _latestSeqValue.stringValue = @"--";
        _liveCountValue.stringValue = @"--";
        _segmentCountValue.stringValue = @"--";
        _manifestHashValue.stringValue = @"--";
    }

    [self updatePollingStatusText];
    _statusTextView.string = ToNSString(DescribeStatusPane(probe.status,
                                                           _client ? _client->config().socketPath : tapescope::defaultSocketPath()));
    if (probe.liveTail.ok()) {
        _liveEvents = probe.liveTail.value;
        [_liveTableView reloadData];
        if (!_liveEvents.empty()) {
            [_liveTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0] byExtendingSelection:NO];
        } else {
            _liveTextView.string = @"No live events returned.";
        }
    } else {
        _liveEvents.clear();
        [_liveTableView reloadData];
        _liveTextView.string = ToNSString(DescribeLiveEventsPane(probe.liveTail));
    }
}

@end
