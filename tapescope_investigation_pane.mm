#include "tapescope_investigation_pane.h"

namespace tapescope {

namespace {

NSString* ToNSString(const std::string& value) {
    if (value.empty()) {
        return @"";
    }
    return [NSString stringWithUTF8String:value.c_str()];
}

} // namespace

void InvestigationPaneController::bind(NSTextField* stateLabel,
                                       NSTextView* detailView,
                                       NSTableView* evidenceTableView,
                                       NSButton* openEvidenceButton,
                                       NSButton* loadReplayButton,
                                       NSString* emptyEvidenceMessage,
                                       NSString* missingSelectionMessage,
                                       NSString* missingArtifactMessage,
                                       NSString* missingReplayMessage) {
    stateLabel_ = stateLabel;
    detailView_ = detailView;
    evidenceTableView_ = evidenceTableView;
    openEvidenceButton_ = openEvidenceButton;
    loadReplayButton_ = loadReplayButton;
    emptyEvidenceMessage_ = emptyEvidenceMessage;
    missingSelectionMessage_ = missingSelectionMessage;
    missingArtifactMessage_ = missingArtifactMessage;
    missingReplayMessage_ = missingReplayMessage;
}

std::uint64_t InvestigationPaneController::beginRequest(NSString* message) {
    ++generation_;
    if (stateLabel_ != nil) {
        stateLabel_.stringValue = message ?: @"Loading…";
        stateLabel_.textColor = [NSColor systemOrangeColor];
    }
    if (detailView_ != nil) {
        detailView_.string = @"Loading investigation details…";
    }
    hasReplayRange_ = false;
    if (loadReplayButton_ != nil) {
        loadReplayButton_.enabled = NO;
    }
    resetEvidence();
    return generation_;
}

bool InvestigationPaneController::isCurrent(std::uint64_t token) const {
    return generation_ == token;
}

void InvestigationPaneController::applyResult(const QueryResult<InvestigationPayload>& result,
                                              NSString* successText) {
    if (!result.ok()) {
        if (stateLabel_ != nil) {
            stateLabel_.stringValue = ToNSString(QueryClient::describeError(result.error));
            stateLabel_.textColor = [NSColor systemRedColor];
        }
        hasReplayRange_ = false;
        if (loadReplayButton_ != nil) {
            loadReplayButton_.enabled = NO;
        }
        resetEvidence();
        return;
    }

    if (stateLabel_ != nil) {
        stateLabel_.stringValue = successText ?: @"Loaded.";
        stateLabel_.textColor = [NSColor systemGreenColor];
    }

    artifactId_ = result.value.artifactId;
    evidenceItems_ = result.value.evidence;
    if (evidenceTableView_ != nil) {
        [evidenceTableView_ reloadData];
    }
    const bool hasRows = !evidenceItems_.empty();
    if (openEvidenceButton_ != nil) {
        openEvidenceButton_.enabled = hasRows;
    }
    if (hasRows && evidenceTableView_ != nil) {
        [evidenceTableView_ selectRowIndexes:[NSIndexSet indexSetWithIndex:0] byExtendingSelection:NO];
    } else if (detailView_ != nil) {
        detailView_.string = emptyEvidenceMessage_ ?: @"Select an evidence row to inspect it.";
    }

    if (!result.value.artifactId.empty() && detailView_ != nil && evidenceItems_.empty()) {
        detailView_.string = ToNSString(result.value.raw.dump(2));
    }

    hasReplayRange_ = result.value.replayRange.has_value();
    if (hasReplayRange_) {
        replayRange_ = *result.value.replayRange;
    }
    if (loadReplayButton_ != nil) {
        loadReplayButton_.enabled = hasReplayRange_;
    }
}

void InvestigationPaneController::applyResult(const QueryResult<EnrichmentPayload>& result,
                                              NSString* successText) {
    if (!result.ok()) {
        if (stateLabel_ != nil) {
            stateLabel_.stringValue = ToNSString(QueryClient::describeError(result.error));
            stateLabel_.textColor = [NSColor systemRedColor];
        }
        hasReplayRange_ = false;
        if (loadReplayButton_ != nil) {
            loadReplayButton_.enabled = NO;
        }
        resetEvidence();
        return;
    }

    if (stateLabel_ != nil) {
        stateLabel_.stringValue = successText ?: @"Loaded.";
        stateLabel_.textColor = [NSColor systemGreenColor];
    }

    artifactId_ = !result.value.artifactId.empty() ? result.value.artifactId : result.value.localEvidence.artifactId;
    evidenceItems_ = result.value.localEvidence.evidence;
    if (evidenceTableView_ != nil) {
        [evidenceTableView_ reloadData];
    }
    const bool hasRows = !evidenceItems_.empty();
    if (openEvidenceButton_ != nil) {
        openEvidenceButton_.enabled = hasRows;
    }
    if (hasRows && evidenceTableView_ != nil) {
        [evidenceTableView_ selectRowIndexes:[NSIndexSet indexSetWithIndex:0] byExtendingSelection:NO];
    }

    hasReplayRange_ = result.value.localEvidence.replayRange.has_value();
    if (hasReplayRange_) {
        replayRange_ = *result.value.localEvidence.replayRange;
    }
    if (loadReplayButton_ != nil) {
        loadReplayButton_.enabled = hasReplayRange_;
    }
}

void InvestigationPaneController::syncArtifactField(NSTextField* artifactField) const {
    if (artifactField == nil) {
        return;
    }
    if (!artifactId_.empty()) {
        artifactField.stringValue = ToNSString(artifactId_);
        return;
    }
    for (const auto& evidence : evidenceItems_) {
        if (!evidence.artifactId.empty()) {
            artifactField.stringValue = ToNSString(evidence.artifactId);
            return;
        }
    }
}

void InvestigationPaneController::clearEvidence() {
    resetEvidence();
}

bool InvestigationPaneController::hasReplayRange() const {
    return hasReplayRange_;
}

const RangeQuery& InvestigationPaneController::replayRange() const {
    return replayRange_;
}

const std::vector<EvidenceCitation>& InvestigationPaneController::evidenceItems() const {
    return evidenceItems_;
}

void InvestigationPaneController::renderSelection() const {
    const NSInteger selected = evidenceTableView_ != nil ? evidenceTableView_.selectedRow : -1;
    if (openEvidenceButton_ != nil) {
        openEvidenceButton_.enabled = (selected >= 0);
    }
    if (detailView_ == nil) {
        return;
    }
    if (selected < 0 || static_cast<std::size_t>(selected) >= evidenceItems_.size()) {
        detailView_.string = emptyEvidenceMessage_ ?: @"Select an evidence row to inspect it.";
        return;
    }
    detailView_.string = ToNSString(evidenceItems_.at(static_cast<std::size_t>(selected)).raw.dump(2));
}

std::string InvestigationPaneController::selectedEvidenceArtifactId() const {
    const NSInteger selected = evidenceTableView_ != nil ? evidenceTableView_.selectedRow : -1;
    if (selected < 0 || static_cast<std::size_t>(selected) >= evidenceItems_.size()) {
        return {};
    }
    return evidenceItems_.at(static_cast<std::size_t>(selected)).artifactId;
}

void InvestigationPaneController::setStateError(NSString* text) const {
    if (stateLabel_ != nil) {
        stateLabel_.stringValue = text ?: @"";
        stateLabel_.textColor = [NSColor systemRedColor];
    }
}

NSString* InvestigationPaneController::missingSelectionMessage() const {
    return missingSelectionMessage_;
}

NSString* InvestigationPaneController::missingArtifactMessage() const {
    return missingArtifactMessage_;
}

NSString* InvestigationPaneController::missingReplayMessage() const {
    return missingReplayMessage_;
}

NSTextField* InvestigationPaneController::stateLabel() const {
    return stateLabel_;
}

void InvestigationPaneController::resetEvidence() {
    evidenceItems_.clear();
    artifactId_.clear();
    if (evidenceTableView_ != nil) {
        [evidenceTableView_ reloadData];
    }
    if (openEvidenceButton_ != nil) {
        openEvidenceButton_.enabled = NO;
    }
}

} // namespace tapescope
