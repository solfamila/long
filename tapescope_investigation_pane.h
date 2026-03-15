#pragma once

#import <AppKit/AppKit.h>

#include "tapescope_client.h"

#include <cstdint>
#include <string>
#include <vector>

namespace tapescope {

class InvestigationPaneController {
public:
    InvestigationPaneController() = default;

    void bind(NSTextField* stateLabel,
              NSTextView* detailView,
              NSTableView* evidenceTableView,
              NSButton* openEvidenceButton,
              NSButton* loadReplayButton,
              NSString* emptyEvidenceMessage,
              NSString* missingSelectionMessage,
              NSString* missingArtifactMessage,
              NSString* missingReplayMessage);

    [[nodiscard]] std::uint64_t beginRequest(NSString* message);
    [[nodiscard]] bool isCurrent(std::uint64_t token) const;

    void applyResult(const QueryResult<InvestigationPayload>& result, NSString* successText);
    void syncArtifactField(NSTextField* artifactField) const;
    void clearEvidence();

    [[nodiscard]] bool hasReplayRange() const;
    [[nodiscard]] const RangeQuery& replayRange() const;
    [[nodiscard]] const std::vector<EvidenceCitation>& evidenceItems() const;

    void renderSelection() const;
    [[nodiscard]] std::string selectedEvidenceArtifactId() const;

    void setStateError(NSString* text) const;
    [[nodiscard]] NSString* missingSelectionMessage() const;
    [[nodiscard]] NSString* missingArtifactMessage() const;
    [[nodiscard]] NSString* missingReplayMessage() const;
    [[nodiscard]] NSTextField* stateLabel() const;

private:
    void resetEvidence();

    NSTextField* stateLabel_ = nil;
    NSTextView* detailView_ = nil;
    NSTableView* evidenceTableView_ = nil;
    NSButton* openEvidenceButton_ = nil;
    NSButton* loadReplayButton_ = nil;

    NSString* emptyEvidenceMessage_ = nil;
    NSString* missingSelectionMessage_ = nil;
    NSString* missingArtifactMessage_ = nil;
    NSString* missingReplayMessage_ = nil;

    std::vector<EvidenceCitation> evidenceItems_;
    std::string artifactId_;
    RangeQuery replayRange_{};
    bool hasReplayRange_ = false;
    std::uint64_t generation_ = 0;
};

} // namespace tapescope
