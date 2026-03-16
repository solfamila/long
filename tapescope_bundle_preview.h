#pragma once

#include "tape_bundle_inspection.h"
#include "tape_query_payloads.h"

#include <string>

namespace tapescope {

struct BundlePreviewDecision {
    bool importSupported = false;
    bool importAllowed = false;
    bool alreadyImported = false;
    std::string status;
    std::string detail;
};

BundlePreviewDecision bundlePreviewDecisionFromInspection(const tape_bundle::PortableBundleInspection& inspection);
BundlePreviewDecision bundlePreviewDecisionFromVerifyPayload(const tape_payloads::BundleVerifyPayload& payload);
BundlePreviewDecision markBundlePreviewDecisionImported(const BundlePreviewDecision& decision,
                                                        const tape_payloads::ImportedCaseRow& importedCase,
                                                        const std::string& matchReason);
std::string describeBundlePreviewDecision(const BundlePreviewDecision& decision);
std::string describeBundlePreviewFailure(const std::string& inspectError);

} // namespace tapescope
