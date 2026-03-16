#include "tapescope_bundle_preview.h"

#include <sstream>

namespace tapescope {

namespace {

std::string boolString(bool value) {
    return value ? "true" : "false";
}

std::string describeImportReason(const std::string& verifyStatus,
                                 const std::string& importReason) {
    if (importReason == "case_bundle_can_be_imported") {
        return "This case bundle can be imported into the local investigation inventory.";
    }
    if (importReason == "payload_already_imported") {
        return "This bundle is already present in the local imported-case inventory.";
    }
    if (importReason == "bundle_type_not_importable") {
        if (verifyStatus == "unknown_bundle_type") {
            return "This bundle type is not recognized by this build and cannot be imported.";
        }
        return "Only Phase 6 case bundles can be imported. Session bundles remain preview-only.";
    }
    if (!importReason.empty()) {
        return importReason;
    }
    if (verifyStatus == "unknown_bundle_type") {
        return "This bundle type is not recognized by this build and cannot be imported.";
    }
    return "Import is currently blocked for this bundle.";
}

} // namespace

BundlePreviewDecision bundlePreviewDecisionFromInspection(const tape_bundle::PortableBundleInspection& inspection) {
    BundlePreviewDecision decision;
    if (inspection.bundleType == "case_bundle") {
        decision.importSupported = true;
        decision.importAllowed = true;
        decision.status = "case_bundle_can_be_imported";
        decision.detail = "This case bundle can be imported into the local investigation inventory.";
        return decision;
    }
    if (inspection.bundleType == "session_bundle") {
        decision.status = "session_bundle_preview_only";
        decision.detail = "This is a session bundle. It can be previewed and reopened, but not imported as a case bundle.";
        return decision;
    }
    decision.status = "unknown_bundle_type";
    decision.detail = "This bundle type is not recognized by this build and cannot be imported.";
    return decision;
}

BundlePreviewDecision bundlePreviewDecisionFromVerifyPayload(const tape_payloads::BundleVerifyPayload& payload) {
    BundlePreviewDecision decision;
    decision.importSupported = payload.importSupported;
    decision.importAllowed = payload.canImport;
    decision.alreadyImported = payload.alreadyImported;
    decision.status = payload.verifyStatus.empty() ? "unknown" : payload.verifyStatus;
    decision.detail = describeImportReason(payload.verifyStatus, payload.importReason);
    if (payload.alreadyImported && payload.hasImportedCase && !payload.importedCase.artifactId.empty()) {
        decision.detail += " Open ";
        decision.detail += payload.importedCase.artifactId;
        decision.detail += " instead of importing it again.";
    }
    return decision;
}

BundlePreviewDecision markBundlePreviewDecisionImported(const BundlePreviewDecision& decision,
                                                        const tape_payloads::ImportedCaseRow& importedCase,
                                                        const std::string& matchReason) {
    BundlePreviewDecision updated = decision;
    updated.importSupported = true;
    updated.importAllowed = false;
    updated.alreadyImported = true;
    updated.status = "already_imported";
    std::ostringstream out;
    out << "This bundle already matches imported case #" << importedCase.importedCaseId;
    if (!importedCase.artifactId.empty()) {
        out << " (" << importedCase.artifactId << ")";
    }
    if (!matchReason.empty()) {
        out << " via " << matchReason;
    }
    out << ". Reopen the imported artifact instead of importing it again.";
    updated.detail = out.str();
    return updated;
}

std::string describeBundlePreviewDecision(const BundlePreviewDecision& decision) {
    std::ostringstream out;
    out << "import_status: " << decision.status << "\n"
        << "import_supported: " << boolString(decision.importSupported) << "\n"
        << "already_imported: " << boolString(decision.alreadyImported) << "\n"
        << "import_allowed: " << boolString(decision.importAllowed) << "\n"
        << "import_detail: " << decision.detail << "\n";
    return out.str();
}

std::string describeBundlePreviewFailure(const std::string& inspectError) {
    if (inspectError == "failed to open bundle") {
        return "Bundle preview failed: unable to open the selected bundle path.";
    }
    if (inspectError == "bundle is empty") {
        return "Bundle preview failed: the selected bundle file is empty.";
    }
    if (inspectError == "bundle_path does not exist") {
        return "Bundle preview failed: the selected bundle path does not exist.";
    }
    if (inspectError == "bundle is not valid MessagePack JSON") {
        return "Bundle preview failed: the selected file is not a valid portable bundle MessagePack payload.";
    }
    if (inspectError == "bundle schema/version is not supported") {
        return "Bundle preview failed: this bundle schema or version is not supported by the current build.";
    }
    if (!inspectError.empty()) {
        return "Bundle preview failed: " + inspectError + ".";
    }
    return "Bundle preview failed: the selected bundle could not be inspected.";
}

} // namespace tapescope
