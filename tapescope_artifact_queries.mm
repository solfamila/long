#import "tapescope_window_internal.h"

#include "tape_bundle_inspection.h"
#include "tapescope_bundle_preview.h"
#include "tapescope_support.h"

#include <optional>
#include <sstream>

namespace {

using namespace tapescope_support;

struct BundlePreviewDetails {
    std::string bundleId;
    std::string bundleType;
    std::string sourceArtifactId;
    std::string payloadSha256;
    std::uint64_t firstSessionSeq = 0;
    std::uint64_t lastSessionSeq = 0;
};

bool ShouldFallbackToLocalBundlePreview(const tapescope::QueryError& error) {
    return error.kind == tapescope::QueryErrorKind::Transport ||
           (error.kind == tapescope::QueryErrorKind::Remote &&
            error.message.find("unsupported") != std::string::npos);
}

std::string DescribeBundleExportPayload(const std::string& label,
                                        std::uint64_t reportId,
                                        const tapescope::QueryResult<tapescope::BundleExportPayload>& result) {
    if (!result.ok()) {
        return label + " failed for report_id=" + std::to_string(reportId) + "\n" +
               tapescope::QueryClient::describeError(result.error);
    }
    std::ostringstream out;
    out << label << " ready for report_id=" << reportId << "\n"
        << "artifact_id: " << result.value.artifactId << "\n"
        << "bundle_id: " << result.value.bundleId << "\n"
        << "bundle_type: " << result.value.bundleType << "\n"
        << "bundle_path: " << result.value.bundlePath << "\n"
        << "served_revision_id: " << result.value.servedRevisionId << "\n";
    return out.str();
}

std::string DescribeImportedCaseListPayload(const tapescope::QueryResult<tapescope::ImportedCaseListPayload>& result) {
    if (!result.ok()) {
        return tapescope::QueryClient::describeError(result.error);
    }
    std::ostringstream out;
    out << "Imported case bundles: " << result.value.importedCases.size() << "\n";
    for (const auto& item : result.value.importedCases) {
        out << "- imported_case_id=" << item.importedCaseId
            << " artifact_id=" << item.artifactId
            << " headline=" << item.headline << "\n";
    }
    return out.str();
}

std::string DescribeImportedCaseRow(const tapescope::ImportedCaseRow& row) {
    std::ostringstream out;
    out << "imported_case_id: " << row.importedCaseId << "\n"
        << "artifact_id: " << row.artifactId << "\n"
        << "headline: " << row.headline << "\n"
        << "bundle_id: " << row.bundleId << "\n"
        << "bundle_type: " << row.bundleType << "\n"
        << "source_artifact_id: " << row.sourceArtifactId << "\n"
        << "source_report_id: " << row.sourceReportId << "\n"
        << "source_revision_id: " << row.sourceRevisionId << "\n"
        << "instrument_id: " << row.instrumentId << "\n"
        << "first_session_seq: " << row.firstSessionSeq << "\n"
        << "last_session_seq: " << row.lastSessionSeq << "\n"
        << "source_bundle_path: " << row.sourceBundlePath << "\n"
        << "file_name: " << row.fileName << "\n"
        << "payload_sha256: " << row.payloadSha256 << "\n";
    return out.str();
}

std::string DescribeCaseBundleImportPayload(const std::string& bundlePath,
                                            const tapescope::QueryResult<tapescope::CaseBundleImportPayload>& result) {
    if (!result.ok()) {
        return "Import failed for " + bundlePath + "\n" + tapescope::QueryClient::describeError(result.error);
    }
    std::ostringstream out;
    out << "Import status: " << result.value.importStatus << "\n"
        << "artifact_id: " << result.value.artifactId << "\n"
        << "imported_case_id: " << result.value.importedCase.importedCaseId << "\n"
        << "bundle_path: " << bundlePath << "\n";
    if (result.value.duplicateImport) {
        out << "duplicate_import: true\n";
    }
    return out.str();
}

BundlePreviewDetails BundlePreviewDetailsFromInspection(const tape_bundle::PortableBundleInspection& inspection) {
    BundlePreviewDetails details;
    details.bundleId = inspection.bundleId;
    details.bundleType = inspection.bundleType;
    details.sourceArtifactId = inspection.sourceArtifactId;
    details.payloadSha256 = inspection.payloadSha256;
    details.firstSessionSeq = inspection.firstSessionSeq;
    details.lastSessionSeq = inspection.lastSessionSeq;
    return details;
}

std::string DescribeBundlePreview(const tape_bundle::PortableBundleInspection& inspection) {
    std::ostringstream out;
    out << "bundle_path: " << inspection.bundlePath.string() << "\n"
        << "bundle_id: " << inspection.bundleId << "\n"
        << "bundle_type: " << inspection.bundleType << "\n"
        << "headline: " << inspection.headline << "\n"
        << "instrument_id: " << inspection.instrumentId << "\n"
        << "source_artifact_id: " << inspection.sourceArtifactId << "\n"
        << "source_report_id: " << inspection.sourceReportId << "\n"
        << "source_revision_id: " << inspection.sourceRevisionId << "\n"
        << "session_seq: [" << inspection.firstSessionSeq << ", " << inspection.lastSessionSeq << "]\n"
        << "payload_sha256: " << inspection.payloadSha256 << "\n"
        << "exported_ts_engine_ns: " << inspection.exportedTsEngineNs << "\n";
    if (inspection.sourceArtifact.is_object()) {
        out << "\nsource_artifact:\n" << inspection.sourceArtifact.dump(2) << "\n";
    }
    if (inspection.sourceReport.is_object()) {
        out << "\nsource_report:\n" << inspection.sourceReport.dump(2) << "\n";
    }
    if (inspection.reportSummary.is_object()) {
        out << "\nreport_summary:\n" << inspection.reportSummary.dump(2) << "\n";
    }
    if (!inspection.reportMarkdown.empty()) {
        out << "\nreport_markdown_preview:\n";
        constexpr std::size_t kMarkdownPreviewLimit = 900;
        if (inspection.reportMarkdown.size() > kMarkdownPreviewLimit) {
            out << inspection.reportMarkdown.substr(0, kMarkdownPreviewLimit) << "\n…";
        } else {
            out << inspection.reportMarkdown;
        }
        out << "\n";
    }
    return out.str();
}

BundlePreviewDetails BundlePreviewDetailsFromVerifyPayload(const tapescope::BundleVerifyPayload& payload) {
    BundlePreviewDetails details;
    details.bundleId = payload.bundleId;
    details.bundleType = payload.bundleType;
    details.sourceArtifactId = payload.sourceArtifact.value("artifact_id", std::string());
    details.payloadSha256 = payload.payloadSha256;
    details.firstSessionSeq = payload.artifact.value("first_session_seq", 0ULL);
    details.lastSessionSeq = payload.artifact.value("last_session_seq", 0ULL);
    return details;
}

std::string DescribeBundleVerifyPayload(const std::string& bundlePath,
                                        const tapescope::BundleVerifyPayload& payload) {
    std::ostringstream out;
    out << "bundle_path: " << (payload.bundlePath.empty() ? bundlePath : payload.bundlePath) << "\n"
        << "bundle_id: " << payload.bundleId << "\n"
        << "bundle_type: " << payload.bundleType << "\n"
        << "source_artifact_id: " << payload.sourceArtifact.value("artifact_id", std::string()) << "\n"
        << "source_report_id: " << payload.sourceReport.value("report_id", 0ULL) << "\n"
        << "session_seq: [" << payload.artifact.value("first_session_seq", 0ULL)
        << ", " << payload.artifact.value("last_session_seq", 0ULL) << "]\n"
        << "payload_sha256: " << payload.payloadSha256 << "\n"
        << "verify_status: " << payload.verifyStatus << "\n"
        << "import_supported: " << (payload.importSupported ? "true" : "false") << "\n"
        << "already_imported: " << (payload.alreadyImported ? "true" : "false") << "\n"
        << "can_import: " << (payload.canImport ? "true" : "false") << "\n"
        << "import_reason: " << payload.importReason << "\n"
        << "served_revision_id: " << payload.servedRevisionId << "\n";
    if (payload.hasImportedCase) {
        out << "\nimported_case:\n" << payload.importedCase.raw.dump(2) << "\n";
    }
    if (payload.sourceArtifact.is_object()) {
        out << "\nsource_artifact:\n" << payload.sourceArtifact.dump(2) << "\n";
    }
    if (payload.sourceReport.is_object()) {
        out << "\nsource_report:\n" << payload.sourceReport.dump(2) << "\n";
    }
    if (payload.reportSummary.is_object()) {
        out << "\nreport_summary:\n" << payload.reportSummary.dump(2) << "\n";
    }
    if (!payload.reportMarkdown.empty()) {
        out << "\nreport_markdown_preview:\n";
        constexpr std::size_t kMarkdownPreviewLimit = 900;
        if (payload.reportMarkdown.size() > kMarkdownPreviewLimit) {
            out << payload.reportMarkdown.substr(0, kMarkdownPreviewLimit) << "\n…";
        } else {
            out << payload.reportMarkdown;
        }
        out << "\n";
    }
    return out.str();
}

struct ImportedCasePreviewMatch {
    std::size_t index = 0;
    std::string reason;
};

std::optional<ImportedCasePreviewMatch> FindImportedCasePreviewMatch(
    const std::string& bundlePath,
    const BundlePreviewDetails& details,
    const std::vector<tapescope::ImportedCaseRow>& importedCases) {
    if (!bundlePath.empty()) {
        for (std::size_t index = 0; index < importedCases.size(); ++index) {
            if (importedCases[index].sourceBundlePath == bundlePath) {
                return ImportedCasePreviewMatch{index, "source_bundle_path"};
            }
        }
    }
    if (!details.payloadSha256.empty()) {
        for (std::size_t index = 0; index < importedCases.size(); ++index) {
            if (importedCases[index].payloadSha256 == details.payloadSha256) {
                return ImportedCasePreviewMatch{index, "payload_sha256"};
            }
        }
    }
    if (!details.bundleId.empty()) {
        for (std::size_t index = 0; index < importedCases.size(); ++index) {
            if (importedCases[index].bundleId == details.bundleId) {
                return ImportedCasePreviewMatch{index, "bundle_id"};
            }
        }
    }
    return std::nullopt;
}

NSColor* PreviewDecisionColor(const tapescope::BundlePreviewDecision& decision) {
    if (decision.importAllowed) {
        return [NSColor systemGreenColor];
    }
    if (decision.alreadyImported || !decision.importSupported) {
        return [NSColor systemOrangeColor];
    }
    return [NSColor systemRedColor];
}

NSString* PreviewDecisionStateText(const tapescope::BundlePreviewDecision& decision,
                                   bool localFallback) {
    std::string state = localFallback ? "Bundle preview ready (local fallback)." : "Bundle preview ready.";
    if (!decision.importAllowed) {
        state += " Import blocked.";
    }
    return ToNSString(state);
}

std::uint64_t ExtractArtifactReportId(const tapescope::InvestigationPayload& payload) {
    if (!payload.summary.is_object()) {
        return 0;
    }
    if (payload.artifactKind == "session_report") {
        return payload.summary.value("session_report", tapescope::json::object()).value("report_id", 0ULL);
    }
    if (payload.artifactKind == "case_report") {
        return payload.summary.value("case_report_artifact", tapescope::json::object()).value("report_id", 0ULL);
    }
    return 0;
}

std::string ExtractArtifactBundlePath(const tapescope::InvestigationPayload& payload) {
    if (!payload.summary.is_object()) {
        return {};
    }
    return payload.summary.value("bundle", tapescope::json::object()).value("bundle_path", std::string());
}

std::string ExtractSourceArtifactId(const tapescope::InvestigationPayload& payload) {
    if (!payload.summary.is_object()) {
        return {};
    }
    return payload.summary.value("source_artifact", tapescope::json::object()).value("artifact_id", std::string());
}

} // namespace

@interface TapeScopeWindowController (ArtifactPreviewState)

- (void)clearBundlePreviewState;
- (tapescope::BundlePreviewDecision)applyBundlePreviewStateForPath:(const std::string&)bundlePath
                                                           preview:(const std::string&)preview
                                                           details:(const BundlePreviewDetails&)details
                                                          decision:(tapescope::BundlePreviewDecision)decision;

@end

@implementation TapeScopeWindowController (ArtifactQueries)

- (void)clearBundlePreviewState {
    _previewedBundlePath.clear();
    _previewedBundleId.clear();
    _previewedBundleType.clear();
    _previewedBundleSourceArtifactId.clear();
    _previewedImportedArtifactId.clear();
    _previewedBundleFirstSessionSeq = 0;
    _previewedBundleLastSessionSeq = 0;
    _previewedBundleImportAllowed = NO;
    _previewedBundleImportStatus.clear();
    _previewedBundleImportDetail.clear();
    _bundlePreviewLoadRangeButton.enabled = NO;
    _bundlePreviewOpenSourceButton.enabled = NO;
    _bundlePreviewOpenImportedButton.enabled = NO;
    _bundleImportButton.enabled = NO;
}

- (tapescope::BundlePreviewDecision)applyBundlePreviewStateForPath:(const std::string&)bundlePath
                                                           preview:(const std::string&)preview
                                                           details:(const BundlePreviewDetails&)details
                                                          decision:(tapescope::BundlePreviewDecision)decision {
    std::string renderedPreview = preview;
    _previewedBundlePath = bundlePath;
    _previewedBundleId = details.bundleId;
    _previewedBundleType = details.bundleType;
    _previewedBundleSourceArtifactId = details.sourceArtifactId;
    _previewedBundleFirstSessionSeq = details.firstSessionSeq;
    _previewedBundleLastSessionSeq = details.lastSessionSeq;
    _bundlePreviewLoadRangeButton.enabled =
        (details.firstSessionSeq > 0 && details.lastSessionSeq >= details.firstSessionSeq);
    _bundlePreviewOpenSourceButton.enabled = !details.sourceArtifactId.empty();
    _previewedImportedArtifactId.clear();

    const auto match = FindImportedCasePreviewMatch(bundlePath, details, _latestImportedCases);

    if (match.has_value()) {
        const auto& row = _latestImportedCases.at(match->index);
        decision = tapescope::markBundlePreviewDecisionImported(decision, row, match->reason);
        _previewedImportedArtifactId = row.artifactId;
        _bundlePreviewOpenImportedButton.enabled = !row.artifactId.empty();
        renderedPreview += "\nimport_inventory_match:\n";
        renderedPreview += "  imported_case_id: " + std::to_string(row.importedCaseId) + "\n";
        renderedPreview += "  artifact_id: " + row.artifactId + "\n";
        renderedPreview += "  match_reason: " + match->reason + "\n";
        const NSInteger selected = _importedCaseTableView.selectedRow;
        if (selected != static_cast<NSInteger>(match->index)) {
            [_importedCaseTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:match->index]
                                byExtendingSelection:NO];
        }
    } else {
        _bundlePreviewOpenImportedButton.enabled = NO;
        renderedPreview += "\nimport_inventory_match: none\n";
    }
    renderedPreview += "\nimport_preview:\n";
    renderedPreview += tapescope::describeBundlePreviewDecision(decision);
    _previewedBundleImportAllowed = decision.importAllowed;
    _previewedBundleImportStatus = decision.status;
    _previewedBundleImportDetail = decision.detail;
    _bundleImportButton.enabled = decision.importAllowed;
    _bundlePreviewTextView.string = ToNSString(renderedPreview);
    return decision;
}

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

- (void)fetchIncidentEnrichment:(id)sender {
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
    _incidentEnrichButton.enabled = NO;
    _incidentExplainButton.enabled = NO;
    _incidentRefreshContextButton.enabled = NO;
    _incidentOpenSelectedButton.enabled = NO;
    const std::uint64_t token = _incidentPane->beginRequest(@"Building fast incident enrichment…");

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_interactiveQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->enrichIncidentPayload(logicalIncidentId);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil || !innerSelf->_incidentPane->isCurrent(token)) {
                return;
            }
            innerSelf->_incidentInFlight = NO;
            innerSelf->_incidentFetchButton.enabled = YES;
            innerSelf->_incidentRefreshButton.enabled = YES;
            innerSelf->_incidentEnrichButton.enabled = YES;
            innerSelf->_incidentExplainButton.enabled = YES;
            innerSelf->_incidentRefreshContextButton.enabled = YES;
            innerSelf->_incidentOpenSelectedButton.enabled = (innerSelf->_incidentTableView.selectedRow >= 0);
            [innerSelf applyEnrichmentResult:result
                              paneController:innerSelf->_incidentPane.get()
                                 successText:@"Incident enrichment loaded."
                            syncArtifactField:YES];
            if (result.ok()) {
                [innerSelf recordRecentHistoryForKind:"incident"
                                             targetId:std::to_string(logicalIncidentId)
                                              payload:result.value.localEvidence
                                        fallbackTitle:"Incident " + std::to_string(logicalIncidentId)
                                       fallbackDetail:"Reopen the incident drilldown."
                                             metadata:tapescope::json{{"logical_incident_id", logicalIncidentId}}];
            }
            innerSelf->_incidentTextView.string =
                ToNSString(DescribeEnrichmentPayload("incident_enrichment", "logical_incident_id=" + std::to_string(logicalIncidentId), result));
        });
    });
}

- (void)fetchIncidentExplanation:(id)sender {
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
    _incidentEnrichButton.enabled = NO;
    _incidentExplainButton.enabled = NO;
    _incidentRefreshContextButton.enabled = NO;
    _incidentOpenSelectedButton.enabled = NO;
    const std::uint64_t token = _incidentPane->beginRequest(@"Building deep incident explanation…");

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_interactiveQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->explainIncidentPayload(logicalIncidentId);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil || !innerSelf->_incidentPane->isCurrent(token)) {
                return;
            }
            innerSelf->_incidentInFlight = NO;
            innerSelf->_incidentFetchButton.enabled = YES;
            innerSelf->_incidentRefreshButton.enabled = YES;
            innerSelf->_incidentEnrichButton.enabled = YES;
            innerSelf->_incidentExplainButton.enabled = YES;
            innerSelf->_incidentRefreshContextButton.enabled = YES;
            innerSelf->_incidentOpenSelectedButton.enabled = (innerSelf->_incidentTableView.selectedRow >= 0);
            [innerSelf applyEnrichmentResult:result
                              paneController:innerSelf->_incidentPane.get()
                                 successText:@"Incident explanation loaded."
                            syncArtifactField:YES];
            if (result.ok()) {
                [innerSelf recordRecentHistoryForKind:"incident"
                                             targetId:std::to_string(logicalIncidentId)
                                              payload:result.value.localEvidence
                                        fallbackTitle:"Incident " + std::to_string(logicalIncidentId)
                                       fallbackDetail:"Reopen the incident drilldown."
                                             metadata:tapescope::json{{"logical_incident_id", logicalIncidentId}}];
            }
            innerSelf->_incidentTextView.string =
                ToNSString(DescribeEnrichmentPayload("incident_explanation", "logical_incident_id=" + std::to_string(logicalIncidentId), result));
        });
    });
}

- (void)refreshIncidentExternalContext:(id)sender {
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
    _incidentEnrichButton.enabled = NO;
    _incidentExplainButton.enabled = NO;
    _incidentRefreshContextButton.enabled = NO;
    _incidentOpenSelectedButton.enabled = NO;
    const std::uint64_t token = _incidentPane->beginRequest(@"Refreshing incident external context…");

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_interactiveQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->refreshIncidentExternalContextPayload(logicalIncidentId);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil || !innerSelf->_incidentPane->isCurrent(token)) {
                return;
            }
            innerSelf->_incidentInFlight = NO;
            innerSelf->_incidentFetchButton.enabled = YES;
            innerSelf->_incidentRefreshButton.enabled = YES;
            innerSelf->_incidentEnrichButton.enabled = YES;
            innerSelf->_incidentExplainButton.enabled = YES;
            innerSelf->_incidentRefreshContextButton.enabled = YES;
            innerSelf->_incidentOpenSelectedButton.enabled = (innerSelf->_incidentTableView.selectedRow >= 0);
            [innerSelf applyEnrichmentResult:result
                              paneController:innerSelf->_incidentPane.get()
                                 successText:@"Incident external context refreshed."
                            syncArtifactField:YES];
            innerSelf->_incidentTextView.string =
                ToNSString(DescribeEnrichmentPayload("incident_refresh", "logical_incident_id=" + std::to_string(logicalIncidentId), result));
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
    _artifactExportBundleButton.enabled = NO;
    _artifactRevealBundleButton.enabled = NO;
    _artifactOpenSourceButton.enabled = NO;
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
                innerSelf->_loadedArtifactReportId = ExtractArtifactReportId(result.value);
                innerSelf->_loadedArtifactKind = result.value.artifactKind;
                innerSelf->_loadedArtifactBundlePath = ExtractArtifactBundlePath(result.value);
                innerSelf->_loadedArtifactSourceArtifactId = ExtractSourceArtifactId(result.value);
                innerSelf->_artifactExportBundleButton.enabled =
                    (innerSelf->_loadedArtifactReportId > 0 &&
                     (innerSelf->_loadedArtifactKind == "session_report" ||
                      innerSelf->_loadedArtifactKind == "case_report"));
                innerSelf->_artifactRevealBundleButton.enabled = !innerSelf->_loadedArtifactBundlePath.empty();
                innerSelf->_artifactOpenSourceButton.enabled = !innerSelf->_loadedArtifactSourceArtifactId.empty();
                [innerSelf recordRecentHistoryForKind:"artifact"
                                             targetId:artifactId
                                              payload:result.value
                                        fallbackTitle:"Artifact " + artifactId
                                       fallbackDetail:"Reopen the durable artifact envelope."
                                             metadata:tapescope::json{{"artifact_id", artifactId}}];
            } else {
                innerSelf->_loadedArtifactReportId = 0;
                innerSelf->_loadedArtifactKind.clear();
                innerSelf->_loadedArtifactBundlePath.clear();
                innerSelf->_loadedArtifactSourceArtifactId.clear();
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
    _artifactExportBundleButton.enabled = NO;
    _artifactRevealBundleButton.enabled = NO;
    _artifactOpenSourceButton.enabled = NO;
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
        const auto result = strongSelf->_client->exportArtifactPayload(artifactId, format);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil || ![innerSelf isRequestTokenCurrent:token storage:&innerSelf->_artifactExportRequestToken]) {
                return;
            }
            innerSelf->_artifactInFlight = NO;
            innerSelf->_artifactFetchButton.enabled = YES;
            innerSelf->_artifactExportButton.enabled = YES;
            innerSelf->_artifactExportBundleButton.enabled =
                (innerSelf->_loadedArtifactReportId > 0 &&
                 (innerSelf->_loadedArtifactKind == "session_report" ||
                  innerSelf->_loadedArtifactKind == "case_report"));
            innerSelf->_artifactRevealBundleButton.enabled = !innerSelf->_loadedArtifactBundlePath.empty();
            innerSelf->_artifactOpenSourceButton.enabled = !innerSelf->_loadedArtifactSourceArtifactId.empty();
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

- (void)exportLoadedArtifactBundle:(id)sender {
    (void)sender;
    if (_artifactInFlight || !_client) {
        return;
    }
    if (_loadedArtifactReportId == 0 ||
        (_loadedArtifactKind != "session_report" && _loadedArtifactKind != "case_report")) {
        _artifactStateLabel.stringValue = @"Loaded artifact is not a bundle-exportable session/case report.";
        _artifactStateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    _artifactInFlight = YES;
    const std::uint64_t token = [self issueRequestToken:&_artifactExportRequestToken];
    _artifactFetchButton.enabled = NO;
    _artifactExportButton.enabled = NO;
    _artifactExportBundleButton.enabled = NO;
    _artifactRevealBundleButton.enabled = NO;
    _artifactOpenSourceButton.enabled = NO;
    _artifactStateLabel.stringValue = @"Exporting portable bundle…";
    _artifactStateLabel.textColor = [NSColor systemOrangeColor];
    _artifactTextView.string = @"Exporting the loaded durable report as a portable Phase 6 bundle…";

    const bool isSessionReport = (_loadedArtifactKind == "session_report");
    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_artifactQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = isSessionReport
            ? strongSelf->_client->exportSessionBundlePayload(strongSelf->_loadedArtifactReportId)
            : strongSelf->_client->exportCaseBundlePayload(strongSelf->_loadedArtifactReportId);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil || ![innerSelf isRequestTokenCurrent:token storage:&innerSelf->_artifactExportRequestToken]) {
                return;
            }
            innerSelf->_artifactInFlight = NO;
            innerSelf->_artifactFetchButton.enabled = YES;
            innerSelf->_artifactExportButton.enabled = YES;
            innerSelf->_artifactExportBundleButton.enabled =
                (innerSelf->_loadedArtifactReportId > 0 &&
                 (innerSelf->_loadedArtifactKind == "session_report" ||
                  innerSelf->_loadedArtifactKind == "case_report"));
            if (result.ok()) {
                innerSelf->_loadedArtifactBundlePath = result.value.bundlePath;
                innerSelf->_artifactRevealBundleButton.enabled = YES;
                innerSelf->_artifactStateLabel.stringValue = @"Portable bundle exported.";
                innerSelf->_artifactStateLabel.textColor = [NSColor systemGreenColor];
                innerSelf->_bundleImportPathField.stringValue = ToNSString(result.value.bundlePath);
                [innerSelf recordBundleHistoryEntry:tapescope::json{
                    {"kind", isSessionReport ? "session_bundle" : "case_bundle"},
                    {"target_id", result.value.bundleId},
                    {"bundle_id", result.value.bundleId},
                    {"headline", isSessionReport ? std::string("Session bundle export") : std::string("Case bundle export")},
                    {"detail", result.value.bundlePath},
                    {"artifact_id", result.value.artifactId},
                    {"source_artifact_id", result.value.artifactId},
                    {"bundle_path", result.value.bundlePath},
                    {"first_session_seq", result.value.artifact.value("first_session_seq", 0ULL)},
                    {"last_session_seq", result.value.artifact.value("last_session_seq", 0ULL)},
                    {"served_revision_id", result.value.servedRevisionId}
                }];
                [innerSelf recordRecentHistoryEntry:tapescope::json{
                    {"kind", "bundle"},
                    {"target_id", result.value.bundleId},
                    {"headline", isSessionReport ? std::string("Session bundle export") : std::string("Case bundle export")},
                    {"detail", result.value.bundlePath},
                    {"artifact_id", result.value.artifactId},
                    {"source_artifact_id", result.value.artifactId},
                    {"bundle_id", result.value.bundleId},
                    {"bundle_path", result.value.bundlePath}
                }];
                [innerSelf previewBundlePath:nil];
            } else {
                innerSelf->_artifactRevealBundleButton.enabled = !innerSelf->_loadedArtifactBundlePath.empty();
                innerSelf->_artifactStateLabel.stringValue = ToNSString(tapescope::QueryClient::describeError(result.error));
                innerSelf->_artifactStateLabel.textColor = ErrorColorForKind(result.error.kind);
            }
            innerSelf->_artifactOpenSourceButton.enabled = !innerSelf->_loadedArtifactSourceArtifactId.empty();
            innerSelf->_artifactTextView.string = ToNSString(
                DescribeBundleExportPayload(isSessionReport ? "Artifact session bundle" : "Artifact case bundle",
                                            innerSelf->_loadedArtifactReportId,
                                            result));
        });
    });
}

- (void)revealLoadedArtifactBundle:(id)sender {
    (void)sender;
    if (_loadedArtifactBundlePath.empty()) {
        _artifactStateLabel.stringValue = @"Loaded artifact does not have an on-disk bundle path.";
        _artifactStateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    _bundleImportPathField.stringValue = ToNSString(_loadedArtifactBundlePath);
    [self revealSelectedBundlePath:nil];
    _artifactStateLabel.stringValue = _reportInventoryStateLabel.stringValue;
    _artifactStateLabel.textColor = _reportInventoryStateLabel.textColor;
}

- (void)openLoadedArtifactSource:(id)sender {
    (void)sender;
    if (_loadedArtifactSourceArtifactId.empty()) {
        _artifactStateLabel.stringValue = @"Loaded artifact does not have a source artifact.";
        _artifactStateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    _artifactIdField.stringValue = ToNSString(_loadedArtifactSourceArtifactId);
    [self fetchArtifact:nil];
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
    _reportInventoryExportSessionBundleButton.enabled = NO;
    _reportInventoryOpenCaseButton.enabled = NO;
    _reportInventoryExportCaseBundleButton.enabled = NO;
    _reportInventoryOpenImportedButton.enabled = NO;
    _reportInventoryLoadImportedRangeButton.enabled = NO;
    _reportInventoryOpenImportedSourceButton.enabled = NO;
    _reportInventoryStateLabel.stringValue = @"Refreshing report inventory…";
    _reportInventoryStateLabel.textColor = [NSColor systemOrangeColor];
    _reportInventoryTextView.string = @"Refreshing session reports, case reports, and imported case bundles…";

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_artifactQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto sessionReports = strongSelf->_client->listSessionReportsPayload(20);
        const auto caseReports = strongSelf->_client->listCaseReportsPayload(20);
        const auto importedCases = strongSelf->_client->listImportedCasesPayload(20);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil || ![innerSelf isRequestTokenCurrent:token storage:&innerSelf->_reportInventoryRequestToken]) {
                return;
            }
            innerSelf->_reportInventoryInFlight = NO;
            innerSelf->_reportInventoryRefreshButton.enabled = YES;

            innerSelf->_latestSessionReports.clear();
            innerSelf->_latestCaseReports.clear();
            innerSelf->_latestImportedCases.clear();
            if (sessionReports.ok()) {
                innerSelf->_latestSessionReports = sessionReports.value.sessionReports;
            }
            if (caseReports.ok()) {
                innerSelf->_latestCaseReports = caseReports.value.caseReports;
            }
            if (importedCases.ok()) {
                innerSelf->_latestImportedCases = importedCases.value.importedCases;
            }
            [innerSelf->_sessionReportTableView reloadData];
            [innerSelf->_caseReportTableView reloadData];
            [innerSelf->_importedCaseTableView reloadData];
            if (!innerSelf->_latestSessionReports.empty()) {
                [innerSelf->_sessionReportTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0] byExtendingSelection:NO];
            }
            if (!innerSelf->_latestCaseReports.empty()) {
                [innerSelf->_caseReportTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0] byExtendingSelection:NO];
            }
            if (!innerSelf->_latestImportedCases.empty()) {
                [innerSelf->_importedCaseTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0] byExtendingSelection:NO];
            }
            innerSelf->_reportInventoryOpenSessionButton.enabled =
                (innerSelf->_sessionReportTableView.selectedRow >= 0);
            innerSelf->_reportInventoryExportSessionBundleButton.enabled =
                (innerSelf->_sessionReportTableView.selectedRow >= 0);
            innerSelf->_reportInventoryOpenCaseButton.enabled =
                (innerSelf->_caseReportTableView.selectedRow >= 0);
            innerSelf->_reportInventoryExportCaseBundleButton.enabled =
                (innerSelf->_caseReportTableView.selectedRow >= 0);
            innerSelf->_reportInventoryOpenImportedButton.enabled =
                (innerSelf->_importedCaseTableView.selectedRow >= 0);
            innerSelf->_reportInventoryLoadImportedRangeButton.enabled =
                (innerSelf->_importedCaseTableView.selectedRow >= 0);
            innerSelf->_reportInventoryOpenImportedSourceButton.enabled =
                (innerSelf->_importedCaseTableView.selectedRow >= 0);

            if (sessionReports.ok() || caseReports.ok() || importedCases.ok()) {
                if (!innerSelf->_latestSessionReports.empty() ||
                    !innerSelf->_latestCaseReports.empty() ||
                    !innerSelf->_latestImportedCases.empty()) {
                    innerSelf->_reportInventoryStateLabel.stringValue = @"Report inventory loaded.";
                    innerSelf->_reportInventoryStateLabel.textColor = [NSColor systemGreenColor];
                } else {
                    innerSelf->_reportInventoryStateLabel.stringValue = @"No durable reports or imported bundles are available yet.";
                    innerSelf->_reportInventoryStateLabel.textColor = TapeInkMutedColor();
                }
            } else {
                innerSelf->_reportInventoryStateLabel.stringValue = @"Report inventory queries failed.";
                innerSelf->_reportInventoryStateLabel.textColor = [NSColor systemRedColor];
            }
            if (innerSelf->_latestSessionReports.empty() &&
                innerSelf->_latestCaseReports.empty() &&
                innerSelf->_latestImportedCases.empty()) {
                innerSelf->_reportInventoryTextView.string = ToNSString(
                    DescribeReportInventoryResult(sessionReports, caseReports) + "\n\n" +
                    DescribeImportedCaseListPayload(importedCases));
            }
            [innerSelf refreshReportInventoryDetailText];
        });
    });
}

- (void)refreshReportInventoryDetailText {
    std::ostringstream summary;
    if (!_lastBundleWorkflowSummary.empty()) {
        summary << _lastBundleWorkflowSummary;
    } else {
        summary << "No bundle export or import has been performed in this TapeScope session yet.\n";
    }

    const NSInteger sessionSelected = _sessionReportTableView.selectedRow;
    if (sessionSelected >= 0 && static_cast<std::size_t>(sessionSelected) < _latestSessionReports.size()) {
        summary << "\nSelected session report:\n"
                << _latestSessionReports.at(static_cast<std::size_t>(sessionSelected)).raw.dump(2) << "\n";
    }

    const NSInteger caseSelected = _caseReportTableView.selectedRow;
    if (caseSelected >= 0 && static_cast<std::size_t>(caseSelected) < _latestCaseReports.size()) {
        summary << "\nSelected case report:\n"
                << _latestCaseReports.at(static_cast<std::size_t>(caseSelected)).raw.dump(2) << "\n";
    }

    _reportInventoryTextView.string = ToNSString(summary.str());

    const NSInteger importedSelected = _importedCaseTableView.selectedRow;
    if (importedSelected >= 0 && static_cast<std::size_t>(importedSelected) < _latestImportedCases.size()) {
        const auto& row = _latestImportedCases.at(static_cast<std::size_t>(importedSelected));
        _importedCaseTextView.string = ToNSString(DescribeImportedCaseRow(row));
        if (!row.sourceBundlePath.empty()) {
            _bundleImportPathField.stringValue = ToNSString(row.sourceBundlePath);
            tape_bundle::PortableBundleInspection inspection;
            std::string inspectError;
            if (tape_bundle::inspectPortableBundle(row.sourceBundlePath, &inspection, &inspectError)) {
                const BundlePreviewDetails details = BundlePreviewDetailsFromInspection(inspection);
                [self applyBundlePreviewStateForPath:row.sourceBundlePath
                                             preview:DescribeBundlePreview(inspection)
                                             details:details
                                            decision:tapescope::bundlePreviewDecisionFromInspection(inspection)];
            } else {
                [self clearBundlePreviewState];
                _bundlePreviewTextView.string = ToNSString(tapescope::describeBundlePreviewFailure(inspectError));
            }
        }
    } else {
        _importedCaseTextView.string = @"Select an imported case row to inspect import diagnostics, source artifact linkage, and replay boundaries.";
    }
}

- (void)exportSelectedSessionBundle:(id)sender {
    (void)sender;
    if (_bundleWorkflowInFlight || !_client) {
        return;
    }
    const NSInteger selected = _sessionReportTableView.selectedRow;
    if (selected < 0 || static_cast<std::size_t>(selected) >= _latestSessionReports.size()) {
        _reportInventoryStateLabel.stringValue = @"Select a session report row first.";
        _reportInventoryStateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    const auto row = _latestSessionReports.at(static_cast<std::size_t>(selected));
    if (row.reportId == 0) {
        _reportInventoryStateLabel.stringValue = @"Selected session report is missing report_id.";
        _reportInventoryStateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    _bundleWorkflowInFlight = YES;
    const std::uint64_t token = [self issueRequestToken:&_bundleWorkflowRequestToken];
    _reportInventoryExportSessionBundleButton.enabled = NO;
    _reportInventoryExportCaseBundleButton.enabled = NO;
    _bundleImportButton.enabled = NO;
    _bundleChooseImportButton.enabled = NO;
    _bundlePreviewButton.enabled = NO;
    _reportInventoryStateLabel.stringValue = @"Exporting session bundle…";
    _reportInventoryStateLabel.textColor = [NSColor systemOrangeColor];
    _reportInventoryTextView.string = @"Exporting selected session report as a portable Phase 6 bundle…";

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_artifactQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->exportSessionBundlePayload(row.reportId);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil || ![innerSelf isRequestTokenCurrent:token storage:&innerSelf->_bundleWorkflowRequestToken]) {
                return;
            }
            innerSelf->_bundleWorkflowInFlight = NO;
            innerSelf->_reportInventoryExportSessionBundleButton.enabled = (innerSelf->_sessionReportTableView.selectedRow >= 0);
            innerSelf->_reportInventoryExportCaseBundleButton.enabled = (innerSelf->_caseReportTableView.selectedRow >= 0);
            innerSelf->_bundleImportButton.enabled = YES;
            innerSelf->_bundleChooseImportButton.enabled = YES;
            innerSelf->_bundlePreviewButton.enabled = YES;
            if (result.ok()) {
                innerSelf->_reportInventoryStateLabel.stringValue = @"Session bundle exported.";
                innerSelf->_reportInventoryStateLabel.textColor = [NSColor systemGreenColor];
                innerSelf->_bundleImportPathField.stringValue = ToNSString(result.value.bundlePath);
                innerSelf->_lastBundleWorkflowSummary =
                    DescribeBundleExportPayload("Session bundle", row.reportId, result);
                [innerSelf recordRecentHistoryEntry:tapescope::json{
                    {"kind", "bundle"},
                    {"target_id", result.value.bundleId},
                    {"headline", "Session bundle export"},
                    {"detail", result.value.bundlePath},
                    {"artifact_id", result.value.artifactId},
                    {"source_artifact_id", result.value.artifactId},
                    {"bundle_id", result.value.bundleId},
                    {"bundle_path", result.value.bundlePath}
                }];
                [innerSelf recordBundleHistoryEntry:tapescope::json{
                    {"kind", "session_bundle"},
                    {"target_id", result.value.bundleId},
                    {"bundle_id", result.value.bundleId},
                    {"headline", "Session bundle export"},
                    {"detail", result.value.bundlePath},
                    {"artifact_id", result.value.artifactId},
                    {"source_artifact_id", result.value.artifactId},
                    {"bundle_path", result.value.bundlePath},
                    {"first_session_seq", result.value.artifact.value("first_session_seq", 0ULL)},
                    {"last_session_seq", result.value.artifact.value("last_session_seq", 0ULL)},
                    {"served_revision_id", result.value.servedRevisionId}
                }];
                [innerSelf previewBundlePath:nil];
            } else {
                innerSelf->_reportInventoryStateLabel.stringValue = ToNSString(tapescope::QueryClient::describeError(result.error));
                innerSelf->_reportInventoryStateLabel.textColor = ErrorColorForKind(result.error.kind);
                innerSelf->_lastBundleWorkflowSummary =
                    DescribeBundleExportPayload("Session bundle", row.reportId, result);
            }
            [innerSelf refreshReportInventoryDetailText];
        });
    });
}

- (void)exportSelectedCaseBundle:(id)sender {
    (void)sender;
    if (_bundleWorkflowInFlight || !_client) {
        return;
    }
    const NSInteger selected = _caseReportTableView.selectedRow;
    if (selected < 0 || static_cast<std::size_t>(selected) >= _latestCaseReports.size()) {
        _reportInventoryStateLabel.stringValue = @"Select a case report row first.";
        _reportInventoryStateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    const auto row = _latestCaseReports.at(static_cast<std::size_t>(selected));
    if (row.reportId == 0) {
        _reportInventoryStateLabel.stringValue = @"Selected case report is missing report_id.";
        _reportInventoryStateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    _bundleWorkflowInFlight = YES;
    const std::uint64_t token = [self issueRequestToken:&_bundleWorkflowRequestToken];
    _reportInventoryExportSessionBundleButton.enabled = NO;
    _reportInventoryExportCaseBundleButton.enabled = NO;
    _bundleImportButton.enabled = NO;
    _bundleChooseImportButton.enabled = NO;
    _bundlePreviewButton.enabled = NO;
    _reportInventoryStateLabel.stringValue = @"Exporting case bundle…";
    _reportInventoryStateLabel.textColor = [NSColor systemOrangeColor];
    _reportInventoryTextView.string = @"Exporting selected case report as a portable Phase 6 bundle…";

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_artifactQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->exportCaseBundlePayload(row.reportId);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil || ![innerSelf isRequestTokenCurrent:token storage:&innerSelf->_bundleWorkflowRequestToken]) {
                return;
            }
            innerSelf->_bundleWorkflowInFlight = NO;
            innerSelf->_reportInventoryExportSessionBundleButton.enabled = (innerSelf->_sessionReportTableView.selectedRow >= 0);
            innerSelf->_reportInventoryExportCaseBundleButton.enabled = (innerSelf->_caseReportTableView.selectedRow >= 0);
            innerSelf->_bundleImportButton.enabled = YES;
            innerSelf->_bundleChooseImportButton.enabled = YES;
            innerSelf->_bundlePreviewButton.enabled = YES;
            if (result.ok()) {
                innerSelf->_reportInventoryStateLabel.stringValue = @"Case bundle exported.";
                innerSelf->_reportInventoryStateLabel.textColor = [NSColor systemGreenColor];
                innerSelf->_bundleImportPathField.stringValue = ToNSString(result.value.bundlePath);
                innerSelf->_lastBundleWorkflowSummary =
                    DescribeBundleExportPayload("Case bundle", row.reportId, result);
                [innerSelf recordRecentHistoryEntry:tapescope::json{
                    {"kind", "bundle"},
                    {"target_id", result.value.bundleId},
                    {"headline", "Case bundle export"},
                    {"detail", result.value.bundlePath},
                    {"artifact_id", result.value.artifactId},
                    {"source_artifact_id", result.value.artifactId},
                    {"bundle_id", result.value.bundleId},
                    {"bundle_path", result.value.bundlePath}
                }];
                [innerSelf recordBundleHistoryEntry:tapescope::json{
                    {"kind", "case_bundle"},
                    {"target_id", result.value.bundleId},
                    {"bundle_id", result.value.bundleId},
                    {"headline", "Case bundle export"},
                    {"detail", result.value.bundlePath},
                    {"artifact_id", result.value.artifactId},
                    {"source_artifact_id", result.value.artifactId},
                    {"bundle_path", result.value.bundlePath},
                    {"first_session_seq", result.value.artifact.value("first_session_seq", 0ULL)},
                    {"last_session_seq", result.value.artifact.value("last_session_seq", 0ULL)},
                    {"served_revision_id", result.value.servedRevisionId}
                }];
                [innerSelf previewBundlePath:nil];
            } else {
                innerSelf->_reportInventoryStateLabel.stringValue = ToNSString(tapescope::QueryClient::describeError(result.error));
                innerSelf->_reportInventoryStateLabel.textColor = ErrorColorForKind(result.error.kind);
                innerSelf->_lastBundleWorkflowSummary =
                    DescribeBundleExportPayload("Case bundle", row.reportId, result);
            }
            [innerSelf refreshReportInventoryDetailText];
        });
    });
}

- (void)chooseImportBundlePath:(id)sender {
    (void)sender;
    NSOpenPanel* panel = [NSOpenPanel openPanel];
    panel.canChooseFiles = YES;
    panel.canChooseDirectories = NO;
    panel.allowsMultipleSelection = NO;
    panel.message = @"Choose a portable Phase 6 case bundle to import.";
    if ([panel runModal] == NSModalResponseOK && panel.URL != nil && panel.URL.path != nil) {
        _bundleImportPathField.stringValue = panel.URL.path;
        [self previewBundlePath:nil];
    }
}

- (void)importSelectedBundlePath:(id)sender {
    (void)sender;
    if (_bundleWorkflowInFlight || !_client) {
        return;
    }
    const std::string bundlePath = TrimAscii(ToStdString(_bundleImportPathField.stringValue));
    if (bundlePath.empty()) {
        _reportInventoryStateLabel.stringValue = @"Choose a bundle_path first.";
        _reportInventoryStateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    _bundleWorkflowInFlight = YES;
    const std::uint64_t token = [self issueRequestToken:&_bundleWorkflowRequestToken];
    _reportInventoryExportSessionBundleButton.enabled = NO;
    _reportInventoryExportCaseBundleButton.enabled = NO;
    _bundleImportButton.enabled = NO;
    _bundleChooseImportButton.enabled = NO;
    _bundlePreviewButton.enabled = NO;
    _reportInventoryOpenImportedButton.enabled = NO;
    _reportInventoryStateLabel.stringValue = @"Importing case bundle…";
    _reportInventoryStateLabel.textColor = [NSColor systemOrangeColor];
    _reportInventoryTextView.string = @"Importing portable case bundle into the local engine inventory…";

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_artifactQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->importCaseBundlePayload(bundlePath);
        const auto importedCases = strongSelf->_client->listImportedCasesPayload(20);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil || ![innerSelf isRequestTokenCurrent:token storage:&innerSelf->_bundleWorkflowRequestToken]) {
                return;
            }
            innerSelf->_bundleWorkflowInFlight = NO;
            innerSelf->_reportInventoryExportSessionBundleButton.enabled = (innerSelf->_sessionReportTableView.selectedRow >= 0);
            innerSelf->_reportInventoryExportCaseBundleButton.enabled = (innerSelf->_caseReportTableView.selectedRow >= 0);
            innerSelf->_bundleImportButton.enabled = YES;
            innerSelf->_bundleChooseImportButton.enabled = YES;
            innerSelf->_bundlePreviewButton.enabled = YES;
            if (result.ok()) {
                innerSelf->_reportInventoryStateLabel.stringValue = @"Case bundle imported.";
                innerSelf->_reportInventoryStateLabel.textColor = [NSColor systemGreenColor];
                innerSelf->_lastBundleWorkflowSummary = DescribeCaseBundleImportPayload(bundlePath, result);
                [innerSelf recordRecentHistoryEntry:tapescope::json{
                    {"kind", "artifact"},
                    {"target_id", result.value.artifactId},
                    {"headline", result.value.importedCase.headline.empty() ? std::string("Imported case bundle")
                                                                              : result.value.importedCase.headline},
                    {"detail", bundlePath},
                    {"artifact_id", result.value.artifactId},
                    {"bundle_path", bundlePath}
                }];
                [innerSelf recordBundleHistoryEntry:tapescope::json{
                    {"kind", "imported_case_bundle"},
                    {"target_id", result.value.importedCase.bundleId.empty() ? result.value.artifactId : result.value.importedCase.bundleId},
                    {"bundle_id", result.value.importedCase.bundleId},
                    {"headline", result.value.importedCase.headline.empty() ? std::string("Imported case bundle")
                                                                             : result.value.importedCase.headline},
                    {"detail", bundlePath},
                    {"artifact_id", result.value.artifactId},
                    {"source_artifact_id", result.value.importedCase.sourceArtifactId},
                    {"bundle_path", bundlePath},
                    {"first_session_seq", result.value.importedCase.firstSessionSeq},
                    {"last_session_seq", result.value.importedCase.lastSessionSeq},
                    {"served_revision_id", result.value.importedCase.sourceRevisionId}
                }];
                [innerSelf previewBundlePath:nil];
            } else {
                innerSelf->_reportInventoryStateLabel.stringValue = ToNSString(tapescope::QueryClient::describeError(result.error));
                innerSelf->_reportInventoryStateLabel.textColor = ErrorColorForKind(result.error.kind);
                innerSelf->_lastBundleWorkflowSummary = DescribeCaseBundleImportPayload(bundlePath, result);
            }
            if (importedCases.ok()) {
                innerSelf->_latestImportedCases = importedCases.value.importedCases;
                [innerSelf->_importedCaseTableView reloadData];
                std::size_t selectedIndex = 0;
                if (result.ok()) {
                    for (std::size_t index = 0; index < innerSelf->_latestImportedCases.size(); ++index) {
                        if (innerSelf->_latestImportedCases[index].artifactId == result.value.artifactId) {
                            selectedIndex = index;
                            break;
                        }
                    }
                }
                if (!innerSelf->_latestImportedCases.empty()) {
                    [innerSelf->_importedCaseTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:selectedIndex] byExtendingSelection:NO];
                }
            }
            innerSelf->_reportInventoryOpenImportedButton.enabled = (innerSelf->_importedCaseTableView.selectedRow >= 0);
            innerSelf->_reportInventoryLoadImportedRangeButton.enabled = (innerSelf->_importedCaseTableView.selectedRow >= 0);
            innerSelf->_reportInventoryOpenImportedSourceButton.enabled =
                (innerSelf->_importedCaseTableView.selectedRow >= 0);
            [innerSelf refreshReportInventoryDetailText];
        });
    });
}

- (void)previewBundlePath:(id)sender {
    (void)sender;
    const std::string bundlePath = TrimAscii(ToStdString(_bundleImportPathField.stringValue));
    if (bundlePath.empty()) {
        [self clearBundlePreviewState];
        _reportInventoryStateLabel.stringValue = @"Choose a bundle_path first.";
        _reportInventoryStateLabel.textColor = [NSColor systemRedColor];
        _bundlePreviewTextView.string = @"Choose or generate a Phase 6 bundle path, then preview it here before import.";
        return;
    }

    _bundleWorkflowInFlight = YES;
    const std::uint64_t token = [self issueRequestToken:&_bundleWorkflowRequestToken];
    _bundlePreviewButton.enabled = NO;
    _bundleImportButton.enabled = NO;
    _bundleChooseImportButton.enabled = NO;
    _reportInventoryStateLabel.stringValue = @"Previewing bundle…";
    _reportInventoryStateLabel.textColor = [NSColor systemOrangeColor];

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_artifactQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        std::string preview;
        BundlePreviewDetails details;
        tapescope::BundlePreviewDecision decision;
        bool previewReady = false;
        bool localFallback = false;

        if (strongSelf != nil && strongSelf->_client) {
            const auto verifyResult = strongSelf->_client->verifyBundlePayload(bundlePath);
            if (verifyResult.ok()) {
                details = BundlePreviewDetailsFromVerifyPayload(verifyResult.value);
                preview = DescribeBundleVerifyPayload(bundlePath, verifyResult.value);
                decision = tapescope::bundlePreviewDecisionFromVerifyPayload(verifyResult.value);
                previewReady = true;
            } else if (ShouldFallbackToLocalBundlePreview(verifyResult.error)) {
                localFallback = true;
                preview = "bundle_preview_source: local_fallback\nfallback_reason: " +
                          tapescope::QueryClient::describeError(verifyResult.error) + "\n\n";
                tape_bundle::PortableBundleInspection inspection;
                std::string inspectError;
                if (tape_bundle::inspectPortableBundle(bundlePath, &inspection, &inspectError)) {
                    details = BundlePreviewDetailsFromInspection(inspection);
                    preview += DescribeBundlePreview(inspection);
                    decision = tapescope::bundlePreviewDecisionFromInspection(inspection);
                    previewReady = true;
                } else {
                    preview += tapescope::describeBundlePreviewFailure(inspectError);
                }
            } else {
                preview = "Bundle verification failed for " + bundlePath + "\n" +
                          tapescope::QueryClient::describeError(verifyResult.error);
            }
        } else {
            tape_bundle::PortableBundleInspection inspection;
            std::string inspectError;
            if (tape_bundle::inspectPortableBundle(bundlePath, &inspection, &inspectError)) {
                details = BundlePreviewDetailsFromInspection(inspection);
                preview = DescribeBundlePreview(inspection);
                decision = tapescope::bundlePreviewDecisionFromInspection(inspection);
                previewReady = true;
            } else {
                preview = tapescope::describeBundlePreviewFailure(inspectError);
            }
        }
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil || ![innerSelf isRequestTokenCurrent:token storage:&innerSelf->_bundleWorkflowRequestToken]) {
                return;
            }
            innerSelf->_bundleWorkflowInFlight = NO;
            innerSelf->_bundlePreviewButton.enabled = YES;
            innerSelf->_bundleChooseImportButton.enabled = YES;
            if (!previewReady) {
                [innerSelf clearBundlePreviewState];
                innerSelf->_bundlePreviewTextView.string = ToNSString(preview);
                innerSelf->_reportInventoryStateLabel.stringValue = @"Bundle preview failed.";
                innerSelf->_reportInventoryStateLabel.textColor = [NSColor systemRedColor];
            } else {
                const auto appliedDecision = [innerSelf applyBundlePreviewStateForPath:bundlePath
                                                                               preview:preview
                                                                               details:details
                                                                              decision:decision];
                innerSelf->_reportInventoryStateLabel.stringValue = PreviewDecisionStateText(appliedDecision, localFallback);
                innerSelf->_reportInventoryStateLabel.textColor = PreviewDecisionColor(appliedDecision);
            }
        });
    });
}

- (void)loadReplayRangeFromPreviewedBundle:(id)sender {
    (void)sender;
    tapescope::RangeQuery range;
    range.firstSessionSeq = _previewedBundleFirstSessionSeq;
    range.lastSessionSeq = _previewedBundleLastSessionSeq;
    [self loadReplayRange:range
                available:(_previewedBundleFirstSessionSeq > 0 &&
                           _previewedBundleLastSessionSeq >= _previewedBundleFirstSessionSeq)
               stateLabel:_reportInventoryStateLabel
           missingMessage:@"Previewed bundle is missing a replayable session_seq window."];
}

- (void)openPreviewBundleSourceArtifact:(id)sender {
    (void)sender;
    if (_previewedBundleSourceArtifactId.empty()) {
        _reportInventoryStateLabel.stringValue = @"Previewed bundle does not expose a source artifact id.";
        _reportInventoryStateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    _artifactIdField.stringValue = ToNSString(_previewedBundleSourceArtifactId);
    if (_tabView != nil) {
        [_tabView selectTabViewItemWithIdentifier:@"ArtifactPane"];
    }
    [self fetchArtifact:nil];
}

- (void)openMatchingImportedBundle:(id)sender {
    (void)sender;
    if (_previewedImportedArtifactId.empty()) {
        _reportInventoryStateLabel.stringValue = @"Previewed bundle does not match an imported case artifact yet.";
        _reportInventoryStateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    _artifactIdField.stringValue = ToNSString(_previewedImportedArtifactId);
    if (_tabView != nil) {
        [_tabView selectTabViewItemWithIdentifier:@"ArtifactPane"];
    }
    [self fetchArtifact:nil];
}

- (void)revealSelectedBundlePath:(id)sender {
    (void)sender;
    const std::string bundlePath = TrimAscii(ToStdString(_bundleImportPathField.stringValue));
    if (bundlePath.empty()) {
        _reportInventoryStateLabel.stringValue = @"No bundle path is selected.";
        _reportInventoryStateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    NSString* path = ToNSString(bundlePath);
    BOOL isDirectory = NO;
    if (![[NSFileManager defaultManager] fileExistsAtPath:path isDirectory:&isDirectory]) {
        _reportInventoryStateLabel.stringValue = @"Selected bundle path does not exist.";
        _reportInventoryStateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    NSURL* url = [NSURL fileURLWithPath:path];
    [[NSWorkspace sharedWorkspace] activateFileViewerSelectingURLs:@[ url ]];
    _reportInventoryStateLabel.stringValue = @"Revealed bundle path in Finder.";
    _reportInventoryStateLabel.textColor = [NSColor systemGreenColor];
}

- (void)loadReplayRangeFromImportedCase:(id)sender {
    (void)sender;
    const NSInteger selected = _importedCaseTableView.selectedRow;
    if (selected < 0 || static_cast<std::size_t>(selected) >= _latestImportedCases.size()) {
        _reportInventoryStateLabel.stringValue = @"Select an imported case row first.";
        _reportInventoryStateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    const auto& row = _latestImportedCases.at(static_cast<std::size_t>(selected));
    tapescope::RangeQuery range;
    range.firstSessionSeq = row.firstSessionSeq;
    range.lastSessionSeq = row.lastSessionSeq;
    [self loadReplayRange:range
                available:(row.firstSessionSeq > 0 && row.lastSessionSeq >= row.firstSessionSeq)
               stateLabel:_reportInventoryStateLabel
           missingMessage:@"Imported case is missing a replayable session_seq window."];
}

- (void)openSelectedImportedSourceArtifact:(id)sender {
    (void)sender;
    const NSInteger selected = _importedCaseTableView.selectedRow;
    if (selected < 0 || static_cast<std::size_t>(selected) >= _latestImportedCases.size()) {
        _reportInventoryStateLabel.stringValue = @"Select an imported case row first.";
        _reportInventoryStateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    const auto& row = _latestImportedCases.at(static_cast<std::size_t>(selected));
    if (row.sourceArtifactId.empty()) {
        _reportInventoryStateLabel.stringValue = @"Selected imported case is missing a source artifact id.";
        _reportInventoryStateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    _artifactIdField.stringValue = ToNSString(row.sourceArtifactId);
    if (_tabView != nil) {
        [_tabView selectTabViewItemWithIdentifier:@"ArtifactPane"];
    }
    [self fetchArtifact:nil];
}

@end
