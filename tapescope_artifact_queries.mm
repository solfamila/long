#import "tapescope_window_internal.h"

#include "tapescope_support.h"

#include <CommonCrypto/CommonDigest.h>

#include <fstream>
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

std::string sha256Hex(const std::vector<std::uint8_t>& input) {
    unsigned char digest[CC_SHA256_DIGEST_LENGTH];
    CC_SHA256(input.data(), static_cast<CC_LONG>(input.size()), digest);

    static constexpr char kHex[] = "0123456789abcdef";
    std::string output;
    output.reserve(CC_SHA256_DIGEST_LENGTH * 2);
    for (unsigned char byte : digest) {
        output.push_back(kHex[(byte >> 4) & 0x0fU]);
        output.push_back(kHex[byte & 0x0fU]);
    }
    return output;
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

std::string DescribeBundlePreview(const std::string& bundlePath,
                                  BundlePreviewDetails* detailsOut) {
    if (detailsOut != nullptr) {
        *detailsOut = BundlePreviewDetails{};
    }

    std::ifstream in(bundlePath, std::ios::binary);
    if (!in.is_open()) {
        return "Failed to open bundle for preview.";
    }
    const std::vector<std::uint8_t> bytes((std::istreambuf_iterator<char>(in)),
                                          std::istreambuf_iterator<char>());
    if (bytes.empty()) {
        return "Bundle file is empty.";
    }
    const tapescope::json bundle = tapescope::json::from_msgpack(bytes, true, false);
    if (bundle.is_discarded()) {
        return "Bundle preview failed: file is not valid MessagePack JSON.";
    }
    if (bundle.value("schema", std::string()) != "com.foxy.tape-engine.report-bundle") {
        return "Bundle preview failed: schema is not com.foxy.tape-engine.report-bundle.";
    }

    const std::string bundleType = bundle.value("bundle_type", std::string());
    const std::uint64_t firstSessionSeq = bundle.value("first_session_seq", 0ULL);
    const std::uint64_t lastSessionSeq = bundle.value("last_session_seq", 0ULL);
    const std::string payloadSha256 = sha256Hex(bytes);
    const tapescope::json sourceArtifact = bundle.value("source_artifact", tapescope::json::object());
    const std::string sourceArtifactId =
        bundle.value("source_artifact_id", sourceArtifact.value("artifact_id", std::string()));
    if (detailsOut != nullptr) {
        detailsOut->bundleId = bundle.value("bundle_id", std::string());
        detailsOut->bundleType = bundleType;
        detailsOut->sourceArtifactId = sourceArtifactId;
        detailsOut->payloadSha256 = payloadSha256;
        detailsOut->firstSessionSeq = firstSessionSeq;
        detailsOut->lastSessionSeq = lastSessionSeq;
    }

    const tapescope::json sourceReport = bundle.value("source_report", tapescope::json::object());
    const tapescope::json reportBundle = bundle.value("report_bundle", tapescope::json::object());
    const tapescope::json reportSummary = reportBundle.value("summary", tapescope::json::object());
    const std::string markdown = bundle.value("report_markdown", std::string());

    std::ostringstream out;
    out << "bundle_path: " << bundlePath << "\n"
        << "bundle_id: " << bundle.value("bundle_id", std::string()) << "\n"
        << "bundle_type: " << bundleType << "\n"
        << "headline: " << bundle.value("headline", std::string()) << "\n"
        << "instrument_id: " << bundle.value("instrument_id", std::string()) << "\n"
        << "source_artifact_id: " << sourceArtifactId << "\n"
        << "source_report_id: " << bundle.value("source_report_id", 0ULL) << "\n"
        << "source_revision_id: " << bundle.value("source_revision_id", 0ULL) << "\n"
        << "session_seq: [" << firstSessionSeq << ", " << lastSessionSeq << "]\n"
        << "payload_sha256: " << payloadSha256 << "\n"
        << "exported_ts_engine_ns: " << bundle.value("exported_ts_engine_ns", 0ULL) << "\n";
    if (sourceArtifact.is_object()) {
        out << "\nsource_artifact:\n" << sourceArtifact.dump(2) << "\n";
    }
    if (sourceReport.is_object()) {
        out << "\nsource_report:\n" << sourceReport.dump(2) << "\n";
    }
    if (reportSummary.is_object()) {
        out << "\nreport_summary:\n" << reportSummary.dump(2) << "\n";
    }
    if (!markdown.empty()) {
        out << "\nreport_markdown_preview:\n";
        constexpr std::size_t kMarkdownPreviewLimit = 900;
        if (markdown.size() > kMarkdownPreviewLimit) {
            out << markdown.substr(0, kMarkdownPreviewLimit) << "\n…";
        } else {
            out << markdown;
        }
        out << "\n";
    }
    return out.str();
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
- (void)applyBundlePreviewStateForPath:(const std::string&)bundlePath
                               preview:(const std::string&)preview
                               details:(const BundlePreviewDetails&)details;

@end

@implementation TapeScopeWindowController (ArtifactQueries)

- (void)clearBundlePreviewState {
    _previewedBundleId.clear();
    _previewedBundleType.clear();
    _previewedBundleSourceArtifactId.clear();
    _previewedImportedArtifactId.clear();
    _previewedBundleFirstSessionSeq = 0;
    _previewedBundleLastSessionSeq = 0;
    _bundlePreviewLoadRangeButton.enabled = NO;
    _bundlePreviewOpenSourceButton.enabled = NO;
    _bundlePreviewOpenImportedButton.enabled = NO;
}

- (void)applyBundlePreviewStateForPath:(const std::string&)bundlePath
                               preview:(const std::string&)preview
                               details:(const BundlePreviewDetails&)details {
    std::string renderedPreview = preview;
    _previewedBundleId = details.bundleId;
    _previewedBundleType = details.bundleType;
    _previewedBundleSourceArtifactId = details.sourceArtifactId;
    _previewedBundleFirstSessionSeq = details.firstSessionSeq;
    _previewedBundleLastSessionSeq = details.lastSessionSeq;
    _bundlePreviewLoadRangeButton.enabled =
        (details.firstSessionSeq > 0 && details.lastSessionSeq >= details.firstSessionSeq);
    _bundlePreviewOpenSourceButton.enabled = !details.sourceArtifactId.empty();
    _previewedImportedArtifactId.clear();

    std::optional<std::size_t> matchIndex;
    if (!bundlePath.empty() || !details.bundleId.empty()) {
        for (std::size_t index = 0; index < _latestImportedCases.size(); ++index) {
            const auto& row = _latestImportedCases[index];
            if (!bundlePath.empty() && row.sourceBundlePath == bundlePath) {
                matchIndex = index;
                break;
            }
        }
        if (!matchIndex.has_value() && !details.payloadSha256.empty()) {
            for (std::size_t index = 0; index < _latestImportedCases.size(); ++index) {
                const auto& row = _latestImportedCases[index];
                if (row.payloadSha256 == details.payloadSha256) {
                    matchIndex = index;
                    break;
                }
            }
        }
        if (!matchIndex.has_value() && !details.bundleId.empty()) {
            for (std::size_t index = 0; index < _latestImportedCases.size(); ++index) {
                const auto& row = _latestImportedCases[index];
                if (row.bundleId == details.bundleId) {
                    matchIndex = index;
                    break;
                }
            }
        }
    }

    if (matchIndex.has_value()) {
        const auto& row = _latestImportedCases.at(*matchIndex);
        _previewedImportedArtifactId = row.artifactId;
        _bundlePreviewOpenImportedButton.enabled = !row.artifactId.empty();
        renderedPreview += "\nimport_inventory_match:\n";
        renderedPreview += "  imported_case_id: " + std::to_string(row.importedCaseId) + "\n";
        renderedPreview += "  artifact_id: " + row.artifactId + "\n";
        renderedPreview += "  match_reason: ";
        if (!bundlePath.empty() && row.sourceBundlePath == bundlePath) {
            renderedPreview += "source_bundle_path\n";
        } else if (!details.payloadSha256.empty() && row.payloadSha256 == details.payloadSha256) {
            renderedPreview += "payload_sha256\n";
        } else {
            renderedPreview += "bundle_id\n";
        }
        const NSInteger selected = _importedCaseTableView.selectedRow;
        if (selected != static_cast<NSInteger>(*matchIndex)) {
            [_importedCaseTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:*matchIndex]
                                byExtendingSelection:NO];
        }
    } else {
        _bundlePreviewOpenImportedButton.enabled = NO;
        renderedPreview += "\nimport_inventory_match: none\n";
    }
    _bundlePreviewTextView.string = ToNSString(renderedPreview);
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
                BundlePreviewDetails details;
                const std::string preview = DescribeBundlePreview(result.value.bundlePath, &details);
                [innerSelf applyBundlePreviewStateForPath:result.value.bundlePath
                                                  preview:preview
                                                  details:details];
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
            BundlePreviewDetails details;
            const std::string preview = DescribeBundlePreview(row.sourceBundlePath, &details);
            [self applyBundlePreviewStateForPath:row.sourceBundlePath
                                         preview:preview
                                         details:details];
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
                BundlePreviewDetails details;
                const std::string preview = DescribeBundlePreview(result.value.bundlePath, &details);
                [innerSelf applyBundlePreviewStateForPath:result.value.bundlePath
                                                  preview:preview
                                                  details:details];
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
                BundlePreviewDetails details;
                const std::string preview = DescribeBundlePreview(result.value.bundlePath, &details);
                [innerSelf applyBundlePreviewStateForPath:result.value.bundlePath
                                                  preview:preview
                                                  details:details];
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
                BundlePreviewDetails details;
                const std::string preview = DescribeBundlePreview(bundlePath, &details);
                [innerSelf applyBundlePreviewStateForPath:bundlePath
                                                  preview:preview
                                                  details:details];
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
        BundlePreviewDetails details;
        const std::string preview = DescribeBundlePreview(bundlePath, &details);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil || ![innerSelf isRequestTokenCurrent:token storage:&innerSelf->_bundleWorkflowRequestToken]) {
                return;
            }
            innerSelf->_bundleWorkflowInFlight = NO;
            innerSelf->_bundlePreviewButton.enabled = YES;
            innerSelf->_bundleImportButton.enabled = YES;
            innerSelf->_bundleChooseImportButton.enabled = YES;
            if (preview.rfind("Bundle preview failed:", 0) == 0 || preview == "Failed to open bundle for preview." ||
                preview == "Bundle file is empty.") {
                [innerSelf clearBundlePreviewState];
                innerSelf->_bundlePreviewTextView.string = ToNSString(preview);
                innerSelf->_reportInventoryStateLabel.stringValue = @"Bundle preview failed.";
                innerSelf->_reportInventoryStateLabel.textColor = [NSColor systemRedColor];
            } else {
                [innerSelf applyBundlePreviewStateForPath:bundlePath
                                                  preview:preview
                                                  details:details];
                innerSelf->_reportInventoryStateLabel.stringValue = @"Bundle preview ready.";
                innerSelf->_reportInventoryStateLabel.textColor = [NSColor systemGreenColor];
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
