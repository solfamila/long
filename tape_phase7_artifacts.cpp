#include "tape_phase7_artifacts.h"

#include "tape_bundle_inspection.h"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <functional>
#include <iomanip>
#include <optional>
#include <set>
#include <sstream>
#include <string_view>
#include <system_error>
#include <unordered_map>
#include <utility>

namespace tape_phase7 {

namespace {

namespace fs = std::filesystem;

std::string normalizeToken(std::string_view value) {
    std::string normalized;
    normalized.reserve(value.size());
    bool previousDash = false;
    for (const unsigned char ch : value) {
        if (std::isalnum(ch) != 0) {
            normalized.push_back(static_cast<char>(std::tolower(ch)));
            previousDash = false;
            continue;
        }
        if (!normalized.empty() && !previousDash) {
            normalized.push_back('-');
            previousDash = true;
        }
    }
    while (!normalized.empty() && normalized.back() == '-') {
        normalized.pop_back();
    }
    return normalized.empty() ? std::string("artifact") : normalized;
}

std::string fnv1aHex(std::string_view text) {
    std::uint64_t hash = 1469598103934665603ULL;
    for (const unsigned char ch : text) {
        hash ^= static_cast<std::uint64_t>(ch);
        hash *= 1099511628211ULL;
    }

    std::ostringstream out;
    out << std::hex << std::nouppercase << std::setw(16) << std::setfill('0') << hash;
    return out.str();
}

std::string utcTimestampIso8601Now() {
    const auto now = std::chrono::system_clock::now();
    const auto msSinceEpoch = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch());
    const auto msPart = static_cast<long long>(msSinceEpoch.count() % 1000);
    const std::time_t time = std::chrono::system_clock::to_time_t(now);

    std::tm utc{};
#if defined(_WIN32)
    gmtime_s(&utc, &time);
#else
    gmtime_r(&time, &utc);
#endif

    std::ostringstream out;
    out << std::put_time(&utc, "%Y-%m-%dT%H:%M:%S")
        << '.'
        << std::setw(3) << std::setfill('0') << msPart
        << 'Z';
    return out.str();
}

fs::path phase7RootDir() {
    fs::path basePath;
    if (const char* overridePath = std::getenv("TWS_GUI_DATA_DIR");
        overridePath != nullptr && *overridePath != '\0') {
        basePath = fs::path(overridePath);
    } else if (const char* home = std::getenv("HOME");
               home != nullptr && *home != '\0') {
        basePath = fs::path(home) / "Library" / "Application Support" / "TWS Trading GUI";
    } else {
        basePath = fs::current_path() / "tws_gui_data";
    }
    return basePath / "phase7_artifacts";
}

const std::vector<AnalyzerProfileSpec>& analyzerProfiles() {
    static const std::vector<AnalyzerProfileSpec> profiles = {
        AnalyzerProfileSpec{
            .analysisProfile = kDefaultAnalyzerProfile,
            .title = "Trace Fill Integrity",
            .summary = "Inspect a portable Phase 6 session/case bundle for integrity gaps, resets, identity drift, and data-quality degradation around the exported evidence window.",
            .supportedSourceBundleTypes = {"case_bundle", "session_bundle"},
            .findingCategories = {"trace_integrity", "identity_integrity", "evidence_confidence"},
            .defaultProfile = true
        },
        AnalyzerProfileSpec{
            .analysisProfile = kIncidentTriageAnalyzerProfile,
            .title = "Incident Triage",
            .summary = "Inspect a portable Phase 6 session/case bundle for the top incident, uncertainty, and timeline hotspots that should drive review-first playbook planning.",
            .supportedSourceBundleTypes = {"case_bundle", "session_bundle"},
            .findingCategories = {"incident_priority", "incident_uncertainty", "timeline_hotspot"},
            .defaultProfile = false
        },
        AnalyzerProfileSpec{
            .analysisProfile = kFillQualityAnalyzerProfile,
            .title = "Fill Quality Review",
            .summary = "Inspect a portable Phase 6 bundle for fill invalidation, adverse-selection, and market-impact evidence that should gate execution readiness.",
            .supportedSourceBundleTypes = {"case_bundle", "session_bundle"},
            .findingCategories = {"fill_quality_risk", "adverse_selection_risk", "market_impact_risk"},
            .defaultProfile = false
        },
        AnalyzerProfileSpec{
            .analysisProfile = kLiquidityBehaviorAnalyzerProfile,
            .title = "Liquidity Behavior",
            .summary = "Inspect a portable Phase 6 bundle for display instability, refill/absorption behavior, and pressure-driven liquidity fades around the touch.",
            .supportedSourceBundleTypes = {"case_bundle", "session_bundle"},
            .findingCategories = {"display_instability_risk", "liquidity_refill_behavior", "pressure_and_thinning"},
            .defaultProfile = false
        },
        AnalyzerProfileSpec{
            .analysisProfile = kAdverseSelectionAnalyzerProfile,
            .title = "Adverse Selection Review",
            .summary = "Inspect a portable Phase 6 bundle for post-fill adverse-selection sequences, invalidation chains, and the fill context that should block execution approval.",
            .supportedSourceBundleTypes = {"case_bundle", "session_bundle"},
            .findingCategories = {"adverse_selection_risk", "fill_quality_risk"},
            .defaultProfile = false
        },
        AnalyzerProfileSpec{
            .analysisProfile = kOrderImpactAnalyzerProfile,
            .title = "Order Impact Review",
            .summary = "Inspect a portable Phase 6 bundle for market-impact footprint, cancel-chain pressure, and pre-impact fill context before execution approval.",
            .supportedSourceBundleTypes = {"case_bundle", "session_bundle"},
            .findingCategories = {"market_impact_risk", "fill_quality_risk"},
            .defaultProfile = false
        }
    };
    return profiles;
}

const AnalyzerProfileSpec* findAnalyzerProfileSpec(std::string_view profile) {
    const std::string requested = normalizeToken(profile.empty() ? kDefaultAnalyzerProfile : profile);
    for (const auto& item : analyzerProfiles()) {
        if (normalizeToken(item.analysisProfile) == requested) {
            return &item;
        }
    }
    return nullptr;
}

bool ensureDirectoryExists(const fs::path& path, std::string* error) {
    std::error_code ec;
    if (fs::exists(path, ec)) {
        if (ec) {
            if (error != nullptr) {
                *error = "Failed to inspect artifact directory " + path.string() + ": " + ec.message();
            }
            return false;
        }
        if (!fs::is_directory(path, ec) || ec) {
            if (error != nullptr) {
                *error = "Artifact directory path is not a directory: " + path.string();
            }
            return false;
        }
        return true;
    }

    fs::create_directories(path, ec);
    if (ec) {
        if (error != nullptr) {
            *error = "Failed to create artifact directory " + path.string() + ": " + ec.message();
        }
        return false;
    }
    return true;
}

bool writeJsonTextFile(const fs::path& path, const json& payload, std::string* error) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out.is_open()) {
        if (error != nullptr) {
            *error = "Failed to open output file: " + path.string();
        }
        return false;
    }
    out << payload.dump(2) << '\n';
    if (!out.good()) {
        if (error != nullptr) {
            *error = "Failed to write output file: " + path.string();
        }
        return false;
    }
    return true;
}

bool readJsonFile(const fs::path& path, json* out, std::string* error) {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "Missing output container.";
        }
        return false;
    }
    std::ifstream in(path, std::ios::binary);
    if (!in.is_open()) {
        if (error != nullptr) {
            *error = "Failed to open json file: " + path.string();
        }
        return false;
    }
    const json parsed = json::parse(in, nullptr, false);
    if (parsed.is_discarded()) {
        if (error != nullptr) {
            *error = "Failed to parse json file: " + path.string();
        }
        return false;
    }
    *out = parsed;
    return true;
}

ArtifactRef sourceArtifactRefFromInspection(const tape_bundle::PortableBundleInspection& inspection) {
    ArtifactRef artifact;
    artifact.artifactType = inspection.bundleType == "case_bundle"
        ? "phase6.case_bundle.v1"
        : inspection.bundleType == "session_bundle"
            ? "phase6.session_bundle.v1"
            : "phase6.bundle.v1";
    artifact.contractVersion = "phase6-portable-bundle-v1";
    artifact.artifactId = !inspection.bundleId.empty()
        ? inspection.bundleId
        : artifact.artifactType + ":" + fnv1aHex(
            inspection.sourceArtifactId + "|" +
            std::to_string(inspection.sourceReportId) + "|" +
            std::to_string(inspection.sourceRevisionId));
    artifact.manifestPath = inspection.bundlePath.string();
    artifact.artifactRootDir = inspection.bundlePath.parent_path().string();
    return artifact;
}

json revisionContextFromInspection(const tape_bundle::PortableBundleInspection& inspection) {
    return {
        {"served_revision_id", inspection.sourceRevisionId == 0 ? json(nullptr) : json(inspection.sourceRevisionId)},
        {"latest_session_seq", inspection.lastSessionSeq == 0 ? json(nullptr) : json(inspection.lastSessionSeq)},
        {"first_session_seq", inspection.firstSessionSeq == 0 ? json(nullptr) : json(inspection.firstSessionSeq)},
        {"last_session_seq", inspection.lastSessionSeq == 0 ? json(nullptr) : json(inspection.lastSessionSeq)},
        {"manifest_hash", nullptr},
        {"includes_mutable_tail", false},
        {"source", "artifact_manifest"},
        {"staleness", "snapshot"}
    };
}

json traceAnchorFromInspection(const tape_bundle::PortableBundleInspection& inspection) {
    if (inspection.sourceReport.is_object() && inspection.sourceReport.contains("anchor") &&
        inspection.sourceReport.at("anchor").is_object()) {
        return inspection.sourceReport.at("anchor");
    }
    return json{
        {"trace_id", nullptr},
        {"order_id", nullptr},
        {"perm_id", nullptr},
        {"exec_id", nullptr}
    };
}

json requestedWindowFromInspection(const tape_bundle::PortableBundleInspection& inspection) {
    if (inspection.firstSessionSeq == 0 || inspection.lastSessionSeq == 0) {
        return nullptr;
    }
    return {
        {"first_session_seq", inspection.firstSessionSeq},
        {"last_session_seq", inspection.lastSessionSeq}
    };
}

json replayContextFromInspection(const tape_bundle::PortableBundleInspection& inspection) {
    const std::string generatedAtUtc = utcTimestampIso8601Now();
    return {
        {"trace_anchor", traceAnchorFromInspection(inspection)},
        {"revision_context", revisionContextFromInspection(inspection)},
        {"generated_at_utc", generatedAtUtc},
        {"requested_window", requestedWindowFromInspection(inspection)},
        {"source_bundle_type", inspection.bundleType},
        {"source_exported_ts_engine_ns", inspection.exportedTsEngineNs}
    };
}

ArtifactRef analysisArtifactRefForBundle(const tape_bundle::PortableBundleInspection& inspection,
                                         std::string_view analysisProfile) {
    const std::string normalizedProfile = normalizeToken(analysisProfile);
    const std::string stableSeed =
        inspection.bundleType + "|" +
        inspection.bundleId + "|" +
        inspection.sourceArtifactId + "|" +
        std::to_string(inspection.sourceReportId) + "|" +
        std::to_string(inspection.sourceRevisionId) + "|" +
        std::to_string(inspection.firstSessionSeq) + "|" +
        std::to_string(inspection.lastSessionSeq) + "|" +
        normalizedProfile;
    const std::string artifactId = "phase7-analysis-" + fnv1aHex(stableSeed);
    const fs::path rootDir = phase7RootDir() / "analysis" / artifactId;
    ArtifactRef artifact;
    artifact.artifactType = kAnalysisArtifactType;
    artifact.contractVersion = kContractVersion;
    artifact.artifactId = artifactId;
    artifact.manifestPath = (rootDir / "manifest.json").string();
    artifact.artifactRootDir = rootDir.string();
    return artifact;
}

ArtifactRef playbookArtifactRefForAnalysis(const AnalysisArtifact& analysis,
                                           const std::vector<std::string>& findingIds,
                                           std::string_view mode) {
    std::ostringstream seed;
    seed << analysis.analysisArtifact.artifactId << "|" << mode;
    for (const auto& findingId : findingIds) {
        seed << "|" << findingId;
    }
    const std::string artifactId = "phase7-playbook-" + fnv1aHex(seed.str());
    const fs::path rootDir = phase7RootDir() / "playbooks" / artifactId;
    ArtifactRef artifact;
    artifact.artifactType = kPlaybookArtifactType;
    artifact.contractVersion = kContractVersion;
    artifact.artifactId = artifactId;
    artifact.manifestPath = (rootDir / "manifest.json").string();
    artifact.artifactRootDir = rootDir.string();
    return artifact;
}

ArtifactRef executionLedgerArtifactRefForPlaybook(const PlaybookArtifact& playbook) {
    const std::string artifactId =
        "phase7-ledger-" + fnv1aHex(playbook.playbookArtifact.artifactId + "|execution-ledger");
    const fs::path rootDir = phase7RootDir() / "ledgers" / artifactId;
    ArtifactRef artifact;
    artifact.artifactType = kExecutionLedgerArtifactType;
    artifact.contractVersion = kContractVersion;
    artifact.artifactId = artifactId;
    artifact.manifestPath = (rootDir / "manifest.json").string();
    artifact.artifactRootDir = rootDir.string();
    return artifact;
}

json sourceBundleEvidenceRef(const tape_bundle::PortableBundleInspection& inspection) {
    return {
        {"kind", "phase6_bundle"},
        {"artifact_id", !inspection.bundleId.empty() ? json(inspection.bundleId) : json(nullptr)},
        {"source_artifact_id", inspection.sourceArtifactId.empty() ? json(nullptr) : json(inspection.sourceArtifactId)},
        {"bundle_path", inspection.bundlePath.string()},
        {"payload_sha256", inspection.payloadSha256},
        {"first_session_seq", inspection.firstSessionSeq},
        {"last_session_seq", inspection.lastSessionSeq}
    };
}

json bundleSummaryObject(const tape_bundle::PortableBundleInspection& inspection) {
    return inspection.reportBundle.value("summary", json::object());
}

json bundleResultObject(const tape_bundle::PortableBundleInspection& inspection) {
    return inspection.reportBundle.value("result", json::object());
}

json firstArrayObject(std::initializer_list<json> candidates) {
    for (const auto& candidate : candidates) {
        if (candidate.is_array()) {
            return candidate;
        }
    }
    return json::array();
}

json firstObject(std::initializer_list<json> candidates) {
    for (const auto& candidate : candidates) {
        if (candidate.is_object()) {
            return candidate;
        }
    }
    return json::object();
}

json incidentRowsFromInspection(const tape_bundle::PortableBundleInspection& inspection) {
    const json summary = bundleSummaryObject(inspection);
    const json result = bundleResultObject(inspection);
    return firstArrayObject({
        inspection.sourceReport.value("incident_rows", json()),
        summary.value("incident_rows", json()),
        result.value("incident_rows", json())
    });
}

json citationRowsFromInspection(const tape_bundle::PortableBundleInspection& inspection) {
    const json summary = bundleSummaryObject(inspection);
    const json result = bundleResultObject(inspection);
    return firstArrayObject({
        inspection.sourceReport.value("citation_rows", json()),
        summary.value("citation_rows", json()),
        result.value("citation_rows", json())
    });
}

json findingKindCountsFromInspection(const tape_bundle::PortableBundleInspection& inspection) {
    const json summary = bundleSummaryObject(inspection);
    const json report = firstObject({
        summary.value("report", json()),
        summary.value("report_summary", json()),
        inspection.reportSummary
    });
    return firstObject({
        summary.value("finding_kind_counts", json()),
        report.value("finding_kind_counts", json())
    });
}

json incidentKindCountsFromInspection(const tape_bundle::PortableBundleInspection& inspection) {
    const json summary = bundleSummaryObject(inspection);
    const json report = firstObject({
        summary.value("report", json()),
        summary.value("report_summary", json()),
        inspection.reportSummary
    });
    return firstObject({
        summary.value("incident_kind_counts", json()),
        report.value("incident_kind_counts", json())
    });
}

std::uint64_t countKinds(const json& countObject, std::initializer_list<const char*> kinds) {
    std::uint64_t total = 0;
    if (!countObject.is_object()) {
        return 0;
    }
    for (const char* kind : kinds) {
        if (!countObject.contains(kind)) {
            continue;
        }
        if (countObject.at(kind).is_number_unsigned()) {
            total += countObject.at(kind).get<std::uint64_t>();
        } else if (countObject.at(kind).is_number_integer()) {
            const auto value = countObject.at(kind).get<long long>();
            total += value > 0 ? static_cast<std::uint64_t>(value) : 0ULL;
        }
    }
    return total;
}

std::uint64_t countIncidentRowsByKind(const json& incidentRows, std::initializer_list<const char*> kinds) {
    if (!incidentRows.is_array()) {
        return 0;
    }
    std::uint64_t total = 0;
    for (const auto& row : incidentRows) {
        const std::string kind = row.value("kind", std::string());
        for (const char* expected : kinds) {
            if (kind == expected) {
                ++total;
                break;
            }
        }
    }
    return total;
}

std::uint64_t countKindsFromInspection(const tape_bundle::PortableBundleInspection& inspection,
                                       std::initializer_list<const char*> kinds) {
    const json incidentRows = incidentRowsFromInspection(inspection);
    const json incidentKindCounts = incidentKindCountsFromInspection(inspection);
    const json findingKindCounts = findingKindCountsFromInspection(inspection);
    return countIncidentRowsByKind(incidentRows, kinds) +
        countKinds(incidentKindCounts, kinds) +
        countKinds(findingKindCounts, kinds);
}

std::vector<FindingRecord> buildTraceFillIntegrityFindings(const ArtifactRef& analysisArtifact,
                                                           const tape_bundle::PortableBundleInspection& inspection) {
    const json summary = inspection.reportBundle.value("summary", json::object());
    const json dataQuality = summary.value("data_quality", json::object());

    const auto count = [&](const char* key) -> std::uint64_t {
        if (!dataQuality.contains(key)) {
            return 0;
        }
        if (dataQuality.at(key).is_number_unsigned()) {
            return dataQuality.at(key).get<std::uint64_t>();
        }
        if (dataQuality.at(key).is_number_integer()) {
            const auto value = dataQuality.at(key).get<long long>();
            return value > 0 ? static_cast<std::uint64_t>(value) : 0ULL;
        }
        return 0;
    };

    const auto maybeScore = [&]() -> std::optional<double> {
        if (!dataQuality.contains("score") || !dataQuality.at("score").is_number()) {
            return std::nullopt;
        }
        return dataQuality.at("score").get<double>();
    }();

    const std::uint64_t gapCount = count("gap_marker_count");
    const std::uint64_t resetCount = count("reset_marker_count");
    const std::uint64_t weakIdentityCount = count("weak_instrument_identity_count");
    const std::uint64_t mismatchedIdentityCount = count("mismatched_instrument_identity_count");
    const std::uint64_t heuristicIdentityCount = count("heuristic_instrument_identity_count");
    const std::uint64_t identityOverrideCount = count("identity_policy_override_count");
    const std::uint64_t missingReceiveTimestampCount = count("missing_receive_timestamp_count");
    const std::uint64_t missingExchangeTimestampCount = count("missing_exchange_timestamp_count");
    const std::uint64_t missingVendorSequenceCount = count("missing_vendor_sequence_count");

    const auto makeFinding = [&](std::string category,
                                 std::string severity,
                                 std::string summaryText) {
        FindingRecord finding;
        finding.category = std::move(category);
        finding.severity = std::move(severity);
        finding.summary = std::move(summaryText);
        finding.findingId = "phase7-finding-" + fnv1aHex(analysisArtifact.artifactId + "|" + finding.category);
        finding.evidenceRefs = json::array();
        finding.evidenceRefs.push_back(sourceBundleEvidenceRef(inspection));
        if (inspection.sourceReport.is_object() && inspection.sourceReport.contains("artifact_id")) {
            finding.evidenceRefs.push_back({
                {"kind", "source_report"},
                {"artifact_id", inspection.sourceReport.value("artifact_id", std::string())},
                {"report_id", inspection.sourceReport.value("report_id", 0ULL)}
            });
        }
        return finding;
    };

    std::vector<FindingRecord> findings;
    if (gapCount > 0 || resetCount > 0) {
        std::ostringstream summaryText;
        summaryText << "Trace integrity degraded: " << gapCount << " gap marker(s) and "
                    << resetCount << " reset marker(s) were preserved in the exported evidence window.";
        findings.push_back(makeFinding("trace_integrity", "warning", summaryText.str()));
    }

    if (weakIdentityCount > 0 || mismatchedIdentityCount > 0 ||
        heuristicIdentityCount > 0 || identityOverrideCount > 0) {
        std::ostringstream summaryText;
        summaryText << "Instrument identity degraded: weak=" << weakIdentityCount
                    << ", mismatched=" << mismatchedIdentityCount
                    << ", heuristic=" << heuristicIdentityCount
                    << ", overrides=" << identityOverrideCount << ".";
        findings.push_back(makeFinding("instrument_identity", "warning", summaryText.str()));
    }

    if ((maybeScore.has_value() && *maybeScore < 75.0) ||
        missingReceiveTimestampCount > 0 ||
        missingExchangeTimestampCount > 0 ||
        missingVendorSequenceCount > 0) {
        std::ostringstream summaryText;
        summaryText << "Evidence confidence is degraded";
        if (maybeScore.has_value()) {
            summaryText << " (score " << std::fixed << std::setprecision(1) << *maybeScore << ")";
        }
        summaryText << ": missing receive timestamps=" << missingReceiveTimestampCount
                    << ", exchange timestamps=" << missingExchangeTimestampCount
                    << ", vendor sequence numbers=" << missingVendorSequenceCount << ".";
        findings.push_back(makeFinding("evidence_confidence", "warning", summaryText.str()));
    }

    if (findings.empty()) {
        findings.push_back(makeFinding(
            "analysis_summary",
            "info",
            "No material trace-integrity degradations were detected in the exported evidence window."));
    }

    return findings;
}

std::vector<FindingRecord> buildIncidentTriageFindings(const ArtifactRef& analysisArtifact,
                                                       const tape_bundle::PortableBundleInspection& inspection) {
    const json reportSummary = inspection.reportSummary.is_object() ? inspection.reportSummary : json::object();
    const json timelineHighlights = reportSummary.value("timeline_highlights", json::array());
    const std::string topIncidentKind = reportSummary.value("top_incident_kind", std::string());
    const std::string topIncidentTitle = reportSummary.value("top_incident_title", std::string());
    const std::string topIncidentWhy = reportSummary.value("top_incident_why_it_matters", std::string());
    const std::string topIncidentUncertainty = reportSummary.value("top_incident_uncertainty", std::string());
    const std::string whatChangedFirst = reportSummary.value("what_changed_first", std::string());

    const auto makeFinding = [&](std::string category,
                                 std::string severity,
                                 std::string summaryText) {
        FindingRecord finding;
        finding.category = std::move(category);
        finding.severity = std::move(severity);
        finding.summary = std::move(summaryText);
        finding.findingId = "phase7-finding-" + fnv1aHex(analysisArtifact.artifactId + "|" + finding.category);
        finding.evidenceRefs = json::array();
        finding.evidenceRefs.push_back(sourceBundleEvidenceRef(inspection));
        if (inspection.sourceReport.is_object() && inspection.sourceReport.contains("artifact_id")) {
            finding.evidenceRefs.push_back({
                {"kind", "source_report"},
                {"artifact_id", inspection.sourceReport.value("artifact_id", std::string())},
                {"report_id", inspection.sourceReport.value("report_id", 0ULL)}
            });
        }
        return finding;
    };

    std::vector<FindingRecord> findings;
    if (!topIncidentKind.empty() || !topIncidentTitle.empty()) {
        std::ostringstream summaryText;
        summaryText << "Top incident priority";
        if (!topIncidentTitle.empty()) {
            summaryText << ": " << topIncidentTitle;
        } else {
            summaryText << ": " << topIncidentKind;
        }
        if (!topIncidentKind.empty()) {
            summaryText << " (" << topIncidentKind << ")";
        }
        if (!topIncidentWhy.empty()) {
            summaryText << ". " << topIncidentWhy;
        }
        findings.push_back(makeFinding("incident_priority", "warning", summaryText.str()));
    }

    if (!topIncidentUncertainty.empty()) {
        findings.push_back(makeFinding("incident_uncertainty",
                                       "warning",
                                       "Top incident uncertainty: " + topIncidentUncertainty));
    }

    if (!whatChangedFirst.empty() || (timelineHighlights.is_array() && !timelineHighlights.empty())) {
        std::ostringstream summaryText;
        summaryText << "Timeline hotspot review";
        if (!whatChangedFirst.empty()) {
            summaryText << ": " << whatChangedFirst;
        }
        if (timelineHighlights.is_array() && !timelineHighlights.empty()) {
            const auto firstHighlight = timelineHighlights.front();
            const std::string headline = firstHighlight.value("headline", std::string());
            if (!headline.empty()) {
                summaryText << ". First highlight: " << headline;
            }
            summaryText << ". Highlight count=" << timelineHighlights.size() << '.';
        }
        findings.push_back(makeFinding("timeline_hotspot", "info", summaryText.str()));
    }

    if (findings.empty()) {
        findings.push_back(makeFinding(
            "analysis_summary",
            "info",
            "No ranked incident or timeline hotspot stood out in the exported report summary."));
    }

    return findings;
}

std::vector<FindingRecord> buildFillQualityFindings(const ArtifactRef& analysisArtifact,
                                                    const tape_bundle::PortableBundleInspection& inspection) {
    const auto makeFinding = [&](std::string category,
                                 std::string severity,
                                 std::string summaryText) {
        FindingRecord finding;
        finding.category = std::move(category);
        finding.severity = std::move(severity);
        finding.summary = std::move(summaryText);
        finding.findingId = "phase7-finding-" + fnv1aHex(analysisArtifact.artifactId + "|" + finding.category);
        finding.evidenceRefs = json::array({sourceBundleEvidenceRef(inspection)});
        if (inspection.sourceReport.is_object() && inspection.sourceReport.contains("artifact_id")) {
            finding.evidenceRefs.push_back({
                {"kind", "source_report"},
                {"artifact_id", inspection.sourceReport.value("artifact_id", std::string())},
                {"report_id", inspection.sourceReport.value("report_id", 0ULL)}
            });
        }
        return finding;
    };

    const std::uint64_t fillQualitySignals = countKindsFromInspection(
        inspection,
        {"order_fill_context", "passive_fill_queue_proxy"});
    const std::uint64_t adverseSignals = countKindsFromInspection(
        inspection,
        {"buy_fill_invalidation", "sell_fill_invalidation", "post_fill_adverse_selection", "fill_to_adverse_move_chain"});
    const std::uint64_t marketImpactSignals = countKindsFromInspection(
        inspection,
        {"order_window_market_impact", "fill_to_cancel_chain"});

    std::vector<FindingRecord> findings;
    if (fillQualitySignals > 0) {
        std::ostringstream summaryText;
        summaryText << "Fill-quality review should inspect " << fillQualitySignals
                    << " fill-context signal(s) preserved in the portable evidence.";
        findings.push_back(makeFinding("fill_quality_risk", "warning", summaryText.str()));
    }
    if (adverseSignals > 0) {
        std::ostringstream summaryText;
        summaryText << "Adverse-selection risk surfaced " << adverseSignals
                    << " post-fill invalidation/adverse-move signal(s) in the exported window.";
        findings.push_back(makeFinding("adverse_selection_risk", "warning", summaryText.str()));
    }
    if (marketImpactSignals > 0) {
        std::ostringstream summaryText;
        summaryText << "Market-impact review should inspect " << marketImpactSignals
                    << " impact/cancel-chain signal(s) before execution approval.";
        findings.push_back(makeFinding("market_impact_risk", "warning", summaryText.str()));
    }

    if (findings.empty()) {
        findings.push_back(makeFinding(
            "analysis_summary",
            "info",
            "No dedicated fill-quality or adverse-selection signals stood out in the exported evidence window."));
    }
    return findings;
}

std::vector<FindingRecord> buildLiquidityBehaviorFindings(const ArtifactRef& analysisArtifact,
                                                          const tape_bundle::PortableBundleInspection& inspection) {
    const auto makeFinding = [&](std::string category,
                                 std::string severity,
                                 std::string summaryText) {
        FindingRecord finding;
        finding.category = std::move(category);
        finding.severity = std::move(severity);
        finding.summary = std::move(summaryText);
        finding.findingId = "phase7-finding-" + fnv1aHex(analysisArtifact.artifactId + "|" + finding.category);
        finding.evidenceRefs = json::array({sourceBundleEvidenceRef(inspection)});
        const json citations = citationRowsFromInspection(inspection);
        if (citations.is_array()) {
            for (const auto& row : citations) {
                if (finding.evidenceRefs.size() >= 4) {
                    break;
                }
                const std::string artifactId = row.value("artifact_id", std::string());
                if (artifactId.empty()) {
                    continue;
                }
                finding.evidenceRefs.push_back({
                    {"kind", row.value("type", std::string("citation"))},
                    {"artifact_id", artifactId}
                });
            }
        }
        return finding;
    };

    const std::uint64_t displaySignals = countKindsFromInspection(
        inspection,
        {"ask_display_instability", "bid_display_instability", "ask_quote_flicker", "bid_quote_flicker"});
    const std::uint64_t refillSignals = countKindsFromInspection(
        inspection,
        {"ask_liquidity_refilled", "bid_liquidity_refilled", "ask_absorption_persistence", "bid_absorption_persistence",
         "ask_genuine_refill", "bid_genuine_refill"});
    const std::uint64_t pressureSignals = countKindsFromInspection(
        inspection,
        {"buy_trade_pressure", "sell_trade_pressure", "ask_trade_after_depletion", "bid_trade_after_depletion",
         "ask_pull_follow_through", "bid_pull_follow_through"});

    std::vector<FindingRecord> findings;
    if (displaySignals > 0) {
        std::ostringstream summaryText;
        summaryText << "Displayed-liquidity instability surfaced " << displaySignals
                    << " flicker/instability signal(s) at the touch.";
        findings.push_back(makeFinding("display_instability_risk", "warning", summaryText.str()));
    }
    if (refillSignals > 0) {
        std::ostringstream summaryText;
        summaryText << "Refill/absorption behavior surfaced " << refillSignals
                    << " refill persistence or genuine-refill signal(s) that should be reviewed together.";
        findings.push_back(makeFinding("liquidity_refill_behavior", "warning", summaryText.str()));
    }
    if (pressureSignals > 0) {
        std::ostringstream summaryText;
        summaryText << "Pressure/thinning review should inspect " << pressureSignals
                    << " pressure, depletion, or pull-follow-through signal(s) around the inside market.";
        findings.push_back(makeFinding("pressure_and_thinning", "warning", summaryText.str()));
    }

    if (findings.empty()) {
        findings.push_back(makeFinding(
            "analysis_summary",
            "info",
            "No display-instability or liquidity-behavior hotspots stood out in the exported evidence window."));
    }
    return findings;
}

std::vector<FindingRecord> buildAdverseSelectionFindings(const ArtifactRef& analysisArtifact,
                                                         const tape_bundle::PortableBundleInspection& inspection) {
    const auto makeFinding = [&](std::string category,
                                 std::string severity,
                                 std::string summaryText) {
        FindingRecord finding;
        finding.category = std::move(category);
        finding.severity = std::move(severity);
        finding.summary = std::move(summaryText);
        finding.findingId = "phase7-finding-" + fnv1aHex(analysisArtifact.artifactId + "|" + finding.category);
        finding.evidenceRefs = json::array({sourceBundleEvidenceRef(inspection)});
        if (inspection.sourceReport.is_object() && inspection.sourceReport.contains("artifact_id")) {
            finding.evidenceRefs.push_back({
                {"kind", "source_report"},
                {"artifact_id", inspection.sourceReport.value("artifact_id", std::string())},
                {"report_id", inspection.sourceReport.value("report_id", 0ULL)}
            });
        }
        return finding;
    };

    const std::uint64_t fillContextSignals = countKindsFromInspection(
        inspection,
        {"order_fill_context", "passive_fill_queue_proxy", "buy_fill_invalidation", "sell_fill_invalidation"});
    const std::uint64_t adverseSignals = countKindsFromInspection(
        inspection,
        {"post_fill_adverse_selection", "fill_to_adverse_move_chain", "buy_fill_invalidation", "sell_fill_invalidation"});

    std::vector<FindingRecord> findings;
    if (fillContextSignals > 0) {
        std::ostringstream summaryText;
        summaryText << "Adverse-selection review should anchor on " << fillContextSignals
                    << " fill-context or invalidation signal(s) before approving the sequence.";
        findings.push_back(makeFinding("fill_quality_risk", "warning", summaryText.str()));
    }
    if (adverseSignals > 0) {
        std::ostringstream summaryText;
        summaryText << "Post-fill adverse-selection review surfaced " << adverseSignals
                    << " adverse-move or invalidation chain signal(s) in the exported window.";
        findings.push_back(makeFinding("adverse_selection_risk", "warning", summaryText.str()));
    }

    if (findings.empty()) {
        findings.push_back(makeFinding(
            "analysis_summary",
            "info",
            "No dedicated adverse-selection sequence stood out in the exported evidence window."));
    }
    return findings;
}

std::vector<FindingRecord> buildOrderImpactFindings(const ArtifactRef& analysisArtifact,
                                                    const tape_bundle::PortableBundleInspection& inspection) {
    const auto makeFinding = [&](std::string category,
                                 std::string severity,
                                 std::string summaryText) {
        FindingRecord finding;
        finding.category = std::move(category);
        finding.severity = std::move(severity);
        finding.summary = std::move(summaryText);
        finding.findingId = "phase7-finding-" + fnv1aHex(analysisArtifact.artifactId + "|" + finding.category);
        finding.evidenceRefs = json::array({sourceBundleEvidenceRef(inspection)});
        const json citations = citationRowsFromInspection(inspection);
        if (citations.is_array()) {
            for (const auto& row : citations) {
                if (finding.evidenceRefs.size() >= 4) {
                    break;
                }
                const std::string artifactId = row.value("artifact_id", std::string());
                if (artifactId.empty()) {
                    continue;
                }
                finding.evidenceRefs.push_back({
                    {"kind", row.value("type", std::string("citation"))},
                    {"artifact_id", artifactId}
                });
            }
        }
        return finding;
    };

    const std::uint64_t fillContextSignals = countKindsFromInspection(
        inspection,
        {"order_fill_context", "passive_fill_queue_proxy"});
    const std::uint64_t impactSignals = countKindsFromInspection(
        inspection,
        {"order_window_market_impact", "fill_to_cancel_chain", "buy_sweep_sequence", "sell_sweep_sequence"});

    std::vector<FindingRecord> findings;
    if (fillContextSignals > 0) {
        std::ostringstream summaryText;
        summaryText << "Order-impact review should ground the impact read on " << fillContextSignals
                    << " fill-context or queue-context signal(s).";
        findings.push_back(makeFinding("fill_quality_risk", "warning", summaryText.str()));
    }
    if (impactSignals > 0) {
        std::ostringstream summaryText;
        summaryText << "Order-impact review surfaced " << impactSignals
                    << " impact, cancel-chain, or sweep sequence signal(s) before approval.";
        findings.push_back(makeFinding("market_impact_risk", "warning", summaryText.str()));
    }

    if (findings.empty()) {
        findings.push_back(makeFinding(
            "analysis_summary",
            "info",
            "No dedicated order-impact footprint stood out in the exported evidence window."));
    }
    return findings;
}

std::vector<FindingRecord> buildFindings(const ArtifactRef& analysisArtifact,
                                         const tape_bundle::PortableBundleInspection& inspection,
                                         const AnalyzerProfileSpec& profile) {
    if (normalizeToken(profile.analysisProfile) == normalizeToken(kIncidentTriageAnalyzerProfile)) {
        return buildIncidentTriageFindings(analysisArtifact, inspection);
    }
    if (normalizeToken(profile.analysisProfile) == normalizeToken(kFillQualityAnalyzerProfile)) {
        return buildFillQualityFindings(analysisArtifact, inspection);
    }
    if (normalizeToken(profile.analysisProfile) == normalizeToken(kLiquidityBehaviorAnalyzerProfile)) {
        return buildLiquidityBehaviorFindings(analysisArtifact, inspection);
    }
    if (normalizeToken(profile.analysisProfile) == normalizeToken(kAdverseSelectionAnalyzerProfile)) {
        return buildAdverseSelectionFindings(analysisArtifact, inspection);
    }
    if (normalizeToken(profile.analysisProfile) == normalizeToken(kOrderImpactAnalyzerProfile)) {
        return buildOrderImpactFindings(analysisArtifact, inspection);
    }
    return buildTraceFillIntegrityFindings(analysisArtifact, inspection);
}

json manifestForAnalysis(const AnalysisArtifact& analysis) {
    json findings = json::array();
    for (const auto& finding : analysis.findings) {
        findings.push_back(findingToJson(finding));
    }

    return {
        {"artifact_type", analysis.analysisArtifact.artifactType},
        {"contract_version", analysis.analysisArtifact.contractVersion},
        {"artifact_id", analysis.analysisArtifact.artifactId},
        {"manifest_path", analysis.analysisArtifact.manifestPath},
        {"artifact_root_dir", analysis.analysisArtifact.artifactRootDir},
        {"analysis_profile", analysis.analysisProfile},
        {"generated_at_utc", analysis.generatedAtUtc},
        {"source_artifact", artifactRefToJson(analysis.sourceArtifact)},
        {"analysis_artifact", artifactRefToJson(analysis.analysisArtifact)},
        {"replay_context", analysis.replayContext},
        {"finding_count", findings.size()},
        {"findings", std::move(findings)}
    };
}

bool parseArtifactRef(const json& payload, ArtifactRef* out) {
    if (out == nullptr || !payload.is_object()) {
        return false;
    }
    out->artifactType = payload.value("artifact_type", std::string());
    out->contractVersion = payload.value("contract_version", std::string());
    out->artifactId = payload.value("artifact_id", std::string());
    out->manifestPath = payload.value("manifest_path", std::string());
    out->artifactRootDir = payload.value("artifact_root_dir", std::string());
    return !out->artifactType.empty() && !out->contractVersion.empty() && !out->artifactId.empty();
}

bool loadAnalysisFromManifestJson(const json& manifest,
                                  AnalysisArtifact* out,
                                  std::string* errorCode,
                                  std::string* errorMessage) {
    if (!manifest.is_object()) {
        if (errorCode != nullptr) {
            *errorCode = "artifact_load_failed";
        }
        if (errorMessage != nullptr) {
            *errorMessage = "analysis manifest must be a json object";
        }
        return false;
    }
    if (manifest.value("artifact_type", std::string()) != kAnalysisArtifactType ||
        manifest.value("contract_version", std::string()) != kContractVersion) {
        if (errorCode != nullptr) {
            *errorCode = "unsupported_source_contract";
        }
        if (errorMessage != nullptr) {
            *errorMessage = "analysis manifest contract/version is not supported";
        }
        return false;
    }
    if (out == nullptr) {
        return true;
    }

    AnalysisArtifact artifact;
    artifact.analysisProfile = manifest.value("analysis_profile", std::string(kDefaultAnalyzerProfile));
    artifact.generatedAtUtc = manifest.value("generated_at_utc",
                                             manifest.value("replay_context", json::object()).value("generated_at_utc",
                                                                                                     std::string()));
    artifact.replayContext = manifest.value("replay_context", json::object());
    artifact.manifest = manifest;

    if (!parseArtifactRef(manifest.value("source_artifact", json::object()), &artifact.sourceArtifact) ||
        !parseArtifactRef(manifest.value("analysis_artifact", manifest), &artifact.analysisArtifact)) {
        if (errorCode != nullptr) {
            *errorCode = "artifact_load_failed";
        }
        if (errorMessage != nullptr) {
            *errorMessage = "analysis manifest is missing artifact identity";
        }
        return false;
    }

    for (const auto& finding : manifest.value("findings", json::array())) {
        FindingRecord record;
        record.findingId = finding.value("finding_id", std::string());
        record.severity = finding.value("severity", std::string());
        record.category = finding.value("category", std::string());
        record.summary = finding.value("summary", std::string());
        record.evidenceRefs = finding.value("evidence_refs", json::array());
        if (record.findingId.empty() || record.category.empty()) {
            if (errorCode != nullptr) {
                *errorCode = "artifact_load_failed";
            }
            if (errorMessage != nullptr) {
                *errorMessage = "analysis manifest contains an incomplete finding record";
            }
            return false;
        }
        artifact.findings.push_back(std::move(record));
    }

    *out = std::move(artifact);
    return true;
}

bool loadAnalysisFromPath(const fs::path& manifestPath,
                          AnalysisArtifact* out,
                          std::string* errorCode,
                          std::string* errorMessage) {
    if (!fs::exists(manifestPath)) {
        if (errorCode != nullptr) {
            *errorCode = "artifact_not_found";
        }
        if (errorMessage != nullptr) {
            *errorMessage = "analysis manifest was not found";
        }
        return false;
    }
    json manifest;
    std::string readError;
    if (!readJsonFile(manifestPath, &manifest, &readError)) {
        if (errorCode != nullptr) {
            *errorCode = "artifact_load_failed";
        }
        if (errorMessage != nullptr) {
            *errorMessage = readError;
        }
        return false;
    }
    return loadAnalysisFromManifestJson(manifest, out, errorCode, errorMessage);
}

bool parsePlaybookAction(const json& payload, PlaybookAction* out) {
    if (out == nullptr || !payload.is_object()) {
        return false;
    }
    out->actionId = payload.value("action_id", std::string());
    out->actionType = payload.value("action_type", std::string());
    out->findingId = payload.value("finding_id", std::string());
    out->title = payload.value("title", std::string());
    out->summary = payload.value("summary", std::string());
    out->suggestedTools = payload.value("suggested_tools", json::array());
    return !out->actionId.empty() && !out->actionType.empty() && !out->findingId.empty();
}

bool loadPlaybookFromManifestJson(const json& manifest,
                                  PlaybookArtifact* out,
                                  std::string* errorCode,
                                  std::string* errorMessage) {
    if (!manifest.is_object()) {
        if (errorCode != nullptr) {
            *errorCode = "artifact_load_failed";
        }
        if (errorMessage != nullptr) {
            *errorMessage = "playbook manifest must be a json object";
        }
        return false;
    }
    if (manifest.value("artifact_type", std::string()) != kPlaybookArtifactType ||
        manifest.value("contract_version", std::string()) != kContractVersion) {
        if (errorCode != nullptr) {
            *errorCode = "unsupported_source_contract";
        }
        if (errorMessage != nullptr) {
            *errorMessage = "playbook manifest contract/version is not supported";
        }
        return false;
    }
    if (out == nullptr) {
        return true;
    }

    PlaybookArtifact artifact;
    artifact.mode = manifest.value("mode", std::string(kDefaultPlaybookMode));
    artifact.generatedAtUtc = manifest.value("generated_at_utc",
                                             manifest.value("replay_context", json::object()).value("generated_at_utc",
                                                                                                     std::string()));
    artifact.replayContext = manifest.value("replay_context", json::object());
    artifact.manifest = manifest;

    if (!parseArtifactRef(manifest.value("analysis_artifact", json::object()), &artifact.analysisArtifact) ||
        !parseArtifactRef(manifest.value("playbook_artifact", manifest), &artifact.playbookArtifact)) {
        if (errorCode != nullptr) {
            *errorCode = "artifact_load_failed";
        }
        if (errorMessage != nullptr) {
            *errorMessage = "playbook manifest is missing artifact identity";
        }
        return false;
    }

    for (const auto& findingId : manifest.value("filtered_finding_ids", json::array())) {
        if (!findingId.is_string() || findingId.get_ref<const std::string&>().empty()) {
            if (errorCode != nullptr) {
                *errorCode = "artifact_load_failed";
            }
            if (errorMessage != nullptr) {
                *errorMessage = "playbook manifest contains an invalid filtered finding id";
            }
            return false;
        }
        artifact.filteredFindingIds.push_back(findingId.get<std::string>());
    }

    for (const auto& action : manifest.value("planned_actions", json::array())) {
        PlaybookAction parsed;
        if (!parsePlaybookAction(action, &parsed)) {
            if (errorCode != nullptr) {
                *errorCode = "artifact_load_failed";
            }
            if (errorMessage != nullptr) {
                *errorMessage = "playbook manifest contains an incomplete planned action";
            }
            return false;
        }
        artifact.plannedActions.push_back(std::move(parsed));
    }

    *out = std::move(artifact);
    return true;
}

bool loadPlaybookFromPath(const fs::path& manifestPath,
                          PlaybookArtifact* out,
                          std::string* errorCode,
                          std::string* errorMessage) {
    if (!fs::exists(manifestPath)) {
        if (errorCode != nullptr) {
            *errorCode = "artifact_not_found";
        }
        if (errorMessage != nullptr) {
            *errorMessage = "playbook manifest was not found";
        }
        return false;
    }
    json manifest;
    std::string readError;
    if (!readJsonFile(manifestPath, &manifest, &readError)) {
        if (errorCode != nullptr) {
            *errorCode = "artifact_load_failed";
        }
        if (errorMessage != nullptr) {
            *errorMessage = readError;
        }
        return false;
    }
    return loadPlaybookFromManifestJson(manifest, out, errorCode, errorMessage);
}

template <typename Artifact>
bool listArtifactsUnder(const fs::path& rootDir,
                        std::size_t limit,
                        std::vector<Artifact>* out,
                        std::string* errorCode,
                        std::string* errorMessage,
                        const std::function<bool(const fs::path&, Artifact*, std::string*, std::string*)>& loader) {
    if (out == nullptr) {
        if (errorCode != nullptr) {
            *errorCode = "artifact_load_failed";
        }
        if (errorMessage != nullptr) {
            *errorMessage = "missing output container";
        }
        return false;
    }

    std::error_code ec;
    if (!fs::exists(rootDir, ec)) {
        if (ec) {
            if (errorCode != nullptr) {
                *errorCode = "artifact_load_failed";
            }
            if (errorMessage != nullptr) {
                *errorMessage = "failed to inspect phase7 artifact directory";
            }
            return false;
        }
        out->clear();
        return true;
    }

    std::vector<fs::path> manifestPaths;
    for (const auto& entry : fs::directory_iterator(rootDir, ec)) {
        if (ec) {
            if (errorCode != nullptr) {
                *errorCode = "artifact_load_failed";
            }
            if (errorMessage != nullptr) {
                *errorMessage = "failed to iterate phase7 artifact directory";
            }
            return false;
        }
        if (!entry.is_directory()) {
            continue;
        }
        const fs::path manifestPath = entry.path() / "manifest.json";
        if (fs::exists(manifestPath)) {
            manifestPaths.push_back(manifestPath);
        }
    }

    std::sort(manifestPaths.begin(), manifestPaths.end(), std::greater<fs::path>());
    out->clear();
    const std::size_t cappedLimit = limit == 0 ? manifestPaths.size() : std::min(limit, manifestPaths.size());
    out->reserve(cappedLimit);
    for (std::size_t index = 0; index < manifestPaths.size() && out->size() < cappedLimit; ++index) {
        Artifact artifact;
        if (!loader(manifestPaths[index], &artifact, errorCode, errorMessage)) {
            return false;
        }
        out->push_back(std::move(artifact));
    }
    return true;
}

json playbookActionTools(std::string_view analysisProfile, std::string_view category) {
    const std::string normalizedProfile = normalizeToken(analysisProfile);
    if (category == "trace_integrity") {
        return json::array({"tapescript_read_artifact", "tapescript_read_session_quality", "tapescript_replay_snapshot"});
    }
    if (category == "instrument_identity") {
        return json::array({"tapescript_read_artifact", "tapescript_read_session_quality", "tapescript_read_order_case"});
    }
    if (category == "evidence_confidence") {
        return json::array({"tapescript_read_session_quality", "tapescript_read_artifact", "tapescript_read_incident"});
    }
    if (category == "incident_priority") {
        return json::array({"tapescript_read_incident", "tapescript_read_artifact", "tapescript_replay_snapshot"});
    }
    if (category == "incident_uncertainty") {
        return json::array({"tapescript_read_incident", "tapescript_read_session_quality", "tapescript_read_artifact"});
    }
    if (category == "timeline_hotspot") {
        return json::array({"tapescript_read_artifact", "tapescript_read_range", "tapescript_replay_snapshot"});
    }
    if (category == "fill_quality_risk") {
        if (normalizedProfile == normalizeToken(kAdverseSelectionAnalyzerProfile)) {
            return json::array({"tapescript_read_order_case", "tapescript_read_session_quality", "tapescript_replay_snapshot"});
        }
        if (normalizedProfile == normalizeToken(kOrderImpactAnalyzerProfile)) {
            return json::array({"tapescript_read_range", "tapescript_read_artifact", "tapescript_read_protected_window"});
        }
        return json::array({"tapescript_read_order_case", "tapescript_read_artifact", "tapescript_replay_snapshot"});
    }
    if (category == "adverse_selection_risk") {
        if (normalizedProfile == normalizeToken(kAdverseSelectionAnalyzerProfile)) {
            return json::array({"tapescript_read_order_case", "tapescript_read_range", "tapescript_replay_snapshot"});
        }
        return json::array({"tapescript_read_incident", "tapescript_read_order_case", "tapescript_read_session_quality"});
    }
    if (category == "market_impact_risk") {
        if (normalizedProfile == normalizeToken(kOrderImpactAnalyzerProfile)) {
            return json::array({"tapescript_read_range", "tapescript_read_protected_window", "tapescript_read_artifact"});
        }
        return json::array({"tapescript_read_incident", "tapescript_read_artifact", "tapescript_read_range"});
    }
    if (category == "display_instability_risk") {
        return json::array({"tapescript_read_incident", "tapescript_read_artifact", "tapescript_replay_snapshot"});
    }
    if (category == "liquidity_refill_behavior") {
        return json::array({"tapescript_read_incident", "tapescript_read_protected_window", "tapescript_read_range"});
    }
    if (category == "pressure_and_thinning") {
        return json::array({"tapescript_read_incident", "tapescript_read_artifact", "tapescript_read_session_overview"});
    }
    return json::array({"tapescript_read_artifact", "tapescript_read_session_overview"});
}

PlaybookAction playbookActionForFinding(const AnalysisArtifact& analysis, const FindingRecord& finding) {
    PlaybookAction action;
    if (finding.category == "trace_integrity") {
        action.actionType = "phase7.review_trace_integrity.v1";
        action.title = "Review trace integrity evidence";
    } else if (finding.category == "instrument_identity") {
        action.actionType = "phase7.review_instrument_identity.v1";
        action.title = "Review instrument identity evidence";
    } else if (finding.category == "evidence_confidence") {
        action.actionType = "phase7.review_evidence_confidence.v1";
        action.title = "Review evidence confidence limits";
    } else if (finding.category == "incident_priority") {
        action.actionType = "phase7.review_incident_priority.v1";
        action.title = "Review top incident context";
    } else if (finding.category == "incident_uncertainty") {
        action.actionType = "phase7.review_incident_uncertainty.v1";
        action.title = "Review incident uncertainty";
    } else if (finding.category == "timeline_hotspot") {
        action.actionType = "phase7.review_timeline_hotspot.v1";
        action.title = "Review timeline hotspot";
    } else if (finding.category == "fill_quality_risk") {
        if (normalizeToken(analysis.analysisProfile) == normalizeToken(kAdverseSelectionAnalyzerProfile)) {
            action.actionType = "phase7.review_fill_context_before_adverse_selection.v1";
            action.title = "Review fill context before adverse selection";
        } else if (normalizeToken(analysis.analysisProfile) == normalizeToken(kOrderImpactAnalyzerProfile)) {
            action.actionType = "phase7.review_fill_context_before_order_impact.v1";
            action.title = "Review fill context before order impact";
        } else {
            action.actionType = "phase7.review_fill_quality.v1";
            action.title = "Review fill-quality evidence";
        }
    } else if (finding.category == "adverse_selection_risk") {
        if (normalizeToken(analysis.analysisProfile) == normalizeToken(kAdverseSelectionAnalyzerProfile)) {
            action.actionType = "phase7.review_post_fill_adverse_selection.v1";
            action.title = "Review post-fill adverse-selection sequence";
        } else {
            action.actionType = "phase7.review_adverse_selection.v1";
            action.title = "Review adverse-selection risk";
        }
    } else if (finding.category == "market_impact_risk") {
        if (normalizeToken(analysis.analysisProfile) == normalizeToken(kOrderImpactAnalyzerProfile)) {
            action.actionType = "phase7.review_order_impact_footprint.v1";
            action.title = "Review order-impact footprint";
        } else {
            action.actionType = "phase7.review_market_impact.v1";
            action.title = "Review market-impact evidence";
        }
    } else if (finding.category == "display_instability_risk") {
        action.actionType = "phase7.review_display_instability.v1";
        action.title = "Review display instability";
    } else if (finding.category == "liquidity_refill_behavior") {
        action.actionType = "phase7.review_liquidity_refill.v1";
        action.title = "Review refill and absorption behavior";
    } else if (finding.category == "pressure_and_thinning") {
        action.actionType = "phase7.review_pressure_thinning.v1";
        action.title = "Review pressure and thinning";
    } else {
        action.actionType = "phase7.review_analysis_summary.v1";
        action.title = "Review analysis summary";
    }
    action.findingId = finding.findingId;
    action.summary = finding.summary;
    action.suggestedTools = playbookActionTools(analysis.analysisProfile, finding.category);
    action.actionId = "phase7-action-" + fnv1aHex(
        analysis.analysisArtifact.artifactId + "|" + finding.findingId + "|" + action.actionType);
    return action;
}

json executionPolicyForLedger() {
    const json reviewPolicy = {
        {"actor_required", true},
        {"comment_required_statuses", json::array({kLedgerEntryStatusBlocked, kLedgerEntryStatusNeedsInfo})},
        {"distinct_approvals_required", 2},
        {"approval_status", kLedgerEntryStatusApproved},
        {"terminal_statuses", json::array({
            kLedgerEntryStatusApproved,
            kLedgerEntryStatusBlocked,
            kLedgerEntryStatusNeedsInfo,
            kLedgerEntryStatusNotApplicable
        })}
    };
    return {
        {"apply_supported", false},
        {"deferred_reason", "live apply remains deferred until Phase 7 has a full execution/audit model"},
        {"recommended_next_step", "review_only"},
        {"manual_confirmation_required", true},
        {"review_policy", reviewPolicy}
    };
}

bool isSupportedLedgerEntryStatus(std::string_view reviewStatus) {
    return reviewStatus == kDefaultLedgerEntryStatus ||
        reviewStatus == kLedgerEntryStatusApproved ||
        reviewStatus == kLedgerEntryStatusBlocked ||
        reviewStatus == kLedgerEntryStatusNeedsInfo ||
        reviewStatus == kLedgerEntryStatusNotApplicable;
}

bool isTerminalLedgerEntryStatus(std::string_view reviewStatus) {
    return reviewStatus == kLedgerEntryStatusApproved ||
        reviewStatus == kLedgerEntryStatusBlocked ||
        reviewStatus == kLedgerEntryStatusNeedsInfo ||
        reviewStatus == kLedgerEntryStatusNotApplicable;
}

std::string trimAscii(std::string_view value) {
    std::size_t first = 0;
    while (first < value.size() && std::isspace(static_cast<unsigned char>(value[first])) != 0) {
        ++first;
    }
    std::size_t last = value.size();
    while (last > first && std::isspace(static_cast<unsigned char>(value[last - 1])) != 0) {
        --last;
    }
    return std::string(value.substr(first, last - first));
}

std::size_t reviewPolicyDistinctApprovalCount(const json& reviewPolicy) {
    if (!reviewPolicy.is_object()) {
        return 2;
    }
    if (reviewPolicy.contains("distinct_approvals_required") &&
        reviewPolicy.at("distinct_approvals_required").is_number_unsigned()) {
        return reviewPolicy.at("distinct_approvals_required").get<std::size_t>();
    }
    if (reviewPolicy.contains("distinct_approvals_required") &&
        reviewPolicy.at("distinct_approvals_required").is_number_integer()) {
        const auto value = reviewPolicy.at("distinct_approvals_required").get<long long>();
        return value > 0 ? static_cast<std::size_t>(value) : 2ULL;
    }
    return 2;
}

bool reviewPolicyActorRequired(const json& reviewPolicy) {
    return !reviewPolicy.is_object() || reviewPolicy.value("actor_required", true);
}

bool reviewPolicyCommentRequired(const json& reviewPolicy, std::string_view reviewStatus) {
    if (!reviewPolicy.is_object()) {
        return reviewStatus == kLedgerEntryStatusBlocked || reviewStatus == kLedgerEntryStatusNeedsInfo;
    }
    const json statuses = reviewPolicy.value("comment_required_statuses", json::array());
    if (!statuses.is_array()) {
        return false;
    }
    for (const auto& item : statuses) {
        if (item.is_string() && item.get_ref<const std::string&>() == reviewStatus) {
            return true;
        }
    }
    return false;
}

std::unordered_map<std::string, std::set<std::string>> ledgerReviewActorsByEntry(const ExecutionLedgerArtifact& artifact,
                                                                                  bool approvalsOnly) {
    std::unordered_map<std::string, std::set<std::string>> actorsByEntry;
    if (!artifact.auditTrail.is_array()) {
        return actorsByEntry;
    }
    for (const auto& event : artifact.auditTrail) {
        if (!event.is_object() || event.value("event_type", std::string()) != "review_status_recorded") {
            continue;
        }
        const std::string actor = trimAscii(event.value("actor", std::string()));
        if (actor.empty()) {
            continue;
        }
        const std::string reviewStatus = event.value("review_status", std::string());
        if (approvalsOnly && reviewStatus != kLedgerEntryStatusApproved) {
            continue;
        }
        for (const auto& entryId : event.value("updated_entry_ids", json::array())) {
            if (!entryId.is_string()) {
                continue;
            }
            actorsByEntry[entryId.get<std::string>()].insert(actor);
        }
    }
    return actorsByEntry;
}

std::set<std::string> distinctLedgerReviewActors(const ExecutionLedgerArtifact& artifact) {
    std::set<std::string> actors;
    if (!artifact.auditTrail.is_array()) {
        return actors;
    }
    for (const auto& event : artifact.auditTrail) {
        if (!event.is_object() || event.value("event_type", std::string()) != "review_status_recorded") {
            continue;
        }
        const std::string actor = trimAscii(event.value("actor", std::string()));
        if (!actor.empty()) {
            actors.insert(actor);
        }
    }
    return actors;
}

std::string summarizeLedgerStatus(const std::vector<ExecutionLedgerEntry>& entries,
                                  std::size_t requiredApprovals) {
    if (entries.empty()) {
        return kDefaultLedgerStatus;
    }

    std::size_t pendingCount = 0;
    std::size_t blockedCount = 0;
    std::size_t needsInfoCount = 0;
    std::size_t approvedCount = 0;
    std::size_t approvedReadyCount = 0;
    std::size_t notApplicableCount = 0;
    for (const auto& entry : entries) {
        if (entry.reviewStatus == kDefaultLedgerEntryStatus) {
            ++pendingCount;
        } else if (entry.reviewStatus == kLedgerEntryStatusBlocked) {
            ++blockedCount;
        } else if (entry.reviewStatus == kLedgerEntryStatusNeedsInfo) {
            ++needsInfoCount;
        } else if (entry.reviewStatus == kLedgerEntryStatusApproved) {
            ++approvedCount;
            if (entry.approvalReviewerCount >= requiredApprovals) {
                ++approvedReadyCount;
            }
        } else if (entry.reviewStatus == kLedgerEntryStatusNotApplicable) {
            ++notApplicableCount;
        }
    }

    if (blockedCount > 0) {
        return kLedgerStatusBlocked;
    }
    if (needsInfoCount > 0) {
        return kLedgerStatusNeedsInformation;
    }
    if (pendingCount == entries.size()) {
        return kDefaultLedgerStatus;
    }
    if (pendingCount > 0) {
        return kLedgerStatusInProgress;
    }
    if (approvedCount == 0 && notApplicableCount == entries.size()) {
        return kLedgerStatusCompleted;
    }
    if (approvedCount > 0 && approvedReadyCount == approvedCount) {
        return kLedgerStatusReadyForExecution;
    }
    if (approvedCount > 0) {
        return kLedgerStatusWaitingApproval;
    }
    return kLedgerStatusCompleted;
}

ExecutionLedgerReviewSummary summarizeExecutionLedgerReviewSummaryEntries(
    const std::vector<ExecutionLedgerEntry>& entries,
    std::size_t requiredApprovals,
    std::size_t distinctReviewerCount,
    std::string_view ledgerStatus) {
    ExecutionLedgerReviewSummary summary;
    summary.requiredApprovalCount = requiredApprovals;
    summary.distinctReviewerCount = distinctReviewerCount;
    summary.readyForExecution = (ledgerStatus == kLedgerStatusReadyForExecution);
    for (const auto& entry : entries) {
        if (entry.reviewStatus != kLedgerEntryStatusNotApplicable) {
            ++summary.actionableEntryCount;
        }
        if (entry.reviewStatus == kLedgerEntryStatusApproved) {
            ++summary.approvedCount;
            ++summary.reviewedCount;
            if (entry.approvalThresholdMet) {
                ++summary.readyEntryCount;
            } else {
                ++summary.waitingApprovalCount;
            }
        } else if (entry.reviewStatus == kLedgerEntryStatusBlocked) {
            ++summary.blockedCount;
            ++summary.reviewedCount;
        } else if (entry.reviewStatus == kLedgerEntryStatusNeedsInfo) {
            ++summary.needsInfoCount;
            ++summary.reviewedCount;
        } else if (entry.reviewStatus == kLedgerEntryStatusNotApplicable) {
            ++summary.notApplicableCount;
            ++summary.reviewedCount;
        } else {
            ++summary.pendingReviewCount;
        }
    }
    return summary;
}

void annotateExecutionLedgerReviewState(ExecutionLedgerArtifact* artifact) {
    if (artifact == nullptr) {
        return;
    }
    if (!artifact->executionPolicy.is_object()) {
        artifact->executionPolicy = executionPolicyForLedger();
    }
    artifact->reviewPolicy = artifact->executionPolicy.value("review_policy", json::object());
    if (!artifact->reviewPolicy.is_object() || artifact->reviewPolicy.empty()) {
        artifact->reviewPolicy = executionPolicyForLedger().value("review_policy", json::object());
        artifact->executionPolicy["review_policy"] = artifact->reviewPolicy;
    }
    const std::size_t requiredApprovals = reviewPolicyDistinctApprovalCount(artifact->reviewPolicy);
    const auto reviewActors = ledgerReviewActorsByEntry(*artifact, false);
    const auto approvalActors = ledgerReviewActorsByEntry(*artifact, true);
    const auto distinctActors = distinctLedgerReviewActors(*artifact);
    for (auto& entry : artifact->entries) {
        const auto actorIt = reviewActors.find(entry.entryId);
        entry.distinctReviewerCount = actorIt == reviewActors.end() ? 0 : actorIt->second.size();
        const auto approvalIt = approvalActors.find(entry.entryId);
        entry.approvalReviewerCount = approvalIt == approvalActors.end() ? 0 : approvalIt->second.size();
        entry.approvalThresholdMet =
            entry.reviewStatus == kLedgerEntryStatusApproved &&
            entry.approvalReviewerCount >= requiredApprovals;
    }
    artifact->ledgerStatus = summarizeLedgerStatus(artifact->entries, requiredApprovals);
    artifact->executionPolicy["review_state"] = {
        {"required_distinct_approvals", requiredApprovals},
        {"distinct_reviewer_count", distinctActors.size()},
        {"ready_for_execution", artifact->ledgerStatus == kLedgerStatusReadyForExecution},
        {"aggregate_status", artifact->ledgerStatus}
    };
}

ExecutionLedgerEntry executionLedgerEntryForAction(const PlaybookArtifact& playbook, const PlaybookAction& action) {
    ExecutionLedgerEntry entry;
    entry.entryId = "phase7-ledger-entry-" +
        fnv1aHex(playbook.playbookArtifact.artifactId + "|" + action.actionId + "|execution-ledger");
    entry.actionId = action.actionId;
    entry.actionType = action.actionType;
    entry.findingId = action.findingId;
    entry.reviewStatus = kDefaultLedgerEntryStatus;
    entry.requiresManualConfirmation = true;
    entry.title = action.title;
    entry.summary = action.summary;
    entry.distinctReviewerCount = 0;
    entry.approvalReviewerCount = 0;
    entry.approvalThresholdMet = false;
    entry.suggestedTools = action.suggestedTools;
    return entry;
}

json auditTrailForLedgerCreation(const PlaybookArtifact& playbook,
                                 const std::string& generatedAtUtc,
                                 std::string_view ledgerStatus) {
    return json::array({
        json{
            {"event_id", "phase7-ledger-event-" +
                             fnv1aHex(playbook.playbookArtifact.artifactId + "|ledger-created")},
            {"event_type", "ledger_created"},
            {"generated_at_utc", generatedAtUtc},
            {"status", ledgerStatus},
            {"message", "Execution remains deferred; this ledger is a review/audit record only."}
        }
    });
}

json auditEventForLedgerReview(const ExecutionLedgerArtifact& ledger,
                               const std::vector<ExecutionLedgerEntry>& previousEntries,
                               const std::vector<std::string>& updatedEntryIds,
                               std::string_view reviewStatus,
                               std::string_view actor,
                               std::string_view comment,
                               std::string_view previousLedgerStatus,
                               const std::string& generatedAtUtc) {
    json previousStatuses = json::array();
    for (const auto& entry : previousEntries) {
        previousStatuses.push_back({
            {"entry_id", entry.entryId},
            {"review_status", entry.reviewStatus}
        });
    }

    std::ostringstream seed;
    seed << ledger.ledgerArtifact.artifactId << "|" << generatedAtUtc << "|" << reviewStatus << "|" << actor;
    for (const auto& entryId : updatedEntryIds) {
        seed << "|" << entryId;
    }

    std::ostringstream message;
    message << "Recorded " << updatedEntryIds.size() << " review entr";
    message << (updatedEntryIds.size() == 1 ? "y" : "ies");
    message << " as `" << reviewStatus << "` by `" << actor << "`.";
    if (!comment.empty()) {
        message << " Comment: " << comment;
    }

    return json{
        {"event_id", "phase7-ledger-event-" + fnv1aHex(seed.str())},
        {"event_type", "review_status_recorded"},
        {"generated_at_utc", generatedAtUtc},
        {"actor", actor},
        {"review_status", reviewStatus},
        {"comment", comment.empty() ? json(nullptr) : json(comment)},
        {"updated_entry_ids", updatedEntryIds},
        {"previous_entry_statuses", std::move(previousStatuses)},
        {"previous_ledger_status", previousLedgerStatus.empty() ? json(nullptr) : json(previousLedgerStatus)},
        {"ledger_status", ledger.ledgerStatus},
        {"message", message.str()}
    };
}

json manifestForPlaybook(const PlaybookArtifact& playbook) {
    json filteredFindingIds = json::array();
    for (const auto& findingId : playbook.filteredFindingIds) {
        filteredFindingIds.push_back(findingId);
    }
    json plannedActions = json::array();
    for (const auto& action : playbook.plannedActions) {
        plannedActions.push_back(playbookActionToJson(action));
    }
    return {
        {"artifact_type", playbook.playbookArtifact.artifactType},
        {"contract_version", playbook.playbookArtifact.contractVersion},
        {"artifact_id", playbook.playbookArtifact.artifactId},
        {"manifest_path", playbook.playbookArtifact.manifestPath},
        {"artifact_root_dir", playbook.playbookArtifact.artifactRootDir},
        {"mode", playbook.mode},
        {"generated_at_utc", playbook.generatedAtUtc},
        {"analysis_artifact", artifactRefToJson(playbook.analysisArtifact)},
        {"playbook_artifact", artifactRefToJson(playbook.playbookArtifact)},
        {"replay_context", playbook.replayContext},
        {"filtered_finding_ids", std::move(filteredFindingIds)},
        {"planned_actions", std::move(plannedActions)}
    };
}

bool parseExecutionLedgerEntry(const json& payload, ExecutionLedgerEntry* out) {
    if (out == nullptr || !payload.is_object()) {
        return false;
    }
    out->entryId = payload.value("entry_id", std::string());
    out->actionId = payload.value("action_id", std::string());
    out->actionType = payload.value("action_type", std::string());
    out->findingId = payload.value("finding_id", std::string());
    out->reviewStatus = payload.value("review_status", std::string(kDefaultLedgerEntryStatus));
    out->requiresManualConfirmation = payload.value("requires_manual_confirmation", true);
    out->title = payload.value("title", std::string());
    out->summary = payload.value("summary", std::string());
    out->reviewedAtUtc = payload.contains("reviewed_at_utc") && payload.at("reviewed_at_utc").is_string()
        ? payload.at("reviewed_at_utc").get<std::string>()
        : std::string();
    out->reviewedBy = payload.contains("reviewed_by") && payload.at("reviewed_by").is_string()
        ? payload.at("reviewed_by").get<std::string>()
        : std::string();
    out->reviewComment = payload.contains("review_comment") && payload.at("review_comment").is_string()
        ? payload.at("review_comment").get<std::string>()
        : std::string();
    out->suggestedTools = payload.value("suggested_tools", json::array());
    return !out->entryId.empty() && !out->actionId.empty() && !out->actionType.empty();
}

json manifestForExecutionLedger(const ExecutionLedgerArtifact& ledger) {
    json filteredFindingIds = json::array();
    for (const auto& findingId : ledger.filteredFindingIds) {
        filteredFindingIds.push_back(findingId);
    }
    json entries = json::array();
    for (const auto& entry : ledger.entries) {
        entries.push_back(executionLedgerEntryToJson(entry));
    }
    return {
        {"artifact_type", ledger.ledgerArtifact.artifactType},
        {"contract_version", ledger.ledgerArtifact.contractVersion},
        {"artifact_id", ledger.ledgerArtifact.artifactId},
        {"manifest_path", ledger.ledgerArtifact.manifestPath},
        {"artifact_root_dir", ledger.ledgerArtifact.artifactRootDir},
        {"mode", ledger.mode},
        {"generated_at_utc", ledger.generatedAtUtc},
        {"ledger_status", ledger.ledgerStatus},
        {"source_artifact", artifactRefToJson(ledger.sourceArtifact)},
        {"analysis_artifact", artifactRefToJson(ledger.analysisArtifact)},
        {"playbook_artifact", artifactRefToJson(ledger.playbookArtifact)},
        {"execution_ledger", artifactRefToJson(ledger.ledgerArtifact)},
        {"execution_policy", ledger.executionPolicy},
        {"replay_context", ledger.replayContext},
        {"filtered_finding_ids", std::move(filteredFindingIds)},
        {"entry_count", entries.size()},
        {"entries", std::move(entries)},
        {"audit_trail", ledger.auditTrail}
    };
}

bool loadExecutionLedgerFromManifestJson(const json& manifest,
                                         ExecutionLedgerArtifact* out,
                                         std::string* errorCode,
                                         std::string* errorMessage) {
    if (!manifest.is_object()) {
        if (errorCode != nullptr) {
            *errorCode = "artifact_load_failed";
        }
        if (errorMessage != nullptr) {
            *errorMessage = "execution ledger manifest must be a json object";
        }
        return false;
    }
    if (manifest.value("artifact_type", std::string()) != kExecutionLedgerArtifactType ||
        manifest.value("contract_version", std::string()) != kContractVersion) {
        if (errorCode != nullptr) {
            *errorCode = "unsupported_source_contract";
        }
        if (errorMessage != nullptr) {
            *errorMessage = "execution ledger manifest contract/version is not supported";
        }
        return false;
    }
    if (out == nullptr) {
        return true;
    }

    ExecutionLedgerArtifact artifact;
    artifact.mode = manifest.value("mode", std::string(kDefaultPlaybookMode));
    artifact.generatedAtUtc = manifest.value("generated_at_utc",
                                             manifest.value("replay_context", json::object())
                                                 .value("generated_at_utc", std::string()));
    artifact.ledgerStatus = manifest.value("ledger_status", std::string(kDefaultLedgerStatus));
    artifact.executionPolicy = manifest.value("execution_policy", json::object());
    artifact.reviewPolicy = artifact.executionPolicy.value("review_policy", json::object());
    artifact.replayContext = manifest.value("replay_context", json::object());
    artifact.auditTrail = manifest.value("audit_trail", json::array());
    artifact.manifest = manifest;

    if (!parseArtifactRef(manifest.value("source_artifact", json::object()), &artifact.sourceArtifact) ||
        !parseArtifactRef(manifest.value("analysis_artifact", json::object()), &artifact.analysisArtifact) ||
        !parseArtifactRef(manifest.value("playbook_artifact", json::object()), &artifact.playbookArtifact) ||
        !parseArtifactRef(manifest.value("execution_ledger", manifest), &artifact.ledgerArtifact)) {
        if (errorCode != nullptr) {
            *errorCode = "artifact_load_failed";
        }
        if (errorMessage != nullptr) {
            *errorMessage = "execution ledger manifest is missing artifact identity";
        }
        return false;
    }

    for (const auto& findingId : manifest.value("filtered_finding_ids", json::array())) {
        if (!findingId.is_string() || findingId.get_ref<const std::string&>().empty()) {
            if (errorCode != nullptr) {
                *errorCode = "artifact_load_failed";
            }
            if (errorMessage != nullptr) {
                *errorMessage = "execution ledger manifest contains an invalid filtered finding id";
            }
            return false;
        }
        artifact.filteredFindingIds.push_back(findingId.get<std::string>());
    }

    for (const auto& entry : manifest.value("entries", json::array())) {
        ExecutionLedgerEntry parsed;
        if (!parseExecutionLedgerEntry(entry, &parsed)) {
            if (errorCode != nullptr) {
                *errorCode = "artifact_load_failed";
            }
            if (errorMessage != nullptr) {
                *errorMessage = "execution ledger manifest contains an incomplete entry";
            }
            return false;
        }
        artifact.entries.push_back(std::move(parsed));
    }

    annotateExecutionLedgerReviewState(&artifact);

    *out = std::move(artifact);
    return true;
}

bool loadExecutionLedgerFromPath(const fs::path& manifestPath,
                                 ExecutionLedgerArtifact* out,
                                 std::string* errorCode,
                                 std::string* errorMessage) {
    if (!fs::exists(manifestPath)) {
        if (errorCode != nullptr) {
            *errorCode = "artifact_not_found";
        }
        if (errorMessage != nullptr) {
            *errorMessage = "execution ledger manifest was not found";
        }
        return false;
    }
    json manifest;
    std::string readError;
    if (!readJsonFile(manifestPath, &manifest, &readError)) {
        if (errorCode != nullptr) {
            *errorCode = "artifact_load_failed";
        }
        if (errorMessage != nullptr) {
            *errorMessage = readError;
        }
        return false;
    }
    return loadExecutionLedgerFromManifestJson(manifest, out, errorCode, errorMessage);
}

} // namespace

json artifactRefToJson(const ArtifactRef& artifact) {
    return {
        {"artifact_type", artifact.artifactType},
        {"contract_version", artifact.contractVersion},
        {"artifact_id", artifact.artifactId},
        {"manifest_path", artifact.manifestPath},
        {"artifact_root_dir", artifact.artifactRootDir}
    };
}

json findingToJson(const FindingRecord& finding) {
    return {
        {"finding_id", finding.findingId},
        {"severity", finding.severity},
        {"category", finding.category},
        {"summary", finding.summary},
        {"evidence_refs", finding.evidenceRefs}
    };
}

json playbookActionToJson(const PlaybookAction& action) {
    return {
        {"action_id", action.actionId},
        {"action_type", action.actionType},
        {"finding_id", action.findingId},
        {"title", action.title},
        {"summary", action.summary},
        {"suggested_tools", action.suggestedTools}
    };
}

json executionLedgerEntryToJson(const ExecutionLedgerEntry& entry) {
    return {
        {"entry_id", entry.entryId},
        {"action_id", entry.actionId},
        {"action_type", entry.actionType},
        {"finding_id", entry.findingId},
        {"review_status", entry.reviewStatus},
        {"requires_manual_confirmation", entry.requiresManualConfirmation},
        {"title", entry.title},
        {"summary", entry.summary},
        {"reviewed_at_utc", entry.reviewedAtUtc.empty() ? json(nullptr) : json(entry.reviewedAtUtc)},
        {"reviewed_by", entry.reviewedBy.empty() ? json(nullptr) : json(entry.reviewedBy)},
        {"review_comment", entry.reviewComment.empty() ? json(nullptr) : json(entry.reviewComment)},
        {"distinct_reviewer_count", entry.distinctReviewerCount},
        {"approval_reviewer_count", entry.approvalReviewerCount},
        {"approval_threshold_met", entry.approvalThresholdMet},
        {"suggested_tools", entry.suggestedTools}
    };
}

ExecutionLedgerReviewSummary summarizeExecutionLedgerReviewSummary(const ExecutionLedgerArtifact& artifact) {
    return summarizeExecutionLedgerReviewSummaryEntries(
        artifact.entries,
        reviewPolicyDistinctApprovalCount(artifact.reviewPolicy),
        distinctLedgerReviewActors(artifact).size(),
        artifact.ledgerStatus);
}

json executionLedgerReviewSummaryToJson(const ExecutionLedgerReviewSummary& summary) {
    return {
        {"pending_review_count", summary.pendingReviewCount},
        {"approved_count", summary.approvedCount},
        {"blocked_count", summary.blockedCount},
        {"needs_info_count", summary.needsInfoCount},
        {"not_applicable_count", summary.notApplicableCount},
        {"reviewed_count", summary.reviewedCount},
        {"waiting_approval_count", summary.waitingApprovalCount},
        {"ready_entry_count", summary.readyEntryCount},
        {"actionable_entry_count", summary.actionableEntryCount},
        {"distinct_reviewer_count", summary.distinctReviewerCount},
        {"required_approval_count", summary.requiredApprovalCount},
        {"ready_for_execution", summary.readyForExecution}
    };
}

json latestExecutionLedgerAuditSummary(const ExecutionLedgerArtifact& artifact) {
    if (!artifact.auditTrail.is_array() || artifact.auditTrail.empty()) {
        return {
            {"event_type", nullptr},
            {"generated_at_utc", nullptr},
            {"actor", nullptr},
            {"review_status", nullptr},
            {"message", nullptr},
            {"ledger_status", nullptr}
        };
    }
    const auto& event = artifact.auditTrail.back();
    return {
        {"event_type", event.contains("event_type") && event.at("event_type").is_string()
                           ? event.at("event_type")
                           : json(nullptr)},
        {"generated_at_utc", event.contains("generated_at_utc") && event.at("generated_at_utc").is_string()
                                 ? event.at("generated_at_utc")
                                 : json(nullptr)},
        {"actor", event.contains("actor") && event.at("actor").is_string()
                      ? event.at("actor")
                      : json(nullptr)},
        {"review_status", event.contains("review_status") && event.at("review_status").is_string()
                               ? event.at("review_status")
                               : json(nullptr)},
        {"message", event.contains("message") && event.at("message").is_string()
                        ? event.at("message")
                        : json(nullptr)},
        {"ledger_status", event.contains("ledger_status") && event.at("ledger_status").is_string()
                              ? event.at("ledger_status")
                              : event.contains("status") && event.at("status").is_string()
                                    ? event.at("status")
                                    : json(nullptr)}
    };
}

bool listAnalyzerProfiles(std::vector<AnalyzerProfileSpec>* out,
                          std::string* errorCode,
                          std::string* errorMessage) {
    if (out != nullptr) {
        *out = analyzerProfiles();
    }
    if (errorCode != nullptr) {
        errorCode->clear();
    }
    if (errorMessage != nullptr) {
        errorMessage->clear();
    }
    return true;
}

bool loadAnalyzerProfile(const std::string& analysisProfile,
                         AnalyzerProfileSpec* out,
                         std::string* errorCode,
                         std::string* errorMessage) {
    const AnalyzerProfileSpec* profile = findAnalyzerProfileSpec(analysisProfile);
    if (profile == nullptr) {
        if (errorCode != nullptr) {
            *errorCode = "unsupported_profile";
        }
        if (errorMessage != nullptr) {
            *errorMessage = "requested analysis_profile is not supported in this Phase 7 slice";
        }
        return false;
    }
    if (out != nullptr) {
        *out = *profile;
    }
    if (errorCode != nullptr) {
        errorCode->clear();
    }
    if (errorMessage != nullptr) {
        errorMessage->clear();
    }
    return true;
}

bool runAnalyzerFromBundlePath(const std::string& bundlePath,
                               const std::string& analysisProfile,
                               AnalysisArtifact* out,
                               bool* created,
                               std::string* errorCode,
                               std::string* errorMessage) {
    tape_bundle::PortableBundleInspection inspection;
    std::string inspectError;
    if (!tape_bundle::inspectPortableBundle(bundlePath, &inspection, &inspectError)) {
        if (errorCode != nullptr) {
            *errorCode = (inspectError == "bundle_path does not exist") ? "artifact_not_found" : "artifact_load_failed";
        }
        if (errorMessage != nullptr) {
            *errorMessage = inspectError;
        }
        return false;
    }
    if (inspection.bundleType != "case_bundle" && inspection.bundleType != "session_bundle") {
        if (errorCode != nullptr) {
            *errorCode = "unsupported_source_contract";
        }
        if (errorMessage != nullptr) {
            *errorMessage = "bundle type is not supported for phase7 analyzer input";
        }
        return false;
    }

    AnalyzerProfileSpec profile;
    if (!loadAnalyzerProfile(analysisProfile, &profile, errorCode, errorMessage)) {
        return false;
    }

    AnalysisArtifact artifact;
    artifact.analysisProfile = profile.analysisProfile;
    artifact.sourceArtifact = sourceArtifactRefFromInspection(inspection);
    artifact.analysisArtifact = analysisArtifactRefForBundle(inspection, artifact.analysisProfile);
    artifact.replayContext = replayContextFromInspection(inspection);
    artifact.generatedAtUtc = artifact.replayContext.value("generated_at_utc", std::string());

    const fs::path manifestPath = artifact.analysisArtifact.manifestPath;
    if (fs::exists(manifestPath)) {
        if (created != nullptr) {
            *created = false;
        }
        return loadAnalysisFromPath(manifestPath, out, errorCode, errorMessage);
    }

    std::string directoryError;
    if (!ensureDirectoryExists(artifact.analysisArtifact.artifactRootDir, &directoryError)) {
        if (errorCode != nullptr) {
            *errorCode = "analysis_failed";
        }
        if (errorMessage != nullptr) {
            *errorMessage = directoryError;
        }
        return false;
    }

    artifact.findings = buildFindings(artifact.analysisArtifact, inspection, profile);
    artifact.manifest = manifestForAnalysis(artifact);

    std::string writeError;
    if (!writeJsonTextFile(manifestPath, artifact.manifest, &writeError)) {
        if (errorCode != nullptr) {
            *errorCode = "analysis_failed";
        }
        if (errorMessage != nullptr) {
            *errorMessage = writeError;
        }
        return false;
    }

    if (out != nullptr) {
        *out = std::move(artifact);
    }
    if (created != nullptr) {
        *created = true;
    }
    return true;
}

bool loadAnalysisArtifact(const std::string& manifestPath,
                          const std::string& artifactId,
                          AnalysisArtifact* out,
                          std::string* errorCode,
                          std::string* errorMessage) {
    const bool hasManifest = !manifestPath.empty();
    const bool hasArtifactId = !artifactId.empty();
    if (hasManifest == hasArtifactId) {
        if (errorCode != nullptr) {
            *errorCode = "invalid_arguments";
        }
        if (errorMessage != nullptr) {
            *errorMessage = "exactly one of analysis_manifest_path or analysis_artifact_id is required";
        }
        return false;
    }

    const fs::path resolvedPath = hasManifest
        ? fs::path(manifestPath)
        : phase7RootDir() / "analysis" / artifactId / "manifest.json";
    return loadAnalysisFromPath(resolvedPath, out, errorCode, errorMessage);
}

bool listAnalysisArtifacts(std::size_t limit,
                           std::vector<AnalysisArtifact>* out,
                           std::string* errorCode,
                           std::string* errorMessage) {
    return listArtifactsUnder<AnalysisArtifact>(
        phase7RootDir() / "analysis",
        limit,
        out,
        errorCode,
        errorMessage,
        [](const fs::path& manifestPath, AnalysisArtifact* artifact, std::string* code, std::string* message) {
            return loadAnalysisFromPath(manifestPath, artifact, code, message);
        });
}

bool buildGuardedPlaybook(const std::string& manifestPath,
                          const std::string& artifactId,
                          const std::vector<std::string>& findingIds,
                          const std::string& mode,
                          PlaybookArtifact* out,
                          bool* created,
                          std::string* errorCode,
                          std::string* errorMessage) {
    AnalysisArtifact analysis;
    if (!loadAnalysisArtifact(manifestPath, artifactId, &analysis, errorCode, errorMessage)) {
        return false;
    }

    if (!mode.empty() && mode != kDefaultPlaybookMode && mode != kApplyPlaybookMode) {
        if (errorCode != nullptr) {
            *errorCode = "invalid_arguments";
        }
        if (errorMessage != nullptr) {
            *errorMessage = "mode must be dry_run or apply";
        }
        return false;
    }

    PlaybookArtifact playbook;
    playbook.analysisArtifact = analysis.analysisArtifact;
    playbook.mode = mode.empty() ? kDefaultPlaybookMode : mode;
    playbook.replayContext = analysis.replayContext;

    std::vector<FindingRecord> selectedFindings;
    if (findingIds.empty()) {
        selectedFindings = analysis.findings;
    } else {
        for (const auto& findingId : findingIds) {
            const auto it = std::find_if(
                analysis.findings.begin(),
                analysis.findings.end(),
                [&](const FindingRecord& finding) { return finding.findingId == findingId; });
            if (it == analysis.findings.end()) {
                if (errorCode != nullptr) {
                    *errorCode = "finding_not_found";
                }
                if (errorMessage != nullptr) {
                    *errorMessage = "playbook filters reference a finding that is not present in the analysis artifact";
                }
                return false;
            }
            selectedFindings.push_back(*it);
            playbook.filteredFindingIds.push_back(findingId);
        }
    }

    if (playbook.filteredFindingIds.empty()) {
        for (const auto& finding : selectedFindings) {
            playbook.filteredFindingIds.push_back(finding.findingId);
        }
    }

    for (const auto& finding : selectedFindings) {
        playbook.plannedActions.push_back(playbookActionForFinding(analysis, finding));
    }

    playbook.playbookArtifact = playbookArtifactRefForAnalysis(analysis, playbook.filteredFindingIds, playbook.mode);
    const fs::path manifestFile = playbook.playbookArtifact.manifestPath;
    if (fs::exists(manifestFile)) {
        if (created != nullptr) {
            *created = false;
        }
        return loadPlaybookFromPath(manifestFile, out, errorCode, errorMessage);
    }

    playbook.generatedAtUtc = utcTimestampIso8601Now();
    playbook.manifest = manifestForPlaybook(playbook);

    std::string directoryError;
    if (!ensureDirectoryExists(playbook.playbookArtifact.artifactRootDir, &directoryError)) {
        if (errorCode != nullptr) {
            *errorCode = "analysis_failed";
        }
        if (errorMessage != nullptr) {
            *errorMessage = directoryError;
        }
        return false;
    }

    std::string writeError;
    if (!writeJsonTextFile(playbook.playbookArtifact.manifestPath, playbook.manifest, &writeError)) {
        if (errorCode != nullptr) {
            *errorCode = "analysis_failed";
        }
        if (errorMessage != nullptr) {
            *errorMessage = writeError;
        }
        return false;
    }

    if (out != nullptr) {
        *out = std::move(playbook);
    }
    if (created != nullptr) {
        *created = true;
    }
    return true;
}

bool loadPlaybookArtifact(const std::string& manifestPath,
                          const std::string& artifactId,
                          PlaybookArtifact* out,
                          std::string* errorCode,
                          std::string* errorMessage) {
    const bool hasManifest = !manifestPath.empty();
    const bool hasArtifactId = !artifactId.empty();
    if (hasManifest == hasArtifactId) {
        if (errorCode != nullptr) {
            *errorCode = "invalid_arguments";
        }
        if (errorMessage != nullptr) {
            *errorMessage = "exactly one of playbook_manifest_path or playbook_artifact_id is required";
        }
        return false;
    }

    const fs::path resolvedPath = hasManifest
        ? fs::path(manifestPath)
        : phase7RootDir() / "playbooks" / artifactId / "manifest.json";
    return loadPlaybookFromPath(resolvedPath, out, errorCode, errorMessage);
}

bool listPlaybookArtifacts(std::size_t limit,
                           std::vector<PlaybookArtifact>* out,
                           std::string* errorCode,
                           std::string* errorMessage) {
    return listArtifactsUnder<PlaybookArtifact>(
        phase7RootDir() / "playbooks",
        limit,
        out,
        errorCode,
        errorMessage,
        [](const fs::path& manifestPath, PlaybookArtifact* artifact, std::string* code, std::string* message) {
            return loadPlaybookFromPath(manifestPath, artifact, code, message);
        });
}

bool buildExecutionLedger(const std::string& manifestPath,
                          const std::string& artifactId,
                          ExecutionLedgerArtifact* out,
                          bool* created,
                          std::string* errorCode,
                          std::string* errorMessage) {
    PlaybookArtifact playbook;
    if (!loadPlaybookArtifact(manifestPath, artifactId, &playbook, errorCode, errorMessage)) {
        return false;
    }

    AnalysisArtifact analysis;
    if (!loadAnalysisArtifact({}, playbook.analysisArtifact.artifactId, &analysis, errorCode, errorMessage)) {
        return false;
    }

    ExecutionLedgerArtifact ledger;
    ledger.sourceArtifact = analysis.sourceArtifact;
    ledger.analysisArtifact = analysis.analysisArtifact;
    ledger.playbookArtifact = playbook.playbookArtifact;
    ledger.ledgerArtifact = executionLedgerArtifactRefForPlaybook(playbook);
    ledger.mode = playbook.mode;
    ledger.ledgerStatus = kDefaultLedgerStatus;
    ledger.executionPolicy = executionPolicyForLedger();
    ledger.reviewPolicy = ledger.executionPolicy.value("review_policy", json::object());
    ledger.replayContext = playbook.replayContext;
    ledger.filteredFindingIds = playbook.filteredFindingIds;
    for (const auto& action : playbook.plannedActions) {
        ledger.entries.push_back(executionLedgerEntryForAction(playbook, action));
    }
    annotateExecutionLedgerReviewState(&ledger);

    const fs::path manifestFile = ledger.ledgerArtifact.manifestPath;
    if (fs::exists(manifestFile)) {
        if (created != nullptr) {
            *created = false;
        }
        return loadExecutionLedgerFromPath(manifestFile, out, errorCode, errorMessage);
    }

    ledger.generatedAtUtc = utcTimestampIso8601Now();
    ledger.auditTrail = auditTrailForLedgerCreation(playbook, ledger.generatedAtUtc, ledger.ledgerStatus);
    ledger.manifest = manifestForExecutionLedger(ledger);

    std::string directoryError;
    if (!ensureDirectoryExists(ledger.ledgerArtifact.artifactRootDir, &directoryError)) {
        if (errorCode != nullptr) {
            *errorCode = "analysis_failed";
        }
        if (errorMessage != nullptr) {
            *errorMessage = directoryError;
        }
        return false;
    }

    std::string writeError;
    if (!writeJsonTextFile(ledger.ledgerArtifact.manifestPath, ledger.manifest, &writeError)) {
        if (errorCode != nullptr) {
            *errorCode = "analysis_failed";
        }
        if (errorMessage != nullptr) {
            *errorMessage = writeError;
        }
        return false;
    }

    if (out != nullptr) {
        *out = std::move(ledger);
    }
    if (created != nullptr) {
        *created = true;
    }
    return true;
}

bool loadExecutionLedgerArtifact(const std::string& manifestPath,
                                 const std::string& artifactId,
                                 ExecutionLedgerArtifact* out,
                                 std::string* errorCode,
                                 std::string* errorMessage) {
    const bool hasManifest = !manifestPath.empty();
    const bool hasArtifactId = !artifactId.empty();
    if (hasManifest == hasArtifactId) {
        if (errorCode != nullptr) {
            *errorCode = "invalid_arguments";
        }
        if (errorMessage != nullptr) {
            *errorMessage = "exactly one of execution_ledger_manifest_path or execution_ledger_artifact_id is required";
        }
        return false;
    }

    const fs::path resolvedPath = hasManifest
        ? fs::path(manifestPath)
        : phase7RootDir() / "ledgers" / artifactId / "manifest.json";
    return loadExecutionLedgerFromPath(resolvedPath, out, errorCode, errorMessage);
}

bool listExecutionLedgers(std::size_t limit,
                          std::vector<ExecutionLedgerArtifact>* out,
                          std::string* errorCode,
                          std::string* errorMessage) {
    return listArtifactsUnder<ExecutionLedgerArtifact>(
        phase7RootDir() / "ledgers",
        limit,
        out,
        errorCode,
        errorMessage,
        [](const fs::path& manifestPath, ExecutionLedgerArtifact* artifact, std::string* code, std::string* message) {
            return loadExecutionLedgerFromPath(manifestPath, artifact, code, message);
        });
}

bool recordExecutionLedgerReview(const std::string& manifestPath,
                                 const std::string& artifactId,
                                 const std::vector<std::string>& entryIds,
                                 const std::string& reviewStatus,
                                 const std::string& actor,
                                 const std::string& comment,
                                 ExecutionLedgerArtifact* out,
                                 std::vector<std::string>* updatedEntryIds,
                                 std::string* auditEventId,
                                 std::string* errorCode,
                                 std::string* errorMessage) {
    if (entryIds.empty()) {
        if (errorCode != nullptr) {
            *errorCode = "invalid_arguments";
        }
        if (errorMessage != nullptr) {
            *errorMessage = "entry_ids must include at least one ledger entry id";
        }
        return false;
    }
    if (!isSupportedLedgerEntryStatus(reviewStatus)) {
        if (errorCode != nullptr) {
            *errorCode = "invalid_arguments";
        }
        if (errorMessage != nullptr) {
            *errorMessage = "review_status must be one of pending_review, approved, blocked, needs_info, or not_applicable";
        }
        return false;
    }

    ExecutionLedgerArtifact artifact;
    if (!loadExecutionLedgerArtifact(manifestPath, artifactId, &artifact, errorCode, errorMessage)) {
        return false;
    }

    const std::string normalizedActor = trimAscii(actor);
    if (reviewPolicyActorRequired(artifact.reviewPolicy) && normalizedActor.empty()) {
        if (errorCode != nullptr) {
            *errorCode = "invalid_arguments";
        }
        if (errorMessage != nullptr) {
            *errorMessage = "actor is required by the Phase 7 ledger review policy";
        }
        return false;
    }
    const std::string normalizedComment = trimAscii(comment);
    if (reviewPolicyCommentRequired(artifact.reviewPolicy, reviewStatus) && normalizedComment.empty()) {
        if (errorCode != nullptr) {
            *errorCode = "invalid_arguments";
        }
        if (errorMessage != nullptr) {
            *errorMessage = "comment is required when review_status is blocked or needs_info";
        }
        return false;
    }

    std::vector<std::string> uniqueEntryIds = entryIds;
    std::sort(uniqueEntryIds.begin(), uniqueEntryIds.end());
    uniqueEntryIds.erase(std::unique(uniqueEntryIds.begin(), uniqueEntryIds.end()), uniqueEntryIds.end());

    std::vector<ExecutionLedgerEntry> previousEntries;
    previousEntries.reserve(uniqueEntryIds.size());
    for (const auto& entryId : uniqueEntryIds) {
        const auto it = std::find_if(
            artifact.entries.begin(),
            artifact.entries.end(),
            [&](const ExecutionLedgerEntry& entry) { return entry.entryId == entryId; });
        if (it == artifact.entries.end()) {
            if (errorCode != nullptr) {
                *errorCode = "entry_not_found";
            }
            if (errorMessage != nullptr) {
                *errorMessage = "execution ledger review references an entry that does not exist";
            }
            return false;
        }
        previousEntries.push_back(*it);
    }

    const std::string generatedAtUtc = utcTimestampIso8601Now();
    const std::string previousLedgerStatus = artifact.ledgerStatus;
    for (auto& entry : artifact.entries) {
        if (std::find(uniqueEntryIds.begin(), uniqueEntryIds.end(), entry.entryId) == uniqueEntryIds.end()) {
            continue;
        }
        entry.reviewStatus = reviewStatus;
        entry.reviewedAtUtc = generatedAtUtc;
        entry.reviewedBy = normalizedActor;
        entry.reviewComment = normalizedComment;
    }
    json auditEvent = auditEventForLedgerReview(artifact,
                                                previousEntries,
                                                uniqueEntryIds,
                                                reviewStatus,
                                                normalizedActor,
                                                normalizedComment,
                                                previousLedgerStatus,
                                                generatedAtUtc);
    artifact.auditTrail.push_back(auditEvent);
    annotateExecutionLedgerReviewState(&artifact);
    artifact.auditTrail.back()["ledger_status"] = artifact.ledgerStatus;
    artifact.manifest = manifestForExecutionLedger(artifact);

    std::string writeError;
    if (!writeJsonTextFile(artifact.ledgerArtifact.manifestPath, artifact.manifest, &writeError)) {
        if (errorCode != nullptr) {
            *errorCode = "analysis_failed";
        }
        if (errorMessage != nullptr) {
            *errorMessage = writeError;
        }
        return false;
    }

    if (updatedEntryIds != nullptr) {
        *updatedEntryIds = uniqueEntryIds;
    }
    if (auditEventId != nullptr) {
        *auditEventId = auditEvent.value("event_id", std::string());
    }
    if (out != nullptr) {
        *out = std::move(artifact);
    }
    if (errorCode != nullptr) {
        errorCode->clear();
    }
    if (errorMessage != nullptr) {
        errorMessage->clear();
    }
    return true;
}

std::string analysisArtifactMarkdown(const AnalysisArtifact& artifact) {
    std::ostringstream out;
    out << "# Phase 7 Analysis\n\n"
        << "- Artifact: `" << artifact.analysisArtifact.artifactId << "`\n"
        << "- Profile: `" << artifact.analysisProfile << "`\n"
        << "- Source artifact: `" << artifact.sourceArtifact.artifactId << "`\n"
        << "- Findings: " << artifact.findings.size() << "\n";
    if (artifact.replayContext.contains("requested_window") &&
        artifact.replayContext.at("requested_window").is_object()) {
        const json window = artifact.replayContext.at("requested_window");
        out << "- Session window: `" << window.value("first_session_seq", 0ULL)
            << "-" << window.value("last_session_seq", 0ULL) << "`\n";
    }
    out << "\n## Findings\n";
    for (const auto& finding : artifact.findings) {
        out << "- [" << finding.severity << "] `" << finding.category << "`: "
            << finding.summary << "\n";
    }
    return out.str();
}

std::string playbookArtifactMarkdown(const PlaybookArtifact& artifact) {
    std::ostringstream out;
    out << "# Phase 7 Playbook\n\n"
        << "- Artifact: `" << artifact.playbookArtifact.artifactId << "`\n"
        << "- Analysis artifact: `" << artifact.analysisArtifact.artifactId << "`\n"
        << "- Mode: `" << artifact.mode << "`\n"
        << "- Filtered findings: " << artifact.filteredFindingIds.size() << "\n"
        << "- Planned actions: " << artifact.plannedActions.size() << "\n";
    out << "\n## Planned Actions\n";
    for (const auto& action : artifact.plannedActions) {
        out << "- `" << action.actionType << "` for `" << action.findingId << "`: "
            << action.title << "\n";
    }
    return out.str();
}

std::string executionLedgerArtifactMarkdown(const ExecutionLedgerArtifact& artifact) {
    const ExecutionLedgerReviewSummary reviewSummary = summarizeExecutionLedgerReviewSummary(artifact);
    const json latestAudit = latestExecutionLedgerAuditSummary(artifact);
    std::ostringstream out;
    out << "# Phase 7 Execution Ledger\n\n"
        << "- Artifact: `" << artifact.ledgerArtifact.artifactId << "`\n"
        << "- Playbook artifact: `" << artifact.playbookArtifact.artifactId << "`\n"
        << "- Analysis artifact: `" << artifact.analysisArtifact.artifactId << "`\n"
        << "- Source artifact: `" << artifact.sourceArtifact.artifactId << "`\n"
        << "- Mode: `" << artifact.mode << "`\n"
        << "- Ledger status: `" << artifact.ledgerStatus << "`\n"
        << "- Entries: " << artifact.entries.size() << "\n"
        << "- Review summary: pending=" << reviewSummary.pendingReviewCount
        << ", approved=" << reviewSummary.approvedCount
        << ", blocked=" << reviewSummary.blockedCount
        << ", needs_info=" << reviewSummary.needsInfoCount
        << ", not_applicable=" << reviewSummary.notApplicableCount
        << ", waiting_approval=" << reviewSummary.waitingApprovalCount
        << ", ready=" << reviewSummary.readyEntryCount << "\n"
        << "- Distinct reviewers: " << reviewSummary.distinctReviewerCount
        << " (required approvals per actionable entry: " << reviewSummary.requiredApprovalCount << ")\n"
        << "- Ready for execution: `" << (reviewSummary.readyForExecution ? "true" : "false") << "`\n";
    if (artifact.executionPolicy.is_object()) {
        out << "- Apply supported: `" << (artifact.executionPolicy.value("apply_supported", false) ? "true" : "false")
            << "`\n"
            << "- Deferred reason: `" << artifact.executionPolicy.value("deferred_reason", std::string()) << "`\n";
    }
    if (latestAudit.is_object() && latestAudit.contains("message") && latestAudit.at("message").is_string()) {
        out << "- Latest audit event: `" << latestAudit.value("event_type", std::string()) << "`";
        if (latestAudit.contains("generated_at_utc") && latestAudit.at("generated_at_utc").is_string()) {
            out << " at `" << latestAudit.at("generated_at_utc").get<std::string>() << "`";
        }
        if (latestAudit.contains("actor") && latestAudit.at("actor").is_string()) {
            out << " by `" << latestAudit.at("actor").get<std::string>() << "`";
        }
        out << "\n  " << latestAudit.at("message").get<std::string>() << "\n";
    }
    out << "\n## Review Entries\n";
    for (const auto& entry : artifact.entries) {
        out << "- [" << entry.reviewStatus << "] `" << entry.actionType << "` for `"
            << entry.findingId << "`: " << entry.title;
        if (entry.reviewStatus == kLedgerEntryStatusApproved) {
            out << " (approvals=" << entry.approvalReviewerCount
                << "/" << reviewSummary.requiredApprovalCount
                << ", ready=`" << (entry.approvalThresholdMet ? "true" : "false") << "`)";
        }
        out << "\n";
        if (!entry.reviewedBy.empty()) {
            out << "  reviewed_by: `" << entry.reviewedBy << "`";
            if (!entry.reviewedAtUtc.empty()) {
                out << " at `" << entry.reviewedAtUtc << "`";
            }
            out << "\n";
        }
        if (!entry.reviewComment.empty()) {
            out << "  comment: " << entry.reviewComment << "\n";
        }
    }
    if (artifact.auditTrail.is_array() && !artifact.auditTrail.empty()) {
        out << "\n## Audit Trail\n";
        for (const auto& event : artifact.auditTrail) {
            out << "- `" << event.value("event_type", std::string()) << "`";
            if (event.contains("generated_at_utc") && event.at("generated_at_utc").is_string()) {
                out << " at `" << event.at("generated_at_utc").get<std::string>() << "`";
            }
            if (event.contains("actor") && event.at("actor").is_string()) {
                out << " by `" << event.at("actor").get<std::string>() << "`";
            }
            out << ": " << event.value("message", std::string()) << "\n";
            if (event.contains("comment") && event.at("comment").is_string()) {
                out << "  comment: " << event.at("comment").get<std::string>() << "\n";
            }
        }
    }
    return out.str();
}

} // namespace tape_phase7
