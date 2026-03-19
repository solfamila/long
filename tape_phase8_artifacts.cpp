#include "tape_phase8_artifacts.h"

#include "tape_bundle_inspection.h"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <optional>
#include <set>
#include <sstream>
#include <string_view>
#include <system_error>

namespace tape_phase8 {

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
        } else if (!normalized.empty() && !previousDash) {
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

std::string utcTimestampIso8601For(const std::chrono::system_clock::time_point timePoint) {
    const auto msSinceEpoch = std::chrono::duration_cast<std::chrono::milliseconds>(timePoint.time_since_epoch());
    const auto msPart = static_cast<long long>(msSinceEpoch.count() % 1000);
    const std::time_t time = std::chrono::system_clock::to_time_t(timePoint);

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

std::optional<std::chrono::system_clock::time_point> parseUtcTimestampIso8601(const std::string& value) {
    if (value.size() < 20 || value.back() != 'Z') {
        return std::nullopt;
    }

    std::tm utc{};
    std::istringstream input(value.substr(0, 19));
    input >> std::get_time(&utc, "%Y-%m-%dT%H:%M:%S");
    if (!input) {
        return std::nullopt;
    }

    long long millisecondPart = 0;
    if (value.size() >= 24 && value[19] == '.') {
        const std::string millis = value.substr(20, 3);
        if (millis.size() == 3 && std::all_of(millis.begin(), millis.end(), [](unsigned char ch) {
                return std::isdigit(ch) != 0;
            })) {
            millisecondPart = std::stoll(millis);
        }
    }

#if defined(_WIN32)
    const std::time_t epochSeconds = _mkgmtime(&utc);
#else
    const std::time_t epochSeconds = timegm(&utc);
#endif
    if (epochSeconds < 0) {
        return std::nullopt;
    }
    return std::chrono::system_clock::from_time_t(epochSeconds) + std::chrono::milliseconds(millisecondPart);
}

std::string scheduleNextEvaluationAtUtc(const std::string& generatedAtUtc,
                                        const std::size_t evaluationCadenceMinutes) {
    if (evaluationCadenceMinutes == 0 || generatedAtUtc.empty()) {
        return {};
    }
    const auto parsed = parseUtcTimestampIso8601(generatedAtUtc);
    if (!parsed.has_value()) {
        return {};
    }
    return utcTimestampIso8601For(*parsed + std::chrono::minutes(evaluationCadenceMinutes));
}

bool watchIsDueAt(const WatchDefinitionArtifact& artifact,
                  const std::chrono::system_clock::time_point now) {
    if (!artifact.enabled || artifact.evaluationCadenceMinutes == 0) {
        return false;
    }
    if (artifact.nextEvaluationAtUtc.empty()) {
        return true;
    }
    const auto parsed = parseUtcTimestampIso8601(artifact.nextEvaluationAtUtc);
    if (!parsed.has_value()) {
        return true;
    }
    return *parsed <= now;
}

bool watchIsDueNow(const WatchDefinitionArtifact& artifact) {
    return watchIsDueAt(artifact, std::chrono::system_clock::now());
}

bool isClosedAttentionStatus(std::string_view status) {
    return status == kAttentionStatusAcknowledged ||
        status == kAttentionStatusResolved ||
        status == kAttentionStatusSuppressed;
}

bool attentionShouldBeOpenAt(const TriggerRunArtifact& artifact,
                             const std::chrono::system_clock::time_point now) {
    if (artifact.triggerOutcome != kTriggerOutcomeTriggered) {
        return false;
    }
    if (artifact.attentionStatus == kAttentionStatusNew) {
        return true;
    }
    if (artifact.attentionStatus == kAttentionStatusSnoozed) {
        if (artifact.snoozedUntilUtc.empty()) {
            return false;
        }
        const auto parsed = parseUtcTimestampIso8601(artifact.snoozedUntilUtc);
        return parsed.has_value() && *parsed <= now;
    }
    if (isClosedAttentionStatus(artifact.attentionStatus)) {
        return false;
    }
    return artifact.attentionOpen;
}

void refreshEffectiveAttentionState(TriggerRunArtifact* artifact) {
    if (artifact == nullptr) {
        return;
    }
    artifact->attentionOpen = attentionShouldBeOpenAt(*artifact, std::chrono::system_clock::now());
}

bool isSupportedAttentionAction(std::string_view action) {
    return action == "acknowledge" || action == "snooze" || action == "resolve";
}

std::string attentionStatusForAction(std::string_view action) {
    if (action == "acknowledge") {
        return kAttentionStatusAcknowledged;
    }
    if (action == "snooze") {
        return kAttentionStatusSnoozed;
    }
    if (action == "resolve") {
        return kAttentionStatusResolved;
    }
    return {};
}

bool triggerMatchesSelection(const TriggerRunArtifact& artifact,
                             const TriggerRunInventorySelection& selection) {
    if (!selection.watchArtifactId.empty() &&
        artifact.watchArtifact.artifactId != selection.watchArtifactId) {
        return false;
    }
    if (!selection.attentionStatus.empty() &&
        artifact.attentionStatus != selection.attentionStatus) {
        return false;
    }
    if (selection.attentionOpen.has_value() &&
        artifact.attentionOpen != *selection.attentionOpen) {
        return false;
    }
    return true;
}

std::string timestampIdSuffixNow() {
    const auto now = std::chrono::system_clock::now();
    const auto micros = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
    return std::to_string(micros);
}

fs::path phase8RootDir() {
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
    return basePath / "phase8_artifacts";
}

bool ensureDirectoryExists(const fs::path& path, std::string* error) {
    std::error_code ec;
    fs::create_directories(path, ec);
    if (ec) {
        if (error != nullptr) {
            *error = "failed to create directory `" + path.string() + "`: " + ec.message();
        }
        return false;
    }
    return true;
}

bool readJsonFile(const fs::path& path, json* out, std::string* error) {
    std::ifstream input(path);
    if (!input.is_open()) {
        if (error != nullptr) {
            *error = "failed to open `" + path.string() + "`";
        }
        return false;
    }
    json payload = json::parse(input, nullptr, false);
    if (payload.is_discarded()) {
        if (error != nullptr) {
            *error = "failed to parse json from `" + path.string() + "`";
        }
        return false;
    }
    if (out != nullptr) {
        *out = std::move(payload);
    }
    return true;
}

bool writeJsonTextFile(const fs::path& path, const json& payload, std::string* error) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    if (!output.is_open()) {
        if (error != nullptr) {
            *error = "failed to open `" + path.string() + "` for write";
        }
        return false;
    }
    output << payload.dump(2) << '\n';
    if (!output.good()) {
        if (error != nullptr) {
            *error = "failed to write `" + path.string() + "`";
        }
        return false;
    }
    return true;
}

std::string stringValueOrEmpty(const json& payload, const char* key) {
    if (!payload.is_object()) {
        return {};
    }
    const auto it = payload.find(key);
    if (it == payload.end() || !it->is_string()) {
        return {};
    }
    return it->get<std::string>();
}

std::string stringValueOrDefault(const json& payload,
                                 const char* key,
                                 std::string fallback = {}) {
    const std::string value = stringValueOrEmpty(payload, key);
    return value.empty() ? fallback : value;
}

bool isSupportedSeverity(std::string_view severity) {
    return severity.empty() ||
        severity == "critical" ||
        severity == "high" ||
        severity == "medium" ||
        severity == "low" ||
        severity == "info";
}

json phase8ArtifactRefToJson(const ArtifactRef& artifact) {
    return {
        {"artifact_type", artifact.artifactType},
        {"contract_version", artifact.contractVersion},
        {"artifact_id", artifact.artifactId},
        {"manifest_path", artifact.manifestPath},
        {"artifact_root_dir", artifact.artifactRootDir}
    };
}

bool parseArtifactRef(const json& payload, ArtifactRef* out) {
    if (!payload.is_object() || out == nullptr) {
        return false;
    }
    out->artifactType = stringValueOrEmpty(payload, "artifact_type");
    out->contractVersion = stringValueOrEmpty(payload, "contract_version");
    out->artifactId = stringValueOrEmpty(payload, "artifact_id");
    out->manifestPath = stringValueOrEmpty(payload, "manifest_path");
    out->artifactRootDir = stringValueOrEmpty(payload, "artifact_root_dir");
    return !out->artifactType.empty() && !out->artifactId.empty() && !out->manifestPath.empty();
}

ArtifactRef watchArtifactRefFor(const std::string& bundlePath, const std::string& analysisProfile) {
    const fs::path bundle(bundlePath);
    const std::string artifactId =
        "watch-" + normalizeToken(bundle.stem().string()) + "-" + fnv1aHex(bundlePath + "|" + analysisProfile);
    const fs::path rootDir = phase8RootDir() / "watch-definitions" / artifactId;
    return ArtifactRef{
        .artifactType = kWatchDefinitionArtifactType,
        .contractVersion = kContractVersion,
        .artifactId = artifactId,
        .manifestPath = (rootDir / "manifest.json").string(),
        .artifactRootDir = rootDir.string()
    };
}

ArtifactRef triggerArtifactRefFor(const ArtifactRef& watchArtifact,
                                  const ArtifactRef& analysisArtifact,
                                  const std::string& generatedAtUtc) {
    const std::string artifactId =
        "trigger-" + normalizeToken(watchArtifact.artifactId) + "-" + timestampIdSuffixNow() + "-" +
        fnv1aHex(watchArtifact.artifactId + "|" + analysisArtifact.artifactId + "|" + generatedAtUtc);
    const fs::path rootDir = phase8RootDir() / "trigger-runs" / artifactId;
    return ArtifactRef{
        .artifactType = kTriggerRunArtifactType,
        .contractVersion = kContractVersion,
        .artifactId = artifactId,
        .manifestPath = (rootDir / "manifest.json").string(),
        .artifactRootDir = rootDir.string()
    };
}

std::string defaultWatchTitle(const std::string& bundlePath, const std::string& analysisProfile) {
    tape_phase7::AnalyzerProfileSpec profile;
    std::string ignoredCode;
    std::string ignoredMessage;
    const std::string profileTitle =
        tape_phase7::loadAnalyzerProfile(analysisProfile, &profile, &ignoredCode, &ignoredMessage)
            ? profile.title
            : analysisProfile;
    return fs::path(bundlePath).stem().string() + " -> " + profileTitle;
}

json bundleSummaryForPath(const std::string& bundlePath,
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
        return json();
    }
    if (errorCode != nullptr) {
        errorCode->clear();
    }
    if (errorMessage != nullptr) {
        errorMessage->clear();
    }
    return {
        {"bundle_path", inspection.bundlePath.string()},
        {"bundle_type", inspection.bundleType},
        {"payload_sha256", inspection.payloadSha256},
        {"headline", inspection.headline},
        {"source_artifact_id", inspection.sourceArtifactId},
        {"source_report_id", inspection.sourceReportId},
        {"source_revision_id", inspection.sourceRevisionId},
        {"first_session_seq", inspection.firstSessionSeq},
        {"last_session_seq", inspection.lastSessionSeq}
    };
}

json manifestForWatchDefinition(const WatchDefinitionArtifact& artifact) {
    return {
        {"artifact_type", kWatchDefinitionArtifactType},
        {"contract_version", kContractVersion},
        {"watch_definition", phase8ArtifactRefToJson(artifact.watchArtifact)},
        {"title", artifact.title},
        {"bundle_path", artifact.bundlePath},
        {"analysis_profile", artifact.analysisProfile},
        {"enabled", artifact.enabled},
        {"evaluation_cadence_minutes", artifact.evaluationCadenceMinutes},
        {"minimum_finding_count", artifact.minimumFindingCount},
        {"minimum_severity", artifact.minimumSeverity.empty() ? json(nullptr) : json(artifact.minimumSeverity)},
        {"required_category", artifact.requiredCategory.empty() ? json(nullptr) : json(artifact.requiredCategory)},
        {"created_at_utc", artifact.createdAtUtc},
        {"updated_at_utc", artifact.updatedAtUtc},
        {"latest_evaluation_at_utc",
         artifact.latestEvaluationAtUtc.empty() ? json(nullptr) : json(artifact.latestEvaluationAtUtc)},
        {"next_evaluation_at_utc",
         artifact.nextEvaluationAtUtc.empty() ? json(nullptr) : json(artifact.nextEvaluationAtUtc)},
        {"latest_trigger_outcome",
         artifact.latestTriggerOutcome.empty() ? json(nullptr) : json(artifact.latestTriggerOutcome)},
        {"latest_trigger_artifact_id",
         artifact.latestTriggerArtifactId.empty() ? json(nullptr) : json(artifact.latestTriggerArtifactId)},
        {"bundle_summary", artifact.bundleSummary}
    };
}

bool loadWatchDefinitionFromManifestJson(const json& manifest,
                                         WatchDefinitionArtifact* out,
                                         std::string* errorCode,
                                         std::string* errorMessage) {
    if (!manifest.is_object()) {
        if (errorCode != nullptr) {
            *errorCode = "artifact_load_failed";
        }
        if (errorMessage != nullptr) {
            *errorMessage = "watch manifest must be a json object";
        }
        return false;
    }
    if (stringValueOrEmpty(manifest, "artifact_type") != kWatchDefinitionArtifactType ||
        stringValueOrEmpty(manifest, "contract_version") != kContractVersion) {
        if (errorCode != nullptr) {
            *errorCode = "unsupported_contract";
        }
        if (errorMessage != nullptr) {
            *errorMessage = "watch manifest contract/version is not supported";
        }
        return false;
    }

    WatchDefinitionArtifact artifact;
    if (!parseArtifactRef(manifest.value("watch_definition", manifest), &artifact.watchArtifact)) {
        if (errorCode != nullptr) {
            *errorCode = "artifact_load_failed";
        }
        if (errorMessage != nullptr) {
            *errorMessage = "watch manifest is missing artifact identity";
        }
        return false;
    }
    artifact.title = stringValueOrEmpty(manifest, "title");
    artifact.bundlePath = stringValueOrEmpty(manifest, "bundle_path");
    artifact.analysisProfile = stringValueOrDefault(manifest, "analysis_profile",
                                                    std::string(tape_phase7::kDefaultAnalyzerProfile));
    artifact.enabled = manifest.value("enabled", true);
    artifact.evaluationCadenceMinutes =
        manifest.value("evaluation_cadence_minutes", static_cast<std::uint64_t>(kDefaultEvaluationCadenceMinutes));
    artifact.minimumFindingCount = manifest.value("minimum_finding_count", 1ULL);
    artifact.minimumSeverity = stringValueOrEmpty(manifest, "minimum_severity");
    artifact.requiredCategory = stringValueOrEmpty(manifest, "required_category");
    artifact.createdAtUtc = stringValueOrEmpty(manifest, "created_at_utc");
    artifact.updatedAtUtc = stringValueOrDefault(manifest, "updated_at_utc", artifact.createdAtUtc);
    artifact.latestEvaluationAtUtc = stringValueOrEmpty(manifest, "latest_evaluation_at_utc");
    artifact.nextEvaluationAtUtc = stringValueOrEmpty(manifest, "next_evaluation_at_utc");
    artifact.latestTriggerOutcome = stringValueOrEmpty(manifest, "latest_trigger_outcome");
    artifact.latestTriggerArtifactId = stringValueOrEmpty(manifest, "latest_trigger_artifact_id");
    artifact.bundleSummary = manifest.value("bundle_summary", json::object());
    artifact.manifest = manifest;
    if (artifact.bundlePath.empty()) {
        if (errorCode != nullptr) {
            *errorCode = "artifact_load_failed";
        }
        if (errorMessage != nullptr) {
            *errorMessage = "watch manifest is missing bundle_path";
        }
        return false;
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

json manifestForTriggerRun(const TriggerRunArtifact& artifact) {
    return {
        {"artifact_type", kTriggerRunArtifactType},
        {"contract_version", kContractVersion},
        {"watch_definition", phase8ArtifactRefToJson(artifact.watchArtifact)},
        {"source_artifact", phase8ArtifactRefToJson(artifact.sourceArtifact)},
        {"analysis_artifact", phase8ArtifactRefToJson(artifact.analysisArtifact)},
        {"trigger_run", phase8ArtifactRefToJson(artifact.triggerArtifact)},
        {"title", artifact.title},
        {"analysis_profile", artifact.analysisProfile},
        {"trigger_reason", artifact.triggerReason},
        {"trigger_outcome", artifact.triggerOutcome},
        {"attention_status", artifact.attentionStatus},
        {"attention_open", artifact.attentionOpen},
        {"attention_updated_at_utc",
         artifact.attentionUpdatedAtUtc.empty() ? json(nullptr) : json(artifact.attentionUpdatedAtUtc)},
        {"attention_actor", artifact.attentionActor.empty() ? json(nullptr) : json(artifact.attentionActor)},
        {"attention_comment", artifact.attentionComment.empty() ? json(nullptr) : json(artifact.attentionComment)},
        {"snoozed_until_utc", artifact.snoozedUntilUtc.empty() ? json(nullptr) : json(artifact.snoozedUntilUtc)},
        {"analysis_created", artifact.analysisCreated},
        {"finding_count", artifact.findingCount},
        {"highest_severity", artifact.highestSeverity.empty() ? json(nullptr) : json(artifact.highestSeverity)},
        {"generated_at_utc", artifact.generatedAtUtc},
        {"headline", artifact.headline},
        {"finding_categories", artifact.findingCategories},
        {"suppression_reasons", artifact.suppressionReasons}
    };
}

bool loadTriggerRunFromManifestJson(const json& manifest,
                                    TriggerRunArtifact* out,
                                    std::string* errorCode,
                                    std::string* errorMessage) {
    if (!manifest.is_object()) {
        if (errorCode != nullptr) {
            *errorCode = "artifact_load_failed";
        }
        if (errorMessage != nullptr) {
            *errorMessage = "trigger manifest must be a json object";
        }
        return false;
    }
    if (stringValueOrEmpty(manifest, "artifact_type") != kTriggerRunArtifactType ||
        stringValueOrEmpty(manifest, "contract_version") != kContractVersion) {
        if (errorCode != nullptr) {
            *errorCode = "unsupported_contract";
        }
        if (errorMessage != nullptr) {
            *errorMessage = "trigger manifest contract/version is not supported";
        }
        return false;
    }

    TriggerRunArtifact artifact;
    if (!parseArtifactRef(manifest.value("watch_definition", json::object()), &artifact.watchArtifact) ||
        !parseArtifactRef(manifest.value("source_artifact", json::object()), &artifact.sourceArtifact) ||
        !parseArtifactRef(manifest.value("analysis_artifact", json::object()), &artifact.analysisArtifact) ||
        !parseArtifactRef(manifest.value("trigger_run", manifest), &artifact.triggerArtifact)) {
        if (errorCode != nullptr) {
            *errorCode = "artifact_load_failed";
        }
        if (errorMessage != nullptr) {
            *errorMessage = "trigger manifest is missing artifact identity";
        }
        return false;
    }
    artifact.title = stringValueOrEmpty(manifest, "title");
    artifact.analysisProfile = stringValueOrDefault(manifest, "analysis_profile",
                                                    std::string(tape_phase7::kDefaultAnalyzerProfile));
    artifact.triggerReason = stringValueOrDefault(manifest, "trigger_reason",
                                                  std::string(kDefaultTriggerReason));
    artifact.triggerOutcome = stringValueOrDefault(manifest, "trigger_outcome",
                                                   std::string(kTriggerOutcomeTriggered));
    artifact.attentionStatus = stringValueOrDefault(manifest, "attention_status",
                                                    std::string(kAttentionStatusNew));
    artifact.attentionOpen = manifest.value("attention_open", true);
    artifact.attentionUpdatedAtUtc = stringValueOrEmpty(manifest, "attention_updated_at_utc");
    artifact.attentionActor = stringValueOrEmpty(manifest, "attention_actor");
    artifact.attentionComment = stringValueOrEmpty(manifest, "attention_comment");
    artifact.snoozedUntilUtc = stringValueOrEmpty(manifest, "snoozed_until_utc");
    artifact.analysisCreated = manifest.value("analysis_created", false);
    artifact.findingCount = manifest.value("finding_count", 0ULL);
    artifact.highestSeverity = stringValueOrEmpty(manifest, "highest_severity");
    artifact.generatedAtUtc = stringValueOrEmpty(manifest, "generated_at_utc");
    artifact.headline = stringValueOrEmpty(manifest, "headline");
    artifact.findingCategories = manifest.value("finding_categories", json::array());
    artifact.suppressionReasons = manifest.value("suppression_reasons", json::array());
    refreshEffectiveAttentionState(&artifact);
    artifact.manifest = manifest;
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

bool loadWatchDefinitionFromPath(const fs::path& manifestPath,
                                 WatchDefinitionArtifact* out,
                                 std::string* errorCode,
                                 std::string* errorMessage) {
    if (!fs::exists(manifestPath)) {
        if (errorCode != nullptr) {
            *errorCode = "artifact_not_found";
        }
        if (errorMessage != nullptr) {
            *errorMessage = "watch manifest was not found";
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
    return loadWatchDefinitionFromManifestJson(manifest, out, errorCode, errorMessage);
}

bool loadTriggerRunFromPath(const fs::path& manifestPath,
                            TriggerRunArtifact* out,
                            std::string* errorCode,
                            std::string* errorMessage) {
    if (!fs::exists(manifestPath)) {
        if (errorCode != nullptr) {
            *errorCode = "artifact_not_found";
        }
        if (errorMessage != nullptr) {
            *errorMessage = "trigger manifest was not found";
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
    return loadTriggerRunFromManifestJson(manifest, out, errorCode, errorMessage);
}

template <typename Artifact>
bool listArtifactsUnder(const fs::path& root,
                        std::size_t limit,
                        bool recursive,
                        std::vector<Artifact>* out,
                        std::string* errorCode,
                        std::string* errorMessage,
                        const std::function<bool(const fs::path&, Artifact*, std::string*, std::string*)>& loader) {
    if (out == nullptr) {
        if (errorCode != nullptr) {
            *errorCode = "invalid_arguments";
        }
        if (errorMessage != nullptr) {
            *errorMessage = "output vector is required";
        }
        return false;
    }
    out->clear();
    if (!fs::exists(root)) {
        if (errorCode != nullptr) {
            errorCode->clear();
        }
        if (errorMessage != nullptr) {
            errorMessage->clear();
        }
        return true;
    }

    std::vector<fs::path> manifestPaths;
    if (recursive) {
        for (const auto& entry : fs::recursive_directory_iterator(root)) {
            if (entry.is_regular_file() && entry.path().filename() == "manifest.json") {
                manifestPaths.push_back(entry.path());
            }
        }
    } else {
        for (const auto& entry : fs::directory_iterator(root)) {
            const fs::path manifestPath = entry.path() / "manifest.json";
            if (fs::exists(manifestPath)) {
                manifestPaths.push_back(manifestPath);
            }
        }
    }
    std::sort(manifestPaths.begin(), manifestPaths.end(), std::greater<fs::path>());
    const std::size_t cappedLimit = limit == 0 ? manifestPaths.size() : std::min(limit, manifestPaths.size());
    for (const auto& manifestPath : manifestPaths) {
        if (out->size() >= cappedLimit) {
            break;
        }
        Artifact artifact;
        if (!loader(manifestPath, &artifact, errorCode, errorMessage)) {
            return false;
        }
        out->push_back(std::move(artifact));
    }
    if (errorCode != nullptr) {
        errorCode->clear();
    }
    if (errorMessage != nullptr) {
        errorMessage->clear();
    }
    return true;
}

int severityRank(std::string_view severity) {
    if (severity == "critical") {
        return 5;
    }
    if (severity == "high") {
        return 4;
    }
    if (severity == "medium") {
        return 3;
    }
    if (severity == "low") {
        return 2;
    }
    if (severity == "info") {
        return 1;
    }
    return 0;
}

std::string highestSeverityFor(const tape_phase7::AnalysisArtifact& analysis) {
    std::string best;
    int bestRank = -1;
    for (const auto& finding : analysis.findings) {
        const int rank = severityRank(finding.severity);
        if (rank > bestRank) {
            bestRank = rank;
            best = finding.severity;
        }
    }
    return best;
}

bool analysisHasCategory(const tape_phase7::AnalysisArtifact& analysis, std::string_view category) {
    if (category.empty()) {
        return true;
    }
    return std::any_of(analysis.findings.begin(),
                       analysis.findings.end(),
                       [&](const tape_phase7::FindingRecord& finding) {
                           return finding.category == category;
                       });
}

json findingCategoriesFor(const tape_phase7::AnalysisArtifact& analysis) {
    std::set<std::string> categories;
    for (const auto& finding : analysis.findings) {
        if (!finding.category.empty()) {
            categories.insert(finding.category);
        }
    }
    json result = json::array();
    for (const auto& category : categories) {
        result.push_back(category);
    }
    return result;
}

std::string triggerHeadlineFor(const WatchDefinitionArtifact& watch,
                               const tape_phase7::AnalysisArtifact& analysis) {
    std::ostringstream out;
    out << watch.title << ": " << analysis.findings.size() << " finding";
    if (analysis.findings.size() != 1U) {
        out << 's';
    }
    const std::string highestSeverity = highestSeverityFor(analysis);
    if (!highestSeverity.empty()) {
        out << " (" << highestSeverity << " highest severity)";
    }
    return out.str();
}

std::string suppressedTriggerHeadlineFor(const WatchDefinitionArtifact& watch,
                                         const json& suppressionReasons) {
    std::ostringstream out;
    out << watch.title << ": suppressed";
    if (suppressionReasons.is_array() && !suppressionReasons.empty()) {
        const json& firstReason = suppressionReasons.front();
        const std::string message = stringValueOrEmpty(firstReason, "message");
        if (!message.empty()) {
            out << " (" << message << ")";
        }
    }
    return out.str();
}

json triggerSuppressionReasonsFor(const WatchDefinitionArtifact& watch,
                                  const tape_phase7::AnalysisArtifact& analysis) {
    json reasons = json::array();
    if (analysis.findings.size() < watch.minimumFindingCount) {
        reasons.push_back({
            {"code", "minimum_finding_count_not_met"},
            {"message", "finding_count below configured minimum"},
            {"actual_finding_count", analysis.findings.size()},
            {"required_finding_count", watch.minimumFindingCount}
        });
    }

    if (!watch.minimumSeverity.empty()) {
        const std::string highestSeverity = highestSeverityFor(analysis);
        if (severityRank(highestSeverity) < severityRank(watch.minimumSeverity)) {
            reasons.push_back({
                {"code", "minimum_severity_not_met"},
                {"message", "highest_severity below configured minimum"},
                {"actual_highest_severity", highestSeverity.empty() ? json(nullptr) : json(highestSeverity)},
                {"required_minimum_severity", watch.minimumSeverity}
            });
        }
    }

    if (!watch.requiredCategory.empty() && !analysisHasCategory(analysis, watch.requiredCategory)) {
        reasons.push_back({
            {"code", "required_category_missing"},
            {"message", "required finding category was not present"},
            {"required_category", watch.requiredCategory}
        });
    }
    return reasons;
}

AttentionInboxItem attentionItemFromTrigger(const TriggerRunArtifact& trigger) {
    return AttentionInboxItem{
        .triggerArtifactId = trigger.triggerArtifact.artifactId,
        .watchArtifactId = trigger.watchArtifact.artifactId,
        .analysisArtifactId = trigger.analysisArtifact.artifactId,
        .sourceArtifactId = trigger.sourceArtifact.artifactId,
        .title = trigger.title,
        .headline = trigger.headline,
        .attentionStatus = trigger.attentionStatus,
        .attentionOpen = trigger.attentionOpen,
        .triggerOutcome = trigger.triggerOutcome,
        .highestSeverity = trigger.highestSeverity,
        .analysisProfile = trigger.analysisProfile,
        .findingCount = trigger.findingCount,
        .generatedAtUtc = trigger.generatedAtUtc,
        .attentionUpdatedAtUtc = trigger.attentionUpdatedAtUtc,
        .snoozedUntilUtc = trigger.snoozedUntilUtc
    };
}

} // namespace

bool createWatchDefinition(const std::string& bundlePath,
                           const std::string& analysisProfile,
                           const std::string& title,
                           bool enabled,
                           const std::size_t evaluationCadenceMinutes,
                           std::size_t minimumFindingCount,
                           const std::string& minimumSeverity,
                           const std::string& requiredCategory,
                           WatchDefinitionArtifact* out,
                           bool* created,
                           std::string* errorCode,
                           std::string* errorMessage) {
    tape_phase7::AnalyzerProfileSpec profile;
    if (!tape_phase7::loadAnalyzerProfile(analysisProfile, &profile, errorCode, errorMessage)) {
        return false;
    }

    std::string bundleCode;
    std::string bundleMessage;
    const json bundleSummary = bundleSummaryForPath(bundlePath, &bundleCode, &bundleMessage);
    if (bundleSummary.is_null() || !bundleSummary.is_object()) {
        if (errorCode != nullptr) {
            *errorCode = bundleCode;
        }
        if (errorMessage != nullptr) {
            *errorMessage = bundleMessage;
        }
        return false;
    }

    if (!isSupportedSeverity(minimumSeverity)) {
        if (errorCode != nullptr) {
            *errorCode = "invalid_arguments";
        }
        if (errorMessage != nullptr) {
            *errorMessage = "minimum_severity must be one of critical, high, medium, low, info, or empty";
        }
        return false;
    }

    WatchDefinitionArtifact artifact;
    artifact.watchArtifact = watchArtifactRefFor(bundlePath, analysisProfile);
    artifact.title = title.empty() ? defaultWatchTitle(bundlePath, analysisProfile) : title;
    artifact.bundlePath = bundlePath;
    artifact.analysisProfile = analysisProfile;
    artifact.enabled = enabled;
    artifact.evaluationCadenceMinutes = evaluationCadenceMinutes;
    artifact.minimumFindingCount = minimumFindingCount;
    artifact.minimumSeverity = minimumSeverity;
    artifact.requiredCategory = requiredCategory;
    artifact.bundleSummary = bundleSummary;

    const fs::path manifestPath = artifact.watchArtifact.manifestPath;
    if (fs::exists(manifestPath)) {
        if (!loadWatchDefinitionFromPath(manifestPath, &artifact, errorCode, errorMessage)) {
            return false;
        }
        artifact.title = title.empty() ? artifact.title : title;
        artifact.enabled = enabled;
        artifact.evaluationCadenceMinutes = evaluationCadenceMinutes;
        artifact.minimumFindingCount = minimumFindingCount;
        artifact.minimumSeverity = minimumSeverity;
        artifact.requiredCategory = requiredCategory;
        artifact.bundleSummary = bundleSummary;
        artifact.updatedAtUtc = utcTimestampIso8601Now();
        if (!artifact.enabled || artifact.evaluationCadenceMinutes == 0) {
            artifact.nextEvaluationAtUtc.clear();
        } else if (artifact.latestEvaluationAtUtc.empty() &&
                   artifact.nextEvaluationAtUtc.empty()) {
            artifact.nextEvaluationAtUtc = artifact.updatedAtUtc;
        }
        artifact.manifest = manifestForWatchDefinition(artifact);
        std::string writeError;
        if (!writeJsonTextFile(manifestPath, artifact.manifest, &writeError)) {
            if (errorCode != nullptr) {
                *errorCode = "watch_update_failed";
            }
            if (errorMessage != nullptr) {
                *errorMessage = writeError;
            }
            return false;
        }
        if (created != nullptr) {
            *created = false;
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

    artifact.createdAtUtc = utcTimestampIso8601Now();
    artifact.updatedAtUtc = artifact.createdAtUtc;
    artifact.nextEvaluationAtUtc =
        (artifact.enabled && artifact.evaluationCadenceMinutes > 0) ? artifact.createdAtUtc : std::string();
    artifact.manifest = manifestForWatchDefinition(artifact);
    std::string directoryError;
    if (!ensureDirectoryExists(artifact.watchArtifact.artifactRootDir, &directoryError)) {
        if (errorCode != nullptr) {
            *errorCode = "watch_create_failed";
        }
        if (errorMessage != nullptr) {
            *errorMessage = directoryError;
        }
        return false;
    }
    std::string writeError;
    if (!writeJsonTextFile(manifestPath, artifact.manifest, &writeError)) {
        if (errorCode != nullptr) {
            *errorCode = "watch_create_failed";
        }
        if (errorMessage != nullptr) {
            *errorMessage = writeError;
        }
        return false;
    }
    if (created != nullptr) {
        *created = true;
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

bool loadWatchDefinition(const std::string& manifestPath,
                         const std::string& artifactId,
                         WatchDefinitionArtifact* out,
                         std::string* errorCode,
                         std::string* errorMessage) {
    const bool hasManifest = !manifestPath.empty();
    const bool hasArtifactId = !artifactId.empty();
    if (hasManifest == hasArtifactId) {
        if (errorCode != nullptr) {
            *errorCode = "invalid_arguments";
        }
        if (errorMessage != nullptr) {
            *errorMessage = "exactly one of watch_manifest_path or watch_artifact_id is required";
        }
        return false;
    }
    const fs::path resolvedPath = hasManifest
        ? fs::path(manifestPath)
        : phase8RootDir() / "watch-definitions" / artifactId / "manifest.json";
    return loadWatchDefinitionFromPath(resolvedPath, out, errorCode, errorMessage);
}

bool listWatchDefinitions(std::size_t limit,
                          std::vector<WatchDefinitionArtifact>* out,
                          std::string* errorCode,
                          std::string* errorMessage) {
    return listArtifactsUnder<WatchDefinitionArtifact>(
        phase8RootDir() / "watch-definitions",
        limit,
        false,
        out,
        errorCode,
        errorMessage,
        [](const fs::path& manifestPath, WatchDefinitionArtifact* artifact, std::string* code, std::string* message) {
            return loadWatchDefinitionFromPath(manifestPath, artifact, code, message);
        });
}

bool listDueWatchDefinitions(std::size_t limit,
                             DueWatchInventoryResult* out,
                             std::string* errorCode,
                             std::string* errorMessage) {
    if (out == nullptr) {
        if (errorCode != nullptr) {
            *errorCode = "invalid_arguments";
        }
        if (errorMessage != nullptr) {
            *errorMessage = "output result is required";
        }
        return false;
    }
    out->watchDefinitions.clear();
    out->matchedCount = 0;

    std::vector<WatchDefinitionArtifact> allWatches;
    if (!listWatchDefinitions(0, &allWatches, errorCode, errorMessage)) {
        return false;
    }

    const auto now = std::chrono::system_clock::now();
    std::vector<WatchDefinitionArtifact> dueWatches;
    for (const auto& watch : allWatches) {
        if (watchIsDueAt(watch, now)) {
            dueWatches.push_back(watch);
        }
    }

    std::sort(dueWatches.begin(), dueWatches.end(), [](const WatchDefinitionArtifact& left,
                                                       const WatchDefinitionArtifact& right) {
        if (left.nextEvaluationAtUtc != right.nextEvaluationAtUtc) {
            if (left.nextEvaluationAtUtc.empty()) {
                return true;
            }
            if (right.nextEvaluationAtUtc.empty()) {
                return false;
            }
            return left.nextEvaluationAtUtc < right.nextEvaluationAtUtc;
        }
        if (left.title != right.title) {
            return left.title < right.title;
        }
        return left.watchArtifact.artifactId < right.watchArtifact.artifactId;
    });

    out->matchedCount = dueWatches.size();
    if (limit > 0 && dueWatches.size() > limit) {
        dueWatches.resize(limit);
    }
    out->watchDefinitions = std::move(dueWatches);
    if (errorCode != nullptr) {
        errorCode->clear();
    }
    if (errorMessage != nullptr) {
        errorMessage->clear();
    }
    return true;
}

bool evaluateWatchDefinition(const std::string& manifestPath,
                             const std::string& artifactId,
                             const std::string& triggerReason,
                             TriggerRunArtifact* out,
                             bool* created,
                             std::string* errorCode,
                             std::string* errorMessage) {
    WatchDefinitionArtifact watch;
    if (!loadWatchDefinition(manifestPath, artifactId, &watch, errorCode, errorMessage)) {
        return false;
    }
    if (!watch.enabled) {
        if (errorCode != nullptr) {
            *errorCode = "watch_disabled";
        }
        if (errorMessage != nullptr) {
            *errorMessage = "watch definition is disabled";
        }
        return false;
    }

    tape_phase7::AnalysisArtifact analysis;
    bool analysisCreated = false;
    if (!tape_phase7::runAnalyzerFromBundlePath(watch.bundlePath,
                                                watch.analysisProfile,
                                                &analysis,
                                                &analysisCreated,
                                                errorCode,
                                                errorMessage)) {
        return false;
    }

    TriggerRunArtifact artifact;
    artifact.watchArtifact = watch.watchArtifact;
    artifact.sourceArtifact = analysis.sourceArtifact;
    artifact.analysisArtifact = analysis.analysisArtifact;
    artifact.title = watch.title;
    artifact.analysisProfile = watch.analysisProfile;
    artifact.triggerReason = triggerReason.empty() ? std::string(kDefaultTriggerReason) : triggerReason;
    artifact.analysisCreated = analysisCreated;
    artifact.findingCount = analysis.findings.size();
    artifact.highestSeverity = highestSeverityFor(analysis);
    artifact.generatedAtUtc = utcTimestampIso8601Now();
    artifact.findingCategories = findingCategoriesFor(analysis);
    artifact.suppressionReasons = triggerSuppressionReasonsFor(watch, analysis);
    const bool triggered = !artifact.suppressionReasons.is_array() || artifact.suppressionReasons.empty();
    artifact.triggerOutcome = triggered ? kTriggerOutcomeTriggered : kTriggerOutcomeSuppressed;
    artifact.attentionStatus = triggered ? kAttentionStatusNew : kAttentionStatusSuppressed;
    artifact.attentionOpen = triggered;
    artifact.attentionUpdatedAtUtc = artifact.generatedAtUtc;
    artifact.snoozedUntilUtc.clear();
    artifact.headline = triggered ? triggerHeadlineFor(watch, analysis)
                                  : suppressedTriggerHeadlineFor(watch, artifact.suppressionReasons);
    artifact.triggerArtifact = triggerArtifactRefFor(watch.watchArtifact, analysis.analysisArtifact, artifact.generatedAtUtc);
    artifact.manifest = manifestForTriggerRun(artifact);

    std::string directoryError;
    if (!ensureDirectoryExists(artifact.triggerArtifact.artifactRootDir, &directoryError)) {
        if (errorCode != nullptr) {
            *errorCode = "trigger_create_failed";
        }
        if (errorMessage != nullptr) {
            *errorMessage = directoryError;
        }
        return false;
    }
    std::string writeError;
    if (!writeJsonTextFile(artifact.triggerArtifact.manifestPath, artifact.manifest, &writeError)) {
        if (errorCode != nullptr) {
            *errorCode = "trigger_create_failed";
        }
        if (errorMessage != nullptr) {
            *errorMessage = writeError;
        }
        return false;
    }

    watch.latestTriggerArtifactId = artifact.triggerArtifact.artifactId;
    watch.latestEvaluationAtUtc = artifact.generatedAtUtc;
    watch.nextEvaluationAtUtc =
        watch.enabled ? scheduleNextEvaluationAtUtc(artifact.generatedAtUtc, watch.evaluationCadenceMinutes)
                      : std::string();
    watch.latestTriggerOutcome = artifact.triggerOutcome;
    watch.updatedAtUtc = artifact.generatedAtUtc;
    watch.manifest = manifestForWatchDefinition(watch);
    if (!writeJsonTextFile(watch.watchArtifact.manifestPath, watch.manifest, &writeError)) {
        if (errorCode != nullptr) {
            *errorCode = "watch_update_failed";
        }
        if (errorMessage != nullptr) {
            *errorMessage = writeError;
        }
        return false;
    }

    if (created != nullptr) {
        *created = true;
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

bool runDueWatchDefinitions(std::size_t limit,
                            const std::string& triggerReason,
                            DueWatchRunResult* out,
                            std::string* errorCode,
                            std::string* errorMessage) {
    if (out == nullptr) {
        if (errorCode != nullptr) {
            *errorCode = "invalid_arguments";
        }
        if (errorMessage != nullptr) {
            *errorMessage = "output result is required";
        }
        return false;
    }

    DueWatchInventoryResult dueInventory;
    if (!listDueWatchDefinitions(limit, &dueInventory, errorCode, errorMessage)) {
        return false;
    }

    DueWatchRunResult result;
    result.triggerReason = triggerReason.empty() ? std::string(kScheduledTriggerReason) : triggerReason;
    result.matchedWatchCount = dueInventory.matchedCount;
    result.triggerRuns.reserve(dueInventory.watchDefinitions.size());

    for (const auto& watch : dueInventory.watchDefinitions) {
        TriggerRunArtifact trigger;
        bool created = false;
        if (!evaluateWatchDefinition(watch.watchArtifact.manifestPath,
                                     {},
                                     result.triggerReason,
                                     &trigger,
                                     &created,
                                     errorCode,
                                     errorMessage)) {
            return false;
        }
        ++result.evaluatedWatchCount;
        if (created) {
            ++result.createdTriggerCount;
        }
        if (trigger.attentionOpen) {
            ++result.attentionOpenedCount;
        }
        if (trigger.triggerOutcome == kTriggerOutcomeSuppressed) {
            ++result.suppressedCount;
        }
        result.triggerRuns.push_back(std::move(trigger));
    }

    if (out != nullptr) {
        *out = std::move(result);
    }
    if (errorCode != nullptr) {
        errorCode->clear();
    }
    if (errorMessage != nullptr) {
        errorMessage->clear();
    }
    return true;
}

bool loadTriggerRunArtifact(const std::string& manifestPath,
                            const std::string& artifactId,
                            TriggerRunArtifact* out,
                            std::string* errorCode,
                            std::string* errorMessage) {
    const bool hasManifest = !manifestPath.empty();
    const bool hasArtifactId = !artifactId.empty();
    if (hasManifest == hasArtifactId) {
        if (errorCode != nullptr) {
            *errorCode = "invalid_arguments";
        }
        if (errorMessage != nullptr) {
            *errorMessage = "exactly one of trigger_manifest_path or trigger_artifact_id is required";
        }
        return false;
    }
    const fs::path resolvedPath = hasManifest
        ? fs::path(manifestPath)
        : phase8RootDir() / "trigger-runs" / artifactId / "manifest.json";
    return loadTriggerRunFromPath(resolvedPath, out, errorCode, errorMessage);
}

bool listTriggerRuns(std::size_t limit,
                     std::vector<TriggerRunArtifact>* out,
                     std::string* errorCode,
                     std::string* errorMessage) {
    TriggerRunInventorySelection selection;
    selection.limit = limit;
    TriggerRunInventoryResult result;
    if (!listTriggerRuns(selection, &result, errorCode, errorMessage)) {
        return false;
    }
    if (out != nullptr) {
        *out = std::move(result.triggerRuns);
    }
    return true;
}

bool listTriggerRuns(const TriggerRunInventorySelection& selection,
                     TriggerRunInventoryResult* out,
                     std::string* errorCode,
                     std::string* errorMessage) {
    if (out == nullptr) {
        if (errorCode != nullptr) {
            *errorCode = "invalid_arguments";
        }
        if (errorMessage != nullptr) {
            *errorMessage = "output result is required";
        }
        return false;
    }
    out->triggerRuns.clear();
    out->appliedFilters = selection;
    out->matchedCount = 0;

    std::vector<TriggerRunArtifact> allArtifacts;
    const std::size_t limit = selection.limit;
    if (!listArtifactsUnder<TriggerRunArtifact>(
            phase8RootDir() / "trigger-runs",
            0,
            true,
            &allArtifacts,
            errorCode,
            errorMessage,
            [](const fs::path& manifestPath,
               TriggerRunArtifact* artifact,
               std::string* code,
               std::string* message) {
                return loadTriggerRunFromPath(manifestPath, artifact, code, message);
            })) {
        return false;
    }

    std::vector<TriggerRunArtifact> filtered;
    for (auto artifact : allArtifacts) {
        refreshEffectiveAttentionState(&artifact);
        if (!triggerMatchesSelection(artifact, selection)) {
            continue;
        }
        filtered.push_back(std::move(artifact));
    }
    out->matchedCount = filtered.size();
    if (limit > 0 && filtered.size() > limit) {
        filtered.resize(limit);
    }
    out->triggerRuns = std::move(filtered);
    if (errorCode != nullptr) {
        errorCode->clear();
    }
    if (errorMessage != nullptr) {
        errorMessage->clear();
    }
    return true;
}

bool listAttentionInbox(std::size_t limit,
                        std::vector<AttentionInboxItem>* out,
                        std::string* errorCode,
                        std::string* errorMessage) {
    if (out == nullptr) {
        if (errorCode != nullptr) {
            *errorCode = "invalid_arguments";
        }
        if (errorMessage != nullptr) {
            *errorMessage = "output vector is required";
        }
        return false;
    }
    out->clear();

    std::vector<TriggerRunArtifact> triggers;
    if (!listTriggerRuns(0, &triggers, errorCode, errorMessage)) {
        return false;
    }
    std::vector<AttentionInboxItem> items;
    for (auto trigger : triggers) {
        refreshEffectiveAttentionState(&trigger);
        if (!trigger.attentionOpen) {
            continue;
        }
        items.push_back(attentionItemFromTrigger(trigger));
    }
    std::sort(items.begin(), items.end(), [](const AttentionInboxItem& lhs, const AttentionInboxItem& rhs) {
        if (lhs.generatedAtUtc != rhs.generatedAtUtc) {
            return lhs.generatedAtUtc > rhs.generatedAtUtc;
        }
        return lhs.triggerArtifactId < rhs.triggerArtifactId;
    });
    if (limit > 0 && items.size() > limit) {
        items.resize(limit);
    }
    *out = std::move(items);
    if (errorCode != nullptr) {
        errorCode->clear();
    }
    if (errorMessage != nullptr) {
        errorMessage->clear();
    }
    return true;
}

bool recordAttentionAction(const std::string& manifestPath,
                           const std::string& artifactId,
                           const std::string& action,
                           const std::size_t snoozeMinutes,
                           const std::string& actor,
                           const std::string& comment,
                           TriggerRunArtifact* out,
                           std::string* errorCode,
                           std::string* errorMessage) {
    if (!isSupportedAttentionAction(action)) {
        if (errorCode != nullptr) {
            *errorCode = "invalid_arguments";
        }
        if (errorMessage != nullptr) {
            *errorMessage = "action must be one of acknowledge, snooze, or resolve";
        }
        return false;
    }
    if (action == "snooze" && snoozeMinutes == 0) {
        if (errorCode != nullptr) {
            *errorCode = "invalid_arguments";
        }
        if (errorMessage != nullptr) {
            *errorMessage = "snooze action requires snooze_minutes greater than zero";
        }
        return false;
    }

    TriggerRunArtifact artifact;
    if (!loadTriggerRunArtifact(manifestPath, artifactId, &artifact, errorCode, errorMessage)) {
        return false;
    }
    if (artifact.triggerOutcome != kTriggerOutcomeTriggered) {
        if (errorCode != nullptr) {
            *errorCode = "trigger_not_actionable";
        }
        if (errorMessage != nullptr) {
            *errorMessage = "only triggered attention items can be updated";
        }
        return false;
    }

    const std::string nowUtc = utcTimestampIso8601Now();
    artifact.attentionStatus = attentionStatusForAction(action);
    artifact.attentionUpdatedAtUtc = nowUtc;
    artifact.attentionActor = actor;
    artifact.attentionComment = comment;
    artifact.snoozedUntilUtc.clear();
    if (action == "snooze") {
        const auto now = parseUtcTimestampIso8601(nowUtc).value_or(std::chrono::system_clock::now());
        artifact.snoozedUntilUtc = utcTimestampIso8601For(now + std::chrono::minutes(snoozeMinutes));
    }
    refreshEffectiveAttentionState(&artifact);
    artifact.manifest = manifestForTriggerRun(artifact);

    std::string writeError;
    if (!writeJsonTextFile(artifact.triggerArtifact.manifestPath, artifact.manifest, &writeError)) {
        if (errorCode != nullptr) {
            *errorCode = "trigger_update_failed";
        }
        if (errorMessage != nullptr) {
            *errorMessage = writeError;
        }
        return false;
    }

    if (out != nullptr) {
        *out = artifact;
    }
    if (errorCode != nullptr) {
        errorCode->clear();
    }
    if (errorMessage != nullptr) {
        errorMessage->clear();
    }
    return true;
}

json watchDefinitionToJson(const WatchDefinitionArtifact& artifact) {
    return {
        {"watch_definition", phase8ArtifactRefToJson(artifact.watchArtifact)},
        {"title", artifact.title},
        {"bundle_path", artifact.bundlePath},
        {"analysis_profile", artifact.analysisProfile},
        {"enabled", artifact.enabled},
        {"evaluation_cadence_minutes", artifact.evaluationCadenceMinutes},
        {"minimum_finding_count", artifact.minimumFindingCount},
        {"minimum_severity", artifact.minimumSeverity.empty() ? json(nullptr) : json(artifact.minimumSeverity)},
        {"required_category", artifact.requiredCategory.empty() ? json(nullptr) : json(artifact.requiredCategory)},
        {"created_at_utc", artifact.createdAtUtc},
        {"updated_at_utc", artifact.updatedAtUtc},
        {"latest_evaluation_at_utc",
         artifact.latestEvaluationAtUtc.empty() ? json(nullptr) : json(artifact.latestEvaluationAtUtc)},
        {"next_evaluation_at_utc",
         artifact.nextEvaluationAtUtc.empty() ? json(nullptr) : json(artifact.nextEvaluationAtUtc)},
        {"latest_trigger_outcome",
         artifact.latestTriggerOutcome.empty() ? json(nullptr) : json(artifact.latestTriggerOutcome)},
        {"latest_trigger_artifact_id",
         artifact.latestTriggerArtifactId.empty() ? json(nullptr) : json(artifact.latestTriggerArtifactId)},
        {"bundle_summary", artifact.bundleSummary}
    };
}

json triggerRunToJson(const TriggerRunArtifact& artifact) {
    return {
        {"watch_definition", phase8ArtifactRefToJson(artifact.watchArtifact)},
        {"source_artifact", phase8ArtifactRefToJson(artifact.sourceArtifact)},
        {"analysis_artifact", phase8ArtifactRefToJson(artifact.analysisArtifact)},
        {"trigger_run", phase8ArtifactRefToJson(artifact.triggerArtifact)},
        {"title", artifact.title},
        {"headline", artifact.headline},
        {"analysis_profile", artifact.analysisProfile},
        {"trigger_reason", artifact.triggerReason},
        {"trigger_outcome", artifact.triggerOutcome},
        {"attention_status", artifact.attentionStatus},
        {"attention_open", artifact.attentionOpen},
        {"attention_updated_at_utc",
         artifact.attentionUpdatedAtUtc.empty() ? json(nullptr) : json(artifact.attentionUpdatedAtUtc)},
        {"attention_actor", artifact.attentionActor.empty() ? json(nullptr) : json(artifact.attentionActor)},
        {"attention_comment", artifact.attentionComment.empty() ? json(nullptr) : json(artifact.attentionComment)},
        {"snoozed_until_utc", artifact.snoozedUntilUtc.empty() ? json(nullptr) : json(artifact.snoozedUntilUtc)},
        {"analysis_created", artifact.analysisCreated},
        {"finding_count", artifact.findingCount},
        {"highest_severity", artifact.highestSeverity.empty() ? json(nullptr) : json(artifact.highestSeverity)},
        {"generated_at_utc", artifact.generatedAtUtc},
        {"finding_categories", artifact.findingCategories},
        {"suppression_reasons", artifact.suppressionReasons}
    };
}

json attentionInboxItemToJson(const AttentionInboxItem& item) {
    return {
        {"trigger_artifact_id", item.triggerArtifactId},
        {"watch_artifact_id", item.watchArtifactId},
        {"analysis_artifact_id", item.analysisArtifactId},
        {"source_artifact_id", item.sourceArtifactId},
        {"title", item.title},
        {"headline", item.headline},
        {"attention_status", item.attentionStatus},
        {"attention_open", item.attentionOpen},
        {"trigger_outcome", item.triggerOutcome},
        {"highest_severity", item.highestSeverity.empty() ? json(nullptr) : json(item.highestSeverity)},
        {"analysis_profile", item.analysisProfile},
        {"finding_count", item.findingCount},
        {"generated_at_utc", item.generatedAtUtc},
        {"attention_updated_at_utc",
         item.attentionUpdatedAtUtc.empty() ? json(nullptr) : json(item.attentionUpdatedAtUtc)},
        {"snoozed_until_utc", item.snoozedUntilUtc.empty() ? json(nullptr) : json(item.snoozedUntilUtc)}
    };
}

std::string watchDefinitionArtifactMarkdown(const WatchDefinitionArtifact& artifact) {
    std::ostringstream out;
    out << "# " << artifact.title << "\n\n";
    out << "- watch_artifact_id: `" << artifact.watchArtifact.artifactId << "`\n";
    out << "- analysis_profile: `" << artifact.analysisProfile << "`\n";
    out << "- enabled: `" << (artifact.enabled ? "true" : "false") << "`\n";
    out << "- evaluation_cadence_minutes: `" << artifact.evaluationCadenceMinutes << "`\n";
    out << "- minimum_finding_count: `" << artifact.minimumFindingCount << "`\n";
    out << "- minimum_severity: `"
        << (artifact.minimumSeverity.empty() ? std::string("--") : artifact.minimumSeverity) << "`\n";
    out << "- required_category: `"
        << (artifact.requiredCategory.empty() ? std::string("--") : artifact.requiredCategory) << "`\n";
    out << "- bundle_path: `" << artifact.bundlePath << "`\n";
    if (!artifact.latestEvaluationAtUtc.empty()) {
        out << "- latest_evaluation_at_utc: `" << artifact.latestEvaluationAtUtc << "`\n";
    }
    if (!artifact.nextEvaluationAtUtc.empty()) {
        out << "- next_evaluation_at_utc: `" << artifact.nextEvaluationAtUtc << "`\n";
        out << "- due_now: `" << (watchIsDueNow(artifact) ? "true" : "false") << "`\n";
    } else {
        out << "- next_evaluation_at_utc: `--`\n";
        out << "- due_now: `false`\n";
    }
    if (!artifact.latestTriggerOutcome.empty()) {
        out << "- latest_trigger_outcome: `" << artifact.latestTriggerOutcome << "`\n";
    }
    if (!artifact.latestTriggerArtifactId.empty()) {
        out << "- latest_trigger_artifact_id: `" << artifact.latestTriggerArtifactId << "`\n";
    }
    if (artifact.bundleSummary.is_object()) {
        out << "- bundle_type: `" << stringValueOrDefault(artifact.bundleSummary, "bundle_type", "--") << "`\n";
        out << "- source_artifact_id: `" << stringValueOrDefault(artifact.bundleSummary, "source_artifact_id", "--") << "`\n";
    }
    return out.str();
}

std::string triggerRunArtifactMarkdown(const TriggerRunArtifact& artifact) {
    std::ostringstream out;
    out << "# " << artifact.headline << "\n\n";
    out << "- trigger_artifact_id: `" << artifact.triggerArtifact.artifactId << "`\n";
    out << "- watch_artifact_id: `" << artifact.watchArtifact.artifactId << "`\n";
    out << "- analysis_artifact_id: `" << artifact.analysisArtifact.artifactId << "`\n";
    out << "- source_artifact_id: `" << artifact.sourceArtifact.artifactId << "`\n";
    out << "- analysis_profile: `" << artifact.analysisProfile << "`\n";
    out << "- trigger_reason: `" << artifact.triggerReason << "`\n";
    out << "- trigger_outcome: `" << artifact.triggerOutcome << "`\n";
    out << "- attention_status: `" << artifact.attentionStatus << "`\n";
    out << "- attention_open: `" << (artifact.attentionOpen ? "true" : "false") << "`\n";
    if (!artifact.attentionUpdatedAtUtc.empty()) {
        out << "- attention_updated_at_utc: `" << artifact.attentionUpdatedAtUtc << "`\n";
    }
    if (!artifact.attentionActor.empty()) {
        out << "- attention_actor: `" << artifact.attentionActor << "`\n";
    }
    if (!artifact.attentionComment.empty()) {
        out << "- attention_comment: `" << artifact.attentionComment << "`\n";
    }
    if (!artifact.snoozedUntilUtc.empty()) {
        out << "- snoozed_until_utc: `" << artifact.snoozedUntilUtc << "`\n";
    }
    out << "- highest_severity: `" << (artifact.highestSeverity.empty() ? std::string("--") : artifact.highestSeverity) << "`\n";
    out << "- finding_count: `" << artifact.findingCount << "`\n";
    out << "- analysis_created: `" << (artifact.analysisCreated ? "true" : "false") << "`\n";
    if (artifact.suppressionReasons.is_array() && !artifact.suppressionReasons.empty()) {
        out << "- suppression_reason_count: `" << artifact.suppressionReasons.size() << "`\n";
    }
    return out.str();
}

} // namespace tape_phase8
