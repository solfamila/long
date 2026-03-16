#pragma once

#include <filesystem>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

namespace tape_phase7 {

using json = nlohmann::json;

inline constexpr const char* kContractVersion = "phase7-analyzer-playbook-v1";
inline constexpr const char* kAnalysisArtifactType = "phase7.analysis_output.v1";
inline constexpr const char* kPlaybookArtifactType = "phase7.playbook_plan.v1";
inline constexpr const char* kExecutionLedgerArtifactType = "phase7.execution_ledger.v1";
inline constexpr const char* kDefaultAnalyzerProfile = "phase7.trace_fill_integrity.v1";
inline constexpr const char* kDefaultPlaybookMode = "dry_run";
inline constexpr const char* kApplyPlaybookMode = "apply";
inline constexpr const char* kDefaultLedgerStatus = "review_pending";
inline constexpr const char* kDefaultLedgerEntryStatus = "pending_review";

struct ArtifactRef {
    std::string artifactType;
    std::string contractVersion;
    std::string artifactId;
    std::string manifestPath;
    std::string artifactRootDir;
};

struct FindingRecord {
    std::string findingId;
    std::string severity;
    std::string category;
    std::string summary;
    json evidenceRefs = json::array();
};

struct AnalyzerProfileSpec {
    std::string analysisProfile = kDefaultAnalyzerProfile;
    std::string title;
    std::string summary;
    std::vector<std::string> supportedSourceBundleTypes;
    std::vector<std::string> findingCategories;
    bool defaultProfile = false;
};

struct AnalysisArtifact {
    ArtifactRef sourceArtifact;
    ArtifactRef analysisArtifact;
    std::string analysisProfile = kDefaultAnalyzerProfile;
    std::string generatedAtUtc;
    json replayContext = json::object();
    std::vector<FindingRecord> findings;
    json manifest = json::object();
};

struct PlaybookAction {
    std::string actionId;
    std::string actionType;
    std::string findingId;
    std::string title;
    std::string summary;
    json suggestedTools = json::array();
};

struct PlaybookArtifact {
    ArtifactRef analysisArtifact;
    ArtifactRef playbookArtifact;
    std::string mode = kDefaultPlaybookMode;
    std::string generatedAtUtc;
    json replayContext = json::object();
    std::vector<std::string> filteredFindingIds;
    std::vector<PlaybookAction> plannedActions;
    json manifest = json::object();
};

struct ExecutionLedgerEntry {
    std::string entryId;
    std::string actionId;
    std::string actionType;
    std::string findingId;
    std::string reviewStatus = kDefaultLedgerEntryStatus;
    bool requiresManualConfirmation = true;
    std::string title;
    std::string summary;
    json suggestedTools = json::array();
};

struct ExecutionLedgerArtifact {
    ArtifactRef sourceArtifact;
    ArtifactRef analysisArtifact;
    ArtifactRef playbookArtifact;
    ArtifactRef ledgerArtifact;
    std::string mode = kDefaultPlaybookMode;
    std::string generatedAtUtc;
    std::string ledgerStatus = kDefaultLedgerStatus;
    json executionPolicy = json::object();
    json replayContext = json::object();
    std::vector<std::string> filteredFindingIds;
    std::vector<ExecutionLedgerEntry> entries;
    json auditTrail = json::array();
    json manifest = json::object();
};

bool listAnalyzerProfiles(std::vector<AnalyzerProfileSpec>* out,
                          std::string* errorCode,
                          std::string* errorMessage);

bool loadAnalyzerProfile(const std::string& analysisProfile,
                         AnalyzerProfileSpec* out,
                         std::string* errorCode,
                         std::string* errorMessage);

bool runAnalyzerFromBundlePath(const std::string& bundlePath,
                               const std::string& analysisProfile,
                               AnalysisArtifact* out,
                               bool* created,
                               std::string* errorCode,
                               std::string* errorMessage);

bool loadAnalysisArtifact(const std::string& manifestPath,
                          const std::string& artifactId,
                          AnalysisArtifact* out,
                          std::string* errorCode,
                          std::string* errorMessage);

bool listAnalysisArtifacts(std::size_t limit,
                           std::vector<AnalysisArtifact>* out,
                           std::string* errorCode,
                           std::string* errorMessage);

bool buildGuardedPlaybook(const std::string& manifestPath,
                          const std::string& artifactId,
                          const std::vector<std::string>& findingIds,
                          const std::string& mode,
                          PlaybookArtifact* out,
                          bool* created,
                          std::string* errorCode,
                          std::string* errorMessage);

bool loadPlaybookArtifact(const std::string& manifestPath,
                          const std::string& artifactId,
                          PlaybookArtifact* out,
                          std::string* errorCode,
                          std::string* errorMessage);

bool listPlaybookArtifacts(std::size_t limit,
                           std::vector<PlaybookArtifact>* out,
                           std::string* errorCode,
                           std::string* errorMessage);

bool buildExecutionLedger(const std::string& manifestPath,
                          const std::string& artifactId,
                          ExecutionLedgerArtifact* out,
                          bool* created,
                          std::string* errorCode,
                          std::string* errorMessage);

bool loadExecutionLedgerArtifact(const std::string& manifestPath,
                                 const std::string& artifactId,
                                 ExecutionLedgerArtifact* out,
                                 std::string* errorCode,
                                 std::string* errorMessage);

bool listExecutionLedgers(std::size_t limit,
                          std::vector<ExecutionLedgerArtifact>* out,
                          std::string* errorCode,
                          std::string* errorMessage);

json artifactRefToJson(const ArtifactRef& artifact);
json findingToJson(const FindingRecord& finding);
json playbookActionToJson(const PlaybookAction& action);
json executionLedgerEntryToJson(const ExecutionLedgerEntry& entry);
std::string analysisArtifactMarkdown(const AnalysisArtifact& artifact);
std::string playbookArtifactMarkdown(const PlaybookArtifact& artifact);
std::string executionLedgerArtifactMarkdown(const ExecutionLedgerArtifact& artifact);

} // namespace tape_phase7
