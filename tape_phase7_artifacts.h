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
inline constexpr const char* kDefaultAnalyzerProfile = "phase7.trace_fill_integrity.v1";
inline constexpr const char* kDefaultPlaybookMode = "dry_run";
inline constexpr const char* kApplyPlaybookMode = "apply";

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

struct AnalysisArtifact {
    ArtifactRef sourceArtifact;
    ArtifactRef analysisArtifact;
    std::string analysisProfile = kDefaultAnalyzerProfile;
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
    json replayContext = json::object();
    std::vector<std::string> filteredFindingIds;
    std::vector<PlaybookAction> plannedActions;
    json manifest = json::object();
};

bool runAnalyzerFromBundlePath(const std::string& bundlePath,
                               const std::string& analysisProfile,
                               AnalysisArtifact* out,
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

json artifactRefToJson(const ArtifactRef& artifact);
json findingToJson(const FindingRecord& finding);
json playbookActionToJson(const PlaybookAction& action);
std::string analysisArtifactMarkdown(const AnalysisArtifact& artifact);
std::string playbookArtifactMarkdown(const PlaybookArtifact& artifact);

} // namespace tape_phase7
