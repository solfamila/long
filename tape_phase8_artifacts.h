#pragma once

#include "tape_phase7_artifacts.h"

#include <cstddef>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

namespace tape_phase8 {

using json = nlohmann::json;
using ArtifactRef = tape_phase7::ArtifactRef;

inline constexpr const char* kContractVersion = "phase8-trigger-inbox-v1";
inline constexpr const char* kWatchDefinitionArtifactType = "phase8.watch_definition.v1";
inline constexpr const char* kTriggerRunArtifactType = "phase8.trigger_run.v1";
inline constexpr const char* kDefaultTriggerReason = "manual_eval";
inline constexpr const char* kScheduledTriggerReason = "scheduled_eval";
inline constexpr const char* kAttentionStatusNew = "attention_new";
inline constexpr const char* kAttentionStatusSuppressed = "attention_suppressed";
inline constexpr const char* kAttentionStatusAcknowledged = "attention_acknowledged";
inline constexpr const char* kAttentionStatusSnoozed = "attention_snoozed";
inline constexpr const char* kAttentionStatusResolved = "attention_resolved";
inline constexpr const char* kTriggerOutcomeTriggered = "triggered";
inline constexpr const char* kTriggerOutcomeSuppressed = "suppressed";
inline constexpr std::size_t kDefaultEvaluationCadenceMinutes = 15;
inline constexpr std::size_t kDefaultAttentionSnoozeMinutes = 60;

struct WatchDefinitionArtifact {
    ArtifactRef watchArtifact;
    std::string title;
    std::string bundlePath;
    std::string analysisProfile = tape_phase7::kDefaultAnalyzerProfile;
    bool enabled = true;
    std::size_t evaluationCadenceMinutes = kDefaultEvaluationCadenceMinutes;
    std::size_t minimumFindingCount = 1;
    std::string minimumSeverity;
    std::string requiredCategory;
    std::string createdAtUtc;
    std::string updatedAtUtc;
    std::string latestEvaluationAtUtc;
    std::string nextEvaluationAtUtc;
    std::string latestTriggerOutcome;
    std::string latestTriggerArtifactId;
    json bundleSummary = json::object();
    json manifest = json::object();
};

struct TriggerRunArtifact {
    ArtifactRef watchArtifact;
    ArtifactRef sourceArtifact;
    ArtifactRef analysisArtifact;
    ArtifactRef triggerArtifact;
    std::string title;
    std::string analysisProfile = tape_phase7::kDefaultAnalyzerProfile;
    std::string triggerReason = kDefaultTriggerReason;
    std::string triggerOutcome = kTriggerOutcomeTriggered;
    std::string attentionStatus = kAttentionStatusNew;
    bool attentionOpen = true;
    std::string attentionUpdatedAtUtc;
    std::string attentionActor;
    std::string attentionComment;
    std::string snoozedUntilUtc;
    bool analysisCreated = false;
    std::size_t findingCount = 0;
    std::string highestSeverity;
    std::string generatedAtUtc;
    std::string headline;
    json findingCategories = json::array();
    json suppressionReasons = json::array();
    json manifest = json::object();
};

struct AttentionInboxItem {
    std::string triggerArtifactId;
    std::string watchArtifactId;
    std::string analysisArtifactId;
    std::string sourceArtifactId;
    std::string title;
    std::string headline;
    std::string attentionStatus = kAttentionStatusNew;
    bool attentionOpen = true;
    std::string triggerOutcome = kTriggerOutcomeTriggered;
    std::string highestSeverity;
    std::string analysisProfile = tape_phase7::kDefaultAnalyzerProfile;
    std::size_t findingCount = 0;
    std::string generatedAtUtc;
    std::string attentionUpdatedAtUtc;
    std::string snoozedUntilUtc;
};

struct DueWatchInventoryResult {
    std::vector<WatchDefinitionArtifact> watchDefinitions;
    std::size_t matchedCount = 0;
};

struct DueWatchRunResult {
    std::string triggerReason = kScheduledTriggerReason;
    std::size_t matchedWatchCount = 0;
    std::size_t evaluatedWatchCount = 0;
    std::size_t createdTriggerCount = 0;
    std::size_t attentionOpenedCount = 0;
    std::size_t suppressedCount = 0;
    std::vector<TriggerRunArtifact> triggerRuns;
};

bool createWatchDefinition(const std::string& bundlePath,
                           const std::string& analysisProfile,
                           const std::string& title,
                           bool enabled,
                           std::size_t evaluationCadenceMinutes,
                           std::size_t minimumFindingCount,
                           const std::string& minimumSeverity,
                           const std::string& requiredCategory,
                           WatchDefinitionArtifact* out,
                           bool* created,
                           std::string* errorCode,
                           std::string* errorMessage);

bool loadWatchDefinition(const std::string& manifestPath,
                         const std::string& artifactId,
                         WatchDefinitionArtifact* out,
                         std::string* errorCode,
                         std::string* errorMessage);

bool listWatchDefinitions(std::size_t limit,
                          std::vector<WatchDefinitionArtifact>* out,
                          std::string* errorCode,
                          std::string* errorMessage);

bool listDueWatchDefinitions(std::size_t limit,
                             DueWatchInventoryResult* out,
                             std::string* errorCode,
                             std::string* errorMessage);

bool evaluateWatchDefinition(const std::string& manifestPath,
                             const std::string& artifactId,
                             const std::string& triggerReason,
                             TriggerRunArtifact* out,
                             bool* created,
                             std::string* errorCode,
                             std::string* errorMessage);

bool runDueWatchDefinitions(std::size_t limit,
                            const std::string& triggerReason,
                            DueWatchRunResult* out,
                            std::string* errorCode,
                            std::string* errorMessage);

bool loadTriggerRunArtifact(const std::string& manifestPath,
                            const std::string& artifactId,
                            TriggerRunArtifact* out,
                            std::string* errorCode,
                            std::string* errorMessage);

bool listTriggerRuns(std::size_t limit,
                     std::vector<TriggerRunArtifact>* out,
                     std::string* errorCode,
                     std::string* errorMessage);

bool listAttentionInbox(std::size_t limit,
                        std::vector<AttentionInboxItem>* out,
                        std::string* errorCode,
                        std::string* errorMessage);

bool recordAttentionAction(const std::string& manifestPath,
                           const std::string& artifactId,
                           const std::string& action,
                           std::size_t snoozeMinutes,
                           const std::string& actor,
                           const std::string& comment,
                           TriggerRunArtifact* out,
                           std::string* errorCode,
                           std::string* errorMessage);

json watchDefinitionToJson(const WatchDefinitionArtifact& artifact);
json triggerRunToJson(const TriggerRunArtifact& artifact);
json attentionInboxItemToJson(const AttentionInboxItem& item);

std::string watchDefinitionArtifactMarkdown(const WatchDefinitionArtifact& artifact);
std::string triggerRunArtifactMarkdown(const TriggerRunArtifact& artifact);

} // namespace tape_phase8
