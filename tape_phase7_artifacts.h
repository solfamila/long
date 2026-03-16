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
inline constexpr const char* kExecutionJournalArtifactType = "phase7.execution_journal.v1";
inline constexpr const char* kExecutionApplyArtifactType = "phase7.execution_apply.v1";
inline constexpr const char* kDefaultAnalyzerProfile = "phase7.trace_fill_integrity.v1";
inline constexpr const char* kIncidentTriageAnalyzerProfile = "phase7.incident_triage.v1";
inline constexpr const char* kFillQualityAnalyzerProfile = "phase7.fill_quality_review.v1";
inline constexpr const char* kLiquidityBehaviorAnalyzerProfile = "phase7.liquidity_behavior_review.v1";
inline constexpr const char* kAdverseSelectionAnalyzerProfile = "phase7.adverse_selection_review.v1";
inline constexpr const char* kOrderImpactAnalyzerProfile = "phase7.order_impact_review.v1";
inline constexpr const char* kDefaultPlaybookMode = "dry_run";
inline constexpr const char* kApplyPlaybookMode = "apply";
inline constexpr const char* kDefaultLedgerStatus = "review_pending";
inline constexpr const char* kDefaultLedgerEntryStatus = "pending_review";
inline constexpr const char* kLedgerStatusInProgress = "review_in_progress";
inline constexpr const char* kLedgerStatusCompleted = "review_completed";
inline constexpr const char* kLedgerStatusBlocked = "review_blocked";
inline constexpr const char* kLedgerStatusNeedsInformation = "needs_information";
inline constexpr const char* kLedgerStatusWaitingApproval = "review_waiting_approval";
inline constexpr const char* kLedgerStatusReadyForExecution = "ready_for_execution";
inline constexpr const char* kLedgerEntryStatusApproved = "approved";
inline constexpr const char* kLedgerEntryStatusBlocked = "blocked";
inline constexpr const char* kLedgerEntryStatusNeedsInfo = "needs_info";
inline constexpr const char* kLedgerEntryStatusNotApplicable = "not_applicable";
inline constexpr const char* kExecutionJournalStatusQueued = "execution_queued";
inline constexpr const char* kExecutionJournalStatusInProgress = "execution_in_progress";
inline constexpr const char* kExecutionJournalStatusSucceeded = "execution_succeeded";
inline constexpr const char* kExecutionJournalStatusPartiallySucceeded = "execution_partially_succeeded";
inline constexpr const char* kExecutionJournalStatusFailed = "execution_failed";
inline constexpr const char* kExecutionJournalStatusCancelled = "execution_cancelled";
inline constexpr const char* kExecutionEntryStatusQueued = "queued";
inline constexpr const char* kExecutionEntryStatusSubmitted = "submitted";
inline constexpr const char* kExecutionEntryStatusSucceeded = "succeeded";
inline constexpr const char* kExecutionEntryStatusFailed = "failed";
inline constexpr const char* kExecutionEntryStatusCancelled = "cancelled";

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
    std::string reviewedAtUtc;
    std::string reviewedBy;
    std::string reviewComment;
    std::size_t distinctReviewerCount = 0;
    std::size_t approvalReviewerCount = 0;
    bool approvalThresholdMet = false;
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
    json reviewPolicy = json::object();
    json replayContext = json::object();
    std::vector<std::string> filteredFindingIds;
    std::vector<ExecutionLedgerEntry> entries;
    json auditTrail = json::array();
    json manifest = json::object();
};

struct ExecutionLedgerReviewSummary {
    std::size_t pendingReviewCount = 0;
    std::size_t approvedCount = 0;
    std::size_t blockedCount = 0;
    std::size_t needsInfoCount = 0;
    std::size_t notApplicableCount = 0;
    std::size_t reviewedCount = 0;
    std::size_t waitingApprovalCount = 0;
    std::size_t readyEntryCount = 0;
    std::size_t actionableEntryCount = 0;
    std::size_t distinctReviewerCount = 0;
    std::size_t requiredApprovalCount = 0;
    bool readyForExecution = false;
};

struct ExecutionJournalEntry {
    std::string journalEntryId;
    std::string ledgerEntryId;
    std::string actionId;
    std::string actionType;
    std::string findingId;
    std::string executionStatus = kExecutionEntryStatusQueued;
    std::string idempotencyKey;
    bool requiresManualConfirmation = true;
    std::string title;
    std::string summary;
    std::string queuedAtUtc;
    std::string startedAtUtc;
    std::string completedAtUtc;
    std::string lastUpdatedAtUtc;
    std::string lastUpdatedBy;
    std::string executionComment;
    std::string failureCode;
    std::string failureMessage;
    std::size_t attemptCount = 0;
    bool terminal = false;
    json suggestedTools = json::array();
};

struct ExecutionJournalArtifact {
    ArtifactRef sourceArtifact;
    ArtifactRef analysisArtifact;
    ArtifactRef playbookArtifact;
    ArtifactRef ledgerArtifact;
    ArtifactRef journalArtifact;
    std::string mode = kDefaultPlaybookMode;
    std::string generatedAtUtc;
    std::string initiatedBy;
    std::string executionCapability;
    std::string journalStatus = kExecutionJournalStatusQueued;
    json executionPolicy = json::object();
    json replayContext = json::object();
    std::vector<std::string> filteredFindingIds;
    std::vector<ExecutionJournalEntry> entries;
    json auditTrail = json::array();
    json manifest = json::object();
};

struct ExecutionJournalSummary {
    std::size_t queuedCount = 0;
    std::size_t submittedCount = 0;
    std::size_t succeededCount = 0;
    std::size_t failedCount = 0;
    std::size_t cancelledCount = 0;
    std::size_t terminalCount = 0;
    std::size_t actionableEntryCount = 0;
    bool allTerminal = false;
};

struct ExecutionApplyEntry {
    std::string applyEntryId;
    std::string journalEntryId;
    std::string ledgerEntryId;
    std::string actionId;
    std::string actionType;
    std::string findingId;
    std::string executionStatus = kExecutionEntryStatusSubmitted;
    std::string idempotencyKey;
    bool requiresManualConfirmation = true;
    std::string title;
    std::string summary;
    std::string submittedAtUtc;
    std::string completedAtUtc;
    std::string lastUpdatedAtUtc;
    std::string lastUpdatedBy;
    std::string executionComment;
    std::string failureCode;
    std::string failureMessage;
    std::size_t attemptCount = 0;
    bool terminal = false;
    json suggestedTools = json::array();
};

struct ExecutionApplyArtifact {
    ArtifactRef sourceArtifact;
    ArtifactRef analysisArtifact;
    ArtifactRef playbookArtifact;
    ArtifactRef ledgerArtifact;
    ArtifactRef journalArtifact;
    ArtifactRef applyArtifact;
    std::string mode = kDefaultPlaybookMode;
    std::string generatedAtUtc;
    std::string initiatedBy;
    std::string executionCapability;
    std::string applyStatus = kExecutionJournalStatusInProgress;
    json executionPolicy = json::object();
    json replayContext = json::object();
    std::vector<std::string> filteredFindingIds;
    std::vector<ExecutionApplyEntry> entries;
    json auditTrail = json::array();
    json manifest = json::object();
};

using ExecutionApplySummary = ExecutionJournalSummary;

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
                                 std::string* errorMessage);

bool startExecutionJournal(const std::string& manifestPath,
                           const std::string& artifactId,
                           const std::string& actor,
                           const std::string& executionCapability,
                           ExecutionJournalArtifact* out,
                           bool* created,
                           std::string* errorCode,
                           std::string* errorMessage);

bool loadExecutionJournalArtifact(const std::string& manifestPath,
                                  const std::string& artifactId,
                                  ExecutionJournalArtifact* out,
                                  std::string* errorCode,
                                  std::string* errorMessage);

bool listExecutionJournals(std::size_t limit,
                           std::vector<ExecutionJournalArtifact>* out,
                           std::string* errorCode,
                           std::string* errorMessage);

bool recordExecutionJournalEvent(const std::string& manifestPath,
                                 const std::string& artifactId,
                                 const std::vector<std::string>& entryIds,
                                 const std::string& executionStatus,
                                 const std::string& actor,
                                 const std::string& comment,
                                 const std::string& failureCode,
                                 const std::string& failureMessage,
                                 ExecutionJournalArtifact* out,
                                 std::vector<std::string>* updatedEntryIds,
                                 std::string* auditEventId,
                                 std::string* errorCode,
                                 std::string* errorMessage);

bool dispatchExecutionJournalEntries(const std::string& manifestPath,
                                     const std::string& artifactId,
                                     const std::vector<std::string>& entryIds,
                                     const std::string& actor,
                                     const std::string& executionCapability,
                                     const std::string& comment,
                                     ExecutionJournalArtifact* out,
                                     std::vector<std::string>* updatedEntryIds,
                                     std::string* auditEventId,
                                     std::string* errorCode,
                                     std::string* errorMessage);

bool startExecutionApply(const std::string& manifestPath,
                         const std::string& artifactId,
                         const std::vector<std::string>& entryIds,
                         const std::string& actor,
                         const std::string& executionCapability,
                         const std::string& comment,
                         ExecutionApplyArtifact* out,
                         bool* created,
                         std::string* errorCode,
                         std::string* errorMessage);

bool loadExecutionApplyArtifact(const std::string& manifestPath,
                                const std::string& artifactId,
                                ExecutionApplyArtifact* out,
                                std::string* errorCode,
                                std::string* errorMessage);

bool listExecutionApplies(std::size_t limit,
                          std::vector<ExecutionApplyArtifact>* out,
                          std::string* errorCode,
                          std::string* errorMessage);

bool recordExecutionApplyEvent(const std::string& manifestPath,
                               const std::string& artifactId,
                               const std::vector<std::string>& entryIds,
                               const std::string& executionStatus,
                               const std::string& actor,
                               const std::string& comment,
                               const std::string& failureCode,
                               const std::string& failureMessage,
                               ExecutionApplyArtifact* out,
                               std::vector<std::string>* updatedEntryIds,
                               std::string* auditEventId,
                               std::string* errorCode,
                               std::string* errorMessage);

json artifactRefToJson(const ArtifactRef& artifact);
json findingToJson(const FindingRecord& finding);
json playbookActionToJson(const PlaybookAction& action);
json executionLedgerEntryToJson(const ExecutionLedgerEntry& entry);
ExecutionLedgerReviewSummary summarizeExecutionLedgerReviewSummary(const ExecutionLedgerArtifact& artifact);
json executionLedgerReviewSummaryToJson(const ExecutionLedgerReviewSummary& summary);
json latestExecutionLedgerAuditSummary(const ExecutionLedgerArtifact& artifact);
json executionJournalEntryToJson(const ExecutionJournalEntry& entry);
ExecutionJournalSummary summarizeExecutionJournalSummary(const ExecutionJournalArtifact& artifact);
json executionJournalSummaryToJson(const ExecutionJournalSummary& summary);
json latestExecutionJournalAuditSummary(const ExecutionJournalArtifact& artifact);
json executionApplyEntryToJson(const ExecutionApplyEntry& entry);
ExecutionApplySummary summarizeExecutionApplySummary(const ExecutionApplyArtifact& artifact);
json executionApplySummaryToJson(const ExecutionApplySummary& summary);
json latestExecutionApplyAuditSummary(const ExecutionApplyArtifact& artifact);
std::string analysisArtifactMarkdown(const AnalysisArtifact& artifact);
std::string playbookArtifactMarkdown(const PlaybookArtifact& artifact);
std::string executionLedgerArtifactMarkdown(const ExecutionLedgerArtifact& artifact);
std::string executionJournalArtifactMarkdown(const ExecutionJournalArtifact& artifact);
std::string executionApplyArtifactMarkdown(const ExecutionApplyArtifact& artifact);

} // namespace tape_phase7
