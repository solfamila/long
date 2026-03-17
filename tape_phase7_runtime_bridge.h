#pragma once

#include "tape_phase7_artifacts.h"
#include "trading_runtime.h"

#include <string>
#include <vector>

namespace tape_phase7 {

struct RuntimeBridgeSweepResult {
    std::size_t scannedJournalCount = 0;
    std::size_t updatedJournalCount = 0;
    std::size_t updatedApplyCount = 0;
    std::vector<std::string> updatedJournalArtifactIds;
    std::vector<std::string> updatedApplyArtifactIds;
    std::vector<std::string> updatedJournalEntryIds;
    std::vector<std::string> updatedApplyEntryIds;
};

struct RuntimeRecoveryBacklogSummary {
    std::size_t journalArtifactCount = 0;
    std::size_t applyArtifactCount = 0;
    std::size_t recoveryRequiredJournalCount = 0;
    std::size_t staleRecoveryRequiredJournalCount = 0;
    std::size_t recoveryRequiredApplyCount = 0;
    std::size_t staleRecoveryRequiredApplyCount = 0;
    std::size_t runtimeBackedSubmittedJournalEntryCount = 0;
    std::size_t staleRuntimeBackedJournalEntryCount = 0;
    std::size_t runtimeBackedSubmittedApplyEntryCount = 0;
    std::size_t staleRuntimeBackedApplyEntryCount = 0;
    bool recoveryRequired = false;
    bool staleRecoveryRequired = false;
};

struct RuntimeStartupRecoveryPlan {
    RuntimeRecoveryBacklogSummary backlog;
    bool actorPresent = false;
    bool recoverySweepRecommended = false;
    bool manualAttentionRecommended = false;
    std::string startupAction = "none";
    std::string detail;
};

struct RuntimeSmokeReport {
    RuntimeRecoveryBacklogSummary backlog;
    RuntimeStartupRecoveryPlan startupPlan;
    bool actorPresent = false;
    bool runtimeStarted = false;
    bool brokerConnected = false;
    bool sessionReady = false;
    RuntimeConnectionConfig connection;
    std::string activeSymbol;
    std::size_t orderCount = 0;
    std::size_t reconcilingOrderCount = 0;
    std::size_t manualReviewOrderCount = 0;
    std::size_t bridgeOutboxCount = 0;
    std::size_t bridgeOutboxLossCount = 0;
};

RuntimeStartupRecoveryPlan runtimeStartupRecoveryPlan(const RuntimeRecoveryBacklogSummary& backlog,
                                                     bool actorPresent);

bool captureRuntimeSmokeReport(TradingRuntime* runtime,
                               bool actorPresent,
                               RuntimeSmokeReport* out,
                               std::string* errorCode,
                               std::string* errorMessage);

json runtimeSmokeReportToJson(const RuntimeSmokeReport& report);

bool summarizeRuntimeRecoveryBacklog(RuntimeRecoveryBacklogSummary* out,
                                     std::string* errorCode,
                                     std::string* errorMessage);

bool planRuntimeRecoveryStartup(bool actorPresent,
                                RuntimeStartupRecoveryPlan* out,
                                std::string* errorCode,
                                std::string* errorMessage);

bool dispatchExecutionJournalEntriesViaRuntime(TradingRuntime* runtime,
                                               const std::string& manifestPath,
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

bool reconcileExecutionJournalEntriesViaRuntime(TradingRuntime* runtime,
                                                const std::string& manifestPath,
                                                const std::string& artifactId,
                                                const std::vector<std::string>& entryIds,
                                                const std::string& actor,
                                                const std::string& comment,
                                                ExecutionJournalArtifact* out,
                                                std::vector<std::string>* updatedEntryIds,
                                                std::string* auditEventId,
                                                std::string* errorCode,
                                                std::string* errorMessage);

bool synchronizeExecutionApplyFromJournal(const std::string& manifestPath,
                                          const std::string& artifactId,
                                          const std::string& actor,
                                          const std::string& comment,
                                          ExecutionApplyArtifact* out,
                                          std::vector<std::string>* updatedEntryIds,
                                          std::string* auditEventId,
                                          std::string* errorCode,
                                          std::string* errorMessage);

bool reconcileExecutionArtifactsViaRuntime(TradingRuntime* runtime,
                                           const std::string& actor,
                                           const std::string& comment,
                                           RuntimeBridgeSweepResult* out,
                                           std::string* errorCode,
                                           std::string* errorMessage);

} // namespace tape_phase7
