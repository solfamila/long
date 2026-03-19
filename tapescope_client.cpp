#include "tapescope_client.h"
#include "tapescope_client_internal.h"

#include <algorithm>
#include <cstdlib>

namespace tapescope {

using namespace client_internal;

namespace {
tape_engine::QueryRequest makeRequest(tape_engine::QueryOperation operation, const char* requestId) {
    return tape_engine::makeQueryRequest(operation, requestId == nullptr ? std::string() : std::string(requestId));
}

template <typename T>
QueryResult<T> packPhase7LocalResult(bool ok,
                                     T value,
                                     const std::string& errorCode,
                                     const std::string& errorMessage) {
    if (ok) {
        return makeSuccess(std::move(value));
    }

    std::string message = errorMessage;
    if (!errorCode.empty()) {
        if (!message.empty()) {
            message = errorCode + ": " + message;
        } else {
            message = errorCode;
        }
    }
    return makeError<T>(QueryErrorKind::Remote, std::move(message));
}

void applyRangeQuery(tape_engine::QueryRequest* request,
                     const RangeQuery& query,
                     bool includeLiveTail = false) {
    if (request == nullptr) {
        return;
    }
    request->fromSessionSeq = query.firstSessionSeq;
    request->toSessionSeq = query.lastSessionSeq;
    request->includeLiveTail = includeLiveTail;
}

void applyLimit(tape_engine::QueryRequest* request, std::size_t limit) {
    if (request == nullptr) {
        return;
    }
    request->limit = limit;
}

void applyOrderAnchorQuery(tape_engine::QueryRequest* request, const OrderAnchorQuery& anchorQuery) {
    if (request == nullptr) {
        return;
    }
    if (anchorQuery.traceId.has_value()) {
        request->traceId = *anchorQuery.traceId;
    }
    if (anchorQuery.orderId.has_value()) {
        request->orderId = *anchorQuery.orderId;
    }
    if (anchorQuery.permId.has_value()) {
        request->permId = *anchorQuery.permId;
    }
    if (anchorQuery.execId.has_value()) {
        request->execId = *anchorQuery.execId;
    }
}

json phase7AnalysisAppliedFilters(const Phase7AnalysisInventorySelection& selection, std::size_t matchedCount) {
    return json{
        {"source_artifact_id", selection.sourceArtifactId.empty() ? json(nullptr) : json(selection.sourceArtifactId)},
        {"analysis_profile", selection.analysisProfile.empty() ? json(nullptr) : json(selection.analysisProfile)},
        {"sort_by", selection.sortBy.empty() ? json("generated_at_desc") : json(selection.sortBy)},
        {"limit", selection.limit == 0 ? json(nullptr) : json(selection.limit)},
        {"matched_count", matchedCount}
    };
}

json phase7PlaybookAppliedFilters(const Phase7PlaybookInventorySelection& selection, std::size_t matchedCount) {
    return json{
        {"analysis_artifact_id", selection.analysisArtifactId.empty() ? json(nullptr) : json(selection.analysisArtifactId)},
        {"source_artifact_id", selection.sourceArtifactId.empty() ? json(nullptr) : json(selection.sourceArtifactId)},
        {"mode", selection.mode.empty() ? json(nullptr) : json(selection.mode)},
        {"sort_by", selection.sortBy.empty() ? json("generated_at_desc") : json(selection.sortBy)},
        {"limit", selection.limit == 0 ? json(nullptr) : json(selection.limit)},
        {"matched_count", matchedCount}
    };
}

json phase7ExecutionLedgerAppliedFilters(const Phase7ExecutionLedgerInventorySelection& selection,
                                         std::size_t matchedCount) {
    return json{
        {"playbook_artifact_id", selection.playbookArtifactId.empty() ? json(nullptr) : json(selection.playbookArtifactId)},
        {"analysis_artifact_id", selection.analysisArtifactId.empty() ? json(nullptr) : json(selection.analysisArtifactId)},
        {"source_artifact_id", selection.sourceArtifactId.empty() ? json(nullptr) : json(selection.sourceArtifactId)},
        {"ledger_status", selection.ledgerStatus.empty() ? json(nullptr) : json(selection.ledgerStatus)},
        {"sort_by", selection.sortBy.empty() ? json("generated_at_desc") : json(selection.sortBy)},
        {"limit", selection.limit == 0 ? json(nullptr) : json(selection.limit)},
        {"matched_count", matchedCount}
    };
}

json phase7ExecutionJournalAppliedFilters(const Phase7ExecutionJournalInventorySelection& selection,
                                          std::size_t matchedCount) {
    json filters = json{
        {"execution_ledger_artifact_id", selection.ledgerArtifactId.empty() ? json(nullptr) : json(selection.ledgerArtifactId)},
        {"playbook_artifact_id", selection.playbookArtifactId.empty() ? json(nullptr) : json(selection.playbookArtifactId)},
        {"analysis_artifact_id", selection.analysisArtifactId.empty() ? json(nullptr) : json(selection.analysisArtifactId)},
        {"source_artifact_id", selection.sourceArtifactId.empty() ? json(nullptr) : json(selection.sourceArtifactId)},
        {"journal_status", selection.journalStatus.empty() ? json(nullptr) : json(selection.journalStatus)},
        {"restart_recovery_state",
         selection.restartRecoveryState.empty() ? json(nullptr) : json(selection.restartRecoveryState)},
        {"restart_resume_policy", selection.restartResumePolicy.empty() ? json(nullptr) : json(selection.restartResumePolicy)},
        {"latest_execution_resolution",
         selection.latestExecutionResolution.empty() ? json(nullptr) : json(selection.latestExecutionResolution)},
        {"sort_by", selection.sortBy.empty() ? json("generated_at_desc") : json(selection.sortBy)},
        {"limit", selection.limit == 0 ? json(nullptr) : json(selection.limit)},
        {"matched_count", matchedCount}
    };
    if (!selection.recoveryState.empty()) {
        filters["recovery_state"] = selection.recoveryState;
    }
    return filters;
}

json phase7ExecutionApplyAppliedFilters(const Phase7ExecutionApplyInventorySelection& selection,
                                        std::size_t matchedCount) {
    json filters = json{
        {"execution_journal_artifact_id", selection.journalArtifactId.empty() ? json(nullptr) : json(selection.journalArtifactId)},
        {"execution_ledger_artifact_id", selection.ledgerArtifactId.empty() ? json(nullptr) : json(selection.ledgerArtifactId)},
        {"playbook_artifact_id", selection.playbookArtifactId.empty() ? json(nullptr) : json(selection.playbookArtifactId)},
        {"analysis_artifact_id", selection.analysisArtifactId.empty() ? json(nullptr) : json(selection.analysisArtifactId)},
        {"source_artifact_id", selection.sourceArtifactId.empty() ? json(nullptr) : json(selection.sourceArtifactId)},
        {"apply_status", selection.applyStatus.empty() ? json(nullptr) : json(selection.applyStatus)},
        {"restart_recovery_state",
         selection.restartRecoveryState.empty() ? json(nullptr) : json(selection.restartRecoveryState)},
        {"restart_resume_policy", selection.restartResumePolicy.empty() ? json(nullptr) : json(selection.restartResumePolicy)},
        {"latest_execution_resolution",
         selection.latestExecutionResolution.empty() ? json(nullptr) : json(selection.latestExecutionResolution)},
        {"sort_by", selection.sortBy.empty() ? json("generated_at_desc") : json(selection.sortBy)},
        {"limit", selection.limit == 0 ? json(nullptr) : json(selection.limit)},
        {"matched_count", matchedCount}
    };
    if (!selection.recoveryState.empty()) {
        filters["recovery_state"] = selection.recoveryState;
    }
    return filters;
}

template <typename RecoverySummary>
bool matchesPhase7RecoveryState(std::string_view recoveryState, const RecoverySummary& recovery) {
    if (recoveryState.empty()) {
        return true;
    }
    if (recoveryState == "recovery_required") {
        return recovery.recoveryRequired;
    }
    if (recoveryState == "stale_recovery_required") {
        return recovery.staleRecoveryRequired;
    }
    return false;
}

bool matchesLatestExecutionTriage(std::string_view restartRecoveryState,
                                  std::string_view restartResumePolicy,
                                  std::string_view latestExecutionResolution,
                                  const json& triageSummary) {
    if (!triageSummary.is_object()) {
        return restartRecoveryState.empty() && restartResumePolicy.empty() &&
               latestExecutionResolution.empty();
    }
    if (!restartRecoveryState.empty() &&
        triageSummary.value("restart_recovery_state", std::string()) != restartRecoveryState) {
        return false;
    }
    if (!restartResumePolicy.empty() &&
        triageSummary.value("restart_resume_policy", std::string()) != restartResumePolicy) {
        return false;
    }
    if (!latestExecutionResolution.empty() &&
        triageSummary.value("resolution", std::string()) != latestExecutionResolution) {
        return false;
    }
    return true;
}

void sortPhase7Analyses(std::vector<Phase7AnalysisArtifact>* artifacts, std::string_view sortBy) {
    if (artifacts == nullptr) {
        return;
    }
    const std::string mode(sortBy.empty() ? "generated_at_desc" : std::string(sortBy));
    std::sort(artifacts->begin(), artifacts->end(), [&](const Phase7AnalysisArtifact& lhs,
                                                        const Phase7AnalysisArtifact& rhs) {
        if (mode == "finding_count_desc") {
            if (lhs.findings.size() != rhs.findings.size()) {
                return lhs.findings.size() > rhs.findings.size();
            }
        } else if (mode == "analysis_profile_asc") {
            if (lhs.analysisProfile != rhs.analysisProfile) {
                return lhs.analysisProfile < rhs.analysisProfile;
            }
        } else if (mode == "source_artifact_asc") {
            if (lhs.sourceArtifact.artifactId != rhs.sourceArtifact.artifactId) {
                return lhs.sourceArtifact.artifactId < rhs.sourceArtifact.artifactId;
            }
        } else {
            if (lhs.generatedAtUtc != rhs.generatedAtUtc) {
                return lhs.generatedAtUtc > rhs.generatedAtUtc;
            }
        }
        return lhs.analysisArtifact.artifactId < rhs.analysisArtifact.artifactId;
    });
}

void sortPhase7Playbooks(std::vector<Phase7PlaybookArtifact>* artifacts,
                         const std::vector<Phase7AnalysisArtifact>& analysisArtifacts,
                         std::string_view sortBy) {
    if (artifacts == nullptr) {
        return;
    }
    auto sourceArtifactIdForPlaybook = [&](const Phase7PlaybookArtifact& artifact) {
        const auto it = std::find_if(
            analysisArtifacts.begin(),
            analysisArtifacts.end(),
            [&](const Phase7AnalysisArtifact& analysis) {
                return analysis.analysisArtifact.artifactId == artifact.analysisArtifact.artifactId;
            });
        return it == analysisArtifacts.end() ? std::string() : it->sourceArtifact.artifactId;
    };

    const std::string mode(sortBy.empty() ? "generated_at_desc" : std::string(sortBy));
    std::sort(artifacts->begin(), artifacts->end(), [&](const Phase7PlaybookArtifact& lhs,
                                                        const Phase7PlaybookArtifact& rhs) {
        if (mode == "planned_action_count_desc") {
            if (lhs.plannedActions.size() != rhs.plannedActions.size()) {
                return lhs.plannedActions.size() > rhs.plannedActions.size();
            }
        } else if (mode == "filtered_finding_count_desc") {
            if (lhs.filteredFindingIds.size() != rhs.filteredFindingIds.size()) {
                return lhs.filteredFindingIds.size() > rhs.filteredFindingIds.size();
            }
        } else if (mode == "mode_asc") {
            if (lhs.mode != rhs.mode) {
                return lhs.mode < rhs.mode;
            }
        } else if (mode == "source_artifact_asc") {
            const std::string lhsSource = sourceArtifactIdForPlaybook(lhs);
            const std::string rhsSource = sourceArtifactIdForPlaybook(rhs);
            if (lhsSource != rhsSource) {
                return lhsSource < rhsSource;
            }
        } else {
            if (lhs.generatedAtUtc != rhs.generatedAtUtc) {
                return lhs.generatedAtUtc > rhs.generatedAtUtc;
            }
        }
        return lhs.playbookArtifact.artifactId < rhs.playbookArtifact.artifactId;
    });
}

Phase7AnalysisInventoryPayload selectPhase7AnalysisArtifacts(std::vector<Phase7AnalysisArtifact> artifacts,
                                                             const Phase7AnalysisInventorySelection& selection) {
    auto matches = [&](const Phase7AnalysisArtifact& artifact) {
        if (!selection.sourceArtifactId.empty() && artifact.sourceArtifact.artifactId != selection.sourceArtifactId) {
            return false;
        }
        if (!selection.analysisProfile.empty() && artifact.analysisProfile != selection.analysisProfile) {
            return false;
        }
        return true;
    };

    std::vector<Phase7AnalysisArtifact> filtered;
    filtered.reserve(artifacts.size());
    for (auto& artifact : artifacts) {
        if (matches(artifact)) {
            filtered.push_back(std::move(artifact));
        }
    }
    sortPhase7Analyses(&filtered, selection.sortBy);

    Phase7AnalysisInventoryPayload payload;
    payload.matchedCount = filtered.size();
    const std::size_t returnedCount = selection.limit == 0 ? filtered.size() : std::min(selection.limit, filtered.size());
    payload.artifacts.assign(filtered.begin(), filtered.begin() + static_cast<std::ptrdiff_t>(returnedCount));
    payload.appliedFilters = phase7AnalysisAppliedFilters(selection, payload.matchedCount);
    return payload;
}

Phase7PlaybookInventoryPayload selectPhase7PlaybookArtifacts(std::vector<Phase7PlaybookArtifact> artifacts,
                                                             const std::vector<Phase7AnalysisArtifact>& analysisArtifacts,
                                                             const Phase7PlaybookInventorySelection& selection) {
    auto sourceArtifactIdForPlaybook = [&](const Phase7PlaybookArtifact& artifact) {
        const auto it = std::find_if(
            analysisArtifacts.begin(),
            analysisArtifacts.end(),
            [&](const Phase7AnalysisArtifact& analysis) {
                return analysis.analysisArtifact.artifactId == artifact.analysisArtifact.artifactId;
            });
        return it == analysisArtifacts.end() ? std::string() : it->sourceArtifact.artifactId;
    };

    auto matches = [&](const Phase7PlaybookArtifact& artifact) {
        if (!selection.analysisArtifactId.empty() &&
            artifact.analysisArtifact.artifactId != selection.analysisArtifactId) {
            return false;
        }
        if (!selection.mode.empty() && artifact.mode != selection.mode) {
            return false;
        }
        if (!selection.sourceArtifactId.empty() &&
            sourceArtifactIdForPlaybook(artifact) != selection.sourceArtifactId) {
            return false;
        }
        return true;
    };

    std::vector<Phase7PlaybookArtifact> filtered;
    filtered.reserve(artifacts.size());
    for (auto& artifact : artifacts) {
        if (matches(artifact)) {
            filtered.push_back(std::move(artifact));
        }
    }
    sortPhase7Playbooks(&filtered, analysisArtifacts, selection.sortBy);

    Phase7PlaybookInventoryPayload payload;
    payload.matchedCount = filtered.size();
    const std::size_t returnedCount = selection.limit == 0 ? filtered.size() : std::min(selection.limit, filtered.size());
    payload.artifacts.assign(filtered.begin(), filtered.begin() + static_cast<std::ptrdiff_t>(returnedCount));
    payload.appliedFilters = phase7PlaybookAppliedFilters(selection, payload.matchedCount);
    return payload;
}

Phase7ExecutionLedgerInventoryPayload selectPhase7ExecutionLedgers(
    std::vector<Phase7ExecutionLedgerArtifact> artifacts,
    const Phase7ExecutionLedgerInventorySelection& selection) {
    auto matches = [&](const Phase7ExecutionLedgerArtifact& artifact) {
        if (!selection.playbookArtifactId.empty() &&
            artifact.playbookArtifact.artifactId != selection.playbookArtifactId) {
            return false;
        }
        if (!selection.analysisArtifactId.empty() &&
            artifact.analysisArtifact.artifactId != selection.analysisArtifactId) {
            return false;
        }
        if (!selection.sourceArtifactId.empty() &&
            artifact.sourceArtifact.artifactId != selection.sourceArtifactId) {
            return false;
        }
        if (!selection.ledgerStatus.empty() && artifact.ledgerStatus != selection.ledgerStatus) {
            return false;
        }
        return true;
    };

    std::vector<Phase7ExecutionLedgerArtifact> filtered;
    filtered.reserve(artifacts.size());
    for (auto& artifact : artifacts) {
        if (matches(artifact)) {
            filtered.push_back(std::move(artifact));
        }
    }
    const auto statusRank = [](const std::string& ledgerStatus) {
        if (ledgerStatus == tape_phase7::kLedgerStatusBlocked) {
            return 0;
        }
        if (ledgerStatus == tape_phase7::kLedgerStatusNeedsInformation) {
            return 1;
        }
        if (ledgerStatus == tape_phase7::kLedgerStatusWaitingApproval) {
            return 2;
        }
        if (ledgerStatus == tape_phase7::kLedgerStatusReadyForExecution) {
            return 3;
        }
        if (ledgerStatus == tape_phase7::kLedgerStatusInProgress) {
            return 4;
        }
        if (ledgerStatus == tape_phase7::kDefaultLedgerStatus) {
            return 5;
        }
        if (ledgerStatus == tape_phase7::kLedgerStatusCompleted) {
            return 6;
        }
        return 7;
    };
    const std::string sortMode(selection.sortBy.empty() ? "generated_at_desc" : selection.sortBy);
    std::sort(filtered.begin(),
              filtered.end(),
              [&](const Phase7ExecutionLedgerArtifact& lhs, const Phase7ExecutionLedgerArtifact& rhs) {
                  if (sortMode == "attention_desc") {
                      const int lhsRank = statusRank(lhs.ledgerStatus);
                      const int rhsRank = statusRank(rhs.ledgerStatus);
                      if (lhsRank != rhsRank) {
                          return lhsRank < rhsRank;
                      }
                  } else if (sortMode == "source_artifact_asc") {
                      if (lhs.sourceArtifact.artifactId != rhs.sourceArtifact.artifactId) {
                          return lhs.sourceArtifact.artifactId < rhs.sourceArtifact.artifactId;
                      }
                  }
                  if (lhs.generatedAtUtc != rhs.generatedAtUtc) {
                      return lhs.generatedAtUtc > rhs.generatedAtUtc;
                  }
                  return lhs.ledgerArtifact.artifactId < rhs.ledgerArtifact.artifactId;
              });

    Phase7ExecutionLedgerInventoryPayload payload;
    payload.matchedCount = filtered.size();
    const std::size_t returnedCount = selection.limit == 0 ? filtered.size() : std::min(selection.limit, filtered.size());
    payload.artifacts.assign(filtered.begin(), filtered.begin() + static_cast<std::ptrdiff_t>(returnedCount));
    payload.appliedFilters = phase7ExecutionLedgerAppliedFilters(selection, payload.matchedCount);
    return payload;
}

Phase7ExecutionJournalInventoryPayload selectPhase7ExecutionJournals(
    std::vector<Phase7ExecutionJournalArtifact> artifacts,
    const Phase7ExecutionJournalInventorySelection& selection) {
    auto matches = [&](const Phase7ExecutionJournalArtifact& artifact) {
        if (!selection.ledgerArtifactId.empty() &&
            artifact.ledgerArtifact.artifactId != selection.ledgerArtifactId) {
            return false;
        }
        if (!selection.playbookArtifactId.empty() &&
            artifact.playbookArtifact.artifactId != selection.playbookArtifactId) {
            return false;
        }
        if (!selection.analysisArtifactId.empty() &&
            artifact.analysisArtifact.artifactId != selection.analysisArtifactId) {
            return false;
        }
        if (!selection.sourceArtifactId.empty() &&
            artifact.sourceArtifact.artifactId != selection.sourceArtifactId) {
            return false;
        }
        if (!selection.journalStatus.empty() && artifact.journalStatus != selection.journalStatus) {
            return false;
        }
        if (!matchesPhase7RecoveryState(selection.recoveryState,
                                        tape_phase7::summarizeExecutionJournalRecovery(artifact))) {
            return false;
        }
        if (!matchesLatestExecutionTriage(selection.restartRecoveryState,
                                          selection.restartResumePolicy,
                                          selection.latestExecutionResolution,
                                          tape_phase7::latestExecutionJournalTriageSummary(artifact))) {
            return false;
        }
        return true;
    };

    std::vector<Phase7ExecutionJournalArtifact> filtered;
    filtered.reserve(artifacts.size());
    for (auto& artifact : artifacts) {
        if (matches(artifact)) {
            filtered.push_back(std::move(artifact));
        }
    }
    const auto statusRank = [](const std::string& journalStatus) {
        if (journalStatus == tape_phase7::kExecutionJournalStatusFailed) {
            return 0;
        }
        if (journalStatus == tape_phase7::kExecutionJournalStatusCancelled) {
            return 1;
        }
        if (journalStatus == tape_phase7::kExecutionJournalStatusInProgress) {
            return 2;
        }
        if (journalStatus == tape_phase7::kExecutionJournalStatusQueued) {
            return 3;
        }
        if (journalStatus == tape_phase7::kExecutionJournalStatusPartiallySucceeded) {
            return 4;
        }
        if (journalStatus == tape_phase7::kExecutionJournalStatusSucceeded) {
            return 5;
        }
        return 6;
    };
    const std::string sortMode(selection.sortBy.empty() ? "generated_at_desc" : selection.sortBy);
    std::sort(filtered.begin(),
              filtered.end(),
              [&](const Phase7ExecutionJournalArtifact& lhs, const Phase7ExecutionJournalArtifact& rhs) {
                  if (sortMode == "attention_desc") {
                      const auto lhsRecovery = tape_phase7::summarizeExecutionJournalRecovery(lhs);
                      const auto rhsRecovery = tape_phase7::summarizeExecutionJournalRecovery(rhs);
                      if (lhsRecovery.staleRecoveryRequired != rhsRecovery.staleRecoveryRequired) {
                          return lhsRecovery.staleRecoveryRequired && !rhsRecovery.staleRecoveryRequired;
                      }
                      if (lhsRecovery.recoveryRequired != rhsRecovery.recoveryRequired) {
                          return lhsRecovery.recoveryRequired && !rhsRecovery.recoveryRequired;
                      }
                      const int lhsRank = statusRank(lhs.journalStatus);
                      const int rhsRank = statusRank(rhs.journalStatus);
                      if (lhsRank != rhsRank) {
                          return lhsRank < rhsRank;
                      }
                  } else if (sortMode == "source_artifact_asc") {
                      if (lhs.sourceArtifact.artifactId != rhs.sourceArtifact.artifactId) {
                          return lhs.sourceArtifact.artifactId < rhs.sourceArtifact.artifactId;
                      }
                  }
                  if (lhs.generatedAtUtc != rhs.generatedAtUtc) {
                      return lhs.generatedAtUtc > rhs.generatedAtUtc;
                  }
                  return lhs.journalArtifact.artifactId < rhs.journalArtifact.artifactId;
              });

    Phase7ExecutionJournalInventoryPayload payload;
    payload.matchedCount = filtered.size();
    const std::size_t returnedCount = selection.limit == 0 ? filtered.size() : std::min(selection.limit, filtered.size());
    payload.artifacts.assign(filtered.begin(), filtered.begin() + static_cast<std::ptrdiff_t>(returnedCount));
    payload.appliedFilters = phase7ExecutionJournalAppliedFilters(selection, payload.matchedCount);
    return payload;
}

Phase7ExecutionApplyInventoryPayload selectPhase7ExecutionApplies(
    std::vector<Phase7ExecutionApplyArtifact> artifacts,
    const Phase7ExecutionApplyInventorySelection& selection) {
    auto matches = [&](const Phase7ExecutionApplyArtifact& artifact) {
        if (!selection.journalArtifactId.empty() &&
            artifact.journalArtifact.artifactId != selection.journalArtifactId) {
            return false;
        }
        if (!selection.ledgerArtifactId.empty() &&
            artifact.ledgerArtifact.artifactId != selection.ledgerArtifactId) {
            return false;
        }
        if (!selection.playbookArtifactId.empty() &&
            artifact.playbookArtifact.artifactId != selection.playbookArtifactId) {
            return false;
        }
        if (!selection.analysisArtifactId.empty() &&
            artifact.analysisArtifact.artifactId != selection.analysisArtifactId) {
            return false;
        }
        if (!selection.sourceArtifactId.empty() &&
            artifact.sourceArtifact.artifactId != selection.sourceArtifactId) {
            return false;
        }
        if (!selection.applyStatus.empty() && artifact.applyStatus != selection.applyStatus) {
            return false;
        }
        if (!matchesPhase7RecoveryState(selection.recoveryState,
                                        tape_phase7::summarizeExecutionApplyRecovery(artifact))) {
            return false;
        }
        if (!matchesLatestExecutionTriage(selection.restartRecoveryState,
                                          selection.restartResumePolicy,
                                          selection.latestExecutionResolution,
                                          tape_phase7::latestExecutionApplyTriageSummary(artifact))) {
            return false;
        }
        return true;
    };

    std::vector<Phase7ExecutionApplyArtifact> filtered;
    filtered.reserve(artifacts.size());
    for (auto& artifact : artifacts) {
        if (matches(artifact)) {
            filtered.push_back(std::move(artifact));
        }
    }
    const auto statusRank = [](const std::string& applyStatus) {
        if (applyStatus == tape_phase7::kExecutionJournalStatusFailed) {
            return 0;
        }
        if (applyStatus == tape_phase7::kExecutionJournalStatusCancelled) {
            return 1;
        }
        if (applyStatus == tape_phase7::kExecutionJournalStatusInProgress) {
            return 2;
        }
        if (applyStatus == tape_phase7::kExecutionJournalStatusQueued) {
            return 3;
        }
        if (applyStatus == tape_phase7::kExecutionJournalStatusPartiallySucceeded) {
            return 4;
        }
        if (applyStatus == tape_phase7::kExecutionJournalStatusSucceeded) {
            return 5;
        }
        return 6;
    };
    const std::string sortMode(selection.sortBy.empty() ? "generated_at_desc" : selection.sortBy);
    std::sort(filtered.begin(),
              filtered.end(),
              [&](const Phase7ExecutionApplyArtifact& lhs, const Phase7ExecutionApplyArtifact& rhs) {
                  if (sortMode == "attention_desc") {
                      const auto lhsRecovery = tape_phase7::summarizeExecutionApplyRecovery(lhs);
                      const auto rhsRecovery = tape_phase7::summarizeExecutionApplyRecovery(rhs);
                      if (lhsRecovery.staleRecoveryRequired != rhsRecovery.staleRecoveryRequired) {
                          return lhsRecovery.staleRecoveryRequired && !rhsRecovery.staleRecoveryRequired;
                      }
                      if (lhsRecovery.recoveryRequired != rhsRecovery.recoveryRequired) {
                          return lhsRecovery.recoveryRequired && !rhsRecovery.recoveryRequired;
                      }
                      const int lhsRank = statusRank(lhs.applyStatus);
                      const int rhsRank = statusRank(rhs.applyStatus);
                      if (lhsRank != rhsRank) {
                          return lhsRank < rhsRank;
                      }
                  } else if (sortMode == "source_artifact_asc") {
                      if (lhs.sourceArtifact.artifactId != rhs.sourceArtifact.artifactId) {
                          return lhs.sourceArtifact.artifactId < rhs.sourceArtifact.artifactId;
                      }
                  }
                  if (lhs.generatedAtUtc != rhs.generatedAtUtc) {
                      return lhs.generatedAtUtc > rhs.generatedAtUtc;
                  }
                  return lhs.applyArtifact.artifactId < rhs.applyArtifact.artifactId;
              });

    Phase7ExecutionApplyInventoryPayload payload;
    payload.matchedCount = filtered.size();
    const std::size_t returnedCount = selection.limit == 0 ? filtered.size() : std::min(selection.limit, filtered.size());
    payload.artifacts.assign(filtered.begin(), filtered.begin() + static_cast<std::ptrdiff_t>(returnedCount));
    payload.appliedFilters = phase7ExecutionApplyAppliedFilters(selection, payload.matchedCount);
    return payload;
}

} // namespace

std::string defaultSocketPath() {
    const char* value = std::getenv("LONG_TAPE_ENGINE_SOCKET");
    if (value != nullptr && value[0] != '\0') {
        return std::string(value);
    }
    return std::string(kDefaultSocketPath);
}

QueryClient::QueryClient(ClientConfig config)
    : config_(std::move(config)),
      client_([&]() {
          if (config_.socketPath.empty()) {
              config_.socketPath = defaultSocketPath();
          }
          return config_.socketPath;
      }()) {}

const ClientConfig& QueryClient::config() const {
    return config_;
}

QueryError QueryClient::classifyFailure(const std::string& errorMessage) {
    QueryError error;
    error.message = errorMessage;
    if (errorMessage.rfind("connect:", 0) == 0 ||
        errorMessage.rfind("socket:", 0) == 0 ||
        errorMessage.rfind("write:", 0) == 0 ||
        errorMessage.rfind("read:", 0) == 0 ||
        errorMessage.find("socket path") != std::string::npos) {
        error.kind = QueryErrorKind::Transport;
        return error;
    }
    error.kind = QueryErrorKind::Remote;
    return error;
}

QueryResult<tape_engine::QueryResponse> QueryClient::performQuery(const tape_engine::QueryRequest& request) const {
    tape_engine::QueryResponse response;
    std::string errorMessage;
    if (!client_.query(request, &response, &errorMessage)) {
        return makeError<tape_engine::QueryResponse>(classifyFailure(errorMessage).kind, errorMessage);
    }
    return makeSuccess(std::move(response));
}

QueryResult<json> QueryClient::packSummaryAndEvents(const QueryResult<tape_engine::QueryResponse>& response) {
    if (!response.ok()) {
        return propagateError<json>(response.error);
    }

    json payload = json::object();
    payload["summary"] = response.value.summary;
    payload["events"] = response.value.events;
    return makeSuccess(std::move(payload));
}

QueryResult<StatusSnapshot> QueryClient::status() const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::Status, "tapescope-status");
    QueryResult<StatusSnapshot> result = packStatusPayload(performQuery(request));
    if (result.ok() && result.value.socketPath.empty()) {
        result.value.socketPath = config_.socketPath;
    }
    return result;
}

QueryResult<std::vector<json>> QueryClient::readLiveTail(std::size_t limit) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::ReadLiveTail, "tapescope-live-tail");
    applyLimit(&request, limit);

    const QueryResult<tape_engine::QueryResponse> response = performQuery(request);
    if (!response.ok()) {
        return propagateError<std::vector<json>>(response.error);
    }
    if (!response.value.events.is_array()) {
        return makeError<std::vector<json>>(QueryErrorKind::MalformedResponse,
                                            "read_live_tail events must be an array");
    }

    std::vector<json> events;
    events.reserve(response.value.events.size());
    for (const auto& event : response.value.events) {
        events.push_back(event);
    }
    return makeSuccess(std::move(events));
}

QueryResult<std::vector<EventRow>> QueryClient::readLiveTailRows(std::size_t limit) const {
    const auto result = readLiveTail(limit);
    if (!result.ok()) {
        return propagateError<std::vector<EventRow>>(result.error);
    }
    std::vector<EventRow> rows;
    rows.reserve(result.value.size());
    for (const auto& event : result.value) {
        rows.push_back(parseEventRow(event));
    }
    return makeSuccess(std::move(rows));
}

QueryResult<std::vector<json>> QueryClient::readRange(const RangeQuery& query) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::ReadRange, "tapescope-read-range");
    applyRangeQuery(&request, query);

    const QueryResult<tape_engine::QueryResponse> response = performQuery(request);
    if (!response.ok()) {
        return propagateError<std::vector<json>>(response.error);
    }
    if (!response.value.events.is_array()) {
        return makeError<std::vector<json>>(QueryErrorKind::MalformedResponse,
                                            "read_range events must be an array");
    }

    std::vector<json> events;
    events.reserve(response.value.events.size());
    for (const auto& event : response.value.events) {
        events.push_back(event);
    }
    return makeSuccess(std::move(events));
}

QueryResult<std::vector<EventRow>> QueryClient::readRangeRows(const RangeQuery& query) const {
    const auto result = readRange(query);
    if (!result.ok()) {
        return propagateError<std::vector<EventRow>>(result.error);
    }
    std::vector<EventRow> rows;
    rows.reserve(result.value.size());
    for (const auto& event : result.value) {
        rows.push_back(parseEventRow(event));
    }
    return makeSuccess(std::move(rows));
}

QueryResult<ReplaySnapshotPayload> QueryClient::replaySnapshotPayload(std::uint64_t targetSessionSeq,
                                                                      std::size_t depthLimit,
                                                                      bool includeLiveTail) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::ReplaySnapshot,
                                                    "tapescope-replay-snapshot");
    request.targetSessionSeq = targetSessionSeq;
    request.limit = depthLimit;
    request.includeLiveTail = includeLiveTail;
    return packReplaySnapshotPayload(performQuery(request));
}

QueryResult<json> QueryClient::readSessionQuality(const RangeQuery& query, bool includeLiveTail) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::ReadSessionQuality,
                                                    "tapescope-read-session-quality");
    applyRangeQuery(&request, query, includeLiveTail);

    return packSummaryAndEvents(performQuery(request));
}

QueryResult<SessionQualityPayload> QueryClient::readSessionQualityPayload(const RangeQuery& query,
                                                                          bool includeLiveTail) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::ReadSessionQuality,
                                                    "tapescope-read-session-quality");
    applyRangeQuery(&request, query, includeLiveTail);

    return packSessionQualityPayload(performQuery(request));
}

QueryResult<json> QueryClient::readSessionOverview(const RangeQuery& query) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::ReadSessionOverview,
                                                    "tapescope-read-session-overview");
    applyRangeQuery(&request, query);

    return packSummaryAndEvents(performQuery(request));
}

QueryResult<InvestigationPayload> QueryClient::readSessionOverviewPayload(const RangeQuery& query) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::ReadSessionOverview,
                                                    "tapescope-read-session-overview");
    applyRangeQuery(&request, query);
    return packInvestigationPayload(performQuery(request));
}

QueryResult<json> QueryClient::scanSessionReport(const RangeQuery& query) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::ScanSessionReport,
                                                    "tapescope-scan-session-report");
    applyRangeQuery(&request, query);

    return packSummaryAndEvents(performQuery(request));
}

QueryResult<InvestigationPayload> QueryClient::scanSessionReportPayload(const RangeQuery& query) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::ScanSessionReport,
                                                    "tapescope-scan-session-report");
    applyRangeQuery(&request, query);
    return packInvestigationPayload(performQuery(request));
}

QueryResult<json> QueryClient::listSessionReports(std::size_t limit) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::ListSessionReports,
                                                    "tapescope-list-session-reports");
    applyLimit(&request, limit);

    return packSummaryAndEvents(performQuery(request));
}

QueryResult<ReportInventoryPayload> QueryClient::listSessionReportsPayload(std::size_t limit) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::ListSessionReports,
                                                    "tapescope-list-session-reports");
    applyLimit(&request, limit);

    return packReportInventoryPayload(performQuery(request), true);
}

QueryResult<json> QueryClient::findOrderAnchor(const OrderAnchorQuery& anchorQuery) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::FindOrderAnchor,
                                                    "tapescope-find-order-anchor");
    applyOrderAnchorQuery(&request, anchorQuery);

    return packSummaryAndEvents(performQuery(request));
}

QueryResult<EventListPayload> QueryClient::findOrderAnchorPayload(const OrderAnchorQuery& anchorQuery) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::FindOrderAnchor,
                                                    "tapescope-find-order-anchor");
    applyOrderAnchorQuery(&request, anchorQuery);

    return packEventListPayload(performQuery(request));
}

QueryResult<json> QueryClient::listOrderAnchors(std::size_t limit) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::ListOrderAnchors,
                                                    "tapescope-list-order-anchors");
    applyLimit(&request, limit);

    return packSummaryAndEvents(performQuery(request));
}

QueryResult<CollectionRowsPayload> QueryClient::listOrderAnchorsPayload(std::size_t limit) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::ListOrderAnchors,
                                                    "tapescope-list-order-anchors");
    applyLimit(&request, limit);

    return packCollectionRowsPayload(performQuery(request), "order_anchors");
}

QueryResult<json> QueryClient::seekOrderAnchor(const OrderAnchorQuery& anchorQuery) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::SeekOrderAnchor,
                                                    "tapescope-seek-order-anchor");
    applyOrderAnchorQuery(&request, anchorQuery);

    return packSummaryAndEvents(performQuery(request));
}

QueryResult<SeekOrderPayload> QueryClient::seekOrderAnchorPayload(const OrderAnchorQuery& anchorQuery) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::SeekOrderAnchor,
                                                    "tapescope-seek-order-anchor");
    applyOrderAnchorQuery(&request, anchorQuery);

    return packSeekOrderPayload(performQuery(request));
}

QueryResult<json> QueryClient::readFinding(std::uint64_t findingId) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::ReadFinding,
                                                    "tapescope-read-finding");
    request.findingId = findingId;

    return packSummaryAndEvents(performQuery(request));
}

QueryResult<InvestigationPayload> QueryClient::readFindingPayload(std::uint64_t findingId) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::ReadFinding,
                                                    "tapescope-read-finding");
    request.findingId = findingId;
    return packInvestigationPayload(performQuery(request));
}

QueryResult<json> QueryClient::readOrderCase(const OrderAnchorQuery& anchorQuery) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::ReadOrderCase,
                                                    "tapescope-read-order-case");
    applyOrderAnchorQuery(&request, anchorQuery);

    return packSummaryAndEvents(performQuery(request));
}

QueryResult<InvestigationPayload> QueryClient::readOrderCasePayload(const OrderAnchorQuery& anchorQuery) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::ReadOrderCase,
                                                    "tapescope-read-order-case");
    applyOrderAnchorQuery(&request, anchorQuery);
    return packInvestigationPayload(performQuery(request));
}

QueryResult<InvestigationPayload> QueryClient::readTradeReviewPayload(const OrderAnchorQuery& anchorQuery) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::ReadTradeReview,
                                                    "tapescope-read-trade-review");
    applyOrderAnchorQuery(&request, anchorQuery);
    return packInvestigationPayload(performQuery(request));
}

QueryResult<json> QueryClient::scanOrderCaseReport(const OrderAnchorQuery& anchorQuery) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::ScanOrderCaseReport,
                                                    "tapescope-scan-order-case-report");
    applyOrderAnchorQuery(&request, anchorQuery);

    return packSummaryAndEvents(performQuery(request));
}

QueryResult<InvestigationPayload> QueryClient::scanOrderCaseReportPayload(const OrderAnchorQuery& anchorQuery) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::ScanOrderCaseReport,
                                                    "tapescope-scan-order-case-report");
    applyOrderAnchorQuery(&request, anchorQuery);
    return packInvestigationPayload(performQuery(request));
}

QueryResult<json> QueryClient::listCaseReports(std::size_t limit) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::ListCaseReports,
                                                    "tapescope-list-case-reports");
    applyLimit(&request, limit);

    return packSummaryAndEvents(performQuery(request));
}

QueryResult<ReportInventoryPayload> QueryClient::listCaseReportsPayload(std::size_t limit) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::ListCaseReports,
                                                    "tapescope-list-case-reports");
    applyLimit(&request, limit);

    return packReportInventoryPayload(performQuery(request), false);
}

QueryResult<json> QueryClient::listIncidents(std::size_t limit) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::ListIncidents,
                                                    "tapescope-list-incidents");
    applyLimit(&request, limit);

    return packSummaryAndEvents(performQuery(request));
}

QueryResult<IncidentListPayload> QueryClient::listIncidentsPayload(std::size_t limit) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::ListIncidents,
                                                    "tapescope-list-incidents");
    applyLimit(&request, limit);

    return packIncidentListPayload(performQuery(request));
}

QueryResult<json> QueryClient::readIncident(std::uint64_t logicalIncidentId) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::ReadIncident,
                                                    "tapescope-read-incident");
    request.logicalIncidentId = logicalIncidentId;

    return packSummaryAndEvents(performQuery(request));
}

QueryResult<InvestigationPayload> QueryClient::readIncidentPayload(std::uint64_t logicalIncidentId) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::ReadIncident,
                                                    "tapescope-read-incident");
    request.logicalIncidentId = logicalIncidentId;
    return packInvestigationPayload(performQuery(request));
}

QueryResult<EnrichmentPayload> QueryClient::enrichIncidentPayload(std::uint64_t logicalIncidentId,
                                                                  bool includeLiveTail,
                                                                  const std::string& focusQuestion) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::EnrichIncident,
                                                    "tapescope-enrich-incident");
    request.logicalIncidentId = logicalIncidentId;
    request.includeLiveTail = includeLiveTail;
    request.focusQuestion = focusQuestion;
    return packEnrichmentPayload(performQuery(request));
}

QueryResult<EnrichmentPayload> QueryClient::explainIncidentPayload(std::uint64_t logicalIncidentId,
                                                                   bool includeLiveTail,
                                                                   const std::string& focusQuestion) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::ExplainIncident,
                                                    "tapescope-explain-incident");
    request.logicalIncidentId = logicalIncidentId;
    request.includeLiveTail = includeLiveTail;
    request.focusQuestion = focusQuestion;
    return packEnrichmentPayload(performQuery(request));
}

QueryResult<EnrichmentPayload> QueryClient::enrichOrderCasePayload(const OrderAnchorQuery& query,
                                                                   const std::string& focusQuestion) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::EnrichOrderCase,
                                                    "tapescope-enrich-order-case");
    applyOrderAnchorQuery(&request, query);
    request.focusQuestion = focusQuestion;
    return packEnrichmentPayload(performQuery(request));
}

QueryResult<EnrichmentPayload> QueryClient::enrichTradeReviewPayload(const OrderAnchorQuery& query,
                                                                     const std::string& focusQuestion) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::EnrichTradeReview,
                                                    "tapescope-enrich-trade-review");
    applyOrderAnchorQuery(&request, query);
    request.focusQuestion = focusQuestion;
    return packEnrichmentPayload(performQuery(request));
}

QueryResult<EnrichmentPayload> QueryClient::refreshIncidentExternalContextPayload(
    std::uint64_t logicalIncidentId,
    bool includeLiveTail) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::RefreshExternalContext,
                                                    "tapescope-refresh-external-context");
    request.logicalIncidentId = logicalIncidentId;
    request.includeLiveTail = includeLiveTail;
    return packEnrichmentPayload(performQuery(request));
}

QueryResult<EnrichmentPayload> QueryClient::refreshOrderCaseExternalContextPayload(
    const OrderAnchorQuery& query) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::RefreshExternalContext,
                                                    "tapescope-refresh-external-context");
    applyOrderAnchorQuery(&request, query);
    return packEnrichmentPayload(performQuery(request));
}

QueryResult<EnrichmentPayload> QueryClient::refreshTradeReviewExternalContextPayload(
    const OrderAnchorQuery& query) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::RefreshTradeReviewExternalContext,
                                                    "tapescope-refresh-trade-review-external-context");
    applyOrderAnchorQuery(&request, query);
    return packEnrichmentPayload(performQuery(request));
}

QueryResult<json> QueryClient::readOrderAnchor(std::uint64_t anchorId) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::ReadOrderAnchor,
                                                    "tapescope-read-order-anchor");
    request.anchorId = anchorId;

    return packSummaryAndEvents(performQuery(request));
}

QueryResult<InvestigationPayload> QueryClient::readOrderAnchorPayload(std::uint64_t anchorId) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::ReadOrderAnchor,
                                                    "tapescope-read-order-anchor");
    request.anchorId = anchorId;
    return packInvestigationPayload(performQuery(request));
}

QueryResult<json> QueryClient::readArtifact(const std::string& artifactId) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::ReadArtifact,
                                                    "tapescope-read-artifact");
    request.artifactId = artifactId;

    return packSummaryAndEvents(performQuery(request));
}

QueryResult<InvestigationPayload> QueryClient::readArtifactPayload(const std::string& artifactId) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::ReadArtifact,
                                                    "tapescope-read-artifact");
    request.artifactId = artifactId;
    return packInvestigationPayload(performQuery(request));
}

QueryResult<json> QueryClient::exportArtifact(const std::string& artifactId, const std::string& exportFormat) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::ExportArtifact,
                                                    "tapescope-export-artifact");
    request.artifactId = artifactId;
    request.exportFormat = exportFormat;

    return packSummaryAndEvents(performQuery(request));
}

QueryResult<ArtifactExportPayload> QueryClient::exportArtifactPayload(const std::string& artifactId,
                                                                      const std::string& exportFormat) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::ExportArtifact,
                                                    "tapescope-export-artifact");
    request.artifactId = artifactId;
    request.exportFormat = exportFormat;

    return packArtifactExportPayload(performQuery(request));
}

QueryResult<BundleExportPayload> QueryClient::exportSessionBundlePayload(std::uint64_t reportId) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::ExportSessionBundle,
                                                    "tapescope-export-session-bundle");
    request.reportId = reportId;
    return packBundleExportPayload(performQuery(request));
}

QueryResult<BundleExportPayload> QueryClient::exportCaseBundlePayload(std::uint64_t reportId) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::ExportCaseBundle,
                                                    "tapescope-export-case-bundle");
    request.reportId = reportId;
    return packBundleExportPayload(performQuery(request));
}

QueryResult<BundleVerifyPayload> QueryClient::verifyBundlePayload(const std::string& bundlePath) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::VerifyBundle,
                                                    "tapescope-verify-bundle");
    request.bundlePath = bundlePath;
    return packBundleVerifyPayload(performQuery(request));
}

QueryResult<CaseBundleImportPayload> QueryClient::importCaseBundlePayload(const std::string& bundlePath) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::ImportCaseBundle,
                                                    "tapescope-import-case-bundle");
    request.bundlePath = bundlePath;
    return packCaseBundleImportPayload(performQuery(request));
}

QueryResult<ImportedCaseListPayload> QueryClient::listImportedCasesPayload(std::size_t limit) const {
    tape_engine::QueryRequest request = makeRequest(tape_engine::QueryOperation::ListImportedCases,
                                                    "tapescope-list-imported-cases");
    applyLimit(&request, limit);
    return packImportedCaseListPayload(performQuery(request));
}

QueryResult<std::vector<Phase7AnalyzerProfile>> QueryClient::listAnalysisProfilesPayload() const {
    std::vector<Phase7AnalyzerProfile> profiles;
    std::string errorCode;
    std::string errorMessage;
    return packPhase7LocalResult(tape_phase7::listAnalyzerProfiles(&profiles, &errorCode, &errorMessage),
                                 std::move(profiles),
                                 errorCode,
                                 errorMessage);
}

QueryResult<Phase7AnalyzerProfile> QueryClient::readAnalysisProfilePayload(const std::string& analysisProfile) const {
    Phase7AnalyzerProfile profile;
    std::string errorCode;
    std::string errorMessage;
    return packPhase7LocalResult(tape_phase7::loadAnalyzerProfile(analysisProfile, &profile, &errorCode, &errorMessage),
                                 std::move(profile),
                                 errorCode,
                                 errorMessage);
}

QueryResult<Phase7AnalysisRunPayload> QueryClient::runAnalysisPayload(const std::string& bundlePath,
                                                                      const std::string& analysisProfile) const {
    Phase7AnalysisRunPayload payload;
    std::string errorCode;
    std::string errorMessage;
    return packPhase7LocalResult(tape_phase7::runAnalyzerFromBundlePath(bundlePath,
                                                                        analysisProfile,
                                                                        &payload.artifact,
                                                                        &payload.created,
                                                                        &errorCode,
                                                                        &errorMessage),
                                 std::move(payload),
                                 errorCode,
                                 errorMessage);
}

QueryResult<std::vector<Phase7AnalysisArtifact>> QueryClient::listAnalysisArtifactsPayload(std::size_t limit) const {
    Phase7AnalysisInventorySelection selection;
    selection.limit = limit;
    const auto filtered = listAnalysisArtifactsPayload(selection);
    if (!filtered.ok()) {
        return propagateError<std::vector<Phase7AnalysisArtifact>>(filtered.error);
    }
    return makeSuccess(std::move(filtered.value.artifacts));
}

QueryResult<Phase7AnalysisInventoryPayload> QueryClient::listAnalysisArtifactsPayload(
    const Phase7AnalysisInventorySelection& selection) const {
    std::vector<Phase7AnalysisArtifact> artifacts;
    std::string errorCode;
    std::string errorMessage;
    return packPhase7LocalResult(tape_phase7::listAnalysisArtifacts(0, &artifacts, &errorCode, &errorMessage),
                                 selectPhase7AnalysisArtifacts(std::move(artifacts), selection),
                                 errorCode,
                                 errorMessage);
}

QueryResult<Phase7AnalysisArtifact> QueryClient::readAnalysisArtifactPayload(const std::string& artifactId) const {
    Phase7AnalysisArtifact artifact;
    std::string errorCode;
    std::string errorMessage;
    return packPhase7LocalResult(tape_phase7::loadAnalysisArtifact("", artifactId, &artifact, &errorCode, &errorMessage),
                                 std::move(artifact),
                                 errorCode,
                                 errorMessage);
}

QueryResult<Phase7PlaybookBuildPayload> QueryClient::buildPlaybookPayload(
    const std::string& analysisArtifactId,
    const std::vector<std::string>& findingIds) const {
    Phase7PlaybookBuildPayload payload;
    std::string errorCode;
    std::string errorMessage;
    return packPhase7LocalResult(tape_phase7::buildGuardedPlaybook("",
                                                                   analysisArtifactId,
                                                                   findingIds,
                                                                   tape_phase7::kDefaultPlaybookMode,
                                                                   &payload.artifact,
                                                                   &payload.created,
                                                                   &errorCode,
                                                                   &errorMessage),
                                 std::move(payload),
                                 errorCode,
                                 errorMessage);
}

QueryResult<std::vector<Phase7PlaybookArtifact>> QueryClient::listPlaybookArtifactsPayload(std::size_t limit) const {
    Phase7PlaybookInventorySelection selection;
    selection.limit = limit;
    const auto filtered = listPlaybookArtifactsPayload(selection);
    if (!filtered.ok()) {
        return propagateError<std::vector<Phase7PlaybookArtifact>>(filtered.error);
    }
    return makeSuccess(std::move(filtered.value.artifacts));
}

QueryResult<Phase7PlaybookInventoryPayload> QueryClient::listPlaybookArtifactsPayload(
    const Phase7PlaybookInventorySelection& selection) const {
    std::vector<Phase7PlaybookArtifact> artifacts;
    std::vector<Phase7AnalysisArtifact> analysisArtifacts;
    std::string errorCode;
    std::string errorMessage;
    if (!tape_phase7::listPlaybookArtifacts(0, &artifacts, &errorCode, &errorMessage)) {
        return packPhase7LocalResult(false, Phase7PlaybookInventoryPayload{}, errorCode, errorMessage);
    }
    if (!tape_phase7::listAnalysisArtifacts(0, &analysisArtifacts, &errorCode, &errorMessage)) {
        return packPhase7LocalResult(false, Phase7PlaybookInventoryPayload{}, errorCode, errorMessage);
    }
    return packPhase7LocalResult(true,
                                 selectPhase7PlaybookArtifacts(std::move(artifacts), analysisArtifacts, selection),
                                 errorCode,
                                 errorMessage);
}

QueryResult<Phase7PlaybookArtifact> QueryClient::readPlaybookArtifactPayload(const std::string& artifactId) const {
    Phase7PlaybookArtifact artifact;
    std::string errorCode;
    std::string errorMessage;
    return packPhase7LocalResult(tape_phase7::loadPlaybookArtifact("", artifactId, &artifact, &errorCode, &errorMessage),
                                 std::move(artifact),
                                 errorCode,
                                 errorMessage);
}

QueryResult<Phase7ExecutionLedgerBuildPayload> QueryClient::buildExecutionLedgerPayload(
    const std::string& playbookArtifactId) const {
    Phase7ExecutionLedgerBuildPayload payload;
    std::string errorCode;
    std::string errorMessage;
    return packPhase7LocalResult(tape_phase7::buildExecutionLedger("",
                                                                   playbookArtifactId,
                                                                   &payload.artifact,
                                                                   &payload.created,
                                                                   &errorCode,
                                                                   &errorMessage),
                                 std::move(payload),
                                 errorCode,
                                 errorMessage);
}

QueryResult<std::vector<Phase7ExecutionLedgerArtifact>> QueryClient::listExecutionLedgerArtifactsPayload(
    std::size_t limit) const {
    Phase7ExecutionLedgerInventorySelection selection;
    selection.limit = limit;
    const auto filtered = listExecutionLedgerArtifactsPayload(selection);
    if (!filtered.ok()) {
        return propagateError<std::vector<Phase7ExecutionLedgerArtifact>>(filtered.error);
    }
    return makeSuccess(std::move(filtered.value.artifacts));
}

QueryResult<Phase7ExecutionLedgerInventoryPayload> QueryClient::listExecutionLedgerArtifactsPayload(
    const Phase7ExecutionLedgerInventorySelection& selection) const {
    std::vector<Phase7ExecutionLedgerArtifact> artifacts;
    std::string errorCode;
    std::string errorMessage;
    return packPhase7LocalResult(tape_phase7::listExecutionLedgers(0, &artifacts, &errorCode, &errorMessage),
                                 selectPhase7ExecutionLedgers(std::move(artifacts), selection),
                                 errorCode,
                                 errorMessage);
}

QueryResult<Phase7ExecutionLedgerArtifact> QueryClient::readExecutionLedgerArtifactPayload(
    const std::string& artifactId) const {
    Phase7ExecutionLedgerArtifact artifact;
    std::string errorCode;
    std::string errorMessage;
    return packPhase7LocalResult(
        tape_phase7::loadExecutionLedgerArtifact("", artifactId, &artifact, &errorCode, &errorMessage),
        std::move(artifact),
        errorCode,
        errorMessage);
}

QueryResult<Phase7ExecutionLedgerReviewPayload> QueryClient::recordExecutionLedgerReviewPayload(
    const std::string& artifactId,
    const std::vector<std::string>& entryIds,
    const std::string& reviewStatus,
    const std::string& actor,
    const std::string& comment) const {
    Phase7ExecutionLedgerReviewPayload payload;
    std::string errorCode;
    std::string errorMessage;
    return packPhase7LocalResult(
        tape_phase7::recordExecutionLedgerReview("",
                                                 artifactId,
                                                 entryIds,
                                                 reviewStatus,
                                                 actor,
                                                 comment,
                                                 &payload.artifact,
                                                 &payload.updatedEntryIds,
                                                 &payload.auditEventId,
                                                 &errorCode,
                                                 &errorMessage),
        std::move(payload),
        errorCode,
        errorMessage);
}

QueryResult<Phase7ExecutionJournalStartPayload> QueryClient::startExecutionJournalPayload(
    const std::string& executionLedgerArtifactId,
    const std::string& actor,
    const std::string& executionCapability) const {
    Phase7ExecutionJournalStartPayload payload;
    std::string errorCode;
    std::string errorMessage;
    return packPhase7LocalResult(
        tape_phase7::startExecutionJournal("",
                                           executionLedgerArtifactId,
                                           actor,
                                           executionCapability,
                                           &payload.artifact,
                                           &payload.created,
                                           &errorCode,
                                           &errorMessage),
        std::move(payload),
        errorCode,
        errorMessage);
}

QueryResult<std::vector<Phase7ExecutionJournalArtifact>> QueryClient::listExecutionJournalArtifactsPayload(
    std::size_t limit) const {
    Phase7ExecutionJournalInventorySelection selection;
    selection.limit = limit;
    const auto filtered = listExecutionJournalArtifactsPayload(selection);
    if (!filtered.ok()) {
        return propagateError<std::vector<Phase7ExecutionJournalArtifact>>(filtered.error);
    }
    return makeSuccess(std::move(filtered.value.artifacts));
}

QueryResult<Phase7ExecutionJournalInventoryPayload> QueryClient::listExecutionJournalArtifactsPayload(
    const Phase7ExecutionJournalInventorySelection& selection) const {
    std::vector<Phase7ExecutionJournalArtifact> artifacts;
    std::string errorCode;
    std::string errorMessage;
    return packPhase7LocalResult(tape_phase7::listExecutionJournals(0, &artifacts, &errorCode, &errorMessage),
                                 selectPhase7ExecutionJournals(std::move(artifacts), selection),
                                 errorCode,
                                 errorMessage);
}

QueryResult<Phase7ExecutionJournalArtifact> QueryClient::readExecutionJournalArtifactPayload(
    const std::string& artifactId) const {
    Phase7ExecutionJournalArtifact artifact;
    std::string errorCode;
    std::string errorMessage;
    return packPhase7LocalResult(
        tape_phase7::loadExecutionJournalArtifact("", artifactId, &artifact, &errorCode, &errorMessage),
        std::move(artifact),
        errorCode,
        errorMessage);
}

QueryResult<Phase7ExecutionJournalDispatchPayload> QueryClient::dispatchExecutionJournalPayload(
    const std::string& artifactId,
    const std::vector<std::string>& entryIds,
    const std::string& actor,
    const std::string& executionCapability,
    const std::string& comment) const {
    Phase7ExecutionJournalDispatchPayload payload;
    std::string errorCode;
    std::string errorMessage;
    return packPhase7LocalResult(
        tape_phase7::dispatchExecutionJournalEntries("",
                                                     artifactId,
                                                     entryIds,
                                                     actor,
                                                     executionCapability,
                                                     comment,
                                                     &payload.artifact,
                                                     &payload.updatedEntryIds,
                                                     &payload.auditEventId,
                                                     &errorCode,
                                                     &errorMessage),
        std::move(payload),
        errorCode,
        errorMessage);
}

QueryResult<Phase7ExecutionJournalEventPayload> QueryClient::recordExecutionJournalEventPayload(
    const std::string& artifactId,
    const std::vector<std::string>& entryIds,
    const std::string& executionStatus,
    const std::string& actor,
    const std::string& comment,
    const std::string& failureCode,
    const std::string& failureMessage) const {
    Phase7ExecutionJournalEventPayload payload;
    std::string errorCode;
    std::string errorMessage;
    return packPhase7LocalResult(
        tape_phase7::recordExecutionJournalEvent("",
                                                 artifactId,
                                                 entryIds,
                                                 executionStatus,
                                                 actor,
                                                 comment,
                                                 failureCode,
                                                 failureMessage,
                                                 &payload.artifact,
                                                 &payload.updatedEntryIds,
                                                 &payload.auditEventId,
                                                 &errorCode,
                                                 &errorMessage),
                                 std::move(payload),
                                 errorCode,
                                 errorMessage);
}

QueryResult<Phase7ExecutionApplyStartPayload> QueryClient::startExecutionApplyPayload(
    const std::string& executionJournalArtifactId,
    const std::vector<std::string>& entryIds,
    const std::string& actor,
    const std::string& executionCapability,
    const std::string& comment) const {
    Phase7ExecutionApplyStartPayload payload;
    std::string errorCode;
    std::string errorMessage;
    return packPhase7LocalResult(
        tape_phase7::startExecutionApply("",
                                         executionJournalArtifactId,
                                         entryIds,
                                         actor,
                                         executionCapability,
                                         comment,
                                         &payload.artifact,
                                         &payload.created,
                                         &errorCode,
                                         &errorMessage),
        std::move(payload),
        errorCode,
        errorMessage);
}

QueryResult<std::vector<Phase7ExecutionApplyArtifact>> QueryClient::listExecutionApplyArtifactsPayload(
    std::size_t limit) const {
    Phase7ExecutionApplyInventorySelection selection;
    selection.limit = limit;
    const auto filtered = listExecutionApplyArtifactsPayload(selection);
    if (!filtered.ok()) {
        return propagateError<std::vector<Phase7ExecutionApplyArtifact>>(filtered.error);
    }
    return makeSuccess(std::move(filtered.value.artifacts));
}

QueryResult<Phase7ExecutionApplyInventoryPayload> QueryClient::listExecutionApplyArtifactsPayload(
    const Phase7ExecutionApplyInventorySelection& selection) const {
    std::vector<Phase7ExecutionApplyArtifact> artifacts;
    std::string errorCode;
    std::string errorMessage;
    return packPhase7LocalResult(tape_phase7::listExecutionApplies(0, &artifacts, &errorCode, &errorMessage),
                                 selectPhase7ExecutionApplies(std::move(artifacts), selection),
                                 errorCode,
                                 errorMessage);
}

QueryResult<Phase7ExecutionApplyArtifact> QueryClient::readExecutionApplyArtifactPayload(
    const std::string& artifactId) const {
    Phase7ExecutionApplyArtifact artifact;
    std::string errorCode;
    std::string errorMessage;
    return packPhase7LocalResult(
        tape_phase7::loadExecutionApplyArtifact("", artifactId, &artifact, &errorCode, &errorMessage),
        std::move(artifact),
        errorCode,
        errorMessage);
}

QueryResult<Phase7ExecutionApplyEventPayload> QueryClient::recordExecutionApplyEventPayload(
    const std::string& artifactId,
    const std::vector<std::string>& entryIds,
    const std::string& executionStatus,
    const std::string& actor,
    const std::string& comment,
    const std::string& failureCode,
    const std::string& failureMessage) const {
    Phase7ExecutionApplyEventPayload payload;
    std::string errorCode;
    std::string errorMessage;
    return packPhase7LocalResult(
        tape_phase7::recordExecutionApplyEvent("",
                                               artifactId,
                                               entryIds,
                                               executionStatus,
                                               actor,
                                               comment,
                                               failureCode,
                                               failureMessage,
                                               &payload.artifact,
                                               &payload.updatedEntryIds,
                                               &payload.auditEventId,
                                               &errorCode,
                                               &errorMessage),
        std::move(payload),
        errorCode,
        errorMessage);
}

QueryResult<Phase8WatchDefinitionCreatePayload> QueryClient::createWatchDefinitionPayload(
    const std::string& bundlePath,
    const std::string& analysisProfile,
    const std::string& title,
    bool enabled,
    std::size_t evaluationCadenceMinutes,
    std::size_t minimumFindingCount,
    const std::string& minimumSeverity,
    const std::string& requiredCategory) const {
    Phase8WatchDefinitionCreatePayload payload;
    std::string errorCode;
    std::string errorMessage;
    return packPhase7LocalResult(tape_phase8::createWatchDefinition(bundlePath,
                                                                    analysisProfile,
                                                                    title,
                                                                    enabled,
                                                                    evaluationCadenceMinutes,
                                                                    minimumFindingCount,
                                                                    minimumSeverity,
                                                                    requiredCategory,
                                                                    &payload.artifact,
                                                                    &payload.created,
                                                                    &errorCode,
                                                                    &errorMessage),
                                 std::move(payload),
                                 errorCode,
                                 errorMessage);
}

QueryResult<std::vector<Phase8WatchDefinitionArtifact>> QueryClient::listWatchDefinitionsPayload(std::size_t limit) const {
    std::vector<Phase8WatchDefinitionArtifact> artifacts;
    std::string errorCode;
    std::string errorMessage;
    return packPhase7LocalResult(tape_phase8::listWatchDefinitions(limit, &artifacts, &errorCode, &errorMessage),
                                 std::move(artifacts),
                                 errorCode,
                                 errorMessage);
}

QueryResult<Phase8DueWatchInventoryPayload> QueryClient::listDueWatchesPayload(std::size_t limit) const {
    Phase8DueWatchInventoryPayload payload;
    std::string errorCode;
    std::string errorMessage;
    return packPhase7LocalResult(tape_phase8::listDueWatchDefinitions(limit,
                                                                      &payload,
                                                                      &errorCode,
                                                                      &errorMessage),
                                 std::move(payload),
                                 errorCode,
                                 errorMessage);
}

QueryResult<Phase8WatchDefinitionArtifact> QueryClient::readWatchDefinitionPayload(const std::string& artifactId) const {
    Phase8WatchDefinitionArtifact artifact;
    std::string errorCode;
    std::string errorMessage;
    return packPhase7LocalResult(tape_phase8::loadWatchDefinition({}, artifactId, &artifact, &errorCode, &errorMessage),
                                 std::move(artifact),
                                 errorCode,
                                 errorMessage);
}

QueryResult<Phase8TriggerRunPayload> QueryClient::evaluateWatchDefinitionPayload(const std::string& artifactId,
                                                                                 const std::string& triggerReason) const {
    Phase8TriggerRunPayload payload;
    std::string errorCode;
    std::string errorMessage;
    return packPhase7LocalResult(tape_phase8::evaluateWatchDefinition({},
                                                                      artifactId,
                                                                      triggerReason,
                                                                      &payload.artifact,
                                                                      &payload.created,
                                                                      &errorCode,
                                                                      &errorMessage),
                                 std::move(payload),
                                 errorCode,
                                 errorMessage);
}

QueryResult<Phase8DueWatchRunPayload> QueryClient::runDueWatchesPayload(std::size_t limit,
                                                                        const std::string& triggerReason) const {
    Phase8DueWatchRunPayload payload;
    std::string errorCode;
    std::string errorMessage;
    return packPhase7LocalResult(tape_phase8::runDueWatchDefinitions(limit,
                                                                     triggerReason,
                                                                     &payload,
                                                                     &errorCode,
                                                                     &errorMessage),
                                 std::move(payload),
                                 errorCode,
                                 errorMessage);
}

QueryResult<Phase8TriggerRunInventoryPayload> QueryClient::listTriggerRunsPayload(
    const Phase8TriggerRunInventorySelection& selection) const {
    tape_phase8::TriggerRunInventorySelection localSelection;
    localSelection.limit = selection.limit;
    localSelection.watchArtifactId = selection.watchArtifactId;
    localSelection.attentionStatus = selection.attentionStatus;
    localSelection.attentionOpen = selection.attentionOpen;

    tape_phase8::TriggerRunInventoryResult inventory;
    std::string errorCode;
    std::string errorMessage;
    return packPhase7LocalResult(tape_phase8::listTriggerRuns(localSelection, &inventory, &errorCode, &errorMessage),
                                 Phase8TriggerRunInventoryPayload{
                                     .artifacts = std::move(inventory.triggerRuns),
                                     .appliedFilters = {
                                         {"watch_artifact_id", inventory.appliedFilters.watchArtifactId.empty()
                                                                   ? json(nullptr)
                                                                   : json(inventory.appliedFilters.watchArtifactId)},
                                         {"attention_status", inventory.appliedFilters.attentionStatus.empty()
                                                                  ? json(nullptr)
                                                                  : json(inventory.appliedFilters.attentionStatus)},
                                         {"attention_open", inventory.appliedFilters.attentionOpen.has_value()
                                                                ? json(*inventory.appliedFilters.attentionOpen)
                                                                : json(nullptr)},
                                         {"limit", inventory.appliedFilters.limit},
                                         {"matched_count", inventory.matchedCount}
                                     },
                                     .matchedCount = inventory.matchedCount
                                 },
                                 errorCode,
                                 errorMessage);
}

QueryResult<Phase8TriggerRunArtifact> QueryClient::readTriggerRunPayload(const std::string& artifactId) const {
    Phase8TriggerRunArtifact artifact;
    std::string errorCode;
    std::string errorMessage;
    return packPhase7LocalResult(tape_phase8::loadTriggerRunArtifact({}, artifactId, &artifact, &errorCode, &errorMessage),
                                 std::move(artifact),
                                 errorCode,
                                 errorMessage);
}

QueryResult<Phase8TriggerRunArtifact> QueryClient::acknowledgeAttentionItemPayload(const std::string& triggerArtifactId,
                                                                                   const std::string& actor,
                                                                                   const std::string& comment) const {
    Phase8TriggerRunArtifact artifact;
    std::string errorCode;
    std::string errorMessage;
    return packPhase7LocalResult(tape_phase8::recordAttentionAction({},
                                                                    triggerArtifactId,
                                                                    "acknowledge",
                                                                    0,
                                                                    actor,
                                                                    comment,
                                                                    &artifact,
                                                                    &errorCode,
                                                                    &errorMessage),
                                 std::move(artifact),
                                 errorCode,
                                 errorMessage);
}

QueryResult<Phase8TriggerRunArtifact> QueryClient::snoozeAttentionItemPayload(const std::string& triggerArtifactId,
                                                                              const std::size_t snoozeMinutes,
                                                                              const std::string& actor,
                                                                              const std::string& comment) const {
    Phase8TriggerRunArtifact artifact;
    std::string errorCode;
    std::string errorMessage;
    return packPhase7LocalResult(tape_phase8::recordAttentionAction({},
                                                                    triggerArtifactId,
                                                                    "snooze",
                                                                    snoozeMinutes,
                                                                    actor,
                                                                    comment,
                                                                    &artifact,
                                                                    &errorCode,
                                                                    &errorMessage),
                                 std::move(artifact),
                                 errorCode,
                                 errorMessage);
}

QueryResult<Phase8TriggerRunArtifact> QueryClient::resolveAttentionItemPayload(const std::string& triggerArtifactId,
                                                                               const std::string& actor,
                                                                               const std::string& comment) const {
    Phase8TriggerRunArtifact artifact;
    std::string errorCode;
    std::string errorMessage;
    return packPhase7LocalResult(tape_phase8::recordAttentionAction({},
                                                                    triggerArtifactId,
                                                                    "resolve",
                                                                    0,
                                                                    actor,
                                                                    comment,
                                                                    &artifact,
                                                                    &errorCode,
                                                                    &errorMessage),
                                 std::move(artifact),
                                 errorCode,
                                 errorMessage);
}

QueryResult<std::vector<Phase8AttentionInboxItem>> QueryClient::listAttentionInboxPayload(std::size_t limit) const {
    std::vector<Phase8AttentionInboxItem> items;
    std::string errorCode;
    std::string errorMessage;
    return packPhase7LocalResult(tape_phase8::listAttentionInbox(limit, &items, &errorCode, &errorMessage),
                                 std::move(items),
                                 errorCode,
                                 errorMessage);
}

std::string QueryClient::describeError(const QueryError& error) {
    switch (error.kind) {
        case QueryErrorKind::None:
            return "No error";
        case QueryErrorKind::Transport:
            return "Engine unavailable: " + error.message;
        case QueryErrorKind::Remote:
            return "Engine query failed: " + error.message;
        case QueryErrorKind::MalformedResponse:
            return "Engine returned malformed data: " + error.message;
    }
    return error.message;
}

} // namespace tapescope
