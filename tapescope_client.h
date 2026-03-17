#pragma once

#include "tape_phase7_artifacts.h"
#include "tape_phase8_artifacts.h"
#include "tape_engine_client.h"
#include "tape_query_payloads.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

namespace tapescope {

using json = nlohmann::json;

inline constexpr const char* kDefaultSocketPath = "/tmp/tape-engine.sock";

using QueryErrorKind = tape_payloads::QueryErrorKind;
using QueryError = tape_payloads::QueryError;
template <typename T>
using QueryResult = tape_payloads::QueryResult<T>;
using StatusSnapshot = tape_payloads::StatusSnapshot;
using RangeQuery = tape_payloads::RangeQuery;
using EvidenceCitation = tape_payloads::EvidenceCitation;
using EventRow = tape_payloads::EventRow;
using EventListPayload = tape_payloads::EventListPayload;
using IncidentListRow = tape_payloads::IncidentListRow;
using InvestigationPayload = tape_payloads::InvestigationPayload;
using EnrichmentPayload = tape_payloads::EnrichmentPayload;
using SessionQualityPayload = tape_payloads::SessionQualityPayload;
using SeekOrderPayload = tape_payloads::SeekOrderPayload;
using IncidentListPayload = tape_payloads::IncidentListPayload;
using ReportInventoryRow = tape_payloads::ReportInventoryRow;
using ReportInventoryPayload = tape_payloads::ReportInventoryPayload;
using BundleExportPayload = tape_payloads::BundleExportPayload;
using BundleVerifyPayload = tape_payloads::BundleVerifyPayload;
using ImportedCaseRow = tape_payloads::ImportedCaseRow;
using ImportedCaseListPayload = tape_payloads::ImportedCaseListPayload;
using CaseBundleImportPayload = tape_payloads::CaseBundleImportPayload;
using ArtifactExportPayload = tape_payloads::ArtifactExportPayload;
using Phase7ArtifactRef = tape_phase7::ArtifactRef;
using Phase7FindingRecord = tape_phase7::FindingRecord;
using Phase7AnalyzerProfile = tape_phase7::AnalyzerProfileSpec;
using Phase7AnalysisArtifact = tape_phase7::AnalysisArtifact;
using Phase7PlaybookAction = tape_phase7::PlaybookAction;
using Phase7PlaybookArtifact = tape_phase7::PlaybookArtifact;
using Phase7ExecutionLedgerEntry = tape_phase7::ExecutionLedgerEntry;
using Phase7ExecutionLedgerArtifact = tape_phase7::ExecutionLedgerArtifact;
using Phase7ExecutionJournalEntry = tape_phase7::ExecutionJournalEntry;
using Phase7ExecutionJournalArtifact = tape_phase7::ExecutionJournalArtifact;
using Phase7ExecutionApplyEntry = tape_phase7::ExecutionApplyEntry;
using Phase7ExecutionApplyArtifact = tape_phase7::ExecutionApplyArtifact;
using Phase8WatchDefinitionArtifact = tape_phase8::WatchDefinitionArtifact;
using Phase8TriggerRunArtifact = tape_phase8::TriggerRunArtifact;
using Phase8AttentionInboxItem = tape_phase8::AttentionInboxItem;

struct Phase7AnalysisRunPayload {
    Phase7AnalysisArtifact artifact;
    bool created = false;
};

struct Phase7PlaybookBuildPayload {
    Phase7PlaybookArtifact artifact;
    bool created = false;
};

struct Phase7ExecutionLedgerBuildPayload {
    Phase7ExecutionLedgerArtifact artifact;
    bool created = false;
};

struct Phase7ExecutionLedgerReviewPayload {
    Phase7ExecutionLedgerArtifact artifact;
    std::vector<std::string> updatedEntryIds;
    std::string auditEventId;
};

struct Phase7ExecutionJournalStartPayload {
    Phase7ExecutionJournalArtifact artifact;
    bool created = false;
};

struct Phase7ExecutionJournalEventPayload {
    Phase7ExecutionJournalArtifact artifact;
    std::vector<std::string> updatedEntryIds;
    std::string auditEventId;
};

using Phase7ExecutionJournalDispatchPayload = Phase7ExecutionJournalEventPayload;

struct Phase7ExecutionApplyStartPayload {
    Phase7ExecutionApplyArtifact artifact;
    bool created = false;
};

struct Phase7ExecutionApplyEventPayload {
    Phase7ExecutionApplyArtifact artifact;
    std::vector<std::string> updatedEntryIds;
    std::string auditEventId;
};

struct Phase8WatchDefinitionCreatePayload {
    Phase8WatchDefinitionArtifact artifact;
    bool created = false;
};

struct Phase8TriggerRunPayload {
    Phase8TriggerRunArtifact artifact;
    bool created = false;
};

using Phase8DueWatchInventoryPayload = tape_phase8::DueWatchInventoryResult;
using Phase8DueWatchRunPayload = tape_phase8::DueWatchRunResult;

struct Phase7ExecutionApplyInventorySelection {
    std::string journalArtifactId;
    std::string ledgerArtifactId;
    std::string playbookArtifactId;
    std::string analysisArtifactId;
    std::string sourceArtifactId;
    std::string applyStatus;
    std::string recoveryState;
    std::string restartRecoveryState;
    std::string restartResumePolicy;
    std::string latestExecutionResolution;
    std::string sortBy = "generated_at_desc";
    std::size_t limit = 20;
};

struct Phase7ExecutionApplyInventoryPayload {
    std::vector<Phase7ExecutionApplyArtifact> artifacts;
    json appliedFilters = json::object();
    std::size_t matchedCount = 0;
};

struct Phase7AnalysisInventorySelection {
    std::string sourceArtifactId;
    std::string analysisProfile;
    std::string sortBy = "generated_at_desc";
    std::size_t limit = 20;
};

struct Phase7AnalysisInventoryPayload {
    std::vector<Phase7AnalysisArtifact> artifacts;
    json appliedFilters = json::object();
    std::size_t matchedCount = 0;
};

struct Phase7PlaybookInventorySelection {
    std::string analysisArtifactId;
    std::string sourceArtifactId;
    std::string mode;
    std::string sortBy = "generated_at_desc";
    std::size_t limit = 20;
};

struct Phase7PlaybookInventoryPayload {
    std::vector<Phase7PlaybookArtifact> artifacts;
    json appliedFilters = json::object();
    std::size_t matchedCount = 0;
};

struct Phase7ExecutionLedgerInventorySelection {
    std::string playbookArtifactId;
    std::string analysisArtifactId;
    std::string sourceArtifactId;
    std::string ledgerStatus;
    std::string sortBy = "generated_at_desc";
    std::size_t limit = 20;
};

struct Phase7ExecutionLedgerInventoryPayload {
    std::vector<Phase7ExecutionLedgerArtifact> artifacts;
    json appliedFilters = json::object();
    std::size_t matchedCount = 0;
};

struct Phase7ExecutionJournalInventorySelection {
    std::string ledgerArtifactId;
    std::string playbookArtifactId;
    std::string analysisArtifactId;
    std::string sourceArtifactId;
    std::string journalStatus;
    std::string recoveryState;
    std::string restartRecoveryState;
    std::string restartResumePolicy;
    std::string latestExecutionResolution;
    std::string sortBy = "generated_at_desc";
    std::size_t limit = 20;
};

struct Phase7ExecutionJournalInventoryPayload {
    std::vector<Phase7ExecutionJournalArtifact> artifacts;
    json appliedFilters = json::object();
    std::size_t matchedCount = 0;
};

struct ClientConfig {
    std::string socketPath;
};

struct OrderAnchorQuery {
    std::optional<std::uint64_t> traceId;
    std::optional<long long> orderId;
    std::optional<long long> permId;
    std::optional<std::string> execId;
};

std::string defaultSocketPath();

class QueryClient {
public:
    explicit QueryClient(ClientConfig config = {});

    [[nodiscard]] const ClientConfig& config() const;

    [[nodiscard]] QueryResult<StatusSnapshot> status() const;
    [[nodiscard]] QueryResult<std::vector<json>> readLiveTail(std::size_t limit = 64) const;
    [[nodiscard]] QueryResult<std::vector<EventRow>> readLiveTailRows(std::size_t limit = 64) const;
    [[nodiscard]] QueryResult<std::vector<json>> readRange(const RangeQuery& query) const;
    [[nodiscard]] QueryResult<std::vector<EventRow>> readRangeRows(const RangeQuery& query) const;
    // Legacy JSON envelopes are retained for compatibility, but the UI and tests
    // should prefer the typed payload variants below.
    [[nodiscard]] [[deprecated("Use readSessionQualityPayload() for typed TapeScope quality reads")]]
    QueryResult<json> readSessionQuality(const RangeQuery& query, bool includeLiveTail = false) const;
    [[nodiscard]] QueryResult<SessionQualityPayload> readSessionQualityPayload(const RangeQuery& query,
                                                                               bool includeLiveTail = false) const;
    [[nodiscard]] [[deprecated("Use readSessionOverviewPayload() for typed TapeScope investigation reads")]]
    QueryResult<json> readSessionOverview(const RangeQuery& query) const;
    [[nodiscard]] QueryResult<InvestigationPayload> readSessionOverviewPayload(const RangeQuery& query) const;
    [[nodiscard]] [[deprecated("Use scanSessionReportPayload() for typed TapeScope report reads")]]
    QueryResult<json> scanSessionReport(const RangeQuery& query) const;
    [[nodiscard]] QueryResult<InvestigationPayload> scanSessionReportPayload(const RangeQuery& query) const;
    [[nodiscard]] [[deprecated("Use listSessionReportsPayload() for typed TapeScope report inventory reads")]]
    QueryResult<json> listSessionReports(std::size_t limit = 20) const;
    [[nodiscard]] QueryResult<ReportInventoryPayload> listSessionReportsPayload(std::size_t limit = 20) const;
    [[nodiscard]] [[deprecated("Use findOrderAnchorPayload() for typed TapeScope order-anchor reads")]]
    QueryResult<json> findOrderAnchor(const OrderAnchorQuery& query) const;
    [[nodiscard]] QueryResult<EventListPayload> findOrderAnchorPayload(const OrderAnchorQuery& query) const;
    [[nodiscard]] [[deprecated("Use seekOrderAnchorPayload() for typed TapeScope replay-target reads")]]
    QueryResult<json> seekOrderAnchor(const OrderAnchorQuery& query) const;
    [[nodiscard]] QueryResult<SeekOrderPayload> seekOrderAnchorPayload(const OrderAnchorQuery& query) const;
    [[nodiscard]] [[deprecated("Use readFindingPayload() for typed TapeScope investigation reads")]]
    QueryResult<json> readFinding(std::uint64_t findingId) const;
    [[nodiscard]] QueryResult<InvestigationPayload> readFindingPayload(std::uint64_t findingId) const;
    [[nodiscard]] [[deprecated("Use readOrderCasePayload() for typed TapeScope investigation reads")]]
    QueryResult<json> readOrderCase(const OrderAnchorQuery& query) const;
    [[nodiscard]] QueryResult<InvestigationPayload> readOrderCasePayload(const OrderAnchorQuery& query) const;
    [[nodiscard]] [[deprecated("Use scanOrderCaseReportPayload() for typed TapeScope report reads")]]
    QueryResult<json> scanOrderCaseReport(const OrderAnchorQuery& query) const;
    [[nodiscard]] QueryResult<InvestigationPayload> scanOrderCaseReportPayload(const OrderAnchorQuery& query) const;
    [[nodiscard]] [[deprecated("Use listCaseReportsPayload() for typed TapeScope report inventory reads")]]
    QueryResult<json> listCaseReports(std::size_t limit = 20) const;
    [[nodiscard]] QueryResult<ReportInventoryPayload> listCaseReportsPayload(std::size_t limit = 20) const;
    [[nodiscard]] [[deprecated("Use listIncidentsPayload() for typed TapeScope incident reads")]]
    QueryResult<json> listIncidents(std::size_t limit = 20) const;
    [[nodiscard]] QueryResult<IncidentListPayload> listIncidentsPayload(std::size_t limit = 20) const;
    [[nodiscard]] [[deprecated("Use readIncidentPayload() for typed TapeScope investigation reads")]]
    QueryResult<json> readIncident(std::uint64_t logicalIncidentId) const;
    [[nodiscard]] QueryResult<InvestigationPayload> readIncidentPayload(std::uint64_t logicalIncidentId) const;
    [[nodiscard]] QueryResult<EnrichmentPayload> enrichIncidentPayload(std::uint64_t logicalIncidentId,
                                                                       bool includeLiveTail = false) const;
    [[nodiscard]] QueryResult<EnrichmentPayload> explainIncidentPayload(std::uint64_t logicalIncidentId,
                                                                        bool includeLiveTail = false) const;
    [[nodiscard]] QueryResult<EnrichmentPayload> enrichOrderCasePayload(const OrderAnchorQuery& query) const;
    [[nodiscard]] QueryResult<EnrichmentPayload> refreshIncidentExternalContextPayload(
        std::uint64_t logicalIncidentId,
        bool includeLiveTail = false) const;
    [[nodiscard]] QueryResult<EnrichmentPayload> refreshOrderCaseExternalContextPayload(
        const OrderAnchorQuery& query) const;
    [[nodiscard]] [[deprecated("Use readOrderAnchorPayload() for typed TapeScope investigation reads")]]
    QueryResult<json> readOrderAnchor(std::uint64_t anchorId) const;
    [[nodiscard]] QueryResult<InvestigationPayload> readOrderAnchorPayload(std::uint64_t anchorId) const;
    [[nodiscard]] [[deprecated("Use readArtifactPayload() for typed TapeScope investigation reads")]]
    QueryResult<json> readArtifact(const std::string& artifactId) const;
    [[nodiscard]] QueryResult<InvestigationPayload> readArtifactPayload(const std::string& artifactId) const;
    [[nodiscard]] [[deprecated("Use exportArtifactPayload() for typed TapeScope artifact exports")]]
    QueryResult<json> exportArtifact(const std::string& artifactId, const std::string& exportFormat) const;
    [[nodiscard]] QueryResult<ArtifactExportPayload> exportArtifactPayload(const std::string& artifactId,
                                                                           const std::string& exportFormat) const;
    [[nodiscard]] QueryResult<BundleExportPayload> exportSessionBundlePayload(std::uint64_t reportId) const;
    [[nodiscard]] QueryResult<BundleExportPayload> exportCaseBundlePayload(std::uint64_t reportId) const;
    [[nodiscard]] QueryResult<BundleVerifyPayload> verifyBundlePayload(const std::string& bundlePath) const;
    [[nodiscard]] QueryResult<CaseBundleImportPayload> importCaseBundlePayload(const std::string& bundlePath) const;
    [[nodiscard]] QueryResult<ImportedCaseListPayload> listImportedCasesPayload(std::size_t limit = 20) const;
    [[nodiscard]] QueryResult<std::vector<Phase7AnalyzerProfile>> listAnalysisProfilesPayload() const;
    [[nodiscard]] QueryResult<Phase7AnalyzerProfile> readAnalysisProfilePayload(const std::string& analysisProfile) const;
    [[nodiscard]] QueryResult<Phase7AnalysisRunPayload> runAnalysisPayload(const std::string& bundlePath,
                                                                           const std::string& analysisProfile) const;
    [[nodiscard]] QueryResult<std::vector<Phase7AnalysisArtifact>> listAnalysisArtifactsPayload(std::size_t limit = 20) const;
    [[nodiscard]] QueryResult<Phase7AnalysisInventoryPayload> listAnalysisArtifactsPayload(
        const Phase7AnalysisInventorySelection& selection) const;
    [[nodiscard]] QueryResult<Phase7AnalysisArtifact> readAnalysisArtifactPayload(const std::string& artifactId) const;
    [[nodiscard]] QueryResult<Phase7PlaybookBuildPayload> buildPlaybookPayload(
        const std::string& analysisArtifactId,
        const std::vector<std::string>& findingIds = {}) const;
    [[nodiscard]] QueryResult<std::vector<Phase7PlaybookArtifact>> listPlaybookArtifactsPayload(std::size_t limit = 20) const;
    [[nodiscard]] QueryResult<Phase7PlaybookInventoryPayload> listPlaybookArtifactsPayload(
        const Phase7PlaybookInventorySelection& selection) const;
    [[nodiscard]] QueryResult<Phase7PlaybookArtifact> readPlaybookArtifactPayload(const std::string& artifactId) const;
    [[nodiscard]] QueryResult<Phase7ExecutionLedgerBuildPayload> buildExecutionLedgerPayload(
        const std::string& playbookArtifactId) const;
    [[nodiscard]] QueryResult<std::vector<Phase7ExecutionLedgerArtifact>> listExecutionLedgerArtifactsPayload(
        std::size_t limit = 20) const;
    [[nodiscard]] QueryResult<Phase7ExecutionLedgerInventoryPayload> listExecutionLedgerArtifactsPayload(
        const Phase7ExecutionLedgerInventorySelection& selection) const;
    [[nodiscard]] QueryResult<Phase7ExecutionLedgerArtifact> readExecutionLedgerArtifactPayload(
        const std::string& artifactId) const;
    [[nodiscard]] QueryResult<Phase7ExecutionLedgerReviewPayload> recordExecutionLedgerReviewPayload(
        const std::string& artifactId,
        const std::vector<std::string>& entryIds,
        const std::string& reviewStatus,
        const std::string& actor,
        const std::string& comment) const;
    [[nodiscard]] QueryResult<Phase7ExecutionJournalStartPayload> startExecutionJournalPayload(
        const std::string& executionLedgerArtifactId,
        const std::string& actor,
        const std::string& executionCapability) const;
    [[nodiscard]] QueryResult<std::vector<Phase7ExecutionJournalArtifact>> listExecutionJournalArtifactsPayload(
        std::size_t limit = 20) const;
    [[nodiscard]] QueryResult<Phase7ExecutionJournalInventoryPayload> listExecutionJournalArtifactsPayload(
        const Phase7ExecutionJournalInventorySelection& selection) const;
    [[nodiscard]] QueryResult<Phase7ExecutionJournalArtifact> readExecutionJournalArtifactPayload(
        const std::string& artifactId) const;
    [[nodiscard]] QueryResult<Phase7ExecutionJournalDispatchPayload> dispatchExecutionJournalPayload(
        const std::string& artifactId,
        const std::vector<std::string>& entryIds,
        const std::string& actor,
        const std::string& executionCapability,
        const std::string& comment = {}) const;
    [[nodiscard]] QueryResult<Phase7ExecutionJournalEventPayload> recordExecutionJournalEventPayload(
        const std::string& artifactId,
        const std::vector<std::string>& entryIds,
        const std::string& executionStatus,
        const std::string& actor,
        const std::string& comment,
        const std::string& failureCode = {},
        const std::string& failureMessage = {}) const;
    [[nodiscard]] QueryResult<Phase7ExecutionApplyStartPayload> startExecutionApplyPayload(
        const std::string& executionJournalArtifactId,
        const std::vector<std::string>& entryIds,
        const std::string& actor,
        const std::string& executionCapability,
        const std::string& comment = {}) const;
    [[nodiscard]] QueryResult<std::vector<Phase7ExecutionApplyArtifact>> listExecutionApplyArtifactsPayload(
        std::size_t limit = 20) const;
    [[nodiscard]] QueryResult<Phase7ExecutionApplyInventoryPayload> listExecutionApplyArtifactsPayload(
        const Phase7ExecutionApplyInventorySelection& selection) const;
    [[nodiscard]] QueryResult<Phase7ExecutionApplyArtifact> readExecutionApplyArtifactPayload(
        const std::string& artifactId) const;
    [[nodiscard]] QueryResult<Phase7ExecutionApplyEventPayload> recordExecutionApplyEventPayload(
        const std::string& artifactId,
        const std::vector<std::string>& entryIds,
        const std::string& executionStatus,
        const std::string& actor,
        const std::string& comment,
        const std::string& failureCode = {},
        const std::string& failureMessage = {}) const;
    [[nodiscard]] QueryResult<Phase8WatchDefinitionCreatePayload> createWatchDefinitionPayload(
        const std::string& bundlePath,
        const std::string& analysisProfile,
        const std::string& title = {},
        bool enabled = true,
        std::size_t evaluationCadenceMinutes = tape_phase8::kDefaultEvaluationCadenceMinutes,
        std::size_t minimumFindingCount = 1,
        const std::string& minimumSeverity = {},
        const std::string& requiredCategory = {}) const;
    [[nodiscard]] QueryResult<std::vector<Phase8WatchDefinitionArtifact>> listWatchDefinitionsPayload(
        std::size_t limit = 20) const;
    [[nodiscard]] QueryResult<Phase8DueWatchInventoryPayload> listDueWatchesPayload(
        std::size_t limit = 20) const;
    [[nodiscard]] QueryResult<Phase8WatchDefinitionArtifact> readWatchDefinitionPayload(
        const std::string& artifactId) const;
    [[nodiscard]] QueryResult<Phase8TriggerRunPayload> evaluateWatchDefinitionPayload(
        const std::string& artifactId,
        const std::string& triggerReason = {}) const;
    [[nodiscard]] QueryResult<Phase8DueWatchRunPayload> runDueWatchesPayload(
        std::size_t limit = 20,
        const std::string& triggerReason = {}) const;
    [[nodiscard]] QueryResult<std::vector<Phase8TriggerRunArtifact>> listTriggerRunsPayload(
        std::size_t limit = 20) const;
    [[nodiscard]] QueryResult<Phase8TriggerRunArtifact> readTriggerRunPayload(
        const std::string& artifactId) const;
    [[nodiscard]] QueryResult<Phase8TriggerRunArtifact> acknowledgeAttentionItemPayload(
        const std::string& triggerArtifactId,
        const std::string& actor = {},
        const std::string& comment = {}) const;
    [[nodiscard]] QueryResult<Phase8TriggerRunArtifact> snoozeAttentionItemPayload(
        const std::string& triggerArtifactId,
        std::size_t snoozeMinutes = tape_phase8::kDefaultAttentionSnoozeMinutes,
        const std::string& actor = {},
        const std::string& comment = {}) const;
    [[nodiscard]] QueryResult<Phase8TriggerRunArtifact> resolveAttentionItemPayload(
        const std::string& triggerArtifactId,
        const std::string& actor = {},
        const std::string& comment = {}) const;
    [[nodiscard]] QueryResult<std::vector<Phase8AttentionInboxItem>> listAttentionInboxPayload(
        std::size_t limit = 20) const;

    [[nodiscard]] static std::string describeError(const QueryError& error);

private:
    [[nodiscard]] QueryResult<tape_engine::QueryResponse> performQuery(const tape_engine::QueryRequest& request) const;
    [[nodiscard]] static QueryError classifyFailure(const std::string& errorMessage);
    [[nodiscard]] static QueryResult<json> packSummaryAndEvents(const QueryResult<tape_engine::QueryResponse>& response);

    ClientConfig config_;
    tape_engine::Client client_;
};

} // namespace tapescope
