#pragma once

#include "tape_engine_protocol.h"
#include "tape_mcp_engine_rpc.h"

#include <string>
#include <string_view>
#include <vector>

#include <nlohmann/json.hpp>

namespace tape_mcp {

using json = nlohmann::json;

struct AdapterConfig {
    std::string engineSocketPath;
};

struct ParsedAdapterArgs {
    AdapterConfig config;
    bool showHelp = false;
    std::string error;
};

enum class ToolId {
    Status = 0,
    ReadLiveTail,
    ReadRange,
    FindOrderAnchor,
    ListIncidents,
    ListOrderAnchors,
    ListProtectedWindows,
    ListFindings,
    ReadSessionOverview,
    ScanSessionReport,
    ReadSessionReport,
    ListSessionReports,
    ScanIncidentReport,
    ScanOrderCaseReport,
    ReadCaseReport,
    ListCaseReports,
    SeekOrderAnchor,
    ReadFinding,
    ReadOrderCase,
    ReadOrderAnchor,
    ReadProtectedWindow,
    ReplaySnapshot,
    ReadIncident,
    ReadArtifact,
    ExportArtifact,
    ExportSessionBundle,
    ExportCaseBundle,
    VerifyBundle,
    ImportCaseBundle,
    ListImportedCases,
    ReadSessionQuality,
    ListAnalysisProfiles,
    ReadAnalysisProfile,
    AnalyzerRun,
    FindingsList,
    ListAnalysisArtifacts,
    ReadAnalysisArtifact,
    PlaybookApply,
    ListPlaybookArtifacts,
    ReadPlaybookArtifact,
    PrepareExecutionLedger,
    ListExecutionLedgers,
    ReadExecutionLedger,
    RecordExecutionLedgerReview
};

struct ToolSpec {
    ToolId id = ToolId::Status;
    std::string name;
    std::string description;
    json inputSchema;
    json outputSchema;
    std::string outputSchemaId;
    tape_engine::QueryOperation engineOperation = tape_engine::QueryOperation::Unknown;
    bool progressEligible = false;
    std::string contractVersion;
    std::string engineCommand;
};

ParsedAdapterArgs parseAdapterArgs(int argc, char** argv);
std::string adapterUsage(std::string_view executableName);

class Adapter {
public:
    explicit Adapter(AdapterConfig config);

    [[nodiscard]] json initializeResult() const;
    [[nodiscard]] json listToolsResult() const;
    [[nodiscard]] json listPromptsResult() const;
    [[nodiscard]] json getPromptResult(const std::string& promptName, const json& args) const;
    [[nodiscard]] json listResourcesResult() const;
    [[nodiscard]] json readResourceResult(const std::string& resourceUri) const;
    [[nodiscard]] bool isProgressEligibleTool(std::string_view toolName) const;
    [[nodiscard]] json callTool(const std::string& toolName, const json& args) const;

private:
    [[nodiscard]] const ToolSpec* findTool(std::string_view toolName) const;
    [[nodiscard]] json invokeTool(const ToolSpec& tool, const json& args) const;

    [[nodiscard]] json invokeStatusTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokeReadLiveTailTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokeReadRangeTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokeFindOrderAnchorTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokeListIncidentsTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokeListOrderAnchorsTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokeListProtectedWindowsTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokeListFindingsTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokeReadSessionOverviewTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokeScanSessionReportTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokeReadSessionReportTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokeListSessionReportsTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokeScanIncidentReportTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokeScanOrderCaseReportTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokeReadCaseReportTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokeListCaseReportsTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokeSeekOrderAnchorTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokeReadFindingTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokeReadOrderCaseTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokeReadOrderAnchorTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokeReadProtectedWindowTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokeReplaySnapshotTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokeReadIncidentTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokeReadArtifactTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokeExportArtifactTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokeExportSessionBundleTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokeExportCaseBundleTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokeVerifyBundleTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokeImportCaseBundleTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokeListImportedCasesTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokeReadSessionQualityTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokeListAnalysisProfilesTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokeReadAnalysisProfileTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokeAnalyzerRunTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokeFindingsListTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokeListAnalysisArtifactsTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokeReadAnalysisArtifactTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokePlaybookApplyTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokeListPlaybookArtifactsTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokeReadPlaybookArtifactTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokePrepareExecutionLedgerTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokeListExecutionLedgersTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokeReadExecutionLedgerTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokeRecordExecutionLedgerReviewTool(const ToolSpec& tool, const json& args) const;

    [[nodiscard]] json makeToolResult(const json& envelope) const;
    [[nodiscard]] json makeSuccessEnvelope(const ToolSpec& tool, json result, json revision) const;
    [[nodiscard]] json makeErrorEnvelope(const std::string& toolName,
                                         std::string_view engineOperation,
                                         std::string_view outputSchemaId,
                                         bool supported,
                                         bool deferred,
                                         const std::string& errorCode,
                                         const std::string& errorMessage,
                                         bool retryable,
                                         json revision,
                                         std::string contractVersion = {}) const;

    std::vector<ToolSpec> tools_;
    EngineRpcClient engineRpc_;
};

} // namespace tape_mcp
