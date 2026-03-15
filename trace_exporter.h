#pragma once

#include "app_shared.h"

struct TraceExportBundle {
    std::string baseName;
    std::string reportText;
    std::string summaryCsv;
    std::string fillsCsv;
    std::string timelineCsv;
};

struct Phase6ReportOutputArtifact {
    std::string artifactType = "phase6.report_output.v1";
    std::string contractVersion = "phase6-case-report-v1";
    std::string artifactId;
    std::string artifactRootDir;
    std::string manifestPath;
    std::string reportPath;
    std::string summaryPath;
    std::string fillsPath;
    std::string timelinePath;
};

struct Phase6CaseBundleArtifact {
    std::string artifactType = "phase6.case_bundle.v1";
    std::string contractVersion = "phase6-case-report-v1";
    std::string artifactId;
    std::string artifactRootDir;
    std::string manifestPath;
    Phase6ReportOutputArtifact reportOutput;
    std::string bridgeRecordsPath;
};

bool replayTradeTraceSnapshotFromLog(std::uint64_t traceId,
                                     TradeTraceSnapshot* outSnapshot,
                                     std::string* error = nullptr,
                                     const std::string& logPath = {});
bool replayTradeTraceSnapshotByIdentityFromLog(OrderId orderId,
                                               long long permId,
                                               const std::string& execId,
                                               TradeTraceSnapshot* outSnapshot,
                                               std::string* error = nullptr,
                                               const std::string& logPath = {});
bool enrichTradeTraceSnapshotFromLog(TradeTraceSnapshot* snapshot,
                                     std::string* error = nullptr,
                                     const std::string& logPath = {});
std::vector<TradeTraceListItem> buildTradeTraceListItemsFromLog(std::size_t maxItems = 100,
                                                                const std::string& logPath = {});
bool buildTraceExportBundle(std::uint64_t traceId, TraceExportBundle* outBundle, std::string* error = nullptr);
std::string buildAllTradesSummaryCsv(std::size_t maxItems = 1000);
bool generatePhase6ReportOutputArtifact(std::uint64_t traceId,
                                        const std::string& artifactRootDirectory,
                                        Phase6ReportOutputArtifact* outArtifact,
                                        std::string* error = nullptr);
bool generatePhase6CaseBundleArtifact(std::uint64_t traceId,
                                      const std::string& artifactRootDirectory,
                                      Phase6CaseBundleArtifact* outArtifact,
                                      std::string* error = nullptr);
