#include "tape_mcp_adapter.h"

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <initializer_list>
#include <limits>
#include <optional>
#include <string_view>
#include <utility>

#include "trace_exporter.h"

namespace tape_mcp {

namespace {

namespace fs = std::filesystem;

constexpr const char* kLegacyContractVersion = "phase5-mcp-compat-v1";
constexpr const char* kPhase7ContractVersion = "phase7-analyzer-playbook-v1";
constexpr const char* kPhase7AnalysisArtifactType = "phase7.analysis_output.v1";
constexpr const char* kPhase7DefaultAnalyzerPassId = "phase7.trace_fill_integrity.v1";
constexpr const char* kServerVersion = "0.1.0";
constexpr std::uint64_t kDefaultReadLiveTailLimit = 64;

std::string envValueOrEmpty(const char* key) {
    const char* value = std::getenv(key);
    if (value == nullptr || value[0] == '\0') {
        return {};
    }
    return std::string(value);
}

json revisionUnavailable() {
    return json{
        {"manifest_hash", nullptr},
        {"latest_session_seq", nullptr},
        {"first_session_seq", nullptr},
        {"last_session_seq", nullptr},
        {"source", "unavailable"},
        {"staleness", "unknown"}
    };
}

json emptyObjectSchema() {
    return json{
        {"type", "object"},
        {"properties", json::object()},
        {"additionalProperties", false}
    };
}

std::optional<std::uint64_t> asUint64(const json& value);
std::optional<long long> asInt64(const json& value);
std::optional<std::string> asNonEmptyString(const json& value);

json emptyPhase6Anchor() {
    return json{
        {"trace_id", nullptr},
        {"order_id", nullptr},
        {"perm_id", nullptr},
        {"exec_id", nullptr}
    };
}

void applyTraceAnchor(const TradeTrace& trace, json* outAnchor) {
    if (outAnchor == nullptr) {
        return;
    }
    (*outAnchor)["trace_id"] = trace.traceId;
    (*outAnchor)["order_id"] = static_cast<long long>(trace.orderId);
    (*outAnchor)["perm_id"] = trace.permId;
    (*outAnchor)["exec_id"] = nullptr;
    for (const auto& fill : trace.fills) {
        if (!fill.execId.empty()) {
            (*outAnchor)["exec_id"] = fill.execId;
            break;
        }
    }
}

json loadJsonFileOrNull(const std::string& pathText) {
    if (pathText.empty()) {
        return nullptr;
    }
    std::ifstream in(pathText, std::ios::binary);
    if (!in.is_open()) {
        return nullptr;
    }
    json parsed = json::parse(in, nullptr, false);
    if (parsed.is_discarded()) {
        return nullptr;
    }
    return parsed;
}

bool readJsonFile(const fs::path& path, json* outJson, std::string* error) {
    if (outJson == nullptr) {
        if (error != nullptr) {
            *error = "Missing output json container.";
        }
        return false;
    }

    std::ifstream in(path, std::ios::binary);
    if (!in.is_open()) {
        if (error != nullptr) {
            *error = "Failed to open json file: " + path.string();
        }
        return false;
    }

    const json parsed = json::parse(in, nullptr, false);
    if (parsed.is_discarded()) {
        if (error != nullptr) {
            *error = "Failed to parse json file: " + path.string();
        }
        return false;
    }

    *outJson = parsed;
    return true;
}

json analysisArtifactResult(const fs::path& manifestPath, const json& manifest) {
    return json{
        {"artifact_type", manifest.value("artifact_type", std::string())},
        {"contract_version", manifest.value("contract_version", std::string())},
        {"artifact_id", manifest.value("artifact_id", std::string())},
        {"manifest_path", manifestPath.string()},
        {"artifact_root_dir", manifestPath.parent_path().string()}
    };
}

std::string mapAnalyzerFailureCode(const std::string& errorMessage) {
    if (errorMessage.find("Unsupported source contract version:") != std::string::npos ||
        errorMessage.find("Unsupported source artifact_type:") != std::string::npos ||
        errorMessage.find("Case bundle references unsupported report_output manifest") != std::string::npos) {
        return "unsupported_source_contract";
    }
    if (errorMessage.find("Failed to open") != std::string::npos ||
        errorMessage.find("No such file") != std::string::npos) {
        return "artifact_not_found";
    }
    if (errorMessage.find("Failed to parse json file:") != std::string::npos) {
        return "artifact_load_failed";
    }
    return "analysis_failed";
}

json reportArtifactResult(const Phase6ReportOutputArtifact& artifact) {
    return json{
        {"artifact_type", artifact.artifactType},
        {"contract_version", artifact.contractVersion},
        {"artifact_id", artifact.artifactId},
        {"artifact_root_dir", artifact.artifactRootDir},
        {"manifest_path", artifact.manifestPath},
        {"report_path", artifact.reportPath},
        {"summary_path", artifact.summaryPath},
        {"fills_path", artifact.fillsPath},
        {"timeline_path", artifact.timelinePath}
    };
}

json caseArtifactResult(const Phase6CaseBundleArtifact& artifact) {
    return json{
        {"artifact_type", artifact.artifactType},
        {"contract_version", artifact.contractVersion},
        {"artifact_id", artifact.artifactId},
        {"artifact_root_dir", artifact.artifactRootDir},
        {"manifest_path", artifact.manifestPath},
        {"bridge_records_path", artifact.bridgeRecordsPath.empty() ? json(nullptr) : json(artifact.bridgeRecordsPath)},
        {"report_output", reportArtifactResult(artifact.reportOutput)}
    };
}

struct ResolvedPhase6Anchor {
    std::uint64_t traceId = 0;
    json requestedAnchor = emptyPhase6Anchor();
    json resolvedAnchor = emptyPhase6Anchor();
};

std::optional<ResolvedPhase6Anchor> resolvePhase6AnchorFromArgs(const json& args,
                                                                std::string* errorCode,
                                                                std::string* errorMessage) {
    auto fail = [&](std::string code, std::string message) -> std::optional<ResolvedPhase6Anchor> {
        if (errorCode != nullptr) {
            *errorCode = std::move(code);
        }
        if (errorMessage != nullptr) {
            *errorMessage = std::move(message);
        }
        return std::nullopt;
    };

    if (!args.is_object()) {
        return fail("invalid_arguments", "Phase 6 tool arguments must be an object.");
    }

    ResolvedPhase6Anchor anchor;
    std::optional<std::uint64_t> traceId;
    std::optional<long long> orderId;
    std::optional<long long> permId;
    std::optional<std::string> execId;
    int provided = 0;

    if (args.contains("trace_id")) {
        traceId = asUint64(args.at("trace_id"));
        if (!traceId.has_value() || *traceId == 0) {
            return fail("invalid_arguments", "trace_id must be a positive integer.");
        }
        anchor.requestedAnchor["trace_id"] = *traceId;
        ++provided;
    }
    if (args.contains("order_id")) {
        orderId = asInt64(args.at("order_id"));
        if (!orderId.has_value() || *orderId <= 0) {
            return fail("invalid_arguments", "order_id must be a positive integer.");
        }
        anchor.requestedAnchor["order_id"] = *orderId;
        ++provided;
    }
    if (args.contains("perm_id")) {
        permId = asInt64(args.at("perm_id"));
        if (!permId.has_value() || *permId <= 0) {
            return fail("invalid_arguments", "perm_id must be a positive integer.");
        }
        anchor.requestedAnchor["perm_id"] = *permId;
        ++provided;
    }
    if (args.contains("exec_id")) {
        execId = asNonEmptyString(args.at("exec_id"));
        if (!execId.has_value()) {
            return fail("invalid_arguments", "exec_id must be a non-empty string.");
        }
        anchor.requestedAnchor["exec_id"] = *execId;
        ++provided;
    }

    if (provided != 1) {
        return fail("invalid_arguments",
                    "Exactly one of trace_id, order_id, perm_id, or exec_id is required.");
    }

    if (traceId.has_value()) {
        anchor.traceId = *traceId;
        TradeTraceSnapshot snapshot;
        if (replayTradeTraceSnapshotFromLog(*traceId, &snapshot, nullptr)) {
            applyTraceAnchor(snapshot.trace, &anchor.resolvedAnchor);
        } else {
            anchor.resolvedAnchor = anchor.requestedAnchor;
        }
        return anchor;
    }

    TradeTraceSnapshot snapshot;
    std::string replayError;
    const bool resolved = replayTradeTraceSnapshotByIdentityFromLog(
        static_cast<OrderId>(orderId.value_or(0)),
        permId.value_or(0),
        execId.value_or(std::string()),
        &snapshot,
        &replayError);
    if (!resolved || !snapshot.found || snapshot.trace.traceId == 0) {
        return fail("trace_not_found",
                    replayError.empty()
                        ? "No matching trace found for the provided anchor."
                        : replayError);
    }

    anchor.traceId = snapshot.trace.traceId;
    applyTraceAnchor(snapshot.trace, &anchor.resolvedAnchor);
    return anchor;
}

std::optional<std::uint64_t> asUint64(const json& value) {
    if (value.is_number_unsigned()) {
        return value.get<std::uint64_t>();
    }
    if (value.is_number_integer()) {
        const auto signedValue = value.get<long long>();
        if (signedValue < 0) {
            return std::nullopt;
        }
        return static_cast<std::uint64_t>(signedValue);
    }
    return std::nullopt;
}

std::optional<long long> asInt64(const json& value) {
    if (value.is_number_unsigned()) {
        const auto unsignedValue = value.get<std::uint64_t>();
        if (unsignedValue > static_cast<std::uint64_t>(std::numeric_limits<long long>::max())) {
            return std::nullopt;
        }
        return static_cast<long long>(unsignedValue);
    }
    if (value.is_number_integer()) {
        return value.get<long long>();
    }
    return std::nullopt;
}

std::optional<std::string> asNonEmptyString(const json& value) {
    if (!value.is_string()) {
        return std::nullopt;
    }
    const std::string text = value.get<std::string>();
    if (text.empty()) {
        return std::nullopt;
    }
    return text;
}

std::optional<std::uint64_t> firstPresentUint64(const json& payload,
                                                std::initializer_list<const char*> keys) {
    if (!payload.is_object()) {
        return std::nullopt;
    }
    for (const char* key : keys) {
        const auto it = payload.find(key);
        if (it == payload.end()) {
            continue;
        }
        const std::optional<std::uint64_t> parsed = asUint64(*it);
        if (parsed.has_value()) {
            return parsed;
        }
    }
    return std::nullopt;
}

std::optional<long long> firstPresentInt64(const json& payload,
                                           std::initializer_list<const char*> keys) {
    if (!payload.is_object()) {
        return std::nullopt;
    }
    for (const char* key : keys) {
        const auto it = payload.find(key);
        if (it == payload.end()) {
            continue;
        }
        const std::optional<long long> parsed = asInt64(*it);
        if (parsed.has_value()) {
            return parsed;
        }
    }
    return std::nullopt;
}

std::optional<std::string> firstPresentString(const json& payload,
                                              std::initializer_list<const char*> keys) {
    if (!payload.is_object()) {
        return std::nullopt;
    }
    for (const char* key : keys) {
        const auto it = payload.find(key);
        if (it == payload.end()) {
            continue;
        }
        const std::optional<std::string> parsed = asNonEmptyString(*it);
        if (parsed.has_value()) {
            return parsed;
        }
    }
    return std::nullopt;
}

std::optional<std::uint64_t> latestSessionSeqFromStatus(const json& payload) {
    const std::optional<std::uint64_t> explicitLatest =
        firstPresentUint64(payload, {"latest_session_seq", "last_session_seq"});
    if (explicitLatest.has_value()) {
        return explicitLatest;
    }

    const std::optional<std::uint64_t> nextSession = firstPresentUint64(payload, {"next_session_seq"});
    if (!nextSession.has_value()) {
        return std::nullopt;
    }
    if (*nextSession == 0) {
        return 0;
    }
    return *nextSession - 1;
}

std::optional<std::vector<json>> extractEventArray(const json& payload) {
    if (payload.is_array()) {
        std::vector<json> events;
        events.reserve(payload.size());
        for (const auto& value : payload) {
            events.push_back(value);
        }
        return events;
    }

    if (!payload.is_object()) {
        return std::nullopt;
    }

    for (const char* key : {"events", "items", "records", "matches"}) {
        const auto it = payload.find(key);
        if (it == payload.end() || !it->is_array()) {
            continue;
        }

        std::vector<json> events;
        events.reserve(it->size());
        for (const auto& value : *it) {
            events.push_back(value);
        }
        return events;
    }

    return std::nullopt;
}

std::optional<std::uint64_t> sessionSeqFromEvent(const json& event) {
    if (!event.is_object()) {
        return std::nullopt;
    }
    return firstPresentUint64(event, {"session_seq", "sessionSeq"});
}

struct SessionBounds {
    std::uint64_t first = 0;
    std::uint64_t last = 0;
};

std::optional<SessionBounds> deriveSessionBounds(const std::vector<json>& events) {
    std::optional<SessionBounds> bounds;
    for (const json& event : events) {
        const std::optional<std::uint64_t> sessionSeq = sessionSeqFromEvent(event);
        if (!sessionSeq.has_value()) {
            continue;
        }

        if (!bounds.has_value()) {
            bounds = SessionBounds{*sessionSeq, *sessionSeq};
            continue;
        }

        bounds->first = std::min(bounds->first, *sessionSeq);
        bounds->last = std::max(bounds->last, *sessionSeq);
    }
    return bounds;
}

json makeReadRevision(std::string_view staleness) {
    json revision = revisionUnavailable();
    revision["staleness"] = staleness;
    return revision;
}

json extractAnchorContext(const json& payload) {
    json anchor = {
        {"trace_id", nullptr},
        {"order_id", nullptr},
        {"perm_id", nullptr},
        {"exec_id", nullptr}
    };

    if (!payload.is_object()) {
        return anchor;
    }

    std::vector<const json*> candidates;
    candidates.push_back(&payload);

    const auto anchorIt = payload.find("anchor");
    if (anchorIt != payload.end() && anchorIt->is_object()) {
        candidates.push_back(&(*anchorIt));
    }

    for (const json* candidate : candidates) {
        if (anchor["trace_id"].is_null()) {
            const auto traceId = firstPresentUint64(*candidate, {"trace_id", "traceId"});
            if (traceId.has_value()) {
                anchor["trace_id"] = *traceId;
            }
        }
        if (anchor["order_id"].is_null()) {
            const auto orderId = firstPresentInt64(*candidate, {"order_id", "orderId"});
            if (orderId.has_value()) {
                anchor["order_id"] = *orderId;
            }
        }
        if (anchor["perm_id"].is_null()) {
            const auto permId = firstPresentInt64(*candidate, {"perm_id", "permId"});
            if (permId.has_value()) {
                anchor["perm_id"] = *permId;
            }
        }
        if (anchor["exec_id"].is_null()) {
            const auto execId = firstPresentString(*candidate, {"exec_id", "execId"});
            if (execId.has_value()) {
                anchor["exec_id"] = *execId;
            }
        }
    }

    return anchor;
}

bool hasUnexpectedKeys(const json& args, std::initializer_list<const char*> allowedKeys) {
    if (!args.is_object()) {
        return true;
    }

    for (const auto& [key, _] : args.items()) {
        bool allowed = false;
        for (const char* allowedKey : allowedKeys) {
            if (key == allowedKey) {
                allowed = true;
                break;
            }
        }
        if (!allowed) {
            return true;
        }
    }

    return false;
}

std::vector<ToolSpec> buildToolSpecs() {
    std::vector<ToolSpec> specs;
    specs.push_back(ToolSpec{
        "tapescript_status",
        "Return current engine status over the stable status seam.",
        emptyObjectSchema(),
        "status",
        kLegacyContractVersion,
        true,
        false
    });
    specs.push_back(ToolSpec{
        "tapescript_read_live_tail",
        "Return recent live events over the stable read_live_tail seam.",
        json{
            {"type", "object"},
            {"properties", json{
                {"limit", json{{"type", "integer"}, {"minimum", 1}}}
            }},
            {"additionalProperties", false}
        },
        "read_live_tail",
        kLegacyContractVersion,
        true,
        false
    });
    specs.push_back(ToolSpec{
        "tapescript_read_range",
        "Return replay events over the stable read_range seam.",
        json{
            {"type", "object"},
            {"properties", json{
                {"first_session_seq", json{{"type", "integer"}, {"minimum", 1}}},
                {"last_session_seq", json{{"type", "integer"}, {"minimum", 1}}}
            }},
            {"required", json::array({"first_session_seq", "last_session_seq"})},
            {"additionalProperties", false}
        },
        "read_range",
        kLegacyContractVersion,
        true,
        false
    });
    specs.push_back(ToolSpec{
        "tapescript_find_order_anchor",
        "Find anchor payloads over the stable find_order_anchor seam.",
        json{
            {"type", "object"},
            {"properties", json{
                {"trace_id", json{{"type", "integer"}}},
                {"order_id", json{{"type", "integer"}}},
                {"perm_id", json{{"type", "integer"}}},
                {"exec_id", json{{"type", "string"}}}
            }},
            {"oneOf", json::array({
                json{{"required", json::array({"trace_id"})}},
                json{{"required", json::array({"order_id"})}},
                json{{"required", json::array({"perm_id"})}},
                json{{"required", json::array({"exec_id"})}}
            })},
            {"additionalProperties", false}
        },
        "find_order_anchor",
        kLegacyContractVersion,
        true,
        false
    });
    specs.push_back(ToolSpec{
        "tapescript_report_generate",
        "Generate a Phase 6 report output artifact for a resolved trace anchor.",
        json{
            {"type", "object"},
            {"properties", json{
                {"trace_id", json{{"type", "integer"}, {"minimum", 1}}},
                {"order_id", json{{"type", "integer"}, {"minimum", 1}}},
                {"perm_id", json{{"type", "integer"}, {"minimum", 1}}},
                {"exec_id", json{{"type", "string"}, {"minLength", 1}}}
            }},
            {"oneOf", json::array({
                json{{"required", json::array({"trace_id"})}},
                json{{"required", json::array({"order_id"})}},
                json{{"required", json::array({"perm_id"})}},
                json{{"required", json::array({"exec_id"})}}
            })},
            {"additionalProperties", false}
        },
        "phase6_report_generate_local",
        kLegacyContractVersion,
        true,
        false
    });
    specs.push_back(ToolSpec{
        "tapescript_export_range",
        "Generate a Phase 6 case bundle artifact for a resolved trace anchor.",
        json{
            {"type", "object"},
            {"properties", json{
                {"trace_id", json{{"type", "integer"}, {"minimum", 1}}},
                {"order_id", json{{"type", "integer"}, {"minimum", 1}}},
                {"perm_id", json{{"type", "integer"}, {"minimum", 1}}},
                {"exec_id", json{{"type", "string"}, {"minLength", 1}}},
                {"first_session_seq", json{{"type", "integer"}, {"minimum", 1}}},
                {"last_session_seq", json{{"type", "integer"}, {"minimum", 1}}}
            }},
            {"oneOf", json::array({
                json{{"required", json::array({"trace_id"})}},
                json{{"required", json::array({"order_id"})}},
                json{{"required", json::array({"perm_id"})}},
                json{{"required", json::array({"exec_id"})}}
            })},
            {"additionalProperties", false}
        },
        "phase6_export_range_local",
        kLegacyContractVersion,
        true,
        false
    });
    specs.push_back(ToolSpec{
        "tapescript_analyzer_run",
        "Run a local Phase 7 analyzer pass over a Phase 6 source manifest.",
        json{
            {"type", "object"},
            {"properties", json{
                {"case_manifest_path", json{{"type", "string"}, {"minLength", 1}}},
                {"report_manifest_path", json{{"type", "string"}, {"minLength", 1}}},
                {"analysis_profile", json{{"type", "string"}, {"minLength", 1}}}
            }},
            {"oneOf", json::array({
                json{{"required", json::array({"case_manifest_path"})}},
                json{{"required", json::array({"report_manifest_path"})}}
            })},
            {"additionalProperties", false}
        },
        "phase7_analyzer_run_local",
        kPhase7ContractVersion,
        true,
        false
    });
    specs.push_back(ToolSpec{
        "tapescript_findings_list",
        "List persisted findings from a local Phase 7 analysis artifact.",
        json{
            {"type", "object"},
            {"properties", json{
                {"analysis_manifest_path", json{{"type", "string"}, {"minLength", 1}}},
                {"analysis_artifact_id", json{{"type", "string"}, {"minLength", 1}}}
            }},
            {"oneOf", json::array({
                json{{"required", json::array({"analysis_manifest_path"})}},
                json{{"required", json::array({"analysis_artifact_id"})}}
            })},
            {"additionalProperties", false}
        },
        "phase7_findings_list_local",
        kPhase7ContractVersion,
        true,
        false
    });
    specs.push_back(ToolSpec{
        "tapescript_playbook_apply",
        "Deferred tool placeholder for guarded playbook behavior.",
        emptyObjectSchema(),
        "phase7_playbook_apply_guarded_local",
        kPhase7ContractVersion,
        false,
        true
    });

    return specs;
}

} // namespace

ParsedAdapterArgs parseAdapterArgs(int argc, char** argv) {
    ParsedAdapterArgs parsed;
    parsed.config.engineSocketPath = envValueOrEmpty("TAPE_MCP_ENGINE_SOCKET");
    if (parsed.config.engineSocketPath.empty()) {
        parsed.config.engineSocketPath = tapescope::defaultSocketPath();
    }
    parsed.config.clientName = envValueOrEmpty("TAPE_MCP_CLIENT_NAME");
    if (parsed.config.clientName.empty()) {
        parsed.config.clientName = "tape-mcp";
    }

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--help" || arg == "-h") {
            parsed.showHelp = true;
            continue;
        }
        if (arg == "--engine-socket") {
            if (i + 1 >= argc) {
                parsed.error = "--engine-socket requires a value";
                return parsed;
            }
            parsed.config.engineSocketPath = argv[++i];
            continue;
        }
        if (arg == "--client-name") {
            if (i + 1 >= argc) {
                parsed.error = "--client-name requires a value";
                return parsed;
            }
            parsed.config.clientName = argv[++i];
            continue;
        }

        parsed.error = "unknown argument: " + arg;
        return parsed;
    }

    return parsed;
}

std::string adapterUsage(const std::string_view executableName) {
    std::string usage;
    usage += "Usage: ";
    usage += std::string(executableName);
    usage += " [--engine-socket PATH] [--client-name NAME]\n";
    usage += "Environment: TAPE_MCP_ENGINE_SOCKET, TAPE_MCP_CLIENT_NAME, LONG_TAPE_ENGINE_SOCKET\n";
    return usage;
}

Adapter::Adapter(AdapterConfig config)
    : tools_(buildToolSpecs()),
      engineRpc_(EngineRpcClientConfig{
          std::move(config.engineSocketPath),
          std::move(config.clientName)
      }) {}

json Adapter::initializeResult() const {
    return json{
        {"protocolVersion", "2024-11-05"},
        {"capabilities", {
            {"tools", {
                {"listChanged", false}
            }}
        }},
        {"serverInfo", {
            {"name", "tape-mcp"},
            {"version", kServerVersion}
        }}
    };
}

json Adapter::listToolsResult() const {
    json tools = json::array();
    for (const ToolSpec& tool : tools_) {
        tools.push_back(json{
            {"name", tool.name},
            {"description", tool.description},
            {"inputSchema", tool.inputSchema}
        });
    }
    return json{{"tools", std::move(tools)}};
}

json Adapter::callTool(const std::string& toolName, const json& args) const {
    const ToolSpec* tool = findTool(toolName);
    if (tool == nullptr) {
        const bool tapescriptPrefix = toolName.rfind("tapescript_", 0) == 0;
        return makeToolResult(makeErrorEnvelope(
            kLegacyContractVersion,
            toolName,
            "",
            false,
            false,
            "unsupported_tool",
            tapescriptPrefix
                ? "Unknown tapescript tool id for this adapter slice."
                : "Tool is not registered in tape-mcp.",
            false,
            revisionUnavailable()));
    }

    if (tool->supportedRead) {
        return invokeSupportedReadTool(*tool, args);
    }
    if (tool->reservedDeferred) {
        return invokeReservedDeferredTool(*tool);
    }

    return makeToolResult(makeErrorEnvelope(
        tool->contractVersion,
        tool->name,
        tool->engineCommand,
        false,
        false,
        "unsupported_tool",
        "Tool is registered but not executable in this adapter slice.",
        false,
        revisionUnavailable()));
}

const ToolSpec* Adapter::findTool(const std::string_view toolName) const {
    for (const ToolSpec& tool : tools_) {
        if (tool.name == toolName) {
            return &tool;
        }
    }
    return nullptr;
}

json Adapter::invokeSupportedReadTool(const ToolSpec& tool, const json& args) const {
    if (tool.name == "tapescript_status") {
        return invokeStatusTool(tool, args);
    }
    if (tool.name == "tapescript_read_live_tail") {
        return invokeReadLiveTailTool(tool, args);
    }
    if (tool.name == "tapescript_read_range") {
        return invokeReadRangeTool(tool, args);
    }
    if (tool.name == "tapescript_find_order_anchor") {
        return invokeFindOrderAnchorTool(tool, args);
    }
    if (tool.name == "tapescript_report_generate") {
        return invokeReportGenerateTool(tool, args);
    }
    if (tool.name == "tapescript_export_range") {
        return invokeExportRangeTool(tool, args);
    }
    if (tool.name == "tapescript_analyzer_run") {
        return invokeAnalyzerRunTool(tool, args);
    }
    if (tool.name == "tapescript_findings_list") {
        return invokeFindingsListTool(tool, args);
    }

    return makeToolResult(makeErrorEnvelope(
        tool.contractVersion,
        tool.name,
        tool.engineCommand,
        true,
        false,
        "unsupported_tool",
        "Tool is marked supported but no adapter executor is registered.",
        false,
        revisionUnavailable()));
}

json Adapter::invokeStatusTool(const ToolSpec& tool, const json& args) const {
    if (!args.is_object() || !args.empty()) {
        return makeToolResult(makeErrorEnvelope(
            tool.contractVersion,
            tool.name,
            tool.engineCommand,
            true,
            false,
            "invalid_arguments",
            "tapescript_status does not accept arguments.",
            false,
            revisionUnavailable()));
    }

    const EngineRpcResult<json> response = engineRpc_.openSession().query(tool.engineCommand, json::object());
    if (!response.ok()) {
        return makeToolResult(makeErrorEnvelope(
            tool.contractVersion,
            tool.name,
            tool.engineCommand,
            true,
            false,
            response.error.code,
            response.error.message,
            response.error.retryable,
            revisionUnavailable()));
    }

    if (!response.value.is_object()) {
        return makeToolResult(makeErrorEnvelope(
            tool.contractVersion,
            tool.name,
            tool.engineCommand,
            true,
            false,
            "malformed_response",
            "status payload must be an object.",
            false,
            revisionUnavailable()));
    }

    json revision = makeReadRevision("live");
    bool hasRevisionData = false;

    const auto manifestHash = firstPresentString(response.value,
                                                 {"manifest_hash", "last_manifest_hash", "manifest_sha256"});
    if (manifestHash.has_value()) {
        revision["manifest_hash"] = *manifestHash;
        hasRevisionData = true;
    }

    const auto latestSessionSeq = latestSessionSeqFromStatus(response.value);
    if (latestSessionSeq.has_value()) {
        revision["latest_session_seq"] = *latestSessionSeq;
        hasRevisionData = true;
    }

    if (hasRevisionData) {
        revision["source"] = "engine_payload";
    } else {
        revision["source"] = "unavailable";
        revision["staleness"] = "unknown";
    }

    return makeToolResult(makeSuccessEnvelope(
        tool.contractVersion,
        tool.name,
        tool.engineCommand,
        response.value,
        std::move(revision)));
}

json Adapter::invokeReadLiveTailTool(const ToolSpec& tool, const json& args) const {
    if (!args.is_object()) {
        return makeToolResult(makeErrorEnvelope(
            tool.contractVersion,
            tool.name,
            tool.engineCommand,
            true,
            false,
            "invalid_arguments",
            "tapescript_read_live_tail arguments must be an object.",
            false,
            revisionUnavailable()));
    }
    if (hasUnexpectedKeys(args, {"limit"})) {
        return makeToolResult(makeErrorEnvelope(
            tool.contractVersion,
            tool.name,
            tool.engineCommand,
            true,
            false,
            "invalid_arguments",
            "tapescript_read_live_tail accepts only the optional limit argument.",
            false,
            revisionUnavailable()));
    }

    std::uint64_t limit = kDefaultReadLiveTailLimit;
    if (args.contains("limit")) {
        const auto parsedLimit = asUint64(args.at("limit"));
        if (!parsedLimit.has_value() || *parsedLimit == 0) {
            return makeToolResult(makeErrorEnvelope(
                tool.contractVersion,
                tool.name,
                tool.engineCommand,
                true,
                false,
                "invalid_arguments",
                "tapescript_read_live_tail limit must be an integer greater than 0.",
                false,
                revisionUnavailable()));
        }
        limit = *parsedLimit;
    }

    const EngineRpcResult<json> response =
        engineRpc_.openSession().query(tool.engineCommand, json{{"limit", limit}});
    if (!response.ok()) {
        return makeToolResult(makeErrorEnvelope(
            tool.contractVersion,
            tool.name,
            tool.engineCommand,
            true,
            false,
            response.error.code,
            response.error.message,
            response.error.retryable,
            revisionUnavailable()));
    }

    const std::optional<std::vector<json>> events = extractEventArray(response.value);
    if (!events.has_value()) {
        return makeToolResult(makeErrorEnvelope(
            tool.contractVersion,
            tool.name,
            tool.engineCommand,
            true,
            false,
            "malformed_response",
            "read_live_tail payload must include an event array.",
            false,
            revisionUnavailable()));
    }

    json revision = makeReadRevision("live");
    const std::optional<SessionBounds> bounds = deriveSessionBounds(*events);
    if (bounds.has_value()) {
        revision["latest_session_seq"] = bounds->last;
        revision["first_session_seq"] = bounds->first;
        revision["last_session_seq"] = bounds->last;
        revision["source"] = "derived_from_events";
    } else {
        revision["source"] = "unavailable";
        revision["staleness"] = "unknown";
    }

    return makeToolResult(makeSuccessEnvelope(
        tool.contractVersion,
        tool.name,
        tool.engineCommand,
        json{
            {"events", *events},
            {"limit", limit}
        },
        std::move(revision)));
}

json Adapter::invokeReadRangeTool(const ToolSpec& tool, const json& args) const {
    if (!args.is_object()) {
        return makeToolResult(makeErrorEnvelope(
            tool.contractVersion,
            tool.name,
            tool.engineCommand,
            true,
            false,
            "invalid_arguments",
            "tapescript_read_range arguments must be an object.",
            false,
            revisionUnavailable()));
    }
    if (hasUnexpectedKeys(args, {"first_session_seq", "last_session_seq"})) {
        return makeToolResult(makeErrorEnvelope(
            tool.contractVersion,
            tool.name,
            tool.engineCommand,
            true,
            false,
            "invalid_arguments",
            "tapescript_read_range accepts only first_session_seq and last_session_seq.",
            false,
            revisionUnavailable()));
    }

    const auto firstSessionSeq = args.contains("first_session_seq")
        ? asUint64(args.at("first_session_seq"))
        : std::optional<std::uint64_t>{};
    const auto lastSessionSeq = args.contains("last_session_seq")
        ? asUint64(args.at("last_session_seq"))
        : std::optional<std::uint64_t>{};

    if (!firstSessionSeq.has_value() || *firstSessionSeq == 0 ||
        !lastSessionSeq.has_value() || *lastSessionSeq == 0) {
        return makeToolResult(makeErrorEnvelope(
            tool.contractVersion,
            tool.name,
            tool.engineCommand,
            true,
            false,
            "invalid_arguments",
            "tapescript_read_range requires positive integer first_session_seq and last_session_seq arguments.",
            false,
            revisionUnavailable()));
    }
    if (*firstSessionSeq > *lastSessionSeq) {
        return makeToolResult(makeErrorEnvelope(
            tool.contractVersion,
            tool.name,
            tool.engineCommand,
            true,
            false,
            "invalid_arguments",
            "tapescript_read_range requires first_session_seq <= last_session_seq.",
            false,
            revisionUnavailable()));
    }

    const json queryArgs = {
        {"first_session_seq", *firstSessionSeq},
        {"last_session_seq", *lastSessionSeq}
    };
    const EngineRpcResult<json> response = engineRpc_.openSession().query(tool.engineCommand, queryArgs);
    if (!response.ok()) {
        return makeToolResult(makeErrorEnvelope(
            tool.contractVersion,
            tool.name,
            tool.engineCommand,
            true,
            false,
            response.error.code,
            response.error.message,
            response.error.retryable,
            revisionUnavailable()));
    }

    const std::optional<std::vector<json>> events = extractEventArray(response.value);
    if (!events.has_value()) {
        return makeToolResult(makeErrorEnvelope(
            tool.contractVersion,
            tool.name,
            tool.engineCommand,
            true,
            false,
            "malformed_response",
            "read_range payload must include an event array.",
            false,
            revisionUnavailable()));
    }

    json revision = makeReadRevision("snapshot");
    revision["first_session_seq"] = *firstSessionSeq;
    revision["last_session_seq"] = *lastSessionSeq;

    json result = {
        {"events", *events},
        {"requested_window", {
            {"first_session_seq", *firstSessionSeq},
            {"last_session_seq", *lastSessionSeq}
        }}
    };

    const std::optional<SessionBounds> bounds = deriveSessionBounds(*events);
    if (bounds.has_value()) {
        revision["latest_session_seq"] = bounds->last;
        revision["source"] = "derived_from_events";
        result["observed_window"] = {
            {"first_session_seq", bounds->first},
            {"last_session_seq", bounds->last}
        };
    }

    return makeToolResult(makeSuccessEnvelope(
        tool.contractVersion,
        tool.name,
        tool.engineCommand,
        std::move(result),
        std::move(revision)));
}

json Adapter::invokeFindOrderAnchorTool(const ToolSpec& tool, const json& args) const {
    if (!args.is_object()) {
        return makeToolResult(makeErrorEnvelope(
            tool.contractVersion,
            tool.name,
            tool.engineCommand,
            true,
            false,
            "invalid_arguments",
            "tapescript_find_order_anchor arguments must be an object.",
            false,
            revisionUnavailable()));
    }
    if (hasUnexpectedKeys(args, {"trace_id", "order_id", "perm_id", "exec_id"})) {
        return makeToolResult(makeErrorEnvelope(
            tool.contractVersion,
            tool.name,
            tool.engineCommand,
            true,
            false,
            "invalid_arguments",
            "tapescript_find_order_anchor accepts only trace_id, order_id, perm_id, or exec_id.",
            false,
            revisionUnavailable()));
    }

    json queryArgs = json::object();
    int provided = 0;

    if (args.contains("trace_id")) {
        const auto traceId = asUint64(args.at("trace_id"));
        if (!traceId.has_value()) {
            return makeToolResult(makeErrorEnvelope(
                tool.contractVersion,
                tool.name,
                tool.engineCommand,
                true,
                false,
                "invalid_arguments",
                "trace_id must be a non-negative integer.",
                false,
                revisionUnavailable()));
        }
        queryArgs["trace_id"] = *traceId;
        ++provided;
    }
    if (args.contains("order_id")) {
        const auto orderId = asInt64(args.at("order_id"));
        if (!orderId.has_value()) {
            return makeToolResult(makeErrorEnvelope(
                tool.contractVersion,
                tool.name,
                tool.engineCommand,
                true,
                false,
                "invalid_arguments",
                "order_id must be an integer.",
                false,
                revisionUnavailable()));
        }
        queryArgs["order_id"] = *orderId;
        ++provided;
    }
    if (args.contains("perm_id")) {
        const auto permId = asInt64(args.at("perm_id"));
        if (!permId.has_value()) {
            return makeToolResult(makeErrorEnvelope(
                tool.contractVersion,
                tool.name,
                tool.engineCommand,
                true,
                false,
                "invalid_arguments",
                "perm_id must be an integer.",
                false,
                revisionUnavailable()));
        }
        queryArgs["perm_id"] = *permId;
        ++provided;
    }
    if (args.contains("exec_id")) {
        const auto execId = asNonEmptyString(args.at("exec_id"));
        if (!execId.has_value()) {
            return makeToolResult(makeErrorEnvelope(
                tool.contractVersion,
                tool.name,
                tool.engineCommand,
                true,
                false,
                "invalid_arguments",
                "exec_id must be a non-empty string.",
                false,
                revisionUnavailable()));
        }
        queryArgs["exec_id"] = *execId;
        ++provided;
    }

    if (provided != 1) {
        return makeToolResult(makeErrorEnvelope(
            tool.contractVersion,
            tool.name,
            tool.engineCommand,
            true,
            false,
            "invalid_arguments",
            "Exactly one of trace_id, order_id, perm_id, or exec_id is required.",
            false,
            revisionUnavailable()));
    }

    const EngineRpcResult<json> response = engineRpc_.openSession().query(tool.engineCommand, queryArgs);
    if (!response.ok()) {
        return makeToolResult(makeErrorEnvelope(
            tool.contractVersion,
            tool.name,
            tool.engineCommand,
            true,
            false,
            response.error.code,
            response.error.message,
            response.error.retryable,
            revisionUnavailable()));
    }

    json revision = makeReadRevision("snapshot");
    const std::optional<std::vector<json>> events = extractEventArray(response.value);
    if (events.has_value()) {
        const std::optional<SessionBounds> bounds = deriveSessionBounds(*events);
        if (bounds.has_value()) {
            revision["latest_session_seq"] = bounds->last;
            revision["first_session_seq"] = bounds->first;
            revision["last_session_seq"] = bounds->last;
            revision["source"] = "derived_from_events";
        } else {
            revision["source"] = "unavailable";
            revision["staleness"] = "unknown";
        }
    } else {
        revision["source"] = "unavailable";
        revision["staleness"] = "unknown";
    }

    return makeToolResult(makeSuccessEnvelope(
        tool.contractVersion,
        tool.name,
        tool.engineCommand,
        json{
            {"payload", response.value},
            {"anchor_context", extractAnchorContext(response.value)}
        },
        std::move(revision)));
}

json Adapter::invokeReportGenerateTool(const ToolSpec& tool, const json& args) const {
    if (!args.is_object()) {
        return makeToolResult(makeErrorEnvelope(
            tool.contractVersion,
            tool.name,
            tool.engineCommand,
            true,
            false,
            "invalid_arguments",
            "tapescript_report_generate arguments must be an object.",
            false,
            revisionUnavailable()));
    }
    if (hasUnexpectedKeys(args, {"trace_id", "order_id", "perm_id", "exec_id"})) {
        return makeToolResult(makeErrorEnvelope(
            tool.contractVersion,
            tool.name,
            tool.engineCommand,
            true,
            false,
            "invalid_arguments",
            "tapescript_report_generate accepts only trace_id, order_id, perm_id, or exec_id.",
            false,
            revisionUnavailable()));
    }

    std::string anchorErrorCode;
    std::string anchorErrorMessage;
    const std::optional<ResolvedPhase6Anchor> resolvedAnchor =
        resolvePhase6AnchorFromArgs(args, &anchorErrorCode, &anchorErrorMessage);
    if (!resolvedAnchor.has_value()) {
        return makeToolResult(makeErrorEnvelope(
            tool.contractVersion,
            tool.name,
            tool.engineCommand,
            true,
            false,
            anchorErrorCode.empty() ? "invalid_arguments" : anchorErrorCode,
            anchorErrorMessage.empty()
                ? "Failed to resolve a trace anchor for report generation."
                : anchorErrorMessage,
            false,
            makeReadRevision("snapshot")));
    }

    Phase6ReportOutputArtifact artifact;
    std::string artifactError;
    if (!generatePhase6ReportOutputArtifact(resolvedAnchor->traceId, "", &artifact, &artifactError)) {
        const bool traceMissing =
            artifactError.find("not found") != std::string::npos ||
            artifactError.find("No matching trace") != std::string::npos;
        return makeToolResult(makeErrorEnvelope(
            tool.contractVersion,
            tool.name,
            tool.engineCommand,
            true,
            false,
            traceMissing ? "trace_not_found" : "artifact_generation_failed",
            artifactError.empty() ? "Phase 6 report artifact generation failed." : artifactError,
            false,
            makeReadRevision("snapshot")));
    }

    const json manifest = loadJsonFileOrNull(artifact.manifestPath);
    json result{
        {"trace_anchor_request", resolvedAnchor->requestedAnchor},
        {"trace_anchor", resolvedAnchor->resolvedAnchor},
        {"artifact", reportArtifactResult(artifact)},
        {"report_output", reportArtifactResult(artifact)},
        {"generated_artifacts", json::array({
            json{
                {"artifact_type", artifact.artifactType},
                {"artifact_id", artifact.artifactId},
                {"manifest_path", artifact.manifestPath}
            }
        })}
    };
    if (manifest.is_object()) {
        result["report_metadata"] = json{
            {"generated_at_utc", manifest.value("generated_at_utc", std::string())},
            {"evidence", manifest.value("evidence", json::object())},
            {"revision_context", manifest.value("revision_context", json::object())}
        };
    }

    json revision = makeReadRevision("snapshot");
    revision["source"] = "artifact_manifest";
    return makeToolResult(makeSuccessEnvelope(
        tool.contractVersion,
        tool.name,
        tool.engineCommand,
        std::move(result),
        std::move(revision)));
}

json Adapter::invokeExportRangeTool(const ToolSpec& tool, const json& args) const {
    if (!args.is_object()) {
        return makeToolResult(makeErrorEnvelope(
            tool.contractVersion,
            tool.name,
            tool.engineCommand,
            true,
            false,
            "invalid_arguments",
            "tapescript_export_range arguments must be an object.",
            false,
            revisionUnavailable()));
    }
    if (hasUnexpectedKeys(args,
                          {"trace_id", "order_id", "perm_id", "exec_id",
                           "first_session_seq", "last_session_seq"})) {
        return makeToolResult(makeErrorEnvelope(
            tool.contractVersion,
            tool.name,
            tool.engineCommand,
            true,
            false,
            "invalid_arguments",
            "tapescript_export_range accepts only trace_id, order_id, perm_id, exec_id, first_session_seq, and last_session_seq.",
            false,
            revisionUnavailable()));
    }

    const bool hasFirst = args.contains("first_session_seq");
    const bool hasLast = args.contains("last_session_seq");
    std::optional<std::uint64_t> firstSessionSeq;
    std::optional<std::uint64_t> lastSessionSeq;
    if (hasFirst != hasLast) {
        return makeToolResult(makeErrorEnvelope(
            tool.contractVersion,
            tool.name,
            tool.engineCommand,
            true,
            false,
            "invalid_arguments",
            "tapescript_export_range requires first_session_seq and last_session_seq together when specifying a window.",
            false,
            revisionUnavailable()));
    }
    if (hasFirst) {
        firstSessionSeq = asUint64(args.at("first_session_seq"));
        lastSessionSeq = asUint64(args.at("last_session_seq"));
        if (!firstSessionSeq.has_value() || !lastSessionSeq.has_value() ||
            *firstSessionSeq == 0 || *lastSessionSeq == 0) {
            return makeToolResult(makeErrorEnvelope(
                tool.contractVersion,
                tool.name,
                tool.engineCommand,
                true,
                false,
                "invalid_arguments",
                "tapescript_export_range window values must be positive integers.",
                false,
                revisionUnavailable()));
        }
        if (*firstSessionSeq > *lastSessionSeq) {
            return makeToolResult(makeErrorEnvelope(
                tool.contractVersion,
                tool.name,
                tool.engineCommand,
                true,
                false,
                "invalid_arguments",
                "tapescript_export_range requires first_session_seq <= last_session_seq.",
                false,
                revisionUnavailable()));
        }
    }

    const bool hasAnchor =
        args.contains("trace_id") || args.contains("order_id") ||
        args.contains("perm_id") || args.contains("exec_id");
    if (!hasAnchor && hasFirst) {
        return makeToolResult(makeErrorEnvelope(
            tool.contractVersion,
            tool.name,
            tool.engineCommand,
            true,
            true,
            "deferred_behavior",
            "Range-only export without a trace anchor is deferred in this Phase 6 slice.",
            false,
            makeReadRevision("snapshot")));
    }

    std::string anchorErrorCode;
    std::string anchorErrorMessage;
    const std::optional<ResolvedPhase6Anchor> resolvedAnchor =
        resolvePhase6AnchorFromArgs(args, &anchorErrorCode, &anchorErrorMessage);
    if (!resolvedAnchor.has_value()) {
        return makeToolResult(makeErrorEnvelope(
            tool.contractVersion,
            tool.name,
            tool.engineCommand,
            true,
            false,
            anchorErrorCode.empty() ? "invalid_arguments" : anchorErrorCode,
            anchorErrorMessage.empty()
                ? "Failed to resolve a trace anchor for case export."
                : anchorErrorMessage,
            false,
            makeReadRevision("snapshot")));
    }

    Phase6CaseBundleArtifact artifact;
    std::string artifactError;
    if (!generatePhase6CaseBundleArtifact(resolvedAnchor->traceId, "", &artifact, &artifactError)) {
        const bool traceMissing =
            artifactError.find("not found") != std::string::npos ||
            artifactError.find("No matching trace") != std::string::npos;
        return makeToolResult(makeErrorEnvelope(
            tool.contractVersion,
            tool.name,
            tool.engineCommand,
            true,
            false,
            traceMissing ? "trace_not_found" : "artifact_generation_failed",
            artifactError.empty() ? "Phase 6 case bundle generation failed." : artifactError,
            false,
            makeReadRevision("snapshot")));
    }

    const json caseManifest = loadJsonFileOrNull(artifact.manifestPath);
    const json reportManifest = loadJsonFileOrNull(artifact.reportOutput.manifestPath);

    json result{
        {"trace_anchor_request", resolvedAnchor->requestedAnchor},
        {"trace_anchor", resolvedAnchor->resolvedAnchor},
        {"requested_window", hasFirst
            ? json{
                {"first_session_seq", *firstSessionSeq},
                {"last_session_seq", *lastSessionSeq}
            }
            : json(nullptr)},
        {"artifact", caseArtifactResult(artifact)},
        {"case_bundle", caseArtifactResult(artifact)},
        {"report_output", reportArtifactResult(artifact.reportOutput)},
        {"generated_artifacts", json::array({
            json{
                {"artifact_type", artifact.artifactType},
                {"artifact_id", artifact.artifactId},
                {"manifest_path", artifact.manifestPath}
            },
            json{
                {"artifact_type", artifact.reportOutput.artifactType},
                {"artifact_id", artifact.reportOutput.artifactId},
                {"manifest_path", artifact.reportOutput.manifestPath}
            }
        })}
    };
    if (caseManifest.is_object()) {
        result["case_metadata"] = json{
            {"generated_at_utc", caseManifest.value("generated_at_utc", std::string())},
            {"bridge_payload", caseManifest.value("bridge_payload", json::object())},
            {"revision_context", caseManifest.value("revision_context", json::object())}
        };
    }
    if (reportManifest.is_object()) {
        result["report_metadata"] = json{
            {"generated_at_utc", reportManifest.value("generated_at_utc", std::string())},
            {"evidence", reportManifest.value("evidence", json::object())}
        };
    }

    json revision = makeReadRevision("snapshot");
    revision["source"] = "artifact_manifest";
    return makeToolResult(makeSuccessEnvelope(
        tool.contractVersion,
        tool.name,
        tool.engineCommand,
        std::move(result),
        std::move(revision)));
}

json Adapter::invokeAnalyzerRunTool(const ToolSpec& tool, const json& args) const {
    if (!args.is_object()) {
        return makeToolResult(makeErrorEnvelope(
            tool.contractVersion,
            tool.name,
            tool.engineCommand,
            true,
            false,
            "invalid_arguments",
            "tapescript_analyzer_run arguments must be an object.",
            false,
            makeReadRevision("snapshot")));
    }
    if (hasUnexpectedKeys(args, {"case_manifest_path", "report_manifest_path", "analysis_profile"})) {
        return makeToolResult(makeErrorEnvelope(
            tool.contractVersion,
            tool.name,
            tool.engineCommand,
            true,
            false,
            "invalid_arguments",
            "tapescript_analyzer_run accepts only case_manifest_path, report_manifest_path, and analysis_profile.",
            false,
            makeReadRevision("snapshot")));
    }

    const bool hasCaseManifest = args.contains("case_manifest_path");
    const bool hasReportManifest = args.contains("report_manifest_path");
    if (hasCaseManifest == hasReportManifest) {
        return makeToolResult(makeErrorEnvelope(
            tool.contractVersion,
            tool.name,
            tool.engineCommand,
            true,
            false,
            "invalid_arguments",
            "Exactly one of case_manifest_path or report_manifest_path is required.",
            false,
            makeReadRevision("snapshot")));
    }

    std::optional<std::string> caseManifestPath;
    if (hasCaseManifest) {
        caseManifestPath = asNonEmptyString(args.at("case_manifest_path"));
        if (!caseManifestPath.has_value()) {
            return makeToolResult(makeErrorEnvelope(
                tool.contractVersion,
                tool.name,
                tool.engineCommand,
                true,
                false,
                "invalid_arguments",
                "case_manifest_path must be a non-empty string.",
                false,
                makeReadRevision("snapshot")));
        }
    }

    std::optional<std::string> reportManifestPath;
    if (hasReportManifest) {
        reportManifestPath = asNonEmptyString(args.at("report_manifest_path"));
        if (!reportManifestPath.has_value()) {
            return makeToolResult(makeErrorEnvelope(
                tool.contractVersion,
                tool.name,
                tool.engineCommand,
                true,
                false,
                "invalid_arguments",
                "report_manifest_path must be a non-empty string.",
                false,
                makeReadRevision("snapshot")));
        }
    }

    std::string analyzerPassId = kPhase7DefaultAnalyzerPassId;
    if (args.contains("analysis_profile")) {
        const std::optional<std::string> profile = asNonEmptyString(args.at("analysis_profile"));
        if (!profile.has_value()) {
            return makeToolResult(makeErrorEnvelope(
                tool.contractVersion,
                tool.name,
                tool.engineCommand,
                true,
                false,
                "invalid_arguments",
                "analysis_profile must be a non-empty string when provided.",
                false,
                makeReadRevision("snapshot")));
        }
        if (*profile != kPhase7DefaultAnalyzerPassId) {
            return makeToolResult(makeErrorEnvelope(
                tool.contractVersion,
                tool.name,
                tool.engineCommand,
                true,
                true,
                "deferred_behavior",
                "Requested analysis_profile is deferred in this Phase 7 slice.",
                false,
                makeReadRevision("snapshot")));
        }
        analyzerPassId = *profile;
    }

    const fs::path sourceManifestPath =
        hasCaseManifest ? fs::path(*caseManifestPath) : fs::path(*reportManifestPath);
    std::error_code existsEc;
    if (!fs::exists(sourceManifestPath, existsEc)) {
        return makeToolResult(makeErrorEnvelope(
            tool.contractVersion,
            tool.name,
            tool.engineCommand,
            true,
            false,
            "artifact_not_found",
            "Source manifest path does not exist.",
            false,
            makeReadRevision("snapshot")));
    }

    Phase7AnalysisOutputArtifact artifact;
    std::string analysisError;
    if (!runPhase7AnalyzerFromPhase6Manifest(sourceManifestPath.string(),
                                             "",
                                             analyzerPassId,
                                             &artifact,
                                             &analysisError)) {
        const std::string mappedCode = mapAnalyzerFailureCode(analysisError);
        return makeToolResult(makeErrorEnvelope(
            tool.contractVersion,
            tool.name,
            tool.engineCommand,
            true,
            false,
            mappedCode,
            analysisError.empty() ? "Phase 7 analyzer execution failed." : analysisError,
            false,
            makeReadRevision("snapshot")));
    }

    json analysisManifest;
    std::string manifestError;
    if (!readJsonFile(fs::path(artifact.manifestPath), &analysisManifest, &manifestError)) {
        return makeToolResult(makeErrorEnvelope(
            tool.contractVersion,
            tool.name,
            tool.engineCommand,
            true,
            false,
            "artifact_load_failed",
            manifestError.empty() ? "Failed to load generated analysis manifest." : manifestError,
            false,
            makeReadRevision("snapshot")));
    }

    json findings;
    std::string findingsError;
    if (!readJsonFile(fs::path(artifact.findingsPath), &findings, &findingsError) || !findings.is_array()) {
        return makeToolResult(makeErrorEnvelope(
            tool.contractVersion,
            tool.name,
            tool.engineCommand,
            true,
            false,
            "artifact_load_failed",
            findingsError.empty()
                ? "Failed to load generated findings payload."
                : findingsError,
            false,
            makeReadRevision("snapshot")));
    }

    const json sourceManifest = loadJsonFileOrNull(artifact.sourceManifestPath);
    const json sourceArtifact{
        {"artifact_type", artifact.sourceArtifactType},
        {"contract_version", sourceManifest.is_object()
            ? sourceManifest.value("contract_version", std::string())
            : std::string()},
        {"artifact_id", artifact.sourceArtifactId},
        {"manifest_path", artifact.sourceManifestPath}
    };
    const json analysisArtifact = analysisArtifactResult(fs::path(artifact.manifestPath), analysisManifest);
    json result{
        {"source_artifact", sourceArtifact},
        {"analysis_artifact", analysisArtifact},
        {"artifact", analysisArtifact},
        {"generated_artifacts", json::array({
            json{
                {"artifact_type", artifact.artifactType},
                {"artifact_id", artifact.artifactId},
                {"manifest_path", artifact.manifestPath}
            }
        })},
        {"replay_context", analysisManifest.value("replay_context", json::object())},
        {"findings_summary", analysisManifest.value("findings_summary", json::object())},
        {"findings", findings}
    };
    if (hasCaseManifest) {
        result["case_manifest_path"] = *caseManifestPath;
    }
    if (hasReportManifest) {
        result["report_manifest_path"] = *reportManifestPath;
    }

    json revision = makeReadRevision("snapshot");
    revision["source"] = "artifact_manifest";
    return makeToolResult(makeSuccessEnvelope(
        tool.contractVersion,
        tool.name,
        tool.engineCommand,
        std::move(result),
        std::move(revision)));
}

json Adapter::invokeFindingsListTool(const ToolSpec& tool, const json& args) const {
    if (!args.is_object()) {
        return makeToolResult(makeErrorEnvelope(
            tool.contractVersion,
            tool.name,
            tool.engineCommand,
            true,
            false,
            "invalid_arguments",
            "tapescript_findings_list arguments must be an object.",
            false,
            makeReadRevision("snapshot")));
    }
    if (hasUnexpectedKeys(args, {"analysis_manifest_path", "analysis_artifact_id"})) {
        return makeToolResult(makeErrorEnvelope(
            tool.contractVersion,
            tool.name,
            tool.engineCommand,
            true,
            false,
            "invalid_arguments",
            "tapescript_findings_list accepts only analysis_manifest_path or analysis_artifact_id.",
            false,
            makeReadRevision("snapshot")));
    }

    const bool hasManifestPath = args.contains("analysis_manifest_path");
    const bool hasArtifactId = args.contains("analysis_artifact_id");
    if (hasManifestPath == hasArtifactId) {
        return makeToolResult(makeErrorEnvelope(
            tool.contractVersion,
            tool.name,
            tool.engineCommand,
            true,
            false,
            "invalid_arguments",
            "Exactly one of analysis_manifest_path or analysis_artifact_id is required.",
            false,
            makeReadRevision("snapshot")));
    }

    fs::path manifestPath;
    if (hasManifestPath) {
        const std::optional<std::string> providedPath = asNonEmptyString(args.at("analysis_manifest_path"));
        if (!providedPath.has_value()) {
            return makeToolResult(makeErrorEnvelope(
                tool.contractVersion,
                tool.name,
                tool.engineCommand,
                true,
                false,
                "invalid_arguments",
                "analysis_manifest_path must be a non-empty string.",
                false,
                makeReadRevision("snapshot")));
        }
        manifestPath = fs::path(*providedPath);
    } else {
        const std::optional<std::string> artifactId = asNonEmptyString(args.at("analysis_artifact_id"));
        if (!artifactId.has_value()) {
            return makeToolResult(makeErrorEnvelope(
                tool.contractVersion,
                tool.name,
                tool.engineCommand,
                true,
                false,
                "invalid_arguments",
                "analysis_artifact_id must be a non-empty string.",
                false,
                makeReadRevision("snapshot")));
        }
        manifestPath = fs::path(appDataDirectory()) /
            "phase7_artifacts" /
            "analysis_output.v1" /
            *artifactId /
            "manifest.json";
    }

    std::error_code existsEc;
    if (!fs::exists(manifestPath, existsEc)) {
        return makeToolResult(makeErrorEnvelope(
            tool.contractVersion,
            tool.name,
            tool.engineCommand,
            true,
            false,
            "artifact_not_found",
            "Analysis manifest path does not exist.",
            false,
            makeReadRevision("snapshot")));
    }

    json manifest;
    std::string manifestError;
    if (!readJsonFile(manifestPath, &manifest, &manifestError) || !manifest.is_object()) {
        return makeToolResult(makeErrorEnvelope(
            tool.contractVersion,
            tool.name,
            tool.engineCommand,
            true,
            false,
            "artifact_load_failed",
            manifestError.empty() ? "Failed to parse analysis manifest json." : manifestError,
            false,
            makeReadRevision("snapshot")));
    }

    if (manifest.value("artifact_type", std::string()) != kPhase7AnalysisArtifactType ||
        manifest.value("contract_version", std::string()) != kPhase7ContractVersion) {
        return makeToolResult(makeErrorEnvelope(
            tool.contractVersion,
            tool.name,
            tool.engineCommand,
            true,
            false,
            "unsupported_source_contract",
            "Analysis manifest artifact type or contract version is unsupported.",
            false,
            makeReadRevision("snapshot")));
    }

    const json files = manifest.value("files", json::object());
    const std::string findingsRelativePath = files.value("findings_json", std::string());
    if (findingsRelativePath.empty()) {
        return makeToolResult(makeErrorEnvelope(
            tool.contractVersion,
            tool.name,
            tool.engineCommand,
            true,
            false,
            "artifact_load_failed",
            "Analysis manifest is missing files.findings_json.",
            false,
            makeReadRevision("snapshot")));
    }

    const fs::path findingsPath = manifestPath.parent_path() / fs::path(findingsRelativePath);
    if (!fs::exists(findingsPath, existsEc)) {
        return makeToolResult(makeErrorEnvelope(
            tool.contractVersion,
            tool.name,
            tool.engineCommand,
            true,
            false,
            "artifact_not_found",
            "Analysis findings payload path does not exist.",
            false,
            makeReadRevision("snapshot")));
    }

    json findings;
    std::string findingsError;
    if (!readJsonFile(findingsPath, &findings, &findingsError) || !findings.is_array()) {
        return makeToolResult(makeErrorEnvelope(
            tool.contractVersion,
            tool.name,
            tool.engineCommand,
            true,
            false,
            "artifact_load_failed",
            findingsError.empty() ? "Failed to parse findings payload json." : findingsError,
            false,
            makeReadRevision("snapshot")));
    }

    const json analysisArtifact = analysisArtifactResult(manifestPath, manifest);
    json result{
        {"analysis_artifact", analysisArtifact},
        {"artifact", analysisArtifact},
        {"findings", findings},
        {"replay_context", manifest.value("replay_context", json::object())},
        {"findings_summary", manifest.value("findings_summary", json::object())}
    };

    json revision = makeReadRevision("snapshot");
    revision["source"] = "artifact_manifest";
    return makeToolResult(makeSuccessEnvelope(
        tool.contractVersion,
        tool.name,
        tool.engineCommand,
        std::move(result),
        std::move(revision)));
}

json Adapter::invokeReservedDeferredTool(const ToolSpec& tool) const {
    return makeToolResult(makeErrorEnvelope(
        tool.contractVersion,
        tool.name,
        tool.engineCommand,
        false,
        true,
        "deferred_tool",
        "Tool is explicitly deferred by the current adapter tool-slice contract.",
        false,
        revisionUnavailable()));
}

json Adapter::makeToolResult(const json& envelope) const {
    return json{
        {"isError", !envelope.value("ok", false)},
        {"structuredContent", envelope},
        {"content", json::array({
            json{
                {"type", "text"},
                {"text", envelope.dump()}
            }
        })}
    };
}

json Adapter::makeSuccessEnvelope(const std::string& contractVersion,
                                  const std::string& toolName,
                                  const std::string& engineCommand,
                                  json result,
                                  json revision) const {
    return json{
        {"ok", true},
        {"result", std::move(result)},
        {"meta", {
            {"contract_version", contractVersion},
            {"tool", toolName},
            {"engine_command", engineCommand.empty() ? json(nullptr) : json(engineCommand)},
            {"supported", true},
            {"deferred", false},
            {"revision", std::move(revision)}
        }},
        {"error", nullptr}
    };
}

json Adapter::makeErrorEnvelope(const std::string& contractVersion,
                                const std::string& toolName,
                                const std::string& engineCommand,
                                const bool supported,
                                const bool deferred,
                                const std::string& errorCode,
                                const std::string& errorMessage,
                                const bool retryable,
                                json revision) const {
    return json{
        {"ok", false},
        {"result", nullptr},
        {"meta", {
            {"contract_version", contractVersion},
            {"tool", toolName},
            {"engine_command", engineCommand.empty() ? json(nullptr) : json(engineCommand)},
            {"supported", supported},
            {"deferred", deferred},
            {"revision", std::move(revision)}
        }},
        {"error", {
            {"code", errorCode},
            {"message", errorMessage},
            {"retryable", retryable}
        }}
    };
}

} // namespace tape_mcp
