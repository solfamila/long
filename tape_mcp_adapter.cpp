#include "tape_mcp_adapter.h"

#include <array>
#include <cstdlib>
#include <utility>

namespace tape_mcp {

namespace {

constexpr const char* kContractVersion = "phase5-mcp-compat-v1";
constexpr const char* kServerVersion = "0.1.0";

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

std::vector<ToolSpec> buildToolSpecs() {
    std::vector<ToolSpec> specs;
    specs.push_back(ToolSpec{
        "tapescript_status",
        "Scaffold tool for status seam wiring (Wave 2: deferred response).",
        emptyObjectSchema(),
        "status",
        true,
        false
    });
    specs.push_back(ToolSpec{
        "tapescript_read_live_tail",
        "Scaffold tool for read_live_tail seam wiring (Wave 2: deferred response).",
        json{
            {"type", "object"},
            {"properties", json{
                {"limit", json{{"type", "integer"}, {"minimum", 1}}}
            }},
            {"additionalProperties", false}
        },
        "read_live_tail",
        true,
        false
    });
    specs.push_back(ToolSpec{
        "tapescript_read_range",
        "Scaffold tool for read_range seam wiring (Wave 2: deferred response).",
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
        true,
        false
    });
    specs.push_back(ToolSpec{
        "tapescript_find_order_anchor",
        "Scaffold tool for find_order_anchor seam wiring (Wave 2: deferred response).",
        json{
            {"type", "object"},
            {"properties", json{
                {"trace_id", json{{"type", "integer"}}},
                {"order_id", json{{"type", "integer"}}},
                {"perm_id", json{{"type", "integer"}}},
                {"exec_id", json{{"type", "string"}}}
            }},
            {"additionalProperties", false}
        },
        "find_order_anchor",
        true,
        false
    });

    constexpr std::array<const char*, 5> deferredTools{
        "tapescript_analyzer_run",
        "tapescript_findings_list",
        "tapescript_playbook_apply",
        "tapescript_report_generate",
        "tapescript_export_range"
    };
    for (const char* toolName : deferredTools) {
        specs.push_back(ToolSpec{
            toolName,
            "Deferred tool placeholder (reserved by Phase 5 contract).",
            emptyObjectSchema(),
            "",
            false,
            true
        });
    }

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
        return makeToolResult(makeEnvelope(
            toolName,
            "",
            false,
            false,
            "unsupported_tool",
            tapescriptPrefix
                ? "Unknown tapescript tool id for this adapter slice."
                : "Tool is not registered in tape-mcp.",
            false));
    }

    if (tool->scaffoldRead) {
        return invokeScaffoldReadTool(*tool, args);
    }
    if (tool->reservedDeferred) {
        return invokeReservedDeferredTool(*tool);
    }

    return makeToolResult(makeEnvelope(
        tool->name,
        tool->engineCommand,
        false,
        false,
        "unsupported_tool",
        "Tool is registered but not executable in this adapter slice.",
        false));
}

const ToolSpec* Adapter::findTool(const std::string_view toolName) const {
    for (const ToolSpec& tool : tools_) {
        if (tool.name == toolName) {
            return &tool;
        }
    }
    return nullptr;
}

json Adapter::invokeScaffoldReadTool(const ToolSpec& tool, const json& args) const {
    const json queryArgs = args.is_object() ? args : json::object();
    const EngineRpcResult<json> probe = engineRpc_.openSession().query(tool.engineCommand, queryArgs);
    if (!probe.ok()) {
        return makeToolResult(makeEnvelope(
            tool.name,
            tool.engineCommand,
            true,
            false,
            probe.error.code,
            probe.error.message,
            probe.error.retryable));
    }

    return makeToolResult(makeEnvelope(
        tool.name,
        tool.engineCommand,
        true,
        true,
        "deferred_tool",
        "Tool is registered in scaffold mode; read behavior is deferred in this wave.",
        false));
}

json Adapter::invokeReservedDeferredTool(const ToolSpec& tool) const {
    return makeToolResult(makeEnvelope(
        tool.name,
        tool.engineCommand,
        false,
        true,
        "deferred_tool",
        "Tool is explicitly deferred by the Phase 5 compatibility contract.",
        false));
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

json Adapter::makeEnvelope(const std::string& toolName,
                           const std::string& engineCommand,
                           const bool supported,
                           const bool deferred,
                           const std::string& errorCode,
                           const std::string& errorMessage,
                           const bool retryable) const {
    return json{
        {"ok", false},
        {"result", nullptr},
        {"meta", {
            {"contract_version", kContractVersion},
            {"tool", toolName},
            {"engine_command", engineCommand.empty() ? json(nullptr) : json(engineCommand)},
            {"supported", supported},
            {"deferred", deferred},
            {"revision", revisionUnavailable()}
        }},
        {"error", {
            {"code", errorCode},
            {"message", errorMessage},
            {"retryable", retryable}
        }}
    };
}

} // namespace tape_mcp
