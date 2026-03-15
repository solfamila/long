#pragma once

#include <string>
#include <string_view>
#include <vector>

#include <nlohmann/json.hpp>

#include "tape_mcp_engine_rpc.h"

namespace tape_mcp {

using json = nlohmann::json;

struct AdapterConfig {
    std::string engineSocketPath;
    std::string clientName = "tape-mcp";
};

struct ParsedAdapterArgs {
    AdapterConfig config;
    bool showHelp = false;
    std::string error;
};

struct ToolSpec {
    std::string name;
    std::string description;
    json inputSchema;
    std::string engineCommand;
    bool supportedRead = false;
    bool reservedDeferred = false;
};

ParsedAdapterArgs parseAdapterArgs(int argc, char** argv);
std::string adapterUsage(std::string_view executableName);

class Adapter {
public:
    explicit Adapter(AdapterConfig config);

    [[nodiscard]] json initializeResult() const;
    [[nodiscard]] json listToolsResult() const;
    [[nodiscard]] json callTool(const std::string& toolName, const json& args) const;

private:
    [[nodiscard]] const ToolSpec* findTool(std::string_view toolName) const;
    [[nodiscard]] json invokeSupportedReadTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokeStatusTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokeReadLiveTailTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokeReadRangeTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokeFindOrderAnchorTool(const ToolSpec& tool, const json& args) const;
    [[nodiscard]] json invokeReservedDeferredTool(const ToolSpec& tool) const;
    [[nodiscard]] json makeToolResult(const json& envelope) const;
    [[nodiscard]] json makeSuccessEnvelope(const std::string& toolName,
                                           const std::string& engineCommand,
                                           json result,
                                           json revision) const;
    [[nodiscard]] json makeErrorEnvelope(const std::string& toolName,
                                         const std::string& engineCommand,
                                         bool supported,
                                         bool deferred,
                                         const std::string& errorCode,
                                         const std::string& errorMessage,
                                         bool retryable,
                                         json revision) const;

    std::vector<ToolSpec> tools_;
    EngineRpcClient engineRpc_;
};

} // namespace tape_mcp
