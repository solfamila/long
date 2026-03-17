#include "uw_context_connectors.h"

#include "uw_http.h"
#include "uw_runtime.h"

#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace {

using uw_context_service::CredentialBinding;
using uw_context_service::FetchPlan;
using uw_context_service::HttpResponse;
using uw_context_service::ProviderStep;
using uw_context_service::UWMcpConnector;
using uw_context_service::UWMcpFacetResolution;
using json = nlohmann::json;

constexpr const char* kSourceDir = TWS_GUI_SOURCE_DIR;
constexpr const char* kDocsUrl = "https://unusualwhales.com/public-api/mcp";
constexpr const char* kDefaultUrl = "https://api.unusualwhales.com/api/mcp";

std::string trimAscii(const std::string& value) {
    const std::size_t begin = value.find_first_not_of(" \t\r\n");
    if (begin == std::string::npos) {
        return {};
    }
    const std::size_t end = value.find_last_not_of(" \t\r\n");
    return value.substr(begin, end - begin + 1);
}

std::string normalizeSymbolToken(std::string value) {
    value = trimAscii(std::move(value));
    for (char& ch : value) {
        if (ch >= 'a' && ch <= 'z') {
            ch = static_cast<char>(ch - 'a' + 'A');
        }
    }
    return value;
}

std::vector<std::string> splitCsv(const std::string& raw) {
    std::vector<std::string> values;
    std::string current;
    std::istringstream stream(raw);
    while (std::getline(stream, current, ',')) {
        current = trimAscii(current);
        if (!current.empty()) {
            values.push_back(current);
        }
    }
    return values;
}

std::pair<std::string, std::string> splitKeyValue(const std::string& raw) {
    const std::size_t separator = raw.find('=');
    if (separator == std::string::npos) {
        return {trimAscii(raw), std::string()};
    }
    return {
        trimAscii(raw.substr(0, separator)),
        trimAscii(raw.substr(separator + 1))
    };
}

std::optional<std::string> normalizeFacet(std::string raw) {
    for (char& ch : raw) {
        if (ch >= 'A' && ch <= 'Z') {
            ch = static_cast<char>(ch - 'A' + 'a');
        }
    }
    if (raw == "option_trades" || raw == "option_trades:symbol" || raw == "options_flow" || raw == "flow") {
        return "options_flow";
    }
    if (raw == "flow-alerts" || raw == "flow_alerts" || raw == "alerts") {
        return "alerts";
    }
    if (raw == "price" || raw == "price:symbol" || raw == "stock_state") {
        return "stock_state";
    }
    if (raw == "gex" || raw == "gex:symbol") {
        return "gex";
    }
    if (raw == "news") {
        return "news";
    }
    return std::nullopt;
}

std::string envOrDefault(const char* key, const char* fallback) {
    if (const char* value = std::getenv(key); value != nullptr && *value != '\0') {
        return value;
    }
    return fallback;
}

json parseCliValue(const std::string& raw) {
    if (raw == "true") {
        return true;
    }
    if (raw == "false") {
        return false;
    }
    if (raw == "null") {
        return nullptr;
    }
    char* end = nullptr;
    const long long integerValue = std::strtoll(raw.c_str(), &end, 10);
    if (end != nullptr && *end == '\0') {
        return integerValue;
    }
    end = nullptr;
    const double doubleValue = std::strtod(raw.c_str(), &end);
    if (end != nullptr && *end == '\0') {
        return doubleValue;
    }
    const json parsed = json::parse(raw, nullptr, false);
    if (!parsed.is_discarded()) {
        return parsed;
    }
    return raw;
}

void ensureCredentialFileFallback() {
    if (const char* path = std::getenv("LONG_CREDENTIAL_FILE"); path != nullptr && *path != '\0') {
        return;
    }
    const fs::path localPath = fs::path(kSourceDir) / ".env.local";
    if (fs::exists(localPath)) {
        (void)::setenv("LONG_CREDENTIAL_FILE", localPath.string().c_str(), 0);
    }
}

std::string payloadPreview(const std::string& body, std::size_t maxBytes = 512U) {
    if (body.size() <= maxBytes) {
        return body;
    }
    return body.substr(0, maxBytes);
}

json parseJsonBody(const std::string& body) {
    const json payload = json::parse(body, nullptr, false);
    return payload.is_discarded() ? json(nullptr) : payload;
}

std::string classifyMcpOutcome(const HttpResponse& response) {
    if (!response.curlOk) {
        return "transport_error";
    }
    if (response.statusCode == 401 || response.statusCode == 403) {
        return "auth_error";
    }
    if (response.statusCode < 200 || response.statusCode >= 300) {
        return "http_error";
    }
    return response.body.find("\"error\"") != std::string::npos ? "mcp_error" : "success";
}

std::string buildInitializeRequest() {
    return R"({"jsonrpc":"2.0","id":"init-1","method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"long-uw-mcp-smoke","version":"0.1"}}})";
}

std::string buildToolsListRequest() {
    return R"({"jsonrpc":"2.0","id":"list-1","method":"tools/list","params":{}})";
}

std::string buildToolsCallRequest(const std::string& toolName,
                                  const json& arguments) {
    return json{
        {"jsonrpc", "2.0"},
        {"id", "call-1"},
        {"method", "tools/call"},
        {"params", {{"name", toolName}, {"arguments", arguments.is_object() ? arguments : json::object()}}}
    }.dump();
}

std::vector<json> listTools(const json& responsePayload) {
    const json result = responsePayload.value("result", json::object());
    const json tools = result.value("tools", json::array());
    if (!tools.is_array()) {
        return {};
    }
    std::vector<json> out;
    out.reserve(tools.size());
    for (const auto& tool : tools) {
        if (tool.is_object()) {
            out.push_back(tool);
        }
    }
    return out;
}

json summarizeTools(const std::vector<json>& tools) {
    json out = json::array();
    for (const auto& tool : tools) {
        out.push_back({
            {"name", tool.value("name", std::string())},
            {"title", tool.value("title", std::string())},
            {"description", tool.value("description", std::string())}
        });
    }
    return out;
}

json summarizeHttpResponse(const HttpResponse& response, const std::string& outcome) {
    return {
        {"outcome", outcome},
        {"status_code", response.statusCode},
        {"latency_ms", response.latencyMs},
        {"curl_ok", response.curlOk},
        {"curl_error", response.curlError.empty() ? json(nullptr) : json(response.curlError)},
        {"body_preview", payloadPreview(response.body)}
    };
}

json providerStepToJson(const ProviderStep& step) {
    return {
        {"provider", step.provider},
        {"status", step.status},
        {"reason", step.reason.empty() ? json(nullptr) : json(step.reason)},
        {"latency_ms", step.latencyMs},
        {"request_payload", step.requestPayload},
        {"metadata", step.metadata},
        {"raw_records", step.rawRecords}
    };
}

void printUsage(const char* argv0) {
    std::cout
        << "Usage:\n"
        << "  " << argv0 << " [--symbol SYMBOL] [--facets options_flow,alerts,stock_state,news,gex]\n"
        << "               [--list-tools] [--tool TOOL_NAME] [--timeout-ms N]\n"
        << "               [--ticker-field ticker] [--arg key=value] [--skip-connector]\n\n"
        << "Notes:\n"
        << "  - Hits the real UW MCP endpoint at " << kDefaultUrl << " unless LONG_UW_MCP_URL overrides it.\n"
        << "  - Loads credentials from LONG_CREDENTIAL_FILE or " << kSourceDir << "/.env.local.\n"
        << "  - Reads UW_API_TOKEN / UW_BEARER_TOKEN for bearer auth.\n"
        << "  - Without --tool, it picks one tool per requested facet using the same selection logic as long.\n"
        << "  - Repeated --arg key=value pairs override the default ticker argument for explicit tool calls.\n"
        << "  - With --list-tools, it only performs initialize + tools/list.\n"
        << "  - Docs: " << kDocsUrl << "\n";
}

} // namespace

int main(int argc, char** argv) {
    std::string symbol = "SPY";
    std::vector<std::string> facets = {"options_flow", "alerts", "stock_state", "news", "gex"};
    bool listOnly = false;
    bool skipConnector = false;
    std::optional<std::string> explicitTool;
    std::string tickerField = "ticker";
    json explicitArguments = json::object();
    long timeoutMs = 12000L;

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--help" || arg == "-h") {
            printUsage(argv[0]);
            return 0;
        }
        if (arg == "--symbol" && i + 1 < argc) {
            symbol = normalizeSymbolToken(argv[++i]);
            continue;
        }
        if (arg == "--facets" && i + 1 < argc) {
            std::vector<std::string> parsed;
            for (const std::string& rawFacet : splitCsv(argv[++i])) {
                const std::optional<std::string> normalized = normalizeFacet(rawFacet);
                if (!normalized.has_value()) {
                    std::cerr << "Unsupported MCP facet: " << rawFacet << "\n";
                    return 2;
                }
                parsed.push_back(*normalized);
            }
            if (!parsed.empty()) {
                facets = std::move(parsed);
            }
            continue;
        }
        if (arg == "--list-tools") {
            listOnly = true;
            continue;
        }
        if (arg == "--tool" && i + 1 < argc) {
            explicitTool = argv[++i];
            continue;
        }
        if (arg == "--timeout-ms" && i + 1 < argc) {
            timeoutMs = std::strtol(argv[++i], nullptr, 10);
            continue;
        }
        if (arg == "--ticker-field" && i + 1 < argc) {
            tickerField = argv[++i];
            continue;
        }
        if (arg == "--arg" && i + 1 < argc) {
            const auto [key, value] = splitKeyValue(argv[++i]);
            if (key.empty()) {
                std::cerr << "Expected non-empty --arg key=value pair\n";
                return 2;
            }
            explicitArguments[key] = parseCliValue(value);
            continue;
        }
        if (arg == "--skip-connector") {
            skipConnector = true;
            continue;
        }
        std::cerr << "Unknown argument: " << arg << "\n";
        printUsage(argv[0]);
        return 2;
    }

    ensureCredentialFileFallback();
    const CredentialBinding credential = uw_context_service::loadCredentialBinding({"UW_API_TOKEN", "UW_BEARER_TOKEN"});
    if (!credential.present) {
        std::cerr << "Missing UW bearer credentials. Set UW_API_TOKEN or UW_BEARER_TOKEN.\n";
        return 3;
    }

    const std::string url = envOrDefault("LONG_UW_MCP_URL", kDefaultUrl);
    const std::vector<std::string> headers = {
        "Content-Type: application/json",
        "Authorization: Bearer " + credential.value,
    };

    json output = {
        {"url", url},
        {"credential_env", credential.sourceEnv},
        {"symbol", symbol},
        {"requested_facets", facets},
        {"list_tools_only", listOnly},
        {"requested_tool", explicitTool.has_value() ? json(*explicitTool) : json(nullptr)},
        {"timeout_ms", timeoutMs}
    };

    const HttpResponse initialize = uw_context_service::httpPostJson(url, headers, buildInitializeRequest(), timeoutMs);
    const std::string initializeOutcome = classifyMcpOutcome(initialize);
    output["initialize"] = summarizeHttpResponse(initialize, initializeOutcome);
    if (initializeOutcome != "success") {
        std::cout << output.dump(2) << std::endl;
        return 4;
    }

    const HttpResponse toolsList = uw_context_service::httpPostJson(url, headers, buildToolsListRequest(), timeoutMs);
    const std::string listOutcome = classifyMcpOutcome(toolsList);
    output["tools_list"] = summarizeHttpResponse(toolsList, listOutcome);
    if (listOutcome != "success") {
        std::cout << output.dump(2) << std::endl;
        return 5;
    }

    const std::vector<json> tools = listTools(parseJsonBody(toolsList.body));
    output["tool_count"] = tools.size();
    output["tools"] = summarizeTools(tools);
    json toolCatalog = json::array();
    for (const auto& tool : tools) {
        toolCatalog.push_back(tool);
    }
    const UWMcpFacetResolution resolution = uw_context_service::resolveUWMcpToolsForFacets(toolCatalog, facets);
    output["facet_resolution"] = {
        {"selected_tools", json::array()},
        {"unsupported_facets", resolution.unsupportedFacets},
        {"facet_diagnostics", resolution.facetDiagnostics}
    };
    for (const auto& selection : resolution.selectedTools) {
        output["facet_resolution"]["selected_tools"].push_back({
            {"facet", selection.facet},
            {"tool_name", selection.toolName},
            {"score", selection.score},
            {"rationale", selection.rationale}
        });
    }

    if (!listOnly) {
        std::vector<std::pair<std::string, std::string>> selectedTools;
        if (explicitTool.has_value()) {
            selectedTools.emplace_back("explicit", *explicitTool);
        } else {
            for (const auto& selection : resolution.selectedTools) {
                selectedTools.emplace_back(selection.facet, selection.toolName);
            }
        }

        json selectedJson = json::array();
        json callResults = json::array();
        for (const auto& [facet, toolName] : selectedTools) {
            json callArguments = explicitArguments.is_object() ? explicitArguments : json::object();
            if (!callArguments.contains(tickerField) &&
                !callArguments.contains("ticker") &&
                !callArguments.contains("symbol")) {
                callArguments[tickerField] = symbol;
            }
            selectedJson.push_back({{"facet", facet}, {"tool_name", toolName}});
            const HttpResponse call = uw_context_service::httpPostJson(
                url,
                headers,
                buildToolsCallRequest(toolName, callArguments),
                timeoutMs);
            const std::string callOutcome = classifyMcpOutcome(call);
            callResults.push_back({
                {"facet", facet},
                {"tool_name", toolName},
                {"arguments", callArguments},
                {"response", summarizeHttpResponse(call, callOutcome)},
                {"parsed_body", parseJsonBody(call.body)}
            });
        }
        output["selected_tools"] = std::move(selectedJson);
        output["tool_calls"] = std::move(callResults);
    }

    if (!skipConnector) {
        FetchPlan plan;
        plan.artifactId = "uw-mcp-smoke";
        plan.requestKind = "refresh_external_context";
        plan.symbol = symbol;
        plan.forceRefresh = true;
        plan.facets = facets;
        output["connector_step"] = providerStepToJson(UWMcpConnector().fetch(plan));
    }

    std::cout << output.dump(2) << std::endl;
    return 0;
}
