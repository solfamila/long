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

#include <nlohmann/json.hpp>

#include <unistd.h>

namespace fs = std::filesystem;

namespace {

using uw_context_service::CredentialBinding;
using uw_context_service::HttpResponse;
using json = nlohmann::json;

constexpr const char* kSourceDir = TWS_GUI_SOURCE_DIR;
constexpr const char* kDefaultModel = "gemini-2.5-flash";
constexpr const char* kGeminiGenerateContentBase =
    "https://generativelanguage.googleapis.com/v1beta/models/";
constexpr const char* kDefaultUwUrl = "https://api.unusualwhales.com/api/mcp";

struct UWMcpSession {
    std::string url;
    std::string authToken;
    std::string sessionId;
    std::string protocolVersion;
    long timeoutMs = 12000L;
    std::uint64_t initializeLatencyMs = 0;
    std::uint64_t initializedNotifyLatencyMs = 0;
    std::uint64_t toolsListLatencyMs = 0;
};

std::string envOrDefault(const char* key, const char* fallback) {
    if (const char* value = std::getenv(key); value != nullptr && *value != '\0') {
        return value;
    }
    return fallback;
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

std::string payloadPreview(const std::string& body, std::size_t maxBytes = 768U) {
    if (body.size() <= maxBytes) {
        return body;
    }
    return body.substr(0, maxBytes);
}

std::vector<std::string> splitCsv(const std::string& raw) {
    std::vector<std::string> values;
    std::string current;
    std::istringstream stream(raw);
    while (std::getline(stream, current, ',')) {
        auto trim = [](std::string value) {
            while (!value.empty() && std::isspace(static_cast<unsigned char>(value.front())) != 0) {
                value.erase(value.begin());
            }
            while (!value.empty() && std::isspace(static_cast<unsigned char>(value.back())) != 0) {
                value.pop_back();
            }
            return value;
        };
        current = trim(std::move(current));
        if (!current.empty()) {
            values.push_back(current);
        }
    }
    return values;
}

json parseJsonBody(const std::string& body) {
    const json payload = json::parse(body, nullptr, false);
    return payload.is_discarded() ? json(nullptr) : payload;
}

std::string generateContentUrl(const std::string& model) {
    return std::string(kGeminiGenerateContentBase) + model + ":generateContent";
}

std::vector<std::string> mcpHeaders(const UWMcpSession& session) {
    std::vector<std::string> headers = {
        "Content-Type: application/json",
        "Accept: application/json, text/event-stream",
        "Authorization: Bearer " + session.authToken,
    };
    if (!session.sessionId.empty()) {
        headers.push_back("Mcp-Session-Id: " + session.sessionId);
    }
    if (!session.protocolVersion.empty()) {
        headers.push_back("MCP-Protocol-Version: " + session.protocolVersion);
    }
    return headers;
}

std::string buildInitializeRequest() {
    return R"({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"long-uw-gemini-session-smoke","version":"0.1"}}})";
}

std::string buildInitializedNotification() {
    return R"({"jsonrpc":"2.0","method":"notifications/initialized","params":{}})";
}

std::string buildToolsListRequest() {
    return R"({"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}})";
}

std::string buildToolsCallRequest(const std::string& toolName, const json& arguments, long requestId) {
    return json{
        {"jsonrpc", "2.0"},
        {"id", requestId},
        {"method", "tools/call"},
        {"params", {{"name", toolName}, {"arguments", arguments}}}
    }.dump();
}

std::vector<json> listTools(const json& responsePayload) {
    const json result = responsePayload.value("result", json::object());
    const json tools = result.value("tools", json::array());
    if (!tools.is_array()) {
        return {};
    }
    std::vector<json> out;
    for (const auto& tool : tools) {
        if (tool.is_object()) {
            out.push_back(tool);
        }
    }
    return out;
}

json sanitizeGeminiSchema(const json& schema) {
    if (schema.is_object()) {
        json out = json::object();
        if (const auto it = schema.find("type"); it != schema.end() && it->is_string()) {
            out["type"] = *it;
        }
        if (const auto it = schema.find("description"); it != schema.end() && it->is_string()) {
            out["description"] = *it;
        }
        if (const auto it = schema.find("format"); it != schema.end() && it->is_string()) {
            out["format"] = *it;
        }
        if (const auto it = schema.find("nullable"); it != schema.end() && it->is_boolean()) {
            out["nullable"] = *it;
        }
        if (const auto it = schema.find("enum"); it != schema.end() && it->is_array()) {
            out["enum"] = *it;
        }
        if (const auto it = schema.find("minimum"); it != schema.end() && it->is_number()) {
            out["minimum"] = *it;
        }
        if (const auto it = schema.find("maximum"); it != schema.end() && it->is_number()) {
            out["maximum"] = *it;
        }
        if (const auto it = schema.find("required"); it != schema.end() && it->is_array()) {
            out["required"] = *it;
        }
        if (const auto it = schema.find("items"); it != schema.end()) {
            out["items"] = sanitizeGeminiSchema(*it);
        }
        if (const auto it = schema.find("properties"); it != schema.end() && it->is_object()) {
            json properties = json::object();
            for (auto propIt = it->begin(); propIt != it->end(); ++propIt) {
                properties[propIt.key()] = sanitizeGeminiSchema(propIt.value());
            }
            out["properties"] = std::move(properties);
        }
        return out;
    }
    if (schema.is_array()) {
        json out = json::array();
        for (const auto& item : schema) {
            out.push_back(sanitizeGeminiSchema(item));
        }
        return out;
    }
    return schema;
}

json makeGeminiToolDeclarations(const std::vector<json>& tools,
                                const std::set<std::string>& allowedToolNames) {
    json functionDeclarations = json::array();
    for (const auto& tool : tools) {
        const std::string name = tool.value("name", std::string());
        if (name.empty()) {
            continue;
        }
        if (!allowedToolNames.empty() && allowedToolNames.count(name) == 0) {
            continue;
        }
        json parameters = sanitizeGeminiSchema(tool.value("inputSchema", json::object()));
        if (!parameters.is_object() || parameters.empty()) {
            parameters = json{{"type", "object"}, {"properties", json::object()}, {"required", json::array()}};
        }
        functionDeclarations.push_back({
            {"name", name},
            {"description", tool.value("description", std::string())},
            {"parameters", parameters}
        });
    }
    return json::array({json{{"functionDeclarations", std::move(functionDeclarations)}}});
}

json extractModelContent(const json& responseJson) {
    const json candidates = responseJson.value("candidates", json::array());
    if (!candidates.is_array() || candidates.empty()) {
        return json(nullptr);
    }
    const json content = candidates.at(0).value("content", json::object());
    return content.is_object() ? content : json(nullptr);
}

std::string extractTextFromParts(const json& content) {
    const json parts = content.value("parts", json::array());
    if (!parts.is_array()) {
        return {};
    }
    std::string text;
    for (const auto& part : parts) {
        const std::string fragment = part.value("text", std::string());
        if (!fragment.empty()) {
            if (!text.empty()) {
                text.push_back('\n');
            }
            text += fragment;
        }
    }
    return text;
}

std::vector<json> extractFunctionCalls(const json& content) {
    std::vector<json> calls;
    const json parts = content.value("parts", json::array());
    if (!parts.is_array()) {
        return calls;
    }
    for (const auto& part : parts) {
        const auto it = part.find("functionCall");
        if (it != part.end() && it->is_object()) {
            calls.push_back(*it);
        }
    }
    return calls;
}

json buildFunctionResponsePart(const json& functionCall, const json& toolResponse) {
    json functionResponse = {
        {"name", functionCall.value("name", std::string())},
        {"response", json{{"result", toolResponse}}}
    };
    if (functionCall.contains("id")) {
        functionResponse["id"] = functionCall.at("id");
    }
    if (functionCall.contains("toolUseId")) {
        functionResponse["toolUseId"] = functionCall.at("toolUseId");
    }
    return json{{"functionResponse", std::move(functionResponse)}};
}

json summarizeHttpResponse(const HttpResponse& response) {
    return {
        {"status_code", response.statusCode},
        {"latency_ms", response.latencyMs},
        {"curl_ok", response.curlOk},
        {"curl_error", response.curlError.empty() ? json(nullptr) : json(response.curlError)},
        {"body_preview", payloadPreview(response.body)}
    };
}

bool initializeSession(UWMcpSession& session, json& diagnostics) {
    const HttpResponse initialize = uw_context_service::httpPostJson(
        session.url,
        mcpHeaders(session),
        buildInitializeRequest(),
        session.timeoutMs);
    session.initializeLatencyMs = initialize.latencyMs;
    diagnostics["initialize"] = summarizeHttpResponse(initialize);
    if (!uw_context_service::httpSuccess(initialize)) {
        return false;
    }
    const json initPayload = parseJsonBody(initialize.body);
    session.protocolVersion = initPayload.value("result", json::object()).value("protocolVersion", std::string());
    const auto it = initialize.headers.find("mcp-session-id");
    if (it != initialize.headers.end()) {
        session.sessionId = it->second;
    }
    diagnostics["session_id"] = session.sessionId.empty() ? json(nullptr) : json(session.sessionId);
    diagnostics["protocol_version"] = session.protocolVersion;
    if (session.protocolVersion.empty()) {
        return false;
    }

    const HttpResponse initialized = uw_context_service::httpPostJson(
        session.url,
        mcpHeaders(session),
        buildInitializedNotification(),
        session.timeoutMs);
    session.initializedNotifyLatencyMs = initialized.latencyMs;
    diagnostics["initialized_notification"] = summarizeHttpResponse(initialized);
    return initialized.curlOk && initialized.statusCode >= 200 && initialized.statusCode < 300;
}

std::vector<json> fetchToolCatalog(UWMcpSession& session, json& diagnostics) {
    const HttpResponse toolsList = uw_context_service::httpPostJson(
        session.url,
        mcpHeaders(session),
        buildToolsListRequest(),
        session.timeoutMs);
    session.toolsListLatencyMs = toolsList.latencyMs;
    diagnostics["tools_list"] = summarizeHttpResponse(toolsList);
    if (!uw_context_service::httpSuccess(toolsList)) {
        return {};
    }
    return listTools(parseJsonBody(toolsList.body));
}

std::string pythonExecutablePath() {
    const fs::path repoRoot = fs::path(kSourceDir);
    const fs::path venvPython = repoRoot / ".venv-gemini-uw" / "bin" / "python";
    if (fs::exists(venvPython)) {
        return venvPython.string();
    }
    return "python3";
}

void printUsage(const char* argv0) {
    std::cout
        << "Usage:\n"
        << "  " << argv0 << " [--model MODEL] [--uw-url URL] [--json] [PROMPT]\n\n"
        << "Notes:\n"
        << "  - Implements MCP session semantics in C++ (initialize, session header, initialized notification).\n"
        << "  - Uses C++ Gemini function orchestration after session-based MCP setup.\n";
}

} // namespace

int main(int argc, char** argv) {
    std::string model = kDefaultModel;
    std::string uwUrl = envOrDefault("LONG_UW_MCP_URL", kDefaultUwUrl);
    bool jsonOutput = false;
    long geminiTimeoutMs = 30000L;
    std::string prompt = "Use get_market_state and tell me the latest market-wide options snapshot.";
    bool promptExplicitlySet = false;
    std::set<std::string> allowedToolNames;

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--help" || arg == "-h") {
            printUsage(argv[0]);
            return 0;
        }
        if (arg == "--model" && i + 1 < argc) {
            model = argv[++i];
            continue;
        }
        if (arg == "--uw-url" && i + 1 < argc) {
            uwUrl = argv[++i];
            continue;
        }
        if (arg == "--json") {
            jsonOutput = true;
            continue;
        }
        if (arg == "--allowed-tools" && i + 1 < argc) {
            for (const std::string& name : splitCsv(argv[++i])) {
                allowedToolNames.insert(name);
            }
            continue;
        }
        if (!promptExplicitlySet) {
            prompt = arg;
            promptExplicitlySet = true;
            continue;
        }
        std::cerr << "Unknown argument: " << arg << "\n";
        return 2;
    }

    ensureCredentialFileFallback();
    const CredentialBinding geminiCredential = uw_context_service::loadCredentialBinding({"GEMINI_API_KEY", "GOOGLE_API_KEY"});
    const CredentialBinding uwCredential = uw_context_service::loadCredentialBinding({"UW_API_TOKEN", "UW_BEARER_TOKEN"});
    if (!geminiCredential.present || !uwCredential.present) {
        std::cerr << "Missing Gemini or UW credentials.\n";
        return 3;
    }

    UWMcpSession session{uwUrl, uwCredential.value};
    json output = {
        {"model", model},
        {"uw_url", uwUrl},
        {"gemini_credential_env", geminiCredential.sourceEnv},
        {"uw_credential_env", uwCredential.sourceEnv},
        {"prompt", prompt}
    };
    if (!initializeSession(session, output)) {
        std::cout << output.dump(2) << std::endl;
        return 4;
    }
    const std::vector<json> tools = fetchToolCatalog(session, output);
    output["tool_count"] = tools.size();
    if (!allowedToolNames.empty()) {
        output["allowed_tools"] = json::array();
        for (const std::string& name : allowedToolNames) {
            output["allowed_tools"].push_back(name);
        }
    }
    if (tools.empty()) {
        std::cout << output.dump(2) << std::endl;
        return 5;
    }

    json conversation = json::array({json{{"role", "user"}, {"parts", json::array({json{{"text", prompt}}})}}});
    const std::vector<std::string> geminiHeaders = {
        "Content-Type: application/json",
        "x-goog-api-key: " + geminiCredential.value,
    };
    json turns = json::array();
    std::string finalText;

    for (long turn = 1; turn <= 6; ++turn) {
        json requestBody = {
            {"contents", conversation},
            {"tools", makeGeminiToolDeclarations(tools, allowedToolNames)},
            {"generationConfig", json{{"temperature", 0.0}, {"candidateCount", 1}}}
        };
        if (!allowedToolNames.empty() && turn == 1) {
            json allowedNames = json::array();
            for (const std::string& name : allowedToolNames) {
                allowedNames.push_back(name);
            }
            requestBody["toolConfig"] = json{{"functionCallingConfig", {{"mode", "ANY"}, {"allowedFunctionNames", std::move(allowedNames)}}}};
        }
        const HttpResponse geminiResponse = uw_context_service::httpPostJson(
            generateContentUrl(model), geminiHeaders, requestBody.dump(), geminiTimeoutMs);
        json turnSummary = {{"turn", turn}, {"gemini", summarizeHttpResponse(geminiResponse)}};
        if (!uw_context_service::httpSuccess(geminiResponse)) {
            output["turns"] = turns;
            output["failed_turn"] = turnSummary;
            std::cout << output.dump(2) << std::endl;
            return 6;
        }
        const json geminiPayload = parseJsonBody(geminiResponse.body);
        const json modelContent = extractModelContent(geminiPayload);
        const std::vector<json> functionCalls = extractFunctionCalls(modelContent);
        turnSummary["function_call_count"] = functionCalls.size();
        turnSummary["response_text"] = extractTextFromParts(modelContent);
        if (functionCalls.empty()) {
            finalText = extractTextFromParts(modelContent);
            turns.push_back(turnSummary);
            break;
        }
        conversation.push_back(modelContent);
        json functionResponseParts = json::array();
        json executedCalls = json::array();
        long requestIdBase = 100 + turn * 10;
        for (std::size_t index = 0; index < functionCalls.size(); ++index) {
            const json& functionCall = functionCalls[index];
            json args = functionCall.value("args", json::object());
            const HttpResponse toolResponse = uw_context_service::httpPostJson(
                session.url,
                mcpHeaders(session),
                buildToolsCallRequest(functionCall.value("name", std::string()), args, requestIdBase + static_cast<long>(index)),
                session.timeoutMs);
            const json parsedTool = parseJsonBody(toolResponse.body);
            const json toolResult = {
                {"tool_name", functionCall.value("name", std::string())},
                {"arguments", args},
                {"response", summarizeHttpResponse(toolResponse)},
                {"parsed_body", parsedTool}
            };
            executedCalls.push_back(toolResult);
            functionResponseParts.push_back(buildFunctionResponsePart(functionCall, toolResult));
        }
        turnSummary["executed_calls"] = executedCalls;
        turns.push_back(turnSummary);
        conversation.push_back(json{{"role", "user"}, {"parts", functionResponseParts}});
    }

    output["final_text"] = finalText;
    output["turns"] = turns;
    output["session_transport"] = {
        {"initialize_latency_ms", session.initializeLatencyMs},
        {"initialized_notify_latency_ms", session.initializedNotifyLatencyMs},
        {"tools_list_latency_ms", session.toolsListLatencyMs}
    };
    if (jsonOutput || finalText.empty()) {
        std::cout << output.dump(2) << std::endl;
    } else {
        std::cout << finalText << std::endl;
    }
    return finalText.empty() ? 7 : 0;
}