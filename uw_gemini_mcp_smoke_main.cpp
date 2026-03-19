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

std::string trimAscii(const std::string& value) {
    const std::size_t begin = value.find_first_not_of(" \t\r\n");
    if (begin == std::string::npos) {
        return {};
    }
    const std::size_t end = value.find_last_not_of(" \t\r\n");
    return value.substr(begin, end - begin + 1);
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

std::string payloadPreview(const std::string& body, std::size_t maxBytes = 512U) {
    if (body.size() <= maxBytes) {
        return body;
    }
    return body.substr(0, maxBytes);
}

std::string buildInitializeRequest() {
    return R"({"jsonrpc":"2.0","id":"init-1","method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"long-uw-gemini-mcp-smoke","version":"0.1"}}})";
}

std::string buildToolsListRequest() {
    return R"({"jsonrpc":"2.0","id":"list-1","method":"tools/list","params":{}})";
}

std::string buildToolsCallRequest(const std::string& toolName, const json& arguments) {
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

std::string geminiUrlForModel(const std::string& model) {
    return std::string(kGeminiGenerateContentBase) + model + ":generateContent";
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
        if (const auto it = schema.find("minItems"); it != schema.end() && it->is_number_integer()) {
            out["minItems"] = *it;
        }
        if (const auto it = schema.find("maxItems"); it != schema.end() && it->is_number_integer()) {
            out["maxItems"] = *it;
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
        json declaration = {
            {"name", name},
            {"description", tool.value("description", std::string())}
        };
        json parameters = sanitizeGeminiSchema(tool.value("inputSchema", json::object()));
        if (!parameters.is_object()) {
            parameters = json::object();
        }
        if (parameters.empty()) {
            parameters = json{{"type", "object"}, {"properties", json::object()}, {"required", json::array()}};
        }
        declaration["parameters"] = std::move(parameters);
        functionDeclarations.push_back(std::move(declaration));
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
        if (fragment.empty()) {
            continue;
        }
        if (!text.empty()) {
            text.push_back('\n');
        }
        text += fragment;
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

void printUsage(const char* argv0) {
    std::cout
        << "Usage:\n"
        << "  " << argv0 << " [--model MODEL] [--uw-url URL] [--gemini-timeout-ms N] [--uw-timeout-ms N]\n"
        << "               [--max-turns N] [--allowed-tools tool1,tool2] [--json] [PROMPT]\n\n"
        << "Notes:\n"
        << "  - Performs initialize + tools/list against the real UW MCP endpoint and exposes those tools to Gemini.\n"
        << "  - Runs a manual C++ function-calling loop over Gemini REST; no Python SDK is used.\n"
        << "  - Loads GEMINI_API_KEY and UW_API_TOKEN/UW_BEARER_TOKEN from LONG_CREDENTIAL_FILE or " << kSourceDir << "/.env.local.\n";
}

} // namespace

int main(int argc, char** argv) {
    std::string prompt = "Use get_market_state to summarize the latest market-wide options snapshot in one short paragraph.";
    std::string model = kDefaultModel;
    std::string uwUrl = envOrDefault("LONG_UW_MCP_URL", kDefaultUwUrl);
    long geminiTimeoutMs = 30000L;
    long uwTimeoutMs = 12000L;
    long maxTurns = 6L;
    bool jsonOutput = false;
    std::set<std::string> allowedToolNames;

    bool promptExplicitlySet = false;
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
        if (arg == "--gemini-timeout-ms" && i + 1 < argc) {
            geminiTimeoutMs = std::strtol(argv[++i], nullptr, 10);
            continue;
        }
        if (arg == "--uw-timeout-ms" && i + 1 < argc) {
            uwTimeoutMs = std::strtol(argv[++i], nullptr, 10);
            continue;
        }
        if (arg == "--max-turns" && i + 1 < argc) {
            maxTurns = std::strtol(argv[++i], nullptr, 10);
            continue;
        }
        if (arg == "--allowed-tools" && i + 1 < argc) {
            for (const std::string& toolName : splitCsv(argv[++i])) {
                allowedToolNames.insert(toolName);
            }
            continue;
        }
        if (arg == "--json") {
            jsonOutput = true;
            continue;
        }
        if (!promptExplicitlySet) {
            prompt = arg;
            promptExplicitlySet = true;
            continue;
        }
        std::cerr << "Unknown argument: " << arg << "\n";
        printUsage(argv[0]);
        return 2;
    }

    ensureCredentialFileFallback();
    const CredentialBinding geminiCredential = uw_context_service::loadCredentialBinding({"GEMINI_API_KEY", "GOOGLE_API_KEY"});
    if (!geminiCredential.present) {
        std::cerr << "Missing Gemini API credential. Set GEMINI_API_KEY or GOOGLE_API_KEY.\n";
        return 3;
    }
    const CredentialBinding uwCredential = uw_context_service::loadCredentialBinding({"UW_API_TOKEN", "UW_BEARER_TOKEN"});
    if (!uwCredential.present) {
        std::cerr << "Missing UW bearer credential. Set UW_API_TOKEN or UW_BEARER_TOKEN.\n";
        return 4;
    }

    const std::vector<std::string> uwHeaders = {
        "Content-Type: application/json",
        "Authorization: Bearer " + uwCredential.value,
    };
    json output = {
        {"model", model},
        {"uw_url", uwUrl},
        {"gemini_credential_env", geminiCredential.sourceEnv},
        {"uw_credential_env", uwCredential.sourceEnv},
        {"prompt", prompt},
        {"max_turns", maxTurns}
    };

    const HttpResponse initialize = uw_context_service::httpPostJson(uwUrl, uwHeaders, buildInitializeRequest(), uwTimeoutMs);
    const std::string initializeOutcome = classifyMcpOutcome(initialize);
    output["initialize"] = summarizeHttpResponse(initialize, initializeOutcome);
    if (initializeOutcome != "success") {
        std::cout << output.dump(2) << std::endl;
        return 5;
    }

    const HttpResponse toolsList = uw_context_service::httpPostJson(uwUrl, uwHeaders, buildToolsListRequest(), uwTimeoutMs);
    const std::string listOutcome = classifyMcpOutcome(toolsList);
    output["tools_list"] = summarizeHttpResponse(toolsList, listOutcome);
    if (listOutcome != "success") {
        std::cout << output.dump(2) << std::endl;
        return 6;
    }

    const std::vector<json> tools = listTools(parseJsonBody(toolsList.body));
    const json geminiTools = makeGeminiToolDeclarations(tools, allowedToolNames);
    std::size_t declaredToolCount = 0;
    if (!geminiTools.empty() && geminiTools.front().is_object()) {
        declaredToolCount = geminiTools.front().value("functionDeclarations", json::array()).size();
    }
    output["tool_count"] = tools.size();
    output["declared_tool_count"] = declaredToolCount;
    if (!allowedToolNames.empty()) {
        output["allowed_tools"] = json::array();
        for (const std::string& name : allowedToolNames) {
            output["allowed_tools"].push_back(name);
        }
    }

    json conversation = json::array({
        json{{"role", "user"}, {"parts", json::array({json{{"text", prompt}}})}}
    });
    json toolCallTrace = json::array();
    std::string finalText;

    const std::vector<std::string> geminiHeaders = {
        "Content-Type: application/json",
        "x-goog-api-key: " + geminiCredential.value,
    };

    for (long turn = 1; turn <= maxTurns; ++turn) {
        json requestBody = {
            {"contents", conversation},
            {"tools", geminiTools},
            {"generationConfig", json{{"temperature", 0.0}, {"candidateCount", 1}}}
        };
        const HttpResponse geminiResponse = uw_context_service::httpPostJson(
            geminiUrlForModel(model),
            geminiHeaders,
            requestBody.dump(),
            geminiTimeoutMs);
        json turnSummary = {
            {"turn", turn},
            {"gemini", summarizeHttpResponse(geminiResponse,
                geminiResponse.curlOk && geminiResponse.statusCode >= 200 && geminiResponse.statusCode < 300
                    ? "success"
                    : "error")}
        };
        if (!uw_context_service::httpSuccess(geminiResponse)) {
            output["turns"] = toolCallTrace;
            output["error"] = "gemini_http_error";
            output["failed_turn"] = turnSummary;
            std::cout << output.dump(2) << std::endl;
            return 7;
        }

        const json geminiPayload = parseJsonBody(geminiResponse.body);
        if (geminiPayload.is_null()) {
            output["turns"] = toolCallTrace;
            output["error"] = "gemini_response_parse_failed";
            output["failed_turn"] = turnSummary;
            std::cout << output.dump(2) << std::endl;
            return 8;
        }
        const json modelContent = extractModelContent(geminiPayload);
        if (modelContent.is_null()) {
            output["turns"] = toolCallTrace;
            output["error"] = "gemini_candidate_content_missing";
            output["failed_turn"] = turnSummary;
            output["gemini_payload_preview"] = geminiPayload;
            std::cout << output.dump(2) << std::endl;
            return 9;
        }

        const std::vector<json> functionCalls = extractFunctionCalls(modelContent);
        turnSummary["finish_reason"] = geminiPayload.value("candidates", json::array()).is_array() &&
                !geminiPayload.value("candidates", json::array()).empty()
            ? geminiPayload.value("candidates", json::array()).at(0).value("finishReason", std::string())
            : std::string();
        turnSummary["response_text"] = extractTextFromParts(modelContent);
        turnSummary["function_call_count"] = functionCalls.size();

        if (functionCalls.empty()) {
            finalText = extractTextFromParts(modelContent);
            toolCallTrace.push_back(std::move(turnSummary));
            break;
        }

        conversation.push_back(modelContent);
        json functionResponseParts = json::array();
        json executedCalls = json::array();
        for (const auto& functionCall : functionCalls) {
            const std::string name = functionCall.value("name", std::string());
            json args = functionCall.value("args", json::object());
            if (!args.is_object()) {
                args = json::object();
            }
            const HttpResponse toolResponse = uw_context_service::httpPostJson(
                uwUrl,
                uwHeaders,
                buildToolsCallRequest(name, args),
                uwTimeoutMs);
            const std::string toolOutcome = classifyMcpOutcome(toolResponse);
            const json parsedToolResponse = parseJsonBody(toolResponse.body);
            const json toolResult = {
                {"tool_name", name},
                {"arguments", args},
                {"response", summarizeHttpResponse(toolResponse, toolOutcome)},
                {"parsed_body", parsedToolResponse}
            };
            executedCalls.push_back(toolResult);
            functionResponseParts.push_back(buildFunctionResponsePart(functionCall, toolResult));
        }
        turnSummary["executed_calls"] = std::move(executedCalls);
        toolCallTrace.push_back(std::move(turnSummary));
        conversation.push_back(json{{"role", "user"}, {"parts", std::move(functionResponseParts)}});
    }

    output["turns"] = std::move(toolCallTrace);
    output["final_text"] = finalText;
    output["completed"] = !finalText.empty();
    if (jsonOutput) {
        std::cout << output.dump(2) << std::endl;
    } else if (!finalText.empty()) {
        std::cout << finalText << std::endl;
    } else {
        std::cout << output.dump(2) << std::endl;
    }
    return finalText.empty() ? 10 : 0;
}