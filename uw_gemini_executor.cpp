#include "uw_gemini_executor.h"

#include "uw_http.h"
#include "uw_runtime.h"

#include <nlohmann/json.hpp>

#include <cctype>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <vector>

#include <sys/wait.h>

namespace uw_context_service {
namespace {

using json = nlohmann::json;

constexpr const char* kGeminiGenerateContentBase =
    "https://generativelanguage.googleapis.com/v1beta/models/";

#if defined(TWS_GEMINI_SDK_SIDECAR_EXECUTABLE_PATH)
constexpr const char* kGeminiSdkSidecarExecutablePath = TWS_GEMINI_SDK_SIDECAR_EXECUTABLE_PATH;
#else
constexpr const char* kGeminiSdkSidecarExecutablePath = "";
#endif

json fastSchema();
json deepSchema();

std::string laneModel(const BuildRequest&) {
    if (const char* model = std::getenv("LONG_GEMINI_MODEL"); model != nullptr && *model != '\0') {
        return model;
    }
    return "gemini-3.1-flash-lite-preview";
}

json laneSchema(const BuildRequest& request) {
    return request.lane == Lane::Deep ? deepSchema() : fastSchema();
}

std::string lanePrompt(const json& packetArtifact, const BuildRequest& request, bool strictJsonOnly) {
    const std::string schemaText = laneSchema(request).dump();
    const std::string focusQuestion = request.focusQuestion.empty()
        ? std::string()
        : ("\nOperator focus question: " + request.focusQuestion + "\nPrioritize answering this question using only evidence present in the packet.");
    if (request.lane == Lane::Deep) {
        return std::string("You are evaluating a backend-built deep incident context packet. ") +
            "Use only the evidence present in the packet. Return JSON matching the required schema." +
            (strictJsonOnly ? " Return only raw JSON with no markdown, no prose, and no code fences." : "") +
            " Schema: " + schemaText +
            focusQuestion +
            "\n\n" +
            packetArtifact.dump();
    }
    return std::string("You are evaluating a backend-built fast incident context packet. ") +
        "Use only the evidence present in the packet. Return JSON matching the required schema." +
        (strictJsonOnly ? " Return only raw JSON with no markdown, no prose, and no code fences." : "") +
        " Schema: " + schemaText +
        focusQuestion +
        "\n\n" +
        packetArtifact.dump();
}

json fastSchema() {
    return {
        {"type", "object"},
        {"additionalProperties", false},
        {"required", json::array({"summary", "tags", "top_evidence", "confidence", "warnings"})},
        {"properties", {
            {"summary", json{{"type", "string"}}},
            {"tags", json{{"type", "array"}, {"items", json{{"type", "string"}}}}},
            {"top_evidence", json{{"type", "array"}, {"items", json{{"type", "string"}}}}},
            {"confidence", json{{"type", "number"}, {"minimum", 0.0}, {"maximum", 1.0}}},
            {"warnings", json{{"type", "array"}, {"items", json{{"type", "string"}}}}}
        }}
    };
}

json deepSchema() {
    return {
        {"type", "object"},
        {"additionalProperties", false},
        {"required", json::array({"headline", "why_it_matters", "ranked_causes", "counterevidence", "confidence", "next_questions", "warnings"})},
        {"properties", {
            {"headline", json{{"type", "string"}}},
            {"why_it_matters", json{{"type", "string"}}},
            {"ranked_causes", json{{"type", "array"}, {"items", json{{"type", "object"}, {"required", json::array({"cause", "score", "support"})}, {"properties", {{"cause", json{{"type", "string"}}}, {"score", json{{"type", "number"}, {"minimum", 0.0}, {"maximum", 1.0}}}, {"support", json{{"type", "array"}, {"items", json{{"type", "string"}}}}}}}, {"additionalProperties", false}}}}},
            {"counterevidence", json{{"type", "array"}, {"items", json{{"type", "string"}}}}},
            {"confidence", json{{"type", "number"}, {"minimum", 0.0}, {"maximum", 1.0}}},
            {"next_questions", json{{"type", "array"}, {"items", json{{"type", "string"}}}}},
            {"warnings", json{{"type", "array"}, {"items", json{{"type", "string"}}}}}
        }}
    };
}

std::string trimAscii(std::string value) {
    auto isSpace = [](unsigned char ch) {
        return std::isspace(ch) != 0;
    };
    while (!value.empty() && isSpace(static_cast<unsigned char>(value.front()))) {
        value.erase(value.begin());
    }
    while (!value.empty() && isSpace(static_cast<unsigned char>(value.back()))) {
        value.pop_back();
    }
    return value;
}

std::string shellEscape(const std::string& value) {
    std::string out = "'";
    for (char ch : value) {
        if (ch == '\'') {
            out += "'\\''";
        } else {
            out.push_back(ch);
        }
    }
    out.push_back('\'');
    return out;
}

bool envFlagEnabled(const char* key, bool defaultValue) {
    if (const char* raw = std::getenv(key); raw != nullptr && *raw != '\0') {
        std::string normalized = trimAscii(raw);
        for (char& ch : normalized) {
            ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
        }
        return normalized == "1" || normalized == "true" || normalized == "yes" || normalized == "on";
    }
    return defaultValue;
}

std::string geminiSdkSidecarPath() {
    if (const char* overridePath = std::getenv("LONG_GEMINI_SDK_SIDECAR_PATH"); overridePath != nullptr && *overridePath != '\0') {
        return overridePath;
    }
    return kGeminiSdkSidecarExecutablePath;
}

int systemExitCode(int rawStatus) {
    if (rawStatus == -1) {
        return -1;
    }
    if (WIFEXITED(rawStatus)) {
        return WEXITSTATUS(rawStatus);
    }
    return rawStatus;
}

std::string readCommandOutput(const std::string& command, int& exitCode) {
    FILE* pipe = popen(command.c_str(), "r");
    if (pipe == nullptr) {
        exitCode = -1;
        return {};
    }
    std::string output;
    char buffer[4096];
    while (fgets(buffer, sizeof(buffer), pipe) != nullptr) {
        output += buffer;
    }
    exitCode = systemExitCode(pclose(pipe));
    return output;
}

json parseCandidateText(std::string text) {
    text = trimAscii(std::move(text));
    if (text.empty()) {
        return json(nullptr);
    }

    json parsed = json::parse(text, nullptr, false);
    if (!parsed.is_discarded()) {
        return parsed;
    }

    if (text.rfind("```", 0) == 0) {
        const std::size_t firstNewline = text.find('\n');
        if (firstNewline != std::string::npos) {
            const std::size_t closingFence = text.rfind("```");
            if (closingFence != std::string::npos && closingFence > firstNewline) {
                parsed = json::parse(trimAscii(text.substr(firstNewline + 1,
                                                          closingFence - firstNewline - 1)),
                                     nullptr,
                                     false);
                if (!parsed.is_discarded()) {
                    return parsed;
                }
            }
        }
    }

    const std::size_t objectStart = text.find('{');
    const std::size_t objectEnd = text.rfind('}');
    if (objectStart != std::string::npos && objectEnd != std::string::npos && objectEnd > objectStart) {
        parsed = json::parse(text.substr(objectStart, objectEnd - objectStart + 1), nullptr, false);
        if (!parsed.is_discarded()) {
            return parsed;
        }
    }

    const std::size_t arrayStart = text.find('[');
    const std::size_t arrayEnd = text.rfind(']');
    if (arrayStart != std::string::npos && arrayEnd != std::string::npos && arrayEnd > arrayStart) {
        parsed = json::parse(text.substr(arrayStart, arrayEnd - arrayStart + 1), nullptr, false);
        if (!parsed.is_discarded()) {
            return parsed;
        }
    }

    return json(nullptr);
}

std::string buildRequestBody(const json& packetArtifact,
                             const BuildRequest& request,
                             bool includeSchema,
                             bool strictJsonOnly) {
    const long maxOutputTokens = request.lane == Lane::Deep ? 3200L : 400L;
    json generationConfig{
        {"temperature", strictJsonOnly ? 0.0 : 0.2},
        {"candidateCount", 1},
        {"maxOutputTokens", maxOutputTokens},
        {"responseMimeType", "application/json"}
    };
    if (includeSchema) {
        generationConfig["responseSchema"] = laneSchema(request);
    }
    return json{
        {"contents", json::array({json{{"parts", json::array({json{{"text", lanePrompt(packetArtifact, request, strictJsonOnly)}}})}}})},
        {"generationConfig", std::move(generationConfig)}
    }.dump();
}

std::string generateContentUrl(const BuildRequest& request) {
    return std::string(kGeminiGenerateContentBase) + laneModel(request) + ":generateContent";
}

json extractContent(const json& responseJson) {
    const json candidates = responseJson.value("candidates", json::array());
    if (!candidates.is_array() || candidates.empty()) {
        return json(nullptr);
    }
    const json parts = candidates.at(0).value("content", json::object()).value("parts", json::array());
    if (!parts.is_array() || parts.empty()) {
        return json(nullptr);
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
    if (text.empty()) {
        return json(nullptr);
    }
    return parseCandidateText(std::move(text));
}

std::string extractRawText(const json& responseJson) {
    const json candidates = responseJson.value("candidates", json::array());
    if (!candidates.is_array() || candidates.empty()) {
        return std::string();
    }
    const json parts = candidates.at(0).value("content", json::object()).value("parts", json::array());
    if (!parts.is_array() || parts.empty()) {
        return std::string();
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

bool validateFastContent(const json& content) {
    return content.is_object() && content.contains("summary") && content.contains("tags") &&
        content.contains("top_evidence") && content.contains("confidence") && content.contains("warnings");
}

bool validateDeepContent(const json& content) {
    return content.is_object() && content.contains("headline") && content.contains("why_it_matters") &&
        content.contains("ranked_causes") && content.contains("counterevidence") &&
        content.contains("confidence") && content.contains("next_questions") && content.contains("warnings");
}

GeminiExecutionResult runGeminiSdkSidecarAttempt(const json& packetArtifact,
                                                 const BuildRequest& request,
                                                 bool strictJsonOnly) {
    GeminiExecutionResult result;
    result.model = laneModel(request);

    const std::string executable = geminiSdkSidecarPath();
    if (executable.empty() || !std::filesystem::exists(executable)) {
        result.error = "sdk_sidecar_unavailable";
        return result;
    }

    const std::string command = shellEscape(executable) + " --model " + shellEscape(result.model) + " " +
        shellEscape(lanePrompt(packetArtifact, request, strictJsonOnly));
    int exitCode = -1;
    const auto start = std::chrono::steady_clock::now();
    const std::string output = readCommandOutput(command, exitCode);
    const auto stop = std::chrono::steady_clock::now();
    result.latencyMs = static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(stop - start).count());
    if (exitCode != 0) {
        result.error = "sdk_sidecar_exit_" + std::to_string(exitCode);
        result.rawText = output;
        return result;
    }

    const json sidecarPayload = json::parse(output, nullptr, false);
    if (sidecarPayload.is_discarded() || !sidecarPayload.is_object()) {
        result.error = "sdk_sidecar_response_parse_failed";
        result.rawText = output;
        return result;
    }
    result.latencyMs = sidecarPayload.value("elapsed_ms", result.latencyMs);
    const std::string stderrText = sidecarPayload.value("stderr", std::string());
    if (!stderrText.empty()) {
        result.warnings.push_back(stderrText);
    }
    const std::string answerText = sidecarPayload.value("stdout", std::string());
    result.rawText = answerText;
    const json content = parseCandidateText(answerText);
    result.jsonValid = !content.is_discarded() && !content.is_null();
    result.content = result.jsonValid ? content : json(nullptr);
    result.schemaValid = result.jsonValid &&
        (request.lane == Lane::Deep ? validateDeepContent(content) : validateFastContent(content));
    result.status = result.schemaValid ? "completed" : (result.jsonValid ? "invalid_schema" : "invalid_json");
    if (!result.jsonValid) {
        result.error = "candidate_text_missing_or_invalid_json";
    }
    return result;
}

GeminiExecutionResult runGeminiAttempt(const json& packetArtifact,
                                       const BuildRequest& request,
                                       const CredentialBinding& binding,
                                       bool includeSchema,
                                       bool strictJsonOnly) {
    GeminiExecutionResult result;
    result.model = laneModel(request);

    const std::vector<std::string> headers = {
        "Content-Type: application/json",
        "x-goog-api-key: " + binding.value,
    };
    const HttpResponse response = httpPostJson(generateContentUrl(request),
                                               headers,
                                               buildRequestBody(packetArtifact, request, includeSchema, strictJsonOnly),
                                               request.lane == Lane::Deep ? 20000L : 15000L);
    result.latencyMs = response.latencyMs;
    if (!httpSuccess(response)) {
        result.error = response.curlOk ? ("http_" + std::to_string(response.statusCode)) : response.curlError;
        result.rawText = response.body;
        return result;
    }

    const json responseJson = json::parse(response.body, nullptr, false);
    if (responseJson.is_discarded()) {
        result.error = "response_parse_failed";
        result.rawText = response.body;
        return result;
    }

    result.finishReason = responseJson.value("candidates", json::array()).is_array() &&
            !responseJson.value("candidates", json::array()).empty()
        ? responseJson.value("candidates", json::array()).at(0).value("finishReason", std::string())
        : std::string();
    result.rawText = extractRawText(responseJson);
    const json content = extractContent(responseJson);
    result.jsonValid = !content.is_discarded() && !content.is_null();
    result.content = result.jsonValid ? content : json(nullptr);
    result.schemaValid = result.jsonValid &&
        (request.lane == Lane::Deep ? validateDeepContent(content) : validateFastContent(content));
    result.status = result.schemaValid ? "completed" : (result.jsonValid ? "invalid_schema" : "invalid_json");
    if (!result.jsonValid) {
        result.error = "candidate_text_missing_or_invalid_json";
    }
    return result;
}

} // namespace

GeminiExecutionResult executeGeminiPacket(const json& packetArtifact,
                                         const BuildRequest& request,
                                         bool allowExecution) {
    GeminiExecutionResult result;
    result.model = laneModel(request);
    if (!allowExecution) {
        result.error = "execution_disabled";
        return result;
    }
    const CredentialBinding binding = loadCredentialBinding({"GEMINI_API_KEY"});
    if (!binding.present) {
        result.error = "missing_credentials";
        return result;
    }

    if (envFlagEnabled("LONG_GEMINI_USE_SDK_SIDECAR", true)) {
        result = runGeminiSdkSidecarAttempt(packetArtifact, request, false);
        if (result.status == "completed") {
            return result;
        }
        if (result.status == "invalid_json" || result.status == "invalid_schema") {
            GeminiExecutionResult retry = runGeminiSdkSidecarAttempt(packetArtifact, request, true);
            retry.warnings.push_back("initial_result_" + result.status);
            if (retry.status == "completed") {
                retry.warnings.push_back("recovered_after_retry");
                return retry;
            }
            result = std::move(retry);
        }
        result.warnings.push_back("sdk_sidecar_fallback_to_direct_http");
    }

    result = runGeminiAttempt(packetArtifact, request, binding, true, false);
    if (result.error == "http_400") {
        GeminiExecutionResult retry = runGeminiAttempt(packetArtifact, request, binding, false, true);
        retry.warnings.push_back("initial_request_http_400");
        return retry;
    }
    if (result.status == "invalid_json" || result.status == "invalid_schema") {
        GeminiExecutionResult retry = runGeminiAttempt(packetArtifact, request, binding, false, true);
        retry.warnings.push_back("initial_result_" + result.status);
        if (retry.status == "completed") {
            retry.warnings.push_back("recovered_after_retry");
        }
        return retry;
    }
    return result;
}

} // namespace uw_context_service
