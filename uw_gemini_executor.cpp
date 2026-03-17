#include "uw_gemini_executor.h"

#include "uw_http.h"
#include "uw_runtime.h"

#include <nlohmann/json.hpp>

#include <cctype>

namespace uw_context_service {
namespace {

constexpr const char* kGeminiGenerateContentBase =
    "https://generativelanguage.googleapis.com/v1beta/models/";

json fastSchema();
json deepSchema();

std::string laneModel(const BuildRequest&) {
    return "gemini-3.1-flash-lite-preview";
}

json laneSchema(const BuildRequest& request) {
    return request.lane == Lane::Deep ? deepSchema() : fastSchema();
}

std::string lanePrompt(const json& packetArtifact, const BuildRequest& request, bool strictJsonOnly) {
    const std::string schemaText = laneSchema(request).dump();
    if (request.lane == Lane::Deep) {
        return std::string("You are evaluating a backend-built deep incident context packet. ") +
            "Use only the evidence present in the packet. Return JSON matching the required schema." +
            (strictJsonOnly ? " Return only raw JSON with no markdown, no prose, and no code fences." : "") +
            " Schema: " + schemaText +
            "\n\n" +
            packetArtifact.dump();
    }
    return std::string("You are evaluating a backend-built fast incident context packet. ") +
        "Use only the evidence present in the packet. Return JSON matching the required schema." +
        (strictJsonOnly ? " Return only raw JSON with no markdown, no prose, and no code fences." : "") +
        " Schema: " + schemaText +
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
