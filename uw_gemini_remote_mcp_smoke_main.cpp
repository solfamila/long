#include "uw_http.h"
#include "uw_runtime.h"

#include <cstdlib>
#include <filesystem>
#include <iostream>
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
constexpr const char* kDefaultModel = "gemini-3.1-flash";
constexpr const char* kGeminiGenerateContentBase =
    "https://generativelanguage.googleapis.com/v1beta/models/";
constexpr const char* kDefaultUwUrl = "https://api.unusualwhales.com/api/mcp";

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

std::string payloadPreview(const std::string& body, std::size_t maxBytes = 1024U) {
    if (body.size() <= maxBytes) {
        return body;
    }
    return body.substr(0, maxBytes);
}

std::string generateContentUrl(const std::string& model) {
    return std::string(kGeminiGenerateContentBase) + model + ":generateContent";
}

json buildRequestBody(const std::string& prompt,
                      const std::string& uwUrl,
                      const std::string& uwToken) {
    return {
        {"systemInstruction", {
            {"parts", json::array({
                json{{"text",
                      "You are a market-data assistant. Use the remote UW MCP server when the user asks for unusual options flow, alerts, or price-state information. If the MCP server does not expose a requested capability, say that clearly."}}
            })}
        }},
        {"contents", json::array({
            json{{"role", "user"}, {"parts", json::array({json{{"text", prompt}}})}}
        })},
        {"tools", json::array({
            json{{"mcpServers", json::array({
                json{{"name", "uw"},
                     {"streamableHttpTransport", {
                         {"url", uwUrl},
                         {"headers", json{{"AUTHORIZATION", "Bearer " + uwToken}}},
                         {"timeout", "15s"}
                     }}}
            })}}
        })}
    };
}

void printUsage(const char* argv0) {
    std::cout
        << "Usage:\n"
        << "  " << argv0 << " [--model MODEL] [--uw-url URL] [--timeout-ms N] [--json] [PROMPT]\n\n"
        << "Notes:\n"
        << "  - Mirrors the Python remote-MCP path by sending Gemini a direct mcpServers config.\n"
        << "  - This does not perform any app-side tools/list or tools/call mediation.\n"
        << "  - Defaults to model " << kDefaultModel << ".\n";
}

} // namespace

int main(int argc, char** argv) {
    std::string model = kDefaultModel;
    std::string uwUrl = envOrDefault("LONG_UW_MCP_URL", kDefaultUwUrl);
    long timeoutMs = 30000L;
    bool jsonOutput = false;
    std::string prompt = "Use the UW MCP tools to summarize unusual options flow in INTC today.";
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
        if (arg == "--timeout-ms" && i + 1 < argc) {
            timeoutMs = std::strtol(argv[++i], nullptr, 10);
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

    const std::vector<std::string> headers = {
        "Content-Type: application/json",
        "x-goog-api-key: " + geminiCredential.value,
    };
    const json requestBody = buildRequestBody(prompt, uwUrl, uwCredential.value);
    const HttpResponse response = uw_context_service::httpPostJson(
        generateContentUrl(model),
        headers,
        requestBody.dump(),
        timeoutMs);

    json output = {
        {"model", model},
        {"uw_url", uwUrl},
        {"prompt", prompt},
        {"gemini_credential_env", geminiCredential.sourceEnv},
        {"uw_credential_env", uwCredential.sourceEnv},
        {"http_status", response.statusCode},
        {"curl_ok", response.curlOk},
        {"curl_error", response.curlError.empty() ? json(nullptr) : json(response.curlError)},
        {"latency_ms", response.latencyMs},
        {"body_preview", payloadPreview(response.body)}
    };

    const json parsed = json::parse(response.body, nullptr, false);
    if (!parsed.is_discarded()) {
        output["response_json"] = parsed;
    }

    if (jsonOutput || !uw_context_service::httpSuccess(response)) {
        std::cout << output.dump(2) << std::endl;
        return uw_context_service::httpSuccess(response) ? 0 : 5;
    }

    const json candidates = parsed.value("candidates", json::array());
    std::string text;
    if (candidates.is_array() && !candidates.empty()) {
        const json parts = candidates.at(0).value("content", json::object()).value("parts", json::array());
        if (parts.is_array()) {
            for (const auto& part : parts) {
                const std::string fragment = part.value("text", std::string());
                if (!fragment.empty()) {
                    if (!text.empty()) {
                        text.push_back('\n');
                    }
                    text += fragment;
                }
            }
        }
    }
    if (!text.empty()) {
        std::cout << text << std::endl;
    } else {
        std::cout << output.dump(2) << std::endl;
    }
    return 0;
}