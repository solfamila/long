#include "uw_context_connectors.h"

#include "uw_http.h"
#include "uw_runtime.h"

#include <cstdlib>
#include <optional>
#include <set>
#include <sstream>
#include <string_view>

namespace uw_context_service {
namespace {

constexpr const char* kUwMcpDefaultUrl = "https://api.unusualwhales.com/api/mcp";
constexpr const char* kUwApiBase = "https://api.unusualwhales.com";

std::string envOrDefault(const char* key, const char* fallback) {
    if (const char* value = std::getenv(key); value != nullptr && *value != '\0') {
        return value;
    }
    return fallback;
}

ProviderStep makeUnavailableStep(std::string provider,
                                 const FetchPlan& plan,
                                 const CredentialBinding& credential,
                                 std::string reason) {
    ProviderStep step;
    step.provider = std::move(provider);
    step.status = "unavailable";
    step.reason = std::move(reason);
    step.requestPayload = {
        {"artifact_id", plan.artifactId},
        {"request_kind", plan.requestKind},
        {"symbol", plan.symbol},
        {"force_refresh", plan.forceRefresh},
        {"evidence_kinds", plan.evidenceKinds}
    };
    step.metadata = {
        {"credential_env", credential.sourceEnv.empty() ? json(nullptr) : json(credential.sourceEnv)},
        {"credential_present", credential.present}
    };
    return step;
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

json parseJsonBody(const std::string& body) {
    const json payload = json::parse(body, nullptr, false);
    return payload.is_discarded() ? json(nullptr) : payload;
}

std::string buildInitializeRequest() {
    return R"({"jsonrpc":"2.0","id":"init-1","method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"long-uw-context","version":"0.1"}}})";
}

std::string buildToolsListRequest() {
    return R"({"jsonrpc":"2.0","id":"list-1","method":"tools/list","params":{}})";
}

std::string buildToolsCallRequest(const std::string& toolName, const std::string& symbol) {
    return json{
        {"jsonrpc", "2.0"},
        {"id", "call-1"},
        {"method", "tools/call"},
        {"params", {{"name", toolName}, {"arguments", {{"ticker", symbol}}}}}
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

bool facetMatchesTool(const std::string& facet, const json& tool) {
    const std::string name = tool.value("name", std::string());
    const std::string description = tool.value("description", std::string());
    const std::string haystack = name + " " + description;
    if (facet == "options_flow") {
        return haystack.find("flow") != std::string::npos || haystack.find("option") != std::string::npos;
    }
    if (facet == "stock_state") {
        return haystack.find("state") != std::string::npos || haystack.find("stock") != std::string::npos;
    }
    if (facet == "news") {
        return haystack.find("news") != std::string::npos;
    }
    if (facet == "gex") {
        return haystack.find("gex") != std::string::npos || haystack.find("gamma") != std::string::npos;
    }
    if (facet == "alerts") {
        return haystack.find("alert") != std::string::npos;
    }
    return false;
}

std::vector<std::pair<std::string, std::string>> selectToolsForFacets(const std::vector<json>& tools,
                                                                      const std::vector<std::string>& facets) {
    std::vector<std::pair<std::string, std::string>> selected;
    std::set<std::string> seen;
    for (const std::string& facet : facets) {
        for (const json& tool : tools) {
            const std::string name = tool.value("name", std::string());
            if (name.empty() || seen.count(name) > 0 || !facetMatchesTool(facet, tool)) {
                continue;
            }
            selected.emplace_back(facet, name);
            seen.insert(name);
            break;
        }
    }
    if (selected.empty() && !tools.empty()) {
        selected.emplace_back("fallback", tools.front().value("name", std::string()));
    }
    return selected;
}

json makeNormalizedRawRecord(const std::string& kind,
                             const std::string& symbol,
                             const std::string& providerRecordId,
                             const HttpResponse& response,
                             const json& structured,
                             const json& extra = json::object()) {
    json record = {
        {"kind", kind},
        {"provider_record_id", providerRecordId},
        {"symbol", symbol},
        {"ts_provider_ns", 0},
        {"ts_fetched_ns", nowUnixSeconds() * 1000000000ULL},
        {"structured", structured.is_null() ? json::object() : structured},
        {"raw_excerpt", payloadPreview(response.body)},
        {"http_status", response.statusCode},
        {"latency_ms", response.latencyMs},
        {"schema", "uw_external_context_record_v1"}
    };
    if (extra.is_object()) {
        for (auto it = extra.begin(); it != extra.end(); ++it) {
            record[it.key()] = it.value();
        }
    }
    return record;
}

std::vector<std::pair<std::string, std::string>> restEndpointsForFacets(const std::string& symbol,
                                                                        const std::vector<std::string>& facets) {
    std::vector<std::pair<std::string, std::string>> endpoints;
    for (const std::string& facet : facets) {
        if (facet == "stock_state") {
            endpoints.emplace_back(facet, std::string(kUwApiBase) + "/api/stock/" + symbol + "/stock-state");
        } else if (facet == "options_flow") {
            endpoints.emplace_back(facet, std::string(kUwApiBase) + "/api/stock/" + symbol + "/flow-recent?limit=25");
        } else if (facet == "news") {
            endpoints.emplace_back(facet, std::string(kUwApiBase) + "/api/news/headlines?symbol=" + symbol + "&limit=10");
        } else if (facet == "alerts") {
            endpoints.emplace_back(facet, std::string(kUwApiBase) + "/api/alerts?symbol=" + symbol + "&limit=10");
        } else if (facet == "gex") {
            endpoints.emplace_back(facet, std::string(kUwApiBase) + "/api/stock/" + symbol + "/gex");
        }
    }
    return endpoints;
}

} // namespace

ProviderStep UWMcpConnector::fetch(const FetchPlan& plan) const {
    const CredentialBinding credential = loadCredentialBinding({"UW_API_TOKEN", "UW_BEARER_TOKEN"});
    if (!credential.present) {
        return makeUnavailableStep("uw_mcp", plan, credential, "missing_credentials");
    }
    if (const char* disabled = std::getenv("LONG_DISABLE_EXTERNAL_CONTEXT"); disabled != nullptr && *disabled == '1') {
        return makeUnavailableStep("uw_mcp", plan, credential, "disabled_by_env");
    }

    ProviderStep step;
    step.provider = "uw_mcp";
    step.status = "unavailable";
    const std::string url = envOrDefault("LONG_UW_MCP_URL", kUwMcpDefaultUrl);
    const std::vector<std::string> headers = {
        "Content-Type: application/json",
        "Authorization: Bearer " + credential.value,
    };

    const HttpResponse initialize = httpPostJson(url, headers, buildInitializeRequest(), 12000L);
    const std::string initializeOutcome = classifyMcpOutcome(initialize);
    step.metadata["initialize_outcome"] = initializeOutcome;
    step.metadata["initialize_status_code"] = initialize.statusCode;
    step.latencyMs += initialize.latencyMs;
    if (initializeOutcome != "success") {
        step.reason = initializeOutcome;
        return step;
    }

    const HttpResponse toolsList = httpPostJson(url, headers, buildToolsListRequest(), 12000L);
    const std::string listOutcome = classifyMcpOutcome(toolsList);
    step.metadata["tools_list_outcome"] = listOutcome;
    step.metadata["tools_list_status_code"] = toolsList.statusCode;
    step.latencyMs += toolsList.latencyMs;
    if (listOutcome != "success") {
        step.reason = listOutcome;
        return step;
    }

    const std::vector<json> tools = listTools(parseJsonBody(toolsList.body));
    const auto selectedTools = selectToolsForFacets(tools, plan.facets.empty() ? std::vector<std::string>{"options_flow", "stock_state", "news", "gex"} : plan.facets);
    step.metadata["tool_count"] = tools.size();
    step.metadata["selected_tools"] = json::array();
    for (const auto& [facet, toolName] : selectedTools) {
        step.metadata["selected_tools"].push_back({{"facet", facet}, {"tool_name", toolName}});
        const HttpResponse call = httpPostJson(url, headers, buildToolsCallRequest(toolName, plan.symbol), 12000L);
        step.latencyMs += call.latencyMs;
        const std::string callOutcome = classifyMcpOutcome(call);
        if (callOutcome == "success") {
            step.rawRecords.push_back(makeNormalizedRawRecord(
                facet == "fallback" ? "mcp_tool_result" : facet,
                plan.symbol,
                toolName + ":" + plan.symbol,
                call,
                parseJsonBody(call.body),
                json{{"tool_name", toolName}, {"source", "uw_mcp"}}));
        } else {
            step.metadata["call_failures"].push_back({{"tool_name", toolName}, {"outcome", callOutcome}, {"status_code", call.statusCode}});
        }
    }

    step.status = !step.rawRecords.empty() ? "ok" : "unavailable";
    step.reason = !step.rawRecords.empty() ? std::string() : "no_successful_tool_calls";
    step.metadata["credential_env"] = credential.sourceEnv;
    step.metadata["credential_present"] = credential.present;
    return step;
}

ProviderStep UWRestConnector::fetch(const FetchPlan& plan) const {
    const CredentialBinding credential = loadCredentialBinding({"UW_API_TOKEN", "UW_BEARER_TOKEN", "UNUSUAL_WHALES_API_KEY"});
    if (!credential.present) {
        return makeUnavailableStep("uw_rest", plan, credential, "missing_credentials");
    }
    if (const char* disabled = std::getenv("LONG_DISABLE_EXTERNAL_CONTEXT"); disabled != nullptr && *disabled == '1') {
        return makeUnavailableStep("uw_rest", plan, credential, "disabled_by_env");
    }

    ProviderStep step;
    step.provider = "uw_rest";
    step.status = "unavailable";
    const std::vector<std::string> headers = {
        "Accept: application/json",
        "Authorization: Bearer " + credential.value,
    };
    const auto endpoints = restEndpointsForFacets(plan.symbol,
                                                  plan.facets.empty() ? std::vector<std::string>{"options_flow", "stock_state", "news", "gex"} : plan.facets);
    step.metadata["requested_endpoints"] = json::array();
    for (const auto& [facet, url] : endpoints) {
        step.metadata["requested_endpoints"].push_back({{"facet", facet}, {"url", url}});
        const HttpResponse response = httpGet(url, headers, 10000L);
        step.latencyMs += response.latencyMs;
        if (httpSuccess(response)) {
            step.rawRecords.push_back(makeNormalizedRawRecord(
                facet,
                plan.symbol,
                stableHashHex(facet + ":" + plan.symbol + ":" + url),
                response,
                parseJsonBody(response.body),
                json{{"source", "uw_rest"}, {"endpoint", url}}));
        } else {
            step.metadata["endpoint_failures"].push_back({
                {"facet", facet},
                {"url", url},
                {"status_code", response.statusCode},
                {"curl_error", response.curlError}
            });
        }
    }
    step.status = !step.rawRecords.empty() ? "ok" : "unavailable";
    step.reason = !step.rawRecords.empty() ? std::string() : "no_successful_rest_endpoints";
    step.metadata["credential_env"] = credential.sourceEnv;
    step.metadata["credential_present"] = credential.present;
    return step;
}

} // namespace uw_context_service