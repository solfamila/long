#include "uw_context_connectors.h"

#include "uw_http.h"
#include "uw_runtime.h"

#include <curl/curl.h>

#include <algorithm>
#include <chrono>
#include <cctype>
#include <cstdlib>
#include <fstream>
#include <optional>
#include <poll.h>
#include <set>
#include <sstream>
#include <string_view>
#include <thread>
#include <vector>

namespace uw_context_service {
namespace {

constexpr const char* kUwMcpDefaultUrl = "https://api.unusualwhales.com/api/mcp";
constexpr const char* kUwApiBase = "https://api.unusualwhales.com";
constexpr const char* kUwWsDefaultUrl = "wss://api.unusualwhales.com/socket";
constexpr long kUwWsConnectTimeoutMs = 5000L;
constexpr long kUwWsPollSliceMs = 100L;
constexpr long kUwWsReconnectDelayMs = 250L;
constexpr long kUwWsDefaultSampleMs = 1500L;
constexpr long kUwWsMaxSampleMs = 60000L;
constexpr long kUwWsDefaultMaxFrames = 8L;
constexpr std::size_t kUwWsPreviewFrameLimit = 8U;

std::string envOrDefault(const char* key, const char* fallback) {
    if (const char* value = std::getenv(key); value != nullptr && *value != '\0') {
        return value;
    }
    return fallback;
}

std::string trimAscii(const std::string& value) {
    const std::size_t begin = value.find_first_not_of(" \t\r\n");
    if (begin == std::string::npos) {
        return {};
    }
    const std::size_t end = value.find_last_not_of(" \t\r\n");
    return value.substr(begin, end - begin + 1);
}

std::string toLowerAscii(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
    return value;
}

bool envFlagEnabled(const char* key) {
    if (const char* raw = std::getenv(key); raw != nullptr && *raw != '\0') {
        const std::string normalized = toLowerAscii(trimAscii(raw));
        return normalized == "1" || normalized == "true" || normalized == "yes" ||
               normalized == "on";
    }
    return false;
}

long envLongOrDefault(const char* key, long fallback, long minimum, long maximum) {
    if (const char* raw = std::getenv(key); raw != nullptr && *raw != '\0') {
        char* end = nullptr;
        const long parsed = std::strtol(raw, &end, 10);
        if (end != nullptr && *end == '\0') {
            return std::clamp(parsed, minimum, maximum);
        }
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
        {"include_live_tail", plan.includeLiveTail},
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

bool websocketSecondaryLaneRequested(const FetchPlan& plan) {
    if (envFlagEnabled("LONG_DISABLE_UW_WEBSOCKET_CONTEXT")) {
        return false;
    }
    if (envFlagEnabled("LONG_ENABLE_UW_WEBSOCKET_CONTEXT")) {
        return true;
    }
    return plan.forceRefresh || plan.includeLiveTail;
}

std::optional<std::string> websocketChannelForFacet(const std::string& facet, const std::string& symbol) {
    if (facet == "options_flow") {
        return "option_trades:" + symbol;
    }
    if (facet == "alerts") {
        return "flow-alerts";
    }
    if (facet == "gex") {
        return "gex:" + symbol;
    }
    if (facet == "stock_state") {
        return "price:" + symbol;
    }
    if (facet == "news") {
        return "news";
    }
    return std::nullopt;
}

std::vector<std::string> websocketChannelsForFacets(const std::string& symbol,
                                                    const std::vector<std::string>& facets) {
    std::vector<std::string> channels;
    for (const std::string& facet : facets) {
        const std::optional<std::string> channel = websocketChannelForFacet(facet, symbol);
        if (!channel.has_value()) {
            continue;
        }
        if (std::find(channels.begin(), channels.end(), *channel) == channels.end()) {
            channels.push_back(*channel);
        }
    }
    return channels;
}

struct WsRuntimeCapabilities {
    std::string libcurlVersion;
    std::vector<std::string> protocols;
    bool hasSsl = false;
    bool hasWs = false;
    bool hasWss = false;
};

WsRuntimeCapabilities detectWsRuntimeCapabilities() {
    ensureCurlReady();
    WsRuntimeCapabilities capabilities;
    const curl_version_info_data* info = curl_version_info(CURLVERSION_NOW);
    if (info == nullptr) {
        return capabilities;
    }
    if (info->version != nullptr) {
        capabilities.libcurlVersion = info->version;
    }
    if (info->protocols != nullptr) {
        for (const char* const* protocol = info->protocols; *protocol != nullptr; ++protocol) {
            capabilities.protocols.emplace_back(*protocol);
        }
    }
    capabilities.hasSsl = (info->features & CURL_VERSION_SSL) != 0;
    capabilities.hasWs = std::find(capabilities.protocols.begin(), capabilities.protocols.end(), "ws") != capabilities.protocols.end();
    capabilities.hasWss = std::find(capabilities.protocols.begin(), capabilities.protocols.end(), "wss") != capabilities.protocols.end();
    return capabilities;
}

std::string channelFamily(const std::string& channel) {
    const std::size_t colon = channel.find(':');
    return colon == std::string::npos ? channel : channel.substr(0, colon);
}

std::optional<std::string> channelSymbol(const std::string& channel) {
    const std::size_t colon = channel.find(':');
    if (colon == std::string::npos || colon + 1 >= channel.size()) {
        return std::nullopt;
    }
    return channel.substr(colon + 1);
}

std::string kindForChannelFamily(const std::string& family) {
    if (family == "option_trades") {
        return "options_flow";
    }
    if (family == "flow-alerts") {
        return "alerts";
    }
    if (family == "price") {
        return "stock_state";
    }
    return family;
}

struct WsEnvelope {
    bool parsed = false;
    bool channelBound = false;
    std::string channel;
    json payload = json(nullptr);
};

WsEnvelope parseWsEnvelope(const std::string& frame) {
    const json parsed = json::parse(frame, nullptr, false);
    if (parsed.is_array() && parsed.size() >= 2 && parsed.at(0).is_string()) {
        return {true, true, parsed.at(0).get<std::string>(), parsed.at(1)};
    }
    if (parsed.is_object()) {
        return {true, false, {}, parsed};
    }
    return {};
}

bool isJoinAck(const WsEnvelope& envelope) {
    return envelope.parsed && envelope.channelBound && envelope.payload.is_object() &&
           envelope.payload.value("status", std::string()) == "ok" &&
           envelope.payload.contains("response");
}

bool isErrorFrame(const WsEnvelope& envelope) {
    return envelope.parsed && envelope.payload.is_object() &&
           envelope.payload.contains("error") &&
           envelope.payload.find("error")->is_string() &&
           !envelope.payload.find("error")->get_ref<const std::string&>().empty();
}

std::string errorMessage(const WsEnvelope& envelope) {
    if (!isErrorFrame(envelope)) {
        return {};
    }
    return envelope.payload.value("error", std::string());
}

bool isAlreadyInRoomFrame(const WsEnvelope& envelope) {
    return errorMessage(envelope) == "Already in room";
}

std::string websocketEventKind(const WsEnvelope& envelope) {
    if (!envelope.parsed) {
        return "unparsed";
    }
    if (isJoinAck(envelope)) {
        return "join_ack";
    }
    if (isAlreadyInRoomFrame(envelope)) {
        return "duplicate_join";
    }
    if (isErrorFrame(envelope)) {
        return "error";
    }
    return "data";
}

json websocketFrameFlags(int flags) {
    json names = json::array();
#if LIBCURL_VERSION_NUM >= 0x075600
    if ((flags & CURLWS_TEXT) != 0) {
        names.push_back("text");
    }
    if ((flags & CURLWS_BINARY) != 0) {
        names.push_back("binary");
    }
    if ((flags & CURLWS_CONT) != 0) {
        names.push_back("continuation");
    }
    if ((flags & CURLWS_CLOSE) != 0) {
        names.push_back("close");
    }
#else
    (void)flags;
#endif
    return names;
}

void appendFramePreview(json& metadata,
                        const std::string& frame,
                        int flags,
                        const WsEnvelope& envelope) {
    json& previews = metadata["frame_previews"];
    if (!previews.is_array()) {
        previews = json::array();
    }
    if (previews.size() >= kUwWsPreviewFrameLimit) {
        return;
    }
    previews.push_back({
        {"event_kind", websocketEventKind(envelope)},
        {"channel", envelope.channelBound ? json(envelope.channel) : json(nullptr)},
        {"channel_family", envelope.channelBound ? json(channelFamily(envelope.channel)) : json(nullptr)},
        {"error_message", errorMessage(envelope).empty() ? json(nullptr) : json(errorMessage(envelope))},
        {"frame_flags", websocketFrameFlags(flags)},
        {"raw_excerpt", payloadPreview(frame, 256U)}
    });
}

std::string inferWsSymbol(const WsEnvelope& envelope, const std::string& fallbackSymbol) {
    if (const std::optional<std::string> symbol = channelSymbol(envelope.channel); symbol.has_value()) {
        return *symbol;
    }
    if (envelope.payload.is_object()) {
        for (const char* key : {"underlying_symbol", "ticker", "symbol"}) {
            const auto it = envelope.payload.find(key);
            if (it != envelope.payload.end() && it->is_string() && !it->get_ref<const std::string&>().empty()) {
                return it->get<std::string>();
            }
        }
    }
    return fallbackSymbol;
}

json structuredPayloadForEnvelope(const WsEnvelope& envelope) {
    if (envelope.payload.is_object() || envelope.payload.is_array()) {
        return envelope.payload;
    }
    if (envelope.payload.is_string()) {
        return json{{"message", envelope.payload.get<std::string>()}};
    }
    return json{{"value", envelope.payload}};
}

json makeWebsocketRawRecord(const FetchPlan& plan,
                            const WsEnvelope& envelope,
                            const std::string& rawFrame,
                            std::uint64_t receivedNs) {
    const std::string symbol = inferWsSymbol(envelope, plan.symbol);
    const std::string family = channelFamily(envelope.channel);
    return {
        {"kind", kindForChannelFamily(family)},
        {"provider_record_id", stableHashHex(envelope.channel + ":" + rawFrame)},
        {"symbol", symbol},
        {"ts_provider_ns", 0ULL},
        {"ts_fetched_ns", receivedNs},
        {"structured", structuredPayloadForEnvelope(envelope)},
        {"raw_excerpt", payloadPreview(rawFrame)},
        {"latency_ms", 0},
        {"schema", "uw_external_context_record_v1"},
        {"channel", envelope.channel},
        {"channel_family", family},
        {"source", "uw_ws"}
    };
}

std::vector<std::string> loadFixtureFrames(const std::string& path) {
    std::ifstream input(path);
    std::vector<std::string> frames;
    if (!input.is_open()) {
        return frames;
    }

    std::string line;
    while (std::getline(input, line)) {
        const std::string trimmed = trimAscii(line);
        if (trimmed.empty()) {
            continue;
        }
        const json parsed = json::parse(trimmed, nullptr, false);
        if (parsed.is_object()) {
            if (const auto frameIt = parsed.find("frame"); frameIt != parsed.end() && frameIt->is_string()) {
                frames.push_back(frameIt->get<std::string>());
                continue;
            }
            if (const auto channelIt = parsed.find("channel"); channelIt != parsed.end() && channelIt->is_string()) {
                frames.push_back(json::array({channelIt->get<std::string>(), parsed.value("payload", json::object())}).dump());
                continue;
            }
        }
        frames.push_back(trimmed);
    }
    return frames;
}

#if LIBCURL_VERSION_NUM >= 0x075600
enum class WaitStatus {
    Ready,
    Timeout,
    Error,
};

WaitStatus waitForSocket(CURL* curl, short events, long timeoutMs, std::string* error) {
    curl_socket_t socketFd = CURL_SOCKET_BAD;
    const CURLcode infoCode = curl_easy_getinfo(curl, CURLINFO_ACTIVESOCKET, &socketFd);
    if (infoCode != CURLE_OK || socketFd == CURL_SOCKET_BAD) {
        if (error != nullptr) {
            *error = "failed_to_access_active_socket";
        }
        return WaitStatus::Error;
    }

    struct pollfd descriptor {};
    descriptor.fd = static_cast<int>(socketFd);
    descriptor.events = events;

    const int pollResult = poll(&descriptor, 1, static_cast<int>(timeoutMs));
    if (pollResult == 0) {
        return WaitStatus::Timeout;
    }
    if (pollResult < 0) {
        if (error != nullptr) {
            *error = "poll_failed";
        }
        return WaitStatus::Error;
    }
    return WaitStatus::Ready;
}

CURLcode sendTextFrame(CURL* curl, const std::string& payload) {
    std::size_t offset = 0;
    while (offset < payload.size()) {
        std::size_t sent = 0;
        const CURLcode code = curl_ws_send(curl,
                                           payload.data() + offset,
                                           payload.size() - offset,
                                           &sent,
                                           0,
                                           CURLWS_TEXT);
        if (code == CURLE_OK) {
            if (sent == 0) {
                return CURLE_SEND_ERROR;
            }
            offset += sent;
            continue;
        }
        if (code == CURLE_AGAIN) {
            std::string waitError;
            const WaitStatus waitStatus = waitForSocket(curl, POLLOUT, kUwWsPollSliceMs, &waitError);
            if (waitStatus == WaitStatus::Ready) {
                continue;
            }
            return waitStatus == WaitStatus::Timeout ? CURLE_AGAIN : CURLE_SEND_ERROR;
        }
        return code;
    }
    return CURLE_OK;
}

CURLcode receiveFrame(CURL* curl, std::string* frame, int* flags) {
    frame->clear();
    *flags = 0;
    while (true) {
        char buffer[8192];
        std::size_t received = 0;
        const struct curl_ws_frame* meta = nullptr;
        const CURLcode code = curl_ws_recv(curl, buffer, sizeof(buffer), &received, &meta);
        if (code == CURLE_OK) {
            if (received > 0) {
                frame->append(buffer, received);
            }
            if (meta != nullptr) {
                *flags |= meta->flags;
                if (meta->bytesleft == 0) {
                    return CURLE_OK;
                }
            } else {
                return CURLE_OK;
            }
            continue;
        }
        if (code == CURLE_AGAIN) {
            std::string waitError;
            const WaitStatus waitStatus = waitForSocket(curl, POLLIN, kUwWsPollSliceMs, &waitError);
            if (waitStatus == WaitStatus::Ready) {
                continue;
            }
            return waitStatus == WaitStatus::Timeout ? CURLE_AGAIN : CURLE_RECV_ERROR;
        }
        return code;
    }
}

void closeSocket(CURL* curl) {
    if (curl == nullptr) {
        return;
    }
    std::size_t sent = 0;
    (void)curl_ws_send(curl, "", 0, &sent, 0, CURLWS_CLOSE);
}

std::string buildWebsocketUrl(CURL* curl, const std::string& token) {
    char* escapedToken = curl_easy_escape(curl, token.c_str(), static_cast<int>(token.size()));
    if (escapedToken == nullptr) {
        return {};
    }
    const std::string base = envOrDefault("LONG_UW_WS_URL", kUwWsDefaultUrl);
    const std::string separator = base.find('?') == std::string::npos ? "?" : "&";
    const std::string url = base + separator + "token=" + escapedToken;
    curl_free(escapedToken);
    return url;
}

void sleepForReconnect(const std::chrono::steady_clock::time_point& deadline) {
    const auto now = std::chrono::steady_clock::now();
    if (now >= deadline) {
        return;
    }
    const auto remainingMs = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count();
    const long long delayMs = std::min<long long>(remainingMs, kUwWsReconnectDelayMs);
    if (delayMs > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(delayMs));
    }
}
#endif

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

ProviderStep UWWsConnector::fetch(const FetchPlan& plan) const {
    const CredentialBinding credential = loadCredentialBinding({"UW_API_TOKEN", "UW_BEARER_TOKEN"});
    if (const char* disabled = std::getenv("LONG_DISABLE_EXTERNAL_CONTEXT"); disabled != nullptr && *disabled == '1') {
        return makeUnavailableStep("uw_ws", plan, credential, "disabled_by_env");
    }
    if (!websocketSecondaryLaneRequested(plan)) {
        return makeUnavailableStep("uw_ws", plan, credential, "secondary_lane_not_requested");
    }
    if (plan.symbol.empty()) {
        return makeUnavailableStep("uw_ws", plan, credential, "missing_symbol");
    }

    ProviderStep step;
    step.provider = "uw_ws";
    step.status = "unavailable";
    step.requestPayload = {
        {"artifact_id", plan.artifactId},
        {"request_kind", plan.requestKind},
        {"symbol", plan.symbol},
        {"force_refresh", plan.forceRefresh},
        {"include_live_tail", plan.includeLiveTail},
        {"facets", plan.facets}
    };
    step.metadata = {
        {"credential_env", credential.sourceEnv.empty() ? json(nullptr) : json(credential.sourceEnv)},
        {"credential_present", credential.present}
    };

    const std::vector<std::string> channels = websocketChannelsForFacets(
        plan.symbol,
        plan.facets.empty() ? std::vector<std::string>{"options_flow", "gex", "news", "stock_state"} : plan.facets);
    step.requestPayload["subscription_channels"] = channels;
    if (channels.empty()) {
        step.reason = "no_supported_live_channels";
        return step;
    }

    if (const char* fixturePath = std::getenv("LONG_UW_WS_FIXTURE_FILE"); fixturePath != nullptr && *fixturePath != '\0') {
        step.metadata["source"] = "fixture";
        step.metadata["fixture_path"] = fixturePath;
        const std::vector<std::string> frames = loadFixtureFrames(fixturePath);
        step.metadata["raw_frame_count"] = frames.size();
        step.metadata["normalized_event_count"] = 0;
        step.metadata["data_frame_count"] = 0;
        step.metadata["join_ack_frame_count"] = 0;
        step.metadata["unparsed_frame_count"] = 0;
        step.metadata["frame_previews"] = json::array();
        step.metadata["error_frame_count"] = 0;
        step.metadata["duplicate_join_frame_count"] = 0;
        for (const std::string& frame : frames) {
            const WsEnvelope envelope = parseWsEnvelope(frame);
            appendFramePreview(step.metadata, frame, CURLWS_TEXT, envelope);
            if (!envelope.parsed) {
                step.metadata["unparsed_frame_count"] = step.metadata.value("unparsed_frame_count", 0ULL) + 1ULL;
                continue;
            }
            if (isJoinAck(envelope)) {
                step.metadata["join_ack_frame_count"] = step.metadata.value("join_ack_frame_count", 0ULL) + 1ULL;
                continue;
            }
            if (isAlreadyInRoomFrame(envelope)) {
                step.metadata["error_frame_count"] = step.metadata.value("error_frame_count", 0ULL) + 1ULL;
                step.metadata["duplicate_join_frame_count"] =
                    step.metadata.value("duplicate_join_frame_count", 0ULL) + 1ULL;
                continue;
            }
            if (isErrorFrame(envelope)) {
                step.metadata["error_frame_count"] = step.metadata.value("error_frame_count", 0ULL) + 1ULL;
                continue;
            }
            step.rawRecords.push_back(makeWebsocketRawRecord(plan, envelope, frame, nowUnixSeconds() * 1000000000ULL));
            step.metadata["normalized_event_count"] = step.metadata.value("normalized_event_count", 0ULL) + 1ULL;
            step.metadata["data_frame_count"] = step.metadata.value("data_frame_count", 0ULL) + 1ULL;
        }
        step.status = !step.rawRecords.empty() ? "ok" : "unavailable";
        if (!step.rawRecords.empty()) {
            step.reason.clear();
        } else if (step.metadata.value("duplicate_join_frame_count", 0ULL) > 0ULL) {
            step.reason = "already_in_room_only";
        } else if (step.metadata.value("error_frame_count", 0ULL) > 0ULL) {
            step.reason = "error_frames_only";
        } else if (step.metadata.value("join_ack_frame_count", 0ULL) > 0ULL) {
            step.reason = "join_ack_only";
        } else if (step.metadata.value("unparsed_frame_count", 0ULL) > 0ULL) {
            step.reason = "unparsed_frames_only";
        } else {
            step.reason = "fixture_without_data_frames";
        }
        return step;
    }

#if LIBCURL_VERSION_NUM < 0x075600
    step.reason = "runtime_missing_websocket_support";
    step.metadata["source"] = "network";
    return step;
#else
    if (!credential.present) {
        step.reason = "missing_credentials";
        return step;
    }

    ensureCurlReady();
    const WsRuntimeCapabilities runtime = detectWsRuntimeCapabilities();
    step.metadata["source"] = "network";
    step.metadata["runtime_capabilities"] = {
        {"libcurl_version", runtime.libcurlVersion},
        {"protocols", runtime.protocols},
        {"has_ssl", runtime.hasSsl},
        {"has_ws", runtime.hasWs},
        {"has_wss", runtime.hasWss}
    };
    if (!runtime.hasWss) {
        step.reason = "blocked_missing_runtime_capability";
        return step;
    }

    const long sampleMs = envLongOrDefault("LONG_UW_WS_SAMPLE_MS", kUwWsDefaultSampleMs, 100L, kUwWsMaxSampleMs);
    const long maxFrames = envLongOrDefault("LONG_UW_WS_MAX_FRAMES", kUwWsDefaultMaxFrames, 1L, 64L);
    step.requestPayload["sample_ms"] = sampleMs;
    step.requestPayload["max_frames"] = maxFrames;

    CURL* curl = curl_easy_init();
    if (curl == nullptr) {
        step.reason = "curl_easy_init_failed";
        return step;
    }

    char errorBuffer[CURL_ERROR_SIZE] = {0};
    const std::string url = buildWebsocketUrl(curl, credential.value);
    if (url.empty()) {
        step.reason = "token_escape_failed";
        curl_easy_cleanup(curl);
        return step;
    }

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_CONNECT_ONLY, 2L);
    curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_1_1);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, kUwWsConnectTimeoutMs);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, errorBuffer);
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "long-uw-context/uw_ws/0.1");

    step.metadata["connection_attempts"] = 0;
    step.metadata["successful_connections"] = 0;
    step.metadata["reconnects"] = 0;
    step.metadata["join_messages_sent"] = 0;
    step.metadata["raw_frame_count"] = 0;
    step.metadata["normalized_event_count"] = 0;
    step.metadata["data_frame_count"] = 0;
    step.metadata["join_ack_frame_count"] = 0;
    step.metadata["unparsed_frame_count"] = 0;
    step.metadata["close_frame_count"] = 0;
    step.metadata["error_frame_count"] = 0;
    step.metadata["duplicate_join_frame_count"] = 0;
    step.metadata["frame_previews"] = json::array();

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(sampleMs);
    bool reconnectRequested = false;
    while (std::chrono::steady_clock::now() < deadline &&
           step.rawRecords.size() < static_cast<std::size_t>(maxFrames)) {
        step.metadata["connection_attempts"] = step.metadata.value("connection_attempts", 0ULL) + 1ULL;

        const CURLcode connectCode = curl_easy_perform(curl);
        if (connectCode != CURLE_OK) {
            step.reason = "connect_failed";
            step.metadata["last_error"] = errorBuffer[0] != '\0' ? errorBuffer : curl_easy_strerror(connectCode);
            break;
        }

        step.metadata["successful_connections"] = step.metadata.value("successful_connections", 0ULL) + 1ULL;
        bool connectionOpen = true;
        reconnectRequested = false;
        for (const std::string& channel : channels) {
            const std::string joinMessage = json{{"channel", channel}, {"msg_type", "join"}}.dump();
            const CURLcode sendCode = sendTextFrame(curl, joinMessage);
            if (sendCode != CURLE_OK) {
                step.reason = "join_failed";
                step.metadata["last_error"] = curl_easy_strerror(sendCode);
                connectionOpen = false;
                break;
            }
            step.metadata["join_messages_sent"] = step.metadata.value("join_messages_sent", 0ULL) + 1ULL;
        }

        while (connectionOpen &&
               std::chrono::steady_clock::now() < deadline &&
               step.rawRecords.size() < static_cast<std::size_t>(maxFrames)) {
            std::string frame;
            int flags = 0;
            const CURLcode receiveCode = receiveFrame(curl, &frame, &flags);
            if (receiveCode == CURLE_OK) {
                step.metadata["raw_frame_count"] = step.metadata.value("raw_frame_count", 0ULL) + 1ULL;
                const WsEnvelope envelope = parseWsEnvelope(frame);
                appendFramePreview(step.metadata, frame, flags, envelope);
                if ((flags & CURLWS_CLOSE) != 0) {
                    step.metadata["close_frame_count"] = step.metadata.value("close_frame_count", 0ULL) + 1ULL;
                    reconnectRequested = true;
                    break;
                }
                if (!envelope.parsed) {
                    step.metadata["unparsed_frame_count"] = step.metadata.value("unparsed_frame_count", 0ULL) + 1ULL;
                    continue;
                }
                if (isJoinAck(envelope)) {
                    step.metadata["join_ack_frame_count"] = step.metadata.value("join_ack_frame_count", 0ULL) + 1ULL;
                    continue;
                }
                if (isAlreadyInRoomFrame(envelope)) {
                    step.metadata["error_frame_count"] = step.metadata.value("error_frame_count", 0ULL) + 1ULL;
                    step.metadata["duplicate_join_frame_count"] =
                        step.metadata.value("duplicate_join_frame_count", 0ULL) + 1ULL;
                    continue;
                }
                if (isErrorFrame(envelope)) {
                    step.metadata["error_frame_count"] = step.metadata.value("error_frame_count", 0ULL) + 1ULL;
                    continue;
                }
                step.rawRecords.push_back(makeWebsocketRawRecord(plan, envelope, frame, nowUnixSeconds() * 1000000000ULL));
                step.metadata["normalized_event_count"] = step.metadata.value("normalized_event_count", 0ULL) + 1ULL;
                step.metadata["data_frame_count"] = step.metadata.value("data_frame_count", 0ULL) + 1ULL;
                continue;
            }
            if (receiveCode == CURLE_AGAIN) {
                continue;
            }
            if (receiveCode == CURLE_GOT_NOTHING) {
                reconnectRequested = true;
                break;
            }
            step.reason = "receive_failed";
            step.metadata["last_error"] = curl_easy_strerror(receiveCode);
            reconnectRequested = true;
            break;
        }

        closeSocket(curl);
        curl_easy_cleanup(curl);
        curl = nullptr;
        if (step.rawRecords.size() >= static_cast<std::size_t>(maxFrames) ||
            std::chrono::steady_clock::now() >= deadline) {
            break;
        }
        if (!reconnectRequested) {
            break;
        }
        step.metadata["reconnects"] = step.metadata.value("reconnects", 0ULL) + 1ULL;
        sleepForReconnect(deadline);
        curl = curl_easy_init();
        if (curl == nullptr) {
            step.reason = "curl_easy_init_failed";
            break;
        }
        std::fill(std::begin(errorBuffer), std::end(errorBuffer), 0);
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_CONNECT_ONLY, 2L);
        curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_1_1);
        curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, kUwWsConnectTimeoutMs);
        curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
        curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, errorBuffer);
        curl_easy_setopt(curl, CURLOPT_USERAGENT, "long-uw-context/uw_ws/0.1");
    }

    if (curl != nullptr) {
        closeSocket(curl);
        curl_easy_cleanup(curl);
    }

    if (!step.rawRecords.empty()) {
        step.status = "ok";
        step.reason.clear();
    } else if (step.reason.empty() && step.metadata.value("duplicate_join_frame_count", 0ULL) > 0ULL) {
        step.reason = "already_in_room_only";
    } else if (step.reason.empty() && step.metadata.value("error_frame_count", 0ULL) > 0ULL) {
        step.reason = "error_frames_only";
    } else if (step.reason.empty() && step.metadata.value("join_ack_frame_count", 0ULL) > 0ULL) {
        step.reason = "join_ack_only";
    } else if (step.reason.empty() && step.metadata.value("unparsed_frame_count", 0ULL) > 0ULL) {
        step.reason = "unparsed_frames_only";
    } else if (step.reason.empty()) {
        step.reason = "no_live_frames";
    }
    return step;
#endif
}

} // namespace uw_context_service
