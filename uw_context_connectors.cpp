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
constexpr long kUwWsRefreshSampleMs = 5000L;
constexpr long kUwWsDeepSampleMs = 2500L;
constexpr long kUwWsMaxSampleMs = 60000L;
constexpr long kUwWsDefaultMaxFrames = 8L;
constexpr long kUwWsSecondPassDefaultSampleMs = 1500L;
constexpr long kUwWsSecondPassDefaultTotalMs = 4500L;
constexpr long kUwWsSecondPassDefaultChannelLimit = 3L;
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

std::string normalizeSymbolToken(std::string value) {
    value = trimAscii(std::move(value));
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
        return static_cast<char>(std::toupper(ch));
    });
    return value;
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

long defaultSampleMsFor(const FetchPlan& plan) {
    if (plan.requestKind == "refresh_external_context") {
        return kUwWsRefreshSampleMs;
    }
    if (plan.requestKind == "deep_enrichment" || plan.requestKind == "order_case_enrichment") {
        return kUwWsDeepSampleMs;
    }
    return kUwWsDefaultSampleMs;
}

long defaultSecondPassSampleMsFor(long firstPassSampleMs) {
    return std::clamp(firstPassSampleMs / 3L, 500L, kUwWsSecondPassDefaultSampleMs);
}

long defaultSecondPassTotalMsFor(long firstPassSampleMs) {
    return std::clamp(firstPassSampleMs / 2L, 1000L, kUwWsSecondPassDefaultTotalMs);
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

std::string toolCatalogText(const json& tool) {
    return toLowerAscii(tool.value("name", std::string()) + " " + tool.value("description", std::string()));
}

int scoreToolForFacet(const std::string& facet, const json& tool) {
    const std::string name = tool.value("name", std::string());
    const std::string text = toolCatalogText(tool);
    if (facet == "options_flow") {
        if (name == "get_option_trades") {
            return 100;
        }
        if (name == "get_interval_flow") {
            return 90;
        }
        if (name == "get_flow_alerts") {
            return 70;
        }
        if (name == "get_ticker_ohlc_latest_or_date") {
            return 55;
        }
        if (text.find("transaction-level flow") != std::string::npos ||
            text.find("options trades") != std::string::npos ||
            text.find("executed options trades") != std::string::npos) {
            return 85;
        }
        if (text.find("interval flow") != std::string::npos) {
            return 75;
        }
        if (text.find("flow alert") != std::string::npos) {
            return 65;
        }
        if (text.find("options-flow metrics") != std::string::npos) {
            return 45;
        }
        if (text.find("market-wide daily options snapshot") != std::string::npos) {
            return 20;
        }
        return 0;
    }
    if (facet == "alerts") {
        if (name == "get_flow_alerts") {
            return 100;
        }
        if (name == "get_flow_alert_rules") {
            return 45;
        }
        if (name == "get_users_flow_alerts_watchlists") {
            return 20;
        }
        return text.find("flow alert") != std::string::npos ? 60 : 0;
    }
    if (facet == "stock_state") {
        if (name == "get_ticker_ohlc_latest_or_date") {
            return 100;
        }
        if (name == "get_ticker_close_prices") {
            return 80;
        }
        if (name == "get_ticker_candles_by_range") {
            return 70;
        }
        if (text.find("day-state") != std::string::npos ||
            text.find("ohlc") != std::string::npos ||
            text.find("close-only price history") != std::string::npos) {
            return 60;
        }
        return 0;
    }
    if (facet == "news") {
        if (text.find("news") != std::string::npos || text.find("headline") != std::string::npos) {
            return 80;
        }
        return 0;
    }
    if (facet == "gex") {
        if (text.find("gex") != std::string::npos ||
            text.find("gamma exposure") != std::string::npos ||
            text.find("gamma") != std::string::npos) {
            return 80;
        }
        return 0;
    }
    return 0;
}

std::string rationaleForFacetTool(const std::string& facet, const json& tool, int score) {
    const std::string name = tool.value("name", std::string());
    if (score <= 0) {
        return "no_direct_match";
    }
    if (facet == "options_flow") {
        if (name == "get_option_trades") {
            return "exact_transaction_level_flow_tool";
        }
        if (name == "get_interval_flow") {
            return "intraday_flow_summary_tool";
        }
        if (name == "get_flow_alerts") {
            return "alert_based_flow_proxy";
        }
        if (name == "get_ticker_ohlc_latest_or_date") {
            return "daily_state_with_options_flow_metrics";
        }
        if (name == "get_market_state") {
            return "market_wide_flow_snapshot_fallback";
        }
    }
    if (facet == "alerts" && name == "get_flow_alerts") {
        return "exact_alert_tool";
    }
    if (facet == "stock_state" && name == "get_ticker_ohlc_latest_or_date") {
        return "exact_ticker_state_tool";
    }
    return "keyword_match";
}

struct FacetCandidate {
    std::string toolName;
    int score = 0;
    std::string rationale;
};

std::vector<FacetCandidate> rankedCandidatesForFacet(const json& toolCatalog,
                                                     const std::string& facet) {
    std::vector<FacetCandidate> candidates;
    if (!toolCatalog.is_array()) {
        return candidates;
    }
    for (const auto& tool : toolCatalog) {
        if (!tool.is_object()) {
            continue;
        }
        const std::string name = tool.value("name", std::string());
        if (name.empty()) {
            continue;
        }
        const int score = scoreToolForFacet(facet, tool);
        if (score <= 0) {
            continue;
        }
        candidates.push_back({name, score, rationaleForFacetTool(facet, tool, score)});
    }
    std::sort(candidates.begin(), candidates.end(), [](const FacetCandidate& left, const FacetCandidate& right) {
        if (left.score != right.score) {
            return left.score > right.score;
        }
        return left.toolName < right.toolName;
    });
    return candidates;
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

std::vector<std::string> prioritizedRetryChannels(const std::vector<std::string>& channels) {
    auto familyForChannel = [](const std::string& channel) {
        const std::size_t colon = channel.find(':');
        return colon == std::string::npos ? channel : channel.substr(0, colon);
    };
    auto priorityForFamily = [](const std::string& family) {
        if (family == "news") {
            return 0;
        }
        if (family == "flow-alerts") {
            return 1;
        }
        if (family == "option_trades") {
            return 2;
        }
        if (family == "price") {
            return 3;
        }
        if (family == "gex") {
            return 4;
        }
        return 5;
    };

    std::vector<std::string> ordered = channels;
    std::stable_sort(ordered.begin(), ordered.end(), [&](const std::string& lhs, const std::string& rhs) {
        return priorityForFamily(familyForChannel(lhs)) < priorityForFamily(familyForChannel(rhs));
    });
    return ordered;
}

bool jsonMentionsSymbol(const json& value, const std::string& requestedSymbol) {
    if (requestedSymbol.empty()) {
        return true;
    }
    if (value.is_string()) {
        return normalizeSymbolToken(value.get<std::string>()) == requestedSymbol;
    }
    if (value.is_array()) {
        for (const auto& item : value) {
            if (jsonMentionsSymbol(item, requestedSymbol)) {
                return true;
            }
        }
    }
    return false;
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

void noteChannelMetric(json& metadata, const std::string& channel, const char* metricKey) {
    if (channel.empty()) {
        return;
    }
    json& stats = metadata["channel_stats"];
    if (!stats.is_object()) {
        stats = json::object();
    }
    json& channelEntry = stats[channel];
    if (!channelEntry.is_object()) {
        channelEntry = json::object();
    }
    channelEntry["channel"] = channel;
    channelEntry["channel_family"] = channelFamily(channel);
    channelEntry[metricKey] = channelEntry.value(metricKey, 0ULL) + 1ULL;
}

void initializeWsCaptureMetadata(json& metadata) {
    metadata["raw_frame_count"] = 0;
    metadata["candidate_data_frame_count"] = 0;
    metadata["normalized_event_count"] = 0;
    metadata["data_frame_count"] = 0;
    metadata["join_ack_frame_count"] = 0;
    metadata["unparsed_frame_count"] = 0;
    metadata["close_frame_count"] = 0;
    metadata["error_frame_count"] = 0;
    metadata["duplicate_join_frame_count"] = 0;
    metadata["filtered_mismatch_frame_count"] = 0;
    metadata["ambient_global_frame_count"] = 0;
    metadata["frame_previews"] = json::array();
    metadata["channel_stats"] = json::object();
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
    return envelope.channelBound ? fallbackSymbol : std::string();
}

bool envelopeHasExplicitSymbolBinding(const WsEnvelope& envelope) {
    if (channelSymbol(envelope.channel).has_value()) {
        return true;
    }
    if (!envelope.payload.is_object()) {
        return false;
    }
    for (const char* key : {"underlying_symbol", "ticker", "symbol", "underlying", "stock_symbol"}) {
        const auto it = envelope.payload.find(key);
        if (it != envelope.payload.end()) {
            if (it->is_string() && !trimAscii(it->get<std::string>()).empty()) {
                return true;
            }
            if (it->is_array() && !it->empty()) {
                return true;
            }
        }
    }
    for (const char* key : {"tickers", "symbols", "related_symbols", "underlyings"}) {
        const auto it = envelope.payload.find(key);
        if (it != envelope.payload.end() && it->is_array() && !it->empty()) {
            return true;
        }
    }
    return false;
}

bool envelopeMatchesRequestedSymbol(const FetchPlan& plan, const WsEnvelope& envelope) {
    const std::string requestedSymbol = normalizeSymbolToken(plan.symbol);
    if (requestedSymbol.empty()) {
        return true;
    }
    if (const std::optional<std::string> symbol = channelSymbol(envelope.channel); symbol.has_value()) {
        return normalizeSymbolToken(*symbol) == requestedSymbol;
    }
    if (!envelope.payload.is_object()) {
        return false;
    }
    for (const char* key : {"underlying_symbol", "ticker", "symbol", "underlying", "stock_symbol"}) {
        if (const auto it = envelope.payload.find(key); it != envelope.payload.end() &&
                                                jsonMentionsSymbol(*it, requestedSymbol)) {
            return true;
        }
    }
    for (const char* key : {"tickers", "symbols", "related_symbols", "underlyings"}) {
        if (const auto it = envelope.payload.find(key); it != envelope.payload.end() &&
                                                jsonMentionsSymbol(*it, requestedSymbol)) {
            return true;
        }
    }
    return false;
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

void processWsEnvelope(const FetchPlan& plan,
                       const WsEnvelope& envelope,
                       const std::string& frame,
                       int flags,
                       std::uint64_t receivedNs,
                       json& metadata,
                       json& rawRecords) {
    appendFramePreview(metadata, frame, flags, envelope);
    if (envelope.channelBound) {
        noteChannelMetric(metadata, envelope.channel, "raw_frame_count");
    }
    if (!envelope.parsed) {
        metadata["unparsed_frame_count"] = metadata.value("unparsed_frame_count", 0ULL) + 1ULL;
        if (envelope.channelBound) {
            noteChannelMetric(metadata, envelope.channel, "unparsed_frame_count");
        }
        return;
    }
    if (isJoinAck(envelope)) {
        metadata["join_ack_frame_count"] = metadata.value("join_ack_frame_count", 0ULL) + 1ULL;
        if (envelope.channelBound) {
            noteChannelMetric(metadata, envelope.channel, "join_ack_frame_count");
        }
        return;
    }
    if (isAlreadyInRoomFrame(envelope)) {
        metadata["error_frame_count"] = metadata.value("error_frame_count", 0ULL) + 1ULL;
        metadata["duplicate_join_frame_count"] = metadata.value("duplicate_join_frame_count", 0ULL) + 1ULL;
        return;
    }
    if (isErrorFrame(envelope)) {
        metadata["error_frame_count"] = metadata.value("error_frame_count", 0ULL) + 1ULL;
        if (envelope.channelBound) {
            noteChannelMetric(metadata, envelope.channel, "error_frame_count");
        }
        return;
    }
    metadata["candidate_data_frame_count"] = metadata.value("candidate_data_frame_count", 0ULL) + 1ULL;
    if (envelope.channelBound) {
        noteChannelMetric(metadata, envelope.channel, "candidate_data_frame_count");
    }
    if (!envelopeMatchesRequestedSymbol(plan, envelope)) {
        const bool explicitBinding = envelopeHasExplicitSymbolBinding(envelope);
        const char* metricKey = explicitBinding ? "filtered_mismatch_frame_count" : "ambient_global_frame_count";
        metadata[metricKey] = metadata.value(metricKey, 0ULL) + 1ULL;
        if (envelope.channelBound) {
            noteChannelMetric(metadata, envelope.channel, metricKey);
        }
        return;
    }
    rawRecords.push_back(makeWebsocketRawRecord(plan, envelope, frame, receivedNs));
    metadata["normalized_event_count"] = metadata.value("normalized_event_count", 0ULL) + 1ULL;
    metadata["data_frame_count"] = metadata.value("data_frame_count", 0ULL) + 1ULL;
    if (envelope.channelBound) {
        noteChannelMetric(metadata, envelope.channel, "data_frame_count");
        noteChannelMetric(metadata, envelope.channel, "normalized_event_count");
    }
}

std::string finalizeWsReasonFromMetadata(const json& metadata, const std::string& currentReason = {}) {
    if (!currentReason.empty()) {
        return currentReason;
    }
    if (metadata.value("duplicate_join_frame_count", 0ULL) > 0ULL) {
        return "already_in_room_only";
    }
    if (metadata.value("error_frame_count", 0ULL) > 0ULL) {
        return "error_frames_only";
    }
    if (metadata.value("ambient_global_frame_count", 0ULL) > 0ULL) {
        return "ambient_global_only";
    }
    if (metadata.value("filtered_mismatch_frame_count", 0ULL) > 0ULL) {
        return "filtered_mismatch_only";
    }
    if (metadata.value("join_ack_frame_count", 0ULL) > 0ULL) {
        return "join_ack_only";
    }
    if (metadata.value("unparsed_frame_count", 0ULL) > 0ULL) {
        return "unparsed_frames_only";
    }
    return "no_live_frames";
}

void mergePreviewArrays(json& target, const json& source) {
    if (!target.is_array()) {
        target = json::array();
    }
    if (!source.is_array()) {
        return;
    }
    for (const auto& preview : source) {
        if (target.size() >= kUwWsPreviewFrameLimit) {
            break;
        }
        target.push_back(preview);
    }
}

void mergeChannelStats(json& target, const json& source) {
    if (!target.is_object()) {
        target = json::object();
    }
    if (!source.is_object()) {
        return;
    }
    for (auto it = source.begin(); it != source.end(); ++it) {
        if (!it.value().is_object()) {
            continue;
        }
        json& entry = target[it.key()];
        if (!entry.is_object()) {
            entry = json::object();
        }
        entry["channel"] = it.value().value("channel", it.key());
        entry["channel_family"] = it.value().value("channel_family", channelFamily(it.key()));
        for (const char* key : {"raw_frame_count",
                                "candidate_data_frame_count",
                                "data_frame_count",
                                "join_ack_frame_count",
                                "duplicate_join_frame_count",
                                "error_frame_count",
                                "filtered_mismatch_frame_count",
                                "ambient_global_frame_count",
                                "normalized_event_count",
                                "unparsed_frame_count"}) {
            entry[key] = entry.value(key, 0ULL) + it.value().value(key, 0ULL);
        }
    }
}

void mergeWsAttempt(ProviderStep& aggregateStep,
                    const ProviderStep& attempt,
                    const std::string& passLabel) {
    for (const auto& record : attempt.rawRecords) {
        aggregateStep.rawRecords.push_back(record);
    }
    for (const char* key : {"raw_frame_count",
                            "candidate_data_frame_count",
                            "normalized_event_count",
                            "data_frame_count",
                            "join_ack_frame_count",
                            "unparsed_frame_count",
                            "close_frame_count",
                            "error_frame_count",
                            "duplicate_join_frame_count",
                            "filtered_mismatch_frame_count",
                            "ambient_global_frame_count",
                            "connection_attempts",
                            "successful_connections",
                            "reconnects",
                            "join_messages_sent"}) {
        aggregateStep.metadata[key] = aggregateStep.metadata.value(key, 0ULL) + attempt.metadata.value(key, 0ULL);
    }
    mergePreviewArrays(aggregateStep.metadata["frame_previews"],
                       attempt.metadata.value("frame_previews", json::array()));
    mergeChannelStats(aggregateStep.metadata["channel_stats"],
                      attempt.metadata.value("channel_stats", json::object()));
    json& passes = aggregateStep.metadata["capture_passes"];
    if (!passes.is_array()) {
        passes = json::array();
    }
    passes.push_back({
        {"label", passLabel},
        {"status", attempt.status},
        {"reason", attempt.reason.empty() ? json(nullptr) : json(attempt.reason)},
        {"channels", attempt.requestPayload.value("subscription_channels", json::array())},
        {"sample_ms", attempt.requestPayload.value("sample_ms", json(nullptr))},
        {"max_frames", attempt.requestPayload.value("max_frames", json(nullptr))},
        {"raw_frame_count", attempt.metadata.value("raw_frame_count", 0ULL)},
        {"candidate_data_frame_count", attempt.metadata.value("candidate_data_frame_count", 0ULL)},
        {"data_frame_count", attempt.metadata.value("data_frame_count", 0ULL)},
        {"join_ack_frame_count", attempt.metadata.value("join_ack_frame_count", 0ULL)},
        {"error_frame_count", attempt.metadata.value("error_frame_count", 0ULL)},
        {"filtered_mismatch_frame_count", attempt.metadata.value("filtered_mismatch_frame_count", 0ULL)},
        {"ambient_global_frame_count", attempt.metadata.value("ambient_global_frame_count", 0ULL)}
    });
}

std::vector<std::string> loadFixtureFrames(const std::string& path,
                                           const std::optional<std::string>& passLabel = std::nullopt) {
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
            if (passLabel.has_value()) {
                const auto passIt = parsed.find("pass");
                if (passIt != parsed.end() && passIt->is_string() &&
                    passIt->get<std::string>() != *passLabel) {
                    continue;
                }
            }
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

UWMcpFacetResolution resolveUWMcpToolsForFacets(const json& toolCatalog,
                                                const std::vector<std::string>& facets) {
    UWMcpFacetResolution resolution;
    std::set<std::string> claimedTools;
    for (const std::string& facet : facets) {
        const auto candidates = rankedCandidatesForFacet(toolCatalog, facet);
        json diagnostic = {
            {"facet", facet},
            {"supported", false},
            {"candidate_count", static_cast<std::uint64_t>(candidates.size())},
            {"selected_tool", nullptr},
            {"score", 0},
            {"rationale", nullptr},
            {"top_candidates", json::array()}
        };
        for (std::size_t index = 0; index < std::min<std::size_t>(3U, candidates.size()); ++index) {
            diagnostic["top_candidates"].push_back({
                {"tool_name", candidates[index].toolName},
                {"score", candidates[index].score},
                {"rationale", candidates[index].rationale}
            });
        }

        const FacetCandidate* chosen = nullptr;
        for (const auto& candidate : candidates) {
            if (claimedTools.count(candidate.toolName) == 0) {
                chosen = &candidate;
                break;
            }
        }
        if (chosen == nullptr) {
            resolution.unsupportedFacets.push_back(facet);
            diagnostic["rationale"] = candidates.empty() ? json("no_matching_tool_in_catalog")
                                                         : json("matching_tools_already_claimed");
            resolution.facetDiagnostics.push_back(std::move(diagnostic));
            continue;
        }

        claimedTools.insert(chosen->toolName);
        resolution.selectedTools.push_back({facet, chosen->toolName, chosen->score, chosen->rationale});
        diagnostic["supported"] = true;
        diagnostic["selected_tool"] = chosen->toolName;
        diagnostic["score"] = chosen->score;
        diagnostic["rationale"] = chosen->rationale;
        resolution.facetDiagnostics.push_back(std::move(diagnostic));
    }
    return resolution;
}

std::vector<std::string> unresolvedUWMcpFacets(const ProviderStep& step,
                                               const std::vector<std::string>& requestedFacets) {
    std::set<std::string> unresolvedSet;
    const json unsupported = step.metadata.value("unsupported_facets", json::array());
    if (unsupported.is_array()) {
        for (const auto& facet : unsupported) {
            if (facet.is_string() && !facet.get<std::string>().empty()) {
                unresolvedSet.insert(facet.get<std::string>());
            }
        }
    }

    std::set<std::string> coveredKinds;
    if (step.rawRecords.is_array()) {
        for (const auto& record : step.rawRecords) {
            if (!record.is_object()) {
                continue;
            }
            const std::string kind = record.value("kind", std::string());
            if (!kind.empty()) {
                coveredKinds.insert(kind);
            }
        }
    }

    for (const std::string& facet : requestedFacets) {
        if (coveredKinds.count(facet) == 0) {
            unresolvedSet.insert(facet);
        }
    }

    std::vector<std::string> unresolved;
    for (const std::string& facet : requestedFacets) {
        if (unresolvedSet.count(facet) > 0) {
            unresolved.push_back(facet);
            unresolvedSet.erase(facet);
        }
    }
    for (const std::string& facet : unresolvedSet) {
        unresolved.push_back(facet);
    }
    return unresolved;
}

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

    const std::vector<std::string> requestedFacets =
        plan.facets.empty() ? std::vector<std::string>{"options_flow", "stock_state", "news", "gex"} : plan.facets;
    const std::vector<json> tools = listTools(parseJsonBody(toolsList.body));
    json toolCatalog = json::array();
    for (const auto& tool : tools) {
        toolCatalog.push_back(tool);
    }
    const UWMcpFacetResolution resolution = resolveUWMcpToolsForFacets(toolCatalog, requestedFacets);
    step.metadata["tool_count"] = tools.size();
    step.metadata["requested_facets"] = requestedFacets;
    step.metadata["unsupported_facets"] = resolution.unsupportedFacets;
    step.metadata["facet_diagnostics"] = resolution.facetDiagnostics;
    step.metadata["coverage_status"] = resolution.unsupportedFacets.empty() ? "full" : "partial";
    step.metadata["selected_tools"] = json::array();
    for (const auto& selection : resolution.selectedTools) {
        const std::string& facet = selection.facet;
        const std::string& toolName = selection.toolName;
        step.metadata["selected_tools"].push_back({
            {"facet", facet},
            {"tool_name", toolName},
            {"score", selection.score},
            {"rationale", selection.rationale}
        });
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
    step.reason = !step.rawRecords.empty()
        ? std::string()
        : (resolution.selectedTools.empty() ? "no_supported_facets_in_mcp_catalog" : "no_successful_tool_calls");
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
    const std::vector<std::string> requestedFacets =
        plan.facets.empty() ? std::vector<std::string>{"options_flow", "stock_state", "news", "gex"} : plan.facets;
    const auto endpoints = restEndpointsForFacets(plan.symbol, requestedFacets);
    step.metadata["requested_facets"] = requestedFacets;
    step.metadata["requested_endpoints"] = json::array();
    std::set<std::string> coveredFacets;
    for (const auto& [facet, url] : endpoints) {
        coveredFacets.insert(facet);
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
    step.metadata["unsupported_facets"] = json::array();
    for (const std::string& facet : requestedFacets) {
        if (coveredFacets.count(facet) == 0) {
            step.metadata["unsupported_facets"].push_back(facet);
        }
    }
    step.metadata["coverage_status"] =
        step.metadata.value("unsupported_facets", json::array()).empty() ? "full" : "partial";
    step.status = !step.rawRecords.empty() ? "ok" : "unavailable";
    step.reason = !step.rawRecords.empty()
        ? std::string()
        : (endpoints.empty() ? "no_supported_rest_facets" : "no_successful_rest_endpoints");
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
    const long sampleMs = envLongOrDefault("LONG_UW_WS_SAMPLE_MS",
                                           defaultSampleMsFor(plan),
                                           100L,
                                           kUwWsMaxSampleMs);
    const long maxFrames = envLongOrDefault("LONG_UW_WS_MAX_FRAMES", kUwWsDefaultMaxFrames, 1L, 64L);
    step.requestPayload["subscription_channels"] = channels;
    step.requestPayload["sample_ms"] = sampleMs;
    step.requestPayload["max_frames"] = maxFrames;
    if (channels.empty()) {
        step.reason = "no_supported_live_channels";
        return step;
    }

    const bool adaptiveSecondPassEnabled =
        !envFlagEnabled("LONG_DISABLE_UW_WS_SECOND_PASS") &&
        plan.requestKind == "refresh_external_context" &&
        channels.size() > 1U;
    const long secondPassSampleMs = envLongOrDefault("LONG_UW_WS_SECOND_PASS_SAMPLE_MS",
                                                     defaultSecondPassSampleMsFor(sampleMs),
                                                     100L,
                                                     kUwWsMaxSampleMs);
    const long secondPassTotalMs = envLongOrDefault("LONG_UW_WS_SECOND_PASS_TOTAL_MS",
                                                    defaultSecondPassTotalMsFor(sampleMs),
                                                    100L,
                                                    kUwWsMaxSampleMs);
    const long secondPassChannelLimit = envLongOrDefault("LONG_UW_WS_SECOND_PASS_CHANNEL_LIMIT",
                                                         kUwWsSecondPassDefaultChannelLimit,
                                                         1L,
                                                         8L);
    step.requestPayload["adaptive_second_pass_enabled"] = adaptiveSecondPassEnabled;
    step.requestPayload["adaptive_second_pass_sample_ms"] = secondPassSampleMs;
    step.requestPayload["adaptive_second_pass_total_ms"] = secondPassTotalMs;
    step.requestPayload["adaptive_second_pass_channel_limit"] = secondPassChannelLimit;
    initializeWsCaptureMetadata(step.metadata);
    step.metadata["capture_passes"] = json::array();
    step.metadata["adaptive_retry_used"] = false;
    step.metadata["rescued_by_targeted_pass"] = false;

    auto runAdaptiveSecondPass = [&](const std::string& reason) {
        return adaptiveSecondPassEnabled &&
            (reason == "join_ack_only" ||
             reason == "no_live_frames" ||
             reason == "ambient_global_only" ||
             reason == "filtered_mismatch_only");
    };

    if (const char* fixturePath = std::getenv("LONG_UW_WS_FIXTURE_FILE"); fixturePath != nullptr && *fixturePath != '\0') {
        step.metadata["source"] = "fixture";
        step.metadata["fixture_path"] = fixturePath;
        auto runFixtureAttempt = [&](const std::vector<std::string>& passChannels,
                                     long passSampleMs,
                                     long passMaxFrames,
                                     const std::string& passLabel,
                                     const std::optional<std::string>& passFilter) {
            ProviderStep attempt;
            attempt.provider = "uw_ws";
            attempt.status = "unavailable";
            attempt.requestPayload = {
                {"symbol", plan.symbol},
                {"subscription_channels", passChannels},
                {"sample_ms", passSampleMs},
                {"max_frames", passMaxFrames}
            };
            attempt.metadata["source"] = "fixture";
            attempt.metadata["fixture_path"] = fixturePath;
            initializeWsCaptureMetadata(attempt.metadata);
            const std::vector<std::string> frames = loadFixtureFrames(fixturePath, passFilter);
            for (const std::string& frame : frames) {
                const WsEnvelope envelope = parseWsEnvelope(frame);
                attempt.metadata["raw_frame_count"] = attempt.metadata.value("raw_frame_count", 0ULL) + 1ULL;
                processWsEnvelope(plan,
                                  envelope,
                                  frame,
                                  CURLWS_TEXT,
                                  nowUnixSeconds() * 1000000000ULL,
                                  attempt.metadata,
                                  attempt.rawRecords);
            }
            attempt.status = !attempt.rawRecords.empty() ? "ok" : "unavailable";
            attempt.reason = !attempt.rawRecords.empty()
                ? std::string()
                : finalizeWsReasonFromMetadata(attempt.metadata,
                                               frames.empty() ? std::string("fixture_without_data_frames")
                                                              : std::string());
            mergeWsAttempt(step, attempt, passLabel);
            return attempt;
        };

        const ProviderStep initialAttempt = runFixtureAttempt(channels, sampleMs, maxFrames, "initial", std::string("initial"));
        step.reason = initialAttempt.reason;
        const bool initialHadData = !initialAttempt.rawRecords.empty();
        if (runAdaptiveSecondPass(step.reason)) {
            const std::vector<std::string> retryChannels = prioritizedRetryChannels(channels);
            const long budgetChannels =
                std::max<long>(1L, secondPassTotalMs / std::max<long>(1L, secondPassSampleMs));
            const std::size_t retryLimit = static_cast<std::size_t>(
                std::min({secondPassChannelLimit,
                          budgetChannels,
                          static_cast<long>(retryChannels.size())}));
            for (std::size_t index = 0; index < retryLimit && step.rawRecords.empty(); ++index) {
                step.metadata["adaptive_retry_used"] = true;
                const std::string& retryChannel = retryChannels[index];
                (void)runFixtureAttempt({retryChannel},
                                        secondPassSampleMs,
                                        maxFrames,
                                        "targeted:" + retryChannel,
                                        std::string("targeted:") + retryChannel);
            }
        }
        step.status = !step.rawRecords.empty() ? "ok" : "unavailable";
        if (!step.rawRecords.empty()) {
            step.reason.clear();
        } else {
            step.reason = finalizeWsReasonFromMetadata(step.metadata, step.reason);
        }
        step.metadata["rescued_by_targeted_pass"] =
            !initialHadData && step.metadata.value("adaptive_retry_used", false) && !step.rawRecords.empty();
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
    CURL* urlCurl = curl_easy_init();
    if (urlCurl == nullptr) {
        step.reason = "curl_easy_init_failed";
        return step;
    }
    const std::string url = buildWebsocketUrl(urlCurl, credential.value);
    curl_easy_cleanup(urlCurl);
    if (url.empty()) {
        step.reason = "token_escape_failed";
        return step;
    }

    auto runNetworkAttempt = [&](const std::vector<std::string>& passChannels,
                                 long passSampleMs,
                                 long passMaxFrames,
                                 const std::string& passLabel) {
        ProviderStep attempt;
        attempt.provider = "uw_ws";
        attempt.status = "unavailable";
        attempt.requestPayload = {
            {"symbol", plan.symbol},
            {"subscription_channels", passChannels},
            {"sample_ms", passSampleMs},
            {"max_frames", passMaxFrames}
        };
        attempt.metadata["source"] = "network";
        initializeWsCaptureMetadata(attempt.metadata);
        attempt.metadata["connection_attempts"] = 0;
        attempt.metadata["successful_connections"] = 0;
        attempt.metadata["reconnects"] = 0;
        attempt.metadata["join_messages_sent"] = 0;

        CURL* curl = curl_easy_init();
        if (curl == nullptr) {
            attempt.reason = "curl_easy_init_failed";
            mergeWsAttempt(step, attempt, passLabel);
            return attempt;
        }

        char errorBuffer[CURL_ERROR_SIZE] = {0};
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_CONNECT_ONLY, 2L);
        curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_1_1);
        curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, kUwWsConnectTimeoutMs);
        curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
        curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, errorBuffer);
        curl_easy_setopt(curl, CURLOPT_USERAGENT, "long-uw-context/uw_ws/0.1");

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(passSampleMs);
        bool reconnectRequested = false;
        while (std::chrono::steady_clock::now() < deadline &&
               attempt.rawRecords.size() < static_cast<std::size_t>(passMaxFrames)) {
            attempt.metadata["connection_attempts"] = attempt.metadata.value("connection_attempts", 0ULL) + 1ULL;

            const CURLcode connectCode = curl_easy_perform(curl);
            if (connectCode != CURLE_OK) {
                attempt.reason = "connect_failed";
                attempt.metadata["last_error"] =
                    errorBuffer[0] != '\0' ? errorBuffer : curl_easy_strerror(connectCode);
                break;
            }

            attempt.metadata["successful_connections"] = attempt.metadata.value("successful_connections", 0ULL) + 1ULL;
            bool connectionOpen = true;
            reconnectRequested = false;
            for (const std::string& channel : passChannels) {
                const std::string joinMessage = json{{"channel", channel}, {"msg_type", "join"}}.dump();
                const CURLcode sendCode = sendTextFrame(curl, joinMessage);
                if (sendCode != CURLE_OK) {
                    attempt.reason = "join_failed";
                    attempt.metadata["last_error"] = curl_easy_strerror(sendCode);
                    connectionOpen = false;
                    break;
                }
                attempt.metadata["join_messages_sent"] = attempt.metadata.value("join_messages_sent", 0ULL) + 1ULL;
            }

            while (connectionOpen &&
                   std::chrono::steady_clock::now() < deadline &&
                   attempt.rawRecords.size() < static_cast<std::size_t>(passMaxFrames)) {
                std::string frame;
                int flags = 0;
                const CURLcode receiveCode = receiveFrame(curl, &frame, &flags);
                if (receiveCode == CURLE_OK) {
                    attempt.metadata["raw_frame_count"] = attempt.metadata.value("raw_frame_count", 0ULL) + 1ULL;
                    const WsEnvelope envelope = parseWsEnvelope(frame);
                    if ((flags & CURLWS_CLOSE) != 0) {
                        appendFramePreview(attempt.metadata, frame, flags, envelope);
                        attempt.metadata["close_frame_count"] = attempt.metadata.value("close_frame_count", 0ULL) + 1ULL;
                        reconnectRequested = true;
                        break;
                    }
                    processWsEnvelope(plan,
                                      envelope,
                                      frame,
                                      flags,
                                      nowUnixSeconds() * 1000000000ULL,
                                      attempt.metadata,
                                      attempt.rawRecords);
                    continue;
                }
                if (receiveCode == CURLE_AGAIN) {
                    continue;
                }
                if (receiveCode == CURLE_GOT_NOTHING) {
                    reconnectRequested = true;
                    break;
                }
                attempt.reason = "receive_failed";
                attempt.metadata["last_error"] = curl_easy_strerror(receiveCode);
                reconnectRequested = true;
                break;
            }

            closeSocket(curl);
            curl_easy_cleanup(curl);
            curl = nullptr;
            if (attempt.rawRecords.size() >= static_cast<std::size_t>(passMaxFrames) ||
                std::chrono::steady_clock::now() >= deadline) {
                break;
            }
            if (!reconnectRequested) {
                break;
            }
            attempt.metadata["reconnects"] = attempt.metadata.value("reconnects", 0ULL) + 1ULL;
            sleepForReconnect(deadline);
            curl = curl_easy_init();
            if (curl == nullptr) {
                attempt.reason = "curl_easy_init_failed";
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

        attempt.status = !attempt.rawRecords.empty() ? "ok" : "unavailable";
        if (attempt.rawRecords.empty()) {
            attempt.reason = finalizeWsReasonFromMetadata(attempt.metadata, attempt.reason);
        }
        mergeWsAttempt(step, attempt, passLabel);
        return attempt;
    };

    const ProviderStep initialAttempt = runNetworkAttempt(channels, sampleMs, maxFrames, "initial");
    step.reason = initialAttempt.reason;
    const bool initialHadData = !initialAttempt.rawRecords.empty();
    if (runAdaptiveSecondPass(step.reason)) {
        const std::vector<std::string> retryChannels = prioritizedRetryChannels(channels);
        const long budgetChannels =
            std::max<long>(1L, secondPassTotalMs / std::max<long>(1L, secondPassSampleMs));
        const std::size_t retryLimit = static_cast<std::size_t>(
            std::min({secondPassChannelLimit,
                      budgetChannels,
                      static_cast<long>(retryChannels.size())}));
        for (std::size_t index = 0; index < retryLimit && step.rawRecords.empty(); ++index) {
            step.metadata["adaptive_retry_used"] = true;
            const std::string& retryChannel = retryChannels[index];
            (void)runNetworkAttempt({retryChannel},
                                    secondPassSampleMs,
                                    maxFrames,
                                    "targeted:" + retryChannel);
        }
    }

    step.status = !step.rawRecords.empty() ? "ok" : "unavailable";
    if (!step.rawRecords.empty()) {
        step.reason.clear();
    } else {
        step.reason = finalizeWsReasonFromMetadata(step.metadata, step.reason);
    }
    step.metadata["rescued_by_targeted_pass"] =
        !initialHadData && step.metadata.value("adaptive_retry_used", false) && !step.rawRecords.empty();
    return step;
#endif
}

} // namespace uw_context_service
