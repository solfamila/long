#include "tape_engine_protocol.h"
#include "uw_context_service.h"
#include "uw_runtime.h"

#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include <unistd.h>

namespace fs = std::filesystem;

namespace {

using tape_engine::QueryOperation;
using tape_engine::QueryRequest;
using tape_engine::QueryResponse;
using uw_context_service::BuildRequest;
using uw_context_service::Lane;
using json = nlohmann::json;

constexpr const char* kSourceDir = TWS_GUI_SOURCE_DIR;

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

struct RequestKindSelection {
    QueryOperation operation = QueryOperation::RefreshExternalContext;
    std::string requestKind = "refresh_external_context";
    Lane lane = Lane::Fast;
};

std::optional<RequestKindSelection> normalizeRequestKind(std::string raw) {
    for (char& ch : raw) {
        if (ch >= 'A' && ch <= 'Z') {
            ch = static_cast<char>(ch - 'A' + 'a');
        }
    }
    if (raw == "refresh" || raw == "refresh_external_context") {
        return RequestKindSelection{};
    }
    if (raw == "fast" || raw == "fast_enrichment" || raw == "enrich_incident") {
        return RequestKindSelection{QueryOperation::EnrichIncident, "fast_enrichment", Lane::Fast};
    }
    if (raw == "deep" || raw == "deep_enrichment" || raw == "explain_incident") {
        return RequestKindSelection{QueryOperation::ExplainIncident, "deep_enrichment", Lane::Deep};
    }
    if (raw == "order" || raw == "order_case" || raw == "order_case_enrichment" || raw == "enrich_order_case") {
        return RequestKindSelection{QueryOperation::EnrichOrderCase, "order_case_enrichment", Lane::Fast};
    }
    return std::nullopt;
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

std::string stringValueOrEmpty(const json& object, const char* key) {
    const auto it = object.find(key);
    if (it != object.end() && it->is_string()) {
        return it->get<std::string>();
    }
    return {};
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

fs::path makeDataDir() {
    char pattern[] = "/tmp/uw_context_smoke.XXXXXX";
    char* created = ::mkdtemp(pattern);
    if (created == nullptr) {
        return fs::temp_directory_path() / "uw_context_smoke";
    }
    return fs::path(created);
}

QueryResponse makeSyntheticLocalEvidence(const std::string& requestId,
                                         const std::string& symbol,
                                         bool includeLiveTail) {
    QueryResponse response;
    response.requestId = requestId;
    response.operation = tape_engine::queryOperationName(QueryOperation::ReadIncident);
    response.status = "ok";
    response.summary = {
        {"served_revision_id", 1ULL},
        {"includes_mutable_tail", includeLiveTail},
        {"artifact", {
            {"artifact_id", "uw-context-smoke:" + symbol},
            {"artifact_type", "synthetic_incident"},
            {"instrument_id", "ib:conid:0:STK:SMART:USD:" + symbol}
        }},
        {"entity", {
            {"symbol", symbol},
            {"ticker", symbol}
        }},
        {"report", {
            {"headline", "UW provider smoke for " + symbol},
            {"summary", "Synthetic local evidence used to exercise merged UW provider enrichment."},
            {"symbol", symbol}
        }},
        {"evidence", {
            {"citations", json::array()}
        }},
        {"replay_range", {
            {"first_session_seq", 1ULL},
            {"last_session_seq", 10ULL},
            {"start_engine_ns", 1ULL},
            {"end_engine_ns", 10ULL}
        }}
    };
    response.events = json::array();
    return response;
}

json summarizeItems(const json& items) {
    json byProvider = json::object();
    json byKind = json::object();
    if (!items.is_array()) {
        return {
            {"item_count", 0ULL},
            {"by_provider", byProvider},
            {"by_kind", byKind}
        };
    }
    for (const auto& item : items) {
        if (!item.is_object()) {
            continue;
        }
        const std::string provider = item.value("provider", std::string("unknown"));
        const std::string kind = item.value("kind", std::string("unknown"));
        byProvider[provider] = byProvider.value(provider, 0ULL) + 1ULL;
        byKind[kind] = byKind.value(kind, 0ULL) + 1ULL;
    }
    return {
        {"item_count", static_cast<std::uint64_t>(items.size())},
        {"by_provider", byProvider},
        {"by_kind", byKind}
    };
}

json summarizeProviderSteps(const json& providerSteps, const json& items) {
    json itemCounts = json::object();
    json itemKinds = json::object();
    if (items.is_array()) {
        for (const auto& item : items) {
            if (!item.is_object()) {
                continue;
            }
            const std::string provider = item.value("provider", std::string("unknown"));
            const std::string kind = item.value("kind", std::string("unknown"));
            itemCounts[provider] = itemCounts.value(provider, 0ULL) + 1ULL;
            if (!itemKinds.contains(provider) || !itemKinds.at(provider).is_object()) {
                itemKinds[provider] = json::object();
            }
            itemKinds[provider][kind] = itemKinds[provider].value(kind, 0ULL) + 1ULL;
        }
    }

    json summaries = json::array();
    if (!providerSteps.is_array()) {
        return summaries;
    }
    for (const auto& step : providerSteps) {
        if (!step.is_object()) {
            continue;
        }
        const json metadata = step.value("metadata", json::object());
        json summary = {
            {"provider", step.value("provider", std::string())},
            {"status", step.value("status", std::string())},
            {"reason", step.contains("reason") ? step.at("reason") : json(nullptr)},
            {"latency_ms", step.value("latency_ms", 0ULL)},
            {"normalized_item_count", itemCounts.value(step.value("provider", std::string()), 0ULL)},
            {"normalized_item_kinds", itemKinds.contains(step.value("provider", std::string()))
                ? itemKinds.at(step.value("provider", std::string()))
                : json::object()}
        };
        if (summary["provider"] == "uw_mcp") {
            summary["coverage_status"] = metadata.value("coverage_status", std::string());
            summary["unsupported_facets"] = metadata.value("unsupported_facets", json::array());
            summary["selected_tools"] = metadata.value("selected_tools", json::array());
        } else if (summary["provider"] == "uw_rest") {
            summary["coverage_status"] = metadata.value("coverage_status", std::string());
            summary["unsupported_facets"] = metadata.value("unsupported_facets", json::array());
            summary["requested_endpoints"] = metadata.value("requested_endpoints", json::array());
        } else if (summary["provider"] == "uw_ws") {
            summary["source"] = metadata.contains("source") ? metadata.at("source") : json(nullptr);
            summary["data_frame_count"] = metadata.value("data_frame_count", 0ULL);
            summary["join_ack_frame_count"] = metadata.value("join_ack_frame_count", 0ULL);
            summary["adaptive_retry_used"] = metadata.value("adaptive_retry_used", false);
        }
        summaries.push_back(std::move(summary));
    }
    return summaries;
}

json buildTriageSummary(const json& result) {
    const json providerMetadata = result.value("provider_metadata", json::object());
    const json externalContext = result.value("external_context", json::object());
    const json providerSteps = providerMetadata.value("provider_steps", json::array());
    const json items = externalContext.value("items", json::array());
    return {
        {"request_kind", result.value("request_kind", std::string())},
        {"artifact_id", result.value("artifact_id", std::string())},
        {"provider_path_used", providerMetadata.value("provider_path_used", std::string())},
        {"degradation", result.value("degradation", json::object())},
        {"cache", result.value("cache", json::object())},
        {"item_summary", summarizeItems(items)},
        {"provider_steps", summarizeProviderSteps(providerSteps, items)},
        {"live_capture_summary", result.value("live_capture_summary", json::object())}
    };
}

void printUsage(const char* argv0) {
    std::cout
        << "Usage:\n"
        << "  " << argv0 << " [--symbol SYMBOL] [--facets options_flow,alerts,stock_state,news,gex]\n"
        << "               [--request-kind refresh|fast|deep|order]\n"
        << "               [--sample-ms N] [--max-frames N]\n"
        << "               [--second-pass-sample-ms N] [--second-pass-total-ms N]\n"
        << "               [--second-pass-limit N] [--disable-websocket] [--include-live-tail]\n\n"
        << "Notes:\n"
        << "  - Calls the real uw_context_service merged provider path with synthetic local evidence.\n"
        << "  - `refresh` exercises the UW-only external-context refresh path.\n"
        << "  - `fast`, `deep`, and `order` also run the Gemini interpretation step when credentials exist.\n"
        << "  - Loads credentials from LONG_CREDENTIAL_FILE or " << kSourceDir << "/.env.local.\n"
        << "  - Unsets LONG_DISABLE_EXTERNAL_CONTEXT for the process and enables the websocket lane unless disabled.\n";
}

} // namespace

int main(int argc, char** argv) {
    std::string symbol = "SPY";
    std::vector<std::string> facets = {"options_flow", "alerts", "stock_state", "news", "gex"};
    RequestKindSelection requestKindSelection;
    bool enableWebsocket = true;
    bool includeLiveTail = false;
    std::optional<std::string> sampleMs;
    std::optional<std::string> maxFrames;
    std::optional<std::string> secondPassSampleMs;
    std::optional<std::string> secondPassTotalMs;
    std::optional<std::string> secondPassLimit;

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
                    std::cerr << "Unsupported context facet: " << rawFacet << "\n";
                    return 2;
                }
                parsed.push_back(*normalized);
            }
            if (!parsed.empty()) {
                facets = std::move(parsed);
            }
            continue;
        }
        if (arg == "--request-kind" && i + 1 < argc) {
            const std::optional<RequestKindSelection> parsed = normalizeRequestKind(argv[++i]);
            if (!parsed.has_value()) {
                std::cerr << "Unsupported request kind.\n";
                printUsage(argv[0]);
                return 2;
            }
            requestKindSelection = *parsed;
            continue;
        }
        if (arg == "--sample-ms" && i + 1 < argc) {
            sampleMs = argv[++i];
            continue;
        }
        if (arg == "--max-frames" && i + 1 < argc) {
            maxFrames = argv[++i];
            continue;
        }
        if (arg == "--second-pass-sample-ms" && i + 1 < argc) {
            secondPassSampleMs = argv[++i];
            continue;
        }
        if (arg == "--second-pass-total-ms" && i + 1 < argc) {
            secondPassTotalMs = argv[++i];
            continue;
        }
        if (arg == "--second-pass-limit" && i + 1 < argc) {
            secondPassLimit = argv[++i];
            continue;
        }
        if (arg == "--disable-websocket") {
            enableWebsocket = false;
            continue;
        }
        if (arg == "--include-live-tail") {
            includeLiveTail = true;
            continue;
        }
        std::cerr << "Unknown argument: " << arg << "\n";
        printUsage(argv[0]);
        return 2;
    }

    ensureCredentialFileFallback();
    (void)::unsetenv("LONG_DISABLE_EXTERNAL_CONTEXT");
    if (enableWebsocket) {
        (void)::setenv("LONG_ENABLE_UW_WEBSOCKET_CONTEXT", "1", 1);
        (void)::unsetenv("LONG_DISABLE_UW_WEBSOCKET_CONTEXT");
    } else {
        (void)::unsetenv("LONG_ENABLE_UW_WEBSOCKET_CONTEXT");
        (void)::setenv("LONG_DISABLE_UW_WEBSOCKET_CONTEXT", "1", 1);
    }
    if (sampleMs.has_value()) {
        (void)::setenv("LONG_UW_WS_SAMPLE_MS", sampleMs->c_str(), 1);
    }
    if (maxFrames.has_value()) {
        (void)::setenv("LONG_UW_WS_MAX_FRAMES", maxFrames->c_str(), 1);
    }
    if (secondPassSampleMs.has_value()) {
        (void)::setenv("LONG_UW_WS_SECOND_PASS_SAMPLE_MS", secondPassSampleMs->c_str(), 1);
    }
    if (secondPassTotalMs.has_value()) {
        (void)::setenv("LONG_UW_WS_SECOND_PASS_TOTAL_MS", secondPassTotalMs->c_str(), 1);
    }
    if (secondPassLimit.has_value()) {
        (void)::setenv("LONG_UW_WS_SECOND_PASS_CHANNEL_LIMIT", secondPassLimit->c_str(), 1);
    }

    const fs::path dataDir = makeDataDir();

    QueryRequest request = tape_engine::makeQueryRequest(requestKindSelection.operation, "uw-context-smoke");
    request.includeLiveTail = includeLiveTail;
    QueryResponse localEvidence = makeSyntheticLocalEvidence(request.requestId, symbol, includeLiveTail);

    BuildRequest buildRequest;
    buildRequest.requestKind = requestKindSelection.requestKind;
    buildRequest.lane = requestKindSelection.lane;
    buildRequest.forceRefresh = buildRequest.requestKind == "refresh_external_context";
    buildRequest.dataDir = dataDir;
    buildRequest.revisionId = 1ULL;
    buildRequest.includeLiveTail = includeLiveTail;
    buildRequest.requestedFacets = facets;

    const QueryResponse response =
        uw_context_service::buildEnrichmentResponse(request, localEvidence, buildRequest);

    json output = {
        {"symbol", symbol},
        {"data_dir", dataDir.string()},
        {"requested_facets", facets},
        {"websocket_enabled", enableWebsocket},
        {"request_kind", buildRequest.requestKind},
        {"triage_summary", buildTriageSummary(response.result)},
        {"result", response.result}
    };
    std::cout << output.dump(2) << std::endl;
    return 0;
}
