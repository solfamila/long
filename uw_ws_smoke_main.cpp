#include "uw_context_connectors.h"

#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace {

using uw_context_service::FetchPlan;
using uw_context_service::ProviderStep;
using uw_context_service::UWWsConnector;
using json = nlohmann::json;

constexpr const char* kSourceDir = TWS_GUI_SOURCE_DIR;
constexpr const char* kDocsUrl = "https://api.unusualwhales.com/api/openapi";

std::vector<std::string> splitCsv(const std::string& raw) {
    std::vector<std::string> values;
    std::string current;
    std::istringstream stream(raw);
    while (std::getline(stream, current, ',')) {
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
    if (raw == "option_trades" || raw == "option_trades:symbol" || raw == "options_flow") {
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

void ensureCredentialFileFallback() {
    if (const char* path = std::getenv("LONG_CREDENTIAL_FILE"); path != nullptr && *path != '\0') {
        return;
    }
    const fs::path localPath = fs::path(kSourceDir) / ".env.local";
    if (fs::exists(localPath)) {
        (void)::setenv("LONG_CREDENTIAL_FILE", localPath.string().c_str(), 0);
    }
}

void printUsage(const char* argv0) {
    std::cout
        << "Usage:\n"
        << "  " << argv0 << " [--symbol SYMBOL] [--facets option_trades,flow-alerts,price,news,gex]\n"
        << "               [--sample-ms N] [--max-frames N]\n\n"
        << "Notes:\n"
        << "  - Uses the same UWWsConnector path as long/TapeScope.\n"
        << "  - Loads credentials from LONG_CREDENTIAL_FILE or " << kSourceDir << "/.env.local.\n"
        << "  - Forces LONG_ENABLE_UW_WEBSOCKET_CONTEXT=1 for this process.\n"
        << "  - Longer windows (15s+) are often needed before `news` yields data.\n"
        << "  - Official docs: " << kDocsUrl << "\n";
}

} // namespace

int main(int argc, char** argv) {
    std::string symbol = "SPY";
    std::vector<std::string> facets = {"options_flow", "alerts", "stock_state", "news", "gex"};
    std::string sampleMs = "15000";
    std::string maxFrames = "16";

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--help" || arg == "-h") {
            printUsage(argv[0]);
            return 0;
        }
        if (arg == "--symbol" && i + 1 < argc) {
            symbol = argv[++i];
            continue;
        }
        if (arg == "--facets" && i + 1 < argc) {
            std::vector<std::string> parsed;
            for (const std::string& rawFacet : splitCsv(argv[++i])) {
                const std::optional<std::string> normalized = normalizeFacet(rawFacet);
                if (!normalized.has_value()) {
                    std::cerr << "Unsupported websocket facet: " << rawFacet << "\n";
                    return 2;
                }
                parsed.push_back(*normalized);
            }
            if (!parsed.empty()) {
                facets = std::move(parsed);
            }
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
        std::cerr << "Unknown argument: " << arg << "\n";
        printUsage(argv[0]);
        return 2;
    }

    ensureCredentialFileFallback();
    (void)::setenv("LONG_ENABLE_UW_WEBSOCKET_CONTEXT", "1", 1);
    (void)::setenv("LONG_UW_WS_SAMPLE_MS", sampleMs.c_str(), 1);
    (void)::setenv("LONG_UW_WS_MAX_FRAMES", maxFrames.c_str(), 1);

    FetchPlan plan;
    plan.artifactId = "uw-ws-smoke";
    plan.requestKind = "refresh_external_context";
    plan.symbol = symbol;
    plan.forceRefresh = true;
    plan.includeLiveTail = true;
    plan.facets = facets;

    const ProviderStep step = UWWsConnector().fetch(plan);
    json output = {
        {"provider", step.provider},
        {"status", step.status},
        {"reason", step.reason},
        {"request_payload", step.requestPayload},
        {"metadata", step.metadata},
        {"raw_record_count", step.rawRecords.is_array() ? step.rawRecords.size() : 0},
        {"raw_records", step.rawRecords}
    };
    std::cout << output.dump(2) << std::endl;

    if (step.status == "ok" || step.reason == "join_ack_only") {
        return 0;
    }
    return 3;
}
