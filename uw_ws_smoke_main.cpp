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

std::string stringValueOrEmpty(const json& object, const char* key) {
    const auto it = object.find(key);
    if (it != object.end() && it->is_string()) {
        return it->get<std::string>();
    }
    return {};
}

std::string channelFamily(const std::string& channel) {
    const std::size_t colon = channel.find(':');
    return colon == std::string::npos ? channel : channel.substr(0, colon);
}

json buildTriageSummary(const ProviderStep& step) {
    const json metadata = step.metadata.is_object() ? step.metadata : json::object();
    const json requestPayload = step.requestPayload.is_object() ? step.requestPayload : json::object();
    const json channelStats = metadata.value("channel_stats", json::object());
    const json subscribedChannels = requestPayload.value("subscription_channels", json::array());
    const std::uint64_t dataFrameCount = metadata.value("data_frame_count", 0ULL);
    const std::string reason = step.reason;

    json summary = {
        {"requested", true},
        {"status", step.status},
        {"outcome", dataFrameCount > 0ULL ? json("live_data")
                                           : json(reason.empty() ? step.status : reason)},
        {"reason", reason.empty() ? json(nullptr) : json(reason)},
        {"source", metadata.contains("source") ? metadata.at("source") : json(nullptr)},
        {"symbol", requestPayload.contains("symbol") ? requestPayload.at("symbol") : json(nullptr)},
        {"subscription_channels", subscribedChannels.is_array() ? subscribedChannels : json::array()},
        {"channel_count", subscribedChannels.is_array() ? static_cast<std::uint64_t>(subscribedChannels.size()) : 0ULL},
        {"sample_ms", requestPayload.contains("sample_ms") ? requestPayload.at("sample_ms") : json(nullptr)},
        {"max_frames", requestPayload.contains("max_frames") ? requestPayload.at("max_frames") : json(nullptr)},
        {"has_live_data", dataFrameCount > 0ULL},
        {"raw_frame_count", metadata.value("raw_frame_count", 0ULL)},
        {"candidate_data_frame_count", metadata.value("candidate_data_frame_count", 0ULL)},
        {"data_frame_count", dataFrameCount},
        {"normalized_event_count", metadata.value("normalized_event_count", 0ULL)},
        {"join_ack_frame_count", metadata.value("join_ack_frame_count", 0ULL)},
        {"error_frame_count", metadata.value("error_frame_count", 0ULL)},
        {"duplicate_join_frame_count", metadata.value("duplicate_join_frame_count", 0ULL)},
        {"filtered_mismatch_frame_count", metadata.value("filtered_mismatch_frame_count", 0ULL)},
        {"ambient_global_frame_count", metadata.value("ambient_global_frame_count", 0ULL)},
        {"frame_preview_count", metadata.value("frame_previews", json::array()).is_array()
             ? static_cast<std::uint64_t>(metadata.value("frame_previews", json::array()).size())
             : 0ULL},
        {"pass_count", metadata.value("capture_passes", json::array()).is_array()
             ? static_cast<std::uint64_t>(metadata.value("capture_passes", json::array()).size())
             : 0ULL},
        {"targeted_retry_used", metadata.value("adaptive_retry_used", false)},
        {"rescued_by_targeted_pass", metadata.value("rescued_by_targeted_pass", false)},
        {"channel_outcomes", json::array()},
        {"summary_text", ""}
    };

    if (subscribedChannels.is_array()) {
        for (const auto& channelValue : subscribedChannels) {
            if (!channelValue.is_string()) {
                continue;
            }
            const std::string channel = channelValue.get<std::string>();
            const json stats = channelStats.is_object() && channelStats.contains(channel)
                ? channelStats.at(channel)
                : json::object();
            const std::uint64_t channelDataCount = stats.value("data_frame_count", 0ULL);
            const std::uint64_t candidateChannelDataCount = stats.value("candidate_data_frame_count", 0ULL);
            const std::uint64_t channelJoinAckCount = stats.value("join_ack_frame_count", 0ULL);
            const std::uint64_t channelDuplicateJoinCount = stats.value("duplicate_join_frame_count", 0ULL);
            const std::uint64_t channelErrorCount = stats.value("error_frame_count", 0ULL);
            const std::uint64_t channelMismatchCount = stats.value("filtered_mismatch_frame_count", 0ULL);
            const std::uint64_t channelAmbientCount = stats.value("ambient_global_frame_count", 0ULL);

            std::string outcome = "idle";
            if (channelDataCount > 0ULL) {
                outcome = "live_data";
            } else if (channelAmbientCount > 0ULL) {
                outcome = "ambient_global_only";
            } else if (channelDuplicateJoinCount > 0ULL) {
                outcome = "already_in_room_only";
            } else if (channelErrorCount > 0ULL) {
                outcome = "error_frames_only";
            } else if (channelJoinAckCount > 0ULL) {
                outcome = "join_ack_only";
            } else if (channelMismatchCount > 0ULL) {
                outcome = "filtered_mismatch_only";
            }

            summary["channel_outcomes"].push_back({
                {"channel", channel},
                {"channel_family", stats.value("channel_family", channelFamily(channel))},
                {"outcome", outcome},
                {"raw_frame_count", stats.value("raw_frame_count", 0ULL)},
                {"candidate_data_frame_count", candidateChannelDataCount},
                {"data_frame_count", channelDataCount},
                {"join_ack_frame_count", channelJoinAckCount},
                {"error_frame_count", channelErrorCount},
                {"filtered_mismatch_frame_count", channelMismatchCount},
                {"ambient_global_frame_count", channelAmbientCount}
            });
        }
    }

    if (dataFrameCount > 0ULL) {
        summary["summary_text"] =
            "UW websocket captured " + std::to_string(dataFrameCount) + " live data frame(s)" +
            (summary.value("rescued_by_targeted_pass", false) ? " after a targeted retry." : ".");
    } else if (metadata.value("ambient_global_frame_count", 0ULL) > 0ULL) {
        summary["summary_text"] =
            "UW websocket saw " + std::to_string(metadata.value("ambient_global_frame_count", 0ULL)) +
            " ambient global frame(s) without symbol binding.";
    } else if (reason == "join_ack_only") {
        summary["summary_text"] = "UW websocket joined successfully but saw no live data frames.";
    } else if (reason == "already_in_room_only") {
        summary["summary_text"] = "UW websocket returned duplicate-join responses only.";
    } else if (reason == "error_frames_only") {
        summary["summary_text"] = "UW websocket returned only error frames.";
    } else if (reason == "filtered_mismatch_only") {
        summary["summary_text"] = "UW websocket saw live frames, but they belonged to other symbols.";
    } else if (reason == "no_live_frames") {
        summary["summary_text"] = "UW websocket connected but produced no live frames in the capture window.";
    } else if (!reason.empty()) {
        summary["summary_text"] = "UW websocket capture produced no accepted live data: " + reason + ".";
    } else {
        summary["summary_text"] = "UW websocket capture produced no live data summary.";
    }

    return summary;
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
        << "               [--sample-ms N] [--max-frames N]\n"
        << "               [--second-pass-sample-ms N] [--second-pass-total-ms N]\n"
        << "               [--second-pass-limit N]\n\n"
        << "Notes:\n"
        << "  - Uses the same UWWsConnector path as long/TapeScope.\n"
        << "  - Loads credentials from LONG_CREDENTIAL_FILE or " << kSourceDir << "/.env.local.\n"
        << "  - Forces LONG_ENABLE_UW_WEBSOCKET_CONTEXT=1 for this process.\n"
        << "  - Second-pass flags override the adaptive retry tuning env vars for this run.\n"
        << "  - Longer windows (15s+) are often needed before `news` yields data.\n"
        << "  - Official docs: " << kDocsUrl << "\n";
}

} // namespace

int main(int argc, char** argv) {
    std::string symbol = "SPY";
    std::vector<std::string> facets = {"options_flow", "alerts", "stock_state", "news", "gex"};
    std::string sampleMs = "15000";
    std::string maxFrames = "16";
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
        std::cerr << "Unknown argument: " << arg << "\n";
        printUsage(argv[0]);
        return 2;
    }

    ensureCredentialFileFallback();
    (void)::setenv("LONG_ENABLE_UW_WEBSOCKET_CONTEXT", "1", 1);
    (void)::setenv("LONG_UW_WS_SAMPLE_MS", sampleMs.c_str(), 1);
    (void)::setenv("LONG_UW_WS_MAX_FRAMES", maxFrames.c_str(), 1);
    if (secondPassSampleMs.has_value()) {
        (void)::setenv("LONG_UW_WS_SECOND_PASS_SAMPLE_MS", secondPassSampleMs->c_str(), 1);
    }
    if (secondPassTotalMs.has_value()) {
        (void)::setenv("LONG_UW_WS_SECOND_PASS_TOTAL_MS", secondPassTotalMs->c_str(), 1);
    }
    if (secondPassLimit.has_value()) {
        (void)::setenv("LONG_UW_WS_SECOND_PASS_CHANNEL_LIMIT", secondPassLimit->c_str(), 1);
    }

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
        {"triage_summary", buildTriageSummary(step)},
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
