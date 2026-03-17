#include "uw_context_service.h"

#include "uw_context_cache.h"
#include "uw_context_connectors.h"
#include "uw_gemini_executor.h"
#include "uw_context_normalizer.h"
#include "uw_packet_builder.h"
#include "uw_runtime.h"

#include <initializer_list>
#include <optional>
#include <cstdlib>
#include <algorithm>
#include <cctype>

namespace uw_context_service {
namespace {

std::string firstStringValue(const json& payload,
                             std::initializer_list<const char*> keys) {
    if (!payload.is_object()) {
        return {};
    }
    for (const char* key : keys) {
        const auto it = payload.find(key);
        if (it != payload.end() && it->is_string()) {
            const std::string value = it->get<std::string>();
            if (!value.empty()) {
                return value;
            }
        }
    }
    return {};
}

std::string stringValueOrEmpty(const json& payload, const char* key) {
    if (!payload.is_object()) {
        return {};
    }
    const auto it = payload.find(key);
    if (it == payload.end() || !it->is_string()) {
        return {};
    }
    return it->get<std::string>();
}

json localEvidenceResultOrFallback(const tape_engine::QueryResponse& localEvidence) {
    if (localEvidence.result.is_object() &&
        localEvidence.result.value("schema", std::string()) == tape_engine::kInvestigationResultSchema) {
        return localEvidence.result;
    }
    const json artifact = localEvidence.summary.value("artifact", json::object());
    const json report = localEvidence.summary.value("report", json::object());
    return {
        {"schema", tape_engine::kInvestigationResultSchema},
        {"version", tape_engine::kInvestigationResultVersion},
        {"artifact_id", artifact.value("artifact_id", std::string())},
        {"artifact_kind", artifact.value("artifact_type", std::string())},
        {"headline", firstStringValue(report, {"headline", "title", "summary"})},
        {"detail", firstStringValue(report, {"summary", "why_it_matters"})},
        {"served_revision_id", localEvidence.summary.value("served_revision_id", 0ULL)},
        {"includes_mutable_tail", localEvidence.summary.value("includes_mutable_tail", false)},
        {"artifact", artifact},
        {"entity", localEvidence.summary.value("entity", json::object())},
        {"report", report},
        {"evidence", localEvidence.summary.value("evidence", json::object())},
        {"data_quality", localEvidence.summary.value("data_quality", json::object())},
        {"replay_range", localEvidence.summary.value("replay_range", json(nullptr))},
        {"incident_rows", json::array()},
        {"citation_rows", localEvidence.summary.value("evidence", json::object()).value("citations", json::array())},
        {"events", localEvidence.events.is_array() ? localEvidence.events : json::array()}
    };
}

std::string inferSymbol(const json& localEvidence) {
    auto symbolFromInstrumentId = [](const std::string& instrumentId) {
        if (instrumentId.empty()) {
            return std::string();
        }
        const std::size_t lastColon = instrumentId.rfind(':');
        if (lastColon != std::string::npos && lastColon + 1 < instrumentId.size()) {
            return instrumentId.substr(lastColon + 1);
        }
        return std::string();
    };

    const json entity = localEvidence.value("entity", json::object());
    std::string symbol = firstStringValue(entity, {"symbol", "ticker"});
    if (!symbol.empty()) {
        return symbol;
    }

    const json artifact = localEvidence.value("artifact", json::object());
    symbol = symbolFromInstrumentId(artifact.value("instrument_id", std::string()));
    if (!symbol.empty()) {
        return symbol;
    }

    symbol = firstStringValue(localEvidence.value("report", json::object()), {"symbol", "ticker"});
    if (!symbol.empty()) {
        return symbol;
    }

    const json relatedIncidents = localEvidence.value("incident_rows", json::array());
    if (relatedIncidents.is_array()) {
        for (const auto& row : relatedIncidents) {
            if (!row.is_object()) {
                continue;
            }
            symbol = firstStringValue(row, {"symbol", "ticker"});
            if (!symbol.empty()) {
                return symbol;
            }
            symbol = symbolFromInstrumentId(row.value("instrument_id", std::string()));
            if (!symbol.empty()) {
                return symbol;
            }
        }
    }

    const json events = localEvidence.value("events", json::array());
    if (events.is_array()) {
        for (const auto& event : events) {
            if (!event.is_object()) {
                continue;
            }
            symbol = firstStringValue(event, {"symbol", "ticker"});
            if (!symbol.empty()) {
                return symbol;
            }
            symbol = symbolFromInstrumentId(event.value("instrument_id", std::string()));
            if (!symbol.empty()) {
                return symbol;
            }
        }
    }

    return {};
}

std::vector<std::string> defaultFacetsFor(const BuildRequest& request) {
    if (!request.requestedFacets.empty()) {
        return request.requestedFacets;
    }
    if (request.requestKind == "deep_enrichment") {
        return {"options_flow", "alerts", "gex", "news", "stock_state"};
    }
    if (request.requestKind == "refresh_external_context") {
        return {"options_flow", "alerts", "gex", "news", "stock_state"};
    }
    if (request.requestKind == "order_case_enrichment") {
        return {"options_flow", "news", "stock_state"};
    }
    return {"options_flow", "gex", "news", "stock_state"};
}

std::pair<std::uint64_t, std::uint64_t> inferWindowNs(const json& localEvidence) {
    const json replayRange = localEvidence.value("replay_range", json(nullptr));
    if (replayRange.is_object()) {
        return {replayRange.value("start_engine_ns", replayRange.value("first_session_seq", 0ULL)),
                replayRange.value("end_engine_ns", replayRange.value("last_session_seq", 0ULL))};
    }
    const json report = localEvidence.value("report", json::object());
    const json protectedWindow = report.value("protected_window", json::object());
    if (protectedWindow.is_object()) {
        return {protectedWindow.value("start_engine_ns", 0ULL),
                protectedWindow.value("end_engine_ns", 0ULL)};
    }
    return {0ULL, 0ULL};
}

std::vector<std::string> evidenceKinds(const json& localEvidence) {
    std::vector<std::string> kinds;
    const json citations = localEvidence.value("citation_rows", json::array());
    if (!citations.is_array()) {
        return kinds;
    }
    for (const auto& citation : citations) {
        if (!citation.is_object()) {
            continue;
        }
        const std::string kind = citation.value("kind", std::string());
        if (!kind.empty()) {
            kinds.push_back(kind);
        }
    }
    return kinds;
}

bool envFlagEnabled(const char* key) {
    if (const char* raw = std::getenv(key); raw != nullptr && *raw != '\0') {
        std::string normalized(raw);
        std::transform(normalized.begin(), normalized.end(), normalized.begin(), [](unsigned char ch) {
            return static_cast<char>(std::tolower(ch));
        });
        return normalized == "1" || normalized == "true" || normalized == "yes" ||
               normalized == "on";
    }
    return false;
}

bool websocketSecondaryLaneRequested(const BuildRequest& request) {
    if (envFlagEnabled("LONG_DISABLE_UW_WEBSOCKET_CONTEXT")) {
        return false;
    }
    if (envFlagEnabled("LONG_ENABLE_UW_WEBSOCKET_CONTEXT")) {
        return true;
    }
    return request.forceRefresh || request.includeLiveTail;
}

std::string cacheKeyFor(const json& localEvidence, const BuildRequest& request) {
    return localEvidence.value("artifact_id", std::string()) + ":" + request.requestKind + ":" +
        (request.lane == Lane::Deep ? "deep" : "fast") + ":" +
        (websocketSecondaryLaneRequested(request) ? "live" : "snapshot");
}

json providerStepToJson(const ProviderStep& step) {
    return {
        {"provider", step.provider},
        {"status", step.status},
        {"reason", step.reason.empty() ? json(nullptr) : json(step.reason)},
        {"latency_ms", step.latencyMs},
        {"request_payload", step.requestPayload},
        {"metadata", step.metadata}
    };
}

json liveCaptureSummary(const json& providerSteps, bool requested) {
    json summary = {
        {"requested", requested},
        {"provider", "uw_ws"},
        {"status", requested ? json("not_attempted") : json("not_requested")},
        {"outcome", requested ? json("not_attempted") : json("not_requested")},
        {"reason", requested ? json(nullptr) : json("secondary_lane_not_requested")},
        {"source", nullptr},
        {"symbol", nullptr},
        {"subscription_channels", json::array()},
        {"channel_count", 0ULL},
        {"sample_ms", nullptr},
        {"max_frames", nullptr},
        {"has_live_data", false},
        {"raw_frame_count", 0ULL},
        {"candidate_data_frame_count", 0ULL},
        {"data_frame_count", 0ULL},
        {"normalized_event_count", 0ULL},
        {"join_ack_frame_count", 0ULL},
        {"error_frame_count", 0ULL},
        {"duplicate_join_frame_count", 0ULL},
        {"filtered_mismatch_frame_count", 0ULL},
        {"ambient_global_frame_count", 0ULL},
        {"unparsed_frame_count", 0ULL},
        {"close_frame_count", 0ULL},
        {"frame_preview_count", 0ULL},
        {"first_frame_preview", nullptr},
        {"channel_outcomes", json::array()},
        {"pass_count", 0ULL},
        {"targeted_retry_used", false},
        {"rescued_by_targeted_pass", false},
        {"last_error", nullptr},
        {"summary_text", requested
            ? json("UW websocket capture was requested but not attempted.")
            : json("UW websocket capture was not requested for this enrichment.")}
    };
    if (!providerSteps.is_array()) {
        return summary;
    }

    for (const auto& step : providerSteps) {
        if (!step.is_object() || step.value("provider", std::string()) != "uw_ws") {
            continue;
        }
        const json requestPayload = step.value("request_payload", json::object());
        const json metadata = step.value("metadata", json::object());
        const json channels = requestPayload.value("subscription_channels", json::array());
        const json previews = metadata.value("frame_previews", json::array());
        const json channelStats = metadata.value("channel_stats", json::object());
        const json capturePasses = metadata.value("capture_passes", json::array());
        const std::string status = step.value("status", std::string("unavailable"));
        const std::string reason = stringValueOrEmpty(step, "reason");
        const std::uint64_t rawFrameCount = metadata.value("raw_frame_count", 0ULL);
        const std::uint64_t candidateDataFrameCount = metadata.value("candidate_data_frame_count", 0ULL);
        const std::uint64_t dataFrameCount = metadata.value("data_frame_count", 0ULL);
        const std::uint64_t joinAckFrameCount = metadata.value("join_ack_frame_count", 0ULL);
        const std::uint64_t duplicateJoinFrameCount = metadata.value("duplicate_join_frame_count", 0ULL);
        const std::uint64_t errorFrameCount = metadata.value("error_frame_count", 0ULL);
        const std::uint64_t ambientGlobalFrameCount = metadata.value("ambient_global_frame_count", 0ULL);
        const std::uint64_t unparsedFrameCount = metadata.value("unparsed_frame_count", 0ULL);
        const bool hasLiveData = dataFrameCount > 0ULL;

        summary["status"] = status;
        summary["outcome"] = hasLiveData ? json("live_data")
                                          : json(reason.empty() ? status : reason);
        summary["reason"] = reason.empty() ? json(nullptr) : json(reason);
        summary["source"] = metadata.contains("source") ? metadata.at("source") : json(nullptr);
        summary["symbol"] = requestPayload.contains("symbol") ? requestPayload.at("symbol") : json(nullptr);
        summary["subscription_channels"] = channels.is_array() ? channels : json::array();
        summary["channel_count"] = channels.is_array() ? static_cast<std::uint64_t>(channels.size()) : 0ULL;
        summary["sample_ms"] = requestPayload.contains("sample_ms") ? requestPayload.at("sample_ms") : json(nullptr);
        summary["max_frames"] = requestPayload.contains("max_frames") ? requestPayload.at("max_frames") : json(nullptr);
        summary["has_live_data"] = hasLiveData;
        summary["raw_frame_count"] = rawFrameCount;
        summary["candidate_data_frame_count"] = candidateDataFrameCount;
        summary["data_frame_count"] = dataFrameCount;
        summary["normalized_event_count"] = metadata.value("normalized_event_count", 0ULL);
        summary["join_ack_frame_count"] = joinAckFrameCount;
        summary["error_frame_count"] = errorFrameCount;
        summary["duplicate_join_frame_count"] = duplicateJoinFrameCount;
        summary["filtered_mismatch_frame_count"] = metadata.value("filtered_mismatch_frame_count", 0ULL);
        summary["ambient_global_frame_count"] = ambientGlobalFrameCount;
        summary["unparsed_frame_count"] = unparsedFrameCount;
        summary["close_frame_count"] = metadata.value("close_frame_count", 0ULL);
        summary["frame_preview_count"] = previews.is_array() ? static_cast<std::uint64_t>(previews.size()) : 0ULL;
        summary["first_frame_preview"] = previews.is_array() && !previews.empty() ? previews.front() : json(nullptr);
        summary["pass_count"] = capturePasses.is_array() ? static_cast<std::uint64_t>(capturePasses.size()) : 0ULL;
        summary["targeted_retry_used"] = metadata.value("adaptive_retry_used", false);
        summary["rescued_by_targeted_pass"] = metadata.value("rescued_by_targeted_pass", false);
        json channelOutcomes = json::array();
        if (channels.is_array()) {
            for (const auto& channelValue : channels) {
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
                std::string channelOutcome = "idle";
                if (channelDataCount > 0ULL) {
                    channelOutcome = "live_data";
                } else if (channelAmbientCount > 0ULL) {
                    channelOutcome = "ambient_global_only";
                } else if (channelDuplicateJoinCount > 0ULL) {
                    channelOutcome = "already_in_room_only";
                } else if (channelErrorCount > 0ULL) {
                    channelOutcome = "error_frames_only";
                } else if (channelJoinAckCount > 0ULL) {
                    channelOutcome = "join_ack_only";
                } else if (channelMismatchCount > 0ULL) {
                    channelOutcome = "filtered_mismatch_only";
                }
                channelOutcomes.push_back({
                    {"channel", channel},
                    {"channel_family", stats.value("channel_family", channel.substr(0, channel.find(':')))},
                    {"outcome", channelOutcome},
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
        summary["channel_outcomes"] = std::move(channelOutcomes);
        summary["last_error"] = metadata.contains("last_error") ? metadata.at("last_error") : json(nullptr);

        if (hasLiveData) {
            summary["summary_text"] =
                "UW websocket captured " + std::to_string(dataFrameCount) + " live data frame(s)" +
                (summary.value("rescued_by_targeted_pass", false)
                    ? " after a targeted retry."
                    : ".");
        } else if (ambientGlobalFrameCount > 0ULL) {
            summary["summary_text"] =
                "UW websocket saw " + std::to_string(ambientGlobalFrameCount) +
                " live global frame(s) without symbol binding, so they were kept out of symbol-scoped context.";
        } else if (reason == "join_ack_only") {
            summary["summary_text"] =
                "UW websocket joined successfully but saw no live data frames in the capture window.";
        } else if (reason == "already_in_room_only") {
            summary["summary_text"] =
                "UW websocket reported duplicate joins only and returned no live data frames.";
        } else if (reason == "error_frames_only") {
            summary["summary_text"] =
                "UW websocket returned only error frames and produced no live data.";
        } else if (reason == "unparsed_frames_only") {
            summary["summary_text"] =
                "UW websocket returned frames that could not be parsed into supported live records.";
        } else if (reason == "no_live_frames") {
            summary["summary_text"] =
                "UW websocket connected but the capture window ended without live data frames.";
        } else if (reason == "blocked_missing_runtime_capability") {
            summary["summary_text"] =
                "UW websocket is blocked because the current libcurl runtime lacks wss support.";
        } else if (!reason.empty()) {
            summary["summary_text"] = "UW websocket capture did not yield live data: " + reason + ".";
        } else {
            summary["summary_text"] = "UW websocket capture produced no live data summary.";
        }
        break;
    }

    return summary;
}

std::string interpretationTask(const BuildRequest& request) {
    if (request.requestKind == "deep_enrichment") {
        return "explain_and_rank_causes";
    }
    if (request.requestKind == "order_case_enrichment") {
        return "summarize_order_case_context";
    }
    if (request.requestKind == "refresh_external_context") {
        return "refresh_external_context";
    }
    return "summarize_as_json";
}

json buildInterpretation(const BuildRequest& request,
                         const json& packetArtifact,
                         bool hasExternalContext) {
    const bool deep = request.lane == Lane::Deep;
    return {
        {"status", hasExternalContext ? "ready_for_inference" : "unavailable"},
        {"lane", deep ? "deep" : "fast"},
        {"task", interpretationTask(request)},
        {"model", hasExternalContext ? json("gemini-3.1-flash-lite-preview") : json(nullptr)},
        {"finish_reason", hasExternalContext ? "pending_execution" : "not_requested"},
        {"content", nullptr},
        {"packet_artifact", packetArtifact}
    };
}

class ContextService {
public:
    tape_engine::QueryResponse buildResponse(const tape_engine::QueryRequest& request,
                                             const tape_engine::QueryResponse& localEvidence,
                                             const BuildRequest& buildRequest) {
        cache_.setRootDir(buildRequest.dataDir / "uw_context" / "cache");
        const json localResult = localEvidenceResultOrFallback(localEvidence);
        const std::string cacheKey = cacheKeyFor(localResult, buildRequest);

        json externalContext = {
            {"provider", "unusual_whales"},
            {"fetched_at", nowUtc()},
            {"cache_status", buildRequest.forceRefresh ? "refresh_requested" : "miss"},
            {"items", json::array()},
            {"summaries", json::array()},
            {"warnings", json::array()}
        };
        json interpretation = json::object();
        json packetArtifact = json::object();
        json providerSteps = json::array();
        json cacheSummary = {
            {"context_status", buildRequest.forceRefresh ? "refresh_requested" : "miss"},
            {"interpretation_status", "miss"},
            {"refreshed", buildRequest.forceRefresh}
        };

        std::string fetchedAtUtc = nowUtc();
        std::string providerPathUsed = buildRequest.forceRefresh ? "refresh_requested" : "none";
        const std::uint64_t cacheTtlSeconds = buildRequest.requestKind == "deep_enrichment" ? 300ULL : 60ULL;

        if (!buildRequest.forceRefresh) {
            if (const std::optional<CacheEntry> cached = cache_.lookup(cacheKey, cacheTtlSeconds); cached.has_value()) {
                providerSteps.push_back({
                    {"provider", "cache"},
                    {"status", "hit"},
                    {"reason", nullptr}
                });
                externalContext = cached->externalContext;
                interpretation = cached->interpretation;
                packetArtifact = cached->packetArtifact;
                fetchedAtUtc = cached->fetchedAtUtc;
                providerPathUsed = cached->providerPathUsed.empty() ? "cache" : cached->providerPathUsed;
                cacheSummary["context_status"] = "hit";
                cacheSummary["interpretation_status"] = interpretation.value("status", std::string()) == "completed" ? "hit" : "miss";
            }
        }

        if (!cacheSummary.value("context_status", std::string()).starts_with("hit")) {
            providerSteps.push_back({
                {"provider", "cache"},
                {"status", buildRequest.forceRefresh ? "bypassed" : "miss"},
                {"reason", nullptr}
            });

            const FetchPlan plan{
                .artifactId = localResult.value("artifact_id", std::string()),
                .requestKind = buildRequest.requestKind,
                .symbol = inferSymbol(localResult),
                .revisionId = buildRequest.revisionId,
                .logicalIncidentId = buildRequest.logicalIncidentId,
                .traceId = buildRequest.traceId,
                .orderId = buildRequest.orderId,
                .permId = buildRequest.permId,
                .execId = buildRequest.execId,
                .windowStartNs = inferWindowNs(localResult).first,
                .windowEndNs = inferWindowNs(localResult).second,
                .forceRefresh = buildRequest.forceRefresh,
                .includeLiveTail = buildRequest.includeLiveTail,
                .evidenceKinds = evidenceKinds(localResult),
                .facets = defaultFacetsFor(buildRequest)
            };

            std::vector<ProviderStep> fetchSteps;
            fetchSteps.push_back(mcpConnector_.fetch(plan));
            const std::vector<std::string> unresolvedMcpFacets =
                unresolvedUWMcpFacets(fetchSteps.back(), plan.facets);
            if (!unresolvedMcpFacets.empty()) {
                FetchPlan restPlan = plan;
                restPlan.facets = unresolvedMcpFacets;
                fetchSteps.push_back(restConnector_.fetch(restPlan));
            }
            if (websocketSecondaryLaneRequested(buildRequest)) {
                fetchSteps.push_back(wsConnector_.fetch(plan));
            }

            for (const ProviderStep& step : fetchSteps) {
                providerSteps.push_back(providerStepToJson(step));
            }

            const NormalizedContext normalized = normalizeProviderRecords(fetchSteps);
            externalContext = {
                {"provider", normalized.provider},
                {"fetched_at", normalized.fetchedAtUtc},
                {"cache_status", normalized.cacheStatus},
                {"items", normalized.items},
                {"summaries", normalized.summaries},
                {"warnings", normalized.warnings}
            };
            packetArtifact = buildPacketArtifact(localResult, normalized, buildRequest);
            const bool hasExternalContext = externalContext.value("items", json::array()).is_array() &&
                !externalContext.value("items", json::array()).empty();
            const bool shouldRunGemini = hasExternalContext && buildRequest.requestKind != "refresh_external_context";
            const GeminiExecutionResult geminiResult = executeGeminiPacket(packetArtifact, buildRequest, shouldRunGemini);
            interpretation = buildInterpretation(buildRequest, packetArtifact, hasExternalContext);
            interpretation["status"] = geminiResult.status;
            interpretation["model"] = geminiResult.model.empty() ? interpretation.value("model", json(nullptr)) : json(geminiResult.model);
            interpretation["finish_reason"] = geminiResult.finishReason.empty() ? interpretation.value("finish_reason", std::string()) : geminiResult.finishReason;
            interpretation["content"] = geminiResult.content;
            interpretation["warnings"] = geminiResult.warnings;
            interpretation["latency_ms"] = geminiResult.latencyMs;
            interpretation["json_valid"] = geminiResult.jsonValid;
            interpretation["schema_valid"] = geminiResult.schemaValid;
            if (!geminiResult.error.empty()) {
                interpretation["error"] = geminiResult.error;
            }

            for (const ProviderStep& step : fetchSteps) {
                if (step.status == "ok") {
                    providerPathUsed = step.provider;
                    break;
                }
            }

            cache_.store(cacheKey, CacheEntry{
                .externalContext = externalContext,
                .interpretation = interpretation,
                .packetArtifact = packetArtifact,
                .fetchedAtUtc = fetchedAtUtc,
                .fetchedAtUnixSeconds = nowUnixSeconds(),
                .providerPathUsed = providerPathUsed
            });
        }

        const bool hasExternalContext = externalContext.value("items", json::array()).is_array() &&
            !externalContext.value("items", json::array()).empty();
        const bool hasInterpretation = interpretation.value("status", std::string()) == "completed" &&
            interpretation.value("content", json(nullptr)).is_object();
        const json degradation = {
            {"is_degraded", !hasExternalContext || (!hasInterpretation && buildRequest.requestKind != "refresh_external_context")},
            {"code", !hasExternalContext
                ? json("external_context_unavailable")
                : (!hasInterpretation && buildRequest.requestKind != "refresh_external_context"
                    ? json("gemini_interpretation_unavailable")
                    : json(nullptr))},
            {"message", !hasExternalContext
                ? json("Returning deterministic local evidence only because mediated UW context is unavailable.")
                : (!hasInterpretation && buildRequest.requestKind != "refresh_external_context"
                    ? json("Returning local evidence and external context without Gemini interpretation.")
                    : json(nullptr))}
        };

        const json liveCapture = liveCaptureSummary(providerSteps, websocketSecondaryLaneRequested(buildRequest));

        const json providerMetadata = {
            {"service", "uw_context_service"},
            {"provider_path_used", providerPathUsed},
            {"fetched_at_utc", fetchedAtUtc},
            {"provider_steps", providerSteps},
            {"packet_artifact", packetArtifact},
            {"live_capture_summary", liveCapture}
        };

        tape_engine::QueryResponse response;
        response.requestId = request.requestId;
        response.operation = request.operation.empty()
            ? std::string(tape_engine::queryOperationName(request.operationKind))
            : request.operation;
        response.status = localEvidence.status;
        response.error = localEvidence.error;
        response.summary = localEvidence.summary.is_object() ? localEvidence.summary : json::object();
        response.events = localEvidence.events.is_array() ? localEvidence.events : json::array();
        response.summary["request_kind"] = buildRequest.requestKind;
        response.summary["provider_metadata"] = providerMetadata;
        response.summary["degradation"] = degradation;
        response.result = {
            {"schema", tape_engine::kEnrichmentResultSchema},
            {"version", tape_engine::kEnrichmentResultVersion},
            {"request_kind", buildRequest.requestKind},
            {"artifact_id", localResult.value("artifact_id", std::string())},
            {"headline", localResult.value("headline", std::string())},
            {"detail", localResult.value("detail", std::string())},
            {"local_evidence", localResult},
            {"external_context", externalContext},
            {"interpretation", interpretation},
            {"provider_metadata", providerMetadata},
            {"live_capture_summary", liveCapture},
            {"degradation", degradation},
            {"cache", cacheSummary}
        };
        return response;
    }

private:
    ContextCache cache_;
    UWMcpConnector mcpConnector_;
    UWRestConnector restConnector_;
    UWWsConnector wsConnector_;
};

ContextService& sharedService() {
    static ContextService service;
    return service;
}

} // namespace

tape_engine::QueryResponse buildEnrichmentResponse(const tape_engine::QueryRequest& request,
                                                   const tape_engine::QueryResponse& localEvidence,
                                                   const BuildRequest& buildRequest) {
    return sharedService().buildResponse(request, localEvidence, buildRequest);
}

} // namespace uw_context_service
