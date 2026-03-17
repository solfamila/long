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
        {"model", hasExternalContext ? json(deep ? "gemini-2.5-flash" : "gemini-3.1-flash-lite") : json(nullptr)},
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
            if (fetchSteps.back().status != "ok") {
                fetchSteps.push_back(restConnector_.fetch(plan));
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

        const json providerMetadata = {
            {"service", "uw_context_service"},
            {"provider_path_used", providerPathUsed},
            {"fetched_at_utc", fetchedAtUtc},
            {"provider_steps", providerSteps},
            {"packet_artifact", packetArtifact}
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
