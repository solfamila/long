#include "uw_packet_builder.h"

#include <algorithm>
#include <cmath>
#include <string>

namespace uw_context_service {
namespace {

std::string laneTask(const BuildRequest& request) {
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

std::size_t laneTokenBudget(Lane lane) {
    return lane == Lane::Deep ? 8000U : 2000U;
}

std::size_t laneSignalLimit(Lane lane) {
    return lane == Lane::Deep ? 20U : 8U;
}

std::size_t laneDigestLimit(Lane lane) {
    return lane == Lane::Deep ? 40U : 12U;
}

std::string firstPresentString(const json& payload, std::initializer_list<const char*> keys) {
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

json makeSignal(const std::string& signalId,
                const std::string& source,
                const std::string& summary,
                const json& evidence,
                double score,
                std::size_t recordIndex) {
    return {
        {"signal_id", signalId},
        {"source", source},
        {"summary", summary},
        {"evidence", evidence},
        {"score", score},
        {"record_index", recordIndex}
    };
}

json buildTopSignals(const json& localEvidence,
                     const NormalizedContext& normalized,
                     std::size_t limit) {
    json signals = json::array();
    const json report = localEvidence.value("report", json::object());
    const std::string headline = localEvidence.value("headline", firstPresentString(report, {"headline", "title"}));
    const std::string detail = localEvidence.value("detail", firstPresentString(report, {"summary", "why_it_matters"}));
    if (!headline.empty()) {
        signals.push_back(makeSignal("local-headline", "local_evidence", headline, report, 1.0, 0));
    }
    if (!detail.empty() && signals.size() < limit) {
        signals.push_back(makeSignal("local-detail", "local_evidence", detail, report, 0.9, 0));
    }

    const json citations = localEvidence.value("citation_rows", json::array());
    if (citations.is_array()) {
        for (std::size_t index = 0; index < citations.size() && signals.size() < limit; ++index) {
            const json& citation = citations[index];
            signals.push_back(makeSignal(
                "citation-" + std::to_string(index + 1),
                "local_citation",
                firstPresentString(citation, {"label", "headline", "artifact_id"}),
                citation,
                0.75 - static_cast<double>(index) * 0.05,
                index));
        }
    }

    if (normalized.items.is_array()) {
        for (std::size_t index = 0; index < normalized.items.size() && signals.size() < limit; ++index) {
            const json& item = normalized.items[index];
            signals.push_back(makeSignal(
                "external-" + std::to_string(index + 1),
                item.value("provider", std::string("external_context")),
                firstPresentString(item.value("structured", json::object()), {"summary", "headline", "title"}).empty()
                    ? item.value("kind", std::string("external_context"))
                    : firstPresentString(item.value("structured", json::object()), {"summary", "headline", "title"}),
                item,
                0.7 - static_cast<double>(index) * 0.05,
                index));
        }
    }
    return signals;
}

json buildRecordDigests(const NormalizedContext& normalized, std::size_t limit) {
    json digests = json::array();
    if (!normalized.items.is_array()) {
        return digests;
    }
    for (std::size_t index = 0; index < normalized.items.size() && digests.size() < limit; ++index) {
        const json& item = normalized.items[index];
        const std::string serialized = item.dump();
        digests.push_back({
            {"line_number", index + 1},
            {"schema", item.value("schema", std::string())},
            {"source", item.value("provider", std::string())},
            {"key", item.value("provider_record_id", item.value("symbol", std::string()))},
            {"outcome", item.value("provider_status", std::string())},
            {"http_status", item.value("http_status", 0)},
            {"latency_ms", item.value("latency_ms", 0)},
            {"payload_bytes", serialized.size()},
            {"preview_excerpt", serialized.substr(0, std::min<std::size_t>(serialized.size(), 160U))}
        });
    }
    return digests;
}

json buildLocalEvidencePacket(const json& localEvidence,
                              const BuildRequest& request) {
    const json entity = localEvidence.value("entity", json::object());
    const json report = localEvidence.value("report", json::object());
    const json evidence = localEvidence.value("evidence", json::object());
    const json replayRange = localEvidence.value("replay_range", json(nullptr));
    const json artifact = localEvidence.value("artifact", json::object());
    const json base = {
        {"symbol", entity.value("symbol", entity.value("ticker", std::string()))},
        {"revision_id", request.revisionId},
        {"incident", request.logicalIncidentId == 0 ? json(nullptr) : json{{"logical_incident_id", request.logicalIncidentId}, {"headline", localEvidence.value("headline", std::string())}, {"detail", localEvidence.value("detail", std::string())}}},
        {"order_case", request.orderId == 0 && request.execId.empty() && request.traceId == 0 && request.permId == 0
            ? json(nullptr)
            : json{{"trace_id", request.traceId}, {"order_id", request.orderId}, {"perm_id", request.permId}, {"exec_id", request.execId}}},
        {"top_findings", evidence.value("citations", json::array())},
        {"supporting_findings", localEvidence.value("citation_rows", json::array())},
        {"counterevidence", json::array()},
        {"protected_window", report.value("protected_window", json::object())},
        {"window", replayRange.is_object() ? replayRange : json::object()},
        {"order_overlap", artifact.value("anchor", json::object())},
        {"related_incidents", localEvidence.value("incident_rows", json::array())}
    };
    return base;
}

} // namespace

json buildPacketArtifact(const json& localEvidence,
                         const NormalizedContext& normalized,
                         const BuildRequest& request) {
    const std::size_t tokenBudget = laneTokenBudget(request.lane);
    const json topSignals = buildTopSignals(localEvidence, normalized, laneSignalLimit(request.lane));
    const json recordDigests = buildRecordDigests(normalized, laneDigestLimit(request.lane));
    const std::string packetKind = request.lane == Lane::Deep
        ? "deep_incident_packet"
        : (request.requestKind == "order_case_enrichment" ? "fast_order_case_packet" : "fast_incident_packet");
    const json packet = {
        {"schema", request.lane == Lane::Deep ? "uw_deep_context_packet" : "uw_fast_context_packet"},
        {"packet_kind", packetKind},
        {"task", laneTask(request)},
        {"focus_question", request.focusQuestion.empty() ? json(nullptr) : json(request.focusQuestion)},
        {"local_evidence", buildLocalEvidencePacket(localEvidence, request)},
        {"external_context", {
            {"provider", normalized.provider},
            {"fetched_at", normalized.fetchedAtUtc},
            {"cache_status", normalized.cacheStatus},
            {"items", normalized.items},
            {"summaries", normalized.summaries},
            {"warnings", normalized.warnings}
        }},
        {"token_budget", tokenBudget},
        {"top_signals", topSignals},
        {"record_digests", recordDigests},
        {"schema_sources", normalized.schemaSources},
        {"source_attribution", normalized.sourceAttribution},
        {"symbol", localEvidence.value("entity", json::object()).value("symbol", std::string())}
    };
    const std::size_t tokenEstimate = static_cast<std::size_t>(std::ceil(packet.dump().size() / 4.0));

    json artifact = packet;
    artifact["token_estimate"] = tokenEstimate;
    artifact["trimmed_for_budget"] = tokenEstimate > tokenBudget;
    return artifact;
}

} // namespace uw_context_service
