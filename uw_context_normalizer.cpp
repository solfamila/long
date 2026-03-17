#include "uw_context_normalizer.h"

#include <algorithm>
#include <set>

namespace uw_context_service {

NormalizedContext normalizeProviderRecords(const std::vector<ProviderStep>& steps) {
    NormalizedContext context;
    context.fetchedAtUtc = nowUtc();
    std::set<std::string> seenRecordIds;
    std::map<std::string, std::vector<std::size_t>> indexesByKind;
    for (const ProviderStep& step : steps) {
        context.sourceAttribution.push_back({
            {"provider", step.provider},
            {"status", step.status},
            {"reason", step.reason.empty() ? json(nullptr) : json(step.reason)},
            {"latency_ms", step.latencyMs},
            {"metadata", step.metadata}
        });

        if (step.status != "ok" && !step.reason.empty()) {
            context.warnings.push_back(step.provider + ": " + step.reason);
        }

        if (!step.rawRecords.is_array()) {
            continue;
        }

        for (const auto& record : step.rawRecords) {
            if (!record.is_object()) {
                continue;
            }
            const std::string recordId = record.value("provider_record_id", stableHashHex(record.dump()));
            if (!seenRecordIds.insert(recordId).second) {
                continue;
            }
            json normalized = {
                {"kind", record.value("kind", std::string("external_context"))},
                {"provider_record_id", recordId},
                {"symbol", record.value("symbol", std::string())},
                {"option_symbol", record.value("option_symbol", std::string())},
                {"ts_provider_ns", record.value("ts_provider_ns", 0ULL)},
                {"ts_fetched_ns", record.value("ts_fetched_ns", 0ULL)},
                {"structured", record.value("structured", json::object())},
                {"raw_excerpt", record.value("raw_excerpt", json::object())}
            };
            normalized["provider"] = step.provider;
            normalized["provider_status"] = step.status;
            normalized["provider_reason"] = step.reason;
            normalized["latency_ms"] = record.value("latency_ms", step.latencyMs);
            normalized["schema"] = record.value("schema", std::string("uw_external_context_record_v1"));
            context.items.push_back(std::move(normalized));

            const std::size_t itemIndex = context.items.size() - 1;
            indexesByKind[record.value("kind", std::string("external_context"))].push_back(itemIndex);

            const std::string schema = record.value("schema", std::string());
            if (!schema.empty() &&
                std::find(context.schemaSources.begin(), context.schemaSources.end(), schema) == context.schemaSources.end()) {
                context.schemaSources.push_back(schema);
            }
        }
    }

    for (const auto& [kind, indexes] : indexesByKind) {
        if (indexes.empty()) {
            continue;
        }
        context.summaries.push_back({
            {"facet", kind},
            {"headline", kind + " context returned " + std::to_string(indexes.size()) + " record(s)"},
            {"body", "Provider-normalized external context for facet " + kind + "."},
            {"relevance", std::min(1.0, 0.5 + static_cast<double>(indexes.size()) * 0.1)},
            {"item_indexes", indexes}
        });
    }
    return context;
}

} // namespace uw_context_service