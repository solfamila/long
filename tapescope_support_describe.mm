#include "tapescope_support.h"

#include <sstream>

namespace tapescope_support {

std::string FirstPresentString(const json& payload,
                               std::initializer_list<const char*> keys) {
    if (!payload.is_object()) {
        return {};
    }
    for (const char* key : keys) {
        const auto it = payload.find(key);
        if (it != payload.end() && it->is_string()) {
            return it->get<std::string>();
        }
    }
    return {};
}

std::uint64_t FirstPresentUInt64(const json& payload,
                                 std::initializer_list<const char*> keys) {
    if (!payload.is_object()) {
        return 0;
    }
    for (const char* key : keys) {
        const auto it = payload.find(key);
        if (it != payload.end() && it->is_number_unsigned()) {
            return it->get<std::uint64_t>();
        }
        if (it != payload.end() && it->is_number_integer()) {
            const long long value = it->get<long long>();
            if (value >= 0) {
                return static_cast<std::uint64_t>(value);
            }
        }
    }
    return 0;
}

std::string DescribeRecentHistoryEntry(const json& entry) {
    std::ostringstream out;
    out << "kind: " << entry.value("kind", std::string("--")) << "\n";
    out << "target_id: " << entry.value("target_id", std::string("--")) << "\n";
    const std::string artifactId = entry.value("artifact_id", std::string());
    if (!artifactId.empty()) {
        out << "artifact_id: " << artifactId << "\n";
    }
    const std::uint64_t firstSessionSeq = entry.value("first_session_seq", 0ULL);
    const std::uint64_t lastSessionSeq = entry.value("last_session_seq", 0ULL);
    if (firstSessionSeq > 0 && lastSessionSeq >= firstSessionSeq) {
        out << "session_seq: [" << firstSessionSeq << ", " << lastSessionSeq << "]\n";
    }
    const std::string anchorKind = entry.value("anchor_kind", std::string());
    const std::string anchorValue = entry.value("anchor_value", std::string());
    if (!anchorKind.empty() && !anchorValue.empty()) {
        out << "anchor: " << anchorKind << "=" << anchorValue << "\n";
    }
    out << "\nheadline:\n" << entry.value("headline", std::string("--")) << "\n";
    out << "\ndetail:\n" << entry.value("detail", std::string("--")) << "\n";
    return out.str();
}

std::string DescribeStatusPane(const tapescope::QueryResult<tapescope::StatusSnapshot>& result,
                               const std::string& configuredSocketPath) {
    std::ostringstream out;
    out << "configured_socket_path: " << configuredSocketPath << "\n";
    if (!result.ok()) {
        out << "status: unavailable\n";
        out << "error: " << tapescope::QueryClient::describeError(result.error) << "\n";
        return out.str();
    }

    const auto& status = result.value;
    out << "status: connected\n";
    out << "socket_path: " << status.socketPath << "\n";
    out << "data_dir: " << status.dataDir << "\n";
    out << "instrument_id: " << status.instrumentId << "\n";
    out << "latest_session_seq: " << status.latestSessionSeq << "\n";
    out << "live_event_count: " << status.liveEventCount << "\n";
    out << "segment_count: " << status.segmentCount << "\n";
    out << "manifest_hash: " << status.manifestHash << "\n";
    return out.str();
}

std::string DescribeLiveEventsPane(const tapescope::QueryResult<std::vector<json>>& result) {
    if (!result.ok()) {
        return tapescope::QueryClient::describeError(result.error);
    }
    if (result.value.empty()) {
        return "No live events returned.";
    }

    std::ostringstream out;
    for (std::size_t index = 0; index < result.value.size(); ++index) {
        const json& event = result.value[index];
        out << '[' << index + 1 << "] session_seq=" << event.value("session_seq", 0ULL)
            << " source_seq=" << event.value("source_seq", 0ULL)
            << " kind=" << event.value("event_kind", std::string())
            << " instrument=" << event.value("instrument_id", std::string());
        const std::string side = FirstPresentString(event, {"side"});
        if (!side.empty()) {
            out << " side=" << side;
        }
        const auto priceIt = event.find("price");
        if (priceIt != event.end() && priceIt->is_number()) {
            out << " price=" << priceIt->get<double>();
        }
        out << '\n';
        const std::string note = FirstPresentString(event, {"note", "details", "summary", "message"});
        if (!note.empty()) {
            out << "    " << note << '\n';
        }
        out << event.dump(2) << "\n\n";
    }
    return out.str();
}

std::string DescribeLiveEventsPane(const tapescope::QueryResult<std::vector<tapescope::EventRow>>& result) {
    if (!result.ok()) {
        tapescope::QueryResult<std::vector<json>> proxy;
        proxy.error = result.error;
        return DescribeLiveEventsPane(proxy);
    }
    std::ostringstream out;
    if (result.value.empty()) {
        return "No live events returned.";
    }
    for (std::size_t index = 0; index < result.value.size(); ++index) {
        const auto& event = result.value[index];
        out << '[' << index + 1 << "] session_seq=" << event.sessionSeq
            << " source_seq=" << event.sourceSeq
            << " kind=" << event.eventKind
            << " instrument=" << event.instrumentId;
        if (!event.side.empty()) {
            out << " side=" << event.side;
        }
        if (event.price.has_value()) {
            out << " price=" << *event.price;
        }
        out << '\n';
        if (!event.summary.empty()) {
            out << "    " << event.summary << '\n';
        }
        out << event.raw.dump(2) << "\n\n";
    }
    return out.str();
}

std::string DescribeRangeResult(const tapescope::RangeQuery& query,
                                const tapescope::QueryResult<std::vector<json>>& result) {
    std::ostringstream out;
    out << "requested_range: [" << query.firstSessionSeq << ", " << query.lastSessionSeq << "]\n\n";
    if (!result.ok()) {
        out << tapescope::QueryClient::describeError(result.error) << '\n';
        return out.str();
    }
    if (result.value.empty()) {
        out << "No events returned.\n";
        return out.str();
    }
    for (const auto& event : result.value) {
        out << "session_seq=" << event.value("session_seq", 0ULL)
            << " source_seq=" << event.value("source_seq", 0ULL)
            << " kind=" << event.value("event_kind", std::string()) << '\n';
        out << event.dump(2) << "\n\n";
    }
    return out.str();
}

std::string DescribeRangeResult(const tapescope::RangeQuery& query,
                                const tapescope::QueryResult<std::vector<tapescope::EventRow>>& result) {
    if (!result.ok()) {
        tapescope::QueryResult<std::vector<json>> proxy;
        proxy.error = result.error;
        return DescribeRangeResult(query, proxy);
    }
    std::ostringstream out;
    out << "requested_range: [" << query.firstSessionSeq << ", " << query.lastSessionSeq << "]\n\n";
    if (result.value.empty()) {
        out << "No events returned.\n";
        return out.str();
    }
    for (const auto& event : result.value) {
        out << "session_seq=" << event.sessionSeq
            << " source_seq=" << event.sourceSeq
            << " kind=" << event.eventKind << '\n';
        out << event.raw.dump(2) << "\n\n";
    }
    return out.str();
}

std::string EventSummaryText(const json& event) {
    std::string summary = FirstPresentString(event, {"summary", "note", "details", "message"});
    if (!summary.empty()) {
        return summary;
    }
    const std::string kind = event.value("event_kind", std::string());
    const std::string side = event.value("side", std::string());
    const auto priceIt = event.find("price");
    if (priceIt != event.end() && priceIt->is_number()) {
        std::ostringstream out;
        out << kind;
        if (!side.empty()) {
            out << " " << side;
        }
        out << " @ " << priceIt->get<double>();
        return out.str();
    }
    return kind;
}

std::string EventSummaryText(const tapescope::EventRow& event) {
    if (!event.summary.empty()) {
        return event.summary;
    }
    return event.eventKind;
}

std::string DescribeOrderLookupResult(const std::string& descriptor,
                                      const tapescope::QueryResult<json>& result) {
    std::ostringstream out;
    out << "lookup: " << descriptor << "\n\n";
    if (!result.ok()) {
        out << tapescope::QueryClient::describeError(result.error) << '\n';
        return out.str();
    }

    const json& payload = result.value;
    const json summary = payload.value("summary", json::object());
    const json events = payload.value("events", json::array());
    out << "event_count: " << (events.is_array() ? events.size() : 0) << "\n";
    if (!summary.is_null()) {
        out << "\nsummary:\n" << summary.dump(2) << "\n";
    }
    if (events.is_array()) {
        out << "\nanchored_events:\n";
        for (const auto& event : events) {
            out << "- session_seq=" << event.value("session_seq", 0ULL)
                << " kind=" << event.value("event_kind", std::string()) << '\n';
        }
        out << "\nraw_events:\n" << events.dump(2) << '\n';
    }
    return out.str();
}

std::string DescribeOrderLookupResult(const std::string& descriptor,
                                      const tapescope::QueryResult<tapescope::EventListPayload>& result) {
    if (!result.ok()) {
        tapescope::QueryResult<json> proxy;
        proxy.error = result.error;
        return DescribeOrderLookupResult(descriptor, proxy);
    }
    std::ostringstream out;
    out << "lookup: " << descriptor << "\n\n";
    out << "event_count: " << result.value.events.size() << "\n";
    out << "\nsummary:\n" << result.value.summary.dump(2) << "\n";
    out << "\nanchored_events:\n";
    for (const auto& event : result.value.events) {
        out << "- session_seq=" << event.sessionSeq
            << " kind=" << event.eventKind << '\n';
    }
    out << "\nraw_events:\n" << result.value.raw.value("events", json::array()).dump(2) << '\n';
    return out.str();
}

std::string DescribeInvestigationPayload(const std::string& heading,
                                         const std::string& descriptor,
                                         const tapescope::QueryResult<json>& result) {
    std::ostringstream out;
    out << heading << '\n';
    if (!descriptor.empty()) {
        out << "query: " << descriptor << "\n\n";
    } else {
        out << '\n';
    }
    if (!result.ok()) {
        out << tapescope::QueryClient::describeError(result.error) << '\n';
        return out.str();
    }

    const json& payload = result.value;
    const json summary = payload.value("summary", json::object());
    const json events = payload.value("events", json::array());

    if (summary.is_object()) {
        const json artifact = summary.value("artifact", json::object());
        const json report = summary.value("report", json::object());
        const json evidence = summary.value("evidence", json::object());
        if (!artifact.is_null() && !artifact.empty()) {
            out << "artifact_id: " << artifact.value("id", std::string("--")) << '\n';
            out << "artifact_kind: " << artifact.value("kind", std::string("--")) << '\n';
        }
        if (!report.is_null() && !report.empty()) {
            const auto highlightsIt = report.find("highlights");
            const std::size_t highlightCount = (highlightsIt != report.end() && highlightsIt->is_array())
                                                   ? highlightsIt->size()
                                                   : 0U;
            out << "report_title: " << report.value("title", std::string("--")) << '\n';
            out << "report_highlights: " << highlightCount << '\n';
        }
        if (!evidence.is_null() && !evidence.empty()) {
            out << "evidence_sections: " << evidence.size() << '\n';
        }
    }

    out << "event_count: " << (events.is_array() ? events.size() : 0) << "\n\n";
    out << "summary:\n" << summary.dump(2) << "\n";
    if (events.is_array()) {
        out << "\nevents:\n";
        for (const auto& event : events) {
            out << "- session_seq=" << event.value("session_seq", 0ULL)
                << " kind=" << event.value("event_kind", std::string()) << '\n';
        }
        out << "\nraw_events:\n" << events.dump(2) << '\n';
    }
    return out.str();
}

std::string DescribeInvestigationPayload(const std::string& heading,
                                         const std::string& descriptor,
                                         const tapescope::QueryResult<tapescope::InvestigationPayload>& result) {
    if (!result.ok()) {
        tapescope::QueryResult<json> proxy;
        proxy.error = result.error;
        return DescribeInvestigationPayload(heading, descriptor, proxy);
    }
    tapescope::QueryResult<json> proxy;
    proxy.value = result.value.raw;
    return DescribeInvestigationPayload(heading, descriptor, proxy);
}

std::string DescribeEnrichmentPayload(const std::string& heading,
                                      const std::string& descriptor,
                                      const tapescope::QueryResult<tapescope::EnrichmentPayload>& result) {
    std::ostringstream out;
    out << heading << '\n';
    if (!descriptor.empty()) {
        out << "query: " << descriptor << "\n\n";
    } else {
        out << '\n';
    }
    if (!result.ok()) {
        out << tapescope::QueryClient::describeError(result.error) << '\n';
        return out.str();
    }

    const auto& payload = result.value;
    out << "request_kind: " << payload.requestKind << "\n";
    out << "artifact_id: " << payload.artifactId << "\n";
    if (!payload.headline.empty()) {
        out << "headline: " << payload.headline << "\n";
    }
    if (!payload.detail.empty()) {
        out << "detail: " << payload.detail << "\n";
    }
    out << '\n';

    out << "local_evidence:\n";
    out << "  artifact_kind: " << payload.localEvidence.artifactKind << "\n";
    out << "  citations: " << payload.localEvidence.evidence.size() << "\n";
    out << "  incidents: " << payload.localEvidence.incidents.size() << "\n";
    if (payload.localEvidence.replayRange.has_value()) {
        out << "  replay_range: [" << payload.localEvidence.replayRange->firstSessionSeq
            << ", " << payload.localEvidence.replayRange->lastSessionSeq << "]\n";
    }
    out << "  headline: " << payload.localEvidence.headline << "\n";
    out << "  detail: " << payload.localEvidence.detail << "\n\n";

    const json externalContext = payload.externalContext.is_object() ? payload.externalContext : json::object();
    const json externalItems = externalContext.value("items", json::array());
    const json externalSummaries = externalContext.value("summaries", json::array());
    out << "external_context:\n";
    out << "  provider: " << externalContext.value("provider", std::string("--")) << "\n";
    out << "  fetched_at: " << externalContext.value("fetched_at", std::string("--")) << "\n";
    out << "  cache_status: " << externalContext.value("cache_status", std::string("--")) << "\n";
    out << "  items: " << (externalItems.is_array() ? externalItems.size() : 0) << "\n";
    out << "  summaries: " << (externalSummaries.is_array() ? externalSummaries.size() : 0) << "\n";
    if (externalContext.contains("warnings")) {
        out << "  warnings: " << externalContext.value("warnings", json::array()).dump() << "\n";
    }
    out << '\n';

    const json liveCapture = payload.liveCaptureSummary.is_object()
        ? payload.liveCaptureSummary
        : payload.providerMetadata.value("live_capture_summary", json::object());
    if (liveCapture.is_object() && !liveCapture.empty()) {
        out << "live_capture:\n";
        out << "  requested: " << (liveCapture.value("requested", false) ? "yes" : "no") << "\n";
        out << "  status: " << liveCapture.value("status", std::string("--")) << "\n";
        out << "  outcome: " << liveCapture.value("outcome", std::string("--")) << "\n";
        out << "  source: " << liveCapture.value("source", std::string("--")) << "\n";
        out << "  channels: " << liveCapture.value("channel_count", 0ULL) << "\n";
        out << "  data_frames: " << liveCapture.value("data_frame_count", 0ULL) << "\n";
        out << "  join_acks: " << liveCapture.value("join_ack_frame_count", 0ULL) << "\n";
        out << "  errors: " << liveCapture.value("error_frame_count", 0ULL) << "\n";
        if (liveCapture.contains("summary_text")) {
            out << "  summary: " << liveCapture.value("summary_text", std::string("--")) << "\n";
        }
        const json channelOutcomes = liveCapture.value("channel_outcomes", json::array());
        if (channelOutcomes.is_array() && !channelOutcomes.empty()) {
            out << "  channel_outcomes:\n";
            for (const auto& channel : channelOutcomes) {
                if (!channel.is_object()) {
                    continue;
                }
                out << "    - " << channel.value("channel", std::string("--"))
                    << " => " << channel.value("outcome", std::string("--"));
                if (channel.value("data_frame_count", 0ULL) > 0ULL) {
                    out << " data=" << channel.value("data_frame_count", 0ULL);
                }
                if (channel.value("join_ack_frame_count", 0ULL) > 0ULL) {
                    out << " join_ack=" << channel.value("join_ack_frame_count", 0ULL);
                }
                if (channel.value("filtered_mismatch_frame_count", 0ULL) > 0ULL) {
                    out << " filtered=" << channel.value("filtered_mismatch_frame_count", 0ULL);
                }
                if (channel.value("ambient_global_frame_count", 0ULL) > 0ULL) {
                    out << " ambient=" << channel.value("ambient_global_frame_count", 0ULL);
                }
                out << "\n";
            }
        }
        out << '\n';
    }

    out << "interpretation:\n";
    out << "  status: " << payload.interpretation.value("status", std::string("--")) << "\n";
    out << "  lane: " << payload.interpretation.value("lane", std::string("--")) << "\n";
    out << "  task: " << payload.interpretation.value("task", std::string("--")) << "\n";
    out << "  model: " << payload.interpretation.value("model", std::string("--")) << "\n";
    out << "  finish_reason: " << payload.interpretation.value("finish_reason", std::string("--")) << "\n";
    if (payload.interpretation.contains("content") && !payload.interpretation["content"].is_null()) {
        out << "  content:\n" << payload.interpretation["content"].dump(2) << "\n";
    }
    if (payload.interpretation.contains("error")) {
        out << "  error: " << payload.interpretation.value("error", std::string()) << "\n";
    }
    out << '\n';

    out << "provider_metadata:\n" << payload.providerMetadata.dump(2) << "\n\n";
    out << "degradation:\n" << payload.degradation.dump(2) << "\n\n";
    out << "cache:\n" << payload.cache.dump(2) << '\n';
    return out.str();
}

std::string DescribeSeekOrderResult(const std::string& descriptor,
                                    const tapescope::QueryResult<json>& result) {
    std::ostringstream out;
    out << "seek_order_anchor\n";
    out << "query: " << descriptor << "\n\n";
    if (!result.ok()) {
        out << tapescope::QueryClient::describeError(result.error) << '\n';
        return out.str();
    }

    const json summary = result.value.value("summary", json::object());
    out << "replay_target_session_seq: " << summary.value("replay_target_session_seq", 0ULL) << '\n';
    out << "first_session_seq: " << summary.value("first_session_seq", 0ULL) << '\n';
    out << "last_session_seq: " << summary.value("last_session_seq", 0ULL) << '\n';
    out << "last_fill_session_seq: " << summary.value("last_fill_session_seq", 0ULL) << '\n';
    if (summary.contains("protected_window")) {
        out << "protected_window:\n" << summary.value("protected_window", json::object()).dump(2) << '\n';
    }
    out << "\nsummary:\n" << summary.dump(2) << '\n';
    return out.str();
}

std::string DescribeSeekOrderResult(const std::string& descriptor,
                                    const tapescope::QueryResult<tapescope::SeekOrderPayload>& result) {
    if (!result.ok()) {
        tapescope::QueryResult<json> proxy;
        proxy.error = result.error;
        return DescribeSeekOrderResult(descriptor, proxy);
    }
    tapescope::QueryResult<json> proxy;
    proxy.value = result.value.raw;
    return DescribeSeekOrderResult(descriptor, proxy);
}

std::string DescribeReportInventoryResult(const tapescope::QueryResult<json>& sessionReports,
                                          const tapescope::QueryResult<json>& caseReports) {
    std::ostringstream out;
    out << "report_inventory\n\n";

    out << "session_reports:\n";
    if (!sessionReports.ok()) {
        out << tapescope::QueryClient::describeError(sessionReports.error) << "\n\n";
    } else {
        const json rows = sessionReports.value.value("events", json::array());
        if (!rows.is_array() || rows.empty()) {
            out << "(none)\n\n";
        } else {
            for (const auto& row : rows) {
                out << "- report_id=" << row.value("report_id", 0ULL)
                    << " artifact_id=" << row.value("artifact_id", std::string())
                    << " revision_id=" << row.value("revision_id", 0ULL)
                    << " headline=" << row.value("headline", std::string()) << '\n';
            }
            out << '\n';
        }
    }

    out << "case_reports:\n";
    if (!caseReports.ok()) {
        out << tapescope::QueryClient::describeError(caseReports.error) << '\n';
    } else {
        const json rows = caseReports.value.value("events", json::array());
        if (!rows.is_array() || rows.empty()) {
            out << "(none)\n";
        } else {
            for (const auto& row : rows) {
                out << "- report_id=" << row.value("report_id", 0ULL)
                    << " artifact_id=" << row.value("artifact_id", std::string())
                    << " report_type=" << row.value("report_type", std::string())
                    << " headline=" << row.value("headline", std::string()) << '\n';
            }
        }
    }

    return out.str();
}

std::string DescribeReportInventoryResult(const tapescope::QueryResult<tapescope::ReportInventoryPayload>& sessionReports,
                                          const tapescope::QueryResult<tapescope::ReportInventoryPayload>& caseReports) {
    if (!sessionReports.ok() || !caseReports.ok()) {
        tapescope::QueryResult<json> sessionProxy;
        sessionProxy.error = sessionReports.error;
        if (sessionReports.ok()) {
            sessionProxy.value = sessionReports.value.raw;
        }
        tapescope::QueryResult<json> caseProxy;
        caseProxy.error = caseReports.error;
        if (caseReports.ok()) {
            caseProxy.value = caseReports.value.raw;
        }
        return DescribeReportInventoryResult(sessionProxy, caseProxy);
    }

    std::ostringstream out;
    out << "report_inventory\n\n";
    out << "session_reports:\n";
    if (sessionReports.value.sessionReports.empty()) {
        out << "(none)\n\n";
    } else {
        for (const auto& row : sessionReports.value.sessionReports) {
            out << "- report_id=" << row.reportId
                << " artifact_id=" << row.artifactId
                << " revision_id=" << row.revisionId
                << " headline=" << row.headline << '\n';
        }
        out << '\n';
    }
    out << "case_reports:\n";
    if (caseReports.value.caseReports.empty()) {
        out << "(none)\n";
    } else {
        for (const auto& row : caseReports.value.caseReports) {
            out << "- report_id=" << row.reportId
                << " artifact_id=" << row.artifactId
                << " report_type=" << row.reportType
                << " headline=" << row.headline << '\n';
        }
    }
    return out.str();
}

std::string DescribeArtifactExportResult(const std::string& artifactId,
                                         const std::string& exportFormat,
                                         const tapescope::QueryResult<json>& result) {
    std::ostringstream out;
    out << "artifact_export\n";
    out << "artifact_id: " << artifactId << '\n';
    out << "format: " << exportFormat << "\n\n";
    if (!result.ok()) {
        out << tapescope::QueryClient::describeError(result.error) << '\n';
        return out.str();
    }

    const json summary = result.value.value("summary", json::object());
    out << "summary:\n" << summary.dump(2) << "\n\n";
    if (exportFormat == "markdown") {
        out << "markdown:\n" << summary.value("markdown", std::string()) << '\n';
    } else {
        out << "bundle:\n" << summary.value("bundle", json::object()).dump(2) << '\n';
    }
    return out.str();
}

std::string DescribeArtifactExportResult(const std::string& artifactId,
                                         const std::string& exportFormat,
                                         const tapescope::QueryResult<tapescope::ArtifactExportPayload>& result) {
    if (!result.ok()) {
        tapescope::QueryResult<json> proxy;
        proxy.error = result.error;
        return DescribeArtifactExportResult(artifactId, exportFormat, proxy);
    }
    tapescope::QueryResult<json> proxy;
    proxy.value = result.value.raw;
    return DescribeArtifactExportResult(artifactId, exportFormat, proxy);
}

std::string DescribeSessionQualityResult(const tapescope::RangeQuery& query,
                                         bool includeLiveTail,
                                         const tapescope::QueryResult<json>& result) {
    std::ostringstream out;
    out << "session_quality\n";
    out << "session_seq=[" << query.firstSessionSeq << ", " << query.lastSessionSeq << "]\n";
    out << "include_live_tail: " << (includeLiveTail ? "true" : "false") << "\n\n";
    if (!result.ok()) {
        out << tapescope::QueryClient::describeError(result.error) << '\n';
        return out.str();
    }

    const json summary = result.value.value("summary", json::object());
    out << "summary:\n" << summary.dump(2) << '\n';
    return out.str();
}

std::string DescribeSessionQualityResult(const tapescope::RangeQuery& query,
                                         bool includeLiveTail,
                                         const tapescope::QueryResult<tapescope::SessionQualityPayload>& result) {
    if (!result.ok()) {
        tapescope::QueryResult<json> proxy;
        proxy.error = result.error;
        return DescribeSessionQualityResult(query, includeLiveTail, proxy);
    }
    tapescope::QueryResult<json> proxy;
    proxy.value = result.value.raw;
    return DescribeSessionQualityResult(query, includeLiveTail, proxy);
}

std::string DescribeIncidentListResult(const tapescope::QueryResult<tapescope::IncidentListPayload>& result) {
    std::ostringstream out;
    out << "incident_list\n\n";
    if (!result.ok()) {
        out << tapescope::QueryClient::describeError(result.error) << '\n';
        return out.str();
    }
    if (result.value.incidents.empty()) {
        out << "(none)\n";
        return out.str();
    }
    for (const auto& row : result.value.incidents) {
        out << "- logical_incident_id=" << row.logicalIncidentId
            << " kind=" << row.kind
            << " score=" << row.score
            << " title=" << row.title << '\n';
    }
    return out.str();
}

bool ReplayRangeFromSeekSummary(const json& summary, tapescope::RangeQuery* query) {
    if (query == nullptr || !summary.is_object()) {
        return false;
    }
    const std::uint64_t replayFrom = summary.value("replay_from_session_seq", 0ULL);
    const std::uint64_t replayTo = summary.value("replay_to_session_seq", 0ULL);
    if (replayFrom > 0 && replayTo >= replayFrom) {
        query->firstSessionSeq = replayFrom;
        query->lastSessionSeq = replayTo;
        return true;
    }

    const json window = summary.value("protected_window", json::object());
    if (window.is_object()) {
        const std::uint64_t first = window.value("first_session_seq", 0ULL);
        const std::uint64_t last = window.value("last_session_seq", 0ULL);
        if (first > 0 && last >= first) {
            query->firstSessionSeq = first;
            query->lastSessionSeq = last;
            return true;
        }
    }

    const std::uint64_t target = summary.value("replay_target_session_seq", 0ULL);
    if (target == 0) {
        return false;
    }
    query->firstSessionSeq = target > 16 ? target - 16 : 1;
    query->lastSessionSeq = target + 16;
    return true;
}

bool ReplayRangeFromInvestigationSummary(const json& summary, tapescope::RangeQuery* query) {
    if (query == nullptr || !summary.is_object()) {
        return false;
    }
    const std::uint64_t from = summary.value("from_session_seq",
                                             summary.value("first_session_seq", 0ULL));
    const std::uint64_t to = summary.value("to_session_seq",
                                           summary.value("last_session_seq", 0ULL));
    if (from > 0 && to >= from) {
        query->firstSessionSeq = from;
        query->lastSessionSeq = to;
        return true;
    }
    return ReplayRangeFromSeekSummary(summary, query);
}

} // namespace tapescope_support
