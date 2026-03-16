#include "tape_query_payloads.h"

namespace tape_payloads {

std::string firstPresentString(const json& payload,
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

bool parseReplayRange(const json& summary, RangeQuery* range) {
    if (range == nullptr || !summary.is_object()) {
        return false;
    }
    const std::uint64_t from = summary.value("from_session_seq",
                                             summary.value("first_session_seq", 0ULL));
    const std::uint64_t to = summary.value("to_session_seq",
                                           summary.value("last_session_seq", 0ULL));
    if (from > 0 && to >= from) {
        range->firstSessionSeq = from;
        range->lastSessionSeq = to;
        return true;
    }

    const std::uint64_t replayFrom = summary.value("replay_from_session_seq", 0ULL);
    const std::uint64_t replayTo = summary.value("replay_to_session_seq", 0ULL);
    if (replayFrom > 0 && replayTo >= replayFrom) {
        range->firstSessionSeq = replayFrom;
        range->lastSessionSeq = replayTo;
        return true;
    }
    return false;
}

bool parseSeekReplayRange(const json& summary, RangeQuery* range) {
    if (parseReplayRange(summary, range)) {
        return true;
    }
    if (!summary.is_object() || range == nullptr) {
        return false;
    }

    const json window = summary.value("protected_window", json::object());
    if (window.is_object()) {
        const std::uint64_t first = window.value("first_session_seq", 0ULL);
        const std::uint64_t last = window.value("last_session_seq", 0ULL);
        if (first > 0 && last >= first) {
            range->firstSessionSeq = first;
            range->lastSessionSeq = last;
            return true;
        }
    }

    const std::uint64_t target = summary.value("replay_target_session_seq", 0ULL);
    if (target == 0) {
        return false;
    }
    range->firstSessionSeq = target > 16 ? target - 16 : 1;
    range->lastSessionSeq = target + 16;
    return true;
}

std::vector<EvidenceCitation> parseEvidenceCitations(const json& summary) {
    std::vector<EvidenceCitation> evidence;
    if (!summary.is_object()) {
        return evidence;
    }
    const json evidencePayload = summary.value("evidence", json::object());
    const json citations = evidencePayload.value("citations", json::array());
    if (!citations.is_array()) {
        return evidence;
    }
    evidence.reserve(citations.size());
    for (const auto& citation : citations) {
        if (!citation.is_object()) {
            continue;
        }
        EvidenceCitation item;
        item.kind = citation.value("kind", citation.value("type", std::string()));
        item.artifactId = citation.value("artifact_id", std::string());
        item.label = firstPresentString(citation, {"label", "headline", "artifact_id"});
        if (item.label.empty()) {
            item.label = item.kind;
        }
        item.raw = citation;
        evidence.push_back(std::move(item));
    }
    return evidence;
}

EventRow parseEventRow(const json& event) {
    EventRow row;
    row.raw = event;
    if (!event.is_object()) {
        return row;
    }
    row.sessionSeq = event.value("session_seq", 0ULL);
    row.sourceSeq = event.value("source_seq", 0ULL);
    row.eventKind = event.value("event_kind", std::string());
    row.instrumentId = event.value("instrument_id", std::string());
    row.side = event.value("side", std::string());
    const auto priceIt = event.find("price");
    if (priceIt != event.end() && priceIt->is_number()) {
        row.price = priceIt->get<double>();
    }
    row.summary = firstPresentString(event, {"summary", "note", "details", "message"});
    if (row.summary.empty()) {
        row.summary = row.eventKind;
        if (!row.side.empty()) {
            row.summary += " " + row.side;
        }
    }
    return row;
}

std::vector<EventRow> parseEventRows(const json& events) {
    std::vector<EventRow> rows;
    if (!events.is_array()) {
        return rows;
    }
    rows.reserve(events.size());
    for (const auto& event : events) {
        rows.push_back(parseEventRow(event));
    }
    return rows;
}

std::vector<IncidentListRow> parseIncidentRows(const json& events) {
    std::vector<IncidentListRow> rows;
    if (!events.is_array()) {
        return rows;
    }
    rows.reserve(events.size());
    for (const auto& event : events) {
        if (!event.is_object()) {
            continue;
        }
        IncidentListRow row;
        row.logicalIncidentId = event.value("logical_incident_id", 0ULL);
        row.kind = event.value("kind", std::string());
        row.score = event.value("score", 0.0);
        row.title = firstPresentString(event, {"title", "why_it_matters", "kind"});
        row.raw = event;
        rows.push_back(std::move(row));
    }
    return rows;
}

std::vector<ReportInventoryRow> parseReportRows(const json& events) {
    std::vector<ReportInventoryRow> rows;
    if (!events.is_array()) {
        return rows;
    }
    rows.reserve(events.size());
    for (const auto& event : events) {
        if (!event.is_object()) {
            continue;
        }
        ReportInventoryRow row;
        row.reportId = event.value("report_id", 0ULL);
        row.revisionId = event.value("revision_id", 0ULL);
        row.artifactId = event.value("artifact_id", std::string());
        row.reportType = event.value("report_type", std::string());
        row.headline = event.value("headline", std::string());
        row.raw = event;
        rows.push_back(std::move(row));
    }
    return rows;
}

QueryResult<InvestigationPayload> packInvestigationPayload(const QueryResult<tape_engine::QueryResponse>& response) {
    if (!response.ok()) {
        return propagateError<InvestigationPayload>(response.error);
    }
    if (!response.value.summary.is_object()) {
        return makeError<InvestigationPayload>(QueryErrorKind::MalformedResponse,
                                               "investigation summary must be an object");
    }
    if (!response.value.events.is_array()) {
        return makeError<InvestigationPayload>(QueryErrorKind::MalformedResponse,
                                               "investigation events must be an array");
    }

    InvestigationPayload payload;
    payload.summary = response.value.summary;
    payload.raw = json::object();
    payload.raw["summary"] = response.value.summary;
    payload.raw["events"] = response.value.events;
    payload.events.reserve(response.value.events.size());
    for (const auto& event : response.value.events) {
        payload.events.push_back(event);
    }
    payload.incidents = parseIncidentRows(response.value.events);
    payload.evidence = parseEvidenceCitations(payload.summary);

    const json artifact = payload.summary.value("artifact", json::object());
    payload.artifactId = artifact.value("artifact_id", artifact.value("id", std::string()));
    payload.artifactKind = artifact.value("kind", artifact.value("artifact_type", std::string()));

    const json report = payload.summary.value("report", json::object());
    payload.headline = firstPresentString(report, {"title", "headline"});
    if (payload.headline.empty()) {
        payload.headline = firstPresentString(payload.summary, {"headline", "title", "why_it_matters"});
    }
    payload.detail = firstPresentString(report, {"summary", "why_it_matters"});
    if (payload.detail.empty()) {
        payload.detail = firstPresentString(payload.summary, {"what_changed_first", "why_it_matters", "headline"});
    }

    RangeQuery replayRange;
    if (parseSeekReplayRange(payload.summary, &replayRange)) {
        payload.replayRange = replayRange;
    }
    return makeSuccess(std::move(payload));
}

QueryResult<EventListPayload> packEventListPayload(const QueryResult<tape_engine::QueryResponse>& response) {
    if (!response.ok()) {
        return propagateError<EventListPayload>(response.error);
    }
    if (!response.value.summary.is_object()) {
        return makeError<EventListPayload>(QueryErrorKind::MalformedResponse,
                                           "event-list summary must be an object");
    }
    if (!response.value.events.is_array()) {
        return makeError<EventListPayload>(QueryErrorKind::MalformedResponse,
                                           "event-list events must be an array");
    }

    EventListPayload payload;
    payload.summary = response.value.summary;
    payload.raw = json::object();
    payload.raw["summary"] = response.value.summary;
    payload.raw["events"] = response.value.events;
    payload.events = parseEventRows(response.value.events);
    return makeSuccess(std::move(payload));
}

QueryResult<SessionQualityPayload> packSessionQualityPayload(const QueryResult<tape_engine::QueryResponse>& response) {
    if (!response.ok()) {
        return propagateError<SessionQualityPayload>(response.error);
    }
    if (!response.value.summary.is_object()) {
        return makeError<SessionQualityPayload>(QueryErrorKind::MalformedResponse,
                                                "session quality summary must be an object");
    }
    SessionQualityPayload payload;
    payload.summary = response.value.summary;
    payload.dataQuality = payload.summary.value("data_quality", json::object());
    payload.raw = json::object();
    payload.raw["summary"] = response.value.summary;
    payload.raw["events"] = response.value.events;
    return makeSuccess(std::move(payload));
}

QueryResult<SeekOrderPayload> packSeekOrderPayload(const QueryResult<tape_engine::QueryResponse>& response) {
    if (!response.ok()) {
        return propagateError<SeekOrderPayload>(response.error);
    }
    if (!response.value.summary.is_object()) {
        return makeError<SeekOrderPayload>(QueryErrorKind::MalformedResponse,
                                           "seek summary must be an object");
    }

    SeekOrderPayload payload;
    payload.summary = response.value.summary;
    payload.raw = json::object();
    payload.raw["summary"] = response.value.summary;
    payload.raw["events"] = response.value.events;
    payload.replayTargetSessionSeq = payload.summary.value("replay_target_session_seq", 0ULL);
    payload.firstSessionSeq = payload.summary.value("first_session_seq", 0ULL);
    payload.lastSessionSeq = payload.summary.value("last_session_seq", 0ULL);
    payload.lastFillSessionSeq = payload.summary.value("last_fill_session_seq", 0ULL);
    RangeQuery replayRange;
    if (parseReplayRange(payload.summary, &replayRange)) {
        payload.replayRange = replayRange;
    }
    return makeSuccess(std::move(payload));
}

QueryResult<IncidentListPayload> packIncidentListPayload(const QueryResult<tape_engine::QueryResponse>& response) {
    if (!response.ok()) {
        return propagateError<IncidentListPayload>(response.error);
    }
    if (!response.value.events.is_array()) {
        return makeError<IncidentListPayload>(QueryErrorKind::MalformedResponse,
                                              "incident list events must be an array");
    }

    IncidentListPayload payload;
    payload.raw = json::object();
    payload.raw["summary"] = response.value.summary;
    payload.raw["events"] = response.value.events;
    payload.incidents = parseIncidentRows(response.value.events);
    return makeSuccess(std::move(payload));
}

QueryResult<ReportInventoryPayload> packReportInventoryPayload(const QueryResult<tape_engine::QueryResponse>& response,
                                                               bool sessionReports) {
    if (!response.ok()) {
        return propagateError<ReportInventoryPayload>(response.error);
    }
    if (!response.value.events.is_array()) {
        return makeError<ReportInventoryPayload>(QueryErrorKind::MalformedResponse,
                                                 "report inventory events must be an array");
    }

    ReportInventoryPayload payload;
    payload.raw = json::object();
    payload.raw["summary"] = response.value.summary;
    payload.raw["events"] = response.value.events;
    if (sessionReports) {
        payload.sessionReports = parseReportRows(response.value.events);
    } else {
        payload.caseReports = parseReportRows(response.value.events);
    }
    return makeSuccess(std::move(payload));
}

QueryResult<ArtifactExportPayload> packArtifactExportPayload(const QueryResult<tape_engine::QueryResponse>& response) {
    if (!response.ok()) {
        return propagateError<ArtifactExportPayload>(response.error);
    }
    if (!response.value.summary.is_object()) {
        return makeError<ArtifactExportPayload>(QueryErrorKind::MalformedResponse,
                                                "artifact export summary must be an object");
    }

    ArtifactExportPayload payload;
    payload.summary = response.value.summary;
    payload.raw = json::object();
    payload.raw["summary"] = response.value.summary;
    payload.raw["events"] = response.value.events;
    payload.artifactId = payload.summary.value("artifact_id", std::string());
    payload.format = payload.summary.value("export_format", std::string());
    payload.servedRevisionId = payload.summary.value("served_revision_id", 0ULL);
    payload.artifactExport = payload.summary.value("artifact_export", json::object());
    payload.markdown = payload.summary.value("markdown", std::string());
    payload.bundle = payload.summary.value("bundle", json::object());

    const std::string exportFormat = payload.artifactExport.value("format", payload.format);
    if (payload.format.empty()) {
        payload.format = exportFormat;
    }
    if (payload.artifactId.empty()) {
        payload.artifactId = payload.artifactExport.value("artifact_id", std::string());
    }
    if (payload.format.empty()) {
        return makeError<ArtifactExportPayload>(QueryErrorKind::MalformedResponse,
                                                "artifact export is missing export format");
    }
    if (payload.artifactId.empty()) {
        return makeError<ArtifactExportPayload>(QueryErrorKind::MalformedResponse,
                                                "artifact export is missing artifact id");
    }
    if (payload.format == "markdown" && payload.markdown.empty()) {
        return makeError<ArtifactExportPayload>(QueryErrorKind::MalformedResponse,
                                                "markdown artifact export is missing markdown content");
    }
    if (payload.format == "json-bundle" && !payload.bundle.is_object()) {
        return makeError<ArtifactExportPayload>(QueryErrorKind::MalformedResponse,
                                                "json-bundle artifact export is missing bundle content");
    }
    return makeSuccess(std::move(payload));
}

} // namespace tape_payloads
