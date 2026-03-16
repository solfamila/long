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

ImportedCaseRow parseImportedCaseRow(const json& item) {
    ImportedCaseRow row;
    row.raw = item;
    if (!item.is_object()) {
        return row;
    }
    row.importedCaseId = item.value("imported_case_id", 0ULL);
    row.artifactId = item.value("artifact_id", std::string());
    row.bundleId = item.value("bundle_id", std::string());
    row.bundleType = item.value("bundle_type", std::string());
    row.sourceArtifactId = item.value("source_artifact_id", std::string());
    row.sourceReportId = item.value("source_report_id", 0ULL);
    row.sourceRevisionId = item.value("source_revision_id", 0ULL);
    row.firstSessionSeq = item.value("first_session_seq", 0ULL);
    row.lastSessionSeq = item.value("last_session_seq", 0ULL);
    row.instrumentId = item.value("instrument_id", std::string());
    row.headline = item.value("headline", std::string());
    row.fileName = item.value("file_name", std::string());
    row.sourceBundlePath = item.value("source_bundle_path", std::string());
    row.payloadSha256 = item.value("payload_sha256", std::string());
    return row;
}

std::vector<ImportedCaseRow> parseImportedCaseRows(const json& events) {
    std::vector<ImportedCaseRow> rows;
    if (!events.is_array()) {
        return rows;
    }
    rows.reserve(events.size());
    for (const auto& event : events) {
        rows.push_back(parseImportedCaseRow(event));
    }
    return rows;
}

const json* typedResultOrNull(const tape_engine::QueryResponse& response,
                              const char* expectedSchema) {
    if (!response.result.is_object()) {
        return nullptr;
    }
    if (response.result.value("schema", std::string()) != expectedSchema) {
        return nullptr;
    }
    return &response.result;
}

const json* typedCollectionResultOrNull(const tape_engine::QueryResponse& response,
                                        const char* expectedKind) {
    const json* result = typedResultOrNull(response, tape_engine::kCollectionResultSchema);
    if (result == nullptr) {
        return nullptr;
    }
    if (result->value("collection_kind", std::string()) != expectedKind) {
        return nullptr;
    }
    return result;
}

json legacyStatusResult(const tape_engine::QueryResponse& response) {
    json result = response.summary.is_object() ? response.summary : json::object();
    result["schema"] = tape_engine::kStatusResultSchema;
    result["version"] = tape_engine::kStatusResultVersion;
    return result;
}

json legacyEventListResult(const tape_engine::QueryResponse& response) {
    return {
        {"schema", tape_engine::kEventListResultSchema},
        {"version", tape_engine::kEventListResultVersion},
        {"served_revision_id", response.summary.value("served_revision_id", 0ULL)},
        {"includes_mutable_tail", response.summary.value("includes_mutable_tail", false)},
        {"returned_count", response.events.is_array() ? response.events.size() : 0},
        {"base_revision_id", response.summary.value("base_revision_id", 0ULL)},
        {"live_tail_high_water_seq", response.summary.value("live_tail_high_water_seq", 0ULL)},
        {"from_session_seq", response.summary.value("from_session_seq", 0ULL)},
        {"to_session_seq", response.summary.value("to_session_seq", 0ULL)},
        {"trace_id", response.summary.value("trace_id", 0ULL)},
        {"order_id", response.summary.value("order_id", 0LL)},
        {"perm_id", response.summary.value("perm_id", 0LL)},
        {"exec_id", response.summary.value("exec_id", std::string())},
        {"events", response.events}
    };
}

json legacySessionQualityResult(const tape_engine::QueryResponse& response) {
    return {
        {"schema", tape_engine::kSessionQualityResultSchema},
        {"version", tape_engine::kSessionQualityResultVersion},
        {"served_revision_id", response.summary.value("served_revision_id", 0ULL)},
        {"includes_mutable_tail", response.summary.value("includes_mutable_tail", false)},
        {"first_session_seq", response.summary.value("from_session_seq", 0ULL)},
        {"last_session_seq", response.summary.value("to_session_seq", 0ULL)},
        {"data_quality", response.summary.value("data_quality", json::object())}
    };
}

json legacyInvestigationIncidentRows(const tape_engine::QueryResponse& response) {
    const json& summary = response.summary;
    if (summary.contains("related_incidents") && summary["related_incidents"].is_array()) {
        return summary["related_incidents"];
    }
    if (summary.contains("top_incidents") && summary["top_incidents"].is_array()) {
        return summary["top_incidents"];
    }
    if (summary.contains("incident_revisions") && summary["incident_revisions"].is_array()) {
        return summary["incident_revisions"];
    }
    if (summary.contains("latest_incident") && summary["latest_incident"].is_object() && !summary["latest_incident"].empty()) {
        return json::array({summary["latest_incident"]});
    }
    if (summary.contains("incident") && summary["incident"].is_object() && !summary["incident"].empty()) {
        return json::array({summary["incident"]});
    }
    if (response.events.is_array()) {
        bool allIncidents = !response.events.empty();
        for (const auto& item : response.events) {
            if (!item.is_object() || !item.contains("logical_incident_id")) {
                allIncidents = false;
                break;
            }
        }
        if (allIncidents) {
            return response.events;
        }
    }
    return json::array();
}

json legacyInvestigationResult(const tape_engine::QueryResponse& response) {
    const json artifact = response.summary.value("artifact", json::object());
    const json report = response.summary.value("report",
        response.summary.value("report_summary", json::object()));
    const json evidence = response.summary.value("evidence", json::object());
    RangeQuery replayRange;
    const bool hasReplayRange = parseSeekReplayRange(response.summary, &replayRange) ||
                                parseReplayRange(response.summary, &replayRange);
    json replayRangeJson = nullptr;
    if (hasReplayRange) {
        replayRangeJson = {
            {"first_session_seq", replayRange.firstSessionSeq},
            {"last_session_seq", replayRange.lastSessionSeq}
        };
    }
    std::string detail = firstPresentString(report, {"summary", "why_it_matters"});
    if (detail.empty()) {
        detail = firstPresentString(response.summary, {"what_changed_first", "why_it_matters", "headline"});
    }
    return {
        {"schema", tape_engine::kInvestigationResultSchema},
        {"version", tape_engine::kInvestigationResultVersion},
        {"artifact_id", artifact.value("artifact_id", std::string())},
        {"artifact_kind", artifact.value("artifact_type", artifact.value("artifact_kind", std::string()))},
        {"headline", firstPresentString(report, {"headline", "title", "summary"})},
        {"detail", detail},
        {"served_revision_id", response.summary.value("served_revision_id", 0ULL)},
        {"includes_mutable_tail", response.summary.value("includes_mutable_tail", false)},
        {"artifact", artifact},
        {"entity", response.summary.value("entity", json::object())},
        {"report", report},
        {"evidence", evidence},
        {"data_quality", response.summary.value("data_quality", json::object())},
        {"replay_range", replayRangeJson},
        {"incident_rows", legacyInvestigationIncidentRows(response)},
        {"citation_rows", evidence.value("citations", json::array())},
        {"events", response.events}
    };
}

json legacyCollectionResult(const tape_engine::QueryResponse& response,
                            const char* collectionKind) {
    return {
        {"schema", tape_engine::kCollectionResultSchema},
        {"version", tape_engine::kCollectionResultVersion},
        {"collection_kind", collectionKind},
        {"served_revision_id", response.summary.value("served_revision_id", 0ULL)},
        {"includes_mutable_tail", response.summary.value("includes_mutable_tail", false)},
        {"returned_count", response.events.is_array() ? response.events.size() : 0},
        {"total_count", response.summary.value("returned_events",
                         response.summary.value("session_report_count",
                         response.summary.value("case_report_count", 0ULL)))},
        {"rows", response.events}
    };
}

json legacySeekOrderResult(const tape_engine::QueryResponse& response) {
    RangeQuery replayRange;
    json replayRangeJson = nullptr;
    if (parseReplayRange(response.summary, &replayRange)) {
        replayRangeJson = {
            {"first_session_seq", replayRange.firstSessionSeq},
            {"last_session_seq", replayRange.lastSessionSeq}
        };
    }
    return {
        {"schema", tape_engine::kSeekOrderResultSchema},
        {"version", tape_engine::kSeekOrderResultVersion},
        {"served_revision_id", response.summary.value("served_revision_id", 0ULL)},
        {"includes_mutable_tail", response.summary.value("includes_mutable_tail", false)},
        {"replay_target_session_seq", response.summary.value("replay_target_session_seq", 0ULL)},
        {"first_session_seq", response.summary.value("first_session_seq", 0ULL)},
        {"last_session_seq", response.summary.value("last_session_seq", 0ULL)},
        {"last_fill_session_seq", response.summary.value("last_fill_session_seq", 0ULL)},
        {"replay_range", replayRangeJson},
        {"anchor", response.summary.value("anchor", json::object())},
        {"protected_window", response.summary.value("protected_window", json::object())}
    };
}

json legacyArtifactExportResult(const tape_engine::QueryResponse& response) {
    return {
        {"schema", tape_engine::kArtifactExportResultSchema},
        {"version", tape_engine::kArtifactExportResultVersion},
        {"artifact_id", response.summary.value("artifact_id", std::string())},
        {"format", response.summary.value("export_format", std::string())},
        {"served_revision_id", response.summary.value("served_revision_id", 0ULL)},
        {"artifact_export", response.summary.value("artifact_export", json::object())},
        {"markdown", response.summary.value("markdown", std::string())},
        {"bundle", response.summary.value("bundle", json::object())}
    };
}

json legacyBundleExportResult(const tape_engine::QueryResponse& response) {
    return {
        {"schema", tape_engine::kBundleExportResultSchema},
        {"version", tape_engine::kBundleExportResultVersion},
        {"artifact", response.summary.value("artifact", json::object())},
        {"bundle", response.summary.value("bundle", json::object())},
        {"source_artifact", response.summary.value("source_artifact", json::object())},
        {"source_report", response.summary.value("source_report", json::object())},
        {"served_revision_id", response.summary.value("served_revision_id", 0ULL)},
        {"export_status", response.summary.value("export_status", std::string())}
    };
}

json legacyBundleVerifyResult(const tape_engine::QueryResponse& response) {
    json result{
        {"schema", tape_engine::kBundleVerifyResultSchema},
        {"version", tape_engine::kBundleVerifyResultVersion},
        {"artifact", response.summary.value("artifact", json::object())},
        {"bundle", response.summary.value("bundle", json::object())},
        {"source_artifact", response.summary.value("source_artifact", json::object())},
        {"source_report", response.summary.value("source_report", json::object())},
        {"report_summary", response.summary.value("report_summary", json::object())},
        {"report_markdown", response.summary.value("report_markdown", std::string())},
        {"verify_status", response.summary.value("verify_status", std::string())},
        {"import_supported", response.summary.value("import_supported", false)},
        {"already_imported", response.summary.value("already_imported", false)},
        {"can_import", response.summary.value("can_import", false)},
        {"import_reason", response.summary.value("import_reason", std::string())},
        {"served_revision_id", response.summary.value("served_revision_id", 0ULL)}
    };
    const json importedCase = response.summary.value("imported_case", json::object());
    if (importedCase.is_object() && !importedCase.empty()) {
        result["imported_case"] = importedCase;
    }
    return result;
}

json legacyCaseBundleImportResult(const tape_engine::QueryResponse& response) {
    return {
        {"schema", tape_engine::kCaseBundleImportResultSchema},
        {"version", tape_engine::kCaseBundleImportResultVersion},
        {"artifact", response.summary.value("artifact", json::object())},
        {"imported_case", response.summary.value("imported_case", json::object())},
        {"import_status", response.summary.value("import_status", std::string())},
        {"duplicate_import", response.summary.value("duplicate_import", false)}
    };
}

json legacyImportedCaseInventoryResult(const tape_engine::QueryResponse& response) {
    return {
        {"schema", tape_engine::kImportedCaseInventoryResultSchema},
        {"version", tape_engine::kImportedCaseInventoryResultVersion},
        {"returned_count", response.events.is_array() ? response.events.size() : 0},
        {"imported_cases", response.events}
    };
}

QueryResult<StatusSnapshot> packStatusPayload(const QueryResult<tape_engine::QueryResponse>& response) {
    if (!response.ok()) {
        return propagateError<StatusSnapshot>(response.error);
    }
    const json* typedResultPtr = typedResultOrNull(response.value, tape_engine::kStatusResultSchema);
    const json typedResult = typedResultPtr != nullptr ? *typedResultPtr : legacyStatusResult(response.value);
    if (!typedResult.is_object()) {
        return makeError<StatusSnapshot>(QueryErrorKind::MalformedResponse,
                                         "status result must be an object");
    }

    StatusSnapshot snapshot;
    snapshot.socketPath = typedResult.value("socket_path", std::string());
    snapshot.dataDir = typedResult.value("data_dir", std::string());
    snapshot.instrumentId = typedResult.value("instrument_id", std::string());
    snapshot.latestSessionSeq = typedResult.value("latest_session_seq", 0ULL);
    snapshot.liveEventCount = typedResult.value("live_event_count", 0ULL);
    snapshot.segmentCount = typedResult.value("segment_count", 0ULL);
    snapshot.manifestHash = typedResult.value("last_manifest_hash", std::string());
    return makeSuccess(std::move(snapshot));
}

QueryResult<InvestigationPayload> packInvestigationPayload(const QueryResult<tape_engine::QueryResponse>& response) {
    if (!response.ok()) {
        return propagateError<InvestigationPayload>(response.error);
    }
    const json* typedResultPtr = typedResultOrNull(response.value, tape_engine::kInvestigationResultSchema);
    const json typedResult = typedResultPtr != nullptr ? *typedResultPtr : legacyInvestigationResult(response.value);
    if (!typedResult.is_object()) {
        return makeError<InvestigationPayload>(QueryErrorKind::MalformedResponse,
                                               "investigation result must be an object");
    }
    const json typedEvents = typedResult.value("events", response.value.events);
    if (!typedEvents.is_array()) {
        return makeError<InvestigationPayload>(QueryErrorKind::MalformedResponse,
                                               "investigation result events must be an array");
    }

    InvestigationPayload payload;
    payload.summary = response.value.summary.is_object() ? response.value.summary : json::object();
    payload.raw = json::object();
    payload.raw["result"] = typedResult;
    payload.raw["summary"] = response.value.summary;
    payload.raw["events"] = response.value.events;
    payload.events.reserve(typedEvents.size());
    for (const auto& event : typedEvents) {
        payload.events.push_back(event);
    }
    payload.incidents = parseIncidentRows(typedResult.value("incident_rows", json::array()));

    const json citationRows = typedResult.value("citation_rows", json::array());
    if (citationRows.is_array()) {
        for (const auto& item : citationRows) {
            EvidenceCitation citation;
            citation.raw = item;
            if (item.is_object()) {
                citation.kind = item.value("kind", std::string());
                citation.artifactId = item.value("artifact_id", std::string());
                citation.label = item.value("label", std::string());
            }
            payload.evidence.push_back(std::move(citation));
        }
    } else {
        payload.evidence = parseEvidenceCitations(payload.summary);
    }

    const json artifact = typedResult.value("artifact", payload.summary.value("artifact", json::object()));
    payload.artifactId = artifact.value("artifact_id", artifact.value("id", std::string()));
    payload.artifactKind = artifact.value("kind", artifact.value("artifact_type", std::string()));

    payload.headline = typedResult.value("headline", std::string());
    if (payload.headline.empty()) {
        const json report = payload.summary.value("report", json::object());
        payload.headline = firstPresentString(report, {"title", "headline"});
        if (payload.headline.empty()) {
            payload.headline = firstPresentString(payload.summary, {"headline", "title", "why_it_matters"});
        }
    }
    payload.detail = typedResult.value("detail", std::string());
    if (payload.detail.empty()) {
        const json report = payload.summary.value("report", json::object());
        payload.detail = firstPresentString(report, {"summary", "why_it_matters"});
        if (payload.detail.empty()) {
            payload.detail = firstPresentString(payload.summary, {"what_changed_first", "why_it_matters", "headline"});
        }
    }

    const json replayRangeJson = typedResult.value("replay_range", json(nullptr));
    if (replayRangeJson.is_object()) {
        payload.replayRange = RangeQuery{
            replayRangeJson.value("first_session_seq", 1ULL),
            replayRangeJson.value("last_session_seq", 0ULL)
        };
    } else {
        RangeQuery replayRange;
        if (parseSeekReplayRange(payload.summary, &replayRange) || parseReplayRange(payload.summary, &replayRange)) {
            payload.replayRange = replayRange;
        }
    }
    return makeSuccess(std::move(payload));
}

QueryResult<EventListPayload> packEventListPayload(const QueryResult<tape_engine::QueryResponse>& response) {
    if (!response.ok()) {
        return propagateError<EventListPayload>(response.error);
    }
    const json* typedResultPtr = typedResultOrNull(response.value, tape_engine::kEventListResultSchema);
    const json typedResult = typedResultPtr != nullptr ? *typedResultPtr : legacyEventListResult(response.value);
    if (!typedResult.is_object()) {
        return makeError<EventListPayload>(QueryErrorKind::MalformedResponse,
                                           "event-list result must be an object");
    }
    const json events = typedResult.value("events", response.value.events);
    if (!events.is_array()) {
        return makeError<EventListPayload>(QueryErrorKind::MalformedResponse,
                                           "event-list result events must be an array");
    }

    EventListPayload payload;
    payload.summary = response.value.summary;
    payload.raw = json::object();
    payload.raw["result"] = typedResult;
    payload.raw["summary"] = response.value.summary;
    payload.raw["events"] = response.value.events;
    payload.events = parseEventRows(events);
    return makeSuccess(std::move(payload));
}

QueryResult<SessionQualityPayload> packSessionQualityPayload(const QueryResult<tape_engine::QueryResponse>& response) {
    if (!response.ok()) {
        return propagateError<SessionQualityPayload>(response.error);
    }
    const json* typedResultPtr = typedResultOrNull(response.value, tape_engine::kSessionQualityResultSchema);
    const json typedResult = typedResultPtr != nullptr ? *typedResultPtr : legacySessionQualityResult(response.value);
    if (!typedResult.is_object()) {
        return makeError<SessionQualityPayload>(QueryErrorKind::MalformedResponse,
                                                "session quality result must be an object");
    }
    SessionQualityPayload payload;
    payload.summary = response.value.summary;
    payload.dataQuality = typedResult.value("data_quality", payload.summary.value("data_quality", json::object()));
    payload.raw = json::object();
    payload.raw["result"] = typedResult;
    payload.raw["summary"] = response.value.summary;
    payload.raw["events"] = response.value.events;
    return makeSuccess(std::move(payload));
}

QueryResult<SeekOrderPayload> packSeekOrderPayload(const QueryResult<tape_engine::QueryResponse>& response) {
    if (!response.ok()) {
        return propagateError<SeekOrderPayload>(response.error);
    }
    const json* typedResultPtr = typedResultOrNull(response.value, tape_engine::kSeekOrderResultSchema);
    const json typedResult = typedResultPtr != nullptr ? *typedResultPtr : legacySeekOrderResult(response.value);
    if (!typedResult.is_object()) {
        return makeError<SeekOrderPayload>(QueryErrorKind::MalformedResponse,
                                           "seek result must be an object");
    }

    SeekOrderPayload payload;
    payload.summary = response.value.summary;
    payload.raw = json::object();
    payload.raw["result"] = typedResult;
    payload.raw["summary"] = response.value.summary;
    payload.raw["events"] = response.value.events;
    payload.replayTargetSessionSeq = typedResult.value("replay_target_session_seq", 0ULL);
    payload.firstSessionSeq = typedResult.value("first_session_seq", 0ULL);
    payload.lastSessionSeq = typedResult.value("last_session_seq", 0ULL);
    payload.lastFillSessionSeq = typedResult.value("last_fill_session_seq", 0ULL);
    const json replayRangeJson = typedResult.value("replay_range", json(nullptr));
    if (replayRangeJson.is_object()) {
        payload.replayRange = RangeQuery{
            replayRangeJson.value("first_session_seq", 1ULL),
            replayRangeJson.value("last_session_seq", 0ULL)
        };
    } else {
        RangeQuery replayRange;
        if (parseReplayRange(payload.summary, &replayRange)) {
            payload.replayRange = replayRange;
        }
    }
    return makeSuccess(std::move(payload));
}

QueryResult<IncidentListPayload> packIncidentListPayload(const QueryResult<tape_engine::QueryResponse>& response) {
    if (!response.ok()) {
        return propagateError<IncidentListPayload>(response.error);
    }
    const json* typedResultPtr = typedCollectionResultOrNull(response.value, "incidents");
    const json typedResult = typedResultPtr != nullptr ? *typedResultPtr : legacyCollectionResult(response.value, "incidents");
    const json incidents = typedResult.value("rows", response.value.events);
    if (!incidents.is_array()) {
        return makeError<IncidentListPayload>(QueryErrorKind::MalformedResponse,
                                              "incident list rows must be an array");
    }

    IncidentListPayload payload;
    payload.raw = json::object();
    payload.raw["result"] = typedResult;
    payload.raw["summary"] = response.value.summary;
    payload.raw["events"] = response.value.events;
    payload.incidents = parseIncidentRows(incidents);
    return makeSuccess(std::move(payload));
}

QueryResult<CollectionRowsPayload> packCollectionRowsPayload(const QueryResult<tape_engine::QueryResponse>& response,
                                                             const char* expectedKind) {
    if (!response.ok()) {
        return propagateError<CollectionRowsPayload>(response.error);
    }
    const json* typedResultPtr = typedCollectionResultOrNull(response.value, expectedKind);
    const json typedResult = typedResultPtr != nullptr ? *typedResultPtr : legacyCollectionResult(response.value, expectedKind);
    const json rows = typedResult.value("rows", response.value.events);
    if (!typedResult.is_object()) {
        return makeError<CollectionRowsPayload>(QueryErrorKind::MalformedResponse,
                                                "collection result must be an object");
    }
    if (!rows.is_array()) {
        return makeError<CollectionRowsPayload>(QueryErrorKind::MalformedResponse,
                                                "collection result rows must be an array");
    }

    CollectionRowsPayload payload;
    payload.summary = response.value.summary;
    payload.collectionKind = typedResult.value("collection_kind", std::string(expectedKind));
    payload.servedRevisionId = typedResult.value("served_revision_id",
                                                 response.value.summary.value("served_revision_id", 0ULL));
    payload.includesMutableTail = typedResult.value("includes_mutable_tail",
                                                    response.value.summary.value("includes_mutable_tail", false));
    payload.totalCount = typedResult.value("total_count",
                                           static_cast<std::size_t>(response.value.summary.value("returned_events", 0ULL)));
    payload.raw = json::object();
    payload.raw["result"] = typedResult;
    payload.raw["summary"] = response.value.summary;
    payload.raw["events"] = response.value.events;
    payload.rows.reserve(rows.size());
    for (const auto& row : rows) {
        payload.rows.push_back(row);
    }
    return makeSuccess(std::move(payload));
}

QueryResult<ReportInventoryPayload> packReportInventoryPayload(const QueryResult<tape_engine::QueryResponse>& response,
                                                               bool sessionReports) {
    if (!response.ok()) {
        return propagateError<ReportInventoryPayload>(response.error);
    }
    const char* expectedKind = sessionReports ? "session_reports" : "case_reports";
    const json* typedResultPtr = typedCollectionResultOrNull(response.value, expectedKind);
    const json typedResult = typedResultPtr != nullptr ? *typedResultPtr : legacyCollectionResult(response.value, expectedKind);
    const json rows = typedResult.value("rows", response.value.events);
    if (!rows.is_array()) {
        return makeError<ReportInventoryPayload>(QueryErrorKind::MalformedResponse,
                                                 "report inventory rows must be an array");
    }

    ReportInventoryPayload payload;
    payload.raw = json::object();
    payload.raw["result"] = typedResult;
    payload.raw["summary"] = response.value.summary;
    payload.raw["events"] = response.value.events;
    if (sessionReports) {
        payload.sessionReports = parseReportRows(rows);
    } else {
        payload.caseReports = parseReportRows(rows);
    }
    return makeSuccess(std::move(payload));
}

QueryResult<ArtifactExportPayload> packArtifactExportPayload(const QueryResult<tape_engine::QueryResponse>& response) {
    if (!response.ok()) {
        return propagateError<ArtifactExportPayload>(response.error);
    }
    const json* typedResultPtr = typedResultOrNull(response.value, tape_engine::kArtifactExportResultSchema);
    const json typedResult = typedResultPtr != nullptr ? *typedResultPtr : legacyArtifactExportResult(response.value);
    if (!typedResult.is_object()) {
        return makeError<ArtifactExportPayload>(QueryErrorKind::MalformedResponse,
                                                "artifact export result must be an object");
    }

    ArtifactExportPayload payload;
    payload.summary = response.value.summary;
    payload.raw = json::object();
    payload.raw["result"] = typedResult;
    payload.raw["summary"] = response.value.summary;
    payload.raw["events"] = response.value.events;
    payload.artifactId = typedResult.value("artifact_id", response.value.summary.value("artifact_id", std::string()));
    payload.format = typedResult.value("format", response.value.summary.value("export_format", std::string()));
    payload.servedRevisionId = typedResult.value("served_revision_id",
                                                 response.value.summary.value("served_revision_id", 0ULL));
    payload.artifactExport = typedResult.value("artifact_export",
                                               response.value.summary.value("artifact_export", json::object()));
    payload.markdown = typedResult.value("markdown", response.value.summary.value("markdown", std::string()));
    payload.bundle = typedResult.value("bundle", response.value.summary.value("bundle", json::object()));

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

QueryResult<ReplaySnapshotPayload> packReplaySnapshotPayload(const QueryResult<tape_engine::QueryResponse>& response) {
    if (!response.ok()) {
        return propagateError<ReplaySnapshotPayload>(response.error);
    }
    const json* typedResultPtr = typedResultOrNull(response.value, tape_engine::kReplaySnapshotResultSchema);
    const json typedResult = typedResultPtr != nullptr
        ? *typedResultPtr
        : (response.value.summary.is_object() ? response.value.summary : json::object());
    if (!typedResult.is_object()) {
        return makeError<ReplaySnapshotPayload>(QueryErrorKind::MalformedResponse,
                                                "replay snapshot result must be an object");
    }

    ReplaySnapshotPayload payload;
    payload.summary = response.value.summary;
    payload.raw = json::object();
    payload.raw["result"] = typedResult;
    payload.raw["summary"] = response.value.summary;
    payload.raw["events"] = response.value.events;
    payload.servedRevisionId = typedResult.value("served_revision_id",
                                                 response.value.summary.value("served_revision_id", 0ULL));
    payload.includesMutableTail = typedResult.value("includes_mutable_tail",
                                                    response.value.summary.value("includes_mutable_tail", false));
    payload.targetSessionSeq = typedResult.value("target_session_seq", 0ULL);
    payload.replayedThroughSessionSeq = typedResult.value("replayed_through_session_seq", 0ULL);
    payload.appliedEventCount = typedResult.value("applied_event_count", 0ULL);
    payload.gapMarkersEncountered = typedResult.value("gap_markers_encountered", 0ULL);
    payload.checkpointUsed = typedResult.value("checkpoint_used", false);
    payload.checkpointRevisionId = typedResult.value("checkpoint_revision_id", 0ULL);
    payload.checkpointSessionSeq = typedResult.value("checkpoint_session_seq", 0ULL);
    payload.bidPrice = typedResult.value("bid_price", json(nullptr));
    payload.askPrice = typedResult.value("ask_price", json(nullptr));
    payload.lastPrice = typedResult.value("last_price", json(nullptr));
    payload.bidBook = typedResult.value("bid_book", json::array());
    payload.askBook = typedResult.value("ask_book", json::array());
    payload.dataQuality = typedResult.value("data_quality", json::object());
    return makeSuccess(std::move(payload));
}

QueryResult<BundleExportPayload> packBundleExportPayload(const QueryResult<tape_engine::QueryResponse>& response) {
    if (!response.ok()) {
        return propagateError<BundleExportPayload>(response.error);
    }
    const json* typedResultPtr = typedResultOrNull(response.value, tape_engine::kBundleExportResultSchema);
    const json typedResult = typedResultPtr != nullptr ? *typedResultPtr : legacyBundleExportResult(response.value);
    if (!typedResult.is_object()) {
        return makeError<BundleExportPayload>(QueryErrorKind::MalformedResponse,
                                              "bundle export result must be an object");
    }

    BundleExportPayload payload;
    payload.summary = response.value.summary;
    payload.raw = json::object();
    payload.raw["result"] = typedResult;
    payload.raw["summary"] = response.value.summary;
    payload.raw["events"] = response.value.events;
    payload.artifact = typedResult.value("artifact", json::object());
    payload.bundle = typedResult.value("bundle", json::object());
    payload.sourceArtifact = typedResult.value("source_artifact", json::object());
    payload.sourceReport = typedResult.value("source_report", json::object());
    payload.artifactId = payload.artifact.value("artifact_id", std::string());
    payload.bundleId = payload.bundle.value("bundle_id", std::string());
    payload.bundleType = payload.bundle.value("bundle_type", std::string());
    payload.bundlePath = payload.bundle.value("bundle_path", std::string());
    payload.servedRevisionId = typedResult.value("served_revision_id",
                                                 response.value.summary.value("served_revision_id", 0ULL));

    if (!payload.artifact.is_object() || payload.artifactId.empty()) {
        return makeError<BundleExportPayload>(QueryErrorKind::MalformedResponse,
                                              "bundle export is missing artifact metadata");
    }
    if (!payload.bundle.is_object() || payload.bundlePath.empty()) {
        return makeError<BundleExportPayload>(QueryErrorKind::MalformedResponse,
                                              "bundle export is missing bundle metadata");
    }
    return makeSuccess(std::move(payload));
}

QueryResult<BundleVerifyPayload> packBundleVerifyPayload(const QueryResult<tape_engine::QueryResponse>& response) {
    if (!response.ok()) {
        return propagateError<BundleVerifyPayload>(response.error);
    }
    const json* typedResultPtr = typedResultOrNull(response.value, tape_engine::kBundleVerifyResultSchema);
    const json typedResult = typedResultPtr != nullptr ? *typedResultPtr : legacyBundleVerifyResult(response.value);
    if (!typedResult.is_object()) {
        return makeError<BundleVerifyPayload>(QueryErrorKind::MalformedResponse,
                                              "bundle verify result must be an object");
    }

    BundleVerifyPayload payload;
    payload.summary = response.value.summary;
    payload.raw = json::object();
    payload.raw["result"] = typedResult;
    payload.raw["summary"] = response.value.summary;
    payload.raw["events"] = response.value.events;
    payload.artifact = typedResult.value("artifact", json::object());
    payload.bundle = typedResult.value("bundle", json::object());
    payload.sourceArtifact = typedResult.value("source_artifact", json::object());
    payload.sourceReport = typedResult.value("source_report", json::object());
    payload.reportSummary = typedResult.value("report_summary", json::object());
    payload.importSupported = typedResult.value("import_supported", false);
    payload.alreadyImported = typedResult.value("already_imported", false);
    payload.canImport = typedResult.value("can_import", false);
    payload.verifyStatus = typedResult.value("verify_status", std::string());
    payload.importReason = typedResult.value("import_reason", std::string());
    payload.artifactId = payload.artifact.value("artifact_id", std::string());
    payload.bundleId = payload.bundle.value("bundle_id", std::string());
    payload.bundleType = payload.bundle.value("bundle_type", std::string());
    payload.bundlePath = payload.bundle.value("bundle_path", std::string());
    payload.payloadSha256 = payload.bundle.value("payload_sha256", std::string());
    payload.servedRevisionId = typedResult.value("served_revision_id",
                                                 response.value.summary.value("served_revision_id", 0ULL));
    payload.reportMarkdown = typedResult.value("report_markdown", std::string());

    const json importedCase = typedResult.value("imported_case", json::object());
    payload.hasImportedCase = importedCase.is_object() && !importedCase.empty();
    if (payload.hasImportedCase) {
        payload.importedCase = parseImportedCaseRow(importedCase);
    }

    if (!payload.artifact.is_object() || payload.artifactId.empty()) {
        return makeError<BundleVerifyPayload>(QueryErrorKind::MalformedResponse,
                                              "bundle verify is missing artifact metadata");
    }
    if (!payload.bundle.is_object() || payload.bundlePath.empty() || payload.bundleId.empty()) {
        return makeError<BundleVerifyPayload>(QueryErrorKind::MalformedResponse,
                                              "bundle verify is missing bundle metadata");
    }
    if (payload.verifyStatus.empty()) {
        return makeError<BundleVerifyPayload>(QueryErrorKind::MalformedResponse,
                                              "bundle verify is missing verify_status");
    }
    return makeSuccess(std::move(payload));
}

QueryResult<ImportedCaseListPayload> packImportedCaseListPayload(const QueryResult<tape_engine::QueryResponse>& response) {
    if (!response.ok()) {
        return propagateError<ImportedCaseListPayload>(response.error);
    }
    const json* typedResultPtr = typedResultOrNull(response.value, tape_engine::kImportedCaseInventoryResultSchema);
    const json typedResult = typedResultPtr != nullptr ? *typedResultPtr : legacyImportedCaseInventoryResult(response.value);
    if (!typedResult.is_object()) {
        return makeError<ImportedCaseListPayload>(QueryErrorKind::MalformedResponse,
                                                  "imported-case inventory result must be an object");
    }
    const json importedCases = typedResult.value("imported_cases", json::array());
    if (!importedCases.is_array()) {
        return makeError<ImportedCaseListPayload>(QueryErrorKind::MalformedResponse,
                                                  "imported-case inventory is missing imported_cases rows");
    }

    ImportedCaseListPayload payload;
    payload.raw = json::object();
    payload.raw["result"] = typedResult;
    payload.raw["summary"] = response.value.summary;
    payload.raw["events"] = response.value.events;
    payload.importedCases = parseImportedCaseRows(importedCases);
    return makeSuccess(std::move(payload));
}

QueryResult<CaseBundleImportPayload> packCaseBundleImportPayload(const QueryResult<tape_engine::QueryResponse>& response) {
    if (!response.ok()) {
        return propagateError<CaseBundleImportPayload>(response.error);
    }
    const json* typedResultPtr = typedResultOrNull(response.value, tape_engine::kCaseBundleImportResultSchema);
    const json typedResult = typedResultPtr != nullptr ? *typedResultPtr : legacyCaseBundleImportResult(response.value);
    if (!typedResult.is_object()) {
        return makeError<CaseBundleImportPayload>(QueryErrorKind::MalformedResponse,
                                                  "case-bundle import result must be an object");
    }

    CaseBundleImportPayload payload;
    payload.summary = response.value.summary;
    payload.raw = json::object();
    payload.raw["result"] = typedResult;
    payload.raw["summary"] = response.value.summary;
    payload.raw["events"] = response.value.events;
    payload.artifact = typedResult.value("artifact", json::object());
    payload.importStatus = typedResult.value("import_status", std::string());
    payload.duplicateImport = typedResult.value("duplicate_import", false);
    payload.artifactId = payload.artifact.value("artifact_id", std::string());

    const json importedCase = typedResult.value("imported_case", json::object());
    if (!importedCase.is_object()) {
        return makeError<CaseBundleImportPayload>(QueryErrorKind::MalformedResponse,
                                                  "case-bundle import is missing imported_case metadata");
    }
    payload.importedCase = parseImportedCaseRow(importedCase);
    if (payload.artifactId.empty()) {
        payload.artifactId = payload.importedCase.artifactId;
    }
    if (payload.artifactId.empty() || payload.importStatus.empty()) {
        return makeError<CaseBundleImportPayload>(QueryErrorKind::MalformedResponse,
                                                  "case-bundle import is missing required result metadata");
    }
    return makeSuccess(std::move(payload));
}

} // namespace tape_payloads
