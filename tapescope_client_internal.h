#pragma once

#include "tapescope_client.h"

#include <initializer_list>
#include <utility>

namespace tapescope::client_internal {

template <typename T>
QueryResult<T> makeError(QueryErrorKind kind, std::string message) {
    QueryResult<T> result;
    result.error.kind = kind;
    result.error.message = std::move(message);
    return result;
}

template <typename T>
QueryResult<T> makeSuccess(T value) {
    QueryResult<T> result;
    result.value = std::move(value);
    return result;
}

template <typename T>
QueryResult<T> propagateError(const QueryError& error) {
    QueryResult<T> result;
    result.error = error;
    return result;
}

std::string firstPresentString(const json& payload,
                               std::initializer_list<const char*> keys);
bool parseReplayRange(const json& summary, RangeQuery* range);
bool parseSeekReplayRange(const json& summary, RangeQuery* range);
std::vector<EvidenceCitation> parseEvidenceCitations(const json& summary);
EventRow parseEventRow(const json& event);
std::vector<EventRow> parseEventRows(const json& events);
std::vector<IncidentListRow> parseIncidentRows(const json& events);
std::vector<ReportInventoryRow> parseReportRows(const json& events);

QueryResult<InvestigationPayload> packInvestigationPayload(const QueryResult<tape_engine::QueryResponse>& response);
QueryResult<EventListPayload> packEventListPayload(const QueryResult<tape_engine::QueryResponse>& response);
QueryResult<SessionQualityPayload> packSessionQualityPayload(const QueryResult<tape_engine::QueryResponse>& response);
QueryResult<SeekOrderPayload> packSeekOrderPayload(const QueryResult<tape_engine::QueryResponse>& response);
QueryResult<IncidentListPayload> packIncidentListPayload(const QueryResult<tape_engine::QueryResponse>& response);
QueryResult<ReportInventoryPayload> packReportInventoryPayload(const QueryResult<tape_engine::QueryResponse>& response,
                                                               bool sessionReports);
QueryResult<ArtifactExportPayload> packArtifactExportPayload(const QueryResult<tape_engine::QueryResponse>& response);

} // namespace tapescope::client_internal
