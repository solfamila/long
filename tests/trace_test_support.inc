#include "app_shared.h"
#include "bridge_batch_codec.h"
#include "bridge_batch_transport.h"
#include "controller_claim.h"
#include "runtime_registry.h"
#include "tape_bundle_inspection.h"
#include "tapescope_bundle_preview.h"
#include "tapescope_client.h"
#include "tape_engine_client.h"
#include "tape_engine.h"
#include "tape_engine_protocol.h"
#include "tape_phase7_artifacts.h"
#include "trace_exporter.h"
#include "trading_ui_format.h"
#include "trading_wrapper.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <condition_variable>
#include <cstring>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <mutex>
#include <stdexcept>
#include <set>
#include <string>
#include <thread>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#define g_data appState()

namespace fs = std::filesystem;

namespace {

void expect(bool condition, const std::string& message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

void expectContains(const std::string& text, const std::string& needle, const std::string& message) {
    expect(text.find(needle) != std::string::npos, message + " (missing: " + needle + ")");
}

template <typename Fn>
void waitUntil(Fn&& predicate,
               const std::string& message,
               std::chrono::milliseconds timeout = std::chrono::milliseconds(2000),
               std::chrono::milliseconds pollInterval = std::chrono::milliseconds(10)) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return;
        }
        std::this_thread::sleep_for(pollInterval);
    }
    expect(predicate(), message);
}

const fs::path& testDataDir() {
    static const fs::path path = [] {
        char pattern[] = "/tmp/tws_gui_tests.XXXXXX";
        char* created = mkdtemp(pattern);
        if (created == nullptr) {
            throw std::runtime_error("mkdtemp failed");
        }
        setenv("TWS_GUI_DATA_DIR", created, 1);
        return fs::path(created);
    }();
    return path;
}

void clearTestFiles() {
    resetSharedDataForTesting();
    std::error_code ec;
    fs::create_directories(testDataDir(), ec);
    fs::remove(tradeTraceLogPath(), ec);
    fs::remove(runtimeJournalLogPath(), ec);
}

json makeTraceLine(std::uint64_t traceId,
                   OrderId orderId,
                   long long permId,
                   const std::string& source,
                   const std::string& symbol,
                   const std::string& side,
                   const std::string& eventType,
                   const std::string& stage,
                   const std::string& details,
                   double sinceTriggerMs,
                   double price = 0.0,
                   int shares = 0,
                   double cumFilled = -1.0) {
    json line;
    line["traceId"] = traceId;
    line["orderId"] = static_cast<long long>(orderId);
    line["permId"] = permId;
    line["source"] = source;
    line["symbol"] = symbol;
    line["side"] = side;
    line["requestedQty"] = 1;
    line["limitPrice"] = 45.64;
    line["closeOnly"] = false;
    line["eventType"] = eventType;
    line["stage"] = stage;
    line["details"] = details;
    line["wallTime"] = "09:30:00.000";
    line["sinceTriggerMs"] = sinceTriggerMs;
    if (price > 0.0) {
        line["price"] = price;
    }
    if (shares > 0) {
        line["shares"] = shares;
    }
    if (cumFilled >= 0.0) {
        line["cumFilled"] = cumFilled;
    }
    return line;
}

void appendTraceLine(const json& line) {
    std::ofstream out(tradeTraceLogPath(), std::ios::app);
    expect(out.is_open(), "failed to open trace log for append");
    out << line.dump() << '\n';
}

void appendJournalLine(const json& line) {
    std::ofstream out(runtimeJournalLogPath(), std::ios::app);
    expect(out.is_open(), "failed to open runtime journal for append");
    out << line.dump() << '\n';
}

std::vector<json> readJsonLines(const std::string& path) {
    std::ifstream in(path);
    expect(in.is_open(), "failed to open json log at " + path);

    std::vector<json> lines;
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) {
            continue;
        }
        json parsed = json::parse(line, nullptr, false);
        expect(!parsed.is_discarded(), "failed to parse json log line from " + path);
        lines.push_back(std::move(parsed));
    }
    return lines;
}

fs::path fixturePath(const std::string& relativePath) {
    return fs::path(TWS_GUI_SOURCE_DIR) / "tests" / "fixtures" / relativePath;
}

std::string readTextFile(const fs::path& path) {
    std::ifstream in(path);
    expect(in.is_open(), "failed to open fixture file at " + path.string());
    return std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
}

json readJsonFixture(const std::string& relativePath) {
    return json::parse(readTextFile(fixturePath(relativePath)));
}

json readMsgpackFile(const fs::path& path) {
    std::ifstream in(path, std::ios::binary);
    expect(in.is_open(), "failed to open msgpack file at " + path.string());
    const std::vector<std::uint8_t> bytes((std::istreambuf_iterator<char>(in)),
                                          std::istreambuf_iterator<char>());
    expect(!bytes.empty(), "msgpack file is empty at " + path.string());
    return json::from_msgpack(bytes, true, false);
}

std::string trimWhitespace(std::string text) {
    text.erase(std::remove_if(text.begin(), text.end(), [](unsigned char ch) {
        return std::isspace(ch) != 0;
    }), text.end());
    return text;
}

std::string normalizeArtifactId(std::string artifactId) {
    const auto replaceSuffix = [&](const std::string& prefix, const std::string& replacement) {
        if (artifactId.rfind(prefix, 0) == 0) {
            artifactId = prefix + replacement;
        }
    };
    replaceSuffix("session-report:", "<id>");
    replaceSuffix("case-report:", "<id>");
    replaceSuffix("session-bundle:report:", "<id>");
    replaceSuffix("case-bundle:report:", "<id>");
    replaceSuffix("imported-case:", "<id>");
    replaceSuffix("incident:", "<id>");
    replaceSuffix("window:", "<id>");
    replaceSuffix("finding:", "<id>");
    replaceSuffix("anchor:", "<id>");
    if (artifactId.rfind("session-overview:", 0) == 0) {
        return "session-overview:<revision>:<from>:<to>";
    }
    if (artifactId.rfind("order-case:order:", 0) == 0) {
        return "order-case:order:<id>";
    }
    if (artifactId.rfind("order-case:trace:", 0) == 0) {
        return "order-case:trace:<id>";
    }
    if (artifactId.rfind("order-case:perm:", 0) == 0) {
        return "order-case:perm:<id>";
    }
    if (artifactId.rfind("order-case:exec:", 0) == 0) {
        return "order-case:exec:<id>";
    }
    return artifactId;
}

json projectArtifactSummary(const json& artifact) {
    if (!artifact.is_object()) {
        return json::object();
    }
    json projected = {
        {"artifact_id", normalizeArtifactId(artifact.value("artifact_id", std::string()))},
        {"artifact_type", artifact.value("artifact_type", std::string())},
        {"artifact_scope", artifact.value("artifact_scope", std::string())}
    };
    if (artifact.contains("schema_version")) {
        projected["schema_version"] = artifact.value("schema_version", 0U);
    }
    return projected;
}

json projectEntitySummary(const json& entity) {
    if (!entity.is_object()) {
        return json::object();
    }
    return {
        {"type", entity.value("type", std::string())},
        {"entity_type", entity.value("entity_type", std::string())},
        {"schema_version", entity.value("schema_version", 0U)}
    };
}

json projectReportSummary(const json& report) {
    if (!report.is_object()) {
        return json::object();
    }
    json projected{
        {"headline", report.value("headline", std::string())},
        {"report_type", report.value("report_type", std::string())},
        {"schema_version", report.value("schema_version", 0U)}
    };
    if (report.contains("what_changed_first")) {
        projected["what_changed_first"] = report.value("what_changed_first", std::string());
    }
    if (report.contains("uncertainty")) {
        projected["uncertainty"] = report.value("uncertainty", std::string());
    }
    if (report.contains("top_incident_kind")) {
        projected["top_incident_kind"] = report.value("top_incident_kind", std::string());
    }
    return projected;
}

json projectEvidenceSummary(const json& evidence) {
    if (!evidence.is_object()) {
        return json::object();
    }
    json projected{
        {"schema_version", evidence.value("schema_version", 0U)},
        {"has_data_quality", evidence.contains("data_quality")},
        {"has_timeline", evidence.contains("timeline")},
        {"has_timeline_summary", evidence.contains("timeline_summary")}
    };
    json citations = json::array();
    for (const auto& citation : evidence.value("citations", json::array())) {
        citations.push_back({
            {"artifact_id", normalizeArtifactId(citation.value("artifact_id", std::string()))},
            {"type", citation.value("type", std::string())}
        });
    }
    projected["citations"] = citations;
    return projected;
}

json projectApiSummary(const json& api) {
    if (!api.is_object()) {
        return json::object();
    }
    return {
        {"response_kind", api.value("response_kind", std::string())},
        {"envelope_schema", api.value("envelope_schema", std::string())},
        {"envelope_version", api.value("envelope_version", 0U)},
        {"wire_schema", api.value("wire_schema", std::string())},
        {"wire_version", api.value("wire_version", 0U)}
    };
}

json projectContractResponse(const tape_engine::QueryResponse& response) {
    json summary = {
        {"api", projectApiSummary(response.summary.value("api", json::object()))},
        {"is_durable_report", response.summary.value("is_durable_report", false)}
    };
    const json artifact = projectArtifactSummary(response.summary.value("artifact", json::object()));
    if (!artifact.empty() &&
        (!artifact.value("artifact_id", std::string()).empty() ||
         !artifact.value("artifact_type", std::string()).empty())) {
        summary["artifact"] = artifact;
    }
    const json entity = projectEntitySummary(response.summary.value("entity", json::object()));
    if (!entity.empty() &&
        (!entity.value("type", std::string()).empty() ||
         !entity.value("entity_type", std::string()).empty())) {
        summary["entity"] = entity;
    }
    const json report = projectReportSummary(response.summary.value("report", response.summary.value("report_summary", json::object())));
    if (!report.empty() &&
        (!report.value("headline", std::string()).empty() ||
         !report.value("report_type", std::string()).empty())) {
        summary["report"] = report;
    }
    const json evidence = projectEvidenceSummary(response.summary.value("evidence", json::object()));
    if (!evidence.empty() &&
        (!evidence.value("citations", json::array()).empty() ||
         evidence.value("has_timeline", false) ||
         evidence.value("has_data_quality", false))) {
        summary["evidence"] = evidence;
    }
    json projected{
        {"operation", response.operation},
        {"status", response.status},
        {"summary", summary}
    };
    if (response.summary.contains("source_artifact")) {
        projected["summary"]["source_artifact"] =
            projectArtifactSummary(response.summary.value("source_artifact", json::object()));
    }
    if (response.summary.contains("artifact_export")) {
        const json artifactExport = response.summary.value("artifact_export", json::object());
        projected["summary"]["artifact_export"] = {
            {"schema", artifactExport.value("schema", std::string())},
            {"version", artifactExport.value("version", 0U)},
            {"format", artifactExport.value("format", std::string())}
        };
    }
    if (response.summary.contains("bundle") && response.summary["bundle"].is_object()) {
        projected["summary"]["bundle"] = {
            {"operation", response.summary["bundle"].value("operation", std::string())},
            {"summary", {
                {"artifact", projectArtifactSummary(response.summary["bundle"].value("summary", json::object()).value("artifact", json::object()))},
                {"entity", projectEntitySummary(response.summary["bundle"].value("summary", json::object()).value("entity", json::object()))},
                {"report", projectReportSummary(response.summary["bundle"].value("summary", json::object()).value("report", json::object()))}
            }}
        };
    }
    return projected;
}

json normalizeHash(const json& value) {
    if (!value.is_string()) {
        return nullptr;
    }
    return value.get<std::string>().empty() ? json(nullptr) : json("<hash>");
}

json normalizePositiveId(std::uint64_t value) {
    return value == 0 ? json(nullptr) : json("<id>");
}

json normalizeTestPath(const json& value) {
    if (!value.is_string()) {
        return nullptr;
    }
    const std::string path = value.get<std::string>();
    const std::string root = testDataDir().string();
    if (path.rfind(root, 0) == 0) {
        return "<tmp>" + path.substr(root.size());
    }
    return path;
}

json projectReplayRangeSummary(const json& replayRange) {
    if (!replayRange.is_object()) {
        return nullptr;
    }
    return {
        {"first_session_seq", replayRange.value("first_session_seq", 0ULL)},
        {"last_session_seq", replayRange.value("last_session_seq", 0ULL)}
    };
}

json projectEventSummary(const json& event) {
    if (!event.is_object()) {
        return json::object();
    }
    json projected{
        {"session_seq", event.value("session_seq", 0ULL)},
        {"event_kind", event.value("event_kind", std::string())},
        {"symbol", event.value("symbol", std::string())}
    };
    if (event.contains("instrument_id")) {
        projected["instrument_id"] = event.value("instrument_id", std::string());
    }
    return projected;
}

json projectInvestigationCitations(const json& citations) {
    std::vector<json> rows;
    for (const auto& citation : citations) {
        rows.push_back({
            {"artifact_id", normalizeArtifactId(citation.value("artifact_id", std::string()))},
            {"type", citation.value("type", std::string())}
        });
    }
    std::sort(rows.begin(), rows.end(), [](const json& lhs, const json& rhs) {
        return lhs.dump() < rhs.dump();
    });
    json projected = json::array();
    for (auto& row : rows) {
        projected.push_back(std::move(row));
    }
    return projected;
}

json projectInvestigationIncidentKinds(const json& incidentRows) {
    std::vector<json> rows;
    for (const auto& row : incidentRows) {
        rows.push_back({
            {"logical_incident_id", normalizePositiveId(row.value("logical_incident_id", 0ULL))},
            {"kind", row.value("kind", std::string())}
        });
    }
    std::sort(rows.begin(), rows.end(), [](const json& lhs, const json& rhs) {
        return lhs.dump() < rhs.dump();
    });
    json projected = json::array();
    for (auto& row : rows) {
        projected.push_back(std::move(row));
    }
    return projected;
}

json projectCollectionRow(const std::string& collectionKind, const json& row) {
    if (!row.is_object()) {
        return json::object();
    }
    if (collectionKind == "order_anchors") {
        const json anchor = row.value("anchor", json::object());
        return {
            {"anchor_id", normalizePositiveId(row.value("anchor_id", 0ULL))},
            {"trace_id", anchor.value("trace_id", row.value("trace_id", 0ULL))},
            {"order_id", anchor.value("order_id", row.value("order_id", 0LL))},
            {"artifact_id", normalizeArtifactId(row.value("artifact_id", std::string()))},
            {"instrument_id", row.value("instrument_id", std::string())}
        };
    }
    if (collectionKind == "protected_windows") {
        return {
            {"window_id", normalizePositiveId(row.value("window_id", 0ULL))},
            {"reason", row.value("reason", std::string())},
            {"artifact_id", normalizeArtifactId(row.value("artifact_id", std::string()))}
        };
    }
    if (collectionKind == "findings") {
        return {
            {"finding_id", normalizePositiveId(row.value("finding_id", 0ULL))},
            {"kind", row.value("kind", std::string())},
            {"severity", row.value("severity", std::string())},
            {"artifact_id", normalizeArtifactId(row.value("artifact_id", std::string()))}
        };
    }
    if (collectionKind == "incidents") {
        return {
            {"logical_incident_id", normalizePositiveId(row.value("logical_incident_id", 0ULL))},
            {"kind", row.value("kind", std::string())},
            {"title", row.value("title", std::string())}
        };
    }
    return row;
}

json projectImportedCaseSummary(const json& row) {
    if (!row.is_object()) {
        return json::object();
    }
    return {
        {"imported_case_id", normalizePositiveId(row.value("imported_case_id", 0ULL))},
        {"artifact_id", normalizeArtifactId(row.value("artifact_id", std::string()))},
        {"bundle_type", row.value("bundle_type", std::string())},
        {"source_artifact_id", normalizeArtifactId(row.value("source_artifact_id", std::string()))},
        {"source_bundle_path", normalizeTestPath(row.contains("source_bundle_path")
                                                     ? row.at("source_bundle_path")
                                                     : json(nullptr))},
        {"payload_sha256", normalizeHash(row.contains("payload_sha256")
                                             ? row.at("payload_sha256")
                                             : json(nullptr))}
    };
}

json projectTransportResultBody(const tape_engine::QueryResponse& response) {
    const json& result = response.result;
    if (!result.is_object()) {
        return json::object();
    }
    const std::string schema = result.value("schema", std::string());
    if (schema == tape_engine::kStatusResultSchema) {
        return {
            {"schema", schema},
            {"version", result.value("version", 0U)},
            {"socket_path", normalizeTestPath(result.contains("socket_path") ? result.at("socket_path") : json(nullptr))},
            {"data_dir", normalizeTestPath(result.contains("data_dir") ? result.at("data_dir") : json(nullptr))},
            {"latest_frozen_revision_id", normalizePositiveId(result.value("latest_frozen_revision_id", 0ULL))},
            {"latest_session_seq", result.value("latest_session_seq", 0ULL)},
            {"manifest_hash", normalizeHash(result.contains("manifest_hash") ? result.at("manifest_hash") : json(nullptr))}
        };
    }
    if (schema == tape_engine::kEventListResultSchema) {
        const json events = result.value("events", json::array());
        return {
            {"schema", schema},
            {"version", result.value("version", 0U)},
            {"served_revision_id", normalizePositiveId(result.value("served_revision_id", 0ULL))},
            {"from_session_seq", result.value("from_session_seq", 0ULL)},
            {"to_session_seq", result.value("to_session_seq", 0ULL)},
            {"returned_count", result.value("returned_count", 0U)},
            {"first_event", events.is_array() && !events.empty() ? projectEventSummary(events.front()) : json(nullptr)},
            {"last_event", events.is_array() && !events.empty() ? projectEventSummary(events.back()) : json(nullptr)}
        };
    }
    if (schema == tape_engine::kSessionQualityResultSchema) {
        const json quality = result.value("data_quality", json::object());
        return {
            {"schema", schema},
            {"version", result.value("version", 0U)},
            {"served_revision_id", normalizePositiveId(result.value("served_revision_id", 0ULL))},
            {"first_session_seq", result.value("first_session_seq", 0ULL)},
            {"last_session_seq", result.value("last_session_seq", 0ULL)},
            {"score", quality.value("score", 0.0)},
            {"issue_count", quality.value("issues", json::array()).size()}
        };
    }
    if (schema == tape_engine::kInvestigationResultSchema) {
        return {
            {"schema", schema},
            {"version", result.value("version", 0U)},
            {"served_revision_id", normalizePositiveId(result.value("served_revision_id", 0ULL))},
            {"artifact_id", normalizeArtifactId(result.value("artifact_id", std::string()))},
            {"artifact_kind", result.value("artifact_kind", std::string())},
            {"headline", result.value("headline", std::string())},
            {"entity_type", result.value("entity", json::object()).value("entity_type", std::string())},
            {"report_type", result.value("report", json::object()).value("report_type", std::string())},
            {"citation_rows", projectInvestigationCitations(result.value("citation_rows", json::array()))},
            {"incident_rows", projectInvestigationIncidentKinds(result.value("incident_rows", json::array()))},
            {"replay_range", projectReplayRangeSummary(result.value("replay_range", json(nullptr)))}
        };
    }
    if (schema == tape_engine::kCollectionResultSchema) {
        const std::string collectionKind = result.value("collection_kind", std::string());
        json rows = json::array();
        for (const auto& row : result.value("rows", json::array())) {
            rows.push_back(projectCollectionRow(collectionKind, row));
        }
        return {
            {"schema", schema},
            {"version", result.value("version", 0U)},
            {"collection_kind", collectionKind},
            {"served_revision_id", normalizePositiveId(result.value("served_revision_id", 0ULL))},
            {"returned_count", result.value("returned_count", 0U)},
            {"rows", std::move(rows)}
        };
    }
    if (schema == tape_engine::kSeekOrderResultSchema) {
        return {
            {"schema", schema},
            {"version", result.value("version", 0U)},
            {"served_revision_id", normalizePositiveId(result.value("served_revision_id", 0ULL))},
            {"replay_target_session_seq", result.value("replay_target_session_seq", 0ULL)},
            {"first_session_seq", result.value("first_session_seq", 0ULL)},
            {"last_session_seq", result.value("last_session_seq", 0ULL)},
            {"last_fill_session_seq", result.value("last_fill_session_seq", 0ULL)},
            {"replay_range", projectReplayRangeSummary(result.value("replay_range", json(nullptr)))},
            {"anchor", {
                {"trace_id", result.value("anchor", json::object()).value("trace_id", 0ULL)},
                {"order_id", result.value("anchor", json::object()).value("order_id", 0LL)},
                {"perm_id", result.value("anchor", json::object()).value("perm_id", 0LL)},
                {"exec_id", result.value("anchor", json::object()).value("exec_id", std::string())}
            }}
        };
    }
    if (schema == tape_engine::kArtifactExportResultSchema) {
        const json artifactExport = result.value("artifact_export", json::object());
        return {
            {"schema", schema},
            {"version", result.value("version", 0U)},
            {"artifact_id", normalizeArtifactId(result.value("artifact_id", std::string()))},
            {"format", result.value("format", std::string())},
            {"served_revision_id", normalizePositiveId(result.value("served_revision_id", 0ULL))},
            {"artifact_export", {
                {"schema", artifactExport.value("schema", std::string())},
                {"version", artifactExport.value("version", 0U)}
            }}
        };
    }
    if (schema == tape_engine::kReplaySnapshotResultSchema) {
        const json quality = result.value("data_quality", json::object());
        return {
            {"schema", schema},
            {"version", result.value("version", 0U)},
            {"served_revision_id", normalizePositiveId(result.value("served_revision_id", 0ULL))},
            {"target_session_seq", result.value("target_session_seq", 0ULL)},
            {"replayed_through_session_seq", result.value("replayed_through_session_seq", 0ULL)},
            {"applied_event_count", result.value("applied_event_count", 0ULL)},
            {"checkpoint_used", result.value("checkpoint_used", false)},
            {"checkpoint_revision_id", normalizePositiveId(result.value("checkpoint_revision_id", 0ULL))},
            {"bid_price", result.value("bid_price", json(nullptr))},
            {"ask_price", result.value("ask_price", json(nullptr))},
            {"data_quality_score", quality.value("score", 0.0)}
        };
    }
    if (schema == tape_engine::kBundleExportResultSchema) {
        const json artifact = result.value("artifact", json::object());
        const json bundle = result.value("bundle", json::object());
        const json sourceArtifact = result.value("source_artifact", json::object());
        return {
            {"schema", schema},
            {"version", result.value("version", 0U)},
            {"artifact_id", normalizeArtifactId(artifact.value("artifact_id", std::string()))},
            {"bundle_type", bundle.value("bundle_type", std::string())},
            {"bundle_path", normalizeTestPath(bundle.contains("bundle_path") ? bundle.at("bundle_path") : json(nullptr))},
            {"served_revision_id", normalizePositiveId(result.value("served_revision_id", 0ULL))},
            {"source_artifact_id", normalizeArtifactId(sourceArtifact.value("artifact_id", std::string()))}
        };
    }
    if (schema == tape_engine::kBundleVerifyResultSchema) {
        const json bundle = result.value("bundle", json::object());
        const json importedCase = result.value("imported_case", json(nullptr));
        return {
            {"schema", schema},
            {"version", result.value("version", 0U)},
            {"artifact_id", normalizeArtifactId(result.value("artifact", json::object()).value("artifact_id", std::string()))},
            {"bundle_type", bundle.value("bundle_type", std::string())},
            {"bundle_path", normalizeTestPath(bundle.contains("bundle_path") ? bundle.at("bundle_path") : json(nullptr))},
            {"payload_sha256", normalizeHash(bundle.contains("payload_sha256") ? bundle.at("payload_sha256") : json(nullptr))},
            {"verify_status", result.value("verify_status", std::string())},
            {"import_supported", result.value("import_supported", false)},
            {"already_imported", result.value("already_imported", false)},
            {"can_import", result.value("can_import", false)},
            {"import_reason", result.value("import_reason", std::string())},
            {"imported_case", importedCase.is_object() ? projectImportedCaseSummary(importedCase) : json(nullptr)}
        };
    }
    if (schema == tape_engine::kCaseBundleImportResultSchema) {
        return {
            {"schema", schema},
            {"version", result.value("version", 0U)},
            {"artifact_id", normalizeArtifactId(result.value("artifact", json::object()).value("artifact_id", std::string()))},
            {"import_status", result.value("import_status", std::string())},
            {"duplicate_import", result.value("duplicate_import", false)},
            {"imported_case", projectImportedCaseSummary(result.value("imported_case", json::object()))}
        };
    }
    if (schema == tape_engine::kImportedCaseInventoryResultSchema) {
        json importedCases = json::array();
        for (const auto& row : result.value("imported_cases", json::array())) {
            importedCases.push_back(projectImportedCaseSummary(row));
        }
        return {
            {"schema", schema},
            {"version", result.value("version", 0U)},
            {"returned_count", result.value("returned_count", 0U)},
            {"imported_cases", std::move(importedCases)}
        };
    }
    return result;
}

json projectTransportResult(const tape_engine::QueryResponse& response) {
    return {
        {"operation", response.operation},
        {"status", response.status},
        {"result", projectTransportResultBody(response)}
    };
}

BridgeOutboxRecord makeBridgeRecord(std::uint64_t sourceSeq,
                                    const std::string& recordType,
                                    const std::string& source,
                                    const std::string& symbol,
                                    const std::string& side,
                                    std::uint64_t traceId,
                                    OrderId orderId,
                                    long long permId,
                                    const std::string& execId,
                                    const std::string& note,
                                    const std::string& wallTime,
                                    const std::string& fallbackState = "queued_for_recovery",
                                    const std::string& fallbackReason = "engine_unavailable") {
    BridgeOutboxRecord record;
    record.sourceSeq = sourceSeq;
    record.recordType = recordType;
    record.source = source;
    record.symbol = symbol;
    record.side = side;
    record.anchor.traceId = traceId;
    record.anchor.orderId = orderId;
    record.anchor.permId = permId;
    record.anchor.execId = execId;
    record.fallbackState = fallbackState;
    record.fallbackReason = fallbackReason;
    record.note = note;
    record.wallTime = wallTime;
    return record;
}

std::vector<std::uint8_t> readAllFromFd(int fd) {
    std::vector<std::uint8_t> bytes;
    std::array<std::uint8_t, 4096> buffer{};
    while (true) {
        const ssize_t readCount = ::read(fd, buffer.data(), buffer.size());
        if (readCount == 0) {
            break;
        }
        if (readCount < 0) {
            throw std::runtime_error("read failed: " + std::string(std::strerror(errno)));
        }
        bytes.insert(bytes.end(), buffer.begin(), buffer.begin() + readCount);
    }
    return bytes;
}

void writeAllToFd(int fd, const std::vector<std::uint8_t>& bytes) {
    std::size_t offset = 0;
    while (offset < bytes.size()) {
        const ssize_t wrote = ::write(fd, bytes.data() + offset, bytes.size() - offset);
        if (wrote < 0) {
            throw std::runtime_error("write failed: " + std::string(std::strerror(errno)));
        }
        offset += static_cast<std::size_t>(wrote);
    }
}
