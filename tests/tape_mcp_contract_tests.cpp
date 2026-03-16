#include "tape_mcp_adapter.h"

#include "app_shared.h"
#include "bridge_batch_codec.h"
#include "bridge_batch_transport.h"
#include "tape_engine.h"
#include "tape_engine_client.h"
#include "tape_engine_protocol.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <poll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>

namespace fs = std::filesystem;
using json = nlohmann::json;

namespace {

#ifndef TAPE_MCP_EXECUTABLE_PATH
#define TAPE_MCP_EXECUTABLE_PATH "tape_mcp"
#endif

void expect(bool condition, const std::string& message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

template <typename Fn>
void waitUntil(Fn&& predicate,
               const std::string& message,
               std::chrono::milliseconds timeout = std::chrono::milliseconds(3000),
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
        char pattern[] = "/tmp/tape_mcp_tests.XXXXXX";
        char* created = ::mkdtemp(pattern);
        if (created == nullptr) {
            throw std::runtime_error("mkdtemp failed");
        }
        return fs::path(created);
    }();
    return path;
}

fs::path fixturePath(const std::string& relativePath) {
    return fs::path(TWS_GUI_SOURCE_DIR) / "tests" / "fixtures" / relativePath;
}

std::string readTextFile(const fs::path& path) {
    std::ifstream in(path, std::ios::binary);
    expect(in.is_open(), "failed to open file at " + path.string());
    return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

json readJsonFixture(const std::string& relativePath) {
    return json::parse(readTextFile(fixturePath(relativePath)));
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
                                    const std::string& instrumentId = "ib:conid:9301:STK:SMART:USD:INTC",
                                    const std::string& fallbackState = "queued_for_recovery",
                                    const std::string& fallbackReason = "engine_unavailable") {
    BridgeOutboxRecord record;
    record.sourceSeq = sourceSeq;
    record.recordType = recordType;
    record.source = source;
    record.symbol = symbol;
    record.instrumentId = instrumentId;
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

std::string normalizeArtifactId(std::string artifactId) {
    const auto replaceSuffix = [&](const std::string& prefix, const std::string& replacement) {
        if (artifactId.rfind(prefix, 0) == 0) {
            artifactId = prefix + replacement;
        }
    };
    replaceSuffix("session-report:", "<id>");
    replaceSuffix("case-report:", "<id>");
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

std::string normalizeResourceUri(std::string uri) {
    const auto replaceTrailingId = [&](const std::string& prefix, const std::string& suffix = std::string()) {
        if (uri.rfind(prefix, 0) == 0) {
            uri = prefix + "<id>" + suffix;
            return true;
        }
        return false;
    };
    if (replaceTrailingId("tape://report/session/")) {
        return uri;
    }
    if (replaceTrailingId("tape://report/case/")) {
        return uri;
    }
    const std::string sessionArtifactPrefix = "tape://artifact/session-report/";
    if (uri.rfind(sessionArtifactPrefix, 0) == 0) {
        const std::size_t formatPos = uri.find('/', sessionArtifactPrefix.size());
        if (formatPos != std::string::npos) {
            uri = sessionArtifactPrefix + "<id>" + uri.substr(formatPos);
        }
        return uri;
    }
    const std::string caseArtifactPrefix = "tape://artifact/case-report/";
    if (uri.rfind(caseArtifactPrefix, 0) == 0) {
        const std::size_t formatPos = uri.find('/', caseArtifactPrefix.size());
        if (formatPos != std::string::npos) {
            uri = caseArtifactPrefix + "<id>" + uri.substr(formatPos);
        }
        return uri;
    }
    return uri;
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

json projectArtifactSummary(const json& artifact) {
    if (!artifact.is_object()) {
        return json::object();
    }
    return {
        {"artifact_id", normalizeArtifactId(artifact.value("artifact_id", std::string()))},
        {"artifact_type", artifact.value("artifact_type", std::string())},
        {"artifact_scope", artifact.value("artifact_scope", std::string())},
        {"schema_version", artifact.value("schema_version", 0U)}
    };
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
    return projected;
}

json projectEvidenceSummary(const json& evidence) {
    if (!evidence.is_object()) {
        return json::object();
    }
    json citations = json::array();
    for (const auto& citation : evidence.value("citations", json::array())) {
        citations.push_back({
            {"artifact_id", normalizeArtifactId(citation.value("artifact_id", std::string()))},
            {"kind", citation.value("kind", citation.value("type", std::string()))}
        });
    }
    return {
        {"schema_version", evidence.value("schema_version", 0U)},
        {"has_data_quality", evidence.contains("data_quality")},
        {"has_timeline", evidence.contains("timeline")},
        {"has_timeline_summary", evidence.contains("timeline_summary")},
        {"citations", std::move(citations)}
    };
}

json projectRevision(const json& revision) {
    if (!revision.is_object()) {
        return json::object();
    }
    return {
        {"served_revision_id", revision.contains("served_revision_id") ? revision.at("served_revision_id") : json(nullptr)},
        {"latest_session_seq", revision.contains("latest_session_seq") ? revision.at("latest_session_seq") : json(nullptr)},
        {"first_session_seq", revision.contains("first_session_seq") ? revision.at("first_session_seq") : json(nullptr)},
        {"last_session_seq", revision.contains("last_session_seq") ? revision.at("last_session_seq") : json(nullptr)},
        {"manifest_hash", normalizeHash(revision.contains("manifest_hash") ? revision.at("manifest_hash") : json(nullptr))},
        {"includes_mutable_tail", revision.value("includes_mutable_tail", false)},
        {"source", revision.value("source", std::string())}
    };
}

json envelopeFromToolResult(const json& toolResult) {
    expect(toolResult.is_object(), "MCP tool result should be an object");
    const json envelope = toolResult.value("structuredContent", json::object());
    expect(envelope.is_object(), "MCP tool result should include structuredContent");
    return envelope;
}

json projectToolList(const json& listResult) {
    json projection = json::array();
    json tools = listResult.value("tools", json::array());
    std::sort(tools.begin(), tools.end(), [](const json& left, const json& right) {
        return left.value("name", std::string()) < right.value("name", std::string());
    });
    for (const auto& tool : tools) {
        projection.push_back({
            {"name", tool.value("name", std::string())},
            {"input_required", tool.value("inputSchema", json::object()).value("required", json::array())},
            {"output_required", tool.value("outputSchema", json::object()).value("required", json::array())}
        });
    }
    return projection;
}

json projectPromptList(const json& listResult) {
    json projection = json::array();
    json prompts = listResult.value("prompts", json::array());
    std::sort(prompts.begin(), prompts.end(), [](const json& left, const json& right) {
        return left.value("name", std::string()) < right.value("name", std::string());
    });
    for (const auto& prompt : prompts) {
        json required = json::array();
        for (const auto& argument : prompt.value("arguments", json::array())) {
            if (argument.value("required", false)) {
                required.push_back(argument.value("name", std::string()));
            }
        }
        projection.push_back({
            {"name", prompt.value("name", std::string())},
            {"title", prompt.value("title", std::string())},
            {"required_args", std::move(required)}
        });
    }
    return projection;
}

json projectPromptGet(const json& promptResult) {
    const json messages = promptResult.value("messages", json::array());
    json projection{
        {"description", promptResult.value("description", std::string())},
        {"prompt", promptResult.value("meta", json::object()).value("prompt", std::string())},
        {"message_count", messages.size()}
    };
    if (messages.is_array() && !messages.empty()) {
        projection["first_role"] = messages.front().value("role", std::string());
        projection["text"] = messages.front().value("content", json::object()).value("text", std::string());
    }
    return projection;
}

json projectInvestigationResult(const json& result);

json projectResourceList(const json& listResult) {
    json projection = json::array();
    json resources = listResult.value("resources", json::array());
    std::sort(resources.begin(), resources.end(), [](const json& left, const json& right) {
        return left.value("uri", std::string()) < right.value("uri", std::string());
    });
    for (const auto& resource : resources) {
        projection.push_back({
            {"uri", normalizeResourceUri(resource.value("uri", std::string()))},
            {"mime_type", resource.value("mimeType", std::string())}
        });
    }
    return projection;
}

json projectResourceRead(const json& readResult) {
    json projection{
        {"ok", readResult.value("meta", json::object()).value("ok", false)}
    };
    const json meta = readResult.value("meta", json::object());
    if (!projection.value("ok", false)) {
        projection["code"] = meta.value("code", std::string());
        return projection;
    }

    const json contents = readResult.value("contents", json::array());
    projection["content_count"] = contents.size();
    if (!contents.is_array() || contents.empty()) {
        return projection;
    }

    const json& content = contents.front();
    const std::string mimeType = content.value("mimeType", std::string());
    projection["uri"] = normalizeResourceUri(content.value("uri", std::string()));
    projection["mime_type"] = mimeType;
    const std::string text = content.value("text", std::string());
    if (mimeType == "application/json") {
        const json parsed = json::parse(text);
        if (parsed.contains("artifact")) {
            projection["payload"] = projectInvestigationResult(parsed);
        } else if (parsed.contains("summary")) {
            projection["payload"] = {
                {"artifact", projectArtifactSummary(parsed.value("summary", json::object()).value("artifact", json::object()))},
                {"entity", projectEntitySummary(parsed.value("summary", json::object()).value("entity", json::object()))},
                {"report", projectReportSummary(parsed.value("summary", json::object()).value("report", json::object()))}
            };
        } else {
            projection["payload"] = parsed;
        }
    } else {
        projection["has_text"] = !text.empty();
        projection["contains_order_case"] = text.find("Order case for order 7401") != std::string::npos;
        projection["contains_session_overview"] = text.find("Session overview") != std::string::npos;
    }
    return projection;
}

json projectReplayRangeSummary(const json& replayRange) {
    if (!replayRange.is_object()) {
        return json(nullptr);
    }

    json keys = json::array();
    for (auto it = replayRange.begin(); it != replayRange.end(); ++it) {
        keys.push_back(it.key());
    }
    std::sort(keys.begin(), keys.end());

    json projection{
        {"keys", std::move(keys)},
        {"has_last_session_seq",
         replayRange.contains("last_session_seq") && !replayRange.at("last_session_seq").is_null()}
    };
    if (replayRange.contains("first_session_seq") && !replayRange.at("first_session_seq").is_null()) {
        projection["first_session_seq"] = replayRange.at("first_session_seq");
    }
    if (replayRange.contains("target_session_seq") && !replayRange.at("target_session_seq").is_null()) {
        projection["target_session_seq"] = replayRange.at("target_session_seq");
    }
    return projection;
}

json projectInvestigationResult(const json& result) {
    json eventKinds = json::array();
    for (const auto& event : result.value("events", json::array())) {
        eventKinds.push_back(event.value("event_kind", std::string()));
    }

    json incidentKinds = json::array();
    for (const auto& row : result.value("incident_rows", json::array())) {
        incidentKinds.push_back(row.value("kind", std::string()));
    }

    json citations = json::array();
    for (const auto& row : result.value("citation_rows", json::array())) {
        citations.push_back({
            {"artifact_id", normalizeArtifactId(row.value("artifact_id", std::string()))},
            {"kind", row.value("kind", std::string())}
        });
    }

    json projection{
        {"artifact_id", normalizeArtifactId(result.value("artifact_id", std::string()))},
        {"artifact_kind", result.value("artifact_kind", std::string())},
        {"headline", result.value("headline", std::string())},
        {"detail", result.value("detail", std::string())},
        {"served_revision_id", result.contains("served_revision_id") ? result.at("served_revision_id") : json(nullptr)},
        {"includes_mutable_tail", result.value("includes_mutable_tail", false)},
        {"artifact", projectArtifactSummary(result.value("artifact", json::object()))},
        {"entity", projectEntitySummary(result.value("entity", json::object()))},
        {"report", projectReportSummary(result.value("report", json::object()))},
        {"evidence", projectEvidenceSummary(result.value("evidence", json::object()))},
        {"has_data_quality", result.value("data_quality", json::object()).is_object() &&
                                 !result.value("data_quality", json::object()).empty()},
        {"replay_range", projectReplayRangeSummary(result.value("replay_range", json(nullptr)))},
        {"incident_kinds", std::move(incidentKinds)},
        {"citation_rows", std::move(citations)},
        {"event_kinds", std::move(eventKinds)}
    };
    return projection;
}

json projectQualityResult(const json& result) {
    const json dataQuality = result.value("data_quality", json::object());
    json keys = json::array();
    if (dataQuality.is_object()) {
        for (auto it = dataQuality.begin(); it != dataQuality.end(); ++it) {
            keys.push_back(it.key());
        }
        std::sort(keys.begin(), keys.end());
    }
    return {
        {"served_revision_id", result.contains("served_revision_id") ? result.at("served_revision_id") : json(nullptr)},
        {"includes_mutable_tail", result.value("includes_mutable_tail", false)},
        {"first_session_seq", result.contains("first_session_seq") ? result.at("first_session_seq") : json(nullptr)},
        {"last_session_seq", result.contains("last_session_seq") ? result.at("last_session_seq") : json(nullptr)},
        {"data_quality_keys", std::move(keys)}
    };
}

json projectEventListResult(const json& result) {
    json eventKinds = json::array();
    for (const auto& event : result.value("events", json::array())) {
        eventKinds.push_back(event.value("event_kind", std::string()));
    }
    json projection{
        {"returned_count", result.value("returned_count", 0ULL)},
        {"event_kinds", std::move(eventKinds)}
    };
    const json events = result.value("events", json::array());
    if (events.is_array() && !events.empty()) {
        projection["first_session_seq"] = events.front().value("session_seq", 0ULL);
        projection["last_session_seq"] = events.back().value("session_seq", 0ULL);
    } else {
        projection["first_session_seq"] = nullptr;
        projection["last_session_seq"] = nullptr;
    }
    return projection;
}

json projectIncidentListResult(const json& result) {
    json incidentKinds = json::array();
    for (const auto& incident : result.value("incidents", json::array())) {
        incidentKinds.push_back(incident.value("kind", std::string()));
    }
    std::sort(incidentKinds.begin(), incidentKinds.end());
    return {
        {"returned_count", result.value("returned_count", 0ULL)},
        {"incident_kinds", std::move(incidentKinds)}
    };
}

json projectOrderAnchorListResult(const json& result) {
    json rows = json::array();
    for (const auto& row : result.value("order_anchors", json::array())) {
        rows.push_back({
            {"artifact_id", normalizeArtifactId(row.value("artifact_id", std::string()))},
            {"anchor_id", normalizePositiveId(row.value("anchor_id", 0ULL))},
            {"revision_id", normalizePositiveId(row.value("revision_id", 0ULL))},
            {"session_seq", row.value("session_seq", 0ULL)},
            {"event_kind", row.value("event_kind", std::string())},
            {"instrument_id", row.value("instrument_id", std::string())}
        });
    }
    std::sort(rows.begin(), rows.end(), [](const json& left, const json& right) {
        return left.value("session_seq", 0ULL) < right.value("session_seq", 0ULL);
    });
    return {
        {"returned_count", result.value("returned_count", 0ULL)},
        {"order_anchors", std::move(rows)}
    };
}

json projectProtectedWindowListResult(const json& result) {
    std::set<std::string> reasons;
    std::uint64_t minFirstSessionSeq = std::numeric_limits<std::uint64_t>::max();
    std::uint64_t maxLastSessionSeq = 0;
    for (const auto& row : result.value("protected_windows", json::array())) {
        reasons.insert(row.value("reason", std::string()));
        const std::uint64_t firstSessionSeq = row.value("first_session_seq", 0ULL);
        const std::uint64_t lastSessionSeq = row.value("last_session_seq", 0ULL);
        if (firstSessionSeq > 0) {
            minFirstSessionSeq = std::min(minFirstSessionSeq, firstSessionSeq);
        }
        maxLastSessionSeq = std::max(maxLastSessionSeq, lastSessionSeq);
    }
    json normalizedReasons = json::array();
    for (const auto& reason : reasons) {
        normalizedReasons.push_back(reason);
    }
    return {
        {"returned_count", result.value("returned_count", 0ULL)},
        {"reasons", std::move(normalizedReasons)},
        {"min_first_session_seq", minFirstSessionSeq == std::numeric_limits<std::uint64_t>::max() ? 0ULL : minFirstSessionSeq},
        {"max_last_session_seq", maxLastSessionSeq}
    };
}

json projectFindingListResult(const json& result) {
    json rows = json::array();
    for (const auto& row : result.value("findings", json::array())) {
        rows.push_back({
            {"artifact_id", normalizeArtifactId(row.value("artifact_id", std::string()))},
            {"finding_id", normalizePositiveId(row.value("finding_id", 0ULL))},
            {"revision_id", normalizePositiveId(row.value("revision_id", 0ULL))},
            {"logical_incident_id", normalizePositiveId(row.value("logical_incident_id", 0ULL))},
            {"incident_revision_id", normalizePositiveId(row.value("incident_revision_id", 0ULL))},
            {"kind", row.value("kind", std::string())},
            {"severity", row.value("severity", std::string())},
            {"confidence", row.value("confidence", 0.0)},
            {"first_session_seq", row.value("first_session_seq", 0ULL)},
            {"last_session_seq", row.value("last_session_seq", 0ULL)},
            {"title", row.value("title", std::string())}
        });
    }
    std::sort(rows.begin(), rows.end(), [](const json& left, const json& right) {
        if (left.value("first_session_seq", 0ULL) != right.value("first_session_seq", 0ULL)) {
            return left.value("first_session_seq", 0ULL) < right.value("first_session_seq", 0ULL);
        }
        if (left.value("last_session_seq", 0ULL) != right.value("last_session_seq", 0ULL)) {
            return left.value("last_session_seq", 0ULL) < right.value("last_session_seq", 0ULL);
        }
        if (left.value("kind", std::string()) != right.value("kind", std::string())) {
            return left.value("kind", std::string()) < right.value("kind", std::string());
        }
        return left.value("artifact_id", std::string()) < right.value("artifact_id", std::string());
    });
    return {
        {"returned_count", result.value("returned_count", 0ULL)},
        {"findings", std::move(rows)}
    };
}

json projectReplaySnapshotResult(const json& result) {
    return {
        {"served_revision_id", result.contains("served_revision_id") ? result.at("served_revision_id") : json(nullptr)},
        {"includes_mutable_tail", result.value("includes_mutable_tail", false)},
        {"target_session_seq", result.value("target_session_seq", 0ULL)},
        {"replayed_through_session_seq", result.value("replayed_through_session_seq", 0ULL)},
        {"applied_event_count", result.value("applied_event_count", 0ULL)},
        {"gap_markers_encountered", result.value("gap_markers_encountered", 0ULL)},
        {"checkpoint_used", result.value("checkpoint_used", false)},
        {"checkpoint_revision_present", result.value("checkpoint_revision_id", 0ULL) > 0ULL},
        {"checkpoint_session_seq", result.value("checkpoint_session_seq", 0ULL)},
        {"bid_price", result.value("bid_price", json(nullptr))},
        {"ask_price", result.value("ask_price", json(nullptr))},
        {"last_price", result.value("last_price", json(nullptr))},
        {"bid_book_depth", result.value("bid_book", json::array()).size()},
        {"ask_book_depth", result.value("ask_book", json::array()).size()},
        {"data_quality_keys", [&]() {
            json keys = json::array();
            const json dataQuality = result.value("data_quality", json::object());
            if (dataQuality.is_object()) {
                for (auto it = dataQuality.begin(); it != dataQuality.end(); ++it) {
                    keys.push_back(it.key());
                }
                std::sort(keys.begin(), keys.end());
            }
            return keys;
        }()}
    };
}

json projectExportResult(const json& result) {
    json projection{
        {"artifact_id", normalizeArtifactId(result.value("artifact_id", std::string()))},
        {"format", result.value("format", std::string())},
        {"served_revision_id", result.contains("served_revision_id") ? result.at("served_revision_id") : json(nullptr)}
    };
    if (result.value("format", std::string()) == "markdown") {
        projection["has_markdown"] = result.contains("markdown") && result.at("markdown").is_string() &&
            !result.at("markdown").get<std::string>().empty();
    } else {
        const json bundle = result.value("bundle", json::object());
        projection["bundle"] = {
            {"artifact", projectArtifactSummary(bundle.value("summary", json::object()).value("artifact", json::object()))},
            {"entity", projectEntitySummary(bundle.value("summary", json::object()).value("entity", json::object()))},
            {"report", projectReportSummary(bundle.value("summary", json::object()).value("report", json::object()))}
        };
    }
    return projection;
}

json projectReportInventoryResult(const json& result, const char* rowKey) {
    json rows = json::array();
    for (const auto& row : result.value(rowKey, json::array())) {
        rows.push_back({
            {"report_id", normalizePositiveId(row.value("report_id", 0ULL))},
            {"revision_id", row.value("revision_id", 0ULL)},
            {"artifact_id", normalizeArtifactId(row.value("artifact_id", std::string()))},
            {"report_type", row.value("report_type", std::string())},
            {"headline", row.value("headline", std::string())}
        });
    }
    std::sort(rows.begin(), rows.end(), [](const json& left, const json& right) {
        if (left.value("report_type", std::string()) != right.value("report_type", std::string())) {
            return left.value("report_type", std::string()) < right.value("report_type", std::string());
        }
        return left.value("headline", std::string()) < right.value("headline", std::string());
    });
    return {
        {"returned_count", result.value("returned_count", 0ULL)},
        {rowKey, std::move(rows)}
    };
}

json projectSeekResult(const json& result) {
    return {
        {"served_revision_id", result.contains("served_revision_id") ? result.at("served_revision_id") : json(nullptr)},
        {"includes_mutable_tail", result.value("includes_mutable_tail", false)},
        {"replay_target_session_seq", result.value("replay_target_session_seq", 0ULL)},
        {"first_session_seq", result.value("first_session_seq", 0ULL)},
        {"last_session_seq", result.value("last_session_seq", 0ULL)},
        {"last_fill_session_seq", result.value("last_fill_session_seq", 0ULL)},
        {"replay_range", projectReplayRangeSummary(result.value("replay_range", json(nullptr)))}
    };
}

json projectEnvelope(const json& envelope) {
    const json meta = envelope.value("meta", json::object());
    const std::string toolName = meta.value("tool", std::string());
    json projection{
        {"ok", envelope.value("ok", false)},
        {"meta", {
            {"tool", toolName},
            {"engine_operation", meta.value("engine_operation", std::string())},
            {"result_schema", meta.value("result_schema", std::string())},
            {"supported", meta.value("supported", false)},
            {"deferred", meta.value("deferred", false)},
            {"retryable", meta.value("retryable", false)}
        }},
        {"revision", projectRevision(envelope.value("revision", json::object()))}
    };
    if (!envelope.value("ok", false)) {
        projection["error"] = envelope.value("error", json::object());
        return projection;
    }

    const json result = envelope.value("result", json::object());
    if (toolName == "tapescript_status") {
        projection["result"] = {
            {"socket_path", normalizeTestPath(result.value("socket_path", json(nullptr)))},
            {"data_dir", normalizeTestPath(result.value("data_dir", json(nullptr)))},
            {"instrument_id", result.value("instrument_id", std::string())},
            {"latest_session_seq", result.value("latest_session_seq", 0ULL)},
            {"live_event_count", result.value("live_event_count", 0ULL)},
            {"segment_count", result.value("segment_count", 0ULL)},
            {"manifest_hash", normalizeHash(result.value("manifest_hash", json(nullptr)))}
        };
    } else if (toolName == "tapescript_read_live_tail" ||
               toolName == "tapescript_read_range" ||
               toolName == "tapescript_find_order_anchor") {
        projection["result"] = projectEventListResult(result);
    } else if (toolName == "tapescript_list_incidents") {
        projection["result"] = projectIncidentListResult(result);
    } else if (toolName == "tapescript_list_order_anchors") {
        projection["result"] = projectOrderAnchorListResult(result);
    } else if (toolName == "tapescript_list_protected_windows") {
        projection["result"] = projectProtectedWindowListResult(result);
    } else if (toolName == "tapescript_list_findings") {
        projection["result"] = projectFindingListResult(result);
    } else if (toolName == "tapescript_read_session_quality") {
        projection["result"] = projectQualityResult(result);
    } else if (toolName == "tapescript_replay_snapshot") {
        projection["result"] = projectReplaySnapshotResult(result);
    } else if (toolName == "tapescript_list_session_reports") {
        projection["result"] = projectReportInventoryResult(result, "session_reports");
    } else if (toolName == "tapescript_list_case_reports") {
        projection["result"] = projectReportInventoryResult(result, "case_reports");
    } else if (toolName == "tapescript_seek_order_anchor") {
        projection["result"] = projectSeekResult(result);
    } else if (toolName == "tapescript_export_artifact") {
        projection["result"] = projectExportResult(result);
    } else {
        projection["result"] = projectInvestigationResult(result);
    }
    return projection;
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

std::string readLineWithTimeout(int fd, int timeoutMs) {
    std::string line;
    while (true) {
        pollfd descriptor{fd, POLLIN, 0};
        const int pollResult = ::poll(&descriptor, 1, timeoutMs);
        if (pollResult < 0) {
            throw std::runtime_error("poll failed: " + std::string(std::strerror(errno)));
        }
        if (pollResult == 0) {
            throw std::runtime_error("timed out while reading MCP response");
        }
        char ch = '\0';
        const ssize_t readCount = ::read(fd, &ch, 1);
        if (readCount == 0) {
            throw std::runtime_error("unexpected EOF while reading MCP response line");
        }
        if (readCount < 0) {
            throw std::runtime_error("read failed: " + std::string(std::strerror(errno)));
        }
        line.push_back(ch);
        if (ch == '\n') {
            break;
        }
    }
    return line;
}

std::vector<std::uint8_t> readExactWithTimeout(int fd, std::size_t bytes, int timeoutMs) {
    std::vector<std::uint8_t> data(bytes);
    std::size_t offset = 0;
    while (offset < bytes) {
        pollfd descriptor{fd, POLLIN, 0};
        const int pollResult = ::poll(&descriptor, 1, timeoutMs);
        if (pollResult < 0) {
            throw std::runtime_error("poll failed: " + std::string(std::strerror(errno)));
        }
        if (pollResult == 0) {
            throw std::runtime_error("timed out while reading MCP response body");
        }
        const ssize_t readCount = ::read(fd, data.data() + offset, bytes - offset);
        if (readCount == 0) {
            throw std::runtime_error("unexpected EOF while reading MCP response body");
        }
        if (readCount < 0) {
            throw std::runtime_error("read failed: " + std::string(std::strerror(errno)));
        }
        offset += static_cast<std::size_t>(readCount);
    }
    return data;
}

void writeJsonRpcMessage(int fd, const json& payload) {
    const std::string body = payload.dump();
    const std::string header = "Content-Length: " + std::to_string(body.size()) + "\r\n\r\n";
    std::vector<std::uint8_t> bytes;
    bytes.reserve(header.size() + body.size());
    bytes.insert(bytes.end(), header.begin(), header.end());
    bytes.insert(bytes.end(), body.begin(), body.end());
    writeAllToFd(fd, bytes);
}

json readJsonRpcMessage(int fd) {
    std::optional<std::size_t> contentLength;
    while (true) {
        std::string line = readLineWithTimeout(fd, 3000);
        if (!line.empty() && line.back() == '\n') {
            line.pop_back();
        }
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        if (line.empty()) {
            break;
        }
        constexpr const char* prefix = "Content-Length:";
        if (line.rfind(prefix, 0) == 0) {
            const std::string value = line.substr(std::strlen(prefix));
            contentLength = static_cast<std::size_t>(std::stoull(value));
        }
    }
    expect(contentLength.has_value(), "MCP response should include Content-Length");
    const auto bodyBytes = readExactWithTimeout(fd, *contentLength, 3000);
    return json::parse(std::string(bodyBytes.begin(), bodyBytes.end()));
}

std::pair<std::vector<json>, json> readProgressNotificationsAndResponse(int fd, int expectedId) {
    std::vector<json> notifications;
    while (true) {
        const json message = readJsonRpcMessage(fd);
        if (message.value("method", std::string()) == "notifications/progress") {
            notifications.push_back(message);
            continue;
        }
        expect(message.value("id", -1) == expectedId,
               "unexpected JSON-RPC response id while waiting for " + std::to_string(expectedId));
        return {std::move(notifications), message};
    }
}

struct ChildProcess {
    pid_t pid = -1;
    int writeFd = -1;
    int readFd = -1;
};

ChildProcess launchTapeMcp(const fs::path& socketPath) {
    int stdinPipe[2] = {-1, -1};
    int stdoutPipe[2] = {-1, -1};

    expect(::pipe(stdinPipe) == 0, "pipe for child stdin should succeed");
    expect(::pipe(stdoutPipe) == 0, "pipe for child stdout should succeed");

    const pid_t pid = ::fork();
    expect(pid >= 0, "fork should succeed");
    if (pid == 0) {
        ::dup2(stdinPipe[0], STDIN_FILENO);
        ::dup2(stdoutPipe[1], STDOUT_FILENO);
        ::close(stdinPipe[0]);
        ::close(stdinPipe[1]);
        ::close(stdoutPipe[0]);
        ::close(stdoutPipe[1]);

        const std::string socketArg = socketPath.string();
        ::execl(TAPE_MCP_EXECUTABLE_PATH,
                TAPE_MCP_EXECUTABLE_PATH,
                "--engine-socket",
                socketArg.c_str(),
                static_cast<char*>(nullptr));
        _exit(127);
    }

    ::close(stdinPipe[0]);
    ::close(stdoutPipe[1]);
    return ChildProcess{pid, stdinPipe[1], stdoutPipe[0]};
}

void stopProcess(const ChildProcess& child) {
    if (child.writeFd >= 0) {
        ::close(child.writeFd);
    }
    if (child.readFd >= 0) {
        ::close(child.readFd);
    }
    int status = 0;
    const pid_t waited = ::waitpid(child.pid, &status, 0);
    expect(waited == child.pid, "waitpid should collect the MCP child process");
    expect(WIFEXITED(status), "MCP child should exit normally");
    expect(WEXITSTATUS(status) == 0, "MCP child should exit with code 0");
}

std::unique_ptr<tape_engine::Server> startPhase5Engine(const fs::path& rootDir, const fs::path& socketPath) {
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "ib:conid:9301:STK:SMART:USD:INTC";
    config.ringCapacity = 32;

    auto server = std::make_unique<tape_engine::Server>(config);
    std::string startError;
    expect(server->start(&startError), "tape-engine should start for MCP tests: " + startError);
    return server;
}

void seedPhase5Engine(const fs::path& socketPath) {
    bridge_batch::BuildOptions options;
    options.appSessionId = "app-tape-mcp-phase5";
    options.runtimeSessionId = "runtime-tape-mcp-phase5";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;

    options.batchSeq = 201;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        makeBridgeRecord(9101, "order_intent", "WebSocket", "INTC", "BUY",
                         401, 7401, 0, "", "tape-mcp order intent", "2026-03-15T09:41:00.100"),
        makeBridgeRecord(9102, "order_status", "BrokerOrderStatus", "INTC", "BUY",
                         401, 7401, 8801, "", "Submitted: filled=0 remaining=1", "2026-03-15T09:41:00.120")
    }, options)), &error), "tape-engine should accept the MCP fixture first batch: " + error);

    options.batchSeq = 202;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        makeBridgeRecord(9104, "fill_execution", "BrokerExecution", "INTC", "BOT",
                         401, 7401, 8801, "EXEC-401", "phase5 fill", "2026-03-15T09:41:00.250")
    }, options)), &error), "tape-engine should accept the MCP fixture second batch: " + error);
}

std::uint64_t queryFirstLogicalIncidentId(const fs::path& socketPath) {
    tape_engine::Client client(socketPath.string());
    tape_engine::QueryResponse response;
    std::string error;
    tape_engine::QueryRequest request = tape_engine::makeQueryRequest(tape_engine::QueryOperation::ListIncidents,
                                                                      "phase5-contract-list-incidents");
    request.limit = 5;
    expect(client.query(request, &response, &error), "phase5 incident query should succeed: " + error);
    expect(response.events.is_array() && !response.events.empty(), "phase5 incident query should return at least one incident");
    return response.events.front().value("logical_incident_id", 0ULL);
}

std::uint64_t queryFirstFindingId(const fs::path& socketPath) {
    tape_engine::Client client(socketPath.string());
    tape_engine::QueryResponse response;
    std::string error;
    tape_engine::QueryRequest request = tape_engine::makeQueryRequest(tape_engine::QueryOperation::ListFindings,
                                                                      "phase5-contract-list-findings");
    request.limit = 5;
    expect(client.query(request, &response, &error), "phase5 finding query should succeed: " + error);
    expect(response.events.is_array() && !response.events.empty(), "phase5 finding query should return at least one finding");
    return response.events.front().value("finding_id", 0ULL);
}

std::uint64_t queryFirstAnchorId(const fs::path& socketPath) {
    tape_engine::Client client(socketPath.string());
    tape_engine::QueryResponse response;
    std::string error;
    tape_engine::QueryRequest request = tape_engine::makeQueryRequest(tape_engine::QueryOperation::ListOrderAnchors,
                                                                      "phase5-contract-list-order-anchors");
    request.limit = 5;
    expect(client.query(request, &response, &error), "phase5 anchor query should succeed: " + error);
    expect(response.events.is_array() && !response.events.empty(), "phase5 anchor query should return at least one anchor");
    return response.events.front().value("anchor_id", 0ULL);
}

std::uint64_t queryFirstWindowId(const fs::path& socketPath) {
    tape_engine::Client client(socketPath.string());
    tape_engine::QueryResponse response;
    std::string error;
    tape_engine::QueryRequest request = tape_engine::makeQueryRequest(tape_engine::QueryOperation::ListProtectedWindows,
                                                                      "phase5-contract-list-protected-windows");
    request.limit = 5;
    expect(client.query(request, &response, &error), "phase5 window query should succeed: " + error);
    expect(response.events.is_array() && !response.events.empty(), "phase5 window query should return at least one window");
    return response.events.front().value("window_id", 0ULL);
}

std::uint64_t parseTrailingNumericId(const std::string& artifactId, const std::string& prefix) {
    expect(artifactId.rfind(prefix, 0) == 0, "artifact id should start with " + prefix);
    return std::strtoull(artifactId.substr(prefix.size()).c_str(), nullptr, 10);
}

void testTapeMcpPhase5Contracts() {
    const fs::path rootDir = testDataDir() / "tape-mcp-phase5-engine";
    const fs::path socketPath = testDataDir() / "tape-mcp-phase5-engine.sock";
    auto server = startPhase5Engine(rootDir, socketPath);
    seedPhase5Engine(socketPath);

    waitUntil([&]() {
        const auto snapshot = server->snapshot();
        return snapshot.segments.size() >= 2 && snapshot.latestFrozenRevisionId >= 2;
    }, "phase5 MCP setup should freeze fixture batches");

    const std::uint64_t logicalIncidentId = queryFirstLogicalIncidentId(socketPath);
    expect(logicalIncidentId > 0, "phase5 MCP setup should expose a logical incident id");
    const std::uint64_t findingId = queryFirstFindingId(socketPath);
    expect(findingId > 0, "phase5 MCP setup should expose a finding id");
    const std::uint64_t anchorId = queryFirstAnchorId(socketPath);
    expect(anchorId > 0, "phase5 MCP setup should expose an anchor id");
    const std::uint64_t windowId = queryFirstWindowId(socketPath);
    expect(windowId > 0, "phase5 MCP setup should expose a protected window id");

    tape_mcp::Adapter adapter(tape_mcp::AdapterConfig{socketPath.string()});

    const json toolsProjection = projectToolList(adapter.listToolsResult());
    const json promptsProjection = projectPromptList(adapter.listPromptsResult());

    const json sessionPromptProjection = projectPromptGet(
        adapter.getPromptResult("investigate_session_range", json{
            {"first_session_seq", 1},
            {"last_session_seq", 4},
            {"revision_id", 7}
        }));

    const json orderPromptProjection = projectPromptGet(
        adapter.getPromptResult("investigate_order_case", json{
            {"order_id", 7401},
            {"revision_id", 7}
        }));

    const json badFillPromptProjection = projectPromptGet(
        adapter.getPromptResult("investigate_bad_fill", json{
            {"exec_id", "EXEC-401"},
            {"revision_id", 7}
        }));

    const json sourceGapPromptProjection = projectPromptGet(
        adapter.getPromptResult("investigate_source_gap", json{
            {"logical_incident_id", 19},
            {"revision_id", 7}
        }));

    const json latestIncidentsPromptProjection = projectPromptGet(
        adapter.getPromptResult("summarize_latest_session_incidents", json{
            {"revision_id", 7}
        }));

    const json statusEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_status", json::object())));

    const json liveTailEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_read_live_tail", json{
            {"limit", 10}
        })));

    const json readRangeEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_read_range", json{
            {"first_session_seq", 1},
            {"last_session_seq", 4}
        })));

    const json findOrderAnchorEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_find_order_anchor", json{
            {"order_id", 7401},
            {"limit", 10}
        })));

    const json listIncidentsEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_list_incidents", json{
            {"limit", 10}
        })));

    const json listOrderAnchorsEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_list_order_anchors", json{
            {"limit", 10}
        })));

    const json listProtectedWindowsEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_list_protected_windows", json{
            {"limit", 10}
        })));

    const json listFindingsEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_list_findings", json{
            {"limit", 10}
        })));

    const json overviewEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_read_session_overview", json{
            {"first_session_seq", 1},
            {"last_session_seq", 4},
            {"limit", 10}
        })));

    const json sessionReportEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_scan_session_report", json{
            {"first_session_seq", 1},
            {"last_session_seq", 4},
            {"limit", 10}
        })));
    const std::string sessionReportArtifactId =
        envelopeFromToolResult(adapter.callTool("tapescript_scan_session_report", json{
            {"first_session_seq", 1},
            {"last_session_seq", 4},
            {"limit", 10}
        })).value("result", json::object()).value("artifact_id", std::string());
    expect(!sessionReportArtifactId.empty(), "phase5 session report scan should expose a durable artifact id");
    const std::uint64_t sessionReportId = parseTrailingNumericId(sessionReportArtifactId, "session-report:");

    const json readSessionReportEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_read_session_report", json{
            {"report_id", sessionReportId}
        })));

    const json listSessionReportsEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_list_session_reports", json{
            {"limit", 10}
        })));

    const json orderCaseEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_read_order_case", json{
            {"order_id", 7401},
            {"limit", 10}
        })));

    const json scanIncidentReportEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_scan_incident_report", json{
            {"logical_incident_id", logicalIncidentId},
            {"limit", 10}
        })));
    const std::string incidentReportArtifactId =
        envelopeFromToolResult(adapter.callTool("tapescript_scan_incident_report", json{
            {"logical_incident_id", logicalIncidentId},
            {"limit", 10}
        })).value("result", json::object()).value("artifact_id", std::string());
    expect(!incidentReportArtifactId.empty(), "phase5 incident report scan should expose a durable artifact id");

    const json scanOrderCaseReportEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_scan_order_case_report", json{
            {"order_id", 7401},
            {"limit", 10}
        })));
    const std::string orderCaseReportArtifactId =
        envelopeFromToolResult(adapter.callTool("tapescript_scan_order_case_report", json{
            {"order_id", 7401},
            {"limit", 10}
        })).value("result", json::object()).value("artifact_id", std::string());
    expect(!orderCaseReportArtifactId.empty(), "phase5 order-case report scan should expose a durable artifact id");
    const std::uint64_t caseReportId = parseTrailingNumericId(orderCaseReportArtifactId, "case-report:");

    const json resourceListProjection = projectResourceList(adapter.listResourcesResult());
    const json sessionResourceProjection = projectResourceRead(adapter.readResourceResult(
        "tape://report/session/" + std::to_string(sessionReportId)));
    const json sessionArtifactBundleProjection = projectResourceRead(adapter.readResourceResult(
        "tape://artifact/session-report/" + std::to_string(sessionReportId) + "/json-bundle"));
    const json caseArtifactMarkdownProjection = projectResourceRead(adapter.readResourceResult(
        "tape://artifact/case-report/" + std::to_string(caseReportId) + "/markdown"));

    const json readCaseReportEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_read_case_report", json{
            {"report_id", caseReportId}
        })));

    const json listCaseReportsEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_list_case_reports", json{
            {"limit", 10}
        })));

    const json seekOrderEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_seek_order_anchor", json{
            {"order_id", 7401}
        })));

    const json findingEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_read_finding", json{
            {"finding_id", findingId},
            {"limit", 10}
        })));

    const json incidentEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_read_incident", json{
            {"logical_incident_id", logicalIncidentId},
            {"limit", 10}
        })));

    const json orderAnchorEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_read_order_anchor", json{
            {"anchor_id", anchorId},
            {"limit", 10}
        })));

    const json protectedWindowEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_read_protected_window", json{
            {"window_id", windowId},
            {"limit", 10}
        })));

    const json replaySnapshotEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_replay_snapshot", json{
            {"target_session_seq", 4},
            {"depth_limit", 5}
        })));

    const json artifactEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_read_artifact", json{
            {"artifact_id", sessionReportArtifactId}
        })));

    const json exportEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_export_artifact", json{
            {"artifact_id", sessionReportArtifactId},
            {"export_format", "json-bundle"}
        })));

    const json qualityEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_read_session_quality", json{
            {"first_session_seq", 1},
            {"last_session_seq", 4}
        })));

    const json fixture = readJsonFixture("phase5_mcp_contracts.json");
    expect(toolsProjection == fixture.value("tools_list", json::array()),
           "phase5 tools/list projection should match golden fixture\nactual:\n" + toolsProjection.dump(2));
    expect(promptsProjection == fixture.value("prompts_list", json::array()),
           "phase5 prompts/list projection should match golden fixture\nactual:\n" + promptsProjection.dump(2));
    expect(sessionPromptProjection == fixture.value("prompt_investigate_session_range", json::object()),
           "phase5 investigate-session-range prompt should match golden fixture\nactual:\n" + sessionPromptProjection.dump(2));
    expect(orderPromptProjection == fixture.value("prompt_investigate_order_case", json::object()),
           "phase5 investigate-order-case prompt should match golden fixture\nactual:\n" + orderPromptProjection.dump(2));
    expect(badFillPromptProjection == fixture.value("prompt_investigate_bad_fill", json::object()),
           "phase5 investigate-bad-fill prompt should match golden fixture\nactual:\n" + badFillPromptProjection.dump(2));
    expect(sourceGapPromptProjection == fixture.value("prompt_investigate_source_gap", json::object()),
           "phase5 investigate-source-gap prompt should match golden fixture\nactual:\n" + sourceGapPromptProjection.dump(2));
    expect(latestIncidentsPromptProjection == fixture.value("prompt_summarize_latest_session_incidents", json::object()),
           "phase5 summarize-latest-session-incidents prompt should match golden fixture\nactual:\n" + latestIncidentsPromptProjection.dump(2));
    expect(statusEnvelope == fixture.value("tapescript_status", json::object()),
           "phase5 status envelope should match golden fixture\nactual:\n" + statusEnvelope.dump(2));
    expect(liveTailEnvelope == fixture.value("tapescript_read_live_tail", json::object()),
           "phase5 live-tail envelope should match golden fixture\nactual:\n" + liveTailEnvelope.dump(2));
    expect(readRangeEnvelope == fixture.value("tapescript_read_range", json::object()),
           "phase5 read-range envelope should match golden fixture\nactual:\n" + readRangeEnvelope.dump(2));
    expect(findOrderAnchorEnvelope == fixture.value("tapescript_find_order_anchor", json::object()),
           "phase5 find-order-anchor envelope should match golden fixture\nactual:\n" + findOrderAnchorEnvelope.dump(2));
    expect(listIncidentsEnvelope == fixture.value("tapescript_list_incidents", json::object()),
           "phase5 list-incidents envelope should match golden fixture\nactual:\n" + listIncidentsEnvelope.dump(2));
    expect(listOrderAnchorsEnvelope == fixture.value("tapescript_list_order_anchors", json::object()),
           "phase5 list-order-anchors envelope should match golden fixture\nactual:\n" + listOrderAnchorsEnvelope.dump(2));
    expect(listProtectedWindowsEnvelope == fixture.value("tapescript_list_protected_windows", json::object()),
           "phase5 list-protected-windows envelope should match golden fixture\nactual:\n" + listProtectedWindowsEnvelope.dump(2));
    expect(listFindingsEnvelope == fixture.value("tapescript_list_findings", json::object()),
           "phase5 list-findings envelope should match golden fixture\nactual:\n" + listFindingsEnvelope.dump(2));
    expect(overviewEnvelope == fixture.value("tapescript_read_session_overview", json::object()),
           "phase5 session overview envelope should match golden fixture\nactual:\n" + overviewEnvelope.dump(2));
    expect(sessionReportEnvelope == fixture.value("tapescript_scan_session_report", json::object()),
           "phase5 session report envelope should match golden fixture\nactual:\n" + sessionReportEnvelope.dump(2));
    expect(readSessionReportEnvelope == fixture.value("tapescript_read_session_report", json::object()),
           "phase5 read-session-report envelope should match golden fixture\nactual:\n" + readSessionReportEnvelope.dump(2));
    expect(listSessionReportsEnvelope == fixture.value("tapescript_list_session_reports", json::object()),
           "phase5 list-session-reports envelope should match golden fixture\nactual:\n" + listSessionReportsEnvelope.dump(2));
    expect(scanIncidentReportEnvelope == fixture.value("tapescript_scan_incident_report", json::object()),
           "phase5 scan-incident-report envelope should match golden fixture\nactual:\n" + scanIncidentReportEnvelope.dump(2));
    expect(scanOrderCaseReportEnvelope == fixture.value("tapescript_scan_order_case_report", json::object()),
           "phase5 scan-order-case-report envelope should match golden fixture\nactual:\n" + scanOrderCaseReportEnvelope.dump(2));
    expect(readCaseReportEnvelope == fixture.value("tapescript_read_case_report", json::object()),
           "phase5 read-case-report envelope should match golden fixture\nactual:\n" + readCaseReportEnvelope.dump(2));
    expect(listCaseReportsEnvelope == fixture.value("tapescript_list_case_reports", json::object()),
           "phase5 list-case-reports envelope should match golden fixture\nactual:\n" + listCaseReportsEnvelope.dump(2));
    expect(seekOrderEnvelope == fixture.value("tapescript_seek_order_anchor", json::object()),
           "phase5 seek-order envelope should match golden fixture\nactual:\n" + seekOrderEnvelope.dump(2));
    expect(findingEnvelope == fixture.value("tapescript_read_finding", json::object()),
           "phase5 read-finding envelope should match golden fixture\nactual:\n" + findingEnvelope.dump(2));
    expect(orderCaseEnvelope == fixture.value("tapescript_read_order_case", json::object()),
           "phase5 order-case envelope should match golden fixture\nactual:\n" + orderCaseEnvelope.dump(2));
    expect(incidentEnvelope == fixture.value("tapescript_read_incident", json::object()),
           "phase5 incident envelope should match golden fixture\nactual:\n" + incidentEnvelope.dump(2));
    expect(orderAnchorEnvelope == fixture.value("tapescript_read_order_anchor", json::object()),
           "phase5 read-order-anchor envelope should match golden fixture\nactual:\n" + orderAnchorEnvelope.dump(2));
    expect(protectedWindowEnvelope == fixture.value("tapescript_read_protected_window", json::object()),
           "phase5 read-protected-window envelope should match golden fixture\nactual:\n" + protectedWindowEnvelope.dump(2));
    expect(replaySnapshotEnvelope == fixture.value("tapescript_replay_snapshot", json::object()),
           "phase5 replay-snapshot envelope should match golden fixture\nactual:\n" + replaySnapshotEnvelope.dump(2));
    expect(artifactEnvelope == fixture.value("tapescript_read_artifact", json::object()),
           "phase5 artifact envelope should match golden fixture\nactual:\n" + artifactEnvelope.dump(2));
    expect(exportEnvelope == fixture.value("tapescript_export_artifact", json::object()),
           "phase5 export envelope should match golden fixture\nactual:\n" + exportEnvelope.dump(2));
    expect(qualityEnvelope == fixture.value("tapescript_read_session_quality", json::object()),
           "phase5 quality envelope should match golden fixture\nactual:\n" + qualityEnvelope.dump(2));
    expect(resourceListProjection == fixture.value("resources_list", json::array()),
           "phase5 resources/list projection should match golden fixture\nactual:\n" + resourceListProjection.dump(2));
    expect(sessionResourceProjection == fixture.value("resource_read_session_report", json::object()),
           "phase5 session report resource should match golden fixture\nactual:\n" + sessionResourceProjection.dump(2));
    expect(sessionArtifactBundleProjection == fixture.value("resource_read_session_report_bundle", json::object()),
           "phase5 session report bundle resource should match golden fixture\nactual:\n" + sessionArtifactBundleProjection.dump(2));
    expect(caseArtifactMarkdownProjection == fixture.value("resource_read_case_report_markdown", json::object()),
           "phase5 case report markdown resource should match golden fixture\nactual:\n" + caseArtifactMarkdownProjection.dump(2));

    const json unknownToolResult = adapter.callTool("tapescript_not_real", json::object());
    expect(unknownToolResult.value("isError", false), "unknown tool should return isError=true");
    const json unknownToolEnvelope = envelopeFromToolResult(unknownToolResult);
    expect(unknownToolEnvelope.value("error", json::object()).value("code", std::string()) == "unsupported_tool",
           "unknown tool should return unsupported_tool");

    const json invalidArgsResult = adapter.callTool("tapescript_replay_snapshot", json::object());
    expect(invalidArgsResult.value("isError", false), "missing replay args should return isError=true");
    const json invalidArgsEnvelope = envelopeFromToolResult(invalidArgsResult);
    expect(invalidArgsEnvelope.value("error", json::object()).value("code", std::string()) == "invalid_arguments",
           "missing replay args should return invalid_arguments");

    const json unknownPromptResult = adapter.getPromptResult("not_a_prompt", json::object());
    expect(!unknownPromptResult.value("meta", json::object()).value("ok", true),
           "unknown prompt should return meta.ok=false");
    expect(unknownPromptResult.value("meta", json::object()).value("code", std::string()) == "unsupported_prompt",
           "unknown prompt should return unsupported_prompt");

    const json unsupportedResourceResult = adapter.readResourceResult("tape://unknown/1");
    expect(!unsupportedResourceResult.value("meta", json::object()).value("ok", true),
           "unsupported resource should return meta.ok=false");
    expect(unsupportedResourceResult.value("meta", json::object()).value("code", std::string()) == "unsupported_resource",
           "unsupported resource should return unsupported_resource");

    tape_mcp::Adapter unavailableAdapter(tape_mcp::AdapterConfig{
        (testDataDir() / "tape-mcp-phase5-missing.sock").string()
    });
    const json unavailableResult = unavailableAdapter.callTool("tapescript_status", json::object());
    expect(unavailableResult.value("isError", false), "engine unavailable should return isError=true");
    const json unavailableEnvelope = envelopeFromToolResult(unavailableResult);
    expect(unavailableEnvelope.value("error", json::object()).value("code", std::string()) == "engine_unavailable",
           "engine unavailable should return engine_unavailable");
    expect(unavailableEnvelope.value("meta", json::object()).value("retryable", false),
           "engine unavailable should be marked retryable");

    server->stop();
}

void testTapeMcpPhase6BundleTools() {
    const fs::path rootDir = testDataDir() / "tape-mcp-phase6-engine";
    const fs::path socketPath = testDataDir() / "tape-mcp-phase6-engine.sock";
    auto server = startPhase5Engine(rootDir, socketPath);
    seedPhase5Engine(socketPath);

    waitUntil([&]() {
        const auto snapshot = server->snapshot();
        return snapshot.segments.size() >= 2 && snapshot.latestFrozenRevisionId >= 2;
    }, "phase6 MCP setup should freeze fixture batches");

    tape_mcp::Adapter adapter(tape_mcp::AdapterConfig{socketPath.string()});

    const json sessionReportEnvelope = envelopeFromToolResult(adapter.callTool("tapescript_scan_session_report", json{
        {"first_session_seq", 1},
        {"last_session_seq", 4},
        {"limit", 10}
    }));
    const std::uint64_t sessionReportId =
        parseTrailingNumericId(sessionReportEnvelope.value("result", json::object()).value("artifact_id", std::string()),
                               "session-report:");
    expect(sessionReportId > 0, "phase6 session report scan should expose a durable session report id");

    const json orderCaseReportEnvelope = envelopeFromToolResult(adapter.callTool("tapescript_scan_order_case_report", json{
        {"order_id", 7401},
        {"limit", 10}
    }));
    const std::uint64_t caseReportId =
        parseTrailingNumericId(orderCaseReportEnvelope.value("result", json::object()).value("artifact_id", std::string()),
                               "case-report:");
    expect(caseReportId > 0, "phase6 order-case report scan should expose a durable case report id");

    const json exportSessionBundleEnvelope = envelopeFromToolResult(adapter.callTool("tapescript_export_session_bundle", json{
        {"report_id", sessionReportId}
    }));
    expect(exportSessionBundleEnvelope.value("ok", false),
           "phase6 export-session-bundle should return ok=true");
    const std::string sessionBundlePath =
        exportSessionBundleEnvelope.value("result", json::object()).value("bundle", json::object()).value("bundle_path", std::string());
    expect(!sessionBundlePath.empty() && fs::exists(sessionBundlePath),
           "phase6 export-session-bundle should write a portable session bundle");
    const json verifySessionBundleEnvelope = envelopeFromToolResult(adapter.callTool("tapescript_verify_bundle", json{
        {"bundle_path", sessionBundlePath}
    }));
    expect(verifySessionBundleEnvelope.value("ok", false),
           "phase6 verify-bundle for a session bundle should return ok=true");
    expect(verifySessionBundleEnvelope.value("result", json::object()).value("verify_status", std::string()) == "valid",
           "phase6 verify-bundle for a session bundle should mark the bundle valid");
    expect(!verifySessionBundleEnvelope.value("result", json::object()).value("import_supported", true),
           "phase6 verify-bundle for a session bundle should mark it non-importable");
    expect(!verifySessionBundleEnvelope.value("result", json::object()).value("can_import", true),
           "phase6 verify-bundle for a session bundle should not offer import");

    const json exportCaseBundleEnvelope = envelopeFromToolResult(adapter.callTool("tapescript_export_case_bundle", json{
        {"report_id", caseReportId}
    }));
    expect(exportCaseBundleEnvelope.value("ok", false),
           "phase6 export-case-bundle should return ok=true");
    const std::string caseBundlePath =
        exportCaseBundleEnvelope.value("result", json::object()).value("bundle", json::object()).value("bundle_path", std::string());
    expect(!caseBundlePath.empty() && fs::exists(caseBundlePath),
           "phase6 export-case-bundle should write a portable case bundle");
    const json verifyCaseBundleEnvelope = envelopeFromToolResult(adapter.callTool("tapescript_verify_bundle", json{
        {"bundle_path", caseBundlePath}
    }));
    expect(verifyCaseBundleEnvelope.value("ok", false),
           "phase6 verify-bundle for a case bundle should return ok=true");
    expect(verifyCaseBundleEnvelope.value("result", json::object()).value("verify_status", std::string()) == "valid",
           "phase6 verify-bundle for a case bundle should mark the bundle valid");
    expect(verifyCaseBundleEnvelope.value("result", json::object()).value("import_supported", false),
           "phase6 verify-bundle for a fresh case bundle should mark it importable");
    expect(!verifyCaseBundleEnvelope.value("result", json::object()).value("already_imported", true),
           "phase6 verify-bundle for a fresh case bundle should not mark it imported");
    expect(verifyCaseBundleEnvelope.value("result", json::object()).value("can_import", false),
           "phase6 verify-bundle for a fresh case bundle should allow import");
    expect(!verifyCaseBundleEnvelope.value("result", json::object())
                .value("bundle", json::object())
                .value("payload_sha256", std::string()).empty(),
           "phase6 verify-bundle for a fresh case bundle should expose its payload hash");

    const json importCaseBundleEnvelope = envelopeFromToolResult(adapter.callTool("tapescript_import_case_bundle", json{
        {"bundle_path", caseBundlePath}
    }));
    expect(importCaseBundleEnvelope.value("ok", false),
           "phase6 import-case-bundle should return ok=true");
    expect(!importCaseBundleEnvelope.value("result", json::object()).value("duplicate_import", true),
           "phase6 first import-case-bundle call should not be marked duplicate");
    const std::string importedArtifactId =
        importCaseBundleEnvelope.value("result", json::object()).value("artifact", json::object()).value("artifact_id", std::string());
    expect(!importedArtifactId.empty() && importedArtifactId.rfind("imported-case:", 0) == 0,
           "phase6 import-case-bundle should surface an imported-case artifact id");

    const json listImportedEnvelope = envelopeFromToolResult(adapter.callTool("tapescript_list_imported_cases", json{
        {"limit", 10}
    }));
    expect(listImportedEnvelope.value("ok", false),
           "phase6 list-imported-cases should return ok=true");
    const json importedRows = listImportedEnvelope.value("result", json::object()).value("imported_cases", json::array());
    expect(importedRows.is_array() && !importedRows.empty(),
           "phase6 list-imported-cases should return the imported bundle inventory");
    expect(importedRows.front().value("artifact_id", std::string()) == importedArtifactId,
           "phase6 list-imported-cases should expose the imported-case artifact id");

    const json importedArtifactEnvelope = envelopeFromToolResult(adapter.callTool("tapescript_read_artifact", json{
        {"artifact_id", importedArtifactId}
    }));
    expect(importedArtifactEnvelope.value("ok", false),
           "phase6 read-artifact for imported-case should return ok=true");
    expect(importedArtifactEnvelope.value("result", json::object()).value("artifact_id", std::string()) == importedArtifactId,
           "phase6 imported-case read-artifact should reopen the imported artifact");
    expect(importedArtifactEnvelope.value("result", json::object()).value("artifact_kind", std::string()) == "imported_case_bundle",
           "phase6 imported-case read-artifact should expose the imported bundle artifact kind");

    const json duplicateImportEnvelope = envelopeFromToolResult(adapter.callTool("tapescript_import_case_bundle", json{
        {"bundle_path", caseBundlePath}
    }));
    expect(duplicateImportEnvelope.value("ok", false),
           "phase6 duplicate import-case-bundle should still return ok=true");
    expect(duplicateImportEnvelope.value("result", json::object()).value("duplicate_import", false),
           "phase6 duplicate import-case-bundle should be marked duplicate");
    const json verifyImportedCaseBundleEnvelope = envelopeFromToolResult(adapter.callTool("tapescript_verify_bundle", json{
        {"bundle_path", caseBundlePath}
    }));
    expect(verifyImportedCaseBundleEnvelope.value("ok", false),
           "phase6 verify-bundle after import should return ok=true");
    expect(verifyImportedCaseBundleEnvelope.value("result", json::object()).value("already_imported", false),
           "phase6 verify-bundle after import should mark the bundle imported");
    expect(!verifyImportedCaseBundleEnvelope.value("result", json::object()).value("can_import", true),
           "phase6 verify-bundle after import should stop offering import");
    expect(verifyImportedCaseBundleEnvelope.value("result", json::object())
               .value("imported_case", json::object())
               .value("artifact_id", std::string()) == importedArtifactId,
           "phase6 verify-bundle after import should surface the imported artifact id");

    server->stop();
}

void testTapeMcpStdioHarness() {
    const fs::path rootDir = testDataDir() / "tape-mcp-phase5-stdio-engine";
    const fs::path socketPath = testDataDir() / "tape-mcp-phase5-stdio-engine.sock";
    auto server = startPhase5Engine(rootDir, socketPath);
    seedPhase5Engine(socketPath);

    waitUntil([&]() {
        const auto snapshot = server->snapshot();
        return snapshot.segments.size() >= 2 && snapshot.latestFrozenRevisionId >= 2;
    }, "phase5 MCP stdio setup should freeze fixture batches");

    const ChildProcess child = launchTapeMcp(socketPath);

    writeJsonRpcMessage(child.writeFd, json{
        {"jsonrpc", "2.0"},
        {"id", 0},
        {"method", "tools/list"},
        {"params", json::object()}
    });
    const json preInitializeResponse = readJsonRpcMessage(child.readFd);
    expect(preInitializeResponse.value("error", json::object()).value("code", 0) == -32002,
           "tools/list before initialize should return server not initialized");

    writeJsonRpcMessage(child.writeFd, json{
        {"jsonrpc", "2.0"},
        {"id", 1},
        {"method", "initialize"},
        {"params", json::object()}
    });
    const json initializeResponse = readJsonRpcMessage(child.readFd);
    expect(initializeResponse.value("jsonrpc", std::string()) == "2.0",
           "initialize response should use jsonrpc=2.0");
    expect(initializeResponse.value("id", 0) == 1,
           "initialize response should preserve the request id");
    expect(initializeResponse.value("result", json::object())
               .value("serverInfo", json::object())
               .value("name", std::string()) == "tape-mcp",
           "initialize response should include tape-mcp server info");
    expect(initializeResponse.value("result", json::object())
               .value("capabilities", json::object())
               .value("resources", json::object())
               .value("listChanged", true) == false,
           "initialize response should advertise MCP resources support");

    writeJsonRpcMessage(child.writeFd, json{
        {"jsonrpc", "2.0"},
        {"method", "notifications/initialized"},
        {"params", json::object()}
    });

    writeJsonRpcMessage(child.writeFd, json{
        {"jsonrpc", "2.0"},
        {"id", 2},
        {"method", "tools/list"},
        {"params", json::object()}
    });
    const json listResponse = readJsonRpcMessage(child.readFd);
    const json tools = listResponse.value("result", json::object()).value("tools", json::array());
    expect(tools.is_array() && tools.size() == 31, "tools/list should expose the expanded phase 6 tool slice");
    json overviewTool = json::object();
    for (const auto& tool : tools) {
        if (tool.value("name", std::string()) == "tapescript_read_session_overview") {
            overviewTool = tool;
            break;
        }
    }
    expect(overviewTool.is_object(), "tools/list should include tapescript_read_session_overview");
    expect(overviewTool.value("title", std::string()) == "Read Session Overview",
           "tools/list should expose a human-readable title");
    expect(overviewTool.value("annotations", json::object()).value("readOnlyHint", false),
           "tools/list should expose readOnlyHint for read-only tools");
    expect(overviewTool.value("inputSchema", json::object()).value("examples", json::array()).is_array() &&
               !overviewTool.value("inputSchema", json::object()).value("examples", json::array()).empty(),
           "tools/list should expose example arguments for high-value tools");
    json scanReportTool = json::object();
    for (const auto& tool : tools) {
        if (tool.value("name", std::string()) == "tapescript_scan_session_report") {
            scanReportTool = tool;
            break;
        }
    }
    expect(scanReportTool.is_object(), "tools/list should include tapescript_scan_session_report");
    expect(scanReportTool.value("progressHint", false),
           "tools/list should advertise progressHint for heavy scan/export tools");
    json verifyBundleTool = json::object();
    for (const auto& tool : tools) {
        if (tool.value("name", std::string()) == "tapescript_verify_bundle") {
            verifyBundleTool = tool;
            break;
        }
    }
    expect(verifyBundleTool.is_object(), "tools/list should include tapescript_verify_bundle");
    expect(verifyBundleTool.value("progressHint", false),
           "tools/list should advertise progressHint for bundle verification");

    writeJsonRpcMessage(child.writeFd, json{
        {"jsonrpc", "2.0"},
        {"id", 21},
        {"method", "prompts/list"},
        {"params", json::object()}
    });
    const json promptListResponse = readJsonRpcMessage(child.readFd);
    const json prompts = promptListResponse.value("result", json::object()).value("prompts", json::array());
    expect(prompts.is_array() && prompts.size() == 8, "prompts/list should expose the expanded phase5 prompt set");

    writeJsonRpcMessage(child.writeFd, json{
        {"jsonrpc", "2.0"},
        {"id", 22},
        {"method", "prompts/get"},
        {"params", {
            {"name", "investigate_session_range"},
            {"arguments", json{
                {"first_session_seq", 1},
                {"last_session_seq", 4},
                {"revision_id", 7}
            }}
        }}
    });
    const json promptGetResponse = readJsonRpcMessage(child.readFd);
    const json promptGetResult = promptGetResponse.value("result", json::object());
    expect(promptGetResult.value("meta", json::object()).value("prompt", std::string()) == "investigate_session_range",
           "prompts/get should resolve the requested prompt");
    expect(promptGetResult.value("messages", json::array()).is_array() &&
               !promptGetResult.value("messages", json::array()).empty(),
           "prompts/get should return at least one message");

    writeJsonRpcMessage(child.writeFd, json{
        {"jsonrpc", "2.0"},
        {"id", 23},
        {"method", "prompts/get"},
        {"params", {
            {"name", "investigate_bad_fill"},
            {"arguments", json{
                {"exec_id", "EXEC-401"},
                {"revision_id", 7}
            }}
        }}
    });
    const json badFillPromptResponse = readJsonRpcMessage(child.readFd);
    expect(badFillPromptResponse.value("result", json::object())
               .value("meta", json::object())
               .value("prompt", std::string()) == "investigate_bad_fill",
           "prompts/get should resolve investigate_bad_fill");

    writeJsonRpcMessage(child.writeFd, json{
        {"jsonrpc", "2.0"},
        {"id", 24},
        {"method", "not/a/real-method"},
        {"params", json::object()}
    });
    const json unknownMethodResponse = readJsonRpcMessage(child.readFd);
    expect(unknownMethodResponse.value("error", json::object()).value("code", 0) == -32601,
           "unknown MCP methods should return -32601");

    writeJsonRpcMessage(child.writeFd, json{
        {"jsonrpc", "2.0"},
        {"id", 25},
        {"method", "resources/read"},
        {"params", json::object()}
    });
    const json invalidResourceReadResponse = readJsonRpcMessage(child.readFd);
    expect(invalidResourceReadResponse.value("error", json::object()).value("code", 0) == -32602,
           "resources/read without uri should return -32602");

    writeJsonRpcMessage(child.writeFd, json{
        {"jsonrpc", "2.0"},
        {"id", 3},
        {"method", "tools/call"},
        {"params", {
            {"name", "tapescript_status"},
            {"arguments", json::object()}
        }}
    });
    const json statusResponse = readJsonRpcMessage(child.readFd);
    const json statusEnvelope = statusResponse.value("result", json::object()).value("structuredContent", json::object());
    expect(statusEnvelope.value("ok", false), "stdio status call should return ok=true");
    expect(statusEnvelope.value("meta", json::object()).value("tool", std::string()) == "tapescript_status",
           "stdio status call should preserve the tool name");

    writeJsonRpcMessage(child.writeFd, json{
        {"jsonrpc", "2.0"},
        {"id", 4},
        {"method", "tools/call"},
        {"params", {
            {"name", "tapescript_read_session_overview"},
            {"arguments", json{
                {"first_session_seq", 1},
                {"last_session_seq", 4}
            }}
        }}
    });
    const json overviewResponse = readJsonRpcMessage(child.readFd);
    const json overviewEnvelope = overviewResponse.value("result", json::object()).value("structuredContent", json::object());
    expect(overviewEnvelope.value("ok", false), "stdio session overview call should return ok=true");
    expect(overviewEnvelope.value("result", json::object()).value("artifact_kind", std::string()) == "session_overview",
           "stdio session overview call should return the typed investigation result");

    writeJsonRpcMessage(child.writeFd, json{
        {"jsonrpc", "2.0"},
        {"id", 41},
        {"method", "tools/call"},
        {"params", {
            {"name", "tapescript_scan_session_report"},
            {"arguments", json{
                {"first_session_seq", 1},
                {"last_session_seq", 4},
                {"limit", 10}
            }},
            {"_meta", {
                {"progressToken", "scan-session-report-1"}
            }}
        }}
    });
    const auto [scanProgressNotifications, scanReportResponse] =
        readProgressNotificationsAndResponse(child.readFd, 41);
    expect(scanProgressNotifications.size() == 5,
           "progress-capable scan should emit queued, dispatching, running, finalizing, and finished notifications");
    expect(scanProgressNotifications.front().value("params", json::object()).value("progress", 0) == 5,
           "scan progress should start with the queued stage");
    expect(scanProgressNotifications[1].value("params", json::object()).value("progress", 0) == 35,
           "scan progress should emit a dispatching stage");
    expect(scanProgressNotifications[2].value("params", json::object()).value("progress", 0) == 70,
           "scan progress should emit a running stage");
    expect(scanProgressNotifications[3].value("params", json::object()).value("progress", 0) == 90,
           "scan progress should emit a finalizing stage");
    expect(scanProgressNotifications.back().value("params", json::object()).value("progress", 0) == 100,
           "scan progress should finish at 100%");
    const json scanReportEnvelope = scanReportResponse.value("result", json::object()).value("structuredContent", json::object());
    expect(scanReportEnvelope.value("ok", false), "stdio scan-session-report call should return ok=true");
    const std::string sessionReportArtifactId = scanReportEnvelope.value("result", json::object()).value("artifact_id", std::string());
    expect(!sessionReportArtifactId.empty(), "stdio scan-session-report should return a durable report artifact id");
    const std::uint64_t sessionReportId = parseTrailingNumericId(sessionReportArtifactId, "session-report:");

    writeJsonRpcMessage(child.writeFd, json{
        {"jsonrpc", "2.0"},
        {"id", 5},
        {"method", "tools/call"},
        {"params", {
            {"name", "tapescript_read_live_tail"},
            {"arguments", json{{"limit", 10}}}
        }}
    });
    const json liveTailResponse = readJsonRpcMessage(child.readFd);
    const json liveTailEnvelope = liveTailResponse.value("result", json::object()).value("structuredContent", json::object());
    expect(liveTailEnvelope.value("ok", false), "stdio live-tail call should return ok=true");
    expect(liveTailEnvelope.value("result", json::object()).value("returned_count", 0ULL) > 0,
           "stdio live-tail call should return the typed event-list result");

    writeJsonRpcMessage(child.writeFd, json{
        {"jsonrpc", "2.0"},
        {"id", 51},
        {"method", "tools/call"},
        {"params", {
            {"name", "tapescript_scan_order_case_report"},
            {"arguments", json{
                {"order_id", 7401},
                {"limit", 10}
            }}
        }}
    });
    const json scanCaseReportResponse = readJsonRpcMessage(child.readFd);
    const json scanCaseEnvelope = scanCaseReportResponse.value("result", json::object()).value("structuredContent", json::object());
    expect(scanCaseEnvelope.value("ok", false), "stdio scan-order-case-report call should return ok=true");
    const std::string caseReportArtifactId = scanCaseEnvelope.value("result", json::object()).value("artifact_id", std::string());
    expect(!caseReportArtifactId.empty(), "stdio scan-order-case-report should return a durable case report id");
    const std::uint64_t caseReportId = parseTrailingNumericId(caseReportArtifactId, "case-report:");

    writeJsonRpcMessage(child.writeFd, json{
        {"jsonrpc", "2.0"},
        {"id", 6},
        {"method", "tools/call"},
        {"params", {
            {"name", "tapescript_list_session_reports"},
            {"arguments", json{{"limit", 10}}}
        }}
    });
    const json sessionReportListResponse = readJsonRpcMessage(child.readFd);
    const json reportListEnvelope = sessionReportListResponse.value("result", json::object()).value("structuredContent", json::object());
    expect(reportListEnvelope.value("ok", false), "stdio session report inventory call should return ok=true");
    expect(reportListEnvelope.value("result", json::object()).value("returned_count", 0ULL) >= 0,
           "stdio session report inventory call should return the typed report inventory result");

    writeJsonRpcMessage(child.writeFd, json{
        {"jsonrpc", "2.0"},
        {"id", 61},
        {"method", "resources/list"},
        {"params", json::object()}
    });
    const json resourcesListResponse = readJsonRpcMessage(child.readFd);
    const json resources = resourcesListResponse.value("result", json::object()).value("resources", json::array());
    expect(resources.is_array() && !resources.empty(), "resources/list should return durable report resources");

    writeJsonRpcMessage(child.writeFd, json{
        {"jsonrpc", "2.0"},
        {"id", 62},
        {"method", "resources/read"},
        {"params", {
            {"uri", "tape://report/session/" + std::to_string(sessionReportId)}
        }}
    });
    const json resourceReadResponse = readJsonRpcMessage(child.readFd);
    expect(resourceReadResponse.value("result", json::object()).value("meta", json::object()).value("ok", false),
           "resources/read should return ok=true for a durable session report resource");

    writeJsonRpcMessage(child.writeFd, json{
        {"jsonrpc", "2.0"},
        {"id", 63},
        {"method", "resources/read"},
        {"params", {
            {"uri", "tape://artifact/case-report/" + std::to_string(caseReportId) + "/markdown"}
        }}
    });
    const json caseMarkdownResourceResponse = readJsonRpcMessage(child.readFd);
    const json caseMarkdownContents = caseMarkdownResourceResponse.value("result", json::object()).value("contents", json::array());
    expect(caseMarkdownResourceResponse.value("result", json::object()).value("meta", json::object()).value("ok", false),
           "resources/read should return ok=true for a durable case report markdown resource");
    expect(caseMarkdownContents.is_array() && !caseMarkdownContents.empty() &&
               caseMarkdownContents.front().value("mimeType", std::string()) == "text/markdown",
           "resources/read should expose markdown case-report exports");

    writeJsonRpcMessage(child.writeFd, json{
        {"jsonrpc", "2.0"},
        {"id", 7},
        {"method", "tools/call"},
        {"params", {
            {"name", "tapescript_replay_snapshot"},
            {"arguments", json{
                {"target_session_seq", 4},
                {"depth_limit", 5}
            }}
        }}
    });
    const json replayResponse = readJsonRpcMessage(child.readFd);
    const json replayEnvelope = replayResponse.value("result", json::object()).value("structuredContent", json::object());
    expect(replayEnvelope.value("ok", false), "stdio replay-snapshot call should return ok=true");
    expect(replayEnvelope.value("result", json::object()).value("target_session_seq", 0ULL) == 4,
           "stdio replay-snapshot call should return the typed replay result");

    stopProcess(child);
    server->stop();
}

} // namespace

int main() {
    try {
        testTapeMcpPhase5Contracts();
        testTapeMcpPhase6BundleTools();
        testTapeMcpStdioHarness();
    } catch (const std::exception& error) {
        std::cerr << "tape_mcp_contract_tests failed: " << error.what() << '\n';
        return 1;
    }

    std::cout << "tape_mcp_contract_tests passed\n";
    return 0;
}
