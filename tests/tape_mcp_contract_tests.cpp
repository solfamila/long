#include "tape_mcp_adapter.h"

#include "app_shared.h"
#include "bridge_batch_codec.h"
#include "bridge_batch_transport.h"
#include "tape_engine.h"
#include "tape_engine_client.h"
#include "tape_engine_protocol.h"
#include "tape_phase7_artifacts.h"
#include "tape_phase8_artifacts.h"
#include "tape_phase7_runtime_bridge.h"
#include "uw_context_connectors.h"

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

void expectContains(const std::string& text, const std::string& needle, const std::string& message) {
    expect(text.find(needle) != std::string::npos, message + " (missing: " + needle + ")");
}

class ScopedEnvVar {
public:
    ScopedEnvVar(const char* key, std::optional<std::string> value)
        : key_(key) {
        if (const char* existing = std::getenv(key); existing != nullptr) {
            previousValue_ = std::string(existing);
        }
        if (value.has_value()) {
            expect(::setenv(key_.c_str(), value->c_str(), 1) == 0,
                   "failed to set env var " + key_);
        } else {
            expect(::unsetenv(key_.c_str()) == 0,
                   "failed to unset env var " + key_);
        }
    }

    ~ScopedEnvVar() {
        if (previousValue_.has_value()) {
            (void)::setenv(key_.c_str(), previousValue_->c_str(), 1);
        } else {
            (void)::unsetenv(key_.c_str());
        }
    }

    ScopedEnvVar(const ScopedEnvVar&) = delete;
    ScopedEnvVar& operator=(const ScopedEnvVar&) = delete;

private:
    std::string key_;
    std::optional<std::string> previousValue_;
};

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

fs::path configurePhase7DataDir(const std::string& name) {
    const fs::path path = testDataDir() / name;
    std::error_code ec;
    fs::create_directories(path, ec);
    expect(!ec, "failed to create phase7 data dir at " + path.string());
    expect(::setenv("TWS_GUI_DATA_DIR", path.string().c_str(), 1) == 0,
           "failed to set TWS_GUI_DATA_DIR for phase7 tests");
        expect(::setenv("LONG_DISABLE_EXTERNAL_CONTEXT", "1", 1) == 0,
            "failed to disable external context for phase7 tests");
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
    replaceSuffix("session-bundle:report:", "<id>");
    replaceSuffix("case-bundle:report:", "<id>");
    replaceSuffix("imported-case:", "<id>");
    if (artifactId.rfind("phase7-analysis-", 0) == 0) {
        return "phase7-analysis:<id>";
    }
    if (artifactId.rfind("phase7-playbook-", 0) == 0) {
        return "phase7-playbook:<id>";
    }
    if (artifactId.rfind("phase7-ledger-", 0) == 0) {
        return "phase7-ledger:<id>";
    }
    if (artifactId.rfind("watch-", 0) == 0) {
        return "phase8-watch:<id>";
    }
    if (artifactId.rfind("trigger-", 0) == 0) {
        return "phase8-trigger:<id>";
    }
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
    const std::string phase7AnalysisPrefix = "tape://phase7/analysis/";
    if (uri.rfind(phase7AnalysisPrefix, 0) == 0) {
        const std::size_t formatPos = uri.find('/', phase7AnalysisPrefix.size());
        if (formatPos == std::string::npos) {
            return phase7AnalysisPrefix + "<id>";
        }
        return phase7AnalysisPrefix + "<id>" + uri.substr(formatPos);
    }
    const std::string phase7PlaybookPrefix = "tape://phase7/playbook/";
    if (uri.rfind(phase7PlaybookPrefix, 0) == 0) {
        const std::size_t formatPos = uri.find('/', phase7PlaybookPrefix.size());
        if (formatPos == std::string::npos) {
            return phase7PlaybookPrefix + "<id>";
        }
        return phase7PlaybookPrefix + "<id>" + uri.substr(formatPos);
    }
    const std::string phase7LedgerPrefix = "tape://phase7/ledger/";
    if (uri.rfind(phase7LedgerPrefix, 0) == 0) {
        const std::size_t formatPos = uri.find('/', phase7LedgerPrefix.size());
        if (formatPos == std::string::npos) {
            return phase7LedgerPrefix + "<id>";
        }
        return phase7LedgerPrefix + "<id>" + uri.substr(formatPos);
    }
    const std::string phase7JournalPrefix = "tape://phase7/journal/";
    if (uri.rfind(phase7JournalPrefix, 0) == 0) {
        const std::size_t formatPos = uri.find('/', phase7JournalPrefix.size());
        if (formatPos == std::string::npos) {
            return phase7JournalPrefix + "<id>";
        }
        return phase7JournalPrefix + "<id>" + uri.substr(formatPos);
    }
    const std::string phase7ApplyPrefix = "tape://phase7/apply/";
    if (uri.rfind(phase7ApplyPrefix, 0) == 0) {
        const std::size_t formatPos = uri.find('/', phase7ApplyPrefix.size());
        if (formatPos == std::string::npos) {
            return phase7ApplyPrefix + "<id>";
        }
        return phase7ApplyPrefix + "<id>" + uri.substr(formatPos);
    }
    const std::string phase8WatchPrefix = "tape://phase8/watch/";
    if (uri.rfind(phase8WatchPrefix, 0) == 0) {
        const std::size_t formatPos = uri.find('/', phase8WatchPrefix.size());
        if (formatPos == std::string::npos) {
            return phase8WatchPrefix + "<id>";
        }
        return phase8WatchPrefix + "<id>" + uri.substr(formatPos);
    }
    const std::string phase8TriggerPrefix = "tape://phase8/trigger/";
    if (uri.rfind(phase8TriggerPrefix, 0) == 0) {
        const std::size_t formatPos = uri.find('/', phase8TriggerPrefix.size());
        if (formatPos == std::string::npos) {
            return phase8TriggerPrefix + "<id>";
        }
        return phase8TriggerPrefix + "<id>" + uri.substr(formatPos);
    }
    return uri;
}

json normalizeHash(const json& value) {
    if (!value.is_string()) {
        return nullptr;
    }
    return value.get<std::string>().empty() ? json(nullptr) : json("<hash>");
}

std::string stringValueOrEmpty(const json& value, const char* key) {
    if (!value.is_object() || key == nullptr || !value.contains(key) || !value.at(key).is_string()) {
        return std::string();
    }
    return value.at(key).get<std::string>();
}

json stringValueOrNull(const json& value, const char* key) {
    const std::string text = stringValueOrEmpty(value, key);
    if (text.empty()) {
        return nullptr;
    }
    return text;
}

json normalizePositiveId(std::uint64_t value) {
    return value == 0 ? json(nullptr) : json("<id>");
}

json normalizePositiveCount(std::uint64_t value) {
    return value == 0 ? json(0ULL) : json("<count>");
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

json projectPhase7ArtifactRef(const json& artifact) {
    if (!artifact.is_object()) {
        return json::object();
    }
    const json normalizedManifestPath = normalizeTestPath(artifact.value("manifest_path", json(nullptr)));
    std::string manifestPath = normalizedManifestPath.is_string()
        ? normalizedManifestPath.get<std::string>()
        : std::string();
    const std::string analysisPrefix = "/phase7_artifacts/analysis/phase7-analysis-";
    const std::string playbookPrefix = "/phase7_artifacts/playbooks/phase7-playbook-";
    const std::string ledgerPrefix = "/phase7_artifacts/ledgers/phase7-ledger-";
    const auto analysisOffset = manifestPath.find(analysisPrefix);
    if (analysisOffset != std::string::npos) {
        manifestPath = manifestPath.substr(0, analysisOffset) +
            "/phase7_artifacts/analysis/phase7-analysis:<id>/manifest.json";
    }
    const auto playbookOffset = manifestPath.find(playbookPrefix);
    if (playbookOffset != std::string::npos) {
        manifestPath = manifestPath.substr(0, playbookOffset) +
            "/phase7_artifacts/playbooks/phase7-playbook:<id>/manifest.json";
    }
    const auto ledgerOffset = manifestPath.find(ledgerPrefix);
    if (ledgerOffset != std::string::npos) {
        manifestPath = manifestPath.substr(0, ledgerOffset) +
            "/phase7_artifacts/ledgers/phase7-ledger:<id>/manifest.json";
    }
    return {
        {"artifact_id", normalizeArtifactId(stringValueOrEmpty(artifact, "artifact_id"))},
        {"artifact_type", stringValueOrEmpty(artifact, "artifact_type")},
        {"contract_version", stringValueOrEmpty(artifact, "contract_version")},
        {"manifest_path", std::move(manifestPath)}
    };
}

json projectPhase8ArtifactRef(const json& artifact) {
    if (!artifact.is_object()) {
        return json::object();
    }
    const json normalizedManifestPath = normalizeTestPath(artifact.value("manifest_path", json(nullptr)));
    std::string manifestPath = normalizedManifestPath.is_string()
        ? normalizedManifestPath.get<std::string>()
        : std::string();
    const std::string watchPrefix = "/phase8_artifacts/watch-definitions/watch-";
    const std::string triggerPrefix = "/phase8_artifacts/trigger-runs/trigger-";
    const auto watchOffset = manifestPath.find(watchPrefix);
    if (watchOffset != std::string::npos) {
        manifestPath = manifestPath.substr(0, watchOffset) +
            "/phase8_artifacts/watch-definitions/phase8-watch:<id>/manifest.json";
    }
    const auto triggerOffset = manifestPath.find(triggerPrefix);
    if (triggerOffset != std::string::npos) {
        manifestPath = manifestPath.substr(0, triggerOffset) +
            "/phase8_artifacts/trigger-runs/phase8-trigger:<id>/manifest.json";
    }
    return {
        {"artifact_id", normalizeArtifactId(stringValueOrEmpty(artifact, "artifact_id"))},
        {"artifact_type", stringValueOrEmpty(artifact, "artifact_type")},
        {"contract_version", stringValueOrEmpty(artifact, "contract_version")},
        {"manifest_path", std::move(manifestPath)}
    };
}

json projectPhase7ReplayContext(const json& replayContext);
json projectPhase7FindingRows(const json& findings);
json projectPhase7PlaybookActions(const json& actions);
json projectPhase7AppliedFilters(const json& filters);
json projectPhase7AnalysisInventoryResult(const json& result);
json projectPhase7PlaybookInventoryResult(const json& result);

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
        const std::string summary = report.value("what_changed_first", std::string());
        projected["what_changed_first"] = summary.empty() ? json(std::string()) : json("<summary>");
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
json projectPhase7ExecutionLedgerResult(const json& result);
json projectPhase7ExecutionLedgerInventoryResult(const json& result);
json projectPhase7ExecutionJournalResult(const json& result);
json projectPhase7ExecutionJournalInventoryResult(const json& result);
json projectPhase7ExecutionApplyResult(const json& result);
json projectPhase7ExecutionApplyInventoryResult(const json& result);
json projectPhase8WatchDefinitionResult(const json& result);
json projectPhase8TriggerRunResult(const json& result);
json projectPhase8AttentionInboxResult(const json& result);
json projectPhase8DueWatchListResult(const json& result);
json projectPhase8RunDueWatchesResult(const json& result);

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

json projectPhase7ResourceList(const json& listResult) {
    json projection = json::array();
    std::vector<std::pair<std::string, std::string>> seen;
    json resources = listResult.value("resources", json::array());
    for (const auto& resource : resources) {
        const std::string uri = normalizeResourceUri(resource.value("uri", std::string()));
        if (uri.rfind("tape://phase7/", 0) != 0) {
            continue;
        }
        const std::string mimeType = resource.value("mimeType", std::string());
        const auto key = std::make_pair(uri, mimeType);
        if (std::find(seen.begin(), seen.end(), key) != seen.end()) {
            continue;
        }
        seen.push_back(key);
        projection.push_back({
            {"uri", uri},
            {"mime_type", mimeType}
        });
    }
    std::sort(projection.begin(), projection.end(), [](const json& left, const json& right) {
        return left.value("uri", std::string()) < right.value("uri", std::string());
    });
    return projection;
}

json projectPhase8ResourceList(const json& listResult) {
    json projection = json::array();
    std::vector<std::pair<std::string, std::string>> seen;
    json resources = listResult.value("resources", json::array());
    for (const auto& resource : resources) {
        const std::string uri = normalizeResourceUri(resource.value("uri", std::string()));
        if (uri.rfind("tape://phase8/", 0) != 0) {
            continue;
        }
        const std::string mimeType = resource.value("mimeType", std::string());
        const auto key = std::make_pair(uri, mimeType);
        if (std::find(seen.begin(), seen.end(), key) != seen.end()) {
            continue;
        }
        seen.push_back(key);
        projection.push_back({
            {"uri", uri},
            {"mime_type", mimeType}
        });
    }
    std::sort(projection.begin(), projection.end(), [](const json& left, const json& right) {
        return left.value("uri", std::string()) < right.value("uri", std::string());
    });
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
        if (parsed.contains("execution_apply") && parsed.contains("execution_journal")) {
            projection["payload"] = projectPhase7ExecutionApplyResult(parsed);
        } else if (parsed.contains("execution_journal") && parsed.contains("execution_ledger")) {
            projection["payload"] = projectPhase7ExecutionJournalResult(parsed);
        } else if (parsed.contains("execution_ledger") && parsed.contains("playbook_artifact")) {
            projection["payload"] = projectPhase7ExecutionLedgerResult(parsed);
        } else if (parsed.contains("trigger_run") && parsed.contains("watch_definition")) {
            projection["payload"] = projectPhase8TriggerRunResult(parsed);
        } else if (parsed.contains("watch_definition") && parsed.contains("bundle_path")) {
            projection["payload"] = projectPhase8WatchDefinitionResult(parsed);
        } else if (parsed.contains("attention_items")) {
            projection["payload"] = projectPhase8AttentionInboxResult(parsed);
        } else if (parsed.contains("source_artifact") && parsed.contains("analysis_artifact")) {
            projection["payload"] = {
                {"analysis_artifact", projectPhase7ArtifactRef(parsed.value("analysis_artifact", json::object()))},
                {"source_artifact", projectPhase7ArtifactRef(parsed.value("source_artifact", json::object()))},
                {"analysis_profile", parsed.value("analysis_profile", std::string())},
                {"finding_count", parsed.value("finding_count", 0ULL)},
                {"replay_context", projectPhase7ReplayContext(parsed.value("replay_context", json::object()))},
                {"findings", projectPhase7FindingRows(parsed.value("findings", json::array()))}
            };
        } else if (parsed.contains("playbook_artifact") && parsed.contains("analysis_artifact")) {
            projection["payload"] = {
                {"analysis_artifact", projectPhase7ArtifactRef(parsed.value("analysis_artifact", json::object()))},
                {"playbook_artifact", projectPhase7ArtifactRef(parsed.value("playbook_artifact", json::object()))},
                {"mode", parsed.value("mode", std::string())},
                {"filtered_finding_ids", parsed.value("filtered_finding_ids", json::array())},
                {"planned_actions", projectPhase7PlaybookActions(parsed.value("planned_actions", json::array()))},
                {"replay_context", projectPhase7ReplayContext(parsed.value("replay_context", json::object()))}
            };
        } else if (parsed.contains("artifact")) {
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
        projection["contains_phase7_analysis"] = text.find("# Phase 7 Analysis") != std::string::npos;
        projection["contains_phase7_playbook"] = text.find("# Phase 7 Playbook") != std::string::npos;
        projection["contains_phase7_execution_ledger"] = text.find("# Phase 7 Execution Ledger") != std::string::npos;
        projection["contains_phase7_execution_apply"] = text.find("# Phase 7 Execution Apply") != std::string::npos;
        projection["contains_phase8_watch"] = text.find("watch_artifact_id: `watch-") != std::string::npos;
        projection["contains_phase8_trigger"] = text.find("trigger_artifact_id: `trigger-") != std::string::npos;
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

json projectPhase7ReplayContext(const json& replayContext) {
    if (!replayContext.is_object()) {
        return json::object();
    }
    const json traceAnchor = replayContext.value("trace_anchor", json::object());
    const bool hasTraceAnchor =
        traceAnchor.contains("trace_id") || traceAnchor.contains("order_id") ||
        traceAnchor.contains("perm_id") || traceAnchor.contains("exec_id");
    return {
        {"source_bundle_type", replayContext.value("source_bundle_type", std::string())},
        {"has_trace_anchor", hasTraceAnchor},
        {"requested_window", projectReplayRangeSummary(replayContext.value("requested_window", json(nullptr)))}
    };
}

json projectPhase7FindingRows(const json& findings) {
    json rows = json::array();
    for (const auto& finding : findings) {
        rows.push_back({
            {"finding_id", finding.value("finding_id", std::string())},
            {"severity", finding.value("severity", std::string())},
            {"category", finding.value("category", std::string())},
            {"summary", finding.value("summary", std::string())}
        });
    }
    std::sort(rows.begin(), rows.end(), [](const json& left, const json& right) {
        if (left.value("category", std::string()) != right.value("category", std::string())) {
            return left.value("category", std::string()) < right.value("category", std::string());
        }
        return left.value("finding_id", std::string()) < right.value("finding_id", std::string());
    });
    return rows;
}

json projectPhase7PlaybookActions(const json& actions) {
    json rows = json::array();
    for (const auto& action : actions) {
        rows.push_back({
            {"action_id", normalizeArtifactId(action.value("action_id", std::string()))},
            {"action_type", action.value("action_type", std::string())},
            {"finding_id", action.value("finding_id", std::string())},
            {"title", action.value("title", std::string())}
        });
    }
    std::sort(rows.begin(), rows.end(), [](const json& left, const json& right) {
        if (left.value("action_type", std::string()) != right.value("action_type", std::string())) {
            return left.value("action_type", std::string()) < right.value("action_type", std::string());
        }
        return left.value("finding_id", std::string()) < right.value("finding_id", std::string());
    });
    return rows;
}

json projectPhase7AppliedFilters(const json& filters) {
    if (!filters.is_object()) {
        return json::object();
    }
    json requestedIds = json::array();
    for (const auto& findingId : filters.value("requested_finding_ids", json::array())) {
        requestedIds.push_back(findingId);
    }
    std::sort(requestedIds.begin(), requestedIds.end());
    return {
        {"requested_finding_ids", std::move(requestedIds)},
        {"minimum_severity", filters.contains("minimum_severity") ? filters.at("minimum_severity") : json(nullptr)},
        {"category", filters.contains("category") ? filters.at("category") : json(nullptr)},
        {"limit", filters.contains("limit") ? filters.at("limit") : json(nullptr)},
        {"selected_count", filters.value("selected_count", 0ULL)}
    };
}

json projectPhase7AnalysisInventoryFilters(const json& filters) {
    if (!filters.is_object()) {
        return json::object();
    }
    return {
        {"source_artifact_id", [&]() -> json {
            if (!filters.contains("source_artifact_id") || filters.at("source_artifact_id").is_null()) {
                return nullptr;
            }
            return normalizeArtifactId(filters.at("source_artifact_id").get<std::string>());
        }()},
        {"analysis_profile", filters.contains("analysis_profile") ? filters.at("analysis_profile") : json(nullptr)},
        {"limit", filters.contains("limit") ? filters.at("limit") : json(nullptr)},
        {"matched_count", filters.value("matched_count", 0ULL)}
    };
}

json projectPhase7PlaybookInventoryFilters(const json& filters) {
    if (!filters.is_object()) {
        return json::object();
    }
    return {
        {"analysis_artifact_id", [&]() -> json {
            if (!filters.contains("analysis_artifact_id") || filters.at("analysis_artifact_id").is_null()) {
                return nullptr;
            }
            return normalizeArtifactId(filters.at("analysis_artifact_id").get<std::string>());
        }()},
        {"source_artifact_id", [&]() -> json {
            if (!filters.contains("source_artifact_id") || filters.at("source_artifact_id").is_null()) {
                return nullptr;
            }
            return normalizeArtifactId(filters.at("source_artifact_id").get<std::string>());
        }()},
        {"mode", filters.contains("mode") ? filters.at("mode") : json(nullptr)},
        {"limit", filters.contains("limit") ? filters.at("limit") : json(nullptr)},
        {"matched_count", filters.value("matched_count", 0ULL)}
    };
}

json projectPhase7AnalysisProfile(const json& profile) {
    return {
        {"analysis_profile", stringValueOrEmpty(profile, "analysis_profile")},
        {"title", stringValueOrEmpty(profile, "title")},
        {"summary", stringValueOrEmpty(profile, "summary")},
        {"default_profile", profile.value("default_profile", false)},
        {"supported_source_bundle_types", profile.value("supported_source_bundle_types", json::array())},
        {"finding_categories", profile.value("finding_categories", json::array())}
    };
}

json projectPhase7AnalysisProfileListResult(const json& result) {
    json rows = json::array();
    for (const auto& row : result.value("analysis_profiles", json::array())) {
        rows.push_back(projectPhase7AnalysisProfile(row));
    }
    std::sort(rows.begin(), rows.end(), [](const json& left, const json& right) {
        return left.value("analysis_profile", std::string()) <
               right.value("analysis_profile", std::string());
    });
    return {
        {"returned_count", result.value("returned_count", 0ULL)},
        {"analysis_profiles", std::move(rows)}
    };
}

json projectPhase7AnalysisInventoryResult(const json& result) {
    json rows = json::array();
    for (const auto& row : result.value("analysis_artifacts", json::array())) {
        rows.push_back({
            {"analysis_artifact", projectPhase7ArtifactRef(row.value("analysis_artifact", json::object()))},
            {"source_artifact", projectPhase7ArtifactRef(row.value("source_artifact", json::object()))},
            {"analysis_profile", stringValueOrEmpty(row, "analysis_profile")},
            {"has_generated_at_utc", !stringValueOrEmpty(row, "generated_at_utc").empty()},
            {"finding_count", row.value("finding_count", 0ULL)},
            {"replay_context", projectPhase7ReplayContext(row.value("replay_context", json::object()))}
        });
    }
    std::sort(rows.begin(), rows.end(), [](const json& left, const json& right) {
        return left.value("analysis_artifact", json::object()).value("artifact_id", std::string()) <
               right.value("analysis_artifact", json::object()).value("artifact_id", std::string());
    });
    return {
        {"returned_count", result.value("returned_count", 0ULL)},
        {"applied_filters", projectPhase7AnalysisInventoryFilters(result.value("applied_filters", json::object()))},
        {"analysis_artifacts", std::move(rows)}
    };
}

json projectPhase7ExecutionLedgerEntries(const json& entries) {
    json rows = json::array();
    for (const auto& entry : entries) {
        const std::string reviewedAtUtc = stringValueOrEmpty(entry, "reviewed_at_utc");
        const std::string reviewComment = stringValueOrEmpty(entry, "review_comment");
        rows.push_back({
            {"entry_id", entry.value("entry_id", std::string())},
            {"action_id", entry.value("action_id", std::string())},
            {"action_type", entry.value("action_type", std::string())},
            {"finding_id", entry.value("finding_id", std::string())},
            {"review_status", stringValueOrEmpty(entry, "review_status")},
            {"reviewed_by", stringValueOrNull(entry, "reviewed_by")},
            {"has_reviewed_at_utc", !reviewedAtUtc.empty()},
            {"has_review_comment", !reviewComment.empty()},
            {"distinct_reviewer_count", entry.value("distinct_reviewer_count", 0ULL)},
            {"approval_reviewer_count", entry.value("approval_reviewer_count", 0ULL)},
            {"approval_threshold_met", entry.value("approval_threshold_met", false)},
            {"requires_manual_confirmation", entry.value("requires_manual_confirmation", false)},
            {"title", stringValueOrEmpty(entry, "title")}
        });
    }
    std::sort(rows.begin(), rows.end(), [](const json& left, const json& right) {
        if (left.value("action_type", std::string()) != right.value("action_type", std::string())) {
            return left.value("action_type", std::string()) < right.value("action_type", std::string());
        }
        return left.value("entry_id", std::string()) < right.value("entry_id", std::string());
    });
    return rows;
}

json projectPhase7ExecutionLedgerFilters(const json& filters) {
    if (!filters.is_object()) {
        return json::object();
    }
    return {
        {"playbook_artifact_id", [&]() -> json {
            if (!filters.contains("playbook_artifact_id") || filters.at("playbook_artifact_id").is_null()) {
                return nullptr;
            }
            return normalizeArtifactId(filters.at("playbook_artifact_id").get<std::string>());
        }()},
        {"analysis_artifact_id", [&]() -> json {
            if (!filters.contains("analysis_artifact_id") || filters.at("analysis_artifact_id").is_null()) {
                return nullptr;
            }
            return normalizeArtifactId(filters.at("analysis_artifact_id").get<std::string>());
        }()},
        {"source_artifact_id", [&]() -> json {
            if (!filters.contains("source_artifact_id") || filters.at("source_artifact_id").is_null()) {
                return nullptr;
            }
            return normalizeArtifactId(filters.at("source_artifact_id").get<std::string>());
        }()},
        {"ledger_status", filters.contains("ledger_status") ? filters.at("ledger_status") : json(nullptr)},
        {"sort_by", filters.contains("sort_by") ? filters.at("sort_by") : json("generated_at_desc")},
        {"limit", filters.contains("limit") ? filters.at("limit") : json(nullptr)},
        {"matched_count", filters.value("matched_count", 0ULL)}
    };
}

json projectPhase7ExecutionLedgerReviewSummary(const json& summary) {
    if (!summary.is_object()) {
        return json::object();
    }
    return {
        {"pending_review_count", summary.value("pending_review_count", 0ULL)},
        {"approved_count", summary.value("approved_count", 0ULL)},
        {"blocked_count", summary.value("blocked_count", 0ULL)},
        {"needs_info_count", summary.value("needs_info_count", 0ULL)},
        {"not_applicable_count", summary.value("not_applicable_count", 0ULL)},
        {"reviewed_count", summary.value("reviewed_count", 0ULL)},
        {"waiting_approval_count", summary.value("waiting_approval_count", 0ULL)},
        {"ready_entry_count", summary.value("ready_entry_count", 0ULL)},
        {"actionable_entry_count", summary.value("actionable_entry_count", 0ULL)},
        {"distinct_reviewer_count", summary.value("distinct_reviewer_count", 0ULL)},
        {"required_approval_count", summary.value("required_approval_count", 0ULL)},
        {"ready_for_execution", summary.value("ready_for_execution", false)}
    };
}

json projectPhase7ExecutionLedgerAuditSummary(const json& event) {
    if (!event.is_object()) {
        return json::object();
    }
    return {
        {"event_type", stringValueOrNull(event, "event_type")},
        {"has_generated_at_utc", !stringValueOrEmpty(event, "generated_at_utc").empty()},
        {"actor", stringValueOrNull(event, "actor")},
        {"review_status", stringValueOrNull(event, "review_status")},
        {"ledger_status", stringValueOrNull(event, "ledger_status")},
        {"message", stringValueOrNull(event, "message")}
    };
}

json projectPhase7ExecutionLedgerResult(const json& result) {
    json auditTrail = json::array();
    for (const auto& event : result.value("audit_trail", json::array())) {
        const std::string eventStatus = stringValueOrEmpty(event, "status");
        const std::string eventLedgerStatus = stringValueOrEmpty(event, "ledger_status");
        const std::string eventComment = stringValueOrEmpty(event, "comment");
        const std::string eventMessage = stringValueOrEmpty(event, "message");
        const std::string generatedAtUtc = stringValueOrEmpty(event, "generated_at_utc");
        auditTrail.push_back({
            {"event_id_present", !stringValueOrEmpty(event, "event_id").empty()},
            {"event_type", stringValueOrEmpty(event, "event_type")},
            {"status", !eventStatus.empty() ? json(eventStatus) : json(eventLedgerStatus)},
            {"review_status", stringValueOrNull(event, "review_status")},
            {"actor", stringValueOrNull(event, "actor")},
            {"updated_entry_count", event.value("updated_entry_ids", json::array()).size()},
            {"previous_entry_status_count", event.value("previous_entry_statuses", json::array()).size()},
            {"has_comment", !eventComment.empty()},
            {"message", eventMessage},
            {"has_generated_at_utc", !generatedAtUtc.empty()}
        });
    }
    std::sort(auditTrail.begin(), auditTrail.end(), [](const json& left, const json& right) {
        if (left.value("event_type", std::string()) != right.value("event_type", std::string())) {
            return left.value("event_type", std::string()) < right.value("event_type", std::string());
        }
        return left.value("message", std::string()) < right.value("message", std::string());
    });

    json projection{
        {"source_artifact", projectPhase7ArtifactRef(result.value("source_artifact", json::object()))},
        {"analysis_artifact", projectPhase7ArtifactRef(result.value("analysis_artifact", json::object()))},
        {"playbook_artifact", projectPhase7ArtifactRef(result.value("playbook_artifact", json::object()))},
        {"execution_ledger", projectPhase7ArtifactRef(result.value("execution_ledger", json::object()))},
        {"mode", stringValueOrEmpty(result, "mode")},
        {"has_generated_at_utc", !stringValueOrEmpty(result, "generated_at_utc").empty()},
        {"ledger_status", stringValueOrEmpty(result, "ledger_status")},
        {"execution_policy", result.value("execution_policy", json::object())},
        {"filtered_finding_ids", result.value("filtered_finding_ids", json::array())},
        {"entry_count", result.value("entry_count", 0ULL)},
        {"review_summary", projectPhase7ExecutionLedgerReviewSummary(result.value("review_summary", json::object()))},
        {"latest_audit_event", projectPhase7ExecutionLedgerAuditSummary(result.value("latest_audit_event", json::object()))},
        {"entries", projectPhase7ExecutionLedgerEntries(result.value("entries", json::array()))},
        {"audit_trail", std::move(auditTrail)},
        {"replay_context", projectPhase7ReplayContext(result.value("replay_context", json::object()))}
    };
    if (result.contains("artifact_status")) {
        projection["artifact_status"] = stringValueOrEmpty(result, "artifact_status");
    }
    if (result.contains("updated_entry_ids")) {
        projection["updated_entry_ids"] = result.value("updated_entry_ids", json::array());
    }
    if (result.contains("audit_event_id")) {
        projection["audit_event_id_present"] = !stringValueOrEmpty(result, "audit_event_id").empty();
    }
    return projection;
}

json projectPhase7ExecutionLedgerInventoryResult(const json& result) {
    json rows = json::array();
    for (const auto& row : result.value("execution_ledgers", json::array())) {
        rows.push_back({
            {"execution_ledger", projectPhase7ArtifactRef(row.value("execution_ledger", json::object()))},
            {"playbook_artifact", projectPhase7ArtifactRef(row.value("playbook_artifact", json::object()))},
            {"analysis_artifact", projectPhase7ArtifactRef(row.value("analysis_artifact", json::object()))},
            {"source_artifact", projectPhase7ArtifactRef(row.value("source_artifact", json::object()))},
            {"mode", stringValueOrEmpty(row, "mode")},
            {"has_generated_at_utc", !stringValueOrEmpty(row, "generated_at_utc").empty()},
            {"ledger_status", stringValueOrEmpty(row, "ledger_status")},
            {"entry_count", row.value("entry_count", 0ULL)},
            {"review_summary", projectPhase7ExecutionLedgerReviewSummary(row.value("review_summary", json::object()))},
            {"latest_audit_event", projectPhase7ExecutionLedgerAuditSummary(row.value("latest_audit_event", json::object()))},
            {"replay_context", projectPhase7ReplayContext(row.value("replay_context", json::object()))}
        });
    }
    std::sort(rows.begin(), rows.end(), [](const json& left, const json& right) {
        return left.value("execution_ledger", json::object()).value("artifact_id", std::string()) <
               right.value("execution_ledger", json::object()).value("artifact_id", std::string());
    });
    return {
        {"returned_count", result.value("returned_count", 0ULL)},
        {"applied_filters", projectPhase7ExecutionLedgerFilters(result.value("applied_filters", json::object()))},
        {"execution_ledgers", std::move(rows)}
    };
}

json projectPhase7ExecutionJournalSummary(const json& summary) {
    if (!summary.is_object()) {
        return json::object();
    }
    return {
        {"queued_count", summary.value("queued_count", 0ULL)},
        {"submitted_count", summary.value("submitted_count", 0ULL)},
        {"succeeded_count", summary.value("succeeded_count", 0ULL)},
        {"failed_count", summary.value("failed_count", 0ULL)},
        {"cancelled_count", summary.value("cancelled_count", 0ULL)},
        {"terminal_count", summary.value("terminal_count", 0ULL)},
        {"actionable_entry_count", summary.value("actionable_entry_count", 0ULL)},
        {"all_terminal", summary.value("all_terminal", false)}
    };
}

json projectPhase7ExecutionRecoverySummary(const json& summary) {
    if (!summary.is_object()) {
        return json::object();
    }
    return {
        {"runtime_backed_submitted_count", summary.value("runtime_backed_submitted_count", 0ULL)},
        {"stale_runtime_backed_count", summary.value("stale_runtime_backed_count", 0ULL)},
        {"recovery_required", summary.value("recovery_required", false)},
        {"stale_recovery_required", summary.value("stale_recovery_required", false)}
    };
}

json projectPhase7LatestExecutionResultSummary(const json& summary) {
    if (!summary.is_object()) {
        return nullptr;
    }
    const json executionResultSummary = summary.value("execution_result_summary", json::object());
    const json brokerIdentity = executionResultSummary.value("broker_identity", json::object());
    const json tradeTrace = executionResultSummary.value("trade_trace", json::object());
    return {
        {"entry_id_present", !stringValueOrEmpty(summary, "entry_id").empty()},
        {"execution_status", stringValueOrNull(summary, "execution_status")},
        {"terminal", summary.contains("terminal") ? summary.at("terminal") : json(nullptr)},
        {"attempt_count", summary.contains("attempt_count") ? summary.at("attempt_count") : json(nullptr)},
        {"result", {
            {"resolution", stringValueOrNull(executionResultSummary, "resolution")},
            {"fill_state", stringValueOrNull(executionResultSummary, "fill_state")},
            {"restart_resume_policy", stringValueOrNull(executionResultSummary, "restart_resume_policy")},
            {"restart_recovery_state", stringValueOrNull(executionResultSummary, "restart_recovery_state")},
            {"restart_recovery_reason", stringValueOrNull(executionResultSummary, "restart_recovery_reason")},
            {"partial_fill_before_terminal", executionResultSummary.value("partial_fill_before_terminal", false)},
            {"cancel_ack_pending", executionResultSummary.contains("cancel_ack_pending")
                                       ? executionResultSummary.at("cancel_ack_pending")
                                       : json(nullptr)},
            {"manual_review_required", executionResultSummary.value("manual_review_required", false)},
            {"broker_status_detail", stringValueOrNull(executionResultSummary, "broker_status_detail")},
            {"latest_exec_id", stringValueOrNull(executionResultSummary, "latest_exec_id")},
            {"broker_identity", {
                {"order_id", brokerIdentity.value("order_id", json(nullptr))},
                {"trace_id", brokerIdentity.value("trace_id", json(nullptr))},
                {"perm_id", brokerIdentity.value("perm_id", json(nullptr))},
                {"latest_exec_id", brokerIdentity.value("latest_exec_id", json(nullptr))},
                {"exec_id_count", brokerIdentity.value("exec_id_count", 0ULL)}
            }},
            {"trade_trace", {
                {"trace_found", tradeTrace.value("trace_found", false)},
                {"trace_id", tradeTrace.value("trace_id", json(nullptr))},
                {"perm_id", tradeTrace.value("perm_id", json(nullptr))},
                {"fill_count", tradeTrace.value("fill_count", 0ULL)},
                {"latest_status", tradeTrace.value("latest_status", json(nullptr))},
                {"terminal_status", tradeTrace.value("terminal_status", json(nullptr))}
            }}
        }}
    };
}

json projectPhase7ExecutionPolicy(const json& policy) {
    if (!policy.is_object()) {
        return json::object();
    }
    json projection{
        {"actor_required", policy.value("actor_required", false)},
        {"apply_supported", policy.value("apply_supported", false)},
        {"capability_required", stringValueOrNull(policy, "capability_required")},
        {"comment_required_statuses", policy.value("comment_required_statuses", json::array())},
        {"execution_state", {
            {"aggregate_status", stringValueOrNull(policy.value("execution_state", json::object()), "aggregate_status")},
            {"all_terminal", policy.value("execution_state", json::object()).value("all_terminal", false)},
            {"cancelled_count", policy.value("execution_state", json::object()).value("cancelled_count", 0ULL)},
            {"failed_count", policy.value("execution_state", json::object()).value("failed_count", 0ULL)},
            {"queued_count", policy.value("execution_state", json::object()).value("queued_count", 0ULL)},
            {"submitted_count", policy.value("execution_state", json::object()).value("submitted_count", 0ULL)},
            {"succeeded_count", policy.value("execution_state", json::object()).value("succeeded_count", 0ULL)}
        }},
        {"idempotency_scope", stringValueOrNull(policy, "idempotency_scope")},
        {"lifecycle_states", policy.value("lifecycle_states", json::array())},
        {"start_requires_ready_ledger", policy.value("start_requires_ready_ledger", false)},
        {"terminal_statuses", policy.value("terminal_statuses", json::array())}
    };
    if (policy.contains("apply_state") && policy.at("apply_state").is_object()) {
        projection["apply_state"] = {
            {"aggregate_status", stringValueOrNull(policy.at("apply_state"), "aggregate_status")},
            {"all_terminal", policy.at("apply_state").value("all_terminal", false)},
            {"cancelled_count", policy.at("apply_state").value("cancelled_count", 0ULL)},
            {"failed_count", policy.at("apply_state").value("failed_count", 0ULL)},
            {"queued_count", policy.at("apply_state").value("queued_count", 0ULL)},
            {"submitted_count", policy.at("apply_state").value("submitted_count", 0ULL)},
            {"succeeded_count", policy.at("apply_state").value("succeeded_count", 0ULL)}
        };
    }
    return projection;
}

json projectPhase7ExecutionJournalAuditSummary(const json& event) {
    if (!event.is_object()) {
        return json::object();
    }
    return {
        {"event_type", stringValueOrNull(event, "event_type")},
        {"has_generated_at_utc", !stringValueOrEmpty(event, "generated_at_utc").empty()},
        {"actor", stringValueOrNull(event, "actor")},
        {"execution_status", stringValueOrNull(event, "execution_status")},
        {"journal_status", stringValueOrNull(event, "journal_status")},
        {"message", stringValueOrNull(event, "message")}
    };
}

json projectPhase7ExecutionJournalEntries(const json& entries) {
    json projected = json::array();
    for (const auto& entry : entries) {
        projected.push_back({
            {"journal_entry_id_present", !stringValueOrEmpty(entry, "journal_entry_id").empty()},
            {"ledger_entry_id_present", !stringValueOrEmpty(entry, "ledger_entry_id").empty()},
            {"action_id_present", !stringValueOrEmpty(entry, "action_id").empty()},
            {"action_type", stringValueOrEmpty(entry, "action_type")},
            {"finding_id", stringValueOrNull(entry, "finding_id")},
            {"execution_status", stringValueOrEmpty(entry, "execution_status")},
            {"idempotency_key_present", !stringValueOrEmpty(entry, "idempotency_key").empty()},
            {"attempt_count", entry.value("attempt_count", 0ULL)},
            {"terminal", entry.value("terminal", false)},
            {"requires_manual_confirmation", entry.value("requires_manual_confirmation", true)},
            {"title", stringValueOrEmpty(entry, "title")},
            {"summary", stringValueOrEmpty(entry, "summary")},
            {"has_queued_at_utc", !stringValueOrEmpty(entry, "queued_at_utc").empty()},
            {"has_started_at_utc", !stringValueOrEmpty(entry, "started_at_utc").empty()},
            {"has_completed_at_utc", !stringValueOrEmpty(entry, "completed_at_utc").empty()},
            {"has_last_updated_at_utc", !stringValueOrEmpty(entry, "last_updated_at_utc").empty()},
            {"last_updated_by", stringValueOrNull(entry, "last_updated_by")},
            {"has_execution_comment", !stringValueOrEmpty(entry, "execution_comment").empty()},
            {"failure_code", stringValueOrNull(entry, "failure_code")},
            {"failure_message", stringValueOrNull(entry, "failure_message")},
            {"suggested_tools_count", entry.value("suggested_tools", json::array()).size()}
        });
    }
    std::sort(projected.begin(), projected.end(), [](const json& left, const json& right) {
        return left.value("title", std::string()) < right.value("title", std::string());
    });
    return projected;
}

json projectPhase7ExecutionJournalResult(const json& result) {
    json auditTrail = json::array();
    for (const auto& event : result.value("audit_trail", json::array())) {
        auditTrail.push_back({
            {"event_id_present", !stringValueOrEmpty(event, "event_id").empty()},
            {"event_type", stringValueOrEmpty(event, "event_type")},
            {"execution_status", stringValueOrNull(event, "execution_status")},
            {"journal_status", stringValueOrNull(event, "journal_status")},
            {"actor", stringValueOrNull(event, "actor")},
            {"updated_entry_count", event.value("updated_entry_ids", json::array()).size()},
            {"previous_entry_status_count", event.value("previous_entry_statuses", json::array()).size()},
            {"has_comment", !stringValueOrEmpty(event, "comment").empty()},
            {"has_failure_code", !stringValueOrEmpty(event, "failure_code").empty()},
            {"has_failure_message", !stringValueOrEmpty(event, "failure_message").empty()},
            {"message", stringValueOrEmpty(event, "message")},
            {"has_generated_at_utc", !stringValueOrEmpty(event, "generated_at_utc").empty()}
        });
    }
    std::sort(auditTrail.begin(), auditTrail.end(), [](const json& left, const json& right) {
        if (left.value("event_type", std::string()) != right.value("event_type", std::string())) {
            return left.value("event_type", std::string()) < right.value("event_type", std::string());
        }
        return left.value("message", std::string()) < right.value("message", std::string());
    });

    json projection{
        {"source_artifact", projectPhase7ArtifactRef(result.value("source_artifact", json::object()))},
        {"analysis_artifact", projectPhase7ArtifactRef(result.value("analysis_artifact", json::object()))},
        {"playbook_artifact", projectPhase7ArtifactRef(result.value("playbook_artifact", json::object()))},
        {"execution_ledger", projectPhase7ArtifactRef(result.value("execution_ledger", json::object()))},
        {"execution_journal", projectPhase7ArtifactRef(result.value("execution_journal", json::object()))},
        {"mode", stringValueOrEmpty(result, "mode")},
        {"has_generated_at_utc", !stringValueOrEmpty(result, "generated_at_utc").empty()},
        {"initiated_by", stringValueOrNull(result, "initiated_by")},
        {"execution_capability", stringValueOrNull(result, "execution_capability")},
        {"journal_status", stringValueOrEmpty(result, "journal_status")},
        {"execution_policy", projectPhase7ExecutionPolicy(result.value("execution_policy", json::object()))},
        {"filtered_finding_ids", result.value("filtered_finding_ids", json::array())},
        {"entry_count", result.value("entry_count", 0ULL)},
        {"execution_summary", projectPhase7ExecutionJournalSummary(result.value("execution_summary", json::object()))},
        {"runtime_recovery_summary", projectPhase7ExecutionRecoverySummary(result.value("runtime_recovery_summary", json::object()))},
        {"latest_audit_event", projectPhase7ExecutionJournalAuditSummary(result.value("latest_audit_event", json::object()))},
        {"entries", projectPhase7ExecutionJournalEntries(result.value("entries", json::array()))},
        {"audit_trail", std::move(auditTrail)},
        {"replay_context", projectPhase7ReplayContext(result.value("replay_context", json::object()))}
    };
    if (result.contains("artifact_status")) {
        projection["artifact_status"] = stringValueOrEmpty(result, "artifact_status");
    }
    if (result.contains("updated_entry_ids")) {
        projection["updated_entry_ids"] = result.value("updated_entry_ids", json::array());
    }
    if (result.contains("audit_event_id")) {
        projection["audit_event_id_present"] = !stringValueOrEmpty(result, "audit_event_id").empty();
    }
    return projection;
}

json projectPhase7ExecutionJournalInventoryResult(const json& result) {
    json rows = json::array();
    for (const auto& row : result.value("execution_journals", json::array())) {
        rows.push_back({
            {"execution_journal", projectPhase7ArtifactRef(row.value("execution_journal", json::object()))},
            {"execution_ledger", projectPhase7ArtifactRef(row.value("execution_ledger", json::object()))},
            {"playbook_artifact", projectPhase7ArtifactRef(row.value("playbook_artifact", json::object()))},
            {"analysis_artifact", projectPhase7ArtifactRef(row.value("analysis_artifact", json::object()))},
            {"source_artifact", projectPhase7ArtifactRef(row.value("source_artifact", json::object()))},
            {"mode", stringValueOrEmpty(row, "mode")},
            {"has_generated_at_utc", !stringValueOrEmpty(row, "generated_at_utc").empty()},
            {"journal_status", stringValueOrEmpty(row, "journal_status")},
            {"entry_count", row.value("entry_count", 0ULL)},
            {"execution_summary", projectPhase7ExecutionJournalSummary(row.value("execution_summary", json::object()))},
            {"runtime_recovery_summary", projectPhase7ExecutionRecoverySummary(row.value("runtime_recovery_summary", json::object()))},
            {"latest_audit_event", projectPhase7ExecutionJournalAuditSummary(row.value("latest_audit_event", json::object()))},
            {"replay_context", projectPhase7ReplayContext(row.value("replay_context", json::object()))}
        });
    }
    std::sort(rows.begin(), rows.end(), [](const json& left, const json& right) {
        return left.value("execution_journal", json::object()).value("artifact_id", std::string()) <
               right.value("execution_journal", json::object()).value("artifact_id", std::string());
    });
    return {
        {"returned_count", result.value("returned_count", 0ULL)},
        {"applied_filters", {
            {"execution_ledger_artifact_id", [&]() -> json {
                const json filters = result.value("applied_filters", json::object());
                if (!filters.contains("execution_ledger_artifact_id") || filters.at("execution_ledger_artifact_id").is_null()) {
                    return nullptr;
                }
                return normalizeArtifactId(filters.at("execution_ledger_artifact_id").get<std::string>());
            }()},
            {"playbook_artifact_id", [&]() -> json {
                const json filters = result.value("applied_filters", json::object());
                if (!filters.contains("playbook_artifact_id") || filters.at("playbook_artifact_id").is_null()) {
                    return nullptr;
                }
                return normalizeArtifactId(filters.at("playbook_artifact_id").get<std::string>());
            }()},
            {"analysis_artifact_id", [&]() -> json {
                const json filters = result.value("applied_filters", json::object());
                if (!filters.contains("analysis_artifact_id") || filters.at("analysis_artifact_id").is_null()) {
                    return nullptr;
                }
                return normalizeArtifactId(filters.at("analysis_artifact_id").get<std::string>());
            }()},
            {"source_artifact_id", [&]() -> json {
                const json filters = result.value("applied_filters", json::object());
                if (!filters.contains("source_artifact_id") || filters.at("source_artifact_id").is_null()) {
                    return nullptr;
                }
                return normalizeArtifactId(filters.at("source_artifact_id").get<std::string>());
            }()},
            {"journal_status", result.value("applied_filters", json::object()).contains("journal_status")
                                   ? result.value("applied_filters", json::object()).at("journal_status")
                                   : json(nullptr)},
            {"recovery_state", result.value("applied_filters", json::object()).contains("recovery_state")
                                   ? result.value("applied_filters", json::object()).at("recovery_state")
                                   : json(nullptr)},
            {"restart_recovery_state", result.value("applied_filters", json::object()).contains("restart_recovery_state")
                                           ? result.value("applied_filters", json::object()).at("restart_recovery_state")
                                           : json(nullptr)},
            {"sort_by", result.value("applied_filters", json::object()).contains("sort_by")
                            ? result.value("applied_filters", json::object()).at("sort_by")
                            : json("generated_at_desc")},
            {"limit", result.value("applied_filters", json::object()).contains("limit")
                          ? result.value("applied_filters", json::object()).at("limit")
                          : json(nullptr)},
            {"matched_count", result.value("applied_filters", json::object()).value("matched_count", 0ULL)}
        }},
        {"execution_journals", std::move(rows)}
    };
}

json projectPhase7ExecutionApplyAuditSummary(const json& event) {
    if (!event.is_object()) {
        return json::object();
    }
    return {
        {"event_type", stringValueOrNull(event, "event_type")},
        {"has_generated_at_utc", !stringValueOrEmpty(event, "generated_at_utc").empty()},
        {"actor", stringValueOrNull(event, "actor")},
        {"execution_status", stringValueOrNull(event, "execution_status")},
        {"apply_status", stringValueOrNull(event, "apply_status")},
        {"message", stringValueOrNull(event, "message")}
    };
}

json projectPhase7ExecutionApplyEntries(const json& entries) {
    json projected = json::array();
    for (const auto& entry : entries) {
        projected.push_back({
            {"apply_entry_id_present", !stringValueOrEmpty(entry, "apply_entry_id").empty()},
            {"journal_entry_id_present", !stringValueOrEmpty(entry, "journal_entry_id").empty()},
            {"ledger_entry_id_present", !stringValueOrEmpty(entry, "ledger_entry_id").empty()},
            {"action_id_present", !stringValueOrEmpty(entry, "action_id").empty()},
            {"action_type", stringValueOrEmpty(entry, "action_type")},
            {"finding_id", stringValueOrNull(entry, "finding_id")},
            {"execution_status", stringValueOrEmpty(entry, "execution_status")},
            {"idempotency_key_present", !stringValueOrEmpty(entry, "idempotency_key").empty()},
            {"attempt_count", entry.value("attempt_count", 0ULL)},
            {"terminal", entry.value("terminal", false)},
            {"requires_manual_confirmation", entry.value("requires_manual_confirmation", true)},
            {"title", stringValueOrEmpty(entry, "title")},
            {"summary", stringValueOrEmpty(entry, "summary")},
            {"has_submitted_at_utc", !stringValueOrEmpty(entry, "submitted_at_utc").empty()},
            {"has_completed_at_utc", !stringValueOrEmpty(entry, "completed_at_utc").empty()},
            {"has_last_updated_at_utc", !stringValueOrEmpty(entry, "last_updated_at_utc").empty()},
            {"last_updated_by", stringValueOrNull(entry, "last_updated_by")},
            {"has_execution_comment", !stringValueOrEmpty(entry, "execution_comment").empty()},
            {"failure_code", stringValueOrNull(entry, "failure_code")},
            {"failure_message", stringValueOrNull(entry, "failure_message")},
            {"suggested_tools_count", entry.value("suggested_tools", json::array()).size()}
        });
    }
    std::sort(projected.begin(), projected.end(), [](const json& left, const json& right) {
        return left.value("title", std::string()) < right.value("title", std::string());
    });
    return projected;
}

json projectPhase7ExecutionApplyResult(const json& result) {
    json auditTrail = json::array();
    for (const auto& event : result.value("audit_trail", json::array())) {
        auditTrail.push_back({
            {"event_id_present", !stringValueOrEmpty(event, "event_id").empty()},
            {"event_type", stringValueOrEmpty(event, "event_type")},
            {"execution_status", stringValueOrNull(event, "execution_status")},
            {"apply_status", stringValueOrNull(event, "apply_status")},
            {"actor", stringValueOrNull(event, "actor")},
            {"updated_entry_count", event.value("updated_entry_ids", json::array()).size()},
            {"previous_entry_status_count", event.value("previous_entry_statuses", json::array()).size()},
            {"has_comment", !stringValueOrEmpty(event, "comment").empty()},
            {"has_failure_code", !stringValueOrEmpty(event, "failure_code").empty()},
            {"has_failure_message", !stringValueOrEmpty(event, "failure_message").empty()},
            {"message", stringValueOrEmpty(event, "message")},
            {"has_generated_at_utc", !stringValueOrEmpty(event, "generated_at_utc").empty()}
        });
    }
    std::sort(auditTrail.begin(), auditTrail.end(), [](const json& left, const json& right) {
        if (left.value("event_type", std::string()) != right.value("event_type", std::string())) {
            return left.value("event_type", std::string()) < right.value("event_type", std::string());
        }
        return left.value("message", std::string()) < right.value("message", std::string());
    });

    json projection{
        {"source_artifact", projectPhase7ArtifactRef(result.value("source_artifact", json::object()))},
        {"analysis_artifact", projectPhase7ArtifactRef(result.value("analysis_artifact", json::object()))},
        {"playbook_artifact", projectPhase7ArtifactRef(result.value("playbook_artifact", json::object()))},
        {"execution_ledger", projectPhase7ArtifactRef(result.value("execution_ledger", json::object()))},
        {"execution_journal", projectPhase7ArtifactRef(result.value("execution_journal", json::object()))},
        {"execution_apply", projectPhase7ArtifactRef(result.value("execution_apply", json::object()))},
        {"mode", stringValueOrEmpty(result, "mode")},
        {"has_generated_at_utc", !stringValueOrEmpty(result, "generated_at_utc").empty()},
        {"initiated_by", stringValueOrNull(result, "initiated_by")},
        {"execution_capability", stringValueOrNull(result, "execution_capability")},
        {"apply_status", stringValueOrEmpty(result, "apply_status")},
        {"execution_policy", projectPhase7ExecutionPolicy(result.value("execution_policy", json::object()))},
        {"filtered_finding_ids", result.value("filtered_finding_ids", json::array())},
        {"entry_count", result.value("entry_count", 0ULL)},
        {"execution_summary", projectPhase7ExecutionJournalSummary(result.value("execution_summary", json::object()))},
        {"runtime_recovery_summary", projectPhase7ExecutionRecoverySummary(result.value("runtime_recovery_summary", json::object()))},
        {"latest_audit_event", projectPhase7ExecutionApplyAuditSummary(result.value("latest_audit_event", json::object()))},
        {"entries", projectPhase7ExecutionApplyEntries(result.value("entries", json::array()))},
        {"audit_trail", std::move(auditTrail)},
        {"replay_context", projectPhase7ReplayContext(result.value("replay_context", json::object()))}
    };
    if (result.contains("artifact_status")) {
        projection["artifact_status"] = stringValueOrEmpty(result, "artifact_status");
    }
    if (result.contains("updated_entry_ids")) {
        projection["updated_entry_ids"] = result.value("updated_entry_ids", json::array());
    }
    if (result.contains("audit_event_id")) {
        projection["audit_event_id_present"] = !stringValueOrEmpty(result, "audit_event_id").empty();
    }
    return projection;
}

json projectPhase7ExecutionApplyInventoryResult(const json& result) {
    json rows = json::array();
    for (const auto& row : result.value("execution_applies", json::array())) {
        rows.push_back({
            {"execution_apply", projectPhase7ArtifactRef(row.value("execution_apply", json::object()))},
            {"execution_journal", projectPhase7ArtifactRef(row.value("execution_journal", json::object()))},
            {"execution_ledger", projectPhase7ArtifactRef(row.value("execution_ledger", json::object()))},
            {"playbook_artifact", projectPhase7ArtifactRef(row.value("playbook_artifact", json::object()))},
            {"analysis_artifact", projectPhase7ArtifactRef(row.value("analysis_artifact", json::object()))},
            {"source_artifact", projectPhase7ArtifactRef(row.value("source_artifact", json::object()))},
            {"mode", stringValueOrEmpty(row, "mode")},
            {"has_generated_at_utc", !stringValueOrEmpty(row, "generated_at_utc").empty()},
            {"apply_status", stringValueOrEmpty(row, "apply_status")},
            {"entry_count", row.value("entry_count", 0ULL)},
            {"execution_summary", projectPhase7ExecutionJournalSummary(row.value("execution_summary", json::object()))},
            {"runtime_recovery_summary", projectPhase7ExecutionRecoverySummary(row.value("runtime_recovery_summary", json::object()))},
            {"latest_audit_event", projectPhase7ExecutionApplyAuditSummary(row.value("latest_audit_event", json::object()))},
            {"replay_context", projectPhase7ReplayContext(row.value("replay_context", json::object()))}
        });
    }
    std::sort(rows.begin(), rows.end(), [](const json& left, const json& right) {
        return left.value("execution_apply", json::object()).value("artifact_id", std::string()) <
               right.value("execution_apply", json::object()).value("artifact_id", std::string());
    });
    return {
        {"returned_count", result.value("returned_count", 0ULL)},
        {"applied_filters", {
            {"execution_journal_artifact_id", [&]() -> json {
                const json filters = result.value("applied_filters", json::object());
                if (!filters.contains("execution_journal_artifact_id") || filters.at("execution_journal_artifact_id").is_null()) {
                    return nullptr;
                }
                return normalizeArtifactId(filters.at("execution_journal_artifact_id").get<std::string>());
            }()},
            {"execution_ledger_artifact_id", [&]() -> json {
                const json filters = result.value("applied_filters", json::object());
                if (!filters.contains("execution_ledger_artifact_id") || filters.at("execution_ledger_artifact_id").is_null()) {
                    return nullptr;
                }
                return normalizeArtifactId(filters.at("execution_ledger_artifact_id").get<std::string>());
            }()},
            {"playbook_artifact_id", [&]() -> json {
                const json filters = result.value("applied_filters", json::object());
                if (!filters.contains("playbook_artifact_id") || filters.at("playbook_artifact_id").is_null()) {
                    return nullptr;
                }
                return normalizeArtifactId(filters.at("playbook_artifact_id").get<std::string>());
            }()},
            {"analysis_artifact_id", [&]() -> json {
                const json filters = result.value("applied_filters", json::object());
                if (!filters.contains("analysis_artifact_id") || filters.at("analysis_artifact_id").is_null()) {
                    return nullptr;
                }
                return normalizeArtifactId(filters.at("analysis_artifact_id").get<std::string>());
            }()},
            {"source_artifact_id", [&]() -> json {
                const json filters = result.value("applied_filters", json::object());
                if (!filters.contains("source_artifact_id") || filters.at("source_artifact_id").is_null()) {
                    return nullptr;
                }
                return normalizeArtifactId(filters.at("source_artifact_id").get<std::string>());
            }()},
            {"apply_status", result.value("applied_filters", json::object()).contains("apply_status")
                                 ? result.value("applied_filters", json::object()).at("apply_status")
                                 : json(nullptr)},
            {"recovery_state", result.value("applied_filters", json::object()).contains("recovery_state")
                                   ? result.value("applied_filters", json::object()).at("recovery_state")
                                   : json(nullptr)},
            {"restart_recovery_state", result.value("applied_filters", json::object()).contains("restart_recovery_state")
                                           ? result.value("applied_filters", json::object()).at("restart_recovery_state")
                                           : json(nullptr)},
            {"sort_by", result.value("applied_filters", json::object()).contains("sort_by")
                            ? result.value("applied_filters", json::object()).at("sort_by")
                            : json("generated_at_desc")},
            {"limit", result.value("applied_filters", json::object()).contains("limit")
                          ? result.value("applied_filters", json::object()).at("limit")
                          : json(nullptr)},
            {"matched_count", result.value("applied_filters", json::object()).value("matched_count", 0ULL)}
        }},
        {"execution_applies", std::move(rows)}
    };
}

json projectPhase7PlaybookInventoryResult(const json& result) {
    json rows = json::array();
    for (const auto& row : result.value("playbook_artifacts", json::array())) {
        rows.push_back({
            {"playbook_artifact", projectPhase7ArtifactRef(row.value("playbook_artifact", json::object()))},
            {"analysis_artifact", projectPhase7ArtifactRef(row.value("analysis_artifact", json::object()))},
            {"mode", stringValueOrEmpty(row, "mode")},
            {"has_generated_at_utc", !stringValueOrEmpty(row, "generated_at_utc").empty()},
            {"filtered_finding_count", row.value("filtered_finding_count", 0ULL)},
            {"planned_action_count", row.value("planned_action_count", 0ULL)},
            {"replay_context", projectPhase7ReplayContext(row.value("replay_context", json::object()))}
        });
    }
    std::sort(rows.begin(), rows.end(), [](const json& left, const json& right) {
        if (left.value("filtered_finding_count", 0ULL) != right.value("filtered_finding_count", 0ULL)) {
            return left.value("filtered_finding_count", 0ULL) < right.value("filtered_finding_count", 0ULL);
        }
        if (left.value("planned_action_count", 0ULL) != right.value("planned_action_count", 0ULL)) {
            return left.value("planned_action_count", 0ULL) < right.value("planned_action_count", 0ULL);
        }
        return left.value("playbook_artifact", json::object()).value("artifact_id", std::string()) <
               right.value("playbook_artifact", json::object()).value("artifact_id", std::string());
    });
    return {
        {"returned_count", result.value("returned_count", 0ULL)},
        {"applied_filters", projectPhase7PlaybookInventoryFilters(result.value("applied_filters", json::object()))},
        {"playbook_artifacts", std::move(rows)}
    };
}

json projectPhase8WatchDefinitionResult(const json& result) {
    return {
        {"watch_definition", projectPhase8ArtifactRef(result.value("watch_definition", json::object()))},
        {"title", stringValueOrEmpty(result, "title")},
        {"bundle_path", normalizeTestPath(result.value("bundle_path", json(nullptr)))},
        {"analysis_profile", stringValueOrEmpty(result, "analysis_profile")},
        {"enabled", result.value("enabled", false)},
        {"evaluation_cadence_minutes", result.value("evaluation_cadence_minutes", 0ULL)},
        {"minimum_finding_count", result.value("minimum_finding_count", 0ULL)},
        {"minimum_severity", stringValueOrNull(result, "minimum_severity")},
        {"required_category", stringValueOrNull(result, "required_category")},
        {"has_created_at_utc", !stringValueOrEmpty(result, "created_at_utc").empty()},
        {"has_updated_at_utc", !stringValueOrEmpty(result, "updated_at_utc").empty()},
        {"has_latest_evaluation_at_utc", !stringValueOrEmpty(result, "latest_evaluation_at_utc").empty()},
        {"has_next_evaluation_at_utc", !stringValueOrEmpty(result, "next_evaluation_at_utc").empty()},
        {"latest_trigger_outcome", stringValueOrNull(result, "latest_trigger_outcome")},
        {"latest_trigger_artifact_id", [&]() -> json {
            const std::string artifactId = stringValueOrEmpty(result, "latest_trigger_artifact_id");
            return artifactId.empty() ? json(nullptr) : json(normalizeArtifactId(artifactId));
        }()},
        {"bundle_summary", {
            {"bundle_type", result.value("bundle_summary", json::object()).value("bundle_type", std::string())},
            {"source_artifact_id", normalizeArtifactId(result.value("bundle_summary", json::object()).value("source_artifact_id", std::string()))},
            {"first_session_seq", result.value("bundle_summary", json::object()).value("first_session_seq", 0ULL)},
            {"last_session_seq", result.value("bundle_summary", json::object()).value("last_session_seq", 0ULL)}
        }}
    };
}

json projectPhase8TriggerRunResult(const json& result) {
    json categories = result.value("finding_categories", json::array());
    std::sort(categories.begin(), categories.end());
    json suppressionCodes = json::array();
    for (const auto& reason : result.value("suppression_reasons", json::array())) {
        suppressionCodes.push_back(reason.value("code", std::string()));
    }
    std::sort(suppressionCodes.begin(), suppressionCodes.end());
    return {
        {"watch_definition", projectPhase8ArtifactRef(result.value("watch_definition", json::object()))},
        {"source_artifact", projectPhase7ArtifactRef(result.value("source_artifact", json::object()))},
        {"analysis_artifact", projectPhase7ArtifactRef(result.value("analysis_artifact", json::object()))},
        {"trigger_run", projectPhase8ArtifactRef(result.value("trigger_run", json::object()))},
        {"title", stringValueOrEmpty(result, "title")},
        {"headline", stringValueOrEmpty(result, "headline")},
        {"analysis_profile", stringValueOrEmpty(result, "analysis_profile")},
        {"trigger_reason", stringValueOrEmpty(result, "trigger_reason")},
        {"trigger_outcome", stringValueOrEmpty(result, "trigger_outcome")},
        {"attention_status", stringValueOrEmpty(result, "attention_status")},
        {"attention_open", result.value("attention_open", false)},
        {"analysis_created", result.value("analysis_created", false)},
        {"finding_count", result.value("finding_count", 0ULL)},
        {"highest_severity", stringValueOrNull(result, "highest_severity")},
        {"has_generated_at_utc", !stringValueOrEmpty(result, "generated_at_utc").empty()},
        {"finding_categories", std::move(categories)},
        {"suppression_reason_codes", std::move(suppressionCodes)}
    };
}

json projectPhase8AttentionInboxResult(const json& result) {
    json rows = json::array();
    for (const auto& item : result.value("attention_items", json::array())) {
        rows.push_back({
            {"trigger_artifact_id", normalizeArtifactId(item.value("trigger_artifact_id", std::string()))},
            {"watch_artifact_id", normalizeArtifactId(item.value("watch_artifact_id", std::string()))},
            {"analysis_artifact_id", normalizeArtifactId(item.value("analysis_artifact_id", std::string()))},
            {"source_artifact_id", normalizeArtifactId(item.value("source_artifact_id", std::string()))},
            {"title", stringValueOrEmpty(item, "title")},
            {"headline", stringValueOrEmpty(item, "headline")},
            {"attention_status", stringValueOrEmpty(item, "attention_status")},
            {"attention_open", item.value("attention_open", false)},
            {"trigger_outcome", stringValueOrEmpty(item, "trigger_outcome")},
            {"highest_severity", stringValueOrNull(item, "highest_severity")},
            {"analysis_profile", stringValueOrEmpty(item, "analysis_profile")},
            {"finding_count", item.value("finding_count", 0ULL)},
            {"has_generated_at_utc", !stringValueOrEmpty(item, "generated_at_utc").empty()}
        });
    }
    std::sort(rows.begin(), rows.end(), [](const json& left, const json& right) {
        return left.value("trigger_artifact_id", std::string()) < right.value("trigger_artifact_id", std::string());
    });
    return {
        {"returned_count", result.value("returned_count", 0ULL)},
        {"attention_items", std::move(rows)}
    };
}

json projectPhase8DueWatchListResult(const json& result) {
    json rows = json::array();
    for (const auto& item : result.value("watch_definitions", json::array())) {
        rows.push_back(projectPhase8WatchDefinitionResult(item));
    }
    std::sort(rows.begin(), rows.end(), [](const json& left, const json& right) {
        if (left.value("title", std::string()) != right.value("title", std::string())) {
            return left.value("title", std::string()) < right.value("title", std::string());
        }
        return left.value("analysis_profile", std::string()) <
               right.value("analysis_profile", std::string());
    });
    return {
        {"returned_count", result.value("returned_count", 0ULL)},
        {"matched_count", result.value("matched_count", 0ULL)},
        {"watch_definitions", std::move(rows)}
    };
}

json projectPhase8RunDueWatchesResult(const json& result) {
    json rows = json::array();
    for (const auto& item : result.value("trigger_runs", json::array())) {
        rows.push_back(projectPhase8TriggerRunResult(item));
    }
    std::sort(rows.begin(), rows.end(), [](const json& left, const json& right) {
        if (left.value("title", std::string()) != right.value("title", std::string())) {
            return left.value("title", std::string()) < right.value("title", std::string());
        }
        return left.value("analysis_profile", std::string()) <
               right.value("analysis_profile", std::string());
    });
    return {
        {"trigger_reason", stringValueOrEmpty(result, "trigger_reason")},
        {"matched_watch_count", result.value("matched_watch_count", 0ULL)},
        {"evaluated_watch_count", result.value("evaluated_watch_count", 0ULL)},
        {"created_trigger_count", result.value("created_trigger_count", 0ULL)},
        {"attention_opened_count", result.value("attention_opened_count", 0ULL)},
        {"suppressed_count", result.value("suppressed_count", 0ULL)},
        {"trigger_runs", std::move(rows)}
    };
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
    std::sort(incidentKinds.begin(), incidentKinds.end());

    json citations = json::array();
    for (const auto& row : result.value("citation_rows", json::array())) {
        citations.push_back({
            {"artifact_id", normalizeArtifactId(row.value("artifact_id", std::string()))},
            {"kind", row.value("kind", row.value("type", std::string()))}
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
    return {
        {"returned_count", result.value("returned_count", 0ULL)}
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
            {"segment_count", normalizePositiveCount(result.value("segment_count", 0ULL))},
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
    } else if (toolName == "tapescript_list_analysis_profiles") {
        projection["result"] = projectPhase7AnalysisProfileListResult(result);
    } else if (toolName == "tapescript_read_analysis_profile") {
        projection["result"] = projectPhase7AnalysisProfile(result);
    } else if (toolName == "tapescript_list_session_reports") {
        projection["result"] = projectReportInventoryResult(result, "session_reports");
    } else if (toolName == "tapescript_list_case_reports") {
        projection["result"] = projectReportInventoryResult(result, "case_reports");
    } else if (toolName == "tapescript_seek_order_anchor") {
        projection["result"] = projectSeekResult(result);
    } else if (toolName == "tapescript_export_artifact") {
        projection["result"] = projectExportResult(result);
    } else if (toolName == "tapescript_analyzer_run") {
        projection["result"] = {
            {"source_artifact", projectPhase7ArtifactRef(result.value("source_artifact", json::object()))},
            {"analysis_artifact", projectPhase7ArtifactRef(result.value("analysis_artifact", json::object()))},
            {"generated_artifacts", [&]() {
                json projected = json::array();
                for (const auto& artifact : result.value("generated_artifacts", json::array())) {
                    projected.push_back(projectPhase7ArtifactRef(artifact));
                }
                return projected;
            }()},
            {"analysis_profile", stringValueOrEmpty(result, "analysis_profile")},
            {"artifact_status", stringValueOrEmpty(result, "artifact_status")},
            {"has_generated_at_utc", !stringValueOrEmpty(result, "generated_at_utc").empty()},
            {"finding_count", result.value("finding_count", 0ULL)},
            {"replay_context", projectPhase7ReplayContext(result.value("replay_context", json::object()))},
            {"findings", projectPhase7FindingRows(result.value("findings", json::array()))}
        };
    } else if (toolName == "tapescript_findings_list") {
        projection["result"] = {
            {"analysis_artifact", projectPhase7ArtifactRef(result.value("analysis_artifact", json::object()))},
            {"analysis_profile", stringValueOrEmpty(result, "analysis_profile")},
            {"has_generated_at_utc", !stringValueOrEmpty(result, "generated_at_utc").empty()},
            {"finding_count", result.value("finding_count", 0ULL)},
            {"filtered_finding_ids", result.value("filtered_finding_ids", json::array())},
            {"applied_filters", projectPhase7AppliedFilters(result.value("applied_filters", json::object()))},
            {"replay_context", projectPhase7ReplayContext(result.value("replay_context", json::object()))},
            {"findings", projectPhase7FindingRows(result.value("findings", json::array()))}
        };
    } else if (toolName == "tapescript_list_analysis_artifacts") {
        projection["result"] = projectPhase7AnalysisInventoryResult(result);
    } else if (toolName == "tapescript_read_analysis_artifact") {
        projection["result"] = {
            {"source_artifact", projectPhase7ArtifactRef(result.value("source_artifact", json::object()))},
            {"analysis_artifact", projectPhase7ArtifactRef(result.value("analysis_artifact", json::object()))},
            {"generated_artifacts", [&]() {
                json projected = json::array();
                for (const auto& artifact : result.value("generated_artifacts", json::array())) {
                    projected.push_back(projectPhase7ArtifactRef(artifact));
                }
                return projected;
            }()},
            {"analysis_profile", stringValueOrEmpty(result, "analysis_profile")},
            {"has_generated_at_utc", !stringValueOrEmpty(result, "generated_at_utc").empty()},
            {"finding_count", result.value("finding_count", 0ULL)},
            {"replay_context", projectPhase7ReplayContext(result.value("replay_context", json::object()))},
            {"findings", projectPhase7FindingRows(result.value("findings", json::array()))}
        };
    } else if (toolName == "tapescript_playbook_apply") {
        projection["result"] = {
            {"analysis_artifact", projectPhase7ArtifactRef(result.value("analysis_artifact", json::object()))},
            {"playbook_artifact", projectPhase7ArtifactRef(result.value("playbook_artifact", json::object()))},
            {"mode", stringValueOrEmpty(result, "mode")},
            {"artifact_status", stringValueOrEmpty(result, "artifact_status")},
            {"has_generated_at_utc", !stringValueOrEmpty(result, "generated_at_utc").empty()},
            {"filtered_finding_ids", result.value("filtered_finding_ids", json::array())},
            {"applied_filters", projectPhase7AppliedFilters(result.value("applied_filters", json::object()))},
            {"planned_actions", projectPhase7PlaybookActions(result.value("planned_actions", json::array()))},
            {"replay_context", projectPhase7ReplayContext(result.value("replay_context", json::object()))}
        };
    } else if (toolName == "tapescript_list_playbook_artifacts") {
        projection["result"] = projectPhase7PlaybookInventoryResult(result);
    } else if (toolName == "tapescript_read_playbook_artifact") {
        projection["result"] = {
            {"analysis_artifact", projectPhase7ArtifactRef(result.value("analysis_artifact", json::object()))},
            {"playbook_artifact", projectPhase7ArtifactRef(result.value("playbook_artifact", json::object()))},
            {"mode", result.value("mode", std::string())},
            {"has_generated_at_utc", result.value("generated_at_utc", std::string()).empty() == false},
            {"filtered_finding_ids", result.value("filtered_finding_ids", json::array())},
            {"applied_filters", projectPhase7AppliedFilters(result.value("applied_filters", json::object()))},
            {"planned_actions", projectPhase7PlaybookActions(result.value("planned_actions", json::array()))},
            {"replay_context", projectPhase7ReplayContext(result.value("replay_context", json::object()))}
        };
    } else if (toolName == "tapescript_prepare_execution_ledger" ||
               toolName == "tapescript_read_execution_ledger" ||
               toolName == "tapescript_record_execution_ledger_review") {
        projection["result"] = projectPhase7ExecutionLedgerResult(result);
    } else if (toolName == "tapescript_list_execution_ledgers") {
        projection["result"] = projectPhase7ExecutionLedgerInventoryResult(result);
    } else if (toolName == "tapescript_start_execution_journal" ||
               toolName == "tapescript_read_execution_journal" ||
               toolName == "tapescript_dispatch_execution_journal" ||
               toolName == "tapescript_record_execution_journal_event") {
        projection["result"] = projectPhase7ExecutionJournalResult(result);
    } else if (toolName == "tapescript_list_execution_journals") {
        projection["result"] = projectPhase7ExecutionJournalInventoryResult(result);
    } else if (toolName == "tapescript_start_execution_apply" ||
               toolName == "tapescript_read_execution_apply" ||
               toolName == "tapescript_record_execution_apply_event") {
        projection["result"] = projectPhase7ExecutionApplyResult(result);
    } else if (toolName == "tapescript_list_execution_applies") {
        projection["result"] = projectPhase7ExecutionApplyInventoryResult(result);
    } else if (toolName == "tapescript_create_watch_definition") {
        projection["result"] = {
            {"artifact_status", stringValueOrEmpty(result, "artifact_status")},
            {"watch_definition", projectPhase8WatchDefinitionResult(result)}
        };
    } else if (toolName == "tapescript_list_watch_definitions") {
        json rows = json::array();
        for (const auto& row : result.value("watch_definitions", json::array())) {
            rows.push_back(projectPhase8WatchDefinitionResult(row));
        }
        std::sort(rows.begin(), rows.end(), [](const json& left, const json& right) {
            if (left.value("title", std::string()) != right.value("title", std::string())) {
                return left.value("title", std::string()) < right.value("title", std::string());
            }
            return left.value("analysis_profile", std::string()) <
                   right.value("analysis_profile", std::string());
        });
        projection["result"] = {
            {"returned_count", result.value("returned_count", 0ULL)},
            {"watch_definitions", std::move(rows)}
        };
    } else if (toolName == "tapescript_list_due_watches") {
        projection["result"] = projectPhase8DueWatchListResult(result);
    } else if (toolName == "tapescript_read_watch_definition") {
        projection["result"] = projectPhase8WatchDefinitionResult(result);
    } else if (toolName == "tapescript_evaluate_watch_definition") {
        projection["result"] = {
            {"artifact_status", stringValueOrEmpty(result, "artifact_status")},
            {"trigger_run", projectPhase8TriggerRunResult(result)}
        };
    } else if (toolName == "tapescript_acknowledge_attention_item" ||
               toolName == "tapescript_snooze_attention_item" ||
               toolName == "tapescript_resolve_attention_item") {
        projection["result"] = projectPhase8TriggerRunResult(result);
    } else if (toolName == "tapescript_run_due_watches") {
        projection["result"] = projectPhase8RunDueWatchesResult(result);
    } else if (toolName == "tapescript_list_trigger_runs") {
        json rows = json::array();
        for (const auto& row : result.value("trigger_runs", json::array())) {
            rows.push_back(projectPhase8TriggerRunResult(row));
        }
        std::sort(rows.begin(), rows.end(), [](const json& left, const json& right) {
            if (left.value("title", std::string()) != right.value("title", std::string())) {
                return left.value("title", std::string()) < right.value("title", std::string());
            }
            return left.value("analysis_profile", std::string()) <
                   right.value("analysis_profile", std::string());
        });
        projection["result"] = {
            {"returned_count", result.value("returned_count", 0ULL)},
            {"trigger_runs", std::move(rows)}
        };
    } else if (toolName == "tapescript_read_trigger_run") {
        projection["result"] = projectPhase8TriggerRunResult(result);
    } else if (toolName == "tapescript_list_attention_items") {
        projection["result"] = projectPhase8AttentionInboxResult(result);
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
    configurePhase7DataDir("tape-mcp-phase5-appdata");

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

    const json enrichIncidentEnvelopeRaw = envelopeFromToolResult(
        adapter.callTool("tapescript_enrich_incident", json{{"logical_incident_id", logicalIncidentId}}));
    const json explainIncidentEnvelopeRaw = envelopeFromToolResult(
        adapter.callTool("tapescript_explain_incident", json{{"logical_incident_id", logicalIncidentId}}));
    const json enrichOrderCaseEnvelopeRaw = envelopeFromToolResult(
        adapter.callTool("tapescript_enrich_order_case", json{{"order_id", 7401}}));
    const json refreshExternalContextEnvelopeRaw = envelopeFromToolResult(
        adapter.callTool("tapescript_refresh_external_context", json{{"logical_incident_id", logicalIncidentId}}));

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
        expect(enrichIncidentEnvelopeRaw.value("ok", false),
            "phase5 enrich-incident should return ok=true\nactual:\n" + enrichIncidentEnvelopeRaw.dump(2));
        expect(explainIncidentEnvelopeRaw.value("ok", false),
            "phase5 explain-incident should return ok=true\nactual:\n" + explainIncidentEnvelopeRaw.dump(2));
        expect(enrichOrderCaseEnvelopeRaw.value("ok", false),
            "phase5 enrich-order-case should return ok=true\nactual:\n" + enrichOrderCaseEnvelopeRaw.dump(2));
        expect(refreshExternalContextEnvelopeRaw.value("ok", false),
            "phase5 refresh-external-context should return ok=true\nactual:\n" + refreshExternalContextEnvelopeRaw.dump(2));
        expect(enrichIncidentEnvelopeRaw.value("result", json::object()).value("request_kind", std::string()) == "fast_enrichment" &&
             explainIncidentEnvelopeRaw.value("result", json::object()).value("request_kind", std::string()) == "deep_enrichment" &&
             enrichOrderCaseEnvelopeRaw.value("result", json::object()).value("request_kind", std::string()) == "order_case_enrichment" &&
             refreshExternalContextEnvelopeRaw.value("result", json::object()).value("request_kind", std::string()) == "refresh_external_context",
            "phase5 enrichment tools should expose the expected request_kind values");
        expect(enrichIncidentEnvelopeRaw.value("result", json::object())
                 .value("external_context", json::object())
                 .value("provider", std::string()) == "unusual_whales" &&
             enrichIncidentEnvelopeRaw.value("result", json::object())
                 .value("external_context", json::object())
                 .value("items", json::array())
                 .is_array(),
            "phase5 enrichment tools should expose the normalized external_context object");
        expect(enrichIncidentEnvelopeRaw.value("result", json::object())
                 .value("degradation", json::object())
                 .value("code", std::string()) == "external_context_unavailable" &&
             explainIncidentEnvelopeRaw.value("result", json::object())
                 .value("degradation", json::object())
                 .value("code", std::string()) == "external_context_unavailable" &&
             enrichOrderCaseEnvelopeRaw.value("result", json::object())
                 .value("degradation", json::object())
                 .value("code", std::string()) == "external_context_unavailable" &&
             refreshExternalContextEnvelopeRaw.value("result", json::object())
                 .value("degradation", json::object())
                 .value("code", std::string()) == "external_context_unavailable",
            "phase5 enrichment tools should degrade explicitly when external connectors are not yet configured");
        expect(!enrichIncidentEnvelopeRaw.value("result", json::object())
                  .value("local_evidence", json::object())
                  .value("artifact_id", std::string())
                  .empty() &&
             refreshExternalContextEnvelopeRaw.value("result", json::object())
                 .value("cache", json::object())
                 .value("refreshed", false),
            "phase5 enrichment tools should preserve local evidence and mark refresh requests in cache metadata");
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

void testUWMcpFacetResolutionCatalog() {
    using uw_context_service::resolveUWMcpToolsForFacets;
    using uw_context_service::unresolvedUWMcpFacets;
    using uw_context_service::ProviderStep;

    const json toolCatalog = json::array({
        {
            {"name", "get_market_state"},
            {"description", "Get the latest market-wide daily options snapshot for the most recent market-open date."}
        },
        {
            {"name", "get_ticker_ohlc_latest_or_date"},
            {"description", "Get daily ticker day-state rows for one symbol, including options-flow metrics."}
        },
        {
            {"name", "get_option_trades"},
            {"description", "Get executed options trades/prints with tape-level filters, including unusual activity."}
        },
        {
            {"name", "get_flow_alerts"},
            {"description", "Screen for options flow alert hits generated by predefined unusual-activity rules."}
        },
        {
            {"name", "get_market_events"},
            {"description", "Get economic calendar events including earnings, Fed events, and macro data releases."}
        }
    });

    const auto resolution = resolveUWMcpToolsForFacets(
        toolCatalog,
        {"options_flow", "alerts", "stock_state", "news", "gex"});

    expect(resolution.selectedTools.size() == 3,
           "UW MCP facet resolution should select exactly the directly supported facet tools");
    expect(resolution.selectedTools[0].facet == "options_flow" &&
               resolution.selectedTools[0].toolName == "get_option_trades" &&
               resolution.selectedTools[0].score >= 100,
           "UW MCP facet resolution should prefer get_option_trades for options_flow");
    expect(resolution.selectedTools[1].facet == "alerts" &&
               resolution.selectedTools[1].toolName == "get_flow_alerts",
           "UW MCP facet resolution should prefer get_flow_alerts for alerts");
    expect(resolution.selectedTools[2].facet == "stock_state" &&
               resolution.selectedTools[2].toolName == "get_ticker_ohlc_latest_or_date",
           "UW MCP facet resolution should prefer get_ticker_ohlc_latest_or_date for stock_state");

    expect(std::find(resolution.unsupportedFacets.begin(), resolution.unsupportedFacets.end(), "news") !=
               resolution.unsupportedFacets.end(),
           "UW MCP facet resolution should mark news unsupported when the remote catalog has no news tool");
    expect(std::find(resolution.unsupportedFacets.begin(), resolution.unsupportedFacets.end(), "gex") !=
               resolution.unsupportedFacets.end(),
           "UW MCP facet resolution should mark gex unsupported when the remote catalog has no gex tool");

    expect(resolution.facetDiagnostics.is_array() && resolution.facetDiagnostics.size() == 5,
           "UW MCP facet resolution should emit one diagnostic row per requested facet");

    const auto findDiagnostic = [&](const std::string& facet) -> json {
        for (const auto& row : resolution.facetDiagnostics) {
            if (row.is_object() && row.value("facet", std::string()) == facet) {
                return row;
            }
        }
        return json();
    };
    const json optionsFlowDiagnostic = findDiagnostic("options_flow");
    expect(optionsFlowDiagnostic.value("supported", false) &&
               optionsFlowDiagnostic.value("selected_tool", std::string()) == "get_option_trades",
           "UW MCP facet diagnostics should record the selected options_flow tool");
    const json newsDiagnostic = findDiagnostic("news");
    expect(!newsDiagnostic.value("supported", true) &&
               newsDiagnostic.value("candidate_count", 0ULL) == 0ULL,
           "UW MCP facet diagnostics should record that news has no matching catalog tools");

    ProviderStep partialStep;
    partialStep.provider = "uw_mcp";
    partialStep.status = "ok";
    partialStep.rawRecords = json::array({
        {{"kind", "options_flow"}},
        {{"kind", "alerts"}},
        {{"kind", "stock_state"}}
    });
    partialStep.metadata = json{
        {"unsupported_facets", json::array({"news", "gex"})}
    };
    const auto unresolved = unresolvedUWMcpFacets(
        partialStep,
        {"options_flow", "alerts", "stock_state", "news", "gex"});
    expect(unresolved == std::vector<std::string>({"news", "gex"}),
           "UW MCP unresolved-facet helper should request REST only for uncovered facets on partial success");

    partialStep.rawRecords = json::array({
        {{"kind", "alerts"}},
        {{"kind", "stock_state"}}
    });
    const auto unresolvedAfterFailure = unresolvedUWMcpFacets(
        partialStep,
        {"options_flow", "alerts", "stock_state", "news", "gex"});
    expect(unresolvedAfterFailure == std::vector<std::string>({"options_flow", "news", "gex"}),
           "UW MCP unresolved-facet helper should include failed-but-requested MCP facets too");
}

void testTapeMcpPhase5WebsocketFixtureEnrichment() {
    configurePhase7DataDir("tape-mcp-phase5-ws-appdata");
    ScopedEnvVar externalContextDisabled("LONG_DISABLE_EXTERNAL_CONTEXT", std::nullopt);
    ScopedEnvVar websocketEnabled("LONG_ENABLE_UW_WEBSOCKET_CONTEXT", std::string("1"));
    ScopedEnvVar websocketFixture("LONG_UW_WS_FIXTURE_FILE",
                                  fixturePath("uw_ws_fixture_frames.jsonl").string());
    ScopedEnvVar websocketSampleMs("LONG_UW_WS_SAMPLE_MS", std::string("250"));
    ScopedEnvVar credentialFile("LONG_CREDENTIAL_FILE",
                                (testDataDir() / "missing-uw-credentials.env").string());
    ScopedEnvVar uwApiToken("UW_API_TOKEN", std::nullopt);
    ScopedEnvVar uwBearerToken("UW_BEARER_TOKEN", std::nullopt);
    ScopedEnvVar uwRestToken("UNUSUAL_WHALES_API_KEY", std::nullopt);
    ScopedEnvVar geminiApiKey("GEMINI_API_KEY", std::nullopt);

    const fs::path rootDir = testDataDir() / "tape-mcp-phase5-ws-engine";
    const fs::path socketPath = testDataDir() / "tape-mcp-phase5-ws-engine.sock";
    auto server = startPhase5Engine(rootDir, socketPath);
    seedPhase5Engine(socketPath);

    waitUntil([&]() {
        const auto snapshot = server->snapshot();
        return snapshot.segments.size() >= 2 && snapshot.latestFrozenRevisionId >= 2;
    }, "phase5 websocket MCP setup should freeze fixture batches");

    const std::uint64_t logicalIncidentId = queryFirstLogicalIncidentId(socketPath);
    expect(logicalIncidentId > 0, "phase5 websocket MCP setup should expose a logical incident id");

    tape_mcp::Adapter adapter(tape_mcp::AdapterConfig{socketPath.string()});
    const json enrichIncidentEnvelope = envelopeFromToolResult(
        adapter.callTool("tapescript_enrich_incident", json{{"logical_incident_id", logicalIncidentId}}));
    expect(enrichIncidentEnvelope.value("ok", false),
           "phase5 websocket enrich-incident should return ok=true\nactual:\n" +
               enrichIncidentEnvelope.dump(2));

    const json result = enrichIncidentEnvelope.value("result", json::object());
    const json externalContext = result.value("external_context", json::object());
    const json providerMetadata = result.value("provider_metadata", json::object());
    const json providerSteps = providerMetadata.value("provider_steps", json::array());
    const json liveCaptureSummary = result.value("live_capture_summary", json::object());
    const json items = externalContext.value("items", json::array());
    auto stringField = [](const json& object, const char* key) {
        const auto it = object.find(key);
        if (it != object.end() && it->is_string()) {
            return it->get<std::string>();
        }
        return std::string();
    };

    expect(items.is_array() && items.size() >= 4,
           "phase5 websocket enrich-incident should expose websocket external_context items\nactual:\n" +
               enrichIncidentEnvelope.dump(2));

    bool sawWebsocketItem = false;
    bool sawPriceKind = false;
    bool sawOptionsFlowKind = false;
    bool sawNewsKind = false;
    for (const auto& item : items) {
        if (stringField(item, "provider") == "uw_ws") {
            sawWebsocketItem = true;
        }
        if (stringField(item, "kind") == "stock_state") {
            sawPriceKind = true;
        } else if (stringField(item, "kind") == "options_flow") {
            sawOptionsFlowKind = true;
        } else if (stringField(item, "kind") == "news") {
            sawNewsKind = true;
        }
    }
    expect(sawWebsocketItem && sawPriceKind && sawOptionsFlowKind && sawNewsKind,
           "phase5 websocket enrich-incident should normalize websocket price, options-flow, and news records\nactual:\n" +
               enrichIncidentEnvelope.dump(2));

    bool sawWebsocketProviderStep = false;
    for (const auto& step : providerSteps) {
        if (stringField(step, "provider") == "uw_ws") {
            sawWebsocketProviderStep = stringField(step, "status") == "ok" &&
                stringField(step.value("metadata", json::object()), "source") == "fixture" &&
                step.value("metadata", json::object()).value("normalized_event_count", 0ULL) >= 4;
            break;
        }
    }
    expect(sawWebsocketProviderStep,
           "phase5 websocket enrich-incident should record an ok fixture-backed uw_ws provider step\nactual:\n" +
               enrichIncidentEnvelope.dump(2));

    expect(liveCaptureSummary.is_object() &&
               liveCaptureSummary.value("requested", false) &&
               stringField(liveCaptureSummary, "status") == "ok" &&
               stringField(liveCaptureSummary, "outcome") == "live_data" &&
               stringField(liveCaptureSummary, "source") == "fixture" &&
               liveCaptureSummary.value("has_live_data", false) &&
               liveCaptureSummary.value("data_frame_count", 0ULL) >= 4 &&
               liveCaptureSummary.value("channel_count", 0ULL) >= 1 &&
               !stringField(liveCaptureSummary, "summary_text").empty(),
           "phase5 websocket enrich-incident should expose a stable live_capture_summary\nactual:\n" +
               enrichIncidentEnvelope.dump(2));
    const json channelOutcomes = liveCaptureSummary.value("channel_outcomes", json::array());
    bool sawPriceLive = false;
    bool sawOptionsLive = false;
    bool sawNewsLive = false;
    for (const auto& channel : channelOutcomes) {
        if (!channel.is_object()) {
            continue;
        }
        if (stringField(channel, "channel") == "price:INTC" &&
            stringField(channel, "outcome") == "live_data" &&
            channel.value("data_frame_count", 0ULL) == 1ULL) {
            sawPriceLive = true;
        } else if (stringField(channel, "channel") == "option_trades:INTC" &&
                   stringField(channel, "outcome") == "live_data" &&
                   channel.value("data_frame_count", 0ULL) == 1ULL) {
            sawOptionsLive = true;
        } else if (stringField(channel, "channel") == "news" &&
                   stringField(channel, "outcome") == "live_data" &&
                   channel.value("data_frame_count", 0ULL) == 1ULL) {
            sawNewsLive = true;
        }
    }
    expect(sawPriceLive && sawOptionsLive && sawNewsLive,
           "phase5 websocket enrich-incident should expose per-channel live outcomes\nactual:\n" +
               enrichIncidentEnvelope.dump(2));

    expect(stringField(result.value("degradation", json::object()), "code") != "external_context_unavailable",
           "phase5 websocket enrich-incident should no longer degrade as external_context_unavailable");

    server->stop();
}

void testTapeMcpPhase5WebsocketJoinAckOnlyDiagnostics() {
    configurePhase7DataDir("tape-mcp-phase5-ws-join-ack-appdata");
    ScopedEnvVar externalContextDisabled("LONG_DISABLE_EXTERNAL_CONTEXT", std::nullopt);
    ScopedEnvVar websocketEnabled("LONG_ENABLE_UW_WEBSOCKET_CONTEXT", std::string("1"));
    ScopedEnvVar websocketFixture("LONG_UW_WS_FIXTURE_FILE",
                                  fixturePath("uw_ws_fixture_join_ack_only.jsonl").string());
    ScopedEnvVar websocketSampleMs("LONG_UW_WS_SAMPLE_MS", std::string("250"));
    ScopedEnvVar credentialFile("LONG_CREDENTIAL_FILE",
                                (testDataDir() / "missing-uw-credentials.env").string());
    ScopedEnvVar uwApiToken("UW_API_TOKEN", std::nullopt);
    ScopedEnvVar uwBearerToken("UW_BEARER_TOKEN", std::nullopt);
    ScopedEnvVar uwRestToken("UNUSUAL_WHALES_API_KEY", std::nullopt);
    ScopedEnvVar geminiApiKey("GEMINI_API_KEY", std::nullopt);

    const fs::path rootDir = testDataDir() / "tape-mcp-phase5-ws-join-ack-engine";
    const fs::path socketPath = testDataDir() / "tape-mcp-phase5-ws-join-ack-engine.sock";
    auto server = startPhase5Engine(rootDir, socketPath);
    seedPhase5Engine(socketPath);

    waitUntil([&]() {
        const auto snapshot = server->snapshot();
        return snapshot.segments.size() >= 2 && snapshot.latestFrozenRevisionId >= 2;
    }, "phase5 websocket join-ack setup should freeze fixture batches");

    const std::uint64_t logicalIncidentId = queryFirstLogicalIncidentId(socketPath);
    expect(logicalIncidentId > 0, "phase5 websocket join-ack setup should expose a logical incident id");

    tape_mcp::Adapter adapter(tape_mcp::AdapterConfig{socketPath.string()});
    const json refreshEnvelope = envelopeFromToolResult(
        adapter.callTool("tapescript_refresh_external_context", json{{"logical_incident_id", logicalIncidentId}}));
    expect(refreshEnvelope.value("ok", false),
           "phase5 websocket join-ack refresh-external-context should return ok=true\nactual:\n" +
               refreshEnvelope.dump(2));

    const json result = refreshEnvelope.value("result", json::object());
    const json providerSteps = result.value("provider_metadata", json::object()).value("provider_steps", json::array());
    const json liveCaptureSummary = result.value("live_capture_summary", json::object());
    auto stringField = [](const json& object, const char* key) {
        const auto it = object.find(key);
        if (it != object.end() && it->is_string()) {
            return it->get<std::string>();
        }
        return std::string();
    };

    bool sawJoinAckOnlyStep = false;
    for (const auto& step : providerSteps) {
        if (stringField(step, "provider") != "uw_ws") {
            continue;
        }
        const json metadata = step.value("metadata", json::object());
        sawJoinAckOnlyStep =
            stringField(step, "status") == "unavailable" &&
            stringField(step, "reason") == "join_ack_only" &&
            stringField(metadata, "source") == "fixture" &&
            metadata.value("join_ack_frame_count", 0ULL) >= 4 &&
            metadata.value("data_frame_count", 0ULL) == 0ULL &&
            metadata.value("raw_frame_count", 0ULL) >= 4 &&
            metadata.value("frame_previews", json::array()).is_array() &&
            !metadata.value("frame_previews", json::array()).empty();
        break;
    }
    expect(sawJoinAckOnlyStep,
           "phase5 websocket join-ack refresh-external-context should record join_ack_only diagnostics\nactual:\n" +
               refreshEnvelope.dump(2));

    expect(liveCaptureSummary.is_object() &&
               liveCaptureSummary.value("requested", false) &&
               stringField(liveCaptureSummary, "status") == "unavailable" &&
               stringField(liveCaptureSummary, "outcome") == "join_ack_only" &&
               stringField(liveCaptureSummary, "source") == "fixture" &&
               !liveCaptureSummary.value("has_live_data", false) &&
               liveCaptureSummary.value("join_ack_frame_count", 0ULL) >= 4 &&
               liveCaptureSummary.value("frame_preview_count", 0ULL) >= 1 &&
               !stringField(liveCaptureSummary, "summary_text").empty(),
           "phase5 websocket join-ack refresh-external-context should expose join_ack_only live_capture_summary\nactual:\n" +
               refreshEnvelope.dump(2));
    std::size_t joinAckChannelCount = 0;
    for (const auto& channel : liveCaptureSummary.value("channel_outcomes", json::array())) {
        if (!channel.is_object()) {
            continue;
        }
        if (stringField(channel, "outcome") == "join_ack_only" &&
            channel.value("join_ack_frame_count", 0ULL) >= 1ULL) {
            ++joinAckChannelCount;
        }
    }
    expect(joinAckChannelCount >= 3,
           "phase5 websocket join-ack refresh-external-context should expose per-channel join-ack outcomes\nactual:\n" +
               refreshEnvelope.dump(2));

    expect(stringField(result.value("degradation", json::object()), "code") == "external_context_unavailable",
           "phase5 websocket join-ack refresh-external-context should still degrade as external_context_unavailable\nactual:\n" +
               refreshEnvelope.dump(2));

    server->stop();
}

void testTapeMcpPhase5WebsocketAlreadyInRoomDiagnostics() {
    configurePhase7DataDir("tape-mcp-phase5-ws-already-in-room-appdata");
    ScopedEnvVar externalContextDisabled("LONG_DISABLE_EXTERNAL_CONTEXT", std::nullopt);
    ScopedEnvVar websocketEnabled("LONG_ENABLE_UW_WEBSOCKET_CONTEXT", std::string("1"));
    ScopedEnvVar websocketFixture("LONG_UW_WS_FIXTURE_FILE",
                                  fixturePath("uw_ws_fixture_already_in_room.jsonl").string());
    ScopedEnvVar websocketSampleMs("LONG_UW_WS_SAMPLE_MS", std::string("250"));
    ScopedEnvVar credentialFile("LONG_CREDENTIAL_FILE",
                                (testDataDir() / "missing-uw-credentials.env").string());
    ScopedEnvVar uwApiToken("UW_API_TOKEN", std::nullopt);
    ScopedEnvVar uwBearerToken("UW_BEARER_TOKEN", std::nullopt);
    ScopedEnvVar uwRestToken("UNUSUAL_WHALES_API_KEY", std::nullopt);
    ScopedEnvVar geminiApiKey("GEMINI_API_KEY", std::nullopt);

    const fs::path rootDir = testDataDir() / "tape-mcp-phase5-ws-already-in-room-engine";
    const fs::path socketPath = testDataDir() / "tape-mcp-phase5-ws-already-in-room-engine.sock";
    auto server = startPhase5Engine(rootDir, socketPath);
    seedPhase5Engine(socketPath);

    waitUntil([&]() {
        const auto snapshot = server->snapshot();
        return snapshot.segments.size() >= 2 && snapshot.latestFrozenRevisionId >= 2;
    }, "phase5 websocket already-in-room setup should freeze fixture batches");

    const std::uint64_t logicalIncidentId = queryFirstLogicalIncidentId(socketPath);
    expect(logicalIncidentId > 0, "phase5 websocket already-in-room setup should expose a logical incident id");

    tape_mcp::Adapter adapter(tape_mcp::AdapterConfig{socketPath.string()});
    const json refreshEnvelope = envelopeFromToolResult(
        adapter.callTool("tapescript_refresh_external_context", json{{"logical_incident_id", logicalIncidentId}}));
    expect(refreshEnvelope.value("ok", false),
           "phase5 websocket already-in-room refresh-external-context should return ok=true\nactual:\n" +
               refreshEnvelope.dump(2));

    const json result = refreshEnvelope.value("result", json::object());
    const json providerSteps = result.value("provider_metadata", json::object()).value("provider_steps", json::array());
    const json liveCaptureSummary = result.value("live_capture_summary", json::object());
    auto stringField = [](const json& object, const char* key) {
        const auto it = object.find(key);
        if (it != object.end() && it->is_string()) {
            return it->get<std::string>();
        }
        return std::string();
    };

    bool sawAlreadyInRoomStep = false;
    for (const auto& step : providerSteps) {
        if (stringField(step, "provider") != "uw_ws") {
            continue;
        }
        const json metadata = step.value("metadata", json::object());
        const json previews = metadata.value("frame_previews", json::array());
        sawAlreadyInRoomStep =
            stringField(step, "status") == "unavailable" &&
            stringField(step, "reason") == "already_in_room_only" &&
            stringField(metadata, "source") == "fixture" &&
            metadata.value("duplicate_join_frame_count", 0ULL) >= 2 &&
            metadata.value("error_frame_count", 0ULL) >= 2 &&
            metadata.value("unparsed_frame_count", 0ULL) == 0ULL &&
            previews.is_array() && !previews.empty() &&
            previews.front().value("event_kind", std::string()) == "duplicate_join" &&
            previews.front().value("error_message", std::string()) == "Already in room";
        break;
    }
    expect(sawAlreadyInRoomStep,
           "phase5 websocket already-in-room refresh-external-context should record already_in_room_only diagnostics\nactual:\n" +
               refreshEnvelope.dump(2));

    expect(liveCaptureSummary.is_object() &&
               liveCaptureSummary.value("requested", false) &&
               stringField(liveCaptureSummary, "status") == "unavailable" &&
               stringField(liveCaptureSummary, "outcome") == "already_in_room_only" &&
               stringField(liveCaptureSummary, "source") == "fixture" &&
               !liveCaptureSummary.value("has_live_data", false) &&
               liveCaptureSummary.value("duplicate_join_frame_count", 0ULL) >= 2 &&
               liveCaptureSummary.value("error_frame_count", 0ULL) >= 2 &&
               liveCaptureSummary.value("frame_preview_count", 0ULL) >= 1 &&
               !stringField(liveCaptureSummary, "summary_text").empty(),
           "phase5 websocket already-in-room refresh-external-context should expose already_in_room_only live_capture_summary\nactual:\n" +
               refreshEnvelope.dump(2));

    expect(stringField(result.value("degradation", json::object()), "code") == "external_context_unavailable",
           "phase5 websocket already-in-room refresh-external-context should still degrade as external_context_unavailable\nactual:\n" +
               refreshEnvelope.dump(2));

    server->stop();
}

void testTapeMcpPhase5WebsocketSymbolFiltering() {
    configurePhase7DataDir("tape-mcp-phase5-ws-symbol-filter-appdata");
    ScopedEnvVar externalContextDisabled("LONG_DISABLE_EXTERNAL_CONTEXT", std::nullopt);
    ScopedEnvVar websocketEnabled("LONG_ENABLE_UW_WEBSOCKET_CONTEXT", std::string("1"));
    ScopedEnvVar websocketFixture("LONG_UW_WS_FIXTURE_FILE",
                                  fixturePath("uw_ws_fixture_symbol_filter.jsonl").string());
    ScopedEnvVar websocketSampleMs("LONG_UW_WS_SAMPLE_MS", std::string("250"));
    ScopedEnvVar credentialFile("LONG_CREDENTIAL_FILE",
                                (testDataDir() / "missing-uw-credentials.env").string());
    ScopedEnvVar uwApiToken("UW_API_TOKEN", std::nullopt);
    ScopedEnvVar uwBearerToken("UW_BEARER_TOKEN", std::nullopt);
    ScopedEnvVar uwRestToken("UNUSUAL_WHALES_API_KEY", std::nullopt);
    ScopedEnvVar geminiApiKey("GEMINI_API_KEY", std::nullopt);

    const fs::path rootDir = testDataDir() / "tape-mcp-phase5-ws-symbol-filter-engine";
    const fs::path socketPath = testDataDir() / "tape-mcp-phase5-ws-symbol-filter-engine.sock";
    auto server = startPhase5Engine(rootDir, socketPath);
    seedPhase5Engine(socketPath);

    waitUntil([&]() {
        const auto snapshot = server->snapshot();
        return snapshot.segments.size() >= 2 && snapshot.latestFrozenRevisionId >= 2;
    }, "phase5 websocket symbol-filter setup should freeze fixture batches");

    const std::uint64_t logicalIncidentId = queryFirstLogicalIncidentId(socketPath);
    expect(logicalIncidentId > 0, "phase5 websocket symbol-filter setup should expose a logical incident id");

    tape_mcp::Adapter adapter(tape_mcp::AdapterConfig{socketPath.string()});
    const json refreshEnvelope = envelopeFromToolResult(
        adapter.callTool("tapescript_refresh_external_context", json{{"logical_incident_id", logicalIncidentId}}));
    expect(refreshEnvelope.value("ok", false),
           "phase5 websocket symbol-filter refresh-external-context should return ok=true\nactual:\n" +
               refreshEnvelope.dump(2));

    const json result = refreshEnvelope.value("result", json::object());
    const json items = result.value("external_context", json::object()).value("items", json::array());
    const json providerSteps = result.value("provider_metadata", json::object()).value("provider_steps", json::array());
    const json liveCaptureSummary = result.value("live_capture_summary", json::object());
    auto stringField = [](const json& object, const char* key) {
        const auto it = object.find(key);
        if (it != object.end() && it->is_string()) {
            return it->get<std::string>();
        }
        return std::string();
    };

    std::size_t matchingUwItems = 0;
    bool sawAlerts = false;
    bool sawNews = false;
    for (const auto& item : items) {
        if (!item.is_object() || stringField(item, "provider") != "uw_ws") {
            continue;
        }
        ++matchingUwItems;
        expect(stringField(item, "symbol") == "INTC",
               "phase5 websocket symbol-filter refresh-external-context should only keep INTC websocket records\nactual:\n" +
                   refreshEnvelope.dump(2));
        if (stringField(item, "kind") == "alerts") {
            sawAlerts = true;
        } else if (stringField(item, "kind") == "news") {
            sawNews = true;
        }
    }
    expect(matchingUwItems == 2 && sawAlerts && sawNews,
           "phase5 websocket symbol-filter refresh-external-context should keep matching alerts/news records only\nactual:\n" +
               refreshEnvelope.dump(2));

    bool sawFilteredProviderStep = false;
    for (const auto& step : providerSteps) {
        if (stringField(step, "provider") != "uw_ws") {
            continue;
        }
        const json metadata = step.value("metadata", json::object());
        sawFilteredProviderStep =
            stringField(step, "status") == "ok" &&
            metadata.value("data_frame_count", 0ULL) == 2ULL &&
            metadata.value("filtered_mismatch_frame_count", 0ULL) == 2ULL;
        break;
    }
    expect(sawFilteredProviderStep,
           "phase5 websocket symbol-filter refresh-external-context should record filtered mismatches in the uw_ws step\nactual:\n" +
               refreshEnvelope.dump(2));

    expect(liveCaptureSummary.is_object() &&
               stringField(liveCaptureSummary, "outcome") == "live_data" &&
               liveCaptureSummary.value("data_frame_count", 0ULL) == 2ULL &&
               liveCaptureSummary.value("filtered_mismatch_frame_count", 0ULL) == 2ULL,
           "phase5 websocket symbol-filter refresh-external-context should expose mismatch counts in live_capture_summary\nactual:\n" +
               refreshEnvelope.dump(2));
    bool sawFilteredFlowAlerts = false;
    bool sawFilteredNews = false;
    for (const auto& channel : liveCaptureSummary.value("channel_outcomes", json::array())) {
        if (!channel.is_object()) {
            continue;
        }
        if (stringField(channel, "channel") == "flow-alerts" &&
            stringField(channel, "outcome") == "live_data" &&
            channel.value("data_frame_count", 0ULL) == 1ULL &&
            channel.value("filtered_mismatch_frame_count", 0ULL) == 1ULL) {
            sawFilteredFlowAlerts = true;
        } else if (stringField(channel, "channel") == "news" &&
                   stringField(channel, "outcome") == "live_data" &&
                   channel.value("data_frame_count", 0ULL) == 1ULL &&
                   channel.value("filtered_mismatch_frame_count", 0ULL) == 1ULL) {
            sawFilteredNews = true;
        }
    }
    expect(sawFilteredFlowAlerts && sawFilteredNews,
           "phase5 websocket symbol-filter refresh-external-context should expose per-channel mismatch counts\nactual:\n" +
               refreshEnvelope.dump(2));

    expect(stringField(result.value("degradation", json::object()), "code") != "external_context_unavailable",
           "phase5 websocket symbol-filter refresh-external-context should still surface usable external context");

    server->stop();
}

void testTapeMcpPhase5WebsocketTargetedRetryRescue() {
    configurePhase7DataDir("tape-mcp-phase5-ws-targeted-retry-appdata");
    ScopedEnvVar externalContextDisabled("LONG_DISABLE_EXTERNAL_CONTEXT", std::nullopt);
    ScopedEnvVar websocketEnabled("LONG_ENABLE_UW_WEBSOCKET_CONTEXT", std::string("1"));
    ScopedEnvVar websocketFixture("LONG_UW_WS_FIXTURE_FILE",
                                  fixturePath("uw_ws_fixture_targeted_retry.jsonl").string());
    ScopedEnvVar websocketSampleMs("LONG_UW_WS_SAMPLE_MS", std::string("250"));
    ScopedEnvVar secondPassSampleMs("LONG_UW_WS_SECOND_PASS_SAMPLE_MS", std::string("250"));
    ScopedEnvVar secondPassTotalMs("LONG_UW_WS_SECOND_PASS_TOTAL_MS", std::string("1000"));
    ScopedEnvVar secondPassChannelLimit("LONG_UW_WS_SECOND_PASS_CHANNEL_LIMIT", std::string("2"));
    ScopedEnvVar credentialFile("LONG_CREDENTIAL_FILE",
                                (testDataDir() / "missing-uw-credentials.env").string());
    ScopedEnvVar uwApiToken("UW_API_TOKEN", std::nullopt);
    ScopedEnvVar uwBearerToken("UW_BEARER_TOKEN", std::nullopt);
    ScopedEnvVar uwRestToken("UNUSUAL_WHALES_API_KEY", std::nullopt);
    ScopedEnvVar geminiApiKey("GEMINI_API_KEY", std::nullopt);

    const fs::path rootDir = testDataDir() / "tape-mcp-phase5-ws-targeted-retry-engine";
    const fs::path socketPath = testDataDir() / "tape-mcp-phase5-ws-targeted-retry-engine.sock";
    auto server = startPhase5Engine(rootDir, socketPath);
    seedPhase5Engine(socketPath);

    waitUntil([&]() {
        const auto snapshot = server->snapshot();
        return snapshot.segments.size() >= 2 && snapshot.latestFrozenRevisionId >= 2;
    }, "phase5 websocket targeted-retry setup should freeze fixture batches");

    const std::uint64_t logicalIncidentId = queryFirstLogicalIncidentId(socketPath);
    expect(logicalIncidentId > 0, "phase5 websocket targeted-retry setup should expose a logical incident id");

    tape_mcp::Adapter adapter(tape_mcp::AdapterConfig{socketPath.string()});
    const json refreshEnvelope = envelopeFromToolResult(
        adapter.callTool("tapescript_refresh_external_context", json{{"logical_incident_id", logicalIncidentId}}));
    expect(refreshEnvelope.value("ok", false),
           "phase5 websocket targeted-retry refresh-external-context should return ok=true\nactual:\n" +
               refreshEnvelope.dump(2));

    const json result = refreshEnvelope.value("result", json::object());
    const json liveCaptureSummary = result.value("live_capture_summary", json::object());
    const json items = result.value("external_context", json::object()).value("items", json::array());
    const json providerSteps = result.value("provider_metadata", json::object()).value("provider_steps", json::array());
    auto stringField = [](const json& object, const char* key) {
        const auto it = object.find(key);
        if (it != object.end() && it->is_string()) {
            return it->get<std::string>();
        }
        return std::string();
    };

    expect(items.is_array() && items.size() == 1 &&
               stringField(items.front(), "provider") == "uw_ws" &&
               stringField(items.front(), "symbol") == "INTC" &&
               stringField(items.front(), "kind") == "news",
           "phase5 websocket targeted-retry refresh-external-context should rescue one matching news record\nactual:\n" +
               refreshEnvelope.dump(2));

    bool sawRescuedProviderStep = false;
    for (const auto& step : providerSteps) {
        if (stringField(step, "provider") != "uw_ws") {
            continue;
        }
        const json metadata = step.value("metadata", json::object());
        const json passes = metadata.value("capture_passes", json::array());
        sawRescuedProviderStep =
            stringField(step, "status") == "ok" &&
            metadata.value("data_frame_count", 0ULL) == 1ULL &&
            metadata.value("join_ack_frame_count", 0ULL) >= 5ULL &&
            metadata.value("adaptive_retry_used", false) &&
            metadata.value("rescued_by_targeted_pass", true) &&
            passes.is_array() && passes.size() == 2ULL &&
            passes.front().value("label", std::string()) == "initial" &&
            passes.back().value("label", std::string()) == "targeted:news";
        break;
    }
    expect(sawRescuedProviderStep,
           "phase5 websocket targeted-retry refresh-external-context should record initial plus targeted capture passes\nactual:\n" +
               refreshEnvelope.dump(2));

    expect(liveCaptureSummary.is_object() &&
               stringField(liveCaptureSummary, "outcome") == "live_data" &&
               liveCaptureSummary.value("targeted_retry_used", false) &&
               liveCaptureSummary.value("rescued_by_targeted_pass", true) &&
               liveCaptureSummary.value("pass_count", 0ULL) == 2ULL &&
               liveCaptureSummary.value("data_frame_count", 0ULL) == 1ULL,
           "phase5 websocket targeted-retry refresh-external-context should expose rescued live_capture_summary\nactual:\n" +
               refreshEnvelope.dump(2));

    bool sawTargetedNewsOutcome = false;
    for (const auto& channel : liveCaptureSummary.value("channel_outcomes", json::array())) {
        if (!channel.is_object()) {
            continue;
        }
        if (stringField(channel, "channel") == "news" &&
            stringField(channel, "outcome") == "live_data" &&
            channel.value("data_frame_count", 0ULL) == 1ULL &&
            channel.value("join_ack_frame_count", 0ULL) == 1ULL) {
            sawTargetedNewsOutcome = true;
            break;
        }
    }
    expect(sawTargetedNewsOutcome,
           "phase5 websocket targeted-retry refresh-external-context should expose per-channel rescue outcome\nactual:\n" +
               refreshEnvelope.dump(2));

    server->stop();
}

void testTapeMcpPhase5WebsocketAmbientGlobalOnly() {
    configurePhase7DataDir("tape-mcp-phase5-ws-ambient-global-appdata");
    ScopedEnvVar externalContextDisabled("LONG_DISABLE_EXTERNAL_CONTEXT", std::nullopt);
    ScopedEnvVar websocketEnabled("LONG_ENABLE_UW_WEBSOCKET_CONTEXT", std::string("1"));
    ScopedEnvVar websocketFixture("LONG_UW_WS_FIXTURE_FILE",
                                  fixturePath("uw_ws_fixture_ambient_global_only.jsonl").string());
    ScopedEnvVar websocketSampleMs("LONG_UW_WS_SAMPLE_MS", std::string("250"));
    ScopedEnvVar secondPassSampleMs("LONG_UW_WS_SECOND_PASS_SAMPLE_MS", std::string("250"));
    ScopedEnvVar secondPassTotalMs("LONG_UW_WS_SECOND_PASS_TOTAL_MS", std::string("1000"));
    ScopedEnvVar secondPassChannelLimit("LONG_UW_WS_SECOND_PASS_CHANNEL_LIMIT", std::string("2"));
    ScopedEnvVar credentialFile("LONG_CREDENTIAL_FILE",
                                (testDataDir() / "missing-uw-credentials.env").string());
    ScopedEnvVar uwApiToken("UW_API_TOKEN", std::nullopt);
    ScopedEnvVar uwBearerToken("UW_BEARER_TOKEN", std::nullopt);
    ScopedEnvVar uwRestToken("UNUSUAL_WHALES_API_KEY", std::nullopt);
    ScopedEnvVar geminiApiKey("GEMINI_API_KEY", std::nullopt);

    const fs::path rootDir = testDataDir() / "tape-mcp-phase5-ws-ambient-global-engine";
    const fs::path socketPath = testDataDir() / "tape-mcp-phase5-ws-ambient-global-engine.sock";
    auto server = startPhase5Engine(rootDir, socketPath);
    seedPhase5Engine(socketPath);

    waitUntil([&]() {
        const auto snapshot = server->snapshot();
        return snapshot.segments.size() >= 2 && snapshot.latestFrozenRevisionId >= 2;
    }, "phase5 websocket ambient-global setup should freeze fixture batches");

    const std::uint64_t logicalIncidentId = queryFirstLogicalIncidentId(socketPath);
    expect(logicalIncidentId > 0, "phase5 websocket ambient-global setup should expose a logical incident id");

    tape_mcp::Adapter adapter(tape_mcp::AdapterConfig{socketPath.string()});
    const json refreshEnvelope = envelopeFromToolResult(
        adapter.callTool("tapescript_refresh_external_context", json{{"logical_incident_id", logicalIncidentId}}));
    expect(refreshEnvelope.value("ok", false),
           "phase5 websocket ambient-global refresh-external-context should return ok=true\nactual:\n" +
               refreshEnvelope.dump(2));

    const json result = refreshEnvelope.value("result", json::object());
    const json liveCaptureSummary = result.value("live_capture_summary", json::object());
    const json items = result.value("external_context", json::object()).value("items", json::array());
    auto stringField = [](const json& object, const char* key) {
        const auto it = object.find(key);
        if (it != object.end() && it->is_string()) {
            return it->get<std::string>();
        }
        return std::string();
    };

    expect(items.is_array() && items.empty(),
           "phase5 websocket ambient-global refresh-external-context should not import ambient global frames into symbol-scoped items\nactual:\n" +
               refreshEnvelope.dump(2));
    expect(liveCaptureSummary.is_object() &&
               stringField(liveCaptureSummary, "outcome") == "ambient_global_only" &&
               stringField(liveCaptureSummary, "ambient_global_policy") == "diagnostic_only" &&
               !liveCaptureSummary.value("ambient_global_context_used", true) &&
               liveCaptureSummary.value("targeted_retry_used", false) &&
               !liveCaptureSummary.value("rescued_by_targeted_pass", true) &&
               liveCaptureSummary.value("ambient_global_frame_count", 0ULL) == 2ULL &&
               liveCaptureSummary.value("candidate_data_frame_count", 0ULL) == 2ULL &&
               stringField(liveCaptureSummary, "summary_text").find("diagnostic-only") != std::string::npos,
           "phase5 websocket ambient-global refresh-external-context should expose ambient global live_capture_summary\nactual:\n" +
               refreshEnvelope.dump(2));

    bool sawAmbientNewsOutcome = false;
    for (const auto& channel : liveCaptureSummary.value("channel_outcomes", json::array())) {
        if (!channel.is_object()) {
            continue;
        }
        if (stringField(channel, "channel") == "news" &&
            stringField(channel, "outcome") == "ambient_global_only" &&
            channel.value("ambient_global_frame_count", 0ULL) == 2ULL &&
            channel.value("candidate_data_frame_count", 0ULL) == 2ULL) {
            sawAmbientNewsOutcome = true;
            break;
        }
    }
    expect(sawAmbientNewsOutcome,
           "phase5 websocket ambient-global refresh-external-context should expose ambient per-channel outcome\nactual:\n" +
               refreshEnvelope.dump(2));

    expect(stringField(result.value("degradation", json::object()), "code") == "external_context_unavailable",
           "phase5 websocket ambient-global refresh-external-context should still degrade as external_context_unavailable\nactual:\n" +
               refreshEnvelope.dump(2));

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

    const fs::path corruptBundlePath = rootDir / "corrupt-case-bundle.msgpack";
    {
        std::ofstream out(corruptBundlePath, std::ios::binary);
        expect(out.is_open(), "phase6 MCP corrupt bundle fixture should open for write");
        out << "not-msgpack";
    }

    const json verifyCorruptResult = adapter.callTool("tapescript_verify_bundle", json{
        {"bundle_path", corruptBundlePath.string()}
    });
    expect(verifyCorruptResult.value("isError", false),
           "phase6 verify-bundle for a corrupt bundle should return isError=true");
    const json verifyCorruptEnvelope = envelopeFromToolResult(verifyCorruptResult);
    expect(verifyCorruptEnvelope.value("error", json::object()).value("code", std::string()) == "engine_query_failed",
           "phase6 verify-bundle for a corrupt bundle should surface engine_query_failed");
    expectContains(verifyCorruptEnvelope.value("error", json::object()).value("message", std::string()),
                   "bundle is not valid MessagePack JSON",
                   "phase6 verify-bundle for a corrupt bundle should surface the parse failure");

    const json importCorruptResult = adapter.callTool("tapescript_import_case_bundle", json{
        {"bundle_path", corruptBundlePath.string()}
    });
    expect(importCorruptResult.value("isError", false),
           "phase6 import-case-bundle for a corrupt bundle should return isError=true");
    const json importCorruptEnvelope = envelopeFromToolResult(importCorruptResult);
    expect(importCorruptEnvelope.value("error", json::object()).value("code", std::string()) == "engine_query_failed",
           "phase6 import-case-bundle for a corrupt bundle should surface engine_query_failed");
    expectContains(importCorruptEnvelope.value("error", json::object()).value("message", std::string()),
                   "bundle is not valid MessagePack JSON",
                   "phase6 import-case-bundle for a corrupt bundle should surface the parse failure");

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

void testTapeMcpPhase7Contracts() {
    configurePhase7DataDir("tape-mcp-phase7-appdata");

    const fs::path rootDir = testDataDir() / "tape-mcp-phase7-engine";
    const fs::path socketPath = testDataDir() / "tape-mcp-phase7-engine.sock";
    auto server = startPhase5Engine(rootDir, socketPath);
    seedPhase5Engine(socketPath);

    waitUntil([&]() {
        const auto snapshot = server->snapshot();
        return snapshot.segments.size() >= 2 && snapshot.latestFrozenRevisionId >= 2;
    }, "phase7 MCP setup should freeze fixture batches");

    tape_mcp::Adapter adapter(tape_mcp::AdapterConfig{socketPath.string()});

    const json scanCaseEnvelope = envelopeFromToolResult(adapter.callTool("tapescript_scan_order_case_report", json{
        {"order_id", 7401},
        {"limit", 10}
    }));
    expect(scanCaseEnvelope.value("ok", false), "phase7 scan-order-case-report should return ok=true");
    const std::uint64_t caseReportId =
        parseTrailingNumericId(scanCaseEnvelope.value("result", json::object()).value("artifact_id", std::string()),
                               "case-report:");
    expect(caseReportId > 0, "phase7 scan-order-case-report should expose a case report id");

    const json exportCaseBundleEnvelope = envelopeFromToolResult(adapter.callTool("tapescript_export_case_bundle", json{
        {"report_id", caseReportId}
    }));
    expect(exportCaseBundleEnvelope.value("ok", false), "phase7 export-case-bundle should return ok=true");
    const std::string caseBundlePath =
        exportCaseBundleEnvelope.value("result", json::object()).value("bundle", json::object()).value("bundle_path", std::string());
    expect(!caseBundlePath.empty() && fs::exists(caseBundlePath),
           "phase7 export-case-bundle should write a portable case bundle");

    const json analysisProfilesEnvelope = projectEnvelope(envelopeFromToolResult(adapter.callTool("tapescript_list_analysis_profiles", json::object())));
    expect(analysisProfilesEnvelope.value("ok", false), "phase7 list_analysis_profiles should return ok=true");
    const json readAnalysisProfileEnvelope = projectEnvelope(envelopeFromToolResult(adapter.callTool("tapescript_read_analysis_profile", json{
        {"analysis_profile", "phase7.trace_fill_integrity.v1"}
    })));
    expect(readAnalysisProfileEnvelope.value("ok", false), "phase7 read_analysis_profile should return ok=true");

    const json analyzerEnvelopeRaw = envelopeFromToolResult(adapter.callTool("tapescript_analyzer_run", json{
        {"case_bundle_path", caseBundlePath}
    }));
    const json analyzerEnvelope = projectEnvelope(analyzerEnvelopeRaw);
    expect(analyzerEnvelope.value("ok", false), "phase7 analyzer_run should return ok=true");
    const std::string analysisArtifactId =
        analyzerEnvelopeRaw.value("result", json::object()).value("analysis_artifact", json::object()).value("artifact_id", std::string());
    expect(!analysisArtifactId.empty(), "phase7 analyzer_run should produce an analysis artifact id");
    const std::string firstPhase7FindingId =
        analyzerEnvelopeRaw.value("result", json::object())
            .value("findings", json::array())
            .empty()
            ? std::string()
            : analyzerEnvelopeRaw.value("result", json::object())
                  .value("findings", json::array())
                  .front()
                  .value("finding_id", std::string());
    expect(!firstPhase7FindingId.empty(), "phase7 analyzer_run should expose at least one finding id");
    const json reusedAnalyzerEnvelopeRaw = envelopeFromToolResult(adapter.callTool("tapescript_analyzer_run", json{
        {"case_bundle_path", caseBundlePath}
    }));
    const json reusedAnalyzerEnvelope = projectEnvelope(reusedAnalyzerEnvelopeRaw);
    expect(reusedAnalyzerEnvelope.value("ok", false), "phase7 analyzer rerun should return ok=true");
    expect(reusedAnalyzerEnvelope.value("result", json::object()).value("artifact_status", std::string()) == "reused",
           "phase7 analyzer rerun should report artifact_status=reused");
    expect(reusedAnalyzerEnvelopeRaw.value("result", json::object()).value("analysis_artifact", json::object()).value("artifact_id", std::string()) ==
               analysisArtifactId,
           "phase7 analyzer rerun should preserve the deterministic analysis artifact id");

    const json analysisInventoryEnvelope = projectEnvelope(envelopeFromToolResult(adapter.callTool("tapescript_list_analysis_artifacts", json{
        {"limit", 10}
    })));
    expect(analysisInventoryEnvelope.value("ok", false), "phase7 list_analysis_artifacts should return ok=true");

    const json filteredAnalysisInventoryEnvelope = projectEnvelope(envelopeFromToolResult(adapter.callTool("tapescript_list_analysis_artifacts", json{
        {"source_artifact_id", "case-bundle:report:1"}
    })));
    expect(filteredAnalysisInventoryEnvelope.value("ok", false), "phase7 filtered list_analysis_artifacts should return ok=true");

    const json readAnalysisEnvelope = projectEnvelope(envelopeFromToolResult(adapter.callTool("tapescript_read_analysis_artifact", json{
        {"analysis_artifact_id", analysisArtifactId}
    })));
    expect(readAnalysisEnvelope.value("ok", false), "phase7 read_analysis_artifact should return ok=true");

    const json findingsEnvelope = projectEnvelope(envelopeFromToolResult(adapter.callTool("tapescript_findings_list", json{
        {"analysis_artifact_id", analysisArtifactId}
    })));
    expect(findingsEnvelope.value("ok", false), "phase7 findings_list should return ok=true");

    const json filteredFindingsEnvelope = projectEnvelope(envelopeFromToolResult(adapter.callTool("tapescript_findings_list", json{
        {"analysis_artifact_id", analysisArtifactId},
        {"category", "trace_integrity"}
    })));
    expect(filteredFindingsEnvelope.value("ok", false), "phase7 filtered findings_list should return ok=true");

    const json playbookEnvelopeRaw = envelopeFromToolResult(adapter.callTool("tapescript_playbook_apply", json{
        {"analysis_artifact_id", analysisArtifactId}
    }));
    const json playbookEnvelope = projectEnvelope(playbookEnvelopeRaw);
    expect(playbookEnvelope.value("ok", false), "phase7 playbook_apply dry_run should return ok=true");
    const std::string playbookArtifactId =
        playbookEnvelopeRaw.value("result", json::object()).value("playbook_artifact", json::object()).value("artifact_id", std::string());
    expect(!playbookArtifactId.empty(), "phase7 playbook_apply dry_run should produce a playbook artifact id");
    const json reusedPlaybookEnvelopeRaw = envelopeFromToolResult(adapter.callTool("tapescript_playbook_apply", json{
        {"analysis_artifact_id", analysisArtifactId}
    }));
    const json reusedPlaybookEnvelope = projectEnvelope(reusedPlaybookEnvelopeRaw);
    expect(reusedPlaybookEnvelope.value("ok", false), "phase7 playbook_apply rerun should return ok=true");
    expect(reusedPlaybookEnvelope.value("result", json::object()).value("artifact_status", std::string()) == "reused",
           "phase7 playbook_apply rerun should report artifact_status=reused");
    expect(reusedPlaybookEnvelopeRaw.value("result", json::object()).value("playbook_artifact", json::object()).value("artifact_id", std::string()) ==
               playbookArtifactId,
           "phase7 playbook_apply rerun should preserve the deterministic playbook artifact id");

    const json filteredPlaybookEnvelope = projectEnvelope(envelopeFromToolResult(adapter.callTool("tapescript_playbook_apply", json{
        {"analysis_artifact_id", analysisArtifactId},
        {"category", "trace_integrity"}
    })));
    expect(filteredPlaybookEnvelope.value("ok", false), "phase7 filtered playbook_apply dry_run should return ok=true");

    const json playbookInventoryEnvelope = projectEnvelope(envelopeFromToolResult(adapter.callTool("tapescript_list_playbook_artifacts", json{
        {"limit", 10}
    })));
    expect(playbookInventoryEnvelope.value("ok", false), "phase7 list_playbook_artifacts should return ok=true");

    const json filteredPlaybookInventoryEnvelope = projectEnvelope(envelopeFromToolResult(adapter.callTool("tapescript_list_playbook_artifacts", json{
        {"analysis_artifact_id", analysisArtifactId}
    })));
    expect(filteredPlaybookInventoryEnvelope.value("ok", false), "phase7 filtered list_playbook_artifacts should return ok=true");

    const json readPlaybookEnvelope = projectEnvelope(envelopeFromToolResult(adapter.callTool("tapescript_read_playbook_artifact", json{
        {"playbook_artifact_id", playbookArtifactId}
    })));
    expect(readPlaybookEnvelope.value("ok", false), "phase7 read_playbook_artifact should return ok=true");

    const json executionLedgerEnvelopeRaw = envelopeFromToolResult(adapter.callTool("tapescript_prepare_execution_ledger", json{
        {"playbook_artifact_id", playbookArtifactId}
    }));
    const json executionLedgerEnvelope = projectEnvelope(executionLedgerEnvelopeRaw);
    expect(executionLedgerEnvelope.value("ok", false), "phase7 prepare_execution_ledger should return ok=true");
    const std::string executionLedgerArtifactId =
        executionLedgerEnvelopeRaw.value("result", json::object()).value("execution_ledger", json::object()).value("artifact_id", std::string());
    expect(!executionLedgerArtifactId.empty(), "phase7 prepare_execution_ledger should produce a ledger artifact id");
    const std::string executionLedgerEntryId =
        executionLedgerEnvelopeRaw.value("result", json::object())
            .value("entries", json::array())
            .empty()
            ? std::string()
            : executionLedgerEnvelopeRaw.value("result", json::object())
                  .value("entries", json::array())
                  .front()
                  .value("entry_id", std::string());
    const std::size_t executionLedgerEntryCount =
        executionLedgerEnvelopeRaw.value("result", json::object()).value("entries", json::array()).size();
    expect(!executionLedgerEntryId.empty(),
           "phase7 prepare_execution_ledger should expose at least one review entry id");
    const std::string expectedReviewedLedgerStatus =
        executionLedgerEntryCount == 1
            ? std::string(tape_phase7::kLedgerStatusCompleted)
            : std::string(tape_phase7::kLedgerStatusInProgress);
    const json reusedExecutionLedgerEnvelope = projectEnvelope(envelopeFromToolResult(adapter.callTool("tapescript_prepare_execution_ledger", json{
        {"playbook_artifact_id", playbookArtifactId}
    })));
    expect(reusedExecutionLedgerEnvelope.value("ok", false),
           "phase7 prepare_execution_ledger rerun should return ok=true");
    expect(reusedExecutionLedgerEnvelope.value("result", json::object()).value("artifact_status", std::string()) == "reused",
           "phase7 prepare_execution_ledger rerun should report artifact_status=reused");

    const json executionLedgerInventoryEnvelope = projectEnvelope(envelopeFromToolResult(adapter.callTool("tapescript_list_execution_ledgers", json{
        {"limit", 10}
    })));
    expect(executionLedgerInventoryEnvelope.value("ok", false), "phase7 list_execution_ledgers should return ok=true");

    const json filteredExecutionLedgerInventoryEnvelope = projectEnvelope(envelopeFromToolResult(adapter.callTool("tapescript_list_execution_ledgers", json{
        {"playbook_artifact_id", playbookArtifactId}
    })));
    expect(filteredExecutionLedgerInventoryEnvelope.value("ok", false),
           "phase7 filtered list_execution_ledgers should return ok=true");

    const json readExecutionLedgerEnvelope = projectEnvelope(envelopeFromToolResult(adapter.callTool("tapescript_read_execution_ledger", json{
        {"execution_ledger_artifact_id", executionLedgerArtifactId}
    })));
    expect(readExecutionLedgerEnvelope.value("ok", false), "phase7 read_execution_ledger should return ok=true");

    const json reviewExecutionLedgerEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_record_execution_ledger_review", json{
            {"execution_ledger_artifact_id", executionLedgerArtifactId},
            {"entry_ids", json::array({executionLedgerEntryId})},
            {"review_status", "approved"},
            {"actor", "mcp-reviewer"},
            {"comment", "Reviewed through the MCP contract test."}
        })));
    expect(reviewExecutionLedgerEnvelope.value("ok", false),
           "phase7 record_execution_ledger_review should return ok=true");

    const json reviewedExecutionLedgerInventoryEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_list_execution_ledgers", json{
            {"playbook_artifact_id", playbookArtifactId},
            {"ledger_status", expectedReviewedLedgerStatus}
        })));
    expect(reviewedExecutionLedgerInventoryEnvelope.value("ok", false),
           "phase7 reviewed list_execution_ledgers should return ok=true");

    const json deferredPlaybookEnvelope = projectEnvelope(envelopeFromToolResult(adapter.callTool("tapescript_playbook_apply", json{
        {"analysis_artifact_id", analysisArtifactId},
        {"mode", "apply"}
    })));
    expect(!deferredPlaybookEnvelope.value("ok", true), "phase7 playbook_apply apply mode should return ok=false");
    expect(deferredPlaybookEnvelope.value("error", json::object()).value("code", std::string()) == "deferred_behavior",
           "phase7 playbook_apply apply mode should return deferred_behavior");

    const json invalidAnalyzerEnvelope = projectEnvelope(envelopeFromToolResult(adapter.callTool("tapescript_analyzer_run", json::object())));
    expect(!invalidAnalyzerEnvelope.value("ok", true), "phase7 analyzer_run without args should return ok=false");
    expect(invalidAnalyzerEnvelope.value("error", json::object()).value("code", std::string()) == "invalid_arguments",
           "phase7 analyzer_run without args should return invalid_arguments");
    const json unsupportedProfileEnvelope = projectEnvelope(envelopeFromToolResult(adapter.callTool("tapescript_analyzer_run", json{
        {"case_bundle_path", caseBundlePath},
        {"analysis_profile", "phase7.unimplemented_profile.v1"}
    })));
    expect(!unsupportedProfileEnvelope.value("ok", true), "phase7 analyzer_run with an unsupported profile should return ok=false");
    expect(unsupportedProfileEnvelope.value("error", json::object()).value("code", std::string()) == "unsupported_profile",
           "phase7 analyzer_run with an unsupported profile should return unsupported_profile");

    const json missingFindingsEnvelope = projectEnvelope(envelopeFromToolResult(adapter.callTool("tapescript_findings_list", json{
        {"analysis_artifact_id", "phase7-analysis-missing"}
    })));
    expect(!missingFindingsEnvelope.value("ok", true), "phase7 findings_list for a missing artifact should return ok=false");
    expect(missingFindingsEnvelope.value("error", json::object()).value("code", std::string()) == "artifact_not_found",
           "phase7 findings_list for a missing artifact should return artifact_not_found");

    const json phase7PromptList = projectPromptList(adapter.listPromptsResult());
    const json analyzeBundlePrompt = projectPromptGet(
        adapter.getPromptResult("analyze_bundle_with_phase7", json{
            {"case_bundle_path", "/tmp/example-case-bundle.msgpack"}
        }));
    const json reviewFindingsPrompt = projectPromptGet(
        adapter.getPromptResult("review_phase7_findings", json{
            {"analysis_artifact_id", "phase7-analysis-1234abcd"}
        }));
    const json reviewPlaybookPrompt = projectPromptGet(
        adapter.getPromptResult("review_phase7_playbook", json{
            {"playbook_artifact_id", "phase7-playbook-1234abcd"}
        }));
    const json reviewExecutionLedgerPrompt = projectPromptGet(
        adapter.getPromptResult("review_phase7_execution_ledger", json{
            {"execution_ledger_artifact_id", "phase7-ledger-1234abcd"}
        }));

    const json resourceListResult = adapter.listResourcesResult();
    const json phase7ResourceList = projectPhase7ResourceList(resourceListResult);
    const json analysisResource = projectResourceRead(adapter.readResourceResult("tape://phase7/analysis/" + analysisArtifactId));
    const json analysisMarkdownResource = projectResourceRead(
        adapter.readResourceResult("tape://phase7/analysis/" + analysisArtifactId + "/markdown"));
    const json playbookResource = projectResourceRead(adapter.readResourceResult("tape://phase7/playbook/" + playbookArtifactId));
    const json playbookMarkdownResource = projectResourceRead(
        adapter.readResourceResult("tape://phase7/playbook/" + playbookArtifactId + "/markdown"));
    const json executionLedgerResource = projectResourceRead(
        adapter.readResourceResult("tape://phase7/ledger/" + executionLedgerArtifactId));
    const json executionLedgerMarkdownResource = projectResourceRead(
        adapter.readResourceResult("tape://phase7/ledger/" + executionLedgerArtifactId + "/markdown"));

    const json fixture = readJsonFixture("phase7_mcp_contracts.json");
    expect(analysisProfilesEnvelope == fixture.value("tapescript_list_analysis_profiles", json::object()),
           "phase7 list_analysis_profiles envelope should match golden fixture\nactual:\n" + analysisProfilesEnvelope.dump(2));
    expect(readAnalysisProfileEnvelope == fixture.value("tapescript_read_analysis_profile", json::object()),
           "phase7 read_analysis_profile envelope should match golden fixture\nactual:\n" + readAnalysisProfileEnvelope.dump(2));
    expect(analyzerEnvelope == fixture.value("tapescript_analyzer_run", json::object()),
           "phase7 analyzer_run envelope should match golden fixture\nactual:\n" + analyzerEnvelope.dump(2));
    expect(reusedAnalyzerEnvelope == fixture.value("tapescript_analyzer_run_reused", json::object()),
           "phase7 analyzer rerun envelope should match golden fixture\nactual:\n" + reusedAnalyzerEnvelope.dump(2));
    expect(analysisInventoryEnvelope == fixture.value("tapescript_list_analysis_artifacts", json::object()),
           "phase7 list_analysis_artifacts envelope should match golden fixture\nactual:\n" + analysisInventoryEnvelope.dump(2));
    expect(filteredAnalysisInventoryEnvelope == fixture.value("tapescript_list_analysis_artifacts_filtered", json::object()),
           "phase7 filtered list_analysis_artifacts envelope should match golden fixture\nactual:\n" + filteredAnalysisInventoryEnvelope.dump(2));
    expect(readAnalysisEnvelope == fixture.value("tapescript_read_analysis_artifact", json::object()),
           "phase7 read_analysis_artifact envelope should match golden fixture\nactual:\n" + readAnalysisEnvelope.dump(2));
    expect(findingsEnvelope == fixture.value("tapescript_findings_list", json::object()),
           "phase7 findings_list envelope should match golden fixture\nactual:\n" + findingsEnvelope.dump(2));
    expect(filteredFindingsEnvelope == fixture.value("tapescript_findings_list_filtered", json::object()),
           "phase7 filtered findings_list envelope should match golden fixture\nactual:\n" + filteredFindingsEnvelope.dump(2));
    expect(playbookEnvelope == fixture.value("tapescript_playbook_apply_dry_run", json::object()),
           "phase7 playbook_apply dry_run envelope should match golden fixture\nactual:\n" + playbookEnvelope.dump(2));
    expect(reusedPlaybookEnvelope == fixture.value("tapescript_playbook_apply_reused", json::object()),
           "phase7 playbook_apply rerun envelope should match golden fixture\nactual:\n" + reusedPlaybookEnvelope.dump(2));
    expect(filteredPlaybookEnvelope == fixture.value("tapescript_playbook_apply_filtered", json::object()),
           "phase7 filtered playbook_apply envelope should match golden fixture\nactual:\n" + filteredPlaybookEnvelope.dump(2));
    expect(playbookInventoryEnvelope == fixture.value("tapescript_list_playbook_artifacts", json::object()),
           "phase7 list_playbook_artifacts envelope should match golden fixture\nactual:\n" + playbookInventoryEnvelope.dump(2));
    expect(filteredPlaybookInventoryEnvelope == fixture.value("tapescript_list_playbook_artifacts_filtered", json::object()),
           "phase7 filtered list_playbook_artifacts envelope should match golden fixture\nactual:\n" + filteredPlaybookInventoryEnvelope.dump(2));
    expect(readPlaybookEnvelope == fixture.value("tapescript_read_playbook_artifact", json::object()),
           "phase7 read_playbook_artifact envelope should match golden fixture\nactual:\n" + readPlaybookEnvelope.dump(2));
    expect(executionLedgerEnvelope == fixture.value("tapescript_prepare_execution_ledger", json::object()),
           "phase7 prepare_execution_ledger envelope should match golden fixture\nactual:\n" + executionLedgerEnvelope.dump(2));
    expect(reusedExecutionLedgerEnvelope == fixture.value("tapescript_prepare_execution_ledger_reused", json::object()),
           "phase7 prepare_execution_ledger rerun envelope should match golden fixture\nactual:\n" + reusedExecutionLedgerEnvelope.dump(2));
    expect(executionLedgerInventoryEnvelope == fixture.value("tapescript_list_execution_ledgers", json::object()),
           "phase7 list_execution_ledgers envelope should match golden fixture\nactual:\n" + executionLedgerInventoryEnvelope.dump(2));
    expect(filteredExecutionLedgerInventoryEnvelope == fixture.value("tapescript_list_execution_ledgers_filtered", json::object()),
           "phase7 filtered list_execution_ledgers envelope should match golden fixture\nactual:\n" + filteredExecutionLedgerInventoryEnvelope.dump(2));
    expect(readExecutionLedgerEnvelope == fixture.value("tapescript_read_execution_ledger", json::object()),
           "phase7 read_execution_ledger envelope should match golden fixture\nactual:\n" + readExecutionLedgerEnvelope.dump(2));
    expect(reviewExecutionLedgerEnvelope == fixture.value("tapescript_record_execution_ledger_review", json::object()),
           "phase7 record_execution_ledger_review envelope should match golden fixture\nactual:\n" + reviewExecutionLedgerEnvelope.dump(2));
    expect(reviewedExecutionLedgerInventoryEnvelope ==
               fixture.value("tapescript_list_execution_ledgers_reviewed", json::object()),
           "phase7 reviewed list_execution_ledgers envelope should match golden fixture\nactual:\n" +
               reviewedExecutionLedgerInventoryEnvelope.dump(2));
    expect(deferredPlaybookEnvelope == fixture.value("tapescript_playbook_apply_deferred", json::object()),
           "phase7 playbook_apply deferred envelope should match golden fixture\nactual:\n" + deferredPlaybookEnvelope.dump(2));
    expect(phase7PromptList == fixture.value("prompts_list_phase7", json::array()),
           "phase7 prompts/list projection should match golden fixture\nactual:\n" + phase7PromptList.dump(2));
    expect(analyzeBundlePrompt == fixture.value("prompt_analyze_bundle_with_phase7", json::object()),
           "phase7 analyze-bundle prompt should match golden fixture\nactual:\n" + analyzeBundlePrompt.dump(2));
    expect(reviewFindingsPrompt == fixture.value("prompt_review_phase7_findings", json::object()),
           "phase7 review-findings prompt should match golden fixture\nactual:\n" + reviewFindingsPrompt.dump(2));
    expect(reviewPlaybookPrompt == fixture.value("prompt_review_phase7_playbook", json::object()),
           "phase7 review-playbook prompt should match golden fixture\nactual:\n" + reviewPlaybookPrompt.dump(2));
    expect(reviewExecutionLedgerPrompt == fixture.value("prompt_review_phase7_execution_ledger", json::object()),
           "phase7 review-execution-ledger prompt should match golden fixture\nactual:\n" + reviewExecutionLedgerPrompt.dump(2));
    expect(phase7ResourceList == fixture.value("resources_list_phase7", json::array()),
           "phase7 resources/list projection should match golden fixture\nactual:\n" + phase7ResourceList.dump(2));
    expect(analysisResource == fixture.value("resource_read_phase7_analysis", json::object()),
           "phase7 analysis resource should match golden fixture\nactual:\n" + analysisResource.dump(2));
    expect(analysisMarkdownResource == fixture.value("resource_read_phase7_analysis_markdown", json::object()),
           "phase7 analysis markdown resource should match golden fixture\nactual:\n" + analysisMarkdownResource.dump(2));
    expect(playbookResource == fixture.value("resource_read_phase7_playbook", json::object()),
           "phase7 playbook resource should match golden fixture\nactual:\n" + playbookResource.dump(2));
    expect(playbookMarkdownResource == fixture.value("resource_read_phase7_playbook_markdown", json::object()),
           "phase7 playbook markdown resource should match golden fixture\nactual:\n" + playbookMarkdownResource.dump(2));
    expect(executionLedgerResource == fixture.value("resource_read_phase7_execution_ledger", json::object()),
           "phase7 execution ledger resource should match golden fixture\nactual:\n" + executionLedgerResource.dump(2));
    expect(executionLedgerMarkdownResource == fixture.value("resource_read_phase7_execution_ledger_markdown", json::object()),
           "phase7 execution ledger markdown resource should match golden fixture\nactual:\n" + executionLedgerMarkdownResource.dump(2));

    const json readIncidentTriageProfileEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_read_analysis_profile", json{
            {"analysis_profile", tape_phase7::kIncidentTriageAnalyzerProfile}
        })));
    expect(readIncidentTriageProfileEnvelope.value("ok", false),
           "phase7 read incident-triage analysis profile should return ok=true");
    const json incidentTriageAnalyzerEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_analyzer_run", json{
            {"case_bundle_path", caseBundlePath},
            {"analysis_profile", tape_phase7::kIncidentTriageAnalyzerProfile}
        })));
    expect(incidentTriageAnalyzerEnvelope.value("ok", false),
           "phase7 incident-triage analyzer_run should return ok=true");
    const json incidentTriageAnalysisInventoryEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_list_analysis_artifacts", json{
            {"analysis_profile", tape_phase7::kIncidentTriageAnalyzerProfile},
            {"source_artifact_id", "case-bundle:report:1"}
        })));
    expect(incidentTriageAnalysisInventoryEnvelope.value("ok", false),
           "phase7 incident-triage list_analysis_artifacts should return ok=true");
    expect(readIncidentTriageProfileEnvelope ==
               fixture.value("tapescript_read_analysis_profile_incident_triage", json::object()),
           "phase7 read incident-triage analysis profile envelope should match golden fixture\nactual:\n" +
               readIncidentTriageProfileEnvelope.dump(2));
    expect(incidentTriageAnalyzerEnvelope ==
               fixture.value("tapescript_analyzer_run_incident_triage", json::object()),
           "phase7 incident-triage analyzer_run envelope should match golden fixture\nactual:\n" +
               incidentTriageAnalyzerEnvelope.dump(2));
    expect(incidentTriageAnalysisInventoryEnvelope ==
               fixture.value("tapescript_list_analysis_artifacts_incident_triage", json::object()),
           "phase7 incident-triage list_analysis_artifacts envelope should match golden fixture\nactual:\n" +
               incidentTriageAnalysisInventoryEnvelope.dump(2));

    const json readFillQualityProfileEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_read_analysis_profile", json{
            {"analysis_profile", tape_phase7::kFillQualityAnalyzerProfile}
        })));
    expect(readFillQualityProfileEnvelope.value("ok", false),
           "phase7 read fill-quality analysis profile should return ok=true");
    const json fillQualityAnalyzerEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_analyzer_run", json{
            {"case_bundle_path", caseBundlePath},
            {"analysis_profile", tape_phase7::kFillQualityAnalyzerProfile}
        })));
    expect(fillQualityAnalyzerEnvelope.value("ok", false),
           "phase7 fill-quality analyzer_run should return ok=true");
    const json readLiquidityBehaviorProfileEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_read_analysis_profile", json{
            {"analysis_profile", tape_phase7::kLiquidityBehaviorAnalyzerProfile}
        })));
    expect(readLiquidityBehaviorProfileEnvelope.value("ok", false),
           "phase7 read liquidity-behavior analysis profile should return ok=true");
    const json liquidityBehaviorAnalyzerEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_analyzer_run", json{
            {"case_bundle_path", caseBundlePath},
            {"analysis_profile", tape_phase7::kLiquidityBehaviorAnalyzerProfile}
        })));
    expect(liquidityBehaviorAnalyzerEnvelope.value("ok", false),
           "phase7 liquidity-behavior analyzer_run should return ok=true");
    expect(readFillQualityProfileEnvelope ==
               fixture.value("tapescript_read_analysis_profile_fill_quality", json::object()),
           "phase7 read fill-quality analysis profile envelope should match golden fixture\nactual:\n" +
               readFillQualityProfileEnvelope.dump(2));
    expect(fillQualityAnalyzerEnvelope ==
               fixture.value("tapescript_analyzer_run_fill_quality", json::object()),
           "phase7 fill-quality analyzer_run envelope should match golden fixture\nactual:\n" +
               fillQualityAnalyzerEnvelope.dump(2));
    expect(readLiquidityBehaviorProfileEnvelope ==
               fixture.value("tapescript_read_analysis_profile_liquidity_behavior", json::object()),
           "phase7 read liquidity-behavior analysis profile envelope should match golden fixture\nactual:\n" +
               readLiquidityBehaviorProfileEnvelope.dump(2));
    expect(liquidityBehaviorAnalyzerEnvelope ==
               fixture.value("tapescript_analyzer_run_liquidity_behavior", json::object()),
           "phase7 liquidity-behavior analyzer_run envelope should match golden fixture\nactual:\n" +
               liquidityBehaviorAnalyzerEnvelope.dump(2));

    const json readAdverseSelectionProfileEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_read_analysis_profile", json{
            {"analysis_profile", tape_phase7::kAdverseSelectionAnalyzerProfile}
        })));
    expect(readAdverseSelectionProfileEnvelope.value("ok", false),
           "phase7 read adverse-selection analysis profile should return ok=true");
    const json adverseSelectionAnalyzerEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_analyzer_run", json{
            {"case_bundle_path", caseBundlePath},
            {"analysis_profile", tape_phase7::kAdverseSelectionAnalyzerProfile}
        })));
    expect(adverseSelectionAnalyzerEnvelope.value("ok", false),
           "phase7 adverse-selection analyzer_run should return ok=true");
    expect(readAdverseSelectionProfileEnvelope ==
               fixture.value("tapescript_read_analysis_profile_adverse_selection", json::object()),
           "phase7 read adverse-selection analysis profile envelope should match golden fixture\nactual:\n" +
               readAdverseSelectionProfileEnvelope.dump(2));
    expect(adverseSelectionAnalyzerEnvelope ==
               fixture.value("tapescript_analyzer_run_adverse_selection", json::object()),
           "phase7 adverse-selection analyzer_run envelope should match golden fixture\nactual:\n" +
               adverseSelectionAnalyzerEnvelope.dump(2));

    const json readOrderImpactProfileEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_read_analysis_profile", json{
            {"analysis_profile", tape_phase7::kOrderImpactAnalyzerProfile}
        })));
    expect(readOrderImpactProfileEnvelope.value("ok", false),
           "phase7 read order-impact analysis profile should return ok=true");
    const json orderImpactAnalyzerEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_analyzer_run", json{
            {"case_bundle_path", caseBundlePath},
            {"analysis_profile", tape_phase7::kOrderImpactAnalyzerProfile}
        })));
    expect(orderImpactAnalyzerEnvelope.value("ok", false),
           "phase7 order-impact analyzer_run should return ok=true");
    expect(readOrderImpactProfileEnvelope ==
               fixture.value("tapescript_read_analysis_profile_order_impact", json::object()),
           "phase7 read order-impact analysis profile envelope should match golden fixture\nactual:\n" +
               readOrderImpactProfileEnvelope.dump(2));
    expect(orderImpactAnalyzerEnvelope ==
               fixture.value("tapescript_analyzer_run_order_impact", json::object()),
           "phase7 order-impact analyzer_run envelope should match golden fixture\nactual:\n" +
               orderImpactAnalyzerEnvelope.dump(2));

    const json singleFindingPlaybookEnvelopeRaw = envelopeFromToolResult(adapter.callTool("tapescript_playbook_apply", json{
        {"analysis_artifact_id", analysisArtifactId},
        {"finding_ids", json::array({firstPhase7FindingId})}
    }));
    const json singleFindingPlaybookEnvelope = projectEnvelope(singleFindingPlaybookEnvelopeRaw);
    expect(singleFindingPlaybookEnvelope.value("ok", false),
           "phase7 single-finding playbook_apply should return ok=true");
    const std::string singleFindingPlaybookArtifactId =
        singleFindingPlaybookEnvelopeRaw.value("result", json::object()).value("playbook_artifact", json::object()).value("artifact_id", std::string());
    expect(!singleFindingPlaybookArtifactId.empty(),
           "phase7 single-finding playbook_apply should produce a playbook artifact id");
    expect(singleFindingPlaybookEnvelope ==
               fixture.value("tapescript_playbook_apply_single_finding", json::object()),
           "phase7 single-finding playbook_apply envelope should match golden fixture\nactual:\n" +
               singleFindingPlaybookEnvelope.dump(2));

    const json singleEntryLedgerEnvelopeRaw = envelopeFromToolResult(adapter.callTool("tapescript_prepare_execution_ledger", json{
        {"playbook_artifact_id", singleFindingPlaybookArtifactId}
    }));
    const json singleEntryLedgerEnvelope = projectEnvelope(singleEntryLedgerEnvelopeRaw);
    expect(singleEntryLedgerEnvelope.value("ok", false),
           "phase7 single-entry prepare_execution_ledger should return ok=true");
    const std::string singleEntryLedgerArtifactId =
        singleEntryLedgerEnvelopeRaw.value("result", json::object()).value("execution_ledger", json::object()).value("artifact_id", std::string());
    const std::string singleEntryLedgerEntryId =
        singleEntryLedgerEnvelopeRaw.value("result", json::object())
            .value("entries", json::array())
            .empty()
            ? std::string()
            : singleEntryLedgerEnvelopeRaw.value("result", json::object())
                  .value("entries", json::array())
                  .front()
                  .value("entry_id", std::string());
    expect(!singleEntryLedgerArtifactId.empty() && !singleEntryLedgerEntryId.empty(),
           "phase7 single-entry prepare_execution_ledger should expose a ledger artifact and entry id");
    expect(singleEntryLedgerEnvelope ==
               fixture.value("tapescript_prepare_execution_ledger_single_entry", json::object()),
           "phase7 single-entry prepare_execution_ledger envelope should match golden fixture\nactual:\n" +
               singleEntryLedgerEnvelope.dump(2));

    const json missingActorLedgerReviewEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_record_execution_ledger_review", json{
            {"execution_ledger_artifact_id", singleEntryLedgerArtifactId},
            {"entry_ids", json::array({singleEntryLedgerEntryId})},
            {"review_status", "approved"},
            {"comment", "Missing actor should fail."}
        })));
    expect(!missingActorLedgerReviewEnvelope.value("ok", true),
           "phase7 record_execution_ledger_review without actor should return ok=false");
    const json missingCommentLedgerReviewEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_record_execution_ledger_review", json{
            {"execution_ledger_artifact_id", singleEntryLedgerArtifactId},
            {"entry_ids", json::array({singleEntryLedgerEntryId})},
            {"review_status", "blocked"},
            {"actor", "mcp-reviewer-a"}
        })));
    expect(!missingCommentLedgerReviewEnvelope.value("ok", true),
           "phase7 blocked record_execution_ledger_review without comment should return ok=false");
    expect(missingActorLedgerReviewEnvelope ==
               fixture.value("tapescript_record_execution_ledger_review_missing_actor", json::object()),
           "phase7 record_execution_ledger_review missing-actor envelope should match golden fixture\nactual:\n" +
               missingActorLedgerReviewEnvelope.dump(2));
    expect(missingCommentLedgerReviewEnvelope ==
               fixture.value("tapescript_record_execution_ledger_review_missing_comment", json::object()),
           "phase7 record_execution_ledger_review missing-comment envelope should match golden fixture\nactual:\n" +
               missingCommentLedgerReviewEnvelope.dump(2));

    const json waitingApprovalLedgerEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_record_execution_ledger_review", json{
            {"execution_ledger_artifact_id", singleEntryLedgerArtifactId},
            {"entry_ids", json::array({singleEntryLedgerEntryId})},
            {"review_status", "approved"},
            {"actor", "mcp-reviewer-a"},
            {"comment", "First approval through the MCP contract test."}
        })));
    expect(waitingApprovalLedgerEnvelope.value("ok", false),
           "phase7 first single-entry approval should return ok=true");
    const json readyExecutionLedgerEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_record_execution_ledger_review", json{
            {"execution_ledger_artifact_id", singleEntryLedgerArtifactId},
            {"entry_ids", json::array({singleEntryLedgerEntryId})},
            {"review_status", "approved"},
            {"actor", "mcp-reviewer-b"},
            {"comment", "Second approval satisfies the review threshold."}
        })));
    expect(readyExecutionLedgerEnvelope.value("ok", false),
           "phase7 second single-entry approval should return ok=true");
    const json readyExecutionLedgerInventoryEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_list_execution_ledgers", json{
            {"playbook_artifact_id", singleFindingPlaybookArtifactId},
            {"ledger_status", tape_phase7::kLedgerStatusReadyForExecution}
        })));
    expect(readyExecutionLedgerInventoryEnvelope.value("ok", false),
           "phase7 ready-for-execution list_execution_ledgers should return ok=true");
    expect(waitingApprovalLedgerEnvelope ==
               fixture.value("tapescript_record_execution_ledger_review_waiting_approval", json::object()),
           "phase7 first single-entry approval envelope should match golden fixture\nactual:\n" +
               waitingApprovalLedgerEnvelope.dump(2));
    expect(readyExecutionLedgerEnvelope ==
               fixture.value("tapescript_record_execution_ledger_review_ready", json::object()),
           "phase7 second single-entry approval envelope should match golden fixture\nactual:\n" +
               readyExecutionLedgerEnvelope.dump(2));
    expect(readyExecutionLedgerInventoryEnvelope ==
               fixture.value("tapescript_list_execution_ledgers_ready", json::object()),
           "phase7 ready-for-execution list_execution_ledgers envelope should match golden fixture\nactual:\n" +
               readyExecutionLedgerInventoryEnvelope.dump(2));

    const json dispatchAnalysisEnvelopeRaw = envelopeFromToolResult(adapter.callTool("tapescript_analyzer_run", json{
        {"case_bundle_path", caseBundlePath},
        {"analysis_profile", tape_phase7::kOrderImpactAnalyzerProfile}
    }));
    expect(dispatchAnalysisEnvelopeRaw.value("ok", false),
           "phase7 dispatch-specific analyzer_run should return ok=true");
    const std::string orderImpactAnalysisArtifactId =
        dispatchAnalysisEnvelopeRaw.value("result", json::object())
            .value("analysis_artifact", json::object())
            .value("artifact_id", std::string());
    const std::string orderImpactFindingId =
        dispatchAnalysisEnvelopeRaw.value("result", json::object())
            .value("findings", json::array())
            .empty()
            ? std::string()
            : dispatchAnalysisEnvelopeRaw.value("result", json::object())
                  .value("findings", json::array())
                  .front()
                  .value("finding_id", std::string());
    expect(!orderImpactAnalysisArtifactId.empty() && !orderImpactFindingId.empty(),
           "phase7 order-impact analyzer should expose a reusable artifact id and finding id");
    const json dispatchPlaybookEnvelopeRaw = envelopeFromToolResult(adapter.callTool("tapescript_playbook_apply", json{
        {"analysis_artifact_id", orderImpactAnalysisArtifactId},
        {"finding_ids", json::array({orderImpactFindingId})}
    }));
    expect(dispatchPlaybookEnvelopeRaw.value("ok", false),
           "phase7 dispatch-specific playbook_apply should return ok=true");
    const std::string dispatchPlaybookArtifactId =
        dispatchPlaybookEnvelopeRaw.value("result", json::object()).value("playbook_artifact", json::object()).value("artifact_id", std::string());
    expect(!dispatchPlaybookArtifactId.empty(),
           "phase7 dispatch-specific playbook_apply should expose a playbook artifact id");
    const json dispatchLedgerEnvelopeRaw = envelopeFromToolResult(adapter.callTool("tapescript_prepare_execution_ledger", json{
        {"playbook_artifact_id", dispatchPlaybookArtifactId}
    }));
    expect(dispatchLedgerEnvelopeRaw.value("ok", false),
           "phase7 dispatch-specific prepare_execution_ledger should return ok=true");
    const std::string dispatchLedgerArtifactId =
        dispatchLedgerEnvelopeRaw.value("result", json::object()).value("execution_ledger", json::object()).value("artifact_id", std::string());
    const std::string dispatchLedgerEntryId =
        dispatchLedgerEnvelopeRaw.value("result", json::object()).value("entries", json::array()).empty()
            ? std::string()
            : dispatchLedgerEnvelopeRaw.value("result", json::object()).value("entries", json::array()).front().value("entry_id", std::string());
    expect(!dispatchLedgerArtifactId.empty() && !dispatchLedgerEntryId.empty(),
           "phase7 dispatch-specific prepare_execution_ledger should expose a ledger artifact and entry id");
    const json dispatchLedgerFirstApprovalEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_record_execution_ledger_review", json{
            {"execution_ledger_artifact_id", dispatchLedgerArtifactId},
            {"entry_ids", json::array({dispatchLedgerEntryId})},
            {"review_status", "approved"},
            {"actor", "mcp-dispatch-reviewer-a"},
            {"comment", "First approval for dispatch-specific journal coverage."}
        })));
    expect(dispatchLedgerFirstApprovalEnvelope.value("ok", false),
           "phase7 dispatch-specific first ledger approval should return ok=true");
    const json dispatchLedgerReadyEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_record_execution_ledger_review", json{
            {"execution_ledger_artifact_id", dispatchLedgerArtifactId},
            {"entry_ids", json::array({dispatchLedgerEntryId})},
            {"review_status", "approved"},
            {"actor", "mcp-dispatch-reviewer-b"},
            {"comment", "Second approval prepares the journal for dispatch."}
        })));
    expect(dispatchLedgerReadyEnvelope.value("ok", false),
           "phase7 dispatch-specific second ledger approval should return ok=true");
    const json dispatchJournalEnvelopeRaw = envelopeFromToolResult(adapter.callTool("tapescript_start_execution_journal", json{
        {"execution_ledger_artifact_id", dispatchLedgerArtifactId},
        {"actor", "mcp-executor-dispatch"},
        {"execution_capability", "phase7.execution_operator.v1"}
    }));
    expect(dispatchJournalEnvelopeRaw.value("ok", false),
           "phase7 dispatch-specific start_execution_journal should return ok=true");
    const std::string dispatchJournalArtifactId =
        dispatchJournalEnvelopeRaw.value("result", json::object()).value("execution_journal", json::object()).value("artifact_id", std::string());
    const std::string dispatchRequiredCapability =
        dispatchJournalEnvelopeRaw.value("result", json::object())
            .value("execution_policy", json::object())
            .value("capability_required", std::string());
    expect(!dispatchJournalArtifactId.empty(),
           "phase7 dispatch-specific start_execution_journal should expose a journal artifact id");
    expect(!dispatchRequiredCapability.empty(),
           "phase7 dispatch-specific start_execution_journal should expose a required execution capability");
    const json missingCapabilityDispatchEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_dispatch_execution_journal", json{
            {"execution_journal_artifact_id", dispatchJournalArtifactId},
            {"actor", "mcp-executor-dispatch"},
            {"comment", "Missing capability should fail."}
        })));
    expect(!missingCapabilityDispatchEnvelope.value("ok", true),
           "phase7 dispatch_execution_journal without execution_capability should return ok=false");
    const json dispatchedExecutionJournalEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_dispatch_execution_journal", json{
            {"execution_journal_artifact_id", dispatchJournalArtifactId},
            {"actor", "mcp-executor-dispatch"},
            {"execution_capability", dispatchRequiredCapability},
            {"comment", "Dispatching queued entries through the controlled path."}
        })));
    expect(dispatchedExecutionJournalEnvelope.value("ok", false),
           "phase7 dispatch_execution_journal should return ok=true");

    const json executionJournalEnvelopeRaw = envelopeFromToolResult(adapter.callTool("tapescript_start_execution_journal", json{
        {"execution_ledger_artifact_id", singleEntryLedgerArtifactId},
        {"actor", "mcp-executor-a"},
        {"execution_capability", "phase7.execution_operator.v1"}
    }));
    const json executionJournalEnvelope = projectEnvelope(executionJournalEnvelopeRaw);
    expect(executionJournalEnvelope.value("ok", false),
           "phase7 start_execution_journal should return ok=true\nactual:\n" + executionJournalEnvelope.dump(2));
    const std::string executionJournalArtifactId =
        executionJournalEnvelopeRaw.value("result", json::object()).value("execution_journal", json::object()).value("artifact_id", std::string());
    const std::string executionJournalEntryId =
        executionJournalEnvelopeRaw.value("result", json::object()).value("entries", json::array()).empty()
            ? std::string()
            : executionJournalEnvelopeRaw.value("result", json::object()).value("entries", json::array()).front().value("journal_entry_id", std::string());
    expect(!executionJournalArtifactId.empty() && !executionJournalEntryId.empty(),
           "phase7 start_execution_journal should expose a journal artifact and entry id");
    const json reusedExecutionJournalEnvelope = projectEnvelope(envelopeFromToolResult(adapter.callTool("tapescript_start_execution_journal", json{
        {"execution_ledger_artifact_id", singleEntryLedgerArtifactId},
        {"actor", "mcp-executor-a"},
        {"execution_capability", "phase7.execution_operator.v1"}
    })));
    expect(reusedExecutionJournalEnvelope.value("ok", false),
           "phase7 start_execution_journal rerun should return ok=true\nactual:\n" + reusedExecutionJournalEnvelope.dump(2));
    const json executionJournalInventoryEnvelope = projectEnvelope(envelopeFromToolResult(adapter.callTool("tapescript_list_execution_journals", json{
        {"execution_ledger_artifact_id", singleEntryLedgerArtifactId},
        {"limit", 10}
    })));
    expect(executionJournalInventoryEnvelope.value("ok", false),
           "phase7 list_execution_journals should return ok=true\nactual:\n" + executionJournalInventoryEnvelope.dump(2));
    const json recoveryExecutionJournalInventoryEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_list_execution_journals", json{
            {"execution_ledger_artifact_id", dispatchLedgerArtifactId},
            {"recovery_state", "recovery_required"},
            {"sort_by", "attention_desc"},
            {"limit", 10}
        })));
    expect(recoveryExecutionJournalInventoryEnvelope.value("ok", false),
           "phase7 recovery list_execution_journals should return ok=true\nactual:\n" +
               recoveryExecutionJournalInventoryEnvelope.dump(2));
    const json readExecutionJournalEnvelope = projectEnvelope(envelopeFromToolResult(adapter.callTool("tapescript_read_execution_journal", json{
        {"execution_journal_artifact_id", executionJournalArtifactId}
    })));
    expect(readExecutionJournalEnvelope.value("ok", false),
           "phase7 read_execution_journal should return ok=true\nactual:\n" + readExecutionJournalEnvelope.dump(2));
    const json missingActorExecutionJournalEventEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_record_execution_journal_event", json{
            {"execution_journal_artifact_id", executionJournalArtifactId},
            {"entry_ids", json::array({executionJournalEntryId})},
            {"execution_status", "submitted"},
            {"comment", "Missing actor should fail."}
        })));
    expect(!missingActorExecutionJournalEventEnvelope.value("ok", true),
           "phase7 record_execution_journal_event without actor should return ok=false");
    const json missingFailureDetailsExecutionJournalEventEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_record_execution_journal_event", json{
            {"execution_journal_artifact_id", executionJournalArtifactId},
            {"entry_ids", json::array({executionJournalEntryId})},
            {"execution_status", "failed"},
            {"actor", "mcp-executor-a"},
            {"comment", "Failed execution requires details."}
        })));
    expect(!missingFailureDetailsExecutionJournalEventEnvelope.value("ok", true),
           "phase7 failed record_execution_journal_event without failure details should return ok=false");
    const json submittedExecutionJournalEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_record_execution_journal_event", json{
            {"execution_journal_artifact_id", executionJournalArtifactId},
            {"entry_ids", json::array({executionJournalEntryId})},
            {"execution_status", "submitted"},
            {"actor", "mcp-executor-a"},
            {"comment", "Submitting the first execution attempt."}
        })));
    expect(submittedExecutionJournalEnvelope.value("ok", false),
           "phase7 submitted record_execution_journal_event should return ok=true");
    const json succeededExecutionJournalEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_record_execution_journal_event", json{
            {"execution_journal_artifact_id", executionJournalArtifactId},
            {"entry_ids", json::array({executionJournalEntryId})},
            {"execution_status", "succeeded"},
            {"actor", "mcp-executor-a"},
            {"comment", "Execution completed successfully."}
        })));
    expect(succeededExecutionJournalEnvelope.value("ok", false),
           "phase7 succeeded record_execution_journal_event should return ok=true");
    const json succeededExecutionJournalInventoryEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_list_execution_journals", json{
            {"execution_ledger_artifact_id", singleEntryLedgerArtifactId},
            {"journal_status", tape_phase7::kExecutionJournalStatusSucceeded}
        })));
    expect(succeededExecutionJournalInventoryEnvelope.value("ok", false),
           "phase7 succeeded list_execution_journals should return ok=true");
    const json executionApplyEnvelopeRaw = envelopeFromToolResult(adapter.callTool("tapescript_start_execution_apply", json{
        {"execution_journal_artifact_id", dispatchJournalArtifactId},
        {"actor", "mcp-executor-dispatch"},
        {"execution_capability", dispatchRequiredCapability},
        {"comment", "Creating a controlled apply artifact from submitted journal entries."}
    }));
    const json executionApplyEnvelope = projectEnvelope(executionApplyEnvelopeRaw);
    expect(executionApplyEnvelope.value("ok", false),
           "phase7 start_execution_apply should return ok=true\nactual:\n" + executionApplyEnvelope.dump(2));
    const std::string executionApplyArtifactId =
        executionApplyEnvelopeRaw.value("result", json::object()).value("execution_apply", json::object()).value("artifact_id", std::string());
    const std::string executionApplyEntryId =
        executionApplyEnvelopeRaw.value("result", json::object()).value("entries", json::array()).empty()
            ? std::string()
            : executionApplyEnvelopeRaw.value("result", json::object()).value("entries", json::array()).front().value("apply_entry_id", std::string());
    expect(!executionApplyArtifactId.empty() && !executionApplyEntryId.empty(),
           "phase7 start_execution_apply should expose an execution-apply artifact and entry id");
    const json reusedExecutionApplyEnvelope = projectEnvelope(envelopeFromToolResult(adapter.callTool("tapescript_start_execution_apply", json{
        {"execution_journal_artifact_id", dispatchJournalArtifactId},
        {"actor", "mcp-executor-dispatch"},
        {"execution_capability", dispatchRequiredCapability}
    })));
    expect(reusedExecutionApplyEnvelope.value("ok", false),
           "phase7 start_execution_apply rerun should return ok=true\nactual:\n" + reusedExecutionApplyEnvelope.dump(2));
    const json executionApplyInventoryEnvelope = projectEnvelope(envelopeFromToolResult(adapter.callTool("tapescript_list_execution_applies", json{
        {"execution_journal_artifact_id", dispatchJournalArtifactId},
        {"limit", 10}
    })));
    expect(executionApplyInventoryEnvelope.value("ok", false),
           "phase7 list_execution_applies should return ok=true\nactual:\n" + executionApplyInventoryEnvelope.dump(2));
    const json recoveryExecutionApplyInventoryEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_list_execution_applies", json{
            {"execution_journal_artifact_id", dispatchJournalArtifactId},
            {"recovery_state", "recovery_required"},
            {"sort_by", "attention_desc"},
            {"limit", 10}
        })));
    expect(recoveryExecutionApplyInventoryEnvelope.value("ok", false),
           "phase7 recovery list_execution_applies should return ok=true\nactual:\n" +
               recoveryExecutionApplyInventoryEnvelope.dump(2));
    const json readExecutionApplyEnvelope = projectEnvelope(envelopeFromToolResult(adapter.callTool("tapescript_read_execution_apply", json{
        {"execution_apply_artifact_id", executionApplyArtifactId}
    })));
    expect(readExecutionApplyEnvelope.value("ok", false),
           "phase7 read_execution_apply should return ok=true\nactual:\n" + readExecutionApplyEnvelope.dump(2));
    const json missingFailureDetailsExecutionApplyEventEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_record_execution_apply_event", json{
            {"execution_apply_artifact_id", executionApplyArtifactId},
            {"entry_ids", json::array({executionApplyEntryId})},
            {"execution_status", "failed"},
            {"actor", "mcp-executor-dispatch"},
            {"comment", "Failed apply requires details."}
        })));
    expect(!missingFailureDetailsExecutionApplyEventEnvelope.value("ok", true),
           "phase7 failed record_execution_apply_event without failure details should return ok=false");
    const json succeededExecutionApplyEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_record_execution_apply_event", json{
            {"execution_apply_artifact_id", executionApplyArtifactId},
            {"entry_ids", json::array({executionApplyEntryId})},
            {"execution_status", "succeeded"},
            {"actor", "mcp-executor-dispatch"},
            {"comment", "Apply completed successfully."}
        })));
    expect(succeededExecutionApplyEnvelope.value("ok", false),
           "phase7 succeeded record_execution_apply_event should return ok=true");
    const json succeededExecutionApplyInventoryEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_list_execution_applies", json{
            {"execution_journal_artifact_id", dispatchJournalArtifactId},
            {"apply_status", tape_phase7::kExecutionJournalStatusSucceeded}
        })));
    expect(succeededExecutionApplyInventoryEnvelope.value("ok", false),
           "phase7 succeeded list_execution_applies should return ok=true");
    const json executionJournalResource = projectResourceRead(
        adapter.readResourceResult("tape://phase7/journal/" + executionJournalArtifactId));
    const json executionJournalMarkdownResource = projectResourceRead(
        adapter.readResourceResult("tape://phase7/journal/" + executionJournalArtifactId + "/markdown"));
    const json executionApplyResource = projectResourceRead(
        adapter.readResourceResult("tape://phase7/apply/" + executionApplyArtifactId));
    const json executionApplyMarkdownResource = projectResourceRead(
        adapter.readResourceResult("tape://phase7/apply/" + executionApplyArtifactId + "/markdown"));
    expect(executionJournalEnvelope ==
               fixture.value("tapescript_start_execution_journal", json::object()),
           "phase7 start_execution_journal envelope should match golden fixture\nactual:\n" +
               executionJournalEnvelope.dump(2));
    expect(reusedExecutionJournalEnvelope ==
               fixture.value("tapescript_start_execution_journal_reused", json::object()),
           "phase7 start_execution_journal rerun envelope should match golden fixture\nactual:\n" +
               reusedExecutionJournalEnvelope.dump(2));
    expect(executionJournalInventoryEnvelope ==
               fixture.value("tapescript_list_execution_journals", json::object()),
           "phase7 list_execution_journals envelope should match golden fixture\nactual:\n" +
               executionJournalInventoryEnvelope.dump(2));
    expect(recoveryExecutionJournalInventoryEnvelope ==
               fixture.value("tapescript_list_execution_journals_recovery_required", json::object()),
           "phase7 recovery list_execution_journals envelope should match golden fixture\nactual:\n" +
               recoveryExecutionJournalInventoryEnvelope.dump(2));
    expect(readExecutionJournalEnvelope ==
               fixture.value("tapescript_read_execution_journal", json::object()),
           "phase7 read_execution_journal envelope should match golden fixture\nactual:\n" +
               readExecutionJournalEnvelope.dump(2));
    expect(missingCapabilityDispatchEnvelope ==
               fixture.value("tapescript_dispatch_execution_journal_missing_capability", json::object()),
           "phase7 dispatch_execution_journal missing-capability envelope should match golden fixture\nactual:\n" +
               missingCapabilityDispatchEnvelope.dump(2));
    expect(dispatchedExecutionJournalEnvelope ==
               fixture.value("tapescript_dispatch_execution_journal", json::object()),
           "phase7 dispatch_execution_journal envelope should match golden fixture\nactual:\n" +
               dispatchedExecutionJournalEnvelope.dump(2));
    expect(missingActorExecutionJournalEventEnvelope ==
               fixture.value("tapescript_record_execution_journal_event_missing_actor", json::object()),
           "phase7 record_execution_journal_event missing-actor envelope should match golden fixture\nactual:\n" +
               missingActorExecutionJournalEventEnvelope.dump(2));
    expect(missingFailureDetailsExecutionJournalEventEnvelope ==
               fixture.value("tapescript_record_execution_journal_event_missing_failure_details", json::object()),
           "phase7 record_execution_journal_event missing-failure-details envelope should match golden fixture\nactual:\n" +
               missingFailureDetailsExecutionJournalEventEnvelope.dump(2));
    expect(submittedExecutionJournalEnvelope ==
               fixture.value("tapescript_record_execution_journal_event_submitted", json::object()),
           "phase7 record_execution_journal_event submitted envelope should match golden fixture\nactual:\n" +
               submittedExecutionJournalEnvelope.dump(2));
    expect(succeededExecutionJournalEnvelope ==
               fixture.value("tapescript_record_execution_journal_event_succeeded", json::object()),
           "phase7 record_execution_journal_event succeeded envelope should match golden fixture\nactual:\n" +
               succeededExecutionJournalEnvelope.dump(2));
    expect(succeededExecutionJournalInventoryEnvelope ==
               fixture.value("tapescript_list_execution_journals_succeeded", json::object()),
           "phase7 succeeded list_execution_journals envelope should match golden fixture\nactual:\n" +
               succeededExecutionJournalInventoryEnvelope.dump(2));
    expect(executionApplyEnvelope ==
               fixture.value("tapescript_start_execution_apply", json::object()),
           "phase7 start_execution_apply envelope should match golden fixture\nactual:\n" +
               executionApplyEnvelope.dump(2));
    expect(reusedExecutionApplyEnvelope ==
               fixture.value("tapescript_start_execution_apply_reused", json::object()),
           "phase7 start_execution_apply rerun envelope should match golden fixture\nactual:\n" +
               reusedExecutionApplyEnvelope.dump(2));
    expect(executionApplyInventoryEnvelope ==
               fixture.value("tapescript_list_execution_applies", json::object()),
           "phase7 list_execution_applies envelope should match golden fixture\nactual:\n" +
               executionApplyInventoryEnvelope.dump(2));
    expect(recoveryExecutionApplyInventoryEnvelope ==
               fixture.value("tapescript_list_execution_applies_recovery_required", json::object()),
           "phase7 recovery list_execution_applies envelope should match golden fixture\nactual:\n" +
               recoveryExecutionApplyInventoryEnvelope.dump(2));
    expect(readExecutionApplyEnvelope ==
               fixture.value("tapescript_read_execution_apply", json::object()),
           "phase7 read_execution_apply envelope should match golden fixture\nactual:\n" +
               readExecutionApplyEnvelope.dump(2));
    expect(missingFailureDetailsExecutionApplyEventEnvelope ==
               fixture.value("tapescript_record_execution_apply_event_missing_failure_details", json::object()),
           "phase7 record_execution_apply_event missing-failure-details envelope should match golden fixture\nactual:\n" +
               missingFailureDetailsExecutionApplyEventEnvelope.dump(2));
    expect(succeededExecutionApplyEnvelope ==
               fixture.value("tapescript_record_execution_apply_event", json::object()),
           "phase7 record_execution_apply_event envelope should match golden fixture\nactual:\n" +
               succeededExecutionApplyEnvelope.dump(2));
    expect(succeededExecutionApplyInventoryEnvelope ==
               fixture.value("tapescript_list_execution_applies_succeeded", json::object()),
           "phase7 succeeded list_execution_applies envelope should match golden fixture\nactual:\n" +
               succeededExecutionApplyInventoryEnvelope.dump(2));
    expect(executionJournalResource ==
               fixture.value("resource_read_phase7_execution_journal", json::object()),
           "phase7 execution journal resource should match golden fixture\nactual:\n" +
               executionJournalResource.dump(2));
    expect(executionJournalMarkdownResource ==
               fixture.value("resource_read_phase7_execution_journal_markdown", json::object()),
           "phase7 execution journal markdown resource should match golden fixture\nactual:\n" +
               executionJournalMarkdownResource.dump(2));
    expect(executionApplyResource ==
               fixture.value("resource_read_phase7_execution_apply", json::object()),
           "phase7 execution apply resource should match golden fixture\nactual:\n" +
               executionApplyResource.dump(2));
    expect(executionApplyMarkdownResource ==
               fixture.value("resource_read_phase7_execution_apply_markdown", json::object()),
           "phase7 execution apply markdown resource should match golden fixture\nactual:\n" +
               executionApplyMarkdownResource.dump(2));

    auto phase7InventoryRowByArtifactId = [](const json& envelope,
                                             const char* rowKey,
                                             const char* artifactKey,
                                             const std::string& artifactId) -> json {
        const json rows = envelope.value("result", json::object()).value(rowKey, json::array());
        for (const auto& row : rows) {
            if (row.value(artifactKey, json::object()).value("artifact_id", std::string()) == artifactId) {
                return row;
            }
        }
        return json(nullptr);
    };

    std::string runtimeBridgeCode;
    std::string runtimeBridgeMessage;
    tape_phase7::ExecutionJournalArtifact runtimeBridgeJournal;
    expect(tape_phase7::loadExecutionJournalArtifact({},
                                                     dispatchJournalArtifactId,
                                                     &runtimeBridgeJournal,
                                                     &runtimeBridgeCode,
                                                     &runtimeBridgeMessage),
           "phase7 runtime bridge should load the dispatch journal for focused MCP projection: " +
               runtimeBridgeCode + " " + runtimeBridgeMessage);
    expect(!runtimeBridgeJournal.entries.empty() &&
               runtimeBridgeJournal.entries.front().executionRequest.value("requested_order_ids", json::array()).is_array() &&
               !runtimeBridgeJournal.entries.front().executionRequest.value("requested_order_ids", json::array()).empty(),
           "phase7 runtime bridge focused MCP projection should have a runtime-backed journal entry");
    const std::string runtimeJournalEntryId = runtimeBridgeJournal.entries.front().journalEntryId;
    expect(!runtimeJournalEntryId.empty(),
           "phase7 runtime bridge focused MCP projection should expose a journal entry id");
    const OrderId runtimeOrderId = static_cast<OrderId>(
        runtimeBridgeJournal.entries.front().executionRequest.value("requested_order_ids", json::array()).front().get<long long>());
    constexpr std::uint64_t runtimeTraceId = 97401;
    constexpr long long runtimePermId = 57401;
    {
        SharedData& state = appState();
        std::lock_guard<std::recursive_mutex> lock(state.mutex);
        auto& order = state.orders[runtimeOrderId];
        order.orderId = runtimeOrderId;
        order.symbol = "INTC";
        order.side = "BUY";
        order.account = "DU900001";
        order.quantity = 30.0;
        order.limitPrice = 46.11;
        order.status = "Submitted";
        order.filledQty = 10.0;
        order.remainingQty = 20.0;
        order.avgFillPrice = 46.11;
        order.localState = LocalOrderState::PartiallyFilled;
        order.seenExecIds.clear();
        order.seenExecIds.insert("E-PHASE7-MCP-1");

        TradeTrace trace;
        trace.traceId = runtimeTraceId;
        trace.orderId = runtimeOrderId;
        trace.permId = runtimePermId;
        trace.source = "phase7-mcp-runtime-bridge";
        trace.symbol = order.symbol;
        trace.side = order.side;
        trace.account = order.account;
        trace.requestedQty = static_cast<int>(order.quantity);
        trace.limitPrice = order.limitPrice;
        trace.latestStatus = "Submitted";
        trace.terminalStatus.clear();
        FillSlice firstFill;
        firstFill.execId = "E-PHASE7-MCP-1";
        firstFill.shares = 10;
        firstFill.price = 46.11;
        firstFill.cumQty = 10.0;
        firstFill.avgPrice = 46.11;
        firstFill.exchange = "SIM";
        trace.fills.push_back(firstFill);
        state.traces[trace.traceId] = trace;
        state.traceRecency.push_back(trace.traceId);
        state.latestTraceId = trace.traceId;
        state.traceIdByOrderId[runtimeOrderId] = trace.traceId;
        state.traceIdByPermId[runtimePermId] = trace.traceId;
        state.traceIdByExecId[firstFill.execId] = trace.traceId;
    }
    publishSharedDataSnapshot();

    tape_phase7::ExecutionJournalArtifact recoverableRuntimeJournal;
    std::vector<std::string> recoverableRuntimeJournalEntryIds;
    std::string recoverableRuntimeJournalAuditEventId;
    expect(tape_phase7::reconcileExecutionJournalEntriesViaRuntime(nullptr,
                                                                   {},
                                                                   dispatchJournalArtifactId,
                                                                   {runtimeJournalEntryId},
                                                                   "mcp-runtime-bridge",
                                                                   "Runtime recovered a live partial fill.",
                                                                   &recoverableRuntimeJournal,
                                                                   &recoverableRuntimeJournalEntryIds,
                                                                   &recoverableRuntimeJournalAuditEventId,
                                                                   &runtimeBridgeCode,
                                                                   &runtimeBridgeMessage),
           "phase7 runtime bridge partial-fill reconciliation for focused MCP projection should succeed: " +
               runtimeBridgeCode + " " + runtimeBridgeMessage);

    tape_phase7::ExecutionApplyArtifact recoverableRuntimeApply;
    std::vector<std::string> recoverableRuntimeApplyEntryIds;
    std::string recoverableRuntimeApplyAuditEventId;
    expect(tape_phase7::synchronizeExecutionApplyFromJournal({},
                                                             executionApplyArtifactId,
                                                             "mcp-runtime-bridge",
                                                             "Mirror the recoverable runtime reconciliation into apply.",
                                                             &recoverableRuntimeApply,
                                                             &recoverableRuntimeApplyEntryIds,
                                                             &recoverableRuntimeApplyAuditEventId,
                                                             &runtimeBridgeCode,
                                                             &runtimeBridgeMessage),
           "phase7 runtime bridge apply synchronization for focused MCP projection should succeed: " +
               runtimeBridgeCode + " " + runtimeBridgeMessage);

    const json runtimeRecoverableReadJournalRaw = envelopeFromToolResult(adapter.callTool("tapescript_read_execution_journal", json{
        {"execution_journal_artifact_id", dispatchJournalArtifactId}
    }));
    expect(runtimeRecoverableReadJournalRaw.value("ok", false),
           "phase7 runtime bridge focused read_execution_journal should return ok=true");
    const json runtimeRecoverableJournalInventoryRaw = envelopeFromToolResult(adapter.callTool("tapescript_list_execution_journals", json{
        {"execution_ledger_artifact_id", dispatchLedgerArtifactId},
        {"limit", 10}
    }));
    expect(runtimeRecoverableJournalInventoryRaw.value("ok", false),
           "phase7 runtime bridge focused list_execution_journals should return ok=true");
    const json runtimeRecoverableJournalTriageInventoryRaw = envelopeFromToolResult(adapter.callTool("tapescript_list_execution_journals", json{
        {"execution_ledger_artifact_id", dispatchLedgerArtifactId},
        {"recovery_state", "recovery_required"},
        {"restart_recovery_state", "recoverable"},
        {"restart_resume_policy", "continue_recovery"},
        {"latest_execution_resolution", "resolved_partially_filled"},
        {"limit", 10}
    }));
    expect(runtimeRecoverableJournalTriageInventoryRaw.value("ok", false),
           "phase7 runtime bridge focused triage list_execution_journals should return ok=true");
    const json runtimeRecoverableReadApplyRaw = envelopeFromToolResult(adapter.callTool("tapescript_read_execution_apply", json{
        {"execution_apply_artifact_id", executionApplyArtifactId}
    }));
    expect(runtimeRecoverableReadApplyRaw.value("ok", false),
           "phase7 runtime bridge focused read_execution_apply should return ok=true");
    const json runtimeRecoverableApplyInventoryRaw = envelopeFromToolResult(adapter.callTool("tapescript_list_execution_applies", json{
        {"execution_journal_artifact_id", dispatchJournalArtifactId},
        {"limit", 10}
    }));
    expect(runtimeRecoverableApplyInventoryRaw.value("ok", false),
           "phase7 runtime bridge focused list_execution_applies should return ok=true");
    const json runtimeRecoverableApplyTriageInventoryRaw = envelopeFromToolResult(adapter.callTool("tapescript_list_execution_applies", json{
        {"execution_journal_artifact_id", dispatchJournalArtifactId},
        {"recovery_state", "recovery_required"},
        {"restart_recovery_state", "recoverable"},
        {"restart_resume_policy", "continue_recovery"},
        {"latest_execution_resolution", "resolved_partially_filled"},
        {"limit", 10}
    }));
    expect(runtimeRecoverableApplyTriageInventoryRaw.value("ok", false),
           "phase7 runtime bridge focused triage list_execution_applies should return ok=true");

    {
        SharedData& state = appState();
        std::lock_guard<std::recursive_mutex> lock(state.mutex);
        auto& order = state.orders[runtimeOrderId];
        order.status = "Filled";
        order.filledQty = 30.0;
        order.remainingQty = 0.0;
        order.avgFillPrice = 46.15;
        order.localState = LocalOrderState::Filled;
        order.seenExecIds.insert("E-PHASE7-MCP-2");

        auto traceIt = state.traces.find(runtimeTraceId);
        expect(traceIt != state.traces.end(),
               "phase7 runtime bridge focused MCP projection should keep the seeded trace available");
        traceIt->second.latestStatus = "Filled";
        traceIt->second.terminalStatus = "Filled";
        FillSlice finalFill;
        finalFill.execId = "E-PHASE7-MCP-2";
        finalFill.shares = 20;
        finalFill.price = 46.17;
        finalFill.cumQty = 30.0;
        finalFill.avgPrice = 46.15;
        finalFill.exchange = "SIM";
        traceIt->second.fills.push_back(finalFill);
        state.traceIdByExecId[finalFill.execId] = runtimeTraceId;
    }
    publishSharedDataSnapshot();

    tape_phase7::ExecutionJournalArtifact terminalRuntimeJournal;
    std::vector<std::string> terminalRuntimeJournalEntryIds;
    std::string terminalRuntimeJournalAuditEventId;
    expect(tape_phase7::reconcileExecutionJournalEntriesViaRuntime(nullptr,
                                                                   {},
                                                                   dispatchJournalArtifactId,
                                                                   {runtimeJournalEntryId},
                                                                   "mcp-runtime-bridge",
                                                                   "Runtime resolved the order as filled.",
                                                                   &terminalRuntimeJournal,
                                                                   &terminalRuntimeJournalEntryIds,
                                                                   &terminalRuntimeJournalAuditEventId,
                                                                   &runtimeBridgeCode,
                                                                   &runtimeBridgeMessage),
           "phase7 runtime bridge terminal reconciliation for focused MCP projection should succeed: " +
               runtimeBridgeCode + " " + runtimeBridgeMessage);

    tape_phase7::ExecutionApplyArtifact terminalRuntimeApply;
    std::vector<std::string> terminalRuntimeApplyEntryIds;
    std::string terminalRuntimeApplyAuditEventId;
    expect(tape_phase7::synchronizeExecutionApplyFromJournal({},
                                                             executionApplyArtifactId,
                                                             "mcp-runtime-bridge",
                                                             "Mirror the terminal runtime reconciliation into apply.",
                                                             &terminalRuntimeApply,
                                                             &terminalRuntimeApplyEntryIds,
                                                             &terminalRuntimeApplyAuditEventId,
                                                             &runtimeBridgeCode,
                                                             &runtimeBridgeMessage),
           "phase7 runtime bridge terminal apply synchronization for focused MCP projection should succeed: " +
               runtimeBridgeCode + " " + runtimeBridgeMessage);

    const json runtimeTerminalReadJournalRaw = envelopeFromToolResult(adapter.callTool("tapescript_read_execution_journal", json{
        {"execution_journal_artifact_id", dispatchJournalArtifactId}
    }));
    expect(runtimeTerminalReadJournalRaw.value("ok", false),
           "phase7 runtime bridge focused terminal read_execution_journal should return ok=true");
    const json runtimeTerminalJournalInventoryRaw = envelopeFromToolResult(adapter.callTool("tapescript_list_execution_journals", json{
        {"execution_ledger_artifact_id", dispatchLedgerArtifactId},
        {"limit", 10}
    }));
    expect(runtimeTerminalJournalInventoryRaw.value("ok", false),
           "phase7 runtime bridge focused terminal list_execution_journals should return ok=true");
    const json runtimeTerminalJournalTriageInventoryRaw = envelopeFromToolResult(adapter.callTool("tapescript_list_execution_journals", json{
        {"execution_ledger_artifact_id", dispatchLedgerArtifactId},
        {"restart_recovery_state", "terminal_completed"},
        {"restart_resume_policy", "completed"},
        {"latest_execution_resolution", "resolved_filled"},
        {"limit", 10}
    }));
    expect(runtimeTerminalJournalTriageInventoryRaw.value("ok", false),
           "phase7 runtime bridge focused terminal triage list_execution_journals should return ok=true");
    const json runtimeTerminalReadApplyRaw = envelopeFromToolResult(adapter.callTool("tapescript_read_execution_apply", json{
        {"execution_apply_artifact_id", executionApplyArtifactId}
    }));
    expect(runtimeTerminalReadApplyRaw.value("ok", false),
           "phase7 runtime bridge focused terminal read_execution_apply should return ok=true");
    const json runtimeTerminalApplyInventoryRaw = envelopeFromToolResult(adapter.callTool("tapescript_list_execution_applies", json{
        {"execution_journal_artifact_id", dispatchJournalArtifactId},
        {"limit", 10}
    }));
    expect(runtimeTerminalApplyInventoryRaw.value("ok", false),
           "phase7 runtime bridge focused terminal list_execution_applies should return ok=true");
    const json runtimeTerminalApplyTriageInventoryRaw = envelopeFromToolResult(adapter.callTool("tapescript_list_execution_applies", json{
        {"execution_journal_artifact_id", dispatchJournalArtifactId},
        {"restart_recovery_state", "terminal_completed"},
        {"restart_resume_policy", "completed"},
        {"latest_execution_resolution", "resolved_filled"},
        {"limit", 10}
    }));
    expect(runtimeTerminalApplyTriageInventoryRaw.value("ok", false),
           "phase7 runtime bridge focused terminal triage list_execution_applies should return ok=true");

    const json runtimeRecoverableJournalRow = phase7InventoryRowByArtifactId(
        runtimeRecoverableJournalInventoryRaw, "execution_journals", "execution_journal", dispatchJournalArtifactId);
    const json runtimeRecoverableJournalTriageRow = phase7InventoryRowByArtifactId(
        runtimeRecoverableJournalTriageInventoryRaw, "execution_journals", "execution_journal", dispatchJournalArtifactId);
    const json runtimeRecoverableApplyRow = phase7InventoryRowByArtifactId(
        runtimeRecoverableApplyInventoryRaw, "execution_applies", "execution_apply", executionApplyArtifactId);
    const json runtimeRecoverableApplyTriageRow = phase7InventoryRowByArtifactId(
        runtimeRecoverableApplyTriageInventoryRaw, "execution_applies", "execution_apply", executionApplyArtifactId);
    const json runtimeTerminalJournalRow = phase7InventoryRowByArtifactId(
        runtimeTerminalJournalInventoryRaw, "execution_journals", "execution_journal", dispatchJournalArtifactId);
    const json runtimeTerminalJournalTriageRow = phase7InventoryRowByArtifactId(
        runtimeTerminalJournalTriageInventoryRaw, "execution_journals", "execution_journal", dispatchJournalArtifactId);
    const json runtimeTerminalApplyRow = phase7InventoryRowByArtifactId(
        runtimeTerminalApplyInventoryRaw, "execution_applies", "execution_apply", executionApplyArtifactId);
    const json runtimeTerminalApplyTriageRow = phase7InventoryRowByArtifactId(
        runtimeTerminalApplyTriageInventoryRaw, "execution_applies", "execution_apply", executionApplyArtifactId);
    expect(runtimeRecoverableJournalRow.is_object() && runtimeRecoverableApplyRow.is_object() &&
               runtimeTerminalJournalRow.is_object() && runtimeTerminalApplyRow.is_object(),
           "phase7 runtime bridge focused MCP projection should find the dispatch journal/apply rows in inventory results");
    expect(runtimeRecoverableJournalTriageRow.is_object() && runtimeRecoverableApplyTriageRow.is_object() &&
               runtimeTerminalJournalTriageRow.is_object() && runtimeTerminalApplyTriageRow.is_object(),
           "phase7 runtime bridge focused MCP triage filters should keep the matching runtime-backed journal/apply rows");
    expect(runtimeRecoverableReadJournalRaw.value("result", json::object()).value("restart_recovery_state", std::string()) ==
                   "recoverable" &&
               runtimeRecoverableReadJournalRaw.value("result", json::object()).value("restart_resume_policy", std::string()) ==
                   "continue_recovery" &&
               runtimeRecoverableReadJournalRaw.value("result", json::object()).value("latest_execution_resolution", std::string()) ==
                   "resolved_partially_filled" &&
               runtimeRecoverableReadApplyRaw.value("result", json::object()).value("restart_recovery_state", std::string()) ==
                   "recoverable" &&
               runtimeRecoverableReadApplyRaw.value("result", json::object()).value("restart_resume_policy", std::string()) ==
                   "continue_recovery" &&
               runtimeRecoverableReadApplyRaw.value("result", json::object()).value("latest_execution_resolution", std::string()) ==
                   "resolved_partially_filled",
           "phase7 runtime bridge focused MCP read payloads should expose recoverable execution triage at the top level");
    expect(runtimeTerminalReadJournalRaw.value("result", json::object()).value("restart_recovery_state", std::string()) ==
                   "terminal_completed" &&
               runtimeTerminalReadJournalRaw.value("result", json::object()).value("restart_resume_policy", std::string()) ==
                   "completed" &&
               runtimeTerminalReadJournalRaw.value("result", json::object()).value("latest_execution_resolution", std::string()) ==
                   "resolved_filled" &&
               runtimeTerminalReadApplyRaw.value("result", json::object()).value("restart_recovery_state", std::string()) ==
                   "terminal_completed" &&
               runtimeTerminalReadApplyRaw.value("result", json::object()).value("restart_resume_policy", std::string()) ==
                   "completed" &&
               runtimeTerminalReadApplyRaw.value("result", json::object()).value("latest_execution_resolution", std::string()) ==
                   "resolved_filled",
           "phase7 runtime bridge focused MCP terminal payloads should expose completed execution triage at the top level");
    expect(runtimeRecoverableJournalTriageInventoryRaw.value("result", json::object())
                   .value("applied_filters", json::object())
                   .value("restart_recovery_state", std::string()) == "recoverable" &&
               runtimeRecoverableJournalTriageInventoryRaw.value("result", json::object())
                   .value("applied_filters", json::object())
                   .value("restart_resume_policy", std::string()) == "continue_recovery" &&
               runtimeRecoverableJournalTriageInventoryRaw.value("result", json::object())
                   .value("applied_filters", json::object())
                   .value("latest_execution_resolution", std::string()) == "resolved_partially_filled" &&
               runtimeRecoverableApplyTriageInventoryRaw.value("result", json::object())
                   .value("applied_filters", json::object())
                   .value("restart_recovery_state", std::string()) == "recoverable" &&
               runtimeRecoverableApplyTriageInventoryRaw.value("result", json::object())
                   .value("applied_filters", json::object())
                   .value("restart_resume_policy", std::string()) == "continue_recovery" &&
               runtimeRecoverableApplyTriageInventoryRaw.value("result", json::object())
                   .value("applied_filters", json::object())
                   .value("latest_execution_resolution", std::string()) == "resolved_partially_filled",
           "phase7 runtime bridge focused MCP recoverable inventories should preserve the requested execution triage filters");
    expect(runtimeTerminalJournalTriageInventoryRaw.value("result", json::object())
                   .value("applied_filters", json::object())
                   .value("restart_recovery_state", std::string()) == "terminal_completed" &&
               runtimeTerminalJournalTriageInventoryRaw.value("result", json::object())
                   .value("applied_filters", json::object())
                   .value("restart_resume_policy", std::string()) == "completed" &&
               runtimeTerminalJournalTriageInventoryRaw.value("result", json::object())
                   .value("applied_filters", json::object())
                   .value("latest_execution_resolution", std::string()) == "resolved_filled" &&
               runtimeTerminalApplyTriageInventoryRaw.value("result", json::object())
                   .value("applied_filters", json::object())
                   .value("restart_recovery_state", std::string()) == "terminal_completed" &&
               runtimeTerminalApplyTriageInventoryRaw.value("result", json::object())
                   .value("applied_filters", json::object())
                   .value("restart_resume_policy", std::string()) == "completed" &&
               runtimeTerminalApplyTriageInventoryRaw.value("result", json::object())
                   .value("applied_filters", json::object())
                   .value("latest_execution_resolution", std::string()) == "resolved_filled",
           "phase7 runtime bridge focused MCP terminal inventories should preserve the requested execution triage filters");

    const json runtimeProjection = {
        {"read_execution_journal_recoverable",
         projectPhase7LatestExecutionResultSummary(
             runtimeRecoverableReadJournalRaw.value("result", json::object()).value("latest_execution_result_summary", json(nullptr)))},
        {"list_execution_journals_recoverable",
         projectPhase7LatestExecutionResultSummary(
             runtimeRecoverableJournalRow.value("latest_execution_result_summary", json(nullptr)))},
        {"read_execution_apply_recoverable",
         projectPhase7LatestExecutionResultSummary(
             runtimeRecoverableReadApplyRaw.value("result", json::object()).value("latest_execution_result_summary", json(nullptr)))},
        {"list_execution_applies_recoverable",
         projectPhase7LatestExecutionResultSummary(
             runtimeRecoverableApplyRow.value("latest_execution_result_summary", json(nullptr)))},
        {"read_execution_journal_terminal",
         projectPhase7LatestExecutionResultSummary(
             runtimeTerminalReadJournalRaw.value("result", json::object()).value("latest_execution_result_summary", json(nullptr)))},
        {"list_execution_journals_terminal",
         projectPhase7LatestExecutionResultSummary(
             runtimeTerminalJournalRow.value("latest_execution_result_summary", json(nullptr)))},
        {"read_execution_apply_terminal",
         projectPhase7LatestExecutionResultSummary(
             runtimeTerminalReadApplyRaw.value("result", json::object()).value("latest_execution_result_summary", json(nullptr)))},
        {"list_execution_applies_terminal",
         projectPhase7LatestExecutionResultSummary(
             runtimeTerminalApplyRow.value("latest_execution_result_summary", json(nullptr)))}
    };
    const json runtimeFixture = readJsonFixture("phase7_execution_runtime_mcp_contracts.json");
    expect(runtimeProjection == runtimeFixture,
           "phase7 runtime bridge MCP execution summary projection should match the focused golden fixture\nactual:\n" +
               runtimeProjection.dump(2));

    server->stop();
}

void testTapeMcpPhase8Contracts() {
    configurePhase7DataDir("tape-mcp-phase8-appdata");

    const fs::path rootDir = testDataDir() / "tape-mcp-phase8-engine";
    const fs::path socketPath = testDataDir() / "tape-mcp-phase8-engine.sock";
    auto server = startPhase5Engine(rootDir, socketPath);
    seedPhase5Engine(socketPath);

    waitUntil([&]() {
        const auto snapshot = server->snapshot();
        return snapshot.segments.size() >= 2 && snapshot.latestFrozenRevisionId >= 2;
    }, "phase8 MCP setup should freeze fixture batches");

    tape_mcp::Adapter adapter(tape_mcp::AdapterConfig{socketPath.string()});

    const json scanCaseEnvelope = envelopeFromToolResult(adapter.callTool("tapescript_scan_order_case_report", json{
        {"order_id", 7401},
        {"limit", 10}
    }));
    expect(scanCaseEnvelope.value("ok", false), "phase8 scan-order-case-report should return ok=true");
    const std::uint64_t caseReportId =
        parseTrailingNumericId(scanCaseEnvelope.value("result", json::object()).value("artifact_id", std::string()),
                               "case-report:");
    expect(caseReportId > 0, "phase8 scan-order-case-report should expose a case report id");

    const json exportCaseBundleEnvelope = envelopeFromToolResult(adapter.callTool("tapescript_export_case_bundle", json{
        {"report_id", caseReportId}
    }));
    expect(exportCaseBundleEnvelope.value("ok", false), "phase8 export-case-bundle should return ok=true");
    const std::string caseBundlePath =
        exportCaseBundleEnvelope.value("result", json::object()).value("bundle", json::object()).value("bundle_path", std::string());
    expect(!caseBundlePath.empty() && fs::exists(caseBundlePath),
           "phase8 export-case-bundle should write a portable case bundle");

    const json createWatchEnvelopeRaw = envelopeFromToolResult(adapter.callTool("tapescript_create_watch_definition", json{
        {"bundle_path", caseBundlePath},
        {"analysis_profile", tape_phase7::kDefaultAnalyzerProfile},
        {"title", "INTC Trigger Watch"},
        {"enabled", true},
        {"evaluation_cadence_minutes", 15}
    }));
    const json createWatchEnvelope = projectEnvelope(createWatchEnvelopeRaw);
    expect(createWatchEnvelope.value("ok", false), "phase8 create_watch_definition should return ok=true");
    const std::string watchArtifactId =
        createWatchEnvelopeRaw.value("result", json::object())
            .value("watch_definition", json::object())
            .value("artifact_id", std::string());
    expect(!watchArtifactId.empty(), "phase8 create_watch_definition should expose a watch artifact id");

    const json reusedWatchEnvelope = projectEnvelope(envelopeFromToolResult(adapter.callTool("tapescript_create_watch_definition", json{
        {"bundle_path", caseBundlePath},
        {"analysis_profile", tape_phase7::kDefaultAnalyzerProfile},
        {"title", "INTC Trigger Watch"},
        {"enabled", true},
        {"evaluation_cadence_minutes", 15}
    })));
    expect(reusedWatchEnvelope.value("ok", false), "phase8 watch rerun should return ok=true");

    const json createSuppressedWatchEnvelopeRaw = envelopeFromToolResult(adapter.callTool("tapescript_create_watch_definition", json{
        {"bundle_path", caseBundlePath},
        {"analysis_profile", tape_phase7::kIncidentTriageAnalyzerProfile},
        {"title", "INTC Strict Trigger Watch"},
        {"enabled", true},
        {"evaluation_cadence_minutes", 15},
        {"minimum_finding_count", 99},
        {"required_category", "never_matches"}
    }));
    const json createSuppressedWatchEnvelope = projectEnvelope(createSuppressedWatchEnvelopeRaw);
    expect(createSuppressedWatchEnvelope.value("ok", false), "phase8 strict create_watch_definition should return ok=true");
    const std::string suppressedWatchArtifactId =
        createSuppressedWatchEnvelopeRaw.value("result", json::object())
            .value("watch_definition", json::object())
            .value("artifact_id", std::string());
    expect(!suppressedWatchArtifactId.empty(), "phase8 strict create_watch_definition should expose a watch artifact id");

    const json watchListEnvelope = projectEnvelope(envelopeFromToolResult(adapter.callTool("tapescript_list_watch_definitions", json{
        {"limit", 10}
    })));
    expect(watchListEnvelope.value("ok", false), "phase8 list_watch_definitions should return ok=true");

    const json readWatchEnvelope = projectEnvelope(envelopeFromToolResult(adapter.callTool("tapescript_read_watch_definition", json{
        {"watch_artifact_id", watchArtifactId}
    })));
    expect(readWatchEnvelope.value("ok", false), "phase8 read_watch_definition should return ok=true");

    const json evaluateWatchEnvelopeRaw = envelopeFromToolResult(adapter.callTool("tapescript_evaluate_watch_definition", json{
        {"watch_artifact_id", watchArtifactId}
    }));
    const json evaluateWatchEnvelope = projectEnvelope(evaluateWatchEnvelopeRaw);
    expect(evaluateWatchEnvelope.value("ok", false), "phase8 evaluate_watch_definition should return ok=true");
    const std::string triggerArtifactId =
        evaluateWatchEnvelopeRaw.value("result", json::object())
            .value("trigger_run", json::object())
            .value("artifact_id", std::string());
    expect(!triggerArtifactId.empty(), "phase8 evaluate_watch_definition should expose a trigger artifact id");

    const json suppressedEvaluateWatchEnvelopeRaw = envelopeFromToolResult(adapter.callTool("tapescript_evaluate_watch_definition", json{
        {"watch_artifact_id", suppressedWatchArtifactId}
    }));
    const json suppressedEvaluateWatchEnvelope = projectEnvelope(suppressedEvaluateWatchEnvelopeRaw);
    expect(suppressedEvaluateWatchEnvelope.value("ok", false), "phase8 suppressed evaluate_watch_definition should return ok=true");
    const std::string suppressedTriggerArtifactId =
        suppressedEvaluateWatchEnvelopeRaw.value("result", json::object())
            .value("trigger_run", json::object())
            .value("artifact_id", std::string());
    expect(!suppressedTriggerArtifactId.empty(),
           "phase8 suppressed evaluate_watch_definition should expose a trigger artifact id");

    const json triggerRunListEnvelope = projectEnvelope(envelopeFromToolResult(adapter.callTool("tapescript_list_trigger_runs", json{
        {"limit", 10}
    })));
    expect(triggerRunListEnvelope.value("ok", false), "phase8 list_trigger_runs should return ok=true");

    const json readTriggerRunEnvelope = projectEnvelope(envelopeFromToolResult(adapter.callTool("tapescript_read_trigger_run", json{
        {"trigger_artifact_id", triggerArtifactId}
    })));
    expect(readTriggerRunEnvelope.value("ok", false), "phase8 read_trigger_run should return ok=true");

    const json readSuppressedTriggerRunEnvelope = projectEnvelope(envelopeFromToolResult(adapter.callTool("tapescript_read_trigger_run", json{
        {"trigger_artifact_id", suppressedTriggerArtifactId}
    })));
    expect(readSuppressedTriggerRunEnvelope.value("ok", false),
           "phase8 read suppressed trigger_run should return ok=true");

    const json attentionItemsEnvelope = projectEnvelope(envelopeFromToolResult(adapter.callTool("tapescript_list_attention_items", json{
        {"limit", 10}
    })));
    expect(attentionItemsEnvelope.value("ok", false), "phase8 list_attention_items should return ok=true");

    const json createScheduledWatchEnvelopeRaw = envelopeFromToolResult(adapter.callTool("tapescript_create_watch_definition", json{
        {"bundle_path", caseBundlePath},
        {"analysis_profile", tape_phase7::kFillQualityAnalyzerProfile},
        {"title", "INTC Scheduled Trigger Watch"},
        {"enabled", true},
        {"evaluation_cadence_minutes", 15}
    }));
    const json createScheduledWatchEnvelope = projectEnvelope(createScheduledWatchEnvelopeRaw);
    expect(createScheduledWatchEnvelope.value("ok", false),
           "phase8 scheduled create_watch_definition should return ok=true");

    const json createScheduledSuppressedWatchEnvelopeRaw = envelopeFromToolResult(adapter.callTool("tapescript_create_watch_definition", json{
        {"bundle_path", caseBundlePath},
        {"analysis_profile", tape_phase7::kOrderImpactAnalyzerProfile},
        {"title", "INTC Scheduled Strict Trigger Watch"},
        {"enabled", true},
        {"evaluation_cadence_minutes", 15},
        {"minimum_finding_count", 99},
        {"required_category", "never_matches"}
    }));
    const json createScheduledSuppressedWatchEnvelope = projectEnvelope(createScheduledSuppressedWatchEnvelopeRaw);
    expect(createScheduledSuppressedWatchEnvelope.value("ok", false),
           "phase8 scheduled strict create_watch_definition should return ok=true");

    const json dueWatchListEnvelope = projectEnvelope(envelopeFromToolResult(adapter.callTool("tapescript_list_due_watches", json{
        {"limit", 10}
    })));
    expect(dueWatchListEnvelope.value("ok", false), "phase8 list_due_watches should return ok=true");

    const json runDueWatchesEnvelope = projectEnvelope(envelopeFromToolResult(adapter.callTool("tapescript_run_due_watches", json{
        {"limit", 10}
    })));
    expect(runDueWatchesEnvelope.value("ok", false), "phase8 run_due_watches should return ok=true");

    const json invalidWatchCreateEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_create_watch_definition", json::object())));
    expect(!invalidWatchCreateEnvelope.value("ok", true),
           "phase8 create_watch_definition without args should return ok=false");
    expect(invalidWatchCreateEnvelope.value("error", json::object()).value("code", std::string()) == "invalid_arguments",
           "phase8 create_watch_definition without args should return invalid_arguments");

    const json missingWatchEnvelope = projectEnvelope(envelopeFromToolResult(adapter.callTool("tapescript_read_watch_definition", json{
        {"watch_artifact_id", "phase8-watch-missing"}
    })));
    expect(!missingWatchEnvelope.value("ok", true), "phase8 read missing watch should return ok=false");
    expect(missingWatchEnvelope.value("error", json::object()).value("code", std::string()) == "artifact_not_found",
           "phase8 read missing watch should return artifact_not_found");

    const json resourceListResult = adapter.listResourcesResult();
    const json phase8ResourceList = projectPhase8ResourceList(resourceListResult);
    const json watchResource = projectResourceRead(adapter.readResourceResult("tape://phase8/watch/" + watchArtifactId));
    const json watchMarkdownResource = projectResourceRead(
        adapter.readResourceResult("tape://phase8/watch/" + watchArtifactId + "/markdown"));
    const json triggerResource = projectResourceRead(adapter.readResourceResult("tape://phase8/trigger/" + triggerArtifactId));
    const json triggerMarkdownResource = projectResourceRead(
        adapter.readResourceResult("tape://phase8/trigger/" + triggerArtifactId + "/markdown"));
    const json attentionResource = projectResourceRead(adapter.readResourceResult("tape://phase8/attention/open"));

    const json createAckWatchEnvelopeRaw = envelopeFromToolResult(adapter.callTool("tapescript_create_watch_definition", json{
        {"bundle_path", caseBundlePath},
        {"analysis_profile", tape_phase7::kLiquidityBehaviorAnalyzerProfile},
        {"title", "INTC Ack Trigger Watch"},
        {"enabled", true},
        {"evaluation_cadence_minutes", 15}
    }));
    expect(createAckWatchEnvelopeRaw.value("ok", false), "phase8 acknowledge watch create should return ok=true");
    const std::string acknowledgeWatchArtifactId =
        createAckWatchEnvelopeRaw.value("result", json::object())
            .value("watch_definition", json::object())
            .value("artifact_id", std::string());

    const json evaluateAckWatchEnvelopeRaw = envelopeFromToolResult(adapter.callTool("tapescript_evaluate_watch_definition", json{
        {"watch_artifact_id", acknowledgeWatchArtifactId}
    }));
    expect(evaluateAckWatchEnvelopeRaw.value("ok", false), "phase8 acknowledge watch evaluation should return ok=true");
    const std::string acknowledgeTriggerArtifactId =
        evaluateAckWatchEnvelopeRaw.value("result", json::object())
            .value("trigger_run", json::object())
            .value("artifact_id", std::string());

    const json acknowledgeAttentionEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_acknowledge_attention_item", json{
            {"trigger_artifact_id", acknowledgeTriggerArtifactId},
            {"actor", "tapescope"},
            {"comment", "Acknowledged for later follow-up."}
        })));
    expect(acknowledgeAttentionEnvelope.value("ok", false), "phase8 acknowledge attention should return ok=true");
    const json attentionAfterAcknowledge = projectEnvelope(envelopeFromToolResult(adapter.callTool("tapescript_list_attention_items", json{
        {"limit", 20}
    })));
    const json attentionAfterAcknowledgeItems =
        attentionAfterAcknowledge.value("result", json::object()).value("attention_items", json::array());
    expect(std::none_of(attentionAfterAcknowledgeItems.begin(),
                        attentionAfterAcknowledgeItems.end(),
                        [&](const json& item) {
                            return item.value("title", std::string()) == "INTC Ack Trigger Watch";
                        }),
           "phase8 acknowledged trigger should leave the open attention inbox");

    const json createSnoozeWatchEnvelopeRaw = envelopeFromToolResult(adapter.callTool("tapescript_create_watch_definition", json{
        {"bundle_path", caseBundlePath},
        {"analysis_profile", tape_phase7::kAdverseSelectionAnalyzerProfile},
        {"title", "INTC Snooze Trigger Watch"},
        {"enabled", true},
        {"evaluation_cadence_minutes", 15}
    }));
    expect(createSnoozeWatchEnvelopeRaw.value("ok", false), "phase8 snooze watch create should return ok=true");
    const std::string snoozeWatchArtifactId =
        createSnoozeWatchEnvelopeRaw.value("result", json::object())
            .value("watch_definition", json::object())
            .value("artifact_id", std::string());

    const json evaluateSnoozeWatchEnvelopeRaw = envelopeFromToolResult(adapter.callTool("tapescript_evaluate_watch_definition", json{
        {"watch_artifact_id", snoozeWatchArtifactId}
    }));
    expect(evaluateSnoozeWatchEnvelopeRaw.value("ok", false), "phase8 snooze watch evaluation should return ok=true");
    const std::string snoozeTriggerArtifactId =
        evaluateSnoozeWatchEnvelopeRaw.value("result", json::object())
            .value("trigger_run", json::object())
            .value("artifact_id", std::string());

    const json snoozeAttentionEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_snooze_attention_item", json{
            {"trigger_artifact_id", snoozeTriggerArtifactId},
            {"snooze_minutes", 30},
            {"actor", "tapescope"},
            {"comment", "Snoozed until the next review pass."}
        })));
    expect(snoozeAttentionEnvelope.value("ok", false), "phase8 snooze attention should return ok=true");
    const json attentionAfterSnooze = projectEnvelope(envelopeFromToolResult(adapter.callTool("tapescript_list_attention_items", json{
        {"limit", 20}
    })));
    const json attentionAfterSnoozeItems =
        attentionAfterSnooze.value("result", json::object()).value("attention_items", json::array());
    expect(std::none_of(attentionAfterSnoozeItems.begin(),
                        attentionAfterSnoozeItems.end(),
                        [&](const json& item) {
                            return item.value("title", std::string()) == "INTC Snooze Trigger Watch";
                        }),
           "phase8 snoozed trigger should leave the open attention inbox until wake-up");

    const json evaluateResolveWatchEnvelopeRaw = envelopeFromToolResult(adapter.callTool("tapescript_evaluate_watch_definition", json{
        {"watch_artifact_id", acknowledgeWatchArtifactId}
    }));
    expect(evaluateResolveWatchEnvelopeRaw.value("ok", false), "phase8 resolve watch evaluation should return ok=true");
    const std::string resolveTriggerArtifactId =
        evaluateResolveWatchEnvelopeRaw.value("result", json::object())
            .value("trigger_run", json::object())
            .value("artifact_id", std::string());

    const json resolveAttentionEnvelope = projectEnvelope(envelopeFromToolResult(
        adapter.callTool("tapescript_resolve_attention_item", json{
            {"trigger_artifact_id", resolveTriggerArtifactId},
            {"actor", "tapescope"},
            {"comment", "Resolved through the inbox workflow."}
        })));
    expect(resolveAttentionEnvelope.value("ok", false), "phase8 resolve attention should return ok=true");
    const json attentionAfterResolve = projectEnvelope(envelopeFromToolResult(adapter.callTool("tapescript_list_attention_items", json{
        {"limit", 20}
    })));
    const json attentionAfterResolveItems =
        attentionAfterResolve.value("result", json::object()).value("attention_items", json::array());
    expect(std::none_of(attentionAfterResolveItems.begin(),
                        attentionAfterResolveItems.end(),
                        [&](const json& item) {
                            return item.value("title", std::string()) == "INTC Ack Trigger Watch";
                        }),
           "phase8 resolved trigger should leave the open attention inbox");

    const json fixture = readJsonFixture("phase8_mcp_contracts.json");
    expect(createWatchEnvelope == fixture.value("tapescript_create_watch_definition", json::object()),
           "phase8 create_watch_definition envelope should match golden fixture\nactual:\n" + createWatchEnvelope.dump(2));
    expect(reusedWatchEnvelope == fixture.value("tapescript_create_watch_definition_reused", json::object()),
           "phase8 create_watch_definition rerun envelope should match golden fixture\nactual:\n" + reusedWatchEnvelope.dump(2));
    expect(watchListEnvelope == fixture.value("tapescript_list_watch_definitions", json::object()),
           "phase8 list_watch_definitions envelope should match golden fixture\nactual:\n" + watchListEnvelope.dump(2));
    expect(readWatchEnvelope == fixture.value("tapescript_read_watch_definition", json::object()),
           "phase8 read_watch_definition envelope should match golden fixture\nactual:\n" + readWatchEnvelope.dump(2));
    expect(createSuppressedWatchEnvelope == fixture.value("tapescript_create_watch_definition_suppressed", json::object()),
           "phase8 strict create_watch_definition envelope should match golden fixture\nactual:\n" +
               createSuppressedWatchEnvelope.dump(2));
    expect(createScheduledWatchEnvelope == fixture.value("tapescript_create_watch_definition_scheduled", json::object()),
           "phase8 scheduled create_watch_definition envelope should match golden fixture\nactual:\n" +
               createScheduledWatchEnvelope.dump(2));
    expect(createScheduledSuppressedWatchEnvelope ==
               fixture.value("tapescript_create_watch_definition_scheduled_suppressed", json::object()),
           "phase8 scheduled strict create_watch_definition envelope should match golden fixture\nactual:\n" +
               createScheduledSuppressedWatchEnvelope.dump(2));
    expect(evaluateWatchEnvelope == fixture.value("tapescript_evaluate_watch_definition", json::object()),
           "phase8 evaluate_watch_definition envelope should match golden fixture\nactual:\n" + evaluateWatchEnvelope.dump(2));
    expect(suppressedEvaluateWatchEnvelope == fixture.value("tapescript_evaluate_watch_definition_suppressed", json::object()),
           "phase8 suppressed evaluate_watch_definition envelope should match golden fixture\nactual:\n" +
               suppressedEvaluateWatchEnvelope.dump(2));
    expect(dueWatchListEnvelope == fixture.value("tapescript_list_due_watches", json::object()),
           "phase8 list_due_watches envelope should match golden fixture\nactual:\n" + dueWatchListEnvelope.dump(2));
    expect(runDueWatchesEnvelope == fixture.value("tapescript_run_due_watches", json::object()),
           "phase8 run_due_watches envelope should match golden fixture\nactual:\n" + runDueWatchesEnvelope.dump(2));
    expect(acknowledgeAttentionEnvelope == fixture.value("tapescript_acknowledge_attention_item", json::object()),
           "phase8 acknowledge attention envelope should match golden fixture\nactual:\n" +
               acknowledgeAttentionEnvelope.dump(2));
    expect(snoozeAttentionEnvelope == fixture.value("tapescript_snooze_attention_item", json::object()),
           "phase8 snooze attention envelope should match golden fixture\nactual:\n" +
               snoozeAttentionEnvelope.dump(2));
    expect(resolveAttentionEnvelope == fixture.value("tapescript_resolve_attention_item", json::object()),
           "phase8 resolve attention envelope should match golden fixture\nactual:\n" +
               resolveAttentionEnvelope.dump(2));
    expect(triggerRunListEnvelope == fixture.value("tapescript_list_trigger_runs", json::object()),
           "phase8 list_trigger_runs envelope should match golden fixture\nactual:\n" + triggerRunListEnvelope.dump(2));
    expect(readTriggerRunEnvelope == fixture.value("tapescript_read_trigger_run", json::object()),
           "phase8 read_trigger_run envelope should match golden fixture\nactual:\n" + readTriggerRunEnvelope.dump(2));
    expect(readSuppressedTriggerRunEnvelope == fixture.value("tapescript_read_trigger_run_suppressed", json::object()),
           "phase8 read suppressed trigger_run envelope should match golden fixture\nactual:\n" +
               readSuppressedTriggerRunEnvelope.dump(2));
    expect(attentionItemsEnvelope == fixture.value("tapescript_list_attention_items", json::object()),
           "phase8 list_attention_items envelope should match golden fixture\nactual:\n" + attentionItemsEnvelope.dump(2));
    expect(phase8ResourceList == fixture.value("resources_list_phase8", json::array()),
           "phase8 resources/list projection should match golden fixture\nactual:\n" + phase8ResourceList.dump(2));
    expect(watchResource == fixture.value("resource_read_phase8_watch", json::object()),
           "phase8 watch resource should match golden fixture\nactual:\n" + watchResource.dump(2));
    expect(watchMarkdownResource == fixture.value("resource_read_phase8_watch_markdown", json::object()),
           "phase8 watch markdown resource should match golden fixture\nactual:\n" + watchMarkdownResource.dump(2));
    expect(triggerResource == fixture.value("resource_read_phase8_trigger", json::object()),
           "phase8 trigger resource should match golden fixture\nactual:\n" + triggerResource.dump(2));
    expect(triggerMarkdownResource == fixture.value("resource_read_phase8_trigger_markdown", json::object()),
           "phase8 trigger markdown resource should match golden fixture\nactual:\n" + triggerMarkdownResource.dump(2));
    expect(attentionResource == fixture.value("resource_read_phase8_attention", json::object()),
           "phase8 attention resource should match golden fixture\nactual:\n" + attentionResource.dump(2));
}

void testTapeMcpStdioHarness() {
    configurePhase7DataDir("tape-mcp-phase7-stdio-appdata");

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
    expect(tools.is_array() && tools.size() == 69, "tools/list should expose the expanded phase 8 inbox-action slice");
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
    json analyzerRunTool = json::object();
    for (const auto& tool : tools) {
        if (tool.value("name", std::string()) == "tapescript_analyzer_run") {
            analyzerRunTool = tool;
            break;
        }
    }
    expect(analyzerRunTool.is_object(), "tools/list should include tapescript_analyzer_run");
    expect(analyzerRunTool.value("progressHint", false),
           "tools/list should advertise progressHint for analyzer runs");
    json createWatchTool = json::object();
    for (const auto& tool : tools) {
        if (tool.value("name", std::string()) == "tapescript_create_watch_definition") {
            createWatchTool = tool;
            break;
        }
    }
    expect(createWatchTool.is_object(), "tools/list should include tapescript_create_watch_definition");
    expect(!createWatchTool.value("annotations", json::object()).value("readOnlyHint", true),
           "tools/list should mark create_watch_definition as mutating");
    json dueWatchTool = json::object();
    json runDueWatchesTool = json::object();
    json acknowledgeAttentionTool = json::object();
    json snoozeAttentionTool = json::object();
    json resolveAttentionTool = json::object();
    for (const auto& tool : tools) {
        if (tool.value("name", std::string()) == "tapescript_list_due_watches") {
            dueWatchTool = tool;
        }
        if (tool.value("name", std::string()) == "tapescript_run_due_watches") {
            runDueWatchesTool = tool;
        }
        if (tool.value("name", std::string()) == "tapescript_acknowledge_attention_item") {
            acknowledgeAttentionTool = tool;
        }
        if (tool.value("name", std::string()) == "tapescript_snooze_attention_item") {
            snoozeAttentionTool = tool;
        }
        if (tool.value("name", std::string()) == "tapescript_resolve_attention_item") {
            resolveAttentionTool = tool;
        }
    }
    expect(dueWatchTool.is_object(), "tools/list should include tapescript_list_due_watches");
    expect(runDueWatchesTool.is_object(), "tools/list should include tapescript_run_due_watches");
    expect(acknowledgeAttentionTool.is_object(), "tools/list should include tapescript_acknowledge_attention_item");
    expect(snoozeAttentionTool.is_object(), "tools/list should include tapescript_snooze_attention_item");
    expect(resolveAttentionTool.is_object(), "tools/list should include tapescript_resolve_attention_item");
    expect(runDueWatchesTool.value("progressHint", false),
           "tools/list should advertise progressHint for due watch batch runs");

    writeJsonRpcMessage(child.writeFd, json{
        {"jsonrpc", "2.0"},
        {"id", 21},
        {"method", "prompts/list"},
        {"params", json::object()}
    });
    const json promptListResponse = readJsonRpcMessage(child.readFd);
    const json prompts = promptListResponse.value("result", json::object()).value("prompts", json::array());
    expect(prompts.is_array() && prompts.size() == 14, "prompts/list should expose the expanded phase7 prompt set");

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
        {"id", 231},
        {"method", "prompts/get"},
        {"params", {
            {"name", "analyze_bundle_with_phase7"},
            {"arguments", json{
                {"case_bundle_path", "/tmp/example-bundle.msgpack"}
            }}
        }}
    });
    const json phase7PromptResponse = readJsonRpcMessage(child.readFd);
    expect(phase7PromptResponse.value("result", json::object())
               .value("meta", json::object())
               .value("prompt", std::string()) == "analyze_bundle_with_phase7",
           "prompts/get should resolve analyze_bundle_with_phase7");

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
        {"id", 64},
        {"method", "tools/call"},
        {"params", {
            {"name", "tapescript_export_case_bundle"},
            {"arguments", json{{"report_id", caseReportId}}}
        }}
    });
    const json exportCaseBundleResponse = readJsonRpcMessage(child.readFd);
    const json exportCaseBundleEnvelope =
        exportCaseBundleResponse.value("result", json::object()).value("structuredContent", json::object());
    expect(exportCaseBundleEnvelope.value("ok", false), "stdio export-case-bundle should return ok=true");
    const std::string exportedCaseBundlePath =
        exportCaseBundleEnvelope.value("result", json::object())
            .value("bundle", json::object())
            .value("bundle_path", std::string());
    expect(!exportedCaseBundlePath.empty(), "stdio export-case-bundle should return a bundle path");

    writeJsonRpcMessage(child.writeFd, json{
        {"jsonrpc", "2.0"},
        {"id", 65},
        {"method", "tools/call"},
        {"params", {
            {"name", "tapescript_analyzer_run"},
            {"arguments", json{{"case_bundle_path", exportedCaseBundlePath}}}
        }}
    });
    const json analyzerRunResponse = readJsonRpcMessage(child.readFd);
    const json analyzerRunEnvelope =
        analyzerRunResponse.value("result", json::object()).value("structuredContent", json::object());
    expect(analyzerRunEnvelope.value("ok", false), "stdio analyzer_run should return ok=true");
    const std::string analysisArtifactId =
        analyzerRunEnvelope.value("result", json::object())
            .value("analysis_artifact", json::object())
            .value("artifact_id", std::string());
    expect(!analysisArtifactId.empty(), "stdio analyzer_run should return an analysis artifact id");

    writeJsonRpcMessage(child.writeFd, json{
        {"jsonrpc", "2.0"},
        {"id", 66},
        {"method", "tools/call"},
        {"params", {
            {"name", "tapescript_findings_list"},
            {"arguments", json{{"analysis_artifact_id", analysisArtifactId}}}
        }}
    });
    const json findingsListResponse = readJsonRpcMessage(child.readFd);
    const json findingsListEnvelope =
        findingsListResponse.value("result", json::object()).value("structuredContent", json::object());
    expect(findingsListEnvelope.value("ok", false), "stdio findings_list should return ok=true");

    writeJsonRpcMessage(child.writeFd, json{
        {"jsonrpc", "2.0"},
        {"id", 67},
        {"method", "tools/call"},
        {"params", {
            {"name", "tapescript_playbook_apply"},
            {"arguments", json{{"analysis_artifact_id", analysisArtifactId}}}
        }}
    });
    const json playbookResponse = readJsonRpcMessage(child.readFd);
    const json playbookEnvelope =
        playbookResponse.value("result", json::object()).value("structuredContent", json::object());
    expect(playbookEnvelope.value("ok", false), "stdio playbook_apply dry_run should return ok=true");
    const std::string playbookArtifactId =
        playbookEnvelope.value("result", json::object())
            .value("playbook_artifact", json::object())
            .value("artifact_id", std::string());
    expect(!playbookArtifactId.empty(), "stdio playbook_apply should return a playbook artifact id");

    writeJsonRpcMessage(child.writeFd, json{
        {"jsonrpc", "2.0"},
        {"id", 671},
        {"method", "tools/call"},
        {"params", {
            {"name", "tapescript_prepare_execution_ledger"},
            {"arguments", json{{"playbook_artifact_id", playbookArtifactId}}}
        }}
    });
    const json executionLedgerResponse = readJsonRpcMessage(child.readFd);
    const json executionLedgerEnvelope =
        executionLedgerResponse.value("result", json::object()).value("structuredContent", json::object());
    expect(executionLedgerEnvelope.value("ok", false), "stdio prepare_execution_ledger should return ok=true");
    const std::string executionLedgerArtifactId =
        executionLedgerEnvelope.value("result", json::object())
            .value("execution_ledger", json::object())
            .value("artifact_id", std::string());
    expect(!executionLedgerArtifactId.empty(), "stdio prepare_execution_ledger should return a ledger artifact id");
    const json executionLedgerEntries =
        executionLedgerEnvelope.value("result", json::object()).value("entries", json::array());
    expect(executionLedgerEntries.is_array() && !executionLedgerEntries.empty(),
           "stdio prepare_execution_ledger should expose at least one review entry");

    writeJsonRpcMessage(child.writeFd, json{
        {"jsonrpc", "2.0"},
        {"id", 672},
        {"method", "tools/call"},
        {"params", {
            {"name", "tapescript_record_execution_ledger_review"},
            {"arguments", json{
                {"execution_ledger_artifact_id", executionLedgerArtifactId},
                {"entry_ids", json::array({executionLedgerEntries.front().value("entry_id", std::string())})},
                {"review_status", "approved"},
                {"actor", "stdio-reviewer"},
                {"comment", "Reviewed through stdio."}
            }}
        }}
    });
    const json recordLedgerReviewResponse = readJsonRpcMessage(child.readFd);
    const json recordLedgerReviewEnvelope =
        recordLedgerReviewResponse.value("result", json::object()).value("structuredContent", json::object());
    expect(recordLedgerReviewEnvelope.value("ok", false), "stdio record_execution_ledger_review should return ok=true");
    const std::string stdioReviewedLedgerStatus =
        executionLedgerEntries.size() == 1
            ? std::string(tape_phase7::kLedgerStatusCompleted)
            : std::string(tape_phase7::kLedgerStatusInProgress);
    expect(recordLedgerReviewEnvelope.value("result", json::object()).value("ledger_status", std::string()) ==
               stdioReviewedLedgerStatus,
           "stdio record_execution_ledger_review should update the aggregate ledger status");

    writeJsonRpcMessage(child.writeFd, json{
        {"jsonrpc", "2.0"},
        {"id", 68},
        {"method", "tools/call"},
        {"params", {
            {"name", "tapescript_playbook_apply"},
            {"arguments", json{{"analysis_artifact_id", analysisArtifactId}, {"mode", "apply"}}}
        }}
    });
    const json deferredPlaybookResponse = readJsonRpcMessage(child.readFd);
    const json deferredPlaybookEnvelope =
        deferredPlaybookResponse.value("result", json::object()).value("structuredContent", json::object());
    expect(!deferredPlaybookEnvelope.value("ok", true), "stdio playbook_apply apply mode should return ok=false");
    expect(deferredPlaybookEnvelope.value("error", json::object()).value("code", std::string()) == "deferred_behavior",
           "stdio playbook_apply apply mode should return deferred_behavior");

    writeJsonRpcMessage(child.writeFd, json{
        {"jsonrpc", "2.0"},
        {"id", 61},
        {"method", "resources/list"},
        {"params", json::object()}
    });
    const json resourcesListResponse = readJsonRpcMessage(child.readFd);
    const json resources = resourcesListResponse.value("result", json::object()).value("resources", json::array());
    expect(resources.is_array() && !resources.empty(), "resources/list should return durable report resources");
    bool sawPhase7AnalysisResource = false;
    for (const auto& resource : resources) {
        if (resource.value("uri", std::string()) == "tape://phase7/analysis/" + analysisArtifactId) {
            sawPhase7AnalysisResource = true;
            break;
        }
    }
    expect(sawPhase7AnalysisResource, "resources/list should expose the stored phase7 analysis resource");

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
        {"id", 631},
        {"method", "resources/read"},
        {"params", {
            {"uri", "tape://phase7/analysis/" + analysisArtifactId}
        }}
    });
    const json phase7AnalysisResourceResponse = readJsonRpcMessage(child.readFd);
    expect(phase7AnalysisResourceResponse.value("result", json::object()).value("meta", json::object()).value("ok", false),
           "resources/read should return ok=true for a durable phase7 analysis resource");

    writeJsonRpcMessage(child.writeFd, json{
        {"jsonrpc", "2.0"},
        {"id", 632},
        {"method", "resources/read"},
        {"params", {
            {"uri", "tape://phase7/playbook/" + playbookArtifactId + "/markdown"}
        }}
    });
    const json phase7PlaybookResourceResponse = readJsonRpcMessage(child.readFd);
    const json phase7PlaybookContents = phase7PlaybookResourceResponse.value("result", json::object()).value("contents", json::array());
    expect(phase7PlaybookResourceResponse.value("result", json::object()).value("meta", json::object()).value("ok", false),
           "resources/read should return ok=true for a durable phase7 playbook markdown resource");
    expect(phase7PlaybookContents.is_array() && !phase7PlaybookContents.empty() &&
               phase7PlaybookContents.front().value("mimeType", std::string()) == "text/markdown",
           "resources/read should expose markdown phase7 playbook exports");

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
        testUWMcpFacetResolutionCatalog();
        testTapeMcpPhase5WebsocketFixtureEnrichment();
        testTapeMcpPhase5WebsocketJoinAckOnlyDiagnostics();
        testTapeMcpPhase5WebsocketAlreadyInRoomDiagnostics();
        testTapeMcpPhase5WebsocketSymbolFiltering();
        testTapeMcpPhase5WebsocketTargetedRetryRescue();
        testTapeMcpPhase5WebsocketAmbientGlobalOnly();
        testTapeMcpPhase6BundleTools();
        testTapeMcpPhase7Contracts();
        testTapeMcpPhase8Contracts();
        testTapeMcpStdioHarness();
    } catch (const std::exception& error) {
        std::cerr << "tape_mcp_contract_tests failed: " << error.what() << '\n';
        return 1;
    }

    std::cout << "tape_mcp_contract_tests passed\n";
    return 0;
}
