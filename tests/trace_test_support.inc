#include "app_shared.h"
#include "bridge_batch_codec.h"
#include "bridge_batch_transport.h"
#include "runtime_registry.h"
#include "tapescope_client.h"
#include "tape_engine_client.h"
#include "tape_engine.h"
#include "tape_engine_protocol.h"
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

