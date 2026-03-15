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

void testReplayPrefersRichLiveTrace() {
    clearTestFiles();

    appendTraceLine(makeTraceLine(7, 113, 5001, "GUI Button", "INTC", "BUY",
                                  "Trigger", "GUI Button", "BUY 1 INTC @ 45.64", 0.0));
    appendTraceLine(makeTraceLine(7, 113, 5001, "GUI Button", "INTC", "BUY",
                                  "ValidationStart", "Validation", "Starting local order validation", 1.0));
    appendTraceLine(makeTraceLine(7, 113, 5001, "GUI Button", "INTC", "BUY",
                                  "ValidationOk", "Validation", "Validation passed", 4.0));
    appendTraceLine(makeTraceLine(7, 113, 5001, "GUI Button", "INTC", "BUY",
                                  "PlaceOrderCallStart", "placeOrder", "placeOrder start", 7.0));
    appendTraceLine(makeTraceLine(7, 113, 5001, "GUI Button", "INTC", "BUY",
                                  "PlaceOrderCallEnd", "placeOrder", "placeOrder return", 10.0));
    appendTraceLine(makeTraceLine(7, 113, 5001, "GUI Button", "INTC", "BUY",
                                  "OrderStatusSeen", "Submitted", "submitted", 28.0));
    appendTraceLine(makeTraceLine(7, 113, 5001, "GUI Button", "INTC", "BUY",
                                  "ExecDetailsSeen", "execDetails", "exch=SMART execId=E1 time=20260313 09:30:02",
                                  2930.0, 45.64, 1, 1.0));
    appendTraceLine(makeTraceLine(7, 113, 5001, "GUI Button", "INTC", "BUY",
                                  "CommissionSeen", "commissionReport", "E1 commission=0.4564 USD", 2935.0));
    appendTraceLine(makeTraceLine(7, 113, 5001, "GUI Button", "INTC", "BUY",
                                  "FinalState", "Terminal", "Filled: order complete", 2936.0));

    appendTraceLine(makeTraceLine(100, 113, 5001, "Broker Reconcile", "INTC", "BUY",
                                  "Note", "Recovered", "Recovered execution during startup reconciliation", 0.0));
    appendTraceLine(makeTraceLine(100, 113, 5001, "Broker Reconcile", "INTC", "BUY",
                                  "ExecDetailsSeen", "execDetails", "exch=SMART execId=E1 time=20260313 09:30:02",
                                  0.05, 45.64, 1, 1.0));

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        TradeTrace recovered;
        recovered.traceId = 100;
        recovered.orderId = 113;
        recovered.permId = 5001;
        recovered.source = "Broker Reconcile";
        recovered.symbol = "INTC";
        recovered.side = "BUY";
        recovered.requestedQty = 1;
        recovered.limitPrice = 45.64;
        FillSlice fill;
        fill.execId = "E1";
        fill.shares = 1;
        fill.price = 45.64;
        recovered.fills.push_back(fill);
        g_data.traces[recovered.traceId] = recovered;
        g_data.traceRecency.push_back(recovered.traceId);
        g_data.latestTraceId = recovered.traceId;
        g_data.traceIdByOrderId[recovered.orderId] = recovered.traceId;
        g_data.traceIdByPermId[recovered.permId] = recovered.traceId;
        g_data.traceIdByExecId["E1"] = recovered.traceId;
    }
    publishSharedDataSnapshot();

    TradeTraceSnapshot replayed;
    expect(replayTradeTraceSnapshotByIdentityFromLog(113, 5001, "E1", &replayed),
           "replay by identity should succeed");
    expect(replayed.found, "replayed snapshot should be found");
    expect(replayed.trace.traceId == 7, "replay should prefer the richer live trace");
    expect(replayed.trace.source == "GUI Button", "replay should use live source");

    TraceExportBundle bundle;
    std::string error;
    expect(buildTraceExportBundle(100, &bundle, &error), "export bundle should succeed: " + error);
    expectContains(bundle.summaryCsv, "GUI Button", "summary should be enriched from live trace");
    expectContains(bundle.summaryCsv, "2936", "summary should contain full fill latency");
    expectContains(bundle.timelineCsv, "ValidationStart", "timeline should include live validation stages");
}

void testBridgeBatchCodecRoundTripPreservesOrderAndMetadata() {
    const std::vector<BridgeOutboxRecord> records{
        makeBridgeRecord(101, "order_intent", "WebSocket", "INTC", "BUY",
                         71, 501, 0, "", "accepted order intent", "2026-03-14T09:30:00.100"),
        makeBridgeRecord(102, "fill_execution", "BrokerExecution", "INTC", "BOT",
                         71, 501, 9501, "EXEC-71", "execution details observed", "2026-03-14T09:30:00.250")
    };

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-phase1-order-fill";
    options.runtimeSessionId = "runtime-phase1-order-fill";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;
    options.batchSeq = 7;

    const bridge_batch::Batch batch = bridge_batch::buildBatch(records, options);
    const std::vector<std::uint8_t> frame = bridge_batch::encodeFrame(batch);
    const bridge_batch::Batch decoded = bridge_batch::decodeFrame(frame);

    expect(decoded.header.version == bridge_batch::kWireVersion, "wire version should survive codec round trip");
    expect(decoded.header.schema == bridge_batch::kSchemaName, "schema name should survive codec round trip");
    expect(decoded.header.transport == bridge_batch::kTransportName, "transport name should survive codec round trip");
    expect(decoded.header.appSessionId == options.appSessionId, "app session id should survive codec round trip");
    expect(decoded.header.runtimeSessionId == options.runtimeSessionId, "runtime session id should survive codec round trip");
    expect(decoded.header.flushReason == bridge_batch::flushReasonName(options.flushReason), "flush reason should survive codec round trip");
    expect(decoded.header.batchSeq == options.batchSeq, "batch sequence should survive codec round trip");
    expect(decoded.header.firstSourceSeq == 101, "first source sequence should reflect the first accepted record");
    expect(decoded.header.lastSourceSeq == 102, "last source sequence should reflect the last accepted record");
    expect(decoded.header.recordCount == records.size(), "record count should reflect the number of encoded records");
    expect(decoded.records.size() == records.size(), "codec round trip should preserve record count");
    expect(decoded.records.front().sourceSeq == 101, "codec round trip should preserve accepted ordering");
    expect(decoded.records.back().sourceSeq == 102, "codec round trip should preserve tail ordering");
    expect(decoded.records.front().anchor.traceId == 71, "codec round trip should preserve first trace anchor");
    expect(decoded.records.back().anchor.permId == 9501, "codec round trip should preserve fill permId");
    expect(decoded.records.back().anchor.execId == "EXEC-71", "codec round trip should preserve fill execId");
    expect(decoded.records.back().fallbackState == "queued_for_recovery", "codec round trip should preserve fallback state");
}

void testTraceIdFloorRecoversFromLog() {
    clearTestFiles();
    appendTraceLine(makeTraceLine(42, 0, 0, "GUI Button", "AAPL", "BUY",
                                  "Trigger", "GUI Button", "BUY 1 AAPL @ 100.00", 0.0));

    SharedData owner;
    bindSharedDataOwner(&owner);

    SubmitIntent intent;
    intent.source = "Test";
    intent.symbol = "MSFT";
    intent.side = "BUY";
    intent.requestedQty = 1;
    intent.limitPrice = 10.0;
    intent.triggerMono = std::chrono::steady_clock::now();
    intent.triggerWall = std::chrono::system_clock::now();

    const std::uint64_t traceId = beginTradeTrace(intent);
    expect(traceId > 42, "next trace id should advance beyond recovered log ids");

    unbindSharedDataOwner(&owner);
    resetSharedDataForTesting();
}

void testReplayHandlesPartialFillsAndCommission() {
    clearTestFiles();

    appendTraceLine(makeTraceLine(9, 114, 6001, "Controller", "INTC", "SELL",
                                  "Trigger", "Controller", "SELL 2 INTC @ 45.65", 0.0));
    appendTraceLine(makeTraceLine(9, 114, 6001, "Controller", "INTC", "SELL",
                                  "PlaceOrderCallEnd", "placeOrder", "placeOrder return", 12.0));
    appendTraceLine(makeTraceLine(9, 114, 6001, "Controller", "INTC", "SELL",
                                  "OrderStatusSeen", "Submitted", "submitted", 20.0));
    appendTraceLine(makeTraceLine(9, 114, 6001, "Controller", "INTC", "SELL",
                                  "ExecDetailsSeen", "execDetails", "exch=SMART execId=E2 time=20260313 09:31:01",
                                  800.0, 45.65, 1, 1.0));
    appendTraceLine(makeTraceLine(9, 114, 6001, "Controller", "INTC", "SELL",
                                  "CommissionSeen", "commissionReport", "E2 commission=0.3000 USD", 801.0));
    appendTraceLine(makeTraceLine(9, 114, 6001, "Controller", "INTC", "SELL",
                                  "ExecDetailsSeen", "execDetails", "exch=SMART execId=E3 time=20260313 09:31:02",
                                  1738.0, 45.66, 1, 2.0));
    appendTraceLine(makeTraceLine(9, 114, 6001, "Controller", "INTC", "SELL",
                                  "CommissionSeen", "commissionReport", "E3 commission=0.1567 USD", 1739.0));
    appendTraceLine(makeTraceLine(9, 114, 6001, "Controller", "INTC", "SELL",
                                  "FinalState", "Terminal", "Filled: order complete", 1740.0));

    TradeTraceSnapshot snapshot;
    std::string error;
    expect(replayTradeTraceSnapshotFromLog(9, &snapshot, &error), "replay should succeed: " + error);
    expect(snapshot.found, "snapshot should be found");
    expect(snapshot.trace.fills.size() == 2, "replay should preserve partial fills");
    expect(snapshot.trace.totalCommission > 0.45, "replay should accumulate commission");

    TraceExportBundle bundle;
    expect(buildTraceExportBundle(9, &bundle, &error), "export should succeed: " + error);
    expectContains(bundle.fillsCsv, "E2", "fills csv should contain first fill");
    expectContains(bundle.fillsCsv, "E3", "fills csv should contain second fill");
    expectContains(bundle.summaryCsv, "1740", "summary should contain trigger-to-fill latency");
}

void testWebSocketRuntimeGuards() {
    clearTestFiles();

    std::string error;
    expect(reserveWebSocketIdempotencyKey("abc-123", &error), "first idempotency key reserve should succeed");
    expect(!reserveWebSocketIdempotencyKey("abc-123", &error), "duplicate idempotency key should fail");
    expectContains(error, "Duplicate", "duplicate idempotency error should be descriptive");

    for (int i = 0; i < 5; ++i) {
        expect(consumeWebSocketOrderRateLimit(&error), "rate limit should allow initial order burst");
    }
    expect(!consumeWebSocketOrderRateLimit(&error), "rate limit should reject excessive order burst");
    expectContains(error, "Too many WebSocket order requests", "rate limit error should be descriptive");
}

void testRecoverySnapshotReportsAbnormalShutdown() {
    clearTestFiles();

    appendTraceLine(makeTraceLine(21, 115, 0, "Controller", "INTC", "BUY",
                                  "Trigger", "Controller", "BUY 1 INTC @ 45.64", 0.0));
    appendJournalLine(json{
        {"event", "runtime_start"},
        {"wallTime", "2026-03-13T09:30:00.000"},
        {"appSessionId", "app-test-session"},
        {"runtimeSessionId", "runtime-test-session"}
    });

    const RuntimeRecoverySnapshot snapshot = recoverRuntimeRecoverySnapshot(5);
    expect(snapshot.priorSessionAbnormal, "recovery snapshot should flag abnormal prior session");
    expect(snapshot.priorAppSessionId == "app-test-session", "recovery snapshot should preserve prior app session id");
    expect(snapshot.priorRuntimeSessionId == "runtime-test-session", "recovery snapshot should preserve prior runtime session id");
    expect(snapshot.unfinishedTraceCount == 1, "recovery snapshot should count unfinished traces");
    expectContains(snapshot.bannerText, "Previous session ended unexpectedly", "recovery banner should mention abnormal shutdown");
    expectContains(snapshot.bannerText, "1 unfinished trace", "recovery banner should mention unfinished trace count");
}

void testBridgeOutboxSourceSeqPreservesAcceptanceOrderingAndAnchors() {
    clearTestFiles();

    appendJournalLine(json{
        {"event", "runtime_start"},
        {"wallTime", "2026-03-13T09:30:00.000"},
        {"appSessionId", "app-ordering-session"},
        {"runtimeSessionId", "runtime-ordering-session"}
    });
    appendJournalLine(json{
        {"event", "bridge_outbox_queued"},
        {"wallTime", "2026-03-13T09:30:02.000"},
        {"appSessionId", "app-ordering-session"},
        {"runtimeSessionId", "runtime-ordering-session"},
        {"details", json{{"sourceSeq", 9ULL}}}
    });
    appendJournalLine(json{
        {"event", "bridge_outbox_queued"},
        {"wallTime", "2026-03-13T09:29:59.000"},
        {"appSessionId", "app-ordering-session"},
        {"runtimeSessionId", "runtime-ordering-session"},
        {"details", json{{"sourceSeq", 12ULL}}}
    });

    SharedData owner;
    bindSharedDataOwner(&owner);

    BridgeOutboxRecordInput intentRecord;
    intentRecord.recordType = "order_intent";
    intentRecord.source = "WebSocket";
    intentRecord.symbol = "intc";
    intentRecord.side = "BUY";
    intentRecord.traceId = 71;
    intentRecord.orderId = 501;
    intentRecord.note = "accepted order intent";
    const BridgeOutboxEnqueueResult accepted = enqueueBridgeOutboxRecord(intentRecord);

    BridgeOutboxRecordInput fillRecord;
    fillRecord.recordType = "fill_execution";
    fillRecord.source = "BrokerExecution";
    fillRecord.symbol = "intc";
    fillRecord.side = "BOT";
    fillRecord.traceId = 71;
    fillRecord.orderId = 501;
    fillRecord.permId = 9501;
    fillRecord.execId = "EXEC-71";
    fillRecord.note = "execution details observed";
    const BridgeOutboxEnqueueResult filled = enqueueBridgeOutboxRecord(fillRecord);

    const BridgeOutboxSnapshot snapshot = captureBridgeOutboxSnapshot(10);
    expect(accepted.queued, "accepted intent should be queued for bridge recovery");
    expect(filled.queued, "fill execution should be queued for bridge recovery");
    expect(accepted.sourceSeq == 13, "source_seq should continue from the highest recovered journal sequence");
    expect(filled.sourceSeq == 14, "source_seq should advance in acceptance order");
    expect(snapshot.pendingCount == 2, "bridge snapshot should report queued records");
    expect(snapshot.lossCount == 0, "bridge snapshot should not invent loss markers");
    expect(snapshot.recoveryRequired, "bridge snapshot should require recovery while records are pending");
    expect(snapshot.lastSourceSeq == filled.sourceSeq, "bridge snapshot should track the highest accepted source_seq");
    expect(snapshot.records.size() == 2, "bridge snapshot should include both queued records");

    const BridgeOutboxRecord& newest = snapshot.records.front();
    const BridgeOutboxRecord& oldest = snapshot.records.back();
    expect(newest.sourceSeq == filled.sourceSeq, "bridge snapshot should expose the newest accepted record first");
    expect(oldest.sourceSeq == accepted.sourceSeq, "older accepted records should retain lower source_seq values");
    expect(oldest.anchor.traceId == 71, "accepted order intent should preserve traceId");
    expect(oldest.anchor.orderId == 501, "accepted order intent should preserve orderId");
    expect(newest.anchor.traceId == 71, "fill execution should preserve traceId");
    expect(newest.anchor.orderId == 501, "fill execution should preserve orderId");
    expect(newest.anchor.permId == 9501, "fill execution should preserve permId");
    expect(newest.anchor.execId == "EXEC-71", "fill execution should preserve execId");
    expect(newest.symbol == "INTC", "bridge snapshot should normalize symbols without changing acceptance order");
    expect(newest.fallbackState == "queued_for_recovery", "bridge snapshot should surface explicit fallback state");
    expect(newest.fallbackReason == "engine_unavailable", "bridge snapshot should surface explicit fallback reason");

    unbindSharedDataOwner(&owner);
    resetSharedDataForTesting();
}

void testBridgeBatchFixtureMatchesGoldenFrame() {
    const json fixtureJson = json::parse(readTextFile(fixturePath("bridge_batch_v1_order_fill.json")));
    const bridge_batch::Batch fixtureBatch = bridge_batch::batchFromJson(fixtureJson);
    const std::string expectedHex = trimWhitespace(readTextFile(fixturePath("bridge_batch_v1_order_fill.msgpack.hex")));
    const std::string actualHex = bridge_batch::encodeFrameHex(fixtureBatch);

    expect(actualHex == expectedHex, "golden frame should match the fixture-backed bridge batch contract");

    const bridge_batch::Batch decoded = bridge_batch::decodeFrame(bridge_batch::decodeHex(expectedHex));
    expect(decoded.header.batchSeq == 7, "golden frame should decode the expected batch sequence");
    expect(decoded.header.firstSourceSeq == 101, "golden frame should decode the expected first source sequence");
    expect(decoded.header.lastSourceSeq == 102, "golden frame should decode the expected last source sequence");
    expect(decoded.records.size() == 2, "golden frame should decode both bridge records");
    expect(decoded.records.front().recordType == "order_intent", "golden frame should keep the first record type");
    expect(decoded.records.back().recordType == "fill_execution", "golden frame should keep the second record type");
    expect(decoded.records.back().anchor.execId == "EXEC-71", "golden frame should keep the fill execution anchor");
}

void testBridgeBatchSenderPreservesOrderingAcrossRetries() {
    bridge_batch::FakeTransport transport;
    bridge_batch::Sender sender(transport);

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-phase1-order-fill";
    options.runtimeSessionId = "runtime-phase1-order-fill";
    options.flushReason = bridge_batch::FlushReason::RecoveryDrain;

    options.batchSeq = 1;
    const bridge_batch::Batch first = bridge_batch::buildBatch({
        makeBridgeRecord(201, "order_intent", "WebSocket", "INTC", "BUY",
                         88, 601, 0, "", "first batch", "2026-03-14T09:31:00.000")
    }, options);

    options.batchSeq = 2;
    const bridge_batch::Batch second = bridge_batch::buildBatch({
        makeBridgeRecord(202, "fill_execution", "BrokerExecution", "INTC", "BOT",
                         88, 601, 9601, "EXEC-88-A", "second batch", "2026-03-14T09:31:00.150")
    }, options);

    options.batchSeq = 3;
    const bridge_batch::Batch third = bridge_batch::buildBatch({
        makeBridgeRecord(203, "fill_execution", "BrokerExecution", "INTC", "BOT",
                         88, 601, 9601, "EXEC-88-B", "third batch", "2026-03-14T09:31:00.300")
    }, options);

    transport.failNext("engine offline");
    const bridge_batch::PublishResult firstResult = sender.publish(first);
    expect(!firstResult.delivered, "first publish should fail while transport is unavailable");
    expect(firstResult.queuedForRetry, "failed first publish should remain queued for retry");
    expect(firstResult.pendingBatchCount == 1, "first failed publish should leave one pending batch");

    const bridge_batch::PublishResult secondResult = sender.publish(second);
    expect(!secondResult.delivered, "second publish should not bypass an earlier failed batch");
    expect(secondResult.queuedForRetry, "second publish should queue behind pending retry work");
    expect(secondResult.pendingBatchCount == 2, "second publish should extend the pending retry queue");

    const bridge_batch::PublishResult thirdResult = sender.publish(third);
    expect(!thirdResult.delivered, "third publish should wait behind earlier pending batches");
    expect(thirdResult.queuedForRetry, "third publish should queue behind earlier pending batches");
    expect(thirdResult.pendingBatchCount == 3, "third publish should leave all queued batches pending");

    const bridge_batch::DrainResult drain = sender.drainPending();
    expect(!drain.blocked, "draining pending batches should succeed once transport recovers");
    expect(drain.deliveredCount == 3, "drain should deliver every queued batch");
    expect(drain.pendingBatchCount == 0, "drain should empty the pending queue");
    expect(sender.pendingBatchCount() == 0, "sender should report no pending batches after drain");
    expect(transport.attemptedSendCount() == 4, "transport should see one failed attempt plus three successful retries");
    expect(transport.deliveredFrames().size() == 3, "transport should receive all three batches in delivery order");

    const bridge_batch::Batch deliveredFirst = bridge_batch::decodeFrame(transport.deliveredFrames().at(0));
    const bridge_batch::Batch deliveredSecond = bridge_batch::decodeFrame(transport.deliveredFrames().at(1));
    const bridge_batch::Batch deliveredThird = bridge_batch::decodeFrame(transport.deliveredFrames().at(2));
    expect(deliveredFirst.header.batchSeq == 1, "drain should preserve the first queued batch");
    expect(deliveredSecond.header.batchSeq == 2, "drain should preserve the second queued batch");
    expect(deliveredThird.header.batchSeq == 3, "drain should preserve the third queued batch");
    expect(deliveredFirst.records.front().sourceSeq == 201, "drain should preserve source ordering in the first batch");
    expect(deliveredSecond.records.front().sourceSeq == 202, "drain should preserve source ordering in the second batch");
    expect(deliveredThird.records.front().sourceSeq == 203, "drain should preserve source ordering in the third batch");
}

void testBridgeDispatchBatchRespectsThresholdsAndImmediateFlush() {
    bridge_batch::BuildOptions options;
    options.appSessionId = "app-dispatch-thresholds";
    options.runtimeSessionId = "runtime-dispatch-thresholds";
    options.flushReason = bridge_batch::FlushReason::Manual;
    options.batchSeq = 11;

    std::vector<BridgeOutboxRecord> burst;
    burst.reserve(70);
    for (std::size_t i = 0; i < 70; ++i) {
        burst.push_back(makeBridgeRecord(500 + i,
                                         "depth_update",
                                         "BookFeed",
                                         "INTC",
                                         "BUY",
                                         900 + i,
                                         700 + static_cast<OrderId>(i),
                                         0,
                                         "",
                                         "depth burst",
                                         "2026-03-14T09:32:00.000"));
    }

    bridge_batch::BatchPolicy policy;
    policy.maxRecords = 64;
    policy.maxPayloadBytes = 64 * 1024;
    const bridge_batch::PreparedBatch thresholdBatch = bridge_batch::prepareBatch(burst, options, policy);
    expect(thresholdBatch.batch.records.size() == 64, "prepareBatch should cap the batch at the record threshold");
    expect(thresholdBatch.reachedRecordLimit, "prepareBatch should report when the record threshold forced a flush");
    expect(!thresholdBatch.immediateFlush, "depth-only batches should wait for threshold or timer flush");

    const std::vector<BridgeOutboxRecord> immediateRecords{
        makeBridgeRecord(801, "order_intent", "WebSocket", "INTC", "BUY",
                         71, 501, 0, "", "accepted order intent", "2026-03-14T09:33:00.100"),
        makeBridgeRecord(802, "depth_update", "BookFeed", "INTC", "BUY",
                         71, 501, 0, "", "book follow-up", "2026-03-14T09:33:00.120")
    };
    const bridge_batch::PreparedBatch immediateBatch = bridge_batch::prepareBatch(immediateRecords, options, policy);
    expect(immediateBatch.immediateFlush, "order and fill lifecycle records should force immediate flush");
    expect(bridge_batch::recordTypeRequiresImmediateFlush("open_order"), "open_order should force immediate bridge flush");
    expect(bridge_batch::recordTypeRequiresImmediateFlush("order_status"), "order_status should force immediate bridge flush");
    expect(bridge_batch::recordTypeRequiresImmediateFlush("commission_report"), "commission_report should force immediate bridge flush");
    expect(bridge_batch::recordTypeRequiresImmediateFlush("broker_error"), "broker_error should force immediate bridge flush");
    expect(bridge_batch::recordTypeRequiresImmediateFlush("feed_reset"), "feed_reset should force immediate bridge flush");
    expect(bridge_batch::recordTypeRequiresImmediateFlush("gap_marker"), "gap_marker should force immediate bridge flush");
    expect(bridge_batch::recordTypeRequiresImmediateFlush("reset_marker"), "reset_marker should force immediate bridge flush");
    expect(!bridge_batch::recordTypeRequiresImmediateFlush("market_depth"), "market_depth should remain batchable");

    bridge_batch::BatchPolicy bytePolicy;
    bytePolicy.maxRecords = 64;
    bytePolicy.maxPayloadBytes = 420;
    const std::vector<BridgeOutboxRecord> byteBoundRecords{
        makeBridgeRecord(901, "depth_update", "BookFeed", "INTC", "BUY",
                         71, 501, 0, "", std::string(220, 'a'), "2026-03-14T09:34:00.100"),
        makeBridgeRecord(902, "depth_update", "BookFeed", "INTC", "BUY",
                         71, 501, 0, "", std::string(220, 'b'), "2026-03-14T09:34:00.120")
    };
    const bridge_batch::PreparedBatch byteBatch = bridge_batch::prepareBatch(byteBoundRecords, options, bytePolicy);
    expect(byteBatch.batch.records.size() == 1, "prepareBatch should stop before exceeding the payload threshold");
    expect(byteBatch.reachedPayloadLimit, "prepareBatch should report payload-triggered flushes");
}

void testBridgeDispatchSnapshotAndDeliveryAck() {
    clearTestFiles();

    SharedData owner;
    bindSharedDataOwner(&owner);

    BridgeOutboxRecordInput firstRecord;
    firstRecord.recordType = "order_intent";
    firstRecord.source = "WebSocket";
    firstRecord.symbol = "INTC";
    firstRecord.side = "BUY";
    firstRecord.traceId = 71;
    firstRecord.orderId = 501;
    firstRecord.note = "accepted order intent";
    const BridgeOutboxEnqueueResult first = enqueueBridgeOutboxRecord(firstRecord);

    BridgeOutboxRecordInput secondRecord;
    secondRecord.recordType = "fill_execution";
    secondRecord.source = "BrokerExecution";
    secondRecord.symbol = "INTC";
    secondRecord.side = "BOT";
    secondRecord.traceId = 71;
    secondRecord.orderId = 501;
    secondRecord.permId = 9501;
    secondRecord.execId = "EXEC-71";
    secondRecord.note = "execution details observed";
    const BridgeOutboxEnqueueResult second = enqueueBridgeOutboxRecord(secondRecord);

    const BridgeDispatchSnapshot dispatch = captureBridgeDispatchSnapshot(10);
    expect(dispatch.records.size() == 2, "dispatch snapshot should expose queued bridge records oldest-first");
    expect(dispatch.records.front().sourceSeq == first.sourceSeq, "dispatch snapshot should begin with the oldest queued record");
    expect(dispatch.records.back().sourceSeq == second.sourceSeq, "dispatch snapshot should preserve acceptance ordering");

    const std::size_t removedFirst = acknowledgeDeliveredBridgeRecords({dispatch.records.front()});
    expect(removedFirst == 1, "delivery ack should remove the delivered prefix record");
    BridgeOutboxSnapshot afterFirstAck = captureBridgeOutboxSnapshot(10);
    expect(afterFirstAck.pendingCount == 1, "delivery ack should decrement the pending outbox count");
    expect(afterFirstAck.records.size() == 1, "delivery ack should leave only the undelivered tail");
    expect(afterFirstAck.records.front().sourceSeq == second.sourceSeq, "delivery ack should preserve the remaining undelivered record");

    const std::size_t removedSecond = acknowledgeDeliveredBridgeRecords({dispatch.records.back()});
    expect(removedSecond == 1, "delivery ack should remove the final queued record");
    const BridgeOutboxSnapshot afterSecondAck = captureBridgeOutboxSnapshot(10);
    expect(afterSecondAck.pendingCount == 0, "delivery ack should clear the pending outbox count once all records are delivered");
    expect(!afterSecondAck.recoveryRequired, "delivery ack should clear recovery-required once no pending or lost records remain");
    expect(afterSecondAck.fallbackState == "live_delivery", "delivery ack should mark the bridge as live once the outbox is drained");
    expect(afterSecondAck.fallbackReason == "engine_connected", "delivery ack should surface the live bridge reason once drained");

    const std::vector<json> journalLines = readJsonLines(runtimeJournalLogPath());
    const auto deliveredCount = static_cast<int>(std::count_if(journalLines.begin(),
                                                               journalLines.end(),
                                                               [](const json& line) {
                                                                   return line.value("event", std::string()) == "bridge_outbox_delivered";
                                                               }));
    expect(deliveredCount == 2, "delivery ack should journal one delivery marker per delivered record");

    unbindSharedDataOwner(&owner);
    resetSharedDataForTesting();
}

void testUnixDomainSocketTransportSendsFramedBatch() {
    const fs::path socketPath = testDataDir() / "bridge-uds-test.sock";
    std::error_code ec;
    fs::remove(socketPath, ec);

    const int serverFd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    expect(serverFd >= 0, "unix-domain test server should create a socket");

    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    std::strncpy(address.sun_path, socketPath.c_str(), sizeof(address.sun_path) - 1);
    expect(::bind(serverFd, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) == 0,
           "unix-domain test server should bind successfully");
    expect(::listen(serverFd, 1) == 0, "unix-domain test server should listen successfully");

    std::vector<std::uint8_t> received;
    std::string serverError;
    std::thread server([&]() {
        const int clientFd = ::accept(serverFd, nullptr, nullptr);
        if (clientFd < 0) {
            serverError = "accept failed: " + std::string(std::strerror(errno));
            return;
        }
        received = readAllFromFd(clientFd);
        tape_engine::IngestAck ack;
        ack.batchSeq = 21;
        ack.adapterId = "long";
        ack.connectionId = "runtime-uds-transport";
        ack.acceptedRecords = 1;
        ack.firstSessionSeq = 1;
        ack.lastSessionSeq = 1;
        ack.firstSourceSeq = 1001;
        ack.lastSourceSeq = 1001;
        const std::vector<std::uint8_t> ackFrame = tape_engine::encodeAckFrame(ack);
        try {
            writeAllToFd(clientFd, ackFrame);
        } catch (const std::exception& error) {
            serverError = error.what();
        }
        ::close(clientFd);
    });

    const std::vector<BridgeOutboxRecord> records{
        makeBridgeRecord(1001, "order_intent", "WebSocket", "INTC", "BUY",
                         71, 501, 0, "", "accepted order intent", "2026-03-14T09:35:00.100")
    };
    bridge_batch::BuildOptions options;
    options.appSessionId = "app-uds-transport";
    options.runtimeSessionId = "runtime-uds-transport";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;
    options.batchSeq = 21;
    const bridge_batch::Batch batch = bridge_batch::buildBatch(records, options);

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;
    expect(transport.sendFrame(bridge_batch::encodeFrame(batch), &error), "unix-domain transport should deliver a framed batch: " + error);

    server.join();
    ::close(serverFd);
    fs::remove(socketPath, ec);
    expect(serverError.empty(), serverError);

    const bridge_batch::Batch decoded = bridge_batch::decodeFrame(received);
    expect(decoded.header.batchSeq == 21, "unix-domain transport should preserve the framed batch sequence");
    expect(decoded.records.size() == 1, "unix-domain transport should preserve the framed bridge record count");
    expect(decoded.records.front().sourceSeq == 1001, "unix-domain transport should preserve the framed source sequence");
}

void testTapeEngineAcceptsBatchAssignsSessionSeqAndWritesSegments() {
    const fs::path rootDir = testDataDir() / "tape-engine-phase1";
    const fs::path socketPath = testDataDir() / "tape-engine-phase1.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "INTC";
    config.ringCapacity = 32;

    tape_engine::Server server(config);
    std::string startError;
    expect(server.start(&startError), "tape-engine should start: " + startError);

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-engine-phase1";
    options.runtimeSessionId = "runtime-engine-phase1";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;
    options.batchSeq = 31;
    const bridge_batch::Batch batch = bridge_batch::buildBatch({
        makeBridgeRecord(1101, "order_intent", "WebSocket", "INTC", "BUY",
                         71, 501, 0, "", "accepted order intent", "2026-03-14T09:36:00.100"),
        makeBridgeRecord(1102, "fill_execution", "BrokerExecution", "INTC", "BOT",
                         71, 501, 9501, "EXEC-71", "execution details observed", "2026-03-14T09:36:00.250")
    }, options);

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;
    expect(transport.sendFrame(bridge_batch::encodeFrame(batch), &error),
           "tape-engine should accept a real bridge batch: " + error);

    waitUntil([&]() {
        const auto snapshot = server.snapshot();
        return snapshot.segments.size() >= 1 && snapshot.latestFrozenRevisionId >= 1;
    }, "tape-engine should freeze the accepted batch into at least one segment");

    const tape_engine::EngineSnapshot snapshot = server.snapshot();
    expect(snapshot.nextSessionSeq == 3, "tape-engine should assign contiguous session_seq values to accepted records");
    expect(snapshot.liveEvents.size() == 2, "tape-engine live ring should retain accepted records");
    expect(snapshot.liveEvents.front().sessionSeq == 1, "tape-engine should assign the first session_seq");
    expect(snapshot.liveEvents.back().sessionSeq == 2, "tape-engine should assign the second session_seq");
    expect(snapshot.liveEvents.back().bridgeRecord.anchor.execId == "EXEC-71", "tape-engine should preserve bridge anchors");
    expect(snapshot.segments.size() >= 1, "tape-engine should emit at least one segment for the accepted batch");
    expect(snapshot.segments.front().revisionId == 1, "tape-engine should assign a frozen revision id to the segment");
    expect(snapshot.segments.front().fileName.find(".events.msgpack") != std::string::npos,
           "tape-engine should freeze segment payloads as binary msgpack files");
    expect(snapshot.segments.front().indexFileName.find(".index.msgpack") != std::string::npos,
           "tape-engine should persist a binary selector index alongside the segment payload");
    expect(snapshot.segments.front().checkpointFileName.find(".checkpoint.msgpack") != std::string::npos,
           "tape-engine should persist a binary replay checkpoint alongside the segment payload");
    expect(snapshot.segments.front().firstSessionSeq == 1, "tape-engine segment metadata should preserve the first session_seq");
    expect(snapshot.segments.front().lastSessionSeq == 2, "tape-engine segment metadata should preserve the last session_seq");
    expect(!snapshot.segments.front().payloadSha256.empty(), "tape-engine segment metadata should include a payload checksum");
    expect(!snapshot.segments.front().manifestHash.empty(), "tape-engine segment metadata should include a manifest hash");

    const fs::path segmentPath = rootDir / "segments" / snapshot.segments.front().fileName;
    const fs::path indexPath = rootDir / "segments" / snapshot.segments.front().indexFileName;
    const fs::path checkpointPath = rootDir / "segments" / snapshot.segments.front().checkpointFileName;
    const fs::path metadataPath = rootDir / "segments" / snapshot.segments.front().metadataFileName;
    const fs::path manifestPath = rootDir / "manifest.jsonl";
    expect(fs::exists(segmentPath), "tape-engine should write the segment payload file");
    expect(fs::exists(indexPath), "tape-engine should write the segment selector index file");
    expect(fs::exists(checkpointPath), "tape-engine should write the segment replay checkpoint file");
    expect(fs::exists(metadataPath), "tape-engine should write the segment metadata file");
    expect(fs::exists(manifestPath), "tape-engine should append the manifest hash chain");

    const std::vector<json> manifestLines = readJsonLines(manifestPath.string());
    expect(manifestLines.size() >= 1, "tape-engine should append at least one manifest line for the accepted batch");
    expect(manifestLines.front().value("manifest_hash", std::string()) == snapshot.segments.front().manifestHash,
           "tape-engine manifest hash should match the in-memory segment metadata");
    expect(manifestLines.front().value("index_file_name", std::string()) == snapshot.segments.front().indexFileName,
           "tape-engine manifest metadata should include the selector index file");
    expect(manifestLines.front().value("checkpoint_file_name", std::string()) == snapshot.segments.front().checkpointFileName,
           "tape-engine manifest metadata should include the replay checkpoint file");

    server.stop();
}

void testTapeEngineEmitsGapMarkersAndDeduplicatesSourceSeq() {
    const fs::path rootDir = testDataDir() / "tape-engine-gap";
    const fs::path socketPath = testDataDir() / "tape-engine-gap.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "INTC";
    config.ringCapacity = 32;

    tape_engine::Server server(config);
    std::string startError;
    expect(server.start(&startError), "tape-engine should start for gap test: " + startError);

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-engine-gap";
    options.runtimeSessionId = "runtime-engine-gap";
    options.flushReason = bridge_batch::FlushReason::Manual;

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;

    options.batchSeq = 41;
    const bridge_batch::Batch first = bridge_batch::buildBatch({
        makeBridgeRecord(2001, "order_intent", "WebSocket", "INTC", "BUY",
                         71, 501, 0, "", "first", "2026-03-14T09:37:00.100")
    }, options);
    expect(transport.sendFrame(bridge_batch::encodeFrame(first), &error),
           "tape-engine should accept the first batch: " + error);

    options.batchSeq = 42;
    const bridge_batch::Batch duplicateAndGap = bridge_batch::buildBatch({
        makeBridgeRecord(2001, "order_intent", "WebSocket", "INTC", "BUY",
                         71, 501, 0, "", "duplicate", "2026-03-14T09:37:00.200"),
        makeBridgeRecord(2003, "fill_execution", "BrokerExecution", "INTC", "BOT",
                         71, 501, 9501, "EXEC-71", "gap after duplicate", "2026-03-14T09:37:00.300")
    }, options);
    expect(transport.sendFrame(bridge_batch::encodeFrame(duplicateAndGap), &error),
           "tape-engine should ignore duplicates across accepted history and still emit a gap marker: " + error);

    const tape_engine::EngineSnapshot snapshot = server.snapshot();
    expect(snapshot.liveEvents.size() == 3, "tape-engine should retain the original event, gap marker, and later event");
    expect(snapshot.liveEvents.at(1).eventKind == "gap_marker", "tape-engine should insert an explicit gap_marker event");
    expect(snapshot.liveEvents.at(1).gapStartSourceSeq == 2002, "gap marker should preserve the missing source_seq start");
    expect(snapshot.liveEvents.at(1).gapEndSourceSeq == 2002, "gap marker should preserve the missing source_seq end");
    expect(snapshot.liveEvents.at(2).sourceSeq == 2003, "tape-engine should still accept the later source_seq");

    server.stop();
}

void testTapeEngineQueryStatusAndReads() {
    const fs::path rootDir = testDataDir() / "tape-engine-query";
    const fs::path socketPath = testDataDir() / "tape-engine-query.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "INTC";
    config.ringCapacity = 32;

    tape_engine::Server server(config);
    std::string startError;
    expect(server.start(&startError), "tape-engine should start for query test: " + startError);

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-engine-query";
    options.runtimeSessionId = "runtime-engine-query";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;

    options.batchSeq = 51;
    const bridge_batch::Batch first = bridge_batch::buildBatch({
        makeBridgeRecord(3001, "order_intent", "WebSocket", "INTC", "BUY",
                         91, 701, 0, "", "accepted order intent", "2026-03-14T09:38:00.100"),
        makeBridgeRecord(3002, "order_status", "BrokerOrderStatus", "INTC", "BUY",
                         91, 701, 9801, "", "Submitted: filled=0 remaining=1", "2026-03-14T09:38:00.150")
    }, options);
    expect(transport.sendFrame(bridge_batch::encodeFrame(first), &error),
           "tape-engine should accept the first query batch: " + error);

    options.batchSeq = 52;
    const bridge_batch::Batch second = bridge_batch::buildBatch({
        makeBridgeRecord(3004, "fill_execution", "BrokerExecution", "INTC", "BOT",
                         91, 701, 9801, "EXEC-91", "execution details observed", "2026-03-14T09:38:00.250")
    }, options);
    expect(transport.sendFrame(bridge_batch::encodeFrame(second), &error),
           "tape-engine should accept the second query batch: " + error);

    waitUntil([&]() {
        const auto snapshot = server.snapshot();
        return snapshot.segments.size() >= 2 && snapshot.latestFrozenRevisionId >= 2;
    }, "tape-engine should freeze both query batches before frozen reads");

    tape_engine::Client client(socketPath.string());
    tape_engine::QueryResponse response;

    tape_engine::QueryRequest statusRequest;
    statusRequest.requestId = "status-1";
    statusRequest.operation = "status";
    expect(client.query(statusRequest, &response, &error), "tape-engine status query should succeed: " + error);
    expect(response.summary.value("segment_count", 0ULL) >= 2, "status query should report at least the written segment count");
    expect(response.summary.value("latest_frozen_revision_id", 0ULL) >= 2, "status query should expose a frozen revision at or beyond the ingest batches");
    expect(response.summary.value("latest_session_seq", 0ULL) == 4, "status query should report the latest accepted session_seq");

    tape_engine::QueryRequest rangeRequest;
    rangeRequest.requestId = "range-1";
    rangeRequest.operation = "read_range";
    rangeRequest.fromSessionSeq = 2;
    rangeRequest.toSessionSeq = 4;
    expect(client.query(rangeRequest, &response, &error), "tape-engine range query should succeed: " + error);
    expect(response.events.is_array() && response.events.size() == 3, "range query should return the requested session_seq window");
    expect(response.summary.value("served_revision_id", 0ULL) >= 2, "range query should say which frozen revision served the read");
    expect(response.events.at(0).value("event_kind", std::string()) == "order_status", "range query should include the original order status event");
    expect(response.events.at(1).value("event_kind", std::string()) == "gap_marker", "range query should expose the explicit gap marker");
    expect(response.events.at(2).value("event_kind", std::string()) == "fill_execution", "range query should expose the later fill event");

    tape_engine::QueryRequest liveTailRequest;
    liveTailRequest.requestId = "tail-1";
    liveTailRequest.operation = "read_live_tail";
    liveTailRequest.limit = 2;
    expect(client.query(liveTailRequest, &response, &error), "tape-engine live tail query should succeed: " + error);
    expect(response.events.is_array() && response.events.size() == 2, "live tail query should honor the requested limit");
    expect(response.events.at(0).value("event_kind", std::string()) == "gap_marker", "live tail query should expose the penultimate live event");
    expect(response.events.at(1).value("event_kind", std::string()) == "fill_execution", "live tail query should expose the latest live event");

    tape_engine::QueryRequest anchorRequest;
    anchorRequest.requestId = "anchor-1";
    anchorRequest.operation = "find_order_anchor";
    anchorRequest.orderId = 701;
    expect(client.query(anchorRequest, &response, &error), "tape-engine anchor query should succeed: " + error);
    expect(response.events.is_array() && response.events.size() == 3, "anchor query should return the order-anchored lifecycle events");
    expect(response.events.at(0).value("event_kind", std::string()) == "order_intent", "anchor query should begin with the queued order intent");
    expect(response.events.at(2).value("event_kind", std::string()) == "fill_execution", "anchor query should include the fill execution");

    tape_engine::QueryRequest seekRequest;
    seekRequest.requestId = "seek-1";
    seekRequest.operation = "seek_order_anchor";
    seekRequest.orderId = 701;
    expect(client.query(seekRequest, &response, &error), "tape-engine seek-order query should succeed: " + error);
    expect(response.summary.value("first_session_seq", 0ULL) == 1, "seek-order should report the first anchored session_seq");
    expect(response.summary.value("last_session_seq", 0ULL) == 4, "seek-order should report the final anchored session_seq");
    expect(response.summary.value("last_fill_session_seq", 0ULL) == 4, "seek-order should identify the fill session_seq as the replay target");
    expect(response.summary.value("replay_target_session_seq", 0ULL) == 4, "seek-order should point replay at the fill when one exists");
    expect(response.summary.contains("protected_window"), "seek-order should include protected-window context");

    tape_engine::QueryRequest qualityRequest;
    qualityRequest.requestId = "quality-1";
    qualityRequest.operation = "read_session_quality";
    qualityRequest.fromSessionSeq = 1;
    qualityRequest.toSessionSeq = 4;
    expect(client.query(qualityRequest, &response, &error), "tape-engine session-quality query should succeed: " + error);
    expect(response.summary.contains("data_quality"), "session-quality query should return a data-quality block");
    expect(response.summary.value("data_quality", json::object()).value("gap_marker_count", 0ULL) == 1ULL,
           "session-quality query should count explicit gap markers");
    expect(response.summary.value("data_quality", json::object()).value("weak_instrument_identity_count", 0ULL) >= 1ULL,
           "session-quality query should flag weak identity fallback when only symbol-style identity is available");

    server.stop();
}

void testTapeScopeClientReadsPhase4EngineSeam() {
    const fs::path rootDir = testDataDir() / "tapescope-phase4-client";
    const fs::path socketPath = testDataDir() / "tapescope-phase4-client.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "ib:conid:9301:STK:SMART:USD:INTC";
    config.ringCapacity = 32;

    tape_engine::Server server(config);
    std::string startError;
    expect(server.start(&startError), "tape-engine should start for TapeScope phase4 seam test: " + startError);

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-tapescope-phase4";
    options.runtimeSessionId = "runtime-tapescope-phase4";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;

    options.batchSeq = 201;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        makeBridgeRecord(9101, "order_intent", "WebSocket", "INTC", "BUY",
                         401, 7401, 0, "", "tapescope order intent", "2026-03-15T09:41:00.100"),
        makeBridgeRecord(9102, "order_status", "BrokerOrderStatus", "INTC", "BUY",
                         401, 7401, 8801, "", "Submitted: filled=0 remaining=1", "2026-03-15T09:41:00.120")
    }, options)), &error), "tape-engine should accept the TapeScope seam first batch: " + error);

    options.batchSeq = 202;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        makeBridgeRecord(9104, "fill_execution", "BrokerExecution", "INTC", "BOT",
                         401, 7401, 8801, "EXEC-401", "phase4 fill", "2026-03-15T09:41:00.250")
    }, options)), &error), "tape-engine should accept the TapeScope seam second batch: " + error);

    waitUntil([&]() {
        const auto snapshot = server.snapshot();
        return snapshot.segments.size() >= 2 && snapshot.latestFrozenRevisionId >= 2;
    }, "tape-engine should freeze TapeScope seam batches");

    tapescope::ClientConfig clientConfig;
    clientConfig.socketPath = socketPath.string();
    tapescope::QueryClient client(clientConfig);

    const auto status = client.status();
    expect(status.ok(), "TapeScope status should succeed: " + tapescope::QueryClient::describeError(status.error));
    expect(status.value.instrumentId == "ib:conid:9301:STK:SMART:USD:INTC",
           "TapeScope status should expose the engine instrument id");
    expect(status.value.latestSessionSeq == 4,
           "TapeScope status should expose the latest session seq");

    const auto liveTail = client.readLiveTail(2);
    expect(liveTail.ok(), "TapeScope live tail should succeed: " + tapescope::QueryClient::describeError(liveTail.error));
    expect(liveTail.value.size() == 2, "TapeScope live tail should honor the requested limit");
    expect(liveTail.value.back().value("event_kind", std::string()) == "fill_execution",
           "TapeScope live tail should expose the latest fill");

    tapescope::RangeQuery range;
    range.firstSessionSeq = 2;
    range.lastSessionSeq = 4;
    const auto rangeResult = client.readRange(range);
    expect(rangeResult.ok(), "TapeScope range read should succeed: " + tapescope::QueryClient::describeError(rangeResult.error));
    expect(rangeResult.value.size() == 3, "TapeScope range read should return the requested seq window");
    expect(rangeResult.value.front().value("event_kind", std::string()) == "order_status",
           "TapeScope range read should include the order status event");

    tapescope::OrderAnchorQuery anchor;
    anchor.orderId = 7401;
    const auto anchorResult = client.findOrderAnchor(anchor);
    expect(anchorResult.ok(), "TapeScope order-anchor lookup should succeed: " + tapescope::QueryClient::describeError(anchorResult.error));
    expect(anchorResult.value.value("events", json::array()).size() == 3,
           "TapeScope order-anchor lookup should return the anchored lifecycle events");

    const auto orderCaseResult = client.readOrderCase(anchor);
    expect(orderCaseResult.ok(), "TapeScope order-case read should succeed: " + tapescope::QueryClient::describeError(orderCaseResult.error));
    expect(orderCaseResult.value.value("summary", json::object()).contains("report"),
           "TapeScope order-case read should expose the investigation report envelope");

    const auto seekResult = client.seekOrderAnchor(anchor);
    expect(seekResult.ok(), "TapeScope seek-order read should succeed: " + tapescope::QueryClient::describeError(seekResult.error));
    expect(seekResult.value.value("summary", json::object()).value("replay_target_session_seq", 0ULL) == 4,
           "TapeScope seek-order read should expose the replay target session seq");

    const auto overviewResult = client.readSessionOverview(range);
    expect(overviewResult.ok(), "TapeScope session-overview read should succeed: " + tapescope::QueryClient::describeError(overviewResult.error));
    expect(overviewResult.value.value("summary", json::object()).contains("report"),
           "TapeScope session-overview read should expose the overview report envelope");

    const auto sessionReportResult = client.scanSessionReport(range);
    expect(sessionReportResult.ok(), "TapeScope session-report scan should succeed: " + tapescope::QueryClient::describeError(sessionReportResult.error));
    const std::string sessionReportArtifactId =
        sessionReportResult.value.value("summary", json::object()).value("artifact", json::object()).value("artifact_id", std::string());
    expect(!sessionReportArtifactId.empty(), "TapeScope session-report scan should expose a durable artifact id");

    const auto sessionReportListResult = client.listSessionReports(10);
    expect(sessionReportListResult.ok(), "TapeScope session-report list should succeed: " + tapescope::QueryClient::describeError(sessionReportListResult.error));
    expect(!sessionReportListResult.value.value("events", json::array()).empty(),
           "TapeScope session-report list should include the newly scanned durable report");

    const auto artifactResult = client.readArtifact(sessionReportArtifactId);
    expect(artifactResult.ok(), "TapeScope artifact read should succeed: " + tapescope::QueryClient::describeError(artifactResult.error));
    expect(artifactResult.value.value("summary", json::object()).value("resolved_artifact_id", std::string()) == sessionReportArtifactId,
           "TapeScope artifact read should reopen the durable session report artifact");

    const auto exportResult = client.exportArtifact(sessionReportArtifactId, "markdown");
    expect(exportResult.ok(), "TapeScope artifact export should succeed: " + tapescope::QueryClient::describeError(exportResult.error));
    expect(exportResult.value.value("summary", json::object()).value("artifact_export", json::object()).value("format", std::string()) == "markdown",
           "TapeScope artifact export should preserve the requested export format");

    const auto orderCaseReportResult = client.scanOrderCaseReport(anchor);
    expect(orderCaseReportResult.ok(), "TapeScope order-case report scan should succeed: " + tapescope::QueryClient::describeError(orderCaseReportResult.error));
    const std::string orderCaseReportArtifactId =
        orderCaseReportResult.value.value("summary", json::object()).value("artifact", json::object()).value("artifact_id", std::string());
    expect(!orderCaseReportArtifactId.empty(), "TapeScope order-case report scan should expose a durable case-report artifact id");

    const auto caseReportListResult = client.listCaseReports(10);
    expect(caseReportListResult.ok(), "TapeScope case-report list should succeed: " + tapescope::QueryClient::describeError(caseReportListResult.error));
    expect(!caseReportListResult.value.value("events", json::array()).empty(),
           "TapeScope case-report list should include the newly scanned durable case report");

    const auto incidentsResult = client.listIncidents(10);
    expect(incidentsResult.ok(), "TapeScope incident list should succeed: " + tapescope::QueryClient::describeError(incidentsResult.error));
    expect(incidentsResult.value.value("events", json::array()).is_array() &&
               !incidentsResult.value.value("events", json::array()).empty(),
           "TapeScope seam test should surface at least one logical incident");
    const std::uint64_t logicalIncidentId =
        incidentsResult.value.value("events", json::array()).at(0).value("logical_incident_id", 0ULL);
    expect(logicalIncidentId > 0, "TapeScope seam test should expose a drilldown-capable logical incident id");

    const auto incidentResult = client.readIncident(logicalIncidentId);
    expect(incidentResult.ok(), "TapeScope incident read should succeed: " + tapescope::QueryClient::describeError(incidentResult.error));
    expect(incidentResult.value.value("summary", json::object()).value("logical_incident_id", 0ULL) == logicalIncidentId,
           "TapeScope incident read should preserve the requested logical incident id");

    server.stop();
}

void testTapeEngineRevisionPinnedReadsCanOverlayMutableTail() {
    const fs::path rootDir = testDataDir() / "tape-engine-revision-overlay";
    const fs::path socketPath = testDataDir() / "tape-engine-revision-overlay.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "ib:conid:6401:STK:SMART:USD:INTC";
    config.ringCapacity = 32;

    tape_engine::Server server(config);
    std::string startError;
    expect(server.start(&startError), "tape-engine should start for revision overlay test: " + startError);

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-engine-revision-overlay";
    options.runtimeSessionId = "runtime-engine-revision-overlay";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;

    options.batchSeq = 71;
    BridgeOutboxRecord firstRecord = makeBridgeRecord(5001, "order_intent", "WebSocket", "INTC", "BUY",
                                                      101, 801, 0, "", "first frozen event", "2026-03-14T09:38:30.100");
    firstRecord.instrumentId = "ib:conid:6401:STK:SMART:USD:INTC";
    const bridge_batch::Batch first = bridge_batch::buildBatch({firstRecord}, options);
    expect(transport.sendFrame(bridge_batch::encodeFrame(first), &error),
           "tape-engine should accept the first revision batch: " + error);

    waitUntil([&]() {
        const auto snapshot = server.snapshot();
        return snapshot.latestFrozenRevisionId >= 1 && snapshot.segments.size() >= 1;
    }, "tape-engine should freeze revision 1 before pinned reads");

    options.batchSeq = 72;
    BridgeOutboxRecord secondRecord = makeBridgeRecord(5002, "order_status", "BrokerOrderStatus", "INTC", "BUY",
                                                       101, 801, 0, "", "mutable tail event", "2026-03-14T09:38:30.200");
    secondRecord.instrumentId = "ib:conid:6401:STK:SMART:USD:INTC";
    const bridge_batch::Batch second = bridge_batch::buildBatch({secondRecord}, options);
    expect(transport.sendFrame(bridge_batch::encodeFrame(second), &error),
           "tape-engine should accept the second revision batch: " + error);

    tape_engine::Client client(socketPath.string());
    tape_engine::QueryResponse response;

    tape_engine::QueryRequest frozenOnly;
    frozenOnly.requestId = "revision-frozen-only";
    frozenOnly.operation = "read_range";
    frozenOnly.revisionId = 1;
    frozenOnly.fromSessionSeq = 1;
    frozenOnly.toSessionSeq = 10;
    expect(client.query(frozenOnly, &response, &error), "pinned frozen read should succeed: " + error);
    expect(response.summary.value("served_revision_id", 0ULL) == 1, "pinned frozen read should report revision 1");
    expect(!response.summary.value("includes_mutable_tail", true), "pinned frozen read should exclude the mutable tail by default");
    expect(response.events.is_array() && response.events.size() == 1, "pinned frozen read should only return the frozen revision events");
    expect(response.events.at(0).value("event_kind", std::string()) == "order_intent", "pinned frozen read should preserve revision-1 events");

    tape_engine::QueryRequest overlay = frozenOnly;
    overlay.requestId = "revision-overlay";
    overlay.includeLiveTail = true;
    expect(client.query(overlay, &response, &error), "pinned overlay read should succeed: " + error);
    expect(response.summary.value("served_revision_id", 0ULL) == 1, "overlay read should stay pinned to revision 1");
    expect(response.summary.value("includes_mutable_tail", false), "overlay read should explicitly report mutable-tail inclusion");
    expect(response.events.is_array() && response.events.size() == 2, "overlay read should merge the live tail on top of the pinned revision");
    expect(response.events.at(1).value("event_kind", std::string()) == "order_status", "overlay read should include the later mutable-tail event");

    server.stop();
}

void testTapeEnginePhase3FindingsIncidentsAndProtectedWindows() {
    const fs::path rootDir = testDataDir() / "tape-engine-phase3";
    const fs::path socketPath = testDataDir() / "tape-engine-phase3.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "ib:conid:8101:STK:SMART:USD:INTC";
    config.ringCapacity = 32;

    tape_engine::Server server(config);
    std::string startError;
    expect(server.start(&startError), "tape-engine should start for phase 3 findings test: " + startError);

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-engine-phase3";
    options.runtimeSessionId = "runtime-engine-phase3";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;

    BridgeOutboxRecord orderIntent = makeBridgeRecord(7001, "order_intent", "WebSocket", "INTC", "BUY",
                                                      211, 1101, 0, "", "phase 3 anchor", "2026-03-14T09:40:00.100");
    orderIntent.instrumentId = "ib:conid:8101:STK:SMART:USD:INTC";
    options.batchSeq = 91;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({orderIntent}, options)), &error),
           "tape-engine should accept the anchor batch for phase 3: " + error);

    BridgeOutboxRecord bidTick = makeBridgeRecord(7002, "market_tick", "BrokerMarketData", "INTC", "BID",
                                                  0, 0, 0, "", "bid tick", "2026-03-14T09:40:00.120");
    bidTick.instrumentId = "ib:conid:8101:STK:SMART:USD:INTC";
    bidTick.marketField = 1;
    bidTick.price = 45.10;

    BridgeOutboxRecord askTick = makeBridgeRecord(7003, "market_tick", "BrokerMarketData", "INTC", "ASK",
                                                  0, 0, 0, "", "ask tick", "2026-03-14T09:40:00.130");
    askTick.instrumentId = "ib:conid:8101:STK:SMART:USD:INTC";
    askTick.marketField = 2;
    askTick.price = 45.11;

    options.batchSeq = 92;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({bidTick, askTick}, options)), &error),
           "tape-engine should accept the initial market ticks for phase 3: " + error);

    BridgeOutboxRecord widenedAsk = makeBridgeRecord(7004, "market_tick", "BrokerMarketData", "INTC", "ASK",
                                                     0, 0, 0, "", "ask widened", "2026-03-14T09:40:00.150");
    widenedAsk.instrumentId = "ib:conid:8101:STK:SMART:USD:INTC";
    widenedAsk.marketField = 2;
    widenedAsk.price = 45.13;

    options.batchSeq = 93;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({widenedAsk}, options)), &error),
           "tape-engine should accept the widening market tick for phase 3: " + error);

    waitUntil([&]() {
        const auto snapshot = server.snapshot();
        return snapshot.latestFrozenRevisionId >= 4 && snapshot.segments.size() >= 4;
    }, "tape-engine should freeze all phase 3 test batches");

    tape_engine::Client client(socketPath.string());
    tape_engine::QueryResponse response;

    tape_engine::QueryRequest anchorsRequest;
    anchorsRequest.requestId = "anchors-phase3";
    anchorsRequest.operation = "list_order_anchors";
    expect(client.query(anchorsRequest, &response, &error), "phase 3 anchor query should succeed: " + error);
    expect(response.events.is_array() && response.events.size() == 1, "phase 3 should record the order anchor");
    expect(response.events.at(0).value("event_kind", std::string()) == "order_intent", "phase 3 anchor list should preserve the order intent event kind");
    expect(response.events.at(0).value("instrument_id", std::string()) == "ib:conid:8101:STK:SMART:USD:INTC", "phase 3 anchor list should preserve canonical instrument identity");

    tape_engine::QueryRequest windowsRequest;
    windowsRequest.requestId = "windows-phase3";
    windowsRequest.operation = "list_protected_windows";
    expect(client.query(windowsRequest, &response, &error), "phase 3 protected window query should succeed: " + error);
    expect(response.events.is_array() && response.events.size() >= 2, "phase 3 should create protected windows for anchors and incident promotion");
    const std::vector<std::string> windowReasons = {
        response.events.at(0).value("reason", std::string()),
        response.events.at(1).value("reason", std::string())
    };
    expect(std::find(windowReasons.begin(), windowReasons.end(), "order_intent") != windowReasons.end() ||
           std::find(windowReasons.begin(), windowReasons.end(), "incident_promotion") != windowReasons.end(),
           "phase 3 protected windows should include anchor or incident reasons");
    expect(response.events.at(0).value("first_session_seq", 0ULL) > 0ULL,
           "phase 3 protected windows should materialize a first session_seq bound");
    expect(response.events.at(0).value("last_session_seq", 0ULL) >= response.events.at(0).value("first_session_seq", 0ULL),
           "phase 3 protected windows should materialize a last session_seq bound");

    tape_engine::QueryRequest findingsRequest;
    findingsRequest.requestId = "findings-phase3";
    findingsRequest.operation = "list_findings";
    expect(client.query(findingsRequest, &response, &error), "phase 3 findings query should succeed: " + error);
    expect(response.events.is_array() && !response.events.empty(), "phase 3 should emit at least one finding");
    bool sawSpreadFinding = false;
    bool sawOrderFlowFinding = false;
    bool sawOrderFillContextFinding = false;
    for (const auto& item : response.events) {
        sawSpreadFinding = sawSpreadFinding || item.value("kind", std::string()) == "spread_widened";
        sawOrderFlowFinding = sawOrderFlowFinding || item.value("kind", std::string()) == "order_flow_timeline";
        sawOrderFillContextFinding = sawOrderFillContextFinding || item.value("kind", std::string()) == "order_fill_context";
    }
    expect(sawSpreadFinding, "phase 3 should emit a spread_widened finding");
    expect(sawOrderFlowFinding, "phase 3 deferred lane should emit an order_flow_timeline finding");
    expect(sawOrderFillContextFinding, "phase 3 deferred lane should emit an order_fill_context finding");

    tape_engine::QueryRequest incidentsRequest;
    incidentsRequest.requestId = "incidents-phase3";
    incidentsRequest.operation = "list_incidents";
    expect(client.query(incidentsRequest, &response, &error), "phase 3 incidents query should succeed: " + error);
    expect(response.events.is_array() && !response.events.empty(), "phase 3 should emit at least one incident");
    bool sawSpreadIncident = false;
    bool sawOrderOverlap = false;
    for (const auto& item : response.events) {
        sawSpreadIncident = sawSpreadIncident || item.value("kind", std::string()) == "spread_widened";
        sawOrderOverlap = sawOrderOverlap || item.value("overlaps_order", false);
    }
    expect(sawSpreadIncident, "phase 3 incident list should include the spread_widened incident");
    expect(sawOrderOverlap, "phase 3 incident list should report at least one order-overlapping incident");

    tape_engine::QueryRequest overviewRequest;
    overviewRequest.requestId = "overview-phase3";
    overviewRequest.operation = "read_session_overview";
    overviewRequest.fromSessionSeq = 1;
    overviewRequest.toSessionSeq = 4;
    overviewRequest.limit = 3;
    expect(client.query(overviewRequest, &response, &error), "phase 3 session overview query should succeed: " + error);
    expect(response.events.is_array() && !response.events.empty(), "phase 3 session overview should return ranked incidents as events");
    expect(response.summary.value("incident_count", 0ULL) >= 1ULL, "phase 3 session overview should count ranked incidents");
    expect(response.summary.value("finding_count", 0ULL) >= 3ULL, "phase 3 session overview should include the phase 3 findings");
    expect(response.summary.value("protected_window_count", 0ULL) >= 2ULL, "phase 3 session overview should count protected windows");
    expect(response.summary.contains("incident_kind_counts"), "phase 3 session overview should summarize incident kinds");
    expect(response.summary.contains("finding_kind_counts"), "phase 3 session overview should summarize finding kinds");
    expect(response.summary.contains("protected_window_reason_counts"), "phase 3 session overview should summarize protected-window reasons");
    expect(response.summary.contains("top_findings"), "phase 3 session overview should include top findings");
    expect(response.summary.contains("top_protected_windows"), "phase 3 session overview should include top protected windows");
    expect(response.summary.contains("top_order_anchors"), "phase 3 session overview should include top order anchors");
    expect(response.summary.contains("timeline"), "phase 3 session overview should include a merged investigation timeline");
    expect(response.summary.contains("timeline_summary"), "phase 3 session overview should summarize the investigation timeline");
    expect(response.summary.contains("report_summary"), "phase 3 session overview should include report-level summary text");
    expect(response.summary.contains("data_quality"), "phase 3 session overview should surface data-quality scoring");
    expect(response.summary.value("top_findings", json::array()).size() >= 1ULL, "phase 3 session overview should return at least one top finding");
    expect(response.summary.value("top_protected_windows", json::array()).size() >= 1ULL, "phase 3 session overview should return at least one top protected window");
    expect(response.summary.value("top_order_anchors", json::array()).size() == 1ULL, "phase 3 session overview should return the anchored order context");
    expect(response.summary.value("timeline_summary", json::object()).value("incident_count", 0ULL) >= 1ULL,
           "phase 3 session overview timeline should include at least one incident entry");
    expect(response.summary.value("incident_kind_counts", json::object()).value("spread_widened", 0ULL) >= 1ULL,
           "phase 3 session overview should count the spread widening incident");

    server.stop();
}

void testTapeEnginePhase3ArtifactsPersistAcrossRestartAndReadProtectedWindow() {
    const fs::path rootDir = testDataDir() / "tape-engine-phase3-restart";
    const fs::path socketPath = testDataDir() / "tape-engine-phase3-restart.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "ib:conid:9101:STK:SMART:USD:INTC";
    config.ringCapacity = 32;

    std::uint64_t persistedWindowId = 0;
    std::uint64_t persistedAnchorId = 0;

    {
        tape_engine::Server server(config);
        std::string startError;
        expect(server.start(&startError), "tape-engine should start for phase 3 restart persistence test: " + startError);

        bridge_batch::BuildOptions options;
        options.appSessionId = "app-engine-phase3-restart";
        options.runtimeSessionId = "runtime-engine-phase3-restart";
        options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

        bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
        std::string error;

        BridgeOutboxRecord orderIntent = makeBridgeRecord(8101, "order_intent", "WebSocket", "INTC", "BUY",
                                                          311, 2101, 0, "", "phase 3 restart anchor", "2026-03-14T09:41:00.100");
        orderIntent.instrumentId = "ib:conid:9101:STK:SMART:USD:INTC";
        options.batchSeq = 101;
        expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({orderIntent}, options)), &error),
               "tape-engine should accept the restart anchor batch: " + error);

        BridgeOutboxRecord bidTick = makeBridgeRecord(8102, "market_tick", "BrokerMarketData", "INTC", "BID",
                                                      0, 0, 0, "", "restart bid", "2026-03-14T09:41:00.120");
        bidTick.instrumentId = "ib:conid:9101:STK:SMART:USD:INTC";
        bidTick.marketField = 1;
        bidTick.price = 45.20;

        BridgeOutboxRecord askTick = makeBridgeRecord(8103, "market_tick", "BrokerMarketData", "INTC", "ASK",
                                                      0, 0, 0, "", "restart ask", "2026-03-14T09:41:00.130");
        askTick.instrumentId = "ib:conid:9101:STK:SMART:USD:INTC";
        askTick.marketField = 2;
        askTick.price = 45.21;

        options.batchSeq = 102;
        expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({bidTick, askTick}, options)), &error),
               "tape-engine should accept the restart market seed batch: " + error);

        BridgeOutboxRecord widenedAsk = makeBridgeRecord(8104, "market_tick", "BrokerMarketData", "INTC", "ASK",
                                                         0, 0, 0, "", "restart widened ask", "2026-03-14T09:41:00.150");
        widenedAsk.instrumentId = "ib:conid:9101:STK:SMART:USD:INTC";
        widenedAsk.marketField = 2;
        widenedAsk.price = 45.24;

        options.batchSeq = 103;
        expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({widenedAsk}, options)), &error),
               "tape-engine should accept the restart widening batch: " + error);

        waitUntil([&]() {
            const auto snapshot = server.snapshot();
            return snapshot.latestFrozenRevisionId >= 4 && snapshot.segments.size() >= 4;
        }, "tape-engine should freeze restart phase 3 batches");

        tape_engine::Client client(socketPath.string());
        tape_engine::QueryResponse response;

        tape_engine::QueryRequest anchorsRequest;
        anchorsRequest.requestId = "anchors-phase3-restart-initial";
        anchorsRequest.operation = "list_order_anchors";
        expect(client.query(anchorsRequest, &response, &error), "initial phase 3 restart anchor query should succeed: " + error);
        expect(response.events.is_array() && !response.events.empty(), "restart phase 3 setup should persist at least one anchor");
        persistedAnchorId = response.events.at(0).value("anchor_id", 0ULL);
        expect(persistedAnchorId > 0, "restart phase 3 setup should expose a persisted anchor id");

        tape_engine::QueryRequest windowsRequest;
        windowsRequest.requestId = "windows-phase3-restart-initial";
        windowsRequest.operation = "list_protected_windows";
        expect(client.query(windowsRequest, &response, &error), "initial phase 3 restart protected window query should succeed: " + error);
        expect(response.events.is_array() && !response.events.empty(), "restart phase 3 setup should persist protected windows");
        for (const auto& item : response.events) {
            if (item.value("reason", std::string()) == "order_intent") {
                persistedWindowId = item.value("window_id", 0ULL);
                break;
            }
        }
        expect(persistedWindowId > 0, "restart phase 3 setup should include an order_intent protected window");

        server.stop();
    }

    {
        tape_engine::Server restarted(config);
        std::string startError;
        expect(restarted.start(&startError), "tape-engine should restart and restore frozen phase 3 state: " + startError);
        const tape_engine::EngineSnapshot restoredSnapshot = restarted.snapshot();
        expect(restoredSnapshot.latestFrozenRevisionId >= 4, "restart should restore the latest frozen revision id");
        expect(restoredSnapshot.segments.size() >= 4, "restart should restore frozen segment metadata");

        tape_engine::Client client(socketPath.string());
        tape_engine::QueryResponse response;
        std::string error;

        tape_engine::QueryRequest findingsRequest;
        findingsRequest.requestId = "findings-phase3-restart-restored";
        findingsRequest.operation = "list_findings";
        expect(client.query(findingsRequest, &response, &error), "restored phase 3 findings query should succeed: " + error);
        expect(response.events.is_array() && !response.events.empty(), "restart should restore frozen findings");
        bool sawSpreadFinding = false;
        bool sawOrderFlowFinding = false;
        for (const auto& item : response.events) {
            sawSpreadFinding = sawSpreadFinding || item.value("kind", std::string()) == "spread_widened";
            sawOrderFlowFinding = sawOrderFlowFinding || item.value("kind", std::string()) == "order_flow_timeline";
        }
        expect(sawSpreadFinding, "restart should restore the spread_widened finding");
        expect(sawOrderFlowFinding, "restart should restore deferred order-flow findings too");

        tape_engine::QueryRequest readWindowRequest;
        readWindowRequest.requestId = "window-phase3-restart-restored";
        readWindowRequest.operation = "read_protected_window";
        readWindowRequest.windowId = persistedWindowId;
        expect(client.query(readWindowRequest, &response, &error), "restored protected window read should succeed: " + error);
        expect(response.summary.value("protected_window", json::object()).value("window_id", 0ULL) == persistedWindowId,
               "restored protected window read should return the requested window id");
        expect(response.events.is_array() && response.events.size() >= 3,
               "restored protected window read should return the anchored evidence window");
        bool sawOrderIntent = false;
        bool sawMarketTick = false;
        for (const auto& item : response.events) {
            const std::string kind = item.value("event_kind", std::string());
            sawOrderIntent = sawOrderIntent || kind == "order_intent";
            sawMarketTick = sawMarketTick || kind == "market_tick";
        }
        expect(sawOrderIntent, "restored protected window read should include the anchored order intent");
        expect(sawMarketTick, "restored protected window read should include surrounding market evidence");

        bridge_batch::BuildOptions options;
        options.appSessionId = "app-engine-phase3-restart";
        options.runtimeSessionId = "runtime-engine-phase3-restart";
        options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;
        options.batchSeq = 104;

        bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
        BridgeOutboxRecord secondOrderIntent = makeBridgeRecord(8105, "order_intent", "WebSocket", "INTC", "BUY",
                                                               312, 2102, 0, "", "phase 3 restart second anchor", "2026-03-14T09:41:01.100");
        secondOrderIntent.instrumentId = "ib:conid:9101:STK:SMART:USD:INTC";
        expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({secondOrderIntent}, options)), &error),
               "restarted tape-engine should accept a new anchored order batch: " + error);

        waitUntil([&]() {
            const auto snapshot = restarted.snapshot();
            return snapshot.latestFrozenRevisionId > restoredSnapshot.latestFrozenRevisionId &&
                   snapshot.segments.size() > restoredSnapshot.segments.size();
        }, "restarted tape-engine should freeze the post-restart anchor batch");

        tape_engine::QueryRequest anchorsRequest;
        anchorsRequest.requestId = "anchors-phase3-restart-restored";
        anchorsRequest.operation = "list_order_anchors";
        expect(client.query(anchorsRequest, &response, &error), "restored phase 3 anchor query should succeed after new ingest: " + error);
        expect(response.events.is_array() && response.events.size() >= 2, "restart should preserve prior anchors and append new ones");
        expect(response.events.at(0).value("anchor_id", 0ULL) > persistedAnchorId,
               "post-restart anchors should continue incrementing anchor ids");

        restarted.stop();
    }
}

void testTapeEnginePhase3ScansSessionIntoDurableReportArtifact() {
    const fs::path rootDir = testDataDir() / "tape-engine-phase3-session-report";
    const fs::path socketPath = testDataDir() / "tape-engine-phase3-session-report.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "ib:conid:9201:STK:SMART:USD:INTC";
    config.ringCapacity = 32;

    std::uint64_t reportId = 0;
    std::uint64_t servedRevisionId = 0;
    std::string overviewArtifactId;

    {
        tape_engine::Server server(config);
        std::string startError;
        expect(server.start(&startError), "tape-engine should start for session-report scan test: " + startError);

        bridge_batch::BuildOptions options;
        options.appSessionId = "app-engine-phase3-session-report";
        options.runtimeSessionId = "runtime-engine-phase3-session-report";
        options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

        bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
        std::string error;

        BridgeOutboxRecord orderIntent = makeBridgeRecord(8201, "order_intent", "WebSocket", "INTC", "BUY",
                                                          411, 2201, 0, "", "session report anchor", "2026-03-14T09:41:30.100");
        orderIntent.instrumentId = "ib:conid:9201:STK:SMART:USD:INTC";
        options.batchSeq = 201;
        expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({orderIntent}, options)), &error),
               "tape-engine should accept the session-report anchor batch: " + error);

        BridgeOutboxRecord bidTick = makeBridgeRecord(8202, "market_tick", "BrokerMarketData", "INTC", "BID",
                                                      0, 0, 0, "", "session report bid", "2026-03-14T09:41:30.120");
        bidTick.instrumentId = "ib:conid:9201:STK:SMART:USD:INTC";
        bidTick.marketField = 1;
        bidTick.price = 45.20;

        BridgeOutboxRecord askTick = makeBridgeRecord(8203, "market_tick", "BrokerMarketData", "INTC", "ASK",
                                                      0, 0, 0, "", "session report ask", "2026-03-14T09:41:30.130");
        askTick.instrumentId = "ib:conid:9201:STK:SMART:USD:INTC";
        askTick.marketField = 2;
        askTick.price = 45.21;

        options.batchSeq = 202;
        expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({bidTick, askTick}, options)), &error),
               "tape-engine should accept the session-report seed batch: " + error);

        BridgeOutboxRecord widenedAsk = makeBridgeRecord(8204, "market_tick", "BrokerMarketData", "INTC", "ASK",
                                                         0, 0, 0, "", "session report widened ask", "2026-03-14T09:41:30.150");
        widenedAsk.instrumentId = "ib:conid:9201:STK:SMART:USD:INTC";
        widenedAsk.marketField = 2;
        widenedAsk.price = 45.24;

        options.batchSeq = 203;
        expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({widenedAsk}, options)), &error),
               "tape-engine should accept the session-report widening batch: " + error);

        waitUntil([&]() {
            const auto snapshot = server.snapshot();
            return snapshot.latestFrozenRevisionId >= 4 && snapshot.segments.size() >= 4;
        }, "tape-engine should freeze session-report batches");

        tape_engine::Client client(socketPath.string());
        tape_engine::QueryResponse response;

        tape_engine::QueryRequest overviewRequest;
        overviewRequest.requestId = "read-phase3-session-overview";
        overviewRequest.operation = "read_session_overview";
        expect(client.query(overviewRequest, &response, &error), "phase 3 session overview read should succeed: " + error);
        overviewArtifactId = response.summary.value("artifact", json::object()).value("artifact_id", std::string());
        expectContains(response.summary.value("api", json::object()).value("envelope_schema", std::string()),
                       "com.foxy.tape-engine.investigation-envelope",
                       "session overview should expose the investigation envelope schema");

        tape_engine::QueryRequest scanRequest;
        scanRequest.requestId = "scan-phase3-session-report";
        scanRequest.operation = "scan_session_report";
        expect(client.query(scanRequest, &response, &error), "phase 3 session report scan should succeed: " + error);
        expect(response.summary.value("is_durable_report", false),
               "session report scan should mark the result as a durable report");
        expect(response.summary.contains("session_report"),
               "session report scan should return persisted artifact metadata");
        expect(response.summary.value("session_report", json::object()).value("artifact_id", std::string()) ==
                   "session-report:" + std::to_string(response.summary.value("session_report", json::object()).value("report_id", 0ULL)),
               "session report scan should surface a stable durable artifact id");
        reportId = response.summary.value("session_report", json::object()).value("report_id", 0ULL);
        servedRevisionId = response.summary.value("served_revision_id", 0ULL);
        expect(reportId > 0, "session report scan should assign a durable report id");
        expect(servedRevisionId > 0, "session report scan should pin the report to a frozen revision");
        expect(response.summary.value("artifact", json::object()).value("artifact_id", std::string()) ==
                   "session-report:" + std::to_string(reportId),
               "session report scans should now promote the durable report artifact to the primary artifact envelope");
        expect(response.summary.value("source_artifact", json::object()).value("artifact_id", std::string()) == overviewArtifactId,
               "session report scans should preserve the source session-overview artifact they were derived from");
        expect(response.summary.value("incident_count", 0ULL) >= 1ULL,
               "session report scan should summarize at least one major incident");
        expect(response.summary.value("report_summary", json::object()).contains("top_incident_why_it_matters"),
               "session report scan should include top-incident explanations");

        tape_engine::QueryRequest rescanRequest = scanRequest;
        rescanRequest.requestId = "scan-phase3-session-report-again";
        expect(client.query(rescanRequest, &response, &error), "phase 3 repeated session report scan should succeed: " + error);
        expect(response.summary.value("session_report", json::object()).value("report_id", 0ULL) == reportId,
               "repeated session scans over the same frozen range should reuse the canonical report artifact");

        tape_engine::QueryRequest listReportsRequest;
        listReportsRequest.requestId = "list-phase3-session-reports";
        listReportsRequest.operation = "list_session_reports";
        expect(client.query(listReportsRequest, &response, &error), "phase 3 session report listing should succeed: " + error);
        expect(response.events.is_array() && !response.events.empty(), "session report listing should return the durable artifact");
        expect(response.events.at(0).value("report_id", 0ULL) == reportId,
               "session report listing should include the newly scanned report");

        expect(fs::exists(rootDir / "session-reports.jsonl"), "session report scanning should persist a report manifest");
        expect(fs::exists(rootDir / "artifact-lookup.msgpack"),
               "session report scanning should persist the artifact lookup index");
        {
            const json lookup = readMsgpackFile(rootDir / "artifact-lookup.msgpack");
            bool foundReport = false;
            for (const auto& item : lookup.value("session_reports", json::array())) {
                foundReport = foundReport || item.value("report_id", 0ULL) == reportId;
            }
            expect(foundReport, "artifact lookup index should include the persisted session report");
        }

        server.stop();
    }

    {
        tape_engine::Server restarted(config);
        std::string startError;
        expect(restarted.start(&startError), "tape-engine should restart and restore session report artifacts: " + startError);

        tape_engine::Client client(socketPath.string());
        tape_engine::QueryResponse response;
        std::string error;

        tape_engine::QueryRequest readReportRequest;
        readReportRequest.requestId = "read-phase3-session-report";
        readReportRequest.operation = "read_session_report";
        readReportRequest.reportId = reportId;
        expect(client.query(readReportRequest, &response, &error), "restored session report read should succeed: " + error);
        expect(response.summary.value("session_report", json::object()).value("report_id", 0ULL) == reportId,
               "restored session report should return the requested durable report id");
        expect(response.summary.value("session_report", json::object()).value("revision_id", 0ULL) == servedRevisionId,
               "restored session report should stay pinned to its frozen revision");
        expect(response.summary.value("is_durable_report", false),
               "restored session report should still be marked durable");
        expect(response.summary.contains("timeline"), "restored session report should keep the investigation timeline");
        expect(response.summary.contains("data_quality"), "restored session report should keep the data-quality block");
        expect(response.events.is_array() && !response.events.empty(),
               "restored session report should retain ranked incident rows");

        tape_engine::QueryRequest readArtifactRequest;
        readArtifactRequest.requestId = "read-artifact-session-report";
        readArtifactRequest.operation = "read_artifact";
        readArtifactRequest.artifactId = "session-report:" + std::to_string(reportId);
        expect(client.query(readArtifactRequest, &response, &error), "read_artifact should reopen the session report by stable artifact id: " + error);
        expect(response.summary.value("resolved_artifact_id", std::string()) == "session-report:" + std::to_string(reportId),
               "read_artifact should echo the resolved durable artifact id");
        expect(response.summary.value("api", json::object()).value("response_kind", std::string()) == "artifact_read",
               "read_artifact should expose a stable artifact-read response kind");

        tape_engine::QueryRequest readOverviewArtifactRequest;
        readOverviewArtifactRequest.requestId = "read-artifact-session-overview";
        readOverviewArtifactRequest.operation = "read_artifact";
        readOverviewArtifactRequest.artifactId = overviewArtifactId;
        expect(client.query(readOverviewArtifactRequest, &response, &error), "read_artifact should reopen a session-overview selector artifact: " + error);
        expect(response.summary.value("resolved_artifact_id", std::string()) == overviewArtifactId,
               "read_artifact should reopen the exact session-overview artifact id that was emitted earlier");

        tape_engine::QueryRequest exportArtifactRequest;
        exportArtifactRequest.requestId = "export-artifact-session-report";
        exportArtifactRequest.operation = "export_artifact";
        exportArtifactRequest.artifactId = "session-report:" + std::to_string(reportId);
        exportArtifactRequest.exportFormat = "markdown";
        expect(client.query(exportArtifactRequest, &response, &error), "export_artifact should render session report markdown: " + error);
        expectContains(response.summary.value("markdown", std::string()), "# Session overview",
                       "export_artifact markdown should include the session report headline");
        expect(response.summary.value("artifact_export", json::object()).value("schema", std::string()) ==
                   "com.foxy.tape-engine.artifact-export",
               "artifact exports should expose a stable export schema identifier");

        restarted.stop();
    }
}

void testTapeEnginePhase3PersistsIncidentAndOrderCaseReports() {
    const fs::path rootDir = testDataDir() / "tape-engine-phase3-case-reports";
    const fs::path socketPath = testDataDir() / "tape-engine-phase3-case-reports.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "ib:conid:9301:STK:SMART:USD:INTC";
    config.ringCapacity = 32;

    std::uint64_t incidentReportId = 0;
    std::uint64_t orderCaseReportId = 0;
    std::uint64_t logicalIncidentId = 0;
    std::uint64_t firstFindingId = 0;
    std::uint64_t firstAnchorId = 0;
    std::string orderCaseArtifactId;

    {
        tape_engine::Server server(config);
        std::string startError;
        expect(server.start(&startError), "tape-engine should start for durable case-report test: " + startError);

        bridge_batch::BuildOptions options;
        options.appSessionId = "app-engine-phase3-case-reports";
        options.runtimeSessionId = "runtime-engine-phase3-case-reports";
        options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

        bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
        std::string error;

        BridgeOutboxRecord orderIntent = makeBridgeRecord(8301, "order_intent", "WebSocket", "INTC", "BUY",
                                                          511, 2301, 0, "", "case report anchor", "2026-03-14T09:42:10.100");
        orderIntent.instrumentId = "ib:conid:9301:STK:SMART:USD:INTC";
        options.batchSeq = 301;
        expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({orderIntent}, options)), &error),
               "tape-engine should accept the case-report anchor batch: " + error);

        BridgeOutboxRecord bidTick = makeBridgeRecord(8302, "market_tick", "BrokerMarketData", "INTC", "BID",
                                                      0, 0, 0, "", "case report bid", "2026-03-14T09:42:10.120");
        bidTick.instrumentId = "ib:conid:9301:STK:SMART:USD:INTC";
        bidTick.marketField = 1;
        bidTick.price = 45.20;

        BridgeOutboxRecord askTick = makeBridgeRecord(8303, "market_tick", "BrokerMarketData", "INTC", "ASK",
                                                      0, 0, 0, "", "case report ask", "2026-03-14T09:42:10.130");
        askTick.instrumentId = "ib:conid:9301:STK:SMART:USD:INTC";
        askTick.marketField = 2;
        askTick.price = 45.21;

        options.batchSeq = 302;
        expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({bidTick, askTick}, options)), &error),
               "tape-engine should accept the case-report seed batch: " + error);

        BridgeOutboxRecord widenedAsk = makeBridgeRecord(8304, "market_tick", "BrokerMarketData", "INTC", "ASK",
                                                         0, 0, 0, "", "case report widened ask", "2026-03-14T09:42:10.150");
        widenedAsk.instrumentId = "ib:conid:9301:STK:SMART:USD:INTC";
        widenedAsk.marketField = 2;
        widenedAsk.price = 45.24;

        options.batchSeq = 303;
        expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({widenedAsk}, options)), &error),
               "tape-engine should accept the case-report widening batch: " + error);

        waitUntil([&]() {
            const auto snapshot = server.snapshot();
            return snapshot.latestFrozenRevisionId >= 4 && snapshot.segments.size() >= 4;
        }, "tape-engine should freeze case-report batches");

        tape_engine::Client client(socketPath.string());
        tape_engine::QueryResponse response;

        tape_engine::QueryRequest incidentsRequest;
        incidentsRequest.requestId = "incident-list-case-reports";
        incidentsRequest.operation = "list_incidents";
        expect(client.query(incidentsRequest, &response, &error), "case-report incident list query should succeed: " + error);
        expect(response.events.is_array() && !response.events.empty(), "case-report setup should create a ranked incident");
        logicalIncidentId = response.events.at(0).value("logical_incident_id", 0ULL);
        expect(logicalIncidentId > 0, "case-report setup should expose a logical incident id");

        tape_engine::QueryRequest incidentReportRequest;
        incidentReportRequest.requestId = "scan-incident-case-report";
        incidentReportRequest.operation = "scan_incident_report";
        incidentReportRequest.logicalIncidentId = logicalIncidentId;
        expect(client.query(incidentReportRequest, &response, &error), "incident case-report scan should succeed: " + error);
        incidentReportId = response.summary.value("case_report_artifact", json::object()).value("report_id", 0ULL);
        expect(incidentReportId > 0, "incident case-report scan should persist a durable artifact");
        expect(response.summary.value("artifact", json::object()).value("artifact_id", std::string()) ==
                   "case-report:" + std::to_string(incidentReportId),
               "incident case-report scans should promote the durable case-report artifact to the primary artifact envelope");
        expect(response.summary.value("source_artifact", json::object()).value("artifact_id", std::string()) ==
                   "incident:" + std::to_string(logicalIncidentId),
               "incident case-report scans should preserve the source incident artifact id");

        tape_engine::QueryRequest orderReportRequest;
        orderReportRequest.requestId = "scan-order-case-report";
        orderReportRequest.operation = "scan_order_case_report";
        orderReportRequest.orderId = 2301;
        expect(client.query(orderReportRequest, &response, &error), "order case-report scan should succeed: " + error);
        orderCaseReportId = response.summary.value("case_report_artifact", json::object()).value("report_id", 0ULL);
        expect(orderCaseReportId > 0, "order case-report scan should persist a durable artifact");
        expect(orderCaseReportId != incidentReportId, "incident and order case reports should persist as distinct artifacts");
        orderCaseArtifactId = response.summary.value("source_artifact", json::object()).value("artifact_id", std::string());
        expect(response.summary.value("artifact", json::object()).value("artifact_id", std::string()) ==
                   "case-report:" + std::to_string(orderCaseReportId),
               "order case-report scans should promote the durable case-report artifact to the primary artifact envelope");

        tape_engine::QueryRequest listReportsRequest;
        listReportsRequest.requestId = "list-case-reports";
        listReportsRequest.operation = "list_case_reports";
        expect(client.query(listReportsRequest, &response, &error), "case-report listing should succeed: " + error);
        expect(response.events.is_array() && response.events.size() >= 2, "case-report listing should include both durable report artifacts");
        expect(fs::exists(rootDir / "case-reports.jsonl"), "case-report scanning should persist a case report manifest");

        tape_engine::QueryRequest findingsRequest;
        findingsRequest.requestId = "list-findings-case-reports";
        findingsRequest.operation = "list_findings";
        expect(client.query(findingsRequest, &response, &error), "case-report finding list query should succeed: " + error);
        expect(response.events.is_array() && !response.events.empty(), "case-report setup should produce at least one finding");
        firstFindingId = response.events.at(0).value("finding_id", 0ULL);

        tape_engine::QueryRequest anchorsRequest;
        anchorsRequest.requestId = "list-anchors-case-reports";
        anchorsRequest.operation = "list_order_anchors";
        expect(client.query(anchorsRequest, &response, &error), "case-report anchor list query should succeed: " + error);
        expect(response.events.is_array() && !response.events.empty(), "case-report setup should persist at least one anchor");
        firstAnchorId = response.events.at(0).value("anchor_id", 0ULL);

        expect(fs::exists(rootDir / "artifact-lookup.msgpack"),
               "case-report scanning should persist the artifact lookup index");
        {
            const json lookup = readMsgpackFile(rootDir / "artifact-lookup.msgpack");
            bool foundIncidentCaseReport = false;
            bool foundOrderCaseReport = false;
            bool foundFinding = false;
            bool foundAnchor = false;
            bool foundIncident = false;
            for (const auto& item : lookup.value("case_reports", json::array())) {
                const std::uint64_t reportId = item.value("report_id", 0ULL);
                foundIncidentCaseReport = foundIncidentCaseReport || reportId == incidentReportId;
                foundOrderCaseReport = foundOrderCaseReport || reportId == orderCaseReportId;
            }
            for (const auto& item : lookup.value("findings", json::array())) {
                foundFinding = foundFinding || item.value("finding_id", 0ULL) == firstFindingId;
            }
            for (const auto& item : lookup.value("order_anchors", json::array())) {
                foundAnchor = foundAnchor || item.value("anchor_id", 0ULL) == firstAnchorId;
            }
            for (const auto& item : lookup.value("incidents", json::array())) {
                foundIncident = foundIncident || item.value("logical_incident_id", 0ULL) == logicalIncidentId;
            }
            expect(foundIncidentCaseReport && foundOrderCaseReport,
                   "artifact lookup index should include both durable case reports");
            expect(foundFinding, "artifact lookup index should include direct finding lookup entries");
            expect(foundAnchor, "artifact lookup index should include direct anchor lookup entries");
            expect(foundIncident, "artifact lookup index should include the latest logical incident lookup entry");
        }

        server.stop();
    }

    {
        tape_engine::Server restarted(config);
        std::string startError;
        expect(restarted.start(&startError), "tape-engine should restart and restore durable case reports: " + startError);

        tape_engine::Client client(socketPath.string());
        tape_engine::QueryResponse response;
        std::string error;

        tape_engine::QueryRequest readIncidentReportRequest;
        readIncidentReportRequest.requestId = "read-incident-case-report";
        readIncidentReportRequest.operation = "read_case_report";
        readIncidentReportRequest.reportId = incidentReportId;
        expect(client.query(readIncidentReportRequest, &response, &error), "restored incident case-report read should succeed: " + error);
        expect(response.summary.value("case_report_artifact", json::object()).value("report_id", 0ULL) == incidentReportId,
               "restored incident case-report should preserve its durable report id");
        expect(response.summary.value("case_report_artifact", json::object()).value("report_type", std::string()) == "incident_case",
               "restored incident case-report should keep its report type");

        tape_engine::QueryRequest readOrderReportRequest;
        readOrderReportRequest.requestId = "read-order-case-report";
        readOrderReportRequest.operation = "read_case_report";
        readOrderReportRequest.reportId = orderCaseReportId;
        expect(client.query(readOrderReportRequest, &response, &error), "restored order case-report read should succeed: " + error);
        expect(response.summary.value("case_report_artifact", json::object()).value("report_id", 0ULL) == orderCaseReportId,
               "restored order case-report should preserve its durable report id");
        expect(response.summary.value("case_report_artifact", json::object()).value("report_type", std::string()) == "order_case",
               "restored order case-report should keep its report type");
        expect(response.summary.contains("case_report"), "restored order case-report should still expose the case summary");

        tape_engine::QueryRequest readOrderCaseArtifactRequest;
        readOrderCaseArtifactRequest.requestId = "read-order-case-artifact";
        readOrderCaseArtifactRequest.operation = "read_artifact";
        readOrderCaseArtifactRequest.artifactId = orderCaseArtifactId;
        expect(client.query(readOrderCaseArtifactRequest, &response, &error), "read_artifact should reopen the order-case selector artifact: " + error);
        expect(response.summary.value("resolved_artifact_id", std::string()) == orderCaseArtifactId,
               "read_artifact should resolve selector-style order-case artifacts directly");

        tape_engine::QueryRequest readFindingArtifactRequest;
        readFindingArtifactRequest.requestId = "read-finding-artifact";
        readFindingArtifactRequest.operation = "read_artifact";
        readFindingArtifactRequest.artifactId = "finding:" + std::to_string(firstFindingId);
        expect(client.query(readFindingArtifactRequest, &response, &error), "read_artifact should reopen finding artifacts directly: " + error);
        expect(response.summary.value("finding", json::object()).value("finding_id", 0ULL) == firstFindingId,
               "finding artifact reads should resolve the requested finding");

        tape_engine::QueryRequest readAnchorArtifactRequest;
        readAnchorArtifactRequest.requestId = "read-anchor-artifact";
        readAnchorArtifactRequest.operation = "read_artifact";
        readAnchorArtifactRequest.artifactId = "anchor:" + std::to_string(firstAnchorId);
        expect(client.query(readAnchorArtifactRequest, &response, &error), "read_artifact should reopen anchor artifacts directly: " + error);
        expect(response.summary.value("order_anchor", json::object()).value("anchor_id", 0ULL) == firstAnchorId,
               "anchor artifact reads should resolve the requested anchor");

        tape_engine::QueryRequest exportCaseArtifactRequest;
        exportCaseArtifactRequest.requestId = "export-case-artifact";
        exportCaseArtifactRequest.operation = "export_artifact";
        exportCaseArtifactRequest.artifactId = "case-report:" + std::to_string(orderCaseReportId);
        exportCaseArtifactRequest.exportFormat = "json-bundle";
        expect(client.query(exportCaseArtifactRequest, &response, &error), "export_artifact should export a case report bundle: " + error);
        expect(response.summary.value("bundle", json::object()).value("summary", json::object()).contains("case_report"),
               "case report export bundle should preserve the case-report payload");

        restarted.stop();
    }
}

void testTapeEnginePhase3InvestigationContractMatchesGoldenFixtures() {
    const fs::path rootDir = testDataDir() / "tape-engine-phase3-contract-fixtures";
    const fs::path socketPath = testDataDir() / "tape-engine-phase3-contract-fixtures.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "ib:conid:9301:STK:SMART:USD:INTC";
    config.ringCapacity = 32;

    tape_engine::Server server(config);
    std::string startError;
    expect(server.start(&startError), "tape-engine should start for contract fixture test: " + startError);

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-engine-phase3-contract-fixtures";
    options.runtimeSessionId = "runtime-engine-phase3-contract-fixtures";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;

    BridgeOutboxRecord orderIntent = makeBridgeRecord(8601, "order_intent", "WebSocket", "INTC", "BUY",
                                                      711, 3301, 0, "", "contract fixture anchor", "2026-03-15T09:42:10.100");
    orderIntent.instrumentId = "ib:conid:9301:STK:SMART:USD:INTC";
    options.batchSeq = 601;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({orderIntent}, options)), &error),
           "tape-engine should accept the contract fixture anchor batch: " + error);

    BridgeOutboxRecord bidTick = makeBridgeRecord(8602, "market_tick", "BrokerMarketData", "INTC", "BID",
                                                  0, 0, 0, "", "contract fixture bid", "2026-03-15T09:42:10.120");
    bidTick.instrumentId = "ib:conid:9301:STK:SMART:USD:INTC";
    bidTick.marketField = 1;
    bidTick.price = 45.20;

    BridgeOutboxRecord askTick = makeBridgeRecord(8603, "market_tick", "BrokerMarketData", "INTC", "ASK",
                                                  0, 0, 0, "", "contract fixture ask", "2026-03-15T09:42:10.130");
    askTick.instrumentId = "ib:conid:9301:STK:SMART:USD:INTC";
    askTick.marketField = 2;
    askTick.price = 45.21;

    options.batchSeq = 602;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({bidTick, askTick}, options)), &error),
           "tape-engine should accept the contract fixture seed batch: " + error);

    BridgeOutboxRecord widenedAsk = makeBridgeRecord(8604, "market_tick", "BrokerMarketData", "INTC", "ASK",
                                                     0, 0, 0, "", "contract fixture widened ask", "2026-03-15T09:42:10.150");
    widenedAsk.instrumentId = "ib:conid:9301:STK:SMART:USD:INTC";
    widenedAsk.marketField = 2;
    widenedAsk.price = 45.24;

    options.batchSeq = 603;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({widenedAsk}, options)), &error),
           "tape-engine should accept the contract fixture widening batch: " + error);

    waitUntil([&]() {
        const auto snapshot = server.snapshot();
        return snapshot.latestFrozenRevisionId >= 4 && snapshot.segments.size() >= 4;
    }, "tape-engine should freeze contract fixture batches");

    tape_engine::Client client(socketPath.string());
    tape_engine::QueryResponse response;

    waitUntil([&]() {
        tape_engine::QueryRequest readyRequest;
        readyRequest.requestId = "contract-order-case-ready";
        readyRequest.operation = "read_order_case";
        readyRequest.orderId = 3301;
        if (!client.query(readyRequest, &response, &error)) {
            return false;
        }
        return response.summary.value("related_finding_count", 0ULL) >= 4ULL &&
               response.summary.value("timeline_summary", json::object()).value("timeline_entry_count", 0ULL) >= 16ULL;
    }, "contract fixture setup should wait for deferred investigation findings to settle");

    tape_engine::QueryRequest incidentsRequest;
    incidentsRequest.requestId = "contract-read-incident-list";
    incidentsRequest.operation = "list_incidents";
    expect(client.query(incidentsRequest, &response, &error), "contract incident list query should succeed: " + error);
    expect(response.events.is_array() && !response.events.empty(), "contract fixture setup should create a ranked incident");
    const std::uint64_t logicalIncidentId = response.events.at(0).value("logical_incident_id", 0ULL);
    expect(logicalIncidentId > 0, "contract fixture setup should expose a logical incident id");

    tape_engine::QueryRequest sessionReportRequest;
    sessionReportRequest.requestId = "contract-scan-session-report";
    sessionReportRequest.operation = "scan_session_report";
    expect(client.query(sessionReportRequest, &response, &error), "contract session-report scan should succeed: " + error);
    const std::uint64_t sessionReportId = response.summary.value("session_report", json::object()).value("report_id", 0ULL);
    expect(sessionReportId > 0, "contract session report should persist a durable report id");
    const json sessionReportProjection = projectContractResponse(response);

    tape_engine::QueryRequest incidentRequest;
    incidentRequest.requestId = "contract-read-incident";
    incidentRequest.operation = "read_incident";
    incidentRequest.logicalIncidentId = logicalIncidentId;
    expect(client.query(incidentRequest, &response, &error), "contract incident read should succeed: " + error);
    const json incidentProjection = projectContractResponse(response);

    tape_engine::QueryRequest incidentReportRequest;
    incidentReportRequest.requestId = "contract-scan-incident-report";
    incidentReportRequest.operation = "scan_incident_report";
    incidentReportRequest.logicalIncidentId = logicalIncidentId;
    expect(client.query(incidentReportRequest, &response, &error), "contract incident-report scan should succeed: " + error);
    const json incidentReportProjection = projectContractResponse(response);

    tape_engine::QueryRequest orderCaseRequest;
    orderCaseRequest.requestId = "contract-read-order-case";
    orderCaseRequest.operation = "read_order_case";
    orderCaseRequest.orderId = 3301;
    expect(client.query(orderCaseRequest, &response, &error), "contract order-case read should succeed: " + error);
    const std::string orderCaseArtifactId = response.summary.value("artifact", json::object()).value("artifact_id", std::string());
    const json orderCaseProjection = projectContractResponse(response);

    tape_engine::QueryRequest orderCaseReportRequest;
    orderCaseReportRequest.requestId = "contract-scan-order-case-report";
    orderCaseReportRequest.operation = "scan_order_case_report";
    orderCaseReportRequest.orderId = 3301;
    expect(client.query(orderCaseReportRequest, &response, &error), "contract order-case report scan should succeed: " + error);
    const json orderCaseReportProjection = projectContractResponse(response);

    tape_engine::QueryRequest readArtifactRequest;
    readArtifactRequest.requestId = "contract-read-artifact";
    readArtifactRequest.operation = "read_artifact";
    readArtifactRequest.artifactId = "session-report:" + std::to_string(sessionReportId);
    expect(client.query(readArtifactRequest, &response, &error), "contract read_artifact query should succeed: " + error);
    const json readArtifactProjection = projectContractResponse(response);

    tape_engine::QueryRequest exportArtifactRequest;
    exportArtifactRequest.requestId = "contract-export-artifact";
    exportArtifactRequest.operation = "export_artifact";
    exportArtifactRequest.artifactId = orderCaseArtifactId;
    exportArtifactRequest.exportFormat = "json-bundle";
    expect(client.query(exportArtifactRequest, &response, &error), "contract export_artifact query should succeed: " + error);
    const json exportArtifactProjection = projectContractResponse(response);

    const json fixture = readJsonFixture("phase3_investigation_contracts.json");
    expect(sessionReportProjection == fixture.value("scan_session_report", json::object()),
           "session report contract projection should match the golden fixture\nactual:\n" + sessionReportProjection.dump(2));
    expect(incidentProjection == fixture.value("read_incident", json::object()),
           "incident contract projection should match the golden fixture\nactual:\n" + incidentProjection.dump(2));
    expect(incidentReportProjection == fixture.value("scan_incident_report", json::object()),
           "incident report contract projection should match the golden fixture\nactual:\n" + incidentReportProjection.dump(2));
    expect(orderCaseProjection == fixture.value("read_order_case", json::object()),
           "order-case contract projection should match the golden fixture\nactual:\n" + orderCaseProjection.dump(2));
    expect(orderCaseReportProjection == fixture.value("scan_order_case_report", json::object()),
           "order-case report contract projection should match the golden fixture\nactual:\n" + orderCaseReportProjection.dump(2));
    expect(readArtifactProjection == fixture.value("read_artifact_session_report", json::object()),
           "artifact-read contract projection should match the golden fixture\nactual:\n" + readArtifactProjection.dump(2));
    expect(exportArtifactProjection == fixture.value("export_artifact_order_case_bundle", json::object()),
           "artifact-export contract projection should match the golden fixture\nactual:\n" + exportArtifactProjection.dump(2));

    server.stop();
}

void testTapeEnginePrefersStrongConfiguredInstrumentIdentityAndMarksHeuristicFallback() {
    const fs::path rootDir = testDataDir() / "tape-engine-identity-hardening";
    const fs::path socketPath = testDataDir() / "tape-engine-identity-hardening.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "ib:conid:9401:STK:SMART:USD:INTC";
    config.ringCapacity = 32;

    tape_engine::Server server(config);
    std::string startError;
    expect(server.start(&startError), "tape-engine should start for identity hardening test: " + startError);

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-engine-identity-hardening";
    options.runtimeSessionId = "runtime-engine-identity-hardening";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;

    BridgeOutboxRecord configuredMatch = makeBridgeRecord(8401, "order_intent", "WebSocket", "INTC", "BUY",
                                                          611, 2401, 0, "", "identity from configured conid", "2026-03-14T09:42:40.100");
    configuredMatch.instrumentId.clear();
    options.batchSeq = 401;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({configuredMatch}, options)), &error),
           "tape-engine should accept the configured-identity batch: " + error);

    BridgeOutboxRecord heuristicFallback = makeBridgeRecord(8402, "order_intent", "WebSocket", "AAPL", "BUY",
                                                            612, 2402, 0, "", "identity heuristic fallback", "2026-03-14T09:42:40.200");
    heuristicFallback.instrumentId.clear();
    options.batchSeq = 402;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({heuristicFallback}, options)), &error),
           "tape-engine should accept the heuristic-fallback batch: " + error);

    BridgeOutboxRecord mismatchedStrong = makeBridgeRecord(8403, "order_intent", "WebSocket", "AAPL", "BUY",
                                                           613, 2403, 0, "", "identity mismatch", "2026-03-14T09:42:40.300");
    mismatchedStrong.instrumentId = "ib:conid:9402:STK:SMART:USD:MSFT";
    options.batchSeq = 403;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({mismatchedStrong}, options)), &error),
           "tape-engine should accept the mismatched strong-identity batch: " + error);

    waitUntil([&]() {
        const auto snapshot = server.snapshot();
        return snapshot.latestFrozenRevisionId >= 3 && snapshot.segments.size() >= 3;
    }, "tape-engine should freeze identity hardening batches");

    tape_engine::Client client(socketPath.string());
    tape_engine::QueryResponse response;

    tape_engine::QueryRequest rangeRequest;
    rangeRequest.requestId = "identity-range";
    rangeRequest.operation = "read_range";
    rangeRequest.fromSessionSeq = 1;
    rangeRequest.toSessionSeq = 3;
    expect(client.query(rangeRequest, &response, &error), "identity hardening range query should succeed: " + error);
    expect(response.events.size() == 3, "identity hardening range query should return all identity test events");
    expect(response.events.at(0).value("instrument_id", std::string()) == "ib:conid:9401:STK:SMART:USD:INTC",
           "matching symbol records should reuse the strong configured instrument identity");
    expect(response.events.at(0).value("instrument_identity_strength", std::string()) == "strong",
           "matching symbol records should surface strong instrument identity strength");
    expect(response.events.at(1).value("instrument_id", std::string()) == "ib:heuristic:STK:SMART:USD:AAPL",
           "mismatched symbol records should fall back to an explicit heuristic identity");
    expect(response.events.at(1).value("instrument_identity_strength", std::string()) == "heuristic",
           "heuristic fallback identity should be surfaced explicitly");
    expect(response.events.at(2).value("instrument_id", std::string()) == "ib:heuristic:STK:SMART:USD:AAPL",
           "mismatched strong identities should be coerced away from broker-supplied strong IDs when the symbol does not match");
    expect(response.events.at(2).value("source_instrument_id", std::string()) == "ib:conid:9402:STK:SMART:USD:MSFT",
           "mismatched strong identities should preserve the original broker-supplied instrument id as source evidence");
    expect(response.events.at(2).value("instrument_identity_status", std::string()) == "mismatch",
           "mismatched strong identities should be surfaced explicitly as mismatches");
    expect(response.events.at(2).value("instrument_identity_policy", std::string()) == "coerced_from_mismatch",
           "mismatched strong identities should surface the coercion policy that kept canonical evidence consistent");
    expect(response.events.at(2).value("source_instrument_identity_strength", std::string()) == "strong",
           "mismatched strong identities should preserve the original source identity strength");

    tape_engine::QueryRequest qualityRequest;
    qualityRequest.requestId = "identity-quality";
    qualityRequest.operation = "read_session_quality";
    qualityRequest.fromSessionSeq = 1;
    qualityRequest.toSessionSeq = 3;
    expect(client.query(qualityRequest, &response, &error), "identity hardening quality query should succeed: " + error);
    expect(response.summary.value("data_quality", json::object()).value("heuristic_instrument_identity_count", 0ULL) == 2ULL,
           "data-quality scoring should count both heuristic fallback and coerced mismatch resolution as heuristic evidence");
    expect(response.summary.value("data_quality", json::object()).value("strong_instrument_identity_count", 0ULL) == 1ULL,
           "data-quality scoring should only count resolved strong identities as strong evidence");
    expect(response.summary.value("data_quality", json::object()).value("source_strong_instrument_identity_count", 0ULL) == 1ULL,
           "data-quality scoring should still report how many broker-supplied source identities were strong-form");
    expect(response.summary.value("data_quality", json::object()).value("mismatched_instrument_identity_count", 0ULL) == 1ULL,
           "data-quality scoring should count mismatched strong identities separately");
    expect(response.summary.value("data_quality", json::object()).value("identity_policy_override_count", 0ULL) == 1ULL,
           "data-quality scoring should count identity coercions when the engine overrides mismatched source identities");

    server.stop();
}

void testTapeEngineCanRejectMismatchedStrongInstrumentIdsInStrictMode() {
    const fs::path rootDir = testDataDir() / "tape-engine-identity-strict";
    const fs::path socketPath = testDataDir() / "tape-engine-identity-strict.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "ib:conid:9401:STK:SMART:USD:INTC";
    config.ringCapacity = 32;
    config.rejectMismatchedStrongInstrumentIds = true;

    tape_engine::Server server(config);
    std::string startError;
    expect(server.start(&startError), "tape-engine should start for strict identity policy test: " + startError);

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-engine-identity-strict";
    options.runtimeSessionId = "runtime-engine-identity-strict";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;

    BridgeOutboxRecord mismatchedStrong = makeBridgeRecord(8501, "order_intent", "WebSocket", "AAPL", "BUY",
                                                           614, 2501, 0, "", "strict identity mismatch", "2026-03-15T09:42:40.300");
    mismatchedStrong.instrumentId = "ib:conid:9502:STK:SMART:USD:MSFT";
    options.batchSeq = 501;
    expect(!transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({mismatchedStrong}, options)), &error),
           "strict identity mode should reject mismatched strong instrument ids at ingest");
    expectContains(error, "mismatched strong instrument_id",
                   "strict identity mode should explain the rejected mismatched strong instrument id");

    waitUntil([&]() {
        const auto snapshot = server.snapshot();
        return snapshot.latestFrozenRevisionId == 0 && snapshot.segments.empty();
    }, "strict identity mode should not freeze a rejected mismatched batch");

    server.stop();
}

void testTapeEnginePhase3CollapsesRepeatedFindingsIntoRankedIncidents() {
    const fs::path rootDir = testDataDir() / "tape-engine-phase3-incidents";
    const fs::path socketPath = testDataDir() / "tape-engine-phase3-incidents.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "ib:conid:9201:STK:SMART:USD:INTC";
    config.ringCapacity = 32;

    tape_engine::Server server(config);
    std::string startError;
    expect(server.start(&startError), "tape-engine should start for incident collapse test: " + startError);

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-engine-phase3-incidents";
    options.runtimeSessionId = "runtime-engine-phase3-incidents";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;

    BridgeOutboxRecord orderIntent = makeBridgeRecord(8201, "order_intent", "WebSocket", "INTC", "BUY",
                                                      411, 3101, 0, "", "incident collapse anchor", "2026-03-14T09:42:00.100");
    orderIntent.instrumentId = "ib:conid:9201:STK:SMART:USD:INTC";
    options.batchSeq = 111;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({orderIntent}, options)), &error),
           "tape-engine should accept the incident collapse anchor batch: " + error);

    BridgeOutboxRecord bidTick = makeBridgeRecord(8202, "market_tick", "BrokerMarketData", "INTC", "BID",
                                                  0, 0, 0, "", "collapse bid", "2026-03-14T09:42:00.120");
    bidTick.instrumentId = "ib:conid:9201:STK:SMART:USD:INTC";
    bidTick.marketField = 1;
    bidTick.price = 45.30;

    BridgeOutboxRecord askTick = makeBridgeRecord(8203, "market_tick", "BrokerMarketData", "INTC", "ASK",
                                                  0, 0, 0, "", "collapse ask", "2026-03-14T09:42:00.130");
    askTick.instrumentId = "ib:conid:9201:STK:SMART:USD:INTC";
    askTick.marketField = 2;
    askTick.price = 45.31;

    options.batchSeq = 112;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({bidTick, askTick}, options)), &error),
           "tape-engine should accept the incident collapse seed market batch: " + error);

    BridgeOutboxRecord widenedAskOne = makeBridgeRecord(8204, "market_tick", "BrokerMarketData", "INTC", "ASK",
                                                        0, 0, 0, "", "collapse widened ask one", "2026-03-14T09:42:00.140");
    widenedAskOne.instrumentId = "ib:conid:9201:STK:SMART:USD:INTC";
    widenedAskOne.marketField = 2;
    widenedAskOne.price = 45.33;

    options.batchSeq = 113;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({widenedAskOne}, options)), &error),
           "tape-engine should accept the first widening batch: " + error);

    BridgeOutboxRecord widenedAskTwo = makeBridgeRecord(8205, "market_tick", "BrokerMarketData", "INTC", "ASK",
                                                        0, 0, 0, "", "collapse widened ask two", "2026-03-14T09:42:00.150");
    widenedAskTwo.instrumentId = "ib:conid:9201:STK:SMART:USD:INTC";
    widenedAskTwo.marketField = 2;
    widenedAskTwo.price = 45.35;

    options.batchSeq = 114;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({widenedAskTwo}, options)), &error),
           "tape-engine should accept the second widening batch: " + error);

    BridgeOutboxRecord widenedAskThree = makeBridgeRecord(8206, "market_tick", "BrokerMarketData", "INTC", "ASK",
                                                          0, 0, 0, "", "collapse widened ask three", "2026-03-14T09:42:06.250");
    widenedAskThree.instrumentId = "ib:conid:9201:STK:SMART:USD:INTC";
    widenedAskThree.marketField = 2;
    widenedAskThree.price = 45.37;

    options.batchSeq = 115;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({widenedAskThree}, options)), &error),
           "tape-engine should accept the third widening batch after the merge window: " + error);

    waitUntil([&]() {
        const auto snapshot = server.snapshot();
        return snapshot.latestFrozenRevisionId >= 5 && snapshot.segments.size() >= 5;
    }, "tape-engine should freeze repeated spread widening batches");

    tape_engine::Client client(socketPath.string());
    tape_engine::QueryResponse response;

    tape_engine::QueryRequest findingsRequest;
    findingsRequest.requestId = "findings-phase3-collapse";
    findingsRequest.operation = "list_findings";
    expect(client.query(findingsRequest, &response, &error), "phase 3 collapse findings query should succeed: " + error);
    expect(response.events.is_array() && response.events.size() >= 3, "phase 3 collapse should retain multiple spread findings");
    std::size_t spreadFindingCount = 0;
    for (const auto& item : response.events) {
        if (item.value("kind", std::string()) == "spread_widened") {
            ++spreadFindingCount;
        }
    }
    expect(spreadFindingCount >= 3, "phase 3 collapse should retain all spread-widened findings even when richer analyzers add newer findings");

    tape_engine::QueryRequest incidentsRequest;
    incidentsRequest.requestId = "incidents-phase3-collapse";
    incidentsRequest.operation = "list_incidents";
    expect(client.query(incidentsRequest, &response, &error), "phase 3 collapse incidents query should succeed: " + error);
    expect(response.events.is_array() && !response.events.empty(), "phase 3 collapse should surface ranked logical incidents");
    expect(response.summary.value("collapsed_logical_incidents", false), "phase 3 collapse should report logical incident collapsing");
    json collapsedSpreadIncident = json::object();
    for (const auto& item : response.events) {
        if (item.value("kind", std::string()) == "spread_widened") {
            collapsedSpreadIncident = item;
            break;
        }
    }
    expect(!collapsedSpreadIncident.is_null() && !collapsedSpreadIncident.empty(), "phase 3 collapse should preserve a spread_widened logical incident");
    expect(collapsedSpreadIncident.value("finding_count", 0ULL) == 3ULL, "collapsed spread incident should count all repeated findings");
    expect(collapsedSpreadIncident.value("score", 0.0) > 3.0, "collapsed spread incident should accumulate a higher incident score");
    expect(collapsedSpreadIncident.value("overlaps_order", false), "collapsed spread incident should preserve order overlap");

    const std::uint64_t logicalIncidentId = collapsedSpreadIncident.value("logical_incident_id", 0ULL);
    expect(logicalIncidentId > 0, "collapsed incident should expose a logical incident id for drilldown");

    tape_engine::QueryRequest readIncidentRequest;
    readIncidentRequest.requestId = "incident-phase3-collapse";
    readIncidentRequest.operation = "read_incident";
    readIncidentRequest.logicalIncidentId = logicalIncidentId;
    expect(client.query(readIncidentRequest, &response, &error), "phase 3 collapse incident read should succeed: " + error);
    expect(response.summary.value("logical_incident_id", 0ULL) == logicalIncidentId, "incident drilldown should preserve the requested logical incident id");
    expect(response.summary.value("incident_revision_count", 0ULL) == 3ULL, "incident drilldown should report all incident revisions");
    expect(response.summary.value("related_finding_count", 0ULL) == 3ULL, "incident drilldown should report all related findings");
    expect(response.summary.contains("score_breakdown"), "incident drilldown should include a score breakdown");
    expect(response.summary.value("score_breakdown", json::object()).contains("data_quality_penalty_factor"),
           "incident drilldown score breakdown should expose the data-quality penalty factor");
    expect(response.summary.contains("why_it_matters"), "incident drilldown should include a why-it-matters summary");
    expect(response.summary.contains("protected_window"), "incident drilldown should include the incident protected window");
    expect(response.summary.contains("data_quality"), "incident drilldown should include data-quality scoring");
    expect(response.summary.contains("timeline"), "incident drilldown should now include a merged investigation timeline");
    expect(response.summary.contains("timeline_summary"), "incident drilldown should summarize the merged investigation timeline");
    expect(response.summary.value("timeline_summary", json::object()).value("incident_count", 0ULL) >= 1ULL,
           "incident drilldown timeline summary should include at least the incident entry");
    expect(response.summary.value("report_summary", json::object()).contains("timeline_highlights"),
           "incident drilldown report summary should include timeline highlights");
    expect(response.events.is_array() && response.events.size() == 3, "incident drilldown should return the related findings as evidence");
    expect(response.events.at(0).value("logical_incident_id", 0ULL) == logicalIncidentId, "incident drilldown findings should be linked to the logical incident");
    const json incidentWindow = response.summary.value("protected_window", json::object());
    expect(incidentWindow.value("first_session_seq", 0ULL) < incidentWindow.value("last_session_seq", 0ULL),
           "incident protected window should widen its session-seq bounds across repeated promotions");

    tape_engine::QueryRequest windowsRequest;
    windowsRequest.requestId = "windows-phase3-collapse";
    windowsRequest.operation = "list_protected_windows";
    expect(client.query(windowsRequest, &response, &error), "phase 3 collapse windows query should succeed: " + error);
    std::set<std::uint64_t> incidentWindowIds;
    std::size_t incidentWindowRevisionCount = 0;
    for (const auto& item : response.events) {
        if (item.value("reason", std::string()) != "incident_promotion") {
            continue;
        }
        if (item.value("logical_incident_id", 0ULL) != logicalIncidentId) {
            continue;
        }
        incidentWindowIds.insert(item.value("window_id", 0ULL));
        ++incidentWindowRevisionCount;
    }
    expect(incidentWindowIds.size() == 1, "repeated promotions should keep one stable incident protected-window identity");
    expect(incidentWindowRevisionCount >= 2, "stable incident protected window should accumulate revisions as evidence widens");

    server.stop();
}

void testTapeEnginePhase3DetectsInsideLiquiditySignals() {
    const fs::path rootDir = testDataDir() / "tape-engine-phase3-liquidity";
    const fs::path socketPath = testDataDir() / "tape-engine-phase3-liquidity.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "ib:conid:9301:STK:SMART:USD:INTC";
    config.ringCapacity = 32;

    tape_engine::Server server(config);
    std::string startError;
    expect(server.start(&startError), "tape-engine should start for liquidity signal test: " + startError);

    auto marketRecord = [](std::uint64_t sourceSeq,
                           const std::string& recordType,
                           const std::string& side,
                           int marketField,
                           int bookPosition,
                           int bookOperation,
                           int bookSide,
                           double price,
                           double size,
                           const std::string& note) {
        BridgeOutboxRecord record = makeBridgeRecord(sourceSeq, recordType, "BrokerMarketData", "INTC", side,
                                                     0, 0, 0, "", note, "2026-03-14T09:43:00.000");
        record.instrumentId = "ib:conid:9301:STK:SMART:USD:INTC";
        record.marketField = marketField;
        record.bookPosition = bookPosition;
        record.bookOperation = bookOperation;
        record.bookSide = bookSide;
        record.price = price;
        record.size = size;
        return record;
    };

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-engine-phase3-liquidity";
    options.runtimeSessionId = "runtime-engine-phase3-liquidity";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;

    BridgeOutboxRecord orderIntent = makeBridgeRecord(8301, "order_intent", "WebSocket", "INTC", "BUY",
                                                      511, 4101, 0, "", "liquidity anchor", "2026-03-14T09:43:00.050");
    orderIntent.instrumentId = "ib:conid:9301:STK:SMART:USD:INTC";
    options.batchSeq = 121;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({orderIntent}, options)), &error),
           "tape-engine should accept the liquidity anchor batch: " + error);

    options.batchSeq = 122;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        marketRecord(8302, "market_tick", "BID", 1, -1, -1, -1, 45.40, std::numeric_limits<double>::quiet_NaN(), "liquidity bid"),
        marketRecord(8303, "market_tick", "ASK", 2, -1, -1, -1, 45.41, std::numeric_limits<double>::quiet_NaN(), "liquidity ask"),
        marketRecord(8304, "market_depth", "ASK", -1, 0, 0, 0, 45.41, 400.0, "liquidity ask insert"),
        marketRecord(8305, "market_depth", "BID", -1, 0, 0, 1, 45.40, 500.0, "liquidity bid insert")
    }, options)), &error), "tape-engine should accept the liquidity seed batch: " + error);

    options.batchSeq = 123;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        marketRecord(8306, "market_depth", "ASK", -1, 0, 1, 0, 45.41, 100.0, "liquidity ask thin"),
        marketRecord(8307, "market_depth", "ASK", -1, 0, 1, 0, 45.41, 350.0, "liquidity ask refill")
    }, options)), &error), "tape-engine should accept the liquidity change batch: " + error);

    waitUntil([&]() {
        const auto snapshot = server.snapshot();
        return snapshot.latestFrozenRevisionId >= 3 && snapshot.segments.size() >= 3;
    }, "tape-engine should freeze liquidity signal batches");

    tape_engine::Client client(socketPath.string());
    tape_engine::QueryResponse response;

    tape_engine::QueryRequest findingsRequest;
    findingsRequest.requestId = "findings-phase3-liquidity";
    findingsRequest.operation = "list_findings";
    expect(client.query(findingsRequest, &response, &error), "phase 3 liquidity findings query should succeed: " + error);
    bool sawAskThin = false;
    bool sawAskRefill = false;
    for (const auto& item : response.events) {
        const std::string kind = item.value("kind", std::string());
        sawAskThin = sawAskThin || kind == "ask_liquidity_thinned";
        sawAskRefill = sawAskRefill || kind == "ask_liquidity_refilled";
    }
    expect(sawAskThin, "phase 3 liquidity should detect inside ask thinning");
    expect(sawAskRefill, "phase 3 liquidity should detect inside ask refill");

    tape_engine::QueryRequest incidentsRequest;
    incidentsRequest.requestId = "incidents-phase3-liquidity";
    incidentsRequest.operation = "list_incidents";
    expect(client.query(incidentsRequest, &response, &error), "phase 3 liquidity incidents query should succeed: " + error);
    bool sawRankedLiquidityIncident = false;
    for (const auto& item : response.events) {
        const std::string kind = item.value("kind", std::string());
        if ((kind == "ask_liquidity_thinned" || kind == "ask_liquidity_refilled") &&
            item.value("score", 0.0) > 0.5) {
            sawRankedLiquidityIncident = true;
        }
    }
    expect(sawRankedLiquidityIncident, "phase 3 liquidity incidents should surface ranked liquidity signals");

    server.stop();
}

void testTapeEnginePhase3DetectsDisplayInstabilitySignals() {
    const fs::path rootDir = testDataDir() / "tape-engine-phase3-display-instability";
    const fs::path socketPath = testDataDir() / "tape-engine-phase3-display-instability.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "ib:conid:9351:STK:SMART:USD:INTC";
    config.ringCapacity = 32;

    tape_engine::Server server(config);
    std::string startError;
    expect(server.start(&startError), "tape-engine should start for display-instability signal test: " + startError);

    auto marketRecord = [](std::uint64_t sourceSeq,
                           const std::string& recordType,
                           const std::string& side,
                           int marketField,
                           int bookPosition,
                           int bookOperation,
                           int bookSide,
                           double price,
                           double size,
                           const std::string& note) {
        BridgeOutboxRecord record = makeBridgeRecord(sourceSeq, recordType, "BrokerMarketData", "INTC", side,
                                                     0, 0, 0, "", note, "2026-03-14T09:43:30.000");
        record.instrumentId = "ib:conid:9351:STK:SMART:USD:INTC";
        record.marketField = marketField;
        record.bookPosition = bookPosition;
        record.bookOperation = bookOperation;
        record.bookSide = bookSide;
        record.price = price;
        record.size = size;
        return record;
    };

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-engine-phase3-display";
    options.runtimeSessionId = "runtime-engine-phase3-display";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;

    BridgeOutboxRecord orderIntent = makeBridgeRecord(8351, "order_intent", "WebSocket", "INTC", "BUY",
                                                      531, 4301, 0, "", "display instability anchor", "2026-03-14T09:43:30.050");
    orderIntent.instrumentId = "ib:conid:9351:STK:SMART:USD:INTC";
    options.batchSeq = 124;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({orderIntent}, options)), &error),
           "tape-engine should accept the display-instability anchor batch: " + error);

    options.batchSeq = 125;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        marketRecord(8352, "market_tick", "BID", 1, -1, -1, -1, 45.45, std::numeric_limits<double>::quiet_NaN(), "display bid"),
        marketRecord(8353, "market_tick", "ASK", 2, -1, -1, -1, 45.46, std::numeric_limits<double>::quiet_NaN(), "display ask"),
        marketRecord(8354, "market_depth", "ASK", -1, 0, 0, 0, 45.46, 400.0, "display ask insert"),
        marketRecord(8355, "market_depth", "BID", -1, 0, 0, 1, 45.45, 500.0, "display bid insert")
    }, options)), &error), "tape-engine should accept the display-instability seed batch: " + error);

    options.batchSeq = 126;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        marketRecord(8356, "market_depth", "ASK", -1, 0, 1, 0, 45.46, 100.0, "display ask remove one"),
        marketRecord(8357, "market_depth", "ASK", -1, 0, 1, 0, 45.46, 360.0, "display ask readd one"),
        marketRecord(8358, "market_depth", "ASK", -1, 0, 1, 0, 45.46, 120.0, "display ask remove two"),
        marketRecord(8359, "market_depth", "ASK", -1, 0, 1, 0, 45.46, 350.0, "display ask readd two")
    }, options)), &error), "tape-engine should accept the display-instability change batch: " + error);

    waitUntil([&]() {
        const auto snapshot = server.snapshot();
        return snapshot.latestFrozenRevisionId >= 3 && snapshot.segments.size() >= 3;
    }, "tape-engine should freeze display-instability signal batches");

    tape_engine::Client client(socketPath.string());
    tape_engine::QueryResponse response;

    tape_engine::QueryRequest findingsRequest;
    findingsRequest.requestId = "findings-phase3-display";
    findingsRequest.operation = "list_findings";
    expect(client.query(findingsRequest, &response, &error), "phase 3 display-instability findings query should succeed: " + error);
    bool sawDisplayInstabilityFinding = false;
    for (const auto& item : response.events) {
        sawDisplayInstabilityFinding = sawDisplayInstabilityFinding || item.value("kind", std::string()) == "ask_display_instability";
    }
    expect(sawDisplayInstabilityFinding, "phase 3 should detect repeated ask display instability at the touch");

    tape_engine::QueryRequest incidentsRequest;
    incidentsRequest.requestId = "incidents-phase3-display";
    incidentsRequest.operation = "list_incidents";
    expect(client.query(incidentsRequest, &response, &error), "phase 3 display-instability incidents query should succeed: " + error);
    bool sawDisplayInstabilityIncident = false;
    for (const auto& item : response.events) {
        if (item.value("kind", std::string()) == "ask_display_instability" &&
            item.value("score", 0.0) > 0.5) {
            sawDisplayInstabilityIncident = true;
        }
    }
    expect(sawDisplayInstabilityIncident, "phase 3 display-instability incidents should surface a ranked instability incident");

    server.stop();
}

void testTapeEnginePhase3DetectsPullFollowThroughAndQuoteFlickerSignals() {
    const fs::path rootDir = testDataDir() / "tape-engine-phase3-pull-flicker";
    const fs::path socketPath = testDataDir() / "tape-engine-phase3-pull-flicker.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "ib:conid:9361:STK:SMART:USD:INTC";
    config.ringCapacity = 32;

    tape_engine::Server server(config);
    std::string startError;
    expect(server.start(&startError), "tape-engine should start for pull-follow-through/quote-flicker signal test: " + startError);

    auto marketRecord = [](std::uint64_t sourceSeq,
                           const std::string& recordType,
                           const std::string& side,
                           int marketField,
                           int bookPosition,
                           int bookOperation,
                           int bookSide,
                           double price,
                           double size,
                           const std::string& note) {
        BridgeOutboxRecord record = makeBridgeRecord(sourceSeq, recordType, "BrokerMarketData", "INTC", side,
                                                     0, 0, 0, "", note, "2026-03-14T09:43:38.000");
        record.instrumentId = "ib:conid:9361:STK:SMART:USD:INTC";
        record.marketField = marketField;
        record.bookPosition = bookPosition;
        record.bookOperation = bookOperation;
        record.bookSide = bookSide;
        record.price = price;
        record.size = size;
        return record;
    };

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-engine-phase3-pull-flicker";
    options.runtimeSessionId = "runtime-engine-phase3-pull-flicker";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;

    BridgeOutboxRecord orderIntent = makeBridgeRecord(8361, "order_intent", "WebSocket", "INTC", "BUY",
                                                      536, 4351, 0, "", "pull/flicker anchor", "2026-03-14T09:43:38.050");
    orderIntent.instrumentId = "ib:conid:9361:STK:SMART:USD:INTC";
    options.batchSeq = 1261;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({orderIntent}, options)), &error),
           "tape-engine should accept the pull/flicker anchor batch: " + error);

    options.batchSeq = 1262;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        marketRecord(8362, "market_tick", "BID", 1, -1, -1, -1, 45.60, std::numeric_limits<double>::quiet_NaN(), "pull/flicker bid"),
        marketRecord(8363, "market_tick", "ASK", 2, -1, -1, -1, 45.61, std::numeric_limits<double>::quiet_NaN(), "pull/flicker ask"),
        marketRecord(8364, "market_depth", "ASK", -1, 0, 0, 0, 45.61, 400.0, "pull/flicker ask insert"),
        marketRecord(8365, "market_depth", "BID", -1, 0, 0, 1, 45.60, 500.0, "pull/flicker bid insert")
    }, options)), &error), "tape-engine should accept the pull/flicker seed batch: " + error);

    options.batchSeq = 1263;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        marketRecord(8366, "market_depth", "ASK", -1, 0, 1, 0, 45.61, 100.0, "pull/flicker ask thin"),
        marketRecord(8367, "market_depth", "ASK", -1, 0, 1, 0, 45.63, 100.0, "pull/flicker ask move one"),
        marketRecord(8368, "market_depth", "ASK", -1, 0, 1, 0, 45.62, 110.0, "pull/flicker ask move two"),
        marketRecord(8369, "market_depth", "ASK", -1, 0, 1, 0, 45.64, 90.0, "pull/flicker ask move three")
    }, options)), &error), "tape-engine should accept the pull/flicker change batch: " + error);

    waitUntil([&]() {
        const auto snapshot = server.snapshot();
        return snapshot.latestFrozenRevisionId >= 3 && snapshot.segments.size() >= 3;
    }, "tape-engine should freeze pull-follow-through and quote-flicker batches");

    tape_engine::Client client(socketPath.string());
    tape_engine::QueryResponse response;

    tape_engine::QueryRequest findingsRequest;
    findingsRequest.requestId = "findings-phase3-pull-flicker";
    findingsRequest.operation = "list_findings";
    expect(client.query(findingsRequest, &response, &error), "phase 3 pull-follow-through findings query should succeed: " + error);
    bool sawPullFollowThrough = false;
    bool sawQuoteFlicker = false;
    for (const auto& item : response.events) {
        const std::string kind = item.value("kind", std::string());
        sawPullFollowThrough = sawPullFollowThrough || kind == "ask_pull_follow_through";
        sawQuoteFlicker = sawQuoteFlicker || kind == "ask_quote_flicker";
    }
    expect(sawPullFollowThrough, "phase 3 should detect ask pull follow-through after inside liquidity pulls");
    expect(sawQuoteFlicker, "phase 3 should detect repeated ask quote flicker at the touch");

    tape_engine::QueryRequest incidentsRequest;
    incidentsRequest.requestId = "incidents-phase3-pull-flicker";
    incidentsRequest.operation = "list_incidents";
    expect(client.query(incidentsRequest, &response, &error), "phase 3 pull/flicker incidents query should succeed: " + error);
    double pullFollowThroughScore = 0.0;
    double quoteFlickerScore = 0.0;
    for (const auto& item : response.events) {
        if (item.value("kind", std::string()) == "ask_pull_follow_through") {
            pullFollowThroughScore = item.value("score", 0.0);
        } else if (item.value("kind", std::string()) == "ask_quote_flicker") {
            quoteFlickerScore = item.value("score", 0.0);
        }
    }
    expect(pullFollowThroughScore > 0.5, "phase 3 pull follow-through should surface as a ranked incident");
    expect(quoteFlickerScore > 0.5, "phase 3 quote flicker should surface as a ranked incident");
    expect(pullFollowThroughScore > quoteFlickerScore,
           "phase 3 ranking should score pull follow-through above quote flicker because it is closer to realized follow-through");

    server.stop();
}

void testTapeEnginePhase3DetectsTradeAfterDepletionAndAbsorptionPersistenceSignals() {
    const fs::path rootDir = testDataDir() / "tape-engine-phase3-depletion-absorption";
    const fs::path socketPath = testDataDir() / "tape-engine-phase3-depletion-absorption.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "ib:conid:9366:STK:SMART:USD:INTC";
    config.ringCapacity = 32;

    tape_engine::Server server(config);
    std::string startError;
    expect(server.start(&startError), "tape-engine should start for depletion/absorption signal test: " + startError);

    auto marketRecord = [](std::uint64_t sourceSeq,
                           const std::string& recordType,
                           const std::string& side,
                           int marketField,
                           int bookPosition,
                           int bookOperation,
                           int bookSide,
                           double price,
                           double size,
                           const std::string& note) {
        BridgeOutboxRecord record = makeBridgeRecord(sourceSeq, recordType, "BrokerMarketData", "INTC", side,
                                                     0, 0, 0, "", note, "2026-03-14T09:43:40.000");
        record.instrumentId = "ib:conid:9366:STK:SMART:USD:INTC";
        record.marketField = marketField;
        record.bookPosition = bookPosition;
        record.bookOperation = bookOperation;
        record.bookSide = bookSide;
        record.price = price;
        record.size = size;
        return record;
    };

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-engine-phase3-depletion-absorption";
    options.runtimeSessionId = "runtime-engine-phase3-depletion-absorption";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;

    BridgeOutboxRecord orderIntent = makeBridgeRecord(83660, "order_intent", "WebSocket", "INTC", "BUY",
                                                      537, 4366, 0, "", "depletion/absorption anchor", "2026-03-14T09:43:40.050");
    orderIntent.instrumentId = "ib:conid:9366:STK:SMART:USD:INTC";
    options.batchSeq = 1266;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({orderIntent}, options)), &error),
           "tape-engine should accept the depletion/absorption anchor batch: " + error);

    options.batchSeq = 1267;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        marketRecord(83661, "market_tick", "BID", 1, -1, -1, -1, 45.70, std::numeric_limits<double>::quiet_NaN(), "depletion/absorption bid"),
        marketRecord(83662, "market_tick", "ASK", 2, -1, -1, -1, 45.71, std::numeric_limits<double>::quiet_NaN(), "depletion/absorption ask"),
        marketRecord(83663, "market_depth", "ASK", -1, 0, 0, 0, 45.71, 400.0, "depletion/absorption ask insert"),
        marketRecord(83664, "market_depth", "BID", -1, 0, 0, 1, 45.70, 500.0, "depletion/absorption bid insert")
    }, options)), &error), "tape-engine should accept the depletion/absorption seed batch: " + error);

    options.batchSeq = 1268;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        marketRecord(83665, "market_tick", "LAST", 4, -1, -1, -1, 45.71, std::numeric_limits<double>::quiet_NaN(), "touch trade before depletion"),
        marketRecord(83666, "market_depth", "ASK", -1, 0, 1, 0, 45.71, 100.0, "depletion ask thin"),
        marketRecord(83667, "market_tick", "LAST", 4, -1, -1, -1, 45.71, std::numeric_limits<double>::quiet_NaN(), "trade after depletion"),
        marketRecord(83668, "market_depth", "ASK", -1, 0, 1, 0, 45.71, 380.0, "absorption ask refill"),
        marketRecord(83669, "market_tick", "LAST", 4, -1, -1, -1, 45.71, std::numeric_limits<double>::quiet_NaN(), "touch trade after refill"),
        marketRecord(83670, "market_depth", "ASK", -1, 0, 1, 0, 45.71, 360.0, "absorption ask holds"),
        marketRecord(83671, "market_depth", "ASK", -1, 0, 1, 0, 45.71, 560.0, "genuine refill ask"),
        marketRecord(83672, "market_tick", "BID", 1, -1, -1, -1, 45.70, std::numeric_limits<double>::quiet_NaN(), "genuine refill stable update one"),
        marketRecord(83673, "market_tick", "ASK", 2, -1, -1, -1, 45.71, std::numeric_limits<double>::quiet_NaN(), "genuine refill stable update two"),
        marketRecord(83674, "market_depth", "BID", -1, 0, 1, 1, 45.70, 480.0, "genuine refill stable update three")
    }, options)), &error), "tape-engine should accept the depletion/absorption change batch: " + error);

    waitUntil([&]() {
        const auto snapshot = server.snapshot();
        return snapshot.latestFrozenRevisionId >= 3 && snapshot.segments.size() >= 3;
    }, "tape-engine should freeze depletion/absorption batches");

    tape_engine::Client client(socketPath.string());
    tape_engine::QueryResponse response;

    tape_engine::QueryRequest findingsRequest;
    findingsRequest.requestId = "findings-phase3-depletion-absorption";
    findingsRequest.operation = "list_findings";
    expect(client.query(findingsRequest, &response, &error), "phase 3 depletion/absorption findings query should succeed: " + error);
    bool sawTradeAfterDepletion = false;
    bool sawDepletionAfterTrade = false;
    bool sawAbsorptionPersistence = false;
    bool sawGenuineRefill = false;
    for (const auto& item : response.events) {
        const std::string kind = item.value("kind", std::string());
        sawTradeAfterDepletion = sawTradeAfterDepletion || kind == "ask_trade_after_depletion";
        sawDepletionAfterTrade = sawDepletionAfterTrade || kind == "ask_depletion_after_trade";
        sawAbsorptionPersistence = sawAbsorptionPersistence || kind == "ask_absorption_persistence";
        sawGenuineRefill = sawGenuineRefill || kind == "ask_genuine_refill";
    }
    expect(sawTradeAfterDepletion, "phase 3 should detect trade-after-depletion at the depleted ask");
    expect(sawDepletionAfterTrade, "phase 3 should detect depletion-after-trade at the ask touch");
    expect(sawAbsorptionPersistence, "phase 3 should detect ask absorption persistence after refill holds");
    expect(sawGenuineRefill, "phase 3 should distinguish a genuine refill from absorption when refill holds without touch trades");

    tape_engine::QueryRequest incidentsRequest;
    incidentsRequest.requestId = "incidents-phase3-depletion-absorption";
    incidentsRequest.operation = "list_incidents";
    expect(client.query(incidentsRequest, &response, &error), "phase 3 depletion/absorption incidents query should succeed: " + error);
    bool sawTradeAfterDepletionIncident = false;
    bool sawDepletionAfterTradeIncident = false;
    bool sawAbsorptionPersistenceIncident = false;
    bool sawGenuineRefillIncident = false;
    for (const auto& item : response.events) {
        const std::string kind = item.value("kind", std::string());
        if (kind == "ask_trade_after_depletion" && item.value("score", 0.0) > 0.5) {
            sawTradeAfterDepletionIncident = true;
        }
        if (kind == "ask_depletion_after_trade" && item.value("score", 0.0) > 0.5) {
            sawDepletionAfterTradeIncident = true;
        }
        if (kind == "ask_absorption_persistence" && item.value("score", 0.0) > 0.5) {
            sawAbsorptionPersistenceIncident = true;
        }
        if (kind == "ask_genuine_refill" && item.value("score", 0.0) > 0.5) {
            sawGenuineRefillIncident = true;
        }
    }
    expect(sawTradeAfterDepletionIncident, "phase 3 trade-after-depletion should surface as a ranked incident");
    expect(sawDepletionAfterTradeIncident, "phase 3 depletion-after-trade should surface as a ranked incident");
    expect(sawAbsorptionPersistenceIncident, "phase 3 absorption persistence should surface as a ranked incident");
    expect(sawGenuineRefillIncident, "phase 3 genuine refill should surface as a ranked incident");

    server.stop();
}

void testTapeEnginePhase3DetectsFillInvalidationSignals() {
    const fs::path rootDir = testDataDir() / "tape-engine-phase3-fill-invalidation";
    const fs::path socketPath = testDataDir() / "tape-engine-phase3-fill-invalidation.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "ib:conid:9381:STK:SMART:USD:INTC";
    config.ringCapacity = 32;

    tape_engine::Server server(config);
    std::string startError;
    expect(server.start(&startError), "tape-engine should start for fill-invalidation signal test: " + startError);

    auto marketTick = [](std::uint64_t sourceSeq,
                         int field,
                         double price,
                         const std::string& note) {
        BridgeOutboxRecord record = makeBridgeRecord(sourceSeq, "market_tick", "BrokerMarketData", "INTC",
                                                     field == 1 ? "BID" : (field == 2 ? "ASK" : "LAST"),
                                                     0, 0, 0, "", note, "2026-03-14T09:43:45.000");
        record.instrumentId = "ib:conid:9381:STK:SMART:USD:INTC";
        record.marketField = field;
        record.price = price;
        return record;
    };

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-engine-phase3-fill-invalid";
    options.runtimeSessionId = "runtime-engine-phase3-fill-invalid";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;

    BridgeOutboxRecord orderIntent = makeBridgeRecord(8381, "order_intent", "WebSocket", "INTC", "BUY",
                                                      541, 4401, 0, "", "fill invalidation anchor", "2026-03-14T09:43:45.050");
    orderIntent.instrumentId = "ib:conid:9381:STK:SMART:USD:INTC";
    options.batchSeq = 127;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({orderIntent}, options)), &error),
           "tape-engine should accept the fill-invalidation anchor batch: " + error);

    options.batchSeq = 128;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        marketTick(8382, 1, 45.50, "fill invalidation bid"),
        marketTick(8383, 2, 45.51, "fill invalidation ask")
    }, options)), &error), "tape-engine should accept the fill-invalidation seed batch: " + error);

    BridgeOutboxRecord fill = makeBridgeRecord(8384, "fill_execution", "BrokerExecution", "INTC", "BOT",
                                               541, 4401, 0, "exec-4401-a", "fill invalidation fill", "2026-03-14T09:43:45.070");
    fill.instrumentId = "ib:conid:9381:STK:SMART:USD:INTC";
    fill.price = 45.51;
    fill.size = 100.0;
    options.batchSeq = 129;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({fill}, options)), &error),
           "tape-engine should accept the fill-invalidation fill batch: " + error);

    options.batchSeq = 130;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        marketTick(8385, 1, 45.48, "fill invalidation bid down"),
        marketTick(8386, 2, 45.49, "fill invalidation ask down")
    }, options)), &error), "tape-engine should accept the post-fill invalidation batch: " + error);

    waitUntil([&]() {
        const auto snapshot = server.snapshot();
        return snapshot.latestFrozenRevisionId >= 4 && snapshot.segments.size() >= 4;
    }, "tape-engine should freeze fill-invalidation signal batches");

    tape_engine::Client client(socketPath.string());
    tape_engine::QueryResponse response;

    tape_engine::QueryRequest findingsRequest;
    findingsRequest.requestId = "findings-phase3-fill-invalid";
    findingsRequest.operation = "list_findings";
    expect(client.query(findingsRequest, &response, &error), "phase 3 fill-invalidation findings query should succeed: " + error);
    bool sawFillInvalidationFinding = false;
    for (const auto& item : response.events) {
        sawFillInvalidationFinding = sawFillInvalidationFinding || item.value("kind", std::string()) == "buy_fill_invalidation";
    }
    expect(sawFillInvalidationFinding, "phase 3 should detect a buy fill invalidation after adverse post-fill movement");

    tape_engine::QueryRequest incidentsRequest;
    incidentsRequest.requestId = "incidents-phase3-fill-invalid";
    incidentsRequest.operation = "list_incidents";
    expect(client.query(incidentsRequest, &response, &error), "phase 3 fill-invalidation incidents query should succeed: " + error);
    bool sawFillInvalidationIncident = false;
    for (const auto& item : response.events) {
        if (item.value("kind", std::string()) == "buy_fill_invalidation" &&
            item.value("score", 0.0) > 0.5) {
            sawFillInvalidationIncident = true;
        }
    }
    expect(sawFillInvalidationIncident, "phase 3 fill-invalidation incidents should surface a ranked invalidation incident");

    tape_engine::QueryRequest caseRequest;
    caseRequest.requestId = "order-case-phase3-fill-invalid";
    caseRequest.operation = "read_order_case";
    caseRequest.orderId = 4401;
    expect(client.query(caseRequest, &response, &error), "phase 3 fill-invalidation order-case query should succeed: " + error);
    bool sawInvalidationRelatedFinding = false;
    for (const auto& item : response.summary.value("related_findings", json::array())) {
        sawInvalidationRelatedFinding = sawInvalidationRelatedFinding || item.value("kind", std::string()) == "buy_fill_invalidation";
    }
    expect(sawInvalidationRelatedFinding, "order-case related findings should include the fill invalidation signal");

    server.stop();
}

void testTapeEnginePhase3BuildsOrderWindowMarketImpactFinding() {
    const fs::path rootDir = testDataDir() / "tape-engine-phase3-market-impact";
    const fs::path socketPath = testDataDir() / "tape-engine-phase3-market-impact.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "ib:conid:9391:STK:SMART:USD:INTC";
    config.ringCapacity = 32;

    tape_engine::Server server(config);
    std::string startError;
    expect(server.start(&startError), "tape-engine should start for order-window market-impact test: " + startError);

    auto marketTick = [](std::uint64_t sourceSeq,
                         int field,
                         double price,
                         const std::string& note) {
        BridgeOutboxRecord record = makeBridgeRecord(sourceSeq, "market_tick", "BrokerMarketData", "INTC",
                                                     field == 1 ? "BID" : (field == 2 ? "ASK" : "LAST"),
                                                     0, 0, 0, "", note, "2026-03-14T09:43:50.000");
        record.instrumentId = "ib:conid:9391:STK:SMART:USD:INTC";
        record.marketField = field;
        record.price = price;
        return record;
    };

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-engine-phase3-market-impact";
    options.runtimeSessionId = "runtime-engine-phase3-market-impact";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;

    BridgeOutboxRecord orderIntent = makeBridgeRecord(8391, "order_intent", "WebSocket", "INTC", "BUY",
                                                      551, 4451, 0, "", "market-impact anchor", "2026-03-14T09:43:50.050");
    orderIntent.instrumentId = "ib:conid:9391:STK:SMART:USD:INTC";
    options.batchSeq = 131;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({orderIntent}, options)), &error),
           "tape-engine should accept the market-impact anchor batch: " + error);

    options.batchSeq = 132;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        marketTick(8392, 1, 45.80, "market impact bid"),
        marketTick(8393, 2, 45.81, "market impact ask")
    }, options)), &error), "tape-engine should accept the market-impact seed batch: " + error);

    BridgeOutboxRecord fill = makeBridgeRecord(8394, "fill_execution", "BrokerExecution", "INTC", "BOT",
                                               551, 4451, 0, "exec-4451-a", "market impact fill", "2026-03-14T09:43:50.070");
    fill.instrumentId = "ib:conid:9391:STK:SMART:USD:INTC";
    fill.price = 45.81;
    fill.size = 100.0;
    options.batchSeq = 133;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({fill}, options)), &error),
           "tape-engine should accept the market-impact fill batch: " + error);

    options.batchSeq = 134;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        marketTick(8395, 1, 45.75, "market impact bid down"),
        marketTick(8396, 2, 45.77, "market impact ask down")
    }, options)), &error), "tape-engine should accept the market-impact follow-through batch: " + error);

    waitUntil([&]() {
        const auto snapshot = server.snapshot();
        return snapshot.latestFrozenRevisionId >= 4 && snapshot.segments.size() >= 4;
    }, "tape-engine should freeze order-window market-impact batches");

    tape_engine::Client client(socketPath.string());
    tape_engine::QueryResponse response;

    tape_engine::QueryRequest findingsRequest;
    findingsRequest.requestId = "findings-phase3-market-impact";
    findingsRequest.operation = "list_findings";
    expect(client.query(findingsRequest, &response, &error), "phase 3 market-impact findings query should succeed: " + error);
    bool sawMarketImpactFinding = false;
    for (const auto& item : response.events) {
        sawMarketImpactFinding = sawMarketImpactFinding || item.value("kind", std::string()) == "order_window_market_impact";
    }
    expect(sawMarketImpactFinding, "phase 3 deferred lane should emit an order-window market-impact finding");

    tape_engine::QueryRequest caseRequest;
    caseRequest.requestId = "order-case-phase3-market-impact";
    caseRequest.operation = "read_order_case";
    caseRequest.orderId = 4451;
    expect(client.query(caseRequest, &response, &error), "phase 3 market-impact order-case query should succeed: " + error);
    bool sawMarketImpactRelatedFinding = false;
    for (const auto& item : response.summary.value("related_findings", json::array())) {
        sawMarketImpactRelatedFinding = sawMarketImpactRelatedFinding || item.value("kind", std::string()) == "order_window_market_impact";
    }
    expect(sawMarketImpactRelatedFinding, "order-case related findings should include the order-window market-impact analysis");

    server.stop();
}

void testTapeEnginePhase3BuildsPassiveFillQueueProxyAndAdverseSelection() {
    const fs::path rootDir = testDataDir() / "tape-engine-phase3-passive-fill";
    const fs::path socketPath = testDataDir() / "tape-engine-phase3-passive-fill.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "ib:conid:9399:STK:SMART:USD:INTC";
    config.ringCapacity = 32;

    tape_engine::Server server(config);
    std::string startError;
    expect(server.start(&startError), "tape-engine should start for passive-fill proxy test: " + startError);

    auto marketTick = [](std::uint64_t sourceSeq,
                         int field,
                         double price,
                         const std::string& note) {
        BridgeOutboxRecord record = makeBridgeRecord(sourceSeq, "market_tick", "BrokerMarketData", "INTC",
                                                     field == 1 ? "BID" : (field == 2 ? "ASK" : "LAST"),
                                                     0, 0, 0, "", note, "2026-03-14T09:43:55.000");
        record.instrumentId = "ib:conid:9399:STK:SMART:USD:INTC";
        record.marketField = field;
        record.price = price;
        return record;
    };
    auto marketDepth = [](std::uint64_t sourceSeq,
                          int side,
                          int operation,
                          double price,
                          double size,
                          const std::string& note) {
        BridgeOutboxRecord record = makeBridgeRecord(sourceSeq, "market_depth", "BrokerMarketData", "INTC",
                                                     side == 1 ? "BID" : "ASK",
                                                     0, 0, 0, "", note, "2026-03-14T09:43:55.000");
        record.instrumentId = "ib:conid:9399:STK:SMART:USD:INTC";
        record.bookPosition = 0;
        record.bookOperation = operation;
        record.bookSide = side;
        record.price = price;
        record.size = size;
        return record;
    };

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-engine-phase3-passive-fill";
    options.runtimeSessionId = "runtime-engine-phase3-passive-fill";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;

    BridgeOutboxRecord orderIntent = makeBridgeRecord(8501, "order_intent", "WebSocket", "INTC", "BUY",
                                                      651, 5501, 0, "", "passive-fill anchor", "2026-03-14T09:43:55.010");
    orderIntent.instrumentId = "ib:conid:9399:STK:SMART:USD:INTC";
    options.batchSeq = 141;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({orderIntent}, options)), &error),
           "tape-engine should accept the passive-fill anchor batch: " + error);

    options.batchSeq = 142;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        marketTick(8502, 1, 45.90, "passive fill bid"),
        marketTick(8503, 2, 45.91, "passive fill ask"),
        marketDepth(8504, 0, 0, 45.91, 220.0, "passive fill ask size"),
        marketDepth(8505, 1, 0, 45.90, 260.0, "passive fill bid size")
    }, options)), &error), "tape-engine should accept the passive-fill seed batch: " + error);

    BridgeOutboxRecord fill = makeBridgeRecord(8506, "fill_execution", "BrokerExecution", "INTC", "BOT",
                                               651, 5501, 0, "exec-5501-a", "passive fill", "2026-03-14T09:43:55.040");
    fill.instrumentId = "ib:conid:9399:STK:SMART:USD:INTC";
    fill.price = 45.91;
    fill.size = 100.0;
    options.batchSeq = 143;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({fill}, options)), &error),
           "tape-engine should accept the passive-fill execution batch: " + error);

    options.batchSeq = 144;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        marketDepth(8507, 0, 1, 45.91, 180.0, "post-fill ask remains"),
        marketDepth(8508, 0, 1, 45.91, 210.0, "post-fill ask refill"),
        marketTick(8509, 1, 45.87, "post-fill bid down"),
        marketTick(8510, 2, 45.89, "post-fill ask down"),
        marketDepth(8511, 0, 1, 45.91, 205.0, "post-fill ask stable")
    }, options)), &error), "tape-engine should accept the passive-fill follow-through batch: " + error);

    waitUntil([&]() {
        const auto snapshot = server.snapshot();
        return snapshot.latestFrozenRevisionId >= 4 && snapshot.segments.size() >= 4;
    }, "tape-engine should freeze passive-fill batches");

    tape_engine::Client client(socketPath.string());
    tape_engine::QueryResponse response;

    tape_engine::QueryRequest caseRequest;
    caseRequest.requestId = "order-case-phase3-passive-fill";
    caseRequest.operation = "read_order_case";
    caseRequest.orderId = 5501;
    expect(client.query(caseRequest, &response, &error), "phase 3 passive-fill order-case query should succeed: " + error);

    bool sawQueueProxy = false;
    bool sawAdverseSelection = false;
    for (const auto& item : response.summary.value("related_findings", json::array())) {
        sawQueueProxy = sawQueueProxy || item.value("kind", std::string()) == "passive_fill_queue_proxy";
        sawAdverseSelection = sawAdverseSelection || item.value("kind", std::string()) == "post_fill_adverse_selection";
    }
    expect(sawQueueProxy, "order-case related findings should include the passive-fill queue proxy analysis");
    expect(sawAdverseSelection, "order-case related findings should include the post-fill adverse-selection analysis");

    server.stop();
}

void testTapeEnginePhase3BuildsPassiveQueueLossAndCutThroughSignals() {
    const fs::path rootDir = testDataDir() / "tape-engine-phase3-passive-queue-loss";
    const fs::path socketPath = testDataDir() / "tape-engine-phase3-passive-queue-loss.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "ib:conid:9451:STK:SMART:USD:INTC";
    config.ringCapacity = 32;

    tape_engine::Server server(config);
    std::string startError;
    expect(server.start(&startError), "tape-engine should start for passive queue-loss test: " + startError);

    auto marketTick = [](std::uint64_t sourceSeq,
                         int field,
                         double price,
                         const std::string& note) {
        BridgeOutboxRecord record = makeBridgeRecord(sourceSeq, "market_tick", "BrokerMarketData", "INTC",
                                                     field == 1 ? "BID" : (field == 2 ? "ASK" : "LAST"),
                                                     0, 0, 0, "", note, "2026-03-14T09:44:10.000");
        record.instrumentId = "ib:conid:9451:STK:SMART:USD:INTC";
        record.marketField = field;
        record.price = price;
        return record;
    };

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-engine-phase3-passive-queue-loss";
    options.runtimeSessionId = "runtime-engine-phase3-passive-queue-loss";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;

    BridgeOutboxRecord orderIntent = makeBridgeRecord(8601, "order_intent", "WebSocket", "INTC", "BUY",
                                                      761, 5601, 0, "", "passive queue-loss anchor", "2026-03-14T09:44:10.010");
    orderIntent.instrumentId = "ib:conid:9451:STK:SMART:USD:INTC";
    options.batchSeq = 151;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({orderIntent}, options)), &error),
           "tape-engine should accept the passive queue-loss anchor batch: " + error);

    options.batchSeq = 152;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        marketTick(8602, 1, 45.00, "queue-loss bid"),
        marketTick(8603, 2, 45.01, "queue-loss ask")
    }, options)), &error), "tape-engine should accept the passive queue-loss seed batch: " + error);

    options.batchSeq = 153;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        marketTick(8604, 4, 45.00, "queue-loss touch trade one"),
        marketTick(8605, 4, 45.00, "queue-loss touch trade two"),
        marketTick(8606, 1, 44.97, "queue-loss bid lower"),
        marketTick(8607, 2, 44.98, "queue-loss ask lower")
    }, options)), &error), "tape-engine should accept the passive queue-loss follow-through batch: " + error);

    BridgeOutboxRecord cancel = makeBridgeRecord(8608, "cancel_request", "WebSocket", "INTC", "BUY",
                                                 761, 5601, 0, "", "queue-loss cancel", "2026-03-14T09:44:10.060");
    cancel.instrumentId = "ib:conid:9451:STK:SMART:USD:INTC";
    options.batchSeq = 154;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({cancel}, options)), &error),
           "tape-engine should accept the passive queue-loss cancel batch: " + error);

    waitUntil([&]() {
        const auto snapshot = server.snapshot();
        return snapshot.latestFrozenRevisionId >= 4 && snapshot.segments.size() >= 4;
    }, "tape-engine should freeze passive queue-loss batches");

    tape_engine::Client client(socketPath.string());
    tape_engine::QueryResponse response;

    tape_engine::QueryRequest caseRequest;
    caseRequest.requestId = "order-case-phase3-passive-queue-loss";
    caseRequest.operation = "read_order_case";
    caseRequest.orderId = 5601;
    caseRequest.includeLiveTail = true;
    expect(client.query(caseRequest, &response, &error), "phase 3 passive queue-loss order-case query should succeed: " + error);

    bool sawQueueLoss = false;
    bool sawCutThrough = false;
    for (const auto& item : response.summary.value("related_findings", json::array())) {
        const std::string kind = item.value("kind", std::string());
        sawQueueLoss = sawQueueLoss || kind == "passive_queue_loss_proxy";
        sawCutThrough = sawCutThrough || kind == "passive_cut_through_proxy";
    }
    expect(sawQueueLoss, "order-case related findings should include the passive queue-loss proxy");
    expect(sawCutThrough, "order-case related findings should include the passive cut-through proxy");

    server.stop();
}

void testTapeEnginePhase3BuildsSweepAndFadeSignals() {
    auto runCase = [&](const fs::path& rootDir,
                       const fs::path& socketPath,
                       OrderId orderId,
                       std::uint64_t traceId,
                       const std::vector<BridgeOutboxRecord>& records,
                       const std::string& expectedKind,
                       const std::string& label) {
        std::error_code ec;
        fs::remove_all(rootDir, ec);
        fs::remove(socketPath, ec);

        tape_engine::EngineConfig config;
        config.socketPath = socketPath.string();
        config.dataDir = rootDir;
        config.instrumentId = "ib:conid:9461:STK:SMART:USD:INTC";
        config.ringCapacity = 32;

        tape_engine::Server server(config);
        std::string startError;
        expect(server.start(&startError), "tape-engine should start for " + label + ": " + startError);

        bridge_batch::BuildOptions options;
        options.appSessionId = "app-engine-phase3-" + label;
        options.runtimeSessionId = "runtime-engine-phase3-" + label;
        options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

        bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
        std::string error;

        BridgeOutboxRecord orderIntent = makeBridgeRecord(records.front().sourceSeq - 1, "order_intent", "WebSocket", "INTC", "BUY",
                                                          traceId, orderId, 0, "", label + " anchor", "2026-03-14T09:44:20.010");
        orderIntent.instrumentId = "ib:conid:9461:STK:SMART:USD:INTC";
        options.batchSeq = 161;
        expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({orderIntent}, options)), &error),
               "tape-engine should accept the " + label + " anchor batch: " + error);

        options.batchSeq = 162;
        expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch(records, options)), &error),
               "tape-engine should accept the " + label + " market batch: " + error);

        waitUntil([&]() {
            const auto snapshot = server.snapshot();
            return snapshot.latestFrozenRevisionId >= 2 && snapshot.segments.size() >= 2;
        }, "tape-engine should freeze " + label + " batches");

        tape_engine::Client client(socketPath.string());
        tape_engine::QueryResponse response;
        std::string lastSeenKinds;

        waitUntil([&]() {
            tape_engine::QueryRequest pollRequest;
            pollRequest.requestId = "poll-findings-phase3-" + label;
            pollRequest.operation = "list_findings";
            pollRequest.limit = 64;
            pollRequest.includeLiveTail = true;
            if (!client.query(pollRequest, &response, &error)) {
                lastSeenKinds = "query failed: " + error;
                return false;
            }
            std::ostringstream seenKinds;
            for (const auto& item : response.events) {
                const std::string kind = item.value("kind", std::string());
                if (!seenKinds.str().empty()) {
                    seenKinds << ", ";
                }
                seenKinds << kind;
                if (kind == expectedKind) {
                    lastSeenKinds = seenKinds.str();
                    return true;
                }
            }
            lastSeenKinds = seenKinds.str();
            return false;
        }, "phase 3 " + label + " findings should surface " + expectedKind +
           " (saw during polling: " + lastSeenKinds + ")");

        tape_engine::QueryRequest caseRequest;
        caseRequest.requestId = "order-case-phase3-" + label;
        caseRequest.operation = "read_order_case";
        caseRequest.orderId = orderId;
        caseRequest.includeLiveTail = true;
        expect(client.query(caseRequest, &response, &error), "phase 3 " + label + " order-case query should succeed: " + error);

        expect(response.summary.value("related_finding_count", 0ULL) >= 1ULL,
               "order-case should still carry related findings for " + label);

        server.stop();
    };

    auto marketTick = [](std::uint64_t sourceSeq,
                         int field,
                         double price,
                         const std::string& note) {
        BridgeOutboxRecord record = makeBridgeRecord(sourceSeq, "market_tick", "BrokerMarketData", "INTC",
                                                     field == 1 ? "BID" : (field == 2 ? "ASK" : "LAST"),
                                                     0, 0, 0, "", note, "2026-03-14T09:44:20.000");
        record.instrumentId = "ib:conid:9461:STK:SMART:USD:INTC";
        record.marketField = field;
        record.price = price;
        return record;
    };

    runCase(testDataDir() / "tape-engine-phase3-sweep-signal",
            testDataDir() / "tape-engine-phase3-sweep-signal.sock",
            5701,
            771,
            {
                marketTick(8701, 1, 45.10, "sweep bid"),
                marketTick(8702, 2, 45.11, "sweep ask"),
                marketTick(8703, 4, 45.11, "sweep print one"),
                marketTick(8704, 4, 45.11, "sweep print two"),
                marketTick(8705, 2, 45.14, "sweep ask up"),
                marketTick(8706, 4, 45.14, "sweep print three"),
                marketTick(8707, 4, 45.14, "sweep print four"),
                marketTick(8708, 1, 45.13, "sweep bid up")
            },
            "buy_sweep_sequence",
            "sweep-sequence");

    runCase(testDataDir() / "tape-engine-phase3-fade-signal",
            testDataDir() / "tape-engine-phase3-fade-signal.sock",
            5702,
            772,
            {
                marketTick(8711, 1, 45.20, "fade bid"),
                marketTick(8712, 2, 45.21, "fade ask"),
                marketTick(8713, 2, 45.24, "fade ask higher"),
                marketTick(8714, 1, 45.22, "fade bid slightly higher")
            },
            "buy_fade_sequence",
            "fade-sequence");
}

void testTapeEnginePhase3BuildsFillOutcomeChains() {
    const fs::path rootDir = testDataDir() / "tape-engine-phase3-fill-outcome";
    const fs::path socketPath = testDataDir() / "tape-engine-phase3-fill-outcome.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "ib:conid:9471:STK:SMART:USD:INTC";
    config.ringCapacity = 32;

    tape_engine::Server server(config);
    std::string startError;
    expect(server.start(&startError), "tape-engine should start for fill outcome-chain test: " + startError);

    auto marketTick = [](std::uint64_t sourceSeq,
                         int field,
                         double price,
                         const std::string& note) {
        BridgeOutboxRecord record = makeBridgeRecord(sourceSeq, "market_tick", "BrokerMarketData", "INTC",
                                                     field == 1 ? "BID" : (field == 2 ? "ASK" : "LAST"),
                                                     0, 0, 0, "", note, "2026-03-14T09:44:30.000");
        record.instrumentId = "ib:conid:9471:STK:SMART:USD:INTC";
        record.marketField = field;
        record.price = price;
        return record;
    };

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-engine-phase3-fill-outcome";
    options.runtimeSessionId = "runtime-engine-phase3-fill-outcome";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;

    BridgeOutboxRecord orderIntent = makeBridgeRecord(8801, "order_intent", "WebSocket", "INTC", "BUY",
                                                      781, 5801, 0, "", "fill outcome anchor", "2026-03-14T09:44:30.010");
    orderIntent.instrumentId = "ib:conid:9471:STK:SMART:USD:INTC";
    options.batchSeq = 171;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({orderIntent}, options)), &error),
           "tape-engine should accept the fill outcome anchor batch: " + error);

    options.batchSeq = 172;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        marketTick(8802, 1, 45.50, "fill outcome bid"),
        marketTick(8803, 2, 45.51, "fill outcome ask")
    }, options)), &error), "tape-engine should accept the fill outcome seed batch: " + error);

    BridgeOutboxRecord fill = makeBridgeRecord(8804, "fill_execution", "BrokerExecution", "INTC", "BOT",
                                               781, 5801, 0, "exec-5801-a", "fill outcome fill", "2026-03-14T09:44:30.040");
    fill.instrumentId = "ib:conid:9471:STK:SMART:USD:INTC";
    fill.price = 45.51;
    fill.size = 100.0;
    options.batchSeq = 173;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({fill}, options)), &error),
           "tape-engine should accept the fill outcome fill batch: " + error);

    BridgeOutboxRecord cancel = makeBridgeRecord(8805, "cancel_request", "WebSocket", "INTC", "BUY",
                                                 781, 5801, 0, "", "fill outcome cancel", "2026-03-14T09:44:30.050");
    cancel.instrumentId = "ib:conid:9471:STK:SMART:USD:INTC";
    options.batchSeq = 174;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        cancel,
        marketTick(8806, 1, 45.47, "fill outcome bid down"),
        marketTick(8807, 2, 45.49, "fill outcome ask down")
    }, options)), &error), "tape-engine should accept the fill outcome follow-through batch: " + error);

    waitUntil([&]() {
        const auto snapshot = server.snapshot();
        return snapshot.latestFrozenRevisionId >= 4 && snapshot.segments.size() >= 4;
    }, "tape-engine should freeze fill outcome batches");

    tape_engine::Client client(socketPath.string());
    tape_engine::QueryResponse response;

    tape_engine::QueryRequest caseRequest;
    caseRequest.requestId = "order-case-phase3-fill-outcome";
    caseRequest.operation = "read_order_case";
    caseRequest.orderId = 5801;
    caseRequest.includeLiveTail = true;
    expect(client.query(caseRequest, &response, &error), "phase 3 fill outcome order-case query should succeed: " + error);

    bool sawFillToCancel = false;
    bool sawFillToAdverseMove = false;
    for (const auto& item : response.summary.value("related_findings", json::array())) {
        const std::string kind = item.value("kind", std::string());
        sawFillToCancel = sawFillToCancel || kind == "fill_to_cancel_chain";
        sawFillToAdverseMove = sawFillToAdverseMove || kind == "fill_to_adverse_move_chain";
    }
    expect(sawFillToCancel, "order-case related findings should include the fill-to-cancel chain");
    expect(sawFillToAdverseMove, "order-case related findings should include the fill-to-adverse-move chain");

    server.stop();
}

void testTapeEnginePhase3BuildsTradePressureOrderCase() {
    const fs::path rootDir = testDataDir() / "tape-engine-phase3-trade-pressure";
    const fs::path socketPath = testDataDir() / "tape-engine-phase3-trade-pressure.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "ib:conid:9401:STK:SMART:USD:INTC";
    config.ringCapacity = 32;

    tape_engine::Server server(config);
    std::string startError;
    expect(server.start(&startError), "tape-engine should start for trade-pressure order case test: " + startError);

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-engine-phase3-trade-pressure";
    options.runtimeSessionId = "runtime-engine-phase3-trade-pressure";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;

    auto marketTick = [](std::uint64_t sourceSeq,
                         int field,
                         double price,
                         const std::string& note) {
        BridgeOutboxRecord record = makeBridgeRecord(sourceSeq, "market_tick", "BrokerMarketData", "INTC",
                                                     field == 1 ? "BID" : (field == 2 ? "ASK" : "LAST"),
                                                     0, 0, 0, "", note, "2026-03-14T09:44:00.000");
        record.instrumentId = "ib:conid:9401:STK:SMART:USD:INTC";
        record.marketField = field;
        record.price = price;
        return record;
    };

    BridgeOutboxRecord orderIntent = makeBridgeRecord(8401, "order_intent", "WebSocket", "INTC", "BUY",
                                                      611, 5101, 0, "", "trade pressure anchor", "2026-03-14T09:44:00.050");
    orderIntent.instrumentId = "ib:conid:9401:STK:SMART:USD:INTC";
    options.batchSeq = 131;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({orderIntent}, options)), &error),
           "tape-engine should accept the trade-pressure anchor batch: " + error);

    options.batchSeq = 132;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        marketTick(8402, 1, 45.50, "trade pressure bid"),
        marketTick(8403, 2, 45.51, "trade pressure ask")
    }, options)), &error), "tape-engine should accept the trade-pressure seed batch: " + error);

    options.batchSeq = 133;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        marketTick(8404, 4, 45.51, "trade pressure print one"),
        marketTick(8405, 4, 45.51, "trade pressure print two"),
        marketTick(8406, 4, 45.51, "trade pressure print three")
    }, options)), &error), "tape-engine should accept the trade-pressure print batch: " + error);

    BridgeOutboxRecord fill = makeBridgeRecord(8407, "fill_execution", "BrokerExecution", "INTC", "BOT",
                                               611, 5101, 0, "exec-5101-a", "trade pressure fill", "2026-03-14T09:44:00.090");
    fill.instrumentId = "ib:conid:9401:STK:SMART:USD:INTC";
    fill.price = 45.51;
    fill.size = 100.0;

    options.batchSeq = 134;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({fill}, options)), &error),
           "tape-engine should accept the trade-pressure fill batch: " + error);

    waitUntil([&]() {
        const auto snapshot = server.snapshot();
        return snapshot.latestFrozenRevisionId >= 4 && snapshot.segments.size() >= 4;
    }, "tape-engine should freeze trade-pressure batches");

    tape_engine::Client client(socketPath.string());
    tape_engine::QueryResponse response;

    tape_engine::QueryRequest findingsRequest;
    findingsRequest.requestId = "findings-phase3-trade-pressure";
    findingsRequest.operation = "list_findings";
    expect(client.query(findingsRequest, &response, &error), "phase 3 trade-pressure findings query should succeed: " + error);
    bool sawTradePressureFinding = false;
    for (const auto& item : response.events) {
        sawTradePressureFinding = sawTradePressureFinding || item.value("kind", std::string()) == "buy_trade_pressure";
    }
    expect(sawTradePressureFinding, "phase 3 should detect buy-side trade pressure at the ask");

    tape_engine::QueryRequest caseRequest;
    caseRequest.requestId = "order-case-phase3-trade-pressure";
    caseRequest.operation = "read_order_case";
    caseRequest.orderId = 5101;
    expect(client.query(caseRequest, &response, &error), "phase 3 order-case query should succeed: " + error);
    expect(response.summary.value("replay_target_session_seq", 0ULL) == response.summary.value("last_fill_session_seq", 0ULL),
           "order-case replay target should prefer the latest fill session seq");
    expect(response.summary.value("related_finding_count", 0ULL) >= 2ULL,
           "order-case summary should include related findings from the protected anchor window");
    expect(response.summary.value("related_incident_count", 0ULL) >= 1ULL,
           "order-case summary should include at least one related incident");
    expect(response.summary.contains("protected_window"), "order-case summary should surface the selected protected window");
    expect(response.summary.value("protected_window_event_count", 0ULL) >= 4ULL,
           "order-case summary should include protected-window evidence beyond the anchor-matched lifecycle events");
    expect(response.summary.contains("data_quality"), "order-case summary should include data-quality scoring");
    expect(response.summary.contains("timeline"), "order-case summary should include a merged case timeline");
    expect(response.summary.contains("timeline_summary"), "order-case summary should summarize the merged case timeline");
    expect(response.summary.value("timeline_summary", json::object()).value("market_event_count", 0ULL) >= 2ULL,
           "order-case timeline summary should count surrounding market evidence");
    expect(response.summary.contains("case_report"), "order-case summary should include a report-style case summary");
    expect(response.summary.value("case_report", json::object()).contains("timeline_highlights"),
           "order-case case report should include timeline highlights");

    bool sawTradePressureRelatedFinding = false;
    bool sawOrderFillContext = false;
    bool sawOrderFillContextRichSummary = false;
    for (const auto& item : response.summary.value("related_findings", json::array())) {
        sawTradePressureRelatedFinding = sawTradePressureRelatedFinding || item.value("kind", std::string()) == "buy_trade_pressure";
        if (item.value("kind", std::string()) == "order_fill_context") {
            sawOrderFillContext = true;
            const std::string summary = item.value("summary", std::string());
            sawOrderFillContextRichSummary =
                sawOrderFillContextRichSummary ||
                summary.find("adverse excursion") != std::string::npos ||
                summary.find("mid ") != std::string::npos ||
                summary.find("bid range ") != std::string::npos;
        }
    }
    expect(sawTradePressureRelatedFinding, "order-case related findings should include the trade-pressure signal");
    expect(sawOrderFillContext, "order-case related findings should include the deferred order-fill context analysis");
    expect(sawOrderFillContextRichSummary,
           "order-case related findings should surface the richer fill-context summary");

    bool sawTradePressureIncident = false;
    for (const auto& item : response.summary.value("related_incidents", json::array())) {
        sawTradePressureIncident = sawTradePressureIncident || item.value("kind", std::string()) == "buy_trade_pressure";
    }
    expect(sawTradePressureIncident, "order-case related incidents should include the trade-pressure incident");

    bool sawOrderIntent = false;
    bool sawFill = false;
    for (const auto& item : response.events) {
        const std::string kind = item.value("event_kind", std::string());
        sawOrderIntent = sawOrderIntent || kind == "order_intent";
        sawFill = sawFill || kind == "fill_execution";
    }
    expect(sawOrderIntent, "order-case evidence should include the order intent");
    expect(sawFill, "order-case evidence should include the fill execution");

    bool sawTimelineMarketTick = false;
    bool sawTimelineIncident = false;
    for (const auto& item : response.summary.value("timeline", json::array())) {
        const std::string entryType = item.value("entry_type", std::string());
        const std::string kind = item.value("kind", std::string());
        sawTimelineMarketTick = sawTimelineMarketTick || (entryType == "event" && kind == "market_tick");
        sawTimelineIncident = sawTimelineIncident || (entryType == "incident" && kind == "buy_trade_pressure");
    }
    expect(sawTimelineMarketTick, "order-case timeline should include protected-window market evidence");
    expect(sawTimelineIncident, "order-case timeline should include incident entries");

    server.stop();
}

void testTapeEngineResetMarkerPreservesCanonicalInstrumentIdentity() {
    const fs::path rootDir = testDataDir() / "tape-engine-reset-marker";
    const fs::path socketPath = testDataDir() / "tape-engine-reset-marker.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "ib:conid:7501:STK:SMART:USD:INTC";
    config.ringCapacity = 32;
    config.dedupeWindowSize = 4;

    tape_engine::Server server(config);
    std::string startError;
    expect(server.start(&startError), "tape-engine should start for reset marker test: " + startError);

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-engine-reset-marker";
    options.runtimeSessionId = "runtime-engine-reset-marker";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;

    BridgeOutboxRecord firstRecord = makeBridgeRecord(6002, "order_intent", "WebSocket", "INTC", "BUY",
                                                      111, 901, 0, "", "initial event", "2026-03-14T09:38:45.100");
    firstRecord.instrumentId = "ib:conid:7501:STK:SMART:USD:INTC";
    options.batchSeq = 81;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({firstRecord}, options)), &error),
           "tape-engine should accept the first reset test batch: " + error);

    BridgeOutboxRecord secondRecord = makeBridgeRecord(6001, "order_status", "BrokerOrderStatus", "INTC", "BUY",
                                                       111, 901, 0, "", "out-of-order reset trigger", "2026-03-14T09:38:45.200");
    secondRecord.instrumentId = "ib:conid:7501:STK:SMART:USD:INTC";
    options.batchSeq = 82;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({secondRecord}, options)), &error),
           "tape-engine should accept the out-of-order batch and emit a reset marker: " + error);

    const tape_engine::EngineSnapshot snapshot = server.snapshot();
    expect(snapshot.liveEvents.size() == 3, "reset flow should keep the original event, reset marker, and replacement event in the live ring");
    expect(snapshot.liveEvents.at(1).eventKind == "reset_marker", "out-of-order source_seq should emit an explicit reset marker");
    expect(snapshot.liveEvents.at(1).resetPreviousSourceSeq == 6002, "reset marker should preserve the previous accepted source_seq");
    expect(snapshot.liveEvents.at(1).resetSourceSeq == 6001, "reset marker should preserve the new reset source_seq");
    expect(snapshot.liveEvents.at(1).instrumentId == "ib:conid:7501:STK:SMART:USD:INTC", "reset marker should keep the canonical instrument identity");
    expect(snapshot.liveEvents.at(2).instrumentId == "ib:conid:7501:STK:SMART:USD:INTC", "accepted replacement events should keep the canonical instrument identity");

    server.stop();
}

void testTapeEngineReplaySnapshotRebuildsFrozenMarketState() {
    const fs::path rootDir = testDataDir() / "tape-engine-replay";
    const fs::path socketPath = testDataDir() / "tape-engine-replay.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "INTC";
    config.ringCapacity = 32;

    tape_engine::Server server(config);
    std::string startError;
    expect(server.start(&startError), "tape-engine should start for replay snapshot test: " + startError);

    auto marketRecord = [](std::uint64_t sourceSeq,
                           const std::string& recordType,
                           const std::string& side,
                           int marketField,
                           int bookPosition,
                           int bookOperation,
                           int bookSide,
                           double price,
                           double size,
                           const std::string& note) {
        BridgeOutboxRecord record = makeBridgeRecord(sourceSeq, recordType, "BrokerMarketData", "INTC", side,
                                                     0, 0, 0, "", note, "2026-03-14T09:39:00.000");
        record.marketField = marketField;
        record.bookPosition = bookPosition;
        record.bookOperation = bookOperation;
        record.bookSide = bookSide;
        record.price = price;
        record.size = size;
        return record;
    };

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-engine-replay";
    options.runtimeSessionId = "runtime-engine-replay";
    options.flushReason = bridge_batch::FlushReason::ThresholdRecords;
    options.batchSeq = 61;

    const bridge_batch::Batch batch = bridge_batch::buildBatch({
        marketRecord(4001, "market_tick", "BID", 1, -1, -1, -1, 45.10, std::numeric_limits<double>::quiet_NaN(), "BID tick 45.10"),
        marketRecord(4002, "market_tick", "ASK", 2, -1, -1, -1, 45.12, std::numeric_limits<double>::quiet_NaN(), "ASK tick 45.12"),
        marketRecord(4003, "market_depth", "ASK", -1, 0, 0, 0, 45.12, 200.0, "ASK depth insert"),
        marketRecord(4004, "market_depth", "BID", -1, 0, 0, 1, 45.10, 300.0, "BID depth insert"),
        marketRecord(4005, "market_tick", "LAST", 4, -1, -1, -1, 45.11, std::numeric_limits<double>::quiet_NaN(), "LAST tick 45.11"),
        marketRecord(4006, "market_depth", "ASK", -1, 0, 1, 0, 45.13, 150.0, "ASK depth update"),
        marketRecord(4007, "market_depth", "BID", -1, 1, 0, 1, 45.09, 500.0, "BID depth insert level 1")
    }, options);

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;
    expect(transport.sendFrame(bridge_batch::encodeFrame(batch), &error),
           "tape-engine should accept replay market data batch: " + error);

    waitUntil([&]() {
        const auto snapshot = server.snapshot();
        return snapshot.segments.size() >= 1 && snapshot.latestFrozenRevisionId >= 1;
    }, "tape-engine should freeze replay market data before rebuilding a frozen snapshot");

    tape_engine::Client client(socketPath.string());
    tape_engine::QueryResponse response;

    tape_engine::QueryRequest replayRequest;
    replayRequest.requestId = "replay-1";
    replayRequest.operation = "replay_snapshot";
    replayRequest.targetSessionSeq = 7;
    replayRequest.limit = 2;
    expect(client.query(replayRequest, &response, &error), "tape-engine replay snapshot query should succeed: " + error);

    expect(response.summary.value("served_revision_id", 0ULL) == 1, "replay snapshot should identify the frozen revision used");
    expect(response.summary.value("target_session_seq", 0ULL) == 7, "replay snapshot should preserve the requested session_seq");
    expect(response.summary.value("replayed_through_session_seq", 0ULL) == 7, "replay snapshot should rebuild through the target session_seq");
    expect(response.summary.value("checkpoint_used", false), "replay snapshot should resume from a persisted checkpoint when one is available");
    expect(response.summary.value("checkpoint_session_seq", 0ULL) == 7, "replay snapshot should report the checkpoint session_seq it resumed from");
    expect(response.summary.value("checkpoint_revision_id", 0ULL) == 1, "replay snapshot should report the checkpoint revision it resumed from");
    expect(response.summary.value("applied_event_count", 0ULL) == 7, "replay snapshot should count the applied market events");
    expect(std::fabs(response.summary.value("bid_price", 0.0) - 45.10) < 0.0001, "replay snapshot should rebuild the inside bid");
    expect(std::fabs(response.summary.value("ask_price", 0.0) - 45.13) < 0.0001, "replay snapshot should rebuild the inside ask from depth updates");
    expect(std::fabs(response.summary.value("last_price", 0.0) - 45.11) < 0.0001, "replay snapshot should rebuild the last trade price");
    expect(response.summary.contains("bid_book") && response.summary["bid_book"].is_array() &&
           response.summary["bid_book"].size() == 2, "replay snapshot should return the requested bid book depth");
    expect(response.summary.contains("ask_book") && response.summary["ask_book"].is_array() &&
           response.summary["ask_book"].size() == 1, "replay snapshot should return the rebuilt ask book");
    expect(std::fabs(response.summary["bid_book"].at(1).value("price", 0.0) - 45.09) < 0.0001,
           "replay snapshot should preserve deeper bid levels");
    expect(std::fabs(response.summary["ask_book"].at(0).value("size", 0.0) - 150.0) < 0.0001,
           "replay snapshot should preserve updated ask size");
    const tape_engine::EngineSnapshot replaySnapshot = server.snapshot();
    expect(fs::exists(rootDir / "segments" / replaySnapshot.segments.front().checkpointFileName),
           "replay snapshot test should have a persisted checkpoint file to reuse");

    server.stop();
}

void testBridgeMarketDataEmissionExpandsPublicEvents() {
    clearTestFiles();

    SharedData owner;
    bindSharedDataOwner(&owner);

    trading_engine::reduce(appState(), trading_engine::MarketSubscriptionStartedEvent{
        "INTC",
        "ib:conid:9101:STK:SMART:USD:INTC",
        9101,
        9201,
        false
    });
    trading_engine::reduce(appState(), trading_engine::BrokerTickPriceEvent{9101, static_cast<TickType>(1), 45.10});
    trading_engine::reduce(appState(), trading_engine::BrokerTickPriceEvent{9101, static_cast<TickType>(2), 45.12});
    trading_engine::reduce(appState(), trading_engine::BrokerTickPriceEvent{9101, static_cast<TickType>(4), 45.11});
    trading_engine::reduce(appState(), trading_engine::BrokerMarketDepthEvent{9201, 0, 0, 0, 45.12, 200.0});
    trading_engine::reduce(appState(), trading_engine::BrokerMarketDepthEvent{9201, 0, 0, 1, 45.10, 300.0});
    publishSharedDataSnapshot();

    const BridgeDispatchSnapshot dispatch = captureBridgeDispatchSnapshot(10);
    expect(dispatch.records.size() >= 5, "public market capture should enqueue quote and depth bridge records");

    const auto bidTick = std::find_if(dispatch.records.begin(), dispatch.records.end(), [](const BridgeOutboxRecord& record) {
        return record.recordType == "market_tick" && record.marketField == 1;
    });
    expect(bidTick != dispatch.records.end(), "bridge outbox should include a bid market_tick record");
    expect(bidTick->instrumentId == "ib:conid:9101:STK:SMART:USD:INTC", "market tick records should preserve the canonical instrument identity");
    expect(bidTick->side == "BID", "market tick record should preserve the bid side label");
    expect(std::fabs(bidTick->price - 45.10) < 0.0001, "market tick record should preserve the quote price");

    const auto askDepth = std::find_if(dispatch.records.begin(), dispatch.records.end(), [](const BridgeOutboxRecord& record) {
        return record.recordType == "market_depth" && record.bookSide == 0;
    });
    expect(askDepth != dispatch.records.end(), "bridge outbox should include ask-side market_depth records");
    expect(askDepth->bookOperation == 0 && askDepth->bookPosition == 0, "market depth record should preserve position and operation");
    expect(std::fabs(askDepth->size - 200.0) < 0.0001, "market depth record should preserve size");

    unbindSharedDataOwner(&owner);
    resetSharedDataForTesting();
}

void testGeneratedRuntimeRegistryMatchesPhase15QueueSpec() {
    expect(runtime_registry::kRegistryVersion == 1, "generated runtime registry should expose version 1");

    const auto bridgeSender = runtime_registry::queueSpec(runtime_registry::QueueId::BridgeSender);
    expect(bridgeSender.label == "com.foxy.long.bridge.sender", "generated runtime registry should preserve the bridge sender label");
    expect(bridgeSender.qosName == "userInitiated", "generated runtime registry should preserve bridge sender QoS");
    expect(runtime_registry::logCategoryName(bridgeSender.category) == "bridge", "generated runtime registry should preserve bridge sender category");

    const auto engineWriter = runtime_registry::queueSpec(runtime_registry::QueueId::EngineSegmentWriter);
    expect(engineWriter.label == "com.foxy.tape-engine.segment-writer", "generated runtime registry should preserve the engine writer label");
    expect(engineWriter.qosName == "utility", "generated runtime registry should preserve engine writer QoS");
    expect(runtime_registry::subsystemName(engineWriter.subsystem) == "com.foxy.tape-engine", "generated runtime registry should preserve the engine subsystem name");

    const auto tapescopeMain = runtime_registry::queueSpec(runtime_registry::QueueId::TapescopeMainUi);
    expect(tapescopeMain.label == "com.foxy.tapescope.main", "generated runtime registry should include the TapeScope main UI queue");
    expect(tapescopeMain.qosName == "userInteractive", "generated runtime registry should preserve TapeScope UI QoS");

    const auto tapeMcpReads = runtime_registry::queueSpec(runtime_registry::QueueId::TapeMcpReads);
    expect(tapeMcpReads.label == "com.foxy.tape-mcp.reads", "generated runtime registry should include the MCP read worker queue");
    expect(runtime_registry::subsystemName(tapeMcpReads.subsystem) == "com.foxy.tape-mcp", "generated runtime registry should preserve the MCP subsystem name");
}

void testBridgeLifecycleEmissionExpandsPrivateOrderEvents() {
    clearTestFiles();

    SharedData owner;
    bindSharedDataOwner(&owner);

    SubmitIntent intent;
    intent.source = "WebSocket";
    intent.symbol = "INTC";
    intent.side = "BUY";
    intent.requestedQty = 1;
    intent.limitPrice = 45.70;
    intent.notes = "accepted order intent";

    const std::uint64_t traceId = beginTradeTrace(intent);
    bindTraceToOrder(traceId, 701);
    bindTraceToPermId(701, 9801);

    Contract contract;
    contract.symbol = "INTC";

    Order order;
    order.orderId = 701;
    order.permId = 9801;
    order.action = "BUY";
    order.totalQuantity = DecimalFunctions::doubleToDecimal(1.0);
    order.lmtPrice = 45.70;
    order.account = "U23154741";

    OrderState orderState;
    orderState.status = "Submitted";
    recordTraceOpenOrder(701, contract, order, orderState);
    recordTraceOrderStatus(701, "Submitted", 0.0, 1.0, 0.0, 9801, 0.0, 0.0);

    Execution execution;
    execution.orderId = 701;
    execution.permId = 9801;
    execution.execId = "EXEC-91";
    execution.side = "BOT";
    execution.shares = DecimalFunctions::doubleToDecimal(1.0);
    execution.price = 45.70;
    execution.exchange = "SMART";
    execution.cumQty = DecimalFunctions::doubleToDecimal(1.0);
    execution.avgPrice = 45.70;
    execution.time = "20260314 09:38:00";
    recordTraceExecution(contract, execution);

    CommissionReport commission;
    commission.execId = "EXEC-91";
    commission.commission = 0.45;
    commission.currency = "USD";
    recordTraceCommission(commission);

    recordTraceCancelRequest(701);
    recordTraceError(701, 399, "warning path");
    recordTraceError(701, 201, "rejected path");

    const BridgeDispatchSnapshot dispatch = captureBridgeDispatchSnapshot(20);
    expect(dispatch.records.size() >= 7, "expanded lifecycle emission should enqueue the widened private-event set");

    std::vector<std::string> recordTypes;
    recordTypes.reserve(dispatch.records.size());
    for (const auto& record : dispatch.records) {
        recordTypes.push_back(record.recordType);
    }

    expect(std::find(recordTypes.begin(), recordTypes.end(), "open_order") != recordTypes.end(),
           "bridge outbox should include open_order records");
    expect(std::find(recordTypes.begin(), recordTypes.end(), "order_status") != recordTypes.end(),
           "bridge outbox should include order_status records");
    expect(std::find(recordTypes.begin(), recordTypes.end(), "fill_execution") != recordTypes.end(),
           "bridge outbox should include fill_execution records");
    expect(std::find(recordTypes.begin(), recordTypes.end(), "commission_report") != recordTypes.end(),
           "bridge outbox should include commission_report records");
    expect(std::find(recordTypes.begin(), recordTypes.end(), "cancel_request") != recordTypes.end(),
           "bridge outbox should include cancel_request records");
    expect(std::find(recordTypes.begin(), recordTypes.end(), "broker_error") != recordTypes.end(),
           "bridge outbox should include general broker_error records");
    expect(std::find(recordTypes.begin(), recordTypes.end(), "order_reject") != recordTypes.end(),
           "bridge outbox should include rejection-specific order_reject records");

    const auto fillRecord = std::find_if(dispatch.records.begin(), dispatch.records.end(), [](const BridgeOutboxRecord& record) {
        return record.recordType == "fill_execution";
    });
    expect(fillRecord != dispatch.records.end(), "expanded lifecycle emission should include a fill_execution record to inspect");
    expect(fillRecord->instrumentId == "ib:STK:SMART:USD:INTC", "lifecycle bridge records should preserve canonical instrument identity even before a live conId arrives");

    unbindSharedDataOwner(&owner);
    resetSharedDataForTesting();
}

void testBridgeOutboxOverflowWritesExplicitLossMarker() {
    clearTestFiles();

    SharedData owner;
    bindSharedDataOwner(&owner);

    BridgeOutboxRecordInput firstRecord;
    firstRecord.recordType = "fill_execution";
    firstRecord.source = "BrokerExecution";
    firstRecord.symbol = "INTC";
    firstRecord.side = "BOT";
    firstRecord.traceId = 301;
    firstRecord.orderId = 901;
    firstRecord.permId = 9901;
    firstRecord.execId = "EXEC-DROP";
    firstRecord.note = "first record should become the explicit loss marker";
    const BridgeOutboxEnqueueResult first = enqueueBridgeOutboxRecord(firstRecord);

    BridgeOutboxSnapshot snapshot = captureBridgeOutboxSnapshot(1);
    int enqueued = 1;
    while (snapshot.lossCount == 0 && enqueued < 2000) {
        BridgeOutboxRecordInput record;
        record.recordType = "fill_execution";
        record.source = "BrokerExecution";
        record.symbol = "INTC";
        record.side = "BOT";
        record.traceId = 301 + enqueued;
        record.orderId = 901 + enqueued;
        record.permId = 9901 + enqueued;
        record.execId = "EXEC-" + std::to_string(enqueued);
        record.note = "overflow probe";
        enqueueBridgeOutboxRecord(record);
        ++enqueued;
        snapshot = captureBridgeOutboxSnapshot(1);
    }

    expect(snapshot.lossCount > 0, "bridge overflow should surface an explicit continuity-loss count");
    expect(snapshot.recoveryRequired, "bridge overflow should keep recovery explicitly required");
    expect(snapshot.pendingCount < enqueued, "bridge overflow should not silently pretend all queued records are still present");

    const std::vector<json> journalLines = readJsonLines(runtimeJournalLogPath());
    const auto lossIt = std::find_if(journalLines.begin(),
                                     journalLines.end(),
                                     [](const json& line) {
                                         return line.value("event", std::string()) == "bridge_outbox_loss";
                                     });
    expect(lossIt != journalLines.end(), "bridge overflow should write a bridge_outbox_loss journal event");
    const json details = lossIt->contains("details") && (*lossIt)["details"].is_object()
        ? (*lossIt)["details"]
        : json::object();
    expect(details.value("reason", std::string()) == "queue_overflow", "loss marker should explain the continuity break");
    expect(details.value("droppedSourceSeq", 0ULL) == first.sourceSeq, "loss marker should identify the dropped source_seq");
    expect(details.value("traceId", 0ULL) == firstRecord.traceId, "loss marker should preserve dropped traceId");
    expect(details.value("orderId", 0LL) == static_cast<long long>(firstRecord.orderId), "loss marker should preserve dropped orderId");
    expect(details.value("permId", 0LL) == firstRecord.permId, "loss marker should preserve dropped permId");
    expect(details.value("execId", std::string()) == firstRecord.execId, "loss marker should preserve dropped execId");

    unbindSharedDataOwner(&owner);
    resetSharedDataForTesting();
}

void testRecoverySnapshotReportsBridgeContinuityLossAfterAbnormalShutdown() {
    clearTestFiles();

    appendJournalLine(json{
        {"event", "runtime_start"},
        {"wallTime", "2026-03-13T09:30:00.000"},
        {"appSessionId", "app-bridge-recovery"},
        {"runtimeSessionId", "runtime-bridge-recovery"}
    });
    appendJournalLine(json{
        {"event", "bridge_outbox_queued"},
        {"wallTime", "2026-03-13T09:30:01.000"},
        {"appSessionId", "app-bridge-recovery"},
        {"runtimeSessionId", "runtime-bridge-recovery"},
        {"details", json{{"sourceSeq", 41ULL}, {"traceId", 81ULL}, {"orderId", 601LL}}}
    });
    appendJournalLine(json{
        {"event", "bridge_outbox_queued"},
        {"wallTime", "2026-03-13T09:30:02.000"},
        {"appSessionId", "app-bridge-recovery"},
        {"runtimeSessionId", "runtime-bridge-recovery"},
        {"details", json{{"sourceSeq", 42ULL}, {"traceId", 81ULL}, {"orderId", 601LL}, {"permId", 9601LL}, {"execId", "EXEC-81"}}}
    });
    appendJournalLine(json{
        {"event", "bridge_outbox_queued"},
        {"wallTime", "2026-03-13T09:30:02.500"},
        {"appSessionId", "app-bridge-recovery"},
        {"runtimeSessionId", "runtime-bridge-recovery"},
        {"details", json{{"sourceSeq", 43ULL}, {"traceId", 82ULL}, {"orderId", 602LL}, {"permId", 9602LL}, {"execId", "EXEC-82"}}}
    });
    appendJournalLine(json{
        {"event", "bridge_outbox_delivered"},
        {"wallTime", "2026-03-13T09:30:03.000"},
        {"appSessionId", "app-bridge-recovery"},
        {"runtimeSessionId", "runtime-bridge-recovery"},
        {"details", json{{"sourceSeq", 41ULL}}}
    });
    appendJournalLine(json{
        {"event", "bridge_outbox_loss"},
        {"wallTime", "2026-03-13T09:30:04.000"},
        {"appSessionId", "app-bridge-recovery"},
        {"runtimeSessionId", "runtime-bridge-recovery"},
        {"details", json{{"reason", "queue_overflow"}, {"droppedSourceSeq", 42ULL}, {"traceId", 81ULL}, {"orderId", 601LL}, {"permId", 9601LL}, {"execId", "EXEC-81"}}}
    });

    const RuntimeRecoverySnapshot snapshot = recoverRuntimeRecoverySnapshot(5);
    expect(snapshot.priorSessionAbnormal, "bridge recovery snapshot should flag abnormal prior shutdown");
    expect(snapshot.priorAppSessionId == "app-bridge-recovery", "bridge recovery snapshot should preserve prior app session id");
    expect(snapshot.priorRuntimeSessionId == "runtime-bridge-recovery", "bridge recovery snapshot should preserve prior runtime session id");
    expect(snapshot.pendingOutboxCount == 1, "bridge recovery snapshot should exclude explicitly lost queued records from pending recovery work");
    expect(snapshot.outboxLossCount == 1, "bridge recovery snapshot should count explicit continuity-loss markers");
    expect(snapshot.lastOutboxSourceSeq == 43, "bridge recovery snapshot should preserve the highest accepted source_seq");
    expect(snapshot.bridgeRecoveryRequired, "bridge recovery snapshot should require recovery when continuity was lost");
    expectContains(snapshot.bannerText, "Previous session ended unexpectedly", "bridge recovery banner should mention abnormal shutdown");
    expectContains(snapshot.bannerText, "Bridge recovery pending: 1 queued intent", "bridge recovery banner should mention pending bridge work");
    expectContains(snapshot.bannerText, "1 loss marker", "bridge recovery banner should mention explicit continuity loss");
    expectContains(snapshot.bannerText, "last source_seq=43", "bridge recovery banner should report the last accepted source_seq");
}

void testTradingWrapperSessionReadyAndReconnect() {
    clearTestFiles();

    SharedData owner;
    bindSharedDataOwner(&owner);

    TradingWrapper wrapper;
    wrapper.connectAck();
    wrapper.managedAccounts("U23154741,U23164862");
    wrapper.nextValidId(113);

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        expect(g_data.connected, "wrapper should mark session connected after connectAck/nextValidId");
        expect(!g_data.sessionReady, "session should not be ready until reconciliation completes");
        expect(g_data.sessionState == RuntimeSessionState::Reconciling, "session should enter reconciling after nextValidId");
        expect(g_data.selectedAccount == "U23154741", "managedAccounts should select configured account");
        expect(g_data.nextOrderId.load(std::memory_order_relaxed) == 113, "nextValidId should update next order id");
    }

    wrapper.positionEnd();
    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        expect(g_data.positionsLoaded, "positionEnd should mark positions loaded");
        expect(!g_data.sessionReady, "session should wait for executions before becoming ready");
    }

    wrapper.execDetailsEnd(1);
    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        expect(g_data.executionsLoaded, "execDetailsEnd should mark executions loaded");
        expect(g_data.sessionReady, "session should become ready after positions and executions load");
        expect(g_data.sessionState == RuntimeSessionState::SessionReady, "session should transition to ready after reconciliation");
        expect(!g_data.messages.empty(), "wrapper should have appended status messages");
    }

    wrapper.connectionClosed();
    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        expect(!g_data.connected, "connectionClosed should clear connected state");
        expect(!g_data.sessionReady, "connectionClosed should clear ready state");
        expect(g_data.sessionState == RuntimeSessionState::Disconnected, "connectionClosed should return session to disconnected");
        expect(!g_data.positionsLoaded, "connectionClosed should clear reconciliation flags");
        expect(!g_data.executionsLoaded, "connectionClosed should clear reconciliation flags");
        expect(g_data.bidPrice == 0.0 && g_data.askPrice == 0.0 && g_data.lastPrice == 0.0,
               "connectionClosed should clear quote state");
    }

    unbindSharedDataOwner(&owner);
    resetSharedDataForTesting();
}

void testTradingWrapperIgnoresDuplicateOrderStatus() {
    clearTestFiles();

    SharedData owner;
    bindSharedDataOwner(&owner);

    TradingWrapper wrapper;
#if defined(TWS_ORDER_STATUS_PERMID_IS_INT)
    const int permId = 7001;
#else
    const long long permId = 7001;
#endif
    const Decimal zero = DecimalFunctions::doubleToDecimal(0.0);
    const Decimal one = DecimalFunctions::doubleToDecimal(1.0);
    wrapper.orderStatus(113, "Submitted", zero, one, 0.0, permId, 0, 0.0, 0, "", 0.0);

    std::size_t firstMessageCount = 0;
    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        const auto orderIt = g_data.orders.find(113);
        expect(orderIt != g_data.orders.end(), "first orderStatus should create order state");
        expect(orderIt->second.status == "Submitted", "first orderStatus should record status");
        firstMessageCount = g_data.messages.size();
    }

    wrapper.orderStatus(113, "Submitted", zero, one, 0.0, permId, 0, 0.0, 0, "", 0.0);

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        expect(g_data.messages.size() == firstMessageCount, "duplicate orderStatus should not add a second message");
        const auto orderIt = g_data.orders.find(113);
        expect(orderIt != g_data.orders.end(), "duplicate orderStatus should preserve order state");
        expect(orderIt->second.remainingQty == 1.0, "duplicate orderStatus should preserve remaining quantity");
    }

    unbindSharedDataOwner(&owner);
    resetSharedDataForTesting();
}

void testRuntimePresentationSnapshotCapturesConsistentState() {
    clearTestFiles();

    SharedData owner;
    bindSharedDataOwner(&owner);

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        g_data.connected = true;
        g_data.sessionReady = true;
        g_data.sessionState = RuntimeSessionState::SessionReady;
        g_data.selectedAccount = "U23154741";
        g_data.twsHost = "127.0.0.1";
        g_data.twsPort = 7496;
        g_data.twsClientId = 7;
        g_data.websocketAuthToken = "token-123";
        g_data.websocketEnabled = true;
        g_data.controllerEnabled = true;
        g_data.wsServerRunning.store(true, std::memory_order_relaxed);
        g_data.wsConnectedClients.store(2, std::memory_order_relaxed);
        g_data.controllerConnected.store(true, std::memory_order_relaxed);
        g_data.controllerDeviceName = "DualSense";
        g_data.controllerLockedDeviceName = "DualSense";
        g_data.currentSymbol = "INTC";
        g_data.activeMktDataReqId = 101;
        g_data.activeDepthReqId = 202;
        g_data.currentQuantity = 9;
        g_data.priceBuffer = 0.05;
        g_data.maxPositionDollars = 42000.0;
        g_data.bidPrice = 45.64;
        g_data.askPrice = 45.65;
        g_data.lastPrice = 45.645;
        g_data.lastQuoteUpdate = std::chrono::steady_clock::now();
        g_data.maxOrderNotional = 25000.0;
        g_data.maxOpenNotional = 60000.0;
        g_data.staleQuoteThresholdMs = 1500;
        g_data.controllerArmMode = ControllerArmMode::Manual;
        g_data.controllerArmed = true;
        g_data.tradingKillSwitch = false;

        OrderInfo order;
        order.orderId = 200;
        order.account = "U23154741";
        order.symbol = "INTC";
        order.side = "BUY";
        order.quantity = 2.0;
        order.remainingQty = 2.0;
        order.limitPrice = 45.65;
        order.status = "Submitted";
        g_data.orders[200] = order;

        PositionInfo position;
        position.account = "U23154741";
        position.symbol = "INTC";
        position.quantity = 5.0;
        position.avgCost = 45.10;
        g_data.positions[makePositionKey("U23154741", "INTC")] = position;

        TradeTrace trace;
        trace.traceId = 44;
        trace.orderId = 200;
        trace.source = "Controller";
        trace.symbol = "INTC";
        trace.side = "BUY";
        trace.requestedQty = 2;
        trace.limitPrice = 45.65;
        trace.latestStatus = "Submitted";
        g_data.traces[44] = trace;
        g_data.traceRecency.push_back(44);
        g_data.latestTraceId = 44;

        g_data.messages.push_back("hello snapshot");
        ++g_data.messagesVersion;
    }
    publishSharedDataSnapshot();

    const RuntimePresentationSnapshot snapshot = captureRuntimePresentationSnapshot("INTC", 10);
    expect(snapshot.status.connected, "presentation snapshot should include connection status");
    expect(snapshot.status.accountText == "U23154741", "presentation snapshot should include selected account");
    expect(snapshot.connection.clientId == 7, "presentation snapshot should include runtime connection");
    expect(snapshot.activeSymbol == "INTC", "presentation snapshot should include active symbol");
    expect(snapshot.subscriptionActive, "presentation snapshot should include subscription state");
    expect(snapshot.currentQuantity == 9, "presentation snapshot should include current quantity");
    expect(snapshot.priceBuffer == 0.05, "presentation snapshot should include price buffer");
    expect(snapshot.maxPositionDollars == 42000.0, "presentation snapshot should include max position");
    expect(snapshot.symbol.hasPosition, "presentation snapshot should include current position");
    expect(snapshot.symbol.availableLongToClose == 5.0, "presentation snapshot should compute closeable shares");
    expect(snapshot.risk.controllerArmed, "presentation snapshot should include risk state");
    expect(snapshot.orders.size() == 1, "presentation snapshot should include orders");
    expect(snapshot.traceItems.size() == 1, "presentation snapshot should include trace list");
    expect(snapshot.latestTraceId == 44, "presentation snapshot should include latest trace id");
    expectContains(snapshot.messagesText, "hello snapshot", "presentation snapshot should include cached messages");

    unbindSharedDataOwner(&owner);
    resetSharedDataForTesting();
}

void testPendingUiSyncUpdateConsumesFlags() {
    clearTestFiles();

    SharedData owner;
    bindSharedDataOwner(&owner);

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        g_data.hasPendingSubscribe = true;
        g_data.pendingSubscribeSymbol = "QBTS";
        g_data.wsQuantityUpdated = true;
        g_data.currentQuantity = 17;
    }
    publishSharedDataSnapshot();

    const PendingUiSyncUpdate update = consumePendingUiSyncUpdate();
    expect(update.hasPendingSubscribe, "pending subscribe should be surfaced");
    expect(update.quantityUpdated, "pending quantity update should be surfaced");
    expect(update.pendingSubscribeSymbol == "QBTS", "pending subscribe symbol should be preserved");
    expect(update.quantityInput == 17, "pending quantity should be preserved");

    const PendingUiSyncUpdate secondUpdate = consumePendingUiSyncUpdate();
    expect(!secondUpdate.hasPendingSubscribe, "pending subscribe should be consumed");
    expect(!secondUpdate.quantityUpdated, "pending quantity update should be consumed");

    unbindSharedDataOwner(&owner);
    resetSharedDataForTesting();
}

void testRuntimePresentationSnapshotTracksQuoteFreshnessAndCancelMarking() {
    clearTestFiles();

    SharedData owner;
    bindSharedDataOwner(&owner);

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        g_data.connected = true;
        g_data.sessionReady = true;
        g_data.selectedAccount = "U23154741";
        g_data.currentSymbol = "INTC";
        g_data.askPrice = 45.65;
        g_data.bidPrice = 45.64;
        g_data.lastPrice = 45.645;
        g_data.staleQuoteThresholdMs = 500;
        g_data.lastQuoteUpdate = std::chrono::steady_clock::now() - std::chrono::milliseconds(1200);

        OrderInfo order;
        order.orderId = 200;
        order.account = "U23154741";
        order.symbol = "INTC";
        order.side = "BUY";
        order.quantity = 1.0;
        order.remainingQty = 1.0;
        order.limitPrice = 45.65;
        order.status = "Submitted";
        g_data.orders[200] = order;
    }
    publishSharedDataSnapshot();

    RuntimePresentationSnapshot staleSnapshot = captureRuntimePresentationSnapshot("INTC", 10);
    expect(!staleSnapshot.symbol.hasFreshQuote, "old quote should be marked stale in presentation snapshot");
    expect(staleSnapshot.orders.size() == 1, "working order should appear in presentation snapshot");
    expect(!staleSnapshot.orders.front().second.cancelPending, "working order should not start as cancel-pending");

    const auto marked = markAllPendingOrdersForCancel();
    expect(marked.size() == 1 && marked.front() == 200, "markAllPendingOrdersForCancel should mark the working order");

    RuntimePresentationSnapshot pendingCancelSnapshot = captureRuntimePresentationSnapshot("INTC", 10);
    expect(pendingCancelSnapshot.orders.size() == 1, "presentation snapshot should still include cancel-pending order");
    expect(pendingCancelSnapshot.orders.front().second.cancelPending, "presentation snapshot should preserve cancel-pending state");

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        g_data.lastQuoteUpdate = std::chrono::steady_clock::now();
    }
    publishSharedDataSnapshot();

    RuntimePresentationSnapshot freshSnapshot = captureRuntimePresentationSnapshot("INTC", 10);
    expect(freshSnapshot.symbol.hasFreshQuote, "recent quote should be marked fresh in presentation snapshot");

    unbindSharedDataOwner(&owner);
    resetSharedDataForTesting();
}

void testOrderWatchdogEscalatesToManualReview() {
    clearTestFiles();

    SharedData owner;
    bindSharedDataOwner(&owner);

    const auto now = std::chrono::steady_clock::now();
    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        OrderInfo order;
        order.orderId = 301;
        order.account = "U23154741";
        order.symbol = "INTC";
        order.side = "BUY";
        order.quantity = 1.0;
        order.remainingQty = 1.0;
        order.limitPrice = 45.64;
        order.status = "Submitted";
        order.submitTime = now - std::chrono::seconds(3);
        order.localState = LocalOrderState::AwaitingBrokerEcho;
        order.watchdogs.brokerEchoArmed = true;
        order.watchdogs.brokerEchoDeadline = now - std::chrono::milliseconds(1);
        g_data.orders[301] = order;
    }
    publishSharedDataSnapshot();

    const OrderWatchdogSweepResult first = sweepOrderWatchdogs(now);
    expect(first.reconciliationOrders.size() == 1, "first watchdog sweep should begin reconciliation");
    expect(first.reconciliationOrders.front().reason == "broker_echo_timeout", "first watchdog reason should be broker echo timeout");

    const OrderWatchdogSweepResult second = sweepOrderWatchdogs(now + std::chrono::seconds(3));
    expect(second.reconciliationOrders.size() == 1, "second watchdog sweep should retry reconciliation");
    expect(second.reconciliationOrders.front().reconciliationAttempts == 2, "second watchdog sweep should increment reconciliation attempts");

    const OrderWatchdogSweepResult third = sweepOrderWatchdogs(now + std::chrono::seconds(9));
    expect(third.reconciliationOrders.size() == 1, "third watchdog sweep should issue final reconciliation retry");
    expect(third.reconciliationOrders.front().reconciliationAttempts == 3, "third watchdog sweep should increment reconciliation attempts to three");

    const OrderWatchdogSweepResult fourth = sweepOrderWatchdogs(now + std::chrono::seconds(15));
    expect(fourth.manualReviewOrders.size() == 1, "fourth watchdog sweep should escalate to manual review");
    expect(fourth.manualReviewOrders.front().reason == "reconciliation_retry_exhausted",
           "manual review should record reconciliation exhaustion");

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        const auto it = g_data.orders.find(301);
        expect(it != g_data.orders.end(), "watchdog test order should still exist");
        expect(it->second.localState == LocalOrderState::NeedsManualReview, "order should end in manual review");
        expect(it->second.watchdogs.reconciliationAttempts == 3, "manual review should preserve reconciliation attempt count");
    }

    unbindSharedDataOwner(&owner);
    resetSharedDataForTesting();
}

void testCancelAndPartialFillWatchdogs() {
    clearTestFiles();

    SharedData owner;
    bindSharedDataOwner(&owner);

    const auto now = std::chrono::steady_clock::now();
    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        OrderInfo cancelOrder;
        cancelOrder.orderId = 401;
        cancelOrder.account = "U23154741";
        cancelOrder.symbol = "INTC";
        cancelOrder.side = "BUY";
        cancelOrder.quantity = 1.0;
        cancelOrder.remainingQty = 1.0;
        cancelOrder.limitPrice = 45.64;
        cancelOrder.status = "Submitted";
        cancelOrder.submitTime = now - std::chrono::seconds(1);
        cancelOrder.localState = LocalOrderState::Working;
        g_data.orders[401] = cancelOrder;

        OrderInfo partialOrder;
        partialOrder.orderId = 402;
        partialOrder.account = "U23154741";
        partialOrder.symbol = "INTC";
        partialOrder.side = "BUY";
        partialOrder.quantity = 2.0;
        partialOrder.remainingQty = 2.0;
        partialOrder.limitPrice = 45.65;
        partialOrder.status = "Submitted";
        partialOrder.submitTime = now - std::chrono::seconds(1);
        partialOrder.localState = LocalOrderState::Working;
        g_data.orders[402] = partialOrder;
    }
    publishSharedDataSnapshot();

    noteCancelRequestsSent({401}, now);
    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        const auto it = g_data.orders.find(401);
        expect(it != g_data.orders.end(), "cancel test order should exist");
        expect(it->second.localState == LocalOrderState::AwaitingCancelAck, "cancel request should arm cancel-ack watchdog");
        expect(it->second.watchdogs.cancelAckArmed, "cancel-ack watchdog should be armed");
    }

    const OrderWatchdogSweepResult cancelSweep = sweepOrderWatchdogs(now + std::chrono::seconds(6));
    expect(cancelSweep.reconciliationOrders.size() == 1, "cancel timeout should begin reconciliation");
    expect(cancelSweep.reconciliationOrders.front().orderId == 401, "cancel timeout should target the cancel order");
    expect(cancelSweep.reconciliationOrders.front().reason == "cancel_ack_timeout", "cancel timeout should record the right reason");

    Contract contract;
    contract.symbol = "INTC";

    Execution execution;
    execution.orderId = 402;
    execution.permId = 9001;
    execution.execId = "EX-1";
    execution.side = "BOT";
    execution.shares = DecimalFunctions::doubleToDecimal(1.0);
    execution.cumQty = DecimalFunctions::doubleToDecimal(1.0);
    execution.price = 45.65;
    execution.avgPrice = 45.65;
    execution.exchange = "SMART";
    execution.time = "20260313 10:15:00";

    trading_engine::reduce(g_data, trading_engine::BrokerExecutionEvent{contract, execution});
    publishSharedDataSnapshot();
    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        const auto it = g_data.orders.find(402);
        expect(it != g_data.orders.end(), "partial-fill order should exist");
        expect(it->second.localState == LocalOrderState::PartiallyFilled, "execution should move order into partially filled");
        expect(std::abs(it->second.filledQty - 1.0) < 1e-9, "execution should update filled quantity");
        expect(it->second.seenExecIds.size() == 1, "first execution should be recorded once");
    }

    trading_engine::reduce(g_data, trading_engine::BrokerExecutionEvent{contract, execution});
    publishSharedDataSnapshot();
    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        const auto it = g_data.orders.find(402);
        expect(it->second.seenExecIds.size() == 1, "duplicate execution should be deduped");
        expect(std::abs(it->second.filledQty - 1.0) < 1e-9, "duplicate execution should not change filled quantity");
    }

    const OrderWatchdogSweepResult partialSweep = sweepOrderWatchdogs(now + std::chrono::seconds(20));
    const auto partialIt = std::find_if(partialSweep.reconciliationOrders.begin(),
                                        partialSweep.reconciliationOrders.end(),
                                        [](const OrderWatchdogAction& action) {
                                            return action.orderId == 402 &&
                                                   action.reason == "partial_fill_quiet_timeout";
                                        });
    expect(partialIt != partialSweep.reconciliationOrders.end(),
           "quiet partial fill should begin reconciliation for the partial order");

    unbindSharedDataOwner(&owner);
    resetSharedDataForTesting();
}

void testOpenOrderResolvesReconcilingStateAndFloorsOrderIds() {
    clearTestFiles();

    SharedData owner;
    bindSharedDataOwner(&owner);

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        OrderInfo order;
        order.orderId = 501;
        order.account = "U23154741";
        order.symbol = "INTC";
        order.side = "BUY";
        order.quantity = 1.0;
        order.remainingQty = 1.0;
        order.limitPrice = 45.70;
        order.status = "Submitted";
        order.localState = LocalOrderState::NeedsReconciliation;
        order.watchdogs.reconciliationAttempts = 2;
        order.watchdogs.brokerEchoArmed = true;
        order.lastReconciliationReason = "broker_echo_timeout";
        g_data.orders[501] = order;
        g_data.nextOrderId.store(10, std::memory_order_relaxed);
    }

    Contract contract;
    contract.symbol = "INTC";

    Order brokerOrder;
    brokerOrder.orderId = 501;
    brokerOrder.action = "BUY";
    brokerOrder.totalQuantity = DecimalFunctions::doubleToDecimal(1.0);
    brokerOrder.lmtPrice = 45.70;
    brokerOrder.account = "U23154741";
    brokerOrder.permId = 9101;

    OrderState brokerState;
    brokerState.status = "Submitted";

    trading_engine::reduce(g_data, trading_engine::BrokerOpenOrderEvent{501, contract, brokerOrder, brokerState});
    publishSharedDataSnapshot();

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        const auto it = g_data.orders.find(501);
        expect(it != g_data.orders.end(), "open-order test order should still exist");
        expect(it->second.localState == LocalOrderState::Working, "openOrder should resolve reconciling order into working");
        expect(!it->second.watchdogs.brokerEchoArmed, "openOrder should disarm broker-echo watchdog");
        expect(g_data.nextOrderId.load(std::memory_order_relaxed) > 501, "openOrder should floor the next order id");
    }

    unbindSharedDataOwner(&owner);
    resetSharedDataForTesting();
}

void testManualReconcileAndAcknowledgeFlow() {
    clearTestFiles();

    SharedData owner;
    bindSharedDataOwner(&owner);

    const auto now = std::chrono::steady_clock::now();
    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);

        OrderInfo reconcilable;
        reconcilable.orderId = 601;
        reconcilable.account = "U23154741";
        reconcilable.symbol = "INTC";
        reconcilable.side = "BUY";
        reconcilable.quantity = 1.0;
        reconcilable.remainingQty = 1.0;
        reconcilable.limitPrice = 45.80;
        reconcilable.status = "Submitted";
        reconcilable.localState = LocalOrderState::Working;
        g_data.orders[601] = reconcilable;

        OrderInfo manualReview;
        manualReview.orderId = 602;
        manualReview.account = "U23154741";
        manualReview.symbol = "INTC";
        manualReview.side = "SELL";
        manualReview.quantity = 1.0;
        manualReview.remainingQty = 1.0;
        manualReview.limitPrice = 45.81;
        manualReview.status = "Submitted";
        manualReview.localState = LocalOrderState::NeedsManualReview;
        manualReview.lastReconciliationReason = "broker_echo_timeout";
        manualReview.watchdogs.reconciliationAttempts = 3;
        g_data.orders[602] = manualReview;
    }
    publishSharedDataSnapshot();

    const OrderWatchdogSweepResult reconcile =
        requestOrderReconciliation({601, 602}, "manual_reconcile", now);
    expect(reconcile.reconciliationOrders.size() == 2, "manual reconcile should target both selected active orders");

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        const auto working = g_data.orders.find(601);
        expect(working != g_data.orders.end(), "manual reconcile working order should exist");
        expect(working->second.localState == LocalOrderState::NeedsReconciliation,
               "manual reconcile should move working order into reconciling");
        expect(working->second.lastReconciliationReason == "manual_reconcile",
               "manual reconcile should record the operator reason");

        const auto review = g_data.orders.find(602);
        expect(review != g_data.orders.end(), "manual review order should exist");
        expect(review->second.localState == LocalOrderState::NeedsReconciliation,
               "manual reconcile should let manual-review orders retry");
        expect(review->second.watchdogs.reconciliationAttempts == 4,
               "manual reconcile should advance the reconciliation attempt count");
    }

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        g_data.orders[602].localState = LocalOrderState::NeedsManualReview;
        g_data.orders[602].manualReviewAcknowledged = false;
    }
    publishSharedDataSnapshot();

    const std::vector<OrderId> acknowledged = acknowledgeManualReviewOrders({601, 602}, now);
    expect(acknowledged.size() == 1 && acknowledged.front() == 602,
           "acknowledge should only mark manual-review orders");

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        const auto review = g_data.orders.find(602);
        expect(review->second.manualReviewAcknowledged, "manual review acknowledgement should persist");
        expect(formatOrderLocalStateText(review->second).find("ack") != std::string::npos,
               "local state text should surface acknowledged manual review");
        expect(formatOrderWatchdogText(review->second).find("ack") != std::string::npos,
               "watchdog text should surface acknowledged manual review");
    }

    unbindSharedDataOwner(&owner);
    resetSharedDataForTesting();
}

} // namespace

int main() {
    try {
        testDataDir();
        testBridgeBatchCodecRoundTripPreservesOrderAndMetadata();
        testReplayPrefersRichLiveTrace();
        testTraceIdFloorRecoversFromLog();
        testReplayHandlesPartialFillsAndCommission();
        testWebSocketRuntimeGuards();
        testRecoverySnapshotReportsAbnormalShutdown();
        testBridgeOutboxSourceSeqPreservesAcceptanceOrderingAndAnchors();
        testBridgeBatchFixtureMatchesGoldenFrame();
        testBridgeBatchSenderPreservesOrderingAcrossRetries();
        testBridgeDispatchBatchRespectsThresholdsAndImmediateFlush();
        testBridgeDispatchSnapshotAndDeliveryAck();
        testUnixDomainSocketTransportSendsFramedBatch();
        testTapeEngineAcceptsBatchAssignsSessionSeqAndWritesSegments();
        testTapeEngineEmitsGapMarkersAndDeduplicatesSourceSeq();
        testTapeEngineQueryStatusAndReads();
        testTapeScopeClientReadsPhase4EngineSeam();
        testTapeEngineRevisionPinnedReadsCanOverlayMutableTail();
        testTapeEnginePhase3FindingsIncidentsAndProtectedWindows();
        testTapeEnginePhase3ArtifactsPersistAcrossRestartAndReadProtectedWindow();
        testTapeEnginePhase3ScansSessionIntoDurableReportArtifact();
        testTapeEnginePhase3PersistsIncidentAndOrderCaseReports();
        testTapeEnginePhase3InvestigationContractMatchesGoldenFixtures();
        testTapeEnginePhase3CollapsesRepeatedFindingsIntoRankedIncidents();
        testTapeEnginePhase3DetectsInsideLiquiditySignals();
        testTapeEnginePhase3DetectsDisplayInstabilitySignals();
        testTapeEnginePhase3DetectsPullFollowThroughAndQuoteFlickerSignals();
        testTapeEnginePhase3DetectsTradeAfterDepletionAndAbsorptionPersistenceSignals();
        testTapeEnginePhase3DetectsFillInvalidationSignals();
        testTapeEnginePhase3BuildsOrderWindowMarketImpactFinding();
        testTapeEnginePhase3BuildsPassiveFillQueueProxyAndAdverseSelection();
        testTapeEnginePhase3BuildsPassiveQueueLossAndCutThroughSignals();
        testTapeEnginePhase3BuildsSweepAndFadeSignals();
        testTapeEnginePhase3BuildsFillOutcomeChains();
        testTapeEnginePhase3BuildsTradePressureOrderCase();
        testTapeEngineResetMarkerPreservesCanonicalInstrumentIdentity();
        testTapeEngineReplaySnapshotRebuildsFrozenMarketState();
        testBridgeMarketDataEmissionExpandsPublicEvents();
        testTapeEnginePrefersStrongConfiguredInstrumentIdentityAndMarksHeuristicFallback();
        testTapeEngineCanRejectMismatchedStrongInstrumentIdsInStrictMode();
        testBridgeLifecycleEmissionExpandsPrivateOrderEvents();
        testGeneratedRuntimeRegistryMatchesPhase15QueueSpec();
        testBridgeOutboxOverflowWritesExplicitLossMarker();
        testRecoverySnapshotReportsBridgeContinuityLossAfterAbnormalShutdown();
        testTradingWrapperSessionReadyAndReconnect();
        testTradingWrapperIgnoresDuplicateOrderStatus();
        testRuntimePresentationSnapshotCapturesConsistentState();
        testPendingUiSyncUpdateConsumesFlags();
        testRuntimePresentationSnapshotTracksQuoteFreshnessAndCancelMarking();
        testOrderWatchdogEscalatesToManualReview();
        testCancelAndPartialFillWatchdogs();
        testOpenOrderResolvesReconcilingStateAndFloorsOrderIds();
        testManualReconcileAndAcknowledgeFlow();
        std::cout << "All trace tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
