#include "app_shared.h"
#include "bridge_batch_codec.h"
#include "bridge_batch_transport.h"
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

std::string trimWhitespace(std::string text) {
    text.erase(std::remove_if(text.begin(), text.end(), [](unsigned char ch) {
        return std::isspace(ch) != 0;
    }), text.end());
    return text;
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

    const tape_engine::EngineSnapshot snapshot = server.snapshot();
    expect(snapshot.nextSessionSeq == 3, "tape-engine should assign contiguous session_seq values to accepted records");
    expect(snapshot.liveEvents.size() == 2, "tape-engine live ring should retain accepted records");
    expect(snapshot.liveEvents.front().sessionSeq == 1, "tape-engine should assign the first session_seq");
    expect(snapshot.liveEvents.back().sessionSeq == 2, "tape-engine should assign the second session_seq");
    expect(snapshot.liveEvents.back().bridgeRecord.anchor.execId == "EXEC-71", "tape-engine should preserve bridge anchors");
    expect(snapshot.segments.size() == 1, "tape-engine should emit one segment for the accepted batch");
    expect(snapshot.segments.front().firstSessionSeq == 1, "tape-engine segment metadata should preserve the first session_seq");
    expect(snapshot.segments.front().lastSessionSeq == 2, "tape-engine segment metadata should preserve the last session_seq");
    expect(!snapshot.segments.front().payloadSha256.empty(), "tape-engine segment metadata should include a payload checksum");
    expect(!snapshot.segments.front().manifestHash.empty(), "tape-engine segment metadata should include a manifest hash");

    const fs::path segmentPath = rootDir / "segments" / snapshot.segments.front().fileName;
    const fs::path metadataPath = rootDir / "segments" / snapshot.segments.front().metadataFileName;
    const fs::path manifestPath = rootDir / "manifest.jsonl";
    expect(fs::exists(segmentPath), "tape-engine should write the segment payload file");
    expect(fs::exists(metadataPath), "tape-engine should write the segment metadata file");
    expect(fs::exists(manifestPath), "tape-engine should append the manifest hash chain");

    const std::vector<json> manifestLines = readJsonLines(manifestPath.string());
    expect(manifestLines.size() == 1, "tape-engine should append one manifest line for the accepted batch");
    expect(manifestLines.front().value("manifest_hash", std::string()) == snapshot.segments.front().manifestHash,
           "tape-engine manifest hash should match the in-memory segment metadata");

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

    tape_engine::Client client(socketPath.string());
    tape_engine::QueryResponse response;

    tape_engine::QueryRequest statusRequest;
    statusRequest.requestId = "status-1";
    statusRequest.operation = "status";
    expect(client.query(statusRequest, &response, &error), "tape-engine status query should succeed: " + error);
    expect(response.summary.value("segment_count", 0ULL) == 2, "status query should report the written segment count");
    expect(response.summary.value("latest_session_seq", 0ULL) == 4, "status query should report the latest accepted session_seq");

    tape_engine::QueryRequest rangeRequest;
    rangeRequest.requestId = "range-1";
    rangeRequest.operation = "read_range";
    rangeRequest.fromSessionSeq = 2;
    rangeRequest.toSessionSeq = 4;
    expect(client.query(rangeRequest, &response, &error), "tape-engine range query should succeed: " + error);
    expect(response.events.is_array() && response.events.size() == 3, "range query should return the requested session_seq window");
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

    tape_engine::Client client(socketPath.string());
    tape_engine::QueryResponse response;

    tape_engine::QueryRequest replayRequest;
    replayRequest.requestId = "replay-1";
    replayRequest.operation = "replay_snapshot";
    replayRequest.targetSessionSeq = 7;
    replayRequest.limit = 2;
    expect(client.query(replayRequest, &response, &error), "tape-engine replay snapshot query should succeed: " + error);

    expect(response.summary.value("target_session_seq", 0ULL) == 7, "replay snapshot should preserve the requested session_seq");
    expect(response.summary.value("replayed_through_session_seq", 0ULL) == 7, "replay snapshot should rebuild through the target session_seq");
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

    server.stop();
}

void testBridgeMarketDataEmissionExpandsPublicEvents() {
    clearTestFiles();

    SharedData owner;
    bindSharedDataOwner(&owner);

    trading_engine::reduce(appState(), trading_engine::MarketSubscriptionStartedEvent{
        "INTC",
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
        testTapeEngineReplaySnapshotRebuildsFrozenMarketState();
        testBridgeMarketDataEmissionExpandsPublicEvents();
        testBridgeLifecycleEmissionExpandsPrivateOrderEvents();
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
