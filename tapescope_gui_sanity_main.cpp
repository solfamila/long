#include "bridge_batch_codec.h"
#include "bridge_batch_transport.h"
#include "tape_engine.h"

#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <optional>
#include <pthread.h>
#include <string>
#include <thread>

namespace fs = std::filesystem;

namespace {

[[noreturn]] void fail(const std::string& message) {
    throw std::runtime_error(message);
}

std::string argValue(int argc, char** argv, const std::string& flag) {
    for (int index = 1; index + 1 < argc; ++index) {
        if (argv[index] == flag) {
            return argv[index + 1];
        }
    }
    return {};
}

std::string envOrDefault(const char* key, const std::string& fallback) {
    const char* value = std::getenv(key);
    return value != nullptr && value[0] != '\0' ? std::string(value) : fallback;
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
                                    const std::string& wallTime) {
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
    record.fallbackState = "queued_for_recovery";
    record.fallbackReason = "engine_unavailable";
    record.note = note;
    record.wallTime = wallTime;
    return record;
}

void sendBatch(bridge_batch::UnixDomainSocketTransport* transport,
               bridge_batch::BuildOptions* options,
               const std::vector<BridgeOutboxRecord>& records,
               std::uint64_t batchSeq,
               const std::string& context) {
    options->batchSeq = batchSeq;
    std::string error;
    if (!transport->sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch(records, *options)), &error)) {
        fail(context + ": " + error);
    }
}

void seedEngine(const std::string& socketPath,
                const std::string& instrumentId,
                const std::string& symbol,
                std::uint64_t traceId,
                OrderId orderId,
                long long permId) {
    bridge_batch::BuildOptions options;
    options.appSessionId = "app-tapescope-gui-sanity";
    options.runtimeSessionId = "runtime-tapescope-gui-sanity";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

    bridge_batch::UnixDomainSocketTransport transport(socketPath);

    BridgeOutboxRecord orderIntent = makeBridgeRecord(12001,
                                                      "order_intent",
                                                      "WebSocket",
                                                      symbol,
                                                      "BUY",
                                                      traceId,
                                                      orderId,
                                                      0,
                                                      "",
                                                      "GUI sanity order intent",
                                                      "2026-03-18T10:15:00.100");
    orderIntent.instrumentId = instrumentId;

    BridgeOutboxRecord orderStatus = makeBridgeRecord(12002,
                                                      "order_status",
                                                      "BrokerOrderStatus",
                                                      symbol,
                                                      "BUY",
                                                      traceId,
                                                      orderId,
                                                      permId,
                                                      "",
                                                      "Submitted: filled=0 remaining=1",
                                                      "2026-03-18T10:15:00.120");
    orderStatus.instrumentId = instrumentId;

    BridgeOutboxRecord bidTick = makeBridgeRecord(12003,
                                                  "market_tick",
                                                  "BrokerMarketData",
                                                  symbol,
                                                  "BID",
                                                  0,
                                                  0,
                                                  0,
                                                  "",
                                                  "bid tick",
                                                  "2026-03-18T10:15:00.130");
    bidTick.instrumentId = instrumentId;
    bidTick.marketField = 1;
    bidTick.price = 585.20;

    BridgeOutboxRecord askTick = makeBridgeRecord(12004,
                                                  "market_tick",
                                                  "BrokerMarketData",
                                                  symbol,
                                                  "ASK",
                                                  0,
                                                  0,
                                                  0,
                                                  "",
                                                  "ask tick",
                                                  "2026-03-18T10:15:00.140");
    askTick.instrumentId = instrumentId;
    askTick.marketField = 2;
    askTick.price = 585.23;

    BridgeOutboxRecord depthBid = makeBridgeRecord(12005,
                                                   "market_depth",
                                                   "BrokerMarketDepth",
                                                   symbol,
                                                   "BID",
                                                   0,
                                                   0,
                                                   0,
                                                   "",
                                                   "BID depth update pos=0 px=585.20 sz=400",
                                                   "2026-03-18T10:15:00.150");
    depthBid.instrumentId = instrumentId;
    depthBid.bookSide = 1;
    depthBid.bookPosition = 0;
    depthBid.bookOperation = 0;
    depthBid.price = 585.20;
    depthBid.size = 400.0;

    BridgeOutboxRecord depthAsk = makeBridgeRecord(12006,
                                                   "market_depth",
                                                   "BrokerMarketDepth",
                                                   symbol,
                                                   "ASK",
                                                   0,
                                                   0,
                                                   0,
                                                   "",
                                                   "ASK depth update pos=0 px=585.23 sz=350",
                                                   "2026-03-18T10:15:00.151");
    depthAsk.instrumentId = instrumentId;
    depthAsk.bookSide = 0;
    depthAsk.bookPosition = 0;
    depthAsk.bookOperation = 0;
    depthAsk.price = 585.23;
    depthAsk.size = 350.0;

    BridgeOutboxRecord widenedAsk = makeBridgeRecord(12007,
                                                     "market_tick",
                                                     "BrokerMarketData",
                                                     symbol,
                                                     "ASK",
                                                     0,
                                                     0,
                                                     0,
                                                     "",
                                                     "ask widened",
                                                     "2026-03-18T10:15:00.170");
    widenedAsk.instrumentId = instrumentId;
    widenedAsk.marketField = 2;
    widenedAsk.price = 585.31;

    BridgeOutboxRecord fillExecution = makeBridgeRecord(12008,
                                                        "fill_execution",
                                                        "BrokerExecution",
                                                        symbol,
                                                        "BOT",
                                                        traceId,
                                                        orderId,
                                                        permId,
                                                        "EXEC-GUI-401",
                                                        "GUI sanity fill",
                                                        "2026-03-18T10:15:00.250");
    fillExecution.instrumentId = instrumentId;
    fillExecution.price = 585.24;
    fillExecution.size = 1.0;

    sendBatch(&transport, &options, {orderIntent, orderStatus}, 401, "seed anchor batch");
    sendBatch(&transport, &options, {bidTick, askTick, depthBid, depthAsk}, 402, "seed market batch");
    sendBatch(&transport, &options, {widenedAsk, fillExecution}, 403, "seed fill batch");
}

void waitForSeededAnchor(const tape_engine::Server& server) {
    for (int attempt = 0; attempt < 100; ++attempt) {
        const auto snapshot = server.snapshot();
        if (!snapshot.segments.empty()) {
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    fail("seeded engine never flushed the reviewable trade");
}

} // namespace

int main(int argc, char** argv) {
    try {
        const std::string symbol = argValue(argc, argv, "--symbol").empty()
            ? envOrDefault("TAPESCOPE_GUI_SANITY_SYMBOL", "SPY")
            : argValue(argc, argv, "--symbol");
        const std::string socketPath = argValue(argc, argv, "--socket").empty()
            ? envOrDefault("TAPESCOPE_GUI_SANITY_SOCKET", "/tmp/tapescope-gui-sanity.sock")
            : argValue(argc, argv, "--socket");
        const fs::path dataDir = argValue(argc, argv, "--data-dir").empty()
            ? fs::path(envOrDefault("TAPESCOPE_GUI_SANITY_DATA_DIR", "/tmp/tapescope-gui-sanity"))
            : fs::path(argValue(argc, argv, "--data-dir"));
        const std::uint64_t traceId = 401;
        const OrderId orderId = 7401;
        const long long permId = 8801;
        const std::string instrumentId = "ib:STK:SMART:USD:" + symbol;

        std::error_code ec;
        fs::remove_all(dataDir, ec);
        fs::remove(socketPath, ec);

        tape_engine::EngineConfig config;
        config.socketPath = socketPath;
        config.dataDir = dataDir;
        config.instrumentId = instrumentId;
        config.ringCapacity = 128;

        sigset_t signals;
        sigemptyset(&signals);
        sigaddset(&signals, SIGINT);
        sigaddset(&signals, SIGTERM);
        if (pthread_sigmask(SIG_BLOCK, &signals, nullptr) != 0) {
            fail("failed to configure signal mask");
        }

        tape_engine::Server server(config);
        std::string startError;
        if (!server.start(&startError)) {
            fail("failed to start seeded tape_engine: " + startError);
        }

        seedEngine(socketPath, instrumentId, symbol, traceId, orderId, permId);
        waitForSeededAnchor(server);

        std::cout << "{\n"
                  << "  \"socket_path\": \"" << socketPath << "\",\n"
                  << "  \"data_dir\": \"" << dataDir.string() << "\",\n"
                  << "  \"symbol\": \"" << symbol << "\",\n"
                  << "  \"trace_id\": " << traceId << ",\n"
                  << "  \"order_id\": " << orderId << ",\n"
                  << "  \"perm_id\": " << permId << "\n"
                  << "}\n";
        std::cout.flush();

        int signalNumber = 0;
        if (sigwait(&signals, &signalNumber) != 0) {
            server.stop();
            fail("failed while waiting for shutdown signal");
        }

        server.stop();
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "tapescope_gui_sanity failed: " << error.what() << '\n';
        return 1;
    }
}
