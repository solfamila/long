#include "tape_engine_ingest.h"

#include <nlohmann/json.hpp>

#include <chrono>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <sys/un.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace fs = std::filesystem;
using json = nlohmann::json;

namespace {

void expect(bool condition, const std::string& message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

const fs::path& tempDir() {
    static const fs::path path = [] {
        char pattern[] = "/tmp/tape_engine_tests.XXXXXX";
        char* created = mkdtemp(pattern);
        if (created == nullptr) {
            throw std::runtime_error("mkdtemp failed");
        }
        return fs::path(created);
    }();
    return path;
}

std::vector<std::uint8_t> encodeFrame(const json& batch) {
    const std::vector<std::uint8_t> payload = json::to_msgpack(batch);
    std::vector<std::uint8_t> frame;
    frame.reserve(4 + payload.size());
    const std::uint32_t size = static_cast<std::uint32_t>(payload.size());
    frame.push_back(static_cast<std::uint8_t>((size >> 24) & 0xffU));
    frame.push_back(static_cast<std::uint8_t>((size >> 16) & 0xffU));
    frame.push_back(static_cast<std::uint8_t>((size >> 8) & 0xffU));
    frame.push_back(static_cast<std::uint8_t>(size & 0xffU));
    frame.insert(frame.end(), payload.begin(), payload.end());
    return frame;
}

json makeRecord(std::uint64_t sourceSeq,
                std::string recordType,
                std::string source,
                std::string symbol,
                std::string side,
                std::uint64_t traceId,
                std::int64_t orderId,
                std::int64_t permId,
                std::string execId,
                std::string note,
                std::string wallTime) {
    return json{
        {"anchor", {
            {"trace_id", traceId},
            {"order_id", orderId},
            {"perm_id", permId},
            {"exec_id", execId}
        }},
        {"fallback_reason", "engine_unavailable"},
        {"fallback_state", "queued_for_recovery"},
        {"note", note},
        {"record_type", recordType},
        {"side", side},
        {"source", source},
        {"source_seq", sourceSeq},
        {"symbol", symbol},
        {"wall_time", wallTime}
    };
}

json makeBatch(std::uint64_t batchSeq,
               std::string adapterId,
               std::string connectionId,
               const std::vector<json>& records) {
    expect(!records.empty(), "test batch requires at least one record");
    return json{
        {"app_session_id", "app-phase1-ingest"},
        {"adapter_id", adapterId},
        {"batch_seq", batchSeq},
        {"category", "bridge"},
        {"connection_id", connectionId},
        {"first_source_seq", records.front().at("source_seq").get<std::uint64_t>()},
        {"flush_reason", "immediate_lifecycle"},
        {"last_source_seq", records.back().at("source_seq").get<std::uint64_t>()},
        {"producer", "long"},
        {"queue_label", "com.foxy.long.bridge.sender"},
        {"queue_qos", "user_initiated"},
        {"record_count", records.size()},
        {"records", records},
        {"runtime_session_id", "runtime-phase1-ingest"},
        {"schema", "com.foxy.long.bridge.batch"},
        {"subsystem", "com.foxy.long.bridge"},
        {"transport", "framed_msgpack_v1"},
        {"version", 1}
    };
}

void writeAll(int fd, const std::vector<std::uint8_t>& bytes) {
    std::size_t written = 0;
    while (written < bytes.size()) {
        const ssize_t chunk = ::write(fd, bytes.data() + written, bytes.size() - written);
        if (chunk < 0) {
            if (errno == EINTR) {
                continue;
            }
            throw std::runtime_error("write failed");
        }
        written += static_cast<std::size_t>(chunk);
    }
}

int connectToSocket(const fs::path& socketPath) {
    int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        throw std::runtime_error("socket failed");
    }

    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    if (socketPath.string().size() >= sizeof(address.sun_path)) {
        ::close(fd);
        throw std::runtime_error("test socket path too long");
    }
    std::snprintf(address.sun_path, sizeof(address.sun_path), "%s", socketPath.c_str());
    if (::connect(fd, reinterpret_cast<sockaddr*>(&address),
                  static_cast<socklen_t>(offsetof(sockaddr_un, sun_path) + std::strlen(address.sun_path) + 1)) != 0) {
        ::close(fd);
        throw std::runtime_error("connect failed");
    }
    return fd;
}

template <typename Predicate>
void waitUntil(Predicate predicate, const std::string& message) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    throw std::runtime_error(message);
}

void testDirectIngestAssignsStrictSessionSeqAndRetainsMetadata() {
    tape_engine::InMemoryLog log;
    tape_engine::IngestService ingest(log);

    const auto firstBatch = makeBatch(7,
                                      "bridge-adapter",
                                      "connection-1",
                                      {
                                          makeRecord(101, "order_intent", "WebSocket", "INTC", "BUY",
                                                     71, 501, 0, "", "accepted order intent",
                                                     "2026-03-14T09:30:00.100"),
                                          makeRecord(102, "fill_execution", "BrokerExecution", "INTC", "BOT",
                                                     71, 501, 9501, "EXEC-71", "execution details observed",
                                                     "2026-03-14T09:30:00.250")
                                      });
    const auto secondBatch = makeBatch(8,
                                       "bridge-adapter",
                                       "connection-1",
                                       {
                                           makeRecord(103, "order_intent", "WebSocket", "AAPL", "SELL",
                                                      72, 777, 0, "", "accepted second order intent",
                                                      "2026-03-14T09:31:00.100")
                                       });

    std::string error;
    expect(ingest.ingestFrame(encodeFrame(firstBatch), &error), "first batch should ingest: " + error);
    expect(ingest.ingestFrame(encodeFrame(secondBatch), &error), "second batch should ingest: " + error);

    const auto snapshot = log.snapshot();
    expect(snapshot.records.size() == 3, "ingest log should append all accepted records");
    expect(snapshot.batches.size() == 2, "ingest log should retain batch metadata");
    expect(snapshot.records[0].sessionSeq == 1, "first accepted record should receive session_seq=1");
    expect(snapshot.records[1].sessionSeq == 2, "second accepted record should receive session_seq=2");
    expect(snapshot.records[2].sessionSeq == 3, "session_seq should continue across batches");
    expect(snapshot.records[0].sourceSeq == 101, "first record should retain source_seq");
    expect(snapshot.records[2].sourceSeq == 103, "later records should retain source_seq ordering");
    expect(snapshot.batches[0].adapterId == "bridge-adapter", "batch metadata should retain adapter_id");
    expect(snapshot.batches[0].connectionId == "connection-1", "batch metadata should retain connection_id");
    expect(snapshot.batches[0].firstSourceSeq == 101, "batch metadata should retain first source_seq");
    expect(snapshot.batches[0].lastSourceSeq == 102, "batch metadata should retain last source_seq");
}

void testDaemonRejectsMalformedBatchAndAcceptsNextValidBatch() {
    tape_engine::InMemoryLog log;
    tape_engine::IngestService ingest(log);
    const fs::path socketPath = tempDir() / "daemon.sock";
    tape_engine::Daemon daemon({socketPath.string()}, ingest);

    std::string daemonError;
    bool daemonOk = false;
    std::thread server([&]() {
        daemonOk = daemon.run(&daemonError);
    });

    waitUntil([&]() { return fs::exists(socketPath); }, "daemon socket was not created");

    auto malformedBatch = makeBatch(9,
                                    "bridge-adapter",
                                    "connection-2",
                                    {
                                        makeRecord(201, "order_intent", "WebSocket", "MSFT", "BUY",
                                                   81, 601, 0, "", "accepted order intent",
                                                   "2026-03-14T10:00:00.100")
                                    });
    malformedBatch.erase("adapter_id");
    malformedBatch.erase("queue_label");

    {
        const int client = connectToSocket(socketPath);
        writeAll(client, encodeFrame(malformedBatch));
        ::shutdown(client, SHUT_WR);
        ::close(client);
    }

    waitUntil([&]() { return ingest.stats().rejectedBatches == 1; },
              "daemon should reject malformed bridge batch");

    const auto validBatch = makeBatch(10,
                                      "bridge-adapter",
                                      "connection-2",
                                      {
                                          makeRecord(202, "fill_execution", "BrokerExecution", "MSFT", "BOT",
                                                     81, 601, 9601, "EXEC-81", "execution details observed",
                                                     "2026-03-14T10:00:00.250")
                                      });
    {
        const int client = connectToSocket(socketPath);
        writeAll(client, encodeFrame(validBatch));
        ::shutdown(client, SHUT_WR);
        ::close(client);
    }

    waitUntil([&]() { return ingest.stats().acceptedBatches == 1; },
              "daemon should continue accepting valid batches after rejection");

    daemon.requestStop();
    server.join();
    expect(daemonOk, "daemon should exit cleanly: " + daemonError);

    const auto snapshot = log.snapshot();
    expect(snapshot.records.size() == 1, "malformed batch should not append records");
    expect(snapshot.records.front().sessionSeq == 1, "first accepted daemon record should receive session_seq=1");
    expect(snapshot.records.front().adapterId == "bridge-adapter", "daemon ingest should retain adapter_id");
    expect(snapshot.records.front().connectionId == "connection-2", "daemon ingest should retain connection_id");
    expect(ingest.stats().lastError.find("adapter_id") != std::string::npos,
           "malformed rejection should report missing adapter_id");
}

} // namespace

int main() {
    try {
        testDirectIngestAssignsStrictSessionSeqAndRetainsMetadata();
        testDaemonRejectsMalformedBatchAndAcceptsNextValidBatch();
        std::cout << "All tape-engine ingest tests passed\n";
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << ex.what() << '\n';
        return 1;
    }
}
