#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

namespace tape_engine {

inline constexpr std::uint32_t kBridgeBatchWireVersion = 1;
inline constexpr const char* kBridgeBatchSchemaName = "com.foxy.long.bridge.batch";
inline constexpr const char* kBridgeBatchProducerName = "long";
inline constexpr const char* kBridgeBatchTransportName = "framed_msgpack_v1";
inline constexpr std::size_t kDefaultMaxFrameBytes = 1024 * 1024;

struct BridgeAnchor {
    std::uint64_t traceId = 0;
    std::int64_t orderId = 0;
    std::int64_t permId = 0;
    std::string execId;
};

struct BridgeRecord {
    std::uint64_t sourceSeq = 0;
    std::string recordType;
    std::string source;
    std::string symbol;
    std::string side;
    BridgeAnchor anchor;
    std::string fallbackState;
    std::string fallbackReason;
    std::string note;
    std::string wallTime;
};

struct BridgeBatchHeader {
    std::uint32_t version = kBridgeBatchWireVersion;
    std::string schema = kBridgeBatchSchemaName;
    std::string producer = kBridgeBatchProducerName;
    std::string transport = kBridgeBatchTransportName;
    std::string subsystem;
    std::string category;
    std::string senderLabel;
    std::string senderQos;
    std::string appSessionId;
    std::string runtimeSessionId;
    std::string adapterId;
    std::string connectionId;
    std::string flushReason;
    std::uint64_t batchSeq = 0;
    std::uint64_t firstSourceSeq = 0;
    std::uint64_t lastSourceSeq = 0;
    std::uint64_t recordCount = 0;
};

struct BridgeBatch {
    BridgeBatchHeader header;
    std::vector<BridgeRecord> records;
};

struct IngestBatchMetadata {
    std::string adapterId;
    std::string connectionId;
    std::string appSessionId;
    std::string runtimeSessionId;
    std::uint64_t batchSeq = 0;
    std::uint64_t firstSourceSeq = 0;
    std::uint64_t lastSourceSeq = 0;
    std::size_t acceptedRecordCount = 0;
};

struct IngestLogRecord {
    std::uint64_t sessionSeq = 0;
    std::uint64_t batchSeq = 0;
    std::uint64_t sourceSeq = 0;
    std::string adapterId;
    std::string connectionId;
    std::string recordType;
    std::string source;
    std::string symbol;
    std::string side;
    BridgeAnchor anchor;
    std::string note;
    std::string wallTime;
};

struct IngestLogSnapshot {
    std::vector<IngestBatchMetadata> batches;
    std::vector<IngestLogRecord> records;
    std::uint64_t nextSessionSeq = 1;
};

class InMemoryLog {
public:
    struct AppendResult {
        std::uint64_t firstSessionSeq = 0;
        std::uint64_t lastSessionSeq = 0;
        std::size_t acceptedRecordCount = 0;
        IngestBatchMetadata batch;
    };

    AppendResult append(const BridgeBatch& batch);
    IngestLogSnapshot snapshot() const;

private:
    mutable std::mutex mutex_;
    std::uint64_t nextSessionSeq_ = 1;
    std::vector<IngestBatchMetadata> batches_;
    std::vector<IngestLogRecord> records_;
};

struct ServiceStats {
    std::uint64_t acceptedBatches = 0;
    std::uint64_t rejectedBatches = 0;
    std::string lastError;
};

class IngestService {
public:
    explicit IngestService(InMemoryLog& log);

    bool ingestFrame(const std::vector<std::uint8_t>& frame, std::string* error = nullptr);
    void recordRejected(const std::string& error);
    ServiceStats stats() const;

private:
    InMemoryLog& log_;
    mutable std::mutex mutex_;
    ServiceStats stats_;
};

struct DaemonOptions {
    std::string socketPath;
    std::size_t maxFrameBytes = kDefaultMaxFrameBytes;
    int listenBacklog = 4;
};

class Daemon {
public:
    Daemon(DaemonOptions options, IngestService& service);
    ~Daemon();

    bool run(std::string* error = nullptr);
    void requestStop();
    const DaemonOptions& options() const;

private:
    bool openListener(std::string* error);
    bool handleClient(int clientFd);
    bool readNextFrame(int clientFd, std::vector<std::uint8_t>* frame, bool* eof, std::string* error);
    void cleanupSocket();

    DaemonOptions options_;
    IngestService& service_;
    std::atomic<bool> stopRequested_{false};
    std::mutex fdMutex_;
    int listenFd_ = -1;
    int activeClientFd_ = -1;
};

BridgeBatch decodeFrame(const std::vector<std::uint8_t>& frame);

} // namespace tape_engine
