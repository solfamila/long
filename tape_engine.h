#pragma once

#include "bridge_batch_codec.h"
#include "tape_engine_protocol.h"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <filesystem>
#include <future>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace tape_engine {

struct EngineConfig {
    std::string socketPath = "/tmp/tape-engine.sock";
    std::filesystem::path dataDir = std::filesystem::temp_directory_path() / "tape-engine";
    std::string instrumentId = "INTC";
    std::size_t ringCapacity = 4096;
};

struct EngineEvent {
    std::uint64_t sessionSeq = 0;
    std::uint64_t sourceSeq = 0;
    std::string eventKind;
    std::string adapterId;
    std::string connectionId;
    std::string instrumentId;
    std::uint64_t tsEngineNs = 0;
    BridgeOutboxRecord bridgeRecord;
    std::uint64_t gapStartSourceSeq = 0;
    std::uint64_t gapEndSourceSeq = 0;
};

struct SegmentInfo {
    std::uint64_t segmentId = 0;
    std::uint64_t firstSessionSeq = 0;
    std::uint64_t lastSessionSeq = 0;
    std::uint64_t eventCount = 0;
    std::string fileName;
    std::string metadataFileName;
    std::string payloadSha256;
    std::string prevManifestHash;
    std::string manifestHash;
};

struct EngineSnapshot {
    std::uint64_t nextSessionSeq = 1;
    std::uint64_t nextSegmentId = 1;
    std::vector<EngineEvent> liveEvents;
    std::vector<SegmentInfo> segments;
};

class Server {
public:
    explicit Server(EngineConfig config);
    ~Server();

    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    bool start(std::string* error = nullptr);
    void stop();

    EngineSnapshot snapshot() const;
    const EngineConfig& config() const;

private:
    struct PendingRequest {
        std::vector<std::uint8_t> frame;
        std::promise<IngestAck> promise;
    };

    struct ConnectionCursor {
        std::uint64_t lastAcceptedSourceSeq = 0;
        bool hasLastAccepted = false;
        std::map<std::uint64_t, bool> seenSourceSeqs;
    };

    void acceptLoop();
    void sequencerLoop();
    IngestAck processFrame(const std::vector<std::uint8_t>& frame);
    IngestAck rejectAck(std::uint64_t batchSeq,
                        const std::string& adapterId,
                        const std::string& connectionId,
                        const std::string& error) const;
    void appendLiveEvent(const EngineEvent& event);
    void writeSegment(const std::vector<EngineEvent>& events);

    EngineConfig config_;
    mutable std::mutex stateMutex_;
    std::deque<EngineEvent> liveRing_;
    std::vector<SegmentInfo> segments_;
    std::map<std::string, ConnectionCursor> cursors_;
    std::uint64_t nextSessionSeq_ = 1;
    std::uint64_t nextSegmentId_ = 1;
    std::string lastManifestHash_;

    std::mutex queueMutex_;
    std::condition_variable queueCv_;
    std::deque<std::shared_ptr<PendingRequest>> queue_;

    std::atomic<bool> running_{false};
    int serverFd_ = -1;
    std::thread acceptThread_;
    std::thread sequencerThread_;
};

} // namespace tape_engine
