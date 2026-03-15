#pragma once

#include "bridge_batch_codec.h"
#include "tape_engine_protocol.h"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <filesystem>
#include <future>
#include <optional>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

namespace tape_engine {

struct EngineConfig {
    std::string socketPath = "/tmp/tape-engine.sock";
    std::filesystem::path dataDir = std::filesystem::temp_directory_path() / "tape-engine";
    std::string instrumentId = "ib:STK:SMART:USD:INTC";
    std::size_t ringCapacity = 4096;
    std::size_t dedupeWindowSize = 8192;
};

struct EngineEvent {
    std::uint64_t sessionSeq = 0;
    std::uint64_t revisionId = 0;
    std::uint64_t sourceSeq = 0;
    std::string eventKind;
    std::string adapterId;
    std::string connectionId;
    std::string instrumentId;
    std::uint64_t tsEngineNs = 0;
    BridgeOutboxRecord bridgeRecord;
    std::uint64_t gapStartSourceSeq = 0;
    std::uint64_t gapEndSourceSeq = 0;
    std::uint64_t resetPreviousSourceSeq = 0;
    std::uint64_t resetSourceSeq = 0;
};

struct SegmentInfo {
    std::uint64_t segmentId = 0;
    std::uint64_t revisionId = 0;
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
    std::uint64_t nextRevisionId = 1;
    std::uint64_t latestFrozenRevisionId = 0;
    std::uint64_t latestFrozenSessionSeq = 0;
    std::size_t writerBacklogSegments = 0;
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
        std::promise<std::vector<std::uint8_t>> promise;
    };

    struct PendingSegment {
        std::uint64_t revisionId = 0;
        std::vector<EngineEvent> events;
    };

    struct ConnectionCursor {
        std::uint64_t lastAcceptedSourceSeq = 0;
        bool hasLastAccepted = false;
        std::deque<std::uint64_t> recentSourceSeqs;
        std::unordered_set<std::uint64_t> recentSourceSeqSet;
    };

    void acceptLoop();
    void sequencerLoop();
    void writerLoop();
    std::vector<std::uint8_t> processRequest(const std::vector<std::uint8_t>& frame);
    IngestAck processIngestFrame(const std::vector<std::uint8_t>& frame);
    QueryResponse processQueryFrame(const std::vector<std::uint8_t>& frame);
    IngestAck rejectAck(std::uint64_t batchSeq,
                        const std::string& adapterId,
                        const std::string& connectionId,
                        const std::string& error) const;
    QueryResponse rejectResponse(const QueryRequest& request,
                                 const std::string& error) const;
    void appendLiveEvent(const EngineEvent& event);
    void writeSegment(const PendingSegment& segment);
    void enqueueSegment(PendingSegment segment);
    std::string resolveInstrumentId(const BridgeOutboxRecord& record) const;
    void rememberSourceSeqUnlocked(ConnectionCursor& cursor, std::uint64_t sourceSeq);
    void resetSourceSeqWindowUnlocked(ConnectionCursor& cursor);
    std::uint64_t resolveFrozenRevisionUnlocked(std::uint64_t requestedRevisionId) const;
    std::vector<json> loadAllEventsUnlocked(std::uint64_t frozenRevisionId) const;
    std::vector<json> mergedEventsUnlocked(std::uint64_t frozenRevisionId, bool includeLiveTail) const;
    std::vector<json> filterEventsByRangeUnlocked(std::uint64_t fromSessionSeq,
                                                  std::uint64_t toSessionSeq,
                                                  std::size_t limit,
                                                  std::uint64_t frozenRevisionId,
                                                  bool includeLiveTail) const;
    std::vector<json> filterEventsByAnchorUnlocked(std::uint64_t traceId,
                                                   long long orderId,
                                                   long long permId,
                                                   const std::string& execId,
                                                   std::size_t limit,
                                                   std::uint64_t frozenRevisionId,
                                                   bool includeLiveTail) const;
    json buildReplaySnapshotUnlocked(std::uint64_t targetSessionSeq,
                                     std::size_t depthLimit,
                                     std::uint64_t frozenRevisionId,
                                     bool includeLiveTail) const;

    EngineConfig config_;
    mutable std::mutex stateMutex_;
    std::deque<EngineEvent> liveRing_;
    std::vector<SegmentInfo> segments_;
    std::map<std::string, ConnectionCursor> cursors_;
    std::uint64_t nextSessionSeq_ = 1;
    std::uint64_t nextSegmentId_ = 1;
    std::uint64_t nextRevisionId_ = 1;
    std::uint64_t latestFrozenRevisionId_ = 0;
    std::uint64_t latestFrozenSessionSeq_ = 0;
    std::string lastManifestHash_;
    std::string writerFailure_;

    std::mutex queueMutex_;
    std::condition_variable queueCv_;
    std::deque<std::shared_ptr<PendingRequest>> queue_;

    mutable std::mutex writerMutex_;
    std::condition_variable writerCv_;
    std::deque<PendingSegment> writerQueue_;

    std::atomic<bool> running_{false};
    int serverFd_ = -1;
    std::thread acceptThread_;
    std::thread sequencerThread_;
    std::thread writerThread_;
};

} // namespace tape_engine
