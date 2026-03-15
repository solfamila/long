#pragma once

#include "bridge_batch_codec.h"
#include "tape_engine_protocol.h"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <filesystem>
#include <future>
#include <memory>
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
    std::string artifactsFileName;
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

struct OrderAnchorRecord {
    std::uint64_t anchorId = 0;
    std::uint64_t revisionId = 0;
    std::uint64_t sessionSeq = 0;
    std::uint64_t tsEngineNs = 0;
    std::string eventKind;
    std::string instrumentId;
    BridgeAnchorIdentity anchor;
    std::string note;
};

struct ProtectedWindowRecord {
    std::uint64_t windowId = 0;
    std::uint64_t revisionId = 0;
    std::uint64_t logicalIncidentId = 0;
    std::uint64_t anchorSessionSeq = 0;
    std::uint64_t startEngineNs = 0;
    std::uint64_t endEngineNs = 0;
    std::string reason;
    std::string instrumentId;
    BridgeAnchorIdentity anchor;
};

struct FindingRecord {
    std::uint64_t findingId = 0;
    std::uint64_t revisionId = 0;
    std::uint64_t logicalIncidentId = 0;
    std::uint64_t incidentRevisionId = 0;
    std::string kind;
    std::string severity;
    double confidence = 0.0;
    std::uint64_t firstSessionSeq = 0;
    std::uint64_t lastSessionSeq = 0;
    std::uint64_t tsEngineNs = 0;
    std::string instrumentId;
    std::string title;
    std::string summary;
    bool overlapsOrder = false;
    BridgeAnchorIdentity overlappingAnchor;
};

struct IncidentRecord {
    std::uint64_t logicalIncidentId = 0;
    std::uint64_t incidentRevisionId = 0;
    std::uint64_t revisionId = 0;
    std::string kind;
    std::string severity;
    double confidence = 0.0;
    double score = 0.0;
    std::uint64_t firstSessionSeq = 0;
    std::uint64_t lastSessionSeq = 0;
    std::uint64_t promotedByFindingId = 0;
    std::uint64_t latestFindingId = 0;
    std::uint64_t findingCount = 0;
    std::uint64_t tsEngineNs = 0;
    std::string instrumentId;
    std::string title;
    std::string summary;
    bool overlapsOrder = false;
    BridgeAnchorIdentity overlappingAnchor;
};

enum class RequestKind {
    Ingest,
    Query,
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
        RequestKind kind = RequestKind::Ingest;
        std::vector<std::uint8_t> frame;
        std::promise<std::vector<std::uint8_t>> promise;
    };

    struct PendingSegment {
        std::uint64_t revisionId = 0;
        std::vector<EngineEvent> events;
        std::vector<OrderAnchorRecord> orderAnchors;
        std::vector<ProtectedWindowRecord> protectedWindows;
        std::vector<FindingRecord> findings;
        std::vector<IncidentRecord> incidents;
    };

    struct ConnectionCursor {
        std::uint64_t lastAcceptedSourceSeq = 0;
        bool hasLastAccepted = false;
        std::deque<std::uint64_t> recentSourceSeqs;
        std::unordered_set<std::uint64_t> recentSourceSeqSet;
    };

    struct QuerySnapshot {
        std::filesystem::path dataDir;
        std::string socketPath;
        std::string instrumentId;
        std::string lastManifestHash;
        std::string writerFailure;
        std::uint64_t nextSessionSeq = 1;
        std::uint64_t nextSegmentId = 1;
        std::uint64_t nextRevisionId = 1;
        std::uint64_t latestFrozenRevisionId = 0;
        std::uint64_t latestFrozenSessionSeq = 0;
        std::vector<SegmentInfo> segments;
        std::vector<EngineEvent> liveEvents;
        std::vector<OrderAnchorRecord> orderAnchors;
        std::vector<ProtectedWindowRecord> protectedWindows;
        std::vector<FindingRecord> findings;
        std::vector<IncidentRecord> incidents;
    };

    struct FrozenArtifacts {
        std::vector<OrderAnchorRecord> orderAnchors;
        std::vector<ProtectedWindowRecord> protectedWindows;
        std::vector<FindingRecord> findings;
        std::vector<IncidentRecord> incidents;
    };

    struct AnalyzerBookState {
        struct DisplayInstabilitySideState {
            double trackedPrice = 0.0;
            bool sawRecentRemoval = false;
            std::uint64_t removalSessionSeq = 0;
            std::uint64_t firstCycleSessionSeq = 0;
            std::size_t completedCycles = 0;
        };

        struct ActiveFillWatch {
            BridgeAnchorIdentity anchor;
            std::string instrumentId;
            bool isBuy = true;
            double fillPrice = 0.0;
            std::uint64_t fillSessionSeq = 0;
            std::uint64_t fillTsEngineNs = 0;
            std::uint64_t expiryTsEngineNs = 0;
        };

        double bidTickPrice = 0.0;
        double askTickPrice = 0.0;
        double lastTradePrice = 0.0;
        std::vector<BookLevel> bidBook;
        std::vector<BookLevel> askBook;
        double lastEffectiveBid = 0.0;
        double lastEffectiveAsk = 0.0;
        double lastEffectiveBidSize = 0.0;
        double lastEffectiveAskSize = 0.0;
        int tradePressureSide = 0;
        std::size_t tradePressureStreakCount = 0;
        std::uint64_t tradePressureFirstSessionSeq = 0;
        std::uint64_t tradePressureLastSessionSeq = 0;
        double tradePressureReferencePrice = 0.0;
        DisplayInstabilitySideState askDisplayInstability;
        DisplayInstabilitySideState bidDisplayInstability;
        std::optional<ActiveFillWatch> activeFillWatch;
        bool hasInside = false;
    };

    struct DeferredAnalyzerTask {
        std::string analyzerName;
        std::uint64_t sourceRevisionId = 0;
        std::uint64_t protectedWindowId = 0;
        std::uint64_t logicalIncidentId = 0;
        std::string instrumentId;
        std::string reason;
        BridgeAnchorIdentity anchor;
    };

    void acceptLoop();
    void handleClientConnection(int clientFd);
    void sequencerLoop();
    void replayLoop();
    void writerLoop();
    void deferredAnalyzerLoop();
    IngestAck processIngestFrame(const std::vector<std::uint8_t>& frame);
    QueryResponse processQueryFrame(const std::vector<std::uint8_t>& frame);
    IngestAck rejectAck(std::uint64_t batchSeq,
                        const std::string& adapterId,
                        const std::string& connectionId,
                        const std::string& error) const;
    QueryResponse rejectResponse(const QueryRequest& request,
                                 const std::string& error) const;
    void appendLiveEvent(const EngineEvent& event);
    bool restoreFrozenState(std::string* error);
    void writeSegment(const PendingSegment& segment);
    void enqueueSegment(PendingSegment segment);
    void enqueueDeferredAnalyzerTask(DeferredAnalyzerTask task);
    std::string resolveInstrumentId(const BridgeOutboxRecord& record) const;
    void rememberSourceSeqUnlocked(ConnectionCursor& cursor, std::uint64_t sourceSeq);
    void resetSourceSeqWindowUnlocked(ConnectionCursor& cursor);
    void updatePhase3StateUnlocked(const EngineEvent& event);
    void recordOrderAnchorUnlocked(const EngineEvent& event);
    void addProtectedWindowUnlocked(const EngineEvent& event,
                                    const std::string& reason,
                                    const BridgeAnchorIdentity& anchor,
                                    std::uint64_t logicalIncidentId = 0);
    void recordFindingUnlocked(const EngineEvent& event,
                               const std::string& kind,
                               const std::string& severity,
                               double confidence,
                               const std::string& title,
                               const std::string& summary,
                               const BridgeAnchorIdentity& overlappingAnchor,
                               bool overlapsOrder);
    void recordFindingRangeUnlocked(std::uint64_t revisionId,
                                    const std::string& kind,
                                    const std::string& severity,
                                    double confidence,
                                    std::uint64_t firstSessionSeq,
                                    std::uint64_t lastSessionSeq,
                                    std::uint64_t tsEngineNs,
                                    const std::string& instrumentId,
                                    const std::string& title,
                                    const std::string& summary,
                                    const BridgeAnchorIdentity& overlappingAnchor,
                                    bool overlapsOrder);
    void recordIncidentUnlocked(const EngineEvent& event,
                                const FindingRecord& finding,
                                const BridgeAnchorIdentity& overlappingAnchor,
                                bool overlapsOrder);
    void updateAnalyzerBookUnlocked(const EngineEvent& event);
    BridgeAnchorIdentity findOverlappingOrderAnchorUnlocked(std::uint64_t tsEngineNs) const;
    QuerySnapshot captureQuerySnapshot() const;
    std::uint64_t resolveFrozenRevision(const QuerySnapshot& snapshot, std::uint64_t requestedRevisionId) const;
    FrozenArtifacts loadFrozenArtifacts(const QuerySnapshot& snapshot,
                                        std::uint64_t frozenRevisionId) const;
    std::vector<json> loadEvents(const QuerySnapshot& snapshot,
                                 std::uint64_t frozenRevisionId,
                                 std::uint64_t fromSessionSeq,
                                 std::uint64_t throughSessionSeq) const;
    std::vector<json> mergedEvents(const QuerySnapshot& snapshot,
                                   std::uint64_t frozenRevisionId,
                                   bool includeLiveTail,
                                   std::uint64_t fromSessionSeq,
                                   std::uint64_t throughSessionSeq) const;
    std::vector<json> filterEventsByRange(const QuerySnapshot& snapshot,
                                          std::uint64_t fromSessionSeq,
                                          std::uint64_t toSessionSeq,
                                          std::size_t limit,
                                          std::uint64_t frozenRevisionId,
                                          bool includeLiveTail) const;
    std::vector<json> filterEventsByAnchor(const QuerySnapshot& snapshot,
                                           std::uint64_t traceId,
                                           long long orderId,
                                           long long permId,
                                           const std::string& execId,
                                           std::size_t limit,
                                           std::uint64_t frozenRevisionId,
                                           bool includeLiveTail) const;
    json buildOrderCaseSummary(const QuerySnapshot& snapshot,
                               const std::vector<json>& events,
                               std::uint64_t traceId,
                               long long orderId,
                               long long permId,
                               const std::string& execId,
                               std::uint64_t frozenRevisionId,
                               bool includeLiveTail) const;
    std::vector<json> filterEventsByProtectedWindow(const QuerySnapshot& snapshot,
                                                    std::uint64_t windowId,
                                                    std::size_t limit,
                                                    std::uint64_t frozenRevisionId,
                                                    bool includeLiveTail,
                                                    json* selectedWindowSummary) const;
    json buildReplaySnapshot(const QuerySnapshot& snapshot,
                             std::uint64_t targetSessionSeq,
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
    AnalyzerBookState analyzerBookState_;
    std::vector<OrderAnchorRecord> orderAnchors_;
    std::vector<ProtectedWindowRecord> protectedWindows_;
    std::vector<FindingRecord> findings_;
    std::vector<IncidentRecord> incidents_;
    std::uint64_t nextOrderAnchorId_ = 1;
    std::uint64_t nextProtectedWindowId_ = 1;
    std::uint64_t nextFindingId_ = 1;
    std::uint64_t nextLogicalIncidentId_ = 1;
    std::uint64_t nextIncidentRevisionId_ = 1;

    std::mutex ingestQueueMutex_;
    std::condition_variable ingestQueueCv_;
    std::deque<std::shared_ptr<PendingRequest>> ingestQueue_;

    std::mutex queryQueueMutex_;
    std::condition_variable queryQueueCv_;
    std::deque<std::shared_ptr<PendingRequest>> queryQueue_;

    mutable std::mutex writerMutex_;
    std::condition_variable writerCv_;
    std::deque<PendingSegment> writerQueue_;

    std::mutex clientThreadsMutex_;
    std::vector<std::thread> clientThreads_;

    std::mutex deferredAnalyzerMutex_;
    std::condition_variable deferredAnalyzerCv_;
    std::deque<DeferredAnalyzerTask> deferredAnalyzerQueue_;

    std::atomic<bool> running_{false};
    int serverFd_ = -1;
    std::thread acceptThread_;
    std::thread sequencerThread_;
    std::thread replayThread_;
    std::thread writerThread_;
    std::thread deferredAnalyzerThread_;
};

} // namespace tape_engine
