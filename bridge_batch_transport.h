#pragma once

#include "bridge_batch_codec.h"

#include <cstddef>
#include <deque>
#include <string>
#include <string_view>
#include <vector>

namespace bridge_batch {

class Transport {
public:
    virtual ~Transport() = default;
    virtual bool sendFrame(const std::vector<std::uint8_t>& frame, std::string* error) = 0;
};

class FakeTransport final : public Transport {
public:
    void failNext(std::string error = "transport unavailable");
    bool sendFrame(const std::vector<std::uint8_t>& frame, std::string* error) override;

    std::size_t attemptedSendCount() const;
    const std::vector<std::vector<std::uint8_t>>& deliveredFrames() const;

private:
    std::deque<std::string> scheduledFailures_;
    std::vector<std::vector<std::uint8_t>> deliveredFrames_;
    std::size_t attemptedSendCount_ = 0;
};

class UnixDomainSocketTransport final : public Transport {
public:
    explicit UnixDomainSocketTransport(std::string socketPath);

    bool sendFrame(const std::vector<std::uint8_t>& frame, std::string* error) override;

    const std::string& socketPath() const;

private:
    std::string socketPath_;
};

struct BatchPolicy {
    std::size_t maxRecords = 64;
    std::size_t maxPayloadBytes = 64 * 1024;
};

struct PreparedBatch {
    Batch batch;
    std::size_t payloadBytes = 0;
    bool immediateFlush = false;
    bool reachedRecordLimit = false;
    bool reachedPayloadLimit = false;
};

bool recordTypeRequiresImmediateFlush(std::string_view recordType);
PreparedBatch prepareBatch(const std::vector<BridgeOutboxRecord>& records,
                           const BuildOptions& options,
                           const BatchPolicy& policy = {});

struct PublishResult {
    bool delivered = false;
    bool queuedForRetry = false;
    std::size_t pendingBatchCount = 0;
    std::string error;
};

struct DrainResult {
    std::size_t deliveredCount = 0;
    std::size_t pendingBatchCount = 0;
    bool blocked = false;
    std::string error;
    std::vector<Batch> deliveredBatches;
};

class Sender {
public:
    explicit Sender(Transport& transport);

    PublishResult publish(const Batch& batch);
    DrainResult drainPending();

    std::size_t pendingBatchCount() const;
    const std::deque<Batch>& pendingBatches() const;

private:
    bool transmit(const Batch& batch, std::string* error);

    Transport& transport_;
    std::deque<Batch> pendingBatches_;
};

} // namespace bridge_batch
