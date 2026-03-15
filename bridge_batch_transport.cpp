#include "bridge_batch_transport.h"

namespace bridge_batch {

void FakeTransport::failNext(std::string error) {
    scheduledFailures_.push_back(std::move(error));
}

bool FakeTransport::sendFrame(const std::vector<std::uint8_t>& frame, std::string* error) {
    ++attemptedSendCount_;
    if (!scheduledFailures_.empty()) {
        if (error != nullptr) {
            *error = scheduledFailures_.front();
        }
        scheduledFailures_.pop_front();
        return false;
    }
    deliveredFrames_.push_back(frame);
    return true;
}

std::size_t FakeTransport::attemptedSendCount() const {
    return attemptedSendCount_;
}

const std::vector<std::vector<std::uint8_t>>& FakeTransport::deliveredFrames() const {
    return deliveredFrames_;
}

Sender::Sender(Transport& transport)
    : transport_(transport) {}

PublishResult Sender::publish(const Batch& batch) {
    PublishResult result;
    if (!pendingBatches_.empty()) {
        pendingBatches_.push_back(batch);
        result.queuedForRetry = true;
        result.pendingBatchCount = pendingBatches_.size();
        result.error = "pending batches must be drained before sending new work";
        return result;
    }

    std::string error;
    if (transmit(batch, &error)) {
        result.delivered = true;
        return result;
    }

    pendingBatches_.push_back(batch);
    result.queuedForRetry = true;
    result.pendingBatchCount = pendingBatches_.size();
    result.error = std::move(error);
    return result;
}

DrainResult Sender::drainPending() {
    DrainResult result;
    while (!pendingBatches_.empty()) {
        std::string error;
        if (!transmit(pendingBatches_.front(), &error)) {
            result.blocked = true;
            result.error = std::move(error);
            break;
        }
        pendingBatches_.pop_front();
        ++result.deliveredCount;
    }
    result.pendingBatchCount = pendingBatches_.size();
    return result;
}

std::size_t Sender::pendingBatchCount() const {
    return pendingBatches_.size();
}

const std::deque<Batch>& Sender::pendingBatches() const {
    return pendingBatches_;
}

bool Sender::transmit(const Batch& batch, std::string* error) {
    return transport_.sendFrame(encodeFrame(batch), error);
}

} // namespace bridge_batch
