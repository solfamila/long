#include "bridge_batch_transport.h"
#include "tape_engine_protocol.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <system_error>

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

namespace bridge_batch {

namespace {

std::string makeErrnoMessage(const std::string& action) {
    return action + ": " + std::strerror(errno);
}

std::vector<std::uint8_t> readExact(int fd, std::size_t bytes) {
    std::vector<std::uint8_t> buffer(bytes);
    std::size_t offset = 0;
    while (offset < bytes) {
        const ssize_t readCount = ::read(fd, buffer.data() + offset, bytes - offset);
        if (readCount == 0) {
            throw std::runtime_error("unexpected EOF while reading tape-engine ack");
        }
        if (readCount < 0) {
            throw std::runtime_error(makeErrnoMessage("read"));
        }
        offset += static_cast<std::size_t>(readCount);
    }
    return buffer;
}

std::vector<std::uint8_t> readFramedAck(int fd) {
    const std::vector<std::uint8_t> prefix = readExact(fd, 4);
    const std::uint32_t payloadSize =
        (static_cast<std::uint32_t>(prefix[0]) << 24) |
        (static_cast<std::uint32_t>(prefix[1]) << 16) |
        (static_cast<std::uint32_t>(prefix[2]) << 8) |
        static_cast<std::uint32_t>(prefix[3]);
    std::vector<std::uint8_t> frame = prefix;
    const std::vector<std::uint8_t> payload = readExact(fd, payloadSize);
    frame.insert(frame.end(), payload.begin(), payload.end());
    return frame;
}

} // namespace

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

UnixDomainSocketTransport::UnixDomainSocketTransport(std::string socketPath)
    : socketPath_(std::move(socketPath)) {}

bool UnixDomainSocketTransport::sendFrame(const std::vector<std::uint8_t>& frame, std::string* error) {
    if (socketPath_.empty()) {
        if (error != nullptr) {
            *error = "bridge socket path is empty";
        }
        return false;
    }
    if (frame.empty()) {
        if (error != nullptr) {
            *error = "bridge frame is empty";
        }
        return false;
    }

    int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        if (error != nullptr) {
            *error = makeErrnoMessage("socket");
        }
        return false;
    }

    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    if (socketPath_.size() >= sizeof(address.sun_path)) {
        if (error != nullptr) {
            *error = "bridge socket path is too long";
        }
        ::close(fd);
        return false;
    }
    std::strncpy(address.sun_path, socketPath_.c_str(), sizeof(address.sun_path) - 1);

    if (::connect(fd, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0) {
        if (error != nullptr) {
            *error = makeErrnoMessage("connect");
        }
        ::close(fd);
        return false;
    }

    std::size_t sent = 0;
    while (sent < frame.size()) {
        const ssize_t wrote = ::send(fd,
                                     frame.data() + sent,
                                     frame.size() - sent,
                                     0);
        if (wrote < 0) {
            if (error != nullptr) {
                *error = makeErrnoMessage("send");
            }
            ::close(fd);
            return false;
        }
        sent += static_cast<std::size_t>(wrote);
    }

    ::shutdown(fd, SHUT_WR);
    try {
        const tape_engine::IngestAck ack = tape_engine::decodeAckFrame(readFramedAck(fd));
        if (ack.status != "accepted") {
            if (error != nullptr) {
                *error = ack.error.empty() ? "tape-engine rejected batch" : ack.error;
            }
            ::close(fd);
            return false;
        }
    } catch (const std::exception& ackError) {
        if (error != nullptr) {
            *error = ackError.what();
        }
        ::close(fd);
        return false;
    }
    ::close(fd);
    return true;
}

const std::string& UnixDomainSocketTransport::socketPath() const {
    return socketPath_;
}

bool recordTypeRequiresImmediateFlush(std::string_view recordType) {
    return recordType == "order_intent" ||
           recordType == "fill_execution" ||
           recordType == "cancel_request" ||
           recordType == "order_reject";
}

PreparedBatch prepareBatch(const std::vector<BridgeOutboxRecord>& records,
                           const BuildOptions& options,
                           const BatchPolicy& policy) {
    PreparedBatch prepared;
    if (records.empty()) {
        prepared.batch = buildBatch({}, options);
        return prepared;
    }

    std::vector<BridgeOutboxRecord> selected;
    selected.reserve(std::min(policy.maxRecords, records.size()));

    for (const auto& record : records) {
        if (!selected.empty() && selected.size() >= policy.maxRecords) {
            prepared.reachedRecordLimit = true;
            break;
        }

        selected.push_back(record);
        const Batch candidate = buildBatch(selected, options);
        const std::size_t payloadBytes = encodePayload(candidate).size();

        if (selected.size() > 1 && payloadBytes > policy.maxPayloadBytes) {
            selected.pop_back();
            prepared.reachedPayloadLimit = true;
            break;
        }

        prepared.batch = candidate;
        prepared.payloadBytes = payloadBytes;
        prepared.immediateFlush = prepared.immediateFlush || recordTypeRequiresImmediateFlush(record.recordType);
    }

    if (selected.size() >= policy.maxRecords && selected.size() < records.size()) {
        prepared.reachedRecordLimit = true;
    }

    if (prepared.batch.records.empty()) {
        prepared.batch = buildBatch({records.front()}, options);
        prepared.payloadBytes = encodePayload(prepared.batch).size();
        prepared.immediateFlush = recordTypeRequiresImmediateFlush(records.front().recordType);
    }

    return prepared;
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
        result.deliveredBatches.push_back(pendingBatches_.front());
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
