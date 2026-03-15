#include "tape_engine_ingest.h"

#include <nlohmann/json.hpp>

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <poll.h>
#include <stdexcept>
#include <string_view>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <utility>

namespace fs = std::filesystem;
using json = nlohmann::json;

namespace tape_engine {

namespace {

std::string readString(const json& payload, std::string_view key) {
    const auto it = payload.find(key);
    if (it == payload.end() || !it->is_string()) {
        return {};
    }
    return it->get<std::string>();
}

std::uint64_t decodeFrameSizePrefix(const std::vector<std::uint8_t>& frame) {
    if (frame.size() < 4) {
        throw std::runtime_error("bridge batch frame too short");
    }
    return (static_cast<std::uint32_t>(frame[0]) << 24) |
           (static_cast<std::uint32_t>(frame[1]) << 16) |
           (static_cast<std::uint32_t>(frame[2]) << 8) |
           static_cast<std::uint32_t>(frame[3]);
}

BridgeAnchor anchorFromJson(const json& payload) {
    BridgeAnchor anchor;
    anchor.traceId = payload.value("trace_id", 0ULL);
    anchor.orderId = payload.value("order_id", 0LL);
    anchor.permId = payload.value("perm_id", 0LL);
    anchor.execId = payload.value("exec_id", std::string());
    return anchor;
}

BridgeRecord recordFromJson(const json& payload) {
    BridgeRecord record;
    record.sourceSeq = payload.value("source_seq", 0ULL);
    record.recordType = payload.value("record_type", std::string());
    record.source = payload.value("source", std::string());
    record.symbol = payload.value("symbol", std::string());
    record.side = payload.value("side", std::string());
    record.anchor = anchorFromJson(payload.value("anchor", json::object()));
    record.fallbackState = payload.value("fallback_state", std::string());
    record.fallbackReason = payload.value("fallback_reason", std::string());
    record.note = payload.value("note", std::string());
    record.wallTime = payload.value("wall_time", std::string());
    return record;
}

BridgeBatch batchFromJson(const json& payload) {
    BridgeBatch batch;
    batch.header.version = payload.value("version", 0U);
    batch.header.schema = payload.value("schema", std::string());
    batch.header.producer = payload.value("producer", std::string());
    batch.header.transport = payload.value("transport", std::string());
    batch.header.subsystem = payload.value("subsystem", std::string());
    batch.header.category = payload.value("category", std::string());
    batch.header.senderLabel = payload.value("queue_label", std::string());
    batch.header.senderQos = payload.value("queue_qos", std::string());
    batch.header.appSessionId = payload.value("app_session_id", std::string());
    batch.header.runtimeSessionId = payload.value("runtime_session_id", std::string());
    batch.header.adapterId = readString(payload, "adapter_id");
    if (batch.header.adapterId.empty()) {
        batch.header.adapterId = batch.header.senderLabel;
    }
    batch.header.connectionId = readString(payload, "connection_id");
    if (batch.header.connectionId.empty()) {
        batch.header.connectionId = batch.header.runtimeSessionId;
    }
    batch.header.flushReason = payload.value("flush_reason", std::string());
    batch.header.batchSeq = payload.value("batch_seq", 0ULL);
    batch.header.firstSourceSeq = payload.value("first_source_seq", 0ULL);
    batch.header.lastSourceSeq = payload.value("last_source_seq", 0ULL);
    batch.header.recordCount = payload.value("record_count", 0ULL);

    const json records = payload.value("records", json::array());
    if (!records.is_array()) {
        throw std::runtime_error("bridge batch records payload must be an array");
    }
    batch.records.reserve(records.size());
    for (const auto& item : records) {
        batch.records.push_back(recordFromJson(item));
    }
    return batch;
}

void validateBatch(const BridgeBatch& batch) {
    if (batch.header.version != kBridgeBatchWireVersion) {
        throw std::runtime_error("unsupported bridge batch version");
    }
    if (batch.header.schema != kBridgeBatchSchemaName) {
        throw std::runtime_error("unexpected bridge batch schema");
    }
    if (batch.header.producer != kBridgeBatchProducerName) {
        throw std::runtime_error("unexpected bridge batch producer");
    }
    if (batch.header.transport != kBridgeBatchTransportName) {
        throw std::runtime_error("unexpected bridge batch transport");
    }
    if (batch.header.appSessionId.empty()) {
        throw std::runtime_error("bridge batch missing app_session_id");
    }
    if (batch.header.runtimeSessionId.empty()) {
        throw std::runtime_error("bridge batch missing runtime_session_id");
    }
    if (batch.header.adapterId.empty()) {
        throw std::runtime_error("bridge batch missing adapter_id");
    }
    if (batch.header.connectionId.empty()) {
        throw std::runtime_error("bridge batch missing connection_id");
    }
    if (batch.header.batchSeq == 0) {
        throw std::runtime_error("bridge batch missing batch_seq");
    }
    if (batch.records.empty()) {
        throw std::runtime_error("bridge batch has no records");
    }
    if (batch.header.recordCount != batch.records.size()) {
        throw std::runtime_error("bridge batch record_count does not match records size");
    }
    if (batch.header.firstSourceSeq != batch.records.front().sourceSeq) {
        throw std::runtime_error("bridge batch first_source_seq does not match first record");
    }
    if (batch.header.lastSourceSeq != batch.records.back().sourceSeq) {
        throw std::runtime_error("bridge batch last_source_seq does not match last record");
    }

    std::uint64_t previousSourceSeq = 0;
    for (std::size_t index = 0; index < batch.records.size(); ++index) {
        const BridgeRecord& record = batch.records[index];
        if (record.sourceSeq == 0) {
            throw std::runtime_error("bridge batch record missing source_seq");
        }
        if (index > 0 && record.sourceSeq != previousSourceSeq + 1) {
            throw std::runtime_error("bridge batch source_seq continuity broken");
        }
        if (record.recordType.empty()) {
            throw std::runtime_error("bridge batch record missing record_type");
        }
        if (record.source.empty()) {
            throw std::runtime_error("bridge batch record missing source");
        }
        if (record.symbol.empty()) {
            throw std::runtime_error("bridge batch record missing symbol");
        }
        previousSourceSeq = record.sourceSeq;
    }
}

BridgeBatch decodePayload(const std::vector<std::uint8_t>& payload) {
    if (payload.empty()) {
        throw std::runtime_error("bridge batch payload is empty");
    }
    BridgeBatch batch = batchFromJson(json::from_msgpack(payload));
    validateBatch(batch);
    return batch;
}

void closeFd(int& fd) {
    if (fd >= 0) {
        ::close(fd);
        fd = -1;
    }
}

bool readExactly(int fd,
                 void* buffer,
                 std::size_t bytesToRead,
                 bool* eof,
                 std::string* error,
                 const std::atomic<bool>& stopRequested) {
    *eof = false;
    auto* dst = static_cast<std::uint8_t*>(buffer);
    std::size_t totalRead = 0;
    while (totalRead < bytesToRead) {
        const ssize_t bytesRead = ::read(fd, dst + totalRead, bytesToRead - totalRead);
        if (bytesRead == 0) {
            if (totalRead == 0) {
                *eof = true;
                return true;
            }
            if (error) {
                *error = "unexpected EOF while reading bridge batch";
            }
            return false;
        }
        if (bytesRead < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (stopRequested.load()) {
                *eof = true;
                return true;
            }
            if (error) {
                *error = std::string("socket read failed: ") + std::strerror(errno);
            }
            return false;
        }
        totalRead += static_cast<std::size_t>(bytesRead);
    }
    return true;
}

} // namespace

InMemoryLog::AppendResult InMemoryLog::append(const BridgeBatch& batch) {
    std::lock_guard<std::mutex> lock(mutex_);

    AppendResult result;
    result.acceptedRecordCount = batch.records.size();
    result.batch.adapterId = batch.header.adapterId;
    result.batch.connectionId = batch.header.connectionId;
    result.batch.appSessionId = batch.header.appSessionId;
    result.batch.runtimeSessionId = batch.header.runtimeSessionId;
    result.batch.batchSeq = batch.header.batchSeq;
    result.batch.firstSourceSeq = batch.header.firstSourceSeq;
    result.batch.lastSourceSeq = batch.header.lastSourceSeq;
    result.batch.acceptedRecordCount = batch.records.size();

    for (const auto& record : batch.records) {
        IngestLogRecord entry;
        entry.sessionSeq = nextSessionSeq_++;
        entry.batchSeq = batch.header.batchSeq;
        entry.sourceSeq = record.sourceSeq;
        entry.adapterId = batch.header.adapterId;
        entry.connectionId = batch.header.connectionId;
        entry.recordType = record.recordType;
        entry.source = record.source;
        entry.symbol = record.symbol;
        entry.side = record.side;
        entry.anchor = record.anchor;
        entry.note = record.note;
        entry.wallTime = record.wallTime;
        records_.push_back(entry);
        if (result.firstSessionSeq == 0) {
            result.firstSessionSeq = entry.sessionSeq;
        }
        result.lastSessionSeq = entry.sessionSeq;
    }

    batches_.push_back(result.batch);
    return result;
}

IngestLogSnapshot InMemoryLog::snapshot() const {
    std::lock_guard<std::mutex> lock(mutex_);
    IngestLogSnapshot snapshot;
    snapshot.batches = batches_;
    snapshot.records = records_;
    snapshot.nextSessionSeq = nextSessionSeq_;
    return snapshot;
}

IngestService::IngestService(InMemoryLog& log)
    : log_(log) {}

bool IngestService::ingestFrame(const std::vector<std::uint8_t>& frame, std::string* error) {
    try {
        BridgeBatch batch = decodeFrame(frame);
        log_.append(batch);
        std::lock_guard<std::mutex> lock(mutex_);
        ++stats_.acceptedBatches;
        return true;
    } catch (const std::exception& ex) {
        recordRejected(ex.what());
        if (error) {
            *error = ex.what();
        }
        return false;
    }
}

void IngestService::recordRejected(const std::string& error) {
    std::lock_guard<std::mutex> lock(mutex_);
    ++stats_.rejectedBatches;
    stats_.lastError = error;
}

ServiceStats IngestService::stats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return stats_;
}

Daemon::Daemon(DaemonOptions options, IngestService& service)
    : options_(std::move(options)),
      service_(service) {}

Daemon::~Daemon() {
    requestStop();
    cleanupSocket();
}

bool Daemon::openListener(std::string* error) {
    if (options_.socketPath.empty()) {
        if (error) {
            *error = "socket path is required";
        }
        return false;
    }

    const fs::path socketPath(options_.socketPath);
    const fs::path socketDir = socketPath.parent_path();
    if (!socketDir.empty()) {
        std::error_code createError;
        fs::create_directories(socketDir, createError);
        if (createError) {
            if (error) {
                *error = "failed to create socket directory: " + createError.message();
            }
            return false;
        }
    }

    std::error_code removeError;
    fs::remove(socketPath, removeError);

    int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        if (error) {
            *error = std::string("failed to create unix socket: ") + std::strerror(errno);
        }
        return false;
    }

    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    if (options_.socketPath.size() >= sizeof(address.sun_path)) {
        if (error) {
            *error = "unix socket path is too long";
        }
        ::close(fd);
        return false;
    }
    std::snprintf(address.sun_path, sizeof(address.sun_path), "%s", options_.socketPath.c_str());

    const socklen_t addressSize =
        static_cast<socklen_t>(offsetof(sockaddr_un, sun_path) + std::strlen(address.sun_path) + 1);
    if (::bind(fd, reinterpret_cast<sockaddr*>(&address), addressSize) != 0) {
        if (error) {
            *error = std::string("failed to bind unix socket: ") + std::strerror(errno);
        }
        ::close(fd);
        return false;
    }

    if (::listen(fd, options_.listenBacklog) != 0) {
        if (error) {
            *error = std::string("failed to listen on unix socket: ") + std::strerror(errno);
        }
        ::close(fd);
        return false;
    }

    {
        std::lock_guard<std::mutex> lock(fdMutex_);
        listenFd_ = fd;
    }
    return true;
}

bool Daemon::handleClient(int clientFd) {
    for (;;) {
        std::vector<std::uint8_t> frame;
        bool eof = false;
        std::string error;
        if (!readNextFrame(clientFd, &frame, &eof, &error)) {
            service_.recordRejected(error);
            return false;
        }
        if (eof) {
            return true;
        }
        if (!service_.ingestFrame(frame, nullptr)) {
            return false;
        }
    }
}

bool Daemon::readNextFrame(int clientFd,
                           std::vector<std::uint8_t>* frame,
                           bool* eof,
                           std::string* error) {
    *eof = false;

    std::uint8_t header[4];
    bool headerEof = false;
    if (!readExactly(clientFd, header, sizeof(header), &headerEof, error, stopRequested_)) {
        return false;
    }
    if (headerEof) {
        *eof = true;
        return true;
    }

    const std::uint64_t payloadSize = (static_cast<std::uint32_t>(header[0]) << 24) |
                                      (static_cast<std::uint32_t>(header[1]) << 16) |
                                      (static_cast<std::uint32_t>(header[2]) << 8) |
                                      static_cast<std::uint32_t>(header[3]);
    if (payloadSize == 0) {
        *error = "bridge batch payload is empty";
        return false;
    }
    if (payloadSize > options_.maxFrameBytes) {
        *error = "bridge batch payload exceeds max frame size";
        return false;
    }

    std::vector<std::uint8_t> payload(payloadSize);
    bool payloadEof = false;
    if (!readExactly(clientFd, payload.data(), payload.size(), &payloadEof, error, stopRequested_)) {
        return false;
    }
    if (payloadEof) {
        *error = "unexpected EOF while reading bridge batch payload";
        return false;
    }

    frame->clear();
    frame->reserve(sizeof(header) + payload.size());
    frame->insert(frame->end(), header, header + sizeof(header));
    frame->insert(frame->end(), payload.begin(), payload.end());
    return true;
}

bool Daemon::run(std::string* error) {
    stopRequested_.store(false);
    if (!openListener(error)) {
        cleanupSocket();
        return false;
    }

    for (;;) {
        if (stopRequested_.load()) {
            break;
        }

        pollfd listener{};
        {
            std::lock_guard<std::mutex> lock(fdMutex_);
            listener.fd = listenFd_;
        }
        if (listener.fd < 0) {
            break;
        }

        listener.events = POLLIN;
        const int pollResult = ::poll(&listener, 1, 100);
        if (pollResult < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (error) {
                *error = std::string("poll failed on unix socket: ") + std::strerror(errno);
            }
            cleanupSocket();
            return false;
        }
        if (pollResult == 0) {
            continue;
        }
        if ((listener.revents & (POLLERR | POLLNVAL)) != 0) {
            if (stopRequested_.load()) {
                break;
            }
            if (error) {
                *error = "unix socket listener entered error state";
            }
            cleanupSocket();
            return false;
        }

        int clientFd = ::accept(listener.fd, nullptr, nullptr);
        if (clientFd < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (stopRequested_.load()) {
                break;
            }
            if (error) {
                *error = std::string("accept failed on unix socket: ") + std::strerror(errno);
            }
            cleanupSocket();
            return false;
        }

        {
            std::lock_guard<std::mutex> lock(fdMutex_);
            activeClientFd_ = clientFd;
        }
        handleClient(clientFd);
        {
            std::lock_guard<std::mutex> lock(fdMutex_);
            closeFd(activeClientFd_);
        }
    }

    cleanupSocket();
    return true;
}

void Daemon::requestStop() {
    stopRequested_.store(true);
    std::lock_guard<std::mutex> lock(fdMutex_);
    closeFd(activeClientFd_);
    closeFd(listenFd_);
}

const DaemonOptions& Daemon::options() const {
    return options_;
}

void Daemon::cleanupSocket() {
    {
        std::lock_guard<std::mutex> lock(fdMutex_);
        closeFd(activeClientFd_);
        closeFd(listenFd_);
    }
    if (!options_.socketPath.empty()) {
        std::error_code removeError;
        fs::remove(fs::path(options_.socketPath), removeError);
    }
}

BridgeBatch decodeFrame(const std::vector<std::uint8_t>& frame) {
    const std::uint64_t payloadSize = decodeFrameSizePrefix(frame);
    if (payloadSize == 0) {
        throw std::runtime_error("bridge batch payload is empty");
    }
    if (frame.size() != sizeof(std::uint32_t) + payloadSize) {
        throw std::runtime_error("bridge batch frame size prefix does not match payload length");
    }
    return decodePayload(std::vector<std::uint8_t>(frame.begin() + 4, frame.end()));
}

} // namespace tape_engine
