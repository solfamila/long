#include "tape_engine.h"

#include <CommonCrypto/CommonDigest.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <system_error>

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

namespace tape_engine {

namespace {

std::string errnoMessage(const std::string& prefix) {
    return prefix + ": " + std::strerror(errno);
}

std::uint64_t nowEngineNs() {
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
}

std::string sha256Hex(const std::string& input) {
    unsigned char digest[CC_SHA256_DIGEST_LENGTH];
    CC_SHA256(input.data(), static_cast<CC_LONG>(input.size()), digest);

    static constexpr char kHex[] = "0123456789abcdef";
    std::string output;
    output.reserve(CC_SHA256_DIGEST_LENGTH * 2);
    for (unsigned char byte : digest) {
        output.push_back(kHex[(byte >> 4) & 0x0fU]);
        output.push_back(kHex[byte & 0x0fU]);
    }
    return output;
}

json eventToJson(const EngineEvent& event) {
    json payload{
        {"adapter_id", event.adapterId},
        {"connection_id", event.connectionId},
        {"event_kind", event.eventKind},
        {"instrument_id", event.instrumentId},
        {"session_seq", event.sessionSeq},
        {"source_seq", event.sourceSeq},
        {"ts_engine_ns", event.tsEngineNs}
    };

    if (event.eventKind == "gap_marker") {
        payload["gap_start_source_seq"] = event.gapStartSourceSeq;
        payload["gap_end_source_seq"] = event.gapEndSourceSeq;
        return payload;
    }

    payload["record_type"] = event.bridgeRecord.recordType;
    payload["source"] = event.bridgeRecord.source;
    payload["symbol"] = event.bridgeRecord.symbol;
    payload["side"] = event.bridgeRecord.side;
    payload["note"] = event.bridgeRecord.note;
    payload["wall_time"] = event.bridgeRecord.wallTime;
    payload["fallback_state"] = event.bridgeRecord.fallbackState;
    payload["fallback_reason"] = event.bridgeRecord.fallbackReason;
    payload["anchor"] = {
        {"trace_id", event.bridgeRecord.anchor.traceId},
        {"order_id", static_cast<long long>(event.bridgeRecord.anchor.orderId)},
        {"perm_id", event.bridgeRecord.anchor.permId},
        {"exec_id", event.bridgeRecord.anchor.execId}
    };
    return payload;
}

std::vector<std::uint8_t> readExact(int fd, std::size_t bytes) {
    std::vector<std::uint8_t> buffer(bytes);
    std::size_t offset = 0;
    while (offset < bytes) {
        const ssize_t readCount = ::read(fd, buffer.data() + offset, bytes - offset);
        if (readCount == 0) {
            throw std::runtime_error("unexpected EOF while reading framed message");
        }
        if (readCount < 0) {
            throw std::runtime_error(errnoMessage("read"));
        }
        offset += static_cast<std::size_t>(readCount);
    }
    return buffer;
}

std::vector<std::uint8_t> readFramedMessage(int fd) {
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

void writeAll(int fd, const std::vector<std::uint8_t>& data) {
    std::size_t offset = 0;
    while (offset < data.size()) {
        const ssize_t wrote = ::write(fd, data.data() + offset, data.size() - offset);
        if (wrote < 0) {
            throw std::runtime_error(errnoMessage("write"));
        }
        offset += static_cast<std::size_t>(wrote);
    }
}

std::string connectionKey(const std::string& adapterId, const std::string& connectionId) {
    return adapterId + "|" + connectionId;
}

} // namespace

Server::Server(EngineConfig config)
    : config_(std::move(config)) {}

Server::~Server() {
    stop();
}

bool Server::start(std::string* error) {
    if (running_.load(std::memory_order_acquire)) {
        return true;
    }

    std::error_code ec;
    std::filesystem::create_directories(config_.dataDir / "segments", ec);
    if (ec) {
        if (error != nullptr) {
            *error = "failed to create tape-engine data dir: " + ec.message();
        }
        return false;
    }

    if (config_.socketPath.size() >= sizeof(sockaddr_un{}.sun_path)) {
        if (error != nullptr) {
            *error = "tape-engine socket path is too long";
        }
        return false;
    }

    std::filesystem::remove(config_.socketPath, ec);

    serverFd_ = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (serverFd_ < 0) {
        if (error != nullptr) {
            *error = errnoMessage("socket");
        }
        return false;
    }

    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    std::strncpy(address.sun_path, config_.socketPath.c_str(), sizeof(address.sun_path) - 1);
    if (::bind(serverFd_, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0) {
        if (error != nullptr) {
            *error = errnoMessage("bind");
        }
        ::close(serverFd_);
        serverFd_ = -1;
        return false;
    }
    if (::listen(serverFd_, 16) != 0) {
        if (error != nullptr) {
            *error = errnoMessage("listen");
        }
        ::close(serverFd_);
        serverFd_ = -1;
        return false;
    }

    running_.store(true, std::memory_order_release);
    sequencerThread_ = std::thread(&Server::sequencerLoop, this);
    acceptThread_ = std::thread(&Server::acceptLoop, this);
    return true;
}

void Server::stop() {
    if (!running_.exchange(false, std::memory_order_acq_rel)) {
        return;
    }

    if (serverFd_ >= 0) {
        ::shutdown(serverFd_, SHUT_RDWR);
        ::close(serverFd_);
        serverFd_ = -1;
    }
    queueCv_.notify_all();

    if (acceptThread_.joinable()) {
        acceptThread_.join();
    }
    if (sequencerThread_.joinable()) {
        sequencerThread_.join();
    }

    std::error_code ec;
    std::filesystem::remove(config_.socketPath, ec);
}

EngineSnapshot Server::snapshot() const {
    std::lock_guard<std::mutex> lock(stateMutex_);
    EngineSnapshot snapshot;
    snapshot.nextSessionSeq = nextSessionSeq_;
    snapshot.nextSegmentId = nextSegmentId_;
    snapshot.liveEvents.assign(liveRing_.begin(), liveRing_.end());
    snapshot.segments = segments_;
    return snapshot;
}

const EngineConfig& Server::config() const {
    return config_;
}

void Server::acceptLoop() {
    while (running_.load(std::memory_order_acquire)) {
        const int clientFd = ::accept(serverFd_, nullptr, nullptr);
        if (clientFd < 0) {
            if (!running_.load(std::memory_order_acquire)) {
                break;
            }
            if (errno == EINTR) {
                continue;
            }
            continue;
        }

        try {
            auto request = std::make_shared<PendingRequest>();
            request->frame = readFramedMessage(clientFd);
            auto future = request->promise.get_future();
            {
                std::lock_guard<std::mutex> lock(queueMutex_);
                queue_.push_back(std::move(request));
            }
            queueCv_.notify_one();
            writeAll(clientFd, future.get());
        } catch (...) {
        }

        ::close(clientFd);
    }
}

void Server::sequencerLoop() {
    while (running_.load(std::memory_order_acquire) || !queue_.empty()) {
        std::shared_ptr<PendingRequest> request;
        {
            std::unique_lock<std::mutex> lock(queueMutex_);
            queueCv_.wait(lock, [&]() {
                return !running_.load(std::memory_order_acquire) || !queue_.empty();
            });
            if (queue_.empty()) {
                continue;
            }
            request = std::move(queue_.front());
            queue_.pop_front();
        }

        try {
            request->promise.set_value(processRequest(request->frame));
        } catch (const std::exception& error) {
            request->promise.set_value(encodeAckFrame(rejectAck(0, "", "", error.what())));
        }
    }
}

IngestAck Server::rejectAck(std::uint64_t batchSeq,
                            const std::string& adapterId,
                            const std::string& connectionId,
                            const std::string& error) const {
    IngestAck ack;
    ack.status = "rejected";
    ack.batchSeq = batchSeq;
    ack.adapterId = adapterId;
    ack.connectionId = connectionId;
    ack.error = error;
    return ack;
}

void Server::appendLiveEvent(const EngineEvent& event) {
    if (config_.ringCapacity == 0) {
        return;
    }
    if (liveRing_.size() >= config_.ringCapacity) {
        liveRing_.pop_front();
    }
    liveRing_.push_back(event);
}

void Server::writeSegment(const std::vector<EngineEvent>& events) {
    if (events.empty()) {
        return;
    }

    const std::uint64_t segmentId = nextSegmentId_++;
    std::ostringstream name;
    name << "segment-" << std::setw(6) << std::setfill('0') << segmentId;
    const std::string baseName = name.str();
    const std::filesystem::path segmentPath = config_.dataDir / "segments" / (baseName + ".events.jsonl");
    const std::filesystem::path metadataPath = config_.dataDir / "segments" / (baseName + ".meta.json");
    const std::filesystem::path manifestPath = config_.dataDir / "manifest.jsonl";

    std::ostringstream payloadBuilder;
    for (const auto& event : events) {
        payloadBuilder << eventToJson(event).dump() << '\n';
    }
    const std::string payload = payloadBuilder.str();

    {
        std::ofstream out(segmentPath);
        if (!out.is_open()) {
            throw std::runtime_error("failed to open tape-engine segment for write");
        }
        out << payload;
    }

    SegmentInfo info;
    info.segmentId = segmentId;
    info.firstSessionSeq = events.front().sessionSeq;
    info.lastSessionSeq = events.back().sessionSeq;
    info.eventCount = static_cast<std::uint64_t>(events.size());
    info.fileName = segmentPath.filename().string();
    info.metadataFileName = metadataPath.filename().string();
    info.payloadSha256 = sha256Hex(payload);
    info.prevManifestHash = lastManifestHash_;

    json manifestEntry{
        {"event_count", info.eventCount},
        {"file_name", info.fileName},
        {"first_session_seq", info.firstSessionSeq},
        {"last_session_seq", info.lastSessionSeq},
        {"metadata_file_name", info.metadataFileName},
        {"payload_sha256", info.payloadSha256},
        {"prev_manifest_hash", info.prevManifestHash},
        {"segment_id", info.segmentId}
    };
    info.manifestHash = sha256Hex(manifestEntry.dump());
    manifestEntry["manifest_hash"] = info.manifestHash;

    {
        std::ofstream metadataOut(metadataPath);
        if (!metadataOut.is_open()) {
            throw std::runtime_error("failed to open tape-engine segment metadata for write");
        }
        metadataOut << manifestEntry.dump(2) << '\n';
    }
    {
        std::ofstream manifestOut(manifestPath, std::ios::app);
        if (!manifestOut.is_open()) {
            throw std::runtime_error("failed to open tape-engine manifest for append");
        }
        manifestOut << manifestEntry.dump() << '\n';
    }

    lastManifestHash_ = info.manifestHash;
    segments_.push_back(std::move(info));
}

std::vector<json> Server::loadAllEventsUnlocked() const {
    std::vector<json> events;
    for (const auto& segment : segments_) {
        const std::filesystem::path segmentPath = config_.dataDir / "segments" / segment.fileName;
        std::ifstream in(segmentPath);
        if (!in.is_open()) {
            continue;
        }
        std::string line;
        while (std::getline(in, line)) {
            if (line.empty()) {
                continue;
            }
            json parsed = json::parse(line, nullptr, false);
            if (!parsed.is_discarded()) {
                events.push_back(std::move(parsed));
            }
        }
    }
    return events;
}

std::vector<json> Server::filterEventsByRangeUnlocked(std::uint64_t fromSessionSeq,
                                                      std::uint64_t toSessionSeq,
                                                      std::size_t limit,
                                                      bool includeLiveTail) const {
    std::vector<json> results;
    const auto allEvents = loadAllEventsUnlocked();
    for (const auto& event : allEvents) {
        const std::uint64_t sessionSeq = event.value("session_seq", 0ULL);
        if (sessionSeq < fromSessionSeq) {
            continue;
        }
        if (toSessionSeq > 0 && sessionSeq > toSessionSeq) {
            continue;
        }
        results.push_back(event);
        if (limit > 0 && results.size() >= limit) {
            return results;
        }
    }

    if (includeLiveTail) {
        for (const auto& event : liveRing_) {
            if (event.sessionSeq < fromSessionSeq) {
                continue;
            }
            if (toSessionSeq > 0 && event.sessionSeq > toSessionSeq) {
                continue;
            }
            const bool alreadyPresent = std::any_of(results.begin(), results.end(), [&](const json& existing) {
                return existing.value("session_seq", 0ULL) == event.sessionSeq;
            });
            if (!alreadyPresent) {
                results.push_back(eventToJson(event));
                if (limit > 0 && results.size() >= limit) {
                    return results;
                }
            }
        }
    }

    return results;
}

std::vector<json> Server::filterEventsByAnchorUnlocked(std::uint64_t traceId,
                                                       long long orderId,
                                                       long long permId,
                                                       const std::string& execId,
                                                       std::size_t limit) const {
    std::vector<json> results;
    const auto allEvents = loadAllEventsUnlocked();
    for (const auto& event : allEvents) {
        const json anchor = event.value("anchor", json::object());
        const bool matchesTrace = traceId > 0 && anchor.value("trace_id", 0ULL) == traceId;
        const bool matchesOrder = orderId > 0 && anchor.value("order_id", 0LL) == orderId;
        const bool matchesPerm = permId > 0 && anchor.value("perm_id", 0LL) == permId;
        const bool matchesExec = !execId.empty() && anchor.value("exec_id", std::string()) == execId;
        if (!matchesTrace && !matchesOrder && !matchesPerm && !matchesExec) {
            continue;
        }
        results.push_back(event);
        if (limit > 0 && results.size() >= limit) {
            break;
        }
    }
    return results;
}

std::vector<std::uint8_t> Server::processRequest(const std::vector<std::uint8_t>& frame) {
    const json payload = decodeFramedJson(frame);
    const std::string schema = payload.value("schema", std::string());
    if (schema == bridge_batch::kSchemaName) {
        return encodeAckFrame(processIngestFrame(frame));
    }
    if (schema == kQueryRequestSchema) {
        return encodeQueryResponseFrame(processQueryFrame(frame));
    }

    QueryRequest request;
    request.operation = "unknown";
    return encodeQueryResponseFrame(rejectResponse(request, "unknown tape-engine request schema"));
}

IngestAck Server::processIngestFrame(const std::vector<std::uint8_t>& frame) {
    const bridge_batch::Batch batch = bridge_batch::decodeFrame(frame);
    IngestAck ack;
    ack.batchSeq = batch.header.batchSeq;
    ack.adapterId = batch.header.producer;
    ack.connectionId = batch.header.runtimeSessionId;
    ack.firstSourceSeq = batch.header.firstSourceSeq;
    ack.lastSourceSeq = batch.header.lastSourceSeq;

    if (batch.header.runtimeSessionId.empty()) {
        return rejectAck(batch.header.batchSeq, batch.header.producer, batch.header.runtimeSessionId,
                         "bridge batch runtime_session_id is required");
    }
    if (batch.header.recordCount != batch.records.size()) {
        return rejectAck(batch.header.batchSeq, batch.header.producer, batch.header.runtimeSessionId,
                         "bridge batch record_count does not match payload");
    }
    if (!batch.records.empty()) {
        if (batch.header.firstSourceSeq != batch.records.front().sourceSeq ||
            batch.header.lastSourceSeq != batch.records.back().sourceSeq) {
            return rejectAck(batch.header.batchSeq, batch.header.producer, batch.header.runtimeSessionId,
                             "bridge batch source_seq bounds do not match payload");
        }
        for (std::size_t i = 1; i < batch.records.size(); ++i) {
            if (batch.records[i - 1].sourceSeq >= batch.records[i].sourceSeq) {
                return rejectAck(batch.header.batchSeq, batch.header.producer, batch.header.runtimeSessionId,
                                 "bridge batch source_seq values must be strictly increasing");
            }
        }
    }

    std::vector<EngineEvent> writtenEvents;

    {
        std::lock_guard<std::mutex> lock(stateMutex_);
        ConnectionCursor& cursor = cursors_[connectionKey(batch.header.producer, batch.header.runtimeSessionId)];

        for (const auto& record : batch.records) {
            if (cursor.seenSourceSeqs.count(record.sourceSeq) > 0) {
                ++ack.duplicateRecords;
                continue;
            }

            if (cursor.hasLastAccepted && record.sourceSeq > cursor.lastAcceptedSourceSeq + 1) {
                EngineEvent gap;
                gap.sessionSeq = nextSessionSeq_++;
                gap.sourceSeq = cursor.lastAcceptedSourceSeq + 1;
                gap.eventKind = "gap_marker";
                gap.adapterId = batch.header.producer;
                gap.connectionId = batch.header.runtimeSessionId;
                gap.instrumentId = record.symbol.empty() ? config_.instrumentId : record.symbol;
                gap.tsEngineNs = nowEngineNs();
                gap.gapStartSourceSeq = cursor.lastAcceptedSourceSeq + 1;
                gap.gapEndSourceSeq = record.sourceSeq - 1;
                appendLiveEvent(gap);
                writtenEvents.push_back(gap);
                ++ack.gapMarkers;
                if (ack.firstSessionSeq == 0) {
                    ack.firstSessionSeq = gap.sessionSeq;
                }
                ack.lastSessionSeq = gap.sessionSeq;
            }

            EngineEvent event;
            event.sessionSeq = nextSessionSeq_++;
            event.sourceSeq = record.sourceSeq;
            event.eventKind = record.recordType;
            event.adapterId = batch.header.producer;
            event.connectionId = batch.header.runtimeSessionId;
            event.instrumentId = record.symbol.empty() ? config_.instrumentId : record.symbol;
            event.tsEngineNs = nowEngineNs();
            event.bridgeRecord = record;

            appendLiveEvent(event);
            writtenEvents.push_back(event);
            cursor.seenSourceSeqs[record.sourceSeq] = true;
            cursor.lastAcceptedSourceSeq = std::max(cursor.lastAcceptedSourceSeq, record.sourceSeq);
            cursor.hasLastAccepted = true;

            ++ack.acceptedRecords;
            if (ack.firstSessionSeq == 0) {
                ack.firstSessionSeq = event.sessionSeq;
            }
            ack.lastSessionSeq = event.sessionSeq;
        }

        writeSegment(writtenEvents);
    }

    return ack;
}

QueryResponse Server::rejectResponse(const QueryRequest& request,
                                     const std::string& error) const {
    QueryResponse response;
    response.requestId = request.requestId;
    response.operation = request.operation;
    response.status = "error";
    response.error = error;
    return response;
}

QueryResponse Server::processQueryFrame(const std::vector<std::uint8_t>& frame) {
    const QueryRequest request = decodeQueryRequestFrame(frame);
    QueryResponse response;
    response.requestId = request.requestId;
    response.operation = request.operation;

    std::lock_guard<std::mutex> lock(stateMutex_);
    if (request.operation == "status") {
        response.summary = {
            {"data_dir", config_.dataDir.string()},
            {"instrument_id", config_.instrumentId},
            {"last_manifest_hash", lastManifestHash_},
            {"latest_session_seq", nextSessionSeq_ > 0 ? nextSessionSeq_ - 1 : 0},
            {"live_event_count", liveRing_.size()},
            {"next_segment_id", nextSegmentId_},
            {"next_session_seq", nextSessionSeq_},
            {"segment_count", segments_.size()},
            {"socket_path", config_.socketPath}
        };
        return response;
    }

    if (request.operation == "read_live_tail") {
        const std::size_t limit = request.limit == 0 ? 50 : request.limit;
        response.summary = {
            {"includes_mutable_tail", true},
            {"live_tail_high_water_seq", liveRing_.empty() ? 0 : liveRing_.back().sessionSeq},
            {"returned_events", 0}
        };
        std::size_t start = liveRing_.size() > limit ? liveRing_.size() - limit : 0;
        response.events = json::array();
        for (std::size_t i = start; i < liveRing_.size(); ++i) {
            response.events.push_back(eventToJson(liveRing_[i]));
        }
        response.summary["returned_events"] = response.events.size();
        return response;
    }

    if (request.operation == "read_range") {
        const std::uint64_t from = request.fromSessionSeq == 0 ? 1 : request.fromSessionSeq;
        const std::uint64_t to = request.toSessionSeq;
        const std::size_t limit = request.limit == 0 ? 200 : request.limit;
        const std::vector<json> events = filterEventsByRangeUnlocked(from, to, limit, request.includeLiveTail);
        response.events = json::array();
        for (const auto& event : events) {
            response.events.push_back(event);
        }
        response.summary = {
            {"from_session_seq", from},
            {"includes_mutable_tail", request.includeLiveTail},
            {"returned_events", response.events.size()},
            {"to_session_seq", to}
        };
        if (request.includeLiveTail) {
            response.summary["live_tail_high_water_seq"] = liveRing_.empty() ? 0 : liveRing_.back().sessionSeq;
        }
        return response;
    }

    if (request.operation == "find_order_anchor") {
        const std::size_t limit = request.limit == 0 ? 100 : request.limit;
        const std::vector<json> events =
            filterEventsByAnchorUnlocked(request.traceId, request.orderId, request.permId, request.execId, limit);
        response.events = json::array();
        for (const auto& event : events) {
            response.events.push_back(event);
        }
        response.summary = {
            {"exec_id", request.execId},
            {"order_id", request.orderId},
            {"perm_id", request.permId},
            {"returned_events", response.events.size()},
            {"trace_id", request.traceId}
        };
        return response;
    }

    return rejectResponse(request, "unknown tape-engine operation");
}

} // namespace tape_engine
