#include "tapescope_client.h"

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string_view>

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

namespace tapescope {

namespace {

constexpr std::string_view kAckSchema = "com.foxy.tape-engine.ingest-ack";
constexpr std::uint32_t kRequestVersion = 1;

std::string errnoMessage(const std::string& prefix) {
    return prefix + ": " + std::strerror(errno);
}

template <typename T>
QueryResult<T> makeSuccess(T value) {
    QueryResult<T> result;
    result.value = std::move(value);
    return result;
}

template <typename T>
QueryResult<T> makeError(QueryErrorKind kind, std::string message) {
    QueryResult<T> result;
    result.error.kind = kind;
    result.error.message = std::move(message);
    return result;
}

template <typename T>
QueryResult<T> propagateError(const QueryError& error) {
    QueryResult<T> result;
    result.error = error;
    return result;
}

std::vector<std::uint8_t> readExact(int fd, std::size_t bytes) {
    std::vector<std::uint8_t> buffer(bytes);
    std::size_t offset = 0;
    while (offset < bytes) {
        const ssize_t readCount = ::read(fd, buffer.data() + offset, bytes - offset);
        if (readCount == 0) {
            throw std::runtime_error("unexpected EOF while reading a query response");
        }
        if (readCount < 0) {
            throw std::runtime_error(errnoMessage("read"));
        }
        offset += static_cast<std::size_t>(readCount);
    }
    return buffer;
}

std::vector<std::uint8_t> readFrame(int fd) {
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

std::vector<std::uint8_t> encodeFrame(const json& payload) {
    const std::vector<std::uint8_t> body = json::to_msgpack(payload);
    if (body.size() > static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
        throw std::runtime_error("query payload too large to frame");
    }

    const auto size = static_cast<std::uint32_t>(body.size());
    std::vector<std::uint8_t> frame;
    frame.reserve(4 + body.size());
    frame.push_back(static_cast<std::uint8_t>((size >> 24) & 0xffU));
    frame.push_back(static_cast<std::uint8_t>((size >> 16) & 0xffU));
    frame.push_back(static_cast<std::uint8_t>((size >> 8) & 0xffU));
    frame.push_back(static_cast<std::uint8_t>(size & 0xffU));
    frame.insert(frame.end(), body.begin(), body.end());
    return frame;
}

json decodeFrame(const std::vector<std::uint8_t>& frame) {
    if (frame.size() < 4) {
        throw std::runtime_error("query response frame is too short");
    }

    const std::uint32_t payloadSize =
        (static_cast<std::uint32_t>(frame[0]) << 24) |
        (static_cast<std::uint32_t>(frame[1]) << 16) |
        (static_cast<std::uint32_t>(frame[2]) << 8) |
        static_cast<std::uint32_t>(frame[3]);
    if (frame.size() != 4 + static_cast<std::size_t>(payloadSize)) {
        throw std::runtime_error("query response size prefix does not match payload length");
    }

    return json::from_msgpack(std::vector<std::uint8_t>(frame.begin() + 4, frame.end()));
}

bool isAckLikeResponse(const json& payload) {
    if (!payload.is_object()) {
        return false;
    }

    if (payload.value("schema", std::string()) == kAckSchema) {
        return true;
    }

    return payload.contains("accepted_records") ||
           payload.contains("duplicate_records") ||
           payload.contains("gap_markers");
}

std::string extractMessage(const json& payload, const std::string& fallback) {
    if (!payload.is_object()) {
        return fallback;
    }

    for (const char* key : {"error", "message", "reason", "detail"}) {
        const auto it = payload.find(key);
        if (it != payload.end() && it->is_string()) {
            return it->get<std::string>();
        }
    }

    const std::string status = payload.value("status", std::string());
    if (!status.empty() && status != "ok" && status != "success") {
        return status;
    }

    return fallback;
}

json buildRequestEnvelope(const std::string& command,
                          const json& args,
                          const std::string& clientName) {
    json payloadArgs = args;
    if (!payloadArgs.is_object()) {
        payloadArgs = json{{"value", payloadArgs}};
    }

    return json{
        {"version", kRequestVersion},
        {"command", command},
        {"args", std::move(payloadArgs)},
        {"client", {
            {"name", clientName},
            {"transport", "framed_msgpack_v1"}
        }}
    };
}

QueryResult<json> classifyResponse(const json& payload) {
    // Current branches still answer with ingest acks. Treat that as a missing
    // query seam instead of accidentally surfacing it as a valid query payload.
    if (isAckLikeResponse(payload)) {
        return makeError<json>(
            QueryErrorKind::SeamUnavailable,
            extractMessage(payload, "engine responded with the ingest-ack schema instead of a query result"));
    }

    if (payload.is_object()) {
        if (payload.contains("ok")) {
            if (!payload.value("ok", false)) {
                return makeError<json>(QueryErrorKind::Remote, extractMessage(payload, "engine query failed"));
            }
            if (payload.contains("result")) {
                return makeSuccess(payload.at("result"));
            }
            if (payload.contains("payload")) {
                return makeSuccess(payload.at("payload"));
            }
            if (payload.contains("data")) {
                return makeSuccess(payload.at("data"));
            }
            return makeSuccess(json::object());
        }

        if (payload.contains("status")) {
            const std::string status = payload.value("status", std::string());
            if (status == "ok" || status == "success") {
                if (payload.contains("result")) {
                    return makeSuccess(payload.at("result"));
                }
                if (payload.contains("payload")) {
                    return makeSuccess(payload.at("payload"));
                }
                if (payload.contains("data")) {
                    return makeSuccess(payload.at("data"));
                }
                return makeSuccess(payload);
            }

            if (status == "accepted" || status == "rejected") {
                return makeError<json>(
                    QueryErrorKind::SeamUnavailable,
                    extractMessage(payload, "engine responded with an ingest-style status instead of a query result"));
            }

            if (payload.contains("error") || payload.contains("message")) {
                return makeError<json>(QueryErrorKind::Remote, extractMessage(payload, "engine query failed"));
            }
        }

        if (payload.contains("result")) {
            return makeSuccess(payload.at("result"));
        }
        if (payload.contains("payload")) {
            return makeSuccess(payload.at("payload"));
        }
        if (payload.contains("data")) {
            return makeSuccess(payload.at("data"));
        }
    }

    return makeSuccess(payload);
}

std::vector<json> toJsonVector(const json& arrayPayload) {
    std::vector<json> values;
    values.reserve(arrayPayload.size());
    for (const auto& item : arrayPayload) {
        values.push_back(item);
    }
    return values;
}

std::uint64_t latestSessionSequenceFromPayload(const json& payload) {
    if (payload.contains("latest_session_seq")) {
        return payload.value("latest_session_seq", 0ULL);
    }
    if (payload.contains("last_session_seq")) {
        return payload.value("last_session_seq", 0ULL);
    }
    if (payload.contains("next_session_seq")) {
        const std::uint64_t next = payload.value("next_session_seq", 0ULL);
        return next > 0 ? next - 1 : 0ULL;
    }
    return 0ULL;
}

std::uint64_t eventCountFromPayload(const json& payload, const char* scalarKey, const char* arrayKey) {
    if (payload.contains(scalarKey)) {
        return payload.value(scalarKey, 0ULL);
    }
    const auto it = payload.find(arrayKey);
    if (it != payload.end() && it->is_array()) {
        return static_cast<std::uint64_t>(it->size());
    }
    return 0ULL;
}

std::string firstPresentString(const json& payload,
                               std::initializer_list<const char*> keys,
                               const std::string& fallback = std::string()) {
    for (const char* key : keys) {
        const auto it = payload.find(key);
        if (it != payload.end() && it->is_string()) {
            return it->get<std::string>();
        }
    }
    return fallback;
}

QueryResult<std::vector<json>> extractEventArray(const QueryResult<json>& response) {
    if (!response.ok()) {
        return propagateError<std::vector<json>>(response.error);
    }

    if (response.value.is_array()) {
        return makeSuccess(toJsonVector(response.value));
    }

    if (response.value.is_object()) {
        for (const char* key : {"events", "items", "records", "matches"}) {
            const auto it = response.value.find(key);
            if (it != response.value.end() && it->is_array()) {
                return makeSuccess(toJsonVector(*it));
            }
        }
    }

    return makeError<std::vector<json>>(QueryErrorKind::MalformedResponse,
                                        "query response did not contain an event array");
}

} // namespace

std::string defaultSocketPath() {
    const char* value = std::getenv("LONG_TAPE_ENGINE_SOCKET");
    if (value != nullptr && value[0] != '\0') {
        return value;
    }
    return std::string(kDefaultSocketPath);
}

QueryClient::QueryClient(ClientConfig config)
    : config_(std::move(config)) {
    if (config_.socketPath.empty()) {
        config_.socketPath = defaultSocketPath();
    }
    if (config_.clientName.empty()) {
        config_.clientName = "TapeScope";
    }
}

const ClientConfig& QueryClient::config() const {
    return config_;
}

QueryResult<json> QueryClient::query(const std::string& command, const json& args) const {
    if (config_.socketPath.empty()) {
        return makeError<json>(QueryErrorKind::Transport, "query socket path is empty");
    }

    int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        return makeError<json>(QueryErrorKind::Transport, errnoMessage("socket"));
    }

    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    if (config_.socketPath.size() >= sizeof(address.sun_path)) {
        ::close(fd);
        return makeError<json>(QueryErrorKind::Transport, "query socket path is too long");
    }
    std::strncpy(address.sun_path, config_.socketPath.c_str(), sizeof(address.sun_path) - 1);

    if (::connect(fd, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0) {
        const std::string error = errnoMessage("connect");
        ::close(fd);
        return makeError<json>(QueryErrorKind::Transport, error);
    }

    try {
        writeAll(fd, encodeFrame(buildRequestEnvelope(command, args, config_.clientName)));
        ::shutdown(fd, SHUT_WR);
        const QueryResult<json> result = classifyResponse(decodeFrame(readFrame(fd)));
        ::close(fd);
        return result;
    } catch (const std::exception& error) {
        ::close(fd);
        return makeError<json>(QueryErrorKind::MalformedResponse, error.what());
    }
}

QueryResult<StatusSnapshot> QueryClient::status() const {
    const QueryResult<json> response = query("status");
    if (!response.ok()) {
        return propagateError<StatusSnapshot>(response.error);
    }
    if (!response.value.is_object()) {
        return makeError<StatusSnapshot>(QueryErrorKind::MalformedResponse,
                                         "status response payload must be an object");
    }

    const json& payload = response.value;
    StatusSnapshot snapshot;
    snapshot.socketPath = firstPresentString(payload, {"socket_path", "socket"}, config_.socketPath);
    snapshot.dataDir = firstPresentString(payload, {"data_dir", "dataDir"});
    snapshot.instrumentId = firstPresentString(payload, {"instrument", "instrument_id"});
    snapshot.latestSessionSeq = latestSessionSequenceFromPayload(payload);
    snapshot.liveEventCount = eventCountFromPayload(payload, "live_event_count", "live_events");
    snapshot.segmentCount = eventCountFromPayload(payload, "segment_count", "segments");
    snapshot.manifestHash = firstPresentString(payload, {"manifest_hash", "last_manifest_hash", "manifest_sha256"});
    return makeSuccess(std::move(snapshot));
}

QueryResult<std::vector<json>> QueryClient::readLiveTail(std::size_t limit) const {
    return extractEventArray(query("read_live_tail", json{{"limit", limit}}));
}

QueryResult<std::vector<json>> QueryClient::readRange(const RangeQuery& queryRange) const {
    return extractEventArray(query("read_range", json{
        {"first_session_seq", queryRange.firstSessionSeq},
        {"last_session_seq", queryRange.lastSessionSeq}
    }));
}

QueryResult<json> QueryClient::findOrderAnchor(const OrderAnchorQuery& anchorQuery) const {
    json args = json::object();
    if (anchorQuery.traceId.has_value()) {
        args["trace_id"] = *anchorQuery.traceId;
    }
    if (anchorQuery.orderId.has_value()) {
        args["order_id"] = *anchorQuery.orderId;
    }
    if (anchorQuery.permId.has_value()) {
        args["perm_id"] = *anchorQuery.permId;
    }
    if (anchorQuery.execId.has_value() && !anchorQuery.execId->empty()) {
        args["exec_id"] = *anchorQuery.execId;
    }
    return query("find_order_anchor", args);
}

std::string QueryClient::describeError(const QueryError& error) {
    switch (error.kind) {
        case QueryErrorKind::None:
            return "No error";
        case QueryErrorKind::Transport:
            return "Engine unavailable: " + error.message;
        case QueryErrorKind::Remote:
            return "Engine query failed: " + error.message;
        case QueryErrorKind::MalformedResponse:
            return "Engine returned a malformed query payload: " + error.message;
        case QueryErrorKind::SeamUnavailable:
            return "Engine reachable, but the app query seam is unavailable: " + error.message;
    }
    return error.message;
}

} // namespace tapescope
