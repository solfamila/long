#include "tape_engine_client.h"

#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <vector>

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

namespace tape_engine {

namespace {

std::string errnoMessage(const std::string& prefix) {
    return prefix + ": " + std::strerror(errno);
}

std::vector<std::uint8_t> readExact(int fd, std::size_t bytes) {
    std::vector<std::uint8_t> buffer(bytes);
    std::size_t offset = 0;
    while (offset < bytes) {
        const ssize_t readCount = ::read(fd, buffer.data() + offset, bytes - offset);
        if (readCount == 0) {
            throw std::runtime_error("unexpected EOF while reading tape-engine response");
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

} // namespace

Client::Client(std::string socketPath)
    : socketPath_(std::move(socketPath)) {}

bool Client::query(const QueryRequest& request, QueryResponse* response, std::string* error) const {
    if (response == nullptr) {
        if (error != nullptr) {
            *error = "missing tape-engine response output";
        }
        return false;
    }
    if (socketPath_.empty()) {
        if (error != nullptr) {
            *error = "tape-engine socket path is empty";
        }
        return false;
    }

    int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        if (error != nullptr) {
            *error = errnoMessage("socket");
        }
        return false;
    }

    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    if (socketPath_.size() >= sizeof(address.sun_path)) {
        if (error != nullptr) {
            *error = "tape-engine socket path is too long";
        }
        ::close(fd);
        return false;
    }
    std::strncpy(address.sun_path, socketPath_.c_str(), sizeof(address.sun_path) - 1);

    if (::connect(fd, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0) {
        if (error != nullptr) {
            *error = errnoMessage("connect");
        }
        ::close(fd);
        return false;
    }

    try {
        writeAll(fd, encodeQueryRequestFrame(request));
        ::shutdown(fd, SHUT_WR);
        *response = decodeQueryResponseFrame(readFramedMessage(fd));
    } catch (const std::exception& requestError) {
        if (error != nullptr) {
            *error = requestError.what();
        }
        ::close(fd);
        return false;
    }

    ::close(fd);
    if (response->status != "ok") {
        if (error != nullptr) {
            *error = response->error.empty() ? "tape-engine query failed" : response->error;
        }
        return false;
    }
    return true;
}

const std::string& Client::socketPath() const {
    return socketPath_;
}

} // namespace tape_engine
