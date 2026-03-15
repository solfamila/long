#include "tape_mcp_adapter.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <functional>
#include <iostream>
#include <limits>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <poll.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>

namespace fs = std::filesystem;
using json = nlohmann::json;

namespace {

#ifndef TAPE_MCP_EXECUTABLE_PATH
#define TAPE_MCP_EXECUTABLE_PATH "tape_mcp"
#endif

void expect(bool condition, const std::string& message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

const fs::path& testDataDir() {
    static const fs::path path = [] {
        char pattern[] = "/tmp/tape_mcp_tests.XXXXXX";
        char* created = ::mkdtemp(pattern);
        if (created == nullptr) {
            throw std::runtime_error("mkdtemp failed");
        }
        return fs::path(created);
    }();
    return path;
}

fs::path makeUniqueSocketPath(const std::string& prefix) {
    static std::size_t counter = 0;
    return testDataDir() / (prefix + "-" + std::to_string(++counter) + ".sock");
}

std::vector<std::uint8_t> readAllFromFd(int fd) {
    std::vector<std::uint8_t> bytes;
    std::array<std::uint8_t, 4096> buffer{};
    while (true) {
        const ssize_t readCount = ::read(fd, buffer.data(), buffer.size());
        if (readCount == 0) {
            break;
        }
        if (readCount < 0) {
            throw std::runtime_error("read failed: " + std::string(std::strerror(errno)));
        }
        bytes.insert(bytes.end(), buffer.begin(), buffer.begin() + readCount);
    }
    return bytes;
}

void writeAllToFd(int fd, const std::vector<std::uint8_t>& bytes) {
    std::size_t offset = 0;
    while (offset < bytes.size()) {
        const ssize_t wrote = ::write(fd, bytes.data() + offset, bytes.size() - offset);
        if (wrote < 0) {
            throw std::runtime_error("write failed: " + std::string(std::strerror(errno)));
        }
        offset += static_cast<std::size_t>(wrote);
    }
}

std::vector<std::uint8_t> encodeJsonFrame(const json& payload) {
    const std::vector<std::uint8_t> body = json::to_msgpack(payload);
    expect(body.size() < static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max()),
           "test response frame should fit framing prefix");

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

json decodeJsonFrame(const std::vector<std::uint8_t>& frame) {
    expect(frame.size() >= 4, "test request frame should include a size prefix");
    const std::uint32_t payloadSize =
        (static_cast<std::uint32_t>(frame[0]) << 24) |
        (static_cast<std::uint32_t>(frame[1]) << 16) |
        (static_cast<std::uint32_t>(frame[2]) << 8) |
        static_cast<std::uint32_t>(frame[3]);
    expect(frame.size() == 4 + static_cast<std::size_t>(payloadSize),
           "test request frame size prefix should match payload length");
    return json::from_msgpack(std::vector<std::uint8_t>(frame.begin() + 4, frame.end()));
}

json exchangeEngineRequest(const fs::path& socketPath,
                           const json& responsePayload,
                           const std::function<void()>& invoke) {
    std::error_code ec;
    fs::remove(socketPath, ec);

    const int serverFd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    expect(serverFd >= 0, "test engine server should create a socket");

    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    std::strncpy(address.sun_path, socketPath.c_str(), sizeof(address.sun_path) - 1);
    expect(::bind(serverFd, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) == 0,
           "test engine server should bind successfully");
    expect(::listen(serverFd, 1) == 0, "test engine server should listen successfully");

    json requestPayload;
    std::string serverError;
    std::thread server([&]() {
        const int clientFd = ::accept(serverFd, nullptr, nullptr);
        if (clientFd < 0) {
            serverError = "accept failed: " + std::string(std::strerror(errno));
            return;
        }
        try {
            requestPayload = decodeJsonFrame(readAllFromFd(clientFd));
            writeAllToFd(clientFd, encodeJsonFrame(responsePayload));
        } catch (const std::exception& error) {
            serverError = error.what();
        }
        ::close(clientFd);
    });

    invoke();

    server.join();
    ::close(serverFd);
    fs::remove(socketPath, ec);
    expect(serverError.empty(), serverError);
    return requestPayload;
}

json envelopeFromToolResult(const json& toolResult) {
    expect(toolResult.is_object(), "tool result should be an object");
    const auto it = toolResult.find("structuredContent");
    expect(it != toolResult.end() && it->is_object(), "tool result should include structuredContent object");
    return *it;
}

void expectRevisionShape(const json& revision) {
    expect(revision.is_object(), "meta.revision should be an object");
    expect(revision.contains("manifest_hash"), "meta.revision should include manifest_hash");
    expect(revision.contains("latest_session_seq"), "meta.revision should include latest_session_seq");
    expect(revision.contains("first_session_seq"), "meta.revision should include first_session_seq");
    expect(revision.contains("last_session_seq"), "meta.revision should include last_session_seq");
    expect(revision.contains("source"), "meta.revision should include source");
    expect(revision.contains("staleness"), "meta.revision should include staleness");
}

void testDeferredAndUnsupportedEnvelopes() {
    tape_mcp::Adapter adapter(tape_mcp::AdapterConfig{
        .engineSocketPath = makeUniqueSocketPath("unused").string(),
        .clientName = "tape-mcp-contract-tests"
    });

    const json deferredEnvelope = envelopeFromToolResult(adapter.callTool("tapescript_analyzer_run", json::object()));
    expect(!deferredEnvelope.value("ok", true), "reserved deferred tool should return ok=false");
    expect(deferredEnvelope.value("result", json(nullptr)).is_null(), "reserved deferred tool should return result=null");
    expect(deferredEnvelope.value("error", json::object()).value("code", std::string()) == "deferred_tool",
           "reserved deferred tool should return deferred_tool error code");
    expect(!deferredEnvelope.value("meta", json::object()).value("supported", true),
           "reserved deferred tool should set meta.supported=false");
    expect(deferredEnvelope.value("meta", json::object()).value("deferred", false),
           "reserved deferred tool should set meta.deferred=true");
    expectRevisionShape(deferredEnvelope.value("meta", json::object()).value("revision", json::object()));

    const json unsupportedEnvelope = envelopeFromToolResult(adapter.callTool("tapescript_unknown", json::object()));
    expect(!unsupportedEnvelope.value("ok", true), "unknown tool should return ok=false");
    expect(unsupportedEnvelope.value("error", json::object()).value("code", std::string()) == "unsupported_tool",
           "unknown tool should return unsupported_tool error code");
    expect(!unsupportedEnvelope.value("meta", json::object()).value("supported", true),
           "unknown tool should set meta.supported=false");
    expect(!unsupportedEnvelope.value("meta", json::object()).value("deferred", true),
           "unknown tool should set meta.deferred=false");
}

void testStatusSuccessEnvelopeAndRevision() {
    const fs::path socketPath = makeUniqueSocketPath("status-success");
    tape_mcp::Adapter adapter(tape_mcp::AdapterConfig{
        .engineSocketPath = socketPath.string(),
        .clientName = "tape-mcp-contract-tests"
    });

    json toolResult;
    const json request = exchangeEngineRequest(
        socketPath,
        json{
            {"ok", true},
            {"result", {
                {"socket_path", socketPath.string()},
                {"latest_session_seq", 42},
                {"manifest_hash", "manifest-42"}
            }}
        },
        [&]() {
            toolResult = adapter.callTool("tapescript_status", json::object());
        });

    expect(request.value("command", std::string()) == "status", "status tool should issue status engine command");
    expect(request.value("args", json::object()).empty(), "status tool should issue empty args");

    const json envelope = envelopeFromToolResult(toolResult);
    expect(envelope.value("ok", false), "status tool should return ok=true on success");
    expect(envelope.value("error", json(nullptr)).is_null(), "status tool should return error=null on success");
    expect(envelope.value("meta", json::object()).value("supported", false),
           "status success should set meta.supported=true");
    expect(!envelope.value("meta", json::object()).value("deferred", true),
           "status success should set meta.deferred=false");

    const json revision = envelope.value("meta", json::object()).value("revision", json::object());
    expectRevisionShape(revision);
    expect(revision.value("manifest_hash", std::string()) == "manifest-42",
           "status revision should include manifest_hash from engine payload");
    expect(revision.value("latest_session_seq", 0ULL) == 42ULL,
           "status revision should include latest_session_seq from engine payload");
    expect(revision.value("source", std::string()) == "engine_payload",
           "status revision source should be engine_payload");
    expect(revision.value("staleness", std::string()) == "live",
           "status revision staleness should be live");
}

void testReadLiveTailDerivesRevisionFromEvents() {
    const fs::path socketPath = makeUniqueSocketPath("live-tail-success");
    tape_mcp::Adapter adapter(tape_mcp::AdapterConfig{
        .engineSocketPath = socketPath.string(),
        .clientName = "tape-mcp-contract-tests"
    });

    json toolResult;
    const json request = exchangeEngineRequest(
        socketPath,
        json{
            {"ok", true},
            {"result", {
                {"events", json::array({
                    json{{"session_seq", 43}, {"kind", "fill"}},
                    json{{"session_seq", 41}, {"kind", "order"}},
                    json{{"session_seq", 42}, {"kind", "cancel"}}
                })}
            }}
        },
        [&]() {
            toolResult = adapter.callTool("tapescript_read_live_tail", json{{"limit", 5}});
        });

    expect(request.value("command", std::string()) == "read_live_tail",
           "read_live_tail tool should issue read_live_tail engine command");
    expect(request.value("args", json::object()).value("limit", 0ULL) == 5ULL,
           "read_live_tail should forward limit argument");

    const json envelope = envelopeFromToolResult(toolResult);
    expect(envelope.value("ok", false), "read_live_tail should return ok=true on success");
    const json revision = envelope.value("meta", json::object()).value("revision", json::object());
    expect(revision.value("first_session_seq", 0ULL) == 41ULL,
           "read_live_tail should derive first_session_seq from events");
    expect(revision.value("last_session_seq", 0ULL) == 43ULL,
           "read_live_tail should derive last_session_seq from events");
    expect(revision.value("latest_session_seq", 0ULL) == 43ULL,
           "read_live_tail should derive latest_session_seq from events");
    expect(revision.value("source", std::string()) == "derived_from_events",
           "read_live_tail revision source should be derived_from_events");
    expect(revision.value("staleness", std::string()) == "live",
           "read_live_tail revision staleness should be live");
}

void testReadRangeAndFindAnchorShaping() {
    {
        const fs::path socketPath = makeUniqueSocketPath("range-success");
        tape_mcp::Adapter adapter(tape_mcp::AdapterConfig{
            .engineSocketPath = socketPath.string(),
            .clientName = "tape-mcp-contract-tests"
        });

        json toolResult;
        const json request = exchangeEngineRequest(
            socketPath,
            json{
                {"ok", true},
                {"result", {
                    {"events", json::array({
                        json{{"session_seq", 12}},
                        json{{"session_seq", 13}}
                    })}
                }}
            },
            [&]() {
                toolResult = adapter.callTool("tapescript_read_range", json{
                    {"first_session_seq", 10},
                    {"last_session_seq", 20}
                });
            });

        expect(request.value("command", std::string()) == "read_range", "read_range should issue read_range engine command");
        expect(request.value("args", json::object()).value("first_session_seq", 0ULL) == 10ULL,
               "read_range should forward first_session_seq");
        expect(request.value("args", json::object()).value("last_session_seq", 0ULL) == 20ULL,
               "read_range should forward last_session_seq");

        const json envelope = envelopeFromToolResult(toolResult);
        expect(envelope.value("ok", false), "read_range should return ok=true on success");
        const json revision = envelope.value("meta", json::object()).value("revision", json::object());
        expect(revision.value("first_session_seq", 0ULL) == 10ULL,
               "read_range revision should keep first_session_seq from request window");
        expect(revision.value("last_session_seq", 0ULL) == 20ULL,
               "read_range revision should keep last_session_seq from request window");
        expect(revision.value("latest_session_seq", 0ULL) == 13ULL,
               "read_range revision should derive latest_session_seq from observed events");
        expect(revision.value("source", std::string()) == "derived_from_events",
               "read_range revision source should be derived_from_events");
        expect(revision.value("staleness", std::string()) == "snapshot",
               "read_range revision staleness should be snapshot");
    }

    {
        const fs::path socketPath = makeUniqueSocketPath("anchor-success");
        tape_mcp::Adapter adapter(tape_mcp::AdapterConfig{
            .engineSocketPath = socketPath.string(),
            .clientName = "tape-mcp-contract-tests"
        });

        json toolResult;
        const json request = exchangeEngineRequest(
            socketPath,
            json{
                {"ok", true},
                {"result", {
                    {"trace_id", 71},
                    {"order_id", 501},
                    {"perm_id", 9501},
                    {"exec_id", "EXEC-71"},
                    {"events", json::array({
                        json{{"session_seq", 90}},
                        json{{"session_seq", 91}}
                    })}
                }}
            },
            [&]() {
                toolResult = adapter.callTool("tapescript_find_order_anchor", json{{"order_id", 501}});
            });

        expect(request.value("command", std::string()) == "find_order_anchor",
               "find_order_anchor should issue find_order_anchor engine command");
        expect(request.value("args", json::object()).value("order_id", 0LL) == 501LL,
               "find_order_anchor should forward provided anchor argument");

        const json envelope = envelopeFromToolResult(toolResult);
        expect(envelope.value("ok", false), "find_order_anchor should return ok=true on success");
        const json result = envelope.value("result", json::object());
        const json context = result.value("anchor_context", json::object());
        expect(context.value("trace_id", 0ULL) == 71ULL,
               "find_order_anchor should expose trace_id context when present");
        expect(context.value("order_id", 0LL) == 501LL,
               "find_order_anchor should expose order_id context when present");
        expect(context.value("perm_id", 0LL) == 9501LL,
               "find_order_anchor should expose perm_id context when present");
        expect(context.value("exec_id", std::string()) == "EXEC-71",
               "find_order_anchor should expose exec_id context when present");

        const json revision = envelope.value("meta", json::object()).value("revision", json::object());
        expect(revision.value("first_session_seq", 0ULL) == 90ULL,
               "find_order_anchor should derive first_session_seq from event payload");
        expect(revision.value("last_session_seq", 0ULL) == 91ULL,
               "find_order_anchor should derive last_session_seq from event payload");
        expect(revision.value("latest_session_seq", 0ULL) == 91ULL,
               "find_order_anchor should derive latest_session_seq from event payload");
        expect(revision.value("source", std::string()) == "derived_from_events",
               "find_order_anchor revision source should be derived_from_events");
        expect(revision.value("staleness", std::string()) == "snapshot",
               "find_order_anchor revision staleness should be snapshot");
    }
}

void testErrorAndInvalidArgumentEnvelopes() {
    {
        tape_mcp::Adapter adapter(tape_mcp::AdapterConfig{
            .engineSocketPath = makeUniqueSocketPath("missing-engine").string(),
            .clientName = "tape-mcp-contract-tests"
        });

        const json envelope = envelopeFromToolResult(adapter.callTool("tapescript_status", json::object()));
        expect(!envelope.value("ok", true), "missing engine should return ok=false");
        expect(envelope.value("error", json::object()).value("code", std::string()) == "engine_unavailable",
               "transport failures should map to engine_unavailable");
        expect(envelope.value("error", json::object()).value("retryable", false),
               "engine_unavailable should be marked retryable");
        expect(envelope.value("meta", json::object()).value("supported", false),
               "supported tools should retain meta.supported=true on transport failure");
        expect(!envelope.value("meta", json::object()).value("deferred", true),
               "supported tools should retain meta.deferred=false on transport failure");
    }

    {
        const fs::path socketPath = makeUniqueSocketPath("seam-unavailable");
        tape_mcp::Adapter adapter(tape_mcp::AdapterConfig{
            .engineSocketPath = socketPath.string(),
            .clientName = "tape-mcp-contract-tests"
        });

        json toolResult;
        exchangeEngineRequest(
            socketPath,
            json{{"status", "accepted"}, {"accepted_records", 1}},
            [&]() {
                toolResult = adapter.callTool("tapescript_read_live_tail", json::object());
            });

        const json envelope = envelopeFromToolResult(toolResult);
        expect(!envelope.value("ok", true), "seam-unavailable response should return ok=false");
        expect(envelope.value("error", json::object()).value("code", std::string()) == "seam_unavailable",
               "ingest-style responses should map to seam_unavailable");
    }

    {
        tape_mcp::Adapter adapter(tape_mcp::AdapterConfig{
            .engineSocketPath = makeUniqueSocketPath("invalid-args").string(),
            .clientName = "tape-mcp-contract-tests"
        });

        const json invalidRange = envelopeFromToolResult(adapter.callTool("tapescript_read_range", json{{"first_session_seq", 4}}));
        expect(!invalidRange.value("ok", true), "invalid range args should return ok=false");
        expect(invalidRange.value("error", json::object()).value("code", std::string()) == "invalid_arguments",
               "invalid range args should return invalid_arguments code");

        const json invalidAnchor = envelopeFromToolResult(adapter.callTool(
            "tapescript_find_order_anchor",
            json{{"order_id", 1}, {"trace_id", 2}}));
        expect(!invalidAnchor.value("ok", true), "invalid anchor args should return ok=false");
        expect(invalidAnchor.value("error", json::object()).value("code", std::string()) == "invalid_arguments",
               "multiple anchor keys should return invalid_arguments code");
    }

    {
        const fs::path socketPath = makeUniqueSocketPath("malformed-range");
        tape_mcp::Adapter adapter(tape_mcp::AdapterConfig{
            .engineSocketPath = socketPath.string(),
            .clientName = "tape-mcp-contract-tests"
        });

        json toolResult;
        exchangeEngineRequest(
            socketPath,
            json{{"ok", true}, {"result", json{{"records_count", 2}}}},
            [&]() {
                toolResult = adapter.callTool("tapescript_read_range", json{{"first_session_seq", 1}, {"last_session_seq", 2}});
            });

        const json envelope = envelopeFromToolResult(toolResult);
        expect(!envelope.value("ok", true), "malformed read_range payload should return ok=false");
        expect(envelope.value("error", json::object()).value("code", std::string()) == "malformed_response",
               "malformed read_range payload should map to malformed_response");
    }
}

bool waitForReadable(int fd, int timeoutMs) {
    pollfd descriptor{};
    descriptor.fd = fd;
    descriptor.events = POLLIN;
    const int pollResult = ::poll(&descriptor, 1, timeoutMs);
    if (pollResult < 0) {
        throw std::runtime_error("poll failed: " + std::string(std::strerror(errno)));
    }
    return pollResult > 0 && (descriptor.revents & POLLIN) != 0;
}

std::string readLineWithTimeout(int fd, int timeoutMs) {
    std::string line;
    char ch = 0;
    while (true) {
        if (!waitForReadable(fd, timeoutMs)) {
            throw std::runtime_error("timed out waiting for MCP response header line");
        }
        const ssize_t readCount = ::read(fd, &ch, 1);
        if (readCount == 0) {
            throw std::runtime_error("unexpected EOF while reading MCP response header line");
        }
        if (readCount < 0) {
            throw std::runtime_error("read failed: " + std::string(std::strerror(errno)));
        }
        line.push_back(ch);
        if (ch == '\n') {
            return line;
        }
    }
}

std::vector<std::uint8_t> readExactWithTimeout(int fd, std::size_t bytes, int timeoutMs) {
    std::vector<std::uint8_t> data(bytes);
    std::size_t offset = 0;
    while (offset < bytes) {
        if (!waitForReadable(fd, timeoutMs)) {
            throw std::runtime_error("timed out waiting for MCP response body bytes");
        }
        const ssize_t readCount = ::read(fd, data.data() + offset, bytes - offset);
        if (readCount == 0) {
            throw std::runtime_error("unexpected EOF while reading MCP response body");
        }
        if (readCount < 0) {
            throw std::runtime_error("read failed: " + std::string(std::strerror(errno)));
        }
        offset += static_cast<std::size_t>(readCount);
    }
    return data;
}

void writeJsonRpcMessage(int fd, const json& payload) {
    const std::string body = payload.dump();
    const std::string header = "Content-Length: " + std::to_string(body.size()) + "\r\n\r\n";

    std::vector<std::uint8_t> bytes;
    bytes.reserve(header.size() + body.size());
    bytes.insert(bytes.end(), header.begin(), header.end());
    bytes.insert(bytes.end(), body.begin(), body.end());
    writeAllToFd(fd, bytes);
}

json readJsonRpcMessage(int fd) {
    std::optional<std::size_t> contentLength;
    while (true) {
        std::string line = readLineWithTimeout(fd, 3000);
        if (!line.empty() && line.back() == '\n') {
            line.pop_back();
        }
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        if (line.empty()) {
            break;
        }

        constexpr const char* prefix = "Content-Length:";
        if (line.rfind(prefix, 0) == 0) {
            const std::string value = line.substr(std::strlen(prefix));
            contentLength = static_cast<std::size_t>(std::stoull(value));
        }
    }

    expect(contentLength.has_value(), "MCP response should include Content-Length");
    const std::vector<std::uint8_t> bodyBytes = readExactWithTimeout(fd, *contentLength, 3000);
    const std::string body(bodyBytes.begin(), bodyBytes.end());
    return json::parse(body);
}

struct ChildProcess {
    pid_t pid = -1;
    int writeFd = -1;
    int readFd = -1;
};

ChildProcess launchTapeMcp(const fs::path& socketPath) {
    int stdinPipe[2] = {-1, -1};
    int stdoutPipe[2] = {-1, -1};

    expect(::pipe(stdinPipe) == 0, "pipe for child stdin should succeed");
    expect(::pipe(stdoutPipe) == 0, "pipe for child stdout should succeed");

    const pid_t pid = ::fork();
    expect(pid >= 0, "fork should succeed");

    if (pid == 0) {
        ::dup2(stdinPipe[0], STDIN_FILENO);
        ::dup2(stdoutPipe[1], STDOUT_FILENO);

        ::close(stdinPipe[0]);
        ::close(stdinPipe[1]);
        ::close(stdoutPipe[0]);
        ::close(stdoutPipe[1]);

        const std::string socketArg = socketPath.string();
        ::execl(TAPE_MCP_EXECUTABLE_PATH,
                TAPE_MCP_EXECUTABLE_PATH,
                "--engine-socket",
                socketArg.c_str(),
                static_cast<char*>(nullptr));
        _exit(127);
    }

    ::close(stdinPipe[0]);
    ::close(stdoutPipe[1]);
    return ChildProcess{pid, stdinPipe[1], stdoutPipe[0]};
}

void stopProcess(const ChildProcess& child) {
    if (child.writeFd >= 0) {
        ::close(child.writeFd);
    }
    if (child.readFd >= 0) {
        ::close(child.readFd);
    }

    int status = 0;
    const pid_t waited = ::waitpid(child.pid, &status, 0);
    expect(waited == child.pid, "waitpid should collect the MCP child process");
    expect(WIFEXITED(status), "MCP child should exit normally");
    expect(WEXITSTATUS(status) == 0, "MCP child should exit with code 0");
}

void testMcpStdioHarnessRegression() {
    const ChildProcess child = launchTapeMcp(makeUniqueSocketPath("stdio-engine-missing"));

    writeJsonRpcMessage(child.writeFd, json{
        {"jsonrpc", "2.0"},
        {"id", 1},
        {"method", "initialize"},
        {"params", json::object()}
    });
    const json initializeResponse = readJsonRpcMessage(child.readFd);
    expect(initializeResponse.value("jsonrpc", std::string()) == "2.0", "initialize response should use jsonrpc=2.0");
    expect(initializeResponse.value("id", 0) == 1, "initialize response should preserve request id");
    expect(initializeResponse.value("result", json::object()).value("serverInfo", json::object()).value("name", std::string()) == "tape-mcp",
           "initialize response should include tape-mcp server info");

    writeJsonRpcMessage(child.writeFd, json{
        {"jsonrpc", "2.0"},
        {"id", 2},
        {"method", "tools/list"},
        {"params", json::object()}
    });
    const json listResponse = readJsonRpcMessage(child.readFd);
    const json tools = listResponse.value("result", json::object()).value("tools", json::array());
    expect(tools.is_array(), "tools/list response should include tools array");
    expect(!tools.empty(), "tools/list response should contain registered tools");

    writeJsonRpcMessage(child.writeFd, json{
        {"jsonrpc", "2.0"},
        {"id", 3},
        {"method", "tools/call"},
        {"params", {
            {"name", "tapescript_analyzer_run"},
            {"arguments", json::object()}
        }}
    });
    const json deferredResponse = readJsonRpcMessage(child.readFd);
    const json deferredEnvelope = deferredResponse.value("result", json::object())
        .value("structuredContent", json::object());
    expect(deferredEnvelope.value("error", json::object()).value("code", std::string()) == "deferred_tool",
           "stdio tools/call should surface deferred_tool for reserved deferred tool IDs");

    writeJsonRpcMessage(child.writeFd, json{
        {"jsonrpc", "2.0"},
        {"id", 4},
        {"method", "tools/call"},
        {"params", {
            {"name", "tapescript_status"},
            {"arguments", json::object()}
        }}
    });
    const json unavailableResponse = readJsonRpcMessage(child.readFd);
    const json unavailableEnvelope = unavailableResponse.value("result", json::object())
        .value("structuredContent", json::object());
    expect(unavailableEnvelope.value("error", json::object()).value("code", std::string()) == "engine_unavailable",
           "stdio tools/call should surface engine_unavailable when engine socket is absent");

    stopProcess(child);
}

} // namespace

int main() {
    try {
        testDeferredAndUnsupportedEnvelopes();
        testStatusSuccessEnvelopeAndRevision();
        testReadLiveTailDerivesRevisionFromEvents();
        testReadRangeAndFindAnchorShaping();
        testErrorAndInvalidArgumentEnvelopes();
        testMcpStdioHarnessRegression();
    } catch (const std::exception& error) {
        std::cerr << "tape_mcp_contract_tests failed: " << error.what() << '\n';
        return 1;
    }

    std::cout << "tape_mcp_contract_tests passed\n";
    return 0;
}
