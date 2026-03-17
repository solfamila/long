#include "bridge_batch_codec.h"
#include "bridge_batch_transport.h"
#include "tape_engine.h"
#include "tape_engine_client.h"
#include "tape_engine_protocol.h"

#include <nlohmann/json.hpp>

#include <array>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <optional>
#include <poll.h>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <sys/wait.h>

namespace fs = std::filesystem;
using json = nlohmann::json;

namespace {

constexpr const char* kSourceDir = TWS_GUI_SOURCE_DIR;

void expect(bool condition, const std::string& message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

void ensureCredentialFileFallback() {
    if (const char* path = std::getenv("LONG_CREDENTIAL_FILE"); path != nullptr && *path != '\0') {
        return;
    }
    const fs::path localPath = fs::path(kSourceDir) / ".env.local";
    if (fs::exists(localPath)) {
        (void)::setenv("LONG_CREDENTIAL_FILE", localPath.string().c_str(), 0);
    }
}

fs::path makeTempDir() {
    char pattern[] = "/tmp/mcp_enrichment_live_sanity.XXXXXX";
    char* created = ::mkdtemp(pattern);
    expect(created != nullptr, "mkdtemp should succeed");
    return fs::path(created);
}

template <typename Fn>
void waitUntil(Fn&& predicate,
               const std::string& message,
               std::chrono::milliseconds timeout = std::chrono::milliseconds(3000),
               std::chrono::milliseconds pollInterval = std::chrono::milliseconds(20)) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return;
        }
        std::this_thread::sleep_for(pollInterval);
    }
    expect(predicate(), message);
}

struct ChildProcess {
    pid_t pid = -1;
    int writeFd = -1;
    int readFd = -1;
};

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

std::string readLineWithTimeout(int fd, int timeoutMs) {
    std::string line;
    while (true) {
        pollfd descriptor{fd, POLLIN, 0};
        const int pollResult = ::poll(&descriptor, 1, timeoutMs);
        if (pollResult < 0) {
            throw std::runtime_error("poll failed: " + std::string(std::strerror(errno)));
        }
        if (pollResult == 0) {
            throw std::runtime_error("timed out while reading MCP response");
        }
        char ch = '\0';
        const ssize_t readCount = ::read(fd, &ch, 1);
        if (readCount == 0) {
            throw std::runtime_error("unexpected EOF while reading MCP response line");
        }
        if (readCount < 0) {
            throw std::runtime_error("read failed: " + std::string(std::strerror(errno)));
        }
        line.push_back(ch);
        if (ch == '\n') {
            break;
        }
    }
    return line;
}

std::vector<std::uint8_t> readExactWithTimeout(int fd, std::size_t bytes, int timeoutMs) {
    std::vector<std::uint8_t> data(bytes);
    std::size_t offset = 0;
    while (offset < bytes) {
        pollfd descriptor{fd, POLLIN, 0};
        const int pollResult = ::poll(&descriptor, 1, timeoutMs);
        if (pollResult < 0) {
            throw std::runtime_error("poll failed: " + std::string(std::strerror(errno)));
        }
        if (pollResult == 0) {
            throw std::runtime_error("timed out while reading MCP response body");
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

json readJsonRpcMessage(int fd, int timeoutMs = 30000) {
    std::optional<std::size_t> contentLength;
    while (true) {
        std::string line = readLineWithTimeout(fd, timeoutMs);
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
    const auto bodyBytes = readExactWithTimeout(fd, *contentLength, timeoutMs);
    return json::parse(std::string(bodyBytes.begin(), bodyBytes.end()));
}

json readResponseForId(int fd, int expectedId) {
    while (true) {
        const json message = readJsonRpcMessage(fd);
        if (message.value("method", std::string()) == "notifications/progress") {
            continue;
        }
        expect(message.value("id", 0) == expectedId, "unexpected JSON-RPC response id");
        return message;
    }
}

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

std::unique_ptr<tape_engine::Server> startEngine(const fs::path& rootDir, const fs::path& socketPath) {
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "ib:conid:9301:STK:SMART:USD:INTC";
    config.ringCapacity = 32;

    auto server = std::make_unique<tape_engine::Server>(config);
    std::string startError;
    expect(server->start(&startError), "tape-engine should start for MCP sanity: " + startError);
    return server;
}

BridgeOutboxRecord makeBridgeRecord(std::uint64_t sourceSeq,
                                    const std::string& recordType,
                                    const std::string& source,
                                    const std::string& symbol,
                                    const std::string& side,
                                    std::uint64_t traceId,
                                    OrderId orderId,
                                    long long permId,
                                    const std::string& execId,
                                    const std::string& note,
                                    const std::string& wallTime,
                                    const std::string& fallbackState = "queued_for_recovery",
                                    const std::string& fallbackReason = "engine_unavailable") {
    BridgeOutboxRecord record;
    record.sourceSeq = sourceSeq;
    record.recordType = recordType;
    record.source = source;
    record.symbol = symbol;
    record.side = side;
    record.anchor.traceId = traceId;
    record.anchor.orderId = orderId;
    record.anchor.permId = permId;
    record.anchor.execId = execId;
    record.fallbackState = fallbackState;
    record.fallbackReason = fallbackReason;
    record.note = note;
    record.wallTime = wallTime;
    return record;
}

void seedEngine(const fs::path& socketPath) {
    bridge_batch::BuildOptions options;
    options.appSessionId = "app-mcp-live-sanity";
    options.runtimeSessionId = "runtime-mcp-live-sanity";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;

    options.batchSeq = 201;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        makeBridgeRecord(9101, "order_intent", "WebSocket", "INTC", "BUY",
                         401, 7401, 0, "", "mcp sanity order intent", "2026-03-15T09:41:00.100"),
        makeBridgeRecord(9102, "order_status", "BrokerOrderStatus", "INTC", "BUY",
                         401, 7401, 8801, "", "Submitted: filled=0 remaining=1", "2026-03-15T09:41:00.120")
    }, options)), &error), "tape-engine should accept MCP sanity first batch: " + error);

    options.batchSeq = 202;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        makeBridgeRecord(9104, "fill_execution", "BrokerExecution", "INTC", "BOT",
                         401, 7401, 8801, "EXEC-401", "mcp sanity fill", "2026-03-15T09:41:00.250")
    }, options)), &error), "tape-engine should accept MCP sanity second batch: " + error);
}

std::uint64_t queryFirstLogicalIncidentId(const fs::path& socketPath) {
    tape_engine::Client client(socketPath.string());
    tape_engine::QueryResponse response;
    std::string error;
    tape_engine::QueryRequest request = tape_engine::makeQueryRequest(tape_engine::QueryOperation::ListIncidents,
                                                                      "mcp-live-sanity-list-incidents");
    request.limit = 5;
    expect(client.query(request, &response, &error), "incident query should succeed: " + error);
    expect(response.events.is_array() && !response.events.empty(), "incident query should return at least one incident");
    return response.events.front().value("logical_incident_id", 0ULL);
}

json initializeMcp(const ChildProcess& child) {
    writeJsonRpcMessage(child.writeFd, json{
        {"jsonrpc", "2.0"},
        {"id", 1},
        {"method", "initialize"},
        {"params", json::object()}
    });
    const json initializeResponse = readJsonRpcMessage(child.readFd);
    expect(initializeResponse.value("result", json::object())
               .value("serverInfo", json::object())
               .value("name", std::string()) == "tape-mcp",
           "initialize should return tape-mcp server info");

    writeJsonRpcMessage(child.writeFd, json{
        {"jsonrpc", "2.0"},
        {"method", "notifications/initialized"},
        {"params", json::object()}
    });
    return initializeResponse;
}

json callTool(const ChildProcess& child, int id, const std::string& toolName, const json& arguments) {
    writeJsonRpcMessage(child.writeFd, json{
        {"jsonrpc", "2.0"},
        {"id", id},
        {"method", "tools/call"},
        {"params", {
            {"name", toolName},
            {"arguments", arguments}
        }}
    });
    const json response = readResponseForId(child.readFd, id);
    return response.value("result", json::object()).value("structuredContent", json::object());
}

json summarizeEnvelope(const json& envelope) {
    const json result = envelope.value("result", json::object());
    const json interpretation = result.value("interpretation", json::object());
    const json content = interpretation.value("content", json(nullptr)).is_object()
        ? interpretation.value("content", json::object())
        : json::object();
    return {
        {"ok", envelope.value("ok", false)},
        {"request_kind", result.value("request_kind", std::string())},
        {"degradation", result.value("degradation", json::object())},
        {"live_capture_summary", result.value("live_capture_summary", json::object())},
        {"provider_path_used", result.value("provider_metadata", json::object()).value("provider_path_used", std::string())},
        {"interpretation", {
            {"status", interpretation.value("status", std::string())},
            {"model", interpretation.value("model", std::string())},
            {"finish_reason", interpretation.value("finish_reason", std::string())},
            {"latency_ms", interpretation.value("latency_ms", 0ULL)},
            {"json_valid", interpretation.value("json_valid", false)},
            {"schema_valid", interpretation.value("schema_valid", false)},
            {"error", interpretation.value("error", std::string())},
            {"summary", content.value("summary", std::string())},
            {"headline", content.value("headline", std::string())}
        }}
    };
}

} // namespace

int main() {
    try {
        ensureCredentialFileFallback();
        (void)::unsetenv("LONG_DISABLE_EXTERNAL_CONTEXT");

        const fs::path tempDir = makeTempDir();
        const fs::path rootDir = tempDir / "engine";
        const fs::path socketPath = tempDir / "engine.sock";

        auto server = startEngine(rootDir, socketPath);
        seedEngine(socketPath);
        waitUntil([&]() {
            const auto snapshot = server->snapshot();
            return snapshot.segments.size() >= 2 && snapshot.latestFrozenRevisionId >= 2;
        }, "seeded engine should freeze at least two revisions");

        const std::uint64_t logicalIncidentId = queryFirstLogicalIncidentId(socketPath);
        expect(logicalIncidentId > 0, "engine should expose a logical incident id");

        const ChildProcess child = launchTapeMcp(socketPath);
        const json initializeResponse = initializeMcp(child);

        const json explainIncident = callTool(child,
                                              2,
                                              "tapescript_explain_incident",
                                              json{{"logical_incident_id", logicalIncidentId}});
        const json enrichOrderCase = callTool(child,
                                              3,
                                              "tapescript_enrich_order_case",
                                              json{{"order_id", 7401}});
        const json refreshExternalContext = callTool(child,
                                                     4,
                                                     "tapescript_refresh_external_context",
                                                     json{{"logical_incident_id", logicalIncidentId}});
        stopProcess(child);
        server->stop();

        json output = {
            {"initialize", {
                {"server", initializeResponse.value("result", json::object()).value("serverInfo", json::object())},
                {"capabilities", initializeResponse.value("result", json::object()).value("capabilities", json::object())}
            }},
            {"logical_incident_id", logicalIncidentId},
            {"explain_incident", summarizeEnvelope(explainIncident)},
            {"enrich_order_case", summarizeEnvelope(enrichOrderCase)},
            {"refresh_external_context", summarizeEnvelope(refreshExternalContext)}
        };
        std::cout << output.dump(2) << std::endl;
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "mcp_enrichment_live_sanity failed: " << error.what() << '\n';
        return 1;
    }
}
