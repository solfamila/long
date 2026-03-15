#include "tape_mcp_adapter.h"
#include "app_shared.h"

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
#include <fstream>
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
        ::setenv("TWS_GUI_DATA_DIR", created, 1);
        return fs::path(created);
    }();
    return path;
}

void clearPhase6FixtureFiles() {
    std::error_code ec;
    fs::create_directories(testDataDir(), ec);
    fs::remove(tradeTraceLogPath(), ec);
    fs::remove(runtimeJournalLogPath(), ec);
    fs::remove_all(testDataDir() / "phase6_artifacts", ec);
}

json makeTraceLine(std::uint64_t traceId,
                   OrderId orderId,
                   long long permId,
                   const std::string& symbol,
                   const std::string& side,
                   const std::string& eventType,
                   const std::string& stage,
                   const std::string& details,
                   double sinceTriggerMs,
                   double price = 0.0,
                   int shares = 0,
                   double cumFilled = -1.0) {
    json line;
    line["traceId"] = traceId;
    line["orderId"] = static_cast<long long>(orderId);
    line["permId"] = permId;
    line["source"] = "tape_mcp_contract_tests";
    line["symbol"] = symbol;
    line["side"] = side;
    line["requestedQty"] = 10;
    line["limitPrice"] = 45.64;
    line["closeOnly"] = false;
    line["eventType"] = eventType;
    line["stage"] = stage;
    line["details"] = details;
    line["wallTime"] = "09:30:00.000";
    line["sinceTriggerMs"] = sinceTriggerMs;
    if (price > 0.0) {
        line["price"] = price;
    }
    if (shares > 0) {
        line["shares"] = shares;
    }
    if (cumFilled >= 0.0) {
        line["cumFilled"] = cumFilled;
    }
    return line;
}

void appendTraceLine(const json& line) {
    std::ofstream out(tradeTraceLogPath(), std::ios::app);
    expect(out.is_open(), "phase6 fixture should open trace log for append");
    out << line.dump() << '\n';
}

void seedPhase6TraceFixture() {
    clearPhase6FixtureFiles();
    appendTraceLine(makeTraceLine(31, 221, 7001, "INTC", "BUY",
                                  "Trigger", "Trigger", "manual submit", 0.0));
    appendTraceLine(makeTraceLine(31, 221, 7001, "INTC", "BUY",
                                  "ValidationStart", "Validation", "risk checks", 2.0));
    appendTraceLine(makeTraceLine(31, 221, 7001, "INTC", "BUY",
                                  "OrderStatusSeen", "Submitted", "Submitted", 8.0));
    appendTraceLine(makeTraceLine(31, 221, 7001, "INTC", "BUY",
                                  "ExecDetailsSeen", "PartialFill", "execId=EXEC-31 exch=NYSE time=09:30:00.500",
                                  21.0, 45.66, 5, 5.0));
    appendTraceLine(makeTraceLine(31, 221, 7001, "INTC", "BUY",
                                  "FinalState", "Filled", "Filled: broker complete", 35.0));
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

bool toolsListContainsName(const json& tools, const std::string& expectedName) {
    if (!tools.is_array()) {
        return false;
    }
    for (const auto& tool : tools) {
        if (tool.is_object() && tool.value("name", std::string()) == expectedName) {
            return true;
        }
    }
    return false;
}

void testDeferredAndUnsupportedEnvelopes() {
    tape_mcp::Adapter adapter(tape_mcp::AdapterConfig{
        .engineSocketPath = makeUniqueSocketPath("unused").string(),
        .clientName = "tape-mcp-contract-tests"
    });

    const json deferredEnvelope = envelopeFromToolResult(adapter.callTool("tapescript_playbook_apply", json::object()));
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

    const json reportEnvelope = envelopeFromToolResult(adapter.callTool("tapescript_report_generate", json::object()));
    expect(!reportEnvelope.value("ok", true),
           "report_generate with missing anchor should return ok=false");
    expect(reportEnvelope.value("error", json::object()).value("code", std::string()) == "invalid_arguments",
           "report_generate with missing anchor should return invalid_arguments");
    expect(reportEnvelope.value("meta", json::object()).value("supported", false),
           "report_generate should remain a supported tool when arguments are invalid");
    expect(!reportEnvelope.value("meta", json::object()).value("deferred", true),
           "report_generate invalid arguments should not be marked deferred");
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

void testPhase6ReportAndCaseToolShaping() {
    seedPhase6TraceFixture();
    tape_mcp::Adapter adapter(tape_mcp::AdapterConfig{
        .engineSocketPath = makeUniqueSocketPath("unused-phase6").string(),
        .clientName = "tape-mcp-contract-tests"
    });

    const json reportEnvelope = envelopeFromToolResult(
        adapter.callTool("tapescript_report_generate", json{{"trace_id", 31}}));
    expect(reportEnvelope.value("ok", false), "report_generate should return ok=true");
    expect(reportEnvelope.value("meta", json::object()).value("supported", false),
           "report_generate should set meta.supported=true");
    expect(!reportEnvelope.value("meta", json::object()).value("deferred", true),
           "report_generate should set meta.deferred=false");
    expect(reportEnvelope.value("meta", json::object()).value("engine_command", std::string()) == "phase6_report_generate_local",
           "report_generate should set the local phase6 engine_command marker");

    const json reportResult = reportEnvelope.value("result", json::object());
    const json reportArtifact = reportResult.value("artifact", json::object());
    expect(reportArtifact.value("artifact_type", std::string()) == "phase6.report_output.v1",
           "report_generate should surface report artifact_type");
    const std::string reportManifestPath = reportArtifact.value("manifest_path", std::string());
    expect(!reportManifestPath.empty(), "report_generate should return a manifest path");
    expect(fs::exists(reportManifestPath), "report_generate manifest path should exist");
    expect(fs::exists(reportArtifact.value("report_path", std::string())),
           "report_generate report_path should exist");
    expect(fs::exists(reportArtifact.value("summary_path", std::string())),
           "report_generate summary_path should exist");
    expect(fs::exists(reportArtifact.value("fills_path", std::string())),
           "report_generate fills_path should exist");
    expect(fs::exists(reportArtifact.value("timeline_path", std::string())),
           "report_generate timeline_path should exist");

    {
        std::ifstream in(reportManifestPath, std::ios::binary);
        expect(in.is_open(), "report_generate should write a readable manifest file");
        const json manifest = json::parse(in, nullptr, false);
        expect(!manifest.is_discarded(), "report_generate manifest should parse as json");
        expect(manifest.value("artifact_type", std::string()) == "phase6.report_output.v1",
               "report manifest should declare phase6.report_output.v1");
    }

    const json exportEnvelope = envelopeFromToolResult(
        adapter.callTool("tapescript_export_range", json{
            {"order_id", 221},
            {"first_session_seq", 30},
            {"last_session_seq", 35}
        }));
    expect(exportEnvelope.value("ok", false), "export_range should return ok=true");
    expect(exportEnvelope.value("meta", json::object()).value("supported", false),
           "export_range should set meta.supported=true");
    expect(!exportEnvelope.value("meta", json::object()).value("deferred", true),
           "export_range should set meta.deferred=false");
    expect(exportEnvelope.value("meta", json::object()).value("engine_command", std::string()) == "phase6_export_range_local",
           "export_range should set the local phase6 engine_command marker");

    const json exportResult = exportEnvelope.value("result", json::object());
    const json caseArtifact = exportResult.value("artifact", json::object());
    expect(caseArtifact.value("artifact_type", std::string()) == "phase6.case_bundle.v1",
           "export_range should surface case bundle artifact_type");
    const std::string caseManifestPath = caseArtifact.value("manifest_path", std::string());
    expect(!caseManifestPath.empty(), "export_range should return a case manifest path");
    expect(fs::exists(caseManifestPath), "export_range case manifest path should exist");

    const json nestedReport = exportResult.value("report_output", json::object());
    expect(nestedReport.value("artifact_type", std::string()) == "phase6.report_output.v1",
           "export_range should expose nested report output metadata");
    expect(fs::exists(nestedReport.value("manifest_path", std::string())),
           "export_range nested report manifest should exist");

    const json requestedWindow = exportResult.value("requested_window", json::object());
    expect(requestedWindow.value("first_session_seq", 0ULL) == 30ULL,
           "export_range should preserve first_session_seq in result shaping");
    expect(requestedWindow.value("last_session_seq", 0ULL) == 35ULL,
           "export_range should preserve last_session_seq in result shaping");

    {
        std::ifstream in(caseManifestPath, std::ios::binary);
        expect(in.is_open(), "export_range should write a readable case manifest file");
        const json manifest = json::parse(in, nullptr, false);
        expect(!manifest.is_discarded(), "export_range case manifest should parse as json");
        expect(manifest.value("artifact_type", std::string()) == "phase6.case_bundle.v1",
               "case manifest should declare phase6.case_bundle.v1");
    }

    const json rangeOnlyEnvelope = envelopeFromToolResult(
        adapter.callTool("tapescript_export_range", json{
            {"first_session_seq", 30},
            {"last_session_seq", 35}
        }));
    expect(!rangeOnlyEnvelope.value("ok", true),
           "range-only export_range should return ok=false in this slice");
    expect(rangeOnlyEnvelope.value("error", json::object()).value("code", std::string()) == "deferred_behavior",
           "range-only export_range should return deferred_behavior");
    expect(rangeOnlyEnvelope.value("meta", json::object()).value("deferred", false),
           "range-only export_range should set meta.deferred=true");
}

void testPhase7AnalyzerAndFindingsToolShaping() {
    seedPhase6TraceFixture();
    tape_mcp::Adapter adapter(tape_mcp::AdapterConfig{
        .engineSocketPath = makeUniqueSocketPath("unused-phase7").string(),
        .clientName = "tape-mcp-contract-tests"
    });

    const json caseEnvelope = envelopeFromToolResult(
        adapter.callTool("tapescript_export_range", json{{"trace_id", 31}}));
    expect(caseEnvelope.value("ok", false), "phase7 setup export_range should succeed");
    const std::string caseManifestPath =
        caseEnvelope.value("result", json::object())
            .value("artifact", json::object())
            .value("manifest_path", std::string());
    expect(!caseManifestPath.empty(), "phase7 setup should return case manifest path");

    const json analyzerEnvelope = envelopeFromToolResult(
        adapter.callTool("tapescript_analyzer_run", json{{"case_manifest_path", caseManifestPath}}));
    expect(analyzerEnvelope.value("ok", false), "analyzer_run should return ok=true");
    expect(analyzerEnvelope.value("meta", json::object()).value("supported", false),
           "analyzer_run should set meta.supported=true");
    expect(!analyzerEnvelope.value("meta", json::object()).value("deferred", true),
           "analyzer_run should set meta.deferred=false");
    expect(analyzerEnvelope.value("meta", json::object()).value("contract_version", std::string()) ==
               "phase7-analyzer-playbook-v1",
           "analyzer_run should use phase7 contract version");
    expect(analyzerEnvelope.value("meta", json::object()).value("engine_command", std::string()) ==
               "phase7_analyzer_run_local",
           "analyzer_run should use the local phase7 engine marker");

    const json analyzerResult = analyzerEnvelope.value("result", json::object());
    const json sourceArtifact = analyzerResult.value("source_artifact", json::object());
    expect(sourceArtifact.value("artifact_type", std::string()) == "phase6.case_bundle.v1",
           "analyzer_run should preserve phase6 case source artifact type");
    const json analysisArtifact = analyzerResult.value("analysis_artifact", json::object());
    expect(analysisArtifact.value("artifact_type", std::string()) == "phase7.analysis_output.v1",
           "analyzer_run should return phase7 analysis artifact type");
    expect(analysisArtifact.value("contract_version", std::string()) == "phase7-analyzer-playbook-v1",
           "analyzer_run analysis artifact should report phase7 contract");
    const std::string analysisManifestPath = analysisArtifact.value("manifest_path", std::string());
    expect(fs::exists(analysisManifestPath), "analyzer_run should write analysis manifest path");
    const json analyzerFindings = analyzerResult.value("findings", json::array());
    expect(analyzerFindings.is_array() && !analyzerFindings.empty(),
           "analyzer_run should return persisted findings payload");

    const json findingsFromPathEnvelope = envelopeFromToolResult(
        adapter.callTool("tapescript_findings_list", json{{"analysis_manifest_path", analysisManifestPath}}));
    expect(findingsFromPathEnvelope.value("ok", false), "findings_list by manifest path should return ok=true");
    expect(findingsFromPathEnvelope.value("meta", json::object()).value("contract_version", std::string()) ==
               "phase7-analyzer-playbook-v1",
           "findings_list should use phase7 contract version");
    expect(findingsFromPathEnvelope.value("meta", json::object()).value("engine_command", std::string()) ==
               "phase7_findings_list_local",
           "findings_list should use local phase7 engine marker");
    const json findingsFromPath = findingsFromPathEnvelope.value("result", json::object()).value("findings", json::array());
    expect(findingsFromPath == analyzerFindings,
           "findings_list by manifest path should return stored findings payload");

    const std::string analysisArtifactId = analysisArtifact.value("artifact_id", std::string());
    const json findingsFromIdEnvelope = envelopeFromToolResult(
        adapter.callTool("tapescript_findings_list", json{{"analysis_artifact_id", analysisArtifactId}}));
    expect(findingsFromIdEnvelope.value("ok", false), "findings_list by artifact id should return ok=true");
    const json findingsFromId = findingsFromIdEnvelope.value("result", json::object()).value("findings", json::array());
    expect(findingsFromId == analyzerFindings,
           "findings_list by artifact id should return stored findings payload");

    const json deferredProfileEnvelope = envelopeFromToolResult(
        adapter.callTool("tapescript_analyzer_run", json{
            {"case_manifest_path", caseManifestPath},
            {"analysis_profile", "phase7.unimplemented_profile.v1"}
        }));
    expect(!deferredProfileEnvelope.value("ok", true),
           "unsupported analysis_profile should return ok=false");
    expect(deferredProfileEnvelope.value("error", json::object()).value("code", std::string()) == "deferred_behavior",
           "unsupported analysis_profile should return deferred_behavior");
    expect(deferredProfileEnvelope.value("meta", json::object()).value("supported", false),
           "unsupported analysis_profile should keep meta.supported=true");
    expect(deferredProfileEnvelope.value("meta", json::object()).value("deferred", false),
           "unsupported analysis_profile should set meta.deferred=true");
}

void testPhase7AnalyzerAndFindingsFailureMapping() {
    seedPhase6TraceFixture();
    tape_mcp::Adapter adapter(tape_mcp::AdapterConfig{
        .engineSocketPath = makeUniqueSocketPath("unused-phase7-errors").string(),
        .clientName = "tape-mcp-contract-tests"
    });

    const json analyzerNeitherEnvelope = envelopeFromToolResult(
        adapter.callTool("tapescript_analyzer_run", json::object()));
    expect(!analyzerNeitherEnvelope.value("ok", true),
           "analyzer_run with neither source manifest should return ok=false");
    expect(analyzerNeitherEnvelope.value("error", json::object()).value("code", std::string()) ==
               "invalid_arguments",
           "analyzer_run with neither source manifest should map to invalid_arguments");

    const json analyzerBothEnvelope = envelopeFromToolResult(
        adapter.callTool("tapescript_analyzer_run", json{
            {"case_manifest_path", "/tmp/case-manifest.json"},
            {"report_manifest_path", "/tmp/report-manifest.json"}
        }));
    expect(!analyzerBothEnvelope.value("ok", true),
           "analyzer_run with both source manifests should return ok=false");
    expect(analyzerBothEnvelope.value("error", json::object()).value("code", std::string()) ==
               "invalid_arguments",
           "analyzer_run with both source manifests should map to invalid_arguments");

    const json analyzerNonStringEnvelope = envelopeFromToolResult(
        adapter.callTool("tapescript_analyzer_run", json{{"case_manifest_path", 31}}));
    expect(!analyzerNonStringEnvelope.value("ok", true),
           "analyzer_run with non-string manifest path should return ok=false");
    expect(analyzerNonStringEnvelope.value("error", json::object()).value("code", std::string()) ==
               "invalid_arguments",
           "analyzer_run with non-string manifest path should map to invalid_arguments");

    const json findingsNeitherEnvelope = envelopeFromToolResult(
        adapter.callTool("tapescript_findings_list", json::object()));
    expect(!findingsNeitherEnvelope.value("ok", true),
           "findings_list with neither analysis reference should return ok=false");
    expect(findingsNeitherEnvelope.value("error", json::object()).value("code", std::string()) ==
               "invalid_arguments",
           "findings_list with neither analysis reference should map to invalid_arguments");

    const json findingsBothEnvelope = envelopeFromToolResult(
        adapter.callTool("tapescript_findings_list", json{
            {"analysis_manifest_path", "/tmp/analysis-manifest.json"},
            {"analysis_artifact_id", "analysis-123"}
        }));
    expect(!findingsBothEnvelope.value("ok", true),
           "findings_list with both analysis references should return ok=false");
    expect(findingsBothEnvelope.value("error", json::object()).value("code", std::string()) ==
               "invalid_arguments",
           "findings_list with both analysis references should map to invalid_arguments");

    const json findingsNonStringEnvelope = envelopeFromToolResult(
        adapter.callTool("tapescript_findings_list", json{{"analysis_manifest_path", 31}}));
    expect(!findingsNonStringEnvelope.value("ok", true),
           "findings_list with non-string manifest path should return ok=false");
    expect(findingsNonStringEnvelope.value("error", json::object()).value("code", std::string()) ==
               "invalid_arguments",
           "findings_list with non-string manifest path should map to invalid_arguments");

    const json missingSourceEnvelope = envelopeFromToolResult(
        adapter.callTool("tapescript_analyzer_run", json{
            {"report_manifest_path", (testDataDir() / "missing-phase6-manifest.json").string()}
        }));
    expect(!missingSourceEnvelope.value("ok", true), "missing source manifest should return ok=false");
    expect(missingSourceEnvelope.value("error", json::object()).value("code", std::string()) == "artifact_not_found",
           "missing source manifest should map to artifact_not_found");

    const fs::path malformedSourceManifestPath = testDataDir() / "malformed-phase6-manifest.json";
    {
        std::ofstream out(malformedSourceManifestPath, std::ios::binary);
        expect(out.is_open(), "should open malformed source manifest path for write");
        out << "{bad-json\n";
    }
    const json malformedSourceEnvelope = envelopeFromToolResult(
        adapter.callTool("tapescript_analyzer_run", json{
            {"report_manifest_path", malformedSourceManifestPath.string()}
        }));
    expect(!malformedSourceEnvelope.value("ok", true), "malformed source manifest should return ok=false");
    expect(malformedSourceEnvelope.value("error", json::object()).value("code", std::string()) == "artifact_load_failed",
           "malformed source manifest should map to artifact_load_failed");

    const fs::path unsupportedContractPath = testDataDir() / "unsupported-phase6-contract.json";
    {
        std::ofstream out(unsupportedContractPath, std::ios::binary);
        expect(out.is_open(), "should open unsupported contract manifest path for write");
        out << json{
            {"contract_version", "phase6-unknown-v1"},
            {"artifact_type", "phase6.report_output.v1"},
            {"artifact_id", "report-unsupported"}
        }.dump(2);
    }
    const json unsupportedContractEnvelope = envelopeFromToolResult(
        adapter.callTool("tapescript_analyzer_run", json{
            {"report_manifest_path", unsupportedContractPath.string()}
        }));
    expect(!unsupportedContractEnvelope.value("ok", true), "unsupported source contract should return ok=false");
    expect(unsupportedContractEnvelope.value("error", json::object()).value("code", std::string()) ==
               "unsupported_source_contract",
           "unsupported source contract should map to unsupported_source_contract");

    const fs::path analysisFailedPath = testDataDir() / "analysis-failed-source.json";
    {
        std::ofstream out(analysisFailedPath, std::ios::binary);
        expect(out.is_open(), "should open analysis-failed source manifest path for write");
        out << json{
            {"contract_version", "phase6-case-report-v1"},
            {"artifact_type", "phase6.report_output.v1"}
        }.dump(2);
    }
    const json analysisFailedEnvelope = envelopeFromToolResult(
        adapter.callTool("tapescript_analyzer_run", json{
            {"report_manifest_path", analysisFailedPath.string()}
        }));
    expect(!analysisFailedEnvelope.value("ok", true), "analysis-failed source should return ok=false");
    expect(analysisFailedEnvelope.value("error", json::object()).value("code", std::string()) == "analysis_failed",
           "non-contract analyzer execution failure should map to analysis_failed");

    const json missingAnalysisArtifactEnvelope = envelopeFromToolResult(
        adapter.callTool("tapescript_findings_list", json{
            {"analysis_artifact_id", "analysis-does-not-exist"}
        }));
    expect(!missingAnalysisArtifactEnvelope.value("ok", true),
           "missing analysis artifact should return ok=false");
    expect(missingAnalysisArtifactEnvelope.value("error", json::object()).value("code", std::string()) ==
               "artifact_not_found",
           "missing analysis artifact should map to artifact_not_found");

    const fs::path malformedAnalysisManifestPath = testDataDir() / "malformed-analysis-manifest.json";
    {
        std::ofstream out(malformedAnalysisManifestPath, std::ios::binary);
        expect(out.is_open(), "should open malformed analysis manifest path for write");
        out << "{bad-json\n";
    }
    const json malformedAnalysisEnvelope = envelopeFromToolResult(
        adapter.callTool("tapescript_findings_list", json{
            {"analysis_manifest_path", malformedAnalysisManifestPath.string()}
        }));
    expect(!malformedAnalysisEnvelope.value("ok", true), "malformed analysis manifest should return ok=false");
    expect(malformedAnalysisEnvelope.value("error", json::object()).value("code", std::string()) ==
               "artifact_load_failed",
           "malformed analysis manifest should map to artifact_load_failed");

    const fs::path unsupportedAnalysisManifestPath = testDataDir() / "unsupported-analysis-manifest.json";
    {
        std::ofstream out(unsupportedAnalysisManifestPath, std::ios::binary);
        expect(out.is_open(), "should open unsupported analysis manifest path for write");
        out << json{
            {"contract_version", "phase6-case-report-v1"},
            {"artifact_type", "phase6.report_output.v1"},
            {"artifact_id", "analysis-wrong-type"},
            {"files", json{{"findings_json", "findings.json"}}}
        }.dump(2);
    }
    const json unsupportedAnalysisEnvelope = envelopeFromToolResult(
        adapter.callTool("tapescript_findings_list", json{
            {"analysis_manifest_path", unsupportedAnalysisManifestPath.string()}
        }));
    expect(!unsupportedAnalysisEnvelope.value("ok", true), "unsupported analysis manifest should return ok=false");
    expect(unsupportedAnalysisEnvelope.value("error", json::object()).value("code", std::string()) ==
               "unsupported_source_contract",
           "unsupported analysis manifest should map to unsupported_source_contract");
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
    seedPhase6TraceFixture();
    tape_mcp::Adapter localAdapter(tape_mcp::AdapterConfig{
        .engineSocketPath = makeUniqueSocketPath("unused-phase7-stdio").string(),
        .clientName = "tape-mcp-contract-tests"
    });
    const json localCaseEnvelope = envelopeFromToolResult(
        localAdapter.callTool("tapescript_export_range", json{{"trace_id", 31}}));
    expect(localCaseEnvelope.value("ok", false),
           "phase7 stdio setup should create a phase6 case manifest");
    const std::string caseManifestPath = localCaseEnvelope.value("result", json::object())
        .value("artifact", json::object())
        .value("manifest_path", std::string());
    expect(!caseManifestPath.empty(), "phase7 stdio setup should return case manifest path");

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
    expect(toolsListContainsName(tools, "tapescript_report_generate"),
           "tools/list should expose tapescript_report_generate in the phase6 slice");
    expect(toolsListContainsName(tools, "tapescript_export_range"),
           "tools/list should expose tapescript_export_range in the phase6 slice");
    expect(toolsListContainsName(tools, "tapescript_analyzer_run"),
           "tools/list should expose tapescript_analyzer_run in the phase7 slice");
    expect(toolsListContainsName(tools, "tapescript_findings_list"),
           "tools/list should expose tapescript_findings_list in the phase7 slice");

    writeJsonRpcMessage(child.writeFd, json{
        {"jsonrpc", "2.0"},
        {"id", 3},
        {"method", "tools/call"},
        {"params", {
            {"name", "tapescript_analyzer_run"},
            {"arguments", json{{"case_manifest_path", caseManifestPath}}}
        }}
    });
    const json analyzerResponse = readJsonRpcMessage(child.readFd);
    const json analyzerEnvelope = analyzerResponse.value("result", json::object())
        .value("structuredContent", json::object());
    expect(analyzerEnvelope.value("ok", false),
           "stdio analyzer_run should return ok=true with local artifact generation");
    const json analysisArtifact = analyzerEnvelope.value("result", json::object())
        .value("analysis_artifact", json::object());
    expect(fs::exists(analysisArtifact.value("manifest_path", std::string())),
           "stdio analyzer_run should write an analysis manifest path");
    const std::string analysisArtifactId = analysisArtifact.value("artifact_id", std::string());
    expect(!analysisArtifactId.empty(),
           "stdio analyzer_run should return analysis artifact id");

    writeJsonRpcMessage(child.writeFd, json{
        {"jsonrpc", "2.0"},
        {"id", 4},
        {"method", "tools/call"},
        {"params", {
            {"name", "tapescript_findings_list"},
            {"arguments", json{{"analysis_artifact_id", analysisArtifactId}}}
        }}
    });
    const json findingsResponse = readJsonRpcMessage(child.readFd);
    const json findingsEnvelope = findingsResponse.value("result", json::object())
        .value("structuredContent", json::object());
    expect(findingsEnvelope.value("ok", false),
           "stdio findings_list should return ok=true");
    expect(findingsEnvelope.value("result", json::object()).value("findings", json::array()).is_array(),
           "stdio findings_list should return findings array payload");

    writeJsonRpcMessage(child.writeFd, json{
        {"jsonrpc", "2.0"},
        {"id", 5},
        {"method", "tools/call"},
        {"params", {
            {"name", "tapescript_playbook_apply"},
            {"arguments", json::object()}
        }}
    });
    const json playbookResponse = readJsonRpcMessage(child.readFd);
    const json playbookEnvelope = playbookResponse.value("result", json::object())
        .value("structuredContent", json::object());
    expect(playbookEnvelope.value("error", json::object()).value("code", std::string()) == "deferred_tool",
           "stdio playbook_apply should remain explicitly deferred");

    writeJsonRpcMessage(child.writeFd, json{
        {"jsonrpc", "2.0"},
        {"id", 6},
        {"method", "tools/call"},
        {"params", {
            {"name", "tapescript_report_generate"},
            {"arguments", json{{"trace_id", 31}}}
        }}
    });
    const json reportResponse = readJsonRpcMessage(child.readFd);
    const json reportEnvelope = reportResponse.value("result", json::object())
        .value("structuredContent", json::object());
    expect(reportEnvelope.value("ok", false),
           "stdio report_generate should return ok=true with local artifact generation");
    const json reportArtifact = reportEnvelope.value("result", json::object())
        .value("artifact", json::object());
    expect(fs::exists(reportArtifact.value("manifest_path", std::string())),
           "stdio report_generate should write a manifest path");

    writeJsonRpcMessage(child.writeFd, json{
        {"jsonrpc", "2.0"},
        {"id", 7},
        {"method", "tools/call"},
        {"params", {
            {"name", "tapescript_export_range"},
            {"arguments", json{
                {"first_session_seq", 30},
                {"last_session_seq", 35}
            }}
        }}
    });
    const json deferredBehaviorResponse = readJsonRpcMessage(child.readFd);
    const json deferredBehaviorEnvelope = deferredBehaviorResponse.value("result", json::object())
        .value("structuredContent", json::object());
    expect(deferredBehaviorEnvelope.value("error", json::object()).value("code", std::string()) == "deferred_behavior",
           "stdio export_range should keep range-only behavior explicitly deferred");

    writeJsonRpcMessage(child.writeFd, json{
        {"jsonrpc", "2.0"},
        {"id", 8},
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
           "stdio status should still surface engine_unavailable when engine socket is absent");

    stopProcess(child);
}

} // namespace

int main() {
    try {
        testDeferredAndUnsupportedEnvelopes();
        testStatusSuccessEnvelopeAndRevision();
        testReadLiveTailDerivesRevisionFromEvents();
        testReadRangeAndFindAnchorShaping();
        testPhase6ReportAndCaseToolShaping();
        testPhase7AnalyzerAndFindingsToolShaping();
        testPhase7AnalyzerAndFindingsFailureMapping();
        testErrorAndInvalidArgumentEnvelopes();
        testMcpStdioHarnessRegression();
    } catch (const std::exception& error) {
        std::cerr << "tape_mcp_contract_tests failed: " << error.what() << '\n';
        return 1;
    }

    std::cout << "tape_mcp_contract_tests passed\n";
    return 0;
}
