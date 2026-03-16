#include "runtime_qos.h"
#include "runtime_registry.h"
#include "tape_mcp_adapter.h"

#include <cctype>
#include <condition_variable>
#include <cstdlib>
#include <deque>
#include <functional>
#include <iostream>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace {

using json = nlohmann::json;

struct ReadMessageResult {
    bool ok = false;
    bool eof = false;
    std::string body;
    std::string error;
};

std::string trimLine(std::string line) {
    while (!line.empty() && (line.back() == '\r' || line.back() == '\n')) {
        line.pop_back();
    }
    return line;
}

std::optional<std::size_t> parseContentLength(const std::string& line) {
    constexpr std::string_view prefix = "Content-Length:";
    if (line.size() < prefix.size()) {
        return std::nullopt;
    }

    for (std::size_t i = 0; i < prefix.size(); ++i) {
        if (std::tolower(static_cast<unsigned char>(line[i])) !=
            std::tolower(static_cast<unsigned char>(prefix[i]))) {
            return std::nullopt;
        }
    }

    std::size_t cursor = prefix.size();
    while (cursor < line.size() && std::isspace(static_cast<unsigned char>(line[cursor])) != 0) {
        ++cursor;
    }
    if (cursor >= line.size()) {
        return std::nullopt;
    }
    try {
        return static_cast<std::size_t>(std::stoull(line.substr(cursor)));
    } catch (...) {
        return std::nullopt;
    }
}

ReadMessageResult readMessage(std::istream& in) {
    std::string line;
    bool sawHeader = false;
    std::optional<std::size_t> contentLength;

    while (true) {
        if (!std::getline(in, line)) {
            if (!sawHeader) {
                return ReadMessageResult{.ok = false, .eof = true};
            }
            return ReadMessageResult{.ok = false, .eof = false, .error = "unexpected EOF while reading headers"};
        }
        sawHeader = true;
        line = trimLine(line);
        if (line.empty()) {
            break;
        }
        const auto parsedLength = parseContentLength(line);
        if (parsedLength.has_value()) {
            contentLength = parsedLength;
        }
    }

    if (!contentLength.has_value()) {
        return ReadMessageResult{.ok = false, .eof = false, .error = "missing Content-Length header"};
    }

    std::string body(contentLength.value(), '\0');
    in.read(body.data(), static_cast<std::streamsize>(body.size()));
    if (in.gcount() != static_cast<std::streamsize>(body.size())) {
        return ReadMessageResult{.ok = false, .eof = false, .error = "unexpected EOF while reading body"};
    }

    return ReadMessageResult{.ok = true, .eof = false, .body = std::move(body)};
}

void writeMessage(std::ostream& out, const json& payload) {
    const std::string body = payload.dump();
    out << "Content-Length: " << body.size() << "\r\n\r\n" << body;
    out.flush();
}

json makeResponse(const json& id, const json& result) {
    return json{
        {"jsonrpc", "2.0"},
        {"id", id},
        {"result", result}
    };
}

json makeErrorResponse(const json& id, int code, const std::string& message) {
    return json{
        {"jsonrpc", "2.0"},
        {"id", id},
        {"error", {
            {"code", code},
            {"message", message}
        }}
    };
}

json makeProgressNotification(const json& token, int progress, int total, const std::string& message) {
    return json{
        {"jsonrpc", "2.0"},
        {"method", "notifications/progress"},
        {"params", {
            {"progressToken", token},
            {"progress", progress},
            {"total", total},
            {"message", message}
        }}
    };
}

std::optional<json> extractProgressToken(const json& params) {
    if (!params.is_object()) {
        return std::nullopt;
    }
    if (params.contains("_meta") && params["_meta"].is_object() && params["_meta"].contains("progressToken")) {
        return params["_meta"]["progressToken"];
    }
    if (params.contains("progressToken")) {
        return params["progressToken"];
    }
    return std::nullopt;
}

bool shouldRespond(const json& request) {
    return request.contains("id");
}

constexpr int kServerNotInitializedCode = -32002;
constexpr int kServerBusyCode = -32001;
constexpr std::size_t kReadWorkerCount = 3;
constexpr std::size_t kExportWorkerCount = 2;
constexpr std::size_t kReadQueueCapacity = 64;
constexpr std::size_t kExportQueueCapacity = 16;

class JsonRpcWriter {
public:
    explicit JsonRpcWriter(std::ostream& out)
        : out_(out) {}

    void write(const json& payload) {
        std::lock_guard<std::mutex> lock(mutex_);
        writeMessage(out_, payload);
    }

private:
    std::ostream& out_;
    std::mutex mutex_;
};

class WorkerPool {
public:
    WorkerPool(runtime_registry::QueueId queueId, std::size_t workerCount, std::size_t queueCapacity)
        : queueId_(queueId),
          queueCapacity_(queueCapacity) {
        workers_.reserve(workerCount);
        for (std::size_t i = 0; i < workerCount; ++i) {
            workers_.emplace_back([this]() { workerLoop(); });
        }
    }

    ~WorkerPool() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stopping_ = true;
        }
        cv_.notify_all();
        for (auto& worker : workers_) {
            if (worker.joinable()) {
                worker.join();
            }
        }
    }

    bool tryEnqueue(std::function<void()> job) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (stopping_ || queue_.size() >= queueCapacity_) {
                return false;
            }
            queue_.push_back(std::move(job));
        }
        cv_.notify_one();
        return true;
    }

private:
    void workerLoop() {
        runtime_qos::applyCurrentThreadSpec(queueId_);
        while (true) {
            std::function<void()> job;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                cv_.wait(lock, [this]() { return stopping_ || !queue_.empty(); });
                if (stopping_ && queue_.empty()) {
                    return;
                }
                job = std::move(queue_.front());
                queue_.pop_front();
            }
            job();
        }
    }

    runtime_registry::QueueId queueId_;
    std::size_t queueCapacity_ = 0;
    std::deque<std::function<void()>> queue_;
    std::mutex mutex_;
    std::condition_variable cv_;
    bool stopping_ = false;
    std::vector<std::thread> workers_;
};

bool requiresInitializedSession(std::string_view method) {
    if (method == "initialize" || method == "ping" || method == "notifications/initialized") {
        return false;
    }
    if (method.rfind("notifications/", 0) == 0) {
        return false;
    }
    return true;
}

bool isExportToolName(std::string_view toolName) {
    return toolName == "tapescript_scan_session_report" ||
        toolName == "tapescript_scan_incident_report" ||
        toolName == "tapescript_scan_order_case_report" ||
        toolName == "tapescript_export_artifact" ||
        toolName == "tapescript_export_session_bundle" ||
        toolName == "tapescript_export_case_bundle" ||
        toolName == "tapescript_import_case_bundle";
}

bool isExportResourceUri(std::string_view resourceUri) {
    return resourceUri.find("/markdown") != std::string_view::npos ||
        resourceUri.find("/json-bundle") != std::string_view::npos;
}

void emitProgress(JsonRpcWriter& writer,
                  const std::optional<json>& token,
                  int progress,
                  int total,
                  const std::string& message) {
    if (!token.has_value()) {
        return;
    }
    writer.write(makeProgressNotification(*token, progress, total, message));
}

} // namespace

int main(int argc, char** argv) {
    runtime_qos::applyCurrentThreadSpec(runtime_registry::QueueId::TapeMcpRequestLoop);

    const tape_mcp::ParsedAdapterArgs parsed = tape_mcp::parseAdapterArgs(argc, argv);
    if (!parsed.error.empty()) {
        std::cerr << "tape-mcp: " << parsed.error << '\n'
                  << tape_mcp::adapterUsage(argc > 0 ? argv[0] : "tape_mcp");
        return EXIT_FAILURE;
    }
    if (parsed.showHelp) {
        std::cerr << tape_mcp::adapterUsage(argc > 0 ? argv[0] : "tape_mcp");
        return EXIT_SUCCESS;
    }

    const tape_mcp::Adapter adapter(parsed.config);
    JsonRpcWriter writer(std::cout);
    WorkerPool readPool(runtime_registry::QueueId::TapeMcpReads, kReadWorkerCount, kReadQueueCapacity);
    WorkerPool exportPool(runtime_registry::QueueId::TapeMcpExports, kExportWorkerCount, kExportQueueCapacity);
    bool initialized = false;
    bool initializedNotificationSeen = false;

    while (true) {
        const ReadMessageResult incoming = readMessage(std::cin);
        if (incoming.eof) {
            break;
        }
        if (!incoming.ok) {
            writer.write(makeErrorResponse(nullptr, -32600, incoming.error));
            continue;
        }

        json request;
        try {
            request = json::parse(incoming.body);
        } catch (const std::exception& error) {
            writer.write(makeErrorResponse(nullptr, -32700, error.what()));
            continue;
        }

        if (!request.is_object()) {
            writer.write(makeErrorResponse(nullptr, -32600, "request must be an object"));
            continue;
        }
        if (request.value("jsonrpc", std::string()) != "2.0") {
            if (shouldRespond(request)) {
                writer.write(makeErrorResponse(request["id"], -32600, "jsonrpc must be 2.0"));
            }
            continue;
        }

        const bool hasId = shouldRespond(request);
        const json requestId = hasId ? request["id"] : nullptr;
        const std::string method = request.value("method", std::string());
        const json params = request.contains("params") ? request["params"] : json::object();

        if (method.empty()) {
            if (hasId) {
                writer.write(makeErrorResponse(requestId, -32600, "method is required"));
            }
            continue;
        }

        if (method == "initialize") {
            if (initialized) {
                if (hasId) {
                    writer.write(makeErrorResponse(requestId, -32600, "initialize has already been received"));
                }
                continue;
            }
            initialized = true;
            if (hasId) {
                writer.write(makeResponse(requestId, adapter.initializeResult()));
            }
            continue;
        }
        if (method == "ping") {
            if (hasId) {
                writer.write(makeResponse(requestId, json::object()));
            }
            continue;
        }
        if (method == "notifications/initialized") {
            initializedNotificationSeen = initialized;
            static_cast<void>(initializedNotificationSeen);
            continue;
        }
        if (!initialized && requiresInitializedSession(method)) {
            if (hasId) {
                writer.write(makeErrorResponse(requestId,
                                               kServerNotInitializedCode,
                                               "server not initialized; call initialize first"));
            }
            continue;
        }
        if (method == "tools/list") {
            if (hasId) {
                writer.write(makeResponse(requestId, adapter.listToolsResult()));
            }
            continue;
        }
        if (method == "resources/list") {
            if (hasId) {
                writer.write(makeResponse(requestId, adapter.listResourcesResult()));
            }
            continue;
        }
        if (method == "resources/read") {
            if (!params.is_object()) {
                if (hasId) {
                    writer.write(makeErrorResponse(requestId, -32602, "resources/read params must be an object"));
                }
                continue;
            }
            const std::string resourceUri = params.value("uri", std::string());
            if (resourceUri.empty()) {
                if (hasId) {
                    writer.write(makeErrorResponse(requestId, -32602, "resources/read params.uri is required"));
                }
                continue;
            }
            if (hasId) {
                const auto progressToken = extractProgressToken(params);
                const bool exportLike = isExportResourceUri(resourceUri);
                WorkerPool& pool = exportLike ? exportPool : readPool;
                if (progressToken.has_value() && exportLike) {
                    emitProgress(writer, progressToken, 5, 100, "queued resources/read");
                }
                const bool accepted = pool.tryEnqueue([&writer, &adapter, requestId, resourceUri, progressToken, exportLike]() {
                    try {
                        if (progressToken.has_value() && exportLike) {
                            emitProgress(writer, progressToken, 40, 100, "opening resource");
                        }
                        const json result = adapter.readResourceResult(resourceUri);
                        if (progressToken.has_value() && exportLike) {
                            emitProgress(writer, progressToken, 85, 100, "serializing resource");
                            emitProgress(writer, progressToken, 100, 100, "finished resources/read");
                        }
                        writer.write(makeResponse(requestId, result));
                    } catch (const std::exception& error) {
                        writer.write(makeErrorResponse(requestId, -32603, error.what()));
                    }
                });
                if (!accepted) {
                    writer.write(makeErrorResponse(requestId, kServerBusyCode, "resource queue is full"));
                }
            }
            continue;
        }
        if (method == "prompts/list") {
            if (hasId) {
                writer.write(makeResponse(requestId, adapter.listPromptsResult()));
            }
            continue;
        }
        if (method == "prompts/get") {
            if (!params.is_object()) {
                if (hasId) {
                    writer.write(makeErrorResponse(requestId, -32602, "prompts/get params must be an object"));
                }
                continue;
            }
            const std::string promptName = params.value("name", std::string());
            if (promptName.empty()) {
                if (hasId) {
                    writer.write(makeErrorResponse(requestId, -32602, "prompts/get params.name is required"));
                }
                continue;
            }
            const json args = params.contains("arguments") ? params["arguments"] : json::object();
            if (hasId) {
                writer.write(makeResponse(requestId, adapter.getPromptResult(promptName, args)));
            }
            continue;
        }
        if (method == "tools/call") {
            if (!params.is_object()) {
                if (hasId) {
                    writer.write(makeErrorResponse(requestId, -32602, "tools/call params must be an object"));
                }
                continue;
            }
            const std::string toolName = params.value("name", std::string());
            if (toolName.empty()) {
                if (hasId) {
                    writer.write(makeErrorResponse(requestId, -32602, "tools/call params.name is required"));
                }
                continue;
            }
            const json args = params.contains("arguments") ? params["arguments"] : json::object();
            if (hasId) {
                const auto progressToken = extractProgressToken(params);
                const bool progressEligible = progressToken.has_value() && adapter.isProgressEligibleTool(toolName);
                WorkerPool& pool = isExportToolName(toolName) ? exportPool : readPool;
                if (progressEligible) {
                    emitProgress(writer, progressToken, 5, 100, "queued " + toolName);
                }
                const bool accepted = pool.tryEnqueue([&writer, &adapter, requestId, toolName, args, progressToken, progressEligible]() {
                    try {
                        if (progressEligible) {
                            emitProgress(writer, progressToken, 35, 100, "dispatching " + toolName);
                            emitProgress(writer, progressToken, 70, 100, "running " + toolName);
                        }
                        const json result = adapter.callTool(toolName, args);
                        if (progressEligible) {
                            emitProgress(writer, progressToken, 90, 100, "finalizing " + toolName);
                            emitProgress(writer, progressToken, 100, 100, "finished " + toolName);
                        }
                        writer.write(makeResponse(requestId, result));
                    } catch (const std::exception& error) {
                        writer.write(makeErrorResponse(requestId, -32603, error.what()));
                    }
                });
                if (!accepted) {
                    writer.write(makeErrorResponse(requestId, kServerBusyCode, "tool request queue is full"));
                }
            }
            continue;
        }
        if (method.rfind("notifications/", 0) == 0) {
            continue;
        }

        if (hasId) {
            writer.write(makeErrorResponse(requestId, -32601, "method not found"));
        }
    }

    return EXIT_SUCCESS;
}
