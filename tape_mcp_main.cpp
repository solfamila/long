#include "runtime_qos.h"
#include "runtime_registry.h"
#include "tape_mcp_adapter.h"

#include <cctype>
#include <cstdlib>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>

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
    while (true) {
        const ReadMessageResult incoming = readMessage(std::cin);
        if (incoming.eof) {
            break;
        }
        if (!incoming.ok) {
            writeMessage(std::cout, makeErrorResponse(nullptr, -32600, incoming.error));
            continue;
        }

        json request;
        try {
            request = json::parse(incoming.body);
        } catch (const std::exception& error) {
            writeMessage(std::cout, makeErrorResponse(nullptr, -32700, error.what()));
            continue;
        }

        if (!request.is_object()) {
            writeMessage(std::cout, makeErrorResponse(nullptr, -32600, "request must be an object"));
            continue;
        }
        if (request.value("jsonrpc", std::string()) != "2.0") {
            if (shouldRespond(request)) {
                writeMessage(std::cout, makeErrorResponse(request["id"], -32600, "jsonrpc must be 2.0"));
            }
            continue;
        }

        const bool hasId = shouldRespond(request);
        const json requestId = hasId ? request["id"] : nullptr;
        const std::string method = request.value("method", std::string());
        const json params = request.contains("params") ? request["params"] : json::object();

        if (method.empty()) {
            if (hasId) {
                writeMessage(std::cout, makeErrorResponse(requestId, -32600, "method is required"));
            }
            continue;
        }

        if (method == "initialize") {
            if (hasId) {
                writeMessage(std::cout, makeResponse(requestId, adapter.initializeResult()));
            }
            continue;
        }
        if (method == "ping") {
            if (hasId) {
                writeMessage(std::cout, makeResponse(requestId, json::object()));
            }
            continue;
        }
        if (method == "tools/list") {
            if (hasId) {
                writeMessage(std::cout, makeResponse(requestId, adapter.listToolsResult()));
            }
            continue;
        }
        if (method == "resources/list") {
            if (hasId) {
                writeMessage(std::cout, makeResponse(requestId, adapter.listResourcesResult()));
            }
            continue;
        }
        if (method == "resources/read") {
            if (!params.is_object()) {
                if (hasId) {
                    writeMessage(std::cout, makeErrorResponse(requestId, -32602, "resources/read params must be an object"));
                }
                continue;
            }
            const std::string resourceUri = params.value("uri", std::string());
            if (resourceUri.empty()) {
                if (hasId) {
                    writeMessage(std::cout, makeErrorResponse(requestId, -32602, "resources/read params.uri is required"));
                }
                continue;
            }
            if (hasId) {
                writeMessage(std::cout, makeResponse(requestId, adapter.readResourceResult(resourceUri)));
            }
            continue;
        }
        if (method == "prompts/list") {
            if (hasId) {
                writeMessage(std::cout, makeResponse(requestId, adapter.listPromptsResult()));
            }
            continue;
        }
        if (method == "prompts/get") {
            if (!params.is_object()) {
                if (hasId) {
                    writeMessage(std::cout, makeErrorResponse(requestId, -32602, "prompts/get params must be an object"));
                }
                continue;
            }
            const std::string promptName = params.value("name", std::string());
            if (promptName.empty()) {
                if (hasId) {
                    writeMessage(std::cout, makeErrorResponse(requestId, -32602, "prompts/get params.name is required"));
                }
                continue;
            }
            const json args = params.contains("arguments") ? params["arguments"] : json::object();
            if (hasId) {
                writeMessage(std::cout, makeResponse(requestId, adapter.getPromptResult(promptName, args)));
            }
            continue;
        }
        if (method == "tools/call") {
            if (!params.is_object()) {
                if (hasId) {
                    writeMessage(std::cout, makeErrorResponse(requestId, -32602, "tools/call params must be an object"));
                }
                continue;
            }
            const std::string toolName = params.value("name", std::string());
            if (toolName.empty()) {
                if (hasId) {
                    writeMessage(std::cout, makeErrorResponse(requestId, -32602, "tools/call params.name is required"));
                }
                continue;
            }
            const json args = params.contains("arguments") ? params["arguments"] : json::object();
            if (hasId) {
                const auto progressToken = extractProgressToken(params);
                if (progressToken.has_value() && adapter.isProgressEligibleTool(toolName)) {
                    writeMessage(std::cout, makeProgressNotification(*progressToken, 0, 100, "starting " + toolName));
                }
                const json result = adapter.callTool(toolName, args);
                if (progressToken.has_value() && adapter.isProgressEligibleTool(toolName)) {
                    writeMessage(std::cout, makeProgressNotification(*progressToken, 100, 100, "finished " + toolName));
                }
                writeMessage(std::cout, makeResponse(requestId, result));
            }
            continue;
        }
        if (method.rfind("notifications/", 0) == 0) {
            continue;
        }

        if (hasId) {
            writeMessage(std::cout, makeErrorResponse(requestId, -32601, "method not found"));
        }
    }

    return EXIT_SUCCESS;
}
