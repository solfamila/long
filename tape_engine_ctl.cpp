#include "tape_engine_client.h"

#include <cstdlib>
#include <iostream>
#include <string>

namespace {

std::string envOrDefault(const char* key, const std::string& fallback) {
    const char* value = std::getenv(key);
    if (value != nullptr && value[0] != '\0') {
        return value;
    }
    return fallback;
}

void printUsage() {
    std::cout << "Usage:\n"
              << "  tape_engine_ctl status\n"
              << "  tape_engine_ctl read-live-tail [limit]\n"
              << "  tape_engine_ctl read-range <from_session_seq> <to_session_seq> [--revision N] [--include-live-tail] [--limit N]\n"
              << "  tape_engine_ctl replay-snapshot <session_seq> [--revision N] [--include-live-tail] [--depth N]\n"
              << "  tape_engine_ctl find-order [--trace-id N] [--order-id N] [--perm-id N] [--exec-id ID] [--revision N] [--include-live-tail] [--limit N]\n"
              << "  tape_engine_ctl list-order-anchors [--revision N] [--include-live-tail] [--limit N]\n"
              << "  tape_engine_ctl list-protected-windows [--revision N] [--include-live-tail] [--limit N]\n"
              << "  tape_engine_ctl read-protected-window <window_id> [--revision N] [--include-live-tail] [--limit N]\n"
              << "  tape_engine_ctl list-findings [--revision N] [--include-live-tail] [--limit N]\n"
              << "  tape_engine_ctl list-incidents [--revision N] [--include-live-tail] [--limit N]\n";
}

} // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        printUsage();
        return 1;
    }

    tape_engine::QueryRequest request;
    request.requestId = "ctl";
    request.operation = argv[1];

    if (request.operation == "read-live-tail") {
        request.operation = "read_live_tail";
        if (argc >= 3) {
            request.limit = static_cast<std::size_t>(std::stoull(argv[2]));
        }
    } else if (request.operation == "read-range") {
        request.operation = "read_range";
        if (argc < 4) {
            printUsage();
            return 1;
        }
        request.fromSessionSeq = std::stoull(argv[2]);
        request.toSessionSeq = std::stoull(argv[3]);
        for (int i = 4; i < argc; ++i) {
            const std::string arg = argv[i];
            if (arg == "--include-live-tail") {
                request.includeLiveTail = true;
            } else if (arg == "--revision" && i + 1 < argc) {
                request.revisionId = std::stoull(argv[++i]);
            } else if (arg == "--limit" && i + 1 < argc) {
                request.limit = static_cast<std::size_t>(std::stoull(argv[++i]));
            }
        }
    } else if (request.operation == "replay-snapshot") {
        request.operation = "replay_snapshot";
        if (argc < 3) {
            printUsage();
            return 1;
        }
        request.targetSessionSeq = std::stoull(argv[2]);
        for (int i = 3; i < argc; ++i) {
            const std::string arg = argv[i];
            if (arg == "--include-live-tail") {
                request.includeLiveTail = true;
            } else if (arg == "--revision" && i + 1 < argc) {
                request.revisionId = std::stoull(argv[++i]);
            } else if (arg == "--depth" && i + 1 < argc) {
                request.limit = static_cast<std::size_t>(std::stoull(argv[++i]));
            }
        }
    } else if (request.operation == "find-order") {
        request.operation = "find_order_anchor";
        for (int i = 2; i < argc; ++i) {
            const std::string arg = argv[i];
            if (arg == "--trace-id" && i + 1 < argc) {
                request.traceId = std::stoull(argv[++i]);
            } else if (arg == "--order-id" && i + 1 < argc) {
                request.orderId = std::stoll(argv[++i]);
            } else if (arg == "--perm-id" && i + 1 < argc) {
                request.permId = std::stoll(argv[++i]);
            } else if (arg == "--exec-id" && i + 1 < argc) {
                request.execId = argv[++i];
            } else if (arg == "--revision" && i + 1 < argc) {
                request.revisionId = std::stoull(argv[++i]);
            } else if (arg == "--include-live-tail") {
                request.includeLiveTail = true;
            } else if (arg == "--limit" && i + 1 < argc) {
                request.limit = static_cast<std::size_t>(std::stoull(argv[++i]));
            }
        }
    } else if (request.operation == "read-protected-window") {
        request.operation = "read_protected_window";
        if (argc < 3) {
            printUsage();
            return 1;
        }
        request.windowId = std::stoull(argv[2]);
        for (int i = 3; i < argc; ++i) {
            const std::string arg = argv[i];
            if (arg == "--revision" && i + 1 < argc) {
                request.revisionId = std::stoull(argv[++i]);
            } else if (arg == "--include-live-tail") {
                request.includeLiveTail = true;
            } else if (arg == "--limit" && i + 1 < argc) {
                request.limit = static_cast<std::size_t>(std::stoull(argv[++i]));
            }
        }
    } else if (request.operation == "list-order-anchors" ||
               request.operation == "list-protected-windows" ||
               request.operation == "list-findings" ||
               request.operation == "list-incidents") {
        if (request.operation == "list-order-anchors") {
            request.operation = "list_order_anchors";
        } else if (request.operation == "list-protected-windows") {
            request.operation = "list_protected_windows";
        } else if (request.operation == "list-findings") {
            request.operation = "list_findings";
        } else {
            request.operation = "list_incidents";
        }
        for (int i = 2; i < argc; ++i) {
            const std::string arg = argv[i];
            if (arg == "--revision" && i + 1 < argc) {
                request.revisionId = std::stoull(argv[++i]);
            } else if (arg == "--include-live-tail") {
                request.includeLiveTail = true;
            } else if (arg == "--limit" && i + 1 < argc) {
                request.limit = static_cast<std::size_t>(std::stoull(argv[++i]));
            }
        }
    } else if (request.operation == "status") {
        request.operation = "status";
    } else {
        printUsage();
        return 1;
    }

    tape_engine::Client client(envOrDefault("LONG_TAPE_ENGINE_SOCKET", "/tmp/tape-engine.sock"));
    tape_engine::QueryResponse response;
    std::string error;
    if (!client.query(request, &response, &error)) {
        std::cerr << "Query failed: " << error << '\n';
        return 1;
    }

    std::cout << tape_engine::queryResponseToJson(response).dump(2) << '\n';
    return 0;
}
