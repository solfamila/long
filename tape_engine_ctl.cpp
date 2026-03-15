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
              << "  tape_engine_ctl read-session-overview [--from N] [--to N] [--revision N] [--include-live-tail] [--limit N]\n"
              << "  tape_engine_ctl scan-session-report [--from N] [--to N] [--revision N] [--limit N]\n"
              << "  tape_engine_ctl read-session-report <report_id>\n"
              << "  tape_engine_ctl export-session-bundle <report_id>\n"
              << "  tape_engine_ctl read-artifact <artifact_id> [--revision N] [--include-live-tail] [--limit N]\n"
              << "  tape_engine_ctl export-artifact <artifact_id> <markdown|json-bundle> [--revision N] [--include-live-tail] [--limit N]\n"
              << "  tape_engine_ctl list-session-reports [--revision N] [--limit N]\n"
              << "  tape_engine_ctl scan-incident-report <logical_incident_id> [--revision N] [--limit N]\n"
              << "  tape_engine_ctl scan-order-case-report [--trace-id N] [--order-id N] [--perm-id N] [--exec-id ID] [--revision N] [--limit N]\n"
              << "  tape_engine_ctl read-case-report <report_id>\n"
              << "  tape_engine_ctl export-case-bundle <report_id>\n"
              << "  tape_engine_ctl list-case-reports [--revision N] [--limit N]\n"
              << "  tape_engine_ctl import-case-bundle <bundle_path>\n"
              << "  tape_engine_ctl list-imported-cases [--limit N]\n"
              << "  tape_engine_ctl read-session-quality [--from N] [--to N] [--revision N] [--include-live-tail]\n"
              << "  tape_engine_ctl replay-snapshot <session_seq> [--revision N] [--include-live-tail] [--depth N]\n"
              << "  tape_engine_ctl find-order [--trace-id N] [--order-id N] [--perm-id N] [--exec-id ID] [--revision N] [--include-live-tail] [--limit N]\n"
              << "  tape_engine_ctl seek-order [--trace-id N] [--order-id N] [--perm-id N] [--exec-id ID] [--revision N] [--include-live-tail] [--limit N]\n"
              << "  tape_engine_ctl read-order-case [--trace-id N] [--order-id N] [--perm-id N] [--exec-id ID] [--revision N] [--include-live-tail] [--limit N]\n"
              << "  tape_engine_ctl read-order-anchor <anchor_id> [--revision N] [--include-live-tail] [--limit N]\n"
              << "  tape_engine_ctl list-order-anchors [--revision N] [--include-live-tail] [--limit N]\n"
              << "  tape_engine_ctl list-protected-windows [--revision N] [--include-live-tail] [--limit N]\n"
              << "  tape_engine_ctl read-protected-window <window_id> [--revision N] [--include-live-tail] [--limit N]\n"
              << "  tape_engine_ctl list-findings [--revision N] [--include-live-tail] [--limit N]\n"
              << "  tape_engine_ctl read-finding <finding_id> [--revision N] [--include-live-tail] [--limit N]\n"
              << "  tape_engine_ctl read-incident <logical_incident_id> [--revision N] [--include-live-tail] [--limit N]\n"
              << "  tape_engine_ctl list-incidents [--revision N] [--include-live-tail] [--limit N]\n";
}

} // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        printUsage();
        return 1;
    }

    const std::string command = tape_engine::canonicalizeQueryOperationName(argv[1]);
    tape_engine::QueryRequest request;
    const auto setOperation = [&](tape_engine::QueryOperation operation) {
        request = tape_engine::makeQueryRequest(operation, "ctl");
    };

    if (command == "read_live_tail") {
        setOperation(tape_engine::QueryOperation::ReadLiveTail);
        if (argc >= 3) {
            request.limit = static_cast<std::size_t>(std::stoull(argv[2]));
        }
    } else if (command == "read_session_overview") {
        setOperation(tape_engine::QueryOperation::ReadSessionOverview);
        for (int i = 2; i < argc; ++i) {
            const std::string arg = argv[i];
            if (arg == "--from" && i + 1 < argc) {
                request.fromSessionSeq = std::stoull(argv[++i]);
            } else if (arg == "--to" && i + 1 < argc) {
                request.toSessionSeq = std::stoull(argv[++i]);
            } else if (arg == "--include-live-tail") {
                request.includeLiveTail = true;
            } else if (arg == "--revision" && i + 1 < argc) {
                request.revisionId = std::stoull(argv[++i]);
            } else if (arg == "--limit" && i + 1 < argc) {
                request.limit = static_cast<std::size_t>(std::stoull(argv[++i]));
            }
        }
    } else if (command == "scan_session_report") {
        setOperation(tape_engine::QueryOperation::ScanSessionReport);
        for (int i = 2; i < argc; ++i) {
            const std::string arg = argv[i];
            if (arg == "--from" && i + 1 < argc) {
                request.fromSessionSeq = std::stoull(argv[++i]);
            } else if (arg == "--to" && i + 1 < argc) {
                request.toSessionSeq = std::stoull(argv[++i]);
            } else if (arg == "--revision" && i + 1 < argc) {
                request.revisionId = std::stoull(argv[++i]);
            } else if (arg == "--limit" && i + 1 < argc) {
                request.limit = static_cast<std::size_t>(std::stoull(argv[++i]));
            }
        }
    } else if (command == "read_session_report") {
        setOperation(tape_engine::QueryOperation::ReadSessionReport);
        if (argc < 3) {
            printUsage();
            return 1;
        }
        request.reportId = std::stoull(argv[2]);
    } else if (command == "export_session_bundle") {
        setOperation(tape_engine::QueryOperation::ExportSessionBundle);
        if (argc < 3) {
            printUsage();
            return 1;
        }
        request.reportId = std::stoull(argv[2]);
    } else if (command == "read_artifact") {
        setOperation(tape_engine::QueryOperation::ReadArtifact);
        if (argc < 3) {
            printUsage();
            return 1;
        }
        request.artifactId = argv[2];
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
    } else if (command == "export_artifact") {
        setOperation(tape_engine::QueryOperation::ExportArtifact);
        if (argc < 4) {
            printUsage();
            return 1;
        }
        request.artifactId = argv[2];
        request.exportFormat = argv[3];
        for (int i = 4; i < argc; ++i) {
            const std::string arg = argv[i];
            if (arg == "--revision" && i + 1 < argc) {
                request.revisionId = std::stoull(argv[++i]);
            } else if (arg == "--include-live-tail") {
                request.includeLiveTail = true;
            } else if (arg == "--limit" && i + 1 < argc) {
                request.limit = static_cast<std::size_t>(std::stoull(argv[++i]));
            }
        }
    } else if (command == "list_session_reports") {
        setOperation(tape_engine::QueryOperation::ListSessionReports);
        for (int i = 2; i < argc; ++i) {
            const std::string arg = argv[i];
            if (arg == "--revision" && i + 1 < argc) {
                request.revisionId = std::stoull(argv[++i]);
            } else if (arg == "--limit" && i + 1 < argc) {
                request.limit = static_cast<std::size_t>(std::stoull(argv[++i]));
            }
        }
    } else if (command == "scan_incident_report") {
        setOperation(tape_engine::QueryOperation::ScanIncidentReport);
        if (argc < 3) {
            printUsage();
            return 1;
        }
        request.logicalIncidentId = std::stoull(argv[2]);
        for (int i = 3; i < argc; ++i) {
            const std::string arg = argv[i];
            if (arg == "--revision" && i + 1 < argc) {
                request.revisionId = std::stoull(argv[++i]);
            } else if (arg == "--limit" && i + 1 < argc) {
                request.limit = static_cast<std::size_t>(std::stoull(argv[++i]));
            }
        }
    } else if (command == "scan_order_case_report") {
        setOperation(tape_engine::QueryOperation::ScanOrderCaseReport);
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
            } else if (arg == "--limit" && i + 1 < argc) {
                request.limit = static_cast<std::size_t>(std::stoull(argv[++i]));
            }
        }
    } else if (command == "read_case_report") {
        setOperation(tape_engine::QueryOperation::ReadCaseReport);
        if (argc < 3) {
            printUsage();
            return 1;
        }
        request.reportId = std::stoull(argv[2]);
    } else if (command == "export_case_bundle") {
        setOperation(tape_engine::QueryOperation::ExportCaseBundle);
        if (argc < 3) {
            printUsage();
            return 1;
        }
        request.reportId = std::stoull(argv[2]);
    } else if (command == "list_case_reports") {
        setOperation(tape_engine::QueryOperation::ListCaseReports);
        for (int i = 2; i < argc; ++i) {
            const std::string arg = argv[i];
            if (arg == "--revision" && i + 1 < argc) {
                request.revisionId = std::stoull(argv[++i]);
            } else if (arg == "--limit" && i + 1 < argc) {
                request.limit = static_cast<std::size_t>(std::stoull(argv[++i]));
            }
        }
    } else if (command == "import_case_bundle") {
        setOperation(tape_engine::QueryOperation::ImportCaseBundle);
        if (argc < 3) {
            printUsage();
            return 1;
        }
        request.bundlePath = argv[2];
    } else if (command == "list_imported_cases") {
        setOperation(tape_engine::QueryOperation::ListImportedCases);
        for (int i = 2; i < argc; ++i) {
            const std::string arg = argv[i];
            if (arg == "--limit" && i + 1 < argc) {
                request.limit = static_cast<std::size_t>(std::stoull(argv[++i]));
            }
        }
    } else if (command == "read_session_quality") {
        setOperation(tape_engine::QueryOperation::ReadSessionQuality);
        for (int i = 2; i < argc; ++i) {
            const std::string arg = argv[i];
            if (arg == "--from" && i + 1 < argc) {
                request.fromSessionSeq = std::stoull(argv[++i]);
            } else if (arg == "--to" && i + 1 < argc) {
                request.toSessionSeq = std::stoull(argv[++i]);
            } else if (arg == "--include-live-tail") {
                request.includeLiveTail = true;
            } else if (arg == "--revision" && i + 1 < argc) {
                request.revisionId = std::stoull(argv[++i]);
            }
        }
    } else if (command == "read_finding") {
        setOperation(tape_engine::QueryOperation::ReadFinding);
        if (argc < 3) {
            printUsage();
            return 1;
        }
        request.findingId = std::stoull(argv[2]);
        for (int i = 3; i < argc; ++i) {
            const std::string arg = argv[i];
            if (arg == "--include-live-tail") {
                request.includeLiveTail = true;
            } else if (arg == "--revision" && i + 1 < argc) {
                request.revisionId = std::stoull(argv[++i]);
            } else if (arg == "--limit" && i + 1 < argc) {
                request.limit = static_cast<std::size_t>(std::stoull(argv[++i]));
            }
        }
    } else if (command == "read_range") {
        setOperation(tape_engine::QueryOperation::ReadRange);
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
    } else if (command == "replay_snapshot") {
        setOperation(tape_engine::QueryOperation::ReplaySnapshot);
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
    } else if (command == "find_order") {
        setOperation(tape_engine::QueryOperation::FindOrderAnchor);
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
    } else if (command == "seek_order") {
        setOperation(tape_engine::QueryOperation::SeekOrderAnchor);
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
    } else if (command == "read_order_case") {
        setOperation(tape_engine::QueryOperation::ReadOrderCase);
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
    } else if (command == "read_order_anchor") {
        setOperation(tape_engine::QueryOperation::ReadOrderAnchor);
        if (argc < 3) {
            printUsage();
            return 1;
        }
        request.anchorId = std::stoull(argv[2]);
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
    } else if (command == "read_protected_window") {
        setOperation(tape_engine::QueryOperation::ReadProtectedWindow);
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
    } else if (command == "read_incident") {
        setOperation(tape_engine::QueryOperation::ReadIncident);
        if (argc < 3) {
            printUsage();
            return 1;
        }
        request.logicalIncidentId = std::stoull(argv[2]);
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
    } else if (command == "list_order_anchors" ||
               command == "list_protected_windows" ||
               command == "list_findings" ||
               command == "list_incidents") {
        if (command == "list_order_anchors") {
            setOperation(tape_engine::QueryOperation::ListOrderAnchors);
        } else if (command == "list_protected_windows") {
            setOperation(tape_engine::QueryOperation::ListProtectedWindows);
        } else if (command == "list_findings") {
            setOperation(tape_engine::QueryOperation::ListFindings);
        } else {
            setOperation(tape_engine::QueryOperation::ListIncidents);
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
    } else if (command == "status") {
        setOperation(tape_engine::QueryOperation::Status);
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
