#include "app_shared.h"
#include "tape_phase7_runtime_bridge.h"
#include "trading_runtime_host.h"

#include <algorithm>
#include <cstdlib>
#include <fcntl.h>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>
#include <unistd.h>

namespace {

using tape_phase7::json;

constexpr int kPhase7SmokeClientIdOffset = 700;

struct Options {
    bool startRuntime = false;
    bool recover = false;
    bool verboseRuntimeLogs = false;
    int clientId = 0;
    std::string actor;
};

class ScopedStdStreamSilence {
public:
    explicit ScopedStdStreamSilence(bool enabled) : enabled_(enabled) {
        if (!enabled_) {
            return;
        }
        nullFd_ = ::open("/dev/null", O_WRONLY);
        if (nullFd_ < 0) {
            enabled_ = false;
            return;
        }
        stdoutFd_ = ::dup(STDOUT_FILENO);
        stderrFd_ = ::dup(STDERR_FILENO);
        if (stdoutFd_ < 0 || stderrFd_ < 0 ||
            ::dup2(nullFd_, STDOUT_FILENO) < 0 || ::dup2(nullFd_, STDERR_FILENO) < 0) {
            restore();
            enabled_ = false;
        }
    }

    ~ScopedStdStreamSilence() {
        restore();
    }

    void restore() {
        if (!enabled_) {
            return;
        }
        if (stdoutFd_ >= 0) {
            ::dup2(stdoutFd_, STDOUT_FILENO);
            ::close(stdoutFd_);
            stdoutFd_ = -1;
        }
        if (stderrFd_ >= 0) {
            ::dup2(stderrFd_, STDERR_FILENO);
            ::close(stderrFd_);
            stderrFd_ = -1;
        }
        if (nullFd_ >= 0) {
            ::close(nullFd_);
            nullFd_ = -1;
        }
        enabled_ = false;
    }

private:
    bool enabled_ = false;
    int nullFd_ = -1;
    int stdoutFd_ = -1;
    int stderrFd_ = -1;
};

std::optional<int> parsePositiveInt(std::string_view text) {
    if (text.empty()) {
        return std::nullopt;
    }
    char* end = nullptr;
    const long parsed = std::strtol(std::string(text).c_str(), &end, 10);
    if (end == nullptr || *end != '\0' || parsed <= 0) {
        return std::nullopt;
    }
    return static_cast<int>(parsed);
}

RuntimeConnectionConfig smokeRuntimeConfig(int preferredClientId) {
    RuntimeConnectionConfig config = captureRuntimeConnectionConfig();
    config.websocketEnabled = false;
    config.controllerEnabled = false;
    config.clientId =
        preferredClientId > 0 ? preferredClientId : std::max(1, config.clientId + kPhase7SmokeClientIdOffset);
    return config;
}

json sweepResultToJson(const tape_phase7::RuntimeBridgeSweepResult& sweep) {
    return {
        {"scanned_journal_count", sweep.scannedJournalCount},
        {"updated_journal_count", sweep.updatedJournalCount},
        {"updated_apply_count", sweep.updatedApplyCount},
        {"updated_journal_artifact_ids", sweep.updatedJournalArtifactIds},
        {"updated_apply_artifact_ids", sweep.updatedApplyArtifactIds},
        {"updated_journal_entry_ids", sweep.updatedJournalEntryIds},
        {"updated_apply_entry_ids", sweep.updatedApplyEntryIds}
    };
}

void printUsage(const char* argv0) {
    std::cout
        << "Usage:\n"
        << "  " << argv0 << " [--start-runtime] [--recover --actor NAME] [--client-id N]\n"
        << "               [--verbose-runtime-logs]\n\n"
        << "Notes:\n"
        << "  - Without --start-runtime, this reports the current Phase 7 execution backlog and startup policy.\n"
        << "  - --start-runtime starts an isolated runtime with websocket/controller disabled.\n"
        << "  - --recover runs one runtime recovery sweep and returns both before/after reports.\n"
        << "  - Runtime logs are muted by default so stdout stays machine-readable JSON.\n";
}

} // namespace

int main(int argc, char** argv) {
    Options options;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--help" || arg == "-h") {
            printUsage(argv[0]);
            return 0;
        }
        if (arg == "--start-runtime") {
            options.startRuntime = true;
            continue;
        }
        if (arg == "--recover") {
            options.recover = true;
            continue;
        }
        if (arg == "--verbose-runtime-logs") {
            options.verboseRuntimeLogs = true;
            continue;
        }
        if (arg == "--actor" && i + 1 < argc) {
            options.actor = argv[++i];
            continue;
        }
        if (arg == "--client-id" && i + 1 < argc) {
            const auto parsed = parsePositiveInt(argv[++i]);
            if (!parsed.has_value()) {
                std::cerr << "Invalid --client-id value\n";
                return 2;
            }
            options.clientId = *parsed;
            continue;
        }
        std::cerr << "Unknown argument: " << arg << "\n";
        printUsage(argv[0]);
        return 2;
    }

    if (options.recover && options.actor.empty()) {
        std::cerr << "--recover requires --actor\n";
        return 2;
    }

    const RuntimeConnectionConfig originalConfig = captureRuntimeConnectionConfig();
    TradingRuntimeHost runtimeHost;
    bool runtimeConnected = false;
    ScopedStdStreamSilence mutedRuntimeLogs(options.startRuntime && !options.verboseRuntimeLogs);
    auto shutdownRuntime = [&]() {
        runtimeHost.shutdown();
        updateRuntimeConnectionConfig(originalConfig);
        mutedRuntimeLogs.restore();
    };

    if (options.startRuntime) {
        updateRuntimeConnectionConfig(smokeRuntimeConfig(options.clientId));
        runtimeConnected = runtimeHost.start([]() {}, [](TradingRuntimeControllerAction) {});
        runtimeHost.setControllerVibration(false);
    }

    std::string errorCode;
    std::string errorMessage;
    tape_phase7::RuntimeSmokeReport initialReport;
    if (!tape_phase7::captureRuntimeSmokeReport(options.startRuntime ? runtimeHost.runtime() : nullptr,
                                                !options.actor.empty(),
                                                &initialReport,
                                                &errorCode,
                                                &errorMessage)) {
        const json failure = {
            {"status", "error"},
            {"error_code", errorCode},
            {"error_message", errorMessage}
        };
        shutdownRuntime();
        std::cout << failure.dump(2) << std::endl;
        return 1;
    }

    json output = {
        {"status", "ok"},
        {"runtime_start",
         {
             {"requested", options.startRuntime},
             {"connected", runtimeConnected}
         }},
        {"initial_report", tape_phase7::runtimeSmokeReportToJson(initialReport)}
    };

    if (options.recover) {
        if (!options.startRuntime || runtimeHost.runtime() == nullptr) {
            const json failure = {
                {"status", "error"},
                {"error_code", "runtime_required"},
                {"error_message", "--recover requires a started runtime"}
            };
            shutdownRuntime();
            std::cout << failure.dump(2) << std::endl;
            return 1;
        }

        tape_phase7::RuntimeBridgeSweepResult sweep;
        if (!tape_phase7::reconcileExecutionArtifactsViaRuntime(runtimeHost.runtime(),
                                                                options.actor,
                                                                "phase7_execution_smoke",
                                                                &sweep,
                                                                &errorCode,
                                                                &errorMessage)) {
            const json failure = {
                {"status", "error"},
                {"error_code", errorCode},
                {"error_message", errorMessage}
            };
            shutdownRuntime();
            std::cout << failure.dump(2) << std::endl;
            return 1;
        }

        tape_phase7::RuntimeSmokeReport finalReport;
        if (!tape_phase7::captureRuntimeSmokeReport(runtimeHost.runtime(),
                                                    true,
                                                    &finalReport,
                                                    &errorCode,
                                                    &errorMessage)) {
            const json failure = {
                {"status", "error"},
                {"error_code", errorCode},
                {"error_message", errorMessage}
            };
            shutdownRuntime();
            std::cout << failure.dump(2) << std::endl;
            return 1;
        }

        output["recovery_sweep"] = sweepResultToJson(sweep);
        output["final_report"] = tape_phase7::runtimeSmokeReportToJson(finalReport);
    }

    shutdownRuntime();
    std::cout << output.dump(2) << std::endl;
    return 0;
}
