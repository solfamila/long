#include "tape_engine_ingest.h"

#include <csignal>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string>

namespace {

tape_engine::Daemon* g_daemon = nullptr;

void handleSignal(int) {
    if (g_daemon != nullptr) {
        g_daemon->requestStop();
    }
}

std::string socketPathFromArgs(int argc, char** argv) {
    std::string socketPath = "/tmp/tape-engine.sock";
    for (int index = 1; index < argc; ++index) {
        const std::string arg = argv[index];
        if (arg == "--socket-path" && index + 1 < argc) {
            socketPath = argv[++index];
            continue;
        }
        if (arg.rfind("--socket-path=", 0) == 0) {
            socketPath = arg.substr(std::string("--socket-path=").size());
            continue;
        }
        throw std::runtime_error("unsupported argument: " + arg);
    }
    return socketPath;
}

} // namespace

int main(int argc, char** argv) {
    try {
        tape_engine::InMemoryLog log;
        tape_engine::IngestService ingest(log);
        tape_engine::Daemon daemon({socketPathFromArgs(argc, argv)}, ingest);
        g_daemon = &daemon;

        std::signal(SIGINT, handleSignal);
        std::signal(SIGTERM, handleSignal);

        std::string error;
        if (!daemon.run(&error)) {
            std::cerr << error << '\n';
            return EXIT_FAILURE;
        }
        return EXIT_SUCCESS;
    } catch (const std::exception& ex) {
        std::cerr << ex.what() << '\n';
        return EXIT_FAILURE;
    }
}
