#include "tape_engine.h"

#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <pthread.h>
#include <signal.h>
#include <string>

std::string envOrDefault(const char* key, const std::string& fallback) {
    const char* value = std::getenv(key);
    if (value != nullptr && value[0] != '\0') {
        return value;
    }
    return fallback;
}

int main(int argc, char** argv) {
    (void)argc;
    (void)argv;

    tape_engine::EngineConfig config;
    config.socketPath = envOrDefault("LONG_TAPE_ENGINE_SOCKET", config.socketPath);
    config.dataDir = envOrDefault("LONG_TAPE_ENGINE_DATA_DIR",
                                  (std::filesystem::temp_directory_path() / "tape-engine").string());

    sigset_t signals;
    sigemptyset(&signals);
    sigaddset(&signals, SIGINT);
    sigaddset(&signals, SIGTERM);
    if (pthread_sigmask(SIG_BLOCK, &signals, nullptr) != 0) {
        std::cerr << "Failed to configure tape_engine signal mask\n";
        return 1;
    }

    tape_engine::Server server(config);
    std::string error;
    if (!server.start(&error)) {
        std::cerr << "Failed to start tape_engine: " << error << '\n';
        return 1;
    }

    std::cout << "tape_engine listening on " << config.socketPath << '\n';
    std::cout << "tape_engine data dir " << config.dataDir.string() << '\n';

    int signalNumber = 0;
    if (sigwait(&signals, &signalNumber) != 0) {
        std::cerr << "Failed while waiting for shutdown signal\n";
        server.stop();
        return 1;
    }

    server.stop();
    return 0;
}
