#include "tape_engine.h"

#include <chrono>
#include <atomic>
#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <string>
#include <thread>

namespace {

std::atomic<bool> g_running{true};

void handleSignal(int) {
    g_running.store(false, std::memory_order_relaxed);
}

std::string envOrDefault(const char* key, const std::string& fallback) {
    const char* value = std::getenv(key);
    if (value != nullptr && value[0] != '\0') {
        return value;
    }
    return fallback;
}

} // namespace

int main(int argc, char** argv) {
    (void)argc;
    (void)argv;

    tape_engine::EngineConfig config;
    config.socketPath = envOrDefault("LONG_TAPE_ENGINE_SOCKET", config.socketPath);
    config.dataDir = envOrDefault("LONG_TAPE_ENGINE_DATA_DIR",
                                  (std::filesystem::temp_directory_path() / "tape-engine").string());

    tape_engine::Server server(config);
    std::string error;
    if (!server.start(&error)) {
        std::cerr << "Failed to start tape_engine: " << error << '\n';
        return 1;
    }

    std::signal(SIGINT, handleSignal);
    std::signal(SIGTERM, handleSignal);

    std::cout << "tape_engine listening on " << config.socketPath << '\n';
    std::cout << "tape_engine data dir " << config.dataDir.string() << '\n';

    while (g_running.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    server.stop();
    return 0;
}
