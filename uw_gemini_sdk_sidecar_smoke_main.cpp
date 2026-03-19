#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>

#include <nlohmann/json.hpp>

#include <sys/wait.h>
#include <unistd.h>

namespace fs = std::filesystem;
using json = nlohmann::json;

namespace {

constexpr const char* kSourceDir = TWS_GUI_SOURCE_DIR;
constexpr const char* kDefaultModel = "gemini-2.5-flash";

std::string shellEscape(const std::string& value) {
    std::string out = "'";
    for (char ch : value) {
        if (ch == '\'') {
            out += "'\\''";
        } else {
            out.push_back(ch);
        }
    }
    out.push_back('\'');
    return out;
}

std::string pythonExecutablePath() {
    const fs::path repoRoot = fs::path(kSourceDir);
    const fs::path venvPython = repoRoot / ".venv-gemini-uw" / "bin" / "python";
    if (fs::exists(venvPython)) {
        return venvPython.string();
    }
    return "python3";
}

std::string readFileOrEmpty(const fs::path& path) {
    std::ifstream input(path);
    if (!input.is_open()) {
        return {};
    }
    std::string output;
    std::string line;
    while (std::getline(input, line)) {
        output += line;
        output.push_back('\n');
    }
    return output;
}

int systemExitCode(int rawStatus) {
    if (rawStatus == -1) {
        return -1;
    }
    if (WIFEXITED(rawStatus)) {
        return WEXITSTATUS(rawStatus);
    }
    return rawStatus;
}

} // namespace

int main(int argc, char** argv) {
    std::string prompt = "Use get_market_state and tell me the latest market-wide options snapshot.";
    std::string model = kDefaultModel;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--model" && i + 1 < argc) {
            model = argv[++i];
            continue;
        }
        if (arg == "--help" || arg == "-h") {
            std::cout << argv[0] << " [--model MODEL] [PROMPT]\n";
            return 0;
        }
        prompt = arg;
    }

    const fs::path repoRoot = fs::path(kSourceDir);
    const std::string python = pythonExecutablePath();
    char stdoutTemplate[] = "/tmp/uw_gemini_sdk_sidecar_stdout.XXXXXX";
    char stderrTemplate[] = "/tmp/uw_gemini_sdk_sidecar_stderr.XXXXXX";
    const int stdoutFd = mkstemp(stdoutTemplate);
    const int stderrFd = mkstemp(stderrTemplate);
    if (stdoutFd == -1 || stderrFd == -1) {
        std::cerr << "Failed to create temporary files for sidecar output capture.\n";
        return 2;
    }
    close(stdoutFd);
    close(stderrFd);
    const fs::path stdoutPath(stdoutTemplate);
    const fs::path stderrPath(stderrTemplate);

    const std::string command =
        "cd " + shellEscape(repoRoot.string()) +
        " && " + shellEscape(python) +
        " tools/gemini_uw_mcp_session_async.py --model " + shellEscape(model) + " " + shellEscape(prompt) +
        " >" + shellEscape(stdoutPath.string()) + " 2>" + shellEscape(stderrPath.string());

    const auto start = std::chrono::steady_clock::now();
    const int rawExitCode = std::system(command.c_str());
    const auto stop = std::chrono::steady_clock::now();
    const auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(stop - start).count();
    const int exitCode = systemExitCode(rawExitCode);

    const std::string stdoutText = readFileOrEmpty(stdoutPath);
    const std::string stderrText = readFileOrEmpty(stderrPath);
    std::error_code ignored;
    fs::remove(stdoutPath, ignored);
    fs::remove(stderrPath, ignored);

    const json result = {
        {"command", command},
        {"model", model},
        {"elapsed_ms", elapsedMs},
        {"exit_code", exitCode},
        {"stdout", stdoutText},
        {"stderr", stderrText}
    };
    std::cout << result.dump(2) << std::endl;
    return exitCode == 0 ? 0 : 1;
}
