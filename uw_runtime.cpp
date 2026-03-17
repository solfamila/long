#include "uw_runtime.h"

#include <CommonCrypto/CommonDigest.h>

#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <map>
#include <sstream>

namespace uw_context_service {
namespace {

std::string trim(const std::string& value) {
    const std::size_t begin = value.find_first_not_of(" \t\r\n");
    if (begin == std::string::npos) {
        return {};
    }
    const std::size_t end = value.find_last_not_of(" \t\r\n");
    return value.substr(begin, end - begin + 1);
}

std::map<std::string, std::string> parseCredentialFile(const std::filesystem::path& path) {
    std::map<std::string, std::string> values;
    std::ifstream input(path);
    if (!input.is_open()) {
        return values;
    }

    std::string line;
    while (std::getline(input, line)) {
        std::string trimmed = trim(line);
        if (trimmed.empty() || trimmed.rfind("#", 0) == 0) {
            continue;
        }
        if (trimmed.rfind("export ", 0) == 0) {
            trimmed = trim(trimmed.substr(7));
        }
        const std::size_t separator = trimmed.find('=');
        if (separator == std::string::npos) {
            continue;
        }
        const std::string key = trim(trimmed.substr(0, separator));
        std::string value = trim(trimmed.substr(separator + 1));
        if (value.size() >= 2 &&
            ((value.front() == '"' && value.back() == '"') ||
             (value.front() == '\'' && value.back() == '\''))) {
            value = value.substr(1, value.size() - 2);
        }
        if (!key.empty()) {
            values[key] = value;
        }
    }
    return values;
}

const std::map<std::string, std::string>& localCredentialValues() {
    static const std::map<std::string, std::string> values = parseCredentialFile(localCredentialFilePath());
    return values;
}

std::tm gmTime(std::time_t value) {
    std::tm tm{};
#if defined(_WIN32)
    gmtime_s(&tm, &value);
#else
    gmtime_r(&value, &tm);
#endif
    return tm;
}

} // namespace

CredentialBinding loadCredentialBinding(std::initializer_list<std::string_view> envNames) {
    for (const std::string_view envName : envNames) {
        if (const char* value = std::getenv(std::string(envName).c_str()); value != nullptr && *value != '\0') {
            return {true, true, value, std::string(envName)};
        }
    }

    const auto& values = localCredentialValues();
    for (const std::string_view envName : envNames) {
        const auto it = values.find(std::string(envName));
        if (it != values.end() && !it->second.empty()) {
            return {true, false, it->second, std::string(envName)};
        }
    }
    return {};
}

std::string localCredentialFilePath() {
    if (const char* overridePath = std::getenv("LONG_CREDENTIAL_FILE"); overridePath != nullptr && *overridePath != '\0') {
        return overridePath;
    }
    return (std::filesystem::current_path() / ".env.local").string();
}

std::string stableHashHex(std::string_view value) {
    unsigned char digest[CC_SHA256_DIGEST_LENGTH];
    CC_SHA256(value.data(), static_cast<CC_LONG>(value.size()), digest);
    static constexpr char kHex[] = "0123456789abcdef";
    std::string out;
    out.reserve(CC_SHA256_DIGEST_LENGTH * 2);
    for (unsigned char byte : digest) {
        out.push_back(kHex[(byte >> 4) & 0x0fU]);
        out.push_back(kHex[byte & 0x0fU]);
    }
    return out;
}

std::string nowUtc() {
    const std::tm tm = gmTime(std::time(nullptr));
    std::ostringstream stream;
    stream << std::put_time(&tm, "%Y-%m-%dT%H:%M:%SZ");
    return stream.str();
}

std::uint64_t nowUnixSeconds() {
    return static_cast<std::uint64_t>(std::time(nullptr));
}

} // namespace uw_context_service