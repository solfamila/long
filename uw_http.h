#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace uw_context_service {

struct HttpResponse {
    bool curlOk = false;
    long statusCode = 0;
    std::uint64_t latencyMs = 0;
    std::string body;
    std::string curlError;
    std::map<std::string, std::string> headers;
};

HttpResponse httpGet(const std::string& url,
                     const std::vector<std::string>& headers,
                     long timeoutMs);
HttpResponse httpPostJson(const std::string& url,
                          const std::vector<std::string>& headers,
                          const std::string& body,
                          long timeoutMs);
bool httpSuccess(const HttpResponse& response);
void ensureCurlReady();

} // namespace uw_context_service
