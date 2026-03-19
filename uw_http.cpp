#include "uw_http.h"

#include <curl/curl.h>

#include <chrono>
#include <cctype>

namespace uw_context_service {
namespace {

size_t curlWriteCallback(char* ptr, size_t size, size_t nmemb, void* userdata) {
    const std::size_t total = size * nmemb;
    auto* out = static_cast<std::string*>(userdata);
    out->append(ptr, total);
    return total;
}

std::string trimAscii(std::string value) {
    while (!value.empty() && std::isspace(static_cast<unsigned char>(value.front())) != 0) {
        value.erase(value.begin());
    }
    while (!value.empty() && std::isspace(static_cast<unsigned char>(value.back())) != 0) {
        value.pop_back();
    }
    return value;
}

std::string toLowerAscii(std::string value) {
    for (char& ch : value) {
        ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    }
    return value;
}

size_t curlHeaderCallback(char* buffer, size_t size, size_t nitems, void* userdata) {
    const std::size_t total = size * nitems;
    auto* out = static_cast<std::map<std::string, std::string>*>(userdata);
    std::string line(buffer, total);
    const std::size_t separator = line.find(':');
    if (separator == std::string::npos) {
        return total;
    }
    std::string key = toLowerAscii(trimAscii(line.substr(0, separator)));
    std::string value = trimAscii(line.substr(separator + 1));
    if (!key.empty()) {
        (*out)[key] = value;
    }
    return total;
}

HttpResponse performRequest(const std::string& url,
                            const std::vector<std::string>& headers,
                            const std::string* body,
                            long timeoutMs) {
    HttpResponse response;
    CURL* curl = curl_easy_init();
    if (curl == nullptr) {
        response.curlError = "curl_easy_init failed";
        return response;
    }

    std::string responseBody;
    std::string errorBuffer(CURL_ERROR_SIZE, '\0');
    struct curl_slist* headerList = nullptr;
    for (const std::string& header : headers) {
        headerList = curl_slist_append(headerList, header.c_str());
    }

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, timeoutMs);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, timeoutMs);
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "long-uw-context/0.1");
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curlWriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &responseBody);
    curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, curlHeaderCallback);
    curl_easy_setopt(curl, CURLOPT_HEADERDATA, &response.headers);
    curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, errorBuffer.data());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headerList);
    if (body != nullptr) {
        curl_easy_setopt(curl, CURLOPT_POST, 1L);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body->c_str());
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, static_cast<long>(body->size()));
    } else {
        curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    }

    const auto start = std::chrono::steady_clock::now();
    const CURLcode code = curl_easy_perform(curl);
    const auto stop = std::chrono::steady_clock::now();
    response.latencyMs = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(stop - start).count());
    response.body = std::move(responseBody);
    response.curlOk = (code == CURLE_OK);
    if (!response.curlOk) {
        response.curlError = errorBuffer.data();
        if (response.curlError.empty()) {
            response.curlError = curl_easy_strerror(code);
        }
    }
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response.statusCode);

    curl_slist_free_all(headerList);
    curl_easy_cleanup(curl);
    return response;
}

struct CurlGlobalInit {
    CurlGlobalInit() {
        curl_global_init(CURL_GLOBAL_DEFAULT);
    }
    ~CurlGlobalInit() {
        curl_global_cleanup();
    }
};

CurlGlobalInit& curlGlobalInit() {
    static CurlGlobalInit init;
    return init;
}

} // namespace

HttpResponse httpGet(const std::string& url,
                     const std::vector<std::string>& headers,
                     long timeoutMs) {
    static_cast<void>(curlGlobalInit());
    return performRequest(url, headers, nullptr, timeoutMs);
}

HttpResponse httpPostJson(const std::string& url,
                          const std::vector<std::string>& headers,
                          const std::string& body,
                          long timeoutMs) {
    static_cast<void>(curlGlobalInit());
    return performRequest(url, headers, &body, timeoutMs);
}

bool httpSuccess(const HttpResponse& response) {
    return response.curlOk && response.statusCode >= 200 && response.statusCode < 300;
}

void ensureCurlReady() {
    static_cast<void>(curlGlobalInit());
}

} // namespace uw_context_service
