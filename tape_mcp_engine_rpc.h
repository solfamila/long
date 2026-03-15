#pragma once

#include <string>

#include <nlohmann/json.hpp>

#include "tapescope_client.h"

namespace tape_mcp {

using json = nlohmann::json;

enum class EngineRpcErrorKind {
    None,
    Configuration,
    Transport,
    Remote,
    MalformedResponse,
    SeamUnavailable
};

struct EngineRpcError {
    EngineRpcErrorKind kind = EngineRpcErrorKind::None;
    std::string code = "ok";
    std::string message;
    bool retryable = false;
};

template <typename T>
struct EngineRpcResult {
    T value{};
    EngineRpcError error{};

    [[nodiscard]] bool ok() const {
        return error.kind == EngineRpcErrorKind::None;
    }
};

struct EngineRpcClientConfig {
    std::string socketPath;
    std::string clientName = "tape-mcp";
};

class EngineRpcSession {
public:
    EngineRpcSession(const tapescope::QueryClient* client, std::string configError);

    [[nodiscard]] EngineRpcResult<json> query(const std::string& command,
                                              const json& args = json::object()) const;

private:
    const tapescope::QueryClient* client_ = nullptr;
    std::string configError_;
};

class EngineRpcClient {
public:
    explicit EngineRpcClient(EngineRpcClientConfig config = {});

    [[nodiscard]] const EngineRpcClientConfig& config() const;
    [[nodiscard]] EngineRpcSession openSession() const;

private:
    EngineRpcClientConfig config_;
    std::string configError_;
    tapescope::QueryClient queryClient_;
};

std::string errorCodeForKind(EngineRpcErrorKind kind);

} // namespace tape_mcp

