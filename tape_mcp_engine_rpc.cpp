#include "tape_mcp_engine_rpc.h"

#include <utility>

namespace tape_mcp {

namespace {

tapescope::ClientConfig buildQueryClientConfig(EngineRpcClientConfig& config,
                                               std::string& configError) {
    if (config.socketPath.empty()) {
        config.socketPath = tapescope::defaultSocketPath();
    }
    if (config.clientName.empty()) {
        config.clientName = "tape-mcp";
    }
    if (config.socketPath.empty()) {
        configError = "engine socket path is empty";
    }

    tapescope::ClientConfig queryConfig;
    queryConfig.socketPath = config.socketPath;
    queryConfig.clientName = config.clientName;
    return queryConfig;
}

EngineRpcErrorKind mapErrorKind(const tapescope::QueryErrorKind kind) {
    switch (kind) {
        case tapescope::QueryErrorKind::None:
            return EngineRpcErrorKind::None;
        case tapescope::QueryErrorKind::Transport:
            return EngineRpcErrorKind::Transport;
        case tapescope::QueryErrorKind::Remote:
            return EngineRpcErrorKind::Remote;
        case tapescope::QueryErrorKind::MalformedResponse:
            return EngineRpcErrorKind::MalformedResponse;
        case tapescope::QueryErrorKind::SeamUnavailable:
            return EngineRpcErrorKind::SeamUnavailable;
    }
    return EngineRpcErrorKind::MalformedResponse;
}

bool isRetryable(const EngineRpcErrorKind kind) {
    return kind == EngineRpcErrorKind::Transport;
}

EngineRpcError makeError(const EngineRpcErrorKind kind, std::string message) {
    EngineRpcError error;
    error.kind = kind;
    error.code = errorCodeForKind(kind);
    error.message = std::move(message);
    error.retryable = isRetryable(kind);
    return error;
}

template <typename T>
EngineRpcResult<T> makeSuccess(T value) {
    EngineRpcResult<T> result;
    result.value = std::move(value);
    return result;
}

template <typename T>
EngineRpcResult<T> makeFailure(const EngineRpcErrorKind kind, std::string message) {
    EngineRpcResult<T> result;
    result.error = makeError(kind, std::move(message));
    return result;
}

} // namespace

EngineRpcSession::EngineRpcSession(const tapescope::QueryClient* client, std::string configError)
    : client_(client),
      configError_(std::move(configError)) {}

EngineRpcResult<json> EngineRpcSession::query(const std::string& command, const json& args) const {
    if (!configError_.empty()) {
        return makeFailure<json>(EngineRpcErrorKind::Configuration, configError_);
    }
    if (client_ == nullptr) {
        return makeFailure<json>(EngineRpcErrorKind::Configuration, "engine RPC client is not initialized");
    }

    const tapescope::QueryResult<json> response = client_->query(command, args);
    if (response.ok()) {
        return makeSuccess(response.value);
    }
    return makeFailure<json>(mapErrorKind(response.error.kind), response.error.message);
}

EngineRpcClient::EngineRpcClient(EngineRpcClientConfig config)
    : config_(std::move(config)),
      queryClient_(buildQueryClientConfig(config_, configError_)) {}

const EngineRpcClientConfig& EngineRpcClient::config() const {
    return config_;
}

EngineRpcSession EngineRpcClient::openSession() const {
    return EngineRpcSession(&queryClient_, configError_);
}

std::string errorCodeForKind(const EngineRpcErrorKind kind) {
    switch (kind) {
        case EngineRpcErrorKind::None:
            return "ok";
        case EngineRpcErrorKind::Configuration:
            return "adapter_config_error";
        case EngineRpcErrorKind::Transport:
            return "engine_unavailable";
        case EngineRpcErrorKind::Remote:
            return "engine_query_failed";
        case EngineRpcErrorKind::MalformedResponse:
            return "malformed_response";
        case EngineRpcErrorKind::SeamUnavailable:
            return "seam_unavailable";
    }
    return "internal_error";
}

} // namespace tape_mcp

