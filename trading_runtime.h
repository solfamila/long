#pragma once

#include <functional>
#include <memory>

class EClientSocket;

enum class TradingRuntimeControllerAction {
    Buy,
    ToggleQuantity,
    Close,
    CancelAll,
};

class TradingRuntime {
public:
    using UiInvalidationCallback = std::function<void()>;
    using ControllerActionCallback = std::function<void(TradingRuntimeControllerAction)>;

    TradingRuntime();
    ~TradingRuntime();

    TradingRuntime(const TradingRuntime&) = delete;
    TradingRuntime& operator=(const TradingRuntime&) = delete;

    void setUiInvalidationCallback(UiInvalidationCallback callback);
    void setControllerActionCallback(ControllerActionCallback callback);

    bool start();
    void shutdown();

    EClientSocket* client() const;
    bool isStarted() const;
    void setControllerVibration(bool enable);

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};
