#pragma once

#import <Foundation/Foundation.h>

#include <functional>
#include <memory>

class TradingRefreshScheduler {
public:
    using Callback = std::function<void()>;

    TradingRefreshScheduler();
    ~TradingRefreshScheduler();

    TradingRefreshScheduler(const TradingRefreshScheduler&) = delete;
    TradingRefreshScheduler& operator=(const TradingRefreshScheduler&) = delete;

    void setCallback(Callback callback);
    void setAppActive(bool active);
    void setThermalState(NSProcessInfoThermalState thermalState);
    void schedule();
    void cancel();

private:
    struct Impl;
    std::shared_ptr<Impl> impl_;
};
