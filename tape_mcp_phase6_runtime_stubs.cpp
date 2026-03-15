#include "app_shared.h"
#include "trace_exporter.h"
#include "trading_ui_format.h"

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iomanip>
#include <sstream>
#include <system_error>

namespace {

const std::string& resolvedPhase6DataDirectory() {
    static const std::string path = []() -> std::string {
        namespace fs = std::filesystem;

        fs::path basePath;
        if (const char* overridePath = std::getenv("TWS_GUI_DATA_DIR");
            overridePath != nullptr && *overridePath != '\0') {
            basePath = fs::path(overridePath);
        } else if (const char* home = std::getenv("HOME");
                   home != nullptr && *home != '\0') {
            basePath = fs::path(home) / "Library" / "Application Support" / "TWS Trading GUI";
        } else {
            basePath = fs::current_path() / "tws_gui_data";
        }

        std::error_code ec;
        fs::create_directories(basePath, ec);
        return basePath.string();
    }();
    return path;
}

template <typename TimePoint>
bool hasTime(TimePoint value) {
    return value.time_since_epoch().count() != 0;
}

} // namespace

std::string appDataDirectory() {
    return resolvedPhase6DataDirectory();
}

std::string tradeTraceLogPath() {
    return (std::filesystem::path(resolvedPhase6DataDirectory()) / TRADE_TRACE_LOG_FILENAME).string();
}

std::string runtimeJournalLogPath() {
    return (std::filesystem::path(resolvedPhase6DataDirectory()) / RUNTIME_JOURNAL_LOG_FILENAME).string();
}

std::vector<std::pair<OrderId, OrderInfo>> captureOrdersSnapshot() {
    return {};
}

std::vector<TradeTraceListItem> captureTradeTraceListItems(std::size_t maxItems) {
    return buildTradeTraceListItemsFromLog(maxItems, tradeTraceLogPath());
}

TradeTraceSnapshot captureTradeTraceSnapshot(std::uint64_t traceId) {
    TradeTraceSnapshot snapshot;
    replayTradeTraceSnapshotFromLog(traceId, &snapshot, nullptr, tradeTraceLogPath());
    return snapshot;
}

BridgeOutboxSnapshot captureBridgeOutboxSnapshot(std::size_t) {
    return {};
}

BridgeDispatchSnapshot captureBridgeDispatchSnapshot(std::size_t) {
    return {};
}

std::string formatOrderLocalStateText(const OrderInfo&) {
    return {};
}

std::string formatOrderWatchdogText(const OrderInfo&) {
    return {};
}

std::string formatWallTime(std::chrono::system_clock::time_point tp) {
    if (!hasTime(tp)) {
        return {};
    }

    const auto timeT = std::chrono::system_clock::to_time_t(tp);
    std::tm tmLocal{};
#if defined(_WIN32)
    localtime_s(&tmLocal, &timeT);
#else
    localtime_r(&timeT, &tmLocal);
#endif
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()) % 1000;

    std::ostringstream oss;
    oss << std::put_time(&tmLocal, "%H:%M:%S")
        << '.'
        << std::setfill('0')
        << std::setw(3)
        << ms.count();
    return oss.str();
}

double durationMs(std::chrono::steady_clock::time_point start, std::chrono::steady_clock::time_point end) {
    if (!hasTime(start) || !hasTime(end)) {
        return -1.0;
    }
    return std::chrono::duration<double, std::milli>(end - start).count();
}

std::string tradeEventTypeToString(TradeEventType type) {
    switch (type) {
        case TradeEventType::Trigger: return "Trigger";
        case TradeEventType::ValidationStart: return "ValidationStart";
        case TradeEventType::ValidationOk: return "ValidationOk";
        case TradeEventType::ValidationFailed: return "ValidationFailed";
        case TradeEventType::PlaceOrderCallStart: return "PlaceOrderCallStart";
        case TradeEventType::PlaceOrderCallEnd: return "PlaceOrderCallEnd";
        case TradeEventType::OpenOrderSeen: return "OpenOrderSeen";
        case TradeEventType::OrderStatusSeen: return "OrderStatusSeen";
        case TradeEventType::ExecDetailsSeen: return "ExecDetailsSeen";
        case TradeEventType::CommissionSeen: return "CommissionSeen";
        case TradeEventType::ErrorSeen: return "ErrorSeen";
        case TradeEventType::CancelRequestSent: return "CancelRequestSent";
        case TradeEventType::CancelAck: return "CancelAck";
        case TradeEventType::FinalState: return "FinalState";
        case TradeEventType::Note: return "Note";
        default: return "Unknown";
    }
}
