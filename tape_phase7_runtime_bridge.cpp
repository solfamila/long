#include "tape_phase7_runtime_bridge.h"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <optional>
#include <sstream>
#include <set>

namespace tape_phase7 {
namespace {

std::string trimAscii(std::string_view text) {
    std::size_t begin = 0;
    while (begin < text.size() && static_cast<unsigned char>(text[begin]) <= 0x20) {
        ++begin;
    }
    std::size_t end = text.size();
    while (end > begin && static_cast<unsigned char>(text[end - 1]) <= 0x20) {
        --end;
    }
    return std::string(text.substr(begin, end - begin));
}

std::string utcTimestampIso8601Now() {
    const auto now = std::chrono::system_clock::now();
    const auto time = std::chrono::system_clock::to_time_t(now);
    std::tm tm{};
#if defined(_WIN32)
    gmtime_s(&tm, &time);
#else
    gmtime_r(&time, &tm);
#endif
    const auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
    std::ostringstream out;
    out << std::put_time(&tm, "%Y-%m-%dT%H:%M:%S") << '.'
        << std::setw(3) << std::setfill('0') << millis.count() << 'Z';
    return out.str();
}

std::optional<std::chrono::system_clock::time_point> parseUtcTimestampIso8601(std::string_view text) {
    const std::string trimmed = trimAscii(text);
    if (trimmed.empty()) {
        return std::nullopt;
    }

    std::tm tm{};
    std::istringstream input(trimmed);
    input >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S");
    if (input.fail()) {
        return std::nullopt;
    }

    int millis = 0;
    if (input.peek() == '.') {
        input.get();
        std::string digits;
        while (std::isdigit(input.peek())) {
            digits.push_back(static_cast<char>(input.get()));
        }
        while (digits.size() < 3) {
            digits.push_back('0');
        }
        if (!digits.empty()) {
            millis = std::stoi(digits.substr(0, 3));
        }
    }
    if (input.peek() == 'Z') {
        input.get();
    }
    if (input.peek() != std::char_traits<char>::eof()) {
        return std::nullopt;
    }

#if defined(_WIN32)
    const std::time_t seconds = _mkgmtime(&tm);
#else
    const std::time_t seconds = timegm(&tm);
#endif
    if (seconds < 0) {
        return std::nullopt;
    }

    return std::chrono::system_clock::from_time_t(seconds) + std::chrono::milliseconds(millis);
}

std::optional<std::chrono::system_clock::time_point> journalEntryTimestamp(const ExecutionJournalEntry& entry) {
    if (const auto parsed = parseUtcTimestampIso8601(entry.lastUpdatedAtUtc)) {
        return parsed;
    }
    if (const auto parsed = parseUtcTimestampIso8601(entry.startedAtUtc)) {
        return parsed;
    }
    return parseUtcTimestampIso8601(entry.queuedAtUtc);
}

bool isStaleRuntimeBridgeEntry(const ExecutionJournalEntry& entry,
                               std::chrono::system_clock::time_point now) {
    constexpr auto kRuntimeBridgeRecoveryGracePeriod = std::chrono::seconds(30);
    const auto timestamp = journalEntryTimestamp(entry);
    if (!timestamp.has_value()) {
        return false;
    }
    return now - *timestamp >= kRuntimeBridgeRecoveryGracePeriod;
}

std::string fnv1aHex(std::string_view value) {
    std::uint64_t hash = 1469598103934665603ULL;
    for (const unsigned char ch : value) {
        hash ^= static_cast<std::uint64_t>(ch);
        hash *= 1099511628211ULL;
    }
    std::ostringstream out;
    out << std::hex << std::setw(16) << std::setfill('0') << hash;
    return out.str();
}

std::optional<OrderId> firstRequestedOrderId(const json& executionRequest) {
    if (!executionRequest.is_object()) {
        return std::nullopt;
    }
    const auto parsePositiveOrderId = [](const json& value) -> std::optional<OrderId> {
        if (value.is_number_integer()) {
            const auto parsed = value.get<long long>();
            if (parsed > 0) {
                return static_cast<OrderId>(parsed);
            }
        } else if (value.is_number_unsigned()) {
            const auto parsed = value.get<unsigned long long>();
            if (parsed > 0) {
                return static_cast<OrderId>(parsed);
            }
        }
        return std::nullopt;
    };
    const auto firstPositiveArrayOrderId = [&](std::string_view key) -> std::optional<OrderId> {
        const json values = executionRequest.value(std::string(key), json::array());
        if (!values.is_array() || values.empty()) {
            return std::nullopt;
        }
        return parsePositiveOrderId(values.front());
    };

    if (const auto requested = firstPositiveArrayOrderId("requested_order_ids")) {
        return requested;
    }
    if (const auto accepted = firstPositiveArrayOrderId("accepted_order_ids")) {
        return accepted;
    }
    if (executionRequest.contains("order_id")) {
        if (const auto direct = parsePositiveOrderId(executionRequest.at("order_id"))) {
            return direct;
        }
    }
    const json brokerIdentity = executionRequest.value("broker_identity", json::object());
    if (brokerIdentity.is_object() && brokerIdentity.contains("order_id")) {
        if (const auto brokerOrderId = parsePositiveOrderId(brokerIdentity.at("order_id"))) {
            return brokerOrderId;
        }
    }
    const json anchor = executionRequest.value("anchor", json::object());
    if (anchor.is_object() && anchor.contains("order_id")) {
        if (const auto anchorOrderId = parsePositiveOrderId(anchor.at("order_id"))) {
            return anchorOrderId;
        }
    }
    return std::nullopt;
}

template <typename Int>
std::optional<Int> positiveJsonInteger(const json& value) {
    if (value.is_number_integer()) {
        const auto parsed = value.get<long long>();
        if (parsed > 0) {
            return static_cast<Int>(parsed);
        }
    }
    if (value.is_number_unsigned()) {
        const auto parsed = value.get<unsigned long long>();
        if (parsed > 0) {
            return static_cast<Int>(parsed);
        }
    }
    return std::nullopt;
}

std::optional<std::uint64_t> fallbackTraceIdFromContext(const json& context) {
    if (!context.is_object()) {
        return std::nullopt;
    }
    if (context.contains("trace_id")) {
        if (const auto parsed = positiveJsonInteger<std::uint64_t>(context.at("trace_id"))) {
            return parsed;
        }
    }
    const json brokerIdentity = context.value("broker_identity", json::object());
    if (brokerIdentity.is_object() && brokerIdentity.contains("trace_id")) {
        if (const auto parsed = positiveJsonInteger<std::uint64_t>(brokerIdentity.at("trace_id"))) {
            return parsed;
        }
    }
    const json anchor = context.value("anchor", json::object());
    if (anchor.is_object() && anchor.contains("trace_id")) {
        if (const auto parsed = positiveJsonInteger<std::uint64_t>(anchor.at("trace_id"))) {
            return parsed;
        }
    }
    return std::nullopt;
}

std::optional<long long> fallbackPermIdFromContext(const json& context) {
    if (!context.is_object()) {
        return std::nullopt;
    }
    if (context.contains("perm_id")) {
        if (const auto parsed = positiveJsonInteger<long long>(context.at("perm_id"))) {
            return parsed;
        }
    }
    const json brokerIdentity = context.value("broker_identity", json::object());
    if (brokerIdentity.is_object() && brokerIdentity.contains("perm_id")) {
        if (const auto parsed = positiveJsonInteger<long long>(brokerIdentity.at("perm_id"))) {
            return parsed;
        }
    }
    const json anchor = context.value("anchor", json::object());
    if (anchor.is_object() && anchor.contains("perm_id")) {
        if (const auto parsed = positiveJsonInteger<long long>(anchor.at("perm_id"))) {
            return parsed;
        }
    }
    return std::nullopt;
}

std::set<std::string> fallbackExecIdsFromContext(const json& context) {
    std::set<std::string> execIds;
    if (!context.is_object()) {
        return execIds;
    }
    const json brokerIdentity = context.value("broker_identity", json::object());
    const json source = brokerIdentity.is_object() ? brokerIdentity : context;
    const json execIdRows = source.value("exec_ids", json::array());
    if (execIdRows.is_array()) {
        for (const auto& item : execIdRows) {
            if (item.is_string() && !trimAscii(item.get_ref<const std::string&>()).empty()) {
                execIds.insert(item.get<std::string>());
            }
        }
    }
    if (source.contains("latest_exec_id") && source.at("latest_exec_id").is_string() &&
        !trimAscii(source.at("latest_exec_id").get_ref<const std::string&>()).empty()) {
        execIds.insert(source.at("latest_exec_id").get<std::string>());
    }
    return execIds;
}

bool isRuntimeDispatchable(const ExecutionJournalEntry& entry) {
    return entry.executionStatus == kExecutionEntryStatusQueued &&
        entry.executionRequest.is_object() &&
        trimAscii(entry.executionRequest.value("operation", std::string())) == "request_order_reconciliation" &&
        firstRequestedOrderId(entry.executionRequest).has_value();
}

RuntimePresentationSnapshot captureSnapshot(TradingRuntime* runtime) {
    if (runtime != nullptr) {
        return runtime->capturePresentationSnapshot({}, 200);
    }
    return captureRuntimePresentationSnapshot({}, 200);
}

std::uint64_t lookupTraceId(TradingRuntime* runtime, OrderId orderId) {
    if (runtime != nullptr) {
        return runtime->findTradeTraceIdByOrderId(orderId);
    }
    return findTraceIdByOrderId(orderId);
}

TradeTraceSnapshot captureTraceSnapshot(TradingRuntime* runtime, std::uint64_t traceId) {
    if (traceId == 0) {
        return {};
    }
    if (runtime != nullptr) {
        return runtime->captureTradeTraceSnapshot(traceId);
    }
    return captureTradeTraceSnapshot(traceId);
}

std::vector<OrderId> requestReconciliation(TradingRuntime* runtime,
                                           OrderId orderId,
                                           const std::string& reason) {
    if (runtime != nullptr) {
        return runtime->requestOrderReconciliation({orderId}, reason);
    }
    const auto sweep = requestOrderReconciliation({orderId}, reason);
    std::vector<OrderId> accepted;
    accepted.reserve(sweep.reconciliationOrders.size());
    for (const auto& item : sweep.reconciliationOrders) {
        accepted.push_back(item.orderId);
    }
    return accepted;
}

const OrderInfo* findOrderInfo(const RuntimePresentationSnapshot& snapshot, OrderId orderId) {
    const auto it = std::find_if(snapshot.orders.begin(),
                                 snapshot.orders.end(),
                                 [&](const auto& item) { return item.first == orderId; });
    return it == snapshot.orders.end() ? nullptr : &it->second;
}

bool isTerminalOrderState(const OrderInfo& order) {
    switch (order.localState) {
        case LocalOrderState::Filled:
        case LocalOrderState::Cancelled:
        case LocalOrderState::Rejected:
        case LocalOrderState::Inactive:
        case LocalOrderState::NeedsManualReview:
            return true;
        default:
            return false;
    }
}

bool isStillReconcilingOrderState(const OrderInfo& order) {
    switch (order.localState) {
        case LocalOrderState::IntentAccepted:
        case LocalOrderState::SentToBroker:
        case LocalOrderState::NeedsReconciliation:
        case LocalOrderState::AwaitingBrokerEcho:
        case LocalOrderState::AwaitingCancelAck:
        case LocalOrderState::CancelRequested:
            return true;
        default:
            return false;
    }
}

bool isCancelPendingOrderState(const OrderInfo& order) {
    return order.cancelPending ||
        order.localState == LocalOrderState::CancelRequested ||
        order.localState == LocalOrderState::AwaitingCancelAck;
}

double contextFilledQty(const json& context) {
    if (!context.is_object()) {
        return 0.0;
    }
    const json filled = context.value("filled_qty", json(nullptr));
    if (filled.is_number()) {
        return filled.get<double>();
    }
    return 0.0;
}

bool hasFillEvidence(const OrderInfo* order,
                     const TradeTraceSnapshot& traceSnapshot,
                     const json& context,
                     const std::set<std::string>& execIds) {
    if (order != nullptr && order->filledQty > 0.0) {
        return true;
    }
    if (traceSnapshot.found && !traceSnapshot.trace.fills.empty()) {
        return true;
    }
    if (contextFilledQty(context) > 0.0) {
        return true;
    }
    return !execIds.empty();
}

std::string fillStateForOrder(const OrderInfo* order,
                              const TradeTraceSnapshot& traceSnapshot,
                              const json& context,
                              const std::set<std::string>& execIds) {
    const bool filled = hasFillEvidence(order, traceSnapshot, context, execIds);
    if (order == nullptr) {
        return filled ? "partial_fill_recovery_unknown" : "no_fill_evidence";
    }
    switch (order->localState) {
        case LocalOrderState::PartiallyFilled:
            return "partial_fill_in_progress";
        case LocalOrderState::Filled:
            return "filled";
        case LocalOrderState::Cancelled:
            return filled ? "cancelled_after_partial_fill" : "cancelled_unfilled";
        case LocalOrderState::Rejected:
            return filled ? "rejected_after_partial_fill" : "rejected_unfilled";
        case LocalOrderState::Inactive:
            return filled ? "inactive_after_partial_fill" : "inactive_unfilled";
        case LocalOrderState::CancelRequested:
        case LocalOrderState::AwaitingCancelAck:
            return filled ? "partial_fill_cancel_pending" : "cancel_pending";
        case LocalOrderState::Working:
            return filled ? "working_after_partial_fill" : "working_unfilled";
        case LocalOrderState::NeedsManualReview:
            return filled ? "manual_review_after_partial_fill" : "manual_review_unfilled";
        default:
            return filled ? "fill_observed" : "no_fill_evidence";
    }
}

std::string restartResumePolicyForOrder(const OrderInfo* order, std::string_view resolution) {
    if (resolution == "manual_review_required") {
        return "manual_review_required";
    }
    if (order == nullptr) {
        return "unknown";
    }
    switch (order->localState) {
        case LocalOrderState::IntentAccepted:
        case LocalOrderState::SentToBroker:
        case LocalOrderState::NeedsReconciliation:
        case LocalOrderState::AwaitingBrokerEcho:
        case LocalOrderState::Working:
        case LocalOrderState::PartiallyFilled:
            return "continue_recovery";
        case LocalOrderState::CancelRequested:
        case LocalOrderState::AwaitingCancelAck:
            return "await_cancel_ack";
        case LocalOrderState::Cancelled:
        case LocalOrderState::Filled:
            return "completed";
        case LocalOrderState::Rejected:
        case LocalOrderState::Inactive:
            return "terminal_no_resume";
        case LocalOrderState::NeedsManualReview:
            return "manual_review_required";
    }
    return "unknown";
}

std::string restartRecoveryStateForOrder(const OrderInfo* order, std::string_view resolution) {
    if (resolution == "manual_review_required") {
        return "manual_review_required";
    }
    if (order == nullptr) {
        return "unknown";
    }
    switch (order->localState) {
        case LocalOrderState::IntentAccepted:
        case LocalOrderState::SentToBroker:
        case LocalOrderState::NeedsReconciliation:
        case LocalOrderState::AwaitingBrokerEcho:
        case LocalOrderState::Working:
        case LocalOrderState::PartiallyFilled:
            return "recoverable";
        case LocalOrderState::CancelRequested:
        case LocalOrderState::AwaitingCancelAck:
            return "awaiting_cancel_ack";
        case LocalOrderState::Cancelled:
        case LocalOrderState::Filled:
            return "terminal_completed";
        case LocalOrderState::Rejected:
        case LocalOrderState::Inactive:
            return "terminal_no_resume";
        case LocalOrderState::NeedsManualReview:
            return "manual_review_required";
    }
    return "unknown";
}

std::string restartRecoveryReasonForOrder(const OrderInfo* order,
                                          const TradeTraceSnapshot& traceSnapshot,
                                          const json& context,
                                          const std::set<std::string>& execIds,
                                          std::string_view resolution) {
    const bool filled = hasFillEvidence(order, traceSnapshot, context, execIds);
    if (order == nullptr) {
        if (resolution == "manual_review_required") {
            return filled ? "missing_order_after_partial_fill" : "missing_order_unfilled";
        }
        return filled ? "recovery_unknown_with_fill_evidence" : "recovery_unknown_without_fill_evidence";
    }
    if (resolution == "manual_review_required") {
        if (isCancelPendingOrderState(*order)) {
            return filled ? "cancel_ack_timeout_after_partial_fill" : "cancel_ack_timeout_unfilled";
        }
        if (isStillReconcilingOrderState(*order)) {
            return filled ? "reconciliation_timeout_after_partial_fill" : "reconciliation_timeout_unfilled";
        }
        return filled ? "manual_review_after_partial_fill" : "manual_review_unfilled";
    }
    switch (order->localState) {
        case LocalOrderState::IntentAccepted:
            return "intent_accepted_pending_dispatch";
        case LocalOrderState::SentToBroker:
            return "sent_to_broker_pending_echo";
        case LocalOrderState::NeedsReconciliation:
            return "needs_reconciliation";
        case LocalOrderState::AwaitingBrokerEcho:
            return filled ? "broker_echo_pending_after_partial_fill" : "broker_echo_pending_unfilled";
        case LocalOrderState::Working:
            return filled ? "working_after_partial_fill" : "working_unfilled";
        case LocalOrderState::PartiallyFilled:
            return "partial_fill_active";
        case LocalOrderState::CancelRequested:
        case LocalOrderState::AwaitingCancelAck:
            return filled ? "cancel_ack_pending_after_partial_fill" : "cancel_ack_pending_unfilled";
        case LocalOrderState::Filled:
            return "terminal_filled";
        case LocalOrderState::Cancelled:
            return filled ? "terminal_cancelled_after_partial_fill" : "terminal_cancelled_unfilled";
        case LocalOrderState::Rejected:
            return filled ? "terminal_rejected_after_partial_fill" : "terminal_rejected_unfilled";
        case LocalOrderState::Inactive:
            return filled ? "terminal_inactive_after_partial_fill" : "terminal_inactive_unfilled";
        case LocalOrderState::NeedsManualReview:
            return filled ? "manual_review_after_partial_fill" : "manual_review_unfilled";
    }
    return filled ? "recovery_unknown_with_fill_evidence" : "recovery_unknown_without_fill_evidence";
}

std::string brokerStatusDetailForContext(const TradeTraceSnapshot& traceSnapshot, const json& context) {
    if (traceSnapshot.found && !trimAscii(traceSnapshot.trace.latestError).empty()) {
        return trimAscii(traceSnapshot.trace.latestError);
    }
    if (!context.is_object()) {
        return {};
    }
    if (context.contains("broker_status_detail") && context.at("broker_status_detail").is_string()) {
        return trimAscii(context.at("broker_status_detail").get_ref<const std::string&>());
    }
    const json tradeTrace = context.value("trade_trace", json::object());
    if (tradeTrace.is_object() && tradeTrace.contains("latest_error") && tradeTrace.at("latest_error").is_string()) {
        return trimAscii(tradeTrace.at("latest_error").get_ref<const std::string&>());
    }
    return {};
}

std::string runtimeResolutionForOrderState(const OrderInfo* order, std::string_view fallback) {
    if (order == nullptr) {
        return std::string(fallback);
    }
    switch (order->localState) {
        case LocalOrderState::IntentAccepted:
        case LocalOrderState::SentToBroker:
        case LocalOrderState::NeedsReconciliation:
            return "reconciling";
        case LocalOrderState::AwaitingBrokerEcho:
            return "broker_echo_pending";
        case LocalOrderState::AwaitingCancelAck:
            return "cancel_ack_pending";
        case LocalOrderState::CancelRequested:
            return "cancel_requested";
        case LocalOrderState::NeedsManualReview:
            return "manual_review_required";
        case LocalOrderState::Working:
            return "resolved_working";
        case LocalOrderState::PartiallyFilled:
            return "resolved_partially_filled";
        case LocalOrderState::Filled:
            return "resolved_filled";
        case LocalOrderState::Cancelled:
            return "resolved_cancelled";
        case LocalOrderState::Rejected:
            return "resolved_rejected";
        case LocalOrderState::Inactive:
            return "resolved_inactive";
    }
    return std::string(fallback);
}

std::string executionStatusForResolvedOrder(const OrderInfo& order) {
    switch (order.localState) {
        case LocalOrderState::Cancelled:
            return kExecutionEntryStatusCancelled;
        case LocalOrderState::Rejected:
        case LocalOrderState::Inactive:
        case LocalOrderState::NeedsManualReview:
            return kExecutionEntryStatusFailed;
        case LocalOrderState::Filled:
            return kExecutionEntryStatusSucceeded;
        case LocalOrderState::Working:
        case LocalOrderState::PartiallyFilled:
            return kExecutionEntryStatusSubmitted;
        default:
            return kExecutionEntryStatusSubmitted;
    }
}

std::string failureCodeForResolvedOrder(const OrderInfo& order) {
    switch (order.localState) {
        case LocalOrderState::NeedsManualReview:
            return "manual_review_required";
        case LocalOrderState::Rejected:
            return "runtime_rejected";
        case LocalOrderState::Inactive:
            return "runtime_inactive";
        default:
            return {};
    }
}

std::string failureMessageForResolvedOrder(const OrderInfo& order, const TradeTraceSnapshot& traceSnapshot) {
    std::string message;
    switch (order.localState) {
        case LocalOrderState::NeedsManualReview:
            message = "Runtime reconciliation escalated the order to manual review.";
            break;
        case LocalOrderState::Rejected:
            message = "Runtime reconciliation resolved the order as rejected.";
            break;
        case LocalOrderState::Inactive:
            message = "Runtime reconciliation resolved the order as inactive.";
            break;
        default:
            return {};
    }
    if (traceSnapshot.found && !trimAscii(traceSnapshot.trace.latestError).empty()) {
        message += " Latest broker detail: " + trimAscii(traceSnapshot.trace.latestError);
    }
    return message;
}

json runtimeReconciliationResult(const OrderInfo* order,
                                 OrderId orderId,
                                 const TradeTraceSnapshot& traceSnapshot,
                                 const json& context,
                                 std::string_view resolution,
                                 bool terminal) {
    std::set<std::string> execIds;
    if (order != nullptr) {
        execIds.insert(order->seenExecIds.begin(), order->seenExecIds.end());
    }
    if (traceSnapshot.found) {
        for (const auto& fill : traceSnapshot.trace.fills) {
            if (!trimAscii(fill.execId).empty()) {
                execIds.insert(fill.execId);
            }
        }
    }
    const auto contextExecIds = fallbackExecIdsFromContext(context);
    execIds.insert(contextExecIds.begin(), contextExecIds.end());

    json execIdsJson = json::array();
    for (const auto& execId : execIds) {
        execIdsJson.push_back(execId);
    }

    std::string latestExecId;
    if (traceSnapshot.found) {
        for (auto it = traceSnapshot.trace.fills.rbegin(); it != traceSnapshot.trace.fills.rend(); ++it) {
            if (!trimAscii(it->execId).empty()) {
                latestExecId = it->execId;
                break;
            }
        }
    }
    if (latestExecId.empty() && !execIds.empty()) {
        latestExecId = *execIds.rbegin();
    }

    const auto traceId = traceSnapshot.found
        ? traceSnapshot.trace.traceId
        : fallbackTraceIdFromContext(context).value_or(0ULL);
    const auto permId = traceSnapshot.found
        ? traceSnapshot.trace.permId
        : fallbackPermIdFromContext(context).value_or(0LL);
    const bool partialFillBeforeTerminal = terminal && hasFillEvidence(order, traceSnapshot, context, execIds);
    const std::string fillState = fillStateForOrder(order, traceSnapshot, context, execIds);
    const std::string restartResumePolicy = restartResumePolicyForOrder(order, resolution);
    const std::string restartRecoveryState = restartRecoveryStateForOrder(order, resolution);
    const std::string restartRecoveryReason =
        restartRecoveryReasonForOrder(order, traceSnapshot, context, execIds, resolution);
    const std::string brokerStatusDetail = brokerStatusDetailForContext(traceSnapshot, context);
    json result = {
        {"runtime_operation", "request_order_reconciliation"},
        {"order_id", static_cast<long long>(orderId)},
        {"requested_order_ids", json::array({static_cast<long long>(orderId)})},
        {"accepted_order_ids", json::array({static_cast<long long>(orderId)})},
        {"trace_id", traceId == 0 ? json(nullptr) : json(traceId)},
        {"order_status", order != nullptr ? json(order->status) : json(nullptr)},
        {"local_state", order != nullptr ? json(localOrderStateToString(order->localState)) : json(nullptr)},
        {"resolution", std::string(resolution)},
        {"terminal", terminal},
        {"cancel_ack_pending", order != nullptr ? json(isCancelPendingOrderState(*order)) : json(nullptr)},
        {"filled_qty", order != nullptr ? json(order->filledQty) : json(nullptr)},
        {"remaining_qty", order != nullptr ? json(order->remainingQty) : json(nullptr)},
        {"avg_fill_price", order != nullptr ? json(order->avgFillPrice) : json(nullptr)},
        {"requested_qty", order != nullptr ? json(order->quantity) : json(nullptr)},
        {"limit_price", order != nullptr ? json(order->limitPrice) : json(nullptr)},
        {"fill_state", fillState},
        {"partial_fill_before_terminal", partialFillBeforeTerminal},
        {"restart_resume_policy", restartResumePolicy},
        {"restart_recovery_state", restartRecoveryState},
        {"restart_recovery_reason", restartRecoveryReason},
        {"latest_exec_id", latestExecId.empty() ? json(nullptr) : json(latestExecId)},
        {"broker_status_detail", brokerStatusDetail.empty() ? json(nullptr) : json(brokerStatusDetail)},
        {"symbol", order != nullptr ? json(order->symbol) : traceSnapshot.found ? json(traceSnapshot.trace.symbol) : json(nullptr)},
        {"side", order != nullptr ? json(order->side) : traceSnapshot.found ? json(traceSnapshot.trace.side) : json(nullptr)},
        {"account", order != nullptr ? json(order->account) : traceSnapshot.found ? json(traceSnapshot.trace.account) : json(nullptr)},
        {"cancel_pending", order != nullptr ? json(order->cancelPending) : json(nullptr)},
        {"last_reconciliation_reason", order != nullptr && !trimAscii(order->lastReconciliationReason).empty()
                                          ? json(order->lastReconciliationReason)
                                          : json(nullptr)},
        {"broker_identity",
         {
             {"order_id", static_cast<long long>(orderId)},
             {"trace_id", traceId == 0 ? json(nullptr) : json(traceId)},
             {"perm_id", permId <= 0 ? json(nullptr) : json(permId)},
             {"exec_ids", std::move(execIdsJson)},
             {"latest_exec_id", latestExecId.empty() ? json(nullptr) : json(latestExecId)}
         }},
        {"trade_trace",
         {
             {"trace_found", traceSnapshot.found},
             {"trace_id", traceId == 0 ? json(nullptr) : json(traceId)},
             {"perm_id", permId <= 0 ? json(nullptr) : json(permId)},
             {"latest_status", traceSnapshot.found && !trimAscii(traceSnapshot.trace.latestStatus).empty()
                                   ? json(traceSnapshot.trace.latestStatus)
                                   : json(nullptr)},
             {"terminal_status", traceSnapshot.found && !trimAscii(traceSnapshot.trace.terminalStatus).empty()
                                     ? json(traceSnapshot.trace.terminalStatus)
                                     : json(nullptr)},
             {"latest_error", traceSnapshot.found && !trimAscii(traceSnapshot.trace.latestError).empty()
                                   ? json(traceSnapshot.trace.latestError)
                                   : json(nullptr)},
             {"fill_count", traceSnapshot.found ? json(traceSnapshot.trace.fills.size()) : json(0)},
             {"total_commission", traceSnapshot.found ? json(traceSnapshot.trace.totalCommission) : json(nullptr)},
             {"commission_currency", traceSnapshot.found && !trimAscii(traceSnapshot.trace.commissionCurrency).empty()
                                         ? json(traceSnapshot.trace.commissionCurrency)
                                         : json(nullptr)}
         }},
        {"manual_review_required",
         resolution == "manual_review_required" ||
             (order != nullptr && order->localState == LocalOrderState::NeedsManualReview)}
    };
    return result;
}

json journalAuditEvent(const ExecutionJournalArtifact& artifact,
                       std::string_view eventType,
                       std::string_view actor,
                       std::string_view comment,
                       const std::vector<std::string>& updatedEntryIds,
                       const std::string& generatedAtUtc) {
    std::ostringstream seed;
    seed << artifact.journalArtifact.artifactId << "|" << eventType << "|" << actor << "|" << generatedAtUtc;
    for (const auto& entryId : updatedEntryIds) {
        seed << "|" << entryId;
    }
    return {
        {"event_id", "phase7-journal-event-" + fnv1aHex(seed.str())},
        {"event_type", std::string(eventType)},
        {"generated_at_utc", generatedAtUtc},
        {"actor", actor.empty() ? json(nullptr) : json(std::string(actor))},
        {"comment", comment.empty() ? json(nullptr) : json(std::string(comment))},
        {"updated_entry_ids", updatedEntryIds},
        {"journal_status", artifact.journalStatus}
    };
}

json applyAuditEvent(const ExecutionApplyArtifact& artifact,
                     std::string_view eventType,
                     std::string_view actor,
                     std::string_view comment,
                     const std::vector<std::string>& updatedEntryIds,
                     const std::string& generatedAtUtc) {
    std::ostringstream seed;
    seed << artifact.applyArtifact.artifactId << "|" << eventType << "|" << actor << "|" << generatedAtUtc;
    for (const auto& entryId : updatedEntryIds) {
        seed << "|" << entryId;
    }
    return {
        {"event_id", "phase7-apply-event-" + fnv1aHex(seed.str())},
        {"event_type", std::string(eventType)},
        {"generated_at_utc", generatedAtUtc},
        {"actor", actor.empty() ? json(nullptr) : json(std::string(actor))},
        {"comment", comment.empty() ? json(nullptr) : json(std::string(comment))},
        {"updated_entry_ids", updatedEntryIds},
        {"apply_status", artifact.applyStatus}
    };
}

} // namespace

bool summarizeRuntimeRecoveryBacklog(RuntimeRecoveryBacklogSummary* out,
                                     std::string* errorCode,
                                     std::string* errorMessage) {
    if (out == nullptr) {
        if (errorCode != nullptr) *errorCode = "invalid_arguments";
        if (errorMessage != nullptr) *errorMessage = "missing runtime recovery backlog output container";
        return false;
    }

    std::vector<ExecutionJournalArtifact> journals;
    if (!listExecutionJournals(0, &journals, errorCode, errorMessage)) {
        return false;
    }

    std::vector<ExecutionApplyArtifact> applies;
    if (!listExecutionApplies(0, &applies, errorCode, errorMessage)) {
        return false;
    }

    RuntimeRecoveryBacklogSummary summary;
    summary.journalArtifactCount = journals.size();
    summary.applyArtifactCount = applies.size();

    for (const auto& journal : journals) {
        const auto recovery = summarizeExecutionJournalRecovery(journal);
        summary.runtimeBackedSubmittedJournalEntryCount += recovery.runtimeBackedSubmittedCount;
        summary.staleRuntimeBackedJournalEntryCount += recovery.staleRuntimeBackedCount;
        if (recovery.recoveryRequired) {
            ++summary.recoveryRequiredJournalCount;
        }
        if (recovery.staleRecoveryRequired) {
            ++summary.staleRecoveryRequiredJournalCount;
        }
    }

    for (const auto& apply : applies) {
        const auto recovery = summarizeExecutionApplyRecovery(apply);
        summary.runtimeBackedSubmittedApplyEntryCount += recovery.runtimeBackedSubmittedCount;
        summary.staleRuntimeBackedApplyEntryCount += recovery.staleRuntimeBackedCount;
        if (recovery.recoveryRequired) {
            ++summary.recoveryRequiredApplyCount;
        }
        if (recovery.staleRecoveryRequired) {
            ++summary.staleRecoveryRequiredApplyCount;
        }
    }

    summary.recoveryRequired =
        summary.recoveryRequiredJournalCount > 0 || summary.recoveryRequiredApplyCount > 0;
    summary.staleRecoveryRequired =
        summary.staleRecoveryRequiredJournalCount > 0 || summary.staleRecoveryRequiredApplyCount > 0;

    *out = summary;
    if (errorCode != nullptr) {
        errorCode->clear();
    }
    if (errorMessage != nullptr) {
        errorMessage->clear();
    }
    return true;
}

RuntimeStartupRecoveryPlan runtimeStartupRecoveryPlan(const RuntimeRecoveryBacklogSummary& backlog,
                                                      bool actorPresent) {
    RuntimeStartupRecoveryPlan plan;
    plan.backlog = backlog;
    plan.actorPresent = actorPresent;
    if (!backlog.recoveryRequired) {
        plan.startupAction = "none";
        plan.detail = "No runtime-backed execution journals or controlled-apply artifacts need recovery.";
        return plan;
    }

    plan.manualAttentionRecommended = backlog.staleRecoveryRequired;
    if (actorPresent) {
        plan.startupAction = "recover_pending";
        plan.recoverySweepRecommended = true;
        if (backlog.staleRecoveryRequired) {
            plan.detail =
                "Runtime recovery backlog includes stale entries; a recovery sweep should reconcile them and may escalate missing or non-terminal orders into manual review.";
        } else {
            plan.detail =
                "Runtime recovery backlog is pending; a recovery sweep should reconcile submitted runtime-backed entries now.";
        }
        return plan;
    }

    plan.startupAction = "await_actor";
    if (backlog.staleRecoveryRequired) {
        plan.detail =
            "Runtime recovery backlog includes stale entries, but an execution actor is required before startup recovery can reconcile or escalate them.";
    } else {
        plan.detail =
            "Runtime recovery backlog is pending, but an execution actor is required before startup recovery can run.";
    }
    return plan;
}

bool planRuntimeRecoveryStartup(bool actorPresent,
                                RuntimeStartupRecoveryPlan* out,
                                std::string* errorCode,
                                std::string* errorMessage) {
    if (out == nullptr) {
        if (errorCode != nullptr) *errorCode = "invalid_arguments";
        if (errorMessage != nullptr) *errorMessage = "missing runtime startup recovery output container";
        return false;
    }

    RuntimeRecoveryBacklogSummary backlog;
    if (!summarizeRuntimeRecoveryBacklog(&backlog, errorCode, errorMessage)) {
        return false;
    }

    *out = runtimeStartupRecoveryPlan(backlog, actorPresent);
    if (errorCode != nullptr) {
        errorCode->clear();
    }
    if (errorMessage != nullptr) {
        errorMessage->clear();
    }
    return true;
}

bool dispatchExecutionJournalEntriesViaRuntime(TradingRuntime* runtime,
                                               const std::string& manifestPath,
                                               const std::string& artifactId,
                                               const std::vector<std::string>& entryIds,
                                               const std::string& actor,
                                               const std::string& executionCapability,
                                               const std::string& comment,
                                               ExecutionJournalArtifact* out,
                                               std::vector<std::string>* updatedEntryIds,
                                               std::string* auditEventId,
                                               std::string* errorCode,
                                               std::string* errorMessage) {
    ExecutionJournalArtifact artifact;
    if (!loadExecutionJournalArtifact(manifestPath, artifactId, &artifact, errorCode, errorMessage)) {
        return false;
    }
    annotateExecutionJournalArtifact(&artifact);

    const std::string normalizedActor = trimAscii(actor);
    if (artifact.executionPolicy.value("actor_required", true) && normalizedActor.empty()) {
        if (errorCode != nullptr) *errorCode = "invalid_arguments";
        if (errorMessage != nullptr) *errorMessage = "actor is required by the Phase 7 execution journal policy";
        return false;
    }

    const std::string requiredCapability =
        trimAscii(artifact.executionPolicy.value("capability_required", std::string()));
    const std::string normalizedCapability = trimAscii(executionCapability);
    if (!requiredCapability.empty() && normalizedCapability.empty()) {
        if (errorCode != nullptr) *errorCode = "invalid_arguments";
        if (errorMessage != nullptr) *errorMessage = "execution_capability is required by the Phase 7 execution journal policy";
        return false;
    }
    if (!requiredCapability.empty() && normalizedCapability != requiredCapability) {
        if (errorCode != nullptr) *errorCode = "capability_mismatch";
        if (errorMessage != nullptr) *errorMessage = "execution_capability does not satisfy the Phase 7 execution journal policy";
        return false;
    }

    std::vector<std::string> dispatchIds = entryIds;
    if (dispatchIds.empty()) {
        for (const auto& entry : artifact.entries) {
            if (isRuntimeDispatchable(entry)) {
                dispatchIds.push_back(entry.journalEntryId);
            }
        }
    }
    std::sort(dispatchIds.begin(), dispatchIds.end());
    dispatchIds.erase(std::unique(dispatchIds.begin(), dispatchIds.end()), dispatchIds.end());
    if (dispatchIds.empty()) {
        if (errorCode != nullptr) *errorCode = "nothing_dispatchable";
        if (errorMessage != nullptr) *errorMessage = "execution journal dispatch requires at least one queued runtime-dispatchable entry";
        return false;
    }

    const std::string generatedAtUtc = utcTimestampIso8601Now();
    std::vector<std::string> changed;
    for (auto& entry : artifact.entries) {
        if (std::find(dispatchIds.begin(), dispatchIds.end(), entry.journalEntryId) == dispatchIds.end()) {
            continue;
        }
        if (!isRuntimeDispatchable(entry)) {
            if (errorCode != nullptr) *errorCode = "nothing_dispatchable";
            if (errorMessage != nullptr) *errorMessage = "selected execution journal entry is not runtime-dispatchable";
            return false;
        }

        const auto orderId = firstRequestedOrderId(entry.executionRequest);
        if (!orderId.has_value()) {
            if (errorCode != nullptr) *errorCode = "invalid_arguments";
            if (errorMessage != nullptr) *errorMessage = "execution request is missing a requested order id";
            return false;
        }

        const std::string reason = trimAscii(entry.executionRequest.value("reason", std::string()));
        const auto accepted = requestReconciliation(runtime, *orderId, reason.empty() ? entry.actionType : reason);
        if (std::find(accepted.begin(), accepted.end(), *orderId) == accepted.end()) {
            if (errorCode != nullptr) *errorCode = "runtime_rejected";
            if (errorMessage != nullptr) *errorMessage = "runtime did not accept the requested order reconciliation";
            return false;
        }

        entry.executionStatus = kExecutionEntryStatusSubmitted;
        entry.startedAtUtc = generatedAtUtc;
        entry.completedAtUtc.clear();
        entry.lastUpdatedAtUtc = generatedAtUtc;
        entry.lastUpdatedBy = normalizedActor;
        entry.executionComment = trimAscii(comment).empty()
            ? "Dispatched through TradingRuntime order reconciliation."
            : trimAscii(comment);
        entry.failureCode.clear();
        entry.failureMessage.clear();
        entry.attemptCount += 1;
        const RuntimePresentationSnapshot snapshot = captureSnapshot(runtime);
        const OrderInfo* order = findOrderInfo(snapshot, *orderId);
        const TradeTraceSnapshot traceSnapshot = captureTraceSnapshot(runtime, lookupTraceId(runtime, *orderId));
        entry.executionResult = runtimeReconciliationResult(order,
                                                            *orderId,
                                                            traceSnapshot,
                                                            entry.executionRequest,
                                                            runtimeResolutionForOrderState(order, "dispatch_accepted"),
                                                            false);
        changed.push_back(entry.journalEntryId);
    }

    annotateExecutionJournalArtifact(&artifact);
    json audit = journalAuditEvent(artifact,
                                   "execution_runtime_dispatched",
                                   normalizedActor,
                                   trimAscii(comment),
                                   changed,
                                   generatedAtUtc);
    audit["message"] = "Dispatched execution journal entries through TradingRuntime reconciliation.";
    artifact.auditTrail.push_back(std::move(audit));
    if (!persistExecutionJournalArtifact(artifact, errorCode, errorMessage)) {
        return false;
    }
    if (!loadExecutionJournalArtifact(artifact.journalArtifact.manifestPath, {}, &artifact, errorCode, errorMessage)) {
        return false;
    }

    if (updatedEntryIds != nullptr) *updatedEntryIds = changed;
    if (auditEventId != nullptr) *auditEventId = artifact.auditTrail.back().value("event_id", std::string());
    if (out != nullptr) *out = std::move(artifact);
    return true;
}

bool reconcileExecutionJournalEntriesViaRuntime(TradingRuntime* runtime,
                                                const std::string& manifestPath,
                                                const std::string& artifactId,
                                                const std::vector<std::string>& entryIds,
                                                const std::string& actor,
                                                const std::string& comment,
                                                ExecutionJournalArtifact* out,
                                                std::vector<std::string>* updatedEntryIds,
                                                std::string* auditEventId,
                                                std::string* errorCode,
                                                std::string* errorMessage) {
    ExecutionJournalArtifact artifact;
    if (!loadExecutionJournalArtifact(manifestPath, artifactId, &artifact, errorCode, errorMessage)) {
        return false;
    }
    annotateExecutionJournalArtifact(&artifact);

    const std::string normalizedActor = trimAscii(actor);
    if (artifact.executionPolicy.value("actor_required", true) && normalizedActor.empty()) {
        if (errorCode != nullptr) *errorCode = "invalid_arguments";
        if (errorMessage != nullptr) *errorMessage = "actor is required by the Phase 7 execution journal policy";
        return false;
    }

    std::vector<std::string> reconcileIds = entryIds;
    if (reconcileIds.empty()) {
        for (const auto& entry : artifact.entries) {
            if (entry.executionStatus == kExecutionEntryStatusSubmitted &&
                trimAscii(entry.executionRequest.value("operation", std::string())) == "request_order_reconciliation") {
                reconcileIds.push_back(entry.journalEntryId);
            }
        }
    }
    std::sort(reconcileIds.begin(), reconcileIds.end());
    reconcileIds.erase(std::unique(reconcileIds.begin(), reconcileIds.end()), reconcileIds.end());
    if (reconcileIds.empty()) {
        if (errorCode != nullptr) *errorCode = "nothing_reconcilable";
        if (errorMessage != nullptr) *errorMessage = "execution journal reconcile requires at least one submitted runtime entry";
        return false;
    }

    const RuntimePresentationSnapshot snapshot = captureSnapshot(runtime);
    const auto now = std::chrono::system_clock::now();
    const std::string generatedAtUtc = utcTimestampIso8601Now();
    std::vector<std::string> changed;
    for (auto& entry : artifact.entries) {
        if (std::find(reconcileIds.begin(), reconcileIds.end(), entry.journalEntryId) == reconcileIds.end()) {
            continue;
        }
        const auto orderId = firstRequestedOrderId(entry.executionResult.is_object() && !entry.executionResult.empty()
                                                       ? entry.executionResult
                                                       : entry.executionRequest);
        if (!orderId.has_value()) {
            continue;
        }
        const json runtimeContext = entry.executionResult.is_object() && !entry.executionResult.empty()
            ? entry.executionResult
            : entry.executionRequest;
        const OrderInfo* order = findOrderInfo(snapshot, *orderId);
        const TradeTraceSnapshot traceSnapshot = captureTraceSnapshot(runtime, lookupTraceId(runtime, *orderId));
        const bool stale = isStaleRuntimeBridgeEntry(entry, now);
        if (order == nullptr) {
            if (!stale) {
                continue;
            }
            entry.lastUpdatedAtUtc = generatedAtUtc;
            entry.lastUpdatedBy = normalizedActor;
            entry.executionComment = trimAscii(comment).empty()
                ? "Escalated to manual review after runtime reconciliation lost the order."
                : trimAscii(comment);
            entry.executionResult = runtimeReconciliationResult(nullptr,
                                                                *orderId,
                                                                traceSnapshot,
                                                                runtimeContext,
                                                                "manual_review_required",
                                                                true);
            entry.executionStatus = kExecutionEntryStatusFailed;
            entry.completedAtUtc = generatedAtUtc;
            entry.failureCode = "runtime_order_missing";
            entry.failureMessage =
                "Runtime reconciliation could not find the requested order after the recovery grace period.";
            entry.terminal = true;
            changed.push_back(entry.journalEntryId);
            continue;
        }
        if (isStillReconcilingOrderState(*order)) {
            const json refreshedResult = runtimeReconciliationResult(order,
                                                                     *orderId,
                                                                     traceSnapshot,
                                                                     runtimeContext,
                                                                     runtimeResolutionForOrderState(order, "reconciling"),
                                                                     false);
            if (!stale) {
                if (entry.executionStatus == kExecutionEntryStatusSubmitted &&
                    !entry.terminal &&
                    entry.failureCode.empty() &&
                    entry.failureMessage.empty() &&
                    entry.executionResult == refreshedResult) {
                    continue;
                }
                entry.lastUpdatedAtUtc = generatedAtUtc;
                entry.lastUpdatedBy = normalizedActor;
                entry.executionComment = trimAscii(comment).empty()
                    ? "Refreshed the runtime-backed order state while reconciliation remains in flight."
                    : trimAscii(comment);
                entry.executionResult = refreshedResult;
                entry.executionStatus = kExecutionEntryStatusSubmitted;
                entry.completedAtUtc.clear();
                entry.failureCode.clear();
                entry.failureMessage.clear();
                entry.terminal = false;
                changed.push_back(entry.journalEntryId);
                continue;
            }
            const bool cancelPending = isCancelPendingOrderState(*order);
            entry.lastUpdatedAtUtc = generatedAtUtc;
            entry.lastUpdatedBy = normalizedActor;
            entry.executionComment = trimAscii(comment).empty()
                ? (cancelPending
                       ? "Escalated to manual review after runtime cancel acknowledgement stayed pending too long."
                       : "Escalated to manual review after runtime reconciliation stayed non-terminal too long.")
                : trimAscii(comment);
            entry.executionResult = runtimeReconciliationResult(order,
                                                                *orderId,
                                                                traceSnapshot,
                                                                runtimeContext,
                                                                "manual_review_required",
                                                                true);
            entry.executionStatus = kExecutionEntryStatusFailed;
            entry.completedAtUtc = generatedAtUtc;
            entry.failureCode = cancelPending ? "runtime_cancel_ack_stale" : "runtime_reconciliation_stale";
            entry.failureMessage =
                cancelPending
                    ? "Runtime reconciliation did not receive a cancellation acknowledgement before the recovery timeout."
                    : "Runtime reconciliation did not reach a terminal order state before the recovery timeout.";
            entry.terminal = true;
            changed.push_back(entry.journalEntryId);
            continue;
        }

        entry.lastUpdatedAtUtc = generatedAtUtc;
        entry.lastUpdatedBy = normalizedActor;
        entry.executionComment = trimAscii(comment).empty()
            ? "Reconciled from TradingRuntime order state."
            : trimAscii(comment);
        const bool terminal = isTerminalOrderState(*order);
        entry.executionResult = runtimeReconciliationResult(order,
                                                            *orderId,
                                                            traceSnapshot,
                                                            runtimeContext,
                                                            runtimeResolutionForOrderState(order, "resolved"),
                                                            terminal);
        entry.executionStatus = executionStatusForResolvedOrder(*order);
        if (terminal) {
            entry.completedAtUtc = generatedAtUtc;
            entry.failureCode = failureCodeForResolvedOrder(*order);
            entry.failureMessage = failureMessageForResolvedOrder(*order, traceSnapshot);
            entry.terminal = true;
        } else {
            entry.completedAtUtc.clear();
            entry.failureCode.clear();
            entry.failureMessage.clear();
            entry.terminal = false;
        }
        changed.push_back(entry.journalEntryId);
    }

    if (changed.empty()) {
        if (errorCode != nullptr) *errorCode = "nothing_reconciled";
        if (errorMessage != nullptr) *errorMessage = "no submitted execution journal entries resolved from runtime state";
        return false;
    }

    annotateExecutionJournalArtifact(&artifact);
    json audit = journalAuditEvent(artifact,
                                   "execution_runtime_reconciled",
                                   normalizedActor,
                                   trimAscii(comment),
                                   changed,
                                   generatedAtUtc);
    audit["message"] = "Reconciled execution journal entries from TradingRuntime order state.";
    artifact.auditTrail.push_back(std::move(audit));
    if (!persistExecutionJournalArtifact(artifact, errorCode, errorMessage)) {
        return false;
    }
    if (!loadExecutionJournalArtifact(artifact.journalArtifact.manifestPath, {}, &artifact, errorCode, errorMessage)) {
        return false;
    }

    if (updatedEntryIds != nullptr) *updatedEntryIds = changed;
    if (auditEventId != nullptr) *auditEventId = artifact.auditTrail.back().value("event_id", std::string());
    if (out != nullptr) *out = std::move(artifact);
    return true;
}

bool synchronizeExecutionApplyFromJournal(const std::string& manifestPath,
                                          const std::string& artifactId,
                                          const std::string& actor,
                                          const std::string& comment,
                                          ExecutionApplyArtifact* out,
                                          std::vector<std::string>* updatedEntryIds,
                                          std::string* auditEventId,
                                          std::string* errorCode,
                                          std::string* errorMessage) {
    ExecutionApplyArtifact artifact;
    if (!loadExecutionApplyArtifactStored(manifestPath, artifactId, &artifact, errorCode, errorMessage)) {
        return false;
    }

    ExecutionJournalArtifact journal;
    if (!loadExecutionJournalArtifact({},
                                      artifact.journalArtifact.artifactId,
                                      &journal,
                                      errorCode,
                                      errorMessage)) {
        return false;
    }

    std::vector<ExecutionApplyEntry> previous = artifact.entries;
    synchronizeExecutionApplyArtifactFromJournal(&artifact, journal);
    annotateExecutionApplyArtifact(&artifact);

    std::vector<std::string> changed;
    for (std::size_t i = 0; i < artifact.entries.size() && i < previous.size(); ++i) {
        if (artifact.entries[i].executionStatus != previous[i].executionStatus ||
            artifact.entries[i].executionResult != previous[i].executionResult ||
            artifact.entries[i].lastUpdatedAtUtc != previous[i].lastUpdatedAtUtc) {
            changed.push_back(artifact.entries[i].applyEntryId);
        }
    }
    if (changed.empty()) {
        if (updatedEntryIds != nullptr) updatedEntryIds->clear();
        if (auditEventId != nullptr) auditEventId->clear();
        if (errorCode != nullptr) errorCode->clear();
        if (errorMessage != nullptr) errorMessage->clear();
        if (out != nullptr) *out = std::move(artifact);
        return true;
    }

    const std::string generatedAtUtc = utcTimestampIso8601Now();
    json audit = applyAuditEvent(artifact,
                                 "execution_apply_synchronized",
                                 trimAscii(actor),
                                 trimAscii(comment),
                                 changed,
                                 generatedAtUtc);
    audit["message"] = "Synchronized controlled-apply entries from the linked execution journal.";
    artifact.auditTrail.push_back(std::move(audit));
    if (!persistExecutionApplyArtifact(artifact, errorCode, errorMessage)) {
        return false;
    }
    if (!loadExecutionApplyArtifact(artifact.applyArtifact.manifestPath, {}, &artifact, errorCode, errorMessage)) {
        return false;
    }

    if (updatedEntryIds != nullptr) *updatedEntryIds = changed;
    if (auditEventId != nullptr) *auditEventId = artifact.auditTrail.back().value("event_id", std::string());
    if (out != nullptr) *out = std::move(artifact);
    return true;
}

bool reconcileExecutionArtifactsViaRuntime(TradingRuntime* runtime,
                                           const std::string& actor,
                                           const std::string& comment,
                                           RuntimeBridgeSweepResult* out,
                                           std::string* errorCode,
                                           std::string* errorMessage) {
    std::vector<ExecutionJournalArtifact> journals;
    if (!listExecutionJournals(0, &journals, errorCode, errorMessage)) {
        return false;
    }

    std::vector<ExecutionApplyArtifact> applies;
    if (!listExecutionApplies(0, &applies, errorCode, errorMessage)) {
        return false;
    }

    RuntimeBridgeSweepResult summary;
    summary.scannedJournalCount = journals.size();

    for (const auto& journal : journals) {
        std::vector<std::string> submittedEntryIds;
        for (const auto& entry : journal.entries) {
            if (entry.executionStatus == kExecutionEntryStatusSubmitted &&
                trimAscii(entry.executionRequest.value("operation", std::string())) == "request_order_reconciliation") {
                submittedEntryIds.push_back(entry.journalEntryId);
            }
        }
        if (submittedEntryIds.empty()) {
            continue;
        }

        ExecutionJournalArtifact reconciledJournal;
        std::vector<std::string> updatedJournalEntryIds;
        std::string journalAuditEventId;
        std::string localErrorCode;
        std::string localErrorMessage;
        if (!reconcileExecutionJournalEntriesViaRuntime(runtime,
                                                        {},
                                                        journal.journalArtifact.artifactId,
                                                        submittedEntryIds,
                                                        actor,
                                                        comment,
                                                        &reconciledJournal,
                                                        &updatedJournalEntryIds,
                                                        &journalAuditEventId,
                                                        &localErrorCode,
                                                        &localErrorMessage)) {
            if (localErrorCode == "nothing_reconciled") {
                continue;
            }
            if (errorCode != nullptr) *errorCode = std::move(localErrorCode);
            if (errorMessage != nullptr) *errorMessage = std::move(localErrorMessage);
            return false;
        }

        summary.updatedJournalCount += 1;
        summary.updatedJournalArtifactIds.push_back(reconciledJournal.journalArtifact.artifactId);
        summary.updatedJournalEntryIds.insert(summary.updatedJournalEntryIds.end(),
                                              updatedJournalEntryIds.begin(),
                                              updatedJournalEntryIds.end());

        for (const auto& apply : applies) {
            if (apply.journalArtifact.artifactId != reconciledJournal.journalArtifact.artifactId) {
                continue;
            }
            ExecutionApplyArtifact synchronizedApply;
            std::vector<std::string> updatedApplyEntryIds;
            std::string applyAuditEventId;
            localErrorCode.clear();
            localErrorMessage.clear();
            if (!synchronizeExecutionApplyFromJournal({},
                                                      apply.applyArtifact.artifactId,
                                                      actor,
                                                      comment,
                                                      &synchronizedApply,
                                                      &updatedApplyEntryIds,
                                                      &applyAuditEventId,
                                                      &localErrorCode,
                                                      &localErrorMessage)) {
                if (errorCode != nullptr) *errorCode = std::move(localErrorCode);
                if (errorMessage != nullptr) *errorMessage = std::move(localErrorMessage);
                return false;
            }
            if (!updatedApplyEntryIds.empty()) {
                summary.updatedApplyCount += 1;
                summary.updatedApplyArtifactIds.push_back(synchronizedApply.applyArtifact.artifactId);
                summary.updatedApplyEntryIds.insert(summary.updatedApplyEntryIds.end(),
                                                    updatedApplyEntryIds.begin(),
                                                    updatedApplyEntryIds.end());
            }
        }
    }

    std::sort(summary.updatedJournalArtifactIds.begin(), summary.updatedJournalArtifactIds.end());
    summary.updatedJournalArtifactIds.erase(
        std::unique(summary.updatedJournalArtifactIds.begin(), summary.updatedJournalArtifactIds.end()),
        summary.updatedJournalArtifactIds.end());
    std::sort(summary.updatedApplyArtifactIds.begin(), summary.updatedApplyArtifactIds.end());
    summary.updatedApplyArtifactIds.erase(
        std::unique(summary.updatedApplyArtifactIds.begin(), summary.updatedApplyArtifactIds.end()),
        summary.updatedApplyArtifactIds.end());
    std::sort(summary.updatedJournalEntryIds.begin(), summary.updatedJournalEntryIds.end());
    summary.updatedJournalEntryIds.erase(
        std::unique(summary.updatedJournalEntryIds.begin(), summary.updatedJournalEntryIds.end()),
        summary.updatedJournalEntryIds.end());
    std::sort(summary.updatedApplyEntryIds.begin(), summary.updatedApplyEntryIds.end());
    summary.updatedApplyEntryIds.erase(
        std::unique(summary.updatedApplyEntryIds.begin(), summary.updatedApplyEntryIds.end()),
        summary.updatedApplyEntryIds.end());

    if (out != nullptr) {
        *out = std::move(summary);
    }
    if (errorCode != nullptr) {
        errorCode->clear();
    }
    if (errorMessage != nullptr) {
        errorMessage->clear();
    }
    return true;
}

} // namespace tape_phase7
