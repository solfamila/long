#import "tapescope_window_internal.h"

#include "tapescope_support.h"

namespace {

using namespace tapescope_support;

} // namespace

@implementation TapeScopeWindowController (OrderQueries)

- (BOOL)buildOrderAnchorQueryFromPopup:(NSPopUpButton*)popup
                            inputField:(NSTextField*)inputField
                              outQuery:(tapescope::OrderAnchorQuery*)outQuery
                            descriptor:(std::string*)descriptor
                                 error:(std::string*)error {
    if (outQuery == nullptr || descriptor == nullptr || error == nullptr) {
        return NO;
    }

    const OrderAnchorType type = OrderAnchorTypeFromIndex(popup.indexOfSelectedItem);
    const std::string rawValue = TrimAscii(ToStdString(inputField.stringValue));
    if (rawValue.empty()) {
        *error = "An anchor value is required.";
        return NO;
    }

    *outQuery = tapescope::OrderAnchorQuery{};
    *descriptor = std::string();
    switch (type) {
        case OrderAnchorType::TraceId: {
            std::uint64_t parsed = 0;
            if (!ParsePositiveUInt64(rawValue, &parsed)) {
                *error = "traceId must be a positive integer.";
                return NO;
            }
            outQuery->traceId = parsed;
            *descriptor = "traceId=" + std::to_string(parsed);
            return YES;
        }
        case OrderAnchorType::OrderId: {
            long long parsed = 0;
            if (!ParsePositiveInt64(rawValue, &parsed)) {
                *error = "orderId must be a positive integer.";
                return NO;
            }
            outQuery->orderId = parsed;
            *descriptor = "orderId=" + std::to_string(parsed);
            return YES;
        }
        case OrderAnchorType::PermId: {
            long long parsed = 0;
            if (!ParsePositiveInt64(rawValue, &parsed)) {
                *error = "permId must be a positive integer.";
                return NO;
            }
            outQuery->permId = parsed;
            *descriptor = "permId=" + std::to_string(parsed);
            return YES;
        }
        case OrderAnchorType::ExecId:
            outQuery->execId = rawValue;
            *descriptor = "execId=" + rawValue;
            return YES;
    }
    *error = "Unsupported anchor selector.";
    return NO;
}

- (void)orderAnchorTypeChanged:(id)sender {
    (void)sender;
    _orderAnchorInputField.placeholderString =
        PlaceholderForOrderAnchorType(OrderAnchorTypeFromIndex(_orderAnchorTypePopup.indexOfSelectedItem));
}

- (void)performOrderLookup:(id)sender {
    (void)sender;
    if (_orderLookupInFlight || !_client) {
        return;
    }

    tapescope::OrderAnchorQuery query;
    std::string descriptor;
    std::string errorMessage;
    if (![self buildOrderAnchorQueryFromPopup:_orderAnchorTypePopup
                                   inputField:_orderAnchorInputField
                                    outQuery:&query
                                  descriptor:&descriptor
                                       error:&errorMessage]) {
        _orderStateLabel.stringValue = ToNSString(errorMessage);
        _orderStateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    _orderLookupInFlight = YES;
    const std::uint64_t token = [self issueRequestToken:&_orderLookupRequestToken];
    _orderLookupButton.enabled = NO;
    _orderStateLabel.stringValue = @"Looking up order anchor…";
    _orderStateLabel.textColor = [NSColor systemOrangeColor];
    _orderTextView.string = @"Looking up anchor-linked events and replay context…";

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_interactiveQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->findOrderAnchorPayload(query);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil || ![innerSelf isRequestTokenCurrent:token storage:&innerSelf->_orderLookupRequestToken]) {
                return;
            }
            innerSelf->_orderLookupInFlight = NO;
            innerSelf->_orderLookupButton.enabled = YES;
            if (result.ok()) {
                innerSelf->_orderEvents = result.value.events;
                [innerSelf->_orderTableView reloadData];
                if (!innerSelf->_orderEvents.empty()) {
                    innerSelf->_orderStateLabel.stringValue = @"Order lookup complete.";
                    innerSelf->_orderStateLabel.textColor = [NSColor systemGreenColor];
                    [innerSelf->_orderTableView selectRowIndexes:[NSIndexSet indexSetWithIndex:0] byExtendingSelection:NO];
                } else {
                    innerSelf->_orderStateLabel.stringValue = @"No anchor-linked events matched that selector.";
                    innerSelf->_orderStateLabel.textColor = TapeInkMutedColor();
                    innerSelf->_orderTextView.string = ToNSString(DescribeOrderLookupResult(descriptor, result));
                }
            } else {
                innerSelf->_orderStateLabel.stringValue = ToNSString(tapescope::QueryClient::describeError(result.error));
                innerSelf->_orderStateLabel.textColor = ErrorColorForKind(result.error.kind);
                innerSelf->_orderEvents.clear();
                [innerSelf->_orderTableView reloadData];
                innerSelf->_orderTextView.string = ToNSString(DescribeOrderLookupResult(descriptor, result));
            }
        });
    });
}

- (void)orderCaseAnchorTypeChanged:(id)sender {
    (void)sender;
    _orderCaseAnchorInputField.placeholderString =
        PlaceholderForOrderAnchorType(OrderAnchorTypeFromIndex(_orderCaseAnchorTypePopup.indexOfSelectedItem));
}

- (void)seekAnchorTypeChanged:(id)sender {
    (void)sender;
    _seekAnchorInputField.placeholderString =
        PlaceholderForOrderAnchorType(OrderAnchorTypeFromIndex(_seekAnchorTypePopup.indexOfSelectedItem));
}

- (void)fetchOrderCase:(id)sender {
    (void)sender;
    if (_orderCaseInFlight || !_client) {
        return;
    }

    tapescope::OrderAnchorQuery query;
    std::string descriptor;
    std::string errorMessage;
    if (![self buildOrderAnchorQueryFromPopup:_orderCaseAnchorTypePopup
                                   inputField:_orderCaseAnchorInputField
                                    outQuery:&query
                                  descriptor:&descriptor
                                       error:&errorMessage]) {
        _orderCaseStateLabel.stringValue = ToNSString(errorMessage);
        _orderCaseStateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    _orderCaseInFlight = YES;
    _orderCaseFetchButton.enabled = NO;
    _orderCaseScanButton.enabled = NO;
    const std::uint64_t token = _orderCasePane->beginRequest(@"Reading order case…");

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_interactiveQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->readOrderCasePayload(query);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil || !innerSelf->_orderCasePane->isCurrent(token)) {
                return;
            }
            innerSelf->_orderCaseInFlight = NO;
            innerSelf->_orderCaseFetchButton.enabled = YES;
            innerSelf->_orderCaseScanButton.enabled = YES;
            [innerSelf applyInvestigationResult:result
                                 paneController:innerSelf->_orderCasePane.get()
                                    successText:@"Order case loaded."
                               syncArtifactField:YES];
            if (result.ok()) {
                [innerSelf recordRecentHistoryForKind:"order_case"
                                             targetId:descriptor
                                              payload:result.value
                                        fallbackTitle:"Order case " + descriptor
                                       fallbackDetail:"Reopen the recent order-case investigation."
                                             metadata:tapescope::json{{"anchor_kind",
                                                                       OrderAnchorTypeKey(OrderAnchorTypeFromIndex(innerSelf->_orderCaseAnchorTypePopup.indexOfSelectedItem))},
                                                                      {"anchor_value",
                                                                       ToStdString(innerSelf->_orderCaseAnchorInputField.stringValue)}}];
            }
            innerSelf->_orderCaseTextView.string =
                ToNSString(DescribeInvestigationPayload("order_case", descriptor, result));
        });
    });
}

- (void)scanOrderCaseReport:(id)sender {
    (void)sender;
    if (_orderCaseInFlight || !_client) {
        return;
    }

    tapescope::OrderAnchorQuery query;
    std::string descriptor;
    std::string errorMessage;
    if (![self buildOrderAnchorQueryFromPopup:_orderCaseAnchorTypePopup
                                   inputField:_orderCaseAnchorInputField
                                    outQuery:&query
                                  descriptor:&descriptor
                                       error:&errorMessage]) {
        _orderCaseStateLabel.stringValue = ToNSString(errorMessage);
        _orderCaseStateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    _orderCaseInFlight = YES;
    _orderCaseFetchButton.enabled = NO;
    _orderCaseScanButton.enabled = NO;
    const std::uint64_t token = _orderCasePane->beginRequest(@"Scanning durable order-case report…");

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_artifactQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->scanOrderCaseReportPayload(query);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil || !innerSelf->_orderCasePane->isCurrent(token)) {
                return;
            }
            innerSelf->_orderCaseInFlight = NO;
            innerSelf->_orderCaseFetchButton.enabled = YES;
            innerSelf->_orderCaseScanButton.enabled = YES;
            [innerSelf applyInvestigationResult:result
                                 paneController:innerSelf->_orderCasePane.get()
                                    successText:@"Durable order-case report scanned."
                               syncArtifactField:YES];
            if (result.ok()) {
                if (!result.value.artifactId.empty()) {
                    [innerSelf recordRecentHistoryForKind:"artifact"
                                                 targetId:result.value.artifactId
                                                  payload:result.value
                                            fallbackTitle:"Order-case report " + result.value.artifactId
                                           fallbackDetail:"Reopen the durable scanned order-case report."
                                                 metadata:tapescope::json{{"artifact_id", result.value.artifactId}}];
                }
                [innerSelf refreshReportInventory:nil];
            }
            innerSelf->_orderCaseTextView.string =
                ToNSString(DescribeInvestigationPayload("order_case_report_scan", descriptor, result));
        });
    });
}

- (void)fetchSeekOrder:(id)sender {
    (void)sender;
    if (_seekInFlight || !_client) {
        return;
    }

    tapescope::OrderAnchorQuery query;
    std::string descriptor;
    std::string errorMessage;
    if (![self buildOrderAnchorQueryFromPopup:_seekAnchorTypePopup
                                   inputField:_seekAnchorInputField
                                    outQuery:&query
                                  descriptor:&descriptor
                                       error:&errorMessage]) {
        _seekStateLabel.stringValue = ToNSString(errorMessage);
        _seekStateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    _seekInFlight = YES;
    const std::uint64_t token = [self issueRequestToken:&_seekRequestToken];
    _seekFetchButton.enabled = NO;
    _seekLoadRangeButton.enabled = NO;
    _hasSeekReplayRange = NO;
    _seekStateLabel.stringValue = @"Computing replay target…";
    _seekStateLabel.textColor = [NSColor systemOrangeColor];
    _seekTextView.string = @"Computing replay target and protected-window context…";

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_interactiveQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->seekOrderAnchorPayload(query);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil || ![innerSelf isRequestTokenCurrent:token storage:&innerSelf->_seekRequestToken]) {
                return;
            }
            innerSelf->_seekInFlight = NO;
            innerSelf->_seekFetchButton.enabled = YES;
            if (result.ok()) {
                innerSelf->_seekStateLabel.stringValue = @"Replay target ready.";
                innerSelf->_seekStateLabel.textColor = [NSColor systemGreenColor];
                innerSelf->_hasSeekReplayRange = result.value.replayRange.has_value();
                if (innerSelf->_hasSeekReplayRange) {
                    innerSelf->_seekReplayRange = *result.value.replayRange;
                }
                innerSelf->_seekLoadRangeButton.enabled = innerSelf->_hasSeekReplayRange;
            } else {
                innerSelf->_seekStateLabel.stringValue = ToNSString(tapescope::QueryClient::describeError(result.error));
                innerSelf->_seekStateLabel.textColor = ErrorColorForKind(result.error.kind);
                innerSelf->_seekLoadRangeButton.enabled = NO;
            }
            innerSelf->_seekTextView.string = ToNSString(DescribeSeekOrderResult(descriptor, result));
        });
    });
}

- (void)loadReplayWindowFromSeek:(id)sender {
    (void)sender;
    if (!_hasSeekReplayRange) {
        _seekStateLabel.stringValue = @"No replay target window is ready yet.";
        _seekStateLabel.textColor = [NSColor systemRedColor];
        return;
    }
    _rangeFirstField.stringValue = UInt64String(_seekReplayRange.firstSessionSeq);
    _rangeLastField.stringValue = UInt64String(_seekReplayRange.lastSessionSeq);
    if (_tabView != nil) {
        [_tabView selectTabViewItemWithIdentifier:@"RangePane"];
    }
    [self fetchRange:nil];
}

- (void)loadReplayWindowFromOrderCase:(id)sender {
    (void)sender;
    [self loadReplayWindowForPane:_orderCasePane.get()];
}

- (void)openSelectedOrderCaseEvidence:(id)sender {
    (void)sender;
    [self openSelectedEvidenceForPane:_orderCasePane.get()];
}

@end
