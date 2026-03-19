#import "tapescope_window_internal.h"

#include "tapescope_support.h"

#include <cstdlib>

namespace {

using namespace tapescope_support;
using tapescope::json;

NSColor* SimplePrimaryButtonColor() {
    return [NSColor colorWithCalibratedRed:0.133 green:0.369 blue:0.698 alpha:1.0];
}

std::string defaultReviewQuestion(const std::string& rawQuestion) {
    const std::string trimmed = TrimAscii(rawQuestion);
    if (!trimmed.empty()) {
        return trimmed;
    }
    return "What happened around this trade, what mattered most in the book, and what did UW add?";
}

bool HasCompletedGeminiInterpretation(const tapescope::EnrichmentPayload& payload) {
    if (!payload.interpretation.is_object()) {
        return false;
    }
    return payload.interpretation.value("status", std::string()) == "completed" &&
           payload.interpretation.value("content", json(nullptr)).is_object();
}

std::string reviewAutorunQuestion() {
    const char* value = std::getenv("LONG_TAPESCOPE_AUTORUN_QUESTION");
    if (value == nullptr || value[0] == '\0') {
        return {};
    }
    return TrimAscii(std::string(value));
}

void ConfigureSimpleReviewBusySpinner(NSProgressIndicator* spinner) {
    if (spinner == nil) {
        return;
    }
    spinner.translatesAutoresizingMaskIntoConstraints = NO;
    spinner.style = NSProgressIndicatorSpinningStyle;
    spinner.controlSize = NSControlSizeSmall;
    spinner.indeterminate = YES;
    spinner.displayedWhenStopped = NO;
    spinner.hidden = YES;
}

std::optional<std::pair<OrderAnchorType, std::string>> PreferredAnchorFromRecentTradeRow(const json& row) {
    const json anchor = row.value("anchor", json::object());
    const std::uint64_t traceId = anchor.value("trace_id", 0ULL);
    if (traceId > 0) {
        return std::make_pair(OrderAnchorType::TraceId, std::to_string(traceId));
    }
    const long long orderId = anchor.value("order_id", 0LL);
    if (orderId > 0) {
        return std::make_pair(OrderAnchorType::OrderId, std::to_string(orderId));
    }
    const long long permId = anchor.value("perm_id", 0LL);
    if (permId > 0) {
        return std::make_pair(OrderAnchorType::PermId, std::to_string(permId));
    }
    const std::string execId = anchor.value("exec_id", std::string());
    if (!execId.empty()) {
        return std::make_pair(OrderAnchorType::ExecId, execId);
    }
    return std::nullopt;
}

std::string RecentTradeTitle(const json& row) {
    const auto preferred = PreferredAnchorFromRecentTradeRow(row);
    if (!preferred.has_value()) {
        const std::string artifactId = row.value("artifact_id", std::string());
        return artifactId.empty() ? "recent trade" : artifactId;
    }
    const std::string prefix = OrderAnchorTypeKey(preferred->first);
    return prefix + " " + preferred->second;
}

std::string RecentTradeSummary(const json& row) {
    std::string summary = RecentTradeTitle(row);
    const std::string eventKind = row.value("event_kind", std::string());
    if (!eventKind.empty()) {
        summary += "  •  ";
        summary += eventKind;
    }
    const std::uint64_t sessionSeq = row.value("session_seq", 0ULL);
    if (sessionSeq > 0) {
        summary += "  •  seq ";
        summary += std::to_string(sessionSeq);
    }
    const std::string note = TrimAscii(row.value("note", std::string()));
    if (!note.empty()) {
        summary += "  •  ";
        summary += note;
    }
    return summary;
}

std::string BuildSimpleReviewDetailText(const std::string& context,
                                        const std::string& replay,
                                        const std::string& answer) {
    auto clippedSection = [](const std::string& raw, std::size_t maxLines) {
        std::istringstream input(raw);
        std::ostringstream output;
        std::string line;
        std::size_t emitted = 0;
        while (std::getline(input, line)) {
            const std::string trimmed = TrimAscii(line);
            if (trimmed.empty()) {
                continue;
            }
            output << trimmed << "\n";
            ++emitted;
            if (emitted >= maxLines) {
                break;
            }
        }
        if (emitted == 0) {
            output << "No details yet.\n";
        } else if (input.good()) {
            output << "...\n";
        }
        return output.str();
    };

    std::ostringstream out;
    out << "Trade Context\n";
    out << "-------------\n";
    out << clippedSection(context.empty() ? "No trade context loaded yet." : context, 5) << "\n";
    out << "Replay + Level 2\n";
    out << "----------------\n";
    out << clippedSection(replay.empty() ? "No replay snapshot loaded yet." : replay, 6) << "\n";
    out << "UW + Gemini Answer\n";
    out << "------------------\n";
    out << clippedSection(answer.empty() ? "No AI answer yet." : answer, 8);
    return out.str();
}

std::vector<json> FallbackRecentTradeRowsFromHistory(const std::vector<json>& historyItems) {
    std::vector<json> rows;
    rows.reserve(historyItems.size());
    for (const auto& item : historyItems) {
        if (!item.is_object() || item.value("kind", std::string()) != "trade_review") {
            continue;
        }
        const std::string anchorKind = item.value("anchor_kind", std::string());
        const std::string anchorValue = item.value("anchor_value", std::string());
        if (anchorKind.empty() || anchorValue.empty()) {
            continue;
        }

        json row = json::object();
        json anchor = json::object();
        if (anchorKind == "traceId") {
            std::uint64_t traceId = 0;
            if (ParsePositiveUInt64(anchorValue, &traceId) && traceId > 0) {
                anchor["trace_id"] = traceId;
            }
        } else if (anchorKind == "orderId") {
            long long orderId = 0;
            if (ParsePositiveInt64(anchorValue, &orderId) && orderId > 0) {
                anchor["order_id"] = orderId;
            }
        } else if (anchorKind == "permId") {
            long long permId = 0;
            if (ParsePositiveInt64(anchorValue, &permId) && permId > 0) {
                anchor["perm_id"] = permId;
            }
        } else if (anchorKind == "execId") {
            anchor["exec_id"] = anchorValue;
        }
        if (anchor.empty()) {
            continue;
        }
        row["anchor"] = std::move(anchor);
        row["event_kind"] = item.value("kind", std::string());
        row["session_seq"] = item.value("first_session_seq", 0ULL);
        row["note"] = item.value("headline", item.value("detail", std::string()));
        row["artifact_id"] = item.value("artifact_id", std::string());
        rows.push_back(std::move(row));
    }
    return rows;
}

void SyncSimpleReviewArtifactField(NSTextField* artifactField,
                                   const tapescope::InvestigationPayload& payload) {
    if (artifactField == nil) {
        return;
    }
    if (!payload.artifactId.empty()) {
        artifactField.stringValue = ToNSString(payload.artifactId);
    }
}

void SyncSimpleReviewArtifactField(NSTextField* artifactField,
                                   const tapescope::EnrichmentPayload& payload) {
    if (artifactField == nil) {
        return;
    }
    const std::string artifactId = !payload.artifactId.empty() ? payload.artifactId : payload.localEvidence.artifactId;
    if (!artifactId.empty()) {
        artifactField.stringValue = ToNSString(artifactId);
    }
}

} // namespace

@implementation TapeScopeWindowController (SimpleReview)

- (void)beginSimpleReviewBusyWithMessage:(NSString*)message color:(NSColor*)color {
    _simpleReviewInFlight = YES;
    _simpleReviewLoadButton.enabled = NO;
    _simpleReviewAskButton.enabled = NO;
    _simpleReviewRefreshUWButton.enabled = NO;
    _simpleReviewRefreshRecentTradesButton.enabled = NO;
    if (_simpleReviewBusySpinner != nil) {
        _simpleReviewBusySpinner.hidden = NO;
        [_simpleReviewBusySpinner startAnimation:nil];
    }
    _simpleReviewStateLabel.stringValue = message ?: @"Working…";
    _simpleReviewStateLabel.textColor = color ?: [NSColor systemOrangeColor];
    if (!_pollingPaused) {
        _simpleReviewSuspendedPolling = YES;
        [_pollTimer invalidate];
        _pollTimer = nil;
        _pollingPaused = YES;
        [self updatePollingStatusText];
    }
}

- (void)endSimpleReviewBusy {
    _simpleReviewInFlight = NO;
    _simpleReviewLoadButton.enabled = YES;
    _simpleReviewAskButton.enabled = YES;
    _simpleReviewRefreshUWButton.enabled = YES;
    _simpleReviewRefreshRecentTradesButton.enabled = YES;
    if (_simpleReviewBusySpinner != nil) {
        [_simpleReviewBusySpinner stopAnimation:nil];
        _simpleReviewBusySpinner.hidden = YES;
    }
    if (_simpleReviewSuspendedPolling) {
        _simpleReviewSuspendedPolling = NO;
        _pollingPaused = NO;
        if (_pollTimer == nil) {
            _pollTimer = [NSTimer scheduledTimerWithTimeInterval:tapescope_window_internal::kPollIntervalSeconds
                                                          target:self
                                                        selector:@selector(refresh:)
                                                        userInfo:nil
                                                         repeats:YES];
        }
        [self updatePollingStatusText];
    }
}

- (std::vector<json>)fallbackSimpleReviewRecentTradeRows {
    return FallbackRecentTradeRowsFromHistory(_recentHistoryItems);
}

- (void)syncSimpleReviewAnchorIntoAdvanced {
    [_orderCaseAnchorTypePopup selectItemAtIndex:_simpleReviewAnchorTypePopup.indexOfSelectedItem];
    _orderCaseAnchorInputField.stringValue = _simpleReviewAnchorInputField.stringValue ?: @"";
    [_seekAnchorTypePopup selectItemAtIndex:_simpleReviewAnchorTypePopup.indexOfSelectedItem];
    _seekAnchorInputField.stringValue = _simpleReviewAnchorInputField.stringValue ?: @"";
    [self orderCaseAnchorTypeChanged:nil];
    [self seekAnchorTypeChanged:nil];
}

- (void)refreshSimpleReviewDetailText {
    if (_simpleReviewDetailTextView == nil) {
        return;
    }
    const std::string detailText = BuildSimpleReviewDetailText(_simpleReviewContextBody,
                                                               _simpleReviewReplayBody,
                                                               _simpleReviewAiBody);
    NSLog(@"simpleReview detail lengths context=%lu replay=%lu ai=%lu total=%lu",
          static_cast<unsigned long>(_simpleReviewContextBody.size()),
          static_cast<unsigned long>(_simpleReviewReplayBody.size()),
          static_cast<unsigned long>(_simpleReviewAiBody.size()),
          static_cast<unsigned long>(detailText.size()));
    SetTextViewString(_simpleReviewDetailTextView, ToNSString(detailText));
}

- (NSTabViewItem*)simpleReviewTabItem {
    const auto paneWithStack = MakePaneWithStack();
    NSStackView* stack = paneWithStack.stack;
    NSView* pane = paneWithStack.view;

    const auto recentTradesCardWithStack = MakeCardWithStack(8.0);
    recentTradesCardWithStack.stack.alignment = NSLayoutAttributeWidth;
    [recentTradesCardWithStack.stack addArrangedSubview:MakeSectionLabel(@"1. Pick A Recent Trade")];

    NSStackView* recentHeaderRow = MakeControlRow();
    _simpleReviewRefreshRecentTradesButton = [NSButton buttonWithTitle:@"Refresh Recent Trades"
                                                                target:self
                                                                action:@selector(loadSimpleReviewRecentTrades:)];
    [recentHeaderRow addArrangedSubview:_simpleReviewRefreshRecentTradesButton];
    [recentTradesCardWithStack.stack addArrangedSubview:recentHeaderRow];

    _simpleReviewRecentTradesTableView = MakeStandardTableView(self, self);
    AddTableColumn(_simpleReviewRecentTradesTableView, @"trade", @"", 190.0);
    AddTableColumn(_simpleReviewRecentTradesTableView, @"event_kind", @"", 98.0);
    AddTableColumn(_simpleReviewRecentTradesTableView, @"session_seq", @"", 84.0);
    ConfigureCompactDarkTableView(_simpleReviewRecentTradesTableView);
    ConfigureTablePrimaryAction(_simpleReviewRecentTradesTableView, self, @selector(loadSelectedSimpleReviewRecentTrade:));
    NSScrollView* recentTradesScroll = MakeTableScrollView(_simpleReviewRecentTradesTableView, 132.0);
    ConfigureCompactDarkTableScrollView(recentTradesScroll);
    [recentTradesCardWithStack.stack addArrangedSubview:recentTradesScroll];

    _simpleReviewRecentTradesStateLabel =
        MakeLabel(@"Recent trades will populate once TapeScope connects to tape_engine.",
                  [NSFont systemFontOfSize:12.0 weight:NSFontWeightMedium],
                  TapeInkMutedColor());
    [recentTradesCardWithStack.stack addArrangedSubview:_simpleReviewRecentTradesStateLabel];
    [recentTradesCardWithStack.box.widthAnchor constraintGreaterThanOrEqualToConstant:220.0].active = YES;

    const auto controlsCardWithStack = MakeCardWithStack(10.0);
    controlsCardWithStack.stack.alignment = NSLayoutAttributeWidth;
    [controlsCardWithStack.stack addArrangedSubview:MakeSectionLabel(@"2. Load, Replay, And Ask")];

    NSStackView* anchorRow = MakeControlRow();
    _simpleReviewAnchorTypePopup = [[NSPopUpButton alloc] initWithFrame:NSMakeRect(0, 0, 120, 26) pullsDown:NO];
    [_simpleReviewAnchorTypePopup addItemsWithTitles:@[@"Trace Id", @"Order Id", @"Perm Id", @"Exec Id"]];
    _simpleReviewAnchorTypePopup.target = self;
    _simpleReviewAnchorTypePopup.action = @selector(simpleReviewAnchorTypeChanged:);
    [anchorRow addArrangedSubview:_simpleReviewAnchorTypePopup];

    _simpleReviewAnchorInputField = MakeMonospacedField(180.0, nil, PlaceholderForOrderAnchorType(OrderAnchorType::TraceId));
    [anchorRow addArrangedSubview:_simpleReviewAnchorInputField];

    _simpleReviewLoadButton = [NSButton buttonWithTitle:@"Load Trade Context"
                                                 target:self
                                                 action:@selector(loadSimpleReviewContext:)];
    _simpleReviewLoadButton.bezelColor = SimplePrimaryButtonColor();
    _simpleReviewLoadButton.contentTintColor = [NSColor whiteColor];
    [anchorRow addArrangedSubview:_simpleReviewLoadButton];
    _simpleReviewJumpButton = [NSButton buttonWithTitle:@"Open Replay Timeline"
                                                 target:self
                                                 action:@selector(loadSimpleReviewReplay:)];
    _simpleReviewJumpButton.enabled = NO;
    [anchorRow addArrangedSubview:_simpleReviewJumpButton];
    [controlsCardWithStack.stack addArrangedSubview:anchorRow];

    NSStackView* replayRow = MakeControlRow();
    _simpleReviewOpenAdvancedButton = [NSButton buttonWithTitle:@"Open Advanced Order Case"
                                                         target:self
                                                         action:@selector(openSimpleReviewAdvanced:)];
    [replayRow addArrangedSubview:_simpleReviewOpenAdvancedButton];
    _simpleReviewRefreshUWButton = [NSButton buttonWithTitle:@"Refresh UW"
                                                      target:self
                                                      action:@selector(refreshSimpleReviewExternalContext:)];
    [replayRow addArrangedSubview:_simpleReviewRefreshUWButton];
    [controlsCardWithStack.stack addArrangedSubview:replayRow];

    [controlsCardWithStack.stack addArrangedSubview:MakeSectionLabel(@"3. Ask UW + Gemini")];
    NSStackView* askRow = MakeControlRow();
    _simpleReviewQuestionField = [[NSTextField alloc] initWithFrame:NSMakeRect(0, 0, 320, 24)];
    _simpleReviewQuestionField.font = [NSFont systemFontOfSize:12.5 weight:NSFontWeightMedium];
    _simpleReviewQuestionField.placeholderString = @"Example: Why did I get filled here? Was liquidity pulling? What did UW add?";
    [askRow addArrangedSubview:_simpleReviewQuestionField];

    _simpleReviewAskButton = [NSButton buttonWithTitle:@"Ask Gemini"
                                                target:self
                                                action:@selector(askSimpleReviewQuestion:)];
    _simpleReviewAskButton.bezelColor = SimplePrimaryButtonColor();
    _simpleReviewAskButton.contentTintColor = [NSColor whiteColor];
    [askRow addArrangedSubview:_simpleReviewAskButton];
    [controlsCardWithStack.stack addArrangedSubview:askRow];

    NSStackView* stateRow = MakeControlRow();
    _simpleReviewBusySpinner = [[NSProgressIndicator alloc] initWithFrame:NSMakeRect(0, 0, 16, 16)];
    ConfigureSimpleReviewBusySpinner(_simpleReviewBusySpinner);
    [stateRow addArrangedSubview:_simpleReviewBusySpinner];
    _simpleReviewStateLabel = MakeLabel(@"Load the trade first, then inspect replay or ask UW + Gemini a focused question.",
                                        [NSFont systemFontOfSize:12.5 weight:NSFontWeightMedium],
                                        TapeInkMutedColor());
    [stateRow addArrangedSubview:_simpleReviewStateLabel];
    [controlsCardWithStack.stack addArrangedSubview:stateRow];
    [controlsCardWithStack.box.widthAnchor constraintGreaterThanOrEqualToConstant:340.0].active = YES;

    const auto detailCardWithStack = MakeCardWithStack(8.0);
    detailCardWithStack.stack.alignment = NSLayoutAttributeWidth;
    [detailCardWithStack.stack addArrangedSubview:MakeSectionLabel(@"Review Details")];
    _simpleReviewDetailTextView = MakeReadOnlyTextView();
    _simpleReviewContextBody = "No trade context loaded yet.";
    _simpleReviewReplayBody = "No replay snapshot loaded yet.";
    _simpleReviewAiBody = "No AI answer yet.";
    [detailCardWithStack.stack addArrangedSubview:MakeScrollView(_simpleReviewDetailTextView, 148.0)];
    [detailCardWithStack.box setContentCompressionResistancePriority:NSLayoutPriorityDefaultLow
                                                      forOrientation:NSLayoutConstraintOrientationVertical];
    [detailCardWithStack.box setContentHuggingPriority:NSLayoutPriorityDefaultLow
                                        forOrientation:NSLayoutConstraintOrientationVertical];
    [self refreshSimpleReviewDetailText];

    NSView* reviewRightColumn = [[NSView alloc] initWithFrame:NSZeroRect];
    reviewRightColumn.translatesAutoresizingMaskIntoConstraints = NO;
    NSStackView* reviewRightStack = MakeColumnStack(8.0);
    reviewRightStack.alignment = NSLayoutAttributeWidth;
    [reviewRightColumn addSubview:reviewRightStack];
    [NSLayoutConstraint activateConstraints:@[
        [reviewRightStack.leadingAnchor constraintEqualToAnchor:reviewRightColumn.leadingAnchor],
        [reviewRightStack.trailingAnchor constraintEqualToAnchor:reviewRightColumn.trailingAnchor],
        [reviewRightStack.topAnchor constraintEqualToAnchor:reviewRightColumn.topAnchor],
        [reviewRightStack.bottomAnchor constraintEqualToAnchor:reviewRightColumn.bottomAnchor]
    ]];
    [reviewRightStack addArrangedSubview:controlsCardWithStack.box];

    [reviewRightStack addArrangedSubview:detailCardWithStack.box];
    [controlsCardWithStack.box.widthAnchor constraintEqualToAnchor:reviewRightColumn.widthAnchor].active = YES;
    [detailCardWithStack.box.widthAnchor constraintEqualToAnchor:reviewRightColumn.widthAnchor].active = YES;

    NSStackView* mainColumns = [[NSStackView alloc] initWithFrame:NSZeroRect];
    mainColumns.translatesAutoresizingMaskIntoConstraints = NO;
    mainColumns.orientation = NSUserInterfaceLayoutOrientationHorizontal;
    mainColumns.alignment = NSLayoutAttributeTop;
    mainColumns.distribution = NSStackViewDistributionFill;
    mainColumns.spacing = 10.0;
    [mainColumns addArrangedSubview:recentTradesCardWithStack.box];
    [mainColumns addArrangedSubview:reviewRightColumn];
    [recentTradesCardWithStack.box.widthAnchor constraintEqualToConstant:260.0].active = YES;
    [reviewRightColumn.widthAnchor constraintGreaterThanOrEqualToConstant:420.0].active = YES;
    [recentTradesCardWithStack.box setContentCompressionResistancePriority:NSLayoutPriorityRequired
                                                            forOrientation:NSLayoutConstraintOrientationHorizontal];
    [reviewRightColumn setContentCompressionResistancePriority:NSLayoutPriorityDefaultLow
                                               forOrientation:NSLayoutConstraintOrientationHorizontal];
    [stack addArrangedSubview:mainColumns];

    NSTabViewItem* item = [[NSTabViewItem alloc] initWithIdentifier:@"SimpleReviewPane"];
    item.label = @"Review";
    item.view = pane;
    return item;
}

- (void)simpleReviewAnchorTypeChanged:(id)sender {
    (void)sender;
    _simpleReviewAnchorInputField.placeholderString =
        PlaceholderForOrderAnchorType(OrderAnchorTypeFromIndex(_simpleReviewAnchorTypePopup.indexOfSelectedItem));
}

- (void)loadSimpleReviewRecentTrades:(id)sender {
    (void)sender;
    if (_simpleReviewRecentTradesInFlight || !_client) {
        return;
    }

    _simpleReviewRecentTradesInFlight = YES;
    const std::uint64_t token = [self issueRequestToken:&_simpleReviewRecentTradesRequestToken];
    _simpleReviewRefreshRecentTradesButton.enabled = NO;
    _simpleReviewRecentTradesStateLabel.stringValue = @"Loading recent trades…";
    _simpleReviewRecentTradesStateLabel.textColor = [NSColor systemOrangeColor];

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_interactiveQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->status();
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil ||
                ![innerSelf isRequestTokenCurrent:token storage:&innerSelf->_simpleReviewRecentTradesRequestToken]) {
                return;
            }

            innerSelf->_simpleReviewRecentTradesInFlight = NO;
            innerSelf->_simpleReviewRefreshRecentTradesButton.enabled = YES;

            if (!result.ok()) {
                innerSelf->_simpleReviewRecentTradesStateLabel.stringValue =
                    ToNSString(tapescope::QueryClient::describeError(result.error));
                innerSelf->_simpleReviewRecentTradesStateLabel.textColor = ErrorColorForKind(result.error.kind);
                return;
            }

            innerSelf->_simpleReviewRecentTradeRows = result.value.topOrderAnchors;
            if (innerSelf->_simpleReviewRecentTradeRows.empty()) {
                innerSelf->_simpleReviewRecentTradeRows =
                    FallbackRecentTradeRowsFromHistory(innerSelf->_recentHistoryItems);
            }
            [innerSelf->_simpleReviewRecentTradesTableView reloadData];
            if (innerSelf->_simpleReviewRecentTradeRows.empty()) {
                innerSelf->_simpleReviewRecentTradesStateLabel.stringValue =
                    @"No recent trade anchors yet. Take a trade or let tape_engine capture one, then refresh.";
                innerSelf->_simpleReviewRecentTradesStateLabel.textColor = TapeInkMutedColor();
                return;
            }

                innerSelf->_simpleReviewRecentTradesStateLabel.stringValue =
                    [NSString stringWithFormat:@"Loaded %lu recent trades. Click any row to review it.",
                                               static_cast<unsigned long>(innerSelf->_simpleReviewRecentTradeRows.size())];
            innerSelf->_simpleReviewRecentTradesStateLabel.textColor = [NSColor systemGreenColor];

            NSInteger selectedRow = innerSelf->_simpleReviewRecentTradesTableView.selectedRow;
            if (selectedRow < 0 || static_cast<std::size_t>(selectedRow) >= innerSelf->_simpleReviewRecentTradeRows.size()) {
                selectedRow = 0;
                [innerSelf->_simpleReviewRecentTradesTableView
                    selectRowIndexes:[NSIndexSet indexSetWithIndex:0]
                byExtendingSelection:NO];
            }
            if (!innerSelf->_simpleReviewDidAutoLoadRecentTrade && selectedRow >= 0) {
                innerSelf->_simpleReviewDidAutoLoadRecentTrade = YES;
                [innerSelf selectSimpleReviewRecentTradeAtIndex:selectedRow autoload:NO];
            }
        });
    });
}

- (void)loadSelectedSimpleReviewRecentTrade:(id)sender {
    (void)sender;
    [self selectSimpleReviewRecentTradeAtIndex:_simpleReviewRecentTradesTableView.selectedRow autoload:YES];
}

- (void)selectSimpleReviewRecentTradeAtIndex:(NSInteger)selectedRow autoload:(BOOL)autoload {
    if (selectedRow < 0 || static_cast<std::size_t>(selectedRow) >= _simpleReviewRecentTradeRows.size()) {
        _simpleReviewRecentTradesStateLabel.stringValue = @"Select a recent trade row first.";
        _simpleReviewRecentTradesStateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    const json& row = _simpleReviewRecentTradeRows.at(static_cast<std::size_t>(selectedRow));
    const auto preferredAnchor = PreferredAnchorFromRecentTradeRow(row);
    if (!preferredAnchor.has_value()) {
        _simpleReviewRecentTradesStateLabel.stringValue =
            @"Selected row is missing trace/order/perm/exec anchor data.";
        _simpleReviewRecentTradesStateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    [_simpleReviewAnchorTypePopup selectItemAtIndex:OrderAnchorTypeIndexForKey(OrderAnchorTypeKey(preferredAnchor->first))];
    _simpleReviewAnchorInputField.stringValue = ToNSString(preferredAnchor->second);
    [self simpleReviewAnchorTypeChanged:nil];

    _simpleReviewRecentTradesStateLabel.stringValue = ToNSString(RecentTradeSummary(row));
    _simpleReviewRecentTradesStateLabel.textColor = TapeInkMutedColor();

    if (autoload) {
        if (_simpleReviewInFlight) {
            _simpleReviewStateLabel.stringValue = @"Selected trade updated. Finish the current request, then load again if needed.";
            _simpleReviewStateLabel.textColor = [NSColor systemOrangeColor];
        } else {
            [self loadSimpleReviewContext:nil];
        }
    } else {
        _simpleReviewStateLabel.stringValue =
            @"Trade selected. Click Load Trade Context when you want replay, Level 2, and UW/Gemini.";
        _simpleReviewStateLabel.textColor = TapeInkMutedColor();
    }
}

- (void)loadSimpleReviewContext:(id)sender {
    (void)sender;
    if (_simpleReviewInFlight || !_client) {
        return;
    }

    tapescope::OrderAnchorQuery query;
    std::string descriptor;
    std::string errorMessage;
    if (![self buildOrderAnchorQueryFromPopup:_simpleReviewAnchorTypePopup
                                   inputField:_simpleReviewAnchorInputField
                                     outQuery:&query
                                   descriptor:&descriptor
                                        error:&errorMessage]) {
        _simpleReviewStateLabel.stringValue = ToNSString(errorMessage);
        _simpleReviewStateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    const std::uint64_t token = [self issueRequestToken:&_simpleReviewRequestToken];
    [self beginSimpleReviewBusyWithMessage:@"Loading fast trade review and replay snapshot…"
                                     color:[NSColor systemOrangeColor]];

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_interactiveQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }

        const auto investigation = strongSelf->_client->readTradeReviewPayload(query);
        tapescope::QueryResult<tapescope::ReplaySnapshotPayload> snapshot;
        if (investigation.ok()) {
            const std::uint64_t replayTargetSessionSeq =
                investigation.value.summary.value("replay_target_session_seq", 0ULL);
            if (replayTargetSessionSeq > 0) {
                snapshot = strongSelf->_client->replaySnapshotPayload(replayTargetSessionSeq, 5, false);
            }
        }

        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil || ![innerSelf isRequestTokenCurrent:token storage:&innerSelf->_simpleReviewRequestToken]) {
                return;
            }

            [innerSelf endSimpleReviewBusy];

            if (investigation.ok()) {
                SyncSimpleReviewArtifactField(innerSelf->_artifactIdField, investigation.value);
                innerSelf->_simpleReviewContextBody = DescribeSimpleReviewContext(investigation.value);
                [innerSelf recordRecentHistoryForKind:"trade_review"
                                             targetId:descriptor
                                              payload:investigation.value
                                        fallbackTitle:"Trade review " + descriptor
                                       fallbackDetail:"Reopen the recent trade review."
                                             metadata:tapescope::json{{"anchor_kind",
                                                                       OrderAnchorTypeKey(OrderAnchorTypeFromIndex(innerSelf->_simpleReviewAnchorTypePopup.indexOfSelectedItem))},
                                                                      {"anchor_value",
                                                                       ToStdString(innerSelf->_simpleReviewAnchorInputField.stringValue)}}];
            } else {
                innerSelf->_simpleReviewContextBody =
                    DescribeInvestigationPayload("trade_review", descriptor, investigation);
            }

            innerSelf->_simpleReviewHasReplayRange = investigation.ok() && investigation.value.replayRange.has_value();
            if (innerSelf->_simpleReviewHasReplayRange) {
                innerSelf->_simpleReviewReplayRange = *investigation.value.replayRange;
            } else {
                innerSelf->_simpleReviewReplayRange = {};
            }
            innerSelf->_simpleReviewJumpButton.enabled = innerSelf->_simpleReviewHasReplayRange;

            if (snapshot.ok()) {
                innerSelf->_simpleReviewReplayBody = DescribeReplaySnapshotPayload(snapshot);
            } else {
                innerSelf->_simpleReviewReplayBody =
                    investigation.ok()
                        ? "Replay target was computed, but the Level 2 snapshot could not be loaded yet."
                        : "Load a trade review first so TapeScope can compute the replay window and Level 2 snapshot.";
            }
            innerSelf->_simpleReviewAiBody = "No AI answer yet.";
            [innerSelf refreshSimpleReviewDetailText];

            if (investigation.ok()) {
                innerSelf->_simpleReviewStateLabel.stringValue =
                    innerSelf->_simpleReviewHasReplayRange
                        ? @"Trade context ready. You can open the replay timeline or ask Gemini a focused question."
                        : @"Trade context loaded, but no replay window was available for this selector.";
                innerSelf->_simpleReviewStateLabel.textColor =
                    innerSelf->_simpleReviewHasReplayRange ? [NSColor systemGreenColor] : [NSColor systemOrangeColor];
                const std::string autorunQuestion = reviewAutorunQuestion();
                if (!autorunQuestion.empty() && !innerSelf->_simpleReviewDidAutorunQuestion) {
                    innerSelf->_simpleReviewDidAutorunQuestion = YES;
                    innerSelf->_simpleReviewQuestionField.stringValue = ToNSString(autorunQuestion);
                    dispatch_after(dispatch_time(DISPATCH_TIME_NOW, static_cast<int64_t>(0.2 * NSEC_PER_SEC)),
                                   dispatch_get_main_queue(), ^{
                                       [innerSelf askSimpleReviewQuestion:nil];
                                   });
                }
            } else {
                innerSelf->_simpleReviewStateLabel.stringValue = ToNSString(tapescope::QueryClient::describeError(investigation.error));
                innerSelf->_simpleReviewStateLabel.textColor = ErrorColorForKind(investigation.error.kind);
            }
        });
    });
}

- (void)loadSimpleReviewReplay:(id)sender {
    (void)sender;
    [self loadReplayRange:_simpleReviewReplayRange
                available:_simpleReviewHasReplayRange
               stateLabel:_simpleReviewStateLabel
           missingMessage:@"Load the trade context first so TapeScope can compute the replay window around it."];
}

- (void)askSimpleReviewQuestion:(id)sender {
    (void)sender;
    if (_simpleReviewInFlight || !_client) {
        return;
    }

    tapescope::OrderAnchorQuery query;
    std::string descriptor;
    std::string errorMessage;
    if (![self buildOrderAnchorQueryFromPopup:_simpleReviewAnchorTypePopup
                                   inputField:_simpleReviewAnchorInputField
                                     outQuery:&query
                                   descriptor:&descriptor
                                        error:&errorMessage]) {
        _simpleReviewStateLabel.stringValue = ToNSString(errorMessage);
        _simpleReviewStateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    const std::string focusQuestion = defaultReviewQuestion(ToStdString(_simpleReviewQuestionField.stringValue));
    const std::uint64_t token = [self issueRequestToken:&_simpleReviewRequestToken];
    [self beginSimpleReviewBusyWithMessage:@"Asking Gemini about this trade…"
                                     color:[NSColor systemOrangeColor]];

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_interactiveQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->enrichTradeReviewPayload(query, focusQuestion);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil || ![innerSelf isRequestTokenCurrent:token storage:&innerSelf->_simpleReviewRequestToken]) {
                return;
            }

            [innerSelf endSimpleReviewBusy];

            if (result.ok()) {
                SyncSimpleReviewArtifactField(innerSelf->_artifactIdField, result.value);
                innerSelf->_simpleReviewContextBody = DescribeSimpleReviewContext(result.value.localEvidence);
                if (result.value.localEvidence.replayRange.has_value()) {
                    innerSelf->_simpleReviewReplayRange = *result.value.localEvidence.replayRange;
                    innerSelf->_simpleReviewHasReplayRange = YES;
                    innerSelf->_simpleReviewJumpButton.enabled = YES;
                }
                innerSelf->_simpleReviewAiBody = DescribeSimpleReviewAnswer(result.value);
                [innerSelf refreshSimpleReviewDetailText];
                if (HasCompletedGeminiInterpretation(result.value)) {
                    innerSelf->_simpleReviewStateLabel.stringValue = @"UW + Gemini answer ready.";
                    innerSelf->_simpleReviewStateLabel.textColor = [NSColor systemGreenColor];
                } else {
                    innerSelf->_simpleReviewStateLabel.stringValue =
                        @"Trade context loaded, but Gemini interpretation was unavailable for this run.";
                    innerSelf->_simpleReviewStateLabel.textColor = [NSColor systemOrangeColor];
                }
                NSLog(@"simpleReview Ask Gemini completed ok=1 question=%@", innerSelf->_simpleReviewQuestionField.stringValue);
            } else {
                innerSelf->_simpleReviewAiBody =
                    DescribeEnrichmentPayload("trade_review_enrichment", descriptor, result);
                [innerSelf refreshSimpleReviewDetailText];
                innerSelf->_simpleReviewStateLabel.stringValue = ToNSString(tapescope::QueryClient::describeError(result.error));
                innerSelf->_simpleReviewStateLabel.textColor = ErrorColorForKind(result.error.kind);
                NSLog(@"simpleReview Ask Gemini completed ok=0 error=%@", innerSelf->_simpleReviewStateLabel.stringValue);
            }
        });
    });
}

- (void)refreshSimpleReviewExternalContext:(id)sender {
    (void)sender;
    if (_simpleReviewInFlight || !_client) {
        return;
    }

    tapescope::OrderAnchorQuery query;
    std::string descriptor;
    std::string errorMessage;
    if (![self buildOrderAnchorQueryFromPopup:_simpleReviewAnchorTypePopup
                                   inputField:_simpleReviewAnchorInputField
                                     outQuery:&query
                                   descriptor:&descriptor
                                        error:&errorMessage]) {
        _simpleReviewStateLabel.stringValue = ToNSString(errorMessage);
        _simpleReviewStateLabel.textColor = [NSColor systemRedColor];
        return;
    }

    const std::uint64_t token = [self issueRequestToken:&_simpleReviewRequestToken];
    [self beginSimpleReviewBusyWithMessage:@"Refreshing Unusual Whales context…"
                                     color:[NSColor systemOrangeColor]];

    __weak TapeScopeWindowController* weakSelf = self;
    dispatch_async(_interactiveQueue, ^{
        TapeScopeWindowController* strongSelf = weakSelf;
        if (strongSelf == nil || !strongSelf->_client) {
            return;
        }
        const auto result = strongSelf->_client->refreshTradeReviewExternalContextPayload(query);
        dispatch_async(dispatch_get_main_queue(), ^{
            TapeScopeWindowController* innerSelf = weakSelf;
            if (innerSelf == nil || ![innerSelf isRequestTokenCurrent:token storage:&innerSelf->_simpleReviewRequestToken]) {
                return;
            }

            [innerSelf endSimpleReviewBusy];

            if (result.ok()) {
                SyncSimpleReviewArtifactField(innerSelf->_artifactIdField, result.value);
                innerSelf->_simpleReviewContextBody = DescribeSimpleReviewContext(result.value.localEvidence);
                innerSelf->_simpleReviewAiBody = DescribeSimpleReviewAnswer(result.value);
                [innerSelf refreshSimpleReviewDetailText];
                innerSelf->_simpleReviewStateLabel.stringValue = @"Unusual Whales context refreshed.";
                innerSelf->_simpleReviewStateLabel.textColor = [NSColor systemGreenColor];
            } else {
                innerSelf->_simpleReviewAiBody =
                    DescribeEnrichmentPayload("trade_review_refresh", descriptor, result);
                [innerSelf refreshSimpleReviewDetailText];
                innerSelf->_simpleReviewStateLabel.stringValue = ToNSString(tapescope::QueryClient::describeError(result.error));
                innerSelf->_simpleReviewStateLabel.textColor = ErrorColorForKind(result.error.kind);
            }
        });
    });
}

- (void)openSimpleReviewAdvanced:(id)sender {
    (void)sender;
    [self syncSimpleReviewAnchorIntoAdvanced];
    [self selectPaneWithIdentifier:@"OrderCasePane"];
    [self persistApplicationState];
}

@end
