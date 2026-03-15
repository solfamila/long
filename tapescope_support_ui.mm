#include "tapescope_support.h"

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cstdlib>
#include <limits>

@implementation ActionTableView

- (void)performPrimaryActionFrom:(id)sender {
    if (self.primaryActionTarget == nil || self.primaryAction == nullptr || self.selectedRow < 0) {
        return;
    }
    [NSApp sendAction:self.primaryAction to:self.primaryActionTarget from:sender];
}

- (void)keyDown:(NSEvent*)event {
    NSString* chars = event.charactersIgnoringModifiers ?: @"";
    if (([chars isEqualToString:@"\r"] || [chars isEqualToString:@" "] || [chars isEqualToString:@"\n"]) &&
        self.selectedRow >= 0 && self.primaryAction != nullptr) {
        [self performPrimaryActionFrom:self];
        return;
    }
    [super keyDown:event];
}

@end

namespace tapescope_support {

NSColor* TapeBackgroundColor() {
    return [NSColor colorWithCalibratedRed:0.953 green:0.941 blue:0.914 alpha:1.0];
}

NSColor* TapeCardColor() {
    return [NSColor colorWithCalibratedRed:0.986 green:0.979 blue:0.965 alpha:0.96];
}

NSColor* TapeCardBorderColor() {
    return [NSColor colorWithCalibratedRed:0.835 green:0.792 blue:0.706 alpha:0.9];
}

NSColor* TapePanelFillColor() {
    return [NSColor colorWithCalibratedRed:0.994 green:0.991 blue:0.982 alpha:1.0];
}

NSColor* TapePanelBorderColor() {
    return [NSColor colorWithCalibratedRed:0.869 green:0.833 blue:0.761 alpha:0.95];
}

NSColor* TapeInkMutedColor() {
    return [NSColor colorWithCalibratedRed:0.345 green:0.333 blue:0.302 alpha:1.0];
}

NSString* ToNSString(const std::string& value) {
    if (value.empty()) {
        return @"";
    }
    return [NSString stringWithUTF8String:value.c_str()];
}

std::string ToStdString(NSString* value) {
    if (value == nil) {
        return {};
    }
    const char* utf8 = [value UTF8String];
    return utf8 == nullptr ? std::string() : std::string(utf8);
}

std::string TrimAscii(std::string value) {
    const auto begin = std::find_if_not(value.begin(), value.end(), [](unsigned char ch) {
        return std::isspace(ch) != 0;
    });
    const auto end = std::find_if_not(value.rbegin(), value.rend(), [](unsigned char ch) {
        return std::isspace(ch) != 0;
    }).base();
    if (begin >= end) {
        return {};
    }
    return std::string(begin, end);
}

bool ParsePositiveUInt64(const std::string& raw, std::uint64_t* parsed) {
    if (parsed == nullptr) {
        return false;
    }
    const std::string value = TrimAscii(raw);
    if (value.empty()) {
        return false;
    }
    char* consumed = nullptr;
    errno = 0;
    const unsigned long long converted = std::strtoull(value.c_str(), &consumed, 10);
    if (errno == ERANGE || consumed == nullptr || *consumed != '\0' || converted == 0) {
        return false;
    }
    *parsed = static_cast<std::uint64_t>(converted);
    return true;
}

bool ParsePositiveInt64(const std::string& raw, long long* parsed) {
    if (parsed == nullptr) {
        return false;
    }
    std::uint64_t value = 0;
    if (!ParsePositiveUInt64(raw, &value) ||
        value > static_cast<std::uint64_t>(std::numeric_limits<long long>::max())) {
        return false;
    }
    *parsed = static_cast<long long>(value);
    return true;
}

NSString* UInt64String(std::uint64_t value) {
    return [NSString stringWithFormat:@"%llu", static_cast<unsigned long long>(value)];
}

NSTextField* MakeLabel(NSString* text, NSFont* font, NSColor* color) {
    NSTextField* label = [NSTextField labelWithString:text ?: @""];
    label.font = font;
    label.textColor = color ?: [NSColor labelColor];
    label.lineBreakMode = NSLineBreakByTruncatingTail;
    return label;
}

NSTextField* MakeValueLabel() {
    return MakeLabel(@"--",
                     [NSFont monospacedSystemFontOfSize:12.5 weight:NSFontWeightMedium],
                     TapeInkMutedColor());
}

NSTextView* MakeReadOnlyTextView() {
    NSTextView* textView = [[NSTextView alloc] initWithFrame:NSZeroRect];
    textView.editable = NO;
    textView.selectable = YES;
    textView.richText = NO;
    textView.automaticQuoteSubstitutionEnabled = NO;
    textView.automaticDashSubstitutionEnabled = NO;
    textView.automaticTextReplacementEnabled = NO;
    textView.font = [NSFont monospacedSystemFontOfSize:12.0 weight:NSFontWeightRegular];
    textView.textColor = [NSColor labelColor];
    textView.backgroundColor = TapePanelFillColor();
    textView.textContainerInset = NSMakeSize(12.0, 12.0);
    return textView;
}

NSScrollView* MakeScrollView(NSTextView* textView, CGFloat minHeight) {
    NSScrollView* scrollView = [[NSScrollView alloc] initWithFrame:NSMakeRect(0, 0, 800, minHeight)];
    scrollView.hasVerticalScroller = YES;
    scrollView.hasHorizontalScroller = YES;
    scrollView.borderType = NSLineBorder;
    scrollView.backgroundColor = TapePanelFillColor();
    scrollView.autoresizingMask = NSViewWidthSizable | NSViewHeightSizable;
    scrollView.documentView = textView;
    [scrollView.heightAnchor constraintGreaterThanOrEqualToConstant:minHeight].active = YES;
    return scrollView;
}

NSStackView* MakeColumnStack(CGFloat spacing) {
    NSStackView* stack = [[NSStackView alloc] initWithFrame:NSZeroRect];
    stack.orientation = NSUserInterfaceLayoutOrientationVertical;
    stack.alignment = NSLayoutAttributeLeading;
    stack.spacing = spacing;
    stack.translatesAutoresizingMaskIntoConstraints = NO;
    return stack;
}

NSBox* MakeCardBox(CGFloat cornerRadius) {
    NSBox* box = [[NSBox alloc] initWithFrame:NSZeroRect];
    box.boxType = NSBoxCustom;
    box.cornerRadius = cornerRadius;
    box.borderWidth = 1.0;
    box.fillColor = TapeCardColor();
    box.borderColor = TapeCardBorderColor();
    box.translatesAutoresizingMaskIntoConstraints = NO;
    return box;
}

ViewWithStack MakePaneWithStack() {
    NSView* pane = [[NSView alloc] initWithFrame:NSZeroRect];
    NSStackView* stack = MakeColumnStack(10.0);
    [pane addSubview:stack];
    [NSLayoutConstraint activateConstraints:@[
        [stack.leadingAnchor constraintEqualToAnchor:pane.leadingAnchor constant:10.0],
        [stack.trailingAnchor constraintEqualToAnchor:pane.trailingAnchor constant:-10.0],
        [stack.topAnchor constraintEqualToAnchor:pane.topAnchor constant:10.0],
        [stack.bottomAnchor constraintEqualToAnchor:pane.bottomAnchor constant:-10.0]
    ]];
    return ViewWithStack{pane, stack};
}

CardWithStack MakeCardWithStack(CGFloat spacing) {
    NSBox* card = MakeCardBox();
    NSStackView* stack = MakeColumnStack(spacing);
    [card addSubview:stack];
    [NSLayoutConstraint activateConstraints:@[
        [stack.leadingAnchor constraintEqualToAnchor:card.leadingAnchor constant:16.0],
        [stack.trailingAnchor constraintEqualToAnchor:card.trailingAnchor constant:-16.0],
        [stack.topAnchor constraintEqualToAnchor:card.topAnchor constant:16.0],
        [stack.bottomAnchor constraintEqualToAnchor:card.bottomAnchor constant:-16.0]
    ]];
    return CardWithStack{card, stack};
}

NSTextField* MakeIntroLabel(NSString* text, NSInteger lines) {
    NSTextField* label = MakeLabel(text,
                                   [NSFont systemFontOfSize:12.5 weight:NSFontWeightMedium],
                                   TapeInkMutedColor());
    label.lineBreakMode = NSLineBreakByWordWrapping;
    label.maximumNumberOfLines = lines;
    return label;
}

NSTextField* MakeSectionLabel(NSString* text) {
    return MakeLabel(text,
                     [NSFont systemFontOfSize:12.0 weight:NSFontWeightSemibold],
                     TapeInkMutedColor());
}

NSStackView* MakeControlRow() {
    NSStackView* controls = [[NSStackView alloc] initWithFrame:NSZeroRect];
    controls.orientation = NSUserInterfaceLayoutOrientationHorizontal;
    controls.alignment = NSLayoutAttributeCenterY;
    controls.spacing = 8.0;
    controls.translatesAutoresizingMaskIntoConstraints = NO;
    return controls;
}

NSTextField* MakeMonospacedField(CGFloat width,
                                 NSString* initialValue,
                                 NSString* placeholder) {
    NSTextField* field = [[NSTextField alloc] initWithFrame:NSMakeRect(0, 0, width, 24)];
    field.font = [NSFont monospacedSystemFontOfSize:12.0 weight:NSFontWeightMedium];
    if (initialValue != nil) {
        field.stringValue = initialValue;
    }
    if (placeholder != nil) {
        field.placeholderString = placeholder;
    }
    return field;
}

NSTableView* MakeStandardTableView(id delegate, id dataSource) {
    NSTableView* tableView = [[ActionTableView alloc] initWithFrame:NSZeroRect];
    tableView.usesAlternatingRowBackgroundColors = NO;
    tableView.allowsEmptySelection = YES;
    tableView.allowsMultipleSelection = NO;
    tableView.rowHeight = 28.0;
    tableView.intercellSpacing = NSMakeSize(6.0, 4.0);
    tableView.selectionHighlightStyle = NSTableViewSelectionHighlightStyleRegular;
    tableView.gridStyleMask = NSTableViewSolidHorizontalGridLineMask;
    tableView.gridColor = [TapePanelBorderColor() colorWithAlphaComponent:0.45];
    tableView.backgroundColor = TapePanelFillColor();
    tableView.delegate = delegate;
    tableView.dataSource = dataSource;
    if (@available(macOS 11.0, *)) {
        tableView.style = NSTableViewStyleInset;
    }
    return tableView;
}

void ConfigureTablePrimaryAction(NSTableView* tableView, id target, SEL action) {
    if (tableView == nil || action == nullptr) {
        return;
    }
    tableView.target = target;
    tableView.doubleAction = action;
    if ([tableView isKindOfClass:[ActionTableView class]]) {
        ActionTableView* actionTableView = (ActionTableView*)tableView;
        actionTableView.primaryActionTarget = target;
        actionTableView.primaryAction = action;
    }
}

void AddTableColumn(NSTableView* tableView,
                    NSString* identifier,
                    NSString* title,
                    CGFloat width) {
    NSTableColumn* column = [[NSTableColumn alloc] initWithIdentifier:identifier];
    column.title = title;
    column.width = width;
    column.minWidth = 90.0;
    [tableView addTableColumn:column];
}

NSScrollView* MakeTableScrollView(NSTableView* tableView, CGFloat minHeight) {
    NSScrollView* scrollView = [[NSScrollView alloc] initWithFrame:NSMakeRect(0, 0, 900, minHeight)];
    scrollView.hasVerticalScroller = YES;
    scrollView.hasHorizontalScroller = YES;
    scrollView.borderType = NSLineBorder;
    scrollView.backgroundColor = TapePanelFillColor();
    scrollView.autoresizingMask = NSViewWidthSizable | NSViewHeightSizable;
    scrollView.documentView = tableView;
    [scrollView.heightAnchor constraintGreaterThanOrEqualToConstant:minHeight].active = YES;
    return scrollView;
}

NSTableView* MakeEvidenceTableView(id delegate, id dataSource) {
    NSTableView* tableView = MakeStandardTableView(delegate, dataSource);
    AddTableColumn(tableView, @"kind", @"kind", 130.0);
    AddTableColumn(tableView, @"artifact_id", @"artifact_id", 260.0);
    AddTableColumn(tableView, @"label", @"label", 360.0);
    return tableView;
}

NSTableCellView* MakeOrReuseTableCell(NSTableView* tableView,
                                      NSString* identifier,
                                      NSFont* font) {
    NSTableCellView* cell = [tableView makeViewWithIdentifier:identifier owner:nil];
    if (cell == nil) {
        cell = [[NSTableCellView alloc] initWithFrame:NSZeroRect];
        cell.identifier = identifier;

        NSTextField* label = [NSTextField labelWithString:@""];
        label.translatesAutoresizingMaskIntoConstraints = NO;
        label.lineBreakMode = NSLineBreakByTruncatingTail;
        [cell addSubview:label];
        [NSLayoutConstraint activateConstraints:@[
            [label.leadingAnchor constraintEqualToAnchor:cell.leadingAnchor constant:4.0],
            [label.trailingAnchor constraintEqualToAnchor:cell.trailingAnchor constant:-4.0],
            [label.centerYAnchor constraintEqualToAnchor:cell.centerYAnchor]
        ]];
        cell.textField = label;
    }
    cell.textField.font = font;
    return cell;
}

NSColor* ErrorColorForKind(tapescope::QueryErrorKind kind) {
    switch (kind) {
        case tapescope::QueryErrorKind::Transport:
            return [NSColor systemRedColor];
        case tapescope::QueryErrorKind::Remote:
            return [NSColor systemOrangeColor];
        case tapescope::QueryErrorKind::MalformedResponse:
            return [NSColor systemYellowColor];
        case tapescope::QueryErrorKind::None:
            break;
    }
    return [NSColor secondaryLabelColor];
}

OrderAnchorType OrderAnchorTypeFromIndex(NSInteger index) {
    switch (index) {
        case 1:
            return OrderAnchorType::OrderId;
        case 2:
            return OrderAnchorType::PermId;
        case 3:
            return OrderAnchorType::ExecId;
        case 0:
        default:
            return OrderAnchorType::TraceId;
    }
}

std::string OrderAnchorTypeKey(OrderAnchorType type) {
    switch (type) {
        case OrderAnchorType::TraceId:
            return "traceId";
        case OrderAnchorType::OrderId:
            return "orderId";
        case OrderAnchorType::PermId:
            return "permId";
        case OrderAnchorType::ExecId:
            return "execId";
    }
    return "traceId";
}

NSInteger OrderAnchorTypeIndexForKey(const std::string& key) {
    if (key == "orderId") {
        return 1;
    }
    if (key == "permId") {
        return 2;
    }
    if (key == "execId") {
        return 3;
    }
    return 0;
}

NSString* PlaceholderForOrderAnchorType(OrderAnchorType type) {
    switch (type) {
        case OrderAnchorType::TraceId:
            return @"e.g. 71";
        case OrderAnchorType::OrderId:
            return @"e.g. 501";
        case OrderAnchorType::PermId:
            return @"e.g. 9501";
        case OrderAnchorType::ExecId:
            return @"e.g. EXEC-71";
    }
    return @"";
}

} // namespace tapescope_support
