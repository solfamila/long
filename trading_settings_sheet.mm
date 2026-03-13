#import "trading_settings_sheet.h"

@interface TradingSettingsActionTarget : NSObject
@property(nonatomic, copy) void (^actionBlock)(void);
- (void)invoke:(id)sender;
@end

@implementation TradingSettingsActionTarget
- (void)invoke:(id)sender {
    (void)sender;
    if (self.actionBlock) {
        self.actionBlock();
    }
}
@end

namespace {

NSString* ToNSString(const std::string& value) {
    return [NSString stringWithUTF8String:value.c_str()];
}

std::string ToStdString(NSString* value) {
    if (value == nil) {
        return {};
    }
    return std::string([value UTF8String]);
}

NSColor* PanelBackgroundColor() {
    return [NSColor colorWithCalibratedWhite:0.985 alpha:1.0];
}

NSTextField* MakeLabel(NSString* text, NSFont* font, NSColor* color) {
    NSTextField* label = [NSTextField labelWithString:text ?: @""];
    label.font = font;
    label.textColor = color ?: [NSColor labelColor];
    label.lineBreakMode = NSLineBreakByTruncatingTail;
    label.maximumNumberOfLines = 1;
    return label;
}

NSTextField* MakeWrappingLabel(NSString* text, NSFont* font, NSColor* color) {
    NSTextField* label = [NSTextField wrappingLabelWithString:text ?: @""];
    label.font = font;
    label.textColor = color ?: [NSColor labelColor];
    return label;
}

NSTextField* MakeInputField(NSString* text, CGFloat width) {
    NSTextField* field = [[NSTextField alloc] initWithFrame:NSZeroRect];
    field.stringValue = text ?: @"";
    field.font = [NSFont monospacedSystemFontOfSize:13.0 weight:NSFontWeightRegular];
    field.bordered = YES;
    field.bezelStyle = NSTextFieldRoundedBezel;
    field.backgroundColor = [NSColor whiteColor];
    field.textColor = [NSColor labelColor];
    [field.widthAnchor constraintEqualToConstant:width].active = YES;
    return field;
}

NSButton* MakeButton(NSString* title, id target, SEL action) {
    NSButton* button = [NSButton buttonWithTitle:title target:target action:action];
    button.bezelStyle = NSBezelStyleRounded;
    button.bordered = YES;
    button.buttonType = NSButtonTypeMomentaryPushIn;
    button.controlSize = NSControlSizeLarge;
    button.font = [NSFont systemFontOfSize:13.0 weight:NSFontWeightSemibold];
    button.translatesAutoresizingMaskIntoConstraints = NO;
    [button.heightAnchor constraintEqualToConstant:32.0].active = YES;
    return button;
}

NSStackView* MakeRowStack() {
    NSStackView* stack = [[NSStackView alloc] initWithFrame:NSZeroRect];
    stack.orientation = NSUserInterfaceLayoutOrientationHorizontal;
    stack.alignment = NSLayoutAttributeCenterY;
    stack.spacing = 10.0;
    stack.translatesAutoresizingMaskIntoConstraints = NO;
    return stack;
}

NSStackView* MakeColumnStack() {
    NSStackView* stack = [[NSStackView alloc] initWithFrame:NSZeroRect];
    stack.orientation = NSUserInterfaceLayoutOrientationVertical;
    stack.alignment = NSLayoutAttributeLeading;
    stack.spacing = 12.0;
    stack.translatesAutoresizingMaskIntoConstraints = NO;
    return stack;
}

NSView* MakeFlexibleSpacer() {
    NSView* spacer = [[NSView alloc] initWithFrame:NSZeroRect];
    spacer.translatesAutoresizingMaskIntoConstraints = NO;
    [spacer setContentHuggingPriority:NSLayoutPriorityDefaultLow forOrientation:NSLayoutConstraintOrientationHorizontal];
    [spacer setContentCompressionResistancePriority:NSLayoutPriorityDefaultLow forOrientation:NSLayoutConstraintOrientationHorizontal];
    return spacer;
}

void StyleTintedButton(NSButton* button, NSColor* bezelColor, NSColor* tintColor) {
    button.bezelColor = bezelColor;
    button.contentTintColor = tintColor;
}

NSInteger ControllerArmModePopupIndex(ControllerArmMode mode) {
    return mode == ControllerArmMode::Manual ? 1 : 0;
}

ControllerArmMode ControllerArmModeFromPopupIndex(NSInteger index) {
    return index == 1 ? ControllerArmMode::Manual : ControllerArmMode::OneShot;
}

} // namespace

bool RunTradingSettingsSheet(NSWindow* parentWindow,
                             const RuntimeConnectionConfig& currentConnection,
                             const RiskControlsSnapshot& currentRisk,
                             RuntimeConnectionConfig* outConnection,
                             RiskControlsSnapshot* outRisk) {
    if (parentWindow == nil || outConnection == nullptr || outRisk == nullptr) {
        return false;
    }

    NSWindow* sheet = [[NSWindow alloc] initWithContentRect:NSMakeRect(0.0, 0.0, 620.0, 480.0)
                                                  styleMask:NSWindowStyleMaskTitled
                                                    backing:NSBackingStoreBuffered
                                                      defer:NO];
    sheet.title = @"Trading Settings";
    sheet.releasedWhenClosed = NO;
    sheet.movable = NO;
    sheet.backgroundColor = PanelBackgroundColor();

    NSView* contentView = sheet.contentView;
    NSStackView* rootStack = MakeColumnStack();
    rootStack.spacing = 16.0;
    rootStack.edgeInsets = NSEdgeInsetsMake(18.0, 20.0, 18.0, 20.0);
    [contentView addSubview:rootStack];
    [NSLayoutConstraint activateConstraints:@[
        [rootStack.leadingAnchor constraintEqualToAnchor:contentView.leadingAnchor],
        [rootStack.trailingAnchor constraintEqualToAnchor:contentView.trailingAnchor],
        [rootStack.topAnchor constraintEqualToAnchor:contentView.topAnchor],
        [rootStack.bottomAnchor constraintEqualToAnchor:contentView.bottomAnchor],
    ]];

    NSTextField* infoLabel = MakeWrappingLabel(@"Connection and device changes apply on next launch. Risk limits, controller arming mode, and the WebSocket token apply immediately.",
                                               [NSFont systemFontOfSize:12.5 weight:NSFontWeightMedium],
                                               [NSColor secondaryLabelColor]);
    [rootStack addArrangedSubview:infoLabel];
    [infoLabel.widthAnchor constraintEqualToAnchor:rootStack.widthAnchor].active = YES;

    NSGridView* grid = [NSGridView gridViewWithNumberOfColumns:2 rows:8];
    grid.rowSpacing = 10.0;
    grid.columnSpacing = 14.0;
    grid.xPlacement = NSGridCellPlacementLeading;
    grid.yPlacement = NSGridCellPlacementCenter;

    NSTextField* hostField = MakeInputField(ToNSString(currentConnection.host), 300.0);
    NSTextField* portField = MakeInputField([NSString stringWithFormat:@"%d", currentConnection.port], 120.0);
    NSTextField* clientIdField = MakeInputField([NSString stringWithFormat:@"%d", currentConnection.clientId], 120.0);
    NSTextField* tokenField = MakeInputField(ToNSString(currentConnection.websocketAuthToken), 300.0);
    NSTextField* staleQuoteField = MakeInputField([NSString stringWithFormat:@"%d", currentRisk.staleQuoteThresholdMs], 120.0);
    NSTextField* maxOrderField = MakeInputField([NSString stringWithFormat:@"%.0f", currentRisk.maxOrderNotional], 140.0);
    NSTextField* maxOpenField = MakeInputField([NSString stringWithFormat:@"%.0f", currentRisk.maxOpenNotional], 140.0);
    NSPopUpButton* controllerArmModePopup = [[NSPopUpButton alloc] initWithFrame:NSZeroRect pullsDown:NO];
    controllerArmModePopup.translatesAutoresizingMaskIntoConstraints = NO;
    controllerArmModePopup.font = [NSFont systemFontOfSize:13.0 weight:NSFontWeightRegular];
    [controllerArmModePopup addItemWithTitle:@"One-shot after controller trade"];
    [controllerArmModePopup addItemWithTitle:@"Stay armed until manual disarm"];
    [controllerArmModePopup selectItemAtIndex:ControllerArmModePopupIndex(currentRisk.controllerArmMode)];
    [controllerArmModePopup.widthAnchor constraintEqualToConstant:280.0].active = YES;

    NSArray<NSString*>* labels = @[
        @"TWS Host",
        @"TWS Port",
        @"TWS Client ID",
        @"WebSocket Token",
        @"Stale Quote (ms)",
        @"Max Order Notional",
        @"Max Open Notional",
        @"Controller Arming",
    ];
    NSArray<NSView*>* fields = @[
        hostField,
        portField,
        clientIdField,
        tokenField,
        staleQuoteField,
        maxOrderField,
        maxOpenField,
        controllerArmModePopup,
    ];

    for (NSInteger i = 0; i < labels.count; ++i) {
        NSTextField* label = MakeLabel(labels[i],
                                       [NSFont systemFontOfSize:12.5 weight:NSFontWeightSemibold],
                                       [NSColor secondaryLabelColor]);
        [label.widthAnchor constraintEqualToConstant:170.0].active = YES;
        [grid cellAtColumnIndex:0 rowIndex:i].contentView = label;
        [grid cellAtColumnIndex:1 rowIndex:i].contentView = fields[static_cast<std::size_t>(i)];
    }
    [rootStack addArrangedSubview:grid];
    [grid.widthAnchor constraintEqualToAnchor:rootStack.widthAnchor].active = YES;

    NSButton* websocketCheckbox = [NSButton checkboxWithTitle:@"Enable localhost WebSocket automation" target:nil action:nil];
    websocketCheckbox.state = currentConnection.websocketEnabled ? NSControlStateValueOn : NSControlStateValueOff;
    NSButton* controllerCheckbox = [NSButton checkboxWithTitle:@"Enable controller input" target:nil action:nil];
    controllerCheckbox.state = currentConnection.controllerEnabled ? NSControlStateValueOn : NSControlStateValueOff;
    [rootStack addArrangedSubview:websocketCheckbox];
    [rootStack addArrangedSubview:controllerCheckbox];

    NSStackView* buttonRow = MakeRowStack();
    buttonRow.spacing = 12.0;
    [buttonRow addArrangedSubview:MakeFlexibleSpacer()];
    NSButton* cancelButton = MakeButton(@"Cancel", nil, nil);
    [cancelButton.widthAnchor constraintEqualToConstant:110.0].active = YES;
    StyleTintedButton(cancelButton, [NSColor colorWithCalibratedWhite:0.72 alpha:1.0], [NSColor labelColor]);
    NSButton* saveButton = MakeButton(@"Save", nil, nil);
    [saveButton.widthAnchor constraintEqualToConstant:120.0].active = YES;
    StyleTintedButton(saveButton, [NSColor colorWithCalibratedRed:0.15 green:0.45 blue:0.95 alpha:1.0], [NSColor whiteColor]);
    [buttonRow addArrangedSubview:cancelButton];
    [buttonRow addArrangedSubview:saveButton];
    [rootStack addArrangedSubview:buttonRow];
    [buttonRow.widthAnchor constraintEqualToAnchor:rootStack.widthAnchor].active = YES;

    __block NSInteger modalResponse = NSModalResponseCancel;
    TradingSettingsActionTarget* saveTarget = [[TradingSettingsActionTarget alloc] init];
    saveTarget.actionBlock = ^{
        modalResponse = NSModalResponseOK;
        [NSApp stopModalWithCode:NSModalResponseOK];
    };
    saveButton.target = saveTarget;
    saveButton.action = @selector(invoke:);

    TradingSettingsActionTarget* cancelTarget = [[TradingSettingsActionTarget alloc] init];
    cancelTarget.actionBlock = ^{
        modalResponse = NSModalResponseCancel;
        [NSApp stopModalWithCode:NSModalResponseCancel];
    };
    cancelButton.target = cancelTarget;
    cancelButton.action = @selector(invoke:);

    [parentWindow beginSheet:sheet completionHandler:nil];
    [sheet makeFirstResponder:hostField];
    [NSApp runModalForWindow:sheet];
    [parentWindow endSheet:sheet];
    [sheet orderOut:nil];

    if (modalResponse != NSModalResponseOK) {
        return false;
    }

    RuntimeConnectionConfig updatedConnection = currentConnection;
    updatedConnection.host = ToStdString(hostField.stringValue);
    updatedConnection.port = std::max(1, portField.intValue);
    updatedConnection.clientId = std::max(1, clientIdField.intValue);
    updatedConnection.websocketAuthToken = ToStdString(tokenField.stringValue);
    updatedConnection.websocketEnabled = (websocketCheckbox.state == NSControlStateValueOn);
    updatedConnection.controllerEnabled = (controllerCheckbox.state == NSControlStateValueOn);

    RiskControlsSnapshot updatedRisk = currentRisk;
    updatedRisk.staleQuoteThresholdMs = std::max(250, staleQuoteField.intValue);
    updatedRisk.maxOrderNotional = std::max(100.0, maxOrderField.doubleValue);
    updatedRisk.maxOpenNotional = std::max(updatedRisk.maxOrderNotional, maxOpenField.doubleValue);
    updatedRisk.controllerArmMode = ControllerArmModeFromPopupIndex(controllerArmModePopup.indexOfSelectedItem);

    *outConnection = updatedConnection;
    *outRisk = updatedRisk;
    return true;
}
