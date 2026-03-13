#pragma once

#import <AppKit/AppKit.h>

#include "app_shared.h"

bool RunTradingSettingsSheet(NSWindow* parentWindow,
                             const RuntimeConnectionConfig& currentConnection,
                             const RiskControlsSnapshot& currentRisk,
                             RuntimeConnectionConfig* outConnection,
                             RiskControlsSnapshot* outRisk);
