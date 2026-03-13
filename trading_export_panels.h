#pragma once

#import <AppKit/AppKit.h>

#include <cstdint>
#include <string>

bool RunSelectedTraceExportPanel(NSWindow* parentWindow,
                                 std::uint64_t traceId,
                                 std::string* successMessage);
bool RunAllTradesSummaryExportPanel(NSWindow* parentWindow,
                                    std::string* successMessage);
