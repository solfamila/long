#import "trading_export_panels.h"

#include "trace_exporter.h"

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

bool WriteStringToURL(const std::string& content, NSURL* url, NSError** error) {
    NSString* text = [[NSString alloc] initWithBytes:content.data()
                                              length:content.size()
                                            encoding:NSUTF8StringEncoding];
    if (text == nil) {
        if (error != nullptr) {
            *error = [NSError errorWithDomain:NSCocoaErrorDomain
                                         code:NSFileWriteInapplicableStringEncodingError
                                     userInfo:@{NSLocalizedDescriptionKey: @"Failed to encode UTF-8 text"}];
        }
        return false;
    }
    return [text writeToURL:url atomically:YES encoding:NSUTF8StringEncoding error:error];
}

void ShowAlert(NSWindow* window, NSString* title, NSString* message, NSAlertStyle style) {
    NSAlert* alert = [[NSAlert alloc] init];
    alert.messageText = title ?: @"Notice";
    alert.informativeText = message ?: @"";
    alert.alertStyle = style;
    [alert beginSheetModalForWindow:window completionHandler:nil];
}

} // namespace

bool RunSelectedTraceExportPanel(NSWindow* parentWindow,
                                 std::uint64_t traceId,
                                 std::string* successMessage) {
    if (successMessage != nullptr) {
        successMessage->clear();
    }

    if (traceId == 0) {
        ShowAlert(parentWindow, @"No Trace Selected", @"Select a trade trace before exporting.", NSAlertStyleWarning);
        return false;
    }

    TraceExportBundle bundle;
    std::string error;
    if (!buildTraceExportBundle(traceId, &bundle, &error)) {
        ShowAlert(parentWindow, @"Export Failed", ToNSString(error), NSAlertStyleCritical);
        return false;
    }

    NSOpenPanel* panel = [NSOpenPanel openPanel];
    panel.canChooseDirectories = YES;
    panel.canChooseFiles = NO;
    panel.canCreateDirectories = YES;
    panel.prompt = @"Export";
    panel.message = @"Choose a folder for the selected trace export.";
    if ([panel runModal] != NSModalResponseOK) {
        return false;
    }

    NSURL* directoryURL = panel.URL;
    NSError* writeError = nil;
    const std::string base = bundle.baseName;
    const bool wroteReport = WriteStringToURL(bundle.reportText,
        [directoryURL URLByAppendingPathComponent:ToNSString(base + "-report.txt")], &writeError);
    const bool wroteSummary = wroteReport && WriteStringToURL(bundle.summaryCsv,
        [directoryURL URLByAppendingPathComponent:ToNSString(base + "-summary.csv")], &writeError);
    const bool wroteFills = wroteSummary && WriteStringToURL(bundle.fillsCsv,
        [directoryURL URLByAppendingPathComponent:ToNSString(base + "-fills.csv")], &writeError);
    const bool wroteTimeline = wroteFills && WriteStringToURL(bundle.timelineCsv,
        [directoryURL URLByAppendingPathComponent:ToNSString(base + "-timeline.csv")], &writeError);

    if (!wroteTimeline) {
        ShowAlert(parentWindow,
                  @"Export Failed",
                  writeError.localizedDescription ?: @"Failed to write export files.",
                  NSAlertStyleCritical);
        return false;
    }

    if (successMessage != nullptr) {
        *successMessage = "Exported trace bundle to " + ToStdString(directoryURL.path);
    }
    return true;
}

bool RunAllTradesSummaryExportPanel(NSWindow* parentWindow,
                                    std::string* successMessage) {
    if (successMessage != nullptr) {
        successMessage->clear();
    }

    NSSavePanel* panel = [NSSavePanel savePanel];
    panel.nameFieldStringValue = @"all-trades-summary.csv";
    panel.prompt = @"Save CSV";
    if ([panel runModal] != NSModalResponseOK) {
        return false;
    }

    NSError* writeError = nil;
    const std::string csv = buildAllTradesSummaryCsv();
    if (!WriteStringToURL(csv, panel.URL, &writeError)) {
        ShowAlert(parentWindow,
                  @"Export Failed",
                  writeError.localizedDescription ?: @"Failed to save CSV.",
                  NSAlertStyleCritical);
        return false;
    }

    if (successMessage != nullptr) {
        *successMessage = "Exported trade summary CSV to " + ToStdString(panel.URL.path);
    }
    return true;
}
