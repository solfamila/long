#include "tape_mcp_adapter.h"
#include "tapescope_client_internal.h"

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <functional>
#include <initializer_list>
#include <limits>
#include <optional>
#include <sstream>
#include <utility>

namespace tape_mcp {

namespace {

constexpr const char* kContractVersion = "phase5-mcp-v1";
constexpr const char* kServerVersion = "0.1.0";
constexpr const char* kProtocolVersion = "2024-11-05";
constexpr std::uint32_t kToolEnvelopeVersion = 1;
constexpr const char* kToolEnvelopeSchema = "com.foxy.tape-mcp.tool-envelope";
constexpr const char* kPromptVersion = "phase5-mcp-prompt-v1";

struct PromptArgumentSpec {
    std::string name;
    std::string description;
    bool required = false;
};

struct PromptSpec {
    std::string name;
    std::string title;
    std::string description;
    std::vector<PromptArgumentSpec> arguments;
};

json emptyObjectSchema() {
    return json{
        {"type", "object"},
        {"properties", json::object()},
        {"additionalProperties", false}
    };
}

json withSchemaExamples(json schema, std::initializer_list<json> examples) {
    schema["examples"] = json::array();
    for (const auto& example : examples) {
        schema["examples"].push_back(example);
    }
    return schema;
}

json positiveIntegerSchema() {
    return json{
        {"type", "integer"},
        {"minimum", 1}
    };
}

json booleanSchema() {
    return json{{"type", "boolean"}};
}

json nonNegativeIntegerSchema() {
    return json{
        {"type", "integer"},
        {"minimum", 0}
    };
}

json stringSchema() {
    return json{{"type", "string"}};
}

json stringEnumSchema(std::initializer_list<const char*> values) {
    json schema = stringSchema();
    schema["enum"] = json::array();
    for (const char* value : values) {
        schema["enum"].push_back(value);
    }
    return schema;
}

json replayRangeSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"first_session_seq", positiveIntegerSchema()},
            {"last_session_seq", positiveIntegerSchema()}
        }},
        {"required", json::array({"first_session_seq", "last_session_seq"})},
        {"additionalProperties", false}
    };
}

json eventRowSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"session_seq", positiveIntegerSchema()},
            {"source_seq", positiveIntegerSchema()},
            {"event_kind", stringSchema()},
            {"instrument_id", stringSchema()},
            {"side", stringSchema()},
            {"price", json{{"type", json::array({"number", "null"})}}},
            {"summary", stringSchema()}
        }},
        {"required", json::array({"session_seq", "source_seq", "event_kind", "instrument_id", "side", "summary"})},
        {"additionalProperties", true}
    };
}

json evidenceCitationSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"kind", stringSchema()},
            {"artifact_id", stringSchema()},
            {"label", stringSchema()}
        }},
        {"required", json::array({"kind", "artifact_id", "label"})},
        {"additionalProperties", true}
    };
}

json eventListResultSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"returned_count", json{{"type", "integer"}, {"minimum", 0}}},
            {"events", json{{"type", "array"}, {"items", eventRowSchema()}}}
        }},
        {"required", json::array({"returned_count", "events"})},
        {"additionalProperties", false}
    };
}

json anchorIdentitySchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"trace_id", positiveIntegerSchema()},
            {"order_id", positiveIntegerSchema()},
            {"perm_id", positiveIntegerSchema()},
            {"exec_id", stringSchema()}
        }},
        {"additionalProperties", true}
    };
}

json incidentRowSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"logical_incident_id", positiveIntegerSchema()},
            {"kind", stringSchema()},
            {"score", json{{"type", "number"}}},
            {"title", stringSchema()}
        }},
        {"required", json::array({"logical_incident_id", "kind", "score", "title"})},
        {"additionalProperties", true}
    };
}

json incidentListResultSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"returned_count", json{{"type", "integer"}, {"minimum", 0}}},
            {"incidents", json{{"type", "array"}, {"items", incidentRowSchema()}}}
        }},
        {"required", json::array({"returned_count", "incidents"})},
        {"additionalProperties", false}
    };
}

json orderAnchorRowSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"artifact_id", stringSchema()},
            {"anchor_id", positiveIntegerSchema()},
            {"revision_id", positiveIntegerSchema()},
            {"session_seq", positiveIntegerSchema()},
            {"event_kind", stringSchema()},
            {"instrument_id", stringSchema()},
            {"anchor", anchorIdentitySchema()}
        }},
        {"required", json::array({"artifact_id", "anchor_id", "revision_id", "session_seq", "event_kind", "instrument_id", "anchor"})},
        {"additionalProperties", true}
    };
}

json protectedWindowRowSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"artifact_id", stringSchema()},
            {"window_id", positiveIntegerSchema()},
            {"revision_id", positiveIntegerSchema()},
            {"logical_incident_id", nonNegativeIntegerSchema()},
            {"anchor_session_seq", nonNegativeIntegerSchema()},
            {"first_session_seq", positiveIntegerSchema()},
            {"last_session_seq", positiveIntegerSchema()},
            {"reason", stringSchema()},
            {"instrument_id", stringSchema()},
            {"anchor", anchorIdentitySchema()}
        }},
        {"required", json::array({"artifact_id", "window_id", "revision_id", "logical_incident_id", "anchor_session_seq", "first_session_seq", "last_session_seq", "reason", "instrument_id", "anchor"})},
        {"additionalProperties", true}
    };
}

json findingRowSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"artifact_id", stringSchema()},
            {"finding_id", positiveIntegerSchema()},
            {"revision_id", positiveIntegerSchema()},
            {"logical_incident_id", nonNegativeIntegerSchema()},
            {"incident_revision_id", nonNegativeIntegerSchema()},
            {"kind", stringSchema()},
            {"severity", stringSchema()},
            {"confidence", json{{"type", "number"}}},
            {"first_session_seq", positiveIntegerSchema()},
            {"last_session_seq", positiveIntegerSchema()},
            {"title", stringSchema()},
            {"summary", stringSchema()}
        }},
        {"required", json::array({"artifact_id", "finding_id", "revision_id", "logical_incident_id", "incident_revision_id", "kind", "severity", "confidence", "first_session_seq", "last_session_seq", "title", "summary"})},
        {"additionalProperties", true}
    };
}

json listRowsResultSchema(const char* rowKey, const json& rowSchema) {
    return json{
        {"type", "object"},
        {"properties", {
            {"returned_count", nonNegativeIntegerSchema()},
            {rowKey, json{{"type", "array"}, {"items", rowSchema}}}
        }},
        {"required", json::array({"returned_count", rowKey})},
        {"additionalProperties", false}
    };
}

json replaySnapshotResultSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"served_revision_id", json{{"type", json::array({"integer", "null"})}}},
            {"includes_mutable_tail", booleanSchema()},
            {"target_session_seq", nonNegativeIntegerSchema()},
            {"replayed_through_session_seq", nonNegativeIntegerSchema()},
            {"applied_event_count", nonNegativeIntegerSchema()},
            {"gap_markers_encountered", nonNegativeIntegerSchema()},
            {"checkpoint_used", booleanSchema()},
            {"checkpoint_revision_id", nonNegativeIntegerSchema()},
            {"checkpoint_session_seq", nonNegativeIntegerSchema()},
            {"bid_price", json{{"type", json::array({"number", "null"})}}},
            {"ask_price", json{{"type", json::array({"number", "null"})}}},
            {"last_price", json{{"type", json::array({"number", "null"})}}},
            {"bid_book", json{{"type", "array"}}},
            {"ask_book", json{{"type", "array"}}},
            {"data_quality", json{{"type", "object"}, {"additionalProperties", true}}}
        }},
        {"required", json::array({
            "served_revision_id",
            "includes_mutable_tail",
            "target_session_seq",
            "replayed_through_session_seq",
            "applied_event_count",
            "gap_markers_encountered",
            "checkpoint_used",
            "checkpoint_revision_id",
            "checkpoint_session_seq",
            "bid_price",
            "ask_price",
            "last_price",
            "bid_book",
            "ask_book",
            "data_quality"
        })},
        {"additionalProperties", false}
    };
}

json investigationResultSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"artifact_id", stringSchema()},
            {"artifact_kind", stringSchema()},
            {"headline", stringSchema()},
            {"detail", stringSchema()},
            {"served_revision_id", json{{"type", json::array({"integer", "null"})}}},
            {"includes_mutable_tail", booleanSchema()},
            {"artifact", json{{"type", "object"}, {"additionalProperties", true}}},
            {"entity", json{{"type", "object"}, {"additionalProperties", true}}},
            {"report", json{{"type", "object"}, {"additionalProperties", true}}},
            {"evidence", json{{"type", "object"}, {"additionalProperties", true}}},
            {"data_quality", json{{"type", "object"}, {"additionalProperties", true}}},
            {"replay_range", json{{"oneOf", json::array({replayRangeSchema(), json{{"type", "null"}}})}}},
            {"incident_rows", json{{"type", "array"}, {"items", incidentRowSchema()}}},
            {"citation_rows", json{{"type", "array"}, {"items", evidenceCitationSchema()}}},
            {"events", json{{"type", "array"}, {"items", eventRowSchema()}}}
        }},
        {"required", json::array({
            "artifact_id",
            "artifact_kind",
            "headline",
            "detail",
            "served_revision_id",
            "includes_mutable_tail",
            "artifact",
            "entity",
            "report",
            "evidence",
            "data_quality",
            "replay_range",
            "incident_rows",
            "citation_rows",
            "events"
        })},
        {"additionalProperties", false}
    };
}

json qualityResultSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"served_revision_id", json{{"type", json::array({"integer", "null"})}}},
            {"includes_mutable_tail", booleanSchema()},
            {"first_session_seq", json{{"type", json::array({"integer", "null"})}}},
            {"last_session_seq", json{{"type", json::array({"integer", "null"})}}},
            {"data_quality", json{{"type", "object"}, {"additionalProperties", true}}}
        }},
        {"required", json::array({
            "served_revision_id",
            "includes_mutable_tail",
            "first_session_seq",
            "last_session_seq",
            "data_quality"
        })},
        {"additionalProperties", false}
    };
}

json reportInventoryRowSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"report_id", positiveIntegerSchema()},
            {"revision_id", positiveIntegerSchema()},
            {"artifact_id", stringSchema()},
            {"report_type", stringSchema()},
            {"headline", stringSchema()}
        }},
        {"required", json::array({"report_id", "revision_id", "artifact_id", "report_type", "headline"})},
        {"additionalProperties", true}
    };
}

json reportInventoryResultSchema(const char* rowKey) {
    return json{
        {"type", "object"},
        {"properties", {
            {"returned_count", json{{"type", "integer"}, {"minimum", 0}}},
            {rowKey, json{{"type", "array"}, {"items", reportInventoryRowSchema()}}}
        }},
        {"required", json::array({"returned_count", rowKey})},
        {"additionalProperties", false}
    };
}

json seekOrderResultSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"served_revision_id", json{{"type", json::array({"integer", "null"})}}},
            {"includes_mutable_tail", booleanSchema()},
            {"replay_target_session_seq", json{{"type", "integer"}, {"minimum", 0}}},
            {"first_session_seq", json{{"type", "integer"}, {"minimum", 0}}},
            {"last_session_seq", json{{"type", "integer"}, {"minimum", 0}}},
            {"last_fill_session_seq", json{{"type", "integer"}, {"minimum", 0}}},
            {"replay_range", json{{"oneOf", json::array({replayRangeSchema(), json{{"type", "null"}}})}}}
        }},
        {"required", json::array({
            "served_revision_id",
            "includes_mutable_tail",
            "replay_target_session_seq",
            "first_session_seq",
            "last_session_seq",
            "last_fill_session_seq",
            "replay_range"
        })},
        {"additionalProperties", false}
    };
}

json statusResultSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"socket_path", stringSchema()},
            {"data_dir", stringSchema()},
            {"instrument_id", stringSchema()},
            {"latest_session_seq", json{{"type", "integer"}, {"minimum", 0}}},
            {"live_event_count", json{{"type", "integer"}, {"minimum", 0}}},
            {"segment_count", json{{"type", "integer"}, {"minimum", 0}}},
            {"manifest_hash", stringSchema()}
        }},
        {"required", json::array({
            "socket_path",
            "data_dir",
            "instrument_id",
            "latest_session_seq",
            "live_event_count",
            "segment_count",
            "manifest_hash"
        })},
        {"additionalProperties", false}
    };
}

json artifactExportResultSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"artifact_id", stringSchema()},
            {"format", stringEnumSchema({"markdown", "json-bundle"})},
            {"served_revision_id", json{{"type", json::array({"integer", "null"})}}},
            {"markdown", json{{"type", json::array({"string", "null"})}}},
            {"bundle", json{{"type", json::array({"object", "null"})}, {"additionalProperties", true}}}
        }},
        {"required", json::array({"artifact_id", "format", "served_revision_id", "markdown", "bundle"})},
        {"additionalProperties", false}
    };
}

json toolAnnotations(bool readOnlyHint, bool idempotentHint, bool openWorldHint = false) {
    return json{
        {"readOnlyHint", readOnlyHint},
        {"destructiveHint", false},
        {"idempotentHint", idempotentHint},
        {"openWorldHint", openWorldHint}
    };
}

std::string toolTitle(const ToolSpec& tool) {
    switch (tool.id) {
        case ToolId::Status:
            return "Read Engine Status";
        case ToolId::ReadLiveTail:
            return "Read Live Tail";
        case ToolId::ReadRange:
            return "Read Session Range";
        case ToolId::FindOrderAnchor:
            return "Find Order Anchor";
        case ToolId::ListIncidents:
            return "List Incidents";
        case ToolId::ListOrderAnchors:
            return "List Order Anchors";
        case ToolId::ListProtectedWindows:
            return "List Protected Windows";
        case ToolId::ListFindings:
            return "List Findings";
        case ToolId::ReadSessionOverview:
            return "Read Session Overview";
        case ToolId::ScanSessionReport:
            return "Scan Session Report";
        case ToolId::ReadSessionReport:
            return "Read Session Report";
        case ToolId::ListSessionReports:
            return "List Session Reports";
        case ToolId::ScanIncidentReport:
            return "Scan Incident Report";
        case ToolId::ScanOrderCaseReport:
            return "Scan Order-Case Report";
        case ToolId::ReadCaseReport:
            return "Read Case Report";
        case ToolId::ListCaseReports:
            return "List Case Reports";
        case ToolId::SeekOrderAnchor:
            return "Seek Order Anchor";
        case ToolId::ReadFinding:
            return "Read Finding";
        case ToolId::ReadOrderCase:
            return "Read Order Case";
        case ToolId::ReadOrderAnchor:
            return "Read Order Anchor";
        case ToolId::ReadProtectedWindow:
            return "Read Protected Window";
        case ToolId::ReplaySnapshot:
            return "Replay Snapshot";
        case ToolId::ReadIncident:
            return "Read Incident";
        case ToolId::ReadArtifact:
            return "Read Artifact";
        case ToolId::ExportArtifact:
            return "Export Artifact";
        case ToolId::ReadSessionQuality:
            return "Read Session Quality";
    }
    return tool.name;
}

json toolAnnotationsForSpec(const ToolSpec& tool) {
    switch (tool.id) {
        case ToolId::ScanSessionReport:
        case ToolId::ScanIncidentReport:
        case ToolId::ScanOrderCaseReport:
            return toolAnnotations(false, true, false);
        default:
            return toolAnnotations(true, true, false);
    }
}

json toolInputSchemaForList(const ToolSpec& tool) {
    json schema = tool.inputSchema;
    switch (tool.id) {
        case ToolId::ReadLiveTail:
            return withSchemaExamples(std::move(schema), {json{{"limit", 20}}});
        case ToolId::ReadRange:
            return withSchemaExamples(std::move(schema), {json{{"first_session_seq", 1}, {"last_session_seq", 50}}});
        case ToolId::FindOrderAnchor:
            return withSchemaExamples(std::move(schema), {json{{"order_id", 7401}}, json{{"trace_id", 401}}});
        case ToolId::ListIncidents:
        case ToolId::ListOrderAnchors:
        case ToolId::ListProtectedWindows:
        case ToolId::ListFindings:
        case ToolId::ListSessionReports:
        case ToolId::ListCaseReports:
            return withSchemaExamples(std::move(schema), {json{{"limit", 20}}});
        case ToolId::ReadSessionOverview:
            return withSchemaExamples(std::move(schema), {json{{"first_session_seq", 1}, {"last_session_seq", 200}, {"limit", 20}}});
        case ToolId::ScanSessionReport:
            return withSchemaExamples(std::move(schema), {json{{"first_session_seq", 1}, {"last_session_seq", 200}}});
        case ToolId::ReadSessionReport:
        case ToolId::ReadCaseReport:
            return withSchemaExamples(std::move(schema), {json{{"report_id", 1}}});
        case ToolId::ScanIncidentReport:
        case ToolId::ReadIncident:
            return withSchemaExamples(std::move(schema), {json{{"logical_incident_id", 1}}});
        case ToolId::ScanOrderCaseReport:
        case ToolId::SeekOrderAnchor:
        case ToolId::ReadOrderCase:
            return withSchemaExamples(std::move(schema), {json{{"order_id", 7401}}, json{{"exec_id", "EXEC-401"}}});
        case ToolId::ReadFinding:
            return withSchemaExamples(std::move(schema), {json{{"finding_id", 1}}});
        case ToolId::ReadOrderAnchor:
            return withSchemaExamples(std::move(schema), {json{{"anchor_id", 1}}});
        case ToolId::ReadProtectedWindow:
            return withSchemaExamples(std::move(schema), {json{{"window_id", 1}}});
        case ToolId::ReplaySnapshot:
            return withSchemaExamples(std::move(schema), {json{{"target_session_seq", 50}, {"depth_limit", 5}}});
        case ToolId::ReadArtifact:
            return withSchemaExamples(std::move(schema), {json{{"artifact_id", "session-report:1"}}, json{{"artifact_id", "window:1"}}});
        case ToolId::ExportArtifact:
            return withSchemaExamples(std::move(schema), {json{{"artifact_id", "case-report:1"}, {"export_format", "markdown"}}});
        case ToolId::ReadSessionQuality:
            return withSchemaExamples(std::move(schema), {json{{"first_session_seq", 1}, {"last_session_seq", 200}}});
        case ToolId::Status:
        default:
            return schema;
    }
}

std::vector<PromptSpec> buildPromptSpecs() {
    return {
        {
            "investigate_session_range",
            "Investigate Session Range",
            "Guide an agent through summary, quality, durable reporting, and replay for a session_seq window.",
            {
                {"first_session_seq", "First session_seq in the investigation window.", true},
                {"last_session_seq", "Last session_seq in the investigation window.", true},
                {"revision_id", "Optional frozen revision to pin the investigation.", false},
                {"include_live_tail", "Optional boolean to include the mutable tail on live reads.", false}
            }
        },
        {
            "investigate_order_case",
            "Investigate Order Case",
            "Guide an agent through order/fill investigation, replay targeting, and durable case reporting.",
            {
                {"order_id", "Order id anchor.", false},
                {"trace_id", "Trace id anchor.", false},
                {"perm_id", "Broker perm id anchor.", false},
                {"exec_id", "Execution id anchor.", false},
                {"revision_id", "Optional frozen revision to pin the investigation.", false}
            }
        },
        {
            "investigate_incident",
            "Investigate Incident",
            "Guide an agent through incident drilldown, protected-window evidence, and durable report generation.",
            {
                {"logical_incident_id", "Logical incident id to inspect.", true},
                {"revision_id", "Optional frozen revision to pin the investigation.", false}
            }
        },
        {
            "replay_key_moment",
            "Replay Key Moment",
            "Guide an agent through deterministic replay at a key session_seq with checkpoint-aware snapshot reads.",
            {
                {"target_session_seq", "Session seq to replay through.", true},
                {"revision_id", "Optional frozen revision to pin the replay.", false},
                {"depth_limit", "Optional book depth limit for replay output.", false},
                {"include_live_tail", "Optional boolean to overlay the mutable tail.", false}
            }
        },
        {
            "export_artifact_for_sharing",
            "Export Artifact For Sharing",
            "Guide an agent through reopening an artifact and exporting it as markdown or a JSON bundle.",
            {
                {"artifact_id", "Artifact id to reopen and export.", true},
                {"export_format", "Export format: markdown or json-bundle.", false}
            }
        },
        {
            "investigate_bad_fill",
            "Investigate Bad Fill",
            "Guide an agent through fill-quality, adverse-selection, and replay analysis around a bad fill.",
            {
                {"order_id", "Order id anchor.", false},
                {"trace_id", "Trace id anchor.", false},
                {"perm_id", "Broker perm id anchor.", false},
                {"exec_id", "Execution id anchor.", false},
                {"revision_id", "Optional frozen revision to pin the investigation.", false}
            }
        },
        {
            "investigate_source_gap",
            "Investigate Source Gap",
            "Guide an agent through a source-gap incident using incident, protected-window, and replay reads.",
            {
                {"logical_incident_id", "Logical incident id for the source-gap case.", true},
                {"revision_id", "Optional frozen revision to pin the investigation.", false}
            }
        },
        {
            "summarize_latest_session_incidents",
            "Summarize Latest Session Incidents",
            "Guide an agent through a fast incident summary for the latest session range and durable report generation.",
            {
                {"first_session_seq", "Optional first session seq if the latest range is already known.", false},
                {"last_session_seq", "Optional last session seq if the latest range is already known.", false},
                {"revision_id", "Optional frozen revision to pin the summary.", false}
            }
        }
    };
}

const PromptSpec* findPromptSpec(const std::vector<PromptSpec>& prompts, std::string_view name) {
    for (const auto& prompt : prompts) {
        if (prompt.name == name) {
            return &prompt;
        }
    }
    return nullptr;
}

json promptArgumentList(const PromptSpec& prompt) {
    json arguments = json::array();
    for (const auto& argument : prompt.arguments) {
        arguments.push_back({
            {"name", argument.name},
            {"description", argument.description},
            {"required", argument.required}
        });
    }
    return arguments;
}

std::string promptStringArg(const json& args, const char* key) {
    if (!args.is_object() || !args.contains(key)) {
        return {};
    }
    if (args.at(key).is_string()) {
        return args.at(key).get<std::string>();
    }
    if (args.at(key).is_number_integer() || args.at(key).is_number_unsigned()) {
        return args.at(key).dump();
    }
    if (args.at(key).is_boolean()) {
        return args.at(key).get<bool>() ? "true" : "false";
    }
    return {};
}

std::string promptMessageForPrompt(const PromptSpec& prompt, const json& args) {
    if (prompt.name == "investigate_session_range") {
        const std::string first = promptStringArg(args, "first_session_seq");
        const std::string last = promptStringArg(args, "last_session_seq");
        const std::string revision = promptStringArg(args, "revision_id");
        const std::string includeLiveTail = promptStringArg(args, "include_live_tail");
        std::ostringstream out;
        out << "Investigate session range `" << (first.empty() ? "<first_session_seq>" : first)
            << "-" << (last.empty() ? "<last_session_seq>" : last) << "`";
        if (!revision.empty()) {
            out << " pinned to revision `" << revision << "`";
        }
        out << ". Start with `tapescript_read_session_overview`, then read `tapescript_read_session_quality`";
        if (includeLiveTail == "true") {
            out << " with `include_live_tail=true` on live-friendly reads";
        }
        out << ". If the summary is worth preserving, call `tapescript_scan_session_report`. For the most important incident rows, drill into `tapescript_read_incident` and `tapescript_read_protected_window`. If you need state at a key moment, use `tapescript_replay_snapshot` on the target `session_seq`. Return: headline, top incidents, evidence trust, replay targets, and next actions.";
        return out.str();
    }
    if (prompt.name == "investigate_order_case") {
        const std::string orderId = promptStringArg(args, "order_id");
        const std::string traceId = promptStringArg(args, "trace_id");
        const std::string permId = promptStringArg(args, "perm_id");
        const std::string execId = promptStringArg(args, "exec_id");
        const std::string revision = promptStringArg(args, "revision_id");
        std::string anchor = !orderId.empty() ? "order_id=`" + orderId + "`"
            : !traceId.empty() ? "trace_id=`" + traceId + "`"
            : !permId.empty() ? "perm_id=`" + permId + "`"
            : !execId.empty() ? "exec_id=`" + execId + "`"
            : "the provided order/fill anchor";
        std::ostringstream out;
        out << "Investigate " << anchor;
        if (!revision.empty()) {
            out << " pinned to revision `" << revision << "`";
        }
        out << ". Start with `tapescript_read_order_case`, then use `tapescript_seek_order_anchor` for replay bounds. If you need the raw anchor slice, use `tapescript_find_order_anchor` and `tapescript_read_order_anchor`. For durable output, call `tapescript_scan_order_case_report`. If the case points at an incident or protected window, follow with `tapescript_read_incident` or `tapescript_read_protected_window`. Return: execution summary, adverse selection / fill context, replay target, and evidence-backed next steps.";
        return out.str();
    }
    if (prompt.name == "investigate_incident") {
        const std::string incidentId = promptStringArg(args, "logical_incident_id");
        const std::string revision = promptStringArg(args, "revision_id");
        std::ostringstream out;
        out << "Investigate logical incident `" << (incidentId.empty() ? "<logical_incident_id>" : incidentId) << "`";
        if (!revision.empty()) {
            out << " pinned to revision `" << revision << "`";
        }
        out << ". Start with `tapescript_read_incident`. Reopen protected-window evidence from the citations with `tapescript_read_protected_window`, and use `tapescript_replay_snapshot` if you need deterministic market/book state at the key `session_seq`. If the incident should be shareable, call `tapescript_scan_incident_report` and then `tapescript_export_artifact`. Return: why it matters, what changed first, uncertainty, and the strongest supporting evidence.";
        return out.str();
    }
    if (prompt.name == "replay_key_moment") {
        const std::string target = promptStringArg(args, "target_session_seq");
        const std::string revision = promptStringArg(args, "revision_id");
        const std::string depth = promptStringArg(args, "depth_limit");
        const std::string includeLiveTail = promptStringArg(args, "include_live_tail");
        std::ostringstream out;
        out << "Replay deterministic state through session_seq `" << (target.empty() ? "<target_session_seq>" : target) << "`";
        if (!revision.empty()) {
            out << " at revision `" << revision << "`";
        }
        out << " with `tapescript_replay_snapshot`";
        if (!depth.empty()) {
            out << " and `depth_limit=" << depth << "`";
        }
        if (includeLiveTail == "true") {
            out << " plus `include_live_tail=true`";
        }
        out << ". Use the returned checkpoint, gap-marker, and data-quality fields to explain how trustworthy the replay is. If the replay is tied to an order or incident, pair it with `tapescript_seek_order_anchor`, `tapescript_read_order_case`, or `tapescript_read_incident`.";
        return out.str();
    }
    if (prompt.name == "export_artifact_for_sharing") {
        const std::string artifactId = promptStringArg(args, "artifact_id");
        std::string format = promptStringArg(args, "export_format");
        if (format.empty()) {
            format = "markdown";
        }
        std::ostringstream out;
        out << "Reopen artifact `" << (artifactId.empty() ? "<artifact_id>" : artifactId)
            << "` with `tapescript_read_artifact`, confirm it is the right report/case/window, then export it with `tapescript_export_artifact` using `export_format=\"" << format << "\"`. Summarize the artifact headline, report type, evidence coverage, and any uncertainty before presenting the exported output.";
        return out.str();
    }
    if (prompt.name == "investigate_bad_fill") {
        const std::string orderId = promptStringArg(args, "order_id");
        const std::string traceId = promptStringArg(args, "trace_id");
        const std::string permId = promptStringArg(args, "perm_id");
        const std::string execId = promptStringArg(args, "exec_id");
        const std::string revision = promptStringArg(args, "revision_id");
        std::string anchor = !execId.empty() ? "exec_id=`" + execId + "`"
            : !orderId.empty() ? "order_id=`" + orderId + "`"
            : !traceId.empty() ? "trace_id=`" + traceId + "`"
            : !permId.empty() ? "perm_id=`" + permId + "`"
            : "the provided fill anchor";
        std::ostringstream out;
        out << "Investigate a bad fill around " << anchor;
        if (!revision.empty()) {
            out << " pinned to revision `" << revision << "`";
        }
        out << ". Start with `tapescript_read_order_case` to get fill context and ranked incidents. Use `tapescript_seek_order_anchor` and `tapescript_replay_snapshot` to inspect the market/book state at the fill and immediately after it. If the evidence should be preserved, call `tapescript_scan_order_case_report`, then `tapescript_export_artifact`. Return: fill-quality summary, adverse-selection evidence, replay target, and whether the issue looks market-driven, feed-driven, or strategy-driven.";
        return out.str();
    }
    if (prompt.name == "investigate_source_gap") {
        const std::string incidentId = promptStringArg(args, "logical_incident_id");
        const std::string revision = promptStringArg(args, "revision_id");
        std::ostringstream out;
        out << "Investigate source-gap incident `" << (incidentId.empty() ? "<logical_incident_id>" : incidentId) << "`";
        if (!revision.empty()) {
            out << " pinned to revision `" << revision << "`";
        }
        out << ". Start with `tapescript_read_incident`, then reopen the cited protected window with `tapescript_read_protected_window`. Use `tapescript_read_session_quality` to measure evidence trust for the affected range, and `tapescript_replay_snapshot` if you need to verify whether the gap changes the visible state at a key `session_seq`. Return: scope of the gap, evidence trust, what changed first, and whether the incident is safe to use for execution forensics.";
        return out.str();
    }
    if (prompt.name == "summarize_latest_session_incidents") {
        const std::string first = promptStringArg(args, "first_session_seq");
        const std::string last = promptStringArg(args, "last_session_seq");
        const std::string revision = promptStringArg(args, "revision_id");
        std::ostringstream out;
        out << "Summarize the latest session incidents.";
        if (!first.empty() && !last.empty()) {
            out << " Use session range `" << first << "-" << last << "`";
        } else {
            out << " Start with `tapescript_status` to discover the latest `session_seq`, then choose a recent investigation window.";
        }
        if (!revision.empty()) {
            out << " Pin the work to revision `" << revision << "`";
        }
        out << ". Use `tapescript_read_session_overview`, `tapescript_list_incidents`, and `tapescript_read_session_quality`, and if the summary should persist, call `tapescript_scan_session_report`. Return: top incidents, confidence/uncertainty, any source-gap or bad-fill evidence, and the best next drilldowns.";
        return out.str();
    }
    return prompt.description;
}

enum class ResourceKind {
    Unknown = 0,
    SessionReportJson,
    CaseReportJson,
    SessionArtifactMarkdown,
    SessionArtifactJsonBundle,
    CaseArtifactMarkdown,
    CaseArtifactJsonBundle
};

struct ParsedResourceUri {
    ResourceKind kind = ResourceKind::Unknown;
    std::uint64_t reportId = 0;
};

std::string sessionReportUri(std::uint64_t reportId) {
    return "tape://report/session/" + std::to_string(reportId);
}

std::string caseReportUri(std::uint64_t reportId) {
    return "tape://report/case/" + std::to_string(reportId);
}

std::string sessionArtifactUri(std::uint64_t reportId, const char* format) {
    return "tape://artifact/session-report/" + std::to_string(reportId) + "/" + format;
}

std::string caseArtifactUri(std::uint64_t reportId, const char* format) {
    return "tape://artifact/case-report/" + std::to_string(reportId) + "/" + format;
}

ParsedResourceUri parseResourceUri(std::string_view uri) {
    ParsedResourceUri parsed;
    const std::string text(uri);
    auto parseTrailingId = [&](std::size_t start, std::size_t end) -> std::uint64_t {
        if (start >= end || end > text.size()) {
            return 0;
        }
        const std::string token = text.substr(start, end - start);
        if (token.empty()) {
            return 0;
        }
        try {
            return static_cast<std::uint64_t>(std::stoull(token));
        } catch (...) {
            return 0;
        }
    };

    constexpr std::string_view kSessionReportPrefix = "tape://report/session/";
    constexpr std::string_view kCaseReportPrefix = "tape://report/case/";
    constexpr std::string_view kSessionArtifactPrefix = "tape://artifact/session-report/";
    constexpr std::string_view kCaseArtifactPrefix = "tape://artifact/case-report/";

    if (text.rfind(std::string(kSessionReportPrefix), 0) == 0) {
        parsed.kind = ResourceKind::SessionReportJson;
        parsed.reportId = parseTrailingId(kSessionReportPrefix.size(), text.size());
        return parsed;
    }
    if (text.rfind(std::string(kCaseReportPrefix), 0) == 0) {
        parsed.kind = ResourceKind::CaseReportJson;
        parsed.reportId = parseTrailingId(kCaseReportPrefix.size(), text.size());
        return parsed;
    }
    if (text.rfind(std::string(kSessionArtifactPrefix), 0) == 0) {
        const std::size_t formatPos = text.find('/', kSessionArtifactPrefix.size());
        parsed.reportId = parseTrailingId(kSessionArtifactPrefix.size(), formatPos);
        if (formatPos != std::string::npos) {
            const std::string format = text.substr(formatPos + 1);
            if (format == "markdown") {
                parsed.kind = ResourceKind::SessionArtifactMarkdown;
            } else if (format == "json-bundle") {
                parsed.kind = ResourceKind::SessionArtifactJsonBundle;
            }
        }
        return parsed;
    }
    if (text.rfind(std::string(kCaseArtifactPrefix), 0) == 0) {
        const std::size_t formatPos = text.find('/', kCaseArtifactPrefix.size());
        parsed.reportId = parseTrailingId(kCaseArtifactPrefix.size(), formatPos);
        if (formatPos != std::string::npos) {
            const std::string format = text.substr(formatPos + 1);
            if (format == "markdown") {
                parsed.kind = ResourceKind::CaseArtifactMarkdown;
            } else if (format == "json-bundle") {
                parsed.kind = ResourceKind::CaseArtifactJsonBundle;
            }
        }
        return parsed;
    }
    return parsed;
}

json resourceOkMeta() {
    return json{
        {"ok", true},
        {"contract_version", kContractVersion}
    };
}

json resourceErrorMeta(const std::string& code, const std::string& message) {
    return json{
        {"ok", false},
        {"contract_version", kContractVersion},
        {"code", code},
        {"message", message}
    };
}

json sessionRangeInputSchema(bool allowIncludeLiveTail, bool allowLimit) {
    json properties{
        {"first_session_seq", positiveIntegerSchema()},
        {"last_session_seq", positiveIntegerSchema()},
        {"revision_id", positiveIntegerSchema()}
    };
    if (allowIncludeLiveTail) {
        properties["include_live_tail"] = booleanSchema();
    }
    if (allowLimit) {
        properties["limit"] = positiveIntegerSchema();
    }
    return json{
        {"type", "object"},
        {"properties", std::move(properties)},
        {"required", json::array({"first_session_seq", "last_session_seq"})},
        {"additionalProperties", false}
    };
}

json orderAnchorInputSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"trace_id", positiveIntegerSchema()},
            {"order_id", positiveIntegerSchema()},
            {"perm_id", positiveIntegerSchema()},
            {"exec_id", json{{"type", "string"}, {"minLength", 1}}},
            {"revision_id", positiveIntegerSchema()},
            {"include_live_tail", booleanSchema()},
            {"limit", positiveIntegerSchema()}
        }},
        {"oneOf", json::array({
            json{{"required", json::array({"trace_id"})}},
            json{{"required", json::array({"order_id"})}},
            json{{"required", json::array({"perm_id"})}},
            json{{"required", json::array({"exec_id"})}}
        })},
        {"additionalProperties", false}
    };
}

json incidentInputSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"logical_incident_id", positiveIntegerSchema()},
            {"revision_id", positiveIntegerSchema()},
            {"include_live_tail", booleanSchema()},
            {"limit", positiveIntegerSchema()}
        }},
        {"required", json::array({"logical_incident_id"})},
        {"additionalProperties", false}
    };
}

json artifactInputSchema(bool exportTool) {
    json properties{
        {"artifact_id", json{{"type", "string"}, {"minLength", 1}}},
        {"revision_id", positiveIntegerSchema()},
        {"include_live_tail", booleanSchema()},
        {"limit", positiveIntegerSchema()}
    };
    json required = json::array({"artifact_id"});
    if (exportTool) {
        properties["export_format"] = stringEnumSchema({"markdown", "json-bundle"});
        required.push_back("export_format");
    }
    return json{
        {"type", "object"},
        {"properties", std::move(properties)},
        {"required", std::move(required)},
        {"additionalProperties", false}
    };
}

json limitOnlyInputSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"limit", positiveIntegerSchema()}
        }},
        {"additionalProperties", false}
    };
}

json replaySnapshotInputSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"target_session_seq", positiveIntegerSchema()},
            {"revision_id", positiveIntegerSchema()},
            {"include_live_tail", booleanSchema()},
            {"depth_limit", positiveIntegerSchema()}
        }},
        {"required", json::array({"target_session_seq"})},
        {"additionalProperties", false}
    };
}

json idInputSchema(const char* fieldName, bool allowRevision, bool allowIncludeLiveTail, bool allowLimit) {
    json properties{
        {fieldName, positiveIntegerSchema()}
    };
    json required = json::array({fieldName});
    if (allowRevision) {
        properties["revision_id"] = positiveIntegerSchema();
    }
    if (allowIncludeLiveTail) {
        properties["include_live_tail"] = booleanSchema();
    }
    if (allowLimit) {
        properties["limit"] = positiveIntegerSchema();
    }
    return json{
        {"type", "object"},
        {"properties", std::move(properties)},
        {"required", std::move(required)},
        {"additionalProperties", false}
    };
}

json listInputSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"revision_id", positiveIntegerSchema()},
            {"limit", positiveIntegerSchema()}
        }},
        {"additionalProperties", false}
    };
}

std::vector<ToolSpec> buildToolSpecs() {
    return {
        {ToolId::Status,
         "tapescript_status",
         "Read live engine status and session counters.",
         emptyObjectSchema(),
         statusResultSchema(),
         "phase5.status.v1",
         tape_engine::QueryOperation::Status},
        {ToolId::ReadLiveTail,
         "tapescript_read_live_tail",
         "Read recent live events from the mutable tail.",
         limitOnlyInputSchema(),
         eventListResultSchema(),
         "phase5.event-list.v1",
         tape_engine::QueryOperation::ReadLiveTail},
        {ToolId::ReadRange,
         "tapescript_read_range",
         "Read replay events for a session_seq range.",
         sessionRangeInputSchema(false, false),
         eventListResultSchema(),
         "phase5.event-list.v1",
         tape_engine::QueryOperation::ReadRange},
        {ToolId::FindOrderAnchor,
         "tapescript_find_order_anchor",
         "Find lifecycle and related events for a trace/order/perm/exec anchor.",
         orderAnchorInputSchema(),
         eventListResultSchema(),
         "phase5.event-list.v1",
         tape_engine::QueryOperation::FindOrderAnchor},
        {ToolId::ListIncidents,
         "tapescript_list_incidents",
         "List ranked logical incidents from the current or requested frozen revision.",
         listInputSchema(),
         incidentListResultSchema(),
         "phase5.incident-list.v1",
         tape_engine::QueryOperation::ListIncidents},
        {ToolId::ListOrderAnchors,
         "tapescript_list_order_anchors",
         "List recorded order-anchor artifacts from the current or requested frozen revision.",
         listInputSchema(),
         listRowsResultSchema("order_anchors", orderAnchorRowSchema()),
         "phase5.order-anchor-list.v1",
         tape_engine::QueryOperation::ListOrderAnchors},
        {ToolId::ListProtectedWindows,
         "tapescript_list_protected_windows",
         "List protected forensic windows from the current or requested frozen revision.",
         listInputSchema(),
         listRowsResultSchema("protected_windows", protectedWindowRowSchema()),
         "phase5.protected-window-list.v1",
         tape_engine::QueryOperation::ListProtectedWindows},
        {ToolId::ListFindings,
         "tapescript_list_findings",
         "List findings from the current or requested frozen revision.",
         listInputSchema(),
         listRowsResultSchema("findings", findingRowSchema()),
         "phase5.finding-list.v1",
         tape_engine::QueryOperation::ListFindings},
        {ToolId::ReadSessionOverview,
         "tapescript_read_session_overview",
         "Read a typed investigation overview for a session_seq range.",
         sessionRangeInputSchema(true, true),
         investigationResultSchema(),
         "phase5.investigation.v1",
         tape_engine::QueryOperation::ReadSessionOverview},
        {ToolId::ScanSessionReport,
         "tapescript_scan_session_report",
         "Create or reopen a durable session report for a frozen session_seq range.",
         sessionRangeInputSchema(false, true),
         investigationResultSchema(),
         "phase5.investigation.v1",
         tape_engine::QueryOperation::ScanSessionReport,
         true},
        {ToolId::ReadSessionReport,
         "tapescript_read_session_report",
         "Read a durable session report by report_id.",
         idInputSchema("report_id", true, false, false),
         investigationResultSchema(),
         "phase5.investigation.v1",
         tape_engine::QueryOperation::ReadSessionReport},
        {ToolId::ListSessionReports,
         "tapescript_list_session_reports",
         "List durable session reports available in the engine.",
         listInputSchema(),
         reportInventoryResultSchema("session_reports"),
         "phase5.report-inventory.v1",
         tape_engine::QueryOperation::ListSessionReports},
        {ToolId::ScanIncidentReport,
         "tapescript_scan_incident_report",
         "Create or reopen a durable report for a logical incident.",
         incidentInputSchema(),
         investigationResultSchema(),
         "phase5.investigation.v1",
         tape_engine::QueryOperation::ScanIncidentReport,
         true},
        {ToolId::ScanOrderCaseReport,
         "tapescript_scan_order_case_report",
         "Create or reopen a durable report for an order or fill anchor.",
         orderAnchorInputSchema(),
         investigationResultSchema(),
         "phase5.investigation.v1",
         tape_engine::QueryOperation::ScanOrderCaseReport,
         true},
        {ToolId::ReadCaseReport,
         "tapescript_read_case_report",
         "Read a durable case report by report_id.",
         idInputSchema("report_id", true, false, false),
         investigationResultSchema(),
         "phase5.investigation.v1",
         tape_engine::QueryOperation::ReadCaseReport},
        {ToolId::ListCaseReports,
         "tapescript_list_case_reports",
         "List durable incident and order-case reports available in the engine.",
         listInputSchema(),
         reportInventoryResultSchema("case_reports"),
         "phase5.report-inventory.v1",
         tape_engine::QueryOperation::ListCaseReports},
        {ToolId::SeekOrderAnchor,
         "tapescript_seek_order_anchor",
         "Resolve replay-target session_seq bounds for an order or fill anchor.",
         orderAnchorInputSchema(),
         seekOrderResultSchema(),
         "phase5.seek-order.v1",
         tape_engine::QueryOperation::SeekOrderAnchor},
        {ToolId::ReadFinding,
         "tapescript_read_finding",
         "Read a finding artifact by finding_id.",
         idInputSchema("finding_id", true, true, true),
         investigationResultSchema(),
         "phase5.investigation.v1",
         tape_engine::QueryOperation::ReadFinding},
        {ToolId::ReadOrderCase,
         "tapescript_read_order_case",
         "Read an order or fill investigation case by trace/order/perm/exec anchor.",
         orderAnchorInputSchema(),
         investigationResultSchema(),
         "phase5.investigation.v1",
         tape_engine::QueryOperation::ReadOrderCase},
        {ToolId::ReadOrderAnchor,
         "tapescript_read_order_anchor",
         "Read an order-anchor artifact by anchor_id.",
         idInputSchema("anchor_id", true, true, true),
         investigationResultSchema(),
         "phase5.investigation.v1",
         tape_engine::QueryOperation::ReadOrderAnchor},
        {ToolId::ReadProtectedWindow,
         "tapescript_read_protected_window",
         "Read a protected-window artifact by window_id.",
         idInputSchema("window_id", true, true, true),
         investigationResultSchema(),
         "phase5.investigation.v1",
         tape_engine::QueryOperation::ReadProtectedWindow},
        {ToolId::ReplaySnapshot,
         "tapescript_replay_snapshot",
         "Read a deterministic replay snapshot for a target session_seq.",
         replaySnapshotInputSchema(),
         replaySnapshotResultSchema(),
         "phase5.replay-snapshot.v1",
         tape_engine::QueryOperation::ReplaySnapshot,
         true},
        {ToolId::ReadIncident,
         "tapescript_read_incident",
         "Read a logical incident drilldown with evidence and replay context.",
         incidentInputSchema(),
         investigationResultSchema(),
         "phase5.investigation.v1",
         tape_engine::QueryOperation::ReadIncident},
        {ToolId::ReadArtifact,
         "tapescript_read_artifact",
         "Read a durable report or selector-style artifact by artifact_id.",
         artifactInputSchema(false),
         investigationResultSchema(),
         "phase5.investigation.v1",
         tape_engine::QueryOperation::ReadArtifact},
        {ToolId::ExportArtifact,
         "tapescript_export_artifact",
         "Export a durable artifact as markdown or json-bundle.",
         artifactInputSchema(true),
         artifactExportResultSchema(),
         "phase5.artifact-export.v1",
         tape_engine::QueryOperation::ExportArtifact,
         true},
        {ToolId::ReadSessionQuality,
         "tapescript_read_session_quality",
         "Read data-quality and provenance scoring for a session_seq range.",
         sessionRangeInputSchema(true, false),
         qualityResultSchema(),
         "phase5.session-quality.v1",
         tape_engine::QueryOperation::ReadSessionQuality}
    };
}

std::string envValueOrEmpty(const char* key) {
    const char* value = std::getenv(key);
    if (value == nullptr || value[0] == '\0') {
        return {};
    }
    return std::string(value);
}

std::string firstPresentString(const json& payload, std::initializer_list<const char*> keys) {
    if (!payload.is_object()) {
        return {};
    }
    for (const char* key : keys) {
        const auto it = payload.find(key);
        if (it != payload.end() && it->is_string()) {
            return it->get<std::string>();
        }
    }
    return {};
}

std::optional<std::uint64_t> asPositiveUInt64(const json& value) {
    if (value.is_number_unsigned()) {
        const auto number = value.get<std::uint64_t>();
        if (number > 0) {
            return number;
        }
        return std::nullopt;
    }
    if (value.is_number_integer()) {
        const auto number = value.get<long long>();
        if (number > 0) {
            return static_cast<std::uint64_t>(number);
        }
    }
    return std::nullopt;
}

std::optional<long long> asPositiveInt64(const json& value) {
    if (value.is_number_integer()) {
        const auto number = value.get<long long>();
        if (number > 0) {
            return number;
        }
    }
    if (value.is_number_unsigned()) {
        const auto number = value.get<std::uint64_t>();
        if (number > 0 && number <= static_cast<std::uint64_t>(std::numeric_limits<long long>::max())) {
            return static_cast<long long>(number);
        }
    }
    return std::nullopt;
}

std::optional<bool> asBoolean(const json& value) {
    if (!value.is_boolean()) {
        return std::nullopt;
    }
    return value.get<bool>();
}

std::optional<std::string> asNonEmptyString(const json& value) {
    if (!value.is_string()) {
        return std::nullopt;
    }
    const std::string text = value.get<std::string>();
    if (text.empty()) {
        return std::nullopt;
    }
    return text;
}

bool hasUnexpectedKeys(const json& payload, std::initializer_list<const char*> allowedKeys) {
    if (!payload.is_object()) {
        return true;
    }
    for (auto it = payload.begin(); it != payload.end(); ++it) {
        bool allowed = false;
        for (const char* key : allowedKeys) {
            if (it.key() == key) {
                allowed = true;
                break;
            }
        }
        if (!allowed) {
            return true;
        }
    }
    return false;
}

json revisionUnavailable() {
    return json{
        {"served_revision_id", nullptr},
        {"latest_session_seq", nullptr},
        {"first_session_seq", nullptr},
        {"last_session_seq", nullptr},
        {"manifest_hash", nullptr},
        {"includes_mutable_tail", false},
        {"source", "unavailable"}
    };
}

json revisionFromSummary(const json& summary) {
    if (!summary.is_object()) {
        return revisionUnavailable();
    }
    return json{
        {"served_revision_id", summary.contains("served_revision_id") ? summary.at("served_revision_id") : json(nullptr)},
        {"latest_session_seq", summary.contains("latest_session_seq") ? summary.at("latest_session_seq") : json(nullptr)},
        {"first_session_seq", summary.contains("first_session_seq") ? summary.at("first_session_seq") : json(nullptr)},
        {"last_session_seq", summary.contains("last_session_seq") ? summary.at("last_session_seq") : json(nullptr)},
        {"manifest_hash", summary.contains("manifest_hash")
                              ? summary.at("manifest_hash")
                              : summary.contains("last_manifest_hash")
                                  ? summary.at("last_manifest_hash")
                                  : json(nullptr)},
        {"includes_mutable_tail", summary.value("includes_mutable_tail", false)},
        {"source", "engine_payload"}
    };
}

json revisionFromStatus(const tapescope::StatusSnapshot& snapshot) {
    return json{
        {"served_revision_id", nullptr},
        {"latest_session_seq", snapshot.latestSessionSeq},
        {"first_session_seq", nullptr},
        {"last_session_seq", snapshot.latestSessionSeq == 0 ? json(nullptr) : json(snapshot.latestSessionSeq)},
        {"manifest_hash", snapshot.manifestHash.empty() ? json(nullptr) : json(snapshot.manifestHash)},
        {"includes_mutable_tail", false},
        {"source", "status_snapshot"}
    };
}

json eventRowToJson(const tapescope::EventRow& row) {
    return json{
        {"session_seq", row.sessionSeq},
        {"source_seq", row.sourceSeq},
        {"event_kind", row.eventKind},
        {"instrument_id", row.instrumentId},
        {"side", row.side},
        {"price", row.price.has_value() ? json(*row.price) : json(nullptr)},
        {"summary", row.summary}
    };
}

json incidentRowToJson(const tapescope::IncidentListRow& row) {
    return json{
        {"logical_incident_id", row.logicalIncidentId},
        {"kind", row.kind},
        {"score", row.score},
        {"title", row.title}
    };
}

json citationToJson(const tapescope::EvidenceCitation& citation) {
    return json{
        {"kind", citation.kind},
        {"artifact_id", citation.artifactId},
        {"label", citation.label}
    };
}

json replayRangeToJson(const std::optional<tapescope::RangeQuery>& replayRange) {
    if (!replayRange.has_value()) {
        return nullptr;
    }
    return json{
        {"first_session_seq", replayRange->firstSessionSeq},
        {"last_session_seq", replayRange->lastSessionSeq}
    };
}

json investigationResultFromPayload(const tapescope::InvestigationPayload& payload) {
    json events = json::array();
    for (const auto& rawEvent : payload.events) {
        events.push_back(eventRowToJson(tapescope::client_internal::parseEventRow(rawEvent)));
    }

    json incidentRows = json::array();
    for (const auto& incident : payload.incidents) {
        incidentRows.push_back(incidentRowToJson(incident));
    }

    json citations = json::array();
    for (const auto& citation : payload.evidence) {
        citations.push_back(citationToJson(citation));
    }

    return json{
        {"artifact_id", payload.artifactId},
        {"artifact_kind", payload.artifactKind},
        {"headline", payload.headline},
        {"detail", payload.detail},
        {"served_revision_id", payload.summary.contains("served_revision_id")
                                   ? payload.summary.at("served_revision_id")
                                   : json(nullptr)},
        {"includes_mutable_tail", payload.summary.value("includes_mutable_tail", false)},
        {"artifact", payload.summary.value("artifact", json::object())},
        {"entity", payload.summary.value("entity", json::object())},
        {"report", payload.summary.value("report", payload.summary.value("report_summary", json::object()))},
        {"evidence", payload.summary.value("evidence", json::object())},
        {"data_quality", payload.summary.value("data_quality", json::object())},
        {"replay_range", replayRangeToJson(payload.replayRange)},
        {"incident_rows", std::move(incidentRows)},
        {"citation_rows", std::move(citations)},
        {"events", std::move(events)}
    };
}

json qualityResultFromPayload(const tapescope::SessionQualityPayload& payload) {
    return json{
        {"served_revision_id", payload.summary.contains("served_revision_id")
                                   ? payload.summary.at("served_revision_id")
                                   : json(nullptr)},
        {"includes_mutable_tail", payload.summary.value("includes_mutable_tail", false)},
        {"first_session_seq", payload.summary.contains("first_session_seq")
                                  ? payload.summary.at("first_session_seq")
                                  : json(nullptr)},
        {"last_session_seq", payload.summary.contains("last_session_seq")
                                 ? payload.summary.at("last_session_seq")
                                 : json(nullptr)},
        {"data_quality", payload.dataQuality}
    };
}

json exportResultFromPayload(const tapescope::ArtifactExportPayload& payload) {
    return json{
        {"artifact_id", payload.artifactId},
        {"format", payload.format},
        {"served_revision_id", payload.servedRevisionId == 0 ? json(nullptr) : json(payload.servedRevisionId)},
        {"markdown", payload.format == "markdown" ? json(payload.markdown) : json(nullptr)},
        {"bundle", payload.format == "json-bundle" ? payload.bundle : json(nullptr)}
    };
}

json eventListResultFromPayload(const tapescope::EventListPayload& payload) {
    json events = json::array();
    for (const auto& row : payload.events) {
        events.push_back(eventRowToJson(row));
    }
    return json{
        {"returned_count", events.size()},
        {"events", std::move(events)}
    };
}

json incidentListResultFromPayload(const tapescope::IncidentListPayload& payload) {
    json incidents = json::array();
    for (const auto& row : payload.incidents) {
        incidents.push_back(incidentRowToJson(row));
    }
    return json{
        {"returned_count", incidents.size()},
        {"incidents", std::move(incidents)}
    };
}

json orderAnchorRowToJson(const json& row) {
    return json{
        {"artifact_id", row.value("artifact_id", std::string())},
        {"anchor_id", row.value("anchor_id", 0ULL)},
        {"revision_id", row.value("revision_id", 0ULL)},
        {"session_seq", row.value("session_seq", 0ULL)},
        {"event_kind", row.value("event_kind", std::string())},
        {"instrument_id", row.value("instrument_id", std::string())},
        {"anchor", row.value("anchor", json::object())}
    };
}

json protectedWindowRowToJson(const json& row) {
    return json{
        {"artifact_id", row.value("artifact_id", std::string())},
        {"window_id", row.value("window_id", 0ULL)},
        {"revision_id", row.value("revision_id", 0ULL)},
        {"logical_incident_id", row.value("logical_incident_id", 0ULL)},
        {"anchor_session_seq", row.value("anchor_session_seq", 0ULL)},
        {"first_session_seq", row.value("first_session_seq", 0ULL)},
        {"last_session_seq", row.value("last_session_seq", 0ULL)},
        {"reason", row.value("reason", std::string())},
        {"instrument_id", row.value("instrument_id", std::string())},
        {"anchor", row.value("anchor", json::object())}
    };
}

json findingRowToJson(const json& row) {
    return json{
        {"artifact_id", row.value("artifact_id", std::string())},
        {"finding_id", row.value("finding_id", 0ULL)},
        {"revision_id", row.value("revision_id", 0ULL)},
        {"logical_incident_id", row.value("logical_incident_id", 0ULL)},
        {"incident_revision_id", row.value("incident_revision_id", 0ULL)},
        {"kind", row.value("kind", std::string())},
        {"severity", row.value("severity", std::string())},
        {"confidence", row.value("confidence", 0.0)},
        {"first_session_seq", row.value("first_session_seq", 0ULL)},
        {"last_session_seq", row.value("last_session_seq", 0ULL)},
        {"title", row.value("title", std::string())},
        {"summary", row.value("summary", std::string())}
    };
}

json listRowsResultFromResponse(const tape_engine::QueryResponse& response,
                                const char* rowKey,
                                const std::function<json(const json&)>& projectRow) {
    json rows = json::array();
    for (const auto& row : response.events) {
        rows.push_back(projectRow(row));
    }
    return json{
        {"returned_count", rows.size()},
        {rowKey, std::move(rows)}
    };
}

json replaySnapshotResultFromResponse(const tape_engine::QueryResponse& response) {
    const json& summary = response.summary;
    return json{
        {"served_revision_id", summary.contains("served_revision_id") ? summary.at("served_revision_id") : json(nullptr)},
        {"includes_mutable_tail", summary.value("includes_mutable_tail", false)},
        {"target_session_seq", summary.value("target_session_seq", 0ULL)},
        {"replayed_through_session_seq", summary.value("replayed_through_session_seq", 0ULL)},
        {"applied_event_count", summary.value("applied_event_count", 0ULL)},
        {"gap_markers_encountered", summary.value("gap_markers_encountered", 0ULL)},
        {"checkpoint_used", summary.value("checkpoint_used", false)},
        {"checkpoint_revision_id", summary.value("checkpoint_revision_id", 0ULL)},
        {"checkpoint_session_seq", summary.value("checkpoint_session_seq", 0ULL)},
        {"bid_price", summary.contains("bid_price") ? summary.at("bid_price") : json(nullptr)},
        {"ask_price", summary.contains("ask_price") ? summary.at("ask_price") : json(nullptr)},
        {"last_price", summary.contains("last_price") ? summary.at("last_price") : json(nullptr)},
        {"bid_book", summary.value("bid_book", json::array())},
        {"ask_book", summary.value("ask_book", json::array())},
        {"data_quality", summary.value("data_quality", json::object())}
    };
}

json reportInventoryRowToJson(const tapescope::ReportInventoryRow& row) {
    return json{
        {"report_id", row.reportId},
        {"revision_id", row.revisionId},
        {"artifact_id", row.artifactId},
        {"report_type", row.reportType},
        {"headline", row.headline}
    };
}

json reportInventoryResultFromPayload(const tapescope::ReportInventoryPayload& payload, bool sessionReports) {
    json rows = json::array();
    const auto& source = sessionReports ? payload.sessionReports : payload.caseReports;
    for (const auto& row : source) {
        rows.push_back(reportInventoryRowToJson(row));
    }
    return json{
        {"returned_count", rows.size()},
        {sessionReports ? "session_reports" : "case_reports", std::move(rows)}
    };
}

json seekOrderResultFromPayload(const tapescope::SeekOrderPayload& payload) {
    return json{
        {"served_revision_id", payload.summary.contains("served_revision_id")
                                   ? payload.summary.at("served_revision_id")
                                   : json(nullptr)},
        {"includes_mutable_tail", payload.summary.value("includes_mutable_tail", false)},
        {"replay_target_session_seq", payload.replayTargetSessionSeq},
        {"first_session_seq", payload.firstSessionSeq},
        {"last_session_seq", payload.lastSessionSeq},
        {"last_fill_session_seq", payload.lastFillSessionSeq},
        {"replay_range", replayRangeToJson(payload.replayRange)}
    };
}

bool parseSessionWindowArgs(const json& args,
                            bool allowIncludeLiveTail,
                            bool allowLimit,
                            SessionWindowQuery* outQuery,
                            std::string* outCode,
                            std::string* outMessage) {
    auto fail = [&](std::string code, std::string message) {
        if (outCode != nullptr) {
            *outCode = std::move(code);
        }
        if (outMessage != nullptr) {
            *outMessage = std::move(message);
        }
        return false;
    };

    if (!args.is_object()) {
        return fail("invalid_arguments", "Tool arguments must be an object.");
    }
    if (hasUnexpectedKeys(args, allowIncludeLiveTail
                                  ? (allowLimit
                                         ? std::initializer_list<const char*>{"first_session_seq", "last_session_seq", "revision_id", "include_live_tail", "limit"}
                                         : std::initializer_list<const char*>{"first_session_seq", "last_session_seq", "revision_id", "include_live_tail"})
                                  : (allowLimit
                                         ? std::initializer_list<const char*>{"first_session_seq", "last_session_seq", "revision_id", "limit"}
                                         : std::initializer_list<const char*>{"first_session_seq", "last_session_seq", "revision_id"}))) {
        return fail("invalid_arguments", "Tool arguments include unsupported keys.");
    }

    const auto first = args.contains("first_session_seq") ? asPositiveUInt64(args.at("first_session_seq")) : std::nullopt;
    const auto last = args.contains("last_session_seq") ? asPositiveUInt64(args.at("last_session_seq")) : std::nullopt;
    if (!first.has_value() || !last.has_value()) {
        return fail("invalid_arguments",
                    "first_session_seq and last_session_seq are required positive integers.");
    }
    if (*last < *first) {
        return fail("invalid_arguments", "last_session_seq must be greater than or equal to first_session_seq.");
    }

    outQuery->firstSessionSeq = *first;
    outQuery->lastSessionSeq = *last;
    outQuery->revisionId = args.contains("revision_id")
        ? asPositiveUInt64(args.at("revision_id")).value_or(0)
        : 0;
    if (args.contains("revision_id") && outQuery->revisionId == 0) {
        return fail("invalid_arguments", "revision_id must be a positive integer.");
    }
    if (allowIncludeLiveTail && args.contains("include_live_tail")) {
        const auto includeLiveTail = asBoolean(args.at("include_live_tail"));
        if (!includeLiveTail.has_value()) {
            return fail("invalid_arguments", "include_live_tail must be a boolean.");
        }
        outQuery->includeLiveTail = *includeLiveTail;
    }
    if (allowLimit && args.contains("limit")) {
        const auto limit = asPositiveUInt64(args.at("limit"));
        if (!limit.has_value()) {
            return fail("invalid_arguments", "limit must be a positive integer.");
        }
        outQuery->limit = static_cast<std::size_t>(*limit);
    }
    return true;
}

bool parseListArgs(const json& args,
                   ListQuery* outQuery,
                   std::string* outCode,
                   std::string* outMessage) {
    auto fail = [&](std::string code, std::string message) {
        if (outCode != nullptr) {
            *outCode = std::move(code);
        }
        if (outMessage != nullptr) {
            *outMessage = std::move(message);
        }
        return false;
    };

    if (!args.is_object()) {
        return fail("invalid_arguments", "Tool arguments must be an object.");
    }
    if (hasUnexpectedKeys(args, {"revision_id", "limit"})) {
        return fail("invalid_arguments", "Tool arguments include unsupported keys.");
    }
    if (args.contains("revision_id")) {
        const auto revisionId = asPositiveUInt64(args.at("revision_id"));
        if (!revisionId.has_value()) {
            return fail("invalid_arguments", "revision_id must be a positive integer.");
        }
        outQuery->revisionId = *revisionId;
    }
    if (args.contains("limit")) {
        const auto limit = asPositiveUInt64(args.at("limit"));
        if (!limit.has_value()) {
            return fail("invalid_arguments", "limit must be a positive integer.");
        }
        outQuery->limit = static_cast<std::size_t>(*limit);
    }
    return true;
}

bool parseLimitOnlyArgs(const json& args,
                        std::size_t* outLimit,
                        std::string* outCode,
                        std::string* outMessage) {
    auto fail = [&](std::string code, std::string message) {
        if (outCode != nullptr) {
            *outCode = std::move(code);
        }
        if (outMessage != nullptr) {
            *outMessage = std::move(message);
        }
        return false;
    };

    if (!args.is_object()) {
        return fail("invalid_arguments", "Tool arguments must be an object.");
    }
    if (hasUnexpectedKeys(args, {"limit"})) {
        return fail("invalid_arguments", "Tool arguments include unsupported keys.");
    }
    if (args.contains("limit")) {
        const auto limit = asPositiveUInt64(args.at("limit"));
        if (!limit.has_value()) {
            return fail("invalid_arguments", "limit must be a positive integer.");
        }
        *outLimit = static_cast<std::size_t>(*limit);
    }
    return true;
}

bool parseNumericIdArgs(const json& args,
                        const char* fieldName,
                        NumericIdQuery* outQuery,
                        std::string* outCode,
                        std::string* outMessage,
                        bool allowRevision,
                        bool allowIncludeLiveTail,
                        bool allowLimit) {
    auto fail = [&](std::string code, std::string message) {
        if (outCode != nullptr) {
            *outCode = std::move(code);
        }
        if (outMessage != nullptr) {
            *outMessage = std::move(message);
        }
        return false;
    };

    if (!args.is_object()) {
        return fail("invalid_arguments", "Tool arguments must be an object.");
    }
    for (auto it = args.begin(); it != args.end(); ++it) {
        const std::string& key = it.key();
        const bool allowed = key == fieldName ||
            (allowRevision && key == "revision_id") ||
            (allowIncludeLiveTail && key == "include_live_tail") ||
            (allowLimit && key == "limit");
        if (!allowed) {
            return fail("invalid_arguments", "Tool arguments include unsupported keys.");
        }
    }

    if (!args.contains(fieldName)) {
        return fail("invalid_arguments", std::string(fieldName) + " is required and must be a positive integer.");
    }
    const auto id = asPositiveUInt64(args.at(fieldName));
    if (!id.has_value()) {
        return fail("invalid_arguments", std::string(fieldName) + " must be a positive integer.");
    }
    outQuery->id = *id;

    if (allowRevision && args.contains("revision_id")) {
        const auto revisionId = asPositiveUInt64(args.at("revision_id"));
        if (!revisionId.has_value()) {
            return fail("invalid_arguments", "revision_id must be a positive integer.");
        }
        outQuery->revisionId = *revisionId;
    }
    if (allowIncludeLiveTail && args.contains("include_live_tail")) {
        const auto includeLiveTail = asBoolean(args.at("include_live_tail"));
        if (!includeLiveTail.has_value()) {
            return fail("invalid_arguments", "include_live_tail must be a boolean.");
        }
        outQuery->includeLiveTail = *includeLiveTail;
    }
    if (allowLimit && args.contains("limit")) {
        const auto limit = asPositiveUInt64(args.at("limit"));
        if (!limit.has_value()) {
            return fail("invalid_arguments", "limit must be a positive integer.");
        }
        outQuery->limit = static_cast<std::size_t>(*limit);
    }
    return true;
}

bool parseReplaySnapshotArgs(const json& args,
                             ReplaySnapshotQuery* outQuery,
                             std::string* outCode,
                             std::string* outMessage) {
    auto fail = [&](std::string code, std::string message) {
        if (outCode != nullptr) {
            *outCode = std::move(code);
        }
        if (outMessage != nullptr) {
            *outMessage = std::move(message);
        }
        return false;
    };

    if (!args.is_object()) {
        return fail("invalid_arguments", "Tool arguments must be an object.");
    }
    if (hasUnexpectedKeys(args, {"target_session_seq", "revision_id", "include_live_tail", "depth_limit"})) {
        return fail("invalid_arguments", "Tool arguments include unsupported keys.");
    }
    if (!args.contains("target_session_seq")) {
        return fail("invalid_arguments", "target_session_seq is required.");
    }
    const auto target = asPositiveUInt64(args.at("target_session_seq"));
    if (!target.has_value()) {
        return fail("invalid_arguments", "target_session_seq must be a positive integer.");
    }
    outQuery->targetSessionSeq = *target;
    if (args.contains("revision_id")) {
        const auto revisionId = asPositiveUInt64(args.at("revision_id"));
        if (!revisionId.has_value()) {
            return fail("invalid_arguments", "revision_id must be a positive integer.");
        }
        outQuery->revisionId = *revisionId;
    }
    if (args.contains("include_live_tail")) {
        const auto includeLiveTail = asBoolean(args.at("include_live_tail"));
        if (!includeLiveTail.has_value()) {
            return fail("invalid_arguments", "include_live_tail must be a boolean.");
        }
        outQuery->includeLiveTail = *includeLiveTail;
    }
    if (args.contains("depth_limit")) {
        const auto depthLimit = asPositiveUInt64(args.at("depth_limit"));
        if (!depthLimit.has_value()) {
            return fail("invalid_arguments", "depth_limit must be a positive integer.");
        }
        outQuery->depthLimit = static_cast<std::size_t>(*depthLimit);
    }
    return true;
}

bool parseOrderCaseArgs(const json& args,
                        OrderCaseQuery* outQuery,
                        std::string* outCode,
                        std::string* outMessage) {
    auto fail = [&](std::string code, std::string message) {
        if (outCode != nullptr) {
            *outCode = std::move(code);
        }
        if (outMessage != nullptr) {
            *outMessage = std::move(message);
        }
        return false;
    };

    if (!args.is_object()) {
        return fail("invalid_arguments", "Tool arguments must be an object.");
    }
    if (hasUnexpectedKeys(args, {"trace_id", "order_id", "perm_id", "exec_id", "revision_id", "include_live_tail", "limit"})) {
        return fail("invalid_arguments", "Tool arguments include unsupported keys.");
    }

    int anchors = 0;
    if (args.contains("trace_id")) {
        const auto traceId = asPositiveUInt64(args.at("trace_id"));
        if (!traceId.has_value()) {
            return fail("invalid_arguments", "trace_id must be a positive integer.");
        }
        outQuery->traceId = *traceId;
        ++anchors;
    }
    if (args.contains("order_id")) {
        const auto orderId = asPositiveInt64(args.at("order_id"));
        if (!orderId.has_value()) {
            return fail("invalid_arguments", "order_id must be a positive integer.");
        }
        outQuery->orderId = *orderId;
        ++anchors;
    }
    if (args.contains("perm_id")) {
        const auto permId = asPositiveInt64(args.at("perm_id"));
        if (!permId.has_value()) {
            return fail("invalid_arguments", "perm_id must be a positive integer.");
        }
        outQuery->permId = *permId;
        ++anchors;
    }
    if (args.contains("exec_id")) {
        const auto execId = asNonEmptyString(args.at("exec_id"));
        if (!execId.has_value()) {
            return fail("invalid_arguments", "exec_id must be a non-empty string.");
        }
        outQuery->execId = *execId;
        ++anchors;
    }
    if (anchors != 1) {
        return fail("invalid_arguments", "Exactly one of trace_id, order_id, perm_id, or exec_id is required.");
    }

    if (args.contains("revision_id")) {
        const auto revisionId = asPositiveUInt64(args.at("revision_id"));
        if (!revisionId.has_value()) {
            return fail("invalid_arguments", "revision_id must be a positive integer.");
        }
        outQuery->revisionId = *revisionId;
    }
    if (args.contains("include_live_tail")) {
        const auto includeLiveTail = asBoolean(args.at("include_live_tail"));
        if (!includeLiveTail.has_value()) {
            return fail("invalid_arguments", "include_live_tail must be a boolean.");
        }
        outQuery->includeLiveTail = *includeLiveTail;
    }
    if (args.contains("limit")) {
        const auto limit = asPositiveUInt64(args.at("limit"));
        if (!limit.has_value()) {
            return fail("invalid_arguments", "limit must be a positive integer.");
        }
        outQuery->limit = static_cast<std::size_t>(*limit);
    }
    return true;
}

bool parseIncidentArgs(const json& args,
                       IncidentQuery* outQuery,
                       std::string* outCode,
                       std::string* outMessage) {
    auto fail = [&](std::string code, std::string message) {
        if (outCode != nullptr) {
            *outCode = std::move(code);
        }
        if (outMessage != nullptr) {
            *outMessage = std::move(message);
        }
        return false;
    };

    if (!args.is_object()) {
        return fail("invalid_arguments", "Tool arguments must be an object.");
    }
    if (hasUnexpectedKeys(args, {"logical_incident_id", "revision_id", "include_live_tail", "limit"})) {
        return fail("invalid_arguments", "Tool arguments include unsupported keys.");
    }
    const auto logicalIncidentId = args.contains("logical_incident_id")
        ? asPositiveUInt64(args.at("logical_incident_id"))
        : std::nullopt;
    if (!logicalIncidentId.has_value()) {
        return fail("invalid_arguments", "logical_incident_id is required and must be a positive integer.");
    }
    outQuery->logicalIncidentId = *logicalIncidentId;
    if (args.contains("revision_id")) {
        const auto revisionId = asPositiveUInt64(args.at("revision_id"));
        if (!revisionId.has_value()) {
            return fail("invalid_arguments", "revision_id must be a positive integer.");
        }
        outQuery->revisionId = *revisionId;
    }
    if (args.contains("include_live_tail")) {
        const auto includeLiveTail = asBoolean(args.at("include_live_tail"));
        if (!includeLiveTail.has_value()) {
            return fail("invalid_arguments", "include_live_tail must be a boolean.");
        }
        outQuery->includeLiveTail = *includeLiveTail;
    }
    if (args.contains("limit")) {
        const auto limit = asPositiveUInt64(args.at("limit"));
        if (!limit.has_value()) {
            return fail("invalid_arguments", "limit must be a positive integer.");
        }
        outQuery->limit = static_cast<std::size_t>(*limit);
    }
    return true;
}

bool parseArtifactArgs(const json& args,
                       ArtifactQuery* outQuery,
                       std::string* outCode,
                       std::string* outMessage) {
    auto fail = [&](std::string code, std::string message) {
        if (outCode != nullptr) {
            *outCode = std::move(code);
        }
        if (outMessage != nullptr) {
            *outMessage = std::move(message);
        }
        return false;
    };

    if (!args.is_object()) {
        return fail("invalid_arguments", "Tool arguments must be an object.");
    }
    if (hasUnexpectedKeys(args, {"artifact_id", "revision_id", "include_live_tail", "limit"})) {
        return fail("invalid_arguments", "Tool arguments include unsupported keys.");
    }
    const auto artifactId = args.contains("artifact_id") ? asNonEmptyString(args.at("artifact_id")) : std::nullopt;
    if (!artifactId.has_value()) {
        return fail("invalid_arguments", "artifact_id is required and must be a non-empty string.");
    }
    outQuery->artifactId = *artifactId;
    if (args.contains("revision_id")) {
        const auto revisionId = asPositiveUInt64(args.at("revision_id"));
        if (!revisionId.has_value()) {
            return fail("invalid_arguments", "revision_id must be a positive integer.");
        }
        outQuery->revisionId = *revisionId;
    }
    if (args.contains("include_live_tail")) {
        const auto includeLiveTail = asBoolean(args.at("include_live_tail"));
        if (!includeLiveTail.has_value()) {
            return fail("invalid_arguments", "include_live_tail must be a boolean.");
        }
        outQuery->includeLiveTail = *includeLiveTail;
    }
    if (args.contains("limit")) {
        const auto limit = asPositiveUInt64(args.at("limit"));
        if (!limit.has_value()) {
            return fail("invalid_arguments", "limit must be a positive integer.");
        }
        outQuery->limit = static_cast<std::size_t>(*limit);
    }
    return true;
}

bool parseExportArgs(const json& args,
                     ArtifactExportQuery* outQuery,
                     std::string* outCode,
                     std::string* outMessage) {
    auto fail = [&](std::string code, std::string message) {
        if (outCode != nullptr) {
            *outCode = std::move(code);
        }
        if (outMessage != nullptr) {
            *outMessage = std::move(message);
        }
        return false;
    };

    ArtifactQuery artifactQuery;
    json artifactArgs = args;
    if (artifactArgs.is_object()) {
        artifactArgs.erase("export_format");
    }
    if (!parseArtifactArgs(artifactArgs, &artifactQuery, outCode, outMessage)) {
        return false;
    }
    outQuery->artifactId = artifactQuery.artifactId;
    outQuery->revisionId = artifactQuery.revisionId;
    outQuery->limit = artifactQuery.limit;
    outQuery->includeLiveTail = artifactQuery.includeLiveTail;
    if (!args.contains("export_format")) {
        return fail("invalid_arguments", "export_format is required and must be markdown or json-bundle.");
    }
    const auto exportFormat = asNonEmptyString(args.at("export_format"));
    if (!exportFormat.has_value() || (*exportFormat != "markdown" && *exportFormat != "json-bundle")) {
        return fail("invalid_arguments", "export_format must be markdown or json-bundle.");
    }
    outQuery->exportFormat = *exportFormat;
    return true;
}

std::string envelopeText(const json& envelope) {
    return envelope.dump(2);
}

} // namespace

ParsedAdapterArgs parseAdapterArgs(int argc, char** argv) {
    ParsedAdapterArgs parsed;
    parsed.config.engineSocketPath = envValueOrEmpty("TAPE_MCP_ENGINE_SOCKET");
    if (parsed.config.engineSocketPath.empty()) {
        parsed.config.engineSocketPath = envValueOrEmpty("LONG_TAPE_ENGINE_SOCKET");
    }

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--help" || arg == "-h") {
            parsed.showHelp = true;
            continue;
        }
        if (arg == "--engine-socket" && i + 1 < argc) {
            parsed.config.engineSocketPath = argv[++i];
            continue;
        }
        parsed.error = "unknown argument: " + arg;
        return parsed;
    }
    return parsed;
}

std::string adapterUsage(std::string_view executableName) {
    std::string usage = "Usage: ";
    usage += std::string(executableName);
    usage += " [--engine-socket PATH]\n";
    usage += "Environment: TAPE_MCP_ENGINE_SOCKET, LONG_TAPE_ENGINE_SOCKET\n";
    return usage;
}

Adapter::Adapter(AdapterConfig config)
    : tools_(buildToolSpecs()),
      engineRpc_(EngineRpcClientConfig{std::move(config.engineSocketPath)}) {}

json Adapter::initializeResult() const {
    return json{
        {"protocolVersion", kProtocolVersion},
        {"capabilities", {
            {"tools", {
                {"listChanged", false}
            }},
            {"resources", {
                {"subscribe", false},
                {"listChanged", false}
            }},
            {"prompts", {
                {"listChanged", false}
            }}
        }},
        {"serverInfo", {
            {"name", "tape-mcp"},
            {"version", kServerVersion}
        }}
    };
}

json Adapter::listToolsResult() const {
    json tools = json::array();
    for (const auto& tool : tools_) {
        json inputSchema = toolInputSchemaForList(tool);
        tools.push_back(json{
            {"name", tool.name},
            {"title", toolTitle(tool)},
            {"description", tool.description},
            {"inputSchema", std::move(inputSchema)},
            {"outputSchema", tool.outputSchema},
            {"annotations", toolAnnotationsForSpec(tool)},
            {"progressHint", tool.progressEligible}
        });
    }
    return json{{"tools", std::move(tools)}};
}

json Adapter::listPromptsResult() const {
    const auto prompts = buildPromptSpecs();
    json result = json::array();
    for (const auto& prompt : prompts) {
        result.push_back({
            {"name", prompt.name},
            {"title", prompt.title},
            {"description", prompt.description},
            {"arguments", promptArgumentList(prompt)}
        });
    }
    return json{{"prompts", std::move(result)}};
}

json Adapter::getPromptResult(const std::string& promptName, const json& args) const {
    const auto prompts = buildPromptSpecs();
    const PromptSpec* prompt = findPromptSpec(prompts, promptName);
    if (prompt == nullptr) {
        return json{
            {"description", "Unknown prompt."},
            {"meta", {
                {"ok", false},
                {"code", "unsupported_prompt"},
                {"prompt", promptName},
                {"contract_version", kPromptVersion}
            }},
            {"messages", json::array({
                json{
                    {"role", "user"},
                    {"content", {
                        {"type", "text"},
                        {"text", "Unknown prompt `" + promptName + "`. Call `prompts/list` to discover supported investigation prompts."}
                    }}
                }
            })}
        };
    }
    return json{
        {"description", prompt->description},
        {"meta", {
            {"ok", true},
            {"prompt", prompt->name},
            {"prompt_title", prompt->title},
            {"contract_version", kPromptVersion}
        }},
        {"messages", json::array({
            json{
                {"role", "user"},
                {"content", {
                    {"type", "text"},
                    {"text", promptMessageForPrompt(*prompt, args)}
                }}
            }
        })}
    };
}

json Adapter::listResourcesResult() const {
    const auto sessionReports = engineRpc_.listSessionReports(ListQuery{.limit = 50});
    if (!sessionReports.ok()) {
        return json{
            {"resources", json::array()},
            {"meta", resourceErrorMeta(sessionReports.error.code, sessionReports.error.message)}
        };
    }
    const auto caseReports = engineRpc_.listCaseReports(ListQuery{.limit = 50});
    if (!caseReports.ok()) {
        return json{
            {"resources", json::array()},
            {"meta", resourceErrorMeta(caseReports.error.code, caseReports.error.message)}
        };
    }

    json resources = json::array();
    for (const auto& report : sessionReports.value.sessionReports) {
        resources.push_back({
            {"uri", sessionReportUri(report.reportId)},
            {"name", "session-report:" + std::to_string(report.reportId)},
            {"title", "Session report: " + report.headline},
            {"description", "Durable session report JSON view."},
            {"mimeType", "application/json"}
        });
        resources.push_back({
            {"uri", sessionArtifactUri(report.reportId, "markdown")},
            {"name", "session-report:" + std::to_string(report.reportId) + ":markdown"},
            {"title", "Session report export: " + report.headline},
            {"description", "Durable session report markdown export."},
            {"mimeType", "text/markdown"}
        });
        resources.push_back({
            {"uri", sessionArtifactUri(report.reportId, "json-bundle")},
            {"name", "session-report:" + std::to_string(report.reportId) + ":json-bundle"},
            {"title", "Session report bundle: " + report.headline},
            {"description", "Durable session report JSON bundle export."},
            {"mimeType", "application/json"}
        });
    }
    for (const auto& report : caseReports.value.caseReports) {
        resources.push_back({
            {"uri", caseReportUri(report.reportId)},
            {"name", "case-report:" + std::to_string(report.reportId)},
            {"title", "Case report: " + report.headline},
            {"description", "Durable incident/order-case report JSON view."},
            {"mimeType", "application/json"}
        });
        resources.push_back({
            {"uri", caseArtifactUri(report.reportId, "markdown")},
            {"name", "case-report:" + std::to_string(report.reportId) + ":markdown"},
            {"title", "Case report export: " + report.headline},
            {"description", "Durable case report markdown export."},
            {"mimeType", "text/markdown"}
        });
        resources.push_back({
            {"uri", caseArtifactUri(report.reportId, "json-bundle")},
            {"name", "case-report:" + std::to_string(report.reportId) + ":json-bundle"},
            {"title", "Case report bundle: " + report.headline},
            {"description", "Durable case report JSON bundle export."},
            {"mimeType", "application/json"}
        });
    }

    std::sort(resources.begin(), resources.end(), [](const json& left, const json& right) {
        return left.value("uri", std::string()) < right.value("uri", std::string());
    });
    return json{
        {"resources", std::move(resources)},
        {"meta", resourceOkMeta()}
    };
}

json Adapter::readResourceResult(const std::string& resourceUri) const {
    const ParsedResourceUri parsed = parseResourceUri(resourceUri);
    if (parsed.kind == ResourceKind::Unknown || parsed.reportId == 0) {
        return json{
            {"contents", json::array()},
            {"meta", resourceErrorMeta("unsupported_resource", "Unsupported resource URI.")}
        };
    }

    auto jsonContents = [&](const std::string& uri, const json& payload) {
        return json{
            {"contents", json::array({
                json{
                    {"uri", uri},
                    {"mimeType", "application/json"},
                    {"text", payload.dump(2)}
                }
            })},
            {"meta", resourceOkMeta()}
        };
    };

    auto textContents = [&](const std::string& uri, const std::string& text, const char* mimeType) {
        return json{
            {"contents", json::array({
                json{
                    {"uri", uri},
                    {"mimeType", mimeType},
                    {"text", text}
                }
            })},
            {"meta", resourceOkMeta()}
        };
    };

    switch (parsed.kind) {
        case ResourceKind::SessionReportJson: {
            const auto result = engineRpc_.readSessionReport(NumericIdQuery{.id = parsed.reportId});
            if (!result.ok()) {
                return json{{"contents", json::array()}, {"meta", resourceErrorMeta(result.error.code, result.error.message)}};
            }
            return jsonContents(resourceUri, investigationResultFromPayload(result.value));
        }
        case ResourceKind::CaseReportJson: {
            const auto result = engineRpc_.readCaseReport(NumericIdQuery{.id = parsed.reportId});
            if (!result.ok()) {
                return json{{"contents", json::array()}, {"meta", resourceErrorMeta(result.error.code, result.error.message)}};
            }
            return jsonContents(resourceUri, investigationResultFromPayload(result.value));
        }
        case ResourceKind::SessionArtifactMarkdown:
        case ResourceKind::SessionArtifactJsonBundle: {
            ArtifactExportQuery query;
            query.artifactId = "session-report:" + std::to_string(parsed.reportId);
            query.exportFormat = parsed.kind == ResourceKind::SessionArtifactMarkdown ? "markdown" : "json-bundle";
            const auto result = engineRpc_.exportArtifact(query);
            if (!result.ok()) {
                return json{{"contents", json::array()}, {"meta", resourceErrorMeta(result.error.code, result.error.message)}};
            }
            if (query.exportFormat == "markdown") {
                return textContents(resourceUri, result.value.markdown, "text/markdown");
            }
            return jsonContents(resourceUri, result.value.bundle);
        }
        case ResourceKind::CaseArtifactMarkdown:
        case ResourceKind::CaseArtifactJsonBundle: {
            ArtifactExportQuery query;
            query.artifactId = "case-report:" + std::to_string(parsed.reportId);
            query.exportFormat = parsed.kind == ResourceKind::CaseArtifactMarkdown ? "markdown" : "json-bundle";
            const auto result = engineRpc_.exportArtifact(query);
            if (!result.ok()) {
                return json{{"contents", json::array()}, {"meta", resourceErrorMeta(result.error.code, result.error.message)}};
            }
            if (query.exportFormat == "markdown") {
                return textContents(resourceUri, result.value.markdown, "text/markdown");
            }
            return jsonContents(resourceUri, result.value.bundle);
        }
        case ResourceKind::Unknown:
            break;
    }
    return json{
        {"contents", json::array()},
        {"meta", resourceErrorMeta("unsupported_resource", "Unsupported resource URI.")}
    };
}

bool Adapter::isProgressEligibleTool(std::string_view toolName) const {
    const ToolSpec* tool = findTool(toolName);
    return tool != nullptr && tool->progressEligible;
}

const ToolSpec* Adapter::findTool(std::string_view toolName) const {
    for (const auto& tool : tools_) {
        if (tool.name == toolName) {
            return &tool;
        }
    }
    return nullptr;
}

json Adapter::callTool(const std::string& toolName, const json& args) const {
    const ToolSpec* tool = findTool(toolName);
    if (tool == nullptr) {
        return makeToolResult(makeErrorEnvelope(
            toolName,
            "",
            "",
            false,
            false,
            "unsupported_tool",
            "Tool is not registered in this tape-mcp slice.",
            false,
            revisionUnavailable()));
    }
    return invokeTool(*tool, args);
}

json Adapter::invokeTool(const ToolSpec& tool, const json& args) const {
    switch (tool.id) {
        case ToolId::Status:
            return invokeStatusTool(tool, args);
        case ToolId::ReadLiveTail:
            return invokeReadLiveTailTool(tool, args);
        case ToolId::ReadRange:
            return invokeReadRangeTool(tool, args);
        case ToolId::FindOrderAnchor:
            return invokeFindOrderAnchorTool(tool, args);
        case ToolId::ListIncidents:
            return invokeListIncidentsTool(tool, args);
        case ToolId::ListOrderAnchors:
            return invokeListOrderAnchorsTool(tool, args);
        case ToolId::ListProtectedWindows:
            return invokeListProtectedWindowsTool(tool, args);
        case ToolId::ListFindings:
            return invokeListFindingsTool(tool, args);
        case ToolId::ReadSessionOverview:
            return invokeReadSessionOverviewTool(tool, args);
        case ToolId::ScanSessionReport:
            return invokeScanSessionReportTool(tool, args);
        case ToolId::ReadSessionReport:
            return invokeReadSessionReportTool(tool, args);
        case ToolId::ListSessionReports:
            return invokeListSessionReportsTool(tool, args);
        case ToolId::ScanIncidentReport:
            return invokeScanIncidentReportTool(tool, args);
        case ToolId::ScanOrderCaseReport:
            return invokeScanOrderCaseReportTool(tool, args);
        case ToolId::ReadCaseReport:
            return invokeReadCaseReportTool(tool, args);
        case ToolId::ListCaseReports:
            return invokeListCaseReportsTool(tool, args);
        case ToolId::SeekOrderAnchor:
            return invokeSeekOrderAnchorTool(tool, args);
        case ToolId::ReadFinding:
            return invokeReadFindingTool(tool, args);
        case ToolId::ReadOrderCase:
            return invokeReadOrderCaseTool(tool, args);
        case ToolId::ReadOrderAnchor:
            return invokeReadOrderAnchorTool(tool, args);
        case ToolId::ReadProtectedWindow:
            return invokeReadProtectedWindowTool(tool, args);
        case ToolId::ReplaySnapshot:
            return invokeReplaySnapshotTool(tool, args);
        case ToolId::ReadIncident:
            return invokeReadIncidentTool(tool, args);
        case ToolId::ReadArtifact:
            return invokeReadArtifactTool(tool, args);
        case ToolId::ExportArtifact:
            return invokeExportArtifactTool(tool, args);
        case ToolId::ReadSessionQuality:
            return invokeReadSessionQualityTool(tool, args);
    }
    return makeToolResult(makeErrorEnvelope(
        tool.name,
        tape_engine::queryOperationName(tool.engineOperation),
        tool.outputSchemaId,
        true,
        false,
        "unsupported_tool",
        "Tool is registered but has no executor.",
        false,
        revisionUnavailable()));
}

json Adapter::invokeStatusTool(const ToolSpec& tool, const json& args) const {
    if (!args.is_object() || !args.empty()) {
        return makeToolResult(makeErrorEnvelope(
            tool.name,
            tape_engine::queryOperationName(tool.engineOperation),
            tool.outputSchemaId,
            true,
            false,
            "invalid_arguments",
            "tapescript_status does not accept arguments.",
            false,
            revisionUnavailable()));
    }
    const auto result = engineRpc_.status();
    if (!result.ok()) {
        return makeToolResult(makeErrorEnvelope(
            tool.name,
            tape_engine::queryOperationName(tool.engineOperation),
            tool.outputSchemaId,
            true,
            false,
            result.error.code,
            result.error.message,
            result.error.retryable,
            revisionUnavailable()));
    }
    return makeToolResult(makeSuccessEnvelope(
        tool,
        json{
            {"socket_path", result.value.socketPath},
            {"data_dir", result.value.dataDir},
            {"instrument_id", result.value.instrumentId},
            {"latest_session_seq", result.value.latestSessionSeq},
            {"live_event_count", result.value.liveEventCount},
            {"segment_count", result.value.segmentCount},
            {"manifest_hash", result.value.manifestHash}
        },
        revisionFromStatus(result.value)));
}

json Adapter::invokeReadLiveTailTool(const ToolSpec& tool, const json& args) const {
    std::size_t limit = 0;
    std::string code;
    std::string message;
    if (!parseLimitOnlyArgs(args, &limit, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable()));
    }
    const auto result = engineRpc_.readLiveTail(limit == 0 ? 64 : limit);
    if (!result.ok()) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                result.error.code,
                                                result.error.message,
                                                result.error.retryable,
                                                revisionUnavailable()));
    }
    return makeToolResult(makeSuccessEnvelope(tool,
                                              eventListResultFromPayload(result.value),
                                              revisionFromSummary(result.value.summary)));
}

json Adapter::invokeReadRangeTool(const ToolSpec& tool, const json& args) const {
    SessionWindowQuery query;
    std::string code;
    std::string message;
    if (!parseSessionWindowArgs(args, false, false, &query, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable()));
    }
    const auto result = engineRpc_.readRange(query);
    if (!result.ok()) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                result.error.code,
                                                result.error.message,
                                                result.error.retryable,
                                                revisionUnavailable()));
    }
    return makeToolResult(makeSuccessEnvelope(tool,
                                              eventListResultFromPayload(result.value),
                                              revisionFromSummary(result.value.summary)));
}

json Adapter::invokeFindOrderAnchorTool(const ToolSpec& tool, const json& args) const {
    OrderCaseQuery query;
    std::string code;
    std::string message;
    if (!parseOrderCaseArgs(args, &query, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable()));
    }
    const auto result = engineRpc_.findOrderAnchor(query);
    if (!result.ok()) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                result.error.code,
                                                result.error.message,
                                                result.error.retryable,
                                                revisionUnavailable()));
    }
    return makeToolResult(makeSuccessEnvelope(tool,
                                              eventListResultFromPayload(result.value),
                                              revisionFromSummary(result.value.summary)));
}

json Adapter::invokeListIncidentsTool(const ToolSpec& tool, const json& args) const {
    ListQuery query;
    std::string code;
    std::string message;
    if (!parseListArgs(args, &query, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable()));
    }
    const auto result = engineRpc_.listIncidents(query);
    if (!result.ok()) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                result.error.code,
                                                result.error.message,
                                                result.error.retryable,
                                                revisionUnavailable()));
    }
    return makeToolResult(makeSuccessEnvelope(tool,
                                              incidentListResultFromPayload(result.value),
                                              revisionUnavailable()));
}

json Adapter::invokeListOrderAnchorsTool(const ToolSpec& tool, const json& args) const {
    ListQuery query;
    std::string code;
    std::string message;
    if (!parseListArgs(args, &query, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable()));
    }
    const auto result = engineRpc_.listOrderAnchors(query);
    if (!result.ok()) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                result.error.code,
                                                result.error.message,
                                                result.error.retryable,
                                                revisionUnavailable()));
    }
    return makeToolResult(makeSuccessEnvelope(tool,
                                              listRowsResultFromResponse(result.value,
                                                                         "order_anchors",
                                                                         orderAnchorRowToJson),
                                              revisionFromSummary(result.value.summary)));
}

json Adapter::invokeListProtectedWindowsTool(const ToolSpec& tool, const json& args) const {
    ListQuery query;
    std::string code;
    std::string message;
    if (!parseListArgs(args, &query, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable()));
    }
    const auto result = engineRpc_.listProtectedWindows(query);
    if (!result.ok()) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                result.error.code,
                                                result.error.message,
                                                result.error.retryable,
                                                revisionUnavailable()));
    }
    return makeToolResult(makeSuccessEnvelope(tool,
                                              listRowsResultFromResponse(result.value,
                                                                         "protected_windows",
                                                                         protectedWindowRowToJson),
                                              revisionFromSummary(result.value.summary)));
}

json Adapter::invokeListFindingsTool(const ToolSpec& tool, const json& args) const {
    ListQuery query;
    std::string code;
    std::string message;
    if (!parseListArgs(args, &query, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable()));
    }
    const auto result = engineRpc_.listFindings(query);
    if (!result.ok()) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                result.error.code,
                                                result.error.message,
                                                result.error.retryable,
                                                revisionUnavailable()));
    }
    return makeToolResult(makeSuccessEnvelope(tool,
                                              listRowsResultFromResponse(result.value,
                                                                         "findings",
                                                                         findingRowToJson),
                                              revisionFromSummary(result.value.summary)));
}

json Adapter::invokeReadSessionOverviewTool(const ToolSpec& tool, const json& args) const {
    SessionWindowQuery query;
    std::string code;
    std::string message;
    if (!parseSessionWindowArgs(args, true, true, &query, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable()));
    }
    const auto result = engineRpc_.readSessionOverview(query);
    if (!result.ok()) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                result.error.code,
                                                result.error.message,
                                                result.error.retryable,
                                                revisionUnavailable()));
    }
    return makeToolResult(makeSuccessEnvelope(tool,
                                              investigationResultFromPayload(result.value),
                                              revisionFromSummary(result.value.summary)));
}

json Adapter::invokeScanSessionReportTool(const ToolSpec& tool, const json& args) const {
    SessionWindowQuery query;
    std::string code;
    std::string message;
    if (!parseSessionWindowArgs(args, false, true, &query, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable()));
    }
    const auto result = engineRpc_.scanSessionReport(query);
    if (!result.ok()) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                result.error.code,
                                                result.error.message,
                                                result.error.retryable,
                                                revisionUnavailable()));
    }
    return makeToolResult(makeSuccessEnvelope(tool,
                                              investigationResultFromPayload(result.value),
                                              revisionFromSummary(result.value.summary)));
}

json Adapter::invokeReadSessionReportTool(const ToolSpec& tool, const json& args) const {
    NumericIdQuery query;
    std::string code;
    std::string message;
    if (!parseNumericIdArgs(args, "report_id", &query, &code, &message, true, false, false)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable()));
    }
    const auto result = engineRpc_.readSessionReport(query);
    if (!result.ok()) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                result.error.code,
                                                result.error.message,
                                                result.error.retryable,
                                                revisionUnavailable()));
    }
    return makeToolResult(makeSuccessEnvelope(tool,
                                              investigationResultFromPayload(result.value),
                                              revisionFromSummary(result.value.summary)));
}

json Adapter::invokeListSessionReportsTool(const ToolSpec& tool, const json& args) const {
    ListQuery query;
    std::string code;
    std::string message;
    if (!parseListArgs(args, &query, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable()));
    }
    const auto result = engineRpc_.listSessionReports(query);
    if (!result.ok()) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                result.error.code,
                                                result.error.message,
                                                result.error.retryable,
                                                revisionUnavailable()));
    }
    return makeToolResult(makeSuccessEnvelope(tool,
                                              reportInventoryResultFromPayload(result.value, true),
                                              revisionUnavailable()));
}

json Adapter::invokeScanIncidentReportTool(const ToolSpec& tool, const json& args) const {
    IncidentQuery query;
    std::string code;
    std::string message;
    if (!parseIncidentArgs(args, &query, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable()));
    }
    const auto result = engineRpc_.scanIncidentReport(query);
    if (!result.ok()) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                result.error.code,
                                                result.error.message,
                                                result.error.retryable,
                                                revisionUnavailable()));
    }
    return makeToolResult(makeSuccessEnvelope(tool,
                                              investigationResultFromPayload(result.value),
                                              revisionFromSummary(result.value.summary)));
}

json Adapter::invokeScanOrderCaseReportTool(const ToolSpec& tool, const json& args) const {
    OrderCaseQuery query;
    std::string code;
    std::string message;
    if (!parseOrderCaseArgs(args, &query, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable()));
    }
    const auto result = engineRpc_.scanOrderCaseReport(query);
    if (!result.ok()) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                result.error.code,
                                                result.error.message,
                                                result.error.retryable,
                                                revisionUnavailable()));
    }
    return makeToolResult(makeSuccessEnvelope(tool,
                                              investigationResultFromPayload(result.value),
                                              revisionFromSummary(result.value.summary)));
}

json Adapter::invokeReadCaseReportTool(const ToolSpec& tool, const json& args) const {
    NumericIdQuery query;
    std::string code;
    std::string message;
    if (!parseNumericIdArgs(args, "report_id", &query, &code, &message, true, false, false)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable()));
    }
    const auto result = engineRpc_.readCaseReport(query);
    if (!result.ok()) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                result.error.code,
                                                result.error.message,
                                                result.error.retryable,
                                                revisionUnavailable()));
    }
    return makeToolResult(makeSuccessEnvelope(tool,
                                              investigationResultFromPayload(result.value),
                                              revisionFromSummary(result.value.summary)));
}

json Adapter::invokeListCaseReportsTool(const ToolSpec& tool, const json& args) const {
    ListQuery query;
    std::string code;
    std::string message;
    if (!parseListArgs(args, &query, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable()));
    }
    const auto result = engineRpc_.listCaseReports(query);
    if (!result.ok()) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                result.error.code,
                                                result.error.message,
                                                result.error.retryable,
                                                revisionUnavailable()));
    }
    return makeToolResult(makeSuccessEnvelope(tool,
                                              reportInventoryResultFromPayload(result.value, false),
                                              revisionUnavailable()));
}

json Adapter::invokeSeekOrderAnchorTool(const ToolSpec& tool, const json& args) const {
    OrderCaseQuery query;
    std::string code;
    std::string message;
    if (!parseOrderCaseArgs(args, &query, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable()));
    }
    const auto result = engineRpc_.seekOrderAnchor(query);
    if (!result.ok()) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                result.error.code,
                                                result.error.message,
                                                result.error.retryable,
                                                revisionUnavailable()));
    }
    return makeToolResult(makeSuccessEnvelope(tool,
                                              seekOrderResultFromPayload(result.value),
                                              revisionFromSummary(result.value.summary)));
}

json Adapter::invokeReadFindingTool(const ToolSpec& tool, const json& args) const {
    NumericIdQuery query;
    std::string code;
    std::string message;
    if (!parseNumericIdArgs(args, "finding_id", &query, &code, &message, true, true, true)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable()));
    }
    const auto result = engineRpc_.readFinding(query);
    if (!result.ok()) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                result.error.code,
                                                result.error.message,
                                                result.error.retryable,
                                                revisionUnavailable()));
    }
    return makeToolResult(makeSuccessEnvelope(tool,
                                              investigationResultFromPayload(result.value),
                                              revisionFromSummary(result.value.summary)));
}

json Adapter::invokeReadOrderCaseTool(const ToolSpec& tool, const json& args) const {
    OrderCaseQuery query;
    std::string code;
    std::string message;
    if (!parseOrderCaseArgs(args, &query, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable()));
    }
    const auto result = engineRpc_.readOrderCase(query);
    if (!result.ok()) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                result.error.code,
                                                result.error.message,
                                                result.error.retryable,
                                                revisionUnavailable()));
    }
    return makeToolResult(makeSuccessEnvelope(tool,
                                              investigationResultFromPayload(result.value),
                                              revisionFromSummary(result.value.summary)));
}

json Adapter::invokeReadOrderAnchorTool(const ToolSpec& tool, const json& args) const {
    NumericIdQuery query;
    std::string code;
    std::string message;
    if (!parseNumericIdArgs(args, "anchor_id", &query, &code, &message, true, true, true)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable()));
    }
    const auto result = engineRpc_.readOrderAnchor(query);
    if (!result.ok()) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                result.error.code,
                                                result.error.message,
                                                result.error.retryable,
                                                revisionUnavailable()));
    }
    return makeToolResult(makeSuccessEnvelope(tool,
                                              investigationResultFromPayload(result.value),
                                              revisionFromSummary(result.value.summary)));
}

json Adapter::invokeReadProtectedWindowTool(const ToolSpec& tool, const json& args) const {
    NumericIdQuery query;
    std::string code;
    std::string message;
    if (!parseNumericIdArgs(args, "window_id", &query, &code, &message, true, true, true)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable()));
    }
    const auto result = engineRpc_.readProtectedWindow(query);
    if (!result.ok()) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                result.error.code,
                                                result.error.message,
                                                result.error.retryable,
                                                revisionUnavailable()));
    }
    const auto packed = tapescope::client_internal::packInvestigationPayload(
        tapescope::client_internal::makeSuccess(result.value));
    if (!packed.ok()) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                "malformed_response",
                                                packed.error.message,
                                                false,
                                                revisionUnavailable()));
    }
    return makeToolResult(makeSuccessEnvelope(tool,
                                              investigationResultFromPayload(packed.value),
                                              revisionFromSummary(result.value.summary)));
}

json Adapter::invokeReplaySnapshotTool(const ToolSpec& tool, const json& args) const {
    ReplaySnapshotQuery query;
    std::string code;
    std::string message;
    if (!parseReplaySnapshotArgs(args, &query, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable()));
    }
    const auto result = engineRpc_.replaySnapshot(query);
    if (!result.ok()) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                result.error.code,
                                                result.error.message,
                                                result.error.retryable,
                                                revisionUnavailable()));
    }
    return makeToolResult(makeSuccessEnvelope(tool,
                                              replaySnapshotResultFromResponse(result.value),
                                              revisionFromSummary(result.value.summary)));
}

json Adapter::invokeReadIncidentTool(const ToolSpec& tool, const json& args) const {
    IncidentQuery query;
    std::string code;
    std::string message;
    if (!parseIncidentArgs(args, &query, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable()));
    }
    const auto result = engineRpc_.readIncident(query);
    if (!result.ok()) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                result.error.code,
                                                result.error.message,
                                                result.error.retryable,
                                                revisionUnavailable()));
    }
    return makeToolResult(makeSuccessEnvelope(tool,
                                              investigationResultFromPayload(result.value),
                                              revisionFromSummary(result.value.summary)));
}

json Adapter::invokeReadArtifactTool(const ToolSpec& tool, const json& args) const {
    ArtifactQuery query;
    std::string code;
    std::string message;
    if (!parseArtifactArgs(args, &query, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable()));
    }
    const auto result = engineRpc_.readArtifact(query);
    if (!result.ok()) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                result.error.code,
                                                result.error.message,
                                                result.error.retryable,
                                                revisionUnavailable()));
    }
    return makeToolResult(makeSuccessEnvelope(tool,
                                              investigationResultFromPayload(result.value),
                                              revisionFromSummary(result.value.summary)));
}

json Adapter::invokeExportArtifactTool(const ToolSpec& tool, const json& args) const {
    ArtifactExportQuery query;
    std::string code;
    std::string message;
    if (!parseExportArgs(args, &query, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable()));
    }
    const auto result = engineRpc_.exportArtifact(query);
    if (!result.ok()) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                result.error.code,
                                                result.error.message,
                                                result.error.retryable,
                                                revisionUnavailable()));
    }
    return makeToolResult(makeSuccessEnvelope(tool,
                                              exportResultFromPayload(result.value),
                                              revisionFromSummary(result.value.summary)));
}

json Adapter::invokeReadSessionQualityTool(const ToolSpec& tool, const json& args) const {
    SessionWindowQuery query;
    std::string code;
    std::string message;
    if (!parseSessionWindowArgs(args, true, false, &query, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable()));
    }
    const auto result = engineRpc_.readSessionQuality(query);
    if (!result.ok()) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                result.error.code,
                                                result.error.message,
                                                result.error.retryable,
                                                revisionUnavailable()));
    }
    return makeToolResult(makeSuccessEnvelope(tool,
                                              qualityResultFromPayload(result.value),
                                              revisionFromSummary(result.value.summary)));
}

json Adapter::makeToolResult(const json& envelope) const {
    return json{
        {"isError", !envelope.value("ok", false)},
        {"structuredContent", envelope},
        {"content", json::array({
            json{
                {"type", "text"},
                {"text", envelopeText(envelope)}
            }
        })}
    };
}

json Adapter::makeSuccessEnvelope(const ToolSpec& tool, json result, json revision) const {
    return json{
        {"ok", true},
        {"meta", {
            {"contract_version", kContractVersion},
            {"tool", tool.name},
            {"engine_operation", tape_engine::queryOperationName(tool.engineOperation)},
            {"engine_socket_path", engineRpc_.config().socketPath},
            {"result_schema", tool.outputSchemaId},
            {"envelope_schema", kToolEnvelopeSchema},
            {"envelope_version", kToolEnvelopeVersion},
            {"supported", true},
            {"deferred", false},
            {"retryable", false}
        }},
        {"revision", std::move(revision)},
        {"result", std::move(result)}
    };
}

json Adapter::makeErrorEnvelope(const std::string& toolName,
                                std::string_view engineOperation,
                                std::string_view outputSchemaId,
                                bool supported,
                                bool deferred,
                                const std::string& errorCode,
                                const std::string& errorMessage,
                                bool retryable,
                                json revision) const {
    return json{
        {"ok", false},
        {"meta", {
            {"contract_version", kContractVersion},
            {"tool", toolName},
            {"engine_operation", std::string(engineOperation)},
            {"engine_socket_path", engineRpc_.config().socketPath},
            {"result_schema", std::string(outputSchemaId)},
            {"envelope_schema", kToolEnvelopeSchema},
            {"envelope_version", kToolEnvelopeVersion},
            {"supported", supported},
            {"deferred", deferred},
            {"retryable", retryable}
        }},
        {"revision", std::move(revision)},
        {"error", {
            {"code", errorCode},
            {"message", errorMessage}
        }}
    };
}

} // namespace tape_mcp
