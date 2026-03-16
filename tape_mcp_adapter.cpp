#include "tape_mcp_adapter.h"
#include "tape_phase7_artifacts.h"
#include "tape_query_payloads.h"

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
constexpr const char* kPhase7ContractVersion = tape_phase7::kContractVersion;
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

json nullableStringSchema() {
    return json{{"oneOf", json::array({stringSchema(), json{{"type", "null"}}})}};
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

json importedCaseRowSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"artifact_id", stringSchema()},
            {"imported_case_id", positiveIntegerSchema()},
            {"bundle_id", stringSchema()},
            {"bundle_type", stringSchema()},
            {"source_artifact_id", stringSchema()},
            {"source_report_id", nonNegativeIntegerSchema()},
            {"source_revision_id", nonNegativeIntegerSchema()},
            {"first_session_seq", nonNegativeIntegerSchema()},
            {"last_session_seq", nonNegativeIntegerSchema()},
            {"instrument_id", stringSchema()},
            {"headline", stringSchema()},
            {"file_name", stringSchema()},
            {"source_bundle_path", stringSchema()},
            {"payload_sha256", stringSchema()}
        }},
        {"required", json::array({
            "artifact_id",
            "imported_case_id",
            "bundle_id",
            "bundle_type",
            "source_artifact_id",
            "source_report_id",
            "source_revision_id",
            "first_session_seq",
            "last_session_seq",
            "instrument_id",
            "headline",
            "file_name",
            "source_bundle_path",
            "payload_sha256"
        })},
        {"additionalProperties", true}
    };
}

json bundleExportResultSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"artifact", json{{"type", "object"}, {"additionalProperties", true}}},
            {"bundle", json{{"type", "object"}, {"additionalProperties", true}}},
            {"source_artifact", json{{"type", "object"}, {"additionalProperties", true}}},
            {"source_report", json{{"type", "object"}, {"additionalProperties", true}}},
            {"served_revision_id", json{{"type", json::array({"integer", "null"})}}},
            {"export_status", stringSchema()}
        }},
        {"required", json::array({
            "artifact",
            "bundle",
            "source_artifact",
            "source_report",
            "served_revision_id",
            "export_status"
        })},
        {"additionalProperties", false}
    };
}

json bundleImportResultSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"artifact", json{{"type", "object"}, {"additionalProperties", true}}},
            {"imported_case", importedCaseRowSchema()},
            {"import_status", stringSchema()},
            {"duplicate_import", booleanSchema()}
        }},
        {"required", json::array({
            "artifact",
            "imported_case",
            "import_status",
            "duplicate_import"
        })},
        {"additionalProperties", false}
    };
}

json bundleVerifyResultSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"artifact", json{{"type", "object"}, {"additionalProperties", true}}},
            {"bundle", json{{"type", "object"}, {"additionalProperties", true}}},
            {"source_artifact", json{{"type", "object"}, {"additionalProperties", true}}},
            {"source_report", json{{"type", "object"}, {"additionalProperties", true}}},
            {"report_summary", json{{"type", "object"}, {"additionalProperties", true}}},
            {"report_markdown", stringSchema()},
            {"verify_status", stringSchema()},
            {"import_supported", booleanSchema()},
            {"already_imported", booleanSchema()},
            {"can_import", booleanSchema()},
            {"import_reason", stringSchema()},
            {"imported_case", json{{"oneOf", json::array({importedCaseRowSchema(), json{{"type", "null"}}})}}},
            {"served_revision_id", json{{"type", json::array({"integer", "null"})}}}
        }},
        {"required", json::array({
            "artifact",
            "bundle",
            "source_artifact",
            "source_report",
            "report_summary",
            "report_markdown",
            "verify_status",
            "import_supported",
            "already_imported",
            "can_import",
            "import_reason",
            "imported_case",
            "served_revision_id"
        })},
        {"additionalProperties", false}
    };
}

json phase7FindingSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"finding_id", stringSchema()},
            {"severity", stringSchema()},
            {"category", stringSchema()},
            {"summary", stringSchema()},
            {"evidence_refs", json{{"type", "array"}, {"items", json{{"type", "object"}, {"additionalProperties", true}}}}}
        }},
        {"required", json::array({"finding_id", "severity", "category", "summary", "evidence_refs"})},
        {"additionalProperties", false}
    };
}

json phase7AppliedFiltersSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"requested_finding_ids", json{{"type", "array"}, {"items", stringSchema()}}},
            {"minimum_severity", nullableStringSchema()},
            {"category", nullableStringSchema()},
            {"limit", json{{"oneOf", json::array({positiveIntegerSchema(), json{{"type", "null"}}})}}},
            {"selected_count", nonNegativeIntegerSchema()}
        }},
        {"required", json::array({
            "requested_finding_ids",
            "minimum_severity",
            "category",
            "limit",
            "selected_count"
        })},
        {"additionalProperties", false}
    };
}

json phase7AnalysisInventoryFiltersSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"source_artifact_id", nullableStringSchema()},
            {"analysis_profile", nullableStringSchema()},
            {"limit", json{{"oneOf", json::array({positiveIntegerSchema(), json{{"type", "null"}}})}}},
            {"matched_count", nonNegativeIntegerSchema()}
        }},
        {"required", json::array({
            "source_artifact_id",
            "analysis_profile",
            "limit",
            "matched_count"
        })},
        {"additionalProperties", false}
    };
}

json phase7PlaybookInventoryFiltersSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"analysis_artifact_id", nullableStringSchema()},
            {"source_artifact_id", nullableStringSchema()},
            {"mode", nullableStringSchema()},
            {"limit", json{{"oneOf", json::array({positiveIntegerSchema(), json{{"type", "null"}}})}}},
            {"matched_count", nonNegativeIntegerSchema()}
        }},
        {"required", json::array({
            "analysis_artifact_id",
            "source_artifact_id",
            "mode",
            "limit",
            "matched_count"
        })},
        {"additionalProperties", false}
    };
}

json phase7ExecutionLedgerFiltersSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"playbook_artifact_id", nullableStringSchema()},
            {"analysis_artifact_id", nullableStringSchema()},
            {"source_artifact_id", nullableStringSchema()},
            {"ledger_status", json{{"oneOf", json::array({
                stringEnumSchema({
                    tape_phase7::kDefaultLedgerStatus,
                    tape_phase7::kLedgerStatusInProgress,
                    tape_phase7::kLedgerStatusBlocked,
                    tape_phase7::kLedgerStatusNeedsInformation,
                    tape_phase7::kLedgerStatusCompleted,
                    tape_phase7::kLedgerStatusWaitingApproval,
                    tape_phase7::kLedgerStatusReadyForExecution
                }),
                json{{"type", "null"}}
            })}}},
            {"sort_by", stringEnumSchema({"generated_at_desc", "attention_desc", "source_artifact_asc"})},
            {"limit", json{{"oneOf", json::array({positiveIntegerSchema(), json{{"type", "null"}}})}}},
            {"matched_count", nonNegativeIntegerSchema()}
        }},
        {"required", json::array({
            "playbook_artifact_id",
            "analysis_artifact_id",
            "source_artifact_id",
            "ledger_status",
            "sort_by",
            "limit",
            "matched_count"
        })},
        {"additionalProperties", false}
    };
}

json phase7PlaybookActionSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"action_id", stringSchema()},
            {"action_type", stringSchema()},
            {"finding_id", stringSchema()},
            {"title", stringSchema()},
            {"summary", stringSchema()},
            {"suggested_tools", json{{"type", "array"}, {"items", stringSchema()}}}
        }},
        {"required", json::array({"action_id", "action_type", "finding_id", "title", "summary", "suggested_tools"})},
        {"additionalProperties", false}
    };
}

json phase7ExecutionLedgerEntryStatusSchema() {
    return stringEnumSchema({
        tape_phase7::kDefaultLedgerEntryStatus,
        tape_phase7::kLedgerEntryStatusApproved,
        tape_phase7::kLedgerEntryStatusBlocked,
        tape_phase7::kLedgerEntryStatusNeedsInfo,
        tape_phase7::kLedgerEntryStatusNotApplicable
    });
}

json phase7ExecutionLedgerAggregateStatusSchema() {
    return stringEnumSchema({
        tape_phase7::kDefaultLedgerStatus,
        tape_phase7::kLedgerStatusInProgress,
        tape_phase7::kLedgerStatusBlocked,
        tape_phase7::kLedgerStatusNeedsInformation,
        tape_phase7::kLedgerStatusCompleted,
        tape_phase7::kLedgerStatusWaitingApproval,
        tape_phase7::kLedgerStatusReadyForExecution
    });
}

json phase7ExecutionLedgerEntrySchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"entry_id", stringSchema()},
            {"action_id", stringSchema()},
            {"action_type", stringSchema()},
            {"finding_id", stringSchema()},
            {"review_status", phase7ExecutionLedgerEntryStatusSchema()},
            {"reviewed_at_utc", nullableStringSchema()},
            {"reviewed_by", nullableStringSchema()},
            {"review_comment", nullableStringSchema()},
            {"distinct_reviewer_count", nonNegativeIntegerSchema()},
            {"approval_reviewer_count", nonNegativeIntegerSchema()},
            {"approval_threshold_met", booleanSchema()},
            {"requires_manual_confirmation", booleanSchema()},
            {"title", stringSchema()},
            {"summary", stringSchema()},
            {"suggested_tools", json{{"type", "array"}, {"items", stringSchema()}}}
        }},
        {"required", json::array({
            "entry_id",
            "action_id",
            "action_type",
            "finding_id",
            "review_status",
            "reviewed_at_utc",
            "reviewed_by",
            "review_comment",
            "distinct_reviewer_count",
            "approval_reviewer_count",
            "approval_threshold_met",
            "requires_manual_confirmation",
            "title",
            "summary",
            "suggested_tools"
        })},
        {"additionalProperties", false}
    };
}

json phase7ExecutionLedgerReviewStatusSchema() {
    return stringEnumSchema({
        tape_phase7::kLedgerEntryStatusApproved,
        tape_phase7::kLedgerEntryStatusBlocked,
        tape_phase7::kLedgerEntryStatusNeedsInfo,
        tape_phase7::kLedgerEntryStatusNotApplicable
    });
}

json phase7ExecutionLedgerReviewSummarySchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"pending_review_count", nonNegativeIntegerSchema()},
            {"approved_count", nonNegativeIntegerSchema()},
            {"blocked_count", nonNegativeIntegerSchema()},
            {"needs_info_count", nonNegativeIntegerSchema()},
            {"not_applicable_count", nonNegativeIntegerSchema()},
            {"reviewed_count", nonNegativeIntegerSchema()},
            {"waiting_approval_count", nonNegativeIntegerSchema()},
            {"ready_entry_count", nonNegativeIntegerSchema()},
            {"actionable_entry_count", nonNegativeIntegerSchema()},
            {"distinct_reviewer_count", nonNegativeIntegerSchema()},
            {"required_approval_count", nonNegativeIntegerSchema()},
            {"ready_for_execution", booleanSchema()}
        }},
        {"required", json::array({
            "pending_review_count",
            "approved_count",
            "blocked_count",
            "needs_info_count",
            "not_applicable_count",
            "reviewed_count",
            "waiting_approval_count",
            "ready_entry_count",
            "actionable_entry_count",
            "distinct_reviewer_count",
            "required_approval_count",
            "ready_for_execution"
        })},
        {"additionalProperties", false}
    };
}

json phase7ExecutionLedgerAuditSummarySchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"event_type", nullableStringSchema()},
            {"generated_at_utc", nullableStringSchema()},
            {"actor", nullableStringSchema()},
            {"review_status", json{{"oneOf", json::array({phase7ExecutionLedgerEntryStatusSchema(), json{{"type", "null"}}})}}},
            {"ledger_status", json{{"oneOf", json::array({phase7ExecutionLedgerAggregateStatusSchema(), json{{"type", "null"}}})}}},
            {"message", nullableStringSchema()}
        }},
        {"required", json::array({"event_type", "generated_at_utc", "actor", "review_status", "ledger_status", "message"})},
        {"additionalProperties", false}
    };
}

json phase7AnalyzerResultSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"source_artifact", json{{"type", "object"}, {"additionalProperties", true}}},
            {"analysis_artifact", json{{"type", "object"}, {"additionalProperties", true}}},
            {"generated_artifacts", json{{"type", "array"}, {"items", json{{"type", "object"}, {"additionalProperties", true}}}}},
            {"analysis_profile", stringSchema()},
            {"generated_at_utc", stringSchema()},
            {"finding_count", nonNegativeIntegerSchema()},
            {"replay_context", json{{"type", "object"}, {"additionalProperties", true}}},
            {"findings", json{{"type", "array"}, {"items", phase7FindingSchema()}}}
        }},
        {"required", json::array({
            "source_artifact",
            "analysis_artifact",
            "generated_artifacts",
            "analysis_profile",
            "generated_at_utc",
            "finding_count",
            "replay_context",
            "findings"
        })},
        {"additionalProperties", false}
    };
}

json phase7AnalyzerRunResultSchema() {
    json schema = phase7AnalyzerResultSchema();
    schema["properties"]["artifact_status"] = stringEnumSchema({"created", "reused"});
    schema["required"].push_back("artifact_status");
    return schema;
}

json phase7FindingsListResultSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"analysis_artifact", json{{"type", "object"}, {"additionalProperties", true}}},
            {"analysis_profile", stringSchema()},
            {"generated_at_utc", stringSchema()},
            {"finding_count", nonNegativeIntegerSchema()},
            {"filtered_finding_ids", json{{"type", "array"}, {"items", stringSchema()}}},
            {"applied_filters", phase7AppliedFiltersSchema()},
            {"replay_context", json{{"type", "object"}, {"additionalProperties", true}}},
            {"findings", json{{"type", "array"}, {"items", phase7FindingSchema()}}}
        }},
        {"required", json::array({
            "analysis_artifact",
            "analysis_profile",
            "generated_at_utc",
            "finding_count",
            "filtered_finding_ids",
            "applied_filters",
            "replay_context",
            "findings"
        })},
        {"additionalProperties", false}
    };
}

json phase7AnalysisArtifactRowSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"analysis_artifact", json{{"type", "object"}, {"additionalProperties", true}}},
            {"source_artifact", json{{"type", "object"}, {"additionalProperties", true}}},
            {"analysis_profile", stringSchema()},
            {"generated_at_utc", stringSchema()},
            {"finding_count", nonNegativeIntegerSchema()},
            {"replay_context", json{{"type", "object"}, {"additionalProperties", true}}}
        }},
        {"required", json::array({
            "analysis_artifact",
            "source_artifact",
            "analysis_profile",
            "generated_at_utc",
            "finding_count",
            "replay_context"
        })},
        {"additionalProperties", false}
    };
}

json phase7AnalysisArtifactListResultSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"returned_count", nonNegativeIntegerSchema()},
            {"applied_filters", phase7AnalysisInventoryFiltersSchema()},
            {"analysis_artifacts", json{{"type", "array"}, {"items", phase7AnalysisArtifactRowSchema()}}}
        }},
        {"required", json::array({"returned_count", "applied_filters", "analysis_artifacts"})},
        {"additionalProperties", false}
    };
}

json phase7PlaybookResultSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"analysis_artifact", json{{"type", "object"}, {"additionalProperties", true}}},
            {"playbook_artifact", json{{"type", "object"}, {"additionalProperties", true}}},
            {"mode", stringEnumSchema({tape_phase7::kDefaultPlaybookMode, tape_phase7::kApplyPlaybookMode})},
            {"generated_at_utc", stringSchema()},
            {"filtered_finding_ids", json{{"type", "array"}, {"items", stringSchema()}}},
            {"applied_filters", phase7AppliedFiltersSchema()},
            {"planned_actions", json{{"type", "array"}, {"items", phase7PlaybookActionSchema()}}},
            {"replay_context", json{{"type", "object"}, {"additionalProperties", true}}}
        }},
        {"required", json::array({
            "analysis_artifact",
            "playbook_artifact",
            "mode",
            "generated_at_utc",
            "filtered_finding_ids",
            "applied_filters",
            "planned_actions",
            "replay_context"
        })},
        {"additionalProperties", false}
    };
}

json phase7PlaybookApplyResultSchema() {
    json schema = phase7PlaybookResultSchema();
    schema["properties"]["artifact_status"] = stringEnumSchema({"created", "reused"});
    schema["required"].push_back("artifact_status");
    return schema;
}

json phase7PlaybookArtifactRowSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"playbook_artifact", json{{"type", "object"}, {"additionalProperties", true}}},
            {"analysis_artifact", json{{"type", "object"}, {"additionalProperties", true}}},
            {"mode", stringEnumSchema({tape_phase7::kDefaultPlaybookMode, tape_phase7::kApplyPlaybookMode})},
            {"generated_at_utc", stringSchema()},
            {"filtered_finding_count", nonNegativeIntegerSchema()},
            {"planned_action_count", nonNegativeIntegerSchema()},
            {"replay_context", json{{"type", "object"}, {"additionalProperties", true}}}
        }},
        {"required", json::array({
            "playbook_artifact",
            "analysis_artifact",
            "mode",
            "generated_at_utc",
            "filtered_finding_count",
            "planned_action_count",
            "replay_context"
        })},
        {"additionalProperties", false}
    };
}

json phase7PlaybookArtifactListResultSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"returned_count", nonNegativeIntegerSchema()},
            {"applied_filters", phase7PlaybookInventoryFiltersSchema()},
            {"playbook_artifacts", json{{"type", "array"}, {"items", phase7PlaybookArtifactRowSchema()}}}
        }},
        {"required", json::array({"returned_count", "applied_filters", "playbook_artifacts"})},
        {"additionalProperties", false}
    };
}

json phase7ExecutionLedgerResultSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"source_artifact", json{{"type", "object"}, {"additionalProperties", true}}},
            {"analysis_artifact", json{{"type", "object"}, {"additionalProperties", true}}},
            {"playbook_artifact", json{{"type", "object"}, {"additionalProperties", true}}},
            {"execution_ledger", json{{"type", "object"}, {"additionalProperties", true}}},
            {"mode", stringEnumSchema({tape_phase7::kDefaultPlaybookMode, tape_phase7::kApplyPlaybookMode})},
            {"generated_at_utc", stringSchema()},
            {"ledger_status", phase7ExecutionLedgerAggregateStatusSchema()},
            {"execution_policy", json{{"type", "object"}, {"additionalProperties", true}}},
            {"filtered_finding_ids", json{{"type", "array"}, {"items", stringSchema()}}},
            {"entry_count", nonNegativeIntegerSchema()},
            {"review_summary", phase7ExecutionLedgerReviewSummarySchema()},
            {"latest_audit_event", phase7ExecutionLedgerAuditSummarySchema()},
            {"entries", json{{"type", "array"}, {"items", phase7ExecutionLedgerEntrySchema()}}},
            {"audit_trail", json{{"type", "array"}, {"items", json{{"type", "object"}, {"additionalProperties", true}}}}},
            {"replay_context", json{{"type", "object"}, {"additionalProperties", true}}}
        }},
        {"required", json::array({
            "source_artifact",
            "analysis_artifact",
            "playbook_artifact",
            "execution_ledger",
            "mode",
            "generated_at_utc",
            "ledger_status",
            "execution_policy",
            "filtered_finding_ids",
            "entry_count",
            "review_summary",
            "latest_audit_event",
            "entries",
            "audit_trail",
            "replay_context"
        })},
        {"additionalProperties", false}
    };
}

json phase7ExecutionLedgerBuildResultSchema() {
    json schema = phase7ExecutionLedgerResultSchema();
    schema["properties"]["artifact_status"] = stringEnumSchema({"created", "reused"});
    schema["required"].push_back("artifact_status");
    return schema;
}

json phase7ExecutionLedgerReviewResultSchema() {
    json schema = phase7ExecutionLedgerResultSchema();
    schema["properties"]["updated_entry_ids"] = json{{"type", "array"}, {"items", stringSchema()}};
    schema["properties"]["audit_event_id"] = stringSchema();
    schema["required"].push_back("updated_entry_ids");
    schema["required"].push_back("audit_event_id");
    return schema;
}

json phase7ExecutionLedgerRowSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"execution_ledger", json{{"type", "object"}, {"additionalProperties", true}}},
            {"playbook_artifact", json{{"type", "object"}, {"additionalProperties", true}}},
            {"analysis_artifact", json{{"type", "object"}, {"additionalProperties", true}}},
            {"source_artifact", json{{"type", "object"}, {"additionalProperties", true}}},
            {"mode", stringEnumSchema({tape_phase7::kDefaultPlaybookMode, tape_phase7::kApplyPlaybookMode})},
            {"generated_at_utc", stringSchema()},
            {"ledger_status", phase7ExecutionLedgerAggregateStatusSchema()},
            {"entry_count", nonNegativeIntegerSchema()},
            {"review_summary", phase7ExecutionLedgerReviewSummarySchema()},
            {"latest_audit_event", phase7ExecutionLedgerAuditSummarySchema()},
            {"replay_context", json{{"type", "object"}, {"additionalProperties", true}}}
        }},
        {"required", json::array({
            "execution_ledger",
            "playbook_artifact",
            "analysis_artifact",
            "source_artifact",
            "mode",
            "generated_at_utc",
            "ledger_status",
            "entry_count",
            "review_summary",
            "latest_audit_event",
            "replay_context"
        })},
        {"additionalProperties", false}
    };
}

json phase7ExecutionLedgerListResultSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"returned_count", nonNegativeIntegerSchema()},
            {"applied_filters", phase7ExecutionLedgerFiltersSchema()},
            {"execution_ledgers", json{{"type", "array"}, {"items", phase7ExecutionLedgerRowSchema()}}}
        }},
        {"required", json::array({"returned_count", "applied_filters", "execution_ledgers"})},
        {"additionalProperties", false}
    };
}

json phase7ExecutionJournalEntryStatusSchema() {
    return stringEnumSchema({
        tape_phase7::kExecutionEntryStatusQueued,
        tape_phase7::kExecutionEntryStatusSubmitted,
        tape_phase7::kExecutionEntryStatusSucceeded,
        tape_phase7::kExecutionEntryStatusFailed,
        tape_phase7::kExecutionEntryStatusCancelled
    });
}

json phase7ExecutionJournalAggregateStatusSchema() {
    return stringEnumSchema({
        tape_phase7::kExecutionJournalStatusQueued,
        tape_phase7::kExecutionJournalStatusInProgress,
        tape_phase7::kExecutionJournalStatusSucceeded,
        tape_phase7::kExecutionJournalStatusPartiallySucceeded,
        tape_phase7::kExecutionJournalStatusFailed,
        tape_phase7::kExecutionJournalStatusCancelled
    });
}

json phase7ExecutionRecoveryStateSchema() {
    return stringEnumSchema({"recovery_required", "stale_recovery_required"});
}

json phase7ExecutionRecoverySummarySchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"runtime_backed_submitted_count", nonNegativeIntegerSchema()},
            {"stale_runtime_backed_count", nonNegativeIntegerSchema()},
            {"recovery_required", booleanSchema()},
            {"stale_recovery_required", booleanSchema()}
        }},
        {"required", json::array({
            "runtime_backed_submitted_count",
            "stale_runtime_backed_count",
            "recovery_required",
            "stale_recovery_required"
        })},
        {"additionalProperties", false}
    };
}

json phase7ExecutionResultSummarySchema();
json phase7LatestExecutionResultSummarySchema();

json phase7ExecutionJournalFiltersSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"execution_ledger_artifact_id", nullableStringSchema()},
            {"playbook_artifact_id", nullableStringSchema()},
            {"analysis_artifact_id", nullableStringSchema()},
            {"source_artifact_id", nullableStringSchema()},
            {"journal_status", json{{"oneOf", json::array({
                phase7ExecutionJournalAggregateStatusSchema(),
                json{{"type", "null"}}
            })}}},
            {"recovery_state", json{{"oneOf", json::array({
                phase7ExecutionRecoveryStateSchema(),
                json{{"type", "null"}}
            })}}},
            {"sort_by", stringEnumSchema({"generated_at_desc", "attention_desc", "source_artifact_asc"})},
            {"limit", json{{"oneOf", json::array({positiveIntegerSchema(), json{{"type", "null"}}})}}},
            {"matched_count", nonNegativeIntegerSchema()}
        }},
        {"required", json::array({
            "execution_ledger_artifact_id",
            "playbook_artifact_id",
            "analysis_artifact_id",
            "source_artifact_id",
            "journal_status",
            "sort_by",
            "limit",
            "matched_count"
        })},
        {"additionalProperties", false}
    };
}

json phase7ExecutionJournalEntrySchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"journal_entry_id", stringSchema()},
            {"ledger_entry_id", stringSchema()},
            {"action_id", stringSchema()},
            {"action_type", stringSchema()},
            {"finding_id", stringSchema()},
            {"execution_status", phase7ExecutionJournalEntryStatusSchema()},
            {"idempotency_key", stringSchema()},
            {"requires_manual_confirmation", booleanSchema()},
            {"title", stringSchema()},
            {"summary", stringSchema()},
            {"queued_at_utc", stringSchema()},
            {"started_at_utc", nullableStringSchema()},
            {"completed_at_utc", nullableStringSchema()},
            {"last_updated_at_utc", nullableStringSchema()},
            {"last_updated_by", nullableStringSchema()},
            {"execution_comment", nullableStringSchema()},
            {"failure_code", nullableStringSchema()},
            {"failure_message", nullableStringSchema()},
            {"attempt_count", nonNegativeIntegerSchema()},
            {"terminal", booleanSchema()},
            {"suggested_tools", json{{"type", "array"}, {"items", stringSchema()}}},
            {"execution_request", json{{"type", "object"}, {"additionalProperties", true}}},
            {"execution_result", json{{"type", "object"}, {"additionalProperties", true}}},
            {"execution_result_summary", phase7ExecutionResultSummarySchema()}
        }},
        {"required", json::array({
            "journal_entry_id",
            "ledger_entry_id",
            "action_id",
            "action_type",
            "finding_id",
            "execution_status",
            "idempotency_key",
            "requires_manual_confirmation",
            "title",
            "summary",
            "queued_at_utc",
            "started_at_utc",
            "completed_at_utc",
            "last_updated_at_utc",
            "last_updated_by",
            "execution_comment",
            "failure_code",
            "failure_message",
            "attempt_count",
            "terminal",
            "suggested_tools",
            "execution_request",
            "execution_result",
            "execution_result_summary"
        })},
        {"additionalProperties", false}
    };
}

json phase7ExecutionJournalSummarySchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"queued_count", nonNegativeIntegerSchema()},
            {"submitted_count", nonNegativeIntegerSchema()},
            {"succeeded_count", nonNegativeIntegerSchema()},
            {"failed_count", nonNegativeIntegerSchema()},
            {"cancelled_count", nonNegativeIntegerSchema()},
            {"terminal_count", nonNegativeIntegerSchema()},
            {"actionable_entry_count", nonNegativeIntegerSchema()},
            {"all_terminal", booleanSchema()}
        }},
        {"required", json::array({
            "queued_count",
            "submitted_count",
            "succeeded_count",
            "failed_count",
            "cancelled_count",
            "terminal_count",
            "actionable_entry_count",
            "all_terminal"
        })},
        {"additionalProperties", false}
    };
}

json phase7ExecutionResultSummarySchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"resolution", nullableStringSchema()},
            {"fill_state", nullableStringSchema()},
            {"restart_resume_policy", nullableStringSchema()},
            {"restart_recovery_state", nullableStringSchema()},
            {"restart_recovery_reason", nullableStringSchema()},
            {"partial_fill_before_terminal", booleanSchema()},
            {"cancel_ack_pending", json{{"oneOf", json::array({booleanSchema(), json{{"type", "null"}}})}}},
            {"manual_review_required", booleanSchema()},
            {"broker_status_detail", nullableStringSchema()},
            {"latest_exec_id", nullableStringSchema()},
            {"broker_identity", json{{"type", "object"}, {"additionalProperties", true}}},
            {"trade_trace", json{{"type", "object"}, {"additionalProperties", true}}}
        }},
        {"required", json::array({
            "resolution",
            "fill_state",
            "restart_resume_policy",
            "restart_recovery_state",
            "restart_recovery_reason",
            "partial_fill_before_terminal",
            "cancel_ack_pending",
            "manual_review_required",
            "broker_status_detail",
            "latest_exec_id",
            "broker_identity",
            "trade_trace"
        })},
        {"additionalProperties", false}
    };
}

json phase7LatestExecutionResultSummarySchema() {
    return json{
        {"oneOf", json::array({
            json{
                {"type", "object"},
                {"properties", {
                    {"entry_id", stringSchema()},
                    {"execution_status", phase7ExecutionJournalEntryStatusSchema()},
                    {"terminal", booleanSchema()},
                    {"action_type", stringSchema()},
                    {"title", stringSchema()},
                    {"attempt_count", nonNegativeIntegerSchema()},
                    {"execution_result_summary", phase7ExecutionResultSummarySchema()}
                }},
                {"required", json::array({
                    "entry_id",
                    "execution_status",
                    "terminal",
                    "action_type",
                    "title",
                    "attempt_count",
                    "execution_result_summary"
                })},
                {"additionalProperties", false}
            },
            json{{"type", "null"}}
        })}
    };
}

json phase7ExecutionJournalAuditSummarySchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"event_type", nullableStringSchema()},
            {"generated_at_utc", nullableStringSchema()},
            {"actor", nullableStringSchema()},
            {"execution_status", json{{"oneOf", json::array({phase7ExecutionJournalEntryStatusSchema(), json{{"type", "null"}}})}}},
            {"journal_status", json{{"oneOf", json::array({phase7ExecutionJournalAggregateStatusSchema(), json{{"type", "null"}}})}}},
            {"message", nullableStringSchema()}
        }},
        {"required", json::array({
            "event_type",
            "generated_at_utc",
            "actor",
            "execution_status",
            "journal_status",
            "message"
        })},
        {"additionalProperties", false}
    };
}

json phase7ExecutionJournalResultSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"source_artifact", json{{"type", "object"}, {"additionalProperties", true}}},
            {"analysis_artifact", json{{"type", "object"}, {"additionalProperties", true}}},
            {"playbook_artifact", json{{"type", "object"}, {"additionalProperties", true}}},
            {"execution_ledger", json{{"type", "object"}, {"additionalProperties", true}}},
            {"execution_journal", json{{"type", "object"}, {"additionalProperties", true}}},
            {"mode", stringEnumSchema({tape_phase7::kDefaultPlaybookMode, tape_phase7::kApplyPlaybookMode})},
            {"generated_at_utc", stringSchema()},
            {"initiated_by", stringSchema()},
            {"execution_capability", stringSchema()},
            {"journal_status", phase7ExecutionJournalAggregateStatusSchema()},
            {"execution_policy", json{{"type", "object"}, {"additionalProperties", true}}},
            {"filtered_finding_ids", json{{"type", "array"}, {"items", stringSchema()}}},
            {"entry_count", nonNegativeIntegerSchema()},
            {"execution_summary", phase7ExecutionJournalSummarySchema()},
            {"runtime_recovery_summary", phase7ExecutionRecoverySummarySchema()},
            {"latest_audit_event", phase7ExecutionJournalAuditSummarySchema()},
            {"latest_execution_result_summary", phase7LatestExecutionResultSummarySchema()},
            {"entries", json{{"type", "array"}, {"items", phase7ExecutionJournalEntrySchema()}}},
            {"audit_trail", json{{"type", "array"}, {"items", json{{"type", "object"}, {"additionalProperties", true}}}}},
            {"replay_context", json{{"type", "object"}, {"additionalProperties", true}}}
        }},
        {"required", json::array({
            "source_artifact",
            "analysis_artifact",
            "playbook_artifact",
            "execution_ledger",
            "execution_journal",
            "mode",
            "generated_at_utc",
            "initiated_by",
            "execution_capability",
            "journal_status",
            "execution_policy",
            "filtered_finding_ids",
            "entry_count",
            "execution_summary",
            "runtime_recovery_summary",
            "latest_audit_event",
            "latest_execution_result_summary",
            "entries",
            "audit_trail",
            "replay_context"
        })},
        {"additionalProperties", false}
    };
}

json phase7ExecutionJournalStartResultSchema() {
    json schema = phase7ExecutionJournalResultSchema();
    schema["properties"]["artifact_status"] = stringEnumSchema({"created", "reused"});
    schema["required"].push_back("artifact_status");
    return schema;
}

json phase7ExecutionJournalEventResultSchema() {
    json schema = phase7ExecutionJournalResultSchema();
    schema["properties"]["updated_entry_ids"] = json{{"type", "array"}, {"items", stringSchema()}};
    schema["properties"]["audit_event_id"] = stringSchema();
    schema["required"].push_back("updated_entry_ids");
    schema["required"].push_back("audit_event_id");
    return schema;
}

json phase7ExecutionJournalRowSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"execution_journal", json{{"type", "object"}, {"additionalProperties", true}}},
            {"execution_ledger", json{{"type", "object"}, {"additionalProperties", true}}},
            {"playbook_artifact", json{{"type", "object"}, {"additionalProperties", true}}},
            {"analysis_artifact", json{{"type", "object"}, {"additionalProperties", true}}},
            {"source_artifact", json{{"type", "object"}, {"additionalProperties", true}}},
            {"mode", stringEnumSchema({tape_phase7::kDefaultPlaybookMode, tape_phase7::kApplyPlaybookMode})},
            {"generated_at_utc", stringSchema()},
            {"journal_status", phase7ExecutionJournalAggregateStatusSchema()},
            {"entry_count", nonNegativeIntegerSchema()},
            {"execution_summary", phase7ExecutionJournalSummarySchema()},
            {"runtime_recovery_summary", phase7ExecutionRecoverySummarySchema()},
            {"latest_audit_event", phase7ExecutionJournalAuditSummarySchema()},
            {"latest_execution_result_summary", phase7LatestExecutionResultSummarySchema()},
            {"replay_context", json{{"type", "object"}, {"additionalProperties", true}}}
        }},
        {"required", json::array({
            "execution_journal",
            "execution_ledger",
            "playbook_artifact",
            "analysis_artifact",
            "source_artifact",
            "mode",
            "generated_at_utc",
            "journal_status",
            "entry_count",
            "execution_summary",
            "runtime_recovery_summary",
            "latest_audit_event",
            "latest_execution_result_summary",
            "replay_context"
        })},
        {"additionalProperties", false}
    };
}

json phase7ExecutionJournalListResultSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"returned_count", nonNegativeIntegerSchema()},
            {"applied_filters", phase7ExecutionJournalFiltersSchema()},
            {"execution_journals", json{{"type", "array"}, {"items", phase7ExecutionJournalRowSchema()}}}
        }},
        {"required", json::array({"returned_count", "applied_filters", "execution_journals"})},
        {"additionalProperties", false}
    };
}

json phase7ExecutionApplyFiltersSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"execution_journal_artifact_id", nullableStringSchema()},
            {"execution_ledger_artifact_id", nullableStringSchema()},
            {"playbook_artifact_id", nullableStringSchema()},
            {"analysis_artifact_id", nullableStringSchema()},
            {"source_artifact_id", nullableStringSchema()},
            {"apply_status", json{{"oneOf", json::array({
                phase7ExecutionJournalAggregateStatusSchema(),
                json{{"type", "null"}}
            })}}},
            {"recovery_state", json{{"oneOf", json::array({
                phase7ExecutionRecoveryStateSchema(),
                json{{"type", "null"}}
            })}}},
            {"sort_by", stringEnumSchema({"generated_at_desc", "attention_desc", "source_artifact_asc"})},
            {"limit", json{{"oneOf", json::array({positiveIntegerSchema(), json{{"type", "null"}}})}}},
            {"matched_count", nonNegativeIntegerSchema()}
        }},
        {"required", json::array({
            "execution_journal_artifact_id",
            "execution_ledger_artifact_id",
            "playbook_artifact_id",
            "analysis_artifact_id",
            "source_artifact_id",
            "apply_status",
            "sort_by",
            "limit",
            "matched_count"
        })},
        {"additionalProperties", false}
    };
}

json phase7ExecutionApplyEntrySchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"apply_entry_id", stringSchema()},
            {"journal_entry_id", stringSchema()},
            {"ledger_entry_id", stringSchema()},
            {"action_id", stringSchema()},
            {"action_type", stringSchema()},
            {"finding_id", stringSchema()},
            {"execution_status", phase7ExecutionJournalEntryStatusSchema()},
            {"idempotency_key", stringSchema()},
            {"requires_manual_confirmation", booleanSchema()},
            {"title", stringSchema()},
            {"summary", stringSchema()},
            {"submitted_at_utc", stringSchema()},
            {"completed_at_utc", nullableStringSchema()},
            {"last_updated_at_utc", nullableStringSchema()},
            {"last_updated_by", nullableStringSchema()},
            {"execution_comment", nullableStringSchema()},
            {"failure_code", nullableStringSchema()},
            {"failure_message", nullableStringSchema()},
            {"attempt_count", nonNegativeIntegerSchema()},
            {"terminal", booleanSchema()},
            {"suggested_tools", json{{"type", "array"}, {"items", stringSchema()}}},
            {"execution_request", json{{"type", "object"}, {"additionalProperties", true}}},
            {"execution_result", json{{"type", "object"}, {"additionalProperties", true}}},
            {"execution_result_summary", phase7ExecutionResultSummarySchema()}
        }},
        {"required", json::array({
            "apply_entry_id",
            "journal_entry_id",
            "ledger_entry_id",
            "action_id",
            "action_type",
            "finding_id",
            "execution_status",
            "idempotency_key",
            "requires_manual_confirmation",
            "title",
            "summary",
            "submitted_at_utc",
            "completed_at_utc",
            "last_updated_at_utc",
            "last_updated_by",
            "execution_comment",
            "failure_code",
            "failure_message",
            "attempt_count",
            "terminal",
            "suggested_tools",
            "execution_request",
            "execution_result",
            "execution_result_summary"
        })},
        {"additionalProperties", false}
    };
}

json phase7ExecutionApplyAuditSummarySchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"event_type", nullableStringSchema()},
            {"generated_at_utc", nullableStringSchema()},
            {"actor", nullableStringSchema()},
            {"execution_status", json{{"oneOf", json::array({phase7ExecutionJournalEntryStatusSchema(), json{{"type", "null"}}})}}},
            {"apply_status", json{{"oneOf", json::array({phase7ExecutionJournalAggregateStatusSchema(), json{{"type", "null"}}})}}},
            {"message", nullableStringSchema()}
        }},
        {"required", json::array({
            "event_type",
            "generated_at_utc",
            "actor",
            "execution_status",
            "apply_status",
            "message"
        })},
        {"additionalProperties", false}
    };
}

json phase7ExecutionApplyResultSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"source_artifact", json{{"type", "object"}, {"additionalProperties", true}}},
            {"analysis_artifact", json{{"type", "object"}, {"additionalProperties", true}}},
            {"playbook_artifact", json{{"type", "object"}, {"additionalProperties", true}}},
            {"execution_ledger", json{{"type", "object"}, {"additionalProperties", true}}},
            {"execution_journal", json{{"type", "object"}, {"additionalProperties", true}}},
            {"execution_apply", json{{"type", "object"}, {"additionalProperties", true}}},
            {"mode", stringEnumSchema({tape_phase7::kDefaultPlaybookMode, tape_phase7::kApplyPlaybookMode})},
            {"generated_at_utc", stringSchema()},
            {"initiated_by", stringSchema()},
            {"execution_capability", stringSchema()},
            {"apply_status", phase7ExecutionJournalAggregateStatusSchema()},
            {"execution_policy", json{{"type", "object"}, {"additionalProperties", true}}},
            {"filtered_finding_ids", json{{"type", "array"}, {"items", stringSchema()}}},
            {"entry_count", nonNegativeIntegerSchema()},
            {"execution_summary", phase7ExecutionJournalSummarySchema()},
            {"runtime_recovery_summary", phase7ExecutionRecoverySummarySchema()},
            {"latest_audit_event", phase7ExecutionApplyAuditSummarySchema()},
            {"latest_execution_result_summary", phase7LatestExecutionResultSummarySchema()},
            {"entries", json{{"type", "array"}, {"items", phase7ExecutionApplyEntrySchema()}}},
            {"audit_trail", json{{"type", "array"}, {"items", json{{"type", "object"}, {"additionalProperties", true}}}}},
            {"replay_context", json{{"type", "object"}, {"additionalProperties", true}}}
        }},
        {"required", json::array({
            "source_artifact",
            "analysis_artifact",
            "playbook_artifact",
            "execution_ledger",
            "execution_journal",
            "execution_apply",
            "mode",
            "generated_at_utc",
            "initiated_by",
            "execution_capability",
            "apply_status",
            "execution_policy",
            "filtered_finding_ids",
            "entry_count",
            "execution_summary",
            "runtime_recovery_summary",
            "latest_audit_event",
            "latest_execution_result_summary",
            "entries",
            "audit_trail",
            "replay_context"
        })},
        {"additionalProperties", false}
    };
}

json phase7ExecutionApplyStartResultSchema() {
    json schema = phase7ExecutionApplyResultSchema();
    schema["properties"]["artifact_status"] = stringEnumSchema({"created", "reused"});
    schema["required"].push_back("artifact_status");
    return schema;
}

json phase7ExecutionApplyEventResultSchema() {
    json schema = phase7ExecutionApplyResultSchema();
    schema["properties"]["updated_entry_ids"] = json{{"type", "array"}, {"items", stringSchema()}};
    schema["properties"]["audit_event_id"] = stringSchema();
    schema["required"].push_back("updated_entry_ids");
    schema["required"].push_back("audit_event_id");
    return schema;
}

json phase7ExecutionApplyRowSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"execution_apply", json{{"type", "object"}, {"additionalProperties", true}}},
            {"execution_journal", json{{"type", "object"}, {"additionalProperties", true}}},
            {"execution_ledger", json{{"type", "object"}, {"additionalProperties", true}}},
            {"playbook_artifact", json{{"type", "object"}, {"additionalProperties", true}}},
            {"analysis_artifact", json{{"type", "object"}, {"additionalProperties", true}}},
            {"source_artifact", json{{"type", "object"}, {"additionalProperties", true}}},
            {"mode", stringEnumSchema({tape_phase7::kDefaultPlaybookMode, tape_phase7::kApplyPlaybookMode})},
            {"generated_at_utc", stringSchema()},
            {"apply_status", phase7ExecutionJournalAggregateStatusSchema()},
            {"entry_count", nonNegativeIntegerSchema()},
            {"execution_summary", phase7ExecutionJournalSummarySchema()},
            {"runtime_recovery_summary", phase7ExecutionRecoverySummarySchema()},
            {"latest_audit_event", phase7ExecutionApplyAuditSummarySchema()},
            {"latest_execution_result_summary", phase7LatestExecutionResultSummarySchema()},
            {"replay_context", json{{"type", "object"}, {"additionalProperties", true}}}
        }},
        {"required", json::array({
            "execution_apply",
            "execution_journal",
            "execution_ledger",
            "playbook_artifact",
            "analysis_artifact",
            "source_artifact",
            "mode",
            "generated_at_utc",
            "apply_status",
            "entry_count",
            "execution_summary",
            "runtime_recovery_summary",
            "latest_audit_event",
            "latest_execution_result_summary",
            "replay_context"
        })},
        {"additionalProperties", false}
    };
}

json phase7ExecutionApplyListResultSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"returned_count", nonNegativeIntegerSchema()},
            {"applied_filters", phase7ExecutionApplyFiltersSchema()},
            {"execution_applies", json{{"type", "array"}, {"items", phase7ExecutionApplyRowSchema()}}}
        }},
        {"required", json::array({"returned_count", "applied_filters", "execution_applies"})},
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
        case ToolId::ExportSessionBundle:
            return "Export Session Bundle";
        case ToolId::ExportCaseBundle:
            return "Export Case Bundle";
        case ToolId::VerifyBundle:
            return "Verify Bundle";
        case ToolId::ImportCaseBundle:
            return "Import Case Bundle";
        case ToolId::ListImportedCases:
            return "List Imported Cases";
        case ToolId::ReadSessionQuality:
            return "Read Session Quality";
        case ToolId::ListAnalysisProfiles:
            return "List Analysis Profiles";
        case ToolId::ReadAnalysisProfile:
            return "Read Analysis Profile";
        case ToolId::AnalyzerRun:
            return "Run Analyzer";
        case ToolId::FindingsList:
            return "List Analyzer Findings";
        case ToolId::ListAnalysisArtifacts:
            return "List Analysis Artifacts";
        case ToolId::ReadAnalysisArtifact:
            return "Read Analysis Artifact";
        case ToolId::PlaybookApply:
            return "Apply Guarded Playbook";
        case ToolId::ListPlaybookArtifacts:
            return "List Playbook Artifacts";
        case ToolId::ReadPlaybookArtifact:
            return "Read Playbook Artifact";
        case ToolId::PrepareExecutionLedger:
            return "Prepare Execution Ledger";
        case ToolId::ListExecutionLedgers:
            return "List Execution Ledgers";
        case ToolId::ReadExecutionLedger:
            return "Read Execution Ledger";
        case ToolId::RecordExecutionLedgerReview:
            return "Record Execution Ledger Review";
        case ToolId::StartExecutionJournal:
            return "Start Execution Journal";
        case ToolId::ListExecutionJournals:
            return "List Execution Journals";
        case ToolId::ReadExecutionJournal:
            return "Read Execution Journal";
        case ToolId::DispatchExecutionJournal:
            return "Dispatch Execution Journal";
        case ToolId::RecordExecutionJournalEvent:
            return "Record Execution Journal Event";
        case ToolId::StartExecutionApply:
            return "Start Execution Apply";
        case ToolId::ListExecutionApplies:
            return "List Execution Applies";
        case ToolId::ReadExecutionApply:
            return "Read Execution Apply";
        case ToolId::RecordExecutionApplyEvent:
            return "Record Execution Apply Event";
    }
    return tool.name;
}

json toolAnnotationsForSpec(const ToolSpec& tool) {
    switch (tool.id) {
        case ToolId::ScanSessionReport:
        case ToolId::ScanIncidentReport:
        case ToolId::ScanOrderCaseReport:
        case ToolId::ExportSessionBundle:
        case ToolId::ExportCaseBundle:
        case ToolId::VerifyBundle:
        case ToolId::ImportCaseBundle:
        case ToolId::AnalyzerRun:
        case ToolId::PlaybookApply:
        case ToolId::PrepareExecutionLedger:
        case ToolId::RecordExecutionLedgerReview:
        case ToolId::StartExecutionJournal:
        case ToolId::DispatchExecutionJournal:
        case ToolId::RecordExecutionJournalEvent:
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
        case ToolId::ExportSessionBundle:
        case ToolId::ExportCaseBundle:
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
        case ToolId::VerifyBundle:
            return withSchemaExamples(std::move(schema), {json{{"bundle_path", "/tmp/case-bundle-report-000001.msgpack"}}});
        case ToolId::ImportCaseBundle:
            return withSchemaExamples(std::move(schema), {json{{"bundle_path", "/tmp/case-bundle-report-000001.msgpack"}}});
        case ToolId::ListImportedCases:
            return withSchemaExamples(std::move(schema), {json{{"limit", 20}}});
        case ToolId::ReadSessionQuality:
            return withSchemaExamples(std::move(schema), {json{{"first_session_seq", 1}, {"last_session_seq", 200}}});
        case ToolId::ListAnalysisProfiles:
            return withSchemaExamples(std::move(schema), {json::object()});
        case ToolId::ReadAnalysisProfile:
            return withSchemaExamples(std::move(schema), {json{{"analysis_profile", "phase7.trace_fill_integrity.v1"}}});
        case ToolId::AnalyzerRun:
            return withSchemaExamples(std::move(schema), {json{{"case_bundle_path", "/tmp/case-bundle-report-000001.msgpack"}}});
        case ToolId::FindingsList:
            return withSchemaExamples(std::move(schema), {
                json{{"analysis_artifact_id", "phase7-analysis-1234abcd"}},
                json{{"analysis_artifact_id", "phase7-analysis-1234abcd"}, {"category", "trace_integrity"}}
            });
        case ToolId::ListAnalysisArtifacts:
            return withSchemaExamples(std::move(schema), {
                json{{"limit", 10}},
                json{{"source_artifact_id", "case-bundle:report:1"}}
            });
        case ToolId::ReadAnalysisArtifact:
            return withSchemaExamples(std::move(schema), {json{{"analysis_artifact_id", "phase7-analysis-1234abcd"}}});
        case ToolId::PlaybookApply:
            return withSchemaExamples(std::move(schema), {
                json{{"analysis_artifact_id", "phase7-analysis-1234abcd"}},
                json{{"analysis_artifact_id", "phase7-analysis-1234abcd"}, {"category", "trace_integrity"}},
                json{{"analysis_artifact_id", "phase7-analysis-1234abcd"}, {"mode", "apply"}}
            });
        case ToolId::ListPlaybookArtifacts:
            return withSchemaExamples(std::move(schema), {
                json{{"limit", 10}},
                json{{"analysis_artifact_id", "phase7-analysis-1234abcd"}}
            });
        case ToolId::ReadPlaybookArtifact:
            return withSchemaExamples(std::move(schema), {json{{"playbook_artifact_id", "phase7-playbook-1234abcd"}}});
        case ToolId::PrepareExecutionLedger:
            return withSchemaExamples(std::move(schema), {json{{"playbook_artifact_id", "phase7-playbook-1234abcd"}}});
        case ToolId::ListExecutionLedgers:
            return withSchemaExamples(std::move(schema), {
                json{{"limit", 10}},
                json{{"playbook_artifact_id", "phase7-playbook-1234abcd"}},
                json{{"ledger_status", "review_blocked"}, {"sort_by", "attention_desc"}},
                json{{"ledger_status", "review_waiting_approval"}, {"sort_by", "attention_desc"}},
                json{{"ledger_status", "ready_for_execution"}, {"sort_by", "attention_desc"}}
            });
        case ToolId::ReadExecutionLedger:
            return withSchemaExamples(std::move(schema), {json{{"execution_ledger_artifact_id", "phase7-ledger-1234abcd"}}});
        case ToolId::RecordExecutionLedgerReview:
            return withSchemaExamples(std::move(schema), {
                json{{"execution_ledger_artifact_id", "phase7-ledger-1234abcd"},
                     {"entry_ids", json::array({"phase7-ledger-entry-1111"})},
                     {"review_status", tape_phase7::kLedgerEntryStatusApproved},
                     {"actor", "tapescope"},
                     {"comment", "Reviewed and safe for deferred follow-up."}}
            });
        case ToolId::StartExecutionJournal:
            return withSchemaExamples(std::move(schema), {
                json{{"execution_ledger_artifact_id", "phase7-ledger-1234abcd"},
                     {"actor", "execution-operator"},
                     {"execution_capability", "phase7.execution_operator.v1"}}
            });
        case ToolId::ListExecutionJournals:
            return withSchemaExamples(std::move(schema), {
                json{{"limit", 10}},
                json{{"execution_ledger_artifact_id", "phase7-ledger-1234abcd"}},
                json{{"journal_status", "execution_in_progress"}, {"sort_by", "attention_desc"}}
            });
        case ToolId::ReadExecutionJournal:
            return withSchemaExamples(std::move(schema), {json{{"execution_journal_artifact_id", "phase7-journal-1234abcd"}}});
        case ToolId::DispatchExecutionJournal:
            return withSchemaExamples(std::move(schema), {
                json{{"execution_journal_artifact_id", "phase7-journal-1234abcd"},
                     {"actor", "execution-operator"},
                     {"execution_capability", "phase7.execution_operator.v1"}},
                json{{"execution_journal_artifact_id", "phase7-journal-1234abcd"},
                     {"entry_ids", json::array({"phase7-journal-entry-1111"})},
                     {"actor", "execution-operator"},
                     {"execution_capability", "phase7.execution_operator.v1"},
                     {"comment", "Dispatching the selected execution step."}}
            });
        case ToolId::RecordExecutionJournalEvent:
            return withSchemaExamples(std::move(schema), {
                json{{"execution_journal_artifact_id", "phase7-journal-1234abcd"},
                     {"entry_ids", json::array({"phase7-journal-entry-1111"})},
                     {"execution_status", tape_phase7::kExecutionEntryStatusSubmitted},
                     {"actor", "execution-operator"},
                     {"comment", "Submitted to the controlled execution path."}},
                json{{"execution_journal_artifact_id", "phase7-journal-1234abcd"},
                     {"entry_ids", json::array({"phase7-journal-entry-1111"})},
                     {"execution_status", tape_phase7::kExecutionEntryStatusFailed},
                     {"actor", "execution-operator"},
                     {"comment", "Submission rejected by execution gate."},
                     {"failure_code", "gate_rejected"},
                     {"failure_message", "Pre-trade limit check rejected the action."}}
            });
        case ToolId::StartExecutionApply:
            return withSchemaExamples(std::move(schema), {
                json{{"execution_journal_artifact_id", "phase7-journal-1234abcd"},
                     {"actor", "execution-operator"},
                     {"execution_capability", "phase7.execution_operator.v1"}},
                json{{"execution_journal_artifact_id", "phase7-journal-1234abcd"},
                     {"entry_ids", json::array({"phase7-journal-entry-1111"})},
                     {"actor", "execution-operator"},
                     {"execution_capability", "phase7.execution_operator.v1"},
                     {"comment", "Creating a durable apply artifact for the submitted entry."}}
            });
        case ToolId::ListExecutionApplies:
            return withSchemaExamples(std::move(schema), {
                json{{"limit", 10}},
                json{{"execution_journal_artifact_id", "phase7-journal-1234abcd"}},
                json{{"apply_status", "execution_in_progress"}, {"sort_by", "attention_desc"}}
            });
        case ToolId::ReadExecutionApply:
            return withSchemaExamples(std::move(schema), {json{{"execution_apply_artifact_id", "phase7-apply-1234abcd"}}});
        case ToolId::RecordExecutionApplyEvent:
            return withSchemaExamples(std::move(schema), {
                json{{"execution_apply_artifact_id", "phase7-apply-1234abcd"},
                     {"entry_ids", json::array({"phase7-apply-entry-1111"})},
                     {"execution_status", tape_phase7::kExecutionEntryStatusSucceeded},
                     {"actor", "execution-operator"},
                     {"comment", "Execution completed successfully."}},
                json{{"execution_apply_artifact_id", "phase7-apply-1234abcd"},
                     {"entry_ids", json::array({"phase7-apply-entry-1111"})},
                     {"execution_status", tape_phase7::kExecutionEntryStatusFailed},
                     {"actor", "execution-operator"},
                     {"comment", "Execution failed after submission."},
                     {"failure_code", "venue_reject"},
                     {"failure_message", "Exchange rejected the submitted action."}}
            });
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
        },
        {
            "analyze_bundle_with_phase7",
            "Analyze Bundle With Phase 7",
            "Guide an agent through running the local Phase 7 analyzer against a portable session/case bundle and persisting findings.",
            {
                {"case_bundle_path", "Portable Phase 6 case-bundle path.", false},
                {"report_bundle_path", "Portable Phase 6 session/report bundle path.", false},
                {"analysis_profile", "Optional analysis profile id.", false}
            }
        },
        {
            "review_phase7_findings",
            "Review Phase 7 Findings",
            "Guide an agent through reopening a stored Phase 7 analysis artifact, summarizing findings, and planning follow-up actions.",
            {
                {"analysis_artifact_id", "Stored Phase 7 analysis artifact id.", true}
            }
        },
        {
            "review_phase7_playbook",
            "Review Phase 7 Playbook",
            "Guide an agent through reopening a stored Phase 7 playbook artifact and converting it into an investigation plan.",
            {
                {"playbook_artifact_id", "Stored Phase 7 playbook artifact id.", true}
            }
        },
        {
            "review_phase7_execution_ledger",
            "Review Phase 7 Execution Ledger",
            "Guide an agent through reopening a stored Phase 7 execution/audit ledger while live apply remains deferred.",
            {
                {"execution_ledger_artifact_id", "Stored Phase 7 execution ledger artifact id.", true}
            }
        },
        {
            "review_phase7_execution_journal",
            "Review Phase 7 Execution Journal",
            "Guide an agent through reopening a stored Phase 7 execution journal and preserving the append-only execution audit trail.",
            {
                {"execution_journal_artifact_id", "Stored Phase 7 execution journal artifact id.", true}
            }
        },
        {
            "review_phase7_execution_apply",
            "Review Phase 7 Execution Apply",
            "Guide an agent through reopening a stored Phase 7 execution-apply artifact and reconciling it with the linked execution journal.",
            {
                {"execution_apply_artifact_id", "Stored Phase 7 execution apply artifact id.", true}
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
    if (prompt.name == "analyze_bundle_with_phase7") {
        const std::string caseBundlePath = promptStringArg(args, "case_bundle_path");
        const std::string reportBundlePath = promptStringArg(args, "report_bundle_path");
        const std::string analysisProfile = promptStringArg(args, "analysis_profile");
        const std::string bundlePath = !caseBundlePath.empty() ? caseBundlePath : reportBundlePath;
        const std::string bundleKey = !caseBundlePath.empty() ? "case_bundle_path" : "report_bundle_path";
        std::ostringstream out;
        out << "Run `tapescript_analyzer_run` against "
            << (!bundlePath.empty() ? ("`" + bundlePath + "`") : "the provided portable bundle")
            << " using `" << bundleKey << "`";
        if (!analysisProfile.empty()) {
            out << " and `analysis_profile=\"" << analysisProfile << "\"`";
        }
        out << ". Then call `tapescript_findings_list` on the returned `analysis_artifact_id`, reopen the durable analysis resource with `resources/read` on `tape://phase7/analysis/<analysis_artifact_id>`, and if follow-up planning is needed call `tapescript_playbook_apply` in `dry_run` mode. Return: analyzer profile, durable artifact ids, ranked findings, replay context, and the best next investigation actions.";
        return out.str();
    }
    if (prompt.name == "review_phase7_findings") {
        const std::string analysisArtifactId = promptStringArg(args, "analysis_artifact_id");
        std::ostringstream out;
        out << "Review stored Phase 7 analysis artifact `"
            << (analysisArtifactId.empty() ? "<analysis_artifact_id>" : analysisArtifactId)
            << "`. Start with `tapescript_findings_list`, then reopen the persisted JSON and markdown resources with `resources/read` on `tape://phase7/analysis/"
            << (analysisArtifactId.empty() ? "<analysis_artifact_id>" : analysisArtifactId)
            << "` and `tape://phase7/analysis/"
            << (analysisArtifactId.empty() ? "<analysis_artifact_id>" : analysisArtifactId)
            << "/markdown`. If the findings warrant action planning, call `tapescript_playbook_apply` in `dry_run` mode. Return: top findings, confidence, replay context, and recommended next tools.";
        return out.str();
    }
    if (prompt.name == "review_phase7_playbook") {
        const std::string playbookArtifactId = promptStringArg(args, "playbook_artifact_id");
        std::ostringstream out;
        out << "Review stored Phase 7 playbook artifact `"
            << (playbookArtifactId.empty() ? "<playbook_artifact_id>" : playbookArtifactId)
            << "`. Reopen it with `resources/read` on `tape://phase7/playbook/"
            << (playbookArtifactId.empty() ? "<playbook_artifact_id>" : playbookArtifactId)
            << "` and `tape://phase7/playbook/"
            << (playbookArtifactId.empty() ? "<playbook_artifact_id>" : playbookArtifactId)
            << "/markdown`, then follow the referenced analysis artifact back through `tapescript_findings_list`. Return: action order, affected findings, suggested tools, and whether any step still needs manual confirmation.";
        return out.str();
    }
    if (prompt.name == "review_phase7_execution_ledger") {
        const std::string ledgerArtifactId = promptStringArg(args, "execution_ledger_artifact_id");
        std::ostringstream out;
        out << "Review stored Phase 7 execution ledger `"
            << (ledgerArtifactId.empty() ? "<execution_ledger_artifact_id>" : ledgerArtifactId)
            << "`. Reopen it with `resources/read` on `tape://phase7/ledger/"
            << (ledgerArtifactId.empty() ? "<execution_ledger_artifact_id>" : ledgerArtifactId)
            << "` and `tape://phase7/ledger/"
            << (ledgerArtifactId.empty() ? "<execution_ledger_artifact_id>" : ledgerArtifactId)
            << "/markdown`, then follow the linked playbook and analysis artifacts with `tapescript_read_playbook_artifact` and `tapescript_findings_list`. If review decisions need to be captured, call `tapescript_record_execution_ledger_review`. Keep apply deferred and treat the ledger as an audit/review surface only. Return: review status, blocked actions, required confirmations, and the next safe investigation steps.";
        return out.str();
    }
    if (prompt.name == "review_phase7_execution_journal") {
        const std::string journalArtifactId = promptStringArg(args, "execution_journal_artifact_id");
        std::ostringstream out;
        out << "Review stored Phase 7 execution journal `"
            << (journalArtifactId.empty() ? "<execution_journal_artifact_id>" : journalArtifactId)
            << "`. Reopen it with `resources/read` on `tape://phase7/journal/"
            << (journalArtifactId.empty() ? "<execution_journal_artifact_id>" : journalArtifactId)
            << "` and `tape://phase7/journal/"
            << (journalArtifactId.empty() ? "<execution_journal_artifact_id>" : journalArtifactId)
            << "/markdown`, then follow the linked ledger/playbook/analysis artifacts. If action lifecycle changes need to be recorded, call `tapescript_record_execution_journal_event` with explicit `entry_ids`, `execution_status`, `actor`, and failure details when needed. Return: execution status, queued/submitted/terminal counts, failed actions, and the safest next execution step.";
        return out.str();
    }
    if (prompt.name == "review_phase7_execution_apply") {
        const std::string applyArtifactId = promptStringArg(args, "execution_apply_artifact_id");
        std::ostringstream out;
        out << "Review stored Phase 7 execution apply `"
            << (applyArtifactId.empty() ? "<execution_apply_artifact_id>" : applyArtifactId)
            << "`. Reopen it with `resources/read` on `tape://phase7/apply/"
            << (applyArtifactId.empty() ? "<execution_apply_artifact_id>" : applyArtifactId)
            << "` and `tape://phase7/apply/"
            << (applyArtifactId.empty() ? "<execution_apply_artifact_id>" : applyArtifactId)
            << "/markdown`, compare it with the linked execution journal, and record outcome changes only through `tapescript_record_execution_apply_event` so the apply artifact and journal stay synchronized. Return: apply status, entry-level outcomes, unresolved items, and the safest next execution step.";
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
    CaseArtifactJsonBundle,
    Phase7AnalysisJson,
    Phase7AnalysisMarkdown,
    Phase7PlaybookJson,
    Phase7PlaybookMarkdown,
    Phase7ExecutionLedgerJson,
    Phase7ExecutionLedgerMarkdown,
    Phase7ExecutionJournalJson,
    Phase7ExecutionJournalMarkdown,
    Phase7ExecutionApplyJson,
    Phase7ExecutionApplyMarkdown
};

struct ParsedResourceUri {
    ResourceKind kind = ResourceKind::Unknown;
    std::uint64_t reportId = 0;
    std::string artifactId;
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

std::string phase7AnalysisUri(std::string_view artifactId) {
    return "tape://phase7/analysis/" + std::string(artifactId);
}

std::string phase7AnalysisMarkdownUri(std::string_view artifactId) {
    return "tape://phase7/analysis/" + std::string(artifactId) + "/markdown";
}

std::string phase7PlaybookUri(std::string_view artifactId) {
    return "tape://phase7/playbook/" + std::string(artifactId);
}

std::string phase7PlaybookMarkdownUri(std::string_view artifactId) {
    return "tape://phase7/playbook/" + std::string(artifactId) + "/markdown";
}

std::string phase7ExecutionLedgerUri(std::string_view artifactId) {
    return "tape://phase7/ledger/" + std::string(artifactId);
}

std::string phase7ExecutionLedgerMarkdownUri(std::string_view artifactId) {
    return "tape://phase7/ledger/" + std::string(artifactId) + "/markdown";
}

std::string phase7ExecutionJournalUri(std::string_view artifactId) {
    return "tape://phase7/journal/" + std::string(artifactId);
}

std::string phase7ExecutionJournalMarkdownUri(std::string_view artifactId) {
    return "tape://phase7/journal/" + std::string(artifactId) + "/markdown";
}

std::string phase7ExecutionApplyUri(std::string_view artifactId) {
    return "tape://phase7/apply/" + std::string(artifactId);
}

std::string phase7ExecutionApplyMarkdownUri(std::string_view artifactId) {
    return "tape://phase7/apply/" + std::string(artifactId) + "/markdown";
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
    constexpr std::string_view kPhase7AnalysisPrefix = "tape://phase7/analysis/";
    constexpr std::string_view kPhase7PlaybookPrefix = "tape://phase7/playbook/";
    constexpr std::string_view kPhase7ExecutionLedgerPrefix = "tape://phase7/ledger/";
    constexpr std::string_view kPhase7ExecutionJournalPrefix = "tape://phase7/journal/";
    constexpr std::string_view kPhase7ExecutionApplyPrefix = "tape://phase7/apply/";

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
    if (text.rfind(std::string(kPhase7AnalysisPrefix), 0) == 0) {
        const std::size_t formatPos = text.find('/', kPhase7AnalysisPrefix.size());
        parsed.artifactId = text.substr(
            kPhase7AnalysisPrefix.size(),
            (formatPos == std::string::npos ? text.size() : formatPos) - kPhase7AnalysisPrefix.size());
        if (formatPos == std::string::npos) {
            parsed.kind = parsed.artifactId.empty() ? ResourceKind::Unknown : ResourceKind::Phase7AnalysisJson;
        } else if (text.substr(formatPos + 1) == "markdown") {
            parsed.kind = parsed.artifactId.empty() ? ResourceKind::Unknown : ResourceKind::Phase7AnalysisMarkdown;
        }
        return parsed;
    }
    if (text.rfind(std::string(kPhase7PlaybookPrefix), 0) == 0) {
        const std::size_t formatPos = text.find('/', kPhase7PlaybookPrefix.size());
        parsed.artifactId = text.substr(
            kPhase7PlaybookPrefix.size(),
            (formatPos == std::string::npos ? text.size() : formatPos) - kPhase7PlaybookPrefix.size());
        if (formatPos == std::string::npos) {
            parsed.kind = parsed.artifactId.empty() ? ResourceKind::Unknown : ResourceKind::Phase7PlaybookJson;
        } else if (text.substr(formatPos + 1) == "markdown") {
            parsed.kind = parsed.artifactId.empty() ? ResourceKind::Unknown : ResourceKind::Phase7PlaybookMarkdown;
        }
        return parsed;
    }
    if (text.rfind(std::string(kPhase7ExecutionLedgerPrefix), 0) == 0) {
        const std::size_t formatPos = text.find('/', kPhase7ExecutionLedgerPrefix.size());
        parsed.artifactId = text.substr(
            kPhase7ExecutionLedgerPrefix.size(),
            (formatPos == std::string::npos ? text.size() : formatPos) - kPhase7ExecutionLedgerPrefix.size());
        if (formatPos == std::string::npos) {
            parsed.kind = parsed.artifactId.empty() ? ResourceKind::Unknown : ResourceKind::Phase7ExecutionLedgerJson;
        } else if (text.substr(formatPos + 1) == "markdown") {
            parsed.kind = parsed.artifactId.empty() ? ResourceKind::Unknown : ResourceKind::Phase7ExecutionLedgerMarkdown;
        }
        return parsed;
    }
    if (text.rfind(std::string(kPhase7ExecutionJournalPrefix), 0) == 0) {
        const std::size_t formatPos = text.find('/', kPhase7ExecutionJournalPrefix.size());
        parsed.artifactId = text.substr(
            kPhase7ExecutionJournalPrefix.size(),
            (formatPos == std::string::npos ? text.size() : formatPos) - kPhase7ExecutionJournalPrefix.size());
        if (formatPos == std::string::npos) {
            parsed.kind = parsed.artifactId.empty() ? ResourceKind::Unknown : ResourceKind::Phase7ExecutionJournalJson;
        } else if (text.substr(formatPos + 1) == "markdown") {
            parsed.kind = parsed.artifactId.empty() ? ResourceKind::Unknown : ResourceKind::Phase7ExecutionJournalMarkdown;
        }
        return parsed;
    }
    if (text.rfind(std::string(kPhase7ExecutionApplyPrefix), 0) == 0) {
        const std::size_t formatPos = text.find('/', kPhase7ExecutionApplyPrefix.size());
        parsed.artifactId = text.substr(
            kPhase7ExecutionApplyPrefix.size(),
            (formatPos == std::string::npos ? text.size() : formatPos) - kPhase7ExecutionApplyPrefix.size());
        if (formatPos == std::string::npos) {
            parsed.kind = parsed.artifactId.empty() ? ResourceKind::Unknown : ResourceKind::Phase7ExecutionApplyJson;
        } else if (text.substr(formatPos + 1) == "markdown") {
            parsed.kind = parsed.artifactId.empty() ? ResourceKind::Unknown : ResourceKind::Phase7ExecutionApplyMarkdown;
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

json bundlePathInputSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"bundle_path", stringSchema()}
        }},
        {"required", json::array({"bundle_path"})},
        {"additionalProperties", false}
    };
}

json phase7AnalyzerInputSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"case_bundle_path", stringSchema()},
            {"report_bundle_path", stringSchema()},
            {"case_manifest_path", stringSchema()},
            {"report_manifest_path", stringSchema()},
            {"analysis_profile", stringSchema()}
        }},
        {"oneOf", json::array({
            json{{"required", json::array({"case_bundle_path"})}},
            json{{"required", json::array({"report_bundle_path"})}},
            json{{"required", json::array({"case_manifest_path"})}},
            json{{"required", json::array({"report_manifest_path"})}}
        })},
        {"additionalProperties", false}
    };
}

json phase7AnalysisRefInputSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"analysis_manifest_path", stringSchema()},
            {"analysis_artifact_id", stringSchema()}
        }},
        {"oneOf", json::array({
            json{{"required", json::array({"analysis_manifest_path"})}},
            json{{"required", json::array({"analysis_artifact_id"})}}
        })},
        {"additionalProperties", false}
    };
}

json phase7FindingSelectionInputSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"analysis_manifest_path", stringSchema()},
            {"analysis_artifact_id", stringSchema()},
            {"finding_ids", json{{"type", "array"}, {"items", stringSchema()}}},
            {"minimum_severity", stringEnumSchema({"info", "warning", "error", "critical"})},
            {"category", stringSchema()},
            {"limit", positiveIntegerSchema()}
        }},
        {"oneOf", json::array({
            json{{"required", json::array({"analysis_manifest_path"})}},
            json{{"required", json::array({"analysis_artifact_id"})}}
        })},
        {"additionalProperties", false}
    };
}

json phase7PlaybookRefInputSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"playbook_manifest_path", stringSchema()},
            {"playbook_artifact_id", stringSchema()}
        }},
        {"oneOf", json::array({
            json{{"required", json::array({"playbook_manifest_path"})}},
            json{{"required", json::array({"playbook_artifact_id"})}}
        })},
        {"additionalProperties", false}
    };
}

json phase7AnalysisInventoryInputSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"limit", positiveIntegerSchema()},
            {"source_artifact_id", stringSchema()},
            {"analysis_profile", stringSchema()}
        }},
        {"additionalProperties", false}
    };
}

json phase7ProfileRefInputSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"analysis_profile", stringSchema()}
        }},
        {"required", json::array({"analysis_profile"})},
        {"additionalProperties", false}
    };
}

json phase7PlaybookInventoryInputSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"limit", positiveIntegerSchema()},
            {"analysis_artifact_id", stringSchema()},
            {"source_artifact_id", stringSchema()},
            {"mode", stringEnumSchema({tape_phase7::kDefaultPlaybookMode, tape_phase7::kApplyPlaybookMode})}
        }},
        {"additionalProperties", false}
    };
}

json phase7ExecutionLedgerRefInputSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"execution_ledger_manifest_path", stringSchema()},
            {"execution_ledger_artifact_id", stringSchema()}
        }},
        {"oneOf", json::array({
            json{{"required", json::array({"execution_ledger_manifest_path"})}},
            json{{"required", json::array({"execution_ledger_artifact_id"})}}
        })},
        {"additionalProperties", false}
    };
}

json phase7ExecutionLedgerInputSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"playbook_manifest_path", stringSchema()},
            {"playbook_artifact_id", stringSchema()}
        }},
        {"oneOf", json::array({
            json{{"required", json::array({"playbook_manifest_path"})}},
            json{{"required", json::array({"playbook_artifact_id"})}}
        })},
        {"additionalProperties", false}
    };
}

json phase7ExecutionLedgerInventoryInputSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"limit", positiveIntegerSchema()},
            {"playbook_artifact_id", stringSchema()},
            {"analysis_artifact_id", stringSchema()},
            {"source_artifact_id", stringSchema()},
            {"ledger_status", stringEnumSchema({
                tape_phase7::kDefaultLedgerStatus,
                tape_phase7::kLedgerStatusInProgress,
                tape_phase7::kLedgerStatusBlocked,
                tape_phase7::kLedgerStatusNeedsInformation,
                tape_phase7::kLedgerStatusCompleted,
                tape_phase7::kLedgerStatusWaitingApproval,
                tape_phase7::kLedgerStatusReadyForExecution
            })},
            {"sort_by", stringEnumSchema({"generated_at_desc", "attention_desc", "source_artifact_asc"})}
        }},
        {"additionalProperties", false}
    };
}

json phase7ExecutionLedgerReviewInputSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"execution_ledger_manifest_path", stringSchema()},
            {"execution_ledger_artifact_id", stringSchema()},
            {"entry_ids", json{{"type", "array"}, {"items", stringSchema()}, {"minItems", 1}}},
            {"review_status", phase7ExecutionLedgerReviewStatusSchema()},
            {"actor", stringSchema()},
            {"comment", stringSchema()}
        }},
        {"oneOf", json::array({
            json{{"required", json::array({"execution_ledger_manifest_path", "entry_ids", "review_status", "actor"})}},
            json{{"required", json::array({"execution_ledger_artifact_id", "entry_ids", "review_status", "actor"})}}
        })},
        {"additionalProperties", false}
    };
}

json phase7ExecutionJournalRefInputSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"execution_journal_manifest_path", stringSchema()},
            {"execution_journal_artifact_id", stringSchema()}
        }},
        {"oneOf", json::array({
            json{{"required", json::array({"execution_journal_manifest_path"})}},
            json{{"required", json::array({"execution_journal_artifact_id"})}}
        })},
        {"additionalProperties", false}
    };
}

json phase7ExecutionJournalInputSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"execution_ledger_manifest_path", stringSchema()},
            {"execution_ledger_artifact_id", stringSchema()},
            {"actor", stringSchema()},
            {"execution_capability", stringSchema()}
        }},
        {"oneOf", json::array({
            json{{"required", json::array({"execution_ledger_manifest_path", "actor", "execution_capability"})}},
            json{{"required", json::array({"execution_ledger_artifact_id", "actor", "execution_capability"})}}
        })},
        {"additionalProperties", false}
    };
}

json phase7ExecutionJournalInventoryInputSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"limit", positiveIntegerSchema()},
            {"execution_ledger_artifact_id", stringSchema()},
            {"playbook_artifact_id", stringSchema()},
            {"analysis_artifact_id", stringSchema()},
            {"source_artifact_id", stringSchema()},
            {"journal_status", phase7ExecutionJournalAggregateStatusSchema()},
            {"recovery_state", phase7ExecutionRecoveryStateSchema()},
            {"sort_by", stringEnumSchema({"generated_at_desc", "attention_desc", "source_artifact_asc"})}
        }},
        {"additionalProperties", false}
    };
}

json phase7ExecutionJournalDispatchInputSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"execution_journal_manifest_path", stringSchema()},
            {"execution_journal_artifact_id", stringSchema()},
            {"entry_ids", json{{"type", "array"}, {"items", stringSchema()}, {"minItems", 1}}},
            {"actor", stringSchema()},
            {"execution_capability", stringSchema()},
            {"comment", stringSchema()}
        }},
        {"oneOf", json::array({
            json{{"required", json::array({"execution_journal_manifest_path", "actor", "execution_capability"})}},
            json{{"required", json::array({"execution_journal_artifact_id", "actor", "execution_capability"})}}
        })},
        {"additionalProperties", false}
    };
}

json phase7ExecutionJournalEventInputSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"execution_journal_manifest_path", stringSchema()},
            {"execution_journal_artifact_id", stringSchema()},
            {"entry_ids", json{{"type", "array"}, {"items", stringSchema()}, {"minItems", 1}}},
            {"execution_status", phase7ExecutionJournalEntryStatusSchema()},
            {"actor", stringSchema()},
            {"comment", stringSchema()},
            {"failure_code", stringSchema()},
            {"failure_message", stringSchema()}
        }},
        {"oneOf", json::array({
            json{{"required", json::array({"execution_journal_manifest_path", "entry_ids", "execution_status", "actor"})}},
            json{{"required", json::array({"execution_journal_artifact_id", "entry_ids", "execution_status", "actor"})}}
        })},
        {"additionalProperties", false}
    };
}

json phase7ExecutionApplyRefInputSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"execution_apply_manifest_path", stringSchema()},
            {"execution_apply_artifact_id", stringSchema()}
        }},
        {"oneOf", json::array({
            json{{"required", json::array({"execution_apply_manifest_path"})}},
            json{{"required", json::array({"execution_apply_artifact_id"})}}
        })},
        {"additionalProperties", false}
    };
}

json phase7ExecutionApplyInputSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"execution_journal_manifest_path", stringSchema()},
            {"execution_journal_artifact_id", stringSchema()},
            {"entry_ids", json{{"type", "array"}, {"items", stringSchema()}, {"minItems", 1}}},
            {"actor", stringSchema()},
            {"execution_capability", stringSchema()},
            {"comment", stringSchema()}
        }},
        {"oneOf", json::array({
            json{{"required", json::array({"execution_journal_manifest_path", "actor", "execution_capability"})}},
            json{{"required", json::array({"execution_journal_artifact_id", "actor", "execution_capability"})}}
        })},
        {"additionalProperties", false}
    };
}

json phase7ExecutionApplyInventoryInputSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"limit", positiveIntegerSchema()},
            {"execution_journal_artifact_id", stringSchema()},
            {"execution_ledger_artifact_id", stringSchema()},
            {"playbook_artifact_id", stringSchema()},
            {"analysis_artifact_id", stringSchema()},
            {"source_artifact_id", stringSchema()},
            {"apply_status", phase7ExecutionJournalAggregateStatusSchema()},
            {"recovery_state", phase7ExecutionRecoveryStateSchema()},
            {"sort_by", stringEnumSchema({"generated_at_desc", "attention_desc", "source_artifact_asc"})}
        }},
        {"additionalProperties", false}
    };
}

json phase7ExecutionApplyEventInputSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"execution_apply_manifest_path", stringSchema()},
            {"execution_apply_artifact_id", stringSchema()},
            {"entry_ids", json{{"type", "array"}, {"items", stringSchema()}, {"minItems", 1}}},
            {"execution_status", phase7ExecutionJournalEntryStatusSchema()},
            {"actor", stringSchema()},
            {"comment", stringSchema()},
            {"failure_code", stringSchema()},
            {"failure_message", stringSchema()}
        }},
        {"oneOf", json::array({
            json{{"required", json::array({"execution_apply_manifest_path", "entry_ids", "execution_status", "actor"})}},
            json{{"required", json::array({"execution_apply_artifact_id", "entry_ids", "execution_status", "actor"})}}
        })},
        {"additionalProperties", false}
    };
}

json phase7AnalysisProfileSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"analysis_profile", stringSchema()},
            {"title", stringSchema()},
            {"summary", stringSchema()},
            {"default_profile", booleanSchema()},
            {"supported_source_bundle_types", json{{"type", "array"}, {"items", stringSchema()}}},
            {"finding_categories", json{{"type", "array"}, {"items", stringSchema()}}}
        }},
        {"required", json::array({
            "analysis_profile",
            "title",
            "summary",
            "default_profile",
            "supported_source_bundle_types",
            "finding_categories"
        })},
        {"additionalProperties", false}
    };
}

json phase7AnalysisProfileListResultSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"returned_count", nonNegativeIntegerSchema()},
            {"analysis_profiles", json{{"type", "array"}, {"items", phase7AnalysisProfileSchema()}}}
        }},
        {"required", json::array({"returned_count", "analysis_profiles"})},
        {"additionalProperties", false}
    };
}

json phase7PlaybookInputSchema() {
    return json{
        {"type", "object"},
        {"properties", {
            {"analysis_manifest_path", stringSchema()},
            {"analysis_artifact_id", stringSchema()},
            {"finding_ids", json{{"type", "array"}, {"items", stringSchema()}}},
            {"minimum_severity", stringEnumSchema({"info", "warning", "error", "critical"})},
            {"category", stringSchema()},
            {"limit", positiveIntegerSchema()},
            {"mode", stringEnumSchema({tape_phase7::kDefaultPlaybookMode, tape_phase7::kApplyPlaybookMode})}
        }},
        {"oneOf", json::array({
            json{{"required", json::array({"analysis_manifest_path"})}},
            json{{"required", json::array({"analysis_artifact_id"})}}
        })},
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
        {ToolId::ExportSessionBundle,
         "tapescript_export_session_bundle",
         "Export a durable session report as a portable Phase 6 bundle.",
         idInputSchema("report_id", true, false, false),
         bundleExportResultSchema(),
         "phase6.bundle-export.v1",
         tape_engine::QueryOperation::ExportSessionBundle,
         true},
        {ToolId::ExportCaseBundle,
         "tapescript_export_case_bundle",
         "Export a durable case report as a portable Phase 6 bundle.",
         idInputSchema("report_id", true, false, false),
         bundleExportResultSchema(),
         "phase6.bundle-export.v1",
         tape_engine::QueryOperation::ExportCaseBundle,
         true},
        {ToolId::VerifyBundle,
         "tapescript_verify_bundle",
         "Verify a portable Phase 6 bundle, including importability and duplicate-import status.",
         bundlePathInputSchema(),
         bundleVerifyResultSchema(),
         "phase6.bundle-verify.v1",
         tape_engine::QueryOperation::VerifyBundle,
         true},
        {ToolId::ImportCaseBundle,
         "tapescript_import_case_bundle",
         "Import a portable case bundle into the local engine inventory.",
         bundlePathInputSchema(),
         bundleImportResultSchema(),
         "phase6.bundle-import.v1",
         tape_engine::QueryOperation::ImportCaseBundle,
         true},
        {ToolId::ListImportedCases,
         "tapescript_list_imported_cases",
         "List case bundles imported into the local engine inventory.",
         listInputSchema(),
         listRowsResultSchema("imported_cases", importedCaseRowSchema()),
         "phase6.imported-case-list.v1",
         tape_engine::QueryOperation::ListImportedCases},
        {ToolId::ReadSessionQuality,
         "tapescript_read_session_quality",
         "Read data-quality and provenance scoring for a session_seq range.",
         sessionRangeInputSchema(true, false),
         qualityResultSchema(),
         "phase5.session-quality.v1",
         tape_engine::QueryOperation::ReadSessionQuality},
        {ToolId::ListAnalysisProfiles,
         "tapescript_list_analysis_profiles",
         "List supported local Phase 7 analyzer profiles.",
         json{
             {"type", "object"},
             {"properties", json::object()},
             {"additionalProperties", false}
         },
         phase7AnalysisProfileListResultSchema(),
         "phase7.analysis-profile-list.v1",
         tape_engine::QueryOperation::Unknown,
         false,
         kPhase7ContractVersion,
         "phase7_list_analysis_profiles_local"},
        {ToolId::ReadAnalysisProfile,
         "tapescript_read_analysis_profile",
         "Read one supported local Phase 7 analyzer profile by id.",
         phase7ProfileRefInputSchema(),
         phase7AnalysisProfileSchema(),
         "phase7.analysis-profile-read.v1",
         tape_engine::QueryOperation::Unknown,
         false,
         kPhase7ContractVersion,
         "phase7_read_analysis_profile_local"},
        {ToolId::AnalyzerRun,
         "tapescript_analyzer_run",
         "Run a local Phase 7 analyzer pass against a portable session or case bundle.",
         phase7AnalyzerInputSchema(),
         phase7AnalyzerRunResultSchema(),
         "phase7.analysis-run.v1",
         tape_engine::QueryOperation::Unknown,
         true,
         kPhase7ContractVersion,
         "phase7_analyzer_run_local"},
        {ToolId::FindingsList,
         "tapescript_findings_list",
         "List findings from a stored Phase 7 analyzer artifact.",
         phase7FindingSelectionInputSchema(),
         phase7FindingsListResultSchema(),
         "phase7.findings-list.v1",
         tape_engine::QueryOperation::Unknown,
         false,
         kPhase7ContractVersion,
         "phase7_findings_list_local"},
        {ToolId::ListAnalysisArtifacts,
         "tapescript_list_analysis_artifacts",
         "List persisted local Phase 7 analyzer artifacts.",
         phase7AnalysisInventoryInputSchema(),
         phase7AnalysisArtifactListResultSchema(),
         "phase7.analysis-artifact-list.v1",
         tape_engine::QueryOperation::Unknown,
         false,
         kPhase7ContractVersion,
         "phase7_list_analysis_artifacts_local"},
        {ToolId::ReadAnalysisArtifact,
         "tapescript_read_analysis_artifact",
         "Read a stored local Phase 7 analyzer artifact by id or manifest path.",
         phase7AnalysisRefInputSchema(),
         phase7AnalyzerResultSchema(),
         "phase7.analysis-artifact-read.v1",
         tape_engine::QueryOperation::Unknown,
         false,
         kPhase7ContractVersion,
         "phase7_read_analysis_artifact_local"},
        {ToolId::PlaybookApply,
         "tapescript_playbook_apply",
         "Build a guarded dry-run playbook from a stored Phase 7 analyzer artifact.",
         phase7PlaybookInputSchema(),
         phase7PlaybookApplyResultSchema(),
         "phase7.playbook-plan.v1",
         tape_engine::QueryOperation::Unknown,
         false,
         kPhase7ContractVersion,
         "phase7_playbook_apply_guarded_local"},
        {ToolId::ListPlaybookArtifacts,
         "tapescript_list_playbook_artifacts",
         "List persisted local Phase 7 guarded playbook artifacts.",
         phase7PlaybookInventoryInputSchema(),
         phase7PlaybookArtifactListResultSchema(),
         "phase7.playbook-artifact-list.v1",
         tape_engine::QueryOperation::Unknown,
         false,
         kPhase7ContractVersion,
         "phase7_list_playbook_artifacts_local"},
        {ToolId::ReadPlaybookArtifact,
         "tapescript_read_playbook_artifact",
         "Read a stored local Phase 7 guarded playbook artifact by id or manifest path.",
         phase7PlaybookRefInputSchema(),
         phase7PlaybookResultSchema(),
         "phase7.playbook-artifact-read.v1",
         tape_engine::QueryOperation::Unknown,
         false,
         kPhase7ContractVersion,
         "phase7_read_playbook_artifact_local"},
        {ToolId::PrepareExecutionLedger,
         "tapescript_prepare_execution_ledger",
         "Create or reopen a durable local Phase 7 execution/audit ledger from a stored guarded playbook artifact.",
         phase7ExecutionLedgerInputSchema(),
         phase7ExecutionLedgerBuildResultSchema(),
         "phase7.execution-ledger.v1",
         tape_engine::QueryOperation::Unknown,
         false,
         kPhase7ContractVersion,
         "phase7_prepare_execution_ledger_local"},
        {ToolId::ListExecutionLedgers,
         "tapescript_list_execution_ledgers",
         "List persisted local Phase 7 execution/audit ledger artifacts.",
         phase7ExecutionLedgerInventoryInputSchema(),
         phase7ExecutionLedgerListResultSchema(),
         "phase7.execution-ledger-list.v1",
         tape_engine::QueryOperation::Unknown,
         false,
         kPhase7ContractVersion,
         "phase7_list_execution_ledgers_local"},
        {ToolId::ReadExecutionLedger,
         "tapescript_read_execution_ledger",
         "Read a stored local Phase 7 execution/audit ledger artifact by id or manifest path.",
         phase7ExecutionLedgerRefInputSchema(),
         phase7ExecutionLedgerResultSchema(),
         "phase7.execution-ledger-read.v1",
         tape_engine::QueryOperation::Unknown,
         false,
         kPhase7ContractVersion,
         "phase7_read_execution_ledger_local"},
        {ToolId::RecordExecutionLedgerReview,
         "tapescript_record_execution_ledger_review",
         "Append a review decision to one or more Phase 7 execution-ledger entries while live apply remains deferred.",
         phase7ExecutionLedgerReviewInputSchema(),
         phase7ExecutionLedgerReviewResultSchema(),
         "phase7.execution-ledger-review.v1",
         tape_engine::QueryOperation::Unknown,
         false,
         kPhase7ContractVersion,
         "phase7_record_execution_ledger_review_local"},
        {ToolId::StartExecutionJournal,
         "tapescript_start_execution_journal",
         "Create or reopen a durable Phase 7 execution journal from a ready execution ledger.",
         phase7ExecutionJournalInputSchema(),
         phase7ExecutionJournalStartResultSchema(),
         "phase7.execution-journal.v1",
         tape_engine::QueryOperation::Unknown,
         false,
         kPhase7ContractVersion,
         "phase7_start_execution_journal_local"},
        {ToolId::ListExecutionJournals,
         "tapescript_list_execution_journals",
         "List persisted local Phase 7 execution journal artifacts.",
         phase7ExecutionJournalInventoryInputSchema(),
         phase7ExecutionJournalListResultSchema(),
         "phase7.execution-journal-list.v1",
         tape_engine::QueryOperation::Unknown,
         false,
         kPhase7ContractVersion,
         "phase7_list_execution_journals_local"},
        {ToolId::ReadExecutionJournal,
         "tapescript_read_execution_journal",
         "Read a stored local Phase 7 execution journal artifact by id or manifest path.",
         phase7ExecutionJournalRefInputSchema(),
         phase7ExecutionJournalResultSchema(),
         "phase7.execution-journal-read.v1",
         tape_engine::QueryOperation::Unknown,
         false,
         kPhase7ContractVersion,
         "phase7_read_execution_journal_local"},
        {ToolId::DispatchExecutionJournal,
         "tapescript_dispatch_execution_journal",
         "Dispatch queued Phase 7 execution journal entries into the controlled execution path.",
         phase7ExecutionJournalDispatchInputSchema(),
         phase7ExecutionJournalEventResultSchema(),
         "phase7.execution-journal-event.v1",
         tape_engine::QueryOperation::Unknown,
         false,
         kPhase7ContractVersion,
         "phase7_dispatch_execution_journal_local"},
        {ToolId::RecordExecutionJournalEvent,
         "tapescript_record_execution_journal_event",
         "Append an execution lifecycle update to one or more Phase 7 execution journal entries.",
         phase7ExecutionJournalEventInputSchema(),
         phase7ExecutionJournalEventResultSchema(),
         "phase7.execution-journal-event.v1",
         tape_engine::QueryOperation::Unknown,
         false,
         kPhase7ContractVersion,
         "phase7_record_execution_journal_event_local"},
        {ToolId::StartExecutionApply,
         "tapescript_start_execution_apply",
         "Create or reopen a durable Phase 7 execution-apply artifact from submitted journal entries.",
         phase7ExecutionApplyInputSchema(),
         phase7ExecutionApplyStartResultSchema(),
         "phase7.execution-apply.v1",
         tape_engine::QueryOperation::Unknown,
         false,
         kPhase7ContractVersion,
         "phase7_start_execution_apply_local"},
        {ToolId::ListExecutionApplies,
         "tapescript_list_execution_applies",
         "List persisted local Phase 7 execution-apply artifacts.",
         phase7ExecutionApplyInventoryInputSchema(),
         phase7ExecutionApplyListResultSchema(),
         "phase7.execution-apply-list.v1",
         tape_engine::QueryOperation::Unknown,
         false,
         kPhase7ContractVersion,
         "phase7_list_execution_applies_local"},
        {ToolId::ReadExecutionApply,
         "tapescript_read_execution_apply",
         "Read a stored local Phase 7 execution-apply artifact by id or manifest path.",
         phase7ExecutionApplyRefInputSchema(),
         phase7ExecutionApplyResultSchema(),
         "phase7.execution-apply-read.v1",
         tape_engine::QueryOperation::Unknown,
         false,
         kPhase7ContractVersion,
         "phase7_read_execution_apply_local"},
        {ToolId::RecordExecutionApplyEvent,
         "tapescript_record_execution_apply_event",
         "Synchronize execution results into a Phase 7 execution-apply artifact and its source journal.",
         phase7ExecutionApplyEventInputSchema(),
         phase7ExecutionApplyEventResultSchema(),
         "phase7.execution-apply-event.v1",
         tape_engine::QueryOperation::Unknown,
         false,
         kPhase7ContractVersion,
         "phase7_record_execution_apply_event_local"}
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

std::optional<std::vector<std::string>> asNonEmptyStringArray(const json& value) {
    if (!value.is_array()) {
        return std::nullopt;
    }
    std::vector<std::string> values;
    values.reserve(value.size());
    for (const auto& item : value) {
        const auto text = asNonEmptyString(item);
        if (!text.has_value()) {
            return std::nullopt;
        }
        values.push_back(*text);
    }
    return values;
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

json revisionFromStatus(const tape_payloads::StatusSnapshot& snapshot) {
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

json revisionFromPhase7ReplayContext(const json& replayContext) {
    if (!replayContext.is_object()) {
        return revisionUnavailable();
    }
    const json revision = replayContext.value("revision_context", json::object());
    if (!revision.is_object()) {
        return revisionUnavailable();
    }
    return json{
        {"served_revision_id", revision.contains("served_revision_id") ? revision.at("served_revision_id") : json(nullptr)},
        {"latest_session_seq", revision.contains("latest_session_seq") ? revision.at("latest_session_seq") : json(nullptr)},
        {"first_session_seq", revision.contains("first_session_seq") ? revision.at("first_session_seq") : json(nullptr)},
        {"last_session_seq", revision.contains("last_session_seq") ? revision.at("last_session_seq") : json(nullptr)},
        {"manifest_hash", revision.contains("manifest_hash") ? revision.at("manifest_hash") : json(nullptr)},
        {"includes_mutable_tail", revision.value("includes_mutable_tail", false)},
        {"source", revision.value("source", std::string("artifact_manifest"))}
    };
}

struct Phase7FindingSelection {
    std::string manifestPath;
    std::string artifactId;
    std::vector<std::string> findingIds;
    std::string minimumSeverity;
    std::string category;
    std::size_t limit = 0;
};

struct Phase7SelectedFindings {
    std::vector<tape_phase7::FindingRecord> findings;
    std::vector<std::string> findingIds;
    json appliedFilters = json::object();
};

struct Phase7AnalysisInventorySelection {
    std::size_t limit = 25;
    std::string sourceArtifactId;
    std::string analysisProfile;
};

struct Phase7PlaybookInventorySelection {
    std::size_t limit = 25;
    std::string analysisArtifactId;
    std::string sourceArtifactId;
    std::string mode;
};

struct Phase7ExecutionLedgerInventorySelection {
    std::size_t limit = 25;
    std::string playbookArtifactId;
    std::string analysisArtifactId;
    std::string sourceArtifactId;
    std::string ledgerStatus;
    std::string sortBy = "generated_at_desc";
};

struct Phase7ExecutionLedgerReviewSelection {
    std::string manifestPath;
    std::string artifactId;
    std::vector<std::string> entryIds;
    std::string reviewStatus;
    std::string actor;
    std::string comment;
};

struct Phase7ExecutionJournalInventorySelection {
    std::size_t limit = 25;
    std::string ledgerArtifactId;
    std::string playbookArtifactId;
    std::string analysisArtifactId;
    std::string sourceArtifactId;
    std::string journalStatus;
    std::string recoveryState;
    std::string sortBy = "generated_at_desc";
};

struct Phase7ExecutionJournalDispatchSelection {
    std::string manifestPath;
    std::string artifactId;
    std::vector<std::string> entryIds;
    std::string actor;
    std::string executionCapability;
    std::string comment;
};

struct Phase7ExecutionJournalEventSelection {
    std::string manifestPath;
    std::string artifactId;
    std::vector<std::string> entryIds;
    std::string executionStatus;
    std::string actor;
    std::string comment;
    std::string failureCode;
    std::string failureMessage;
};

struct Phase7ExecutionApplySelection {
    std::string manifestPath;
    std::string artifactId;
    std::vector<std::string> entryIds;
    std::string actor;
    std::string executionCapability;
    std::string comment;
};

struct Phase7ExecutionApplyInventorySelection {
    std::size_t limit = 25;
    std::string journalArtifactId;
    std::string ledgerArtifactId;
    std::string playbookArtifactId;
    std::string analysisArtifactId;
    std::string sourceArtifactId;
    std::string applyStatus;
    std::string recoveryState;
    std::string sortBy = "generated_at_desc";
};

struct Phase7ExecutionApplyEventSelection {
    std::string manifestPath;
    std::string artifactId;
    std::vector<std::string> entryIds;
    std::string executionStatus;
    std::string actor;
    std::string comment;
    std::string failureCode;
    std::string failureMessage;
};

std::optional<int> phase7SeverityRank(std::string_view severity) {
    if (severity == "info") {
        return 0;
    }
    if (severity == "warning") {
        return 1;
    }
    if (severity == "error") {
        return 2;
    }
    if (severity == "critical") {
        return 3;
    }
    return std::nullopt;
}

bool parsePhase7SelectionArgs(const json& args,
                              Phase7FindingSelection* outSelection,
                              std::string* outCode,
                              std::string* outMessage,
                              bool allowMode) {
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

    if (allowMode) {
        if (hasUnexpectedKeys(args, {"analysis_manifest_path", "analysis_artifact_id", "finding_ids", "minimum_severity", "category", "limit", "mode"})) {
            return fail("invalid_arguments",
                        "playbook arguments must include one analysis reference plus optional finding filters and mode");
        }
    } else if (hasUnexpectedKeys(args, {"analysis_manifest_path", "analysis_artifact_id", "finding_ids", "minimum_severity", "category", "limit"})) {
        return fail("invalid_arguments",
                    "findings arguments must include one analysis reference plus optional finding filters");
    }

    Phase7FindingSelection selection;
    const bool hasManifest = args.contains("analysis_manifest_path");
    const bool hasArtifactId = args.contains("analysis_artifact_id");
    if (hasManifest == hasArtifactId) {
        return fail("invalid_arguments",
                    "exactly one of analysis_manifest_path or analysis_artifact_id is required");
    }
    if (hasManifest) {
        const auto manifestPath = asNonEmptyString(args.at("analysis_manifest_path"));
        if (!manifestPath.has_value()) {
            return fail("invalid_arguments", "analysis_manifest_path must be a non-empty string");
        }
        selection.manifestPath = *manifestPath;
    }
    if (hasArtifactId) {
        const auto artifactId = asNonEmptyString(args.at("analysis_artifact_id"));
        if (!artifactId.has_value()) {
            return fail("invalid_arguments", "analysis_artifact_id must be a non-empty string");
        }
        selection.artifactId = *artifactId;
    }
    if (args.contains("finding_ids")) {
        const auto parsed = asNonEmptyStringArray(args.at("finding_ids"));
        if (!parsed.has_value()) {
            return fail("invalid_arguments", "finding_ids must be an array of non-empty strings");
        }
        selection.findingIds = *parsed;
    }
    if (args.contains("minimum_severity")) {
        const auto severity = asNonEmptyString(args.at("minimum_severity"));
        if (!severity.has_value() || !phase7SeverityRank(*severity).has_value()) {
            return fail("invalid_arguments",
                        "minimum_severity must be one of info, warning, error, or critical");
        }
        selection.minimumSeverity = *severity;
    }
    if (args.contains("category")) {
        const auto category = asNonEmptyString(args.at("category"));
        if (!category.has_value()) {
            return fail("invalid_arguments", "category must be a non-empty string");
        }
        selection.category = *category;
    }
    if (args.contains("limit")) {
        const auto limit = asPositiveUInt64(args.at("limit"));
        if (!limit.has_value()) {
            return fail("invalid_arguments", "limit must be a positive integer");
        }
        selection.limit = static_cast<std::size_t>(*limit);
    }

    if (outSelection != nullptr) {
        *outSelection = std::move(selection);
    }
    return true;
}

bool parsePhase7PlaybookRefArgs(const json& args,
                                std::string* outManifestPath,
                                std::string* outArtifactId,
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
    if (hasUnexpectedKeys(args, {"playbook_manifest_path", "playbook_artifact_id"})) {
        return fail("invalid_arguments",
                    "exactly one of playbook_manifest_path or playbook_artifact_id is required");
    }

    const bool hasManifest = args.contains("playbook_manifest_path");
    const bool hasArtifactId = args.contains("playbook_artifact_id");
    if (hasManifest == hasArtifactId) {
        return fail("invalid_arguments",
                    "exactly one of playbook_manifest_path or playbook_artifact_id is required");
    }

    if (hasManifest) {
        const auto manifestPath = asNonEmptyString(args.at("playbook_manifest_path"));
        if (!manifestPath.has_value()) {
            return fail("invalid_arguments", "playbook_manifest_path must be a non-empty string");
        }
        if (outManifestPath != nullptr) {
            *outManifestPath = *manifestPath;
        }
    }
    if (hasArtifactId) {
        const auto artifactId = asNonEmptyString(args.at("playbook_artifact_id"));
        if (!artifactId.has_value()) {
            return fail("invalid_arguments", "playbook_artifact_id must be a non-empty string");
        }
        if (outArtifactId != nullptr) {
            *outArtifactId = *artifactId;
        }
    }
    return true;
}

bool parsePhase7ExecutionLedgerRefArgs(const json& args,
                                       std::string* outManifestPath,
                                       std::string* outArtifactId,
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
    if (hasUnexpectedKeys(args, {"execution_ledger_manifest_path", "execution_ledger_artifact_id"})) {
        return fail("invalid_arguments",
                    "exactly one of execution_ledger_manifest_path or execution_ledger_artifact_id is required");
    }

    const bool hasManifest = args.contains("execution_ledger_manifest_path");
    const bool hasArtifactId = args.contains("execution_ledger_artifact_id");
    if (hasManifest == hasArtifactId) {
        return fail("invalid_arguments",
                    "exactly one of execution_ledger_manifest_path or execution_ledger_artifact_id is required");
    }

    if (hasManifest) {
        const auto manifestPath = asNonEmptyString(args.at("execution_ledger_manifest_path"));
        if (!manifestPath.has_value()) {
            return fail("invalid_arguments", "execution_ledger_manifest_path must be a non-empty string");
        }
        if (outManifestPath != nullptr) {
            *outManifestPath = *manifestPath;
        }
    }
    if (hasArtifactId) {
        const auto artifactId = asNonEmptyString(args.at("execution_ledger_artifact_id"));
        if (!artifactId.has_value()) {
            return fail("invalid_arguments", "execution_ledger_artifact_id must be a non-empty string");
        }
        if (outArtifactId != nullptr) {
            *outArtifactId = *artifactId;
        }
    }
    return true;
}

bool parsePhase7ExecutionLedgerReviewArgs(const json& args,
                                          Phase7ExecutionLedgerReviewSelection* outSelection,
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
    if (hasUnexpectedKeys(args,
                          {"execution_ledger_manifest_path",
                           "execution_ledger_artifact_id",
                           "entry_ids",
                           "review_status",
                           "actor",
                           "comment"})) {
        return fail("invalid_arguments",
                    "execution ledger review arguments support one ledger reference plus entry_ids, review_status, actor, and comment");
    }

    Phase7ExecutionLedgerReviewSelection selection;
    const bool hasManifest = args.contains("execution_ledger_manifest_path");
    const bool hasArtifactId = args.contains("execution_ledger_artifact_id");
    if (hasManifest == hasArtifactId) {
        return fail("invalid_arguments",
                    "exactly one of execution_ledger_manifest_path or execution_ledger_artifact_id is required");
    }
    if (hasManifest) {
        const auto manifestPath = asNonEmptyString(args.at("execution_ledger_manifest_path"));
        if (!manifestPath.has_value()) {
            return fail("invalid_arguments", "execution_ledger_manifest_path must be a non-empty string");
        }
        selection.manifestPath = *manifestPath;
    }
    if (hasArtifactId) {
        const auto artifactId = asNonEmptyString(args.at("execution_ledger_artifact_id"));
        if (!artifactId.has_value()) {
            return fail("invalid_arguments", "execution_ledger_artifact_id must be a non-empty string");
        }
        selection.artifactId = *artifactId;
    }
    if (!args.contains("entry_ids")) {
        return fail("invalid_arguments", "entry_ids is required");
    }
    const auto entryIds = asNonEmptyStringArray(args.at("entry_ids"));
    if (!entryIds.has_value() || entryIds->empty()) {
        return fail("invalid_arguments", "entry_ids must be an array of non-empty strings");
    }
    selection.entryIds = *entryIds;
    if (!args.contains("review_status")) {
        return fail("invalid_arguments", "review_status is required");
    }
    const auto reviewStatus = asNonEmptyString(args.at("review_status"));
    if (!reviewStatus.has_value() ||
        (*reviewStatus != tape_phase7::kLedgerEntryStatusApproved &&
         *reviewStatus != tape_phase7::kLedgerEntryStatusBlocked &&
         *reviewStatus != tape_phase7::kLedgerEntryStatusNeedsInfo &&
         *reviewStatus != tape_phase7::kLedgerEntryStatusNotApplicable)) {
        return fail("invalid_arguments",
                    "review_status must be one of approved, blocked, needs_info, or not_applicable");
    }
    selection.reviewStatus = *reviewStatus;
    if (!args.contains("actor")) {
        return fail("invalid_arguments", "actor is required");
    }
    const auto actor = asNonEmptyString(args.at("actor"));
    if (!actor.has_value()) {
        return fail("invalid_arguments", "actor must be a non-empty string");
    }
    selection.actor = *actor;
    if (args.contains("comment")) {
        const auto comment = asNonEmptyString(args.at("comment"));
        if (!comment.has_value()) {
            return fail("invalid_arguments", "comment must be a non-empty string");
        }
        selection.comment = *comment;
    }
    if ((selection.reviewStatus == tape_phase7::kLedgerEntryStatusBlocked ||
         selection.reviewStatus == tape_phase7::kLedgerEntryStatusNeedsInfo) &&
        selection.comment.empty()) {
        return fail("invalid_arguments", "comment is required for blocked and needs_info reviews");
    }

    if (outSelection != nullptr) {
        *outSelection = std::move(selection);
    }
    return true;
}

bool parsePhase7ExecutionJournalRefArgs(const json& args,
                                        std::string* outManifestPath,
                                        std::string* outArtifactId,
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
    if (hasUnexpectedKeys(args, {"execution_journal_manifest_path", "execution_journal_artifact_id"})) {
        return fail("invalid_arguments",
                    "exactly one of execution_journal_manifest_path or execution_journal_artifact_id is required");
    }

    const bool hasManifest = args.contains("execution_journal_manifest_path");
    const bool hasArtifactId = args.contains("execution_journal_artifact_id");
    if (hasManifest == hasArtifactId) {
        return fail("invalid_arguments",
                    "exactly one of execution_journal_manifest_path or execution_journal_artifact_id is required");
    }

    if (hasManifest) {
        const auto manifestPath = asNonEmptyString(args.at("execution_journal_manifest_path"));
        if (!manifestPath.has_value()) {
            return fail("invalid_arguments", "execution_journal_manifest_path must be a non-empty string");
        }
        if (outManifestPath != nullptr) {
            *outManifestPath = *manifestPath;
        }
    }
    if (hasArtifactId) {
        const auto artifactId = asNonEmptyString(args.at("execution_journal_artifact_id"));
        if (!artifactId.has_value()) {
            return fail("invalid_arguments", "execution_journal_artifact_id must be a non-empty string");
        }
        if (outArtifactId != nullptr) {
            *outArtifactId = *artifactId;
        }
    }
    return true;
}

bool parsePhase7ExecutionJournalDispatchArgs(const json& args,
                                             Phase7ExecutionJournalDispatchSelection* outSelection,
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
    if (hasUnexpectedKeys(args,
                          {"execution_journal_manifest_path",
                           "execution_journal_artifact_id",
                           "entry_ids",
                           "actor",
                           "execution_capability",
                           "comment"})) {
        return fail("invalid_arguments",
                    "execution journal dispatch arguments support one journal reference plus optional entry_ids, actor, execution_capability, and comment");
    }

    Phase7ExecutionJournalDispatchSelection selection;
    const bool hasManifest = args.contains("execution_journal_manifest_path");
    const bool hasArtifactId = args.contains("execution_journal_artifact_id");
    if (hasManifest == hasArtifactId) {
        return fail("invalid_arguments",
                    "exactly one of execution_journal_manifest_path or execution_journal_artifact_id is required");
    }
    if (hasManifest) {
        const auto manifestPath = asNonEmptyString(args.at("execution_journal_manifest_path"));
        if (!manifestPath.has_value()) {
            return fail("invalid_arguments", "execution_journal_manifest_path must be a non-empty string");
        }
        selection.manifestPath = *manifestPath;
    }
    if (hasArtifactId) {
        const auto artifactId = asNonEmptyString(args.at("execution_journal_artifact_id"));
        if (!artifactId.has_value()) {
            return fail("invalid_arguments", "execution_journal_artifact_id must be a non-empty string");
        }
        selection.artifactId = *artifactId;
    }
    if (!args.contains("actor")) {
        return fail("invalid_arguments", "actor is required");
    }
    const auto actor = asNonEmptyString(args.at("actor"));
    if (!actor.has_value()) {
        return fail("invalid_arguments", "actor must be a non-empty string");
    }
    selection.actor = *actor;
    if (!args.contains("execution_capability")) {
        return fail("invalid_arguments", "execution_capability is required");
    }
    const auto executionCapability = asNonEmptyString(args.at("execution_capability"));
    if (!executionCapability.has_value()) {
        return fail("invalid_arguments", "execution_capability must be a non-empty string");
    }
    selection.executionCapability = *executionCapability;
    if (args.contains("entry_ids")) {
        const auto entryIds = asNonEmptyStringArray(args.at("entry_ids"));
        if (!entryIds.has_value() || entryIds->empty()) {
            return fail("invalid_arguments", "entry_ids must be an array of non-empty strings when provided");
        }
        selection.entryIds = *entryIds;
    }
    if (args.contains("comment")) {
        const auto comment = asNonEmptyString(args.at("comment"));
        if (!comment.has_value()) {
            return fail("invalid_arguments", "comment must be a non-empty string");
        }
        selection.comment = *comment;
    }

    if (outSelection != nullptr) {
        *outSelection = std::move(selection);
    }
    return true;
}

bool parsePhase7ExecutionJournalEventArgs(const json& args,
                                          Phase7ExecutionJournalEventSelection* outSelection,
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
    if (hasUnexpectedKeys(args,
                          {"execution_journal_manifest_path",
                           "execution_journal_artifact_id",
                           "entry_ids",
                           "execution_status",
                           "actor",
                           "comment",
                           "failure_code",
                           "failure_message"})) {
        return fail("invalid_arguments",
                    "execution journal event arguments support one journal reference plus entry_ids, execution_status, actor, comment, and optional failure details");
    }

    Phase7ExecutionJournalEventSelection selection;
    const bool hasManifest = args.contains("execution_journal_manifest_path");
    const bool hasArtifactId = args.contains("execution_journal_artifact_id");
    if (hasManifest == hasArtifactId) {
        return fail("invalid_arguments",
                    "exactly one of execution_journal_manifest_path or execution_journal_artifact_id is required");
    }
    if (hasManifest) {
        const auto manifestPath = asNonEmptyString(args.at("execution_journal_manifest_path"));
        if (!manifestPath.has_value()) {
            return fail("invalid_arguments", "execution_journal_manifest_path must be a non-empty string");
        }
        selection.manifestPath = *manifestPath;
    }
    if (hasArtifactId) {
        const auto artifactId = asNonEmptyString(args.at("execution_journal_artifact_id"));
        if (!artifactId.has_value()) {
            return fail("invalid_arguments", "execution_journal_artifact_id must be a non-empty string");
        }
        selection.artifactId = *artifactId;
    }
    if (!args.contains("entry_ids")) {
        return fail("invalid_arguments", "entry_ids is required");
    }
    const auto entryIds = asNonEmptyStringArray(args.at("entry_ids"));
    if (!entryIds.has_value() || entryIds->empty()) {
        return fail("invalid_arguments", "entry_ids must be an array of non-empty strings");
    }
    selection.entryIds = *entryIds;
    if (!args.contains("execution_status")) {
        return fail("invalid_arguments", "execution_status is required");
    }
    const auto executionStatus = asNonEmptyString(args.at("execution_status"));
    if (!executionStatus.has_value() ||
        (*executionStatus != tape_phase7::kExecutionEntryStatusQueued &&
         *executionStatus != tape_phase7::kExecutionEntryStatusSubmitted &&
         *executionStatus != tape_phase7::kExecutionEntryStatusSucceeded &&
         *executionStatus != tape_phase7::kExecutionEntryStatusFailed &&
         *executionStatus != tape_phase7::kExecutionEntryStatusCancelled)) {
        return fail("invalid_arguments",
                    "execution_status must be one of queued, submitted, succeeded, failed, or cancelled");
    }
    selection.executionStatus = *executionStatus;
    if (!args.contains("actor")) {
        return fail("invalid_arguments", "actor is required");
    }
    const auto actor = asNonEmptyString(args.at("actor"));
    if (!actor.has_value()) {
        return fail("invalid_arguments", "actor must be a non-empty string");
    }
    selection.actor = *actor;
    if (args.contains("comment")) {
        const auto comment = asNonEmptyString(args.at("comment"));
        if (!comment.has_value()) {
            return fail("invalid_arguments", "comment must be a non-empty string");
        }
        selection.comment = *comment;
    }
    if (args.contains("failure_code")) {
        const auto failureCode = asNonEmptyString(args.at("failure_code"));
        if (!failureCode.has_value()) {
            return fail("invalid_arguments", "failure_code must be a non-empty string");
        }
        selection.failureCode = *failureCode;
    }
    if (args.contains("failure_message")) {
        const auto failureMessage = asNonEmptyString(args.at("failure_message"));
        if (!failureMessage.has_value()) {
            return fail("invalid_arguments", "failure_message must be a non-empty string");
        }
        selection.failureMessage = *failureMessage;
    }
    if ((selection.executionStatus == tape_phase7::kExecutionEntryStatusFailed ||
         selection.executionStatus == tape_phase7::kExecutionEntryStatusCancelled) &&
        selection.comment.empty()) {
        return fail("invalid_arguments", "comment is required for failed and cancelled execution updates");
    }

    if (outSelection != nullptr) {
        *outSelection = std::move(selection);
    }
    return true;
}

bool parsePhase7ExecutionApplyRefArgs(const json& args,
                                      std::string* outManifestPath,
                                      std::string* outArtifactId,
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
    if (hasUnexpectedKeys(args, {"execution_apply_manifest_path", "execution_apply_artifact_id"})) {
        return fail("invalid_arguments",
                    "exactly one of execution_apply_manifest_path or execution_apply_artifact_id is required");
    }

    const bool hasManifest = args.contains("execution_apply_manifest_path");
    const bool hasArtifactId = args.contains("execution_apply_artifact_id");
    if (hasManifest == hasArtifactId) {
        return fail("invalid_arguments",
                    "exactly one of execution_apply_manifest_path or execution_apply_artifact_id is required");
    }
    if (hasManifest) {
        const auto manifestPath = asNonEmptyString(args.at("execution_apply_manifest_path"));
        if (!manifestPath.has_value()) {
            return fail("invalid_arguments", "execution_apply_manifest_path must be a non-empty string");
        }
        if (outManifestPath != nullptr) {
            *outManifestPath = *manifestPath;
        }
    }
    if (hasArtifactId) {
        const auto artifactId = asNonEmptyString(args.at("execution_apply_artifact_id"));
        if (!artifactId.has_value()) {
            return fail("invalid_arguments", "execution_apply_artifact_id must be a non-empty string");
        }
        if (outArtifactId != nullptr) {
            *outArtifactId = *artifactId;
        }
    }
    return true;
}

bool parsePhase7ExecutionApplyArgs(const json& args,
                                   Phase7ExecutionApplySelection* outSelection,
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
    if (hasUnexpectedKeys(args,
                          {"execution_journal_manifest_path",
                           "execution_journal_artifact_id",
                           "entry_ids",
                           "actor",
                           "execution_capability",
                           "comment"})) {
        return fail("invalid_arguments",
                    "execution apply arguments support one journal reference plus optional entry_ids, actor, execution_capability, and comment");
    }

    Phase7ExecutionApplySelection selection;
    const bool hasManifest = args.contains("execution_journal_manifest_path");
    const bool hasArtifactId = args.contains("execution_journal_artifact_id");
    if (hasManifest == hasArtifactId) {
        return fail("invalid_arguments",
                    "exactly one of execution_journal_manifest_path or execution_journal_artifact_id is required");
    }
    if (hasManifest) {
        const auto manifestPath = asNonEmptyString(args.at("execution_journal_manifest_path"));
        if (!manifestPath.has_value()) {
            return fail("invalid_arguments", "execution_journal_manifest_path must be a non-empty string");
        }
        selection.manifestPath = *manifestPath;
    }
    if (hasArtifactId) {
        const auto artifactId = asNonEmptyString(args.at("execution_journal_artifact_id"));
        if (!artifactId.has_value()) {
            return fail("invalid_arguments", "execution_journal_artifact_id must be a non-empty string");
        }
        selection.artifactId = *artifactId;
    }
    if (!args.contains("actor")) {
        return fail("invalid_arguments", "actor is required");
    }
    const auto actor = asNonEmptyString(args.at("actor"));
    if (!actor.has_value()) {
        return fail("invalid_arguments", "actor must be a non-empty string");
    }
    selection.actor = *actor;
    if (!args.contains("execution_capability")) {
        return fail("invalid_arguments", "execution_capability is required");
    }
    const auto executionCapability = asNonEmptyString(args.at("execution_capability"));
    if (!executionCapability.has_value()) {
        return fail("invalid_arguments", "execution_capability must be a non-empty string");
    }
    selection.executionCapability = *executionCapability;
    if (args.contains("entry_ids")) {
        const auto entryIds = asNonEmptyStringArray(args.at("entry_ids"));
        if (!entryIds.has_value() || entryIds->empty()) {
            return fail("invalid_arguments", "entry_ids must be an array of non-empty strings when provided");
        }
        selection.entryIds = *entryIds;
    }
    if (args.contains("comment")) {
        const auto comment = asNonEmptyString(args.at("comment"));
        if (!comment.has_value()) {
            return fail("invalid_arguments", "comment must be a non-empty string");
        }
        selection.comment = *comment;
    }
    if (outSelection != nullptr) {
        *outSelection = std::move(selection);
    }
    return true;
}

bool parsePhase7ExecutionApplyEventArgs(const json& args,
                                        Phase7ExecutionApplyEventSelection* outSelection,
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
    if (hasUnexpectedKeys(args,
                          {"execution_apply_manifest_path",
                           "execution_apply_artifact_id",
                           "entry_ids",
                           "execution_status",
                           "actor",
                           "comment",
                           "failure_code",
                           "failure_message"})) {
        return fail("invalid_arguments",
                    "execution apply event arguments support one apply reference plus entry_ids, execution_status, actor, comment, and optional failure details");
    }

    Phase7ExecutionApplyEventSelection selection;
    const bool hasManifest = args.contains("execution_apply_manifest_path");
    const bool hasArtifactId = args.contains("execution_apply_artifact_id");
    if (hasManifest == hasArtifactId) {
        return fail("invalid_arguments",
                    "exactly one of execution_apply_manifest_path or execution_apply_artifact_id is required");
    }
    if (hasManifest) {
        const auto manifestPath = asNonEmptyString(args.at("execution_apply_manifest_path"));
        if (!manifestPath.has_value()) {
            return fail("invalid_arguments", "execution_apply_manifest_path must be a non-empty string");
        }
        selection.manifestPath = *manifestPath;
    }
    if (hasArtifactId) {
        const auto artifactId = asNonEmptyString(args.at("execution_apply_artifact_id"));
        if (!artifactId.has_value()) {
            return fail("invalid_arguments", "execution_apply_artifact_id must be a non-empty string");
        }
        selection.artifactId = *artifactId;
    }
    if (!args.contains("entry_ids")) {
        return fail("invalid_arguments", "entry_ids is required");
    }
    const auto entryIds = asNonEmptyStringArray(args.at("entry_ids"));
    if (!entryIds.has_value() || entryIds->empty()) {
        return fail("invalid_arguments", "entry_ids must be an array of non-empty strings");
    }
    selection.entryIds = *entryIds;
    if (!args.contains("execution_status")) {
        return fail("invalid_arguments", "execution_status is required");
    }
    const auto executionStatus = asNonEmptyString(args.at("execution_status"));
    if (!executionStatus.has_value()) {
        return fail("invalid_arguments", "execution_status must be a non-empty string");
    }
    selection.executionStatus = *executionStatus;
    if (!args.contains("actor")) {
        return fail("invalid_arguments", "actor is required");
    }
    const auto actor = asNonEmptyString(args.at("actor"));
    if (!actor.has_value()) {
        return fail("invalid_arguments", "actor must be a non-empty string");
    }
    selection.actor = *actor;
    if (args.contains("comment")) {
        const auto comment = asNonEmptyString(args.at("comment"));
        if (!comment.has_value()) {
            return fail("invalid_arguments", "comment must be a non-empty string");
        }
        selection.comment = *comment;
    }
    if (args.contains("failure_code")) {
        const auto failureCode = asNonEmptyString(args.at("failure_code"));
        if (!failureCode.has_value()) {
            return fail("invalid_arguments", "failure_code must be a non-empty string");
        }
        selection.failureCode = *failureCode;
    }
    if (args.contains("failure_message")) {
        const auto failureMessage = asNonEmptyString(args.at("failure_message"));
        if (!failureMessage.has_value()) {
            return fail("invalid_arguments", "failure_message must be a non-empty string");
        }
        selection.failureMessage = *failureMessage;
    }
    if (outSelection != nullptr) {
        *outSelection = std::move(selection);
    }
    return true;
}

bool parsePhase7AnalysisInventoryArgs(const json& args,
                                      Phase7AnalysisInventorySelection* outSelection,
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
    if (hasUnexpectedKeys(args, {"limit", "source_artifact_id", "analysis_profile"})) {
        return fail("invalid_arguments",
                    "analysis inventory arguments support only limit, source_artifact_id, and analysis_profile");
    }

    Phase7AnalysisInventorySelection selection;
    if (args.contains("limit")) {
        const auto limit = asPositiveUInt64(args.at("limit"));
        if (!limit.has_value()) {
            return fail("invalid_arguments", "limit must be a positive integer");
        }
        selection.limit = static_cast<std::size_t>(*limit);
    }
    if (args.contains("source_artifact_id")) {
        const auto sourceArtifactId = asNonEmptyString(args.at("source_artifact_id"));
        if (!sourceArtifactId.has_value()) {
            return fail("invalid_arguments", "source_artifact_id must be a non-empty string");
        }
        selection.sourceArtifactId = *sourceArtifactId;
    }
    if (args.contains("analysis_profile")) {
        const auto analysisProfile = asNonEmptyString(args.at("analysis_profile"));
        if (!analysisProfile.has_value()) {
            return fail("invalid_arguments", "analysis_profile must be a non-empty string");
        }
        selection.analysisProfile = *analysisProfile;
    }

    if (outSelection != nullptr) {
        *outSelection = std::move(selection);
    }
    return true;
}

bool parsePhase7PlaybookInventoryArgs(const json& args,
                                      Phase7PlaybookInventorySelection* outSelection,
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
    if (hasUnexpectedKeys(args, {"limit", "analysis_artifact_id", "source_artifact_id", "mode"})) {
        return fail("invalid_arguments",
                    "playbook inventory arguments support only limit, analysis_artifact_id, source_artifact_id, and mode");
    }

    Phase7PlaybookInventorySelection selection;
    if (args.contains("limit")) {
        const auto limit = asPositiveUInt64(args.at("limit"));
        if (!limit.has_value()) {
            return fail("invalid_arguments", "limit must be a positive integer");
        }
        selection.limit = static_cast<std::size_t>(*limit);
    }
    if (args.contains("analysis_artifact_id")) {
        const auto analysisArtifactId = asNonEmptyString(args.at("analysis_artifact_id"));
        if (!analysisArtifactId.has_value()) {
            return fail("invalid_arguments", "analysis_artifact_id must be a non-empty string");
        }
        selection.analysisArtifactId = *analysisArtifactId;
    }
    if (args.contains("source_artifact_id")) {
        const auto sourceArtifactId = asNonEmptyString(args.at("source_artifact_id"));
        if (!sourceArtifactId.has_value()) {
            return fail("invalid_arguments", "source_artifact_id must be a non-empty string");
        }
        selection.sourceArtifactId = *sourceArtifactId;
    }
    if (args.contains("mode")) {
        const auto mode = asNonEmptyString(args.at("mode"));
        if (!mode.has_value() || (*mode != tape_phase7::kDefaultPlaybookMode && *mode != tape_phase7::kApplyPlaybookMode)) {
            return fail("invalid_arguments", "mode must be dry_run or apply");
        }
        selection.mode = *mode;
    }

    if (outSelection != nullptr) {
        *outSelection = std::move(selection);
    }
    return true;
}

bool parsePhase7ExecutionLedgerInventoryArgs(const json& args,
                                             Phase7ExecutionLedgerInventorySelection* outSelection,
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
    if (hasUnexpectedKeys(args, {"limit", "playbook_artifact_id", "analysis_artifact_id", "source_artifact_id", "ledger_status", "sort_by"})) {
        return fail("invalid_arguments",
                    "execution ledger inventory arguments support only limit, playbook_artifact_id, analysis_artifact_id, source_artifact_id, ledger_status, and sort_by");
    }

    Phase7ExecutionLedgerInventorySelection selection;
    if (args.contains("limit")) {
        const auto limit = asPositiveUInt64(args.at("limit"));
        if (!limit.has_value()) {
            return fail("invalid_arguments", "limit must be a positive integer");
        }
        selection.limit = static_cast<std::size_t>(*limit);
    }
    if (args.contains("playbook_artifact_id")) {
        const auto playbookArtifactId = asNonEmptyString(args.at("playbook_artifact_id"));
        if (!playbookArtifactId.has_value()) {
            return fail("invalid_arguments", "playbook_artifact_id must be a non-empty string");
        }
        selection.playbookArtifactId = *playbookArtifactId;
    }
    if (args.contains("analysis_artifact_id")) {
        const auto analysisArtifactId = asNonEmptyString(args.at("analysis_artifact_id"));
        if (!analysisArtifactId.has_value()) {
            return fail("invalid_arguments", "analysis_artifact_id must be a non-empty string");
        }
        selection.analysisArtifactId = *analysisArtifactId;
    }
    if (args.contains("source_artifact_id")) {
        const auto sourceArtifactId = asNonEmptyString(args.at("source_artifact_id"));
        if (!sourceArtifactId.has_value()) {
            return fail("invalid_arguments", "source_artifact_id must be a non-empty string");
        }
        selection.sourceArtifactId = *sourceArtifactId;
    }
    if (args.contains("ledger_status")) {
        const auto ledgerStatus = asNonEmptyString(args.at("ledger_status"));
        if (!ledgerStatus.has_value() ||
            (*ledgerStatus != tape_phase7::kDefaultLedgerStatus &&
             *ledgerStatus != tape_phase7::kLedgerStatusInProgress &&
             *ledgerStatus != tape_phase7::kLedgerStatusBlocked &&
             *ledgerStatus != tape_phase7::kLedgerStatusNeedsInformation &&
             *ledgerStatus != tape_phase7::kLedgerStatusCompleted &&
             *ledgerStatus != tape_phase7::kLedgerStatusWaitingApproval &&
             *ledgerStatus != tape_phase7::kLedgerStatusReadyForExecution)) {
            return fail("invalid_arguments",
                        "ledger_status must be one of review_pending, review_in_progress, review_blocked, needs_information, review_completed, review_waiting_approval, or ready_for_execution");
        }
        selection.ledgerStatus = *ledgerStatus;
    }
    if (args.contains("sort_by")) {
        const auto sortBy = asNonEmptyString(args.at("sort_by"));
        if (!sortBy.has_value() ||
            (*sortBy != "generated_at_desc" && *sortBy != "attention_desc" && *sortBy != "source_artifact_asc")) {
            return fail("invalid_arguments",
                        "sort_by must be one of generated_at_desc, attention_desc, or source_artifact_asc");
        }
        selection.sortBy = *sortBy;
    }

    if (outSelection != nullptr) {
        *outSelection = std::move(selection);
    }
    return true;
}

bool parsePhase7ExecutionJournalInventoryArgs(const json& args,
                                              Phase7ExecutionJournalInventorySelection* outSelection,
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
    if (hasUnexpectedKeys(args,
                          {"limit",
                           "execution_ledger_artifact_id",
                           "playbook_artifact_id",
                           "analysis_artifact_id",
                           "source_artifact_id",
                           "journal_status",
                           "recovery_state",
                           "sort_by"})) {
        return fail("invalid_arguments",
                    "execution journal inventory arguments support only limit, execution_ledger_artifact_id, playbook_artifact_id, analysis_artifact_id, source_artifact_id, journal_status, recovery_state, and sort_by");
    }

    Phase7ExecutionJournalInventorySelection selection;
    if (args.contains("limit")) {
        const auto limit = asPositiveUInt64(args.at("limit"));
        if (!limit.has_value()) {
            return fail("invalid_arguments", "limit must be a positive integer");
        }
        selection.limit = static_cast<std::size_t>(*limit);
    }
    if (args.contains("execution_ledger_artifact_id")) {
        const auto ledgerArtifactId = asNonEmptyString(args.at("execution_ledger_artifact_id"));
        if (!ledgerArtifactId.has_value()) {
            return fail("invalid_arguments", "execution_ledger_artifact_id must be a non-empty string");
        }
        selection.ledgerArtifactId = *ledgerArtifactId;
    }
    if (args.contains("playbook_artifact_id")) {
        const auto playbookArtifactId = asNonEmptyString(args.at("playbook_artifact_id"));
        if (!playbookArtifactId.has_value()) {
            return fail("invalid_arguments", "playbook_artifact_id must be a non-empty string");
        }
        selection.playbookArtifactId = *playbookArtifactId;
    }
    if (args.contains("analysis_artifact_id")) {
        const auto analysisArtifactId = asNonEmptyString(args.at("analysis_artifact_id"));
        if (!analysisArtifactId.has_value()) {
            return fail("invalid_arguments", "analysis_artifact_id must be a non-empty string");
        }
        selection.analysisArtifactId = *analysisArtifactId;
    }
    if (args.contains("source_artifact_id")) {
        const auto sourceArtifactId = asNonEmptyString(args.at("source_artifact_id"));
        if (!sourceArtifactId.has_value()) {
            return fail("invalid_arguments", "source_artifact_id must be a non-empty string");
        }
        selection.sourceArtifactId = *sourceArtifactId;
    }
    if (args.contains("journal_status")) {
        const auto journalStatus = asNonEmptyString(args.at("journal_status"));
        if (!journalStatus.has_value() ||
            (*journalStatus != tape_phase7::kExecutionJournalStatusQueued &&
             *journalStatus != tape_phase7::kExecutionJournalStatusInProgress &&
             *journalStatus != tape_phase7::kExecutionJournalStatusSucceeded &&
             *journalStatus != tape_phase7::kExecutionJournalStatusPartiallySucceeded &&
             *journalStatus != tape_phase7::kExecutionJournalStatusFailed &&
             *journalStatus != tape_phase7::kExecutionJournalStatusCancelled)) {
            return fail("invalid_arguments",
                        "journal_status must be one of execution_queued, execution_in_progress, execution_succeeded, execution_partially_succeeded, execution_failed, or execution_cancelled");
        }
        selection.journalStatus = *journalStatus;
    }
    if (args.contains("recovery_state")) {
        const auto recoveryState = asNonEmptyString(args.at("recovery_state"));
        if (!recoveryState.has_value() ||
            (*recoveryState != "recovery_required" &&
             *recoveryState != "stale_recovery_required")) {
            return fail("invalid_arguments",
                        "recovery_state must be one of recovery_required or stale_recovery_required");
        }
        selection.recoveryState = *recoveryState;
    }
    if (args.contains("sort_by")) {
        const auto sortBy = asNonEmptyString(args.at("sort_by"));
        if (!sortBy.has_value() ||
            (*sortBy != "generated_at_desc" && *sortBy != "attention_desc" && *sortBy != "source_artifact_asc")) {
            return fail("invalid_arguments",
                        "sort_by must be one of generated_at_desc, attention_desc, or source_artifact_asc");
        }
        selection.sortBy = *sortBy;
    }

    if (outSelection != nullptr) {
        *outSelection = std::move(selection);
    }
    return true;
}

bool parsePhase7ExecutionApplyInventoryArgs(const json& args,
                                            Phase7ExecutionApplyInventorySelection* outSelection,
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
    if (hasUnexpectedKeys(args,
                          {"limit",
                           "execution_journal_artifact_id",
                           "execution_ledger_artifact_id",
                           "playbook_artifact_id",
                           "analysis_artifact_id",
                           "source_artifact_id",
                           "apply_status",
                           "recovery_state",
                           "sort_by"})) {
        return fail("invalid_arguments",
                    "execution apply inventory arguments support only limit, execution_journal_artifact_id, execution_ledger_artifact_id, playbook_artifact_id, analysis_artifact_id, source_artifact_id, apply_status, recovery_state, and sort_by");
    }

    Phase7ExecutionApplyInventorySelection selection;
    if (args.contains("limit")) {
        const auto limit = asPositiveUInt64(args.at("limit"));
        if (!limit.has_value()) {
            return fail("invalid_arguments", "limit must be a positive integer");
        }
        selection.limit = static_cast<std::size_t>(*limit);
    }
    if (args.contains("execution_journal_artifact_id")) {
        const auto journalArtifactId = asNonEmptyString(args.at("execution_journal_artifact_id"));
        if (!journalArtifactId.has_value()) {
            return fail("invalid_arguments", "execution_journal_artifact_id must be a non-empty string");
        }
        selection.journalArtifactId = *journalArtifactId;
    }
    if (args.contains("execution_ledger_artifact_id")) {
        const auto ledgerArtifactId = asNonEmptyString(args.at("execution_ledger_artifact_id"));
        if (!ledgerArtifactId.has_value()) {
            return fail("invalid_arguments", "execution_ledger_artifact_id must be a non-empty string");
        }
        selection.ledgerArtifactId = *ledgerArtifactId;
    }
    if (args.contains("playbook_artifact_id")) {
        const auto playbookArtifactId = asNonEmptyString(args.at("playbook_artifact_id"));
        if (!playbookArtifactId.has_value()) {
            return fail("invalid_arguments", "playbook_artifact_id must be a non-empty string");
        }
        selection.playbookArtifactId = *playbookArtifactId;
    }
    if (args.contains("analysis_artifact_id")) {
        const auto analysisArtifactId = asNonEmptyString(args.at("analysis_artifact_id"));
        if (!analysisArtifactId.has_value()) {
            return fail("invalid_arguments", "analysis_artifact_id must be a non-empty string");
        }
        selection.analysisArtifactId = *analysisArtifactId;
    }
    if (args.contains("source_artifact_id")) {
        const auto sourceArtifactId = asNonEmptyString(args.at("source_artifact_id"));
        if (!sourceArtifactId.has_value()) {
            return fail("invalid_arguments", "source_artifact_id must be a non-empty string");
        }
        selection.sourceArtifactId = *sourceArtifactId;
    }
    if (args.contains("apply_status")) {
        const auto applyStatus = asNonEmptyString(args.at("apply_status"));
        if (!applyStatus.has_value() ||
            (*applyStatus != tape_phase7::kExecutionJournalStatusQueued &&
             *applyStatus != tape_phase7::kExecutionJournalStatusInProgress &&
             *applyStatus != tape_phase7::kExecutionJournalStatusSucceeded &&
             *applyStatus != tape_phase7::kExecutionJournalStatusPartiallySucceeded &&
             *applyStatus != tape_phase7::kExecutionJournalStatusFailed &&
             *applyStatus != tape_phase7::kExecutionJournalStatusCancelled)) {
            return fail("invalid_arguments",
                        "apply_status must be one of execution_queued, execution_in_progress, execution_succeeded, execution_partially_succeeded, execution_failed, or execution_cancelled");
        }
        selection.applyStatus = *applyStatus;
    }
    if (args.contains("recovery_state")) {
        const auto recoveryState = asNonEmptyString(args.at("recovery_state"));
        if (!recoveryState.has_value() ||
            (*recoveryState != "recovery_required" &&
             *recoveryState != "stale_recovery_required")) {
            return fail("invalid_arguments",
                        "recovery_state must be one of recovery_required or stale_recovery_required");
        }
        selection.recoveryState = *recoveryState;
    }
    if (args.contains("sort_by")) {
        const auto sortBy = asNonEmptyString(args.at("sort_by"));
        if (!sortBy.has_value() ||
            (*sortBy != "generated_at_desc" && *sortBy != "attention_desc" && *sortBy != "source_artifact_asc")) {
            return fail("invalid_arguments",
                        "sort_by must be one of generated_at_desc, attention_desc, or source_artifact_asc");
        }
        selection.sortBy = *sortBy;
    }

    if (outSelection != nullptr) {
        *outSelection = std::move(selection);
    }
    return true;
}

bool selectPhase7Findings(const tape_phase7::AnalysisArtifact& artifact,
                          const Phase7FindingSelection& selection,
                          Phase7SelectedFindings* outSelection,
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

    std::vector<tape_phase7::FindingRecord> findings;
    if (selection.findingIds.empty()) {
        findings = artifact.findings;
    } else {
        for (const auto& requestedId : selection.findingIds) {
            const auto it = std::find_if(
                artifact.findings.begin(),
                artifact.findings.end(),
                [&](const tape_phase7::FindingRecord& finding) { return finding.findingId == requestedId; });
            if (it == artifact.findings.end()) {
                return fail("finding_not_found",
                            "finding filters reference a finding that is not present in the analysis artifact");
            }
            if (std::find_if(findings.begin(),
                             findings.end(),
                             [&](const tape_phase7::FindingRecord& finding) {
                                 return finding.findingId == requestedId;
                             }) == findings.end()) {
                findings.push_back(*it);
            }
        }
    }

    if (!selection.minimumSeverity.empty()) {
        const int minimumRank = *phase7SeverityRank(selection.minimumSeverity);
        findings.erase(std::remove_if(findings.begin(),
                                      findings.end(),
                                      [&](const tape_phase7::FindingRecord& finding) {
                                          const auto rank = phase7SeverityRank(finding.severity);
                                          return !rank.has_value() || *rank < minimumRank;
                                      }),
                       findings.end());
    }
    if (!selection.category.empty()) {
        findings.erase(std::remove_if(findings.begin(),
                                      findings.end(),
                                      [&](const tape_phase7::FindingRecord& finding) {
                                          return finding.category != selection.category;
                                      }),
                       findings.end());
    }
    if (selection.limit > 0 && findings.size() > selection.limit) {
        findings.resize(selection.limit);
    }

    Phase7SelectedFindings selected;
    selected.findings = std::move(findings);
    for (const auto& finding : selected.findings) {
        selected.findingIds.push_back(finding.findingId);
    }
    selected.appliedFilters = {
        {"requested_finding_ids", selection.findingIds},
        {"minimum_severity", selection.minimumSeverity.empty() ? json(nullptr) : json(selection.minimumSeverity)},
        {"category", selection.category.empty() ? json(nullptr) : json(selection.category)},
        {"limit", selection.limit == 0 ? json(nullptr) : json(selection.limit)},
        {"selected_count", selected.findings.size()}
    };

    if (outSelection != nullptr) {
        *outSelection = std::move(selected);
    }
    return true;
}

json phase7AnalyzerResultPayload(const tape_phase7::AnalysisArtifact& artifact,
                                 std::string_view artifactStatus = {}) {
    json findings = json::array();
    for (const auto& finding : artifact.findings) {
        findings.push_back(tape_phase7::findingToJson(finding));
    }
    json payload = {
        {"source_artifact", tape_phase7::artifactRefToJson(artifact.sourceArtifact)},
        {"analysis_artifact", tape_phase7::artifactRefToJson(artifact.analysisArtifact)},
        {"generated_artifacts", json::array({tape_phase7::artifactRefToJson(artifact.analysisArtifact)})},
        {"analysis_profile", artifact.analysisProfile},
        {"generated_at_utc", artifact.generatedAtUtc},
        {"finding_count", findings.size()},
        {"replay_context", artifact.replayContext},
        {"findings", std::move(findings)}
    };
    if (!artifactStatus.empty()) {
        payload["artifact_status"] = std::string(artifactStatus);
    }
    return payload;
}

json phase7FindingsListPayload(const tape_phase7::AnalysisArtifact& artifact,
                               const Phase7SelectedFindings& selectedFindings) {
    json findings = json::array();
    for (const auto& finding : selectedFindings.findings) {
        findings.push_back(tape_phase7::findingToJson(finding));
    }
    return {
        {"analysis_artifact", tape_phase7::artifactRefToJson(artifact.analysisArtifact)},
        {"analysis_profile", artifact.analysisProfile},
        {"generated_at_utc", artifact.generatedAtUtc},
        {"finding_count", findings.size()},
        {"filtered_finding_ids", selectedFindings.findingIds},
        {"applied_filters", selectedFindings.appliedFilters},
        {"replay_context", artifact.replayContext},
        {"findings", std::move(findings)}
    };
}

json phase7AnalysisProfilePayload(const tape_phase7::AnalyzerProfileSpec& profile) {
    return {
        {"analysis_profile", profile.analysisProfile},
        {"title", profile.title},
        {"summary", profile.summary},
        {"default_profile", profile.defaultProfile},
        {"supported_source_bundle_types", profile.supportedSourceBundleTypes},
        {"finding_categories", profile.findingCategories}
    };
}

json phase7AnalysisProfileListPayload(const std::vector<tape_phase7::AnalyzerProfileSpec>& profiles) {
    json rows = json::array();
    for (const auto& profile : profiles) {
        rows.push_back(phase7AnalysisProfilePayload(profile));
    }
    return {
        {"returned_count", rows.size()},
        {"analysis_profiles", std::move(rows)}
    };
}

json phase7AnalysisInventoryPayload(const std::vector<tape_phase7::AnalysisArtifact>& artifacts) {
    json appliedFilters = {
        {"source_artifact_id", nullptr},
        {"analysis_profile", nullptr},
        {"limit", nullptr},
        {"matched_count", artifacts.size()}
    };
    json rows = json::array();
    for (const auto& artifact : artifacts) {
        rows.push_back({
            {"analysis_artifact", tape_phase7::artifactRefToJson(artifact.analysisArtifact)},
            {"source_artifact", tape_phase7::artifactRefToJson(artifact.sourceArtifact)},
            {"analysis_profile", artifact.analysisProfile},
            {"generated_at_utc", artifact.generatedAtUtc},
            {"finding_count", artifact.findings.size()},
            {"replay_context", artifact.replayContext}
        });
    }
    return {
        {"returned_count", rows.size()},
        {"applied_filters", std::move(appliedFilters)},
        {"analysis_artifacts", std::move(rows)}
    };
}

json phase7AnalysisInventoryPayload(const std::vector<tape_phase7::AnalysisArtifact>& artifacts,
                                    const Phase7AnalysisInventorySelection& selection,
                                    std::size_t matchedCount) {
    json rows = json::array();
    for (const auto& artifact : artifacts) {
        rows.push_back({
            {"analysis_artifact", tape_phase7::artifactRefToJson(artifact.analysisArtifact)},
            {"source_artifact", tape_phase7::artifactRefToJson(artifact.sourceArtifact)},
            {"analysis_profile", artifact.analysisProfile},
            {"generated_at_utc", artifact.generatedAtUtc},
            {"finding_count", artifact.findings.size()},
            {"replay_context", artifact.replayContext}
        });
    }
    return {
        {"returned_count", rows.size()},
        {"applied_filters", {
            {"source_artifact_id", selection.sourceArtifactId.empty() ? json(nullptr) : json(selection.sourceArtifactId)},
            {"analysis_profile", selection.analysisProfile.empty() ? json(nullptr) : json(selection.analysisProfile)},
            {"limit", selection.limit == 0 ? json(nullptr) : json(selection.limit)},
            {"matched_count", matchedCount}
        }},
        {"analysis_artifacts", std::move(rows)}
    };
}

json phase7PlaybookPayload(const tape_phase7::PlaybookArtifact& playbook,
                           const json& appliedFilters = json::object(),
                           std::string_view artifactStatus = {}) {
    json filteredFindingIds = json::array();
    for (const auto& findingId : playbook.filteredFindingIds) {
        filteredFindingIds.push_back(findingId);
    }
    json plannedActions = json::array();
    for (const auto& action : playbook.plannedActions) {
        plannedActions.push_back(tape_phase7::playbookActionToJson(action));
    }
    json payload = {
        {"analysis_artifact", tape_phase7::artifactRefToJson(playbook.analysisArtifact)},
        {"playbook_artifact", tape_phase7::artifactRefToJson(playbook.playbookArtifact)},
        {"mode", playbook.mode},
        {"generated_at_utc", playbook.generatedAtUtc},
        {"filtered_finding_ids", std::move(filteredFindingIds)},
        {"applied_filters", (appliedFilters.is_object() && !appliedFilters.empty()) ? appliedFilters : json{
            {"requested_finding_ids", playbook.filteredFindingIds},
            {"minimum_severity", nullptr},
            {"category", nullptr},
            {"limit", nullptr},
            {"selected_count", playbook.filteredFindingIds.size()}
        }},
        {"planned_actions", std::move(plannedActions)},
        {"replay_context", playbook.replayContext}
    };
    if (!artifactStatus.empty()) {
        payload["artifact_status"] = std::string(artifactStatus);
    }
    return payload;
}

json phase7PlaybookInventoryPayload(const std::vector<tape_phase7::PlaybookArtifact>& artifacts) {
    json appliedFilters = {
        {"analysis_artifact_id", nullptr},
        {"source_artifact_id", nullptr},
        {"mode", nullptr},
        {"limit", nullptr},
        {"matched_count", artifacts.size()}
    };
    json rows = json::array();
    for (const auto& artifact : artifacts) {
        rows.push_back({
            {"playbook_artifact", tape_phase7::artifactRefToJson(artifact.playbookArtifact)},
            {"analysis_artifact", tape_phase7::artifactRefToJson(artifact.analysisArtifact)},
            {"mode", artifact.mode},
            {"generated_at_utc", artifact.generatedAtUtc},
            {"filtered_finding_count", artifact.filteredFindingIds.size()},
            {"planned_action_count", artifact.plannedActions.size()},
            {"replay_context", artifact.replayContext}
        });
    }
    return {
        {"returned_count", rows.size()},
        {"applied_filters", std::move(appliedFilters)},
        {"playbook_artifacts", std::move(rows)}
    };
}

json phase7PlaybookInventoryPayload(const std::vector<tape_phase7::PlaybookArtifact>& artifacts,
                                    const Phase7PlaybookInventorySelection& selection,
                                    std::size_t matchedCount) {
    json rows = json::array();
    for (const auto& artifact : artifacts) {
        rows.push_back({
            {"playbook_artifact", tape_phase7::artifactRefToJson(artifact.playbookArtifact)},
            {"analysis_artifact", tape_phase7::artifactRefToJson(artifact.analysisArtifact)},
            {"mode", artifact.mode},
            {"generated_at_utc", artifact.generatedAtUtc},
            {"filtered_finding_count", artifact.filteredFindingIds.size()},
            {"planned_action_count", artifact.plannedActions.size()},
            {"replay_context", artifact.replayContext}
        });
    }
    return {
        {"returned_count", rows.size()},
        {"applied_filters", {
            {"analysis_artifact_id", selection.analysisArtifactId.empty() ? json(nullptr) : json(selection.analysisArtifactId)},
            {"source_artifact_id", selection.sourceArtifactId.empty() ? json(nullptr) : json(selection.sourceArtifactId)},
            {"mode", selection.mode.empty() ? json(nullptr) : json(selection.mode)},
            {"limit", selection.limit == 0 ? json(nullptr) : json(selection.limit)},
            {"matched_count", matchedCount}
        }},
        {"playbook_artifacts", std::move(rows)}
    };
}

json phase7ExecutionLedgerPayload(const tape_phase7::ExecutionLedgerArtifact& ledger,
                                  std::string_view artifactStatus = {}) {
    json entries = json::array();
    for (const auto& entry : ledger.entries) {
        entries.push_back(tape_phase7::executionLedgerEntryToJson(entry));
    }
    const auto reviewSummary = tape_phase7::summarizeExecutionLedgerReviewSummary(ledger);
    json payload = {
        {"source_artifact", tape_phase7::artifactRefToJson(ledger.sourceArtifact)},
        {"analysis_artifact", tape_phase7::artifactRefToJson(ledger.analysisArtifact)},
        {"playbook_artifact", tape_phase7::artifactRefToJson(ledger.playbookArtifact)},
        {"execution_ledger", tape_phase7::artifactRefToJson(ledger.ledgerArtifact)},
        {"mode", ledger.mode},
        {"generated_at_utc", ledger.generatedAtUtc},
        {"ledger_status", ledger.ledgerStatus},
        {"execution_policy", ledger.executionPolicy},
        {"filtered_finding_ids", ledger.filteredFindingIds},
        {"entry_count", ledger.entries.size()},
        {"review_summary", tape_phase7::executionLedgerReviewSummaryToJson(reviewSummary)},
        {"latest_audit_event", tape_phase7::latestExecutionLedgerAuditSummary(ledger)},
        {"entries", std::move(entries)},
        {"audit_trail", ledger.auditTrail},
        {"replay_context", ledger.replayContext}
    };
    if (!artifactStatus.empty()) {
        payload["artifact_status"] = std::string(artifactStatus);
    }
    return payload;
}

json phase7ExecutionLedgerReviewPayload(const tape_phase7::ExecutionLedgerArtifact& ledger,
                                        const std::vector<std::string>& updatedEntryIds,
                                        std::string_view auditEventId) {
    json payload = phase7ExecutionLedgerPayload(ledger);
    payload["updated_entry_ids"] = updatedEntryIds;
    payload["audit_event_id"] = std::string(auditEventId);
    return payload;
}

json phase7ExecutionLedgerInventoryPayload(const std::vector<tape_phase7::ExecutionLedgerArtifact>& artifacts,
                                           const Phase7ExecutionLedgerInventorySelection& selection,
                                           std::size_t matchedCount) {
    json rows = json::array();
    for (const auto& artifact : artifacts) {
        const auto reviewSummary = tape_phase7::summarizeExecutionLedgerReviewSummary(artifact);
        rows.push_back({
            {"execution_ledger", tape_phase7::artifactRefToJson(artifact.ledgerArtifact)},
            {"playbook_artifact", tape_phase7::artifactRefToJson(artifact.playbookArtifact)},
            {"analysis_artifact", tape_phase7::artifactRefToJson(artifact.analysisArtifact)},
            {"source_artifact", tape_phase7::artifactRefToJson(artifact.sourceArtifact)},
            {"mode", artifact.mode},
            {"generated_at_utc", artifact.generatedAtUtc},
            {"ledger_status", artifact.ledgerStatus},
            {"entry_count", artifact.entries.size()},
            {"review_summary", tape_phase7::executionLedgerReviewSummaryToJson(reviewSummary)},
            {"latest_audit_event", tape_phase7::latestExecutionLedgerAuditSummary(artifact)},
            {"replay_context", artifact.replayContext}
        });
    }
    return {
        {"returned_count", rows.size()},
        {"applied_filters", {
            {"playbook_artifact_id", selection.playbookArtifactId.empty() ? json(nullptr) : json(selection.playbookArtifactId)},
            {"analysis_artifact_id", selection.analysisArtifactId.empty() ? json(nullptr) : json(selection.analysisArtifactId)},
            {"source_artifact_id", selection.sourceArtifactId.empty() ? json(nullptr) : json(selection.sourceArtifactId)},
            {"ledger_status", selection.ledgerStatus.empty() ? json(nullptr) : json(selection.ledgerStatus)},
            {"sort_by", selection.sortBy.empty() ? json("generated_at_desc") : json(selection.sortBy)},
            {"limit", selection.limit == 0 ? json(nullptr) : json(selection.limit)},
            {"matched_count", matchedCount}
        }},
        {"execution_ledgers", std::move(rows)}
    };
}

json phase7ExecutionJournalPayload(const tape_phase7::ExecutionJournalArtifact& journal,
                                   std::string_view artifactStatus = {}) {
    json entries = json::array();
    for (const auto& entry : journal.entries) {
        entries.push_back(tape_phase7::executionJournalEntryToJson(entry));
    }
    const auto executionSummary = tape_phase7::summarizeExecutionJournalSummary(journal);
    const auto recoverySummary = tape_phase7::summarizeExecutionJournalRecovery(journal);
    json payload = {
        {"source_artifact", tape_phase7::artifactRefToJson(journal.sourceArtifact)},
        {"analysis_artifact", tape_phase7::artifactRefToJson(journal.analysisArtifact)},
        {"playbook_artifact", tape_phase7::artifactRefToJson(journal.playbookArtifact)},
        {"execution_ledger", tape_phase7::artifactRefToJson(journal.ledgerArtifact)},
        {"execution_journal", tape_phase7::artifactRefToJson(journal.journalArtifact)},
        {"mode", journal.mode},
        {"generated_at_utc", journal.generatedAtUtc},
        {"initiated_by", journal.initiatedBy},
        {"execution_capability", journal.executionCapability},
        {"journal_status", journal.journalStatus},
        {"execution_policy", journal.executionPolicy},
        {"filtered_finding_ids", journal.filteredFindingIds},
        {"entry_count", journal.entries.size()},
        {"execution_summary", tape_phase7::executionJournalSummaryToJson(executionSummary)},
        {"runtime_recovery_summary", tape_phase7::executionRecoverySummaryToJson(recoverySummary)},
        {"latest_audit_event", tape_phase7::latestExecutionJournalAuditSummary(journal)},
        {"latest_execution_result_summary", tape_phase7::latestExecutionJournalResultSummary(journal)},
        {"entries", std::move(entries)},
        {"audit_trail", journal.auditTrail},
        {"replay_context", journal.replayContext}
    };
    if (!artifactStatus.empty()) {
        payload["artifact_status"] = std::string(artifactStatus);
    }
    return payload;
}

json phase7ExecutionJournalEventPayload(const tape_phase7::ExecutionJournalArtifact& journal,
                                        const std::vector<std::string>& updatedEntryIds,
                                        std::string_view auditEventId) {
    json payload = phase7ExecutionJournalPayload(journal);
    payload["updated_entry_ids"] = updatedEntryIds;
    payload["audit_event_id"] = std::string(auditEventId);
    return payload;
}

json phase7ExecutionJournalInventoryPayload(const std::vector<tape_phase7::ExecutionJournalArtifact>& artifacts,
                                            const Phase7ExecutionJournalInventorySelection& selection,
                                            std::size_t matchedCount) {
    json rows = json::array();
    for (const auto& artifact : artifacts) {
        const auto executionSummary = tape_phase7::summarizeExecutionJournalSummary(artifact);
        const auto recoverySummary = tape_phase7::summarizeExecutionJournalRecovery(artifact);
        rows.push_back({
            {"execution_journal", tape_phase7::artifactRefToJson(artifact.journalArtifact)},
            {"execution_ledger", tape_phase7::artifactRefToJson(artifact.ledgerArtifact)},
            {"playbook_artifact", tape_phase7::artifactRefToJson(artifact.playbookArtifact)},
            {"analysis_artifact", tape_phase7::artifactRefToJson(artifact.analysisArtifact)},
            {"source_artifact", tape_phase7::artifactRefToJson(artifact.sourceArtifact)},
            {"mode", artifact.mode},
            {"generated_at_utc", artifact.generatedAtUtc},
            {"journal_status", artifact.journalStatus},
            {"entry_count", artifact.entries.size()},
            {"execution_summary", tape_phase7::executionJournalSummaryToJson(executionSummary)},
            {"runtime_recovery_summary", tape_phase7::executionRecoverySummaryToJson(recoverySummary)},
            {"latest_audit_event", tape_phase7::latestExecutionJournalAuditSummary(artifact)},
            {"latest_execution_result_summary", tape_phase7::latestExecutionJournalResultSummary(artifact)},
            {"replay_context", artifact.replayContext}
        });
    }
    json appliedFilters = {
        {"execution_ledger_artifact_id", selection.ledgerArtifactId.empty() ? json(nullptr) : json(selection.ledgerArtifactId)},
        {"playbook_artifact_id", selection.playbookArtifactId.empty() ? json(nullptr) : json(selection.playbookArtifactId)},
        {"analysis_artifact_id", selection.analysisArtifactId.empty() ? json(nullptr) : json(selection.analysisArtifactId)},
        {"source_artifact_id", selection.sourceArtifactId.empty() ? json(nullptr) : json(selection.sourceArtifactId)},
        {"journal_status", selection.journalStatus.empty() ? json(nullptr) : json(selection.journalStatus)},
        {"sort_by", selection.sortBy.empty() ? json("generated_at_desc") : json(selection.sortBy)},
        {"limit", selection.limit == 0 ? json(nullptr) : json(selection.limit)},
        {"matched_count", matchedCount}
    };
    if (!selection.recoveryState.empty()) {
        appliedFilters["recovery_state"] = selection.recoveryState;
    }
    return {
        {"returned_count", rows.size()},
        {"applied_filters", std::move(appliedFilters)},
        {"execution_journals", std::move(rows)}
    };
}

json phase7ExecutionApplyPayload(const tape_phase7::ExecutionApplyArtifact& apply,
                                 std::string_view artifactStatus = {}) {
    json entries = json::array();
    for (const auto& entry : apply.entries) {
        entries.push_back(tape_phase7::executionApplyEntryToJson(entry));
    }
    const auto executionSummary = tape_phase7::summarizeExecutionApplySummary(apply);
    const auto recoverySummary = tape_phase7::summarizeExecutionApplyRecovery(apply);
    json payload = {
        {"source_artifact", tape_phase7::artifactRefToJson(apply.sourceArtifact)},
        {"analysis_artifact", tape_phase7::artifactRefToJson(apply.analysisArtifact)},
        {"playbook_artifact", tape_phase7::artifactRefToJson(apply.playbookArtifact)},
        {"execution_ledger", tape_phase7::artifactRefToJson(apply.ledgerArtifact)},
        {"execution_journal", tape_phase7::artifactRefToJson(apply.journalArtifact)},
        {"execution_apply", tape_phase7::artifactRefToJson(apply.applyArtifact)},
        {"mode", apply.mode},
        {"generated_at_utc", apply.generatedAtUtc},
        {"initiated_by", apply.initiatedBy},
        {"execution_capability", apply.executionCapability},
        {"apply_status", apply.applyStatus},
        {"execution_policy", apply.executionPolicy},
        {"filtered_finding_ids", apply.filteredFindingIds},
        {"entry_count", apply.entries.size()},
        {"execution_summary", tape_phase7::executionApplySummaryToJson(executionSummary)},
        {"runtime_recovery_summary", tape_phase7::executionRecoverySummaryToJson(recoverySummary)},
        {"latest_audit_event", tape_phase7::latestExecutionApplyAuditSummary(apply)},
        {"latest_execution_result_summary", tape_phase7::latestExecutionApplyResultSummary(apply)},
        {"entries", std::move(entries)},
        {"audit_trail", apply.auditTrail},
        {"replay_context", apply.replayContext}
    };
    if (!artifactStatus.empty()) {
        payload["artifact_status"] = std::string(artifactStatus);
    }
    return payload;
}

json phase7ExecutionApplyEventPayload(const tape_phase7::ExecutionApplyArtifact& apply,
                                      const std::vector<std::string>& updatedEntryIds,
                                      std::string_view auditEventId) {
    json payload = phase7ExecutionApplyPayload(apply);
    payload["updated_entry_ids"] = updatedEntryIds;
    payload["audit_event_id"] = std::string(auditEventId);
    return payload;
}

json phase7ExecutionApplyInventoryPayload(const std::vector<tape_phase7::ExecutionApplyArtifact>& artifacts,
                                          const Phase7ExecutionApplyInventorySelection& selection,
                                          std::size_t matchedCount) {
    json rows = json::array();
    for (const auto& artifact : artifacts) {
        const auto executionSummary = tape_phase7::summarizeExecutionApplySummary(artifact);
        const auto recoverySummary = tape_phase7::summarizeExecutionApplyRecovery(artifact);
        rows.push_back({
            {"execution_apply", tape_phase7::artifactRefToJson(artifact.applyArtifact)},
            {"execution_journal", tape_phase7::artifactRefToJson(artifact.journalArtifact)},
            {"execution_ledger", tape_phase7::artifactRefToJson(artifact.ledgerArtifact)},
            {"playbook_artifact", tape_phase7::artifactRefToJson(artifact.playbookArtifact)},
            {"analysis_artifact", tape_phase7::artifactRefToJson(artifact.analysisArtifact)},
            {"source_artifact", tape_phase7::artifactRefToJson(artifact.sourceArtifact)},
            {"mode", artifact.mode},
            {"generated_at_utc", artifact.generatedAtUtc},
            {"apply_status", artifact.applyStatus},
            {"entry_count", artifact.entries.size()},
            {"execution_summary", tape_phase7::executionApplySummaryToJson(executionSummary)},
            {"runtime_recovery_summary", tape_phase7::executionRecoverySummaryToJson(recoverySummary)},
            {"latest_audit_event", tape_phase7::latestExecutionApplyAuditSummary(artifact)},
            {"latest_execution_result_summary", tape_phase7::latestExecutionApplyResultSummary(artifact)},
            {"replay_context", artifact.replayContext}
        });
    }
    json appliedFilters = {
        {"execution_journal_artifact_id", selection.journalArtifactId.empty() ? json(nullptr) : json(selection.journalArtifactId)},
        {"execution_ledger_artifact_id", selection.ledgerArtifactId.empty() ? json(nullptr) : json(selection.ledgerArtifactId)},
        {"playbook_artifact_id", selection.playbookArtifactId.empty() ? json(nullptr) : json(selection.playbookArtifactId)},
        {"analysis_artifact_id", selection.analysisArtifactId.empty() ? json(nullptr) : json(selection.analysisArtifactId)},
        {"source_artifact_id", selection.sourceArtifactId.empty() ? json(nullptr) : json(selection.sourceArtifactId)},
        {"apply_status", selection.applyStatus.empty() ? json(nullptr) : json(selection.applyStatus)},
        {"sort_by", selection.sortBy.empty() ? json("generated_at_desc") : json(selection.sortBy)},
        {"limit", selection.limit == 0 ? json(nullptr) : json(selection.limit)},
        {"matched_count", matchedCount}
    };
    if (!selection.recoveryState.empty()) {
        appliedFilters["recovery_state"] = selection.recoveryState;
    }
    return {
        {"returned_count", rows.size()},
        {"applied_filters", std::move(appliedFilters)},
        {"execution_applies", std::move(rows)}
    };
}

template <typename RecoverySummary>
bool matchesPhase7RecoveryState(std::string_view recoveryState, const RecoverySummary& recovery) {
    if (recoveryState.empty()) {
        return true;
    }
    if (recoveryState == "recovery_required") {
        return recovery.recoveryRequired;
    }
    if (recoveryState == "stale_recovery_required") {
        return recovery.staleRecoveryRequired;
    }
    return false;
}

std::string toolContractVersion(const ToolSpec& tool) {
    return tool.contractVersion.empty() ? std::string(kContractVersion) : tool.contractVersion;
}

std::string toolEngineCommand(const ToolSpec& tool) {
    if (!tool.engineCommand.empty()) {
        return tool.engineCommand;
    }
    return std::string(tape_engine::queryOperationName(tool.engineOperation));
}

json eventRowToJson(const tape_payloads::EventRow& row) {
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

json incidentRowToJson(const tape_payloads::IncidentListRow& row) {
    return json{
        {"logical_incident_id", row.logicalIncidentId},
        {"kind", row.kind},
        {"score", row.score},
        {"title", row.title}
    };
}

json citationToJson(const tape_payloads::EvidenceCitation& citation) {
    return json{
        {"kind", citation.kind},
        {"artifact_id", citation.artifactId},
        {"label", citation.label}
    };
}

json replayRangeToJson(const std::optional<tape_payloads::RangeQuery>& replayRange) {
    if (!replayRange.has_value()) {
        return nullptr;
    }
    return json{
        {"first_session_seq", replayRange->firstSessionSeq},
        {"last_session_seq", replayRange->lastSessionSeq}
    };
}

json investigationResultFromPayload(const tape_payloads::InvestigationPayload& payload) {
    json events = json::array();
    for (const auto& rawEvent : payload.events) {
        events.push_back(eventRowToJson(tape_payloads::parseEventRow(rawEvent)));
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

json qualityResultFromPayload(const tape_payloads::SessionQualityPayload& payload) {
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

json exportResultFromPayload(const tape_payloads::ArtifactExportPayload& payload) {
    return json{
        {"artifact_id", payload.artifactId},
        {"format", payload.format},
        {"served_revision_id", payload.servedRevisionId == 0 ? json(nullptr) : json(payload.servedRevisionId)},
        {"markdown", payload.format == "markdown" ? json(payload.markdown) : json(nullptr)},
        {"bundle", payload.format == "json-bundle" ? payload.bundle : json(nullptr)}
    };
}

json eventListResultFromPayload(const tape_payloads::EventListPayload& payload) {
    json events = json::array();
    for (const auto& row : payload.events) {
        events.push_back(eventRowToJson(row));
    }
    return json{
        {"returned_count", events.size()},
        {"events", std::move(events)}
    };
}

json incidentListResultFromPayload(const tape_payloads::IncidentListPayload& payload) {
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

json listRowsResultFromPayload(const tape_payloads::CollectionRowsPayload& payload,
                               const char* rowKey,
                               const std::function<json(const json&)>& projectRow) {
    json rows = json::array();
    for (const auto& row : payload.rows) {
        rows.push_back(projectRow(row));
    }
    return json{
        {"returned_count", rows.size()},
        {rowKey, std::move(rows)}
    };
}

json replaySnapshotResultFromPayload(const tape_payloads::ReplaySnapshotPayload& payload) {
    return json{
        {"served_revision_id", payload.servedRevisionId == 0 ? json(nullptr) : json(payload.servedRevisionId)},
        {"includes_mutable_tail", payload.includesMutableTail},
        {"target_session_seq", payload.targetSessionSeq},
        {"replayed_through_session_seq", payload.replayedThroughSessionSeq},
        {"applied_event_count", payload.appliedEventCount},
        {"gap_markers_encountered", payload.gapMarkersEncountered},
        {"checkpoint_used", payload.checkpointUsed},
        {"checkpoint_revision_id", payload.checkpointRevisionId},
        {"checkpoint_session_seq", payload.checkpointSessionSeq},
        {"bid_price", payload.bidPrice},
        {"ask_price", payload.askPrice},
        {"last_price", payload.lastPrice},
        {"bid_book", payload.bidBook},
        {"ask_book", payload.askBook},
        {"data_quality", payload.dataQuality}
    };
}

json reportInventoryRowToJson(const tape_payloads::ReportInventoryRow& row) {
    return json{
        {"report_id", row.reportId},
        {"revision_id", row.revisionId},
        {"artifact_id", row.artifactId},
        {"report_type", row.reportType},
        {"headline", row.headline}
    };
}

json reportInventoryResultFromPayload(const tape_payloads::ReportInventoryPayload& payload, bool sessionReports) {
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

json importedCaseRowToJson(const json& row) {
    return json{
        {"artifact_id", row.value("artifact_id", std::string())},
        {"imported_case_id", row.value("imported_case_id", 0ULL)},
        {"bundle_id", row.value("bundle_id", std::string())},
        {"bundle_type", row.value("bundle_type", std::string())},
        {"source_artifact_id", row.value("source_artifact_id", std::string())},
        {"source_report_id", row.value("source_report_id", 0ULL)},
        {"source_revision_id", row.value("source_revision_id", 0ULL)},
        {"first_session_seq", row.value("first_session_seq", 0ULL)},
        {"last_session_seq", row.value("last_session_seq", 0ULL)},
        {"instrument_id", row.value("instrument_id", std::string())},
        {"headline", row.value("headline", std::string())},
        {"file_name", row.value("file_name", std::string())},
        {"source_bundle_path", row.value("source_bundle_path", std::string())},
        {"payload_sha256", row.value("payload_sha256", std::string())}
    };
}

json importedCaseListResultFromPayload(const tape_payloads::ImportedCaseListPayload& payload) {
    json rows = json::array();
    for (const auto& row : payload.importedCases) {
        rows.push_back(importedCaseRowToJson(row.raw));
    }
    return json{
        {"returned_count", rows.size()},
        {"imported_cases", std::move(rows)}
    };
}

json bundleExportResultFromPayload(const tape_payloads::BundleExportPayload& payload) {
    return json{
        {"artifact", payload.artifact},
        {"bundle", payload.bundle},
        {"source_artifact", payload.sourceArtifact},
        {"source_report", payload.sourceReport},
        {"served_revision_id", payload.servedRevisionId == 0 ? json(nullptr) : json(payload.servedRevisionId)},
        {"export_status", payload.raw.value("result", json::object()).value("export_status", std::string())}
    };
}

json bundleImportResultFromPayload(const tape_payloads::CaseBundleImportPayload& payload) {
    return json{
        {"artifact", payload.artifact},
        {"imported_case", importedCaseRowToJson(payload.importedCase.raw)},
        {"import_status", payload.importStatus},
        {"duplicate_import", payload.duplicateImport}
    };
}

json bundleVerifyResultFromPayload(const tape_payloads::BundleVerifyPayload& payload) {
    return json{
        {"artifact", payload.artifact},
        {"bundle", payload.bundle},
        {"source_artifact", payload.sourceArtifact},
        {"source_report", payload.sourceReport},
        {"report_summary", payload.reportSummary},
        {"report_markdown", payload.reportMarkdown},
        {"verify_status", payload.verifyStatus},
        {"import_supported", payload.importSupported},
        {"already_imported", payload.alreadyImported},
        {"can_import", payload.canImport},
        {"import_reason", payload.importReason},
        {"imported_case", payload.hasImportedCase
                              ? importedCaseRowToJson(payload.importedCase.raw)
                              : json(nullptr)},
        {"served_revision_id", payload.servedRevisionId == 0 ? json(nullptr) : json(payload.servedRevisionId)}
    };
}

json seekOrderResultFromPayload(const tape_payloads::SeekOrderPayload& payload) {
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
    std::vector<tape_phase7::AnalysisArtifact> analysisArtifacts;
    std::string phase7Code;
    std::string phase7Message;
    if (!tape_phase7::listAnalysisArtifacts(25, &analysisArtifacts, &phase7Code, &phase7Message)) {
        return json{
            {"resources", json::array()},
            {"meta", resourceErrorMeta(phase7Code, phase7Message)}
        };
    }
    std::vector<tape_phase7::PlaybookArtifact> playbookArtifacts;
    if (!tape_phase7::listPlaybookArtifacts(25, &playbookArtifacts, &phase7Code, &phase7Message)) {
        return json{
            {"resources", json::array()},
            {"meta", resourceErrorMeta(phase7Code, phase7Message)}
        };
    }
    std::vector<tape_phase7::ExecutionLedgerArtifact> executionLedgers;
    if (!tape_phase7::listExecutionLedgers(25, &executionLedgers, &phase7Code, &phase7Message)) {
        return json{
            {"resources", json::array()},
            {"meta", resourceErrorMeta(phase7Code, phase7Message)}
        };
    }
    std::vector<tape_phase7::ExecutionJournalArtifact> executionJournals;
    if (!tape_phase7::listExecutionJournals(25, &executionJournals, &phase7Code, &phase7Message)) {
        return json{
            {"resources", json::array()},
            {"meta", resourceErrorMeta(phase7Code, phase7Message)}
        };
    }
    std::vector<tape_phase7::ExecutionApplyArtifact> executionApplies;
    if (!tape_phase7::listExecutionApplies(25, &executionApplies, &phase7Code, &phase7Message)) {
        return json{
            {"resources", json::array()},
            {"meta", resourceErrorMeta(phase7Code, phase7Message)}
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
    for (const auto& artifact : analysisArtifacts) {
        resources.push_back({
            {"uri", phase7AnalysisUri(artifact.analysisArtifact.artifactId)},
            {"name", artifact.analysisArtifact.artifactId},
            {"title", "Phase 7 analysis: " + artifact.analysisArtifact.artifactId},
            {"description", "Persisted Phase 7 analyzer output JSON view."},
            {"mimeType", "application/json"}
        });
        resources.push_back({
            {"uri", phase7AnalysisMarkdownUri(artifact.analysisArtifact.artifactId)},
            {"name", artifact.analysisArtifact.artifactId + ":markdown"},
            {"title", "Phase 7 analysis markdown: " + artifact.analysisArtifact.artifactId},
            {"description", "Persisted Phase 7 analyzer markdown summary."},
            {"mimeType", "text/markdown"}
        });
    }
    for (const auto& artifact : playbookArtifacts) {
        resources.push_back({
            {"uri", phase7PlaybookUri(artifact.playbookArtifact.artifactId)},
            {"name", artifact.playbookArtifact.artifactId},
            {"title", "Phase 7 playbook: " + artifact.playbookArtifact.artifactId},
            {"description", "Persisted Phase 7 guarded playbook JSON view."},
            {"mimeType", "application/json"}
        });
        resources.push_back({
            {"uri", phase7PlaybookMarkdownUri(artifact.playbookArtifact.artifactId)},
            {"name", artifact.playbookArtifact.artifactId + ":markdown"},
            {"title", "Phase 7 playbook markdown: " + artifact.playbookArtifact.artifactId},
            {"description", "Persisted Phase 7 guarded playbook markdown summary."},
            {"mimeType", "text/markdown"}
        });
    }
    for (const auto& artifact : executionLedgers) {
        resources.push_back({
            {"uri", phase7ExecutionLedgerUri(artifact.ledgerArtifact.artifactId)},
            {"name", artifact.ledgerArtifact.artifactId},
            {"title", "Phase 7 execution ledger: " + artifact.ledgerArtifact.artifactId},
            {"description", "Persisted Phase 7 execution/audit ledger JSON view."},
            {"mimeType", "application/json"}
        });
        resources.push_back({
            {"uri", phase7ExecutionLedgerMarkdownUri(artifact.ledgerArtifact.artifactId)},
            {"name", artifact.ledgerArtifact.artifactId + ":markdown"},
            {"title", "Phase 7 execution ledger markdown: " + artifact.ledgerArtifact.artifactId},
            {"description", "Persisted Phase 7 execution/audit ledger markdown summary."},
            {"mimeType", "text/markdown"}
        });
    }
    for (const auto& artifact : executionJournals) {
        resources.push_back({
            {"uri", phase7ExecutionJournalUri(artifact.journalArtifact.artifactId)},
            {"name", artifact.journalArtifact.artifactId},
            {"title", "Phase 7 execution journal: " + artifact.journalArtifact.artifactId},
            {"description", "Persisted Phase 7 execution journal JSON view."},
            {"mimeType", "application/json"}
        });
        resources.push_back({
            {"uri", phase7ExecutionJournalMarkdownUri(artifact.journalArtifact.artifactId)},
            {"name", artifact.journalArtifact.artifactId + ":markdown"},
            {"title", "Phase 7 execution journal markdown: " + artifact.journalArtifact.artifactId},
            {"description", "Persisted Phase 7 execution journal markdown summary."},
            {"mimeType", "text/markdown"}
        });
    }
    for (const auto& artifact : executionApplies) {
        resources.push_back({
            {"uri", phase7ExecutionApplyUri(artifact.applyArtifact.artifactId)},
            {"name", artifact.applyArtifact.artifactId},
            {"title", "Phase 7 execution apply: " + artifact.applyArtifact.artifactId},
            {"description", "Persisted Phase 7 execution apply JSON view."},
            {"mimeType", "application/json"}
        });
        resources.push_back({
            {"uri", phase7ExecutionApplyMarkdownUri(artifact.applyArtifact.artifactId)},
            {"name", artifact.applyArtifact.artifactId + ":markdown"},
            {"title", "Phase 7 execution apply markdown: " + artifact.applyArtifact.artifactId},
            {"description", "Persisted Phase 7 execution apply markdown summary."},
            {"mimeType", "text/markdown"}
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
    const bool missingReportId =
        (parsed.kind == ResourceKind::SessionReportJson ||
         parsed.kind == ResourceKind::CaseReportJson ||
         parsed.kind == ResourceKind::SessionArtifactMarkdown ||
         parsed.kind == ResourceKind::SessionArtifactJsonBundle ||
         parsed.kind == ResourceKind::CaseArtifactMarkdown ||
         parsed.kind == ResourceKind::CaseArtifactJsonBundle) &&
        parsed.reportId == 0;
    const bool missingArtifactId =
        (parsed.kind == ResourceKind::Phase7AnalysisJson ||
         parsed.kind == ResourceKind::Phase7AnalysisMarkdown ||
         parsed.kind == ResourceKind::Phase7PlaybookJson ||
         parsed.kind == ResourceKind::Phase7PlaybookMarkdown ||
         parsed.kind == ResourceKind::Phase7ExecutionLedgerJson ||
         parsed.kind == ResourceKind::Phase7ExecutionLedgerMarkdown ||
         parsed.kind == ResourceKind::Phase7ExecutionJournalJson ||
         parsed.kind == ResourceKind::Phase7ExecutionJournalMarkdown ||
         parsed.kind == ResourceKind::Phase7ExecutionApplyJson ||
         parsed.kind == ResourceKind::Phase7ExecutionApplyMarkdown) &&
        parsed.artifactId.empty();
    if (parsed.kind == ResourceKind::Unknown || missingReportId || missingArtifactId) {
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
        case ResourceKind::Phase7AnalysisJson:
        case ResourceKind::Phase7AnalysisMarkdown: {
            tape_phase7::AnalysisArtifact artifact;
            std::string code;
            std::string message;
            if (!tape_phase7::loadAnalysisArtifact({}, parsed.artifactId, &artifact, &code, &message)) {
                return json{{"contents", json::array()}, {"meta", resourceErrorMeta(code, message)}};
            }
            if (parsed.kind == ResourceKind::Phase7AnalysisMarkdown) {
                return textContents(resourceUri, tape_phase7::analysisArtifactMarkdown(artifact), "text/markdown");
            }
            return jsonContents(resourceUri, phase7AnalyzerResultPayload(artifact));
        }
        case ResourceKind::Phase7PlaybookJson:
        case ResourceKind::Phase7PlaybookMarkdown: {
            tape_phase7::PlaybookArtifact artifact;
            std::string code;
            std::string message;
            if (!tape_phase7::loadPlaybookArtifact({}, parsed.artifactId, &artifact, &code, &message)) {
                return json{{"contents", json::array()}, {"meta", resourceErrorMeta(code, message)}};
            }
            if (parsed.kind == ResourceKind::Phase7PlaybookMarkdown) {
                return textContents(resourceUri, tape_phase7::playbookArtifactMarkdown(artifact), "text/markdown");
            }
            return jsonContents(resourceUri, phase7PlaybookPayload(artifact));
        }
        case ResourceKind::Phase7ExecutionLedgerJson:
        case ResourceKind::Phase7ExecutionLedgerMarkdown: {
            tape_phase7::ExecutionLedgerArtifact artifact;
            std::string code;
            std::string message;
            if (!tape_phase7::loadExecutionLedgerArtifact({}, parsed.artifactId, &artifact, &code, &message)) {
                return json{{"contents", json::array()}, {"meta", resourceErrorMeta(code, message)}};
            }
            if (parsed.kind == ResourceKind::Phase7ExecutionLedgerMarkdown) {
                return textContents(resourceUri, tape_phase7::executionLedgerArtifactMarkdown(artifact), "text/markdown");
            }
            return jsonContents(resourceUri, phase7ExecutionLedgerPayload(artifact));
        }
        case ResourceKind::Phase7ExecutionJournalJson:
        case ResourceKind::Phase7ExecutionJournalMarkdown: {
            tape_phase7::ExecutionJournalArtifact artifact;
            std::string code;
            std::string message;
            if (!tape_phase7::loadExecutionJournalArtifact({}, parsed.artifactId, &artifact, &code, &message)) {
                return json{{"contents", json::array()}, {"meta", resourceErrorMeta(code, message)}};
            }
            if (parsed.kind == ResourceKind::Phase7ExecutionJournalMarkdown) {
                return textContents(resourceUri, tape_phase7::executionJournalArtifactMarkdown(artifact), "text/markdown");
            }
            return jsonContents(resourceUri, phase7ExecutionJournalPayload(artifact));
        }
        case ResourceKind::Phase7ExecutionApplyJson:
        case ResourceKind::Phase7ExecutionApplyMarkdown: {
            tape_phase7::ExecutionApplyArtifact artifact;
            std::string code;
            std::string message;
            if (!tape_phase7::loadExecutionApplyArtifact({}, parsed.artifactId, &artifact, &code, &message)) {
                return json{{"contents", json::array()}, {"meta", resourceErrorMeta(code, message)}};
            }
            if (parsed.kind == ResourceKind::Phase7ExecutionApplyMarkdown) {
                return textContents(resourceUri, tape_phase7::executionApplyArtifactMarkdown(artifact), "text/markdown");
            }
            return jsonContents(resourceUri, phase7ExecutionApplyPayload(artifact));
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
        case ToolId::ExportSessionBundle:
            return invokeExportSessionBundleTool(tool, args);
        case ToolId::ExportCaseBundle:
            return invokeExportCaseBundleTool(tool, args);
        case ToolId::VerifyBundle:
            return invokeVerifyBundleTool(tool, args);
        case ToolId::ImportCaseBundle:
            return invokeImportCaseBundleTool(tool, args);
        case ToolId::ListImportedCases:
            return invokeListImportedCasesTool(tool, args);
        case ToolId::ReadSessionQuality:
            return invokeReadSessionQualityTool(tool, args);
        case ToolId::ListAnalysisProfiles:
            return invokeListAnalysisProfilesTool(tool, args);
        case ToolId::ReadAnalysisProfile:
            return invokeReadAnalysisProfileTool(tool, args);
        case ToolId::AnalyzerRun:
            return invokeAnalyzerRunTool(tool, args);
        case ToolId::FindingsList:
            return invokeFindingsListTool(tool, args);
        case ToolId::ListAnalysisArtifacts:
            return invokeListAnalysisArtifactsTool(tool, args);
        case ToolId::ReadAnalysisArtifact:
            return invokeReadAnalysisArtifactTool(tool, args);
        case ToolId::PlaybookApply:
            return invokePlaybookApplyTool(tool, args);
        case ToolId::ListPlaybookArtifacts:
            return invokeListPlaybookArtifactsTool(tool, args);
        case ToolId::ReadPlaybookArtifact:
            return invokeReadPlaybookArtifactTool(tool, args);
        case ToolId::PrepareExecutionLedger:
            return invokePrepareExecutionLedgerTool(tool, args);
        case ToolId::ListExecutionLedgers:
            return invokeListExecutionLedgersTool(tool, args);
        case ToolId::ReadExecutionLedger:
            return invokeReadExecutionLedgerTool(tool, args);
        case ToolId::RecordExecutionLedgerReview:
            return invokeRecordExecutionLedgerReviewTool(tool, args);
        case ToolId::StartExecutionJournal:
            return invokeStartExecutionJournalTool(tool, args);
        case ToolId::ListExecutionJournals:
            return invokeListExecutionJournalsTool(tool, args);
        case ToolId::ReadExecutionJournal:
            return invokeReadExecutionJournalTool(tool, args);
        case ToolId::DispatchExecutionJournal:
            return invokeDispatchExecutionJournalTool(tool, args);
        case ToolId::RecordExecutionJournalEvent:
            return invokeRecordExecutionJournalEventTool(tool, args);
        case ToolId::StartExecutionApply:
            return invokeStartExecutionApplyTool(tool, args);
        case ToolId::ListExecutionApplies:
            return invokeListExecutionAppliesTool(tool, args);
        case ToolId::ReadExecutionApply:
            return invokeReadExecutionApplyTool(tool, args);
        case ToolId::RecordExecutionApplyEvent:
            return invokeRecordExecutionApplyEventTool(tool, args);
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
                                              listRowsResultFromPayload(result.value,
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
                                              listRowsResultFromPayload(result.value,
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
                                              listRowsResultFromPayload(result.value,
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
    return makeToolResult(makeSuccessEnvelope(tool,
                                              investigationResultFromPayload(result.value),
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
                                              replaySnapshotResultFromPayload(result.value),
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

json Adapter::invokeExportSessionBundleTool(const ToolSpec& tool, const json& args) const {
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
    const auto result = engineRpc_.exportSessionBundle(BundleExportQuery{.reportId = query.id});
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
                                              bundleExportResultFromPayload(result.value),
                                              revisionFromSummary(result.value.summary)));
}

json Adapter::invokeExportCaseBundleTool(const ToolSpec& tool, const json& args) const {
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
    const auto result = engineRpc_.exportCaseBundle(BundleExportQuery{.reportId = query.id});
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
                                              bundleExportResultFromPayload(result.value),
                                              revisionFromSummary(result.value.summary)));
}

json Adapter::invokeVerifyBundleTool(const ToolSpec& tool, const json& args) const {
    if (!args.is_object()) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                "invalid_arguments",
                                                "bundle_path is required.",
                                                false,
                                                revisionUnavailable()));
    }
    const auto bundlePath = asNonEmptyString(args.value("bundle_path", json()));
    if (!bundlePath.has_value()) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                "invalid_arguments",
                                                "bundle_path is required.",
                                                false,
                                                revisionUnavailable()));
    }
    const auto result = engineRpc_.verifyBundle(BundleImportQuery{.bundlePath = *bundlePath});
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
                                              bundleVerifyResultFromPayload(result.value),
                                              revisionFromSummary(result.value.summary)));
}

json Adapter::invokeImportCaseBundleTool(const ToolSpec& tool, const json& args) const {
    if (!args.is_object()) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                "invalid_arguments",
                                                "bundle_path is required.",
                                                false,
                                                revisionUnavailable()));
    }
    const auto bundlePath = asNonEmptyString(args.value("bundle_path", json()));
    if (!bundlePath.has_value()) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tape_engine::queryOperationName(tool.engineOperation),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                "invalid_arguments",
                                                "bundle_path is required.",
                                                false,
                                                revisionUnavailable()));
    }
    const auto result = engineRpc_.importCaseBundle(BundleImportQuery{.bundlePath = *bundlePath});
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
                                              bundleImportResultFromPayload(result.value),
                                              revisionFromSummary(result.value.summary)));
}

json Adapter::invokeListImportedCasesTool(const ToolSpec& tool, const json& args) const {
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
    const auto result = engineRpc_.listImportedCases(query);
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
                                              importedCaseListResultFromPayload(result.value),
                                              revisionUnavailable()));
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

json Adapter::invokeListAnalysisProfilesTool(const ToolSpec& tool, const json& args) const {
    if (!args.is_object() || !args.empty()) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tool.engineCommand,
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                "invalid_arguments",
                                                "analysis profile inventory does not accept arguments.",
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    std::vector<tape_phase7::AnalyzerProfileSpec> profiles;
    std::string code;
    std::string message;
    if (!tape_phase7::listAnalyzerProfiles(&profiles, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tool.engineCommand,
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    return makeToolResult(makeSuccessEnvelope(tool,
                                              phase7AnalysisProfileListPayload(profiles),
                                              revisionUnavailable()));
}

json Adapter::invokeReadAnalysisProfileTool(const ToolSpec& tool, const json& args) const {
    if (!args.is_object() || hasUnexpectedKeys(args, {"analysis_profile"})) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tool.engineCommand,
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                "invalid_arguments",
                                                "analysis_profile is required.",
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }
    const auto analysisProfile = asNonEmptyString(args.value("analysis_profile", json()));
    if (!analysisProfile.has_value()) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tool.engineCommand,
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                "invalid_arguments",
                                                "analysis_profile must be a non-empty string.",
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    tape_phase7::AnalyzerProfileSpec profile;
    std::string code;
    std::string message;
    if (!tape_phase7::loadAnalyzerProfile(*analysisProfile, &profile, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                tool.engineCommand,
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    return makeToolResult(makeSuccessEnvelope(tool,
                                              phase7AnalysisProfilePayload(profile),
                                              revisionUnavailable()));
}

json Adapter::invokeAnalyzerRunTool(const ToolSpec& tool, const json& args) const {
    if (hasUnexpectedKeys(args, {"case_bundle_path", "report_bundle_path", "case_manifest_path", "report_manifest_path", "analysis_profile"})) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                "invalid_arguments",
                                                "exactly one bundle path and an optional analysis_profile are supported",
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    std::vector<std::string> bundlePaths;
    for (const char* key : {"case_bundle_path", "report_bundle_path", "case_manifest_path", "report_manifest_path"}) {
        if (args.contains(key)) {
            const auto value = asNonEmptyString(args.at(key));
            if (!value.has_value()) {
                return makeToolResult(makeErrorEnvelope(tool.name,
                                                        toolEngineCommand(tool),
                                                        tool.outputSchemaId,
                                                        true,
                                                        false,
                                                        "invalid_arguments",
                                                        "bundle path inputs must be non-empty strings",
                                                        false,
                                                        revisionUnavailable(),
                                                        toolContractVersion(tool)));
            }
            bundlePaths.push_back(*value);
        }
    }
    if (bundlePaths.size() != 1) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                "invalid_arguments",
                                                "exactly one of case_bundle_path, report_bundle_path, case_manifest_path, or report_manifest_path is required",
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    const std::string analysisProfile =
        args.contains("analysis_profile")
            ? asNonEmptyString(args.at("analysis_profile")).value_or(std::string(tape_phase7::kDefaultAnalyzerProfile))
            : std::string(tape_phase7::kDefaultAnalyzerProfile);

    tape_phase7::AnalysisArtifact artifact;
    bool created = false;
    std::string code;
    std::string message;
    if (!tape_phase7::runAnalyzerFromBundlePath(bundlePaths.front(),
                                                analysisProfile,
                                                &artifact,
                                                &created,
                                                &code,
                                                &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    return makeToolResult(makeSuccessEnvelope(tool,
                                              phase7AnalyzerResultPayload(artifact, created ? "created" : "reused"),
                                              revisionFromPhase7ReplayContext(artifact.replayContext)));
}

json Adapter::invokeFindingsListTool(const ToolSpec& tool, const json& args) const {
    Phase7FindingSelection selection;
    std::string code;
    std::string message;
    if (!parsePhase7SelectionArgs(args, &selection, &code, &message, false)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    tape_phase7::AnalysisArtifact artifact;
    if (!tape_phase7::loadAnalysisArtifact(selection.manifestPath,
                                           selection.artifactId,
                                           &artifact,
                                           &code,
                                           &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    Phase7SelectedFindings selectedFindings;
    if (!selectPhase7Findings(artifact, selection, &selectedFindings, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionFromPhase7ReplayContext(artifact.replayContext),
                                                toolContractVersion(tool)));
    }

    return makeToolResult(makeSuccessEnvelope(tool,
                                              phase7FindingsListPayload(artifact, selectedFindings),
                                              revisionFromPhase7ReplayContext(artifact.replayContext)));
}

json Adapter::invokeListAnalysisArtifactsTool(const ToolSpec& tool, const json& args) const {
    Phase7AnalysisInventorySelection selection;
    std::string code;
    std::string message;
    if (!parsePhase7AnalysisInventoryArgs(args, &selection, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    std::vector<tape_phase7::AnalysisArtifact> artifacts;
    if (!tape_phase7::listAnalysisArtifacts(0, &artifacts, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    artifacts.erase(std::remove_if(artifacts.begin(),
                                   artifacts.end(),
                                   [&](const tape_phase7::AnalysisArtifact& artifact) {
                                       if (!selection.sourceArtifactId.empty() &&
                                           artifact.sourceArtifact.artifactId != selection.sourceArtifactId) {
                                           return true;
                                       }
                                       if (!selection.analysisProfile.empty() &&
                                           artifact.analysisProfile != selection.analysisProfile) {
                                           return true;
                                       }
                                       return false;
                                   }),
                    artifacts.end());
    const std::size_t matchedCount = artifacts.size();
    if (selection.limit > 0 && artifacts.size() > selection.limit) {
        artifacts.resize(selection.limit);
    }

    return makeToolResult(makeSuccessEnvelope(tool,
                                              phase7AnalysisInventoryPayload(artifacts, selection, matchedCount),
                                              revisionUnavailable()));
}

json Adapter::invokeReadAnalysisArtifactTool(const ToolSpec& tool, const json& args) const {
    if (hasUnexpectedKeys(args, {"analysis_manifest_path", "analysis_artifact_id"})) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                "invalid_arguments",
                                                "exactly one of analysis_manifest_path or analysis_artifact_id is required",
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    const bool hasManifest = args.contains("analysis_manifest_path");
    const bool hasArtifactId = args.contains("analysis_artifact_id");
    if (hasManifest == hasArtifactId) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                "invalid_arguments",
                                                "exactly one of analysis_manifest_path or analysis_artifact_id is required",
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    const std::string manifestPath =
        hasManifest ? asNonEmptyString(args.at("analysis_manifest_path")).value_or(std::string()) : std::string();
    const std::string artifactId =
        hasArtifactId ? asNonEmptyString(args.at("analysis_artifact_id")).value_or(std::string()) : std::string();
    if ((hasManifest && manifestPath.empty()) || (hasArtifactId && artifactId.empty())) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                "invalid_arguments",
                                                hasManifest ? "analysis_manifest_path must be a non-empty string"
                                                            : "analysis_artifact_id must be a non-empty string",
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    tape_phase7::AnalysisArtifact artifact;
    std::string code;
    std::string message;
    if (!tape_phase7::loadAnalysisArtifact(manifestPath, artifactId, &artifact, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    return makeToolResult(makeSuccessEnvelope(tool,
                                              phase7AnalyzerResultPayload(artifact),
                                              revisionFromPhase7ReplayContext(artifact.replayContext)));
}

json Adapter::invokePlaybookApplyTool(const ToolSpec& tool, const json& args) const {
    Phase7FindingSelection selection;
    std::string code;
    std::string message;
    if (!parsePhase7SelectionArgs(args, &selection, &code, &message, true)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    const std::string mode =
        args.contains("mode")
            ? asNonEmptyString(args.at("mode")).value_or(std::string(tape_phase7::kDefaultPlaybookMode))
            : std::string(tape_phase7::kDefaultPlaybookMode);

    tape_phase7::AnalysisArtifact analysis;
    if (!tape_phase7::loadAnalysisArtifact(selection.manifestPath,
                                           selection.artifactId,
                                           &analysis,
                                           &code,
                                           &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    Phase7SelectedFindings selectedFindings;
    if (!selectPhase7Findings(analysis, selection, &selectedFindings, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionFromPhase7ReplayContext(analysis.replayContext),
                                                toolContractVersion(tool)));
    }

    if (mode == tape_phase7::kApplyPlaybookMode) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                true,
                                                "deferred_behavior",
                                                "mode=apply is intentionally deferred in the guarded Phase 7 slice; rerun with mode=dry_run.",
                                                false,
                                                revisionFromPhase7ReplayContext(analysis.replayContext),
                                                toolContractVersion(tool)));
    }

    tape_phase7::PlaybookArtifact playbook;
    bool created = false;
    if (!tape_phase7::buildGuardedPlaybook(selection.manifestPath,
                                           selection.artifactId,
                                           selectedFindings.findingIds,
                                           mode,
                                           &playbook,
                                           &created,
                                           &code,
                                           &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    return makeToolResult(makeSuccessEnvelope(tool,
                                              phase7PlaybookPayload(playbook,
                                                                   selectedFindings.appliedFilters,
                                                                   created ? "created" : "reused"),
                                              revisionFromPhase7ReplayContext(playbook.replayContext)));
}

json Adapter::invokeListPlaybookArtifactsTool(const ToolSpec& tool, const json& args) const {
    Phase7PlaybookInventorySelection selection;
    std::string code;
    std::string message;
    if (!parsePhase7PlaybookInventoryArgs(args, &selection, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    std::vector<tape_phase7::PlaybookArtifact> artifacts;
    if (!tape_phase7::listPlaybookArtifacts(0, &artifacts, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    artifacts.erase(std::remove_if(artifacts.begin(),
                                   artifacts.end(),
                                   [&](const tape_phase7::PlaybookArtifact& artifact) {
                                       if (!selection.analysisArtifactId.empty() &&
                                           artifact.analysisArtifact.artifactId != selection.analysisArtifactId) {
                                           return true;
                                       }
                                       if (!selection.mode.empty() && artifact.mode != selection.mode) {
                                           return true;
                                       }
                                       if (!selection.sourceArtifactId.empty()) {
                                           tape_phase7::AnalysisArtifact analysisArtifact;
                                           std::string nestedCode;
                                           std::string nestedMessage;
                                           if (!tape_phase7::loadAnalysisArtifact({},
                                                                                  artifact.analysisArtifact.artifactId,
                                                                                  &analysisArtifact,
                                                                                  &nestedCode,
                                                                                  &nestedMessage)) {
                                               return true;
                                           }
                                           if (analysisArtifact.sourceArtifact.artifactId != selection.sourceArtifactId) {
                                               return true;
                                           }
                                       }
                                       return false;
                                   }),
                    artifacts.end());
    const std::size_t matchedCount = artifacts.size();
    if (selection.limit > 0 && artifacts.size() > selection.limit) {
        artifacts.resize(selection.limit);
    }

    return makeToolResult(makeSuccessEnvelope(tool,
                                              phase7PlaybookInventoryPayload(artifacts, selection, matchedCount),
                                              revisionUnavailable()));
}

json Adapter::invokeReadPlaybookArtifactTool(const ToolSpec& tool, const json& args) const {
    std::string manifestPath;
    std::string artifactId;
    std::string code;
    std::string message;
    if (!parsePhase7PlaybookRefArgs(args, &manifestPath, &artifactId, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    tape_phase7::PlaybookArtifact artifact;
    if (!tape_phase7::loadPlaybookArtifact(manifestPath, artifactId, &artifact, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    return makeToolResult(makeSuccessEnvelope(tool,
                                              phase7PlaybookPayload(artifact),
                                              revisionFromPhase7ReplayContext(artifact.replayContext)));
}

json Adapter::invokePrepareExecutionLedgerTool(const ToolSpec& tool, const json& args) const {
    std::string manifestPath;
    std::string artifactId;
    std::string code;
    std::string message;
    if (!parsePhase7PlaybookRefArgs(args, &manifestPath, &artifactId, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    tape_phase7::ExecutionLedgerArtifact artifact;
    bool created = false;
    if (!tape_phase7::buildExecutionLedger(manifestPath,
                                           artifactId,
                                           &artifact,
                                           &created,
                                           &code,
                                           &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    return makeToolResult(makeSuccessEnvelope(tool,
                                              phase7ExecutionLedgerPayload(artifact, created ? "created" : "reused"),
                                              revisionFromPhase7ReplayContext(artifact.replayContext)));
}

json Adapter::invokeListExecutionLedgersTool(const ToolSpec& tool, const json& args) const {
    Phase7ExecutionLedgerInventorySelection selection;
    std::string code;
    std::string message;
    if (!parsePhase7ExecutionLedgerInventoryArgs(args, &selection, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    std::vector<tape_phase7::ExecutionLedgerArtifact> artifacts;
    if (!tape_phase7::listExecutionLedgers(0, &artifacts, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    artifacts.erase(std::remove_if(artifacts.begin(),
                                   artifacts.end(),
                                   [&](const tape_phase7::ExecutionLedgerArtifact& artifact) {
                                       if (!selection.playbookArtifactId.empty() &&
                                           artifact.playbookArtifact.artifactId != selection.playbookArtifactId) {
                                           return true;
                                       }
                                       if (!selection.analysisArtifactId.empty() &&
                                           artifact.analysisArtifact.artifactId != selection.analysisArtifactId) {
                                           return true;
                                       }
                                       if (!selection.sourceArtifactId.empty() &&
                                           artifact.sourceArtifact.artifactId != selection.sourceArtifactId) {
                                           return true;
                                       }
                                       if (!selection.ledgerStatus.empty() &&
                                           artifact.ledgerStatus != selection.ledgerStatus) {
                                           return true;
                                       }
                                       return false;
                                   }),
                    artifacts.end());
    const auto statusRank = [](const std::string& ledgerStatus) {
        if (ledgerStatus == tape_phase7::kLedgerStatusBlocked) {
            return 0;
        }
        if (ledgerStatus == tape_phase7::kLedgerStatusNeedsInformation) {
            return 1;
        }
        if (ledgerStatus == tape_phase7::kLedgerStatusWaitingApproval) {
            return 2;
        }
        if (ledgerStatus == tape_phase7::kLedgerStatusReadyForExecution) {
            return 3;
        }
        if (ledgerStatus == tape_phase7::kLedgerStatusInProgress) {
            return 4;
        }
        if (ledgerStatus == tape_phase7::kDefaultLedgerStatus) {
            return 5;
        }
        if (ledgerStatus == tape_phase7::kLedgerStatusCompleted) {
            return 6;
        }
        return 7;
    };
    std::sort(artifacts.begin(),
              artifacts.end(),
              [&](const tape_phase7::ExecutionLedgerArtifact& lhs,
                  const tape_phase7::ExecutionLedgerArtifact& rhs) {
                  if (selection.sortBy == "attention_desc") {
                      const int lhsRank = statusRank(lhs.ledgerStatus);
                      const int rhsRank = statusRank(rhs.ledgerStatus);
                      if (lhsRank != rhsRank) {
                          return lhsRank < rhsRank;
                      }
                  } else if (selection.sortBy == "source_artifact_asc") {
                      if (lhs.sourceArtifact.artifactId != rhs.sourceArtifact.artifactId) {
                          return lhs.sourceArtifact.artifactId < rhs.sourceArtifact.artifactId;
                      }
                  }
                  if (lhs.generatedAtUtc != rhs.generatedAtUtc) {
                      return lhs.generatedAtUtc > rhs.generatedAtUtc;
                  }
                  return lhs.ledgerArtifact.artifactId < rhs.ledgerArtifact.artifactId;
              });
    const std::size_t matchedCount = artifacts.size();
    if (selection.limit > 0 && artifacts.size() > selection.limit) {
        artifacts.resize(selection.limit);
    }

    return makeToolResult(makeSuccessEnvelope(tool,
                                              phase7ExecutionLedgerInventoryPayload(artifacts, selection, matchedCount),
                                              revisionUnavailable()));
}

json Adapter::invokeReadExecutionLedgerTool(const ToolSpec& tool, const json& args) const {
    std::string manifestPath;
    std::string artifactId;
    std::string code;
    std::string message;
    json ledgerRefArgs = json::object();
    if (args.contains("execution_ledger_manifest_path")) {
        ledgerRefArgs["execution_ledger_manifest_path"] = args.at("execution_ledger_manifest_path");
    }
    if (args.contains("execution_ledger_artifact_id")) {
        ledgerRefArgs["execution_ledger_artifact_id"] = args.at("execution_ledger_artifact_id");
    }
    if (!parsePhase7ExecutionLedgerRefArgs(ledgerRefArgs, &manifestPath, &artifactId, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    tape_phase7::ExecutionLedgerArtifact artifact;
    if (!tape_phase7::loadExecutionLedgerArtifact(manifestPath, artifactId, &artifact, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    return makeToolResult(makeSuccessEnvelope(tool,
                                              phase7ExecutionLedgerPayload(artifact),
                                              revisionFromPhase7ReplayContext(artifact.replayContext)));
}

json Adapter::invokeRecordExecutionLedgerReviewTool(const ToolSpec& tool, const json& args) const {
    Phase7ExecutionLedgerReviewSelection selection;
    std::string code;
    std::string message;
    if (!parsePhase7ExecutionLedgerReviewArgs(args, &selection, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    tape_phase7::ExecutionLedgerArtifact artifact;
    std::vector<std::string> updatedEntryIds;
    std::string auditEventId;
    if (!tape_phase7::recordExecutionLedgerReview(selection.manifestPath,
                                                  selection.artifactId,
                                                  selection.entryIds,
                                                  selection.reviewStatus,
                                                  selection.actor,
                                                  selection.comment,
                                                  &artifact,
                                                  &updatedEntryIds,
                                                  &auditEventId,
                                                  &code,
                                                  &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    return makeToolResult(makeSuccessEnvelope(tool,
                                              phase7ExecutionLedgerReviewPayload(artifact, updatedEntryIds, auditEventId),
                                              revisionFromPhase7ReplayContext(artifact.replayContext)));
}

json Adapter::invokeStartExecutionJournalTool(const ToolSpec& tool, const json& args) const {
    if (!args.is_object() ||
        hasUnexpectedKeys(args, {"execution_ledger_manifest_path", "execution_ledger_artifact_id", "actor", "execution_capability"})) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                "invalid_arguments",
                                                "execution journal start requires one ledger reference plus actor and execution_capability",
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    std::string manifestPath;
    std::string artifactId;
    std::string code;
    std::string message;
    json ledgerRefArgs = json::object();
    if (args.contains("execution_ledger_manifest_path")) {
        ledgerRefArgs["execution_ledger_manifest_path"] = args.at("execution_ledger_manifest_path");
    }
    if (args.contains("execution_ledger_artifact_id")) {
        ledgerRefArgs["execution_ledger_artifact_id"] = args.at("execution_ledger_artifact_id");
    }
    if (!parsePhase7ExecutionLedgerRefArgs(ledgerRefArgs, &manifestPath, &artifactId, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    const auto actor = asNonEmptyString(args.value("actor", json()));
    const auto executionCapability = asNonEmptyString(args.value("execution_capability", json()));
    if (!actor.has_value() || !executionCapability.has_value()) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                "invalid_arguments",
                                                "actor and execution_capability are required",
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    tape_phase7::ExecutionJournalArtifact artifact;
    bool created = false;
    if (!tape_phase7::startExecutionJournal(manifestPath,
                                            artifactId,
                                            *actor,
                                            *executionCapability,
                                            &artifact,
                                            &created,
                                            &code,
                                            &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    return makeToolResult(makeSuccessEnvelope(tool,
                                              phase7ExecutionJournalPayload(artifact, created ? "created" : "reused"),
                                              revisionFromPhase7ReplayContext(artifact.replayContext)));
}

json Adapter::invokeListExecutionJournalsTool(const ToolSpec& tool, const json& args) const {
    Phase7ExecutionJournalInventorySelection selection;
    std::string code;
    std::string message;
    if (!parsePhase7ExecutionJournalInventoryArgs(args, &selection, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    std::vector<tape_phase7::ExecutionJournalArtifact> artifacts;
    if (!tape_phase7::listExecutionJournals(0, &artifacts, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    artifacts.erase(std::remove_if(artifacts.begin(),
                                   artifacts.end(),
                                   [&](const tape_phase7::ExecutionJournalArtifact& artifact) {
                                       if (!selection.ledgerArtifactId.empty() &&
                                           artifact.ledgerArtifact.artifactId != selection.ledgerArtifactId) {
                                           return true;
                                       }
                                       if (!selection.playbookArtifactId.empty() &&
                                           artifact.playbookArtifact.artifactId != selection.playbookArtifactId) {
                                           return true;
                                       }
                                       if (!selection.analysisArtifactId.empty() &&
                                           artifact.analysisArtifact.artifactId != selection.analysisArtifactId) {
                                           return true;
                                       }
                                       if (!selection.sourceArtifactId.empty() &&
                                           artifact.sourceArtifact.artifactId != selection.sourceArtifactId) {
                                           return true;
                                       }
                                       if (!selection.journalStatus.empty() &&
                                           artifact.journalStatus != selection.journalStatus) {
                                           return true;
                                       }
                                       if (!matchesPhase7RecoveryState(selection.recoveryState,
                                                                       tape_phase7::summarizeExecutionJournalRecovery(artifact))) {
                                           return true;
                                       }
                                       return false;
                                   }),
                    artifacts.end());
    const auto statusRank = [](const std::string& journalStatus) {
        if (journalStatus == tape_phase7::kExecutionJournalStatusFailed) {
            return 0;
        }
        if (journalStatus == tape_phase7::kExecutionJournalStatusCancelled) {
            return 1;
        }
        if (journalStatus == tape_phase7::kExecutionJournalStatusInProgress) {
            return 2;
        }
        if (journalStatus == tape_phase7::kExecutionJournalStatusQueued) {
            return 3;
        }
        if (journalStatus == tape_phase7::kExecutionJournalStatusPartiallySucceeded) {
            return 4;
        }
        if (journalStatus == tape_phase7::kExecutionJournalStatusSucceeded) {
            return 5;
        }
        return 6;
    };
    std::sort(artifacts.begin(),
              artifacts.end(),
              [&](const tape_phase7::ExecutionJournalArtifact& lhs,
                  const tape_phase7::ExecutionJournalArtifact& rhs) {
                  if (selection.sortBy == "attention_desc") {
                      const auto lhsRecovery = tape_phase7::summarizeExecutionJournalRecovery(lhs);
                      const auto rhsRecovery = tape_phase7::summarizeExecutionJournalRecovery(rhs);
                      if (lhsRecovery.staleRecoveryRequired != rhsRecovery.staleRecoveryRequired) {
                          return lhsRecovery.staleRecoveryRequired && !rhsRecovery.staleRecoveryRequired;
                      }
                      if (lhsRecovery.recoveryRequired != rhsRecovery.recoveryRequired) {
                          return lhsRecovery.recoveryRequired && !rhsRecovery.recoveryRequired;
                      }
                      const int lhsRank = statusRank(lhs.journalStatus);
                      const int rhsRank = statusRank(rhs.journalStatus);
                      if (lhsRank != rhsRank) {
                          return lhsRank < rhsRank;
                      }
                  } else if (selection.sortBy == "source_artifact_asc") {
                      if (lhs.sourceArtifact.artifactId != rhs.sourceArtifact.artifactId) {
                          return lhs.sourceArtifact.artifactId < rhs.sourceArtifact.artifactId;
                      }
                  }
                  if (lhs.generatedAtUtc != rhs.generatedAtUtc) {
                      return lhs.generatedAtUtc > rhs.generatedAtUtc;
                  }
                  return lhs.journalArtifact.artifactId < rhs.journalArtifact.artifactId;
              });
    const std::size_t matchedCount = artifacts.size();
    if (selection.limit > 0 && artifacts.size() > selection.limit) {
        artifacts.resize(selection.limit);
    }

    return makeToolResult(makeSuccessEnvelope(tool,
                                              phase7ExecutionJournalInventoryPayload(artifacts, selection, matchedCount),
                                              revisionUnavailable()));
}

json Adapter::invokeReadExecutionJournalTool(const ToolSpec& tool, const json& args) const {
    std::string manifestPath;
    std::string artifactId;
    std::string code;
    std::string message;
    if (!parsePhase7ExecutionJournalRefArgs(args, &manifestPath, &artifactId, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    tape_phase7::ExecutionJournalArtifact artifact;
    if (!tape_phase7::loadExecutionJournalArtifact(manifestPath, artifactId, &artifact, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    return makeToolResult(makeSuccessEnvelope(tool,
                                              phase7ExecutionJournalPayload(artifact),
                                              revisionFromPhase7ReplayContext(artifact.replayContext)));
}

json Adapter::invokeDispatchExecutionJournalTool(const ToolSpec& tool, const json& args) const {
    Phase7ExecutionJournalDispatchSelection selection;
    std::string code;
    std::string message;
    if (!parsePhase7ExecutionJournalDispatchArgs(args, &selection, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    tape_phase7::ExecutionJournalArtifact artifact;
    std::vector<std::string> updatedEntryIds;
    std::string auditEventId;
    if (!tape_phase7::dispatchExecutionJournalEntries(selection.manifestPath,
                                                      selection.artifactId,
                                                      selection.entryIds,
                                                      selection.actor,
                                                      selection.executionCapability,
                                                      selection.comment,
                                                      &artifact,
                                                      &updatedEntryIds,
                                                      &auditEventId,
                                                      &code,
                                                      &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    return makeToolResult(makeSuccessEnvelope(tool,
                                              phase7ExecutionJournalEventPayload(artifact, updatedEntryIds, auditEventId),
                                              revisionFromPhase7ReplayContext(artifact.replayContext)));
}

json Adapter::invokeRecordExecutionJournalEventTool(const ToolSpec& tool, const json& args) const {
    Phase7ExecutionJournalEventSelection selection;
    std::string code;
    std::string message;
    if (!parsePhase7ExecutionJournalEventArgs(args, &selection, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    tape_phase7::ExecutionJournalArtifact artifact;
    std::vector<std::string> updatedEntryIds;
    std::string auditEventId;
    if (!tape_phase7::recordExecutionJournalEvent(selection.manifestPath,
                                                  selection.artifactId,
                                                  selection.entryIds,
                                                  selection.executionStatus,
                                                  selection.actor,
                                                  selection.comment,
                                                  selection.failureCode,
                                                  selection.failureMessage,
                                                  &artifact,
                                                  &updatedEntryIds,
                                                  &auditEventId,
                                                  &code,
                                                  &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    return makeToolResult(makeSuccessEnvelope(tool,
                                              phase7ExecutionJournalEventPayload(artifact, updatedEntryIds, auditEventId),
                                              revisionFromPhase7ReplayContext(artifact.replayContext)));
}

json Adapter::invokeStartExecutionApplyTool(const ToolSpec& tool, const json& args) const {
    Phase7ExecutionApplySelection selection;
    std::string code;
    std::string message;
    if (!parsePhase7ExecutionApplyArgs(args, &selection, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    tape_phase7::ExecutionApplyArtifact artifact;
    bool created = false;
    if (!tape_phase7::startExecutionApply(selection.manifestPath,
                                          selection.artifactId,
                                          selection.entryIds,
                                          selection.actor,
                                          selection.executionCapability,
                                          selection.comment,
                                          &artifact,
                                          &created,
                                          &code,
                                          &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    return makeToolResult(makeSuccessEnvelope(tool,
                                              phase7ExecutionApplyPayload(artifact, created ? "created" : "reused"),
                                              revisionFromPhase7ReplayContext(artifact.replayContext)));
}

json Adapter::invokeListExecutionAppliesTool(const ToolSpec& tool, const json& args) const {
    Phase7ExecutionApplyInventorySelection selection;
    std::string code;
    std::string message;
    if (!parsePhase7ExecutionApplyInventoryArgs(args, &selection, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    std::vector<tape_phase7::ExecutionApplyArtifact> artifacts;
    if (!tape_phase7::listExecutionApplies(0, &artifacts, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    artifacts.erase(std::remove_if(artifacts.begin(),
                                   artifacts.end(),
                                   [&](const tape_phase7::ExecutionApplyArtifact& artifact) {
                                       if (!selection.journalArtifactId.empty() &&
                                           artifact.journalArtifact.artifactId != selection.journalArtifactId) {
                                           return true;
                                       }
                                       if (!selection.ledgerArtifactId.empty() &&
                                           artifact.ledgerArtifact.artifactId != selection.ledgerArtifactId) {
                                           return true;
                                       }
                                       if (!selection.playbookArtifactId.empty() &&
                                           artifact.playbookArtifact.artifactId != selection.playbookArtifactId) {
                                           return true;
                                       }
                                       if (!selection.analysisArtifactId.empty() &&
                                           artifact.analysisArtifact.artifactId != selection.analysisArtifactId) {
                                           return true;
                                       }
                                       if (!selection.sourceArtifactId.empty() &&
                                           artifact.sourceArtifact.artifactId != selection.sourceArtifactId) {
                                           return true;
                                       }
                                       if (!selection.applyStatus.empty() &&
                                           artifact.applyStatus != selection.applyStatus) {
                                           return true;
                                       }
                                       if (!matchesPhase7RecoveryState(selection.recoveryState,
                                                                       tape_phase7::summarizeExecutionApplyRecovery(artifact))) {
                                           return true;
                                       }
                                       return false;
                                   }),
                    artifacts.end());
    const auto statusRank = [](const std::string& applyStatus) {
        if (applyStatus == tape_phase7::kExecutionJournalStatusFailed) {
            return 0;
        }
        if (applyStatus == tape_phase7::kExecutionJournalStatusCancelled) {
            return 1;
        }
        if (applyStatus == tape_phase7::kExecutionJournalStatusInProgress) {
            return 2;
        }
        if (applyStatus == tape_phase7::kExecutionJournalStatusQueued) {
            return 3;
        }
        if (applyStatus == tape_phase7::kExecutionJournalStatusPartiallySucceeded) {
            return 4;
        }
        if (applyStatus == tape_phase7::kExecutionJournalStatusSucceeded) {
            return 5;
        }
        return 6;
    };
    const std::string sortMode(selection.sortBy.empty() ? "generated_at_desc" : selection.sortBy);
    std::sort(artifacts.begin(),
              artifacts.end(),
              [&](const tape_phase7::ExecutionApplyArtifact& lhs,
                  const tape_phase7::ExecutionApplyArtifact& rhs) {
                  if (sortMode == "attention_desc") {
                      const auto lhsRecovery = tape_phase7::summarizeExecutionApplyRecovery(lhs);
                      const auto rhsRecovery = tape_phase7::summarizeExecutionApplyRecovery(rhs);
                      if (lhsRecovery.staleRecoveryRequired != rhsRecovery.staleRecoveryRequired) {
                          return lhsRecovery.staleRecoveryRequired && !rhsRecovery.staleRecoveryRequired;
                      }
                      if (lhsRecovery.recoveryRequired != rhsRecovery.recoveryRequired) {
                          return lhsRecovery.recoveryRequired && !rhsRecovery.recoveryRequired;
                      }
                      const int lhsRank = statusRank(lhs.applyStatus);
                      const int rhsRank = statusRank(rhs.applyStatus);
                      if (lhsRank != rhsRank) {
                          return lhsRank < rhsRank;
                      }
                  } else if (sortMode == "source_artifact_asc") {
                      if (lhs.sourceArtifact.artifactId != rhs.sourceArtifact.artifactId) {
                          return lhs.sourceArtifact.artifactId < rhs.sourceArtifact.artifactId;
                      }
                  }
                  if (lhs.generatedAtUtc != rhs.generatedAtUtc) {
                      return lhs.generatedAtUtc > rhs.generatedAtUtc;
                  }
                  return lhs.applyArtifact.artifactId < rhs.applyArtifact.artifactId;
              });
    const std::size_t matchedCount = artifacts.size();
    if (selection.limit > 0 && artifacts.size() > selection.limit) {
        artifacts.resize(selection.limit);
    }

    return makeToolResult(makeSuccessEnvelope(tool,
                                              phase7ExecutionApplyInventoryPayload(artifacts, selection, matchedCount),
                                              revisionUnavailable()));
}

json Adapter::invokeReadExecutionApplyTool(const ToolSpec& tool, const json& args) const {
    std::string manifestPath;
    std::string artifactId;
    std::string code;
    std::string message;
    if (!parsePhase7ExecutionApplyRefArgs(args, &manifestPath, &artifactId, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    tape_phase7::ExecutionApplyArtifact artifact;
    if (!tape_phase7::loadExecutionApplyArtifact(manifestPath, artifactId, &artifact, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    return makeToolResult(makeSuccessEnvelope(tool,
                                              phase7ExecutionApplyPayload(artifact),
                                              revisionFromPhase7ReplayContext(artifact.replayContext)));
}

json Adapter::invokeRecordExecutionApplyEventTool(const ToolSpec& tool, const json& args) const {
    Phase7ExecutionApplyEventSelection selection;
    std::string code;
    std::string message;
    if (!parsePhase7ExecutionApplyEventArgs(args, &selection, &code, &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    tape_phase7::ExecutionApplyArtifact artifact;
    std::vector<std::string> updatedEntryIds;
    std::string auditEventId;
    if (!tape_phase7::recordExecutionApplyEvent(selection.manifestPath,
                                                selection.artifactId,
                                                selection.entryIds,
                                                selection.executionStatus,
                                                selection.actor,
                                                selection.comment,
                                                selection.failureCode,
                                                selection.failureMessage,
                                                &artifact,
                                                &updatedEntryIds,
                                                &auditEventId,
                                                &code,
                                                &message)) {
        return makeToolResult(makeErrorEnvelope(tool.name,
                                                toolEngineCommand(tool),
                                                tool.outputSchemaId,
                                                true,
                                                false,
                                                code,
                                                message,
                                                false,
                                                revisionUnavailable(),
                                                toolContractVersion(tool)));
    }

    return makeToolResult(makeSuccessEnvelope(tool,
                                              phase7ExecutionApplyEventPayload(artifact, updatedEntryIds, auditEventId),
                                              revisionFromPhase7ReplayContext(artifact.replayContext)));
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
    const std::string engineCommand = toolEngineCommand(tool);
    return json{
        {"ok", true},
        {"meta", {
            {"contract_version", toolContractVersion(tool)},
            {"tool", tool.name},
            {"engine_operation", engineCommand},
            {"engine_command", engineCommand},
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
                                json revision,
                                std::string contractVersion) const {
    return json{
        {"ok", false},
        {"meta", {
            {"contract_version", contractVersion.empty() ? json(kContractVersion) : json(contractVersion)},
            {"tool", toolName},
            {"engine_operation", std::string(engineOperation)},
            {"engine_command", std::string(engineOperation)},
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
