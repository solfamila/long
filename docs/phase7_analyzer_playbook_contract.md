# Phase 7 Analyzer, Findings, and Guarded Playbook Contract (Slice 1)

## Purpose
Freeze the first supported Phase 7 MCP boundary for:

- `tapescript_analyzer_run`
- `tapescript_findings_list`
- `tapescript_playbook_apply`

This slice is intentionally local and artifact-backed. It must not depend on new engine seam commands, external services, or unguarded mutation behavior.

## Repo Reality Baseline (Before Phase 7)
Current branch evidence shows these three tool IDs are placeholders, not implemented behavior:

- `tape_mcp_adapter.cpp` registers the three IDs only in a deferred list (`deferredTools`) with `reservedDeferred=true`.
- `Adapter::invokeSupportedReadTool` routes only Phase 5 and Phase 6 tools (`status`, `read_live_tail`, `read_range`, `find_order_anchor`, `report_generate`, `export_range`), with no analyzer/findings/playbook handler.
- `Adapter::invokeReservedDeferredTool` returns `error.code="deferred_tool"` for reserved IDs.
- `tests/tape_mcp_contract_tests.cpp` asserts `tapescript_analyzer_run` returns `deferred_tool` in both direct adapter and stdio MCP paths.
- No other repo implementation of `analyzer_run`, `findings_list`, or `playbook_apply` exists outside deferred placeholders.

This makes the Phase 7 slice net-new functionality, not an extension of existing analyzer code.

## Source-of-Truth Inputs (Repo-Backed)
Phase 7 analysis must read from persisted artifacts produced by Phase 6, not live runtime-only state.

Required source fields are derived from Phase 6 manifests and outputs already present in repo code paths:

- `contract_version`, `artifact_type`, `artifact_id`
- `trace_anchor`
- `evidence`
- `revision_context`
- `generated_at_utc`
- source artifact `manifest_path`

These fields are emitted by `trace_exporter.cpp` manifest writers and surfaced by `tape_mcp_adapter.cpp` in Phase 6 tool results.

## Supported MCP Boundary (Phase 7 Slice 1)
The first supported Phase 7 MCP surface is:

| MCP tool id | Phase 7 Slice 1 status | Engine command marker | Input boundary | Output boundary |
| --- | --- | --- | --- | --- |
| `tapescript_analyzer_run` | supported | `phase7_analyzer_run_local` | exactly one of `case_manifest_path` or `report_manifest_path`; optional `analysis_profile` | writes and returns one analyzer artifact manifest and findings payload |
| `tapescript_findings_list` | supported | `phase7_findings_list_local` | exactly one of `analysis_manifest_path` or `analysis_artifact_id` | returns findings from stored analyzer artifact only |
| `tapescript_playbook_apply` | supported (guarded only) | `phase7_playbook_apply_guarded_local` | analyzer reference plus optional finding filters; optional `mode` | returns dry-run plan/result only; no unguarded mutation |

Notes:

- These tools are snapshot-style, artifact-local operations.
- They do not require new query-seam commands.
- Existing Phase 5/6 tools remain unchanged in this slice.

## Result and Error Envelope Contract
Each `tools/call` response continues to use existing wrapper shape:

- `isError`: boolean
- `structuredContent`: contract envelope
- `content[0].text`: serialized envelope JSON

The `structuredContent` envelope MUST remain:

```json
{
  "ok": true,
  "result": {},
  "meta": {
    "contract_version": "phase7-analyzer-playbook-v1",
    "tool": "tapescript_analyzer_run",
    "engine_command": "phase7_analyzer_run_local",
    "supported": true,
    "deferred": false,
    "revision": {
      "manifest_hash": null,
      "latest_session_seq": null,
      "first_session_seq": null,
      "last_session_seq": null,
      "source": "artifact_manifest",
      "staleness": "snapshot"
    }
  },
  "error": null
}
```

Error envelope remains:

- `ok=false`
- `result=null`
- `meta` present with `supported` and `deferred`
- `error={code,message,retryable}`

Required Phase 7 error codes:

- `invalid_arguments`
- `artifact_not_found`
- `artifact_load_failed`
- `unsupported_source_contract`
- `analysis_failed`
- `finding_not_found`
- `deferred_behavior`
- `unsupported_tool`

Deferred-vs-supported signaling rules:

- `meta.supported=true`, `meta.deferred=true` for deferred behavior within an otherwise supported tool.
- `meta.supported=false`, `meta.deferred=false` for unknown/unregistered tool IDs (`unsupported_tool`).

## Artifact Identifiers and Replayability
Phase 7 outputs must be replayable from repo artifacts.

### Analyzer output requirements
`result` from `tapescript_analyzer_run` MUST include:

- `source_artifact`: `{artifact_type, contract_version, artifact_id, manifest_path}`
- `analysis_artifact`: `{artifact_type, contract_version, artifact_id, manifest_path, artifact_root_dir}`
- `generated_artifacts`: array including analyzer artifact identity
- `replay_context`:
  - `trace_anchor`
  - `revision_context`
  - source `generated_at_utc`
  - optional `requested_window` when available from source artifacts

Recommended analyzer artifact type/contract identifiers for this slice:

- `artifact_type = "phase7.analysis_output.v1"`
- `contract_version = "phase7-analyzer-playbook-v1"`

### Findings list requirements
`result` from `tapescript_findings_list` MUST include:

- analyzer artifact identity (`artifact_id`, `manifest_path`, `artifact_type`)
- `findings`: array of stable finding records
- `replay_context` copied from analyzer artifact manifest

Finding records MUST include stable IDs to make downstream playbook calls deterministic:

- `finding_id`
- `severity`
- `category`
- `summary`
- `evidence_refs`

### Guarded playbook requirements
`tapescript_playbook_apply` in Slice 1 is guarded-only:

- default `mode` is `dry_run`
- `mode="apply"` is explicitly deferred (`deferred_behavior`)
- response includes `planned_actions` with deterministic action IDs and no irreversible side effects

Recommended playbook artifact identifier when emitted:

- `artifact_type = "phase7.playbook_plan.v1"`
- `contract_version = "phase7-analyzer-playbook-v1"`

## Explicitly Deferred Behavior
Deferred behavior is explicit in Slice 1 and must not be implicit:

- Unrestricted playbook mutation (`mode="apply"` or equivalent side-effecting paths).
- External/non-repo analyzers or remote model execution.
- Analyzer execution that bypasses repo artifacts and depends directly on live runtime streams.
- Cross-run findings aggregation beyond one analyzer artifact reference.

For deferred behavior inside supported Phase 7 tools:

- return `error.code="deferred_behavior"`
- set `meta.supported=true`
- set `meta.deferred=true`

## MCP Contract Evolution Requirement
Phase 7 implementation must update `meta.contract_version` from current `phase5-mcp-compat-v1` to:

- `phase7-analyzer-playbook-v1`

for all three newly supported Phase 7 tools, while preserving envelope shape and error semantics established by existing adapter behavior.
