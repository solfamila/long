# Phase 5 MCP Compatibility Contract (Slice 1)

## Purpose
Freeze the first supported `tape-mcp` compatibility surface against current branch reality.
This contract is intentionally limited to the stable app/engine query seam:

- `status`
- `read_live_tail`
- `read_range`
- `find_order_anchor`

Out of scope for this slice: adapter process internals, engine query seam changes, and analyzer/playbook/report/export execution behavior.

## Branch Reality Baseline
- No `tape-mcp` target/process exists in this branch yet.
- No existing `tapescript_*` adapter implementation exists in this branch yet.
- `tapescope::QueryClient` already proves request/response shaping for the four seam commands.
- Existing tests assert seam-command request names and seam-unavailable behavior when ingest ack payloads are returned.

## Compatibility Matrix
The adapter MUST expose only this first `tapescript_*` tool slice as supported:

| MCP tool id | Backing seam command | Slice 1 status | Required argument mapping |
| --- | --- | --- | --- |
| `tapescript_status` | `status` | supported | no args |
| `tapescript_read_live_tail` | `read_live_tail` | supported | `limit` |
| `tapescript_read_range` | `read_range` | supported | `first_session_seq`, `last_session_seq` |
| `tapescript_find_order_anchor` | `find_order_anchor` | supported | one of `trace_id`, `order_id`, `perm_id`, `exec_id` |

No other `tapescript_*` tool is supported in Slice 1.

## Deferred/Unsupported Tool Behavior
The adapter MUST NOT silently omit unsupported tools.

### Reserved deferred tool ids (explicitly deferred in Slice 1)
- `tapescript_analyzer_run`
- `tapescript_findings_list`
- `tapescript_playbook_apply`
- `tapescript_report_generate`
- `tapescript_export_range`

Behavior for each reserved deferred tool id:
- Tool remains discoverable.
- Invocation returns a contract envelope with `ok=false`.
- `error.code` is `deferred_tool`.
- `error.retryable` is `false`.
- `meta.supported` is `false`.
- `meta.deferred` is `true`.

Behavior for any other unknown `tapescript_*` id:
- Invocation returns a contract envelope with `ok=false`.
- `error.code` is `unsupported_tool`.
- `error.retryable` is `false`.
- `meta.supported` is `false`.
- `meta.deferred` is `false`.

## Revision-Aware Response Envelope
All Slice 1 read-oriented tools MUST return this top-level envelope:

```json
{
  "ok": true,
  "result": {},
  "meta": {
    "contract_version": "phase5-mcp-compat-v1",
    "tool": "tapescript_status",
    "engine_command": "status",
    "supported": true,
    "deferred": false,
    "revision": {
      "manifest_hash": "manifest-42",
      "latest_session_seq": 42,
      "first_session_seq": null,
      "last_session_seq": null,
      "source": "engine_payload",
      "staleness": "live"
    }
  },
  "error": null
}
```

Rules:
- `meta.revision` MUST always be present for read-oriented tools, even when values are unknown.
- Unknown revision fields MUST be `null` (not omitted).
- `source` indicates where revision fields came from: `engine_payload`, `derived_from_events`, or `unavailable`.
- `staleness` values:
  - `live` for `tapescript_status` and `tapescript_read_live_tail`
  - `snapshot` for `tapescript_read_range` and `tapescript_find_order_anchor`
  - `unknown` only when revision data cannot be derived

Per-tool revision shaping:
- `tapescript_status`: read `manifest_hash` and `latest_session_seq` from status payload keys (including existing fallback keys already handled in `tapescope::QueryClient`).
- `tapescript_read_live_tail`: derive `latest_session_seq`, `first_session_seq`, and `last_session_seq` from returned event `session_seq` values when present.
- `tapescript_read_range`: set `first_session_seq`/`last_session_seq` from requested window; if returned events exist, adapter MAY also derive observed min/max and include them in `result`.
- `tapescript_find_order_anchor`: if payload includes event arrays, derive revision bounds from event `session_seq`; otherwise leave revision fields `null` with `source=unavailable`.

## Error Mapping Contract
Adapter error codes MUST preserve current seam semantics:

| Existing seam classification (`tapescope::QueryErrorKind`) | MCP envelope `error.code` |
| --- | --- |
| `Transport` | `engine_unavailable` |
| `Remote` | `engine_query_failed` |
| `MalformedResponse` | `malformed_response` |
| `SeamUnavailable` | `seam_unavailable` |

For these errors:
- `ok=false`
- `result=null`
- `meta.supported=true`
- `meta.deferred=false`

## Contract Assumptions (Frozen For Slice 1)
- Slice 1 intentionally depends only on seam commands already used by `TapeScope.app`.
- No engine-internal data structures are consumed directly by the adapter.
- Revision metadata is adapter-shaped in Slice 1 where the seam payload does not expose full revision state.
- Explicit deferred/unsupported responses are required to avoid ambiguous client behavior.
