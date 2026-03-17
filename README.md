# long

`long` is a macOS-only TWS trading GUI.

Current stack:
- Native `AppKit` window and controls
- Apple `GameController.framework` for controller input on macOS
- Interactive Brokers TWS / Gateway connectivity
- `ixwebsocket` localhost control server
- bridge batch sender for `tape-engine` over local Unix domain sockets

Important architecture note:
- The app now uses native AppKit views instead of `GLFW + Dear ImGui`.
- The trading logic, TWS integration, WebSocket handling, and controller handling remain in C++ / ObjC++ code.

Build requirements:
- macOS
- Xcode Command Line Tools
- Protocol Buffers (`protoc` and headers/libs) when building against the newer IB API

Build:

```bash
cmake -S . -B build
cmake --build build -j4
```

Run:

```bash
open build/tws_gui.app
```

Direct executable inside the bundle:

```bash
./build/tws_gui.app/Contents/MacOS/tws_gui
```

Run the native TapeScope app bundle:

```bash
open build/TapeScope.app
```

Direct executable inside the TapeScope bundle:

```bash
./build/TapeScope.app/Contents/MacOS/TapeScope
```

Run the Phase-1 engine daemon:

```bash
./build/tape_engine
```

Query the engine daemon:

```bash
./build/tape_engine_ctl status
./build/tape_engine_ctl read-live-tail 20
./build/tape_engine_ctl read-range 1 50 --include-live-tail
./build/tape_engine_ctl read-range 1 50 --revision 1
./build/tape_engine_ctl replay-snapshot 50 --depth 5
./build/tape_engine_ctl replay-snapshot 50 --depth 5 --revision 1 --include-live-tail
./build/tape_engine_ctl scan-session-report --revision 1
./build/tape_engine_ctl list-session-reports --limit 20
./build/tape_engine_ctl read-session-report 1
./build/tape_engine_ctl scan-incident-report 1 --revision 1
./build/tape_engine_ctl scan-order-case-report --order-id 701 --revision 1
./build/tape_engine_ctl list-case-reports --limit 20
./build/tape_engine_ctl read-case-report 1
./build/tape_engine_ctl find-order --order-id 701 --revision 1
./build/tape_engine_ctl seek-order --order-id 701 --revision 1
./build/tape_engine_ctl read-order-case --order-id 701 --revision 1
./build/tape_engine_ctl read-order-anchor 1 --revision 1
./build/tape_engine_ctl list-order-anchors --limit 20
./build/tape_engine_ctl list-protected-windows --limit 20
./build/tape_engine_ctl read-protected-window 1 --revision 3
./build/tape_engine_ctl list-findings --limit 20
./build/tape_engine_ctl read-finding 1 --revision 3
./build/tape_engine_ctl list-incidents --limit 20
./build/tape_engine_ctl read-incident 1 --revision 4
```

TapeScope Phase-4 shell notes:

- `TapeScope` is the native AppKit investigation shell for `tape_engine`.
- It uses the same `LONG_TAPE_ENGINE_SOCKET` env var as the bridge sender and defaults to `/tmp/tape-engine.sock`.
- The current Phase-4 slice includes:
  - `StatusPane`
  - `LiveEventsPane`
  - `SessionOverviewPane`
  - `IncidentPane`
  - `ReplayTargetPane`
  - `RangePane`
  - `OrderLookupPane`
  - `OrderCasePane`
  - `ReportInventoryPane`
  - `ArtifactPane`
- `ReplayTargetPane` can now push the computed replay target window directly into `RangePane`.
- `ArtifactPane` now previews `export_artifact` outputs for both Markdown and JSON-bundle formats.
- `IncidentPane` now supports both manual incident-id drilldown and a structured incident table with selectable rows.
- `ReportInventoryPane` now uses structured session/case report tables with selectable rows and direct open-to-artifact actions.
- `RangePane` and `OrderLookupPane` now use structured event tables with selectable rows and decoded-payload detail views, so replay and anchored lifecycle inspection follow the same browsing model as incidents/reports.
- `OrderCasePane` and `ArtifactPane` now expose selectable evidence-citation tables, can jump directly into cited artifacts, and let order-case reads push their replay window straight into `RangePane`.
- `SessionOverviewPane` and `IncidentPane` now follow the same investigation flow: selectable evidence citations, direct jump-to-artifact actions, and replay/range handoff from the loaded summary.
- Actionable TapeScope tables now support double-click plus `Return` / `Space` keyboard activation for the common drilldown actions (`open selected incident`, `open selected evidence`, `open selected report`), which makes the shell usable with much less pointer travel.
- Loading, empty, and error states are now calmer and more explicit across the shell: investigation panes show loading placeholders while requests are in flight, empty result sets explain what to try next, and report/incident inventory panes distinguish between “loaded but empty” and actual query failures.
- TapeScope now also drops keyboard focus into the overview range field on launch, so a fresh session can be driven immediately from the keyboard without clicking into the first pane.
- TapeScope now keeps a recent-history surface for reopened overviews, incidents, findings, anchors, order cases, and artifacts, so common investigations can be revisited without retyping ids or ranges.
- TapeScope now restores its last selected pane, recent-history list, polling mode, and key investigation inputs across launches, so the shell comes back in the same working context instead of resetting to a blank session.
- The header now includes `Refresh Now`, polling pause/resume, and a `Last updated` timestamp so live investigation can be paused deliberately, refreshed on demand, and checked for staleness at a glance.
- `QualityPane` now exposes `read_session_quality`, and `RangePane` can hand its current replay window straight into that data-quality view.
- `FindingPane` and `AnchorPane` now expose the engine’s direct `read_finding` and `read_order_anchor` investigation reads, including replay-window handoff and evidence-citation browsing.
- The app talks directly to the current query seam (`status`, `read_live_tail`, `read_session_quality`, `read_session_overview`, `scan_session_report`, `list_session_reports`, `read_range`, `find_order_anchor`, `seek_order_anchor`, `read_finding`, `read_order_anchor`, `read_order_case`, `scan_order_case_report`, `list_case_reports`, `list_incidents`, `read_incident`, `read_artifact`, `export_artifact`) instead of wrapping the older generic command client from earlier prototype branches.
- Phase 4.5 cleanup hardens the shell architecture: the highest-value investigation reads now have typed client payloads, the main live/range/order/report tables also use typed row models instead of raw JSON blobs, reusable investigation-pane controllers own evidence/replay state instead of leaving it all in the window controller, polling/query/report orchestration is now split across `tapescope_queries.mm`, `tapescope_session_queries.mm`, `tapescope_order_queries.mm`, and `tapescope_artifact_queries.mm`, shared AppKit builders live in `tapescope_support_ui.mm`, formatting/description helpers live in `tapescope_support_describe.mm`, typed payload parsing/packing now lives in `tapescope_client_payloads.cpp` behind `tapescope_client_internal.h`, table/report bindings live in `tapescope_table_bindings.mm` behind `tapescope_window_internal.h`, poll/interact/artifact work use separate dispatch lanes, request tokens prevent stale async responses from overwriting newer user actions, and query operation names are centralized in the protocol registry instead of being scattered across the app/CLI/client boundary.
- Phase 4.6 cleanup keeps that boundary maintainable as the shell grows: the TapeScope seam tests are now split into subsystem fragments instead of one giant source file, the high-value investigation/report/export reads default to typed payloads in both the UI and tests, and the legacy `QueryResult<json>` helpers remain only as compatibility shims instead of being the main path the app exercises.

Package a distributable zip:

```bash
cmake --build build --target tws_gui_zip
```

Dependency paths:
- ixWebSocket is expected at `./ixwebsocket`
- nlohmann/json is expected at `./nlohmann_json`
- The build auto-detects a downloaded IB API under `~/Downloads/twsapi_macunix/...` when present
- On macOS, the build now prefers Homebrew `curl` from `/opt/homebrew/opt/curl` or `/usr/local/opt/curl` when available so the UW websocket lane links against a `ws`/`wss`-capable libcurl instead of the SDK stub

External dependency paths also work. If you keep `ixwebsocket` and `nlohmann_json`
outside this workspace, pass them explicitly:

```bash
cmake -S . -B build \
  -DIXWEBSOCKET_DIR=/absolute/path/to/ixwebsocket \
  -DNLOHMANN_JSON_DIR=/absolute/path/to/nlohmann_json
cmake --build build -j4
```

Phase-0 mock bridge validation:

- If no vendored IB API is available, the workspace falls back to the local mock IB API.
- The bridge and recovery semantics are covered by `tws_gui_tests`.

```bash
cmake -S . -B build-phase0 \
  -DCMAKE_BUILD_TYPE=Debug \
  -DIXWEBSOCKET_DIR=/absolute/path/to/ixwebsocket \
  -DNLOHMANN_JSON_DIR=/absolute/path/to/nlohmann_json
cmake --build build-phase0 --target tws_gui_tests -j4
ctest --test-dir build-phase0 --output-on-failure
```

Phase-1 bridge sender notes:

- `long` now batches queued bridge records into framed MessagePack batches for `tape-engine`.
- The sender uses the Unix domain socket path from `LONG_TAPE_ENGINE_SOCKET` when set.
- If that env var is unset, the sender defaults to `/tmp/tape-engine.sock`.
- `tape_engine` now assigns ordered `session_seq` values on accept and freezes accepted batches into revisioned segments on a dedicated writer thread.
- `tape_engine` now routes ingest and query/replay work through separate worker queues so investigative reads do not share the ingest sequencer path.
- `tape_engine` writes per-batch binary MessagePack segment payloads plus JSON metadata and a hash-linked `manifest.jsonl` under `LONG_TAPE_ENGINE_DATA_DIR`.
- If `LONG_TAPE_ENGINE_DATA_DIR` is unset, the daemon defaults to `/tmp/tape-engine`.
- The daemon now answers `status`, `read_live_tail`, `read_range`, `replay_snapshot`, and `find_order_anchor` queries over the same framed MessagePack UDS transport.
- `seek_order_anchor` now returns replay targets and protected-window context for order/fill investigations, so a client can jump straight to the right `session_seq` and replay window around a fill or order-state transition.
- `read_order_case` now returns a report-style order/fill investigation summary with replay targets, protected-window context, related findings, and ranked incidents for the selected anchor.
- `read_order_case` and `read_incident` now include merged investigation timelines and timeline highlights, so clients can render case/incident narratives instead of stitching raw events together themselves.
- `read_session_overview` now returns a ranked session-level investigation summary with top incidents, top findings, protected-window coverage, timeline highlights, and data-quality scoring for any requested `session_seq` range.
- `scan_session_report` now turns that same session-level investigation summary into a canonical durable report artifact pinned to a frozen revision and `session_seq` range, and `read_session_report` / `list_session_reports` let clients reopen those persisted summaries later.
- `scan_incident_report` and `scan_order_case_report` now persist revision-pinned durable case reports for incident drilldowns and order/fill investigations, and `read_case_report` / `list_case_reports` reopen those artifacts later without rebuilding the narrative from live query logic.
- `read_artifact` and `export_artifact` now give the engine a stable artifact-facing surface for future MCP/tool clients. The resolver now supports durable report IDs plus selector-style artifacts like `session-overview:<revision>:<from>:<to>`, `order-case:order:<id>`, `finding:<id>`, and `anchor:<id>`.
- Investigation/report responses now share a versioned envelope with `api`, `artifact`, `entity`, `report`, and `evidence` sections, so session, incident, protected-window, finding, and order-case outputs line up structurally instead of only sharing ad hoc summary fields.
- `export_artifact` supports Markdown summaries and JSON bundles, which makes durable report artifacts easier to hand to future native UI and MCP clients without rebuilding them from live query code.
- Phase 5 adds a native `tape_mcp` stdio server on top of the engine query socket. The current tool slice now mirrors the stable engine investigation seam: `status`, `read_live_tail`, `read_range`, `replay_snapshot`, `find_order_anchor`, `list_incidents`, `list_order_anchors`, `list_protected_windows`, `list_findings`, `read_session_overview`, `scan_session_report`, `read_session_report`, `list_session_reports`, `scan_incident_report`, `scan_order_case_report`, `read_case_report`, `list_case_reports`, `seek_order_anchor`, `read_finding`, `read_order_case`, `read_order_anchor`, `read_protected_window`, `read_incident`, `read_artifact`, `export_artifact`, and `read_session_quality`, all backed by typed request/result helpers instead of raw UI-side JSON unpacking.
- The UW/Gemini enrichment path now also carries over the standalone spike's live WebSocket lane as an optional secondary provider. `refresh_external_context` and other live-tail-biased enrichments can blend short-lived UW `wss` captures (`option_trades`, `flow-alerts`, `gex`, `price`, and `news`) alongside the existing mediated MCP/REST path, while `LONG_ENABLE_UW_WEBSOCKET_CONTEXT=1` forces that lane on for other enrichments, `LONG_UW_WS_SAMPLE_MS` caps the live capture budget, and `LONG_UW_WS_FIXTURE_FILE` lets tests or offline runs inject saved websocket frames deterministically.
- Explicit `refresh_external_context` calls now get a longer websocket capture budget by default than background enrichments, so the live lane is more likely to return real data without having to export a custom sample window first. `LONG_UW_WS_SAMPLE_MS` still overrides that policy when you want tighter or longer captures.
- The smoke examples below use a generic `build` directory. If you configured a different build root locally, substitute that path in the commands.
- `uw_ws_smoke` is now a first-class repo tool for validating the same websocket connector `long` uses in production. From the repo root:
  - `cmake --build build --target uw_ws_smoke -j8`
  - `./build/uw_ws_smoke --symbol SPY --facets option_trades,flow-alerts,price,news,gex --sample-ms 15000 --max-frames 16`
  - The smoke tool auto-loads `.env.local`, forces the websocket lane on for the process, and returns both raw connector metadata and a compact `triage_summary` with per-channel outcomes.
  - `--second-pass-sample-ms`, `--second-pass-total-ms`, and `--second-pass-limit` let you tune the adaptive retry path for a single run without editing env vars.
  - It distinguishes `ok`, `join_ack_only`, `ambient_global_only`, `already_in_room_only`, `error_frames_only`, `unparsed_frames_only`, and transport/runtime failures. In practice, `news` is often the first live data channel to yield records, so 15s+ windows are a better smoke default than the very short production live-tail budget.
- `uw_mcp_smoke` is the companion repo tool for the remote UW MCP server. It exercises the real `initialize` / `tools/list` / `tools/call` flow against `https://api.unusualwhales.com/api/mcp`, using the same bearer-token lookup as the in-app connector:
  - `cmake --build build --target uw_mcp_smoke -j8`
  - `./build/uw_mcp_smoke --symbol SPY --facets options_flow,news,gex`
  - `./build/uw_mcp_smoke --list-tools`
  - `./build/uw_mcp_smoke --tool get_option_trades --symbol SPY`
  - `./build/uw_mcp_smoke --tool get_market_events --arg start_date=2026-03-16 --arg end_date=2026-03-20`
  - The tool prints raw remote MCP handshake/results plus the mediated `UWMcpConnector` step, so it is easy to compare “what UW MCP returned” with “what `long` accepted into enrichment.” It now also prints a shared `facet_resolution` block, so you can see which requested facets the current UW MCP catalog really supports directly and which facets (`news` and `gex` at the moment) still need REST/websocket backfill.
  - `long` now treats remote UW MCP coverage honestly: direct MCP is preferred for `options_flow`, `alerts`, and `stock_state`, while any uncovered requested facets are backfilled through the existing REST/websocket lanes instead of being silently dropped after a partial MCP success.
- `uw_context_smoke` is the merged-provider smoke for the real enrichment path. It drives `uw_context_service` with synthetic local evidence and shows the compact provider merge triage plus the full typed enrichment result:
  - `cmake --build build --target uw_context_smoke -j8`
  - `./build/uw_context_smoke --symbol SPY --sample-ms 8000 --max-frames 12 --second-pass-sample-ms 1200 --second-pass-total-ms 3600 --second-pass-limit 3`
  - The `triage_summary` shows which facets MCP covered directly, which facets REST backfilled, whether websocket produced live data or only join acks, the final normalized item mix by provider/kind, and whether the merged result still degraded.
- Enrichment results now also expose a stable `live_capture_summary` alongside raw provider metadata, so MCP clients and TapeScope can tell whether the UW websocket lane produced live data, only join acks, duplicate-join errors, or no frames at all without decoding `uw_ws` provider-step internals.
- Global UW websocket channels are now symbol-filtered before normalization, so widened live subscriptions like `news` and `flow-alerts` can be used for refreshes without smuggling unrelated symbols into a symbol-scoped enrichment. The websocket diagnostics also record how many frames were dropped as symbol mismatches.
- `live_capture_summary` now also includes per-channel outcomes, so callers can tell whether `price`, `option_trades`, `news`, `flow-alerts`, or `gex` actually produced live data versus only join acks or filtered mismatches during the capture window.
- Global websocket frames without symbol binding are now classified separately as `ambient_global_only`. They stay out of symbol-scoped external context items, but the live summary records that the feed was active and that the activity came from untargeted global channels rather than a dead socket.
- The MCP surface is now golden-tested with both direct adapter projections and a real stdio JSON-RPC harness, so the native MCP layer has a locked contract for live tail, replay, selector lists, report artifacts, and investigation drilldowns before any LLM-facing workflow builds on top of it.
- The MCP adapter now also exposes prompt ergonomics on top of that tool surface: `prompts/list` advertises investigation workflows, `prompts/get` can scaffold session-range, order-case, incident, replay, and artifact-export investigations, and `tools/list` now carries human-readable titles, read-only/idempotent annotations, and example argument payloads for the highest-value reads.
- The MCP adapter now also exposes browseable durable report resources: `resources/list` enumerates persisted session/case reports plus their export variants, and `resources/read` reopens those resources as JSON views, Markdown exports, or JSON bundles without going through the tool layer.
- Prompt coverage is now broader for agent workflows: in addition to the original session/order/incident/export prompts, `prompts/get` now supports `investigate_bad_fill`, `investigate_source_gap`, and `summarize_latest_session_incidents`.
- Heavy scan/export/replay tools now honor MCP progress tokens. When a client supplies `_meta.progressToken` on eligible `tools/call` requests, `tape_mcp` emits `notifications/progress` before and after the long-running work.
- The MCP surface is now locked by dedicated golden contract tests in `tape_mcp_contract_tests`, including both direct adapter projections and a real stdio JSON-RPC harness against a live `tape_engine` fixture.
- The Phase 5.5 hardening pass makes the stdio server less fragile under heavier agent usage: request handling now gates on a prior `initialize`, `notifications/initialized` is recognized explicitly, and request execution runs through bounded read/export worker pools instead of one fully synchronous request loop.
- MCP payload parsing is also no longer owned by the TapeScope client implementation. The shared typed result layer now lives in `tape_query_payloads.*`, so `tape_mcp` and `TapeScope` share payload shaping without the MCP target linking against the native UI client sources.
- Progress support is richer now for heavy MCP calls: progress-capable tools emit queued, dispatching, running, finalizing, and finished stages instead of only start/finish notifications.
- Phase 6 adds portable report-bundle workflows on top of that artifact surface. `export_session_bundle` and `export_case_bundle` now freeze revision-pinned MessagePack bundles under the engine data dir with pinned revision metadata, structured report payloads, Markdown export text, evidence citations, and data-quality context; `import_case_bundle` and `list_imported_cases` let the engine inventory those portable case bundles later and reopen them through `read_artifact` as `imported-case:<id>`. The bundle export/import path now publishes files and imported-case manifests through temp-file-plus-rename writes, startup will rebuild a stale imported-case lookup index from persisted manifests instead of trusting drifted metadata, and the transport now carries explicit typed `result` payloads across the core query families too: status, event-list reads, investigation/report reads, collection/list reads, seek-order replies, artifact exports, replay snapshots, and the Phase 6 bundle workflows no longer depend only on the generic `summary/events` envelope. The trace suite now also freezes normalized engine-side transport fixtures for those typed `result` bodies, so the raw query seam is exercised directly rather than only through TapeScope and MCP payload projections.
- TapeScope now exposes that same Phase 6 workflow natively in the `ReportInventoryPane`: you can export the selected session or case report as a portable bundle, choose and import a case bundle from disk, browse imported bundles, preview a bundle before import, load its replay range, jump to its source artifact, and reopen matching imported cases directly in `ArtifactPane`. Preview now prefers the engine-backed `verify_bundle` path and falls back to local inspection if the engine is unavailable, so the UI can surface importability, duplicate-import status, pinned revision metadata, the imported-case match reason, and a clearer “why import is blocked” diagnostic for session, duplicate, or corrupted bundles before import. It also keeps a dedicated `BundleHistoryPane`, so bundle exports/imports remain reopenable inside the app with reveal-path, source-artifact, and replay-range actions.
- The MCP layer mirrors that Phase 6 bundle workflow with `tapescript_export_session_bundle`, `tapescript_export_case_bundle`, `tapescript_verify_bundle`, `tapescript_import_case_bundle`, and `tapescript_list_imported_cases`, so agent clients can validate, move, and reopen durable case/session packages without reaching into engine internals.
- Phase 7 now starts from those persisted Phase 6 bundles instead of adding a new engine seam. `tapescript_list_analysis_profiles` and `tapescript_read_analysis_profile` expose the supported local analyzer profiles up front, so agents do not need to guess which `analysis_profile` strings are valid, and unsupported profiles fail fast with a stable `unsupported_profile` error. The local analyzer now has six durable profile families: `phase7.trace_fill_integrity.v1` for evidence/identity/trace gaps, `phase7.incident_triage.v1` for top-incident uncertainty and timeline-hotspot review, `phase7.fill_quality_review.v1` for broad fill-quality/adverse-selection/market-impact review, `phase7.liquidity_behavior_review.v1` for display-instability, refill behavior, and liquidity-thinning review, `phase7.adverse_selection_review.v1` for post-fill invalidation/adverse-move review, and `phase7.order_impact_review.v1` for market-impact/cancel-chain footprint review. `tapescript_analyzer_run` runs one of those local artifact-backed analyzer passes against a portable session/case bundle and persists a deterministic `phase7.analysis_output.v1` manifest under the app data dir, `tapescript_findings_list` reopens the stored findings by manifest path or artifact id, and `tapescript_playbook_apply` builds a guarded `dry_run` playbook plan artifact with deterministic action ids while `mode="apply"` stays explicitly deferred. Those dry-run playbooks are profile-specific now: integrity plans stay focused on evidence/identity review, triage plans focus on incident priority/uncertainty review, fill-quality plans focus on fill/adverse-selection checks, liquidity-behavior plans focus on instability/thinning review, adverse-selection plans split post-fill sequence review from fill-context gating, and order-impact plans focus on impact footprint plus pre-impact context instead of collapsing everything into the same generic action family. Phase 7 now also turns guarded playbooks into durable review-only execution ledgers with `tapescript_prepare_execution_ledger`, `tapescript_list_execution_ledgers`, `tapescript_read_execution_ledger`, and `tapescript_record_execution_ledger_review`, so agents and TapeScope can reopen the pending-review action set, linked playbook/analysis/source artifacts, replay context, review summaries, and append-only audit trail without pretending live execution exists yet. Once a ledger is ready, `tapescript_start_execution_journal`, `tapescript_list_execution_journals`, `tapescript_read_execution_journal`, `tapescript_dispatch_execution_journal`, and `tapescript_record_execution_journal_event` advance that same durable lineage into a controlled execution journal: queued entries can be dispatched into `submitted` with capability gating, then completed/cancelled/failed through append-only execution events while the audit trail and idempotency keys stay intact. Order-anchored journal entries now also carry an explicit runtime reconciliation request, and the local execution bridge can dispatch them through `request_order_reconciliation`, reconcile them back from live order-state snapshots, keep `Working`, `PartiallyFilled`, and `AwaitingCancelAck` orders in explicit recoverable submitted state instead of collapsing them into false terminal success, preserve the best available broker/runtime linkage (`order_id`, `trace_id`, `perm_id`, and any known `exec_ids`) inside the persisted execution result, persist explicit fill-state, restart-resume-policy, restart-recovery-state, and restart-recovery-reason hints for cancel-pending, cancelled, rejected, inactive, manual-review, and stale-timeout paths, carry the latest broker detail into both journal and apply views, expose journal/apply inventory triage directly through `restart_recovery_state`, `restart_resume_policy`, and `latest_execution_resolution` filters, sweep runtime-backed journals that are waiting on broker resolution, escalate stale missing/reconciling entries into explicit manual-review failures after the recovery grace period, summarize persisted recovery backlog across journals and applies, compute an explicit startup recovery policy (`none`, `recover_pending`, or `await_actor`) for those in-flight artifacts, and mirror the resolved state into controlled-apply artifacts without bypassing the ledger/journal audit chain. Submitted journal entries can then be promoted into durable controlled-apply artifacts with `tapescript_start_execution_apply`, `tapescript_list_execution_applies`, `tapescript_read_execution_apply`, and `tapescript_record_execution_apply_event`, so the final execution lifecycle is still journal-backed, append-only, and capability-gated instead of bypassing the review chain. Ledger review semantics are richer now too: actionable entries move through `pending_review`, `approved`, `blocked`, `needs_info`, and `not_applicable`, aggregate ledger status distinguishes `review_pending`, `review_in_progress`, `review_waiting_approval`, and `ready_for_execution`, reviewers are required to identify themselves, `blocked`/`needs_info` updates require comments, and `approved` entries only become execution-ready after two distinct approvers. Those Phase 7 analyzer/playbook/ledger/journal/apply writes are idempotent now too: rerunning the same analyzer profile, guarded playbook selection, ledger preparation, journal start, or controlled-apply start reuses the existing manifest instead of rewriting it, and the MCP results expose both `artifact_status` (`created` vs `reused`) and `generated_at_utc` so agents can tell whether they just minted a new durable artifact or reopened an existing one. The MCP surface also exposes direct durable artifact inventory and reopen flows with `tapescript_list_analysis_artifacts`, `tapescript_read_analysis_artifact`, `tapescript_list_playbook_artifacts`, `tapescript_read_playbook_artifact`, `tapescript_list_execution_ledgers`, `tapescript_read_execution_ledger`, `tapescript_list_execution_journals`, `tapescript_read_execution_journal`, `tapescript_list_execution_applies`, and `tapescript_read_execution_apply`, and the inventory calls now support lineage filters, explicit runtime-recovery filters for journal/apply inventories, execution-triage filters (`restart_recovery_state`, `restart_resume_policy`, and `latest_execution_resolution`), and attention-oriented sorting so agents can resolve the right stored Phase 7 artifacts without scraping `resources/list`. TapeScope now has a matching native `Phase7Pane` for running a local analyzer from a chosen Phase 6 bundle, filtering and sorting durable analyses/playbooks/ledgers/journals by lineage, mode, status, attention rank, recovery pressure, and runtime triage state, reviewing findings/actions in structured tables, building a guarded dry-run playbook from either all findings or a selected finding subset, preparing a review-only execution ledger from that playbook, recording review decisions with actor/comment policy enforcement, starting and reopening execution journals, dispatching queued entries, starting and reopening controlled applies from submitted journal entries, recording both journal and apply lifecycle events, reopening those durable artifacts later, inspecting their linked source artifacts, and loading their replay windows without surfacing a fully automatic live-apply path before the execution/audit model is complete. TapeScope can now also opt into an isolated local runtime bridge for that same Phase 7 surface: it starts a dedicated `TradingRuntime` with a TapeScope-specific client id offset and controller/websocket disabled, summarizes any persisted runtime recovery backlog on startup, computes the startup recovery policy from that backlog plus the configured execution actor, automatically runs a startup recovery sweep only when recovery is pending and an actor is already present, and otherwise surfaces the pending/stale backlog in the native runtime status until the operator explicitly runs `Recover Pending`. The live runtime can then dispatch queued journal entries, reconcile selected submitted entries against live order state, sweep runtime-backed artifacts in one pass, and synchronize controlled-apply artifacts from the linked journal without leaving the native review workflow. The Phase 7 MCP slice is golden-tested end-to-end through the adapter and real stdio harness, and the execution-bridge lifecycle now also has its own focused engine-side golden fixture for the cancel-pending, cancel-resolved, reject, inactive, and stale-timeout restart semantics, so analyzer/playbook/ledger/journal/apply contracts are frozen before any broader agent workflow grows on top of them.
- `phase7_execution_smoke` is now a small operator-facing CLI for the execution side. It reports the persisted runtime recovery backlog, startup recovery policy, and current runtime/order/outbox snapshot as machine-readable JSON, and it can optionally start the same isolated runtime bridge TapeScope uses with `--start-runtime` and run one recovery sweep with `--recover --actor NAME` when you want a quick sanity pass outside the app.

Minimal MCP client wiring example:

```json
{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}
{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}
{"jsonrpc":"2.0","id":3,"method":"prompts/list","params":{}}
{"jsonrpc":"2.0","id":4,"method":"resources/list","params":{}}
{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"tapescript_scan_session_report","arguments":{"first_session_seq":1,"last_session_seq":200},"_meta":{"progressToken":"scan-1"}}}
```

Run the MCP server over stdio:

```bash
./build/tape_mcp --engine-socket /tmp/tape-engine.sock
```
- `TapeScope` Phase 4 work is now more navigable: `LiveEventsPane`, `RangePane`, `OrderLookupPane`, `IncidentPane`, and `ReportInventoryPane` all use structured row selection instead of only text dumps, and `SessionOverviewPane` now exposes top incidents in a table that can jump directly into incident drilldown.
- Scan operations now promote the persisted durable report artifact to the primary `artifact` envelope and preserve the live source object under `source_artifact`, which keeps later `read_*_report` and `read_artifact` semantics aligned.
- `read_finding` and `read_order_anchor` now provide direct investigation reads for those artifact IDs instead of forcing clients to back into them through list endpoints or order-case drilldowns.
- `read_session_quality` now summarizes evidence trust for the whole frozen session or any requested `session_seq` range, and case/incident/protected-window reads now surface a `data_quality` block alongside their narrative output.
- Frozen range/replay reads now snapshot engine state up front and use segment `session_seq` bounds to avoid holding the main engine lock across disk I/O and broad rescans.
- Query responses now expose frozen-revision state such as `latest_frozen_revision_id`, `served_revision_id`, and optional mutable-tail overlay via `--include-live-tail`.
- `long` now emits normalized public market records (`market_tick`, `market_depth`) alongside widened private lifecycle records including `open_order`, `order_status`, `commission_report`, `cancel_request`, `broker_error`, and `order_reject`.
- Bridge records now carry canonical `instrument_id`, receive/exchange timestamps, and vendor sequence placeholders so the engine can preserve stronger forensic provenance.
- `replay_snapshot` rebuilds a deterministic bid/ask/last and depth snapshot from frozen `session_seq` history, with optional live-tail overlay.
- Phase 3 now includes hot-path spread-widening, source-quality, trade-pressure, display-instability, pull-follow-through, quote-flicker cadence, trade-after-depletion, depletion-after-trade, post-fill invalidation, genuine-refill vs absorption discrimination, and inside-liquidity findings, plus deferred order-window market-impact, passive-fill queue-position proxies, post-fill adverse-selection analysis, and richer order/fill-context analysis. Repeated findings collapse into ranked logical incidents, so `list-incidents` surfaces the latest scored incident snapshot instead of one row per finding, and `read-incident` returns a report-style drilldown with score breakdown, related findings, protected-window context, uncertainty, and why-it-matters explanations.
- Frozen revisions now persist Phase 3 artifact sidecars (`.artifacts.msgpack`) next to segment payloads, so anchors, protected windows, findings, and incidents survive daemon restart and remain revision-pinned evidence.
- Protected windows now materialize `first_session_seq` / `last_session_seq` bounds in addition to timestamp bounds, so replay, export, and protected-window evidence reads can stay deterministic and segment-bounded.
- Analyzer execution is now split into hot-path analyzers and an explicit deferred analyzer lane owned by the engine runtime. The deferred lane emits both order-flow timeline and order/fill-context findings from protected order/fill windows on its own background queue instead of keeping all analysis inline on the sequencer path.
- Query and deferred-analysis artifact reads now build indexed views from the captured engine snapshot instead of reloading frozen artifact sidecars from disk on every request.
- Query lookups now use selector indexes for anchors/findings/incidents plus a frozen segment event cache, which keeps incident/window/order-case reads cheaper as sessions grow.
- Frozen segments now also persist `.index.msgpack` selector indexes and `.checkpoint.msgpack` replay checkpoints, so anchor reads can skip non-matching segments and `replay_snapshot` can resume from the latest frozen checkpoint instead of rebuilding the whole session from scratch.
- Data-quality scoring is now a first-class Phase 3 output. Query surfaces report gaps, resets, weak instrument identity, timestamp coverage, vendor-sequence coverage, and mutable-tail caveats so investigations can say how strong the evidence is, not just what happened.
- Canonical instrument identity now prefers strong bridged or configured `ib:conid:...` identities, and any last-resort fallback is surfaced explicitly as `ib:heuristic:...` with both resolved and source identity fields in query output. Mismatched symbol-vs-identity events are now coerced away from the bad source ID by default, surfaced with `instrument_identity_policy`, and can be rejected at ingest entirely with `EngineConfig::rejectMismatchedStrongInstrumentIds`.
- Incident ranking now applies clearer score factors for severity, overlap, kind, range, evidence breadth, corroboration, and uncertainty penalties driven by incident-local data quality.

Runtime registry and QoS:

- Queue labels, categories, and QoS names are generated from `config/runtime/queues.yaml` into `runtime_registry.generated.h` at build time.
- The generated registry now covers `long`, `tape_engine`, `TapeScope`, and `tape-mcp` queue identities from one checked-in source of truth.
- `runtime_qos` applies the generated queue spec to bridge and engine threads on macOS, including pthread QoS classes and thread names.
- `tape_engine` now waits on POSIX signals for shutdown instead of polling in a timed sleep loop.

Packaging notes:
- The app is built as a real macOS `.app` bundle.
- Non-system protobuf/abseil dylibs are embedded into `Contents/Frameworks`.
- The bundle is ad-hoc signed locally after dependency fixup.
- Notarization is not automated yet.
