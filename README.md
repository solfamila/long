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

Package a distributable zip:

```bash
cmake --build build --target tws_gui_zip
```

Dependency paths:
- ixWebSocket is expected at `./ixwebsocket`
- nlohmann/json is expected at `./nlohmann_json`
- The build auto-detects a downloaded IB API under `~/Downloads/twsapi_macunix/...` when present

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
