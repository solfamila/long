void testTapeEngineRevisionPinnedReadsCanOverlayMutableTail() {
    const fs::path rootDir = testDataDir() / "tape-engine-revision-overlay";
    const fs::path socketPath = testDataDir() / "tape-engine-revision-overlay.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "ib:conid:6401:STK:SMART:USD:INTC";
    config.ringCapacity = 32;

    tape_engine::Server server(config);
    std::string startError;
    expect(server.start(&startError), "tape-engine should start for revision overlay test: " + startError);

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-engine-revision-overlay";
    options.runtimeSessionId = "runtime-engine-revision-overlay";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;

    options.batchSeq = 71;
    BridgeOutboxRecord firstRecord = makeBridgeRecord(5001, "order_intent", "WebSocket", "INTC", "BUY",
                                                      101, 801, 0, "", "first frozen event", "2026-03-14T09:38:30.100");
    firstRecord.instrumentId = "ib:conid:6401:STK:SMART:USD:INTC";
    const bridge_batch::Batch first = bridge_batch::buildBatch({firstRecord}, options);
    expect(transport.sendFrame(bridge_batch::encodeFrame(first), &error),
           "tape-engine should accept the first revision batch: " + error);

    waitUntil([&]() {
        const auto snapshot = server.snapshot();
        return snapshot.latestFrozenRevisionId >= 1 && snapshot.segments.size() >= 1;
    }, "tape-engine should freeze revision 1 before pinned reads");

    options.batchSeq = 72;
    BridgeOutboxRecord secondRecord = makeBridgeRecord(5002, "order_status", "BrokerOrderStatus", "INTC", "BUY",
                                                       101, 801, 0, "", "mutable tail event", "2026-03-14T09:38:30.200");
    secondRecord.instrumentId = "ib:conid:6401:STK:SMART:USD:INTC";
    const bridge_batch::Batch second = bridge_batch::buildBatch({secondRecord}, options);
    expect(transport.sendFrame(bridge_batch::encodeFrame(second), &error),
           "tape-engine should accept the second revision batch: " + error);

    tape_engine::Client client(socketPath.string());
    tape_engine::QueryResponse response;

    tape_engine::QueryRequest frozenOnly;
    frozenOnly.requestId = "revision-frozen-only";
    frozenOnly.operation = "read_range";
    frozenOnly.revisionId = 1;
    frozenOnly.fromSessionSeq = 1;
    frozenOnly.toSessionSeq = 10;
    expect(client.query(frozenOnly, &response, &error), "pinned frozen read should succeed: " + error);
    expect(response.summary.value("served_revision_id", 0ULL) == 1, "pinned frozen read should report revision 1");
    expect(!response.summary.value("includes_mutable_tail", true), "pinned frozen read should exclude the mutable tail by default");
    expect(response.events.is_array() && response.events.size() == 1, "pinned frozen read should only return the frozen revision events");
    expect(response.events.at(0).value("event_kind", std::string()) == "order_intent", "pinned frozen read should preserve revision-1 events");

    tape_engine::QueryRequest overlay = frozenOnly;
    overlay.requestId = "revision-overlay";
    overlay.includeLiveTail = true;
    expect(client.query(overlay, &response, &error), "pinned overlay read should succeed: " + error);
    expect(response.summary.value("served_revision_id", 0ULL) == 1, "overlay read should stay pinned to revision 1");
    expect(response.summary.value("includes_mutable_tail", false), "overlay read should explicitly report mutable-tail inclusion");
    expect(response.events.is_array() && response.events.size() == 2, "overlay read should merge the live tail on top of the pinned revision");
    expect(response.events.at(1).value("event_kind", std::string()) == "order_status", "overlay read should include the later mutable-tail event");

    server.stop();
}

void testTapeEnginePhase3FindingsIncidentsAndProtectedWindows() {
    const fs::path rootDir = testDataDir() / "tape-engine-phase3";
    const fs::path socketPath = testDataDir() / "tape-engine-phase3.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "ib:conid:8101:STK:SMART:USD:INTC";
    config.ringCapacity = 32;

    tape_engine::Server server(config);
    std::string startError;
    expect(server.start(&startError), "tape-engine should start for phase 3 findings test: " + startError);

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-engine-phase3";
    options.runtimeSessionId = "runtime-engine-phase3";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;

    BridgeOutboxRecord orderIntent = makeBridgeRecord(7001, "order_intent", "WebSocket", "INTC", "BUY",
                                                      211, 1101, 0, "", "phase 3 anchor", "2026-03-14T09:40:00.100");
    orderIntent.instrumentId = "ib:conid:8101:STK:SMART:USD:INTC";
    options.batchSeq = 91;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({orderIntent}, options)), &error),
           "tape-engine should accept the anchor batch for phase 3: " + error);

    BridgeOutboxRecord bidTick = makeBridgeRecord(7002, "market_tick", "BrokerMarketData", "INTC", "BID",
                                                  0, 0, 0, "", "bid tick", "2026-03-14T09:40:00.120");
    bidTick.instrumentId = "ib:conid:8101:STK:SMART:USD:INTC";
    bidTick.marketField = 1;
    bidTick.price = 45.10;

    BridgeOutboxRecord askTick = makeBridgeRecord(7003, "market_tick", "BrokerMarketData", "INTC", "ASK",
                                                  0, 0, 0, "", "ask tick", "2026-03-14T09:40:00.130");
    askTick.instrumentId = "ib:conid:8101:STK:SMART:USD:INTC";
    askTick.marketField = 2;
    askTick.price = 45.11;

    options.batchSeq = 92;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({bidTick, askTick}, options)), &error),
           "tape-engine should accept the initial market ticks for phase 3: " + error);

    BridgeOutboxRecord widenedAsk = makeBridgeRecord(7004, "market_tick", "BrokerMarketData", "INTC", "ASK",
                                                     0, 0, 0, "", "ask widened", "2026-03-14T09:40:00.150");
    widenedAsk.instrumentId = "ib:conid:8101:STK:SMART:USD:INTC";
    widenedAsk.marketField = 2;
    widenedAsk.price = 45.13;

    options.batchSeq = 93;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({widenedAsk}, options)), &error),
           "tape-engine should accept the widening market tick for phase 3: " + error);

    waitUntil([&]() {
        const auto snapshot = server.snapshot();
        return snapshot.latestFrozenRevisionId >= 4 && snapshot.segments.size() >= 4;
    }, "tape-engine should freeze all phase 3 test batches");

    tape_engine::Client client(socketPath.string());
    tape_engine::QueryResponse response;

    tape_engine::QueryRequest anchorsRequest;
    anchorsRequest.requestId = "anchors-phase3";
    anchorsRequest.operation = "list_order_anchors";
    expect(client.query(anchorsRequest, &response, &error), "phase 3 anchor query should succeed: " + error);
    expect(response.events.is_array() && response.events.size() == 1, "phase 3 should record the order anchor");
    expect(response.events.at(0).value("event_kind", std::string()) == "order_intent", "phase 3 anchor list should preserve the order intent event kind");
    expect(response.events.at(0).value("instrument_id", std::string()) == "ib:conid:8101:STK:SMART:USD:INTC", "phase 3 anchor list should preserve canonical instrument identity");

    tape_engine::QueryRequest windowsRequest;
    windowsRequest.requestId = "windows-phase3";
    windowsRequest.operation = "list_protected_windows";
    expect(client.query(windowsRequest, &response, &error), "phase 3 protected window query should succeed: " + error);
    expect(response.events.is_array() && response.events.size() >= 2, "phase 3 should create protected windows for anchors and incident promotion");
    const std::vector<std::string> windowReasons = {
        response.events.at(0).value("reason", std::string()),
        response.events.at(1).value("reason", std::string())
    };
    expect(std::find(windowReasons.begin(), windowReasons.end(), "order_intent") != windowReasons.end() ||
           std::find(windowReasons.begin(), windowReasons.end(), "incident_promotion") != windowReasons.end(),
           "phase 3 protected windows should include anchor or incident reasons");
    expect(response.events.at(0).value("first_session_seq", 0ULL) > 0ULL,
           "phase 3 protected windows should materialize a first session_seq bound");
    expect(response.events.at(0).value("last_session_seq", 0ULL) >= response.events.at(0).value("first_session_seq", 0ULL),
           "phase 3 protected windows should materialize a last session_seq bound");

    tape_engine::QueryRequest findingsRequest;
    findingsRequest.requestId = "findings-phase3";
    findingsRequest.operation = "list_findings";
    expect(client.query(findingsRequest, &response, &error), "phase 3 findings query should succeed: " + error);
    expect(response.events.is_array() && !response.events.empty(), "phase 3 should emit at least one finding");
    bool sawSpreadFinding = false;
    bool sawOrderFlowFinding = false;
    bool sawOrderFillContextFinding = false;
    for (const auto& item : response.events) {
        sawSpreadFinding = sawSpreadFinding || item.value("kind", std::string()) == "spread_widened";
        sawOrderFlowFinding = sawOrderFlowFinding || item.value("kind", std::string()) == "order_flow_timeline";
        sawOrderFillContextFinding = sawOrderFillContextFinding || item.value("kind", std::string()) == "order_fill_context";
    }
    expect(sawSpreadFinding, "phase 3 should emit a spread_widened finding");
    expect(sawOrderFlowFinding, "phase 3 deferred lane should emit an order_flow_timeline finding");
    expect(sawOrderFillContextFinding, "phase 3 deferred lane should emit an order_fill_context finding");

    tape_engine::QueryRequest incidentsRequest;
    incidentsRequest.requestId = "incidents-phase3";
    incidentsRequest.operation = "list_incidents";
    expect(client.query(incidentsRequest, &response, &error), "phase 3 incidents query should succeed: " + error);
    expect(response.events.is_array() && !response.events.empty(), "phase 3 should emit at least one incident");
    bool sawSpreadIncident = false;
    bool sawOrderOverlap = false;
    for (const auto& item : response.events) {
        sawSpreadIncident = sawSpreadIncident || item.value("kind", std::string()) == "spread_widened";
        sawOrderOverlap = sawOrderOverlap || item.value("overlaps_order", false);
    }
    expect(sawSpreadIncident, "phase 3 incident list should include the spread_widened incident");
    expect(sawOrderOverlap, "phase 3 incident list should report at least one order-overlapping incident");

    tape_engine::QueryRequest overviewRequest;
    overviewRequest.requestId = "overview-phase3";
    overviewRequest.operation = "read_session_overview";
    overviewRequest.fromSessionSeq = 1;
    overviewRequest.toSessionSeq = 4;
    overviewRequest.limit = 3;
    expect(client.query(overviewRequest, &response, &error), "phase 3 session overview query should succeed: " + error);
    expect(response.events.is_array() && !response.events.empty(), "phase 3 session overview should return ranked incidents as events");
    expect(response.summary.value("incident_count", 0ULL) >= 1ULL, "phase 3 session overview should count ranked incidents");
    expect(response.summary.value("finding_count", 0ULL) >= 3ULL, "phase 3 session overview should include the phase 3 findings");
    expect(response.summary.value("protected_window_count", 0ULL) >= 2ULL, "phase 3 session overview should count protected windows");
    expect(response.summary.contains("incident_kind_counts"), "phase 3 session overview should summarize incident kinds");
    expect(response.summary.contains("finding_kind_counts"), "phase 3 session overview should summarize finding kinds");
    expect(response.summary.contains("protected_window_reason_counts"), "phase 3 session overview should summarize protected-window reasons");
    expect(response.summary.contains("top_findings"), "phase 3 session overview should include top findings");
    expect(response.summary.contains("top_protected_windows"), "phase 3 session overview should include top protected windows");
    expect(response.summary.contains("top_order_anchors"), "phase 3 session overview should include top order anchors");
    expect(response.summary.contains("timeline"), "phase 3 session overview should include a merged investigation timeline");
    expect(response.summary.contains("timeline_summary"), "phase 3 session overview should summarize the investigation timeline");
    expect(response.summary.contains("report_summary"), "phase 3 session overview should include report-level summary text");
    expect(response.summary.contains("data_quality"), "phase 3 session overview should surface data-quality scoring");
    expect(response.summary.value("top_findings", json::array()).size() >= 1ULL, "phase 3 session overview should return at least one top finding");
    expect(response.summary.value("top_protected_windows", json::array()).size() >= 1ULL, "phase 3 session overview should return at least one top protected window");
    expect(response.summary.value("top_order_anchors", json::array()).size() == 1ULL, "phase 3 session overview should return the anchored order context");
    expect(response.summary.value("timeline_summary", json::object()).value("incident_count", 0ULL) >= 1ULL,
           "phase 3 session overview timeline should include at least one incident entry");
    expect(response.summary.value("incident_kind_counts", json::object()).value("spread_widened", 0ULL) >= 1ULL,
           "phase 3 session overview should count the spread widening incident");

    server.stop();
}

void testTapeEnginePhase3ArtifactsPersistAcrossRestartAndReadProtectedWindow() {
    const fs::path rootDir = testDataDir() / "tape-engine-phase3-restart";
    const fs::path socketPath = testDataDir() / "tape-engine-phase3-restart.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "ib:conid:9101:STK:SMART:USD:INTC";
    config.ringCapacity = 32;

    std::uint64_t persistedWindowId = 0;
    std::uint64_t persistedAnchorId = 0;

    {
        tape_engine::Server server(config);
        std::string startError;
        expect(server.start(&startError), "tape-engine should start for phase 3 restart persistence test: " + startError);

        bridge_batch::BuildOptions options;
        options.appSessionId = "app-engine-phase3-restart";
        options.runtimeSessionId = "runtime-engine-phase3-restart";
        options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

        bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
        std::string error;

        BridgeOutboxRecord orderIntent = makeBridgeRecord(8101, "order_intent", "WebSocket", "INTC", "BUY",
                                                          311, 2101, 0, "", "phase 3 restart anchor", "2026-03-14T09:41:00.100");
        orderIntent.instrumentId = "ib:conid:9101:STK:SMART:USD:INTC";
        options.batchSeq = 101;
        expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({orderIntent}, options)), &error),
               "tape-engine should accept the restart anchor batch: " + error);

        BridgeOutboxRecord bidTick = makeBridgeRecord(8102, "market_tick", "BrokerMarketData", "INTC", "BID",
                                                      0, 0, 0, "", "restart bid", "2026-03-14T09:41:00.120");
        bidTick.instrumentId = "ib:conid:9101:STK:SMART:USD:INTC";
        bidTick.marketField = 1;
        bidTick.price = 45.20;

        BridgeOutboxRecord askTick = makeBridgeRecord(8103, "market_tick", "BrokerMarketData", "INTC", "ASK",
                                                      0, 0, 0, "", "restart ask", "2026-03-14T09:41:00.130");
        askTick.instrumentId = "ib:conid:9101:STK:SMART:USD:INTC";
        askTick.marketField = 2;
        askTick.price = 45.21;

        options.batchSeq = 102;
        expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({bidTick, askTick}, options)), &error),
               "tape-engine should accept the restart market seed batch: " + error);

        BridgeOutboxRecord widenedAsk = makeBridgeRecord(8104, "market_tick", "BrokerMarketData", "INTC", "ASK",
                                                         0, 0, 0, "", "restart widened ask", "2026-03-14T09:41:00.150");
        widenedAsk.instrumentId = "ib:conid:9101:STK:SMART:USD:INTC";
        widenedAsk.marketField = 2;
        widenedAsk.price = 45.24;

        options.batchSeq = 103;
        expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({widenedAsk}, options)), &error),
               "tape-engine should accept the restart widening batch: " + error);

        waitUntil([&]() {
            const auto snapshot = server.snapshot();
            return snapshot.latestFrozenRevisionId >= 4 && snapshot.segments.size() >= 4;
        }, "tape-engine should freeze restart phase 3 batches");

        tape_engine::Client client(socketPath.string());
        tape_engine::QueryResponse response;

        tape_engine::QueryRequest anchorsRequest;
        anchorsRequest.requestId = "anchors-phase3-restart-initial";
        anchorsRequest.operation = "list_order_anchors";
        expect(client.query(anchorsRequest, &response, &error), "initial phase 3 restart anchor query should succeed: " + error);
        expect(response.events.is_array() && !response.events.empty(), "restart phase 3 setup should persist at least one anchor");
        persistedAnchorId = response.events.at(0).value("anchor_id", 0ULL);
        expect(persistedAnchorId > 0, "restart phase 3 setup should expose a persisted anchor id");

        tape_engine::QueryRequest windowsRequest;
        windowsRequest.requestId = "windows-phase3-restart-initial";
        windowsRequest.operation = "list_protected_windows";
        expect(client.query(windowsRequest, &response, &error), "initial phase 3 restart protected window query should succeed: " + error);
        expect(response.events.is_array() && !response.events.empty(), "restart phase 3 setup should persist protected windows");
        for (const auto& item : response.events) {
            if (item.value("reason", std::string()) == "order_intent") {
                persistedWindowId = item.value("window_id", 0ULL);
                break;
            }
        }
        expect(persistedWindowId > 0, "restart phase 3 setup should include an order_intent protected window");

        server.stop();
    }

    {
        tape_engine::Server restarted(config);
        std::string startError;
        expect(restarted.start(&startError), "tape-engine should restart and restore frozen phase 3 state: " + startError);
        const tape_engine::EngineSnapshot restoredSnapshot = restarted.snapshot();
        expect(restoredSnapshot.latestFrozenRevisionId >= 4, "restart should restore the latest frozen revision id");
        expect(restoredSnapshot.segments.size() >= 4, "restart should restore frozen segment metadata");

        tape_engine::Client client(socketPath.string());
        tape_engine::QueryResponse response;
        std::string error;

        tape_engine::QueryRequest findingsRequest;
        findingsRequest.requestId = "findings-phase3-restart-restored";
        findingsRequest.operation = "list_findings";
        expect(client.query(findingsRequest, &response, &error), "restored phase 3 findings query should succeed: " + error);
        expect(response.events.is_array() && !response.events.empty(), "restart should restore frozen findings");
        bool sawSpreadFinding = false;
        bool sawOrderFlowFinding = false;
        for (const auto& item : response.events) {
            sawSpreadFinding = sawSpreadFinding || item.value("kind", std::string()) == "spread_widened";
            sawOrderFlowFinding = sawOrderFlowFinding || item.value("kind", std::string()) == "order_flow_timeline";
        }
        expect(sawSpreadFinding, "restart should restore the spread_widened finding");
        expect(sawOrderFlowFinding, "restart should restore deferred order-flow findings too");

        tape_engine::QueryRequest readWindowRequest;
        readWindowRequest.requestId = "window-phase3-restart-restored";
        readWindowRequest.operation = "read_protected_window";
        readWindowRequest.windowId = persistedWindowId;
        expect(client.query(readWindowRequest, &response, &error), "restored protected window read should succeed: " + error);
        expect(response.summary.value("protected_window", json::object()).value("window_id", 0ULL) == persistedWindowId,
               "restored protected window read should return the requested window id");
        expect(response.events.is_array() && response.events.size() >= 3,
               "restored protected window read should return the anchored evidence window");
        bool sawOrderIntent = false;
        bool sawMarketTick = false;
        for (const auto& item : response.events) {
            const std::string kind = item.value("event_kind", std::string());
            sawOrderIntent = sawOrderIntent || kind == "order_intent";
            sawMarketTick = sawMarketTick || kind == "market_tick";
        }
        expect(sawOrderIntent, "restored protected window read should include the anchored order intent");
        expect(sawMarketTick, "restored protected window read should include surrounding market evidence");

        bridge_batch::BuildOptions options;
        options.appSessionId = "app-engine-phase3-restart";
        options.runtimeSessionId = "runtime-engine-phase3-restart";
        options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;
        options.batchSeq = 104;

        bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
        BridgeOutboxRecord secondOrderIntent = makeBridgeRecord(8105, "order_intent", "WebSocket", "INTC", "BUY",
                                                               312, 2102, 0, "", "phase 3 restart second anchor", "2026-03-14T09:41:01.100");
        secondOrderIntent.instrumentId = "ib:conid:9101:STK:SMART:USD:INTC";
        expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({secondOrderIntent}, options)), &error),
               "restarted tape-engine should accept a new anchored order batch: " + error);

        waitUntil([&]() {
            const auto snapshot = restarted.snapshot();
            return snapshot.latestFrozenRevisionId > restoredSnapshot.latestFrozenRevisionId &&
                   snapshot.segments.size() > restoredSnapshot.segments.size();
        }, "restarted tape-engine should freeze the post-restart anchor batch");

        tape_engine::QueryRequest anchorsRequest;
        anchorsRequest.requestId = "anchors-phase3-restart-restored";
        anchorsRequest.operation = "list_order_anchors";
        expect(client.query(anchorsRequest, &response, &error), "restored phase 3 anchor query should succeed after new ingest: " + error);
        expect(response.events.is_array() && response.events.size() >= 2, "restart should preserve prior anchors and append new ones");
        expect(response.events.at(0).value("anchor_id", 0ULL) > persistedAnchorId,
               "post-restart anchors should continue incrementing anchor ids");

        restarted.stop();
    }
}

void testTapeEnginePhase3ScansSessionIntoDurableReportArtifact() {
    const fs::path rootDir = testDataDir() / "tape-engine-phase3-session-report";
    const fs::path socketPath = testDataDir() / "tape-engine-phase3-session-report.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "ib:conid:9201:STK:SMART:USD:INTC";
    config.ringCapacity = 32;

    std::uint64_t reportId = 0;
    std::uint64_t servedRevisionId = 0;
    std::string overviewArtifactId;

    {
        tape_engine::Server server(config);
        std::string startError;
        expect(server.start(&startError), "tape-engine should start for session-report scan test: " + startError);

        bridge_batch::BuildOptions options;
        options.appSessionId = "app-engine-phase3-session-report";
        options.runtimeSessionId = "runtime-engine-phase3-session-report";
        options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

        bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
        std::string error;

        BridgeOutboxRecord orderIntent = makeBridgeRecord(8201, "order_intent", "WebSocket", "INTC", "BUY",
                                                          411, 2201, 0, "", "session report anchor", "2026-03-14T09:41:30.100");
        orderIntent.instrumentId = "ib:conid:9201:STK:SMART:USD:INTC";
        options.batchSeq = 201;
        expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({orderIntent}, options)), &error),
               "tape-engine should accept the session-report anchor batch: " + error);

        BridgeOutboxRecord bidTick = makeBridgeRecord(8202, "market_tick", "BrokerMarketData", "INTC", "BID",
                                                      0, 0, 0, "", "session report bid", "2026-03-14T09:41:30.120");
        bidTick.instrumentId = "ib:conid:9201:STK:SMART:USD:INTC";
        bidTick.marketField = 1;
        bidTick.price = 45.20;

        BridgeOutboxRecord askTick = makeBridgeRecord(8203, "market_tick", "BrokerMarketData", "INTC", "ASK",
                                                      0, 0, 0, "", "session report ask", "2026-03-14T09:41:30.130");
        askTick.instrumentId = "ib:conid:9201:STK:SMART:USD:INTC";
        askTick.marketField = 2;
        askTick.price = 45.21;

        options.batchSeq = 202;
        expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({bidTick, askTick}, options)), &error),
               "tape-engine should accept the session-report seed batch: " + error);

        BridgeOutboxRecord widenedAsk = makeBridgeRecord(8204, "market_tick", "BrokerMarketData", "INTC", "ASK",
                                                         0, 0, 0, "", "session report widened ask", "2026-03-14T09:41:30.150");
        widenedAsk.instrumentId = "ib:conid:9201:STK:SMART:USD:INTC";
        widenedAsk.marketField = 2;
        widenedAsk.price = 45.24;

        options.batchSeq = 203;
        expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({widenedAsk}, options)), &error),
               "tape-engine should accept the session-report widening batch: " + error);

        waitUntil([&]() {
            const auto snapshot = server.snapshot();
            return snapshot.latestFrozenRevisionId >= 4 && snapshot.segments.size() >= 4;
        }, "tape-engine should freeze session-report batches");

        tape_engine::Client client(socketPath.string());
        tape_engine::QueryResponse response;

        tape_engine::QueryRequest overviewRequest;
        overviewRequest.requestId = "read-phase3-session-overview";
        overviewRequest.operation = "read_session_overview";
        expect(client.query(overviewRequest, &response, &error), "phase 3 session overview read should succeed: " + error);
        overviewArtifactId = response.summary.value("artifact", json::object()).value("artifact_id", std::string());
        expectContains(response.summary.value("api", json::object()).value("envelope_schema", std::string()),
                       "com.foxy.tape-engine.investigation-envelope",
                       "session overview should expose the investigation envelope schema");
        expect(response.result.value("schema", std::string()) == tape_engine::kInvestigationResultSchema,
               "session overview should publish a typed investigation transport result");

        tape_engine::QueryRequest scanRequest;
        scanRequest.requestId = "scan-phase3-session-report";
        scanRequest.operation = "scan_session_report";
        expect(client.query(scanRequest, &response, &error), "phase 3 session report scan should succeed: " + error);
        expect(response.summary.value("is_durable_report", false),
               "session report scan should mark the result as a durable report");
        expect(response.summary.contains("session_report"),
               "session report scan should return persisted artifact metadata");
        expect(response.summary.value("session_report", json::object()).value("artifact_id", std::string()) ==
                   "session-report:" + std::to_string(response.summary.value("session_report", json::object()).value("report_id", 0ULL)),
               "session report scan should surface a stable durable artifact id");
        reportId = response.summary.value("session_report", json::object()).value("report_id", 0ULL);
        servedRevisionId = response.summary.value("served_revision_id", 0ULL);
        expect(reportId > 0, "session report scan should assign a durable report id");
        expect(servedRevisionId > 0, "session report scan should pin the report to a frozen revision");
        expect(response.summary.value("artifact", json::object()).value("artifact_id", std::string()) ==
                   "session-report:" + std::to_string(reportId),
               "session report scans should now promote the durable report artifact to the primary artifact envelope");
        expect(response.summary.value("source_artifact", json::object()).value("artifact_id", std::string()) == overviewArtifactId,
               "session report scans should preserve the source session-overview artifact they were derived from");
        expect(response.summary.value("incident_count", 0ULL) >= 1ULL,
               "session report scan should summarize at least one major incident");
        expect(response.summary.value("report_summary", json::object()).contains("top_incident_why_it_matters"),
               "session report scan should include top-incident explanations");
        expect(response.result.value("schema", std::string()) == tape_engine::kInvestigationResultSchema,
               "session report scans should publish a typed investigation transport result");

        tape_engine::QueryRequest rescanRequest = scanRequest;
        rescanRequest.requestId = "scan-phase3-session-report-again";
        expect(client.query(rescanRequest, &response, &error), "phase 3 repeated session report scan should succeed: " + error);
        expect(response.summary.value("session_report", json::object()).value("report_id", 0ULL) == reportId,
               "repeated session scans over the same frozen range should reuse the canonical report artifact");

        tape_engine::QueryRequest listReportsRequest;
        listReportsRequest.requestId = "list-phase3-session-reports";
        listReportsRequest.operation = "list_session_reports";
        expect(client.query(listReportsRequest, &response, &error), "phase 3 session report listing should succeed: " + error);
        expect(response.events.is_array() && !response.events.empty(), "session report listing should return the durable artifact");
        expect(response.events.at(0).value("report_id", 0ULL) == reportId,
               "session report listing should include the newly scanned report");
        expect(response.result.value("schema", std::string()) == tape_engine::kCollectionResultSchema &&
                   response.result.value("collection_kind", std::string()) == "session_reports",
               "session report listing should publish a typed collection transport result");

        expect(fs::exists(rootDir / "session-reports.jsonl"), "session report scanning should persist a report manifest");
        expect(fs::exists(rootDir / "artifact-lookup.msgpack"),
               "session report scanning should persist the artifact lookup index");
        {
            const json lookup = readMsgpackFile(rootDir / "artifact-lookup.msgpack");
            bool foundReport = false;
            for (const auto& item : lookup.value("session_reports", json::array())) {
                foundReport = foundReport || item.value("report_id", 0ULL) == reportId;
            }
            expect(foundReport, "artifact lookup index should include the persisted session report");
        }

        server.stop();
    }

    {
        tape_engine::Server restarted(config);
        std::string startError;
        expect(restarted.start(&startError), "tape-engine should restart and restore session report artifacts: " + startError);

        tape_engine::Client client(socketPath.string());
        tape_engine::QueryResponse response;
        std::string error;

        tape_engine::QueryRequest readReportRequest;
        readReportRequest.requestId = "read-phase3-session-report";
        readReportRequest.operation = "read_session_report";
        readReportRequest.reportId = reportId;
        expect(client.query(readReportRequest, &response, &error), "restored session report read should succeed: " + error);
        expect(response.summary.value("session_report", json::object()).value("report_id", 0ULL) == reportId,
               "restored session report should return the requested durable report id");
        expect(response.summary.value("session_report", json::object()).value("revision_id", 0ULL) == servedRevisionId,
               "restored session report should stay pinned to its frozen revision");
        expect(response.summary.value("is_durable_report", false),
               "restored session report should still be marked durable");
        expect(response.summary.contains("timeline"), "restored session report should keep the investigation timeline");
        expect(response.summary.contains("data_quality"), "restored session report should keep the data-quality block");
        expect(response.events.is_array() && !response.events.empty(),
               "restored session report should retain ranked incident rows");
        expect(response.result.value("schema", std::string()) == tape_engine::kInvestigationResultSchema,
               "restored session report reads should publish a typed investigation transport result");

        tape_engine::QueryRequest readArtifactRequest;
        readArtifactRequest.requestId = "read-artifact-session-report";
        readArtifactRequest.operation = "read_artifact";
        readArtifactRequest.artifactId = "session-report:" + std::to_string(reportId);
        expect(client.query(readArtifactRequest, &response, &error), "read_artifact should reopen the session report by stable artifact id: " + error);
        expect(response.summary.value("resolved_artifact_id", std::string()) == "session-report:" + std::to_string(reportId),
               "read_artifact should echo the resolved durable artifact id");
        expect(response.summary.value("api", json::object()).value("response_kind", std::string()) == "artifact_read",
               "read_artifact should expose a stable artifact-read response kind");
        expect(response.result.value("schema", std::string()) == tape_engine::kInvestigationResultSchema,
               "read_artifact should publish a typed investigation transport result");

        tape_engine::QueryRequest readOverviewArtifactRequest;
        readOverviewArtifactRequest.requestId = "read-artifact-session-overview";
        readOverviewArtifactRequest.operation = "read_artifact";
        readOverviewArtifactRequest.artifactId = overviewArtifactId;
        expect(client.query(readOverviewArtifactRequest, &response, &error), "read_artifact should reopen a session-overview selector artifact: " + error);
        expect(response.summary.value("resolved_artifact_id", std::string()) == overviewArtifactId,
               "read_artifact should reopen the exact session-overview artifact id that was emitted earlier");

        tape_engine::QueryRequest exportArtifactRequest;
        exportArtifactRequest.requestId = "export-artifact-session-report";
        exportArtifactRequest.operation = "export_artifact";
        exportArtifactRequest.artifactId = "session-report:" + std::to_string(reportId);
        exportArtifactRequest.exportFormat = "markdown";
        expect(client.query(exportArtifactRequest, &response, &error), "export_artifact should render session report markdown: " + error);
        expectContains(response.summary.value("markdown", std::string()), "# Session overview",
                       "export_artifact markdown should include the session report headline");
        expect(response.summary.value("artifact_export", json::object()).value("schema", std::string()) ==
                   "com.foxy.tape-engine.artifact-export",
               "artifact exports should expose a stable export schema identifier");
        expect(response.result.value("schema", std::string()) == tape_engine::kArtifactExportResultSchema,
               "artifact exports should publish a typed transport result");

        restarted.stop();
    }
}

void testTapeEnginePhase3PersistsIncidentAndOrderCaseReports() {
    const fs::path rootDir = testDataDir() / "tape-engine-phase3-case-reports";
    const fs::path socketPath = testDataDir() / "tape-engine-phase3-case-reports.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "ib:conid:9301:STK:SMART:USD:INTC";
    config.ringCapacity = 32;

    std::uint64_t incidentReportId = 0;
    std::uint64_t orderCaseReportId = 0;
    std::uint64_t logicalIncidentId = 0;
    std::uint64_t firstFindingId = 0;
    std::uint64_t firstAnchorId = 0;
    std::string orderCaseArtifactId;

    {
        tape_engine::Server server(config);
        std::string startError;
        expect(server.start(&startError), "tape-engine should start for durable case-report test: " + startError);

        bridge_batch::BuildOptions options;
        options.appSessionId = "app-engine-phase3-case-reports";
        options.runtimeSessionId = "runtime-engine-phase3-case-reports";
        options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

        bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
        std::string error;

        BridgeOutboxRecord orderIntent = makeBridgeRecord(8301, "order_intent", "WebSocket", "INTC", "BUY",
                                                          511, 2301, 0, "", "case report anchor", "2026-03-14T09:42:10.100");
        orderIntent.instrumentId = "ib:conid:9301:STK:SMART:USD:INTC";
        options.batchSeq = 301;
        expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({orderIntent}, options)), &error),
               "tape-engine should accept the case-report anchor batch: " + error);

        BridgeOutboxRecord bidTick = makeBridgeRecord(8302, "market_tick", "BrokerMarketData", "INTC", "BID",
                                                      0, 0, 0, "", "case report bid", "2026-03-14T09:42:10.120");
        bidTick.instrumentId = "ib:conid:9301:STK:SMART:USD:INTC";
        bidTick.marketField = 1;
        bidTick.price = 45.20;

        BridgeOutboxRecord askTick = makeBridgeRecord(8303, "market_tick", "BrokerMarketData", "INTC", "ASK",
                                                      0, 0, 0, "", "case report ask", "2026-03-14T09:42:10.130");
        askTick.instrumentId = "ib:conid:9301:STK:SMART:USD:INTC";
        askTick.marketField = 2;
        askTick.price = 45.21;

        options.batchSeq = 302;
        expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({bidTick, askTick}, options)), &error),
               "tape-engine should accept the case-report seed batch: " + error);

        BridgeOutboxRecord widenedAsk = makeBridgeRecord(8304, "market_tick", "BrokerMarketData", "INTC", "ASK",
                                                         0, 0, 0, "", "case report widened ask", "2026-03-14T09:42:10.150");
        widenedAsk.instrumentId = "ib:conid:9301:STK:SMART:USD:INTC";
        widenedAsk.marketField = 2;
        widenedAsk.price = 45.24;

        options.batchSeq = 303;
        expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({widenedAsk}, options)), &error),
               "tape-engine should accept the case-report widening batch: " + error);

        waitUntil([&]() {
            const auto snapshot = server.snapshot();
            return snapshot.latestFrozenRevisionId >= 4 && snapshot.segments.size() >= 4;
        }, "tape-engine should freeze case-report batches");

        tape_engine::Client client(socketPath.string());
        tape_engine::QueryResponse response;

        tape_engine::QueryRequest incidentsRequest;
        incidentsRequest.requestId = "incident-list-case-reports";
        incidentsRequest.operation = "list_incidents";
        expect(client.query(incidentsRequest, &response, &error), "case-report incident list query should succeed: " + error);
        expect(response.events.is_array() && !response.events.empty(), "case-report setup should create a ranked incident");
        logicalIncidentId = response.events.at(0).value("logical_incident_id", 0ULL);
        expect(logicalIncidentId > 0, "case-report setup should expose a logical incident id");
        expect(response.result.value("schema", std::string()) == tape_engine::kCollectionResultSchema &&
                   response.result.value("collection_kind", std::string()) == "incidents",
               "incident listing should publish a typed collection transport result");

        tape_engine::QueryRequest incidentReportRequest;
        incidentReportRequest.requestId = "scan-incident-case-report";
        incidentReportRequest.operation = "scan_incident_report";
        incidentReportRequest.logicalIncidentId = logicalIncidentId;
        expect(client.query(incidentReportRequest, &response, &error), "incident case-report scan should succeed: " + error);
        incidentReportId = response.summary.value("case_report_artifact", json::object()).value("report_id", 0ULL);
        expect(incidentReportId > 0, "incident case-report scan should persist a durable artifact");
        expect(response.summary.value("artifact", json::object()).value("artifact_id", std::string()) ==
                   "case-report:" + std::to_string(incidentReportId),
               "incident case-report scans should promote the durable case-report artifact to the primary artifact envelope");
        expect(response.summary.value("source_artifact", json::object()).value("artifact_id", std::string()) ==
                   "incident:" + std::to_string(logicalIncidentId),
               "incident case-report scans should preserve the source incident artifact id");
        expect(response.result.value("schema", std::string()) == tape_engine::kInvestigationResultSchema,
               "incident report scans should publish a typed investigation transport result");

        tape_engine::QueryRequest orderReportRequest;
        orderReportRequest.requestId = "scan-order-case-report";
        orderReportRequest.operation = "scan_order_case_report";
        orderReportRequest.orderId = 2301;
        expect(client.query(orderReportRequest, &response, &error), "order case-report scan should succeed: " + error);
        orderCaseReportId = response.summary.value("case_report_artifact", json::object()).value("report_id", 0ULL);
        expect(orderCaseReportId > 0, "order case-report scan should persist a durable artifact");
        expect(orderCaseReportId != incidentReportId, "incident and order case reports should persist as distinct artifacts");
        orderCaseArtifactId = response.summary.value("source_artifact", json::object()).value("artifact_id", std::string());
        expect(response.summary.value("artifact", json::object()).value("artifact_id", std::string()) ==
                   "case-report:" + std::to_string(orderCaseReportId),
               "order case-report scans should promote the durable case-report artifact to the primary artifact envelope");
        expect(response.result.value("schema", std::string()) == tape_engine::kInvestigationResultSchema,
               "order case-report scans should publish a typed investigation transport result");

        tape_engine::QueryRequest listReportsRequest;
        listReportsRequest.requestId = "list-case-reports";
        listReportsRequest.operation = "list_case_reports";
        expect(client.query(listReportsRequest, &response, &error), "case-report listing should succeed: " + error);
        expect(response.events.is_array() && response.events.size() >= 2, "case-report listing should include both durable report artifacts");
        expect(fs::exists(rootDir / "case-reports.jsonl"), "case-report scanning should persist a case report manifest");
        expect(response.result.value("schema", std::string()) == tape_engine::kCollectionResultSchema &&
                   response.result.value("collection_kind", std::string()) == "case_reports",
               "case-report listing should publish a typed collection transport result");

        tape_engine::QueryRequest findingsRequest;
        findingsRequest.requestId = "list-findings-case-reports";
        findingsRequest.operation = "list_findings";
        expect(client.query(findingsRequest, &response, &error), "case-report finding list query should succeed: " + error);
        expect(response.events.is_array() && !response.events.empty(), "case-report setup should produce at least one finding");
        firstFindingId = response.events.at(0).value("finding_id", 0ULL);
        expect(response.result.value("schema", std::string()) == tape_engine::kCollectionResultSchema &&
                   response.result.value("collection_kind", std::string()) == "findings",
               "finding listing should publish a typed collection transport result");

        tape_engine::QueryRequest anchorsRequest;
        anchorsRequest.requestId = "list-anchors-case-reports";
        anchorsRequest.operation = "list_order_anchors";
        expect(client.query(anchorsRequest, &response, &error), "case-report anchor list query should succeed: " + error);
        expect(response.events.is_array() && !response.events.empty(), "case-report setup should persist at least one anchor");
        firstAnchorId = response.events.at(0).value("anchor_id", 0ULL);
        expect(response.result.value("schema", std::string()) == tape_engine::kCollectionResultSchema &&
                   response.result.value("collection_kind", std::string()) == "order_anchors",
               "order-anchor listing should publish a typed collection transport result");

        expect(fs::exists(rootDir / "artifact-lookup.msgpack"),
               "case-report scanning should persist the artifact lookup index");
        {
            const json lookup = readMsgpackFile(rootDir / "artifact-lookup.msgpack");
            bool foundIncidentCaseReport = false;
            bool foundOrderCaseReport = false;
            bool foundFinding = false;
            bool foundAnchor = false;
            bool foundIncident = false;
            for (const auto& item : lookup.value("case_reports", json::array())) {
                const std::uint64_t reportId = item.value("report_id", 0ULL);
                foundIncidentCaseReport = foundIncidentCaseReport || reportId == incidentReportId;
                foundOrderCaseReport = foundOrderCaseReport || reportId == orderCaseReportId;
            }
            for (const auto& item : lookup.value("findings", json::array())) {
                foundFinding = foundFinding || item.value("finding_id", 0ULL) == firstFindingId;
            }
            for (const auto& item : lookup.value("order_anchors", json::array())) {
                foundAnchor = foundAnchor || item.value("anchor_id", 0ULL) == firstAnchorId;
            }
            for (const auto& item : lookup.value("incidents", json::array())) {
                foundIncident = foundIncident || item.value("logical_incident_id", 0ULL) == logicalIncidentId;
            }
            expect(foundIncidentCaseReport && foundOrderCaseReport,
                   "artifact lookup index should include both durable case reports");
            expect(foundFinding, "artifact lookup index should include direct finding lookup entries");
            expect(foundAnchor, "artifact lookup index should include direct anchor lookup entries");
            expect(foundIncident, "artifact lookup index should include the latest logical incident lookup entry");
        }

        server.stop();
    }

    {
        tape_engine::Server restarted(config);
        std::string startError;
        expect(restarted.start(&startError), "tape-engine should restart and restore durable case reports: " + startError);

        tape_engine::Client client(socketPath.string());
        tape_engine::QueryResponse response;
        std::string error;

        tape_engine::QueryRequest readIncidentReportRequest;
        readIncidentReportRequest.requestId = "read-incident-case-report";
        readIncidentReportRequest.operation = "read_case_report";
        readIncidentReportRequest.reportId = incidentReportId;
        expect(client.query(readIncidentReportRequest, &response, &error), "restored incident case-report read should succeed: " + error);
        expect(response.summary.value("case_report_artifact", json::object()).value("report_id", 0ULL) == incidentReportId,
               "restored incident case-report should preserve its durable report id");
        expect(response.summary.value("case_report_artifact", json::object()).value("report_type", std::string()) == "incident_case",
               "restored incident case-report should keep its report type");
        expect(response.result.value("schema", std::string()) == tape_engine::kInvestigationResultSchema,
               "restored incident case-report reads should publish a typed investigation transport result");

        tape_engine::QueryRequest readOrderReportRequest;
        readOrderReportRequest.requestId = "read-order-case-report";
        readOrderReportRequest.operation = "read_case_report";
        readOrderReportRequest.reportId = orderCaseReportId;
        expect(client.query(readOrderReportRequest, &response, &error), "restored order case-report read should succeed: " + error);
        expect(response.summary.value("case_report_artifact", json::object()).value("report_id", 0ULL) == orderCaseReportId,
               "restored order case-report should preserve its durable report id");
        expect(response.summary.value("case_report_artifact", json::object()).value("report_type", std::string()) == "order_case",
               "restored order case-report should keep its report type");
        expect(response.summary.contains("case_report"), "restored order case-report should still expose the case summary");
        expect(response.result.value("schema", std::string()) == tape_engine::kInvestigationResultSchema,
               "restored order case-report reads should publish a typed investigation transport result");

        tape_engine::QueryRequest readOrderCaseArtifactRequest;
        readOrderCaseArtifactRequest.requestId = "read-order-case-artifact";
        readOrderCaseArtifactRequest.operation = "read_artifact";
        readOrderCaseArtifactRequest.artifactId = orderCaseArtifactId;
        expect(client.query(readOrderCaseArtifactRequest, &response, &error), "read_artifact should reopen the order-case selector artifact: " + error);
        expect(response.summary.value("resolved_artifact_id", std::string()) == orderCaseArtifactId,
               "read_artifact should resolve selector-style order-case artifacts directly");
        expect(response.result.value("schema", std::string()) == tape_engine::kInvestigationResultSchema,
               "order-case artifact reads should publish a typed investigation transport result");

        tape_engine::QueryRequest readFindingArtifactRequest;
        readFindingArtifactRequest.requestId = "read-finding-artifact";
        readFindingArtifactRequest.operation = "read_artifact";
        readFindingArtifactRequest.artifactId = "finding:" + std::to_string(firstFindingId);
        expect(client.query(readFindingArtifactRequest, &response, &error), "read_artifact should reopen finding artifacts directly: " + error);
        expect(response.summary.value("finding", json::object()).value("finding_id", 0ULL) == firstFindingId,
               "finding artifact reads should resolve the requested finding");
        expect(response.result.value("schema", std::string()) == tape_engine::kInvestigationResultSchema,
               "finding artifact reads should publish a typed investigation transport result");

        tape_engine::QueryRequest readAnchorArtifactRequest;
        readAnchorArtifactRequest.requestId = "read-anchor-artifact";
        readAnchorArtifactRequest.operation = "read_artifact";
        readAnchorArtifactRequest.artifactId = "anchor:" + std::to_string(firstAnchorId);
        expect(client.query(readAnchorArtifactRequest, &response, &error), "read_artifact should reopen anchor artifacts directly: " + error);
        expect(response.summary.value("order_anchor", json::object()).value("anchor_id", 0ULL) == firstAnchorId,
               "anchor artifact reads should resolve the requested anchor");
        expect(response.result.value("schema", std::string()) == tape_engine::kInvestigationResultSchema,
               "anchor artifact reads should publish a typed investigation transport result");

        tape_engine::QueryRequest exportCaseArtifactRequest;
        exportCaseArtifactRequest.requestId = "export-case-artifact";
        exportCaseArtifactRequest.operation = "export_artifact";
        exportCaseArtifactRequest.artifactId = "case-report:" + std::to_string(orderCaseReportId);
        exportCaseArtifactRequest.exportFormat = "json-bundle";
        expect(client.query(exportCaseArtifactRequest, &response, &error), "export_artifact should export a case report bundle: " + error);
        expect(response.summary.value("bundle", json::object()).value("summary", json::object()).contains("case_report"),
               "case report export bundle should preserve the case-report payload");
        expect(response.result.value("schema", std::string()) == tape_engine::kArtifactExportResultSchema,
               "case-report export_artifact should publish a typed transport result");

        restarted.stop();
    }
}

void testTapeEnginePhase6ExportsAndImportsPortableBundles() {
    const fs::path rootDir = testDataDir() / "tape-engine-phase6-bundles";
    const fs::path socketPath = testDataDir() / "tape-engine-phase6-bundles.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "ib:conid:9401:STK:SMART:USD:INTC";
    config.ringCapacity = 32;

    std::uint64_t sessionReportId = 0;
    std::uint64_t caseReportId = 0;
    std::uint64_t importedCaseId = 0;
    std::string caseBundlePath;

    {
        tape_engine::Server server(config);
        std::string startError;
        expect(server.start(&startError), "tape-engine should start for phase 6 bundle test: " + startError);

        bridge_batch::BuildOptions options;
        options.appSessionId = "app-engine-phase6-bundles";
        options.runtimeSessionId = "runtime-engine-phase6-bundles";
        options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

        bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
        std::string error;

        BridgeOutboxRecord orderIntent = makeBridgeRecord(8401, "order_intent", "WebSocket", "INTC", "BUY",
                                                          611, 2401, 0, "", "phase 6 bundle anchor", "2026-03-14T09:42:40.100");
        orderIntent.instrumentId = "ib:conid:9401:STK:SMART:USD:INTC";
        options.batchSeq = 401;
        expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({orderIntent}, options)), &error),
               "tape-engine should accept the phase 6 order anchor batch: " + error);

        BridgeOutboxRecord bidTick = makeBridgeRecord(8402, "market_tick", "BrokerMarketData", "INTC", "BID",
                                                      0, 0, 0, "", "phase 6 bid", "2026-03-14T09:42:40.120");
        bidTick.instrumentId = "ib:conid:9401:STK:SMART:USD:INTC";
        bidTick.marketField = 1;
        bidTick.price = 45.30;

        BridgeOutboxRecord askTick = makeBridgeRecord(8403, "market_tick", "BrokerMarketData", "INTC", "ASK",
                                                      0, 0, 0, "", "phase 6 ask", "2026-03-14T09:42:40.130");
        askTick.instrumentId = "ib:conid:9401:STK:SMART:USD:INTC";
        askTick.marketField = 2;
        askTick.price = 45.31;

        options.batchSeq = 402;
        expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({bidTick, askTick}, options)), &error),
               "tape-engine should accept the phase 6 market seed batch: " + error);

        BridgeOutboxRecord widenedAsk = makeBridgeRecord(8404, "market_tick", "BrokerMarketData", "INTC", "ASK",
                                                         0, 0, 0, "", "phase 6 widened ask", "2026-03-14T09:42:40.150");
        widenedAsk.instrumentId = "ib:conid:9401:STK:SMART:USD:INTC";
        widenedAsk.marketField = 2;
        widenedAsk.price = 45.35;

        options.batchSeq = 403;
        expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({widenedAsk}, options)), &error),
               "tape-engine should accept the phase 6 widening batch: " + error);

        waitUntil([&]() {
            const auto snapshot = server.snapshot();
            return snapshot.latestFrozenRevisionId >= 4 && snapshot.segments.size() >= 4;
        }, "tape-engine should freeze phase 6 bundle batches");

        tape_engine::Client client(socketPath.string());
        tape_engine::QueryResponse response;

        tape_engine::QueryRequest sessionReportRequest;
        sessionReportRequest.requestId = "phase6-scan-session-report";
        sessionReportRequest.operation = "scan_session_report";
        expect(client.query(sessionReportRequest, &response, &error), "phase 6 session report scan should succeed: " + error);
        sessionReportId = response.summary.value("session_report", json::object()).value("report_id", 0ULL);
        expect(sessionReportId > 0, "phase 6 session report scan should persist a durable session report");

        tape_engine::QueryRequest caseReportRequest;
        caseReportRequest.requestId = "phase6-scan-order-case-report";
        caseReportRequest.operation = "scan_order_case_report";
        caseReportRequest.orderId = 2401;
        expect(client.query(caseReportRequest, &response, &error), "phase 6 order-case report scan should succeed: " + error);
        caseReportId = response.summary.value("case_report_artifact", json::object()).value("report_id", 0ULL);
        expect(caseReportId > 0, "phase 6 order-case report scan should persist a durable case report");

        tape_engine::QueryRequest exportSessionBundleRequest;
        exportSessionBundleRequest.requestId = "phase6-export-session-bundle";
        exportSessionBundleRequest.operation = "export_session_bundle";
        exportSessionBundleRequest.reportId = sessionReportId;
        expect(client.query(exportSessionBundleRequest, &response, &error), "phase 6 session bundle export should succeed: " + error);
        const std::string sessionBundlePath =
            response.summary.value("bundle", json::object()).value("bundle_path", std::string());
        expect(!sessionBundlePath.empty() && fs::exists(sessionBundlePath),
               "phase 6 session bundle export should write a portable session bundle");
        const json sessionBundle = readMsgpackFile(sessionBundlePath);
        expect(sessionBundle.value("schema", std::string()) == tape_engine::kReportBundleSchema,
               "phase 6 session bundle should use the stable bundle schema");
        expect(sessionBundle.value("bundle_type", std::string()) == "session_bundle",
               "phase 6 session bundle should record the session bundle type");

        tape_engine::QueryRequest verifySessionBundleRequest;
        verifySessionBundleRequest.requestId = "phase6-verify-session-bundle";
        verifySessionBundleRequest.operation = "verify_bundle";
        verifySessionBundleRequest.bundlePath = sessionBundlePath;
        expect(client.query(verifySessionBundleRequest, &response, &error),
               "phase 6 session bundle verify should succeed: " + error);
        expect(response.summary.value("verify_status", std::string()) == "valid",
               "phase 6 session bundle verify should mark the bundle valid");
        expect(!response.summary.value("import_supported", true),
               "phase 6 session bundle verify should mark the bundle non-importable");
        expect(!response.summary.value("already_imported", true),
               "phase 6 session bundle verify should not mark the session bundle imported");
        expect(!response.summary.value("can_import", true),
               "phase 6 session bundle verify should not allow importing a session bundle");
        expect(response.result.value("schema", std::string()) == tape_engine::kBundleVerifyResultSchema,
               "phase 6 session bundle verify should publish a typed bundle verify transport result");

        const fs::path corruptBundlePath = rootDir / "corrupt-case-bundle.msgpack";
        {
            std::ofstream out(corruptBundlePath, std::ios::binary);
            expect(out.is_open(), "phase 6 corrupt bundle fixture should open for write");
            out << "not-msgpack";
        }

        tape_engine::QueryRequest verifyCorruptBundleRequest;
        verifyCorruptBundleRequest.requestId = "phase6-verify-corrupt-bundle";
        verifyCorruptBundleRequest.operation = "verify_bundle";
        verifyCorruptBundleRequest.bundlePath = corruptBundlePath.string();
        expect(!client.query(verifyCorruptBundleRequest, &response, &error),
               "phase 6 corrupt bundle verify should fail");
        expectContains(error, "bundle is not valid MessagePack JSON",
                       "phase 6 corrupt bundle verify should expose a stable parse error");

        tape_engine::QueryRequest importCorruptBundleRequest;
        importCorruptBundleRequest.requestId = "phase6-import-corrupt-bundle";
        importCorruptBundleRequest.operation = "import_case_bundle";
        importCorruptBundleRequest.bundlePath = corruptBundlePath.string();
        expect(!client.query(importCorruptBundleRequest, &response, &error),
               "phase 6 corrupt bundle import should fail");
        expectContains(error, "bundle is not valid MessagePack JSON",
                       "phase 6 corrupt bundle import should expose a stable parse error");
        expect(!fs::exists(rootDir / "imported-case-bundles.jsonl"),
               "phase 6 corrupt bundle import should not create an imported-case manifest");

        tape_engine::QueryRequest exportCaseBundleRequest;
        exportCaseBundleRequest.requestId = "phase6-export-case-bundle";
        exportCaseBundleRequest.operation = "export_case_bundle";
        exportCaseBundleRequest.reportId = caseReportId;
        expect(client.query(exportCaseBundleRequest, &response, &error), "phase 6 case bundle export should succeed: " + error);
        caseBundlePath = response.summary.value("bundle", json::object()).value("bundle_path", std::string());
        expect(!caseBundlePath.empty() && fs::exists(caseBundlePath),
               "phase 6 case bundle export should write a portable case bundle");
        expect(response.result.value("schema", std::string()) == tape_engine::kBundleExportResultSchema,
               "phase 6 case bundle export should publish a typed bundle export transport result");
        const json caseBundle = readMsgpackFile(caseBundlePath);
        expect(caseBundle.value("bundle_type", std::string()) == "case_bundle",
               "phase 6 case bundle should record the case bundle type");
        expect(caseBundle.value("source_report_id", 0ULL) == caseReportId,
               "phase 6 case bundle should carry the source case report id");
        const fs::path renamedCaseBundlePath = rootDir / "renamed-case-bundle.msgpack";
        fs::rename(caseBundlePath, renamedCaseBundlePath, ec);
        expect(!ec && fs::exists(renamedCaseBundlePath),
               "phase 6 should allow renaming the exported case bundle before import");
        caseBundlePath = renamedCaseBundlePath.string();
        tape_bundle::PortableBundleInspection renamedBundleInspection;
        std::string renamedInspectError;
        expect(tape_bundle::inspectPortableBundle(caseBundlePath, &renamedBundleInspection, &renamedInspectError),
               "phase 6 should inspect the renamed case bundle: " + renamedInspectError);

        tape_engine::QueryRequest verifyCaseBundleRequest;
        verifyCaseBundleRequest.requestId = "phase6-verify-case-bundle";
        verifyCaseBundleRequest.operation = "verify_bundle";
        verifyCaseBundleRequest.bundlePath = caseBundlePath;
        expect(client.query(verifyCaseBundleRequest, &response, &error),
               "phase 6 case bundle verify should succeed: " + error);
        expect(response.summary.value("verify_status", std::string()) == "valid",
               "phase 6 case bundle verify should mark the bundle valid");
        expect(response.summary.value("import_supported", false),
               "phase 6 case bundle verify should mark the case bundle importable");
        expect(!response.summary.value("already_imported", true),
               "phase 6 case bundle verify should not mark the fresh bundle imported");
        expect(response.summary.value("can_import", false),
               "phase 6 case bundle verify should allow importing the fresh case bundle");
        expect(!response.summary.value("bundle", json::object()).value("payload_sha256", std::string()).empty(),
               "phase 6 case bundle verify should expose the payload hash");
        expect(response.summary.value("bundle", json::object()).value("payload_sha256", std::string()) ==
                   renamedBundleInspection.payloadSha256,
               "phase 6 case bundle verify should preserve payload hash after rename");
        expect(response.result.value("schema", std::string()) == tape_engine::kBundleVerifyResultSchema,
               "phase 6 case bundle verify should publish a typed bundle verify transport result");

        tape_engine::QueryRequest importRequest;
        importRequest.requestId = "phase6-import-case-bundle";
        importRequest.operation = "import_case_bundle";
        importRequest.bundlePath = caseBundlePath;
        expect(client.query(importRequest, &response, &error), "phase 6 case bundle import should succeed: " + error);
        expect(!response.summary.value("duplicate_import", true),
               "phase 6 first case bundle import should not be marked duplicate");
        importedCaseId = response.summary.value("imported_case", json::object()).value("imported_case_id", 0ULL);
        expect(importedCaseId > 0, "phase 6 case bundle import should assign an imported case id");
        expect(fs::exists(rootDir / "imported-case-bundles.jsonl"),
               "phase 6 case bundle import should persist an imported-case manifest");
        expect(response.result.value("schema", std::string()) == tape_engine::kCaseBundleImportResultSchema,
               "phase 6 case bundle import should publish a typed import transport result");

        tape_engine::QueryRequest listImportedCasesRequest;
        listImportedCasesRequest.requestId = "phase6-list-imported-cases";
        listImportedCasesRequest.operation = "list_imported_cases";
        expect(client.query(listImportedCasesRequest, &response, &error), "phase 6 imported-case listing should succeed: " + error);
        expect(response.events.is_array() && !response.events.empty(),
               "phase 6 imported-case listing should return the imported bundle inventory");
        expect(response.events.at(0).value("imported_case_id", 0ULL) == importedCaseId,
               "phase 6 imported-case listing should surface the imported bundle record");
        expect(response.events.at(0).value("payload_sha256", std::string()) == renamedBundleInspection.payloadSha256,
               "phase 6 imported-case listing should preserve renamed-bundle recognition by payload hash");
        expect(response.events.at(0).value("source_bundle_path", std::string()) == caseBundlePath,
               "phase 6 imported-case listing should preserve the renamed bundle path");
        expect(response.result.value("schema", std::string()) == tape_engine::kImportedCaseInventoryResultSchema,
               "phase 6 imported-case listing should publish a typed inventory transport result");

        tape_engine::QueryRequest readImportedArtifactRequest;
        readImportedArtifactRequest.requestId = "phase6-read-imported-artifact";
        readImportedArtifactRequest.operation = "read_artifact";
        readImportedArtifactRequest.artifactId = "imported-case:" + std::to_string(importedCaseId);
        expect(client.query(readImportedArtifactRequest, &response, &error), "phase 6 imported case read_artifact should succeed: " + error);
        expect(response.summary.value("imported_case", json::object()).value("imported_case_id", 0ULL) == importedCaseId,
               "phase 6 imported case read_artifact should surface the imported case record");
        expect(response.summary.value("artifact", json::object()).value("artifact_id", std::string()) ==
                   "imported-case:" + std::to_string(importedCaseId),
               "phase 6 imported case read_artifact should promote the imported artifact envelope");
        expect(response.summary.value("source_artifact", json::object()).value("artifact_id", std::string()) ==
                   "case-report:" + std::to_string(caseReportId),
               "phase 6 imported case read_artifact should preserve the source case report artifact id");

        tape_engine::QueryRequest duplicateImportRequest = importRequest;
        duplicateImportRequest.requestId = "phase6-import-case-bundle-duplicate";
        expect(client.query(duplicateImportRequest, &response, &error), "phase 6 duplicate case bundle import should succeed: " + error);
        expect(response.summary.value("duplicate_import", false),
               "phase 6 duplicate case bundle import should be marked duplicate");
        expect(response.summary.value("imported_case", json::object()).value("imported_case_id", 0ULL) == importedCaseId,
               "phase 6 duplicate case bundle import should reuse the existing imported case record");

        tape_engine::QueryRequest verifyImportedCaseBundleRequest = verifyCaseBundleRequest;
        verifyImportedCaseBundleRequest.requestId = "phase6-verify-case-bundle-imported";
        expect(client.query(verifyImportedCaseBundleRequest, &response, &error),
               "phase 6 imported case bundle verify should succeed: " + error);
        expect(response.summary.value("already_imported", false),
               "phase 6 imported case bundle verify should mark the bundle already imported");
        expect(!response.summary.value("can_import", true),
               "phase 6 imported case bundle verify should stop offering import after import");
        expect(response.summary.value("imported_case", json::object()).value("imported_case_id", 0ULL) == importedCaseId,
               "phase 6 imported case bundle verify should surface the existing imported case id");

        {
            const json lookup = readMsgpackFile(rootDir / "artifact-lookup.msgpack");
            bool foundImportedCase = false;
            for (const auto& item : lookup.value("imported_cases", json::array())) {
                foundImportedCase = foundImportedCase || item.value("imported_case_id", 0ULL) == importedCaseId;
            }
            expect(foundImportedCase, "phase 6 artifact lookup index should include imported case records");
        }

        server.stop();
    }

    {
        json staleLookup = readMsgpackFile(rootDir / "artifact-lookup.msgpack");
        staleLookup["imported_cases"] = json::array();
        std::ofstream out(rootDir / "artifact-lookup.msgpack", std::ios::binary | std::ios::trunc);
        expect(out.is_open(), "phase 6 stale lookup fixture should open for write");
        const std::vector<std::uint8_t> payload = json::to_msgpack(staleLookup);
        out.write(reinterpret_cast<const char*>(payload.data()), static_cast<std::streamsize>(payload.size()));
        expect(out.good(), "phase 6 stale lookup fixture should write");
    }

    {
        tape_engine::Server restarted(config);
        std::string startError;
        expect(restarted.start(&startError), "tape-engine should restart and restore imported case bundles: " + startError);

        tape_engine::Client client(socketPath.string());
        tape_engine::QueryResponse response;
        std::string error;

        tape_engine::QueryRequest listImportedCasesRequest;
        listImportedCasesRequest.requestId = "phase6-list-imported-cases-restored";
        listImportedCasesRequest.operation = "list_imported_cases";
        expect(client.query(listImportedCasesRequest, &response, &error), "phase 6 restored imported-case listing should succeed: " + error);
        expect(response.events.is_array() && !response.events.empty(),
               "phase 6 restored imported-case listing should retain the imported inventory");
        expect(response.events.at(0).value("imported_case_id", 0ULL) == importedCaseId,
               "phase 6 restored imported-case listing should preserve the imported case id");

        tape_engine::QueryRequest readImportedArtifactRequest;
        readImportedArtifactRequest.requestId = "phase6-read-imported-artifact-restored";
        readImportedArtifactRequest.operation = "read_artifact";
        readImportedArtifactRequest.artifactId = "imported-case:" + std::to_string(importedCaseId);
        expect(client.query(readImportedArtifactRequest, &response, &error), "phase 6 restored imported case read_artifact should succeed: " + error);
        expect(response.summary.value("imported_case", json::object()).value("source_bundle_path", std::string()) == caseBundlePath,
               "phase 6 restored imported case should preserve the original source bundle path");
        expect(response.summary.value("artifact", json::object()).value("artifact_id", std::string()) ==
                   "imported-case:" + std::to_string(importedCaseId),
               "phase 6 restored imported case should preserve the imported artifact id");
        {
            const json lookup = readMsgpackFile(rootDir / "artifact-lookup.msgpack");
            bool foundImportedCase = false;
            for (const auto& item : lookup.value("imported_cases", json::array())) {
                foundImportedCase = foundImportedCase || item.value("imported_case_id", 0ULL) == importedCaseId;
            }
            expect(foundImportedCase,
                   "phase 6 restart should rebuild a stale imported-case lookup index from the manifest");
        }

        restarted.stop();
    }
}

void testTapeEnginePhase7ArtifactsReuseDeterministically() {
    const fs::path rootDir = testDataDir() / "tape-engine-phase7-reuse";
    const fs::path socketPath = testDataDir() / "tape-engine-phase7-reuse.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "ib:conid:9501:STK:SMART:USD:INTC";
    config.ringCapacity = 32;

    tape_engine::Server server(config);
    std::string startError;
    expect(server.start(&startError), "tape-engine should start for phase 7 reuse test: " + startError);

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-engine-phase7-reuse";
    options.runtimeSessionId = "runtime-engine-phase7-reuse";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;

    BridgeOutboxRecord orderIntent = makeBridgeRecord(9501, "order_intent", "WebSocket", "INTC", "BUY",
                                                      4101, 9101, 5101, "", "phase 7 order intent", "2026-03-14T10:12:00.100");
    orderIntent.instrumentId = "ib:conid:9501:STK:SMART:USD:INTC";

    BridgeOutboxRecord orderStatus = makeBridgeRecord(9502, "order_status", "BrokerOrderStatus", "INTC", "BUY",
                                                      4101, 9101, 5101, "", "phase 7 submitted", "2026-03-14T10:12:00.200");
    orderStatus.instrumentId = "ib:conid:9501:STK:SMART:USD:INTC";

    BridgeOutboxRecord fillExecution = makeBridgeRecord(9503, "fill_execution", "BrokerExecution", "INTC", "BUY",
                                                        4101, 9101, 5101, "E-PHASE7-1", "phase 7 fill", "2026-03-14T10:12:00.300");
    fillExecution.instrumentId = "ib:conid:9501:STK:SMART:USD:INTC";

    options.batchSeq = 501;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({orderIntent}, options)), &error),
           "phase 7 reuse fixture should accept the order intent batch: " + error);
    options.batchSeq = 502;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({orderStatus}, options)), &error),
           "phase 7 reuse fixture should accept the order status batch: " + error);
    options.batchSeq = 503;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({fillExecution}, options)), &error),
           "phase 7 reuse fixture should accept the fill batch: " + error);

    waitUntil([&]() {
        const auto snapshot = server.snapshot();
        return snapshot.latestFrozenRevisionId >= 3 && snapshot.segments.size() >= 3;
    }, "phase 7 reuse fixture should freeze the seeded batches");

    tape_engine::Client client(socketPath.string());
    tape_engine::QueryResponse response;

    tape_engine::QueryRequest caseReportRequest;
    caseReportRequest.requestId = "phase7-case-report";
    caseReportRequest.operation = "scan_order_case_report";
    caseReportRequest.orderId = 9101;
    expect(client.query(caseReportRequest, &response, &error), "phase 7 case report scan should succeed: " + error);
    const std::uint64_t caseReportId =
        response.summary.value("case_report_artifact", json::object()).value("report_id", 0ULL);
    expect(caseReportId > 0, "phase 7 case report scan should persist a durable case report");

    tape_engine::QueryRequest exportCaseBundleRequest;
    exportCaseBundleRequest.requestId = "phase7-export-case-bundle";
    exportCaseBundleRequest.operation = "export_case_bundle";
    exportCaseBundleRequest.reportId = caseReportId;
    expect(client.query(exportCaseBundleRequest, &response, &error), "phase 7 case bundle export should succeed: " + error);
    const std::string caseBundlePath =
        response.summary.value("bundle", json::object()).value("bundle_path", std::string());
    expect(!caseBundlePath.empty() && fs::exists(caseBundlePath),
           "phase 7 case bundle export should write a portable case bundle");

    tape_phase7::AnalysisArtifact firstAnalysis;
    bool analysisCreated = false;
    std::string phase7Code;
    std::string phase7Message;
    expect(tape_phase7::runAnalyzerFromBundlePath(caseBundlePath,
                                                  {},
                                                  &firstAnalysis,
                                                  &analysisCreated,
                                                  &phase7Code,
                                                  &phase7Message),
           "phase 7 analyzer first run should succeed: " + phase7Code + " " + phase7Message);
    expect(analysisCreated, "phase 7 analyzer first run should create a new analysis artifact");

    tape_phase7::AnalysisArtifact secondAnalysis;
    analysisCreated = true;
    expect(tape_phase7::runAnalyzerFromBundlePath(caseBundlePath,
                                                  {},
                                                  &secondAnalysis,
                                                  &analysisCreated,
                                                  &phase7Code,
                                                  &phase7Message),
           "phase 7 analyzer rerun should succeed: " + phase7Code + " " + phase7Message);
    expect(!analysisCreated, "phase 7 analyzer rerun should reuse the persisted analysis artifact");
    expect(firstAnalysis.analysisArtifact.artifactId == secondAnalysis.analysisArtifact.artifactId,
           "phase 7 analyzer rerun should preserve the deterministic analysis artifact id");
    expect(firstAnalysis.analysisArtifact.manifestPath == secondAnalysis.analysisArtifact.manifestPath,
           "phase 7 analyzer rerun should preserve the deterministic analysis manifest path");
    expect(firstAnalysis.generatedAtUtc == secondAnalysis.generatedAtUtc,
           "phase 7 analyzer rerun should preserve the generated_at_utc stamp");
    expect(readTextFile(firstAnalysis.analysisArtifact.manifestPath) ==
               readTextFile(secondAnalysis.analysisArtifact.manifestPath),
           "phase 7 analyzer rerun should leave the persisted manifest unchanged");

    std::vector<tape_phase7::AnalyzerProfileSpec> profiles;
    expect(tape_phase7::listAnalyzerProfiles(&profiles, &phase7Code, &phase7Message),
           "phase 7 profile discovery should succeed: " + phase7Code + " " + phase7Message);
    expect(std::any_of(profiles.begin(), profiles.end(),
                       [](const tape_phase7::AnalyzerProfileSpec& profile) {
                           return profile.analysisProfile == tape_phase7::kFillQualityAnalyzerProfile;
                       }),
           "phase 7 profile discovery should expose the fill-quality review profile");
    expect(std::any_of(profiles.begin(), profiles.end(),
                       [](const tape_phase7::AnalyzerProfileSpec& profile) {
                           return profile.analysisProfile == tape_phase7::kLiquidityBehaviorAnalyzerProfile;
                       }),
           "phase 7 profile discovery should expose the liquidity-behavior review profile");
    expect(std::any_of(profiles.begin(), profiles.end(),
                       [](const tape_phase7::AnalyzerProfileSpec& profile) {
                           return profile.analysisProfile == tape_phase7::kAdverseSelectionAnalyzerProfile;
                       }),
           "phase 7 profile discovery should expose the adverse-selection review profile");
    expect(std::any_of(profiles.begin(), profiles.end(),
                       [](const tape_phase7::AnalyzerProfileSpec& profile) {
                           return profile.analysisProfile == tape_phase7::kOrderImpactAnalyzerProfile;
                       }),
           "phase 7 profile discovery should expose the order-impact review profile");

    tape_phase7::AnalysisArtifact fillQualityAnalysis;
    bool extraProfileCreated = false;
    expect(tape_phase7::runAnalyzerFromBundlePath(caseBundlePath,
                                                  tape_phase7::kFillQualityAnalyzerProfile,
                                                  &fillQualityAnalysis,
                                                  &extraProfileCreated,
                                                  &phase7Code,
                                                  &phase7Message),
           "phase 7 fill-quality analyzer should succeed: " + phase7Code + " " + phase7Message);
    expect(std::any_of(fillQualityAnalysis.findings.begin(),
                       fillQualityAnalysis.findings.end(),
                       [](const tape_phase7::FindingRecord& finding) {
                           return finding.category == "fill_quality_risk" ||
                               finding.category == "adverse_selection_risk" ||
                               finding.category == "market_impact_risk" ||
                               finding.category == "analysis_summary";
                       }),
           "phase 7 fill-quality analyzer should emit fill-quality categories");

    tape_phase7::AnalysisArtifact liquidityBehaviorAnalysis;
    extraProfileCreated = false;
    expect(tape_phase7::runAnalyzerFromBundlePath(caseBundlePath,
                                                  tape_phase7::kLiquidityBehaviorAnalyzerProfile,
                                                  &liquidityBehaviorAnalysis,
                                                  &extraProfileCreated,
                                                  &phase7Code,
                                                  &phase7Message),
           "phase 7 liquidity-behavior analyzer should succeed: " + phase7Code + " " + phase7Message);
    expect(std::any_of(liquidityBehaviorAnalysis.findings.begin(),
                       liquidityBehaviorAnalysis.findings.end(),
                       [](const tape_phase7::FindingRecord& finding) {
                           return finding.category == "display_instability_risk" ||
                               finding.category == "liquidity_refill_behavior" ||
                               finding.category == "pressure_and_thinning" ||
                               finding.category == "analysis_summary";
                       }),
           "phase 7 liquidity-behavior analyzer should emit liquidity categories");

    tape_phase7::AnalysisArtifact adverseSelectionAnalysis;
    extraProfileCreated = false;
    expect(tape_phase7::runAnalyzerFromBundlePath(caseBundlePath,
                                                  tape_phase7::kAdverseSelectionAnalyzerProfile,
                                                  &adverseSelectionAnalysis,
                                                  &extraProfileCreated,
                                                  &phase7Code,
                                                  &phase7Message),
           "phase 7 adverse-selection analyzer should succeed: " + phase7Code + " " + phase7Message);
    expect(std::any_of(adverseSelectionAnalysis.findings.begin(),
                       adverseSelectionAnalysis.findings.end(),
                       [](const tape_phase7::FindingRecord& finding) {
                           return finding.category == "adverse_selection_risk" ||
                               finding.category == "fill_quality_risk" ||
                               finding.category == "analysis_summary";
                       }),
           "phase 7 adverse-selection analyzer should emit adverse-selection categories");

    tape_phase7::AnalysisArtifact orderImpactAnalysis;
    extraProfileCreated = false;
    expect(tape_phase7::runAnalyzerFromBundlePath(caseBundlePath,
                                                  tape_phase7::kOrderImpactAnalyzerProfile,
                                                  &orderImpactAnalysis,
                                                  &extraProfileCreated,
                                                  &phase7Code,
                                                  &phase7Message),
           "phase 7 order-impact analyzer should succeed: " + phase7Code + " " + phase7Message);
    expect(std::any_of(orderImpactAnalysis.findings.begin(),
                       orderImpactAnalysis.findings.end(),
                       [](const tape_phase7::FindingRecord& finding) {
                           return finding.category == "market_impact_risk" ||
                               finding.category == "fill_quality_risk" ||
                               finding.category == "analysis_summary";
                       }),
           "phase 7 order-impact analyzer should emit order-impact categories");

    tape_phase7::PlaybookArtifact adverseSelectionPlaybook;
    bool profilePlaybookCreated = false;
    expect(tape_phase7::buildGuardedPlaybook({},
                                             adverseSelectionAnalysis.analysisArtifact.artifactId,
                                             {},
                                             tape_phase7::kDefaultPlaybookMode,
                                             &adverseSelectionPlaybook,
                                             &profilePlaybookCreated,
                                             &phase7Code,
                                             &phase7Message),
           "phase 7 adverse-selection playbook should succeed: " + phase7Code + " " + phase7Message);
    expect(std::any_of(adverseSelectionPlaybook.plannedActions.begin(),
                       adverseSelectionPlaybook.plannedActions.end(),
                       [](const tape_phase7::PlaybookAction& action) {
                           return action.actionType == "phase7.review_post_fill_adverse_selection.v1" ||
                               action.actionType == "phase7.review_fill_context_before_adverse_selection.v1";
                       }),
           "phase 7 adverse-selection playbook should emit profile-specific review actions");

    tape_phase7::PlaybookArtifact orderImpactPlaybook;
    profilePlaybookCreated = false;
    expect(tape_phase7::buildGuardedPlaybook({},
                                             orderImpactAnalysis.analysisArtifact.artifactId,
                                             {},
                                             tape_phase7::kDefaultPlaybookMode,
                                             &orderImpactPlaybook,
                                             &profilePlaybookCreated,
                                             &phase7Code,
                                             &phase7Message),
           "phase 7 order-impact playbook should succeed: " + phase7Code + " " + phase7Message);
    expect(std::any_of(orderImpactPlaybook.plannedActions.begin(),
                       orderImpactPlaybook.plannedActions.end(),
                       [](const tape_phase7::PlaybookAction& action) {
                           return action.actionType == "phase7.review_order_impact_footprint.v1" ||
                               action.actionType == "phase7.review_fill_context_before_order_impact.v1";
                       }),
           "phase 7 order-impact playbook should emit profile-specific review actions");

    expect(!firstAnalysis.findings.empty(), "phase 7 analyzer should emit findings for the reusable playbook test");
    const std::vector<std::string> findingIds = {firstAnalysis.findings.front().findingId};

    tape_phase7::PlaybookArtifact firstPlaybook;
    bool playbookCreated = false;
    expect(tape_phase7::buildGuardedPlaybook({},
                                             firstAnalysis.analysisArtifact.artifactId,
                                             findingIds,
                                             tape_phase7::kDefaultPlaybookMode,
                                             &firstPlaybook,
                                             &playbookCreated,
                                             &phase7Code,
                                             &phase7Message),
           "phase 7 playbook first run should succeed: " + phase7Code + " " + phase7Message);
    expect(playbookCreated, "phase 7 playbook first run should create a new playbook artifact");

    tape_phase7::PlaybookArtifact secondPlaybook;
    playbookCreated = true;
    expect(tape_phase7::buildGuardedPlaybook({},
                                             firstAnalysis.analysisArtifact.artifactId,
                                             findingIds,
                                             tape_phase7::kDefaultPlaybookMode,
                                             &secondPlaybook,
                                             &playbookCreated,
                                             &phase7Code,
                                             &phase7Message),
           "phase 7 playbook rerun should succeed: " + phase7Code + " " + phase7Message);
    expect(!playbookCreated, "phase 7 playbook rerun should reuse the persisted playbook artifact");
    expect(firstPlaybook.playbookArtifact.artifactId == secondPlaybook.playbookArtifact.artifactId,
           "phase 7 playbook rerun should preserve the deterministic playbook artifact id");
    expect(firstPlaybook.playbookArtifact.manifestPath == secondPlaybook.playbookArtifact.manifestPath,
           "phase 7 playbook rerun should preserve the deterministic playbook manifest path");
    expect(firstPlaybook.generatedAtUtc == secondPlaybook.generatedAtUtc,
           "phase 7 playbook rerun should preserve the generated_at_utc stamp");
    expect(readTextFile(firstPlaybook.playbookArtifact.manifestPath) ==
               readTextFile(secondPlaybook.playbookArtifact.manifestPath),
           "phase 7 playbook rerun should leave the persisted manifest unchanged");

    tape_phase7::ExecutionLedgerArtifact firstLedger;
    bool ledgerCreated = false;
    expect(tape_phase7::buildExecutionLedger({},
                                             firstPlaybook.playbookArtifact.artifactId,
                                             &firstLedger,
                                             &ledgerCreated,
                                             &phase7Code,
                                             &phase7Message),
           "phase 7 execution ledger first run should succeed: " + phase7Code + " " + phase7Message);
    expect(ledgerCreated, "phase 7 execution ledger first run should create a new ledger artifact");
    expect(firstLedger.entries.size() == firstPlaybook.plannedActions.size(),
           "phase 7 execution ledger should preserve one review entry per planned playbook action");
    expect(firstLedger.entries.size() == 1,
           "phase 7 execution ledger should preserve the selected-finding scope for approval-threshold review");
    expect(firstLedger.ledgerStatus == tape_phase7::kDefaultLedgerStatus,
           "phase 7 execution ledger should start in the default deferred review status");

    tape_phase7::ExecutionLedgerArtifact secondLedger;
    ledgerCreated = true;
    expect(tape_phase7::buildExecutionLedger({},
                                             firstPlaybook.playbookArtifact.artifactId,
                                             &secondLedger,
                                             &ledgerCreated,
                                             &phase7Code,
                                             &phase7Message),
           "phase 7 execution ledger rerun should succeed: " + phase7Code + " " + phase7Message);
    expect(!ledgerCreated, "phase 7 execution ledger rerun should reuse the persisted ledger artifact");
    expect(firstLedger.ledgerArtifact.artifactId == secondLedger.ledgerArtifact.artifactId,
           "phase 7 execution ledger rerun should preserve the deterministic ledger artifact id");
    expect(firstLedger.generatedAtUtc == secondLedger.generatedAtUtc,
           "phase 7 execution ledger rerun should preserve the generated_at_utc stamp");
    expect(readTextFile(firstLedger.ledgerArtifact.manifestPath) ==
               readTextFile(secondLedger.ledgerArtifact.manifestPath),
           "phase 7 execution ledger rerun should leave the persisted manifest unchanged");

    tape_phase7::ExecutionLedgerArtifact readLedger;
    expect(tape_phase7::loadExecutionLedgerArtifact({},
                                                    firstLedger.ledgerArtifact.artifactId,
                                                    &readLedger,
                                                    &phase7Code,
                                                    &phase7Message),
           "phase 7 execution ledger read should succeed: " + phase7Code + " " + phase7Message);
    expect(readLedger.playbookArtifact.artifactId == firstPlaybook.playbookArtifact.artifactId,
           "phase 7 execution ledger read should preserve the linked playbook artifact id");
    expect(readLedger.sourceArtifact.artifactId == firstAnalysis.sourceArtifact.artifactId,
           "phase 7 execution ledger read should preserve the linked source artifact id");

    std::vector<std::string> reviewedEntryIds = {firstLedger.entries.front().entryId};
    tape_phase7::ExecutionLedgerArtifact invalidPolicyLedger;
    std::vector<std::string> invalidPolicyUpdatedEntryIds;
    std::string invalidPolicyAuditEventId;
    expect(!tape_phase7::recordExecutionLedgerReview({},
                                                     firstLedger.ledgerArtifact.artifactId,
                                                     reviewedEntryIds,
                                                     tape_phase7::kLedgerEntryStatusApproved,
                                                     "",
                                                     "Missing actor should fail.",
                                                     &invalidPolicyLedger,
                                                     &invalidPolicyUpdatedEntryIds,
                                                     &invalidPolicyAuditEventId,
                                                     &phase7Code,
                                                     &phase7Message),
           "phase 7 execution ledger review should require a reviewer identity");
    expect(phase7Code == "invalid_arguments" &&
               phase7Message.find("actor is required") != std::string::npos,
           "phase 7 execution ledger review should surface the actor-required policy");
    expect(!tape_phase7::recordExecutionLedgerReview({},
                                                     firstLedger.ledgerArtifact.artifactId,
                                                     reviewedEntryIds,
                                                     tape_phase7::kLedgerEntryStatusBlocked,
                                                     "reviewer-a",
                                                     "",
                                                     &invalidPolicyLedger,
                                                     &invalidPolicyUpdatedEntryIds,
                                                     &invalidPolicyAuditEventId,
                                                     &phase7Code,
                                                     &phase7Message),
           "phase 7 execution ledger review should require a comment for blocked status");
    expect(phase7Code == "invalid_arguments" &&
               phase7Message.find("comment is required") != std::string::npos,
           "phase 7 execution ledger review should surface the comment-required policy");

    tape_phase7::ExecutionLedgerArtifact reviewedLedger;
    std::vector<std::string> updatedEntryIds;
    std::string auditEventId;
    expect(tape_phase7::recordExecutionLedgerReview({},
                                                    firstLedger.ledgerArtifact.artifactId,
                                                    reviewedEntryIds,
                                                    tape_phase7::kLedgerEntryStatusApproved,
                                                    "reviewer-a",
                                                    "First approval for deterministic artifact test.",
                                                    &reviewedLedger,
                                                    &updatedEntryIds,
                                                    &auditEventId,
                                                    &phase7Code,
                                                    &phase7Message),
           "phase 7 execution ledger review should succeed: " + phase7Code + " " + phase7Message);
    expect(updatedEntryIds == reviewedEntryIds,
           "phase 7 execution ledger review should report the updated entry ids");
    expect(!auditEventId.empty(),
           "phase 7 execution ledger review should return a deterministic audit event id");
    expect(reviewedLedger.ledgerStatus == tape_phase7::kLedgerStatusWaitingApproval,
           "phase 7 execution ledger review should wait for the required second approval");
    expect(reviewedLedger.auditTrail.is_array() && reviewedLedger.auditTrail.size() == 2,
           "phase 7 execution ledger review should append a review audit event");
    expect(reviewedLedger.auditTrail.back().value("event_type", std::string()) == "review_status_recorded",
           "phase 7 execution ledger review should append the expected audit event type");
    expect(reviewedLedger.auditTrail.back().value("actor", std::string()) == "reviewer-a",
           "phase 7 execution ledger review should preserve the reviewing actor");
    expect(reviewedLedger.auditTrail.back().value("review_status", std::string()) ==
               tape_phase7::kLedgerEntryStatusApproved,
           "phase 7 execution ledger review should preserve the recorded review status");
    expect(reviewedLedger.entries.front().reviewStatus == tape_phase7::kLedgerEntryStatusApproved,
           "phase 7 execution ledger review should update the entry review status");
    expect(reviewedLedger.entries.front().reviewedBy == "reviewer-a",
           "phase 7 execution ledger review should preserve the reviewing actor on the entry");
    expect(!reviewedLedger.entries.front().reviewedAtUtc.empty(),
           "phase 7 execution ledger review should stamp reviewed_at_utc");
    expect(reviewedLedger.entries.front().reviewComment == "First approval for deterministic artifact test.",
           "phase 7 execution ledger review should preserve the review comment");
    expect(reviewedLedger.entries.front().approvalReviewerCount == 1 &&
               !reviewedLedger.entries.front().approvalThresholdMet,
           "phase 7 execution ledger review should track the first distinct approver without marking the entry ready");
    const auto reviewedLedgerSummary = tape_phase7::summarizeExecutionLedgerReviewSummary(reviewedLedger);
    expect(reviewedLedgerSummary.waitingApprovalCount == 1 &&
               reviewedLedgerSummary.readyEntryCount == 0 &&
               reviewedLedgerSummary.distinctReviewerCount == 1 &&
               reviewedLedgerSummary.requiredApprovalCount == 2 &&
               !reviewedLedgerSummary.readyForExecution,
           "phase 7 execution ledger review summary should expose the waiting-for-approval state");

    tape_phase7::ExecutionLedgerArtifact readyLedger;
    std::vector<std::string> readyUpdatedEntryIds;
    std::string readyAuditEventId;
    expect(tape_phase7::recordExecutionLedgerReview({},
                                                    firstLedger.ledgerArtifact.artifactId,
                                                    reviewedEntryIds,
                                                    tape_phase7::kLedgerEntryStatusApproved,
                                                    "reviewer-b",
                                                    "Second approval completes the review threshold.",
                                                    &readyLedger,
                                                    &readyUpdatedEntryIds,
                                                    &readyAuditEventId,
                                                    &phase7Code,
                                                    &phase7Message),
           "phase 7 execution ledger second approval should succeed: " + phase7Code + " " + phase7Message);
    expect(readyUpdatedEntryIds == reviewedEntryIds && !readyAuditEventId.empty(),
           "phase 7 execution ledger second approval should update the same entry and append an audit event");
    expect(readyLedger.ledgerStatus == tape_phase7::kLedgerStatusReadyForExecution,
           "phase 7 execution ledger second approval should mark the ledger ready for execution");
    expect(readyLedger.entries.front().approvalReviewerCount == 2 &&
               readyLedger.entries.front().approvalThresholdMet,
           "phase 7 execution ledger second approval should satisfy the distinct-approval threshold");
    const auto readyLedgerSummary = tape_phase7::summarizeExecutionLedgerReviewSummary(readyLedger);
    expect(readyLedgerSummary.waitingApprovalCount == 0 &&
               readyLedgerSummary.readyEntryCount == 1 &&
               readyLedgerSummary.distinctReviewerCount >= 2 &&
               readyLedgerSummary.readyForExecution,
           "phase 7 execution ledger second approval should expose the ready-for-execution summary state");

    tape_phase7::ExecutionJournalArtifact invalidJournal;
    bool journalCreated = false;
    expect(!tape_phase7::startExecutionJournal({},
                                               firstLedger.ledgerArtifact.artifactId,
                                               "",
                                               "phase7.execution_operator.v1",
                                               &invalidJournal,
                                               &journalCreated,
                                               &phase7Code,
                                               &phase7Message),
           "phase 7 execution journal start should require an actor");
    expect(phase7Code == "invalid_arguments" &&
               phase7Message.find("actor is required") != std::string::npos,
           "phase 7 execution journal start should surface the actor-required policy");

    tape_phase7::ExecutionJournalArtifact firstJournal;
    journalCreated = false;
    expect(tape_phase7::startExecutionJournal({},
                                              firstLedger.ledgerArtifact.artifactId,
                                              "executor-a",
                                              "phase7.execution_operator.v1",
                                              &firstJournal,
                                              &journalCreated,
                                              &phase7Code,
                                              &phase7Message),
           "phase 7 execution journal first start should succeed: " + phase7Code + " " + phase7Message);
    expect(journalCreated, "phase 7 execution journal first start should create a new journal artifact");
    expect(firstJournal.entries.size() == 1 &&
               firstJournal.entries.front().executionStatus == tape_phase7::kExecutionEntryStatusQueued,
           "phase 7 execution journal should start queued with one executable entry from the ready ledger");
    expect(firstJournal.journalStatus == tape_phase7::kExecutionJournalStatusQueued,
           "phase 7 execution journal should begin in the queued aggregate state");
    expect(firstJournal.initiatedBy == "executor-a" &&
               firstJournal.executionCapability == "phase7.execution_operator.v1",
           "phase 7 execution journal should preserve the initiating actor and capability");

    tape_phase7::ExecutionJournalArtifact secondJournal;
    journalCreated = true;
    expect(tape_phase7::startExecutionJournal({},
                                              firstLedger.ledgerArtifact.artifactId,
                                              "executor-a",
                                              "phase7.execution_operator.v1",
                                              &secondJournal,
                                              &journalCreated,
                                              &phase7Code,
                                              &phase7Message),
           "phase 7 execution journal rerun should succeed: " + phase7Code + " " + phase7Message);
    expect(!journalCreated, "phase 7 execution journal rerun should reuse the persisted journal artifact");
    expect(firstJournal.journalArtifact.artifactId == secondJournal.journalArtifact.artifactId &&
               firstJournal.generatedAtUtc == secondJournal.generatedAtUtc,
           "phase 7 execution journal rerun should preserve deterministic journal identity and timestamp");

    tape_phase7::ExecutionJournalArtifact readJournal;
    expect(tape_phase7::loadExecutionJournalArtifact({},
                                                     firstJournal.journalArtifact.artifactId,
                                                     &readJournal,
                                                     &phase7Code,
                                                     &phase7Message),
           "phase 7 execution journal read should succeed: " + phase7Code + " " + phase7Message);
    expect(readJournal.ledgerArtifact.artifactId == firstLedger.ledgerArtifact.artifactId &&
               readJournal.playbookArtifact.artifactId == firstPlaybook.playbookArtifact.artifactId &&
               readJournal.sourceArtifact.artifactId == firstAnalysis.sourceArtifact.artifactId,
           "phase 7 execution journal read should preserve the linked ledger/playbook/source lineage");

    tape_phase7::ExecutionLedgerArtifact dispatchLedger;
    bool dispatchLedgerCreated = false;
    expect(tape_phase7::buildExecutionLedger({},
                                             orderImpactPlaybook.playbookArtifact.artifactId,
                                             &dispatchLedger,
                                             &dispatchLedgerCreated,
                                             &phase7Code,
                                             &phase7Message),
           "phase 7 dispatch-specific execution ledger build should succeed: " + phase7Code + " " + phase7Message);
    expect(dispatchLedgerCreated,
           "phase 7 dispatch-specific execution ledger build should create a distinct ledger artifact");
    std::vector<std::string> dispatchReviewedEntryIds;
    for (const auto& entry : dispatchLedger.entries) {
        dispatchReviewedEntryIds.push_back(entry.entryId);
    }
    tape_phase7::ExecutionLedgerArtifact dispatchReviewedLedger;
    std::vector<std::string> dispatchReviewedUpdatedEntryIds;
    std::string dispatchReviewedAuditEventId;
    expect(tape_phase7::recordExecutionLedgerReview({},
                                                    dispatchLedger.ledgerArtifact.artifactId,
                                                    dispatchReviewedEntryIds,
                                                    tape_phase7::kLedgerEntryStatusApproved,
                                                    "dispatch-reviewer-a",
                                                    "First approval for the dispatch-specific journal.",
                                                    &dispatchReviewedLedger,
                                                    &dispatchReviewedUpdatedEntryIds,
                                                    &dispatchReviewedAuditEventId,
                                                    &phase7Code,
                                                    &phase7Message),
           "phase 7 dispatch-specific first execution-ledger approval should succeed: " + phase7Code + " " + phase7Message);
    tape_phase7::ExecutionLedgerArtifact dispatchReadyLedger;
    std::vector<std::string> dispatchReadyUpdatedEntryIds;
    std::string dispatchReadyAuditEventId;
    expect(tape_phase7::recordExecutionLedgerReview({},
                                                    dispatchLedger.ledgerArtifact.artifactId,
                                                    dispatchReviewedEntryIds,
                                                    tape_phase7::kLedgerEntryStatusApproved,
                                                    "dispatch-reviewer-b",
                                                    "Second approval prepares the dispatch-specific journal.",
                                                    &dispatchReadyLedger,
                                                    &dispatchReadyUpdatedEntryIds,
                                                    &dispatchReadyAuditEventId,
                                                    &phase7Code,
                                                    &phase7Message),
           "phase 7 dispatch-specific second execution-ledger approval should succeed: " + phase7Code + " " + phase7Message);
    expect(dispatchReadyLedger.ledgerStatus == tape_phase7::kLedgerStatusReadyForExecution,
           "phase 7 dispatch-specific execution ledger should become ready for execution");

    tape_phase7::ExecutionJournalArtifact dispatchJournal;
    bool dispatchJournalCreated = false;
    expect(tape_phase7::startExecutionJournal({},
                                              dispatchReadyLedger.ledgerArtifact.artifactId,
                                              "executor-dispatch",
                                              "phase7.execution_operator.v1",
                                              &dispatchJournal,
                                              &dispatchJournalCreated,
                                              &phase7Code,
                                              &phase7Message),
           "phase 7 dispatch-specific execution journal start should succeed: " + phase7Code + " " + phase7Message);
    expect(dispatchJournalCreated && !dispatchJournal.entries.empty(),
           "phase 7 dispatch-specific execution journal should start with queued entries");
    const std::string dispatchRequiredCapability =
        dispatchJournal.executionPolicy.value("capability_required", std::string());
    expect(!dispatchRequiredCapability.empty(),
           "phase 7 dispatch-specific execution journal should expose a required execution capability");
    tape_phase7::ExecutionJournalArtifact invalidDispatchJournal;
    std::vector<std::string> invalidDispatchUpdatedEntryIds;
    std::string invalidDispatchAuditEventId;
    expect(!tape_phase7::dispatchExecutionJournalEntries({},
                                                         dispatchJournal.journalArtifact.artifactId,
                                                         {},
                                                         "executor-dispatch",
                                                         {},
                                                         "Missing capability should fail.",
                                                         &invalidDispatchJournal,
                                                         &invalidDispatchUpdatedEntryIds,
                                                         &invalidDispatchAuditEventId,
                                                         &phase7Code,
                                                         &phase7Message),
           "phase 7 execution journal dispatch should require an execution capability when the policy requires one");
    expect(phase7Code == "invalid_arguments" &&
               phase7Message.find("execution_capability is required") != std::string::npos,
           "phase 7 execution journal dispatch should surface the missing-capability policy");
    expect(!tape_phase7::dispatchExecutionJournalEntries({},
                                                         dispatchJournal.journalArtifact.artifactId,
                                                         {},
                                                         "executor-dispatch",
                                                         dispatchRequiredCapability + ".mismatch",
                                                         "Capability mismatch should fail.",
                                                         &invalidDispatchJournal,
                                                         &invalidDispatchUpdatedEntryIds,
                                                         &invalidDispatchAuditEventId,
                                                         &phase7Code,
                                                         &phase7Message),
           "phase 7 execution journal dispatch should reject mismatched capabilities");
    expect(phase7Code == "capability_mismatch",
           "phase 7 execution journal dispatch should report capability_mismatch for the wrong capability");
    tape_phase7::ExecutionJournalArtifact dispatchedJournal;
    std::vector<std::string> dispatchedJournalEntryIds;
    std::string dispatchedJournalAuditEventId;
    expect(tape_phase7::dispatchExecutionJournalEntries({},
                                                        dispatchJournal.journalArtifact.artifactId,
                                                        {},
                                                        "executor-dispatch",
                                                        dispatchRequiredCapability,
                                                        "Dispatching all queued entries through the controlled path.",
                                                        &dispatchedJournal,
                                                        &dispatchedJournalEntryIds,
                                                        &dispatchedJournalAuditEventId,
                                                        &phase7Code,
                                                        &phase7Message),
           "phase 7 execution journal dispatch should succeed: " + phase7Code + " " + phase7Message);
    expect(!dispatchedJournalEntryIds.empty() &&
               dispatchedJournal.journalStatus == tape_phase7::kExecutionJournalStatusInProgress &&
               std::all_of(dispatchedJournal.entries.begin(),
                           dispatchedJournal.entries.end(),
                           [](const tape_phase7::ExecutionJournalEntry& entry) {
                               return entry.executionStatus == tape_phase7::kExecutionEntryStatusSubmitted;
                           }),
           "phase 7 execution journal dispatch should move queued entries into the submitted in-progress state");
    expect(!tape_phase7::dispatchExecutionJournalEntries({},
                                                         dispatchJournal.journalArtifact.artifactId,
                                                         {},
                                                         "executor-dispatch",
                                                         dispatchRequiredCapability,
                                                         "Dispatching again should fail.",
                                                         &invalidDispatchJournal,
                                                         &invalidDispatchUpdatedEntryIds,
                                                         &invalidDispatchAuditEventId,
                                                         &phase7Code,
                                                         &phase7Message),
           "phase 7 execution journal dispatch should reject journals without queued entries");
    expect(phase7Code == "nothing_dispatchable",
           "phase 7 execution journal dispatch should report nothing_dispatchable once the queued entries are gone");

    const std::vector<std::string> journalEntryIds = {firstJournal.entries.front().journalEntryId};
    tape_phase7::ExecutionJournalArtifact invalidJournalUpdate;
    std::vector<std::string> invalidJournalUpdatedEntryIds;
    std::string invalidJournalAuditEventId;
    expect(!tape_phase7::recordExecutionJournalEvent({},
                                                     firstJournal.journalArtifact.artifactId,
                                                     journalEntryIds,
                                                     tape_phase7::kExecutionEntryStatusSubmitted,
                                                     "",
                                                     "Missing actor should fail.",
                                                     {},
                                                     {},
                                                     &invalidJournalUpdate,
                                                     &invalidJournalUpdatedEntryIds,
                                                     &invalidJournalAuditEventId,
                                                     &phase7Code,
                                                     &phase7Message),
           "phase 7 execution journal event should require an actor");
    expect(phase7Code == "invalid_arguments" &&
               phase7Message.find("actor is required") != std::string::npos,
           "phase 7 execution journal event should surface the actor-required policy");
    expect(!tape_phase7::recordExecutionJournalEvent({},
                                                     firstJournal.journalArtifact.artifactId,
                                                     journalEntryIds,
                                                     tape_phase7::kExecutionEntryStatusFailed,
                                                     "executor-a",
                                                     "Failure details are required.",
                                                     {},
                                                     {},
                                                     &invalidJournalUpdate,
                                                     &invalidJournalUpdatedEntryIds,
                                                     &invalidJournalAuditEventId,
                                                     &phase7Code,
                                                     &phase7Message),
           "phase 7 execution journal failed status should require failure details");
    expect(phase7Code == "invalid_arguments" &&
               phase7Message.find("failure_code or failure_message") != std::string::npos,
           "phase 7 execution journal failed status should surface the failure-detail policy");

    tape_phase7::ExecutionJournalArtifact submittedJournal;
    std::vector<std::string> submittedJournalEntryIds;
    std::string submittedJournalAuditEventId;
    expect(tape_phase7::recordExecutionJournalEvent({},
                                                    firstJournal.journalArtifact.artifactId,
                                                    journalEntryIds,
                                                    tape_phase7::kExecutionEntryStatusSubmitted,
                                                    "executor-a",
                                                    "Submitting the first execution attempt.",
                                                    {},
                                                    {},
                                                    &submittedJournal,
                                                    &submittedJournalEntryIds,
                                                    &submittedJournalAuditEventId,
                                                    &phase7Code,
                                                    &phase7Message),
           "phase 7 execution journal submitted transition should succeed: " + phase7Code + " " + phase7Message);
    expect(submittedJournalEntryIds == journalEntryIds && !submittedJournalAuditEventId.empty(),
           "phase 7 execution journal submitted transition should report updated entry ids and an audit event id");
    expect(submittedJournal.entries.front().executionStatus == tape_phase7::kExecutionEntryStatusSubmitted &&
               submittedJournal.entries.front().attemptCount == 1 &&
               !submittedJournal.entries.front().startedAtUtc.empty(),
           "phase 7 execution journal submitted transition should mark the entry submitted and increment attempts");
    expect(submittedJournal.journalStatus == tape_phase7::kExecutionJournalStatusInProgress,
           "phase 7 execution journal submitted transition should move the aggregate journal in progress");

    tape_phase7::ExecutionJournalArtifact failedJournal;
    std::vector<std::string> failedJournalEntryIds;
    std::string failedJournalAuditEventId;
    expect(tape_phase7::recordExecutionJournalEvent({},
                                                    firstJournal.journalArtifact.artifactId,
                                                    journalEntryIds,
                                                    tape_phase7::kExecutionEntryStatusFailed,
                                                    "executor-a",
                                                    "Attempt failed on venue routing.",
                                                    "venue_reject",
                                                    "Exchange rejected the child order.",
                                                    &failedJournal,
                                                    &failedJournalEntryIds,
                                                    &failedJournalAuditEventId,
                                                    &phase7Code,
                                                    &phase7Message),
           "phase 7 execution journal failed transition should succeed: " + phase7Code + " " + phase7Message);
    expect(failedJournal.entries.front().executionStatus == tape_phase7::kExecutionEntryStatusFailed &&
               failedJournal.entries.front().terminal &&
               !failedJournal.entries.front().completedAtUtc.empty() &&
               failedJournal.entries.front().failureCode == "venue_reject",
           "phase 7 execution journal failed transition should persist terminal failure details");
    expect(failedJournal.journalStatus == tape_phase7::kExecutionJournalStatusFailed,
           "phase 7 execution journal failed transition should move the aggregate journal into failed status");

    tape_phase7::ExecutionJournalArtifact requeuedJournal;
    std::vector<std::string> requeuedJournalEntryIds;
    std::string requeuedJournalAuditEventId;
    expect(tape_phase7::recordExecutionJournalEvent({},
                                                    firstJournal.journalArtifact.artifactId,
                                                    journalEntryIds,
                                                    tape_phase7::kExecutionEntryStatusQueued,
                                                    "executor-b",
                                                    "Requeued after failure triage.",
                                                    {},
                                                    {},
                                                    &requeuedJournal,
                                                    &requeuedJournalEntryIds,
                                                    &requeuedJournalAuditEventId,
                                                    &phase7Code,
                                                    &phase7Message),
           "phase 7 execution journal requeue transition should succeed: " + phase7Code + " " + phase7Message);
    expect(requeuedJournal.entries.front().executionStatus == tape_phase7::kExecutionEntryStatusQueued &&
               !requeuedJournal.entries.front().terminal &&
               requeuedJournal.journalStatus == tape_phase7::kExecutionJournalStatusQueued,
           "phase 7 execution journal requeue transition should reset the entry and aggregate journal to queued");

    tape_phase7::ExecutionJournalArtifact resubmittedJournal;
    std::vector<std::string> resubmittedJournalEntryIds;
    std::string resubmittedJournalAuditEventId;
    expect(tape_phase7::recordExecutionJournalEvent({},
                                                    firstJournal.journalArtifact.artifactId,
                                                    journalEntryIds,
                                                    tape_phase7::kExecutionEntryStatusSubmitted,
                                                    "executor-b",
                                                    "Submitting a second execution attempt.",
                                                    {},
                                                    {},
                                                    &resubmittedJournal,
                                                    &resubmittedJournalEntryIds,
                                                    &resubmittedJournalAuditEventId,
                                                    &phase7Code,
                                                    &phase7Message),
           "phase 7 execution journal resubmit transition should succeed: " + phase7Code + " " + phase7Message);
    expect(resubmittedJournal.entries.front().attemptCount == 2,
           "phase 7 execution journal resubmit transition should increment the attempt counter");

    tape_phase7::ExecutionJournalArtifact succeededJournal;
    std::vector<std::string> succeededJournalEntryIds;
    std::string succeededJournalAuditEventId;
    expect(tape_phase7::recordExecutionJournalEvent({},
                                                    firstJournal.journalArtifact.artifactId,
                                                    journalEntryIds,
                                                    tape_phase7::kExecutionEntryStatusSucceeded,
                                                    "executor-b",
                                                    "Execution completed successfully.",
                                                    {},
                                                    {},
                                                    &succeededJournal,
                                                    &succeededJournalEntryIds,
                                                    &succeededJournalAuditEventId,
                                                    &phase7Code,
                                                    &phase7Message),
           "phase 7 execution journal success transition should succeed: " + phase7Code + " " + phase7Message);
    const auto succeededJournalSummary = tape_phase7::summarizeExecutionJournalSummary(succeededJournal);
    expect(succeededJournal.entries.front().executionStatus == tape_phase7::kExecutionEntryStatusSucceeded &&
               succeededJournal.entries.front().terminal &&
               succeededJournal.journalStatus == tape_phase7::kExecutionJournalStatusSucceeded,
           "phase 7 execution journal success transition should persist terminal success state");
    expect(succeededJournalSummary.succeededCount == 1 &&
               succeededJournalSummary.terminalCount == 1 &&
               succeededJournalSummary.allTerminal,
           "phase 7 execution journal success transition should expose a fully terminal execution summary");

    std::vector<tape_phase7::ExecutionJournalArtifact> executionJournals;
    expect(tape_phase7::listExecutionJournals(0, &executionJournals, &phase7Code, &phase7Message),
           "phase 7 execution journal inventory should succeed: " + phase7Code + " " + phase7Message);
    expect(std::any_of(executionJournals.begin(), executionJournals.end(),
                       [&](const tape_phase7::ExecutionJournalArtifact& artifact) {
                           return artifact.journalArtifact.artifactId == firstJournal.journalArtifact.artifactId &&
                               artifact.journalStatus == tape_phase7::kExecutionJournalStatusSucceeded;
                       }),
           "phase 7 execution journal inventory should include the deterministic journal in its latest lifecycle state");

    tape_phase7::ExecutionApplyArtifact firstApply;
    bool applyCreated = false;
    expect(tape_phase7::startExecutionApply({},
                                            dispatchJournal.journalArtifact.artifactId,
                                            {},
                                            "executor-dispatch",
                                            dispatchRequiredCapability,
                                            "Creating an apply artifact from the dispatched journal.",
                                            &firstApply,
                                            &applyCreated,
                                            &phase7Code,
                                            &phase7Message),
           "phase 7 execution apply first start should succeed: " + phase7Code + " " + phase7Message);
    expect(applyCreated, "phase 7 execution apply first start should create a new apply artifact");
    expect(firstApply.entries.size() == dispatchedJournal.entries.size() &&
               firstApply.applyStatus == tape_phase7::kExecutionJournalStatusInProgress,
           "phase 7 execution apply should mirror the submitted journal entries and aggregate status");
    expect(firstApply.initiatedBy == "executor-dispatch" &&
               firstApply.executionCapability == dispatchRequiredCapability,
           "phase 7 execution apply should preserve the initiating actor and capability");

    tape_phase7::ExecutionApplyArtifact secondApply;
    applyCreated = true;
    expect(tape_phase7::startExecutionApply({},
                                            dispatchJournal.journalArtifact.artifactId,
                                            {},
                                            "executor-dispatch",
                                            dispatchRequiredCapability,
                                            {},
                                            &secondApply,
                                            &applyCreated,
                                            &phase7Code,
                                            &phase7Message),
           "phase 7 execution apply rerun should succeed: " + phase7Code + " " + phase7Message);
    expect(!applyCreated, "phase 7 execution apply rerun should reuse the persisted apply artifact");
    expect(firstApply.applyArtifact.artifactId == secondApply.applyArtifact.artifactId &&
               firstApply.generatedAtUtc == secondApply.generatedAtUtc,
           "phase 7 execution apply rerun should preserve deterministic identity and timestamp");

    tape_phase7::ExecutionApplyArtifact readApply;
    expect(tape_phase7::loadExecutionApplyArtifact({},
                                                   firstApply.applyArtifact.artifactId,
                                                   &readApply,
                                                   &phase7Code,
                                                   &phase7Message),
           "phase 7 execution apply read should succeed: " + phase7Code + " " + phase7Message);
    expect(readApply.journalArtifact.artifactId == dispatchJournal.journalArtifact.artifactId &&
               readApply.ledgerArtifact.artifactId == dispatchReadyLedger.ledgerArtifact.artifactId,
           "phase 7 execution apply read should preserve the linked journal and ledger lineage");

    std::vector<std::string> applyEntryIds = {firstApply.entries.front().applyEntryId};
    tape_phase7::ExecutionApplyArtifact invalidApplyUpdate;
    std::vector<std::string> invalidApplyUpdatedEntryIds;
    std::string invalidApplyAuditEventId;
    expect(!tape_phase7::recordExecutionApplyEvent({},
                                                   firstApply.applyArtifact.artifactId,
                                                   applyEntryIds,
                                                   tape_phase7::kExecutionEntryStatusFailed,
                                                   "executor-dispatch",
                                                   "Failed apply requires details.",
                                                   {},
                                                   {},
                                                   &invalidApplyUpdate,
                                                   &invalidApplyUpdatedEntryIds,
                                                   &invalidApplyAuditEventId,
                                                   &phase7Code,
                                                   &phase7Message),
           "phase 7 execution apply failed status should require failure details");
    expect(phase7Code == "invalid_arguments" &&
               phase7Message.find("failure_code or failure_message") != std::string::npos,
           "phase 7 execution apply failed status should surface the failure-detail policy");

    tape_phase7::ExecutionApplyArtifact succeededApply;
    std::vector<std::string> succeededApplyEntryIds;
    std::string succeededApplyAuditEventId;
    expect(tape_phase7::recordExecutionApplyEvent({},
                                                  firstApply.applyArtifact.artifactId,
                                                  applyEntryIds,
                                                  tape_phase7::kExecutionEntryStatusSucceeded,
                                                  "executor-dispatch",
                                                  "Apply completed successfully.",
                                                  {},
                                                  {},
                                                  &succeededApply,
                                                  &succeededApplyEntryIds,
                                                  &succeededApplyAuditEventId,
                                                  &phase7Code,
                                                  &phase7Message),
           "phase 7 execution apply success transition should succeed: " + phase7Code + " " + phase7Message);
    expect(succeededApplyEntryIds == applyEntryIds && !succeededApplyAuditEventId.empty(),
           "phase 7 execution apply success transition should report updated entry ids and an audit event id");
    const auto succeededApplySummary = tape_phase7::summarizeExecutionApplySummary(succeededApply);
    expect(succeededApply.entries.front().executionStatus == tape_phase7::kExecutionEntryStatusSucceeded &&
               succeededApply.entries.front().terminal &&
               succeededApply.applyStatus == tape_phase7::kExecutionJournalStatusSucceeded,
           "phase 7 execution apply success transition should persist terminal success state");
    expect(succeededApplySummary.succeededCount == 1 &&
               succeededApplySummary.terminalCount == 1 &&
               succeededApplySummary.allTerminal,
           "phase 7 execution apply success transition should expose a fully terminal execution summary");

    std::vector<tape_phase7::ExecutionApplyArtifact> executionApplies;
    expect(tape_phase7::listExecutionApplies(0, &executionApplies, &phase7Code, &phase7Message),
           "phase 7 execution apply inventory should succeed: " + phase7Code + " " + phase7Message);
    expect(std::any_of(executionApplies.begin(), executionApplies.end(),
                       [&](const tape_phase7::ExecutionApplyArtifact& artifact) {
                           return artifact.applyArtifact.artifactId == firstApply.applyArtifact.artifactId &&
                               artifact.applyStatus == tape_phase7::kExecutionJournalStatusSucceeded;
                       }),
           "phase 7 execution apply inventory should include the deterministic apply artifact in its latest lifecycle state");

    tape_phase7::ExecutionJournalArtifact rereadDispatchJournal;
    expect(tape_phase7::loadExecutionJournalArtifact({},
                                                     dispatchJournal.journalArtifact.artifactId,
                                                     &rereadDispatchJournal,
                                                     &phase7Code,
                                                     &phase7Message),
           "phase 7 execution journal reread after apply should succeed: " + phase7Code + " " + phase7Message);
    expect(rereadDispatchJournal.journalStatus == tape_phase7::kExecutionJournalStatusSucceeded &&
               rereadDispatchJournal.entries.front().executionStatus == tape_phase7::kExecutionEntryStatusSucceeded,
           "phase 7 execution apply updates should synchronize the linked execution journal");

    tape_phase7::ExecutionLedgerArtifact rereadReviewedLedger;
    expect(tape_phase7::loadExecutionLedgerArtifact({},
                                                    firstLedger.ledgerArtifact.artifactId,
                                                    &rereadReviewedLedger,
                                                    &phase7Code,
                                                    &phase7Message),
           "phase 7 reviewed execution ledger read should succeed: " + phase7Code + " " + phase7Message);
    expect(rereadReviewedLedger.entries.front().reviewStatus == tape_phase7::kLedgerEntryStatusApproved,
           "phase 7 reviewed execution ledger read should persist the entry review status");
    expect(rereadReviewedLedger.ledgerStatus == tape_phase7::kLedgerStatusReadyForExecution,
           "phase 7 reviewed execution ledger read should persist the aggregate review status");
    expect(rereadReviewedLedger.entries.front().approvalReviewerCount == 2 &&
               rereadReviewedLedger.entries.front().approvalThresholdMet,
           "phase 7 reviewed execution ledger read should persist the approval threshold state");

    tape_phase7::ExecutionLedgerArtifact invalidReviewLedger;
    std::vector<std::string> invalidUpdatedEntryIds;
    std::string invalidAuditEventId;
    expect(!tape_phase7::recordExecutionLedgerReview({},
                                                     firstLedger.ledgerArtifact.artifactId,
                                                     {"phase7-ledger-entry-missing"},
                                                     tape_phase7::kLedgerEntryStatusBlocked,
                                                     "test-reviewer",
                                                     "Missing entry should fail.",
                                                     &invalidReviewLedger,
                                                     &invalidUpdatedEntryIds,
                                                     &invalidAuditEventId,
                                                     &phase7Code,
                                                     &phase7Message),
           "phase 7 execution ledger review should reject unknown entry ids");
    expect(phase7Code == "entry_not_found",
           "phase 7 execution ledger review should surface entry_not_found for unknown entry ids");

    std::vector<tape_phase7::AnalysisArtifact> analysisArtifacts;
    expect(tape_phase7::listAnalysisArtifacts(0, &analysisArtifacts, &phase7Code, &phase7Message),
           "phase 7 analysis inventory should succeed: " + phase7Code + " " + phase7Message);
    expect(std::any_of(analysisArtifacts.begin(), analysisArtifacts.end(),
                       [&](const tape_phase7::AnalysisArtifact& artifact) {
                           return artifact.analysisArtifact.artifactId == firstAnalysis.analysisArtifact.artifactId;
                       }),
           "phase 7 analysis inventory should include the deterministic reused analysis artifact");

    std::vector<tape_phase7::PlaybookArtifact> playbookArtifacts;
    expect(tape_phase7::listPlaybookArtifacts(0, &playbookArtifacts, &phase7Code, &phase7Message),
           "phase 7 playbook inventory should succeed: " + phase7Code + " " + phase7Message);
    expect(std::any_of(playbookArtifacts.begin(), playbookArtifacts.end(),
                       [&](const tape_phase7::PlaybookArtifact& artifact) {
                           return artifact.playbookArtifact.artifactId == firstPlaybook.playbookArtifact.artifactId;
                       }),
           "phase 7 playbook inventory should include the deterministic reused playbook artifact");

    std::vector<tape_phase7::ExecutionLedgerArtifact> executionLedgers;
    expect(tape_phase7::listExecutionLedgers(0, &executionLedgers, &phase7Code, &phase7Message),
           "phase 7 execution ledger inventory should succeed: " + phase7Code + " " + phase7Message);
    expect(std::any_of(executionLedgers.begin(), executionLedgers.end(),
                       [&](const tape_phase7::ExecutionLedgerArtifact& artifact) {
                           return artifact.ledgerArtifact.artifactId == firstLedger.ledgerArtifact.artifactId;
                       }),
           "phase 7 execution ledger inventory should include the deterministic reused ledger artifact");

    server.stop();
}

void testTapePhase7RuntimeBridgeDispatchesAndReconcilesOrderAnchoredJournal() {
    clearTestFiles();
    std::error_code phase7Ec;
    fs::remove_all(testDataDir() / "phase7_artifacts", phase7Ec);

    SharedData owner;
    bindSharedDataOwner(&owner);

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);

        OrderInfo order;
        order.orderId = 9101;
        order.account = "U23154741";
        order.symbol = "INTC";
        order.side = "BUY";
        order.quantity = 10.0;
        order.remainingQty = 10.0;
        order.limitPrice = 45.21;
        order.status = "Submitted";
        order.localState = LocalOrderState::Working;
        g_data.orders[order.orderId] = order;

        TradeTrace trace;
        trace.traceId = 99101;
        trace.orderId = order.orderId;
        trace.permId = 5101;
        trace.source = "phase7-runtime-bridge";
        trace.symbol = order.symbol;
        trace.side = order.side;
        trace.account = order.account;
        trace.requestedQty = static_cast<int>(order.quantity);
        trace.limitPrice = order.limitPrice;
        g_data.traces[trace.traceId] = trace;
        g_data.traceRecency.push_back(trace.traceId);
        g_data.latestTraceId = trace.traceId;
    }
    publishSharedDataSnapshot();

    const fs::path rootDir = testDataDir() / "tape-engine-phase7-runtime-bridge";
    const fs::path socketPath = testDataDir() / "tape-engine-phase7-runtime-bridge.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "ib:conid:9501:STK:SMART:USD:INTC";
    config.ringCapacity = 32;

    tape_engine::Server server(config);
    std::string startError;
    expect(server.start(&startError), "tape-engine should start for the phase 7 runtime bridge test: " + startError);

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-engine-phase7-runtime-bridge";
    options.runtimeSessionId = "runtime-engine-phase7-runtime-bridge";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;

    BridgeOutboxRecord orderIntent = makeBridgeRecord(9601, "order_intent", "WebSocket", "INTC", "BUY",
                                                      4101, 9101, 5101, "", "phase 7 runtime bridge order intent",
                                                      "2026-03-16T10:12:00.100");
    orderIntent.instrumentId = "ib:conid:9501:STK:SMART:USD:INTC";

    BridgeOutboxRecord orderStatus = makeBridgeRecord(9602, "order_status", "BrokerOrderStatus", "INTC", "BUY",
                                                      4101, 9101, 5101, "",
                                                      "phase 7 runtime bridge submitted",
                                                      "2026-03-16T10:12:00.200");
    orderStatus.instrumentId = "ib:conid:9501:STK:SMART:USD:INTC";

    BridgeOutboxRecord fillExecution = makeBridgeRecord(9603, "fill_execution", "BrokerExecution", "INTC", "BUY",
                                                        4101, 9101, 5101, "E-PHASE7-BRIDGE-1",
                                                        "phase 7 runtime bridge fill",
                                                        "2026-03-16T10:12:00.300");
    fillExecution.instrumentId = "ib:conid:9501:STK:SMART:USD:INTC";

    options.batchSeq = 601;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({orderIntent}, options)), &error),
           "phase 7 runtime bridge fixture should accept the order intent batch: " + error);
    options.batchSeq = 602;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({orderStatus}, options)), &error),
           "phase 7 runtime bridge fixture should accept the order status batch: " + error);
    options.batchSeq = 603;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({fillExecution}, options)), &error),
           "phase 7 runtime bridge fixture should accept the fill batch: " + error);

    waitUntil([&]() {
        const auto snapshot = server.snapshot();
        return snapshot.latestFrozenRevisionId >= 3 && snapshot.segments.size() >= 3;
    }, "phase 7 runtime bridge fixture should freeze the seeded batches");

    tape_engine::Client client(socketPath.string());
    tape_engine::QueryResponse response;

    tape_engine::QueryRequest caseReportRequest;
    caseReportRequest.requestId = "phase7-runtime-bridge-case-report";
    caseReportRequest.operation = "scan_order_case_report";
    caseReportRequest.orderId = 9101;
    expect(client.query(caseReportRequest, &response, &error),
           "phase 7 runtime bridge case report scan should succeed: " + error);
    const std::uint64_t caseReportId =
        response.summary.value("case_report_artifact", json::object()).value("report_id", 0ULL);
    expect(caseReportId > 0, "phase 7 runtime bridge case report scan should persist a durable case report");

    tape_engine::QueryRequest exportCaseBundleRequest;
    exportCaseBundleRequest.requestId = "phase7-runtime-bridge-export-case-bundle";
    exportCaseBundleRequest.operation = "export_case_bundle";
    exportCaseBundleRequest.reportId = caseReportId;
    expect(client.query(exportCaseBundleRequest, &response, &error),
           "phase 7 runtime bridge case bundle export should succeed: " + error);
    const std::string caseBundlePath =
        response.summary.value("bundle", json::object()).value("bundle_path", std::string());
    expect(!caseBundlePath.empty() && fs::exists(caseBundlePath),
           "phase 7 runtime bridge case bundle export should write a portable case bundle");

    tape_phase7::AnalysisArtifact orderImpactAnalysis;
    bool analysisCreated = false;
    std::string phase7Code;
    std::string phase7Message;
    expect(tape_phase7::runAnalyzerFromBundlePath(caseBundlePath,
                                                  tape_phase7::kOrderImpactAnalyzerProfile,
                                                  &orderImpactAnalysis,
                                                  &analysisCreated,
                                                  &phase7Code,
                                                  &phase7Message),
           "phase 7 runtime bridge order-impact analyzer should succeed: " + phase7Code + " " + phase7Message);
    expect(!orderImpactAnalysis.findings.empty(),
           "phase 7 runtime bridge analysis should emit findings");

    const auto dispatchableFindingIt = std::find_if(orderImpactAnalysis.findings.begin(),
                                                    orderImpactAnalysis.findings.end(),
                                                    [](const tape_phase7::FindingRecord& finding) {
                                                        return finding.category == "market_impact_risk" ||
                                                            finding.category == "fill_quality_risk";
                                                    });
    expect(dispatchableFindingIt != orderImpactAnalysis.findings.end(),
           "phase 7 runtime bridge analysis should emit at least one runtime-dispatchable finding");

    tape_phase7::PlaybookArtifact playbook;
    bool playbookCreated = false;
    expect(tape_phase7::buildGuardedPlaybook({},
                                             orderImpactAnalysis.analysisArtifact.artifactId,
                                             {},
                                             tape_phase7::kDefaultPlaybookMode,
                                             &playbook,
                                             &playbookCreated,
                                             &phase7Code,
                                             &phase7Message),
           "phase 7 runtime bridge playbook build should succeed: " + phase7Code + " " + phase7Message);
    expect(!playbook.plannedActions.empty(),
           "phase 7 runtime bridge playbook should expose planned actions for the order-impact review profile");
    const auto dispatchableActionIt = std::find_if(playbook.plannedActions.begin(),
                                                   playbook.plannedActions.end(),
                                                   [](const tape_phase7::PlaybookAction& action) {
                                                       return action.executionRequest.is_object() &&
                                                           !action.executionRequest.empty();
                                                   });
    expect(dispatchableActionIt != playbook.plannedActions.end(),
           "phase 7 runtime bridge playbook should include a runtime-dispatchable action");
    expect(dispatchableActionIt->executionRequest.value("operation", std::string()) ==
               "request_order_reconciliation" &&
               dispatchableActionIt->executionRequest.value("executor", std::string()) ==
                   "trading_runtime",
           "phase 7 runtime bridge playbook should carry the executable reconciliation request");
    expect(dispatchableActionIt->executionRequest.value("requested_order_ids", json::array()).size() == 1 &&
               dispatchableActionIt->executionRequest.value("requested_order_ids", json::array()).front().get<long long>() == 9101,
           "phase 7 runtime bridge playbook should target the anchored order id");

    tape_phase7::ExecutionLedgerArtifact ledger;
    bool ledgerCreated = false;
    expect(tape_phase7::buildExecutionLedger({},
                                             playbook.playbookArtifact.artifactId,
                                             &ledger,
                                             &ledgerCreated,
                                             &phase7Code,
                                             &phase7Message),
           "phase 7 runtime bridge execution ledger build should succeed: " + phase7Code + " " + phase7Message);
    expect(ledgerCreated && !ledger.entries.empty(),
           "phase 7 runtime bridge execution ledger should preserve the playbook entries");
    const auto dispatchableLedgerEntryIt = std::find_if(ledger.entries.begin(),
                                                        ledger.entries.end(),
                                                        [&](const tape_phase7::ExecutionLedgerEntry& entry) {
                                                            return entry.actionId == dispatchableActionIt->actionId &&
                                                                entry.executionRequest.is_object() &&
                                                                !entry.executionRequest.empty();
                                                        });
    expect(dispatchableLedgerEntryIt != ledger.entries.end(),
           "phase 7 runtime bridge execution ledger should preserve the dispatchable action entry");

    const std::vector<std::string> approvedLedgerEntryIds = {dispatchableLedgerEntryIt->entryId};
    std::vector<std::string> nonApplicableLedgerEntryIds;
    for (const auto& entry : ledger.entries) {
        if (entry.entryId != dispatchableLedgerEntryIt->entryId) {
            nonApplicableLedgerEntryIds.push_back(entry.entryId);
        }
    }
    tape_phase7::ExecutionLedgerArtifact reviewedLedger;
    std::vector<std::string> reviewedLedgerEntryIds;
    std::string reviewedLedgerAuditEventId;
    expect(tape_phase7::recordExecutionLedgerReview({},
                                                    ledger.ledgerArtifact.artifactId,
                                                    approvedLedgerEntryIds,
                                                    tape_phase7::kLedgerEntryStatusApproved,
                                                    "bridge-reviewer-a",
                                                    "First approval for the runtime-bridge journal.",
                                                    &reviewedLedger,
                                                    &reviewedLedgerEntryIds,
                                                    &reviewedLedgerAuditEventId,
                                                    &phase7Code,
                                                    &phase7Message),
           "phase 7 runtime bridge first ledger approval should succeed: " + phase7Code + " " + phase7Message);
    expect(reviewedLedgerEntryIds == approvedLedgerEntryIds,
           "phase 7 runtime bridge first ledger approval should update the dispatchable ledger entry");

    if (!nonApplicableLedgerEntryIds.empty()) {
        tape_phase7::ExecutionLedgerArtifact narrowedLedger;
        std::vector<std::string> narrowedLedgerEntryIds;
        std::string narrowedLedgerAuditEventId;
        expect(tape_phase7::recordExecutionLedgerReview({},
                                                        ledger.ledgerArtifact.artifactId,
                                                        nonApplicableLedgerEntryIds,
                                                        tape_phase7::kLedgerEntryStatusNotApplicable,
                                                        "bridge-reviewer-a",
                                                        "Non-runtime actions stay outside the execution journal bridge.",
                                                        &narrowedLedger,
                                                        &narrowedLedgerEntryIds,
                                                        &narrowedLedgerAuditEventId,
                                                        &phase7Code,
                                                        &phase7Message),
               "phase 7 runtime bridge non-applicable ledger narrowing should succeed: " +
                   phase7Code + " " + phase7Message);
        expect(narrowedLedgerEntryIds == nonApplicableLedgerEntryIds,
               "phase 7 runtime bridge non-applicable ledger narrowing should update the non-runtime entries");
    }

    tape_phase7::ExecutionLedgerArtifact readyLedger;
    std::vector<std::string> readyLedgerEntryIds;
    std::string readyLedgerAuditEventId;
    expect(tape_phase7::recordExecutionLedgerReview({},
                                                    ledger.ledgerArtifact.artifactId,
                                                    approvedLedgerEntryIds,
                                                    tape_phase7::kLedgerEntryStatusApproved,
                                                    "bridge-reviewer-b",
                                                    "Second approval makes the journal ready.",
                                                    &readyLedger,
                                                    &readyLedgerEntryIds,
                                                    &readyLedgerAuditEventId,
                                                    &phase7Code,
                                                    &phase7Message),
           "phase 7 runtime bridge second ledger approval should succeed: " + phase7Code + " " + phase7Message);
    expect(readyLedger.ledgerStatus == tape_phase7::kLedgerStatusReadyForExecution,
           "phase 7 runtime bridge execution ledger should become ready for execution");

    tape_phase7::ExecutionJournalArtifact journal;
    bool journalCreated = false;
    expect(tape_phase7::startExecutionJournal({},
                                              readyLedger.ledgerArtifact.artifactId,
                                              "bridge-executor",
                                              "phase7.execution_operator.v1",
                                              &journal,
                                              &journalCreated,
                                              &phase7Code,
                                              &phase7Message),
           "phase 7 runtime bridge journal start should succeed: " + phase7Code + " " + phase7Message);
    expect(journalCreated && journal.entries.size() == 1 &&
               journal.entries.front().executionStatus == tape_phase7::kExecutionEntryStatusQueued,
           "phase 7 runtime bridge journal should start with one queued runtime-dispatchable entry");

    const std::vector<std::string> journalEntryIds = {journal.entries.front().journalEntryId};
    tape_phase7::ExecutionJournalArtifact dispatchedJournal;
    std::vector<std::string> dispatchedJournalEntryIds;
    std::string dispatchedJournalAuditEventId;
    expect(tape_phase7::dispatchExecutionJournalEntriesViaRuntime(nullptr,
                                                                  {},
                                                                  journal.journalArtifact.artifactId,
                                                                  journalEntryIds,
                                                                  "bridge-executor",
                                                                  "phase7.execution_operator.v1",
                                                                  "Dispatch the order-anchored execution journal entry.",
                                                                  &dispatchedJournal,
                                                                  &dispatchedJournalEntryIds,
                                                                  &dispatchedJournalAuditEventId,
                                                                  &phase7Code,
                                                                  &phase7Message),
           "phase 7 runtime bridge journal dispatch should succeed: " + phase7Code + " " + phase7Message);
    expect(dispatchedJournalEntryIds == journalEntryIds && !dispatchedJournalAuditEventId.empty(),
           "phase 7 runtime bridge journal dispatch should report the updated journal entry and audit event");
    expect(dispatchedJournal.entries.front().executionStatus == tape_phase7::kExecutionEntryStatusSubmitted &&
               dispatchedJournal.entries.front().attemptCount == 1 &&
               dispatchedJournal.journalStatus == tape_phase7::kExecutionJournalStatusInProgress,
           "phase 7 runtime bridge journal dispatch should move the entry into submitted in-progress state");
    {
        const auto recoverySummary = tape_phase7::summarizeExecutionJournalRecovery(dispatchedJournal);
        expect(recoverySummary.recoveryRequired &&
                   recoverySummary.runtimeBackedSubmittedCount == 1 &&
                   !recoverySummary.staleRecoveryRequired,
               "phase 7 runtime bridge dispatched journal should advertise pending runtime recovery");
    }
    expect(dispatchedJournal.entries.front().executionRequest.value("operation", std::string()) ==
               "request_order_reconciliation" &&
               dispatchedJournal.entries.front().executionResult.value("runtime_operation", std::string()) ==
                   "request_order_reconciliation" &&
               dispatchedJournal.entries.front().executionResult.value("accepted_order_ids", json::array()).size() == 1 &&
               dispatchedJournal.entries.front().executionResult.value("accepted_order_ids", json::array()).front().get<long long>() == 9101,
           "phase 7 runtime bridge journal dispatch should record runtime reconciliation metadata");

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        const auto it = g_data.orders.find(9101);
        expect(it != g_data.orders.end(), "phase 7 runtime bridge dispatch order should still exist");
        expect(it->second.localState == LocalOrderState::NeedsReconciliation,
               "phase 7 runtime bridge dispatch should push the order into manual reconciliation");
        expect(it->second.lastReconciliationReason == journal.entries.front().actionType,
               "phase 7 runtime bridge dispatch should preserve the playbook action as reconciliation reason");
    }

    tape_phase7::ExecutionApplyArtifact apply;
    bool applyCreated = false;
    expect(tape_phase7::startExecutionApply({},
                                            dispatchedJournal.journalArtifact.artifactId,
                                            {},
                                            "bridge-executor",
                                            "phase7.execution_operator.v1",
                                            "Create an apply artifact before runtime reconciliation completes.",
                                            &apply,
                                            &applyCreated,
                                            &phase7Code,
                                            &phase7Message),
           "phase 7 runtime bridge apply start should succeed: " + phase7Code + " " + phase7Message);
    expect(applyCreated &&
               apply.entries.front().executionStatus == tape_phase7::kExecutionEntryStatusSubmitted &&
               apply.applyStatus == tape_phase7::kExecutionJournalStatusInProgress,
           "phase 7 runtime bridge apply start should mirror the submitted journal state");
    {
        const auto recoverySummary = tape_phase7::summarizeExecutionApplyRecovery(apply);
        expect(recoverySummary.recoveryRequired &&
                   recoverySummary.runtimeBackedSubmittedCount == 1 &&
                   !recoverySummary.staleRecoveryRequired,
               "phase 7 runtime bridge apply start should advertise pending runtime recovery");
    }
    {
        tape_phase7::RuntimeRecoveryBacklogSummary backlog;
        expect(tape_phase7::summarizeRuntimeRecoveryBacklog(&backlog, &phase7Code, &phase7Message),
               "phase 7 runtime bridge backlog summary should succeed after dispatch/apply start: " +
                   phase7Code + " " + phase7Message);
        expect(backlog.journalArtifactCount >= 1 &&
                   backlog.applyArtifactCount >= 1 &&
                   backlog.recoveryRequiredJournalCount >= 1 &&
                   backlog.recoveryRequiredApplyCount >= 1 &&
                   backlog.runtimeBackedSubmittedJournalEntryCount >= 1 &&
                   backlog.runtimeBackedSubmittedApplyEntryCount >= 1 &&
                   backlog.recoveryRequired &&
                   !backlog.staleRecoveryRequired,
               "phase 7 runtime bridge backlog summary should report pending journal/apply recovery after dispatch");
        const auto noActorPlan = tape_phase7::runtimeStartupRecoveryPlan(backlog, false);
        expect(noActorPlan.startupAction == "await_actor" &&
                   !noActorPlan.recoverySweepRecommended &&
                   !noActorPlan.manualAttentionRecommended,
               "phase 7 runtime bridge startup policy should wait for an actor when recovery is pending");
        const auto actorPlan = tape_phase7::runtimeStartupRecoveryPlan(backlog, true);
        expect(actorPlan.startupAction == "recover_pending" &&
                   actorPlan.recoverySweepRecommended &&
                   !actorPlan.manualAttentionRecommended,
               "phase 7 runtime bridge startup policy should recommend recovery when an actor is present");
    }

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        auto& order = g_data.orders[9101];
        order.status = "Submitted";
        order.filledQty = 10.0;
        order.remainingQty = 20.0;
        order.avgFillPrice = 46.11;
        order.localState = LocalOrderState::PartiallyFilled;
        order.seenExecIds.insert("E-PHASE7-BRIDGE-1");
    }
    publishSharedDataSnapshot();

    tape_phase7::ExecutionJournalArtifact partiallyFilledJournal;
    std::vector<std::string> partiallyFilledJournalEntryIds;
    std::string partiallyFilledJournalAuditEventId;
    expect(tape_phase7::reconcileExecutionJournalEntriesViaRuntime(nullptr,
                                                                   {},
                                                                   journal.journalArtifact.artifactId,
                                                                   journalEntryIds,
                                                                   "bridge-executor",
                                                                   "Runtime reports the order as partially filled.",
                                                                   &partiallyFilledJournal,
                                                                   &partiallyFilledJournalEntryIds,
                                                                   &partiallyFilledJournalAuditEventId,
                                                                   &phase7Code,
                                                                   &phase7Message),
           "phase 7 runtime bridge partial-fill reconciliation should succeed: " +
               phase7Code + " " + phase7Message);
    expect(partiallyFilledJournalEntryIds == journalEntryIds && !partiallyFilledJournalAuditEventId.empty(),
           "phase 7 runtime bridge partial-fill reconciliation should report the updated entry and audit event");
    expect(partiallyFilledJournal.entries.front().executionStatus == tape_phase7::kExecutionEntryStatusSubmitted &&
               !partiallyFilledJournal.entries.front().terminal &&
               partiallyFilledJournal.journalStatus == tape_phase7::kExecutionJournalStatusInProgress &&
               partiallyFilledJournal.entries.front().executionResult.value("resolution", std::string()) ==
                   "resolved_partially_filled" &&
               partiallyFilledJournal.entries.front().executionResult.value("fill_state", std::string()) ==
                   "partial_fill_in_progress" &&
               partiallyFilledJournal.entries.front().executionResult.value("restart_resume_policy", std::string()) ==
                   "continue_recovery" &&
               partiallyFilledJournal.entries.front().executionResult.value("restart_recovery_state", std::string()) ==
                   "recoverable" &&
               partiallyFilledJournal.entries.front().executionResult.value("restart_recovery_reason", std::string()) ==
                   "partial_fill_active" &&
               partiallyFilledJournal.entries.front().executionResult.value("filled_qty", 0.0) == 10.0 &&
               partiallyFilledJournal.entries.front().executionResult.value("remaining_qty", 0.0) == 20.0 &&
               partiallyFilledJournal.entries.front().executionResult.value("avg_fill_price", 0.0) == 46.11,
           "phase 7 runtime bridge partial-fill reconciliation should keep the journal nonterminal while recording fill state");
    expect(partiallyFilledJournal.entries.front().executionResult.value("broker_identity", json::object()).value("perm_id", 0LL) == 5101 &&
               partiallyFilledJournal.entries.front().executionResult.value("broker_identity", json::object()).value("trace_id", 0ULL) != 0 &&
               partiallyFilledJournal.entries.front().executionResult.value("trade_trace", json::object()).value("trace_id", 0ULL) != 0,
           "phase 7 runtime bridge partial-fill reconciliation should preserve broker and trace linkage");
    {
        const json execIds = partiallyFilledJournal.entries.front().executionResult.value("broker_identity", json::object())
                                  .value("exec_ids", json::array());
        expect(execIds.is_array() &&
                   std::find(execIds.begin(), execIds.end(), json("E-PHASE7-BRIDGE-1")) != execIds.end(),
               "phase 7 runtime bridge partial-fill reconciliation should preserve execution ids");
    }
    {
        const auto recoverySummary = tape_phase7::summarizeExecutionJournalRecovery(partiallyFilledJournal);
        expect(recoverySummary.recoveryRequired &&
                   recoverySummary.runtimeBackedSubmittedCount == 1 &&
                   !recoverySummary.staleRecoveryRequired,
               "phase 7 runtime bridge partial-fill reconciliation should keep runtime recovery active without becoming stale");
    }

    tape_phase7::ExecutionJournalArtifact stalePartialJournal;
    expect(tape_phase7::loadExecutionJournalArtifact({},
                                                     journal.journalArtifact.artifactId,
                                                     &stalePartialJournal,
                                                     &phase7Code,
                                                     &phase7Message),
           "phase 7 runtime bridge should reload the partially-filled journal before stale refresh: " +
               phase7Code + " " + phase7Message);
    stalePartialJournal.entries.front().startedAtUtc = "2026-03-16T00:00:00.000Z";
    stalePartialJournal.entries.front().lastUpdatedAtUtc = "2026-03-16T00:00:00.000Z";
    expect(tape_phase7::persistExecutionJournalArtifact(stalePartialJournal, &phase7Code, &phase7Message),
           "phase 7 runtime bridge should persist the stale partially-filled journal: " +
               phase7Code + " " + phase7Message);

    tape_phase7::ExecutionJournalArtifact resumedJournal;
    std::vector<std::string> resumedJournalEntryIds;
    std::string resumedJournalAuditEventId;
    expect(tape_phase7::reconcileExecutionJournalEntriesViaRuntime(nullptr,
                                                                   {},
                                                                   journal.journalArtifact.artifactId,
                                                                   journalEntryIds,
                                                                   "bridge-executor",
                                                                   "Refresh restart-resume state from a live partial fill.",
                                                                   &resumedJournal,
                                                                   &resumedJournalEntryIds,
                                                                   &resumedJournalAuditEventId,
                                                                   &phase7Code,
                                                                   &phase7Message),
           "phase 7 runtime bridge restart-resume reconciliation should succeed: " +
               phase7Code + " " + phase7Message);
    expect(resumedJournal.entries.front().executionStatus == tape_phase7::kExecutionEntryStatusSubmitted &&
               !resumedJournal.entries.front().terminal &&
               resumedJournal.entries.front().executionResult.value("resolution", std::string()) ==
                   "resolved_partially_filled" &&
               resumedJournal.entries.front().executionResult.value("restart_recovery_state", std::string()) ==
                   "recoverable" &&
               resumedJournal.entries.front().executionResult.value("restart_recovery_reason", std::string()) ==
                   "partial_fill_active",
           "phase 7 runtime bridge restart-resume reconciliation should keep an active partial fill submitted");
    {
        const auto recoverySummary = tape_phase7::summarizeExecutionJournalRecovery(resumedJournal);
        expect(recoverySummary.recoveryRequired &&
                   recoverySummary.runtimeBackedSubmittedCount == 1 &&
                   !recoverySummary.staleRecoveryRequired,
               "phase 7 runtime bridge restart-resume reconciliation should clear stale pressure after refreshing a live partial fill");
    }

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        auto& order = g_data.orders[9101];
        order.status = "Filled";
        order.filledQty = 30.0;
        order.remainingQty = 0.0;
        order.avgFillPrice = 46.15;
        order.localState = LocalOrderState::Filled;
        order.seenExecIds.insert("E-PHASE7-BRIDGE-2");

        auto traceIt = g_data.traces.find(4101);
        if (traceIt == g_data.traces.end()) {
            TradeTrace trace;
            trace.traceId = 4101;
            trace.orderId = 9101;
            trace.permId = 5101;
            trace.source = "phase7-runtime-bridge";
            trace.symbol = order.symbol;
            trace.side = order.side;
            trace.account = order.account;
            trace.requestedQty = static_cast<int>(order.quantity);
            trace.limitPrice = order.limitPrice;
            traceIt = g_data.traces.emplace(trace.traceId, std::move(trace)).first;
            g_data.traceRecency.push_back(traceIt->first);
            g_data.latestTraceId = traceIt->first;
            g_data.traceIdByOrderId[order.orderId] = traceIt->first;
            g_data.traceIdByPermId[5101] = traceIt->first;
        }
        traceIt->second.fills.clear();
        FillSlice firstFill;
        firstFill.execId = "E-PHASE7-BRIDGE-1";
        firstFill.shares = 10;
        firstFill.price = 46.11;
        firstFill.cumQty = 10.0;
        firstFill.avgPrice = 46.11;
        firstFill.exchange = "SIM";
        traceIt->second.fills.push_back(firstFill);
        FillSlice finalFill;
        finalFill.execId = "E-PHASE7-BRIDGE-2";
        finalFill.shares = 20;
        finalFill.price = 46.17;
        finalFill.cumQty = 30.0;
        finalFill.avgPrice = 46.15;
        finalFill.exchange = "SIM";
        traceIt->second.fills.push_back(finalFill);
        traceIt->second.latestStatus = "Filled";
        traceIt->second.terminalStatus = "Filled";
        g_data.traceIdByExecId[firstFill.execId] = 4101;
        g_data.traceIdByExecId[finalFill.execId] = 4101;
    }
    publishSharedDataSnapshot();

    tape_phase7::ExecutionJournalArtifact reconciledJournal;
    std::vector<std::string> reconciledJournalEntryIds;
    std::string reconciledJournalAuditEventId;
    expect(tape_phase7::reconcileExecutionJournalEntriesViaRuntime(nullptr,
                                                                   {},
                                                                   journal.journalArtifact.artifactId,
                                                                   journalEntryIds,
                                                                   "bridge-executor",
                                                                   "Runtime reports the order as filled.",
                                                                   &reconciledJournal,
                                                                   &reconciledJournalEntryIds,
                                                                   &reconciledJournalAuditEventId,
                                                                   &phase7Code,
                                                                   &phase7Message),
           "phase 7 runtime bridge journal reconciliation should succeed: " + phase7Code + " " + phase7Message);
    expect(reconciledJournalEntryIds == journalEntryIds && !reconciledJournalAuditEventId.empty(),
           "phase 7 runtime bridge journal reconciliation should report the updated entry and audit event");
    expect(reconciledJournal.entries.front().executionStatus == tape_phase7::kExecutionEntryStatusSucceeded &&
               reconciledJournal.entries.front().terminal &&
               reconciledJournal.journalStatus == tape_phase7::kExecutionJournalStatusSucceeded &&
               reconciledJournal.entries.front().executionResult.value("order_status", std::string()) == "Filled" &&
               reconciledJournal.entries.front().executionResult.value("fill_state", std::string()) == "filled" &&
               reconciledJournal.entries.front().executionResult.value("restart_resume_policy", std::string()) ==
                   "completed" &&
               reconciledJournal.entries.front().executionResult.value("filled_qty", 0.0) == 30.0 &&
               reconciledJournal.entries.front().executionResult.value("remaining_qty", 1.0) == 0.0 &&
               reconciledJournal.entries.front().executionResult.value("local_state", std::string()) ==
                   localOrderStateToString(LocalOrderState::Filled) &&
               reconciledJournal.entries.front().executionResult.value("latest_exec_id", std::string()) ==
                   "E-PHASE7-BRIDGE-2" &&
               reconciledJournal.entries.front().executionResult.value("broker_identity", json::object()).value("perm_id", 0LL) == 5101,
           "phase 7 runtime bridge journal reconciliation should resolve the entry to terminal success from runtime state");
    {
        const json execIds = reconciledJournal.entries.front().executionResult.value("broker_identity", json::object())
                                  .value("exec_ids", json::array());
        expect(execIds.is_array() &&
                   std::find(execIds.begin(), execIds.end(), json("E-PHASE7-BRIDGE-1")) != execIds.end() &&
                   std::find(execIds.begin(), execIds.end(), json("E-PHASE7-BRIDGE-2")) != execIds.end() &&
                   reconciledJournal.entries.front().executionResult.value("trade_trace", json::object()).value("fill_count", 0ULL) == 2,
               "phase 7 runtime bridge journal reconciliation should preserve both execution ids and the final trade-trace fill count");
    }
    {
        const auto recoverySummary = tape_phase7::summarizeExecutionJournalRecovery(reconciledJournal);
        expect(!recoverySummary.recoveryRequired &&
                   recoverySummary.runtimeBackedSubmittedCount == 0,
               "phase 7 runtime bridge reconciled journal should clear runtime recovery flags");
    }

    tape_phase7::ExecutionApplyArtifact synchronizedApply;
    std::vector<std::string> synchronizedApplyEntryIds;
    std::string synchronizedApplyAuditEventId;
    expect(tape_phase7::synchronizeExecutionApplyFromJournal({},
                                                             apply.applyArtifact.artifactId,
                                                             "bridge-executor",
                                                             "Synchronize the controlled apply artifact from the journal.",
                                                             &synchronizedApply,
                                                             &synchronizedApplyEntryIds,
                                                             &synchronizedApplyAuditEventId,
                                                             &phase7Code,
                                                             &phase7Message),
           "phase 7 runtime bridge apply synchronization should succeed: " + phase7Code + " " + phase7Message);
    expect(synchronizedApply.entries.front().executionStatus == tape_phase7::kExecutionEntryStatusSucceeded &&
               synchronizedApply.entries.front().terminal &&
               synchronizedApply.applyStatus == tape_phase7::kExecutionJournalStatusSucceeded &&
               synchronizedApply.entries.front().executionResult.value("order_status", std::string()) == "Filled" &&
               synchronizedApply.entries.front().executionResult.value("latest_exec_id", std::string()) ==
                   "E-PHASE7-BRIDGE-2" &&
               synchronizedApply.entries.front().executionResult.value("broker_identity", json::object()).value("perm_id", 0LL) == 5101 &&
               synchronizedApply.journalArtifact.artifactId == journal.journalArtifact.artifactId,
           "phase 7 runtime bridge apply synchronization should mirror the reconciled journal success state");
    {
        const json execIds = synchronizedApply.entries.front().executionResult.value("broker_identity", json::object())
                                  .value("exec_ids", json::array());
        expect(execIds.is_array() &&
                   std::find(execIds.begin(), execIds.end(), json("E-PHASE7-BRIDGE-1")) != execIds.end() &&
                   std::find(execIds.begin(), execIds.end(), json("E-PHASE7-BRIDGE-2")) != execIds.end(),
               "phase 7 runtime bridge apply synchronization should preserve the reconciled broker execution identifiers");
    }
    {
        const json journalEntrySummary =
            tape_phase7::executionJournalEntryToJson(reconciledJournal.entries.front()).value("execution_result_summary",
                                                                                               json::object());
        const json applyEntrySummary =
            tape_phase7::executionApplyEntryToJson(synchronizedApply.entries.front()).value("execution_result_summary",
                                                                                            json::object());
        expect(journalEntrySummary.value("latest_exec_id", std::string()) == "E-PHASE7-BRIDGE-2" &&
                   journalEntrySummary.value("broker_identity", json::object()).value("exec_id_count", 0ULL) == 2 &&
                   journalEntrySummary.value("trade_trace", json::object()).value("fill_count", 0ULL) == 2,
               "phase 7 runtime bridge journal entry summary should expose the reconciled broker execution linkage");
        expect(applyEntrySummary.value("latest_exec_id", std::string()) == "E-PHASE7-BRIDGE-2" &&
                   applyEntrySummary.value("broker_identity", json::object()).value("exec_id_count", 0ULL) == 2 &&
                   applyEntrySummary.value("trade_trace", json::object()).value("fill_count", 0ULL) == 2,
               "phase 7 runtime bridge apply entry summary should expose the reconciled broker execution linkage");
    }
    {
        const auto recoverySummary = tape_phase7::summarizeExecutionApplyRecovery(synchronizedApply);
        expect(!recoverySummary.recoveryRequired &&
                   recoverySummary.runtimeBackedSubmittedCount == 0,
               "phase 7 runtime bridge synchronized apply should clear runtime recovery flags");
    }
    {
        tape_phase7::RuntimeRecoveryBacklogSummary backlog;
        expect(tape_phase7::summarizeRuntimeRecoveryBacklog(&backlog, &phase7Code, &phase7Message),
               "phase 7 runtime bridge backlog summary should succeed after reconciliation: " +
                   phase7Code + " " + phase7Message);
        expect(backlog.runtimeBackedSubmittedJournalEntryCount == 0 &&
                   backlog.runtimeBackedSubmittedApplyEntryCount == 0 &&
                   !backlog.recoveryRequired &&
                   !backlog.staleRecoveryRequired,
               "phase 7 runtime bridge backlog summary should clear after reconciliation succeeds");
        const auto clearedPlan = tape_phase7::runtimeStartupRecoveryPlan(backlog, true);
        expect(clearedPlan.startupAction == "none" &&
                   !clearedPlan.recoverySweepRecommended &&
                   !clearedPlan.manualAttentionRecommended,
               "phase 7 runtime bridge startup policy should clear once recovery backlog is resolved");
    }

    server.stop();
    unbindSharedDataOwner(&owner);
    resetSharedDataForTesting();
}

void testTapePhase7RuntimeBridgeSweepMirrorsManualReviewIntoApplyArtifacts() {
    clearTestFiles();
    std::error_code phase7Ec;
    fs::remove_all(testDataDir() / "phase7_artifacts", phase7Ec);

    SharedData owner;
    bindSharedDataOwner(&owner);

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);

        OrderInfo order;
        order.orderId = 9201;
        order.account = "U23154741";
        order.symbol = "INTC";
        order.side = "BUY";
        order.quantity = 25.0;
        order.remainingQty = 25.0;
        order.limitPrice = 46.10;
        order.status = "Submitted";
        order.localState = LocalOrderState::Working;
        g_data.orders[order.orderId] = order;

        TradeTrace trace;
        trace.traceId = 99201;
        trace.orderId = order.orderId;
        trace.permId = 5201;
        trace.source = "phase7-runtime-sweep";
        trace.symbol = order.symbol;
        trace.side = order.side;
        trace.account = order.account;
        trace.requestedQty = static_cast<int>(order.quantity);
        trace.limitPrice = order.limitPrice;
        g_data.traces[trace.traceId] = trace;
        g_data.traceRecency.push_back(trace.traceId);
        g_data.latestTraceId = trace.traceId;
    }
    publishSharedDataSnapshot();

    const fs::path rootDir = testDataDir() / "tape-engine-phase7-runtime-sweep";
    const fs::path socketPath = testDataDir() / "tape-engine-phase7-runtime-sweep.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "ib:conid:9601:STK:SMART:USD:INTC";
    config.ringCapacity = 32;

    tape_engine::Server server(config);
    std::string startError;
    expect(server.start(&startError), "tape-engine should start for the phase 7 runtime sweep test: " + startError);

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-engine-phase7-runtime-sweep";
    options.runtimeSessionId = "runtime-engine-phase7-runtime-sweep";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;

    BridgeOutboxRecord orderIntent = makeBridgeRecord(9701, "order_intent", "WebSocket", "INTC", "BUY",
                                                      4201, 9201, 5201, "", "phase 7 runtime sweep order intent",
                                                      "2026-03-16T10:22:00.100");
    orderIntent.instrumentId = "ib:conid:9601:STK:SMART:USD:INTC";

    BridgeOutboxRecord orderStatus = makeBridgeRecord(9702, "order_status", "BrokerOrderStatus", "INTC", "BUY",
                                                      4201, 9201, 5201, "",
                                                      "phase 7 runtime sweep submitted",
                                                      "2026-03-16T10:22:00.200");
    orderStatus.instrumentId = "ib:conid:9601:STK:SMART:USD:INTC";

    BridgeOutboxRecord fillExecution = makeBridgeRecord(9703, "fill_execution", "BrokerExecution", "INTC", "BUY",
                                                        4201, 9201, 5201, "E-PHASE7-SWEEP-1",
                                                        "phase 7 runtime sweep fill",
                                                        "2026-03-16T10:22:00.300");
    fillExecution.instrumentId = "ib:conid:9601:STK:SMART:USD:INTC";

    options.batchSeq = 701;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({orderIntent}, options)), &error),
           "phase 7 runtime sweep fixture should accept the order intent batch: " + error);
    options.batchSeq = 702;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({orderStatus}, options)), &error),
           "phase 7 runtime sweep fixture should accept the order status batch: " + error);
    options.batchSeq = 703;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({fillExecution}, options)), &error),
           "phase 7 runtime sweep fixture should accept the fill batch: " + error);

    waitUntil([&]() {
        const auto snapshot = server.snapshot();
        return snapshot.latestFrozenRevisionId >= 3 && snapshot.segments.size() >= 3;
    }, "phase 7 runtime sweep fixture should freeze the seeded batches");

    tape_engine::Client client(socketPath.string());
    tape_engine::QueryResponse response;

    tape_engine::QueryRequest caseReportRequest;
    caseReportRequest.requestId = "phase7-runtime-sweep-case-report";
    caseReportRequest.operation = "scan_order_case_report";
    caseReportRequest.orderId = 9201;
    expect(client.query(caseReportRequest, &response, &error),
           "phase 7 runtime sweep case report scan should succeed: " + error);
    const std::uint64_t caseReportId =
        response.summary.value("case_report_artifact", json::object()).value("report_id", 0ULL);
    expect(caseReportId > 0, "phase 7 runtime sweep case report scan should persist a durable case report");

    tape_engine::QueryRequest exportCaseBundleRequest;
    exportCaseBundleRequest.requestId = "phase7-runtime-sweep-export-case-bundle";
    exportCaseBundleRequest.operation = "export_case_bundle";
    exportCaseBundleRequest.reportId = caseReportId;
    expect(client.query(exportCaseBundleRequest, &response, &error),
           "phase 7 runtime sweep case bundle export should succeed: " + error);
    const std::string caseBundlePath =
        response.summary.value("bundle", json::object()).value("bundle_path", std::string());
    expect(!caseBundlePath.empty() && fs::exists(caseBundlePath),
           "phase 7 runtime sweep case bundle export should write a portable case bundle");

    tape_phase7::AnalysisArtifact analysis;
    bool analysisCreated = false;
    std::string phase7Code;
    std::string phase7Message;
    expect(tape_phase7::runAnalyzerFromBundlePath(caseBundlePath,
                                                  tape_phase7::kOrderImpactAnalyzerProfile,
                                                  &analysis,
                                                  &analysisCreated,
                                                  &phase7Code,
                                                  &phase7Message),
           "phase 7 runtime sweep order-impact analyzer should succeed: " + phase7Code + " " + phase7Message);
    expect(!analysis.findings.empty(),
           "phase 7 runtime sweep analysis should emit findings");
    const auto dispatchableFindingIt = std::find_if(analysis.findings.begin(),
                                                    analysis.findings.end(),
                                                    [](const tape_phase7::FindingRecord& finding) {
                                                        return finding.category == "market_impact_risk" ||
                                                            finding.category == "fill_quality_risk";
                                                    });
    expect(dispatchableFindingIt != analysis.findings.end(),
           "phase 7 runtime sweep analysis should emit at least one runtime-dispatchable finding");

    tape_phase7::PlaybookArtifact playbook;
    bool playbookCreated = false;
    expect(tape_phase7::buildGuardedPlaybook({},
                                             analysis.analysisArtifact.artifactId,
                                             {dispatchableFindingIt->findingId},
                                             tape_phase7::kDefaultPlaybookMode,
                                             &playbook,
                                             &playbookCreated,
                                             &phase7Code,
                                             &phase7Message),
           "phase 7 runtime sweep playbook build should succeed: " + phase7Code + " " + phase7Message);
    const auto dispatchableActionIt = std::find_if(playbook.plannedActions.begin(),
                                                   playbook.plannedActions.end(),
                                                   [](const tape_phase7::PlaybookAction& action) {
                                                       return action.executionRequest.is_object() &&
                                                           !action.executionRequest.empty();
                                                   });
    expect(dispatchableActionIt != playbook.plannedActions.end(),
           "phase 7 runtime sweep playbook should include a runtime-dispatchable action");

    tape_phase7::ExecutionLedgerArtifact ledger;
    bool ledgerCreated = false;
    expect(tape_phase7::buildExecutionLedger({},
                                             playbook.playbookArtifact.artifactId,
                                             &ledger,
                                             &ledgerCreated,
                                             &phase7Code,
                                             &phase7Message),
           "phase 7 runtime sweep execution ledger build should succeed: " + phase7Code + " " + phase7Message);
    const auto dispatchableLedgerEntryIt = std::find_if(ledger.entries.begin(),
                                                        ledger.entries.end(),
                                                        [&](const tape_phase7::ExecutionLedgerEntry& entry) {
                                                            return entry.actionId == dispatchableActionIt->actionId;
                                                        });
    expect(dispatchableLedgerEntryIt != ledger.entries.end(),
           "phase 7 runtime sweep execution ledger should preserve the dispatchable action entry");

    const std::vector<std::string> approvedLedgerEntryIds = {dispatchableLedgerEntryIt->entryId};
    tape_phase7::ExecutionLedgerArtifact reviewedLedger;
    std::vector<std::string> reviewedLedgerEntryIds;
    std::string reviewedLedgerAuditEventId;
    expect(tape_phase7::recordExecutionLedgerReview({},
                                                    ledger.ledgerArtifact.artifactId,
                                                    approvedLedgerEntryIds,
                                                    tape_phase7::kLedgerEntryStatusApproved,
                                                    "bridge-reviewer-a",
                                                    "First approval for the runtime sweep journal.",
                                                    &reviewedLedger,
                                                    &reviewedLedgerEntryIds,
                                                    &reviewedLedgerAuditEventId,
                                                    &phase7Code,
                                                    &phase7Message),
           "phase 7 runtime sweep first ledger approval should succeed: " + phase7Code + " " + phase7Message);
    tape_phase7::ExecutionLedgerArtifact readyLedger;
    std::vector<std::string> readyLedgerEntryIds;
    std::string readyLedgerAuditEventId;
    expect(tape_phase7::recordExecutionLedgerReview({},
                                                    ledger.ledgerArtifact.artifactId,
                                                    approvedLedgerEntryIds,
                                                    tape_phase7::kLedgerEntryStatusApproved,
                                                    "bridge-reviewer-b",
                                                    "Second approval makes the runtime sweep journal ready.",
                                                    &readyLedger,
                                                    &readyLedgerEntryIds,
                                                    &readyLedgerAuditEventId,
                                                    &phase7Code,
                                                    &phase7Message),
           "phase 7 runtime sweep second ledger approval should succeed: " + phase7Code + " " + phase7Message);
    expect(readyLedger.ledgerStatus == tape_phase7::kLedgerStatusReadyForExecution,
           "phase 7 runtime sweep execution ledger should become ready for execution");

    tape_phase7::ExecutionJournalArtifact journal;
    bool journalCreated = false;
    expect(tape_phase7::startExecutionJournal({},
                                              readyLedger.ledgerArtifact.artifactId,
                                              "bridge-executor",
                                              "phase7.execution_operator.v1",
                                              &journal,
                                              &journalCreated,
                                              &phase7Code,
                                              &phase7Message),
           "phase 7 runtime sweep journal start should succeed: " + phase7Code + " " + phase7Message);
    expect(journalCreated && journal.entries.size() == 1,
           "phase 7 runtime sweep should start one execution journal entry");

    const std::vector<std::string> journalEntryIds = {journal.entries.front().journalEntryId};
    tape_phase7::ExecutionJournalArtifact dispatchedJournal;
    std::vector<std::string> dispatchedJournalEntryIds;
    std::string dispatchedJournalAuditEventId;
    expect(tape_phase7::dispatchExecutionJournalEntriesViaRuntime(nullptr,
                                                                  {},
                                                                  journal.journalArtifact.artifactId,
                                                                  journalEntryIds,
                                                                  "bridge-executor",
                                                                  "phase7.execution_operator.v1",
                                                                  "Dispatch the order-anchored execution journal entry.",
                                                                  &dispatchedJournal,
                                                                  &dispatchedJournalEntryIds,
                                                                  &dispatchedJournalAuditEventId,
                                                                  &phase7Code,
                                                                  &phase7Message),
           "phase 7 runtime sweep journal dispatch should succeed: " + phase7Code + " " + phase7Message);
    expect(dispatchedJournal.entries.front().executionStatus == tape_phase7::kExecutionEntryStatusSubmitted,
           "phase 7 runtime sweep dispatch should move the journal into submitted state");

    tape_phase7::ExecutionApplyArtifact apply;
    bool applyCreated = false;
    expect(tape_phase7::startExecutionApply({},
                                            dispatchedJournal.journalArtifact.artifactId,
                                            {},
                                            "bridge-executor",
                                            "phase7.execution_operator.v1",
                                            "Create an apply artifact before the runtime sweep resolves.",
                                            &apply,
                                            &applyCreated,
                                            &phase7Code,
                                            &phase7Message),
           "phase 7 runtime sweep apply start should succeed: " + phase7Code + " " + phase7Message);
    expect(applyCreated &&
               apply.entries.front().executionStatus == tape_phase7::kExecutionEntryStatusSubmitted,
           "phase 7 runtime sweep apply start should mirror the submitted journal state");
    {
        const auto recoverySummary = tape_phase7::summarizeExecutionJournalRecovery(dispatchedJournal);
        expect(recoverySummary.recoveryRequired &&
                   recoverySummary.runtimeBackedSubmittedCount == 1,
               "phase 7 runtime sweep dispatched journal should advertise pending runtime recovery");
    }

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        auto& order = g_data.orders[9201];
        order.status = "Inactive";
        order.localState = LocalOrderState::NeedsManualReview;
    }
    publishSharedDataSnapshot();

    tape_phase7::RuntimeBridgeSweepResult sweep;
    expect(tape_phase7::reconcileExecutionArtifactsViaRuntime(nullptr,
                                                              "bridge-executor",
                                                              "Sweep runtime-backed execution artifacts after manual review escalation.",
                                                              &sweep,
                                                              &phase7Code,
                                                              &phase7Message),
           "phase 7 runtime sweep should reconcile runtime-backed artifacts: " + phase7Code + " " + phase7Message);
    {
        std::ostringstream sweepSummary;
        sweepSummary << "scanned=" << sweep.scannedJournalCount
                     << ", updated_journals=" << sweep.updatedJournalCount
                     << ", updated_applies=" << sweep.updatedApplyCount;
        expect(sweep.scannedJournalCount >= 1 &&
                   sweep.updatedJournalCount >= 1 &&
                   sweep.updatedApplyCount >= 1,
               "phase 7 runtime sweep should update at least one journal/apply pair: " + sweepSummary.str());
    }
    expect(std::find(sweep.updatedJournalArtifactIds.begin(),
                     sweep.updatedJournalArtifactIds.end(),
                     journal.journalArtifact.artifactId) != sweep.updatedJournalArtifactIds.end(),
           "phase 7 runtime sweep should include the updated execution journal artifact id");
    expect(std::find(sweep.updatedApplyArtifactIds.begin(),
                     sweep.updatedApplyArtifactIds.end(),
                     apply.applyArtifact.artifactId) != sweep.updatedApplyArtifactIds.end(),
           "phase 7 runtime sweep should include the updated execution apply artifact id");
    expect(std::find(sweep.updatedJournalEntryIds.begin(),
                     sweep.updatedJournalEntryIds.end(),
                     journal.entries.front().journalEntryId) != sweep.updatedJournalEntryIds.end(),
           "phase 7 runtime sweep should include the updated journal entry id");
    expect(std::find(sweep.updatedApplyEntryIds.begin(),
                     sweep.updatedApplyEntryIds.end(),
                     apply.entries.front().applyEntryId) != sweep.updatedApplyEntryIds.end(),
           "phase 7 runtime sweep should include the updated apply entry id");

    tape_phase7::ExecutionJournalArtifact sweptJournal;
    expect(tape_phase7::loadExecutionJournalArtifact({},
                                                     journal.journalArtifact.artifactId,
                                                     &sweptJournal,
                                                     &phase7Code,
                                                     &phase7Message),
           "phase 7 runtime sweep should reload the updated journal: " + phase7Code + " " + phase7Message);
    expect(sweptJournal.entries.front().executionStatus == tape_phase7::kExecutionEntryStatusFailed &&
               sweptJournal.entries.front().terminal &&
               sweptJournal.entries.front().failureCode == "manual_review_required" &&
               sweptJournal.entries.front().executionResult.value("manual_review_required", false) &&
               sweptJournal.entries.front().executionResult.value("resolution", std::string()) ==
                   "manual_review_required" &&
               sweptJournal.entries.front().executionResult.value("broker_identity", json::object()).value("perm_id", 0LL) == 5201 &&
               sweptJournal.entries.front().executionResult.value("trade_trace", json::object()).value("trace_id", 0ULL) != 0 &&
               sweptJournal.journalStatus == tape_phase7::kExecutionJournalStatusFailed,
           "phase 7 runtime sweep should resolve the journal to terminal manual-review failure");
    {
        const auto recoverySummary = tape_phase7::summarizeExecutionJournalRecovery(sweptJournal);
        expect(!recoverySummary.recoveryRequired &&
                   recoverySummary.runtimeBackedSubmittedCount == 0,
               "phase 7 runtime sweep resolved journal should clear runtime recovery flags");
    }

    tape_phase7::ExecutionApplyArtifact sweptApply;
    expect(tape_phase7::loadExecutionApplyArtifact({},
                                                   apply.applyArtifact.artifactId,
                                                   &sweptApply,
                                                   &phase7Code,
                                                   &phase7Message),
           "phase 7 runtime sweep should reload the updated apply artifact: " + phase7Code + " " + phase7Message);
    expect(sweptApply.entries.front().executionStatus == tape_phase7::kExecutionEntryStatusFailed &&
               sweptApply.entries.front().terminal &&
               sweptApply.entries.front().failureCode == "manual_review_required" &&
               sweptApply.entries.front().executionResult.value("manual_review_required", false) &&
               sweptApply.entries.front().executionResult.value("broker_identity", json::object()).value("perm_id", 0LL) == 5201 &&
               sweptApply.applyStatus == tape_phase7::kExecutionJournalStatusFailed &&
               sweptApply.journalArtifact.artifactId == journal.journalArtifact.artifactId,
           "phase 7 runtime sweep should mirror the manual-review failure into the linked apply artifact");
    {
        const auto recoverySummary = tape_phase7::summarizeExecutionApplyRecovery(sweptApply);
        expect(!recoverySummary.recoveryRequired &&
                   recoverySummary.runtimeBackedSubmittedCount == 0,
               "phase 7 runtime sweep resolved apply should clear runtime recovery flags");
    }

    server.stop();
    unbindSharedDataOwner(&owner);
    resetSharedDataForTesting();
}

void testTapePhase7RuntimeBridgeSweepEscalatesStaleMissingOrders() {
    clearTestFiles();
    std::error_code phase7Ec;
    fs::remove_all(testDataDir() / "phase7_artifacts", phase7Ec);

    SharedData owner;
    bindSharedDataOwner(&owner);

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);

        OrderInfo order;
        order.orderId = 9301;
        order.account = "U23154741";
        order.symbol = "INTC";
        order.side = "BUY";
        order.quantity = 30.0;
        order.remainingQty = 30.0;
        order.limitPrice = 46.45;
        order.status = "Submitted";
        order.localState = LocalOrderState::Working;
        g_data.orders[order.orderId] = order;

        TradeTrace trace;
        trace.traceId = 99301;
        trace.orderId = order.orderId;
        trace.permId = 5301;
        trace.source = "phase7-runtime-recovery";
        trace.symbol = order.symbol;
        trace.side = order.side;
        trace.account = order.account;
        trace.requestedQty = static_cast<int>(order.quantity);
        trace.limitPrice = order.limitPrice;
        g_data.traces[trace.traceId] = trace;
        g_data.traceRecency.push_back(trace.traceId);
        g_data.latestTraceId = trace.traceId;
    }
    publishSharedDataSnapshot();

    const fs::path rootDir = testDataDir() / "tape-engine-phase7-runtime-recovery";
    const fs::path socketPath = testDataDir() / "tape-engine-phase7-runtime-recovery.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "ib:conid:9701:STK:SMART:USD:INTC";
    config.ringCapacity = 32;

    tape_engine::Server server(config);
    std::string startError;
    expect(server.start(&startError), "tape-engine should start for the phase 7 runtime recovery test: " + startError);

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-engine-phase7-runtime-recovery";
    options.runtimeSessionId = "runtime-engine-phase7-runtime-recovery";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;

    BridgeOutboxRecord orderIntent = makeBridgeRecord(9801, "order_intent", "WebSocket", "INTC", "BUY",
                                                      4301, 9301, 5301, "", "phase 7 runtime recovery order intent",
                                                      "2026-03-16T10:32:00.100");
    orderIntent.instrumentId = "ib:conid:9701:STK:SMART:USD:INTC";

    BridgeOutboxRecord orderStatus = makeBridgeRecord(9802, "order_status", "BrokerOrderStatus", "INTC", "BUY",
                                                      4301, 9301, 5301, "",
                                                      "phase 7 runtime recovery submitted",
                                                      "2026-03-16T10:32:00.200");
    orderStatus.instrumentId = "ib:conid:9701:STK:SMART:USD:INTC";

    BridgeOutboxRecord fillExecution = makeBridgeRecord(9803, "fill_execution", "BrokerExecution", "INTC", "BUY",
                                                        4301, 9301, 5301, "E-PHASE7-RECOVERY-1",
                                                        "phase 7 runtime recovery fill",
                                                        "2026-03-16T10:32:00.300");
    fillExecution.instrumentId = "ib:conid:9701:STK:SMART:USD:INTC";

    options.batchSeq = 801;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({orderIntent}, options)), &error),
           "phase 7 runtime recovery fixture should accept the order intent batch: " + error);
    options.batchSeq = 802;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({orderStatus}, options)), &error),
           "phase 7 runtime recovery fixture should accept the order status batch: " + error);
    options.batchSeq = 803;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({fillExecution}, options)), &error),
           "phase 7 runtime recovery fixture should accept the fill batch: " + error);

    waitUntil([&]() {
        const auto snapshot = server.snapshot();
        return snapshot.latestFrozenRevisionId >= 3 && snapshot.segments.size() >= 3;
    }, "phase 7 runtime recovery fixture should freeze the seeded batches");

    tape_engine::Client client(socketPath.string());
    tape_engine::QueryResponse response;

    tape_engine::QueryRequest caseReportRequest;
    caseReportRequest.requestId = "phase7-runtime-recovery-case-report";
    caseReportRequest.operation = "scan_order_case_report";
    caseReportRequest.orderId = 9301;
    expect(client.query(caseReportRequest, &response, &error),
           "phase 7 runtime recovery case report scan should succeed: " + error);
    const std::uint64_t caseReportId =
        response.summary.value("case_report_artifact", json::object()).value("report_id", 0ULL);
    expect(caseReportId > 0, "phase 7 runtime recovery case report scan should persist a durable case report");

    tape_engine::QueryRequest exportCaseBundleRequest;
    exportCaseBundleRequest.requestId = "phase7-runtime-recovery-export-case-bundle";
    exportCaseBundleRequest.operation = "export_case_bundle";
    exportCaseBundleRequest.reportId = caseReportId;
    expect(client.query(exportCaseBundleRequest, &response, &error),
           "phase 7 runtime recovery case bundle export should succeed: " + error);
    const std::string caseBundlePath =
        response.summary.value("bundle", json::object()).value("bundle_path", std::string());
    expect(!caseBundlePath.empty() && fs::exists(caseBundlePath),
           "phase 7 runtime recovery case bundle export should write a portable case bundle");

    tape_phase7::AnalysisArtifact analysis;
    bool analysisCreated = false;
    std::string phase7Code;
    std::string phase7Message;
    expect(tape_phase7::runAnalyzerFromBundlePath(caseBundlePath,
                                                  tape_phase7::kOrderImpactAnalyzerProfile,
                                                  &analysis,
                                                  &analysisCreated,
                                                  &phase7Code,
                                                  &phase7Message),
           "phase 7 runtime recovery order-impact analyzer should succeed: " + phase7Code + " " + phase7Message);
    const auto dispatchableFindingIt = std::find_if(analysis.findings.begin(),
                                                    analysis.findings.end(),
                                                    [](const tape_phase7::FindingRecord& finding) {
                                                        return finding.category == "market_impact_risk" ||
                                                            finding.category == "fill_quality_risk";
                                                    });
    expect(dispatchableFindingIt != analysis.findings.end(),
           "phase 7 runtime recovery analysis should emit at least one runtime-dispatchable finding");

    tape_phase7::PlaybookArtifact playbook;
    bool playbookCreated = false;
    expect(tape_phase7::buildGuardedPlaybook({},
                                             analysis.analysisArtifact.artifactId,
                                             {dispatchableFindingIt->findingId},
                                             tape_phase7::kDefaultPlaybookMode,
                                             &playbook,
                                             &playbookCreated,
                                             &phase7Code,
                                             &phase7Message),
           "phase 7 runtime recovery playbook build should succeed: " + phase7Code + " " + phase7Message);
    const auto dispatchableActionIt = std::find_if(playbook.plannedActions.begin(),
                                                   playbook.plannedActions.end(),
                                                   [](const tape_phase7::PlaybookAction& action) {
                                                       return action.executionRequest.is_object() &&
                                                           !action.executionRequest.empty();
                                                   });
    expect(dispatchableActionIt != playbook.plannedActions.end(),
           "phase 7 runtime recovery playbook should include a runtime-dispatchable action");

    tape_phase7::ExecutionLedgerArtifact ledger;
    bool ledgerCreated = false;
    expect(tape_phase7::buildExecutionLedger({},
                                             playbook.playbookArtifact.artifactId,
                                             &ledger,
                                             &ledgerCreated,
                                             &phase7Code,
                                             &phase7Message),
           "phase 7 runtime recovery execution ledger build should succeed: " + phase7Code + " " + phase7Message);
    const auto dispatchableLedgerEntryIt = std::find_if(ledger.entries.begin(),
                                                        ledger.entries.end(),
                                                        [&](const tape_phase7::ExecutionLedgerEntry& entry) {
                                                            return entry.actionId == dispatchableActionIt->actionId;
                                                        });
    expect(dispatchableLedgerEntryIt != ledger.entries.end(),
           "phase 7 runtime recovery execution ledger should preserve the dispatchable action entry");

    const std::vector<std::string> approvedLedgerEntryIds = {dispatchableLedgerEntryIt->entryId};
    tape_phase7::ExecutionLedgerArtifact reviewedLedger;
    std::vector<std::string> reviewedLedgerEntryIds;
    std::string reviewedLedgerAuditEventId;
    expect(tape_phase7::recordExecutionLedgerReview({},
                                                    ledger.ledgerArtifact.artifactId,
                                                    approvedLedgerEntryIds,
                                                    tape_phase7::kLedgerEntryStatusApproved,
                                                    "bridge-reviewer-a",
                                                    "First approval for the runtime recovery journal.",
                                                    &reviewedLedger,
                                                    &reviewedLedgerEntryIds,
                                                    &reviewedLedgerAuditEventId,
                                                    &phase7Code,
                                                    &phase7Message),
           "phase 7 runtime recovery first ledger approval should succeed: " + phase7Code + " " + phase7Message);
    tape_phase7::ExecutionLedgerArtifact readyLedger;
    std::vector<std::string> readyLedgerEntryIds;
    std::string readyLedgerAuditEventId;
    expect(tape_phase7::recordExecutionLedgerReview({},
                                                    ledger.ledgerArtifact.artifactId,
                                                    approvedLedgerEntryIds,
                                                    tape_phase7::kLedgerEntryStatusApproved,
                                                    "bridge-reviewer-b",
                                                    "Second approval makes the runtime recovery journal ready.",
                                                    &readyLedger,
                                                    &readyLedgerEntryIds,
                                                    &readyLedgerAuditEventId,
                                                    &phase7Code,
                                                    &phase7Message),
           "phase 7 runtime recovery second ledger approval should succeed: " + phase7Code + " " + phase7Message);

    tape_phase7::ExecutionJournalArtifact journal;
    bool journalCreated = false;
    expect(tape_phase7::startExecutionJournal({},
                                              readyLedger.ledgerArtifact.artifactId,
                                              "bridge-executor",
                                              "phase7.execution_operator.v1",
                                              &journal,
                                              &journalCreated,
                                              &phase7Code,
                                              &phase7Message),
           "phase 7 runtime recovery journal start should succeed: " + phase7Code + " " + phase7Message);

    const std::vector<std::string> journalEntryIds = {journal.entries.front().journalEntryId};
    tape_phase7::ExecutionJournalArtifact dispatchedJournal;
    std::vector<std::string> dispatchedJournalEntryIds;
    std::string dispatchedJournalAuditEventId;
    expect(tape_phase7::dispatchExecutionJournalEntriesViaRuntime(nullptr,
                                                                  {},
                                                                  journal.journalArtifact.artifactId,
                                                                  journalEntryIds,
                                                                  "bridge-executor",
                                                                  "phase7.execution_operator.v1",
                                                                  "Dispatch the order-anchored execution journal entry.",
                                                                  &dispatchedJournal,
                                                                  &dispatchedJournalEntryIds,
                                                                  &dispatchedJournalAuditEventId,
                                                                  &phase7Code,
                                                                  &phase7Message),
           "phase 7 runtime recovery journal dispatch should succeed: " + phase7Code + " " + phase7Message);

    tape_phase7::ExecutionApplyArtifact apply;
    bool applyCreated = false;
    expect(tape_phase7::startExecutionApply({},
                                            dispatchedJournal.journalArtifact.artifactId,
                                            {},
                                            "bridge-executor",
                                            "phase7.execution_operator.v1",
                                            "Create an apply artifact before the runtime recovery sweep resolves.",
                                            &apply,
                                            &applyCreated,
                                            &phase7Code,
                                            &phase7Message),
           "phase 7 runtime recovery apply start should succeed: " + phase7Code + " " + phase7Message);
    {
        const auto recoverySummary = tape_phase7::summarizeExecutionJournalRecovery(dispatchedJournal);
        expect(recoverySummary.recoveryRequired &&
                   recoverySummary.runtimeBackedSubmittedCount == 1,
               "phase 7 runtime recovery dispatched journal should advertise pending runtime recovery");
    }

    tape_phase7::ExecutionJournalArtifact staleJournal;
    expect(tape_phase7::loadExecutionJournalArtifact({},
                                                     journal.journalArtifact.artifactId,
                                                     &staleJournal,
                                                     &phase7Code,
                                                     &phase7Message),
           "phase 7 runtime recovery should reload the dispatched journal: " + phase7Code + " " + phase7Message);
    staleJournal.entries.front().startedAtUtc = "2026-03-16T00:00:00.000Z";
    staleJournal.entries.front().lastUpdatedAtUtc = "2026-03-16T00:00:00.000Z";
    expect(tape_phase7::persistExecutionJournalArtifact(staleJournal, &phase7Code, &phase7Message),
           "phase 7 runtime recovery should persist the stale dispatched journal: " + phase7Code + " " + phase7Message);
    {
        tape_phase7::ExecutionJournalArtifact staleLoadedJournal;
        expect(tape_phase7::loadExecutionJournalArtifact({},
                                                         journal.journalArtifact.artifactId,
                                                         &staleLoadedJournal,
                                                         &phase7Code,
                                                         &phase7Message),
               "phase 7 runtime recovery should reload the stale journal before sweep: " +
                   phase7Code + " " + phase7Message);
        const auto recoverySummary = tape_phase7::summarizeExecutionJournalRecovery(staleLoadedJournal);
        expect(recoverySummary.recoveryRequired &&
                   recoverySummary.staleRecoveryRequired &&
                   recoverySummary.staleRuntimeBackedCount == 1,
               "phase 7 runtime recovery stale journal should advertise stale runtime recovery");
    }
    {
        tape_phase7::RuntimeRecoveryBacklogSummary backlog;
        expect(tape_phase7::summarizeRuntimeRecoveryBacklog(&backlog, &phase7Code, &phase7Message),
               "phase 7 runtime recovery backlog summary should succeed before stale sweep: " +
                   phase7Code + " " + phase7Message);
        expect(backlog.recoveryRequiredJournalCount >= 1 &&
                   backlog.recoveryRequiredApplyCount >= 1 &&
                   backlog.staleRecoveryRequiredJournalCount >= 1 &&
                   backlog.runtimeBackedSubmittedJournalEntryCount >= 1 &&
                   backlog.runtimeBackedSubmittedApplyEntryCount >= 1 &&
                   backlog.recoveryRequired &&
                   backlog.staleRecoveryRequired,
               "phase 7 runtime recovery backlog summary should expose stale recovery pressure before sweep");
        const auto noActorPlan = tape_phase7::runtimeStartupRecoveryPlan(backlog, false);
        expect(noActorPlan.startupAction == "await_actor" &&
                   !noActorPlan.recoverySweepRecommended &&
                   noActorPlan.manualAttentionRecommended,
               "phase 7 runtime recovery startup policy should wait for an actor while highlighting stale pressure");
        const auto actorPlan = tape_phase7::runtimeStartupRecoveryPlan(backlog, true);
        expect(actorPlan.startupAction == "recover_pending" &&
                   actorPlan.recoverySweepRecommended &&
                   actorPlan.manualAttentionRecommended,
               "phase 7 runtime recovery startup policy should recommend a sweep and manual attention when stale entries exist");
    }

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        g_data.orders.erase(9301);
    }
    publishSharedDataSnapshot();

    tape_phase7::RuntimeBridgeSweepResult sweep;
    expect(tape_phase7::reconcileExecutionArtifactsViaRuntime(nullptr,
                                                              "bridge-executor",
                                                              "Sweep stale runtime-backed execution artifacts after the order disappears.",
                                                              &sweep,
                                                              &phase7Code,
                                                              &phase7Message),
           "phase 7 runtime recovery sweep should reconcile stale artifacts: " + phase7Code + " " + phase7Message);
    expect(std::find(sweep.updatedJournalArtifactIds.begin(),
                     sweep.updatedJournalArtifactIds.end(),
                     journal.journalArtifact.artifactId) != sweep.updatedJournalArtifactIds.end() &&
               std::find(sweep.updatedApplyArtifactIds.begin(),
                         sweep.updatedApplyArtifactIds.end(),
                         apply.applyArtifact.artifactId) != sweep.updatedApplyArtifactIds.end(),
           "phase 7 runtime recovery sweep should report both the journal and apply artifacts as updated");

    tape_phase7::ExecutionJournalArtifact recoveredJournal;
    expect(tape_phase7::loadExecutionJournalArtifact({},
                                                     journal.journalArtifact.artifactId,
                                                     &recoveredJournal,
                                                     &phase7Code,
                                                     &phase7Message),
           "phase 7 runtime recovery sweep should reload the recovered journal: " + phase7Code + " " + phase7Message);
    expect(recoveredJournal.entries.front().executionStatus == tape_phase7::kExecutionEntryStatusFailed &&
               recoveredJournal.entries.front().terminal &&
               recoveredJournal.entries.front().failureCode == "runtime_order_missing" &&
               recoveredJournal.entries.front().executionResult.value("manual_review_required", false) &&
               recoveredJournal.entries.front().executionResult.value("resolution", std::string()) ==
                   "manual_review_required" &&
               recoveredJournal.entries.front().executionResult.value("broker_identity", json::object()).value("perm_id", 0LL) == 5301 &&
               recoveredJournal.entries.front().executionResult.value("trade_trace", json::object()).value("trace_id", 0ULL) != 0 &&
               recoveredJournal.journalStatus == tape_phase7::kExecutionJournalStatusFailed,
           "phase 7 runtime recovery sweep should escalate a stale missing order into manual-review failure");
    {
        const auto recoverySummary = tape_phase7::summarizeExecutionJournalRecovery(recoveredJournal);
        expect(!recoverySummary.recoveryRequired &&
                   recoverySummary.runtimeBackedSubmittedCount == 0,
               "phase 7 runtime recovery resolved journal should clear runtime recovery flags");
    }

    tape_phase7::ExecutionApplyArtifact recoveredApply;
    expect(tape_phase7::loadExecutionApplyArtifact({},
                                                   apply.applyArtifact.artifactId,
                                                   &recoveredApply,
                                                   &phase7Code,
                                                   &phase7Message),
           "phase 7 runtime recovery sweep should reload the recovered apply artifact: " + phase7Code + " " + phase7Message);
    expect(recoveredApply.entries.front().executionStatus == tape_phase7::kExecutionEntryStatusFailed &&
               recoveredApply.entries.front().terminal &&
               recoveredApply.entries.front().failureCode == "runtime_order_missing" &&
               recoveredApply.entries.front().executionResult.value("broker_identity", json::object()).value("perm_id", 0LL) == 5301 &&
               recoveredApply.applyStatus == tape_phase7::kExecutionJournalStatusFailed,
           "phase 7 runtime recovery sweep should mirror the stale missing-order failure into the apply artifact");
    {
        const auto recoverySummary = tape_phase7::summarizeExecutionApplyRecovery(recoveredApply);
        expect(!recoverySummary.recoveryRequired &&
                   recoverySummary.runtimeBackedSubmittedCount == 0,
               "phase 7 runtime recovery resolved apply should clear runtime recovery flags");
    }
    {
        tape_phase7::RuntimeRecoveryBacklogSummary backlog;
        expect(tape_phase7::summarizeRuntimeRecoveryBacklog(&backlog, &phase7Code, &phase7Message),
               "phase 7 runtime recovery backlog summary should succeed after stale sweep: " +
                   phase7Code + " " + phase7Message);
        expect(backlog.runtimeBackedSubmittedJournalEntryCount == 0 &&
                   backlog.runtimeBackedSubmittedApplyEntryCount == 0 &&
                   !backlog.recoveryRequired &&
                   !backlog.staleRecoveryRequired,
               "phase 7 runtime recovery backlog summary should clear after stale recovery sweep resolves");
        const auto clearedPlan = tape_phase7::runtimeStartupRecoveryPlan(backlog, true);
        expect(clearedPlan.startupAction == "none" &&
                   !clearedPlan.recoverySweepRecommended &&
                   !clearedPlan.manualAttentionRecommended,
               "phase 7 runtime recovery startup policy should clear after stale recovery sweep resolves");
    }

    server.stop();
    unbindSharedDataOwner(&owner);
    resetSharedDataForTesting();
}

void testTapePhase7RuntimeBridgeHandlesCancelAndRejectLifecycleStates() {
    clearTestFiles();
    std::error_code phase7Ec;
    fs::remove_all(testDataDir() / "phase7_artifacts", phase7Ec);

    SharedData owner;
    bindSharedDataOwner(&owner);

    auto makeArtifactRef = [&](const fs::path& root,
                               const std::string& artifactType,
                               const std::string& artifactId) {
        fs::create_directories(root, phase7Ec);
        return tape_phase7::ArtifactRef{
            .artifactType = artifactType,
            .contractVersion = tape_phase7::kContractVersion,
            .artifactId = artifactId,
            .manifestPath = (root / "manifest.json").string(),
            .artifactRootDir = root.string()
        };
    };

    auto seedRuntimeState = [&](OrderId orderId,
                                std::uint64_t traceId,
                                long long permId,
                                const std::string& execId,
                                LocalOrderState localState,
                                const std::string& status,
                                bool cancelPending,
                                const std::string& latestError,
                                double filledQty,
                                double remainingQty,
                                double avgFillPrice) {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);

        OrderInfo order;
        order.orderId = orderId;
        order.account = "U23154741";
        order.symbol = "INTC";
        order.side = "BUY";
        order.quantity = 30.0;
        order.limitPrice = 46.40;
        order.status = status;
        order.filledQty = filledQty;
        order.remainingQty = remainingQty;
        order.avgFillPrice = avgFillPrice;
        order.localState = localState;
        order.cancelPending = cancelPending;
        if (!execId.empty() && filledQty > 0.0) {
            order.seenExecIds.insert(execId);
        }
        g_data.orders[orderId] = order;

        TradeTrace trace;
        trace.traceId = traceId;
        trace.orderId = orderId;
        trace.permId = permId;
        trace.source = "phase7-runtime-lifecycle";
        trace.symbol = order.symbol;
        trace.side = order.side;
        trace.account = order.account;
        trace.requestedQty = static_cast<int>(order.quantity);
        trace.limitPrice = order.limitPrice;
        trace.latestStatus = status;
        trace.latestError = latestError;
        if (!execId.empty() && filledQty > 0.0) {
            FillSlice fill;
            fill.execId = execId;
            fill.shares = static_cast<int>(filledQty);
            fill.price = avgFillPrice > 0.0 ? avgFillPrice : order.limitPrice;
            fill.cumQty = filledQty;
            fill.avgPrice = avgFillPrice > 0.0 ? avgFillPrice : order.limitPrice;
            fill.exchange = "SIM";
            trace.fills.push_back(fill);
            g_data.traceIdByExecId[execId] = traceId;
        }
        g_data.traces[traceId] = trace;
        g_data.traceRecency.push_back(traceId);
        g_data.latestTraceId = traceId;
        g_data.traceIdByOrderId[orderId] = traceId;
        g_data.traceIdByPermId[permId] = traceId;
    };

    auto persistSubmittedRuntimeJournal = [&](const std::string& suffix,
                                              OrderId orderId,
                                              std::uint64_t traceId,
                                              long long permId,
                                              const std::string& startedAtUtc) {
        const fs::path phase7Root = testDataDir() / "phase7_artifacts";
        const std::string sourceArtifactId = "phase7-source-" + suffix;
        const std::string analysisArtifactId = "phase7-analysis-" + suffix;
        const std::string playbookArtifactId = "phase7-playbook-" + suffix;
        const std::string ledgerArtifactId = "phase7-ledger-" + suffix;
        const std::string journalArtifactId = "phase7-journal-" + suffix;
        tape_phase7::ExecutionJournalArtifact journal;
        journal.sourceArtifact = makeArtifactRef(phase7Root / "sources" / sourceArtifactId,
                                                 "phase6.case_bundle.v1",
                                                 sourceArtifactId);
        journal.analysisArtifact = makeArtifactRef(phase7Root / "analysis" / analysisArtifactId,
                                                   tape_phase7::kAnalysisArtifactType,
                                                   analysisArtifactId);
        journal.playbookArtifact = makeArtifactRef(phase7Root / "playbooks" / playbookArtifactId,
                                                   tape_phase7::kPlaybookArtifactType,
                                                   playbookArtifactId);
        journal.ledgerArtifact = makeArtifactRef(phase7Root / "ledgers" / ledgerArtifactId,
                                                 tape_phase7::kExecutionLedgerArtifactType,
                                                 ledgerArtifactId);
        journal.journalArtifact = makeArtifactRef(phase7Root / "journals" / journalArtifactId,
                                                  tape_phase7::kExecutionJournalArtifactType,
                                                  journalArtifactId);
        journal.mode = tape_phase7::kDefaultPlaybookMode;
        journal.generatedAtUtc = "2026-03-16T20:00:00.000Z";
        journal.initiatedBy = "bridge-executor";
        journal.executionCapability = "phase7.execution_operator.v1";
        journal.filteredFindingIds = {"phase7-finding:" + suffix};
        journal.replayContext = {
            {"trace_anchor",
             {
                 {"order_id", static_cast<long long>(orderId)},
                 {"trace_id", traceId},
                 {"perm_id", permId}
             }}
        };

        tape_phase7::ExecutionJournalEntry entry;
        entry.journalEntryId = "phase7-journal-entry:" + suffix;
        entry.ledgerEntryId = "phase7-ledger-entry:" + suffix;
        entry.actionId = "phase7-action:" + suffix;
        entry.actionType = "phase7.review_order_impact_footprint.v1";
        entry.findingId = "phase7-finding:" + suffix;
        entry.executionStatus = tape_phase7::kExecutionEntryStatusSubmitted;
        entry.idempotencyKey = "phase7-idempotency:" + suffix;
        entry.requiresManualConfirmation = true;
        entry.title = "Runtime lifecycle fixture";
        entry.summary = "Exercise runtime bridge reconciliation semantics without a full bundle pipeline.";
        entry.queuedAtUtc = "2026-03-16T19:59:59.000Z";
        entry.startedAtUtc = startedAtUtc;
        entry.lastUpdatedAtUtc = startedAtUtc;
        entry.lastUpdatedBy = "bridge-executor";
        entry.executionComment = "Submitted into the runtime bridge test fixture.";
        entry.attemptCount = 1;
        entry.executionRequest = {
            {"operation", "request_order_reconciliation"},
            {"executor", "trading_runtime"},
            {"execution_capability", "phase7.execution_operator.v1"},
            {"reason", entry.actionType},
            {"anchor",
             {
                 {"order_id", static_cast<long long>(orderId)},
                 {"trace_id", traceId},
                 {"perm_id", permId}
             }},
            {"requested_order_ids", json::array({static_cast<long long>(orderId)})}
        };
        entry.executionResult = {
            {"runtime_operation", "request_order_reconciliation"},
            {"requested_order_ids", json::array({static_cast<long long>(orderId)})},
            {"accepted_order_ids", json::array({static_cast<long long>(orderId)})},
            {"trace_id", traceId},
            {"broker_identity",
             {
                 {"order_id", static_cast<long long>(orderId)},
                 {"trace_id", traceId},
                 {"perm_id", permId}
             }}
        };
        journal.entries.push_back(std::move(entry));
        tape_phase7::annotateExecutionJournalArtifact(&journal);

        std::string phase7Code;
        std::string phase7Message;
        expect(tape_phase7::persistExecutionJournalArtifact(journal, &phase7Code, &phase7Message),
               "phase 7 runtime lifecycle fixture should persist the execution journal: " +
                   phase7Code + " " + phase7Message);
        return journal;
    };

    std::string phase7Code;
    std::string phase7Message;

    seedRuntimeState(9401,
                     99401,
                     5401,
                     "E-PHASE7-CANCEL-1",
                     LocalOrderState::Cancelled,
                     "Cancelled",
                     false,
                     {},
                     12.0,
                     18.0,
                     46.04);
    publishSharedDataSnapshot();

    const auto cancelJournal = persistSubmittedRuntimeJournal("cancel-runtime-resolution",
                                                              9401,
                                                              99401,
                                                              5401,
                                                              "2026-03-16T19:59:59.500Z");
    tape_phase7::ExecutionApplyArtifact cancelApply;
    bool cancelApplyCreated = false;
    expect(tape_phase7::startExecutionApply({},
                                            cancelJournal.journalArtifact.artifactId,
                                            {},
                                            "bridge-executor",
                                            "phase7.execution_operator.v1",
                                            "Create an apply artifact before cancel reconciliation.",
                                            &cancelApply,
                                            &cancelApplyCreated,
                                            &phase7Code,
                                            &phase7Message),
           "phase 7 runtime lifecycle cancel apply start should succeed: " +
               phase7Code + " " + phase7Message);
    expect(cancelApplyCreated,
           "phase 7 runtime lifecycle cancel apply should be created from the submitted journal");

    tape_phase7::ExecutionJournalArtifact reconciledCancelJournal;
    std::vector<std::string> updatedEntryIds;
    std::string auditEventId;
    expect(tape_phase7::reconcileExecutionJournalEntriesViaRuntime(nullptr,
                                                                   {},
                                                                   cancelJournal.journalArtifact.artifactId,
                                                                   {},
                                                                   "bridge-executor",
                                                                   "Runtime acknowledged the cancellation.",
                                                                   &reconciledCancelJournal,
                                                                   &updatedEntryIds,
                                                                   &auditEventId,
                                                                   &phase7Code,
                                                                   &phase7Message),
           "phase 7 runtime lifecycle cancel reconciliation should succeed: " +
               phase7Code + " " + phase7Message);
    expect(reconciledCancelJournal.entries.front().executionStatus == tape_phase7::kExecutionEntryStatusCancelled &&
               reconciledCancelJournal.entries.front().terminal &&
               reconciledCancelJournal.journalStatus == tape_phase7::kExecutionJournalStatusCancelled &&
               reconciledCancelJournal.entries.front().executionResult.value("resolution", std::string()) ==
                   "resolved_cancelled" &&
               reconciledCancelJournal.entries.front().executionResult.value("fill_state", std::string()) ==
                   "cancelled_after_partial_fill" &&
               reconciledCancelJournal.entries.front().executionResult.value("partial_fill_before_terminal", false) &&
               reconciledCancelJournal.entries.front().executionResult.value("restart_resume_policy", std::string()) ==
                   "completed" &&
               reconciledCancelJournal.entries.front().executionResult.value("latest_exec_id", std::string()) ==
                   "E-PHASE7-CANCEL-1" &&
               !reconciledCancelJournal.entries.front().executionResult.value("cancel_ack_pending", true) &&
               reconciledCancelJournal.entries.front().executionResult.value("broker_identity", json::object()).value("perm_id", 0LL) == 5401,
           "phase 7 runtime lifecycle cancel reconciliation should resolve to terminal cancelled with broker identity intact");

    tape_phase7::ExecutionApplyArtifact synchronizedCancelApply;
    std::vector<std::string> synchronizedApplyEntryIds;
    std::string synchronizedApplyAuditEventId;
    expect(tape_phase7::synchronizeExecutionApplyFromJournal({},
                                                             cancelApply.applyArtifact.artifactId,
                                                             "bridge-executor",
                                                             "Synchronize the apply from the cancelled journal.",
                                                             &synchronizedCancelApply,
                                                             &synchronizedApplyEntryIds,
                                                             &synchronizedApplyAuditEventId,
                                                             &phase7Code,
                                                             &phase7Message),
           "phase 7 runtime lifecycle cancel apply sync should succeed: " +
               phase7Code + " " + phase7Message);
    expect(synchronizedCancelApply.entries.front().executionStatus == tape_phase7::kExecutionEntryStatusCancelled &&
               synchronizedCancelApply.applyStatus == tape_phase7::kExecutionJournalStatusCancelled &&
               synchronizedCancelApply.entries.front().executionResult.value("fill_state", std::string()) ==
                   "cancelled_after_partial_fill" &&
               synchronizedCancelApply.entries.front().executionResult.value("restart_recovery_state", std::string()) ==
                   "terminal_completed" &&
               synchronizedCancelApply.entries.front().executionResult.value("restart_recovery_reason", std::string()) ==
                   "terminal_cancelled_after_partial_fill" &&
               synchronizedCancelApply.entries.front().executionResult.value("latest_exec_id", std::string()) ==
                   "E-PHASE7-CANCEL-1" &&
               synchronizedCancelApply.entries.front().executionResult.value("broker_identity", json::object()).value("perm_id", 0LL) == 5401,
           "phase 7 runtime lifecycle cancel apply should mirror the cancelled journal state");

    seedRuntimeState(9405,
                     99405,
                     5405,
                     "E-PHASE7-CANCEL-RESTART-1",
                     LocalOrderState::AwaitingCancelAck,
                     "Submitted",
                     true,
                     {},
                     9.0,
                     21.0,
                     46.09);
    publishSharedDataSnapshot();

    const auto cancelRestartJournal = persistSubmittedRuntimeJournal("cancel-restart-resolution",
                                                                     9405,
                                                                     99405,
                                                                     5405,
                                                                     "2099-03-16T19:59:59.550Z");
    tape_phase7::ExecutionJournalArtifact cancelPendingJournal;
    expect(tape_phase7::reconcileExecutionJournalEntriesViaRuntime(nullptr,
                                                                   {},
                                                                   cancelRestartJournal.journalArtifact.artifactId,
                                                                   {},
                                                                   "bridge-executor",
                                                                   "Runtime restart found the order waiting on a cancel acknowledgement.",
                                                                   &cancelPendingJournal,
                                                                   &updatedEntryIds,
                                                                   &auditEventId,
                                                                   &phase7Code,
                                                                   &phase7Message),
           "phase 7 runtime lifecycle cancel-pending reconciliation should succeed: " +
               phase7Code + " " + phase7Message);
    expect(cancelPendingJournal.entries.front().executionStatus == tape_phase7::kExecutionEntryStatusSubmitted &&
               !cancelPendingJournal.entries.front().terminal &&
               cancelPendingJournal.entries.front().executionResult.value("resolution", std::string()) ==
                   "cancel_ack_pending" &&
               cancelPendingJournal.entries.front().executionResult.value("fill_state", std::string()) ==
                   "partial_fill_cancel_pending" &&
               cancelPendingJournal.entries.front().executionResult.value("restart_resume_policy", std::string()) ==
                   "await_cancel_ack" &&
               cancelPendingJournal.entries.front().executionResult.value("restart_recovery_state", std::string()) ==
                   "awaiting_cancel_ack" &&
               cancelPendingJournal.entries.front().executionResult.value("restart_recovery_reason", std::string()) ==
                   "cancel_ack_pending_after_partial_fill",
           "phase 7 runtime lifecycle cancel-pending reconciliation should preserve the restart cancel-ack wait state");
    {
        const auto recoverySummary = tape_phase7::summarizeExecutionJournalRecovery(cancelPendingJournal);
        expect(recoverySummary.recoveryRequired &&
                   recoverySummary.runtimeBackedSubmittedCount == 1 &&
                   !recoverySummary.staleRecoveryRequired,
               "phase 7 runtime lifecycle cancel-pending reconciliation should stay recoverable before the cancel acknowledgement arrives");
    }

    tape_phase7::ExecutionApplyArtifact cancelRestartApply;
    bool cancelRestartApplyCreated = false;
    expect(tape_phase7::startExecutionApply({},
                                            cancelRestartJournal.journalArtifact.artifactId,
                                            {},
                                            "bridge-executor",
                                            "phase7.execution_operator.v1",
                                            "Create an apply artifact while the cancel acknowledgement is still pending.",
                                            &cancelRestartApply,
                                            &cancelRestartApplyCreated,
                                            &phase7Code,
                                            &phase7Message),
           "phase 7 runtime lifecycle cancel-pending apply start should succeed: " +
               phase7Code + " " + phase7Message);
    expect(cancelRestartApplyCreated,
           "phase 7 runtime lifecycle cancel-pending apply should be created from the submitted journal");

    tape_phase7::ExecutionJournalArtifact staleCancelPendingJournal;
    expect(tape_phase7::loadExecutionJournalArtifact({},
                                                     cancelRestartJournal.journalArtifact.artifactId,
                                                     &staleCancelPendingJournal,
                                                     &phase7Code,
                                                     &phase7Message),
           "phase 7 runtime lifecycle should reload the cancel-pending journal before the restart acknowledgement: " +
               phase7Code + " " + phase7Message);
    staleCancelPendingJournal.entries.front().startedAtUtc = "2000-01-01T00:00:00.000Z";
    staleCancelPendingJournal.entries.front().lastUpdatedAtUtc = "2000-01-01T00:00:00.000Z";
    expect(tape_phase7::persistExecutionJournalArtifact(staleCancelPendingJournal, &phase7Code, &phase7Message),
           "phase 7 runtime lifecycle should persist the stale cancel-pending journal before the restart acknowledgement: " +
               phase7Code + " " + phase7Message);

    seedRuntimeState(9405,
                     99405,
                     5405,
                     "E-PHASE7-CANCEL-RESTART-1",
                     LocalOrderState::Cancelled,
                     "Cancelled",
                     false,
                     "broker acknowledged the cancel after a partial fill",
                     9.0,
                     21.0,
                     46.09);
    publishSharedDataSnapshot();

    tape_phase7::ExecutionJournalArtifact reconciledCancelRestartJournal;
    expect(tape_phase7::reconcileExecutionJournalEntriesViaRuntime(nullptr,
                                                                   {},
                                                                   cancelRestartJournal.journalArtifact.artifactId,
                                                                   {},
                                                                   "bridge-executor",
                                                                   "Runtime restart recovered the final cancel acknowledgement.",
                                                                   &reconciledCancelRestartJournal,
                                                                   &updatedEntryIds,
                                                                   &auditEventId,
                                                                   &phase7Code,
                                                                   &phase7Message),
           "phase 7 runtime lifecycle cancel restart reconciliation should succeed: " +
               phase7Code + " " + phase7Message);
    expect(reconciledCancelRestartJournal.entries.front().executionStatus == tape_phase7::kExecutionEntryStatusCancelled &&
               reconciledCancelRestartJournal.entries.front().terminal &&
               reconciledCancelRestartJournal.entries.front().executionResult.value("resolution", std::string()) ==
                   "resolved_cancelled" &&
               reconciledCancelRestartJournal.entries.front().executionResult.value("fill_state", std::string()) ==
                   "cancelled_after_partial_fill" &&
               reconciledCancelRestartJournal.entries.front().executionResult.value("restart_resume_policy", std::string()) ==
                   "completed" &&
               reconciledCancelRestartJournal.entries.front().executionResult.value("restart_recovery_state", std::string()) ==
                   "terminal_completed" &&
               reconciledCancelRestartJournal.entries.front().executionResult.value("restart_recovery_reason", std::string()) ==
                   "terminal_cancelled_after_partial_fill" &&
               reconciledCancelRestartJournal.entries.front().executionResult.value("latest_exec_id", std::string()) ==
                   "E-PHASE7-CANCEL-RESTART-1" &&
               reconciledCancelRestartJournal.entries.front().executionResult.value("broker_status_detail", std::string()) ==
                   "broker acknowledged the cancel after a partial fill",
           "phase 7 runtime lifecycle cancel restart reconciliation should resolve a stale cancel wait once the broker acknowledgement arrives");

    tape_phase7::ExecutionApplyArtifact synchronizedCancelRestartApply;
    expect(tape_phase7::synchronizeExecutionApplyFromJournal({},
                                                             cancelRestartApply.applyArtifact.artifactId,
                                                             "bridge-executor",
                                                             "Synchronize the apply after the restart cancel acknowledgement resolved.",
                                                             &synchronizedCancelRestartApply,
                                                             &synchronizedApplyEntryIds,
                                                             &synchronizedApplyAuditEventId,
                                                             &phase7Code,
                                                             &phase7Message),
           "phase 7 runtime lifecycle cancel restart apply sync should succeed: " +
               phase7Code + " " + phase7Message);
    expect(synchronizedCancelRestartApply.entries.front().executionStatus == tape_phase7::kExecutionEntryStatusCancelled &&
               synchronizedCancelRestartApply.entries.front().executionResult.value("restart_recovery_state", std::string()) ==
                   "terminal_completed" &&
               synchronizedCancelRestartApply.entries.front().executionResult.value("restart_recovery_reason", std::string()) ==
                   "terminal_cancelled_after_partial_fill" &&
               synchronizedCancelRestartApply.entries.front().executionResult.value("latest_exec_id", std::string()) ==
                   "E-PHASE7-CANCEL-RESTART-1",
           "phase 7 runtime lifecycle cancel restart apply should mirror the resolved partial-cancel state");

    seedRuntimeState(9402,
                     99402,
                     5402,
                     "E-PHASE7-REJECT-1",
                     LocalOrderState::Rejected,
                     "Rejected",
                     false,
                     "broker rejected the order after validation",
                     0.0,
                     30.0,
                     0.0);
    publishSharedDataSnapshot();

    const auto rejectJournal = persistSubmittedRuntimeJournal("reject-runtime-resolution",
                                                              9402,
                                                              99402,
                                                              5402,
                                                              "2026-03-16T19:59:59.600Z");
    tape_phase7::ExecutionApplyArtifact rejectApply;
    bool rejectApplyCreated = false;
    expect(tape_phase7::startExecutionApply({},
                                            rejectJournal.journalArtifact.artifactId,
                                            {},
                                            "bridge-executor",
                                            "phase7.execution_operator.v1",
                                            "Create an apply artifact before reject reconciliation.",
                                            &rejectApply,
                                            &rejectApplyCreated,
                                            &phase7Code,
                                            &phase7Message),
           "phase 7 runtime lifecycle reject apply start should succeed: " +
               phase7Code + " " + phase7Message);
    expect(rejectApplyCreated,
           "phase 7 runtime lifecycle reject apply should be created from the submitted journal");

    tape_phase7::ExecutionJournalArtifact reconciledRejectJournal;
    expect(tape_phase7::reconcileExecutionJournalEntriesViaRuntime(nullptr,
                                                                   {},
                                                                   rejectJournal.journalArtifact.artifactId,
                                                                   {},
                                                                   "bridge-executor",
                                                                   "Runtime rejected the order.",
                                                                   &reconciledRejectJournal,
                                                                   &updatedEntryIds,
                                                                   &auditEventId,
                                                                   &phase7Code,
                                                                   &phase7Message),
           "phase 7 runtime lifecycle reject reconciliation should succeed: " +
               phase7Code + " " + phase7Message);
    expect(reconciledRejectJournal.entries.front().executionStatus == tape_phase7::kExecutionEntryStatusFailed &&
               reconciledRejectJournal.entries.front().terminal &&
               reconciledRejectJournal.entries.front().failureCode == "runtime_rejected" &&
               reconciledRejectJournal.entries.front().executionResult.value("resolution", std::string()) ==
                   "resolved_rejected" &&
               reconciledRejectJournal.entries.front().executionResult.value("fill_state", std::string()) ==
                   "rejected_unfilled" &&
               reconciledRejectJournal.entries.front().executionResult.value("restart_resume_policy", std::string()) ==
                   "terminal_no_resume" &&
               reconciledRejectJournal.entries.front().executionResult.value("restart_recovery_state", std::string()) ==
                   "terminal_no_resume" &&
               reconciledRejectJournal.entries.front().executionResult.value("restart_recovery_reason", std::string()) ==
                   "terminal_rejected_unfilled" &&
               reconciledRejectJournal.entries.front().executionResult.value("broker_status_detail", std::string()) ==
                   "broker rejected the order after validation" &&
               reconciledRejectJournal.entries.front().executionResult.value("broker_identity", json::object()).value("perm_id", 0LL) == 5402,
           "phase 7 runtime lifecycle reject reconciliation should resolve to terminal failure with broker identity intact");
    expectContains(reconciledRejectJournal.entries.front().failureMessage,
                   "broker rejected the order after validation",
                   "phase 7 runtime lifecycle reject reconciliation should preserve the latest broker detail");

    tape_phase7::ExecutionApplyArtifact synchronizedRejectApply;
    expect(tape_phase7::synchronizeExecutionApplyFromJournal({},
                                                             rejectApply.applyArtifact.artifactId,
                                                             "bridge-executor",
                                                             "Synchronize the apply from the rejected journal.",
                                                             &synchronizedRejectApply,
                                                             &synchronizedApplyEntryIds,
                                                             &synchronizedApplyAuditEventId,
                                                             &phase7Code,
                                                             &phase7Message),
           "phase 7 runtime lifecycle reject apply sync should succeed: " +
               phase7Code + " " + phase7Message);
    expect(synchronizedRejectApply.entries.front().executionStatus == tape_phase7::kExecutionEntryStatusFailed &&
               synchronizedRejectApply.entries.front().failureCode == "runtime_rejected" &&
               synchronizedRejectApply.entries.front().executionResult.value("restart_resume_policy", std::string()) ==
                   "terminal_no_resume" &&
               synchronizedRejectApply.entries.front().executionResult.value("restart_recovery_state", std::string()) ==
                   "terminal_no_resume" &&
               synchronizedRejectApply.entries.front().executionResult.value("restart_recovery_reason", std::string()) ==
                   "terminal_rejected_unfilled" &&
               synchronizedRejectApply.entries.front().executionResult.value("broker_status_detail", std::string()) ==
                   "broker rejected the order after validation",
           "phase 7 runtime lifecycle reject apply should mirror the rejected no-resume broker detail");

    seedRuntimeState(9404,
                     99404,
                     5404,
                     "E-PHASE7-INACTIVE-1",
                     LocalOrderState::Inactive,
                     "Inactive",
                     false,
                     "broker marked the order inactive after validation",
                     0.0,
                     30.0,
                     0.0);
    publishSharedDataSnapshot();

    const auto inactiveJournal = persistSubmittedRuntimeJournal("inactive-runtime-resolution",
                                                                9404,
                                                                99404,
                                                                5404,
                                                                "2026-03-16T19:59:59.650Z");
    tape_phase7::ExecutionApplyArtifact inactiveApply;
    bool inactiveApplyCreated = false;
    expect(tape_phase7::startExecutionApply({},
                                            inactiveJournal.journalArtifact.artifactId,
                                            {},
                                            "bridge-executor",
                                            "phase7.execution_operator.v1",
                                            "Create an apply artifact before inactive reconciliation.",
                                            &inactiveApply,
                                            &inactiveApplyCreated,
                                            &phase7Code,
                                            &phase7Message),
           "phase 7 runtime lifecycle inactive apply start should succeed: " +
               phase7Code + " " + phase7Message);
    expect(inactiveApplyCreated,
           "phase 7 runtime lifecycle inactive apply should be created from the submitted journal");

    tape_phase7::ExecutionJournalArtifact reconciledInactiveJournal;
    expect(tape_phase7::reconcileExecutionJournalEntriesViaRuntime(nullptr,
                                                                   {},
                                                                   inactiveJournal.journalArtifact.artifactId,
                                                                   {},
                                                                   "bridge-executor",
                                                                   "Runtime marked the order inactive.",
                                                                   &reconciledInactiveJournal,
                                                                   &updatedEntryIds,
                                                                   &auditEventId,
                                                                   &phase7Code,
                                                                   &phase7Message),
           "phase 7 runtime lifecycle inactive reconciliation should succeed: " +
               phase7Code + " " + phase7Message);
    expect(reconciledInactiveJournal.entries.front().executionStatus == tape_phase7::kExecutionEntryStatusFailed &&
               reconciledInactiveJournal.entries.front().terminal &&
               reconciledInactiveJournal.entries.front().failureCode == "runtime_inactive" &&
               reconciledInactiveJournal.entries.front().executionResult.value("resolution", std::string()) ==
                   "resolved_inactive" &&
               reconciledInactiveJournal.entries.front().executionResult.value("fill_state", std::string()) ==
                   "inactive_unfilled" &&
               reconciledInactiveJournal.entries.front().executionResult.value("restart_resume_policy", std::string()) ==
                   "terminal_no_resume" &&
               reconciledInactiveJournal.entries.front().executionResult.value("restart_recovery_state", std::string()) ==
                   "terminal_no_resume" &&
               reconciledInactiveJournal.entries.front().executionResult.value("restart_recovery_reason", std::string()) ==
                   "terminal_inactive_unfilled" &&
               reconciledInactiveJournal.entries.front().executionResult.value("broker_status_detail", std::string()) ==
                   "broker marked the order inactive after validation" &&
               reconciledInactiveJournal.entries.front().executionResult.value("broker_identity", json::object()).value("perm_id", 0LL) == 5404,
           "phase 7 runtime lifecycle inactive reconciliation should resolve to terminal inactive failure with broker detail intact");
    expectContains(reconciledInactiveJournal.entries.front().failureMessage,
                   "broker marked the order inactive after validation",
                   "phase 7 runtime lifecycle inactive reconciliation should preserve the latest inactive broker detail");

    tape_phase7::ExecutionApplyArtifact synchronizedInactiveApply;
    expect(tape_phase7::synchronizeExecutionApplyFromJournal({},
                                                             inactiveApply.applyArtifact.artifactId,
                                                             "bridge-executor",
                                                             "Synchronize the apply from the inactive journal.",
                                                             &synchronizedInactiveApply,
                                                             &synchronizedApplyEntryIds,
                                                             &synchronizedApplyAuditEventId,
                                                             &phase7Code,
                                                             &phase7Message),
           "phase 7 runtime lifecycle inactive apply sync should succeed: " +
               phase7Code + " " + phase7Message);
    expect(synchronizedInactiveApply.entries.front().executionStatus == tape_phase7::kExecutionEntryStatusFailed &&
               synchronizedInactiveApply.entries.front().failureCode == "runtime_inactive" &&
               synchronizedInactiveApply.entries.front().executionResult.value("restart_resume_policy", std::string()) ==
                   "terminal_no_resume" &&
               synchronizedInactiveApply.entries.front().executionResult.value("restart_recovery_state", std::string()) ==
                   "terminal_no_resume" &&
               synchronizedInactiveApply.entries.front().executionResult.value("restart_recovery_reason", std::string()) ==
                   "terminal_inactive_unfilled" &&
               synchronizedInactiveApply.entries.front().executionResult.value("broker_status_detail", std::string()) ==
                   "broker marked the order inactive after validation",
           "phase 7 runtime lifecycle inactive apply should mirror the inactive no-resume broker detail");

    seedRuntimeState(9403,
                     99403,
                     5403,
                     "E-PHASE7-CANCEL-STUCK-1",
                     LocalOrderState::AwaitingCancelAck,
                     "Submitted",
                     true,
                     {},
                     7.0,
                     23.0,
                     46.07);
    publishSharedDataSnapshot();

    const auto staleCancelJournal = persistSubmittedRuntimeJournal("cancel-ack-stale",
                                                                   9403,
                                                                   99403,
                                                                   5403,
                                                                   "2000-01-01T00:00:00.000Z");
    tape_phase7::ExecutionJournalArtifact reconciledStaleCancelJournal;
    expect(tape_phase7::reconcileExecutionJournalEntriesViaRuntime(nullptr,
                                                                   {},
                                                                   staleCancelJournal.journalArtifact.artifactId,
                                                                   {},
                                                                   "bridge-executor",
                                                                   "Cancel acknowledgement never arrived.",
                                                                   &reconciledStaleCancelJournal,
                                                                   &updatedEntryIds,
                                                                   &auditEventId,
                                                                   &phase7Code,
                                                                   &phase7Message),
           "phase 7 runtime lifecycle stale cancel reconciliation should succeed: " +
               phase7Code + " " + phase7Message);
    expect(reconciledStaleCancelJournal.entries.front().executionStatus == tape_phase7::kExecutionEntryStatusFailed &&
               reconciledStaleCancelJournal.entries.front().terminal &&
               reconciledStaleCancelJournal.entries.front().failureCode == "runtime_cancel_ack_stale" &&
               reconciledStaleCancelJournal.entries.front().executionResult.value("restart_recovery_state", std::string()) ==
                   "manual_review_required" &&
               reconciledStaleCancelJournal.entries.front().executionResult.value("restart_recovery_reason", std::string()) ==
                   "cancel_ack_timeout_after_partial_fill" &&
               reconciledStaleCancelJournal.entries.front().executionResult.value("manual_review_required", false) &&
               reconciledStaleCancelJournal.entries.front().executionResult.value("cancel_ack_pending", false) &&
               reconciledStaleCancelJournal.entries.front().executionResult.value("broker_identity", json::object()).value("perm_id", 0LL) == 5403,
           "phase 7 runtime lifecycle stale cancel reconciliation should escalate a stuck cancel acknowledgement into manual-review failure");
    expectContains(reconciledStaleCancelJournal.entries.front().failureMessage,
                   "cancellation acknowledgement",
                   "phase 7 runtime lifecycle stale cancel reconciliation should explain the cancel-ack timeout");

    const auto projectExecutionResult = [](const json& executionResult) {
        return json{
            {"resolution", executionResult.value("resolution", std::string())},
            {"fill_state", executionResult.value("fill_state", std::string())},
            {"restart_resume_policy", executionResult.value("restart_resume_policy", std::string())},
            {"restart_recovery_state", executionResult.value("restart_recovery_state", std::string())},
            {"restart_recovery_reason", executionResult.value("restart_recovery_reason", std::string())},
            {"partial_fill_before_terminal", executionResult.value("partial_fill_before_terminal", false)},
            {"cancel_ack_pending", executionResult.value("cancel_ack_pending", false)},
            {"latest_exec_id", executionResult.contains("latest_exec_id")
                                   ? executionResult.at("latest_exec_id")
                                   : json(nullptr)},
            {"broker_status_detail", executionResult.contains("broker_status_detail")
                                         ? executionResult.at("broker_status_detail")
                                         : json(nullptr)},
            {"broker_identity",
             {
                 {"order_id",
                  executionResult.value("broker_identity", json::object()).contains("order_id")
                      ? executionResult.value("broker_identity", json::object()).at("order_id")
                      : json(nullptr)},
                 {"trace_id",
                  executionResult.value("broker_identity", json::object()).contains("trace_id")
                      ? executionResult.value("broker_identity", json::object()).at("trace_id")
                      : json(nullptr)},
                 {"perm_id",
                  executionResult.value("broker_identity", json::object()).contains("perm_id")
                      ? executionResult.value("broker_identity", json::object()).at("perm_id")
                      : json(nullptr)},
                 {"latest_exec_id",
                  executionResult.value("broker_identity", json::object()).contains("latest_exec_id")
                      ? executionResult.value("broker_identity", json::object()).at("latest_exec_id")
                      : json(nullptr)},
                 {"exec_id_count",
                  executionResult.value("broker_identity", json::object()).value("exec_ids", json::array()).size()}
             }},
            {"trade_trace",
             {
                 {"trace_found", executionResult.value("trade_trace", json::object()).value("trace_found", false)},
                 {"fill_count", executionResult.value("trade_trace", json::object()).value("fill_count", 0ULL)},
                 {"latest_status",
                  executionResult.value("trade_trace", json::object()).contains("latest_status")
                      ? executionResult.value("trade_trace", json::object()).at("latest_status")
                      : json(nullptr)},
                 {"terminal_status",
                  executionResult.value("trade_trace", json::object()).contains("terminal_status")
                      ? executionResult.value("trade_trace", json::object()).at("terminal_status")
                      : json(nullptr)}
             }}
        };
    };

    const json bridgeLifecycleProjection = {
        {"cancel_terminal", projectExecutionResult(reconciledCancelJournal.entries.front().executionResult)},
        {"cancel_pending_restart_wait", projectExecutionResult(cancelPendingJournal.entries.front().executionResult)},
        {"cancel_pending_restart_resolved", projectExecutionResult(reconciledCancelRestartJournal.entries.front().executionResult)},
        {"cancel_pending_restart_apply_sync", projectExecutionResult(synchronizedCancelRestartApply.entries.front().executionResult)},
        {"reject_terminal", projectExecutionResult(reconciledRejectJournal.entries.front().executionResult)},
        {"inactive_terminal", projectExecutionResult(reconciledInactiveJournal.entries.front().executionResult)},
        {"stale_cancel_ack_timeout", projectExecutionResult(reconciledStaleCancelJournal.entries.front().executionResult)}
    };
    const json lifecycleFixture = readJsonFixture("phase7_execution_bridge_contracts.json");
    expect(bridgeLifecycleProjection == lifecycleFixture,
           "phase 7 runtime lifecycle bridge projection should match the golden fixture\nactual fixture:\n" +
               bridgeLifecycleProjection.dump(2));

    unbindSharedDataOwner(&owner);
    resetSharedDataForTesting();
}

void testTapeEnginePhase3InvestigationContractMatchesGoldenFixtures() {
    const fs::path rootDir = testDataDir() / "tape-engine-phase3-contract-fixtures";
    const fs::path socketPath = testDataDir() / "tape-engine-phase3-contract-fixtures.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "ib:conid:9301:STK:SMART:USD:INTC";
    config.ringCapacity = 32;

    tape_engine::Server server(config);
    std::string startError;
    expect(server.start(&startError), "tape-engine should start for contract fixture test: " + startError);

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-engine-phase3-contract-fixtures";
    options.runtimeSessionId = "runtime-engine-phase3-contract-fixtures";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;

    BridgeOutboxRecord orderIntent = makeBridgeRecord(8601, "order_intent", "WebSocket", "INTC", "BUY",
                                                      711, 3301, 0, "", "contract fixture anchor", "2026-03-15T09:42:10.100");
    orderIntent.instrumentId = "ib:conid:9301:STK:SMART:USD:INTC";
    options.batchSeq = 601;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({orderIntent}, options)), &error),
           "tape-engine should accept the contract fixture anchor batch: " + error);

    BridgeOutboxRecord bidTick = makeBridgeRecord(8602, "market_tick", "BrokerMarketData", "INTC", "BID",
                                                  0, 0, 0, "", "contract fixture bid", "2026-03-15T09:42:10.120");
    bidTick.instrumentId = "ib:conid:9301:STK:SMART:USD:INTC";
    bidTick.marketField = 1;
    bidTick.price = 45.20;

    BridgeOutboxRecord askTick = makeBridgeRecord(8603, "market_tick", "BrokerMarketData", "INTC", "ASK",
                                                  0, 0, 0, "", "contract fixture ask", "2026-03-15T09:42:10.130");
    askTick.instrumentId = "ib:conid:9301:STK:SMART:USD:INTC";
    askTick.marketField = 2;
    askTick.price = 45.21;

    options.batchSeq = 602;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({bidTick, askTick}, options)), &error),
           "tape-engine should accept the contract fixture seed batch: " + error);

    BridgeOutboxRecord widenedAsk = makeBridgeRecord(8604, "market_tick", "BrokerMarketData", "INTC", "ASK",
                                                     0, 0, 0, "", "contract fixture widened ask", "2026-03-15T09:42:10.150");
    widenedAsk.instrumentId = "ib:conid:9301:STK:SMART:USD:INTC";
    widenedAsk.marketField = 2;
    widenedAsk.price = 45.24;

    options.batchSeq = 603;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({widenedAsk}, options)), &error),
           "tape-engine should accept the contract fixture widening batch: " + error);

    waitUntil([&]() {
        const auto snapshot = server.snapshot();
        return snapshot.latestFrozenRevisionId >= 4 && snapshot.segments.size() >= 4;
    }, "tape-engine should freeze contract fixture batches");

    tape_engine::Client client(socketPath.string());
    tape_engine::QueryResponse response;

    waitUntil([&]() {
        tape_engine::QueryRequest readyRequest;
        readyRequest.requestId = "contract-order-case-ready";
        readyRequest.operation = "read_order_case";
        readyRequest.orderId = 3301;
        if (!client.query(readyRequest, &response, &error)) {
            return false;
        }
        return response.summary.value("related_finding_count", 0ULL) >= 4ULL &&
               response.summary.value("timeline_summary", json::object()).value("timeline_entry_count", 0ULL) >= 16ULL;
    }, "contract fixture setup should wait for deferred investigation findings to settle");

    tape_engine::QueryRequest incidentsRequest;
    incidentsRequest.requestId = "contract-read-incident-list";
    incidentsRequest.operation = "list_incidents";
    expect(client.query(incidentsRequest, &response, &error), "contract incident list query should succeed: " + error);
    expect(response.events.is_array() && !response.events.empty(), "contract fixture setup should create a ranked incident");
    const std::uint64_t logicalIncidentId = response.events.at(0).value("logical_incident_id", 0ULL);
    expect(logicalIncidentId > 0, "contract fixture setup should expose a logical incident id");

    tape_engine::QueryRequest sessionReportRequest;
    sessionReportRequest.requestId = "contract-scan-session-report";
    sessionReportRequest.operation = "scan_session_report";
    expect(client.query(sessionReportRequest, &response, &error), "contract session-report scan should succeed: " + error);
    const std::uint64_t sessionReportId = response.summary.value("session_report", json::object()).value("report_id", 0ULL);
    expect(sessionReportId > 0, "contract session report should persist a durable report id");
    const json sessionReportProjection = projectContractResponse(response);

    tape_engine::QueryRequest incidentRequest;
    incidentRequest.requestId = "contract-read-incident";
    incidentRequest.operation = "read_incident";
    incidentRequest.logicalIncidentId = logicalIncidentId;
    expect(client.query(incidentRequest, &response, &error), "contract incident read should succeed: " + error);
    const json incidentProjection = projectContractResponse(response);

    tape_engine::QueryRequest incidentReportRequest;
    incidentReportRequest.requestId = "contract-scan-incident-report";
    incidentReportRequest.operation = "scan_incident_report";
    incidentReportRequest.logicalIncidentId = logicalIncidentId;
    expect(client.query(incidentReportRequest, &response, &error), "contract incident-report scan should succeed: " + error);
    const json incidentReportProjection = projectContractResponse(response);

    tape_engine::QueryRequest orderCaseRequest;
    orderCaseRequest.requestId = "contract-read-order-case";
    orderCaseRequest.operation = "read_order_case";
    orderCaseRequest.orderId = 3301;
    expect(client.query(orderCaseRequest, &response, &error), "contract order-case read should succeed: " + error);
    const std::string orderCaseArtifactId = response.summary.value("artifact", json::object()).value("artifact_id", std::string());
    const json orderCaseProjection = projectContractResponse(response);

    tape_engine::QueryRequest orderCaseReportRequest;
    orderCaseReportRequest.requestId = "contract-scan-order-case-report";
    orderCaseReportRequest.operation = "scan_order_case_report";
    orderCaseReportRequest.orderId = 3301;
    expect(client.query(orderCaseReportRequest, &response, &error), "contract order-case report scan should succeed: " + error);
    const json orderCaseReportProjection = projectContractResponse(response);

    tape_engine::QueryRequest readArtifactRequest;
    readArtifactRequest.requestId = "contract-read-artifact";
    readArtifactRequest.operation = "read_artifact";
    readArtifactRequest.artifactId = "session-report:" + std::to_string(sessionReportId);
    expect(client.query(readArtifactRequest, &response, &error), "contract read_artifact query should succeed: " + error);
    const json readArtifactProjection = projectContractResponse(response);

    tape_engine::QueryRequest exportArtifactRequest;
    exportArtifactRequest.requestId = "contract-export-artifact";
    exportArtifactRequest.operation = "export_artifact";
    exportArtifactRequest.artifactId = orderCaseArtifactId;
    exportArtifactRequest.exportFormat = "json-bundle";
    expect(client.query(exportArtifactRequest, &response, &error), "contract export_artifact query should succeed: " + error);
    const json exportArtifactProjection = projectContractResponse(response);

    const json fixture = readJsonFixture("phase3_investigation_contracts.json");
    expect(sessionReportProjection == fixture.value("scan_session_report", json::object()),
           "session report contract projection should match the golden fixture\nactual:\n" + sessionReportProjection.dump(2));
    expect(incidentProjection == fixture.value("read_incident", json::object()),
           "incident contract projection should match the golden fixture\nactual:\n" + incidentProjection.dump(2));
    expect(incidentReportProjection == fixture.value("scan_incident_report", json::object()),
           "incident report contract projection should match the golden fixture\nactual:\n" + incidentReportProjection.dump(2));
    expect(orderCaseProjection == fixture.value("read_order_case", json::object()),
           "order-case contract projection should match the golden fixture\nactual:\n" + orderCaseProjection.dump(2));
    expect(orderCaseReportProjection == fixture.value("scan_order_case_report", json::object()),
           "order-case report contract projection should match the golden fixture\nactual:\n" + orderCaseReportProjection.dump(2));
    expect(readArtifactProjection == fixture.value("read_artifact_session_report", json::object()),
           "artifact-read contract projection should match the golden fixture\nactual:\n" + readArtifactProjection.dump(2));
    expect(exportArtifactProjection == fixture.value("export_artifact_order_case_bundle", json::object()),
           "artifact-export contract projection should match the golden fixture\nactual:\n" + exportArtifactProjection.dump(2));

    server.stop();
}

void testTapeEngineTypedTransportResultsMatchGoldenFixtures() {
    const fs::path rootDir = testDataDir() / "tape-engine-transport-contract-fixtures";
    const fs::path socketPath = testDataDir() / "tape-engine-transport-contract-fixtures.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "ib:conid:9501:STK:SMART:USD:INTC";
    config.ringCapacity = 32;

    tape_engine::Server server(config);
    std::string startError;
    expect(server.start(&startError), "tape-engine should start for transport contract fixture test: " + startError);

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-engine-transport-contract-fixtures";
    options.runtimeSessionId = "runtime-engine-transport-contract-fixtures";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;

    BridgeOutboxRecord orderIntent = makeBridgeRecord(8701, "order_intent", "WebSocket", "INTC", "BUY",
                                                      712, 3401, 0, "", "transport fixture anchor",
                                                      "2026-03-15T09:42:20.100");
    orderIntent.instrumentId = "ib:conid:9501:STK:SMART:USD:INTC";
    options.batchSeq = 701;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({orderIntent}, options)), &error),
           "tape-engine should accept the transport fixture anchor batch: " + error);

    BridgeOutboxRecord bidTick = makeBridgeRecord(8702, "market_tick", "BrokerMarketData", "INTC", "BID",
                                                  0, 0, 0, "", "transport fixture bid", "2026-03-15T09:42:20.120");
    bidTick.instrumentId = "ib:conid:9501:STK:SMART:USD:INTC";
    bidTick.marketField = 1;
    bidTick.price = 45.20;

    BridgeOutboxRecord askTick = makeBridgeRecord(8703, "market_tick", "BrokerMarketData", "INTC", "ASK",
                                                  0, 0, 0, "", "transport fixture ask", "2026-03-15T09:42:20.130");
    askTick.instrumentId = "ib:conid:9501:STK:SMART:USD:INTC";
    askTick.marketField = 2;
    askTick.price = 45.21;

    options.batchSeq = 702;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({bidTick, askTick}, options)), &error),
           "tape-engine should accept the transport fixture seed batch: " + error);

    BridgeOutboxRecord widenedAsk = makeBridgeRecord(8704, "market_tick", "BrokerMarketData", "INTC", "ASK",
                                                     0, 0, 0, "", "transport fixture widened ask",
                                                     "2026-03-15T09:42:20.150");
    widenedAsk.instrumentId = "ib:conid:9501:STK:SMART:USD:INTC";
    widenedAsk.marketField = 2;
    widenedAsk.price = 45.24;

    options.batchSeq = 703;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({widenedAsk}, options)), &error),
           "tape-engine should accept the transport fixture widening batch: " + error);

    waitUntil([&]() {
        const auto snapshot = server.snapshot();
        return snapshot.latestFrozenRevisionId >= 4 && snapshot.segments.size() >= 4;
    }, "tape-engine should freeze transport contract fixture batches");

    tape_engine::Client client(socketPath.string());
    tape_engine::QueryResponse response;

    waitUntil([&]() {
        tape_engine::QueryRequest readyRequest;
        readyRequest.requestId = "transport-order-case-ready";
        readyRequest.operation = "read_order_case";
        readyRequest.orderId = 3401;
        if (!client.query(readyRequest, &response, &error)) {
            return false;
        }
        return response.summary.value("related_finding_count", 0ULL) >= 4ULL &&
               response.summary.value("timeline_summary", json::object()).value("timeline_entry_count", 0ULL) >= 16ULL;
    }, "transport contract fixture setup should wait for deferred findings to settle");

    auto queryProjection = [&](tape_engine::QueryRequest request,
                               const std::string& message) -> json {
        expect(client.query(request, &response, &error), message + ": " + error);
        return projectTransportResult(response);
    };

    tape_engine::QueryRequest statusRequest;
    statusRequest.requestId = "transport-status";
    statusRequest.operation = "status";
    const json statusProjection = queryProjection(statusRequest, "transport status query should succeed");

    tape_engine::QueryRequest rangeRequest;
    rangeRequest.requestId = "transport-read-range";
    rangeRequest.operation = "read_range";
    rangeRequest.fromSessionSeq = 1;
    rangeRequest.toSessionSeq = 4;
    const json rangeProjection = queryProjection(rangeRequest, "transport read_range query should succeed");

    tape_engine::QueryRequest qualityRequest;
    qualityRequest.requestId = "transport-read-session-quality";
    qualityRequest.operation = "read_session_quality";
    qualityRequest.fromSessionSeq = 1;
    qualityRequest.toSessionSeq = 4;
    const json sessionQualityProjection = queryProjection(qualityRequest, "transport session quality query should succeed");

    tape_engine::QueryRequest sessionReportRequest;
    sessionReportRequest.requestId = "transport-scan-session-report";
    sessionReportRequest.operation = "scan_session_report";
    expect(client.query(sessionReportRequest, &response, &error),
           "transport session report scan should succeed: " + error);
    const std::uint64_t sessionReportId = response.summary.value("session_report", json::object()).value("report_id", 0ULL);
    expect(sessionReportId > 0, "transport session report scan should persist a durable report id");
    const json sessionReportProjection = projectTransportResult(response);

    tape_engine::QueryRequest listAnchorsRequest;
    listAnchorsRequest.requestId = "transport-list-order-anchors";
    listAnchorsRequest.operation = "list_order_anchors";
    const json listAnchorsProjection = queryProjection(listAnchorsRequest, "transport list_order_anchors query should succeed");

    tape_engine::QueryRequest seekOrderRequest;
    seekOrderRequest.requestId = "transport-seek-order-anchor";
    seekOrderRequest.operation = "seek_order_anchor";
    seekOrderRequest.orderId = 3401;
    const json seekOrderProjection = queryProjection(seekOrderRequest, "transport seek_order_anchor query should succeed");

    tape_engine::QueryRequest replayRequest;
    replayRequest.requestId = "transport-replay-snapshot";
    replayRequest.operation = "replay_snapshot";
    replayRequest.targetSessionSeq = 4;
    const json replayProjection = queryProjection(replayRequest, "transport replay_snapshot query should succeed");

    tape_engine::QueryRequest exportArtifactRequest;
    exportArtifactRequest.requestId = "transport-export-artifact";
    exportArtifactRequest.operation = "export_artifact";
    exportArtifactRequest.artifactId = "session-report:" + std::to_string(sessionReportId);
    exportArtifactRequest.exportFormat = "markdown";
    const json exportArtifactProjection = queryProjection(exportArtifactRequest, "transport export_artifact query should succeed");

    tape_engine::QueryRequest exportSessionBundleRequest;
    exportSessionBundleRequest.requestId = "transport-export-session-bundle";
    exportSessionBundleRequest.operation = "export_session_bundle";
    exportSessionBundleRequest.reportId = sessionReportId;
    expect(client.query(exportSessionBundleRequest, &response, &error),
           "transport export_session_bundle query should succeed: " + error);
    const std::string sessionBundlePath =
        response.summary.value("bundle", json::object()).value("bundle_path", std::string());
    expect(!sessionBundlePath.empty(), "transport session bundle export should return a bundle path");
    const json exportSessionBundleProjection = projectTransportResult(response);

    tape_engine::QueryRequest verifySessionBundleRequest;
    verifySessionBundleRequest.requestId = "transport-verify-session-bundle";
    verifySessionBundleRequest.operation = "verify_bundle";
    verifySessionBundleRequest.bundlePath = sessionBundlePath;
    const json verifySessionBundleProjection = queryProjection(verifySessionBundleRequest,
                                                               "transport verify session bundle query should succeed");

    tape_engine::QueryRequest caseReportRequest;
    caseReportRequest.requestId = "transport-scan-order-case-report";
    caseReportRequest.operation = "scan_order_case_report";
    caseReportRequest.orderId = 3401;
    expect(client.query(caseReportRequest, &response, &error),
           "transport order-case report scan should succeed: " + error);
    const std::uint64_t caseReportId = response.summary.value("case_report_artifact", json::object()).value("report_id", 0ULL);
    expect(caseReportId > 0, "transport order-case report scan should persist a durable report id");

    tape_engine::QueryRequest exportCaseBundleRequest;
    exportCaseBundleRequest.requestId = "transport-export-case-bundle";
    exportCaseBundleRequest.operation = "export_case_bundle";
    exportCaseBundleRequest.reportId = caseReportId;
    expect(client.query(exportCaseBundleRequest, &response, &error),
           "transport export_case_bundle query should succeed: " + error);
    const std::string caseBundlePath =
        response.summary.value("bundle", json::object()).value("bundle_path", std::string());
    expect(!caseBundlePath.empty(), "transport case bundle export should return a bundle path");
    const json exportCaseBundleProjection = projectTransportResult(response);

    tape_engine::QueryRequest verifyCaseBundleRequest;
    verifyCaseBundleRequest.requestId = "transport-verify-case-bundle";
    verifyCaseBundleRequest.operation = "verify_bundle";
    verifyCaseBundleRequest.bundlePath = caseBundlePath;
    const json verifyCaseBundleProjection = queryProjection(verifyCaseBundleRequest,
                                                            "transport verify case bundle query should succeed");

    tape_engine::QueryRequest importCaseBundleRequest;
    importCaseBundleRequest.requestId = "transport-import-case-bundle";
    importCaseBundleRequest.operation = "import_case_bundle";
    importCaseBundleRequest.bundlePath = caseBundlePath;
    const json importCaseBundleProjection = queryProjection(importCaseBundleRequest,
                                                            "transport import_case_bundle query should succeed");

    tape_engine::QueryRequest listImportedCasesRequest;
    listImportedCasesRequest.requestId = "transport-list-imported-cases";
    listImportedCasesRequest.operation = "list_imported_cases";
    const json listImportedCasesProjection = queryProjection(listImportedCasesRequest,
                                                             "transport list_imported_cases query should succeed");

    const json actualFixture = {
        {"status", statusProjection},
        {"read_range", rangeProjection},
        {"read_session_quality", sessionQualityProjection},
        {"scan_session_report", sessionReportProjection},
        {"list_order_anchors", listAnchorsProjection},
        {"seek_order_anchor", seekOrderProjection},
        {"replay_snapshot", replayProjection},
        {"export_artifact", exportArtifactProjection},
        {"export_session_bundle", exportSessionBundleProjection},
        {"verify_session_bundle", verifySessionBundleProjection},
        {"export_case_bundle", exportCaseBundleProjection},
        {"verify_case_bundle", verifyCaseBundleProjection},
        {"import_case_bundle", importCaseBundleProjection},
        {"list_imported_cases", listImportedCasesProjection}
    };
    const json fixture = readJsonFixture("phase6_engine_transport_contracts.json");
    expect(statusProjection == fixture.value("status", json::object()),
           "status transport result should match the golden fixture\nactual fixture:\n" + actualFixture.dump(2));
    expect(rangeProjection == fixture.value("read_range", json::object()),
           "read_range transport result should match the golden fixture\nactual fixture:\n" + actualFixture.dump(2));
    expect(sessionQualityProjection == fixture.value("read_session_quality", json::object()),
           "read_session_quality transport result should match the golden fixture\nactual fixture:\n" + actualFixture.dump(2));
    expect(sessionReportProjection == fixture.value("scan_session_report", json::object()),
           "scan_session_report transport result should match the golden fixture\nactual fixture:\n" + actualFixture.dump(2));
    expect(listAnchorsProjection == fixture.value("list_order_anchors", json::object()),
           "list_order_anchors transport result should match the golden fixture\nactual fixture:\n" + actualFixture.dump(2));
    expect(seekOrderProjection == fixture.value("seek_order_anchor", json::object()),
           "seek_order_anchor transport result should match the golden fixture\nactual fixture:\n" + actualFixture.dump(2));
    expect(replayProjection == fixture.value("replay_snapshot", json::object()),
           "replay_snapshot transport result should match the golden fixture\nactual fixture:\n" + actualFixture.dump(2));
    expect(exportArtifactProjection == fixture.value("export_artifact", json::object()),
           "export_artifact transport result should match the golden fixture\nactual fixture:\n" + actualFixture.dump(2));
    expect(exportSessionBundleProjection == fixture.value("export_session_bundle", json::object()),
           "export_session_bundle transport result should match the golden fixture\nactual fixture:\n" + actualFixture.dump(2));
    expect(verifySessionBundleProjection == fixture.value("verify_session_bundle", json::object()),
           "verify_session_bundle transport result should match the golden fixture\nactual fixture:\n" + actualFixture.dump(2));
    expect(exportCaseBundleProjection == fixture.value("export_case_bundle", json::object()),
           "export_case_bundle transport result should match the golden fixture\nactual fixture:\n" + actualFixture.dump(2));
    expect(verifyCaseBundleProjection == fixture.value("verify_case_bundle", json::object()),
           "verify_case_bundle transport result should match the golden fixture\nactual fixture:\n" + actualFixture.dump(2));
    expect(importCaseBundleProjection == fixture.value("import_case_bundle", json::object()),
           "import_case_bundle transport result should match the golden fixture\nactual fixture:\n" + actualFixture.dump(2));
    expect(listImportedCasesProjection == fixture.value("list_imported_cases", json::object()),
           "list_imported_cases transport result should match the golden fixture\nactual fixture:\n" + actualFixture.dump(2));

    server.stop();
}

void testTapeEnginePrefersStrongConfiguredInstrumentIdentityAndMarksHeuristicFallback() {
    const fs::path rootDir = testDataDir() / "tape-engine-identity-hardening";
    const fs::path socketPath = testDataDir() / "tape-engine-identity-hardening.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "ib:conid:9401:STK:SMART:USD:INTC";
    config.ringCapacity = 32;

    tape_engine::Server server(config);
    std::string startError;
    expect(server.start(&startError), "tape-engine should start for identity hardening test: " + startError);

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-engine-identity-hardening";
    options.runtimeSessionId = "runtime-engine-identity-hardening";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;

    BridgeOutboxRecord configuredMatch = makeBridgeRecord(8401, "order_intent", "WebSocket", "INTC", "BUY",
                                                          611, 2401, 0, "", "identity from configured conid", "2026-03-14T09:42:40.100");
    configuredMatch.instrumentId.clear();
    options.batchSeq = 401;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({configuredMatch}, options)), &error),
           "tape-engine should accept the configured-identity batch: " + error);

    BridgeOutboxRecord heuristicFallback = makeBridgeRecord(8402, "order_intent", "WebSocket", "AAPL", "BUY",
                                                            612, 2402, 0, "", "identity heuristic fallback", "2026-03-14T09:42:40.200");
    heuristicFallback.instrumentId.clear();
    options.batchSeq = 402;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({heuristicFallback}, options)), &error),
           "tape-engine should accept the heuristic-fallback batch: " + error);

    BridgeOutboxRecord mismatchedStrong = makeBridgeRecord(8403, "order_intent", "WebSocket", "AAPL", "BUY",
                                                           613, 2403, 0, "", "identity mismatch", "2026-03-14T09:42:40.300");
    mismatchedStrong.instrumentId = "ib:conid:9402:STK:SMART:USD:MSFT";
    options.batchSeq = 403;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({mismatchedStrong}, options)), &error),
           "tape-engine should accept the mismatched strong-identity batch: " + error);

    waitUntil([&]() {
        const auto snapshot = server.snapshot();
        return snapshot.latestFrozenRevisionId >= 3 && snapshot.segments.size() >= 3;
    }, "tape-engine should freeze identity hardening batches");

    tape_engine::Client client(socketPath.string());
    tape_engine::QueryResponse response;

    tape_engine::QueryRequest rangeRequest;
    rangeRequest.requestId = "identity-range";
    rangeRequest.operation = "read_range";
    rangeRequest.fromSessionSeq = 1;
    rangeRequest.toSessionSeq = 3;
    expect(client.query(rangeRequest, &response, &error), "identity hardening range query should succeed: " + error);
    expect(response.events.size() == 3, "identity hardening range query should return all identity test events");
    expect(response.events.at(0).value("instrument_id", std::string()) == "ib:conid:9401:STK:SMART:USD:INTC",
           "matching symbol records should reuse the strong configured instrument identity");
    expect(response.events.at(0).value("instrument_identity_strength", std::string()) == "strong",
           "matching symbol records should surface strong instrument identity strength");
    expect(response.events.at(1).value("instrument_id", std::string()) == "ib:heuristic:STK:SMART:USD:AAPL",
           "mismatched symbol records should fall back to an explicit heuristic identity");
    expect(response.events.at(1).value("instrument_identity_strength", std::string()) == "heuristic",
           "heuristic fallback identity should be surfaced explicitly");
    expect(response.events.at(2).value("instrument_id", std::string()) == "ib:heuristic:STK:SMART:USD:AAPL",
           "mismatched strong identities should be coerced away from broker-supplied strong IDs when the symbol does not match");
    expect(response.events.at(2).value("source_instrument_id", std::string()) == "ib:conid:9402:STK:SMART:USD:MSFT",
           "mismatched strong identities should preserve the original broker-supplied instrument id as source evidence");
    expect(response.events.at(2).value("instrument_identity_status", std::string()) == "mismatch",
           "mismatched strong identities should be surfaced explicitly as mismatches");
    expect(response.events.at(2).value("instrument_identity_policy", std::string()) == "coerced_from_mismatch",
           "mismatched strong identities should surface the coercion policy that kept canonical evidence consistent");
    expect(response.events.at(2).value("source_instrument_identity_strength", std::string()) == "strong",
           "mismatched strong identities should preserve the original source identity strength");

    tape_engine::QueryRequest qualityRequest;
    qualityRequest.requestId = "identity-quality";
    qualityRequest.operation = "read_session_quality";
    qualityRequest.fromSessionSeq = 1;
    qualityRequest.toSessionSeq = 3;
    expect(client.query(qualityRequest, &response, &error), "identity hardening quality query should succeed: " + error);
    expect(response.summary.value("data_quality", json::object()).value("heuristic_instrument_identity_count", 0ULL) == 2ULL,
           "data-quality scoring should count both heuristic fallback and coerced mismatch resolution as heuristic evidence");
    expect(response.summary.value("data_quality", json::object()).value("strong_instrument_identity_count", 0ULL) == 1ULL,
           "data-quality scoring should only count resolved strong identities as strong evidence");
    expect(response.summary.value("data_quality", json::object()).value("source_strong_instrument_identity_count", 0ULL) == 1ULL,
           "data-quality scoring should still report how many broker-supplied source identities were strong-form");
    expect(response.summary.value("data_quality", json::object()).value("mismatched_instrument_identity_count", 0ULL) == 1ULL,
           "data-quality scoring should count mismatched strong identities separately");
    expect(response.summary.value("data_quality", json::object()).value("identity_policy_override_count", 0ULL) == 1ULL,
           "data-quality scoring should count identity coercions when the engine overrides mismatched source identities");

    server.stop();
}

void testTapeEngineCanRejectMismatchedStrongInstrumentIdsInStrictMode() {
    const fs::path rootDir = testDataDir() / "tape-engine-identity-strict";
    const fs::path socketPath = testDataDir() / "tape-engine-identity-strict.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "ib:conid:9401:STK:SMART:USD:INTC";
    config.ringCapacity = 32;
    config.rejectMismatchedStrongInstrumentIds = true;

    tape_engine::Server server(config);
    std::string startError;
    expect(server.start(&startError), "tape-engine should start for strict identity policy test: " + startError);

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-engine-identity-strict";
    options.runtimeSessionId = "runtime-engine-identity-strict";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;

    BridgeOutboxRecord mismatchedStrong = makeBridgeRecord(8501, "order_intent", "WebSocket", "AAPL", "BUY",
                                                           614, 2501, 0, "", "strict identity mismatch", "2026-03-15T09:42:40.300");
    mismatchedStrong.instrumentId = "ib:conid:9502:STK:SMART:USD:MSFT";
    options.batchSeq = 501;
    expect(!transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({mismatchedStrong}, options)), &error),
           "strict identity mode should reject mismatched strong instrument ids at ingest");
    expectContains(error, "mismatched strong instrument_id",
                   "strict identity mode should explain the rejected mismatched strong instrument id");

    waitUntil([&]() {
        const auto snapshot = server.snapshot();
        return snapshot.latestFrozenRevisionId == 0 && snapshot.segments.empty();
    }, "strict identity mode should not freeze a rejected mismatched batch");

    server.stop();
}
