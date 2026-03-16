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

    std::vector<std::string> findingIds;
    for (const auto& finding : firstAnalysis.findings) {
        findingIds.push_back(finding.findingId);
    }
    expect(!findingIds.empty(), "phase 7 analyzer should emit findings for the reusable playbook test");

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
