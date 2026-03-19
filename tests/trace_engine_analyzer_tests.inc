void testTapeEnginePhase3CollapsesRepeatedFindingsIntoRankedIncidents() {
    const fs::path rootDir = testDataDir() / "tape-engine-phase3-incidents";
    const fs::path socketPath = testDataDir() / "tape-engine-phase3-incidents.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "ib:conid:9201:STK:SMART:USD:INTC";
    config.ringCapacity = 32;

    tape_engine::Server server(config);
    std::string startError;
    expect(server.start(&startError), "tape-engine should start for incident collapse test: " + startError);

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-engine-phase3-incidents";
    options.runtimeSessionId = "runtime-engine-phase3-incidents";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;

    BridgeOutboxRecord orderIntent = makeBridgeRecord(8201, "order_intent", "WebSocket", "INTC", "BUY",
                                                      411, 3101, 0, "", "incident collapse anchor", "2026-03-14T09:42:00.100");
    orderIntent.instrumentId = "ib:conid:9201:STK:SMART:USD:INTC";
    options.batchSeq = 111;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({orderIntent}, options)), &error),
           "tape-engine should accept the incident collapse anchor batch: " + error);

    BridgeOutboxRecord bidTick = makeBridgeRecord(8202, "market_tick", "BrokerMarketData", "INTC", "BID",
                                                  0, 0, 0, "", "collapse bid", "2026-03-14T09:42:00.120");
    bidTick.instrumentId = "ib:conid:9201:STK:SMART:USD:INTC";
    bidTick.marketField = 1;
    bidTick.price = 45.30;

    BridgeOutboxRecord askTick = makeBridgeRecord(8203, "market_tick", "BrokerMarketData", "INTC", "ASK",
                                                  0, 0, 0, "", "collapse ask", "2026-03-14T09:42:00.130");
    askTick.instrumentId = "ib:conid:9201:STK:SMART:USD:INTC";
    askTick.marketField = 2;
    askTick.price = 45.31;

    options.batchSeq = 112;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({bidTick, askTick}, options)), &error),
           "tape-engine should accept the incident collapse seed market batch: " + error);

    BridgeOutboxRecord widenedAskOne = makeBridgeRecord(8204, "market_tick", "BrokerMarketData", "INTC", "ASK",
                                                        0, 0, 0, "", "collapse widened ask one", "2026-03-14T09:42:00.140");
    widenedAskOne.instrumentId = "ib:conid:9201:STK:SMART:USD:INTC";
    widenedAskOne.marketField = 2;
    widenedAskOne.price = 45.33;

    options.batchSeq = 113;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({widenedAskOne}, options)), &error),
           "tape-engine should accept the first widening batch: " + error);

    BridgeOutboxRecord widenedAskTwo = makeBridgeRecord(8205, "market_tick", "BrokerMarketData", "INTC", "ASK",
                                                        0, 0, 0, "", "collapse widened ask two", "2026-03-14T09:42:00.150");
    widenedAskTwo.instrumentId = "ib:conid:9201:STK:SMART:USD:INTC";
    widenedAskTwo.marketField = 2;
    widenedAskTwo.price = 45.35;

    options.batchSeq = 114;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({widenedAskTwo}, options)), &error),
           "tape-engine should accept the second widening batch: " + error);

    BridgeOutboxRecord widenedAskThree = makeBridgeRecord(8206, "market_tick", "BrokerMarketData", "INTC", "ASK",
                                                          0, 0, 0, "", "collapse widened ask three", "2026-03-14T09:42:06.250");
    widenedAskThree.instrumentId = "ib:conid:9201:STK:SMART:USD:INTC";
    widenedAskThree.marketField = 2;
    widenedAskThree.price = 45.37;

    options.batchSeq = 115;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({widenedAskThree}, options)), &error),
           "tape-engine should accept the third widening batch after the merge window: " + error);

    waitUntil([&]() {
        const auto snapshot = server.snapshot();
        return snapshot.latestFrozenRevisionId >= 5 && snapshot.segments.size() >= 5;
    }, "tape-engine should freeze repeated spread widening batches");

    tape_engine::Client client(socketPath.string());
    tape_engine::QueryResponse response;

    tape_engine::QueryRequest findingsRequest;
    findingsRequest.requestId = "findings-phase3-collapse";
    findingsRequest.operation = "list_findings";
    expect(client.query(findingsRequest, &response, &error), "phase 3 collapse findings query should succeed: " + error);
    expect(response.events.is_array() && response.events.size() >= 3, "phase 3 collapse should retain multiple spread findings");
    std::size_t spreadFindingCount = 0;
    for (const auto& item : response.events) {
        if (item.value("kind", std::string()) == "spread_widened") {
            ++spreadFindingCount;
        }
    }
    expect(spreadFindingCount >= 3, "phase 3 collapse should retain all spread-widened findings even when richer analyzers add newer findings");

    tape_engine::QueryRequest incidentsRequest;
    incidentsRequest.requestId = "incidents-phase3-collapse";
    incidentsRequest.operation = "list_incidents";
    expect(client.query(incidentsRequest, &response, &error), "phase 3 collapse incidents query should succeed: " + error);
    expect(response.events.is_array() && !response.events.empty(), "phase 3 collapse should surface ranked logical incidents");
    expect(response.summary.value("collapsed_logical_incidents", false), "phase 3 collapse should report logical incident collapsing");
    json collapsedSpreadIncident = json::object();
    for (const auto& item : response.events) {
        if (item.value("kind", std::string()) == "spread_widened") {
            collapsedSpreadIncident = item;
            break;
        }
    }
    expect(!collapsedSpreadIncident.is_null() && !collapsedSpreadIncident.empty(), "phase 3 collapse should preserve a spread_widened logical incident");
    expect(collapsedSpreadIncident.value("finding_count", 0ULL) == 3ULL, "collapsed spread incident should count all repeated findings");
    expect(collapsedSpreadIncident.value("score", 0.0) > 3.0, "collapsed spread incident should accumulate a higher incident score");
    expect(collapsedSpreadIncident.value("overlaps_order", false), "collapsed spread incident should preserve order overlap");

    const std::uint64_t logicalIncidentId = collapsedSpreadIncident.value("logical_incident_id", 0ULL);
    expect(logicalIncidentId > 0, "collapsed incident should expose a logical incident id for drilldown");

    tape_engine::QueryRequest readIncidentRequest;
    readIncidentRequest.requestId = "incident-phase3-collapse";
    readIncidentRequest.operation = "read_incident";
    readIncidentRequest.logicalIncidentId = logicalIncidentId;
    expect(client.query(readIncidentRequest, &response, &error), "phase 3 collapse incident read should succeed: " + error);
    expect(response.summary.value("logical_incident_id", 0ULL) == logicalIncidentId, "incident drilldown should preserve the requested logical incident id");
    expect(response.summary.value("incident_revision_count", 0ULL) == 3ULL, "incident drilldown should report all incident revisions");
    expect(response.summary.value("related_finding_count", 0ULL) == 3ULL, "incident drilldown should report all related findings");
    expect(response.summary.contains("score_breakdown"), "incident drilldown should include a score breakdown");
    expect(response.summary.value("score_breakdown", json::object()).contains("data_quality_penalty_factor"),
           "incident drilldown score breakdown should expose the data-quality penalty factor");
    expect(response.summary.contains("why_it_matters"), "incident drilldown should include a why-it-matters summary");
    expect(response.summary.contains("protected_window"), "incident drilldown should include the incident protected window");
    expect(response.summary.contains("data_quality"), "incident drilldown should include data-quality scoring");
    expect(response.summary.contains("timeline"), "incident drilldown should now include a merged investigation timeline");
    expect(response.summary.contains("timeline_summary"), "incident drilldown should summarize the merged investigation timeline");
    expect(response.summary.value("timeline_summary", json::object()).value("incident_count", 0ULL) >= 1ULL,
           "incident drilldown timeline summary should include at least the incident entry");
    expect(response.summary.value("report_summary", json::object()).contains("timeline_highlights"),
           "incident drilldown report summary should include timeline highlights");
    expect(response.events.is_array() && response.events.size() == 3, "incident drilldown should return the related findings as evidence");
    expect(response.events.at(0).value("logical_incident_id", 0ULL) == logicalIncidentId, "incident drilldown findings should be linked to the logical incident");
    const json incidentWindow = response.summary.value("protected_window", json::object());
    expect(incidentWindow.value("first_session_seq", 0ULL) < incidentWindow.value("last_session_seq", 0ULL),
           "incident protected window should widen its session-seq bounds across repeated promotions");

    tape_engine::QueryRequest windowsRequest;
    windowsRequest.requestId = "windows-phase3-collapse";
    windowsRequest.operation = "list_protected_windows";
    expect(client.query(windowsRequest, &response, &error), "phase 3 collapse windows query should succeed: " + error);
    std::set<std::uint64_t> incidentWindowIds;
    std::size_t incidentWindowRevisionCount = 0;
    for (const auto& item : response.events) {
        if (item.value("reason", std::string()) != "incident_promotion") {
            continue;
        }
        if (item.value("logical_incident_id", 0ULL) != logicalIncidentId) {
            continue;
        }
        incidentWindowIds.insert(item.value("window_id", 0ULL));
        ++incidentWindowRevisionCount;
    }
    expect(incidentWindowIds.size() == 1, "repeated promotions should keep one stable incident protected-window identity");
    expect(incidentWindowRevisionCount >= 2, "stable incident protected window should accumulate revisions as evidence widens");

    server.stop();
}

void testTapeEnginePhase3DetectsInsideLiquiditySignals() {
    const fs::path rootDir = testDataDir() / "tape-engine-phase3-liquidity";
    const fs::path socketPath = testDataDir() / "tape-engine-phase3-liquidity.sock";
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
    expect(server.start(&startError), "tape-engine should start for liquidity signal test: " + startError);

    auto marketRecord = [](std::uint64_t sourceSeq,
                           const std::string& recordType,
                           const std::string& side,
                           int marketField,
                           int bookPosition,
                           int bookOperation,
                           int bookSide,
                           double price,
                           double size,
                           const std::string& note) {
        BridgeOutboxRecord record = makeBridgeRecord(sourceSeq, recordType, "BrokerMarketData", "INTC", side,
                                                     0, 0, 0, "", note, "2026-03-14T09:43:00.000");
        record.instrumentId = "ib:conid:9301:STK:SMART:USD:INTC";
        record.marketField = marketField;
        record.bookPosition = bookPosition;
        record.bookOperation = bookOperation;
        record.bookSide = bookSide;
        record.price = price;
        record.size = size;
        return record;
    };

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-engine-phase3-liquidity";
    options.runtimeSessionId = "runtime-engine-phase3-liquidity";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;

    BridgeOutboxRecord orderIntent = makeBridgeRecord(8301, "order_intent", "WebSocket", "INTC", "BUY",
                                                      511, 4101, 0, "", "liquidity anchor", "2026-03-14T09:43:00.050");
    orderIntent.instrumentId = "ib:conid:9301:STK:SMART:USD:INTC";
    options.batchSeq = 121;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({orderIntent}, options)), &error),
           "tape-engine should accept the liquidity anchor batch: " + error);

    options.batchSeq = 122;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        marketRecord(8302, "market_tick", "BID", 1, -1, -1, -1, 45.40, std::numeric_limits<double>::quiet_NaN(), "liquidity bid"),
        marketRecord(8303, "market_tick", "ASK", 2, -1, -1, -1, 45.41, std::numeric_limits<double>::quiet_NaN(), "liquidity ask"),
        marketRecord(8304, "market_depth", "ASK", -1, 0, 0, 0, 45.41, 400.0, "liquidity ask insert"),
        marketRecord(8305, "market_depth", "BID", -1, 0, 0, 1, 45.40, 500.0, "liquidity bid insert")
    }, options)), &error), "tape-engine should accept the liquidity seed batch: " + error);

    options.batchSeq = 123;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        marketRecord(8306, "market_depth", "ASK", -1, 0, 1, 0, 45.41, 100.0, "liquidity ask thin"),
        marketRecord(8307, "market_depth", "ASK", -1, 0, 1, 0, 45.41, 350.0, "liquidity ask refill")
    }, options)), &error), "tape-engine should accept the liquidity change batch: " + error);

    waitUntil([&]() {
        const auto snapshot = server.snapshot();
        return snapshot.latestFrozenRevisionId >= 3 && snapshot.segments.size() >= 3;
    }, "tape-engine should freeze liquidity signal batches");

    tape_engine::Client client(socketPath.string());
    tape_engine::QueryResponse response;

    tape_engine::QueryRequest findingsRequest;
    findingsRequest.requestId = "findings-phase3-liquidity";
    findingsRequest.operation = "list_findings";
    expect(client.query(findingsRequest, &response, &error), "phase 3 liquidity findings query should succeed: " + error);
    bool sawAskThin = false;
    bool sawAskRefill = false;
    for (const auto& item : response.events) {
        const std::string kind = item.value("kind", std::string());
        sawAskThin = sawAskThin || kind == "ask_liquidity_thinned";
        sawAskRefill = sawAskRefill || kind == "ask_liquidity_refilled";
    }
    expect(sawAskThin, "phase 3 liquidity should detect inside ask thinning");
    expect(sawAskRefill, "phase 3 liquidity should detect inside ask refill");

    tape_engine::QueryRequest incidentsRequest;
    incidentsRequest.requestId = "incidents-phase3-liquidity";
    incidentsRequest.operation = "list_incidents";
    expect(client.query(incidentsRequest, &response, &error), "phase 3 liquidity incidents query should succeed: " + error);
    bool sawRankedLiquidityIncident = false;
    for (const auto& item : response.events) {
        const std::string kind = item.value("kind", std::string());
        if ((kind == "ask_liquidity_thinned" || kind == "ask_liquidity_refilled") &&
            item.value("score", 0.0) > 0.5) {
            sawRankedLiquidityIncident = true;
        }
    }
    expect(sawRankedLiquidityIncident, "phase 3 liquidity incidents should surface ranked liquidity signals");

    server.stop();
}

void testTapeEnginePhase3DetectsDisplayInstabilitySignals() {
    const fs::path rootDir = testDataDir() / "tape-engine-phase3-display-instability";
    const fs::path socketPath = testDataDir() / "tape-engine-phase3-display-instability.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "ib:conid:9351:STK:SMART:USD:INTC";
    config.ringCapacity = 32;

    tape_engine::Server server(config);
    std::string startError;
    expect(server.start(&startError), "tape-engine should start for display-instability signal test: " + startError);

    auto marketRecord = [](std::uint64_t sourceSeq,
                           const std::string& recordType,
                           const std::string& side,
                           int marketField,
                           int bookPosition,
                           int bookOperation,
                           int bookSide,
                           double price,
                           double size,
                           const std::string& note) {
        BridgeOutboxRecord record = makeBridgeRecord(sourceSeq, recordType, "BrokerMarketData", "INTC", side,
                                                     0, 0, 0, "", note, "2026-03-14T09:43:30.000");
        record.instrumentId = "ib:conid:9351:STK:SMART:USD:INTC";
        record.marketField = marketField;
        record.bookPosition = bookPosition;
        record.bookOperation = bookOperation;
        record.bookSide = bookSide;
        record.price = price;
        record.size = size;
        return record;
    };

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-engine-phase3-display";
    options.runtimeSessionId = "runtime-engine-phase3-display";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;

    BridgeOutboxRecord orderIntent = makeBridgeRecord(8351, "order_intent", "WebSocket", "INTC", "BUY",
                                                      531, 4301, 0, "", "display instability anchor", "2026-03-14T09:43:30.050");
    orderIntent.instrumentId = "ib:conid:9351:STK:SMART:USD:INTC";
    options.batchSeq = 124;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({orderIntent}, options)), &error),
           "tape-engine should accept the display-instability anchor batch: " + error);

    options.batchSeq = 125;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        marketRecord(8352, "market_tick", "BID", 1, -1, -1, -1, 45.45, std::numeric_limits<double>::quiet_NaN(), "display bid"),
        marketRecord(8353, "market_tick", "ASK", 2, -1, -1, -1, 45.46, std::numeric_limits<double>::quiet_NaN(), "display ask"),
        marketRecord(8354, "market_depth", "ASK", -1, 0, 0, 0, 45.46, 400.0, "display ask insert"),
        marketRecord(8355, "market_depth", "BID", -1, 0, 0, 1, 45.45, 500.0, "display bid insert")
    }, options)), &error), "tape-engine should accept the display-instability seed batch: " + error);

    options.batchSeq = 126;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        marketRecord(8356, "market_depth", "ASK", -1, 0, 1, 0, 45.46, 100.0, "display ask remove one"),
        marketRecord(8357, "market_depth", "ASK", -1, 0, 1, 0, 45.46, 360.0, "display ask readd one"),
        marketRecord(8358, "market_depth", "ASK", -1, 0, 1, 0, 45.46, 120.0, "display ask remove two"),
        marketRecord(8359, "market_depth", "ASK", -1, 0, 1, 0, 45.46, 350.0, "display ask readd two")
    }, options)), &error), "tape-engine should accept the display-instability change batch: " + error);

    waitUntil([&]() {
        const auto snapshot = server.snapshot();
        return snapshot.latestFrozenRevisionId >= 3 && snapshot.segments.size() >= 3;
    }, "tape-engine should freeze display-instability signal batches");

    tape_engine::Client client(socketPath.string());
    tape_engine::QueryResponse response;

    tape_engine::QueryRequest findingsRequest;
    findingsRequest.requestId = "findings-phase3-display";
    findingsRequest.operation = "list_findings";
    expect(client.query(findingsRequest, &response, &error), "phase 3 display-instability findings query should succeed: " + error);
    bool sawDisplayInstabilityFinding = false;
    for (const auto& item : response.events) {
        sawDisplayInstabilityFinding = sawDisplayInstabilityFinding || item.value("kind", std::string()) == "ask_display_instability";
    }
    expect(sawDisplayInstabilityFinding, "phase 3 should detect repeated ask display instability at the touch");

    tape_engine::QueryRequest incidentsRequest;
    incidentsRequest.requestId = "incidents-phase3-display";
    incidentsRequest.operation = "list_incidents";
    expect(client.query(incidentsRequest, &response, &error), "phase 3 display-instability incidents query should succeed: " + error);
    bool sawDisplayInstabilityIncident = false;
    for (const auto& item : response.events) {
        if (item.value("kind", std::string()) == "ask_display_instability" &&
            item.value("score", 0.0) > 0.5) {
            sawDisplayInstabilityIncident = true;
        }
    }
    expect(sawDisplayInstabilityIncident, "phase 3 display-instability incidents should surface a ranked instability incident");

    server.stop();
}

void testTapeEnginePhase3DetectsPullFollowThroughAndQuoteFlickerSignals() {
    const fs::path rootDir = testDataDir() / "tape-engine-phase3-pull-flicker";
    const fs::path socketPath = testDataDir() / "tape-engine-phase3-pull-flicker.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "ib:conid:9361:STK:SMART:USD:INTC";
    config.ringCapacity = 32;

    tape_engine::Server server(config);
    std::string startError;
    expect(server.start(&startError), "tape-engine should start for pull-follow-through/quote-flicker signal test: " + startError);

    auto marketRecord = [](std::uint64_t sourceSeq,
                           const std::string& recordType,
                           const std::string& side,
                           int marketField,
                           int bookPosition,
                           int bookOperation,
                           int bookSide,
                           double price,
                           double size,
                           const std::string& note) {
        BridgeOutboxRecord record = makeBridgeRecord(sourceSeq, recordType, "BrokerMarketData", "INTC", side,
                                                     0, 0, 0, "", note, "2026-03-14T09:43:38.000");
        record.instrumentId = "ib:conid:9361:STK:SMART:USD:INTC";
        record.marketField = marketField;
        record.bookPosition = bookPosition;
        record.bookOperation = bookOperation;
        record.bookSide = bookSide;
        record.price = price;
        record.size = size;
        return record;
    };

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-engine-phase3-pull-flicker";
    options.runtimeSessionId = "runtime-engine-phase3-pull-flicker";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;

    BridgeOutboxRecord orderIntent = makeBridgeRecord(8361, "order_intent", "WebSocket", "INTC", "BUY",
                                                      536, 4351, 0, "", "pull/flicker anchor", "2026-03-14T09:43:38.050");
    orderIntent.instrumentId = "ib:conid:9361:STK:SMART:USD:INTC";
    options.batchSeq = 1261;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({orderIntent}, options)), &error),
           "tape-engine should accept the pull/flicker anchor batch: " + error);

    options.batchSeq = 1262;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        marketRecord(8362, "market_tick", "BID", 1, -1, -1, -1, 45.60, std::numeric_limits<double>::quiet_NaN(), "pull/flicker bid"),
        marketRecord(8363, "market_tick", "ASK", 2, -1, -1, -1, 45.61, std::numeric_limits<double>::quiet_NaN(), "pull/flicker ask"),
        marketRecord(8364, "market_depth", "ASK", -1, 0, 0, 0, 45.61, 400.0, "pull/flicker ask insert"),
        marketRecord(8365, "market_depth", "BID", -1, 0, 0, 1, 45.60, 500.0, "pull/flicker bid insert")
    }, options)), &error), "tape-engine should accept the pull/flicker seed batch: " + error);

    options.batchSeq = 1263;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        marketRecord(8366, "market_depth", "ASK", -1, 0, 1, 0, 45.61, 100.0, "pull/flicker ask thin"),
        marketRecord(8367, "market_depth", "ASK", -1, 0, 1, 0, 45.63, 100.0, "pull/flicker ask move one"),
        marketRecord(8368, "market_depth", "ASK", -1, 0, 1, 0, 45.62, 110.0, "pull/flicker ask move two"),
        marketRecord(8369, "market_depth", "ASK", -1, 0, 1, 0, 45.64, 90.0, "pull/flicker ask move three")
    }, options)), &error), "tape-engine should accept the pull/flicker change batch: " + error);

    waitUntil([&]() {
        const auto snapshot = server.snapshot();
        return snapshot.latestFrozenRevisionId >= 3 && snapshot.segments.size() >= 3;
    }, "tape-engine should freeze pull-follow-through and quote-flicker batches");

    tape_engine::Client client(socketPath.string());
    tape_engine::QueryResponse response;

    tape_engine::QueryRequest findingsRequest;
    findingsRequest.requestId = "findings-phase3-pull-flicker";
    findingsRequest.operation = "list_findings";
    expect(client.query(findingsRequest, &response, &error), "phase 3 pull-follow-through findings query should succeed: " + error);
    bool sawPullFollowThrough = false;
    bool sawQuoteFlicker = false;
    for (const auto& item : response.events) {
        const std::string kind = item.value("kind", std::string());
        sawPullFollowThrough = sawPullFollowThrough || kind == "ask_pull_follow_through";
        sawQuoteFlicker = sawQuoteFlicker || kind == "ask_quote_flicker";
    }
    expect(sawPullFollowThrough, "phase 3 should detect ask pull follow-through after inside liquidity pulls");
    expect(sawQuoteFlicker, "phase 3 should detect repeated ask quote flicker at the touch");

    tape_engine::QueryRequest incidentsRequest;
    incidentsRequest.requestId = "incidents-phase3-pull-flicker";
    incidentsRequest.operation = "list_incidents";
    expect(client.query(incidentsRequest, &response, &error), "phase 3 pull/flicker incidents query should succeed: " + error);
    double pullFollowThroughScore = 0.0;
    double quoteFlickerScore = 0.0;
    for (const auto& item : response.events) {
        if (item.value("kind", std::string()) == "ask_pull_follow_through") {
            pullFollowThroughScore = item.value("score", 0.0);
        } else if (item.value("kind", std::string()) == "ask_quote_flicker") {
            quoteFlickerScore = item.value("score", 0.0);
        }
    }
    expect(pullFollowThroughScore > 0.5, "phase 3 pull follow-through should surface as a ranked incident");
    expect(quoteFlickerScore > 0.5, "phase 3 quote flicker should surface as a ranked incident");
    expect(pullFollowThroughScore > quoteFlickerScore,
           "phase 3 ranking should score pull follow-through above quote flicker because it is closer to realized follow-through");

    server.stop();
}

void testTapeEnginePhase3DetectsTradeAfterDepletionAndAbsorptionPersistenceSignals() {
    const fs::path rootDir = testDataDir() / "tape-engine-phase3-depletion-absorption";
    const fs::path socketPath = testDataDir() / "tape-engine-phase3-depletion-absorption.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "ib:conid:9366:STK:SMART:USD:INTC";
    config.ringCapacity = 32;

    tape_engine::Server server(config);
    std::string startError;
    expect(server.start(&startError), "tape-engine should start for depletion/absorption signal test: " + startError);

    auto marketRecord = [](std::uint64_t sourceSeq,
                           const std::string& recordType,
                           const std::string& side,
                           int marketField,
                           int bookPosition,
                           int bookOperation,
                           int bookSide,
                           double price,
                           double size,
                           const std::string& note) {
        BridgeOutboxRecord record = makeBridgeRecord(sourceSeq, recordType, "BrokerMarketData", "INTC", side,
                                                     0, 0, 0, "", note, "2026-03-14T09:43:40.000");
        record.instrumentId = "ib:conid:9366:STK:SMART:USD:INTC";
        record.marketField = marketField;
        record.bookPosition = bookPosition;
        record.bookOperation = bookOperation;
        record.bookSide = bookSide;
        record.price = price;
        record.size = size;
        return record;
    };

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-engine-phase3-depletion-absorption";
    options.runtimeSessionId = "runtime-engine-phase3-depletion-absorption";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;

    BridgeOutboxRecord orderIntent = makeBridgeRecord(83660, "order_intent", "WebSocket", "INTC", "BUY",
                                                      537, 4366, 0, "", "depletion/absorption anchor", "2026-03-14T09:43:40.050");
    orderIntent.instrumentId = "ib:conid:9366:STK:SMART:USD:INTC";
    options.batchSeq = 1266;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({orderIntent}, options)), &error),
           "tape-engine should accept the depletion/absorption anchor batch: " + error);

    options.batchSeq = 1267;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        marketRecord(83661, "market_tick", "BID", 1, -1, -1, -1, 45.70, std::numeric_limits<double>::quiet_NaN(), "depletion/absorption bid"),
        marketRecord(83662, "market_tick", "ASK", 2, -1, -1, -1, 45.71, std::numeric_limits<double>::quiet_NaN(), "depletion/absorption ask"),
        marketRecord(83663, "market_depth", "ASK", -1, 0, 0, 0, 45.71, 400.0, "depletion/absorption ask insert"),
        marketRecord(83664, "market_depth", "BID", -1, 0, 0, 1, 45.70, 500.0, "depletion/absorption bid insert")
    }, options)), &error), "tape-engine should accept the depletion/absorption seed batch: " + error);

    options.batchSeq = 1268;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        marketRecord(83665, "market_tick", "LAST", 4, -1, -1, -1, 45.71, std::numeric_limits<double>::quiet_NaN(), "touch trade before depletion"),
        marketRecord(83666, "market_depth", "ASK", -1, 0, 1, 0, 45.71, 100.0, "depletion ask thin"),
        marketRecord(83667, "market_tick", "LAST", 4, -1, -1, -1, 45.71, std::numeric_limits<double>::quiet_NaN(), "trade after depletion"),
        marketRecord(83668, "market_depth", "ASK", -1, 0, 1, 0, 45.71, 380.0, "absorption ask refill"),
        marketRecord(83669, "market_tick", "LAST", 4, -1, -1, -1, 45.71, std::numeric_limits<double>::quiet_NaN(), "touch trade after refill"),
        marketRecord(83670, "market_depth", "ASK", -1, 0, 1, 0, 45.71, 360.0, "absorption ask holds"),
        marketRecord(83671, "market_depth", "ASK", -1, 0, 1, 0, 45.71, 560.0, "genuine refill ask"),
        marketRecord(83672, "market_tick", "BID", 1, -1, -1, -1, 45.70, std::numeric_limits<double>::quiet_NaN(), "genuine refill stable update one"),
        marketRecord(83673, "market_tick", "ASK", 2, -1, -1, -1, 45.71, std::numeric_limits<double>::quiet_NaN(), "genuine refill stable update two"),
        marketRecord(83674, "market_depth", "BID", -1, 0, 1, 1, 45.70, 480.0, "genuine refill stable update three")
    }, options)), &error), "tape-engine should accept the depletion/absorption change batch: " + error);

    waitUntil([&]() {
        const auto snapshot = server.snapshot();
        return snapshot.latestFrozenRevisionId >= 3 && snapshot.segments.size() >= 3;
    }, "tape-engine should freeze depletion/absorption batches");

    tape_engine::Client client(socketPath.string());
    tape_engine::QueryResponse response;

    tape_engine::QueryRequest findingsRequest;
    findingsRequest.requestId = "findings-phase3-depletion-absorption";
    findingsRequest.operation = "list_findings";
    expect(client.query(findingsRequest, &response, &error), "phase 3 depletion/absorption findings query should succeed: " + error);
    bool sawTradeAfterDepletion = false;
    bool sawDepletionAfterTrade = false;
    bool sawAbsorptionPersistence = false;
    bool sawGenuineRefill = false;
    for (const auto& item : response.events) {
        const std::string kind = item.value("kind", std::string());
        sawTradeAfterDepletion = sawTradeAfterDepletion || kind == "ask_trade_after_depletion";
        sawDepletionAfterTrade = sawDepletionAfterTrade || kind == "ask_depletion_after_trade";
        sawAbsorptionPersistence = sawAbsorptionPersistence || kind == "ask_absorption_persistence";
        sawGenuineRefill = sawGenuineRefill || kind == "ask_genuine_refill";
    }
    expect(sawTradeAfterDepletion, "phase 3 should detect trade-after-depletion at the depleted ask");
    expect(sawDepletionAfterTrade, "phase 3 should detect depletion-after-trade at the ask touch");
    expect(sawAbsorptionPersistence, "phase 3 should detect ask absorption persistence after refill holds");
    expect(sawGenuineRefill, "phase 3 should distinguish a genuine refill from absorption when refill holds without touch trades");

    tape_engine::QueryRequest incidentsRequest;
    incidentsRequest.requestId = "incidents-phase3-depletion-absorption";
    incidentsRequest.operation = "list_incidents";
    expect(client.query(incidentsRequest, &response, &error), "phase 3 depletion/absorption incidents query should succeed: " + error);
    bool sawTradeAfterDepletionIncident = false;
    bool sawDepletionAfterTradeIncident = false;
    bool sawAbsorptionPersistenceIncident = false;
    bool sawGenuineRefillIncident = false;
    for (const auto& item : response.events) {
        const std::string kind = item.value("kind", std::string());
        if (kind == "ask_trade_after_depletion" && item.value("score", 0.0) > 0.5) {
            sawTradeAfterDepletionIncident = true;
        }
        if (kind == "ask_depletion_after_trade" && item.value("score", 0.0) > 0.5) {
            sawDepletionAfterTradeIncident = true;
        }
        if (kind == "ask_absorption_persistence" && item.value("score", 0.0) > 0.5) {
            sawAbsorptionPersistenceIncident = true;
        }
        if (kind == "ask_genuine_refill" && item.value("score", 0.0) > 0.5) {
            sawGenuineRefillIncident = true;
        }
    }
    expect(sawTradeAfterDepletionIncident, "phase 3 trade-after-depletion should surface as a ranked incident");
    expect(sawDepletionAfterTradeIncident, "phase 3 depletion-after-trade should surface as a ranked incident");
    expect(sawAbsorptionPersistenceIncident, "phase 3 absorption persistence should surface as a ranked incident");
    expect(sawGenuineRefillIncident, "phase 3 genuine refill should surface as a ranked incident");

    server.stop();
}

void testTapeEnginePhase3DetectsFillInvalidationSignals() {
    const fs::path rootDir = testDataDir() / "tape-engine-phase3-fill-invalidation";
    const fs::path socketPath = testDataDir() / "tape-engine-phase3-fill-invalidation.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "ib:conid:9381:STK:SMART:USD:INTC";
    config.ringCapacity = 32;

    tape_engine::Server server(config);
    std::string startError;
    expect(server.start(&startError), "tape-engine should start for fill-invalidation signal test: " + startError);

    auto marketTick = [](std::uint64_t sourceSeq,
                         int field,
                         double price,
                         const std::string& note) {
        BridgeOutboxRecord record = makeBridgeRecord(sourceSeq, "market_tick", "BrokerMarketData", "INTC",
                                                     field == 1 ? "BID" : (field == 2 ? "ASK" : "LAST"),
                                                     0, 0, 0, "", note, "2026-03-14T09:43:45.000");
        record.instrumentId = "ib:conid:9381:STK:SMART:USD:INTC";
        record.marketField = field;
        record.price = price;
        return record;
    };

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-engine-phase3-fill-invalid";
    options.runtimeSessionId = "runtime-engine-phase3-fill-invalid";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;

    BridgeOutboxRecord orderIntent = makeBridgeRecord(8381, "order_intent", "WebSocket", "INTC", "BUY",
                                                      541, 4401, 0, "", "fill invalidation anchor", "2026-03-14T09:43:45.050");
    orderIntent.instrumentId = "ib:conid:9381:STK:SMART:USD:INTC";
    options.batchSeq = 127;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({orderIntent}, options)), &error),
           "tape-engine should accept the fill-invalidation anchor batch: " + error);

    options.batchSeq = 128;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        marketTick(8382, 1, 45.50, "fill invalidation bid"),
        marketTick(8383, 2, 45.51, "fill invalidation ask")
    }, options)), &error), "tape-engine should accept the fill-invalidation seed batch: " + error);

    BridgeOutboxRecord fill = makeBridgeRecord(8384, "fill_execution", "BrokerExecution", "INTC", "BOT",
                                               541, 4401, 0, "exec-4401-a", "fill invalidation fill", "2026-03-14T09:43:45.070");
    fill.instrumentId = "ib:conid:9381:STK:SMART:USD:INTC";
    fill.price = 45.51;
    fill.size = 100.0;
    options.batchSeq = 129;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({fill}, options)), &error),
           "tape-engine should accept the fill-invalidation fill batch: " + error);

    options.batchSeq = 130;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        marketTick(8385, 1, 45.48, "fill invalidation bid down"),
        marketTick(8386, 2, 45.49, "fill invalidation ask down")
    }, options)), &error), "tape-engine should accept the post-fill invalidation batch: " + error);

    waitUntil([&]() {
        const auto snapshot = server.snapshot();
        return snapshot.latestFrozenRevisionId >= 4 && snapshot.segments.size() >= 4;
    }, "tape-engine should freeze fill-invalidation signal batches");

    tape_engine::Client client(socketPath.string());
    tape_engine::QueryResponse response;

    tape_engine::QueryRequest findingsRequest;
    findingsRequest.requestId = "findings-phase3-fill-invalid";
    findingsRequest.operation = "list_findings";
    expect(client.query(findingsRequest, &response, &error), "phase 3 fill-invalidation findings query should succeed: " + error);
    bool sawFillInvalidationFinding = false;
    for (const auto& item : response.events) {
        sawFillInvalidationFinding = sawFillInvalidationFinding || item.value("kind", std::string()) == "buy_fill_invalidation";
    }
    expect(sawFillInvalidationFinding, "phase 3 should detect a buy fill invalidation after adverse post-fill movement");

    tape_engine::QueryRequest incidentsRequest;
    incidentsRequest.requestId = "incidents-phase3-fill-invalid";
    incidentsRequest.operation = "list_incidents";
    expect(client.query(incidentsRequest, &response, &error), "phase 3 fill-invalidation incidents query should succeed: " + error);
    bool sawFillInvalidationIncident = false;
    for (const auto& item : response.events) {
        if (item.value("kind", std::string()) == "buy_fill_invalidation" &&
            item.value("score", 0.0) > 0.5) {
            sawFillInvalidationIncident = true;
        }
    }
    expect(sawFillInvalidationIncident, "phase 3 fill-invalidation incidents should surface a ranked invalidation incident");

    tape_engine::QueryRequest caseRequest;
    caseRequest.requestId = "order-case-phase3-fill-invalid";
    caseRequest.operation = "read_order_case";
    caseRequest.orderId = 4401;
    expect(client.query(caseRequest, &response, &error), "phase 3 fill-invalidation order-case query should succeed: " + error);
    bool sawInvalidationRelatedFinding = false;
    for (const auto& item : response.summary.value("related_findings", json::array())) {
        sawInvalidationRelatedFinding = sawInvalidationRelatedFinding || item.value("kind", std::string()) == "buy_fill_invalidation";
    }
    expect(sawInvalidationRelatedFinding, "order-case related findings should include the fill invalidation signal");

    server.stop();
}

void testTapeEnginePhase3BuildsOrderWindowMarketImpactFinding() {
    const fs::path rootDir = testDataDir() / "tape-engine-phase3-market-impact";
    const fs::path socketPath = testDataDir() / "tape-engine-phase3-market-impact.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "ib:conid:9391:STK:SMART:USD:INTC";
    config.ringCapacity = 32;

    tape_engine::Server server(config);
    std::string startError;
    expect(server.start(&startError), "tape-engine should start for order-window market-impact test: " + startError);

    auto marketTick = [](std::uint64_t sourceSeq,
                         int field,
                         double price,
                         const std::string& note) {
        BridgeOutboxRecord record = makeBridgeRecord(sourceSeq, "market_tick", "BrokerMarketData", "INTC",
                                                     field == 1 ? "BID" : (field == 2 ? "ASK" : "LAST"),
                                                     0, 0, 0, "", note, "2026-03-14T09:43:50.000");
        record.instrumentId = "ib:conid:9391:STK:SMART:USD:INTC";
        record.marketField = field;
        record.price = price;
        return record;
    };

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-engine-phase3-market-impact";
    options.runtimeSessionId = "runtime-engine-phase3-market-impact";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;

    BridgeOutboxRecord orderIntent = makeBridgeRecord(8391, "order_intent", "WebSocket", "INTC", "BUY",
                                                      551, 4451, 0, "", "market-impact anchor", "2026-03-14T09:43:50.050");
    orderIntent.instrumentId = "ib:conid:9391:STK:SMART:USD:INTC";
    options.batchSeq = 131;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({orderIntent}, options)), &error),
           "tape-engine should accept the market-impact anchor batch: " + error);

    options.batchSeq = 132;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        marketTick(8392, 1, 45.80, "market impact bid"),
        marketTick(8393, 2, 45.81, "market impact ask")
    }, options)), &error), "tape-engine should accept the market-impact seed batch: " + error);

    BridgeOutboxRecord fill = makeBridgeRecord(8394, "fill_execution", "BrokerExecution", "INTC", "BOT",
                                               551, 4451, 0, "exec-4451-a", "market impact fill", "2026-03-14T09:43:50.070");
    fill.instrumentId = "ib:conid:9391:STK:SMART:USD:INTC";
    fill.price = 45.81;
    fill.size = 100.0;
    options.batchSeq = 133;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({fill}, options)), &error),
           "tape-engine should accept the market-impact fill batch: " + error);

    options.batchSeq = 134;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        marketTick(8395, 1, 45.75, "market impact bid down"),
        marketTick(8396, 2, 45.77, "market impact ask down")
    }, options)), &error), "tape-engine should accept the market-impact follow-through batch: " + error);

    waitUntil([&]() {
        const auto snapshot = server.snapshot();
        return snapshot.latestFrozenRevisionId >= 4 && snapshot.segments.size() >= 4;
    }, "tape-engine should freeze order-window market-impact batches");

    tape_engine::Client client(socketPath.string());
    tape_engine::QueryResponse response;

    tape_engine::QueryRequest findingsRequest;
    findingsRequest.requestId = "findings-phase3-market-impact";
    findingsRequest.operation = "list_findings";
    expect(client.query(findingsRequest, &response, &error), "phase 3 market-impact findings query should succeed: " + error);
    bool sawMarketImpactFinding = false;
    for (const auto& item : response.events) {
        sawMarketImpactFinding = sawMarketImpactFinding || item.value("kind", std::string()) == "order_window_market_impact";
    }
    expect(sawMarketImpactFinding, "phase 3 deferred lane should emit an order-window market-impact finding");

    tape_engine::QueryRequest caseRequest;
    caseRequest.requestId = "order-case-phase3-market-impact";
    caseRequest.operation = "read_order_case";
    caseRequest.orderId = 4451;
    expect(client.query(caseRequest, &response, &error), "phase 3 market-impact order-case query should succeed: " + error);
    bool sawMarketImpactRelatedFinding = false;
    for (const auto& item : response.summary.value("related_findings", json::array())) {
        sawMarketImpactRelatedFinding = sawMarketImpactRelatedFinding || item.value("kind", std::string()) == "order_window_market_impact";
    }
    expect(sawMarketImpactRelatedFinding, "order-case related findings should include the order-window market-impact analysis");

    server.stop();
}

void testTapeEnginePhase3BuildsPassiveFillQueueProxyAndAdverseSelection() {
    const fs::path rootDir = testDataDir() / "tape-engine-phase3-passive-fill";
    const fs::path socketPath = testDataDir() / "tape-engine-phase3-passive-fill.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "ib:conid:9399:STK:SMART:USD:INTC";
    config.ringCapacity = 32;

    tape_engine::Server server(config);
    std::string startError;
    expect(server.start(&startError), "tape-engine should start for passive-fill proxy test: " + startError);

    auto marketTick = [](std::uint64_t sourceSeq,
                         int field,
                         double price,
                         const std::string& note) {
        BridgeOutboxRecord record = makeBridgeRecord(sourceSeq, "market_tick", "BrokerMarketData", "INTC",
                                                     field == 1 ? "BID" : (field == 2 ? "ASK" : "LAST"),
                                                     0, 0, 0, "", note, "2026-03-14T09:43:55.000");
        record.instrumentId = "ib:conid:9399:STK:SMART:USD:INTC";
        record.marketField = field;
        record.price = price;
        return record;
    };
    auto marketDepth = [](std::uint64_t sourceSeq,
                          int side,
                          int operation,
                          double price,
                          double size,
                          const std::string& note) {
        BridgeOutboxRecord record = makeBridgeRecord(sourceSeq, "market_depth", "BrokerMarketData", "INTC",
                                                     side == 1 ? "BID" : "ASK",
                                                     0, 0, 0, "", note, "2026-03-14T09:43:55.000");
        record.instrumentId = "ib:conid:9399:STK:SMART:USD:INTC";
        record.bookPosition = 0;
        record.bookOperation = operation;
        record.bookSide = side;
        record.price = price;
        record.size = size;
        return record;
    };

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-engine-phase3-passive-fill";
    options.runtimeSessionId = "runtime-engine-phase3-passive-fill";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;

    BridgeOutboxRecord orderIntent = makeBridgeRecord(8501, "order_intent", "WebSocket", "INTC", "BUY",
                                                      651, 5501, 0, "", "passive-fill anchor", "2026-03-14T09:43:55.010");
    orderIntent.instrumentId = "ib:conid:9399:STK:SMART:USD:INTC";
    options.batchSeq = 141;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({orderIntent}, options)), &error),
           "tape-engine should accept the passive-fill anchor batch: " + error);

    options.batchSeq = 142;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        marketTick(8502, 1, 45.90, "passive fill bid"),
        marketTick(8503, 2, 45.91, "passive fill ask"),
        marketDepth(8504, 0, 0, 45.91, 220.0, "passive fill ask size"),
        marketDepth(8505, 1, 0, 45.90, 260.0, "passive fill bid size")
    }, options)), &error), "tape-engine should accept the passive-fill seed batch: " + error);

    BridgeOutboxRecord fill = makeBridgeRecord(8506, "fill_execution", "BrokerExecution", "INTC", "BOT",
                                               651, 5501, 0, "exec-5501-a", "passive fill", "2026-03-14T09:43:55.040");
    fill.instrumentId = "ib:conid:9399:STK:SMART:USD:INTC";
    fill.price = 45.91;
    fill.size = 100.0;
    options.batchSeq = 143;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({fill}, options)), &error),
           "tape-engine should accept the passive-fill execution batch: " + error);

    options.batchSeq = 144;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        marketDepth(8507, 0, 1, 45.91, 180.0, "post-fill ask remains"),
        marketDepth(8508, 0, 1, 45.91, 210.0, "post-fill ask refill"),
        marketTick(8509, 1, 45.87, "post-fill bid down"),
        marketTick(8510, 2, 45.89, "post-fill ask down"),
        marketDepth(8511, 0, 1, 45.91, 205.0, "post-fill ask stable")
    }, options)), &error), "tape-engine should accept the passive-fill follow-through batch: " + error);

    waitUntil([&]() {
        const auto snapshot = server.snapshot();
        return snapshot.latestFrozenRevisionId >= 4 && snapshot.segments.size() >= 4;
    }, "tape-engine should freeze passive-fill batches");

    tape_engine::Client client(socketPath.string());
    tape_engine::QueryResponse response;

    tape_engine::QueryRequest caseRequest;
    caseRequest.requestId = "order-case-phase3-passive-fill";
    caseRequest.operation = "read_order_case";
    caseRequest.orderId = 5501;
    expect(client.query(caseRequest, &response, &error), "phase 3 passive-fill order-case query should succeed: " + error);

    bool sawQueueProxy = false;
    bool sawAdverseSelection = false;
    for (const auto& item : response.summary.value("related_findings", json::array())) {
        sawQueueProxy = sawQueueProxy || item.value("kind", std::string()) == "passive_fill_queue_proxy";
        sawAdverseSelection = sawAdverseSelection || item.value("kind", std::string()) == "post_fill_adverse_selection";
    }
    expect(sawQueueProxy, "order-case related findings should include the passive-fill queue proxy analysis");
    expect(sawAdverseSelection, "order-case related findings should include the post-fill adverse-selection analysis");

    server.stop();
}

void testTapeEnginePhase3BuildsPassiveQueueLossAndCutThroughSignals() {
    const fs::path rootDir = testDataDir() / "tape-engine-phase3-passive-queue-loss";
    const fs::path socketPath = testDataDir() / "tape-engine-phase3-passive-queue-loss.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "ib:conid:9451:STK:SMART:USD:INTC";
    config.ringCapacity = 32;

    tape_engine::Server server(config);
    std::string startError;
    expect(server.start(&startError), "tape-engine should start for passive queue-loss test: " + startError);

    auto marketTick = [](std::uint64_t sourceSeq,
                         int field,
                         double price,
                         const std::string& note) {
        BridgeOutboxRecord record = makeBridgeRecord(sourceSeq, "market_tick", "BrokerMarketData", "INTC",
                                                     field == 1 ? "BID" : (field == 2 ? "ASK" : "LAST"),
                                                     0, 0, 0, "", note, "2026-03-14T09:44:10.000");
        record.instrumentId = "ib:conid:9451:STK:SMART:USD:INTC";
        record.marketField = field;
        record.price = price;
        return record;
    };

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-engine-phase3-passive-queue-loss";
    options.runtimeSessionId = "runtime-engine-phase3-passive-queue-loss";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;

    BridgeOutboxRecord orderIntent = makeBridgeRecord(8601, "order_intent", "WebSocket", "INTC", "BUY",
                                                      761, 5601, 0, "", "passive queue-loss anchor", "2026-03-14T09:44:10.010");
    orderIntent.instrumentId = "ib:conid:9451:STK:SMART:USD:INTC";
    options.batchSeq = 151;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({orderIntent}, options)), &error),
           "tape-engine should accept the passive queue-loss anchor batch: " + error);

    options.batchSeq = 152;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        marketTick(8602, 1, 45.00, "queue-loss bid"),
        marketTick(8603, 2, 45.01, "queue-loss ask")
    }, options)), &error), "tape-engine should accept the passive queue-loss seed batch: " + error);

    options.batchSeq = 153;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        marketTick(8604, 4, 45.00, "queue-loss touch trade one"),
        marketTick(8605, 4, 45.00, "queue-loss touch trade two"),
        marketTick(8606, 1, 44.97, "queue-loss bid lower"),
        marketTick(8607, 2, 44.98, "queue-loss ask lower")
    }, options)), &error), "tape-engine should accept the passive queue-loss follow-through batch: " + error);

    BridgeOutboxRecord cancel = makeBridgeRecord(8608, "cancel_request", "WebSocket", "INTC", "BUY",
                                                 761, 5601, 0, "", "queue-loss cancel", "2026-03-14T09:44:10.060");
    cancel.instrumentId = "ib:conid:9451:STK:SMART:USD:INTC";
    options.batchSeq = 154;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({cancel}, options)), &error),
           "tape-engine should accept the passive queue-loss cancel batch: " + error);

    waitUntil([&]() {
        const auto snapshot = server.snapshot();
        return snapshot.latestFrozenRevisionId >= 4 && snapshot.segments.size() >= 4;
    }, "tape-engine should freeze passive queue-loss batches");

    tape_engine::Client client(socketPath.string());
    tape_engine::QueryResponse response;

    tape_engine::QueryRequest caseRequest;
    caseRequest.requestId = "order-case-phase3-passive-queue-loss";
    caseRequest.operation = "read_order_case";
    caseRequest.orderId = 5601;
    caseRequest.includeLiveTail = true;
    expect(client.query(caseRequest, &response, &error), "phase 3 passive queue-loss order-case query should succeed: " + error);

    bool sawQueueLoss = false;
    bool sawCutThrough = false;
    for (const auto& item : response.summary.value("related_findings", json::array())) {
        const std::string kind = item.value("kind", std::string());
        sawQueueLoss = sawQueueLoss || kind == "passive_queue_loss_proxy";
        sawCutThrough = sawCutThrough || kind == "passive_cut_through_proxy";
    }
    expect(sawQueueLoss, "order-case related findings should include the passive queue-loss proxy");
    expect(sawCutThrough, "order-case related findings should include the passive cut-through proxy");

    server.stop();
}

void testTapeEnginePhase3BuildsSweepAndFadeSignals() {
    auto runCase = [&](const fs::path& rootDir,
                       const fs::path& socketPath,
                       OrderId orderId,
                       std::uint64_t traceId,
                       const std::vector<BridgeOutboxRecord>& records,
                       const std::string& expectedKind,
                       const std::string& label) {
        std::error_code ec;
        fs::remove_all(rootDir, ec);
        fs::remove(socketPath, ec);

        tape_engine::EngineConfig config;
        config.socketPath = socketPath.string();
        config.dataDir = rootDir;
        config.instrumentId = "ib:conid:9461:STK:SMART:USD:INTC";
        config.ringCapacity = 32;

        tape_engine::Server server(config);
        std::string startError;
        expect(server.start(&startError), "tape-engine should start for " + label + ": " + startError);

        bridge_batch::BuildOptions options;
        options.appSessionId = "app-engine-phase3-" + label;
        options.runtimeSessionId = "runtime-engine-phase3-" + label;
        options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

        bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
        std::string error;

        BridgeOutboxRecord orderIntent = makeBridgeRecord(records.front().sourceSeq - 1, "order_intent", "WebSocket", "INTC", "BUY",
                                                          traceId, orderId, 0, "", label + " anchor", "2026-03-14T09:44:20.010");
        orderIntent.instrumentId = "ib:conid:9461:STK:SMART:USD:INTC";
        options.batchSeq = 161;
        expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({orderIntent}, options)), &error),
               "tape-engine should accept the " + label + " anchor batch: " + error);

        options.batchSeq = 162;
        expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch(records, options)), &error),
               "tape-engine should accept the " + label + " market batch: " + error);

        waitUntil([&]() {
            const auto snapshot = server.snapshot();
            return snapshot.latestFrozenRevisionId >= 2 && snapshot.segments.size() >= 2;
        }, "tape-engine should freeze " + label + " batches");

        tape_engine::Client client(socketPath.string());
        tape_engine::QueryResponse response;
        std::string lastSeenKinds;

        waitUntil([&]() {
            tape_engine::QueryRequest pollRequest;
            pollRequest.requestId = "poll-findings-phase3-" + label;
            pollRequest.operation = "list_findings";
            pollRequest.limit = 64;
            pollRequest.includeLiveTail = true;
            if (!client.query(pollRequest, &response, &error)) {
                lastSeenKinds = "query failed: " + error;
                return false;
            }
            std::ostringstream seenKinds;
            for (const auto& item : response.events) {
                const std::string kind = item.value("kind", std::string());
                if (!seenKinds.str().empty()) {
                    seenKinds << ", ";
                }
                seenKinds << kind;
                if (kind == expectedKind) {
                    lastSeenKinds = seenKinds.str();
                    return true;
                }
            }
            lastSeenKinds = seenKinds.str();
            return false;
        }, "phase 3 " + label + " findings should surface " + expectedKind +
           " (saw during polling: " + lastSeenKinds + ")");

        tape_engine::QueryRequest caseRequest;
        caseRequest.requestId = "order-case-phase3-" + label;
        caseRequest.operation = "read_order_case";
        caseRequest.orderId = orderId;
        caseRequest.includeLiveTail = true;
        expect(client.query(caseRequest, &response, &error), "phase 3 " + label + " order-case query should succeed: " + error);

        expect(response.summary.value("related_finding_count", 0ULL) >= 1ULL,
               "order-case should still carry related findings for " + label);

        server.stop();
    };

    auto marketTick = [](std::uint64_t sourceSeq,
                         int field,
                         double price,
                         const std::string& note) {
        BridgeOutboxRecord record = makeBridgeRecord(sourceSeq, "market_tick", "BrokerMarketData", "INTC",
                                                     field == 1 ? "BID" : (field == 2 ? "ASK" : "LAST"),
                                                     0, 0, 0, "", note, "2026-03-14T09:44:20.000");
        record.instrumentId = "ib:conid:9461:STK:SMART:USD:INTC";
        record.marketField = field;
        record.price = price;
        return record;
    };

    runCase(testDataDir() / "tape-engine-phase3-sweep-signal",
            testDataDir() / "tape-engine-phase3-sweep-signal.sock",
            5701,
            771,
            {
                marketTick(8701, 1, 45.10, "sweep bid"),
                marketTick(8702, 2, 45.11, "sweep ask"),
                marketTick(8703, 4, 45.11, "sweep print one"),
                marketTick(8704, 4, 45.11, "sweep print two"),
                marketTick(8705, 2, 45.14, "sweep ask up"),
                marketTick(8706, 4, 45.14, "sweep print three"),
                marketTick(8707, 4, 45.14, "sweep print four"),
                marketTick(8708, 1, 45.13, "sweep bid up")
            },
            "buy_sweep_sequence",
            "sweep-sequence");

    runCase(testDataDir() / "tape-engine-phase3-fade-signal",
            testDataDir() / "tape-engine-phase3-fade-signal.sock",
            5702,
            772,
            {
                marketTick(8711, 1, 45.20, "fade bid"),
                marketTick(8712, 2, 45.21, "fade ask"),
                marketTick(8713, 2, 45.24, "fade ask higher"),
                marketTick(8714, 1, 45.22, "fade bid slightly higher")
            },
            "buy_fade_sequence",
            "fade-sequence");
}

void testTapeEnginePhase3BuildsFillOutcomeChains() {
    const fs::path rootDir = testDataDir() / "tape-engine-phase3-fill-outcome";
    const fs::path socketPath = testDataDir() / "tape-engine-phase3-fill-outcome.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "ib:conid:9471:STK:SMART:USD:INTC";
    config.ringCapacity = 32;

    tape_engine::Server server(config);
    std::string startError;
    expect(server.start(&startError), "tape-engine should start for fill outcome-chain test: " + startError);

    auto marketTick = [](std::uint64_t sourceSeq,
                         int field,
                         double price,
                         const std::string& note) {
        BridgeOutboxRecord record = makeBridgeRecord(sourceSeq, "market_tick", "BrokerMarketData", "INTC",
                                                     field == 1 ? "BID" : (field == 2 ? "ASK" : "LAST"),
                                                     0, 0, 0, "", note, "2026-03-14T09:44:30.000");
        record.instrumentId = "ib:conid:9471:STK:SMART:USD:INTC";
        record.marketField = field;
        record.price = price;
        return record;
    };

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-engine-phase3-fill-outcome";
    options.runtimeSessionId = "runtime-engine-phase3-fill-outcome";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;

    BridgeOutboxRecord orderIntent = makeBridgeRecord(8801, "order_intent", "WebSocket", "INTC", "BUY",
                                                      781, 5801, 0, "", "fill outcome anchor", "2026-03-14T09:44:30.010");
    orderIntent.instrumentId = "ib:conid:9471:STK:SMART:USD:INTC";
    options.batchSeq = 171;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({orderIntent}, options)), &error),
           "tape-engine should accept the fill outcome anchor batch: " + error);

    options.batchSeq = 172;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        marketTick(8802, 1, 45.50, "fill outcome bid"),
        marketTick(8803, 2, 45.51, "fill outcome ask")
    }, options)), &error), "tape-engine should accept the fill outcome seed batch: " + error);

    BridgeOutboxRecord fill = makeBridgeRecord(8804, "fill_execution", "BrokerExecution", "INTC", "BOT",
                                               781, 5801, 0, "exec-5801-a", "fill outcome fill", "2026-03-14T09:44:30.040");
    fill.instrumentId = "ib:conid:9471:STK:SMART:USD:INTC";
    fill.price = 45.51;
    fill.size = 100.0;
    options.batchSeq = 173;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({fill}, options)), &error),
           "tape-engine should accept the fill outcome fill batch: " + error);

    BridgeOutboxRecord cancel = makeBridgeRecord(8805, "cancel_request", "WebSocket", "INTC", "BUY",
                                                 781, 5801, 0, "", "fill outcome cancel", "2026-03-14T09:44:30.050");
    cancel.instrumentId = "ib:conid:9471:STK:SMART:USD:INTC";
    options.batchSeq = 174;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        cancel,
        marketTick(8806, 1, 45.47, "fill outcome bid down"),
        marketTick(8807, 2, 45.49, "fill outcome ask down")
    }, options)), &error), "tape-engine should accept the fill outcome follow-through batch: " + error);

    waitUntil([&]() {
        const auto snapshot = server.snapshot();
        return snapshot.latestFrozenRevisionId >= 4 && snapshot.segments.size() >= 4;
    }, "tape-engine should freeze fill outcome batches");

    tape_engine::Client client(socketPath.string());
    tape_engine::QueryResponse response;

    tape_engine::QueryRequest caseRequest;
    caseRequest.requestId = "order-case-phase3-fill-outcome";
    caseRequest.operation = "read_order_case";
    caseRequest.orderId = 5801;
    caseRequest.includeLiveTail = true;
    expect(client.query(caseRequest, &response, &error), "phase 3 fill outcome order-case query should succeed: " + error);

    bool sawFillToCancel = false;
    bool sawFillToAdverseMove = false;
    for (const auto& item : response.summary.value("related_findings", json::array())) {
        const std::string kind = item.value("kind", std::string());
        sawFillToCancel = sawFillToCancel || kind == "fill_to_cancel_chain";
        sawFillToAdverseMove = sawFillToAdverseMove || kind == "fill_to_adverse_move_chain";
    }
    expect(sawFillToCancel, "order-case related findings should include the fill-to-cancel chain");
    expect(sawFillToAdverseMove, "order-case related findings should include the fill-to-adverse-move chain");

    server.stop();
}

void testTapeEnginePhase3BuildsTradePressureOrderCase() {
    const fs::path rootDir = testDataDir() / "tape-engine-phase3-trade-pressure";
    const fs::path socketPath = testDataDir() / "tape-engine-phase3-trade-pressure.sock";
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
    expect(server.start(&startError), "tape-engine should start for trade-pressure order case test: " + startError);

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-engine-phase3-trade-pressure";
    options.runtimeSessionId = "runtime-engine-phase3-trade-pressure";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;

    auto marketTick = [](std::uint64_t sourceSeq,
                         int field,
                         double price,
                         const std::string& note) {
        BridgeOutboxRecord record = makeBridgeRecord(sourceSeq, "market_tick", "BrokerMarketData", "INTC",
                                                     field == 1 ? "BID" : (field == 2 ? "ASK" : "LAST"),
                                                     0, 0, 0, "", note, "2026-03-14T09:44:00.000");
        record.instrumentId = "ib:conid:9401:STK:SMART:USD:INTC";
        record.marketField = field;
        record.price = price;
        return record;
    };

    BridgeOutboxRecord orderIntent = makeBridgeRecord(8401, "order_intent", "WebSocket", "INTC", "BUY",
                                                      611, 5101, 0, "", "trade pressure anchor", "2026-03-14T09:44:00.050");
    orderIntent.instrumentId = "ib:conid:9401:STK:SMART:USD:INTC";
    options.batchSeq = 131;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({orderIntent}, options)), &error),
           "tape-engine should accept the trade-pressure anchor batch: " + error);

    options.batchSeq = 132;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        marketTick(8402, 1, 45.50, "trade pressure bid"),
        marketTick(8403, 2, 45.51, "trade pressure ask")
    }, options)), &error), "tape-engine should accept the trade-pressure seed batch: " + error);

    options.batchSeq = 133;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        marketTick(8404, 4, 45.51, "trade pressure print one"),
        marketTick(8405, 4, 45.51, "trade pressure print two"),
        marketTick(8406, 4, 45.51, "trade pressure print three")
    }, options)), &error), "tape-engine should accept the trade-pressure print batch: " + error);

    BridgeOutboxRecord fill = makeBridgeRecord(8407, "fill_execution", "BrokerExecution", "INTC", "BOT",
                                               611, 5101, 0, "exec-5101-a", "trade pressure fill", "2026-03-14T09:44:00.090");
    fill.instrumentId = "ib:conid:9401:STK:SMART:USD:INTC";
    fill.price = 45.51;
    fill.size = 100.0;

    options.batchSeq = 134;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({fill}, options)), &error),
           "tape-engine should accept the trade-pressure fill batch: " + error);

    waitUntil([&]() {
        const auto snapshot = server.snapshot();
        return snapshot.latestFrozenRevisionId >= 4 && snapshot.segments.size() >= 4;
    }, "tape-engine should freeze trade-pressure batches");

    tape_engine::Client client(socketPath.string());
    tape_engine::QueryResponse response;

    tape_engine::QueryRequest findingsRequest;
    findingsRequest.requestId = "findings-phase3-trade-pressure";
    findingsRequest.operation = "list_findings";
    expect(client.query(findingsRequest, &response, &error), "phase 3 trade-pressure findings query should succeed: " + error);
    bool sawTradePressureFinding = false;
    for (const auto& item : response.events) {
        sawTradePressureFinding = sawTradePressureFinding || item.value("kind", std::string()) == "buy_trade_pressure";
    }
    expect(sawTradePressureFinding, "phase 3 should detect buy-side trade pressure at the ask");

    tape_engine::QueryRequest caseRequest;
    caseRequest.requestId = "order-case-phase3-trade-pressure";
    caseRequest.operation = "read_order_case";
    caseRequest.orderId = 5101;
    expect(client.query(caseRequest, &response, &error), "phase 3 order-case query should succeed: " + error);
    expect(response.summary.value("replay_target_session_seq", 0ULL) == response.summary.value("last_fill_session_seq", 0ULL),
           "order-case replay target should prefer the latest fill session seq");
    expect(response.summary.value("related_finding_count", 0ULL) >= 2ULL,
           "order-case summary should include related findings from the protected anchor window");
    expect(response.summary.value("related_incident_count", 0ULL) >= 1ULL,
           "order-case summary should include at least one related incident");
    expect(response.summary.contains("protected_window"), "order-case summary should surface the selected protected window");
    expect(response.summary.value("protected_window_event_count", 0ULL) >= 4ULL,
           "order-case summary should include protected-window evidence beyond the anchor-matched lifecycle events");
    expect(response.summary.contains("data_quality"), "order-case summary should include data-quality scoring");
    expect(response.summary.contains("timeline"), "order-case summary should include a merged case timeline");
    expect(response.summary.contains("timeline_summary"), "order-case summary should summarize the merged case timeline");
    expect(response.summary.value("timeline_summary", json::object()).value("market_event_count", 0ULL) >= 2ULL,
           "order-case timeline summary should count surrounding market evidence");
    expect(response.summary.contains("case_report"), "order-case summary should include a report-style case summary");
    expect(response.summary.value("case_report", json::object()).contains("timeline_highlights"),
           "order-case case report should include timeline highlights");

    bool sawTradePressureRelatedFinding = false;
    bool sawOrderFillContext = false;
    bool sawOrderFillContextRichSummary = false;
    for (const auto& item : response.summary.value("related_findings", json::array())) {
        sawTradePressureRelatedFinding = sawTradePressureRelatedFinding || item.value("kind", std::string()) == "buy_trade_pressure";
        if (item.value("kind", std::string()) == "order_fill_context") {
            sawOrderFillContext = true;
            const std::string summary = item.value("summary", std::string());
            sawOrderFillContextRichSummary =
                sawOrderFillContextRichSummary ||
                summary.find("adverse excursion") != std::string::npos ||
                summary.find("mid ") != std::string::npos ||
                summary.find("bid range ") != std::string::npos;
        }
    }
    expect(sawTradePressureRelatedFinding, "order-case related findings should include the trade-pressure signal");
    expect(sawOrderFillContext, "order-case related findings should include the deferred order-fill context analysis");
    expect(sawOrderFillContextRichSummary,
           "order-case related findings should surface the richer fill-context summary");

    bool sawTradePressureIncident = false;
    for (const auto& item : response.summary.value("related_incidents", json::array())) {
        sawTradePressureIncident = sawTradePressureIncident || item.value("kind", std::string()) == "buy_trade_pressure";
    }
    expect(sawTradePressureIncident, "order-case related incidents should include the trade-pressure incident");

    bool sawOrderIntent = false;
    bool sawFill = false;
    for (const auto& item : response.events) {
        const std::string kind = item.value("event_kind", std::string());
        sawOrderIntent = sawOrderIntent || kind == "order_intent";
        sawFill = sawFill || kind == "fill_execution";
    }
    expect(sawOrderIntent, "order-case evidence should include the order intent");
    expect(sawFill, "order-case evidence should include the fill execution");

    bool sawTimelineMarketTick = false;
    bool sawTimelineIncident = false;
    for (const auto& item : response.summary.value("timeline", json::array())) {
        const std::string entryType = item.value("entry_type", std::string());
        const std::string kind = item.value("kind", std::string());
        sawTimelineMarketTick = sawTimelineMarketTick || (entryType == "event" && kind == "market_tick");
        sawTimelineIncident = sawTimelineIncident || (entryType == "incident" && kind == "buy_trade_pressure");
    }
    expect(sawTimelineMarketTick, "order-case timeline should include protected-window market evidence");
    expect(sawTimelineIncident, "order-case timeline should include incident entries");

    server.stop();
}

void testTapeEngineResetMarkerPreservesCanonicalInstrumentIdentity() {
    const fs::path rootDir = testDataDir() / "tape-engine-reset-marker";
    const fs::path socketPath = testDataDir() / "tape-engine-reset-marker.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "ib:conid:7501:STK:SMART:USD:INTC";
    config.ringCapacity = 32;
    config.dedupeWindowSize = 4;

    tape_engine::Server server(config);
    std::string startError;
    expect(server.start(&startError), "tape-engine should start for reset marker test: " + startError);

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-engine-reset-marker";
    options.runtimeSessionId = "runtime-engine-reset-marker";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;

    BridgeOutboxRecord firstRecord = makeBridgeRecord(6002, "order_intent", "WebSocket", "INTC", "BUY",
                                                      111, 901, 0, "", "initial event", "2026-03-14T09:38:45.100");
    firstRecord.instrumentId = "ib:conid:7501:STK:SMART:USD:INTC";
    options.batchSeq = 81;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({firstRecord}, options)), &error),
           "tape-engine should accept the first reset test batch: " + error);

    BridgeOutboxRecord secondRecord = makeBridgeRecord(6001, "order_status", "BrokerOrderStatus", "INTC", "BUY",
                                                       111, 901, 0, "", "out-of-order reset trigger", "2026-03-14T09:38:45.200");
    secondRecord.instrumentId = "ib:conid:7501:STK:SMART:USD:INTC";
    options.batchSeq = 82;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({secondRecord}, options)), &error),
           "tape-engine should accept the out-of-order batch and emit a reset marker: " + error);

    const tape_engine::EngineSnapshot snapshot = server.snapshot();
    expect(snapshot.liveEvents.size() == 3, "reset flow should keep the original event, reset marker, and replacement event in the live ring");
    expect(snapshot.liveEvents.at(1).eventKind == "reset_marker", "out-of-order source_seq should emit an explicit reset marker");
    expect(snapshot.liveEvents.at(1).resetPreviousSourceSeq == 6002, "reset marker should preserve the previous accepted source_seq");
    expect(snapshot.liveEvents.at(1).resetSourceSeq == 6001, "reset marker should preserve the new reset source_seq");
    expect(snapshot.liveEvents.at(1).instrumentId == "ib:conid:7501:STK:SMART:USD:INTC", "reset marker should keep the canonical instrument identity");
    expect(snapshot.liveEvents.at(2).instrumentId == "ib:conid:7501:STK:SMART:USD:INTC", "accepted replacement events should keep the canonical instrument identity");

    server.stop();
}

void testTapeEngineReplaySnapshotRebuildsFrozenMarketState() {
    const fs::path rootDir = testDataDir() / "tape-engine-replay";
    const fs::path socketPath = testDataDir() / "tape-engine-replay.sock";
    std::error_code ec;
    fs::remove_all(rootDir, ec);
    fs::remove(socketPath, ec);

    tape_engine::EngineConfig config;
    config.socketPath = socketPath.string();
    config.dataDir = rootDir;
    config.instrumentId = "INTC";
    config.ringCapacity = 32;

    tape_engine::Server server(config);
    std::string startError;
    expect(server.start(&startError), "tape-engine should start for replay snapshot test: " + startError);

    auto marketRecord = [](std::uint64_t sourceSeq,
                           const std::string& recordType,
                           const std::string& side,
                           int marketField,
                           int bookPosition,
                           int bookOperation,
                           int bookSide,
                           double price,
                           double size,
                           const std::string& note) {
        BridgeOutboxRecord record = makeBridgeRecord(sourceSeq, recordType, "BrokerMarketData", "INTC", side,
                                                     0, 0, 0, "", note, "2026-03-14T09:39:00.000");
        record.marketField = marketField;
        record.bookPosition = bookPosition;
        record.bookOperation = bookOperation;
        record.bookSide = bookSide;
        record.price = price;
        record.size = size;
        return record;
    };

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-engine-replay";
    options.runtimeSessionId = "runtime-engine-replay";
    options.flushReason = bridge_batch::FlushReason::ThresholdRecords;
    options.batchSeq = 61;

    const bridge_batch::Batch batch = bridge_batch::buildBatch({
        marketRecord(4001, "market_tick", "BID", 1, -1, -1, -1, 45.10, std::numeric_limits<double>::quiet_NaN(), "BID tick 45.10"),
        marketRecord(4002, "market_tick", "ASK", 2, -1, -1, -1, 45.12, std::numeric_limits<double>::quiet_NaN(), "ASK tick 45.12"),
        marketRecord(4003, "market_depth", "ASK", -1, 0, 0, 0, 45.12, 200.0, "ASK depth insert"),
        marketRecord(4004, "market_depth", "BID", -1, 0, 0, 1, 45.10, 300.0, "BID depth insert"),
        marketRecord(4005, "market_tick", "LAST", 4, -1, -1, -1, 45.11, std::numeric_limits<double>::quiet_NaN(), "LAST tick 45.11"),
        marketRecord(4006, "market_depth", "ASK", -1, 0, 1, 0, 45.13, 150.0, "ASK depth update"),
        marketRecord(4007, "market_depth", "BID", -1, 1, 0, 1, 45.09, 500.0, "BID depth insert level 1")
    }, options);

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;
    expect(transport.sendFrame(bridge_batch::encodeFrame(batch), &error),
           "tape-engine should accept replay market data batch: " + error);

    waitUntil([&]() {
        const auto snapshot = server.snapshot();
        return snapshot.segments.size() >= 1 && snapshot.latestFrozenRevisionId >= 1;
    }, "tape-engine should freeze replay market data before rebuilding a frozen snapshot");

    tape_engine::Client client(socketPath.string());
    tape_engine::QueryResponse response;

    tape_engine::QueryRequest replayRequest;
    replayRequest.requestId = "replay-1";
    replayRequest.operation = "replay_snapshot";
    replayRequest.targetSessionSeq = 7;
    replayRequest.limit = 2;
    expect(client.query(replayRequest, &response, &error), "tape-engine replay snapshot query should succeed: " + error);

    expect(response.summary.value("served_revision_id", 0ULL) == 1, "replay snapshot should identify the frozen revision used");
    expect(response.summary.value("target_session_seq", 0ULL) == 7, "replay snapshot should preserve the requested session_seq");
    expect(response.summary.value("replayed_through_session_seq", 0ULL) == 7, "replay snapshot should rebuild through the target session_seq");
    expect(response.summary.value("checkpoint_used", false), "replay snapshot should resume from a persisted checkpoint when one is available");
    expect(response.summary.value("checkpoint_session_seq", 0ULL) == 7, "replay snapshot should report the checkpoint session_seq it resumed from");
    expect(response.summary.value("checkpoint_revision_id", 0ULL) == 1, "replay snapshot should report the checkpoint revision it resumed from");
    expect(response.summary.value("applied_event_count", 0ULL) == 7, "replay snapshot should count the applied market events");
    expect(response.result.value("schema", std::string()) == tape_engine::kReplaySnapshotResultSchema,
           "replay snapshot should publish a typed transport result");
    expect(std::fabs(response.summary.value("bid_price", 0.0) - 45.10) < 0.0001, "replay snapshot should rebuild the inside bid");
    expect(std::fabs(response.summary.value("ask_price", 0.0) - 45.13) < 0.0001, "replay snapshot should rebuild the inside ask from depth updates");
    expect(std::fabs(response.summary.value("last_price", 0.0) - 45.11) < 0.0001, "replay snapshot should rebuild the last trade price");
    expect(response.summary.contains("bid_book") && response.summary["bid_book"].is_array() &&
           response.summary["bid_book"].size() == 2, "replay snapshot should return the requested bid book depth");
    expect(response.summary.contains("ask_book") && response.summary["ask_book"].is_array() &&
           response.summary["ask_book"].size() == 1, "replay snapshot should return the rebuilt ask book");
    expect(std::fabs(response.summary["bid_book"].at(1).value("price", 0.0) - 45.09) < 0.0001,
           "replay snapshot should preserve deeper bid levels");
    expect(std::fabs(response.summary["ask_book"].at(0).value("size", 0.0) - 150.0) < 0.0001,
           "replay snapshot should preserve updated ask size");
    const tape_engine::EngineSnapshot replaySnapshot = server.snapshot();
    expect(fs::exists(rootDir / "segments" / replaySnapshot.segments.front().checkpointFileName),
           "replay snapshot test should have a persisted checkpoint file to reuse");

    server.stop();
}

void testBridgeMarketDataEmissionExpandsPublicEvents() {
    clearTestFiles();

    SharedData owner;
    bindSharedDataOwner(&owner);

    trading_engine::reduce(appState(), trading_engine::MarketSubscriptionStartedEvent{
        "INTC",
        "ib:conid:9101:STK:SMART:USD:INTC",
        9101,
        9201,
        false
    });
    trading_engine::reduce(appState(), trading_engine::BrokerTickPriceEvent{9101, static_cast<TickType>(1), 45.10});
    trading_engine::reduce(appState(), trading_engine::BrokerTickPriceEvent{9101, static_cast<TickType>(2), 45.12});
    trading_engine::reduce(appState(), trading_engine::BrokerTickPriceEvent{9101, static_cast<TickType>(4), 45.11});
    trading_engine::reduce(appState(), trading_engine::BrokerMarketDepthEvent{9201, 0, 0, 0, 45.12, 200.0});
    trading_engine::reduce(appState(), trading_engine::BrokerMarketDepthEvent{9201, 0, 0, 1, 45.10, 300.0});
    publishSharedDataSnapshot();

    const BridgeDispatchSnapshot dispatch = captureBridgeDispatchSnapshot(10);
    expect(dispatch.records.size() >= 5, "public market capture should enqueue quote and depth bridge records");

    const auto bidTick = std::find_if(dispatch.records.begin(), dispatch.records.end(), [](const BridgeOutboxRecord& record) {
        return record.recordType == "market_tick" && record.marketField == 1;
    });
    expect(bidTick != dispatch.records.end(), "bridge outbox should include a bid market_tick record");
    expect(bidTick->instrumentId == "ib:conid:9101:STK:SMART:USD:INTC", "market tick records should preserve the canonical instrument identity");
    expect(bidTick->side == "BID", "market tick record should preserve the bid side label");
    expect(std::fabs(bidTick->price - 45.10) < 0.0001, "market tick record should preserve the quote price");

    const auto askDepth = std::find_if(dispatch.records.begin(), dispatch.records.end(), [](const BridgeOutboxRecord& record) {
        return record.recordType == "market_depth" && record.bookSide == 0;
    });
    expect(askDepth != dispatch.records.end(), "bridge outbox should include ask-side market_depth records");
    expect(askDepth->bookOperation == 0 && askDepth->bookPosition == 0, "market depth record should preserve position and operation");
    expect(std::fabs(askDepth->size - 200.0) < 0.0001, "market depth record should preserve size");

    unbindSharedDataOwner(&owner);
    resetSharedDataForTesting();
}

void testGeneratedRuntimeRegistryMatchesPhase15QueueSpec() {
    expect(runtime_registry::kRegistryVersion == 1, "generated runtime registry should expose version 1");

    const auto bridgeSender = runtime_registry::queueSpec(runtime_registry::QueueId::BridgeSender);
    expect(bridgeSender.label == "com.foxy.long.bridge.sender", "generated runtime registry should preserve the bridge sender label");
    expect(bridgeSender.qosName == "userInitiated", "generated runtime registry should preserve bridge sender QoS");
    expect(runtime_registry::logCategoryName(bridgeSender.category) == "bridge", "generated runtime registry should preserve bridge sender category");

    const auto engineWriter = runtime_registry::queueSpec(runtime_registry::QueueId::EngineSegmentWriter);
    expect(engineWriter.label == "com.foxy.tape-engine.segment-writer", "generated runtime registry should preserve the engine writer label");
    expect(engineWriter.qosName == "utility", "generated runtime registry should preserve engine writer QoS");
    expect(runtime_registry::subsystemName(engineWriter.subsystem) == "com.foxy.tape-engine", "generated runtime registry should preserve the engine subsystem name");

    const auto tapescopeMain = runtime_registry::queueSpec(runtime_registry::QueueId::TapescopeMainUi);
    expect(tapescopeMain.label == "com.foxy.tapescope.main", "generated runtime registry should include the TapeScope main UI queue");
    expect(tapescopeMain.qosName == "userInteractive", "generated runtime registry should preserve TapeScope UI QoS");

    const auto tapeMcpReads = runtime_registry::queueSpec(runtime_registry::QueueId::TapeMcpReads);
    expect(tapeMcpReads.label == "com.foxy.tape-mcp.reads", "generated runtime registry should include the MCP read worker queue");
    expect(runtime_registry::subsystemName(tapeMcpReads.subsystem) == "com.foxy.tape-mcp", "generated runtime registry should preserve the MCP subsystem name");
}

void testBridgeLifecycleEmissionExpandsPrivateOrderEvents() {
    clearTestFiles();

    SharedData owner;
    bindSharedDataOwner(&owner);

    SubmitIntent intent;
    intent.source = "WebSocket";
    intent.symbol = "INTC";
    intent.side = "BUY";
    intent.requestedQty = 1;
    intent.limitPrice = 45.70;
    intent.notes = "accepted order intent";

    const std::uint64_t traceId = beginTradeTrace(intent);
    bindTraceToOrder(traceId, 701);
    bindTraceToPermId(701, 9801);

    Contract contract;
    contract.symbol = "INTC";

    Order order;
    order.orderId = 701;
    order.permId = 9801;
    order.action = "BUY";
    order.totalQuantity = DecimalFunctions::doubleToDecimal(1.0);
    order.lmtPrice = 45.70;
    order.account = "U23154741";

    OrderState orderState;
    orderState.status = "Submitted";
    recordTraceOpenOrder(701, contract, order, orderState);
    recordTraceOrderStatus(701, "Submitted", 0.0, 1.0, 0.0, 9801, 0.0, 0.0);

    Execution execution;
    execution.orderId = 701;
    execution.permId = 9801;
    execution.execId = "EXEC-91";
    execution.side = "BOT";
    execution.shares = DecimalFunctions::doubleToDecimal(1.0);
    execution.price = 45.70;
    execution.exchange = "SMART";
    execution.cumQty = DecimalFunctions::doubleToDecimal(1.0);
    execution.avgPrice = 45.70;
    execution.time = "20260314 09:38:00";
    recordTraceExecution(contract, execution);

    CommissionReport commission;
    commission.execId = "EXEC-91";
#if defined(TWS_HAS_COMMISSION_AND_FEES_REPORT)
    commission.commissionAndFees = 0.45;
#else
    commission.commission = 0.45;
#endif
    commission.currency = "USD";
    recordTraceCommission(commission);

    recordTraceCancelRequest(701);
    recordTraceError(701, 399, "warning path");
    recordTraceError(701, 201, "rejected path");

    const BridgeDispatchSnapshot dispatch = captureBridgeDispatchSnapshot(20);
    expect(dispatch.records.size() >= 7, "expanded lifecycle emission should enqueue the widened private-event set");

    std::vector<std::string> recordTypes;
    recordTypes.reserve(dispatch.records.size());
    for (const auto& record : dispatch.records) {
        recordTypes.push_back(record.recordType);
    }

    expect(std::find(recordTypes.begin(), recordTypes.end(), "open_order") != recordTypes.end(),
           "bridge outbox should include open_order records");
    expect(std::find(recordTypes.begin(), recordTypes.end(), "order_status") != recordTypes.end(),
           "bridge outbox should include order_status records");
    expect(std::find(recordTypes.begin(), recordTypes.end(), "fill_execution") != recordTypes.end(),
           "bridge outbox should include fill_execution records");
    expect(std::find(recordTypes.begin(), recordTypes.end(), "commission_report") != recordTypes.end(),
           "bridge outbox should include commission_report records");
    expect(std::find(recordTypes.begin(), recordTypes.end(), "cancel_request") != recordTypes.end(),
           "bridge outbox should include cancel_request records");
    expect(std::find(recordTypes.begin(), recordTypes.end(), "broker_error") != recordTypes.end(),
           "bridge outbox should include general broker_error records");
    expect(std::find(recordTypes.begin(), recordTypes.end(), "order_reject") != recordTypes.end(),
           "bridge outbox should include rejection-specific order_reject records");

    const auto fillRecord = std::find_if(dispatch.records.begin(), dispatch.records.end(), [](const BridgeOutboxRecord& record) {
        return record.recordType == "fill_execution";
    });
    expect(fillRecord != dispatch.records.end(), "expanded lifecycle emission should include a fill_execution record to inspect");
    expect(fillRecord->instrumentId == "ib:STK:SMART:USD:INTC", "lifecycle bridge records should preserve canonical instrument identity even before a live conId arrives");

    unbindSharedDataOwner(&owner);
    resetSharedDataForTesting();
}

void testBridgeOutboxOverflowWritesExplicitLossMarker() {
    clearTestFiles();

    SharedData owner;
    bindSharedDataOwner(&owner);

    BridgeOutboxRecordInput firstRecord;
    firstRecord.recordType = "fill_execution";
    firstRecord.source = "BrokerExecution";
    firstRecord.symbol = "INTC";
    firstRecord.side = "BOT";
    firstRecord.traceId = 301;
    firstRecord.orderId = 901;
    firstRecord.permId = 9901;
    firstRecord.execId = "EXEC-DROP";
    firstRecord.note = "first record should become the explicit loss marker";
    const BridgeOutboxEnqueueResult first = enqueueBridgeOutboxRecord(firstRecord);

    BridgeOutboxSnapshot snapshot = captureBridgeOutboxSnapshot(1);
    int enqueued = 1;
    while (snapshot.lossCount == 0 && enqueued < 2000) {
        BridgeOutboxRecordInput record;
        record.recordType = "fill_execution";
        record.source = "BrokerExecution";
        record.symbol = "INTC";
        record.side = "BOT";
        record.traceId = 301 + enqueued;
        record.orderId = 901 + enqueued;
        record.permId = 9901 + enqueued;
        record.execId = "EXEC-" + std::to_string(enqueued);
        record.note = "overflow probe";
        enqueueBridgeOutboxRecord(record);
        ++enqueued;
        snapshot = captureBridgeOutboxSnapshot(1);
    }

    expect(snapshot.lossCount > 0, "bridge overflow should surface an explicit continuity-loss count");
    expect(snapshot.recoveryRequired, "bridge overflow should keep recovery explicitly required");
    expect(snapshot.pendingCount < enqueued, "bridge overflow should not silently pretend all queued records are still present");

    const std::vector<json> journalLines = readJsonLines(runtimeJournalLogPath());
    const auto lossIt = std::find_if(journalLines.begin(),
                                     journalLines.end(),
                                     [](const json& line) {
                                         return line.value("event", std::string()) == "bridge_outbox_loss";
                                     });
    expect(lossIt != journalLines.end(), "bridge overflow should write a bridge_outbox_loss journal event");
    const json details = lossIt->contains("details") && (*lossIt)["details"].is_object()
        ? (*lossIt)["details"]
        : json::object();
    expect(details.value("reason", std::string()) == "queue_overflow", "loss marker should explain the continuity break");
    expect(details.value("droppedSourceSeq", 0ULL) == first.sourceSeq, "loss marker should identify the dropped source_seq");
    expect(details.value("traceId", 0ULL) == firstRecord.traceId, "loss marker should preserve dropped traceId");
    expect(details.value("orderId", 0LL) == static_cast<long long>(firstRecord.orderId), "loss marker should preserve dropped orderId");
    expect(details.value("permId", 0LL) == firstRecord.permId, "loss marker should preserve dropped permId");
    expect(details.value("execId", std::string()) == firstRecord.execId, "loss marker should preserve dropped execId");

    unbindSharedDataOwner(&owner);
    resetSharedDataForTesting();
}

void testRecoverySnapshotReportsBridgeContinuityLossAfterAbnormalShutdown() {
    clearTestFiles();

    appendJournalLine(json{
        {"event", "runtime_start"},
        {"wallTime", "2026-03-13T09:30:00.000"},
        {"appSessionId", "app-bridge-recovery"},
        {"runtimeSessionId", "runtime-bridge-recovery"}
    });
    appendJournalLine(json{
        {"event", "bridge_outbox_queued"},
        {"wallTime", "2026-03-13T09:30:01.000"},
        {"appSessionId", "app-bridge-recovery"},
        {"runtimeSessionId", "runtime-bridge-recovery"},
        {"details", json{{"sourceSeq", 41ULL}, {"traceId", 81ULL}, {"orderId", 601LL}}}
    });
    appendJournalLine(json{
        {"event", "bridge_outbox_queued"},
        {"wallTime", "2026-03-13T09:30:02.000"},
        {"appSessionId", "app-bridge-recovery"},
        {"runtimeSessionId", "runtime-bridge-recovery"},
        {"details", json{{"sourceSeq", 42ULL}, {"traceId", 81ULL}, {"orderId", 601LL}, {"permId", 9601LL}, {"execId", "EXEC-81"}}}
    });
    appendJournalLine(json{
        {"event", "bridge_outbox_queued"},
        {"wallTime", "2026-03-13T09:30:02.500"},
        {"appSessionId", "app-bridge-recovery"},
        {"runtimeSessionId", "runtime-bridge-recovery"},
        {"details", json{{"sourceSeq", 43ULL}, {"traceId", 82ULL}, {"orderId", 602LL}, {"permId", 9602LL}, {"execId", "EXEC-82"}}}
    });
    appendJournalLine(json{
        {"event", "bridge_outbox_delivered"},
        {"wallTime", "2026-03-13T09:30:03.000"},
        {"appSessionId", "app-bridge-recovery"},
        {"runtimeSessionId", "runtime-bridge-recovery"},
        {"details", json{{"sourceSeq", 41ULL}}}
    });
    appendJournalLine(json{
        {"event", "bridge_outbox_loss"},
        {"wallTime", "2026-03-13T09:30:04.000"},
        {"appSessionId", "app-bridge-recovery"},
        {"runtimeSessionId", "runtime-bridge-recovery"},
        {"details", json{{"reason", "queue_overflow"}, {"droppedSourceSeq", 42ULL}, {"traceId", 81ULL}, {"orderId", 601LL}, {"permId", 9601LL}, {"execId", "EXEC-81"}}}
    });

    const RuntimeRecoverySnapshot snapshot = recoverRuntimeRecoverySnapshot(5);
    expect(snapshot.priorSessionAbnormal, "bridge recovery snapshot should flag abnormal prior shutdown");
    expect(snapshot.priorAppSessionId == "app-bridge-recovery", "bridge recovery snapshot should preserve prior app session id");
    expect(snapshot.priorRuntimeSessionId == "runtime-bridge-recovery", "bridge recovery snapshot should preserve prior runtime session id");
    expect(snapshot.pendingOutboxCount == 1, "bridge recovery snapshot should exclude explicitly lost queued records from pending recovery work");
    expect(snapshot.outboxLossCount == 1, "bridge recovery snapshot should count explicit continuity-loss markers");
    expect(snapshot.lastOutboxSourceSeq == 43, "bridge recovery snapshot should preserve the highest accepted source_seq");
    expect(snapshot.bridgeRecoveryRequired, "bridge recovery snapshot should require recovery when continuity was lost");
    expectContains(snapshot.bannerText, "Previous session ended unexpectedly", "bridge recovery banner should mention abnormal shutdown");
    expectContains(snapshot.bannerText, "Bridge recovery pending: 1 queued intent", "bridge recovery banner should mention pending bridge work");
    expectContains(snapshot.bannerText, "1 loss marker", "bridge recovery banner should mention explicit continuity loss");
    expectContains(snapshot.bannerText, "last source_seq=43", "bridge recovery banner should report the last accepted source_seq");
}
