void testReplayPrefersRichLiveTrace() {
    clearTestFiles();

    appendTraceLine(makeTraceLine(7, 113, 5001, "GUI Button", "INTC", "BUY",
                                  "Trigger", "GUI Button", "BUY 1 INTC @ 45.64", 0.0));
    appendTraceLine(makeTraceLine(7, 113, 5001, "GUI Button", "INTC", "BUY",
                                  "ValidationStart", "Validation", "Starting local order validation", 1.0));
    appendTraceLine(makeTraceLine(7, 113, 5001, "GUI Button", "INTC", "BUY",
                                  "ValidationOk", "Validation", "Validation passed", 4.0));
    appendTraceLine(makeTraceLine(7, 113, 5001, "GUI Button", "INTC", "BUY",
                                  "PlaceOrderCallStart", "placeOrder", "placeOrder start", 7.0));
    appendTraceLine(makeTraceLine(7, 113, 5001, "GUI Button", "INTC", "BUY",
                                  "PlaceOrderCallEnd", "placeOrder", "placeOrder return", 10.0));
    appendTraceLine(makeTraceLine(7, 113, 5001, "GUI Button", "INTC", "BUY",
                                  "OrderStatusSeen", "Submitted", "submitted", 28.0));
    appendTraceLine(makeTraceLine(7, 113, 5001, "GUI Button", "INTC", "BUY",
                                  "ExecDetailsSeen", "execDetails", "exch=SMART execId=E1 time=20260313 09:30:02",
                                  2930.0, 45.64, 1, 1.0));
    appendTraceLine(makeTraceLine(7, 113, 5001, "GUI Button", "INTC", "BUY",
                                  "CommissionSeen", "commissionReport", "E1 commission=0.4564 USD", 2935.0));
    appendTraceLine(makeTraceLine(7, 113, 5001, "GUI Button", "INTC", "BUY",
                                  "FinalState", "Terminal", "Filled: order complete", 2936.0));

    appendTraceLine(makeTraceLine(100, 113, 5001, "Broker Reconcile", "INTC", "BUY",
                                  "Note", "Recovered", "Recovered execution during startup reconciliation", 0.0));
    appendTraceLine(makeTraceLine(100, 113, 5001, "Broker Reconcile", "INTC", "BUY",
                                  "ExecDetailsSeen", "execDetails", "exch=SMART execId=E1 time=20260313 09:30:02",
                                  0.05, 45.64, 1, 1.0));

    {
        std::lock_guard<std::recursive_mutex> lock(g_data.mutex);
        TradeTrace recovered;
        recovered.traceId = 100;
        recovered.orderId = 113;
        recovered.permId = 5001;
        recovered.source = "Broker Reconcile";
        recovered.symbol = "INTC";
        recovered.side = "BUY";
        recovered.requestedQty = 1;
        recovered.limitPrice = 45.64;
        FillSlice fill;
        fill.execId = "E1";
        fill.shares = 1;
        fill.price = 45.64;
        recovered.fills.push_back(fill);
        g_data.traces[recovered.traceId] = recovered;
        g_data.traceRecency.push_back(recovered.traceId);
        g_data.latestTraceId = recovered.traceId;
        g_data.traceIdByOrderId[recovered.orderId] = recovered.traceId;
        g_data.traceIdByPermId[recovered.permId] = recovered.traceId;
        g_data.traceIdByExecId["E1"] = recovered.traceId;
    }
    publishSharedDataSnapshot();

    TradeTraceSnapshot replayed;
    expect(replayTradeTraceSnapshotByIdentityFromLog(113, 5001, "E1", &replayed),
           "replay by identity should succeed");
    expect(replayed.found, "replayed snapshot should be found");
    expect(replayed.trace.traceId == 7, "replay should prefer the richer live trace");
    expect(replayed.trace.source == "GUI Button", "replay should use live source");

    TraceExportBundle bundle;
    std::string error;
    expect(buildTraceExportBundle(100, &bundle, &error), "export bundle should succeed: " + error);
    expectContains(bundle.summaryCsv, "GUI Button", "summary should be enriched from live trace");
    expectContains(bundle.summaryCsv, "2936", "summary should contain full fill latency");
    expectContains(bundle.timelineCsv, "ValidationStart", "timeline should include live validation stages");
}

void testBridgeBatchCodecRoundTripPreservesOrderAndMetadata() {
    const std::vector<BridgeOutboxRecord> records{
        makeBridgeRecord(101, "order_intent", "WebSocket", "INTC", "BUY",
                         71, 501, 0, "", "accepted order intent", "2026-03-14T09:30:00.100"),
        makeBridgeRecord(102, "fill_execution", "BrokerExecution", "INTC", "BOT",
                         71, 501, 9501, "EXEC-71", "execution details observed", "2026-03-14T09:30:00.250")
    };

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-phase1-order-fill";
    options.runtimeSessionId = "runtime-phase1-order-fill";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;
    options.batchSeq = 7;

    const bridge_batch::Batch batch = bridge_batch::buildBatch(records, options);
    const std::vector<std::uint8_t> frame = bridge_batch::encodeFrame(batch);
    const bridge_batch::Batch decoded = bridge_batch::decodeFrame(frame);

    expect(decoded.header.version == bridge_batch::kWireVersion, "wire version should survive codec round trip");
    expect(decoded.header.schema == bridge_batch::kSchemaName, "schema name should survive codec round trip");
    expect(decoded.header.transport == bridge_batch::kTransportName, "transport name should survive codec round trip");
    expect(decoded.header.appSessionId == options.appSessionId, "app session id should survive codec round trip");
    expect(decoded.header.runtimeSessionId == options.runtimeSessionId, "runtime session id should survive codec round trip");
    expect(decoded.header.flushReason == bridge_batch::flushReasonName(options.flushReason), "flush reason should survive codec round trip");
    expect(decoded.header.batchSeq == options.batchSeq, "batch sequence should survive codec round trip");
    expect(decoded.header.firstSourceSeq == 101, "first source sequence should reflect the first accepted record");
    expect(decoded.header.lastSourceSeq == 102, "last source sequence should reflect the last accepted record");
    expect(decoded.header.recordCount == records.size(), "record count should reflect the number of encoded records");
    expect(decoded.records.size() == records.size(), "codec round trip should preserve record count");
    expect(decoded.records.front().sourceSeq == 101, "codec round trip should preserve accepted ordering");
    expect(decoded.records.back().sourceSeq == 102, "codec round trip should preserve tail ordering");
    expect(decoded.records.front().anchor.traceId == 71, "codec round trip should preserve first trace anchor");
    expect(decoded.records.back().anchor.permId == 9501, "codec round trip should preserve fill permId");
    expect(decoded.records.back().anchor.execId == "EXEC-71", "codec round trip should preserve fill execId");
    expect(decoded.records.back().fallbackState == "queued_for_recovery", "codec round trip should preserve fallback state");
}

void testTraceIdFloorRecoversFromLog() {
    clearTestFiles();
    appendTraceLine(makeTraceLine(42, 0, 0, "GUI Button", "AAPL", "BUY",
                                  "Trigger", "GUI Button", "BUY 1 AAPL @ 100.00", 0.0));

    SharedData owner;
    bindSharedDataOwner(&owner);

    SubmitIntent intent;
    intent.source = "Test";
    intent.symbol = "MSFT";
    intent.side = "BUY";
    intent.requestedQty = 1;
    intent.limitPrice = 10.0;
    intent.triggerMono = std::chrono::steady_clock::now();
    intent.triggerWall = std::chrono::system_clock::now();

    const std::uint64_t traceId = beginTradeTrace(intent);
    expect(traceId > 42, "next trace id should advance beyond recovered log ids");

    unbindSharedDataOwner(&owner);
    resetSharedDataForTesting();
}

void testReplayHandlesPartialFillsAndCommission() {
    clearTestFiles();

    appendTraceLine(makeTraceLine(9, 114, 6001, "Controller", "INTC", "SELL",
                                  "Trigger", "Controller", "SELL 2 INTC @ 45.65", 0.0));
    appendTraceLine(makeTraceLine(9, 114, 6001, "Controller", "INTC", "SELL",
                                  "PlaceOrderCallEnd", "placeOrder", "placeOrder return", 12.0));
    appendTraceLine(makeTraceLine(9, 114, 6001, "Controller", "INTC", "SELL",
                                  "OrderStatusSeen", "Submitted", "submitted", 20.0));
    appendTraceLine(makeTraceLine(9, 114, 6001, "Controller", "INTC", "SELL",
                                  "ExecDetailsSeen", "execDetails", "exch=SMART execId=E2 time=20260313 09:31:01",
                                  800.0, 45.65, 1, 1.0));
    appendTraceLine(makeTraceLine(9, 114, 6001, "Controller", "INTC", "SELL",
                                  "CommissionSeen", "commissionReport", "E2 commission=0.3000 USD", 801.0));
    appendTraceLine(makeTraceLine(9, 114, 6001, "Controller", "INTC", "SELL",
                                  "ExecDetailsSeen", "execDetails", "exch=SMART execId=E3 time=20260313 09:31:02",
                                  1738.0, 45.66, 1, 2.0));
    appendTraceLine(makeTraceLine(9, 114, 6001, "Controller", "INTC", "SELL",
                                  "CommissionSeen", "commissionReport", "E3 commission=0.1567 USD", 1739.0));
    appendTraceLine(makeTraceLine(9, 114, 6001, "Controller", "INTC", "SELL",
                                  "FinalState", "Terminal", "Filled: order complete", 1740.0));

    TradeTraceSnapshot snapshot;
    std::string error;
    expect(replayTradeTraceSnapshotFromLog(9, &snapshot, &error), "replay should succeed: " + error);
    expect(snapshot.found, "snapshot should be found");
    expect(snapshot.trace.fills.size() == 2, "replay should preserve partial fills");
    expect(snapshot.trace.totalCommission > 0.45, "replay should accumulate commission");

    TraceExportBundle bundle;
    expect(buildTraceExportBundle(9, &bundle, &error), "export should succeed: " + error);
    expectContains(bundle.fillsCsv, "E2", "fills csv should contain first fill");
    expectContains(bundle.fillsCsv, "E3", "fills csv should contain second fill");
    expectContains(bundle.summaryCsv, "1740", "summary should contain trigger-to-fill latency");
}

void testWebSocketRuntimeGuards() {
    clearTestFiles();

    std::string error;
    expect(reserveWebSocketIdempotencyKey("abc-123", &error), "first idempotency key reserve should succeed");
    expect(!reserveWebSocketIdempotencyKey("abc-123", &error), "duplicate idempotency key should fail");
    expectContains(error, "Duplicate", "duplicate idempotency error should be descriptive");

    for (int i = 0; i < 5; ++i) {
        expect(consumeWebSocketOrderRateLimit(&error), "rate limit should allow initial order burst");
    }
    expect(!consumeWebSocketOrderRateLimit(&error), "rate limit should reject excessive order burst");
    expectContains(error, "Too many WebSocket order requests", "rate limit error should be descriptive");
}

void testRecoverySnapshotReportsAbnormalShutdown() {
    clearTestFiles();

    appendTraceLine(makeTraceLine(21, 115, 0, "Controller", "INTC", "BUY",
                                  "Trigger", "Controller", "BUY 1 INTC @ 45.64", 0.0));
    appendJournalLine(json{
        {"event", "runtime_start"},
        {"wallTime", "2026-03-13T09:30:00.000"},
        {"appSessionId", "app-test-session"},
        {"runtimeSessionId", "runtime-test-session"}
    });

    const RuntimeRecoverySnapshot snapshot = recoverRuntimeRecoverySnapshot(5);
    expect(snapshot.priorSessionAbnormal, "recovery snapshot should flag abnormal prior session");
    expect(snapshot.priorAppSessionId == "app-test-session", "recovery snapshot should preserve prior app session id");
    expect(snapshot.priorRuntimeSessionId == "runtime-test-session", "recovery snapshot should preserve prior runtime session id");
    expect(snapshot.unfinishedTraceCount == 1, "recovery snapshot should count unfinished traces");
    expectContains(snapshot.bannerText, "Previous session ended unexpectedly", "recovery banner should mention abnormal shutdown");
    expectContains(snapshot.bannerText, "1 unfinished trace", "recovery banner should mention unfinished trace count");
}

void testBridgeOutboxSourceSeqPreservesAcceptanceOrderingAndAnchors() {
    clearTestFiles();

    appendJournalLine(json{
        {"event", "runtime_start"},
        {"wallTime", "2026-03-13T09:30:00.000"},
        {"appSessionId", "app-ordering-session"},
        {"runtimeSessionId", "runtime-ordering-session"}
    });
    appendJournalLine(json{
        {"event", "bridge_outbox_queued"},
        {"wallTime", "2026-03-13T09:30:02.000"},
        {"appSessionId", "app-ordering-session"},
        {"runtimeSessionId", "runtime-ordering-session"},
        {"details", json{{"sourceSeq", 9ULL}}}
    });
    appendJournalLine(json{
        {"event", "bridge_outbox_queued"},
        {"wallTime", "2026-03-13T09:29:59.000"},
        {"appSessionId", "app-ordering-session"},
        {"runtimeSessionId", "runtime-ordering-session"},
        {"details", json{{"sourceSeq", 12ULL}}}
    });

    SharedData owner;
    bindSharedDataOwner(&owner);

    BridgeOutboxRecordInput intentRecord;
    intentRecord.recordType = "order_intent";
    intentRecord.source = "WebSocket";
    intentRecord.symbol = "intc";
    intentRecord.side = "BUY";
    intentRecord.traceId = 71;
    intentRecord.orderId = 501;
    intentRecord.note = "accepted order intent";
    const BridgeOutboxEnqueueResult accepted = enqueueBridgeOutboxRecord(intentRecord);

    BridgeOutboxRecordInput fillRecord;
    fillRecord.recordType = "fill_execution";
    fillRecord.source = "BrokerExecution";
    fillRecord.symbol = "intc";
    fillRecord.side = "BOT";
    fillRecord.traceId = 71;
    fillRecord.orderId = 501;
    fillRecord.permId = 9501;
    fillRecord.execId = "EXEC-71";
    fillRecord.note = "execution details observed";
    const BridgeOutboxEnqueueResult filled = enqueueBridgeOutboxRecord(fillRecord);

    const BridgeOutboxSnapshot snapshot = captureBridgeOutboxSnapshot(10);
    expect(accepted.queued, "accepted intent should be queued for bridge recovery");
    expect(filled.queued, "fill execution should be queued for bridge recovery");
    expect(accepted.sourceSeq == 13, "source_seq should continue from the highest recovered journal sequence");
    expect(filled.sourceSeq == 14, "source_seq should advance in acceptance order");
    expect(snapshot.pendingCount == 2, "bridge snapshot should report queued records");
    expect(snapshot.lossCount == 0, "bridge snapshot should not invent loss markers");
    expect(snapshot.recoveryRequired, "bridge snapshot should require recovery while records are pending");
    expect(snapshot.lastSourceSeq == filled.sourceSeq, "bridge snapshot should track the highest accepted source_seq");
    expect(snapshot.records.size() == 2, "bridge snapshot should include both queued records");

    const BridgeOutboxRecord& newest = snapshot.records.front();
    const BridgeOutboxRecord& oldest = snapshot.records.back();
    expect(newest.sourceSeq == filled.sourceSeq, "bridge snapshot should expose the newest accepted record first");
    expect(oldest.sourceSeq == accepted.sourceSeq, "older accepted records should retain lower source_seq values");
    expect(oldest.anchor.traceId == 71, "accepted order intent should preserve traceId");
    expect(oldest.anchor.orderId == 501, "accepted order intent should preserve orderId");
    expect(newest.anchor.traceId == 71, "fill execution should preserve traceId");
    expect(newest.anchor.orderId == 501, "fill execution should preserve orderId");
    expect(newest.anchor.permId == 9501, "fill execution should preserve permId");
    expect(newest.anchor.execId == "EXEC-71", "fill execution should preserve execId");
    expect(newest.symbol == "INTC", "bridge snapshot should normalize symbols without changing acceptance order");
    expect(newest.fallbackState == "queued_for_recovery", "bridge snapshot should surface explicit fallback state");
    expect(newest.fallbackReason == "engine_unavailable", "bridge snapshot should surface explicit fallback reason");

    unbindSharedDataOwner(&owner);
    resetSharedDataForTesting();
}

void testBridgeBatchFixtureMatchesGoldenFrame() {
    const json fixtureJson = json::parse(readTextFile(fixturePath("bridge_batch_v1_order_fill.json")));
    const bridge_batch::Batch fixtureBatch = bridge_batch::batchFromJson(fixtureJson);
    const std::string expectedHex = trimWhitespace(readTextFile(fixturePath("bridge_batch_v1_order_fill.msgpack.hex")));
    const std::string actualHex = bridge_batch::encodeFrameHex(fixtureBatch);

    expect(actualHex == expectedHex, "golden frame should match the fixture-backed bridge batch contract");

    const bridge_batch::Batch decoded = bridge_batch::decodeFrame(bridge_batch::decodeHex(expectedHex));
    expect(decoded.header.batchSeq == 7, "golden frame should decode the expected batch sequence");
    expect(decoded.header.firstSourceSeq == 101, "golden frame should decode the expected first source sequence");
    expect(decoded.header.lastSourceSeq == 102, "golden frame should decode the expected last source sequence");
    expect(decoded.records.size() == 2, "golden frame should decode both bridge records");
    expect(decoded.records.front().recordType == "order_intent", "golden frame should keep the first record type");
    expect(decoded.records.back().recordType == "fill_execution", "golden frame should keep the second record type");
    expect(decoded.records.back().anchor.execId == "EXEC-71", "golden frame should keep the fill execution anchor");
}

void testBridgeBatchSenderPreservesOrderingAcrossRetries() {
    bridge_batch::FakeTransport transport;
    bridge_batch::Sender sender(transport);

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-phase1-order-fill";
    options.runtimeSessionId = "runtime-phase1-order-fill";
    options.flushReason = bridge_batch::FlushReason::RecoveryDrain;

    options.batchSeq = 1;
    const bridge_batch::Batch first = bridge_batch::buildBatch({
        makeBridgeRecord(201, "order_intent", "WebSocket", "INTC", "BUY",
                         88, 601, 0, "", "first batch", "2026-03-14T09:31:00.000")
    }, options);

    options.batchSeq = 2;
    const bridge_batch::Batch second = bridge_batch::buildBatch({
        makeBridgeRecord(202, "fill_execution", "BrokerExecution", "INTC", "BOT",
                         88, 601, 9601, "EXEC-88-A", "second batch", "2026-03-14T09:31:00.150")
    }, options);

    options.batchSeq = 3;
    const bridge_batch::Batch third = bridge_batch::buildBatch({
        makeBridgeRecord(203, "fill_execution", "BrokerExecution", "INTC", "BOT",
                         88, 601, 9601, "EXEC-88-B", "third batch", "2026-03-14T09:31:00.300")
    }, options);

    transport.failNext("engine offline");
    const bridge_batch::PublishResult firstResult = sender.publish(first);
    expect(!firstResult.delivered, "first publish should fail while transport is unavailable");
    expect(firstResult.queuedForRetry, "failed first publish should remain queued for retry");
    expect(firstResult.pendingBatchCount == 1, "first failed publish should leave one pending batch");

    const bridge_batch::PublishResult secondResult = sender.publish(second);
    expect(!secondResult.delivered, "second publish should not bypass an earlier failed batch");
    expect(secondResult.queuedForRetry, "second publish should queue behind pending retry work");
    expect(secondResult.pendingBatchCount == 2, "second publish should extend the pending retry queue");

    const bridge_batch::PublishResult thirdResult = sender.publish(third);
    expect(!thirdResult.delivered, "third publish should wait behind earlier pending batches");
    expect(thirdResult.queuedForRetry, "third publish should queue behind earlier pending batches");
    expect(thirdResult.pendingBatchCount == 3, "third publish should leave all queued batches pending");

    const bridge_batch::DrainResult drain = sender.drainPending();
    expect(!drain.blocked, "draining pending batches should succeed once transport recovers");
    expect(drain.deliveredCount == 3, "drain should deliver every queued batch");
    expect(drain.pendingBatchCount == 0, "drain should empty the pending queue");
    expect(sender.pendingBatchCount() == 0, "sender should report no pending batches after drain");
    expect(transport.attemptedSendCount() == 4, "transport should see one failed attempt plus three successful retries");
    expect(transport.deliveredFrames().size() == 3, "transport should receive all three batches in delivery order");

    const bridge_batch::Batch deliveredFirst = bridge_batch::decodeFrame(transport.deliveredFrames().at(0));
    const bridge_batch::Batch deliveredSecond = bridge_batch::decodeFrame(transport.deliveredFrames().at(1));
    const bridge_batch::Batch deliveredThird = bridge_batch::decodeFrame(transport.deliveredFrames().at(2));
    expect(deliveredFirst.header.batchSeq == 1, "drain should preserve the first queued batch");
    expect(deliveredSecond.header.batchSeq == 2, "drain should preserve the second queued batch");
    expect(deliveredThird.header.batchSeq == 3, "drain should preserve the third queued batch");
    expect(deliveredFirst.records.front().sourceSeq == 201, "drain should preserve source ordering in the first batch");
    expect(deliveredSecond.records.front().sourceSeq == 202, "drain should preserve source ordering in the second batch");
    expect(deliveredThird.records.front().sourceSeq == 203, "drain should preserve source ordering in the third batch");
}

void testBridgeDispatchBatchRespectsThresholdsAndImmediateFlush() {
    bridge_batch::BuildOptions options;
    options.appSessionId = "app-dispatch-thresholds";
    options.runtimeSessionId = "runtime-dispatch-thresholds";
    options.flushReason = bridge_batch::FlushReason::Manual;
    options.batchSeq = 11;

    std::vector<BridgeOutboxRecord> burst;
    burst.reserve(70);
    for (std::size_t i = 0; i < 70; ++i) {
        burst.push_back(makeBridgeRecord(500 + i,
                                         "depth_update",
                                         "BookFeed",
                                         "INTC",
                                         "BUY",
                                         900 + i,
                                         700 + static_cast<OrderId>(i),
                                         0,
                                         "",
                                         "depth burst",
                                         "2026-03-14T09:32:00.000"));
    }

    bridge_batch::BatchPolicy policy;
    policy.maxRecords = 64;
    policy.maxPayloadBytes = 64 * 1024;
    const bridge_batch::PreparedBatch thresholdBatch = bridge_batch::prepareBatch(burst, options, policy);
    expect(thresholdBatch.batch.records.size() == 64, "prepareBatch should cap the batch at the record threshold");
    expect(thresholdBatch.reachedRecordLimit, "prepareBatch should report when the record threshold forced a flush");
    expect(!thresholdBatch.immediateFlush, "depth-only batches should wait for threshold or timer flush");

    const std::vector<BridgeOutboxRecord> immediateRecords{
        makeBridgeRecord(801, "order_intent", "WebSocket", "INTC", "BUY",
                         71, 501, 0, "", "accepted order intent", "2026-03-14T09:33:00.100"),
        makeBridgeRecord(802, "depth_update", "BookFeed", "INTC", "BUY",
                         71, 501, 0, "", "book follow-up", "2026-03-14T09:33:00.120")
    };
    const bridge_batch::PreparedBatch immediateBatch = bridge_batch::prepareBatch(immediateRecords, options, policy);
    expect(immediateBatch.immediateFlush, "order and fill lifecycle records should force immediate flush");
    expect(bridge_batch::recordTypeRequiresImmediateFlush("open_order"), "open_order should force immediate bridge flush");
    expect(bridge_batch::recordTypeRequiresImmediateFlush("order_status"), "order_status should force immediate bridge flush");
    expect(bridge_batch::recordTypeRequiresImmediateFlush("commission_report"), "commission_report should force immediate bridge flush");
    expect(bridge_batch::recordTypeRequiresImmediateFlush("broker_error"), "broker_error should force immediate bridge flush");
    expect(bridge_batch::recordTypeRequiresImmediateFlush("feed_reset"), "feed_reset should force immediate bridge flush");
    expect(bridge_batch::recordTypeRequiresImmediateFlush("gap_marker"), "gap_marker should force immediate bridge flush");
    expect(bridge_batch::recordTypeRequiresImmediateFlush("reset_marker"), "reset_marker should force immediate bridge flush");
    expect(!bridge_batch::recordTypeRequiresImmediateFlush("market_depth"), "market_depth should remain batchable");

    bridge_batch::BatchPolicy bytePolicy;
    bytePolicy.maxRecords = 64;
    bytePolicy.maxPayloadBytes = 420;
    const std::vector<BridgeOutboxRecord> byteBoundRecords{
        makeBridgeRecord(901, "depth_update", "BookFeed", "INTC", "BUY",
                         71, 501, 0, "", std::string(220, 'a'), "2026-03-14T09:34:00.100"),
        makeBridgeRecord(902, "depth_update", "BookFeed", "INTC", "BUY",
                         71, 501, 0, "", std::string(220, 'b'), "2026-03-14T09:34:00.120")
    };
    const bridge_batch::PreparedBatch byteBatch = bridge_batch::prepareBatch(byteBoundRecords, options, bytePolicy);
    expect(byteBatch.batch.records.size() == 1, "prepareBatch should stop before exceeding the payload threshold");
    expect(byteBatch.reachedPayloadLimit, "prepareBatch should report payload-triggered flushes");
}

void testBridgeDispatchSnapshotAndDeliveryAck() {
    clearTestFiles();

    SharedData owner;
    bindSharedDataOwner(&owner);

    BridgeOutboxRecordInput firstRecord;
    firstRecord.recordType = "order_intent";
    firstRecord.source = "WebSocket";
    firstRecord.symbol = "INTC";
    firstRecord.side = "BUY";
    firstRecord.traceId = 71;
    firstRecord.orderId = 501;
    firstRecord.note = "accepted order intent";
    const BridgeOutboxEnqueueResult first = enqueueBridgeOutboxRecord(firstRecord);

    BridgeOutboxRecordInput secondRecord;
    secondRecord.recordType = "fill_execution";
    secondRecord.source = "BrokerExecution";
    secondRecord.symbol = "INTC";
    secondRecord.side = "BOT";
    secondRecord.traceId = 71;
    secondRecord.orderId = 501;
    secondRecord.permId = 9501;
    secondRecord.execId = "EXEC-71";
    secondRecord.note = "execution details observed";
    const BridgeOutboxEnqueueResult second = enqueueBridgeOutboxRecord(secondRecord);

    const BridgeDispatchSnapshot dispatch = captureBridgeDispatchSnapshot(10);
    expect(dispatch.records.size() == 2, "dispatch snapshot should expose queued bridge records oldest-first");
    expect(dispatch.records.front().sourceSeq == first.sourceSeq, "dispatch snapshot should begin with the oldest queued record");
    expect(dispatch.records.back().sourceSeq == second.sourceSeq, "dispatch snapshot should preserve acceptance ordering");

    const std::size_t removedFirst = acknowledgeDeliveredBridgeRecords({dispatch.records.front()});
    expect(removedFirst == 1, "delivery ack should remove the delivered prefix record");
    BridgeOutboxSnapshot afterFirstAck = captureBridgeOutboxSnapshot(10);
    expect(afterFirstAck.pendingCount == 1, "delivery ack should decrement the pending outbox count");
    expect(afterFirstAck.records.size() == 1, "delivery ack should leave only the undelivered tail");
    expect(afterFirstAck.records.front().sourceSeq == second.sourceSeq, "delivery ack should preserve the remaining undelivered record");

    const std::size_t removedSecond = acknowledgeDeliveredBridgeRecords({dispatch.records.back()});
    expect(removedSecond == 1, "delivery ack should remove the final queued record");
    const BridgeOutboxSnapshot afterSecondAck = captureBridgeOutboxSnapshot(10);
    expect(afterSecondAck.pendingCount == 0, "delivery ack should clear the pending outbox count once all records are delivered");
    expect(!afterSecondAck.recoveryRequired, "delivery ack should clear recovery-required once no pending or lost records remain");
    expect(afterSecondAck.fallbackState == "live_delivery", "delivery ack should mark the bridge as live once the outbox is drained");
    expect(afterSecondAck.fallbackReason == "engine_connected", "delivery ack should surface the live bridge reason once drained");

    const std::vector<json> journalLines = readJsonLines(runtimeJournalLogPath());
    const auto deliveredCount = static_cast<int>(std::count_if(journalLines.begin(),
                                                               journalLines.end(),
                                                               [](const json& line) {
                                                                   return line.value("event", std::string()) == "bridge_outbox_delivered";
                                                               }));
    expect(deliveredCount == 2, "delivery ack should journal one delivery marker per delivered record");

    unbindSharedDataOwner(&owner);
    resetSharedDataForTesting();
}

void testUnixDomainSocketTransportSendsFramedBatch() {
    const fs::path socketPath = testDataDir() / "bridge-uds-test.sock";
    std::error_code ec;
    fs::remove(socketPath, ec);

    const int serverFd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    expect(serverFd >= 0, "unix-domain test server should create a socket");

    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    std::strncpy(address.sun_path, socketPath.c_str(), sizeof(address.sun_path) - 1);
    expect(::bind(serverFd, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) == 0,
           "unix-domain test server should bind successfully");
    expect(::listen(serverFd, 1) == 0, "unix-domain test server should listen successfully");

    std::vector<std::uint8_t> received;
    std::string serverError;
    std::thread server([&]() {
        const int clientFd = ::accept(serverFd, nullptr, nullptr);
        if (clientFd < 0) {
            serverError = "accept failed: " + std::string(std::strerror(errno));
            return;
        }
        received = readAllFromFd(clientFd);
        tape_engine::IngestAck ack;
        ack.batchSeq = 21;
        ack.adapterId = "long";
        ack.connectionId = "runtime-uds-transport";
        ack.acceptedRecords = 1;
        ack.firstSessionSeq = 1;
        ack.lastSessionSeq = 1;
        ack.firstSourceSeq = 1001;
        ack.lastSourceSeq = 1001;
        const std::vector<std::uint8_t> ackFrame = tape_engine::encodeAckFrame(ack);
        try {
            writeAllToFd(clientFd, ackFrame);
        } catch (const std::exception& error) {
            serverError = error.what();
        }
        ::close(clientFd);
    });

    const std::vector<BridgeOutboxRecord> records{
        makeBridgeRecord(1001, "order_intent", "WebSocket", "INTC", "BUY",
                         71, 501, 0, "", "accepted order intent", "2026-03-14T09:35:00.100")
    };
    bridge_batch::BuildOptions options;
    options.appSessionId = "app-uds-transport";
    options.runtimeSessionId = "runtime-uds-transport";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;
    options.batchSeq = 21;
    const bridge_batch::Batch batch = bridge_batch::buildBatch(records, options);

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;
    expect(transport.sendFrame(bridge_batch::encodeFrame(batch), &error), "unix-domain transport should deliver a framed batch: " + error);

    server.join();
    ::close(serverFd);
    fs::remove(socketPath, ec);
    expect(serverError.empty(), serverError);

    const bridge_batch::Batch decoded = bridge_batch::decodeFrame(received);
    expect(decoded.header.batchSeq == 21, "unix-domain transport should preserve the framed batch sequence");
    expect(decoded.records.size() == 1, "unix-domain transport should preserve the framed bridge record count");
    expect(decoded.records.front().sourceSeq == 1001, "unix-domain transport should preserve the framed source sequence");
}

void testTapeEngineAcceptsBatchAssignsSessionSeqAndWritesSegments() {
    const fs::path rootDir = testDataDir() / "tape-engine-phase1";
    const fs::path socketPath = testDataDir() / "tape-engine-phase1.sock";
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
    expect(server.start(&startError), "tape-engine should start: " + startError);

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-engine-phase1";
    options.runtimeSessionId = "runtime-engine-phase1";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;
    options.batchSeq = 31;
    const bridge_batch::Batch batch = bridge_batch::buildBatch({
        makeBridgeRecord(1101, "order_intent", "WebSocket", "INTC", "BUY",
                         71, 501, 0, "", "accepted order intent", "2026-03-14T09:36:00.100"),
        makeBridgeRecord(1102, "fill_execution", "BrokerExecution", "INTC", "BOT",
                         71, 501, 9501, "EXEC-71", "execution details observed", "2026-03-14T09:36:00.250")
    }, options);

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;
    expect(transport.sendFrame(bridge_batch::encodeFrame(batch), &error),
           "tape-engine should accept a real bridge batch: " + error);

    waitUntil([&]() {
        const auto snapshot = server.snapshot();
        return snapshot.segments.size() >= 1 && snapshot.latestFrozenRevisionId >= 1;
    }, "tape-engine should freeze the accepted batch into at least one segment");

    const tape_engine::EngineSnapshot snapshot = server.snapshot();
    expect(snapshot.nextSessionSeq == 3, "tape-engine should assign contiguous session_seq values to accepted records");
    expect(snapshot.liveEvents.size() == 2, "tape-engine live ring should retain accepted records");
    expect(snapshot.liveEvents.front().sessionSeq == 1, "tape-engine should assign the first session_seq");
    expect(snapshot.liveEvents.back().sessionSeq == 2, "tape-engine should assign the second session_seq");
    expect(snapshot.liveEvents.back().bridgeRecord.anchor.execId == "EXEC-71", "tape-engine should preserve bridge anchors");
    expect(snapshot.segments.size() >= 1, "tape-engine should emit at least one segment for the accepted batch");
    expect(snapshot.segments.front().revisionId == 1, "tape-engine should assign a frozen revision id to the segment");
    expect(snapshot.segments.front().fileName.find(".events.msgpack") != std::string::npos,
           "tape-engine should freeze segment payloads as binary msgpack files");
    expect(snapshot.segments.front().indexFileName.find(".index.msgpack") != std::string::npos,
           "tape-engine should persist a binary selector index alongside the segment payload");
    expect(snapshot.segments.front().checkpointFileName.find(".checkpoint.msgpack") != std::string::npos,
           "tape-engine should persist a binary replay checkpoint alongside the segment payload");
    expect(snapshot.segments.front().firstSessionSeq == 1, "tape-engine segment metadata should preserve the first session_seq");
    expect(snapshot.segments.front().lastSessionSeq == 2, "tape-engine segment metadata should preserve the last session_seq");
    expect(!snapshot.segments.front().payloadSha256.empty(), "tape-engine segment metadata should include a payload checksum");
    expect(!snapshot.segments.front().manifestHash.empty(), "tape-engine segment metadata should include a manifest hash");

    const fs::path segmentPath = rootDir / "segments" / snapshot.segments.front().fileName;
    const fs::path indexPath = rootDir / "segments" / snapshot.segments.front().indexFileName;
    const fs::path checkpointPath = rootDir / "segments" / snapshot.segments.front().checkpointFileName;
    const fs::path metadataPath = rootDir / "segments" / snapshot.segments.front().metadataFileName;
    const fs::path manifestPath = rootDir / "manifest.jsonl";
    expect(fs::exists(segmentPath), "tape-engine should write the segment payload file");
    expect(fs::exists(indexPath), "tape-engine should write the segment selector index file");
    expect(fs::exists(checkpointPath), "tape-engine should write the segment replay checkpoint file");
    expect(fs::exists(metadataPath), "tape-engine should write the segment metadata file");
    expect(fs::exists(manifestPath), "tape-engine should append the manifest hash chain");

    const std::vector<json> manifestLines = readJsonLines(manifestPath.string());
    expect(manifestLines.size() >= 1, "tape-engine should append at least one manifest line for the accepted batch");
    expect(manifestLines.front().value("manifest_hash", std::string()) == snapshot.segments.front().manifestHash,
           "tape-engine manifest hash should match the in-memory segment metadata");
    expect(manifestLines.front().value("index_file_name", std::string()) == snapshot.segments.front().indexFileName,
           "tape-engine manifest metadata should include the selector index file");
    expect(manifestLines.front().value("checkpoint_file_name", std::string()) == snapshot.segments.front().checkpointFileName,
           "tape-engine manifest metadata should include the replay checkpoint file");

    server.stop();
}

void testTapeEngineEmitsGapMarkersAndDeduplicatesSourceSeq() {
    const fs::path rootDir = testDataDir() / "tape-engine-gap";
    const fs::path socketPath = testDataDir() / "tape-engine-gap.sock";
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
    expect(server.start(&startError), "tape-engine should start for gap test: " + startError);

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-engine-gap";
    options.runtimeSessionId = "runtime-engine-gap";
    options.flushReason = bridge_batch::FlushReason::Manual;

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;

    options.batchSeq = 41;
    const bridge_batch::Batch first = bridge_batch::buildBatch({
        makeBridgeRecord(2001, "order_intent", "WebSocket", "INTC", "BUY",
                         71, 501, 0, "", "first", "2026-03-14T09:37:00.100")
    }, options);
    expect(transport.sendFrame(bridge_batch::encodeFrame(first), &error),
           "tape-engine should accept the first batch: " + error);

    options.batchSeq = 42;
    const bridge_batch::Batch duplicateAndGap = bridge_batch::buildBatch({
        makeBridgeRecord(2001, "order_intent", "WebSocket", "INTC", "BUY",
                         71, 501, 0, "", "duplicate", "2026-03-14T09:37:00.200"),
        makeBridgeRecord(2003, "fill_execution", "BrokerExecution", "INTC", "BOT",
                         71, 501, 9501, "EXEC-71", "gap after duplicate", "2026-03-14T09:37:00.300")
    }, options);
    expect(transport.sendFrame(bridge_batch::encodeFrame(duplicateAndGap), &error),
           "tape-engine should ignore duplicates across accepted history and still emit a gap marker: " + error);

    const tape_engine::EngineSnapshot snapshot = server.snapshot();
    expect(snapshot.liveEvents.size() == 3, "tape-engine should retain the original event, gap marker, and later event");
    expect(snapshot.liveEvents.at(1).eventKind == "gap_marker", "tape-engine should insert an explicit gap_marker event");
    expect(snapshot.liveEvents.at(1).gapStartSourceSeq == 2002, "gap marker should preserve the missing source_seq start");
    expect(snapshot.liveEvents.at(1).gapEndSourceSeq == 2002, "gap marker should preserve the missing source_seq end");
    expect(snapshot.liveEvents.at(2).sourceSeq == 2003, "tape-engine should still accept the later source_seq");

    server.stop();
}

void testTapeEngineQueryStatusAndReads() {
    const fs::path rootDir = testDataDir() / "tape-engine-query";
    const fs::path socketPath = testDataDir() / "tape-engine-query.sock";
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
    expect(server.start(&startError), "tape-engine should start for query test: " + startError);

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-engine-query";
    options.runtimeSessionId = "runtime-engine-query";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;

    options.batchSeq = 51;
    const bridge_batch::Batch first = bridge_batch::buildBatch({
        makeBridgeRecord(3001, "order_intent", "WebSocket", "INTC", "BUY",
                         91, 701, 0, "", "accepted order intent", "2026-03-14T09:38:00.100"),
        makeBridgeRecord(3002, "order_status", "BrokerOrderStatus", "INTC", "BUY",
                         91, 701, 9801, "", "Submitted: filled=0 remaining=1", "2026-03-14T09:38:00.150")
    }, options);
    expect(transport.sendFrame(bridge_batch::encodeFrame(first), &error),
           "tape-engine should accept the first query batch: " + error);

    options.batchSeq = 52;
    const bridge_batch::Batch second = bridge_batch::buildBatch({
        makeBridgeRecord(3004, "fill_execution", "BrokerExecution", "INTC", "BOT",
                         91, 701, 9801, "EXEC-91", "execution details observed", "2026-03-14T09:38:00.250")
    }, options);
    expect(transport.sendFrame(bridge_batch::encodeFrame(second), &error),
           "tape-engine should accept the second query batch: " + error);

    waitUntil([&]() {
        const auto snapshot = server.snapshot();
        return snapshot.segments.size() >= 2 && snapshot.latestFrozenRevisionId >= 2;
    }, "tape-engine should freeze both query batches before frozen reads");

    tape_engine::Client client(socketPath.string());
    tape_engine::QueryResponse response;

    tape_engine::QueryRequest statusRequest;
    statusRequest.requestId = "status-1";
    statusRequest.operation = "status";
    expect(client.query(statusRequest, &response, &error), "tape-engine status query should succeed: " + error);
    expect(response.summary.value("segment_count", 0ULL) >= 2, "status query should report at least the written segment count");
    expect(response.summary.value("latest_frozen_revision_id", 0ULL) >= 2, "status query should expose a frozen revision at or beyond the ingest batches");
    expect(response.summary.value("latest_session_seq", 0ULL) == 4, "status query should report the latest accepted session_seq");
    expect(response.result.value("schema", std::string()) == tape_engine::kStatusResultSchema,
           "status query should publish a typed transport result");

    tape_engine::QueryRequest rangeRequest;
    rangeRequest.requestId = "range-1";
    rangeRequest.operation = "read_range";
    rangeRequest.fromSessionSeq = 2;
    rangeRequest.toSessionSeq = 4;
    expect(client.query(rangeRequest, &response, &error), "tape-engine range query should succeed: " + error);
    expect(response.events.is_array() && response.events.size() == 3, "range query should return the requested session_seq window");
    expect(response.summary.value("served_revision_id", 0ULL) >= 2, "range query should say which frozen revision served the read");
    expect(response.events.at(0).value("event_kind", std::string()) == "order_status", "range query should include the original order status event");
    expect(response.events.at(1).value("event_kind", std::string()) == "gap_marker", "range query should expose the explicit gap marker");
    expect(response.events.at(2).value("event_kind", std::string()) == "fill_execution", "range query should expose the later fill event");
    expect(response.result.value("schema", std::string()) == tape_engine::kEventListResultSchema,
           "range query should publish a typed event-list transport result");

    tape_engine::QueryRequest liveTailRequest;
    liveTailRequest.requestId = "tail-1";
    liveTailRequest.operation = "read_live_tail";
    liveTailRequest.limit = 2;
    expect(client.query(liveTailRequest, &response, &error), "tape-engine live tail query should succeed: " + error);
    expect(response.events.is_array() && response.events.size() == 2, "live tail query should honor the requested limit");
    expect(response.events.at(0).value("event_kind", std::string()) == "gap_marker", "live tail query should expose the penultimate live event");
    expect(response.events.at(1).value("event_kind", std::string()) == "fill_execution", "live tail query should expose the latest live event");
    expect(response.result.value("schema", std::string()) == tape_engine::kEventListResultSchema,
           "live tail query should publish a typed event-list transport result");

    tape_engine::QueryRequest liveTailAliasRequest;
    liveTailAliasRequest.requestId = "tail-alias-1";
    liveTailAliasRequest.operation = "read-live-tail";
    liveTailAliasRequest.limit = 1;
    expect(client.query(liveTailAliasRequest, &response, &error),
           "tape-engine should accept hyphenated query-operation aliases after protocol normalization: " + error);
    expect(response.operation == "read_live_tail",
           "tape-engine should echo the canonical query operation name after alias normalization");
    expect(response.events.is_array() && response.events.size() == 1,
           "hyphenated alias query should still honor the requested limit");

    tape_engine::QueryRequest anchorRequest;
    anchorRequest.requestId = "anchor-1";
    anchorRequest.operation = "find_order_anchor";
    anchorRequest.orderId = 701;
    expect(client.query(anchorRequest, &response, &error), "tape-engine anchor query should succeed: " + error);
    expect(response.events.is_array() && response.events.size() == 3, "anchor query should return the order-anchored lifecycle events");
    expect(response.events.at(0).value("event_kind", std::string()) == "order_intent", "anchor query should begin with the queued order intent");
    expect(response.events.at(2).value("event_kind", std::string()) == "fill_execution", "anchor query should include the fill execution");
    expect(response.result.value("schema", std::string()) == tape_engine::kEventListResultSchema,
           "anchor query should publish a typed event-list transport result");

    tape_engine::QueryRequest seekRequest;
    seekRequest.requestId = "seek-1";
    seekRequest.operation = "seek_order_anchor";
    seekRequest.orderId = 701;
    expect(client.query(seekRequest, &response, &error), "tape-engine seek-order query should succeed: " + error);
    expect(response.summary.value("first_session_seq", 0ULL) == 1, "seek-order should report the first anchored session_seq");
    expect(response.summary.value("last_session_seq", 0ULL) == 4, "seek-order should report the final anchored session_seq");
    expect(response.summary.value("last_fill_session_seq", 0ULL) == 4, "seek-order should identify the fill session_seq as the replay target");
    expect(response.summary.value("replay_target_session_seq", 0ULL) == 4, "seek-order should point replay at the fill when one exists");
    expect(response.summary.contains("protected_window"), "seek-order should include protected-window context");
    expect(response.result.value("schema", std::string()) == tape_engine::kSeekOrderResultSchema,
           "seek-order should publish a typed seek transport result");

    tape_engine::QueryRequest qualityRequest;
    qualityRequest.requestId = "quality-1";
    qualityRequest.operation = "read_session_quality";
    qualityRequest.fromSessionSeq = 1;
    qualityRequest.toSessionSeq = 4;
    expect(client.query(qualityRequest, &response, &error), "tape-engine session-quality query should succeed: " + error);
    expect(response.summary.contains("data_quality"), "session-quality query should return a data-quality block");
    expect(response.summary.value("data_quality", json::object()).value("gap_marker_count", 0ULL) == 1ULL,
           "session-quality query should count explicit gap markers");
    expect(response.summary.value("data_quality", json::object()).value("weak_instrument_identity_count", 0ULL) >= 1ULL,
           "session-quality query should flag weak identity fallback when only symbol-style identity is available");
    expect(response.result.value("schema", std::string()) == tape_engine::kSessionQualityResultSchema,
           "session-quality query should publish a typed transport result");

    server.stop();
}

void testTapeEngineQueryOperationRegistryCanonicalizesAliases() {
    expect(std::string(tape_engine::queryOperationName(tape_engine::QueryOperation::ReadArtifact)) == "read_artifact",
           "query operation registry should expose canonical operation names");
    expect(tape_engine::canonicalizeQueryOperationName("scan-order-case-report") == "scan_order_case_report",
           "query operation canonicalization should normalize hyphenated aliases");
    expect(tape_engine::queryOperationFromString("read-live-tail") == tape_engine::QueryOperation::ReadLiveTail,
           "query operation parsing should recognize hyphenated aliases");

    tape_engine::QueryRequest built = tape_engine::makeQueryRequest(tape_engine::QueryOperation::SeekOrderAnchor,
                                                                    "protocol-op-test");
    expect(built.requestId == "protocol-op-test",
           "makeQueryRequest should preserve the supplied request id");
    expect(built.operation == "seek_order_anchor",
           "makeQueryRequest should stamp the canonical operation name");
    expect(built.operationKind == tape_engine::QueryOperation::SeekOrderAnchor,
           "makeQueryRequest should preserve the typed operation kind");

    tape_engine::QueryRequest aliased;
    aliased.requestId = "alias-op-test";
    aliased.operation = "read-artifact";
    const tape_engine::QueryRequest decoded =
        tape_engine::queryRequestFromJson(tape_engine::queryRequestToJson(aliased));
    expect(decoded.operation == "read_artifact",
           "query request encoding should canonicalize aliased operation names");
    expect(decoded.operationKind == tape_engine::QueryOperation::ReadArtifact,
           "query request decoding should recover the typed operation kind");
}
