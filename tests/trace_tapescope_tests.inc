void testTapeScopeClientReadsPhase4EngineSeam() {
    const fs::path rootDir = testDataDir() / "tapescope-phase4-client";
    const fs::path socketPath = testDataDir() / "tapescope-phase4-client.sock";
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
    expect(server.start(&startError), "tape-engine should start for TapeScope phase4 seam test: " + startError);

    bridge_batch::BuildOptions options;
    options.appSessionId = "app-tapescope-phase4";
    options.runtimeSessionId = "runtime-tapescope-phase4";
    options.flushReason = bridge_batch::FlushReason::ImmediateLifecycle;

    bridge_batch::UnixDomainSocketTransport transport(socketPath.string());
    std::string error;

    options.batchSeq = 201;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        makeBridgeRecord(9101, "order_intent", "WebSocket", "INTC", "BUY",
                         401, 7401, 0, "", "tapescope order intent", "2026-03-15T09:41:00.100"),
        makeBridgeRecord(9102, "order_status", "BrokerOrderStatus", "INTC", "BUY",
                         401, 7401, 8801, "", "Submitted: filled=0 remaining=1", "2026-03-15T09:41:00.120")
    }, options)), &error), "tape-engine should accept the TapeScope seam first batch: " + error);

    options.batchSeq = 202;
    expect(transport.sendFrame(bridge_batch::encodeFrame(bridge_batch::buildBatch({
        makeBridgeRecord(9104, "fill_execution", "BrokerExecution", "INTC", "BOT",
                         401, 7401, 8801, "EXEC-401", "phase4 fill", "2026-03-15T09:41:00.250")
    }, options)), &error), "tape-engine should accept the TapeScope seam second batch: " + error);

    waitUntil([&]() {
        const auto snapshot = server.snapshot();
        return snapshot.segments.size() >= 2 && snapshot.latestFrozenRevisionId >= 2;
    }, "tape-engine should freeze TapeScope seam batches");

    tapescope::ClientConfig clientConfig;
    clientConfig.socketPath = socketPath.string();
    tapescope::QueryClient client(clientConfig);

    const auto status = client.status();
    expect(status.ok(), "TapeScope status should succeed: " + tapescope::QueryClient::describeError(status.error));
    expect(status.value.instrumentId == "ib:conid:9301:STK:SMART:USD:INTC",
           "TapeScope status should expose the engine instrument id");
    expect(status.value.latestSessionSeq == 4,
           "TapeScope status should expose the latest session seq");

    const auto liveTail = client.readLiveTail(2);
    expect(liveTail.ok(), "TapeScope live tail should succeed: " + tapescope::QueryClient::describeError(liveTail.error));
    expect(liveTail.value.size() == 2, "TapeScope live tail should honor the requested limit");
    expect(liveTail.value.back().value("event_kind", std::string()) == "fill_execution",
           "TapeScope live tail should expose the latest fill");
    const auto typedLiveTail = client.readLiveTailRows(2);
    expect(typedLiveTail.ok(), "TapeScope typed live tail should succeed: " +
                                   tapescope::QueryClient::describeError(typedLiveTail.error));
    expect(typedLiveTail.value.size() == 2 &&
               typedLiveTail.value.back().eventKind == "fill_execution",
           "TapeScope typed live tail should parse stable event rows");

    tapescope::RangeQuery range;
    range.firstSessionSeq = 2;
    range.lastSessionSeq = 4;
    const auto rangeResult = client.readRange(range);
    expect(rangeResult.ok(), "TapeScope range read should succeed: " + tapescope::QueryClient::describeError(rangeResult.error));
    expect(rangeResult.value.size() == 3, "TapeScope range read should return the requested seq window");
    expect(rangeResult.value.front().value("event_kind", std::string()) == "order_status",
           "TapeScope range read should include the order status event");
    const auto typedRangeResult = client.readRangeRows(range);
    expect(typedRangeResult.ok(), "TapeScope typed range read should succeed: " +
                                      tapescope::QueryClient::describeError(typedRangeResult.error));
    expect(typedRangeResult.value.size() == 3 &&
               typedRangeResult.value.front().eventKind == "order_status",
           "TapeScope typed range read should parse stable event rows");

    tapescope::OrderAnchorQuery anchor;
    anchor.orderId = 7401;
    const auto typedAnchorResult = client.findOrderAnchorPayload(anchor);
    expect(typedAnchorResult.ok(), "TapeScope typed order-anchor lookup should succeed: " +
                                       tapescope::QueryClient::describeError(typedAnchorResult.error));
    expect(typedAnchorResult.value.events.size() == 3 &&
               typedAnchorResult.value.events.front().eventKind == "order_intent" &&
               typedAnchorResult.value.events.back().eventKind == "fill_execution",
           "TapeScope typed order-anchor lookup should parse anchored event rows");

    const auto typedOrderCase = client.readOrderCasePayload(anchor);
    expect(typedOrderCase.ok(), "TapeScope typed order-case read should succeed: " +
                                    tapescope::QueryClient::describeError(typedOrderCase.error));
    expect(typedOrderCase.value.summary.contains("report"),
           "TapeScope typed order-case read should expose the investigation report envelope");

    const auto typedSeekResult = client.seekOrderAnchorPayload(anchor);
    expect(typedSeekResult.ok(), "TapeScope typed seek-order read should succeed: " +
                                     tapescope::QueryClient::describeError(typedSeekResult.error));
    expect(typedSeekResult.value.replayTargetSessionSeq == 4,
           "TapeScope typed seek-order read should expose the replay target session seq");
    expect(typedSeekResult.value.replayRange.has_value() &&
               typedSeekResult.value.replayRange->firstSessionSeq <= typedSeekResult.value.replayTargetSessionSeq &&
               typedSeekResult.value.replayRange->lastSessionSeq >= typedSeekResult.value.replayTargetSessionSeq,
           "TapeScope typed seek-order read should parse a replay target window that contains the target session");

    const auto typedOverview = client.readSessionOverviewPayload(range);
    expect(typedOverview.ok(), "TapeScope typed session-overview read should succeed: " +
                                   tapescope::QueryClient::describeError(typedOverview.error));
    expect(typedOverview.value.summary.contains("report"),
           "TapeScope typed session-overview read should expose the overview report envelope");
    expect(!typedOverview.value.evidence.empty(),
           "TapeScope typed session-overview read should parse evidence citations for the UI");
    expect(!typedOverview.value.incidents.empty(),
           "TapeScope typed session-overview read should parse incident rows for the UI");
    expect(typedOverview.value.replayRange.has_value() &&
               typedOverview.value.replayRange->firstSessionSeq == 2 &&
               typedOverview.value.replayRange->lastSessionSeq == 4,
           "TapeScope typed session-overview read should parse the replay window");

    const auto typedQuality = client.readSessionQualityPayload(range);
    expect(typedQuality.ok(), "TapeScope typed session-quality read should succeed: " +
                                  tapescope::QueryClient::describeError(typedQuality.error));
    expect(typedQuality.value.dataQuality.is_object(),
           "TapeScope typed session-quality read should parse the data-quality object");

    const auto typedSessionReport = client.scanSessionReportPayload(range);
    expect(typedSessionReport.ok(), "TapeScope typed session-report scan should succeed: " +
                                        tapescope::QueryClient::describeError(typedSessionReport.error));
    const std::string sessionReportArtifactId = typedSessionReport.value.artifactId;
    expect(!sessionReportArtifactId.empty(), "TapeScope session-report scan should expose a durable artifact id");

    const auto typedSessionReportList = client.listSessionReportsPayload(10);
    expect(typedSessionReportList.ok(), "TapeScope typed session-report list should succeed: " +
                                            tapescope::QueryClient::describeError(typedSessionReportList.error));
    expect(!typedSessionReportList.value.sessionReports.empty(),
           "TapeScope typed session-report list should parse report rows for the UI");

    const auto typedArtifact = client.readArtifactPayload(sessionReportArtifactId);
    expect(typedArtifact.ok(), "TapeScope typed artifact read should succeed: " +
                                   tapescope::QueryClient::describeError(typedArtifact.error));
    expect(typedArtifact.value.artifactId == sessionReportArtifactId,
           "TapeScope typed artifact read should preserve the durable artifact id");

    const auto typedExport = client.exportArtifactPayload(sessionReportArtifactId, "markdown");
    expect(typedExport.ok(), "TapeScope typed artifact export should succeed: " +
                                 tapescope::QueryClient::describeError(typedExport.error));
    expect(typedExport.value.format == "markdown" && !typedExport.value.markdown.empty(),
           "TapeScope typed artifact export should preserve the requested export format and payload");

    const auto typedOrderCaseReport = client.scanOrderCaseReportPayload(anchor);
    expect(typedOrderCaseReport.ok(), "TapeScope typed order-case report scan should succeed: " +
                                          tapescope::QueryClient::describeError(typedOrderCaseReport.error));
    const std::string orderCaseReportArtifactId = typedOrderCaseReport.value.artifactId;
    expect(!orderCaseReportArtifactId.empty(), "TapeScope order-case report scan should expose a durable case-report artifact id");

    const auto typedCaseReportList = client.listCaseReportsPayload(10);
    expect(typedCaseReportList.ok(), "TapeScope typed case-report list should succeed: " +
                                         tapescope::QueryClient::describeError(typedCaseReportList.error));
    expect(!typedCaseReportList.value.caseReports.empty(),
           "TapeScope typed case-report list should parse report rows for the UI");

    const std::uint64_t sessionReportId = typedSessionReportList.value.sessionReports.front().reportId;
    expect(sessionReportId > 0, "TapeScope session-report inventory should expose a durable report id");
    const auto typedSessionBundleExport = client.exportSessionBundlePayload(sessionReportId);
    expect(typedSessionBundleExport.ok(), "TapeScope typed session-bundle export should succeed: " +
                                              tapescope::QueryClient::describeError(typedSessionBundleExport.error));
    expect(!typedSessionBundleExport.value.bundlePath.empty() &&
               fs::exists(typedSessionBundleExport.value.bundlePath),
           "TapeScope typed session-bundle export should surface a written bundle path");

    const std::uint64_t caseReportId = typedCaseReportList.value.caseReports.front().reportId;
    expect(caseReportId > 0, "TapeScope case-report inventory should expose a durable report id");
    const auto typedCaseBundleExport = client.exportCaseBundlePayload(caseReportId);
    expect(typedCaseBundleExport.ok(), "TapeScope typed case-bundle export should succeed: " +
                                           tapescope::QueryClient::describeError(typedCaseBundleExport.error));
    expect(!typedCaseBundleExport.value.bundlePath.empty() &&
               fs::exists(typedCaseBundleExport.value.bundlePath),
           "TapeScope typed case-bundle export should surface a written bundle path");

    const auto typedCaseBundleImport = client.importCaseBundlePayload(typedCaseBundleExport.value.bundlePath);
    expect(typedCaseBundleImport.ok(), "TapeScope typed case-bundle import should succeed: " +
                                           tapescope::QueryClient::describeError(typedCaseBundleImport.error));
    expect(!typedCaseBundleImport.value.artifactId.empty() &&
               typedCaseBundleImport.value.artifactId.rfind("imported-case:", 0) == 0,
           "TapeScope typed case-bundle import should surface an imported-case artifact id");

    const auto typedImportedCaseList = client.listImportedCasesPayload(10);
    expect(typedImportedCaseList.ok(), "TapeScope typed imported-case list should succeed: " +
                                           tapescope::QueryClient::describeError(typedImportedCaseList.error));
    expect(!typedImportedCaseList.value.importedCases.empty(),
           "TapeScope typed imported-case list should parse imported-case rows for the UI");

    const auto typedImportedArtifact = client.readArtifactPayload(typedCaseBundleImport.value.artifactId);
    expect(typedImportedArtifact.ok(), "TapeScope typed imported-case artifact read should succeed: " +
                                           tapescope::QueryClient::describeError(typedImportedArtifact.error));
    expect(typedImportedArtifact.value.artifactId == typedCaseBundleImport.value.artifactId,
           "TapeScope typed imported-case artifact read should preserve the imported artifact id");
    expect(typedImportedArtifact.value.artifactKind == "imported_case_bundle",
           "TapeScope typed imported-case artifact read should surface the imported bundle artifact kind");

    const auto typedIncidents = client.listIncidentsPayload(10);
    expect(typedIncidents.ok(), "TapeScope typed incident list should succeed: " +
                                    tapescope::QueryClient::describeError(typedIncidents.error));
    expect(!typedIncidents.value.incidents.empty(),
           "TapeScope typed incident list should parse incident rows for the UI");
    const std::uint64_t logicalIncidentId = typedIncidents.value.incidents.front().logicalIncidentId;
    expect(logicalIncidentId > 0, "TapeScope seam test should expose a drilldown-capable logical incident id");

    const auto typedIncident = client.readIncidentPayload(logicalIncidentId);
    expect(typedIncident.ok(), "TapeScope typed incident read should succeed: " +
                                   tapescope::QueryClient::describeError(typedIncident.error));
    expect(typedIncident.value.summary.value("logical_incident_id", 0ULL) == logicalIncidentId,
           "TapeScope typed incident read should preserve the requested logical incident id");
    expect(!typedIncident.value.headline.empty(),
           "TapeScope typed incident read should expose a parsed incident headline");

    std::uint64_t findingId = 0;
    for (const auto& citation : typedIncident.value.evidence) {
        const std::string artifactId = citation.artifactId;
        if (artifactId.rfind("finding:", 0) == 0) {
            findingId = std::stoull(artifactId.substr(std::string("finding:").size()));
            break;
        }
    }
    expect(findingId > 0, "TapeScope incident read should expose at least one linked finding artifact id");

    const auto typedFinding = client.readFindingPayload(findingId);
    expect(typedFinding.ok(), "TapeScope typed finding read should succeed: " +
                                  tapescope::QueryClient::describeError(typedFinding.error));
    expect(typedFinding.value.summary.value("finding", json::object()).value("finding_id", 0ULL) == findingId,
           "TapeScope typed finding read should preserve the requested finding id");
    expect(!typedFinding.value.evidence.empty(),
           "TapeScope typed finding read should parse evidence citations");

    const auto typedOrderAnchor = client.readOrderAnchorPayload(1);
    expect(typedOrderAnchor.ok(), "TapeScope typed order-anchor read should succeed: " +
                                      tapescope::QueryClient::describeError(typedOrderAnchor.error));
    expect(typedOrderAnchor.value.summary.value("order_anchor", json::object()).value("anchor_id", 0ULL) == 1,
           "TapeScope typed order-anchor read should preserve the requested anchor id");
    expect(!typedOrderAnchor.value.artifactId.empty(),
           "TapeScope typed order-anchor read should expose the linked artifact id");

    server.stop();
}
