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
    const auto typedSessionBundleVerify = client.verifyBundlePayload(typedSessionBundleExport.value.bundlePath);
    expect(typedSessionBundleVerify.ok(), "TapeScope typed session-bundle verify should succeed: " +
                                              tapescope::QueryClient::describeError(typedSessionBundleVerify.error));
    expect(typedSessionBundleVerify.value.verifyStatus == "valid" &&
               !typedSessionBundleVerify.value.importSupported &&
               !typedSessionBundleVerify.value.canImport,
           "TapeScope typed session-bundle verify should mark the bundle valid but non-importable");

    const std::uint64_t caseReportId = typedCaseReportList.value.caseReports.front().reportId;
    expect(caseReportId > 0, "TapeScope case-report inventory should expose a durable report id");
    const auto typedCaseBundleExport = client.exportCaseBundlePayload(caseReportId);
    expect(typedCaseBundleExport.ok(), "TapeScope typed case-bundle export should succeed: " +
                                           tapescope::QueryClient::describeError(typedCaseBundleExport.error));
    expect(!typedCaseBundleExport.value.bundlePath.empty() &&
               fs::exists(typedCaseBundleExport.value.bundlePath),
           "TapeScope typed case-bundle export should surface a written bundle path");
    const fs::path movedCaseBundlePath = rootDir / "renamed-case-bundle.msgpack";
    fs::rename(typedCaseBundleExport.value.bundlePath, movedCaseBundlePath, ec);
    expect(!ec && fs::exists(movedCaseBundlePath),
           "TapeScope phase6 seam should be able to rename the exported case bundle");
    tape_bundle::PortableBundleInspection movedBundleInspection;
    std::string movedInspectError;
    expect(tape_bundle::inspectPortableBundle(movedCaseBundlePath, &movedBundleInspection, &movedInspectError),
           "TapeScope phase6 seam should inspect the renamed bundle: " + movedInspectError);
    const auto typedCaseBundleVerify = client.verifyBundlePayload(movedCaseBundlePath.string());
    expect(typedCaseBundleVerify.ok(), "TapeScope typed case-bundle verify should succeed: " +
                                           tapescope::QueryClient::describeError(typedCaseBundleVerify.error));
    expect(typedCaseBundleVerify.value.verifyStatus == "valid" &&
               typedCaseBundleVerify.value.importSupported &&
               typedCaseBundleVerify.value.canImport &&
               !typedCaseBundleVerify.value.alreadyImported &&
               !typedCaseBundleVerify.value.payloadSha256.empty(),
           "TapeScope typed case-bundle verify should mark the fresh case bundle importable and expose its hash");
    expect(typedCaseBundleVerify.value.payloadSha256 == movedBundleInspection.payloadSha256,
           "TapeScope typed case-bundle verify should preserve payload-hash recognition after rename");

    const auto typedCaseBundleImport = client.importCaseBundlePayload(movedCaseBundlePath.string());
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
    bool matchedRenamedBundleByPayloadHash = false;
    for (const auto& row : typedImportedCaseList.value.importedCases) {
        if (row.payloadSha256 == movedBundleInspection.payloadSha256) {
            matchedRenamedBundleByPayloadHash = true;
            expect(row.sourceBundlePath == movedCaseBundlePath.string(),
                   "TapeScope typed imported-case list should preserve the renamed source bundle path");
            expect(row.artifactId == typedCaseBundleImport.value.artifactId,
                   "TapeScope typed imported-case list should map the renamed bundle hash to the imported artifact");
            break;
        }
    }
    expect(matchedRenamedBundleByPayloadHash,
           "TapeScope typed imported-case list should recognize a renamed bundle by payload hash");

    const auto typedImportedArtifact = client.readArtifactPayload(typedCaseBundleImport.value.artifactId);
    expect(typedImportedArtifact.ok(), "TapeScope typed imported-case artifact read should succeed: " +
                                           tapescope::QueryClient::describeError(typedImportedArtifact.error));
    expect(typedImportedArtifact.value.artifactId == typedCaseBundleImport.value.artifactId,
           "TapeScope typed imported-case artifact read should preserve the imported artifact id");
    expect(typedImportedArtifact.value.artifactKind == "imported_case_bundle",
           "TapeScope typed imported-case artifact read should surface the imported bundle artifact kind");
    const auto typedImportedCaseBundleVerify = client.verifyBundlePayload(movedCaseBundlePath.string());
    expect(typedImportedCaseBundleVerify.ok(), "TapeScope typed imported case-bundle verify should succeed: " +
                                                   tapescope::QueryClient::describeError(typedImportedCaseBundleVerify.error));
    expect(typedImportedCaseBundleVerify.value.alreadyImported &&
               !typedImportedCaseBundleVerify.value.canImport &&
               typedImportedCaseBundleVerify.value.hasImportedCase &&
               typedImportedCaseBundleVerify.value.importedCase.artifactId == typedCaseBundleImport.value.artifactId,
           "TapeScope typed imported case-bundle verify should surface the matching imported artifact");

    const auto typedPhase7Profiles = client.listAnalysisProfilesPayload();
    expect(typedPhase7Profiles.ok(), "TapeScope typed phase7 profile list should succeed: " +
                                         tapescope::QueryClient::describeError(typedPhase7Profiles.error));
    expect(!typedPhase7Profiles.value.empty() &&
               typedPhase7Profiles.value.front().analysisProfile == tape_phase7::kDefaultAnalyzerProfile,
           "TapeScope typed phase7 profile list should expose the default analysis profile");
    const auto incidentTriageProfileIt = std::find_if(
        typedPhase7Profiles.value.begin(),
        typedPhase7Profiles.value.end(),
        [](const tapescope::Phase7AnalyzerProfile& profile) {
            return profile.analysisProfile == tape_phase7::kIncidentTriageAnalyzerProfile;
        });
    expect(incidentTriageProfileIt != typedPhase7Profiles.value.end(),
           "TapeScope typed phase7 profile list should expose the incident-triage analysis profile");
    const auto fillQualityProfileIt = std::find_if(
        typedPhase7Profiles.value.begin(),
        typedPhase7Profiles.value.end(),
        [](const tapescope::Phase7AnalyzerProfile& profile) {
            return profile.analysisProfile == tape_phase7::kFillQualityAnalyzerProfile;
        });
    expect(fillQualityProfileIt != typedPhase7Profiles.value.end(),
           "TapeScope typed phase7 profile list should expose the fill-quality analysis profile");
    const auto liquidityBehaviorProfileIt = std::find_if(
        typedPhase7Profiles.value.begin(),
        typedPhase7Profiles.value.end(),
        [](const tapescope::Phase7AnalyzerProfile& profile) {
            return profile.analysisProfile == tape_phase7::kLiquidityBehaviorAnalyzerProfile;
        });
    expect(liquidityBehaviorProfileIt != typedPhase7Profiles.value.end(),
           "TapeScope typed phase7 profile list should expose the liquidity-behavior analysis profile");
    const auto adverseSelectionProfileIt = std::find_if(
        typedPhase7Profiles.value.begin(),
        typedPhase7Profiles.value.end(),
        [](const tapescope::Phase7AnalyzerProfile& profile) {
            return profile.analysisProfile == tape_phase7::kAdverseSelectionAnalyzerProfile;
        });
    expect(adverseSelectionProfileIt != typedPhase7Profiles.value.end(),
           "TapeScope typed phase7 profile list should expose the adverse-selection analysis profile");
    const auto orderImpactProfileIt = std::find_if(
        typedPhase7Profiles.value.begin(),
        typedPhase7Profiles.value.end(),
        [](const tapescope::Phase7AnalyzerProfile& profile) {
            return profile.analysisProfile == tape_phase7::kOrderImpactAnalyzerProfile;
        });
    expect(orderImpactProfileIt != typedPhase7Profiles.value.end(),
           "TapeScope typed phase7 profile list should expose the order-impact analysis profile");

    const auto typedPhase7Profile = client.readAnalysisProfilePayload(tape_phase7::kDefaultAnalyzerProfile);
    expect(typedPhase7Profile.ok(), "TapeScope typed phase7 profile read should succeed: " +
                                        tapescope::QueryClient::describeError(typedPhase7Profile.error));
    expect(typedPhase7Profile.value.defaultProfile,
           "TapeScope typed phase7 profile read should preserve the default profile flag");
    const auto typedIncidentTriageProfile =
        client.readAnalysisProfilePayload(tape_phase7::kIncidentTriageAnalyzerProfile);
    expect(typedIncidentTriageProfile.ok(), "TapeScope typed incident-triage phase7 profile read should succeed: " +
                                                tapescope::QueryClient::describeError(typedIncidentTriageProfile.error));
    expect(!typedIncidentTriageProfile.value.defaultProfile,
           "TapeScope typed incident-triage phase7 profile read should remain non-default");
    const auto typedFillQualityProfile =
        client.readAnalysisProfilePayload(tape_phase7::kFillQualityAnalyzerProfile);
    expect(typedFillQualityProfile.ok(), "TapeScope typed fill-quality phase7 profile read should succeed: " +
                                             tapescope::QueryClient::describeError(typedFillQualityProfile.error));
    const auto typedLiquidityBehaviorProfile =
        client.readAnalysisProfilePayload(tape_phase7::kLiquidityBehaviorAnalyzerProfile);
    expect(typedLiquidityBehaviorProfile.ok(), "TapeScope typed liquidity-behavior phase7 profile read should succeed: " +
                                                   tapescope::QueryClient::describeError(typedLiquidityBehaviorProfile.error));
    const auto typedAdverseSelectionProfile =
        client.readAnalysisProfilePayload(tape_phase7::kAdverseSelectionAnalyzerProfile);
    expect(typedAdverseSelectionProfile.ok(), "TapeScope typed adverse-selection phase7 profile read should succeed: " +
                                                  tapescope::QueryClient::describeError(typedAdverseSelectionProfile.error));
    const auto typedOrderImpactProfile =
        client.readAnalysisProfilePayload(tape_phase7::kOrderImpactAnalyzerProfile);
    expect(typedOrderImpactProfile.ok(), "TapeScope typed order-impact phase7 profile read should succeed: " +
                                             tapescope::QueryClient::describeError(typedOrderImpactProfile.error));

    const auto phase7AnalysisRun = client.runAnalysisPayload(movedCaseBundlePath.string(),
                                                             tape_phase7::kDefaultAnalyzerProfile);
    expect(phase7AnalysisRun.ok(), "TapeScope typed phase7 analysis run should succeed: " +
                                       tapescope::QueryClient::describeError(phase7AnalysisRun.error));
    expect(phase7AnalysisRun.value.created,
           "TapeScope typed phase7 analysis run should create the artifact on first run");
    const auto reusedPhase7AnalysisRun = client.runAnalysisPayload(movedCaseBundlePath.string(),
                                                                   tape_phase7::kDefaultAnalyzerProfile);
    expect(reusedPhase7AnalysisRun.ok(), "TapeScope typed phase7 analysis rerun should succeed: " +
                                             tapescope::QueryClient::describeError(reusedPhase7AnalysisRun.error));
    expect(!reusedPhase7AnalysisRun.value.created,
           "TapeScope typed phase7 analysis rerun should reuse the deterministic artifact");
    const auto& phase7AnalysisArtifact = phase7AnalysisRun.value.artifact;
    const auto phase7SessionAnalysisRun = client.runAnalysisPayload(typedSessionBundleExport.value.bundlePath,
                                                                    tape_phase7::kDefaultAnalyzerProfile);
    expect(phase7SessionAnalysisRun.ok(), "TapeScope typed phase7 session analysis run should succeed: " +
                                              tapescope::QueryClient::describeError(phase7SessionAnalysisRun.error));
    const auto& phase7SessionAnalysisArtifact = phase7SessionAnalysisRun.value.artifact;
    expect(!phase7AnalysisArtifact.findings.empty(),
           "TapeScope typed phase7 analysis should emit at least one finding for dry-run planning");
    const std::vector<std::string> selectedFindingIds = {phase7AnalysisArtifact.findings.front().findingId};

    const auto typedPhase7AnalysisList = client.listAnalysisArtifactsPayload(10);
    expect(typedPhase7AnalysisList.ok(), "TapeScope typed phase7 analysis inventory should succeed: " +
                                             tapescope::QueryClient::describeError(typedPhase7AnalysisList.error));
    expect(!typedPhase7AnalysisList.value.empty(),
           "TapeScope typed phase7 analysis inventory should parse durable analysis artifacts");

    const auto typedPhase7Analysis = client.readAnalysisArtifactPayload(
        phase7AnalysisArtifact.analysisArtifact.artifactId);
    expect(typedPhase7Analysis.ok(), "TapeScope typed phase7 analysis read should succeed: " +
                                         tapescope::QueryClient::describeError(typedPhase7Analysis.error));
    expect(typedPhase7Analysis.value.analysisArtifact.artifactId ==
               phase7AnalysisArtifact.analysisArtifact.artifactId,
           "TapeScope typed phase7 analysis read should preserve the requested artifact id");
    expect(typedPhase7Analysis.value.sourceArtifact.artifactId ==
               phase7AnalysisArtifact.sourceArtifact.artifactId,
           "TapeScope typed phase7 analysis read should preserve the linked source artifact id");

    tapescope::Phase7AnalysisInventorySelection filteredAnalysisSelection;
    filteredAnalysisSelection.sourceArtifactId = phase7AnalysisArtifact.sourceArtifact.artifactId;
    filteredAnalysisSelection.analysisProfile = tape_phase7::kDefaultAnalyzerProfile;
    filteredAnalysisSelection.limit = 10;
    const auto filteredPhase7Analyses = client.listAnalysisArtifactsPayload(filteredAnalysisSelection);
    expect(filteredPhase7Analyses.ok(), "TapeScope filtered phase7 analysis inventory should succeed: " +
                                            tapescope::QueryClient::describeError(filteredPhase7Analyses.error));
    expect(filteredPhase7Analyses.value.matchedCount >= 1 && !filteredPhase7Analyses.value.artifacts.empty(),
           "TapeScope filtered phase7 analysis inventory should return the matching durable analysis");
    expect(filteredPhase7Analyses.value.appliedFilters.value("source_artifact_id", std::string()) ==
                   phase7AnalysisArtifact.sourceArtifact.artifactId &&
               filteredPhase7Analyses.value.appliedFilters.value("analysis_profile", std::string()) ==
                   tape_phase7::kDefaultAnalyzerProfile,
           "TapeScope filtered phase7 analysis inventory should preserve the requested lineage filters");

    tapescope::Phase7AnalysisInventorySelection sortedAnalysisSelection;
    sortedAnalysisSelection.sortBy = "source_artifact_asc";
    sortedAnalysisSelection.limit = 10;
    const auto sortedPhase7Analyses = client.listAnalysisArtifactsPayload(sortedAnalysisSelection);
    expect(sortedPhase7Analyses.ok(), "TapeScope sorted phase7 analysis inventory should succeed: " +
                                          tapescope::QueryClient::describeError(sortedPhase7Analyses.error));
    expect(sortedPhase7Analyses.value.artifacts.size() >= 2,
           "TapeScope sorted phase7 analysis inventory should expose both session and case analyses");
    expect(sortedPhase7Analyses.value.appliedFilters.value("sort_by", std::string()) == "source_artifact_asc",
           "TapeScope sorted phase7 analysis inventory should preserve the requested sort mode");
    expect(sortedPhase7Analyses.value.artifacts.front().sourceArtifact.artifactId <=
               sortedPhase7Analyses.value.artifacts.back().sourceArtifact.artifactId,
           "TapeScope sorted phase7 analysis inventory should sort by source artifact id");

    const auto incidentTriageAnalysisRun = client.runAnalysisPayload(movedCaseBundlePath.string(),
                                                                     tape_phase7::kIncidentTriageAnalyzerProfile);
    expect(incidentTriageAnalysisRun.ok(), "TapeScope typed incident-triage phase7 analysis run should succeed: " +
                                               tapescope::QueryClient::describeError(incidentTriageAnalysisRun.error));
    expect(incidentTriageAnalysisRun.value.created,
           "TapeScope typed incident-triage phase7 analysis run should create the artifact on first run");
    expect(incidentTriageAnalysisRun.value.artifact.analysisProfile == tape_phase7::kIncidentTriageAnalyzerProfile,
           "TapeScope typed incident-triage phase7 analysis should preserve the requested profile");
    expect(std::any_of(incidentTriageAnalysisRun.value.artifact.findings.begin(),
                       incidentTriageAnalysisRun.value.artifact.findings.end(),
                       [](const tape_phase7::FindingRecord& finding) {
                           return finding.category == "incident_priority" ||
                               finding.category == "incident_uncertainty" ||
                               finding.category == "timeline_hotspot";
                       }),
           "TapeScope typed incident-triage phase7 analysis should emit incident triage finding categories");
    const auto reusedIncidentTriageAnalysisRun = client.runAnalysisPayload(
        movedCaseBundlePath.string(),
        tape_phase7::kIncidentTriageAnalyzerProfile);
    expect(reusedIncidentTriageAnalysisRun.ok(),
           "TapeScope typed incident-triage phase7 analysis rerun should succeed: " +
               tapescope::QueryClient::describeError(reusedIncidentTriageAnalysisRun.error));
    expect(!reusedIncidentTriageAnalysisRun.value.created,
           "TapeScope typed incident-triage phase7 analysis rerun should reuse the deterministic artifact");

    tapescope::Phase7AnalysisInventorySelection incidentTriageSelection;
    incidentTriageSelection.sourceArtifactId = incidentTriageAnalysisRun.value.artifact.sourceArtifact.artifactId;
    incidentTriageSelection.analysisProfile = tape_phase7::kIncidentTriageAnalyzerProfile;
    incidentTriageSelection.limit = 10;
    const auto incidentTriageAnalyses = client.listAnalysisArtifactsPayload(incidentTriageSelection);
    expect(incidentTriageAnalyses.ok(), "TapeScope filtered incident-triage analysis inventory should succeed: " +
                                            tapescope::QueryClient::describeError(incidentTriageAnalyses.error));
    expect(incidentTriageAnalyses.value.matchedCount >= 1 && !incidentTriageAnalyses.value.artifacts.empty(),
           "TapeScope filtered incident-triage analysis inventory should return the matching durable analysis");
    expect(incidentTriageAnalyses.value.appliedFilters.value("analysis_profile", std::string()) ==
               tape_phase7::kIncidentTriageAnalyzerProfile,
           "TapeScope filtered incident-triage analysis inventory should preserve the requested profile filter");

    const auto fillQualityAnalysisRun = client.runAnalysisPayload(movedCaseBundlePath.string(),
                                                                  tape_phase7::kFillQualityAnalyzerProfile);
    expect(fillQualityAnalysisRun.ok(), "TapeScope typed fill-quality phase7 analysis run should succeed: " +
                                            tapescope::QueryClient::describeError(fillQualityAnalysisRun.error));
    expect(std::any_of(fillQualityAnalysisRun.value.artifact.findings.begin(),
                       fillQualityAnalysisRun.value.artifact.findings.end(),
                       [](const tape_phase7::FindingRecord& finding) {
                           return finding.category == "fill_quality_risk" ||
                               finding.category == "adverse_selection_risk" ||
                               finding.category == "market_impact_risk" ||
                               finding.category == "analysis_summary";
                       }),
           "TapeScope typed fill-quality phase7 analysis should emit fill-quality categories");

    const auto liquidityBehaviorAnalysisRun = client.runAnalysisPayload(movedCaseBundlePath.string(),
                                                                        tape_phase7::kLiquidityBehaviorAnalyzerProfile);
    expect(liquidityBehaviorAnalysisRun.ok(),
           "TapeScope typed liquidity-behavior phase7 analysis run should succeed: " +
               tapescope::QueryClient::describeError(liquidityBehaviorAnalysisRun.error));
    expect(std::any_of(liquidityBehaviorAnalysisRun.value.artifact.findings.begin(),
                       liquidityBehaviorAnalysisRun.value.artifact.findings.end(),
                       [](const tape_phase7::FindingRecord& finding) {
                           return finding.category == "display_instability_risk" ||
                               finding.category == "liquidity_refill_behavior" ||
                               finding.category == "pressure_and_thinning" ||
                               finding.category == "analysis_summary";
                       }),
           "TapeScope typed liquidity-behavior phase7 analysis should emit liquidity categories");

    const auto adverseSelectionAnalysisRun = client.runAnalysisPayload(movedCaseBundlePath.string(),
                                                                       tape_phase7::kAdverseSelectionAnalyzerProfile);
    expect(adverseSelectionAnalysisRun.ok(),
           "TapeScope typed adverse-selection phase7 analysis run should succeed: " +
               tapescope::QueryClient::describeError(adverseSelectionAnalysisRun.error));
    expect(std::any_of(adverseSelectionAnalysisRun.value.artifact.findings.begin(),
                       adverseSelectionAnalysisRun.value.artifact.findings.end(),
                       [](const tape_phase7::FindingRecord& finding) {
                           return finding.category == "adverse_selection_risk" ||
                               finding.category == "fill_quality_risk" ||
                               finding.category == "analysis_summary";
                       }),
           "TapeScope typed adverse-selection phase7 analysis should emit adverse-selection categories");

    const auto orderImpactAnalysisRun = client.runAnalysisPayload(movedCaseBundlePath.string(),
                                                                  tape_phase7::kOrderImpactAnalyzerProfile);
    expect(orderImpactAnalysisRun.ok(),
           "TapeScope typed order-impact phase7 analysis run should succeed: " +
               tapescope::QueryClient::describeError(orderImpactAnalysisRun.error));
    expect(std::any_of(orderImpactAnalysisRun.value.artifact.findings.begin(),
                       orderImpactAnalysisRun.value.artifact.findings.end(),
                       [](const tape_phase7::FindingRecord& finding) {
                           return finding.category == "market_impact_risk" ||
                               finding.category == "fill_quality_risk" ||
                               finding.category == "analysis_summary";
                       }),
           "TapeScope typed order-impact phase7 analysis should emit order-impact categories");

    const auto phase7PlaybookBuild =
        client.buildPlaybookPayload(phase7AnalysisArtifact.analysisArtifact.artifactId, selectedFindingIds);
    expect(phase7PlaybookBuild.ok(), "TapeScope typed phase7 playbook build should succeed: " +
                                         tapescope::QueryClient::describeError(phase7PlaybookBuild.error));
    expect(phase7PlaybookBuild.value.created,
           "TapeScope typed phase7 playbook build should create the dry-run playbook on first run");
    expect(phase7PlaybookBuild.value.artifact.filteredFindingIds == selectedFindingIds,
           "TapeScope typed phase7 playbook build should preserve the selected finding scope");
    const auto reusedPhase7PlaybookBuild =
        client.buildPlaybookPayload(phase7AnalysisArtifact.analysisArtifact.artifactId, selectedFindingIds);
    expect(reusedPhase7PlaybookBuild.ok(), "TapeScope typed phase7 playbook rerun should succeed: " +
                                               tapescope::QueryClient::describeError(reusedPhase7PlaybookBuild.error));
    expect(!reusedPhase7PlaybookBuild.value.created,
           "TapeScope typed phase7 playbook rerun should reuse the deterministic dry-run playbook");
    const auto& phase7PlaybookArtifact = phase7PlaybookBuild.value.artifact;
    const auto phase7FullPlaybookBuild =
        client.buildPlaybookPayload(phase7AnalysisArtifact.analysisArtifact.artifactId, {});
    expect(phase7FullPlaybookBuild.ok(), "TapeScope typed full-scope phase7 playbook build should succeed: " +
                                             tapescope::QueryClient::describeError(phase7FullPlaybookBuild.error));
    const auto& phase7FullPlaybookArtifact = phase7FullPlaybookBuild.value.artifact;

    const auto typedPhase7PlaybookList = client.listPlaybookArtifactsPayload(10);
    expect(typedPhase7PlaybookList.ok(), "TapeScope typed phase7 playbook inventory should succeed: " +
                                             tapescope::QueryClient::describeError(typedPhase7PlaybookList.error));
    expect(!typedPhase7PlaybookList.value.empty(),
           "TapeScope typed phase7 playbook inventory should parse durable dry-run playbooks");

    const auto typedPhase7Playbook = client.readPlaybookArtifactPayload(
        phase7PlaybookArtifact.playbookArtifact.artifactId);
    expect(typedPhase7Playbook.ok(), "TapeScope typed phase7 playbook read should succeed: " +
                                         tapescope::QueryClient::describeError(typedPhase7Playbook.error));
    expect(typedPhase7Playbook.value.playbookArtifact.artifactId ==
               phase7PlaybookArtifact.playbookArtifact.artifactId,
           "TapeScope typed phase7 playbook read should preserve the requested artifact id");
    expect(typedPhase7Playbook.value.mode == tape_phase7::kDefaultPlaybookMode,
           "TapeScope typed phase7 playbook read should keep apply deferred as dry_run");
    expect(typedPhase7Playbook.value.analysisArtifact.artifactId ==
               phase7AnalysisArtifact.analysisArtifact.artifactId,
           "TapeScope typed phase7 playbook read should preserve the linked analysis artifact id");
    expect(typedPhase7Playbook.value.filteredFindingIds == selectedFindingIds,
           "TapeScope typed phase7 playbook read should preserve the selected finding scope");

    const auto phase7ExecutionLedgerBuild =
        client.buildExecutionLedgerPayload(phase7PlaybookArtifact.playbookArtifact.artifactId);
    expect(phase7ExecutionLedgerBuild.ok(), "TapeScope typed phase7 execution ledger build should succeed: " +
                                                tapescope::QueryClient::describeError(phase7ExecutionLedgerBuild.error));
    expect(phase7ExecutionLedgerBuild.value.created,
           "TapeScope typed phase7 execution ledger build should create the ledger on first run");
    expect(phase7ExecutionLedgerBuild.value.artifact.ledgerStatus == tape_phase7::kDefaultLedgerStatus,
           "TapeScope typed phase7 execution ledger should keep apply deferred behind the default review status");
    expect(phase7ExecutionLedgerBuild.value.artifact.entries.size() ==
               phase7PlaybookArtifact.plannedActions.size(),
           "TapeScope typed phase7 execution ledger should preserve one review entry per planned action");
    const auto reusedPhase7ExecutionLedgerBuild =
        client.buildExecutionLedgerPayload(phase7PlaybookArtifact.playbookArtifact.artifactId);
    expect(reusedPhase7ExecutionLedgerBuild.ok(), "TapeScope typed phase7 execution ledger rerun should succeed: " +
                                                      tapescope::QueryClient::describeError(reusedPhase7ExecutionLedgerBuild.error));
    expect(!reusedPhase7ExecutionLedgerBuild.value.created,
           "TapeScope typed phase7 execution ledger rerun should reuse the deterministic ledger");

    const auto typedPhase7ExecutionLedger = client.readExecutionLedgerArtifactPayload(
        phase7ExecutionLedgerBuild.value.artifact.ledgerArtifact.artifactId);
    expect(typedPhase7ExecutionLedger.ok(), "TapeScope typed phase7 execution ledger read should succeed: " +
                                                tapescope::QueryClient::describeError(typedPhase7ExecutionLedger.error));
    expect(typedPhase7ExecutionLedger.value.playbookArtifact.artifactId ==
               phase7PlaybookArtifact.playbookArtifact.artifactId,
           "TapeScope typed phase7 execution ledger read should preserve the linked playbook artifact id");
    expect(typedPhase7ExecutionLedger.value.sourceArtifact.artifactId ==
               phase7AnalysisArtifact.sourceArtifact.artifactId,
           "TapeScope typed phase7 execution ledger read should preserve the linked source artifact id");

    const std::vector<std::string> reviewedEntryIds = {
        phase7ExecutionLedgerBuild.value.artifact.entries.front().entryId
    };
    const auto missingActorPhase7ExecutionLedgerReview = client.recordExecutionLedgerReviewPayload(
        phase7ExecutionLedgerBuild.value.artifact.ledgerArtifact.artifactId,
        reviewedEntryIds,
        tape_phase7::kLedgerEntryStatusApproved,
        "",
        "Missing actor should fail.");
    expect(!missingActorPhase7ExecutionLedgerReview.ok(),
           "TapeScope typed phase7 execution ledger review should require a reviewer identity");
    expect(tapescope::QueryClient::describeError(missingActorPhase7ExecutionLedgerReview.error).find("actor is required") !=
               std::string::npos,
           "TapeScope typed phase7 execution ledger review should surface the actor-required policy");
    const auto missingCommentPhase7ExecutionLedgerReview = client.recordExecutionLedgerReviewPayload(
        phase7ExecutionLedgerBuild.value.artifact.ledgerArtifact.artifactId,
        reviewedEntryIds,
        tape_phase7::kLedgerEntryStatusBlocked,
        "tapescope-test",
        "");
    expect(!missingCommentPhase7ExecutionLedgerReview.ok(),
           "TapeScope typed phase7 execution ledger review should require a comment for blocked status");
    expect(tapescope::QueryClient::describeError(missingCommentPhase7ExecutionLedgerReview.error).find("comment is required") !=
               std::string::npos,
           "TapeScope typed phase7 execution ledger review should surface the comment-required policy");
    const auto reviewedPhase7ExecutionLedger = client.recordExecutionLedgerReviewPayload(
        phase7ExecutionLedgerBuild.value.artifact.ledgerArtifact.artifactId,
        reviewedEntryIds,
        tape_phase7::kLedgerEntryStatusApproved,
        "tapescope-reviewer-a",
        "First approval through the typed TapeScope seam.");
    expect(reviewedPhase7ExecutionLedger.ok(),
           "TapeScope typed phase7 execution ledger review should succeed: " +
               tapescope::QueryClient::describeError(reviewedPhase7ExecutionLedger.error));
    expect(reviewedPhase7ExecutionLedger.value.updatedEntryIds == reviewedEntryIds,
           "TapeScope typed phase7 execution ledger review should report the updated entry ids");
    expect(!reviewedPhase7ExecutionLedger.value.auditEventId.empty(),
           "TapeScope typed phase7 execution ledger review should expose the appended audit event id");
    expect(reviewedPhase7ExecutionLedger.value.artifact.ledgerStatus == tape_phase7::kLedgerStatusWaitingApproval,
           "TapeScope typed phase7 execution ledger review should wait for the required second approval");
    expect(reviewedPhase7ExecutionLedger.value.artifact.entries.front().reviewStatus ==
               tape_phase7::kLedgerEntryStatusApproved,
           "TapeScope typed phase7 execution ledger review should persist the updated entry status");
    expect(reviewedPhase7ExecutionLedger.value.artifact.entries.front().reviewedBy == "tapescope-reviewer-a",
           "TapeScope typed phase7 execution ledger review should preserve the reviewing actor");
    expect(!reviewedPhase7ExecutionLedger.value.artifact.entries.front().reviewedAtUtc.empty(),
           "TapeScope typed phase7 execution ledger review should stamp reviewed_at_utc");
    expect(reviewedPhase7ExecutionLedger.value.artifact.entries.front().approvalReviewerCount == 1 &&
               !reviewedPhase7ExecutionLedger.value.artifact.entries.front().approvalThresholdMet,
           "TapeScope typed phase7 execution ledger review should track the first approval without marking the entry ready");
    const auto reviewedLedgerSummary =
        tape_phase7::summarizeExecutionLedgerReviewSummary(reviewedPhase7ExecutionLedger.value.artifact);
    expect(reviewedLedgerSummary.approvedCount == 1 &&
               reviewedLedgerSummary.waitingApprovalCount == 1 &&
               reviewedLedgerSummary.requiredApprovalCount == 2 &&
               reviewedLedgerSummary.distinctReviewerCount == 1 &&
               !reviewedLedgerSummary.readyForExecution,
           "TapeScope typed phase7 execution ledger review should expose the waiting-for-approval summary state");

    const auto readyPhase7ExecutionLedger = client.recordExecutionLedgerReviewPayload(
        phase7ExecutionLedgerBuild.value.artifact.ledgerArtifact.artifactId,
        reviewedEntryIds,
        tape_phase7::kLedgerEntryStatusApproved,
        "tapescope-reviewer-b",
        "Second approval through the typed TapeScope seam.");
    expect(readyPhase7ExecutionLedger.ok(),
           "TapeScope typed phase7 execution ledger second approval should succeed: " +
               tapescope::QueryClient::describeError(readyPhase7ExecutionLedger.error));
    expect(readyPhase7ExecutionLedger.value.artifact.ledgerStatus == tape_phase7::kLedgerStatusReadyForExecution,
           "TapeScope typed phase7 execution ledger second approval should mark the ledger ready for execution");
    expect(readyPhase7ExecutionLedger.value.artifact.entries.front().approvalReviewerCount == 2 &&
               readyPhase7ExecutionLedger.value.artifact.entries.front().approvalThresholdMet,
           "TapeScope typed phase7 execution ledger second approval should satisfy the distinct-approval threshold");
    const auto readyLedgerSummary =
        tape_phase7::summarizeExecutionLedgerReviewSummary(readyPhase7ExecutionLedger.value.artifact);
    expect(readyLedgerSummary.readyEntryCount == 1 &&
               readyLedgerSummary.waitingApprovalCount == 0 &&
               readyLedgerSummary.distinctReviewerCount >= 2 &&
               readyLedgerSummary.readyForExecution,
           "TapeScope typed phase7 execution ledger second approval should expose the ready-for-execution summary");

    const auto phase7FullExecutionLedgerBuild =
        client.buildExecutionLedgerPayload(phase7FullPlaybookArtifact.playbookArtifact.artifactId);
    expect(phase7FullExecutionLedgerBuild.ok(), "TapeScope typed full-scope phase7 execution ledger build should succeed: " +
                                                    tapescope::QueryClient::describeError(phase7FullExecutionLedgerBuild.error));
    expect(phase7FullExecutionLedgerBuild.value.artifact.ledgerStatus == tape_phase7::kDefaultLedgerStatus,
           "TapeScope typed full-scope phase7 execution ledger should start pending review");

    tapescope::Phase7ExecutionLedgerInventorySelection filteredLedgerSelection;
    filteredLedgerSelection.playbookArtifactId = phase7PlaybookArtifact.playbookArtifact.artifactId;
    filteredLedgerSelection.sourceArtifactId = phase7AnalysisArtifact.sourceArtifact.artifactId;
    filteredLedgerSelection.ledgerStatus = tape_phase7::kLedgerStatusReadyForExecution;
    filteredLedgerSelection.limit = 10;
    const auto filteredPhase7ExecutionLedgers = client.listExecutionLedgerArtifactsPayload(filteredLedgerSelection);
    expect(filteredPhase7ExecutionLedgers.ok(), "TapeScope filtered phase7 execution ledger inventory should succeed: " +
                                                    tapescope::QueryClient::describeError(filteredPhase7ExecutionLedgers.error));
    expect(filteredPhase7ExecutionLedgers.value.matchedCount >= 1 &&
               !filteredPhase7ExecutionLedgers.value.artifacts.empty(),
           "TapeScope filtered phase7 execution ledger inventory should return the matching review ledger");
    expect(filteredPhase7ExecutionLedgers.value.appliedFilters.value("playbook_artifact_id", std::string()) ==
                   phase7PlaybookArtifact.playbookArtifact.artifactId &&
               filteredPhase7ExecutionLedgers.value.appliedFilters.value("source_artifact_id", std::string()) ==
                   phase7AnalysisArtifact.sourceArtifact.artifactId &&
               filteredPhase7ExecutionLedgers.value.appliedFilters.value("ledger_status", std::string()) ==
                   tape_phase7::kLedgerStatusReadyForExecution,
           "TapeScope filtered phase7 execution ledger inventory should preserve the requested lineage and review-status filters");
    expect(filteredPhase7ExecutionLedgers.value.appliedFilters.value("sort_by", std::string()) == "generated_at_desc",
           "TapeScope filtered phase7 execution ledger inventory should preserve the default sort mode");

    tapescope::Phase7ExecutionLedgerInventorySelection sortedLedgerSelection;
    sortedLedgerSelection.analysisArtifactId = phase7AnalysisArtifact.analysisArtifact.artifactId;
    sortedLedgerSelection.sortBy = "attention_desc";
    sortedLedgerSelection.limit = 10;
    const auto sortedPhase7ExecutionLedgers = client.listExecutionLedgerArtifactsPayload(sortedLedgerSelection);
    expect(sortedPhase7ExecutionLedgers.ok(), "TapeScope sorted phase7 execution ledger inventory should succeed: " +
                                                  tapescope::QueryClient::describeError(sortedPhase7ExecutionLedgers.error));
    expect(sortedPhase7ExecutionLedgers.value.artifacts.size() >= 2,
           "TapeScope sorted phase7 execution ledger inventory should expose both reviewed and pending ledgers");
    expect(sortedPhase7ExecutionLedgers.value.appliedFilters.value("sort_by", std::string()) == "attention_desc",
           "TapeScope sorted phase7 execution ledger inventory should preserve the requested sort mode");
    const auto ledgerAttentionRank = [](const std::string& ledgerStatus) {
        if (ledgerStatus == tape_phase7::kLedgerStatusBlocked) {
            return 6;
        }
        if (ledgerStatus == tape_phase7::kLedgerStatusNeedsInformation) {
            return 5;
        }
        if (ledgerStatus == tape_phase7::kLedgerStatusWaitingApproval) {
            return 4;
        }
        if (ledgerStatus == tape_phase7::kLedgerStatusReadyForExecution) {
            return 3;
        }
        if (ledgerStatus == tape_phase7::kLedgerStatusInProgress) {
            return 2;
        }
        if (ledgerStatus == tape_phase7::kDefaultLedgerStatus) {
            return 1;
        }
        return 0;
    };
    const auto findLedgerIndex = [&](const std::string& artifactId) {
        for (size_t index = 0; index < sortedPhase7ExecutionLedgers.value.artifacts.size(); ++index) {
            if (sortedPhase7ExecutionLedgers.value.artifacts[index].ledgerArtifact.artifactId == artifactId) {
                return index;
            }
        }
        return sortedPhase7ExecutionLedgers.value.artifacts.size();
    };
    const size_t reviewedLedgerIndex =
        findLedgerIndex(readyPhase7ExecutionLedger.value.artifact.ledgerArtifact.artifactId);
    const size_t pendingLedgerIndex =
        findLedgerIndex(phase7FullExecutionLedgerBuild.value.artifact.ledgerArtifact.artifactId);
    expect(reviewedLedgerIndex < sortedPhase7ExecutionLedgers.value.artifacts.size() &&
               pendingLedgerIndex < sortedPhase7ExecutionLedgers.value.artifacts.size(),
           "TapeScope sorted phase7 execution ledger inventory should include both reviewed and untouched pending ledgers");
    expect(ledgerAttentionRank(readyPhase7ExecutionLedger.value.artifact.ledgerStatus) >=
                   ledgerAttentionRank(phase7FullExecutionLedgerBuild.value.artifact.ledgerStatus)
               ? reviewedLedgerIndex <= pendingLedgerIndex
               : reviewedLedgerIndex >= pendingLedgerIndex,
           "TapeScope sorted phase7 execution ledger inventory should order ledgers by attention rank instead of assuming one fixed review status");

    tapescope::Phase7PlaybookInventorySelection filteredPlaybookSelection;
    filteredPlaybookSelection.analysisArtifactId = phase7AnalysisArtifact.analysisArtifact.artifactId;
    filteredPlaybookSelection.sourceArtifactId = phase7AnalysisArtifact.sourceArtifact.artifactId;
    filteredPlaybookSelection.mode = tape_phase7::kDefaultPlaybookMode;
    filteredPlaybookSelection.limit = 10;
    const auto filteredPhase7Playbooks = client.listPlaybookArtifactsPayload(filteredPlaybookSelection);
    expect(filteredPhase7Playbooks.ok(), "TapeScope filtered phase7 playbook inventory should succeed: " +
                                             tapescope::QueryClient::describeError(filteredPhase7Playbooks.error));
    expect(filteredPhase7Playbooks.value.matchedCount >= 1 && !filteredPhase7Playbooks.value.artifacts.empty(),
           "TapeScope filtered phase7 playbook inventory should return the matching dry-run playbook");
    expect(filteredPhase7Playbooks.value.appliedFilters.value("analysis_artifact_id", std::string()) ==
                   phase7AnalysisArtifact.analysisArtifact.artifactId &&
               filteredPhase7Playbooks.value.appliedFilters.value("source_artifact_id", std::string()) ==
                   phase7AnalysisArtifact.sourceArtifact.artifactId &&
               filteredPhase7Playbooks.value.appliedFilters.value("mode", std::string()) ==
                   tape_phase7::kDefaultPlaybookMode,
           "TapeScope filtered phase7 playbook inventory should preserve the requested lineage and mode filters");

    tapescope::Phase7PlaybookInventorySelection sortedPlaybookSelection;
    sortedPlaybookSelection.analysisArtifactId = phase7AnalysisArtifact.analysisArtifact.artifactId;
    sortedPlaybookSelection.sortBy = "filtered_finding_count_desc";
    sortedPlaybookSelection.limit = 10;
    const auto sortedPhase7Playbooks = client.listPlaybookArtifactsPayload(sortedPlaybookSelection);
    expect(sortedPhase7Playbooks.ok(), "TapeScope sorted phase7 playbook inventory should succeed: " +
                                           tapescope::QueryClient::describeError(sortedPhase7Playbooks.error));
    expect(sortedPhase7Playbooks.value.artifacts.size() >= 2,
           "TapeScope sorted phase7 playbook inventory should expose both filtered and full-scope dry-run plans");
    expect(sortedPhase7Playbooks.value.appliedFilters.value("sort_by", std::string()) == "filtered_finding_count_desc",
           "TapeScope sorted phase7 playbook inventory should preserve the requested sort mode");
    expect(sortedPhase7Playbooks.value.artifacts.front().filteredFindingIds.size() >=
               sortedPhase7Playbooks.value.artifacts.back().filteredFindingIds.size(),
           "TapeScope sorted phase7 playbook inventory should sort by filtered finding count");
    expect(phase7FullPlaybookArtifact.filteredFindingIds.size() >= phase7PlaybookArtifact.filteredFindingIds.size(),
           "TapeScope full-scope dry-run playbook should include at least as many findings as the selected subset plan");

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

void testTapeScopeBundlePreviewDecisionHelpers() {
    tape_bundle::PortableBundleInspection sessionInspection;
    sessionInspection.bundleType = "session_bundle";
    sessionInspection.bundleId = "session-bundle:1";
    const auto sessionDecision = tapescope::bundlePreviewDecisionFromInspection(sessionInspection);
    expect(!sessionDecision.importSupported && !sessionDecision.importAllowed,
           "TapeScope bundle preview helper should keep session bundles preview-only");
    expect(sessionDecision.detail.find("not imported") != std::string::npos ||
               sessionDecision.detail.find("previewed") != std::string::npos,
           "TapeScope bundle preview helper should explain why session bundles are blocked from import");

    tape_bundle::PortableBundleInspection caseInspection;
    caseInspection.bundleType = "case_bundle";
    caseInspection.bundleId = "case-bundle:1";
    const auto caseDecision = tapescope::bundlePreviewDecisionFromInspection(caseInspection);
    expect(caseDecision.importSupported && caseDecision.importAllowed,
           "TapeScope bundle preview helper should allow fresh case bundles");

    tapescope::BundleVerifyPayload verifyPayload;
    verifyPayload.verifyStatus = "valid";
    verifyPayload.importSupported = true;
    verifyPayload.alreadyImported = true;
    verifyPayload.canImport = false;
    verifyPayload.importReason = "payload_already_imported";
    verifyPayload.hasImportedCase = true;
    verifyPayload.importedCase.importedCaseId = 77;
    verifyPayload.importedCase.artifactId = "imported-case:77";
    const auto verifyDecision = tapescope::bundlePreviewDecisionFromVerifyPayload(verifyPayload);
    expect(verifyDecision.alreadyImported && !verifyDecision.importAllowed,
           "TapeScope bundle preview helper should block already-imported bundles");
    expect(verifyDecision.detail.find("imported-case:77") != std::string::npos,
           "TapeScope bundle preview helper should surface the matching imported artifact id");

    tapescope::ImportedCaseRow importedCase;
    importedCase.importedCaseId = 88;
    importedCase.artifactId = "imported-case:88";
    const auto matchedDecision =
        tapescope::markBundlePreviewDecisionImported(caseDecision, importedCase, "payload_sha256");
    expect(matchedDecision.alreadyImported && !matchedDecision.importAllowed,
           "TapeScope bundle preview helper should let the UI block duplicate imports after a local hash match");
    expect(matchedDecision.detail.find("payload_sha256") != std::string::npos,
           "TapeScope bundle preview helper should explain why a local imported-case match blocked import");

    const std::string corrupted =
        tapescope::describeBundlePreviewFailure("bundle is not valid MessagePack JSON");
    expect(corrupted.find("valid portable bundle MessagePack payload") != std::string::npos,
           "TapeScope bundle preview helper should give the UI a stable corrupted-bundle diagnostic");
}
