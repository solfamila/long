// Keep one test binary, but split the large trace suite into subsystem fragments
// so phase-specific coverage can evolve without turning this file back into a monolith.
#include "trace_test_support.inc"
#include "trace_core_tests.inc"
#include "trace_tapescope_tests.inc"
#include "trace_engine_core_tests.inc"
#include "trace_engine_analyzer_tests.inc"
#include "trace_runtime_tests.inc"

} // namespace

int main() {
    try {
        testDataDir();
        testBridgeBatchCodecRoundTripPreservesOrderAndMetadata();
        testReplayPrefersRichLiveTrace();
        testTraceIdFloorRecoversFromLog();
        testReplayHandlesPartialFillsAndCommission();
        testWebSocketRuntimeGuards();
        testRecoverySnapshotReportsAbnormalShutdown();
        testBridgeOutboxSourceSeqPreservesAcceptanceOrderingAndAnchors();
        testBridgeBatchFixtureMatchesGoldenFrame();
        testBridgeBatchSenderPreservesOrderingAcrossRetries();
        testBridgeDispatchBatchRespectsThresholdsAndImmediateFlush();
        testBridgeDispatchSnapshotAndDeliveryAck();
        testUnixDomainSocketTransportSendsFramedBatch();
        testTapeEngineAcceptsBatchAssignsSessionSeqAndWritesSegments();
        testTapeEngineEmitsGapMarkersAndDeduplicatesSourceSeq();
        testTapeEngineQueryStatusAndReads();
        testTapeEngineQueryOperationRegistryCanonicalizesAliases();
        testTapeScopeClientReadsPhase4EngineSeam();
        testTapeEngineRevisionPinnedReadsCanOverlayMutableTail();
        testTapeEnginePhase3FindingsIncidentsAndProtectedWindows();
        testTapeEnginePhase3ArtifactsPersistAcrossRestartAndReadProtectedWindow();
        testTapeEnginePhase3ScansSessionIntoDurableReportArtifact();
        testTapeEnginePhase3PersistsIncidentAndOrderCaseReports();
        testTapeEnginePhase6ExportsAndImportsPortableBundles();
        testTapeEnginePhase3InvestigationContractMatchesGoldenFixtures();
        testTapeEnginePhase3CollapsesRepeatedFindingsIntoRankedIncidents();
        testTapeEnginePhase3DetectsInsideLiquiditySignals();
        testTapeEnginePhase3DetectsDisplayInstabilitySignals();
        testTapeEnginePhase3DetectsPullFollowThroughAndQuoteFlickerSignals();
        testTapeEnginePhase3DetectsTradeAfterDepletionAndAbsorptionPersistenceSignals();
        testTapeEnginePhase3DetectsFillInvalidationSignals();
        testTapeEnginePhase3BuildsOrderWindowMarketImpactFinding();
        testTapeEnginePhase3BuildsPassiveFillQueueProxyAndAdverseSelection();
        testTapeEnginePhase3BuildsPassiveQueueLossAndCutThroughSignals();
        testTapeEnginePhase3BuildsSweepAndFadeSignals();
        testTapeEnginePhase3BuildsFillOutcomeChains();
        testTapeEnginePhase3BuildsTradePressureOrderCase();
        testTapeEngineResetMarkerPreservesCanonicalInstrumentIdentity();
        testTapeEngineReplaySnapshotRebuildsFrozenMarketState();
        testBridgeMarketDataEmissionExpandsPublicEvents();
        testTapeEnginePrefersStrongConfiguredInstrumentIdentityAndMarksHeuristicFallback();
        testTapeEngineCanRejectMismatchedStrongInstrumentIdsInStrictMode();
        testBridgeLifecycleEmissionExpandsPrivateOrderEvents();
        testGeneratedRuntimeRegistryMatchesPhase15QueueSpec();
        testBridgeOutboxOverflowWritesExplicitLossMarker();
        testRecoverySnapshotReportsBridgeContinuityLossAfterAbnormalShutdown();
        testTradingWrapperSessionReadyAndReconnect();
        testTradingWrapperIgnoresDuplicateOrderStatus();
        testRuntimePresentationSnapshotCapturesConsistentState();
        testPendingUiSyncUpdateConsumesFlags();
        testRuntimePresentationSnapshotTracksQuoteFreshnessAndCancelMarking();
        testOrderWatchdogEscalatesToManualReview();
        testCancelAndPartialFillWatchdogs();
        testOpenOrderResolvesReconcilingStateAndFloorsOrderIds();
        testManualReconcileAndAcknowledgeFlow();
        std::cout << "All trace tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
