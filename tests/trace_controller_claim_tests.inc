const fs::path& testControllerClaimDir() {
    static const fs::path path = [] {
        char pattern[] = "/tmp/tws_controller_claim_tests.XXXXXX";
        char* created = mkdtemp(pattern);
        if (created == nullptr) {
            throw std::runtime_error("mkdtemp failed for controller claim dir");
        }
        setenv("TWS_CONTROLLER_CLAIM_DIR", created, 1);
        return fs::path(created);
    }();
    return path;
}

void clearControllerClaimFiles() {
    std::error_code ec;
    fs::remove_all(testControllerClaimDir(), ec);
    fs::create_directories(testControllerClaimDir(), ec);
}

void testControllerClaimLeaseBlocksReclaimUntilRelease() {
    clearControllerClaimFiles();

    ControllerClaimLease first;
    ControllerClaimLease second;
    ControllerClaimLease different;
    std::string error;

    expect(tryAcquireControllerClaim("DUALSENSE_slot_0", first, &error),
           "first controller claim should succeed: " + error);
    expect(hasControllerClaim(first), "first controller claim should hold a lease");

    expect(!tryAcquireControllerClaim("DUALSENSE_slot_0", second, &error),
           "second claim for the same key should fail while leased");
    expectContains(error, "already claimed by another app process",
                   "same-key lease failure should describe the active claim");

    error.clear();
    expect(tryAcquireControllerClaim("DUALSENSE_slot_1", different, &error),
           "different controller key should claim independently: " + error);
    expect(hasControllerClaim(different), "different controller claim should hold a lease");

    releaseControllerClaim(first);
    expect(!hasControllerClaim(first), "released claim should clear the original lease");

    error.clear();
    expect(tryAcquireControllerClaim("DUALSENSE_slot_0", second, &error),
           "same key should be reclaimable after release: " + error);

    releaseControllerClaim(second);
    releaseControllerClaim(different);
}

void testControllerLightFallbackBypassesStaleLightWhenClaimHeld() {
    clearControllerClaimFiles();

    const std::string claimKey = controllerClaimKeyForPlayerIndex(0);
    expect(!claimKey.empty(), "stable player index should produce a claim key");

    ControllerClaimLease lease;
    expect(shouldUseControllerLightOwnershipFallback(claimKey, lease),
           "without a claim lease, light fallback should still apply");

    std::string error;
    expect(tryAcquireControllerClaim(claimKey, lease, &error),
           "controller claim should succeed: " + error);
    expect(!shouldUseControllerLightOwnershipFallback(claimKey, lease),
           "held claim should bypass light fallback to avoid stale-light false positives");
    expect(shouldUseControllerLightOwnershipFallback("", lease),
           "missing claim key should keep light fallback enabled");

    releaseControllerClaim(lease);
}

void testControllerClaimKeyUsesStablePlayerIndexIdentity() {
    std::vector<int> firstOrder = {0, 1};
    std::vector<int> secondOrder = {1, 0};
    std::vector<std::string> firstKeys;
    std::vector<std::string> secondKeys;

    for (int playerIndex : firstOrder) {
        firstKeys.push_back(controllerClaimKeyForPlayerIndex(playerIndex));
    }
    for (int playerIndex : secondOrder) {
        secondKeys.push_back(controllerClaimKeyForPlayerIndex(playerIndex));
    }

    std::sort(firstKeys.begin(), firstKeys.end());
    std::sort(secondKeys.begin(), secondKeys.end());

    expect(firstKeys == secondKeys,
           "player-index claim keys should stay stable even when controller enumeration order differs");
    expect(controllerClaimKeyForPlayerIndex(-1).empty(),
           "unset player index should not produce a stable cross-process claim key");
    expect(controllerClaimKeyForPlayerIndex(4).empty(),
           "out-of-range player index should not produce a stable cross-process claim key");
}
