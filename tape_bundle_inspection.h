#pragma once

#include "tape_engine_protocol.h"

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

namespace tape_bundle {

struct PortableBundleInspection {
    std::filesystem::path bundlePath;
    std::vector<std::uint8_t> bytes;
    json bundle = json::object();
    json sourceArtifact = json::object();
    json sourceReport = json::object();
    json reportBundle = json::object();
    json reportSummary = json::object();
    std::string reportMarkdown;
    std::string bundleId;
    std::string bundleType;
    std::string fileName;
    std::string instrumentId;
    std::string headline;
    std::string payloadSha256;
    std::string sourceArtifactId;
    std::uint64_t sourceReportId = 0;
    std::uint64_t sourceRevisionId = 0;
    std::uint64_t firstSessionSeq = 0;
    std::uint64_t lastSessionSeq = 0;
    std::uint64_t exportedTsEngineNs = 0;
};

bool inspectPortableBundle(const std::filesystem::path& bundlePath,
                           PortableBundleInspection* out,
                           std::string* error);

} // namespace tape_bundle
