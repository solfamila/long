#include "tape_bundle_inspection.h"

#include <CommonCrypto/CommonDigest.h>

#include <fstream>
#include <iterator>

namespace tape_bundle {

namespace {

std::string sha256Hex(const std::uint8_t* data, std::size_t size) {
    unsigned char digest[CC_SHA256_DIGEST_LENGTH];
    CC_SHA256(data, static_cast<CC_LONG>(size), digest);

    static constexpr char kHex[] = "0123456789abcdef";
    std::string output;
    output.reserve(CC_SHA256_DIGEST_LENGTH * 2);
    for (unsigned char byte : digest) {
        output.push_back(kHex[(byte >> 4) & 0x0fU]);
        output.push_back(kHex[byte & 0x0fU]);
    }
    return output;
}

std::string sha256Hex(const std::vector<std::uint8_t>& input) {
    return sha256Hex(input.data(), input.size());
}

} // namespace

bool inspectPortableBundle(const std::filesystem::path& inputBundlePath,
                           PortableBundleInspection* out,
                           std::string* error) {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "bundle inspection requires an output struct";
        }
        return false;
    }

    *out = PortableBundleInspection{};

    std::filesystem::path bundlePath = inputBundlePath;
    std::error_code pathError;
    const std::filesystem::path absolutePath = std::filesystem::absolute(bundlePath, pathError);
    if (!pathError) {
        bundlePath = absolutePath;
    }
    if (!std::filesystem::exists(bundlePath)) {
        if (error != nullptr) {
            *error = "bundle_path does not exist";
        }
        return false;
    }

    std::ifstream in(bundlePath, std::ios::binary);
    if (!in.is_open()) {
        if (error != nullptr) {
            *error = "failed to open bundle";
        }
        return false;
    }

    const std::vector<std::uint8_t> bytes((std::istreambuf_iterator<char>(in)),
                                          std::istreambuf_iterator<char>());
    if (bytes.empty()) {
        if (error != nullptr) {
            *error = "bundle is empty";
        }
        return false;
    }

    const json bundle = json::from_msgpack(bytes, true, false);
    if (bundle.is_discarded()) {
        if (error != nullptr) {
            *error = "bundle is not valid MessagePack JSON";
        }
        return false;
    }
    if (bundle.value("schema", std::string()) != tape_engine::kReportBundleSchema ||
        bundle.value("version", 0U) != tape_engine::kReportBundleVersion) {
        if (error != nullptr) {
            *error = "bundle schema/version is not supported";
        }
        return false;
    }

    const json sourceArtifact = bundle.value("source_artifact", json::object());
    const json sourceReport = bundle.value("source_report", json::object());
    const json reportBundle = bundle.value("report_bundle", json::object());

    out->bundlePath = bundlePath;
    out->bytes = bytes;
    out->bundle = bundle;
    out->sourceArtifact = sourceArtifact;
    out->sourceReport = sourceReport;
    out->reportBundle = reportBundle;
    out->reportSummary = reportBundle.value("summary", json::object())
        .value("report", reportBundle.value("summary", json::object()).value("report_summary", json::object()));
    out->reportMarkdown = bundle.value("report_markdown", std::string());
    out->bundleId = bundle.value("bundle_id", std::string());
    out->bundleType = bundle.value("bundle_type", std::string());
    out->fileName = bundle.value("file_name", bundlePath.filename().string());
    out->instrumentId = bundle.value("instrument_id", std::string());
    out->headline = bundle.value("headline", std::string());
    out->payloadSha256 = sha256Hex(bytes);
    out->sourceArtifactId =
        bundle.value("source_artifact_id", sourceArtifact.value("artifact_id", std::string()));
    out->sourceReportId =
        bundle.value("source_report_id", sourceReport.value("report_id", 0ULL));
    out->sourceRevisionId = bundle.value("source_revision_id",
        sourceReport.value("revision_id", sourceArtifact.value("revision_id", 0ULL)));
    out->firstSessionSeq = bundle.value("first_session_seq",
        sourceArtifact.value("first_session_seq", sourceArtifact.value("from_session_seq", 0ULL)));
    out->lastSessionSeq = bundle.value("last_session_seq",
        sourceArtifact.value("last_session_seq", sourceArtifact.value("to_session_seq", 0ULL)));
    out->exportedTsEngineNs = bundle.value("exported_ts_engine_ns", 0ULL);
    return true;
}

} // namespace tape_bundle
