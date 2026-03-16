#pragma once

#include "tape_query_payloads.h"

namespace tapescope::client_internal {

using tape_payloads::firstPresentString;
using tape_payloads::makeError;
using tape_payloads::makeSuccess;
using tape_payloads::packArtifactExportPayload;
using tape_payloads::packEventListPayload;
using tape_payloads::packIncidentListPayload;
using tape_payloads::packInvestigationPayload;
using tape_payloads::packReportInventoryPayload;
using tape_payloads::packSeekOrderPayload;
using tape_payloads::packSessionQualityPayload;
using tape_payloads::parseEvidenceCitations;
using tape_payloads::parseEventRow;
using tape_payloads::parseEventRows;
using tape_payloads::parseIncidentRows;
using tape_payloads::parseReplayRange;
using tape_payloads::parseReportRows;
using tape_payloads::parseSeekReplayRange;
using tape_payloads::propagateError;

} // namespace tapescope::client_internal
