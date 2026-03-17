#pragma once

#include "tape_engine_protocol.h"
#include "uw_context_types.h"

namespace uw_context_service {

using json = nlohmann::json;

tape_engine::QueryResponse buildEnrichmentResponse(const tape_engine::QueryRequest& request,
                                                   const tape_engine::QueryResponse& localEvidence,
                                                   const BuildRequest& buildRequest);

} // namespace uw_context_service