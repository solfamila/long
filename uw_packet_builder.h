#pragma once

#include "uw_context_types.h"

namespace uw_context_service {

[[nodiscard]] json buildPacketArtifact(const json& localEvidence,
                                       const NormalizedContext& normalized,
                                       const BuildRequest& request);

} // namespace uw_context_service