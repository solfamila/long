#pragma once

#include "uw_context_types.h"

namespace uw_context_service {

GeminiExecutionResult executeGeminiPacket(const json& packetArtifact,
                                         const BuildRequest& request,
                                         bool allowExecution);

} // namespace uw_context_service