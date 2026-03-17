#pragma once

#include "uw_context_types.h"

#include "uw_runtime.h"

#include <vector>

namespace uw_context_service {

[[nodiscard]] NormalizedContext normalizeProviderRecords(const std::vector<ProviderStep>& steps);

} // namespace uw_context_service