#pragma once

#include "uw_context_types.h"

namespace uw_context_service {

class UWMcpConnector {
public:
    [[nodiscard]] ProviderStep fetch(const FetchPlan& plan) const;
};

class UWRestConnector {
public:
    [[nodiscard]] ProviderStep fetch(const FetchPlan& plan) const;
};

class UWWsConnector {
public:
    [[nodiscard]] ProviderStep fetch(const FetchPlan& plan) const;
};

} // namespace uw_context_service
