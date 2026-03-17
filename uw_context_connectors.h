#pragma once

#include "uw_context_types.h"

namespace uw_context_service {

struct UWMcpFacetToolSelection {
    std::string facet;
    std::string toolName;
    int score = 0;
    std::string rationale;
};

struct UWMcpFacetResolution {
    std::vector<UWMcpFacetToolSelection> selectedTools;
    std::vector<std::string> unsupportedFacets;
    json facetDiagnostics = json::array();
};

[[nodiscard]] UWMcpFacetResolution resolveUWMcpToolsForFacets(const json& toolCatalog,
                                                              const std::vector<std::string>& facets);
[[nodiscard]] std::vector<std::string> unresolvedUWMcpFacets(const ProviderStep& step,
                                                             const std::vector<std::string>& requestedFacets);

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
