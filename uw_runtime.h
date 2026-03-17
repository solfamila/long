#pragma once

#include <initializer_list>
#include <string>
#include <string_view>

namespace uw_context_service {

struct CredentialBinding {
    bool present = false;
    bool fromProcessEnv = false;
    std::string value;
    std::string sourceEnv;
};

CredentialBinding loadCredentialBinding(std::initializer_list<std::string_view> envNames);
std::string localCredentialFilePath();
std::string stableHashHex(std::string_view value);
std::string nowUtc();
std::uint64_t nowUnixSeconds();

} // namespace uw_context_service