#pragma once

#include "tape_engine_protocol.h"

#include <string>

namespace tape_engine {

class Client {
public:
    explicit Client(std::string socketPath);

    bool query(const QueryRequest& request, QueryResponse* response, std::string* error = nullptr) const;

    const std::string& socketPath() const;

private:
    std::string socketPath_;
};

} // namespace tape_engine
