#pragma once

#include <cstdint>
#include <string>

void macLogInfo(const std::string& category, const std::string& message);
void macLogError(const std::string& category, const std::string& message);
void macTraceBegin(std::uint64_t traceId, const std::string& stage, const std::string& message = {});
void macTraceEnd(std::uint64_t traceId, const std::string& stage, const std::string& message = {});
void macTraceEvent(std::uint64_t traceId, const std::string& stage, const std::string& message = {});
