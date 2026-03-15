#!/usr/bin/env python3

import json
import pathlib
import sys


def to_pascal(identifier: str) -> str:
    return "".join(part.capitalize() for part in identifier.split("_"))


def emit_header(config: dict) -> str:
    subsystems = config["subsystems"]
    categories = config["categories"]
    queues = config["queues"]

    lines = []
    lines.append("#pragma once")
    lines.append("")
    lines.append("#include <array>")
    lines.append("#include <string_view>")
    lines.append("")
    lines.append("namespace runtime_registry {")
    lines.append("")
    lines.append(f"inline constexpr int kRegistryVersion = {int(config['version'])};")
    lines.append("")

    lines.append("enum class SubsystemId {")
    for key in subsystems:
        lines.append(f"    {to_pascal(key)},")
    lines.append("};")
    lines.append("")
    lines.append("constexpr std::string_view subsystemName(SubsystemId id) {")
    lines.append("    switch (id) {")
    for key, value in subsystems.items():
        lines.append(f"        case SubsystemId::{to_pascal(key)}:")
        lines.append(f'            return "{value}";')
    lines.append("        default:")
    lines.append('            return "";')
    lines.append("    }")
    lines.append("}")
    lines.append("")

    lines.append("enum class LogCategory {")
    for category in categories:
        lines.append(f"    {to_pascal(category)},")
    lines.append("};")
    lines.append("")
    lines.append("constexpr std::string_view logCategoryName(LogCategory category) {")
    lines.append("    switch (category) {")
    for category in categories:
        lines.append(f"        case LogCategory::{to_pascal(category)}:")
        lines.append(f'            return "{category}";')
    lines.append("        default:")
    lines.append('            return "";')
    lines.append("    }")
    lines.append("}")
    lines.append("")

    lines.append("enum class QueueId {")
    for queue in queues:
        lines.append(f"    {to_pascal(queue['id'])},")
    lines.append("};")
    lines.append("")
    lines.append("struct QueueSpec {")
    lines.append("    QueueId id;")
    lines.append("    SubsystemId subsystem;")
    lines.append("    LogCategory category;")
    lines.append("    std::string_view label;")
    lines.append("    std::string_view qosName;")
    lines.append("};")
    lines.append("")
    lines.append(f"inline constexpr std::array<QueueSpec, {len(queues)}> kQueueSpecs = {{{{")
    for queue in queues:
        lines.append(
            "    {QueueId::" + to_pascal(queue["id"]) +
            ", SubsystemId::" + to_pascal(queue["subsystem"]) +
            ", LogCategory::" + to_pascal(queue["category"]) +
            ', "' + queue["label"] + '"' +
            ', "' + queue["qos"] + "\"},"
        )
    lines.append("}};")
    lines.append("")
    lines.append("constexpr QueueSpec queueSpec(QueueId id) {")
    lines.append("    switch (id) {")
    for index, queue in enumerate(queues):
        lines.append(f"        case QueueId::{to_pascal(queue['id'])}:")
        lines.append(f"            return kQueueSpecs[{index}];")
    lines.append("        default:")
    lines.append("            return kQueueSpecs[0];")
    lines.append("    }")
    lines.append("}")
    lines.append("")
    lines.append("inline constexpr std::string_view kObservabilitySubsystem = subsystemName(SubsystemId::LongBridge);")
    lines.append("")
    lines.append("} // namespace runtime_registry")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: generate_runtime_registry.py <input> <output>", file=sys.stderr)
        return 1

    input_path = pathlib.Path(sys.argv[1])
    output_path = pathlib.Path(sys.argv[2])
    config = json.loads(input_path.read_text())
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(emit_header(config))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
