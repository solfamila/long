#!/usr/bin/env python3

import argparse
import json
import os
import sys
from pathlib import Path


DEFAULT_MODEL = "gemini-3.1-pro-preview"
DEFAULT_UW_URL = "https://api.unusualwhales.com/api/mcp"


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if value and len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        os.environ.setdefault(key, value)


def require_env(*names: str) -> tuple[str, str]:
    for name in names:
        value = os.getenv(name)
        if value:
            return name, value
    joined = ", ".join(names)
    raise SystemExit(f"Missing required credential. Set one of: {joined}")


def build_config(uw_token: str, uw_url: str) -> dict:
    return {
        "system_instruction": (
            "You are a market-data assistant. Use the remote UW MCP server when the "
            "user asks for unusual options flow, alerts, or price-state information. "
            "If the MCP server does not expose a requested capability, say that clearly."
        ),
        "tools": [
            {
                "mcp_servers": [
                    {
                        "name": "uw",
                        "streamable_http_transport": {
                            "url": uw_url,
                            "headers": {
                                "AUTHORIZATION": f"Bearer {uw_token}",
                            },
                            "timeout": "15s",
                        },
                    }
                ]
            }
        ],
    }


def classify_gemini_error(exc: Exception) -> tuple[str, dict]:
    message = str(exc)
    status_code = getattr(exc, "status_code", None)

    if status_code == 403 or "SERVICE_DISABLED" in message:
        project = None
        activation_url = None

        project_marker = "project "
        project_index = message.find(project_marker)
        if project_index != -1:
            project_start = project_index + len(project_marker)
            project_digits = []
            for ch in message[project_start:]:
                if ch.isdigit():
                    project_digits.append(ch)
                else:
                    break
            if project_digits:
                project = "".join(project_digits)

        activation_marker = "https://console.developers.google.com/apis/api/generativelanguage.googleapis.com/overview?project="
        activation_index = message.find(activation_marker)
        if activation_index != -1:
            activation_url = message[activation_index:].split()[0].rstrip(".',}")

        return (
            "gemini_service_disabled",
            {
                "project": project,
                "activation_url": activation_url,
            },
        )

    if status_code == 429 or "RESOURCE_EXHAUSTED" in message or "Quota exceeded" in message:
        return ("gemini_quota_exhausted", {})

    return ("gemini_api_error", {})


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Call Gemini with Unusual Whales as a remote MCP server."
    )
    parser.add_argument(
        "prompt",
        nargs="?",
        default="Use the UW MCP tools to summarize unusual options flow in INTC today.",
        help="Prompt to send to Gemini.",
    )
    parser.add_argument(
        "--model",
        default=os.getenv("GEMINI_MODEL", DEFAULT_MODEL),
        help="Gemini model name.",
    )
    parser.add_argument(
        "--uw-url",
        default=os.getenv("LONG_UW_MCP_URL", DEFAULT_UW_URL),
        help="Remote UW MCP URL.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print the full response JSON instead of only text.",
    )
    return parser.parse_args()


def main() -> int:
    repo_root = Path(__file__).resolve().parent.parent
    load_env_file(repo_root / ".env.local")

    try:
        from google import genai
        from google.genai import errors as genai_errors
    except ImportError:
        print(
            "Missing dependency: google-genai. Install it with `python3 -m pip install google-genai`.",
            file=sys.stderr,
        )
        return 2

    args = parse_args()
    _, gemini_api_key = require_env("GEMINI_API_KEY", "GOOGLE_API_KEY")
    token_name, uw_token = require_env("UW_API_TOKEN", "UW_BEARER_TOKEN")

    client = genai.Client(api_key=gemini_api_key)
    config = build_config(uw_token=uw_token, uw_url=args.uw_url)

    try:
        response = client.models.generate_content(
            model=args.model,
            contents=args.prompt,
            config=config,
        )
    except genai_errors.ClientError as exc:
        diagnostic, extra = classify_gemini_error(exc)
        print(f"Gemini request failed: {exc}", file=sys.stderr)
        print(
            json.dumps(
                {
                    "model": args.model,
                    "uw_url": args.uw_url,
                    "uw_credential_env": token_name,
                    "diagnostic": diagnostic,
                    **extra,
                },
                indent=2,
            ),
            file=sys.stderr,
        )
        return 1

    if args.json:
        if hasattr(response, "model_dump"):
            print(json.dumps(response.model_dump(exclude_none=True), indent=2))
        else:
            print(response)
        return 0

    text = getattr(response, "text", None)
    if text:
        print(text)
    else:
        if hasattr(response, "model_dump"):
            print(json.dumps(response.model_dump(exclude_none=True), indent=2))
        else:
            print(response)

    print(
        json.dumps(
            {
                "model": args.model,
                "uw_url": args.uw_url,
                "uw_credential_env": token_name,
            },
            indent=2,
        ),
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())