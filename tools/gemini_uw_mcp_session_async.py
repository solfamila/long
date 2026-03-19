#!/usr/bin/env python3

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

import httpx
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client
from google import genai


DEFAULT_MODEL = "gemini-2.5-flash"
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
    raise SystemExit(f"Missing required credential. Set one of: {', '.join(names)}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Use Google GenAI async MCP-session integration against the UW remote MCP server."
    )
    parser.add_argument(
        "prompt",
        nargs="?",
        default="What tools do you have access to? Name a few relevant ones.",
        help="Prompt to send to Gemini.",
    )
    parser.add_argument("--model", default=DEFAULT_MODEL, help="Gemini model name.")
    parser.add_argument("--uw-url", default=DEFAULT_UW_URL, help="Remote UW MCP URL.")
    parser.add_argument("--list-tools", action="store_true", help="List MCP tools before calling Gemini.")
    parser.add_argument("--json", action="store_true", help="Print response JSON.")
    return parser.parse_args()


async def async_main() -> int:
    repo_root = Path(__file__).resolve().parent.parent
    load_env_file(repo_root / ".env.local")
    args = parse_args()

    _, gemini_api_key = require_env("GEMINI_API_KEY", "GOOGLE_API_KEY")
    token_name, uw_token = require_env("UW_API_TOKEN", "UW_BEARER_TOKEN")

    async with httpx.AsyncClient(headers={"Authorization": f"Bearer {uw_token}"}) as http_client:
        async with streamable_http_client(args.uw_url, http_client=http_client) as streams:
            read_stream, write_stream = streams[:2]
            async with ClientSession(read_stream, write_stream) as session:
                initialize_result = await session.initialize()
                tools_result = await session.list_tools()

                print(
                    json.dumps(
                        {
                            "uw_url": args.uw_url,
                            "uw_credential_env": token_name,
                            "mcp_server": getattr(
                                getattr(initialize_result, "server_info", None)
                                or getattr(initialize_result, "serverInfo", None),
                                "name",
                                "unknown",
                            ),
                            "tool_count": len(tools_result.tools),
                        },
                        indent=2,
                    ),
                    file=sys.stderr,
                )

                if args.list_tools:
                    for tool in tools_result.tools[:20]:
                        print(tool.name)

                client = genai.Client(api_key=gemini_api_key)
                try:
                    response = await client.aio.models.generate_content(
                        model=args.model,
                        contents=args.prompt,
                        config=genai.types.GenerateContentConfig(
                            temperature=0,
                            tools=[session],
                        ),
                    )
                finally:
                    await client.aio.aclose()

                if args.json and hasattr(response, "model_dump"):
                    print(json.dumps(response.model_dump(exclude_none=True), indent=2))
                else:
                    print(response.text)
    return 0


def main() -> int:
    return asyncio.run(async_main())


if __name__ == "__main__":
    raise SystemExit(main())