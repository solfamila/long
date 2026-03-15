#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: ./scripts/bootstrap_deps.sh [--skip-readme-check]

Initializes pinned dependency submodules used by this repository:
  - ixwebsocket
  - nlohmann_json
EOF
}

check_readme_pins=1
for arg in "$@"; do
    case "$arg" in
        --skip-readme-check)
            check_readme_pins=0
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "error: unknown argument: $arg" >&2
            usage >&2
            exit 2
            ;;
    esac
done

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"
cd "${repo_root}"

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    echo "error: ${repo_root} is not a git work tree" >&2
    exit 1
fi

echo "[bootstrap] syncing submodule remotes"
git submodule sync --recursive

echo "[bootstrap] initializing pinned submodule revisions"
git submodule update --init --recursive

require_path() {
    local path="$1"
    if [[ ! -e "$path" ]]; then
        echo "error: expected path missing after bootstrap: $path" >&2
        exit 1
    fi
}

submodule_sha() {
    local path="$1"
    local sha
    sha="$(git submodule status -- "$path" | awk 'NR==1 { gsub(/^[+\\-U]/, "", $1); print $1 }')"
    if [[ -z "$sha" ]]; then
        echo "error: unable to resolve pinned submodule SHA for $path" >&2
        exit 1
    fi
    printf '%s\n' "$sha"
}

require_path "ixwebsocket/CMakeLists.txt"
require_path "nlohmann_json/single_include/nlohmann/json.hpp"

if [[ "$check_readme_pins" -eq 1 ]]; then
    ix_sha="$(submodule_sha "ixwebsocket")"
    nlohmann_sha="$(submodule_sha "nlohmann_json")"

    if ! grep -Fq "$ix_sha" README.md; then
        echo "error: README.md does not mention pinned ixwebsocket SHA $ix_sha" >&2
        exit 1
    fi
    if ! grep -Fq "$nlohmann_sha" README.md; then
        echo "error: README.md does not mention pinned nlohmann_json SHA $nlohmann_sha" >&2
        exit 1
    fi
fi

echo "[bootstrap] complete"
git submodule status --recursive -- ixwebsocket nlohmann_json
