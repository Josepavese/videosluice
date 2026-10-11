#!/usr/bin/env bash
set -euo pipefail
command -v python3 >/dev/null || { echo 'Python 3.10+ is required' >&2; exit 1; }
command -v curl >/dev/null || { echo 'curl is required' >&2; exit 1; }
scratch="$(mktemp -d)"
trap 'rm -rf -- "$scratch"' EXIT
version=latest
args=("$@")
for ((i=0; i<${#args[@]}; i++)); do
    if [ "${args[i]}" = --version ]; then version="${args[i+1]:?missing version}"; fi
done
if [ "$version" = latest ]; then
    endpoint=https://github.com/Josepavese/videosluice/releases/latest/download
elif [[ "$version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    endpoint="https://github.com/Josepavese/videosluice/releases/download/$version"
else
    echo 'invalid version' >&2; exit 2
fi
curl --fail --location --proto '=https' --tlsv1.2 "$endpoint/install.py" -o "$scratch/install.py"
python3 "$scratch/install.py" "$@"
