#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export GOWORK=off PYTHONDONTWRITEBYTECODE=1
python3 -B scripts/check-boundaries.py
go test ./...
go vet ./...
PYTHONPATH="$PWD/sdk/python" python3 -B -m unittest discover -s sdk/tests
python3 -B -m unittest discover -s tools -p 'test_*.py'
bash -n install/install.sh scripts/build-release.sh
python3 -B install/install.py --help >/dev/null
