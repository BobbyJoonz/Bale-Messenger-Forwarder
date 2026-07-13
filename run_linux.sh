#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

if [ ! -x ".venv/bin/python" ]; then
  python3 -m venv .venv
fi

. .venv/bin/activate
python -m pip install --disable-pip-version-check -r requirements.txt
exec python main.py "$@"
