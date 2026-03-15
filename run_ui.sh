#!/bin/sh
set -eu

HTMLUI_HOST="${HTMLUI_HOST:-0.0.0.0}"
HTMLUI_PORT="${HTMLUI_PORT:-8501}"
set -- uvicorn tateros_app:app --host "${HTMLUI_HOST}" --port "${HTMLUI_PORT}" --no-access-log

exec "$@"
