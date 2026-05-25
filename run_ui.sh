#!/bin/sh
set -eu

if [ -f ".runtime/tater_profile.env" ]; then
  # shellcheck disable=SC1091
  . ".runtime/tater_profile.env"
fi

HTMLUI_HOST="${HTMLUI_HOST:-0.0.0.0}"
HTMLUI_PORT="${HTMLUI_PORT:-8501}"
TATER_PYTHON="${TATER_PYTHON:-}"
TATER_VENV_DIR="${TATER_VENV_DIR:-.venv}"

if [ -z "${TATER_PYTHON}" ]; then
  if [ -f "${TATER_VENV_DIR}/bin/activate" ]; then
    venv_path="$(cd "${TATER_VENV_DIR}" && pwd -P)"
    current_venv=""
    if [ "${VIRTUAL_ENV:-}" ]; then
      current_venv="$(cd "${VIRTUAL_ENV}" 2>/dev/null && pwd -P || printf '%s' "${VIRTUAL_ENV}")"
    fi
    if [ "${current_venv}" != "${venv_path}" ]; then
      # shellcheck disable=SC1091
      . "${TATER_VENV_DIR}/bin/activate"
    fi
  fi

  if [ "${VIRTUAL_ENV:-}" ] && [ -x "${VIRTUAL_ENV}/bin/python" ]; then
    TATER_PYTHON="${VIRTUAL_ENV}/bin/python"
  elif [ -x "${TATER_VENV_DIR}/bin/python" ]; then
    TATER_PYTHON="${TATER_VENV_DIR}/bin/python"
  else
    TATER_PYTHON="python"
  fi
fi

set -- "${TATER_PYTHON}" -m uvicorn tateros_app:app --host "${HTMLUI_HOST}" --port "${HTMLUI_PORT}" --no-access-log

exec "$@"
