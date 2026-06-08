#!/bin/sh
set -eu

PUID="${PUID:-1000}"
PGID="${PGID:-1000}"

case "$PUID:$PGID" in
  *[!0-9:]* | :* | *: )
    echo "PUID and PGID must be numeric" >&2
    exit 1
    ;;
esac

echo "PUID: $PUID"
echo "PGID: $PGID"
echo "UMASK: $UMASK"

if [ -n "$UMASK" ]; then
    umask "$UMASK"
else
    umask "0022"
fi

mkdir -p /app/.runtime /app/agent_lab

if [ "$PUID" = "0" ]; then
  exec "$@"
fi

if ! getent group "$PGID" >/dev/null 2>&1; then
  groupadd --gid "$PGID" tater
fi

if ! getent passwd "$PUID" >/dev/null 2>&1; then
  useradd \
    --uid "$PUID" \
    --gid "$PGID" \
    --home-dir /app \
    --no-create-home \
    --shell /usr/sbin/nologin \
    tater
fi

chown -R "$PUID:$PGID" /app

exec gosu "$PUID:$PGID" "$@"
