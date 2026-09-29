#!/bin/sh
# 以 PUID/PGID 运行引擎；root 启动时降权，非 root 直接跑
set -e
PUID="${PUID:-1000}"
PGID="${PGID:-1001}"

if [ "$(id -u)" = "0" ] && [ "$PUID" != "0" ]; then
    getent group "$PGID" >/dev/null 2>&1 || addgroup -g "$PGID" qsync 2>/dev/null || true
    id -u "$PUID" >/dev/null 2>&1 || adduser -D -u "$PUID" -G "$(getent group "$PGID" | cut -d: -f1)" qsync 2>/dev/null || true
    for d in "$STATE_DIR" "$SRC_BASE" "$DST_BASE"; do
        mkdir -p "$d" 2>/dev/null || true
        [ -d "$d" ] && chown -R "$PUID:$PGID" "$d" || true
    done
    exec su-exec "$PUID:$PGID" python3 /app/engine.py
fi

exec python3 /app/engine.py
