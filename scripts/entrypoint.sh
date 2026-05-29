#!/bin/bash
set -e

PUID=${PUID:-1000}
PGID=${PGID:-1000}

groupmod -o -g "$PGID" appuser 2>/dev/null || groupadd -o -g "$PGID" appuser
usermod -o -u "$PUID" -g "$PGID" appuser 2>/dev/null || useradd -o -u "$PUID" -g "$PGID" appuser

mkdir -p /data /data/downloads

# Fix ownership so the app can read/write its data directory.
# This is essential for users upgrading from a previous version where the
# container ran as root — existing files (app.db, config.json, downloads)
# will be owned by root and inaccessible to the new non-root user.
if chown -R "$PUID:$PGID" /data 2>/tmp/chown_err; then
    echo "[entrypoint] /data ownership set to $PUID:$PGID"
else
    echo "[entrypoint] WARNING: chown failed — /data may not be writable by UID $PUID"
    echo "[entrypoint] $(cat /tmp/chown_err 2>/dev/null)"
    # If /data is not writable by the target user, fall back to root so the
    # container at least starts. The user should fix host permissions manually.
    if ! su -s /bin/sh appuser -c "test -w /data" 2>/dev/null; then
        echo "[entrypoint] Falling back to root — set PUID=0 / PGID=0 or fix host permissions"
        export HOME=/root
        exec "$@"
    fi
fi

export HOME=/home/appuser
echo "[entrypoint] Starting as UID=$PUID GID=$PGID"
exec gosu "$PUID:$PGID" "$@"
