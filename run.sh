#!/usr/bin/env bash
set -e

PROJ_DIR="$(cd "$(dirname "$0")" && pwd)"
SERVICE_USER="agent"

# If running as root — create service user and re-exec
if [ "$(id -u)" = "0" ]; then
    # Create user if needed
    if ! id "$SERVICE_USER" &>/dev/null; then
        useradd -m -s /bin/bash "$SERVICE_USER"
        echo "[run.sh] Created user '$SERVICE_USER'"
    fi

    # Give ownership of project dir
    chown -R "$SERVICE_USER":"$SERVICE_USER" "$PROJ_DIR"

    # Ensure service user can traverse parent directories
    PARENT="$PROJ_DIR"
    while [ "$PARENT" != "/" ]; do
        chmod o+x "$PARENT"
        PARENT="$(dirname "$PARENT")"
    done

    # Claude auth: long-lived token from config.json is set via CLAUDE_CODE_OAUTH_TOKEN
    # in src/config.py at startup. No credential symlinks needed.
    # Generate token with: claude setup-token

    # Create tmp dir for mem0 SDK and workdirs
    install -d -o "$SERVICE_USER" -g "$SERVICE_USER" /tmp/mem0-claude-code
    install -d -o "$SERVICE_USER" -g "$SERVICE_USER" \
        "$PROJ_DIR/data/analyst_workdir" \
        "$PROJ_DIR/data/classifier_workdir"

    # Ensure docx-js is installed for report generation
    if [ -f "$PROJ_DIR/data/analyst_workdir/package.json" ] && \
       [ ! -d "$PROJ_DIR/data/analyst_workdir/node_modules/docx" ]; then
        (cd "$PROJ_DIR/data/analyst_workdir" && npm install --silent 2>/dev/null)
        chown -R "$SERVICE_USER":"$SERVICE_USER" "$PROJ_DIR/data/analyst_workdir"
        echo "[run.sh] docx-js installed"
    fi

    # Launch in screen as service user
    echo "[run.sh] Starting in screen session 'agent' as '$SERVICE_USER'..."
    sudo -u "$SERVICE_USER" screen -dmS agent bash -c "cd $PROJ_DIR && .venv/bin/python main.py"
    echo "[run.sh] Done. View logs: sudo -u $SERVICE_USER screen -r agent"
    exit 0
fi

# If running as non-root — run directly
cd "$PROJ_DIR"
exec .venv/bin/python main.py
