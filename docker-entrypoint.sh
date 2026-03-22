#!/bin/bash
set -e

SERVICE_USER="bot"

# Create service user if needed
if ! id "$SERVICE_USER" &>/dev/null; then
    useradd -m -s /bin/bash "$SERVICE_USER"
fi

SERVICE_HOME="$(eval echo ~$SERVICE_USER)"

# Copy Claude credentials (mounted read-only at /run/claude-creds)
CREDS_SRC="/run/claude-creds/.credentials.json"
if [ -f "$CREDS_SRC" ]; then
    mkdir -p "$SERVICE_HOME/.claude"
    cp "$CREDS_SRC" "$SERVICE_HOME/.claude/.credentials.json"
    chown -R "$SERVICE_USER":"$SERVICE_USER" "$SERVICE_HOME/.claude"
    chmod 600 "$SERVICE_HOME/.claude/.credentials.json"
fi

# Ensure data directories exist
install -d -o "$SERVICE_USER" -g "$SERVICE_USER" \
    /app/data/sessions \
    /app/data/logs \
    /app/data/media \
    /app/data/reports \
    /app/data/analyst_workdir \
    /app/data/classifier_workdir \
    /app/data/skills \
    /tmp/mem0-claude-code

# Fix data ownership
chown -R "$SERVICE_USER":"$SERVICE_USER" /app/data

# Install docx npm package if missing (data/ is bind-mounted, not in image)
if [ -f "/app/data/analyst_workdir/package.json" ] && [ ! -d "/app/data/analyst_workdir/node_modules/docx" ]; then
    (cd /app/data/analyst_workdir && npm install --silent 2>/dev/null)
    chown -R "$SERVICE_USER":"$SERVICE_USER" /app/data/analyst_workdir
fi

# Run as service user
exec gosu "$SERVICE_USER" /app/.venv/bin/python main.py
