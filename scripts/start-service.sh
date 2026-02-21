#!/bin/bash
# Startup script for claude-hub service

# Source environment from ~/.sprite-config (single source of truth)
if [ -f "$HOME/.sprite-config" ]; then
    set -a
    source "$HOME/.sprite-config"
    set +a
fi

# Set defaults if not already set
export CLAUDE_WORK_DIR="${CLAUDE_WORK_DIR:-$HOME}"
export CLAUDE_PROJECTS_DIR="${CLAUDE_PROJECTS_DIR:-}"  # Computed from CLAUDE_WORK_DIR if empty
export STATE_FILE="${STATE_FILE:-$HOME/.claude-hub/state.json}"
export LOG_LEVEL="${LOG_LEVEL:-info}"
export PORT="${PORT:-9090}"

# Ensure data directory exists
mkdir -p "$HOME/.claude-hub/data"

# Run the service
exec "$HOME/.claude-hub/bin/claude-hub"
