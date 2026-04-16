#!/bin/sh
set -e

# Pre-populate known_hosts for SSH-based Docker hosts so that
# the Docker SDK's ssh shell-out does not fail on first connect.
mkdir -p /root/.ssh
chmod 700 /root/.ssh

# Collect SSH host list from config (extract host:port from ssh:// URLs)
SSH_HOSTS=""
if [ -f /app/config/logdog.yaml ]; then
    grep -oP 'ssh://[^@]+@\K[^/\s"]+' /app/config/logdog.yaml 2>/dev/null | while read -r hostport; do
        # Skip unexpanded env var references
        case "$hostport" in *'${'*) continue ;; esac
        host="${hostport%%:*}"
        port="${hostport##*:}"
        if [ "$port" = "$host" ]; then
            port=22
        fi
        echo "ssh-keyscan: scanning $host:$port"
        ssh-keyscan -p "$port" "$host" >> /root/.ssh/known_hosts 2>/dev/null || true
    done

    # Extract unique hostnames for SSH config scope
    SSH_HOSTS=$(grep -oP 'ssh://[^@]+@\K[^/\s":]+' /app/config/logdog.yaml 2>/dev/null \
        | grep -v '[$]{' | sort -u | paste -sd ' ' -)
fi

# Copy SSH key with correct permissions (source is read-only mount)
if [ -f /root/.ssh/id_ed25519 ]; then
    cp /root/.ssh/id_ed25519 /tmp/_ssh_key
    chmod 600 /tmp/_ssh_key
fi

# Determine host-key checking policy:
#   SSH_STRICT_HOST_KEY=yes  → always strict (reject unknown hosts)
#   SSH_STRICT_HOST_KEY=no   → accept-new for all hosts (legacy behavior)
#   (default)                → accept-new only for configured Docker hosts
STRICT_MODE="${SSH_STRICT_HOST_KEY:-}"

# Write SSH config — scope relaxed checking to known Docker hosts only
{
    if [ "$STRICT_MODE" = "no" ]; then
        # Legacy: relax for all hosts
        cat <<'BLOCK'
Host *
    StrictHostKeyChecking accept-new
BLOCK
    elif [ "$STRICT_MODE" = "yes" ]; then
        # Strict: never auto-accept
        cat <<'BLOCK'
Host *
    StrictHostKeyChecking yes
BLOCK
    elif [ -n "$SSH_HOSTS" ]; then
        # Default: relax only for configured Docker hosts
        printf "Host %s\n" "${SSH_HOSTS}"
        cat <<'BLOCK'
    StrictHostKeyChecking accept-new
BLOCK
    elif grep -q 'ssh://' /app/config/logdog.yaml 2>/dev/null; then
        # Config has ssh:// URLs but all hostnames are env-var references
        # that we cannot resolve at shell level — fall back to accept-new
        # for all hosts so first connect succeeds.
        cat <<'BLOCK'
Host *
    StrictHostKeyChecking accept-new
BLOCK
    fi

    # Common settings for all hosts
    cat <<'BLOCK'

Host *
    IdentityFile /tmp/_ssh_key
    ServerAliveInterval 30
    ServerAliveCountMax 3
BLOCK
} > /root/.ssh/config
chmod 600 /root/.ssh/config

exec "$@"
