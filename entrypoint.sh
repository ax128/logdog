#!/bin/sh
set -e

# Pre-populate known_hosts for SSH-based Docker hosts so that
# the Docker SDK's ssh shell-out does not fail on first connect.
mkdir -p /root/.ssh
chmod 700 /root/.ssh

# Scan all SSH host URLs from config (extract host:port from ssh:// URLs)
if [ -f /app/config/logdog.yaml ]; then
    grep -oP 'ssh://[^@]+@\K[^/\s"]+' /app/config/logdog.yaml 2>/dev/null | while read -r hostport; do
        host="${hostport%%:*}"
        port="${hostport##*:}"
        if [ "$port" = "$host" ]; then
            port=22
        fi
        echo "ssh-keyscan: scanning $host:$port"
        ssh-keyscan -p "$port" "$host" >> /root/.ssh/known_hosts 2>/dev/null || true
    done
fi

# Copy SSH key with correct permissions (source is read-only mount)
if [ -f /root/.ssh/id_ed25519 ]; then
    cp /root/.ssh/id_ed25519 /tmp/_ssh_key
    chmod 600 /tmp/_ssh_key
fi

# Write SSH config to disable strict host checking for Docker hosts
cat > /root/.ssh/config <<'SSHEOF'
Host *
    StrictHostKeyChecking accept-new
    IdentityFile /tmp/_ssh_key
    ServerAliveInterval 30
    ServerAliveCountMax 3
SSHEOF
chmod 600 /root/.ssh/config

exec "$@"
