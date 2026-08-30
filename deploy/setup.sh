#!/usr/bin/env bash
# One-shot provisioning for a fresh Ubuntu 24.04 VM (Oracle free-tier ARM or
# anything comparable). Run as root:  sudo bash deploy/setup.sh
# Idempotent — safe to re-run.
set -euo pipefail

REPO="${EXCHANGE_REPO:-https://github.com/rvsiyad/exchange.git}"
DIR=/opt/exchange

apt-get update
apt-get install -y openjdk-21-jdk-headless git curl docker.io docker-compose-v2
systemctl enable --now docker

# Dedicated system user; needs docker for the infra containers.
id -u exchange >/dev/null 2>&1 || useradd --system --create-home --shell /usr/sbin/nologin exchange
usermod -aG docker exchange

[ -d "$DIR/.git" ] || git clone "$REPO" "$DIR"
chown -R exchange:exchange "$DIR"
cd "$DIR"

# Infra first (restart: unless-stopped keeps it across reboots), then the jars.
sudo -u exchange docker compose up -d redpanda redpanda-init tigerbeetle prometheus grafana
sudo -u exchange ./mvnw -q -DskipTests package

install -m 644 deploy/systemd/*.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now exchange-engine exchange-settlement exchange-gateway exchange-market-data

# Oracle's Ubuntu images reject all inbound traffic but SSH at the host
# layer, on top of the cloud security list. Open the venue's public ports
# there too; elsewhere these are no-ops ahead of an accept-all policy.
for port in 8090 8092 3001; do
  iptables -C INPUT -p tcp --dport "$port" -m state --state NEW -j ACCEPT 2>/dev/null \
    || iptables -I INPUT -p tcp --dport "$port" -m state --state NEW -j ACCEPT
done
if command -v netfilter-persistent >/dev/null; then netfilter-persistent save; fi

# CD: let the deploy key run exactly one command as root, nothing else.
echo 'ubuntu ALL=(root) NOPASSWD: /opt/exchange/deploy/redeploy.sh' > /etc/sudoers.d/exchange-deploy
chmod 440 /etc/sudoers.d/exchange-deploy

sleep 5
curl -fsS localhost:8091/health >/dev/null && echo "gateway healthy"
echo "exchange is up — UI on :8090, WebSocket feed on :8092, Grafana on :3001 (open those ports in the cloud firewall)"
