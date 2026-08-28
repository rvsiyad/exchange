#!/usr/bin/env bash
# What CD runs on every merge to main: pull, rebuild, restart, verify.
# Runs as root (via the sudoers entry setup.sh installs); build steps drop to
# the exchange user.
set -euo pipefail
cd /opt/exchange

sudo -u exchange git fetch origin main
sudo -u exchange git reset --hard origin/main
sudo -u exchange docker compose up -d redpanda redpanda-init tigerbeetle prometheus grafana
sudo -u exchange ./mvnw -q -DskipTests package

systemctl restart exchange-engine exchange-settlement exchange-gateway exchange-market-data

sleep 5
curl -fsS localhost:8091/health >/dev/null
curl -fsS localhost:8090/ >/dev/null
echo "deployed $(git rev-parse --short HEAD)"
