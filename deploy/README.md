# Deploying

The venue runs on one small VM: infra (Redpanda, TigerBeetle, Prometheus,
Grafana) in Docker via the repo's compose file, the four JVM services as
systemd units running the packaged jars. Oracle Cloud's Always Free ARM
shape (VM.Standard.A1.Flex, up to 4 OCPU / 24 GB) fits comfortably; any
Ubuntu 24.04 box works.

## One-time setup

1. **Provision the VM** — Ubuntu 24.04, 2+ OCPU / 8+ GB recommended. In the
   cloud firewall / security list, open ingress TCP **8090** (UI + WebSocket)
   and optionally **3001** (Grafana). Leave everything else closed — the
   gateway (8091) is only reached through the market-data proxy.
2. **Run the setup script** on the VM:

   ```
   sudo bash -c "$(curl -fsSL https://raw.githubusercontent.com/rvsiyad/exchange/main/deploy/setup.sh)"
   ```

   or clone the repo and `sudo bash deploy/setup.sh`. It installs JDK 21 +
   Docker, creates the `exchange` system user, clones to `/opt/exchange`,
   brings up the infra containers (`restart: unless-stopped`, so they survive
   reboots), packages the jars, and installs + starts the four
   `exchange-*.service` units.
3. **Check**: `http://<vm>:8090` shows the UI trading against the real
   pipeline; `systemctl status 'exchange-*'` shows the services;
   `http://<vm>:3001/d/exchange` shows the dashboard.

## Continuous deployment

`.github/workflows/deploy.yml` redeploys on every merge to `main` once the
repository is told where to deploy:

- **Repository variable** `DEPLOY_ENABLED` = `true` (Settings → Secrets and
  variables → Actions → Variables). Until it exists the job skips — CI stays
  green with no target configured.
- **Secrets**: `DEPLOY_HOST` (the VM's IP), `DEPLOY_USER` (e.g. `ubuntu`),
  `DEPLOY_SSH_KEY` (a private key whose public half is in the VM user's
  `~/.ssh/authorized_keys`; generate a dedicated pair with
  `ssh-keygen -t ed25519 -f deploy_key -N ""`).

The job SSHes in and runs `sudo /opt/exchange/deploy/redeploy.sh` — the
sudoers entry installed by setup.sh allows exactly that one command — which
pulls `main`, rebuilds the jars, restarts the units, and fails the workflow
if the gateway or UI doesn't come back healthy.

## Operations

```
journalctl -u exchange-gateway -f          # logs
systemctl restart 'exchange-*'             # bounce the services
docker compose -f /opt/exchange/docker-compose.yml ps   # infra state
sudo /opt/exchange/deploy/redeploy.sh      # manual redeploy
```

The engine's snapshots live in `/opt/exchange/snapshots`; the event log and
ledger live in the `redpanda-data` / `tigerbeetle-data` Docker volumes.
Killing any service (or the whole box) and restarting is the session-3
recovery demo, not an incident: snapshot + replay rebuilds the book, and
settlement replays the fills stream into TigerBeetle's deterministic
transfer ids as no-ops.
