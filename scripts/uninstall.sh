#!/usr/bin/env bash
# Remove the default per-user installation. Preserve inbox data and checkpoints.
set -euo pipefail
systemctl --user disable --now codex-peer-notify.service codex-peer-bridge.service
rm -f "$HOME/.config/systemd/user/codex-peer-notify.service" \
      "$HOME/.config/systemd/user/codex-peer-bridge.service"
systemctl --user daemon-reload
for file in bridge.py notify.py README.md PROTOCOL.md LICENSE CONTRIBUTING.md AGENTS.md docs/INSTALL.md; do
  rm -f "$HOME/.local/share/codex-peer-bridge/$file"
done
printf '%s\n' 'Services and default installation removed; inbox state is preserved.'
