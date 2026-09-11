# Changelog

User-visible changes to Codex Peer Bridge are recorded here. Unreleased entries move
into a dated release section when promoted to `main`.

## Unreleased

## 2026-09-11 — Codex-wide registration

### Added

- Codex-wide installation with a managed global instruction section that preserves
  existing guidance and supports `AGENTS.override.md` precedence.
- Per-thread registration with isolated inboxes, checkpoints, and service instances.
- A supervisor for bridge/notifier lifecycle and a persistent-process fallback when
  no systemd user manager is available.
- Live peer discovery through `bridge.py peers`, exposing only selected registry metadata.
- Explicit session status, stop, and rename commands.
- Tests for concurrent sessions, repeated registration, global instruction preservation,
  partial-service shutdown, and safe name allocation.

### Changed

- Peer names follow the fleet form `codex-<repo>-<two-hex>`. Full thread identity stays
  internal; names remain stable unless explicitly renamed.
- Registration verifies both the bridge and notifier before reporting a healthy session.
- Uninstallation stops the installed sessions and removes owned services and managed
  guidance while retaining inbox data.

### Fixed

- Failed or interrupted renames cannot truncate the previous session registration.
- Short-name collisions within an installation select another suffix rather than
  silently creating an ambiguous name.
- Manual sessions can stop even when an inactive unit file exists or systemd is unavailable.
- Notifier readiness is independent of the invoking shell's Claude configuration directory.

## 2026-09-11 — Initial release

### Added

- Linux Unix-domain socket bridge for Claude Code peer messages, with a persistent
  SQLite inbox and a private local control interface.
- Codex queue notifications, named Claude peer registration, and same-user socket checks.
- User-level installation/removal scripts, systemd service setup, and protocol documentation.
- MIT license, personal repository contribution conventions, agent setup guidance,
  signed contributions, and protected `develop`/`main` pull-request flow.
- Python 3.11–3.13 continuous integration.

### Fixed

- Python 3.13 test cleanup tolerates sockets already removed by asyncio.
- Installer upgrades abort on service-stop errors and refuse unrelated service files.
