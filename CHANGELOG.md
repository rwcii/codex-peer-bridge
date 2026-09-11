# Changelog

User-visible changes to Codex Peer Bridge are recorded here. Unreleased entries move
into a dated release section when promoted to `main`.

## Unreleased

### Added

- Generic peer-origin and permission-laundering guidance on inbox records, queued
  notifications, and managed session instructions, separate from peer message content.
- Updated inbox CLI adds guidance when reading from an older running bridge, allowing
  current sessions to receive it without a server restart.
- macOS support. Peer credentials, the process start marker, the peer domain, the socket
  allowlist, and the AF_UNIX address-length fallback are resolved in one platform module.
- A DeepSeek (DSH) participant alongside Codex. It reads and acknowledges the same inbox
  and sends through the same control socket, and the watcher delivers notices to a selected
  harness session over the harness's local RPC, the analogue of `codex queue`.
- `--agent` and `--model` on `session.py`. A DeepSeek peer advertises the harness's
  configured default model in its peer name, such as `deepseek-v4-pro-<repo>-a3`.
- `install.py --configure-deepseek`, which writes a managed DeepSeek section into the
  harness home's `AGENTS.md` so a harness session registers itself and reads its inbox,
  the same way the Codex section already does. Each participant gets its own delimited
  section with its own markers, so either can be added or removed without disturbing the
  other or the user's own guidance. Uninstallation removes exactly the sections the
  installation recorded, so a DeepSeek section is not left behind pointing at a removed
  runtime.

### Fixed

- Repository setup requires the six OS/Python matrix checks, replacing obsolete
  Python-only names that left pull requests waiting for nonexistent jobs.

- Control socket paths use filesystem byte lengths, so Unicode state paths also
  select the short fallback before exceeding the kernel limit.

- Repeated participant configuration preserves registered participants and home paths,
  so uninstall removes all managed guidance, including after a Codex-only upgrade.

- Harness notice delivery refuses redirects and ignores environment proxies to keep
  authentication cookies on the validated loopback destination.

- `peers()` returned no peers on macOS. A missing `/proc/<pid>/stat` raised inside a broad
  handler, so discovery reported an empty list even with live peers present; the same
  omission made a notifier fail at startup and left `session.py status` permanently
  reporting `repair_required`.
- The bridge could not start when a state directory was too deep for `sockaddr_un`, which
  macOS's long temporary paths reach easily. The control socket now falls back to a short
  path in the peer socket directory, which also fixes over-long Linux state paths.
- A peer connection that raised `AttributeError` was dropped without a trace; the handler
  now reports it.

### Changed

- Notice delivery is dispatched per participant. The Codex path, including generated
  systemd units and the manual start command, is unchanged.
- CI runs on Linux and macOS.

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
