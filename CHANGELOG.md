# Changelog

User-visible changes to Codex Peer Bridge are recorded here. Unreleased entries move
into a dated release section when promoted to `main`.

## Unreleased

## 2026-09-18 — Repository memory and session readiness

### Added

- `memory.py`, a shared per-repository memory service, in its pull-only form. Agents working
  in one repository append typed entries and read them back through a private control socket,
  so a session that started earlier can still learn what a later session recorded. Repository
  identity is the absolute Git common directory, so every worktree of one repository shares one
  store. Liveness is recorded on the entry it affects rather than derived, so reclaiming a
  replacement cannot resurrect what it replaced. A snapshot is frozen as immutable copies against
  a fixed head, so a revocation or a reclamation cannot change what a reader is still paging
  through. The server records page issuance and completion, so a cursor advances only on an
  acknowledgement it actually issued, and a retained acknowledgement replays after a lost
  response. Responses are bounded by encoded bytes with continuation. Storage enforces a logical
  budget and a durable page ceiling, with slots and pages reserved so a withdrawal stays
  recordable, and every retained record has a lifetime whose expiry returns a defined recovery
  result. The store is opened by a single exclusive owner; a second owner is told the store is
  busy rather than that the file is unreadable. When storage cannot be written safely the
  service records a blocked state and refuses writes until recovery is requested explicitly,
  while status, search and stop stay available. Expiry removes entries in batches, and falls
  back to invalidating the index rather than requiring room to maintain it, so a full store can
  always be reclaimed. A search index that cannot be rebuilt leaves the store serving complete
  scans instead of failing to open, and an index the store cannot maintain is marked invalid
  rather than left silently short. Initialisation writes the schema and the identity that
  describes it in one transaction, so an interrupted first start leaves nothing half-made, and a
  store left in that state by an earlier version completes rather than being reported as another
  repository's. Recovery is available as a `recover` subcommand. Search answers from the index only while the
  index is known to cover every live entry, and otherwise from a complete scan, and the reply
  says which answered. Start is
  serialized, and a socket left by an unclean exit is recovered only after its recorded owner is
  proved dead. Entries are reported data and grant no authority. There is no bus integration and
  no compaction in this form.
- `docs/STORAGE-BOUND-DERIVATION.md`, the derivation of the storage bound the memory service
  enforces, with its terms traced to the SQLite sources at a pinned tag. It records why the log
  a single transaction can produce is finite, why the shared-memory and sub-journal files do not
  contribute to the declared total, what the choice costs in memory instead, and which figures
  are an example workload rather than a bound. No runtime behaviour changes with this entry.
- `docs/PARITY-MEMORY-DESIGN.md`, the agreed design and acceptance contract for peer
  capability parity and a shared per-repository memory service. It records the contracts
  for identity, delivery, presence, and memory, the capabilities that remain unverified
  until they are measured, and the acceptance criteria that judge completion. No runtime
  behaviour changes with this entry.

### Fixed

- Session registration releases its registration lock before starting the systemd
  supervisor. This prevents a false readiness timeout. Concurrent lifecycle commands
  for the same session remain serialized.

### Changed

- Git ignores local `_handoff/` directories to keep handoff content out of commits.

## 2026-09-11 — macOS, DeepSeek, and peer guidance support

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
