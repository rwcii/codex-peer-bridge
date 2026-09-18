# Install and enable Koinon

## Name and path compatibility

Koinon was previously named Codex Peer Bridge. The project name and clone URL have
changed. The installed runtime still uses `codex-peer-bridge` paths, existing
`codex-peer-*` service names, and the `codex-peer-bridge` registry entrypoint. Commands
below deliberately retain those names. No reinstall or restart is needed for this
documentation change.

The systemd ownership marker in `scripts/install.py` and the managed guidance markers
in `codex_instructions.py` are also unchanged. Do not replace them by hand. A future
migration must recognize existing markers so that upgrades and removal still find the
owned units and managed sections.

Existing local checkouts and worktrees can keep their directory names. Moving a local
checkout is a separate operation: worktree links, repository identity, peer registration,
and agent memory keyed by the working-directory path can depend on those paths. Do not
move state or reset inbox and memory cursors as part of a documentation rename.

## Requirements

Linux or macOS, Python 3.11+, Claude Code with local peer messaging, and — for a Codex
participant — a Codex CLI that supports `codex queue --thread ... --message ...`. Agents
must run under the same OS user. Verify `codex queue --help` and `python3 --version`.

A DeepSeek (DSH) participant needs no Codex CLI. It needs the running harness, which
exports `DSH_HOME`, `DSH_SESSION_ID` and `DSH_WEB_URL` to a session's shell.

On macOS the system `python3` is often 3.9, which is below the floor; use a 3.11+
interpreter explicitly, for example `python3.12`.

The bridge targets an existing conversation. A standalone API key or unrelated
Codex daemon does not provide access to that conversation. Hosted clients without
local shell/queue access are not automatically supported.

## macOS

There is no systemd on macOS, so the service path is unavailable. Install the runtime
and managed guidance, then start each session's supervisor in a persistent session:

```sh
python3.12 scripts/install.py --configure-codex --no-start
python3.12 session.py ensure                    # in a Codex session: uses CODEX_THREAD_ID
python3.12 session.py ensure --agent deepseek   # in a harness session: uses DSH_SESSION_ID
```

`ensure` reports `manual_required` with an exact `start_command` on macOS. Run that
command in a persistent terminal or managed tool session and keep it alive while using
the bridge. `install.py` refuses the systemd path on macOS rather than writing units that
nothing would load.

### Recovering from a killed instance

A start binds exclusively and never removes a socket it did not create, so a bridge killed
with `SIGKILL` leaves its socket behind and blocks the next start. The failure names the
path in a JSON diagnostic on stdout and exits with status 78. For example
(the operating-system error number can differ):

```json
{"ok": false, "code": "endpoint_unavailable", "error": "cannot bind /tmp/cc-socks/<hash>-control.sock: [Errno 48] Address already in use"}
```

On Linux, installed systemd bridge and session services prevent automatic restart
on exit 70 (internal software error) or 78 (configuration refusal). On macOS,
these errors end the manually started process; after
correcting the path, start it again with the command described in the
[macOS setup](#macos). The macOS leftover-socket note at the end of this recovery
section also applies. An unsafe startup directory also reports this refusal. Correct the named path
before starting the service again; do not bypass its ownership checks.

Proving the owner is gone cannot be done by connecting. A live listener whose accept queue
is full refuses a connection on macOS exactly as a dead owner does, so a refusal is not
evidence. Use `lsof`, which shows the owning process only when one exists:

```sh
lsof /tmp/cc-socks/<hash>-control.sock    # no output means nothing holds it
rm /tmp/cc-socks/<hash>-control.sock      # remove that one path
```

If `lsof` does print a process, the bridge is still running: stop it with `session.py stop`
or `bridge.py stop` rather than deleting the file. Then the same for the peer socket if its
error was reported too.

Remove only the specific path from the error. Never clear `/tmp/cc-socks` or the session
registry wholesale, and never remove a socket `lsof` reports as held.

The `<hash>` name appears when the state directory is too deep for a Unix socket path.
It is the first 16 hex characters of `sha256` of the **resolved** state directory, and it
can also be read directly:

```sh
python3 -c "import bridge,platform_support,pathlib;print(platform_support.control_socket_path(pathlib.Path('<state-dir>')))"
```

For a shorter state directory the control socket sits at `<state-dir>/control.sock` and the
same procedure applies. macOS has no automatic temporary-directory cleanup, so a leftover
socket stays in the way until it is removed by hand.

## Recommended: configure Codex once

```sh
git clone https://github.com/rwcii/koinon.git
cd koinon
python3 scripts/install.py --configure-codex
```

For a DeepSeek (DSH) participant, install the harness guidance instead of, or as well as,
the Codex guidance:

```sh
python3 scripts/install.py --configure-deepseek            # uses $DSH_HOME
python3 scripts/install.py --configure-deepseek --dsh-home /path/to/harness
```

This manages a clearly marked DeepSeek section in `$DSH_HOME/AGENTS.md`, leaving all other
content untouched. Each participant has its own delimited section and markers, so the two
can be installed and removed independently. Repeated configuration retains previously
registered participants and uses their recorded homes when home flags are omitted.
Uninstallation removes every managed section recorded by the installation.

This installs runtime files in `~/.local/share/codex-peer-bridge` and adds a clearly
marked section to `$CODEX_HOME/AGENTS.md` (normally `~/.codex/AGENTS.md`). If a global
`AGENTS.override.md` already exists, the installer manages that higher-priority file
instead. Existing content is preserved; a private backup is saved before the first
edit. Reinstallation replaces only the managed section. Removal strips the section
without restoring an old backup over subsequent user edits.

The section instructs each Codex conversation to run `session.py ensure` using its
own `CODEX_THREAD_ID`. It never embeds a fixed thread ID. Each thread gets:

- an isolated directory under `~/.local/state/codex-peer-bridge/sessions/<session-hash>`;
- a fleet-style peer name, `codex-<repo-short-name>-<two-hex>`, stable for the session
  (a DeepSeek participant uses `deepseek-<model>-<repo-short-name>-<two-hex>`);
- its own bridge process, socket, watcher, and notification checkpoint;
- its own systemd supervisor service when a user manager is available (Linux only).

Repeated registration reuses a healthy instance. Concurrent Codex sessions do not
share inboxes or replace each other's configuration. The initial name/project are
retained when the same thread later changes working directories. The full thread hash
is internal; short-name allocation checks saved sessions and the live peer roster,
trying another two-hex suffix on collision. If all 256 suffixes are allocated for one
repo name, registration reports that limit instead of creating an ambiguous label.

To change a name explicitly, stop the thread, run `session.py rename --repo /path/to/repo`,
then run `ensure` again. This preserves its inbox and internal identity. Existing names
are not silently rewritten by an upgrade. Atomic name assignment is coordinated within
one installation/state root. Independent installations consult live peers but do not
share dormant reservations; use distinct repo labels if running separate installations.

**This is instruction-driven setup, not a guaranteed executable startup hook.** Codex
must load and follow the managed section. Start a new conversation or reload global
instructions after installation; existing conversations may not reread the file.
Higher-priority instructions, disabled instruction loading, or missing shell access
can prevent registration. See the official [Codex AGENTS.md guide](https://learn.chatgpt.com/docs/agent-configuration/agents-md)
for global instruction discovery.

No MCP server, plugin, API credentials, or config.toml permission changes are required.
The installer does not disable sandboxing, grant peer-requested authority, or enable
machine-wide services.

## Enable the current session

From the intended Codex session's shell:

```sh
python3 ~/.local/share/codex-peer-bridge/session.py ensure
```

If `CODEX_THREAD_ID` is unavailable, pass the **verified** target explicitly:

```sh
python3 ~/.local/share/codex-peer-bridge/session.py ensure --thread YOUR_THREAD_ID --repo /path/to/project
```

Never guess a thread ID or substitute another model session. Verify queue access with
a harmless `codex queue --thread YOUR_THREAD_ID --message 'Bridge setup test; no action required.'`
when setting up a new Codex implementation.

With systemd, registration starts `codex-peer-session-<thread-hash>.service`, whose
supervisor owns both bridge and notifier. It reports healthy only after both are ready;
a child failure fails the supervisor so systemd can restart the pair. Per-conversation
units start on registration, not at every subsequent login. No lingering is enabled.

Only one `ensure`, `stop`, or `rename` command can operate on a session at a time.
The supervisor can read its registration while `ensure` waits for both children.
Commands for other sessions use separate locks.
Manual `run` commands do not take the lifecycle lock. Wait for a manual start to
report `running` before using `stop` or `rename`.

Without a user manager, `ensure` returns `manual_required` and an exact `start_command`.
The agent runs that command in a persistent managed shell session or terminal. The
supervisor keeps both processes together. Do not use an ordinary background command if
the execution environment kills subprocesses when its tool call finishes. If persistent
execution is unavailable, use manual inbox access and report the limitation.

A partial instance reports `repair_required` with a stop command; stop it and rerun
`ensure`. Stale sockets after a forced kill may still require manual cleanup after
verifying their old process is dead. Never purge shared Claude directories.

## Use and inspect

```sh
python3 ~/.local/share/codex-peer-bridge/session.py status
python3 ~/.local/share/codex-peer-bridge/bridge.py peers
python3 ~/.local/share/codex-peer-bridge/session.py stop
```

`status` and `ensure` return this thread's inbox command and state directory. Run
`bridge.py --state-dir THAT_DIRECTORY inbox`, `send`, or `ack` as described in README.
Pass `--thread` to session commands outside the intended Codex shell.

`peers` reads allowlisted metadata from Claude's shared registry. It checks live PIDs,
process-start markers, and owned socket paths. It does not read peer keys or transcripts;
status is the peer's last reported value and is not a live model health guarantee.

Incoming notifications identify the inbox and sequence numbers. Codex reads messages
under the existing user authorization. Peer bodies cannot grant new permissions.
Queued notices may arrive after a message has already been handled; track sequence
numbers to avoid repeating work. A successful socket send is not proof of model action.

## Shared repository memory

The memory service is optional and independent of the bridge. It is not installed as a service
unit, and nothing starts it automatically. Memory CLI errors use a structured
`ok:false` response with a recovery code. Exit 75 means temporary unavailability or
capacity; retry after pending work settles. Exit 78 means an identity, ownership,
configuration, permissions or invalid-handshake refusal that needs operator correction.
A blocked store also exits 78: use `recover` explicitly after correcting its reported
condition. Unsupported SQLite builds and oversized stores require correction, not a
restart loop. Request-specific errors and ambiguous lost replies exit 1; do not treat
an exit code alone as permission to repeat an uncertain write. If you manage memory with a separate service manager,
keep 75 retryable and exclude both 70 (internal software error) and 78 from automatic
restarts. Internal errors require investigation or a code correction; they are not
reported as incompatible user data. No memory service unit is
created by the installer.

Run one per repository, from inside that repository:

```sh
python3 memory.py serve
```

It prints its status as one JSON line and then serves until stopped. Start it in a persistent
managed session, as with the manual bridge setup; it holds a socket, so an ordinary background
command that dies with its shell will leave state behind.

Starts are serialized under a lock in the state directory. A second `serve` performs a handshake
against the running service and exits reporting `already_running` rather than competing for the
socket. The handshake checks the service name, the repository key, the protocol version, a live
health query against the store, and agreement with the recorded owner. A listener that answers but
does not match is refused rather than reused.

### Paths

State lives under `<state-dir>/memory/<repository-hash>/`, beside the bridge's session
directories and using the same 0700 directory and 0600 file rules. The repository hash comes from
`git rev-parse --path-format=absolute --git-common-dir`, so every worktree of one repository maps
to one directory. That directory holds `memory.sqlite3`, `owner.json`, `start.lock`, and the
control socket, unless the path would exceed the kernel's `sun_path` limit, in which case the
socket falls back to the shared peer socket directory exactly as the bridge's does.

Use `--state-dir` and `--repo-path` to run an isolated instance for a preview or a test. Give a
distinct state root when you intend an independent installation.

### Stopping and recovery from a killed instance

Stop the service with `python3 memory.py stop`, or with SIGTERM or SIGINT. A clean stop drains
requests already in flight, closes the database, and removes only the socket and ownership record
it owns.

A killed service leaves its socket and its ownership record behind. The next `serve` recovers
automatically, but only after proving the previous owner is gone: it compares the recorded process
ID and process start marker against the live system. If that process is still running, or if its
state cannot be read, the socket is left in place and the start is refused with `socket_in_use`.
Nothing removes a socket because a connection was refused, since a live listener with a full
accept queue and a dead owner are indistinguishable by probing.

If you must clear state by hand, verify the recorded owner in `owner.json` is dead first, and
remove only files inside that repository's own directory. Never clear the shared socket directory.

### Upgrades

Stop the repository's memory service before replacing runtime code, then start it again. The
database carries its schema version and refuses a state directory belonging to another repository.
An existing store opened by a runtime that provides full-text search when the previous one did not
backfills its index on first open; no manual step is needed. Do not run memory commands from an
older runtime during an upgrade.

## Paths and options

`--prefix`, `--state-dir`, `--unit-dir`, `--codex-home`, and an absolute `--codex` path
support customized installations. In Codex-wide mode, `--state-dir` is the root for
all per-thread directories. Installed `install.json` records these private local paths;
do not commit it. `--no-start` with `--configure-codex` installs files/guidance without
registering a thread. It still edits the selected Codex instructions; use temporary
paths for a dry-run preview.

The pre-existing explicit single-thread mode remains available:

```sh
python3 scripts/install.py --thread YOUR_THREAD_ID --name codex-project --repo /path/to/project
```

That legacy mode manages the fixed `codex-peer-bridge`/`codex-peer-notify` pair and does
not add global guidance. Prefer Codex-wide mode for concurrent sessions. It does not
adopt an already running prototype or legacy inbox automatically; stop or migrate that
instance deliberately to avoid duplicate registrations for one conversation.

## Notifier ownership

The notifier takes its state-directory lock first, then a lock for the provider and
session identity across the OS account. Both acquisitions are nonblocking. A conflict
fails startup before registration or delivery, with `participant_in_use`, the provider,
the `account-local` scope, and the lock digest. Match that digest to `participant_lock`
in `session.py status` or the running notifier's private `notify-ready.json`. Under systemd, read the refusal JSON with
`journalctl --user -u codex-peer-notify -n 50 --no-pager` for the legacy pair, or
`journalctl --user -u codex-peer-session-INSTANCE -n 50 --no-pager` for a session.
Ownership refusals exit with status 78. Both service forms prevent automatic restart
on that status; the supervisor preserves it after stopping its bridge child. Correct
the reported condition before explicitly starting the instance again. Stop an
unwanted instance through its own session command; do not remove a lock file to bypass it.

Participant identity compares exact UTF-8 bytes; callers must supply the provider's
canonical session ID. Different spellings are not normalized into one identity. Real
and effective user IDs must match; set-user-ID execution is refused as `uid_mismatch`.

The shared namespace is derived from the effective user's operating-system account
entry, not `HOME`, `XDG_STATE_HOME`, `CODEX_HOME`, `DSH_HOME`, or the delivery URL:

- Linux: `<account-home>/.local/state/koinon-locks`.
- macOS: `<account-home>/Library/Application Support/koinon-locks`.

An unavailable account home produces `account_home_unavailable`; startup does not fall
back to a different namespace. A state directory equal to or below the resolved lock
namespace is refused, including symlink aliases. An ancestor state directory is allowed;
removal must preserve the shared namespace and must not recursively purge that state root.

Lock files persist after exit and uninstall. Each distinct historical provider/session
identity adds a zero-length file; inode and directory-entry use is not bounded by a
journal or memory-store budget. Never unlink these files while notifiers may use them.
Descriptors are close-on-exec, so a launched delivery command cannot retain the lock
once the notifier exits. Unrelated tools that do not take this lock are not coordinated.

When upgrading from a runtime without participant locks, stop and upgrade all Koinon
notifiers that could target the same participant before restarting them. An older
notifier in another state directory does not hold the new lock and cannot be excluded
by it. Preserve inboxes, checkpoints, registration targets and unrelated bridge instances;
do not treat installing files with `--no-start` as activating the new exclusion rule.

## Upgrades and removal

Stop this installation's registered sessions before upgrading runtime code, then rerun
`--configure-codex` with the same paths. Existing state and instructions are preserved;
rerun `ensure` in active conversations afterward. Configure a distinct state root when
you intend an independent installation. Never silently reset a checkpoint.
Do not run session commands from an older runtime during an upgrade. Older commands
do not use the lifecycle lock that protects session startup.

For the peer-message guidance update, an operator may stage the compatible runtime
files and replace each file atomically, installing `peer_guidance.py` before its
importers, without stopping existing sessions. Preserve `install.json`, all state,
units, and unrelated global instructions. The updated inbox CLI adds guidance even
when connected to an older server. Running notifiers retain their loaded wording
until their sessions restart normally; existing conversations may also need to reload
managed instructions. This staged procedure is specific to this compatible update,
not a general guarantee for future runtime or schema changes.

```sh
scripts/uninstall.sh
# For a custom installed runtime:
python3 scripts/uninstall.py --prefix /path/to/installed/runtime
```

Removal stops this installation's registered sessions, removes its owned units, strips
managed global guidance, and deletes runtime files. Inbox state and instruction backups
remain. Stop/ownership errors abort removal instead of deleting files under a running
service. Unmarked units from pre-release experiments are refused; inspect and remove
only confirmed bridge units before migration.

## Troubleshooting

- **No registration:** confirm the managed section is in the global file Codex actually
  loads, restart/reload the conversation, then run `session.py ensure` explicitly.
- **No notification:** check the exact target with a direct queue test and inspect both
  bridge and notifier health, not just socket existence.
- **Service failed:** `journalctl --user -u codex-peer-session-INSTANCE -n 50 --no-pager`.
  The instance suffix is the state-directory hash returned by `ensure`.
- **Name missing in Claude:** refresh its listing; registry `messagingSocketPath` is a
  bare filesystem path, while message addresses use `uds:`.
- **Custom Claude home:** set `CLAUDE_CONFIG_DIR` consistently for the supervisor. For
  systemd use a service override with `Environment=CLAUDE_CONFIG_DIR=/your/path`.
- **Inbox full:** read and acknowledge handled entries; the limit is 1,000 records.

## Inbox schema upgrade

Inbox schema 3 migrates legacy and schema-2 inboxes on bridge startup after exclusive socket reservation. Stop the old
bridge and notifier together, install the new runtime at the existing target and paths,
and restart both. Apply the same sequence to manual macOS processes. An installation
with `--no-start` does not upgrade running processes. Preserve the inbox and legacy
notification checkpoint; do not reset either to activate the schema.

Do not run an older bridge against an upgraded inbox. Schema-2 runtimes refuse
schema 3; earlier runtimes do not maintain the acknowledgement watermark. Restore a consistent pre-upgrade backup for a
rollback instead of mixing runtime and metadata versions. The schema change preserves
ordinary-reader compatibility but does not itself implement a notifier journal or
upgrade an old notifier to the new delivery protocol. Local change subscriptions are available after both service runtimes are restarted,
but the existing notifier does not yet use them. Explicit bindings require memory
schema 4; restart each optional memory service with the new runtime before binding. The explicit bridge activation controls store evidence only; they are
not a substitute for the planned stopped-notifier rebuild procedure.

Migration and acknowledgements use SQLite transactions. Process-termination tests
verify rollback and retry; they do not establish power-loss durability on every
filesystem. Keep the existing state and checkpoint backups when upgrading.

Inbox startup storage failures, including a lock that outlasts SQLite's finite
busy timeout, exit 78. This deliberately requires inspection and an explicit restart;
it does not classify every SQLite operational error as retryable. Do not assume
that a storage failure means the migration or a prior mutation was lost.
The supervisor preserves bridge exit codes 70 and 78 through each startup phase
and its running loop. All installer-managed units exclude both permanent statuses
70 and 78 from automatic restart, while 75 remains retryable.


## Explicit memory bindings

Memory remains optional and is not started by the installer. After starting the
repository's memory service, use its exact state directory to bind it:

```sh
python3 bridge.py --state-dir /private/bridge-state bind-memory \
  --repo-path /path/to/repository --memory-state-dir /private/memory-state
python3 bridge.py --state-dir /private/bridge-state memory-bindings
python3 bridge.py --state-dir /private/bridge-state refresh-memory BINDING_KEY
python3 bridge.py --state-dir /private/bridge-state ack-binding-health BINDING_KEY
python3 bridge.py --state-dir /private/bridge-state unbind-memory BINDING_KEY
```

Binding verifies the live service but does not create a pointer until refresh.
The legacy notifier does not deliver memory-pointer notices. Read memory with its
existing sync commands; binding controls do not acknowledge or import memory.

Memory schema 3 upgrades to schema 4 in a transaction that adds a durable store UUID
and changes the schema version together. Records, snapshots and consumer cursors
are retained. A schema-4 store with missing or invalid identity is refused, never
silently assigned a replacement identity. Older runtimes refuse schema 4. Preserve
consistent backups before upgrade; rollback means restoring a compatible backup,
not changing a schema number. An inbox upgrade does not restart memory for you.
If `memory_upgrade_required` is returned, stop that memory service and start it
with the new runtime. Missing or unhealthy memory leaves existing bridge state intact.


### Upgrading a state directory reached through an alias

New private control sockets use the resolved state path when checking the Unix
socket path-length limit. Roots without aliases keep their endpoint. A short
alias to a long directory could have selected a direct socket in an older release;
a long alias to a short directory could have selected the hashed fallback instead.
Clients retain both legacy routes; new startup refuses a distinct retained legacy
endpoint before creating a second listener.

Stop the old service before restarting it with the new code. The new bridge
client can reach the old endpoint when given the original configured state path.
Memory clients can also recover its exact old path from the validated owner
record. An ownerless bridge cannot reconstruct an unknown alias from a long
canonical path: use its original configured `--state-dir`, or stop its verified
process through the service manager. Do not remove a socket while its owner is
alive. If both old and new control endpoints exist, clients refuse the ambiguity;
inspect their owners before proceeding. No inbox or memory state is reset.
