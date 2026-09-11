# Install and enable in Codex

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
path:

```text
OSError: cannot bind /tmp/cc-socks/<hash>-control.sock: [Errno 48] Address already in use
```

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
git clone https://github.com/rwcii/codex-peer-bridge.git
cd codex-peer-bridge
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

## Upgrades and removal

Stop this installation's registered sessions before upgrading runtime code, then rerun
`--configure-codex` with the same paths. Existing state and instructions are preserved;
rerun `ensure` in active conversations afterward. Configure a distinct state root when
you intend an independent installation. Never silently reset a checkpoint.

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
