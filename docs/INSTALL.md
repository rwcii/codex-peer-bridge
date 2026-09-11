# Install and enable in Codex

## Requirements

Linux, Python 3.11+, Claude Code with local peer messaging, and a Codex CLI that
supports `codex queue --thread ... --message ...`. Agents must run under the same
OS user. Verify `codex queue --help` and `python3 --version`.

The bridge targets an existing Codex conversation. A standalone API key or unrelated
Codex daemon does not provide access to that conversation. Hosted clients without
local shell/queue access are not automatically supported.

## Recommended: configure Codex once

```sh
git clone https://github.com/rwcii/codex-peer-bridge.git
cd codex-peer-bridge
python3 scripts/install.py --configure-codex
```

This installs runtime files in `~/.local/share/codex-peer-bridge` and adds a clearly
marked section to `$CODEX_HOME/AGENTS.md` (normally `~/.codex/AGENTS.md`). If a global
`AGENTS.override.md` already exists, the installer manages that higher-priority file
instead. Existing content is preserved; a private backup is saved before the first
edit. Reinstallation replaces only the managed section. Removal strips the section
without restoring an old backup over subsequent user edits.

The section instructs each Codex conversation to run `session.py ensure` using its
own `CODEX_THREAD_ID`. It never embeds a fixed thread ID. Each thread gets:

- an isolated directory under `~/.local/state/codex-peer-bridge/sessions/<thread-hash>`;
- a fleet-style peer name, `codex-<repo-short-name>-<two-hex>`, stable for the session;
- its own bridge process, socket, watcher, and notification checkpoint;
- its own systemd supervisor service when a user manager is available.

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
