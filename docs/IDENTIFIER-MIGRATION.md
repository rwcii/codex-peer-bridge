# Koinon identifier migration

This source change gives new installations Koinon names. It preserves existing
installation paths, state, participant targets and registered peer names. It does
not rename a checkout or deploy a runtime.

## Selection and preservation

Resolve the prefix first: an explicit prefix wins; otherwise, check both prefix
names under `~/.local/share`. Read `install.json` only from the selected prefix.
A both-present prefix conflict refuses before reading configuration or creating
files. Missing configuration is valid for a historical explicit-thread install;
malformed, symlinked or unreadable configuration refuses without fallback.

Explicit CLI paths take precedence. For an existing configured installation,
omitted state and unit paths come from its saved configuration. Otherwise, default
selection checks both the Koinon and legacy locations before creating anything.
A legacy location is reused when it is the only existing location. If both names
exist, selection refuses with a named error and requires an explicit path.
A retained symlink or unreadable location is evidence, not an absent installation.
A default that is a symlink or a non-directory refuses and requires an explicit
path. Default selection reports the selected path and reason on stderr, including
for status commands. JSON replies remain on stdout.
No default selector creates directories or moves data.

Fresh prefixes use `~/.local/share/koinon`. Fresh state uses `koinon` under the
configured state base (`XDG_STATE_HOME`, or `~/.local/state`). The legacy basename
is `codex-peer-bridge`. Existing configured custom paths remain authoritative.
Neither an inbox nor a notification or memory cursor is copied, reset or recreated
as a naming step. The notifier migration protocol remains separate.

New service units use `koinon-bridge.service`, `koinon-notify.service`, or
`koinon-session-INSTANCE.service`. Existing owned legacy unit filenames remain
usable at the selected unit directory. An old/new family conflict refuses before
modification. Unit ownership checks recognize both managed markers and preserve
unrelated units. A file with a recognized marker is not authority to replace a
unit that belongs to a different installation prefix. Uninstall recognizes both
unit families and both managed markers. It removes only units owned by the
selected prefix, while preserving other installations, state and lock inodes.

## Managed participant guidance

The implementation moves to `participant_instructions.py`. The old import name
remains a compatibility shim to the same implementation. There must not be two
independent update paths.

New managed sections use Koinon delimiters. Updates recognize the existing Codex
and DeepSeek delimiter pairs and replace one valid section in place. A malformed,
duplicate or overlapping managed section causes a refusal before file changes.
Text outside the selected participant's section is preserved. Codex override-file
precedence and independent participant removal remain supported.

The updater retains its old lock filenames. Old and new updaters must contend on
the same inode. No migration deletes or renames a lock file. Source tests use
synthetic instruction files; this work does not modify live agent guidance.

## Compatibility constants

These identifiers remain unchanged:

- Claude registry entrypoint: `codex-peer-bridge`.
- Memory handshake service: `codex-peer-memory`.
- Account-local participant lock namespace: `koinon-locks`.
- Existing participant guidance lock filenames.

The first two cross a boundary with an external client or an older running service.
A product rename is not evidence of interoperability with different wire values.
The account-local lock namespace is already Koinon-named. Claude socket directories,
literal peer addresses and key derivation are outside this migration.

## Required evidence

Tests must cover fresh and repeated installation, legacy default detection,
explicit and saved-path precedence, conflicting old/new paths and units, both
ownership-marker families, both participant section migrations and removals,
malformed-section preservation, and uninstall recognition of both unit families
with preservation of state and lock inodes.
Tests must also pin the unchanged compatibility constants. Review and Linux/macOS
CI must use the exact frozen source tree.
