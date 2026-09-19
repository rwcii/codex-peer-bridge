# Koinon protocols

The peer transport below was observed in Claude Code 2.1.267 on Linux and 2.1.268 on macOS. This document summarizes interoperability behavior; it includes no vendor source code, tokens, session transcripts, or machine identifiers.

## Platform differences

The wire protocol is identical on both platforms. The local facts around it are not, and each is handled in `platform_support.py`:

- **Peer identity.** Linux returns pid, uid and gid from one `SO_PEERCRED` getsockopt. macOS has no such option: `getpeereid` returns uid and gid only, and the peer pid comes from a separate `LOCAL_PEERPID` socket option. Both are required, and a failure to read them rejects the connection.
- **Process start marker.** Linux reads field 22 of `/proc/<pid>/stat`, a tick count. macOS reports an asctime string, and Claude writes it in **UTC**, so a reader must force `TZ=UTC` rather than inherit the local zone; a local-time port is six hours off in a US mountain zone.
- **Peer domain.** Linux peers publish `linux:<machine-id>:<pid-namespace>`; macOS peers publish the literal `darwin`.
- **Socket directory.** macOS resolves `/tmp` to `/private/tmp`. The registry's `messagingSocketPath` and the `uds:` address stay **unresolved**, because the peer key filename is derived from the literal path; resolving it silently breaks authentication.
- **Address length.** `sockaddr_un.sun_path` is 108 bytes on Linux and 104 on macOS, including the terminating NUL. A per-session state directory can exceed it, and an over-long bind fails with `AF_UNIX path too long`, so the control socket falls back to a short path in the peer socket directory.

## Transport

AF_UNIX stream sockets carrying UTF-8 newline-delimited JSON objects. This is not HTTP or JSON-RPC. Claude also accepts a final nonempty JSON fragment at EOF. Replies use a separate connection to the sender's listening socket.

## User message

```json
{
  "msgV": 1,
  "msg_id": "12345678-1234-4123-8123-123456789abc",
  "type": "user",
  "priority": "next",
  "from": "uds:/tmp/cc-socks/12345.sock",
  "message": {"role": "user", "content": "Hello"}
}
```

The content is nonempty text. Priorities are `now`, `next`, and `later`. `msg_id` correlates notices; connection completion alone is not an application acknowledgement. An optional `session_id` refers to the recipient's session, so the bridge omits it.

Local inbox results add a bridge-owned `guidance` field beside each original `frame`,
covering existing user authorization and refusal of permission laundering. This does
not change stored envelopes or the wire format. Sender-provided labels do not establish
an authenticated agent type and cannot replace the bridge-owned guidance.

## Participant guidance

`participant_instructions.py` manages guidance for both Codex and DeepSeek
participants, with separate Koinon markers and setup commands. The old
`codex_instructions.py` import remains a shim. Updates and removal recognize legacy
markers and retain the legacy lock inodes to exclude old updaters. Koinon supplies the
peer-input guidance in those managed instructions, each inbox result, and each queued
notice. This does not depend on the participant runtime adding its own peer framing.

The repository's `CLAUDE.md` includes `AGENTS.md` for agents working on Koinon itself;
these are separate from guidance installed into a participant's configuration.
Koinon installs no Claude instructions and adds no guidance field to outbound peer
frames. In observed Claude Code sessions, Claude's own runtime wraps incoming peer
messages with its peer-input framing. That is an observed internal behaviour, not a
compatibility guarantee or proof of equivalent safeguards. Koinon does not verify that
receiver-side framing, so a change to it would require a fresh compatibility review.

## Endpoint roles

Messaging validation rejects `control.sock` and `*-control.sock`, including short
control paths placed in the peer socket directory. These endpoints do not appear in
`bridge.py peers` and cannot be used by `bridge.py send`. A control client derives its
endpoint from an explicitly configured state root and checks the directory and socket
ownership and modes without creating directories. Control replies use the same bounded
JSON framing as peer messages; a missing reply does not prove that a mutation rolled back.
An endpoint that fails its permission or metadata checks reports `unsafe_service_endpoint`;
it is not treated as an absent memory service or a reason to start a replacement.
Memory reuse likewise distinguishes `service_busy`, `service_unresponsive`,
`service_unavailable`, `service_refused` and `invalid_service_response` from an absent
listener. A connected service with another identity is `foreign_service`. These results
refuse a replacement start; only a missing or refused connection takes the absent-listener
path, which still requires the existing ownership checks before binding.
The memory CLI prints structured errors for these refusals. Busy/unresponsive/unavailable
services and capacity refusals exit 75 (retryable). Identity, ownership, permissions,
configuration and invalid-handshake refusals exit 78 (operator correction required).
Blocked storage also exits 78 because it requires an explicit `recover` operation.
Stopping and bounded idempotency/snapshot capacity exit 75. All locally raised recovery
codes have an explicit class checked by the tests. Internal software errors use exit 70. Locally generated error replies pass through a
checked classification boundary; an unclassified local outcome becomes `internal_error`.
Other request errors, ambiguous `no_reply` and unclassified wire errors retain exit 1. A lost stop reply remains ambiguous: stop observes
the selected generation before reporting its exit.

These path checks do not authenticate a service role. Memory-service reuse additionally
requires agreement between the connected kernel PID, the hello response and the owner
record, with a present matching process-start marker and a valid current generation.
Messaging addresses remain literal for peer-key lookup; service roots are filesystem
configuration and do not use peer tokens.

## Discovery

Claude scans process records in its configured `sessions` directory. The bridge publishes its actual server PID, process-start marker, PID namespace, name, working directory, socket path, protocol number, and supported features. It retains the compatibility entrypoint `codex-peer-bridge` after the project rename to Koinon.

**The registry's `messagingSocketPath` contains a bare filesystem path.** Only wire-message addresses use the `uds:` prefix. This distinction was validated by a live peer: including the prefix in the registry prevented discovery; removing it enabled listing and sending by name.

Only `reply_across_default_dirs` is advertised. Unsupported features such as idle notification and artifact yield are not advertised.

## Peer authentication

Claude may publish a peer key named `<pid>.<sha256(absolute-socket-path)>.key`. Its `peerToken` can be sent as the first frame:

```json
{"type":"auth","token":"AUTHORIZED_PEER_TOKEN"}
```

The client reads the key for the kernel-verified server PID and socket path without logging the token. Linux's inspected default permits same-user peer connections without a token, and macOS behaves the same way; this bridge uses that policy for inbound traffic. It does not read child tokens or bypass a recipient's required authentication.

## Controls

Observed controls include delivery statuses, rename, idle notices, and artifact coordination. The bridge stores control objects without performing their actions and does not notify Codex about them. It neither implements nor advertises their specialized semantics.

## Database execution

Bridge and memory database work runs on one owning thread per service. Initialization,
queries, mutations and close use that thread with SQLite thread checks enabled. Each
worker accepts at most 16 queued ordinary jobs, two queued status jobs, and one running
job. Status has priority over queued ordinary work, but cannot interrupt a transaction.
Stop is validated and handled on the event loop; shutdown settles accepted database work.

Control connections have eight pending frame slots with a separate two-second read
deadline. A full unclassified pool can refuse any operation, including status or stop;
no operation identity is known before its frame arrives. These slots are released after
parsing or expiry. Established ordinary requests do not occupy them. After parsing, each service admits
16 ordinary handlers and two separate status/stop handlers. Bridge peer connections use
the same ordinary allowance. Excess control requests return `capacity`; excess peer
connections close. These bounds do not promise a deadline for disk operations.

A timeout, disconnect or cancellation does not cancel an accepted database mutation and
is not proof of rollback. Use the existing memory idempotency contract for uncertain note
replies. Services stop accepting connections, drain handlers, then close the worker after
all accepted jobs settle. Socket waits cannot keep a database transaction open.

Unwrapped database failures return `storage_error`; programming failures return
`internal_error`, including programming errors wrapped by a storage recovery exception.
Expected storage recovery errors retain codes such as `write_failed` and `storage_blocked`
and also record an observed storage fault.
Status waits at most one second for a priority database read, then returns known process
identity and a lock-protected worker snapshot without waiting for that read to finish.
`database_status` is `ready`, `busy`, `capacity`, `closing` or an error class. When the read
cannot complete, bridge `inbox_count` is null; memory omits database-derived fields such
as `head` and sets `healthy` false. Unknown values are never replaced with zero.

`database_worker` reports the running flag, both queue counts and the closing flag.
`database_observed_fault` is null until a storage or programming failure is observed,
then records the last error class until restart. This is historical evidence; a
successful unrelated query does not clear it or prove recovery. Memory also sets
`healthy` false after an observed worker fault. Invalid requests and capacity refusals
do not set a historical fault. These fields are not a complete database integrity check
or the notifier's separate delivery-health record. A status fallback requires a parsed
request; it cannot bypass a full unclassified connection pool.

## Memory control protocol

This section describes a protocol **this project defines**, unlike the rest of this document,
which records behaviour observed in another implementation. The memory service at `memory.py`
listens on its own private control socket and carries no peer traffic.

Transport is an AF_UNIX stream socket carrying one UTF-8 JSON request line and one JSON response
line, then close. Same-UID kernel peer credentials are the authentication policy, as for the
bridge's control socket. No token is published or accepted.

```json
{"op": "sync", "consumer": "session-a", "page_token": 0}
```

A response is `{"ok": true, "result": ...}` or `{"ok": false, "code": "...", "error": "..."}`.
The `code` names a recovery path and is the field a caller should branch on: `snapshot_expired`
and `stale_page_token` require restarting `sync`; `snapshot_incomplete` requires paging to the
end before acknowledging; `consumer_retired` requires a new consumer key; `capacity`,
`entry_too_large`, `idempotency_conflict`, `retry_deadline_expired`, `not_issued`,
`foreign_snapshot`, `snapshot_open` and `not_bootstrapped` describe a refused request that changed
nothing. There is no conflict code for competing revisions: a second replacement of the same entry
is a successful write whose result carries `conflicts_with`, naming the replacement it competes
with, and both remain live.

A `note` carrying `key` must also carry `deadline`, an absolute epoch second fixed before the first
send and repeated on every retry. Within it a repeat returns the original sequence with
`duplicate` true; past it the request is refused with `retry_deadline_expired` rather than appended,
because the service cannot tell whether the first attempt landed. The result echoes `deadline` and
reports `idempotency_horizon`, the longest deadline the service will accept.

A `sync` returns either `kind: snapshot` with `snapshot_id`, `entries`, `page_token`, `total` and
`more`, or `kind: delta` with `entries`, `cursor`, `next_cursor`, `head` and `more`. Continuing a
snapshot requires both `snapshot_id` and `page_token`. An `ack` carries `snapshot_id` once every
page has been issued, or `through` for a delta. `recall` and `status` return `more` with
`next_before` and `next_after` respectively, null when nothing remains.

Operations are `hello`, `note`, `sync`, `ack`, `recall`, `status` and `stop`. `hello` is the
reuse handshake and reports the service name, repository key, protocol and schema versions,
generation, process ID, health and search capability. `sync` returns either a `snapshot` page or
a `delta` batch and never advances a cursor; only `ack` does. Pages are bounded by encoded bytes
rather than by a row count, because a row limit multiplied by the maximum body size exceeds one
frame.

Sequence numbers come from a durable head that only ever advances. Reclaiming entries moves a
retained-history floor rather than the head, and a consumer below the floor is returned to a
fresh snapshot rather than handed a gap. The protocol field remains `floor`. This is a history
availability boundary; it does not imply summarization. A snapshot contains stored records,
not a generated summary.

## Limitations

Admission rules, deduplication, rate limits, and loop checks on Claude's side may reject a transported message. Return-path validation also depends on socket ownership and kernel process identity. A bridge that sends from a different process than its advertised listener can fail these checks; all outbound peer connections here originate in the listener process.

### Bridge startup and control failures

The bridge reserves its control and messaging socket paths before opening the inbox
database. It listens only after database initialization commits. An existing path
refuses startup without opening the inbox; `serve` reports `endpoint_unavailable`
and exits 78. No status probe or new advisory lock substitutes for this reservation.
Shutdown keeps the reservation until accepted database operations finish.

Control timeouts, lost replies, transport failures and invalid replies produce
structured CLI errors and exit 1. A failed reply does not establish whether a
mutation committed. The CLI does not automatically repeat that mutation.

On Linux, installed systemd bridge and session services do not restart on exit 70
(internal software error) or 78 (configuration refusal).
On macOS, the manual process exits and must be started again after correction; see
[macOS setup](docs/INSTALL.md#macos). For a leftover socket, follow [recovery from a killed instance](docs/INSTALL.md#recovering-from-a-killed-instance).
Remove a socket only after verifying that its owner is dead. Unsafe startup
directories also produce a structured ownership refusal with exit 78.

## Inbox schema 2 and journal activation

The bridge migrates its inbox in one explicit `BEGIN IMMEDIATE` transaction after
reserving both endpoints and before listening. Existing rows and AUTOINCREMENT
allocation are preserved. Rows gain `kind` (initially `peer`) and nullable `binding`.
A fixed-key `inbox_meta` table stores JSON values for `schema`, `ack_through`, and
`journal_activation`. Current initial values are 3, 0 and null. Schema 2 introduced
these fields; schema 3 adds binding observations as described below. No historical acknowledgement
is inferred. The inbox keeps its journal mode; migration does not enable WAL.

`ack` deletes rows and updates the monotone `ack_through` value in the same explicit
write transaction. The requested position is clamped to the allocated inbox head
read from `sqlite_sequence` within that transaction. A never-used inbox has head
zero. This prevents a large acknowledgement from covering future arrivals. Neither
peer receipt nor notification delivery acknowledges the inbox or a memory consumer.

Status includes a per-process 32-hex `generation`, `inbox_schema`, `ack_through`,
`journal_activation`, and `capabilities`. Implemented capabilities are
`inbox_ack_watermark`, `notification_journal_activation`, `inbox_subscription` and
`memory_binding`. Their advertisement
requires a successful read of validated committed metadata. Unavailable or corrupt
metadata produces database diagnostics, null database fields and no capabilities;
it is never substituted with legacy defaults. Incompatible startup metadata exits
78 as `incompatible_inbox`; other SQLite storage failures exit 78 as `storage_error`.
SQLite programming errors exit 70 as `internal_error`.

The control operation `activate-notification-journal` accepts `target_digest`
(64 lowercase hex characters) and `nonce` (32 lowercase hex characters). It commits
that pair before replying. Repeating the pair is idempotent. A different target is
always refused, and ordinary activation cannot replace a nonce.
`rebuild-notification-journal-activation` additionally requires
`expected_previous_nonce` and `accept_history_loss: true`; it replaces only the
matching target's expected nonce. Repeating the resulting pair is safe after a lost
reply. This is the bridge-side evidence operation, not a complete notifier rebuild.
The notifier journal and its maintenance command are not yet implemented.

The `memory_binding` table stores explicit bindings. Repository and state paths
are absolute and at most 4096 UTF-8 bytes; SQL constraints enforce these bounds.
Existing ordinary notification readers can still read the original inbox columns.
New notifier binding integration, capability negotiation and journal migration
remain pending; binding controls alone do not activate them.

CLI forms use the same operation names with `--target-digest` and `--nonce`.
Explicit activation replacement also requires `--expected-previous-nonce` and
`--accept-history-loss`. These controls are operator commands; stored peer controls
remain inert data and cannot invoke them.


## Change subscriptions

The bridge control socket accepts `{"op":"subscribe-inbox","protocol":1,
"generation":GENERATION}`. The memory control socket accepts `{"op":"subscribe",
"protocol":1,"generation":GENERATION,"repo":REPO_KEY,"consumer":CONSUMER_KEY}`.
Use the generation from that service's current status (or memory hello), and an
explicit service directory. Memory hello/status advertise `memory_subscription`.
Consumer keys are asserted provenance, not authorization; the service validates the
repository and generation. Binding-derived consumer keys remain a later component.

The first reply uses `ok/result` with `protocol`, `generation` and a random 32-hex
`subscription` identifier. Following frames contain only those three fields and
`event:"changed"`. No message body, sequence number or receipt is included. The
client sends no further frames. End of input closes the subscription.

Each service has at most 32 reserved subscription slots, including handshakes, and
at most eight handshakes awaiting a reply. These do not consume ordinary request or
reserved status/stop slots after request classification. The initial request read
has the existing two-second deadline; handshake replies and each subsequent frame
have a five-second write deadline. One queued hint coalesces further changes while
another frame may be in flight. A slow receiver is disconnected without blocking
other subscribers or a database transaction. There is no total connection lifetime.
An idle connection sends the same content-free hint after 30 seconds.

Database owners publish only after commit. Memory publishes when its durable head
changes; bridge insertion and acknowledgement publish after their transactions.
A hint is not durable evidence. The shared `subscriptions.watch_changes` helper
installs a subscription before rechecking durable state, repeats that sequence on
reconnect, and independently rescans every two seconds. Reconnect delays are 1, 2,
4, 8, 16, then 30 seconds. Its caller must verify service identity on each connection
and reconcile durable state. The notifier uses this helper for inbox delivery and
bound memory refresh. Memory contents still require explicit reads. Automatic memory
notices contain only a sync pointer. These are local service subscriptions, not
peer-bus subscriptions. Explicit binding and refresh controls are described below.


## Memory bindings and pointers

These bridge controls require explicit operator action. Session registration does
not bind memory automatically. `bind-memory` accepts only `repo_path` and
`memory_state_dir`; the CLI uses `--repo-path` and `--memory-state-dir`. Paths must
exist. The bridge derives the canonical Git common directory and repository key,
resolves the memory state root, and hashes the compact JSON array of repository key
and resolved state root with SHA-256 for the binding key. At most 16 bindings exist.
Each creation also assigns a durable random 32-hex `binding_instance`. An idempotent
bind preserves it; unbind followed by rebind changes it even when the key is the same.
Internal pointer rows retain this instance so later journal work can distinguish an
obsolete prior binding from unexpected missing data. The pointer frame stays binding-only.

Before binding or refresh, the bridge verifies the memory service name, protocol,
repository, recorded owner, kernel PID, process-start marker and generation.
Memory schema 4 is required for its durable 32-hex `store_id`. An older live memory
service yields `memory_upgrade_required`; restart it with the new runtime. Missing
memory yields `memory_unavailable`; incompatible identity or unhealthy storage is
refused. Binding refusals include `recovery:"retry"` for transient failures or
`recovery:"operator_action"` for conditions such as an old runtime. These are
operation replies, not process exits. Expiry of the bridge request deadline
returns `binding_timeout`, `recovery:"retry"`, and `outcome:"unknown"`. A timeout
is not proof of rollback; reread durable state before retrying.
Binding operations retain ordinary admission slots but use a 19-second deadline:
three seconds for Git, five for hello, five for process identity, two for status,
two one-second socket cleanup allowances and two seconds of margin. The binding
CLI allows 23 seconds, including request-header, response and cleanup margin.
These values are derived from the inner policies; they are not hard disk-latency
bounds. Other ordinary bridge operations retain their six-second deadline.
Process identity probes run outside the event loop, with at most two accepted
probes per process. Cancellation does not release a probe slot before completion.
The macOS process query has a five-second subprocess timeout. Probe saturation
returns a retryable service-busy refusal and leaves status/stop admission intact. Automatic integration must not repeatedly
refresh an unchanged service that requires an operator action. These failures do
not change a binding observation or pointer. The bridge's
`memory_binding` capability describes implemented controls, not the readiness of
any optional memory service. Each service must independently pass verification.

`refresh-memory BINDING` accepts no caller-supplied head or message. It reads the
verified memory store ID and head outside the inbox transaction. In one transaction,
it compares that exact observation with the saved one, deletes the previous pointer,
inserts a new pointer with a new inbox sequence, and saves the observation. A concurrent
refresh that changed the saved observation causes `binding_observation_changed`;
a caller must reread rather than apply a stale observation. A recreated binding
instead returns `binding_instance_changed`, also requiring a fresh lookup. Identical observations
do nothing, even after inbox acknowledgement or bridge restart. The first refresh
also creates a pointer for an empty store so a consumer can bootstrap explicitly.

Pointer frames contain only `type:"memory-pointer"` and `binding`. They carry no
memory body, store ID, head or sequence range. Ordinary peer input cannot mint this
row kind. Inbox and CLI projections label them `kind:"memory-pointer"`, include
`source_service_pid` instead of `peer_pid`, and carry separate inert pointer
guidance. Ordinary frame fields cannot claim that internal provenance. Up to 16 pointer rows have their own allowance; the 1,000 ordinary-row
allowance is unchanged. `unbind-memory BINDING` atomically removes the binding and
its pointer as obsolete, without advancing inbox or memory acknowledgements.

`memory-bindings [--after BINDING]` returns a bounded page with `bindings`, `more`
and `next_after`. Each binding exposes its paths, repository key, binding instance, observed store ID
and head, and persistent `anomalies` flags: bit 1 means store replacement; bit 2
means head regression within the same store ID. Both cause a replacement pointer;
`ack-binding-health BINDING` clears these flags without deleting a pointer,
changing the observed head, or acknowledging either inbox or memory. A clone restored with identical store ID
and head is indistinguishable here; content verification is outside this mechanism.
The binding instance is intentional diagnostic metadata on this listing; it is
not part of a pointer frame.
`service_state` is `verified`, `refused`, or `unknown`, with a classified
`service_reason` on refusal. It is a bounded process-local observation, expires to
unknown after 30 seconds, and starts unknown after restart. Listing does not contact
memory or infer current health from a retained pointer. An unpageable single record
fails explicitly as `binding_row_too_large` rather than returning an empty loop.

No binding operation advances memory consumer cursors. The saved observation means
only that the bridge issued a pointer. The notifier owns per-binding subscriptions
and finite recovery checks. Each check refreshes the verified service observation.
An operator-action refusal suspends repeated remote work until owner metadata changes
or an explicit successful refresh changes the observed service state.


### Private control path aliases

Private service control endpoints use the resolved state directory for both the
socket-length decision and the fallback digest. All spellings of one state
therefore select one new endpoint. This rule does not apply to peer messaging
addresses or registry socket paths, which remain literal for key lookup.

A client can still use an existing legacy direct `control.sock` through the
original configured state path. A memory client can also use the old spelling
from its owner record, but only when it resolves to that state's direct socket.
The deterministic legacy hashed endpoint is also retained as a client route,
including when a long alias had selected it for a short canonical root. The normal
private-directory, socket-mode, kernel-PID and service-identity checks still apply. Two distinct old/new endpoints cause a refusal. New bridge and
memory startup refuses a retained distinct legacy endpoint before database startup;
it never removes that endpoint or starts a second listener beside it.


## Notifier controls and delivery health

The notifier owns a private control endpoint under `<bridge-state>/notifier`.
Same-user credentials and canonical private endpoint rules apply. Requests are
single JSON lines with `op` equal to `status`, `stop`, `retry`, or `ack-health`.
Only `retry` accepts another field: `sequences`, a nonempty bounded list of inbox
sequence numbers for retained exhausted work. A reply is an operation result,
not a peer receipt. `notifier_not_ready` means the journal is unavailable; it does
not identify a programming fault. Recovery values are `retry`, `capacity`,
`invalid_request`, `operator_action`, and `internal_error`. `retry` requires a new
observation or completion of accepted work; it is not permission to repeat an
ambiguous mutation without checking state. Rebuild reports `bridge_storage_unavailable`
with exit 75 before checking capabilities when the bridge cannot read its store.
A readable older bridge instead yields `bridge_upgrade_required` with exit 78. Control timeouts do not prove rollback.

Status reports lifecycle separately from `delivery_health`. The latter contains
`state` (`healthy`, `degraded`, or `unknown`), fixed reason codes and a bounded
journal summary. Pending work, exhausted attempts, ambiguous outcomes, compatibility
mode, accepted history loss, memory refusal and storage faults remain visible.
A journal query has a one-second observation deadline. No fresh result means unknown
journal values, not a reused healthy result. This deadline is not a disk I/O bound.

A separate worker publishes `notify-health.json` every two seconds. Readers require
a matching verified readiness owner and a snapshot no older than 15 seconds.
Missing, stale, malformed or mismatched evidence yields unknown delivery health.
Snapshot failure is reported in process memory and a content-free diagnostic;
readers do not assume a final failure snapshot could be written. A healthy process
can have degraded delivery health. A provider fault must not trigger a second notifier.

`retry` resets the current automatic budget for selected retained exhausted units.
It first reconciles source acknowledgements and obsolete pointers. `ack-health`
clears acknowledged diagnostic aggregates without acknowledging source records,
resetting budgets or clearing uncertainty on retained work. Rebuild is a stopped
owner operation, not a socket control; see [notifier recovery](docs/NOTIFIER.md).

Memory services advertise `memory_target_guard` when requests can carry both
`repo` and `generation`. A mismatch is refused before maintenance
or mutation. The exact-path memory CLI verifies the owner and uses this guard;
older services without it require an upgrade for this route.

## Local usage reports

`usage_report.py` emits versioned local JSON described in [USAGE.md](docs/USAGE.md).
It introduces no peer frames, remote transcript access, hooks, or model wakeups.
Selections and reports are local data and cannot grant authorization.

## Delivery ledger and presence

Inbox schema 4 and notification journal schema 2 provide the local-only
[delivery evidence contract](docs/DELIVERY.md). Private control operations `delivery`,
`handled`, and `record-notification` do not create peer frames. Deduplication is
bound to an observed sender process lifetime, recipient installation and message
ID, with a canonical payload fingerprint and bounded retention. Outgoing attempt
IDs and deadlines are local controls; no new native wire fields are introduced.

Service health and model activity have separate evidence and observation times.
Unsupported activity remains unknown. The daemon omits registry activity instead
of claiming a permanent wait. Native status-omission discovery verification remains
an open release gate, as documented in the delivery contract.
