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

Despite its filename, `codex_instructions.py` manages guidance for both Codex and
DeepSeek participants, with separate markers and setup commands. Koinon supplies the
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
floor rather than the head, and a consumer below the floor is returned to a fresh snapshot rather
than handed a gap.

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

On Linux, installed systemd bridge and session services do not restart on exit 78.
On macOS, the manual process exits and must be started again after correction; see
[macOS setup](docs/INSTALL.md#macos). For a leftover socket, follow [recovery from a killed instance](docs/INSTALL.md#recovering-from-a-killed-instance).
Remove a socket only after verifying that its owner is dead. Unsafe startup
directories also produce a structured ownership refusal with exit 78.
