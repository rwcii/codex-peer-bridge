# Peer parity and shared memory

Design and acceptance contract for one programme of work. Two agent sessions, one Claude
participant and one Codex participant, agreed this contract with the maintainer before any
implementation started. It states what the programme must deliver, what it must refuse to
deliver, and how completion is judged.

Read `PROTOCOL.md` for the observed wire protocol and `AGENTS.md` for the operating rules
this contract inherits.

**How to read a disagreement with the code.** This is a target design, so "the code wins"
is the wrong rule. The code states current behaviour. This contract states required new
behaviour. Where the two differ, the difference is either a defect this programme removes
or an error in this document, and the text must say which. A statement about what the code
does today is corrected by reading the code. A statement about what the system must do is
changed only by agreement.

## Purpose

A Claude peer and a bridge participant do not have the same capabilities on the local peer
bus. The transport is symmetric. The visibility around it is not. A bridge participant
publishes a static status value that is not evidence of current model activity, receives a
pointer only after a polling interval, and can answer no question about delivery.

Separately, agents working in one repository cannot share working memory. A decision one
agent records is invisible to another agent whose session started earlier.

This programme removes both gaps, and it treats the existing false values as defects to fix
rather than as features that are merely absent.

## Scope

Four contracts delivered as one programme: identity and provenance, delivery, presence, and
repository memory, over a stated trust boundary. The programme is not complete until the
acceptance criteria pass.

## Non-goals

No cloud component; everything is local, same-user, and carried on Unix sockets. No new
dependencies; the runtime stays Python standard library only, 3.11 or later, on Linux and
macOS. No change to the Claude wire protocol; every new participant is an ordinary peer on
it. No embeddings and no vector search. No content from a private handoff file and no
runtime data enters this repository.

## Observed asymmetries

Each row names the code that produces the current behaviour. Claims about a Claude peer are
marked where they are not yet verified, because a native interface that exposes a feature is
not the same as verified wire semantics.

| Behaviour | Bridge participant today | Origin | Claude peer |
| --- | --- | --- | --- |
| Activity | constant `waiting`, written once at registration | `notify.py:114` | no refresh observed over four seconds; update policy unverified |
| Idle notice | not advertised, not implemented | `notify.py:113` | native support exposed; wire semantics unverified |
| Priority | accepted, then discarded | set at `bridge.py:143`, never read by `notify.py` | native field exists; handling unverified |
| Delivery answer | control frames stored inert, never answered | `bridge.py:176` | control frames observed; semantics unverified |
| Arrival delay | a polling interval of up to two seconds is added before the notice is even sent | `notify.py:154` | not measured |
| Session identity | not published at all | `notify.py:110-114` | published as `sessionId`, not projected by `peers()` at `bridge.py:121-123` |

The polling interval adds between zero and two seconds in normal operation. It is not a
two-second minimum. End-to-end delay also includes provider scheduling, the participant's
own turn boundary, and any retry after a failure, so the poll is one term and not the whole
figure.

One measured sample shows how large the non-transport terms can be. A queued notice was accepted by
the Codex CLI in 0.279 seconds. A tool timestamp taken after the participant's model had received it
was 343.126 seconds after submission, and 28.090 seconds after the participant prepared to yield its
turn. That tool timestamp is not the moment of first model action, and the interval contains an active
turn, an earlier queued notice, and model and tool dispatch. A separate sender-supplied timestamp sat
21.357 seconds before storage, but it was taken before the send call, so it mixes model composition,
tool dispatch and transport and measures none of them.

The conclusion the sample supports is narrow. An event wake removes the notifier's local detection
delay, which is a real term of zero to two seconds. Provider scheduling and the participant's turn
boundary are separate terms and can be far larger. One sample establishes neither a distribution nor a
scheduling policy, and none of these figures may be quoted as an end-to-end latency.

## Contract 1 — Identity and provenance

**What the kernel verifies.** The peer UID and PID on a connected socket are kernel-supplied
and are the only authenticated facts available. The same-user boundary rests on the UID.

**What the registry supplies.** A registry record is same-user metadata written by a peer
about itself. It is not an authenticated session claim. A Claude peer publishes `sessionId`;
this bridge publishes no such field today, so a bridge participant has no equivalent. Read a
registry field as a useful label for attribution, never as proof of who is calling.

**Start markers.** `same_process()` treats an absent recorded marker as a match, which is a
deliberate leniency for discovery of older records. That leniency must not be promoted into
proof of identity. Identity attribution requires a marker that is present and that matches;
an absent marker yields unknown attribution, not a verified one.

**Stable consumer identity is required and must be defined explicitly.** A cursor, a claim owner, and
an acknowledgement all need a consumer that outlives one connection. A PID does not qualify: a direct
CLI invocation has a fresh PID for every command, so a PID-keyed cursor would restart on each call and a
PID-keyed claim would have no owner able to renew it. Each access route therefore declares its consumer
key. A route bound through a bridge derives the key from the stable participant session and the
repository, never from the transient bridge process or its instance generation, so a service restart
preserves the cursor. Instance generation exists for stale endpoint and lease checks, not for consumer
identity. A direct CLI call presents an explicit, caller-supplied consumer key, which the service records
without treating it as an authority claim.

**Repository identity** is the canonical Git common directory in absolute form, obtained with
`git rev-parse --path-format=absolute --git-common-dir`, then hashed. The bare form returns a path
relative to the current directory, so two repositories can yield the same value and collide. For
example, two conventional checkouts can both return `.git`. Worktrees, bare repositories and separate
git directories return other values, so collision is possible rather than universal. Worktrees of one
repository share one memory service.

**Provenance is a record, not an authentication.** Every stored entry records the asserted
author, the observing session, and the time. Under a same-user boundary any process of this
user can produce a plausible claim, so provenance supports attribution and recovery and never
establishes authority. No component may read a provenance field as proof that a human
approved anything.

## Contract 2 — Delivery

A sender currently learns only that a socket write completed. Delivery gains explicit stages,
and each stage requires its own evidence:

`transport` — the connection completed and the frame was written. This is not receiver
persistence.
`stored` — the receiving bridge committed the frame durably.
`notified` — a content-free pointer was handed to the participant's provider.
`fetched` — an inbox request returned the entry. Server output is not proof that a model read
anything.
`handled` — the participant explicitly reported an outcome.

`handled` is a new explicit operation and is not `ack`. Acknowledgement removes an entry from
the inbox and says nothing about the work. `handled` carries an outcome, so it can report
failure or refusal; it never means success by default. A stage is reported only with the
evidence that defines it, and a stage without evidence is not claimed.

**Indeterminate outcomes are a distinct state.** A timeout after the receiver may already have
accepted a frame is neither success nor failure. Automatic replay in that state can duplicate
work on a native peer that does not deduplicate, so a stable identifier alone does not make a
retry safe. The programme records the indeterminate state, bounds retries, and terminates
explicitly rather than retrying without limit.

**Deduplication state outlives the inbox.** `ack` deletes inbox rows at `bridge.py:240`, so receipt and
deduplication records live in their own table. Otherwise a duplicate arriving after an acknowledgement is
indistinguishable from a new message. The following are implementation acceptance gates, not measured
facts: a delivery deduplication key is scoped to the logical sender, the recipient, and `msg_id`; the key
is bound to a canonical fingerprint of the payload; a key presented again with different content is
rejected rather than accepted or silently ignored; retries have a finite deadline; and deduplication
state is retained through at least that deadline.

**Receipts must never wake a model and must never produce a receipt loop.** A receipt is a
control frame. `notify.py:29` filters notification to frames of type `user`, so control frames
raise no notice today. That behaviour is currently incidental; it becomes a deliberate, tested
invariant, and no receipt is ever answered with a further receipt. Wire receipts are used only
where the peer's protocol semantics are verified. Where a peer lacks them, the stage is
recorded as local status and is not sent.

A bus operation cannot reply on the connection that carried it, because `PROTOCOL.md` records
that replies use a separate connection. A correlated result may return on a new verified
connection, and both Claude peers and bridge participants listen, so a correlated result is
available to either. Synchronous results still belong on a control socket.

**Priority is declared per provider, from measurement.** A peer-supplied priority never becomes
permission to interrupt a user or to act. A provider whose notification path cannot request a
scheduling priority still preserves the value in the stored frame, and reports the limit at that
boundary, rather than accepting the value and discarding it.

## Contract 3 — Presence

Presence separates two facts that one field currently confuses: **service health**, meaning
whether the adapter runs, and **model activity**, meaning what the participant's model does.
They have different sources and different lifetimes, and they are published separately.

Every presence value carries its evidence source and the time of observation. `unknown` is an
explicit value and is the default. No component synthesises `idle`. A heartbeat does not prove
idleness, and an inbox acknowledgement does not prove that a task succeeded.

A capability declaration separates three things that are easy to confuse: schema support,
endpoint reachability, and live verification. A schema that names a status value proves neither
of the other two.

Before any new value is published into the Claude registry, the accepted set must be measured.
Publishing an unrecognised status risks breaking discovery, which would regress behaviour this
project already provides.

A provider hook or the server that owns a participant's session is a preferred source, but only
when it can be read without side effects and without changing the thread.

## Contract 4 — Repository memory

### Lifecycle, ownership, and reuse

One memory service per repository, shared by every session in it, with its own lifecycle. It is
not a child of a per-session supervisor: `session.py` keys a supervisor to one participant
session, and `supervisor()` treats any child exit as fatal to that session, so a shared process
placed under it would die with an unrelated session and take healthy sessions down with it.

**Reuse is verified, not assumed.** A bind conflict plus any successful control response does
not establish that the listening process is the right service. Reuse requires a control
handshake that confirms the canonical repository identity, the protocol version, the instance
generation, the ownership record, and full health. Anything less is an error, not a reuse. The
service reuses an instance; it never claims to adopt another process.

**Ownership and stop.** The repository service outlives the sessions that use it. Stopping a
participant session must not stop it. Shutdown belongs to an explicit repository-level stop or
upgrade operation, which owns the decision and performs it.

**Crash recovery.** A stale socket is removed only after ownership and death are proved, never
on a probe alone, because a live listener with a full accept queue and a dead owner are
indistinguishable to a connection attempt, as `bridge.py:259-270` already records. Recovery
without that proof is refused and reported.

### Lock order and wait graph

This is written before the lifecycle code, because the project has already shipped one deadlock
of exactly this shape: a command held a lock while waiting for readiness of a process that needed
the same lock to proceed, so the wait could only ever time out. The rules below exist to make that
class of defect impossible rather than unlikely.

**Locks.** There are two, and only two, that a memory operation may hold. `start.lock` is a file
lock in the repository's state directory, held by a caller that is starting or stopping a service.
A database transaction is held by the serving process alone. When both are held the order is
`start.lock` then transaction, never the reverse.

**Invariant 1: the request-serving path never acquires `start.lock`.** A starting caller holds
that lock while it probes the running service, so a service that needed the lock to answer could
never answer, and the probe could only time out. Nothing that serves a request may take it. The
same process does take it once, after its listener has closed and it is removing its endpoint, which
is invariant 3; by then it serves nothing and no probe is waiting on it.

**Invariant 2: no transaction is held across a wait.** A transaction never spans an `await`, a
socket read, or a subprocess call. A reader blocked on a peer must not hold write access to the
store.

**Invariant 3: endpoint publication and removal are one locked operation that never waits.**
Ownership alone is not enough. Checking a generation and then unlinking the socket and the record
separately is a race: a successor can bind and publish between the two unlinks, and the
predecessor's second unlink then deletes it. Both publication and removal therefore take
`start.lock`, they verify the generation inside it, and they perform no waiting at all while
holding it. A missing record is not permission to delete; absence means another process has taken
over the bookkeeping, so cleanup declines.

**Invariant 4: a caller waits only for bounded work it does not itself block.** A start caller
holds `start.lock` across the handshake and the bind, both bounded and neither requiring anything
the caller holds. A stop caller sends its request and waits for the instance to go **without**
holding the lock, because invariant 3 means the exit path needs it; only afterwards, and only if
residue remains, does it acquire the lock. Completion is judged by the ownership record and the
endpoint rather than by a refused connection: a service that has closed its listener and is still
draining refuses connections while very much alive, so treating a failed handshake as exit would
report success with work still in flight. A stop is bound to the generation it asked to stop, so a
successor is never mistaken for it.

**Wait graph.** A starting caller waits on the serving process answering a handshake. A stopping
caller waits on the serving process exiting. The serving process waits on neither: while it serves
requests it holds no file lock, and its transactions are internal and bounded. It takes
`start.lock` only during its final endpoint removal, where it waits for nothing and no one is
waiting on a reply from it. The graph is therefore acyclic by construction, and a regression test
asserts each edge rather than trusting the reasoning.

A stop is addressed to one instance. The request carries the repository and the generation it
intends to stop, and the serving process validates both before changing any state. Deciding the
target from a record read beforehand would be a race: a successor can replace the endpoint between
that read and the connection, and an unqualified request would then stop the wrong service.

### Storage bound

The full argument, with source references and measurements, is in
[STORAGE-BOUND-DERIVATION.md](STORAGE-BOUND-DERIVATION.md). This section states the
policy the store implements; the derivation is what justifies the numbers.

Two earlier designs for this section are withdrawn and must not return. Bounding the log
with `wal_autocheckpoint` and `journal_size_limit` was wrong: the first triggers a passive
checkpoint rather than capping anything, and the second governs what is retained after a
reset, not the peak. Bounding it by measurement plus a check after commit was also wrong:
a check after the fact is a diagnostic for an assumption already violated, and it cannot
hold a peak below a hard limit.

**The log a single transaction can produce is finite and derived, not observed.** It has
two terms. The pages the transaction dirties, `D`, reach the log exactly once each,
because `cache_spill=OFF` makes the commit list the only write path. The final frame is
then repeated to the next sector boundary, which adds at most `ceil((65535)/4120) = 16`
frames at this page size. Since `max_page_count` caps `D`:

```
WAL <= 32 + (N + 16) * (24 + page_size)          N = max_page_count
```

Durability is not traded for this. Padding disappears if `synchronous` is lowered, and it
is carried instead.

**Five settings are applied when the store opens, and every one is read back and
compared with what was requested.** A pragma that is silently ignored is the failure mode
this project has already met once with `auto_vacuum`.

| Setting | Why |
|---|---|
| `locking_mode=EXCLUSIVE`, before `journal_mode` | Engine-enforced single ownership, and no `-shm` file exists at all |
| `auto_vacuum=INCREMENTAL`, before `journal_mode` | Freed pages can leave the file |
| `journal_mode=WAL` | The log this section bounds |
| `cache_spill=OFF` | Makes the commit list the only path to the log |
| `temp_store=MEMORY` | Keeps sorters out of the filesystem |
| `max_page_count=N` | The durable ceiling, enforced by the engine |

`max_page_count` cannot shrink an existing database; asked to, it returns the current
count. The returned value is therefore compared with the request, and an oversized
incompatible store is refused **without deleting anything**.

**Ownership.** All readers reach the store through the control socket. No database read
transaction spans a response or an await. Exclusive locking mode makes this an engine
property rather than a convention: a second process cannot open the store at all.

**Admission compares durable data pages, and nothing else.** The earlier design compared
the total file size including the log and then added a fixed margin for it. That made
admission depend on checkpoint timing, so the same write succeeded or failed according to
when the last checkpoint ran, and how far a given SQLite build shrank the file during
recovery. Admission now reads `page_count` alone. The log is not in the comparison
because it is bounded separately and reset before every write.

**A reset is verified before every write transaction, and the test is a conjunction.**
`PRAGMA wal_checkpoint(TRUNCATE)` must return `busy == 0` **and** `log_pages == 0`, and
the `-wal` file must be absent or exactly zero bytes. No single one of these is
sufficient: a passive checkpoint reports `busy == 0` while moving nothing, and `(0, 0, 0)`
is also what a store returns when no log has ever existed. A failed reset reports
`storage_blocked`, admits no further writes, and keeps reads and explicit recovery
available. Under exclusive mode no foreign reader can exist, so a busy result is a
genuine recovery condition and not ordinary contention.

**Maintenance and control capacity are reserved from ordinary admission.** An
acknowledgement, a retirement record, a withdrawal and an index rebuild draw on a reserve
that ordinary appends may not consume, so a full store can still record progress and
retract a directive. A rebuild that would exceed the reserve is refused before it starts,
never part way through, and a known incomplete index is never served.

**Auxiliary files are bounded rather than excused.** Sorters are held in memory.
Sub-journals have only two write sites in the pager: the cache-spill path, unreachable
once spilling is off, and one gated on an open savepoint. The service opens no explicit
savepoint, so the only source is a statement journal, whose size is bounded by the pages
one statement must undo. No maintenance statement may have an unbounded undo set.

**The memory cost is stated, not hidden.** Disabling the spill holds the dirty set until
commit. The payload term is bounded by `N * page_size`; page metadata, FTS5 working
structures, in-memory sorters and the rows in flight are additional and are bounded by the
same operation design that bounds `D`. An allocation failure fails the transaction by
rollback, like any other error, and never applies it partly.

**Verification is by invariant, not by size.** Different correct SQLite builds allocate
different numbers of pages for the same content, so a test demanding a particular size
would be testing the build. The supported matrix asserts instead that every setting reads
back as requested, that no `-shm` file exists, that the log holds zero bytes before any
commit, that frames per dirty page never exceed `1 + 16/D`, that a reset satisfies the
conjunction above, that a `max_page_count` breach rolls back whole with integrity intact,
and that no sub-journal reaches disk.

### Durable transitions

Every durable change passes one admission point, so the advertised budget is a limit
rather than a claim. A mutation that wrote around it would grow the store while the
accounting said otherwise, which is how the first implementation advertised bounds it
did not enforce.

| Transition | Owner | Transaction | Budget charge | Retained record | Expiry |
| --- | --- | --- | --- | --- | --- |
| Append an entry | writer's consumer key | one, rolled back whole | body plus path, author, scope target, consumer and key, plus row overhead; one slot | the entry, and an idempotency key when given | `expires` when set |
| Supersede or revoke | writer's consumer key | one, with the link written onto the replaced row | as above, drawn from reserved slots and reserved bytes | the replaced row, its revision and the link | follows the entry |
| Freeze a snapshot | the reading consumer | one | every copied payload plus per-row overhead | the snapshot and its frozen members | `SNAPSHOT_TTL` unacknowledged, `ACK_RETENTION` after acknowledgement |
| Issue a page | the snapshot's consumer | one | none; updates a counter | the highest position issued | with its snapshot |
| Acknowledge | the snapshot's consumer | one | none | the acknowledgement, for replay | `ACK_RETENTION` |
| Register a consumer | the consumer key | one | key plus row overhead | the cursor | `CONSUMER_TTL` idle, then a tombstone |
| Retire a consumer | the service | one | charged in advance at registration | the tombstone, reporting `consumer_retired` | `RETIRED_TTL` |

Registration is bounded by live consumers and by the tombstones they will become: it is refused
when live consumers reach `MAX_CONSUMERS`, or when live consumers plus retirement records reach
`MAX_CONSUMERS + MAX_RETIRED`. The combined form is deliberate and is not a cap of `MAX_RETIRED`
tombstones: if every live consumer retires, the number of tombstones can reach
`MAX_CONSUMERS + MAX_RETIRED` before a further registration is refused. That is the accurate
statement of the bound. The charge taken at registration is an admission buffer rather than stored
usage, held so that retirement can never be refused for space later; `usage` reports a tombstone
only once one exists.

Progress transitions — acknowledgement, page issuance and activity refresh — are bounded rather
than admitted. They may never be refused for space: a reader that cannot acknowledge can never
advance, so refusing one would make the store unreadable-forward exactly when it most needs
draining. They add no rows a caller controls, and they relieve write-ahead log growth when the file
approaches its limit.

Reclamation is not a transition a caller makes. It removes only records already past the
lifetime above, and it may never evict one still inside its window to make room: an
acknowledgement inside its retention is a promise of replay, so the service refuses a new
write rather than break it. Refusal is always explicit, names the reason, and leaves
stored data and protocol progress unchanged.

Physical accounting includes the write-ahead log, which `page_count` excludes and which
can hold a large share of the bytes on disk. Logical accounting measures variable fields
rather than approximating them with a constant.

### Subscriptions and notices

Notices go only to explicit subscribers of one canonical repository. There is no global peer
broadcast. The originating session is excluded where that is meaningful. Notices are debounced
and coalesced, the subscriber set and each queue are bounded, and every destination is validated
against current process identity before a notice is sent. Notices stay content-free and name no
sequence range.

### Storage

SQLite in WAL mode, under a state directory keyed by the repository identity. Entry types are
`decision`, `finding`, `gotcha`, `handoff`, `status`, and `directive`. Full text search is used
when the runtime provides FTS5 and falls back to a pattern match when it does not; both paths
are tested, and a missing optional module never fails the build.

### Operations

`note` appends an entry. `sync` returns entries after the caller's cursor. `recall` searches live
entries and moves no cursor. `claim` and `release` manage advisory claims. `status` reports
subscribers, cursor lag, and live claims.

**`note` is idempotent within a declared key scope.** The following are implementation acceptance
gates, not measured facts: a note idempotency key is scoped to the repository, the stable consumer
identity, and a caller-supplied key; the key is bound to a canonical fingerprint of the payload; and
reuse of one key with different content is rejected rather than silently accepted or silently ignored.

### Cursors, snapshots, and acknowledgement

A first sync, or a cursor below the compaction floor, enters a snapshot. The snapshot is taken against
a fixed durable head `H`. The reader pages through the whole snapshot, acknowledges that snapshot, and
only then consumes deltas after `H`. **The page token is not the event cursor**, and finishing one page
does not advance progress. Snapshot versions are retained for a bounded period.

Acknowledgements are monotonic and idempotent, and each is tied to the issued batch or snapshot
and to a stable consumer identity. A crash before acknowledgement replays the data, which is the
intended behaviour. An expired snapshot must not advance progress; it produces an explicit
recovery path instead.

Snapshot content is defined, not left implicit: live claims, live directives, non-superseded
decisions and gotchas, and the most recent findings and handoffs, each with a stated ordering and
a stated cap.

Concurrent revisions of one entry must not lose a write silently. A revision states the revision
it replaces, and a conflicting revision is retained and reported rather than overwritten.

### Directives

A `directive` records a reported preference or working rule. It carries scope, source, observing
session, time, and revision. Scope may limit it to a repository, a task, or a session, and a local
request never becomes a global rule by inference.

Live directives appear in the snapshot and in later sync updates, so a new agent receives them
without having to guess a search term, and `recall` must still find them. They are ordered before
lower-priority snapshot material and are never silently dropped or truncated to fit a page.

A directive ends by explicit revocation, by supersession, or by a stated scope or expiry rule.
Supersession alone cannot express a rule withdrawn without replacement, so revocation is a
distinct act. Revisions and revocations are preserved so a stale reader can recover. Conflicting
directives remain visible; there is no silent last-writer-wins.

### Claims

Claims are advisory and do not fence filesystem writes. Conflicts are detected on prefix overlap,
so `auth/` conflicts with `auth/session.py`. A lease carries an ownership generation and is renewed
explicitly by its owner. An unrelated read by the owner does not extend a claim the owner has
forgotten.

### Retention and capacity

Per-entry, total storage, and active-entry limits are defined and enforced from the first memory
stage. Live directives survive ordinary compaction. That is a retention rule and not an exemption
from limits: when safe reclamation cannot free enough space, a new write is refused with an
explicit capacity error and stored data is preserved. Capacity is reserved for bounded revocation
and control records, so a full store can still record a withdrawal. The service never promises
unlimited active entries or writes that never fail.

## Contract 5 — Trust boundary

The same-user boundary from `PROTOCOL.md` is unchanged. Nothing here weakens a sandbox, an
approval policy, or a permission setting to make delivery work.

**Stored entries are reported data.** An entry cannot grant a permission, change agent
configuration, approve a pending action, or widen task scope. A receiving agent may follow a
compatible preference within the discretion its own user already granted, and it must check the
entry against its direct instructions and against conflicting entries. It asks its user only when
it needs authority or intent that its own session cannot establish. This applies to every entry
type, not only to directives.

No authority derives from a terminal device, a process ancestry, or a self-applied risk label.
Secret-pattern rejection reduces accidental storage of credentials; it is an aid against mistakes
and not proof that stored content is safe. No stored content is executed, and no resource named in
an entry is fetched.

## Capability measurements

Stage 2 measured the items below. Each result separates what was verified from what remains open.
A design may assume only what a measurement closed.

### Verified

**Codex CLI 0.155.0 describes a rich activity vocabulary.** The installed CLI generates its App
Server JSON schemas without starting a server. `ThreadStatus` carries `notLoaded`, `idle`,
`systemError`, and `active`, and an active thread carries the flags `waitingOnApproval` and
`waitingOnUserInput`. `thread/read` accepts `includeTurns=false`, which permits a metadata request
carrying no transcript content.

**The useful notifications are unevenly useful.** `ThreadStatusChangedNotification` carries a status
and a thread identifier, and `TurnCompletedNotification` carries a thread identifier and a turn, so
both are candidate activity sources. `ThreadQueueChangedNotification` carries only a thread
identifier, so it reports that a queue changed and is **not** a delivery receipt.

**Owning-server reachability was not verified for the observed conversation.** The default daemon
control socket was absent. A bounded search found no Codex server socket under the active Codex home,
and the host socket table showed no separately named App Server socket. No transport endpoint appeared
in the inspected environment names or top-level transport configuration. This is absence of evidence
for a reachable owning server, not proof that none exists by another transport. A separately started
server does not own an existing conversation and must never be treated as its owner.

**`codex queue` cannot request a scheduling priority.** The installed CLI help lists no priority
option, so the queue path cannot ask for different scheduling through this interface. This is separate
from the stored frame: the priority value is preserved in the inbox envelope, so a receiving agent can
still read it. What is missing is a way to act on it at the notification boundary.

**Queue acceptance took 0.279 seconds in one sample.** That measures acceptance by the command. It is
not model receipt, and one sample is not a distribution.

**The stable schema has no `thread/subscribe` method, but it does contain `thread/unsubscribe`.** An
unsubscribe with no matching subscribe suggests that a subscription is established elsewhere. It does
not prove where, and it does not establish that any particular call has subscription as a side effect.
Source documentation or a live check must establish those effects before any call is used for
observation, because a method that changes thread lifecycle is not an acceptable way to read status.

**A Claude peer did not refresh its activity value during a four second observation.** Three samples
of one live record across roughly four seconds returned an unchanged value whose recorded age advanced
with the wall clock, from 201.5 to 205.5 seconds. This rules out refresh at that interval. It does not
establish the update policy: a longer heartbeat or some other rule remains possible and is unverified.
Whatever the policy, the recorded age cannot distinguish a long-held state from a peer that stopped
while in it, so presence requires an independent liveness and freshness check regardless.

**Priority involves three separate surfaces, and two of them have no switch.** The bridge's own send
command exposes `--priority now|next|later` at `bridge.py:330` and preserves the value on the outbound
frame at `bridge.py:143`, so a bridge participant can set it. `codex queue`, the downstream path that
notifies a Codex model, has no priority switch. The native peer-messaging surface available to a Claude
session also has none. Conflating the three is an error: the wire carries priority, one sender surface
sets it, and two model-facing surfaces cannot.

**An observed value is not a policy.** A native Claude send was observed emitting priority `next` in
one sample. That records a default, not a scheduling policy. Untested priority behaviour is recorded
as unverified, never as unsupported on the wire.

**A native send produces the same envelope this bridge produces.** The stored frame carried the fields
`from`, `message`, `msgV`, `msg_id`, `priority` and `type`, with `content` and `role` nested inside
`message`, and the inbox wrapper added `frame`, `guidance`, `peer_pid`, `received` and `seq`. The wire
shapes agree, so the asymmetry is in handling rather than in format.

**Sender-side timestamps cannot measure transport.** A timestamp supplied by the sender sat 21.357
seconds before stored receipt in one sample. That interval contains unknown amounts of model
composition, tool dispatch and transport; it is not measured model turn time and it separates none of
those terms. Latency must be measured at the send boundary and the receive boundary, and a checkpoint
file time records post-success bookkeeping rather than the moment a call was made.

**A native idle-notice request is refused before any frame is emitted.** Requesting an idle notice
against a peer that does not advertise the feature failed at the sender with an explicit refusal, and
nothing was subscribed. No control frame reached the bridge, so the frame shape cannot be captured from a
peer that does not already advertise support. This creates a bootstrapping constraint: the frame is
observable only through an instance that advertises the feature, while Contract 3 says a capability is
advertised only after a conformance test passes.

The resolution is a disposable capture fixture, bound by these rules. It runs with its own private state
directory, its own process, socket and registry record, and a clear test name. It advertises only the
exact feature string observed in native metadata, and if that string is not known it reports the gap
rather than advertising a guess. It is a passive recorder with no model queue target, and it receives only
the agreed idle request. It does not alter the live bridge, does not read key material, and does not
advertise production support. Raw captures stay private; only sanitized field names, types and results are
published. The fixture is stopped afterwards, and only the records and sockets it owns are removed. A
disposable fixture is not a released capability claim, so this preserves the rule in Contract 3.

**This bridge publishes no session identifier.** The record written at `notify.py:110-114` has no
`sessionId` field, so a bridge participant has no published session identity today.

### Consequences for this design

Presence reports `unknown` whenever no owning server can be observed, which is the present state for a
Codex participant. The path that notifies a Codex participant cannot express priority, so a priority sent to one is
recorded and the limit reported at that boundary; the bridge's own outbound send still sets the field
normally. `codex queue` remains the verified delivery path and the fallback. No transcript is read and no permission is
altered to obtain activity data.

### Open

1. The control frame shape a Claude peer sends when it requests an idle notice. Closed only through a
   disposable instance that advertises the feature, because a non-advertising peer produces a
   sender-side refusal with no frame emitted.
2. The control frame shapes used for delivery status.
3. Which activity values the Claude registry accepts beyond those already observed.
4. Model receipt latency as a distribution. One sample measured 343.126 seconds from submission to the
   first tool timestamp recorded after model receipt. It includes an active turn, an earlier queued
   notice, and model and tool dispatch. The components and the distribution remain unmeasured.
5. FTS5 across the supported CI matrix. This needs a workflow run rather than a host check, and is
   closed in stage 3 by a CI job.
6. Whether provider hooks emit events for a queued peer notice and for a stop-hook continuation. A
   local feature list that reports hooks as stable does not prove that these paths emit the events this
   design needs.
7. Whether any read-only subscription exists, given the `thread/unsubscribe` finding.
8. Whether a native peer deduplicates a repeated delivery, which decides whether replay after an
   indeterminate outcome is ever safe.
9. Whether a native peer schedules by priority, as distinct from accepting a priority field. One
   observed `next` records a default and proves no policy. The question is separate from which sender
   surface exposes the field, and both must be stated independently.

Checks use only consenting sessions and safe native operations. Controls stay inert. No replacement
conversation is created or resumed. Recorded results contain capability metadata only: no keys, no
credentials, no inbox content, and no private session identifiers.

## Acceptance criteria

The programme is complete when the whole agreed behaviour passes tests, not when a stage merges.
Required coverage:

- Restart of each service, and recovery of state across it.
- Concurrent sessions in one repository, and isolation between repositories.
- A lost event wake, proving that a subscriber still converges through its recheck and its bounded
  recovery check.
- A wake published before the receiver commits, proving that the ordering rule is enforced.
- Reconnection, proving that the subscribe handshake repeats and resumes from a cursor.
- Duplicate delivery, and an indeterminate outcome after a timeout, proving that replay does not
  duplicate work and that deduplication state survives an inbox acknowledgement.
- Stale presence, disconnection, and an approval wait.
- Unsupported provider capability, reported explicitly rather than silently ignored.
- Snapshot pagination during concurrent compaction, an expired snapshot, and a crash before
  acknowledgement that correctly replays.
- Idempotent `note`, including rejection of one key reused with a different payload.
- Concurrent revisions of one entry, proving no silent loss.
- Revoked, superseded, expired, and conflicting directives.
- Claim expiry, prefix-overlap conflict, and ownership generation.
- Reuse handshake rejection when repository identity, protocol version, or generation does not match.
- A repository service that survives a session stop, and a stale socket that is not removed without
  proof of ownership and death.
- Subscriber scoping, originator exclusion, and bounded subscriber and queue limits.
- Storage failure and capacity refusal, with stored data preserved.
- Receipts that raise no model notification and no receipt loop.
- Consumer identity stability across separate CLI invocations.

Documentation and tests accompany every change. Tests use synthetic peers and never send traffic to a
live agent session by default. One driver writes at a time.

## Build order

1. This contract, with every unverified capability marked.
2. Controlled capability checks, then closure of the open contract points with measured facts.
3. Pull-only repository memory, including lifecycle and failure recovery. Scope, provenance, stable
   consumer identity, explicit cursor acknowledgement, stable pagination, size limits, and safe
   capacity failure are enforced from this stage; compaction is added later without changing them.
4. Shared transport extracted against both consumers, then bus registration, scoped subscriptions,
   coalesced pointers, and an event wake with durable catch-up. Extraction happens when the second
   consumer exists and its requirements are known, so the abstraction fits both rather than one.
5. Provider presence, priority declaration, and delivery receipts.
6. Remaining claims, compaction, and content checks.

Stage size does not determine correctness. A small change is acceptable when it meets a complete
contract, preserves compatibility, and includes failure recovery. A stage must not leave a known defect
for later and must not install a design already known to need replacement.

## Implementation notes carried from review

Three defects found during review must not be reintroduced.

**Pointer coalescing.** Coalescing keeps only the newest pointer for one inbox. It must delete the
stale row and insert a new one in one atomic step, because `notify.py:28` selects rows with
`seq > after`, and an in-place update would leave the sequence unchanged so the participant would never
be notified. Only verified memory pointers for the same canonical repository may be coalesced with each
other; ordinary peer messages are never coalesced.

**Event wake ordering, stated for each side separately.** The publisher commits durably before it
publishes a wake; a wake published first can be observed before the data exists. The subscriber installs
its subscription and then rechecks the durable head and backlog, or performs an atomic
subscribe-with-cursor handshake; subscribing after a check leaves a gap in which an event is lost.
Persisting before subscribing does not prevent a missed wake on its own; it still requires the
subscriber's backlog recheck, and subscribing is not the publisher's half of the rule. Reconnection repeats the handshake. A bounded recovery check remains in place regardless, so a
lost wake can never strand an inbox.

**Subscription lifetime.** A subscription must live outside the six second request deadline at
`bridge.py:189`, and its resource use must be bounded.
