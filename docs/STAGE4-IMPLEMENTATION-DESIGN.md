# Stage 4 implementation design for review

Status: all five concrete design gates accepted in independent review. Shared transport, participant ownership, database workers and startup exclusion are implemented and reviewed. Inbox schema-2 migration, acknowledgement metadata and activation evidence are implemented and reviewed. Subscriptions and the shared reconnect/rescan helper are implemented and reviewed. Explicit bindings/pointers and store identity migrations are implemented in the current candidate, pending review. Automatic notifier integration and the notifier journal remain pending. No stage 4 runtime changes are deployed.
Baseline: develop 84f35e727b0f17a9469de182db9a843e7a28d092, integrated by signed merge 7503dc1.
Codex is the sole driver. Claude is the reviewer. The merged baseline was independently verified. Implementation branch: feature/shared-transport-delivery.

## Saved positions

- Memory consumer cursor: the position explicitly acknowledged by a memory consumer; belongs to the memory protocol only.
- Legacy notification checkpoint: the deployed `{thread, through}` file imported during migration.
- Notification scan checkpoint: the position through which enumerated notification work has a durable disposition; not proof of delivery.
- Inbox acknowledgement watermark: the bridge-owned position explicitly acknowledged through inbox ack, clamped to the AUTOINCREMENT allocated head in sqlite_sequence, read in the deletion transaction.
- Delivery disposition: the per-unit result (pending, reserved, delivered, failed/unknown, acknowledged, or obsolete), not a cursor.

## Scope and ownership

Extract socket mechanics only after testing both bridge and memory as consumers. Keep database operations, provider delivery, memory semantics, and message formatting outside peer_transport. Preserve bridge helper imports for existing callers. Platform differences remain in platform_support.

The bridge owns inbox writes. The notifier retains a read-only inbox connection and owns a separate notification journal under the configured bridge state directory (both legacy root and per-session layouts). The memory service owns its repository database. Each service database connection is created, used, and closed on one owning worker with check_same_thread enabled. Bound admitted worker jobs. Complete database initialization before accepting requests or reporting readiness. No transaction waits for network delivery. Worker results return to the event loop before touching asyncio state. Shutdown settles accepted mutations before closing the database. Cancellation is not proof of rollback. Storage and programming failures are distinct from invalid requests and must affect reported health.

## Transport and endpoint roles

Shared mechanics include private-directory checks, bounded newline JSON framing, kernel peer credentials, outbound authentication, socket closure, and configurable per-class deadlines. Preserve the 262144-byte frame limit and separate 65536-byte inbox payload limit. Both limits measure the ASCII-escaped JSON wire frame including its terminating newline. The 65536-byte check remains one byte conservative relative to the JSON stored without that newline. Apply the frame limit to outbound peer frames, control requests and replies, and subscription event frames; batching slack is not proof of a bound.

Messaging endpoints and service endpoints have separate validators and discovery results. A control socket that falls back into a peer socket directory must not become a generic send destination. Service records cannot appear as messaging peers merely because they publish a socket. Preserve literal messaging paths for token lookup, including macOS aliases. Registry fields describe peers; they confer no authority. A present matching process-start marker and current endpoint generation are required for a service binding, and checked again before use.

## Subscriptions and pointers

Subscriptions are explicit and scoped to one canonical repository. Stable consumer keys derive from configured participant/repository bindings, not transient PIDs or registry sessionId. Direct CLI keys remain asserted provenance. Kernel credentials establish same-user access, not human approval or authenticated session provenance.

Use separate request and subscription reservations. Subscription exhaustion must leave peer reception, status, and stop available. Limit subscription handshakes, queued work, and blocked writes. Long-lived subscriptions do not inherit the ordinary request lifetime or frame-count limit. A slow subscriber cannot hold a transaction or delay unrelated subscriber sends. This does not promise bounded disk operation latency.

Only an explicitly bound service route can create a memory-pointer record. The ordinary peer handler cannot mint that record kind. Pointers contain no memory content or memory sequence range. Coalesce only pointers for the same verified repository binding. Delete the old row and insert a new inbox sequence in one transaction. Never coalesce ordinary peer messages. Ordinary admission counts ordinary rows; pointer admission counts pointer rows against its separate binding allowance. Neither class consumes the other's quota. Storage failure is reported and preserves pull/sync access where storage remains readable. Receipt of a pointer advances no memory consumer cursor.

## Wakes and delivery

Commit durable state before publishing a wake. Install a subscription before rechecking the durable head and backlog. Repeat on reconnect. Retain a finite recovery check so a lost hint cannot strand existing work. A wake is only a hint to the notifier; it never calls the provider directly.

One bridge notifier may deliver to a participant. Retain the state-directory lock and add an advisory flock keyed by a digest of provider, stable provider namespace, and participant identity. Put lock files in a common private same-user namespace independent of session state roots, supplied by platform_support. Derive the home directory from the operating-system user account for the effective UID, rather than per-agent HOME or state-directory overrides. Use <account-home>/.local/state/koinon-locks on Linux and <account-home>/Library/Application Support/koinon-locks on macOS. These are persistent application-state paths, not temporary/runtime paths selected for automatic cleanup. Validate ownership and private modes. Tests check exact platform selection and independence from HOME, XDG_STATE_HOME, and session/provider home overrides. Tests mock the platform_support account-home resolver to return a temporary directory; subprocess fixtures receive synthetic paths through test-owned harness code. No production environment variable or CLI override changes the common namespace. An explicit test asserts production lookup does not consult those environment overrides. The namespace is a sibling of the default application state tree. Independence means its derivation does not depend on configured state roots; arbitrary --state-dir choices can still overlap it, so do not claim universal path disjointness. Resolve both filesystem paths before testing containment; reject a state root equal to or below the resolved lock namespace, including symlink aliases, and preserve the fixed namespace when a chosen root is an ancestor. Uninstall must never recursively purge arbitrary state roots. Containment uses resolved state-directory paths, unlike peer messaging addresses and registry socket paths, which retain their literal form for token lookup. A missing/unusable operating-system account-home entry causes an explicit startup failure with reason account_home_unavailable. Do not fall back to HOME, silently skip the lock, or emit an unclassified traceback. Correctness requires external tools not to unlink the directory or lock files while in use; arbitrary same-user deletion cannot be prevented by flock. Acquire the state-directory lock first and the participant lock second, both non-blocking. Release acquired resources on failure. Both descriptors must be non-inheritable/close-on-exec, and child launches must not pass them. Add this order to the documented wait graph. Never unlink an active lock file. Derive namespace from verified provider configuration, not a mutable URL or an unverified environment guess. Codex and DeepSeek namespace derivation must be specified and tested before this part is implemented. The lock coordinates this bridge's processes, not unrelated programs that bypass the protocol.

The notifier processes bounded notice units in sequence order. It rereads durable rows on retry and builds fresh content-free notices. A successfully checkpointed unit is not resent merely because a later unit failed. If a pointer is replaced during delivery, its new sequence remains pending. Provider acceptance followed by a crash before checkpointing can cause duplication. A failed provider response can also mean delivery occurred but confirmation was lost.

## Acknowledgement and retry

Keep ack as a bridge-owned operation. In an explicit BEGIN IMMEDIATE transaction containing deletion and the watermark write, persist a monotone acknowledged-through watermark, clamped to the AUTOINCREMENT allocated head read from sqlite_sequence for inbox inside that same BEGIN IMMEDIATE transaction (zero only before any sequence has been allocated). A missing sqlite_sequence row for a never-used inbox means allocated head zero. Ack on that inbox succeeds as a no-op with acknowledgement watermark zero. A database read error is not a missing row. A large caller value must not pre-acknowledge future arrivals. This is bounded metadata; no per-row tombstone list is required. The notifier reads it through its read-only connection. Its journal needs no write transaction shared with the inbox.

Before constructing or retrying a notice, reconcile retained rows and this watermark. An acknowledged failed unit closes as acknowledged, not delivered and not unrecoverable. Unacknowledged retained members remain retryable. Work removed by pointer coalescing is obsolete; the replacement remains pending at its new sequence. Never infer acknowledgement solely from a missing row. Unexpected missing data above the watermark is a distinct diagnostic, not successful delivery.

An ack can still occur after a notifier read or while a provider call is in flight. A content-free notice already in flight can therefore arrive after ack. Do not claim to cancel a provider call atomically with local deletion. On its next reconciliation the journal closes acknowledged work and does not retry it. Tests must force ack before scan, after failure, during notice preparation and during provider delivery. Migration starts the new watermark without inventing historical acknowledgement; an absent old row must not be recreated or replayed.

Create the additive metadata schema in a transaction before bridge readiness, preserving all existing inbox rows and allocated sequence values. Test rollback after deletion but before watermark update, including abrupt process termination. Do not depend on implicit Python sqlite3 transactions.

## Inbox recovery and version compatibility

Retain rollback-journal mode for the inbox in this stage. Its single bridge writer and read-only notifier are different from the memory service's exclusive connection. Do not copy the memory store's WAL/locking settings. Short read snapshots must read rows and acknowledgement metadata consistently, then release the read transaction before any provider call. Set a finite busy timeout as an operational policy, not a maximum filesystem latency claim.

The bridge's writable owner performs recovery after an unclean exit. A notifier encountering a hot journal must not acquire write access or attempt repair; it reports degraded state and waits for bridge recovery. Transient busy/read errors leave checkpoints and dispositions unchanged and trigger bounded-delay retry. Unreadable/corrupt storage is a distinct health failure, never an empty result or notification scan checkpoint reset. Programming defects must remain distinguishable from expected SQLite operational failures. This handling must work without a service manager.

Expose inbox-schema/acknowledgement capability in bridge status. The new notifier checks that capability before querying new metadata. Against an older bridge, retain ordinary-message notification compatibility, report version mismatch for new features, and treat ack attribution as unavailable. A missing watermark table on that older schema means no recorded acknowledgement, not a failed SQL lookup. Do not infer loss or latch a permanent anomaly from disappearing rows in this mode. It must not advance memory consumer cursors or claim new pointer/subscription features are active. In compatibility mode only, a failed unit with no remaining source rows closes as unknown with acknowledgement attribution unavailable. Release its retry payload/individual record after folding that outcome into bounded aggregate health metadata. Do not keep a retryable disposition that cannot be reconstructed. This is neither acknowledgement nor delivery success. If some members remain, retain retry work only for those members while preserving the unknown outcome for removed members. Once the new capability is active, use the inbox acknowledgement watermark; do not retain the permissive compatibility rule.

If a bridge advertises the new schema but its required table is absent, report an integrity/version fault rather than silently accepting a legacy state.

Install/upgrade documentation must require coordinated bridge/notifier restart to activate the new feature set, including manual macOS operation and --no-start installs that leave old processes running. Keep the legacy notification checkpoint intact until its transactional migration completes; preserve the resulting notification scan checkpoint thereafter. Compatibility checks do not create tables from the notifier's read-only connection.

## Observable health

Preserve `notifier_ready` as a generation/liveness/readiness check. Do not turn a provider outage into repeated process replacement by `ensure`. Add a notifier-owned, content-free health snapshot in the configured state directory with owner generation, process-start identity, update time, health state, bounded reason codes and attempt/backlog counters. Publish it atomically and validate its owner/freshness when read. It contains no raw provider errors, credentials, peer content, or participant identifiers.

Session status reports lifecycle and health separately: a live notifier can be running with degraded delivery health. Missing, stale, or mismatched health data reports unknown health, never healthy by inference from liveness. A new notifier explicitly reports storage wait, version compatibility, journal capacity refusal, pending/exhausted delivery failure, or internal fault as applicable. Recovery clears transient reasons only after checking their conditions; unresolved exhausted/unknown delivery remains visible until resolved or explicitly acknowledged as an operator health record. Provide the same readable health result through a notifier status command for manual installations. Internal programming faults cannot be described as invalid peer input.

Tests check all degraded states through the public status surface, recovery to healthy, stale generation rejection, and absence of an ensure restart loop. The health snapshot is a separate bounded JSON file, not a journal row. Journal capacity refusal cannot consume its application-level storage allowance. Bound the encoded snapshot to 4096 bytes by using fixed enums and bounded counters; reject/truncate no arbitrary exception or peer text into it. Publication may temporarily hold the prior 4096-byte file plus one 4096-byte replacement; count both separately from the journal budget. Filesystem allocation/metadata overhead is additional, so 8192 logical bytes is not a physical-disk bound.

A separate file can still fail on filesystem exhaustion, I/O error or permissions. No software reserve guarantees successful writes through those failures. On publication failure, retain the in-memory fault, emit a content-free diagnostic, and retry with bounded delay; do not claim that the new health state was persisted. Consumers treat the previous snapshot as stale/unknown after the configured freshness interval, not indefinitely healthy. A readable status endpoint should expose the live fault where available; if both process access and snapshot access fail, status is unknown/unavailable. Tests inject journal-capacity failure independently of health-file failure, then test full-filesystem/publication failure and stale-snapshot handling. No success guarantee depends on a final write to failed storage.

## Notification journal and migration

Use a separate transactional journal in the state directory; never make the notifier's inbox handle writable. The journal contains the notification scan checkpoint, unit dispositions, attempt reservations, retry state, and degraded-health information. The notification scan checkpoint records the dispositions of enumerated work, not delivery to a model and not an assertion that every historical integer sequence was observed. Rows handled and acknowledged before scanning need no model notice.

Persist an attempt reservation before calling the provider. A crash between reservation and sending consumes an attempt without proving that a send occurred. Recovery treats an in-flight reservation as uncertain; it cannot reset the budget. After the configured automatic attempt budget is exhausted, persist a failed/unknown disposition before advancing the scan past it. Later work may proceed. Retain failed work for inspection and explicit retry while the corresponding inbox work is unacknowledged. Explicit inbox acknowledgement ends notification responsibility for those rows; it must not become a notification failure. Do not report exhausted work as delivered. A provider-wide outage still prevents successful delivery.

Bound journal metadata and represent suitable pending ranges compactly. Coalescing must not erase different outcomes, retry budgets, or unresolved uncertainty. On metadata-capacity refusal, retain backlog, stop unsupported progress, and report degraded health. Notification success does not acknowledge inbox data; unread inbox capacity can still be reached even when delivery works correctly.

Migrate the deployed {thread, through} legacy notification checkpoint without resetting it. Verify target identity before adopting it. A fresh migration imports that checkpoint and records migration completion transactionally with an empty failure history. Preserve the legacy notification checkpoint until the new state is committed and verified. After migration, a missing or unreadable journal is an explicit recovery error; do not restart from zero or invent failure records. Define recovery from the crash windows around first journal creation and migration completion before implementation.

Use a transactional durability mechanism and document its filesystem/platform assumptions. File fsync followed by rename alone is not the design's power-loss guarantee. Initial journal creation and any replacement metadata need directory durability where supported. Do not claim power-loss durability from process-kill tests. Validate the chosen SQLite synchronization mode and metadata publication protocol on supported platforms before claiming that guarantee. Upgrade and uninstall preserve the journal alongside inbox state, in both supported layouts. They also leave the common lock namespace and its files intact, including files used by other sessions. Put this rule in installation/lifecycle documentation; peer requests do not authorize edits to agent instruction files.

The memory database's existing 128 MiB physical limit remains scoped to that repository memory store and its named SQLite files. It is not an aggregate installation or per-participant disk limit. The per-bridge notification journal gets its own separately derived and enforced budget, including its journal/WAL and temporary files; account for the separate health file and its replacement outside this journal budget. Documentation must report the components and their multiplicity; multiple participant journals cannot be hidden inside the memory-store total. Do not reuse memory constants or its write-pattern proof for notifier storage.

The common lock namespace deliberately retains one zero-length lock file per distinct provider/namespace/participant key. Its inode/directory-entry count is not bounded by the journal or memory budget and grows with distinct historical participants. This is an explicit resource tradeoff, not a total-installation disk bound. No automatic reaper is allowed: acquiring then unlinking can split ownership across inodes. Filesystem exhaustion fails notifier startup visibly. Uninstall leaves the namespace intact. Any future bounded replacement requires a separately reviewed ownership protocol; no cleanup heuristic is part of this stage.

## Acceptance matrix

| Area | Required evidence |
| --- | --- |
| Extraction | Real bridge and memory consumer tests; framing limits; auth; literal paths; Linux/macOS aliases |
| Endpoint roles | Fallback control path excluded from messaging; service discovery separate; stale generation/start marker rejected |
| Worker ownership | Actual thread ownership; initialization-before-accept; shutdown with admitted mutation; internal-fault classification |
| Admission | Saturated subscriptions still permit send/status/stop; both inbox quota directions; explicit capacity error |
| Wake ordering | Commit precedes wake; write during subscription setup; lost wake; reconnect and process restart with backlog |
| Slow subscribers | Blocked receiver does not hold database work or unrelated sends; bounded resource cleanup |
| Pointer integrity | Ordinary input cannot mint pointers; repository isolation; atomic delete/insert above notification scan checkpoint |
| Target ownership | Two state roots, same participant: second notifier refused; different participants coexist; fixed non-blocking lock order; lock released after notifier death while a spawned child survives; persistent platform namespace independent of per-agent overrides; resolver mocked for tests so no real account-home writes; paths containing spaces; symlinked state root into lock namespace rejected; missing account-home lookup fails explicitly |
| Retry | Interleaved kinds; later work after durable failure disposition; uncertain provider result; coalescing during retry |
| Checkpoint | Crash after provider acceptance; restart retains attempt reservation; successful prior unit not replayed by later failure |
| Migration | Real legacy notification checkpoint in legacy-root and per-session layouts; target mismatch; crash during creation/migration; missing/corrupt post-migration journal refuses reset |
| Acknowledgement | Ack before scan, after failure, during preparation and provider call; no retry after reconciliation; watermark clamped to sqlite_sequence allocated head; empty/full-drain and repeated ack cases; future arrivals remain unacknowledged; abrupt exit between DELETE and watermark rolls back both |
| Inbox recovery/skew | Busy writer; hot journal; recovery by restarted bridge; no manager present; new notifier with old bridge, including disappeared failed-work rows; coordinated upgrade preserves notification scan checkpoint and inbox; advertised schema with missing table is a fault |
| Durability | Synchronization/configuration checks; publication ordering fault injection; explicitly separate process-crash evidence from power-loss assumptions |
| Capacity | Journal exhaustion; bounded failure representation preserves uncertainty; provider outage distinct from unread inbox saturation |
| Health | Public status exposes each degraded reason; stale snapshot is unknown; generation checked; manual status works; ensure does not restart a live notifier for provider failure; journal-capacity fault with independent health publication; health-file write failure expires to unknown |
| Lifecycle | Upgrade/uninstall preserve journal and inbox; shared lock namespace preserved; runtime state/private identifiers never enter Git |

## Implementation gates

Before code: close participant namespace derivation, exact wire fields, queue/connection/attempt limits, journal schema and migration states, and enforceable storage limits. Constants are policy unless independently enforced; no timing sample is a hard bound. Publish these details with tests in the feature branch from the verified merged develop baseline. This proposal is not production readiness or permission to deploy.

## Concrete gate decisions (review draft)

These decisions apply to new stage 4 behavior. Existing peer envelopes and memory sync,
acknowledgement, and snapshot formats remain unchanged. Transport-helper extraction,
the participant ownership boundary, endpoint-role separation and bridge/memory worker
ownership are implemented on this feature branch. Subscription services and their shared
client helper are reviewed. Explicit pointer controls are implemented in the current
candidate; automatic notifier integration,
the journal and its delivery-health reporting remain pending. This branch is not deployed.

### Participant identity and lock scope

Use the tuple `(provider, "account-local", participant_id)` under the effective OS
account. Both provider adapters currently accept a session identifier without a common,
verified deployment-identity API. Do not manufacture a narrower namespace from CODEX_HOME,
DSH_HOME, a credential pathname, or a URL. All Codex homes for one account share the Codex
namespace; all DeepSeek harnesses for that account share the DeepSeek namespace. Thus
repeated IDs in independent harnesses conflict conservatively, even if they are different
sessions. Report `participant_in_use` with provider, account-local scope, and lock digest; never silently deliver from both. Running notifier status and its readiness record expose the same digest so an operator can match a conflict to a registered instance without publishing the participant identifier. Distinct IDs and
distinct providers coexist. This is an explicit admission policy, not a claim that provider
IDs are globally unique. A future narrower namespace requires a verified provider identity
contract and lock-transition design.

The lock key is SHA-256 of the canonical UTF-8 JSON array above (compact separators),
with nonempty participant identifiers bounded to 512 UTF-8 bytes before hashing. Store
only the digest in filenames. Use `koinon-locks` in the persistent OS-account locations
specified above. State lock precedes participant lock; both nonblocking, close-on-exec.
No published provider endpoint or environment variable can change this exclusion scope.

### Wire fields and endpoint roles

Bridge `status` adds `inbox_schema: 2`, `generation` (32 lowercase hex characters), and
`capabilities: ["inbox_ack_watermark", "inbox_subscription", "memory_binding"]` only
when each corresponding feature is available. A legacy status with none of those fields
is supported for ordinary notices. Feature claims are checked separately from schema.

A dedicated bridge control operation `bind-memory` accepts `repo_path` and
`memory_state_dir`, both explicit filesystem paths. It derives the absolute Git common
directory and memory repository key itself; it does not accept an arbitrary registry
socket as a service. A matching memory owner record and connected kernel PID, nonempty
process-start marker, repository key, service name, protocol, and current generation are
required. The root and repository binding persist; PID, socket and generation are
revalidated on every connection, not persisted as durable authority. `unbind-memory`
accepts the derived binding key; `memory-bindings` lists content-free binding health.
Session registration does not silently bind a memory service.

The memory service is not added to generic messaging discovery. Shared transport has
separate messaging and service validators. Messaging rejects `control.sock` and all
`*-control.sock` names, including fallback aliases, before any connection. A service
endpoint is derived from its configured root, checked for ownership/mode, and verified
by the handshake. A filename alone cannot prove service identity.

Memory subscription request: `{"op":"subscribe","protocol":1,"repo":REPO_KEY,
"generation":GENERATION,"consumer":CONSUMER_KEY}`. Bridge inbox subscription request:
`{"op":"subscribe-inbox","protocol":1,"generation":GENERATION}`. A successful response
uses the existing `ok/result` envelope, with `protocol`, `generation`, and `subscription`
(random 32-hex connection identifier). Subsequent frames contain only `event:"changed"`,
`protocol:1`, `generation`, and `subscription`. They are hints, not sequence receipts.
The memory consumer key is a digest of the configured provider/account-local/session/
repository tuple prefixed `bridge-`; direct CLI consumer keys remain asserted provenance.
Binding does not acknowledge or advance a memory consumer cursor.

An inbox memory pointer is a separate row kind with internal binding key. Its content-free
wire projection has `type:"memory-pointer"` and `binding`; it contains no memory body or
sequence range. The ordinary peer route continues to accept only `user` and inert `control`
frames and cannot produce this kind. Coalescing atomically removes the old pointer and
inserts the replacement with a new inbox sequence. Only a verified bound-service route
can request this mutation. Unbinding removes its pointer as obsolete, not acknowledged.

### Resource and retry policy

Ordinary handlers: 16; pending subscription handshakes: 8; established subscriptions:
32 per service. These are separate pools. A subscription has one queued hint; another
wake coalesces it. Handshake deadline: 5 seconds. Frame write deadline: 5 seconds. There
is no total subscription lifetime deadline. Idle subscribers get a content-free hint
at least every 30 seconds so a disconnected reader is eventually detected. Reconnect
backoff is 1, 2, 4, 8, 16, then 30 seconds; a durable rescan occurs every 2 seconds even
with an open subscription. Each reconnect subscribes before rechecking durable state.
All time values are policies, not bounds on disk latency or model response time.

Ordinary control frames have a separate two-second read deadline and eight pending
slots. An unclassified full pool can refuse any operation until expiry; established
ordinary/subscription handlers do not retain those slots. Status waits one second for
a database read, then returns known identity and queue diagnostics with database fields
unknown. Historical worker faults persist until restart and are distinct from the
notifier delivery-health recovery rules.

Worker admission: 16 queued operations plus one running operation per owning worker.
Status has a separate allowance of two queued operations and priority over queued
ordinary work. Status and stop share two reserved handler slots; stop validates its
instance and signals shutdown on the event loop without database admission. Neither
interrupts the running transaction. Excess work is refused with
`capacity`, not placed on an unbounded executor queue. No DB connection crosses workers.

Bindings: 16 per bridge, with one pointer slot per binding. Ordinary rows retain the
1000-row allowance. Wire limits remain 262144 and 65536 bytes as defined above. Source
scans read at most 128 rows per short snapshot. A notice covers at most ten rows of one
kind; a pointer notice covers one binding and gives the appropriate memory sync command.
Ordinary and pointer records are not combined into a single notice. Pending work is
represented by individual inbox sequence rows in the journal; groups are built for a
provider call, not used to erase different attempt histories.

Provider call deadline: 15 seconds. Automatic budget: three reserved attempts per row;
retry delays: 30 then 60 seconds. Reserve all members in one transaction before calling
the provider. Exhaustion records failure/uncertainty durably and allows later rows to
proceed. A call with ambiguous completion does not claim delivery. Manual retry is an
explicit notifier control operation; it restores a three-attempt budget for selected
retained rows while preserving cumulative uncertainty. Acknowledged rows are not retried.

### Notification journal schema and lifecycle

`notify-journal.sqlite3`, schema 1, separate from the read-only inbox. Tables:
- `meta(key TEXT PRIMARY KEY, value TEXT NOT NULL)` with a fixed key set for schema,
  provider/account-local/target digest, scan checkpoint, and aggregate outcome counters.
- `work(seq INTEGER PRIMARY KEY, kind TEXT NOT NULL, binding TEXT, binding_instance TEXT, disposition TEXT NOT
  NULL, attempts INTEGER NOT NULL, retry_at INTEGER NOT NULL, uncertain INTEGER NOT NULL)`.
- `attempt(id INTEGER PRIMARY KEY, started INTEGER NOT NULL)` and
  `attempt_member(attempt_id INTEGER, seq INTEGER PRIMARY KEY)` for the single in-flight
  provider call. Membership is limited to ten existing work rows. Both attempt tables contain only the current call, not cumulative history. Resolve a call by deleting its attempt and membership rows in the same transaction that updates work disposition, cumulative attempts and uncertainty; recovery does the same for an interrupted call before another reservation. Thus the per-sequence primary key never blocks a later attempt.

Only enum values, bounded identifiers, and signed-64-bit nonnegative counters are stored.
Counters saturate instead of overflowing. No peer content or provider error body enters
the journal. At most 2048 work rows may be retained; 1016 inbox rows can be current, while
extra journal slots permit reconciliation and bounded historical failure accounting.
Successful, acknowledged and obsolete work folds into aggregate counters and is deleted
only in the transaction that saves its disposition and advances eligible scan progress.
Exhausted retained work remains until explicit retry or inbox acknowledgement. The scan
checkpoint may pass it only after the failed/unknown disposition commits.

An explicit control `ack-health` clears accumulated historical health counters; it does
not acknowledge source inbox work, reset retry budgets, or advance a memory cursor.

Before first journal creation, atomically persist `notify-migration.json` with version 1,
state `preparing`, target digest, and imported `through`. The legacy cursor is validated
for target and integer bounds first. Missing legacy state uses the explicit initial
`--after` value, never a guessed checkpoint. The marker and any replacement are each
bounded to 4096 bytes. Refuse an existing unrecognized journal or marker; never adopt it.
Initialize schema, identity and checkpoint in one explicit transaction, fsync required
directory metadata, verify the committed state, then atomically replace the marker with
state `ready`. Do not call a provider until `ready` is durably published.

A well-formed marker with a different target digest is refused in every marker state (preparing, rebuilding, and ready); it is never completed or retargeted.

Recovery states: no marker/no journal permits first migration only when the bridge-owned journal activation record is absent; `preparing` permits creation
or completion only of an empty or exactly matching initialized journal, with no delivery
history; `rebuilding` uses the exact accepted-loss predicate below; `ready` requires an intact matching journal. Here no delivery history means empty work, attempt and attempt_member tables and zero outcome counters, with the scan checkpoint exactly equal to the imported through value (which may be nonzero). Schema, identity, migration nonce and that imported checkpoint are expected metadata, not delivery history. Missing/corrupt ready journal is
`journal_recovery_required` and stops delivery. Journal without marker is also refused.
A preparing marker never permits replay of arbitrary populated tables. Retain the legacy
cursor, never write it after migration, and never use it to recover a missing ready journal.
A crash after initializing the journal but before ready publication cannot have sent a
notice, because delivery has not yet been enabled. Provider calls occur only after durable
attempt reservation. Recovery consumes an unresolved reservation and marks it uncertain.

The migration-marker publication protocol fsyncs the new file, replaces it atomically,
and fsyncs the parent directory. Linux/macOS platform support owns directory-sync and
stronger platform flush details. Durability claims remain conditional on the filesystem
and device honoring synchronization; process-crash tests are not power-loss proof.

### Independent journal storage budget

Use SQLite exclusive WAL mode with 4096-byte pages, `max_page_count=1024`,
`cache_spill=OFF`, `temp_store=MEMORY`, `synchronous=FULL`, and disabled autocheckpoint.
Reject a build that cannot honor in-memory temporary storage. No attached databases, FTS,
VACUUM, incremental vacuum, external readers, or raw user SQL are supported. All schema,
migration, reconciliation, attempt and acknowledgement mutations use one explicit writer
transaction, preceded by a successful verified TRUNCATE checkpoint. On checkpoint failure,
refuse the mutation and report degraded health; never permit a second transaction to grow
the unreset log. Close/recovery follows the same validation before admitting new writes.

For this schema, every modified b-tree page belongs to the same database capped at 1024
pages, with no vacuum relocation or external page source. With cache spill disabled the
commit writes each dirty page once. Carry the separate sector-padding allowance of 16
frames from the pinned SQLite source analysis in STORAGE-BOUND-DERIVATION.md, rather than
copying the memory store's larger constants or workload measurements:

- Database: at most `1024 * 4096 = 4194304` bytes.
- WAL: at most `32 + (1024 + 16) * (4096 + 24) = 4284832` bytes.
- Named SQLite files: at most `8479136` logical file bytes per bridge journal.

This derivation needs tests that enumerate every transaction entry point and verify these
pragmas, reset-before-write, rollback, and peak files under maximal admitted work. Exclusive
WAL keeps the wal-index in heap memory; temporary sort/subjournal storage must remain in
memory. Account for dirty-page and temporary-work memory separately; do not call this a
process-RSS or allocated-disk-sector bound. After a FULL-synchronized WAL commit the latest state can reside in the WAL alone until checkpoint; recovery must preserve the database and its WAL together. Pre-existing stores above the cap are refused,
not silently shrunk or modified. A capacity error preserves source backlog and leaves
health publication possible on its separate budget; it need not guarantee another DB write.

In addition to the SQLite files, count the migration marker plus replacement (8192 bytes)
and health snapshot plus replacement (8192 bytes). Fixed lock files and filesystem metadata
are separate. There is one journal budget per bridge state, one memory budget per repository,
and historical participant-lock inode growth as stated above; there is no aggregate
installation bound. Disk-full or failed synchronization can still stop all writes.

### Public control and inbox schema details

Notifier control uses a separate service endpoint derived with
`control_socket_path(state_dir / "notifier")`. It does not enter messaging discovery.
Requests use the existing `op` and `ok/result` envelope. `status` takes no target-changing
argument and returns lifecycle/delivery health only; `retry` requires an explicit list
of one to ten positive inbox sequences; `ack-health` requires no sequence and returns
the cleared aggregate counters. Responses contain no participant identifier or provider
error body. CLI forms are `notify.py --state-dir STATE status`, `... retry SEQ...`, and
`... ack-health`. Existing invocations without a subcommand remain the notifier start
form and still require the explicit selected participant identity. Status/retry operations
never open the journal directly, so only its owning worker can write it.

Inbox schema 2 keeps the existing inbox columns and adds `kind TEXT NOT NULL DEFAULT
'peer'` and nullable `binding TEXT`, with an additive fixed-key `inbox_meta` table for
schema, `ack_through`, and the journal activation record, plus `memory_binding` with binding key, absolute repository
common-directory path, repository key, and memory state root. Paths are bounded to 4096
UTF-8 bytes each. Binding keys are SHA-256 of the canonical repository key and resolved
memory state root. These binding records are separate from peer envelopes, not claimed
attributes within them. Legacy rows remain kind `peer`; their existing `user`/`control`
frame type retains its original meaning. Inert controls need no notice and advance scan
progress as `ignored`, with a bounded aggregate counter; they are never called delivered.
Schema mutation, retained inbox data, and existing allocated sequence values commit
atomically before readiness. Coalescing/unbinding applies only to kind `memory-pointer`
rows with the selected internal binding key.

The bridge and memory service each retain a reserved two-handler control allowance for
status and stop, apart from ordinary requests and subscription handshakes. Before an
operation is known, the header/frame reader uses a separate bounded handshake pool and
five-second deadline. This avoids admitting arbitrary long-lived connections into the
reserved pool. A caller can still saturate handshakes; the same-user boundary does not
promise service availability against a hostile process of the same user. Tests distinguish
ordinary/subscription saturation from this deliberate same-user denial of service.

The journal fixes `auto_vacuum=NONE` and validates all pragmas on reopen before accepting
writes. Initial creation permits only an empty catalog or the exact recognized schema
without history; arbitrary familiar table names or data without identity are refused.
The preparing marker stores a fresh migration nonce echoed in journal identity. Only the
same nonce and imported checkpoint can complete preparation; a ready marker also retains
the nonce, so a mismatched replacement database cannot be adopted.

After an atomic marker replacement reports an uncertain durability error, startup stops
before any provider call. It rereads and validates on the next startup. A failed provider
attempt-reservation commit similarly prevents the provider call. A failed result commit
leaves the existing reservation uncertain for recovery, never reports delivery success,
and does not proceed to later work until journal state is readable and consistent.

### Explicit recovery and test separation

`notify.py --state-dir STATE --thread TARGET --agent PROVIDER rebuild-journal
--accept-history-loss` is a stopped-notifier maintenance operation. It takes the same
state-then-participant locks, refuses a live notifier, requires matching target evidence from a valid ready marker or the bridge-owned activation record, and requires all prior journal SQLite files to be absent.
If damaged files remain, it refuses and instructs the operator to preserve them outside
the managed state before retrying; it never deletes or copies them silently. Such manual
archives are outside the managed journal budget. No automatic recovery runs this command.

The operator acceptance explicitly permits duplicate notices and loss of old attempt
history. Rebuild reads a consistent current inbox acknowledgement watermark (requires
schema 2), uses it as the new scan checkpoint, and writes a `rebuilding` marker with new
nonce and `history_lost:true` before journal creation. The marker transition follows the
same durable sequence as first migration. A matching rebuilding state permits only empty
work/attempt tables, zero outcome counters, the exact recorded watermark, and the explicit
history_lost identity flag. Ready publication preserves that flag. Subsequent scanning
reconstructs retained work above the watermark. No memory cursor is touched. Delivery
health remains degraded for lost history until explicit `ack-health`; acknowledge does
not undo possible duplicates. A failed rebuild resumes only its exact recorded bootstrap
state, never falls back to the stale legacy cursor. The command refuses an old bridge
without acknowledgement capability and never invents a watermark from missing rows.

Subscription-path tests disable the fallback rescan or set its deadline beyond the test
window. They must prove setup-race, lost-hint/reconnect, and existing-backlog delivery
through the subscription itself. A separate test disables subscriptions and proves the
finite rescan alone recovers retained work. The two paths must not mask each other's
failures. No sub-two-second latency claim is inferred from ordinary fallback-enabled tests.

### Migration evidence outside notifier files

Schema-2 inbox metadata contains a bridge-owned journal activation record (target digest
and migration nonce). `activate-notification-journal` is an explicit control request with
those two fields; the bridge persists it transactionally and acknowledges only after
commit. Repeating the same pair is idempotent. A different target is always refused.
Replacing the nonce requires the explicit rebuild operation with the expected previous
nonce and the operator's accepted-history-loss flag; a normal start cannot replace it.
The notifier reads this evidence through the advertised schema-2 status capability.

After ready-marker publication but before any provider attempt, the notifier registers
activation with the bridge and verifies the result. Thus every migrated installation
that could have sent a notice has evidence in the inbox database as well as the notifier
files. A crash before activation cannot have sent one. A lost activation reply is retried
idempotently; it does not permit delivery by assumption.

Missing marker and journal with a prior activation record is recovery-required, never
first migration from the frozen legacy cursor. Explicit rebuild can use that record as
its target evidence even when the marker is missing. It does not reconstruct old attempt
history or claim that a retained message was never delivered. A new ready journal with
a new nonce replaces activation only through the explicit rebuild transition. If both
the inbox identity evidence and all notifier state are lost, this protocol cannot infer
history; restoring only selected files from inconsistent backups is outside automatic
recovery and requires operator reconciliation.

New journals require the schema-2 activation capability. Against an older bridge, the
notifier retains legacy ordinary-notice behavior and reports version compatibility;
it does not begin journal migration without durable bridge-owned evidence. Migration
also records `journal_required:true` in the retained legacy cursor before any provider
attempt, preserving its target and through fields. A migrated installation without a
usable journal must never resume through that legacy path. Coordinated upgrade is
required; concurrently running an older notifier that overwrites this guard is unsupported.
Existing ready journals can use old-bridge acknowledgement-attribution compatibility,
but are not rebuilt or newly migrated against an old bridge.

Tests delete marker and journal together while retaining the inbox and legacy cursor,
then require explicit recovery with no automatic provider call. They also crash before
and after bridge activation, lose its reply, and test same-pair retry, target mismatch,
and refused nonce replacement outside accepted rebuild.

### Schema-2 candidate wire names

The implemented activation capability is `notification_journal_activation`.
Its normal control is `activate-notification-journal`; explicit evidence replacement
uses `rebuild-notification-journal-activation` with `expected_previous_nonce` and
`accept_history_loss: true`, preserving the target. These are bridge-side operations;
the planned notifier rebuild must still enforce stopped ownership and local evidence
before invoking replacement. The current candidate does not implement that notifier
workflow. Subscription and explicit binding capabilities are implemented separately.


### Binding observation and store replacement

A binding records the last memory store UUID and head for which the bridge created
an inbox pointer. These fields are not memory consumer cursors. Save them in the
same transaction that deletes the old pointer and inserts its replacement. An
unchanged observation does not write, even if the earlier pointer was acknowledged.
The notifier owns per-binding subscriptions and finite recovery scans, with stable
consumer keys derived from its configured provider/account-local/session/repository.
Every recovery scan rereads memory; a missing pointer is not proof of no new work.

Memory schema 4 introduces one durable random 32-hex store UUID. Schema 3 migrates
transactionally; schema 4 with missing or invalid UUID is refused, never re-identified.
The UUID persists across process restarts and changes for a newly created store.
Inbox schema 3 adds bounded observation and diagnostic fields to each of at most
16 bindings, migrating schema 2 transactionally. Older runtimes must refuse these
newer schemas. No data, cursor or notification checkpoint is reset by migration.

The bridge refresh route resolves a persisted binding, verifies the memory owner,
process-start marker, connected PID, repository and generation, then reads store UUID
and head. It accepts no caller-supplied head. It reads memory outside an inbox
transaction, then compares and records exactly that observation inside the pointer
transaction. Memory may advance afterwards; the next hint or finite rescan closes
that window. Missing, blocked or invalid services produce a classified refusal and
leave both pointer and observation unchanged. The wait graph is notifier -> bridge
refresh -> memory read, followed by the bridge's inbox transaction; no transaction
waits for a remote service and memory does not call back into the bridge.

A different store UUID or a regressed head creates a replacement pointer and a
bounded persistent diagnostic, rather than silently suppressing future work. An
unchanged UUID/head does nothing. An identical restored clone with the same UUID
and head cannot be distinguished without content verification; this mechanism does
not claim that property. Binding status exposes observed identity/head and diagnostic
state. It does not persist a counter on every unchanged recovery scan.


Binding-version skew is explicit: a bridge at inbox schema 3 supports the binding
controls but refuses an old memory service with `memory_upgrade_required`. This
capability is service support, not a readiness claim about optional bound services.
Service verification state is a process-local bounded cache (16 entries), expires
after 30 seconds, and begins unknown after restart. Persistent anomaly bits record
store replacement (1) and same-store head regression (2) until `ack-binding-health`.
They do not clear merely because a later refresh succeeds. The current component
exposes explicit refresh only; notifier-driven subscription/scan integration is next.

Journal integration must account for explicit pointers that predate its deployment:
the legacy notifier advances its checkpoint across pointer rows without notifying
them. First journal migration must seed retained pointers once, independently of
the imported ordinary-message checkpoint. Record completion and bounded seed work
transactionally after ready publication and before provider delivery. Repeated
startup must not reseed delivered pointers; ordinary messages must not be replayed.
This integration requirement is pending and must have crash/retry coverage.


Each binding creation has a durable random 32-hex `binding_instance`, separate from
its deterministic binding key and the memory process generation. Idempotent binding
preserves it; unbinding and rebinding produces a new instance. Internal pointer rows
and future journal work retain that instance. A missing pointer whose binding is
absent or has a different instance is obsolete, not unexplained loss. A stale remote
observation from the previous instance cannot write into a recreated binding.
`ack-binding-health` clears anomaly bits without deleting work or changing observed
heads or either acknowledgement position. Rebinding is not a health-clearing procedure.


Binding operation deadlines are derived rather than independently chosen: Git 3s,
memory hello 5s, process-start query 5s, memory status 2s, two socket-cleanup
allowances of 1s, and 2s margin give a 19s server deadline. Binding clients allow
23s, including header/response/cleanup margin. The ordinary handler capacity stays
16, and status/stop keep their reserved slots. A binding timeout is retryable with
unknown mutation outcome. A real regression commits a pointer, delays its result
past that deadline, and verifies the CLI reports uncertainty while the row remains.
A separate successful slow hello/status path exceeds the old 6s deadline.

Process-start probes use two bounded executor slots and do not block the service
event loop. Slots are released by actual completion, not caller cancellation; the
macOS subprocess has a finite 5s policy. This is not a claim that filesystem or
kernel operations have hard physical latency bounds. Notifier binding refresh
clients must use the complete binding client budget, not the generic RPC default.
