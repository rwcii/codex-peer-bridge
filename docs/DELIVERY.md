# Delivery evidence and presence

Delivery records are local to one bridge installation. A sender can inspect its
transport attempt; the receiving installation can inspect stored, notified,
fetched and handled evidence. This release sends no native wire receipts, so a
remote sender cannot infer receiver stages from a completed socket exchange.

```sh
python3 bridge.py delivery --seq 42
python3 bridge.py handled 42 --outcome refused
python3 bridge.py delivery --key OUTGOING_KEY
```

`handled` requires `done`, `failed`, or `refused`. Repeating the same outcome is
idempotent; a different outcome is refused and the first remains final. It works
after `ack` while the receipt is retained. `ack` only deletes inbox entries.

## What each stage proves

| Stage | Evidence | Limit |
| --- | --- | --- |
| transport | Outbound socket write and connection completion | Does not prove receiver persistence |
| stored | Inbox and receipt committed in one transaction | Does not prove notification |
| notified | Provider result and per-sequence evidence committed to the notifier journal | `outcome: delivered` means provider acceptance; `unknown` means an indeterminate attempt |
| fetched | Inbox control reply drained successfully, then evidence committed | Server output, not model comprehension |
| handled | Explicit local participant outcome | A same-user report, not authenticated human approval |

A missing stage is unknown. Stages are not inferred from one another. A provider
timeout is never reported as confirmed notification. A later confirmed provider
acceptance can upgrade an earlier unknown outcome. Codex acceptance means a
successful `codex queue` call; DeepSeek acceptance means the queue adapter accepted
`session/prompt`. Neither establishes model receipt latency or task success.

An inbox read returns entries even if recording its subsequent fetched evidence
fails. The service emits a content-free diagnostic and the receipt retains no new
fetched claim. A crash after output and before evidence commit also leaves that
stage unknown. Direct database inspection and notifier scans do not mark fetched.

Notification results enter a bounded durable outbox in the same transaction as
journal resolution, before delivered work is pruned. The notifier exports a batch
of at most ten records through the private bridge control socket, guarded by the
journal's target digest and activation nonce. A lost reply repeats this idempotent
handoff, not the peer message. The outbox holds at most 2,048 rows. Full capacity
refuses new notification reservations with `notified_outbox_full`; it never evicts
unreported evidence. Pending handoffs appear as `pending_receipt_evidence` in
health. Export resumes on recovery. Evidence whose receiving ledger record has
already expired is counted as `receipt_unrecorded`; health reports
`receipt_evidence_unrecorded` until explicitly acknowledged with `ack-health`.

A crash before the provider outcome reaches the journal retains only an unknown
attempt. Existing bounded retries may repeat a content-free pointer notice. They
never replay native user frames. Controls, including native receipt-shaped frames,
remain inert, are excluded from notices, and generate no answering receipt.

## Identity, retries and limits

For received frames with a bounded `msg_id`, the deduplication key covers the
observed kernel PID and process-start marker, the persistent recipient installation
ID, and that message ID. The marker is read while processing the connection; if
it is unavailable, the frame can still be stored but its receipt explicitly says
deduplication is unavailable. This identifies an observed native process lifetime,
not an agent across restarts. It does not turn an asserted sender into authority.

The key is bound to SHA-256 of the complete canonical JSON frame: keys sorted,
compact separators, non-finite numbers refused. This includes message and
attachment metadata, priority and asserted return address. A changed frame with
the same key is refused as `delivery_payload_conflict`; the first frame stays
unchanged and its receipt records a bounded conflict count and timestamps. No
attachment is fetched. Frames without a usable ID get sequence-local evidence.

Receipt retention is at least 24 hours after creation or the latest new stage,
and always while the corresponding inbox entry remains. Deduplication survives
acknowledgement. After both retention and inbox ownership end, a native frame may
be accepted again: the bridge does not know a native sender's retry policy.
Never assume native peers deduplicate an uncertain send.

There are at most 10,000 incoming and outgoing records combined. Record JSON is
limited to 4,096 UTF-8 bytes; internal keys and fingerprints have fixed bounded
lengths. The JSON allowance is therefore at most 40,960,000 bytes, plus bounded
SQLite row/index overhead; this is not a physical database size guarantee.
Admission prunes eligible expired records and otherwise refuses with
`delivery_capacity`. It never evicts an unexpired key. Inbox capacity remains
1,000 peer entries. Original message bodies are not copied into the ledger.

For a repeatable outgoing command, choose both identifiers before sending:

```sh
python3 bridge.py send uds:/tmp/cc-socks/12345.sock 'Hello' \
  --msg-id YOUR_UNIQUE_ID --deadline ABSOLUTE_EPOCH_SECONDS
```

The deadline must be in the future and no more than 24 hours ahead. Reuse it
unchanged with the same ID, target, body and priority when checking an uncertain
attempt. One durable reservation precedes socket I/O. Repeating it returns the
recorded outcome and **never sends a second native frame**, including after an
indeterminate result or restart. Changed parameters conflict. Expired deadlines
are refused. A genuinely new send requires a new ID and an explicit decision about
possible duplicate work. No automatic native replay is implemented.

Without supplied identifiers, the CLI creates an ID and a five-minute deadline
and includes them in its JSON response, including ambiguous reply errors. The
socket attempt lasts at most four seconds, bounded further by the deadline.
An indeterminate send returns nonzero CLI status; it does not mean the receiver
failed. Receipt lookup is separate from replay. No new fields are added to native
wire frames; the random persistent installation ID stays local.

## Presence and priority

`status` separates service observation from model activity. A live control reply
proves that adapter is reachable at the observation time. `peers` verifies a live
process-start marker, which proves process liveness, not socket reachability.
Every presence value carries source, observation time and a 15-second freshness
window. Unknown activity has no fabricated observation. Consumers must expire
cached observations; an old successful status response does not survive a disconnect.

For Claude `cli` records, a fresh `statusUpdatedAt` with `busy`, `shell`, `idle`, or
`waiting` provides registry activity evidence. `shell` maps to busy. Waiting does
not distinguish approval from user-input waits. Missing, future-dated, stale or
unrecognized evidence is unknown. Koinon's own daemon registry record omits
activity instead of permanently asserting waiting. Claude's UI may display Idle
for an absent status; that UI fallback is not a Koinon activity claim.

Codex and DeepSeek activity remains unknown: no verified read-only source owned by
the selected participant is integrated. Codex app-server schemas expose status and
wait flags, but schema availability does not prove that connecting observes the
actual owning server without side effects. No thread is resumed, replaced, loaded
or subscribed as part of this feature.

The notifier's `priority` declaration states that `now`, `next` and `later` are
preserved in inbox frames, while the Codex and DeepSeek content-free queue adapters
have no scheduling-priority mapping. A peer priority grants no permission. Native
Claude 2.1.276 parses these values; running-model scheduling and interruption
semantics are unverified. Native receipt controls also remain disabled despite
observed parser support. Schema support, endpoint reachability and live
verification are distinct fields; none substitutes for another.

## Compatibility and verification limits

Inbox schema 4 adds the persistent identity and ledger atomically. Retained older
rows gain only `stored` evidence marked `pre_migration`, without an invented sender
lifetime or historical notified/fetched/handled stage. The notifier journal migrates
from schema 1 to 2 atomically, retaining work, attempts, activation and counters.
Old binaries refuse the newer schemas; rollback requires the stopped-state backup
procedure in [INSTALL.md](INSTALL.md#delivery-evidence-upgrade).

A new notifier can read inbox schemas 2 and 3 without exporting stages when the
bridge lacks `delivery_ledger`; it does not manufacture evidence or accumulate a
new unsupported export backlog. An already running old notifier may refuse a
new inbox schema rather than lose state. Upgrade the pair together. A new bridge
cannot infer notification evidence from an old notifier's checkpoint.

The Claude registry status enum and native receipt/priority parsers were inspected
in version 2.1.276. They are undocumented, version-dependent interfaces. The
status-omission discovery fixture has not run: its shared-registry mutation was
rejected by the reviewer's automatic approval layer. This remains a release gate,
not a verified compatibility claim. No runtime upgrade has been performed for
this feature.

An offline check traced the actual daemon reader and `listLivePeerSessions` path
in Claude 2.1.276. Extracted parser and listing functions accepted both an
in-memory daemon record with `idle` and one with status omitted. Liveness and
socket reachability were supplied by synthetic stubs; no registry files, processes,
native CLI instances or model sessions were created. The extracted function set
had SHA-256 `67781457284b87697cde91432429ac5aec7b97fdd6937ef585b12df57a08379a`.
This establishes parser/filter acceptance, not end-to-end native discovery. The
live gate remains open. Vendor implementation text is not included in this repository.
