# Notifier operation and recovery

The notifier serves one explicitly selected participant. Its state-directory and
account-local participant locks remain held until shutdown finishes. A provider
failure changes delivery health; it does not remove lifecycle readiness or authorize
a second notifier. `session.py ensure` preserves a live instance with degraded delivery.
A failed control probe alone does not prove the instance is stopped.

## Delivery

The notifier subscribes before checking the durable inbox. It repeats that check
on reconnect and at a two-second recovery interval. Memory bindings have separate
subscriptions and recovery checks. These intervals are scheduling policies, not
hard disk or catch-up deadlines. Status and stop have reserved admission capacity.

A notice contains at most ten ordinary message pointers or one memory pointer.
Peer bodies and memory content never enter the provider request. Memory notices
name the exact configured service root with `memory.py --service-dir`, the repository
path and a stable consumer ID. Reading the notice advances no memory cursor. The
consumer must explicitly sync and acknowledge pages. The service verifies the
repository and generation before acting on an exact-path request.

Each journal reservation commits before provider I/O and consumes one attempt.
There are three automatic attempts per unit, with 30- and 60-second retry delays.
A failed launch is known non-delivery. Timeout, interrupted confirmation and an
ambiguous provider exit are unknown outcomes. Provider processes have a 15-second
deadline and are reaped before the next invocation. Process creation and cleanup
can take additional time. The deadline is not proof that the provider rejected a request.

The journal stores outcomes separately from the inbox acknowledgement watermark
and memory consumer cursors. A successful checkpointed unit is not resent because
a later unit failed. An unresolved committed reservation becomes unknown after
restart; it does not regain its consumed attempt. Duplicates remain possible after
provider acceptance and a crash before outcome persistence. Model processing is a
separate stage which this bridge cannot infer from provider acceptance.

## Controls and health

Run commands against the same state directory as the bridge:

```sh
python3 notify.py --state-dir /private/bridge-state status
python3 notify.py --state-dir /private/bridge-state retry 17 18
python3 notify.py --state-dir /private/bridge-state ack-health
python3 notify.py --state-dir /private/bridge-state stop
```

`retry` selects one to ten retained exhausted units. It reconciles current source
state first, and cannot revive acknowledged or obsolete work. It resets the selected
automatic budgets without erasing prior uncertainty or cumulative accounting.
`ack-health` clears diagnostic aggregates and acknowledges the history-loss warning.
It does not acknowledge inbox messages, change memory cursors, reset retry budgets
or clear uncertainty on retained work. Stop commands are for controlled maintenance;
stop the managing supervisor first to prevent its service policy from restarting a child.

Status separates lifecycle from delivery health. Delivery is degraded while work
is pending, exhausted or uncertain, or when a storage, compatibility or optional
memory fault is observed. Pending work can be normal backlog. Unknown health means
fresh facts were unavailable. A journal observation times out after one second;
status does not reuse an old healthy journal summary as a current result.

The independent health worker publishes a bounded snapshot every two seconds.
`session.py status` and a notifier status fallback accept it only with a verified
live readiness owner and a snapshot no older than 15 seconds. Missing, stale,
malformed or mismatched files report unknown. A publication error remains visible
in live diagnostics; an old file expires even if storage cannot accept a final
failure record. None of these states alone triggers a new notifier.

## Upgrade and compatibility

Stop the bridge, notifier and their supervisor before installing an upgrade. Keep
the same target and state paths. Preserve all inbox, checkpoint, journal and migration
files. Restart both services with the new code. Optional memory services must also
be upgraded for guarded exact-path sync commands.

The first journal migration imports the validated legacy `notify-cursor.json`
position. It writes durable preparation evidence, creates the bounded journal,
records activation in the bridge and reads that evidence back before delivery.
The legacy cursor becomes a frozen guard against silent downgrade. Startup never
reimports it to replace a missing or corrupt activated journal.

With an older bridge, a state directory with no migration or activation evidence
can use ordinary-message legacy compatibility. That mode reports degraded health,
does not send memory pointers and has no durable attempt accounting. An intact,
previously activated journal can use its documented reduced-capability source
mode. Missing acknowledgement or pointer evidence is never invented. Upgrade both
services to obtain the full protocol.

Retained sockets and registry records cause a named configuration refusal (exit 78).
Verify ownership, process death and refused connections before removing specific
stale endpoints. Never clear shared directories. Preserve journal and inbox files.

## Explicit journal recovery

A journal operator-action refusal or invalid source metadata stops further delivery
attempts in that notifier process. Status and stop remain available. Repair requires
stopped ownership and an explicit restart; files must not be repaired under a live
journal owner. Transient SQLite BUSY/LOCKED errors keep the normal recovery cadence.
Other disk errors are reported as storage faults, never as successful delivery.

First inspect and back up all state. Stop the supervisor and notifier and establish
that their ownership locks are free. Keep or start only the corresponding upgraded
bridge for this maintenance operation. Use the original provider and participant.

A damaged journal requires operator inspection. Preserve its SQLite main file and
any sidecars together outside their active names, while the notifier is stopped.
The rebuild command refuses existing journal files; it never deletes them. Keep the
migration marker, legacy cursor, inbox and bridge activation evidence in place.

Only after accepting the loss of notification history, run:

```sh
python3 notify.py --state-dir /private/bridge-state --agent codex \
  --thread EXACT_EXISTING_THREAD rebuild-journal --accept-history-loss
```

The command acquires both notifier locks and verifies the bridge identity. It uses
the recorded inbox acknowledgement watermark for a fresh scan, stores a new nonce,
changes activation through the guarded rebuild transition and verifies source evidence.
No provider is called. `history_lost` records the accepted loss; a later health
acknowledgement leaves this immutable identity intact. Retained work can be notified
again, including memory pointers older than the imported scan position.

An interrupted preparation resumes its recorded nonce and watermark. If preparation
already reached ready state, normal notifier startup completes any pending activation.
Do not delete files to force a second rebuild. Restore the normal service arrangement
after maintenance and verify status and authorized delivery in both directions.
