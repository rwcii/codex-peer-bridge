"""Worker-owned journal and read-only source; provider I/O stays on the caller."""
from pathlib import Path

from notification_journal import JournalError
from notification_migration import Migration
from notification_source import InboxSource


class NotificationState:
    """Construct, call and close only on one DatabaseWorker thread.

    The caller holds notifier ownership and supplies capabilities from the verified
    bridge process. Activation evidence is then read from the same inbox snapshot
    as source metadata, before any activation control request can mask missing data.
    """
    def __init__(self, root, provider, participant, after, schema, capabilities):
        self.source = self.migration = None
        self.capable = 'notification_journal_activation' in capabilities
        if self.capable and 'inbox_ack_watermark' not in capabilities:
            raise JournalError('journal_invalid_capability')
        try:
            self.source = InboxSource(Path(root) / 'inbox.sqlite3', schema=schema,
                                      capabilities=capabilities)
            snapshot = self.source.retained([])
            self.migration = Migration(root, provider, participant, after,
                                       capable=self.capable, activation=snapshot['activation'])
        except BaseException:
            self.close()
            raise

    @property
    def journal(self):
        return self.migration.store

    def close(self):
        try:
            if self.migration is not None:
                self.migration.close()
        finally:
            if self.source is not None:
                self.source.close()
                self.source = None

    def activation(self):
        return self.migration.activation_request

    def confirm_activation(self, evidence):
        # Read back the bridge-owned committed record. A successful control reply
        # alone must not turn a different source database into an activated one.
        snapshot = self.source.retained([])
        if snapshot['activation'] != evidence:
            raise JournalError('journal_activation_mismatch')
        self.migration.confirm_activation(evidence)

    def ready(self, now):
        """Run once per serial delivery owner, before any new reservation."""
        self.journal.recover_attempt(now)
        if not self.journal.meta()['pointers_seeded']:
            seed = self.source.seed_pointers() if self.source.pointers else dict(records=[])
            self.journal.seed_pointers(seed)
        return self.status()

    def reconcile(self):
        retained = self.source.retained([row['seq'] for row in self.journal.rows()])
        self.journal.reconcile(retained)

    def prepare(self, now):
        self.reconcile()
        snapshot = self.source.scan(self.journal.meta()['enumerated_through'])
        admission = None
        try:
            self.journal.ingest(snapshot)
        except JournalError as exc:
            if exc.code != 'journal_capacity':
                raise
            # Existing admitted work can still complete and free capacity. The
            # refused scan transaction has not advanced either progress boundary.
            admission = exc.code
        return dict(sequences=self.journal.due(now, memory_available=self.source.pointers),
                    admission=admission, **self.status())

    def reserve_next(self, now):
        # This second read closes the preparation/render scheduling window. An ack
        # after the reservation can still race a provider call, as documented.
        self.reconcile()
        sequences = self.journal.due(now, memory_available=self.source.pointers)
        return self.journal.reserve(sequences, now) if sequences else []

    def resolve(self, outcome, now):
        self.journal.resolve(outcome, now)
        return self.status()

    def retry(self, sequences):
        # An explicit retry must never revive a source row already acknowledged.
        self.reconcile()
        self.journal.retry(sequences)
        return self.status()

    def acknowledge_health(self):
        return self.journal.acknowledge_health()

    def status(self):
        return dict(journal=self.journal.status(), compatibility=not self.source.acknowledgements,
                    memory_available=self.source.pointers)
