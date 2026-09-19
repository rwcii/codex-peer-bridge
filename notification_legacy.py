"""Explicit old-bridge compatibility; never used to recover a migrated journal."""
from pathlib import Path

import durable_state
from notification_journal import JournalError, integer
from notification_migration import legacy_cursor, present, sqlite_files
from notification_source import InboxSource


class LegacyState:
    """Single-worker legacy checkpoint, without claims of journal attempt history."""
    def __init__(self, root, participant, after, schema, capabilities):
        self.root, self.participant = Path(root), participant
        self.cursor_path = self.root / 'notify-cursor.json'
        if 'memory_binding' in capabilities:
            raise JournalError('journal_upgrade_required')
        saved = legacy_cursor(root, participant, after)
        if (present(self.root / 'notify-migration.json')
                or any(present(path) for path in sqlite_files(root))
                or (saved is not None and saved.get('journal_required'))):
            raise JournalError('journal_recovery_required')
        self.after = saved['through'] if saved is not None else after
        self.source = InboxSource(self.root / 'inbox.sqlite3', schema=schema, capabilities=capabilities)
        try:
            if self.source.retained([])['activation'] is not None:
                raise JournalError('journal_recovery_required')
        except BaseException:
            self.source.close()
            raise
        self.retry_at = 0
        self.inflight = None

    def close(self):
        self.source.close()

    def activation(self):
        return None

    def ready(self, now):
        integer(now)
        if self.inflight is not None:
            self.retry_at = now + 30
            self.inflight = None
        saved = legacy_cursor(self.root, self.participant, self.after)
        if saved is not None:
            if saved.get('journal_required') or saved['through'] < self.after:
                raise JournalError('journal_recovery_required')
            self.after = saved['through']
        return self.status()

    def save(self, through):
        durable_state.publish(self.cursor_path, dict(thread=self.participant, through=through))
        self.after = through

    def selection(self):
        snapshot = self.source.scan(self.after)
        rows = snapshot['records'][:10]
        through = snapshot['through'] if len(snapshot['records']) <= 10 else rows[-1]['seq']
        return rows, through

    def prepare(self, now):
        integer(now)
        if now < self.retry_at:
            return dict(sequences=[], admission=None, **self.status())
        rows, through = self.selection()
        if not rows and through > self.after:
            self.save(through)
        return dict(sequences=[row['seq'] for row in rows], admission=None, **self.status())

    def reserve_next(self, now):
        integer(now)
        if self.inflight is not None:
            raise JournalError('journal_attempt_in_progress')
        if now < self.retry_at:
            return []
        rows, through = self.selection()
        if rows:
            self.inflight = through
        elif through > self.after:
            self.save(through)
        return rows

    def resolve(self, outcome, now):
        integer(now)
        if self.inflight is None:
            raise JournalError('journal_no_attempt')
        if outcome not in ('delivered', 'failed', 'unknown'):
            raise JournalError('journal_invalid_outcome')
        if outcome == 'delivered':
            self.save(self.inflight)
            self.retry_at = 0
        else:
            self.retry_at = now + 30
        self.inflight = None
        return self.status()

    def retry(self, sequences):
        raise JournalError('journal_upgrade_required')

    def acknowledge_health(self):
        raise JournalError('journal_upgrade_required')

    def status(self):
        return dict(journal=None, compatibility=True, memory_available=False,
                    legacy_checkpoint=self.after)
