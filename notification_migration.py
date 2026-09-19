"""Crash-consistent journal bootstrap under notifier state/participant ownership."""
import os
from pathlib import Path
import sqlite3
import uuid

import durable_state
from inbox_schema import hex_value
import notification_journal as journal
from participant_lock import identity
import platform_support

MARKER_KEYS = {'version', 'state', 'target_digest', 'nonce', 'through', 'history_lost', 'previous_nonce'}


def validate_marker(value, target):
    if (not isinstance(value, dict) or set(value) != MARKER_KEYS
            or type(value['version']) is not int or value['version'] != 1
            or value['state'] not in ('preparing', 'rebuilding', 'ready')
            or not hex_value(value['target_digest'], 64) or not hex_value(value['nonce'], 32)
            or type(value['history_lost']) is not bool
            or (value['previous_nonce'] is not None and not hex_value(value['previous_nonce'], 32))):
        raise journal.JournalError('journal_recovery_required')
    try:
        journal.integer(value['through'])
    except journal.JournalError as exc:
        raise journal.JournalError('journal_recovery_required') from exc
    if value['target_digest'] != target:
        raise journal.JournalError('journal_identity_mismatch')
    if ((value['state'] == 'preparing' and value['history_lost'])
            or (value['state'] == 'rebuilding' and not value['history_lost'])
            or (not value['history_lost'] and value['previous_nonce'] is not None)):
        raise journal.JournalError('journal_recovery_required')
    return value


def read_state(path):
    try:
        return durable_state.read(path)
    except (ValueError, OSError) as exc:
        raise journal.JournalError('journal_recovery_required') from exc


def legacy_cursor(root, participant, after):
    journal.integer(after)
    value = read_state(Path(root) / 'notify-cursor.json')
    if value is None:
        return None
    if (set(value) - {'thread', 'through', 'journal_required'} or not {'thread', 'through'} <= set(value)
            or value['thread'] != participant
            or ('journal_required' in value and type(value['journal_required']) is not bool)):
        raise journal.JournalError('journal_identity_mismatch')
    try:
        journal.integer(value['through'])
    except journal.JournalError as exc:
        raise journal.JournalError('journal_recovery_required') from exc
    return value


def sqlite_files(root):
    path = Path(root) / 'notify-journal.sqlite3'
    return tuple(Path(str(path) + suffix) for suffix in ('', '-wal', '-shm', '-journal'))


def present(path):
    try:
        path.lstat()
        return True
    except FileNotFoundError:
        return False


class Migration:
    """Prepare storage and return the exact activation request before any provider call.

    This owner never performs provider calls or acquires locks in a different order.
    Its caller already holds state ownership followed by participant ownership.
    """
    def __init__(self, root, provider, participant, after, *, capable, activation):
        self.root = Path(root)
        self.provider, self.participant = provider, participant
        self.target = identity(provider, participant)['digest']
        self.capable, self.activation = capable, activation
        self.marker_path = self.root / 'notify-migration.json'
        self.store = None
        if type(capable) is not bool:
            raise journal.JournalError('journal_invalid_capability')
        if activation is not None and (not isinstance(activation, dict)
                or set(activation) != {'target_digest', 'nonce'}
                or not hex_value(activation['target_digest'], 64) or not hex_value(activation['nonce'], 32)):
            raise journal.JournalError('journal_recovery_required')
        if not capable and activation is not None:
            raise journal.JournalError('journal_invalid_capability')
        saved = legacy_cursor(self.root, participant, after)
        marker = read_state(self.marker_path)
        if marker is None:
            if any(present(path) for path in sqlite_files(self.root)) or activation is not None or (saved and saved.get('journal_required')):
                raise journal.JournalError('journal_recovery_required')
            if not capable:
                raise journal.JournalError('journal_upgrade_required')
            marker = dict(version=1, state='preparing', target_digest=self.target,
                          nonce=uuid.uuid4().hex, through=saved['through'] if saved else after,
                          history_lost=False, previous_nonce=None)
            durable_state.publish(self.marker_path, marker)
        self.marker = validate_marker(marker, self.target)
        expected = journal.identity(provider, self.target, marker['nonce'], marker['through'], marker['history_lost'])
        try:
            if marker['state'] != 'ready':
                if not capable:
                    raise journal.JournalError('journal_upgrade_required')
                self.store = journal.Journal(sqlite_files(self.root)[0], expected, create=True, bootstrap=True)
                fd = os.open(sqlite_files(self.root)[0], os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC)
                try:
                    platform_support.sync_state_file(fd)
                    platform_support.sync_state_directory(self.root)
                    # On macOS this requests the device flush after directory metadata
                    # sync too. Process-death tests do not establish power-loss safety.
                    platform_support.sync_state_file(fd)
                finally:
                    os.close(fd)
                self.marker = dict(marker, state='ready')
                durable_state.publish(self.marker_path, self.marker)
            else:
                self.store = journal.Journal(sqlite_files(self.root)[0], expected)
            self.activation_request = self.activation_gate()
            if saved is None:
                saved = dict(thread=participant, through=marker['through'])
            if not marker['history_lost'] and saved['through'] != marker['through']:
                raise journal.JournalError('journal_identity_mismatch')
            if not saved.get('journal_required'):
                durable_state.publish(self.root / 'notify-cursor.json', dict(saved, journal_required=True))
        except BaseException as exc:
            if self.store is not None:
                self.store.close()
                self.store = None
            if isinstance(exc, sqlite3.ProgrammingError):
                raise
            if isinstance(exc, (sqlite3.DatabaseError, FileNotFoundError)):
                raise journal.JournalError('journal_recovery_required') from exc
            raise

    def close(self):
        if self.store is not None:
            self.store.close()
            self.store = None

    def activation_gate(self):
        state = self.store.meta()
        wanted = dict(target_digest=self.target, nonce=self.marker['nonce'])
        if not self.capable:
            if not state['activation_confirmed']:
                raise journal.JournalError('journal_upgrade_required')
            return None
        # Do not register first: that could recreate and conceal lost inbox evidence.
        if state['activation_confirmed'] and self.activation != wanted:
            raise journal.JournalError('journal_recovery_required')
        request = dict(op='activate-notification-journal', **wanted)
        if self.activation is not None and self.activation != wanted:
            if (self.activation['target_digest'] != self.target or not self.marker['history_lost']
                    or self.marker['previous_nonce'] != self.activation['nonce']):
                raise journal.JournalError('journal_recovery_required')
            request = dict(op='rebuild-notification-journal-activation', **wanted,
                           expected_previous_nonce=self.marker['previous_nonce'], accept_history_loss=True)
        return request

    def confirm_activation(self, evidence):
        if self.activation_request is None:
            raise journal.JournalError('journal_invalid_activation')
        # Repeating this commit is safe after a lost reply because bridge activation
        # of the same target/nonce pair is idempotent.
        self.store.confirm_activation(evidence)

    @classmethod
    def rebuild(cls, root, provider, participant, *, accept_history_loss, capable, activation, ack_through):
        if accept_history_loss is not True or capable is not True:
            raise journal.JournalError('journal_rebuild_refused')
        journal.integer(ack_through)
        root = Path(root)
        target = identity(provider, participant)['digest']
        marker_path = root / 'notify-migration.json'
        marker = read_state(marker_path)
        if marker is not None:
            validate_marker(marker, target)
        if marker is not None and marker['state'] == 'rebuilding':
            return cls(root, provider, participant, ack_through, capable=capable, activation=activation)
        if any(present(path) for path in sqlite_files(root)):
            raise journal.JournalError('journal_preserve_existing_files')
        if activation is not None:
            if (not isinstance(activation, dict) or set(activation) != {'target_digest', 'nonce'}
                    or activation['target_digest'] != target or not hex_value(activation['nonce'], 32)):
                raise journal.JournalError('journal_identity_mismatch')
        elif marker is None or marker['state'] != 'ready':
            raise journal.JournalError('journal_recovery_required')
        marker = dict(version=1, state='rebuilding', target_digest=target, nonce=uuid.uuid4().hex,
                      through=ack_through, history_lost=True,
                      previous_nonce=activation['nonce'] if activation is not None else None)
        durable_state.publish(marker_path, marker)
        return cls(root, provider, participant, ack_through, capable=capable, activation=activation)
