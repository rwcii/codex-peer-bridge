"""Real child-process death at durable boundaries; not a power-loss simulation."""
from contextlib import closing
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import durable_state
import notification_migration as migration
from participant_lock import identity

PARTICIPANT = 'synthetic-crash-session'
TARGET = identity('codex', PARTICIPANT)['digest']

# Each child dies without Python finally blocks or SQLite connection cleanup.
# The file representing bridge evidence is synthetic; no service or peer is used.
CHILD = r'''
import os
from pathlib import Path
import sqlite3
import sys
import durable_state
import notification_migration as migration

root, boundary = Path(sys.argv[1]), sys.argv[2]
participant = 'synthetic-crash-session'
phase = 'schema'
base_connect = sqlite3.connect
class Connection(sqlite3.Connection):
    def commit(self):
        if boundary == phase + '-before-commit':
            os._exit(73)
        super().commit()
        if boundary == phase + '-after-commit':
            os._exit(73)
def connect(*args, **kwargs):
    return base_connect(*args, factory=Connection, **kwargs)
sqlite3.connect = connect
base_publish = durable_state.publish
def publish(path, value):
    base_publish(path, value)
    if path.name == 'notify-migration.json' and boundary == value['state']:
        os._exit(73)
durable_state.publish = publish
owner = migration.Migration(root, 'codex', participant, 17, capable=True, activation=None)
evidence = dict(target_digest=owner.target, nonce=owner.marker['nonce'])
durable_state.publish(root / 'synthetic-evidence.json', evidence)
if boundary == 'activation-reply-lost':
    os._exit(73)
phase = 'activation'
owner.confirm_activation(evidence)
phase = 'seed'
owner.store.seed_pointers(dict(records=[]))
phase = 'ingest'
owner.store.ingest(dict(through=19, records=[dict(seq=seq, kind='peer', binding=None,
                          binding_instance=None) for seq in (18, 19)]))
phase = 'reservation'
owner.store.reserve([18], 10)
if boundary == 'provider-accepted':
    os._exit(73)
phase = 'result'
owner.store.resolve('delivered', 11)
phase = 'second-reservation'
owner.store.reserve([19], 12)
os._exit(74)
'''


class ProcessDeathTests(unittest.TestCase):
    def crash(self, boundary):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name)
        result = subprocess.run([sys.executable, '-c', CHILD, str(root), boundary],
                                cwd=Path(__file__).resolve().parent, capture_output=True,
                                text=True, timeout=15)
        self.assertEqual(result.returncode, 73, result.stderr)
        # Exclusive WAL must not create a shared-memory file, even before recovery.
        self.assertFalse((root / 'notify-journal.sqlite3-shm').exists())
        return root

    def reopen(self, root):
        return migration.Migration(root, 'codex', PARTICIPANT, 0, capable=True,
                                   activation=durable_state.read(root / 'synthetic-evidence.json'))

    def test_bootstrap_death_preserves_import_and_nonce_at_each_boundary(self):
        for boundary in ('preparing', 'schema-before-commit', 'schema-after-commit', 'ready'):
            with self.subTest(boundary=boundary):
                root = self.crash(boundary)
                marker = durable_state.read(root / 'notify-migration.json')
                with closing(self.reopen(root)) as owner:
                    self.assertEqual(owner.marker['nonce'], marker['nonce'])
                    self.assertEqual(owner.store.meta()['scan_through'], 17)
                    self.assertFalse(owner.store.meta()['activation_confirmed'])
                    self.assertEqual(owner.store.rows(), [])
                    self.assertFalse(any(owner.store.meta()['counters'].values()))
                    self.assertFalse((root / 'notify-journal.sqlite3-shm').exists())

    def test_activation_death_repeats_same_pair_without_inventing_confirmation(self):
        for boundary, confirmed in (('activation-reply-lost', False),
                                    ('activation-before-commit', False),
                                    ('activation-after-commit', True)):
            with self.subTest(boundary=boundary):
                root = self.crash(boundary)
                evidence = durable_state.read(root / 'synthetic-evidence.json')
                with closing(self.reopen(root)) as owner:
                    self.assertEqual(owner.activation_request,
                                     dict(op='activate-notification-journal', **evidence))
                    self.assertEqual(owner.store.meta()['activation_confirmed'], confirmed)
                    owner.confirm_activation(evidence)
                    self.assertTrue(owner.store.meta()['activation_confirmed'])
                    self.assertEqual(owner.store.meta()['scan_through'], 17)

    def test_uncommitted_reservation_does_not_consume_an_attempt(self):
        root = self.crash('reservation-before-commit')
        with closing(self.reopen(root)) as owner:
            self.assertFalse(owner.store.recover_attempt(20))
            self.assertEqual(owner.store.meta()['counters']['attempts'], 0)
            self.assertEqual(owner.store.due(20), [18, 19])

    def test_committed_reservation_consumes_budget_before_and_after_provider_acceptance(self):
        for boundary in ('reservation-after-commit', 'provider-accepted', 'result-before-commit'):
            with self.subTest(boundary=boundary):
                root = self.crash(boundary)
                with closing(self.reopen(root)) as owner:
                    self.assertTrue(owner.store.recover_attempt(20))
                    first = owner.store.rows()[0]
                    self.assertEqual((first['attempts'], first['uncertain'], first['retry_at']), (1, 1, 50))
                    self.assertEqual(owner.store.meta()['scan_through'], 17)
                    self.assertEqual(owner.store.meta()['counters']['delivered'], 0)
                    self.assertFalse(owner.store.recover_attempt(21))

    def test_committed_success_survives_death_and_later_unresolved_work(self):
        for boundary in ('result-after-commit', 'second-reservation-after-commit'):
            with self.subTest(boundary=boundary):
                root = self.crash(boundary)
                with closing(self.reopen(root)) as owner:
                    owner.store.recover_attempt(20)
                    self.assertEqual(owner.store.meta()['scan_through'], 18)
                    self.assertEqual(owner.store.meta()['counters']['delivered'], 1)
                    self.assertEqual([row['seq'] for row in owner.store.rows()], [19])
                    self.assertFalse((root / 'notify-journal.sqlite3-shm').exists())


if __name__ == '__main__':
    unittest.main()
