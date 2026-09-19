from contextlib import closing
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

import durable_state
from notification_journal import JournalError
from notification_legacy import LegacyState


class LegacyCompatibilityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.db = sqlite3.connect(self.root / 'inbox.sqlite3')
        self.addCleanup(self.db.close)
        self.db.execute('CREATE TABLE inbox(seq INTEGER PRIMARY KEY,pid INTEGER,frame TEXT)')
        for seq in range(1, 14):
            self.db.execute('INSERT INTO inbox VALUES(?,?,?)', (seq, 7,
                json.dumps(dict(type='control' if seq == 13 else 'user', message=dict(content='private synthetic')))))
        self.db.commit()

    def open(self):
        return LegacyState(self.root, 'synthetic-session', 0, None, [])

    def test_memory_capability_cannot_silently_use_ordinary_legacy_mode(self):
        with self.assertRaises(JournalError) as raised:
            LegacyState(self.root, 'synthetic-session', 0, 3, ['memory_binding'])
        self.assertEqual(str(raised.exception), 'journal_upgrade_required')
        self.assertFalse((self.root / 'notify-cursor.json').exists())

    def test_ordinary_compatibility_uses_bounded_groups_and_success_only_checkpoint(self):
        with closing(self.open()) as state:
            self.assertEqual(state.prepare(0)['sequences'], list(range(1, 11)))
            self.assertEqual(len(state.reserve_next(0)), 10)
            state.resolve('unknown', 0)
            self.assertEqual(state.status()['legacy_checkpoint'], 0)
            self.assertEqual(state.prepare(29)['sequences'], [])
            state.reserve_next(30)
            state.resolve('delivered', 30)
            self.assertEqual(state.status()['legacy_checkpoint'], 10)
            self.assertEqual(state.prepare(31)['sequences'], [11, 12])
            state.reserve_next(31)
            state.resolve('delivered', 31)
            self.assertEqual(state.status()['legacy_checkpoint'], 13)
            self.assertIsNone(state.status()['journal'])
        self.assertFalse((self.root / 'notify-journal.sqlite3').exists())
        self.assertEqual(durable_state.read(self.root / 'notify-cursor.json')['through'], 13)

    def test_migration_evidence_always_refuses_legacy_fallback(self):
        for filename in ('notify-migration.json', 'notify-journal.sqlite3', 'notify-journal.sqlite3-wal'):
            path = self.root / filename
            path.touch()
            with self.subTest(filename=filename), self.assertRaises(JournalError):
                self.open()
            path.unlink()
        durable_state.publish(self.root / 'notify-cursor.json',
            dict(thread='synthetic-session', through=7, journal_required=True))
        with self.assertRaises(JournalError):
            self.open()

    def test_foreign_legacy_checkpoint_is_not_retargeted(self):
        durable_state.publish(self.root / 'notify-cursor.json', dict(thread='another-session', through=7))
        with self.assertRaises(JournalError):
            self.open()
        self.assertEqual(durable_state.read(self.root / 'notify-cursor.json')['thread'], 'another-session')

    def test_ack_during_preparation_does_not_send_missing_rows(self):
        with closing(self.open()) as state:
            state.prepare(0)
            self.db.execute('DELETE FROM inbox WHERE seq<=10')
            self.db.commit()
            self.assertEqual([row['seq'] for row in state.reserve_next(0)], [11, 12])
            state.resolve('delivered', 0)
            self.assertEqual(state.status()['legacy_checkpoint'], 13)
