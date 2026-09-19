from contextlib import closing
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

import bridge
import inbox_schema
import notification_source as source
from test_memory_bindings import record, observation


class InboxSourceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.store = bridge.InboxStore(self.root)
        self.addCleanup(self.store.close)
        self.reader = source.InboxSource(self.root / 'inbox.sqlite3', schema=3,
            capabilities=(*inbox_schema.CAPABILITIES, 'memory_binding'))
        self.addCleanup(self.reader.close)

    def pointer(self, head=1):
        binding = self.store.bind_memory(record())
        self.store.refresh_memory(self.store.binding(binding['binding']), observation(head))
        return inbox_schema.allocated_head(self.store.db)

    def test_reader_cannot_modify_source(self):
        before = self.store.db.total_changes
        with self.assertRaises(sqlite3.OperationalError):
            self.reader.db.execute('DELETE FROM inbox')
        self.assertEqual(self.store.db.total_changes, before)
        self.assertEqual(self.reader.scan(0)['through'], 0)
        self.assertFalse(self.reader.db.in_transaction)

    def test_scan_bounds_rows_and_never_returns_message_content(self):
        for index in range(130):
            self.store.store(7, dict(type='user' if index % 2 else 'control', message=dict(content='private text')))
        first = self.reader.scan(0)
        self.assertEqual(first['through'], 128)
        self.assertEqual(len(first['records']), 64)
        self.assertNotIn('private text', json.dumps(first))
        second = self.reader.scan(first['through'])
        self.assertEqual(second['through'], 130)
        self.assertEqual([item['seq'] for item in second['records']], [130])

    def test_pointer_seed_is_independent_of_scan_checkpoint(self):
        seq = self.pointer()
        self.assertEqual(self.reader.scan(seq)['records'], [])
        seed = self.reader.seed_pointers()
        self.assertEqual(seed['records'][0]['seq'], seq)
        self.assertEqual(seed['records'][0]['kind'], 'memory-pointer')
        key = seed['records'][0]['binding']
        self.assertEqual(seed['records'][0]['binding_instance'], seed['bindings'][key])
        self.assertEqual(seed['ack_through'], 0)

    def test_reconciliation_keeps_coalescing_separate_from_acknowledgement(self):
        old = self.pointer()
        new = self.pointer(2)
        state = self.reader.retained([old, new])
        self.assertEqual([item['seq'] for item in state['records']], [new])
        self.assertEqual(state['ack_through'], 0)
        self.assertEqual(state['pointers'][record()['binding']]['seq'], new)
        self.assertEqual(self.reader.retained([old])['pointers'][record()['binding']]['seq'], new)
        self.store.command(dict(op='ack', through=new))
        state = self.reader.retained([old, new])
        self.assertEqual(state['records'], [])
        self.assertEqual(state['ack_through'], new)

    def test_rebinding_changes_instance_even_if_old_row_is_missing(self):
        seq = self.pointer()
        first = self.reader.retained([seq])
        key = record()['binding']
        self.store.unbind_memory(key)
        self.store.bind_memory(record())
        second = self.reader.retained([seq])
        self.assertEqual(second['records'], [])
        self.assertNotEqual(first['bindings'][key], second['bindings'][key])
        self.assertEqual(second['ack_through'], 0)

    def test_reconciliation_batches_all_selected_sequences(self):
        for _ in range(270):
            self.store.store(7, dict(type='user', message=dict(content='synthetic')))
        wanted = list(range(270, 0, -1))
        self.assertEqual([item['seq'] for item in self.reader.retained(wanted)['records']],
                         list(reversed(wanted)))
        self.assertFalse(self.reader.db.in_transaction)

    def test_corrupt_advertised_metadata_is_not_empty_legacy_state(self):
        self.store.db.execute('DROP TABLE inbox_meta')
        with self.assertRaises(sqlite3.DatabaseError):
            self.reader.scan(0)
        self.assertFalse(self.reader.db.in_transaction)

    def test_invalid_records_fail_without_leaking_read_transaction(self):
        self.store.store(7, dict(type='user', message=dict(content='synthetic')))
        self.store.db.execute("UPDATE inbox SET kind='memory-pointer'")
        self.store.db.commit()
        with self.assertRaisesRegex(source.SourceError, 'source_pointer_invalid'):
            self.reader.scan(0)
        self.assertFalse(self.reader.db.in_transaction)

    def test_snapshot_cannot_mix_acknowledgement_and_source_rows(self):
        self.store.store(7, dict(type='user', message=dict(content='synthetic')))
        self.store.db.execute('PRAGMA busy_timeout=1')
        with self.reader.snapshot() as state:
            self.assertEqual(state['ack_through'], 0)
            with self.assertRaises(sqlite3.OperationalError):
                self.store.command(dict(op='ack', through=1))
            self.assertEqual(self.reader.db.execute('SELECT seq FROM inbox').fetchall(), [(1,)])
            self.assertFalse(self.store.db.in_transaction)
        self.store.command(dict(op='ack', through=1))
        state = self.reader.retained([1])
        self.assertEqual(state['ack_through'], 1)
        self.assertEqual(state['records'], [])

    def test_request_bounds_are_strict(self):
        for value in (True, -1, 1 << 63, '1'):
            with self.subTest(value=value), self.assertRaises(source.SourceError):
                self.reader.scan(value)
        for values in ([1, 1], [True], [0], [1] * 2049, '1'):
            with self.subTest(values=str(values)[:30]), self.assertRaises(source.SourceError):
                self.reader.retained(values)

    def test_legacy_has_unknown_acknowledgement_and_no_pointer_capability(self):
        path = self.root / 'legacy.sqlite3'
        with closing(sqlite3.connect(path)) as db:
            db.execute('CREATE TABLE inbox(seq INTEGER PRIMARY KEY,pid INTEGER,frame TEXT)')
            db.execute('INSERT INTO inbox VALUES(3,7,?)', (json.dumps(dict(type='user', message=dict(content='synthetic'))),))
            db.commit()
        with closing(source.InboxSource(path)) as reader:
            result = reader.scan(0)
            self.assertIsNone(result['ack_through'])
            self.assertIsNone(result['activation'])
            self.assertEqual(result['through'], 3)
            with self.assertRaises(source.SourceError):
                reader.seed_pointers()

    def test_invalid_advertised_capability_is_refused(self):
        for schema, capabilities in ((True, ['inbox_ack_watermark']),
                                     (2, ['memory_binding']),
                                     (4, ['inbox_ack_watermark'])):
            with self.subTest(schema=schema), self.assertRaises(source.SourceError):
                source.InboxSource(self.root / 'inbox.sqlite3', schema=schema,
                                   capabilities=capabilities)
