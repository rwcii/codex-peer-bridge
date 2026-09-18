import json
import asyncio
import socket
from unittest import mock
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest

import bridge
import inbox_schema as schema

FRAME = {'type':'user', 'message':{'content':'synthetic message'}}
TARGET, NONCE, NEXT = 'a'*64, 'b'*32, 'c'*32


class InboxSchemaTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.path = self.root/'inbox.sqlite3'

    def store(self):
        store = bridge.InboxStore(self.root)
        self.addCleanup(store.close)
        return store

    def legacy(self):
        db = sqlite3.connect(self.path)
        db.execute('CREATE TABLE inbox (seq INTEGER PRIMARY KEY AUTOINCREMENT, received REAL, pid INTEGER, frame TEXT)')
        db.execute('INSERT INTO inbox VALUES (7,1,42,?)', (json.dumps(FRAME),))
        db.commit()
        db.close()

    def test_migration_preserves_rows_allocated_head_and_legacy_readers(self):
        self.legacy()
        store = self.store()
        self.assertEqual(store.db.execute('PRAGMA journal_mode').fetchone()[0], 'delete')
        self.assertEqual(schema.allocated_head(store.db), 7)
        self.assertEqual(schema.metadata(store.db)['ack_through'], 0)
        row = store.db.execute('SELECT seq,pid,frame,kind,binding FROM inbox').fetchone()
        self.assertEqual(row, (7,42,json.dumps(FRAME),'peer',None))
        import notify
        reader = sqlite3.connect(self.path.as_uri()+'?mode=ro', uri=True)
        try:
            self.assertEqual(notify.unread(reader, 0), (7,[(7,42)]))
        finally:
            reader.close()
        before = self.path.read_bytes()
        other = self.store()
        self.assertEqual(self.path.read_bytes(), before)
        self.assertEqual(schema.metadata(other.db)['schema'], schema.SCHEMA)

    def test_metadata_is_not_visible_before_migration_commit(self):
        self.legacy()
        db = sqlite3.connect(self.path)
        observations = []
        def trace(statement):
            if statement == 'COMMIT':
                reader = sqlite3.connect(self.path.as_uri()+'?mode=ro', uri=True)
                try:
                    observations.append(tuple(row[1] for row in reader.execute('PRAGMA table_info(inbox)')))
                    try:
                        schema.metadata(reader)
                    except sqlite3.DatabaseError:
                        observations.append('metadata absent')
                    else:
                        observations.append('metadata visible')
                finally:
                    reader.close()
        db.set_trace_callback(trace)
        try:
            schema.initialize(db)
            self.assertEqual(observations, [schema.LEGACY_COLUMNS, 'metadata absent'])
            self.assertEqual(schema.metadata(db)['schema'], schema.SCHEMA)
        finally:
            db.close()

    def test_empty_ack_and_head_clamp_never_ack_future_arrivals(self):
        store = self.store()
        self.assertEqual(schema.allocated_head(store.db), 0)
        schema.acknowledge(store.db, 10**100)
        self.assertEqual(schema.metadata(store.db)['ack_through'], 0)
        store.store(42, FRAME)
        schema.acknowledge(store.db, 10**100)
        self.assertEqual(schema.metadata(store.db)['ack_through'], 1)
        self.assertEqual(schema.allocated_head(store.db), 1)
        store.store(42, FRAME)
        self.assertEqual(store.command({'op':'inbox'})[0]['seq'], 2)
        schema.acknowledge(store.db, 0)
        self.assertEqual(schema.metadata(store.db)['ack_through'], 1)
        self.assertEqual(store.command({'op':'inbox'})[0]['seq'], 2)

    def test_ack_rollback_after_delete_on_metadata_write_failure(self):
        store = self.store()
        store.store(42, FRAME)
        store.db.execute("CREATE TRIGGER fail_ack BEFORE UPDATE ON inbox_meta BEGIN SELECT RAISE(ABORT,'synthetic failure'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            schema.acknowledge(store.db, 1)
        self.assertFalse(store.db.in_transaction)
        self.assertEqual(schema.metadata(store.db)['ack_through'], 0)
        self.assertEqual(len(store.command({'op':'inbox'})), 1)

    def test_ack_reads_head_only_inside_write_transaction(self):
        store = self.store()
        statements = []
        store.db.set_trace_callback(statements.append)
        schema.acknowledge(store.db, 5)
        store.db.set_trace_callback(None)
        self.assertEqual(statements[0], 'BEGIN IMMEDIATE')
        head_reads = [i for i,s in enumerate(statements) if 'SELECT seq FROM sqlite_sequence' in s]
        deletion = next(i for i,s in enumerate(statements) if s.startswith('DELETE'))
        self.assertTrue(head_reads)
        self.assertTrue(all(0 < i < deletion for i in head_reads))
        self.assertEqual(statements[-1], 'COMMIT')

    def test_missing_metadata_and_read_errors_do_not_become_empty_state(self):
        store = self.store()
        store.db.execute("DELETE FROM inbox_meta WHERE key='ack_through'")
        store.db.commit()
        with self.assertRaises(schema.InboxSchemaError):
            schema.metadata(store.db)
        with self.assertRaises(schema.InboxSchemaError):
            schema.initialize(store.db)
        store.db.execute('DROP TABLE inbox_meta')
        with self.assertRaises(sqlite3.DatabaseError):
            store.command({'op':'status'})
        with self.assertRaises(schema.InboxSchemaError):
            schema.initialize(store.db)

    def test_activation_is_durable_idempotent_and_target_is_fixed(self):
        store = self.store()
        request = dict(op='activate-notification-journal', target_digest=TARGET, nonce=NONCE)
        self.assertEqual(store.command(request), {'target_digest':TARGET,'nonce':NONCE})
        self.assertFalse(store.db.in_transaction)
        reader = sqlite3.connect(self.path)
        try:
            self.assertEqual(schema.metadata(reader)['journal_activation'], store.command(request))
        finally:
            reader.close()
        for changed in (dict(request, nonce=NEXT), dict(request, target_digest='d'*64)):
            with self.assertRaises(ValueError):
                store.command(changed)
        rebuild = dict(op='rebuild-notification-journal-activation', target_digest=TARGET,
                       nonce=NEXT, expected_previous_nonce=NONCE, accept_history_loss=True)
        with self.assertRaises(ValueError):
            store.command(dict(rebuild, accept_history_loss=False))
        with self.assertRaises(ValueError):
            store.command(dict(rebuild, expected_previous_nonce='e'*32))
        self.assertEqual(store.command(rebuild)['nonce'], NEXT)
        self.assertEqual(store.command(rebuild)['nonce'], NEXT)  # lost reply retry
        self.assertEqual(schema.metadata(store.db)['journal_activation']['nonce'], NEXT)

    def test_activation_invalid_input_and_failed_write_leave_evidence_unchanged(self):
        store = self.store()
        request = dict(op='activate-notification-journal', target_digest=TARGET, nonce=NONCE)
        for changes in ({'nonce':'B'*32}, {'target_digest':None}, {'unexpected':True}):
            with self.assertRaises(ValueError):
                store.command(dict(request, **changes))
        store.db.execute("CREATE TRIGGER fail_activation BEFORE UPDATE ON inbox_meta BEGIN SELECT RAISE(ABORT,'synthetic'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            store.command(request)
        self.assertIsNone(schema.metadata(store.db)['journal_activation'])

    def test_binding_path_limits_are_enforced_in_utf8_bytes(self):
        store = self.store()
        for column in ('repo_path', 'memory_state_dir'):
            for path in ('relative', '/'+'x'*4096, '/'+'\u00e9'*2048):
                values = dict(binding=TARGET, repo_path='/repo', repo_key='a'*16, memory_state_dir='/state')
                values[column] = path
                with self.subTest(column=column, length=len(path)), self.assertRaises(sqlite3.IntegrityError):
                    store.db.execute('INSERT INTO memory_binding(binding,repo_path,repo_key,memory_state_dir) VALUES (:binding,:repo_path,:repo_key,:memory_state_dir)', values)
                store.db.rollback()
        store.db.execute('INSERT INTO memory_binding(binding,repo_path,repo_key,memory_state_dir) VALUES (?,?,?,?)',
                         (TARGET, '/'+'x'*4095, 'a'*16, '/'+'x'*4095))
        store.db.commit()
        self.assertEqual(store.db.execute('SELECT count(*) FROM memory_binding').fetchone()[0], 1)

    def test_metadata_keys_and_size_are_bounded(self):
        store = self.store()
        for key, value in [('unknown','0'),(None,'0'),('schema','x'*513)]:
            with self.assertRaises(sqlite3.IntegrityError):
                store.db.execute('INSERT OR REPLACE INTO inbox_meta VALUES (?,?)', (key,value))
            store.db.rollback()
        self.assertEqual(schema.metadata(store.db)['schema'], schema.SCHEMA)

    def crash(self, match, operation):
        code = '''
import os, sqlite3, sys
import inbox_schema
connection = sqlite3.connect(sys.argv[1])
def trace(statement):
    if statement.startswith(sys.argv[2]):
        os._exit(73)
connection.set_trace_callback(trace)
if sys.argv[3] == 'migrate':
    inbox_schema.initialize(connection)
else:
    inbox_schema.acknowledge(connection, 7)
raise SystemExit('crash point not reached')
'''
        result = subprocess.run([sys.executable,'-c',code,str(self.path),match,operation],
                                cwd=Path(bridge.__file__).parent,capture_output=True,text=True,timeout=10)
        self.assertEqual(result.returncode, 73, result.stderr)

    def test_process_death_mid_ddl_rolls_back_then_retry_migrates(self):
        self.legacy()
        self.crash('ALTER TABLE inbox ADD COLUMN binding', 'migrate')
        db = sqlite3.connect(self.path)
        try:
            self.assertEqual(tuple(row[1] for row in db.execute('PRAGMA table_info(inbox)')), schema.LEGACY_COLUMNS)
            self.assertEqual(db.execute('SELECT seq FROM inbox').fetchone()[0], 7)
            self.assertEqual(schema.allocated_head(db), 7)
        finally:
            db.close()
        store = self.store()
        self.assertEqual(schema.metadata(store.db)['schema'], schema.SCHEMA)
        self.assertEqual(schema.allocated_head(store.db), 7)

    def test_process_death_between_delete_and_watermark_rolls_back(self):
        self.legacy()
        store = self.store()
        self.crash('UPDATE inbox_meta SET value=', 'ack')
        self.assertEqual(store.db.execute('SELECT seq FROM inbox').fetchone()[0], 7)
        self.assertEqual(schema.metadata(store.db)['ack_through'], 0)
        schema.acknowledge(store.db, 7)
        self.assertEqual(schema.metadata(store.db)['ack_through'], 7)
        self.assertEqual(store.command({'op':'inbox'}), [])


class SchemaPublicTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.addCleanup(self.temp.cleanup)

    async def test_status_requires_readable_committed_evidence(self):
        class TestStore(bridge.InboxStore):
            def command(self, request):
                if request['op'] == 'test-drop-metadata':
                    self.db.execute('DROP TABLE inbox_meta')
                    return None
                return super().command(request)
        service = bridge.Bridge(self.root)
        service.worker = bridge.DatabaseWorker(lambda: TestStore(self.root))
        try:
            status = await service.command({'op':'status'})
            self.assertEqual(status['inbox_schema'], schema.SCHEMA)
            self.assertEqual(set(status['capabilities']), set(schema.CAPABILITIES) | {'inbox_subscription', 'memory_binding'})
            self.assertIn('memory_binding', status['capabilities'])
            self.assertTrue(schema.hex_value(status['generation'], 32))
            await service.worker.call('command', {'op':'test-drop-metadata'})
            status = await service.command({'op':'status'})
            self.assertEqual(status['database_status'], 'storage_error')
            self.assertIsNone(status['inbox_schema'])
            self.assertEqual(status['capabilities'], [])
        finally:
            await service.worker.close()

    async def test_refused_start_preserves_legacy_and_migrated_store(self):
        path = self.root/'inbox.sqlite3'
        db = sqlite3.connect(path)
        db.execute('CREATE TABLE inbox (seq INTEGER PRIMARY KEY AUTOINCREMENT, received REAL, pid INTEGER, frame TEXT)')
        db.execute('INSERT INTO inbox VALUES(3,1,42,?)', (json.dumps(FRAME),))
        db.commit()
        db.close()
        for migrated in (False, True):
            if migrated:
                store = bridge.InboxStore(self.root)
                schema.acknowledge(store.db, 2)
                store.command(dict(op='activate-notification-journal', target_digest=TARGET, nonce=NONCE))
                store.close()
            before = path.read_bytes()
            service = bridge.Bridge(self.root)
            endpoint = bridge.platform_support.control_socket_path(self.root)
            held = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            held.bind(str(endpoint))
            held.listen(1)
            try:
                with mock.patch.object(bridge, 'InboxStore') as factory:
                    with self.assertRaises(bridge.BridgeOwnershipError):
                        await service.run()
                    factory.assert_not_called()
                self.assertEqual(path.read_bytes(), before)
            finally:
                held.close()
                endpoint.unlink()

    async def test_startup_incompatible_and_corrupt_store_are_classified(self):
        path = self.root/'inbox.sqlite3'
        store = bridge.InboxStore(self.root)
        store.db.execute("UPDATE inbox_meta SET value=? WHERE key='schema'", (str(schema.SCHEMA+1),))
        store.db.commit()
        store.close()
        for corrupt in (False, True):
            if corrupt:
                path.write_bytes(b'not a SQLite database')
            before = path.read_bytes()
            process = await asyncio.create_subprocess_exec(
                sys.executable, str(Path(bridge.__file__)), '--state-dir', str(self.root), 'serve',
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            stdout, stderr = await asyncio.wait_for(process.communicate(), 5)
            self.assertEqual(process.returncode, 78)
            self.assertEqual(json.loads(stdout)['code'], 'storage_error' if corrupt else 'incompatible_inbox')
            self.assertEqual(stderr, b'')
            self.assertEqual(path.read_bytes(), before)
            self.assertFalse(bridge.platform_support.control_socket_path(self.root).exists())

    async def test_factory_programming_error_is_software_failure(self):
        code = """
import bridge, inbox_schema
original = inbox_schema.initialize
def broken(db):
    db.close()
    original(db)
inbox_schema.initialize = broken
bridge.main()
"""
        process = await asyncio.create_subprocess_exec(
            sys.executable, '-c', code, '--state-dir', str(self.root), 'serve',
            cwd=Path(bridge.__file__).parent,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await asyncio.wait_for(process.communicate(), 5)
        self.assertEqual(process.returncode, 70)
        self.assertEqual(json.loads(stdout)['code'], 'internal_error')
        self.assertEqual(stderr, b'')
        self.assertFalse(bridge.platform_support.control_socket_path(self.root).exists())

    async def test_activation_control_reply_follows_commit(self):
        service = bridge.Bridge(self.root)
        service.worker = bridge.DatabaseWorker(lambda: bridge.InboxStore(self.root))
        endpoint = bridge.platform_support.control_socket_path(self.root)
        server = await asyncio.start_unix_server(lambda r,w: service.handle(r,w,True), str(endpoint))
        endpoint.chmod(0o600)
        try:
            reply, _ = await bridge.control_exchange(self.root,
                dict(op='activate-notification-journal', target_digest=TARGET, nonce=NONCE))
            self.assertTrue(reply['ok'])
            reader = sqlite3.connect((self.root/'inbox.sqlite3').as_uri()+'?mode=ro', uri=True)
            try:
                self.assertEqual(schema.metadata(reader)['journal_activation'], reply['result'])
            finally:
                reader.close()
        finally:
            server.close()
            await server.wait_closed()
            await bridge.drain_handlers(service.tasks)
            await service.worker.close()

    async def test_public_ack_rejects_lossy_coercion_and_caps_read_cursor(self):
        service = bridge.Bridge(self.root)
        service.worker = bridge.DatabaseWorker(lambda: bridge.InboxStore(self.root))
        try:
            await service.store(42, FRAME)
            for value in (True, 1.5, -1, None):
                with self.assertRaises(ValueError):
                    await service.command(dict(op='ack', through=value))
            self.assertEqual(len(await service.command(dict(op='inbox'))), 1)
            self.assertEqual(await service.command(dict(op='inbox', after=10**100)), [])
            await service.command(dict(op='ack', through=10**100))
            self.assertEqual((await service.command(dict(op='status')))['ack_through'], 1)
        finally:
            await service.worker.close()
