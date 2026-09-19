import asyncio
import ast
import inspect
from contextlib import closing, redirect_stdout
import io
import json
import os
from pathlib import Path
import sqlite3
import socket
import subprocess
import sys
import tempfile
import time
import threading
import unittest
from unittest import mock

import bridge
import inbox_schema
import memory
import memory_bindings as bindings
import subscriptions

FRAME = dict(type='user', message=dict(content='synthetic'))


def record(index=1):
    return dict(binding=f'{index:064x}', repo_path=f'/synthetic/repo-{index}',
                repo_key=f'{index:016x}', memory_state_dir=f'/synthetic/state-{index}')


def observation(head=1, store_id='a'*32):
    return dict(store_id=store_id, head=head, pid=42, generation='b'*32)


class BindingStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.store = bridge.InboxStore(self.root)
        self.addCleanup(self.store.close)

    def bind(self, index=1):
        return self.store.bind_memory(record(index))

    def refresh(self, head=1, store_id='a'*32, index=1):
        return self.store.refresh_memory(self.store.binding(record(index)['binding']), observation(head, store_id))

    def test_unchanged_observation_survives_ack_and_reopen(self):
        binding = self.bind()
        self.refresh()
        sequence = inbox_schema.allocated_head(self.store.db)
        self.store.command(dict(op='ack', through=sequence))
        self.assertFalse(self.refresh()['changed'])
        other = bridge.InboxStore(self.root)
        try:
            self.assertFalse(other.refresh_memory(other.binding(binding['binding']), observation())['changed'])
            self.assertEqual(inbox_schema.allocated_head(other.db), sequence)
            self.assertEqual(other.command(dict(op='inbox')), [])
        finally:
            other.close()

    def test_replacement_allocates_new_sequence_and_wire_has_no_memory_head(self):
        self.bind()
        self.refresh()
        first = inbox_schema.allocated_head(self.store.db)
        self.store.store(7, FRAME)
        self.refresh(2)
        rows = self.store.command(dict(op='inbox'))
        self.assertEqual(len(rows), 2)
        pointer = rows[-1]
        self.assertGreater(pointer['seq'], first)
        self.assertEqual(pointer['frame'], dict(type='memory-pointer', binding=record()['binding']))
        self.assertEqual(pointer['kind'], 'memory-pointer')
        self.assertEqual(pointer['guidance'], bridge.MEMORY_POINTER_GUIDANCE)
        self.assertNotIn('peer_pid', pointer)
        self.assertEqual(rows[0]['guidance'], bridge.PEER_GUIDANCE)
        self.assertEqual(rows[0]['frame'], FRAME)
        self.assertEqual(inbox_schema.metadata(self.store.db)['ack_through'], 0)

    def test_store_replacement_and_head_regression_remain_visible(self):
        self.bind()
        self.refresh(9)
        self.assertEqual(self.refresh(9, 'c'*32)['anomalies'], 1)
        self.assertEqual(self.refresh(2, 'c'*32)['anomalies'], 3)
        self.assertEqual(self.refresh(3, 'c'*32)['anomalies'], 3)
        self.assertFalse(self.refresh(3, 'c'*32)['changed'])

    def test_concurrent_old_observation_cannot_undo_newer_refresh(self):
        stale = self.bind()
        self.refresh(4)
        with self.assertRaisesRegex(bindings.BindingError, 'binding_observation_changed'):
            self.store.refresh_memory(stale, observation(2))
        self.assertEqual(self.store.binding(stale['binding'])['observed_head'], 4)

    def test_pointer_and_observation_roll_back_together(self):
        self.bind()
        self.refresh()
        before = self.store.command(dict(op='inbox'))
        allocated = inbox_schema.allocated_head(self.store.db)
        self.store.db.execute("CREATE TRIGGER refuse_observation BEFORE UPDATE ON memory_binding BEGIN SELECT RAISE(ABORT, 'synthetic'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.refresh(2)
        self.assertEqual(self.store.command(dict(op='inbox')), before)
        self.assertEqual(inbox_schema.allocated_head(self.store.db), allocated)
        self.assertEqual(self.store.binding(record()['binding'])['observed_head'], 1)

    def test_quotas_are_independent_and_unbind_is_not_ack(self):
        for index in range(1, 17):
            self.bind(index)
            self.refresh(index=index)
        with self.assertRaisesRegex(bindings.BindingError, 'binding_capacity'):
            self.bind(17)
        for _ in range(1000):
            self.store.store(42, FRAME)
        with self.assertRaisesRegex(ValueError, 'inbox full'):
            self.store.store(42, FRAME)
        self.assertTrue(self.refresh(2)['changed'])
        self.store.unbind_memory(record()['binding'])
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM inbox').fetchone()[0], 1015)
        self.assertEqual(inbox_schema.metadata(self.store.db)['ack_through'], 0)
        with self.assertRaisesRegex(bindings.BindingError, 'binding_not_found'):
            self.store.refresh_memory(record(), observation())

    def test_ordinary_peer_cannot_mint_pointer(self):
        self.bind()
        with self.assertRaisesRegex(ValueError, 'unsupported frame'):
            self.store.store(42, dict(type='memory-pointer', binding=record()['binding']))
        self.assertEqual(self.store.command(dict(op='inbox')), [])

    def test_rebind_has_new_instance_and_old_refresh_cannot_mutate_it(self):
        old = self.bind()
        self.refresh()
        prior_pointer = self.store.db.execute("SELECT binding,binding_instance FROM inbox WHERE kind='memory-pointer'").fetchone()
        self.store.unbind_memory(old['binding'])
        new = self.bind()
        self.assertEqual(old['binding'], new['binding'])
        self.assertNotEqual(old['binding_instance'], new['binding_instance'])
        self.assertEqual(prior_pointer, (old['binding'], old['binding_instance']))
        with self.assertRaisesRegex(bindings.BindingError, 'binding_instance_changed'):
            self.store.refresh_memory(old, observation())
        self.assertEqual(self.store.command(dict(op='inbox')), [])
        self.assertTrue(self.refresh()['changed'])
        current_pointer = self.store.db.execute("SELECT binding_instance FROM inbox WHERE kind='memory-pointer'").fetchone()[0]
        self.assertEqual(current_pointer, new['binding_instance'])

    def test_health_acknowledgement_preserves_pointer_observation_and_watermark(self):
        self.bind()
        self.refresh(9)
        self.refresh(2)
        pointer = self.store.command(dict(op='inbox'))
        before = self.store.binding(record()['binding'])
        result = self.store.acknowledge_binding_health(record()['binding'])
        self.assertEqual(result['cleared'], 2)
        after = self.store.binding(record()['binding'])
        self.assertEqual(after, dict(before, anomalies=0))
        self.assertEqual(self.store.command(dict(op='inbox')), pointer)
        self.assertEqual(inbox_schema.metadata(self.store.db)['ack_through'], 0)
        self.assertFalse(self.refresh(2)['changed'])

    def test_zero_instance_migration_sentinel_is_not_valid_runtime_identity(self):
        binding = self.bind()
        self.store.db.execute('UPDATE memory_binding SET binding_instance=? WHERE binding=?',
                              ('0'*32, binding['binding']))
        self.store.db.commit()
        with self.assertRaisesRegex(inbox_schema.InboxSchemaError, 'invalid binding observation'):
            inbox_schema.validate_bindings(self.store.db)

    def test_single_oversize_binding_row_is_explicitly_refused(self):
        item = dict(record(), repo_path='x'*262144)
        with self.assertRaisesRegex(bindings.BindingError, 'binding_row_too_large'):
            bindings.page([item])

    def test_binding_listing_obeys_wire_limit_with_long_escaped_paths(self):
        records = [dict(record(index), repo_path='/'+'\n'*4095, memory_state_dir='/'+'\n'*4095)
                   for index in range(1, 17)]
        first = bindings.page(records)
        self.assertTrue(first['more'])
        self.assertLess(len(json.dumps(dict(ok=True, result=first)).encode()), 262144)
        second = bindings.page(records, first['next_after'])
        self.assertTrue(set(x['binding'] for x in first['bindings']).isdisjoint(x['binding'] for x in second['bindings']))


class IdentityMigrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def legacy_memory(self):
        path = self.root/'memory.sqlite3'
        store = memory.Store(path, 'a'*16)
        store.note('synthetic', 'decision', 'preserved')
        commands = memory.MemoryCommands(self.root, 'a'*16, store, 'b'*32)
        commands.command(dict(op='sync', consumer='synthetic'), 42)
        store.close()
        with closing(sqlite3.connect(path)) as db:
            db.execute("DELETE FROM meta WHERE key='store_id'")
            db.execute("UPDATE meta SET value='3' WHERE key='schema'")
            db.commit()
        return path

    def test_memory_migration_preserves_data_and_consumers_and_identity(self):
        path = self.legacy_memory()
        with closing(sqlite3.connect(path)) as db:
            before = db.execute('SELECT * FROM cursors').fetchall()
        store = memory.Store(path, 'a'*16)
        identity = store.meta('store_id')
        self.assertTrue(inbox_schema.hex_value(identity, 32))
        self.assertEqual(store.head(), 1)
        self.assertEqual(store.db.execute('SELECT * FROM cursors').fetchall(), before)
        store.close()
        store = memory.Store(path, 'a'*16)
        try:
            self.assertEqual(store.meta('store_id'), identity)
        finally:
            store.close()
        other = memory.Store(self.root/'other.sqlite3', 'a'*16)
        try:
            self.assertNotEqual(other.meta('store_id'), identity)
        finally:
            other.close()

    def test_missing_identity_in_new_schema_is_refused_without_reidentification(self):
        path = self.root/'memory.sqlite3'
        memory.Store(path, 'a'*16).close()
        with closing(sqlite3.connect(path)) as db:
            db.execute("DELETE FROM meta WHERE key='store_id'")
            db.commit()
        before = path.read_bytes()
        with self.assertRaisesRegex(memory.MemoryError_, 'identity is missing'):
            memory.Store(path, 'a'*16)
        self.assertEqual(path.read_bytes(), before)

    def test_memory_migration_process_death_rolls_back_identity_and_version(self):
        path = self.legacy_memory()
        source = """import os,sys,memory
from pathlib import Path
class Interrupted(memory.Store):
 def set_meta(self,key,value):
  super().set_meta(key,value)
  if key == 'store_id': os._exit(73)
Interrupted(Path(sys.argv[1]), 'a'*16)
"""
        child = subprocess.run([sys.executable, '-c', source, str(path)], timeout=10)
        self.assertEqual(child.returncode, 73)
        with closing(sqlite3.connect(path)) as db:
            self.assertEqual(db.execute("SELECT value FROM meta WHERE key='schema'").fetchone()[0], '3')
            self.assertIsNone(db.execute("SELECT value FROM meta WHERE key='store_id'").fetchone())
        store = memory.Store(path, 'a'*16)
        try:
            self.assertEqual(store.head(), 1)
            self.assertTrue(inbox_schema.hex_value(store.meta('store_id'), 32))
        finally:
            store.close()

    def legacy_inbox(self):
        db = sqlite3.connect(self.root/'inbox.sqlite3')
        db.executescript("""
CREATE TABLE inbox(seq INTEGER PRIMARY KEY AUTOINCREMENT,received REAL,pid INTEGER,frame TEXT,kind TEXT NOT NULL DEFAULT 'peer',binding TEXT);
CREATE TABLE inbox_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
INSERT INTO inbox_meta VALUES('schema','2'),('ack_through','5'),('journal_activation','null');
CREATE TABLE memory_binding(binding TEXT PRIMARY KEY,repo_path TEXT,repo_key TEXT,memory_state_dir TEXT);
""")
        db.execute('INSERT INTO inbox(seq,received,pid,frame) VALUES(?,?,?,?)', (7,1,42,json.dumps(FRAME)))
        db.execute('INSERT INTO memory_binding VALUES(?,?,?,?)', tuple(record().values()))
        db.commit()
        db.close()

    def test_inbox_schema2_migrates_observations_and_preserves_existing_rows(self):
        self.legacy_inbox()
        store = bridge.InboxStore(self.root)
        try:
            self.assertEqual(inbox_schema.metadata(store.db)['schema'], inbox_schema.SCHEMA)
            self.assertEqual(inbox_schema.allocated_head(store.db), 7)
            self.assertEqual(inbox_schema.metadata(store.db)['ack_through'], 5)
            self.assertEqual(store.command(dict(op='inbox'))[0]['frame'], FRAME)
            binding = store.binding(record()['binding'])
            self.assertIsNone(binding['observed_head'])
            self.assertIsNone(binding['observed_store'])
            self.assertTrue(store.refresh_memory(binding, observation())['changed'])
        finally:
            store.close()

    def test_inbox_migration_process_death_rolls_back_columns_and_version(self):
        self.legacy_inbox()
        source = """import os,sys,sqlite3,inbox_schema
from pathlib import Path
db=sqlite3.connect(Path(sys.argv[1])/'inbox.sqlite3')
def trace(sql):
 if sql.startswith('ALTER TABLE memory_binding ADD COLUMN observed_head'): os._exit(73)
db.set_trace_callback(trace)
inbox_schema.initialize(db)
"""
        child = subprocess.run([sys.executable, '-c', source, str(self.root)], timeout=10)
        self.assertEqual(child.returncode, 73)
        with closing(sqlite3.connect(self.root/'inbox.sqlite3')) as db:
            self.assertEqual(db.execute("SELECT value FROM inbox_meta WHERE key='schema'").fetchone()[0], '2')
            self.assertEqual(len(db.execute('PRAGMA table_info(memory_binding)').fetchall()), 4)
        store = bridge.InboxStore(self.root)
        try:
            self.assertEqual(inbox_schema.metadata(store.db)['schema'], inbox_schema.SCHEMA)
            self.assertEqual(inbox_schema.allocated_head(store.db), 7)
            self.assertEqual(inbox_schema.metadata(store.db)['ack_through'], 5)
        finally:
            store.close()



class BindingPublicTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.repo = self.root/'repo'
        subprocess.run(['git','init','-q',str(self.repo)], check=True)
        self.repo_key = memory.repo_identity(self.repo)
        self.memroot, self.broot = self.root/'memory', self.root/'bridge'
        self.memroot.mkdir(mode=0o700)
        self.broot.mkdir(mode=0o700)
        self.mem = memory.Service(self.memroot, self.repo_key,
                                 lambda: memory.Store(self.memroot/'memory.sqlite3', self.repo_key))
        sock, _ = memory.bind_exclusive(self.memroot, self.repo_key, self.mem.generation)
        self.bus = bridge.Bridge(self.broot)
        self.bus.address = 'uds:' + str(self.broot/'peer.sock')
        self.output = io.StringIO()
        self.redirect = redirect_stdout(self.output)
        self.redirect.__enter__()
        self.running = [asyncio.create_task(self.mem.run(sock)), asyncio.create_task(self.bus.run())]
        async with asyncio.timeout(3):
            while len(self.output.getvalue().splitlines()) < 2:
                for task in self.running:
                    if task.done():
                        await task
                await asyncio.sleep(.001)

    async def asyncTearDown(self):
        self.mem.stop.set()
        self.bus.stop.set()
        try:
            await asyncio.wait_for(asyncio.gather(*self.running), 4)
        finally:
            self.redirect.__exit__(None, None, None)
            self.temp.cleanup()

    async def request(self, request):
        response, _ = await bridge.control_exchange(self.broot, request, timeout=bindings.CLIENT_TIMEOUT)
        return response

    async def bind(self):
        response = await self.request(dict(op='bind-memory', repo_path=str(self.repo), memory_state_dir=str(self.memroot)))
        self.assertTrue(response['ok'], response)
        return response['result']

    async def test_explicit_binding_refresh_rechecks_head_and_preserves_memory_cursors(self):
        binding = await self.bind()
        self.assertEqual(binding['repo_path'], str((self.repo/'.git').resolve()))
        refresh = dict(op='refresh-memory', binding=binding['binding'])
        self.assertTrue((await self.request(refresh))['result']['changed'])
        first = (await self.request(dict(op='inbox')))['result'][0]['seq']
        self.assertFalse((await self.request(refresh))['result']['changed'])
        reply, _ = await bridge.control_exchange(self.memroot,
            dict(op='note', consumer='synthetic', type='decision', body='synthetic'))
        self.assertTrue(reply['ok'], reply)
        self.assertTrue((await self.request(refresh))['result']['changed'])
        second = (await self.request(dict(op='inbox')))['result'][0]['seq']
        self.assertGreater(second, first)
        reply, _ = await bridge.control_exchange(self.memroot, dict(op='status'))
        self.assertEqual(reply['result']['consumers'], [])
        self.assertEqual((await self.request(dict(op='status')))['result']['ack_through'], 0)

    async def test_absent_memory_preserves_binding_and_pointer(self):
        binding = await self.bind()
        await self.request(dict(op='refresh-memory', binding=binding['binding']))
        before = (await self.request(dict(op='memory-bindings')))['result']
        inbox = (await self.request(dict(op='inbox')))['result']
        self.mem.stop.set()
        await self.running[0]
        reply = await self.request(dict(op='refresh-memory', binding=binding['binding']))
        self.assertFalse(reply['ok'])
        self.assertEqual(reply['code'], 'memory_unavailable')
        self.assertEqual(reply['recovery'], 'retry')
        after = (await self.request(dict(op='memory-bindings')))['result']
        self.assertEqual(after['bindings'][0]['service_state'], 'refused')
        self.assertEqual(after['bindings'][0]['service_reason'], 'memory_unavailable')
        for field in bindings.FIELDS:
            self.assertEqual(after['bindings'][0][field], before['bindings'][0][field])
        self.assertEqual((await self.request(dict(op='inbox')))['result'], inbox)
        self.assertTrue((await self.request(dict(op='status')))['ok'])

    async def test_owner_mismatch_and_caller_supplied_head_are_refused(self):
        binding = await self.bind()
        owner_path = self.memroot/'owner.json'
        original = owner_path.read_text()
        owner = json.loads(original)
        owner['generation'] = '0'*32
        owner_path.write_text(json.dumps(owner))
        reply = await self.request(dict(op='refresh-memory', binding=binding['binding']))
        self.assertFalse(reply['ok'])
        self.assertEqual(reply['code'], 'ownership_mismatch')
        owner_path.write_text(original)
        reply = await self.request(dict(op='refresh-memory', binding=binding['binding'], head=999999))
        self.assertFalse(reply['ok'])
        self.assertEqual(reply['code'], 'invalid_binding_request')
        self.assertEqual((await self.request(dict(op='inbox')))['result'], [])

    async def test_foreign_repository_cannot_bind_the_service(self):
        other = self.root/'other'
        subprocess.run(['git','init','-q',str(other)], check=True)
        reply = await self.request(dict(op='bind-memory', repo_path=str(other), memory_state_dir=str(self.memroot)))
        self.assertFalse(reply['ok'])
        self.assertEqual(reply['code'], 'foreign_service')
        self.assertEqual((await self.request(dict(op='memory-bindings')))['result']['bindings'], [])

    async def test_binding_health_expires_and_cache_is_bounded(self):
        binding = await self.bind()
        self.bus.binding_health[binding['binding']] = (time.monotonic()-31, 'verified', None)
        result = (await self.request(dict(op='memory-bindings')))['result']['bindings'][0]
        self.assertEqual(result['service_state'], 'unknown')
        for index in range(20):
            self.bus.record_binding_health(f'{index:064x}', 'refused', 'memory_unavailable')
        self.assertEqual(len(self.bus.binding_health), 16)

    async def test_old_memory_runtime_requires_upgrade_before_binding(self):
        original = self.mem.command
        async def old(request, pid):
            result = await original(request, pid)
            if request['op'] == 'hello':
                result['schema'] = 3
                result.pop('store_id')
            return result
        with mock.patch.object(self.mem, 'command', side_effect=old):
            reply = await self.request(dict(op='bind-memory', repo_path=str(self.repo), memory_state_dir=str(self.memroot)))
        self.assertFalse(reply['ok'])
        self.assertEqual(reply['code'], 'memory_upgrade_required')
        self.assertEqual(reply['recovery'], 'operator_action')
        self.assertEqual((await self.request(dict(op='memory-bindings')))['result']['bindings'], [])

    async def test_memory_advance_after_observation_is_caught_by_next_refresh(self):
        binding = await self.bind()
        original = bindings.observe
        async def advance_after_read(record):
            result = await original(record)
            reply, _ = await bridge.control_exchange(self.memroot,
                dict(op='note', consumer='synthetic', type='decision', body='arrived after read'))
            self.assertTrue(reply['ok'])
            return result
        with mock.patch.object(bindings, 'observe', side_effect=advance_after_read):
            reply = await self.request(dict(op='refresh-memory', binding=binding['binding']))
        self.assertTrue(reply['ok'])
        row = (await self.request(dict(op='memory-bindings')))['result']['bindings'][0]
        self.assertEqual(row['observed_head'], 0)
        self.assertTrue((await self.request(dict(op='refresh-memory', binding=binding['binding'])))['result']['changed'])
        row = (await self.request(dict(op='memory-bindings')))['result']['bindings'][0]
        self.assertEqual(row['observed_head'], 1)

    async def test_unhealthy_memory_refuses_refresh_without_mutation(self):
        binding = await self.bind()
        with mock.patch.object(memory.Store, 'healthy', return_value=False):
            reply = await self.request(dict(op='refresh-memory', binding=binding['binding']))
        self.assertFalse(reply['ok'])
        self.assertEqual(reply['code'], 'unhealthy_service')
        self.assertEqual((await self.request(dict(op='inbox')))['result'], [])
        row = (await self.request(dict(op='memory-bindings')))['result']['bindings'][0]
        self.assertIsNone(row['observed_head'])

    async def test_cli_preserves_internal_pointer_provenance_and_peer_guidance(self):
        binding = await self.bind()
        await self.request(dict(op='refresh-memory', binding=binding['binding']))
        await self.bus.store(42, dict(FRAME, kind='memory-pointer', binding=binding['binding']))
        capture = io.StringIO()
        with redirect_stdout(capture):
            result = await bridge.client(self.broot, dict(op='inbox'))
        self.assertEqual(result, 0)
        entries = json.loads(capture.getvalue())['result']
        self.assertEqual(entries[0]['guidance'], bridge.MEMORY_POINTER_GUIDANCE)
        self.assertEqual(entries[0]['kind'], 'memory-pointer')
        self.assertNotIn('peer_pid', entries[0])
        self.assertEqual(entries[1]['guidance'], bridge.PEER_GUIDANCE)
        self.assertNotIn('kind', entries[1])

    async def test_public_health_acknowledgement_does_not_delete_pointer(self):
        binding = await self.bind()
        await self.request(dict(op='refresh-memory', binding=binding['binding']))
        before = (await self.request(dict(op='inbox')))['result']
        reply = await self.request(dict(op='ack-binding-health', binding=binding['binding']))
        self.assertTrue(reply['ok'])
        self.assertEqual(reply['result']['cleared'], 0)
        self.assertEqual((await self.request(dict(op='inbox')))['result'], before)

    async def test_legacy_length_boundary_service_can_bind_refresh_and_stop(self):
        await self.legacy_length_boundary_service_case(False)

    async def test_legacy_fallback_service_can_bind_refresh_and_stop(self):
        await self.legacy_length_boundary_service_case(True)

    async def legacy_length_boundary_service_case(self, inverse):
        with tempfile.TemporaryDirectory(dir='/tmp') as temp:
            base = Path(temp)
            parent = base / ('real' if inverse else 'long-' + 'x' * 110)
            root = parent / 'state'
            memory.private_state_dir(root)
            alias_parent = base / ('alias-parent-' + 'x' * 110) if inverse else base
            alias_parent.mkdir(mode=0o700, exist_ok=True)
            alias = alias_parent / 'alias'
            alias.symlink_to(parent, target_is_directory=True)
            old_path = (memory.platform_support.fallback_control_socket(root) if inverse
                        else alias / 'state' / 'control.sock')
            memory.private_state_dir(old_path.parent)
            self.assertNotEqual(old_path.resolve(), memory.platform_support.control_socket_path(root).resolve())
            service = memory.Service(root, self.repo_key,
                                     lambda: memory.Store(root / 'memory.sqlite3', self.repo_key))
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.bind(str(old_path))
            sock.listen(16)
            sock.setblocking(False)
            old_path.chmod(0o600)
            memory.write_owner(root, old_path, service.generation, self.repo_key)
            async def run():
                try:
                    await service.run(sock)
                finally:
                    memory.release(root, old_path, service.generation)
            running = asyncio.create_task(run())
            try:
                reply = await self.request(dict(op='bind-memory', repo_path=str(self.repo),
                                                memory_state_dir=str(root)))
                self.assertTrue(reply['ok'], reply)
                result = await self.request(dict(op='refresh-memory', binding=reply['result']['binding']))
                self.assertTrue(result['ok'], result)
                stopped = await asyncio.to_thread(memory.stop_service, root, self.repo_key, 2)
                self.assertEqual(stopped['status'], 'stopped')
                self.assertFalse(old_path.exists())
                self.assertIsNone(memory.read_owner(root))
                await running
                generation = 'c' * 32
                replacement, canonical = memory.bind_exclusive(root, self.repo_key, generation)
                try:
                    self.assertEqual(str(canonical), memory.read_owner(root)['socket'])
                    self.assertEqual(canonical, memory.platform_support.control_socket_path(root))
                finally:
                    replacement.close()
                    memory.release(root, canonical, generation)
            finally:
                service.stop.set()
                await asyncio.wait_for(running, 4)
                sock.close()

    async def test_owner_control_path_alias_identifies_the_same_service(self):
        alias = self.root / 'state-alias'
        alias.symlink_to(self.root, target_is_directory=True)
        owner = memory.read_owner(self.memroot)
        owner['socket'] = str(alias / 'memory' / 'control.sock')
        (self.memroot / 'owner.json').write_text(json.dumps(owner))
        # The owner can predate a caller that uses the canonical state spelling.
        # This recreates /var versus /private/var without platform gating.
        result = await memory.verify_running(self.memroot, self.repo_key)
        self.assertEqual(result['generation'], self.mem.generation)

    async def test_slow_hello_and_status_complete_within_binding_budget(self):
        binding = await self.bind()
        original = self.mem.command
        async def slow(request, pid):
            if request['op'] == 'hello':
                await asyncio.sleep(4.5)
            elif request['op'] == 'status':
                await asyncio.sleep(1.7)
            return await original(request, pid)
        with mock.patch.object(self.mem, 'command', side_effect=slow):
            reply = await self.request(dict(op='refresh-memory', binding=binding['binding']))
        self.assertTrue(reply['ok'], reply)
        self.assertTrue(reply['result']['changed'])

    async def test_outer_binding_deadline_is_retryable_with_unknown_outcome(self):
        binding = await self.bind()
        committed, release = threading.Event(), threading.Event()
        original = bridge.InboxStore.refresh_memory
        def delay_reply(store, record, observation):
            result = original(store, record, observation)
            committed.set()
            if not release.wait(bindings.CLIENT_TIMEOUT):
                raise RuntimeError('synthetic test barrier expired')
            return result
        try:
            capture = io.StringIO()
            with mock.patch.object(bridge.InboxStore, 'refresh_memory', new=delay_reply), redirect_stdout(capture):
                exit_code = await bridge.client(self.broot, dict(op='refresh-memory', binding=binding['binding']))
            reply = json.loads(capture.getvalue())
            self.assertEqual(exit_code, 1)
            self.assertTrue(committed.is_set())
            self.assertFalse(reply['ok'])
            self.assertEqual(reply['code'], 'binding_timeout')
            self.assertEqual(reply['recovery'], 'retry')
            self.assertEqual(reply['outcome'], 'unknown')
        finally:
            release.set()
        # The operation committed despite the lost reply; timeout is not rollback.
        self.assertEqual(len((await self.request(dict(op='inbox')))['result']), 1)
        self.assertEqual((await self.request(dict(op='memory-bindings')))['result']['bindings'][0]['service_state'], 'refused')



class BindingRecoveryPolicyTests(unittest.TestCase):
    def test_binding_and_client_deadlines_cover_serial_inner_budgets(self):
        import platform_support
        import peer_transport
        import service_runtime
        inner = (bindings.GIT_TIMEOUT + memory.VERIFY_TIMEOUT + bindings.STATUS_TIMEOUT
                 + platform_support.PROCESS_QUERY_TIMEOUT + 2*peer_transport.CONTROL_CLOSE_TIMEOUT)
        self.assertGreater(bindings.REQUEST_TIMEOUT, inner)
        self.assertGreater(bindings.CLIENT_TIMEOUT,
                           bindings.REQUEST_TIMEOUT + service_runtime.HANDSHAKE_TIMEOUT)

    def test_every_local_and_delegated_code_has_an_explicit_policy(self):
        codes = set()
        sources = [(inspect.getsource(bindings), 'BindingError'),
                   (inspect.getsource(bridge), 'BindingError'),
                   (inspect.getsource(memory.verify_running), 'MemoryError_')]
        for source, name in sources:
            for node in ast.walk(ast.parse(source)):
                if not isinstance(node, ast.Call) or not node.args:
                    continue
                called = (node.func.id if isinstance(node.func, ast.Name) else
                          node.func.attr if isinstance(node.func, ast.Attribute) else None)
                if called == name and isinstance(node.args[0], ast.Constant):
                    codes.add(node.args[0].value)
        self.assertEqual(codes, set(bindings.RECOVERY))
        self.assertEqual(set(bindings.RECOVERY.values()), {'retry', 'operator_action'})
        self.assertEqual(bindings.BindingError('memory_unavailable').recovery, 'retry')
        self.assertEqual(bindings.BindingError('memory_upgrade_required').recovery, 'operator_action')

    def test_unmapped_code_is_a_programming_fault(self):
        with self.assertRaises(KeyError):
            bindings.BindingError('synthetic_unclassified_code')
