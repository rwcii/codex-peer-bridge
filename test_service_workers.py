import asyncio
import ast
from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest import mock

import bridge
import memory
from service_runtime import close_writer, drain_handlers

REPO = '0123456789abcdef'


async def until(predicate):
    async with asyncio.timeout(3):
        while not predicate():
            await asyncio.sleep(.001)


class ServiceWorkerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='worker-')
        self.root = Path(self.temp.name)
        self.release, self.entered = threading.Event(), threading.Event()
        self.services, self.servers, self.writers = [], [], []

    async def asyncTearDown(self):
        self.release.set()
        for server in self.servers:
            server.close()
        for writer in self.writers:
            await close_writer(writer)
        for service in self.services:
            await drain_handlers(service.tasks)
            await service.worker.close()
        for server in self.servers:
            await server.wait_closed()
        self.temp.cleanup()

    def memory_service(self):
        test = self
        class BlockingStore(memory.Store):
            def note(self, *args, **kwargs):
                test.entered.set()
                if not test.release.wait(5):
                    raise RuntimeError('test barrier timed out')
                return super().note(*args, **kwargs)
        service = memory.Service(self.root, REPO,
                                 lambda: BlockingStore(self.root/'memory.sqlite3', REPO))
        self.services.append(service)
        return service

    async def listen(self, handler, name='control.sock'):
        path = self.root/name
        server = await asyncio.start_unix_server(handler, path, limit=bridge.LIMIT)
        os.chmod(path, 0o600)
        self.servers.append(server)
        return path

    async def test_memory_status_and_stop_survive_ordinary_saturation(self):
        service = self.memory_service()
        await self.listen(service.handle)
        notes = []
        try:
            for i in range(16):
                notes.append(asyncio.create_task(memory.request(
                    self.root, dict(op='note', consumer='synthetic', type='decision', body=str(i)))))
                await until(lambda: service.admission.counts['ordinary'] == i+1)
            extra = await memory.request(self.root, dict(op='hello'))
            self.assertEqual(extra['code'], 'capacity')
            factories = []
            with self.assertRaises(memory.MemoryError_) as caught:
                await asyncio.to_thread(memory.start, self.root, REPO, lambda: factories.append(True))
            self.assertEqual(caught.exception.code, 'service_busy')
            self.assertEqual(factories, [])
            status = asyncio.create_task(memory.request(self.root, dict(op='status')))
            await until(lambda: service.admission.counts['control'] == 1)
            wrong = await memory.request(self.root, dict(op='stop', repo=REPO, generation='wrong'))
            self.assertEqual(wrong['code'], 'not_this_instance')
            self.assertFalse(service.stop.is_set())
            stopped = await memory.request(self.root, dict(op='stop', repo=REPO,
                                                          generation=service.generation))
            self.assertTrue(stopped['result']['stopping'])
            self.assertTrue(service.stop.is_set())
            self.assertFalse(status.done())  # No running database transaction is interrupted.
            self.release.set()
            self.assertEqual((await status)['result']['head'], 1)
            replies = await asyncio.gather(*notes)
            self.assertTrue(all(reply['ok'] for reply in replies))
        finally:
            self.release.set()
            await asyncio.gather(*notes, return_exceptions=True)

    async def test_shutdown_settles_write_after_socket_handler_is_cancelled(self):
        service = self.memory_service()
        path = self.root/'control.sock'
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.bind(str(path))
        os.chmod(path, 0o600)
        sock.listen(16)
        sock.setblocking(False)
        with redirect_stdout(io.StringIO()):
            running = asyncio.create_task(service.run(sock))
            note = None
            try:
                await until(lambda: not running.done() and bool(service.worker))
                note = asyncio.create_task(memory.request(
                    self.root, dict(op='note', consumer='synthetic', type='decision', body='accepted')))
                await until(self.entered.is_set)
                stopped = await memory.request(self.root, dict(op='stop', repo=REPO,
                                                              generation=service.generation))
                self.assertTrue(stopped['ok'])
                await until(lambda: service.closing)
                for task in list(service.tasks):
                    task.cancel()
                await asyncio.sleep(.01)
                self.assertFalse(running.done())
                self.release.set()
                await asyncio.wait_for(running, 3)
                with self.assertRaises(memory.MemoryError_) as caught:
                    await note
                self.assertEqual(caught.exception.code, 'no_reply')
            finally:
                self.release.set()
                service.stop.set()
                await asyncio.gather(running, *([note] if note else []), return_exceptions=True)
                sock.close()
        store = memory.Store(self.root/'memory.sqlite3', REPO)
        try:
            self.assertEqual(store.head(), 1)
        finally:
            store.close()

    async def test_bridge_control_is_reachable_with_sixteen_open_peers(self):
        service = bridge.Bridge(self.root)
        self.services.append(service)
        peer_path = await self.listen(service.handle, 'peer.sock')
        await self.listen(lambda r, w: service.handle(r, w, True))
        for i in range(16):
            reader, writer = await asyncio.open_unix_connection(peer_path)
            self.writers.append(writer)
            await until(lambda: service.admission.counts['ordinary'] == i+1)
        status, _pid = await bridge.control_exchange(self.root, dict(op='status'))
        self.assertEqual(status['result']['inbox_count'], 0)
        stop, _pid = await bridge.control_exchange(self.root, dict(op='stop'))
        self.assertEqual(stop['result'], 'stopping')
        self.assertTrue(service.stop.is_set())

    async def test_memory_start_failure_does_not_publish_a_listener(self):
        def fail():
            raise RuntimeError('initialization failed')
        with self.assertRaisesRegex(RuntimeError, 'initialization failed'):
            memory.Service(self.root, REPO, fail)
        self.assertFalse((self.root/'control.sock').exists())
        self.assertFalse((self.root/'owner.json').exists())

    async def test_memory_status_reports_unknown_while_database_is_blocked(self):
        service = self.memory_service()
        await self.listen(service.handle)
        note = asyncio.create_task(memory.request(
            self.root, dict(op='note', consumer='synthetic', type='decision', body='accepted')))
        try:
            await until(self.entered.is_set)
            status = (await memory.request(self.root, dict(op='status')))['result']
            self.assertEqual(status['database_status'], 'busy')
            self.assertIsNone(status['database_observed_fault'])
            self.assertTrue(status['database_worker']['running'])
            self.assertFalse(status['healthy'])
            self.assertNotIn('head', status)
            self.assertEqual(status['generation'], service.generation)
        finally:
            self.release.set()
            await note
        status = (await memory.request(self.root, dict(op='status')))['result']
        self.assertEqual(status['head'], 1)
        self.assertEqual(status['database_status'], 'ready')

    async def test_bridge_status_reports_unknown_while_database_is_blocked(self):
        test = self
        class BlockingInbox(bridge.InboxStore):
            def store(self, *args):
                test.entered.set()
                if not test.release.wait(5):
                    raise RuntimeError('test barrier timed out')
                return super().store(*args)
        with mock.patch.object(bridge, 'InboxStore', BlockingInbox):
            service = bridge.Bridge(self.root)
        self.services.append(service)
        await self.listen(lambda r,w: service.handle(r,w,True))
        write = asyncio.create_task(service.store(4242, dict(type='user', message=dict(content='accepted'))))
        try:
            await until(self.entered.is_set)
            reply, _pid = await bridge.control_exchange(self.root, dict(op='status'))
            status = reply['result']
            self.assertIsNone(status['inbox_count'])
            self.assertEqual(status['database_status'], 'busy')
            self.assertIsNone(status['database_observed_fault'])
            self.assertTrue(status['database_worker']['running'])
            stopped, _pid = await bridge.control_exchange(self.root, dict(op='stop'))
            self.assertEqual(stopped['result'], 'stopping')
        finally:
            self.release.set()
            await write
        reply, _pid = await bridge.control_exchange(self.root, dict(op='status'))
        self.assertEqual(reply['result']['inbox_count'], 1)

    async def test_unclassified_connection_bound_expires_without_occupying_ordinary_slots(self):
        service = self.memory_service()
        await self.listen(service.handle)
        for i in range(8):
            reader, writer = await asyncio.open_unix_connection(self.root/'control.sock')
            self.writers.append(writer)
            writer.write(b'{"op":')
            await writer.drain()
            await until(lambda: service.admission.counts['handshake'] == i+1)
        refused = await memory.request(self.root, dict(op='status'))
        self.assertEqual(refused['code'], 'capacity')
        self.assertEqual(service.admission.counts['ordinary'], 0)
        self.assertEqual(service.admission.counts['control'], 0)
        await until(lambda: service.admission.counts['handshake'] == 0)
        status = await memory.request(self.root, dict(op='status'))
        self.assertTrue(status['ok'])
        self.assertEqual(status['result']['database_status'], 'ready')

    async def test_start_initializes_database_before_publishing_endpoint(self):
        finished = threading.Event()
        result, errors = [], []
        def factory():
            self.entered.set()
            if not self.release.wait(5):
                raise RuntimeError('test barrier timed out')
            return memory.Store(self.root/'memory.sqlite3', REPO)
        def start():
            try:
                result.append(memory.start(self.root, REPO, factory))
            except BaseException as exc:
                errors.append(exc)
            finally:
                finished.set()
        thread = threading.Thread(target=start)
        thread.start()
        try:
            await until(self.entered.is_set)
            self.assertFalse((self.root/'control.sock').exists())
            self.assertFalse((self.root/'owner.json').exists())
        finally:
            self.release.set()
            await until(finished.is_set)
            thread.join()
        if errors:
            raise errors[0]
        started, existing = result[0]
        service, sock, control = started
        try:
            self.assertIsNone(existing)
            self.assertTrue(control.exists())
            self.assertTrue((self.root/'memory.sqlite3').exists())
        finally:
            await service.worker.close()
            sock.close()
            memory.release(self.root, control, service.generation)

    async def test_handshake_timeout_does_not_mean_service_is_absent(self):
        gate = asyncio.Event()
        async def stalled(reader, writer):
            try:
                await reader.readline()
                await gate.wait()
            finally:
                await close_writer(writer)
        await self.listen(stalled)
        exchange = memory.control_exchange
        async def short_exchange(root, payload, timeout):
            return await exchange(root, payload, timeout=.05)
        try:
            with mock.patch.object(memory, 'control_exchange', short_exchange):
                with self.assertRaises(memory.MemoryError_) as caught:
                    await memory.verify_running(self.root, REPO)
            self.assertEqual(caught.exception.code, 'service_unresponsive')
        finally:
            gate.set()

    async def test_connected_invalid_or_refused_handshake_is_not_absence(self):
        response = {}
        async def answer(reader, writer):
            try:
                await reader.readline()
                writer.write(bridge.encode(response))
                await writer.drain()
            finally:
                await close_writer(writer)
        await self.listen(answer)
        for reply, code in ((dict(ok=False, code='rejected'), 'service_refused'),
                            (dict(ok=True, result={}), 'foreign_service'),
                            (dict(ok=True, result=[]), 'invalid_service_response'),
                            (dict(ok='yes', result={}), 'invalid_service_response')):
            response.clear()
            response.update(reply)
            with self.subTest(code=code, reply=reply):
                with self.assertRaises(memory.MemoryError_) as caught:
                    await memory.verify_running(self.root, REPO)
                self.assertEqual(caught.exception.code, code)

    async def test_memory_cli_reports_retryable_and_permanent_refusals_without_tracebacks(self):
        repo_path = self.root/'repo'
        repo_path.mkdir()
        subprocess.run(['git', 'init', '-q', str(repo_path)], check=True, capture_output=True)
        repo = memory.repo_identity(repo_path)
        home = memory.state_dir(self.root/'state', repo)
        memory.private_dir(self.root/'state')
        memory.private_dir(home)
        response = {}
        async def answer(reader, writer):
            try:
                await reader.readline()
                writer.write(bridge.encode(response))
                await writer.drain()
            finally:
                await close_writer(writer)
        control = memory.platform_support.control_socket_path(home)
        memory.private_dir(control.parent)
        server = await asyncio.start_unix_server(answer, control)
        self.servers.append(server)
        os.chmod(control, 0o600)
        for reply, code, exit_status in ((dict(ok=False, code='capacity'), 'service_busy', 75),
                                        (dict(ok=True, result=dict(service='foreign')), 'foreign_service', 78)):
            response.clear()
            response.update(reply)
            with self.subTest(code=code):
                child = await asyncio.create_subprocess_exec(
                    sys.executable, str(Path(memory.__file__)), '--state-dir', str(self.root/'state'),
                    '--repo-path', str(repo_path), 'serve',
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                stdout, stderr = await asyncio.wait_for(child.communicate(), 5)
                self.assertEqual(child.returncode, exit_status, stderr.decode())
                self.assertEqual(json.loads(stdout)['code'], code)
                self.assertNotIn(b'Traceback', stderr)
                self.assertFalse((home/'memory.sqlite3').exists())
        (home/'owner.json').write_text(json.dumps(dict(repo='foreign-repository')))
        child = await asyncio.create_subprocess_exec(
            sys.executable, str(Path(memory.__file__)), '--state-dir', str(self.root/'state'),
            '--repo-path', str(repo_path), 'stop',
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await asyncio.wait_for(child.communicate(), 5)
        self.assertEqual(child.returncode, 78, stderr.decode())
        self.assertEqual(json.loads(stdout)['code'], 'wrong_repository')
        self.assertNotIn(b'Traceback', stderr)

        os.chmod(self.root/'state', 0o755)
        try:
            child = await asyncio.create_subprocess_exec(
                sys.executable, str(Path(memory.__file__)), '--state-dir', str(self.root/'state'),
                '--repo-path', str(repo_path), 'serve',
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            stdout, stderr = await asyncio.wait_for(child.communicate(), 5)
            self.assertEqual(child.returncode, 78, stderr.decode())
            self.assertEqual(json.loads(stdout)['code'], 'unsafe_state_directory')
            self.assertNotIn(b'Traceback', stderr)
        finally:
            os.chmod(self.root/'state', 0o700)

    async def test_wrapped_storage_failure_stays_visible_in_worker_diagnostics(self):
        def factory():
            store = memory.Store(self.root/'memory.sqlite3', REPO)
            store.db.execute("CREATE TRIGGER refuse_note BEFORE INSERT ON entries "
                             "BEGIN SELECT RAISE(ABORT, 'synthetic failure'); END")
            return store
        service = memory.Service(self.root, REPO, factory)
        self.services.append(service)
        await self.listen(service.handle)
        reply = await memory.request(self.root, dict(op='note', consumer='synthetic',
                                                    type='decision', body='will roll back'))
        self.assertEqual(reply['code'], 'write_failed')
        status = (await memory.request(self.root, dict(op='status')))['result']
        self.assertEqual(status['head'], 0)
        self.assertEqual(status['database_observed_fault'], 'storage_error')
        self.assertFalse(status['healthy'])

    async def test_wrapped_sqlite_programming_error_is_not_an_input_refusal(self):
        class BadBinding:
            def __init__(self, db):
                self.db = db
            def __getattr__(self, name):
                return getattr(self.db, name)
            def execute(self, sql, *args):
                if sql.startswith('INSERT INTO entries'):
                    return self.db.execute(sql, ())  # Real SQLite binding error.
                return self.db.execute(sql, *args)
        def factory():
            store = memory.Store(self.root/'memory.sqlite3', REPO)
            store.db = BadBinding(store.db)
            return store
        service = memory.Service(self.root, REPO, factory)
        self.services.append(service)
        await self.listen(service.handle)
        reply = await memory.request(self.root, dict(op='note', consumer='synthetic',
                                                    type='decision', body='will roll back'))
        self.assertEqual(reply['code'], 'internal_error')
        status = (await memory.request(self.root, dict(op='status')))['result']
        self.assertEqual(status['head'], 0)
        self.assertEqual(status['database_observed_fault'], 'internal_error')
        self.assertFalse(status['healthy'])

    async def test_factory_programming_error_is_not_reported_as_incompatible_user_data(self):
        repo_path = self.root/'repo'
        repo_path.mkdir()
        subprocess.run(['git', 'init', '-q', str(repo_path)], check=True, capture_output=True)
        script = """import memory
class ClosedDuringInspection(memory.Store):
    def classify(self, repo):
        self.db.close()
        return super().classify(repo)
memory.Store = ClosedDuringInspection
memory.main()
"""
        child = await asyncio.create_subprocess_exec(
            sys.executable, '-c', script, '--state-dir', str(self.root/'state'),
            '--repo-path', str(repo_path), 'serve',
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await asyncio.wait_for(child.communicate(), 5)
        self.assertEqual(child.returncode, 70, stderr.decode())
        reply = json.loads(stdout)
        self.assertEqual(reply['code'], 'internal_error')
        self.assertNotIn('incompatible', reply['error'])
        home = memory.state_dir(self.root/'state', memory.repo_identity(repo_path))
        self.assertFalse((home/'owner.json').exists())
        self.assertFalse(memory.platform_support.control_socket_path(home).exists())


class ErrorClassificationTests(unittest.TestCase):
    def test_every_local_recovery_code_has_exactly_one_explicit_exit_class(self):
        tree = ast.parse(Path(memory.__file__).read_text())
        raised = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == 'MemoryError_':
                self.assertTrue(node.args and isinstance(node.args[0], ast.Constant),
                                'recovery codes must be explicit literals')
                raised.add(node.args[0].value)
        classes = memory.ERROR_EXIT_CLASSES
        flattened = [code for codes in classes.values() for code in codes]
        self.assertEqual(len(flattened), len(set(flattened)), 'exit classes must not overlap')
        self.assertEqual(set(flattened), raised | memory.SYNTHESIZED_ERROR_CODES,
                         'every recovery and synthesized code needs a deliberate exit policy')
        for name, code in (('unsupported_runtime', 78), ('store_too_large', 78),
                           ('service_refused', 78), ('storage_blocked', 78),
                           ('stopping', 75), ('idem_capacity', 75), ('snapshot_capacity', 75),
                           ('no_reply', 1), ('write_failed', 1), ('internal_error', 70), ('unknown-wire-code', 1)):
            self.assertEqual(memory.memory_error_exit_status(name), code, name)

    def test_every_chained_recovery_code_declares_its_database_fault_policy(self):
        tree = ast.parse(Path(memory.__file__).read_text())
        chained = set()
        for node in ast.walk(tree):
            if (isinstance(node, ast.Raise) and node.cause is not None and
                    not (isinstance(node.cause, ast.Constant) and node.cause.value is None) and
                    isinstance(node.exc, ast.Call) and isinstance(node.exc.func, ast.Name) and
                    node.exc.func.id == 'MemoryError_'):
                self.assertIsInstance(node.exc.args[0], ast.Constant)
                chained.add(node.exc.args[0].value)
        self.assertEqual(set(memory.CHAINED_DATABASE_FAULTS), chained,
                         'new chained recovery errors require an explicit fault decision')
        self.assertEqual({code for code, fault in memory.CHAINED_DATABASE_FAULTS.items() if fault},
                         {'write_failed', 'storage_blocked'})
        for code, fault in memory.CHAINED_DATABASE_FAULTS.items():
            self.assertEqual(memory.MemoryError_(code, 'synthetic').database_fault, fault)

    def test_programming_cause_overrides_an_expected_no_fault_code(self):
        db = sqlite3.connect(':memory:')
        db.close()
        try:
            db.execute('SELECT 1')
        except sqlite3.ProgrammingError as cause:
            try:
                raise memory.MemoryError_('incompatible_store', 'synthetic inspection failure') from cause
            except memory.MemoryError_ as error:
                self.assertIsNone(memory.CHAINED_DATABASE_FAULTS[error.code])
                self.assertEqual(error.database_fault, 'internal_error')
        else:
            self.fail('closed SQLite connection did not raise ProgrammingError')

    def test_locally_emitted_errors_cannot_bypass_classification(self):
        reply = memory.local_error_reply('new-unclassified-local-code', 'synthetic')
        self.assertEqual(reply['code'], 'internal_error')
        self.assertEqual(memory.memory_error_exit_status(reply['code']), 70)
        for invalid in (None, [], {}):
            self.assertEqual(memory.memory_error_exit_status(invalid), 1)
            self.assertEqual(memory.local_error_reply(invalid, 'synthetic')['code'], 'internal_error')
        # Keep all local response construction behind this checked boundary. Received
        # wire replies are printed unchanged and intentionally have a different policy.
        tree = ast.parse(Path(memory.__file__).read_text())
        constructors = []
        for function in (node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.ClassDef))):
            for node in ast.walk(function):
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == 'dict':
                    fields = {item.arg: item.value for item in node.keywords}
                    if 'code' in fields and isinstance(fields.get('ok'), ast.Constant) and fields['ok'].value is False:
                        constructors.append(function.name)
        self.assertEqual(constructors, ['local_error_reply'])
