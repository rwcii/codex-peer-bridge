import asyncio
from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import socket
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
