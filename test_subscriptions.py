import asyncio
from contextlib import AsyncExitStack, asynccontextmanager, redirect_stdout
import io
import json
import os
import sqlite3
from pathlib import Path
import tempfile
import threading
import unittest
from unittest import mock

import bridge
import memory
import subscriptions as sub
from database_worker import DatabaseWorker
from service_runtime import close_writer, drain_handlers

REPO = 'a'*16


async def until(predicate):
    async with asyncio.timeout(3):
        while not predicate():
            await asyncio.sleep(.001)


class SubscriptionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.service = None
        self.server = None
        self.release = threading.Event()

    async def asyncTearDown(self):
        self.release.set()
        if self.service:
            self.service.closing = True
            self.service.hints.close()
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        if self.service:
            await drain_handlers(self.service.tasks)
            await self.service.worker.close()
        self.temp.cleanup()

    async def start(self, kind, *, blocking=False):
        entered = threading.Event()
        release = self.release
        if kind == 'bridge':
            self.service = bridge.Bridge(self.root)
            class Store(bridge.InboxStore):
                def store(self, *args):
                    if blocking:
                        entered.set()
                        if not release.wait(3):
                            raise RuntimeError('test barrier expired')
                    return super().store(*args)
            def factory():
                store = Store(self.root)
                store.on_change = self.service.hints.notify_committed
                return store
            self.service.worker = DatabaseWorker(factory)
            handler = lambda r,w: self.service.handle(r,w,True)
            request = dict(op='subscribe-inbox', protocol=1, generation=self.service.generation)
        else:
            class Store(memory.Store):
                def note(self, *args, **kwargs):
                    if blocking:
                        entered.set()
                        if not release.wait(3):
                            raise RuntimeError('test barrier expired')
                    return super().note(*args, **kwargs)
            self.service = memory.Service(self.root, REPO, lambda: Store(self.root/'memory.sqlite3', REPO))
            handler = self.service.handle
            request = dict(op='subscribe', protocol=1, generation=self.service.generation, repo=REPO, consumer='synthetic')
        path = self.root/'control.sock'
        self.server = await asyncio.start_unix_server(handler, str(path))
        path.chmod(0o600)
        return request, entered

    async def write(self, kind):
        if kind == 'bridge':
            return await self.service.store(42, dict(type='user', message=dict(content='synthetic')))
        return await self.service.command(dict(op='note', consumer='synthetic', type='decision', body='synthetic'), 42)

    async def head(self, kind):
        status = await self.service.command(dict(op='status'), 42) if kind == 'memory' else await self.service.command(dict(op='status'))
        return status['head'] if kind == 'memory' else status['inbox_count']

    async def check_committed_wake(self, kind):
        request, entered = await self.start(kind, blocking=True)
        async with sub.open_hints(self.root, request, expected_pid=os.getpid()) as connection:
            write = asyncio.create_task(self.write(kind))
            await until(entered.is_set)
            wake = asyncio.create_task(connection.changed())
            await asyncio.sleep(.02)
            self.assertFalse(wake.done(), 'no wake before commit')
            write.cancel()
            await asyncio.gather(write, return_exceptions=True)
            self.release.set()
            await asyncio.wait_for(wake, 2)
            self.assertEqual(await self.head(kind), 1)
        await until(lambda: len(self.service.hints.queues) == 0)

    async def test_bridge_cancelled_waiter_still_gets_committed_wake(self):
        await self.check_committed_wake('bridge')

    async def test_memory_cancelled_waiter_still_gets_committed_wake(self):
        await self.check_committed_wake('memory')

    async def check_capacity(self, kind):
        request, _ = await self.start(kind)
        async with AsyncExitStack() as stack:
            for _ in range(32):
                await stack.enter_async_context(sub.open_hints(self.root, request))
            reply, _ = await bridge.control_exchange(self.root, request)
            self.assertFalse(reply['ok'])
            self.assertEqual(reply['code'], 'capacity')
            self.assertEqual(self.service.admission.counts['ordinary'], 0)
            self.assertEqual(self.service.admission.counts['handshake'], 0)
            await asyncio.wait_for(self.write(kind), 2)
            reply, _ = await bridge.control_exchange(self.root, dict(op='status'))
            self.assertTrue(reply['ok'])
            stop = dict(op='stop') if kind == 'bridge' else dict(op='stop', repo=REPO, generation=self.service.generation)
            reply, _ = await bridge.control_exchange(self.root, stop)
            self.assertTrue(reply['ok'])
        await until(lambda: not self.service.hints.queues)

    async def test_bridge_subscription_capacity_preserves_control_and_writes(self):
        await self.check_capacity('bridge')

    async def test_memory_subscription_capacity_preserves_control_and_writes(self):
        await self.check_capacity('memory')

    async def test_production_bridge_factory_publishes_after_commit(self):
        self.service = bridge.Bridge(self.root)
        self.service.address = 'uds:' + str(self.root/'peer.sock')
        output = io.StringIO()
        with redirect_stdout(output):
            run = asyncio.create_task(self.service.run())
            try:
                await until(lambda: bool(output.getvalue()))
                request = dict(op='subscribe-inbox', protocol=1, generation=self.service.generation)
                async with sub.open_hints(self.root, request) as connection:
                    await self.write('bridge')
                    await asyncio.wait_for(connection.changed(), 2)
                    self.assertEqual(await self.head('bridge'), 1)
            finally:
                self.service.stop.set()
                await asyncio.wait_for(run, 3)

    async def test_wrong_generation_and_repository_do_not_allocate(self):
        request, _ = await self.start('memory')
        for changed in (dict(request, generation='0'*32), dict(request, repo='b'*16), dict(request, protocol=True)):
            with self.assertRaises(ValueError):
                async with sub.open_hints(self.root, changed):
                    self.fail('invalid handshake accepted')
            self.assertEqual(len(self.service.hints.queues), 0)

    async def test_idle_hint_and_coalescing_are_content_free(self):
        request, _ = await self.start('bridge')
        with mock.patch.object(sub, 'HEARTBEAT', .03):
            async with sub.open_hints(self.root, request) as connection:
                await asyncio.wait_for(connection.changed(), 1)
                for _ in range(100):
                    self.service.hints.publish()
                self.assertEqual([queue.qsize() for queue in self.service.hints.queues], [1])
                await asyncio.wait_for(connection.changed(), 1)

    async def test_subscription_only_detects_existing_backlog_and_new_commit(self):
        request, _ = await self.start('bridge')
        await self.write('bridge')
        stop, scanned = asyncio.Event(), asyncio.Event()
        seen = []
        async def scan():
            seen.append(await self.head('bridge'))
            scanned.set()
            if seen[-1] == 2:
                stop.set()
        watch = asyncio.create_task(sub.watch_changes(lambda: sub.open_hints(self.root, request), scan, stop, rescan_interval=None))
        try:
            await asyncio.wait_for(scanned.wait(), 2)
            self.assertEqual(seen, [1])
            await self.write('bridge')
            await asyncio.wait_for(watch, 2)
            self.assertEqual(seen[-1], 2)
        finally:
            stop.set()
            watch.cancel()
            await asyncio.gather(watch, return_exceptions=True)

    async def test_rescan_only_detects_retained_work(self):
        request, _ = await self.start('bridge')
        await self.write('bridge')
        stop = asyncio.Event()
        async def scan():
            self.assertEqual(await self.head('bridge'), 1)
            stop.set()
        def forbidden():
            raise AssertionError('subscription path must be disabled')
        await asyncio.wait_for(sub.watch_changes(forbidden, scan, stop, rescan_interval=.03, subscriptions_enabled=False), 1)
        self.assertEqual(len(self.service.hints.queues), 0)

    async def test_reconnect_subscribes_before_recheck_without_fallback(self):
        request, _ = await self.start('bridge')
        stop = asyncio.Event()
        opened, scans = [], []
        @asynccontextmanager
        async def reconnecting():
            async with sub.open_hints(self.root, request) as connection:
                opened.append(connection.identity['subscription'])
                if len(opened) == 1:
                    async def lost():
                        await self.write('bridge')
                        raise ConnectionError('synthetic lost hint')
                    connection.changed = lost
                yield connection
        async def scan():
            scans.append((len(opened), await self.head('bridge')))
            if len(opened) == 2 and scans[-1][1] == 1:
                stop.set()
        await asyncio.wait_for(sub.watch_changes(reconnecting, scan, stop, rescan_interval=None, reconnect_delays=(.01,)), 2)
        self.assertEqual(len(set(opened)), 2)
        self.assertEqual(scans[-1], (2,1))


class HubBoundTests(unittest.IsolatedAsyncioTestCase):
    async def test_pending_handshake_bound_and_cancellation_cleanup(self):
        hub = sub.HintHub('a'*32)
        blocked = asyncio.Event()
        class Writer:
            def write(self, data):
                pass
            async def drain(self):
                await blocked.wait()
        tasks = [asyncio.create_task(hub.serve(asyncio.StreamReader(), Writer())) for _ in range(8)]
        try:
            await until(lambda: hub.pending == 8)
            with self.assertRaises(bridge.CapacityError):
                await hub.serve(asyncio.StreamReader(), Writer())
            self.assertEqual(len(hub.queues), 8)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        self.assertEqual(hub.pending, 0)
        self.assertEqual(len(hub.queues), 0)

    async def test_slow_subscriber_does_not_delay_fast_one(self):
        hub = sub.HintHub('a'*32)
        blocked = asyncio.Event()
        class Writer:
            def __init__(self, slow):
                self.slow = slow
                self.frames = []
            def write(self, data):
                self.frames.append(json.loads(data))
            async def drain(self):
                if self.slow and len(self.frames) > 1:
                    await blocked.wait()
        slow, fast = Writer(True), Writer(False)
        with mock.patch.object(sub, 'WRITE_TIMEOUT', .03):
            tasks = [asyncio.create_task(hub.serve(asyncio.StreamReader(), writer)) for writer in (slow,fast)]
            try:
                await until(lambda: len(hub.queues) == 2 and hub.pending == 0)
                hub.publish()
                await until(lambda: len(fast.frames) == 2)
                await until(lambda: len(hub.queues) == 1)
                self.assertEqual(fast.frames[1]['event'], 'changed')
                self.assertEqual(set(fast.frames[1]), {'event','protocol','generation','subscription'})
            finally:
                hub.close()
                await asyncio.gather(*tasks)
        self.assertFalse(hub.queues)

    async def test_watch_does_not_hide_programming_failure(self):
        @asynccontextmanager
        async def broken():
            raise RuntimeError('synthetic programming failure')
            yield
        async def scan():
            pass
        with self.assertRaisesRegex(RuntimeError, 'synthetic programming failure'):
            await asyncio.wait_for(sub.watch_changes(broken, scan, asyncio.Event(), rescan_interval=None), 1)


class CommitHintTests(unittest.TestCase):
    def test_bridge_rollback_does_not_publish(self):
        with tempfile.TemporaryDirectory() as directory:
            store = bridge.InboxStore(Path(directory))
            try:
                store.on_change = mock.Mock()
                store.db.execute("CREATE TRIGGER refuse_insert BEFORE INSERT ON inbox BEGIN SELECT RAISE(ABORT, 'synthetic'); END")
                with self.assertRaises(sqlite3.IntegrityError):
                    store.store(42, dict(type='user', message=dict(content='synthetic')))
                store.on_change.assert_not_called()
                self.assertEqual(store.db.execute('SELECT count(*) FROM inbox').fetchone()[0], 0)
            finally:
                store.close()

    def test_memory_rollback_does_not_publish(self):
        with tempfile.TemporaryDirectory() as directory:
            store = memory.Store(Path(directory)/'memory.sqlite3', REPO)
            try:
                store.on_change = mock.Mock()
                with mock.patch.object(store, 'enforce_pages', side_effect=RuntimeError('synthetic precommit failure')):
                    with self.assertRaisesRegex(RuntimeError, 'synthetic precommit failure'):
                        store.note('synthetic', 'decision', 'synthetic')
                store.on_change.assert_not_called()
                self.assertEqual(store.head(), 0)
            finally:
                store.close()
