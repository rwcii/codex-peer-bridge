import asyncio
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest

from database_worker import DatabaseWorker, CapacityError, WorkerClosed, WorkerFailure
from service_runtime import drain_handlers


async def reached(event):
    async with asyncio.timeout(3):
        while not event.is_set():
            await asyncio.sleep(.001)


class WorkerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / 'owned.sqlite3'
        self.entered, self.release = threading.Event(), threading.Event()
        self.threads, self.order = [], []
        test = self

        class Owned:
            def __init__(self):
                test.threads.append(threading.get_ident())
                self.db = sqlite3.connect(test.path)
                self.db.execute('CREATE TABLE data (value TEXT)')

            def write(self, value, block=False):
                test.threads.append(threading.get_ident())
                if block:
                    test.entered.set()
                    if not test.release.wait(5):
                        raise RuntimeError('test barrier timed out')
                with self.db:
                    self.db.execute('INSERT INTO data VALUES (?)', (value,))
                test.order.append(value)
                return value

            def fail(self, error):
                raise error

            def close(self):
                test.threads.append(threading.get_ident())
                self.db.close()

        self.worker = DatabaseWorker(Owned)

    async def asyncTearDown(self):
        self.release.set()
        await self.worker.close()
        self.temp.cleanup()

    def values(self):
        db = sqlite3.connect(self.path)
        try:
            return [row[0] for row in db.execute('SELECT value FROM data')]
        finally:
            db.close()

    async def test_one_thread_creates_uses_and_closes_the_connection(self):
        await self.worker.call('write', 'one')
        await self.worker.close()
        self.assertEqual(len(set(self.threads)), 1)
        self.assertNotEqual(self.threads[0], threading.get_ident())
        self.assertEqual(self.values(), ['one'])

    async def test_bound_and_control_priority_while_event_loop_runs(self):
        running = asyncio.create_task(self.worker.call('write', 'running', True))
        await reached(self.entered)
        ordinary = [asyncio.create_task(self.worker.call('write', str(i))) for i in range(16)]
        await asyncio.sleep(0)
        with self.assertRaises(CapacityError):
            await self.worker.call('write', 'excess')
        controls = [asyncio.create_task(self.worker.call('write', 'control'+str(i), priority=True))
                    for i in range(2)]
        await asyncio.sleep(0)
        with self.assertRaises(CapacityError):
            await self.worker.call('write', 'excess-control', priority=True)
        self.assertFalse(running.done())  # Loop is responsive while the worker is blocked.
        self.release.set()
        await asyncio.gather(running, *ordinary, *controls)
        self.assertEqual(self.order[:3], ['running', 'control0', 'control1'])
        self.assertEqual(len(self.values()), 19)

    async def test_cancelled_accepted_mutations_survive_and_close_drains(self):
        running = asyncio.create_task(self.worker.call('write', 'running', True))
        await reached(self.entered)
        queued = asyncio.create_task(self.worker.call('write', 'queued'))
        await asyncio.sleep(0)
        for task in (running, queued):
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        closing = asyncio.create_task(self.worker.close())
        await asyncio.sleep(0)
        self.assertFalse(closing.done())
        with self.assertRaises(WorkerClosed):
            await self.worker.call('write', 'late')
        self.release.set()
        await closing
        self.assertEqual(self.values(), ['running', 'queued'])

    async def test_cancelling_close_does_not_cancel_drain(self):
        running = asyncio.create_task(self.worker.call('write', 'running', True))
        await reached(self.entered)
        closing = asyncio.create_task(self.worker.close())
        await asyncio.sleep(0)
        closing.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await closing
        self.release.set()
        await running
        await self.worker.close()
        self.assertEqual(self.values(), ['running'])

    async def test_input_storage_and_programming_failures_are_distinct(self):
        with self.assertRaises(ValueError):
            await self.worker.call('fail', ValueError('invalid'))
        self.assertIsNone(self.worker.fault)
        for error, code in ((sqlite3.OperationalError('disk'), 'storage_error'),
                            (OSError('disk'), 'storage_error'),
                            (sqlite3.ProgrammingError('wrong thread'), 'internal_error'),
                            (AssertionError('bug'), 'internal_error')):
            with self.assertRaises(WorkerFailure) as caught:
                await self.worker.call('fail', error)
            self.assertEqual(caught.exception.code, code)
            self.assertIs(caught.exception.__cause__, error)
            self.assertEqual(self.worker.fault, code)
        await self.worker.call('write', 'still-operable')
        self.assertEqual(self.worker.fault, 'internal_error')

    async def test_initialization_failure_leaves_no_worker_thread(self):
        before = set(threading.enumerate())
        def fail():
            raise sqlite3.OperationalError('cannot open')
        with self.assertRaises(sqlite3.OperationalError):
            DatabaseWorker(fail)
        self.assertEqual(set(threading.enumerate()), before)

    async def test_handler_drain_joins_cancellation_cleanup(self):
        entered, cleaned = asyncio.Event(), asyncio.Event()
        async def handler():
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                await asyncio.sleep(.01)
                cleaned.set()
        task = asyncio.create_task(handler())
        await entered.wait()
        await drain_handlers({task}, timeout=.01)
        self.assertTrue(cleaned.is_set())
        self.assertTrue(task.cancelled())
