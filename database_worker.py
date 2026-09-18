"""Bounded single-owner database execution; no shared SQLite connections."""
import asyncio
from collections import deque
from concurrent.futures import Future
import sqlite3
import threading


class CapacityError(ValueError):
    pass


class WorkerClosed(RuntimeError):
    pass


class WorkerFailure(RuntimeError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


async def settled(future):
    # Shield the accepted operation, not the caller. Retrieve late exceptions even
    # when the socket handler has already timed out or been cancelled.
    wrapped = asyncio.wrap_future(future)
    wrapped.add_done_callback(lambda result: None if result.cancelled() else result.exception())
    return await asyncio.shield(wrapped)


class DatabaseWorker:
    """Create, use and close the owned object on one non-daemon thread.

    Construct before accepting connections. The factory must close its own partially
    initialized resources if it fails. Admission never waits for queue space. Close
    rejects new jobs, drains every accepted job, then closes the owned object.
    """
    def __init__(self, factory):
        self._condition = threading.Condition()
        self._ordinary = deque()
        self._control = deque()
        self._closing = False
        self._ready = Future()
        self._done = Future()
        self._fault = None
        self._running = False
        self._thread = threading.Thread(target=self._run, args=(factory,), name='database-owner')
        self._thread.start()
        try:
            self._ready.result()
        except BaseException:
            self._thread.join()
            raise

    @property
    def fault(self):
        with self._condition:
            return self._fault

    def snapshot(self):
        with self._condition:
            return dict(running=self._running, ordinary_queued=len(self._ordinary),
                        control_queued=len(self._control), closing=self._closing,
                        last_fault=self._fault)

    def _run(self, factory):
        owner = None
        failure = None
        try:
            owner = factory()
            self._ready.set_result(None)
            while True:
                with self._condition:
                    while not (self._control or self._ordinary or self._closing):
                        self._condition.wait()
                    if not (self._control or self._ordinary):
                        break
                    queue = self._control if self._control else self._ordinary
                    method, args, result = queue.popleft()
                    self._running = True
                try:
                    value = getattr(owner, method)(*args)
                except Exception as exc:
                    with self._condition:
                        if isinstance(exc, sqlite3.ProgrammingError):
                            self._fault = 'internal_error'
                        elif isinstance(exc, (sqlite3.Error, OSError)):
                            self._fault = 'storage_error'
                        elif not isinstance(exc, ValueError):
                            self._fault = 'internal_error'
                    if isinstance(exc, ValueError):
                        result.set_exception(exc)
                    else:
                        failure_result = WorkerFailure(self.fault)
                        failure_result.__cause__ = exc
                        result.set_exception(failure_result)
                except BaseException as exc:
                    result.set_exception(exc)
                    raise
                else:
                    result.set_result(value)
                finally:
                    with self._condition:
                        self._running = False
        except BaseException as exc:
            failure = exc
            if not self._ready.done():
                self._ready.set_exception(exc)
        finally:
            with self._condition:
                self._closing = True
                for queue in (self._control, self._ordinary):
                    while queue:
                        queue.popleft()[2].set_exception(WorkerClosed('database worker stopped'))
            if owner is not None:
                try:
                    owner.close()
                except BaseException as exc:
                    failure = exc
            if failure is None:
                self._done.set_result(None)
            else:
                self._done.set_exception(failure)

    async def call(self, method, *args, priority=False):
        with self._condition:
            if self._closing:
                raise WorkerClosed('database worker is closing')
            queue, limit = (self._control, 2) if priority else (self._ordinary, 16)
            if len(queue) >= limit:
                raise CapacityError('database request capacity reached')
            result = Future()
            queue.append((method, args, result))
            self._condition.notify()
        return await settled(result)

    def stop_accepting(self):
        with self._condition:
            self._closing = True
            self._condition.notify()

    async def close(self):
        self.stop_accepting()
        await settled(self._done)

    def close_sync(self):
        """Startup failure cleanup only; never block a running event loop."""
        self.stop_accepting()
        self._thread.join()
        self._done.result()
