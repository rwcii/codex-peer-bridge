"""Bounded content-free change hints; durable state remains the source of truth."""
import asyncio
from contextlib import asynccontextmanager
import json
import re
import uuid

from database_worker import CapacityError
from peer_transport import encode, service_path, credentials, LIMIT
from service_runtime import close_writer

MAX_SUBSCRIPTIONS = 32
WRITE_TIMEOUT = 5
HEARTBEAT = 30


class HintHub:
    """All queue state belongs to one event loop; workers only schedule publish."""
    def __init__(self, generation):
        self.generation = generation
        self.queues = set()
        self.loop = None
        self.closed = False
        self.pending = 0

    def notify_committed(self):
        loop = self.loop
        if loop is not None:
            try:
                loop.call_soon_threadsafe(self.publish)
            except RuntimeError:
                # No live receiver remains once its event loop has closed.
                pass

    def publish(self):
        if not self.closed:
            for queue in self.queues:
                if queue.empty():
                    queue.put_nowait(True)

    def close(self):
        self.closed = True
        for queue in self.queues:
            if not queue.empty():
                queue.get_nowait()
            queue.put_nowait(None)

    async def serve(self, reader, writer):
        if self.closed:
            raise CapacityError('service is stopping')
        if self.pending >= 8:
            raise CapacityError('subscription handshake capacity reached')
        if len(self.queues) >= MAX_SUBSCRIPTIONS:
            raise CapacityError('subscription capacity reached')
        self.loop = asyncio.get_running_loop()
        queue = asyncio.Queue(maxsize=1)
        self.queues.add(queue)
        self.pending += 1
        established = False
        identity = dict(protocol=1, generation=self.generation, subscription=uuid.uuid4().hex)
        disconnected = None
        wake = None
        try:
            writer.write(encode(dict(ok=True, result=identity)))
            await asyncio.wait_for(writer.drain(), WRITE_TIMEOUT)
            self.pending -= 1
            established = True
            # Additional client bytes are invalid after the one request. EOF or
            # extra bytes both end this connection without another protocol frame.
            disconnected = asyncio.create_task(reader.read(1))
            while not self.closed:
                wake = asyncio.create_task(queue.get())
                done, _ = await asyncio.wait((wake, disconnected), timeout=HEARTBEAT,
                                             return_when=asyncio.FIRST_COMPLETED)
                if disconnected in done:
                    return
                if wake in done:
                    if wake.result() is None:
                        return
                else:
                    wake.cancel()
                    await asyncio.gather(wake, return_exceptions=True)
                wake = None
                writer.write(encode(dict(event='changed', **identity)))
                await asyncio.wait_for(writer.drain(), WRITE_TIMEOUT)
        except (OSError, TimeoutError):
            # Once streaming starts, never append an ordinary response envelope.
            pass
        finally:
            if not established:
                self.pending -= 1
            self.queues.discard(queue)
            pending = [task for task in (wake, disconnected) if task is not None]
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)


def validate(request, generation, *, repo=None):
    fields = {'op', 'protocol', 'generation'}
    if repo is not None:
        fields |= {'repo', 'consumer'}
        consumer = request.get('consumer')
        if request.get('repo') != repo or not isinstance(consumer, str) or not consumer.strip() or len(consumer) > 128:
            raise ValueError('subscription requires this repository and a stable consumer key')
    if set(request) != fields or type(request.get('protocol')) is not int or request['protocol'] != 1:
        raise ValueError('invalid subscription request')
    if request.get('generation') != generation:
        raise ValueError('subscription generation does not match')


class HintConnection:
    def __init__(self, reader, identity, pid):
        self.reader, self.identity, self.pid = reader, identity, pid

    async def changed(self):
        async with asyncio.timeout(HEARTBEAT + WRITE_TIMEOUT + 1):
            line = await self.reader.readline()
        if not line:
            raise ConnectionError('subscription ended')
        if len(line) > LIMIT:
            raise ValueError('subscription frame too large')
        frame = json.loads(line)
        if not isinstance(frame, dict) or type(frame.get('protocol')) is not int or frame != dict(event='changed', **self.identity):
            raise ValueError('invalid subscription hint')


@asynccontextmanager
async def open_hints(root, request, *, expected_pid=None):
    """Connect to an explicit service; caller verifies any stronger service identity."""
    writer = None
    try:
        async with asyncio.timeout(5):
            path = service_path(root)
            reader, writer = await asyncio.open_unix_connection(str(path), limit=LIMIT)
            pid = credentials(writer.get_extra_info('socket'))
            if expected_pid is not None and pid != expected_pid:
                raise ValueError('subscription service process changed')
            payload = encode(request)
            if len(payload) > LIMIT:
                raise ValueError('subscription request too large')
            writer.write(payload)
            await writer.drain()
            line = await reader.readline()
            if not line or len(line) > LIMIT:
                raise ValueError('invalid subscription handshake')
            reply = json.loads(line)
            if not isinstance(reply, dict) or reply.get('ok') is not True:
                raise ValueError('subscription refused')
            identity = reply.get('result')
            if (not isinstance(identity, dict) or set(identity) != {'protocol','generation','subscription'}
                    or type(identity['protocol']) is not int or identity['protocol'] != 1
                    or identity['generation'] != request.get('generation')
                    or not isinstance(identity['subscription'], str)
                    or re.fullmatch('[0-9a-f]{32}', identity['subscription']) is None):
                raise ValueError('invalid subscription identity')
        yield HintConnection(reader, identity, pid)
    finally:
        if writer is not None:
            await close_writer(writer)


async def watch_changes(open_current, rescan, stop, *, rescan_interval=2,
                        reconnect_delays=(1, 2, 4, 8, 16, 30), subscriptions_enabled=True):
    """Hints and an independent finite scan drive the same durable reconciliation.

    open_current supplies a fresh, verified service context on every reconnect.
    It must not guess a repository or participant from peer text. Set the fallback
    to None only in tests that prove the subscription path in isolation.
    """
    if not reconnect_delays or any(delay <= 0 for delay in reconnect_delays):
        raise ValueError('reconnect delays must be positive')
    if rescan_interval is not None and rescan_interval <= 0:
        raise ValueError('rescan interval must be positive')
    changed = asyncio.Queue(maxsize=1)
    def signal():
        if changed.empty():
            changed.put_nowait(True)

    async def pump():
        attempt = 0
        while not stop.is_set():
            try:
                async with open_current() as connection:
                    # Install the subscription before the first durable recheck.
                    signal()
                    while not stop.is_set():
                        await connection.changed()
                        attempt = 0
                        signal()
            except (OSError, ValueError, TimeoutError):
                delay = reconnect_delays[min(attempt, len(reconnect_delays)-1)]
                attempt += 1
                try:
                    await asyncio.wait_for(stop.wait(), delay)
                except TimeoutError:
                    pass

    task = asyncio.create_task(pump()) if subscriptions_enabled else None
    stopped = asyncio.create_task(stop.wait())
    pending = None
    try:
        while not stop.is_set():
            pending = asyncio.create_task(changed.get())
            watched = [pending, stopped] + ([task] if task is not None else [])
            done, _ = await asyncio.wait(watched, timeout=rescan_interval,
                                         return_when=asyncio.FIRST_COMPLETED)
            if stopped in done:
                return
            if task is not None and task in done:
                await task
                return
            if pending not in done:
                pending.cancel()
                await asyncio.gather(pending, return_exceptions=True)
            pending = None
            await rescan()
    finally:
        tasks = [item for item in (task, stopped, pending) if item is not None]
        for item in tasks:
            item.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
