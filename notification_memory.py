"""Bounded watchers for explicit memory bindings; hints only trigger fresh reads."""
import asyncio
from contextlib import asynccontextmanager
from concurrent.futures import ThreadPoolExecutor
import threading
from pathlib import Path

import memory
import memory_bindings
from notification_notices import consumer_key
import subscriptions


_STAMPS = ThreadPoolExecutor(max_workers=2, thread_name_prefix='memory-owner-evidence')
_STAMP_SLOTS = threading.BoundedSemaphore(2)
_UNOBSERVED = object()


async def async_owner_stamp(root):
    if not _STAMP_SLOTS.acquire(blocking=False):
        raise BlockingIOError('memory owner evidence capacity')
    try:
        future = _STAMPS.submit(owner_stamp, root)
    except BaseException:
        _STAMP_SLOTS.release()
        raise
    future.add_done_callback(lambda _result: _STAMP_SLOTS.release())
    result = asyncio.wrap_future(future)
    result.add_done_callback(lambda done: None if done.cancelled() else done.exception())
    return await asyncio.shield(result)


def owner_stamp(root):
    """Detect changed service evidence without interpreting an owner as authority."""
    try:
        info = (Path(root) / 'owner.json').lstat()
        return info.st_dev, info.st_ino, info.st_mtime_ns, info.st_size
    except FileNotFoundError:
        return None
    except OSError:
        return ('unreadable',)


class BindingWatch:
    def __init__(self, binding, participant, bridge_call):
        self.binding = binding
        self.consumer = consumer_key(participant, binding['repo_key'])
        self.bridge_call = bridge_call
        self.stop = asyncio.Event()
        self.reason = None
        self.operator_stamp = None
        self.operator_blocked = False
        self.task = None

    async def refused(self, code):
        policy = memory_bindings.RECOVERY[code]
        self.reason = 'memory_operator_action' if policy == 'operator_action' else 'memory_unavailable'
        if policy == 'operator_action':
            self.operator_blocked = True
            try:
                self.operator_stamp = await async_owner_stamp(self.binding['memory_state_dir'])
            except BlockingIOError:
                self.operator_stamp = _UNOBSERVED

    async def refresh(self):
        if self.operator_blocked:
            try:
                current = await async_owner_stamp(self.binding['memory_state_dir'])
            except BlockingIOError:
                return
            if self.operator_stamp is _UNOBSERVED:
                self.operator_stamp = current
                return
            if current == self.operator_stamp:
                return
            self.operator_blocked = False
        try:
            reply = await self.bridge_call(dict(op='refresh-memory', binding=self.binding['binding']))
        except (OSError, TimeoutError):
            self.reason = 'memory_unavailable'
            return
        if reply.get('ok') is not True:
            code = reply.get('code')
            if code in ('capacity', 'rejected'):
                self.reason = 'memory_unavailable'
            else:
                await self.refused(code)
            return
        self.reason = None

    @asynccontextmanager
    async def open_current(self):
        if self.operator_blocked:
            raise ConnectionError('memory requires operator action')
        try:
            service = await memory.verify_running(self.binding['memory_state_dir'], self.binding['repo_key'])
            if service is None:
                raise ConnectionError('memory service unavailable')
            if 'memory_subscription' not in service.get('capabilities', []):
                await self.refused('memory_upgrade_required')
                raise ConnectionError('memory subscription upgrade required')
        except memory.MemoryError_ as exc:
            await self.refused(exc.code)
            raise ConnectionError('memory identity unavailable') from None
        request = dict(op='subscribe', protocol=1, generation=service['generation'],
                       repo=self.binding['repo_key'], consumer=self.consumer)
        owner = memory.read_owner(self.binding['memory_state_dir'])
        async with subscriptions.open_hints(self.binding['memory_state_dir'], request,
                expected_pid=service['pid'], legacy_socket=owner.get('socket') if owner else None) as connection:
            yield connection

    async def run(self):
        await subscriptions.watch_changes(self.open_current, self.refresh, self.stop)

    async def close(self):
        self.stop.set()
        if self.task is not None:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)


class MemoryWatches:
    """At most one watcher per binding instance, with no memory cursor mutations."""
    def __init__(self, participant, bridge_call):
        self.participant, self.bridge_call = participant, bridge_call
        self.watches = {}
        self.bindings = {}

    async def update(self, bindings):
        if (not isinstance(bindings, list) or len(bindings) > memory_bindings.MAX_BINDINGS
                or len({item['binding'] for item in bindings}) != len(bindings)):
            raise ValueError('invalid memory binding list')
        fresh = {item['binding']: item for item in bindings}
        for key, watch in list(self.watches.items()):
            replacement = fresh.get(key)
            if replacement is None or replacement['binding_instance'] != watch.binding['binding_instance']:
                await watch.close()
                del self.watches[key]
        self.bindings = fresh
        for key, binding in fresh.items():
            if key not in self.watches:
                watch = BindingWatch(binding, self.participant, self.bridge_call)
                self.watches[key] = watch
                watch.task = asyncio.create_task(watch.run())
            else:
                watch = self.watches[key]
                # An explicit successful refresh is also operator evidence of repair.
                if (watch.operator_blocked and binding.get('service_state') == 'verified'
                        and watch.binding.get('service_state') != 'verified'):
                    watch.operator_blocked = False
                watch.binding = binding

    def reasons(self):
        reasons = set()
        for watch in self.watches.values():
            if watch.task.done():
                # Retrieve and propagate a programming failure; do not report it as
                # ordinary memory unavailability or leave a dead watcher unnoticed.
                watch.task.result()
                if not watch.stop.is_set():
                    raise RuntimeError('memory watcher stopped unexpectedly')
            if watch.reason is not None:
                reasons.add(watch.reason)
        return reasons

    async def close(self):
        results = await asyncio.gather(*(watch.close() for watch in self.watches.values()),
                                       return_exceptions=True)
        self.watches.clear()
        self.bindings.clear()
        failures = [value for value in results if isinstance(value, BaseException)]
        if failures:
            raise BaseExceptionGroup('memory watcher shutdown failed', failures)
