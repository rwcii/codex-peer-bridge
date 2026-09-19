import asyncio
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from participant_lock import identity
import notification_memory as watching
from test_memory_bindings import record


class MemoryWatchTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.binding = dict(record(), binding_instance='a' * 32, service_state='unknown')
        self.binding['memory_state_dir'] = self.temp.name
        self.calls = []
        self.response = dict(ok=True, result=dict(changed=False))
        async def call(request):
            self.calls.append(request)
            return self.response
        self.call = call
        self.participant = identity('codex', 'synthetic-memory-watcher')
        self.watch = watching.BindingWatch(self.binding, self.participant, call)

    async def test_subscription_uses_the_validated_legacy_owner_route(self):
        from contextlib import asynccontextmanager
        route = str(Path(self.temp.name) / 'control.sock')
        service = dict(pid=123, generation='b'*32, capabilities=['memory_subscription'])
        seen = []
        @asynccontextmanager
        async def hints(root, request, **kwargs):
            seen.append((root, request, kwargs))
            yield 'synthetic-connection'
        with mock.patch.object(watching.memory, 'verify_running', mock.AsyncMock(return_value=service)), \
             mock.patch.object(watching.memory, 'read_owner', return_value=dict(socket=route)), \
             mock.patch.object(watching.subscriptions, 'open_hints', hints):
            async with self.watch.open_current() as connection:
                self.assertEqual(connection, 'synthetic-connection')
        self.assertEqual(seen[0][2], dict(expected_pid=123, legacy_socket=route))
        self.assertEqual(seen[0][1]['generation'], service['generation'])
        self.assertEqual(seen[0][1]['repo'], self.binding['repo_key'])

    async def test_fallback_always_requests_a_fresh_verified_observation(self):
        await self.watch.refresh()
        await self.watch.refresh()
        self.assertEqual(self.calls, [dict(op='refresh-memory', binding=self.binding['binding'])] * 2)
        self.assertIsNone(self.watch.reason)
        self.assertFalse(any(call['op'] in ('ack', 'sync') for call in self.calls))

    async def test_unchanged_operator_refusal_does_not_repeat_remote_work(self):
        self.response = dict(ok=False, code='memory_upgrade_required')
        await self.watch.refresh()
        await self.watch.refresh()
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.watch.reason, 'memory_operator_action')
        (Path(self.temp.name) / 'owner.json').write_text('synthetic changed service evidence')
        self.response = dict(ok=True, result=dict(changed=True))
        await self.watch.refresh()
        self.assertEqual(len(self.calls), 2)
        self.assertIsNone(self.watch.reason)

    async def test_retryable_unavailability_recovers_without_recreating_binding(self):
        self.response = dict(ok=False, code='memory_unavailable')
        await self.watch.refresh()
        self.assertFalse(self.watch.operator_blocked)
        self.assertEqual(self.watch.reason, 'memory_unavailable')
        self.response = dict(ok=True, result=dict(changed=True))
        await self.watch.refresh()
        self.assertIsNone(self.watch.reason)

    async def test_unclassified_refusal_is_a_programming_fault(self):
        self.response = dict(ok=False, code='synthetic_unclassified_code')
        with self.assertRaises(KeyError):
            await self.watch.refresh()

    async def test_stale_verified_listing_does_not_clear_an_operator_refusal(self):
        async def wait_only(watch):
            await watch.stop.wait()
        with mock.patch.object(watching.BindingWatch, 'run', wait_only):
            manager = watching.MemoryWatches(self.participant, self.call)
            binding = dict(self.binding, service_state='verified')
            try:
                await manager.update([binding])
                watch = manager.watches[binding['binding']]
                await watch.refused('memory_upgrade_required')
                await manager.update([dict(binding)])
                self.assertTrue(watch.operator_blocked)
                await manager.update([dict(binding, service_state='refused')])
                await manager.update([dict(binding)])
                self.assertFalse(watch.operator_blocked)
            finally:
                await manager.close()

    async def test_rebinding_replaces_watch_and_unbinding_closes_it(self):
        async def wait_only(watch):
            await watch.stop.wait()
        with mock.patch.object(watching.BindingWatch, 'run', wait_only):
            manager = watching.MemoryWatches(self.participant, self.call)
            try:
                await manager.update([dict(self.binding)])
                old = manager.watches[self.binding['binding']]
                await manager.update([dict(self.binding, binding_instance='b' * 32)])
                self.assertTrue(old.task.done())
                new = manager.watches[self.binding['binding']]
                self.assertIsNot(old, new)
                await manager.update([])
                self.assertTrue(new.task.done())
                self.assertEqual(manager.bindings, {})
            finally:
                await manager.close()

    async def test_a_dead_watcher_cannot_silently_disappear_from_health(self):
        async def fail(watch):
            raise RuntimeError('synthetic watcher defect')
        with mock.patch.object(watching.BindingWatch, 'run', fail):
            manager = watching.MemoryWatches(self.participant, self.call)
            try:
                await manager.update([dict(self.binding)])
                await asyncio.sleep(0)
                with self.assertRaisesRegex(RuntimeError, 'synthetic watcher defect'):
                    manager.reasons()
            finally:
                await manager.close()
