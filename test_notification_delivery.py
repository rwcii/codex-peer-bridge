import asyncio
from pathlib import Path
import tempfile
import unittest

import bridge
from database_worker import DatabaseWorker
import inbox_schema
from notification_delivery import DeliveryLoop
from notification_journal import JournalError
from notification_state import NotificationState
from test_memory_bindings import record, observation

CAPABILITIES = (*inbox_schema.CAPABILITIES, 'memory_binding')


class DeliveryIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.inbox = bridge.InboxStore(self.root)
        self.worker = DatabaseWorker(lambda: NotificationState(
            self.root, 'codex', 'synthetic-delivery-session', 0, inbox_schema.SCHEMA, CAPABILITIES))
        request = await self.worker.call('activation')
        evidence = self.inbox.command(request)
        await self.worker.call('confirm_activation', evidence)
        self.now = 0
        self.sent = []
        async def render(rows):
            # Content-free rendering surrogate, not a live provider adapter.
            return tuple(row['seq'] for row in rows)
        async def deliver(notice):
            self.sent.append(notice)
            return 'delivered'
        self.loop = DeliveryLoop(self.worker, render, deliver, clock=lambda: self.now)

    async def asyncTearDown(self):
        await self.worker.close()
        self.inbox.close()
        self.temp.cleanup()

    def peer(self):
        self.inbox.store(7, dict(type='user', message=dict(content='synthetic private content')))
        return inbox_schema.allocated_head(self.inbox.db)

    async def test_worker_scan_reservation_provider_and_result_complete_without_ack(self):
        self.peer()
        result = await self.loop.step()
        self.assertEqual(self.sent, [(1,)])
        self.assertEqual(result['journal']['scan_through'], 1)
        self.assertEqual(result['journal']['counters']['delivered'], 1)
        self.assertEqual(self.inbox.db.execute('SELECT seq FROM inbox').fetchall(), [(1,)])
        await self.loop.step()
        self.assertEqual(self.sent, [(1,)])

    async def test_ack_between_prepare_and_reserve_prevents_notice(self):
        self.peer()
        await self.worker.call('ready', 0)
        prepared = await self.worker.call('prepare', 0)
        self.assertEqual(prepared['sequences'], [1])
        self.inbox.command(dict(op='ack', through=1))
        self.assertEqual(await self.worker.call('reserve_next', 0), [])
        state = await self.worker.call('status')
        self.assertEqual(state['journal']['counters']['acknowledged'], 1)
        self.assertEqual(state['journal']['counters']['delivered'], 0)

    async def test_ack_during_provider_closes_responsibility_after_unknown_result(self):
        self.peer()
        async def deliver(notice):
            self.sent.append(notice)
            self.inbox.command(dict(op='ack', through=1))
            return 'unknown'
        self.loop.deliver = deliver
        await self.loop.step()
        self.now = 30
        state = await self.loop.step()
        self.assertEqual(self.sent, [(1,)])
        self.assertEqual(state['journal']['counters']['acknowledged'], 1)
        self.assertEqual(state['journal']['counters']['delivered'], 0)
        self.assertEqual(state['journal']['pending'], 0)

    async def test_status_stays_responsive_while_provider_is_waiting(self):
        self.peer()
        entered, release = asyncio.Event(), asyncio.Event()
        async def deliver(notice):
            entered.set()
            await release.wait()
            return 'delivered'
        self.loop.deliver = deliver
        task = asyncio.create_task(self.loop.step())
        try:
            await asyncio.wait_for(entered.wait(), 2)
            state = await asyncio.wait_for(self.worker.call('status', priority=True), 1)
            self.assertEqual(state['journal']['pending'], 1)
            self.assertEqual(state['journal']['counters']['attempts'], 1)
            with self.assertRaisesRegex(RuntimeError, 'concurrent'):
                await self.loop.step()
        finally:
            release.set()
            await task

    async def test_cancelled_delivery_keeps_committed_budget_and_uncertainty(self):
        self.peer()
        entered = asyncio.Event()
        async def deliver(notice):
            entered.set()
            await asyncio.Event().wait()
        self.loop.deliver = deliver
        task = asyncio.create_task(self.loop.step())
        await asyncio.wait_for(entered.wait(), 2)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.now = 1
        state = await self.loop.step()
        self.assertEqual(state['journal']['uncertain'], 1)
        self.assertEqual(state['journal']['counters']['attempts'], 1)
        self.assertEqual(self.sent, [])

    async def test_failed_unit_exhaustion_does_not_block_later_notice(self):
        for _ in range(11):
            self.peer()
        async def fail(notice):
            self.sent.append(notice)
            return 'failed'
        self.loop.deliver = fail
        for self.now in (0, 30, 90):
            await self.loop.step()
        state = await self.loop.step()
        self.assertEqual(self.sent[-1], (11,))
        self.assertEqual(state['journal']['exhausted'], 10)
        self.assertEqual(state['journal']['counters']['delivered'], 0)
        self.inbox.command(dict(op='ack', through=10))
        with self.assertRaisesRegex(JournalError, 'journal_invalid_retry'):
            await self.worker.call('retry', [1])
        state = await self.worker.call('status')
        self.assertEqual(state['journal']['exhausted'], 0)

    async def test_unseen_coalesced_pointer_gap_is_not_missing_peer_data(self):
        binding = self.inbox.bind_memory(record())
        self.inbox.refresh_memory(self.inbox.binding(binding['binding']), observation(1))
        self.inbox.refresh_memory(self.inbox.binding(binding['binding']), observation(2))
        self.inbox.unbind_memory(binding['binding'])
        self.assertEqual(self.peer(), 3)
        result = await self.loop.step()
        self.assertEqual(self.sent, [(3,)])
        self.assertEqual(result['journal']['scan_through'], 3)
        self.assertEqual(result['journal']['counters']['delivered'], 1)
        self.assertEqual(result['journal']['counters']['acknowledged'], 0)

    async def test_missing_admitted_peer_above_ack_is_diagnosed_without_sending(self):
        self.peer()
        await self.worker.call('ready', 0)
        await self.worker.call('prepare', 0)
        self.inbox.db.execute('DELETE FROM inbox WHERE seq=1')
        self.inbox.db.commit()
        with self.assertRaisesRegex(JournalError, 'journal_source_missing'):
            await self.loop.step()
        self.assertEqual(self.sent, [])
        state = await self.worker.call('status')
        self.assertEqual(state['journal']['scan_through'], 0)
        self.assertEqual(state['journal']['pending'], 1)

    async def test_inert_controls_advance_enumeration_without_provider_or_delivery_count(self):
        for _ in range(130):
            self.inbox.store(7, dict(type='control', payload='synthetic inert content'))
        first = await self.loop.step()
        self.assertEqual(first['journal']['scan_through'], 128)
        second = await self.loop.step()
        self.assertEqual(second['journal']['scan_through'], 130)
        third = await self.loop.step()
        self.assertEqual(third['journal']['counters']['ignored'], 130)
        self.assertEqual(third['journal']['counters']['delivered'], 0)
        self.assertEqual(self.sent, [])

    async def test_activation_reply_cannot_override_mismatched_committed_source(self):
        request = await self.worker.call('activation')
        evidence = dict(target_digest=request['target_digest'], nonce='e' * 32)
        with self.assertRaisesRegex(JournalError, 'journal_activation_mismatch'):
            await self.worker.call('confirm_activation', evidence)
