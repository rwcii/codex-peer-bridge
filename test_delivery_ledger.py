import asyncio
from contextlib import closing, redirect_stdout
import io
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import time
import unittest
from unittest import mock

import bridge
import delivery_ledger as ledger
import inbox_schema
import notification_journal as journal
import participant_presence as presence
from test_notification_journal import work

FRAME = dict(msgV=1, msg_id='synthetic-message', type='user', priority='next',
             message=dict(role='user', content='synthetic content'), **{'from': 'uds:/tmp/synthetic.sock'})


class LedgerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.store = bridge.InboxStore(self.root)
        self.addCleanup(lambda: self.store.close())

    def receipt(self, seq=1):
        return self.store.command(dict(op='delivery', seq=seq))['data']

    def test_duplicate_after_ack_and_restart_preserves_original_and_identity(self):
        first = self.store.store(os.getpid(), FRAME)
        self.store.command(dict(op='ack', through=first['seq']))
        identity = ledger.identity(self.store.db)
        with closing(bridge.InboxStore(self.root)) as restarted:
            self.assertEqual(ledger.identity(restarted.db), identity)
            self.assertEqual(restarted.store(os.getpid(), FRAME), dict(seq=first['seq'], duplicate=True))
            self.assertEqual(restarted.command(dict(op='inbox')), [])
        self.assertEqual(set(self.receipt()['stages']), {'stored'})

    def test_payload_conflict_is_durable_and_does_not_replace_original(self):
        self.store.store(os.getpid(), FRAME)
        with self.assertRaisesRegex(ledger.DeliveryError, 'delivery_payload_conflict'):
            self.store.store(os.getpid(), dict(FRAME, message=dict(content='changed')))
        self.assertEqual(self.store.command(dict(op='inbox'))[0]['frame'], FRAME)
        self.assertEqual(self.receipt()['payload_conflicts']['count'], 1)
        with self.assertRaisesRegex(ledger.DeliveryError, 'delivery_payload_conflict'):
            self.store.store(os.getpid(), dict(FRAME, priority='now'))
        self.assertEqual(self.receipt()['payload_conflicts']['count'], 2)

    def test_process_lifetime_change_does_not_claim_cross_restart_dedup(self):
        with mock.patch.object(bridge.platform_support, 'proc_start', return_value='one'):
            self.store.store(os.getpid(), FRAME)
        with mock.patch.object(bridge.platform_support, 'proc_start', return_value='two'):
            result = self.store.store(os.getpid(), FRAME)
        self.assertFalse(result['duplicate'])
        self.assertEqual(len(self.store.command(dict(op='inbox'))), 2)

    def test_capacity_refuses_without_eviction_or_partial_inbox_write(self):
        with mock.patch.object(ledger, 'MAX_RECORDS', 1):
            self.store.store(os.getpid(), FRAME)
            with self.assertRaisesRegex(ledger.DeliveryError, 'delivery_capacity'):
                self.store.store(os.getpid(), dict(FRAME, msg_id='second'))
            self.assertEqual(len(self.store.command(dict(op='inbox'))), 1)
            self.assertTrue(self.store.store(os.getpid(), FRAME)['duplicate'])

    def test_expiry_never_evicts_retained_inbox_but_acknowledged_key_can_expire(self):
        with mock.patch.object(bridge.time, 'time', return_value=100):
            self.store.store(os.getpid(), FRAME)
        with mock.patch.object(bridge.time, 'time', return_value=100 + ledger.RETENTION + 1):
            self.assertTrue(self.store.store(os.getpid(), FRAME)['duplicate'])
            self.store.command(dict(op='ack', through=1))
            self.assertFalse(self.store.store(os.getpid(), FRAME)['duplicate'])
        self.assertEqual(len(self.store.command(dict(op='inbox'))), 1)

    def test_handled_is_explicit_final_idempotent_and_survives_ack(self):
        self.store.store(os.getpid(), FRAME)
        self.store.command(dict(op='ack', through=1))
        request = dict(op='handled', seq=1, outcome='refused')
        self.assertTrue(self.store.command(request)['recorded'])
        before = self.receipt()
        self.store.command(request)
        self.assertEqual(self.receipt(), before)
        with self.assertRaisesRegex(ledger.DeliveryError, 'delivery_outcome_conflict'):
            self.store.command(dict(request, outcome='done'))
        self.assertEqual(self.receipt(), before)

    def test_notified_unknown_is_distinct_and_success_upgrades_it(self):
        self.store.store(os.getpid(), FRAME)
        activation = dict(target_digest='a'*64, nonce='b'*32)
        self.store.command(dict(op='activate-notification-journal', **activation))
        item = dict(seq=1, started=1, at=2, provider='codex', outcome='unknown')
        request = dict(op='record-notification', records=[item], **activation)
        with self.assertRaisesRegex(ledger.DeliveryError, 'delivery_activation_mismatch'):
            self.store.command(dict(request, nonce='c'*32))
        self.store.command(request)
        self.assertEqual(self.receipt()['stages']['notified']['outcome'], 'unknown')
        self.store.command(dict(request, records=[dict(item, outcome='delivered', at=3)]))
        self.store.command(request)
        self.assertEqual(self.receipt()['stages']['notified']['outcome'], 'delivered')
        self.assertNotIn('handled', self.receipt()['stages'])

    def test_outgoing_reservation_is_single_attempt_until_deadline(self):
        with mock.patch.object(bridge.time, 'time', return_value=100):
            first = self.store.outgoing('uds:/tmp/target.sock', 'body', 'next', 'id', 200)
            duplicate = self.store.outgoing('uds:/tmp/target.sock', 'body', 'next', 'id', 200)
            self.assertTrue(first['new'])
            self.assertFalse(duplicate['new'])
            self.assertEqual(duplicate['status'], 'indeterminate')
            with self.assertRaisesRegex(ledger.DeliveryError, 'delivery_payload_conflict'):
                self.store.outgoing('uds:/tmp/target.sock', 'different', 'next', 'id', 200)
        with mock.patch.object(bridge.time, 'time', return_value=201):
            with self.assertRaisesRegex(ledger.DeliveryError, 'delivery_deadline_expired_or_invalid'):
                self.store.outgoing('uds:/tmp/target.sock', 'body', 'next', 'id', 200)

    def test_receipt_write_failure_rolls_back_inbox_insert(self):
        self.store.db.execute("CREATE TRIGGER fail_receipt BEFORE INSERT ON delivery_record BEGIN SELECT RAISE(ABORT,'synthetic'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.store(os.getpid(), FRAME)
        self.assertEqual(self.store.command(dict(op='inbox')), [])

    def test_schema3_migration_stored_only_and_atomic_refusal(self):
        # Build the exact predecessor schema from an otherwise current empty DB.
        db = self.store.db
        db.execute('DROP TABLE delivery_record')
        db.execute('DROP TABLE delivery_identity')
        db.execute("UPDATE inbox_meta SET value='3' WHERE key='schema'")
        db.execute('INSERT INTO inbox(received,pid,frame) VALUES(1,42,?)', (json.dumps(FRAME),))
        db.commit()
        original = ledger.initialize
        def interrupted(connection):
            original(connection)
            raise RuntimeError('synthetic crash before commit')
        with mock.patch.object(ledger, 'initialize', side_effect=interrupted):
            with self.assertRaises(RuntimeError):
                inbox_schema.initialize(db)
        self.assertEqual(inbox_schema.metadata(db, versions=(3,))['schema'], 3)
        self.assertIsNone(db.execute("SELECT name FROM sqlite_master WHERE name='delivery_record'").fetchone())
        inbox_schema.initialize(db)
        data = self.receipt()
        self.assertEqual(set(data['stages']), {'stored'})
        self.assertEqual(data['stages']['stored']['source'], 'pre_migration')
        self.assertIsNone(data['sender'])


class OutboxTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'journal.sqlite3'
        self.identity = journal.identity('codex', 'a'*64, 'b'*32, 0)
        self.store = journal.Journal(self.path, self.identity, create=True)
        self.addCleanup(lambda: self.store.close())
        self.store.confirm_activation(dict(target_digest='a'*64, nonce='b'*32))
        self.store.seed_pointers(dict(records=[]))
        self.store.ingest(dict(through=2, records=[work(1), work(2)]))

    def test_notified_evidence_survives_pruning_restart_and_lost_ack(self):
        self.store.reserve([1, 2], 10)
        self.store.resolve('delivered', 11)
        self.assertEqual(self.store.rows(), [])
        expected = self.store.receipts()
        self.assertEqual([item['seq'] for item in expected], [1, 2])
        self.store.close()
        self.store = journal.Journal(self.path, self.identity)
        self.assertEqual(self.store.receipts(), expected)
        self.store.confirm_receipts(expected)
        self.store.confirm_receipts(expected)
        self.assertEqual(self.store.receipts(), [])

    def test_recovery_records_only_uncertainty_and_later_success_upgrades(self):
        self.store.reserve([1], 10)
        self.store.recover_attempt(11)
        self.assertEqual(self.store.receipts()[0]['outcome'], 'unknown')
        old = self.store.receipts()
        self.store.reserve([1], 41)
        self.store.resolve('delivered', 42)
        self.store.confirm_receipts(old)
        self.assertEqual(self.store.receipts()[0]['outcome'], 'delivered')

    def test_outbox_capacity_backpressures_then_recovers_without_losing_work(self):
        self.store.reserve([1], 10)
        self.store.resolve('delivered', 11)
        with mock.patch.object(journal, 'MAX_WORK', 1):
            with self.assertRaisesRegex(journal.JournalError, 'notified_outbox_full'):
                self.store.reserve([2], 12)
        self.store.confirm_receipts(self.store.receipts())
        self.store.reserve([2], 12)
        self.store.resolve('delivered', 13)
        self.assertEqual(self.store.receipts()[0]['seq'], 2)

    def test_failed_provider_and_old_bridge_do_not_invent_notified(self):
        self.store.reserve([1], 1)
        self.store.resolve('failed', 2)
        self.assertEqual(self.store.receipts(), [])
        self.store.reserve([2], 3)
        self.store.resolve('delivered', 4, record_receipts=False)
        self.assertEqual(self.store.receipts(), [])

    def test_refused_bootstrap_does_not_migrate_an_existing_v1_journal(self):
        with self.store.transaction(validate=False):
            self.store.db.execute('DROP TABLE receipt_outbox')
            self.store.put('schema', 1)
        self.store.close()
        # Cleanup uses the replacement connection opened after the refusal.
        try:
            with self.assertRaisesRegex(journal.JournalError, 'journal_recovery_required'):
                journal.Journal(self.path, self.identity, bootstrap=True)
            with closing(sqlite3.connect(self.path)) as db:
                self.assertEqual(json.loads(db.execute("SELECT value FROM meta WHERE key='schema'").fetchone()[0]), 1)
                self.assertIsNone(db.execute("SELECT name FROM sqlite_master WHERE name='receipt_outbox'").fetchone())
        finally:
            self.store = journal.Journal(self.path, self.identity)

    def test_v1_journal_migration_retains_attempt_and_only_recovers_unknown(self):
        self.store.reserve([1], 10)
        with self.store.transaction(validate=False):
            self.store.db.execute('DROP TABLE receipt_outbox')
            self.store.put('schema', 1)
        self.store.close()
        self.store = journal.Journal(self.path, self.identity)
        self.assertEqual(self.store.meta()['schema'], 2)
        self.assertEqual(self.store.receipts(), [])
        self.store.recover_attempt(11)
        self.assertEqual(self.store.receipts()[0]['outcome'], 'unknown')


class PresenceTests(unittest.TestCase):
    def test_stale_missing_foreign_and_future_activity_stays_unknown(self):
        record = dict(entrypoint='cli', status='idle', statusUpdatedAt=100)
        self.assertEqual(presence.registry_activity(record, 101)['state'], 'idle')
        for altered, now in [(record, 99), (record, 15101),
                             (dict(record, entrypoint='codex-peer-bridge'), 101),
                             (dict(record, statusUpdatedAt=None), 101),
                             (dict(record, status='invented'), 101),
                             (dict(record, status=[]), 101)]:
            self.assertEqual(presence.registry_activity(altered, now)['state'], 'unknown')
        self.assertEqual(presence.registry_activity(dict(record, status='waiting'), 101)['state'], 'waiting')

    def test_priority_limits_separate_schema_reachability_and_verification(self):
        for provider in ('codex', 'deepseek'):
            declaration = presence.priority(provider)
            self.assertEqual(declaration['stored_values'], ['now', 'next', 'later'])
            self.assertEqual(declaration['notification_mapping'], 'unsupported')
            self.assertFalse(declaration['schema_support'])
            self.assertFalse(declaration['live_verification'])


class SocketEvidenceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.bus = bridge.Bridge(self.root)
        self.bus.worker = bridge.DatabaseWorker(lambda: bridge.InboxStore(self.root))
        self.path = self.root / 'control-test.sock'
        self.server = await asyncio.start_unix_server(
            lambda reader, writer: self.bus.handle(reader, writer, control=True), str(self.path))

    async def asyncTearDown(self):
        self.server.close()
        await self.server.wait_closed()
        await bridge.drain_handlers(self.bus.tasks)
        await self.bus.worker.close()
        self.temp.cleanup()

    async def test_successful_inbox_reply_records_fetched_but_not_handled(self):
        await self.bus.store(os.getpid(), FRAME)
        self.assertNotIn('fetched', (await self.bus.command(dict(op='delivery', seq=1)))['data']['stages'])
        reader, writer = await asyncio.open_unix_connection(str(self.path))
        writer.write(bridge.encode(dict(op='inbox')))
        await writer.drain()
        reply = json.loads(await reader.readline())
        self.assertEqual(reply['result'][0]['frame'], FRAME)
        await reader.read()
        writer.close()
        await writer.wait_closed()
        receipt = await self.bus.command(dict(op='delivery', seq=1))
        self.assertIn('fetched', receipt['data']['stages'])
        self.assertNotIn('handled', receipt['data']['stages'])

    async def test_fetched_write_failure_cannot_retract_entries_or_claim_evidence(self):
        await self.bus.store(os.getpid(), FRAME)
        reader, writer = await asyncio.open_unix_connection(str(self.path))
        with mock.patch.object(bridge.InboxStore, 'fetched', side_effect=sqlite3.OperationalError('synthetic')):
            writer.write(bridge.encode(dict(op='inbox')))
            await writer.drain()
            reply = json.loads(await reader.readline())
            self.assertTrue(reply['ok'])
            self.assertEqual(await reader.read(), b'')
        writer.close()
        await writer.wait_closed()
        receipt = await self.bus.command(dict(op='delivery', seq=1))
        self.assertNotIn('fetched', receipt['data']['stages'])

    async def test_fetch_evidence_timeout_never_appends_a_second_reply(self):
        import threading
        await self.bus.store(os.getpid(), FRAME)
        release = threading.Event()
        original = bridge.InboxStore.fetched
        def blocked(store, sequences):
            release.wait(2)
            return original(store, sequences)
        timeout = asyncio.timeout
        def short_control_timeout(seconds):
            return timeout(.02 if seconds == 6 else seconds)
        reader, writer = await asyncio.open_unix_connection(str(self.path))
        try:
            with mock.patch.object(bridge.InboxStore, 'fetched', blocked), mock.patch.object(bridge.asyncio, 'timeout', short_control_timeout):
                writer.write(bridge.encode(dict(op='inbox')))
                await writer.drain()
                reply = json.loads(await reader.readline())
                self.assertTrue(reply['ok'])
                self.assertEqual(await asyncio.wait_for(reader.read(), 1), b'')
        finally:
            release.set()
            writer.close()
            await writer.wait_closed()

    async def test_cancelled_native_send_is_never_replayed(self):
        reached, release = asyncio.Event(), asyncio.Event()
        frames = []
        async def peer(reader, writer):
            frames.append(json.loads(await reader.readline()))
            reached.set()
            await release.wait()
            writer.close()
            await writer.wait_closed()
        path = self.root / 'peer.sock'
        server = await asyncio.start_unix_server(peer, str(path))
        deadline = time.time() + 60
        try:
            with mock.patch.object(bridge, 'target_path', return_value=path), mock.patch.object(bridge, 'peer_token', return_value=None):
                task = asyncio.create_task(self.bus.send('uds:'+str(path), 'synthetic', 'later', 'one', deadline))
                await asyncio.wait_for(reached.wait(), 1)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                result = await self.bus.send('uds:'+str(path), 'synthetic', 'later', 'one', deadline)
                self.assertEqual(result['status'], 'indeterminate')
                self.assertEqual(result['attempts'], 1)
                self.assertEqual(len(frames), 1)
                self.assertEqual(frames[0]['priority'], 'later')
                self.assertNotIn('deadline', frames[0])
        finally:
            release.set()
            server.close()
            await server.wait_closed()
            await asyncio.sleep(.01)

    async def test_lost_control_reply_exposes_the_original_retry_identifiers(self):
        output = io.StringIO()
        request = dict(op='send', to='uds:/tmp/synthetic.sock', message='synthetic', msg_id='id', deadline=123)
        with mock.patch.object(bridge, 'control_exchange', side_effect=TimeoutError), redirect_stdout(output):
            self.assertNotEqual(await bridge.client(self.root, request), 0)
        self.assertEqual(json.loads(output.getvalue())['delivery_attempt'], dict(msg_id='id', deadline=123))


class ProviderEvidenceTests(unittest.IsolatedAsyncioTestCase):
    async def test_each_provider_records_acceptance_without_model_handling(self):
        from notification_state import NotificationState
        from notification_delivery import DeliveryLoop
        for provider in ('codex', 'deepseek'):
            with self.subTest(provider=provider), tempfile.TemporaryDirectory() as folder:
                root = Path(folder)
                inbox = bridge.InboxStore(root)
                worker = bridge.DatabaseWorker(lambda: NotificationState(
                    root, provider, 'synthetic-session', 0, inbox_schema.SCHEMA, inbox_schema.CAPABILITIES))
                try:
                    request = await worker.call('activation')
                    await worker.call('confirm_activation', inbox.command(request))
                    inbox.store(os.getpid(), FRAME)
                    notices = []
                    async def render(rows):
                        return [row['seq'] for row in rows]
                    async def deliver(notice):
                        notices.append(notice)
                        return 'delivered'
                    async def export():
                        evidence = await worker.call('receipts')
                        if evidence['records']:
                            inbox.command(dict(op='record-notification', **evidence))
                            await worker.call('confirm_receipts', evidence['records'])
                    loop = DeliveryLoop(worker, render, deliver, export=export)
                    await loop.step()
                    self.assertEqual(notices, [[1]])
                    receipt = inbox.command(dict(op='delivery', seq=1))['data']
                    self.assertEqual(receipt['stages']['notified']['provider'], provider)
                    self.assertEqual(receipt['stages']['notified']['outcome'], 'delivered')
                    self.assertNotIn('fetched', receipt['stages'])
                    self.assertNotIn('handled', receipt['stages'])
                    self.assertEqual((await worker.call('receipts'))['records'], [])
                    inbox.store(os.getpid(), dict(type='control', action='peer_message_status', orig_msg_id='synthetic-message', status='delivered'))
                    await loop.step()
                    self.assertEqual(notices, [[1]])
                finally:
                    await worker.close()
                    inbox.close()
