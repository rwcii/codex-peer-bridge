import argparse
import asyncio
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import tempfile
import unittest

import bridge
import memory
import subprocess
from notification_runtime import Runtime, RuntimeRefusal, create_worker
import threading
from unittest import mock
import platform_support
from participant_lock import identity
from peer_transport import control_exchange


class SyntheticProvider:
    def __init__(self):
        self.messages = []
        self.outcome = 'delivered'
        self.entered = asyncio.Event()
        self.release = None

    async def deliver(self, text):
        self.messages.append(text)
        self.entered.set()
        if self.release is not None:
            await self.release.wait()
        return self.outcome


class RuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.state = self.root / 'state'
        self.state.mkdir(mode=0o700)
        self.bus = bridge.Bridge(self.state)
        self.bus.address = 'uds:' + str(self.root / 'synthetic-peer.sock')
        self.capture = io.StringIO()
        self.redirect = redirect_stdout(self.capture)
        self.redirect.__enter__()
        self.bus_task = asyncio.create_task(self.bus.run())
        async with asyncio.timeout(4):
            while self.bus.worker is None or not self.capture.getvalue():
                if self.bus_task.done():
                    await self.bus_task
                await asyncio.sleep(.001)
        self.provider = SyntheticProvider()
        options = argparse.Namespace(agent='codex', thread='synthetic-runtime-session', after=0,
                                     name='synthetic-notifier', repo=str(self.root))
        self.runtime = Runtime(options, self.state, identity('codex', options.thread),
                               provider=self.provider, registry=self.root / 'registry')
        self.task = asyncio.create_task(self.runtime.run())
        async with asyncio.timeout(5):
            while self.runtime.delivery is None or self.runtime.last_status is None or not (self.state / 'notify-ready.json').exists():
                if self.task.done():
                    await self.task
                await asyncio.sleep(.005)

    async def asyncTearDown(self):
        self.runtime.stop.set()
        self.bus.stop.set()
        if self.provider.release is not None:
            self.provider.release.set()
        try:
            await asyncio.wait_for(asyncio.gather(self.task, self.bus_task), 6)
        finally:
            self.redirect.__exit__(None, None, None)
            self.temp.cleanup()

    async def request(self, op, **kwargs):
        reply, _ = await control_exchange(self.state / 'notifier', dict(op=op, **kwargs))
        return reply

    async def wait_for(self, predicate):
        async with asyncio.timeout(5):
            while not predicate():
                if self.task.done():
                    await self.task
                await asyncio.sleep(.01)

    async def test_registry_entrypoint_keeps_the_external_literal(self):
        record = self.root / 'registry' / f'{self.runtime.bridge["pid"]}.json'
        self.assertEqual(json.loads(record.read_text())['entrypoint'], 'codex-peer-bridge')

    async def test_committed_inbox_hint_delivers_content_free_notice(self):
        await self.bus.worker.call('store', 7, dict(type='user', message=dict(content='PRIVATE SYNTHETIC BODY')))
        await self.wait_for(lambda: len(self.provider.messages) == 1)
        self.assertNotIn('PRIVATE SYNTHETIC BODY', self.provider.messages[0])
        self.assertIn('through sequence 1', self.provider.messages[0])
        await self.wait_for(lambda: self.runtime.last_status['journal']['counters']['delivered'] == 1)
        reply = await self.request('status')
        self.assertTrue(reply['ok'])
        self.assertEqual(reply['result']['lifecycle'], 'running')
        self.assertEqual(reply['result']['delivery_health']['state'], 'healthy')
        self.assertEqual(len((await self.bus.worker.call('command', dict(op='inbox')))), 1)

    async def test_control_status_and_health_publish_continue_during_provider_wait(self):
        self.provider.release = asyncio.Event()
        await self.bus.worker.call('store', 7, dict(type='user', message=dict(content='synthetic')))
        await asyncio.wait_for(self.provider.entered.wait(), 4)
        reply = await asyncio.wait_for(self.request('status'), 2)
        self.assertTrue(reply['ok'])
        self.assertEqual(reply['result']['delivery_health']['journal']['pending'], 1)
        def published_pending():
            path = self.state / 'notify-health.json'
            if not path.exists():
                return False
            value = json.loads(path.read_text())
            return value['journal'] is not None and value['journal']['pending'] == 1
        await self.wait_for(published_pending)
        self.assertFalse(self.task.done())
        self.provider.release.set()

    async def test_provider_failure_leaves_running_lifecycle_and_degraded_health(self):
        self.provider.outcome = 'unknown'
        await self.bus.worker.call('store', 7, dict(type='user', message=dict(content='synthetic')))
        await self.wait_for(lambda: self.runtime.last_status['journal']['uncertain'] == 1)
        reply = await self.request('status')
        self.assertEqual(reply['result']['lifecycle'], 'running')
        self.assertEqual(reply['result']['delivery_health']['state'], 'degraded')
        self.assertIn('uncertain_delivery', reply['result']['delivery_health']['reasons'])
        self.assertFalse(self.task.done())

    async def test_stop_control_settles_and_removes_only_owned_readiness(self):
        reply = await self.request('stop')
        self.assertTrue(reply['ok'])
        await asyncio.wait_for(self.task, 4)
        self.assertFalse((self.state / 'notify-ready.json').exists())
        self.assertTrue((self.state / 'notify-journal.sqlite3').exists())
        self.assertTrue((self.state / 'inbox.sqlite3').exists())

    async def test_unexpected_provider_timeout_is_not_reported_as_bridge_failure(self):
        async def broken_adapter(text):
            raise TimeoutError('synthetic adapter defect')
        self.provider.deliver = broken_adapter
        await self.bus.worker.call('store', 7, dict(type='user', message=dict(content='synthetic')))
        await self.wait_for(lambda: self.runtime.internal_fault)
        reply = await self.request('status')
        reasons = reply['result']['delivery_health']['reasons']
        self.assertIn('internal_error', reasons)
        self.assertNotIn('bridge_unavailable', reasons)
        self.assertFalse(self.task.done())

    async def test_retained_registry_record_is_named_and_preserved_before_journal_open(self):
        await self.request('stop')
        await self.task
        registry = self.root / 'registry'
        record = registry / f'{self.runtime.bridge["pid"]}.json'
        original = json.dumps(dict(bridgeOwner='retained-other-owner', name='synthetic-retained'))
        record.write_text(original)
        journal_before = (self.state / 'notify-journal.sqlite3').read_bytes()
        replacement = Runtime(self.runtime.options, self.state, self.runtime.participant,
                              provider=self.provider, registry=registry)
        with self.assertRaises(RuntimeRefusal) as raised:
            await replacement.run()
        self.assertEqual(raised.exception.code, 'notifier_registry_refused')
        self.assertEqual(raised.exception.path, str(record))
        self.assertEqual(record.read_text(), original)
        self.assertEqual((self.state / 'notify-journal.sqlite3').read_bytes(), journal_before)
        self.assertIsNone(replacement.worker)

    async def test_retained_control_endpoint_is_preserved_before_journal_open(self):
        await self.request('stop')
        await self.task
        endpoint = platform_support.control_socket_path(self.state / 'notifier')
        endpoint.write_bytes(b'synthetic retained endpoint')
        replacement = Runtime(self.runtime.options, self.state, self.runtime.participant,
                              provider=self.provider, registry=self.root / 'registry')
        try:
            with self.assertRaises(RuntimeRefusal) as raised:
                await replacement.run()
            self.assertEqual(raised.exception.code, 'notifier_endpoint_refused')
            self.assertEqual(endpoint.read_bytes(), b'synthetic retained endpoint')
            self.assertIsNone(replacement.worker)
        finally:
            endpoint.unlink()

    async def test_bound_memory_changes_produce_sync_pointer_without_advancing_consumer(self):
        repo = self.root / 'repository'
        subprocess.run(['git', 'init', '-q', str(repo)], check=True)
        key = memory.repo_identity(repo)
        home = self.root / 'custom-memory'
        home.mkdir(mode=0o700)
        service = memory.Service(home, key, lambda: memory.Store(home / 'memory.sqlite3', key))
        sock, _ = memory.bind_exclusive(home, key, service.generation)
        task = asyncio.create_task(service.run(sock))
        try:
            await asyncio.sleep(.02)
            await self.bus.command(dict(op='bind-memory', repo_path=str(repo), memory_state_dir=str(home)))
            await self.wait_for(lambda: any('memory pointer' in text for text in self.provider.messages))
            notice = next(text for text in self.provider.messages if 'memory pointer' in text)
            self.assertIn('--service-dir', notice)
            self.assertIn(str(home), notice)
            state = await memory.request(home, dict(op='status'))
            self.assertEqual(state['result']['consumers'], [])
            await memory.request(home, dict(op='note', consumer='synthetic-writer',
                                            type='finding', body='PRIVATE MEMORY CONTENT'))
            await self.wait_for(lambda: len(self.provider.messages) >= 2)
            self.assertNotIn('PRIVATE MEMORY CONTENT', self.provider.messages[-1])
        finally:
            service.stop.set()
            await asyncio.wait_for(task, 4)

    async def cli(self, *args):
        import notify
        output = io.StringIO()
        with mock.patch('sys.argv', ['notify.py', '--state-dir', str(self.state), *args]), redirect_stdout(output):
            code = await asyncio.to_thread(notify.main)
        return code, json.loads(output.getvalue())

    async def test_cli_status_health_ack_and_retry_preserve_source_responsibility(self):
        self.provider.outcome = 'unknown'
        await self.bus.worker.call('store', 7, dict(type='user', message=dict(content='synthetic')))
        await self.wait_for(lambda: self.runtime.last_status['journal']['uncertain'] == 1)
        code, status = await self.cli('status')
        self.assertEqual(code, 0)
        self.assertEqual(status['result']['lifecycle'], 'running')
        before = len(await self.bus.worker.call('command', dict(op='inbox')))
        code, result = await self.cli('ack-health')
        self.assertEqual(code, 0)
        self.assertTrue(result['ok'])
        code, result = await self.cli('retry', '1')
        self.assertEqual(code, 1)  # This unit is waiting for automatic retry, not exhausted.
        self.assertEqual(result['code'], 'journal_invalid_retry')
        self.assertEqual(len(await self.bus.worker.call('command', dict(op='inbox'))), before)
        code, status = await self.cli('status')
        self.assertEqual(status['result']['delivery_health']['journal']['uncertain'], 1)
        self.assertNotIn('journal_recovery_required', status['result']['delivery_health']['reasons'])
        self.assertFalse(self.runtime.operator_blocked)

    async def test_manual_control_storage_fault_is_visible_through_public_status(self):
        from notification_journal import JournalError
        original = self.runtime.worker.call
        async def fail_retry(method, *args, **kwargs):
            if method == 'retry':
                raise JournalError('journal_checkpoint_failed')
            return await original(method, *args, **kwargs)
        entered, release = asyncio.Event(), asyncio.Event()
        original_step = self.runtime.delivery.step
        async def pause_next_recheck():
            entered.set()
            await release.wait()
            return await original_step()
        with mock.patch.object(self.runtime.worker, 'call', fail_retry), \
             mock.patch.object(self.runtime.delivery, 'step', pause_next_recheck):
            try:
                # Prevent a successful recovery scan from clearing the transient
                # fault between the mutation reply and the status observation.
                await asyncio.wait_for(entered.wait(), 4)
                reply = await self.request('retry', sequences=[1])
                self.assertFalse(reply['ok'])
                self.assertEqual(reply['code'], 'journal_checkpoint_failed')
                self.assertEqual(reply['recovery'], 'retry')
                status = await self.request('status')
                self.assertEqual(status['result']['lifecycle'], 'running')
                reasons = status['result']['delivery_health']['reasons']
                self.assertIn('storage_error', reasons)
                self.assertNotIn('journal_recovery_required', reasons)
                self.assertFalse(self.runtime.operator_blocked)
            finally:
                release.set()

    async def test_cli_rebuild_refuses_foreign_target_and_preserves_original_evidence(self):
        from notification_migration import sqlite_files
        await self.request('stop')
        await self.task
        before = (self.state / 'notify-migration.json').read_bytes()
        cursor = (self.state / 'notify-cursor.json').read_bytes()
        for path in sqlite_files(self.state):
            path.unlink(missing_ok=True)  # Synthetic loss, after the journal owner stopped.
        with mock.patch('platform_support.account_home', return_value=self.root / 'account'):
            (self.root / 'account').mkdir()
            code, result = await self.cli('--thread', 'other-synthetic-target',
                                          'rebuild-journal', '--accept-history-loss')
            self.assertEqual(code, 78)
            self.assertEqual(result['code'], 'journal_identity_mismatch')
            self.assertEqual((self.state / 'notify-migration.json').read_bytes(), before)
            self.assertEqual((self.state / 'notify-cursor.json').read_bytes(), cursor)
            code, result = await self.cli('--thread', self.runtime.options.thread,
                                          'rebuild-journal', '--accept-history-loss')
            self.assertEqual(code, 0, result)
            self.assertTrue(result['result']['rebuilt'])
            self.assertTrue(result['result']['history_lost'])
            self.assertTrue(result['result']['activation_confirmed'])
            self.assertEqual((self.state / 'notify-cursor.json').read_bytes(), cursor)
            self.assertEqual(self.provider.messages, [])



class WorkerCreationCancellationTests(unittest.IsolatedAsyncioTestCase):
    async def test_repeated_cancellation_waits_for_created_worker_to_close(self):
        entered, release = threading.Event(), threading.Event()
        closing, finish = asyncio.Event(), asyncio.Event()
        closed = []
        class Owned:
            def __init__(self, factory):
                entered.set()
                if not release.wait(3):
                    raise RuntimeError('synthetic creation barrier timed out')
            async def close(self):
                closing.set()
                await finish.wait()
                closed.append(True)
        with mock.patch('notification_runtime.DatabaseWorker', Owned):
            task = asyncio.create_task(create_worker(lambda: None))
            try:
                async with asyncio.timeout(3):
                    while not entered.is_set():
                        await asyncio.sleep(.001)
                task.cancel()
                release.set()
                await asyncio.wait_for(closing.wait(), 3)
                task.cancel()
                await asyncio.sleep(0)
                self.assertFalse(task.done())
                finish.set()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                self.assertEqual(closed, [True])
            finally:
                release.set()
                finish.set()
                if not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)


class CleanupTests(unittest.IsolatedAsyncioTestCase):
    async def test_cleanup_failures_do_not_skip_workers_or_owned_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runtime = Runtime(argparse.Namespace(), root, 'synthetic', provider=SyntheticProvider())
            ready, record, control = (root / name for name in ('notify-ready.json', 'record.json', 'control.sock'))
            ready.write_text(json.dumps(dict(owner=runtime.generation)))
            record.write_text(json.dumps(dict(bridgeOwner=runtime.generation)))
            control.touch()
            info = control.stat()
            runtime.memory = mock.Mock(close=mock.AsyncMock(side_effect=RuntimeError('memory close defect')))
            runtime.worker = mock.Mock(close=mock.AsyncMock(side_effect=RuntimeError('database close defect')))
            runtime.health_worker = mock.Mock(close=mock.AsyncMock())
            server = mock.Mock(wait_closed=mock.AsyncMock())
            unlink = Path.unlink
            def fail_ready(path, *args, **kwargs):
                if path == ready:
                    raise PermissionError('synthetic ready removal refusal')
                return unlink(path, *args, **kwargs)
            with mock.patch.object(Path, 'unlink', fail_ready):
                with self.assertRaises(ExceptionGroup) as raised:
                    await runtime.cleanup([], server, record, control, (info.st_dev, info.st_ino))
            self.assertEqual(len(raised.exception.exceptions), 3)
            runtime.worker.close.assert_awaited_once()
            runtime.health_worker.close.assert_awaited_once()
            server.wait_closed.assert_awaited_once()
            self.assertTrue(ready.exists())
            self.assertFalse(record.exists())
            self.assertFalse(control.exists())

    async def test_memory_close_waits_for_all_watchers_after_one_fails(self):
        from notification_memory import MemoryWatches
        manager = MemoryWatches('synthetic', mock.AsyncMock())
        entered, release = asyncio.Event(), asyncio.Event()
        async def slow_close():
            entered.set()
            await release.wait()
        manager.watches = {'failed': mock.Mock(close=mock.AsyncMock(side_effect=RuntimeError('watcher defect'))),
                           'slow': mock.Mock(close=mock.AsyncMock(side_effect=slow_close))}
        task = asyncio.create_task(manager.close())
        try:
            await asyncio.wait_for(entered.wait(), 1)
            await asyncio.sleep(0)
            self.assertFalse(task.done())
            release.set()
            with self.assertRaises(ExceptionGroup):
                await task
            self.assertEqual(manager.watches, {})
            self.assertEqual(manager.bindings, {})
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)


class FailureClassificationTests(unittest.TestCase):
    def test_retryable_journal_faults_do_not_claim_operator_recovery(self):
        from notification_journal import JournalError
        for code, reason in (('journal_checkpoint_failed', 'storage_error'),
                             ('journal_attempt_in_progress', 'storage_wait')):
            with self.subTest(code=code):
                runtime = Runtime(argparse.Namespace(), Path('/synthetic'), 'synthetic', provider=SyntheticProvider())
                runtime.record_failure(JournalError(code))
                self.assertEqual(runtime.reason, reason)
                self.assertFalse(runtime.operator_blocked)
                self.assertFalse(runtime.internal_fault)
        runtime = Runtime(argparse.Namespace(), Path('/synthetic'), 'synthetic', provider=SyntheticProvider())
        unexpected = JournalError('journal_invalid_retry')
        runtime.record_failure(unexpected)
        self.assertEqual(runtime.reason, 'internal_error')
        self.assertTrue(runtime.internal_fault)

    def test_invalid_source_metadata_is_not_a_transient_disk_fault(self):
        from inbox_schema import InboxSchemaError
        from database_worker import WorkerFailure
        fault = InboxSchemaError('synthetic malformed acknowledgement')
        wrapped = WorkerFailure('storage_error')
        wrapped.__cause__ = fault
        for error in (fault, wrapped):
            runtime = Runtime(argparse.Namespace(), Path('/synthetic'), 'synthetic', provider=SyntheticProvider())
            runtime.record_failure(error)
            self.assertEqual(runtime.reason, 'source_fault')
            self.assertTrue(runtime.operator_blocked)

    def test_only_sqlite_lock_codes_report_storage_wait(self):
        import sqlite3
        from database_worker import WorkerFailure
        for code, wanted in ((sqlite3.SQLITE_BUSY, 'storage_wait'),
                             (sqlite3.SQLITE_LOCKED, 'storage_wait'),
                             (sqlite3.SQLITE_BUSY | (2 << 8), 'storage_wait'),
                             (sqlite3.SQLITE_IOERR, 'storage_error'),
                             (sqlite3.SQLITE_FULL, 'storage_error'),
                             (sqlite3.SQLITE_CANTOPEN, 'storage_error')):
            for wrapped in (False, True):
                with self.subTest(code=code, wrapped=wrapped):
                    runtime = Runtime(argparse.Namespace(), Path('/synthetic'), 'synthetic',
                                      provider=SyntheticProvider())
                    fault = sqlite3.OperationalError('synthetic storage failure')
                    fault.sqlite_errorcode = code
                    if wrapped:
                        worker_fault = WorkerFailure('storage_error')
                        worker_fault.__cause__ = fault
                        fault = worker_fault
                    runtime.record_failure(fault)
                    self.assertEqual(runtime.reason, wanted)
                    self.assertFalse(runtime.internal_fault)


class OperatorRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_operator_fault_suspends_delivery_but_keeps_controls_available(self):
        from notification_journal import JournalError
        from notification_runtime import ControlRefusal
        runtime = Runtime(argparse.Namespace(), Path('/synthetic'), 'synthetic', provider=SyntheticProvider())
        runtime.owner = dict(owner='a'*32, bridge_pid=1, notifier_pid=2, proc_start='synthetic')
        runtime.observe_bridge = mock.AsyncMock()
        runtime.initialize_journal = mock.AsyncMock()
        runtime.record_failure(JournalError('journal_recovery_required'))
        await runtime.scan()
        await runtime.scan()
        runtime.initialize_journal.assert_not_awaited()
        self.assertEqual((await runtime.command(dict(op='status')))['lifecycle'], 'running')
        with self.assertRaises(ControlRefusal):
            await runtime.command(dict(op='retry', sequences=[1]))
        self.assertFalse(runtime.internal_fault)
        self.assertTrue((await runtime.command(dict(op='stop')))['stopping'])


class ControlPolicyTests(unittest.TestCase):
    def test_every_control_refusal_is_enumerated_with_the_journal_recovery_vocabulary(self):
        import ast
        import notification_runtime
        from notification_journal import ERROR_POLICY
        raised = set()
        for name in ('notify.py', 'notification_runtime.py'):
            tree = ast.parse(Path(__file__).with_name(name).read_text())
            for node in ast.walk(tree):
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == 'ControlRefusal':
                    self.assertIsInstance(node.args[0], ast.Constant)
                    raised.add(node.args[0].value)
        self.assertEqual(raised, set(notification_runtime.CONTROL_POLICY))
        self.assertLessEqual(set(notification_runtime.CONTROL_POLICY.values()),
                             {value[0] for value in ERROR_POLICY.values()})
        with self.assertRaises(KeyError):
            notification_runtime.ControlRefusal('unclassified')

    def test_rebuild_reports_storage_before_capability_without_changing_evidence(self):
        import notify
        from contextlib import nullcontext
        for database_status, capabilities, code, exit_status in (
                ('busy', [], 'bridge_storage_unavailable', 75),
                ('storage_error', [], 'bridge_storage_unavailable', 75),
                ('ready', [], 'bridge_upgrade_required', 78)):
            with self.subTest(database_status=database_status), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                marker = root / 'notify-migration.json'
                marker.write_bytes(b'synthetic retained evidence')
                reply = dict(ok=True, result=dict(pid=123, database_status=database_status, capabilities=capabilities))
                output = io.StringIO()
                with mock.patch('sys.argv', ['notify.py', '--state-dir', tmp, '--thread', 'synthetic',
                                           'rebuild-journal', '--accept-history-loss']), \
                     mock.patch('notify.notifier_ownership', return_value=nullcontext()), \
                     mock.patch('peer_transport.control_exchange', mock.AsyncMock(return_value=(reply, 123))), \
                     mock.patch('notification_migration.Migration.rebuild') as rebuild, redirect_stdout(output):
                    self.assertEqual(notify.main(), exit_status)
                self.assertEqual(json.loads(output.getvalue())['code'], code)
                self.assertEqual(marker.read_bytes(), b'synthetic retained evidence')
                self.assertEqual(list(root.iterdir()), [marker])
                rebuild.assert_not_called()
