import argparse
import asyncio
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from notification_provider import Provider, MAX_NOTICE_BYTES


class ProviderTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.exe = self.root / 'synthetic-codex'
        self.options = argparse.Namespace(agent='codex', codex=str(self.exe),
            thread='synthetic-target', dsh_url=None, dsh_credentials=None)

    async def asyncTearDown(self):
        self.temp.cleanup()

    def executable(self, body):
        self.exe.write_text('#!/usr/bin/env python3\n' + body)
        self.exe.chmod(0o700)

    async def test_exact_target_and_notice_are_passed_without_a_shell(self):
        output = self.root / 'arguments.json'
        self.executable('import json,sys\nfrom pathlib import Path\n'
                        f'Path({str(output)!r}).write_text(json.dumps(sys.argv[1:]))\n')
        notice = 'content-free pointer; $(no shell execution)'
        self.assertEqual(await Provider(self.options).deliver(notice), 'delivered')
        self.assertEqual(json.loads(output.read_text()),
            ['queue', '--thread', 'synthetic-target', '--message', notice])

    async def test_failed_launch_and_nonzero_exit_have_distinct_outcomes(self):
        self.assertEqual(await Provider(self.options).deliver('notice'), 'failed')
        self.executable('raise SystemExit(3)\n')
        self.assertEqual(await Provider(self.options).deliver('notice'), 'unknown')

    async def test_timeout_after_provider_activity_is_uncertain_and_reaped(self):
        marker = self.root / 'entered'
        self.executable('import time\nfrom pathlib import Path\n'
                        f'Path({str(marker)!r}).touch()\ntime.sleep(30)\n')
        provider = Provider(self.options, timeout=.2)
        self.assertEqual(await provider.deliver('notice'), 'unknown')
        self.assertTrue(marker.exists())
        self.assertFalse(provider.active)

    async def test_provider_output_is_discarded_without_pipe_buffering(self):
        self.executable("import os\nfor _ in range(32):\n os.write(1,b'x'*65536)\n os.write(2,b'y'*65536)\n")
        self.assertEqual(await Provider(self.options).deliver('notice'), 'delivered')

    async def test_cancellation_settles_invocation_before_another_can_start(self):
        marker = self.root / 'entered'
        self.executable('import time\nfrom pathlib import Path\n'
                        f'Path({str(marker)!r}).touch()\ntime.sleep(30)\n')
        provider = Provider(self.options)
        task = asyncio.create_task(provider.deliver('notice'))
        try:
            async with asyncio.timeout(3):
                while not marker.exists():
                    await asyncio.sleep(.01)
            with self.assertRaisesRegex(RuntimeError, 'already active'):
                await provider.deliver('second notice')
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertFalse(provider.active)
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    async def test_cancellation_during_spawn_does_not_leave_an_unowned_process(self):
        self.executable('import time\ntime.sleep(30)\n')
        provider = Provider(self.options)
        original = asyncio.create_subprocess_exec
        entered, release = asyncio.Event(), asyncio.Event()
        children = []
        async def delayed_creation(*args, **kwargs):
            process = await original(*args, **kwargs)
            children.append(process)
            entered.set()
            await release.wait()
            return process
        with mock.patch('notification_provider.asyncio.create_subprocess_exec', delayed_creation):
            task = asyncio.create_task(provider.deliver('notice'))
            try:
                await asyncio.wait_for(entered.wait(), 3)
                task.cancel()
                await asyncio.sleep(0)
                self.assertTrue(provider.active)
                self.assertFalse(task.done())
                release.set()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                self.assertIsNotNone(children[0].returncode)
                self.assertFalse(provider.active)
            finally:
                release.set()
                if not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)

    async def test_oversized_notice_is_refused_before_process_start(self):
        marker = self.root / 'entered'
        self.executable(f'from pathlib import Path\nPath({str(marker)!r}).touch()\n')
        with self.assertRaises(ValueError):
            await Provider(self.options).deliver('x' * (MAX_NOTICE_BYTES + 1))
        self.assertFalse(marker.exists())


class DeepSeekProviderTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_child_delivers_only_to_exact_synthetic_harness_session(self):
        from http.server import BaseHTTPRequestHandler, HTTPServer
        import threading
        from test_dsh_delivery import credential_file
        seen = []
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass
            def do_POST(self):
                seen.append((self.path, json.loads(self.rfile.read(int(self.headers['Content-Length'])))))
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b'{"ok":true,"value":{"accepted":true}}')
        with tempfile.TemporaryDirectory() as tmp:
            server = HTTPServer(('127.0.0.1', 0), Handler)
            thread = threading.Thread(target=server.serve_forever)
            thread.start()
            try:
                options = argparse.Namespace(agent='deepseek', codex='/must-not-run',
                    thread='synthetic-deepseek-session',
                    dsh_url=f'http://127.0.0.1:{server.server_port}',
                    dsh_credentials=credential_file(tmp))
                self.assertEqual(await Provider(options).deliver('synthetic pointer'), 'delivered')
                self.assertEqual(len(seen), 1)
                self.assertEqual(seen[0][0], '/api/session/prompt')
                request = seen[0][1]['payload']['args']['request']
                self.assertEqual(request['sessionId'], options.thread)
                self.assertEqual(request['mode'], 'queue')
                self.assertEqual(request['content'], [dict(type='text', text='synthetic pointer')])
            finally:
                await asyncio.to_thread(server.shutdown)
                server.server_close()
                await asyncio.to_thread(thread.join)

    async def test_private_child_exit_contract_is_not_applied_to_codex(self):
        from notification_provider import CHILD_INTERNAL, CHILD_REFUSED
        for agent in ('deepseek', 'codex'):
            for code in (0, 1, CHILD_REFUSED, CHILD_INTERNAL):
                with self.subTest(agent=agent, code=code):
                    process = mock.Mock(returncode=code, communicate=mock.AsyncMock(return_value=(None, None)))
                    options = argparse.Namespace(agent=agent, codex='/synthetic', thread='synthetic',
                                                 dsh_url='http://127.0.0.1:1', dsh_credentials=None)
                    with mock.patch('notification_provider.asyncio.create_subprocess_exec',
                                    mock.AsyncMock(return_value=process)):
                        if agent == 'deepseek' and code == CHILD_INTERNAL:
                            with self.assertRaisesRegex(RuntimeError, 'internal failure'):
                                await Provider(options).deliver('notice')
                        else:
                            expected = 'delivered' if code == 0 else (
                                'failed' if agent == 'deepseek' and code == CHILD_REFUSED else 'unknown')
                            self.assertEqual(await Provider(options).deliver('notice'), expected)


class DeepSeekChildTests(unittest.TestCase):
    def child(self, raw):
        import io
        from notification_provider import deepseek_child
        with mock.patch('notification_provider.sys.stdin', mock.Mock(buffer=io.BytesIO(raw))):
            return deepseek_child(['--deepseek-child', '--url', 'http://127.0.0.1:1',
                                  '--session', 'synthetic'])

    def test_payload_refusal_precedes_any_adapter_call(self):
        from notification_provider import CHILD_REFUSED
        for raw in (b'x' * (MAX_NOTICE_BYTES + 1), b'\xff'):
            with self.subTest(size=len(raw)), mock.patch('notification_provider.dsh_delivery.deliver') as deliver:
                self.assertEqual(self.child(raw), CHILD_REFUSED)
                deliver.assert_not_called()

    def test_adapter_expected_and_programming_errors_are_distinct(self):
        import dsh_delivery
        from notification_provider import CHILD_INTERNAL
        for fault, code in ((dsh_delivery.DeliveryError('synthetic'), 1),
                            (OSError('synthetic'), 1), (TypeError('synthetic defect'), CHILD_INTERNAL)):
            with self.subTest(kind=type(fault)), mock.patch('notification_provider.dsh_delivery.deliver', side_effect=fault):
                self.assertEqual(self.child(b'notice'), code)
