import asyncio
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

import memory


class ExactMemoryTargetTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.repo = self.root / 'repo'
        subprocess.run(['git', 'init', '-q', str(self.repo)], check=True)
        self.repo_key = memory.repo_identity(self.repo)
        self.home = self.root / 'custom service path'
        self.home.mkdir(mode=0o700)
        self.service = memory.Service(self.home, self.repo_key,
            lambda: memory.Store(self.home / 'memory.sqlite3', self.repo_key))
        sock, _ = memory.bind_exclusive(self.home, self.repo_key, self.service.generation)
        self.output = io.StringIO()
        self.redirect = redirect_stdout(self.output)
        self.redirect.__enter__()
        self.running = asyncio.create_task(self.service.run(sock))
        async with asyncio.timeout(3):
            while not self.output.getvalue():
                if self.running.done():
                    await self.running
                await asyncio.sleep(.001)

    async def test_memory_handshake_keeps_the_old_client_literal(self):
        reply = await memory.request(self.home, dict(op='hello'))
        self.assertEqual(reply['result']['service'], 'codex-peer-memory')
        verified = await memory.verify_running(self.home, self.repo_key)
        self.assertIsNotNone(verified)
        original = self.service.command
        async def renamed_service(request, pid):
            result = await original(request, pid)
            if request.get('op') == 'hello':
                result['service'] = 'koinon-memory'
            return result
        with mock.patch.object(self.service, 'command', side_effect=renamed_service):
            with self.assertRaises(memory.MemoryError_) as raised:
                await memory.verify_running(self.home, self.repo_key)
            self.assertEqual(raised.exception.code, 'foreign_service')

    async def asyncTearDown(self):
        self.service.stop.set()
        try:
            await asyncio.wait_for(self.running, 4)
        finally:
            self.redirect.__exit__(None, None, None)
            self.temp.cleanup()

    async def cli(self, *args):
        process = await asyncio.create_subprocess_exec(sys.executable, 'memory.py',
            '--service-dir', str(self.home), '--repo-path', str(self.repo), *args,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await asyncio.wait_for(process.communicate(), 10)
        return process.returncode, stdout.decode(), stderr.decode()

    async def test_exact_custom_root_sync_does_not_append_another_memory_suffix(self):
        code, stdout, stderr = await self.cli('--consumer', 'synthetic-reader', 'sync')
        self.assertEqual(code, 0, (stdout, stderr))
        reply = json.loads(stdout)
        self.assertTrue(reply['ok'])
        self.assertEqual(reply['result']['kind'], 'snapshot')
        self.assertFalse((self.home / 'memory').exists())

    async def test_wrong_repository_is_refused_before_read_or_mutation(self):
        with self.assertRaises(memory.MemoryError_) as raised:
            await memory.request_bound(self.home, 'f' * 16, dict(op='sync', consumer='wrong'))
        self.assertEqual(raised.exception.code, 'foreign_service')
        status = await memory.request(self.home, dict(op='status'))
        self.assertEqual(status['result']['consumers'], [])

    async def test_instance_guard_refuses_changed_target_before_maintenance(self):
        reply = await memory.request(self.home, dict(op='note', repo=self.repo_key,
            generation='e' * 32, consumer='writer', type='finding', body='must not write'))
        self.assertFalse(reply['ok'])
        self.assertEqual(reply['code'], 'not_this_instance')
        status = await memory.request(self.home, dict(op='status'))
        self.assertEqual(status['result']['head'], 0)

    async def test_old_service_without_target_guard_cannot_receive_bound_request(self):
        service = await memory.verify_running(self.home, self.repo_key)
        service['capabilities'] = ['memory_subscription']
        with mock.patch.object(memory, 'verify_running', return_value=service), \
             mock.patch.object(memory, 'control_exchange') as exchange:
            with self.assertRaises(memory.MemoryError_) as raised:
                await memory.request_bound(self.home, self.repo_key, dict(op='sync', consumer='reader'))
            self.assertEqual(raised.exception.code, 'service_refused')
            exchange.assert_not_called()

    async def test_unhealthy_service_identity_does_not_blanket_block_readable_status(self):
        original = self.service.command
        async def unhealthy_hello(request, pid):
            result = await original(request, pid)
            if request.get('op') == 'hello':
                result = dict(result, healthy=False)
            return result
        with mock.patch.object(self.service, 'command', side_effect=unhealthy_hello):
            with self.assertRaises(memory.MemoryError_) as raised:
                await memory.verify_running(self.home, self.repo_key)
            self.assertEqual(raised.exception.code, 'unhealthy_service')
            reply = await memory.request_bound(self.home, self.repo_key, dict(op='status'))
            self.assertTrue(reply['ok'])
            self.assertEqual(reply['result']['repo'], self.repo_key)

    async def test_ambiguous_root_flags_are_refused(self):
        code, _stdout, stderr = await self.cli('--state-dir', str(self.root / 'other'), 'status')
        self.assertEqual(code, 2)
        self.assertIn('not allowed with argument', stderr)
