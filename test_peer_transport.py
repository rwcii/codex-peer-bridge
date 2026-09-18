import asyncio
import contextlib
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch, Mock

import bridge
import memory
import peer_transport as transport
import platform_support


class ControlTransportTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.root.chmod(0o700)
        self.servers = []
        self.paths = []
        self.tasks = set()

    async def asyncTearDown(self):
        for server in self.servers:
            server.close()
            await server.wait_closed()
        for task in list(self.tasks):
            task.cancel()
        await asyncio.gather(*list(self.tasks), return_exceptions=True)
        for path in self.paths:
            path.unlink(missing_ok=True)
        self.temp.cleanup()

    async def server(self, response=b'{"ok":true,"result":{}}\n', root=None):
        root = root or self.root
        transport.private_dir(root)
        path = platform_support.control_socket_path(root)
        transport.private_dir(path.parent)
        async def handle(reader, writer):
            task = asyncio.current_task()
            self.tasks.add(task)
            try:
                await reader.readline()
                if response is None:
                    await reader.read()
                else:
                    writer.write(response)
                    await writer.drain()
            except (OSError, asyncio.CancelledError):
                pass
            finally:
                writer.close()
                with contextlib.suppress(OSError):
                    await writer.wait_closed()
                self.tasks.discard(task)
        server = await asyncio.start_unix_server(handle, str(path), limit=transport.LIMIT)
        self.servers.append(server)
        self.paths.append(path)
        path.chmod(0o600)
        return path

    async def test_control_fallback_is_usable_as_service_but_never_as_message(self):
        root = self.root/('long-root-'+'x'*120)
        path = await self.server(root=root)
        self.assertTrue(path.name.endswith('-control.sock'))
        self.assertEqual(transport.service_path(root), path)
        reply, pid = await transport.control_exchange(root, {'op': 'status'})
        self.assertTrue(reply['ok'])
        self.assertEqual(pid, os.getpid())
        with self.assertRaisesRegex(ValueError, 'not a messaging'):
            transport.target_path('uds:'+str(path))
        instance = bridge.Bridge(self.root)
        with self.assertRaisesRegex(ValueError, 'not a messaging'):
            await instance.send('uds:'+str(path), 'synthetic message')
        self.assertIsNone(instance.worker)

    async def test_discovery_does_not_publish_a_control_socket_as_a_peer(self):
        path = await self.server(root=self.root/('x'*120))
        claude = self.root/'claude'
        records = claude/'sessions'
        records.mkdir(parents=True)
        record = dict(pid=os.getpid(), procStart=platform_support.proc_start(os.getpid()),
                      messagingSocketPath=str(path), name='synthetic-service', peerProtocol=1)
        (records/f'{os.getpid()}.json').write_text(json.dumps(record))
        with patch.dict(os.environ, CLAUDE_CONFIG_DIR=str(claude)):
            self.assertEqual(bridge.peers(), [])

    async def test_private_control_socket_and_object_response_are_required(self):
        path = await self.server(b'[]\n')
        with self.assertRaisesRegex(ValueError, 'response object'):
            await transport.control_exchange(self.root, {'op': 'status'})
        path.chmod(0o666)
        with self.assertRaisesRegex(ValueError, 'private'):
            await transport.control_exchange(self.root, {'op': 'status'})
        with self.assertRaisesRegex(ValueError, 'absolute'):
            transport.service_path(Path('relative'))

    async def test_unsafe_endpoint_is_not_reported_as_an_absent_memory_service(self):
        path = await self.server()
        path.chmod(0o666)
        for call in (memory.verify_running(self.root, 'synthetic'),
                     memory.request(self.root, {'op': 'hello'})):
            with self.assertRaises(memory.MemoryError_) as caught:
                await call
            self.assertEqual(caught.exception.code, 'unsafe_service_endpoint')
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            self.assertEqual(await bridge.client(self.root, {'op': 'status'}), 1)
        self.assertEqual(json.loads(output.getvalue())['code'], 'unsafe_service_endpoint')

    async def test_unsafe_directory_is_refused_even_when_socket_is_absent(self):
        self.root.chmod(0o755)
        with self.assertRaises(memory.MemoryError_) as caught:
            await memory.verify_running(self.root, 'synthetic')
        self.assertEqual(caught.exception.code, 'unsafe_service_endpoint')

    async def test_cancellation_during_cleanup_aborts_and_stays_cancelled(self):
        await self.server()
        entered = asyncio.Event()
        class Writer:
            transport = Mock()
            def get_extra_info(self, name):
                return None
            def write(self, data):
                pass
            async def drain(self):
                pass
            def close(self):
                pass
            async def wait_closed(self):
                entered.set()
                await asyncio.Future()
        class Reader:
            async def readline(self):
                return b'{"ok":true}\n'
        writer = Writer()
        async def connect(*args, **kwargs):
            return Reader(), writer
        with patch('peer_transport.asyncio.open_unix_connection', connect), \
                patch('peer_transport.credentials', return_value=os.getpid()):
            task = asyncio.create_task(transport.control_exchange(self.root, {'op': 'status'}))
            await asyncio.wait_for(entered.wait(), 1)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        writer.transport.abort.assert_called_once()

    async def test_reply_frame_bound_includes_the_newline(self):
        prefix, suffix = b'{"ok":true,"result":"', b'"}\n'
        limit = transport.LIMIT
        accepted = prefix + b'x'*(limit-len(prefix)-len(suffix)) + suffix
        await self.server(accepted)
        reply, _pid = await transport.control_exchange(self.root, {'op': 'status'})
        self.assertEqual(len(reply['result']), limit-len(prefix)-len(suffix))
        other = self.root/'over-limit'
        await self.server(accepted[:-1]+b' \n', root=other)
        with self.assertRaises(ValueError):
            await transport.control_exchange(other, {'op': 'status'})

    async def test_missing_service_does_not_create_state_directories(self):
        root = self.root/'not-started'/'memory'/'repository'
        with self.assertRaises(FileNotFoundError):
            await transport.control_exchange(root, {'op': 'hello'})
        self.assertFalse((self.root/'not-started').exists())

    async def test_request_size_is_checked_before_connection(self):
        await self.server()
        with patch('peer_transport.asyncio.open_unix_connection') as connect:
            with self.assertRaisesRegex(ValueError, 'frame too large'):
                await transport.control_exchange(self.root, {'op': 'x', 'body': 'x'*transport.LIMIT})
            connect.assert_not_called()

    async def test_timeout_closes_connection_and_does_not_claim_rollback(self):
        await self.server(None)
        with self.assertRaises(TimeoutError):
            await transport.control_exchange(self.root, {'op': 'note'}, timeout=.05)
        for _ in range(20):
            if not self.tasks:
                break
            await asyncio.sleep(.01)
        self.assertFalse(self.tasks)

    async def test_memory_no_reply_keeps_its_recovery_error(self):
        await self.server(b'')
        with self.assertRaises(memory.MemoryError_) as caught:
            await memory.request(self.root, {'op': 'note'})
        self.assertEqual(caught.exception.code, 'no_reply')

    async def test_bridge_cli_uses_shared_control_exchange(self):
        await self.server(b'{"ok":true,"result":[]}\n')
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            self.assertEqual(await bridge.client(self.root, {'op': 'inbox'}), 0)
        self.assertEqual(json.loads(output.getvalue()), {'ok': True, 'result': []})

    async def test_non_object_owner_record_is_not_reusable_identity(self):
        (self.root/'owner.json').write_text('[]')
        self.assertIsNone(memory.read_owner(self.root))

    async def test_binding_requires_connected_pid_even_when_claims_agree(self):
        generation = 'a'*32
        # No messages or commands are executed; a synthetic hello response tests
        # that filesystem metadata cannot substitute for kernel connection identity.
        result = dict(service='codex-peer-memory', repo='synthetic', protocol=memory.PROTOCOL,
                      healthy=True, pid=os.getpid()+1000000, generation=generation)
        path = await self.server(transport.encode({'ok': True, 'result': result}))
        memory.write_owner(self.root, path, generation, 'synthetic')
        owner = memory.read_owner(self.root)
        owner['pid'] = result['pid']
        (self.root/'owner.json').write_text(json.dumps(owner))
        with self.assertRaises(memory.MemoryError_) as caught:
            await memory.verify_running(self.root, 'synthetic')
        self.assertEqual(caught.exception.code, 'ownership_mismatch')


class PrivateDirectoryTests(unittest.TestCase):
    def test_missing_ancestors_are_private_under_a_permissive_umask(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            prior = os.umask(0o022)
            try:
                transport.private_dir(root/'one'/'two'/'leaf')
            finally:
                os.umask(prior)
            for path in (root/'one', root/'one'/'two', root/'one'/'two'/'leaf'):
                self.assertEqual(path.stat().st_mode & 0o777, 0o700)

    def test_existing_parent_permissions_are_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            parent = root/'existing'
            parent.mkdir(mode=0o755)
            parent.chmod(0o755)
            transport.private_dir(parent/'leaf')
            self.assertEqual(parent.stat().st_mode & 0o777, 0o755)
            self.assertEqual((parent/'leaf').stat().st_mode & 0o777, 0o700)
