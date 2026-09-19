import asyncio
from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import socket
import sqlite3
import sys
import tempfile
import unittest
from unittest import mock

import bridge
import platform_support


class BridgeStartupTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='bridge-start-')
        self.root = Path(self.temp.name)
        self.service = bridge.Bridge(self.root)
        self.service.address = 'uds:' + str(self.root/'peer.sock')
        self.control = platform_support.control_socket_path(self.root)

    async def asyncTearDown(self):
        self.temp.cleanup()

    async def test_constructor_does_not_open_database(self):
        self.assertIsNone(self.service.worker)
        self.assertFalse((self.root/'inbox.sqlite3').exists())

    async def test_existing_control_refuses_before_factory_and_preserves_store(self):
        database = self.root/'inbox.sqlite3'
        with sqlite3.connect(database) as db:
            db.execute('CREATE TABLE sentinel (value TEXT)')
            db.execute("INSERT INTO sentinel VALUES ('preserved')")
        db.close()
        before = database.read_bytes()
        held = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        held.bind(str(self.control))
        inode = self.control.stat().st_ino
        try:
            with mock.patch.object(bridge, 'InboxStore') as factory:
                with self.assertRaisesRegex(OSError, 'cannot bind'):
                    await self.service.run()
                factory.assert_not_called()
            self.assertEqual(database.read_bytes(), before)
            self.assertEqual(self.control.stat().st_ino, inode)
            self.assertFalse((self.root/'peer.sock').exists())
        finally:
            held.close()
            self.control.unlink()

    async def test_serve_cli_refuses_live_endpoint_without_creating_database(self):
        held = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        held.bind(str(self.control))
        held.listen(1)
        try:
            process = await asyncio.create_subprocess_exec(
                sys.executable, str(Path(bridge.__file__)), '--state-dir', str(self.root), 'serve',
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            stdout, stderr = await asyncio.wait_for(process.communicate(), 5)
            self.assertEqual(process.returncode, 78)
            self.assertEqual(json.loads(stdout)['code'], 'endpoint_unavailable')
            self.assertEqual(stderr, b'')
            self.assertFalse((self.root/'inbox.sqlite3').exists())
            self.assertTrue(self.control.exists())
        finally:
            held.close()

    async def test_unsafe_state_root_cli_is_structured(self):
        self.root.chmod(0o755)
        try:
            process = await asyncio.create_subprocess_exec(
                sys.executable, str(Path(bridge.__file__)), '--state-dir', str(self.root), 'serve',
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            stdout, stderr = await asyncio.wait_for(process.communicate(), 5)
            self.assertEqual(process.returncode, 78)
            self.assertEqual(json.loads(stdout)['code'], 'endpoint_unavailable')
            self.assertIn('unsafe startup directory', json.loads(stdout)['error'])
            self.assertEqual(stderr, b'')
            self.assertFalse((self.root/'inbox.sqlite3').exists())
        finally:
            self.root.chmod(0o700)

    async def test_invalid_filesystem_paths_are_structured_cli_refusals(self):
        loop = self.root/'loop'
        loop.symlink_to('loop')
        for state in (self.root/('x'*300), loop):
            with self.subTest(state=state.name):
                process = await asyncio.create_subprocess_exec(
                    sys.executable, str(Path(bridge.__file__)), '--state-dir', str(state), 'serve',
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                stdout, stderr = await asyncio.wait_for(process.communicate(), 5)
                self.assertEqual(process.returncode, 78)
                reply = json.loads(stdout)
                self.assertEqual(reply['code'], 'endpoint_unavailable')
                self.assertIn('unsafe startup directory', reply['error'])
                self.assertEqual(stderr, b'')
        self.assertFalse((self.root/'inbox.sqlite3').exists())

    async def test_both_socket_directory_checks_precede_database_open(self):
        for failed_call in (1, 2):
            with self.subTest(failed_call=failed_call):
                calls = 0
                def directory(path):
                    nonlocal calls
                    calls += 1
                    if calls == failed_call:
                        raise ValueError('synthetic unsafe directory')
                service = bridge.Bridge(self.root)
                with mock.patch.object(bridge, 'private_dir', side_effect=directory), mock.patch.object(bridge, 'InboxStore') as factory:
                    with self.assertRaisesRegex(bridge.BridgeOwnershipError, 'unsafe startup directory'):
                        await service.run()
                    factory.assert_not_called()
                self.assertFalse(self.control.exists())
                self.assertFalse((self.root/'inbox.sqlite3').exists())

    async def test_peer_bind_refusal_also_precedes_store(self):
        held = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        held.bind(self.service.address[4:])
        try:
            with mock.patch.object(bridge, 'InboxStore') as factory:
                with self.assertRaisesRegex(OSError, 'cannot bind'):
                    await self.service.run()
                factory.assert_not_called()
            self.assertFalse(self.control.exists())
            self.assertFalse((self.root/'inbox.sqlite3').exists())
            self.assertTrue(Path(self.service.address[4:]).exists())
        finally:
            held.close()

    async def test_factory_sees_exclusive_non_listening_sockets_and_failure_cleans_up(self):
        def factory(root):
            for path in (self.control, Path(self.service.address[4:])):
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as competitor:
                    with self.assertRaises(OSError):
                        competitor.bind(str(path))
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                    client.settimeout(.5)
                    with self.assertRaises(ConnectionRefusedError):
                        client.connect(str(path))
            raise RuntimeError('synthetic initialization failure')
        with mock.patch.object(bridge, 'InboxStore', side_effect=factory):
            with self.assertRaisesRegex(RuntimeError, 'synthetic initialization failure'):
                await self.service.run()
        self.assertFalse(self.control.exists())
        self.assertFalse(Path(self.service.address[4:]).exists())
        self.assertIsNone(self.service.worker)

    async def test_reservation_survives_database_close_and_normal_start_serves(self):
        test = self
        class Store(bridge.InboxStore):
            def close(self):
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as competitor:
                    with test.assertRaises(OSError):
                        competitor.bind(str(test.control))
                super().close()
        output = io.StringIO()
        with mock.patch.object(bridge, 'InboxStore', Store), redirect_stdout(output):
            running = asyncio.create_task(self.service.run())
            try:
                async with asyncio.timeout(3):
                    while not output.getvalue():
                        if running.done():
                            await running
                        await asyncio.sleep(.001)
                reply, _ = await bridge.control_exchange(self.root, {'op':'status'})
                self.assertTrue(reply['ok'])
                self.assertEqual(reply['result']['inbox_count'], 0)
            finally:
                self.service.stop.set()
                await running
        self.assertFalse(self.control.exists())
        self.assertFalse(Path(self.service.address[4:]).exists())


class BridgeClientFailureTests(unittest.IsolatedAsyncioTestCase):
    async def test_transport_errors_are_structured_and_never_retried(self):
        for error, code in [(TimeoutError(), 'service_unresponsive'),
                            (bridge.NoControlReply(), 'no_reply'),
                            (OSError('synthetic'), 'service_unavailable'),
                            (ValueError('synthetic'), 'invalid_service_response')]:
            with self.subTest(code=code):
                output = io.StringIO()
                with mock.patch.object(bridge, 'control_exchange', side_effect=error) as exchange, redirect_stdout(output):
                    result = await bridge.client(Path('/synthetic'), {'op':'ack', 'through':1})
                self.assertEqual(result, 1)
                reply = json.loads(output.getvalue())
                self.assertEqual(reply['code'], code)
                self.assertIn('unknown', reply['error'])
                exchange.assert_awaited_once()

    async def test_malformed_envelopes_are_diagnostics(self):
        for reply in ({}, {'ok':1}, {'ok':True}, {'ok':True, 'result':[None]},
                      {'ok':True, 'result':{}}):
            with self.subTest(reply=reply):
                output = io.StringIO()
                with mock.patch.object(bridge, 'control_exchange', return_value=(reply, os.getpid())), redirect_stdout(output):
                    result = await bridge.client(Path('/synthetic'), {'op':'inbox'})
                self.assertEqual(result, 1)
                self.assertEqual(json.loads(output.getvalue())['code'], 'invalid_service_response')
