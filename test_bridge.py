import asyncio
import json
import os
from pathlib import Path
import socket
import tempfile
import unittest
import bridge

class BridgeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.b = bridge.Bridge(Path(self.tmp.name))
        self.sock = Path(self.tmp.name) / 'test.sock'
        self.server = await asyncio.start_unix_server(self.b.handle, str(self.sock), limit=bridge.LIMIT)

    async def asyncTearDown(self):
        self.server.close()
        await self.server.wait_closed()
        self.b.db.close()
        self.tmp.cleanup()

    async def put(self, data):
        r,w = await asyncio.open_unix_connection(str(self.sock))
        for chunk in data:
            w.write(chunk)
            await w.drain()
        w.write_eof()
        await r.read()
        w.close()
        await w.wait_closed()

    async def test_fragmented_and_eof_frames(self):
        frame = bridge.encode({'type':'user','message':{'content':'hello'}})
        await self.put([frame[:7],frame[7:],frame.rstrip()])
        rows = await self.b.command({'op':'inbox'})
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]['peer_pid'], os.getpid())
        await self.b.command({'op':'ack','through':rows[-1]['seq']})
        self.assertEqual(await self.b.command({'op':'inbox'}), [])

    async def test_invalid_and_control_inert(self):
        for data in [b'[]\n',b'{broken\n',b'{"type":"auth","token":"test"}\n',b'x'*(bridge.LIMIT+1)+b'\n']:
            await self.put([data])
        self.assertEqual(await self.b.command({'op':'inbox'}), [])
        await self.put([bridge.encode({'type':'control','action':'rename','name':'untrusted'})])
        self.assertEqual(len(await self.b.command({'op':'inbox'})), 1)

    async def test_outbound_and_reply(self):
        folder = Path(f'/tmp/cc-socks-{os.getuid()}')
        bridge.private_dir(folder)
        target = folder / f'{os.getpid()}-abcdef12.sock'
        got = []
        async def receive(r,w):
            got.append(json.loads(await r.readline()))
            self.assertEqual(bridge.credentials(w.get_extra_info('socket')), os.getpid())
            w.close()
            await w.wait_closed()
        server = await asyncio.start_unix_server(receive, str(target))
        os.chmod(target, 0o600)
        try:
            result = await self.b.send('uds:'+str(target), 'hello peer')
            self.assertEqual(result['status'], 'transport_complete')
            self.assertEqual(got[0]['from'], self.b.address)
            self.assertEqual(got[0]['message']['content'], 'hello peer')
        finally:
            server.close()
            await server.wait_closed()
            target.unlink(missing_ok=True)

    async def test_persistence_and_size_limit(self):
        self.b.store(os.getpid(), {'type':'user','message':{'content':'saved'}})
        with self.assertRaises(ValueError):
            self.b.store(os.getpid(), {'type':'user','message':{'content':'x'*65536}})
        other = bridge.Bridge(Path(self.tmp.name))
        try:
            rows = await other.command({'op':'inbox'})
            self.assertEqual(rows[0]['frame']['message']['content'], 'saved')
        finally:
            other.db.close()

    async def test_private_control(self):
        control = Path(self.tmp.name) / 'control.sock'
        server = await asyncio.start_unix_server(lambda r,w:self.b.handle(r,w,True), str(control))
        try:
            r,w = await asyncio.open_unix_connection(str(control))
            w.write(bridge.encode({'op':'status'}))
            await w.drain()
            result = json.loads(await r.readline())
            self.assertTrue(result['ok'])
            self.assertEqual(result['result']['address'], self.b.address)
            w.close()
            await w.wait_closed()
        finally:
            server.close()
            await server.wait_closed()

    async def test_unsafe_target(self):
        with self.assertRaises(ValueError):
            bridge.target_path('uds:/tmp/arbitrary.sock')
        with self.assertRaises(ValueError):
            await self.b.send('uds:/tmp/no.sock', '')

if __name__ == '__main__':
    unittest.main()
