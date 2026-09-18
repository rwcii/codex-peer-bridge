#!/usr/bin/env python3
"""Local Claude peer protocol adapter. Python standard library only."""
import argparse
import asyncio
import json
import os
from pathlib import Path
import signal
import socket
import sqlite3
import stat
import subprocess
import time
import uuid

from database_worker import DatabaseWorker, CapacityError, WorkerFailure
from service_runtime import Admission, close_writer, drain_handlers, database_status, HANDSHAKE_TIMEOUT
from peer_guidance import PEER_GUIDANCE
import platform_support
import inbox_schema

from peer_transport import LIMIT, credentials, encode, peer_token, private_dir, target_path, control_exchange, UnsafeServiceEndpoint, NoControlReply
DEFAULT = str(Path(os.environ.get('XDG_STATE_HOME', str(Path.home() / '.local/state'))) / 'codex-peer-bridge')


def peers():
    """Allowlisted live registry metadata; never read authentication keys.

    A record is only reported when its pid is still alive and its published
    start marker still matches the live process, which is what distinguishes a
    live peer from a recycled pid. On macOS the marker is an asctime string read
    through `ps`; a failure to read it is treated as a stale record rather than
    an error, so one unreadable entry never hides the others.
    """
    folder = Path(os.environ.get('CLAUDE_CONFIG_DIR', str(Path.home()/'.claude'))) / 'sessions'
    found=[]
    for path in sorted(folder.glob('*.json')):
        if not path.stem.isdigit() or path.is_symlink():
            continue
        try:
            info=path.stat()
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_size > 65536:
                continue
            record=json.loads(path.read_text())
            pid=int(path.stem)
            if not platform_support.process_alive(pid):
                continue
            if not platform_support.same_process(record.get('procStart'), platform_support.proc_start(pid)):
                continue
            address=record.get('messagingSocketPath','')
            target_path('uds:'+address)
            found.append(dict(pid=pid,name=record.get('name'),address='uds:'+address,
                              repo=record.get('cwd'),status=record.get('status'),
                              implementation=record.get('entrypoint'),protocol=record.get('peerProtocol')))
        except (OSError,ValueError,TypeError,KeyError,IndexError,subprocess.SubprocessError):
            continue
    return found


class BridgeOwnershipError(OSError):
    """A required endpoint could not be reserved before database access."""


def startup_directory(path):
    try:
        private_dir(path)
    except (ValueError, OSError) as exc:
        raise BridgeOwnershipError(f'unsafe startup directory {path}: {exc}') from exc


class InboxStore:
    def __init__(self, root):
        self.db = sqlite3.connect(root / 'inbox.sqlite3')
        try:
            inbox_schema.initialize(self.db)
        except BaseException:
            self.db.close()
            raise

    def close(self):
        self.db.close()

    def store(self, pid, frame):
        if not isinstance(frame, dict):
            raise ValueError('expected object')
        if len(encode(frame)) > 65536:
            raise ValueError('inbox message exceeds 64 KiB')
        kind = frame.get('type')
        if kind == 'user':
            msg = frame.get('message')
            if not isinstance(msg, dict) or not isinstance(msg.get('content'), str) or not msg['content'].strip():
                raise ValueError('invalid user message')
        elif kind != 'control':
            raise ValueError('unsupported frame')
        # Store controls as inert data; never execute rename or any other action.
        if self.db.execute('SELECT count(*) FROM inbox').fetchone()[0] >= 1000:
            raise ValueError('inbox full; acknowledge older entries')
        with self.db:
            self.db.execute('INSERT INTO inbox(received,pid,frame) VALUES(?,?,?)', (time.time(), pid, json.dumps(frame)))

    def command(self, r):
        op = r['op']
        if op == 'status':
            with inbox_schema.transaction(self.db, write=False):
                state = inbox_schema.metadata(self.db)
                count = self.db.execute('SELECT count(*) FROM inbox').fetchone()[0]
            return dict(inbox_count=count, inbox_schema=state['schema'],
                        ack_through=state['ack_through'], journal_activation=state['journal_activation'],
                        capabilities=list(inbox_schema.CAPABILITIES))
        if op == 'inbox':
            rows = self.db.execute('SELECT seq,received,pid,frame FROM inbox WHERE seq>? ORDER BY seq LIMIT 10', (int(r.get('after', 0)),)).fetchall()
            entries = []
            for seq, received, pid, frame in rows:
                item = dict(seq=seq, received=received, peer_pid=pid,
                            guidance=PEER_GUIDANCE, frame=json.loads(frame))
                if len(encode(entries)) + len(encode(item)) > LIMIT - 1000:
                    break
                entries.append(item)
            return entries
        if op == 'ack':
            return inbox_schema.acknowledge(self.db, r['through'])
        if op in ('activate-notification-journal', 'rebuild-notification-journal-activation'):
            return inbox_schema.activate(self.db, r)
        raise ValueError('unknown database operation')


class Bridge:
    def __init__(self, root):
        self.root = root
        self.address = f'uds:/tmp/cc-socks/{os.getpid()}.sock'
        self.stop = asyncio.Event()
        self.generation = uuid.uuid4().hex
        # Construction must not open or migrate a database before endpoint ownership.
        self.worker = None
        self.admission = Admission()
        self.tasks = set()
        self.closing = False

    async def send(self, address, message, priority='next'):
        if not isinstance(message, str) or not message.strip():
            raise ValueError('message must be nonempty text')
        if priority not in ('now', 'next', 'later'):
            raise ValueError('invalid priority')
        frame = dict(msgV=1, msg_id=str(uuid.uuid4()), type='user', priority=priority,
                     message=dict(role='user', content=message), **{'from': self.address})
        data = encode(frame)
        path = target_path(address)
        reader, writer = await asyncio.open_unix_connection(str(path), limit=LIMIT)
        try:
            pid = credentials(writer.get_extra_info('socket'))
            token = peer_token(pid, path)
            if token:
                writer.write(encode(dict(type='auth', token=token)))
            writer.write(data)
            await writer.drain()
            writer.write_eof()
            # Transport completion is not a model acknowledgement.
            while await reader.read(4096):
                pass
        finally:
            writer.close()
            await writer.wait_closed()
        return dict(msg_id=frame['msg_id'], status='transport_complete', peer_pid=pid)

    async def store(self, pid, frame):
        return await self.worker.call('store', pid, frame)

    async def handle(self, reader, writer, control=False):
        task = asyncio.current_task()
        self.tasks.add(task)
        slot = None
        try:
            if self.closing:
                raise CapacityError('service is stopping')
            slot = self.admission.enter('handshake' if control else 'ordinary')
            pid = credentials(writer.get_extra_info('socket'))
            if control:
                async with asyncio.timeout(HANDSHAKE_TIMEOUT):
                    request = json.loads(await reader.readline())
            async with asyncio.timeout(6):
                if control:
                    if not isinstance(request, dict):
                        raise ValueError('expected operation object')
                    self.admission.leave(slot)
                    slot = None
                    slot = self.admission.enter('control' if request.get('op') in ('status', 'stop') else 'ordinary')
                    result = await self.command(request)
                    writer.write(encode(dict(ok=True, result=result)))
                    await writer.drain()
                else:
                    for _ in range(32):
                        line = await reader.readline()
                        if not line:
                            break
                        if len(line) > LIMIT:
                            raise ValueError('frame too large')
                        # Same-UID credentials are the peer authentication policy.
                        await self.store(pid, json.loads(line))
        except Exception as exc:
            if isinstance(exc, CapacityError):
                code = 'capacity'
            elif isinstance(exc, WorkerFailure):
                code = exc.code
            elif isinstance(exc, (ValueError, OSError, TimeoutError)):
                code = 'rejected'
            else:
                code = 'internal_error'
            if control:
                try:
                    writer.write(encode(dict(ok=False, code=code, error=type(exc).__name__)))
                    await asyncio.wait_for(writer.drain(), 1)
                except (OSError, TimeoutError):
                    pass
            else:
                print(f'peer request failed: {code}', flush=True)
        finally:
            try:
                await close_writer(writer)
            finally:
                if slot is not None:
                    self.admission.leave(slot)
                self.tasks.discard(task)

    async def command(self, r):
        if not isinstance(r, dict) or not isinstance(r.get('op'), str):
            raise ValueError('expected operation object')
        op = r['op']
        if op == 'status':
            state, diagnostics = await database_status(self.worker, r)
            if state is None:
                state = dict(inbox_count=None, inbox_schema=None, ack_through=None,
                             journal_activation=None, capabilities=[])
            return dict(pid=os.getpid(), address=self.address, generation=self.generation,
                        **state, **diagnostics,
                        delivery='inbox available; run notify.py to notify the selected participant session')
        if op == 'send':
            return await self.send(r.get('to'), r.get('message'), r.get('priority', 'next'))
        if op in ('inbox', 'ack'):
            if op == 'ack' and 'through' not in r:
                raise ValueError('through is required')
            key = 'after' if op == 'inbox' else 'through'
            value = r.get(key, 0)
            if isinstance(value, str) and value.isascii() and value.isdecimal():
                value = int(value)
            if type(value) is not int or value < 0:
                raise ValueError(f'{key} must be a nonnegative integer')
            if op == 'inbox':
                value = min(value, inbox_schema.MAX_SEQUENCE)
            request = dict(r, **{key: value})
            return await self.worker.call('command', request)
        if op in ('activate-notification-journal', 'rebuild-notification-journal-activation'):
            return await self.worker.call('command', r)
        if op == 'stop':
            self.stop.set()
            return 'stopping'
        raise ValueError('unknown operation')

    async def run(self):
        if self.worker is not None or self.closing:
            raise RuntimeError('bridge instance cannot be started twice')
        sockets, servers, identities = [], [], {}
        try:
            startup_directory(Path('/tmp/cc-socks'))
            peer = Path(self.address[4:])
            control = platform_support.control_socket_path(self.root)
            startup_directory(control.parent)
            # Bind exclusively. Never remove a pre-existing process socket.
            for path in (control, peer):
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                try:
                    # Bind exclusively. A pre-existing socket is never removed, on
                    # either route: a live listener and a saturated one look identical
                    # to a probe, because a full accept queue refuses a connection on
                    # macOS exactly as a dead owner does. A leftover from a killed
                    # instance is removed by hand after verifying the owner is dead.
                    sock.bind(str(path))
                except OSError as exc:
                    sock.close()
                    # Carry the path. A bare "Address already in use" does not say
                    # which socket is in the way, and deciding whether its owner is
                    # gone is the operator's call, so the message must name it.
                    raise BridgeOwnershipError(f'cannot bind {path}: {exc}') from exc
                except BaseException:
                    sock.close()
                    raise
                sockets.append((sock, path))
                info = path.lstat()
                identities[path] = (info.st_dev, info.st_ino)
                sock.setblocking(False)
                os.chmod(path, 0o600)
            # A bind reserves the path without admitting connections. Both paths
            # are owned before the worker can create or migrate the database.
            self.worker = DatabaseWorker(lambda: InboxStore(self.root))
            for sock, _path in sockets:
                sock.listen(16)
            servers.append(await asyncio.start_unix_server(lambda r,w: self.handle(r,w,True), sock=sockets[0][0], limit=LIMIT))
            servers.append(await asyncio.start_unix_server(self.handle, sock=sockets[1][0], limit=LIMIT))
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(sig, self.stop.set)
            print(json.dumps(await self.command({'op':'status'})), flush=True)
            await self.stop.wait()
        finally:
            self.closing = True
            # Keep the endpoint reservation until accepted database work drains.
            # New handlers see closing and cannot submit work. Some Python
            # versions unlink Unix paths when Server.close() is called.
            try:
                await drain_handlers(self.tasks)
            finally:
                try:
                    if self.worker is not None:
                        await self.worker.close()
                finally:
                    for server in servers:
                        server.close()
                    try:
                        await drain_handlers(self.tasks)
                        for server in servers:
                            await server.wait_closed()
                    finally:
                        for sock, path in sockets:
                            sock.close()
                            # Python may already have removed its listener path.
                            # Never remove a successor's socket after that release.
                            try:
                                info = path.lstat()
                            except FileNotFoundError:
                                continue
                            if (info.st_dev, info.st_ino) == identities.get(path):
                                path.unlink()


async def client(root, request):
    control = platform_support.control_socket_path(root)
    try:
        result, _pid = await control_exchange(root, request)
    except UnsafeServiceEndpoint as exc:
        print(json.dumps(dict(ok=False, code='unsafe_service_endpoint', error=str(exc))))
        return 1
    except (ConnectionRefusedError, FileNotFoundError) as exc:
        # Keep the existing CLI diagnostic for a missing or stale endpoint.
        raise SystemExit(f'no bridge is running for {root} (nothing is listening on {control})') from exc
    except TimeoutError:
        print(json.dumps(dict(ok=False, code='service_unresponsive',
                              error='control request timed out; mutation outcome is unknown')))
        return 1
    except NoControlReply:
        print(json.dumps(dict(ok=False, code='no_reply',
                              error='service closed without a reply; mutation outcome is unknown')))
        return 1
    except OSError:
        print(json.dumps(dict(ok=False, code='service_unavailable',
                              error='control exchange failed; mutation outcome is unknown')))
        return 1
    except ValueError:
        print(json.dumps(dict(ok=False, code='invalid_service_response',
                              error='invalid control reply; mutation outcome is unknown')))
        return 1
    if type(result.get('ok')) is not bool or (result['ok'] and 'result' not in result):
        print(json.dumps(dict(ok=False, code='invalid_service_response',
                              error='invalid control reply; mutation outcome is unknown')))
        return 1
    if request['op'] == 'inbox' and result.get('ok'):
        if not isinstance(result['result'], list) or any(not isinstance(entry, dict) for entry in result['result']):
            print(json.dumps(dict(ok=False, code='invalid_service_response',
                                  error='invalid inbox reply')))
            return 1
        # Also protect reads from servers started before a runtime upgrade.
        for entry in result['result']:
            entry['guidance'] = PEER_GUIDANCE
    print(json.dumps(result, indent=2))
    return 0 if result['ok'] else 1


def cli_main():
    os.umask(0o077)
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--state-dir', default=DEFAULT)
    sub = p.add_subparsers(dest='op', required=True)
    for op in ('serve','status','stop','peers'):
        sub.add_parser(op)
    s = sub.add_parser('send')
    s.add_argument('to')
    s.add_argument('message')
    s.add_argument('--priority', choices=['now','next','later'], default='next')
    s = sub.add_parser('inbox')
    s.add_argument('--after', type=int, default=0)
    s = sub.add_parser('ack')
    s.add_argument('through', type=int)
    for op in ('activate-notification-journal', 'rebuild-notification-journal-activation'):
        s = sub.add_parser(op)
        s.add_argument('--target-digest', required=True)
        s.add_argument('--nonce', required=True)
        if op.startswith('rebuild-'):
            s.add_argument('--expected-previous-nonce', required=True)
            s.add_argument('--accept-history-loss', action='store_true', required=True)
    a = vars(p.parse_args())
    root = Path(a.pop('state_dir')).absolute()
    startup_directory(root)
    if a['op'] == 'peers':
        print(json.dumps(peers(), indent=2))
    elif a['op'] == 'serve':
        asyncio.run(Bridge(root).run())
    else:
        raise SystemExit(asyncio.run(client(root, a)))


def main():
    try:
        cli_main()
    except inbox_schema.InboxSchemaError as exc:
        print(json.dumps(dict(ok=False, code='incompatible_inbox', error=str(exc))))
        raise SystemExit(platform_support.CONFIGURATION_EXIT_STATUS) from None
    except sqlite3.ProgrammingError:
        print(json.dumps(dict(ok=False, code='internal_error', error='inbox initialization failed')))
        raise SystemExit(platform_support.SOFTWARE_EXIT_STATUS) from None
    except sqlite3.DatabaseError:
        print(json.dumps(dict(ok=False, code='storage_error', error='inbox storage is unavailable')))
        raise SystemExit(platform_support.CONFIGURATION_EXIT_STATUS) from None
    except BridgeOwnershipError as exc:
        print(json.dumps(dict(ok=False, code='endpoint_unavailable', error=str(exc))))
        raise SystemExit(platform_support.CONFIGURATION_EXIT_STATUS) from None


if __name__ == '__main__':
    main()
