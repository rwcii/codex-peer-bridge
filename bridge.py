#!/usr/bin/env python3
"""Local Claude peer protocol adapter. Python standard library only."""
import argparse
import asyncio
import hashlib
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

from peer_guidance import PEER_GUIDANCE
import platform_support

LIMIT = 262144
DEFAULT = str(Path(os.environ.get('XDG_STATE_HOME', str(Path.home() / '.local/state'))) / 'codex-peer-bridge')


def private_dir(path):
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    s = path.lstat()
    if not stat.S_ISDIR(s.st_mode) or s.st_uid != os.getuid() or s.st_mode & 0o077:
        raise ValueError('directory must be owned by this user and mode 0700')


def credentials(sock):
    """Kernel-verified peer pid for a connected socket, same-user only.

    The mechanism differs by platform (Linux `SO_PEERCRED`, macOS
    `getpeereid` plus `LOCAL_PEERPID`); see `platform_support.peer_pid`.
    """
    return platform_support.peer_pid(sock)


def target_path(address):
    """Validate a peer address and return it **unresolved**.

    The returned path is the literal string from the wire. `peer_token` hashes
    it to find the sender's key file, and Claude Code hashes the same literal
    path, so resolving it here would break authentication silently: a resolved
    `/tmp/...` becomes `/private/tmp/...` on macOS and no key would be found.
    Only the allowlist comparison uses resolved paths, so the platform's `/tmp`
    symlink does not reject every peer.
    """
    if not isinstance(address, str) or not address.startswith('uds:'):
        raise ValueError('expected uds:/absolute/path')
    literal = address[4:]
    p = Path(literal)
    # The literal is what `peer_token` hashes, and what Claude hashed to name its
    # key file. An address that does not round-trip - a `.` or `..` component, a
    # doubled separator - could never match a published key, so the auth prelude
    # would be skipped silently. Reject it instead of normalizing it.
    if '..' in p.parts or str(p) != literal:
        raise ValueError('peer address must be in canonical literal form')
    # A symlinked parent could redirect an allowlisted-looking path elsewhere,
    # so it is rejected before the resolved directory is checked.
    if p.suffix != '.sock' or p.parent.is_symlink() or p.parent.resolve() not in platform_support.allowed_socket_dirs():
        raise ValueError('unsupported or symlinked peer address')
    private_dir(p.parent)
    s = p.lstat()
    if not platform_support.socket_mode_ok(s):
        raise ValueError('peer socket must be private and owned by this user')
    return p


def encode(frame):
    data = json.dumps(frame, ensure_ascii=True).encode() + b'\n'
    if len(data) > LIMIT:
        raise ValueError('frame too large')
    return data


def peer_token(pid, path):
    folder = Path(os.environ.get('CLAUDE_CONFIG_DIR', str(Path.home() / '.claude'))) / 'sessions'
    key = folder / f'{pid}.{hashlib.sha256(str(path).encode()).hexdigest()}.key'
    try:
        fd = os.open(key, os.O_RDONLY | os.O_NOFOLLOW)
    except FileNotFoundError:
        return None
    with os.fdopen(fd) as f:
        s = os.fstat(f.fileno())
        if not stat.S_ISREG(s.st_mode) or s.st_uid != os.getuid() or s.st_mode & 0o077 or s.st_size > 4096:
            raise ValueError('unsafe peer key')
        token = json.load(f).get('peerToken')
    if not isinstance(token, str) or len(token) != 32 or any(c not in '0123456789abcdef' for c in token):
        raise ValueError('invalid peer key')
    return token


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


class Bridge:
    def __init__(self, root):
        self.root = root
        self.address = f'uds:/tmp/cc-socks/{os.getpid()}.sock'
        self.stop = asyncio.Event()
        self.db = sqlite3.connect(root / 'inbox.sqlite3')
        self.db.execute('CREATE TABLE IF NOT EXISTS inbox (seq INTEGER PRIMARY KEY AUTOINCREMENT, received REAL, pid INTEGER, frame TEXT)')
        self.active = 0

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
        self.db.execute('INSERT INTO inbox(received,pid,frame) VALUES(?,?,?)', (time.time(), pid, json.dumps(frame)))
        self.db.commit()

    async def handle(self, reader, writer, control=False):
        if self.active >= 16:
            writer.close()
            return
        self.active += 1
        try:
            pid = credentials(writer.get_extra_info('socket'))
            async with asyncio.timeout(6):
                if control:
                    request = json.loads(await reader.readline())
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
                        frame = json.loads(line)
                        # Same-UID kernel peer credentials are our authentication policy.
                        # No key is published, so an auth prelude is neither needed nor accepted.
                        self.store(pid, frame)
        except (ValueError, KeyError, TypeError, OSError, TimeoutError, AttributeError, sqlite3.Error) as exc:
            if control:
                writer.write(encode(dict(ok=False, error=type(exc).__name__)))
                try:
                    await writer.drain()
                except OSError:
                    pass
            else:
                print(f'rejected peer input: {type(exc).__name__}', flush=True)
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass
            self.active -= 1

    async def command(self, r):
        op = r['op']
        if op == 'status':
            return dict(pid=os.getpid(), address=self.address, inbox_count=self.db.execute('SELECT count(*) FROM inbox').fetchone()[0], delivery='inbox available; run notify.py to notify the selected participant session')
        if op == 'send':
            return await self.send(r['to'], r['message'], r.get('priority', 'next'))
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
            self.db.execute('DELETE FROM inbox WHERE seq<=?', (int(r['through']),))
            self.db.commit()
            return 'acknowledged locally'
        if op == 'stop':
            self.stop.set()
            return 'stopping'
        raise ValueError('unknown operation')

    async def run(self):
        private_dir(Path('/tmp/cc-socks'))
        peer = Path(self.address[4:])
        control = platform_support.control_socket_path(self.root)
        private_dir(control.parent)
        # Bind exclusively. Never remove a pre-existing process socket.
        sockets = []
        try:
            for path, is_control in [(peer, False), (control, True)]:
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
                    raise OSError(f'cannot bind {path}: {exc}') from exc
                except BaseException:
                    sock.close()
                    raise
                sockets.append((sock, path))
                sock.listen(16)
                sock.setblocking(False)
                os.chmod(path, 0o600)
            servers = [await asyncio.start_unix_server(lambda r,w: self.handle(r,w), sock=sockets[0][0], limit=LIMIT),
                       await asyncio.start_unix_server(lambda r,w: self.handle(r,w,True), sock=sockets[1][0], limit=LIMIT)]
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(sig, self.stop.set)
            print(json.dumps(await self.command({'op':'status'})), flush=True)
            await self.stop.wait()
            for server in servers:
                server.close()
                await server.wait_closed()
        finally:
            for sock, path in sockets:
                sock.close()
                path.unlink(missing_ok=True)
            self.db.close()


async def client(root, request):
    control = platform_support.control_socket_path(root)
    private_dir(control.parent)
    try:
        r, w = await asyncio.open_unix_connection(str(control), limit=LIMIT)
    except (ConnectionRefusedError, FileNotFoundError) as exc:
        # A leftover socket from a killed instance is the first thing a user meets,
        # so report it plainly rather than as a traceback.
        raise SystemExit(f'no bridge is running for {root} (nothing is listening on {control})') from exc
    try:
        credentials(w.get_extra_info('socket'))
        w.write(encode(request))
        await w.drain()
        result = json.loads(await asyncio.wait_for(r.readline(), 10))
        if request['op'] == 'inbox' and result.get('ok'):
            # Also protect reads from servers started before a runtime upgrade.
            for entry in result['result']:
                entry['guidance'] = PEER_GUIDANCE
        print(json.dumps(result, indent=2))
        return 0 if result['ok'] else 1
    finally:
        w.close()
        await w.wait_closed()


def main():
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
    a = vars(p.parse_args())
    root = Path(a.pop('state_dir')).absolute()
    private_dir(root)
    if a['op'] == 'peers':
        print(json.dumps(peers(), indent=2))
    elif a['op'] == 'serve':
        asyncio.run(Bridge(root).run())
    else:
        raise SystemExit(asyncio.run(client(root, a)))


if __name__ == '__main__':
    main()
