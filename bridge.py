#!/usr/bin/env python3
"""Local Claude peer protocol adapter. Python standard library only."""
import argparse
import runtime_names
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
from peer_guidance import PEER_GUIDANCE, MEMORY_POINTER_GUIDANCE
import platform_support
import inbox_schema
import delivery_ledger
import participant_presence
import subscriptions
import memory_bindings

from peer_transport import LIMIT, credentials, encode, peer_token, private_dir, target_path, control_exchange, UnsafeServiceEndpoint, NoControlReply
DEFAULT = str(Path(os.environ.get('XDG_STATE_HOME', str(Path.home() / '.local/state'))) / 'koinon')


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
            activity = participant_presence.registry_activity(record)
            found.append(dict(pid=pid,name=record.get('name'),address='uds:'+address,
                              repo=record.get('cwd'),status=activity['state'],
                              presence=dict(service=participant_presence.service('kernel_process_start', 'running'),
                                            model_activity=activity),
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
        self.on_change = None
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
        # No ID means no deduplication claim and no process probe is needed.
        sender = None
        msg_id = frame.get('msg_id')
        if isinstance(msg_id, str) and 1 <= len(msg_id.encode()) <= 128:
            try:
                start = platform_support.proc_start(pid)
                sender = delivery_ledger.digest([pid, platform_support.normalize_start(start)])
            except (OSError, ValueError, subprocess.SubprocessError):
                pass
        now = time.time()
        try:
            with inbox_schema.transaction(self.db):
                key, fingerprint, duplicate = delivery_ledger.incoming(self.db, frame, sender, now)
                if duplicate is not None:
                    return dict(seq=duplicate, duplicate=True)
                if self.db.execute("SELECT count(*) FROM inbox WHERE kind='peer'").fetchone()[0] >= 1000:
                    raise ValueError('inbox full; acknowledge older entries')
                cursor = self.db.execute('INSERT INTO inbox(received,pid,frame) VALUES(?,?,?)', (now, pid, json.dumps(frame)))
                seq = cursor.lastrowid
                delivery_ledger.stored(self.db, key, fingerprint, seq, frame, sender, now)
        except delivery_ledger.DeliveryError as exc:
            if exc.code == 'delivery_payload_conflict':
                with inbox_schema.transaction(self.db):
                    delivery_ledger.conflict(self.db, delivery_ledger.incoming_key(self.db, frame, sender), now)
            raise
        if self.on_change is not None:
            self.on_change()
        return dict(seq=seq, duplicate=False)

    def outgoing(self, address, message, priority, msg_id, deadline):
        now = time.time()
        try:
            with inbox_schema.transaction(self.db):
                return delivery_ledger.outbound(self.db, address, message, priority, msg_id, deadline, now)
        except delivery_ledger.DeliveryError as exc:
            if exc.code == 'delivery_payload_conflict':
                with inbox_schema.transaction(self.db):
                    key = 'out:'+delivery_ledger.digest([delivery_ledger.identity(self.db), address, msg_id])
                    delivery_ledger.conflict(self.db, key, now)
            raise

    def failed_before_connect(self, key):
        with inbox_schema.transaction(self.db):
            return delivery_ledger.failed_before_connect(self.db, key, time.time())

    def transported(self, key, pid):
        with inbox_schema.transaction(self.db):
            return delivery_ledger.transported(self.db, key, pid, time.time())

    def fetched(self, sequences):
        now = time.time()
        with inbox_schema.transaction(self.db):
            return [delivery_ledger.stage(self.db, seq, 'fetched',
                    dict(at=now, source='control_reply_drained'), now) for seq in sequences]

    def bindings(self):
        return memory_bindings.rows(self.db)

    def binding(self, key):
        return memory_bindings.get(self.db, key)

    def bind_memory(self, binding):
        return memory_bindings.bind(self.db, binding)

    def unbind_memory(self, key):
        result = memory_bindings.unbind(self.db, key)
        if self.on_change is not None:
            self.on_change()
        return result

    def acknowledge_binding_health(self, key):
        return memory_bindings.acknowledge_health(self.db, key)

    def refresh_memory(self, binding, observation):
        result = memory_bindings.refresh(self.db, binding, observation)
        if result['changed'] and self.on_change is not None:
            self.on_change()
        return result

    def command(self, r):
        op = r['op']
        if op == 'status':
            with inbox_schema.transaction(self.db, write=False):
                state = inbox_schema.metadata(self.db)
                inbox_schema.validate_bindings(self.db)
                count = self.db.execute('SELECT count(*) FROM inbox').fetchone()[0]
            return dict(inbox_count=count, inbox_schema=state['schema'],
                        ack_through=state['ack_through'], journal_activation=state['journal_activation'],
                        capabilities=list(inbox_schema.CAPABILITIES))
        if op == 'delivery':
            seq, key = r.get('seq'), r.get('key')
            if (seq is None) == (key is None):
                raise ValueError('select seq or key')
            if seq is not None and (type(seq) is not int or not 0 < seq <= inbox_schema.MAX_SEQUENCE):
                raise ValueError('invalid sequence')
            if key is not None and (not isinstance(key, str) or len(key) > 128):
                raise ValueError('invalid delivery key')
            record = delivery_ledger.get(self.db, key=key, seq=seq)
            return dict(available=False) if record is None else dict(available=True, **record)
        if op == 'handled':
            seq, outcome = r.get('seq'), r.get('outcome')
            if type(seq) is not int or not 0 < seq <= inbox_schema.MAX_SEQUENCE or outcome not in ('done', 'failed', 'refused'):
                raise ValueError('explicit sequence and handled outcome required')
            now = time.time()
            with inbox_schema.transaction(self.db):
                return delivery_ledger.stage(self.db, seq, 'handled',
                    dict(at=now, source='participant_report', outcome=outcome), now)
        if op == 'record-notification':
            activation = inbox_schema.metadata(self.db)['journal_activation']
            if activation != dict(target_digest=r.get('target_digest'), nonce=r.get('nonce')):
                raise delivery_ledger.DeliveryError('delivery_activation_mismatch')
            records = r.get('records')
            if not isinstance(records, list) or not 1 <= len(records) <= 10:
                raise ValueError('invalid notification evidence batch')
            now = time.time()
            with inbox_schema.transaction(self.db):
                results = []
                for item in records:
                    if (not isinstance(item, dict) or set(item) != {'seq', 'at', 'started', 'provider', 'outcome'}
                            or type(item['seq']) is not int or not 0 < item['seq'] <= inbox_schema.MAX_SEQUENCE
                            or item['provider'] not in ('codex', 'deepseek') or item['outcome'] not in ('delivered', 'unknown')
                            or type(item['at']) is not int or type(item['started']) is not int
                            or item['started'] < 0 or not 0 <= item['at'] <= now + 1):
                        raise ValueError('invalid notification evidence')
                    evidence = dict(item, source='provider_result')
                    evidence.pop('seq')
                    results.append(delivery_ledger.stage(self.db, item['seq'], 'notified', evidence, now))
            return results
        if op == 'inbox':
            rows = self.db.execute('SELECT seq,received,pid,frame,kind,binding FROM inbox WHERE seq>? ORDER BY seq LIMIT 10', (int(r.get('after', 0)),)).fetchall()
            entries = []
            for seq, received, pid, frame, kind, binding in rows:
                if kind == 'memory-pointer':
                    item = dict(seq=seq, received=received, kind=kind, binding=binding,
                                source_service_pid=pid, guidance=MEMORY_POINTER_GUIDANCE,
                                frame=dict(type='memory-pointer', binding=binding))
                else:
                    item = dict(seq=seq, received=received, peer_pid=pid,
                                guidance=PEER_GUIDANCE, frame=json.loads(frame))
                if len(encode(entries)) + len(encode(item)) > LIMIT - 1000:
                    break
                entries.append(item)
            return entries
        if op == 'ack':
            result = inbox_schema.acknowledge(self.db, r['through'])
            if self.on_change is not None:
                self.on_change()
            return result
        if op in ('activate-notification-journal', 'rebuild-notification-journal-activation'):
            return inbox_schema.activate(self.db, r)
        raise ValueError('unknown database operation')


class Bridge:
    def __init__(self, root):
        self.root = root
        self.address = f'uds:/tmp/cc-socks/{os.getpid()}.sock'
        self.stop = asyncio.Event()
        self.generation = uuid.uuid4().hex
        self.hints = subscriptions.HintHub(self.generation)
        self.binding_health = {}
        # Construction must not open or migrate a database before endpoint ownership.
        self.worker = None
        self.admission = Admission()
        self.tasks = set()
        self.closing = False

    async def send(self, address, message, priority='next', msg_id=None, deadline=None):
        if not isinstance(message, str) or not message.strip():
            raise ValueError('message must be nonempty text')
        if priority not in ('now', 'next', 'later'):
            raise ValueError('invalid priority')
        path = target_path(address)
        msg_id = msg_id or str(uuid.uuid4())
        deadline = deadline if deadline is not None else time.time() + 300
        frame = dict(msgV=1, msg_id=msg_id, type='user', priority=priority,
                     message=dict(role='user', content=message), **{'from': self.address})
        data = encode(frame)
        if len(data) > 65536:
            raise ValueError('message exceeds 64 KiB')
        attempt = await self.worker.call('outgoing', address, message, priority, msg_id, deadline)
        if not attempt.pop('new'):
            return attempt
        writer = None
        try:
            async with asyncio.timeout(min(4, max(0, deadline - time.time()))):
                try:
                    reader, writer = await asyncio.open_unix_connection(str(path), limit=LIMIT)
                except (FileNotFoundError, ConnectionRefusedError):
                    result = await self.worker.call('failed_before_connect', attempt['key'])
                    return dict(key=attempt['key'], **result)
                pid = credentials(writer.get_extra_info('socket'))
                token = peer_token(pid, path)
                if token:
                    writer.write(encode(dict(type='auth', token=token)))
                writer.write(data)
                await writer.drain()
                writer.write_eof()
                while await reader.read(4096):
                    pass
            result = await self.worker.call('transported', attempt['key'], pid)
            return dict(key=attempt['key'], **result)
        except (OSError, TimeoutError):
            # The reservation is durable. Never replay a possibly accepted native
            # frame. Unclassified failures retain this conservative outcome.
            return attempt
        finally:
            if writer is not None:
                await close_writer(writer)

    async def store(self, pid, frame):
        return await self.worker.call('store', pid, frame)

    async def handle(self, reader, writer, control=False):
        task = asyncio.current_task()
        self.tasks.add(task)
        slot = None
        request = None
        reply_started = False
        try:
            if self.closing:
                raise CapacityError('service is stopping')
            slot = self.admission.enter('handshake' if control else 'ordinary')
            pid = credentials(writer.get_extra_info('socket'))
            if control:
                async with asyncio.timeout(HANDSHAKE_TIMEOUT):
                    request = json.loads(await reader.readline())
            if control and isinstance(request, dict) and request.get('op') == 'subscribe-inbox':
                self.admission.leave(slot)
                slot = None
                subscriptions.validate(request, self.generation)
                await self.hints.serve(reader, writer)
                return
            binding_operation = (control and isinstance(request, dict)
                                 and isinstance(request.get('op'), str)
                                 and request['op'] in memory_bindings.OPERATIONS)
            deadline = memory_bindings.REQUEST_TIMEOUT if binding_operation else 6
            async with asyncio.timeout(deadline):
                if control:
                    if not isinstance(request, dict):
                        raise ValueError('expected operation object')
                    self.admission.leave(slot)
                    slot = None
                    slot = self.admission.enter('control' if request.get('op') in ('status', 'stop') else 'ordinary')
                    result = await self.command(request)
                    response = encode(dict(ok=True, result=result))
                    reply_started = True
                    writer.write(response)
                    await writer.drain()
                    if request.get('op') == 'inbox':
                        # Evidence is written only after the successful reply drain.
                        # A failed write leaves receipt evidence unknown; it cannot
                        # retract the already returned entries or emit a second reply.
                        try:
                            await self.worker.call('fetched', [item['seq'] for item in result])
                        except Exception:
                            print('fetched evidence could not be recorded', flush=True)
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
            if (isinstance(exc, TimeoutError) and control and isinstance(request, dict)
                    and isinstance(request.get('op'), str)
                    and request['op'] in memory_bindings.OPERATIONS):
                exc = memory_bindings.BindingError('binding_timeout')
                key = request.get('binding')
                if isinstance(key, str) and key in self.binding_health:
                    self.record_binding_health(key, 'refused', exc.code)
            if isinstance(exc, (memory_bindings.BindingError, delivery_ledger.DeliveryError)):
                code = exc.code
            elif isinstance(exc, CapacityError):
                code = 'capacity'
            elif isinstance(exc, WorkerFailure):
                code = exc.code
            elif isinstance(exc, (ValueError, OSError, TimeoutError)):
                code = 'rejected'
            else:
                code = 'internal_error'
            if control and not reply_started:
                try:
                    error = dict(ok=False, code=code, error=type(exc).__name__)
                    if (isinstance(request, dict) and request.get('op') == 'send'
                            and isinstance(exc, (TimeoutError, OSError, WorkerFailure))):
                        error.update(code='delivery_indeterminate', outcome='unknown')
                    if isinstance(exc, memory_bindings.BindingError):
                        error['recovery'] = exc.recovery
                        if exc.code == 'binding_timeout':
                            error['outcome'] = 'unknown'
                    writer.write(encode(error))
                    await asyncio.wait_for(writer.drain(), 1)
                except (OSError, TimeoutError):
                    pass
            else:
                print(f'request completion failed: {code}', flush=True)
        finally:
            try:
                await close_writer(writer)
            finally:
                if slot is not None:
                    self.admission.leave(slot)
                self.tasks.discard(task)

    def record_binding_health(self, key, state, reason=None):
        self.binding_health[key] = (time.monotonic(), state, reason)
        if len(self.binding_health) > memory_bindings.MAX_BINDINGS:
            oldest = min(self.binding_health, key=lambda item: self.binding_health[item][0])
            del self.binding_health[oldest]

    async def command(self, r):
        if not isinstance(r, dict) or not isinstance(r.get('op'), str):
            raise ValueError('expected operation object')
        op = r['op']
        if op == 'status':
            state, diagnostics = await database_status(self.worker, r)
            if state is None:
                state = dict(inbox_count=None, inbox_schema=None, ack_through=None,
                             journal_activation=None, capabilities=[])
            if state['inbox_schema'] == inbox_schema.SCHEMA:
                state['capabilities'].extend(('inbox_subscription', 'memory_binding'))
            return dict(pid=os.getpid(), address=self.address, generation=self.generation,
                        **state, **diagnostics,
                        presence=dict(service=participant_presence.service('live_bridge_control'),
                                      model_activity=participant_presence.unknown()),
                        delivery='inbox available; run notify.py to notify the selected participant session')
        if op == 'send':
            return await self.send(r.get('to'), r.get('message'), r.get('priority', 'next'), r.get('msg_id'), r.get('deadline'))
        if op in ('delivery', 'handled', 'record-notification'):
            return await self.worker.call('command', r)
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
        if op == 'bind-memory':
            if set(r) != {'op', 'repo_path', 'memory_state_dir'}:
                raise memory_bindings.BindingError('invalid_binding_request')
            binding = await memory_bindings.resolve(r['repo_path'], r['memory_state_dir'])
            await memory_bindings.observe(binding)
            result = await self.worker.call('bind_memory', binding)
            self.record_binding_health(binding['binding'], 'verified')
            return result
        if op in ('unbind-memory', 'refresh-memory', 'ack-binding-health'):
            if set(r) != {'op', 'binding'}:
                raise memory_bindings.BindingError('invalid_binding_request')
            if op == 'ack-binding-health':
                return await self.worker.call('acknowledge_binding_health', r['binding'])
            if op == 'unbind-memory':
                result = await self.worker.call('unbind_memory', r['binding'])
                self.binding_health.pop(r['binding'], None)
                return result
            binding = await self.worker.call('binding', r['binding'])
            try:
                observation = await memory_bindings.observe(binding)
                result = await self.worker.call('refresh_memory', binding, observation)
            except memory_bindings.BindingError as exc:
                self.record_binding_health(r['binding'], 'refused', exc.code)
                raise
            self.record_binding_health(r['binding'], 'verified')
            return result
        if op == 'memory-bindings':
            if set(r) - {'op', 'after'}:
                raise memory_bindings.BindingError('invalid_binding_request')
            bindings = await self.worker.call('bindings')
            known = {item['binding'] for item in bindings}
            self.binding_health = {key: value for key, value in self.binding_health.items() if key in known}
            for item in bindings:
                checked, state, reason = self.binding_health.get(item['binding'], (0, 'unknown', None))
                if time.monotonic()-checked > 30:
                    state, reason = 'unknown', None
                item['service_state'], item['service_reason'] = state, reason
            return memory_bindings.page(bindings, r.get('after', ''))
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
            try:
                control = platform_support.control_socket_path(self.root)
                platform_support.refuse_legacy_control_conflict(self.root)
            except (OSError, RuntimeError) as exc:
                raise BridgeOwnershipError(f'control endpoint unavailable: {exc}') from exc
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
            def store_factory():
                store = InboxStore(self.root)
                store.on_change = self.hints.notify_committed
                return store
            self.worker = DatabaseWorker(store_factory)
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
            self.hints.close()
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
    def emit(value):
        if request.get('op') == 'send':
            value['delivery_attempt'] = {key: request.get(key) for key in ('msg_id', 'deadline')}
        print(json.dumps(value, indent=2))

    try:
        if isinstance(request.get('op'), str) and request['op'] in memory_bindings.OPERATIONS:
            result, _pid = await control_exchange(root, request, timeout=memory_bindings.CLIENT_TIMEOUT)
        else:
            result, _pid = await control_exchange(root, request)
    except UnsafeServiceEndpoint as exc:
        emit(dict(ok=False, code='unsafe_service_endpoint', error=str(exc)))
        return 1
    except (ConnectionRefusedError, FileNotFoundError) as exc:
        # Keep the existing CLI diagnostic for a missing or stale endpoint.
        raise SystemExit(f'no bridge is running for {root} (no control endpoint is listening)') from exc
    except TimeoutError:
        emit(dict(ok=False, code='service_unresponsive',
                              error='control request timed out; mutation outcome is unknown'))
        return 1
    except NoControlReply:
        emit(dict(ok=False, code='no_reply',
                              error='service closed without a reply; mutation outcome is unknown'))
        return 1
    except OSError:
        emit(dict(ok=False, code='service_unavailable',
                              error='control exchange failed; mutation outcome is unknown'))
        return 1
    except ValueError:
        emit(dict(ok=False, code='invalid_service_response',
                              error='invalid control reply; mutation outcome is unknown'))
        return 1
    if type(result.get('ok')) is not bool or (result['ok'] and 'result' not in result):
        emit(dict(ok=False, code='invalid_service_response',
                              error='invalid control reply; mutation outcome is unknown'))
        return 1
    if request['op'] == 'inbox' and result.get('ok'):
        if not isinstance(result['result'], list) or any(not isinstance(entry, dict) for entry in result['result']):
            emit(dict(ok=False, code='invalid_service_response',
                                  error='invalid inbox reply'))
            return 1
        # Also protect reads from servers started before a runtime upgrade.
        for entry in result['result']:
            entry['guidance'] = (MEMORY_POINTER_GUIDANCE if entry.get('kind') == 'memory-pointer'
                                 else PEER_GUIDANCE)
    emit(result)
    if request['op'] == 'send' and result.get('ok') and result.get('result', {}).get('status') in ('indeterminate', 'failed_before_connect'):
        return 1
    return 0 if result['ok'] else 1


def cli_main():
    os.umask(0o077)
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--state-dir')
    sub = p.add_subparsers(dest='op', required=True)
    for op in ('serve','status','stop','peers'):
        sub.add_parser(op)
    s = sub.add_parser('send')
    s.add_argument('to')
    s.add_argument('message')
    s.add_argument('--priority', choices=['now','next','later'], default='next')
    s.add_argument('--msg-id')
    s.add_argument('--deadline', type=float)
    s = sub.add_parser('delivery')
    selection = s.add_mutually_exclusive_group(required=True)
    selection.add_argument('--seq', type=int)
    selection.add_argument('--key')
    s = sub.add_parser('handled')
    s.add_argument('seq', type=int)
    s.add_argument('--outcome', required=True, choices=['done', 'failed', 'refused'])
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
    s = sub.add_parser('bind-memory')
    s.add_argument('--repo-path', required=True)
    s.add_argument('--memory-state-dir', required=True)
    for op in ('unbind-memory', 'refresh-memory', 'ack-binding-health'):
        s = sub.add_parser(op)
        s.add_argument('binding')
    s = sub.add_parser('memory-bindings')
    s.add_argument('--after', default='')
    a = vars(p.parse_args())
    if a['op'] == 'send':
        if (a['msg_id'] is None) != (a['deadline'] is None):
            p.error('--msg-id and --deadline must be supplied together')
        a['msg_id'] = a['msg_id'] or str(uuid.uuid4())
        a['deadline'] = a['deadline'] if a['deadline'] is not None else time.time() + 300
    root = Path(a.pop('state_dir') or runtime_names.default_state_root()).absolute()
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
    except runtime_names.NameConflict as exc:
        print(json.dumps(dict(ok=False, code=exc.code, paths=exc.paths)))
        raise SystemExit(platform_support.CONFIGURATION_EXIT_STATUS) from None
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
