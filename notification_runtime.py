"""Notifier orchestration: private control, journal owner, provider, and health."""
import asyncio
import runtime_names
from contextlib import asynccontextmanager
import json
import os
from pathlib import Path
import socket
import sqlite3
import time
import uuid

from database_worker import DatabaseWorker, CapacityError, WorkerFailure
import durable_state
from inbox_schema import hex_value, InboxSchemaError
from notification_delivery import DeliveryLoop
import notification_health as health
from notification_journal import JournalError
from notification_legacy import LegacyState
from notification_migration import legacy_cursor, present, sqlite_files
from notification_memory import MemoryWatches
from notification_notices import render
from notification_provider import Provider
from notification_source import SourceError
from notification_state import NotificationState
import platform_support
from peer_transport import control_exchange, credentials, encode, LIMIT, private_dir
from service_runtime import Admission, HANDSHAKE_TIMEOUT, close_writer, drain_handlers
import subscriptions


class RuntimeRefusal(ValueError):
    def __init__(self, code, path):
        self.code, self.path = code, str(path)
        super().__init__(code)


CONTROL_POLICY = {
    'notifier_not_ready': 'retry',
    'bridge_storage_unavailable': 'retry',
    'bridge_upgrade_required': 'operator_action',
}


class ControlRefusal(ValueError):
    def __init__(self, code):
        self.code = code
        self.recovery = CONTROL_POLICY[code]
        super().__init__(code)


class BridgeUnavailable(ConnectionError):
    pass


def sqlite_busy(exc):
    # SQLite extended result codes retain the primary code in the low byte.
    # OperationalError also covers disk faults; its type alone is not retry proof.
    return (isinstance(exc, sqlite3.Error)
            and getattr(exc, 'sqlite_errorcode', 0) & 0xff in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED))


async def settled_cleanup(task):
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            continue
    return task.result()


async def create_worker(factory):
    creation = asyncio.create_task(asyncio.to_thread(DatabaseWorker, factory))
    try:
        return await asyncio.shield(creation)
    except asyncio.CancelledError:
        while not creation.done():
            try:
                await asyncio.shield(creation)
            except asyncio.CancelledError:
                continue
        try:
            worker = creation.result()
        except Exception:
            pass
        else:
            await settled_cleanup(asyncio.create_task(worker.close()))
        raise


class Runtime:
    def __init__(self, options, root, participant, *, provider=None, registry=None):
        self.options, self.root, self.participant = options, Path(root), participant
        self.provider = provider or Provider(options)
        self.registry = Path(registry) if registry is not None else Path(
            os.environ.get('CLAUDE_CONFIG_DIR', str(Path.home() / '.claude'))) / 'sessions'
        self.generation = uuid.uuid4().hex
        self.stop = asyncio.Event()
        self.admission = Admission()
        self.handlers = set()
        self.worker = self.health_worker = self.delivery = None
        self.memory = MemoryWatches(participant, self.bridge_call)
        self.bridge = self.owner = None
        self.last_status = None
        self.reason = 'starting'
        self.publication_failed = False
        self.internal_fault = False
        self.operator_blocked = False
        self.closing = False

    async def bridge_call(self, request):
        timeout = 23 if request['op'] == 'refresh-memory' else 5
        try:
            reply, pid = await control_exchange(self.root, request, timeout=timeout)
        except (OSError, TimeoutError) as exc:
            raise BridgeUnavailable('bridge control unavailable') from exc
        if self.bridge is not None and pid != self.bridge['pid']:
            raise ConnectionError('bridge process changed')
        return reply

    async def observe_bridge(self):
        try:
            reply, pid = await control_exchange(self.root, dict(op='status'), timeout=5)
        except (OSError, TimeoutError) as exc:
            raise BridgeUnavailable('bridge status unavailable') from exc
        result = reply.get('result') if reply.get('ok') is True else None
        if (not isinstance(result, dict) or type(result.get('pid')) is not int or result['pid'] != pid
                or not isinstance(result.get('address'), str) or not result['address'].startswith('uds:')):
            raise ValueError('invalid bridge status identity')
        if self.bridge is not None and (pid != self.bridge['pid']
                or result.get('generation') != self.bridge.get('generation')):
            self.stop.set()
            raise ConnectionError('bridge instance changed')
        return result

    async def initialize_journal(self):
        status = await self.observe_bridge()
        if status.get('database_status', 'ready') != 'ready':
            self.reason = 'storage_wait'
            return
        def factory():
            capabilities = status.get('capabilities', [])
            saved = legacy_cursor(self.root, self.options.thread, self.options.after)
            journal_required = ('notification_journal_activation' in capabilities
                or present(self.root / 'notify-migration.json')
                or any(present(path) for path in sqlite_files(self.root))
                or (saved is not None and saved.get('journal_required')))
            if journal_required:
                return NotificationState(self.root, self.options.agent, self.options.thread,
                    self.options.after, status.get('inbox_schema'), capabilities)
            return LegacyState(self.root, self.options.thread, self.options.after,
                               status.get('inbox_schema'), capabilities)
        self.worker = await create_worker(factory)
        try:
            request = await self.worker.call('activation')
            if request is not None:
                response = await self.bridge_call(request)
                if response.get('ok') is not True:
                    raise JournalError('journal_activation_mismatch')
                await self.worker.call('confirm_activation', response.get('result'))
            self.last_status = await self.worker.call('ready', int(time.time()))
            self.delivery = DeliveryLoop(self.worker, self.render, self.deliver)
            self.reason = None
        except BaseException:
            worker, self.worker = self.worker, None
            await settled_cleanup(asyncio.create_task(worker.close()))
            raise

    async def list_bindings(self):
        if 'memory_binding' not in self.bridge.get('capabilities', []):
            return []
        bindings, after = [], ''
        while True:
            reply = await self.bridge_call(dict(op='memory-bindings', after=after))
            if reply.get('ok') is not True:
                raise ConnectionError('memory binding listing unavailable')
            page = reply.get('result')
            if not isinstance(page, dict) or not isinstance(page.get('bindings'), list):
                raise ValueError('invalid memory binding page')
            bindings.extend(page['bindings'])
            if len(bindings) > 16:
                raise ValueError('memory binding capacity exceeded')
            if page.get('more') is False:
                return bindings
            cursor = page.get('next_after')
            if not hex_value(cursor, 64) or cursor <= after or not page['bindings']:
                raise ValueError('invalid memory binding cursor')
            after = cursor

    async def render(self, rows):
        # Refresh configured routes before constructing a pointer command. No
        # registry claim or incoming peer text selects a memory service.
        if rows[0]['kind'] == 'memory-pointer':
            bindings = {item['binding']: item for item in await self.list_bindings()}
        else:
            bindings = {}
        return render(rows, self.root, self.participant, bindings)

    async def deliver(self, notice):
        try:
            outcome = await self.provider.deliver(notice)
        except Exception as exc:
            # Adapter failures are not bridge transport errors. Expected provider
            # deadlines must become unknown outcomes inside the adapter boundary.
            raise RuntimeError('provider adapter contract failed') from exc
        if outcome not in ('delivered', 'failed', 'unknown'):
            raise RuntimeError('provider adapter returned an invalid outcome')
        return outcome

    def record_failure(self, exc):
        if self.internal_fault:
            self.reason = 'internal_error'
            return
        if isinstance(exc, JournalError):
            if exc.recovery == 'operator_action':
                self.operator_blocked = True
            if exc.recovery == 'internal_error':
                self.internal_fault = True
                self.reason = 'internal_error'
            elif exc.database_fault == 'storage_error':
                self.reason = 'storage_error'
            elif exc.recovery == 'retry':
                self.reason = 'storage_wait'
            elif exc.code in health.REASONS:
                self.reason = exc.code
            elif exc.code.startswith('journal_source_'):
                self.reason = 'source_fault'
            elif exc.recovery == 'operator_action':
                self.reason = 'journal_recovery_required'
            else:
                # A request-only or newly introduced disposition reaching the
                # delivery-fault path is a software defect, never inferred recovery.
                self.internal_fault = True
                self.reason = 'internal_error'
        elif isinstance(exc, (SourceError, InboxSchemaError)):
            self.operator_blocked = True
            self.reason = 'source_fault'
        elif isinstance(exc, WorkerFailure):
            self.internal_fault = exc.code == 'internal_error'
            cause = exc.__cause__
            if isinstance(cause, InboxSchemaError):
                self.operator_blocked = True
                self.reason = 'source_fault'
            else:
                self.reason = 'storage_wait' if sqlite_busy(cause) else exc.code
        elif isinstance(exc, sqlite3.ProgrammingError):
            self.internal_fault = True
            self.reason = 'internal_error'
        elif isinstance(exc, BridgeUnavailable):
            self.reason = 'bridge_unavailable'
        elif sqlite_busy(exc) or isinstance(exc, CapacityError):
            self.reason = 'storage_wait'
        elif isinstance(exc, (sqlite3.DatabaseError, OSError)):
            self.reason = 'storage_error'
        else:
            self.internal_fault = True
            self.reason = 'internal_error'

    async def scan(self):
        if self.internal_fault or self.closing:
            return
        try:
            try:
                await self.observe_bridge()
            except (OSError, TimeoutError):
                self.reason = 'bridge_unavailable'
                try:
                    current = await platform_support.async_proc_start(self.bridge['pid'])
                    if not platform_support.same_process(self.bridge_start, current):
                        self.stop.set()
                except ProcessLookupError:
                    self.stop.set()
                except OSError:
                    pass
                return
            if self.operator_blocked:
                return
            if self.worker is None:
                await self.initialize_journal()
            if self.worker is None:
                return
            self.last_status = await self.delivery.step()
            self.reason = self.last_status.get('admission')
        except (Exception,) as exc:
            self.record_failure(exc)

    @asynccontextmanager
    async def open_current(self):
        current = await self.observe_bridge()
        request = dict(op='subscribe', protocol=1, generation=current.get('generation'))
        async with subscriptions.open_hints(self.root, request, expected_pid=current['pid']) as connection:
            yield connection

    async def memory_loop(self):
        while not self.stop.is_set():
            try:
                await self.memory.update(await self.list_bindings())
                self.memory.reasons()
            except (OSError, TimeoutError):
                # Optional memory observation does not stop ordinary delivery.
                pass
            except Exception as exc:
                self.record_failure(exc)
            try:
                await asyncio.wait_for(self.stop.wait(), 2)
            except TimeoutError:
                pass

    def health_snapshot(self):
        reasons = {self.reason} if self.reason is not None else set()
        try:
            reasons.update(self.memory.reasons())
        except Exception:
            self.internal_fault = True
            reasons.add('internal_error')
        if self.publication_failed:
            reasons.add('health_publication_failed')
        return health.snapshot(self.owner, self.last_status, reasons)

    async def observed_health(self):
        current = None
        if self.worker is not None:
            try:
                async with asyncio.timeout(1):
                    current = await self.worker.call('status', priority=True)
            except (TimeoutError, CapacityError):
                pass
            except Exception as exc:
                self.record_failure(exc)
        if current is not None:
            self.last_status = current
        value = self.health_snapshot()
        if current is None:
            reasons = set(value['reasons']) | {'storage_wait'}
            value = dict(value, state='unknown', reasons=sorted(reasons), journal=None)
        return value

    async def publish_loop(self):
        while not self.stop.is_set():
            try:
                # Clear only after publication succeeds. A previous failure remains
                # visible in this successful snapshot until the next checked write.
                value = await self.observed_health()
                await self.health_worker.call('publish', value)
                self.publication_failed = False
            except Exception as exc:
                self.publication_failed = True
                if not isinstance(exc, (OSError, CapacityError)) and not (
                        isinstance(exc, WorkerFailure) and exc.code == 'storage_error'):
                    self.record_failure(exc)
                print('notifier health publication failed; snapshot may be stale', flush=True)
            try:
                await asyncio.wait_for(self.stop.wait(), health.PUBLISH_INTERVAL)
            except TimeoutError:
                pass

    async def command(self, request):
        if not isinstance(request, dict) or not isinstance(request.get('op'), str):
            raise ValueError('invalid notifier control request')
        op = request['op']
        fields = {'op', 'sequences'} if op == 'retry' else {'op'}
        if set(request) != fields:
            raise ValueError('invalid notifier control fields')
        if op == 'stop':
            self.stop.set()
            return dict(stopping=True, generation=self.generation)
        if op == 'status':
            value = await self.observed_health()
            return dict(generation=self.generation, pid=os.getpid(), lifecycle='stopping' if self.closing else 'running',
                        delivery_health={key: value[key] for key in ('state', 'reasons', 'journal')})
        if op not in ('retry', 'ack-health'):
            raise ValueError('unknown notifier operation')
        if self.worker is None:
            raise ControlRefusal('notifier_not_ready')
        if op == 'retry':
            return await self.worker.call('retry', request['sequences'])
        if op == 'ack-health':
            return await self.worker.call('acknowledge_health')
        raise ValueError('unknown notifier operation')

    async def handle(self, reader, writer):
        task = asyncio.current_task()
        self.handlers.add(task)
        slot = None
        try:
            try:
                if self.closing:
                    raise CapacityError('notifier stopping')
                slot = self.admission.enter('handshake')
                credentials(writer.get_extra_info('socket'))
                async with asyncio.timeout(HANDSHAKE_TIMEOUT):
                    line = await reader.readline()
                    if not line or len(line) > LIMIT:
                        raise ValueError('invalid notifier frame')
                    request = json.loads(line)
                self.admission.leave(slot)
                slot = None
                op = request.get('op') if isinstance(request, dict) else None
                slot = self.admission.enter('control' if op in ('status', 'stop') else 'ordinary')
                async with asyncio.timeout(5):
                    result = await self.command(request)
                reply = dict(ok=True, result=result)
            except ControlRefusal as exc:
                reply = dict(ok=False, code=exc.code, recovery=exc.recovery)
            except JournalError as exc:
                if exc.database_fault is not None or exc.recovery in ('operator_action', 'capacity'):
                    self.record_failure(exc)
                reply = dict(ok=False, code=exc.code, recovery=exc.recovery)
            except SourceError as exc:
                self.record_failure(exc)
                reply = dict(ok=False, code='source_fault', recovery='operator_action')
            except WorkerFailure as exc:
                self.record_failure(exc)
                reply = dict(ok=False, code=exc.code)
            except CapacityError:
                reply = dict(ok=False, code='capacity')
            except (ValueError, OSError, TimeoutError):
                reply = dict(ok=False, code='rejected')
            except Exception as exc:
                self.record_failure(exc)
                reply = dict(ok=False, code='internal_error')
            writer.write(encode(reply))
            await asyncio.wait_for(writer.drain(), 1)
        except (OSError, TimeoutError):
            pass
        finally:
            try:
                await close_writer(writer)
            finally:
                if slot is not None:
                    self.admission.leave(slot)
                self.handlers.discard(task)

    async def run(self):
        tasks, server, record, control, inode = [], None, None, None, None
        try:
            self.bridge = await self.observe_bridge()
            self.owner = dict(owner=self.generation, bridge_pid=self.bridge['pid'], notifier_pid=os.getpid(),
                              proc_start=await platform_support.async_proc_start(os.getpid()))
            control_root = self.root / 'notifier'
            try:
                private_dir(control_root)
                control = platform_support.control_socket_path(control_root)
                platform_support.refuse_legacy_control_conflict(control_root)
                private_dir(control.parent)
            except (OSError, ValueError, RuntimeError) as exc:
                raise RuntimeRefusal('notifier_endpoint_refused', control_root) from exc
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                try:
                    sock.bind(str(control))
                except OSError as exc:
                    raise RuntimeRefusal('notifier_endpoint_refused', control) from exc
                sock.setblocking(False)
                control.chmod(0o600)
                info = control.lstat()
                inode = (info.st_dev, info.st_ino)
                server = await asyncio.start_unix_server(self.handle, sock=sock, limit=LIMIT)
            except BaseException:
                sock.close()
                raise
            self.health_worker = await create_worker(lambda: health.HealthFile(self.root))
            private_dir(self.registry)
            record = self.registry / f'{self.bridge["pid"]}.json'
            started = await platform_support.async_proc_start(self.bridge['pid'])
            self.bridge_start = started
            metadata = dict(pid=self.bridge['pid'], name=self.options.name, cwd=self.options.repo,
                startedAt=int(time.time()*1000), procStart=started, kind='daemon',
                entrypoint=runtime_names.REGISTRY_ENTRYPOINT, pidDomain=platform_support.pid_domain(),
                messagingSocketPath=self.bridge['address'].removeprefix('uds:'), peerProtocol=1,
                peerFeatures=['reply_across_default_dirs'], status='waiting',
                statusUpdatedAt=int(time.time()*1000), bridgeOwner=self.generation)
            try:
                fd = os.open(record, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            except OSError as exc:
                raise RuntimeRefusal('notifier_registry_refused', record) from exc
            with os.fdopen(fd, 'w') as stream:
                json.dump(metadata, stream)
            durable_state.publish(self.root / 'notify-ready.json', dict(self.owner, participant_lock=self.participant))
            print(json.dumps(dict(registered=self.bridge['address'], name=self.options.name)), flush=True)
            tasks = [asyncio.create_task(subscriptions.watch_changes(self.open_current, self.scan, self.stop,
                        subscriptions_enabled='inbox_subscription' in self.bridge.get('capabilities', []))),
                     asyncio.create_task(self.memory_loop()), asyncio.create_task(self.publish_loop()),
                     asyncio.create_task(self.stop.wait())]
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
        finally:
            await settled_cleanup(asyncio.create_task(self.cleanup(tasks, server, record, control, inode)))

    async def cleanup(self, tasks, server, record, control, inode):
        self.closing = True
        self.stop.set()
        # Keep both ownership locks until every cleanup operation has settled.
        # A failure in one subsystem must not skip the other owned resources.
        failures = []

        async def settle(operation):
            try:
                await operation
            except BaseException as exc:
                failures.append(exc)

        if server is not None:
            try:
                server.close()
            except Exception as exc:
                failures.append(exc)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await settle(self.memory.close())
        await settle(drain_handlers(self.handlers))
        workers = [worker for worker in (self.worker, self.health_worker) if worker is not None]
        await asyncio.gather(*(settle(worker.close()) for worker in workers))
        if server is not None:
            await settle(server.wait_closed())
        for path, key in ((self.root / 'notify-ready.json', 'owner'), (record, 'bridgeOwner')):
            if path is not None:
                try:
                    if json.loads(path.read_text()).get(key) == self.generation:
                        path.unlink()
                except (FileNotFoundError, ValueError):
                    pass
                except Exception as exc:
                    failures.append(exc)
        if control is not None and inode is not None:
            try:
                info = control.lstat()
                if (info.st_dev, info.st_ino) == inode:
                    control.unlink()
            except FileNotFoundError:
                pass
            except Exception as exc:
                failures.append(exc)
        if failures:
            raise BaseExceptionGroup('notifier shutdown failed', failures)
