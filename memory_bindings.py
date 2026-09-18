"""Explicit repository bindings and bridge-owned, content-free memory pointers."""
import asyncio
from contextlib import suppress
import hashlib
import json
from pathlib import Path
import time
import uuid

import inbox_schema as schema
import memory
from peer_transport import encode, LIMIT, CONTROL_CLOSE_TIMEOUT
from service_runtime import HANDSHAKE_TIMEOUT
import platform_support

MAX_BINDINGS = 16
GIT_TIMEOUT = 3
STATUS_TIMEOUT = 2
REQUEST_MARGIN = 2
REQUEST_TIMEOUT = (GIT_TIMEOUT + memory.VERIFY_TIMEOUT + platform_support.PROCESS_QUERY_TIMEOUT
                   + STATUS_TIMEOUT + 2 * CONTROL_CLOSE_TIMEOUT + REQUEST_MARGIN)
CLIENT_TIMEOUT = REQUEST_TIMEOUT + HANDSHAKE_TIMEOUT + CONTROL_CLOSE_TIMEOUT + 1
OPERATIONS = frozenset(('bind-memory', 'unbind-memory', 'refresh-memory',
                        'memory-bindings', 'ack-binding-health'))
FIELDS = ('binding', 'repo_path', 'repo_key', 'memory_state_dir',
          'binding_instance', 'observed_store', 'observed_head', 'anomalies')


RECOVERY = {
    'binding_capacity': 'operator_action',
    'binding_changed': 'operator_action',
    'binding_instance_changed': 'retry',
    'binding_not_found': 'operator_action',
    'binding_observation_changed': 'retry',
    'binding_row_too_large': 'operator_action',
    'binding_timeout': 'retry',
    'foreign_service': 'operator_action',
    'invalid_binding': 'operator_action',
    'invalid_binding_cursor': 'operator_action',
    'invalid_binding_path': 'operator_action',
    'invalid_binding_request': 'operator_action',
    'invalid_service_response': 'operator_action',
    'memory_identity_changed': 'retry',
    'memory_observation_invalid': 'operator_action',
    'memory_response_invalid': 'operator_action',
    'memory_unavailable': 'retry',
    'memory_unhealthy': 'retry',
    'memory_upgrade_required': 'operator_action',
    'memory_version_invalid': 'operator_action',
    'memory_version_unsupported': 'operator_action',
    'ownership_mismatch': 'retry',
    'repository_unresolved': 'operator_action',
    'service_busy': 'retry',
    'service_refused': 'operator_action',
    'service_unavailable': 'retry',
    'service_unresponsive': 'retry',
    'unhealthy_service': 'retry',
    'unknown_owner': 'operator_action',
    'unsafe_service_endpoint': 'operator_action',
}


class BindingError(ValueError):
    def __init__(self, code):
        self.code = code
        # An unclassified new code is a programming fault, never a silent policy.
        self.recovery = RECOVERY[code]
        super().__init__(code)


def path_value(value):
    if not isinstance(value, str) or not value or '\0' in value:
        raise BindingError('invalid_binding_path')
    try:
        path = Path(value).resolve(strict=True)
        valid = path.is_dir() and len(str(path).encode()) <= 4096
    except (OSError, RuntimeError, UnicodeError) as exc:
        raise BindingError('invalid_binding_path') from exc
    if not valid:
        raise BindingError('invalid_binding_path')
    return path


async def resolve(repo_path, state_path):
    repo, state = path_value(repo_path), path_value(state_path)
    process = await asyncio.create_subprocess_exec(
        'git', 'rev-parse', '--path-format=absolute', '--git-common-dir',
        cwd=str(repo), stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
    try:
        output, _ = await asyncio.wait_for(process.communicate(), GIT_TIMEOUT)
    finally:
        if process.returncode is None:
            with suppress(ProcessLookupError):
                process.kill()
            await process.wait()
    try:
        common = path_value(output.decode().strip())
    except (ValueError, OSError) as exc:
        raise BindingError('repository_unresolved') from exc
    if process.returncode != 0:
        raise BindingError('repository_unresolved')
    repo_key = hashlib.sha256(str(common).encode()).hexdigest()[:16]
    key = hashlib.sha256(json.dumps([repo_key, str(state)], separators=(',', ':')).encode()).hexdigest()
    return dict(binding=key, repo_path=str(common), repo_key=repo_key,
                memory_state_dir=str(state))


async def observe(binding):
    """Read outside any inbox transaction; record only this verified observation."""
    root, repo = Path(binding['memory_state_dir']), binding['repo_key']
    try:
        hello = await memory.verify_running(root, repo)
        if hello is None:
            raise BindingError('memory_unavailable')
        version = hello.get('schema')
        if type(version) is not int:
            raise BindingError('memory_version_invalid')
        if version < memory.SCHEMA:
            raise BindingError('memory_upgrade_required')
        if version != memory.SCHEMA:
            raise BindingError('memory_version_unsupported')
        reply, pid = await memory.control_exchange(root, dict(op='status'), timeout=STATUS_TIMEOUT)
        state = reply.get('result') if reply.get('ok') is True else None
        if not isinstance(state, dict):
            raise BindingError('memory_unavailable')
        if (pid != hello['pid'] or state.get('generation') != hello['generation']
                or state.get('repo') != repo or type(state.get('protocol')) is not int
                or state['protocol'] != memory.PROTOCOL):
            raise BindingError('memory_identity_changed')
        if not state.get('healthy') or state.get('blocked'):
            raise BindingError('memory_unhealthy')
        store_id, head = state.get('store_id'), state.get('head')
        if (not schema.hex_value(store_id, 32) or store_id != hello.get('store_id')
                or type(head) is not int or not 0 <= head <= schema.MAX_SEQUENCE):
            raise BindingError('memory_observation_invalid')
        return dict(store_id=store_id, head=head, pid=pid, generation=hello['generation'])
    except memory.MemoryError_ as exc:
        raise BindingError(exc.code) from exc
    except BindingError:
        raise
    except (OSError, TimeoutError) as exc:
        raise BindingError('memory_unavailable') from exc
    except ValueError as exc:
        raise BindingError('memory_response_invalid') from exc


def rows(db):
    return [dict(zip(FIELDS, row)) for row in db.execute(
        'SELECT '+','.join(FIELDS)+' FROM memory_binding ORDER BY binding LIMIT 17')]


def get(db, key):
    if not schema.hex_value(key, 64):
        raise BindingError('invalid_binding')
    row = db.execute('SELECT '+','.join(FIELDS)+' FROM memory_binding WHERE binding=?', (key,)).fetchone()
    if row is None:
        raise BindingError('binding_not_found')
    return dict(zip(FIELDS, row))


def bind(db, binding):
    with schema.transaction(db):
        existing = db.execute('SELECT binding FROM memory_binding WHERE binding=?', (binding['binding'],)).fetchone()
        if existing is None:
            if db.execute('SELECT count(*) FROM memory_binding').fetchone()[0] >= MAX_BINDINGS:
                raise BindingError('binding_capacity')
            db.execute('INSERT INTO memory_binding(binding,repo_path,repo_key,memory_state_dir,binding_instance) VALUES (?,?,?,?,?)',
                       tuple(binding[key] for key in FIELDS[:4])+(uuid.uuid4().hex,))
        return get(db, binding['binding'])


def unbind(db, key):
    if not schema.hex_value(key, 64):
        raise BindingError('invalid_binding')
    with schema.transaction(db):
        # Obsolete, never acknowledged: no watermark is advanced here.
        db.execute("DELETE FROM inbox WHERE kind='memory-pointer' AND binding=?", (key,))
        db.execute('DELETE FROM memory_binding WHERE binding=?', (key,))
    return dict(binding=key, removed=True)


def refresh(db, binding, observation):
    with schema.transaction(db):
        current = get(db, binding['binding'])
        if any(current[key] != binding[key] for key in FIELDS[:4]):
            raise BindingError('binding_changed')
        # A concurrent refresh already won. Re-read memory instead of applying an
        # older observation as a false regression or store replacement.
        if current['binding_instance'] != binding['binding_instance']:
            raise BindingError('binding_instance_changed')
        if any(current[key] != binding[key] for key in FIELDS[5:]):
            raise BindingError('binding_observation_changed')
        store_id, head = observation['store_id'], observation['head']
        if current['observed_store'] == store_id and current['observed_head'] == head:
            return dict(binding=current['binding'], changed=False, anomalies=current['anomalies'])
        anomalies = current['anomalies']
        if current['observed_store'] is not None:
            if current['observed_store'] != store_id:
                anomalies |= 1
            elif head < current['observed_head']:
                anomalies |= 2
        db.execute("DELETE FROM inbox WHERE kind='memory-pointer' AND binding=?", (current['binding'],))
        frame = json.dumps(dict(type='memory-pointer', binding=current['binding']))
        db.execute("INSERT INTO inbox(received,pid,frame,kind,binding,binding_instance) VALUES(?,?,?,'memory-pointer',?,?)",
                   (time.time(), observation['pid'], frame, current['binding'], current['binding_instance']))
        db.execute('UPDATE memory_binding SET observed_store=?,observed_head=?,anomalies=? WHERE binding=?',
                   (store_id, head, anomalies, current['binding']))
    return dict(binding=current['binding'], changed=True, anomalies=anomalies)


def acknowledge_health(db, key):
    with schema.transaction(db):
        binding = get(db, key)
        db.execute('UPDATE memory_binding SET anomalies=0 WHERE binding=?', (key,))
    return dict(binding=key, cleared=binding['anomalies'])


def page(bindings, after=''):
    if after and not schema.hex_value(after, 64):
        raise BindingError('invalid_binding_cursor')
    result = []
    pending = [item for item in bindings if item['binding'] > after]
    for item in pending:
        try:
            size = len(encode(dict(bindings=result+[item])))
        except ValueError:
            if not result:
                raise BindingError('binding_row_too_large') from None
            break
        if size > LIMIT-1024:
            if not result:
                raise BindingError('binding_row_too_large')
            break
        result.append(item)
    more = len(result) < len(pending)
    return dict(bindings=result, more=more, next_after=result[-1]['binding'] if more and result else None)
