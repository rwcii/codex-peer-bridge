#!/usr/bin/env python3
"""Register the live bridge and queue inbox notifications to a selected participant."""
import argparse
import json
import os
from pathlib import Path
import signal
import shlex
import sys
import uuid

import dsh_delivery
import platform_support
from participant_lock import OwnershipError, notifier_ownership
from bridge import DEFAULT, private_dir
from peer_guidance import PEER_GUIDANCE


def proc_start(pid):
    """Local process-start marker in this platform's own registry form."""
    return platform_support.proc_start(pid)


def unread(db, after):
    rows = db.execute('SELECT seq,pid,frame FROM inbox WHERE seq>? ORDER BY seq', (after,)).fetchall()
    messages = [(seq, pid) for seq, pid, frame in rows if json.loads(frame).get('type') == 'user']
    return rows[-1][0] if rows else after, messages


def notification(messages, root=DEFAULT):
    return (f'Agent bridge inbox has {len(messages)} new peer message(s), through sequence '
            f'{messages[-1][0]}. Read with: {shlex.quote(sys.executable)} '
            f'{shlex.quote(str(Path(__file__).resolve().with_name("bridge.py")))} '
            f'--state-dir {shlex.quote(str(root))} inbox '
            f'--after {messages[0][0]-1}. {PEER_GUIDANCE} '
            'This is a bridge notification, not a peer reply.')


def dsh_credentials_default():
    """Default harness credential file that holds the browser-session secret."""
    home = os.environ.get('DSH_HOME')
    return (Path(home) / '.credentials.yaml') if home else None


def save(path, value):
    temp = path.with_name(path.name + '.tmp.' + uuid.uuid4().hex)
    try:
        with temp.open('x') as f:
            json.dump(value, f)
            f.flush()
            os.fsync(f.fileno())
        temp.replace(path)
    finally:
        temp.unlink(missing_ok=True)


def proc_start_value(pid):
    """Process-start marker for the notifier itself, used to prove it is the live owner."""
    return platform_support.proc_start(pid)


def run(a):
    os.umask(0o077)
    root = Path(a.state_dir).absolute()
    with notifier_ownership(root, a.agent, a.thread) as participant:
        return run_owned(a, root, participant)


def run_owned(a, root, participant):
    import asyncio
    from notification_runtime import Runtime
    async def serving():
        runtime = Runtime(a, root, participant)
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, runtime.stop.set)
        try:
            return await runtime.run()
        finally:
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.remove_signal_handler(sig)
    return asyncio.run(serving())


async def control(a):
    from peer_transport import control_exchange
    root = Path(a.state_dir).absolute()
    payload = dict(op=a.action)
    if a.action == 'retry':
        payload['sequences'] = a.sequences
    try:
        reply, pid = await control_exchange(root / 'notifier', payload)
    except (OSError, ValueError, TimeoutError):
        if a.action != 'status':
            raise
        import notification_health
        import asyncio
        def observe():
            ready = notification_health.verify_owner(root)
            return ready, notification_health.read(root, ready)
        ready, value = await asyncio.to_thread(observe)
        result = dict(lifecycle='running' if ready is not None else 'unknown',
                      control_status='unavailable', delivery_health=value)
        print(json.dumps(dict(ok=ready is not None, result=result), indent=2))
        return 0 if ready is not None else 1
    if a.action == 'status' and reply.get('ok'):
        if reply.get('result', {}).get('pid') != pid:
            raise ValueError('notifier identity mismatch')
    print(json.dumps(reply, indent=2))
    return 0 if reply.get('ok') else 1


async def rebuild_owned(a, root):
    from notification_migration import Migration, read_state
    from notification_runtime import create_worker, settled_cleanup, ControlRefusal
    from notification_source import InboxSource
    from notification_journal import JournalError
    from contextlib import closing
    import asyncio
    from peer_transport import control_exchange
    reply, pid = await control_exchange(root, dict(op='status'))
    status = reply.get('result') if reply.get('ok') else None
    if not isinstance(status, dict) or status.get('pid') != pid:
        raise ValueError('bridge identity unavailable')
    if status.get('database_status', 'ready') != 'ready':
        raise ControlRefusal('bridge_storage_unavailable')
    if 'notification_journal_activation' not in status.get('capabilities', []):
        raise ControlRefusal('bridge_upgrade_required')
    marker = read_state(root / 'notify-migration.json')
    resumed = marker is not None and marker.get('state') == 'rebuilding'
    class Maintenance:
        def __init__(self):
            self.owner = Migration.rebuild(root, a.agent, a.thread,
                accept_history_loss=a.accept_history_loss, capable=True,
                activation=status['journal_activation'], ack_through=status['ack_through'])
        def activation(self):
            return self.owner.activation_request
        def confirm(self, evidence):
            with closing(InboxSource(root / 'inbox.sqlite3', schema=status.get('inbox_schema'),
                                     capabilities=status.get('capabilities', []))) as source:
                if source.retained([])['activation'] != evidence:
                    raise JournalError('journal_activation_mismatch')
            self.owner.confirm_activation(evidence)
            return self.owner.store.status()
        def close(self):
            self.owner.close()
    worker = await create_worker(Maintenance)
    try:
        request = await worker.call('activation')
        response, activated_pid = await control_exchange(root, request)
        if activated_pid != pid or response.get('ok') is not True:
            raise ValueError('journal activation was not confirmed')
        result = await worker.call('confirm', response['result'])
        print(json.dumps(dict(ok=True, result=dict(rebuilt=True, resumed=resumed, **result)), indent=2))
        return 0
    finally:
        await settled_cleanup(asyncio.create_task(worker.close()))


def main():
    import asyncio
    from notification_journal import JournalError
    from notification_runtime import RuntimeRefusal, ControlRefusal
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--thread', help='exact existing Codex thread or DeepSeek session; required to start or rebuild')
    p.add_argument('--agent', choices=['codex', 'deepseek'], default='codex')
    p.add_argument('--codex', default='codex', help='Codex CLI executable')
    p.add_argument('--dsh-url', default=os.environ.get('DSH_WEB_URL'))
    p.add_argument('--dsh-credentials', type=Path, default=dsh_credentials_default())
    p.add_argument('--state-dir', default=DEFAULT)
    p.add_argument('--name', default='codex-peer')
    p.add_argument('--repo', default=os.getcwd())
    p.add_argument('--after', type=int, default=0)
    sub = p.add_subparsers(dest='action')
    sub.add_parser('status')
    sub.add_parser('stop')
    sub.add_parser('ack-health')
    retry = sub.add_parser('retry')
    retry.add_argument('sequences', nargs='+', type=int)
    rebuild = sub.add_parser('rebuild-journal')
    rebuild.add_argument('--accept-history-loss', action='store_true', required=True)
    a = p.parse_args()
    if a.action in (None, 'rebuild-journal') and not a.thread:
        p.error('--thread is required to start or rebuild the notifier')
    if a.after < 0 or a.after > (1 << 63) - 1:
        p.error('--after must be a nonnegative signed-64-bit sequence')
    if a.action is None and a.agent == 'deepseek':
        if not a.dsh_url:
            p.error('--dsh-url or DSH_WEB_URL is required for DeepSeek delivery')
        try:
            dsh_delivery.require_loopback(a.dsh_url)
        except dsh_delivery.DeliveryError:
            p.error('DeepSeek delivery requires a valid loopback harness URL')
    try:
        if a.action == 'rebuild-journal':
            os.umask(0o077)
            root = Path(a.state_dir).absolute()
            with notifier_ownership(root, a.agent, a.thread):
                return asyncio.run(rebuild_owned(a, root))
        if a.action is not None:
            return asyncio.run(control(a))
        return run(a) or 0
    except OwnershipError as exc:
        print(json.dumps(exc.result()), flush=True)
        return platform_support.CONFIGURATION_EXIT_STATUS
    except ControlRefusal as exc:
        print(json.dumps(dict(ok=False, code=exc.code, recovery=exc.recovery)), flush=True)
        return (platform_support.TEMPORARY_EXIT_STATUS if exc.recovery == 'retry'
                else platform_support.CONFIGURATION_EXIT_STATUS)
    except RuntimeRefusal as exc:
        print(json.dumps(dict(ok=False, code=exc.code, path=exc.path)), flush=True)
        return platform_support.CONFIGURATION_EXIT_STATUS
    except JournalError as exc:
        print(json.dumps(dict(ok=False, code=exc.code, recovery=exc.recovery)), flush=True)
        return platform_support.CONFIGURATION_EXIT_STATUS
    except (OSError, ValueError, TimeoutError):
        print(json.dumps(dict(ok=False, code='notifier_unavailable', lifecycle='unknown',
                              delivery_health=dict(state='unknown', reasons=['control_unavailable']))), flush=True)
        return 1
    except Exception:
        print(json.dumps(dict(ok=False, code='internal_error')), flush=True)
        return platform_support.SOFTWARE_EXIT_STATUS


if __name__ == '__main__':
    raise SystemExit(main())
