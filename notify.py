#!/usr/bin/env python3
"""Register the live bridge and queue inbox notifications to a selected participant."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import signal
import shlex
import sqlite3
import subprocess
import sys
import time
import uuid

import dsh_delivery
import platform_support
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


class DeliveryFailed(RuntimeError):
    """A notice could not be handed to the participant's session."""


def dsh_credentials_default():
    """Default harness credential file that holds the browser-session secret."""
    home = os.environ.get('DSH_HOME')
    return (Path(home) / '.credentials.yaml') if home else None


def deliver(a, text):
    """Deliver one content-free notice to the selected participant's session.

    Codex is reached through its own CLI. DeepSeek is reached through the
    harness's local HTTP RPC, which is the direct analogue of `codex queue`:
    both hand a pointer notice to one specific existing session without
    carrying any peer content.
    """
    if a.agent == 'deepseek':
        try:
            dsh_delivery.deliver(a.dsh_url, a.thread, text, credentials=a.dsh_credentials, timeout=15)
        except (dsh_delivery.DeliveryError, OSError) as exc:
            raise DeliveryFailed(str(exc)) from exc
        return
    result = subprocess.run([a.codex, 'queue', '--thread', a.thread, '--message', text],
                            capture_output=True, text=True, timeout=15)
    if result.returncode:
        raise DeliveryFailed(result.stderr.strip() or 'codex queue failed')


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
    private_dir(root)
    lock = (root / 'notifier.lock').open('a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    status = json.loads(subprocess.check_output([sys.executable, str(Path(__file__).with_name('bridge.py')),
                                               '--state-dir', str(root), 'status'], timeout=10))['result']
    pid, address = status['pid'], status['address']
    started = proc_start(pid)
    cursor_file = root / 'notify-cursor.json'
    after = a.after
    if cursor_file.exists():
        saved = json.loads(cursor_file.read_text())
        if saved['thread'] != a.thread:
            raise ValueError('state belongs to another thread; use a separate state directory')
        after = saved['through']
    registry = Path(os.environ.get('CLAUDE_CONFIG_DIR', str(Path.home() / '.claude'))) / 'sessions'
    private_dir(registry)
    record = registry / f'{pid}.json'
    owner = uuid.uuid4().hex
    metadata = dict(pid=pid, name=a.name, cwd=a.repo, startedAt=int(time.time()*1000),
                    procStart=started, kind='daemon', entrypoint='codex-peer-bridge',
                    pidDomain=platform_support.pid_domain(),
                    messagingSocketPath=address.removeprefix('uds:'), peerProtocol=1, peerFeatures=['reply_across_default_dirs'],
                    status='waiting', statusUpdatedAt=int(time.time()*1000), bridgeOwner=owner)
    # Never overwrite another agent's registry record, including on restart.
    with record.open('x') as f:
        json.dump(metadata, f)
    stopped = False
    def stop(*_):
        nonlocal stopped
        stopped = True
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, stop)
    db = sqlite3.connect((root / 'inbox.sqlite3').as_uri() + '?mode=ro', uri=True)
    ready = root/'notify-ready.json'
    save(ready,dict(owner=owner,bridge_pid=pid,notifier_pid=os.getpid(),
                    proc_start=proc_start_value(os.getpid())))
    print(json.dumps(dict(registered=address, name=a.name, thread=a.thread)), flush=True)
    try:
        while not stopped:
            try:
                if proc_start(pid) != started:
                    break
            except (ProcessLookupError, FileNotFoundError):
                # The bridge is gone. `proc_start` reports a vanished process as
                # ProcessLookupError on both platforms, so a stopped bridge breaks
                # the loop cleanly here instead of raising out of the notifier.
                break
            through, messages = unread(db, after)
            if through > after:
                if messages:
                    try:
                        deliver(a, notification(messages, root))
                    except (DeliveryFailed, subprocess.TimeoutExpired) as exc:
                        # The notice is content-free by construction, so reporting the
                        # failure reason cannot leak peer text.
                        print(f'notification failed ({type(exc).__name__}: {exc}); '
                              'inbox retained; retrying in 30 seconds', flush=True)
                        time.sleep(30)
                        continue
                save(cursor_file, dict(thread=a.thread, through=through))
                after = through
                print(json.dumps(dict(notified_through=through, user_messages=len(messages))), flush=True)
            time.sleep(2)
    finally:
        db.close()
        try:
            if json.loads(ready.read_text()).get('owner') == owner:
                ready.unlink()
        except FileNotFoundError:
            pass
        try:
            if json.loads(record.read_text()).get('bridgeOwner') == owner:
                record.unlink()
        except FileNotFoundError:
            pass
        lock.close()


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--thread', required=True,
                   help='exact existing Codex thread ID, or DeepSeek session ID with --agent deepseek')
    p.add_argument('--agent', choices=['codex', 'deepseek'], default='codex',
                   help='participant that receives notices (default: codex)')
    p.add_argument('--codex', default='codex', help='Codex CLI executable')
    p.add_argument('--dsh-url', default=os.environ.get('DSH_WEB_URL'),
                   help='DeepSeek harness web URL, such as http://127.0.0.1:51992')
    p.add_argument('--dsh-credentials', type=Path, default=dsh_credentials_default(),
                   help='harness .credentials.yaml holding the browser-session secret')
    p.add_argument('--state-dir', default=DEFAULT)
    p.add_argument('--name', default='codex-peer')
    p.add_argument('--repo', default=os.getcwd())
    p.add_argument('--after', type=int, default=0)
    run(p.parse_args())
