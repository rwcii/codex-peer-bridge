#!/usr/bin/env python3
"""Idempotent per-session registration for bridge participants.

One instance serves one participant session, identified by its Codex thread ID
or its DeepSeek session ID. Peer names follow the fleet form
`<agent>[-<model>]-<repo>-<two hex>`, so a Claude peer can tell which agent and,
for DeepSeek, which model it is addressing.
"""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import signal
import subprocess
import sys
import time

from bridge import private_dir, peers
import dsh_delivery
from notify import save
import platform_support
from scripts.install import units, check_owned_unit, start_command_for


def identity(thread):
    if not thread or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}',thread):
        raise ValueError('a valid explicit thread or CODEX_THREAD_ID is required')
    return hashlib.sha256(thread.encode()).hexdigest()[:16]


def peer_base(agent, model, label):
    """Human-facing peer-name stem, before the two-hex fleet suffix.

    A known model contributes the stem on its own when it already names the
    agent, so DeepSeek's `deepseek-v4-pro` yields `deepseek-v4-pro-<repo>` rather
    than a doubled `deepseek-deepseek-v4-pro-<repo>`. When no model is known the
    agent name alone is used, so a participant whose model cannot be determined
    still registers instead of failing.
    """
    stem = re.sub(r'[^a-z0-9]+','-',str(agent or 'codex').lower()).strip('-') or 'codex'
    if model:
        slug = re.sub(r'[^a-z0-9]+','-',str(model).lower()).strip('-')[:32]
        if slug:
            stem = slug if slug.startswith(stem) else f'{stem}-{slug}'
    return f'{stem}-{label}'


def bridge_status(prefix, state):
    try:
        result = subprocess.run([sys.executable,str(prefix/'bridge.py'),'--state-dir',str(state),'status'],
                                capture_output=True,text=True,timeout=10)
        if result.returncode == 0:
            return json.loads(result.stdout)['result']
    except (OSError,ValueError,subprocess.TimeoutExpired):
        pass
    return None


def notifier_ready(state, bridge):
    if not bridge:
        return False
    try:
        ready=json.loads((state/'notify-ready.json').read_text())
        if ready['bridge_pid'] != bridge['pid']:
            return False
        pid=ready['notifier_pid']
        if not platform_support.same_process(ready['proc_start'], platform_support.proc_start(pid)):
            return False
    except (OSError,ValueError,KeyError,IndexError,subprocess.SubprocessError):
        return False
    try:
        with (state/'notifier.lock').open('a') as lock:
            try:
                fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
            except BlockingIOError:
                return True
    except OSError:
        pass
    return False


def read_config(prefix):
    return json.loads((prefix/'install.json').read_text())


def details(prefix, config, thread, repo, agent='codex', model=None):
    key = identity(thread)
    state = Path(config['state_root'])/'sessions'/key
    label = re.sub(r'[^a-z0-9-]+','-',Path(repo).name.lower()).strip('-')[:32] or 'session'
    return state, f'{peer_base(agent, model, label)}-{key[:2]}', key


def save_registration(state, state_root, thread, repo, rename=False, agent='codex', model=None):
    # Serialize name assignment across threads in this installation. Internal identity
    # remains the full digest; only the human-facing label uses the fleet suffix.
    private_dir(state_root)
    with (state_root/'names.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        occupied={p['name'] for p in peers() if isinstance(p.get('name'),str)}
        for path in (state_root/'sessions').glob('*/session.json'):
            if path == state/'session.json':
                continue
            other=json.loads(path.read_text())
            occupied.add(other['name'])
        _, candidate, key=details(Path('.'),{'state_root':str(state_root)},thread,repo,agent,model)
        base=candidate.rsplit('-',1)[0]
        for offset in range(256):
            name=f'{base}-{(int(key[:2],16)+offset)%256:02x}'
            if name not in occupied:
                data=dict(thread=thread,name=name,repo=repo,agent=agent,model=model)
                save(state/'session.json',data)
                return data
        raise ValueError('all two-hex names for this repository are allocated; choose another descriptive repository name')


def start_command(prefix, thread, repo, agent='codex', model=None):
    # Shared with the service renderer so a manual start and a managed one cannot
    # drift apart.
    return start_command_for(sys.executable, prefix, thread, repo, agent, model)


def result(prefix, state, name, thread, repo, status, agent='codex', model=None):
    return dict(status=status, name=name, state_dir=str(state),
                start_command=shlex.join(start_command(prefix,thread,repo,agent,model)),
                inbox_command=shlex.join([sys.executable,str(prefix/'bridge.py'),'--state-dir',str(state),'inbox']))


def notify_command(prefix, config, state, thread, repo, name, agent='codex', model=None):
    """argv for the notifier that serves one participant session.

    A DeepSeek participant is pointed at the harness explicitly only when the
    installation recorded it; otherwise the notifier falls back to the
    `DSH_HOME` and `DSH_WEB_URL` of the environment it is started from, which is
    how a session started by the harness itself resolves them.
    """
    command = [sys.executable,str(prefix/'notify.py'),'--state-dir',str(state),
               '--thread',thread,'--name',name,'--repo',repo]
    # Codex is the notifier's own default, so it is named only when it differs.
    if agent != 'codex':
        command += ['--agent',agent]
    if agent == 'deepseek':
        for flag, key in (('--dsh-url','dsh_url'),('--dsh-credentials','dsh_credentials')):
            if config.get(key):
                command += [flag,str(config[key])]
    else:
        command += ['--codex',config['codex']]
    return command


def supervisor(prefix, config, state, thread, repo, name, agent='codex', model=None):
    # A persistent managed session owns both children; repeated calls cannot duplicate it.
    with (state/'supervisor.lock').open('a') as lock:
        try:
            fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError('this thread already has a managed supervisor')
        if bridge_status(prefix,state):
            raise ValueError('bridge is already running; use ensure/status')
        children=[]
        stopped=False
        def stop(*_):
            nonlocal stopped
            stopped=True
        for sig in (signal.SIGTERM,signal.SIGINT):
            signal.signal(sig,stop)
        try:
            children.append(subprocess.Popen([sys.executable,str(prefix/'bridge.py'),'--state-dir',str(state),'serve']))
            for _ in range(100):
                if stopped or children[0].poll() is not None:
                    raise RuntimeError('bridge exited during startup')
                if bridge_status(prefix,state):
                    break
                time.sleep(.1)
            else:
                raise RuntimeError('bridge did not become ready')
            children.append(subprocess.Popen(notify_command(prefix,config,state,thread,repo,name,agent,model)))
            for _ in range(100):
                if stopped or children[-1].poll() is not None:
                    raise RuntimeError('notifier exited during startup')
                if notifier_ready(state,bridge_status(prefix,state)):
                    break
                time.sleep(.1)
            else:
                raise RuntimeError('notifier did not become ready')
            print(json.dumps(result(prefix,state,name,thread,repo,'running',agent,model)),flush=True)
            while not stopped and all(p.poll() is None for p in children):
                time.sleep(.2)
            if not stopped:
                raise RuntimeError('session child exited; restart the complete session')
        finally:
            for child in reversed(children):
                if child.poll() is None:
                    child.terminate()
                    try:
                        child.wait(timeout=20)
                    except subprocess.TimeoutExpired:
                        child.kill()
                        child.wait()


def main():
    os.umask(0o077)
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('action',choices=['ensure','run','status','stop','rename'])
    p.add_argument('--agent',choices=['codex','deepseek'],default=None,
                   help='participant kind this instance serves; defaults to the registered kind, '
                        'or is inferred from the session environment, and is codex otherwise')
    p.add_argument('--thread',default=None,
                   help='exact participant session identity; defaults to CODEX_THREAD_ID or DSH_SESSION_ID')
    p.add_argument('--model',default=None,
                   help='model id advertised in the peer name; for deepseek it defaults to the '
                        'harness agent-default-model when one can be read')
    p.add_argument('--repo',default=os.getcwd())
    a=p.parse_args()
    prefix=Path(__file__).resolve().parent
    config=read_config(prefix)
    repo=str(Path(a.repo).resolve())
    # Keep what was actually requested separate from what gets inferred, because
    # only an explicit request may override a registered identity.
    explicit_agent=a.agent
    if a.agent is None and not os.environ.get('CODEX_THREAD_ID') and os.environ.get('DSH_SESSION_ID'):
        a.agent='deepseek'
    # The participant's own environment names its session, exactly as
    # CODEX_THREAD_ID does for Codex. Neither is guessed.
    if not a.thread:
        a.thread = os.environ.get('DSH_SESSION_ID' if a.agent=='deepseek' else 'CODEX_THREAD_ID')
    agent=a.agent or 'codex'
    model=a.model or (dsh_delivery.default_model() if agent=='deepseek' else None)
    state,name,key=details(prefix,config,a.thread,repo,agent,model)
    private_dir(state)
    with (state/'registration.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        registration=state/'session.json'
        if registration.exists():
            saved=json.loads(registration.read_text())
            if saved['thread'] != a.thread:
                raise ValueError('thread identity collision')
            name=saved['name']
            # A registered instance keeps the identity it was created with, so
            # repeated `ensure` calls cannot silently rename a live peer.
            agent=saved.get('agent',agent)
            model=saved.get('model',model)
            if a.action != 'rename':
                repo=saved['repo']
        else:
            saved=save_registration(state,Path(config['state_root']),a.thread,repo,agent=agent,model=model)
            name=saved['name']
        active=bridge_status(prefix,state)
        if a.action=='rename':
            if active:
                raise ValueError('stop this thread before explicitly renaming it')
            # Rename is the one action where an explicit request wins over the
            # saved identity, so an advertised model can actually be corrected. An
            # omitted flag must fall back to the saved record, otherwise a bare
            # `rename --repo` would silently turn a DeepSeek peer into a Codex one.
            saved=save_registration(state,Path(config['state_root']),a.thread,repo,rename=True,
                                    agent=explicit_agent or agent,
                                    model=a.model or model)
            print(json.dumps(saved))
            return
        healthy=notifier_ready(state,active)
        if a.action=='status' or (a.action=='ensure' and active):
            data=result(prefix,state,name,a.thread,repo,'running' if healthy else ('repair_required' if active else 'stopped'),agent,model)
            data['bridge']=active
            if active and not healthy:
                data['repair_command']=shlex.join([sys.executable,str(prefix/'session.py'),'stop','--thread',a.thread])
            print(json.dumps(data))
            return
        if a.action=='stop':
            unit=Path(config['unit_dir'])/f'codex-peer-session-{key}.service'
            if unit.exists():
                check_owned_unit(unit)
                try:
                    available=subprocess.run(['systemctl','--user','show-environment'],stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,timeout=5).returncode==0
                except (OSError,subprocess.TimeoutExpired):
                    available=False
                if available:
                    subprocess.run(['systemctl','--user','stop',unit.name],check=True)
                active=bridge_status(prefix,state)
            if active:
                subprocess.run([sys.executable,str(prefix/'bridge.py'),'--state-dir',str(state),'stop'],check=True)
                for _ in range(100):
                    if not bridge_status(prefix,state) and not notifier_ready(state,active):
                        break
                    time.sleep(.1)
                else:
                    raise RuntimeError('session did not stop completely')
            # systemd's Restart=on-failure does not restart a clean stop.
            return
        if a.action=='ensure':
            try:
                available=subprocess.run(['systemctl','--user','show-environment'],stdout=subprocess.DEVNULL,
                                         stderr=subprocess.DEVNULL,timeout=5).returncode==0
            except (OSError,subprocess.TimeoutExpired):
                available=False
            if not available:
                print(json.dumps(result(prefix,state,name,a.thread,repo,'manual_required',agent,model)))
                return
            unit_dir=Path(config['unit_dir'])
            rendered=units(prefix,state,a.thread,name,repo,sys.executable,config['codex'],instance=key,
                           agent=agent,model=model,dsh_url=config.get('dsh_url'),
                           dsh_credentials=config.get('dsh_credentials'))
            unit_dir.mkdir(parents=True,exist_ok=True)
            for filename,content in rendered.items():
                target=unit_dir/filename
                check_owned_unit(target)
                target.write_text(content)
            subprocess.run(['systemctl','--user','daemon-reload'],check=True)
            # Per-conversation services start now, not at every login forever.
            subprocess.run(['systemctl','--user','start',*rendered],check=True)
            for _ in range(50):
                if notifier_ready(state,bridge_status(prefix,state)):
                    print(json.dumps(result(prefix,state,name,a.thread,repo,'running',agent,model)))
                    return
                time.sleep(.1)
            raise RuntimeError('service started but bridge is not ready; inspect its journal')
    # Release the short registration lock before entering the long-lived supervisor.
    if a.action=='run':
        supervisor(prefix,config,state,a.thread,repo,name,agent,model)


if __name__=='__main__':
    main()
