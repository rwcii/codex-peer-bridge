#!/usr/bin/env python3
"""Idempotent per-thread registration for Codex sessions."""
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

from bridge import private_dir
from scripts.install import units, check_owned_unit


def identity(thread):
    if not thread or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}',thread):
        raise ValueError('a valid explicit thread or CODEX_THREAD_ID is required')
    return hashlib.sha256(thread.encode()).hexdigest()[:16]


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
        actual=Path(f'/proc/{pid}/stat').read_text().rsplit(')',1)[1].split()[19]
        if actual != ready['proc_start']:
            return False
    except (OSError,ValueError,KeyError,IndexError):
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


def details(prefix, config, thread, repo):
    key = identity(thread)
    state = Path(config['state_root'])/'sessions'/key
    label = re.sub(r'[^a-z0-9-]+','-',Path(repo).name.lower()).strip('-')[:32] or 'session'
    return state, f'codex-{label}-{key}', key


def start_command(prefix, thread, repo):
    return [sys.executable,str(prefix/'session.py'),'run','--thread',thread,'--repo',repo]


def result(prefix, state, name, thread, repo, status):
    return dict(status=status, name=name, state_dir=str(state),
                start_command=shlex.join(start_command(prefix,thread,repo)),
                inbox_command=shlex.join([sys.executable,str(prefix/'bridge.py'),'--state-dir',str(state),'inbox']))


def supervisor(prefix, config, state, thread, repo, name):
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
            children.append(subprocess.Popen([sys.executable,str(prefix/'notify.py'),'--state-dir',str(state),
                            '--thread',thread,'--name',name,'--repo',repo,'--codex',config['codex']]))
            for _ in range(100):
                if stopped or children[-1].poll() is not None:
                    raise RuntimeError('notifier exited during startup')
                if notifier_ready(state,bridge_status(prefix,state)):
                    break
                time.sleep(.1)
            else:
                raise RuntimeError('notifier did not become ready')
            print(json.dumps(result(prefix,state,name,thread,repo,'running')),flush=True)
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
    p.add_argument('action',choices=['ensure','run','status','stop'])
    p.add_argument('--thread',default=os.environ.get('CODEX_THREAD_ID'))
    p.add_argument('--repo',default=os.getcwd())
    a=p.parse_args()
    prefix=Path(__file__).resolve().parent
    config=read_config(prefix)
    repo=str(Path(a.repo).resolve())
    state,name,key=details(prefix,config,a.thread,repo)
    private_dir(state)
    with (state/'registration.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        registration=state/'session.json'
        if registration.exists():
            saved=json.loads(registration.read_text())
            if saved['thread'] != a.thread:
                raise ValueError('thread identity collision')
            name,repo=saved['name'],saved['repo']
        else:
            registration.write_text(json.dumps(dict(thread=a.thread,name=name,repo=repo)))
        active=bridge_status(prefix,state)
        healthy=notifier_ready(state,active)
        if a.action=='status' or (a.action=='ensure' and active):
            data=result(prefix,state,name,a.thread,repo,'running' if healthy else ('repair_required' if active else 'stopped'))
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
                print(json.dumps(result(prefix,state,name,a.thread,repo,'manual_required')))
                return
            unit_dir=Path(config['unit_dir'])
            rendered=units(prefix,state,a.thread,name,repo,sys.executable,config['codex'],instance=key)
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
                    print(json.dumps(result(prefix,state,name,a.thread,repo,'running')))
                    return
                time.sleep(.1)
            raise RuntimeError('service started but bridge is not ready; inspect its journal')
    # Release the short registration lock before entering the long-lived supervisor.
    if a.action=='run':
        supervisor(prefix,config,state,a.thread,repo,name)


if __name__=='__main__':
    main()
