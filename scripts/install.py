#!/usr/bin/env python3
"""Install a per-user bridge and optional systemd user services."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

MARKER = '# Managed by codex-peer-bridge\n'
SERVICES = ('codex-peer-notify.service', 'codex-peer-bridge.service')

FILES = ('CHANGELOG.md', 'session.py', 'codex_instructions.py', 'scripts/install.py', 'scripts/uninstall.py', 'scripts/uninstall.sh', 'bridge.py', 'notify.py', 'README.md', 'PROTOCOL.md', 'LICENSE', 'CONTRIBUTING.md', 'AGENTS.md', 'docs/INSTALL.md')


def unit_arg(value):
    if any(c in str(value) for c in '\n\r\x00'):
        raise ValueError('unit arguments cannot contain newlines or NUL')
    # systemd specifiers and ExecStart environment expansion are distinct from shell quoting.
    return json.dumps(str(value).replace('%', '%%').replace('$', '$$'), ensure_ascii=False)


def units(prefix, state, thread, name, repo, python, codex, instance=None):
    base = [python, str(prefix/'bridge.py'), '--state-dir', str(state), 'serve']
    watcher = [python, str(prefix/'notify.py'), '--state-dir', str(state), '--thread', thread,
               '--name', name, '--repo', repo, '--codex', codex]
    common = '\nRestart=on-failure\nRestartSec=5\nUMask=0077\n\n[Install]\nWantedBy=default.target\n'
    rendered = {
        'codex-peer-bridge.service': MARKER + '[Unit]\nDescription=Local Codex peer messaging bridge\n\n[Service]\nType=simple\nExecStart=' + ' '.join(map(unit_arg,base)) + common,
        'codex-peer-notify.service': MARKER + '[Unit]\nDescription=Codex peer inbox notifications\nRequires=codex-peer-bridge.service\nAfter=codex-peer-bridge.service\n\n[Service]\nType=simple\nExecStart=' + ' '.join(map(unit_arg,watcher)) + common,
    }

    if instance:
        import re
        if not re.fullmatch(r'[a-f0-9]{16}',instance):
            raise ValueError('invalid service instance')
        supervisor = [python,str(prefix/'session.py'),'run','--thread',thread,'--repo',repo]
        rendered = {f'codex-peer-session-{instance}.service': MARKER +
                    '[Unit]\nDescription=Codex peer session supervisor\n\n[Service]\nType=simple\nExecStart=' +
                    ' '.join(map(unit_arg,supervisor)) + common}
    return rendered


def check_owned_unit(path):
    if path.is_symlink():
        raise ValueError(f'refusing symlinked service file: {path}')
    if path.exists() and (not path.is_file() or path.stat().st_uid != os.getuid()
                          or not path.read_text().startswith(MARKER)):
        raise ValueError(f'refusing unrelated service file: {path}')


def active_units(unit_dir):
    existing = []
    for name in SERVICES:
        local = unit_dir/name
        check_owned_unit(local)
        result = subprocess.run(['systemctl','--user','show',name,'--property=FragmentPath',
                                 '--value'],check=True,capture_output=True,text=True)
        fragment = result.stdout.strip()
        if fragment:
            actual = Path(fragment)
            if actual != local:
                raise ValueError(f'refusing service outside selected unit directory: {name}')
            check_owned_unit(actual)
            existing.append(name)
        elif local.exists():
            existing.append(name)
    return existing


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--thread', help='exact existing Codex thread ID')
    p.add_argument('--configure-codex', action='store_true', help='install managed global guidance and per-session registration')
    p.add_argument('--codex-home', type=Path, default=Path(os.environ.get('CODEX_HOME', str(Path.home()/'.codex'))))
    p.add_argument('--name', default='codex-peer')
    p.add_argument('--repo', default=os.getcwd())
    p.add_argument('--prefix', type=Path, default=Path.home()/'.local/share/codex-peer-bridge')
    p.add_argument('--state-dir', type=Path, default=Path.home()/'.local/state/codex-peer-bridge')
    p.add_argument('--unit-dir', type=Path, default=Path.home()/'.config/systemd/user')
    p.add_argument('--codex', default=shutil.which('codex'))
    p.add_argument('--no-start', action='store_true', help='write files and units without calling systemctl')
    a = p.parse_args()
    if sys.platform != 'linux' or sys.version_info < (3,11):
        p.error('Linux and Python 3.11+ are required')
    if not a.codex or not Path(a.codex).is_absolute() or not os.access(a.codex,os.X_OK):
        p.error('provide an executable absolute --codex path, or install Codex CLI on PATH')
    a.prefix, a.state_dir, a.unit_dir = [x.expanduser().resolve() for x in (a.prefix,a.state_dir,a.unit_dir)]
    if not a.thread and not a.configure_codex:
        p.error('--thread or --configure-codex is required')
    if a.configure_codex:
        os.umask(0o077)
        a.prefix.mkdir(parents=True,exist_ok=True)
        source = Path(__file__).resolve().parent.parent
        for file in FILES:
            dest=a.prefix/file
            dest.parent.mkdir(parents=True,exist_ok=True)
            if (source/file).resolve() != dest.resolve():
                shutil.copyfile(source/file,dest)
        sys.path.insert(0,str(a.prefix))
        from codex_instructions import update
        guidance=update(a.codex_home,a.prefix)
        (a.prefix/'install.json').write_text(json.dumps(dict(state_root=str(a.state_dir),
            unit_dir=str(a.unit_dir),codex=a.codex,codex_home=str(a.codex_home.expanduser().resolve()))))
        print('Installed runtime:',a.prefix)
        print('Managed Codex guidance:',guidance)
        print('New sessions run session.py ensure with their own CODEX_THREAD_ID.')
        if a.thread and not a.no_start:
            subprocess.run([sys.executable,str(a.prefix/'session.py'),'ensure','--thread',a.thread,'--repo',a.repo],check=True)
        return
    checkpoint = a.state_dir/'notify-cursor.json'
    if checkpoint.exists() and json.loads(checkpoint.read_text())['thread'] != a.thread:
        p.error('existing state belongs to a different thread; select another state directory')
    rendered = units(a.prefix,a.state_dir,a.thread,a.name,str(Path(a.repo).resolve()),sys.executable,a.codex)
    for name in SERVICES:
        check_owned_unit(a.unit_dir/name)
    if not a.no_start:
        # Fail before modifying installation if the user manager is unavailable.
        subprocess.run(['systemctl','--user','show-environment'],check=True,stdout=subprocess.DEVNULL)
        existing = active_units(a.unit_dir)
        if existing:
            subprocess.run(['systemctl','--user','stop',*existing],check=True)
    os.umask(0o077)
    a.prefix.mkdir(parents=True,exist_ok=True)
    a.unit_dir.mkdir(parents=True,exist_ok=True)
    source = Path(__file__).resolve().parent.parent
    for file in FILES:
        (a.prefix/file).parent.mkdir(parents=True,exist_ok=True)
        shutil.copyfile(source/file,a.prefix/file)
    for name,content in rendered.items():
        (a.unit_dir/name).write_text(content)
    if a.no_start:
        print('Files and units written; services were not changed.')
    else:
        subprocess.run(['systemctl','--user','daemon-reload'],check=True)
        subprocess.run(['systemctl','--user','enable','--now',*rendered],check=True)
    print('Installed at',a.prefix)
    print('State directory:',a.state_dir)
    print('Check: systemctl --user status codex-peer-bridge codex-peer-notify')


if __name__ == '__main__':
    main()
