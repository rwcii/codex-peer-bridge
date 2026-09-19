#!/usr/bin/env python3
"""Install a per-user bridge and optional systemd user services."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

# Run directly, `scripts/` is sys.path[0], so the project root is added to reach
# the platform module rather than testing the platform here.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import platform_support
import runtime_names

MARKER = runtime_names.SERVICE_MARKER
SERVICES = runtime_names.service_names()

FILES = ('runtime_names.py', 'participant_instructions.py', 'session_observation.py', 'durable_state.py', 'notification_delivery.py', 'notification_health.py', 'notification_journal.py', 'notification_legacy.py', 'notification_memory.py', 'notification_migration.py', 'notification_notices.py', 'notification_provider.py', 'notification_runtime.py', 'notification_source.py', 'notification_state.py', 'subscriptions.py','memory_bindings.py', 'inbox_schema.py', 'database_worker.py', 'service_runtime.py', 'participant_lock.py', 'peer_transport.py', 'peer_guidance.py', 'CHANGELOG.md', 'memory.py', 'session.py', 'codex_instructions.py', 'platform_support.py', 'dsh_delivery.py', 'scripts/install.py', 'scripts/uninstall.py', 'scripts/uninstall.sh', 'bridge.py', 'notify.py', 'README.md', 'PROTOCOL.md', 'LICENSE', 'CONTRIBUTING.md', 'AGENTS.md', 'docs/INSTALL.md', 'docs/NOTIFIER.md', 'docs/IDENTIFIER-MIGRATION.md', 'docs/PARITY-MEMORY-DESIGN.md')


def unit_arg(value):
    if any(c in str(value) for c in '\n\r\x00'):
        raise ValueError('unit arguments cannot contain newlines or NUL')
    # systemd specifiers and ExecStart environment expansion are distinct from shell quoting.
    return json.dumps(str(value).replace('%', '%%').replace('$', '$$'), ensure_ascii=False)


def units(prefix, state, thread, name, repo, python, codex, instance=None,
          agent='codex', model=None, dsh_url=None, dsh_credentials=None, legacy=False):
    base = [python, str(prefix/'bridge.py'), '--state-dir', str(state), 'serve']
    watcher = [python, str(prefix/'notify.py'), '--state-dir', str(state), '--thread', thread,
               '--name', name, '--repo', repo]
    # Codex is the notifier's own default, so a Codex install renders exactly the
    # argv it did before participants existed.
    if agent != 'codex':
        watcher += ['--agent', agent]
    if agent == 'deepseek':
        # A service does not inherit the harness environment, so the endpoint and
        # credential path are recorded explicitly.
        for flag, value in (('--dsh-url', dsh_url), ('--dsh-credentials', dsh_credentials)):
            if value:
                watcher += [flag, str(value)]
    else:
        watcher += ['--codex', codex]
    common = '\nRestart=on-failure\nRestartSec=5\nUMask=0077\n\n[Install]\nWantedBy=default.target\n'
    permanent = '\nRestartPreventExitStatus=' + ' '.join(map(str, platform_support.PERMANENT_EXIT_STATUSES))
    notify_name, bridge_name = runtime_names.service_names(legacy=legacy)
    rendered = {
        bridge_name: MARKER + '[Unit]\nDescription=Koinon local peer messaging bridge\n\n[Service]\nType=simple\nExecStart=' + ' '.join(map(unit_arg,base)) + permanent + common,
        notify_name: MARKER + f'[Unit]\nDescription=Koinon inbox notifications\nRequires={bridge_name}\nAfter={bridge_name}\n\n[Service]\nType=simple\nExecStart=' + ' '.join(map(unit_arg,watcher)) + permanent + common,
    }

    if instance:
        import re
        if not re.fullmatch(r'[a-f0-9]{16}',instance):
            raise ValueError('invalid service instance')
        supervisor = start_command_for(python, prefix, thread, repo, agent, model)
        rendered = {runtime_names.service_names(instance, legacy=legacy)[0]: MARKER +
                    '[Unit]\nDescription=Koinon session supervisor\n\n[Service]\nType=simple\nExecStart=' +
                    ' '.join(map(unit_arg,supervisor)) + permanent + common}
    return rendered


def start_command_for(python, prefix, thread, repo, agent, model):
    """Supervisor argv for a service instance, mirroring session.py's own shape."""
    command = [python, str(prefix/'session.py'), 'run', '--thread', thread, '--repo', repo]
    if agent != 'codex':
        command += ['--agent', agent]
    if model:
        command += ['--model', model]
    return command


def check_owned_unit(path, prefix=None):
    if path.is_symlink():
        raise ValueError(f'refusing symlinked service file: {path}')
    if path.exists() and (not path.is_file() or path.stat().st_uid != os.getuid()
                          or not path.read_text().startswith(runtime_names.SERVICE_MARKERS)):
        raise ValueError(f'refusing unrelated service file: {path}')

    if prefix is not None and path.exists() and not unit_targets_prefix(path, prefix):
        raise ValueError(f'refusing service owned by a different installation: {path}')


def unit_arguments(path):
    starts = [line[len('ExecStart='):] for line in path.read_text().splitlines()
              if line.startswith('ExecStart=')]
    if len(starts) != 1:
        raise ValueError(f'expected one managed service command: {path}')
    # The renderer uses JSON-quoted string literals. Shell parsing does not
    # decode escapes such as \t the same way and can misidentify an owned unit.
    text, arguments, offset = starts[0], [], 0
    decoder = json.JSONDecoder()
    while offset < len(text):
        if text[offset].isspace():
            offset += 1
            continue
        if text[offset] == '"':
            value, end = decoder.raw_decode(text, offset)
            if end < len(text) and not text[end].isspace():
                raise ValueError(f'ambiguous managed service argument: {path}')
        else:
            end = offset
            while end < len(text) and not text[end].isspace():
                end += 1
            value = text[offset:end]
            if any(char in value for char in ('"', "'", '\\')):
                raise ValueError(f'unsupported managed service quoting: {path}')
        arguments.append(value)
        offset = end
    return arguments


def unit_literal(value, path):
    """Decode only literal systemd escaping, never a variable or specifier."""
    result = []
    index = 0
    while index < len(value):
        char = value[index]
        if char in ('%', '$'):
            if index + 1 == len(value) or value[index + 1] != char:
                raise ValueError(f'managed unit contains a variable or specifier: {path}')
            index += 1
        result.append(char)
        index += 1
    return ''.join(result)


def unit_targets_prefix(path, prefix):
    executable = 'session.py' if '-session-' in path.name else ('notify.py' if '-notify.' in path.name else 'bridge.py')
    arguments = unit_arguments(path)
    if len(arguments) < 2:
        raise ValueError(f'managed service executable cannot be determined: {path}')
    supplied = Path(unit_literal(arguments[1], path))
    if not supplied.is_absolute():
        raise ValueError(f'managed service executable is not absolute: {path}')
    # These are installation files, not peer socket addresses. macOS aliases
    # and explicit directory symlinks must identify the same installed script.
    return supplied.resolve() == (Path(prefix) / executable).resolve()


def saved_unit_option(path, flag):
    if not path.exists():
        return None
    arguments = unit_arguments(path)
    if arguments.count(flag) != 1:
        raise ValueError(f'missing or duplicate {flag} in managed service: {path}')
    index = arguments.index(flag) + 1
    if index >= len(arguments):
        raise ValueError(f'missing {flag} value in managed service: {path}')
    return unit_literal(arguments[index], path)


def active_units(unit_dir, names=SERVICES, prefix=None):
    existing = []
    for name in names:
        local = unit_dir/name
        check_owned_unit(local, prefix)
        result = subprocess.run(['systemctl','--user','show',name,'--property=FragmentPath',
                                 '--value'],check=True,capture_output=True,text=True)
        fragment = result.stdout.strip()
        if fragment:
            actual = Path(fragment)
            if actual != local:
                raise ValueError(f'refusing service outside selected unit directory: {name}')
            check_owned_unit(actual, prefix)
            existing.append(name)
        elif local.exists():
            existing.append(name)
    return existing


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--thread', help='exact existing Codex thread ID')
    p.add_argument('--configure-codex', action='store_true', help='install managed global guidance and per-session registration')
    p.add_argument('--configure-deepseek', action='store_true',
                   help='install managed harness guidance for DeepSeek (DSH) sessions')
    p.add_argument('--codex-home', type=Path)
    p.add_argument('--dsh-home', type=Path,
                   default=None,
                   help='harness home whose AGENTS.md receives the managed DeepSeek section')
    p.add_argument('--name')
    p.add_argument('--repo')
    p.add_argument('--prefix', type=Path)
    p.add_argument('--state-dir', type=Path)
    p.add_argument('--unit-dir', type=Path)
    p.add_argument('--codex', default=shutil.which('codex'))
    p.add_argument('--no-start', action='store_true', help='write files and units without calling systemctl')
    a = p.parse_args()
    if not platform_support.SUPPORTED or sys.version_info < (3,11):
        p.error('Linux or macOS with Python 3.11+ is required')
    if not a.codex or not Path(a.codex).is_absolute() or not os.access(a.codex,os.X_OK):
        p.error('provide an executable absolute --codex path, or install Codex CLI on PATH')
    a.prefix = (a.prefix or runtime_names.default_prefix()).expanduser().resolve()
    config_path = a.prefix / 'install.json'
    previous = runtime_names.install_config(a.prefix)
    a.state_dir = Path(a.state_dir or previous.get('state_root') or runtime_names.default_state_root()).expanduser().resolve()
    a.unit_dir = Path(a.unit_dir or previous.get('unit_dir') or Path.home()/'.config/systemd/user').expanduser().resolve()
    if not a.thread and not (a.configure_codex or a.configure_deepseek):
        p.error('--thread, --configure-codex or --configure-deepseek is required')
    if a.configure_codex or a.configure_deepseek:
        a.codex_home = a.codex_home or Path(previous.get('codex_home') or
                                          os.environ.get('CODEX_HOME', str(Path.home()/'.codex')))
        a.dsh_home = a.dsh_home or Path(previous.get('dsh_home') or
                                      os.environ.get('DSH_HOME', str(Path.home()/'.dsh')))
        participants = set(previous.get('participants', ['codex'] if previous else []))
        participants.update(name for name, chosen in (('codex', a.configure_codex),
                                                      ('deepseek', a.configure_deepseek)) if chosen)
        os.umask(0o077)
        a.prefix.mkdir(parents=True,exist_ok=True)
        source = Path(__file__).resolve().parent.parent
        for file in FILES:
            dest=a.prefix/file
            dest.parent.mkdir(parents=True,exist_ok=True)
            if (source/file).resolve() != dest.resolve():
                shutil.copyfile(source/file,dest)
        sys.path.insert(0,str(a.prefix))
        from participant_instructions import update
        guidance = update(a.codex_home,a.prefix) if a.configure_codex else None
        # The harness reads its guidance from AGENTS.md in the harness home, so a
        # DeepSeek session learns to register and read its inbox the same way a
        # Codex session does.
        dsh_guidance = update(a.dsh_home,a.prefix,agent='deepseek') if a.configure_deepseek else None
        (a.prefix/'install.json').write_text(json.dumps(dict(state_root=str(a.state_dir),
            unit_dir=str(a.unit_dir),codex=a.codex,codex_home=str(a.codex_home.expanduser().resolve()),
            dsh_home=str(a.dsh_home.expanduser().resolve()),
            # Which managed sections this installation wrote, so uninstall removes
            # exactly those and leaves any other participant's guidance alone.
            participants=sorted(participants),
            dsh_url=(os.environ.get('DSH_WEB_URL', previous.get('dsh_url'))
                     if a.configure_deepseek else previous.get('dsh_url')),
            dsh_credentials=(str(a.dsh_home.expanduser().resolve()/'.credentials.yaml')
                             if a.configure_deepseek else previous.get('dsh_credentials')))))
        print('Installed runtime:',a.prefix)
        print('State directory:',a.state_dir)
        if guidance is not None:
            print('Managed Codex guidance:',guidance)
        if dsh_guidance is not None:
            print('Managed DeepSeek guidance:',dsh_guidance)
        print('New sessions run session.py ensure with their own session identity.')
        if a.thread and not a.no_start:
            subprocess.run([sys.executable,str(a.prefix/'session.py'),'ensure','--thread',a.thread,'--repo',a.repo or os.getcwd()],check=True)
        return
    if platform_support.SERVICE_MANAGER is None and not a.no_start:
        # The managed supervisor is the portable alternative to a service manager:
        # `session.py ensure` reports `manual_required` with a start command, and
        # `session.py run` owns both children in one persistent session.
        p.error('systemd user services are Linux-only; on macOS use --configure-codex and run the '
                'printed start_command in a managed session, or pass --no-start to write files only')
    checkpoint = a.state_dir/'notify-cursor.json'
    if checkpoint.exists() and json.loads(checkpoint.read_text())['thread'] != a.thread:
        p.error('existing state belongs to a different thread; select another state directory')
    selected = runtime_names.selected_service_names(a.unit_dir)
    for name in selected:
        check_owned_unit(a.unit_dir/name, a.prefix)
    notifier_unit = a.unit_dir / selected[0]
    saved_target = saved_unit_option(notifier_unit, '--thread')
    if saved_target is not None and saved_target != a.thread:
        p.error('existing service belongs to a different thread; preserve its state and select a separate installation')
    a.name = a.name or saved_unit_option(notifier_unit, '--name') or 'codex-peer'
    a.repo = a.repo or saved_unit_option(notifier_unit, '--repo') or os.getcwd()
    rendered = units(a.prefix,a.state_dir,a.thread,a.name,str(Path(a.repo).resolve()),sys.executable,a.codex,
                     legacy=selected == runtime_names.service_names(legacy=True))
    if not a.no_start:
        # Fail before modifying installation if the user manager is unavailable.
        subprocess.run(['systemctl','--user','show-environment'],check=True,stdout=subprocess.DEVNULL)
        existing = active_units(a.unit_dir, selected, a.prefix)
        if existing:
            subprocess.run(['systemctl','--user','stop',*existing],check=True)
    os.umask(0o077)
    a.prefix.mkdir(parents=True,exist_ok=True)
    a.unit_dir.mkdir(parents=True,exist_ok=True)
    source = Path(__file__).resolve().parent.parent
    for file in FILES:
        (a.prefix/file).parent.mkdir(parents=True,exist_ok=True)
        if (source/file).resolve() != (a.prefix/file).resolve():
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
    print('Check: systemctl --user status', *selected)


if __name__ == '__main__':
    try:
        main()
    except runtime_names.NameConflict as exc:
        print(json.dumps(dict(ok=False, code=exc.code, paths=exc.paths)))
        raise SystemExit(platform_support.CONFIGURATION_EXIT_STATUS) from None
