#!/usr/bin/env python3
"""Stop installed sessions, remove owned services/guidance, and preserve inbox data."""
import argparse
import json
from pathlib import Path
import re
import subprocess
import sys
from install import FILES, check_owned_unit, unit_targets_prefix
import platform_support
import runtime_names


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--prefix',type=Path)
    a=p.parse_args()
    prefix=(a.prefix or runtime_names.default_prefix()).expanduser().resolve()
    config_path=prefix/'install.json'
    config=runtime_names.install_config(prefix)
    unit_dir=Path(config.get('unit_dir',str(Path.home()/'.config/systemd/user')))
    state_root=Path(config.get('state_root') or runtime_names.default_state_root())
    candidates = set(runtime_names.service_names()) | set(runtime_names.service_names(legacy=True))
    for stem in ('koinon', 'codex-peer'):
        candidates.update(path.name for path in unit_dir.glob(f'{stem}-session-*.service')
                          if re.fullmatch(r'(?:koinon|codex-peer)-session-[a-f0-9]{16}\.service', path.name))
    owned = []
    for name in sorted(candidates):
        path = unit_dir / name
        if not runtime_names.present(path):
            continue
        # Other installations can share the unit directory. Remove only a unit
        # whose marker and executable both identify this installation.
        if path.is_symlink() or not path.is_file():
            raise ValueError(f'refusing unsafe service file: {path}')
        if not unit_targets_prefix(path, prefix):
            continue
        # An apparent unit for this prefix with a missing marker is a refusal,
        # not a reason to delete its executable and orphan the enabled unit.
        check_owned_unit(path, prefix)
        owned.append(name)
    if config:
        for registration in (state_root/'sessions').glob('*/session.json'):
            thread=json.loads(registration.read_text())['thread']
            subprocess.run([sys.executable,str(prefix/'session.py'),'stop','--thread',thread],check=True)
    existing=[name for name in owned if (unit_dir/name).exists()]
    if existing:
        subprocess.run(['systemctl','--user','disable','--now',*existing],check=True)
        for name in existing:
            (unit_dir/name).unlink()
        subprocess.run(['systemctl','--user','daemon-reload'],check=True)
    if config:
        sys.path.insert(0,str(prefix))
        from participant_instructions import update
        # Remove the managed section for every participant this installation
        # configured, not just Codex: a harness section left behind would keep
        # telling sessions to register against a runtime that is gone. Older
        # installations recorded no participant list and were Codex-only.
        homes = {'codex': config.get('codex_home'), 'deepseek': config.get('dsh_home')}
        for agent in config.get('participants', ['codex']):
            home = homes.get(agent)
            if home:
                update(Path(home),prefix,remove=True,agent=agent)
    for file in FILES:
        (prefix/file).unlink(missing_ok=True)
    config_path.unlink(missing_ok=True)
    print('Owned services, managed guidance and runtime removed; inbox state preserved.')


if __name__ == '__main__':
    try:
        main()
    except runtime_names.NameConflict as exc:
        print(json.dumps(dict(ok=False, code=exc.code, paths=exc.paths)))
        raise SystemExit(platform_support.CONFIGURATION_EXIT_STATUS) from None
