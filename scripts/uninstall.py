#!/usr/bin/env python3
"""Stop installed sessions, remove owned services/guidance, and preserve inbox data."""
import argparse
import json
from pathlib import Path
import re
import subprocess
import sys
from install import FILES, SERVICES, active_units, check_owned_unit, unit_arg


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--prefix',type=Path,default=Path.home()/'.local/share/codex-peer-bridge')
    a=p.parse_args()
    prefix=a.prefix.expanduser().resolve()
    config_path=prefix/'install.json'
    config=json.loads(config_path.read_text()) if config_path.exists() else {}
    unit_dir=Path(config.get('unit_dir',str(Path.home()/'.config/systemd/user')))
    state_root=Path(config.get('state_root',str(Path.home()/'.local/state/codex-peer-bridge')))
    owned=[name for name in SERVICES if not (unit_dir/name).exists() or
           unit_arg(str(prefix/('notify.py' if 'notify' in name else 'bridge.py'))) in (unit_dir/name).read_text()]
    owned += [path.name for path in unit_dir.glob('codex-peer-session-*.service')
              if re.fullmatch(r'codex-peer-session-[a-f0-9]{16}\.service',path.name)
              and unit_arg(str(prefix/'session.py')) in path.read_text()]
    for name in owned:
        check_owned_unit(unit_dir/name)
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
        from codex_instructions import update
        update(Path(config['codex_home']),prefix,remove=True)
    for file in FILES:
        (prefix/file).unlink(missing_ok=True)
    config_path.unlink(missing_ok=True)
    print('Owned services, managed guidance and runtime removed; inbox state preserved.')


if __name__ == '__main__':
    main()
