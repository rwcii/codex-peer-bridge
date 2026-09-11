#!/usr/bin/env python3
"""Remove the default installation while preserving inbox state."""
from pathlib import Path
import subprocess
from install import FILES, SERVICES, active_units, check_owned_unit


def main():
    unit_dir = Path.home()/'.config/systemd/user'
    prefix = Path.home()/'.local/share/codex-peer-bridge'
    subprocess.run(['systemctl','--user','show-environment'],check=True,stdout=subprocess.DEVNULL)
    existing = active_units(unit_dir)
    if existing:
        subprocess.run(['systemctl','--user','disable','--now',*existing],check=True)
    for name in SERVICES:
        path = unit_dir/name
        check_owned_unit(path)
        path.unlink(missing_ok=True)
    subprocess.run(['systemctl','--user','daemon-reload'],check=True)
    for file in FILES:
        (prefix/file).unlink(missing_ok=True)
    print('Services and default installation removed; inbox state is preserved.')


if __name__ == '__main__':
    main()
