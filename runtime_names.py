"""Product defaults and deliberately stable cross-version identifiers."""
import os
import json
import sys
from pathlib import Path
import re

NAME = 'koinon'
LEGACY_NAME = 'codex-peer-bridge'
SERVICE_MARKER = '# Managed by koinon\n'
LEGACY_SERVICE_MARKER = '# Managed by codex-peer-bridge\n'
SERVICE_MARKERS = (SERVICE_MARKER, LEGACY_SERVICE_MARKER)

# These values cross an external or old-client boundary; they are not branding.
REGISTRY_ENTRYPOINT = 'codex-peer-bridge'
MEMORY_SERVICE = 'codex-peer-memory'
PATH_SELECTION_CODES = frozenset(('ambiguous_default_paths', 'unsafe_default_path', 'default_path_unavailable'))
CONFIGURATION_CODES = PATH_SELECTION_CODES | {'ambiguous_service_units', 'invalid_install_configuration'}
GUIDANCE_LOCK_NAMES = {'codex': '.codex-peer-bridge.lock',
                       'deepseek': '.deepseek-peer-bridge.lock'}


class NameConflict(ValueError):
    def __init__(self, code, paths):
        if code not in CONFIGURATION_CODES:
            raise KeyError(code)
        self.code = code
        self.paths = tuple(str(path) for path in paths)
        advice = 'preserve and repair configuration' if code == 'invalid_install_configuration' else 'select an explicit path'
        super().__init__(code + ': ' + advice + '; ' + ', '.join(self.paths))


def present(path):
    try:
        Path(path).lstat()
        return True
    except FileNotFoundError:
        return False


def choose_default(parent):
    """Read-only selection. Even a broken symlink prevents a silent new install."""
    current, legacy = (Path(parent) / name for name in (NAME, LEGACY_NAME))
    try:
        new_present, old_present = present(current), present(legacy)
    except OSError as exc:
        raise NameConflict('default_path_unavailable', (current, legacy)) from exc
    if new_present and old_present:
        raise NameConflict('ambiguous_default_paths', (current, legacy))
    selected = legacy if old_present else current
    if present(selected) and (selected.is_symlink() or not selected.is_dir()):
        raise NameConflict('unsafe_default_path', (selected,))
    return selected


def default_prefix():
    return reported_default(Path.home() / '.local' / 'share', 'prefix')


def default_state_root():
    base = Path(os.environ.get('XDG_STATE_HOME', str(Path.home() / '.local' / 'state')))
    return reported_default(base, 'state')


def service_names(instance=None, *, legacy=False):
    if instance is not None and re.fullmatch('[a-f0-9]{16}', instance) is None:
        raise ValueError('invalid service instance')
    stem = 'codex-peer' if legacy else NAME
    if instance is not None:
        return (f'{stem}-session-{instance}.service',)
    return (f'{stem}-notify.service', f'{stem}-bridge.service')


def selected_service_names(unit_dir, instance=None):
    """Keep an existing unit family; never silently create a parallel family."""
    current, legacy = service_names(instance), service_names(instance, legacy=True)
    new_present = [Path(unit_dir) / name for name in current if present(Path(unit_dir) / name)]
    old_present = [Path(unit_dir) / name for name in legacy if present(Path(unit_dir) / name)]
    if new_present and old_present:
        raise NameConflict('ambiguous_service_units', (*new_present, *old_present))
    return legacy if old_present else current


def reported_default(base, label):
    selected = choose_default(base)
    reason = 'legacy path reused' if selected.name == LEGACY_NAME else 'Koinon default'
    print(f'{label}: {selected} ({reason})', file=sys.stderr)
    return selected


def install_config(prefix):
    """Absence is valid for old explicit-thread installs; invalid evidence refuses."""
    path = Path(prefix) / 'install.json'
    try:
        if not present(path):
            return {}
        if path.is_symlink() or not path.is_file():
            raise ValueError('configuration is not a regular file')
        result = json.loads(path.read_text())
        if not isinstance(result, dict):
            raise ValueError('configuration is not an object')
        for key in ('state_root', 'unit_dir'):
            if not isinstance(result.get(key), str) or not Path(result[key]).is_absolute():
                raise ValueError('missing or nonabsolute installation path')
        return result
    except (OSError, ValueError) as exc:
        raise NameConflict('invalid_install_configuration', (path,)) from exc
