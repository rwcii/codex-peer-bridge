"""Conservative lifecycle observations; failed control requests are not stops."""
import fcntl
import os
from pathlib import Path
import stat

import platform_support


def lock_held(path):
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
    except FileNotFoundError:
        return False
    try:
        info = os.fstat(fd)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                or info.st_mode & 0o077 or info.st_nlink != 1):
            raise ValueError('unsafe lifecycle lock')
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        return False
    finally:
        os.close(fd)


def endpoint_present(root):
    root = Path(root)
    candidates = {platform_support.control_socket_path(root),
                  platform_support.fallback_control_socket(root)}
    legacy = platform_support.legacy_control_socket(root)
    if legacy is not None:
        candidates.add(legacy)
    for candidate in candidates:
        try:
            candidate.lstat()
        except FileNotFoundError:
            continue
        return True
    return False


def lifecycle(state, bridge):
    if bridge is not None:
        return 'running'
    try:
        if (endpoint_present(state) or endpoint_present(Path(state) / 'notifier')
                or lock_held(Path(state) / 'notifier.lock')
                or lock_held(Path(state) / 'supervisor.lock')):
            return 'unknown'
    except (OSError, ValueError, RuntimeError):
        return 'unknown'
    # This observes absence under the caller's managed lifecycle lock. A future
    # independent manual start can change it; no probe promises future absence.
    return 'stopped'
