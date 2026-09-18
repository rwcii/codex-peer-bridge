"""Account-local exclusion for bridge notifiers, independent of state roots."""
from contextlib import contextmanager, ExitStack
import fcntl
import hashlib
import json
import os
from pathlib import Path
import stat

import platform_support
from peer_transport import private_dir


class OwnershipError(RuntimeError):
    def __init__(self, code, identity=None):
        super().__init__(code)
        self.code = code
        self.identity = identity

    def result(self):
        return dict(ok=False, code=self.code, participant_lock=self.identity)


def identity(provider, participant):
    if provider not in ('codex', 'deepseek'):
        raise OwnershipError('invalid_provider')
    if (not isinstance(participant, str) or not participant or
            participant != participant.strip() or
            any(ord(c) < 32 or ord(c) == 127 for c in participant)):
        raise OwnershipError('invalid_participant')
    try:
        raw = participant.encode('utf-8')
    except UnicodeError:
        raise OwnershipError('invalid_participant') from None
    if len(raw) > 512:
        raise OwnershipError('invalid_participant')
    payload = json.dumps([provider, 'account-local', participant],
                         ensure_ascii=False, separators=(',', ':')).encode('utf-8')
    return dict(provider=provider, namespace='account-local',
                digest=hashlib.sha256(payload).hexdigest())


@contextmanager
def file_lock(path, conflict, owner):
    """Acquire an owned regular file without following a link or unlinking it.

    Opening a persistent inode is intentional: unlink-on-release could let a new
    caller lock a different inode while an older caller still holds this one.
    """
    fd = None
    try:
        try:
            fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW |
                         os.O_CLOEXEC | os.O_NONBLOCK, 0o600)
            info = os.fstat(fd)
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or
                    info.st_mode & 0o077 or info.st_nlink != 1):
                raise OwnershipError('unsafe_lock_file', owner)
            os.set_inheritable(fd, False)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise OwnershipError(conflict, owner) from None
        except OSError:
            raise OwnershipError('lock_unavailable', owner) from None
        # Do not classify exceptions from the caller's work as lock failures.
        yield fd
    finally:
        if fd is not None:
            os.close(fd)


@contextmanager
def notifier_ownership(state_dir, provider, participant):
    """State lock first, participant lock second; failures release both.

    The account-local policy intentionally excludes duplicate session IDs across
    different homes/harnesses for one provider. A path or URL cannot prove a
    distinct delivery namespace. File descriptors never pass through exec.
    """
    owner = identity(provider, participant)
    if os.getuid() != os.geteuid():
        raise OwnershipError('uid_mismatch', owner)
    try:
        namespace = platform_support.participant_lock_dir().resolve()
    except platform_support.AccountHomeUnavailable:
        raise OwnershipError('account_home_unavailable', owner) from None
    except OSError:
        raise OwnershipError('lock_unavailable', owner) from None
    try:
        state = Path(state_dir).resolve()
    except (OSError, RuntimeError):
        raise OwnershipError('unsafe_lock_directory', owner) from None
    if state == namespace or namespace in state.parents:
        raise OwnershipError('state_overlaps_lock_namespace', owner)
    try:
        private_dir(state)
        private_dir(namespace)
    except (OSError, ValueError):
        raise OwnershipError('unsafe_lock_directory', owner) from None
    with ExitStack() as stack:
        stack.enter_context(file_lock(state / 'notifier.lock', 'notifier_in_use', owner))
        stack.enter_context(file_lock(namespace / (owner['digest'] + '.lock'),
                                      'participant_in_use', owner))
        yield owner
