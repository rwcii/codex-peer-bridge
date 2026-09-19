#!/usr/bin/env python3
"""Platform-specific process, credential, and socket primitives.

The bridge speaks one wire protocol on every platform, but the local facts it
relies on differ between Linux and macOS. This module is the only place that
knows those differences, so `bridge.py`, `notify.py`, and `session.py` stay
platform-neutral.

Three differences matter:

* **Peer identity.** Linux exposes `SO_PEERCRED`, one getsockopt returning the
  peer pid, uid, and gid. macOS has no such option: `getpeereid(3)` returns only
  uid and gid, and the peer pid comes from a separate `LOCAL_PEERPID` socket
  option. Both halves are required, so macOS needs two calls where Linux needs
  one.
* **Process start time.** Linux reads field 22 of `/proc/<pid>/stat`, a count of
  clock ticks since boot. macOS has no `/proc`; `ps -o lstart=` reports the
  start time as an asctime string. Claude Code writes `procStart` in UTC
  asctime form on macOS and as the tick count on Linux, so the value we publish
  and the value we compare against must match the local convention exactly.
* **Process namespace.** Linux peers identify themselves as
  `linux:<machine-id>:<pid-namespace>`. macOS Claude Code uses the literal
  string `darwin`.

Python standard library only.
"""
import asyncio
from concurrent.futures import ThreadPoolExecutor
import hashlib
import threading
import os
from pathlib import Path
import socket
import stat
import subprocess
import sys

DARWIN = sys.platform == 'darwin'
LINUX = sys.platform.startswith('linux')

# Capability facts other modules consult instead of testing `sys.platform`
# themselves, so this module stays the only place that knows the differences.
SUPPORTED = DARWIN or LINUX
SERVICE_MANAGER = 'systemd' if LINUX else None
# Explicit configuration/ownership refusals require operator action, not restart loops.
CONFIGURATION_EXIT_STATUS = 78
TEMPORARY_EXIT_STATUS = 75
SOFTWARE_EXIT_STATUS = 70
PERMANENT_EXIT_STATUSES = (SOFTWARE_EXIT_STATUS, CONFIGURATION_EXIT_STATUS)

if not SUPPORTED:
    raise RuntimeError(f'unsupported platform: {sys.platform}')

# macOS socket options. Python exposes neither, so they are spelled out here.
# SOL_LOCAL is 0 on Darwin; LOCAL_PEERPID returns the peer's pid as a C int.
SOL_LOCAL = 0
LOCAL_PEERPID = 0x002

# `sockaddr_un.sun_path`, including the terminating NUL. Keyed by DARWIN so the
# lookup reads as the platform question it is. The usable path is one byte less.
SUN_PATH_BYTES = {True: 104, False: 108}


def peer_pid(sock):
    """Kernel-verified PID of the process at the other end of a unix socket.

    The uid is verified before the pid is returned on both platforms: this
    bridge's authentication policy is same-user only, so a peer from another
    account is rejected rather than described.

    @param sock - a connected AF_UNIX socket.
    @returns the peer pid.
    @throws ValueError when the peer belongs to another user.
    @throws OSError when the kernel cannot report the peer.
    """
    if LINUX:
        import struct
        pid, uid, _ = struct.unpack('3i', sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
        if uid != os.getuid():
            raise ValueError('different user')
        return pid
    uid, _ = _getpeereid(sock.fileno())
    if uid != os.getuid():
        raise ValueError('different user')
    # A peer that connected and vanished before this call makes the option
    # unavailable. Fail closed: the caller treats an unusable credential as a
    # rejected connection.
    import struct
    return struct.unpack('i', sock.getsockopt(SOL_LOCAL, LOCAL_PEERPID, 4))[0]


def _getpeereid(fd):
    """Read uid and gid of a connected peer.

    Darwin's `getpeereid(3)` has no Python binding, so it is called through
    ctypes. It carries no pid; `peer_pid` combines it with LOCAL_PEERPID.
    """
    import ctypes
    libc = ctypes.CDLL(None, use_errno=True)
    uid = ctypes.c_uint()
    gid = ctypes.c_uint()
    if libc.getpeereid(ctypes.c_int(fd), ctypes.byref(uid), ctypes.byref(gid)) != 0:
        errno = ctypes.get_errno()
        raise OSError(errno, os.strerror(errno))
    return uid.value, gid.value


PROCESS_QUERY_TIMEOUT = 5
_PROCESS_PROBES = ThreadPoolExecutor(max_workers=2, thread_name_prefix='process-identity')
_PROCESS_PROBE_SLOTS = threading.BoundedSemaphore(2)


async def async_proc_start(pid):
    """Keep accepted OS probes bounded and off the service event loop."""
    if not _PROCESS_PROBE_SLOTS.acquire(blocking=False):
        raise BlockingIOError('process identity probe capacity reached')
    try:
        future = _PROCESS_PROBES.submit(proc_start, pid)
    except BaseException:
        _PROCESS_PROBE_SLOTS.release()
        raise
    # Release on actual completion, never merely because a caller stopped waiting.
    future.add_done_callback(lambda result: _PROCESS_PROBE_SLOTS.release())
    wrapped = asyncio.wrap_future(future)
    wrapped.add_done_callback(lambda result: None if result.cancelled() else result.exception())
    return await asyncio.shield(wrapped)


def proc_start(pid):
    """Local process-start marker for one pid, in this platform's own form.

    Used as a PID-reuse guard: a pid alone can be recycled, but the pair of pid
    and start time identifies one specific process. The value is also published
    in the peer registry, so it must use the form Claude Code expects on this
    platform.

    A pid that no longer exists raises `ProcessLookupError` on **both**
    platforms, so callers have one thing to catch when a process disappears
    mid-check rather than a platform-specific error each.

    @param pid - the process to describe.
    @returns the platform's start marker as a string.
    @throws ProcessLookupError when the process is gone.
    """
    if LINUX:
        try:
            return Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()[19]
        except FileNotFoundError as exc:
            raise ProcessLookupError(f'no such process: {pid}') from exc
    # `ps` reports local time by default while Claude writes UTC, so the zone is
    # forced rather than inherited. `lstart` uses asctime's space-padded day,
    # matching Claude's format; `normalize_start` absorbs any residual padding
    # difference so a comparison never fails on whitespace alone.
    try:
        result = subprocess.run(['ps', '-o', 'lstart=', '-p', str(pid)],
                                capture_output=True, text=True, check=True,
                                env={**os.environ, 'TZ': 'UTC'}, timeout=PROCESS_QUERY_TIMEOUT)
    except subprocess.CalledProcessError as exc:
        raise ProcessLookupError(f'no such process: {pid}') from exc
    value = result.stdout.strip()
    if not value:
        raise ProcessLookupError(f'no start time reported for pid {pid}')
    return value


def normalize_start(value):
    """Collapse whitespace so start markers compare independent of padding.

    asctime pads a single-digit day with a space (`Sep  3`); ps and Claude can
    disagree on that padding. Only whitespace is normalized — the remaining
    fields must still match exactly for the guard to hold.
    """
    return ' '.join(str(value).split())


def same_process(recorded, actual):
    """Whether a recorded start marker still describes the live process.

    A `None` record means the writer published no marker; the pid check alone
    then decides, which is the behavior older registry entries rely on.
    """
    return recorded is None or normalize_start(recorded) == normalize_start(actual)


def process_alive(pid):
    """Whether a pid is currently in use by a process this user may signal."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # Exists but belongs to another user; it cannot be our peer.
        return False
    return True


def pid_domain():
    """Peer-domain identifier recorded alongside this process in the registry.

    Linux peers disambiguate by machine id and pid namespace, so a pid is only
    meaningful within one of those. macOS Claude Code publishes the literal
    string `darwin` and performs no further namespacing.
    """
    if DARWIN:
        return 'darwin'
    return ('linux:' + Path('/etc/machine-id').read_text().strip() + ':'
            + os.readlink('/proc/self/ns/pid'))


def allowed_socket_dirs():
    """Directories a peer address may live in, canonicalized.

    These are the locations Claude Code uses for its peer sockets. macOS
    resolves `/tmp` to `/private/tmp`, so both forms are returned and callers
    compare resolved paths; the allowlist itself stays the literal, auditable
    list.
    """
    uid = os.getuid()
    dirs = [Path('/tmp/cc-socks'), Path(f'/tmp/cc-socks-{uid}')]
    if LINUX:
        dirs.append(Path(f'/run/user/{uid}/cc-socks'))
    resolved = set()
    for directory in dirs:
        resolved.add(directory.resolve())
    return resolved


def socket_mode_ok(info):
    """Whether a socket's metadata satisfies the private-socket requirement."""
    return stat.S_ISSOCK(info.st_mode) and info.st_uid == os.getuid() and not info.st_mode & 0o077


def control_socket_path(root):
    """Path for one instance's private control socket.

    AF_UNIX addresses are bounded by the kernel's `sun_path` field: 108 bytes on
    Linux and 104 on macOS, both including the terminating NUL, so the usable
    path is one byte shorter. An over-long `bind` fails with an opaque
    "AF_UNIX path too long" and the bridge simply never starts.

    The state directory can legitimately be deeper than that. A per-session
    state directory adds `sessions/<16 hex>`, and macOS puts temporary
    directories under a long `/var/folders/...` path, which is enough to exceed
    the limit. When the natural path does not fit, the socket is placed in the
    peer socket directory instead, under a name derived from the state
    directory, so it stays unique per instance and just as private: that
    directory is already required to be mode 0700 and owned by this user.

    The digest uses the **resolved** directory, so one state directory reached
    through two spellings (a symlinked parent, a relative path) still maps to one
    socket. A server and a later CLI invocation must agree on this path.

    The fallback lives in the shared socket directory rather than in the instance
    state directory, so cleanup does not own it. A path left behind by a killed
    instance is therefore removed by hand after verifying the old process is dead
    and its socket refuses connections, exactly as the project already requires for
    stale peer sockets. Nothing here removes a socket automatically: a listening
    socket and a saturated one are indistinguishable by probing, because a full
    accept queue refuses a connection on macOS just as a dead owner does.
    """
    root = Path(root).resolve()
    direct = root / 'control.sock'
    if len(os.fsencode(direct)) < SUN_PATH_BYTES[DARWIN]:
        return direct
    return fallback_control_socket(root)


def fallback_control_socket(root):
    """The stable private fallback used by both old and new service code."""
    digest = hashlib.sha256(str(Path(root).resolve()).encode()).hexdigest()[:16]
    return Path('/tmp/cc-socks') / f'{digest}-control.sock'


def legacy_control_socket(root, recorded=None):
    """A bounded old direct endpoint; never a peer or registry address.

    An old server could select a short alias before checking the path length.
    Only that root's direct socket is a permitted compatibility destination.
    """
    candidate = Path(root) / 'control.sock' if recorded is None else recorded
    if not isinstance(candidate, (str, Path)):
        return None
    candidate = Path(candidate)
    try:
        if (candidate.is_absolute() and len(os.fsencode(candidate)) < SUN_PATH_BYTES[DARWIN]
                and candidate.resolve() == (Path(root).resolve() / 'control.sock')):
            return candidate
    except (OSError, RuntimeError, ValueError):
        pass
    return None


def same_control_socket(recorded, expected):
    """Compare private service endpoint identities, never peer token paths."""
    if not isinstance(recorded, str) or not Path(recorded).is_absolute():
        return False
    try:
        return Path(recorded).resolve() == Path(expected).resolve()
    except (OSError, RuntimeError, ValueError):
        return False


def refuse_legacy_control_conflict(root):
    """Never start a new endpoint beside either retained legacy endpoint."""
    canonical = control_socket_path(root).resolve()
    candidates = (Path(root).resolve() / 'control.sock', fallback_control_socket(root))
    for candidate in candidates:
        if candidate.resolve() == canonical:
            continue
        try:
            candidate.lstat()
        except FileNotFoundError:
            continue
        raise FileExistsError(f'legacy control endpoint remains at {candidate}; '
                              'stop its owner and verify it is gone before removing a leftover socket')


class AccountHomeUnavailable(RuntimeError):
    """The effective OS account has no usable persistent home directory."""


def account_home():
    """Read the OS account entry, never HOME or a provider-specific override."""
    import pwd
    try:
        value = pwd.getpwuid(os.geteuid()).pw_dir
        if not value or not Path(value).is_absolute() or not Path(value).is_dir():
            raise AccountHomeUnavailable('account_home_unavailable')
        return Path(value)
    except (KeyError, OSError, ValueError):
        raise AccountHomeUnavailable('account_home_unavailable') from None


def participant_lock_dir():
    """Persistent singleton namespace shared by every state root of this account."""
    home = account_home()
    if DARWIN:
        return home / 'Library' / 'Application Support' / 'koinon-locks'
    return home / '.local' / 'state' / 'koinon-locks'


def sync_state_file(fd):
    """Flush one state file; request the stronger device flush on macOS.

    Fail visibly if the platform cannot honor the requested flush. Filesystem and
    device compliance remains an assumption, not a process-crash test result.
    """
    os.fsync(fd)
    if DARWIN:
        import fcntl
        operation = getattr(fcntl, 'F_FULLFSYNC', None)
        if operation is None:
            raise OSError('full state synchronization is unavailable')
        fcntl.fcntl(fd, operation)


def sync_state_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
