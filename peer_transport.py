"""Shared local socket primitives for bridge and memory services.

Protocol policy, request lifetimes, persistence and delivery stay with callers.
Messaging paths remain literal for peer-token lookup. Platform differences live
in platform_support.
"""
import asyncio
import hashlib
import json
import os
from pathlib import Path
import stat

import platform_support

LIMIT = 262144


def private_dir(path):
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    s = path.lstat()
    if not stat.S_ISDIR(s.st_mode) or s.st_uid != os.getuid() or s.st_mode & 0o077:
        raise ValueError('directory must be owned by this user and mode 0700')


def credentials(sock):
    """Kernel-verified peer pid for a connected socket, same-user only.

    The mechanism differs by platform (Linux `SO_PEERCRED`, macOS
    `getpeereid` plus `LOCAL_PEERPID`); see `platform_support.peer_pid`.
    """
    return platform_support.peer_pid(sock)


def target_path(address):
    """Validate a peer address and return it **unresolved**.

    Known control endpoint names are excluded even when a long control path falls
    back into a messaging directory. This is a messaging validator, not proof of
    a service binding. Service callers use service_path and a verified handshake.

    The returned path is the literal string from the wire. `peer_token` hashes
    it to find the sender's key file, and Claude Code hashes the same literal
    path, so resolving it here would break authentication silently: a resolved
    `/tmp/...` becomes `/private/tmp/...` on macOS and no key would be found.
    Only the allowlist comparison uses resolved paths, so the platform's `/tmp`
    symlink does not reject every peer.
    """
    if not isinstance(address, str) or not address.startswith('uds:'):
        raise ValueError('expected uds:/absolute/path')
    literal = address[4:]
    p = Path(literal)
    # The literal is what `peer_token` hashes, and what Claude hashed to name its
    # key file. An address that does not round-trip - a `.` or `..` component, a
    # doubled separator - could never match a published key, so the auth prelude
    # would be skipped silently. Reject it instead of normalizing it.
    if '..' in p.parts or str(p) != literal:
        raise ValueError('peer address must be in canonical literal form')
    if p.name == 'control.sock' or p.name.endswith('-control.sock'):
        raise ValueError('service control endpoint is not a messaging address')
    # A symlinked parent could redirect an allowlisted-looking path elsewhere,
    # so it is rejected before the resolved directory is checked.
    if p.suffix != '.sock' or p.parent.is_symlink() or p.parent.resolve() not in platform_support.allowed_socket_dirs():
        raise ValueError('unsupported or symlinked peer address')
    private_dir(p.parent)
    s = p.lstat()
    if not platform_support.socket_mode_ok(s):
        raise ValueError('peer socket must be private and owned by this user')
    return p


def encode(frame):
    data = json.dumps(frame, ensure_ascii=True).encode() + b'\n'
    if len(data) > LIMIT:
        raise ValueError('frame too large')
    return data


def peer_token(pid, path):
    folder = Path(os.environ.get('CLAUDE_CONFIG_DIR', str(Path.home() / '.claude'))) / 'sessions'
    key = folder / f'{pid}.{hashlib.sha256(str(path).encode()).hexdigest()}.key'
    try:
        fd = os.open(key, os.O_RDONLY | os.O_NOFOLLOW)
    except FileNotFoundError:
        return None
    with os.fdopen(fd) as f:
        s = os.fstat(f.fileno())
        if not stat.S_ISREG(s.st_mode) or s.st_uid != os.getuid() or s.st_mode & 0o077 or s.st_size > 4096:
            raise ValueError('unsafe peer key')
        token = json.load(f).get('peerToken')
    if not isinstance(token, str) or len(token) != 32 or any(c not in '0123456789abcdef' for c in token):
        raise ValueError('invalid peer key')
    return token




class UnsafeServiceEndpoint(ValueError):
    """The configured endpoint cannot be validated; it is not an absent service."""


def service_path(root):
    """Validate the control endpoint derived from an explicitly configured root.

    Service roots are filesystem configuration, not peer addresses. They are not
    discovered from peer registry claims and never use peer-token authentication.
    A valid socket path does not prove service identity; callers must also check
    the connected kernel PID and the service's owner/generation handshake.
    """
    root = Path(root)
    if not root.is_absolute():
        raise UnsafeServiceEndpoint('service root must be absolute')
    control = platform_support.control_socket_path(root)
    # A client must not create directories while a service is starting. In
    # particular, mkdir(parents=True) can create intermediate state directories
    # with the client umask before the owner applies its private-mode policy.
    try:
        info = control.parent.lstat()
    except FileNotFoundError:
        raise
    except OSError:
        raise UnsafeServiceEndpoint('service endpoint metadata cannot be read') from None
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or
            info.st_mode & 0o077):
        raise UnsafeServiceEndpoint('service directory must be private and owned by this user')
    try:
        socket_info = control.lstat()
    except FileNotFoundError:
        raise
    except OSError:
        raise UnsafeServiceEndpoint('service endpoint metadata cannot be read') from None
    if not platform_support.socket_mode_ok(socket_info):
        raise UnsafeServiceEndpoint('service socket must be private and owned by this user')
    return control


class NoControlReply(ValueError):
    """The service closed without a response; a mutation may still have committed."""


async def control_exchange(root, payload, timeout=10):
    """One bounded control exchange, returning the reply and kernel peer PID.

    The timeout covers connection, request writing and response reading. Cleanup
    gets its own one-second allowance, then aborts a stuck transport. No reply is
    not proof of rollback. The caller owns protocol and service identity checks.
    """
    path = service_path(root)
    data = encode(payload)
    writer = None
    try:
        async with asyncio.timeout(timeout):
            reader, writer = await asyncio.open_unix_connection(str(path), limit=LIMIT)
            pid = credentials(writer.get_extra_info('socket'))
            writer.write(data)
            await writer.drain()
            line = await reader.readline()
            if not line:
                raise NoControlReply('service closed without a reply')
            if len(line) > LIMIT:
                raise ValueError('frame too large')
            reply = json.loads(line)
            if not isinstance(reply, dict):
                raise ValueError('expected control response object')
            return reply, pid
    finally:
        if writer is not None:
            writer.close()
            try:
                await asyncio.wait_for(writer.wait_closed(), 1)
            except (OSError, TimeoutError):
                writer.transport.abort()
            except asyncio.CancelledError:
                writer.transport.abort()
                raise
