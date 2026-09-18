"""Shared local socket primitives for bridge and memory services.

Protocol policy, request lifetimes, persistence and delivery stay with callers.
Messaging paths remain literal for peer-token lookup. Platform differences live
in platform_support.
"""
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

    This applies the existing messaging-address path checks, not service-endpoint
    validation. Do not use it to validate a service control endpoint or to prove
    an endpoint's role: a long control path can fall back to an allowed directory
    as `<digest>-control.sock` and pass these structural checks. Service callers
    need their own endpoint validation and binding checks.

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


