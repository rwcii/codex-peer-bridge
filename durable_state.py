"""Bounded private JSON publication under an already-held service ownership lock."""
import json
import os
from pathlib import Path
import stat

import platform_support

MAX_BYTES = 4096


class StateFileError(ValueError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def validate(info):
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
            or info.st_mode & 0o077 or info.st_nlink != 1 or info.st_size > MAX_BYTES):
        raise StateFileError('unsafe_state_file')


def pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            raise StateFileError('invalid_state_file')
        result[key] = value
    return result


def read(path):
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
    except FileNotFoundError:
        return None
    try:
        validate(os.fstat(fd))
        data = bytearray()
        while len(data) <= MAX_BYTES:
            chunk = os.read(fd, MAX_BYTES + 1 - len(data))
            if not chunk:
                break
            data.extend(chunk)
        if len(data) > MAX_BYTES:
            raise StateFileError('unsafe_state_file')
        try:
            value = json.loads(data, object_pairs_hook=pairs)
        except (ValueError, UnicodeError, RecursionError) as exc:
            raise StateFileError('invalid_state_file') from exc
        if not isinstance(value, dict):
            raise StateFileError('invalid_state_file')
        return value
    finally:
        os.close(fd)


def publish(path, value):
    """Publish or raise; an error after replacement has an unknown durable outcome.

    The caller serializes this with the notifier's state/participant ownership.
    A fixed replacement name bounds retained scratch files across process crashes.
    No caller may treat an exception as proof that the replacement did not occur.
    """
    path = Path(path)
    if not isinstance(value, dict):
        raise StateFileError('invalid_state_file')
    data = json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True).encode()
    if len(data) > MAX_BYTES:
        raise StateFileError('state_file_capacity')
    directory = path.parent.lstat()
    if (not stat.S_ISDIR(directory.st_mode) or directory.st_uid != os.geteuid()
            or directory.st_mode & 0o077):
        raise StateFileError('unsafe_state_directory')
    try:
        validate(path.lstat())
    except FileNotFoundError:
        pass
    temp = path.with_name(path.name + '.tmp')
    try:
        validate(temp.lstat())
    except FileNotFoundError:
        pass
    else:
        # Only this fixed, private replacement belongs to the held ownership lock.
        temp.unlink()
    fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    try:
        offset = 0
        while offset < len(data):
            written = os.write(fd, data[offset:])
            if written <= 0:
                raise OSError('state write made no progress')
            offset += written
        platform_support.sync_state_file(fd)
        os.replace(temp, path)
        platform_support.sync_state_directory(path.parent)
        # On macOS this also requests a device flush after directory metadata sync.
        platform_support.sync_state_file(fd)
    finally:
        os.close(fd)
        temp.unlink(missing_ok=True)
