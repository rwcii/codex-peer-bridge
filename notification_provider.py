"""Asynchronous bounded provider process with conservative delivery outcomes."""
import argparse
import asyncio
import os
from pathlib import Path
import signal
import sys

import dsh_delivery

PROVIDER_TIMEOUT = 15
CHILD_REFUSED = 65
CHILD_INTERNAL = 70
# Two bound paths, an inbox path and two installed script paths can each need
# shell quoting expansion. Keep one notice below 128 KiB; platform argv refusals
# remain explicit provider failures.
MAX_NOTICE_BYTES = 120 * 1024


async def _settle(task):
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            continue
    return task.result()


async def _terminate(process, completion):
    # This process group was created only for this provider invocation. Terminate
    # its synchronous descendants too; a handed-off request can still be accepted.
    if process.returncode is None:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    try:
        await _settle(completion)
    except OSError:
        # A closed provider pipe does not prevent process reaping.
        await _settle(asyncio.create_task(process.wait()))


class Provider:
    def __init__(self, options, *, timeout=PROVIDER_TIMEOUT):
        self.options, self.timeout = options, timeout
        self.active = False

    async def deliver(self, text):
        if self.active:
            raise RuntimeError('provider invocation already active')
        if not isinstance(text, str) or len(text.encode('utf-8')) > MAX_NOTICE_BYTES:
            raise ValueError('invalid content-free notice size')
        a = self.options
        if a.agent == 'codex':
            argv = [a.codex, 'queue', '--thread', a.thread, '--message', text]
            payload = None
        elif a.agent == 'deepseek':
            argv = [sys.executable, str(Path(__file__).resolve()), '--deepseek-child',
                    '--url', a.dsh_url, '--session', a.thread]
            if a.dsh_credentials is not None:
                argv.extend(['--credentials', str(a.dsh_credentials)])
            payload = text.encode('utf-8')
        else:
            raise ValueError('unknown participant provider')
        self.active = True
        try:
            try:
                creation = asyncio.create_task(asyncio.create_subprocess_exec(*argv,
                    stdin=asyncio.subprocess.PIPE if payload is not None else asyncio.subprocess.DEVNULL,
                    stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
                    start_new_session=True))
                try:
                    process = await asyncio.shield(creation)
                except asyncio.CancelledError:
                    # Cancellation can arrive after spawn but before the process
                    # object is returned. Settle creation before releasing ownership.
                    try:
                        process = await _settle(creation)
                    except OSError:
                        raise asyncio.CancelledError from None
                    await _terminate(process, asyncio.create_task(process.communicate(payload)))
                    raise
            except OSError:
                # No provider process was started, so no request was submitted.
                return 'failed'
            completion = asyncio.create_task(process.communicate(payload))
            try:
                await asyncio.wait_for(asyncio.shield(completion), self.timeout)
            except TimeoutError:
                await _terminate(process, completion)
                return 'unknown'
            except asyncio.CancelledError:
                await _terminate(process, completion)
                raise
            except OSError:
                await _terminate(process, completion)
                return 'unknown'
            if a.agent == 'deepseek':
                if process.returncode == CHILD_REFUSED:
                    return 'failed'
                if process.returncode == CHILD_INTERNAL:
                    raise RuntimeError('DeepSeek adapter internal failure')
            # Other nonzero exits are not proof that remote enqueue failed.
            return 'delivered' if process.returncode == 0 else 'unknown'
        finally:
            self.active = False


def deepseek_child(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--deepseek-child', action='store_true', required=True)
    parser.add_argument('--url', required=True)
    parser.add_argument('--session', required=True)
    parser.add_argument('--credentials', type=Path)
    options = parser.parse_args(argv)
    raw = sys.stdin.buffer.read(MAX_NOTICE_BYTES + 1)
    if len(raw) > MAX_NOTICE_BYTES:
        return CHILD_REFUSED
    try:
        text = raw.decode('utf-8')
    except UnicodeError:
        return CHILD_REFUSED
    try:
        dsh_delivery.deliver(options.url, options.session, text,
                             credentials=options.credentials, timeout=PROVIDER_TIMEOUT)
    except (dsh_delivery.DeliveryError, OSError):
        # Raw errors can include destination or credential paths. The parent needs
        # only a conservative outcome, never provider response text.
        return 1
    except Exception:
        return CHILD_INTERNAL
    return 0


if __name__ == '__main__':
    raise SystemExit(deepseek_child())
