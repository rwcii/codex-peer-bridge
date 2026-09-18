"""Event-loop-owned service admission and socket shutdown."""
import asyncio
from database_worker import CapacityError, WorkerClosed, WorkerFailure

HANDSHAKE_TIMEOUT = 2
STATUS_TIMEOUT = 1


class Admission:
    def __init__(self):
        self.counts = dict(handshake=0, ordinary=0, control=0)
        self.limits = dict(handshake=8, ordinary=16, control=2)

    def enter(self, kind):
        if self.counts[kind] >= self.limits[kind]:
            raise CapacityError('service request capacity reached')
        self.counts[kind] += 1
        return kind

    def leave(self, kind):
        self.counts[kind] -= 1


async def close_writer(writer):
    writer.close()
    try:
        await asyncio.wait_for(writer.wait_closed(), 1)
    except (OSError, TimeoutError):
        writer.transport.abort()
    except asyncio.CancelledError:
        writer.transport.abort()
        raise


async def drain_handlers(tasks):
    # Cancelling a handler stops waiting for its answer, not its accepted mutation.
    # The owning worker is drained separately after these socket tasks settle.
    if not tasks:
        return
    pending = set(tasks)
    try:
        await asyncio.wait_for(asyncio.gather(*pending, return_exceptions=True), 10)
    except TimeoutError:
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)


async def database_status(worker, *args):
    """A diagnostic must answer even when the subsystem being diagnosed is stuck."""
    result = None
    try:
        async with asyncio.timeout(STATUS_TIMEOUT):
            result = await worker.call('command', *args, priority=True)
        state = 'ready'
    except TimeoutError:
        state = 'busy'
    except CapacityError:
        state = 'capacity'
    except WorkerClosed:
        state = 'closing'
    except WorkerFailure as exc:
        state = exc.code
    snapshot = worker.snapshot()
    diagnostics = dict(database_status=state, database_observed_fault=snapshot.pop('last_fault'),
                       database_worker=snapshot)
    return result, diagnostics
