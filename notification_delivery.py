"""Serial provider calls, with durable reservation and independent worker status."""
import time


class DeliveryLoop:
    """A provider adapter returns delivered, failed, or unknown without error text.

    render and deliver are asynchronous callbacks. They must not perform blocking
    I/O on the event loop. The adapter owns its finite provider deadline and settles
    accepted subprocess work on cancellation before returning. No provider callback
    runs inside a source snapshot or journal transaction.
    """
    def __init__(self, worker, render, deliver, *, clock=time.time):
        self.worker, self.render, self.deliver = worker, render, deliver
        self.clock = clock
        self.busy = False
        self.started = False
        self.unresolved = False

    async def step(self):
        if self.busy:
            raise RuntimeError('concurrent notification delivery step')
        self.busy = True
        try:
            if not self.started or self.unresolved:
                await self.worker.call('ready', int(self.clock()))
                self.started, self.unresolved = True, False
            prepared = await self.worker.call('prepare', int(self.clock()))
            if not prepared['sequences']:
                return prepared
            # Set this before waiting: cancellation is not proof that an accepted
            # reservation failed to commit. Recovery consumes it on the next step.
            self.unresolved = True
            rows = await self.worker.call('reserve_next', int(self.clock()))
            if not rows:
                self.unresolved = False
                return await self.worker.call('status', priority=True)
            notice = await self.render(rows)
            outcome = await self.deliver(notice)
            result = await self.worker.call('resolve', outcome, int(self.clock()))
            self.unresolved = False
            return dict(admission=prepared['admission'], **result)
        finally:
            self.busy = False
