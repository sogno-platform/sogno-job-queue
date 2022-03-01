import asyncio


class DoOnce(asyncio.locks._ContextManagerMixin):
    """Context that works like a lock but also allows to wait in a different place
    (e.g. before a health check) when lock is inplace (e.g. because health is being restored).
    """

    def __init__(self, lock=None, event=None):
        if lock is None:
            lock = asyncio.Lock()
        self._lock = lock
        if event is None:
            event = asyncio.Event()
        self._event = event
        self.acquire = lock.acquire
        self.locked = lock.locked
        self.wait = event.wait
        self.is_set = event.is_set

    async def wait_if_locked(self):
        """Wait here if the self is currently locked."""
        if self.locked():
            await self.wait()

    def release(self):
        self._lock.release()
        self._event.set()
