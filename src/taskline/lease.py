import asyncio
import logging
import threading
import time
import uuid
from collections.abc import Awaitable, Callable
from typing import Protocol

from taskline.store import HeartbeatResult

log = logging.getLogger(__name__)


class HeartbeatStore(Protocol):
    async def heartbeat(
        self,
        task_instance_id: uuid.UUID,
        attempt: int,
        lease_seconds: float,
        progress_pct: float | None = None,
        progress_note: str | None = None,
    ) -> HeartbeatResult: ...


class Lease:
    """Keeps one running attempt's lease alive and watches for cancel and for lost ownership.

    The lease does not stop tasks itself. It sets flags and calls the callbacks it is given;
    the caller decides what stopping means for each kind of task.
    """

    def __init__(
        self,
        store: HeartbeatStore,
        task_instance_id: uuid.UUID,
        attempt: int,
        *,
        heartbeat_interval: float,
        lease_seconds: float,
        on_cancel: Callable[[], None] | None = None,
        on_lost: Callable[[], None] | None = None,
        cancel_grace_seconds: float | None = None,
        on_grace_expired: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self._store = store
        self._task_instance_id = task_instance_id
        self._attempt = attempt
        self._interval = heartbeat_interval
        self._lease_seconds = lease_seconds
        self._on_cancel = on_cancel
        self._on_lost = on_lost
        self._grace_seconds = cancel_grace_seconds
        self._on_grace_expired = on_grace_expired
        self._cancel_requested = threading.Event()
        self._lost = threading.Event()
        self._progress_pct: float | None = None
        self._progress_note: str | None = None
        self._loop_task: asyncio.Task | None = None
        self._grace_task: asyncio.Task | None = None

    @property
    def cancel_requested(self) -> bool:
        return self._cancel_requested.is_set()

    @property
    def lost(self) -> bool:
        return self._lost.is_set()

    def set_progress(self, pct: float | None = None, note: str | None = None) -> None:
        """Record progress; it is sent with the next heartbeat."""
        if pct is not None:
            self._progress_pct = pct
        if note is not None:
            self._progress_note = note

    def start(self) -> None:
        self._loop_task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        for task in (self._loop_task, self._grace_task):
            if task is not None:
                task.cancel()
        for task in (self._loop_task, self._grace_task):
            if task is not None:
                await asyncio.gather(task, return_exceptions=True)

    async def _run(self) -> None:
        last_success = time.monotonic()
        while True:
            await asyncio.sleep(self._interval)
            pct, note = self._progress_pct, self._progress_note
            self._progress_pct = self._progress_note = None
            try:
                async with asyncio.timeout(self._interval):
                    result = await self._store.heartbeat(
                        self._task_instance_id, self._attempt, self._lease_seconds, pct, note
                    )
            except Exception:
                log.exception("heartbeat failed for task %s", self._task_instance_id)
                self._progress_pct = self._progress_pct if self._progress_pct is not None else pct
                self._progress_note = self._progress_note if self._progress_note is not None else note
                if time.monotonic() - last_success >= self._lease_seconds:
                    self._mark_lost()
                    return
                continue

            last_success = time.monotonic()
            if not result.held:
                self._mark_lost()
                return
            if result.cancel_requested and not self._cancel_requested.is_set():
                self._cancel_requested.set()
                self._call(self._on_cancel)
                self._start_grace_timer()

    def _mark_lost(self) -> None:
        self._lost.set()
        self._call(self._on_lost)

    def _start_grace_timer(self) -> None:
        if self._grace_seconds is not None and self._on_grace_expired is not None:
            self._grace_task = asyncio.create_task(self._grace())

    async def _grace(self) -> None:
        await asyncio.sleep(self._grace_seconds)
        await self._on_grace_expired()

    @staticmethod
    def _call(callback: Callable[[], None] | None) -> None:
        if callback is None:
            return
        try:
            callback()
        except Exception:
            log.exception("lease callback failed")
