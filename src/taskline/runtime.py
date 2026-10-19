import asyncio
import threading

from taskline.broker import TasklineBroker, build_broker
from taskline.config import get_config
from taskline.store import Store

# Reentrant: get_broker takes the lock and then calls get_store, which takes it again.
_lock = threading.RLock()
_store: Store | None = None
_broker: TasklineBroker | None = None
_started = False
_start_lock = asyncio.Lock()


def get_store() -> Store:
    """The process's one Store, created from the settings the first time it is asked for."""
    global _store
    with _lock:
        if _store is None:
            _store = Store(get_config().database_url)
        return _store


def get_broker() -> TasklineBroker:
    """The process's one broker, created from the settings the first time it is asked for."""
    global _broker
    with _lock:
        if _broker is None:
            _broker = build_broker(get_store())
        return _broker


async def start_runtime() -> None:
    """Start the process's broker if it has not been started yet. Safe to call repeatedly."""
    global _started
    broker = get_broker()
    async with _start_lock:
        if not _started:
            await broker.startup()
            _started = True


async def shutdown_runtime() -> None:
    """Stop the broker and close the database pool, then forget both."""
    broker, store, started = _broker, _store, _started
    if broker is not None and started:
        await broker.shutdown()
    if store is not None:
        await store.close()
    reset_runtime()


def reset_runtime() -> None:
    """Forget the broker and the store so the next call builds new ones. For tests only."""
    global _store, _broker, _started, _start_lock
    with _lock:
        _store = None
        _broker = None
        _started = False
        _start_lock = asyncio.Lock()
