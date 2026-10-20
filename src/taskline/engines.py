from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import Engine, create_engine


@contextmanager
def sync_engine(url_or_engine: str | Engine) -> Iterator[Engine]:
    """The engine to use: the one given, which stays open, or a new one for a URL, closed afterwards."""
    if isinstance(url_or_engine, Engine):
        yield url_or_engine
        return
    engine = create_engine(url_or_engine)
    try:
        yield engine
    finally:
        engine.dispose()
