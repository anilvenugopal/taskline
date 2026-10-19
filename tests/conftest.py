import uuid

import pytest
from redis import Redis
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from taskline.config import get_config
from taskline.migrate import migrate
from taskline.runtime import reset_runtime
from taskline.store import Store


@pytest.fixture
def empty_database_url():
    """URL of a new, empty database on the server named in .env; dropped after the test."""
    server_url = make_url(get_config().database_url)
    name = f"taskline_test_{uuid.uuid4().hex[:12]}"
    admin = create_engine(server_url, isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        conn.execute(text(f'CREATE DATABASE "{name}"'))
    try:
        yield server_url.set(database=name).render_as_string(hide_password=False)
    finally:
        with admin.connect() as conn:
            conn.execute(text(f'DROP DATABASE "{name}" WITH (FORCE)'))
        admin.dispose()


@pytest.fixture
def runtime(monkeypatch, empty_database_url):
    """A fresh default broker and store: the test database, a unique Valkey namespace, queue work.

    Yields the namespace and removes its Valkey keys afterwards.
    """
    namespace = f"t{uuid.uuid4().hex[:8]}"
    monkeypatch.setenv("TASKLINE_DATABASE_URL", empty_database_url)
    monkeypatch.setenv("TASKLINE_NAMESPACE", namespace)
    monkeypatch.setenv("TASKLINE_CONSUME_QUEUE", "work")
    get_config.cache_clear()
    reset_runtime()
    yield namespace
    reset_runtime()
    client = Redis.from_url(get_config().valkey_url)
    keys = list(client.scan_iter(match=f"{namespace}:*"))
    if keys:
        client.delete(*keys)
    client.close()
    get_config.cache_clear()


@pytest.fixture
async def store(empty_database_url):
    """A Store on a migrated database with a producer and two reference kinds registered."""
    migrate(empty_database_url)
    engine = create_engine(empty_database_url)
    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO jobs.producer_kind (code, name) VALUES ('test.producer', 'Test producer')")
        )
        conn.execute(
            text(
                "INSERT INTO jobs.reference_kind (code, name) VALUES "
                "('document', 'Document'), ('source', 'Source')"
            )
        )
    engine.dispose()

    store = Store(empty_database_url)
    yield store
    await store.close()
