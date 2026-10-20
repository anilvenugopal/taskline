"""A database role granted by `migrate(reader_role=...)` can read the views and nothing in jobs."""

import uuid

import pytest
from helpers import rows, taskline
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ProgrammingError

from taskline.migrate import migrate

PASSWORD = "reader"


@pytest.fixture
def reader_role(empty_database_url):
    """A new login role that has no rights; removed, with any grants it was given, afterwards."""
    role = f"taskline_reader_{uuid.uuid4().hex[:8]}"
    admin = create_engine(empty_database_url, isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        conn.execute(text(f"CREATE ROLE {role} LOGIN PASSWORD '{PASSWORD}'"))
    yield role
    with admin.connect() as conn:
        conn.execute(text(f"DROP OWNED BY {role}"))
        conn.execute(text(f"DROP ROLE {role}"))
    admin.dispose()


def read_as(role, url, sql):
    engine = create_engine(make_url(url).set(username=role, password=PASSWORD))
    try:
        with engine.connect() as conn:
            return conn.execute(text(sql)).scalar_one()
    finally:
        engine.dispose()


def test_the_reader_role_can_read_the_views(empty_database_url, reader_role):
    migrate(empty_database_url, reader_role=reader_role)

    assert read_as(reader_role, empty_database_url, "SELECT count(*) FROM taskline_api.run") == 0
    assert read_as(reader_role, empty_database_url, "SELECT count(*) FROM taskline_api.instance") == 1


def test_the_reader_role_cannot_read_anything_in_jobs(empty_database_url, reader_role):
    migrate(empty_database_url, reader_role=reader_role)

    with pytest.raises(ProgrammingError, match="permission denied"):
        read_as(reader_role, empty_database_url, "SELECT count(*) FROM jobs.job_run")


def test_migrating_again_keeps_the_grants(empty_database_url, reader_role):
    migrate(empty_database_url, reader_role=reader_role)

    assert migrate(empty_database_url, reader_role=reader_role) == []
    assert migrate(empty_database_url) == []  # no role named this time: earlier grants stay
    assert read_as(reader_role, empty_database_url, "SELECT count(*) FROM taskline_api.task") == 0


def test_an_unknown_reader_role_is_refused_and_nothing_is_applied(empty_database_url):
    with pytest.raises(ValueError, match="no_such_role_anywhere"):
        migrate(empty_database_url, reader_role="no_such_role_anywhere")

    assert rows(empty_database_url, "SELECT to_regclass('jobs._schema_version')")[0][0] is None


def test_the_migrate_command_grants_the_role_named_in_the_settings(
    runtime, reader_role, monkeypatch, empty_database_url
):
    monkeypatch.setenv("TASKLINE_API_READER_ROLE", reader_role)

    result = taskline("migrate")

    assert result.returncode == 0, result.stderr
    assert read_as(reader_role, empty_database_url, "SELECT count(*) FROM taskline_api.event") == 0
