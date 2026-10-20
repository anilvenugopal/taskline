import importlib.metadata

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import create_async_engine

from taskline.migrate import migrate

KIND_TABLE_ROWS = {
    "job_run_status_kind": 4,
    "task_status_kind": 8,
    "task_event_kind": 11,
    "job_trigger_kind": 3,
    "worker_status_kind": 3,
    "producer_kind": 0,
    "reference_kind": 0,
    "reference_relation_kind": 3,
    "schedule_overlap_kind": 2,
    "schedule_misfire_kind": 2,
    "schedule_fire_outcome_kind": 3,
}


def query(url: str, sql: str):
    engine = create_engine(url)
    try:
        with engine.connect() as conn:
            return conn.execute(text(sql)).all()
    finally:
        engine.dispose()


def test_applies_schema_to_empty_database(empty_database_url):
    assert migrate(empty_database_url) == ["0001_schema", "0002_instance", "0003_api_views"]

    tables = {
        row[0]
        for row in query(
            empty_database_url,
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'jobs' AND table_name LIKE '%\\_kind'",
        )
    }
    assert tables == set(KIND_TABLE_ROWS)

    for table, expected_rows in KIND_TABLE_ROWS.items():
        (count,) = query(empty_database_url, f"SELECT count(*) FROM jobs.{table}")[0]
        assert count == expected_rows, table


def test_second_run_applies_nothing(empty_database_url):
    migrate(empty_database_url)

    assert migrate(empty_database_url) == []

    versions = query(empty_database_url, "SELECT version FROM jobs._schema_version")
    assert sorted(row[0] for row in versions) == ["0001_schema", "0002_instance", "0003_api_views"]


def test_migrate_takes_an_engine_and_leaves_it_open(empty_database_url):
    engine = create_engine(empty_database_url)
    try:
        assert migrate(engine) == ["0001_schema", "0002_instance", "0003_api_views"]
        with engine.connect() as conn:  # still usable: migrate did not close it
            assert conn.execute(text("SELECT count(*) FROM jobs.instance")).scalar_one() == 1
    finally:
        engine.dispose()


def test_migrate_refuses_an_async_engine(empty_database_url):
    engine = create_async_engine(empty_database_url)
    with pytest.raises(TypeError, match="synchronous"):
        migrate(engine)


def test_the_instance_is_named_and_records_the_taskline_version(empty_database_url):
    migrate(empty_database_url, instance_name="strategiq-prod")

    ((name, recorded),) = query(
        empty_database_url, "SELECT name, taskline_version FROM jobs.instance"
    )
    assert name == "strategiq-prod"
    assert recorded == importlib.metadata.version("taskline")


def test_the_name_defaults_to_taskline_is_kept_and_can_be_changed(empty_database_url):
    migrate(empty_database_url)
    assert query(empty_database_url, "SELECT name FROM jobs.instance")[0][0] == "taskline"

    migrate(empty_database_url, instance_name="ops")
    migrate(empty_database_url)  # no name given: the recorded one is kept
    assert query(empty_database_url, "SELECT name FROM jobs.instance")[0][0] == "ops"
    assert query(empty_database_url, "SELECT count(*) FROM jobs.instance")[0][0] == 1


def test_every_code_column_references_its_kind_table(empty_database_url):
    migrate(empty_database_url)

    code_columns = query(
        empty_database_url,
        "SELECT table_name, column_name FROM information_schema.columns "
        "WHERE table_schema = 'jobs' AND column_name LIKE '%\\_code'",
    )
    foreign_keys = query(
        empty_database_url,
        "SELECT kcu.table_name, kcu.column_name, ccu.table_name "
        "FROM information_schema.key_column_usage kcu "
        "JOIN information_schema.referential_constraints rc "
        "  ON rc.constraint_name = kcu.constraint_name AND rc.constraint_schema = kcu.table_schema "
        "JOIN information_schema.constraint_column_usage ccu "
        "  ON ccu.constraint_name = rc.unique_constraint_name "
        "WHERE kcu.table_schema = 'jobs'",
    )
    references = {(table, column): target for table, column, target in foreign_keys}

    assert code_columns
    for table, column in code_columns:
        target = references.get((table, column))
        assert target is not None, f"{table}.{column} has no foreign key"
        assert target.endswith("_kind"), f"{table}.{column} points at {target}"
        assert column.startswith(target.removesuffix("_kind")), f"{table}.{column} points at {target}"
