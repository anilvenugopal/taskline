import pytest
from helpers import make_plan, rows
from sqlalchemy import create_engine, text

from taskline.errors import NotMigrated
from taskline.migrate import migrate
from taskline.setup import (
    Kind,
    SchemaVersion,
    register_producers,
    register_reference_kinds,
    schema_version,
)
from taskline.store import ReferencePlan, Store


@pytest.fixture
def migrated_url(empty_database_url):
    migrate(empty_database_url)
    return empty_database_url


def codes(url, table):
    return {r[0] for r in rows(url, f"SELECT code FROM jobs.{table}")}


def test_new_producers_are_added_and_reported(migrated_url):
    added = register_producers(
        migrated_url, [Kind("myapp.ingest", "Ingest"), Kind("myapp.api", "API")]
    )

    assert added == ["myapp.ingest", "myapp.api"]
    assert codes(migrated_url, "producer_kind") == {"myapp.ingest", "myapp.api"}


def test_registering_again_changes_nothing_and_reports_nothing_new(migrated_url):
    kinds = [Kind("myapp.ingest", "Ingest", "Fetches documents")]
    register_producers(migrated_url, kinds)

    assert register_producers(migrated_url, kinds) == []
    assert rows(migrated_url, "SELECT name, description FROM jobs.producer_kind") == [
        ("Ingest", "Fetches documents")
    ]


def test_an_existing_code_gets_the_new_name_and_description(migrated_url):
    register_producers(migrated_url, [Kind("myapp.ingest", "Ingest", "old")])
    register_producers(migrated_url, [Kind("myapp.ingest", "Document ingest")])

    assert rows(migrated_url, "SELECT name, description FROM jobs.producer_kind") == [
        ("Document ingest", None)
    ]


def test_codes_not_in_the_list_are_left_alone(migrated_url):
    register_producers(migrated_url, [Kind("a.one", "One"), Kind("a.two", "Two")])
    register_producers(migrated_url, [Kind("a.one", "One")])

    assert codes(migrated_url, "producer_kind") == {"a.one", "a.two"}


def test_reference_kinds_are_registered_in_their_own_table(migrated_url):
    register_reference_kinds(migrated_url, [Kind("strategiq.document", "Document")])

    assert codes(migrated_url, "reference_kind") == {"strategiq.document"}
    assert codes(migrated_url, "producer_kind") == set()


@pytest.mark.parametrize(
    "kinds",
    [
        [Kind("", "Empty")],
        [Kind("has space", "Spaces")],
        [Kind(" padded", "Padded")],
        [Kind("a.b", "  ")],
        [Kind("a.b", "One"), Kind("a.b", "Again")],
    ],
)
def test_an_unusable_list_is_refused_before_anything_is_written(migrated_url, kinds):
    with pytest.raises(ValueError):
        register_producers(migrated_url, [Kind("fine.one", "Fine"), *kinds])

    assert codes(migrated_url, "producer_kind") == set()


async def test_registered_kinds_are_accepted_when_a_run_is_recorded(migrated_url):
    register_producers(migrated_url, [Kind("myapp.ingest", "Ingest")])
    register_reference_kinds(migrated_url, [Kind("strategiq.document", "Document")])
    store = Store(migrated_url)
    try:
        await store.register_run(
            make_plan(
                producer_code="myapp.ingest",
                references=[ReferencePlan("strategiq.document", "doc-1", "subject")],
            )
        )
    finally:
        await store.close()

    assert rows(migrated_url, "SELECT count(*) FROM jobs.job_reference")[0][0] == 1


def test_schema_version_reports_what_last_migrated_the_database(empty_database_url):
    migrate(empty_database_url, instance_name="strategiq-prod")

    found = schema_version(empty_database_url)

    assert found == SchemaVersion(
        taskline_version=found.taskline_version,
        schema_version="0003_api_views",
        instance_name="strategiq-prod",
    )
    assert found.taskline_version


def test_schema_version_takes_an_engine_and_leaves_it_open(empty_database_url):
    migrate(empty_database_url)
    engine = create_engine(empty_database_url)
    try:
        schema_version(engine)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    finally:
        engine.dispose()


def test_a_database_never_migrated_raises_not_migrated(empty_database_url):
    with pytest.raises(NotMigrated, match="taskline migrate"):
        schema_version(empty_database_url)


def test_a_database_without_the_instance_table_raises_not_migrated(migrated_url):
    engine = create_engine(migrated_url)
    try:
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE jobs.instance CASCADE"))  # the instance view depends on it
    finally:
        engine.dispose()

    with pytest.raises(NotMigrated):
        schema_version(migrated_url)
