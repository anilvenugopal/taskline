from sqlalchemy import create_engine, text

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
    assert migrate(empty_database_url) == ["0001_schema"]

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
    assert [row[0] for row in versions] == ["0001_schema"]


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
