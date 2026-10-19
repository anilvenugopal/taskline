from importlib.resources import files

from sqlalchemy import create_engine, text

from taskline.config import get_config

CREATE_VERSION_TABLE = """
CREATE TABLE IF NOT EXISTS jobs._schema_version (
  version    TEXT PRIMARY KEY,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""


def migrate(database_url: str | None = None) -> list[str]:
    """Apply the SQL files in taskline/sql that are not yet recorded, in name order.

    All pending files run in one transaction, so a failure leaves the database
    unchanged. Returns the versions applied, empty when nothing was pending.
    """
    engine = create_engine(database_url or get_config().database_url)
    applied: list[str] = []
    try:
        with engine.begin() as conn:
            conn.execute(text("SELECT pg_advisory_xact_lock(hashtext('taskline.migrate'))"))
            conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS jobs")
            conn.exec_driver_sql(CREATE_VERSION_TABLE)
            done = {row[0] for row in conn.execute(text("SELECT version FROM jobs._schema_version"))}
            for path in sorted(files("taskline").joinpath("sql").iterdir(), key=lambda p: p.name):
                if not path.name.endswith(".sql"):
                    continue
                version = path.name.removesuffix(".sql")
                if version in done:
                    continue
                conn.exec_driver_sql(path.read_text())
                conn.execute(
                    text("INSERT INTO jobs._schema_version (version) VALUES (:v)"), {"v": version}
                )
                applied.append(version)
    finally:
        engine.dispose()
    return applied
