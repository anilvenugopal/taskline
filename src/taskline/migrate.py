from importlib.metadata import PackageNotFoundError, version
from importlib.resources import files

from sqlalchemy import Connection, Engine, text
from sqlalchemy.ext.asyncio import AsyncEngine

from taskline.config import get_config
from taskline.engines import sync_engine

__all__ = ["migrate"]  # the names docs/public_contract.md lists

DEFAULT_INSTANCE_NAME = "taskline"

CREATE_VERSION_TABLE = """
CREATE TABLE IF NOT EXISTS jobs._schema_version (
  version    TEXT PRIMARY KEY,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""


def installed_version() -> str:
    """The version of the installed taskline package."""
    try:
        return version("taskline")
    except PackageNotFoundError:
        return "0+unknown"


def migrate(
    url_or_engine: str | Engine | None = None,
    *,
    instance_name: str | None = None,
    reader_role: str | None = None,
) -> list[str]:
    """Apply the SQL files in taskline/sql that are not yet recorded, in name order.

    Takes a database URL, or a synchronous SQLAlchemy engine, which is left open; with
    neither it uses the settings. All pending files run in one transaction, so a failure
    leaves the database unchanged. It then records the instance's name (the one given, else
    the one already recorded, else "taskline") and this Taskline's version in jobs.instance.
    With reader_role, that database role is granted read access to the taskline_api views and
    nothing else. The role must already exist; migrate never creates one. The grants are
    repeated on every run, so views added by a later migration are covered.
    Returns the versions applied, empty when nothing was pending.
    """
    if isinstance(url_or_engine, AsyncEngine):
        raise TypeError("migrate needs a database URL or a synchronous engine, not an AsyncEngine")
    target = get_config().database_url if url_or_engine is None else url_or_engine

    applied: list[str] = []
    with sync_engine(target) as engine, engine.begin() as conn:
        conn.execute(text("SELECT pg_advisory_xact_lock(hashtext('taskline.migrate'))"))
        conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS jobs")
        conn.exec_driver_sql(CREATE_VERSION_TABLE)
        done = {row[0] for row in conn.execute(text("SELECT version FROM jobs._schema_version"))}
        for path in sorted(files("taskline").joinpath("sql").iterdir(), key=lambda p: p.name):
            if not path.name.endswith(".sql"):
                continue
            file_version = path.name.removesuffix(".sql")
            if file_version in done:
                continue
            conn.exec_driver_sql(path.read_text())
            conn.execute(
                text("INSERT INTO jobs._schema_version (version) VALUES (:v)"), {"v": file_version}
            )
            applied.append(file_version)
        _record_instance(conn, instance_name)
        if reader_role is not None:
            _grant_reader(conn, reader_role)
    return applied


def _grant_reader(conn: Connection, role: str) -> None:
    if conn.execute(text("SELECT 1 FROM pg_roles WHERE rolname = :role"), {"role": role}).first() is None:
        raise ValueError(
            f"the database role {role!r} does not exist; create it first, Taskline never creates roles"
        )
    quoted = conn.dialect.identifier_preparer.quote(role)
    conn.exec_driver_sql(f"GRANT USAGE ON SCHEMA taskline_api TO {quoted}")
    conn.exec_driver_sql(f"GRANT SELECT ON ALL TABLES IN SCHEMA taskline_api TO {quoted}")


def _record_instance(conn: Connection, instance_name: str | None) -> None:
    conn.execute(
        text(
            "INSERT INTO jobs.instance (name, taskline_version) VALUES (:name, :version) "
            "ON CONFLICT (instance_id) DO UPDATE SET "
            "name = COALESCE(CAST(:given AS text), jobs.instance.name), "
            "taskline_version = EXCLUDED.taskline_version, updated_at = now()"
        ),
        {
            "name": instance_name or DEFAULT_INSTANCE_NAME,
            "given": instance_name,
            "version": installed_version(),
        },
    )
