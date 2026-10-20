from collections.abc import Sequence
from dataclasses import dataclass

from sqlalchemy import Engine, text

from taskline.engines import sync_engine
from taskline.errors import NotMigrated

__all__ = [  # the names docs/public_contract.md lists
    "Kind",
    "NotMigrated",
    "SchemaVersion",
    "register_producers",
    "register_reference_kinds",
    "schema_version",
]


@dataclass(frozen=True)
class Kind:
    """A producer or reference kind a host registers: a code such as 'strategiq.document'."""

    code: str
    name: str
    description: str | None = None


@dataclass(frozen=True)
class SchemaVersion:
    taskline_version: str  # the Taskline version that last ran migrate on the database
    schema_version: str  # the latest migration file applied, for example '0002_instance'
    instance_name: str


def register_producers(url_or_engine: str | Engine, producers: Sequence[Kind]) -> list[str]:
    """Make the producer kinds match the list given; returns the codes that were new.

    Existing codes get the given name and description. Codes not in the list are never
    changed or deleted, because runs refer to them. Safe to call on every start.
    """
    return _register("producer_kind", url_or_engine, producers)


def register_reference_kinds(url_or_engine: str | Engine, kinds: Sequence[Kind]) -> list[str]:
    """Like register_producers, for the kinds of host objects a run can refer to."""
    return _register("reference_kind", url_or_engine, kinds)


def schema_version(url_or_engine: str | Engine) -> SchemaVersion:
    """Which Taskline last migrated this database. Raises NotMigrated if it never was."""
    with sync_engine(url_or_engine) as engine, engine.connect() as conn:
        if not conn.execute(text("SELECT to_regclass('jobs.instance') IS NOT NULL")).scalar_one():
            raise NotMigrated("the jobs schema is missing or too old; run `taskline migrate`")
        row = conn.execute(
            text(
                "SELECT name, taskline_version, "
                "(SELECT max(version) FROM jobs._schema_version) AS schema_version "
                "FROM jobs.instance"
            )
        ).first()
    if row is None:
        raise NotMigrated("the database was never migrated; run `taskline migrate`")
    return SchemaVersion(row.taskline_version, row.schema_version, row.name)


def _register(table: str, url_or_engine: str | Engine, kinds: Sequence[Kind]) -> list[str]:
    codes = [kind.code for kind in kinds]
    for kind in kinds:
        if not kind.code or kind.code != kind.code.strip() or " " in kind.code:
            raise ValueError(f"{kind.code!r} is not a usable code: no spaces, not empty")
        if not kind.name.strip():
            raise ValueError(f"{kind.code!r} needs a name")
    if len(set(codes)) != len(codes):
        raise ValueError("a code appears twice in the list")

    with sync_engine(url_or_engine) as engine, engine.begin() as conn:
        existing = {
            row[0]
            for row in conn.execute(
                text(f"SELECT code FROM jobs.{table} WHERE code = ANY(:codes)"), {"codes": codes}
            )
        }
        for kind in kinds:
            conn.execute(
                text(
                    f"INSERT INTO jobs.{table} (code, name, description) "
                    "VALUES (:code, :name, :description) "
                    "ON CONFLICT (code) DO UPDATE SET "
                    "name = EXCLUDED.name, description = EXCLUDED.description"
                ),
                {"code": kind.code, "name": kind.name, "description": kind.description},
            )
    return [code for code in codes if code not in existing]
