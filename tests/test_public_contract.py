"""docs/public_contract.md is the contract. This test reads it and fails when something it lists
is gone or no longer fits. A heading marked [planned: step N] is not built yet and is skipped."""

import dataclasses
import importlib
import inspect
from pathlib import Path

from helpers import rows
from sqlalchemy import create_engine, text

import taskline
from taskline.migrate import migrate

CONTRACT = Path(__file__).parent.parent / "docs" / "public_contract.md"

# These sections must stay in the document, so deleting one cannot make the test pass.
REQUIRED_MODULES = {
    "taskline",
    "taskline.api",
    "taskline.catalogue",
    "taskline.migrate",
    "taskline.setup",
    "taskline.testing",
}
REQUIRED_VIEWS = {"run", "task", "reference", "event", "queue", "worker", "instance"}

# Status sets that are kind tables. "worker role" is not one: it is checked against the worker view.
KIND_TABLES = {
    "run status": "job_run_status_kind",
    "task status": "task_status_kind",
    "worker status": "worker_status_kind",
    "reference relation": "reference_relation_kind",
    "run trigger": "job_trigger_kind",
    "event": "task_event_kind",
}
NOT_KIND_TABLES = {"worker role"}

KINDS = {
    "class": inspect.isclass,
    "exception": lambda obj: inspect.isclass(obj) and issubclass(obj, BaseException),
    "async function": inspect.iscoroutinefunction,
    "function": lambda obj: callable(obj) and not inspect.iscoroutinefunction(obj),
    "function (sync)": lambda obj: callable(obj) and not inspect.iscoroutinefunction(obj),
    "decorator": callable,
    "registry": lambda obj: obj is not None,
    "constant": lambda obj: obj is not None,
}


def read_sections(markdown: str) -> dict[str, list[dict[str, str]]]:
    """Each heading that has a table, with the table's rows as dictionaries."""
    sections: dict[str, list[dict[str, str]]] = {}
    heading, header = None, None
    for line in markdown.splitlines():
        if line.startswith("#"):
            heading, header = line.lstrip("#").strip(), None
        elif line.startswith("|") and heading is not None:
            cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
            if header is None:
                header = cells
                sections[heading] = []
            elif not set("".join(cells)) <= set("-: "):
                sections[heading].append(dict(zip(header, cells, strict=False)))
    return sections


def python_sections(
    sections: dict[str, list[dict[str, str]]],
) -> list[tuple[str, list[dict[str, str]]]]:
    """The Python-name tables that are in force: (module, rows)."""
    return [
        (heading.split()[0], rows)
        for heading, rows in sections.items()
        if heading.split()[0].startswith("taskline")
        and not heading.split()[0].startswith("taskline_api")
        and "[planned" not in heading
    ]


def parameter_names(obj) -> set[str]:
    if dataclasses.is_dataclass(obj):
        return {field.name for field in dataclasses.fields(obj)}
    return set(inspect.signature(obj).parameters)


def check_row(module_name: str, row: dict[str, str]) -> list[str]:
    """What is wrong with one listed name; empty when it is as listed."""
    where = f"{module_name}.{row['name']}"
    try:
        obj = getattr(importlib.import_module(module_name), row["name"])
    except (ImportError, AttributeError) as error:
        return [f"{where}: cannot be imported ({error})"]
    problems = []
    fits = KINDS.get(row["kind"])
    if fits is None:
        problems.append(f"{where}: the contract gives an unknown kind {row['kind']!r}")
    elif not fits(obj):
        problems.append(f"{where}: is not a {row['kind']}")
    listed = [name.strip() for name in row.get("parameters", "none").split(",")]
    if listed != ["none"] and listed != [""]:
        missing = set(listed) - parameter_names(obj)
        if missing:
            problems.append(f"{where}: no longer has {sorted(missing)}")
    return problems


def contract_sections():
    return read_sections(CONTRACT.read_text())


def test_every_listed_python_name_is_there_as_listed():
    problems = [
        problem
        for module, rows in python_sections(contract_sections())
        for row in rows
        for problem in check_row(module, row)
    ]
    assert not problems, "\n".join(problems)


def test_each_module_lists_exactly_its_public_names():
    """A module's __all__ and the names the document lists for it are the same set, so a name
    cannot be dropped from the document, or added to the module, without the other."""
    listed: dict[str, set[str]] = {}
    for module, module_rows in python_sections(contract_sections()):
        listed.setdefault(module, set()).update(row["name"] for row in module_rows)
    for module, names in listed.items():
        declared = getattr(importlib.import_module(module), "__all__", None)
        assert declared is not None, f"{module} has no __all__, and the contract lists its names"
        assert names == set(declared), module
    assert set(taskline.__all__) == listed["taskline"]


def test_the_required_sections_are_still_in_the_document():
    present = {module for module, _ in python_sections(contract_sections())}
    assert present >= REQUIRED_MODULES


def test_the_status_values_match_the_kind_tables(empty_database_url):
    migrate(empty_database_url)
    engine = create_engine(empty_database_url)
    try:
        with engine.connect() as conn:
            for row in contract_sections()["Status values"]:
                name = row["set"]
                if name in NOT_KIND_TABLES:
                    continue
                assert name in KIND_TABLES, f"the status set {name!r} is not known to this test"
                listed = set(row["values"].split(", "))
                actual = {
                    r[0] for r in conn.execute(text(f"SELECT code FROM jobs.{KIND_TABLES[name]}"))
                }
                assert listed == actual, name
    finally:
        engine.dispose()


def view_sections() -> list[tuple[str, list[dict[str, str]]]]:
    """The view tables in force: (view name, rows)."""
    return [
        (heading.split()[0].removeprefix("taskline_api."), rows_)
        for heading, rows_ in contract_sections().items()
        if heading.startswith("taskline_api.") and "[planned" not in heading
    ]


def test_the_required_views_are_still_in_the_document():
    assert {view for view, _ in view_sections()} >= REQUIRED_VIEWS


def test_the_views_have_exactly_the_listed_columns_and_types(empty_database_url):
    migrate(empty_database_url)
    engine = create_engine(empty_database_url)
    try:
        with engine.connect() as conn:
            existing = {
                r[0]
                for r in conn.execute(
                    text(
                        "SELECT table_name FROM information_schema.views "
                        "WHERE table_schema = 'taskline_api'"
                    )
                )
            }
            assert existing == {view for view, _ in view_sections()}  # none the document omits
            for view, view_rows in view_sections():
                actual = dict(
                    conn.execute(
                        text(
                            "SELECT column_name, data_type FROM information_schema.columns "
                            "WHERE table_schema = 'taskline_api' AND table_name = :view"
                        ),
                        {"view": view},
                    ).all()
                )
                assert {r["column"]: r["type"] for r in view_rows} == actual, view
    finally:
        engine.dispose()


async def test_the_worker_roles_are_the_listed_ones(store, empty_database_url):
    await store.register_worker("host-1:4242:abc123", "parse")
    await store.register_worker("reaper:host-1:99:def456", "reaper")

    listed = next(r for r in contract_sections()["Status values"] if r["set"] == "worker role")
    found = {r[0] for r in rows(empty_database_url, "SELECT DISTINCT role FROM taskline_api.worker")}
    assert found == set(listed["values"].split(", "))


def test_the_refusal_codes_are_the_documented_ones():
    from taskline.api import REFUSAL_CODES

    documented = {}
    for row in contract_sections()["Refusal codes"]:
        cell = row["codes"]
        documented[row["command"]] = () if cell.startswith("none") else tuple(cell.split(", "))
    assert documented == REFUSAL_CODES


def test_a_missing_name_or_parameter_is_reported():
    assert check_row("taskline", {"name": "no_such_name", "kind": "function"})
    assert check_row(
        "taskline.migrate",
        {"name": "migrate", "kind": "function (sync)", "parameters": "no_such_parameter"},
    )
    assert check_row("taskline.migrate", {"name": "migrate", "kind": "class"})
    assert not check_row(
        "taskline.migrate",
        {"name": "migrate", "kind": "function (sync)", "parameters": "url_or_engine"},
    )
