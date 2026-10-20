"""NFR-1: Taskline imports only the standard library, itself and its declared dependencies,
so it can never depend on a host application, Lattice or Taskline UI."""

import ast
import re
import sys
import tomllib
from importlib.metadata import packages_distributions
from pathlib import Path

ROOT = Path(__file__).parent.parent
FORBIDDEN = {"lattice", "taskline_ui", "strategiq"}  # named for a clearer message; the rule below covers all
PART_OF = {"pydantic_core": "pydantic"}  # pydantic's own engine, installed and pinned by pydantic


def normalise(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def declared_distributions() -> set[str]:
    dependencies = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["dependencies"]
    return {normalise(re.split(r"[\[<>=!~;\s]", d, maxsplit=1)[0]) for d in dependencies}


def is_dynamic_import(call: ast.Call) -> bool:
    target = call.func
    name = target.attr if isinstance(target, ast.Attribute) else getattr(target, "id", "")
    return name in {"import_module", "__import__"}


def imported_names(root: Path) -> list[tuple[Path, str]]:
    """Every top-level module imported anywhere under root, with the file that imports it."""
    found: list[tuple[Path, str]] = []
    for path in sorted(root.rglob("*.py")):
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.Import):
                found += [(path, alias.name) for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                found.append((path, node.module))
            elif (
                isinstance(node, ast.Call)
                and is_dynamic_import(node)
                and node.args
                and isinstance(node.args[0], ast.Constant)
                and isinstance(node.args[0].value, str)
            ):
                found.append((path, node.args[0].value))
    return [(path, name.split(".")[0]) for path, name in found]


def violations(root: Path) -> list[str]:
    providers = packages_distributions()
    declared = declared_distributions()
    problems = []
    for path, top in imported_names(root):
        if top in FORBIDDEN:
            problems.append(f"{path.name} imports {top}, which uses Taskline")
        elif top == "taskline" or top in sys.stdlib_module_names:
            continue
        else:
            owners = (
                {normalise(PART_OF[top])}
                if top in PART_OF
                else {normalise(d) for d in providers.get(top, [])}
            )
            if not owners & declared:
                problems.append(f"{path.name} imports {top}, which pyproject.toml does not declare")
    return problems


def test_taskline_imports_only_the_standard_library_itself_and_its_dependencies():
    assert violations(ROOT / "src" / "taskline") == []


def test_an_import_of_a_host_package_is_caught(tmp_path):
    (tmp_path / "bad.py").write_text("import lattice\nfrom taskline_ui import pages\n")
    assert len(violations(tmp_path)) == 2


def test_an_import_of_an_undeclared_package_is_caught(tmp_path):
    (tmp_path / "bad.py").write_text("import some_package_nobody_declared\n")
    assert violations(tmp_path)


def test_a_literal_dynamic_import_is_caught_but_a_configured_one_is_not(tmp_path):
    (tmp_path / "bad.py").write_text('import importlib\nimportlib.import_module("lattice")\n')
    (tmp_path / "fine.py").write_text("import importlib\nname = 'x'\nimportlib.import_module(name)\n")
    assert violations(tmp_path) == ["bad.py imports lattice, which uses Taskline"]


def test_the_standard_library_and_declared_packages_are_fine(tmp_path):
    (tmp_path / "ok.py").write_text("import json\nimport sqlalchemy\nfrom taskline import job\n")
    assert violations(tmp_path) == []
