"""Run before a release: `uv run pytest -m release`. Needs uv, and the network if uv has not
cached the dependencies."""

import os
import subprocess
from pathlib import Path

import pytest

from taskline.config import get_config
from taskline.setup import schema_version

pytestmark = pytest.mark.release

ROOT = Path(__file__).parent.parent


def run(command, **kwargs):
    return subprocess.run(
        command, capture_output=True, text=True, timeout=600, check=True, **kwargs
    )


def test_the_built_wheel_installs_in_a_clean_environment_and_migrates(tmp_path, empty_database_url):
    dist, venv = tmp_path / "dist", tmp_path / "venv"
    run(["uv", "build", "--wheel", "--out-dir", str(dist)], cwd=ROOT)
    run(["uv", "venv", "--python", "3.14", str(venv)])
    python = venv / "bin" / "python"
    run(["uv", "pip", "install", "--python", str(python), str(next(dist.glob("taskline-*.whl")))])

    env = {
        key: value for key, value in os.environ.items() if key not in {"VIRTUAL_ENV", "PYTHONPATH"}
    } | {
        "TASKLINE_DATABASE_URL": empty_database_url,
        "TASKLINE_VALKEY_URL": get_config().valkey_url,
        "TASKLINE_INSTANCE_NAME": "wheel-test",
    }
    # Run from a temporary folder, so the repository's .env and source are not on the path.
    migrated = run([str(venv / "bin" / "taskline"), "migrate"], cwd=tmp_path, env=env)

    assert "0001_schema" in migrated.stdout and "0003_api_views" in migrated.stdout
    assert schema_version(empty_database_url).instance_name == "wheel-test"

    # The test helpers ship in the wheel, and work from the installed copy.
    helpers = (
        "import sys; from taskline.testing import add_run; "
        "print(add_run(sys.argv[1], state='failed').run_id)"
    )
    made = run([str(python), "-c", helpers, empty_database_url], cwd=tmp_path, env=env)
    assert len(made.stdout.strip()) == 36  # a run id
