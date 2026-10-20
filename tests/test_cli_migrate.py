from helpers import rows, taskline

from taskline.config import get_config
from taskline.setup import schema_version


def test_the_migrate_command_names_the_instance_after_the_namespace(runtime):
    result = taskline("migrate")

    assert result.returncode == 0, result.stderr
    assert "0003_api_views" in result.stdout
    assert schema_version(get_config().database_url).instance_name == runtime


def test_the_migrate_command_uses_the_instance_name_setting(runtime, monkeypatch):
    monkeypatch.setenv("TASKLINE_INSTANCE_NAME", "strategiq-prod")

    result = taskline("migrate")

    assert result.returncode == 0, result.stderr
    assert rows(get_config().database_url, "SELECT name FROM jobs.instance") == [("strategiq-prod",)]
