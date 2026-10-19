import dataclasses
import subprocess
import sys

import pytest

from taskline.spec import TaskSpec


def test_settings_become_text_labels_in_one_place():
    spec = TaskSpec(
        name="parse_pdf", queue="parse", max_attempts=3, timeout_seconds=900, cancel_grace_seconds=600
    )

    labels = spec.labels()

    assert labels == {
        "queue_name": "parse",
        "max_attempts": "3",
        "timeout": "900",
        "cancel_grace_seconds": "600",
        "retry_delay_seconds": "5.0",
        "retry_backoff_factor": "2.0",
        "retry_max_delay_seconds": "300.0",
        "retry_jitter": "0.2",
    }
    assert all(isinstance(value, str) for value in labels.values())


def test_optional_settings_that_are_not_set_are_left_out_of_the_labels():
    labels = TaskSpec(name="fetch_source").labels()

    assert "timeout" not in labels
    assert "cancel_grace_seconds" not in labels
    assert labels["queue_name"] == "default"
    assert labels["retry_delay_seconds"] == "5.0"


def test_retry_settings_can_be_chosen_per_task():
    spec = TaskSpec(
        name="fetch_source",
        retry_delay_seconds=1,
        retry_backoff_factor=3,
        retry_max_delay_seconds=60,
        retry_jitter=0,
    )

    labels = spec.labels()

    assert labels["retry_delay_seconds"] == "1"
    assert labels["retry_backoff_factor"] == "3"
    assert labels["retry_max_delay_seconds"] == "60"
    assert labels["retry_jitter"] == "0"


def test_the_spec_keeps_ordinary_python_values():
    spec = TaskSpec(name="parse_pdf", max_attempts=3)

    assert spec.max_attempts == 3
    assert isinstance(spec.max_attempts, int)


@pytest.mark.parametrize(
    ("options", "message"),
    [
        ({"name": ""}, "needs a name"),
        ({"name": "t", "queue": ""}, "needs a queue"),
        ({"name": "t", "max_attempts": 0}, "max_attempts must be at least 1"),
        ({"name": "t", "timeout_seconds": 0}, "timeout_seconds must be positive"),
        ({"name": "t", "cancel_grace_seconds": -5}, "cancel_grace_seconds must be positive"),
        ({"name": "t", "retry_delay_seconds": -1}, "retry_delay_seconds cannot be negative"),
        ({"name": "t", "retry_backoff_factor": 0.5}, "retry_backoff_factor must be at least 1"),
        (
            {"name": "t", "retry_delay_seconds": 10, "retry_max_delay_seconds": 5},
            "retry_max_delay_seconds cannot be less",
        ),
        ({"name": "t", "retry_jitter": 1.5}, "retry_jitter must be between 0 and 1"),
    ],
)
def test_bad_values_are_refused(options, message):
    with pytest.raises(ValueError, match=message):
        TaskSpec(**options)


def test_a_spec_cannot_be_changed_after_it_is_created():
    spec = TaskSpec(name="parse_pdf")

    with pytest.raises(dataclasses.FrozenInstanceError):
        spec.queue = "other"


def test_importing_a_spec_does_not_pull_in_taskiq_or_the_database_code():
    code = (
        "import sys, taskline.spec; "
        "heavy = [m for m in ('taskiq', 'sqlalchemy', 'redis') if m in sys.modules]; "
        "sys.exit(len(heavy))"
    )

    result = subprocess.run([sys.executable, "-c", code], capture_output=True)

    assert result.returncode == 0
