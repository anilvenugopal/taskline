import pytest

import taskline
from taskline.context import cancel_requested, report_progress
from taskline.declaration import task
from taskline.errors import NonRetryable, TaskCancelled
from taskline.hook_registry import hooks
from taskline.job import Job, Step
from taskline.lineage import current_lineage
from taskline.spec import TaskSpec
from taskline.store import ReferencePlan
from taskline.submitting import submit


def test_every_public_name_resolves_to_the_right_object():
    expected = {
        "Job": Job,
        "NonRetryable": NonRetryable,
        "Reference": ReferencePlan,
        "Step": Step,
        "TaskCancelled": TaskCancelled,
        "TaskSpec": TaskSpec,
        "cancel_requested": cancel_requested,
        "current_lineage": current_lineage,
        "hooks": hooks,
        "report_progress": report_progress,
        "submit": submit,
        "task": task,
    }

    assert set(taskline.__all__) == set(expected)
    for name, value in expected.items():
        assert getattr(taskline, name) is value


def test_a_name_that_is_not_public_raises_the_usual_error():
    with pytest.raises(AttributeError, match="no attribute 'migrate_everything'"):
        _ = taskline.migrate_everything


def test_the_public_names_are_listed_by_dir():
    assert set(taskline.__all__) <= set(dir(taskline))
