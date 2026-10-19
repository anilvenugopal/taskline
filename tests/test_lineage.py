import asyncio
import io
import logging

from test_hook_registry import make_lineage

from taskline.lineage import bound_lineage, current_lineage
from taskline.logs import LOG_FORMAT, LineageFilter


def make_logger(format_string: str):
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(logging.Formatter(format_string))
    handler.addFilter(LineageFilter())
    logger = logging.getLogger("taskline.test_lineage")
    logger.handlers = [handler]
    logger.propagate = False
    logger.setLevel(logging.INFO)
    return logger, stream


def test_there_is_no_lineage_outside_a_task():
    assert current_lineage() is None


async def test_lineage_is_set_inside_the_block_and_seen_from_a_helper_thread():
    lineage = make_lineage()

    with bound_lineage(lineage):
        assert current_lineage() is lineage
        assert await asyncio.to_thread(current_lineage) is lineage

    assert current_lineage() is None


def test_log_lines_outside_a_task_show_a_dash():
    logger, stream = make_logger(LOG_FORMAT)

    logger.info("hello")

    assert "[-] hello" in stream.getvalue()


def test_log_lines_inside_a_task_show_its_ids():
    logger, stream = make_logger(LOG_FORMAT)
    lineage = make_lineage()

    with bound_lineage(lineage):
        logger.info("parsing")

    line = stream.getvalue()
    assert f"run={str(lineage.job_run_id)[-8:]}" in line
    assert f"task={str(lineage.task_instance_id)[-8:]}" in line
    assert "step=2/3" in line
    assert "try=1" in line
    assert "corr=upload-42" in line
    assert "parsing" in line


def test_the_filter_does_not_change_a_handlers_own_format():
    logger, stream = make_logger("%(levelname)s %(message)s")

    with bound_lineage(make_lineage()):
        logger.info("plain")

    assert stream.getvalue() == "INFO plain\n"
