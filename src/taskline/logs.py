import logging

from taskline.lineage import current_lineage

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s [%(taskline)s] %(message)s"


def configure_logging(level: int = logging.INFO) -> None:
    """Log to the console with each line tagged by its task. For taskline's own commands only."""
    handler = logging.StreamHandler()
    handler.addFilter(LineageFilter())
    handler.setFormatter(logging.Formatter(LOG_FORMAT))
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)


class LineageFilter(logging.Filter):
    """Adds a `taskline` attribute to each record: the current task's ids, or "-" outside a task.

    Attach it to a handler. It does not change the handler's format; use LOG_FORMAT or
    include %(taskline)s in your own.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        lineage = current_lineage()
        if lineage is None:
            record.taskline = "-"
        else:
            record.taskline = (
                f"run={str(lineage.job_run_id)[-8:]} task={str(lineage.task_instance_id)[-8:]} "
                f"step={lineage.step_index + 1}/{lineage.step_count} try={lineage.attempt} "
                f"corr={lineage.correlation_id}"
            )
        return True
