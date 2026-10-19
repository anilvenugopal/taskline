from dataclasses import dataclass

LABEL_QUEUE = "queue_name"
LABEL_MAX_ATTEMPTS = "max_attempts"
LABEL_CANCEL_GRACE = "cancel_grace_seconds"
LABEL_TIMEOUT = "timeout"
LABEL_RETRY_DELAY = "retry_delay_seconds"
LABEL_RETRY_FACTOR = "retry_backoff_factor"
LABEL_RETRY_MAX_DELAY = "retry_max_delay_seconds"
LABEL_RETRY_JITTER = "retry_jitter"


@dataclass(frozen=True)
class TaskSpec:
    """A task's contract: its name and the settings every run of it is subject to.

    Submitters import specs; they never need the task's code.
    """

    name: str
    queue: str = "default"
    max_attempts: int = 1
    timeout_seconds: float | None = None
    cancel_grace_seconds: float | None = None
    requires_references: tuple[str, ...] = ()
    retry_delay_seconds: float = 5.0
    retry_backoff_factor: float = 2.0
    retry_max_delay_seconds: float = 300.0
    retry_jitter: float = 0.2

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("a task needs a name")
        if not self.queue:
            raise ValueError(f"task {self.name!r} needs a queue")
        if self.max_attempts < 1:
            raise ValueError(f"task {self.name!r}: max_attempts must be at least 1")
        if self.timeout_seconds is not None and self.timeout_seconds <= 0:
            raise ValueError(f"task {self.name!r}: timeout_seconds must be positive")
        if self.cancel_grace_seconds is not None and self.cancel_grace_seconds <= 0:
            raise ValueError(f"task {self.name!r}: cancel_grace_seconds must be positive")
        if self.retry_delay_seconds < 0:
            raise ValueError(f"task {self.name!r}: retry_delay_seconds cannot be negative")
        if self.retry_backoff_factor < 1:
            raise ValueError(f"task {self.name!r}: retry_backoff_factor must be at least 1")
        if self.retry_max_delay_seconds < self.retry_delay_seconds:
            raise ValueError(
                f"task {self.name!r}: retry_max_delay_seconds cannot be less than retry_delay_seconds"
            )
        if not 0 <= self.retry_jitter <= 1:
            raise ValueError(f"task {self.name!r}: retry_jitter must be between 0 and 1")

    def labels(self) -> dict[str, str]:
        """The settings as the text labels that travel with each message."""
        labels = {
            LABEL_QUEUE: self.queue,
            LABEL_MAX_ATTEMPTS: str(self.max_attempts),
            LABEL_RETRY_DELAY: str(self.retry_delay_seconds),
            LABEL_RETRY_FACTOR: str(self.retry_backoff_factor),
            LABEL_RETRY_MAX_DELAY: str(self.retry_max_delay_seconds),
            LABEL_RETRY_JITTER: str(self.retry_jitter),
        }
        if self.cancel_grace_seconds is not None:
            labels[LABEL_CANCEL_GRACE] = str(self.cancel_grace_seconds)
        if self.timeout_seconds is not None:
            labels[LABEL_TIMEOUT] = str(self.timeout_seconds)
        return labels
