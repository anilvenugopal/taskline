import random
from collections.abc import Mapping
from typing import Any

from pydantic import ValidationError

from taskline.errors import (
    InvalidStepResult,
    MissingValue,
    NonRetryable,
    TaskCancelled,
    ValuesTooLarge,
)
from taskline.lineage import LineageContext
from taskline.spec import (
    LABEL_RETRY_DELAY,
    LABEL_RETRY_FACTOR,
    LABEL_RETRY_JITTER,
    LABEL_RETRY_MAX_DELAY,
    TaskSpec,
)

# Errors that would happen again with the same values, or that were asked for.
NOT_RETRIED = (
    NonRetryable,
    TaskCancelled,
    ValidationError,
    MissingValue,
    InvalidStepResult,
    ValuesTooLarge,
)

_DEFAULTS = TaskSpec(name="defaults")


def is_retryable(error: BaseException | None) -> bool:
    return error is not None and not isinstance(error, NOT_RETRIED)


def should_retry(lineage: LineageContext, error: BaseException | None) -> bool:
    """True when the failed attempt has attempts left and the error is worth repeating."""
    return lineage.attempt < lineage.max_attempts and is_retryable(error)


def retry_delay(attempt: int, labels: Mapping[str, Any]) -> float:
    """Seconds to wait after the given failed attempt: backoff, capped, with jitter."""
    base = float(labels.get(LABEL_RETRY_DELAY, _DEFAULTS.retry_delay_seconds))
    factor = float(labels.get(LABEL_RETRY_FACTOR, _DEFAULTS.retry_backoff_factor))
    cap = float(labels.get(LABEL_RETRY_MAX_DELAY, _DEFAULTS.retry_max_delay_seconds))
    jitter = float(labels.get(LABEL_RETRY_JITTER, _DEFAULTS.retry_jitter))

    delay = min(cap, base * factor ** (attempt - 1))
    if jitter:
        delay *= random.uniform(1 - jitter, 1 + jitter)
    return max(0.0, min(cap, delay))
