import inspect
from collections.abc import Callable
from typing import Any

from pydantic import ConfigDict, validate_call
from pydantic_core import PydanticSerializationError, to_json

from taskline.config import get_config
from taskline.errors import InvalidStepResult, MissingValue, ValuesTooLarge


def stepped(function: Callable[..., Any]) -> Callable[..., Any]:
    """Wrap a task function so that it takes a job's values and a step's wiring.

    The wrapper has the signature (values, taskline_step). It maps the named values the
    step needs to the function's parameters, checks them against the function's type
    hints, calls the function, and returns the values with the step's outputs added.
    """
    checked = validate_call(function, config=ConfigDict(arbitrary_types_allowed=True))

    if inspect.iscoroutinefunction(function):

        async def run_step(values: dict[str, Any], taskline_step: dict[str, Any]) -> dict[str, Any]:
            result = await checked(**_arguments(values, taskline_step))
            return _add_outputs(values, taskline_step, result)

    else:

        def run_step(values: dict[str, Any], taskline_step: dict[str, Any]) -> dict[str, Any]:
            result = checked(**_arguments(values, taskline_step))
            return _add_outputs(values, taskline_step, result)

    run_step.__name__ = function.__name__
    run_step.__qualname__ = function.__qualname__
    run_step.__doc__ = function.__doc__
    return run_step


def _arguments(values: dict[str, Any], step: dict[str, Any]) -> dict[str, Any]:
    arguments = dict(step.get("arguments") or {})
    for parameter, name in (step.get("inputs") or {}).items():
        if name not in values:
            raise MissingValue(
                f"parameter {parameter!r} needs the value {name!r}, which is not available"
            )
        arguments[parameter] = values[name]
    return arguments


def _add_outputs(values: dict[str, Any], step: dict[str, Any], result: Any) -> dict[str, Any]:
    produced: dict[str, Any] = {}
    output = step.get("output")
    outputs = step.get("outputs") or []
    if output is not None:
        produced[output] = result
    elif outputs:
        if not isinstance(result, dict):
            raise InvalidStepResult(
                f"the task returned {type(result).__name__}; it must return a dictionary "
                f"with the keys {list(outputs)}"
            )
        for key in outputs:
            if key not in result:
                raise InvalidStepResult(f"the task's result has no value for the output {key!r}")
            produced[key] = result[key]

    for name, value in produced.items():
        try:
            to_json(value)
        except (PydanticSerializationError, TypeError, ValueError) as error:
            raise InvalidStepResult(f"the output {name!r} cannot be sent as JSON: {error}") from error

    new_values = {**values, **produced}
    _check_size(new_values)
    return new_values


def _check_size(values: dict[str, Any]) -> None:
    limit = get_config().max_values_bytes
    size = len(to_json(values))
    if size > limit:
        largest = max(values, key=lambda name: len(to_json(values[name])))
        raise ValuesTooLarge(
            f"the job's values are {size} bytes, over the limit of {limit}; the largest is "
            f"{largest!r} at {len(to_json(values[largest]))} bytes. Pass a reference, not the data"
        )
