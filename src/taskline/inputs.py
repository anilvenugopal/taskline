"""Checking the values a job is submitted with against the inputs it declares."""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from pydantic import ValidationError

from taskline.job import Job

MISSING = "is required and was not given"
UNKNOWN = "is not an input of this job"
WHOLE = "(inputs)"  # the name given to a problem that is about the values together, not one input


@dataclass(frozen=True)
class InputProblem:
    name: str  # the job input, or "(inputs)" for a rule about several together
    message: str


def check_inputs(job: Job, values: Mapping[str, Any]) -> tuple[dict[str, Any], list[InputProblem]]:
    """The values to send, and what is wrong with the ones given (empty when nothing is).

    A job with names only needs every name given and no other. A job with a model has its values
    validated by the model, with each wrong input named, and what is sent is the model's own
    result: coerced to the declared types, defaults filled in, and dumped as JSON, because the
    values travel through Valkey and Postgres.
    """
    problems = [InputProblem(name, UNKNOWN) for name in values if name not in job.inputs]
    if job.input_model is None:
        missing = [InputProblem(name, MISSING) for name in job.inputs if name not in values]
        return dict(values), missing + problems

    known = {name: value for name, value in values.items() if name in job.inputs}
    try:
        validated = job.input_model.model_validate(known)
    except ValidationError as error:
        return dict(values), _from_validation(error) + problems
    return validated.model_dump(mode="json"), problems


def _from_validation(error: ValidationError) -> list[InputProblem]:
    """One problem for each wrong input and message; the value itself is never repeated."""
    problems: list[InputProblem] = []
    for item in error.errors():
        name = str(item["loc"][0]) if item["loc"] else WHOLE
        message = MISSING if item["type"] == "missing" else item["msg"]
        problem = InputProblem(name, message)
        if problem not in problems:
            problems.append(problem)
    return problems
