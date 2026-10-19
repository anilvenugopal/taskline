from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from taskline.spec import TaskSpec

# The name under which a step's wiring is sent to the worker as a keyword argument.
STEP_ARGUMENT = "taskline_step"

# Parameters of submit(); a job input cannot have one of these names.
RESERVED_INPUT_NAMES = frozenset({"producer", "references", "correlation_id", "triggered_by"})


@dataclass(frozen=True)
class Step:
    """One place in a job where a task is used, and how it is wired.

    inputs maps the task's parameter names to the names of the values they take: job
    inputs or outputs of earlier steps. arguments are constants. A step with one output
    declares output; a step whose task returns a dictionary declares outputs.
    """

    task: TaskSpec
    inputs: dict[str, str] = field(default_factory=dict)
    arguments: dict[str, Any] = field(default_factory=dict)
    output: str | None = None
    outputs: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "outputs", tuple(self.outputs))
        if self.output is not None and self.outputs:
            raise ValueError(f"step {self.task.name!r}: give output or outputs, not both")
        for parameter in self.inputs:
            if parameter in self.arguments:
                raise ValueError(
                    f"step {self.task.name!r}: parameter {parameter!r} is both an input and an argument"
                )

    @property
    def produced_names(self) -> tuple[str, ...]:
        return (self.output,) if self.output is not None else self.outputs

    def wiring(self) -> dict[str, Any]:
        """What the worker needs to run this step: sent with the message as taskline_step."""
        return {
            "inputs": dict(self.inputs),
            "arguments": dict(self.arguments),
            "output": self.output,
            "outputs": list(self.outputs),
        }


@dataclass(frozen=True)
class Job:
    """A named unit of work: declared inputs and one or more steps run in order."""

    name: str
    steps: Sequence[Step]
    inputs: Sequence[str] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "steps", tuple(self.steps))
        object.__setattr__(self, "inputs", tuple(self.inputs))
        if not self.name:
            raise ValueError("a job needs a name")
        if not self.steps:
            raise ValueError(f"job {self.name!r} needs at least one step")
        self._check_wiring()

    @classmethod
    def single(cls, task: TaskSpec, inputs: Sequence[str]) -> Job:
        """The job of one task, named after it, that takes the task's parameters as inputs."""
        names = tuple(inputs)
        return cls(task.name, inputs=names, steps=[Step(task, inputs={name: name for name in names})])

    def required_references(self) -> set[str]:
        return {kind for step in self.steps for kind in step.task.requires_references}

    def _check_wiring(self) -> None:
        available: set[str] = set()
        for name in self.inputs:
            if name in RESERVED_INPUT_NAMES:
                raise ValueError(
                    f"job {self.name!r}: {name!r} cannot be an input name; it is a parameter of submit()"
                )
            if name in available:
                raise ValueError(f"job {self.name!r}: input {name!r} is declared twice")
            available.add(name)

        for position, step in enumerate(self.steps, start=1):
            where = f"job {self.name!r}, step {position} ({step.task.name})"
            for parameter, name in step.inputs.items():
                if name not in available:
                    raise ValueError(
                        f"{where}: parameter {parameter!r} takes {name!r}, which is neither a "
                        "job input nor the output of an earlier step"
                    )
            for name in step.produced_names:
                if name in available:
                    raise ValueError(f"{where}: the name {name!r} is already a job input or an output")
                available.add(name)
