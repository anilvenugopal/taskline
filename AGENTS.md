# Taskline — Agent Instructions

Taskline is a task execution engine on taskiq: jobs as chains of steps,
lifecycle hooks, leased heartbeats and lineage tracked in Postgres. It is a
product in its own right and **never imports or depends on the applications
that use it**, Lattice included.

Read before changing anything they cover:

- `docs/product_requirement.md`: what Taskline must do;
- `docs/architecture.md`: how it does it;
- `docs/product_development_plan.md`: the order of the work and its log;
- `docs/lattice_integration.md`: what Lattice needs from Taskline, under
  review.

## Running

```bash
uv sync
uv run pytest
```

Settings are `TASKLINE_*` environment variables (`.env.example`). The tests
use the Valkey on 6380 and Postgres on 5435 from `compose.yaml`.

- Agents do not start, stop or rebuild containers. Ask the user.
- Never read `.env`.

## Git

No AI attribution in git history: never add `Co-Authored-By` lines or
"Generated with" lines to commits or PR bodies.
