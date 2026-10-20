# Changelog

Newest first. A **Contract** entry changes what `docs/public_contract.md` lists. Before 1.0 a minor release
may include breaking contract changes and a patch release never does.

## 0.2.0, 2026-10-19

The first contract release: Taskline UI can build against it.

### Contract
- Added `taskline.api`: the async commands `cancel_run`, `retry_run`, `requeue` and `submit_job`, and
  `shutdown`, with the results `CancelResult`, `RetryResult`, `RequeueResult` and `SubmitResult`, `Refusal`,
  `InputProblem` and `REFUSAL_CODES`. Every command takes an `actor`, written into the event log. A request
  Taskline refuses comes back as a result with a refusal code, not an exception.
- Added `taskline.catalogue.register`, so `submit_job` can find a job by name.
- Added `taskline.testing` (`add_run`, `add_worker`, `clear`, `RunHandle`, `STATES`): synchronous test-data
  helpers that write runs, tasks, references, events and workers in known states through the real store
  operations, with no worker or Valkey.
- Added the `taskline_api` views `run`, `task`, `reference`, `event`, `queue`, `worker` and `instance`, and
  `migrate(reader_role=...)`, which grants a database role read access to them and nothing else.
- Added `taskline.setup`: `register_producers`, `register_reference_kinds`, `schema_version`, `Kind`,
  `SchemaVersion` and `NotMigrated`.
- `taskline.migrate.migrate` takes a database URL or a synchronous engine, and an `instance_name`. Its first
  parameter is renamed `url_or_engine` (breaking for callers that pass it by keyword).

### Other
- The commands can be called with a fresh `asyncio.run` each time: a runtime built on an event loop that has
  ended is rebuilt. (Before, the Valkey connection of a finished loop made the second send fail.) One
  long-lived loop is still the recommended way.
- `taskline cancel`, `retry` and `requeue` now call the same functions, print the same messages, and record
  themselves as `cli:<user>` in the event log.
- The contract test now requires each contract module's `__all__` to equal the names the contract lists for
  it, so a name cannot be dropped from the document or added to a module without the other.
- New migration `0003_api_views`: the views, and indexes for the filters a list uses. `taskline runs`,
  `show` and `queues`, and the reaper's queue check, now read through the views. **After upgrading, run
  `taskline migrate` before starting workers or a reaper.**
- New table `jobs.instance` (migration `0002_instance`) holds the instance name and the Taskline version
  that last migrated the database.
- Dependencies now have version ceilings, and `pydantic`, which was imported but not declared, is declared.
- The tests treat deprecation warnings as errors. New tests enforce the public contract and the import
  boundary, and `pytest -m release` checks that the built wheel installs and migrates.
