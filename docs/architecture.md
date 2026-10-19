# Taskline: Architecture

Read this first. Every other document and every line of code should be explainable from this one.

## 1. What taskline is

Taskline runs background work for an application. The application hands taskline a job: one task, or an ordered chain of tasks. Taskline sends each task to a worker, watches the worker while it runs, retries it if it fails, takes it back if the worker dies, and records everything that happened in a database so a person can look at it afterwards.

Taskline is a product in its own right. It lives in `packages/taskline/`, imports nothing from the application that uses it, and could be copied into another repository unchanged.

Taskline is built on **taskiq**, an open-source Python task queue library. Taskiq provides the parts that are hard to get right and are not our business: sending messages through a queue, workers that receive them, and a plug-in point (called a middleware) that lets us run our own code before and after every task. Taskline adds what taskiq does not have: a durable record of every task, leased heartbeats, recovery of lost work, and a clear history of each chain.

## Terms

These words have one meaning throughout taskline's documents, tables and code.

| Term | Meaning | Example |
|---|---|---|
| **Task** | One function that does one piece of work. Declared with the `@task` decorator, which gives it a queue, a maximum number of attempts and other settings | `parse_pdf` |
| **Job** | A named unit of work made of one or more tasks run in order. A job of one task is the simple case | `ingest_document`: fetch_source, then parse_pdf, then extract_claims |
| **Step** | One place in a job where a task is used, together with how it is wired: its inputs, its outputs and its constants. The same task can be used in several steps and in several jobs | `Step(PARSE_PDF, inputs={"path": "source_path"}, output="parsed_path")` |
| **Job run** | One submission of a job. A row in `job_run`. "Run" is short for job run | |
| **Task instance** | One run of a step within a job run. A row in `task_instance`, whose `step_index` says which step | |
| **Attempt** | One try at a task instance. A task instance has several attempts if it is retried | |
| **Queue** | A named line of messages in Valkey. Each task declares its queue | `parse` |
| **Worker** | One operating-system process that takes messages off a queue and runs tasks, one at a time by default | PID 4812 |

## 2. The pieces

| Piece | What it is | Where it runs | Why it exists |
|---|---|---|---|
| **Producer** | The application code that submits a job | Inside the application (web app, script, a running task) | Starts work |
| **Valkey** | A Redis-compatible in-memory server holding the queue | Its own container | Carries messages from producers to workers quickly. Holds no history we depend on |
| **Worker** | A process that takes messages off one queue and runs tasks | `taskline worker --queue <name>` | Does the work |
| **Lifecycle middleware** | Taskline code plugged into taskiq inside every producer and worker | Same processes as above | Records what is about to happen and what did happen, starts the heartbeat, runs hooks |
| **Postgres, schema `jobs`** | The record of every run, task and event | The application's Postgres | The single source of truth. If Valkey is wiped, Postgres still knows what was supposed to happen |
| **Reaper** | A small separate process that checks Postgres for work that has gone wrong | `taskline reaper` | Takes back tasks from dead workers, sends due retries, fails lost hand-offs, and reports queues that look wrong. It never touches a queued task |
| **Scheduler** | A process that fires schedules stored in Postgres | `taskline scheduler` | Starts jobs at their cron or interval times, once each, even if two schedulers run |
| **Command line** | `taskline migrate`, `worker`, `reaper`, `scheduler`, `schedules`, `runs`, `show`, `cancel` | A terminal | Operating and inspecting the system |
| **Admin UI** | A web page over the `jobs` schema | Later step | Lets people see runs, step status, heartbeats and event logs, and cancel or retry |

## 3. How the pieces connect

```
  Application (producer)
     |  1. submit a job
     v
  Lifecycle middleware ---- 2. write plan + "queued" ----> Postgres  (schema jobs)
     |                                                        ^   ^
     |  3. put message on queue                               |   |
     v                                                        |   |
  Valkey (queue = a stream per queue name)                    |   |
     |  4. worker reads message                               |   |
     v                                                        |   |
  Worker + Lifecycle middleware ---- 5. claim a lease --------+   |
     |        |                      6. heartbeat every N secs ---+
     |        +--- runs your task function (+ pre/post hooks)
     |  7. write outcome (success, failure, retry)  -----------> Postgres
     |  8. if part of a chain, send the next step to the queue
     v
  Valkey

  Reaper (separate process) ---- reads Postgres every few seconds:
     expired leases, retries that are due, steps never sent
     ---> updates Postgres, and sends the stored message of a due retry to Valkey
```

The important idea: **Valkey is the delivery truck, Postgres is the ledger.** Messages can be lost or delivered twice by the truck. The ledger decides what is true.

## 4. The life of one task

1. **Submit.** The application submits a job. The middleware, before the first message is sent, writes a row in `jobs.job_run` (the lineage) and a row in `jobs.task_instance` for each task in the job (the first with status `queued`). It stores a copy of the full message in that row. It also records **who submitted it** (the producer) and any **external references** supplied with the submission (section 10.1). A submission with no producer, or with a reference kind nobody registered, is refused before anything is queued. It then sends the message to Valkey.
2. **Pick up.** A worker for that queue reads the message.
3. **Claim.** Before running anything, the middleware asks Postgres: "mark this task `running`, and give me a lease." This is one atomic update that only succeeds if the task is currently `queued`. The same update adds one to the **attempt number**, and the worker holds that number for the rest of the attempt. If the update changes nothing (someone else already claimed it, or it already finished, or it was cancelled), this worker skips the message and records a `duplicate_skipped` event. This is what makes a duplicated message harmless.
4. **Heartbeat.** The worker starts a small background loop next to the task. Every 15 seconds by default it tells Postgres "still alive" and extends the lease (60 seconds by default, four heartbeats). Both can be set in `.env` and overridden per job. It can also report progress (percent, note). The heartbeat only works if the task is still `running` under the attempt number the worker holds.
5. **Run.** The task function runs. Taskline's pre-hooks run first; post-hooks run after.
6. **Finish.** The middleware writes the outcome, again naming its attempt number: `succeeded`, or `failed` with the error and traceback, or `retrying` with a time to try again. If the task is no longer `running` under that attempt number (it was taken back while this worker was slow), the write is refused and this worker knows it lost.
7. **Log.** Every step above also appends a row to `jobs.task_event`. That table is the task's log.

## 5. The life of a job with several tasks

A job of several tasks is a chain: an ordered list of tasks where each task receives the previous task's result. Taskiq's companion library `taskiq-pipelines` does the passing: when a step finishes and its result is saved, the library sends the next step.

Taskline adds the lineage on top:

- At submit time, taskline knows every task in the job, because the library assigns each its task id up front. Taskline writes one `job_run` row and one `task_instance` row per task. Task instances not yet reached have status `planned`. So a person can see the whole plan from the first moment.
- The first task's id doubles as the job run's id (the lineage id). Later tasks learn it by reading the chain data that the library attaches to every message, so no extra plumbing is needed.
- If a task fails for good, taskline marks every later task `upstream_failed` (they will never run), updates the job run to `failed`, and makes sure the library's own bookkeeping reflects the failure so anyone waiting on the final result is released.
- The job run's status is derived from its task instances: running while any is unfinished, `succeeded` when all succeeded, `failed` if one failed, `cancelled` if cancelled.

Version 1 supports tasks run in sequence only. The library also offers map and filter steps; those are out of scope for now.

### 5.1 Scheduled runs

A schedule says "run this job at these times". The job can be one task or several tasks in order. A schedule is data in Postgres, not code, so people can create, pause, resume and delete schedules from the command line or the admin UI without a deployment.

- **What a schedule holds:** a name, the job to run, the arguments, a cron expression or a fixed interval, a time zone, an optional start time and end time, enabled or paused, the **producer** and **references** every run it creates will carry (so scheduled runs are never anonymous), and two policies described below.
- **Start and end times.** A schedule can be entered ahead of time. Nothing fires before its start time, and nothing fires at or after its end time, so deploying at 3 pm for a midnight start needs no action at midnight, and a schedule with an end time stops on its own. The first run is the first scheduled time at or after the start time. An interval schedule counts its intervals from the start time, or from when it was created if there is none. Times before the start are never counted as missed. Whether a schedule has ended is worked out from its end time; no flag is changed.
- **The scheduler process** (`taskline scheduler`) wakes every second, finds schedules that are due, and submits them like any other producer, with trigger `schedule`. Every run it creates records which schedule created it.
- **Built on taskiq's scheduler.** Taskiq already provides the loop that checks sources every second and sends due tasks through the broker. Taskline supplies a schedule source that reads Postgres. Because the task goes through the normal broker path, the lifecycle middleware records it exactly as it records any other task.
- **Two scheduler processes must not double-fire.** Taskiq's own scheduler has no protection against that, and forgets what it has fired when it restarts. Taskline adds both. Before firing, the scheduler writes a row to `schedule_fire` keyed by schedule and intended fire time, and creates the run's task rows in the same database transaction. The key is unique, so a second scheduler trying the same moment is refused and skips it. Only after that transaction commits is the message sent. If the process dies before sending, the task is already in Postgres as queued with its stored message, and the reaper sends it. The `schedule_fire` row also remembers the run that resulted, or why nothing ran. Any number of schedulers can run at once.
- **Overlap policy**, per schedule: `allow` (start a new run even if the last one is still going) or `skip_if_running` (record the fire as skipped).
- **Missed-fire policy**, per schedule, for when the scheduler was down at the intended time: `skip` (ignore missed times) or `run_once` (fire once for the missed window when it returns). Never one run per missed time.
- **Declared in code:** a job's definition may declare a default schedule. A `taskline schedules sync` command creates it if no schedule with that name exists, and never overwrites one a person has edited.

### 5.2 Passing data between steps

A job declares its **inputs**: the named values a submitter must give. Each step declares which parameters of its task take which named values, and what it **outputs**. The names a step can use are the job's inputs and the outputs of any earlier step, not only the previous one. A step's own constants are given as `arguments`.

```python
INGEST_DOCUMENT = Job(
    "ingest_document",
    inputs=["url", "project_id"],
    steps=[
        Step(FETCH_SOURCE, inputs={"url": "url"}, output="source_path"),
        Step(PARSE_PDF, inputs={"path": "source_path"}, output="parsed_path", arguments={"dpi": 300}),
        Step(EXTRACT_CLAIMS, inputs={"parsed_path": "parsed_path", "project_id": "project_id"}),
    ],
)
```

`inputs={"path": "source_path"}` reads "this task's parameter `path` takes the value named `source_path`". A step with one output declares `output="name"`; a step whose task returns a dictionary of several values declares `outputs=["a", "b"]`, and a missing key fails that task with a clear message. A step uses only the values it names, so there is nothing to ignore.

Underneath, the chain library hands each step one value. Taskline makes that value a dictionary of everything known so far: the job inputs and the outputs of finished steps. A wrapper around each task picks the named values it needs, calls the function with them, and adds the return value to the dictionary under the step's output name. Job authors never see the dictionary.

Rules checked when the job definition loads: every name a step uses is a job input or an earlier step's output, and all inputs and outputs have different names. When a job is submitted, taskline checks that every job input is given and none is unknown. A task can be submitted alone as a job of one task, named after it, whose inputs are the arguments given. This is how part of a flow is run again.

**Pass references, not payloads.** Every value in the dictionary travels through Valkey and is kept in Postgres inside the stored message of each following task instance, which the reaper needs in order to send it again. The same is true of the inputs given to `submit`. So values should be small, such as a document id, a file path or an object-store key, and a task should fetch the data itself. Task instances store no output of their own, only that message.

Mapped values are checked against the task function's type hints when the task starts, not when the job is submitted, because the submitter does not have the task's code. A wrong type fails that task, after earlier steps have run.

### 5.3 When the next task is not known in advance

A job's tasks are fixed when the job is submitted. When what comes next depends on a result, the task submits a new job. The new job records the submitting task as its parent and inherits its producer and correlation id, so related work can be found by one id. A task that finds many items, such as 200 documents, can submit one job for each, and each has its own history, retries and cancel. The submitting job's run can end `succeeded` while the jobs it started are still running, so the whole flow is seen through the correlation id or the parent links. Listing every possible task in the job and letting the unneeded ones end quickly also works, but a task that does nothing is recorded as `succeeded`, so it is not recommended.

## 6. Heartbeats and leases in plain words

A **lease** is a promise with an expiry: "this worker owns this task until time T, and will renew before T." The worker's heartbeat renews it. If the worker dies, hangs, or loses its network, the renewals stop and the lease runs out. The reaper then sees an expired lease and acts.

Four design choices make this more reliable than a simple "last seen" timestamp:

1. **The database clock is the only clock.** Leases are computed with Postgres's `now()`, never with a worker's clock, so a worker with a wrong clock cannot extend or shorten a lease.
2. **The attempt number fences every write.** Each claim adds one to the attempt number, and every heartbeat and result write says "only if the task is still running under my attempt number". Imagine worker A freezes for a minute, the reaper takes the task back, and worker B claims it as the next attempt. When A wakes up, its heartbeat and final write name the old attempt number, so Postgres refuses both. A discovers it lost the lease and stops. Without this, A and B could both think they own the task.
3. **The heartbeat runs on the worker's event loop, and heavy work stays off it.** A task is either an `async` function or a plain function. An `async` task runs on the event loop, so it must not block it. A plain task is run by taskline on a helper thread, so it can take hours and the heartbeat keeps ticking. Whichever kind it is, if something does block the loop, heartbeats stop, the lease expires, and the reaper reclaims the task. That is intended: a wedged worker looks the same as a dead one. Choosing the right kind for the work is up to the task's author: `async` for work that mostly waits (downloads, API calls, database, object storage), plain for heavy computation such as parsing.
4. **Cancellation rides on the heartbeat.** A cancel request sets a flag in Postgres. The next heartbeat reads it and acts on the running task. What "acts" means depends on the kind of task; see section 6.2.

If a worker cannot reach Postgres, it stops its own task once its local view of the lease has run out, because it can no longer prove it owns the task.

### 6.1 Scaling workers

Each worker process reads one queue. Starting another worker for the same queue adds capacity: workers on a queue share its stream, and each message goes to only one of them. Stopping a worker removes capacity. A worker that is stopped cleanly finishes what it can within a grace period, then releases the leases of anything still running, so the reaper puts that work back on the queue at once. A worker that is killed instead is handled like any dead worker: its leases expire and the reaper recovers the work.

Capacity has three layers:

1. **Queues.** Each kind of work, or each group of tasks that should share capacity, gets its own queue, so a backlog in one never delays another. A queue can hold several tasks.
2. **Worker processes per queue.** How many Linux processes read a queue, for example 10 for `parse` and 20 for `fetch`. This is how heavy work scales, because each process can use its own CPU core.
3. **Tasks at once per worker process.** A per-queue concurrency setting, default one. `async` tasks share the process's event loop, which suits work that mostly waits, such as downloads. Plain tasks run on helper threads, which add no speed for heavy computation in Python, because threads of one process share one core.

The tasks running at once on a queue are the number of worker processes times the concurrency. Ending a worker process affects every task in it, which is why a task with a cancel grace period requires concurrency one.

Taskline does not start or stop workers. It reports, per queue, the tasks waiting, the tasks running, the age of the oldest waiting task and the live workers, in the command line and the admin UI. Whatever runs your workers (podman compose, systemd, Kubernetes, a script) uses those numbers to scale.

### 6.2 Cancelling a running task

A cancel request does not send a signal to a process. It sets a cancel flag on the running task instance, and until the task actually stops it shows "cancel requested". Tasks that have not started are cancelled at once. What happens to a running task depends on its kind:

- **An `async` task** is stopped at its next `await`. Its cleanup code runs, and taskline records `cancelled`.
- **A plain task** runs on a thread, and a thread cannot be stopped from outside. It stops in one of two ways:
  1. **The task checks.** A long task is written in steps, such as a document page by page, and calls `cancel_requested()` between steps. When it returns true, the task stops and returns, and taskline records `cancelled`. The task stops within one step.
  2. **The process ends.** A task can declare a cancel grace period. If the task has not stopped when the grace period is over, taskline records `cancelled` and then ends that worker process. The task dies with it, and the process manager starts a fresh worker. A grace period has three safeguards. It has no default: a task without one is never ended. It cannot be shorter than the lease length (60 seconds by default). And the clock starts when the worker learns of the cancel, at the heartbeat that reports it, not when the cancel was requested. A task with a grace period must be on a queue whose workers run one task at a time; taskline refuses to start a worker that breaks this, so ending the process cannot affect any other task.
- A plain task that neither checks nor has a grace period keeps running until it returns. It shows "cancel requested" in the meantime.

It is up to the task's author to divide long work into steps and to check for cancel between them.

## 7. When things go wrong

| What goes wrong | Who notices | What happens |
|---|---|---|
| Task raises an error, retries left | The worker (middleware) | Task becomes `retrying` with a backoff delay and a `retry_scheduled` event. The reaper sends it again when due. No result is published yet, so chain waiters are not told it failed |
| Task raises an error, no retries left | The worker | `failed`; later tasks `upstream_failed`; run `failed` |
| Task raises `NonRetryable` | The worker | Fails immediately regardless of attempts left |
| Worker process killed (power, crash, OOM) | The reaper, when the lease expires | Task marked lost. If it has attempts left, it is queued again as the next attempt. If this was its last attempt (a task that allows only one attempt is never run twice), it fails with `LeaseExpired` |
| Worker hangs or blocks its loop | The reaper, same as above | Same as above; if the hung worker wakes up later, the attempt number stops it |
| Messages missing from Valkey (it was wiped or restarted) | The reaper: Valkey holds fewer messages for a queue than Postgres expects | A warning in the log and in the queue statistics. The reaper changes nothing; a person decides whether to run `taskline requeue --queue <name>` |
| Message for a task the receiving worker does not know | The worker's broker | The task instance is failed with the reason `UnknownTask`, naming the task and the queue. It is recorded in the run history and fires the failure hook. Taskiq on its own would drop the message silently |
| A queue has tasks waiting and no live worker | The reaper | A warning in the log and in the queue statistics |
| A worker can reach Postgres but not Valkey | The worker's alive ping | The ping first checks that Valkey answers and skips the update if it does not, so the worker goes stale and is marked dead instead of appearing alive while consuming nothing |
| Chain hand-off lost (the process dies after a step's result is saved but before the next step is sent) | The reaper: a `planned` step whose previous step succeeded and which has waited for the hand-off time (two minutes by default) | The step is marked `failed` with the reason "hand-off lost", later steps are `upstream_failed`, and the run is `failed`. Resubmitting runs the chain again from its first step |
| Same message delivered twice | The second worker's claim | Claim fails; `duplicate_skipped` event; nothing runs twice |
| Worker shut down cleanly mid-task | The worker on shutdown | Its running tasks have their leases expired at once, so the reaper reclaims them immediately |
| Postgres unreachable at claim time | The worker | The message is not acknowledged and is redelivered later. The task does not run untracked |
| A hook raises | The middleware | Logged in the application log. The task outcome is not changed |
| Person cancels | Postgres flag, then heartbeat | Queued or planned tasks are cancelled immediately. A running task is told at its next heartbeat and stops as described in section 6.2 |

Any number of reapers can run at once. Each update locks the rows it takes and skips rows another reaper already holds, so two reapers split the work and never recover the same task twice. If one reaper dies, the others carry on at once.

### 7.1 Recovering a failed job

A job that fails at its third step of five does not have to start again from the top. There are two kinds of recovery.

- **Automatic retry.** A task that fails with attempts left is recorded as `retrying` and sent again after a wait, with the same values. The wait is `first delay × factor^(attempt − 1)`, capped at a maximum and varied by a jitter fraction; all four numbers are settings of the task. `NonRetryable`, cancellation, and errors that would repeat identically are not retried. The chain is not marked failed while a retry is pending.
- **Resume by a person.** After a task has failed for good, for example because someone corrected the data it works on, the failed run can be resumed in place. The failed task goes back in the queue with a fresh set of attempts, the later tasks return from `upstream_failed` to `planned`, and the stored message is sent again. That message holds the values of every earlier step and the plan for every later one, so the earlier tasks are not run again and the run continues with the same values. The history of that run shows the failure, the resume and what followed. This works when the fix was made outside taskline. If the values themselves must change, the job can be submitted again with a starting step and the values the earlier steps would have produced; that option is not built yet.

## 8. Why both Valkey and Postgres

- Valkey is fast at handing out messages. It is not where we want history, queries or audits.
- Postgres is the durable record, but polling it for new work would be slow and wasteful.

So messages travel through Valkey and truth lives in Postgres. The cost: two systems must agree. The reaper watches for disagreement and reports it, such as Valkey holding fewer messages for a queue than Postgres expects. It repairs only what is certain: tasks whose worker died, and retries that are due. The rule when they disagree: Postgres wins.

The queue is a Valkey **stream** with consumer groups. A worker acknowledges a message only after the task's result is saved. A message not acknowledged within ten minutes is redelivered automatically. That is a second safety net behind the reaper, and the claim step makes any redelivery safe.

## 9. Hooks versus the middleware

- The **middleware** is taskline's own code. It always runs. It does the claim, heartbeat, recording and chain bookkeeping. Users do not edit it.
- **Hooks** are functions an application registers: before a task starts, after success, after failure, when a retry is scheduled, when a task is cancelled, when a whole run finishes. Hooks can be registered for all tasks or for one task by name. They are for the application's own concerns (notifications, metrics, audit). They can read the lineage context but cannot change the outcome.

Any task can read its own identity through `current_lineage()`: run id, task position, attempt, correlation id. The same fields appear on every log line the task writes. A **correlation id** is a free-form label the application attaches at submit time (for example an ingest batch id). It is inherited by anything the task starts, so a whole tree of related work can be found by one id.

## 10. The data (schema `jobs`)

| Table | One row per | Holds |
|---|---|---|
| `job_run` | submitted job | name, trigger (api, schedule, manual), status, correlation id, **producer** (which application component submitted it, plus the host), the person who triggered it if any, the task that started it if any, the schedule that created it if any |
| `job_reference` | external reference attached to a run or one task | reference kind, external id, optional label and link, relation (subject, produced, caused_by). See section 10.1 |
| `task_instance` | task in a job run | task name, queue, status, attempt and max attempts, stored message, worker, lease expiry, last heartbeat, retry time, cancel flag, progress, error and traceback |
| `task_event` | thing that happened | append-only log: queued, claimed, succeeded, failed, retry scheduled, lost, requeued, cancel requested, cancelled, upstream failed, duplicate skipped |
| `worker` | worker process | queue, status (alive, stopped, dead), last seen |
| `schedule` | schedule definition | name, job, arguments, cron or interval, time zone, start time, end time, enabled, producer, overlap policy, missed-fire policy, who created it |
| `schedule_reference` | external reference copied onto every run a schedule creates | same shape as `job_reference`, attached to the schedule |
| `schedule_fire` | time a schedule was due | schedule, intended fire time (unique together), outcome (fired, skipped for overlap, skipped as missed), the run created if any |
| `*_kind` | allowed value | Lookup tables for every status and event type (no database enums, matching the project convention). Two of them are filled by the host application rather than by taskline: `producer_kind` and `reference_kind` (section 10.1) |

### 10.1 Producer and external references

A run with no context is litter: someone sees "fetch_source, failed" and cannot tell what it was for. So every run says who asked for it and what it concerns.

**Producer.** The application component that submitted the run, for example `strategiq.ingest` or `strategiq.api`. It is required. Producers are registered in `producer_kind` (code, name, description), and a submission naming an unregistered producer is refused, so spellings cannot drift. The run also records the host and process that submitted it and, where a person triggered it, who. A run started from inside a running task takes the parent's producer unless the submitter says otherwise, and records the parent task.

**External references.** Each reference ties a run, or one task inside it, to something in the host application's world:

- a **kind**, registered in `reference_kind`: for example `source`, `ingest_run`, `document`, `project`;
- an **external id**, which is the host's own id as text (taskline never looks it up);
- an optional label and link, so the admin UI can show "Zurich 10-K, 2026" and link back to the host's page;
- a **relation**: `subject` (what the job is about), `produced` (what the job created), or `caused_by` (what triggered it).

References come from two places. The submitter attaches them when submitting. A running task can add more once it knows them: a fetch task attaches `produced: document <id>` for each document it stores. Then you can start from a document and find the run that made it, or start from a source and see every run for it. References are indexed by kind and id so both lookups are fast.

A task can also declare reference kinds it requires. A submission of a job containing that task must supply them, or it is refused. This is how a host guarantees that no run of a given task can exist without its context.

Taskline does not interpret references. It stores and indexes them. Meaning stays with the host. If the host later deletes the object a reference points to, the reference stays as plain history.

All of it lives in a schema named `jobs`, so it never collides with the host application's tables. Taskline creates it with its own `taskline migrate` command and keeps its own version number in `jobs._schema_version`, so it does not depend on the host's migration tool.

Ids are UUID version 7, which start with the creation time, so rows sort by when they were made and new rows land at the end of the indexes. Taskline's broker generates run and task ids; the database generates the others with `uuidv7()`.

## 11. The boundary with the host application

What taskline needs from a host: a Postgres database it may create the `jobs` schema in, a Valkey/Redis server, and one or more Python modules that define tasks and jobs.

What taskline does not know about: the host's tables, settings, domain terms or package name. An automated test will enforce that nothing in `taskline` imports the host.

What the host does: declare tasks with taskline's `@task` decorator (name, queue, maximum attempts, timeout, cancel grace period, required reference kinds), define jobs as ordered lists of tasks, register its producers and reference kinds, submit jobs with a producer and references, register hooks, and point taskline at its modules through configuration.

In this repository, the StrategIQ application owns the Postgres container and hosts the `jobs` schema in its database. Taskline's own folder holds taskline's own `.env.example`, container file (Valkey, plus an optional Postgres for running taskline on its own) and docs.

### 11.1 Several applications

Each application runs its own taskline instance: its own workers, reaper and scheduler, its own database, and its own Valkey namespace. Instances can share a Postgres server and a Valkey server, but not tables or streams. The namespace is part of every stream name and consumer group name, so `strategiq:parse` and `otherapp:parse` never meet. Task names, schedule names and queues in one instance cannot clash with another's.

Execution is federated and management is central. The admin UI is meant to read several instances' databases and show them together, filtered by instance, while each instance keeps running on its own. A single shared ledger with an application field on every table is not planned; it would be added only if a shared view cannot be had this way.

## 12. Configuration

Taskline reads environment variables prefixed `TASKLINE_` (and its own `.env`): database URL, Valkey URL, queue-name prefix (so several applications can share one Valkey), the queue a worker consumes, which modules define tasks and jobs, which modules register hooks, heartbeat interval, lease multiple, result retention, the reaper's interval and batch size, the hand-off wait, and how long a silent worker waits before it is marked dead. Each task can override heartbeat, retries and timeout.

## 13. Assumptions still to prove by test

This comes from reading library source, not from running it, and needs a real worker process:

1. A plain task running on a helper thread of a real worker process is cancelled, or ended, as described in section 6.2.
