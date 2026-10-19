-- taskline schema, version 1. Applied by `taskline migrate`. Never edit after release;
-- add 0002_*.sql instead.
CREATE SCHEMA IF NOT EXISTS jobs;

-- Closed value sets are kind tables (code, name, description, sort_order), not enums.
CREATE TABLE jobs.job_run_status_kind (code TEXT PRIMARY KEY, name TEXT NOT NULL, description TEXT, sort_order INT NOT NULL DEFAULT 0);
INSERT INTO jobs.job_run_status_kind VALUES
  ('running',   'Running',   'At least one task instance is queued, running or retrying', 1),
  ('succeeded', 'Succeeded', 'Every task instance succeeded', 2),
  ('failed',    'Failed',    'A task instance failed for good; later steps are upstream_failed', 3),
  ('cancelled', 'Cancelled', 'Cancelled by a person or the system', 4);

CREATE TABLE jobs.task_status_kind (code TEXT PRIMARY KEY, name TEXT NOT NULL, description TEXT, sort_order INT NOT NULL DEFAULT 0);
INSERT INTO jobs.task_status_kind VALUES
  ('planned',         'Planned',         'Chain step waiting for the previous step; not yet sent to the queue', 1),
  ('queued',          'Queued',          'Sent to the queue; no worker holds it', 2),
  ('running',         'Running',         'A worker holds the lease and is sending heartbeats', 3),
  ('retrying',        'Retrying',        'Failed or lost; waiting for retry_at before it is queued again', 4),
  ('succeeded',       'Succeeded',       'Finished without error', 5),
  ('failed',          'Failed',          'Out of attempts, or failed with a non-retryable error', 6),
  ('cancelled',       'Cancelled',       'Stopped on request', 7),
  ('upstream_failed', 'Upstream failed', 'Never ran because an earlier chain step failed', 8);

CREATE TABLE jobs.task_event_kind (code TEXT PRIMARY KEY, name TEXT NOT NULL, description TEXT, sort_order INT NOT NULL DEFAULT 0);
INSERT INTO jobs.task_event_kind VALUES
  ('queued',            'Queued',            'Message sent to the queue', 1),
  ('claimed',           'Claimed',           'A worker took the lease and started the attempt', 2),
  ('succeeded',         'Succeeded',         'Attempt finished without error', 3),
  ('failed',            'Failed',            'Attempt raised and no retry is left', 4),
  ('retry_scheduled',   'Retry scheduled',   'Attempt raised; another attempt is due at retry_at', 5),
  ('lost',              'Lost',              'Lease expired without a heartbeat; the reaper took the task back', 6),
  ('requeued',          'Requeued',          'The reaper sent the stored message to the queue again', 7),
  ('cancel_requested',  'Cancel requested',  'Cancellation asked for', 8),
  ('cancelled',         'Cancelled',         'Attempt or queued task stopped on request', 9),
  ('upstream_failed',   'Upstream failed',   'Marked because an earlier chain step failed', 10),
  ('duplicate_skipped', 'Duplicate skipped', 'A second delivery found the task already claimed or finished', 11);

CREATE TABLE jobs.job_trigger_kind (code TEXT PRIMARY KEY, name TEXT NOT NULL, description TEXT, sort_order INT NOT NULL DEFAULT 0);
INSERT INTO jobs.job_trigger_kind VALUES
  ('api',      'API',      'Submitted from application code', 1),
  ('schedule', 'Schedule', 'Fired by a scheduler', 2),
  ('manual',   'Manual',   'Submitted by a person from the CLI or a UI', 3);

CREATE TABLE jobs.worker_status_kind (code TEXT PRIMARY KEY, name TEXT NOT NULL, description TEXT, sort_order INT NOT NULL DEFAULT 0);
INSERT INTO jobs.worker_status_kind VALUES
  ('alive',   'Alive',   'Heartbeating', 1),
  ('stopped', 'Stopped', 'Shut down cleanly', 2),
  ('dead',    'Dead',    'Stopped heartbeating without shutting down', 3);

-- Filled by the host application with its own components, for example 'myapp.ingest'.
CREATE TABLE jobs.producer_kind (code TEXT PRIMARY KEY, name TEXT NOT NULL, description TEXT, sort_order INT NOT NULL DEFAULT 0);

-- Filled by the host application with the kinds of its objects a run can refer to, for example 'document'.
CREATE TABLE jobs.reference_kind (code TEXT PRIMARY KEY, name TEXT NOT NULL, description TEXT, sort_order INT NOT NULL DEFAULT 0);

CREATE TABLE jobs.reference_relation_kind (code TEXT PRIMARY KEY, name TEXT NOT NULL, description TEXT, sort_order INT NOT NULL DEFAULT 0);
INSERT INTO jobs.reference_relation_kind VALUES
  ('subject',   'Subject',   'What the job is about', 1),
  ('produced',  'Produced',  'What the job created', 2),
  ('caused_by', 'Caused by', 'What triggered the job', 3);

CREATE TABLE jobs.schedule_overlap_kind (code TEXT PRIMARY KEY, name TEXT NOT NULL, description TEXT, sort_order INT NOT NULL DEFAULT 0);
INSERT INTO jobs.schedule_overlap_kind VALUES
  ('allow',           'Allow',           'Start a new run even if the previous run is still going', 1),
  ('skip_if_running', 'Skip if running', 'Do not start a run while the previous run is still going', 2);

CREATE TABLE jobs.schedule_misfire_kind (code TEXT PRIMARY KEY, name TEXT NOT NULL, description TEXT, sort_order INT NOT NULL DEFAULT 0);
INSERT INTO jobs.schedule_misfire_kind VALUES
  ('skip',     'Skip',     'Ignore times missed while the scheduler was down', 1),
  ('run_once', 'Run once', 'Run once when the scheduler returns, however many times were missed', 2);

CREATE TABLE jobs.schedule_fire_outcome_kind (code TEXT PRIMARY KEY, name TEXT NOT NULL, description TEXT, sort_order INT NOT NULL DEFAULT 0);
INSERT INTO jobs.schedule_fire_outcome_kind VALUES
  ('fired',           'Fired',           'A run was created', 1),
  ('skipped_overlap', 'Skipped overlap', 'Not run because the previous run was still going', 2),
  ('skipped_missed',  'Skipped missed',  'Not run because the scheduler was down at the due time', 3);

CREATE TABLE jobs.worker (
  worker_id          TEXT PRIMARY KEY,                 -- host:pid:suffix
  queue_name         TEXT NOT NULL,
  worker_status_code TEXT NOT NULL REFERENCES jobs.worker_status_kind(code),
  started_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  stopped_at         TIMESTAMPTZ
);

CREATE TABLE jobs.job_run (
  job_run_id              UUID PRIMARY KEY,            -- lineage id: the first step's task id
  job_name                TEXT NOT NULL,
  job_trigger_code        TEXT NOT NULL REFERENCES jobs.job_trigger_kind(code),
  job_run_status_code     TEXT NOT NULL DEFAULT 'running' REFERENCES jobs.job_run_status_kind(code),
  step_count              INT  NOT NULL,
  correlation_id          TEXT,
  producer_code           TEXT NOT NULL REFERENCES jobs.producer_kind(code),
  producer_host           TEXT,                        -- machine that submitted the run
  triggered_by            TEXT,                        -- person who triggered the run, if any
  parent_task_instance_id UUID,                        -- set when a task started this run
  schedule_id             UUID,                        -- set when a schedule created this run
  created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at             TIMESTAMPTZ
);
CREATE INDEX job_run_status_idx ON jobs.job_run (job_run_status_code, created_at);
CREATE INDEX job_run_correlation_idx ON jobs.job_run (correlation_id);
CREATE INDEX job_run_producer_idx ON jobs.job_run (producer_code, created_at);

CREATE TABLE jobs.task_instance (
  task_instance_id    UUID PRIMARY KEY,                -- the taskiq task id
  job_run_id          UUID NOT NULL REFERENCES jobs.job_run(job_run_id),
  step_index          INT  NOT NULL,
  task_name           TEXT NOT NULL,
  queue_name          TEXT NOT NULL,
  task_status_code    TEXT NOT NULL REFERENCES jobs.task_status_kind(code),
  attempt             INT  NOT NULL DEFAULT 0,         -- claims so far; guards heartbeat and result writes
  max_attempts        INT  NOT NULL DEFAULT 1,
  message             JSONB,                           -- stored taskiq message; the reaper sends it again
  worker_id           TEXT REFERENCES jobs.worker(worker_id),
  queued_at           TIMESTAMPTZ,
  started_at          TIMESTAMPTZ,
  finished_at         TIMESTAMPTZ,
  heartbeat_at        TIMESTAMPTZ,
  lease_expires_at    TIMESTAMPTZ,
  retry_at            TIMESTAMPTZ,
  cancel_requested_at TIMESTAMPTZ,
  progress_pct        NUMERIC,
  progress_note       TEXT,
  error_type          TEXT,
  error_detail        TEXT,
  error_traceback     TEXT,
  UNIQUE (job_run_id, step_index)
);
CREATE INDEX task_instance_running_idx ON jobs.task_instance (lease_expires_at) WHERE task_status_code = 'running';
CREATE INDEX task_instance_retry_idx   ON jobs.task_instance (retry_at)        WHERE task_status_code = 'retrying';
CREATE INDEX task_instance_queued_idx  ON jobs.task_instance (queued_at)       WHERE task_status_code = 'queued';

-- job_run is created before task_instance, so this key is added afterwards.
ALTER TABLE jobs.job_run
  ADD FOREIGN KEY (parent_task_instance_id) REFERENCES jobs.task_instance(task_instance_id);

-- Links a run, or one of its tasks, to an object in the host application.
CREATE TABLE jobs.job_reference (
  job_reference_id        UUID PRIMARY KEY DEFAULT uuidv7(),
  job_run_id              UUID NOT NULL REFERENCES jobs.job_run(job_run_id),
  task_instance_id        UUID REFERENCES jobs.task_instance(task_instance_id),  -- empty: the reference is about the whole run
  reference_kind_code     TEXT NOT NULL REFERENCES jobs.reference_kind(code),
  external_id             TEXT NOT NULL,               -- the host's id for the object
  reference_relation_code TEXT NOT NULL REFERENCES jobs.reference_relation_kind(code),
  label                   TEXT,
  url                     TEXT,
  created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE NULLS NOT DISTINCT (job_run_id, task_instance_id, reference_kind_code, external_id, reference_relation_code)
);
CREATE INDEX job_reference_object_idx ON jobs.job_reference (reference_kind_code, external_id);
CREATE INDEX job_reference_run_idx ON jobs.job_reference (job_run_id);

-- Append-only log of what happened to each task instance.
CREATE TABLE jobs.task_event (
  task_event_id    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  task_instance_id UUID NOT NULL REFERENCES jobs.task_instance(task_instance_id),
  task_event_code  TEXT NOT NULL REFERENCES jobs.task_event_kind(code),
  attempt          INT,
  worker_id        TEXT REFERENCES jobs.worker(worker_id),
  detail           JSONB,
  occurred_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX task_event_task_idx ON jobs.task_event (task_instance_id, task_event_id);

-- A standing instruction to run a job at set times. Nothing fires before starts_at or at/after ends_at.
CREATE TABLE jobs.schedule (
  schedule_id           UUID PRIMARY KEY DEFAULT uuidv7(),
  name                  TEXT NOT NULL UNIQUE,
  job_name              TEXT NOT NULL,
  arguments             JSONB NOT NULL DEFAULT '{"args": [], "kwargs": {}}',
  cron                  TEXT,
  interval_seconds      INT,                           -- counted from starts_at, or created_at when starts_at is empty
  time_zone             TEXT NOT NULL DEFAULT 'UTC',   -- zone in which cron is read
  starts_at             TIMESTAMPTZ,
  ends_at               TIMESTAMPTZ,
  enabled               BOOLEAN NOT NULL DEFAULT true,
  producer_code         TEXT NOT NULL REFERENCES jobs.producer_kind(code),
  schedule_overlap_code TEXT NOT NULL DEFAULT 'skip_if_running' REFERENCES jobs.schedule_overlap_kind(code),
  schedule_misfire_code TEXT NOT NULL DEFAULT 'skip' REFERENCES jobs.schedule_misfire_kind(code),
  created_by            TEXT,
  created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK ((cron IS NULL) <> (interval_seconds IS NULL)),
  CHECK (interval_seconds IS NULL OR interval_seconds > 0),
  CHECK (starts_at IS NULL OR ends_at IS NULL OR ends_at > starts_at)
);

-- References copied onto every run the schedule creates.
CREATE TABLE jobs.schedule_reference (
  schedule_reference_id   UUID PRIMARY KEY DEFAULT uuidv7(),
  schedule_id             UUID NOT NULL REFERENCES jobs.schedule(schedule_id),
  reference_kind_code     TEXT NOT NULL REFERENCES jobs.reference_kind(code),
  external_id             TEXT NOT NULL,
  reference_relation_code TEXT NOT NULL REFERENCES jobs.reference_relation_kind(code),
  label                   TEXT,
  url                     TEXT,
  UNIQUE (schedule_id, reference_kind_code, external_id, reference_relation_code)
);

-- One row per time a schedule came due. The primary key stops two schedulers
-- from firing the same moment.
CREATE TABLE jobs.schedule_fire (
  schedule_id                UUID NOT NULL REFERENCES jobs.schedule(schedule_id),
  fire_time                  TIMESTAMPTZ NOT NULL,     -- the intended time, not the time the scheduler noticed
  schedule_fire_outcome_code TEXT NOT NULL REFERENCES jobs.schedule_fire_outcome_kind(code),
  job_run_id                 UUID REFERENCES jobs.job_run(job_run_id),
  decided_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (schedule_id, fire_time)
);

-- job_run is created before schedule, so this key is added afterwards.
ALTER TABLE jobs.job_run
  ADD FOREIGN KEY (schedule_id) REFERENCES jobs.schedule(schedule_id);
