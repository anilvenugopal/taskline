-- Taskline's published read contract: the schema taskline_api, made of read-only views, and the
-- indexes its filters need. The tables in jobs are private; other programs read only these views.
-- Never edit after release; a later change is a new numbered file. See docs/public_contract.md.

-- Indexes for the filters a list uses (the status, created time, producer and correlation indexes
-- already exist from 0001).
CREATE INDEX job_run_created_idx ON jobs.job_run (created_at);
CREATE INDEX job_run_name_idx ON jobs.job_run (job_name, created_at);
CREATE INDEX job_run_finished_idx ON jobs.job_run (job_run_status_code, finished_at);
CREATE INDEX job_run_schedule_idx ON jobs.job_run (schedule_id, created_at) WHERE schedule_id IS NOT NULL;
-- "Runs for this object" is answered from the index alone.
DROP INDEX jobs.job_reference_object_idx;
CREATE INDEX job_reference_object_idx ON jobs.job_reference (reference_kind_code, external_id, job_run_id);
-- A worker's running tasks, and runs with a cancel waiting to be acted on: both are small sets.
CREATE INDEX task_instance_worker_idx ON jobs.task_instance (worker_id) WHERE task_status_code = 'running';
CREATE INDEX task_instance_cancel_idx ON jobs.task_instance (job_run_id)
  WHERE task_status_code = 'running' AND cancel_requested_at IS NOT NULL;

CREATE SCHEMA taskline_api;

-- The columns worked out from a run's tasks are one small subquery each, so a column nobody
-- selects costs nothing, and a page count or a filter on the indexed columns never reads a task.
-- The current step is the lowest step that has not succeeded.
CREATE VIEW taskline_api.run AS
SELECT
  r.job_run_id AS run_id,
  r.job_name,
  r.job_run_status_code AS status,
  r.job_trigger_code AS trigger,
  r.producer_code AS producer,
  r.correlation_id,
  r.triggered_by,
  r.step_count,
  (SELECT t.step_index FROM jobs.task_instance t
    WHERE t.job_run_id = r.job_run_id AND t.task_status_code <> 'succeeded'
    ORDER BY t.step_index LIMIT 1) AS current_step_index,
  (SELECT t.task_name FROM jobs.task_instance t
    WHERE t.job_run_id = r.job_run_id AND t.task_status_code <> 'succeeded'
    ORDER BY t.step_index LIMIT 1) AS current_step_name,
  (SELECT t.progress_pct FROM jobs.task_instance t
    WHERE t.job_run_id = r.job_run_id AND t.task_status_code <> 'succeeded'
    ORDER BY t.step_index LIMIT 1) AS progress_pct,
  EXISTS (SELECT 1 FROM jobs.task_instance t
    WHERE t.job_run_id = r.job_run_id AND t.task_status_code = 'running'
      AND t.cancel_requested_at IS NOT NULL) AS cancel_requested,
  (SELECT t.error_type FROM jobs.task_instance t
    WHERE t.job_run_id = r.job_run_id AND t.task_status_code <> 'succeeded'
    ORDER BY t.step_index LIMIT 1) AS error_type,
  (SELECT left(split_part(t.error_detail, E'\n', 1), 200) FROM jobs.task_instance t
    WHERE t.job_run_id = r.job_run_id AND t.task_status_code <> 'succeeded'
    ORDER BY t.step_index LIMIT 1) AS error_summary,
  (SELECT sum(t.attempt)::integer FROM jobs.task_instance t
    WHERE t.job_run_id = r.job_run_id) AS attempts_total,
  r.created_at,
  (SELECT min(t.started_at) FROM jobs.task_instance t
    WHERE t.job_run_id = r.job_run_id) AS started_at,
  r.finished_at,
  extract(epoch FROM r.finished_at - (SELECT min(t.started_at) FROM jobs.task_instance t
    WHERE t.job_run_id = r.job_run_id))::double precision AS duration_seconds,
  r.schedule_id,
  (SELECT p.job_run_id FROM jobs.task_instance p
    WHERE p.task_instance_id = r.parent_task_instance_id) AS parent_run_id
FROM jobs.job_run r;

CREATE VIEW taskline_api.task AS
SELECT
  t.task_instance_id AS task_id,
  t.job_run_id AS run_id,
  t.step_index,
  t.task_name,
  t.queue_name AS queue,
  t.task_status_code AS status,
  t.attempt,
  t.max_attempts,
  t.worker_id,
  t.queued_at,
  t.started_at,
  t.finished_at,
  t.heartbeat_at,
  t.lease_expires_at,
  t.progress_pct,
  t.progress_note,
  t.cancel_requested_at,
  t.error_type,
  t.error_detail,
  t.error_traceback,
  t.retry_at AS next_retry_at
FROM jobs.task_instance t;

CREATE VIEW taskline_api.reference AS
SELECT
  f.job_run_id AS run_id,
  t.step_index,
  f.reference_kind_code AS kind,
  f.external_id,
  f.reference_relation_code AS relation,
  f.label,
  f.url,
  f.created_at
FROM jobs.job_reference f
LEFT JOIN jobs.task_instance t ON t.task_instance_id = f.task_instance_id;

-- One run's log. The ids increase but can become visible out of order: not a cursor.
CREATE VIEW taskline_api.event AS
SELECT
  e.task_event_id AS event_id,
  t.job_run_id AS run_id,
  e.task_instance_id AS task_id,
  t.step_index,
  t.task_name,
  e.task_event_code AS event,
  e.attempt,
  e.worker_id,
  e.detail,
  e.occurred_at
FROM jobs.task_event e
JOIN jobs.task_instance t ON t.task_instance_id = e.task_instance_id;

-- A queue with open tasks or live workers. The reaper registers itself under the queue 'reaper'
-- and is not a worker.
CREATE VIEW taskline_api.queue AS
SELECT
  COALESCE(l.queue_name, w.queue_name) AS queue,
  COALESCE(l.queued, 0) AS queued,
  COALESCE(l.running, 0) AS running,
  COALESCE(l.retrying, 0) AS retrying,
  l.oldest_queued_seconds,
  COALESCE(w.live_workers, 0) AS live_workers
FROM (
  SELECT queue_name,
         count(*) FILTER (WHERE task_status_code = 'queued') AS queued,
         count(*) FILTER (WHERE task_status_code = 'running') AS running,
         count(*) FILTER (WHERE task_status_code = 'retrying') AS retrying,
         extract(epoch FROM now() - min(queued_at) FILTER (WHERE task_status_code = 'queued'))
           ::double precision AS oldest_queued_seconds
  FROM jobs.task_instance
  WHERE task_status_code IN ('queued', 'running', 'retrying')
  GROUP BY queue_name
) l
FULL JOIN (
  SELECT queue_name, count(*) AS live_workers
  FROM jobs.worker
  WHERE worker_status_code = 'alive' AND queue_name <> 'reaper'
  GROUP BY queue_name
) w ON w.queue_name = l.queue_name;

-- A worker's id is host:pid:suffix and a reaper's is reaper:host:pid:suffix. Host and pid are read
-- from it; a pid that is not a number is left empty rather than failing every read.
CREATE VIEW taskline_api.worker AS
SELECT
  w.worker_id,
  CASE WHEN w.queue_name = 'reaper' THEN 'reaper' ELSE 'worker' END AS role,
  CASE WHEN w.queue_name = 'reaper' THEN NULL ELSE w.queue_name END AS queue,
  split_part(x.id, ':', 1) AS host,
  CASE WHEN split_part(x.id, ':', 2) ~ '^[0-9]{1,9}$' THEN split_part(x.id, ':', 2)::integer END AS pid,
  w.worker_status_code AS status,
  w.started_at,
  w.last_seen_at AS last_heartbeat_at,
  w.stopped_at,
  (SELECT count(*) FROM jobs.task_instance t
    WHERE t.worker_id = w.worker_id AND t.task_status_code = 'running') AS running_tasks,
  (SELECT t.task_instance_id FROM jobs.task_instance t
    WHERE t.worker_id = w.worker_id AND t.task_status_code = 'running'
    ORDER BY t.started_at LIMIT 1) AS current_task_id
FROM jobs.worker w
CROSS JOIN LATERAL (
  SELECT CASE WHEN w.queue_name = 'reaper' THEN regexp_replace(w.worker_id, '^reaper:', '')
              ELSE w.worker_id END AS id
) x;

CREATE VIEW taskline_api.instance AS
SELECT
  i.name,
  i.taskline_version,
  (SELECT max(version) FROM jobs._schema_version) AS schema_version
FROM jobs.instance i;
