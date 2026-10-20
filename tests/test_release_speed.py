"""Run before a release: `uv run pytest -m release -s`. It seeds a million runs and times what a
list over the run view does (docs/public_contract.md, "Speed of the views"). It takes a few
minutes. A page or count that takes a second or more fails it."""

import statistics
import time

import psycopg
import pytest
from sqlalchemy.engine import make_url

from taskline.migrate import migrate

pytestmark = pytest.mark.release

RUNS = 1_000_000
LIMIT_MS = 1000

# 90% of runs succeeded, 5% failed, 4% are running and 1% were cancelled. Each has three tasks
# and one reference; 20 job names; 100,000 correlation ids.
SEED = f"""
CREATE TABLE seed AS SELECT g, uuidv7() AS run_id,
  CASE WHEN g % 100 < 90 THEN 'succeeded' WHEN g % 100 < 95 THEN 'failed'
       WHEN g % 100 < 99 THEN 'running' ELSE 'cancelled' END AS status
FROM generate_series(1, {RUNS}) g;
INSERT INTO jobs.producer_kind (code, name) VALUES ('test.producer', 'Test');
INSERT INTO jobs.reference_kind (code, name) VALUES ('document', 'Document');
INSERT INTO jobs.job_run (job_run_id, job_name, job_trigger_code, job_run_status_code, step_count,
  correlation_id, producer_code, created_at, finished_at)
SELECT run_id, 'job_' || (g % 20), 'api', status, 3, 'corr-' || (g % 100000), 'test.producer',
  now() - make_interval(secs => ({RUNS} - g)),
  CASE WHEN status = 'running' THEN NULL ELSE now() - make_interval(secs => ({RUNS} - g) - 5) END
FROM seed;
INSERT INTO jobs.task_instance (task_instance_id, job_run_id, step_index, task_name, queue_name,
  task_status_code, attempt, max_attempts, started_at, finished_at, error_type, error_detail)
SELECT uuidv7(), s.run_id, k, 'task_' || k, 'queue_' || k,
  CASE WHEN s.status = 'succeeded' THEN 'succeeded' WHEN k = 0 THEN 'succeeded'
       WHEN s.status = 'failed' THEN CASE WHEN k = 1 THEN 'failed' ELSE 'upstream_failed' END
       WHEN s.status = 'running' THEN CASE WHEN k = 1 THEN 'running' ELSE 'planned' END
       ELSE 'cancelled' END,
  1, 3, now(), now(),
  CASE WHEN s.status = 'failed' AND k = 1 THEN 'ValueError' END,
  CASE WHEN s.status = 'failed' AND k = 1 THEN E'boom\\nsecond line' END
FROM seed s CROSS JOIN generate_series(0, 2) k;
INSERT INTO jobs.job_reference (job_run_id, reference_kind_code, external_id, reference_relation_code)
SELECT run_id, 'document', 'doc-' || g, 'subject' FROM seed;
"""

NEWEST = "ORDER BY created_at DESC, run_id DESC"
OPERATIONS = {
    "first page": f"SELECT * FROM taskline_api.run {NEWEST} LIMIT 50",
    "page 100 (skip 5,000 rows)": f"SELECT * FROM taskline_api.run {NEWEST} OFFSET 5000 LIMIT 50",
    "count, no filter": "SELECT count(*) FROM taskline_api.run",
    "count, status": "SELECT count(*) FROM taskline_api.run WHERE status = 'failed'",
    "page, status": f"SELECT * FROM taskline_api.run WHERE status = 'failed' {NEWEST} LIMIT 50",
    "page, job name": f"SELECT * FROM taskline_api.run WHERE job_name = 'job_7' {NEWEST} LIMIT 50",
    "count, job name": "SELECT count(*) FROM taskline_api.run WHERE job_name = 'job_7'",
    "page, correlation id": (
        f"SELECT * FROM taskline_api.run WHERE correlation_id = 'corr-123' {NEWEST} LIMIT 50"
    ),
    "page, reference": (
        "SELECT * FROM taskline_api.run WHERE run_id IN (SELECT run_id FROM taskline_api.reference "
        "WHERE kind = 'document' AND external_id = 'doc-4242') LIMIT 50"
    ),
    "poll, failed in the last 7 days": (
        "SELECT run_id, status, finished_at, error_summary FROM taskline_api.run "
        "WHERE status = 'failed' AND finished_at > now() - interval '7 days' ORDER BY finished_at"
    ),
    "queue view": "SELECT * FROM taskline_api.queue",
}


def test_the_run_view_stays_fast_on_a_million_runs(empty_database_url):
    migrate(empty_database_url)
    url = make_url(empty_database_url).set(drivername="postgresql").render_as_string(hide_password=False)

    timings: dict[str, float] = {}
    with psycopg.connect(url, autocommit=True) as conn:
        conn.execute(SEED)
        conn.execute("ANALYZE")
        for label, sql in OPERATIONS.items():
            runs = []
            for _ in range(3):
                started = time.perf_counter()
                conn.execute(sql).fetchall()
                runs.append((time.perf_counter() - started) * 1000)
            timings[label] = statistics.median(runs)

    for label, milliseconds in timings.items():
        print(f"  {label:<36}{milliseconds:>9.1f} ms")
    slow = {label: round(ms) for label, ms in timings.items() if ms >= LIMIT_MS}
    assert not slow, f"slower than {LIMIT_MS} ms at {RUNS:,} runs: {slow}"
