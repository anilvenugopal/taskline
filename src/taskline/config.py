from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class TasklineConfig(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="TASKLINE_", env_file=".env", extra="ignore")

    database_url: str
    valkey_url: str
    namespace: str = "taskline"  # prefix of every Valkey stream and consumer group name
    result_ttl_seconds: int = 86400  # how long Valkey keeps a task's result
    redelivery_seconds: int = 600  # an unacknowledged message is given to another worker after this
    reaper_interval_seconds: float = 30  # time between the reaper's passes
    reaper_batch_size: int = 100  # the most rows one check of one pass takes
    handoff_grace_seconds: float = 120  # a planned step waits this long for its previous step to send it
    dead_worker_seconds: float = 60  # a worker silent for this long is marked dead
    max_values_bytes: int = 262144  # the most a job's values dictionary may hold when sent as JSON
    heartbeat_interval_seconds: float = 15
    lease_intervals: int = 4  # the lease lasts this many heartbeat intervals
    consume_queue: str = "default"  # the queue a worker process reads
    job_modules: str = ""  # comma-separated modules that declare tasks; workers load them
    hook_modules: str = ""  # comma-separated modules that register hooks; workers and the reaper load them
    shutdown_wait_seconds: float = 30  # a stopping worker waits this long for running tasks


@lru_cache
def get_config() -> TasklineConfig:
    return TasklineConfig()
