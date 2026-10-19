import logging
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

from redis.asyncio import Redis
from taskiq import AsyncBroker, BrokerMessage
from taskiq_redis import RedisAsyncResultBackend, RedisStreamBroker

from taskline.config import TasklineConfig, get_config
from taskline.errors import UnknownTask
from taskline.execution import guarded
from taskline.hook_registry import HookRegistry, hooks
from taskline.middleware import ChainMiddleware, TasklineMiddleware
from taskline.spec import LABEL_QUEUE
from taskline.store import Store

log = logging.getLogger(__name__)

RECOVERY_LOCK_SECONDS = 30


class TasklineBroker(RedisStreamBroker):
    """A taskiq broker on Valkey streams, with one stream and consumer group per queue.

    Tasks and the database use plain queue names such as "parse". Only this class turns
    a queue into the stream "<namespace>:parse", when sending and when reading.
    """

    def __init__(
        self,
        url: str,
        *,
        namespace: str,
        consume_queue: str,
        redelivery_seconds: int,
        result_ttl_seconds: int,
        default_queue: str = "default",
    ) -> None:
        self.namespace = namespace
        self.consume_queue = consume_queue
        self.default_queue = default_queue
        super().__init__(
            url,
            queue_name=self.stream_name(consume_queue),
            consumer_group_name=self.stream_name(consume_queue),
            consumer_id="0",  # a new group starts at the beginning of the stream
            idle_timeout=redelivery_seconds * 1000,
            unacknowledged_lock_timeout=RECOVERY_LOCK_SECONDS,
        )
        self.with_result_backend(RedisAsyncResultBackend(url, result_ex_time=result_ttl_seconds))
        self.with_id_generator(lambda: uuid.uuid7().hex)

    def stream_name(self, queue: str) -> str:
        return f"{self.namespace}:{queue}"

    async def startup(self) -> None:
        if self.is_worker_process:
            await super().startup()
            await self._check_consumer_group()
        else:
            await AsyncBroker.startup(self)  # a producer never creates a consumer group

    async def kick(self, message: BrokerMessage) -> None:
        queue = message.labels.get(LABEL_QUEUE) or self.default_queue
        routed = message.model_copy(
            update={"labels": {**message.labels, LABEL_QUEUE: self.stream_name(queue)}}
        )
        await super().kick(routed)

    def find_task(self, task_name: str) -> Any:
        return super().find_task(task_name) or self._register_unknown_task(task_name)

    async def ping(self) -> None:
        async with Redis(connection_pool=self.connection_pool) as redis_conn:
            await redis_conn.ping()

    async def queue_length(self, queue: str) -> int:
        """The messages Valkey holds for a queue: waiting, plus read by a worker but not acknowledged."""
        async with Redis(connection_pool=self.connection_pool) as redis_conn:
            return await redis_conn.xlen(self.stream_name(queue))

    def _ack_generator(self, id: str, queue_name: str) -> Callable[[], Awaitable[None]]:
        async def ack() -> None:
            async with Redis(connection_pool=self.connection_pool) as redis_conn:
                pipeline = redis_conn.pipeline()
                pipeline.xack(queue_name, self.consumer_group_name, id)
                pipeline.xdel(queue_name, id)
                await pipeline.execute()

        return ack

    def _register_unknown_task(self, task_name: str) -> Any:
        @guarded
        async def unknown(*args: Any, **kwargs: Any) -> None:
            raise UnknownTask(
                f"task {task_name!r} is not defined in this worker (queue {self.consume_queue!r})"
            )

        return self.register_task(unknown, task_name=task_name)

    async def _check_consumer_group(self) -> None:
        async with Redis(connection_pool=self.connection_pool) as redis_conn:
            groups = await redis_conn.xinfo_groups(self.queue_name)
        for group in groups:
            name = group["name"]
            if (name.decode() if isinstance(name, bytes) else name) == self.consumer_group_name:
                log.info("consumer group %s on stream %s: %s", self.consumer_group_name,
                         self.queue_name, group)
                return
        raise RuntimeError(
            f"consumer group {self.consumer_group_name!r} does not exist on {self.queue_name!r}"
        )


def build_broker(
    store: Store,
    hook_registry: HookRegistry = hooks,
    config: TasklineConfig | None = None,
    consume_queue: str | None = None,
    **middleware_options: Any,
) -> TasklineBroker:
    """A broker with taskline's middleware and the chain library's middleware attached."""
    config = config or get_config()
    queue = consume_queue or config.consume_queue
    broker = TasklineBroker(
        config.valkey_url,
        namespace=config.namespace,
        consume_queue=queue,
        redelivery_seconds=config.redelivery_seconds,
        result_ttl_seconds=config.result_ttl_seconds,
    )
    broker.with_middlewares(
        TasklineMiddleware(
            store, hook_registry, consume_queue=queue, queue_reachable=broker.ping,
            **middleware_options,
        ),
        ChainMiddleware(),
    )
    return broker
