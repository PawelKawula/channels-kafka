import asyncio
import logging
import uuid
from collections import defaultdict
from typing import Any, Union

import msgpack
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import (
    GroupCoordinatorNotAvailableError,
    KafkaConnectionError,
    KafkaTimeoutError,
)
from channels.exceptions import StopConsumer
from channels.layers import BaseChannelLayer

from channels_kafka.multiqueue import MultiQueue
from channels_kafka.util import (
    ChannelRecipient,
    GroupRecipient,
    deserialize_message,
    serialize_message,
)

logger = logging.getLogger(__name__)


async def _poll_new_records(
    consumer: AIOKafkaConsumer,
    timeout: float,
    polling_error: asyncio.Event,
    queue: MultiQueue,
):
    try:
        while True:
            async for record in consumer:
                recipient, data = deserialize_message(record.value)
                logger.debug("%s received data: %s", recipient, data)
                time = asyncio.get_running_loop().time()
                queue.put_nowait(recipient, data, time, lambda: None)
    except Exception as ex:
        logger.exception(ex)
        polling_error.set()
        raise


class KafkaChannelLayer(BaseChannelLayer):
    extensions = ["groups", "flush"]

    def __init__(
        self,
        hosts: list[str] | None = None,
        client_id: str = "asgi",
        group_id: str = "django_channels_group",
        topic: str = "django_channels",
        local_capacity: int = 100,
        local_expiry=5,
        timeout=1,
    ):
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            raise RuntimeError(
                "Refusing to initialize channel layer without a running event loop."
            )

        self.dct = defaultdict(int)
        self.hosts = hosts if hosts else ["localhost:9092"]
        self.client_id = client_id
        self.group_id = group_id
        self.topic = topic
        self._queue = MultiQueue(local_capacity)
        self.timeout = timeout
        self.local_expiry = local_expiry

        self._closed = asyncio.Event()
        self._polling_error = asyncio.Event()
        self._want_close = False

        self._producer_future, self._consumer_future = (
            asyncio.Future[AIOKafkaProducer](),
            asyncio.Future[AIOKafkaConsumer](),
        )

        self.EXPECTED_EXCEPTIONS = (KafkaTimeoutError, KafkaConnectionError, OSError)

    async def _reconnect_forever(self, *, producer=False, consumer=False):
        assert sum((producer, consumer)) == 1
        instance = "producer" if producer else "consumer"
        future_name = f"_{instance}_future"
        connection = None
        logger.info("%s instance to be run", instance)
        while not self._want_close:
            while not self._want_close:
                try:
                    connection = await getattr(self, f"_get_{instance}")()
                except self.EXPECTED_EXCEPTIONS:
                    logger.warning(
                        "Retrying connecting to %s at %s", instance, self.hosts
                    )
                    await asyncio.sleep(1)
                    continue
                if self._want_close:
                    break

                break

            try:
                retries = 3
                for i in range(1, retries + 1):
                    try:
                        await connection.start()
                        future = getattr(self, future_name)
                        if not future.done():
                            future.set_result(connection)
                        logger.debug("%s connected to Kafka", instance)
                        break
                    except GroupCoordinatorNotAvailableError:
                        if i != 2:
                            logger.error(
                                f"Retrying connecting consumer since group coordinator not available...({i}/{retries})"
                            )
                            await asyncio.sleep(2)
                        else:
                            continue
                if consumer:
                    await _poll_new_records(
                        await self.consumer,
                        self.timeout,
                        self._polling_error,
                        self._queue,
                    )
                    await self._closed.wait()
                else:
                    close_task = asyncio.create_task(self._closed.wait())
                    polling_error_task = asyncio.create_task(self._polling_error.wait())
                    await asyncio.wait(
                        {close_task, polling_error_task},
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    self._polling_error.clear()
            except self.EXPECTED_EXCEPTIONS as ex:
                try:
                    connection.stop()
                except Exception as exc:
                    logger.exception(exc)
                logger.warning(
                    "Disconnected %s from Kafka: %s. Will reconnect.", instance, str(ex)
                )
                await asyncio.sleep(3)
                continue
        try:
            await self.close()
        except self.EXPECTED_EXCEPTIONS:
            pass

        if connection is not None and not self._closed.is_set():
            self._closed.set()
            await self.close()

    async def _get_producer(self):
        retries = 3
        for i in range(1, retries + 1):
            try:
                return AIOKafkaProducer(
                    bootstrap_servers=",".join(self.hosts),
                    client_id=self.client_id,
                    enable_idempotence=True,
                    acks="all",
                    value_serializer=msgpack.dumps,
                    linger_ms=50,
                )
            except Exception as ex:
                if i == retries:
                    raise ex
                await asyncio.sleep(3)

    async def _get_consumer(self):
        retries = 3
        for i in range(1, retries + 1):
            try:
                return AIOKafkaConsumer(
                    self.topic,
                    bootstrap_servers=",".join(self.hosts),
                    client_id=self.client_id,
                    group_id=self.group_id,
                    value_deserializer=msgpack.unpackb,
                    auto_offset_reset="earliest",
                    fetch_min_bytes=1,
                    fetch_max_wait_ms=50,
                )
            except GroupCoordinatorNotAvailableError as ex:
                if i != 2:
                    logger.error(
                        f"Retrying connecting consumer since group coordinator not available...({i}/{retries})"
                    )
                else:
                    raise ex

    @property
    async def producer(self):
        if self._producer_future.done():
            return self._producer_future.result()
        self.kafka_connection(producer=True)
        return await self._producer_future

    @property
    async def consumer(self):
        if self._consumer_future.done():
            return self._consumer_future.result()
        self.kafka_connection(consumer=True)
        return await self._consumer_future

    def kafka_connection(self, *, consumer=False, producer=False):
        assert sum([consumer, producer]) == 1
        if self._want_close:
            raise StopConsumer
        instance = "consumer" if consumer else "producer"
        reconnect_task_name = f"_{instance}_reconnect_forever_task"
        self.dct[reconnect_task_name] += 1
        if self.dct[reconnect_task_name] > 4:
            raise Exception()
        if not hasattr(self, reconnect_task_name):
            setattr(
                self,
                reconnect_task_name,
                asyncio.create_task(
                    self._reconnect_forever(producer=producer, consumer=consumer),
                    name=f"Create {instance} task",
                ),
            )
        expire_task_name = "_expire_task"
        if not hasattr(self, expire_task_name):
            expire_task = asyncio.create_task(
                self._queue.expire_until_closed(local_expiry=self.local_expiry)
            )
            logger.warning("created task expire locally")
            setattr(self, expire_task_name, expire_task)

    async def send(self, channel: str, message: dict) -> None:
        assert self.require_valid_channel_name(channel), "Invalid channel name"
        producer = await self.producer
        record = serialize_message(ChannelRecipient(channel), message)
        assert isinstance(channel, str)
        logger.debug("channel sending record %s to %s", record, channel)
        await producer.send_and_wait(self.topic, record)
        logger.debug("channel sent record %s to %s", record, channel)
        await self.consumer

    async def group_add(self, group, channel):
        assert self.require_valid_channel_name(channel), "Invalid channel name"
        logger.debug("channel %s added to group %s", channel, group)
        self._queue.group_add(group, channel)

    async def group_discard(self, group, channel):
        logger.debug("channel %s discarded from group %s", channel, group)
        self._queue.group_discard(group, channel)

    async def group_send(self, group: str, message: dict):
        producer = await self.producer
        assert isinstance(group, str)
        record = serialize_message(GroupRecipient(group), message)
        await self.consumer
        logger.debug("group sending record %s to %s", record, group)
        await producer.send_and_wait(self.topic, record)
        logger.debug("group sent record %s to %s", record, group)

    async def receive(self, channel: str) -> Any:
        assert self.require_valid_channel_name(channel), "Invalid channel name"
        logger.debug("receive %s channel", channel)
        await self.producer
        await self.consumer
        logger.debug("waiting for channel %s", channel)
        msg = await self._queue.get(channel)
        logger.debug("received %s for channel %s", msg, channel)
        return msg

    async def flush(self):
        if self._producer_future.done():
            producer = await self.producer
            try:
                async with asyncio.timeout(5):
                    await producer.flush()
            except asyncio.TimeoutError:
                logger.error("Producer couldn't flush all messages in time")
        if self._consumer_future.done():
            consumer = await self.consumer
            try:
                async with asyncio.timeout(5):
                    await consumer.seek_to_end()
            except asyncio.TimeoutError:
                logger.error("Consumer couldn't seek to end of the log in time")

    async def new_channel(self):
        return self.client_id + str(uuid.uuid1())

    async def close(self):
        logger.info("closing kafka channel layer")
        self._want_close = True
        poll_task = getattr(self, "_poll_new_records_task", None)
        if poll_task:
            poll_task.cancel()
        expire_task = getattr(self, "_expire_task", None)
        if expire_task:
            expire_task.cancel()
        self._closed.set()
        for name in ("producer", "consumer"):
            instance: asyncio.Future[Union[AIOKafkaProducer, AIOKafkaConsumer]] = (
                getattr(self, f"_{name}_future")
            )
            if instance and instance.done():
                await instance.result().stop()
