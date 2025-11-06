import asyncio
from asyncio import Queue
from dataclasses import dataclass
from logging import getLogger
from typing import Any, Callable, Set

from channels.exceptions import StopConsumer

from channels_kafka.util import ChannelRecipient, GroupRecipient, Recipient

logger = getLogger(__name__)

BackpressureWarningInterval = 5.0  # seconds
ExpiryWarningInterval = 5.0  # seconds


@dataclass
class _Message:
    data: dict[str, Any]
    n_queues: int
    time: float
    ack: Callable[[], None]

    def mark_delivered(self):
        self.n_queues -= 1
        if self.n_queues == 0:
            self.ack()


class _Channel:
    def __init__(self, max_size: int):
        self._queue: Queue[_Message] = Queue(max_size)

    def put_nowait(self, item: _Message):
        return self._queue.put_nowait(item)

    async def get(self) -> dict[str, Any]:
        message = await self._queue.get()
        message.mark_delivered()
        return message.data

    def remove_older_than(self, expire_time: float):
        cleaned = False

        while self._queue.qsize() and self._queue._queue[0].time < expire_time:
            message = self._queue._queue.popleft()
            message.mark_delivered()
            cleaned = True

        return cleaned

    def is_unused(self):
        return self._queue.empty() and len(self._queue._getters) == 0

    def close(self):
        for waiter in self._queue._getters:
            if not waiter.done():
                waiter.set_exception(StopConsumer("Django Channels is shutting down"))

    def is_empty(self):
        self._queue.empty()

    @property
    def oldest_message_time(self):
        return self._queue._queue[0].time


class MultiQueue:
    def __init__(self, max_size: int):
        self.max_size = max_size
        self.channels: dict[str, _Channel] = {}
        self._local_groups: dict[str, Set[str]] = {}
        self.n_messages = 0
        self._non_empty = asyncio.Event()
        self._last_logged_backpressure = 0
        self._last_logged_expired = 0
        self._closed = asyncio.Event()

    def add_channel(self, queue_name: str):
        assert queue_name not in self.channels, f"{queue_name} queue already exists"
        self.channels[queue_name] = _Channel(self.max_size)

    def _increase_n(self):
        assert self.n_messages < self.max_size
        self.n_messages += 1
        if self.n_messages == self.max_size:
            self.log_backpressure()
        self._non_empty.set()

    def _decrease_n(self):
        assert self.n_messages != 0
        self.n_messages -= 1
        if self.n_messages == 0:
            self._non_empty.clear()

    def _build_big_queues_str(self):
        queues = [(name, len(q._queue._queue)) for name, q in self.channels.items()]
        queues.sort(key=lambda q: q[1], reverse=True)
        return ", ".join(f"{name}: {q}" for name, q in queues[:3])

    def log_backpressure(self):
        now = asyncio.get_running_loop().time()
        if now - self._last_logged_backpressure > BackpressureWarningInterval:
            logger.warning(
                "Back-pressuring. Biggest queues: %s", self._build_big_queues_str()
            )
            self._last_logged_backpressure = now

    def put_nowait(
        self,
        recipient: Recipient,
        data: dict[str, Any],
        time: float,
        ack: Callable[[], None],
    ):
        if isinstance(recipient, ChannelRecipient):
            channels = [recipient.channel]
        else:
            assert isinstance(recipient, GroupRecipient)
            channels = self._local_groups.get(recipient.group, [])
            if len(channels) == 0:
                ack()
                return

        def ack_and_decrease_n():
            ack()
            self._decrease_n()

        message = _Message(data, len(channels), time, ack_and_decrease_n)
        for channel in channels:
            if channel not in self.channels:
                self.channels[channel] = _Channel(self.max_size)
            self.channels[channel].put_nowait(message)

        self._increase_n()

    async def get(self, channel: str):
        if self._closed.is_set():
            raise StopConsumer("Django Channels is shutting down")

        if channel not in self.channels:
            self.channels[channel] = _Channel(self.max_size)

        try:
            return await self.channels[channel].get()
        finally:
            if channel in self.channels and self.channels[channel].is_unused():
                del self.channels[channel]

    async def expire_until_closed(self, local_expiry: float):
        closed_task = asyncio.create_task(self._closed.wait())
        loop = asyncio.get_running_loop()

        while not closed_task.done():
            while not self._non_empty.is_set():
                non_empty_task = asyncio.create_task(self._non_empty.wait())
                await asyncio.wait(
                    {closed_task, non_empty_task}, return_when=asyncio.FIRST_COMPLETED
                )
                if closed_task.done():
                    return

            oldest_message_time = min(
                queue.oldest_message_time
                for queue in self.channels.values()
                if not queue.is_empty()
            )

            now = loop.time()
            if now - local_expiry < oldest_message_time:
                await asyncio.wait(
                    {closed_task}, timeout=oldest_message_time - now + local_expiry
                )
                if closed_task.done():
                    return
                now = loop.time()

            expired = self._clean_expired(now - local_expiry)
            if expired:
                self._log_expired()

    def _clean_expired(self, expire_time: float):
        cleaned = False
        to_delete = []

        for q_name, queue in self.channels.items():
            cleaned = cleaned or queue.remove_older_than(expire_time)
            if queue.is_unused():
                to_delete.append(q_name)

        for q_name in to_delete:
            del self.channels[q_name]

        return cleaned

    def _log_expired(self):
        now = asyncio.get_running_loop().time()
        if now - self._last_logged_expired > ExpiryWarningInterval:
            logger.warning(
                "Message expired locally. Biggest queues: %s",
                self._build_big_queues_str(),
            )
            self._last_logged_expired = now

    def group_add(self, group: str, channel: str):
        channels = self._local_groups.setdefault(group, set())
        channels.add(channel)
        return len(channels)

    def group_discard(self, group: str, channel):
        if group not in self._local_groups:
            return None

        channels = self._local_groups[group]
        if channel not in channels:
            return None
        channels.discard(channel)

        ret = len(channels)

        if ret == 0:
            del self._local_groups[group]

        return ret

    def close(self):
        if self._closed.is_set():
            return
        self._closed.set()

        for channel in self.channels.values():
            channel.close()

        self.channels.clear()
